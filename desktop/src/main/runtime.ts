import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { appendFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import fs from "node:fs/promises";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import path from "node:path";
import { app, safeStorage, shell, systemPreferences } from "electron";
import packageJson from "../../package.json";
import {
  classifyStartupFailure,
  shouldAutoReconnectDashboardFailure,
  type StartupFailureAssessment,
  type StartupFailureKind,
} from "./dashboardStartup";
import { deriveOperationalReadiness } from "./shared/operationalReadiness";

type JsonRecord = Record<string, unknown>;

type LocalOperatorAuthMethod = "TOUCH_ID" | "PASSWORD_FALLBACK" | "NONE";
type LocalOperatorAuthResult =
  | "NONE"
  | "SUCCEEDED"
  | "FAILED"
  | "CANCELED"
  | "UNAVAILABLE"
  | "NOT_ENROLLED"
  | "LOCKOUT"
  | "EXPIRED";

export interface LocalOperatorAuthState {
  auth_available: boolean;
  auth_platform: string;
  auth_method: LocalOperatorAuthMethod;
  last_authenticated_at: string | null;
  last_auth_result: LocalOperatorAuthResult;
  last_auth_detail: string | null;
  auth_session_expires_at: string | null;
  auth_session_ttl_seconds: number | null;
  auth_session_active: boolean;
  local_operator_identity: string | null;
  auth_session_id: string | null;
  touch_id_available: boolean;
  secret_protection: {
    available: boolean;
    provider: "KEYCHAIN_SAFE_STORAGE" | "NONE";
    wrapper_ready: boolean;
    wrapper_path: string | null;
    protects_token_file_directly: boolean;
    detail: string;
  };
  latest_event: JsonRecord | null;
  recent_events: JsonRecord[];
  artifacts: {
    state_path: string;
    events_path: string;
    secret_wrapper_path: string;
  };
}

export interface DesktopCommandResult {
  ok: boolean;
  message: string;
  detail?: string;
  output?: string;
  state?: DesktopState;
  payload?: JsonRecord;
}

export interface DesktopState {
  connection: "live" | "snapshot" | "unavailable";
  dashboard: JsonRecord | null;
  health: JsonRecord | null;
  backendUrl: string | null;
  source: {
    mode: "live_api" | "attached_snapshot_bridge" | "snapshot_fallback" | "degraded_reconnecting" | "backend_down";
    label: string;
    detail: string;
    canRunLiveActions: boolean;
    healthReachable: boolean;
    apiReachable: boolean;
  };
  backend: {
    state: "starting" | "healthy" | "reconnecting" | "degraded" | "backend_down";
    label: string;
    detail: string;
    lastError: string | null;
    nextRetryAt: string | null;
    retryCount: number;
    pid: number | null;
    apiStatus: "responding" | "timed_out" | "unreachable" | "unknown";
    healthStatus: "ok" | "degraded" | "unreachable" | "unknown";
    managerOwned: boolean;
    startupFailureKind: StartupFailureKind;
    actionHint: string | null;
    staleListenerDetected: boolean;
    healthReachable: boolean;
    dashboardApiTimedOut: boolean;
    portConflictDetected: boolean;
  };
  startup: {
    preferredHost: string;
    preferredPort: number;
    preferredUrl: string;
    allowPortFallback: boolean;
    chosenHost: string | null;
    chosenPort: number | null;
    chosenUrl: string | null;
    mode: "SERVICE_ATTACHED" | "DESKTOP_MANAGED_DIAGNOSTIC" | "SNAPSHOT_ONLY" | "UNAVAILABLE";
    ownership: "attached_existing" | "started_managed" | "snapshot_only" | "unavailable";
    latestEvent: string | null;
    recentEvents: string[];
    failureKind: StartupFailureKind;
    recommendedAction: string | null;
    staleListenerDetected: boolean;
    healthReachable: boolean;
    dashboardApiTimedOut: boolean;
    managedExitCode: number | null;
    managedExitSignal: string | null;
  };
  infoFiles: string[];
  errors: string[];
  runtimeLogPath: string | null;
  backendLogPath: string | null;
  desktopLogPath: string | null;
  appVersion: string;
  manager: {
    running: boolean;
    lastExitCode: number | null;
    lastExitSignal: string | null;
    recentOutput: string[];
  };
  localAuth: LocalOperatorAuthState;
  refreshedAt: string;
}

function isWorkspaceRepoRoot(candidate: string): boolean {
  return (
    existsSync(path.join(candidate, "desktop", "package.json")) &&
    existsSync(path.join(candidate, "src", "mgc_v05l"))
  );
}

function findWorkspaceRepoRoot(start: string): string | null {
  let current = path.resolve(start);
  while (true) {
    if (isWorkspaceRepoRoot(current)) {
      return current;
    }
    const parent = path.dirname(current);
    if (parent === current) {
      return null;
    }
    current = parent;
  }
}

function bundledRepoRootHint(): string | null {
  const resourcesPath = typeof process.resourcesPath === "string" && process.resourcesPath.trim()
    ? process.resourcesPath
    : null;
  if (!resourcesPath) {
    return null;
  }
  const hintPaths = [
    path.join(resourcesPath, "app", ".mgc-local-config.json"),
    path.join(resourcesPath, ".mgc-local-config.json"),
  ];
  for (const hintPath of hintPaths) {
    try {
      if (!existsSync(hintPath)) {
        continue;
      }
      const payload = JSON.parse(readFileSync(hintPath, "utf8")) as JsonRecord;
      const repoRoot = typeof payload.repo_root === "string" ? payload.repo_root.trim() : "";
      if (repoRoot) {
        return path.resolve(repoRoot);
      }
    } catch {
      continue;
    }
  }
  return null;
}

function cliSwitchValue(name: string): string | null {
  const prefix = `--${name}=`;
  for (const arg of process.argv.slice(1)) {
    if (arg.startsWith(prefix)) {
      return arg.slice(prefix.length);
    }
    if (arg === `--${name}`) {
      return "1";
    }
  }
  return null;
}

function resolveWorkspaceRepoRoot(): string {
  const explicit = process.env.MGC_REPO_ROOT || cliSwitchValue("mgc-repo-root");
  if (explicit) {
    return path.resolve(explicit);
  }

  const bundledHint = bundledRepoRootHint();
  if (bundledHint) {
    return bundledHint;
  }

  const candidates = [
    process.cwd(),
    __dirname,
    process.resourcesPath,
    path.dirname(process.execPath),
    process.execPath,
  ];
  for (const candidate of candidates) {
    const resolved = findWorkspaceRepoRoot(candidate);
    if (resolved) {
      return resolved;
    }
  }

  const fallbackDesktopRoot = path.resolve(__dirname, "..", "..");
  return path.resolve(fallbackDesktopRoot, "..");
}

const REPO_ROOT = resolveWorkspaceRepoRoot();
const DESKTOP_ROOT = path.join(REPO_ROOT, "desktop");
const OUTPUT_ROOT = path.join(REPO_ROOT, "outputs", "operator_dashboard");
const RUNTIME_ROOT = path.join(OUTPUT_ROOT, "runtime");
const DEFAULT_INFO_FILE = path.join(RUNTIME_ROOT, "operator_dashboard.json");
const DEFAULT_LOG_FILE = path.join(RUNTIME_ROOT, "operator_dashboard.log");
function desktopAppStateRoot(): string {
  const explicit = String(process.env.MGC_DESKTOP_STATE_CACHE_ROOT || "").trim();
  if (explicit) {
    return explicit;
  }
  try {
    return path.join(app.getPath("userData"), "runtime");
  } catch {
    const tempRoot = String(process.env.TMPDIR || "/tmp").trim() || "/tmp";
    return path.join(tempRoot, "mgc-operator-runtime");
  }
}

const DESKTOP_APP_STATE_ROOT = desktopAppStateRoot();
const DESKTOP_LOG_FILE = path.join(DESKTOP_APP_STATE_ROOT, "desktop_electron.log");
const DESKTOP_STARTUP_STATUS_FILE = path.join(DESKTOP_APP_STATE_ROOT, "desktop_dashboard_startup_status.json");
const DESKTOP_LOCAL_STATE_ROOT = (() => {
  const explicit = String(process.env.MGC_DESKTOP_WORKSPACE_CACHE_ROOT || "").trim();
  if (explicit) {
    return explicit;
  }
  return path.join(DESKTOP_APP_STATE_ROOT, "desktop_cache");
})();
const DESKTOP_LOCAL_DASHBOARD_CACHE_FILE = path.join(DESKTOP_LOCAL_STATE_ROOT, "dashboard_api_snapshot.cache.json");
const DESKTOP_LOCAL_READINESS_FILE = path.join(DESKTOP_LOCAL_STATE_ROOT, "operator_dashboard_readiness.json");
const LOCAL_OPERATOR_AUTH_ROOT = path.join(DESKTOP_APP_STATE_ROOT, "local_operator_auth");
const DASHBOARD_READINESS_FILE = path.join(RUNTIME_ROOT, "operator_dashboard_readiness.json");
const LOCAL_OPERATOR_AUTH_STATE_FILE = path.join(LOCAL_OPERATOR_AUTH_ROOT, "local_operator_auth_state.json");
const LOCAL_OPERATOR_AUTH_EVENTS_FILE = path.join(LOCAL_OPERATOR_AUTH_ROOT, "local_operator_auth_events.jsonl");
const LOCAL_SECRET_WRAPPER_FILE = path.join(LOCAL_OPERATOR_AUTH_ROOT, "local_secret_wrapper.json");
const DEFAULT_DASHBOARD_HOST = process.env.MGC_OPERATOR_DASHBOARD_HOST || "127.0.0.1";
const DEFAULT_DASHBOARD_PORT = Number(process.env.MGC_OPERATOR_DASHBOARD_PORT || 8790);
const DEFAULT_DASHBOARD_URL = `http://${DEFAULT_DASHBOARD_HOST}:${DEFAULT_DASHBOARD_PORT}/`;
const ALLOW_PORT_FALLBACK = process.env.MGC_OPERATOR_DASHBOARD_ALLOW_PORT_FALLBACK === "1";
const LOCAL_OPERATOR_AUTH_SESSION_TTL_SECONDS = Math.max(
  60,
  Number(process.env.MGC_LOCAL_OPERATOR_AUTH_SESSION_TTL_SECONDS || 28800),
);
const EXPLICIT_DASHBOARD_URLS = String(process.env.MGC_OPERATOR_DASHBOARD_URLS || "")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);
const DESKTOP_STATE_FIXTURE_PATH = String(process.env.MGC_DESKTOP_STATE_FIXTURE_PATH || "").trim();
const ATTACHED_SNAPSHOT_BRIDGE_MAX_AGE_MS = 60_000;
const PACKAGED_SYNCHRONIZED_SNAPSHOT_MAX_AGE_MS = 10 * 60_000;
const SNAPSHOT_FILES = {
  dashboardApi: path.join(OUTPUT_ROOT, "dashboard_api_snapshot.json"),
  historicalPlayback: path.join(OUTPUT_ROOT, "historical_playback_snapshot.json"),
  researchRuntimeBridge: path.join(OUTPUT_ROOT, "research_runtime_bridge_snapshot.json"),
  strategyAnalysis: path.join(OUTPUT_ROOT, "strategy_analysis_snapshot.json"),
  marketIndexStrip: path.join(OUTPUT_ROOT, "market_index_strip_snapshot.json"),
  operatorSurface: path.join(OUTPUT_ROOT, "operator_surface_snapshot.json"),
  paperApprovedModels: path.join(OUTPUT_ROOT, "paper_approved_models_snapshot.json"),
  paperBlotter: path.join(OUTPUT_ROOT, "paper_latest_blotter_snapshot.json"),
  paperCarryForward: path.join(OUTPUT_ROOT, "paper_carry_forward_state.json"),
  paperFills: path.join(OUTPUT_ROOT, "paper_latest_fills_snapshot.json"),
  paperIntents: path.join(OUTPUT_ROOT, "paper_latest_intents_snapshot.json"),
  paperLaneActivity: path.join(OUTPUT_ROOT, "paper_lane_activity_snapshot.json"),
  paperNonApprovedLanes: path.join(OUTPUT_ROOT, "paper_non_approved_lanes_snapshot.json"),
  paperPerformance: path.join(OUTPUT_ROOT, "paper_performance_snapshot.json"),
  paperPosition: path.join(OUTPUT_ROOT, "paper_position_state_snapshot.json"),
  paperReadiness: path.join(OUTPUT_ROOT, "paper_readiness_snapshot.json"),
  startupControlPlane: path.join(OUTPUT_ROOT, "startup_control_plane_snapshot.json"),
  treasuryCurve: path.join(OUTPUT_ROOT, "treasury_curve_snapshot.json"),
  actionLog: path.join(OUTPUT_ROOT, "action_log.jsonl"),
  productionLink: path.join(OUTPUT_ROOT, "production_link_snapshot.json"),
};
const HEALTH_TIMEOUT_MS = 5000;
const DASHBOARD_TIMEOUT_MS = 120000;
const DASHBOARD_STARTUP_TIMEOUT_MS = 120000;
const STARTUP_HEALTH_TIMEOUT_MS = 1000;
const STARTUP_DASHBOARD_TIMEOUT_MS = 5000;
const SNAPSHOT_PROMOTION_GRACE_MS = 1500;
const HISTORICAL_PLAYBACK_MANIFEST_CACHE_TTL_MS = 10000;
const RECONNECT_BACKOFF_MS = [2000, 5000, 10000, 20000, 30000];
const DASHBOARD_AUTH_RECOVERY_BACKOFF_MS = [0, 5000, 15000, 30000];

let dashboardManager: ChildProcess | null = null;
let recentManagerOutput: string[] = [];
let lastExitCode: number | null = null;
let lastExitSignal: string | null = null;
let desktopStateRequestPromise: Promise<DesktopState> | null = null;
let dashboardLaunchPromise: Promise<DesktopState> | null = null;
let serviceHostBootstrapPromise: Promise<void> | null = null;
let authGateRecoveryPromise: Promise<void> | null = null;
let reconnectTimer: NodeJS.Timeout | null = null;
let reconnectAttemptCount = 0;
let nextRetryAt: string | null = null;
let authGateRecoveryAttemptCount = 0;
let authGateRecoveryNextRetryAt: string | null = null;
let managerLastError: string | null = null;
let managerLifecycle: "idle" | "starting" | "healthy" | "reconnecting" | "degraded" = "idle";
let managerOwnsBackend = false;
let stopWasRequested = false;
let shutdownRequested = false;
let testGetDesktopStateHook: (() => Promise<DesktopState>) | null = null;
let testBeginDashboardLaunchHook: ((options: { manual: boolean }) => Promise<DesktopState>) | null = null;
let testEnsureServiceHostUsableHook: (() => Promise<void>) | null = null;
let testFetchHook: ((input: string | URL, init?: RequestInit) => Promise<Response>) | null = null;
let testExecScriptHook: ((args: string[]) => Promise<{ ok: boolean; stdout: string; stderr: string; code: number | null }>) | null = null;
let testCurlJsonHook: ((url: string, timeoutMs: number) => Promise<JsonRecord>) | null = null;
let testBuildLocalOperatorAuthStateHook: (() => Promise<LocalOperatorAuthState>) | null = null;
let testAutoBootstrapBlockedHook: (() => boolean) | null = null;
let testLoadLiveDashboardHook:
  | ((urls: string[], options?: LoadLiveDashboardOptions) => Promise<LoadLiveDashboardResult>)
  | null = null;
let testLoadSnapshotBundleHook: (() => Promise<JsonRecord | null>) | null = null;
let testLoadAttachedSnapshotBridgeHook: ((snapshot: JsonRecord | null) => Promise<AttachedSnapshotBridge | null>) | null = null;
let testPackagedLocalBundleLaunchContextHook: (() => boolean) | null = null;
let historicalPlaybackManifestInfoCache:
  | {
      fetchedAtMs: number;
      value: { path: string | null; runStamp: string | null; modifiedAt: string | null };
    }
  | null = null;

function packagedLocalBundleLaunchContext(): boolean {
  if (testPackagedLocalBundleLaunchContextHook) {
    return testPackagedLocalBundleLaunchContextHook();
  }
  if (bundledRepoRootHint()) {
    return true;
  }
  return /\/MGC Operator\.app\/Contents\/MacOS\/MGC Operator$/u.test(process.execPath)
    || process.execPath.includes("/MGC Operator.app/Contents/MacOS/");
}

function autoBootstrapBlockedBySandbox(): boolean {
  if (testAutoBootstrapBlockedHook) {
    return testAutoBootstrapBlockedHook();
  }
  const explicit = String(process.env.MGC_DESKTOP_AUTO_BOOTSTRAP || "").trim().toLowerCase();
  if (explicit === "1" || explicit === "true" || explicit === "yes" || explicit === "on") {
    return false;
  }
  if (explicit === "0" || explicit === "false" || explicit === "no" || explicit === "off") {
    return true;
  }
  if (String(process.env.CODEX_SANDBOX || "").trim()) {
    return true;
  }
  return packagedLocalBundleLaunchContext();
}

const SENSITIVE_DASHBOARD_ACTIONS = new Set([
  "same-underlying-acknowledge",
  "same-underlying-mark-observational",
  "same-underlying-hold-entries",
  "same-underlying-clear-hold",
  "same-underlying-reset-review",
  "paper-force-lane-resume-session-override",
]);

const SENSITIVE_PRODUCTION_ACTIONS = new Set([
  "submit-order",
  "flatten-position",
  "cancel-order",
  "replace-order",
]);

function normalizeBaseUrl(url: string): string {
  return url.endsWith("/") ? url : `${url}/`;
}

function parsePort(value: unknown): number | null {
  const numeric = Number(value);
  return Number.isFinite(numeric) && numeric > 0 ? numeric : null;
}

function parseHost(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  return value.trim() || null;
}

function safeUrlParts(url: string | null): { host: string | null; port: number | null } {
  if (!url) {
    return { host: null, port: null };
  }
  try {
    const parsed = new URL(url);
    return {
      host: parsed.hostname || null,
      port: parsed.port ? Number(parsed.port) : parsed.protocol === "https:" ? 443 : 80,
    };
  } catch {
    return { host: null, port: null };
  }
}

function asJsonRecord(value: unknown): JsonRecord {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as JsonRecord) : {};
}

function deepMergeJson(base: unknown, override: unknown): unknown {
  if (override === undefined) {
    return base;
  }
  if (Array.isArray(override)) {
    return override;
  }
  if (override === null || typeof override !== "object") {
    return override;
  }
  const baseRecord = asJsonRecord(base);
  const overrideRecord = asJsonRecord(override);
  const merged: JsonRecord = { ...baseRecord };
  for (const [key, value] of Object.entries(overrideRecord)) {
    merged[key] = deepMergeJson(baseRecord[key], value);
  }
  return merged;
}

function nowIso(): string {
  return new Date().toISOString();
}

function parseIsoDate(value: unknown): Date | null {
  if (typeof value !== "string" || !value.trim()) {
    return null;
  }
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function normalizeLocalOperatorIdentity(value: unknown): string | null {
  const text = typeof value === "string" ? value.trim() : "";
  return text || null;
}

function localAuthArtifacts(): LocalOperatorAuthState["artifacts"] {
  return {
    state_path: LOCAL_OPERATOR_AUTH_STATE_FILE,
    events_path: LOCAL_OPERATOR_AUTH_EVENTS_FILE,
    secret_wrapper_path: LOCAL_SECRET_WRAPPER_FILE,
  };
}

function localAuthAvailability(): {
  auth_available: boolean;
  auth_platform: string;
  auth_method: LocalOperatorAuthMethod;
  touch_id_available: boolean;
  availability_reason: string;
} {
  if (process.platform !== "darwin") {
    return {
      auth_available: false,
      auth_platform: process.platform,
      auth_method: "NONE",
      touch_id_available: false,
      availability_reason: "Touch ID local operator auth is only available on macOS in this pass.",
    };
  }
  try {
    const touchIdAvailable = Boolean(systemPreferences.canPromptTouchID());
    return {
      auth_available: touchIdAvailable,
      auth_platform: "macOS",
      auth_method: touchIdAvailable ? "TOUCH_ID" : "NONE",
      touch_id_available: touchIdAvailable,
      availability_reason: touchIdAvailable
        ? "Touch ID is available for local operator authentication."
        : "Touch ID is unavailable or not enrolled on this Mac.",
    };
  } catch (error) {
    return {
      auth_available: false,
      auth_platform: "macOS",
      auth_method: "NONE",
      touch_id_available: false,
      availability_reason: error instanceof Error ? error.message : String(error),
    };
  }
}

function classifyTouchIdFailure(error: unknown): { result: LocalOperatorAuthResult; detail: string } {
  const detail = error instanceof Error ? error.message : String(error);
  const text = detail.toLowerCase();
  if (text.includes("cancel")) {
    return { result: "CANCELED", detail };
  }
  if (text.includes("lockout") || text.includes("locked")) {
    return { result: "LOCKOUT", detail };
  }
  if (text.includes("enrolled") || text.includes("biometry") || text.includes("touch id not available")) {
    return { result: "NOT_ENROLLED", detail };
  }
  if (text.includes("not available") || text.includes("unsupported")) {
    return { result: "UNAVAILABLE", detail };
  }
  return { result: "FAILED", detail };
}

function defaultLocalOperatorAuthState(): Omit<LocalOperatorAuthState, "secret_protection" | "latest_event" | "recent_events" | "artifacts"> {
  const availability = localAuthAvailability();
  return {
    auth_available: availability.auth_available,
    auth_platform: availability.auth_platform,
    auth_method: availability.auth_method,
    last_authenticated_at: null,
    last_auth_result: "NONE",
    last_auth_detail: availability.availability_reason,
    auth_session_expires_at: null,
    auth_session_ttl_seconds: LOCAL_OPERATOR_AUTH_SESSION_TTL_SECONDS,
    auth_session_active: false,
    local_operator_identity: null,
    auth_session_id: null,
    touch_id_available: availability.touch_id_available,
  };
}

async function appendJsonlRecord(filePath: string, payload: JsonRecord): Promise<void> {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.appendFile(filePath, `${JSON.stringify(payload)}\n`, "utf8");
}

async function writeJsonFileAtomic(filePath: string, payload: unknown): Promise<void> {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  const tempPath = path.join(
    path.dirname(filePath),
    `.${path.basename(filePath)}.${process.pid}.${Date.now()}.${Math.random().toString(16).slice(2)}.tmp`,
  );
  try {
    await fs.writeFile(tempPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
    await fs.rename(tempPath, filePath);
  } finally {
    try {
      await fs.unlink(tempPath);
    } catch {
      // Temp file already renamed or never created.
    }
  }
}

async function readJsonlRecords(filePath: string, limit = 50): Promise<JsonRecord[]> {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return raw
      .split(/\r?\n/)
      .filter(Boolean)
      .slice(-limit)
      .map((line) => JSON.parse(line) as JsonRecord)
      .reverse();
  } catch {
    return [];
  }
}

async function ensureLocalSecretWrapper(): Promise<LocalOperatorAuthState["secret_protection"]> {
  if (process.platform !== "darwin") {
    return {
      available: false,
      provider: "NONE",
      wrapper_ready: false,
      wrapper_path: null,
      protects_token_file_directly: false,
      detail: "Keychain-backed local secret wrapping is only available on macOS in this pass.",
    };
  }
  if (!safeStorage.isEncryptionAvailable()) {
    return {
      available: false,
      provider: "NONE",
      wrapper_ready: false,
      wrapper_path: null,
      protects_token_file_directly: false,
      detail: "Electron safeStorage is not available, so no Keychain-backed local secret wrapper is active.",
    };
  }
  try {
    await fs.access(LOCAL_SECRET_WRAPPER_FILE);
  } catch {
    const envelope = {
      version: 1,
      provider: "KEYCHAIN_SAFE_STORAGE",
      created_at: nowIso(),
      note: "Keychain-backed local secret wrapper for future sensitive local secret unseal flows. Schwab token-file encryption remains deferred in this pass.",
      protected_material_b64: safeStorage.encryptString(JSON.stringify({ key_id: randomUUID(), created_at: nowIso() })).toString("base64"),
    };
    await fs.mkdir(path.dirname(LOCAL_SECRET_WRAPPER_FILE), { recursive: true });
    await fs.writeFile(LOCAL_SECRET_WRAPPER_FILE, `${JSON.stringify(envelope, null, 2)}\n`, "utf8");
  }
  return {
    available: true,
    provider: "KEYCHAIN_SAFE_STORAGE",
    wrapper_ready: true,
    wrapper_path: LOCAL_SECRET_WRAPPER_FILE,
    protects_token_file_directly: false,
    detail:
      "A macOS Keychain-backed local secret wrapper is ready via Electron safeStorage. Schwab OAuth and SCHWAB_TOKEN_FILE remain unchanged; direct token-file encryption is deferred.",
  };
}

function passiveLocalSecretProtectionState(): LocalOperatorAuthState["secret_protection"] {
  if (process.platform !== "darwin") {
    return {
      available: false,
      provider: "NONE",
      wrapper_ready: false,
      wrapper_path: null,
      protects_token_file_directly: false,
      detail: "Keychain-backed local secret wrapping is only available on macOS in this pass.",
    };
  }
  return {
    available: true,
    provider: "KEYCHAIN_SAFE_STORAGE",
    wrapper_ready: existsSync(LOCAL_SECRET_WRAPPER_FILE),
    wrapper_path: existsSync(LOCAL_SECRET_WRAPPER_FILE) ? LOCAL_SECRET_WRAPPER_FILE : null,
    protects_token_file_directly: false,
    detail:
      "Keychain-backed local secret wrapping is supported on macOS, but normal app launch does not initialize or touch Keychain Safe Storage.",
  };
}

async function readStoredLocalOperatorAuthState(): Promise<JsonRecord> {
  return (await readJsonFile<JsonRecord>(LOCAL_OPERATOR_AUTH_STATE_FILE)) ?? {};
}

async function writeLocalOperatorAuthState(state: JsonRecord): Promise<void> {
  await fs.mkdir(path.dirname(LOCAL_OPERATOR_AUTH_STATE_FILE), { recursive: true });
  await fs.writeFile(LOCAL_OPERATOR_AUTH_STATE_FILE, `${JSON.stringify(state, null, 2)}\n`, "utf8");
}

async function appendLocalOperatorAuthEvent(payload: JsonRecord): Promise<void> {
  const event = {
    event_id: String(payload.event_id ?? randomUUID()),
    occurred_at: String(payload.occurred_at ?? nowIso()),
    ...payload,
  };
  await appendJsonlRecord(LOCAL_OPERATOR_AUTH_EVENTS_FILE, event);
}

async function buildLocalOperatorAuthState(): Promise<LocalOperatorAuthState> {
  if (testBuildLocalOperatorAuthStateHook) {
    return testBuildLocalOperatorAuthStateHook();
  }
  const availability = localAuthAvailability();
  const stored = asJsonRecord(await readStoredLocalOperatorAuthState());
  const secretProtection = passiveLocalSecretProtectionState();
  const now = new Date();
  const expiresAt = parseIsoDate(stored.auth_session_expires_at);
  const sessionStillActive = Boolean(stored.auth_session_active) && expiresAt !== null && expiresAt.getTime() > now.getTime();
  let nextResult = String(stored.last_auth_result || defaultLocalOperatorAuthState().last_auth_result) as LocalOperatorAuthResult;
  let nextDetail = typeof stored.last_auth_detail === "string" ? stored.last_auth_detail : availability.availability_reason;
  if (Boolean(stored.auth_session_active) && !sessionStillActive && expiresAt !== null) {
    nextResult = "EXPIRED";
    nextDetail = `Local operator auth session expired at ${expiresAt.toISOString()}.`;
  }
  const normalized: LocalOperatorAuthState = {
    auth_available: availability.auth_available,
    auth_platform: availability.auth_platform,
    auth_method: availability.auth_method,
    last_authenticated_at: typeof stored.last_authenticated_at === "string" ? stored.last_authenticated_at : null,
    last_auth_result: nextResult,
    last_auth_detail: nextDetail,
    auth_session_expires_at: typeof stored.auth_session_expires_at === "string" ? stored.auth_session_expires_at : null,
    auth_session_ttl_seconds: Number(stored.auth_session_ttl_seconds || LOCAL_OPERATOR_AUTH_SESSION_TTL_SECONDS),
    auth_session_active: sessionStillActive,
    local_operator_identity:
      sessionStillActive || typeof stored.local_operator_identity === "string"
        ? normalizeLocalOperatorIdentity(stored.local_operator_identity)
        : null,
    auth_session_id: sessionStillActive && typeof stored.auth_session_id === "string" ? stored.auth_session_id : null,
    touch_id_available: availability.touch_id_available,
    secret_protection: secretProtection,
    latest_event: null,
    recent_events: [],
    artifacts: localAuthArtifacts(),
  };
  const recentEvents = await readJsonlRecords(LOCAL_OPERATOR_AUTH_EVENTS_FILE, 40);
  normalized.recent_events = recentEvents;
  normalized.latest_event = recentEvents[0] ?? null;
  const persistedState = {
    auth_available: normalized.auth_available,
    auth_platform: normalized.auth_platform,
    auth_method: normalized.auth_method,
    last_authenticated_at: normalized.last_authenticated_at,
    last_auth_result: normalized.last_auth_result,
    last_auth_detail: normalized.last_auth_detail,
    auth_session_expires_at: normalized.auth_session_expires_at,
    auth_session_ttl_seconds: normalized.auth_session_ttl_seconds,
    auth_session_active: normalized.auth_session_active,
    local_operator_identity: normalized.local_operator_identity,
    auth_session_id: normalized.auth_session_id,
    touch_id_available: normalized.touch_id_available,
    updated_at: nowIso(),
    artifacts: normalized.artifacts,
    secret_protection: normalized.secret_protection,
  };
  if (JSON.stringify(stored) !== JSON.stringify(persistedState)) {
    await writeLocalOperatorAuthState(persistedState);
  }
  return normalized;
}

function sensitiveActionReason(kind: "dashboard" | "production", action: string, payload: JsonRecord): string {
  const instrument = String(payload.instrument ?? payload.symbol ?? "").trim().toUpperCase();
  if (kind === "dashboard") {
    switch (action) {
      case "same-underlying-acknowledge":
        return `Authenticate with Touch ID to acknowledge the same-underlying conflict on ${instrument || "this instrument"}.`;
      case "same-underlying-mark-observational":
        return `Authenticate with Touch ID to mark the same-underlying conflict on ${instrument || "this instrument"} as observational-only.`;
      case "same-underlying-hold-entries":
        return `Authenticate with Touch ID to hold new entries on ${instrument || "this instrument"}.`;
      case "same-underlying-clear-hold":
        return `Authenticate with Touch ID to clear the same-underlying entry hold on ${instrument || "this instrument"}.`;
      case "same-underlying-reset-review":
        return `Authenticate with Touch ID to reset the same-underlying review state on ${instrument || "this instrument"}.`;
      default:
        return `Authenticate with Touch ID to run sensitive dashboard action ${action}.`;
    }
  }
  switch (action) {
    case "submit-order":
      return `Authenticate with Touch ID to submit the broker order for ${instrument || "the selected instrument"}.`;
    case "flatten-position":
      return `Authenticate with Touch ID to flatten the broker position for ${instrument || "the selected instrument"}.`;
    case "cancel-order":
      return `Authenticate with Touch ID to cancel the broker order for ${instrument || "the selected instrument"}.`;
    case "replace-order":
      return `Authenticate with Touch ID to replace the broker order for ${instrument || "the selected instrument"}.`;
    default:
      return `Authenticate with Touch ID to run sensitive production action ${action}.`;
  }
}

function productionActionRiskBucket(action: string, payload: JsonRecord): "INCREASE_RISK" | "REDUCE_RISK" | "OPERATOR_CONTROL" {
  const normalizedAction = String(action || "").trim().toLowerCase();
  const intentType = String(payload.intent_type ?? "").trim().toUpperCase();
  if (normalizedAction === "preview-order") {
    return intentType === "FLATTEN" ? "REDUCE_RISK" : "INCREASE_RISK";
  }
  if (normalizedAction === "submit-order") {
    return intentType === "FLATTEN" ? "REDUCE_RISK" : "INCREASE_RISK";
  }
  if (normalizedAction === "flatten-position" || normalizedAction === "cancel-order") {
    return "REDUCE_RISK";
  }
  if (normalizedAction === "replace-order") {
    if (
      intentType === "FLATTEN"
      || Boolean(payload.reduce_only)
      || Boolean(payload.replace_reduces_risk)
      || Boolean(payload.operator_reduce_only)
    ) {
      return "REDUCE_RISK";
    }
    return "INCREASE_RISK";
  }
  return "OPERATOR_CONTROL";
}

function canAuthorizeReduceOnlyProductionAction(action: string, payload: JsonRecord): boolean {
  return productionActionRiskBucket(action, payload) === "REDUCE_RISK"
    && new Set(["preview-order", "submit-order", "flatten-position", "cancel-order", "replace-order"]).has(action);
}

async function authorizeSensitiveAction(
  kind: "dashboard" | "production",
  action: string,
  payload: JsonRecord,
): Promise<
  | { ok: true; authState: LocalOperatorAuthState; payload: JsonRecord }
  | { ok: false; result: DesktopCommandResult }
> {
  const authState = await buildLocalOperatorAuthState();
  const instrument = String(payload.instrument ?? payload.symbol ?? "").trim().toUpperCase() || null;
  const riskBucket = kind === "production" ? productionActionRiskBucket(action, payload) : "OPERATOR_CONTROL";
  if (authState.auth_session_active && authState.auth_session_expires_at && authState.local_operator_identity) {
    await appendLocalOperatorAuthEvent({
      event_type: "sensitive_action_authorized",
      occurred_at: nowIso(),
      action_kind: kind,
      action,
      instrument,
      authorization_policy: "FULL_ACTIVE_SESSION",
      risk_bucket: riskBucket,
      local_operator_identity: authState.local_operator_identity,
      auth_method: authState.auth_method,
      authenticated_at: authState.last_authenticated_at,
      auth_session_id: authState.auth_session_id,
      operator_triggered: true,
      automatic: false,
      note: "Existing local auth session reused for sensitive operator action.",
    });
    return {
      ok: true,
      authState,
      payload: {
        ...payload,
        local_operator_identity: authState.local_operator_identity,
        auth_method: authState.auth_method,
        authenticated_at: authState.last_authenticated_at,
        auth_session_id: authState.auth_session_id,
        operator_authenticated: true,
        requested_operator_label:
          typeof payload.operator_label === "string" ? payload.operator_label : undefined,
        operator_label: authState.local_operator_identity,
      },
    };
  }

  if (kind === "production" && canAuthorizeReduceOnlyProductionAction(action, payload)) {
    await appendLocalOperatorAuthEvent({
      event_type: action === "preview-order" ? "sensitive_action_preview_built_without_auth" : "sensitive_action_authorized_reduce_only",
      occurred_at: nowIso(),
      action_kind: kind,
      action,
      instrument,
      authorization_policy: action === "preview-order" ? "PREVIEW_ONLY" : "REDUCE_ONLY_POLICY",
      risk_bucket: riskBucket,
      local_operator_identity: authState.local_operator_identity,
      auth_method: authState.auth_method,
      authenticated_at: authState.last_authenticated_at,
      auth_session_id: authState.auth_session_id,
      operator_triggered: true,
      automatic: false,
      note:
        action === "preview-order"
          ? "Preview built without an active local operator auth session."
          : "Reduce-only production action authorized despite inactive normal session.",
    });
    return {
      ok: true,
      authState,
      payload: {
        ...payload,
        local_operator_identity: authState.local_operator_identity,
        auth_method: authState.auth_method,
        authenticated_at: authState.last_authenticated_at,
        auth_session_id: authState.auth_session_id,
        operator_authenticated: false,
        operator_reduce_only_authorized: action === "preview-order" ? riskBucket === "REDUCE_RISK" : true,
        operator_auth_policy: action === "preview-order" ? "PREVIEW_ONLY" : "REDUCE_ONLY_POLICY",
        operator_auth_risk_bucket: riskBucket,
        requested_operator_label:
          typeof payload.operator_label === "string" ? payload.operator_label : undefined,
        operator_label:
          typeof authState.local_operator_identity === "string" && authState.local_operator_identity
            ? authState.local_operator_identity
            : undefined,
      },
    };
  }

  if (!authState.auth_available || !authState.touch_id_available) {
    const deniedAt = nowIso();
    const unavailableState = {
      ...authState,
      auth_session_active: false,
      auth_session_expires_at: null,
      auth_session_id: null,
      last_auth_result: authState.touch_id_available ? "UNAVAILABLE" : "NOT_ENROLLED",
      last_auth_detail: authState.last_auth_detail ?? "Touch ID is unavailable or not enrolled on this Mac.",
    };
    await writeLocalOperatorAuthState({
      auth_available: unavailableState.auth_available,
      auth_platform: unavailableState.auth_platform,
      auth_method: unavailableState.auth_method,
      last_authenticated_at: unavailableState.last_authenticated_at,
      last_auth_result: unavailableState.last_auth_result,
      last_auth_detail: unavailableState.last_auth_detail,
      auth_session_expires_at: unavailableState.auth_session_expires_at,
      auth_session_ttl_seconds:
        unavailableState.auth_session_ttl_seconds ?? LOCAL_OPERATOR_AUTH_SESSION_TTL_SECONDS,
      auth_session_active: unavailableState.auth_session_active,
      local_operator_identity: unavailableState.local_operator_identity,
      auth_session_id: unavailableState.auth_session_id,
      touch_id_available: unavailableState.touch_id_available,
      updated_at: deniedAt,
      artifacts: localAuthArtifacts(),
      secret_protection: unavailableState.secret_protection,
    });
    await appendLocalOperatorAuthEvent({
      event_type: "local_operator_auth_failed",
      occurred_at: deniedAt,
      action_kind: kind,
      action,
      instrument,
      authorization_policy: "DENIED_NO_ACTIVE_SESSION",
      risk_bucket: riskBucket,
      local_operator_identity: null,
      auth_method: "NONE",
      authenticated_at: null,
      auth_session_id: null,
      operator_triggered: true,
      automatic: false,
      note: unavailableState.last_auth_detail,
      auth_result: unavailableState.last_auth_result,
    });
    await appendLocalOperatorAuthEvent({
      event_type: "sensitive_action_denied_no_auth",
      occurred_at: deniedAt,
      action_kind: kind,
      action,
      instrument,
      authorization_policy: "DENIED_NO_ACTIVE_SESSION",
      risk_bucket: riskBucket,
      local_operator_identity: null,
      auth_method: "NONE",
      authenticated_at: null,
      auth_session_id: null,
      operator_triggered: true,
      automatic: false,
      note: unavailableState.last_auth_detail,
    });
    return {
      ok: false,
      result: {
        ok: false,
        message: "Local operator authentication is unavailable.",
        detail: unavailableState.last_auth_detail ?? "Touch ID is unavailable or not enrolled on this Mac.",
        state: await getDesktopState(),
      },
    };
  }

  const reason = sensitiveActionReason(kind, action, payload);
  try {
    await systemPreferences.promptTouchID(reason);
    const authenticatedAt = nowIso();
    const expiresAt = new Date(Date.now() + LOCAL_OPERATOR_AUTH_SESSION_TTL_SECONDS * 1000).toISOString();
    const sessionId = randomUUID();
    const nextStateRecord = {
      auth_available: authState.auth_available,
      auth_platform: authState.auth_platform,
      auth_method: "TOUCH_ID",
      last_authenticated_at: authenticatedAt,
      last_auth_result: "SUCCEEDED",
      last_auth_detail: reason,
      auth_session_expires_at: expiresAt,
      auth_session_ttl_seconds: LOCAL_OPERATOR_AUTH_SESSION_TTL_SECONDS,
      auth_session_active: true,
      local_operator_identity: "local_touch_id_operator",
      auth_session_id: sessionId,
      touch_id_available: authState.touch_id_available,
      updated_at: authenticatedAt,
      artifacts: localAuthArtifacts(),
      secret_protection: authState.secret_protection,
    };
    await writeLocalOperatorAuthState(nextStateRecord);
    await appendLocalOperatorAuthEvent({
      event_type: "local_operator_auth_succeeded",
      occurred_at: authenticatedAt,
      action_kind: kind,
      action,
      instrument,
      authorization_policy: "FULL_ACTIVE_SESSION",
      risk_bucket: riskBucket,
      local_operator_identity: "local_touch_id_operator",
      auth_method: "TOUCH_ID",
      authenticated_at: authenticatedAt,
      auth_session_id: sessionId,
      operator_triggered: true,
      automatic: false,
      note: reason,
      auth_result: "SUCCEEDED",
    });
    await appendLocalOperatorAuthEvent({
      event_type: "sensitive_action_authorized",
      occurred_at: authenticatedAt,
      action_kind: kind,
      action,
      instrument,
      authorization_policy: "FULL_ACTIVE_SESSION",
      risk_bucket: riskBucket,
      local_operator_identity: "local_touch_id_operator",
      auth_method: "TOUCH_ID",
      authenticated_at: authenticatedAt,
      auth_session_id: sessionId,
      operator_triggered: true,
      automatic: false,
      note: reason,
    });
    const nextState = await buildLocalOperatorAuthState();
    return {
      ok: true,
      authState: nextState,
      payload: {
        ...payload,
        local_operator_identity: "local_touch_id_operator",
        auth_method: "TOUCH_ID",
        authenticated_at: authenticatedAt,
        auth_session_id: sessionId,
        operator_authenticated: true,
        operator_reduce_only_authorized: false,
        operator_auth_policy: "FULL_ACTIVE_SESSION",
        operator_auth_risk_bucket: riskBucket,
        requested_operator_label:
          typeof payload.operator_label === "string" ? payload.operator_label : undefined,
        operator_label: "local_touch_id_operator",
      },
    };
  } catch (error) {
    const failure = classifyTouchIdFailure(error);
    const failedAt = nowIso();
    const failedStateRecord = {
      auth_available: authState.auth_available,
      auth_platform: authState.auth_platform,
      auth_method: authState.auth_method,
      last_authenticated_at: authState.last_authenticated_at,
      last_auth_result: failure.result,
      last_auth_detail: failure.detail,
      auth_session_expires_at: null,
      auth_session_ttl_seconds: authState.auth_session_ttl_seconds ?? LOCAL_OPERATOR_AUTH_SESSION_TTL_SECONDS,
      auth_session_active: false,
      local_operator_identity: authState.local_operator_identity,
      auth_session_id: null,
      touch_id_available: authState.touch_id_available,
      updated_at: failedAt,
      artifacts: localAuthArtifacts(),
      secret_protection: authState.secret_protection,
    };
    await writeLocalOperatorAuthState(failedStateRecord);
    await appendLocalOperatorAuthEvent({
      event_type: failure.result === "CANCELED" ? "local_operator_auth_canceled" : "local_operator_auth_failed",
      occurred_at: failedAt,
      action_kind: kind,
      action,
      instrument,
      authorization_policy: "DENIED_NO_ACTIVE_SESSION",
      risk_bucket: riskBucket,
      local_operator_identity: authState.local_operator_identity,
      auth_method: authState.auth_method,
      authenticated_at: authState.last_authenticated_at,
      auth_session_id: null,
      operator_triggered: true,
      automatic: false,
      note: failure.detail,
      auth_result: failure.result,
    });
    await appendLocalOperatorAuthEvent({
      event_type: "sensitive_action_denied_no_auth",
      occurred_at: failedAt,
      action_kind: kind,
      action,
      instrument,
      authorization_policy: "DENIED_NO_ACTIVE_SESSION",
      risk_bucket: riskBucket,
      local_operator_identity: authState.local_operator_identity,
      auth_method: authState.auth_method,
      authenticated_at: authState.last_authenticated_at,
      auth_session_id: null,
      operator_triggered: true,
      automatic: false,
      note: failure.detail,
    });
    return {
      ok: false,
      result: {
        ok: false,
        message:
          failure.result === "CANCELED"
            ? "Sensitive action canceled because local operator authentication was canceled."
            : "Sensitive action denied because local operator authentication failed.",
        detail: failure.detail,
        state: await getDesktopState(),
      },
    };
  }
}

export function appendDesktopLog(line: string): void {
  mkdirSync(DESKTOP_APP_STATE_ROOT, { recursive: true });
  appendFileSync(DESKTOP_LOG_FILE, `[${new Date().toISOString()}] ${line}\n`, "utf8");
}

function setManagerError(detail: string | null): void {
  managerLastError = detail;
  if (detail) {
    appendDesktopLog(detail);
  }
}

function summarizeErrorText(detail: string | null): string | null {
  if (!detail) {
    return null;
  }
  const lines = detail
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  return lines.length ? lines[lines.length - 1] : null;
}

function startupFailureLabelForCommand(kind: StartupFailureKind): string {
  switch (kind) {
    case "stale_dashboard_instance":
      return "stale dashboard instance";
    case "stale_listener_conflict":
      return "stale listener or port conflict";
    case "build_mismatch":
      return "build mismatch";
    case "dashboard_api_not_ready":
      return "dashboard API not ready";
    case "early_process_exit":
      return "early process exit";
    case "permission_or_bind_failure":
      return "permission or bind failure";
    case "environment_failure":
      return "environment failure";
    case "unexpected_startup_failure":
      return "unexpected startup failure";
    default:
      return "startup failure";
  }
}

async function readLogTail(filePath: string, maxLines = 40): Promise<string> {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return raw
      .split(/\r?\n/)
      .filter(Boolean)
      .slice(-maxLines)
      .join("\n");
  } catch {
    return "";
  }
}

function clearReconnectTimer(): void {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  nextRetryAt = null;
}

function serviceHostBootstrapActive(): boolean {
  return serviceHostBootstrapPromise !== null;
}

function appendManagerOutput(chunk: string): void {
  const lines = chunk
    .split(/\r?\n/)
    .map((line) => line.trimEnd())
    .filter(Boolean);
  if (!lines.length) {
    return;
  }
  for (const line of lines) {
    appendDesktopLog(`[manager] ${line}`);
  }
  recentManagerOutput = [...recentManagerOutput, ...lines].slice(-120);
}

function managerOutputText(): string {
  return recentManagerOutput.join("\n").trim();
}

function launcherFailureDetailFromManagerOutput(code: number | null, signal: string | null): string {
  const output = managerOutputText();
  const assessment = classifyStartupFailure(output);
  if (assessment.kind !== "none") {
    return output;
  }
  return `STARTUP_FAILURE_KIND=early_process_exit\nManaged dashboard process exited unexpectedly${code !== null ? ` with code ${code}` : ""}${signal ? ` (${signal})` : ""}.`;
}

function shouldContinueWaitingForRecovery(state: DesktopState): boolean {
  if (state.connection === "live") {
    return false;
  }
  if (!state.backend.managerOwned) {
    return false;
  }
  if (state.backend.state === "starting" || state.backend.state === "reconnecting") {
    return true;
  }
  if (!managerLastError) {
    return false;
  }
  const assessment = classifyStartupFailure(managerLastError, {
    healthReachable: state.backend.healthReachable,
    dashboardApiTimedOut: state.backend.dashboardApiTimedOut,
  });
  if (assessment.kind === "stale_listener_conflict" || assessment.kind === "stale_dashboard_instance") {
    return Boolean(reconnectTimer || state.backend.nextRetryAt);
  }
  return false;
}

function trackDashboardManager(child: ChildProcess): void {
  dashboardManager = child;
  recentManagerOutput = [];
  lastExitCode = null;
  lastExitSignal = null;
  child.stdout?.on("data", (chunk) => appendManagerOutput(String(chunk)));
  child.stderr?.on("data", (chunk) => appendManagerOutput(String(chunk)));
  child.on("close", (code, signal) => {
    if (dashboardManager === child) {
      dashboardManager = null;
    }
    lastExitCode = typeof code === "number" ? code : null;
    lastExitSignal = signal;
    if (!stopWasRequested && managerOwnsBackend && !shutdownRequested) {
      const exitDetail = launcherFailureDetailFromManagerOutput(code, signal);
      const assessment = classifyStartupFailure(exitDetail);
      setManagerError(summarizeErrorText(exitDetail) ?? exitDetail);
      if (shouldAutoReconnectDashboardFailure(assessment, reconnectAttemptCount)) {
        managerLifecycle = "reconnecting";
        scheduleReconnect();
      } else {
        clearReconnectTimer();
        managerLifecycle = "degraded";
      }
      return;
    }
    managerLifecycle = "degraded";
  });
  child.on("error", (error) => {
    if (dashboardManager === child) {
      dashboardManager = null;
    }
    const errorDetail = `STARTUP_FAILURE_KIND=unexpected_startup_failure\nManaged dashboard process error: ${error.message}`;
    const assessment = classifyStartupFailure(errorDetail);
    setManagerError(`Managed dashboard process error: ${error.message}`);
    if (!stopWasRequested && managerOwnsBackend && !shutdownRequested && shouldAutoReconnectDashboardFailure(assessment, reconnectAttemptCount)) {
      managerLifecycle = "reconnecting";
      scheduleReconnect();
      return;
    }
    clearReconnectTimer();
    managerLifecycle = "degraded";
  });
}

function scheduleReconnect(): void {
  if (reconnectTimer || shutdownRequested || !managerOwnsBackend) {
    return;
  }
  const attemptIndex = Math.min(reconnectAttemptCount, RECONNECT_BACKOFF_MS.length - 1);
  const delayMs = RECONNECT_BACKOFF_MS[attemptIndex];
  reconnectAttemptCount += 1;
  nextRetryAt = new Date(Date.now() + delayMs).toISOString();
  appendDesktopLog(`Scheduling dashboard reconnect attempt ${reconnectAttemptCount} in ${delayMs}ms.`);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    void beginDashboardLaunch({ manual: false });
  }, delayMs);
}

function dashboardApiBlockedByAuth(detail: string | null | undefined): boolean {
  const normalized = String(detail ?? "").toLowerCase();
  if (!normalized.trim()) {
    return false;
  }
  return (
    normalized.includes("refresh_token_authentication_error") ||
    normalized.includes("unsupported_token_type") ||
    normalized.includes("exception while authenticating refresh token") ||
    normalized.includes("failed refresh token authentication") ||
    normalized.includes("token expired") ||
    normalized.includes("schwab auth")
  );
}

function authGateRecoveryScheduled(): boolean {
  return authGateRecoveryPromise !== null || authGateRecoveryNextRetryAt !== null;
}

function clearAuthGateRecoveryState(): void {
  authGateRecoveryPromise = null;
  authGateRecoveryNextRetryAt = null;
  authGateRecoveryAttemptCount = 0;
}

function parseAuthGateReady(result: { ok: boolean; stdout: string; stderr: string }): boolean {
  const stdout = String(result.stdout ?? "").trim();
  if (stdout) {
    try {
      const payload = JSON.parse(stdout) as JsonRecord;
      if (payload.runtime_ready === true || payload.ready === true || payload.refresh_succeeds === true) {
        return true;
      }
      if (payload.runtime_ready === false || payload.ready === false || payload.refresh_succeeds === false) {
        return false;
      }
    } catch {
      // Fall through to text heuristics when the script emits plain text.
    }
  }
  const combined = `${result.stdout}\n${result.stderr}`.toLowerCase();
  return result.ok && combined.includes("runtime_ready") && combined.includes("true");
}

function authGateRecoveryBackoffMs(): number {
  const index = Math.min(authGateRecoveryAttemptCount, DASHBOARD_AUTH_RECOVERY_BACKOFF_MS.length - 1);
  return DASHBOARD_AUTH_RECOVERY_BACKOFF_MS[index];
}

function scheduleAuthGateRecovery(detail: string | null | undefined): void {
  if (autoBootstrapBlockedBySandbox()) {
    return;
  }
  if (shutdownRequested || authGateRecoveryPromise) {
    return;
  }
  if (authGateRecoveryAttemptCount >= DASHBOARD_AUTH_RECOVERY_BACKOFF_MS.length) {
    return;
  }
  if (authGateRecoveryNextRetryAt) {
    const nextRetryMs = Date.parse(authGateRecoveryNextRetryAt);
    if (!Number.isNaN(nextRetryMs) && nextRetryMs > Date.now()) {
      return;
    }
  }

  const delayMs = authGateRecoveryBackoffMs();
  authGateRecoveryAttemptCount += 1;
  authGateRecoveryNextRetryAt = new Date(Date.now() + delayMs).toISOString();
  managerLifecycle = "reconnecting";
  setManagerError((summarizeErrorText(String(detail ?? "")) ?? String(detail ?? "").trim()) || managerLastError);

  authGateRecoveryPromise = (async () => {
    if (delayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
    authGateRecoveryNextRetryAt = null;
    appendDesktopLog("[electron] auth-gate recovery:begin");
    const result = await execScript(["scripts/run_schwab_auth_gate.sh"]);
    const recoveryDetail = [result.stdout, result.stderr].filter(Boolean).join("\n").trim() || "Auth gate recovery failed.";
    appendDesktopLog(`[electron] auth-gate recovery:${result.ok ? "completed" : "failed"} ${recoveryDetail}`);
    if (parseAuthGateReady(result)) {
      clearAuthGateRecoveryState();
      setManagerError(null);
      return;
    }
    authGateRecoveryNextRetryAt = authGateRecoveryAttemptCount < DASHBOARD_AUTH_RECOVERY_BACKOFF_MS.length
      ? new Date(Date.now() + authGateRecoveryBackoffMs()).toISOString()
      : null;
    setManagerError(summarizeErrorText(recoveryDetail) ?? recoveryDetail);
  })().finally(() => {
    authGateRecoveryPromise = null;
    if (!reconnectTimer && !dashboardLaunchPromise && managerLifecycle === "reconnecting" && !authGateRecoveryScheduled()) {
      managerLifecycle = "degraded";
    }
  });
}

async function readJsonFile<T = JsonRecord>(filePath: string): Promise<T | null> {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return JSON.parse(raw) as T;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const shouldRetry =
      /\bdashboard_api_snapshot\.json$/.test(filePath)
      || /\boperator_surface_snapshot\.json$/.test(filePath)
      || /\bstartup_control_plane_snapshot\.json$/.test(filePath);
    if (shouldRetry) {
      appendDesktopLog(`Snapshot read failed for ${filePath}: ${message}`);
      try {
        await new Promise((resolve) => setTimeout(resolve, 75));
        const retryRaw = await fs.readFile(filePath, "utf8");
        return JSON.parse(retryRaw) as T;
      } catch (retryError) {
        const retryMessage = retryError instanceof Error ? retryError.message : String(retryError);
        appendDesktopLog(`Snapshot reread failed for ${filePath}: ${retryMessage}`);
      }
    }
    return null;
  }
}

function looksLikeDashboardSnapshot(payload: JsonRecord | null | undefined): payload is JsonRecord {
  if (!payload || typeof payload !== "object") {
    return false;
  }
  return (
    typeof payload.generated_at === "string"
    || Boolean(payload.operator_surface)
    || Boolean(payload.paper)
    || Boolean(payload.lane_registry)
  );
}

function compactPlaybackStudyCatalogItem(item: JsonRecord): JsonRecord {
  const summary = asJsonRecord(item.summary);
  const calendarBreakdown = Array.isArray(summary.calendar_breakdown)
    ? summary.calendar_breakdown.filter((entry): entry is JsonRecord => Boolean(entry) && typeof entry === "object")
    : [];
  return {
    study_key: item.study_key ?? null,
    label: item.label ?? null,
    run_stamp: item.run_stamp ?? null,
    run_timestamp: item.run_timestamp ?? null,
    symbol: item.symbol ?? null,
    strategy_id: item.strategy_id ?? null,
    candidate_id: item.candidate_id ?? null,
    scope_label: item.scope_label ?? null,
    strategy_family: item.strategy_family ?? null,
    context_resolution: item.context_resolution ?? null,
    execution_resolution: item.execution_resolution ?? null,
    coverage_start: item.coverage_start ?? null,
    coverage_end: item.coverage_end ?? null,
    study_mode: item.study_mode ?? null,
    entry_model: item.entry_model ?? null,
    closed_trade_count: item.closed_trade_count ?? summary.closed_trade_count ?? 0,
    summary: {
      closed_trade_count: summary.closed_trade_count ?? item.closed_trade_count ?? 0,
      calendar_breakdown: calendarBreakdown,
    },
    compacted_for_startup: true,
  };
}

function compactDashboardForDesktopTransfer(dashboard: JsonRecord | null | undefined): JsonRecord | null {
  if (!looksLikeDashboardSnapshot(dashboard)) {
    return null;
  }

  const strategyAnalysis = asJsonRecord(dashboard.strategy_analysis);
  const researchAnalytics = asJsonRecord(strategyAnalysis.research_analytics);
  const compactStrategyAnalysis = Object.keys(strategyAnalysis).length
    ? {
        ...strategyAnalysis,
        results_board: {
          generated_at: strategyAnalysis.generated_at ?? dashboard.generated_at ?? null,
          compacted_for_startup: true,
          row_count: Number.isFinite(Number(asJsonRecord(strategyAnalysis.results_board).row_count))
            ? Number(asJsonRecord(strategyAnalysis.results_board).row_count)
            : 0,
          rows: [],
        },
        details_by_strategy_key: {},
        research_analytics: researchAnalytics,
      }
    : strategyAnalysis;

  const historicalPlayback = asJsonRecord(dashboard.historical_playback);
  const studyCatalog = asJsonRecord(historicalPlayback.study_catalog);
  const studyCatalogItems = Array.isArray(studyCatalog.items)
    ? studyCatalog.items.filter((item): item is JsonRecord => Boolean(item) && typeof item === "object")
    : [];
  const compactHistoricalPlayback = Object.keys(historicalPlayback).length
    ? {
        ...historicalPlayback,
        study_catalog: {
          ...studyCatalog,
          compacted_for_startup: true,
          items: studyCatalogItems.map(compactPlaybackStudyCatalogItem),
        },
      }
    : historicalPlayback;

  const paper = asJsonRecord(dashboard.paper);
  const alertsState = asJsonRecord(paper.alerts_state);
  const compactPaper = Object.keys(paper).length
    ? {
        ...paper,
        alerts_state: {
          ...alertsState,
          compacted_for_startup: true,
          active_alerts: [],
          rows: [],
          recent_events: [],
        },
      }
    : paper;

  return {
    ...dashboard,
    strategy_analysis: compactStrategyAnalysis,
    historical_playback: compactHistoricalPlayback,
    paper: compactPaper,
    desktop_compacted_for_startup: true,
  };
}

async function persistDesktopDashboardCache(dashboard: JsonRecord | null | undefined): Promise<void> {
  if (!looksLikeDashboardSnapshot(dashboard)) {
    return;
  }
  try {
    await writeJsonFileAtomic(DESKTOP_LOCAL_DASHBOARD_CACHE_FILE, compactDashboardForDesktopTransfer(dashboard));
  } catch (error) {
    appendDesktopLog(`Failed to persist desktop dashboard cache: ${String(error)}`);
  }
}

async function applyDesktopStateFixtureOverride(state: DesktopState): Promise<DesktopState> {
  if (!DESKTOP_STATE_FIXTURE_PATH) {
    return state;
  }
  const override = await readJsonFile<JsonRecord>(DESKTOP_STATE_FIXTURE_PATH);
  if (!override) {
    return state;
  }
  const merged = deepMergeJson(state as unknown as JsonRecord, override) as DesktopState;
  return {
    ...merged,
    refreshedAt: nowIso(),
  };
}

async function loadDesktopStateFixtureState(): Promise<DesktopState> {
  const override = await readJsonFile<JsonRecord>(DESKTOP_STATE_FIXTURE_PATH);
  const localAuth = await buildLocalOperatorAuthState();
  const base: DesktopState = {
    connection: "unavailable",
    dashboard: null,
    health: null,
    backendUrl: null,
    source: {
      mode: "backend_down",
      label: "BACKEND DOWN",
      detail: "Desktop state fixture fallback base.",
      canRunLiveActions: false,
      healthReachable: false,
      apiReachable: false,
    },
    backend: {
      state: "backend_down",
      label: "BACKEND DOWN",
      detail: "Desktop state fixture fallback base.",
      lastError: null,
      nextRetryAt: null,
      retryCount: 0,
      pid: null,
      apiStatus: "unknown",
      healthStatus: "unknown",
      managerOwned: false,
      startupFailureKind: "none",
      actionHint: null,
      staleListenerDetected: false,
      healthReachable: false,
      dashboardApiTimedOut: false,
      portConflictDetected: false,
    },
    startup: {
      preferredHost: DEFAULT_DASHBOARD_HOST,
      preferredPort: DEFAULT_DASHBOARD_PORT,
      preferredUrl: DEFAULT_DASHBOARD_URL,
      allowPortFallback: ALLOW_PORT_FALLBACK,
      chosenHost: null,
      chosenPort: null,
      chosenUrl: null,
      mode: "UNAVAILABLE",
      ownership: "unavailable",
      latestEvent: null,
      recentEvents: [],
      failureKind: "none",
      recommendedAction: null,
      staleListenerDetected: false,
      healthReachable: false,
      dashboardApiTimedOut: false,
      managedExitCode: null,
      managedExitSignal: null,
    },
    infoFiles: [],
    errors: [],
    runtimeLogPath: DEFAULT_LOG_FILE,
    backendLogPath: DEFAULT_LOG_FILE,
    desktopLogPath: DESKTOP_LOG_FILE,
    appVersion: String(packageJson.version ?? "0.0.0"),
    manager: {
      running: false,
      lastExitCode,
      lastExitSignal,
      recentOutput: recentManagerOutput,
    },
    localAuth,
    refreshedAt: nowIso(),
  };
  if (!override) {
    return base;
  }
  const merged = deepMergeJson(base as unknown as JsonRecord, override) as DesktopState;
  return {
    ...merged,
    appVersion: String(packageJson.version ?? "0.0.0"),
    refreshedAt: nowIso(),
  };
}

async function readActionLog(limit = 40): Promise<JsonRecord[]> {
  try {
    const raw = await fs.readFile(SNAPSHOT_FILES.actionLog, "utf8");
    const rows = raw
      .split(/\r?\n/)
      .filter(Boolean)
      .slice(-limit)
      .map((line) => JSON.parse(line) as JsonRecord);
    return rows.reverse();
  } catch {
    return [];
  }
}

async function runtimeInfoFiles(): Promise<string[]> {
  if (packagedLocalBundleLaunchContext()) {
    return [];
  }
  try {
    const entries = await fs.readdir(RUNTIME_ROOT);
    const candidatePaths = entries
      .filter((entry) => entry.endsWith(".json"))
      .map((entry) => path.join(RUNTIME_ROOT, entry));
    const stats = await Promise.all(
      candidatePaths.map(async (candidate) => ({
        candidate,
        stat: await fs.stat(candidate),
      })),
    );
    return stats.sort((left, right) => right.stat.mtimeMs - left.stat.mtimeMs).map((entry) => entry.candidate);
  } catch {
    return [];
  }
}

async function candidateUrls(): Promise<{ urls: string[]; infoFiles: string[] }> {
  const infoFiles = await runtimeInfoFiles();
  const urls: string[] = [];
  const envUrl = process.env.MGC_OPERATOR_DASHBOARD_URL;
  if (envUrl) {
    urls.push(envUrl);
  }
  urls.push(DEFAULT_DASHBOARD_URL);
  urls.push(...EXPLICIT_DASHBOARD_URLS);
  if (!packagedLocalBundleLaunchContext()) {
    for (const infoFile of [DEFAULT_INFO_FILE, ...infoFiles]) {
      const payload = await readJsonFile<{ url?: string }>(infoFile);
      if (payload?.url) {
        urls.push(payload.url);
      }
    }
  }
  return {
    urls: Array.from(new Set(urls.map((url) => normalizeBaseUrl(url)))),
    infoFiles: packagedLocalBundleLaunchContext() ? [] : Array.from(new Set([DEFAULT_INFO_FILE, ...infoFiles])),
  };
}

function buildStartupState({
  dashboard,
  health,
  backendUrl,
  assessment,
  healthReachable,
  dashboardApiTimedOut,
}: {
  dashboard: JsonRecord | null;
  health: JsonRecord | null;
  backendUrl: string | null;
  assessment: StartupFailureAssessment;
  healthReachable: boolean;
  dashboardApiTimedOut: boolean;
}): DesktopState["startup"] {
  const meta = asJsonRecord((dashboard?.dashboard_meta as JsonRecord | undefined) ?? null);
  const metaUrl = typeof meta.server_url === "string" && meta.server_url.trim() ? meta.server_url : null;
  const chosenUrl = backendUrl ?? metaUrl;
  const chosenHost =
    parseHost(meta.server_host) ?? safeUrlParts(metaUrl).host ?? safeUrlParts(backendUrl).host;
  const chosenPort =
    parsePort(meta.server_port) ?? parsePort(health?.port) ?? safeUrlParts(metaUrl).port ?? safeUrlParts(backendUrl).port;
  const ownership: DesktopState["startup"]["ownership"] =
    backendUrl && managerOwnsBackend
      ? "started_managed"
      : backendUrl
        ? "attached_existing"
        : dashboard
          ? "snapshot_only"
          : "unavailable";
  const mode: DesktopState["startup"]["mode"] =
    ownership === "started_managed"
      ? "DESKTOP_MANAGED_DIAGNOSTIC"
      : ownership === "attached_existing"
        ? "SERVICE_ATTACHED"
        : ownership === "snapshot_only"
          ? "SNAPSHOT_ONLY"
          : "UNAVAILABLE";
  return {
    preferredHost: DEFAULT_DASHBOARD_HOST,
    preferredPort: DEFAULT_DASHBOARD_PORT,
    preferredUrl: DEFAULT_DASHBOARD_URL,
    allowPortFallback: ALLOW_PORT_FALLBACK,
    chosenHost,
    chosenPort,
    chosenUrl,
    mode,
    ownership,
    latestEvent: recentManagerOutput.length ? recentManagerOutput[recentManagerOutput.length - 1] : null,
    recentEvents: recentManagerOutput.slice(-12),
    failureKind: assessment.kind,
    recommendedAction: assessment.hint,
    staleListenerDetected: assessment.staleListenerDetected,
    healthReachable,
    dashboardApiTimedOut,
    managedExitCode: lastExitCode,
    managedExitSignal: lastExitSignal,
  };
}

async function writeDesktopStartupStatus(state: DesktopState): Promise<void> {
  try {
    await fs.mkdir(path.dirname(DESKTOP_STARTUP_STATUS_FILE), { recursive: true });
    await fs.writeFile(
      DESKTOP_STARTUP_STATUS_FILE,
      JSON.stringify(
        {
          refreshed_at: state.refreshedAt,
          source: state.source,
          backend: state.backend,
          startup: state.startup,
          manager: state.manager,
          backendUrl: state.backendUrl,
          infoFiles: state.infoFiles,
        },
        null,
        2,
      ) + "\n",
      "utf8",
    );
  } catch (error) {
    appendDesktopLog(`Failed to write desktop startup status artifact: ${String(error)}`);
  }
}

async function fetchJson<T = JsonRecord>(url: string, timeoutMs = 3500): Promise<T> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    let response: Response;
    try {
      response = await fetch(url, {
        signal: controller.signal,
        headers: { Accept: "application/json" },
      });
    } catch (error) {
      if (isLocalLoopbackUrl(url)) {
        const transportDetail = describeLocalApiTransportError(url, error);
        appendDesktopLog(`[electron] local API fetch transport failed; trying curl fallback: ${transportDetail}`);
        try {
          return await fetchJsonViaCurl<T>(url, timeoutMs);
        } catch (curlError) {
          const curlDetail = describeLocalApiTransportError(url, curlError);
          appendDesktopLog(`[electron] curl fallback failed for local API: ${curlDetail}`);
          throw new Error(`${transportDetail}; curl fallback failed: ${curlDetail}`);
        }
      }
      if (controller.signal.aborted || (error instanceof Error && error.name === "AbortError")) {
        throw new Error(`Timed out after ${Math.round(timeoutMs / 1000)}s`);
      }
      throw error;
    }
    if (!response.ok) {
      const responseText = await response.text();
      let detail = responseText.trim();
      try {
        const payload = JSON.parse(responseText) as JsonRecord;
        detail = String(payload.message ?? payload.error ?? responseText).trim();
      } catch {
        // Keep raw text when the response is not JSON.
      }
      throw new Error(detail ? `HTTP ${response.status} ${response.statusText}: ${detail}` : `HTTP ${response.status} ${response.statusText}`);
    }
    return (await response.json()) as T;
  } finally {
    clearTimeout(timeout);
  }
}

function isLocalLoopbackUrl(value: string): boolean {
  try {
    const parsed = new URL(value);
    return parsed.hostname === "127.0.0.1" || parsed.hostname === "localhost";
  } catch {
    return false;
  }
}

async function fetchJsonViaCurl<T = JsonRecord>(url: string, timeoutMs: number): Promise<T> {
  if (testCurlJsonHook) {
    return (await testCurlJsonHook(url, timeoutMs)) as T;
  }
  const curlWorkingDirectory = packagedLocalBundleLaunchContext() ? DESKTOP_APP_STATE_ROOT : REPO_ROOT;
  return await new Promise<T>((resolve, reject) => {
    const timeoutSeconds = Math.max(1, Math.ceil(timeoutMs / 1000));
    const child = spawn(
      "curl",
      ["-sS", "--max-time", String(timeoutSeconds), "-H", "Accept: application/json", url],
      {
        cwd: curlWorkingDirectory,
        env: process.env,
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    let stdout = "";
    let stderr = "";
    let timedOut = false;
    const killTimer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, timeoutMs + 250);
    child.stdout.on("data", (chunk) => {
      stdout += String(chunk);
    });
    child.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });
    child.on("error", (error) => {
      clearTimeout(killTimer);
      reject(error);
    });
    child.on("close", (code, signal) => {
      clearTimeout(killTimer);
      if (timedOut) {
        reject(new Error(`Timed out after ${Math.round(timeoutMs / 1000)}s`));
        return;
      }
      if ((code ?? 1) !== 0) {
        const detail = [stderr.trim(), stdout.trim()].filter(Boolean).join(" | ")
          || `curl exited with code ${code ?? "unknown"}${signal ? ` (${signal})` : ""}`;
        reject(new Error(detail));
        return;
      }
      try {
        resolve(JSON.parse(stdout) as T);
      } catch (error) {
        const detail = stdout.trim().slice(0, 500);
        reject(
          new Error(
            `curl returned non-JSON payload${detail ? `: ${detail}` : ""}${error instanceof Error && error.message ? ` (${error.message})` : ""}`,
          ),
        );
      }
    });
  });
}

interface LocalApiResponse {
  ok: boolean;
  status: number;
  statusText: string;
  bodyText: string;
}

interface LoadLiveDashboardOptions {
  healthTimeoutMs?: number;
  dashboardTimeoutMs?: number;
}

type LoadLiveDashboardResult =
  | { mode: "live"; url: string; health: JsonRecord; dashboard: JsonRecord }
  | { mode: "health-only"; url: string; health: JsonRecord; error: string }
  | null;

function describeLocalApiTransportError(url: string, error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  const maybeCause = error instanceof Error ? (error as Error & { cause?: unknown }).cause : undefined;
  const cause = typeof maybeCause === "object" && maybeCause ? maybeCause as Record<string, unknown> : null;
  const fragments = [
    cause?.code,
    cause?.errno,
    cause?.syscall,
    cause?.address,
    cause?.port,
  ]
    .filter((value) => value !== undefined && value !== null && String(value).trim())
    .map((value) => String(value).trim());
  const detail = fragments.length ? ` [${fragments.join(" / ")}]` : "";
  return `${message}${detail} @ ${url}`;
}

async function postLocalJson(url: string, payload: JsonRecord, timeoutMs = DASHBOARD_TIMEOUT_MS): Promise<LocalApiResponse> {
  const body = JSON.stringify(payload ?? {});
  if (testFetchHook) {
    const response = await testFetchHook(url, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body,
    });
    return {
      ok: response.ok,
      status: response.status,
      statusText: response.statusText,
      bodyText: await response.text(),
    };
  }
  return await new Promise<LocalApiResponse>((resolve, reject) => {
    const parsedUrl = new URL(url);
    const request = (parsedUrl.protocol === "https:" ? httpsRequest : httpRequest)(
      parsedUrl,
      {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(body),
        },
      },
      (response) => {
        let responseBody = "";
        response.setEncoding("utf8");
        response.on("data", (chunk) => {
          responseBody += chunk;
        });
        response.on("end", () => {
          resolve({
            ok: (response.statusCode ?? 0) >= 200 && (response.statusCode ?? 0) < 300,
            status: response.statusCode ?? 0,
            statusText: response.statusMessage ?? "",
            bodyText: responseBody,
          });
        });
      },
    );
    request.setTimeout(timeoutMs, () => {
      request.destroy(new Error(`Timed out after ${Math.round(timeoutMs / 1000)}s`));
    });
    request.on("error", (error) => reject(error));
    request.write(body);
    request.end();
  });
}

function parseActionPayload(bodyText: string): JsonRecord {
  if (!bodyText.trim()) {
    return {};
  }
  return JSON.parse(bodyText) as JsonRecord;
}

function normalizeActionResult(
  action: string,
  response: LocalApiResponse,
  payloadRecord: JsonRecord,
): Pick<DesktopCommandResult, "ok" | "message" | "detail" | "output" | "payload"> {
  const actionLabel = String(payloadRecord.action_label ?? payloadRecord.action ?? action);
  const normalizedMessage = String(payloadRecord.message ?? "").trim();
  const normalizedError = String(payloadRecord.error ?? "").trim();
  const normalizedOutput = String(payloadRecord.output ?? "").trim();
  const normalizedDetail = String(
    payloadRecord.detail
    ?? (
      payloadRecord.reason_code
        ? `${String(payloadRecord.reason_code)}${payloadRecord.next_action ? ` | Next action: ${String(payloadRecord.next_action)}` : ""}`
        : ""
    ),
  ).trim();
  const primaryMessage = !response.ok || payloadRecord.ok === false
    ? (normalizedMessage || normalizedError || actionLabel)
    : (normalizedMessage || actionLabel);
  return {
    ok: response.ok && Boolean(payloadRecord.ok ?? true),
    message: primaryMessage,
    detail: normalizedDetail || normalizedError || normalizedOutput || undefined,
    output: normalizedOutput || normalizedMessage || normalizedError || undefined,
    payload: payloadRecord,
  };
}

async function loadLiveDashboard(
  urls: string[],
  options: LoadLiveDashboardOptions = {},
): Promise<LoadLiveDashboardResult> {
  if (testLoadLiveDashboardHook) {
    return testLoadLiveDashboardHook(urls, options);
  }
  const healthTimeoutMs = options.healthTimeoutMs ?? HEALTH_TIMEOUT_MS;
  const dashboardTimeoutMs = options.dashboardTimeoutMs ?? DASHBOARD_TIMEOUT_MS;
  let healthOnlyFallback: { mode: "health-only"; url: string; health: JsonRecord; error: string } | null = null;
  for (const baseUrl of urls) {
    try {
      const health = await fetchJson<JsonRecord>(new URL("health", baseUrl).toString(), healthTimeoutMs);
      try {
        const dashboard = await fetchJson<JsonRecord>(new URL("api/dashboard", baseUrl).toString(), dashboardTimeoutMs);
        return { mode: "live", url: baseUrl, health, dashboard };
      } catch (error) {
        healthOnlyFallback = {
          mode: "health-only",
          url: baseUrl,
          health,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    } catch {
      continue;
    }
  }
  return healthOnlyFallback;
}

async function promiseWithTimeout<T>(promise: Promise<T>, timeoutMs: number, fallbackValue: T): Promise<T> {
  let timeoutHandle: NodeJS.Timeout | null = null;
  try {
    return await Promise.race<T>([
      promise,
      new Promise<T>((resolve) => {
        timeoutHandle = setTimeout(() => resolve(fallbackValue), timeoutMs);
      }),
    ]);
  } finally {
    if (timeoutHandle) {
      clearTimeout(timeoutHandle);
    }
  }
}

async function loadSnapshotBundle(
  options: { includeHeavyPayload?: boolean; preferDesktopCache?: boolean } = {},
): Promise<JsonRecord | null> {
  if (testLoadSnapshotBundleHook) {
    return testLoadSnapshotBundleHook();
  }
  const includeHeavyPayload = options.includeHeavyPayload !== false;
  const packagedLocalLaunch = packagedLocalBundleLaunchContext();
  const candidatePaths = packagedLocalLaunch
    ? [DESKTOP_LOCAL_DASHBOARD_CACHE_FILE]
    : options.preferDesktopCache
      ? [DESKTOP_LOCAL_DASHBOARD_CACHE_FILE, SNAPSHOT_FILES.dashboardApi]
      : [SNAPSHOT_FILES.dashboardApi, DESKTOP_LOCAL_DASHBOARD_CACHE_FILE];
  for (const candidatePath of candidatePaths) {
    const dashboardApiSnapshot = await readJsonFile<JsonRecord>(candidatePath);
    if (looksLikeDashboardSnapshot(dashboardApiSnapshot)) {
      if (candidatePath !== DESKTOP_LOCAL_DASHBOARD_CACHE_FILE) {
        void persistDesktopDashboardCache(dashboardApiSnapshot);
      }
      return includeHeavyPayload ? dashboardApiSnapshot : compactDashboardForDesktopTransfer(dashboardApiSnapshot);
    }
  }
  const [
    historicalPlayback,
    researchRuntimeBridge,
    strategyAnalysis,
    marketIndexStrip,
    operatorSurface,
    paperApprovedModels,
    paperBlotter,
    paperCarryForward,
    paperFills,
    paperIntents,
    paperLaneActivity,
    paperNonApprovedLanes,
    paperPerformance,
    paperPosition,
    paperReadiness,
    startupControlPlane,
    treasuryCurve,
    productionLink,
    actionLog,
  ] = await Promise.all([
    packagedLocalLaunch ? Promise.resolve(null) : readJsonFile(SNAPSHOT_FILES.historicalPlayback),
    packagedLocalLaunch ? Promise.resolve(null) : readJsonFile(SNAPSHOT_FILES.researchRuntimeBridge),
    packagedLocalLaunch ? Promise.resolve(null) : readJsonFile(SNAPSHOT_FILES.strategyAnalysis),
    packagedLocalLaunch ? Promise.resolve(null) : readJsonFile(SNAPSHOT_FILES.marketIndexStrip),
    packagedLocalLaunch ? Promise.resolve(null) : readJsonFile(SNAPSHOT_FILES.operatorSurface),
    packagedLocalLaunch ? Promise.resolve(null) : readJsonFile(SNAPSHOT_FILES.paperApprovedModels),
    packagedLocalLaunch ? Promise.resolve(null) : readJsonFile(SNAPSHOT_FILES.paperBlotter),
    packagedLocalLaunch ? Promise.resolve(null) : readJsonFile(SNAPSHOT_FILES.paperCarryForward),
    packagedLocalLaunch ? Promise.resolve(null) : readJsonFile(SNAPSHOT_FILES.paperFills),
    packagedLocalLaunch ? Promise.resolve(null) : readJsonFile(SNAPSHOT_FILES.paperIntents),
    packagedLocalLaunch ? Promise.resolve(null) : readJsonFile(SNAPSHOT_FILES.paperLaneActivity),
    packagedLocalLaunch ? Promise.resolve(null) : readJsonFile(SNAPSHOT_FILES.paperNonApprovedLanes),
    packagedLocalLaunch ? Promise.resolve(null) : readJsonFile(SNAPSHOT_FILES.paperPerformance),
    packagedLocalLaunch ? Promise.resolve(null) : readJsonFile(SNAPSHOT_FILES.paperPosition),
    packagedLocalLaunch ? Promise.resolve(null) : readJsonFile(SNAPSHOT_FILES.paperReadiness),
    packagedLocalLaunch ? Promise.resolve(null) : readJsonFile(SNAPSHOT_FILES.startupControlPlane),
    packagedLocalLaunch ? Promise.resolve(null) : readJsonFile(SNAPSHOT_FILES.treasuryCurve),
    packagedLocalLaunch ? Promise.resolve(null) : readJsonFile(SNAPSHOT_FILES.productionLink),
    packagedLocalLaunch ? Promise.resolve([]) : readActionLog(),
  ]);

  if (!operatorSurface) {
    appendDesktopLog(
      `[electron] loadSnapshotBundle:no-operator-surface repo=${SNAPSHOT_FILES.dashboardApi} cache=${DESKTOP_LOCAL_DASHBOARD_CACHE_FILE}`,
    );
    return null;
  }

  const readinessValues = ((operatorSurface.runtime_readiness as JsonRecord | undefined)?.values ?? {}) as JsonRecord;
  const entriesEnabled = Boolean((paperReadiness as JsonRecord | null)?.entries_enabled ?? readinessValues.entries_enabled);
  const runtimeRunning = Boolean((paperReadiness as JsonRecord | null)?.runtime_running ?? (readinessValues.runtime_status === "RUNNING"));
  const blockingFaultsCount = Number(readinessValues.blocking_faults_count ?? 0);

  const bundledSnapshot = {
    generated_at: String((operatorSurface.generated_at as string | undefined) ?? new Date().toISOString()),
    dashboard_meta: {
      build_stamp: null,
      server_pid: null,
      server_started_at: null,
      server_url: null,
      server_host: null,
      server_port: null,
      source: "artifact_snapshot",
    },
    refresh: {
      default_interval_seconds: 15,
      options_seconds: [0, 5, 10, 15, 30, 60],
      last_refreshed_at: new Date().toISOString(),
    },
    global: {
      mode: runtimeRunning ? "PAPER" : "IDLE",
      mode_label: runtimeRunning ? "PAPER" : "IDLE",
      live_disabled: true,
      auth_ready: Boolean(readinessValues.auth_readiness),
      auth_label: Boolean(readinessValues.auth_readiness) ? "AUTH READY" : "AUTH NOT READY",
      desk_clean: !Boolean((paperCarryForward as JsonRecord | null)?.active),
      desk_clean_label: Boolean((paperCarryForward as JsonRecord | null)?.active) ? "DESK GUARDED" : "DESK CLEAN",
      paper_run_ready: Boolean((paperReadiness as JsonRecord | null)?.runtime_running),
      paper_run_ready_label: runtimeRunning ? "RUNTIME ACTIVE" : "RUNTIME NOT ACTIVE",
      market_data_status: String(readinessValues.market_data_readiness ?? "UNKNOWN"),
      market_data_label: String(readinessValues.market_data_readiness ?? "UNKNOWN"),
      runtime_health: String(readinessValues.runtime_status ?? "UNKNOWN"),
      runtime_health_label: String(readinessValues.runtime_status ?? "UNKNOWN"),
      reconciliation_status: "SNAPSHOT",
      fault_state: blockingFaultsCount > 0 ? "FAULTS ACTIVE" : "CLEAR",
      shadow_running: false,
      paper_running: runtimeRunning,
      shadow_label: "STOPPED",
      paper_label: runtimeRunning ? "RUNNING" : "STOPPED",
      entries_enabled: entriesEnabled,
      last_processed_bar_timestamp: null,
      last_update_timestamp: operatorSurface.generated_at ?? null,
      current_session_date: (paperPerformance as JsonRecord | null)?.current_session_date ?? null,
      stale: false,
      artifact_age_seconds: null,
    },
    market_context: (operatorSurface.market_context as JsonRecord | undefined) ?? marketIndexStrip,
    treasury_curve: treasuryCurve ?? ((operatorSurface.market_context as JsonRecord | undefined)?.treasury_curve as JsonRecord | undefined) ?? null,
    lane_registry: {
      rows: [
        ...((((paperApprovedModels as JsonRecord | null)?.rows ?? []) as unknown[]) || []),
        ...((((paperNonApprovedLanes as JsonRecord | null)?.rows ?? []) as unknown[]) || []),
      ],
      sections: [],
      total_rows:
        ((((paperApprovedModels as JsonRecord | null)?.rows ?? []) as unknown[]) || []).length +
        ((((paperNonApprovedLanes as JsonRecord | null)?.rows ?? []) as unknown[]) || []).length,
    },
    operator_surface: operatorSurface,
    paper: {
      running: runtimeRunning,
      latest_fills: ((paperFills as JsonRecord | null)?.rows as unknown[]) ?? [],
      latest_intents: ((paperIntents as JsonRecord | null)?.rows as unknown[]) ?? [],
      latest_blotter_rows: ((paperBlotter as JsonRecord | null)?.rows as unknown[]) ?? [],
      position: paperPosition ?? {},
      performance: paperPerformance ?? {},
      readiness: paperReadiness ?? {},
      lane_activity: paperLaneActivity ?? {},
      approved_models: paperApprovedModels ?? {},
      non_approved_lanes: paperNonApprovedLanes ?? {},
    },
    manual_controls: null,
    action_log: actionLog,
    paper_operator_state: {
      entries_enabled: entriesEnabled,
    },
    paper_carry_forward: paperCarryForward,
    paper_pre_session_review: null,
    historical_playback: historicalPlayback,
    research_runtime_bridge: researchRuntimeBridge,
    startup_control_plane: startupControlPlane ?? {},
    strategy_analysis: strategyAnalysis,
    production_link: productionLink,
  };
  void persistDesktopDashboardCache(bundledSnapshot);
  return includeHeavyPayload ? bundledSnapshot : compactDashboardForDesktopTransfer(bundledSnapshot);
}

interface AttachedSnapshotBridge {
  readiness: JsonRecord;
  health: JsonRecord | null;
  backendUrl: string | null;
  detail: string;
}

function synthesizeAttachedSnapshotBridgeFromSnapshot(snapshot: JsonRecord | null): AttachedSnapshotBridge | null {
  if (!snapshot) {
    return null;
  }
  const generatedAt = parseIsoDate(snapshot.generated_at);
  if (!generatedAt || Date.now() - generatedAt.getTime() > PACKAGED_SYNCHRONIZED_SNAPSHOT_MAX_AGE_MS) {
    return null;
  }
  const meta = asJsonRecord(snapshot.dashboard_meta);
  const startupControlPlane = asJsonRecord(snapshot.startup_control_plane);
  const supervisedPaperOperability = asJsonRecord(snapshot.supervised_paper_operability);
  const backendUrl = typeof meta.server_url === "string" && meta.server_url.trim()
    ? meta.server_url.trim()
    : null;
  const launchAllowed = startupControlPlane.launch_allowed === true || String(startupControlPlane.overall_state ?? "").toUpperCase() === "READY";
  const serviceIdentified = Boolean(
    backendUrl
    || String(meta.server_instance_id ?? "").trim()
    || Number(meta.server_pid ?? 0) > 0,
  );
  const operabilityKnown = supervisedPaperOperability.app_usable_for_supervised_paper === true
    || String(supervisedPaperOperability.state ?? "").toUpperCase() === "USABLE";
  if (!serviceIdentified || (!launchAllowed && !operabilityKnown)) {
    return null;
  }
  return {
    readiness: {
      generated_at: snapshot.generated_at ?? null,
      readiness_state: launchAllowed ? "READY" : "USABLE",
      launch_allowed: launchAllowed,
      configured_url: backendUrl,
      payload: {
        reachable: true,
        ready: true,
        generated_at: snapshot.generated_at ?? null,
        instance_id: meta.server_instance_id ?? null,
        pid: meta.server_pid ?? null,
      },
      control_plane: {
        present: true,
        state: startupControlPlane.overall_state ?? (launchAllowed ? "READY" : null),
        launch_allowed: launchAllowed,
        dashboard_attached: true,
        paper_runtime_ready: true,
      },
    },
    health: null,
    backendUrl,
    detail: "Service is attached through the local synchronized operator snapshot.",
  };
}

async function loadPackagedAttachedSnapshotBridge(
  options: { includeHeavyPayload?: boolean } = {},
): Promise<{ snapshot: JsonRecord; bridge: AttachedSnapshotBridge } | null> {
  if (!packagedLocalBundleLaunchContext()) {
    return null;
  }
  const snapshot = await loadSnapshotBundle({
    includeHeavyPayload: options.includeHeavyPayload,
    preferDesktopCache: true,
  });
  if (!snapshot) {
    return null;
  }
  const synthesizedBridge = synthesizeAttachedSnapshotBridgeFromSnapshot(snapshot);
  if (synthesizedBridge) {
    return { snapshot, bridge: synthesizedBridge };
  }
  const bridge = await loadAttachedSnapshotBridge(snapshot);
  if (!bridge) {
    return null;
  }
  return { snapshot, bridge };
}

async function loadAttachedSnapshotBridge(snapshot: JsonRecord | null): Promise<AttachedSnapshotBridge | null> {
  if (testLoadAttachedSnapshotBridgeHook) {
    return testLoadAttachedSnapshotBridgeHook(snapshot);
  }
  if (!snapshot) {
    return null;
  }
  const readinessCandidatePaths = packagedLocalBundleLaunchContext()
    ? [DESKTOP_LOCAL_READINESS_FILE]
    : [DESKTOP_LOCAL_READINESS_FILE, DASHBOARD_READINESS_FILE];
  let readiness: JsonRecord = {};
  for (const candidatePath of readinessCandidatePaths) {
    readiness = asJsonRecord(await readJsonFile<JsonRecord>(candidatePath));
    if (Object.keys(readiness).length > 0) {
      break;
    }
  }
  if (String(readiness.readiness_state ?? "").toUpperCase() !== "READY") {
    return null;
  }
  const generatedAt = parseIsoDate(readiness.generated_at);
  if (!generatedAt || Date.now() - generatedAt.getTime() > ATTACHED_SNAPSHOT_BRIDGE_MAX_AGE_MS) {
    return null;
  }
  const payload = asJsonRecord(readiness.payload);
  const controlPlane = asJsonRecord(readiness.control_plane);
  const listener = asJsonRecord(readiness.listener);
  const health = asJsonRecord(readiness.health);
  if (!(payload.reachable === true && payload.ready === true && controlPlane.launch_allowed === true && listener.reachable === true)) {
    return null;
  }
  const snapshotMeta = asJsonRecord(snapshot.dashboard_meta);
  const snapshotInstanceId = String(snapshotMeta.server_instance_id ?? "").trim();
  const readinessInstanceId = String(
    health.instance_id
      ?? payload.instance_id
      ?? asJsonRecord(readiness.publisher).manager_instance_id
      ?? "",
  ).trim();
  if (snapshotInstanceId && readinessInstanceId && snapshotInstanceId !== readinessInstanceId) {
    return null;
  }
  const backendUrl = typeof readiness.configured_url === "string" && readiness.configured_url.trim()
    ? readiness.configured_url.trim()
    : null;
  return {
    readiness,
    health: Object.keys(health).length ? health : null,
    backendUrl,
    detail:
      "Service is attached through the local readiness bridge and synchronized operator snapshot.",
  };
}

async function latestHistoricalPlaybackManifestInfo(): Promise<{ path: string | null; runStamp: string | null; modifiedAt: string | null }> {
  if (
    historicalPlaybackManifestInfoCache
    && Date.now() - historicalPlaybackManifestInfoCache.fetchedAtMs <= HISTORICAL_PLAYBACK_MANIFEST_CACHE_TTL_MS
  ) {
    return historicalPlaybackManifestInfoCache.value;
  }
  const historicalPlaybackDir = path.join(REPO_ROOT, "outputs", "historical_playback");
  try {
    const names = await fs.readdir(historicalPlaybackDir);
    const manifestNames = names.filter((name) => /^historical_playback_.*\.manifest\.json$/u.test(name));
    if (!manifestNames.length) {
      const emptyResult = { path: null, runStamp: null, modifiedAt: null };
      historicalPlaybackManifestInfoCache = {
        fetchedAtMs: Date.now(),
        value: emptyResult,
      };
      return emptyResult;
    }
    const entries = await Promise.all(
      manifestNames.map(async (name) => {
        const fullPath = path.join(historicalPlaybackDir, name);
        const stat = await fs.stat(fullPath);
        const runStamp = fullPath.match(/historical_playback_(.*)\.manifest\.json$/u)?.[1] ?? "";
        return { fullPath, stat, runStamp };
      }),
    );
    entries.sort((left, right) => {
      if (left.runStamp !== right.runStamp) {
        return right.runStamp.localeCompare(left.runStamp);
      }
      return right.stat.mtimeMs - left.stat.mtimeMs;
    });
    const latest = entries[0];
    const result = {
      path: latest.fullPath,
      runStamp: latest.runStamp || null,
      modifiedAt: new Date(latest.stat.mtimeMs).toISOString(),
    };
    historicalPlaybackManifestInfoCache = {
      fetchedAtMs: Date.now(),
      value: result,
    };
    return result;
  } catch {
    const emptyResult = { path: null, runStamp: null, modifiedAt: null };
    historicalPlaybackManifestInfoCache = {
      fetchedAtMs: Date.now(),
      value: emptyResult,
    };
    return emptyResult;
  }
}

function dashboardHistoricalPlaybackRunStamp(dashboard: JsonRecord | null): string | null {
  const historicalPlayback = (dashboard?.historical_playback as JsonRecord | undefined) ?? null;
  const latestRun = (historicalPlayback?.latest_run as JsonRecord | undefined) ?? null;
  const runStamp = String(latestRun?.run_stamp ?? "").trim();
  return runStamp || null;
}

function dashboardStrategyAnalysisAvailable(dashboard: JsonRecord | null): boolean {
  const strategyAnalysis = (dashboard?.strategy_analysis as JsonRecord | undefined) ?? null;
  if (!strategyAnalysis) {
    return false;
  }
  if (strategyAnalysis.available === true) {
    return true;
  }
  const strategyCount = Number(strategyAnalysis.strategy_count ?? 0);
  const laneCount = Number(strategyAnalysis.lane_count ?? 0);
  return strategyCount > 0 || laneCount > 0;
}

async function buildHistoricalPlaybackSyncStatus(dashboard: JsonRecord | null): Promise<JsonRecord> {
  if (packagedLocalBundleLaunchContext()) {
    const embedded = asJsonRecord((dashboard?.historical_playback_sync as JsonRecord | undefined) ?? null);
    if (Object.keys(embedded).length > 0) {
      return embedded;
    }
    return {
      in_sync: true,
      latest_manifest_path: null,
      latest_manifest_run_stamp: null,
      latest_manifest_modified_at: null,
      dashboard_run_stamp: dashboardHistoricalPlaybackRunStamp(dashboard),
      strategy_analysis_available: dashboardStrategyAnalysisAvailable(dashboard),
      detail: "Historical playback sync is not evaluated from workspace manifests in packaged local launch context.",
    };
  }
  const historicalPlaybackPayload = asJsonRecord((dashboard?.historical_playback as JsonRecord | undefined) ?? null);
  const latestRun = asJsonRecord(historicalPlaybackPayload.latest_run);
  const hasLoadedPlaybackRun =
    Object.keys(historicalPlaybackPayload).length > 0
    || Boolean(typeof latestRun.run_stamp === "string" && latestRun.run_stamp.trim())
    || dashboardStrategyAnalysisAvailable(dashboard);
  if (!hasLoadedPlaybackRun) {
    return {
      in_sync: true,
      latest_manifest_path: null,
      latest_manifest_run_stamp: null,
      latest_manifest_modified_at: null,
      dashboard_run_stamp: null,
      strategy_analysis_available: false,
      detail: "No historical playback run is loaded in the current desktop state.",
    };
  }
  const latestManifest = await latestHistoricalPlaybackManifestInfo();
  const dashboardRunStamp = dashboardHistoricalPlaybackRunStamp(dashboard);
  const strategyAnalysisAvailable = dashboardStrategyAnalysisAvailable(dashboard);
  const hasLatestManifest = Boolean(latestManifest.runStamp);
  const runStampMatches = !hasLatestManifest || latestManifest.runStamp === dashboardRunStamp;
  const inSync = runStampMatches && (!hasLatestManifest || strategyAnalysisAvailable);
  const detail = !hasLatestManifest
    ? "No historical playback manifest is present under outputs/historical_playback."
    : !dashboardRunStamp
      ? `Latest historical playback manifest is ${latestManifest.runStamp}, but the current dashboard state has no loaded playback run stamp.`
      : latestManifest.runStamp !== dashboardRunStamp
        ? `Latest historical playback manifest is ${latestManifest.runStamp}, but the current dashboard state exposes ${dashboardRunStamp}.`
        : !strategyAnalysisAvailable
          ? `Historical playback run ${dashboardRunStamp} is loaded, but strategy analysis is missing from the current desktop state.`
          : `Historical playback run ${dashboardRunStamp} matches the latest manifest and strategy analysis is present.`;
  return {
    in_sync: inSync,
    latest_manifest_path: latestManifest.path,
    latest_manifest_run_stamp: latestManifest.runStamp,
    latest_manifest_modified_at: latestManifest.modifiedAt,
    dashboard_run_stamp: dashboardRunStamp,
    strategy_analysis_available: strategyAnalysisAvailable,
    detail,
  };
}

function attachHistoricalPlaybackSync(dashboard: JsonRecord | null, syncStatus: JsonRecord): void {
  if (!dashboard) {
    return;
  }
  dashboard.historical_playback_sync = syncStatus;
}

function applyHistoricalPlaybackSyncWarning(state: DesktopState, syncStatus: JsonRecord): DesktopState {
  if (syncStatus.in_sync === true) {
    return state;
  }
  const warning = String(syncStatus.detail ?? "Historical playback state is not synchronized.").trim();
  const errors = state.errors.includes(warning) ? state.errors : [...state.errors, warning];
  const sourceLabel = state.source.mode === "snapshot_fallback" ? "SNAPSHOT (PLAYBACK STALE)" : state.source.label;
  return {
    ...state,
    errors,
    source: {
      ...state.source,
      label: sourceLabel,
      detail: `${state.source.detail} ${warning}`.trim(),
    },
  };
}

function readDashboardPid(dashboard: JsonRecord | null, health: JsonRecord | null): number | null {
  const healthPid = Number((health?.pid as number | string | undefined) ?? NaN);
  if (!Number.isNaN(healthPid) && healthPid > 0) {
    return healthPid;
  }
  const metaPid = Number((((dashboard?.dashboard_meta as JsonRecord | undefined)?.server_pid as number | string | undefined) ?? NaN));
  return !Number.isNaN(metaPid) && metaPid > 0 ? metaPid : null;
}

function buildRuntimeStates({
  live,
  snapshotAvailable,
  attachedSnapshotBridge,
  dashboard,
  health,
  infoFiles,
}: {
  live:
    | { mode: "live"; url: string; health: JsonRecord; dashboard: JsonRecord }
    | { mode: "health-only"; url: string; health: JsonRecord; error: string }
    | null;
  snapshotAvailable: boolean;
  attachedSnapshotBridge: AttachedSnapshotBridge | null;
  dashboard: JsonRecord | null;
  health: JsonRecord | null;
  infoFiles: string[];
}): Pick<DesktopState, "source" | "backend" | "connection"> {
  const healthReachable = Boolean(live);
  const apiReachable = live?.mode === "live";
  const staleInfoFile = infoFiles.length > 0 && !healthReachable;
  const dashboardApiTimedOut = live?.mode === "health-only";
  const authRecoveryActive = live?.mode === "health-only" && dashboardApiBlockedByAuth(live.error) && authGateRecoveryScheduled();
  const serviceBootstrapPending = serviceHostBootstrapActive();
  const pid = readDashboardPid(dashboard, health);
  const activeManagedLifecycle = authRecoveryActive
    ? "reconnecting"
    : reconnectTimer
      ? "reconnecting"
      : dashboardLaunchPromise || serviceBootstrapPending
        ? "starting"
        : managerLifecycle;
  const healthStatus: DesktopState["backend"]["healthStatus"] = !healthReachable
    ? "unreachable"
    : String(health?.status ?? "").toLowerCase() === "ok"
      ? "ok"
      : "degraded";
  const apiStatus: DesktopState["backend"]["apiStatus"] = apiReachable
    ? "responding"
    : live?.mode === "health-only"
      ? "timed_out"
      : healthReachable
        ? "unknown"
        : "unreachable";
  const currentError = managerLastError ?? (live?.mode === "health-only" ? live.error : null);
  const failure = classifyStartupFailure(currentError, {
    healthReachable,
    dashboardApiTimedOut,
  });
  const backendPayload = (
    state: DesktopState["backend"]["state"],
    label: string,
    detail: string,
    lastError: string | null,
  ): DesktopState["backend"] => ({
    state,
    label,
    detail,
    lastError,
    nextRetryAt: authRecoveryActive ? authGateRecoveryNextRetryAt : nextRetryAt,
    retryCount: authRecoveryActive ? authGateRecoveryAttemptCount : reconnectAttemptCount,
    pid,
    apiStatus,
    healthStatus,
    managerOwned: managerOwnsBackend,
    startupFailureKind: failure.kind,
    actionHint: failure.hint,
    staleListenerDetected: failure.staleListenerDetected,
    healthReachable,
    dashboardApiTimedOut,
    portConflictDetected: failure.portConflictDetected,
  });

  if (apiReachable) {
    clearAuthGateRecoveryState();
    const dashboardRecovery = asJsonRecord((dashboard?.dashboard_recovery as JsonRecord | undefined) ?? null);
    const dashboardRecoveryActive = dashboardRecovery.active === true;
    const dashboardRecoveryReason = String(dashboardRecovery.reason ?? "").trim();
    const dashboardRecoveryNextAttemptAt = String(dashboardRecovery.next_recovery_attempt_at ?? "").trim();
    const operationalReadiness = deriveOperationalReadiness({
      connection: "live",
      backendUrl: live?.mode === "live" ? live.url : null,
      source: {
        mode: "live_api",
        healthReachable: true,
        apiReachable: true,
        canRunLiveActions: true,
      },
      backend: {
        state: "healthy",
        healthStatus,
        apiStatus,
        dashboardApiTimedOut: false,
      },
      startup: null,
      startupControlPlane: asJsonRecord((dashboard?.startup_control_plane as JsonRecord | undefined) ?? null),
      supervisedPaperOperability: asJsonRecord((dashboard?.supervised_paper_operability as JsonRecord | undefined) ?? null),
      paperReadiness: asJsonRecord(asJsonRecord((dashboard?.paper as JsonRecord | undefined) ?? null).readiness),
      temporaryPaperRuntimeIntegrity: asJsonRecord(
        asJsonRecord((dashboard?.paper as JsonRecord | undefined) ?? null).temporary_paper_runtime_integrity,
      ),
      authReadyForPaperStartup: Boolean(asJsonRecord((dashboard?.global as JsonRecord | undefined) ?? null).auth_ready),
    });
    managerLifecycle = "healthy";
    reconnectAttemptCount = 0;
    clearReconnectTimer();
    setManagerError(null);
    if (operationalReadiness.overallState !== "READY") {
      if (operationalReadiness.dashboardAttached) {
        const serviceAttachedDetail = dashboardRecoveryActive
          ? (
              dashboardRecoveryNextAttemptAt
                ? `${dashboardRecoveryReason || "Automatic backend recovery is active."} Next retry is scheduled for ${dashboardRecoveryNextAttemptAt}.`
                : (dashboardRecoveryReason || "Automatic backend recovery is active.")
            )
          : (
              operationalReadiness.appUsableForSupervisedPaper
                ? "Desktop is attached to the running service-first backend and paper runtime is operational."
                : "Desktop is attached to the running service-first backend, but supervised paper operation still requires attention."
            );
        return {
          connection: "live",
          source: {
            mode: "live_api",
            label: dashboardRecoveryActive ? "SERVICE ATTACHED / RECOVERING" : "SERVICE ATTACHED",
            detail: operationalReadiness.summaryLine || serviceAttachedDetail,
            canRunLiveActions: true,
            healthReachable: true,
            apiReachable: true,
          },
          backend: {
            ...backendPayload("healthy", "HEALTHY", "Backend health and dashboard API are both responding.", null),
            startupFailureKind: "none",
            actionHint: operationalReadiness.primaryAction.label,
            staleListenerDetected: false,
            dashboardApiTimedOut: false,
            portConflictDetected: false,
          },
        };
      }
      const liveDashboardDegraded = operationalReadiness.dashboardAttached;
      const dashboardDetail = liveDashboardDegraded
        ? "Live /api/dashboard is attached, but the operator path still has blocking backend conditions to resolve."
        : "Live /health is up, but the operator path is not fully attached yet.";
      return {
        connection: "live",
        source: {
          mode: "live_api",
          label: liveDashboardDegraded ? "LIVE API (DEGRADED)" : "ATTACH INCOMPLETE",
          detail: operationalReadiness.summaryLine || dashboardDetail,
          canRunLiveActions: liveDashboardDegraded,
          healthReachable: true,
          apiReachable: true,
        },
        backend: {
          ...backendPayload(
            "degraded",
            liveDashboardDegraded ? "DEGRADED" : "ATTACH INCOMPLETE",
            operationalReadiness.explanation || dashboardDetail,
            null,
          ),
          startupFailureKind: "none",
          actionHint: operationalReadiness.primaryAction.label,
          staleListenerDetected: false,
          dashboardApiTimedOut: false,
          portConflictDetected: false,
        },
      };
    }
    return {
      connection: "live",
      source: {
        mode: "live_api",
        label: "LIVE API",
        detail: "Using live /health and /api/dashboard responses from the local dashboard server.",
        canRunLiveActions: true,
        healthReachable: true,
        apiReachable: true,
      },
      backend: {
        ...backendPayload("healthy", "HEALTHY", "Backend health and dashboard API are both responding.", null),
        startupFailureKind: "none",
        actionHint: null,
        staleListenerDetected: false,
        dashboardApiTimedOut: false,
        portConflictDetected: false,
      },
    };
  }

  if (activeManagedLifecycle === "starting") {
    return {
      connection: snapshotAvailable ? "snapshot" : "unavailable",
      source: {
        mode: "degraded_reconnecting",
        label: "STARTING / RECOVERING",
        detail: serviceBootstrapPending
          ? "Automatic backend recovery is bringing the local dashboard API online; the desktop will attach as soon as live state is available."
          : "Backend start is in progress; the desktop will wait for live API readiness before enabling live actions.",
        canRunLiveActions: false,
        healthReachable,
        apiReachable: false,
      },
      backend: backendPayload(
        "starting",
        "STARTING",
        serviceBootstrapPending
          ? "Service-first recovery is starting the local backend and waiting for health and /api/dashboard readiness."
          : "Dashboard manager is starting the local backend and waiting for readiness.",
        managerLastError ?? (live?.mode === "health-only" ? live.error : null),
      ),
    };
  }

  if (activeManagedLifecycle === "reconnecting") {
    const recoveryDetail = authRecoveryActive
      ? (
          authGateRecoveryNextRetryAt
            ? `Automatic Schwab auth recovery is active. Next retry is scheduled for ${authGateRecoveryNextRetryAt}.`
            : "Automatic Schwab auth recovery is active."
        )
      : (
          nextRetryAt
            ? `Managed backend recovery is active. Next reconnect attempt is scheduled for ${nextRetryAt}.`
            : "Managed backend recovery is active."
        );
    return {
      connection: snapshotAvailable ? "snapshot" : "unavailable",
      source: {
        mode: "degraded_reconnecting",
        label: "RECOVERING",
        detail: recoveryDetail,
        canRunLiveActions: false,
        healthReachable,
        apiReachable: false,
      },
      backend: backendPayload(
        "reconnecting",
        "RECOVERING",
        authRecoveryActive
          ? recoveryDetail
          : (
              nextRetryAt
                ? `Next reconnect attempt scheduled for ${nextRetryAt}.`
                : "Reconnect recovery is active for the managed backend."
            ),
        managerLastError ?? (live?.mode === "health-only" ? live.error : null),
      ),
    };
  }

  if (live?.mode === "health-only") {
    return {
      connection: snapshotAvailable ? "snapshot" : "unavailable",
      source: {
        mode: snapshotAvailable ? "snapshot_fallback" : "backend_down",
        label: "API NOT READY",
        detail: `Live /health is reachable at ${live.url}, but /api/dashboard did not become ready quickly enough for startup attach.`,
        canRunLiveActions: false,
        healthReachable: true,
        apiReachable: false,
      },
      backend: backendPayload(
        "degraded",
        "API NOT READY",
        "Backend health is reachable, but the full /api/dashboard payload is not responsive.",
        live.error,
      ),
    };
  }

  if (attachedSnapshotBridge && snapshotAvailable) {
    return {
      connection: "snapshot",
      source: {
        mode: "attached_snapshot_bridge",
        label: "SERVICE ATTACHED",
        detail: attachedSnapshotBridge.detail,
        canRunLiveActions: false,
        healthReachable: true,
        apiReachable: false,
      },
      backend: {
        ...backendPayload(
          "healthy",
          "HEALTHY",
          "Dashboard readiness is healthy, but this launch context cannot use the direct localhost API transport.",
          null,
        ),
        startupFailureKind: "none",
        actionHint: "Refresh",
        staleListenerDetected: false,
        healthReachable: true,
        dashboardApiTimedOut: false,
        portConflictDetected: false,
      },
    };
  }

  if (snapshotAvailable) {
    const snapshotLabel = failure.kind !== "none" ? "STARTUP FAILURE / SNAPSHOT" : "SNAPSHOT FALLBACK";
    const snapshotDetail = failure.kind !== "none"
      ? `Using persisted operator snapshots because Dashboard/API startup failed with ${failure.kind.replace(/_/g, " ")}.`
      : staleInfoFile
        ? "Using persisted operator snapshots because the stored backend endpoint is stale or unreachable."
        : "Using persisted operator snapshots because no live dashboard API is currently available.";
    return {
      connection: "snapshot",
      source: {
        mode: "snapshot_fallback",
        label: snapshotLabel,
        detail: snapshotDetail,
        canRunLiveActions: false,
        healthReachable: false,
        apiReachable: false,
      },
      backend: backendPayload(
        "backend_down",
        failure.kind !== "none" ? "STARTUP FAILURE" : "BACKEND DOWN",
        staleInfoFile
          ? "Stored dashboard info exists, but the backend did not answer health checks."
          : "No live backend answered; the app is running from the latest persisted dashboard artifacts.",
        managerLastError,
      ),
    };
  }

  return {
    connection: "unavailable",
    source: {
      mode: "backend_down",
      label: "BACKEND DOWN",
      detail: "No live backend or snapshot artifacts are currently available.",
      canRunLiveActions: false,
      healthReachable: false,
      apiReachable: false,
    },
    backend: backendPayload(
      "backend_down",
      failure.kind !== "none" ? "STARTUP FAILURE" : "BACKEND DOWN",
      "No backend answered health checks and no persisted snapshot bundle was available.",
      managerLastError,
    ),
  };
}

async function waitForLiveDashboard(timeoutMs = DASHBOARD_STARTUP_TIMEOUT_MS): Promise<DesktopState> {
  const deadline = Date.now() + timeoutMs;
  let latestState = await getDesktopState();
  while (Date.now() < deadline) {
    if (latestState.connection === "live") {
      return latestState;
    }
    if (!shouldContinueWaitingForRecovery(latestState) && managerLastError) {
      return latestState;
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
    latestState = await getDesktopState();
  }
  return latestState;
}

async function execScript(args: string[]): Promise<{ ok: boolean; stdout: string; stderr: string; code: number | null }> {
  if (testExecScriptHook) {
    return testExecScriptHook(args);
  }
  return new Promise((resolve) => {
    const child = spawn("bash", args, {
      cwd: REPO_ROOT,
      env: process.env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      stdout += String(chunk);
    });
    child.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });
    child.on("close", (code) => {
      resolve({
        ok: code === 0,
        stdout: stdout.trim(),
        stderr: stderr.trim(),
        code,
      });
    });
  });
}

async function ensureServiceHostUsable(): Promise<void> {
  if (testEnsureServiceHostUsableHook) {
    await testEnsureServiceHostUsableHook();
    return;
  }
  if (serviceHostBootstrapPromise) {
    return serviceHostBootstrapPromise;
  }
  serviceHostBootstrapPromise = (async () => {
    appendDesktopLog("[electron] service-first bootstrap:begin");
    const result = await execScript(["scripts/run_headless_supervised_paper_service.sh"]);
    if (!result.ok) {
      const detail = [result.stdout, result.stderr].filter(Boolean).join("\n").trim() || "Service-first host bootstrap failed.";
      appendDesktopLog(`[electron] service-first bootstrap:failed ${detail}`);
      setManagerError(summarizeErrorText(detail) ?? detail);
      managerLifecycle = "degraded";
      throw new Error(detail);
    }
    setManagerError(null);
    appendDesktopLog("[electron] service-first bootstrap:ready");
  })().finally(() => {
    serviceHostBootstrapPromise = null;
  });
  return serviceHostBootstrapPromise;
}

function requestServiceHostBootstrap(): void {
  if (DESKTOP_STATE_FIXTURE_PATH || managerOwnsBackend || shutdownRequested || serviceHostBootstrapActive()) {
    return;
  }
  if (autoBootstrapBlockedBySandbox()) {
    appendDesktopLog("[electron] service-first bootstrap:skipped sandboxed launch context blocks automatic backend bootstrap");
    managerLifecycle = "degraded";
    return;
  }
  managerLifecycle = "starting";
  void ensureServiceHostUsable().catch(() => {
    // ensureServiceHostUsable already records the failure detail and lifecycle.
  });
}

export async function prepareDesktopForLaunch(): Promise<void> {
  if (DESKTOP_STATE_FIXTURE_PATH) {
    return;
  }
  const packagedBridge = await loadPackagedAttachedSnapshotBridge({ includeHeavyPayload: false });
  if (packagedBridge) {
    return;
  }
  if (packagedLocalBundleLaunchContext()) {
    if (autoBootstrapBlockedBySandbox()) {
      return;
    }
    requestServiceHostBootstrap();
    return;
  }
  const { urls } = await candidateUrls();
  const live = await loadLiveDashboard(urls, {
    healthTimeoutMs: STARTUP_HEALTH_TIMEOUT_MS,
    dashboardTimeoutMs: STARTUP_DASHBOARD_TIMEOUT_MS,
  });
  if (live?.mode === "live") {
    return;
  }
  if (autoBootstrapBlockedBySandbox()) {
    return;
  }
  requestServiceHostBootstrap();
}

async function probeDesktopState(
  options: { allowServiceBootstrap?: boolean; includeHeavyPayload?: boolean } = {},
): Promise<DesktopState> {
  const allowServiceBootstrap = options.allowServiceBootstrap !== false;
  const includeHeavyPayload = options.includeHeavyPayload !== false;
  const packagedLocalLaunch = packagedLocalBundleLaunchContext();
  const localAuth = await buildLocalOperatorAuthState();
  const { urls, infoFiles } = await candidateUrls();
  const errors: string[] = [];
  const packagedBridge = await loadPackagedAttachedSnapshotBridge({ includeHeavyPayload });
  let snapshot = packagedBridge?.snapshot ?? null;
  let attachedSnapshotBridge = packagedBridge?.bridge ?? null;
  const packagedBridgeAttached = Boolean(packagedBridge);
  let live: LoadLiveDashboardResult | null = null;
  if (!packagedBridgeAttached && !packagedLocalLaunch) {
    const livePromise = loadLiveDashboard(urls, {
      healthTimeoutMs: STARTUP_HEALTH_TIMEOUT_MS,
      dashboardTimeoutMs: STARTUP_DASHBOARD_TIMEOUT_MS,
    });
    if (!snapshot) {
      snapshot = await loadSnapshotBundle({
        includeHeavyPayload,
        preferDesktopCache: !includeHeavyPayload || packagedLocalLaunch,
      });
    }
    live = snapshot
      ? await promiseWithTimeout(livePromise, SNAPSHOT_PROMOTION_GRACE_MS, null)
      : await livePromise;
  } else if (!snapshot) {
    snapshot = await loadSnapshotBundle({
      includeHeavyPayload,
      preferDesktopCache: !includeHeavyPayload || packagedLocalLaunch,
    });
  }
  if (live?.mode === "live") {
    snapshot = null;
    attachedSnapshotBridge = null;
  }
  const liveDashboard = live?.mode === "live" ? live.dashboard : null;
  const effectiveSnapshot = liveDashboard ? null : snapshot;
  if (!attachedSnapshotBridge && !liveDashboard) {
    attachedSnapshotBridge = await loadAttachedSnapshotBridge(effectiveSnapshot);
  }
  if (live?.mode === "health-only" && dashboardApiBlockedByAuth(live.error)) {
    scheduleAuthGateRecovery(live.error);
  }
  if (
    allowServiceBootstrap
    && live === null
    && !(packagedLocalLaunch && Boolean(snapshot))
    && !managerOwnsBackend
    && !shutdownRequested
    && !DESKTOP_STATE_FIXTURE_PATH
    && !autoBootstrapBlockedBySandbox()
  ) {
    requestServiceHostBootstrap();
  }
  const liveHealth = live?.mode === "live" ? live.health : live?.mode === "health-only" ? live.health : attachedSnapshotBridge?.health ?? null;
  const dashboard = liveDashboard ?? effectiveSnapshot;
  const returnedDashboard = includeHeavyPayload ? dashboard : compactDashboardForDesktopTransfer(dashboard);
  const historicalPlaybackSync = await buildHistoricalPlaybackSyncStatus(dashboard);
  attachHistoricalPlaybackSync(returnedDashboard, historicalPlaybackSync);
  const runtimeStates = buildRuntimeStates({
    live,
    snapshotAvailable: Boolean(effectiveSnapshot),
    attachedSnapshotBridge,
    dashboard,
    health: liveHealth,
    infoFiles,
  });
  const startupAssessment = classifyStartupFailure(runtimeStates.backend.lastError, {
    healthReachable: runtimeStates.source.healthReachable,
    dashboardApiTimedOut: runtimeStates.backend.dashboardApiTimedOut,
  });

  if (live?.mode === "live" && dashboard) {
    void persistDesktopDashboardCache(dashboard);
    const state: DesktopState = {
      connection: runtimeStates.connection,
      dashboard: returnedDashboard,
      health: live.health,
      backendUrl: live.url,
      source: runtimeStates.source,
      backend: runtimeStates.backend,
      startup: buildStartupState({
        dashboard,
        health: live.health,
        backendUrl: live.url,
        assessment: startupAssessment,
        healthReachable: runtimeStates.source.healthReachable,
        dashboardApiTimedOut: runtimeStates.backend.dashboardApiTimedOut,
      }),
      infoFiles,
      errors,
      runtimeLogPath: DEFAULT_LOG_FILE,
      backendLogPath: DEFAULT_LOG_FILE,
      desktopLogPath: DESKTOP_LOG_FILE,
      appVersion: String(packageJson.version ?? "0.0.0"),
      manager: {
        running: Boolean(dashboardManager),
        lastExitCode,
        lastExitSignal,
        recentOutput: recentManagerOutput,
      },
      localAuth,
      refreshedAt: new Date().toISOString(),
    };
    const syncedState = applyHistoricalPlaybackSyncWarning(state, historicalPlaybackSync);
    await writeDesktopStartupStatus(syncedState);
    return syncedState;
  }

  if (effectiveSnapshot) {
    void persistDesktopDashboardCache(effectiveSnapshot);
    if (live?.mode === "health-only") {
      errors.push(
        `Live dashboard health is reachable at ${live.url}, but /api/dashboard did not become ready quickly; showing latest persisted operator snapshots immediately while live attach continues in the background.`,
      );
    } else if (!attachedSnapshotBridge) {
      errors.push("Live dashboard API is unavailable; showing latest persisted operator snapshots.");
    }
    const state: DesktopState = {
      connection: runtimeStates.connection,
      dashboard: returnedDashboard,
      health: live?.mode === "health-only" ? live.health : null,
      backendUrl: live?.mode === "health-only" ? live.url : attachedSnapshotBridge?.backendUrl ?? null,
      source: runtimeStates.source,
      backend: runtimeStates.backend,
      startup: buildStartupState({
        dashboard: snapshot,
        health: live?.mode === "health-only" ? live.health : attachedSnapshotBridge?.health ?? null,
        backendUrl: live?.mode === "health-only" ? live.url : attachedSnapshotBridge?.backendUrl ?? null,
        assessment: startupAssessment,
        healthReachable: runtimeStates.source.healthReachable,
        dashboardApiTimedOut: runtimeStates.backend.dashboardApiTimedOut,
      }),
      infoFiles,
      errors,
      runtimeLogPath: DEFAULT_LOG_FILE,
      backendLogPath: DEFAULT_LOG_FILE,
      desktopLogPath: DESKTOP_LOG_FILE,
      appVersion: String(packageJson.version ?? "0.0.0"),
      manager: {
        running: Boolean(dashboardManager),
        lastExitCode,
        lastExitSignal,
        recentOutput: recentManagerOutput,
      },
      localAuth,
      refreshedAt: new Date().toISOString(),
    };
    const syncedState = applyHistoricalPlaybackSyncWarning(state, historicalPlaybackSync);
    await writeDesktopStartupStatus(syncedState);
    return syncedState;
  }

  errors.push("No live dashboard API responded quickly and no persisted operator snapshots were available yet.");
  const state: DesktopState = {
    connection: runtimeStates.connection,
    dashboard: null,
    health: live?.mode === "health-only" ? live.health : null,
    backendUrl: live?.mode === "health-only" ? live.url : null,
    source: runtimeStates.source,
    backend: runtimeStates.backend,
    startup: buildStartupState({
      dashboard: null,
      health: live?.mode === "health-only" ? live.health : null,
      backendUrl: live?.mode === "health-only" ? live.url : null,
      assessment: startupAssessment,
      healthReachable: runtimeStates.source.healthReachable,
      dashboardApiTimedOut: runtimeStates.backend.dashboardApiTimedOut,
    }),
    infoFiles,
    errors,
    runtimeLogPath: DEFAULT_LOG_FILE,
    backendLogPath: DEFAULT_LOG_FILE,
    desktopLogPath: DESKTOP_LOG_FILE,
    appVersion: String(packageJson.version ?? "0.0.0"),
    manager: {
      running: Boolean(dashboardManager),
      lastExitCode,
      lastExitSignal,
      recentOutput: recentManagerOutput,
    },
    localAuth,
    refreshedAt: new Date().toISOString(),
  };
  await writeDesktopStartupStatus(state);
  return state;
}

export async function getDesktopState(options: { includeHeavyPayload?: boolean } = {}): Promise<DesktopState> {
  if (testGetDesktopStateHook) {
    return testGetDesktopStateHook();
  }
  const includeHeavyPayload = options.includeHeavyPayload !== false;
  if (desktopStateRequestPromise && includeHeavyPayload) {
    return desktopStateRequestPromise;
  }
  const requestPromise = (
    DESKTOP_STATE_FIXTURE_PATH
      ? loadDesktopStateFixtureState()
      : probeDesktopState({ includeHeavyPayload }).then(applyDesktopStateFixtureOverride)
  );
  if (includeHeavyPayload) {
    desktopStateRequestPromise = requestPromise.finally(() => {
      desktopStateRequestPromise = null;
    });
    return desktopStateRequestPromise;
  }
  return requestPromise;
}

async function beginDashboardLaunch({ manual }: { manual: boolean }): Promise<DesktopState> {
  if (dashboardLaunchPromise) {
    return dashboardLaunchPromise;
  }
  clearReconnectTimer();
  stopWasRequested = false;
  shutdownRequested = false;
  managerOwnsBackend = true;
  managerLifecycle = manual ? "starting" : "reconnecting";
  setManagerError(null);
  const launchArgs = [
    "scripts/run_operator_dashboard.sh",
    "--no-open-browser",
    "--verify-dashboard-api",
    "--host",
    DEFAULT_DASHBOARD_HOST,
    "--port",
    String(DEFAULT_DASHBOARD_PORT),
  ];
  if (ALLOW_PORT_FALLBACK) {
    launchArgs.push("--allow-port-fallback");
  }
  const child = spawn(
    "bash",
    launchArgs,
    {
      cwd: REPO_ROOT,
      env: process.env,
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  trackDashboardManager(child);
  dashboardLaunchPromise = waitForLiveDashboard(DASHBOARD_STARTUP_TIMEOUT_MS).finally(() => {
    dashboardLaunchPromise = null;
  });
  return dashboardLaunchPromise;
}

export async function startDashboard(): Promise<DesktopCommandResult> {
  const currentState = await getDesktopState();
  if (currentState.connection === "live" && currentState.source.canRunLiveActions) {
    return {
      ok: true,
      message:
        currentState.startup.ownership === "started_managed"
          ? "Dashboard/API is already live and managed by this desktop session."
          : "Dashboard/API is already live; the desktop app is attached to the existing local backend.",
      state: currentState,
    };
  }

  if (dashboardLaunchPromise) {
    return {
      ok: true,
      message: "Dashboard/API start is already in progress.",
      state: currentState,
    };
  }

  if (dashboardManager && managerLifecycle !== "reconnecting") {
    return {
      ok: true,
      message: "Dashboard manager is already running in this desktop session.",
      state: currentState,
    };
  }

  const state = await (testBeginDashboardLaunchHook ?? beginDashboardLaunch)({ manual: true });
  if (state.connection === "live") {
    return {
      ok: true,
      message:
        state.startup.ownership === "started_managed"
          ? `Dashboard/API started at ${state.startup.chosenUrl ?? state.backendUrl ?? DEFAULT_DASHBOARD_URL}.`
          : `Dashboard/API attached at ${state.startup.chosenUrl ?? state.backendUrl ?? DEFAULT_DASHBOARD_URL}.`,
      output: recentManagerOutput.join("\n"),
      state,
    };
  }

  const failureOutput = recentManagerOutput.join("\n");
  const backendLogTail = await readLogTail(DEFAULT_LOG_FILE, 40);
  const compactError = summarizeErrorText(backendLogTail) ?? summarizeErrorText(failureOutput) ?? state.backend.lastError ?? state.backend.detail;
  setManagerError(compactError ?? null);
  const failureAssessment = classifyStartupFailure([failureOutput, backendLogTail, compactError].filter(Boolean).join("\n"), {
    healthReachable: state.backend.healthReachable,
    dashboardApiTimedOut: state.backend.dashboardApiTimedOut,
  });
  if (shouldAutoReconnectDashboardFailure(failureAssessment, reconnectAttemptCount)) {
    managerLifecycle = "reconnecting";
    scheduleReconnect();
  } else {
    clearReconnectTimer();
    managerLifecycle = "degraded";
  }
  const failureState = await getDesktopState();
  return {
    ok: false,
    message:
      failureState.backend.state === "reconnecting"
        ? `Dashboard/API recovery is active after ${startupFailureLabelForCommand(failureState.backend.startupFailureKind)}.`
        : `Dashboard/API start failed: ${startupFailureLabelForCommand(failureState.backend.startupFailureKind)}.`,
    detail: failureState.backend.lastError ?? undefined,
    output: [failureOutput, backendLogTail].filter(Boolean).join("\n\n"),
    state: failureState,
  };
}

export async function stopDashboard(): Promise<DesktopCommandResult> {
  const stateBeforeStop = await getDesktopState();
  if (!dashboardManager && stateBeforeStop.connection !== "live") {
    return {
      ok: true,
      message: "Dashboard/API is already stopped.",
      state: stateBeforeStop,
    };
  }
  stopWasRequested = true;
  managerOwnsBackend = false;
  shutdownRequested = false;
  clearReconnectTimer();
  const result = await execScript(["scripts/stop_operator_dashboard.sh"]);
  if (dashboardManager && !dashboardManager.killed) {
    dashboardManager.kill("SIGTERM");
  }
  if (!result.ok) {
    setManagerError(summarizeErrorText(result.stderr || result.stdout || "Dashboard stop command failed."));
  } else {
    setManagerError(null);
    managerLifecycle = "idle";
  }
  return {
    ok: result.ok,
    message: result.ok ? "Dashboard/API stopped." : "Dashboard/API stop command failed.",
    detail: result.stderr || undefined,
    output: [result.stdout, result.stderr].filter(Boolean).join("\n"),
    state: await getDesktopState(),
  };
}

export async function restartDashboard(): Promise<DesktopCommandResult> {
  if (dashboardLaunchPromise) {
    return {
      ok: false,
      message: "A dashboard start or reconnect attempt is already in progress.",
      state: await getDesktopState(),
    };
  }
  const stopped = await stopDashboard();
  if (!stopped.ok && !String(stopped.output ?? "").includes("No operator dashboard PID file found")) {
    return stopped;
  }
  return startDashboard();
}

export async function runDashboardAction(action: string, payload: JsonRecord = {}): Promise<DesktopCommandResult> {
  const state = await getDesktopState();
  if (action === "auth-gate-check" && (!state.source.canRunLiveActions || !state.backendUrl)) {
    const result = await execScript(["scripts/run_schwab_auth_gate.sh"]);
    const detail = [result.stderr, result.stdout].filter(Boolean).join("\n").trim();
    return {
      ok: result.ok,
      message: result.ok ? "Auth Gate Check completed." : "Auth Gate Check failed.",
      detail: detail || undefined,
      output: [result.stdout, result.stderr].filter(Boolean).join("\n"),
      state: await getDesktopState(),
    };
  }
  if (!state.source.canRunLiveActions || !state.backendUrl) {
    return {
      ok: false,
      message: "Dashboard actions require a live local API connection.",
      detail: state.source.detail,
      state,
    };
  }
  try {
    let authorizedPayload = payload;
    if (SENSITIVE_DASHBOARD_ACTIONS.has(action)) {
      const authorization = await authorizeSensitiveAction("dashboard", action, payload);
      if (!authorization.ok) {
        return authorization.result;
      }
      authorizedPayload = authorization.payload;
    }
    const response = await (testFetchHook ?? fetch)(new URL(`api/action/${action}`, state.backendUrl).toString(), {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(authorizedPayload),
    });
    const responsePayload = (await response.json()) as JsonRecord;
    const actionLabel = String(responsePayload.action_label ?? responsePayload.action ?? action);
    const normalizedMessage = String(responsePayload.message ?? "").trim();
    const normalizedDetail = String(
      responsePayload.detail
      ?? (
        responsePayload.reason_code
          ? `${String(responsePayload.reason_code)}${responsePayload.next_action ? ` | Next action: ${String(responsePayload.next_action)}` : ""}`
          : ""
      ),
    ).trim();
    const primaryMessage = !response.ok || responsePayload.ok === false
      ? (normalizedMessage || actionLabel)
      : actionLabel;
    return {
      ok: response.ok && Boolean(responsePayload.ok ?? true),
      message: primaryMessage,
      detail: normalizedDetail || normalizedMessage,
      output: String(responsePayload.output ?? normalizedMessage ?? ""),
      state: await getDesktopState(),
    };
  } catch (error) {
    setManagerError(summarizeErrorText(error instanceof Error ? error.message : String(error)));
    managerLifecycle = managerOwnsBackend ? "reconnecting" : "degraded";
    return {
      ok: false,
      message: `Failed to run dashboard action ${action}.`,
      detail: error instanceof Error ? error.message : String(error),
      state,
    };
  }
}

export const __testing = {
  resetRuntimeState(): void {
    dashboardManager = null;
    recentManagerOutput = [];
    lastExitCode = null;
    lastExitSignal = null;
    desktopStateRequestPromise = null;
    dashboardLaunchPromise = null;
    serviceHostBootstrapPromise = null;
    authGateRecoveryPromise = null;
    clearReconnectTimer();
    reconnectAttemptCount = 0;
    nextRetryAt = null;
    authGateRecoveryAttemptCount = 0;
    authGateRecoveryNextRetryAt = null;
    managerLastError = null;
    managerLifecycle = "idle";
    managerOwnsBackend = false;
    stopWasRequested = false;
    shutdownRequested = false;
    testGetDesktopStateHook = null;
    testBeginDashboardLaunchHook = null;
    testEnsureServiceHostUsableHook = null;
    testFetchHook = null;
    testExecScriptHook = null;
    testCurlJsonHook = null;
    testBuildLocalOperatorAuthStateHook = null;
    testAutoBootstrapBlockedHook = null;
    testLoadLiveDashboardHook = null;
    testLoadSnapshotBundleHook = null;
    testLoadAttachedSnapshotBridgeHook = null;
    testPackagedLocalBundleLaunchContextHook = null;
    historicalPlaybackManifestInfoCache = null;
  },
  setGetDesktopStateHook(hook: (() => Promise<DesktopState>) | null): void {
    testGetDesktopStateHook = hook;
  },
  setBeginDashboardLaunchHook(hook: ((options: { manual: boolean }) => Promise<DesktopState>) | null): void {
    testBeginDashboardLaunchHook = hook;
  },
  setEnsureServiceHostUsableHook(hook: (() => Promise<void>) | null): void {
    testEnsureServiceHostUsableHook = hook;
  },
  setFetchHook(hook: ((input: string | URL, init?: RequestInit) => Promise<Response>) | null): void {
    testFetchHook = hook;
  },
  setExecScriptHook(hook: ((args: string[]) => Promise<{ ok: boolean; stdout: string; stderr: string; code: number | null }>) | null): void {
    testExecScriptHook = hook;
  },
  setCurlJsonHook(hook: ((url: string, timeoutMs: number) => Promise<JsonRecord>) | null): void {
    testCurlJsonHook = hook;
  },
  setBuildLocalOperatorAuthStateHook(hook: (() => Promise<LocalOperatorAuthState>) | null): void {
    testBuildLocalOperatorAuthStateHook = hook;
  },
  setAutoBootstrapBlockedHook(hook: (() => boolean) | null): void {
    testAutoBootstrapBlockedHook = hook;
  },
  setLoadLiveDashboardHook(
    hook: ((urls: string[], options?: LoadLiveDashboardOptions) => Promise<LoadLiveDashboardResult>) | null,
  ): void {
    testLoadLiveDashboardHook = hook;
  },
  setLoadSnapshotBundleHook(hook: (() => Promise<JsonRecord | null>) | null): void {
    testLoadSnapshotBundleHook = hook;
  },
  setLoadAttachedSnapshotBridgeHook(hook: ((snapshot: JsonRecord | null) => Promise<AttachedSnapshotBridge | null>) | null): void {
    testLoadAttachedSnapshotBridgeHook = hook;
  },
  setPackagedLocalBundleLaunchContextHook(hook: (() => boolean) | null): void {
    testPackagedLocalBundleLaunchContextHook = hook;
  },
  shouldContinueWaitingForRecovery(state: DesktopState): boolean {
    return shouldContinueWaitingForRecovery(state);
  },
};

export async function runProductionLinkAction(action: string, payload: JsonRecord): Promise<DesktopCommandResult> {
  const state = await getDesktopState();
  if (!state.source.canRunLiveActions || !state.backendUrl) {
    return {
      ok: false,
      message: "Production-link actions require a live local API connection.",
      detail: state.source.detail,
      state,
    };
  }
  try {
    let authorizedPayload = payload ?? {};
    if (SENSITIVE_PRODUCTION_ACTIONS.has(action)) {
      const authorization = await authorizeSensitiveAction("production", action, authorizedPayload);
      if (!authorization.ok) {
        return authorization.result;
      }
      authorizedPayload = authorization.payload;
    }
    const requestUrl = (backendUrl: string) => new URL(`api/production-link/${action}`, backendUrl).toString();
    let response: LocalApiResponse;
    try {
      response = await postLocalJson(requestUrl(state.backendUrl), authorizedPayload);
    } catch (initialError) {
      const firstAttemptDetail = describeLocalApiTransportError(requestUrl(state.backendUrl), initialError);
      const bootstrapBlocked = autoBootstrapBlockedBySandbox();
      if (!bootstrapBlocked) {
        try {
          await (testEnsureServiceHostUsableHook ?? ensureServiceHostUsable)();
        } catch {
          // Keep the first transport detail and let the retry decision below determine the final message.
        }
      }
      if (bootstrapBlocked) {
        throw new Error(`Local production-link API transport failed. ${firstAttemptDetail}`);
      }
      const refreshedState = await getDesktopState();
      if (!refreshedState.source.canRunLiveActions || !refreshedState.backendUrl) {
        throw new Error(`Local production-link API became unavailable after refresh. ${firstAttemptDetail}`);
      }
      try {
        response = await postLocalJson(requestUrl(refreshedState.backendUrl), authorizedPayload);
      } catch (retryError) {
        throw new Error(
          `Local production-link API transport failed. First attempt: ${firstAttemptDetail}. Retry: ${describeLocalApiTransportError(requestUrl(refreshedState.backendUrl), retryError)}`,
        );
      }
    }
    if (response.ok && !response.bodyText.trim()) {
      return {
        ok: false,
        message: `Production-link action ${action} returned an empty response.`,
        detail: `Local production-link API returned HTTP ${response.status} ${response.statusText || "OK"} without a JSON body.`,
        state: await getDesktopState(),
      };
    }
    const payloadRecord = parseActionPayload(response.bodyText);
    const normalized = normalizeActionResult(action, response, payloadRecord);
    return {
      ...normalized,
      state: await getDesktopState(),
    };
  } catch (error) {
    setManagerError(summarizeErrorText(error instanceof Error ? error.message : String(error)));
    managerLifecycle = managerOwnsBackend ? "reconnecting" : "degraded";
    return {
      ok: false,
      message: `Failed to run production-link action ${action}.`,
      detail: error instanceof Error ? error.message : String(error),
      state,
    };
  }
}

export async function openPathInShell(targetPath: string): Promise<DesktopCommandResult> {
  if (!targetPath) {
    return { ok: false, message: "No path was provided." };
  }
  const opened = await shell.openPath(targetPath);
  return opened
    ? { ok: false, message: "Failed to open path.", detail: opened }
    : { ok: true, message: "Opened path in the OS shell." };
}

export async function openExternalUrl(url: string): Promise<DesktopCommandResult> {
  if (!url) {
    return { ok: false, message: "No URL was provided." };
  }
  await shell.openExternal(url);
  return { ok: true, message: "Opened URL in the default browser." };
}

export async function copyText(text: string): Promise<DesktopCommandResult> {
  if (!text) {
    return { ok: false, message: "No text was provided." };
  }
  const { clipboard } = await import("electron");
  clipboard.writeText(text);
  return { ok: true, message: "Copied diagnostics summary." };
}

export async function authenticateLocalOperator(reason?: string): Promise<DesktopCommandResult> {
  const authorization = await authorizeSensitiveAction(
    "dashboard",
    "local-operator-authenticate",
    { reason: reason ?? "Authenticate local operator access for this desktop session." },
  );
  if (!authorization.ok) {
    return authorization.result;
  }
  return {
    ok: true,
    message: "Local operator authentication succeeded.",
    detail: `Touch ID session active until ${authorization.authState.auth_session_expires_at ?? "unknown expiry"}.`,
    state: await getDesktopState(),
  };
}

export async function clearLocalOperatorAuthSession(): Promise<DesktopCommandResult> {
  const current = await buildLocalOperatorAuthState();
  const clearedAt = nowIso();
  await writeLocalOperatorAuthState({
    auth_available: current.auth_available,
    auth_platform: current.auth_platform,
    auth_method: current.auth_method,
    last_authenticated_at: current.last_authenticated_at,
    last_auth_result: current.last_auth_result,
    last_auth_detail: "Local operator auth session cleared manually.",
    auth_session_expires_at: null,
    auth_session_ttl_seconds: current.auth_session_ttl_seconds,
    auth_session_active: false,
    local_operator_identity: current.local_operator_identity,
    auth_session_id: null,
    touch_id_available: current.touch_id_available,
    updated_at: clearedAt,
    artifacts: localAuthArtifacts(),
    secret_protection: current.secret_protection,
  });
  await appendLocalOperatorAuthEvent({
    event_type: "local_operator_auth_session_cleared",
    occurred_at: clearedAt,
    local_operator_identity: current.local_operator_identity,
    auth_method: current.auth_method,
    authenticated_at: current.last_authenticated_at,
    auth_session_id: current.auth_session_id,
    operator_triggered: true,
    automatic: false,
    note: "Local operator auth session cleared manually.",
  });
  return {
    ok: true,
    message: "Local operator auth session cleared.",
    state: await getDesktopState(),
  };
}

export function shutdownDashboardManager(): void {
  shutdownRequested = true;
  stopWasRequested = true;
  managerOwnsBackend = false;
  managerLifecycle = "idle";
  clearReconnectTimer();
  if (dashboardManager && !dashboardManager.killed) {
    dashboardManager.kill("SIGTERM");
  }
}
