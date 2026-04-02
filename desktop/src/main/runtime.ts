import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { appendFileSync, mkdirSync } from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import { safeStorage, shell, systemPreferences } from "electron";
import packageJson from "../../package.json";

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
    mode: "live_api" | "snapshot_fallback" | "degraded_reconnecting" | "backend_down";
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
    startupFailureKind:
      | "none"
      | "permission_denied"
      | "port_in_use"
      | "conflicting_dashboard"
      | "backend_not_ready"
      | "unexpected_bind_error";
    actionHint: string | null;
  };
  startup: {
    preferredHost: string;
    preferredPort: number;
    preferredUrl: string;
    allowPortFallback: boolean;
    chosenHost: string | null;
    chosenPort: number | null;
    chosenUrl: string | null;
    ownership: "attached_existing" | "started_managed" | "snapshot_only" | "unavailable";
    latestEvent: string | null;
    recentEvents: string[];
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

const DESKTOP_ROOT = path.resolve(__dirname, "..", "..");
const REPO_ROOT = path.resolve(DESKTOP_ROOT, "..");
const OUTPUT_ROOT = path.join(REPO_ROOT, "outputs", "operator_dashboard");
const RUNTIME_ROOT = path.join(OUTPUT_ROOT, "runtime");
const DEFAULT_INFO_FILE = path.join(RUNTIME_ROOT, "operator_dashboard.json");
const DEFAULT_LOG_FILE = path.join(RUNTIME_ROOT, "operator_dashboard.log");
const DESKTOP_LOG_FILE = path.join(RUNTIME_ROOT, "desktop_electron.log");
const LOCAL_OPERATOR_AUTH_STATE_FILE = path.join(OUTPUT_ROOT, "local_operator_auth_state.json");
const LOCAL_OPERATOR_AUTH_EVENTS_FILE = path.join(OUTPUT_ROOT, "local_operator_auth_events.jsonl");
const LOCAL_SECRET_WRAPPER_FILE = path.join(OUTPUT_ROOT, "local_secret_wrapper.json");
const DEFAULT_DASHBOARD_HOST = process.env.MGC_OPERATOR_DASHBOARD_HOST || "127.0.0.1";
const DEFAULT_DASHBOARD_PORT = Number(process.env.MGC_OPERATOR_DASHBOARD_PORT || 8790);
const DEFAULT_DASHBOARD_URL = `http://${DEFAULT_DASHBOARD_HOST}:${DEFAULT_DASHBOARD_PORT}/`;
const ALLOW_PORT_FALLBACK = process.env.MGC_OPERATOR_DASHBOARD_ALLOW_PORT_FALLBACK === "1";
const LOCAL_OPERATOR_AUTH_SESSION_TTL_SECONDS = Math.max(
  60,
  Number(process.env.MGC_LOCAL_OPERATOR_AUTH_SESSION_TTL_SECONDS || 300),
);
const EXPLICIT_DASHBOARD_URLS = String(process.env.MGC_OPERATOR_DASHBOARD_URLS || "")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);
const SNAPSHOT_FILES = {
  historicalPlayback: path.join(OUTPUT_ROOT, "historical_playback_snapshot.json"),
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
  treasuryCurve: path.join(OUTPUT_ROOT, "treasury_curve_snapshot.json"),
  actionLog: path.join(OUTPUT_ROOT, "action_log.jsonl"),
  productionLink: path.join(OUTPUT_ROOT, "production_link_snapshot.json"),
};
const HEALTH_TIMEOUT_MS = 5000;
const DASHBOARD_TIMEOUT_MS = 45000;
const DASHBOARD_STARTUP_TIMEOUT_MS = 120000;
const RECONNECT_BACKOFF_MS = [2000, 5000, 10000, 20000, 30000];

let dashboardManager: ChildProcess | null = null;
let recentManagerOutput: string[] = [];
let lastExitCode: number | null = null;
let lastExitSignal: string | null = null;
let desktopStateRequestPromise: Promise<DesktopState> | null = null;
let dashboardLaunchPromise: Promise<DesktopState> | null = null;
let reconnectTimer: NodeJS.Timeout | null = null;
let reconnectAttemptCount = 0;
let nextRetryAt: string | null = null;
let managerLastError: string | null = null;
let managerLifecycle: "idle" | "starting" | "healthy" | "reconnecting" | "degraded" = "idle";
let managerOwnsBackend = false;
let stopWasRequested = false;
let shutdownRequested = false;

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
  const availability = localAuthAvailability();
  const stored = asJsonRecord(await readStoredLocalOperatorAuthState());
  const secretProtection = await ensureLocalSecretWrapper();
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
  if (authState.auth_session_active && authState.auth_session_expires_at && authState.local_operator_identity) {
    await appendLocalOperatorAuthEvent({
      event_type: "sensitive_action_authorized",
      occurred_at: nowIso(),
      action_kind: kind,
      action,
      instrument,
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
  mkdirSync(RUNTIME_ROOT, { recursive: true });
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
      managerLifecycle = "reconnecting";
      setManagerError(
        `Managed dashboard process exited unexpectedly${code !== null ? ` with code ${code}` : ""}${signal ? ` (${signal})` : ""}.`,
      );
      scheduleReconnect();
      return;
    }
    managerLifecycle = "degraded";
  });
  child.on("error", (error) => {
    if (dashboardManager === child) {
      dashboardManager = null;
    }
    managerLifecycle = "degraded";
    setManagerError(`Managed dashboard process error: ${error.message}`);
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

async function readJsonFile<T = JsonRecord>(filePath: string): Promise<T | null> {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
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
  for (const infoFile of [DEFAULT_INFO_FILE, ...infoFiles]) {
    const payload = await readJsonFile<{ url?: string }>(infoFile);
    if (payload?.url) {
      urls.push(payload.url);
    }
  }
  return {
    urls: Array.from(new Set(urls.map((url) => normalizeBaseUrl(url)))),
    infoFiles: Array.from(new Set([DEFAULT_INFO_FILE, ...infoFiles])),
  };
}

function classifyStartupFailure(detail: string | null): {
  kind: DesktopState["backend"]["startupFailureKind"];
  hint: string | null;
} {
  const text = String(detail ?? "").toLowerCase();
  if (!text) {
    return { kind: "none", hint: null };
  }
  if (text.includes("permission was denied") || text.includes("operation not permitted") || text.includes("eacces")) {
    return {
      kind: "permission_denied",
      hint: "Local bind permission was denied. Retry from a normal desktop shell and confirm localhost binds are allowed on this machine.",
    };
  }
  if (text.includes("build mismatch") || text.includes("existing dashboard process is not ready") || text.includes("could not stop the old dashboard instance cleanly")) {
    return {
      kind: "conflicting_dashboard",
      hint: "A different or stale dashboard instance is conflicting with this build. Stop the old dashboard first, then retry.",
    };
  }
  if (text.includes("port conflict") || text.includes("port is already in use") || text.includes("listener pid")) {
    return {
      kind: "port_in_use",
      hint: "The preferred dashboard port is already occupied. Stop the conflicting listener or choose a different configured port before retrying.",
    };
  }
  if (
    text.includes("did not become responsive") ||
    text.includes("failed to become healthy before timeout") ||
    text.includes("api never became ready") ||
    text.includes("not become responsive before timeout") ||
    text.includes("dashboard_snapshot_failed") ||
    text.includes("jsondecodeerror") ||
    text.includes("timed out after")
  ) {
    return {
      kind: "backend_not_ready",
      hint: "The backend process started but did not become ready in time. Check the backend log and retry after the underlying service issue is resolved.",
    };
  }
  return {
    kind: "unexpected_bind_error",
    hint: "The desktop app hit an unexpected local startup error. Check the backend and desktop logs, then retry with the configured host and port.",
  };
}

function buildStartupState({
  dashboard,
  health,
  backendUrl,
}: {
  dashboard: JsonRecord | null;
  health: JsonRecord | null;
  backendUrl: string | null;
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
  return {
    preferredHost: DEFAULT_DASHBOARD_HOST,
    preferredPort: DEFAULT_DASHBOARD_PORT,
    preferredUrl: DEFAULT_DASHBOARD_URL,
    allowPortFallback: ALLOW_PORT_FALLBACK,
    chosenHost,
    chosenPort,
    chosenUrl,
    ownership,
    latestEvent: recentManagerOutput.length ? recentManagerOutput[recentManagerOutput.length - 1] : null,
    recentEvents: recentManagerOutput.slice(-12),
  };
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

async function loadLiveDashboard(
  urls: string[],
): Promise<
  | { mode: "live"; url: string; health: JsonRecord; dashboard: JsonRecord }
  | { mode: "health-only"; url: string; health: JsonRecord; error: string }
  | null
> {
  let healthOnlyFallback: { mode: "health-only"; url: string; health: JsonRecord; error: string } | null = null;
  for (const baseUrl of urls) {
    try {
      const health = await fetchJson<JsonRecord>(new URL("health", baseUrl).toString(), HEALTH_TIMEOUT_MS);
      try {
        const dashboard = await fetchJson<JsonRecord>(new URL("api/dashboard", baseUrl).toString(), DASHBOARD_TIMEOUT_MS);
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

async function loadSnapshotBundle(): Promise<JsonRecord | null> {
  const [
    historicalPlayback,
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
    treasuryCurve,
    productionLink,
    actionLog,
  ] = await Promise.all([
    readJsonFile(SNAPSHOT_FILES.historicalPlayback),
    readJsonFile(SNAPSHOT_FILES.marketIndexStrip),
    readJsonFile(SNAPSHOT_FILES.operatorSurface),
    readJsonFile(SNAPSHOT_FILES.paperApprovedModels),
    readJsonFile(SNAPSHOT_FILES.paperBlotter),
    readJsonFile(SNAPSHOT_FILES.paperCarryForward),
    readJsonFile(SNAPSHOT_FILES.paperFills),
    readJsonFile(SNAPSHOT_FILES.paperIntents),
    readJsonFile(SNAPSHOT_FILES.paperLaneActivity),
    readJsonFile(SNAPSHOT_FILES.paperNonApprovedLanes),
    readJsonFile(SNAPSHOT_FILES.paperPerformance),
    readJsonFile(SNAPSHOT_FILES.paperPosition),
    readJsonFile(SNAPSHOT_FILES.paperReadiness),
    readJsonFile(SNAPSHOT_FILES.treasuryCurve),
    readJsonFile(SNAPSHOT_FILES.productionLink),
    readActionLog(),
  ]);

  if (!operatorSurface) {
    return null;
  }

  const readinessValues = ((operatorSurface.runtime_readiness as JsonRecord | undefined)?.values ?? {}) as JsonRecord;
  const entriesEnabled = Boolean((paperReadiness as JsonRecord | null)?.entries_enabled ?? readinessValues.entries_enabled);
  const runtimeRunning = Boolean((paperReadiness as JsonRecord | null)?.runtime_running ?? (readinessValues.runtime_status === "RUNNING"));
  const blockingFaultsCount = Number(readinessValues.blocking_faults_count ?? 0);

  return {
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
    production_link: productionLink,
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
  dashboard,
  health,
  infoFiles,
}: {
  live:
    | { mode: "live"; url: string; health: JsonRecord; dashboard: JsonRecord }
    | { mode: "health-only"; url: string; health: JsonRecord; error: string }
    | null;
  snapshotAvailable: boolean;
  dashboard: JsonRecord | null;
  health: JsonRecord | null;
  infoFiles: string[];
}): Pick<DesktopState, "source" | "backend" | "connection"> {
  const healthReachable = Boolean(live);
  const apiReachable = live?.mode === "live";
  const staleInfoFile = infoFiles.length > 0 && !healthReachable;
  const pid = readDashboardPid(dashboard, health);
  const activeManagedLifecycle = reconnectTimer ? "reconnecting" : dashboardLaunchPromise ? "starting" : managerLifecycle;
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
  const failure = classifyStartupFailure(currentError);

  if (apiReachable) {
    managerLifecycle = "healthy";
    reconnectAttemptCount = 0;
    clearReconnectTimer();
    setManagerError(null);
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
        state: "healthy",
        label: "HEALTHY",
        detail: "Backend health and dashboard API are both responding.",
        lastError: null,
        nextRetryAt,
        retryCount: reconnectAttemptCount,
        pid,
        apiStatus,
        healthStatus,
        managerOwned: managerOwnsBackend,
        startupFailureKind: "none",
        actionHint: null,
      },
    };
  }

  if (activeManagedLifecycle === "starting") {
    return {
      connection: snapshotAvailable ? "snapshot" : "unavailable",
      source: {
        mode: "degraded_reconnecting",
        label: "DEGRADED / RECONNECTING",
        detail: "Backend start is in progress; snapshot fallback remains active until the live API is fully ready.",
        canRunLiveActions: false,
        healthReachable,
        apiReachable: false,
      },
      backend: {
        state: "starting",
        label: "STARTING",
        detail: "Dashboard manager is starting the local backend and waiting for readiness.",
        lastError: managerLastError ?? (live?.mode === "health-only" ? live.error : null),
        nextRetryAt,
        retryCount: reconnectAttemptCount,
        pid,
        apiStatus,
        healthStatus,
        managerOwned: managerOwnsBackend,
        startupFailureKind: failure.kind,
        actionHint: failure.hint,
      },
    };
  }

  if (activeManagedLifecycle === "reconnecting") {
    return {
      connection: snapshotAvailable ? "snapshot" : "unavailable",
      source: {
        mode: "degraded_reconnecting",
        label: "DEGRADED / RECONNECTING",
        detail: "The managed backend is reconnecting after an exit or slow recovery; snapshot fallback is active.",
        canRunLiveActions: false,
        healthReachable,
        apiReachable: false,
      },
      backend: {
        state: "reconnecting",
        label: "RECONNECTING",
        detail: nextRetryAt
          ? `Next reconnect attempt scheduled for ${nextRetryAt}.`
          : "Reconnect recovery is active for the managed backend.",
        lastError: managerLastError ?? (live?.mode === "health-only" ? live.error : null),
        nextRetryAt,
        retryCount: reconnectAttemptCount,
        pid,
        apiStatus,
        healthStatus,
        managerOwned: managerOwnsBackend,
        startupFailureKind: failure.kind,
        actionHint: failure.hint,
      },
    };
  }

  if (live?.mode === "health-only") {
    return {
      connection: snapshotAvailable ? "snapshot" : "unavailable",
      source: {
        mode: "degraded_reconnecting",
        label: "DEGRADED / RECONNECTING",
        detail: `Live /health is reachable at ${live.url}, but /api/dashboard is not completing within ${DASHBOARD_TIMEOUT_MS / 1000}s.`,
        canRunLiveActions: false,
        healthReachable: true,
        apiReachable: false,
      },
      backend: {
        state: "degraded",
        label: "DEGRADED",
        detail: "Backend is up enough to answer /health, but the full dashboard payload is not responsive.",
        lastError: live.error,
        nextRetryAt,
        retryCount: reconnectAttemptCount,
        pid,
        apiStatus,
        healthStatus,
        managerOwned: managerOwnsBackend,
        startupFailureKind: classifyStartupFailure(live.error).kind,
        actionHint: classifyStartupFailure(live.error).hint,
      },
    };
  }

  if (snapshotAvailable) {
    return {
      connection: "snapshot",
      source: {
        mode: "snapshot_fallback",
        label: "SNAPSHOT FALLBACK",
        detail: staleInfoFile
          ? "Using persisted operator snapshots because the stored backend endpoint is stale or unreachable."
          : "Using persisted operator snapshots because no live dashboard API is currently available.",
        canRunLiveActions: false,
        healthReachable: false,
        apiReachable: false,
      },
      backend: {
        state: "backend_down",
        label: "BACKEND DOWN",
        detail: staleInfoFile
          ? "Stored dashboard info exists, but the backend did not answer health checks."
          : "No live backend answered; the app is running from the latest persisted dashboard artifacts.",
        lastError: managerLastError,
        nextRetryAt,
        retryCount: reconnectAttemptCount,
        pid,
        apiStatus,
        healthStatus,
        managerOwned: managerOwnsBackend,
        startupFailureKind: classifyStartupFailure(managerLastError).kind,
        actionHint: classifyStartupFailure(managerLastError).hint,
      },
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
    backend: {
      state: "backend_down",
      label: "BACKEND DOWN",
      detail: "No backend answered health checks and no persisted snapshot bundle was available.",
      lastError: managerLastError,
      nextRetryAt,
      retryCount: reconnectAttemptCount,
      pid,
      apiStatus,
      healthStatus,
      managerOwned: managerOwnsBackend,
      startupFailureKind: classifyStartupFailure(managerLastError).kind,
      actionHint: classifyStartupFailure(managerLastError).hint,
    },
  };
}

async function waitForLiveDashboard(timeoutMs = DASHBOARD_STARTUP_TIMEOUT_MS): Promise<DesktopState> {
  const deadline = Date.now() + timeoutMs;
  let latestState = await getDesktopState();
  while (Date.now() < deadline) {
    if (latestState.connection === "live") {
      return latestState;
    }
    if (
      managerLastError &&
      (latestState.backend.state === "reconnecting" ||
        latestState.backend.state === "degraded" ||
        latestState.backend.state === "backend_down")
    ) {
      return latestState;
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
    latestState = await getDesktopState();
  }
  return latestState;
}

async function execScript(args: string[]): Promise<{ ok: boolean; stdout: string; stderr: string; code: number | null }> {
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

async function probeDesktopState(): Promise<DesktopState> {
  const localAuth = await buildLocalOperatorAuthState();
  const { urls, infoFiles } = await candidateUrls();
  const errors: string[] = [];
  const live = await loadLiveDashboard(urls);
  const liveDashboard = live?.mode === "live" ? live.dashboard : null;
  const liveHealth = live?.mode === "live" ? live.health : live?.mode === "health-only" ? live.health : null;
  const snapshot = liveDashboard ? null : await loadSnapshotBundle();
  const dashboard = liveDashboard ?? snapshot;
  const runtimeStates = buildRuntimeStates({
    live,
    snapshotAvailable: Boolean(snapshot),
    dashboard,
    health: liveHealth,
    infoFiles,
  });

  if (live?.mode === "live" && dashboard) {
    return {
      connection: runtimeStates.connection,
      dashboard,
      health: live.health,
      backendUrl: live.url,
      source: runtimeStates.source,
      backend: runtimeStates.backend,
      startup: buildStartupState({
        dashboard,
        health: live.health,
        backendUrl: live.url,
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
  }

  if (snapshot) {
    if (live?.mode === "health-only") {
      errors.push(
        `Live dashboard health is reachable at ${live.url}, but /api/dashboard did not return within ${DASHBOARD_TIMEOUT_MS / 1000}s; showing latest persisted operator snapshots.`,
      );
    } else {
      errors.push("Live dashboard API is unavailable; showing latest persisted operator snapshots.");
    }
    return {
      connection: runtimeStates.connection,
      dashboard: snapshot,
      health: live?.mode === "health-only" ? live.health : null,
      backendUrl: live?.mode === "health-only" ? live.url : null,
      source: runtimeStates.source,
      backend: runtimeStates.backend,
      startup: buildStartupState({
        dashboard: snapshot,
        health: live?.mode === "health-only" ? live.health : null,
        backendUrl: live?.mode === "health-only" ? live.url : null,
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
  }

  errors.push("No live dashboard API responded and no persisted operator snapshots were available.");
  return {
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
}

export async function getDesktopState(): Promise<DesktopState> {
  if (desktopStateRequestPromise) {
    return desktopStateRequestPromise;
  }
  desktopStateRequestPromise = probeDesktopState().finally(() => {
    desktopStateRequestPromise = null;
  });
  return desktopStateRequestPromise;
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

  const state = await beginDashboardLaunch({ manual: true });
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
  managerLifecycle = state.backend.state === "reconnecting" ? "reconnecting" : "degraded";
  const failureState = await getDesktopState();
  return {
    ok: false,
    message:
      failureState.backend.state === "reconnecting"
        ? "Dashboard/API failed to come up cleanly; automatic reconnect is scheduled and snapshot fallback remains active."
        : failureState.health && failureState.backendUrl
          ? "Dashboard listener started, but /api/dashboard did not become responsive before timeout; snapshot fallback remains active."
          : "Dashboard launch command ran, but the API never became ready.",
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
    const response = await fetch(new URL(`api/action/${action}`, state.backendUrl).toString(), {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(authorizedPayload),
    });
    const responsePayload = (await response.json()) as JsonRecord;
    return {
      ok: response.ok && Boolean(responsePayload.ok ?? true),
      message: String(responsePayload.action_label ?? responsePayload.action ?? action),
      detail: String(responsePayload.message ?? ""),
      output: String(responsePayload.output ?? responsePayload.message ?? ""),
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
    const response = await fetch(new URL(`api/production-link/${action}`, state.backendUrl).toString(), {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(authorizedPayload),
    });
    const bodyText = await response.text();
    const payloadRecord = bodyText ? (JSON.parse(bodyText) as JsonRecord) : {};
    return {
      ok: response.ok && Boolean(payloadRecord.ok ?? true),
      message: String(payloadRecord.action_label ?? payloadRecord.action ?? action),
      detail: String(payloadRecord.message ?? ""),
      output: String(payloadRecord.output ?? payloadRecord.message ?? ""),
      payload: payloadRecord,
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
