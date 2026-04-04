import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties, type ReactNode } from "react";
import type { DesktopCommandResult, DesktopState, JsonRecord, OperatorDesktopApi } from "./types";
import {
  asArray,
  asRecord,
  formatMaybePnL,
  formatRelativeAge,
  formatShortNumber,
  formatTimestamp,
  formatValue,
  sentenceCase,
} from "./lib/format";

type PageId =
  | "home"
  | "calendar"
  | "runtime"
  | "strategies"
  | "positions"
  | "market"
  | "replay"
  | "logs"
  | "configuration"
  | "diagnostics"
  | "settings";

type Tone = "good" | "warn" | "danger" | "muted";

interface AppSettings {
  refreshSeconds: number;
  defaultPage: PageId;
  showDiagnostics: boolean;
}

interface RecentAction {
  id: string;
  label: string;
  ok: boolean;
  message: string;
  detail?: string;
  occurredAt: string;
}

interface ManualOrderFormState {
  accountHash: string;
  symbol: string;
  assetClass: string;
  structureType: string;
  intentType: string;
  side: string;
  quantity: string;
  orderType: string;
  limitPrice: string;
  stopPrice: string;
  trailValueType: string;
  trailValue: string;
  trailTriggerBasis: string;
  trailLimitOffset: string;
  timeInForce: string;
  session: string;
  operatorNote: string;
  reviewConfirmed: boolean;
  ocoLegs: OcoLegFormState[];
}

interface OcoLegFormState {
  legLabel: string;
  side: string;
  quantity: string;
  orderType: string;
  limitPrice: string;
  stopPrice: string;
  trailValueType: string;
  trailValue: string;
  trailTriggerBasis: string;
  trailLimitOffset: string;
}

interface PreflightCheck {
  key: string;
  label: string;
  status: "pass" | "warn" | "fail";
  value: string;
  detail: string;
}

interface PreflightModel {
  verdict: "READY" | "DEGRADED" | "NOT READY";
  checks: PreflightCheck[];
  blockers: string[];
  warnings: string[];
  informational: string[];
}

interface FirstLiveStockLimitCheck {
  label: string;
  ok: boolean;
  detail: string;
}

type PositionsViewMode = "broker" | "paper" | "combined";
type PositionsDrawerTab = "summary" | "trades" | "orders" | "attribution" | "margin" | "conflict" | "activity" | "instrument";
type SortDirection = "asc" | "desc";
type PositionsSourceClass = "BROKER" | "PAPER" | "EXPERIMENTAL";
type PnlCalendarPeriod = "monthly" | "weekly" | "quarterly" | "ytd" | "custom";
type PnlCalendarViewMode = "calendar" | "line" | "bar";
type PnlCalendarSource = "all" | "live" | "paper" | "benchmark_replay" | "research_execution";

interface CalendarSourceEntry {
  source: Exclude<PnlCalendarSource, "all">;
  date: string;
  laneId: string | null;
  strategyId: string;
  strategyName: string;
  pnl: number;
  tradeCount: number;
}

interface CalendarStrategyContribution {
  source: Exclude<PnlCalendarSource, "all">;
  laneId: string | null;
  strategyId: string;
  strategyName: string;
  pnl: number;
  tradeCount: number;
}

interface CalendarDayPoint {
  date: string;
  pnl: number;
  tradeCount: number;
  cumulative: number;
  contributions: CalendarStrategyContribution[];
  coveredSources: Array<Exclude<PnlCalendarSource, "all" | "live" | "paper">>;
}

interface PositionsSortState {
  columnId: string;
  direction: SortDirection;
}

interface PositionsLayoutState {
  currentColumnsByMode: Record<PositionsViewMode, string[]>;
  savedLayoutsByMode: Record<PositionsViewMode, Record<string, string[]>>;
}

interface PositionsMetricItem {
  label: string;
  value: ReactNode;
  tone?: Tone;
}

interface PositionsMonitorRow {
  id: string;
  symbol: string;
  description: string;
  displaySymbol?: string;
  displayDescription?: string;
  sourceBadges?: PositionsSourceClass[];
  exposureMarker?: "LONG" | "SHORT" | "BOTH" | null;
  childRows?: PositionsMonitorRow[];
  isSpreadParent?: boolean;
  isSpreadLeg?: boolean;
  spreadKey?: string | null;
  spreadLabel?: string | null;
  brokerRows: JsonRecord[];
  paperRows: JsonRecord[];
  approvedPaperRows: JsonRecord[];
  experimentalRows: JsonRecord[];
  sameUnderlyingRows: JsonRecord[];
  brokerOrders: JsonRecord[];
  brokerFills: JsonRecord[];
  brokerEvents: JsonRecord[];
  closedTrades: JsonRecord[];
  brokerQty: number | null;
  paperQty: number | null;
  brokerAvgPrice: number | null;
  paperAvgEntry: number | null;
  brokerMark: number | null;
  paperMark: number | null;
  brokerDayPnl: number | null;
  paperDayPnl: number | null;
  brokerOpenPnl: number | null;
  paperOpenPnl: number | null;
  brokerRealized: number | null;
  paperRealized: number | null;
  brokerYtdPnl: number | null;
  brokerMarketValue: number | null;
  brokerMarginEffect: number | null;
  brokerDelta: number | null;
  brokerTheta: number | null;
  strategyCount: number;
  tradeCount: number;
  maxDrawdown: number | null;
  primaryStrategy: string;
  paperClass: PositionsSourceClass | null;
  currentStatus: string;
  conflict: string;
  conflictState: string;
  reviewState: string;
  session: string;
  runtimeLoaded: string;
  entryHold: string;
  lastActivity: string | null;
  latestIntentTime: string | null;
  latestFillTime: string | null;
  latestTradeTime: string | null;
  latestBrokerUpdateTime: string | null;
  gamma: number | null;
  vega: number | null;
  iv: number | null;
  ivPercentile: number | null;
  daysToExp: number | null;
  roc: number | null;
  yieldValue: number | null;
  expectedMove: number | null;
  quoteTrend: string | null;
  initialMargin: number | null;
  probItm: number | null;
  probOtm: number | null;
  extrinsic: number | null;
  intrinsic: number | null;
}

interface PositionsMonitorColumn {
  id: string;
  label: string;
  align?: "left" | "right";
  sticky?: boolean;
  hideable?: boolean;
  render: (row: PositionsMonitorRow) => ReactNode;
  sortValue: (row: PositionsMonitorRow) => string | number | null;
}

const DEFAULT_SETTINGS: AppSettings = {
  refreshSeconds: 15,
  defaultPage: "home",
  showDiagnostics: true,
};

const POSITIONS_LAYOUT_STORAGE_KEY = "mgc.operatorDesktop.positionsLayouts.v1";
const POSITIONS_VIEW_LABELS: Record<PositionsViewMode, string> = {
  broker: "Broker / Real Money",
  paper: "Paper / Experimental",
  combined: "Combined",
};
const POSITIONS_DEFAULT_COLUMNS: Record<PositionsViewMode, string[]> = {
  broker: ["symbol", "description", "qty", "avgPrice", "mark", "dayPnl", "openPnl", "ytdPnl", "marketValue", "marginEffect", "delta", "theta", "lastActivity"],
  paper: ["symbol", "strategyCount", "netQty", "avgEntry", "mark", "dayPnl", "openPnl", "realizedPnl", "tradeCount", "maxDrawdown", "primaryStrategy", "class", "lastActivity", "currentStatus"],
  combined: ["symbol", "brokerQty", "paperQty", "brokerOpenPnl", "paperOpenPnl", "paperRealized", "netValue", "conflict", "lastActivity"],
};
const POSITIONS_AVAILABLE_COLUMNS: Record<PositionsViewMode, string[]> = {
  broker: [
    "symbol",
    "description",
    "qty",
    "avgPrice",
    "mark",
    "dayPnl",
    "openPnl",
    "ytdPnl",
    "marketValue",
    "marginEffect",
    "delta",
    "theta",
    "lastActivity",
    "gamma",
    "vega",
    "iv",
    "ivPercentile",
    "daysToExp",
    "roc",
    "yield",
    "expectedMove",
    "quoteTrend",
    "initialMargin",
    "probItm",
    "probOtm",
    "extrinsic",
    "intrinsic",
    "runtimeLoaded",
    "session",
    "entryHold",
    "reviewState",
    "latestIntentTime",
    "latestFillTime",
    "latestTradeTime",
    "conflictState",
  ],
  paper: [
    "symbol",
    "strategyCount",
    "netQty",
    "avgEntry",
    "mark",
    "dayPnl",
    "openPnl",
    "realizedPnl",
    "tradeCount",
    "maxDrawdown",
    "primaryStrategy",
    "class",
    "lastActivity",
    "currentStatus",
    "gamma",
    "vega",
    "iv",
    "ivPercentile",
    "daysToExp",
    "roc",
    "yield",
    "expectedMove",
    "quoteTrend",
    "initialMargin",
    "probItm",
    "probOtm",
    "extrinsic",
    "intrinsic",
    "runtimeLoaded",
    "session",
    "entryHold",
    "reviewState",
    "latestIntentTime",
    "latestFillTime",
    "latestTradeTime",
    "conflictState",
  ],
  combined: [
    "symbol",
    "brokerQty",
    "paperQty",
    "brokerOpenPnl",
    "paperOpenPnl",
    "paperRealized",
    "combinedRealized",
    "marketValue",
    "marginEffect",
    "netValue",
    "conflict",
    "lastActivity",
    "gamma",
    "vega",
    "iv",
    "ivPercentile",
    "daysToExp",
    "roc",
    "yield",
    "expectedMove",
    "quoteTrend",
    "initialMargin",
    "probItm",
    "probOtm",
    "extrinsic",
    "intrinsic",
    "runtimeLoaded",
    "session",
    "entryHold",
    "reviewState",
    "latestIntentTime",
    "latestFillTime",
    "latestTradeTime",
    "strategyCount",
    "conflictState",
  ],
};

const NAV_ITEMS: Array<{ id: PageId; label: string }> = [
  { id: "home", label: "Dashboard" },
  { id: "calendar", label: "P&L Calendar" },
  { id: "positions", label: "Live P&L" },
  { id: "market", label: "Trade Entry" },
  { id: "strategies", label: "Strategy Deep-Dive" },
  { id: "diagnostics", label: "Evidence" },
  { id: "settings", label: "Settings" },
];

const DEMOTED_PRIMARY_SECTION_TITLES = new Set([
  "Strategy Roster Table",
  "Strategy Risk Context",
  "Market Context",
  "Instrument Rollup Preview",
  "Active Lane Preview",
  "Current Active Positions",
  "Strategy Analysis Lab",
  "Standalone Strategy Lens",
  "Same-Underlying Review Events",
  "Live Eligibility",
  "Strategy Performance",
  "Expected Fire / Historical Cadence",
  "Signal / Intent / Fill Audit",
  "Strategy Trade Log",
  "Attribution Summary",
  "Positions Monitor",
  "Broker Orders and Fills",
]);

const PRIMARY_WORKSTATION_PAGES = new Set<PageId>(["home", "calendar", "positions", "market", "strategies"]);

const ATP_PRODUCT_CATALOG: Array<{
  laneId: string;
  standaloneStrategyId: string;
  trackedStrategyId: string;
  displayName: string;
  instrument: string;
  designation: "benchmark" | "candidate";
  experimentalStatus: string;
  participationPolicy: string;
  candidateId?: string;
}> = [
  {
    laneId: "atp_companion_v1_asia_us",
    standaloneStrategyId: "atp_companion_v1__benchmark_mgc_asia_us",
    trackedStrategyId: "atp_companion_v1_asia_us",
    displayName: "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only",
    instrument: "MGC",
    designation: "benchmark",
    experimentalStatus: "tracked_paper_benchmark",
    participationPolicy: "SINGLE_ENTRY_ONLY",
  },
  {
    laneId: "atp_companion_v1_gc_asia_us",
    standaloneStrategyId: "atp_companion_v1__paper_gc_asia_us",
    trackedStrategyId: "atp_companion_v1__paper_gc_asia_us",
    displayName: "ATP Companion Candidate v1 — GC / Asia + US Executable, London Diagnostic-Only",
    instrument: "GC",
    designation: "candidate",
    experimentalStatus: "paper_candidate",
    participationPolicy: "STAGED_SAME_DIRECTION",
  },
  {
    laneId: "atp_companion_v1_pl_asia_us",
    standaloneStrategyId: "atp_companion_v1__paper_pl_asia_us",
    trackedStrategyId: "atp_companion_v1__paper_pl_asia_us",
    displayName: "ATP Companion Candidate v1 — PL / Asia + US Executable, London Diagnostic-Only",
    instrument: "PL",
    designation: "candidate",
    experimentalStatus: "paper_candidate",
    participationPolicy: "STAGED_SAME_DIRECTION",
  },
  {
    laneId: "atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only",
    standaloneStrategyId: "atp_companion_v1__paper_mgc_asia__promotion_1_075r_favorable_only",
    trackedStrategyId: "atp_companion_v1__paper_mgc_asia__promotion_1_075r_favorable_only",
    displayName: "ATP Companion Candidate — MGC / Asia Only / Promotion 1 +0.75R Favorable Only",
    instrument: "MGC",
    designation: "candidate",
    experimentalStatus: "paper_candidate",
    participationPolicy: "STAGED_SAME_DIRECTION",
    candidateId: "promotion_1_075r_favorable_only",
  },
  {
    laneId: "atp_companion_v1_gc_asia_promotion_1_075r_favorable_only",
    standaloneStrategyId: "atp_companion_v1__paper_gc_asia__promotion_1_075r_favorable_only",
    trackedStrategyId: "atp_companion_v1__paper_gc_asia__promotion_1_075r_favorable_only",
    displayName: "ATP Companion Candidate — GC / Asia Only / Promotion 1 +0.75R Favorable Only",
    instrument: "GC",
    designation: "candidate",
    experimentalStatus: "paper_candidate",
    participationPolicy: "STAGED_SAME_DIRECTION",
    candidateId: "promotion_1_075r_favorable_only",
  },
];

const EVIDENCE_ONLY_SECTION_TITLES = new Set([
  "Lane Detail",
  "ATP / Temp Paper Truth",
  "Strategy Roster Table",
  "Desktop Startup",
  "Paper Runtime Launch",
  "Paper Soak Continuity",
  "Paper Soak Validation",
  "Live Shadow Runtime",
  "Live Strategy Pilot",
  "Broker Truth Shadow Validation",
  "Live Timing Summary",
  "Live Timing Validation",
  "Exit Parity Summary",
  "Extended Paper Soak",
  "Unattended Paper Soak",
  "Local Operator Auth",
  "Sunday Open Preflight",
  "Strategy Runtime Truth",
  "Same-Underlying Conflicts",
  "Portfolio P&L",
  "Replay / Backtest Truth",
  "Research History Capture",
  "Broker Portfolio Truth",
  "Operator Alerts",
  "Runtime / Readiness Context",
  "Strategy Risk Context",
  "Market Context",
  "Instrument Rollup Preview",
  "Active Lane Preview",
  "Current Active Positions",
  "Positions Monitor",
  "Closed Trades",
  "Manual Order Ticket",
  "Broker Orders and Fills",
  "Operator Results Board",
  "Historical Playback Context",
  "Strategy Analysis Lab",
  "Standalone Strategy Lens",
  "Standalone Strategy Registry",
  "Same-Underlying Conflict Table",
  "Same-Underlying Review Events",
  "Live Eligibility",
  "Strategy Performance",
  "Expected Fire / Historical Cadence",
  "Signal / Intent / Fill Audit",
  "Strategy Trade Log",
  "Attribution Summary",
  "Temp-Paper Runtime Integrity",
  "Paper Capture Integrity",
  "Runtime Identity",
  "Health / Readiness",
  "Manager Output",
]);

let currentSectionPageContext: PageId | null = null;

const POSITIONS_PAGE_POLL_SECONDS = 2;
const POSITIONS_PAGE_BROKER_REFRESH_SECONDS = 5;

const SUNDAY_RUNBOOK_RELATIVE_PATH = "outputs/reports/sunday_evening_operator_runbook.md";
const DEFAULT_MANUAL_ORDER_FORM: ManualOrderFormState = {
  accountHash: "",
  symbol: "",
  assetClass: "STOCK",
  structureType: "SINGLE",
  intentType: "MANUAL_LIVE_PILOT",
  side: "BUY",
  quantity: "1",
  orderType: "LIMIT",
  limitPrice: "",
  stopPrice: "",
  trailValueType: "AMOUNT",
  trailValue: "",
  trailTriggerBasis: "LAST",
  trailLimitOffset: "",
  timeInForce: "DAY",
  session: "NORMAL",
  operatorNote: "",
  reviewConfirmed: false,
  ocoLegs: [
    {
      legLabel: "Profit Leg",
      side: "SELL",
      quantity: "1",
      orderType: "LIMIT",
      limitPrice: "",
      stopPrice: "",
      trailValueType: "AMOUNT",
      trailValue: "",
      trailTriggerBasis: "LAST",
      trailLimitOffset: "",
    },
    {
      legLabel: "Stop Leg",
      side: "SELL",
      quantity: "1",
      orderType: "STOP",
      limitPrice: "",
      stopPrice: "",
      trailValueType: "AMOUNT",
      trailValue: "",
      trailTriggerBasis: "LAST",
      trailLimitOffset: "",
    },
  ],
};

const API_FALLBACK: OperatorDesktopApi = {
  async getDesktopState() {
    return {
      connection: "unavailable",
      dashboard: null,
      health: null,
      backendUrl: null,
      source: {
        mode: "backend_down",
        label: "BACKEND DOWN",
        detail: "Electron preload bridge is unavailable in this renderer context.",
        canRunLiveActions: false,
        healthReachable: false,
        apiReachable: false,
      },
      backend: {
        state: "backend_down",
        label: "BACKEND DOWN",
        detail: "Electron preload bridge is unavailable in this renderer context.",
        lastError: "Electron preload bridge is unavailable in this renderer context.",
        nextRetryAt: null,
        retryCount: 0,
        pid: null,
        apiStatus: "unknown",
        healthStatus: "unknown",
        managerOwned: false,
        startupFailureKind: "unexpected_startup_failure",
        actionHint: "Run the desktop app from Electron so the preload bridge is available.",
        staleListenerDetected: false,
        healthReachable: false,
        dashboardApiTimedOut: false,
        portConflictDetected: false,
      },
      startup: {
        preferredHost: "127.0.0.1",
        preferredPort: 8790,
        preferredUrl: "http://127.0.0.1:8790/",
        allowPortFallback: false,
        chosenHost: null,
        chosenPort: null,
        chosenUrl: null,
        ownership: "unavailable",
        latestEvent: "Electron preload bridge is unavailable in this renderer context.",
        recentEvents: [],
        failureKind: "unexpected_startup_failure",
        recommendedAction: "Run the desktop app from Electron so the preload bridge is available.",
        staleListenerDetected: false,
        healthReachable: false,
        dashboardApiTimedOut: false,
        managedExitCode: null,
        managedExitSignal: null,
      },
      infoFiles: [],
      errors: ["Electron preload bridge is unavailable in this renderer context."],
      runtimeLogPath: null,
      backendLogPath: null,
      desktopLogPath: null,
      appVersion: "0.0.0",
      manager: {
        running: false,
        lastExitCode: null,
        lastExitSignal: null,
        recentOutput: [],
      },
      localAuth: {
        auth_available: false,
        auth_platform: "unavailable",
        auth_method: "NONE",
        last_authenticated_at: null,
        last_auth_result: "UNAVAILABLE",
        last_auth_detail: "Electron preload bridge is unavailable in this renderer context.",
        auth_session_expires_at: null,
        auth_session_active: false,
        local_operator_identity: null,
        auth_session_id: null,
        touch_id_available: false,
        secret_protection: {
          available: false,
          provider: "NONE",
          wrapper_ready: false,
          wrapper_path: null,
          protects_token_file_directly: false,
          detail: "Electron preload bridge is unavailable in this renderer context.",
        },
        latest_event: null,
        recent_events: [],
        artifacts: {
          state_path: "",
          events_path: "",
          secret_wrapper_path: "",
        },
      },
      refreshedAt: new Date().toISOString(),
    };
  },
  async startDashboard() {
    return { ok: false, message: "Electron preload bridge is unavailable." };
  },
  async stopDashboard() {
    return { ok: false, message: "Electron preload bridge is unavailable." };
  },
  async restartDashboard() {
    return { ok: false, message: "Electron preload bridge is unavailable." };
  },
  async runDashboardAction() {
    return { ok: false, message: "Electron preload bridge is unavailable." };
  },
  async runProductionLinkAction() {
    return { ok: false, message: "Electron preload bridge is unavailable." };
  },
  async authenticateLocalOperator() {
    return { ok: false, message: "Electron preload bridge is unavailable." };
  },
  async clearLocalOperatorAuthSession() {
    return { ok: false, message: "Electron preload bridge is unavailable." };
  },
  async openPath() {
    return { ok: false, message: "Electron preload bridge is unavailable." };
  },
  async openExternalUrl() {
    return { ok: false, message: "Electron preload bridge is unavailable." };
  },
  async copyText() {
    return { ok: false, message: "Electron preload bridge is unavailable." };
  },
};

function deriveAdvancedModeLabel(timeInForce: string, session: string): string {
  if (session === "EXTO" && timeInForce === "GTC") {
    return "GTC_EXTO";
  }
  if (session === "EXTO") {
    return "EXTO";
  }
  if (session === "EXT") {
    return "EXT";
  }
  if (timeInForce === "GTC") {
    return "GTC";
  }
  return "STANDARD";
}

function isAdvancedDryRunMode(timeInForce: string, session: string, structureType: string): boolean {
  return structureType === "OCO" || ["EXT", "EXTO", "GTC_EXTO"].includes(deriveAdvancedModeLabel(timeInForce, session));
}

function deriveManualVerificationKey(form: ManualOrderFormState, advancedMode: string): string | null {
  if (form.structureType === "OCO") {
    return "ADVANCED:OCO";
  }
  if (advancedMode === "EXTO") {
    return "ADVANCED:EXTO";
  }
  if (advancedMode === "GTC_EXTO") {
    return "ADVANCED:GTC_EXTO";
  }
  if (!form.assetClass || !form.orderType) {
    return null;
  }
  return `${form.assetClass}:${form.orderType}`;
}

function verificationEntryByKey(matrix: JsonRecord, verificationKey: string | null): JsonRecord {
  if (!verificationKey) {
    return {};
  }
  const [assetClass, orderType] = verificationKey.split(":", 2);
  return asRecord(asRecord(matrix[assetClass])[orderType]);
}

function getApi(): OperatorDesktopApi {
  return window.operatorDesktop ?? API_FALLBACK;
}

function hashPage(defaultPage: PageId): PageId {
  const raw = window.location.hash.replace(/^#\/?/, "");
  const redirected =
    raw === "runtime"
      ? "home"
      : raw === "replay"
        ? "strategies"
        : raw === "logs" || raw === "configuration"
          ? "diagnostics"
          : raw;
  const matched = NAV_ITEMS.find((item) => item.id === redirected);
  return matched?.id ?? defaultPage;
}

function readSettings(): AppSettings {
  try {
    const raw = window.localStorage.getItem("mgc.operatorDesktop.settings.v1");
    return raw ? { ...DEFAULT_SETTINGS, ...JSON.parse(raw) } : DEFAULT_SETTINGS;
  } catch {
    return DEFAULT_SETTINGS;
  }
}

function writeSettings(settings: AppSettings): void {
  window.localStorage.setItem("mgc.operatorDesktop.settings.v1", JSON.stringify(settings));
}

function statusTone(label: unknown): "good" | "warn" | "danger" | "muted" {
  const text = String(label ?? "").toLowerCase();
  if (
    text.includes("ready") ||
    text.includes("running") ||
    text.includes("live") ||
    text.includes("enabled") ||
    text.includes("ok") ||
    text.includes("healthy") ||
    text.includes("filled") ||
    text.includes("responding") ||
    text.includes("pass") ||
    text.includes("succeeded")
  ) {
    return "good";
  }
  if (
    text.includes("starting") ||
    text.includes("progress") ||
    text.includes("reconnecting") ||
    text.includes("warning") ||
    text.includes("warn") ||
    text.includes("blocked") ||
    text.includes("gated") ||
    text.includes("snapshot") ||
    text.includes("degraded") ||
    text.includes("insufficient")
  ) {
    return "warn";
  }
  if (
    text.includes("halt") ||
    text.includes("fault") ||
    text.includes("stale") ||
    text.includes("stop") ||
    text.includes("down") ||
    text.includes("unreachable") ||
    text.includes("timed_out") ||
    text.includes("mismatch") ||
    text.includes("fail") ||
    text.includes("error")
  ) {
    return "danger";
  }
  return "muted";
}

function alertSeverityTone(severity: unknown): Tone {
  const text = String(severity ?? "").toUpperCase();
  if (text === "BLOCKING") {
    return "danger";
  }
  if (text === "ACTION") {
    return "warn";
  }
  if (text === "RECOVERY") {
    return "good";
  }
  if (text === "INFO" || text === "AUDIT_ONLY") {
    return "muted";
  }
  return statusTone(text);
}

function researchCaptureTone(runStatus: unknown, freshnessState: unknown): Tone {
  const freshness = String(freshnessState ?? "").toLowerCase();
  const status = String(runStatus ?? "").toLowerCase();
  if (freshness === "stale") {
    return "danger";
  }
  if (freshness === "no_run") {
    return "warn";
  }
  if (status === "success") {
    return "good";
  }
  if (status === "partial_failure") {
    return "warn";
  }
  if (status === "failure") {
    return "danger";
  }
  return "muted";
}

function freshnessState(record: unknown, fallback: string = "SNAPSHOT"): string {
  const value = String(asRecord(record).state ?? "").toUpperCase();
  return value || fallback;
}

function freshnessUpdatedAt(record: unknown): string | null {
  const value = asRecord(record).updated_at;
  return value == null ? null : String(value);
}

function freshnessToneFromState(state: string): Tone {
  return state === "STALE" ? "danger" : state === "SNAPSHOT" || state === "DELAYED" ? "warn" : "good";
}

function selectedManualVerificationStatusLabel(status: JsonRecord): string {
  if (status.live_verified === true) {
    return "LIVE VERIFIED";
  }
  if (status.live_enabled === true) {
    return "READY TO VERIFY";
  }
  const blockerReason = String(status.blocker_reason ?? "").trim();
  if (blockerReason.includes("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED")) {
    return "PREVIEW ONLY | LIMIT FLAG OFF";
  }
  if (blockerReason.includes("MGC_PRODUCTION_STOCK_MARKET_LIVE_SUBMIT_ENABLED")) {
    return "PREVIEW ONLY | MARKET FLAG OFF";
  }
  if (blockerReason.includes("MGC_PRODUCTION_STOCK_STOP_LIVE_SUBMIT_ENABLED")) {
    return "PREVIEW ONLY | STOP FLAG OFF";
  }
  if (blockerReason.includes("MGC_PRODUCTION_STOCK_STOP_LIMIT_LIVE_SUBMIT_ENABLED")) {
    return "PREVIEW ONLY | STOP-LIMIT FLAG OFF";
  }
  if (blockerReason.includes("Await live verification")) {
    return "PREVIEW ONLY | SEQUENCE LOCK";
  }
  if (status.previewable === true) {
    return "PREVIEW ONLY";
  }
  return "BLOCKED";
}

function manualIntentTypeLabel(intentType: string): string {
  const normalized = intentType.trim().toUpperCase();
  if (normalized === "MANUAL_LIVE_PILOT") {
    return "BUY_TO_OPEN";
  }
  if (normalized === "FLATTEN") {
    return "SELL_TO_CLOSE";
  }
  return sentenceCase(normalized.toLowerCase());
}

function auditVerdictTone(verdict: unknown): "good" | "warn" | "danger" | "muted" {
  const normalized = String(verdict ?? "").trim().toUpperCase();
  switch (normalized) {
    case "FILLED":
      return "good";
    case "SETUP_GATED":
    case "INTENT_NO_FILL_YET":
      return "warn";
    case "SURFACING_MISMATCH_SUSPECTED":
      return "danger";
    case "NO_SETUP_OBSERVED":
    case "INSUFFICIENT_HISTORY":
    default:
      return "muted";
  }
}

function parseTimestampMs(value: unknown): number {
  if (!value) {
    return 0;
  }
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatDuration(totalSeconds: number): string {
  if (!Number.isFinite(totalSeconds) || totalSeconds <= 0) {
    return "0m";
  }
  const days = Math.floor(totalSeconds / 86400);
  const hours = Math.floor((totalSeconds % 86400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  if (days > 0) {
    return `${days}d ${hours}h`;
  }
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  return `${minutes}m`;
}

function tradeDateLabel(row: JsonRecord): string {
  const raw = String(row.exit_timestamp ?? row.entry_timestamp ?? "").trim();
  return raw.includes("T") ? raw.slice(0, 10) : raw;
}

function tradeSessionLabel(row: JsonRecord): string {
  return String(row.entry_session_phase ?? row.exit_session_phase ?? row.current_session ?? "").trim();
}

function runtimeStateLabel(row: JsonRecord): string {
  const runtimePresent = row.runtime_instance_present === true;
  const stateLoaded = row.runtime_state_loaded === true;
  const canProcessBars = row.can_process_bars === true;
  if (runtimePresent && stateLoaded && canProcessBars) {
    return "READY";
  }
  if (runtimePresent && canProcessBars) {
    return "INSTANCE_ONLY";
  }
  if (runtimePresent) {
    return "SURFACED_ONLY";
  }
  return "MISSING";
}

function sameUnderlyingLabel(row: JsonRecord): string {
  return row.same_underlying_ambiguity === true ? "AMBIGUOUS" : "CLEAR";
}

function strategyStatusLabel(row: JsonRecord): string {
  return String(row.status ?? row.current_strategy_status ?? row.risk_state ?? "").trim();
}

function isTemporaryPaperStrategyRow(row: JsonRecord | null | undefined): boolean {
  return (
    row?.temporary_paper_strategy === true ||
    String(row?.paper_strategy_class ?? "").trim() === "temporary_paper_strategy" ||
    String(row?.experimental_status ?? "").trim() === "experimental_temp_paper" ||
    String(row?.lane_mode ?? "").trim() === "TEMP_PAPER_EXPERIMENTAL" ||
    String(row?.runtime_kind ?? "").trim() === "gc_mgc_london_open_acceptance_temp_paper"
  );
}

function paperStrategyClassLabel(row: JsonRecord | null | undefined): string {
  if (isTemporaryPaperStrategyRow(row)) {
    if (row?.snapshot_only === true || row?.runtime_instance_present === false) {
      return "TEMP PAPER | EXPERIMENTAL | PAPER ONLY | NON-APPROVED | SNAPSHOT ONLY";
    }
    return "TEMP PAPER | EXPERIMENTAL | PAPER ONLY | NON-APPROVED";
  }
  return "ADMITTED PAPER";
}

function paperStrategyClassTone(row: JsonRecord | null | undefined): "good" | "warn" | "danger" | "muted" {
  if (isTemporaryPaperStrategyRow(row)) {
    return row?.snapshot_only === true || row?.runtime_instance_present === false ? "danger" : "warn";
  }
  return "good";
}

function laneClassLabel(row: JsonRecord | null | undefined): string {
  const explicit = String(row?.lane_class_label ?? row?.designation_label ?? "").trim();
  if (explicit) {
    return explicit;
  }
  if (isTemporaryPaperStrategyRow(row)) {
    return "Temporary Paper Strategy";
  }
  return "Approved / Admitted Paper Lane";
}

function designationLabel(row: JsonRecord | null | undefined): string {
  const explicit = String(row?.designation_label ?? row?.scope_label ?? "").trim();
  if (explicit) {
    return explicit;
  }
  if (row?.benchmark_designation) {
    return "Benchmark Lane";
  }
  if (row?.candidate_designation) {
    return "Candidate Staged Lane";
  }
  return isTemporaryPaperStrategyRow(row) ? "Audit / Experimental" : "Shared Paper Lane";
}

function runtimeAttachmentLabel(row: JsonRecord | null | undefined): string {
  if (row?.audit_only === true || row?.snapshot_only === true) {
    return "Audit Only";
  }
  if (row?.runtime_instance_present === true && row?.runtime_state_loaded === true) {
    return "Attached Live";
  }
  if (row?.runtime_instance_present === true) {
    return "Attached / State Pending";
  }
  return "Not Loaded";
}

function stagedPostureLabel(row: JsonRecord | null | undefined): string {
  const side = String(row?.net_side ?? row?.position_side ?? "FLAT").trim() || "FLAT";
  const quantity = row?.total_quantity ?? row?.internal_position_qty ?? 0;
  const openLegs = row?.open_entry_leg_count ?? 0;
  const adds = row?.open_add_count ?? row?.add_count ?? 0;
  const canAddMore = row?.additional_entry_allowed ?? row?.can_add_more;
  return `${side} | Qty ${formatShortNumber(quantity)} | Legs ${formatShortNumber(openLegs)} | Adds ${formatShortNumber(adds)} | ${canAddMore === true ? "Can Add" : "At Cap"}`;
}

function cadenceLabel(row: JsonRecord | null | undefined): string {
  const execution = String(row?.execution_timeframe ?? "—").trim() || "—";
  const contexts = asArray<string>(row?.context_timeframes).length ? asArray<string>(row?.context_timeframes).join(" / ") : "—";
  return `${execution} exec | ${contexts} ctx`;
}

function isAtpRow(row: JsonRecord | null | undefined): boolean {
  return /atp_companion/i.test(
    [
      row?.lane_id,
      row?.standalone_strategy_id,
      row?.display_name,
      row?.branch,
      row?.strategy_family,
      row?.strategy_status,
    ]
      .filter(Boolean)
      .join(" "),
  );
}

function compactBranchLabel(row: JsonRecord | null | undefined): string {
  const raw = String(row?.branch ?? row?.strategy_name ?? row?.lane_id ?? row?.standalone_strategy_id ?? "Unnamed Lane");
  return raw.replace(/\s*\/\s*/g, " / ");
}

function trackedStrategyDisplayName(detail: JsonRecord | null | undefined): string {
  return String(
    detail?.display_name
      ?? detail?.strategy_name
      ?? detail?.strategy_id
      ?? detail?.tracked_strategy_id
      ?? "Tracked ATP Strategy",
  );
}

function rosterStatusChip(row: JsonRecord | null | undefined): string {
  if (row?.benchmark_designation) {
    return "BENCH";
  }
  if (row?.candidate_designation) {
    return "CAND";
  }
  if (isTemporaryPaperStrategyRow(row)) {
    return row?.runtime_instance_present === true ? "TEMP LIVE" : "TEMP";
  }
  if (runtimeAttachmentLabel(row) === "Attached Live") {
    return "LIVE";
  }
  if (runtimeAttachmentLabel(row) === "Attached / State Pending") {
    return "ATTACH";
  }
  if (String(row?.state ?? row?.strategy_status ?? "").toUpperCase().includes("PAUSE")) {
    return "PAUSED";
  }
  return "PAPER";
}

function rosterStatusTone(row: JsonRecord | null | undefined): Tone {
  const chip = rosterStatusChip(row);
  if (chip === "LIVE" || chip === "ATTACH") {
    return "good";
  }
  if (chip === "CAND" || chip === "TEMP" || chip === "TEMP LIVE") {
    return "warn";
  }
  if (chip === "PAUSED") {
    return "muted";
  }
  return "muted";
}

function rosterCardAccentClass(row: JsonRecord | null | undefined): string {
  const chip = rosterStatusChip(row);
  if (chip === "LIVE" || chip === "ATTACH") {
    return "live";
  }
  if (chip === "CAND") {
    return "candidate";
  }
  if (chip === "PAUSED") {
    return "paused";
  }
  return "paper";
}

function normalizeTemporaryPaperRegistryRow(row: JsonRecord, runtimeRow?: JsonRecord): JsonRecord {
  const runtimePresent = runtimeRow?.runtime_instance_present === true || row.runtime_instance_present === true;
  const runtimeLoaded = runtimeRow?.runtime_state_loaded === true || row.runtime_state_loaded === true;
  const canProcessBars = runtimeRow?.can_process_bars === true || row.can_process_bars === true;
  const snapshotOnly = row.snapshot_only === true || !runtimePresent;
  return {
    ...row,
    standalone_strategy_id: String(row.lane_id ?? row.display_name ?? "temporary_paper_strategy"),
    display_name: String(row.display_name ?? row.branch ?? row.lane_id ?? "Temporary Paper Strategy"),
    strategy_family: String(row.source_family ?? row.branch ?? row.display_name ?? "TEMPORARY_PAPER"),
    family: String(row.source_family ?? row.branch ?? row.display_name ?? "TEMPORARY_PAPER"),
    source_family: String(row.source_family ?? row.branch ?? row.display_name ?? "TEMPORARY_PAPER"),
    enabled: row.state === "ENABLED",
    runtime_instance_present: runtimePresent,
    runtime_state_loaded: runtimeLoaded,
    can_process_bars: canProcessBars,
    config_source: String(runtimeRow?.config_source ?? row.config_source ?? (snapshotOnly ? "paper.non_approved_lanes_snapshot" : "paper.non_approved_lanes")),
    temporary_paper_strategy: true,
    paper_strategy_class: "temporary_paper_strategy",
    current_session: String(row.session_restriction ?? "ALL"),
    current_strategy_status: String(snapshotOnly ? "SNAPSHOT ONLY" : row.lifecycle_state ?? row.state ?? "ENABLED"),
    status: String(snapshotOnly ? "SNAPSHOT ONLY" : row.state ?? "ENABLED"),
    same_underlying_ambiguity: false,
    same_underlying_conflict_present: false,
    same_underlying_entry_block_effective: false,
    same_underlying_ambiguity_note: String(snapshotOnly ? "Snapshot Only | Not Loaded In Runtime." : (row.note ?? "")),
    snapshot_only: snapshotOnly,
  };
}

function strategyRowIdentity(row: JsonRecord): string {
  const laneId = String(row.lane_id ?? "").trim();
  if (laneId) {
    return `lane:${laneId}`;
  }
  return String(
    row.standalone_strategy_id ??
      row.lane_id ??
      row.id ??
      row.strategy_key ??
      row.display_name ??
      "unknown_strategy_row",
  );
}

function mergeStrategyRows(baseRows: JsonRecord[], overlayRows: JsonRecord[]): JsonRecord[] {
  const merged = new Map<string, JsonRecord>();
  for (const row of [...baseRows, ...overlayRows]) {
    const key = strategyRowIdentity(row);
    const existing = merged.get(key);
    if (!existing) {
      merged.set(key, row);
      continue;
    }
    if (isTemporaryPaperStrategyRow(existing) || isTemporaryPaperStrategyRow(row)) {
      merged.set(key, {
        ...row,
        ...existing,
        temporary_paper_strategy: true,
        paper_strategy_class: "temporary_paper_strategy",
        experimental_status: existing.experimental_status ?? row.experimental_status,
        paper_only: existing.paper_only ?? row.paper_only ?? true,
        non_approved: existing.non_approved ?? row.non_approved ?? true,
        metrics_bucket: existing.metrics_bucket ?? row.metrics_bucket ?? "experimental_temporary_paper",
      });
      continue;
    }
  }
  return [...merged.values()];
}

function normalizeTemporaryPaperPerformanceRow(row: JsonRecord, runtimeRow?: JsonRecord): JsonRecord {
  return {
    ...normalizeTemporaryPaperRegistryRow(row, runtimeRow),
    strategy_name: String(row.display_name ?? row.branch ?? row.lane_id ?? "Temporary Paper Strategy"),
    instrument: String(row.instrument ?? "UNKNOWN"),
    realized_pnl: row.realized_pnl ?? row.metrics_net_pnl_cash ?? null,
    unrealized_pnl: row.open_position ? row.metrics_net_pnl_cash ?? null : null,
    day_pnl: row.realized_pnl ?? row.metrics_net_pnl_cash ?? null,
    cumulative_pnl: row.realized_pnl ?? row.metrics_net_pnl_cash ?? null,
    max_drawdown: row.metrics_max_drawdown ?? null,
    trade_count: row.trade_count ?? row.fill_count ?? 0,
    pnl_unavailable_reason: "Temporary paper metrics bucket",
    latest_activity_timestamp: row.latest_activity_timestamp ?? row.last_update_timestamp ?? null,
    position_side: String(row.position_side ?? row.side ?? (row.open_position ? "OPEN" : "FLAT")),
  };
}

function normalizeTemporaryPaperAuditRow(row: JsonRecord, runtimeRow?: JsonRecord): JsonRecord {
  return {
    ...normalizeTemporaryPaperRegistryRow(row, runtimeRow),
    strategy_name: String(row.display_name ?? row.branch ?? row.lane_id ?? "Temporary Paper Strategy"),
    instrument: String(row.instrument ?? "UNKNOWN"),
    current_session: String(row.session_restriction ?? "ALL"),
    eligible_now: row.state === "ENABLED" && row.kill_switch_active !== true && (runtimeRow?.runtime_instance_present === true || row.runtime_instance_present === true),
    auditable_now: true,
    performance_row_present: true,
    current_strategy_status: String(
      row.snapshot_only === true || row.runtime_instance_present === false
        ? "SNAPSHOT ONLY"
        : row.lifecycle_state ?? row.state ?? "ENABLED",
    ),
    latest_fault_or_blocker:
      row.snapshot_only === true || row.runtime_instance_present === false
        ? "snapshot_only_not_loaded_in_runtime"
        : row.kill_switch_active === true
        ? "kill_switch_active"
        : String(row.override_reason ?? row.latest_signal_label ?? row.note ?? "paper_only_experimental_canary"),
    last_processed_bar_end_ts: row.latest_activity_timestamp ?? row.last_update_timestamp ?? null,
    audit_verdict:
      Number(row.fill_count ?? 0) > 0
        ? "FILLED"
        : Number(row.signal_count ?? row.recent_signal_count ?? 0) > 0
          ? "INTENT_NO_FILL_YET"
          : "NO_SETUP_OBSERVED",
    audit_reason: String(row.note ?? row.override_reason ?? "Temporary paper strategy surfaced in the desktop app."),
  };
}

function numericOrNull(value: unknown): number | null {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const numeric = Number(value);
  return Number.isNaN(numeric) ? null : numeric;
}

function formatCompactMetric(value: unknown, digits = 2): string {
  const numeric = numericOrNull(value);
  if (numeric === null) {
    return "—";
  }
  return new Intl.NumberFormat(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  }).format(numeric);
}

function formatCompactPrice(value: unknown): string {
  return formatCompactMetric(value, 4);
}

function formatCompactPnL(value: unknown): string {
  const numeric = numericOrNull(value);
  if (numeric === null) {
    return "—";
  }
  return `${numeric > 0 ? "+" : ""}${formatCompactMetric(numeric, 2)}`;
}

function pnlTone(value: unknown): Tone {
  const numeric = numericOrNull(value);
  if (numeric === null) {
    return "muted";
  }
  if (numeric > 0) {
    return "good";
  }
  if (numeric < 0) {
    return "danger";
  }
  return "muted";
}

function renderPnlValue(value: unknown): ReactNode {
  const numeric = numericOrNull(value);
  if (numeric === null) {
    return "—";
  }
  const className = numeric > 0 ? "pnl-positive" : numeric < 0 ? "pnl-negative" : "pnl-neutral";
  return <span className={`pnl-value ${className}`}>{formatCompactPnL(numeric)}</span>;
}

function formatCompactTimestamp(value: unknown): string {
  return value ? formatTimestamp(String(value)) : "—";
}

function parseSortTimestamp(value: unknown): number | null {
  if (!value) {
    return null;
  }
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : null;
}

function sumNullable(values: Array<number | null>): number | null {
  const filtered = values.filter((value): value is number => value !== null);
  if (!filtered.length) {
    return null;
  }
  return filtered.reduce((sum, value) => sum + value, 0);
}

function averageNullable(values: Array<number | null>): number | null {
  const filtered = values.filter((value): value is number => value !== null);
  if (!filtered.length) {
    return null;
  }
  return filtered.reduce((sum, value) => sum + value, 0) / filtered.length;
}

function weightedAverage(items: Array<{ value: number | null; weight: number | null }>): number | null {
  let weightedSum = 0;
  let totalWeight = 0;
  for (const item of items) {
    if (item.value === null) {
      continue;
    }
    const weight = Math.abs(item.weight ?? 0);
    if (weight <= 0) {
      continue;
    }
    weightedSum += item.value * weight;
    totalWeight += weight;
  }
  return totalWeight > 0 ? weightedSum / totalWeight : null;
}

function latestTimestamp(values: Array<unknown>): string | null {
  let latest: string | null = null;
  let latestMs = -1;
  for (const value of values) {
    if (!value) {
      continue;
    }
    const nextValue = String(value);
    const nextMs = Date.parse(nextValue);
    if (Number.isFinite(nextMs) && nextMs > latestMs) {
      latest = nextValue;
      latestMs = nextMs;
    }
  }
  return latest;
}

function dateKeyFromTimestamp(value: unknown): string | null {
  const text = String(value ?? "").trim();
  if (!text) {
    return null;
  }
  const isoMatch = text.match(/^(\d{4}-\d{2}-\d{2})/);
  if (isoMatch) {
    return isoMatch[1];
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  const year = parsed.getFullYear();
  const month = String(parsed.getMonth() + 1).padStart(2, "0");
  const day = String(parsed.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function dateFromKey(dateKey: string): Date {
  const [year, month, day] = dateKey.split("-").map((part) => Number(part));
  return new Date(year, Math.max(month - 1, 0), day || 1);
}

function formatMonthTitle(dateKey: string): string {
  return new Intl.DateTimeFormat(undefined, { month: "long", year: "numeric" }).format(dateFromKey(dateKey));
}

function formatWeekTitle(dateKey: string): string {
  const start = startOfWeek(dateKey);
  const end = addDays(start, 4);
  const formatter = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", year: "numeric" });
  return `Week of ${formatter.format(dateFromKey(start))} - ${formatter.format(dateFromKey(end))}`;
}

function formatLongDate(dateKey: string): string {
  return new Intl.DateTimeFormat(undefined, {
    weekday: "long",
    month: "long",
    day: "numeric",
    year: "numeric",
  }).format(dateFromKey(dateKey));
}

function addDays(dateKey: string, offset: number): string {
  const next = dateFromKey(dateKey);
  next.setDate(next.getDate() + offset);
  return dateKeyFromTimestamp(next.toISOString()) ?? dateKey;
}

function addMonths(dateKey: string, offset: number): string {
  const next = dateFromKey(dateKey);
  next.setMonth(next.getMonth() + offset, 1);
  return dateKeyFromTimestamp(next.toISOString()) ?? dateKey;
}

function startOfMonth(dateKey: string): string {
  const value = dateFromKey(dateKey);
  value.setDate(1);
  return dateKeyFromTimestamp(value.toISOString()) ?? dateKey;
}

function endOfMonth(dateKey: string): string {
  const value = dateFromKey(startOfMonth(dateKey));
  value.setMonth(value.getMonth() + 1, 0);
  return dateKeyFromTimestamp(value.toISOString()) ?? dateKey;
}

function startOfYear(dateKey: string): string {
  const value = dateFromKey(dateKey);
  value.setMonth(0, 1);
  return dateKeyFromTimestamp(value.toISOString()) ?? dateKey;
}

function startOfQuarter(dateKey: string): string {
  const value = dateFromKey(dateKey);
  const quarterStartMonth = Math.floor(value.getMonth() / 3) * 3;
  value.setMonth(quarterStartMonth, 1);
  return dateKeyFromTimestamp(value.toISOString()) ?? dateKey;
}

function endOfQuarter(dateKey: string): string {
  const value = dateFromKey(startOfQuarter(dateKey));
  value.setMonth(value.getMonth() + 3, 0);
  return dateKeyFromTimestamp(value.toISOString()) ?? dateKey;
}

function startOfWeek(dateKey: string): string {
  const value = dateFromKey(dateKey);
  const day = value.getDay();
  const offset = day === 0 ? -6 : 1 - day;
  value.setDate(value.getDate() + offset);
  return dateKeyFromTimestamp(value.toISOString()) ?? dateKey;
}

function isWeekendDate(dateKey: string): boolean {
  const day = dateFromKey(dateKey).getDay();
  return day === 0 || day === 6;
}

function daysBetween(startDate: string, endDate: string): number {
  const diff = dateFromKey(endDate).getTime() - dateFromKey(startDate).getTime();
  return Math.round(diff / 86400000);
}

function formatPercentValue(value: number | null, digits = 1): string {
  if (value === null || !Number.isFinite(value)) {
    return "—";
  }
  return `${value.toFixed(digits)}%`;
}

function formatRatioValue(value: number | null, digits = 2): string {
  if (value === null || !Number.isFinite(value)) {
    return "—";
  }
  return value.toFixed(digits);
}

function calendarSourceLabel(source: PnlCalendarSource): string {
  switch (source) {
    case "live":
      return "Live";
    case "paper":
      return "Paper";
    case "benchmark_replay":
      return "Benchmark / Replay";
    case "research_execution":
      return "Research Execution";
    default:
      return "All Accounts";
  }
}

function formatSignedCompactWhole(value: number | null): string {
  if (value === null || !Number.isFinite(value)) {
    return "—";
  }
  const rounded = Math.round(value);
  return `${rounded > 0 ? "+" : rounded < 0 ? "-" : ""}$${Math.abs(rounded).toLocaleString()}`;
}

function formatCompactCurrency(value: number | null): string {
  if (value === null || !Number.isFinite(value)) {
    return "—";
  }
  return `${value > 0 ? "+" : value < 0 ? "-" : ""}$${Math.abs(value).toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function sharpeTone(value: number | null): Tone {
  if (value === null) {
    return "muted";
  }
  if (value >= 1.5) {
    return "good";
  }
  if (value >= 1) {
    return "warn";
  }
  return "danger";
}

function winRateTone(value: number | null): Tone {
  if (value === null) {
    return "muted";
  }
  if (value >= 55) {
    return "good";
  }
  if (value >= 50) {
    return "warn";
  }
  return "danger";
}

function sourcePeriodLabel(period: PnlCalendarPeriod, anchorDate: string, startDate: string, endDate: string): string {
  if (period === "weekly") {
    return formatWeekTitle(anchorDate);
  }
  if (period === "quarterly") {
    const start = startOfQuarter(anchorDate);
    const end = endOfQuarter(anchorDate);
    return `${formatMonthTitle(start)} - ${formatMonthTitle(end)}`;
  }
  if (period === "ytd") {
    const formatter = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", year: "numeric" });
    return `${formatter.format(dateFromKey(startDate))} - ${formatter.format(dateFromKey(endDate))}`;
  }
  if (period === "custom") {
    const formatter = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", year: "numeric" });
    return `${formatter.format(dateFromKey(startDate))} - ${formatter.format(dateFromKey(endDate))}`;
  }
  return formatMonthTitle(anchorDate);
}

interface ParsedOptionContract {
  underlying: string;
  expiry: string;
  putCall: "PUT" | "CALL";
  strike: number | null;
}

function parseBrokerOptionContract(row: JsonRecord): ParsedOptionContract | null {
  if (String(row.asset_class ?? "").toUpperCase() !== "OPTION") {
    return null;
  }
  const instrument = asRecord(asRecord(row.raw_payload).instrument);
  const underlying = String(instrument.underlyingSymbol ?? "").trim() || String(row.symbol ?? "").trim();
  const putCall = String(instrument.putCall ?? "").trim().toUpperCase();
  const description = String(row.description ?? instrument.description ?? "").trim();
  const descriptionMatch = description.match(/(\d{2})\/(\d{2})\/(\d{4})\s+\$([0-9.]+)\s+(Put|Call)/i);
  if (descriptionMatch) {
    return {
      underlying,
      expiry: `${descriptionMatch[3]}-${descriptionMatch[1]}-${descriptionMatch[2]}`,
      strike: Number(descriptionMatch[4]),
      putCall: descriptionMatch[5].toUpperCase() === "CALL" ? "CALL" : "PUT",
    };
  }
  const symbol = String(row.symbol ?? instrument.symbol ?? "").trim();
  const symbolMatch = symbol.match(/^\S+\s+(\d{6})([CP])(\d{8})$/);
  if (!symbolMatch || (putCall !== "PUT" && putCall !== "CALL")) {
    return null;
  }
  const [, yyMMdd, sideCode, strikeDigits] = symbolMatch;
  return {
    underlying,
    expiry: `20${yyMMdd.slice(0, 2)}-${yyMMdd.slice(2, 4)}-${yyMMdd.slice(4, 6)}`,
    strike: Number(strikeDigits) / 1000,
    putCall: sideCode === "C" ? "CALL" : "PUT",
  };
}

function sourceBadgesForRow(row: PositionsMonitorRow): PositionsSourceClass[] {
  const badges: PositionsSourceClass[] = [];
  if (row.brokerRows.length > 0) {
    badges.push("BROKER");
  }
  if (row.experimentalRows.length > 0) {
    badges.push("EXPERIMENTAL");
  } else if (row.paperRows.length > 0) {
    badges.push("PAPER");
  }
  return badges;
}

function exposureMarkerForRow(row: PositionsMonitorRow): "LONG" | "SHORT" | "BOTH" | null {
  if (row.isSpreadParent) {
    return "BOTH";
  }
  const brokerLong = row.brokerRows.some((brokerRow) => String(brokerRow.side ?? "").toUpperCase() === "LONG");
  const brokerShort = row.brokerRows.some((brokerRow) => String(brokerRow.side ?? "").toUpperCase() === "SHORT");
  const paperQty = row.paperQty ?? 0;
  const paperLong = paperQty > 0;
  const paperShort = paperQty < 0;
  if ((brokerLong || paperLong) && (brokerShort || paperShort)) {
    return "BOTH";
  }
  if (brokerLong || paperLong) {
    return "LONG";
  }
  if (brokerShort || paperShort) {
    return "SHORT";
  }
  return null;
}

function paperMonitorInterestScore(row: PositionsMonitorRow): number {
  let score = 0;
  const activePaperPosition = row.paperRows.some((paperRow) => String(paperRow.position_side ?? "").toUpperCase() !== "FLAT");
  if (activePaperPosition) {
    score += 400;
  }
  if (Math.abs(row.paperDayPnl ?? 0) > 0.01) {
    score += 220;
  }
  if (Math.abs(row.paperRealized ?? 0) > 0.01) {
    score += 180;
  }
  if (Math.abs(row.paperOpenPnl ?? 0) > 0.01) {
    score += 140;
  }
  if ((row.tradeCount ?? 0) > 0) {
    score += 80;
  }
  if (row.lastActivity) {
    score += 40;
  }
  return score;
}

function compareMonitorValues(left: string | number | null, right: string | number | null, direction: SortDirection): number {
  if (left === right) {
    return 0;
  }
  if (left === null) {
    return 1;
  }
  if (right === null) {
    return -1;
  }
  const modifier = direction === "asc" ? 1 : -1;
  if (typeof left === "number" && typeof right === "number") {
    return (left - right) * modifier;
  }
  return String(left).localeCompare(String(right), undefined, { numeric: true, sensitivity: "base" }) * modifier;
}

function readPositionsLayoutState(): PositionsLayoutState {
  const defaultState: PositionsLayoutState = {
    currentColumnsByMode: {
      broker: [...POSITIONS_DEFAULT_COLUMNS.broker],
      paper: [...POSITIONS_DEFAULT_COLUMNS.paper],
      combined: [...POSITIONS_DEFAULT_COLUMNS.combined],
    },
    savedLayoutsByMode: {
      broker: {},
      paper: {},
      combined: {},
    },
  };
  try {
    const raw = window.localStorage.getItem(POSITIONS_LAYOUT_STORAGE_KEY);
    if (!raw) {
      return defaultState;
    }
    const parsed = JSON.parse(raw) as Partial<PositionsLayoutState>;
    return {
      currentColumnsByMode: {
        broker: Array.isArray(parsed.currentColumnsByMode?.broker) ? parsed.currentColumnsByMode.broker : defaultState.currentColumnsByMode.broker,
        paper: Array.isArray(parsed.currentColumnsByMode?.paper) ? parsed.currentColumnsByMode.paper : defaultState.currentColumnsByMode.paper,
        combined: Array.isArray(parsed.currentColumnsByMode?.combined) ? parsed.currentColumnsByMode.combined : defaultState.currentColumnsByMode.combined,
      },
      savedLayoutsByMode: {
        broker: asRecord(parsed.savedLayoutsByMode?.broker),
        paper: asRecord(parsed.savedLayoutsByMode?.paper),
        combined: asRecord(parsed.savedLayoutsByMode?.combined),
      },
    };
  } catch {
    return defaultState;
  }
}

function writePositionsLayoutState(state: PositionsLayoutState): void {
  window.localStorage.setItem(POSITIONS_LAYOUT_STORAGE_KEY, JSON.stringify(state));
}

function normalizeMonitorColumns(columns: string[] | undefined, mode: PositionsViewMode): string[] {
  const available = POSITIONS_AVAILABLE_COLUMNS[mode];
  const base = columns?.length ? columns : POSITIONS_DEFAULT_COLUMNS[mode];
  const deduped = ["symbol", ...base]
    .filter((columnId, index, values) => values.indexOf(columnId) === index)
    .filter((columnId) => available.includes(columnId));
  return deduped.length ? deduped : [...POSITIONS_DEFAULT_COLUMNS[mode]];
}

function paperClassLabel(row: PositionsMonitorRow): PositionsSourceClass | null {
  if (!row.paperRows.length) {
    return null;
  }
  return row.experimentalRows.length > 0 ? "EXPERIMENTAL" : "PAPER";
}

function tradeTopLevelClass(row: JsonRecord): PositionsSourceClass {
  return isTemporaryPaperStrategyRow(row) ? "EXPERIMENTAL" : "PAPER";
}

function quantityFromPaperRow(row: JsonRecord): number {
  const explicit = numericOrNull(row.quantity);
  if (explicit !== null) {
    const side = String(row.position_side ?? row.side ?? "").toUpperCase();
    if (side.includes("SHORT")) {
      return explicit * -1;
    }
    if (side.includes("LONG")) {
      return explicit;
    }
  }
  const side = String(row.position_side ?? row.side ?? "").toUpperCase();
  if (side.includes("SHORT")) {
    return -1;
  }
  if (side.includes("LONG")) {
    return 1;
  }
  return 0;
}

function buildSpreadParentRow(longRow: PositionsMonitorRow, shortRow: PositionsMonitorRow): PositionsMonitorRow {
  const longContract = parseBrokerOptionContract(longRow.brokerRows[0] ?? {});
  const shortContract = parseBrokerOptionContract(shortRow.brokerRows[0] ?? {});
  const strikes = [longContract?.strike, shortContract?.strike].filter((value): value is number => value !== null).sort((left, right) => left - right);
  const expiry = longContract?.expiry ?? shortContract?.expiry ?? "";
  const expiryLabel = expiry ? new Date(`${expiry}T00:00:00`).toLocaleDateString() : "Unknown Exp";
  const typeLabel = longContract?.putCall === "CALL" ? "Call" : "Put";
  const spreadLabel = `${longContract?.underlying ?? shortContract?.underlying ?? "OPTION"} ${expiryLabel} ${typeLabel} Vertical`;
  const spreadQty = Math.max(Math.abs(longRow.brokerQty ?? 0), Math.abs(shortRow.brokerQty ?? 0));
  const netMarketValue = sumNullable([longRow.brokerMarketValue, shortRow.brokerMarketValue]);
  const derivedMark = netMarketValue !== null && spreadQty > 0 ? Math.abs(netMarketValue) / (spreadQty * 100) : null;
  return {
    ...longRow,
    id: `spread:${longRow.id}:${shortRow.id}`,
    symbol: longContract?.underlying ?? shortContract?.underlying ?? longRow.symbol,
    description: strikes.length === 2 ? `${spreadLabel} ${formatCompactMetric(strikes[0], 2)}/${formatCompactMetric(strikes[1], 2)}` : spreadLabel,
    displaySymbol: longContract?.underlying ?? shortContract?.underlying ?? longRow.symbol,
    displayDescription: strikes.length === 2 ? `${typeLabel.toUpperCase()} ${formatCompactMetric(strikes[0], 2)}/${formatCompactMetric(strikes[1], 2)} • ${expiryLabel}` : `${typeLabel.toUpperCase()} • ${expiryLabel}`,
    sourceBadges: ["BROKER"],
    exposureMarker: "BOTH",
    childRows: [
      {
        ...longRow,
        displaySymbol: longRow.symbol,
        displayDescription: longRow.description,
        sourceBadges: ["BROKER"],
        exposureMarker: "LONG",
        isSpreadLeg: true,
        spreadKey: `spread:${longRow.id}:${shortRow.id}`,
      },
      {
        ...shortRow,
        displaySymbol: shortRow.symbol,
        displayDescription: shortRow.description,
        sourceBadges: ["BROKER"],
        exposureMarker: "SHORT",
        isSpreadLeg: true,
        spreadKey: `spread:${longRow.id}:${shortRow.id}`,
      },
    ],
    isSpreadParent: true,
    spreadKey: `spread:${longRow.id}:${shortRow.id}`,
    spreadLabel,
    brokerRows: [...longRow.brokerRows, ...shortRow.brokerRows],
    brokerOrders: [...longRow.brokerOrders, ...shortRow.brokerOrders],
    brokerFills: [...longRow.brokerFills, ...shortRow.brokerFills],
    brokerEvents: [...longRow.brokerEvents, ...shortRow.brokerEvents],
    closedTrades: [...longRow.closedTrades, ...shortRow.closedTrades],
    brokerQty: spreadQty || null,
    brokerAvgPrice: weightedAverage([
      { value: longRow.brokerAvgPrice, weight: longRow.brokerQty },
      { value: shortRow.brokerAvgPrice, weight: shortRow.brokerQty },
    ]),
    brokerMark: derivedMark,
    brokerDayPnl: sumNullable([longRow.brokerDayPnl, shortRow.brokerDayPnl]),
    brokerOpenPnl: sumNullable([longRow.brokerOpenPnl, shortRow.brokerOpenPnl]),
    brokerRealized: sumNullable([longRow.brokerRealized, shortRow.brokerRealized]),
    brokerYtdPnl: sumNullable([longRow.brokerYtdPnl, shortRow.brokerYtdPnl]),
    brokerMarketValue: netMarketValue,
    brokerMarginEffect: sumNullable([longRow.brokerMarginEffect, shortRow.brokerMarginEffect]),
    brokerDelta: sumNullable([longRow.brokerDelta, shortRow.brokerDelta]),
    brokerTheta: sumNullable([longRow.brokerTheta, shortRow.brokerTheta]),
    lastActivity: latestTimestamp([longRow.lastActivity, shortRow.lastActivity]),
    latestFillTime: latestTimestamp([longRow.latestFillTime, shortRow.latestFillTime]),
    latestTradeTime: latestTimestamp([longRow.latestTradeTime, shortRow.latestTradeTime]),
    latestBrokerUpdateTime: latestTimestamp([longRow.latestBrokerUpdateTime, shortRow.latestBrokerUpdateTime]),
    conflict: "Spread",
  };
}

function buildSpreadDisplayRows(rows: PositionsMonitorRow[], expandedSpreadRowIds: string[]): PositionsMonitorRow[] {
  const topLevelRows = [...rows];
  const groupedRowIds = new Set<string>();
  const spreadParents: PositionsMonitorRow[] = [];
  const candidateRows = topLevelRows.filter((row) => row.brokerRows.length > 0 && String(row.brokerRows[0]?.asset_class ?? "").toUpperCase() === "OPTION");
  const buckets = new Map<string, { longs: PositionsMonitorRow[]; shorts: PositionsMonitorRow[] }>();
  for (const row of candidateRows) {
    const contract = parseBrokerOptionContract(row.brokerRows[0] ?? {});
    if (!contract) {
      continue;
    }
    const quantity = Math.abs(row.brokerQty ?? 0);
    const bucketKey = `${contract.underlying}|${contract.expiry}|${contract.putCall}|${quantity}`;
    const bucket = buckets.get(bucketKey) ?? { longs: [], shorts: [] };
    if (String(row.brokerRows[0]?.side ?? "").toUpperCase() === "SHORT") {
      bucket.shorts.push(row);
    } else {
      bucket.longs.push(row);
    }
    buckets.set(bucketKey, bucket);
  }
  for (const bucket of buckets.values()) {
    const longs = [...bucket.longs].sort((left, right) => (parseBrokerOptionContract(left.brokerRows[0] ?? {})?.strike ?? 0) - (parseBrokerOptionContract(right.brokerRows[0] ?? {})?.strike ?? 0));
    const shorts = [...bucket.shorts].sort((left, right) => (parseBrokerOptionContract(left.brokerRows[0] ?? {})?.strike ?? 0) - (parseBrokerOptionContract(right.brokerRows[0] ?? {})?.strike ?? 0));
    while (longs.length && shorts.length) {
      const longRow = longs.shift()!;
      let bestShortIndex = -1;
      let bestDistance = Number.POSITIVE_INFINITY;
      const longStrike = parseBrokerOptionContract(longRow.brokerRows[0] ?? {})?.strike;
      shorts.forEach((shortRow, index) => {
        const shortStrike = parseBrokerOptionContract(shortRow.brokerRows[0] ?? {})?.strike;
        if (longStrike === null || longStrike === undefined || shortStrike === null || shortStrike === undefined || shortStrike === longStrike) {
          return;
        }
        const distance = Math.abs(shortStrike - longStrike);
        if (distance < bestDistance) {
          bestDistance = distance;
          bestShortIndex = index;
        }
      });
      if (bestShortIndex < 0) {
        continue;
      }
      const [shortRow] = shorts.splice(bestShortIndex, 1);
      const spreadParent = buildSpreadParentRow(longRow, shortRow);
      groupedRowIds.add(longRow.id);
      groupedRowIds.add(shortRow.id);
      spreadParents.push(spreadParent);
    }
  }

  const output: PositionsMonitorRow[] = [];
  for (const row of topLevelRows) {
    const spreadParent = spreadParents.find((candidate) => candidate.childRows?.some((childRow) => childRow.id === row.id));
    if (spreadParent && !output.some((existing) => existing.id === spreadParent.id)) {
      output.push({
        ...spreadParent,
        sourceBadges: spreadParent.sourceBadges ?? sourceBadgesForRow(spreadParent),
      });
      if (expandedSpreadRowIds.includes(spreadParent.id)) {
        output.push(...(spreadParent.childRows ?? []));
      }
      continue;
    }
    if (!groupedRowIds.has(row.id)) {
      output.push({
        ...row,
        displaySymbol: row.displaySymbol ?? row.symbol,
        displayDescription: row.displayDescription ?? row.description,
        sourceBadges: row.sourceBadges ?? sourceBadgesForRow(row),
        exposureMarker: row.exposureMarker ?? exposureMarkerForRow(row),
      });
    }
  }
  return output;
}

function buildStrategyPortfolioSnapshotFromRows(rows: JsonRecord[]): JsonRecord {
  const totalRealized = rows.reduce((sum, row) => sum + (numericOrNull(row.realized_pnl) ?? 0), 0);
  const totalDay = rows.reduce((sum, row) => sum + (numericOrNull(row.day_pnl) ?? 0), 0);
  const totalMaxDrawdown = rows.reduce((sum, row) => sum + (numericOrNull(row.max_drawdown) ?? 0), 0);
  const unrealizedValues = rows.map((row) => numericOrNull(row.unrealized_pnl));
  const missingUnrealizedRows = rows
    .filter((row, index) => unrealizedValues[index] === null)
    .map((row) => String(row.strategy_name ?? row.lane_id ?? "UNKNOWN"));
  const availableUnrealized = unrealizedValues.filter((value): value is number => value !== null);
  const totalUnrealized = availableUnrealized.length ? availableUnrealized.reduce((sum, value) => sum + value, 0) : null;
  const totalCumulative = totalUnrealized === null ? null : totalRealized + totalUnrealized;
  const activeStrategyCount = rows.filter((row) => row.entries_enabled !== false).length;
  const activeInstrumentCount = new Set(
    rows.map((row) => String(row.instrument ?? "").trim()).filter(Boolean),
  ).size;
  return {
    total_realized_pnl: totalRealized,
    total_unrealized_pnl: totalUnrealized,
    total_day_pnl: totalDay,
    total_cumulative_pnl: totalCumulative,
    total_max_drawdown: totalMaxDrawdown,
    active_strategy_count: activeStrategyCount,
    active_instrument_count: activeInstrumentCount,
    unrealized_complete: missingUnrealizedRows.length === 0,
    unrealized_missing_strategy_count: missingUnrealizedRows.length,
    unrealized_missing_strategies: missingUnrealizedRows,
    summary_line:
      missingUnrealizedRows.length === 0
        ? "Unrealized and cumulative totals are complete across the currently surfaced strategy rows."
        : `Unrealized and cumulative totals are partial; ${missingUnrealizedRows.length} strategy rows are missing trusted open-position marks.`,
  };
}

function truthBadgeTone(label: string): "good" | "warn" | "danger" | "muted" {
  switch (label) {
    case "REPLAY":
    case "PAPER":
    case "STRATEGY LEDGER":
      return "warn";
    case "LIVE BROKER":
      return "good";
    case "RUNTIME TRUTH":
      return "muted";
    case "SNAPSHOT FALLBACK":
      return "danger";
    default:
      return "muted";
  }
}

function replayStudyModeLabel(mode: unknown): string {
  switch (String(mode ?? "").trim().toUpperCase()) {
    case "ATP_ENHANCED":
      return "ATP-ENHANCED";
    case "LEGACY_ONLY":
      return "LEGACY-ONLY";
    default:
      return "NO DATA";
  }
}

function replayStudyModeTone(mode: unknown): Tone {
  switch (String(mode ?? "").trim().toUpperCase()) {
    case "ATP_ENHANCED":
      return "good";
    case "LEGACY_ONLY":
      return "warn";
    default:
      return "muted";
  }
}

function replayStudyTimingLabel(
  timingAvailable: boolean,
  {
    runLoaded,
    artifactFound,
  }: {
    runLoaded: boolean;
    artifactFound: boolean;
  },
): string {
  if (!runLoaded) {
    return "NOT LOADED";
  }
  if (!artifactFound) {
    return "NO ARTIFACT";
  }
  return timingAvailable ? "AVAILABLE" : "UNAVAILABLE";
}

function auditVerdictRank(verdict: unknown): number {
  switch (String(verdict ?? "").trim().toUpperCase()) {
    case "SURFACING_MISMATCH_SUSPECTED":
      return 0;
    case "SETUP_GATED":
      return 1;
    case "INTENT_NO_FILL_YET":
      return 2;
    case "FILLED":
      return 3;
    case "NO_SETUP_OBSERVED":
      return 4;
    case "INSUFFICIENT_HISTORY":
    default:
      return 5;
  }
}

function sameUnderlyingConflictTone(severity: unknown): "good" | "warn" | "danger" | "muted" {
  switch (String(severity ?? "").trim().toUpperCase()) {
    case "BLOCKING":
      return "danger";
    case "ACTION":
    case "WARNING":
      return "warn";
    case "INFO":
      return "muted";
    default:
      return "muted";
  }
}

function sameUnderlyingConflictRank(severity: unknown): number {
  switch (String(severity ?? "").trim().toUpperCase()) {
    case "BLOCKING":
      return 0;
    case "ACTION":
    case "WARNING":
      return 1;
    case "INFO":
      return 2;
    default:
      return 3;
  }
}

function sameUnderlyingConflictLabel(row: JsonRecord): string {
  const severity = String(row.same_underlying_conflict_severity ?? "").trim();
  return severity || "CLEAR";
}

function sameUnderlyingReviewTone(status: unknown): "good" | "warn" | "danger" | "muted" {
  switch (String(status ?? "").trim().toUpperCase()) {
    case "HOLDING":
      return "danger";
    case "HOLD_EXPIRED":
    case "STALE":
      return "warn";
    case "ACKNOWLEDGED":
    case "OVERRIDDEN":
      return "good";
    case "OPEN":
      return "muted";
    default:
      return "muted";
  }
}

function sameUnderlyingModeLabel(row: JsonRecord): string {
  if (row.same_underlying_conflict_execution_risk === true) {
    return "EXECUTION-RELEVANT";
  }
  if (String(row.same_underlying_conflict_severity ?? "").trim().toUpperCase() === "ACTION") {
    return "ACTIONABLE";
  }
  if (row.same_underlying_conflict_present === true) {
    return "OBSERVATIONAL";
  }
  return "CLEAR";
}

function laneTradabilityTone(status: unknown): Tone {
  switch (String(status ?? "").trim().toUpperCase()) {
    case "ELIGIBLE_TO_TRADE":
      return "good";
    case "INFORMATIONAL_ONLY":
      return "muted";
    case "LOADED_NOT_ELIGIBLE":
    case "LOADED_CONFIG_ONLY":
    case "HALTED_BY_RISK":
      return "warn";
    case "RECONCILING":
    case "FAULTED":
      return "danger";
    default:
      return "muted";
  }
}

function laneTradabilityLabel(status: unknown): string {
  switch (String(status ?? "").trim().toUpperCase()) {
    case "ELIGIBLE_TO_TRADE":
      return "Eligible To Trade";
    case "INFORMATIONAL_ONLY":
      return "Informational Only";
    case "LOADED_NOT_ELIGIBLE":
      return "Loaded, Not Eligible";
    case "LOADED_CONFIG_ONLY":
      return "Configured, Not Loaded";
    case "HALTED_BY_RISK":
      return "Halted By Risk";
    case "RECONCILING":
      return "Reconciling";
    case "FAULTED":
      return "Faulted";
    default:
      return formatValue(status);
  }
}

function heartbeatReconciliationTone(status: unknown): Tone {
  switch (String(status ?? "").trim().toUpperCase()) {
    case "CLEAN":
      return "good";
    case "SAFE_REPAIR":
      return "warn";
    case "BROKER_UNAVAILABLE":
      return "warn";
    case "RECONCILING":
    case "FAULT":
      return "danger";
    case "AWAITING_FIRST_HEARTBEAT":
      return "muted";
    default:
      return "muted";
  }
}

function orderTimeoutWatchdogTone(status: unknown): Tone {
  switch (String(status ?? "").trim().toUpperCase()) {
    case "HEALTHY":
      return "good";
    case "SAFE_REPAIR":
      return "warn";
    case "ACTIVE_TIMEOUTS":
    case "BROKER_UNAVAILABLE":
      return "warn";
    case "RECONCILING":
    case "FAULT":
      return "danger";
    default:
      return "muted";
  }
}

function pageTitle(page: PageId): string {
  const item = NAV_ITEMS.find((candidate) => candidate.id === page);
  return item?.label ?? "Home";
}

function ownershipLabel(ownership: DesktopState["startup"]["ownership"] | undefined): string {
  switch (ownership) {
    case "started_managed":
      return "Started By Desktop";
    case "attached_existing":
      return "Attached To Existing Backend";
    case "snapshot_only":
      return "Snapshot Only";
    default:
      return "Unavailable";
  }
}

function startupFailureLabel(kind: DesktopState["backend"]["startupFailureKind"] | undefined): string {
  switch (kind) {
    case "stale_dashboard_instance":
      return "Stale Dashboard Instance";
    case "stale_listener_conflict":
      return "Stale Listener / Port Conflict";
    case "build_mismatch":
      return "Build Mismatch";
    case "dashboard_api_not_ready":
      return "Health Up / API Not Ready";
    case "early_process_exit":
      return "Early Process Exit";
    case "permission_or_bind_failure":
      return "Permission / Bind Failure";
    case "environment_failure":
      return "Environment Failure";
    case "unexpected_startup_failure":
      return "Unexpected Startup Failure";
    default:
      return "None";
  }
}

function maskAccountNumber(value: unknown): string {
  const text = String(value ?? "").trim();
  if (!text) {
    return "Unknown";
  }
  if (text.length <= 4) {
    return text;
  }
  return `••••${text.slice(-4)}`;
}

function standaloneStrategyId(row: JsonRecord | null | undefined): string {
  return String(row?.standalone_strategy_id ?? row?.strategy_key ?? row?.id ?? "").trim();
}

function standaloneStrategyLabel(row: JsonRecord | null | undefined): string {
  const identity = standaloneStrategyId(row);
  if (identity) {
    return identity;
  }
  const strategy = String(row?.strategy_name ?? row?.display_name ?? row?.lane_id ?? "UNKNOWN").trim();
  const instrument = String(row?.instrument ?? row?.symbol ?? "").trim();
  return instrument ? `${strategy}__${instrument}` : strategy;
}

function laneHaltReasonLabel(value: unknown): string {
  const text = String(value ?? "").trim();
  if (!text) {
    return "Unknown halt reason";
  }
  const normalized = text.toUpperCase();
  if (normalized === "LANE_REALIZED_LOSER_LIMIT_PER_SESSION") {
    return "Lane realized loser limit per session reached";
  }
  if (normalized === "WARNING_OPEN_LOSS_BREACH") {
    return "Warning open-loss threshold breached";
  }
  return text.replace(/_/g, " ");
}

function laneHaltLatchedLabel(row: JsonRecord | null | undefined): string {
  return String(row?.risk_state ?? "").toUpperCase() === "HALTED_DEGRADATION" ? "Yes - latched until cleared" : "Unknown";
}

function runtimeFaultTitle(row: JsonRecord | null | undefined): string {
  return String(row?.code ?? row?.fault_code ?? "UNKNOWN_FAULT").trim() || "UNKNOWN_FAULT";
}

function openArtifact(api: OperatorDesktopApi, state: DesktopState | null, target: unknown): Promise<DesktopCommandResult> {
  if (!target || typeof target !== "string") {
    return Promise.resolve({ ok: false, message: "No artifact target is available." });
  }
  if (target.startsWith("http://") || target.startsWith("https://")) {
    return api.openExternalUrl(target);
  }
  if (target.startsWith("/api/") && state?.backendUrl) {
    return api.openExternalUrl(new URL(target.replace(/^\//, ""), state.backendUrl).toString());
  }
  return api.openPath(target);
}

function inferOperatorHalt(global: JsonRecord, paperReadiness: JsonRecord): boolean {
  if (typeof paperReadiness.operator_halt === "boolean") {
    return paperReadiness.operator_halt;
  }
  if (typeof paperReadiness.halt_reason === "string" && paperReadiness.halt_reason.trim()) {
    return true;
  }
  return global.entries_enabled === false;
}

function buildPreflightModel(input: {
  desktopState: DesktopState | null;
  global: JsonRecord;
  runtimeReadiness: JsonRecord;
  paperReadiness: JsonRecord;
}): PreflightModel {
  const { desktopState, global, runtimeReadiness, paperReadiness } = input;
  const runtimeValues = asRecord(runtimeReadiness.values);
  const health = asRecord(desktopState?.health);
  const laneEligibilityRows = asArray<JsonRecord>(paperReadiness.lane_eligibility_rows);
  const laneRiskRows = asArray<JsonRecord>(paperReadiness.lane_risk_rows);
  const blockingFaults = asArray<unknown>(runtimeReadiness.blocking_faults);
  const degradedInformationalFeeds = asArray<string>(runtimeValues.degraded_informational_feeds ?? runtimeReadiness.degraded_informational_feeds);
  const sourceLive = desktopState?.source.mode === "live_api";
  const backendHealthy = desktopState?.backend.state === "healthy";
  const dashboardProbeReady = health.ready === true;
  const runtimeStatus = String(runtimeValues.runtime_status ?? paperReadiness.runtime_phase ?? global.paper_label ?? "");
  const paperEnabled = runtimeValues.paper_enabled === undefined ? global.paper_running === true : runtimeValues.paper_enabled === true;
  const authReady = runtimeValues.auth_readiness === undefined ? String(global.auth_label ?? "").toUpperCase().includes("READY") : runtimeValues.auth_readiness === true;
  const entriesEnabled = (global.entries_enabled ?? paperReadiness.entries_enabled ?? runtimeValues.entries_enabled) === true;
  const operatorHalt = inferOperatorHalt(global, paperReadiness);
  const marketDataLive = String(global.market_data_label ?? runtimeValues.market_data_readiness ?? "").toUpperCase() === "LIVE";
  const currentSession = String(paperReadiness.current_detected_session ?? paperReadiness.runtime_phase ?? runtimeValues.runtime_status ?? "");
  const sessionKnown = Boolean(currentSession) && currentSession !== "UNKNOWN";
  const firstActionableBar = laneEligibilityRows
    .map((row) => row.latest_completed_bar_end_ts ?? row.last_processed_bar_end_ts)
    .find((value) => Boolean(value));
  const laneRiskIssues = laneRiskRows.filter((row) => String(row.risk_state ?? "UNKNOWN").toUpperCase() !== "OK");
  const degradationLaneIssues = laneRiskRows.filter((row) => String(row.risk_state ?? "").toUpperCase() === "HALTED_DEGRADATION");
  const laneRosterHealthy = laneEligibilityRows.length > 0 && laneRiskIssues.length === 0;
  const staleRuntime = global.stale === true;
  const criticalFaultsClear = blockingFaults.length === 0 && runtimeValues.blocking_faults_count !== undefined
    ? Number(runtimeValues.blocking_faults_count) === 0
    : blockingFaults.length === 0;
  const runtimeActiveDetail = paperEnabled
    ? "Paper runtime is active."
    : "Paper runtime is stopped, so trading readiness is not active yet.";
  const dashboardProbeDetail = dashboardProbeReady
    ? "The dashboard /health self-probe reports that operator_surface loaded successfully."
    : "The dashboard /health self-probe is not reporting operator_surface ready. This is a dashboard probe signal, not the same thing as trading readiness.";
  const marketDataDetail = marketDataLive
    ? "Market data surface is live."
    : !paperEnabled || runtimeStatus === "STOPPED"
      ? "Paper runtime is stopped, so live runtime market data is not active yet."
      : !authReady
        ? "Auth is not ready, so live market data cannot be confirmed yet."
        : staleRuntime
          ? "Runtime data is stale, so market data is not fresh enough to treat as live."
          : "Runtime is up, but market data is not reporting LIVE.";
  const backendDetail = sourceLive && backendHealthy
    ? "Desktop transport can reach a healthy backend server. This is server/API health, not by itself trading readiness."
    : desktopState?.backend.detail ?? "Backend state is unavailable.";
  const dashboardApiDetail = desktopState?.source.apiReachable
    ? "The live /api/dashboard endpoint is responding. This does not by itself mean the paper runtime is active."
    : desktopState?.source.detail ?? "Dashboard API status is unavailable.";

  const checks: PreflightCheck[] = [
    {
      key: "backend",
      label: "Backend / Server Healthy",
      status: sourceLive && backendHealthy ? "pass" : "fail",
      value: desktopState?.backend.label ?? "Unavailable",
      detail: backendDetail,
    },
    {
      key: "dashboard-api",
      label: "Dashboard API Responding",
      status: desktopState?.source.apiReachable ? "pass" : "fail",
      value: desktopState?.source.apiReachable ? "Yes" : "No",
      detail: dashboardApiDetail,
    },
    {
      key: "health-ready",
      label: "Dashboard Probe Ready (/health)",
      status: dashboardProbeReady ? "pass" : "fail",
      value: formatValue(health.ready ?? false),
      detail: dashboardProbeDetail,
    },
    {
      key: "runtime-active",
      label: "Paper Runtime Active",
      status: paperEnabled ? "pass" : "fail",
      value: paperEnabled ? "RUNNING" : runtimeStatus || "STOPPED",
      detail: runtimeActiveDetail,
    },
    {
      key: "entries",
      label: "Entries Enabled",
      status: entriesEnabled ? "pass" : "fail",
      value: formatValue(entriesEnabled),
      detail: entriesEnabled ? "Entries are enabled." : "Entries are disabled.",
    },
    {
      key: "halt",
      label: "Operator Halt Clear",
      status: operatorHalt ? "fail" : "pass",
      value: operatorHalt ? "Halt active" : "No halt",
      detail: operatorHalt ? String(paperReadiness.halt_reason ?? "Operator halt is active.") : "Operator halt is not active.",
    },
    {
      key: "market-data",
      label: "Market Data Live",
      status: marketDataLive ? "pass" : "fail",
      value: formatValue(global.market_data_label ?? runtimeValues.market_data_readiness),
      detail: marketDataDetail,
    },
    {
      key: "session",
      label: "Current Session Detected",
      status: sessionKnown ? "pass" : "warn",
      value: currentSession || "Unknown",
      detail: sessionKnown ? "Current runtime session is available." : "Current session is still UNKNOWN in the dashboard payload.",
    },
    {
      key: "first-bar",
      label: "First Actionable Completed Bar Expectation",
      status: firstActionableBar ? "pass" : "warn",
      value: firstActionableBar ? formatTimestamp(firstActionableBar) : "Not exposed",
      detail: firstActionableBar
        ? "A completed-bar timestamp is present in the lane eligibility surface."
        : "The current dashboard payload does not expose a dedicated first-actionable-bar expectation field.",
    },
    {
      key: "lanes",
      label: "Lane Eligibility Roster",
      status: laneRosterHealthy ? "pass" : laneEligibilityRows.length ? "warn" : "fail",
      value: laneEligibilityRows.length ? `${laneEligibilityRows.length} lanes` : "No rows",
      detail: laneRosterHealthy
        ? "Lane eligibility and lane risk rows are present with OK risk state."
        : laneEligibilityRows.length
          ? degradationLaneIssues.length
            ? `${degradationLaneIssues.length} lanes are in HALTED_DEGRADATION. Realized loser limit per session reached; lane halt stays latched until cleared.`
            : `${laneRiskIssues.length} lane risk rows are not OK.`
          : "Lane eligibility rows are missing from the readiness payload.",
    },
    {
      key: "faults",
      label: "Runtime Fault Blockers",
      status: !staleRuntime && criticalFaultsClear ? "pass" : "fail",
      value: staleRuntime ? "Stale" : blockingFaults.length ? `${blockingFaults.length} blocker${blockingFaults.length === 1 ? "" : "s"}` : "Clear",
      detail:
        !staleRuntime && criticalFaultsClear
          ? "No stale-runtime indicator or blocking faults are active."
          : staleRuntime
            ? "Dashboard reports stale runtime data."
            : "True runtime blockers are active in runtime readiness. Informational feed degradation is tracked separately.",
    },
    {
      key: "feeds",
      label: "Informational Feed Degradation",
      status: degradedInformationalFeeds.length ? "warn" : "pass",
      value: degradedInformationalFeeds.length ? `${degradedInformationalFeeds.length} degraded feeds` : "Clear",
      detail: degradedInformationalFeeds.length
        ? `Lower-severity informational feeds are degraded: ${degradedInformationalFeeds.join(", ")}. These do not count as blocking runtime faults.`
        : "No degraded informational feeds are currently surfaced.",
    },
  ];

  const blockers = checks.filter((item) => item.status === "fail").map((item) => `${item.label}: ${item.detail}`);
  const warnings = checks.filter((item) => item.status === "warn").map((item) => `${item.label}: ${item.detail}`);
  const informational = degradedInformationalFeeds.length
    ? [`Informational feed degradation: ${degradedInformationalFeeds.join(", ")}. These are visible context items, not blocking runtime faults.`]
    : [];
  const verdict = blockers.length ? "NOT READY" : warnings.length ? "DEGRADED" : "READY";
  return { verdict, checks, blockers, warnings, informational };
}

function buildDiagnosticsSummary(input: {
  desktopState: DesktopState | null;
  global: JsonRecord;
  runtimeReadiness: JsonRecord;
  paperReadiness: JsonRecord;
  productionLink: JsonRecord;
  sameUnderlyingConflicts: JsonRecord;
}): string {
  const { desktopState, global, runtimeReadiness, paperReadiness, productionLink, sameUnderlyingConflicts } = input;
  const runtimeValues = asRecord(runtimeReadiness.values);
  const laneEligibilityRows = asArray<JsonRecord>(paperReadiness.lane_eligibility_rows);
  const eligibleNow = laneEligibilityRows.filter((row) => row.eligible_now === true).length;
  const blockedNow = laneEligibilityRows.filter((row) => row.eligible_now === false).length;
  const latestError = desktopState?.backend.lastError ?? desktopState?.errors?.[0] ?? "None";
  const localAuth = desktopState?.localAuth;
  const productionAccounts = asArray<JsonRecord>(asRecord(productionLink.accounts).rows);
  const selectedProductionAccount = productionAccounts.find((row) => row.selected === true) ?? productionAccounts[0] ?? null;
  const productionDiagnostics = asRecord(productionLink.diagnostics);
  const productionReconciliation = asRecord(productionLink.reconciliation);
  const productionHealth = asRecord(productionLink.health);
  const productionManualSafety = asRecord(productionLink.manual_order_safety);
  const productionCapabilities = asRecord(productionLink.capabilities);
  const nextLiveVerificationStep = asRecord(productionCapabilities.next_live_verification_step);
  const liveVerifiedOrderKeys = asArray<string>(productionCapabilities.live_verified_order_keys);
  const orderLifecycleReadiness = asRecord(productionDiagnostics.order_lifecycle_readiness);
  const sameUnderlyingEvents = asRecord(sameUnderlyingConflicts.events);
  const latestSameUnderlyingEvent = asRecord(sameUnderlyingEvents.latest_event);
  const latestEntryBlockedEvent = asRecord(sameUnderlyingEvents.latest_entry_blocked_event);

  return [
    `App Version: ${desktopState?.appVersion ?? "Unknown"}`,
    `Backend PID: ${formatValue(desktopState?.backend.pid)}`,
    `Backend URL: ${formatValue(desktopState?.backendUrl)}`,
    `Chosen Port: ${formatValue(desktopState?.startup.chosenPort)}`,
    `Preferred URL: ${formatValue(desktopState?.startup.preferredUrl)}`,
    `Backend Ownership: ${ownershipLabel(desktopState?.startup.ownership)}`,
    `Health Status: ${formatValue(desktopState?.backend.healthStatus)}`,
    `API Status: ${formatValue(desktopState?.backend.apiStatus)}`,
    `Source Mode: ${desktopState?.source.label ?? "Unknown"}`,
    `Backend State: ${desktopState?.backend.label ?? "Unknown"}`,
    `Local Operator Auth: ${formatValue(localAuth?.auth_session_active ? "AUTHENTICATED" : localAuth?.auth_available ? "REQUIRES AUTH" : "UNAVAILABLE")}`,
    `Local Operator Auth Method: ${formatValue(localAuth?.auth_method ?? "NONE")}`,
    `Local Operator Auth Last Result: ${formatValue(localAuth?.last_auth_result ?? "NONE")}`,
    `Local Operator Auth Last Authenticated At: ${formatTimestamp(localAuth?.last_authenticated_at)}`,
    `Local Operator Auth Session Expires: ${formatTimestamp(localAuth?.auth_session_expires_at)}`,
    `Local Operator Identity: ${formatValue(localAuth?.local_operator_identity)}`,
    `Local Secret Protection: ${formatValue(localAuth?.secret_protection.detail ?? "Unavailable")}`,
    `Startup Failure Kind: ${startupFailureLabel(desktopState?.backend.startupFailureKind)}`,
    `Current Session: ${formatValue(paperReadiness.current_detected_session ?? paperReadiness.runtime_phase ?? global.current_session_date)}`,
    `Runtime Freshness: ${formatValue(global.stale ? "STALE" : formatRelativeAge(global.last_update_timestamp ?? desktopState?.refreshedAt))}`,
    `Lane Eligibility Summary: total=${laneEligibilityRows.length}, eligible=${eligibleNow}, blocked=${blockedNow}`,
    `Entries Enabled: ${formatValue(global.entries_enabled ?? paperReadiness.entries_enabled ?? runtimeValues.entries_enabled)}`,
    `Operator Halt: ${formatValue(inferOperatorHalt(global, paperReadiness))}`,
    `Production Link Status: ${formatValue(productionLink.label ?? productionLink.status ?? "Disabled")}`,
    `Production Account: ${selectedProductionAccount ? `${maskAccountNumber(selectedProductionAccount.account_number)} (${formatValue(selectedProductionAccount.account_hash)})` : "None selected"}`,
    `Production API Base URL: ${formatValue(productionDiagnostics.trader_api_base_url)}`,
    `Production Broker Reachable: ${formatValue(asRecord(productionHealth.broker_reachable).label)}`,
    `Production Account Selected: ${formatValue(asRecord(productionHealth.account_selected).label)}`,
    `Production Last Balances Refresh: ${formatTimestamp(productionDiagnostics.last_balances_refresh_at)}`,
    `Production Last Positions Refresh: ${formatTimestamp(productionDiagnostics.last_positions_refresh_at)}`,
    `Production Last Orders Refresh: ${formatTimestamp(productionDiagnostics.last_orders_refresh_at)}`,
    `Production Live Submit Safety: ${formatValue(productionManualSafety.submit_enabled === true ? "READY" : "BLOCKED")}`,
    `Production Live-Verified Order Keys: ${liveVerifiedOrderKeys.length ? liveVerifiedOrderKeys.join(", ") : "None"}`,
    `Production Next Live Verification Step: ${formatValue(nextLiveVerificationStep.label ?? nextLiveVerificationStep.verification_key ?? "None")}`,
    `Production First Live Stock Limit Flags: ${formatValue(asArray<string>(asRecord(productionManualSafety.constraints).first_live_stock_limit_test?.required_flags).join(", ") || "None")}`,
    `Production First Live Stock Limit Whitelist: ${formatValue(asArray<string>(asRecord(asRecord(productionManualSafety.constraints).first_live_stock_limit_test?.required_config).manual_symbol_whitelist).join(", ") || "None configured")}`,
    `Production First Live Stock Limit Runbook: See Manual Order Ticket and Production Link Diagnostics.`,
    `Production Advanced TIF UI: ${formatValue(productionCapabilities.advanced_tif_ticket_support === true ? "DRY_RUN_ENABLED" : "DISABLED")}`,
    `Production OCO UI: ${formatValue(productionCapabilities.oco_ticket_support === true ? "DRY_RUN_ENABLED" : "DISABLED")}`,
    `Production Advanced Live Submit: EXTO=${formatValue(productionCapabilities.ext_exto_live_submit === true ? "ENABLED" : "DISABLED")}, OCO=${formatValue(productionCapabilities.oco_live_submit === true ? "ENABLED" : "DISABLED")}`,
    `Production Type Gates: stock_market=${formatValue(productionCapabilities.stock_market_live_submit === true ? "ENABLED" : "DISABLED")}, stock_limit=${formatValue(productionCapabilities.stock_limit_live_submit === true ? "ENABLED" : "DISABLED")}, stock_stop=${formatValue(productionCapabilities.stock_stop_live_submit === true ? "ENABLED" : "DISABLED")}, stock_stop_limit=${formatValue(productionCapabilities.stock_stop_limit_live_submit === true ? "ENABLED" : "DISABLED")}, trailing=${formatValue(productionCapabilities.trailing_live_submit === true ? "ENABLED" : "DISABLED")}, close=${formatValue(productionCapabilities.close_order_live_submit === true ? "ENABLED" : "DISABLED")}, futures=${formatValue(productionCapabilities.futures_live_submit === true ? "ENABLED" : "DISABLED")}`,
    `Production Reconciliation: ${formatValue(productionReconciliation.label ?? productionReconciliation.status ?? "Unknown")}`,
    `Production Last Manual Order: ${formatValue(asRecord(productionDiagnostics.last_manual_order_result).broker_order_id ?? asRecord(productionDiagnostics.last_manual_order_request).symbol)}`,
    `Production Last Manual Preview: ${formatValue(asRecord(productionDiagnostics.last_manual_order_preview).requested_at ?? "None")}`,
    `Production Order Lifecycle: request=${formatValue(asRecord(orderLifecycleReadiness.last_request).requested_at ?? "None")}, broker_order_id=${formatValue(asRecord(orderLifecycleReadiness.last_result).broker_order_id ?? "None")}, status=${formatValue(asRecord(orderLifecycleReadiness.last_result).status ?? asRecord(orderLifecycleReadiness.last_result).status_code ?? "Unknown")}`,
    `Same-Underlying Conflicts: total=${formatValue(asRecord(sameUnderlyingConflicts.summary).conflict_count ?? 0)}, holds=${formatValue(asRecord(sameUnderlyingConflicts.summary).holding_count ?? 0)}, expired=${formatValue(asRecord(sameUnderlyingConflicts.summary).hold_expired_count ?? 0)}, stale=${formatValue(asRecord(sameUnderlyingConflicts.summary).stale_count ?? 0)}`,
    `Same-Underlying Latest Event: ${formatValue(latestSameUnderlyingEvent.event_type ?? "None")} at ${formatTimestamp(latestSameUnderlyingEvent.occurred_at)}`,
    `Same-Underlying Latest Entry Block: ${formatValue(latestEntryBlockedEvent.blocked_standalone_strategy_id ?? latestEntryBlockedEvent.instrument ?? "None")} at ${formatTimestamp(latestEntryBlockedEvent.occurred_at)}`,
    `Latest Error: ${latestError}`,
    `Production Link Error: ${formatValue(productionDiagnostics.last_error)}`,
    `Action Hint: ${desktopState?.startup.recommendedAction ?? desktopState?.backend.actionHint ?? "None"}`,
  ].join("\n");
}

function deriveRepoRootFromDesktopState(desktopState: DesktopState | null): string | null {
  const candidates = [desktopState?.backendLogPath, desktopState?.desktopLogPath, desktopState?.runtimeLogPath];
  for (const candidate of candidates) {
    if (!candidate) {
      continue;
    }
    const markerIndex = candidate.indexOf("/outputs/");
    if (markerIndex > 0) {
      return candidate.slice(0, markerIndex);
    }
  }
  return null;
}

function deriveSundayRunbookPath(desktopState: DesktopState | null): string | null {
  const repoRoot = deriveRepoRootFromDesktopState(desktopState);
  return repoRoot ? `${repoRoot}/${SUNDAY_RUNBOOK_RELATIVE_PATH}` : null;
}

export function App() {
  const api = getApi();
  const [settings, setSettings] = useState<AppSettings>(() => readSettings());
  const [page, setPage] = useState<PageId>(() => hashPage(readSettings().defaultPage));
  const [desktopState, setDesktopState] = useState<DesktopState | null>(null);
  const [loading, setLoading] = useState(true);
  const [busyAction, setBusyAction] = useState<string | null>(null);
  const [lastResult, setLastResult] = useState<DesktopCommandResult | null>(null);
  const [recentActions, setRecentActions] = useState<RecentAction[]>([]);
  const [manualOrderForm, setManualOrderForm] = useState<ManualOrderFormState>(DEFAULT_MANUAL_ORDER_FORM);
  const [cancelOrderId, setCancelOrderId] = useState("");
  const [replaceOrderId, setReplaceOrderId] = useState("");
  const [positionsViewMode, setPositionsViewMode] = useState<PositionsViewMode>("broker");
  const [positionsDrawerOpen, setPositionsDrawerOpen] = useState(false);
  const [positionsDrawerTab, setPositionsDrawerTab] = useState<PositionsDrawerTab>("summary");
  const [selectedPositionsRowId, setSelectedPositionsRowId] = useState("");
  const [openPositionsMenuRowId, setOpenPositionsMenuRowId] = useState<string | null>(null);
  const [expandedSpreadRowIds, setExpandedSpreadRowIds] = useState<string[]>([]);
  const [positionsLayoutEditorOpen, setPositionsLayoutEditorOpen] = useState(false);
  const [positionsClosedTradesOpen, setPositionsClosedTradesOpen] = useState(false);
  const [positionsClosedTradesClassFilter, setPositionsClosedTradesClassFilter] = useState<"all" | "paper" | "experimental">("all");
  const [positionsClosedTradesPageSize, setPositionsClosedTradesPageSize] = useState(25);
  const [positionsLayoutState, setPositionsLayoutState] = useState<PositionsLayoutState>(() => readPositionsLayoutState());
  const [positionsSortByMode, setPositionsSortByMode] = useState<Record<PositionsViewMode, PositionsSortState>>({
    broker: { columnId: "openPnl", direction: "desc" },
    paper: { columnId: "lastActivity", direction: "desc" },
    combined: { columnId: "lastActivity", direction: "desc" },
  });
  const [selectedProductionPositionKey, setSelectedProductionPositionKey] = useState("");
  const [selectedAuditStrategyKey, setSelectedAuditStrategyKey] = useState("");
  const [selectedWorkspaceLaneId, setSelectedWorkspaceLaneId] = useState("");
  const [calendarPeriod, setCalendarPeriod] = useState<PnlCalendarPeriod>("monthly");
  const [calendarViewMode, setCalendarViewMode] = useState<PnlCalendarViewMode>("calendar");
  const [calendarSource, setCalendarSource] = useState<PnlCalendarSource>("all");
  const [calendarAutoRangeApplied, setCalendarAutoRangeApplied] = useState(false);
  const [calendarAnchorDate, setCalendarAnchorDate] = useState(() => new Date().toISOString().slice(0, 10));
  const [calendarCustomStart, setCalendarCustomStart] = useState(() => {
    const value = new Date();
    value.setDate(1);
    return value.toISOString().slice(0, 10);
  });
  const [calendarCustomEnd, setCalendarCustomEnd] = useState(() => new Date().toISOString().slice(0, 10));
  const [selectedCalendarDay, setSelectedCalendarDay] = useState<string | null>(null);
  const [calendarContextLabel, setCalendarContextLabel] = useState<string | null>(null);
  const [selectedSameUnderlyingConflictInstrument, setSelectedSameUnderlyingConflictInstrument] = useState("");
  const [sameUnderlyingOperatorLabel, setSameUnderlyingOperatorLabel] = useState("manual operator");
  const [sameUnderlyingReviewNote, setSameUnderlyingReviewNote] = useState("");
  const [sameUnderlyingHoldExpiresAt, setSameUnderlyingHoldExpiresAt] = useState("");
  const [sameUnderlyingEventInstrumentFilter, setSameUnderlyingEventInstrumentFilter] = useState("");
  const [sameUnderlyingEventTypeFilter, setSameUnderlyingEventTypeFilter] = useState("");
  const [strategyLensIdentityFilter, setStrategyLensIdentityFilter] = useState("");
  const [strategyLensFamilyFilter, setStrategyLensFamilyFilter] = useState("");
  const [strategyLensInstrumentFilter, setStrategyLensInstrumentFilter] = useState("");
  const [strategyLensStatusFilter, setStrategyLensStatusFilter] = useState("");
  const [strategyLensRuntimeStateFilter, setStrategyLensRuntimeStateFilter] = useState("");
  const [strategyLensAmbiguityFilter, setStrategyLensAmbiguityFilter] = useState("");
  const [auditStrategyIdentityFilter, setAuditStrategyIdentityFilter] = useState("");
  const [auditFamilyFilter, setAuditFamilyFilter] = useState("");
  const [auditInstrumentFilter, setAuditInstrumentFilter] = useState("");
  const [auditSessionFilter, setAuditSessionFilter] = useState("");
  const [auditVerdictFilter, setAuditVerdictFilter] = useState("");
  const [strategyFilterKey, setStrategyFilterKey] = useState("");
  const [strategyFamilyFilter, setStrategyFamilyFilter] = useState("");
  const [strategyInstrumentFilter, setStrategyInstrumentFilter] = useState("");
  const [strategySignalFamilyFilter, setStrategySignalFamilyFilter] = useState("");
  const [strategyTradeDateFilter, setStrategyTradeDateFilter] = useState("");
  const [strategyTradeSessionFilter, setStrategyTradeSessionFilter] = useState("");
  const [strategyTradeStatusFilter, setStrategyTradeStatusFilter] = useState("");
  const [clock, setClock] = useState(() => new Date());
  const [isVisible, setIsVisible] = useState(() => document.visibilityState !== "hidden");
  const refreshInFlightRef = useRef(false);
  const positionsBrokerRefreshInFlightRef = useRef(false);
  const positionsMenuContainerRef = useRef<HTMLDivElement | null>(null);
  const mountedRef = useRef(true);

  useEffect(() => {
    return () => {
      mountedRef.current = false;
    };
  }, []);

  useEffect(() => {
    const onHashChange = () => setPage(hashPage(settings.defaultPage));
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, [settings.defaultPage]);

  useEffect(() => {
    writeSettings(settings);
  }, [settings]);

  useEffect(() => {
    writePositionsLayoutState(positionsLayoutState);
  }, [positionsLayoutState]);

  useEffect(() => {
    if (!window.location.hash) {
      window.location.hash = `#/${settings.defaultPage}`;
    }
  }, [settings.defaultPage]);

  useEffect(() => {
    if (!openPositionsMenuRowId) {
      return;
    }
    const closeMenuOnOutsidePointer = (event: PointerEvent) => {
      const target = event.target;
      if (target instanceof Node && positionsMenuContainerRef.current?.contains(target)) {
        return;
      }
      setOpenPositionsMenuRowId(null);
    };
    const closeMenuOnEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setOpenPositionsMenuRowId(null);
      }
    };
    document.addEventListener("pointerdown", closeMenuOnOutsidePointer);
    window.addEventListener("keydown", closeMenuOnEscape);
    return () => {
      document.removeEventListener("pointerdown", closeMenuOnOutsidePointer);
      window.removeEventListener("keydown", closeMenuOnEscape);
    };
  }, [openPositionsMenuRowId]);

  useEffect(() => {
    const timer = window.setInterval(() => setClock(new Date()), 1000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    const onVisibilityChange = () => setIsVisible(document.visibilityState !== "hidden");
    document.addEventListener("visibilitychange", onVisibilityChange);
    return () => document.removeEventListener("visibilitychange", onVisibilityChange);
  }, []);

  async function refreshState(): Promise<void> {
    if (refreshInFlightRef.current) {
      return;
    }
    refreshInFlightRef.current = true;
    try {
      const state = await api.getDesktopState();
      if (!mountedRef.current) {
        return;
      }
      setDesktopState(state);
      setLoading(false);
    } finally {
      refreshInFlightRef.current = false;
    }
  }

  async function manualRefresh(): Promise<void> {
    setBusyAction("refresh");
    try {
      await refreshState();
      const result: DesktopCommandResult = {
        ok: true,
        message: "Operator state refreshed.",
      };
      setLastResult(result);
      setRecentActions((current) => [
        {
          id: `${Date.now()}-refresh`,
          label: "refresh",
          ok: true,
          message: "Operator state refreshed.",
          occurredAt: new Date().toISOString(),
        },
        ...current,
      ].slice(0, 8));
    } finally {
      setBusyAction(null);
    }
  }

  useEffect(() => {
    void refreshState();
  }, []);

  useEffect(() => {
    if (isVisible && !refreshInFlightRef.current) {
      void refreshState();
    }
  }, [isVisible]);

  useEffect(() => {
    if (!settings.refreshSeconds || settings.refreshSeconds <= 0) {
      return;
    }
    const timer = window.setInterval(() => {
      if (!isVisible || busyAction || refreshInFlightRef.current) {
        return;
      }
      void refreshState();
    }, settings.refreshSeconds * 1000);
    return () => window.clearInterval(timer);
  }, [busyAction, isVisible, settings.refreshSeconds]);

  useEffect(() => {
    if (page !== "positions" || !isVisible || busyAction || refreshInFlightRef.current) {
      return;
    }
    const runPositionsPagePoll = () => {
      if (busyAction || refreshInFlightRef.current || document.visibilityState === "hidden") {
        return;
      }
      void refreshState();
    };
    runPositionsPagePoll();
    const timer = window.setInterval(runPositionsPagePoll, POSITIONS_PAGE_POLL_SECONDS * 1000);
    return () => window.clearInterval(timer);
  }, [busyAction, isVisible, page]);

  useEffect(() => {
    const brokerModeActive = page === "positions" && (positionsViewMode === "broker" || positionsViewMode === "combined");
    const canForceBrokerRefresh =
      brokerModeActive &&
      isVisible &&
      !busyAction &&
      productionLinkEnabled() &&
      desktopState?.source.canRunLiveActions === true &&
      desktopState?.source.mode === "live_api";
    if (!canForceBrokerRefresh) {
      return;
    }
    const runBrokerRefresh = async () => {
      if (positionsBrokerRefreshInFlightRef.current || document.visibilityState === "hidden") {
        return;
      }
      positionsBrokerRefreshInFlightRef.current = true;
      try {
        const result = await api.runProductionLinkAction("refresh", {});
        if (!mountedRef.current) {
          return;
        }
        if (result.state) {
          setDesktopState(result.state);
        } else {
          await refreshState();
        }
      } finally {
        positionsBrokerRefreshInFlightRef.current = false;
      }
    };
    void runBrokerRefresh();
    const timer = window.setInterval(() => {
      void runBrokerRefresh();
    }, POSITIONS_PAGE_BROKER_REFRESH_SECONDS * 1000);
    return () => window.clearInterval(timer);
  }, [api, busyAction, desktopState?.source.canRunLiveActions, desktopState?.source.mode, isVisible, page, positionsViewMode]);

  useEffect(() => {
    setSameUnderlyingReviewNote("");
    setSameUnderlyingHoldExpiresAt("");
  }, [selectedSameUnderlyingConflictInstrument]);

  async function runCommand(
    label: string,
    command: () => Promise<DesktopCommandResult>,
    options?: { confirmMessage?: string; requiresLive?: boolean },
  ): Promise<void> {
    if (options?.confirmMessage && !window.confirm(options.confirmMessage)) {
      return;
    }
    if (options?.requiresLive && !desktopState?.source.canRunLiveActions) {
      const blockedResult: DesktopCommandResult = {
        ok: false,
        message: "Live operator action is unavailable while the app is not on the live API.",
        detail: desktopState?.source.detail ?? "Snapshot fallback is active.",
        state: desktopState ?? undefined,
      };
      setLastResult(blockedResult);
      setRecentActions((current) => [
        {
          id: `${Date.now()}-${label}`,
          label,
          ok: false,
          message: blockedResult.message,
          detail: blockedResult.detail,
          occurredAt: new Date().toISOString(),
        },
        ...current,
      ].slice(0, 8));
      return;
    }

    setBusyAction(label);
    try {
      const result = await command();
      setLastResult(result);
      setRecentActions((current) => [
        {
          id: `${Date.now()}-${label}`,
          label,
          ok: result.ok,
          message: result.message,
          detail: result.detail,
          occurredAt: new Date().toISOString(),
        },
        ...current,
      ].slice(0, 8));
      if (result.state) {
        setDesktopState(result.state);
      } else {
        await refreshState();
      }
    } finally {
      setBusyAction(null);
    }
  }

  function sameUnderlyingActionPayload(): JsonRecord {
    const note = sameUnderlyingReviewNote.trim();
    return {
      instrument: String(selectedSameUnderlyingConflict?.instrument ?? ""),
      operator_label: sameUnderlyingOperatorLabel.trim() || "manual operator",
      note: note || undefined,
      reason: note || undefined,
      acknowledgement_note: note || undefined,
      override_reason: note || undefined,
      hold_reason: note || undefined,
      hold_expires_at: sameUnderlyingHoldExpiresAt ? new Date(sameUnderlyingHoldExpiresAt).toISOString() : undefined,
  };
}

function paperStartupCategoryFromReason(reason: string): string {
  const text = reason.toLowerCase();
  if (!text.trim()) {
    return "unknown";
  }
  if (text.includes("temporary paper") || text.includes("startup mapping") || text.includes("missing lane")) {
    return "config mismatch";
  }
  if (text.includes("auth")) {
    return "auth";
  }
  if (
    text.includes("review") ||
    text.includes("guarded startup") ||
    text.includes("inherited prior-session risk") ||
    text.includes("reconciliation") ||
    text.includes("fault") ||
    text.includes("risk") ||
    text.includes("halt")
  ) {
    return "readiness";
  }
  if (
    text.includes("signal 15") ||
    text.includes("stop after current cycle") ||
    text.includes("stopped after") ||
    text.includes("runtime exited") ||
    text.includes("process")
  ) {
    return "process lifetime";
  }
  return "runtime";
}

function paperStartupCategoryTone(category: string): Tone {
  const normalized = category.trim().toLowerCase();
  if (normalized === "none" || normalized === "running") {
    return "good";
  }
  if (normalized === "readiness" || normalized === "auth" || normalized === "config mismatch" || normalized === "backend/api") {
    return "warn";
  }
  if (normalized === "process lifetime" || normalized === "runtime") {
    return "danger";
  }
  return "muted";
}

  function productionLinkEnabled(): boolean {
    return productionLink.enabled === true;
  }

  function buildManualOrderPayload(): JsonRecord {
    const structureType = manualOrderForm.structureType;
    return {
      account_hash: manualOrderForm.accountHash,
      symbol: manualOrderForm.symbol.trim().toUpperCase(),
      asset_class: manualOrderForm.assetClass,
      structure_type: structureType,
      intent_type: manualOrderForm.intentType,
      side: structureType === "OCO" ? "OCO" : manualOrderForm.side,
      quantity: manualOrderForm.quantity,
      order_type: structureType === "OCO" ? "OCO" : manualOrderForm.orderType,
      limit_price: structureType === "OCO" ? null : manualOrderForm.limitPrice || null,
      stop_price: structureType === "OCO" ? null : manualOrderForm.stopPrice || null,
      trail_value_type: structureType === "OCO" ? null : manualOrderForm.trailValueType || null,
      trail_value: structureType === "OCO" ? null : manualOrderForm.trailValue || null,
      trail_trigger_basis: structureType === "OCO" ? null : manualOrderForm.trailTriggerBasis || null,
      trail_limit_offset: structureType === "OCO" ? null : manualOrderForm.trailLimitOffset || null,
      time_in_force: manualOrderForm.timeInForce,
      session: manualOrderForm.session,
      operator_note: manualOrderForm.operatorNote.trim() || null,
      review_confirmed: manualOrderForm.reviewConfirmed,
      oco_group_id: structureType === "OCO" ? `oco-${manualOrderForm.symbol.trim().toUpperCase() || "ticket"}` : null,
      oco_legs:
        structureType === "OCO"
          ? manualOrderForm.ocoLegs.map((leg) => ({
              leg_label: leg.legLabel,
              side: leg.side,
              quantity: leg.quantity,
              order_type: leg.orderType,
              limit_price: leg.limitPrice || null,
              stop_price: leg.stopPrice || null,
              trail_value_type: leg.trailValueType || null,
              trail_value: leg.trailValue || null,
              trail_trigger_basis: leg.trailTriggerBasis || null,
              trail_limit_offset: leg.trailLimitOffset || null,
            }))
          : [],
    };
  }

  function updateOcoLeg(index: number, field: keyof OcoLegFormState, value: string): void {
    setManualOrderForm((current) => ({
      ...current,
      ocoLegs: current.ocoLegs.map((leg, legIndex) =>
        legIndex === index
          ? {
              ...leg,
              [field]: field === "side" || field === "orderType" || field === "trailTriggerBasis" || field === "trailValueType" ? value.toUpperCase() : value,
            }
          : leg,
      ),
    }));
  }

  const dashboard = desktopState?.dashboard ?? null;
  const localOperatorAuth = asRecord(desktopState?.localAuth);
  const localAuthEvents = asArray<JsonRecord>(desktopState?.localAuth.recent_events);
  const latestLocalAuthEvent = asRecord(desktopState?.localAuth.latest_event);
  const global = asRecord(dashboard?.global);
  const operatorSurface = asRecord(dashboard?.operator_surface);
  const runtimeReadiness = asRecord(operatorSurface.runtime_readiness);
  const runtimeValues = asRecord(runtimeReadiness.values);
  const portfolio = asRecord(operatorSurface.operator_metrics_portfolio);
  const marketContext = asRecord(operatorSurface.market_context ?? dashboard?.market_context);
  const instrumentRollup = asArray<JsonRecord>(asRecord(operatorSurface.operator_metrics_by_instrument).rows);
  const laneRows = asArray<JsonRecord>(operatorSurface.lane_rows);
  const currentPositions = asArray<JsonRecord>(asRecord(operatorSurface.current_active_positions).rows);
  const shadow = asRecord(dashboard?.shadow);
  const paper = asRecord(dashboard?.paper);
  const paperApprovedModels = asRecord(paper.approved_models);
  const approvedModelRows = asArray<JsonRecord>(paperApprovedModels.rows);
  const approvedModelDetailsByBranch = asRecord(paperApprovedModels.details_by_branch);
  const paperEvents = asRecord(paper.events);
  const paperAlertEvents = asArray<JsonRecord>(paperEvents.alerts);
  const paperAlertsState = asRecord(paper.alerts_state);
  const paperActiveAlertRows = asArray<JsonRecord>(paperAlertsState.active_alerts);
  const researchCapture = asRecord(dashboard?.research_capture);
  const paperReadiness = asRecord(paper.readiness);
  const paperEntryEligibility = asRecord(paper.entry_eligibility);
  const paperPreSessionReview = asRecord(dashboard?.paper_pre_session_review);
  const paperRunStart = asRecord(dashboard?.paper_run_start);
  const paperRunStartCurrent = asRecord(paperRunStart.current);
  const paperRunStartBlockedRows = asArray<JsonRecord>(paperRunStart.blocked_history);
  const paperRuntimeRecovery = asRecord(paper.runtime_recovery);
  const paperSoakContinuity = asRecord(paper.soak_continuity);
  const paperBrokerTruthShadowValidation = asRecord(paper.broker_truth_shadow_validation);
  const paperLiveTimingSummary = asRecord(paper.live_timing_summary);
  const paperLiveTimingValidation = asRecord(paper.live_timing_validation);
  const paperSoakValidation = asRecord(paper.soak_validation);
  const paperSoakExtended = asRecord(paper.soak_extended);
  const paperSoakUnattended = asRecord(paper.soak_unattended);
  const paperStrategyPerformance = asRecord(paper.strategy_performance);
  const paperTrackedStrategies = asRecord(paper.tracked_strategies);
  const trackedStrategyRows = asArray<JsonRecord>(paperTrackedStrategies.rows);
  const trackedStrategyDetailsById = asRecord(paperTrackedStrategies.details_by_strategy_id);
  const strategyRuntimeSummary = asRecord(paper.strategy_runtime_summary);
  const paperSignalIntentFillAudit = asRecord(paper.signal_intent_fill_audit);
  const paperExitParitySummary = asRecord(paper.exit_parity_summary);
  const shadowLiveSummary = asRecord(shadow.live_shadow_summary ?? asRecord(shadow.raw_operator_status).live_shadow_summary);
  const liveStrategyPilotSummary = asRecord(shadow.live_strategy_pilot_summary ?? asRecord(shadow.raw_operator_status).live_strategy_pilot_summary);
  const signalSelectivityAnalysis = asRecord(shadow.signal_selectivity_analysis);
  const temporaryPaperStrategyRows = asArray<JsonRecord>(asRecord(paper.non_approved_lanes).rows).filter((row) => isTemporaryPaperStrategyRow(row));
  const temporaryPaperRuntimeIntegrity = asRecord(paper.temporary_paper_runtime_integrity);
  const sameUnderlyingConflicts = asRecord(dashboard?.same_underlying_conflicts);
  const sameUnderlyingConflictRows = asArray<JsonRecord>(sameUnderlyingConflicts.rows);
  const sameUnderlyingConflictSummary = asRecord(sameUnderlyingConflicts.summary);
  const sameUnderlyingConflictNotes = asArray<string>(sameUnderlyingConflicts.notes);
  const sameUnderlyingEvents = asRecord(sameUnderlyingConflicts.events);
  const sameUnderlyingEventRows = asArray<JsonRecord>(sameUnderlyingEvents.rows);
  const sameUnderlyingLatestEvent = asRecord(sameUnderlyingEvents.latest_event);
  const sameUnderlyingLatestEntryBlockedEvent = asRecord(sameUnderlyingEvents.latest_entry_blocked_event);
  const sameUnderlyingEventSummary = asRecord(sameUnderlyingEvents.summary);
  const runtimeRegistry = asRecord(paper.runtime_registry);
  const runtimeRegistryLookup = useMemo(() => {
    const lookup = new Map<string, JsonRecord>();
    for (const row of asArray<JsonRecord>(runtimeRegistry.rows)) {
      const key = String(row.standalone_strategy_id ?? row.lane_id ?? row.strategy_key ?? "");
      if (key) {
        lookup.set(key, row);
      }
    }
    return lookup;
  }, [runtimeRegistry.rows]);
  const runtimeRegistryRows = useMemo(
    () =>
      mergeStrategyRows(
        asArray<JsonRecord>(runtimeRegistry.rows),
        temporaryPaperStrategyRows.map((row) => normalizeTemporaryPaperRegistryRow(row, runtimeRegistryLookup.get(String(row.lane_id ?? row.standalone_strategy_id ?? "")))),
      ),
    [runtimeRegistry.rows, temporaryPaperStrategyRows, runtimeRegistryLookup],
  );
  const runtimeRegistrySummary = asRecord(runtimeRegistry.summary);
  const strategyPerformanceRows = useMemo(
    () =>
      mergeStrategyRows(
        asArray<JsonRecord>(paperStrategyPerformance.rows),
        temporaryPaperStrategyRows.map((row) => normalizeTemporaryPaperPerformanceRow(row, runtimeRegistryLookup.get(String(row.lane_id ?? row.standalone_strategy_id ?? "")))),
      ),
    [paperStrategyPerformance.rows, temporaryPaperStrategyRows, runtimeRegistryLookup],
  );
  const strategyPortfolioSnapshot = asRecord(paperStrategyPerformance.portfolio_snapshot);
  const temporaryPaperPortfolioSnapshot = useMemo(
    () => buildStrategyPortfolioSnapshotFromRows(temporaryPaperStrategyRows.map((row) => normalizeTemporaryPaperPerformanceRow(row, runtimeRegistryLookup.get(String(row.lane_id ?? row.standalone_strategy_id ?? ""))))),
    [temporaryPaperStrategyRows, runtimeRegistryLookup],
  );
  const combinedStrategyPortfolioSnapshot = useMemo(
    () => buildStrategyPortfolioSnapshotFromRows(strategyPerformanceRows),
    [strategyPerformanceRows],
  );
  const strategyExecutionLikelihood = asRecord(paperStrategyPerformance.execution_likelihood);
  const strategyExecutionLikelihoodRows = asArray<JsonRecord>(strategyExecutionLikelihood.rows);
  const strategyTradeLogRows = asArray<JsonRecord>(paperStrategyPerformance.trade_log);
  const closedStrategyTradeRows = useMemo(
    () =>
      [...strategyTradeLogRows]
        .filter((row) => String(row.status ?? "").toUpperCase() === "CLOSED" || row.exit_timestamp)
        .sort((left, right) =>
          String(right.exit_timestamp ?? right.entry_timestamp ?? "").localeCompare(
            String(left.exit_timestamp ?? left.entry_timestamp ?? ""),
          ),
        ),
    [strategyTradeLogRows],
  );
  const strategyAttributionRows = asArray<JsonRecord>(asRecord(paperStrategyPerformance.attribution).rows);
  const signalIntentFillAuditRows = useMemo(
    () =>
      mergeStrategyRows(
        asArray<JsonRecord>(paperSignalIntentFillAudit.rows),
        temporaryPaperStrategyRows.map((row) => normalizeTemporaryPaperAuditRow(row, runtimeRegistryLookup.get(String(row.lane_id ?? row.standalone_strategy_id ?? "")))),
      ),
    [paperSignalIntentFillAudit.rows, temporaryPaperStrategyRows, runtimeRegistryLookup],
  );
  const laneEligibilityRows = asArray<JsonRecord>(paperReadiness.lane_eligibility_rows);
  const laneRiskRows = asArray<JsonRecord>(paperReadiness.lane_risk_rows);
  const productionLink = asRecord(dashboard?.production_link);
  const productionAccounts = asArray<JsonRecord>(asRecord(productionLink.accounts).rows);
  const productionPortfolio = asRecord(productionLink.portfolio);
  const productionBalances = asRecord(productionPortfolio.balances);
  const productionTotals = asRecord(productionPortfolio.account_totals);
  const productionPositions = asArray<JsonRecord>(productionPortfolio.positions);
  const productionQuotes = asRecord(productionLink.quotes);
  const productionQuoteRows = asArray<JsonRecord>(productionQuotes.rows);
  const productionOrders = asRecord(productionLink.orders);
  const productionOpenOrders = asArray<JsonRecord>(productionOrders.open_rows);
  const productionRecentFills = asArray<JsonRecord>(productionOrders.recent_fill_rows);
  const productionEvents = asArray<JsonRecord>(productionOrders.recent_events);
  const productionReconciliation = asRecord(productionLink.reconciliation);
  const productionReconciliationCategories = asRecord(productionReconciliation.categories);
  const productionDiagnostics = asRecord(productionLink.diagnostics);
  const productionConnection = asRecord(productionLink.connection);
  const productionFeatureFlags = asRecord(productionLink.feature_flags);
  const productionHealth = asRecord(productionLink.health);
  const productionFreshness = asRecord(productionLink.freshness);
  const productionCapabilities = asRecord(productionLink.capabilities);
  const productionManualSafety = asRecord(productionLink.manual_order_safety);
  const productionPilotMode = asRecord(productionManualSafety.pilot_mode);
  const productionPilotReadiness = asRecord(productionManualSafety.pilot_readiness);
  const productionPilotScope = asRecord(productionCapabilities.manual_live_pilot_scope);
  const supportedManualAssetClasses = asArray<string>(productionCapabilities.supported_manual_asset_classes);
  const supportedManualOrderTypes = asArray<string>(productionCapabilities.supported_manual_order_types);
  const supportedManualDryRunOrderTypes = asArray<string>(productionCapabilities.supported_manual_dry_run_order_types);
  const supportedManualTimeInForceValues = asArray<string>(productionCapabilities.supported_manual_time_in_force_values);
  const supportedManualSessionValues = asArray<string>(productionCapabilities.supported_manual_session_values);
  const orderTypeMatrixByAssetClass = asRecord(productionCapabilities.order_type_matrix_by_asset_class);
  const liveEnabledOrderTypesByAssetClass = asRecord(productionCapabilities.live_enabled_order_types_by_asset_class);
  const dryRunOnlyOrderTypesByAssetClass = asRecord(productionCapabilities.dry_run_only_order_types_by_asset_class);
  const orderTypeLiveVerificationMatrix = asRecord(productionCapabilities.order_type_live_verification_matrix);
  const orderTypeLiveVerificationSequence = asArray<JsonRecord>(productionCapabilities.order_type_live_verification_sequence);
  const nextLiveVerificationStep = asRecord(productionCapabilities.next_live_verification_step);
  const nearTermLiveVerificationRunbooks = asRecord(productionCapabilities.near_term_live_verification_runbooks);
  const liveVerifiedOrderKeys = asArray<string>(productionCapabilities.live_verified_order_keys);
  const productionRuntimeState = asRecord(productionLink.runtime_state);
  const productionPilotCycle = asRecord(asRecord(productionLink.pilot_cycle).last_completed ?? productionRuntimeState.last_completed_pilot_cycle ?? productionDiagnostics.last_completed_pilot_cycle);
  const productionPilotCycleBuy = asRecord(productionPilotCycle.buy);
  const productionPilotCycleClose = asRecord(productionPilotCycle.close);
  const productionPilotCycleFlat = asRecord(productionPilotCycle.flat_confirmation);
  const productionPilotCycleReconciliation = asRecord(productionPilotCycle.reconciliation_clear_confirmation);
  const productionPilotCyclePassive = asRecord(productionPilotCycle.passive_refresh_restart_confirmation);
  const productionRefreshSummary = asRecord(productionRuntimeState.last_refresh_summary);
  const productionLastOrdersRefreshAt = productionDiagnostics.last_orders_refresh_at ?? productionRefreshSummary.orders_refresh_at;
  const productionLastManualOrder = asRecord(productionRuntimeState.last_manual_order);
  const productionLastManualOrderPreview = asRecord(productionDiagnostics.last_manual_order_preview ?? productionRuntimeState.last_manual_order_preview);
  const productionManualLiveOrders = asRecord(productionLink.manual_live_orders);
  const productionManualLiveOrderSummary = asRecord(productionManualLiveOrders.summary);
  const productionActiveManualLiveOrders = asArray<JsonRecord>(productionManualLiveOrders.active_rows);
  const productionRecentManualLiveOrders = asArray<JsonRecord>(productionManualLiveOrders.recent_rows);
  const productionLifecycleAlertRows = asArray<JsonRecord>(asRecord(productionLink.alerts).recent);
  const productionLifecycleActiveAlertRows = asArray<JsonRecord>(asRecord(productionLink.alerts).active);
  const productionManualValidation = asRecord(productionLink.manual_validation);
  const productionManualValidationLatest = asRecord(productionManualValidation.latest_event);
  const productionManualValidationEvents = asArray<JsonRecord>(productionManualValidation.recent_events);
  const productionOrderLifecycleReadiness = asRecord(productionDiagnostics.order_lifecycle_readiness);
  const productionEndpointUncertainty = asArray<string>(productionDiagnostics.endpoint_uncertainty);
  const playback = asRecord(dashboard?.historical_playback);
  const strategyAnalysis = asRecord(dashboard?.strategy_analysis);
  const playbackStudyCatalog = asRecord(playback.study_catalog);
  const playbackStudyItems = asArray<JsonRecord>(playbackStudyCatalog.items);
  const playbackLatestStudyItems = useMemo(() => {
    const uniqueItems: JsonRecord[] = [];
    const seenStudyKeys = new Set<string>();
    for (const item of playbackStudyItems) {
      const studyKey = String(item.study_key ?? "").trim();
      if (!studyKey || seenStudyKeys.has(studyKey)) {
        continue;
      }
      seenStudyKeys.add(studyKey);
      uniqueItems.push(item);
    }
    return uniqueItems;
  }, [playbackStudyItems]);
  const playbackLatestRun = asRecord(playback.latest_run);
  const playbackLatestRunArtifacts = asRecord(playbackLatestRun.artifacts);
  const playbackSync = asRecord(dashboard?.historical_playback_sync);
  const playbackAggregateSummary = asRecord(playbackLatestRun.aggregate_portfolio_summary);
  const playbackPerStrategySummaries = asArray<JsonRecord>(playbackLatestRun.per_strategy_summaries);
  const playbackSelectedStudyPreview = asRecord(playback.selected_study ?? playbackLatestRun.strategy_study);
  const playbackSelectedStudyMeta = asRecord(playbackSelectedStudyPreview.meta);
  const playbackSelectedStudyKey = String(
    playbackSelectedStudyMeta.study_id
    ?? playbackStudyCatalog.selected_study_key
    ?? "",
  ).trim();
  const playbackStudyStatus = asRecord(playbackLatestRun.strategy_study_status ?? playback.strategy_study_status);
  const playbackStudyArtifactFound = playbackStudyStatus.artifact_found === true;
  const [playbackStudyLoaded, setPlaybackStudyLoaded] = useState<JsonRecord | null>(null);
  useEffect(() => {
    const artifactTarget = String(playbackLatestRunArtifacts.strategy_study_json ?? "").trim();
    const backendUrl = String(desktopState?.backendUrl ?? "").trim();
    if (!artifactTarget || !backendUrl || !playbackStudyArtifactFound) {
      setPlaybackStudyLoaded(null);
      return;
    }
    let cancelled = false;
    const artifactUrl = artifactTarget.startsWith("/api/")
      ? new URL(artifactTarget.replace(/^\//, ""), backendUrl).toString()
      : artifactTarget;
    void fetch(artifactUrl)
      .then((response) => {
        if (!response.ok) {
          throw new Error(`Selected playback study fetch failed (${response.status})`);
        }
        return response.json() as Promise<JsonRecord>;
      })
      .then((payload) => {
        if (!cancelled) {
          setPlaybackStudyLoaded(asRecord(payload));
        }
      })
      .catch(() => {
        if (!cancelled) {
          setPlaybackStudyLoaded(null);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [desktopState?.backendUrl, playbackLatestRunArtifacts.strategy_study_json, playbackStudyArtifactFound]);
  const playbackStudy = playbackStudyLoaded ?? playbackSelectedStudyPreview;
  const playbackStudyRows = asArray<JsonRecord>(playbackStudy.bars ?? playbackStudy.rows);
  const playbackStudySummary = asRecord(playbackStudy.summary);
  const playbackStudyAvailable =
    (playbackLatestStudyItems.length > 0 || playbackLatestRun.strategy_study_available === true) && playbackStudyRows.length > 0;
  const playbackStudyRunLoaded = playbackStudyStatus.run_loaded === true;
  const playbackStudyBaseTimeframe = formatValue(playbackStudyStatus.base_timeframe ?? "Not loaded");
  const playbackStudyStructuralTimeframe = formatValue(playbackStudyStatus.structural_signal_timeframe ?? playbackStudyStatus.base_timeframe ?? "Not loaded");
  const playbackStudyExecutionTimeframe = formatValue(playbackStudyStatus.execution_resolution ?? "Not loaded");
  const playbackStudyTruthMode = formatValue(playbackStudyStatus.study_mode ?? "baseline_parity_mode");
  const playbackStudyMode = replayStudyModeLabel(playbackStudyStatus.mode);
  const playbackStudyModeToneValue = replayStudyModeTone(playbackStudyStatus.mode);
  const playbackStudyTimingLabelValue = replayStudyTimingLabel(playbackStudyStatus.atp_timing_available === true, {
    runLoaded: playbackStudyRunLoaded,
    artifactFound: playbackStudyArtifactFound,
  });
  const playbackReplaySummaryAvailable =
    playbackLatestRun.replay_summary_available === true &&
    (playbackPerStrategySummaries.length > 0 || Object.keys(playbackAggregateSummary).length > 0);
  const playbackRunStampLabel = String(playbackLatestRun.run_stamp ?? "Not loaded");
  const playbackStrategyCount = Number(strategyAnalysis.strategy_count ?? 0);
  const playbackLaneCount = Number(strategyAnalysis.lane_count ?? 0);
  const playbackSyncInState = playbackSync.in_sync === true;
  const playbackSyncLabel = playback.available === true
    ? playbackSyncInState
      ? "Latest playback manifest loaded"
      : formatValue(playbackSync.detail ?? "Playback state is stale")
    : "Historical playback not loaded";
  const playbackCoverageDateKeysBySource = useMemo(() => {
    const coverage: Record<Exclude<PnlCalendarSource, "all" | "live" | "paper">, Set<string>> = {
      benchmark_replay: new Set<string>(),
      research_execution: new Set<string>(),
    };
    for (const item of playbackLatestStudyItems) {
      const studyMode = String(item.study_mode ?? "").trim();
      const source: Exclude<PnlCalendarSource, "all" | "live" | "paper"> =
        studyMode === "research_execution_mode" ? "research_execution" : "benchmark_replay";
      const startDate = dateKeyFromTimestamp(item.coverage_start);
      const endDate = dateKeyFromTimestamp(item.coverage_end);
      if (!startDate || !endDate) {
        continue;
      }
      for (let cursor = startDate; cursor <= endDate; cursor = addDays(cursor, 1)) {
        coverage[source].add(cursor);
      }
    }
    return coverage;
  }, [playbackLatestStudyItems]);
  const earliestPlaybackCoverageDate = useMemo(() => {
    const allDates = [
      ...playbackCoverageDateKeysBySource.benchmark_replay,
      ...playbackCoverageDateKeysBySource.research_execution,
    ].sort();
    return allDates[0] ?? null;
  }, [playbackCoverageDateKeysBySource]);
  const sortedSameUnderlyingConflictRows = useMemo(
    () =>
      [...sameUnderlyingConflictRows].sort((left, right) => {
        const severityDelta = sameUnderlyingConflictRank(left.severity) - sameUnderlyingConflictRank(right.severity);
        if (severityDelta !== 0) {
          return severityDelta;
        }
        return String(left.instrument ?? "").localeCompare(String(right.instrument ?? ""));
      }),
    [sameUnderlyingConflictRows],
  );
  const sameUnderlyingBlockingConflictRows = useMemo(
    () => sortedSameUnderlyingConflictRows.filter((row) => String(row.severity ?? "").toUpperCase() === "BLOCKING"),
    [sortedSameUnderlyingConflictRows],
  );
  const sameUnderlyingHoldingRows = useMemo(
    () => sortedSameUnderlyingConflictRows.filter((row) => row.hold_new_entries === true),
    [sortedSameUnderlyingConflictRows],
  );
  const sameUnderlyingExpiredRows = useMemo(
    () => sortedSameUnderlyingConflictRows.filter((row) => row.hold_expired === true),
    [sortedSameUnderlyingConflictRows],
  );
  const sameUnderlyingStaleRows = useMemo(
    () => sortedSameUnderlyingConflictRows.filter((row) => String(row.review_state_status ?? "").toUpperCase() === "STALE"),
    [sortedSameUnderlyingConflictRows],
  );
  const sameUnderlyingBrokerConflictRows = useMemo(
    () => sortedSameUnderlyingConflictRows.filter((row) => row.broker_overlap_present === true),
    [sortedSameUnderlyingConflictRows],
  );
  const strategyLensRows = useMemo(
    () => [...runtimeRegistryRows, ...strategyPerformanceRows, ...signalIntentFillAuditRows],
    [runtimeRegistryRows, signalIntentFillAuditRows, strategyPerformanceRows],
  );
  const sameUnderlyingEventInstrumentOptions = useMemo(
    () => Array.from(new Set(sameUnderlyingEventRows.map((row) => String(row.instrument ?? "").trim()).filter(Boolean))).sort(),
    [sameUnderlyingEventRows],
  );
  const sameUnderlyingEventTypeOptions = useMemo(
    () => Array.from(new Set(sameUnderlyingEventRows.map((row) => String(row.event_type ?? "").trim()).filter(Boolean))).sort(),
    [sameUnderlyingEventRows],
  );
  const filteredSameUnderlyingEventRows = useMemo(
    () =>
      sameUnderlyingEventRows.filter((row) => {
        if (sameUnderlyingEventInstrumentFilter && String(row.instrument ?? "") !== sameUnderlyingEventInstrumentFilter) {
          return false;
        }
        if (sameUnderlyingEventTypeFilter && String(row.event_type ?? "") !== sameUnderlyingEventTypeFilter) {
          return false;
        }
        return true;
      }),
    [sameUnderlyingEventInstrumentFilter, sameUnderlyingEventRows, sameUnderlyingEventTypeFilter],
  );
  const strategyLensIdentityOptions = useMemo(
    () => Array.from(new Set(strategyLensRows.map((row) => standaloneStrategyId(row)).filter(Boolean))).sort(),
    [strategyLensRows],
  );
  const strategyLensFamilyOptions = useMemo(
    () => Array.from(new Set(strategyLensRows.map((row) => String(row.family ?? row.source_family ?? row.strategy_family ?? "").trim()).filter(Boolean))).sort(),
    [strategyLensRows],
  );
  const strategyLensInstrumentOptions = useMemo(
    () => Array.from(new Set(strategyLensRows.map((row) => String(row.instrument ?? row.symbol ?? "").trim()).filter(Boolean))).sort(),
    [strategyLensRows],
  );
  const strategyLensStatusOptions = useMemo(
    () => Array.from(new Set(strategyLensRows.map((row) => strategyStatusLabel(row)).filter(Boolean))).sort(),
    [strategyLensRows],
  );
  const strategyLensMatches = (row: JsonRecord): boolean => {
    if (strategyLensIdentityFilter && standaloneStrategyId(row) !== strategyLensIdentityFilter) {
      return false;
    }
    if (strategyLensFamilyFilter && String(row.family ?? row.source_family ?? row.strategy_family ?? "") !== strategyLensFamilyFilter) {
      return false;
    }
    if (strategyLensInstrumentFilter && String(row.instrument ?? row.symbol ?? "") !== strategyLensInstrumentFilter) {
      return false;
    }
    if (strategyLensStatusFilter && strategyStatusLabel(row) !== strategyLensStatusFilter) {
      return false;
    }
    if (strategyLensRuntimeStateFilter && runtimeStateLabel(row) !== strategyLensRuntimeStateFilter) {
      return false;
    }
    if (strategyLensAmbiguityFilter && sameUnderlyingLabel(row) !== strategyLensAmbiguityFilter) {
      return false;
    }
    return true;
  };
  const filteredRuntimeRegistryRows = useMemo(
    () =>
      [...runtimeRegistryRows]
        .filter((row) => strategyLensMatches(row))
        .sort((left, right) => {
          const enabledDelta = Number(right.enabled === true) - Number(left.enabled === true);
          if (enabledDelta !== 0) {
            return enabledDelta;
          }
          const ambiguityDelta = Number(right.same_underlying_ambiguity === true) - Number(left.same_underlying_ambiguity === true);
          if (ambiguityDelta !== 0) {
            return ambiguityDelta;
          }
          return standaloneStrategyLabel(left).localeCompare(standaloneStrategyLabel(right));
        }),
    [runtimeRegistryRows, strategyLensAmbiguityFilter, strategyLensFamilyFilter, strategyLensIdentityFilter, strategyLensInstrumentFilter, strategyLensRuntimeStateFilter, strategyLensStatusFilter],
  );
  const filteredStrategyPerformanceRows = useMemo(
    () =>
      [...strategyPerformanceRows]
        .filter((row) => strategyLensMatches(row))
        .sort((left, right) => {
          const inPositionDelta =
            Number(String(right.position_side ?? "FLAT").toUpperCase() !== "FLAT") -
            Number(String(left.position_side ?? "FLAT").toUpperCase() !== "FLAT");
          if (inPositionDelta !== 0) {
            return inPositionDelta;
          }
          const faultDelta =
            Number(String(right.status ?? "").toUpperCase().startsWith("FAULT")) -
            Number(String(left.status ?? "").toUpperCase().startsWith("FAULT"));
          if (faultDelta !== 0) {
            return faultDelta;
          }
          return parseTimestampMs(right.latest_activity_timestamp) - parseTimestampMs(left.latest_activity_timestamp);
        }),
    [strategyLensAmbiguityFilter, strategyLensFamilyFilter, strategyLensIdentityFilter, strategyLensInstrumentFilter, strategyLensRuntimeStateFilter, strategyLensStatusFilter, strategyPerformanceRows],
  );
  const filteredStrategyExecutionLikelihoodRows = useMemo(
    () =>
      [...strategyExecutionLikelihoodRows]
        .filter((row) => strategyLensMatches(row))
        .sort((left, right) => parseTimestampMs(right.last_fire_timestamp) - parseTimestampMs(left.last_fire_timestamp)),
    [strategyExecutionLikelihoodRows, strategyLensAmbiguityFilter, strategyLensFamilyFilter, strategyLensIdentityFilter, strategyLensInstrumentFilter, strategyLensRuntimeStateFilter, strategyLensStatusFilter],
  );
  const strategyFilterOptions = useMemo(
    () => strategyPerformanceRows.map((row) => ({
      key: standaloneStrategyId(row),
      label: standaloneStrategyLabel(row),
    })),
    [strategyPerformanceRows],
  );
  const strategyFamilyOptions = useMemo(
    () => Array.from(new Set(strategyTradeLogRows.map((row) => String(row.family ?? row.source_family ?? "").trim()).filter(Boolean))).sort(),
    [strategyTradeLogRows],
  );
  const strategyInstrumentOptions = useMemo(
    () => Array.from(new Set(strategyTradeLogRows.map((row) => String(row.instrument ?? "").trim()).filter(Boolean))).sort(),
    [strategyTradeLogRows],
  );
  const strategySignalFamilyOptions = useMemo(
    () => Array.from(new Set(strategyTradeLogRows.map((row) => String(row.signal_family_label ?? row.signal_family ?? "").trim()).filter(Boolean))).sort(),
    [strategyTradeLogRows],
  );
  const strategyTradeDateOptions = useMemo(
    () => Array.from(new Set(strategyTradeLogRows.map((row) => tradeDateLabel(row)).filter(Boolean))).sort().reverse(),
    [strategyTradeLogRows],
  );
  const strategyTradeSessionOptions = useMemo(
    () => Array.from(new Set(strategyTradeLogRows.map((row) => tradeSessionLabel(row)).filter(Boolean))).sort(),
    [strategyTradeLogRows],
  );
  const strategyTradeStatusOptions = useMemo(
    () => Array.from(new Set(strategyTradeLogRows.map((row) => String(row.status ?? "").trim()).filter(Boolean))).sort(),
    [strategyTradeLogRows],
  );
  const filteredStrategyTradeLogRows = useMemo(
    () =>
      [...strategyTradeLogRows]
        .filter((row) => strategyLensMatches(row))
        .filter((row) => {
        if (strategyFilterKey && standaloneStrategyId(row) !== strategyFilterKey) {
          return false;
        }
        if (strategyFamilyFilter && String(row.family ?? row.source_family ?? "") !== strategyFamilyFilter) {
          return false;
        }
        if (strategyInstrumentFilter && String(row.instrument ?? "") !== strategyInstrumentFilter) {
          return false;
        }
        if (strategySignalFamilyFilter && String(row.signal_family_label ?? row.signal_family ?? "") !== strategySignalFamilyFilter) {
          return false;
        }
        if (strategyTradeDateFilter && tradeDateLabel(row) !== strategyTradeDateFilter) {
          return false;
        }
        if (strategyTradeSessionFilter && tradeSessionLabel(row) !== strategyTradeSessionFilter) {
          return false;
        }
        if (strategyTradeStatusFilter && String(row.status ?? "") !== strategyTradeStatusFilter) {
          return false;
        }
        return true;
      }),
    [
      strategyFamilyFilter,
      strategyFilterKey,
      strategyInstrumentFilter,
      strategySignalFamilyFilter,
      strategyTradeDateFilter,
      strategyTradeSessionFilter,
      strategyTradeStatusFilter,
      strategyTradeLogRows,
      strategyLensAmbiguityFilter,
      strategyLensFamilyFilter,
      strategyLensIdentityFilter,
      strategyLensInstrumentFilter,
      strategyLensRuntimeStateFilter,
      strategyLensStatusFilter,
    ],
  );
  const strategyPerformanceLimitations = asArray<string>(paperStrategyPerformance.notes);
  const strategyExecutionLikelihoodNotes = asArray<string>(strategyExecutionLikelihood.notes);
  const signalIntentFillAuditNotes = asArray<string>(paperSignalIntentFillAudit.notes);
  const signalIntentFillAuditSummary = asRecord(paperSignalIntentFillAudit.summary);
  const signalIntentFillAuditStrategyOptions = useMemo(
    () => Array.from(new Set(signalIntentFillAuditRows.map((row) => standaloneStrategyId(row)).filter(Boolean))).sort(),
    [signalIntentFillAuditRows],
  );
  const signalIntentFillAuditFamilyOptions = useMemo(
    () => Array.from(new Set(signalIntentFillAuditRows.map((row) => String(row.family ?? row.source_family ?? "").trim()).filter(Boolean))).sort(),
    [signalIntentFillAuditRows],
  );
  const signalIntentFillAuditInstrumentOptions = useMemo(
    () => Array.from(new Set(signalIntentFillAuditRows.map((row) => String(row.instrument ?? "").trim()).filter(Boolean))).sort(),
    [signalIntentFillAuditRows],
  );
  const signalIntentFillAuditSessionOptions = useMemo(
    () => Array.from(new Set(signalIntentFillAuditRows.map((row) => String(row.current_session ?? "").trim()).filter(Boolean))).sort(),
    [signalIntentFillAuditRows],
  );
  const signalIntentFillAuditVerdictOptions = useMemo(
    () => Array.from(new Set(signalIntentFillAuditRows.map((row) => String(row.audit_verdict ?? "").trim()).filter(Boolean))).sort(),
    [signalIntentFillAuditRows],
  );
  const filteredSignalIntentFillAuditRows = useMemo(
    () =>
      [...signalIntentFillAuditRows]
        .filter((row) => strategyLensMatches(row))
        .filter((row) => {
        if (auditStrategyIdentityFilter && standaloneStrategyId(row) !== auditStrategyIdentityFilter) {
          return false;
        }
        if (auditFamilyFilter && String(row.family ?? row.source_family ?? "") !== auditFamilyFilter) {
          return false;
        }
        if (auditInstrumentFilter && String(row.instrument ?? "") !== auditInstrumentFilter) {
          return false;
        }
        if (auditSessionFilter && String(row.current_session ?? "") !== auditSessionFilter) {
          return false;
        }
        if (auditVerdictFilter && String(row.audit_verdict ?? "") !== auditVerdictFilter) {
          return false;
        }
        return true;
      })
        .sort((left, right) => {
          const verdictDelta = auditVerdictRank(left.audit_verdict) - auditVerdictRank(right.audit_verdict);
          if (verdictDelta !== 0) {
            return verdictDelta;
          }
          return parseTimestampMs(right.last_processed_bar_end_ts) - parseTimestampMs(left.last_processed_bar_end_ts);
        }),
    [
      auditFamilyFilter,
      auditInstrumentFilter,
      auditSessionFilter,
      auditStrategyIdentityFilter,
      auditVerdictFilter,
      signalIntentFillAuditRows,
      strategyLensAmbiguityFilter,
      strategyLensFamilyFilter,
      strategyLensIdentityFilter,
      strategyLensInstrumentFilter,
      strategyLensRuntimeStateFilter,
      strategyLensStatusFilter,
    ],
  );
  const infoFiles = desktopState?.infoFiles ?? [];
  const researchCaptureTargetRows = asArray<JsonRecord>(researchCapture.target_rows);
  const researchCaptureFailedSymbols = asArray<JsonRecord>(researchCapture.failed_symbols);
  const actionLog = asArray<JsonRecord>(dashboard?.action_log);
  const dashboardMeta = asRecord(dashboard?.dashboard_meta);
  const diagnosticsSummary = useMemo(
    () =>
      buildDiagnosticsSummary({
        desktopState,
        global,
        runtimeReadiness,
        paperReadiness,
        productionLink,
        sameUnderlyingConflicts,
      }),
    [desktopState, global, paperReadiness, productionLink, runtimeReadiness, sameUnderlyingConflicts],
  );
  const sundayRunbookPath = deriveSundayRunbookPath(desktopState);
  const preflight = useMemo(
    () =>
      buildPreflightModel({
        desktopState,
        global,
        runtimeReadiness,
        paperReadiness,
      }),
    [desktopState, global, paperReadiness, runtimeReadiness],
  );
  const canRunLiveActions = Boolean(desktopState?.source.canRunLiveActions);
  const authReadyForPaperStartup =
    runtimeValues.auth_readiness === undefined ? String(global.auth_label ?? "").toUpperCase().includes("READY") : runtimeValues.auth_readiness === true;
  const tempPaperMismatchActive = String(temporaryPaperRuntimeIntegrity.mismatch_status ?? "").toUpperCase() !== "MATCHED";
  const paperStartupPrimaryReason = String(paperEntryEligibility.primary_reason ?? "").trim().toUpperCase();
  const latestPaperStartBlock = asRecord(paperRunStartBlockedRows[0]);
  const latestPaperStartBlockReason = String(latestPaperStartBlock.blocked_reason ?? latestPaperStartBlock.output ?? "").trim();
  const paperRuntimeRecoveryState = String(paperRuntimeRecovery.supervisor_status ?? paperRuntimeRecovery.status ?? "NOT_APPLICABLE").trim().toUpperCase();
  const paperRuntimeRecoveryMessage = String(
    paperRuntimeRecovery.operator_message ??
      paperRuntimeRecovery.detail ??
      paperRuntimeRecovery.reason ??
      "",
  ).trim();
  const paperRuntimeRecoveryNextAction = String(paperRuntimeRecovery.next_action ?? "").trim();
  const paperAutoStartEligible = paperRuntimeRecovery.auto_restart_eligible === true;
  const paperAutoRestartAllowed = paperRuntimeRecovery.auto_restart_allowed === true;
  const paperRuntimeRestartAttemptsInWindow = Number(paperRuntimeRecovery.restart_attempts_in_window ?? 0) || 0;
  const paperRuntimeRestartBudget = Number(paperRuntimeRecovery.max_auto_restarts_per_window ?? 0) || 0;
  const paperRuntimeRestartRemaining = Number(paperRuntimeRecovery.restart_attempts_remaining_in_window ?? 0) || 0;
  const paperRuntimeRestartSuppressed = paperRuntimeRecovery.restart_suppressed === true;
  const paperRuntimeRestartSuppressedUntil = paperRuntimeRecovery.restart_suppressed_until;
  const paperRuntimeRestartBackoffUntil = paperRuntimeRecovery.restart_backoff_until;
  const paperRuntimeLastRestartAttemptAt = paperRuntimeRecovery.last_restart_attempt_at ?? paperRuntimeRecovery.attempted_at;
  const paperRuntimeLastRestartResult = String(paperRuntimeRecovery.last_restart_result ?? "UNKNOWN").trim().toUpperCase();
  const paperStartupCategory = paperReadiness.runtime_running === true
    ? paperRuntimeRecoveryState === "AUTO_RESTART_SUCCEEDED"
      ? "process lifetime"
      : "running"
    : paperRuntimeRecoveryState === "AUTO_RESTART_SUPPRESSED"
      ? "restart suppression"
      : paperRuntimeRecoveryState === "AUTO_RESTART_BACKOFF"
        ? "restart backoff"
    : paperRuntimeRecoveryState === "AUTO_RESTART_FAILED"
      ? paperStartupCategoryFromReason(paperRuntimeRecoveryMessage)
      : paperRuntimeRecoveryState === "STOPPED_MANUAL_REQUIRED"
        ? paperStartupCategoryFromReason(paperRuntimeRecoveryMessage || latestPaperStartBlockReason)
        : paperRuntimeRecoveryState === "AUTO_RESTART_IN_PROGRESS"
          ? "runtime"
      : !canRunLiveActions
        ? "backend/api"
        : tempPaperMismatchActive
          ? "config mismatch"
          : !authReadyForPaperStartup
            ? "auth"
            : latestPaperStartBlockReason
              ? paperStartupCategoryFromReason(latestPaperStartBlockReason)
              : paperStartupPrimaryReason && paperStartupPrimaryReason !== "RUNTIME_STOPPED"
                ? "readiness"
                : paperAutoStartEligible
                  ? "none"
                  : "runtime";
  const paperStartupStateLabel = paperReadiness.runtime_running === true
    ? paperRuntimeRecoveryState === "AUTO_RESTART_SUCCEEDED"
      ? "AUTO-RESTART SUCCEEDED"
      : "RUNNING"
    : paperRuntimeRecoveryState === "AUTO_RESTART_SUPPRESSED"
      ? "AUTO-RESTART SUPPRESSED"
      : paperRuntimeRecoveryState === "AUTO_RESTART_BACKOFF"
        ? "AUTO-RESTART BACKOFF"
    : paperRuntimeRecoveryState === "AUTO_RESTART_IN_PROGRESS"
      ? "AUTO-RESTART IN PROGRESS"
      : paperRuntimeRecoveryState === "AUTO_RESTART_FAILED"
        ? "AUTO-RESTART FAILED"
        : paperAutoStartEligible
          ? "AUTO-RECOVERABLE"
          : "STOPPED";
  const paperStartupReasonText =
    paperRuntimeRecoveryMessage ||
    (paperReadiness.runtime_running === true
      ? "Paper runtime is active."
      : latestPaperStartBlockReason ||
        String(paperEntryEligibility.state_note ?? paperEntryEligibility.verdict ?? "Paper runtime is stopped."));
  const paperStartupActionLabel =
    paperReadiness.runtime_running === true
      ? "Runtime Active"
      : paperRuntimeRecoveryState === "AUTO_RESTART_SUPPRESSED"
        ? "Manual Runtime Start Required"
      : paperRuntimeRecoveryState === "AUTO_RESTART_BACKOFF"
        ? "Auto-Restart Backoff Active"
      : paperRuntimeRecoveryState === "AUTO_RESTART_IN_PROGRESS"
        ? "Auto-Restart In Progress"
      : !canRunLiveActions
      ? "Start Dashboard/API"
      : tempPaperMismatchActive
        ? "Restart Runtime + Temp Paper"
        : !authReadyForPaperStartup
          ? "Auth Gate Check"
          : paperRuntimeRecoveryNextAction ||
            (paperStartupPrimaryReason === "STARTUP_REVIEW_GATING"
            ? "Complete Pre-Session Review"
            : "Start Runtime");
  const paperStartupActionDescription =
    paperRuntimeRecoveryState === "AUTO_RESTART_IN_PROGRESS"
      ? "The dashboard is auto-restarting the paper runtime because stopped-runtime recovery was judged safe."
      : paperRuntimeRecoveryState === "AUTO_RESTART_BACKOFF"
        ? `The last automatic restart attempt failed or is cooling down. The supervisor will retry automatically after ${formatTimestamp(paperRuntimeRestartBackoffUntil)} if the stop remains safe and budget remains available.`
      : paperRuntimeRecoveryState === "AUTO_RESTART_SUPPRESSED"
        ? `Automatic restart is suppressed because the rolling restart budget was exhausted. Manual intervention is required until suppression expires at ${formatTimestamp(paperRuntimeRestartSuppressedUntil)} or the operator restarts the runtime intentionally.`
      : paperReadiness.runtime_running === true
        ? (paperRuntimeRecoveryState === "AUTO_RESTART_SUCCEEDED"
            ? "The dashboard detected a safe stopped-runtime condition and brought paper soak back automatically."
            : "Auto-start protections are satisfied and the paper runtime is already active.")
      : paperAutoStartEligible
        ? "The dashboard has classified this stopped-runtime condition as safe for automatic recovery."
        : !canRunLiveActions
          ? "The desktop app is not currently attached to the live backend/API, so paper soak cannot be started yet."
          : tempPaperMismatchActive
            ? "Enabled temp-paper lanes must match the runtime launch overlays before the paper soak can be trusted."
            : !authReadyForPaperStartup
              ? "Broker/auth readiness is not yet green, so paper soak will not auto-start."
              : String(paperRuntimeRecoveryNextAction || paperEntryEligibility.clear_action || "Start Runtime");
  const startup = desktopState?.startup ?? null;
  const playbackArtifacts = asRecord(playbackLatestRun.artifact_paths);
  const auditFiltersActive = Boolean(auditStrategyIdentityFilter || auditFamilyFilter || auditInstrumentFilter || auditSessionFilter || auditVerdictFilter);
  const selectedProductionAccount = productionAccounts.find((row) => row.selected === true) ?? productionAccounts[0] ?? null;
  const selectedProductionPosition = productionPositions.find((row) => String(row.position_key) === selectedProductionPositionKey) ?? productionPositions[0] ?? null;
  const selectedSignalIntentFillAuditRow =
    filteredSignalIntentFillAuditRows.find((row) => standaloneStrategyId(row) === selectedAuditStrategyKey)
    ?? filteredSignalIntentFillAuditRows[0]
    ?? (!auditFiltersActive
      ? signalIntentFillAuditRows.find((row) => standaloneStrategyId(row) === selectedAuditStrategyKey) ?? signalIntentFillAuditRows[0]
      : null)
    ?? null;
  const selectedSameUnderlyingConflict =
    sortedSameUnderlyingConflictRows.find((row) => String(row.instrument ?? "") === selectedSameUnderlyingConflictInstrument)
    ?? sortedSameUnderlyingConflictRows[0]
    ?? null;
  const selectedSameUnderlyingConflictEvents = useMemo(
    () =>
      selectedSameUnderlyingConflict
        ? sameUnderlyingEventRows.filter(
            (row) => String(row.instrument ?? "") === String(selectedSameUnderlyingConflict.instrument ?? ""),
          )
        : [],
    [sameUnderlyingEventRows, selectedSameUnderlyingConflict],
  );
  const selectedManualAccount =
    productionAccounts.find((row) => String(row.account_hash) === manualOrderForm.accountHash) ?? selectedProductionAccount;
  const selectedProductionSymbol = String(selectedProductionPosition?.symbol ?? "").trim().toUpperCase();
  const selectedPositionOpenOrders = productionOpenOrders.filter((row) => String(row.symbol ?? "").trim().toUpperCase() === selectedProductionSymbol);
  const selectedPositionRecentFills = productionRecentFills.filter((row) => String(row.symbol ?? "").trim().toUpperCase() === selectedProductionSymbol);
  const selectedPositionEvents = productionEvents.filter((row) => String(row.broker_order_id ?? "").trim() && (
    selectedPositionOpenOrders.some((order) => String(order.broker_order_id) === String(row.broker_order_id)) ||
    selectedPositionRecentFills.some((order) => String(order.broker_order_id) === String(row.broker_order_id))
  ));
  const positionsMonitorRows = useMemo<PositionsMonitorRow[]>(() => {
    const grouped = new Map<string, PositionsMonitorRow>();
    const ensureRow = (symbolValue: unknown): PositionsMonitorRow | null => {
      const symbol = String(symbolValue ?? "").trim().toUpperCase();
      if (!symbol) {
        return null;
      }
      const existing = grouped.get(symbol);
      if (existing) {
        return existing;
      }
      const created: PositionsMonitorRow = {
        id: symbol,
        symbol,
        description: "",
        displaySymbol: symbol,
        displayDescription: "",
        sourceBadges: [],
        exposureMarker: null,
        childRows: undefined,
        isSpreadParent: false,
        isSpreadLeg: false,
        spreadKey: null,
        spreadLabel: null,
        brokerRows: [],
        paperRows: [],
        approvedPaperRows: [],
        experimentalRows: [],
        sameUnderlyingRows: [],
        brokerOrders: [],
        brokerFills: [],
        brokerEvents: [],
        closedTrades: [],
        brokerQty: null,
        paperQty: null,
        brokerAvgPrice: null,
        paperAvgEntry: null,
        brokerMark: null,
        paperMark: null,
        brokerDayPnl: null,
        paperDayPnl: null,
        brokerOpenPnl: null,
        paperOpenPnl: null,
        brokerRealized: null,
        paperRealized: null,
        brokerYtdPnl: null,
        brokerMarketValue: null,
        brokerMarginEffect: null,
        brokerDelta: null,
        brokerTheta: null,
        strategyCount: 0,
        tradeCount: 0,
        maxDrawdown: null,
        primaryStrategy: "",
        paperClass: null,
        currentStatus: "READY",
        conflict: "Clear",
        conflictState: "CLEAR",
        reviewState: "CLEAR",
        session: "—",
        runtimeLoaded: "—",
        entryHold: "No Hold",
        lastActivity: null,
        latestIntentTime: null,
        latestFillTime: null,
        latestTradeTime: null,
        latestBrokerUpdateTime: null,
        gamma: null,
        vega: null,
        iv: null,
        ivPercentile: null,
        daysToExp: null,
        roc: null,
        yieldValue: null,
        expectedMove: null,
        quoteTrend: null,
        initialMargin: null,
        probItm: null,
        probOtm: null,
        extrinsic: null,
        intrinsic: null,
      };
      grouped.set(symbol, created);
      return created;
    };

    for (const brokerRow of productionPositions) {
      const row = ensureRow(brokerRow.symbol);
      if (row) {
        row.brokerRows.push(brokerRow);
      }
    }

    for (const paperRow of strategyPerformanceRows) {
      const row = ensureRow(paperRow.instrument ?? paperRow.symbol);
      if (!row) {
        continue;
      }
      row.paperRows.push(paperRow);
      if (isTemporaryPaperStrategyRow(paperRow)) {
        row.experimentalRows.push(paperRow);
      } else {
        row.approvedPaperRows.push(paperRow);
      }
    }

    for (const tradeRow of closedStrategyTradeRows) {
      const row = ensureRow(tradeRow.instrument ?? tradeRow.symbol);
      if (row) {
        row.closedTrades.push(tradeRow);
      }
    }

    for (const conflictRow of sameUnderlyingConflictRows) {
      const row = ensureRow(conflictRow.instrument ?? conflictRow.symbol);
      if (row) {
        row.sameUnderlyingRows.push(conflictRow);
      }
    }

    for (const orderRow of productionOpenOrders) {
      const row = ensureRow(orderRow.symbol);
      if (row) {
        row.brokerOrders.push(orderRow);
      }
    }

    for (const fillRow of productionRecentFills) {
      const row = ensureRow(fillRow.symbol);
      if (row) {
        row.brokerFills.push(fillRow);
      }
    }

    for (const eventRow of productionEvents) {
      const row = ensureRow(eventRow.symbol ?? eventRow.instrument);
      if (row) {
        row.brokerEvents.push(eventRow);
      }
    }

    return [...grouped.values()]
      .map((row) => {
        const primaryBrokerRow =
          [...row.brokerRows].sort((left, right) => parseTimestampMs(right.fetched_at) - parseTimestampMs(left.fetched_at))[0] ?? null;
        const primaryPaperRow =
          [...row.paperRows].sort((left, right) => {
            const openDelta =
              Number(String(right.position_side ?? "").toUpperCase() !== "FLAT") -
              Number(String(left.position_side ?? "").toUpperCase() !== "FLAT");
            if (openDelta !== 0) {
              return openDelta;
            }
            return parseTimestampMs(right.latest_activity_timestamp) - parseTimestampMs(left.latest_activity_timestamp);
          })[0] ?? null;
        const latestConflictRow =
          [...row.sameUnderlyingRows].sort((left, right) => sameUnderlyingConflictRank(left.severity) - sameUnderlyingConflictRank(right.severity))[0] ?? null;
        const brokerQty = sumNullable(row.brokerRows.map((brokerRow) => numericOrNull(brokerRow.quantity)));
        const brokerAvgPrice = weightedAverage(
          row.brokerRows.map((brokerRow) => ({
            value: numericOrNull(brokerRow.average_cost),
            weight: numericOrNull(brokerRow.quantity),
          })),
        );
        const brokerMark = averageNullable(
          row.brokerRows.map((brokerRow) => {
            const direct = numericOrNull(brokerRow.mark_price);
            if (direct !== null) {
              return direct;
            }
            const marketValue = numericOrNull(brokerRow.market_value);
            const quantity = numericOrNull(brokerRow.quantity);
            return marketValue !== null && quantity ? marketValue / quantity : null;
          }),
        );
        const brokerDayPnl = sumNullable(row.brokerRows.map((brokerRow) => numericOrNull(brokerRow.current_day_pnl)));
        const brokerOpenPnl = sumNullable(row.brokerRows.map((brokerRow) => numericOrNull(brokerRow.open_pnl)));
        const brokerYtdPnl = sumNullable(row.brokerRows.map((brokerRow) => numericOrNull(brokerRow.ytd_pnl)));
        const brokerMarketValue = sumNullable(row.brokerRows.map((brokerRow) => numericOrNull(brokerRow.market_value)));
        const brokerMarginEffect = sumNullable(row.brokerRows.map((brokerRow) => numericOrNull(brokerRow.margin_impact)));
        const brokerDelta = sumNullable(row.brokerRows.map((brokerRow) => numericOrNull(brokerRow.delta ?? asRecord(brokerRow.raw_payload).delta)));
        const brokerTheta = sumNullable(row.brokerRows.map((brokerRow) => numericOrNull(brokerRow.theta ?? asRecord(brokerRow.raw_payload).theta)));
        const paperQty = row.paperRows.reduce((sum, paperRow) => sum + quantityFromPaperRow(paperRow), 0);
        const paperAvgEntry = weightedAverage(
          row.paperRows.map((paperRow) => ({
            value: numericOrNull(paperRow.entry_price),
            weight: Math.abs(quantityFromPaperRow(paperRow)),
          })),
        );
        const paperMark = averageNullable(row.paperRows.map((paperRow) => numericOrNull(paperRow.last_mark)));
        const paperDayPnl = sumNullable(row.paperRows.map((paperRow) => numericOrNull(paperRow.day_pnl)));
        const paperOpenPnl = sumNullable(row.paperRows.map((paperRow) => numericOrNull(paperRow.unrealized_pnl)));
        const paperRealized = sumNullable(row.paperRows.map((paperRow) => numericOrNull(paperRow.realized_pnl)));
        const tradeCount = row.paperRows.reduce((sum, paperRow) => sum + (numericOrNull(paperRow.trade_count) ?? 0), 0);
        const maxDrawdown = sumNullable(row.paperRows.map((paperRow) => numericOrNull(paperRow.max_drawdown)));
        const lastActivity = latestTimestamp([
          ...row.brokerRows.map((brokerRow) => brokerRow.fetched_at),
          ...row.paperRows.map((paperRow) => paperRow.latest_activity_timestamp),
          ...row.closedTrades.map((tradeRow) => tradeRow.exit_timestamp ?? tradeRow.entry_timestamp),
          ...row.brokerEvents.map((eventRow) => eventRow.occurred_at ?? eventRow.updated_at),
        ]);
        const latestFillTime = latestTimestamp([
          ...row.paperRows.map((paperRow) => paperRow.latest_fill_timestamp),
          ...row.brokerFills.map((fillRow) => fillRow.updated_at),
        ]);
        const paperClass = paperClassLabel(row);
        const hasEntryHold = row.sameUnderlyingRows.some((conflictRow) => conflictRow.hold_new_entries === true);
        const conflictLabel = latestConflictRow
          ? hasEntryHold
            ? "Held"
            : String(latestConflictRow.severity ?? "Review")
          : row.brokerRows.length && row.paperRows.length
            ? "Split"
            : "—";
        const conflictState = latestConflictRow
          ? String(latestConflictRow.severity ?? "REVIEW")
          : row.brokerRows.length && row.paperRows.length
            ? "SPLIT"
            : "CLEAR";
        const currentStatus =
          row.paperRows.some((paperRow) => String(paperRow.position_side ?? "").toUpperCase() !== "FLAT")
            ? "IN POSITION"
            : String(primaryPaperRow?.status ?? primaryPaperRow?.risk_state ?? primaryBrokerRow?.side ?? "READY");
        return {
          ...row,
          description: String(primaryBrokerRow?.description ?? primaryPaperRow?.strategy_name ?? primaryPaperRow?.signal_family_label ?? "—"),
          displaySymbol: row.symbol,
          displayDescription: String(primaryBrokerRow?.description ?? primaryPaperRow?.strategy_name ?? primaryPaperRow?.signal_family_label ?? "—"),
          brokerQty,
          paperQty,
          brokerAvgPrice,
          paperAvgEntry,
          brokerMark,
          paperMark,
          brokerDayPnl,
          paperDayPnl,
          brokerOpenPnl,
          paperOpenPnl,
          brokerRealized: null,
          paperRealized,
          brokerYtdPnl,
          brokerMarketValue,
          brokerMarginEffect,
          brokerDelta,
          brokerTheta,
          strategyCount: row.paperRows.length,
          tradeCount,
          maxDrawdown,
          primaryStrategy: String(primaryPaperRow?.strategy_name ?? primaryPaperRow?.signal_family_label ?? primaryBrokerRow?.symbol ?? "—"),
          paperClass,
          currentStatus,
          conflict: conflictLabel,
          conflictState,
          reviewState: String(latestConflictRow?.review_state_status ?? "CLEAR"),
          session: String(primaryPaperRow?.current_session ?? primaryPaperRow?.session_restriction ?? "—"),
          runtimeLoaded:
            row.paperRows.length > 0
              ? row.paperRows.every((paperRow) => paperRow.runtime_state_loaded === true)
                ? "Loaded"
                : row.paperRows.some((paperRow) => paperRow.runtime_instance_present === true)
                  ? "Partial"
                  : "Snapshot"
              : "Broker",
          entryHold: hasEntryHold ? "Held" : "No Hold",
          lastActivity,
          latestIntentTime: latestTimestamp(row.paperRows.map((paperRow) => paperRow.last_fire_timestamp ?? paperRow.latest_activity_timestamp)),
          latestFillTime,
          latestTradeTime: latestTimestamp(row.closedTrades.map((tradeRow) => tradeRow.exit_timestamp ?? tradeRow.entry_timestamp)),
          latestBrokerUpdateTime: latestTimestamp(row.brokerRows.map((brokerRow) => brokerRow.fetched_at)),
          quoteTrend: primaryBrokerRow ? formatValue(primaryBrokerRow.net_change ?? asRecord(primaryBrokerRow.quote).net_change ?? asRecord(asRecord(primaryBrokerRow.raw_payload).instrument).netChange) : null,
          initialMargin: brokerMarginEffect,
          sourceBadges: sourceBadgesForRow(row),
          exposureMarker: exposureMarkerForRow({
            ...row,
            brokerQty,
            paperQty,
          } as PositionsMonitorRow),
        };
      })
      .sort((left, right) => left.symbol.localeCompare(right.symbol));
  }, [
    closedStrategyTradeRows,
    productionEvents,
    productionOpenOrders,
    productionPositions,
    productionRecentFills,
    sameUnderlyingConflictRows,
    strategyPerformanceRows,
  ]);
  const visiblePositionsRows = useMemo(
    () =>
      positionsMonitorRows.filter((row) => {
        if (positionsViewMode === "broker") {
          return row.brokerRows.length > 0;
        }
        if (positionsViewMode === "paper") {
          return row.paperRows.length > 0;
        }
        return row.brokerRows.length > 0 || row.paperRows.length > 0;
      }),
    [positionsMonitorRows, positionsViewMode],
  );
  const displayPositionsRows = useMemo(
    () => buildSpreadDisplayRows(visiblePositionsRows, expandedSpreadRowIds),
    [expandedSpreadRowIds, visiblePositionsRows],
  );
  const currentPositionsSort = positionsSortByMode[positionsViewMode];
  const sortedPositionsRows = useMemo(() => {
    const valueFor = (row: PositionsMonitorRow, columnId: string): string | number | null => {
      switch (columnId) {
        case "symbol":
          return row.symbol;
        case "description":
          return row.description;
        case "qty":
          return row.brokerQty;
        case "avgPrice":
          return row.brokerAvgPrice;
        case "mark":
          return positionsViewMode === "broker" ? row.brokerMark : row.paperMark;
        case "dayPnl":
          return positionsViewMode === "broker" ? row.brokerDayPnl : row.paperDayPnl;
        case "openPnl":
          return positionsViewMode === "broker" ? row.brokerOpenPnl : row.paperOpenPnl;
        case "realizedPnl":
          return positionsViewMode === "broker" ? row.brokerRealized : row.paperRealized;
        case "ytdPnl":
          return row.brokerYtdPnl;
        case "marketValue":
          return row.brokerMarketValue;
        case "marginEffect":
          return row.brokerMarginEffect;
        case "delta":
          return row.brokerDelta;
        case "theta":
          return row.brokerTheta;
        case "strategyCount":
          return row.strategyCount;
        case "netQty":
          return row.paperQty;
        case "avgEntry":
          return row.paperAvgEntry;
        case "tradeCount":
          return row.tradeCount;
        case "maxDrawdown":
          return row.maxDrawdown;
        case "primaryStrategy":
          return row.primaryStrategy;
        case "class":
          return row.paperClass;
        case "currentStatus":
          return row.currentStatus;
        case "brokerQty":
          return row.brokerQty;
        case "paperQty":
          return row.paperQty;
        case "brokerOpenPnl":
          return row.brokerOpenPnl;
        case "paperOpenPnl":
          return row.paperOpenPnl;
        case "brokerRealized":
          return row.brokerRealized;
        case "paperRealized":
          return row.paperRealized;
        case "combinedRealized":
          return row.brokerRealized !== null && row.paperRealized !== null ? row.brokerRealized + row.paperRealized : row.paperRealized;
        case "netValue":
          return sumNullable([row.brokerMarketValue, row.paperOpenPnl]);
        case "conflict":
          return row.conflict;
        case "lastActivity":
          return parseSortTimestamp(row.lastActivity);
        case "runtimeLoaded":
          return row.runtimeLoaded;
        case "session":
          return row.session;
        case "entryHold":
          return row.entryHold;
        case "reviewState":
          return row.reviewState;
        case "latestIntentTime":
          return parseSortTimestamp(row.latestIntentTime);
        case "latestFillTime":
          return parseSortTimestamp(row.latestFillTime);
        case "latestTradeTime":
          return parseSortTimestamp(row.latestTradeTime);
        case "conflictState":
          return row.conflictState;
        default:
          return null;
      }
    };
    const topLevelRows = displayPositionsRows.filter((row) => !row.isSpreadLeg);
    const sortedTopLevelRows = [...topLevelRows].sort((left, right) => {
      if (positionsViewMode === "paper" && currentPositionsSort.columnId === "lastActivity") {
        const interestCompared = paperMonitorInterestScore(right) - paperMonitorInterestScore(left);
        if (interestCompared !== 0) {
          return interestCompared;
        }
      }
      const compared = compareMonitorValues(
        valueFor(left, currentPositionsSort.columnId),
        valueFor(right, currentPositionsSort.columnId),
        currentPositionsSort.direction,
      );
      return compared !== 0 ? compared : left.symbol.localeCompare(right.symbol);
    });
    const output: PositionsMonitorRow[] = [];
    for (const row of sortedTopLevelRows) {
      output.push(row);
      if (row.childRows && expandedSpreadRowIds.includes(row.id)) {
        output.push(...row.childRows);
      }
    }
    return output;
  }, [currentPositionsSort.columnId, currentPositionsSort.direction, displayPositionsRows, expandedSpreadRowIds, positionsViewMode]);
  const normalizedCurrentColumns = normalizeMonitorColumns(positionsLayoutState.currentColumnsByMode[positionsViewMode], positionsViewMode);
  const savedPositionLayouts = positionsLayoutState.savedLayoutsByMode[positionsViewMode];
  const filteredClosedTradeRows = useMemo(() => {
    if (positionsViewMode === "broker") {
      return [] as JsonRecord[];
    }
    const rows = [...closedStrategyTradeRows]
      .filter((row) => {
        if (positionsClosedTradesClassFilter === "all") {
          return true;
        }
        return tradeTopLevelClass(row).toLowerCase() === positionsClosedTradesClassFilter;
      })
      .sort((left, right) => parseTimestampMs(right.exit_timestamp ?? right.entry_timestamp) - parseTimestampMs(left.exit_timestamp ?? left.entry_timestamp));
    return rows;
  }, [closedStrategyTradeRows, positionsClosedTradesClassFilter, positionsViewMode]);
  const visibleClosedTradeRows = useMemo(
    () => filteredClosedTradeRows.slice(0, positionsClosedTradesPageSize),
    [filteredClosedTradeRows, positionsClosedTradesPageSize],
  );
  const selectedPositionsRow =
    sortedPositionsRows.find((row) => row.id === selectedPositionsRowId) ??
    sortedPositionsRows[0] ??
    null;
  const positionsSummaryMetrics = useMemo<PositionsMetricItem[]>(() => {
    if (positionsViewMode === "broker") {
      return [
        { label: "Net Liq", value: formatCompactMetric(productionBalances.liquidation_value), tone: "muted" },
        { label: "Available Cash", value: formatCompactMetric(productionBalances.available_funds ?? productionBalances.cash_balance), tone: "muted" },
        { label: "Buying Power", value: formatCompactMetric(productionBalances.buying_power), tone: "muted" },
        { label: "Margin / BP In Use", value: formatCompactMetric(productionBalances.maintenance_requirement ?? productionBalances.margin_balance), tone: "muted" },
        { label: "P/L Day", value: renderPnlValue(productionTotals.total_current_day_pnl), tone: pnlTone(productionTotals.total_current_day_pnl) },
        { label: "P/L Open", value: renderPnlValue(productionTotals.total_open_pnl), tone: pnlTone(productionTotals.total_open_pnl) },
        { label: "Symbols", value: formatCompactMetric(sortedPositionsRows.filter((row) => !row.isSpreadLeg).length, 0), tone: "muted" },
        { label: "P/L YTD", value: renderPnlValue(sumNullable(productionPositions.map((row) => numericOrNull(row.ytd_pnl)))), tone: pnlTone(sumNullable(productionPositions.map((row) => numericOrNull(row.ytd_pnl)))) },
      ];
    }
    if (positionsViewMode === "paper") {
      return [
        { label: "Paper Realized", value: renderPnlValue(combinedStrategyPortfolioSnapshot.total_realized_pnl), tone: pnlTone(combinedStrategyPortfolioSnapshot.total_realized_pnl) },
        { label: "Paper Open P/L", value: renderPnlValue(combinedStrategyPortfolioSnapshot.total_unrealized_pnl), tone: pnlTone(combinedStrategyPortfolioSnapshot.total_unrealized_pnl) },
        { label: "Paper Day P/L", value: renderPnlValue(combinedStrategyPortfolioSnapshot.total_day_pnl), tone: pnlTone(combinedStrategyPortfolioSnapshot.total_day_pnl) },
        {
          label: "Active Paper Positions",
          value: formatCompactMetric(
            sortedPositionsRows.filter((row) => row.paperRows.some((paperRow) => String(paperRow.position_side ?? "").toUpperCase() !== "FLAT")).length,
            0,
          ),
          tone: "muted",
        },
        { label: "Trade Count", value: formatCompactMetric(sumNullable(sortedPositionsRows.map((row) => row.tradeCount)), 0), tone: "muted" },
        { label: "Max Drawdown", value: renderPnlValue(combinedStrategyPortfolioSnapshot.total_max_drawdown), tone: pnlTone(combinedStrategyPortfolioSnapshot.total_max_drawdown) },
      ];
    }
    return [
      { label: "Broker Net Liq", value: formatCompactMetric(productionBalances.liquidation_value), tone: "muted" },
      { label: "Broker Open P/L", value: renderPnlValue(productionTotals.total_open_pnl), tone: pnlTone(productionTotals.total_open_pnl) },
      { label: "Broker Day P/L", value: renderPnlValue(productionTotals.total_current_day_pnl), tone: pnlTone(productionTotals.total_current_day_pnl) },
      { label: "Paper Realized", value: renderPnlValue(combinedStrategyPortfolioSnapshot.total_realized_pnl), tone: pnlTone(combinedStrategyPortfolioSnapshot.total_realized_pnl) },
      { label: "Paper Open P/L", value: renderPnlValue(combinedStrategyPortfolioSnapshot.total_unrealized_pnl), tone: pnlTone(combinedStrategyPortfolioSnapshot.total_unrealized_pnl) },
      { label: "Paper Day P/L", value: renderPnlValue(combinedStrategyPortfolioSnapshot.total_day_pnl), tone: pnlTone(combinedStrategyPortfolioSnapshot.total_day_pnl) },
      { label: "Broker Symbols", value: formatCompactMetric(productionPositions.length, 0), tone: "muted" },
      { label: "Paper Symbols", value: formatCompactMetric(positionsMonitorRows.filter((row) => row.paperRows.length > 0).length, 0), tone: "muted" },
    ];
  }, [
    combinedStrategyPortfolioSnapshot.total_day_pnl,
    combinedStrategyPortfolioSnapshot.total_max_drawdown,
    combinedStrategyPortfolioSnapshot.total_realized_pnl,
    combinedStrategyPortfolioSnapshot.total_unrealized_pnl,
    positionsMonitorRows,
    positionsViewMode,
    productionBalances.available_funds,
    productionBalances.buying_power,
    productionBalances.cash_balance,
    productionBalances.liquidation_value,
    productionBalances.maintenance_requirement,
    productionBalances.margin_balance,
    productionPositions,
    productionTotals.total_current_day_pnl,
    productionTotals.total_open_pnl,
    sortedPositionsRows,
  ]);
  const brokerPositionsFreshness = asRecord(productionFreshness.positions);
  const brokerQuotesFreshness = asRecord(productionFreshness.quotes);
  const brokerOrdersFreshness = asRecord(productionFreshness.orders);
  const brokerFillsFreshness = asRecord(productionFreshness.fills);
  const brokerBalancesFreshness = asRecord(productionFreshness.balances);
  const brokerPositionsFallbackTimestamp =
    productionDiagnostics.last_positions_refresh_at ?? productionBalances.fetched_at ?? null;
  const brokerQuotesFallbackTimestamp =
    productionDiagnostics.last_quotes_refresh_at ?? productionQuotes.updated_at ?? null;
  const brokerOrdersFallbackTimestamp = productionDiagnostics.last_orders_refresh_at ?? null;
  const brokerFillsFallbackTimestamp =
    productionDiagnostics.last_fills_refresh_at ?? productionDiagnostics.last_orders_refresh_at ?? null;
  const brokerSnapshotTimestamp =
    freshnessUpdatedAt(brokerPositionsFreshness) ??
    freshnessUpdatedAt(brokerBalancesFreshness) ??
    brokerPositionsFallbackTimestamp;
  const brokerQuotesTimestamp =
    freshnessUpdatedAt(brokerQuotesFreshness) ??
    brokerQuotesFallbackTimestamp;
  const brokerOrdersTimestamp = freshnessUpdatedAt(brokerOrdersFreshness) ?? brokerOrdersFallbackTimestamp;
  const brokerFillsTimestamp = freshnessUpdatedAt(brokerFillsFreshness) ?? brokerFillsFallbackTimestamp;
  const paperRuntimeTimestamp = global.last_update_timestamp ?? desktopState?.refreshedAt ?? null;
  const positionsPagePollingActive = page === "positions" && isVisible;
  const brokerPollingActive =
    positionsPagePollingActive &&
    (positionsViewMode === "broker" || positionsViewMode === "combined") &&
    desktopState?.source.mode === "live_api" &&
    desktopState?.source.canRunLiveActions === true &&
    productionLinkEnabled();
  const paperPollingActive =
    positionsPagePollingActive &&
    desktopState?.source.mode === "live_api" &&
    paperReadiness.runtime_running === true;
  const brokerPositionsFreshnessState = productionLinkEnabled()
    ? freshnessState(brokerPositionsFreshness, desktopState?.source.mode === "snapshot_fallback" ? "SNAPSHOT" : "STALE")
    : "SNAPSHOT";
  const brokerQuotesFreshnessState = productionLinkEnabled()
    ? freshnessState(brokerQuotesFreshness, desktopState?.source.mode === "snapshot_fallback" ? "SNAPSHOT" : "STALE")
    : "SNAPSHOT";
  const brokerDataStale = brokerPositionsFreshnessState === "STALE" || brokerPositionsFreshnessState === "SNAPSHOT";
  const paperFreshnessState = paperReadiness.runtime_running !== true
    ? "SNAPSHOT"
    : desktopState?.source.mode === "snapshot_fallback"
      ? "SNAPSHOT"
      : paperPollingActive
        ? "LIVE"
        : "DELAYED";
  const ordersFillsFreshnessState = productionLinkEnabled()
    ? (() => {
        const orderState = freshnessState(brokerOrdersFreshness, desktopState?.source.mode === "snapshot_fallback" ? "SNAPSHOT" : "STALE");
        const fillState = freshnessState(brokerFillsFreshness, orderState);
        if (orderState === "STALE" || fillState === "STALE") {
          return "STALE";
        }
        if (orderState === "SNAPSHOT" || fillState === "SNAPSHOT") {
          return "SNAPSHOT";
        }
        if (orderState === "DELAYED" || fillState === "DELAYED") {
          return "DELAYED";
        }
        return "LIVE";
      })()
    : "SNAPSHOT";
  const brokerFreshnessTone: Tone = freshnessToneFromState(brokerPositionsFreshnessState);
  const brokerFeedLabel = !productionLinkEnabled()
    ? "SNAPSHOT CACHE"
    : desktopState?.source.mode === "snapshot_fallback"
      ? "SNAPSHOT FALLBACK"
      : "LIVE SCHWAB FEED";
  const brokerFreshnessMessage = brokerSnapshotTimestamp
    ? `Last broker positions refresh: ${formatTimestamp(brokerSnapshotTimestamp)} (${formatRelativeAge(brokerSnapshotTimestamp)}).`
    : "No broker positions refresh timestamp is available.";
  const brokerFreshnessDetail = productionLinkEnabled()
    ? desktopState?.source.mode === "snapshot_fallback"
      ? "The desktop app is running from persisted snapshots because the live dashboard API is unavailable."
      : brokerPositionsFreshnessState === "STALE"
        ? `Broker source is ${formatValue(productionLink.source_of_record ?? productionLink.status)} and is not currently fresh.`
        : brokerPollingActive
          ? `Broker positions are being refreshed from the live Schwab production-link feed every ${POSITIONS_PAGE_BROKER_REFRESH_SECONDS}s, with dashboard recompute every ${POSITIONS_PAGE_POLL_SECONDS}s.`
          : "Broker positions are available from the live Schwab production-link feed, but this page is not currently in active poll mode."
    : `Production link is disabled, so broker rows are being read from persisted snapshot cache rather than a live Schwab pull.`;
  const brokerFreshnessItems = [
    {
      label: "Positions",
      state: brokerPositionsFreshnessState,
      text: brokerSnapshotTimestamp ? `${formatTimestamp(brokerSnapshotTimestamp)} • ${formatRelativeAge(brokerSnapshotTimestamp)}` : "Unavailable",
    },
    {
      label: "Quotes",
      state: brokerQuotesFreshnessState,
      text: brokerQuotesTimestamp ? `${formatTimestamp(brokerQuotesTimestamp)} • ${formatRelativeAge(brokerQuotesTimestamp)}` : productionQuoteRows.length ? "Unavailable" : "No broker symbols",
    },
    {
      label: "Paper",
      state: paperFreshnessState,
      text: paperRuntimeTimestamp ? `${formatTimestamp(paperRuntimeTimestamp)} • ${formatRelativeAge(paperRuntimeTimestamp)}` : "Unavailable",
    },
    {
      label: "Orders/Fills",
      state: ordersFillsFreshnessState,
      text: brokerOrdersTimestamp ? `${formatTimestamp(brokerOrdersTimestamp)} • ${formatRelativeAge(brokerOrdersTimestamp)}` : "Unavailable",
    },
  ];
  const positionsTotalsLine = useMemo<PositionsMetricItem[]>(() => {
    if (positionsViewMode === "broker") {
      return [
        { label: "Rows", value: formatCompactMetric(sortedPositionsRows.filter((row) => !row.isSpreadLeg).length, 0), tone: "muted" },
        { label: "Day", value: renderPnlValue(productionTotals.total_current_day_pnl), tone: pnlTone(productionTotals.total_current_day_pnl) },
        { label: "Open", value: renderPnlValue(productionTotals.total_open_pnl), tone: pnlTone(productionTotals.total_open_pnl) },
        { label: "YTD", value: renderPnlValue(sumNullable(productionPositions.map((row) => numericOrNull(row.ytd_pnl)))), tone: pnlTone(sumNullable(productionPositions.map((row) => numericOrNull(row.ytd_pnl)))) },
        { label: "Feed", value: `${brokerPositionsFreshnessState} • ${brokerFeedLabel}`, tone: brokerFreshnessTone },
      ];
    }
    if (positionsViewMode === "paper") {
      return [
        { label: "Rows", value: formatCompactMetric(sortedPositionsRows.filter((row) => !row.isSpreadLeg).length, 0), tone: "muted" },
        { label: "Day", value: renderPnlValue(combinedStrategyPortfolioSnapshot.total_day_pnl), tone: pnlTone(combinedStrategyPortfolioSnapshot.total_day_pnl) },
        { label: "Open", value: renderPnlValue(combinedStrategyPortfolioSnapshot.total_unrealized_pnl), tone: pnlTone(combinedStrategyPortfolioSnapshot.total_unrealized_pnl) },
        { label: "Realized", value: renderPnlValue(combinedStrategyPortfolioSnapshot.total_realized_pnl), tone: pnlTone(combinedStrategyPortfolioSnapshot.total_realized_pnl) },
        { label: "Max DD", value: renderPnlValue(combinedStrategyPortfolioSnapshot.total_max_drawdown), tone: pnlTone(combinedStrategyPortfolioSnapshot.total_max_drawdown) },
        {
          label: "Runtime",
          value: paperFreshnessState,
          tone: paperFreshnessState === "LIVE" ? "good" : paperFreshnessState === "DELAYED" ? "warn" : "danger",
        },
      ];
    }
    return [
      { label: "Broker Open", value: renderPnlValue(productionTotals.total_open_pnl), tone: pnlTone(productionTotals.total_open_pnl) },
      { label: "Paper Open", value: renderPnlValue(combinedStrategyPortfolioSnapshot.total_unrealized_pnl), tone: pnlTone(combinedStrategyPortfolioSnapshot.total_unrealized_pnl) },
      { label: "Broker Day", value: renderPnlValue(productionTotals.total_current_day_pnl), tone: pnlTone(productionTotals.total_current_day_pnl) },
      { label: "Paper Day", value: renderPnlValue(combinedStrategyPortfolioSnapshot.total_day_pnl), tone: pnlTone(combinedStrategyPortfolioSnapshot.total_day_pnl) },
      { label: "Broker", value: brokerPositionsFreshnessState, tone: brokerFreshnessTone },
      {
        label: "Paper",
        value: paperFreshnessState,
        tone: paperFreshnessState === "LIVE" ? "good" : paperFreshnessState === "DELAYED" ? "warn" : "danger",
      },
    ];
  }, [
    brokerDataStale,
    brokerFeedLabel,
    brokerFreshnessTone,
    brokerPositionsFreshnessState,
    combinedStrategyPortfolioSnapshot.total_day_pnl,
    combinedStrategyPortfolioSnapshot.total_max_drawdown,
    combinedStrategyPortfolioSnapshot.total_realized_pnl,
    combinedStrategyPortfolioSnapshot.total_unrealized_pnl,
    paperFreshnessState,
    positionsViewMode,
    productionPositions,
    productionTotals.total_current_day_pnl,
    productionTotals.total_open_pnl,
    sortedPositionsRows,
  ]);
  const positionsColumnTotals = useMemo<Record<string, ReactNode>>(() => {
    const topLevelRows = sortedPositionsRows.filter((row) => !row.isSpreadLeg);
    const totals: Record<string, ReactNode> = {
      symbol: <span className="monitor-total-label">TOTAL</span>,
    };
    const withNote = (value: ReactNode, note?: string): ReactNode =>
      note ? (
        <div className="monitor-stack compact">
          <span>{value}</span>
          <span className="monitor-cell-note">{note}</span>
        </div>
      ) : (
        value
      );
    if (!topLevelRows.length) {
      return totals;
    }
    if (positionsViewMode === "broker") {
      totals.dayPnl = renderPnlValue(sumNullable(topLevelRows.map((row) => row.brokerDayPnl)));
      totals.openPnl = renderPnlValue(sumNullable(topLevelRows.map((row) => row.brokerOpenPnl)));
      totals.ytdPnl = renderPnlValue(sumNullable(topLevelRows.map((row) => row.brokerYtdPnl)));
      totals.marketValue = withNote(formatCompactMetric(sumNullable(topLevelRows.map((row) => row.brokerMarketValue))), "PORTFOLIO MV");
      totals.marginEffect = withNote(formatCompactMetric(sumNullable(topLevelRows.map((row) => row.brokerMarginEffect))), "BP IN USE");
      return totals;
    }
    if (positionsViewMode === "paper") {
      totals.dayPnl = renderPnlValue(sumNullable(topLevelRows.map((row) => row.paperDayPnl)));
      totals.openPnl = renderPnlValue(sumNullable(topLevelRows.map((row) => row.paperOpenPnl)));
      totals.realizedPnl = withNote(renderPnlValue(sumNullable(topLevelRows.map((row) => row.paperRealized))), "PAPER REALIZED");
      totals.tradeCount = formatCompactMetric(sumNullable(topLevelRows.map((row) => row.tradeCount)), 0);
      return totals;
    }
    totals.brokerOpenPnl = renderPnlValue(sumNullable(topLevelRows.map((row) => row.brokerOpenPnl)));
    totals.paperOpenPnl = renderPnlValue(sumNullable(topLevelRows.map((row) => row.paperOpenPnl)));
    totals.paperRealized = withNote(renderPnlValue(sumNullable(topLevelRows.map((row) => row.paperRealized))), "PAPER REALIZED");
    totals.combinedRealized = withNote(
      renderPnlValue(sumNullable(topLevelRows.map((row) => sumNullable([row.brokerRealized, row.paperRealized])))),
      "REALIZED TOTAL",
    );
    totals.marketValue = withNote(formatCompactMetric(sumNullable(topLevelRows.map((row) => row.brokerMarketValue))), "BROKER MV");
    totals.marginEffect = withNote(formatCompactMetric(sumNullable(topLevelRows.map((row) => row.brokerMarginEffect))), "BROKER BP");
    totals.netValue = withNote(
      formatCompactMetric(sumNullable(topLevelRows.map((row) => sumNullable([row.brokerMarketValue, row.paperOpenPnl])))),
      "B MV + P OPEN",
    );
    return totals;
  }, [positionsViewMode, sortedPositionsRows]);
  useEffect(() => {
    const nextId = sortedPositionsRows[0]?.id ?? "";
    if (!nextId) {
      if (selectedPositionsRowId) {
        setSelectedPositionsRowId("");
      }
      return;
    }
    if (!sortedPositionsRows.some((row) => row.id === selectedPositionsRowId)) {
      setSelectedPositionsRowId(nextId);
    }
  }, [selectedPositionsRowId, sortedPositionsRows]);
  useEffect(() => {
    setPositionsClosedTradesPageSize(25);
  }, [positionsClosedTradesClassFilter, positionsViewMode]);

  function updatePositionsCurrentColumns(mode: PositionsViewMode, nextColumns: string[]): void {
    setPositionsLayoutState((current) => ({
      ...current,
      currentColumnsByMode: {
        ...current.currentColumnsByMode,
        [mode]: normalizeMonitorColumns(nextColumns, mode),
      },
    }));
  }

  function togglePositionsColumn(columnId: string): void {
    if (columnId === "symbol") {
      return;
    }
    const currentColumns = normalizedCurrentColumns;
    if (currentColumns.includes(columnId)) {
      updatePositionsCurrentColumns(
        positionsViewMode,
        currentColumns.filter((currentColumnId) => currentColumnId !== columnId),
      );
      return;
    }
    updatePositionsCurrentColumns(positionsViewMode, [...currentColumns, columnId]);
  }

  function movePositionsColumn(columnId: string, direction: -1 | 1): void {
    if (columnId === "symbol") {
      return;
    }
    const nextColumns = [...normalizedCurrentColumns];
    const currentIndex = nextColumns.indexOf(columnId);
    if (currentIndex < 0) {
      return;
    }
    const targetIndex = Math.max(1, Math.min(nextColumns.length - 1, currentIndex + direction));
    if (currentIndex === targetIndex) {
      return;
    }
    nextColumns.splice(currentIndex, 1);
    nextColumns.splice(targetIndex, 0, columnId);
    updatePositionsCurrentColumns(positionsViewMode, nextColumns);
  }

  function resetPositionsColumns(): void {
    updatePositionsCurrentColumns(positionsViewMode, [...POSITIONS_DEFAULT_COLUMNS[positionsViewMode]]);
  }

  function saveCurrentPositionsLayout(): void {
    const suggestedName = `Layout ${Object.keys(savedPositionLayouts).length + 1}`;
    const layoutName = window.prompt("Save this positions layout as:", suggestedName)?.trim();
    if (!layoutName) {
      return;
    }
    setPositionsLayoutState((current) => ({
      ...current,
      savedLayoutsByMode: {
        ...current.savedLayoutsByMode,
        [positionsViewMode]: {
          ...current.savedLayoutsByMode[positionsViewMode],
          [layoutName]: normalizedCurrentColumns,
        },
      },
    }));
  }

  function loadSavedPositionsLayout(layoutName: string): void {
    const savedColumns = savedPositionLayouts[layoutName];
    if (!Array.isArray(savedColumns)) {
      return;
    }
    updatePositionsCurrentColumns(positionsViewMode, savedColumns);
  }

  function updatePositionsSort(columnId: string): void {
    setPositionsSortByMode((current) => {
      const activeSort = current[positionsViewMode];
      const nextDirection =
        activeSort.columnId === columnId
          ? activeSort.direction === "asc"
            ? "desc"
            : "asc"
          : "desc";
      return {
        ...current,
        [positionsViewMode]: {
          columnId,
          direction: nextDirection,
        },
      };
    });
  }

  function openPositionsDrawer(row: PositionsMonitorRow, tab: PositionsDrawerTab): void {
    setSelectedPositionsRowId(row.id);
    setPositionsDrawerTab(tab);
    setPositionsDrawerOpen(true);
    setOpenPositionsMenuRowId(null);
    if (row.brokerRows[0]?.position_key) {
      setSelectedProductionPositionKey(String(row.brokerRows[0].position_key));
    }
  }

  function toggleSpreadRow(rowId: string): void {
    setExpandedSpreadRowIds((current) => (current.includes(rowId) ? current.filter((value) => value !== rowId) : [...current, rowId]));
  }
  const positionsMonitorColumns = useMemo<Record<string, PositionsMonitorColumn>>(() => ({
    symbol: {
      id: "symbol",
      label: "Symbol",
      sticky: true,
      hideable: false,
      render: (row) => (
        <div className="monitor-symbol-cell">
          <div className="monitor-symbol-main">
            {row.childRows?.length ? (
              <button
                className="monitor-expander"
                onClick={(event) => {
                  event.stopPropagation();
                  toggleSpreadRow(row.id);
                }}
                aria-label={expandedSpreadRowIds.includes(row.id) ? "Collapse spread legs" : "Expand spread legs"}
              >
                {expandedSpreadRowIds.includes(row.id) ? "▾" : "▸"}
              </button>
            ) : row.isSpreadLeg ? <span className="monitor-leg-indent">•</span> : null}
            <button className={`monitor-symbol-button ${row.isSpreadLeg ? "leg-row" : ""}`} onClick={() => openPositionsDrawer(row, "summary")}>
              <span className="monitor-symbol">{row.displaySymbol ?? row.symbol}</span>
              <span className="monitor-symbol-subtitle">{row.displayDescription ?? row.description}</span>
            </button>
          </div>
          <div className="monitor-symbol-meta">
            <div className="monitor-chip-row">
              {(row.sourceBadges ?? []).map((badge) => (
                <Badge key={badge} label={badge} tone={badge === "BROKER" ? "good" : badge === "EXPERIMENTAL" ? "warn" : "muted"} />
              ))}
              {row.exposureMarker ? <span className={`monitor-flag ${row.exposureMarker.toLowerCase()}`}>{row.exposureMarker}</span> : null}
            </div>
            {row.isSpreadParent ? <div className="monitor-spread-hint">Vertical spread • {row.childRows?.length ?? 0} legs • net values from legs</div> : null}
          </div>
          <div
            className="monitor-symbol-actions"
            ref={openPositionsMenuRowId === row.id ? positionsMenuContainerRef : undefined}
          >
            <button
              className="monitor-menu-button"
              aria-label={`Open actions for ${row.displaySymbol ?? row.symbol}`}
              aria-expanded={openPositionsMenuRowId === row.id}
              onClick={(event) => {
                event.stopPropagation();
                setSelectedPositionsRowId(row.id);
                setOpenPositionsMenuRowId((current) => (current === row.id ? null : row.id));
              }}
            >
              ⋯
            </button>
            {openPositionsMenuRowId === row.id ? (
              <div className="monitor-action-menu" onClick={(event) => event.stopPropagation()}>
                <div className="monitor-action-header">
                  <div className="monitor-action-symbol">{row.displaySymbol ?? row.symbol}</div>
                  <div className="monitor-action-description">{row.displayDescription ?? row.description}</div>
                </div>
                <div className="monitor-action-divider" />
                <div className="monitor-action-group-label">Open</div>
                <button className="monitor-action-item" onClick={() => openPositionsDrawer(row, "summary")}>View Position Details</button>
                <button className="monitor-action-item" onClick={() => openPositionsDrawer(row, "trades")}>View Trades</button>
                <button className="monitor-action-item" onClick={() => openPositionsDrawer(row, "orders")}>View Orders</button>
                <div className="monitor-action-divider" />
                <div className="monitor-action-group-label">Analyze</div>
                <button className="monitor-action-item" onClick={() => openPositionsDrawer(row, "attribution")}>View Strategy Attribution</button>
                <button className="monitor-action-item" onClick={() => openPositionsDrawer(row, "conflict")}>View Conflict Detail</button>
                <button className="monitor-action-item" onClick={() => openPositionsDrawer(row, "margin")}>Explain Margin / Buying Power</button>
                <div className="monitor-action-divider" />
                <div className="monitor-action-group-label">Reference</div>
                <button className="monitor-action-item" onClick={() => openPositionsDrawer(row, "activity")}>Open Recent Activity</button>
                <button className="monitor-action-item" onClick={() => openPositionsDrawer(row, "instrument")}>Open Instrument Detail</button>
              </div>
            ) : null}
          </div>
        </div>
      ),
      sortValue: (row) => row.symbol,
    },
    description: {
      id: "description",
      label: "Description",
      render: (row) => row.displayDescription ?? row.description ?? "—",
      sortValue: (row) => row.displayDescription ?? row.description,
    },
    qty: {
      id: "qty",
      label: "Qty",
      align: "right",
      render: (row) => formatCompactMetric(row.brokerQty, 4),
      sortValue: (row) => row.brokerQty,
    },
    avgPrice: {
      id: "avgPrice",
      label: "Avg Price",
      align: "right",
      render: (row) => formatCompactPrice(row.brokerAvgPrice),
      sortValue: (row) => row.brokerAvgPrice,
    },
    mark: {
      id: "mark",
      label: "Mark / Last",
      align: "right",
      render: (row) => {
        const value = positionsViewMode === "broker" ? row.brokerMark : row.paperMark;
        if (row.isSpreadParent && positionsViewMode !== "paper") {
          return (
            <div className="monitor-stack compact">
              <span>{formatCompactPrice(value)}</span>
              <span className="monitor-cell-note">NET MARK</span>
            </div>
          );
        }
        return formatCompactPrice(value);
      },
      sortValue: (row) => (positionsViewMode === "broker" ? row.brokerMark : row.paperMark),
    },
    dayPnl: {
      id: "dayPnl",
      label: "P/L Day",
      align: "right",
      render: (row) => renderPnlValue(positionsViewMode === "broker" ? row.brokerDayPnl : row.paperDayPnl),
      sortValue: (row) => (positionsViewMode === "broker" ? row.brokerDayPnl : row.paperDayPnl),
    },
    openPnl: {
      id: "openPnl",
      label: "P/L Open",
      align: "right",
      render: (row) => renderPnlValue(positionsViewMode === "broker" ? row.brokerOpenPnl : row.paperOpenPnl),
      sortValue: (row) => (positionsViewMode === "broker" ? row.brokerOpenPnl : row.paperOpenPnl),
    },
    realizedPnl: {
      id: "realizedPnl",
      label: "Realized P/L",
      align: "right",
      render: (row) => renderPnlValue(positionsViewMode === "broker" ? row.brokerRealized : row.paperRealized),
      sortValue: (row) => (positionsViewMode === "broker" ? row.brokerRealized : row.paperRealized),
    },
    ytdPnl: {
      id: "ytdPnl",
      label: "P/L YTD",
      align: "right",
      render: (row) => renderPnlValue(row.brokerYtdPnl),
      sortValue: (row) => row.brokerYtdPnl,
    },
    marketValue: {
      id: "marketValue",
      label: "Net Liq / Market Value",
      align: "right",
      render: (row) =>
        row.isSpreadParent ? (
          <div className="monitor-stack compact">
            <span>{formatCompactMetric(row.brokerMarketValue)}</span>
            <span className="monitor-cell-note">NET VALUE</span>
          </div>
        ) : (
          formatCompactMetric(row.brokerMarketValue)
        ),
      sortValue: (row) => row.brokerMarketValue,
    },
    marginEffect: {
      id: "marginEffect",
      label: "Margin / BP Effect",
      align: "right",
      render: (row) => formatCompactMetric(row.brokerMarginEffect),
      sortValue: (row) => row.brokerMarginEffect,
    },
    delta: {
      id: "delta",
      label: "Delta",
      align: "right",
      render: (row) => formatCompactMetric(row.brokerDelta, 4),
      sortValue: (row) => row.brokerDelta,
    },
    theta: {
      id: "theta",
      label: "Theta",
      align: "right",
      render: (row) => formatCompactMetric(row.brokerTheta, 4),
      sortValue: (row) => row.brokerTheta,
    },
    lastActivity: {
      id: "lastActivity",
      label: "Last Activity",
      render: (row) =>
        positionsViewMode === "broker" && brokerDataStale ? (
          <div className="monitor-stack compact">
            <span>{formatCompactTimestamp(row.lastActivity)}</span>
            <span className="monitor-cell-note stale">STALE</span>
          </div>
        ) : (
          formatCompactTimestamp(row.lastActivity)
        ),
      sortValue: (row) => parseSortTimestamp(row.lastActivity),
    },
    strategyCount: {
      id: "strategyCount",
      label: "Strategy Count",
      align: "right",
      render: (row) => formatCompactMetric(row.strategyCount, 0),
      sortValue: (row) => row.strategyCount,
    },
    netQty: {
      id: "netQty",
      label: "Net Qty",
      align: "right",
      render: (row) => formatCompactMetric(row.paperQty, 0),
      sortValue: (row) => row.paperQty,
    },
    avgEntry: {
      id: "avgEntry",
      label: "Avg Entry",
      align: "right",
      render: (row) => formatCompactPrice(row.paperAvgEntry),
      sortValue: (row) => row.paperAvgEntry,
    },
    tradeCount: {
      id: "tradeCount",
      label: "Trade Count",
      align: "right",
      render: (row) => formatCompactMetric(row.tradeCount, 0),
      sortValue: (row) => row.tradeCount,
    },
    maxDrawdown: {
      id: "maxDrawdown",
      label: "Max Drawdown",
      align: "right",
      render: (row) => renderPnlValue(row.maxDrawdown),
      sortValue: (row) => row.maxDrawdown,
    },
    primaryStrategy: {
      id: "primaryStrategy",
      label: "Primary Strategy",
      render: (row) => row.primaryStrategy,
      sortValue: (row) => row.primaryStrategy,
    },
    class: {
      id: "class",
      label: "Class",
      render: (row) => (row.paperClass ? <Badge label={row.paperClass} tone={row.paperClass === "EXPERIMENTAL" ? "warn" : "muted"} /> : "—"),
      sortValue: (row) => row.paperClass,
    },
    currentStatus: {
      id: "currentStatus",
      label: "Current Status",
      render: (row) => row.currentStatus,
      sortValue: (row) => row.currentStatus,
    },
    brokerQty: {
      id: "brokerQty",
      label: "Broker Qty",
      align: "right",
      render: (row) => formatCompactMetric(row.brokerQty, 4),
      sortValue: (row) => row.brokerQty,
    },
    paperQty: {
      id: "paperQty",
      label: "Paper Qty",
      align: "right",
      render: (row) => formatCompactMetric(row.paperQty, 0),
      sortValue: (row) => row.paperQty,
    },
    brokerOpenPnl: {
      id: "brokerOpenPnl",
      label: "Broker Open P/L",
      align: "right",
      render: (row) => renderPnlValue(row.brokerOpenPnl),
      sortValue: (row) => row.brokerOpenPnl,
    },
    paperOpenPnl: {
      id: "paperOpenPnl",
      label: "Paper Open P/L",
      align: "right",
      render: (row) => renderPnlValue(row.paperOpenPnl),
      sortValue: (row) => row.paperOpenPnl,
    },
    brokerRealized: {
      id: "brokerRealized",
      label: "Broker Realized",
      align: "right",
      render: (row) => renderPnlValue(row.brokerRealized),
      sortValue: (row) => row.brokerRealized,
    },
    paperRealized: {
      id: "paperRealized",
      label: "Paper Realized",
      align: "right",
      render: (row) => renderPnlValue(row.paperRealized),
      sortValue: (row) => row.paperRealized,
    },
    combinedRealized: {
      id: "combinedRealized",
      label: "Combined Realized",
      align: "right",
      render: (row) => renderPnlValue(row.brokerRealized !== null && row.paperRealized !== null ? row.brokerRealized + row.paperRealized : row.paperRealized),
      sortValue: (row) => (row.brokerRealized !== null && row.paperRealized !== null ? row.brokerRealized + row.paperRealized : row.paperRealized),
    },
    netValue: {
      id: "netValue",
      label: "Net Value",
      render: (row) => (
        <div className="monitor-combined-cell">
          <span className="monitor-source-inline">B {formatCompactMetric(row.brokerMarketValue)}</span>
          <span className="monitor-source-inline">P {renderPnlValue(row.paperOpenPnl)}</span>
        </div>
      ),
      sortValue: (row) => sumNullable([row.brokerMarketValue, row.paperOpenPnl]),
    },
    conflict: {
      id: "conflict",
      label: "Conflict",
      render: (row) =>
        row.conflict === "—" ? (
          <span className="monitor-conflict-chip quiet">—</span>
        ) : (
          <span className={`monitor-conflict-chip ${row.conflict === "Split" ? "quiet" : "actionable"}`}>{row.conflict}</span>
        ),
      sortValue: (row) => row.conflict,
    },
    runtimeLoaded: {
      id: "runtimeLoaded",
      label: "Runtime Loaded",
      render: (row) => row.runtimeLoaded,
      sortValue: (row) => row.runtimeLoaded,
    },
    session: {
      id: "session",
      label: "Session",
      render: (row) => row.session,
      sortValue: (row) => row.session,
    },
    entryHold: {
      id: "entryHold",
      label: "Entry Hold",
      render: (row) => row.entryHold,
      sortValue: (row) => row.entryHold,
    },
    reviewState: {
      id: "reviewState",
      label: "Review State",
      render: (row) => row.reviewState,
      sortValue: (row) => row.reviewState,
    },
    latestIntentTime: {
      id: "latestIntentTime",
      label: "Latest Intent Time",
      render: (row) => formatCompactTimestamp(row.latestIntentTime),
      sortValue: (row) => parseSortTimestamp(row.latestIntentTime),
    },
    latestFillTime: {
      id: "latestFillTime",
      label: "Latest Fill Time",
      render: (row) => formatCompactTimestamp(row.latestFillTime),
      sortValue: (row) => parseSortTimestamp(row.latestFillTime),
    },
    latestTradeTime: {
      id: "latestTradeTime",
      label: "Latest Trade Time",
      render: (row) => formatCompactTimestamp(row.latestTradeTime),
      sortValue: (row) => parseSortTimestamp(row.latestTradeTime),
    },
    conflictState: {
      id: "conflictState",
      label: "Conflict State",
      render: (row) => row.conflictState,
      sortValue: (row) => row.conflictState,
    },
    gamma: { id: "gamma", label: "Gamma", align: "right", render: (row) => formatCompactMetric(row.gamma, 4), sortValue: (row) => row.gamma },
    vega: { id: "vega", label: "Vega", align: "right", render: (row) => formatCompactMetric(row.vega, 4), sortValue: (row) => row.vega },
    iv: { id: "iv", label: "IV", align: "right", render: (row) => formatCompactMetric(row.iv, 4), sortValue: (row) => row.iv },
    ivPercentile: { id: "ivPercentile", label: "IV Percentile", align: "right", render: (row) => formatCompactMetric(row.ivPercentile, 2), sortValue: (row) => row.ivPercentile },
    daysToExp: { id: "daysToExp", label: "Days to Exp", align: "right", render: (row) => formatCompactMetric(row.daysToExp, 0), sortValue: (row) => row.daysToExp },
    roc: { id: "roc", label: "ROC", align: "right", render: (row) => formatCompactMetric(row.roc, 2), sortValue: (row) => row.roc },
    yield: { id: "yield", label: "Yield", align: "right", render: (row) => formatCompactMetric(row.yieldValue, 2), sortValue: (row) => row.yieldValue },
    expectedMove: { id: "expectedMove", label: "Expected Move", align: "right", render: (row) => formatCompactMetric(row.expectedMove, 2), sortValue: (row) => row.expectedMove },
    quoteTrend: { id: "quoteTrend", label: "Quote Trend", render: (row) => row.quoteTrend ?? "—", sortValue: (row) => row.quoteTrend },
    initialMargin: { id: "initialMargin", label: "Initial Margin", align: "right", render: (row) => formatCompactMetric(row.initialMargin), sortValue: (row) => row.initialMargin },
    probItm: { id: "probItm", label: "Prob ITM", align: "right", render: (row) => formatCompactMetric(row.probItm, 2), sortValue: (row) => row.probItm },
    probOtm: { id: "probOtm", label: "Prob OTM", align: "right", render: (row) => formatCompactMetric(row.probOtm, 2), sortValue: (row) => row.probOtm },
    extrinsic: { id: "extrinsic", label: "Extrinsic", align: "right", render: (row) => formatCompactMetric(row.extrinsic), sortValue: (row) => row.extrinsic },
    intrinsic: { id: "intrinsic", label: "Intrinsic", align: "right", render: (row) => formatCompactMetric(row.intrinsic), sortValue: (row) => row.intrinsic },
  }), [brokerDataStale, openPositionsMenuRowId, positionsViewMode]);
  const visiblePositionsColumns = normalizedCurrentColumns
    .map((columnId) => positionsMonitorColumns[columnId])
    .filter((column): column is PositionsMonitorColumn => Boolean(column));
  const productionBrokerHealthy = asRecord(productionHealth.broker_reachable).ok === true;
  const productionAuthHealthy = asRecord(productionHealth.auth_healthy).ok === true;
  const productionAccountSelected = asRecord(productionHealth.account_selected).ok === true;
  const productionBalancesFresh = asRecord(productionHealth.balances_fresh).ok === true;
  const productionPositionsFresh = asRecord(productionHealth.positions_fresh).ok === true;
  const productionOrdersFresh = asRecord(productionHealth.orders_fresh).ok === true;
  const brokerOrdersOrFillsAvailable = productionOpenOrders.length > 0 || productionRecentFills.length > 0 || productionEvents.length > 0;
  const productionReconciliationFresh = asRecord(productionHealth.reconciliation_fresh).ok === true;
  const productionLinkDegraded = String(productionLink.status ?? "").toLowerCase() === "degraded";
  const manualAssetClasses = supportedManualAssetClasses.length ? supportedManualAssetClasses : ["STOCK"];
  const assetScopedOrderTypes = asArray<string>(orderTypeMatrixByAssetClass[manualOrderForm.assetClass]);
  const manualOrderTypes = assetScopedOrderTypes.length
    ? assetScopedOrderTypes
    : supportedManualDryRunOrderTypes.length
      ? supportedManualDryRunOrderTypes
      : ["LIMIT"];
  const manualTimeInForceOptions = supportedManualTimeInForceValues.length ? supportedManualTimeInForceValues : ["DAY", "GTC"];
  const manualSessionOptions = supportedManualSessionValues.length ? supportedManualSessionValues : ["NORMAL"];
  const manualSideOptions = productionCapabilities.sell_short === true ? ["BUY", "SELL", "SELL_SHORT", "BUY_TO_COVER"] : ["BUY", "SELL", "BUY_TO_COVER"];
  const productionManualSafetyConstraints = asRecord(productionManualSafety.constraints);
  const productionManualSafetyBlockers = asArray<string>(productionManualSafety.blockers);
  const productionManualSafetyWarnings = asArray<string>(productionManualSafety.warnings);
  const productionPilotLockedPolicy = asRecord(productionPilotReadiness.locked_policy ?? productionPilotScope);
  const firstLiveStockLimitTest = asRecord(productionManualSafetyConstraints.first_live_stock_limit_test);
  const advancedTifTicketSupport = productionCapabilities.advanced_tif_ticket_support === true;
  const ocoTicketSupport = productionCapabilities.oco_ticket_support === true;
  const extExtoLiveSubmitEnabled = productionCapabilities.ext_exto_live_submit === true;
  const ocoLiveSubmitEnabled = productionCapabilities.oco_live_submit === true;
  const trailingLiveSubmitEnabled = productionCapabilities.trailing_live_submit === true;
  const closeOrderLiveSubmitEnabled = productionCapabilities.close_order_live_submit === true;
  const futuresLiveSubmitEnabled = productionCapabilities.futures_live_submit === true;
  const productionManualSubmitEnabled =
    productionLinkEnabled() &&
    productionFeatureFlags.manual_order_ticket_enabled === true &&
    productionManualSafety.submit_enabled === true;
  const productionManualCancelEnabled =
    productionLinkEnabled() &&
    productionFeatureFlags.manual_order_ticket_enabled === true &&
    productionFeatureFlags.live_order_submit_enabled === true &&
    productionAuthHealthy &&
    productionBrokerHealthy &&
    productionAccountSelected &&
    productionOrdersFresh;
  const productionReplaceEnabled = productionCapabilities.manual_order_replace === true;
  const manualSymbolWhitelist = asArray<string>(productionManualSafetyConstraints.symbol_whitelist);
  const manualMaxQuantity = String(productionManualSafetyConstraints.max_quantity ?? "1");
  const preferredPilotSymbol =
    manualOrderForm.symbol.trim().toUpperCase()
    || selectedProductionSymbol
    || String(productionPilotCycle.symbol ?? "").trim().toUpperCase()
    || manualSymbolWhitelist[0]
    || "";
  const selectedProductionPositionIsLongOneShare =
    String(selectedProductionPosition?.side ?? "").trim().toUpperCase() === "LONG"
    && Number(selectedProductionPosition?.quantity ?? 0) === 1;
  const selectedProductionPositionIsWhitelisted =
    !manualSymbolWhitelist.length || manualSymbolWhitelist.includes(selectedProductionSymbol);
  const pilotRunbookEntryEligible =
    productionPilotReadiness.submit_eligible === true
    && Boolean(preferredPilotSymbol)
    && !(selectedProductionPositionIsLongOneShare && selectedProductionSymbol === preferredPilotSymbol);
  const pilotRunbookEntryDetail =
    productionPilotReadiness.submit_eligible !== true
      ? String(productionPilotReadiness.blocked_reason ?? productionPilotReadiness.detail ?? "Pilot submit is not currently eligible.")
      : !preferredPilotSymbol
        ? "No whitelisted pilot symbol is currently selected in the operator ticket."
        : selectedProductionPositionIsLongOneShare && selectedProductionSymbol === preferredPilotSymbol
          ? `An existing LONG 1 ${preferredPilotSymbol} broker position is open. Use SELL_TO_CLOSE before another BUY_TO_OPEN pilot submit.`
          : `BUY_TO_OPEN is eligible now for ${preferredPilotSymbol}.`
  const pilotRunbookCloseEligible =
    productionPilotReadiness.submit_eligible === true
    && selectedProductionPositionIsLongOneShare
    && selectedProductionPositionIsWhitelisted;
  const pilotRunbookCloseDetail =
    productionPilotReadiness.submit_eligible !== true
      ? String(productionPilotReadiness.blocked_reason ?? productionPilotReadiness.detail ?? "Pilot submit is not currently eligible.")
      : !selectedProductionPositionIsLongOneShare
        ? "SELL_TO_CLOSE requires a selected live broker position of LONG 1 share."
        : !selectedProductionPositionIsWhitelisted
          ? `Selected position ${selectedProductionSymbol || "UNKNOWN"} is outside the locked pilot whitelist.`
          : `SELL_TO_CLOSE is eligible now for ${selectedProductionSymbol}.`
  const manualAdvancedMode = deriveAdvancedModeLabel(manualOrderForm.timeInForce, manualOrderForm.session);
  const manualAdvancedDryRun = isAdvancedDryRunMode(manualOrderForm.timeInForce, manualOrderForm.session, manualOrderForm.structureType);
  const selectedManualVerificationKey = deriveManualVerificationKey(manualOrderForm, manualAdvancedMode);
  const selectedManualVerification = verificationEntryByKey(orderTypeLiveVerificationMatrix, selectedManualVerificationKey);
  const productionManualSubmitStatusLabel = String(
    productionManualSafety.submit_status_label ??
      (productionManualSubmitEnabled
        ? "LIVE SUBMIT ELIGIBLE"
        : productionFeatureFlags.manual_order_ticket_enabled !== true
          ? "TICKET FLAG OFF"
          : productionFeatureFlags.live_order_submit_enabled !== true
            ? "SUBMIT SAFETY OFF"
            : "LIVE SUBMIT BLOCKED"),
  ).trim();
  const productionManualSubmitStatusDetail = String(
    productionManualSafety.submit_status_detail ??
      productionManualSafetyBlockers[0] ??
      (productionManualSubmitEnabled ? "All current manual live-submit safety gates are satisfied for the configured pilot scope." : ""),
  ).trim();
  const selectedManualVerificationStatusText = selectedManualVerificationStatusLabel(selectedManualVerification);
  const selectedManualVerificationDetail = String(
    selectedManualVerification.blocker_reason ??
      (selectedManualVerification.live_enabled === true
        ? "This order type is within the current live-verification scope."
        : selectedManualVerification.previewable === true
          ? "This order type is reviewable in preview mode, but live submit is still gated."
          : "This order type is blocked in the current environment."),
  ).trim();
  const productionReplaceStatusText = productionReplaceEnabled ? "ENABLED" : "REPLACE FLAG OFF";
  const productionReplaceStatusDetail = productionReplaceEnabled
    ? "Replace is enabled for the current environment."
    : "Replace is separately blocked because MGC_PRODUCTION_REPLACE_ORDER_ENABLED is false and Schwab replace semantics are not yet live-verified.";
  const productionPilotOpenRoute = asRecord(productionPilotLockedPolicy.allowed_open_route);
  const productionPilotCloseRoute = asRecord(productionPilotLockedPolicy.allowed_close_route);
  const firstLiveStockLimitActive = selectedManualVerificationKey === "STOCK:LIMIT";
  const liveEnabledOrderTypesForAsset = asArray<string>(liveEnabledOrderTypesByAssetClass[manualOrderForm.assetClass]);
  const dryRunOnlyOrderTypesForAsset = asArray<string>(dryRunOnlyOrderTypesByAssetClass[manualOrderForm.assetClass]);
  const firstLiveStockLimitReadiness = useMemo<FirstLiveStockLimitCheck[]>(() => {
    const checks: FirstLiveStockLimitCheck[] = [
      {
        label: "Feature flags",
        ok:
          productionLinkEnabled() &&
          productionFeatureFlags.manual_order_ticket_enabled === true &&
          productionFeatureFlags.live_order_submit_enabled === true &&
          productionCapabilities.stock_limit_live_submit === true,
        detail: `link=${productionLinkEnabled()} ticket=${productionFeatureFlags.manual_order_ticket_enabled === true} live_submit=${productionFeatureFlags.live_order_submit_enabled === true} stock_limit=${productionCapabilities.stock_limit_live_submit === true}`,
      },
      {
        label: "Whitelist",
        ok: manualSymbolWhitelist.length > 0 && manualOrderForm.symbol.trim().length > 0 && manualSymbolWhitelist.includes(manualOrderForm.symbol.trim().toUpperCase()),
        detail: manualSymbolWhitelist.length
          ? `whitelist=${manualSymbolWhitelist.join(", ")} selected=${manualOrderForm.symbol.trim().toUpperCase() || "None"}`
          : "Manual live submit requires a non-empty stock symbol whitelist.",
      },
      {
        label: "Asset / type / quantity",
        ok: manualOrderForm.assetClass === "STOCK" && manualOrderForm.orderType === "LIMIT" && Number(manualOrderForm.quantity) === 1,
        detail: `asset=${manualOrderForm.assetClass} type=${manualOrderForm.orderType} qty=${manualOrderForm.quantity || "None"}`,
      },
      {
        label: "Session / TIF",
        ok: manualOrderForm.timeInForce === "DAY" && manualOrderForm.session === "NORMAL",
        detail: `tif=${manualOrderForm.timeInForce} session=${manualOrderForm.session}`,
      },
      {
        label: "Account / auth / broker",
        ok: productionAuthHealthy && productionBrokerHealthy && productionAccountSelected,
        detail: `auth=${productionAuthHealthy} broker=${productionBrokerHealthy} account_selected=${productionAccountSelected}`,
      },
      {
        label: "Freshness",
        ok: productionBalancesFresh && productionPositionsFresh && productionOrdersFresh,
        detail: `balances=${productionBalancesFresh} positions=${productionPositionsFresh} orders=${productionOrdersFresh}`,
      },
      {
        label: "Reconciliation",
        ok: productionReconciliationFresh && String(productionReconciliation.status ?? "").toLowerCase() === "clear",
        detail: formatValue(productionReconciliation.label ?? productionReconciliation.status ?? productionReconciliation.detail),
      },
      {
        label: "Review confirmation",
        ok: manualOrderForm.reviewConfirmed,
        detail: manualOrderForm.reviewConfirmed ? "Review confirmed is checked." : "Check Review confirmed before submit.",
      },
    ];
    return checks;
  }, [
    manualOrderForm.assetClass,
    manualOrderForm.orderType,
    manualOrderForm.quantity,
    manualOrderForm.reviewConfirmed,
    manualOrderForm.session,
    manualOrderForm.symbol,
    manualOrderForm.timeInForce,
    manualSymbolWhitelist,
    productionAccountSelected,
    productionAuthHealthy,
    productionBalancesFresh,
    productionBrokerHealthy,
    productionCapabilities.stock_limit_live_submit,
    productionFeatureFlags.live_order_submit_enabled,
    productionFeatureFlags.manual_order_ticket_enabled,
    productionLink,
    productionOrdersFresh,
    productionPositionsFresh,
    productionReconciliation,
    productionReconciliationFresh,
  ]);
  const firstLiveStockLimitReadyNow = firstLiveStockLimitReadiness.every((item) => item.ok);
  const manualPreviewBlockers = useMemo(() => {
    const blockers: string[] = [];
    const quantity = Number(manualOrderForm.quantity);
    const maxQuantity = Number(manualMaxQuantity);
    if (!manualOrderForm.accountHash) {
      blockers.push("No broker account is selected.");
    }
    if (!manualOrderForm.symbol.trim()) {
      blockers.push("Symbol is required.");
    }
    if (!manualAssetClasses.includes(manualOrderForm.assetClass)) {
      blockers.push(`Asset class ${manualOrderForm.assetClass} is not enabled.`);
    }
    if (!manualTimeInForceOptions.includes(manualOrderForm.timeInForce)) {
      blockers.push(`Time in force ${manualOrderForm.timeInForce} is not enabled.`);
    }
    if (!manualSessionOptions.includes(manualOrderForm.session)) {
      blockers.push(`Session ${manualOrderForm.session} is not enabled.`);
    }
    if (manualOrderForm.structureType === "OCO" && !ocoTicketSupport) {
      blockers.push("OCO review support is disabled by feature flag.");
    }
    if (["EXT", "EXTO", "GTC_EXTO"].includes(manualAdvancedMode)) {
      if (!advancedTifTicketSupport) {
        blockers.push("EXTO / GTC_EXTO review support is disabled by feature flag.");
      }
      if (manualOrderForm.assetClass !== "STOCK") {
        blockers.push("Advanced EXTO / GTC_EXTO review is only modeled for STOCK in this phase.");
      }
    }
    if (manualOrderForm.structureType === "SINGLE" && !manualOrderTypes.includes(manualOrderForm.orderType)) {
      blockers.push(`Order type ${manualOrderForm.orderType} is not enabled for ${manualOrderForm.assetClass}.`);
    }
    if (!(quantity > 0)) {
      blockers.push("Quantity must be greater than zero.");
    }
    if (Number.isFinite(maxQuantity) && quantity > maxQuantity) {
      blockers.push(`Quantity exceeds max safety quantity ${manualMaxQuantity}.`);
    }
    if (manualSymbolWhitelist.length && !manualSymbolWhitelist.includes(manualOrderForm.symbol.trim().toUpperCase())) {
      blockers.push(`Symbol ${manualOrderForm.symbol.trim().toUpperCase() || "(blank)"} is not in the manual live-order whitelist.`);
    }
    if (manualOrderForm.structureType === "SINGLE") {
      if ((manualOrderForm.orderType === "LIMIT" || manualOrderForm.orderType === "STOP_LIMIT" || manualOrderForm.orderType === "LIMIT_ON_CLOSE") && !manualOrderForm.limitPrice.trim()) {
        blockers.push("Limit price is required for the selected order type.");
      }
      if ((manualOrderForm.orderType === "STOP" || manualOrderForm.orderType === "STOP_LIMIT") && !manualOrderForm.stopPrice.trim()) {
        blockers.push("Stop price is required for the selected order type.");
      }
      if ((manualOrderForm.orderType === "TRAIL_STOP" || manualOrderForm.orderType === "TRAIL_STOP_LIMIT") && !manualOrderForm.trailValue.trim()) {
        blockers.push("Trailing value is required for the selected order type.");
      }
      if ((manualOrderForm.orderType === "TRAIL_STOP" || manualOrderForm.orderType === "TRAIL_STOP_LIMIT") && !manualOrderForm.trailTriggerBasis.trim()) {
        blockers.push("Trail trigger basis is required for the selected order type.");
      }
      if (manualOrderForm.orderType === "TRAIL_STOP_LIMIT" && !manualOrderForm.trailLimitOffset.trim()) {
        blockers.push("Trail limit offset is required for TRAIL_STOP_LIMIT.");
      }
      if ((manualOrderForm.orderType === "TRAIL_STOP" || manualOrderForm.orderType === "TRAIL_STOP_LIMIT") && !["AMOUNT", "PERCENT"].includes(manualOrderForm.trailValueType)) {
        blockers.push("Trail value type must be AMOUNT or PERCENT.");
      }
      if (manualOrderForm.assetClass === "FUTURE" && ["MARKET_ON_CLOSE", "LIMIT_ON_CLOSE"].includes(manualOrderForm.orderType)) {
        blockers.push(`${manualOrderForm.orderType} is not supported for FUTURE.`);
      }
      if (["EXT", "EXTO", "GTC_EXTO"].includes(manualAdvancedMode) && !["LIMIT", "STOP_LIMIT"].includes(manualOrderForm.orderType)) {
        blockers.push("Advanced EXTO / GTC_EXTO review is only modeled for LIMIT and STOP_LIMIT orders in this phase.");
      }
    } else {
      if (manualOrderForm.ocoLegs.length !== 2) {
        blockers.push("OCO review requires exactly two legs.");
      }
      manualOrderForm.ocoLegs.forEach((leg) => {
        const legQuantity = Number(leg.quantity);
        if (!leg.side) {
          blockers.push(`${leg.legLabel} requires side.`);
        }
        if (!leg.orderType) {
          blockers.push(`${leg.legLabel} requires order type.`);
        }
        if (!(legQuantity > 0)) {
          blockers.push(`${leg.legLabel} quantity must be greater than zero.`);
        }
        if ((leg.orderType === "LIMIT" || leg.orderType === "STOP_LIMIT") && !leg.limitPrice.trim()) {
          blockers.push(`${leg.legLabel} requires a limit price.`);
        }
        if ((leg.orderType === "STOP" || leg.orderType === "STOP_LIMIT") && !leg.stopPrice.trim()) {
          blockers.push(`${leg.legLabel} requires a stop price.`);
        }
        if ((leg.orderType === "TRAIL_STOP" || leg.orderType === "TRAIL_STOP_LIMIT") && !leg.trailValue.trim()) {
          blockers.push(`${leg.legLabel} requires a trailing value.`);
        }
        if ((leg.orderType === "TRAIL_STOP" || leg.orderType === "TRAIL_STOP_LIMIT") && !leg.trailTriggerBasis.trim()) {
          blockers.push(`${leg.legLabel} requires a trail trigger basis.`);
        }
        if (leg.orderType === "TRAIL_STOP_LIMIT" && !leg.trailLimitOffset.trim()) {
          blockers.push(`${leg.legLabel} requires a trail limit offset.`);
        }
        if ((leg.orderType === "TRAIL_STOP" || leg.orderType === "TRAIL_STOP_LIMIT") && !["AMOUNT", "PERCENT"].includes(leg.trailValueType)) {
          blockers.push(`${leg.legLabel} requires AMOUNT or PERCENT trail mode.`);
        }
        if (manualOrderForm.assetClass === "FUTURE" && ["MARKET_ON_CLOSE", "LIMIT_ON_CLOSE"].includes(leg.orderType)) {
          blockers.push(`${leg.legLabel} uses ${leg.orderType}, which is not supported for FUTURE.`);
        }
        if (["EXT", "EXTO", "GTC_EXTO"].includes(manualAdvancedMode) && !["LIMIT", "STOP_LIMIT"].includes(leg.orderType)) {
          blockers.push("Advanced EXTO / GTC_EXTO OCO review is only modeled for LIMIT and STOP_LIMIT legs in this phase.");
        }
      });
    }
    if (!manualOrderForm.reviewConfirmed) {
      blockers.push("Explicit review confirmation is required.");
    }
    return Array.from(new Set(blockers));
  }, [
    advancedTifTicketSupport,
    manualAdvancedMode,
    manualAssetClasses,
    manualOrderForm.ocoLegs,
    manualOrderForm.structureType,
    manualMaxQuantity,
    manualOrderForm.accountHash,
    manualOrderForm.assetClass,
    manualOrderForm.limitPrice,
    manualOrderForm.orderType,
    manualOrderForm.quantity,
    manualOrderForm.reviewConfirmed,
    manualOrderForm.session,
    manualOrderForm.stopPrice,
    manualOrderForm.symbol,
    manualOrderForm.trailLimitOffset,
    manualOrderForm.trailTriggerBasis,
    manualOrderForm.trailValue,
    manualOrderForm.trailValueType,
    manualOrderForm.timeInForce,
    manualOrderTypes,
    manualSessionOptions,
    manualSymbolWhitelist,
    manualTimeInForceOptions,
    ocoTicketSupport,
  ]);
  const manualOrderGateBlockers = useMemo(() => {
    const blockers = [...productionManualSafetyBlockers, ...manualPreviewBlockers];
    if (manualOrderForm.structureType === "OCO") {
      blockers.push("OCO live submission remains disabled pending live Schwab verification.");
    }
    if (["EXT", "EXTO", "GTC_EXTO"].includes(manualAdvancedMode)) {
      blockers.push("EXTO / GTC_EXTO live submission remains disabled pending live Schwab verification.");
    }
    const selectedOrderTypes =
      manualOrderForm.structureType === "OCO" ? manualOrderForm.ocoLegs.map((leg) => leg.orderType) : [manualOrderForm.orderType];
    selectedOrderTypes.forEach((orderType) => {
      const verificationStatus = verificationEntryByKey(orderTypeLiveVerificationMatrix, `${manualOrderForm.assetClass}:${orderType}`);
      if (verificationStatus.blocked === true && verificationStatus.blocker_reason) {
        blockers.push(String(verificationStatus.blocker_reason));
      } else if (!liveEnabledOrderTypesForAsset.includes(orderType)) {
        blockers.push(`Order type ${orderType} is supported in preview but not live-enabled for ${manualOrderForm.assetClass}.`);
      }
      if (["TRAIL_STOP", "TRAIL_STOP_LIMIT"].includes(orderType) && !trailingLiveSubmitEnabled) {
        blockers.push("Trailing order live submission remains disabled pending live Schwab verification.");
      }
      if (["MARKET_ON_CLOSE", "LIMIT_ON_CLOSE"].includes(orderType) && !closeOrderLiveSubmitEnabled) {
        blockers.push("Market-on-close / limit-on-close live submission remains disabled pending live Schwab verification.");
      }
    });
    if (manualOrderForm.assetClass === "FUTURE" && !futuresLiveSubmitEnabled) {
      blockers.push("Futures live submission remains disabled pending live Schwab verification.");
    }
    if (manualOrderForm.timeInForce !== "DAY") {
      blockers.push("Only DAY time-in-force is enabled in the first live-order safety mode.");
    }
    if (manualOrderForm.session !== "NORMAL") {
      blockers.push("Only NORMAL session is enabled in the first live-order safety mode.");
    }
    return Array.from(new Set(blockers));
  }, [
    closeOrderLiveSubmitEnabled,
    futuresLiveSubmitEnabled,
    liveEnabledOrderTypesForAsset,
    manualAdvancedMode,
    manualOrderForm.assetClass,
    manualOrderForm.ocoLegs,
    manualOrderForm.orderType,
    manualOrderForm.session,
    manualOrderForm.structureType,
    manualOrderForm.timeInForce,
    manualPreviewBlockers,
    orderTypeLiveVerificationMatrix,
    productionManualSafetyBlockers,
    trailingLiveSubmitEnabled,
  ]);

  useEffect(() => {
    const selectedHash = String(selectedProductionAccount?.account_hash ?? "");
    if (!selectedHash) {
      return;
    }
    setManualOrderForm((current) => (current.accountHash ? current : { ...current, accountHash: selectedHash }));
  }, [selectedProductionAccount]);

  useEffect(() => {
    const nextKey = String(selectedProductionPosition?.position_key ?? "");
    if (!nextKey) {
      return;
    }
    setSelectedProductionPositionKey((current) => (current ? current : nextKey));
  }, [selectedProductionPosition]);

  useEffect(() => {
    if (!manualAssetClasses.length) {
      return;
    }
    setManualOrderForm((current) =>
      manualAssetClasses.includes(current.assetClass) ? current : { ...current, assetClass: manualAssetClasses[0] },
    );
  }, [manualAssetClasses]);

  useEffect(() => {
    if (!manualOrderTypes.length) {
      return;
    }
    setManualOrderForm((current) =>
      current.structureType === "SINGLE" && !manualOrderTypes.includes(current.orderType)
        ? { ...current, orderType: manualOrderTypes[0] }
        : current,
    );
  }, [manualOrderTypes]);

  useEffect(() => {
    setManualOrderForm((current) =>
      manualSideOptions.includes(current.side) ? current : { ...current, side: "BUY" },
    );
  }, [manualSideOptions]);

  useEffect(() => {
    if (!manualTimeInForceOptions.length) {
      return;
    }
    setManualOrderForm((current) =>
      manualTimeInForceOptions.includes(current.timeInForce) ? current : { ...current, timeInForce: manualTimeInForceOptions[0] },
    );
  }, [manualTimeInForceOptions]);

  useEffect(() => {
    if (!manualSessionOptions.length) {
      return;
    }
    setManualOrderForm((current) =>
      manualSessionOptions.includes(current.session) ? current : { ...current, session: manualSessionOptions[0] },
    );
  }, [manualSessionOptions]);

  useEffect(() => {
    const authenticatedIdentity = desktopState?.localAuth.local_operator_identity;
    if (!authenticatedIdentity) {
      return;
    }
    setSameUnderlyingOperatorLabel((current) =>
      !current.trim() || current.trim() === "manual operator" ? authenticatedIdentity : current,
    );
  }, [desktopState?.localAuth.local_operator_identity]);

  const homeMetrics = [
    { label: "Today Realized", value: formatMaybePnL(portfolio.daily_realized_pnl) },
    { label: "Current Unrealized", value: formatMaybePnL(portfolio.daily_unrealized_pnl) },
    { label: "Current Net", value: formatMaybePnL(portfolio.daily_net_pnl) },
    { label: "Session Max DD", value: formatMaybePnL(portfolio.intraday_max_drawdown) },
    { label: "Active Positions", value: formatShortNumber(portfolio.active_positions_count) },
    { label: "Active Signals", value: formatShortNumber(portfolio.active_signals_count) },
    { label: "Blocked Lanes", value: formatShortNumber(portfolio.blocked_lanes_count) },
    { label: "Active Instruments", value: formatShortNumber(portfolio.active_instruments_count) },
  ];
  const strategyRuntimeMetrics: Array<{ label: string; value: string; tone?: Tone }> = [
    {
      label: "Configured Standalone Strategies",
      value: formatShortNumber(
        strategyRuntimeSummary.configured_standalone_strategies ?? runtimeRegistrySummary.configured_standalone_strategies ?? runtimeRegistryRows.length,
      ),
    },
    {
      label: "Runtime Instances Present",
      value: formatShortNumber(strategyRuntimeSummary.runtime_instances_present ?? runtimeRegistrySummary.runtime_instances_present),
    },
    {
      label: "Runtime States Loaded",
      value: formatShortNumber(strategyRuntimeSummary.runtime_states_loaded ?? runtimeRegistrySummary.runtime_states_loaded),
      tone: Number(strategyRuntimeSummary.runtime_states_loaded ?? runtimeRegistrySummary.runtime_states_loaded ?? 0) > 0 ? "good" : "warn",
    },
    {
      label: "Can Process Bars",
      value: formatShortNumber(strategyRuntimeSummary.can_process_bars ?? runtimeRegistrySummary.can_process_bars),
    },
    {
      label: "In-Position Strategies",
      value: formatShortNumber(strategyRuntimeSummary.in_position_strategies),
      tone: Number(strategyRuntimeSummary.in_position_strategies ?? 0) > 0 ? "warn" : "muted",
    },
    {
      label: "Faults / Blockers",
      value: formatShortNumber(strategyRuntimeSummary.strategies_with_faults_or_blockers),
      tone: Number(strategyRuntimeSummary.strategies_with_faults_or_blockers ?? 0) > 0 ? "danger" : "good",
    },
    {
      label: "Same-Underlying Ambiguities",
      value: formatShortNumber(
        strategyRuntimeSummary.same_underlying_ambiguity_count ?? runtimeRegistrySummary.same_underlying_ambiguity_count,
      ),
      tone:
        Number(strategyRuntimeSummary.same_underlying_ambiguity_count ?? runtimeRegistrySummary.same_underlying_ambiguity_count ?? 0) > 0
          ? "warn"
          : "good",
    },
    {
      label: "Last Refresh",
      value: formatTimestamp(strategyRuntimeSummary.generated_at ?? desktopState?.refreshedAt),
    },
  ];
  const temporaryPaperIntegrityRows = asArray<JsonRecord>(temporaryPaperRuntimeIntegrity.rows);
  const temporaryPaperMissingLaneIds = asArray<unknown>(temporaryPaperRuntimeIntegrity.missing_lane_ids).map((item) => String(item));
  const temporaryPaperUnresolvedLaneIds = asArray<unknown>(temporaryPaperRuntimeIntegrity.unresolved_start_lane_ids).map((item) => String(item));
  const temporaryPaperStartFlags = asArray<unknown>(temporaryPaperRuntimeIntegrity.start_flags).map((item) => String(item));
  const temporaryPaperIntegrityMetrics: Array<{ label: string; value: string; tone?: Tone }> = [
    {
      label: "Enabled In App",
      value: formatShortNumber(temporaryPaperRuntimeIntegrity.enabled_in_app_count ?? temporaryPaperStrategyRows.length),
    },
    {
      label: "Loaded In Runtime",
      value: formatShortNumber(temporaryPaperRuntimeIntegrity.loaded_in_runtime_count),
      tone: Number(temporaryPaperRuntimeIntegrity.loaded_in_runtime_count ?? 0) > 0 ? "good" : "warn",
    },
    {
      label: "Runtime State Loaded",
      value: formatShortNumber(temporaryPaperRuntimeIntegrity.runtime_state_loaded_count),
      tone: Number(temporaryPaperRuntimeIntegrity.runtime_state_loaded_count ?? 0) > 0 ? "good" : "warn",
    },
    {
      label: "Snapshot Only",
      value: formatShortNumber(temporaryPaperRuntimeIntegrity.snapshot_only_count),
      tone: Number(temporaryPaperRuntimeIntegrity.snapshot_only_count ?? 0) > 0 ? "danger" : "good",
    },
    {
      label: "Mismatch Status",
      value: formatValue(temporaryPaperRuntimeIntegrity.mismatch_status ?? "UNKNOWN"),
      tone: statusTone(temporaryPaperRuntimeIntegrity.mismatch_status),
    },
  ];
  const paperStatus = asRecord(paper.status);
  const rawPaperOperatorStatus = asRecord(paper.raw_operator_status);
  const latestRuntimeCaptureTimestamp =
    String(rawPaperOperatorStatus.updated_at ?? paperStatus.updated_at ?? paperReadiness.generated_at ?? "") || null;
  const latestStrategyActivityTimestamp = strategyPerformanceRows.reduce<string | null>((latest, row) => {
    const candidate = String(row.latest_activity_timestamp ?? "");
    if (!candidate) {
      return latest;
    }
    return !latest || parseTimestampMs(candidate) > parseTimestampMs(latest) ? candidate : latest;
  }, null);
  const runtimeCaptureLagMinutes =
    latestRuntimeCaptureTimestamp ? Math.max(0, Math.round((Date.now() - parseTimestampMs(latestRuntimeCaptureTimestamp)) / 60000)) : null;
  const paperRuntimeStopped = paper.running !== true;
  const staleStrategyRows = strategyPerformanceRows.filter((row) => {
    const ts = String(row.latest_activity_timestamp ?? "");
    if (!ts) {
      return true;
    }
    return latestRuntimeCaptureTimestamp ? parseTimestampMs(ts) < parseTimestampMs(latestRuntimeCaptureTimestamp) : false;
  });
  const captureHealthLabel =
    paperRuntimeStopped
      ? "CAPTURE FAULT"
      : temporaryPaperRuntimeIntegrity.mismatch_status === "MISMATCH"
        ? "RUNTIME MISMATCH"
        : runtimeCaptureLagMinutes !== null && runtimeCaptureLagMinutes > 10
          ? "STALE / NOT REFRESHING"
          : "CAPTURE HEALTHY";
  const captureHealthTone: Tone =
    captureHealthLabel === "CAPTURE HEALTHY" ? "good" : captureHealthLabel === "STALE / NOT REFRESHING" ? "warn" : "danger";
  const paperCaptureIntegrityMetrics: Array<{ label: string; value: string; tone?: Tone }> = [
    { label: "Runtime", value: paperRuntimeStopped ? "STOPPED" : "RUNNING", tone: paperRuntimeStopped ? "danger" : "good" },
    { label: "Capture Health", value: captureHealthLabel, tone: captureHealthTone },
    { label: "Latest Runtime Update", value: latestRuntimeCaptureTimestamp ? formatTimestamp(latestRuntimeCaptureTimestamp) : "Unavailable", tone: latestRuntimeCaptureTimestamp ? "good" : "warn" },
    { label: "Latest Strategy Activity", value: latestStrategyActivityTimestamp ? formatTimestamp(latestStrategyActivityTimestamp) : "Unavailable", tone: latestStrategyActivityTimestamp ? "good" : "warn" },
    { label: "Stale Strategy Rows", value: formatShortNumber(staleStrategyRows.length), tone: staleStrategyRows.length > 0 ? "warn" : "good" },
    { label: "Temp Runtime Mismatch", value: formatValue(temporaryPaperRuntimeIntegrity.mismatch_status ?? "UNKNOWN"), tone: statusTone(temporaryPaperRuntimeIntegrity.mismatch_status) },
  ];
  const localAuthMetrics: Array<{ label: string; value: string; tone?: Tone }> = [
    {
      label: "Touch ID Available",
      value: formatValue(localOperatorAuth.touch_id_available ?? false),
      tone: localOperatorAuth.touch_id_available === true ? "good" : "warn",
    },
    {
      label: "Auth Session",
      value: formatValue(localOperatorAuth.auth_session_active ? "ACTIVE" : "INACTIVE"),
      tone: localOperatorAuth.auth_session_active ? "good" : localOperatorAuth.auth_available ? "warn" : "muted",
    },
    {
      label: "Method",
      value: formatValue(localOperatorAuth.auth_method ?? "NONE"),
      tone: localOperatorAuth.auth_method === "TOUCH_ID" ? "good" : "muted",
    },
    {
      label: "Identity",
      value: formatValue(localOperatorAuth.local_operator_identity ?? "Unauthenticated"),
      tone: localOperatorAuth.auth_session_active ? "good" : "muted",
    },
    {
      label: "Last Auth",
      value: formatTimestamp(localOperatorAuth.last_authenticated_at),
      tone: localOperatorAuth.last_authenticated_at ? "good" : "muted",
    },
    {
      label: "Session Expires",
      value: formatTimestamp(localOperatorAuth.auth_session_expires_at),
      tone: localOperatorAuth.auth_session_active ? "warn" : "muted",
    },
    {
      label: "Last Result",
      value: formatValue(localOperatorAuth.last_auth_result ?? "NONE"),
      tone:
        localOperatorAuth.last_auth_result === "SUCCEEDED"
          ? "good"
          : localOperatorAuth.last_auth_result === "CANCELED"
            ? "warn"
            : localOperatorAuth.last_auth_result === "NONE"
              ? "muted"
              : "danger",
    },
    {
      label: "Secret Wrapper",
      value: formatValue(localOperatorAuth.secret_protection?.wrapper_ready ? "READY" : "DEFERRED"),
      tone: localOperatorAuth.secret_protection?.wrapper_ready ? "good" : "warn",
    },
  ];
  const sameUnderlyingConflictMetrics: Array<{ label: string; value: string; tone?: Tone }> = [
    {
      label: "Total Conflicts",
      value: formatShortNumber(sameUnderlyingConflictSummary.conflict_count ?? sortedSameUnderlyingConflictRows.length),
      tone: sortedSameUnderlyingConflictRows.length > 0 ? "warn" : "good",
    },
    {
      label: "Blocking",
      value: formatShortNumber(
        sameUnderlyingConflictSummary.blocking_conflict_count ?? sameUnderlyingBlockingConflictRows.length,
      ),
      tone: sameUnderlyingBlockingConflictRows.length > 0 ? "danger" : "good",
    },
    {
      label: "Actionable",
      value: formatShortNumber(
        Number(asRecord(sameUnderlyingConflictSummary.severity_counts).ACTION ?? 0) +
          Number(asRecord(sameUnderlyingConflictSummary.severity_counts).WARNING ?? 0),
      ),
      tone:
        Number(asRecord(sameUnderlyingConflictSummary.severity_counts).ACTION ?? 0) +
          Number(asRecord(sameUnderlyingConflictSummary.severity_counts).WARNING ?? 0) >
        0
          ? "warn"
          : "good",
    },
    {
      label: "Info",
      value: formatShortNumber(asRecord(sameUnderlyingConflictSummary.severity_counts).INFO ?? 0),
      tone: Number(asRecord(sameUnderlyingConflictSummary.severity_counts).INFO ?? 0) > 0 ? "muted" : "good",
    },
    {
      label: "Broker Overlap",
      value: formatShortNumber(sameUnderlyingBrokerConflictRows.length),
      tone: sameUnderlyingBrokerConflictRows.length > 0 ? "danger" : "good",
    },
    {
      label: "Acknowledged",
      value: formatShortNumber(sameUnderlyingConflictSummary.acknowledged_count ?? 0),
      tone: Number(sameUnderlyingConflictSummary.acknowledged_count ?? 0) > 0 ? "good" : "muted",
    },
    {
      label: "Holding",
      value: formatShortNumber(sameUnderlyingConflictSummary.holding_count ?? sameUnderlyingHoldingRows.length),
      tone: sameUnderlyingHoldingRows.length > 0 ? "danger" : "good",
    },
    {
      label: "Hold Expired",
      value: formatShortNumber(sameUnderlyingConflictSummary.hold_expired_count ?? sameUnderlyingExpiredRows.length),
      tone: sameUnderlyingExpiredRows.length > 0 ? "warn" : "good",
    },
    {
      label: "Stale / Reopened",
      value: formatShortNumber(sameUnderlyingConflictSummary.stale_count ?? sameUnderlyingStaleRows.length),
      tone: sameUnderlyingStaleRows.length > 0 ? "warn" : "good",
    },
    {
      label: "Affected Instruments",
      value: formatShortNumber(asArray<string>(sameUnderlyingConflictSummary.affected_instruments).length),
    },
    {
      label: "Blocking Unacknowledged",
      value: formatShortNumber(sameUnderlyingConflictSummary.blocking_unacknowledged_count ?? sameUnderlyingBlockingConflictRows.length),
      tone: Number(sameUnderlyingConflictSummary.blocking_unacknowledged_count ?? sameUnderlyingBlockingConflictRows.length) > 0 ? "danger" : "good",
    },
    {
      label: "Recent Conflict Activity",
      value: formatShortNumber(sameUnderlyingEventSummary.event_count ?? 0),
      tone: Number(sameUnderlyingEventSummary.event_count ?? 0) > 0 ? "warn" : "muted",
    },
    {
      label: "Last Refresh",
      value: formatTimestamp(sameUnderlyingConflicts.generated_at ?? desktopState?.refreshedAt),
    },
  ];
  const portfolioSnapshotMetrics = [
    { label: "Approved/Admitted Realized", value: formatMaybePnL(strategyPortfolioSnapshot.total_realized_pnl) },
    { label: "Temp Paper Realized", value: formatMaybePnL(temporaryPaperPortfolioSnapshot.total_realized_pnl) },
    { label: "Combined Realized", value: formatMaybePnL(combinedStrategyPortfolioSnapshot.total_realized_pnl) },
    {
      label: "Approved/Admitted Unrealized",
      value:
        strategyPortfolioSnapshot.unrealized_complete === false
          ? `${formatMaybePnL(strategyPortfolioSnapshot.total_unrealized_pnl)} (partial)`
          : formatMaybePnL(strategyPortfolioSnapshot.total_unrealized_pnl),
    },
    {
      label: "Temp Paper Unrealized",
      value:
        temporaryPaperPortfolioSnapshot.unrealized_complete === false
          ? `${formatMaybePnL(temporaryPaperPortfolioSnapshot.total_unrealized_pnl)} (partial)`
          : formatMaybePnL(temporaryPaperPortfolioSnapshot.total_unrealized_pnl),
    },
    { label: "Approved/Admitted Day P&L", value: formatMaybePnL(strategyPortfolioSnapshot.total_day_pnl) },
    { label: "Temp Paper Day P&L", value: formatMaybePnL(temporaryPaperPortfolioSnapshot.total_day_pnl) },
    { label: "Combined Day P&L", value: formatMaybePnL(combinedStrategyPortfolioSnapshot.total_day_pnl) },
    {
      label: "Combined Cumulative P&L",
      value:
        combinedStrategyPortfolioSnapshot.unrealized_complete === false
          ? `${formatMaybePnL(combinedStrategyPortfolioSnapshot.total_cumulative_pnl)} (partial)`
          : formatMaybePnL(combinedStrategyPortfolioSnapshot.total_cumulative_pnl),
    },
    { label: "Approved/Admitted Max DD", value: formatMaybePnL(strategyPortfolioSnapshot.total_max_drawdown) },
    { label: "Temp Paper Max DD", value: formatMaybePnL(temporaryPaperPortfolioSnapshot.total_max_drawdown) },
    { label: "Temp Paper Strategies", value: formatShortNumber(temporaryPaperPortfolioSnapshot.active_strategy_count) },
    { label: "Combined Strategies", value: formatShortNumber(combinedStrategyPortfolioSnapshot.active_strategy_count) },
    { label: "Combined Instruments", value: formatShortNumber(combinedStrategyPortfolioSnapshot.active_instrument_count) },
    { label: "Last Refresh", value: formatTimestamp(strategyPortfolioSnapshot.generated_at ?? desktopState?.refreshedAt) },
  ];
  const paperStartupMetrics: Array<{ label: string; value: string; tone?: Tone }> = [
    { label: "Launch Policy", value: "BACKEND AUTO-RECOVER IF SAFE", tone: "good" },
    { label: "Current State", value: paperStartupStateLabel, tone: statusTone(paperStartupStateLabel) },
    { label: "Block Category", value: paperStartupCategory, tone: paperStartupCategoryTone(paperStartupCategory) },
    { label: "Runtime Phase", value: formatValue(paperReadiness.runtime_phase ?? "STOPPED"), tone: statusTone(paperReadiness.runtime_phase) },
    { label: "Recovery State", value: formatValue(paperRuntimeRecoveryState), tone: statusTone(paperRuntimeRecoveryState) },
    { label: "Auth", value: authReadyForPaperStartup ? "READY" : "NOT READY", tone: authReadyForPaperStartup ? "good" : "warn" },
    { label: "Temp-Paper Inclusion", value: tempPaperMismatchActive ? "MISMATCH" : "MATCHED", tone: tempPaperMismatchActive ? "danger" : "good" },
    { label: "Auto-Recoverable", value: formatValue(paperAutoStartEligible), tone: paperAutoStartEligible ? "good" : "warn" },
    { label: "Manual Action", value: formatValue(paperRuntimeRecovery.manual_action_required ?? false), tone: paperRuntimeRecovery.manual_action_required ? "warn" : "good" },
    { label: "Latest Attempt", value: formatTimestamp(paperRuntimeRecovery.attempted_at ?? latestPaperStartBlock.timestamp ?? paperRunStartCurrent.timestamp) },
  ];
  const dashboardRosterRows = useMemo(
    () => {
      const liveApprovedRows = [...approvedModelRows].map((row) => ({
        ...asRecord(approvedModelDetailsByBranch[String(row.branch ?? "")]),
        ...row,
      }));
      const liveLookup = new Map<string, JsonRecord>();
      for (const row of [...liveApprovedRows, ...runtimeRegistryRows, ...temporaryPaperStrategyRows]) {
        for (const key of [
          String(row.lane_id ?? ""),
          String(row.standalone_strategy_id ?? ""),
          String(row.tracked_strategy_id ?? ""),
        ]) {
          if (key) {
            liveLookup.set(key, row);
          }
        }
      }
      const trackedLookup = new Map<string, JsonRecord>();
      for (const row of trackedStrategyRows) {
        const key = String(row.strategy_id ?? row.tracked_strategy_id ?? "");
        if (key) {
          trackedLookup.set(key, row);
        }
      }
      const roster = new Map<string, JsonRecord>();
      for (const row of liveApprovedRows) {
        roster.set(String(row.lane_id ?? row.standalone_strategy_id ?? row.branch ?? strategyRowIdentity(row)), row);
      }
      for (const meta of ATP_PRODUCT_CATALOG) {
        const liveRow =
          liveLookup.get(meta.laneId)
          ?? liveLookup.get(meta.standaloneStrategyId)
          ?? liveLookup.get(meta.trackedStrategyId)
          ?? null;
        const trackedDetail = asRecord(trackedStrategyDetailsById[meta.trackedStrategyId] ?? trackedStrategyDetailsById[meta.laneId]);
        const trackedRow = trackedLookup.get(meta.trackedStrategyId) ?? trackedLookup.get(meta.laneId) ?? null;
        const mergedRow: JsonRecord = {
          ...trackedDetail,
          ...trackedRow,
          ...liveRow,
          lane_id: meta.laneId,
          branch: meta.displayName,
          display_name: meta.displayName,
          strategy_name: meta.displayName,
          tracked_strategy_id: meta.trackedStrategyId,
          standalone_strategy_id: String(liveRow?.standalone_strategy_id ?? meta.standaloneStrategyId),
          instrument: String(liveRow?.instrument ?? meta.instrument),
          observed_instruments: liveRow?.observed_instruments ?? [meta.instrument],
          strategy_family: String(liveRow?.strategy_family ?? "active_trend_participation_engine"),
          paper_strategy_class: meta.designation === "benchmark" ? "atp_benchmark_lane" : "atp_experimental_strategy",
          lane_class_label: meta.designation === "benchmark" ? "ATP Benchmark Lane" : "ATP Experimental Strategy",
          designation_label:
            meta.designation === "benchmark"
              ? "Benchmark Lane"
              : meta.candidateId
                ? "Experimental Candidate Lane"
                : "Experimental Strategy",
          benchmark_designation:
            meta.designation === "benchmark"
              ? String(liveRow?.benchmark_designation ?? trackedDetail.benchmark_designation ?? "CURRENT_ATP_COMPANION_BENCHMARK")
              : null,
          candidate_designation:
            meta.designation === "candidate"
              ? String(liveRow?.candidate_designation ?? trackedDetail.candidate_id ?? meta.candidateId ?? "ATP_COMPANION_CANDIDATE")
              : null,
          candidate_id: String(liveRow?.candidate_id ?? trackedDetail.candidate_id ?? meta.candidateId ?? ""),
          experimental_status: String(liveRow?.experimental_status ?? meta.experimentalStatus),
          participation_policy: String(liveRow?.participation_policy ?? meta.participationPolicy),
          execution_timeframe: String(liveRow?.execution_timeframe ?? "1m"),
          context_timeframes: asArray<string>(liveRow?.context_timeframes).length ? asArray<string>(liveRow?.context_timeframes) : ["5m"],
          runtime_instance_present: liveRow?.runtime_instance_present === true || trackedDetail.runtime_attached === true,
          runtime_state_loaded: liveRow?.runtime_state_loaded === true,
          can_process_bars: liveRow?.can_process_bars === true,
          audit_only: liveRow ? false : true,
          snapshot_only: liveRow ? false : true,
          truth_label: liveRow ? "LIVE_SHARED_PAPER_ROSTER" : "CONFIGURED_ATP_UNIVERSE",
          current_strategy_status: String(
            liveRow?.current_strategy_status
              ?? liveRow?.strategy_status
              ?? trackedDetail.status
              ?? (meta.designation === "benchmark" ? "AUDIT ONLY" : "CONFIGURED / NOT LOADED"),
          ),
          status: String(
            liveRow?.status
              ?? trackedDetail.status
              ?? (meta.designation === "benchmark" ? "AUDIT_ONLY" : "NOT_LOADED"),
          ),
          status_reason: String(
            liveRow?.status_reason
              ?? trackedDetail.status_reason
              ?? (meta.designation === "benchmark"
                ? "ATP benchmark is present through tracked/audit truth even when not surfaced as a live shared-paper lane row."
                : "ATP experimental strategy is configured in the platform universe but is not currently attached to the live paper runtime."),
          ),
          total_quantity: Number(liveRow?.total_quantity ?? 0),
          open_entry_leg_count: Number(liveRow?.open_entry_leg_count ?? 0),
          open_add_count: Number(liveRow?.open_add_count ?? 0),
          additional_entry_allowed: liveRow?.additional_entry_allowed ?? false,
          net_side: String(liveRow?.net_side ?? "FLAT"),
          last_execution_bar_evaluated_at: String(liveRow?.last_execution_bar_evaluated_at ?? trackedDetail.runtime_heartbeat_at ?? ""),
          last_completed_context_bars_at: liveRow?.last_completed_context_bars_at ?? {},
          config_source: String(
            liveRow?.config_source
              ?? trackedDetail?.config_identity?.config_source
              ?? trackedDetail?.config_identity?.benchmark_overlay_config
              ?? "ATP product catalog",
          ),
        };
        roster.set(meta.laneId, mergedRow);
      }
      return [...roster.values()].sort((left, right) => {
        const leftAtp = isAtpRow(left) ? 0 : 1;
        const rightAtp = isAtpRow(right) ? 0 : 1;
        if (leftAtp !== rightAtp) {
          return leftAtp - rightAtp;
        }
        const leftBench = left.benchmark_designation ? 0 : left.candidate_designation ? 1 : 2;
        const rightBench = right.benchmark_designation ? 0 : right.candidate_designation ? 1 : 2;
        if (leftBench !== rightBench) {
          return leftBench - rightBench;
        }
        return String(left.branch ?? left.display_name ?? "").localeCompare(String(right.branch ?? right.display_name ?? ""));
      });
    },
    [approvedModelDetailsByBranch, approvedModelRows, runtimeRegistryRows, temporaryPaperStrategyRows, trackedStrategyRows, trackedStrategyDetailsById],
  );
  useEffect(() => {
    if (!dashboardRosterRows.length) {
      return;
    }
    if (!selectedWorkspaceLaneId || !dashboardRosterRows.some((row) => String(row.lane_id ?? "") === selectedWorkspaceLaneId)) {
      setSelectedWorkspaceLaneId(String(dashboardRosterRows[0]?.lane_id ?? ""));
    }
  }, [dashboardRosterRows, selectedWorkspaceLaneId]);
  const selectedWorkspaceRow = useMemo(
    () => dashboardRosterRows.find((row) => String(row.lane_id ?? "") === selectedWorkspaceLaneId) ?? dashboardRosterRows[0] ?? null,
    [dashboardRosterRows, selectedWorkspaceLaneId],
  );
  useEffect(() => {
    if (calendarAutoRangeApplied || !playback.available || !earliestPlaybackCoverageDate) {
      return;
    }
    if (earliestPlaybackCoverageDate < startOfMonth(new Date().toISOString().slice(0, 10))) {
      setCalendarPeriod("ytd");
    }
    setCalendarAutoRangeApplied(true);
  }, [calendarAutoRangeApplied, earliestPlaybackCoverageDate, playback.available]);
  const selectedWorkspaceInstrument = String(selectedWorkspaceRow?.instrument ?? "").trim().toUpperCase();
  const selectedWorkspacePerformanceRow = useMemo(
    () =>
      strategyPerformanceRows.find(
        (row) =>
          String(row.lane_id ?? "") === String(selectedWorkspaceRow?.lane_id ?? "")
          || String(row.standalone_strategy_id ?? "") === String(selectedWorkspaceRow?.tracked_strategy_id ?? ""),
      ) ?? null,
    [selectedWorkspaceRow, strategyPerformanceRows],
  );
  const selectedWorkspaceTrades = useMemo(
    () =>
      closedStrategyTradeRows
        .filter((row) => String(row.lane_id ?? "") === String(selectedWorkspaceRow?.lane_id ?? ""))
        .slice(0, 12),
    [closedStrategyTradeRows, selectedWorkspaceRow],
  );
  const selectedWorkspacePlaybackStudyItem = useMemo(() => {
    if (!playbackLatestStudyItems.length) {
      return null;
    }
    const exactStrategyIds = [
      String(selectedWorkspaceRow?.tracked_strategy_id ?? "").trim(),
      String(selectedWorkspaceRow?.standalone_strategy_id ?? "").trim(),
    ].filter(Boolean);
    const exactMatch = playbackLatestStudyItems.find((item) => {
      return exactStrategyIds.includes(String(item.strategy_id ?? "").trim());
    });
    if (exactMatch) {
      return exactMatch;
    }
    const preferredMode = selectedWorkspaceRow?.candidate_designation ? "research_execution_mode" : "baseline_parity_mode";
    const instrumentMatches = playbackLatestStudyItems
      .filter((item) => {
        return (
          String(item.symbol ?? "").trim().toUpperCase() === selectedWorkspaceInstrument
          && String(item.study_mode ?? "").trim() === preferredMode
        );
      })
      .sort((left, right) => {
        const leftSpan = Math.max(
          0,
          parseTimestampMs(String(left.coverage_end ?? "")) - parseTimestampMs(String(left.coverage_start ?? "")),
        );
        const rightSpan = Math.max(
          0,
          parseTimestampMs(String(right.coverage_end ?? "")) - parseTimestampMs(String(right.coverage_start ?? "")),
        );
        if (rightSpan !== leftSpan) {
          return rightSpan - leftSpan;
        }
        const leftTrades = asArray<JsonRecord>(asRecord(left.summary).closed_trade_breakdown).length;
        const rightTrades = asArray<JsonRecord>(asRecord(right.summary).closed_trade_breakdown).length;
        return rightTrades - leftTrades;
      });
    if (instrumentMatches.length) {
      return instrumentMatches[0];
    }
    const resultsBoard = asRecord(strategyAnalysis.results_board);
    const boardRows = asArray<JsonRecord>(resultsBoard.rows);
    const defaultRowId = String(resultsBoard.default_row_id ?? "").trim();
    const preferredReplayRow =
      boardRows.find((row) => String(row.lane_id ?? row.id ?? "") === defaultRowId)
      ?? boardRows.find((row) => String(row.source_lane ?? "").trim() === "historical_playback")
      ?? null;
    const preferredStudyKey = String(asRecord(asRecord(preferredReplayRow?.evidence).bars).ref?.study_key ?? "").trim();
    if (preferredStudyKey) {
      return playbackLatestStudyItems.find((item) => String(item.study_key ?? "").trim() === preferredStudyKey) ?? playbackLatestStudyItems[0];
    }
    return playbackLatestStudyItems[0];
  }, [playbackLatestStudyItems, selectedWorkspaceInstrument, selectedWorkspaceRow, strategyAnalysis.results_board]);
  const selectedWorkspacePlaybackSummary = asRecord(selectedWorkspacePlaybackStudyItem?.summary);
  const selectedWorkspacePlaybackCoverage = useMemo(
    () => ({
      start_timestamp: selectedWorkspacePlaybackStudyItem?.coverage_start ?? null,
      end_timestamp: selectedWorkspacePlaybackStudyItem?.coverage_end ?? null,
    }),
    [selectedWorkspacePlaybackStudyItem],
  );
  const [selectedWorkspacePlaybackStudyLoaded, setSelectedWorkspacePlaybackStudyLoaded] = useState<JsonRecord | null>(null);
  useEffect(() => {
    const artifactTarget = String(asRecord(selectedWorkspacePlaybackStudyItem?.artifact_paths).strategy_study_json ?? "").trim();
    const backendUrl = String(desktopState?.backendUrl ?? "").trim();
    if (!artifactTarget || !backendUrl) {
      setSelectedWorkspacePlaybackStudyLoaded(null);
      return;
    }
    let cancelled = false;
    const artifactUrl = artifactTarget.startsWith("/api/")
      ? new URL(artifactTarget.replace(/^\//, ""), backendUrl).toString()
      : artifactTarget;
    void fetch(artifactUrl)
      .then((response) => {
        if (!response.ok) {
          throw new Error(`Workspace playback study fetch failed (${response.status})`);
        }
        return response.json() as Promise<JsonRecord>;
      })
      .then((payload) => {
        if (!cancelled) {
          setSelectedWorkspacePlaybackStudyLoaded(asRecord(payload));
        }
      })
      .catch(() => {
        if (!cancelled) {
          setSelectedWorkspacePlaybackStudyLoaded(null);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [desktopState?.backendUrl, selectedWorkspacePlaybackStudyItem]);
  const selectedWorkspacePlaybackStudy = selectedWorkspacePlaybackStudyLoaded ?? asRecord(selectedWorkspacePlaybackStudyItem?.study_preview);
  const selectedWorkspacePlaybackStudyRows = asArray<JsonRecord>(
    selectedWorkspacePlaybackStudy.bars ?? selectedWorkspacePlaybackStudy.rows,
  );
  const selectedWorkspacePlaybackCanRenderStudy =
    Boolean(selectedWorkspacePlaybackStudyItem) && selectedWorkspacePlaybackStudyRows.length > 0;
  const selectedWorkspaceEquityCurve = useMemo(() => {
    let running = 0;
    const ordered = [...selectedWorkspaceTrades].sort(
      (left, right) => parseTimestampMs(left.exit_timestamp ?? left.entry_timestamp) - parseTimestampMs(right.exit_timestamp ?? right.entry_timestamp),
    );
    return ordered.map((row) => {
      running += Number(row.realized_pnl ?? 0) || 0;
      return {
        timestamp: String(row.exit_timestamp ?? row.entry_timestamp ?? ""),
        value: running,
      };
    });
  }, [selectedWorkspaceTrades]);
  const atpStrategyRows = useMemo(
    () =>
      [...runtimeRegistryRows, ...approvedModelRows, ...temporaryPaperStrategyRows].filter((row, index, rows) => {
        if (!isAtpRow(row)) {
          return false;
        }
        const key = String(row.standalone_strategy_id ?? row.lane_id ?? row.branch ?? index);
        return rows.findIndex((candidate) => String(candidate.standalone_strategy_id ?? candidate.lane_id ?? candidate.branch ?? "") === key) === index;
      }),
    [runtimeRegistryRows, approvedModelRows, temporaryPaperStrategyRows],
  );
  const atpBenchmarkRows = useMemo(
    () => atpStrategyRows.filter((row) => Boolean(row.benchmark_designation) || /benchmark/i.test(String(row.designation_label ?? row.strategy_status ?? ""))),
    [atpStrategyRows],
  );
  const atpCandidateRows = useMemo(
    () => atpStrategyRows.filter((row) => !atpBenchmarkRows.includes(row)),
    [atpBenchmarkRows, atpStrategyRows],
  );
  const selectWorkspaceLane = useCallback(
    (laneId: string, options?: { navigateTo?: PageId; syncTradeEntry?: boolean }) => {
      const nextRow =
        dashboardRosterRows.find((row) => String(row.lane_id ?? "") === String(laneId))
        ?? dashboardRosterRows[0]
        ?? null;
      const nextLaneId = String(nextRow?.lane_id ?? laneId ?? "");
      const nextInstrument = String(nextRow?.instrument ?? "").trim().toUpperCase();
      setSelectedWorkspaceLaneId(nextLaneId);
      if (options?.syncTradeEntry !== false && nextInstrument) {
        setManualOrderForm((current) => ({ ...current, symbol: nextInstrument }));
        const matchingProductionPosition = productionPositions.find(
          (row) => String(row.symbol ?? "").trim().toUpperCase() === nextInstrument,
        );
        if (matchingProductionPosition?.position_key) {
          setSelectedProductionPositionKey(String(matchingProductionPosition.position_key));
        }
      }
      if (options?.navigateTo) {
        window.location.hash = `#/${options.navigateTo}`;
        setPage(options.navigateTo);
      }
    },
    [dashboardRosterRows, productionPositions],
  );
  const openCalendarContributionStrategy = useCallback(
    (contribution: CalendarStrategyContribution) => {
      const matched =
        dashboardRosterRows.find((row) => String(row.lane_id ?? "") === String(contribution.laneId ?? ""))
        ?? dashboardRosterRows.find((row) => String(row.standalone_strategy_id ?? row.tracked_strategy_id ?? "") === String(contribution.strategyId))
        ?? dashboardRosterRows.find((row) => String(row.display_name ?? row.branch ?? "").trim() === contribution.strategyName.trim())
        ?? null;
      if (!matched) {
        return;
      }
      setCalendarContextLabel(`${selectedCalendarDay ?? "Selected Day"} • ${calendarSourceLabel(calendarSource)}`);
      selectWorkspaceLane(String(matched.lane_id ?? ""), { navigateTo: "strategies" });
    },
    [calendarSource, dashboardRosterRows, selectWorkspaceLane, selectedCalendarDay],
  );
  const tradeEntrySymbol = (manualOrderForm.symbol.trim() || selectedWorkspaceInstrument || selectedProductionSymbol || "").toUpperCase();
  const tradeEntryQuoteRow = useMemo(
    () => productionQuoteRows.find((row) => String(row.symbol ?? row.instrument ?? "").trim().toUpperCase() === tradeEntrySymbol) ?? null,
    [productionQuoteRows, tradeEntrySymbol],
  );
  const headerQuoteRow = tradeEntryQuoteRow ?? productionQuoteRows[0] ?? null;
  const headerSymbol = String(
    tradeEntrySymbol
    || headerQuoteRow?.symbol
    || headerQuoteRow?.instrument
    || selectedWorkspaceRow?.instrument
    || "MGC",
  ).toUpperCase();
  const headerLastPrice = headerQuoteRow?.last_price ?? headerQuoteRow?.mark ?? headerQuoteRow?.price ?? "—";
  const headerNetChange = headerQuoteRow?.net_change ?? headerQuoteRow?.quoteTrend ?? "No live quote";
  const headerNetChangeTone = statusTone(headerQuoteRow?.net_change ?? headerQuoteRow?.quoteTrend);
  const selectedWorkspaceDesignation = designationLabel(selectedWorkspaceRow);
  const selectedWorkspaceRuntimeLabel = runtimeAttachmentLabel(selectedWorkspaceRow);
  const selectedWorkspaceContextTimes = asArray<string>(selectedWorkspaceRow?.context_timeframes).join(" / ") || "5m";
  const tradeEntryPaperRows = useMemo(
    () => {
      const matched = strategyPerformanceRows.filter(
        (row) => String(row.instrument ?? row.symbol ?? "").trim().toUpperCase() === tradeEntrySymbol,
      );
      const workspaceMatchesSymbol =
        Boolean(tradeEntrySymbol)
        && String(selectedWorkspaceRow?.instrument ?? "").trim().toUpperCase() === tradeEntrySymbol;
      if (
        workspaceMatchesSymbol
        && selectedWorkspaceRow
        && !matched.some((row) => String(row.lane_id ?? row.standalone_strategy_id ?? "") === String(selectedWorkspaceRow.lane_id ?? selectedWorkspaceRow.standalone_strategy_id ?? ""))
      ) {
        return [selectedWorkspaceRow, ...matched];
      }
      return matched;
    },
    [selectedWorkspaceRow, strategyPerformanceRows, tradeEntrySymbol],
  );
  const paperIntradayCurveRows = useMemo(() => {
    const today = new Date().toISOString().slice(0, 10);
    const rows = closedStrategyTradeRows
      .filter((row) => String(row.exit_timestamp ?? row.entry_timestamp ?? "").slice(0, 10) === today)
      .sort((left, right) => parseTimestampMs(left.exit_timestamp ?? left.entry_timestamp) - parseTimestampMs(right.exit_timestamp ?? right.entry_timestamp));
    let running = 0;
    return rows.map((row) => {
      running += Number(row.realized_pnl ?? 0) || 0;
      return {
        timestamp: String(row.exit_timestamp ?? row.entry_timestamp ?? ""),
        value: running,
      };
    });
  }, [closedStrategyTradeRows]);
  const liveIntradayCurveRows = useMemo(() => {
    const fills = [...productionRecentFills]
      .sort((left, right) => parseTimestampMs(left.updated_at ?? left.occurred_at) - parseTimestampMs(right.updated_at ?? right.occurred_at));
    if (!fills.length) {
      return productionTotals.total_current_day_pnl == null
        ? []
        : [{ timestamp: desktopState?.refreshedAt ?? new Date().toISOString(), value: Number(productionTotals.total_current_day_pnl ?? 0) || 0 }];
    }
    const currentValue = Number(productionTotals.total_current_day_pnl ?? 0) || 0;
    return fills.map((row, index) => ({
      timestamp: String(row.updated_at ?? row.occurred_at ?? desktopState?.refreshedAt ?? new Date().toISOString()),
      value: currentValue * ((index + 1) / fills.length),
    }));
  }, [desktopState?.refreshedAt, productionRecentFills, productionTotals.total_current_day_pnl]);
  const paperCalendarEntries = useMemo<CalendarSourceEntry[]>(
    () => {
      const entries: CalendarSourceEntry[] = [];
      for (const row of closedStrategyTradeRows) {
          const date = dateKeyFromTimestamp(row.exit_timestamp ?? row.entry_timestamp);
          const pnl = numericOrNull(row.realized_pnl ?? row.gross_pnl);
          if (!date || pnl === null) {
            continue;
          }
          entries.push({
            source: "paper",
            date,
            laneId: String(row.lane_id ?? "") || null,
            strategyId: String(row.standalone_strategy_id ?? row.strategy_key ?? row.lane_id ?? row.instrument ?? "paper_lane"),
            strategyName: String(row.strategy_name ?? row.standalone_strategy_label ?? row.lane_id ?? row.instrument ?? "Paper Lane"),
            pnl,
            tradeCount: Math.max(1, numericOrNull(row.trade_count) ?? 1),
          });
      }
      return entries;
    },
    [closedStrategyTradeRows],
  );
  const liveCalendarEntries = useMemo<CalendarSourceEntry[]>(
    () => {
      const entries: CalendarSourceEntry[] = [];
      for (const row of productionRecentFills) {
          const date = dateKeyFromTimestamp(row.updated_at ?? row.occurred_at ?? row.fill_timestamp);
          const pnl = numericOrNull(row.realized_pnl ?? row.fill_pnl ?? row.pnl ?? row.net_pnl ?? row.profit_loss);
          if (!date || pnl === null) {
            continue;
          }
          entries.push({
            source: "live",
            date,
            laneId: null,
            strategyId: String(row.strategy_id ?? row.strategy_tag ?? row.symbol ?? row.instrument ?? "live_fill"),
            strategyName: String(row.strategy_name ?? row.strategy_tag ?? row.symbol ?? row.instrument ?? "Live Activity"),
            pnl,
            tradeCount: Math.max(1, numericOrNull(row.trade_count ?? row.filled_quantity ?? row.quantity) ?? 1),
          });
      }
      return entries;
    },
    [productionRecentFills],
  );
  const benchmarkReplayCalendarEntries = useMemo<CalendarSourceEntry[]>(
    () => {
      const entries: CalendarSourceEntry[] = [];
      for (const item of playbackLatestStudyItems) {
        const summary = asRecord(item.summary);
        const studyMode = String(item.study_mode ?? "").trim();
        if (studyMode !== "baseline_parity_mode") {
          continue;
        }
        for (const trade of asArray<JsonRecord>(summary.closed_trade_breakdown)) {
          const date = dateKeyFromTimestamp(trade.exit_timestamp ?? trade.entry_timestamp);
          const pnl = numericOrNull(trade.realized_pnl);
          if (!date || pnl === null) {
            continue;
          }
          entries.push({
            source: "benchmark_replay",
            date,
            laneId: String(item.study_key ?? "") || null,
            strategyId: String(item.strategy_id ?? item.study_key ?? item.symbol ?? "benchmark_replay"),
            strategyName: String(item.label ?? item.strategy_id ?? item.symbol ?? "Benchmark Replay"),
            pnl,
            tradeCount: 1,
          });
        }
      }
      return entries;
    },
    [playbackLatestStudyItems],
  );
  const researchExecutionCalendarEntries = useMemo<CalendarSourceEntry[]>(
    () => {
      const entries: CalendarSourceEntry[] = [];
      for (const item of playbackLatestStudyItems) {
        const summary = asRecord(item.summary);
        const studyMode = String(item.study_mode ?? "").trim();
        if (studyMode !== "research_execution_mode") {
          continue;
        }
        for (const trade of asArray<JsonRecord>(summary.closed_trade_breakdown)) {
          const date = dateKeyFromTimestamp(trade.exit_timestamp ?? trade.entry_timestamp);
          const pnl = numericOrNull(trade.realized_pnl);
          if (!date || pnl === null) {
            continue;
          }
          entries.push({
            source: "research_execution",
            date,
            laneId: String(item.study_key ?? "") || null,
            strategyId: String(item.strategy_id ?? item.study_key ?? item.symbol ?? "research_execution"),
            strategyName: String(item.label ?? item.strategy_id ?? item.symbol ?? "Research Execution"),
            pnl,
            tradeCount: 1,
          });
        }
      }
      return entries;
    },
    [playbackLatestStudyItems],
  );
  const calendarEntriesBySource = useMemo(
    () => ({
      live: liveCalendarEntries,
      paper: paperCalendarEntries,
      benchmark_replay: benchmarkReplayCalendarEntries,
      research_execution: researchExecutionCalendarEntries,
    }),
    [benchmarkReplayCalendarEntries, liveCalendarEntries, paperCalendarEntries, researchExecutionCalendarEntries],
  );
  const calendarAvailableSources = useMemo(
    () =>
      (Object.entries(calendarEntriesBySource) as Array<[Exclude<PnlCalendarSource, "all">, CalendarSourceEntry[]]>)
        .filter(([, entries]) => entries.length > 0)
        .map(([source]) => source),
    [calendarEntriesBySource],
  );
  const calendarSourceSelection = useMemo(() => {
    if (calendarSource === "all") {
      const includedSources = calendarAvailableSources.length ? calendarAvailableSources : ["paper"];
      const uniqueSources = [...new Set(includedSources)];
      const mergedEntries = uniqueSources.flatMap((source) => calendarEntriesBySource[source as Exclude<PnlCalendarSource, "all">] ?? []);
      return {
        selectedSourceLabel: "All Accounts",
        includedSources: uniqueSources,
        entries: mergedEntries,
        note:
          uniqueSources.length > 1
            ? `Combined only across provenance-safe daily streams: ${uniqueSources.map((source) => source.replace(/_/g, " ")).join(" + ")}.`
            : `Only ${uniqueSources[0].replace(/_/g, " ")} currently exposes closed-trade daily history in the loaded workstation snapshot.`,
      };
    }
    const entries = calendarEntriesBySource[calendarSource] ?? [];
      return {
        selectedSourceLabel: calendarSourceLabel(calendarSource),
        includedSources: [calendarSource],
        entries,
      note:
        entries.length > 0
          ? `${calendarSource === "paper" ? "Persisted paper/runtime trade ledger." : calendarSource === "live" ? "Broker/live closed-fill stream." : calendarSource === "benchmark_replay" ? "Replay/backtest artifact stream." : "Research execution artifact stream."}`
          : `${calendarSource === "benchmark_replay" ? "Replay" : calendarSource === "research_execution" ? "Research-execution" : sentenceCase(calendarSource)} daily history is not loaded in the current workstation snapshot.`,
    };
  }, [calendarAvailableSources, calendarEntriesBySource, calendarSource]);
  const effectiveCalendarViewMode = calendarPeriod === "ytd" ? "line" : calendarViewMode;
  const calendarRange = useMemo(() => {
    const today = new Date().toISOString().slice(0, 10);
    if (calendarPeriod === "weekly") {
      const start = startOfWeek(calendarAnchorDate);
      return { start, end: addDays(start, 4) };
    }
    if (calendarPeriod === "quarterly") {
      return { start: startOfQuarter(calendarAnchorDate), end: endOfQuarter(calendarAnchorDate) };
    }
    if (calendarPeriod === "ytd") {
      return { start: startOfYear(today), end: today };
    }
    if (calendarPeriod === "custom") {
      return {
        start: calendarCustomStart <= calendarCustomEnd ? calendarCustomStart : calendarCustomEnd,
        end: calendarCustomEnd >= calendarCustomStart ? calendarCustomEnd : calendarCustomStart,
      };
    }
    return { start: startOfMonth(calendarAnchorDate), end: endOfMonth(calendarAnchorDate) };
  }, [calendarAnchorDate, calendarCustomEnd, calendarCustomStart, calendarPeriod]);
  const calendarPeriodTitle = useMemo(
    () => sourcePeriodLabel(calendarPeriod, calendarAnchorDate, calendarRange.start, calendarRange.end),
    [calendarAnchorDate, calendarPeriod, calendarRange.end, calendarRange.start],
  );
  const calendarDayPoints = useMemo<CalendarDayPoint[]>(() => {
    const grouped = new Map<string, { pnl: number; tradeCount: number; contributions: Map<string, CalendarStrategyContribution>; coveredSources: Set<Exclude<PnlCalendarSource, "all" | "live" | "paper">> }>();
    const coverageDates = new Set<string>();
    for (const source of calendarSourceSelection.includedSources) {
      if (source === "benchmark_replay" || source === "research_execution") {
        for (const dateKey of playbackCoverageDateKeysBySource[source]) {
          if (dateKey >= calendarRange.start && dateKey <= calendarRange.end) {
            coverageDates.add(dateKey);
            const existing = grouped.get(dateKey) ?? { pnl: 0, tradeCount: 0, contributions: new Map(), coveredSources: new Set() };
            existing.coveredSources.add(source);
            grouped.set(dateKey, existing);
          }
        }
      }
    }
    const filteredEntries = calendarSourceSelection.entries
      .filter((entry) => entry.date >= calendarRange.start && entry.date <= calendarRange.end)
      .sort((left, right) => left.date.localeCompare(right.date));
    for (const entry of filteredEntries) {
      const day = grouped.get(entry.date) ?? { pnl: 0, tradeCount: 0, contributions: new Map(), coveredSources: new Set() };
      day.pnl += entry.pnl;
      day.tradeCount += entry.tradeCount;
      const contributionKey = `${entry.source}:${entry.strategyId}:${entry.laneId ?? ""}`;
      const existing = day.contributions.get(contributionKey) ?? {
        source: entry.source,
        laneId: entry.laneId,
        strategyId: entry.strategyId,
        strategyName: entry.strategyName,
        pnl: 0,
        tradeCount: 0,
      };
      existing.pnl += entry.pnl;
      existing.tradeCount += entry.tradeCount;
      day.contributions.set(contributionKey, existing);
      grouped.set(entry.date, day);
    }
    for (const dateKey of coverageDates) {
      if (!grouped.has(dateKey)) {
        grouped.set(dateKey, { pnl: 0, tradeCount: 0, contributions: new Map(), coveredSources: new Set() });
      }
    }
    let running = 0;
    return [...grouped.entries()]
      .sort((left, right) => left[0].localeCompare(right[0]))
      .map(([date, payload]) => {
        running += payload.pnl;
        return {
          date,
          pnl: payload.pnl,
          tradeCount: payload.tradeCount,
          cumulative: running,
          contributions: [...payload.contributions.values()].sort((left, right) => Math.abs(right.pnl) - Math.abs(left.pnl)),
          coveredSources: [...payload.coveredSources.values()].sort(),
        };
      });
  }, [calendarRange.end, calendarRange.start, calendarSourceSelection.entries, calendarSourceSelection.includedSources, playbackCoverageDateKeysBySource]);
  const calendarDayPointMap = useMemo(() => new Map(calendarDayPoints.map((point) => [point.date, point])), [calendarDayPoints]);
  const selectedCalendarDayPoint = useMemo(
    () => (selectedCalendarDay ? calendarDayPointMap.get(selectedCalendarDay) ?? null : null),
    [calendarDayPointMap, selectedCalendarDay],
  );
  const calendarDailyPnls = useMemo(() => calendarDayPoints.map((point) => point.pnl), [calendarDayPoints]);
  const calendarGrossPnl = useMemo(() => calendarDailyPnls.reduce((sum, value) => sum + value, 0), [calendarDailyPnls]);
  const calendarWinningDays = useMemo(() => calendarDailyPnls.filter((value) => value > 0).length, [calendarDailyPnls]);
  const calendarLosingDays = useMemo(() => calendarDailyPnls.filter((value) => value < 0).length, [calendarDailyPnls]);
  const calendarProfitFactor = useMemo(() => {
    const grossWins = calendarDailyPnls.filter((value) => value > 0).reduce((sum, value) => sum + value, 0);
    const grossLosses = Math.abs(calendarDailyPnls.filter((value) => value < 0).reduce((sum, value) => sum + value, 0));
    if (grossWins <= 0 || grossLosses <= 0) {
      return grossWins > 0 && grossLosses === 0 ? grossWins : null;
    }
    return grossWins / grossLosses;
  }, [calendarDailyPnls]);
  const calendarSharpe = useMemo(() => {
    if (calendarDailyPnls.length < 2) {
      return null;
    }
    const mean = calendarDailyPnls.reduce((sum, value) => sum + value, 0) / calendarDailyPnls.length;
    const variance =
      calendarDailyPnls.reduce((sum, value) => sum + ((value - mean) ** 2), 0) / Math.max(calendarDailyPnls.length - 1, 1);
    const stdDev = Math.sqrt(variance);
    if (!Number.isFinite(stdDev) || stdDev === 0) {
      return null;
    }
    return (mean / stdDev) * Math.sqrt(252);
  }, [calendarDailyPnls]);
  const calendarMaxDrawdown = useMemo(() => {
    let peak = 0;
    let maxDrawdown = 0;
    let cumulative = 0;
    for (const value of calendarDailyPnls) {
      cumulative += value;
      if (cumulative > peak) {
        peak = cumulative;
      }
      const drawdown = peak - cumulative;
      if (drawdown > maxDrawdown) {
        maxDrawdown = drawdown;
      }
    }
    return maxDrawdown === 0 ? 0 : -maxDrawdown;
  }, [calendarDailyPnls]);
  const calendarPortfolioBase = useMemo(() => {
    if (calendarSource === "live") {
      return numericOrNull(productionBalances.liquidation_value ?? productionBalances.cash_balance ?? productionTotals.buying_power);
    }
    if (calendarSource === "paper" || calendarSource === "all") {
      return numericOrNull(
        asRecord(paperStrategyPerformance.portfolio_snapshot).starting_equity
        ?? asRecord(paperStrategyPerformance.portfolio_snapshot).account_basis
        ?? asRecord(paperStrategyPerformance.portfolio_snapshot).starting_balance,
      );
    }
    return null;
  }, [
    calendarSource,
    paperStrategyPerformance.portfolio_snapshot,
    productionBalances.cash_balance,
    productionBalances.liquidation_value,
    productionTotals.buying_power,
  ]);
  const calendarPortfolioReturn = calendarPortfolioBase && calendarPortfolioBase !== 0 ? (calendarGrossPnl / calendarPortfolioBase) * 100 : null;
  const calendarWinRate = calendarDayPoints.length ? (calendarWinningDays / calendarDayPoints.length) * 100 : null;
  const calendarAvgDailyPnl = calendarDayPoints.length ? calendarGrossPnl / calendarDayPoints.length : null;
  const calendarTotalTrades = useMemo(
    () => calendarDayPoints.reduce((sum, point) => sum + point.tradeCount, 0),
    [calendarDayPoints],
  );
  const calendarStreaks = useMemo(() => {
    let bestWin = 0;
    let maxLoss = 0;
    let currentWins = 0;
    let currentLosses = 0;
    for (const value of calendarDailyPnls) {
      if (value > 0) {
        currentWins += 1;
        currentLosses = 0;
      } else if (value < 0) {
        currentLosses += 1;
        currentWins = 0;
      } else {
        currentWins = 0;
        currentLosses = 0;
      }
      bestWin = Math.max(bestWin, currentWins);
      maxLoss = Math.max(maxLoss, currentLosses);
    }
    return { bestWin, maxLoss };
  }, [calendarDailyPnls]);
  const calendarAlertRows = useMemo(() => {
    const alerts: Array<{ label: string; note: string; tone: Tone }> = [];
    if (calendarDayPoints.length) {
      const latest = calendarDayPoints[calendarDayPoints.length - 1] ?? null;
      if (latest && latest.cumulative >= Math.max(...calendarDayPoints.map((point) => point.cumulative))) {
        alerts.push({ label: "High-Water Mark", note: `${formatCompactCurrency(latest.cumulative)} cumulative through ${latest.date}.`, tone: "good" });
      }
    }
    if (calendarMaxDrawdown < -100) {
      alerts.push({ label: "Drawdown Watch", note: `${formatCompactCurrency(calendarMaxDrawdown)} peak-to-trough drawdown in the selected period.`, tone: "danger" });
    }
    if (calendarStreaks.maxLoss >= 3) {
      alerts.push({ label: "Loss Streak", note: `${calendarStreaks.maxLoss} consecutive losing days in the selected range.`, tone: "warn" });
    }
    if (calendarStreaks.bestWin >= 3) {
      alerts.push({ label: "Win Streak", note: `${calendarStreaks.bestWin} consecutive winning days in the selected range.`, tone: "good" });
    }
    return alerts.slice(0, 3);
  }, [calendarDayPoints, calendarMaxDrawdown, calendarStreaks.bestWin, calendarStreaks.maxLoss]);
  const calendarKpis = useMemo(
    () => [
      { label: "Gross P&L", value: formatCompactCurrency(calendarGrossPnl), tone: pnlTone(calendarGrossPnl) },
      { label: "Portfolio Return", value: formatPercentValue(calendarPortfolioReturn, 2), tone: pnlTone(calendarPortfolioReturn) },
      { label: "Sharpe Ratio", value: formatRatioValue(calendarSharpe), tone: sharpeTone(calendarSharpe) },
      { label: "Max Drawdown", value: formatCompactCurrency(calendarMaxDrawdown), tone: "danger" as Tone },
      { label: "Win Rate", value: formatPercentValue(calendarWinRate), tone: winRateTone(calendarWinRate) },
      { label: "Profit Factor", value: formatRatioValue(calendarProfitFactor), tone: sharpeTone(calendarProfitFactor) },
      { label: "Avg Daily P&L", value: formatCompactCurrency(calendarAvgDailyPnl), tone: pnlTone(calendarAvgDailyPnl) },
      { label: "Total Trades", value: formatShortNumber(calendarTotalTrades), tone: "muted" as Tone },
      { label: "Best Win Streak", value: formatShortNumber(calendarStreaks.bestWin), tone: "good" as Tone },
      { label: "Max Loss Streak", value: formatShortNumber(calendarStreaks.maxLoss), tone: "danger" as Tone },
    ],
    [calendarAvgDailyPnl, calendarGrossPnl, calendarMaxDrawdown, calendarPortfolioReturn, calendarProfitFactor, calendarSharpe, calendarStreaks.bestWin, calendarStreaks.maxLoss, calendarTotalTrades, calendarWinRate],
  );
  const calendarGridDays = useMemo(() => {
    if (calendarPeriod === "weekly") {
      const start = startOfWeek(calendarAnchorDate);
      return Array.from({ length: 5 }, (_, index) => addDays(start, index));
    }
    if (calendarPeriod === "quarterly") {
      const start = startOfQuarter(calendarAnchorDate);
      const end = endOfQuarter(calendarAnchorDate);
      return Array.from({ length: daysBetween(start, end) + 1 }, (_, index) => addDays(start, index));
    }
    const monthStart = startOfMonth(calendarAnchorDate);
    const monthEnd = endOfMonth(calendarAnchorDate);
    const leadingOffset = dateFromKey(monthStart).getDay();
    const firstVisible = addDays(monthStart, -leadingOffset);
    return Array.from({ length: 42 }, (_, index) => addDays(firstVisible, index)).filter((date) => {
      if (calendarPeriod === "monthly") {
        return true;
      }
      return date >= calendarRange.start && date <= calendarRange.end;
    });
  }, [calendarAnchorDate, calendarPeriod, calendarRange.end, calendarRange.start]);

  useEffect(() => {
    if (!calendarDayPoints.length) {
      setSelectedCalendarDay(null);
      return;
    }
    if (!selectedCalendarDay || !calendarDayPointMap.has(selectedCalendarDay)) {
      setSelectedCalendarDay(calendarDayPoints[calendarDayPoints.length - 1]?.date ?? null);
    }
  }, [calendarDayPointMap, calendarDayPoints, selectedCalendarDay]);

  useEffect(() => {
    if (page !== "calendar" && page !== "strategies" && calendarContextLabel) {
      setCalendarContextLabel(null);
    }
  }, [calendarContextLabel, page]);

  const primaryNavItems = NAV_ITEMS.filter((item) => ["home", "calendar", "positions", "market", "strategies"].includes(item.id));
  const utilityNavItems = NAV_ITEMS.filter((item) => !primaryNavItems.includes(item));
  const processControlCards = [
    {
      label: "Dashboard/API",
      status: desktopState?.backend.label ?? "Unknown",
      tone: statusTone(desktopState?.backend.label),
      onClick: () => void runCommand("restart-dashboard", () => api.restartDashboard()),
      disabled: busyAction !== null,
    },
    {
      label: "Paper Runtime",
      status: formatValue(paperReadiness.runtime_phase ?? "STOPPED"),
      tone: statusTone(paperReadiness.runtime_phase),
      onClick: () => void runCommand("restart-paper-with-temp-paper", () => api.runDashboardAction("restart-paper-with-temp-paper"), {
        confirmMessage: "Restart Runtime + Temp Paper will restart the paper soak with all enabled temporary paper lanes included. Proceed?",
        requiresLive: true,
      }),
      disabled: busyAction !== null || !canRunLiveActions,
    },
    {
      label: "Auth Gate",
      status: authReadyForPaperStartup ? "READY" : "BLOCKED",
      tone: authReadyForPaperStartup ? "good" : "warn",
      onClick: () => void runCommand("auth-gate-check", () => api.runDashboardAction("auth-gate-check"), { requiresLive: true }),
      disabled: busyAction !== null || !canRunLiveActions,
    },
    {
      label: "Entries",
      status: formatValue(global.entries_enabled ?? paperReadiness.entries_enabled),
      tone: statusTone(global.entries_enabled ?? paperReadiness.entries_enabled),
      onClick: () => void runCommand("paper-resume-entries", () => api.runDashboardAction("paper-resume-entries"), { requiresLive: true }),
      disabled: busyAction !== null || !canRunLiveActions,
    },
    {
      label: "Production Link",
      status: formatValue(productionLink.label ?? productionLink.status ?? "Unavailable"),
      tone: statusTone(productionLink.label ?? productionLink.status),
      onClick: () => {
        window.location.hash = "#/positions";
        setPage("positions");
      },
      disabled: false,
    },
    {
      label: "Evidence",
      status: "OPEN",
      tone: "muted" as Tone,
      onClick: () => {
        window.location.hash = "#/diagnostics";
        setPage("diagnostics");
      },
      disabled: false,
    },
  ];
  const rosterSummaryCounts = {
    live: dashboardRosterRows.filter((row) => rosterStatusChip(row) === "LIVE" || rosterStatusChip(row) === "ATTACH").length,
    paper: dashboardRosterRows.filter((row) => rosterStatusChip(row) === "PAPER" || rosterStatusChip(row) === "BENCH").length,
    candidate: dashboardRosterRows.filter((row) => rosterStatusChip(row) === "CAND").length,
    paused: dashboardRosterRows.filter((row) => rosterStatusChip(row) === "PAUSED").length,
  };
  const replaySummaryMetrics: Array<{ label: string; value: string; tone?: Tone }> = playbackReplaySummaryAvailable
    ? [
        { label: "Standalone Strategies", value: formatShortNumber(playbackAggregateSummary.standalone_strategy_count) },
        { label: "Processed Bars", value: formatShortNumber(playbackAggregateSummary.processed_bars) },
        { label: "Order Intents", value: formatShortNumber(playbackAggregateSummary.order_intents) },
        { label: "Fills", value: formatShortNumber(playbackAggregateSummary.fills) },
        { label: "Entries", value: formatShortNumber(playbackAggregateSummary.entries) },
        { label: "Exits", value: formatShortNumber(playbackAggregateSummary.exits) },
        { label: "Run Timestamp", value: formatTimestamp(playbackLatestRun.run_timestamp) },
        { label: "Primary Standalone Strategy", value: formatValue(playbackLatestRun.primary_standalone_strategy_id) },
      ]
    : [
        { label: "Available", value: formatValue(playback.available) },
        { label: "Run Stamp", value: formatValue(playbackLatestRun.run_stamp) },
        { label: "Bars Processed", value: formatShortNumber(playbackLatestRun.bars_processed) },
        { label: "Intents Created", value: formatShortNumber(playbackLatestRun.intents_created) },
        { label: "Fills Created", value: formatShortNumber(playbackLatestRun.fills_created) },
        { label: "Symbols", value: formatValue(playbackLatestRun.symbols) },
        { label: "Run Timestamp", value: formatTimestamp(playbackLatestRun.run_timestamp) },
      ];
  const brokerSummaryMetrics: Array<{ label: string; value: string; tone?: Tone }> = productionLinkEnabled()
    ? [
        {
          label: "Broker Connected",
          value: formatValue(productionLink.label ?? productionLink.status),
          tone: statusTone(productionLink.label ?? productionLink.status),
        },
        {
          label: "Selected Account",
          value: selectedProductionAccount ? `${maskAccountNumber(selectedProductionAccount.account_number)} / ${formatValue(selectedProductionAccount.account_type)}` : "None",
        },
        {
          label: "Balances Fresh",
          value: formatValue(asRecord(productionHealth.balances_fresh).label),
          tone: statusTone(asRecord(productionHealth.balances_fresh).label),
        },
        {
          label: "Positions Fresh",
          value: formatValue(asRecord(productionHealth.positions_fresh).label),
          tone: statusTone(asRecord(productionHealth.positions_fresh).label),
        },
        {
          label: "Orders Fresh",
          value: formatValue(asRecord(productionHealth.orders_fresh).label),
          tone: statusTone(asRecord(productionHealth.orders_fresh).label),
        },
        {
          label: "Reconciliation",
          value: formatValue(productionReconciliation.label ?? productionReconciliation.status),
          tone: statusTone(productionReconciliation.label ?? productionReconciliation.status),
        },
        {
          label: "Buying Power",
          value: formatValue(productionBalances.buying_power),
        },
        {
          label: "Liquidation Value",
          value: formatValue(productionBalances.liquidation_value),
        },
      ]
    : [
        { label: "Broker Connected", value: "Disabled by feature flag", tone: "warn" as const },
        { label: "Selected Account", value: "None" },
        { label: "Reconciliation", value: "Unavailable" },
      ];
  const researchCaptureRunStatus = String(researchCapture.run_status ?? "no_run");
  const researchCaptureFreshnessState = String(researchCapture.freshness_state ?? "no_run");
  const researchCaptureMetrics: Array<{ label: string; value: string; tone?: Tone }> = [
    {
      label: "Last Run Status",
      value: sentenceCase(researchCaptureRunStatus.replace(/_/g, " ")),
      tone: researchCaptureTone(researchCaptureRunStatus, researchCaptureFreshnessState),
    },
    {
      label: "Freshness",
      value: sentenceCase(researchCaptureFreshnessState.replace(/_/g, " ")),
      tone: researchCaptureTone(researchCaptureRunStatus, researchCaptureFreshnessState),
    },
    { label: "Last Attempted", value: formatTimestamp(researchCapture.last_attempted_at), tone: researchCapture.last_attempted_at ? "good" : "warn" },
    { label: "Last Successful", value: formatTimestamp(researchCapture.last_succeeded_at), tone: researchCapture.last_succeeded_at ? "good" : "warn" },
    { label: "Symbols Attempted", value: formatShortNumber(researchCapture.attempted_count ?? asArray<string>(researchCapture.attempted_symbols).length) },
    {
      label: "Symbols Succeeded",
      value: formatShortNumber(researchCapture.succeeded_count ?? asArray<string>(researchCapture.succeeded_symbols).length),
      tone: Number(researchCapture.succeeded_count ?? asArray<string>(researchCapture.succeeded_symbols).length ?? 0) > 0 ? "good" : "muted",
    },
    {
      label: "Symbols Failed",
      value: formatShortNumber(researchCapture.failed_count ?? researchCaptureFailedSymbols.length),
      tone: Number(researchCapture.failed_count ?? researchCaptureFailedSymbols.length ?? 0) > 0 ? "warn" : "good",
    },
    { label: "Research DB", value: formatValue(researchCapture.research_database_path ?? "Unavailable") },
  ];
  const synthesizedRecoveryAlerts = useMemo<JsonRecord[]>(() => {
    const status = String(paperRuntimeRecovery.status ?? "").toUpperCase();
    if (!status || status === "RUNNING" || status === "NOT_APPLICABLE") {
      return [];
    }
    const severity =
      status === "AUTO_RESTART_SUCCEEDED"
        ? "RECOVERY"
        : status === "AUTO_RESTART_SUPPRESSED"
          ? "BLOCKING"
          : status === "AUTO_RESTART_FAILED" || status === "STOPPED_MANUAL_REQUIRED"
            ? "ACTION"
          : "INFO";
    return [
      {
        occurred_at: paperRuntimeRecovery.attempted_at ?? paperRuntimeRecovery.updated_at ?? desktopState?.refreshedAt,
        category: "runtime_recovery",
        severity,
        dedup_key: "paper-runtime-supervisor",
        title: "Paper Runtime Recovery",
        message: paperRuntimeRecovery.operator_message ?? "Paper runtime recovery state changed.",
        recommended_action: paperRuntimeRecovery.manual_action_required
          ? paperRuntimeRecovery.operator_message ?? "Manual runtime intervention is required."
          : "No manual action required.",
        active: paperRuntimeRecovery.manual_action_required === true,
        source_subsystem: "operator_dashboard",
      },
    ];
  }, [desktopState?.refreshedAt, paperRuntimeRecovery.attempted_at, paperRuntimeRecovery.manual_action_required, paperRuntimeRecovery.operator_message, paperRuntimeRecovery.status, paperRuntimeRecovery.updated_at]);
  const operatorRecentAlertRows = useMemo(
    () =>
      [...paperAlertEvents, ...synthesizedRecoveryAlerts, ...productionLifecycleAlertRows]
        .sort((left, right) => parseTimestampMs(right.occurred_at ?? right.logged_at) - parseTimestampMs(left.occurred_at ?? left.logged_at))
        .slice(0, 10),
    [paperAlertEvents, productionLifecycleAlertRows, synthesizedRecoveryAlerts],
  );
  const operatorActiveAlertRows = useMemo(
    () =>
      [...paperActiveAlertRows, ...synthesizedRecoveryAlerts.filter((row) => row.active === true), ...productionLifecycleActiveAlertRows]
        .sort((left, right) => parseTimestampMs(right.last_seen_at ?? right.occurred_at ?? right.logged_at) - parseTimestampMs(left.last_seen_at ?? left.occurred_at ?? left.logged_at))
        .slice(0, 8),
    [paperActiveAlertRows, productionLifecycleActiveAlertRows, synthesizedRecoveryAlerts],
  );
  const operatorAlertMetrics: Array<{ label: string; value: string; tone?: Tone }> = [
    {
      label: "Active Alerts",
      value: formatShortNumber(operatorActiveAlertRows.length),
      tone: operatorActiveAlertRows.length > 0 ? "warn" : "good",
    },
    {
      label: "Blocking",
      value: formatShortNumber(operatorActiveAlertRows.filter((row) => String(row.severity ?? "").toUpperCase() === "BLOCKING").length),
      tone: operatorActiveAlertRows.some((row) => String(row.severity ?? "").toUpperCase() === "BLOCKING") ? "danger" : "good",
    },
    {
      label: "Action Required",
      value: formatShortNumber(operatorActiveAlertRows.filter((row) => String(row.severity ?? "").toUpperCase() === "ACTION").length),
      tone: operatorActiveAlertRows.some((row) => String(row.severity ?? "").toUpperCase() === "ACTION") ? "warn" : "good",
    },
    {
      label: "Recoveries",
      value: formatShortNumber(operatorRecentAlertRows.filter((row) => String(row.severity ?? "").toUpperCase() === "RECOVERY").length),
      tone: operatorRecentAlertRows.some((row) => String(row.severity ?? "").toUpperCase() === "RECOVERY") ? "good" : "muted",
    },
    {
      label: "Latest Alert",
      value: formatRelativeAge(operatorRecentAlertRows[0]?.occurred_at ?? operatorRecentAlertRows[0]?.logged_at),
      tone: operatorRecentAlertRows.length ? "muted" : "good",
    },
  ];
  const operatorAlertsStatusLine = operatorActiveAlertRows.length
    ? `${operatorActiveAlertRows.length} active operator alert${operatorActiveAlertRows.length === 1 ? "" : "s"} currently require visibility.`
    : "No active operator alerts are currently open.";
  const strategyBrokerDriftSummary = productionLinkEnabled()
    ? `Strategy ledger shows ${formatShortNumber(strategyPortfolioSnapshot.active_strategy_count)} active standalone strategies across ${formatShortNumber(strategyPortfolioSnapshot.active_instrument_count)} instruments. Broker truth currently shows ${formatShortNumber(productionPositions.length)} broker positions. Reconciliation: ${formatValue(productionReconciliation.label ?? productionReconciliation.status)}.`
    : "Broker production-link is disabled, so only strategy-ledger paper truth is available in this session.";
  const runtimeBlockingFaultRows = asArray<JsonRecord>(runtimeReadiness.blocking_faults);
  const readinessDegradedFeeds = asArray<string>(runtimeValues.degraded_informational_feeds ?? runtimeReadiness.degraded_informational_feeds);
  const readinessLaneRiskRows = asArray<JsonRecord>(paperReadiness.lane_risk_rows);
  const readinessLaneStatusRows = asArray<JsonRecord>(paperReadiness.lane_status_rows);
  const readinessLaneStatusSummary = asRecord(paperReadiness.lane_status_summary);
  const heartbeatReconciliationSummary = asRecord(paperReadiness.heartbeat_reconciliation_summary);
  const orderTimeoutWatchdogSummary = asRecord(paperReadiness.order_timeout_watchdog_summary);
  const laneEligibilityById = new Map(
    asArray<JsonRecord>(paperReadiness.lane_eligibility_rows).map((row) => [String(row.lane_id ?? ""), row] as const),
  );
  const loadedNotEligibleRows = readinessLaneStatusRows.filter((row) =>
    ["LOADED_NOT_ELIGIBLE", "LOADED_CONFIG_ONLY"].includes(String(row.tradability_status ?? "").toUpperCase()),
  );
  const reconcilingLaneRows = readinessLaneStatusRows.filter((row) => String(row.tradability_status ?? "").toUpperCase() === "RECONCILING");
  const faultedLaneRows = readinessLaneStatusRows.filter((row) => String(row.tradability_status ?? "").toUpperCase() === "FAULTED");
  const informationalOnlyLaneRows = readinessLaneStatusRows.filter(
    (row) => String(row.tradability_status ?? "").toUpperCase() === "INFORMATIONAL_ONLY",
  );
  const haltedDegradationRows = readinessLaneRiskRows.filter((row) => String(row.risk_state ?? "").toUpperCase() === "HALTED_DEGRADATION");
  const runtimeUpButLaneHalted = (paperReadiness.runtime_running === true || String(paperReadiness.runtime_phase ?? runtimeValues.runtime_status ?? "").toUpperCase() === "RUNNING") && haltedDegradationRows.length > 0;
  const runtimeEntriesEnabled = (global.entries_enabled ?? runtimeValues.entries_enabled ?? paperReadiness.entries_enabled) === true;
  const runtimeOperatorHalt = inferOperatorHalt(global, paperReadiness);
  const atpeLongLaneHalts = haltedDegradationRows.filter((row) =>
    ["atpe_long_medium_high_canary__MES", "atpe_long_medium_high_canary__MNQ"].includes(String(row.lane_id ?? "")),
  );
  const laneHaltRecoveryRows = haltedDegradationRows.map((row) => {
    const laneId = String(row.lane_id ?? row.display_name ?? row.symbol ?? "unknown_lane");
    const eligibilityRow = laneEligibilityById.get(laneId) ?? {};
    const nextSessionResetRequired =
      row.auto_clear_on_session_reset === true ||
      String(row.halt_reason ?? "").trim() === "lane_realized_loser_limit_per_session";
    const clearAction = nextSessionResetRequired
      ? "Wait for next session reset (auto-clear)"
      : formatValue(row.unblock_action ?? "Clear Risk Halts");
    const haltReasonRaw = String(row.halt_reason ?? row.risk_state ?? "").trim();
    const canForceSessionOverride =
      String(row.risk_state ?? "").toUpperCase() === "HALTED_DEGRADATION" && haltReasonRaw === "lane_realized_loser_limit_per_session";
    return {
      laneId,
      laneLabel: formatValue(row.display_name ?? row.lane_id),
      symbolLabel: formatValue(row.symbol ?? "—"),
      riskStateLabel: formatValue(row.risk_state ?? "HALTED_DEGRADATION"),
      haltReasonRaw,
      haltReasonLabel: laneHaltReasonLabel(row.halt_reason ?? row.risk_state),
      latchedLabel: laneHaltLatchedLabel(row),
      clearActionLabel: clearAction,
      nextSessionResetRequired,
      canForceSessionOverride,
      rawEligibilityStateLabel: formatValue(
        eligibilityRow.eligible_now === true
          ? "eligible_now=true"
          : eligibilityRow.eligibility_reason ?? "No eligibility blocker surfaced",
      ),
      effectiveTradingStateLabel: nextSessionResetRequired
        ? "Done for current session"
        : "Risk-halted until cleared",
      latchVsSessionDetail: nextSessionResetRequired
        ? "The halt latch stays active for the current session, then auto-clears at the next valid session boundary. Same-session realized-loss protection is not overridden automatically."
        : "This halt is controlled by the active lane-risk latch and can clear when the blocking condition is removed.",
      recoveryDetail: nextSessionResetRequired
        ? "No manual clear is required for next-session recovery. The lane returns to normal policy after session reset unless you deliberately use the audited same-session override."
        : runtimeEntriesEnabled && !runtimeOperatorHalt
          ? "Resume Entries may restore this lane once the risk halt is cleared and no other blocker remains."
          : "Clear the risk halt first, then resume entries if the runtime is still intentionally halted.",
      supportedOverrideLabel: canForceSessionOverride ? "Available with explicit session override" : "No supported same-session override",
    };
  });
  const anyLaneNextSessionResetRequired = laneHaltRecoveryRows.some((row) => row.nextSessionResetRequired);
  const sessionOverrideRows = readinessLaneRiskRows
    .filter((row) => row.session_override_active === true)
    .map((row) => ({
      laneId: String(row.lane_id ?? row.display_name ?? row.symbol ?? "unknown_lane"),
      laneLabel: formatValue(row.display_name ?? row.lane_id),
      symbolLabel: formatValue(row.symbol ?? "—"),
      appliedAtLabel: formatTimestamp(row.session_override_applied_at),
      appliedByLabel: formatValue(row.session_override_applied_by ?? "Unknown"),
      scopeLabel: formatValue(row.session_override_session_date ? `Session ${row.session_override_session_date}` : "Current session"),
      noteLabel: formatValue(row.session_override_note ?? "Session override active."),
      reasonLabel: laneHaltReasonLabel(row.session_override_reason ?? row.halt_reason ?? row.risk_state),
    }));
  const runtimeFaultDetailRows = runtimeBlockingFaultRows.map((row) => ({
    title: runtimeFaultTitle(row),
    severityLabel: formatValue(row.severity ?? "BLOCKING"),
    detailText: formatValue(row.details ?? row.detail ?? row.message ?? "No detail surfaced."),
    recommendationText: row.recommendation ? formatValue(row.recommendation) : null,
    staleOnlyClear:
      String(row.code ?? row.fault_code ?? "").toUpperCase() === "DECISION_WITHOUT_INTENT"
        ? "Use Clear Fault only if you verified this is stale and the missing intent condition is no longer real."
        : "Clear Fault should follow verification that the underlying condition is no longer active.",
  }));
  const runtimeFaultSummary = runtimeBlockingFaultRows.length
    ? runtimeBlockingFaultRows.map((row) => String(row.code ?? "UNKNOWN_FAULT")).join(", ")
    : "No blocking runtime faults.";
  const degradedFeedSummary = readinessDegradedFeeds.length ? readinessDegradedFeeds.join(", ") : "No degraded informational feeds.";
  const haltedLaneSummary = haltedDegradationRows.length
    ? haltedDegradationRows
        .map((row) => `${formatValue(row.display_name ?? row.lane_id)} (${formatValue(row.halt_reason ?? row.risk_state)})`)
        .join(", ")
    : "No lane-local degradation halts.";
  const heartbeatActiveIssueRows = asArray<JsonRecord>(heartbeatReconciliationSummary.active_issue_rows);
  const heartbeatLastStatus = heartbeatReconciliationSummary.last_status ?? "UNAVAILABLE";
  const heartbeatCadenceSeconds = Number(heartbeatReconciliationSummary.cadence_seconds ?? 0) || null;
  const heartbeatLastAttemptedAt = heartbeatReconciliationSummary.last_attempted_at;
  const heartbeatReason = heartbeatReconciliationSummary.reason;
  const heartbeatRecommendedAction = heartbeatReconciliationSummary.recommended_action;
  const heartbeatStatusLine =
    heartbeatLastStatus === "UNAVAILABLE"
      ? "No heartbeat reconciliation has been recorded in the current runtime snapshot yet."
      : `Heartbeat reconciliation runs every ${heartbeatCadenceSeconds ?? "?"}s while runtime is active. Last result: ${formatValue(heartbeatLastStatus)} at ${formatTimestamp(heartbeatLastAttemptedAt)}.`;
  const orderTimeoutActiveIssueRows = asArray<JsonRecord>(orderTimeoutWatchdogSummary.active_issue_rows);
  const orderTimeoutLastStatus = orderTimeoutWatchdogSummary.last_status ?? "UNAVAILABLE";
  const orderTimeoutLastCheckedAt = orderTimeoutWatchdogSummary.last_checked_at;
  const orderTimeoutOverdueAckCount = Number(orderTimeoutWatchdogSummary.overdue_ack_count ?? 0) || 0;
  const orderTimeoutOverdueFillCount = Number(orderTimeoutWatchdogSummary.overdue_fill_count ?? 0) || 0;
  const orderTimeoutReason = orderTimeoutWatchdogSummary.reason;
  const orderTimeoutRecommendedAction = orderTimeoutWatchdogSummary.recommended_action;
  const restoreValidationSummary = asRecord(paperReadiness.restore_validation_summary);
  const restoreValidationActiveIssueRows = asArray<JsonRecord>(restoreValidationSummary.active_issue_rows);
  const restoreValidationLastResult = String(restoreValidationSummary.last_restore_result ?? paperSoakContinuity.last_restore_result ?? "UNAVAILABLE");
  const restoreValidationLastCompletedAt = restoreValidationSummary.last_restore_completed_at ?? paperSoakContinuity.last_restore_completed_at;
  const restoreValidationSafeCleanupCount = Number(restoreValidationSummary.safe_cleanup_count ?? 0) || 0;
  const restoreValidationUnresolvedIssueCount = Number(restoreValidationSummary.unresolved_issue_count ?? 0) || 0;
  const restoreDuplicateActionPreventionHeld = restoreValidationSummary.duplicate_action_prevention_held !== false;
  const orderTimeoutStatusLine =
    orderTimeoutLastStatus === "UNAVAILABLE"
      ? "Pending-order timeout automation has not run in the current runtime snapshot yet."
      : `Pending-order watchdog last checked at ${formatTimestamp(orderTimeoutLastCheckedAt)}. ACK overdue: ${orderTimeoutOverdueAckCount}. Fill overdue: ${orderTimeoutOverdueFillCount}. Status: ${formatValue(orderTimeoutLastStatus)}.`;
  const runtimeSupervisorStatusLine =
    paperRuntimeRecoveryState === "NOT_APPLICABLE"
      ? "Runtime supervisor is idle because the paper runtime is not currently in a stopped-state recovery path."
      : `Runtime supervisor status: ${formatValue(paperRuntimeRecoveryState)}. Restart budget: ${paperRuntimeRestartAttemptsInWindow}/${paperRuntimeRestartBudget || "?"}. Last result: ${formatValue(paperRuntimeLastRestartResult)} at ${formatTimestamp(paperRuntimeLastRestartAttemptAt)}.`;
  const runtimeSupervisorActionLine =
    paperRuntimeRestartSuppressed
      ? `Auto-restart is suppressed until ${formatTimestamp(paperRuntimeRestartSuppressedUntil)}. Next action: ${formatValue(paperRuntimeRecoveryNextAction || "Start Runtime")}.`
      : paperRuntimeRecoveryState === "AUTO_RESTART_BACKOFF"
        ? `Auto-restart backoff is active until ${formatTimestamp(paperRuntimeRestartBackoffUntil)}. The supervisor will retry automatically if the stop remains safe.`
        : paperAutoRestartAllowed
          ? "Auto-restart is currently allowed if the runtime remains safely stopped."
          : paperRuntimeRecoveryMessage || "No runtime-supervisor intervention is currently required.";
  const paperSoakUptimeSeconds = Number(paperSoakContinuity.runtime_uptime_seconds ?? 0) || 0;
  const paperSoakHealthy = paperSoakContinuity.healthy_soak === true;
  const paperSoakContinuityMetrics: Array<{ label: string; value: string; tone?: Tone }> = [
    { label: "Runtime Uptime", value: paperSoakUptimeSeconds > 0 ? formatDuration(paperSoakUptimeSeconds) : "Unavailable", tone: paperSoakUptimeSeconds > 0 ? "good" : "muted" },
    { label: "Last Restart", value: formatTimestamp(paperSoakContinuity.last_restart_time), tone: paperSoakContinuity.last_restart_time ? "warn" : "muted" },
    { label: "Restart Count", value: `${formatShortNumber(paperSoakContinuity.restart_count_window ?? paperRuntimeRestartAttemptsInWindow)}/${formatShortNumber((paperSoakContinuity.restart_budget_window ?? paperRuntimeRestartBudget) || 0)}`, tone: paperRuntimeRestartSuppressed ? "danger" : Number(paperSoakContinuity.restart_count_window ?? paperRuntimeRestartAttemptsInWindow ?? 0) > 0 ? "warn" : "good" },
    { label: "Last Restore", value: formatValue(restoreValidationLastResult), tone: restoreValidationUnresolvedIssueCount > 0 ? "danger" : restoreValidationLastResult === "SAFE_CLEANUP_READY" ? "warn" : restoreValidationLastResult === "READY" ? "good" : "muted" },
    { label: "Restore Issues", value: formatShortNumber(restoreValidationUnresolvedIssueCount), tone: restoreValidationUnresolvedIssueCount > 0 ? "danger" : "good" },
    { label: "Soak Health", value: paperSoakHealthy ? "HEALTHY" : "DEGRADED", tone: paperSoakHealthy ? "good" : "warn" },
  ];
  const paperSoakContinuityStatusLine = `Last restore result: ${formatValue(restoreValidationLastResult)} at ${formatTimestamp(restoreValidationLastCompletedAt)}. Duplicate-action prevention: ${restoreDuplicateActionPreventionHeld ? "held" : "needs review"}.`;
  const paperSoakContinuityDetailLine =
    restoreValidationUnresolvedIssueCount > 0
      ? `Restore validation still has ${restoreValidationUnresolvedIssueCount} unresolved issue${restoreValidationUnresolvedIssueCount === 1 ? "" : "s"}. ${formatValue(restoreValidationSummary.recommended_action ?? "Inspect reconciliation/fault detail before resuming entries.")}`
      : restoreValidationSafeCleanupCount > 0
        ? `Restore validation applied ${restoreValidationSafeCleanupCount} safe cleanup ${restoreValidationSafeCleanupCount === 1 ? "action" : "actions"} automatically.`
        : paperSoakHealthy
          ? "Paper soak continuity is healthy across restart/restore surfaces."
          : "Paper soak continuity is available, but one or more health or recovery surfaces are degraded.";
  const shadowBrokerTruthSummary = asRecord(shadowLiveSummary.broker_truth_summary);
  const shadowLatestSignalSummary = asRecord(shadowLiveSummary.latest_signal_summary);
  const shadowLatestIntent = asRecord(shadowLiveSummary.latest_shadow_intent);
  const shadowPositionState = asRecord(shadowLiveSummary.position_state);
  const liveStrategyPilotGate = asRecord(liveStrategyPilotSummary.submit_gate);
  const liveStrategyPilotBrokerTruthSummary = asRecord(liveStrategyPilotSummary.broker_truth_summary);
  const liveStrategyPilotPositionState = asRecord(liveStrategyPilotSummary.position_state);
  const liveStrategyPilotCycle = asRecord(liveStrategyPilotSummary.pilot_cycle);
  const liveStrategyPilotCycleEntry = asRecord(liveStrategyPilotCycle.entry);
  const liveStrategyPilotCycleExit = asRecord(liveStrategyPilotCycle.exit);
  const liveStrategyPilotLatestBar = asRecord(liveStrategyPilotSummary.latest_evaluated_bar);
  const liveStrategyPilotLatestSignal = asRecord(liveStrategyPilotSummary.latest_signal_decision);
  const liveStrategyPilotLatestIntent = asRecord(liveStrategyPilotSummary.latest_live_strategy_intent);
  const liveStrategyPilotSignalObservability = asRecord(liveStrategyPilotSummary.signal_observability);
  const liveStrategyPilotSignalCounts = asRecord(liveStrategyPilotSignalObservability.session_counts);
  const liveStrategyPilotSignalRawVsFinal = asRecord(liveStrategyPilotSignalObservability.raw_candidates_seen_vs_final_entries);
  const liveStrategyPilotSignalRawVsFinalLong = asRecord(liveStrategyPilotSignalRawVsFinal.long);
  const liveStrategyPilotSignalRawVsFinalShort = asRecord(liveStrategyPilotSignalRawVsFinal.short);
  const liveStrategyPilotSignalAntiChurn = asRecord(liveStrategyPilotSignalObservability.anti_churn);
  const liveStrategyPilotSignalTopFailedPredicates = asRecord(liveStrategyPilotSignalObservability.top_failed_predicates);
  const liveStrategyPilotBullTopFailures = asArray<JsonRecord>(liveStrategyPilotSignalTopFailedPredicates.bullSnapLong);
  const liveStrategyPilotAsiaTopFailures = asArray<JsonRecord>(liveStrategyPilotSignalTopFailedPredicates.asiaVWAPLong);
  const liveStrategyPilotBearTopFailures = asArray<JsonRecord>(liveStrategyPilotSignalTopFailedPredicates.bearSnapShort);
  const liveStrategyPilotRecentNoTradeRows = [...asArray<JsonRecord>(liveStrategyPilotSignalObservability.per_bar_rows)].slice(-8).reverse();
  const signalSelectivityLiveFocus = asRecord(signalSelectivityAnalysis.live_pilot_focus);
  const signalSelectivityTopFailed = asRecord(signalSelectivityLiveFocus.top_failed_predicates);
  const signalSelectivityRangeLadder = asRecord(signalSelectivityAnalysis.bear_snap_range_ladder);
  const signalSelectivityStretchLadder = asRecord(signalSelectivityAnalysis.bear_snap_up_stretch_ladder);
  const signalSelectivityBullTop = asArray<JsonRecord>(signalSelectivityTopFailed.bullSnapLong);
  const signalSelectivityAsiaTop = asArray<JsonRecord>(signalSelectivityTopFailed.asiaVWAPLong);
  const signalSelectivityBearTop = asArray<JsonRecord>(signalSelectivityTopFailed.bearSnapShort);
  const signalSelectivityRawVsFinal = asRecord(signalSelectivityLiveFocus.raw_candidates_vs_final_entries);
  const signalSelectivityRawVsFinalLong = asRecord(signalSelectivityRawVsFinal.long);
  const signalSelectivityRawVsFinalShort = asRecord(signalSelectivityRawVsFinal.short);
  const signalSelectivityAntiChurn = asRecord(signalSelectivityLiveFocus.anti_churn);
  const shadowLiveMetrics: Array<{ label: string; value: string; tone?: Tone }> = [
    { label: "Runtime Phase", value: formatValue(shadowLiveSummary.current_runtime_phase ?? "UNKNOWN"), tone: statusTone(shadowLiveSummary.current_runtime_phase) },
    { label: "Strategy State", value: formatValue(shadowLiveSummary.strategy_state ?? "UNKNOWN"), tone: statusTone(shadowLiveSummary.strategy_state) },
    { label: "Last Finalized Bar", value: formatValue(shadowLiveSummary.last_finalized_live_bar_id ?? "Unavailable") },
    { label: "Session", value: formatValue(shadowLiveSummary.session_classification ?? "UNKNOWN"), tone: statusTone(shadowLiveSummary.session_classification) },
    { label: "Would Submit", value: shadowLiveSummary.submit_would_be_allowed_if_shadow_disabled ? "YES" : "NO", tone: shadowLiveSummary.submit_would_be_allowed_if_shadow_disabled ? "good" : "warn" },
    { label: "Blocker", value: formatValue(shadowLiveSummary.entries_disabled_blocker ?? "None"), tone: shadowLiveSummary.entries_disabled_blocker ? "warn" : "good" },
    { label: "Broker Truth", value: formatValue(shadowBrokerTruthSummary.classification ?? "UNKNOWN"), tone: statusTone(shadowBrokerTruthSummary.classification) },
    { label: "Position State", value: `${formatValue(shadowPositionState.side ?? "UNKNOWN")} / ${formatValue(shadowPositionState.internal_qty ?? 0)}`, tone: statusTone(shadowPositionState.side) },
  ];
  const liveStrategyPilotMetrics: Array<{ label: string; value: string; tone?: Tone }> = [
    { label: "Pilot Mode", value: liveStrategyPilotSummary.live_strategy_pilot_enabled ? "ENABLED" : "DISABLED", tone: liveStrategyPilotSummary.live_strategy_pilot_enabled ? "good" : "warn" },
    { label: "Submit Flag", value: liveStrategyPilotSummary.live_strategy_submit_enabled ? "ENABLED" : "DISABLED", tone: liveStrategyPilotSummary.live_strategy_submit_enabled ? "good" : "warn" },
    { label: "Pilot Armed", value: liveStrategyPilotSummary.pilot_armed ? "ARMED" : "DISARMED", tone: liveStrategyPilotSummary.pilot_armed ? "good" : "warn" },
    { label: "Cycle Status", value: formatValue(liveStrategyPilotSummary.cycle_status ?? "waiting_for_entry"), tone: statusTone(liveStrategyPilotSummary.cycle_status) },
    { label: "Submits Left", value: formatValue(liveStrategyPilotSummary.remaining_allowed_live_submits ?? 0), tone: Number(liveStrategyPilotSummary.remaining_allowed_live_submits ?? 0) > 0 ? "good" : "warn" },
    { label: "Runtime Phase", value: formatValue(liveStrategyPilotSummary.current_runtime_phase ?? "UNKNOWN"), tone: statusTone(liveStrategyPilotSummary.current_runtime_phase) },
    { label: "Strategy State", value: formatValue(liveStrategyPilotSummary.strategy_state ?? "UNKNOWN"), tone: statusTone(liveStrategyPilotSummary.strategy_state) },
    { label: "Submit Eligible", value: liveStrategyPilotSummary.submit_currently_enabled ? "YES" : "NO", tone: liveStrategyPilotSummary.submit_currently_enabled ? "good" : "warn" },
    { label: "Pending Stage", value: formatValue(liveStrategyPilotSummary.pending_stage ?? "IDLE"), tone: statusTone(liveStrategyPilotSummary.pending_stage) },
    { label: "Blocker", value: formatValue(liveStrategyPilotSummary.entries_disabled_blocker ?? "None"), tone: liveStrategyPilotSummary.entries_disabled_blocker ? "warn" : "good" },
    { label: "Broker Truth", value: formatValue(liveStrategyPilotBrokerTruthSummary.classification ?? "UNKNOWN"), tone: statusTone(liveStrategyPilotBrokerTruthSummary.classification) },
    { label: "Position State", value: `${formatValue(liveStrategyPilotPositionState.side ?? "UNKNOWN")} / ${formatValue(liveStrategyPilotPositionState.internal_qty ?? 0)}`, tone: statusTone(liveStrategyPilotPositionState.side) },
  ];
  const liveStrategyPilotSignalMetrics: Array<{ label: string; value: string; tone?: Tone }> = [
    { label: "Why No Trade", value: formatValue(liveStrategyPilotSignalObservability.why_no_trade_so_far ?? "Unavailable"), tone: Number(liveStrategyPilotSignalCounts.longEntry ?? 0) === 0 && Number(liveStrategyPilotSignalCounts.shortEntry ?? 0) === 0 ? "warn" : "good" },
    { label: "Bull Candidates", value: `${formatShortNumber(liveStrategyPilotSignalCounts.bull_snap_turn_candidate ?? 0)} -> ${formatShortNumber(liveStrategyPilotSignalCounts.firstBullSnapTurn ?? 0)}`, tone: Number(liveStrategyPilotSignalCounts.firstBullSnapTurn ?? 0) > 0 ? "good" : "muted" },
    { label: "Asia VWAP Chain", value: `${formatShortNumber(liveStrategyPilotSignalCounts.asia_reclaim_bar_raw ?? 0)} / ${formatShortNumber(liveStrategyPilotSignalCounts.asia_hold_bar_ok ?? 0)} / ${formatShortNumber(liveStrategyPilotSignalCounts.asia_acceptance_bar_ok ?? 0)} / ${formatShortNumber(liveStrategyPilotSignalCounts.asiaVWAPLongSignal ?? 0)}`, tone: Number(liveStrategyPilotSignalCounts.asiaVWAPLongSignal ?? 0) > 0 ? "good" : "muted" },
    { label: "Bear Candidates", value: `${formatShortNumber(liveStrategyPilotSignalCounts.bear_snap_turn_candidate ?? 0)} -> ${formatShortNumber(liveStrategyPilotSignalCounts.firstBearSnapTurn ?? 0)}`, tone: Number(liveStrategyPilotSignalCounts.firstBearSnapTurn ?? 0) > 0 ? "good" : "muted" },
    { label: "Long Raw -> Final", value: `${formatShortNumber(liveStrategyPilotSignalRawVsFinalLong.raw_candidates_seen ?? liveStrategyPilotSignalCounts.longEntryRaw ?? 0)} -> ${formatShortNumber(liveStrategyPilotSignalRawVsFinalLong.final_entries_produced ?? liveStrategyPilotSignalCounts.longEntry ?? 0)}`, tone: Number(liveStrategyPilotSignalRawVsFinalLong.final_entries_produced ?? liveStrategyPilotSignalCounts.longEntry ?? 0) > 0 ? "good" : "warn" },
    { label: "Short Raw -> Final", value: `${formatShortNumber(liveStrategyPilotSignalRawVsFinalShort.raw_candidates_seen ?? liveStrategyPilotSignalCounts.shortEntryRaw ?? 0)} -> ${formatShortNumber(liveStrategyPilotSignalRawVsFinalShort.final_entries_produced ?? liveStrategyPilotSignalCounts.shortEntry ?? 0)}`, tone: Number(liveStrategyPilotSignalRawVsFinalShort.final_entries_produced ?? liveStrategyPilotSignalCounts.shortEntry ?? 0) > 0 ? "good" : "warn" },
    { label: "Recent Long Setup", value: `${formatShortNumber(liveStrategyPilotSignalAntiChurn.recentLongSetup_true_bars ?? 0)} bars / ${formatShortNumber(liveStrategyPilotSignalAntiChurn.recentLongSetup_suppressed_bars ?? 0)} suppressed`, tone: Number(liveStrategyPilotSignalAntiChurn.recentLongSetup_suppressed_bars ?? 0) > 0 ? "warn" : "muted" },
    { label: "Recent Short Setup", value: `${formatShortNumber(liveStrategyPilotSignalAntiChurn.recentShortSetup_true_bars ?? 0)} bars / ${formatShortNumber(liveStrategyPilotSignalAntiChurn.recentShortSetup_suppressed_bars ?? 0)} suppressed`, tone: Number(liveStrategyPilotSignalAntiChurn.recentShortSetup_suppressed_bars ?? 0) > 0 ? "warn" : "muted" },
  ];
  const signalSelectivityMetrics: Array<{ label: string; value: string; tone?: Tone }> = [
    { label: "Analysis Datasets", value: formatValue(signalSelectivityAnalysis.dataset_count ?? 0), tone: Number(signalSelectivityAnalysis.dataset_count ?? 0) > 0 ? "good" : "muted" },
    { label: "Live Why No Trade", value: formatValue(signalSelectivityLiveFocus.why_no_trade_so_far ?? "Unavailable"), tone: Number(signalSelectivityRawVsFinalLong.final_entries ?? 0) === 0 && Number(signalSelectivityRawVsFinalShort.final_entries ?? 0) === 0 ? "warn" : "good" },
    { label: "Long Raw -> Final", value: `${formatShortNumber(signalSelectivityRawVsFinalLong.raw_candidates ?? 0)} -> ${formatShortNumber(signalSelectivityRawVsFinalLong.final_entries ?? 0)}`, tone: Number(signalSelectivityRawVsFinalLong.final_entries ?? 0) > 0 ? "good" : "warn" },
    { label: "Short Raw -> Final", value: `${formatShortNumber(signalSelectivityRawVsFinalShort.raw_candidates ?? 0)} -> ${formatShortNumber(signalSelectivityRawVsFinalShort.final_entries ?? 0)}`, tone: Number(signalSelectivityRawVsFinalShort.final_entries ?? 0) > 0 ? "good" : "warn" },
    { label: "Long Anti-Churn", value: formatShortNumber(asRecord(signalSelectivityAntiChurn.suppression_by_family).bullSnapLong?.suppressed_count ?? 0), tone: Number(asRecord(signalSelectivityAntiChurn.suppression_by_family).bullSnapLong?.suppressed_count ?? 0) > 0 ? "warn" : "muted" },
    { label: "Short Anti-Churn", value: formatShortNumber(asRecord(signalSelectivityAntiChurn.suppression_by_family).bearSnapShort?.suppressed_count ?? 0), tone: Number(asRecord(signalSelectivityAntiChurn.suppression_by_family).bearSnapShort?.suppressed_count ?? 0) > 0 ? "warn" : "muted" },
    { label: "Bear Snap Range Ladder", value: formatValue(signalSelectivityRangeLadder.summary_line ?? "Unavailable"), tone: signalSelectivityRangeLadder.available === true && signalSelectivityRangeLadder.recommended_value ? "good" : signalSelectivityRangeLadder.available === true ? "warn" : "muted" },
    { label: "Recommended Range ATR", value: formatValue(signalSelectivityRangeLadder.recommended_value ?? "Unavailable"), tone: signalSelectivityRangeLadder.recommended_value ? "good" : "muted" },
    { label: "Next Short Blocker", value: formatValue(signalSelectivityRangeLadder.next_dominant_blocker_after_recommended ?? "Unavailable"), tone: signalSelectivityRangeLadder.next_dominant_blocker_after_recommended ? "warn" : "muted" },
    { label: "Bear Snap Stretch Ladder", value: formatValue(signalSelectivityStretchLadder.summary_line ?? "Unavailable"), tone: signalSelectivityStretchLadder.available === true ? "muted" : "muted" },
  ];
  const paperBrokerTruthShadowSummary = asRecord(paperBrokerTruthShadowValidation.summary);
  const paperBrokerTruthShadowValidations = asRecord(paperBrokerTruthShadowValidation.validations);
  const paperBrokerTruthShadowSchemas = asRecord(paperBrokerTruthShadowValidation.schemas);
  const paperBrokerTruthShadowComponentRows: JsonRecord[] = ["order_status", "open_orders", "position", "account_health"].map((key) => {
    const validation = asRecord(paperBrokerTruthShadowValidations[key]);
    const schema = asRecord(paperBrokerTruthShadowSchemas[key]);
    return {
      schema_name: key,
      classification: validation.classification ?? "UNKNOWN",
      issues: asArray(validation.issues).map((value) => String(value)).join(", ") || "none",
      required_count: asArray(schema.required_fields).length,
      optional_count: asArray(schema.optional_fields).length,
    };
  });
  const paperBrokerTruthShadowMetrics: Array<{ label: string; value: string; tone?: Tone }> = [
    { label: "Validation Result", value: formatValue(paperBrokerTruthShadowSummary.result ?? "UNAVAILABLE"), tone: statusTone(paperBrokerTruthShadowSummary.result) },
    { label: "Overall Classification", value: formatValue(paperBrokerTruthShadowSummary.overall_classification ?? "UNKNOWN"), tone: statusTone(paperBrokerTruthShadowSummary.overall_classification) },
    { label: "Selected Account", value: formatValue(paperBrokerTruthShadowValidation.selected_account_hash ?? "Unavailable") },
    { label: "Representative Order", value: formatValue(paperBrokerTruthShadowSummary.representative_broker_order_id ?? "None"), tone: paperBrokerTruthShadowSummary.representative_broker_order_id ? "good" : "muted" },
    { label: "Order Status", value: formatValue(asRecord(paperBrokerTruthShadowValidations.order_status).classification ?? "UNKNOWN"), tone: statusTone(asRecord(paperBrokerTruthShadowValidations.order_status).classification) },
    { label: "Open Orders", value: formatValue(asRecord(paperBrokerTruthShadowValidations.open_orders).classification ?? "UNKNOWN"), tone: statusTone(asRecord(paperBrokerTruthShadowValidations.open_orders).classification) },
    { label: "Position", value: formatValue(asRecord(paperBrokerTruthShadowValidations.position).classification ?? "UNKNOWN"), tone: statusTone(asRecord(paperBrokerTruthShadowValidations.position).classification) },
    { label: "Account Health", value: formatValue(asRecord(paperBrokerTruthShadowValidations.account_health).classification ?? "UNKNOWN"), tone: statusTone(asRecord(paperBrokerTruthShadowValidations.account_health).classification) },
  ];
  const paperLiveTimingBrokerTruth = asRecord(paperLiveTimingSummary.broker_truth);
  const paperLiveTimingPositionState = asRecord(paperLiveTimingSummary.position_state);
  const paperLiveTimingMetrics: Array<{ label: string; value: string; tone?: Tone }> = [
    { label: "Runtime Phase", value: formatValue(paperLiveTimingSummary.runtime_phase ?? "UNKNOWN"), tone: statusTone(paperLiveTimingSummary.runtime_phase) },
    { label: "Strategy State", value: formatValue(paperLiveTimingSummary.strategy_state ?? "UNKNOWN"), tone: statusTone(paperLiveTimingSummary.strategy_state) },
    { label: "Pending Stage", value: formatValue(paperLiveTimingSummary.pending_stage ?? "IDLE"), tone: statusTone(paperLiveTimingSummary.pending_stage) },
    { label: "Last Evaluated Bar", value: formatValue(paperLiveTimingSummary.evaluated_bar_id ?? "Unavailable") },
    { label: "Position State", value: `${formatValue(paperLiveTimingPositionState.side ?? "UNKNOWN")} / ${formatValue(paperLiveTimingPositionState.internal_qty ?? 0)}`, tone: statusTone(paperLiveTimingPositionState.side) },
    { label: "Ack Timestamp", value: formatTimestamp(paperLiveTimingSummary.broker_ack_at), tone: paperLiveTimingSummary.broker_ack_at ? "good" : "muted" },
    { label: "Fill Timestamp", value: formatTimestamp(paperLiveTimingSummary.broker_fill_at), tone: paperLiveTimingSummary.broker_fill_at ? "good" : "muted" },
    { label: "Entry Blocker", value: formatValue(paperLiveTimingSummary.entries_disabled_blocker ?? "None"), tone: paperLiveTimingSummary.entries_disabled_blocker ? "warn" : "good" },
  ];
  const paperLiveTimingValidationSummary = asRecord(paperLiveTimingValidation.summary);
  const paperLiveTimingValidationScenarioRows = asArray<JsonRecord>(paperLiveTimingValidation.scenario_rows);
  const paperLiveTimingValidationMetrics: Array<{ label: string; value: string; tone?: Tone }> = [
    { label: "Validation Result", value: formatValue(paperLiveTimingValidationSummary.result ?? "UNAVAILABLE"), tone: statusTone(paperLiveTimingValidationSummary.result) },
    { label: "Scenarios Passed", value: `${formatShortNumber(paperLiveTimingValidationSummary.passed_count ?? 0)}/${formatShortNumber(paperLiveTimingValidationSummary.scenario_count ?? 0)}`, tone: Number(paperLiveTimingValidationSummary.passed_count ?? 0) === Number(paperLiveTimingValidationSummary.scenario_count ?? 0) && Number(paperLiveTimingValidationSummary.scenario_count ?? 0) > 0 ? "good" : "warn" },
    { label: "Final Phase", value: formatValue(paperLiveTimingValidationSummary.final_runtime_phase ?? "UNKNOWN"), tone: statusTone(paperLiveTimingValidationSummary.final_runtime_phase) },
    { label: "Final Stage", value: formatValue(paperLiveTimingValidationSummary.final_pending_stage ?? "UNKNOWN"), tone: statusTone(paperLiveTimingValidationSummary.final_pending_stage) },
    { label: "Final State", value: formatValue(paperLiveTimingValidationSummary.final_strategy_state ?? "UNKNOWN"), tone: statusTone(paperLiveTimingValidationSummary.final_strategy_state) },
    { label: "Final Blocker", value: formatValue(paperLiveTimingValidationSummary.final_blocker ?? "None"), tone: paperLiveTimingValidationSummary.final_blocker ? "warn" : "good" },
  ];
  const paperSoakValidationSummary = asRecord(paperSoakValidation.summary);
  const paperSoakValidationScenarioRows = asArray<JsonRecord>(paperSoakValidation.scenario_rows);
  const paperSoakValidationAvailable = paperSoakValidation.available === true;
  const paperSoakValidationMarketDataHealth = asRecord(paperSoakValidationSummary.market_data_health);
  const paperSoakValidationTimeoutWatchdog = asRecord(paperSoakValidationSummary.latest_order_timeout_watchdog);
  const paperSoakValidationMetrics: Array<{ label: string; value: string; tone?: Tone }> = [
    { label: "Validation Result", value: formatValue(paperSoakValidationSummary.result ?? "UNAVAILABLE"), tone: statusTone(paperSoakValidationSummary.result) },
    { label: "Scenarios Passed", value: `${formatShortNumber(paperSoakValidationSummary.passed_count ?? 0)}/${formatShortNumber(paperSoakValidationSummary.scenario_count ?? 0)}`, tone: Number(paperSoakValidationSummary.failed_count ?? 0) > 0 ? "danger" : paperSoakValidationAvailable ? "good" : "muted" },
    { label: "Validation Phase", value: formatValue(paperSoakValidationSummary.runtime_phase ?? "UNKNOWN"), tone: statusTone(paperSoakValidationSummary.runtime_phase) },
    { label: "Validation State", value: formatValue(paperSoakValidationSummary.strategy_state ?? "UNKNOWN"), tone: statusTone(paperSoakValidationSummary.strategy_state) },
    { label: "Last Bar", value: formatValue(paperSoakValidationSummary.last_processed_bar_id ?? "Unavailable") },
    { label: "Market Data Health", value: paperSoakValidationMarketDataHealth.market_data_ok === false ? "DEGRADED" : "HEALTHY", tone: paperSoakValidationMarketDataHealth.market_data_ok === false ? "warn" : "good" },
    { label: "Timeout Watchdog", value: formatValue(paperSoakValidationTimeoutWatchdog.status ?? "IDLE"), tone: statusTone(paperSoakValidationTimeoutWatchdog.status) },
    { label: "Entry Blocker", value: formatValue(paperSoakValidationSummary.entries_disabled_blocker ?? "None"), tone: paperSoakValidationSummary.entries_disabled_blocker ? "warn" : "good" },
  ];
  const paperSoakExtendedSummary = asRecord(paperSoakExtended.summary);
  const paperSoakExtendedCheckpointRows = asArray<JsonRecord>(paperSoakExtended.checkpoint_rows);
  const paperSoakExtendedAvailable = paperSoakExtended.available === true;
  const paperSoakExtendedMetrics: Array<{ label: string; value: string; tone?: Tone }> = [
    { label: "Extended Result", value: formatValue(paperSoakExtendedSummary.result ?? "UNAVAILABLE"), tone: statusTone(paperSoakExtendedSummary.result) },
    { label: "Bars Processed", value: formatValue(paperSoakExtendedSummary.bars_processed ?? 0) },
    { label: "Restart Count", value: formatValue(paperSoakExtendedSummary.restart_count ?? 0), tone: Number(paperSoakExtendedSummary.restart_count ?? 0) > 0 ? "warn" : "muted" },
    { label: "Drift Detected", value: paperSoakExtendedSummary.drift_detected ? "YES" : "NO", tone: paperSoakExtendedSummary.drift_detected ? "danger" : paperSoakExtendedAvailable ? "good" : "muted" },
    { label: "Final Phase", value: formatValue(paperSoakExtendedSummary.final_runtime_phase ?? "UNKNOWN"), tone: statusTone(paperSoakExtendedSummary.final_runtime_phase) },
    { label: "Final State", value: formatValue(paperSoakExtendedSummary.final_strategy_state ?? "UNKNOWN"), tone: statusTone(paperSoakExtendedSummary.final_strategy_state) },
    { label: "Final Blocker", value: formatValue(paperSoakExtendedSummary.final_entry_blocker ?? "None"), tone: paperSoakExtendedSummary.final_entry_blocker ? "warn" : "good" },
  ];
  const paperSoakUnattendedSummary = asRecord(paperSoakUnattended.summary);
  const paperSoakUnattendedCheckpointRows = asArray<JsonRecord>(paperSoakUnattended.checkpoint_rows);
  const paperSoakUnattendedAvailable = paperSoakUnattended.available === true;
  const paperSoakUnattendedMetrics: Array<{ label: string; value: string; tone?: Tone }> = [
    { label: "Unattended Result", value: formatValue(paperSoakUnattendedSummary.result ?? "UNAVAILABLE"), tone: statusTone(paperSoakUnattendedSummary.result) },
    { label: "Bars Processed", value: formatValue(paperSoakUnattendedSummary.bars_processed ?? 0) },
    { label: "Duration", value: `${formatShortNumber(paperSoakUnattendedSummary.runtime_duration_minutes ?? 0)}m` },
    { label: "Restart Count", value: formatValue(paperSoakUnattendedSummary.restart_count ?? 0), tone: Number(paperSoakUnattendedSummary.restart_count ?? 0) > 0 ? "warn" : "muted" },
    { label: "Drift Detected", value: paperSoakUnattendedSummary.drift_detected ? "YES" : "NO", tone: paperSoakUnattendedSummary.drift_detected ? "danger" : paperSoakUnattendedAvailable ? "good" : "muted" },
    { label: "Final Phase", value: formatValue(paperSoakUnattendedSummary.final_runtime_phase ?? "UNKNOWN"), tone: statusTone(paperSoakUnattendedSummary.final_runtime_phase) },
    { label: "Final State", value: formatValue(paperSoakUnattendedSummary.final_strategy_state ?? "UNKNOWN"), tone: statusTone(paperSoakUnattendedSummary.final_strategy_state) },
    { label: "Final Blocker", value: formatValue(paperSoakUnattendedSummary.final_entry_blocker ?? "None"), tone: paperSoakUnattendedSummary.final_entry_blocker ? "warn" : "good" },
  ];
  const exitParityDecision = asRecord(paperExitParitySummary.latest_exit_decision);
  const exitParityStopRefs = asRecord(paperExitParitySummary.stop_refs);
  const exitParityBreakEven = asRecord(paperExitParitySummary.break_even);
  const paperExitParityMetrics: Array<{ label: string; value: string; tone?: Tone }> = [
    { label: "Position Family", value: formatValue(paperExitParitySummary.current_position_family ?? "NONE"), tone: statusTone(paperExitParitySummary.current_position_family) },
    { label: "Primary Exit Reason", value: formatValue(exitParityDecision.primary_reason ?? "NONE"), tone: statusTone(exitParityDecision.primary_reason) },
    { label: "All True Reasons", value: asArray(exitParityDecision.all_true_reasons).length ? asArray(exitParityDecision.all_true_reasons).map((value) => String(value)).join(", ") : "None" },
    { label: "Exit Fill Pending", value: paperExitParitySummary.exit_fill_pending === true ? "YES" : "NO", tone: paperExitParitySummary.exit_fill_pending === true ? "warn" : "good" },
    { label: "Exit Fill Confirmed", value: paperExitParitySummary.exit_fill_confirmed === true ? "YES" : "NO", tone: paperExitParitySummary.exit_fill_confirmed === true ? "good" : "muted" },
    { label: "Restore Result", value: formatValue(paperExitParitySummary.latest_restore_result ?? "Unknown"), tone: statusTone(paperExitParitySummary.latest_restore_result) },
  ];

  const readinessCards: Array<{ label: string; value: unknown; tone?: Tone }> = [
    { label: "Runtime Status", value: runtimeReadiness.values?.runtime_status ?? global.runtime_health_label ?? "Unknown", tone: statusTone(runtimeReadiness.values?.runtime_status ?? global.runtime_health_label) },
    { label: "Entries Enabled", value: global.entries_enabled ?? runtimeValues.entries_enabled ?? paperReadiness.entries_enabled, tone: statusTone(global.entries_enabled ?? runtimeValues.entries_enabled ?? paperReadiness.entries_enabled) },
    { label: "Operator Halt", value: inferOperatorHalt(global, paperReadiness), tone: statusTone(inferOperatorHalt(global, paperReadiness)) },
    { label: "Broker / Auth", value: global.auth_label ?? runtimeValues.auth_readiness, tone: statusTone(global.auth_label ?? runtimeValues.auth_readiness) },
    { label: "Market Data", value: global.market_data_label ?? runtimeValues.market_data_readiness, tone: statusTone(global.market_data_label ?? runtimeValues.market_data_readiness) },
    { label: "Runtime Recovery", value: runtimeValues.runtime_recovery_state ?? paperRuntimeRecoveryState ?? "Unknown", tone: statusTone(runtimeValues.runtime_recovery_state ?? paperRuntimeRecoveryState) },
    { label: "Restart Budget", value: `${paperRuntimeRestartAttemptsInWindow}/${paperRuntimeRestartBudget || "?"}`, tone: paperRuntimeRestartSuppressed ? "danger" : paperRuntimeRestartAttemptsInWindow > 0 ? "warn" : "good" },
    { label: "Auto-Restart", value: paperRuntimeRestartSuppressed ? "SUPPRESSED" : paperAutoRestartAllowed ? "ALLOWED" : paperRuntimeRecovery.manual_action_required === true ? "MANUAL ONLY" : "IDLE", tone: paperRuntimeRestartSuppressed ? "danger" : paperAutoRestartAllowed ? "good" : paperRuntimeRecovery.manual_action_required === true ? "warn" : "muted" },
    { label: "Loaded In Runtime", value: String(readinessLaneStatusSummary.loaded_in_runtime_count ?? 0), tone: Number(readinessLaneStatusSummary.loaded_in_runtime_count ?? 0) > 0 ? "good" : "warn" },
    { label: "Tradable Now", value: String(readinessLaneStatusSummary.eligible_to_trade_count ?? 0), tone: Number(readinessLaneStatusSummary.eligible_to_trade_count ?? 0) > 0 ? "good" : "warn" },
    { label: "Loaded, Not Eligible", value: String(loadedNotEligibleRows.length), tone: loadedNotEligibleRows.length ? "warn" : "good" },
    { label: "True Faults", value: String(runtimeBlockingFaultRows.length), tone: runtimeBlockingFaultRows.length ? "danger" : "good" },
    { label: "Info Feed Degradation", value: String(readinessDegradedFeeds.length), tone: readinessDegradedFeeds.length ? "warn" : "good" },
    { label: "Lane Risk Halts", value: String(haltedDegradationRows.length), tone: haltedDegradationRows.length ? "warn" : "good" },
    { label: "Reconciling", value: String(reconcilingLaneRows.length), tone: reconcilingLaneRows.length ? "danger" : "good" },
    { label: "Heartbeat Reconcile", value: formatValue(heartbeatLastStatus), tone: heartbeatReconciliationTone(heartbeatLastStatus) },
    { label: "Pending Order Health", value: formatValue(orderTimeoutLastStatus), tone: orderTimeoutWatchdogTone(orderTimeoutLastStatus) },
    { label: "Informational Only", value: String(informationalOnlyLaneRows.length), tone: informationalOnlyLaneRows.length ? "muted" : "good" },
    { label: "Backend / API", value: desktopState?.backend.label ?? global.runtime_health_label ?? runtimeValues.runtime_status, tone: statusTone(desktopState?.backend.label ?? global.runtime_health_label ?? runtimeValues.runtime_status) },
    { label: "Current Session", value: paperReadiness.current_detected_session ?? paperReadiness.runtime_phase ?? global.current_session_date ?? "Unknown", tone: statusTone(paperReadiness.current_detected_session ?? paperReadiness.runtime_phase ?? global.current_session_date) },
    { label: "Last Refresh", value: formatRelativeAge(desktopState?.refreshedAt), tone: "muted" as const },
  ];
  const showGlobalStatusBanner = !PRIMARY_WORKSTATION_PAGES.has(page);
  const showGlobalCommandStrip = !PRIMARY_WORKSTATION_PAGES.has(page);
  const showWorkspaceContextBar = false;
  const showPrimaryCommandResult = page === "home" || page === "market" || page === "diagnostics";

  currentSectionPageContext = page;

  return (
    <div className="operator-app">
      <aside className="sidebar">
        <div className="brand-block">
          <div className="brand-line">
            <div className="brand-title">MGC Trade Engine</div>
            <div className="brand-version">{desktopState?.appVersion ?? "v0.51"}</div>
          </div>
          <div className="brand-meta-row">
            <Badge label={desktopState?.backend.healthStatus === "ok" ? "Connected" : desktopState?.backend.label ?? "Disconnected"} tone={statusTone(desktopState?.backend.healthStatus === "ok" ? "ready" : desktopState?.backend.label)} />
            <span className="brand-eyebrow">Shared Paper API</span>
            <span className="ticker-symbol">{headerSymbol}</span>
            <span className="ticker-price">{formatValue(headerLastPrice)}</span>
            <span className={`ticker-change ${headerNetChangeTone}`}>{formatValue(headerNetChange)}</span>
            <span className="ticker-time">{clock.toLocaleTimeString()}</span>
          </div>
        </div>
        <nav className="nav-stack">
          {primaryNavItems.map((item) => (
            <button
              key={item.id}
              data-page={item.id}
              className={`nav-item ${page === item.id ? "active" : ""}`}
              onClick={() => {
                window.location.hash = `#/${item.id}`;
                setPage(item.id);
              }}
            >
              <span>{item.label}</span>
            </button>
          ))}
        </nav>
        <div className="sidebar-footer">
          <div className="header-utility-links">
            {utilityNavItems.map((item) => (
              <button
                key={item.id}
                data-page={item.id}
                className={`utility-nav-item ${page === item.id ? "active" : ""}`}
                onClick={() => {
                  window.location.hash = `#/${item.id}`;
                  setPage(item.id);
                }}
              >
                {item.label}
              </button>
            ))}
          </div>
          <div className="header-chip-row">
            <MetricMini label="Mode" value={global.mode_label ?? "Loading"} tone={statusTone(global.mode_label)} />
            <MetricMini label="Source" value={desktopState?.source.label ?? "Loading"} tone={statusTone(desktopState?.source.label)} />
            <MetricMini label="Session" value={paperReadiness.current_detected_session ?? "Unknown"} />
          </div>
          <button
            className="danger-button"
            disabled={busyAction !== null || !canRunLiveActions}
            onClick={() =>
              void runCommand("paper-halt-entries", () => api.runDashboardAction("paper-halt-entries"), {
                confirmMessage: "Emergency Halt will stop new paper entries immediately. Proceed?",
                requiresLive: true,
              })
            }
          >
            {busyAction === "paper-halt-entries" ? "Halting..." : "Emergency Halt"}
          </button>
        </div>
      </aside>

      <div className="shell">
        <header className="topbar">
          <div className="topbar-copy">
            <div className="page-eyebrow">{pageTitle(page)}</div>
            <h1 className="page-title">{pageTitle(page)}</h1>
          </div>
          <div className="topbar-status">
            <Badge label={desktopState?.backend.healthStatus === "ok" ? "Backend Live" : desktopState?.backend.label ?? "Unknown"} tone={statusTone(desktopState?.backend.healthStatus === "ok" ? "ready" : desktopState?.backend.label)} />
            <Badge label={authReadyForPaperStartup ? "Auth Ready" : "Auth Blocked"} tone={authReadyForPaperStartup ? "good" : "warn"} />
            <Badge label={tempPaperMismatchActive ? "Temp Paper Blocked" : "Temp Paper Clear"} tone={tempPaperMismatchActive ? "warn" : "good"} />
            <div className="time-card">
              <div className="time-label">Session Time</div>
              <div className="time-value">{clock.toLocaleString()}</div>
            </div>
          </div>
        </header>

        {showGlobalStatusBanner ? (
        <section className={`status-banner ${statusTone(desktopState?.source.label ?? desktopState?.backend.label)}`}>
          <div className="status-banner-main">
            <div className="status-banner-title">
              {desktopState?.source.label ?? "Loading transport state"}
              <span className="status-banner-separator">|</span>
              {desktopState?.backend.label ?? "Waiting for backend state"}
            </div>
            <div className="status-banner-body">{desktopState?.source.detail ?? "Loading operator source-of-truth state."}</div>
            <div className="status-banner-body secondary">{desktopState?.backend.detail ?? "Backend state detail is unavailable."}</div>
            {desktopState?.backend.lastError ? <div className="status-banner-error">Latest error: {desktopState.backend.lastError}</div> : null}
            {desktopState?.startup.recommendedAction || desktopState?.backend.actionHint ? (
              <div className="status-banner-warning">Next step: {desktopState?.startup.recommendedAction ?? desktopState?.backend.actionHint}</div>
            ) : null}
            {!canRunLiveActions ? (
              <div className="status-banner-warning">Live operator actions are locked until the app is back on the live dashboard API.</div>
            ) : null}
          </div>
          <div className="status-banner-meta">
            <MetricMini label="Backend URL" value={desktopState?.backendUrl ?? "Unavailable"} />
            <MetricMini label="Chosen Port" value={formatValue(startup?.chosenPort)} />
            <MetricMini label="Ownership" value={ownershipLabel(startup?.ownership)} />
            <MetricMini label="PID" value={formatValue(desktopState?.backend.pid)} />
            <MetricMini label="API Status" value={formatValue(desktopState?.backend.apiStatus)} tone={statusTone(desktopState?.backend.apiStatus)} />
            <MetricMini label="Health" value={formatValue(desktopState?.backend.healthStatus)} tone={statusTone(desktopState?.backend.healthStatus)} />
            <div className="banner-actions">
              <button className="panel-button subtle" disabled={busyAction !== null} onClick={() => void manualRefresh()}>
                {busyAction === "refresh" ? "Refreshing..." : "Refresh"}
              </button>
              <button
                className="panel-button subtle"
                disabled={busyAction !== null || !desktopState?.desktopLogPath}
                onClick={() => void runCommand("open-desktop-log", () => api.openPath(desktopState?.desktopLogPath ?? ""))}
              >
                Electron Log
              </button>
              <button className="panel-button subtle" disabled={busyAction !== null} onClick={() => void runCommand("copy-diagnostics", () => api.copyText(diagnosticsSummary))}>
                Copy Diagnostics
              </button>
            </div>
          </div>
        </section>
        ) : null}

        {showGlobalCommandStrip ? (
        <div className="action-row">
          <button className="panel-button" disabled={busyAction !== null} onClick={() => void manualRefresh()}>
            Refresh
          </button>
          <button className="panel-button" disabled={busyAction !== null || desktopState?.backend.state === "starting"} onClick={() => void runCommand("start-dashboard", () => api.startDashboard())}>
            {busyAction === "start-dashboard" ? "Starting..." : "Start Dashboard/API"}
          </button>
          <button
            className="panel-button"
            disabled={busyAction !== null || desktopState?.backend.state === "starting" || desktopState?.backend.state === "reconnecting"}
            onClick={() =>
              void runCommand("restart-dashboard", () => api.restartDashboard(), {
                confirmMessage: "Restart Dashboard/API will interrupt live operator refresh briefly. Proceed?",
              })
            }
          >
            {busyAction === "restart-dashboard" ? "Restarting..." : "Restart Dashboard/API"}
          </button>
          <button
            className="panel-button"
            disabled={busyAction !== null || !canRunLiveActions}
            onClick={() => void runCommand("start-paper", () => api.runDashboardAction("start-paper"), { requiresLive: true })}
          >
            {busyAction === "start-paper" ? "Starting..." : "Start Runtime"}
          </button>
          <button
            className="panel-button"
            disabled={busyAction !== null || !canRunLiveActions}
            onClick={() =>
              void runCommand("restart-paper-with-temp-paper", () => api.runDashboardAction("restart-paper-with-temp-paper"), {
                confirmMessage: "Restart Runtime + Temp Paper will restart the paper soak with all enabled temporary paper lanes included. Proceed?",
                requiresLive: true,
              })
            }
          >
            {busyAction === "restart-paper-with-temp-paper" ? "Restarting..." : "Restart Runtime + Temp Paper"}
          </button>
          <button
            className="panel-button"
            disabled={busyAction !== null || !canRunLiveActions}
            onClick={() =>
              void runCommand("stop-paper", () => api.runDashboardAction("stop-paper"), {
                confirmMessage: "Stop Runtime will stop the current paper runtime. Proceed?",
                requiresLive: true,
              })
            }
          >
            {busyAction === "stop-paper" ? "Stopping..." : "Stop Runtime"}
          </button>
          <button
            className="panel-button"
            disabled={busyAction !== null || !canRunLiveActions}
            onClick={() => void runCommand("auth-gate-check", () => api.runDashboardAction("auth-gate-check"), { requiresLive: true })}
          >
            {busyAction === "auth-gate-check" ? "Checking..." : "Auth Gate Check"}
          </button>
          {busyAction ? <span className="busy-indicator">Working: {busyAction}</span> : null}
        </div>
        ) : null}

        {showWorkspaceContextBar ? (
        <section className="workspace-context-bar">
          <div className="workspace-context-copy">
            <div className="workspace-context-kicker">Selected Workspace</div>
            <div className="workspace-context-title">{compactBranchLabel(selectedWorkspaceRow)}</div>
            <div className="workspace-context-meta">
              <Badge label={laneClassLabel(selectedWorkspaceRow)} tone={paperStrategyClassTone(selectedWorkspaceRow)} />
              <Badge label={selectedWorkspaceDesignation} tone={selectedWorkspaceRow?.benchmark_designation ? "warn" : selectedWorkspaceRow?.candidate_designation ? "good" : "muted"} />
              <Badge label={selectedWorkspaceRuntimeLabel} tone={selectedWorkspaceRuntimeLabel === "Attached Live" ? "good" : selectedWorkspaceRuntimeLabel === "Audit Only" ? "warn" : "muted"} />
              <span>{selectedWorkspaceInstrument || "No instrument"}</span>
              <span>Exec {formatValue(selectedWorkspaceRow?.execution_timeframe ?? "1m")}</span>
              <span>Context {selectedWorkspaceContextTimes}</span>
              <span>Last eval {formatTimestamp(selectedWorkspaceRow?.last_execution_bar_evaluated_at)}</span>
            </div>
          </div>
          <div className="workspace-context-actions">
            <button className={`panel-button subtle ${page === "strategies" ? "active-pill" : ""}`} onClick={() => selectWorkspaceLane(String(selectedWorkspaceRow?.lane_id ?? ""), { navigateTo: "strategies" })}>
              Open Deep-Dive
            </button>
            <button className={`panel-button subtle ${page === "market" ? "active-pill" : ""}`} onClick={() => selectWorkspaceLane(String(selectedWorkspaceRow?.lane_id ?? ""), { navigateTo: "market" })}>
              Open Trade Entry
            </button>
            <button
              className="panel-button subtle"
              onClick={() => {
                window.location.hash = "#/diagnostics";
                setPage("diagnostics");
              }}
            >
              Open Evidence
            </button>
          </div>
        </section>
        ) : null}

        {desktopState?.errors?.length ? (
          <section className="notice-strip">
            {desktopState.errors.map((error) => (
              <div key={error} className="notice-item">
                {error}
              </div>
            ))}
          </section>
        ) : null}

        {lastResult && showPrimaryCommandResult ? (
          <section className={`command-result ${lastResult.ok ? "success" : "failure"}`}>
            <div className="command-result-title">{lastResult.message}</div>
            {lastResult.detail ? <div className="command-result-body">{lastResult.detail}</div> : null}
            {lastResult.output ? <pre className="command-output">{lastResult.output}</pre> : null}
          </section>
        ) : null}

        {page === "diagnostics" ? (
        <section className="recent-actions-card">
          <div className="section-header">
            <div>
              <div className="section-title small">Recent Actions</div>
              <div className="section-subtitle">Local operator actions and feedback from this desktop session</div>
            </div>
          </div>
          {recentActions.length ? (
            <div className="recent-actions-list">
              {recentActions.map((action) => (
                <div key={action.id} className={`recent-action ${action.ok ? "good" : "danger"}`}>
                  <div className="recent-action-header">
                    <span className="recent-action-label">{action.label}</span>
                    <span className="recent-action-time">{formatRelativeAge(action.occurredAt)}</span>
                  </div>
                  <div className="recent-action-message">{action.message}</div>
                  {action.detail ? <div className="recent-action-detail">{action.detail}</div> : null}
                </div>
              ))}
            </div>
          ) : (
            <div className="placeholder-note">No operator actions have been run in this desktop session yet.</div>
          )}
        </section>
        ) : null}

        <main className="content">
          {loading ? <Section title="Loading">Loading operator state…</Section> : null}

          {!loading && page === "home" ? (
            <>
              <Section
                title="Control Center"
                subtitle="Shared-paper runtime control, operating posture, and strategy-universe summary"
                className="dashboard-command-center"
                headerClassName="section-header-tight"
              >
                <div className="dashboard-command-grid">
                  <div className="dashboard-command-column">
                    <div className="process-control-grid">
                      {processControlCards.map((card) => (
                        <button
                          key={card.label}
                          className={`process-control-card ${card.tone}`}
                          disabled={card.disabled}
                          onClick={card.onClick}
                        >
                          <span className={`process-dot ${card.tone}`} />
                          <span className="process-control-label">{card.label}</span>
                          <span className="process-control-status">{card.status}</span>
                        </button>
                      ))}
                    </div>
                    <div className="action-row inline dashboard-primary-actions">
                      <ControlButton
                        label="Start Runtime"
                        onClick={() => void runCommand("start-paper", () => api.runDashboardAction("start-paper"), { requiresLive: true })}
                        busyAction={busyAction}
                        disabled={!canRunLiveActions}
                      />
                      <ControlButton
                        label="Restart Runtime + Temp Paper"
                        onClick={() =>
                          void runCommand("restart-paper-with-temp-paper", () => api.runDashboardAction("restart-paper-with-temp-paper"), {
                            confirmMessage: "Restart Runtime + Temp Paper will restart the paper soak with all enabled temporary paper lanes included. Proceed?",
                            requiresLive: true,
                          })
                        }
                        busyAction={busyAction}
                        disabled={!canRunLiveActions}
                      />
                      <ControlButton
                        label="Auth Gate Check"
                        onClick={() => void runCommand("auth-gate-check", () => api.runDashboardAction("auth-gate-check"), { requiresLive: true })}
                        busyAction={busyAction}
                        disabled={!canRunLiveActions}
                      />
                    </div>
                  </div>
                  <div className="dashboard-command-column">
                    <div className="metric-grid dashboard-overview-grid">
                      <MetricCard label="Runtime Mode" value={formatValue(global.mode_label ?? runtimeValues.runtime_status ?? "Unknown")} tone={statusTone(global.mode_label ?? runtimeValues.runtime_status)} />
                      <MetricCard label="Paper Runtime" value={formatValue(paperReadiness.runtime_phase ?? "STOPPED")} tone={statusTone(paperReadiness.runtime_phase)} />
                      <MetricCard label="Auth" value={authReadyForPaperStartup ? "READY" : "NOT READY"} tone={authReadyForPaperStartup ? "good" : "warn"} />
                      <MetricCard label="Entries Enabled" value={formatValue(global.entries_enabled ?? paperReadiness.entries_enabled)} tone={statusTone(global.entries_enabled ?? paperReadiness.entries_enabled)} />
                      <MetricCard label="Execution" value="1m decision surface" tone="good" />
                      <MetricCard label="Context" value="Completed 5m context" tone="muted" />
                      <MetricCard label="ATP Bench" value={formatShortNumber(atpBenchmarkRows.length)} tone={atpBenchmarkRows.length ? "warn" : "muted"} />
                      <MetricCard label="ATP Cand" value={formatShortNumber(atpCandidateRows.length)} tone={atpCandidateRows.length ? "good" : "muted"} />
                    </div>
                    <div className="status-chip-row dashboard-status-chip-row">
                      <Badge label={`LIVE ${rosterSummaryCounts.live}`} tone="good" />
                      <Badge label={`PAPER ${rosterSummaryCounts.paper}`} tone="muted" />
                      <Badge label={`CAND ${rosterSummaryCounts.candidate}`} tone="warn" />
                      <Badge label={`PAUSED ${rosterSummaryCounts.paused}`} tone="muted" />
                      <Badge label={tempPaperMismatchActive ? "TEMP PAPER BLOCKED" : "TEMP PAPER CLEAR"} tone={tempPaperMismatchActive ? "danger" : "good"} />
                    </div>
                  </div>
                </div>
              </Section>

              <Section
                title={`Strategy Roster — ${dashboardRosterRows.length} Lanes`}
                subtitle="Benchmark, candidate, temporary, and admitted lanes in a scan-first grid. Click any card to jump into Strategy Deep-Dive."
                className="dashboard-roster-section"
                headerClassName="section-header-tight"
              >
                <div className="strategy-roster-grid">
                  {dashboardRosterRows.map((row) => {
                    const performance = strategyPerformanceRows.find((candidate) => String(candidate.lane_id ?? "") === String(row.lane_id ?? "")) ?? null;
                    const pnlValue = Number(
                      performance?.day_pnl ?? performance?.realized_pnl ?? row.realized_pnl ?? row.unrealized_pnl ?? 0,
                    ) || 0;
                    return (
                      <button
                        key={String(row.lane_id ?? row.branch)}
                        className={`strategy-card strategy-card-${rosterCardAccentClass(row)} ${String(selectedWorkspaceLaneId) === String(row.lane_id ?? "") ? "active" : ""}`}
                        onClick={() => {
                          selectWorkspaceLane(String(row.lane_id ?? ""), { navigateTo: "strategies" });
                        }}
                      >
                        <div className="strategy-card-header">
                          <div className="strategy-card-kicker">{formatValue(row.instrument ?? performance?.instrument ?? "Lane")}</div>
                          <Badge label={rosterStatusChip(row)} tone={rosterStatusTone(row)} />
                        </div>
                        <div className="strategy-card-title">{compactBranchLabel(row)}</div>
                        <div className={`strategy-card-pnl ${pnlTone(pnlValue)}`}>{renderPnlValue(pnlValue)}</div>
                        <div className="strategy-card-meta">
                          <span>{designationLabel(row)}</span>
                          <span>{stagedPostureLabel(row)}</span>
                        </div>
                        <div className="strategy-card-footer">
                          <span>{cadenceLabel(row)}</span>
                          <span>{runtimeAttachmentLabel(row)}</span>
                        </div>
                      </button>
                    );
                  })}
                </div>
              </Section>

              <Section title="Strategy Roster Table" subtitle="Dense secondary table retained for scan-heavy operator workflows and audit cross-checks">
                <DataTable
                  columns={[
                    { key: "branch", label: "Lane", render: (row) => formatValue(row.branch ?? row.display_name ?? row.lane_id) },
                    { key: "lane_class_label", label: "Class", render: (row) => <Badge label={laneClassLabel(row)} tone={paperStrategyClassTone(row)} /> },
                    { key: "designation_label", label: "Designation", render: (row) => <Badge label={designationLabel(row)} tone={row.benchmark_designation ? "warn" : row.candidate_designation ? "good" : "muted"} /> },
                    { key: "strategy_status", label: "Status", render: (row) => <Badge label={formatValue(row.strategy_status ?? row.current_strategy_status ?? "Unknown")} tone={statusTone(row.strategy_status ?? row.current_strategy_status)} /> },
                    { key: "participation_policy", label: "Participation", render: (row) => formatValue(row.participation_policy ?? "Unavailable") },
                    { key: "posture", label: "Current Posture", render: (row) => stagedPostureLabel(row) },
                    { key: "cadence", label: "Cadence", render: (row) => cadenceLabel(row) },
                    { key: "runtime_attachment", label: "Runtime", render: (row) => <Badge label={runtimeAttachmentLabel(row)} tone={runtimeAttachmentLabel(row) === "Attached Live" ? "good" : runtimeAttachmentLabel(row) === "Audit Only" ? "warn" : "muted"} /> },
                    { key: "last_execution_bar_evaluated_at", label: "Last 1m Eval", render: (row) => formatTimestamp(row.last_execution_bar_evaluated_at) },
                    {
                      key: "detail",
                      label: "Detail",
                      render: (row) => (
                        <button
                          className="panel-button subtle"
                          onClick={() => {
                            setStrategyLensIdentityFilter(standaloneStrategyId(row));
                            selectWorkspaceLane(String(row.lane_id ?? ""), { navigateTo: "strategies" });
                          }}
                        >
                          Open Strategy
                        </button>
                      ),
                    },
                  ]}
                  rows={dashboardRosterRows.slice(0, 14)}
                  emptyLabel="No shared paper lanes are currently available in the roster."
                />
              </Section>

              <Section title="ATP / Temp Paper Truth" subtitle="Unambiguous view of whether ATP and temporary paper lanes exist, attach live, stay audit-only, or are not loaded">
                <div className="metric-grid">
                  <MetricCard label="ATP Benchmark Lanes" value={formatShortNumber(atpBenchmarkRows.length)} tone={atpBenchmarkRows.length ? "warn" : "muted"} />
                  <MetricCard label="ATP Candidate Lanes" value={formatShortNumber(atpCandidateRows.length)} tone={atpCandidateRows.length ? "good" : "muted"} />
                  <MetricCard label="ATP Attached Live" value={formatShortNumber(atpStrategyRows.filter((row) => runtimeAttachmentLabel(row) === "Attached Live").length)} tone={atpStrategyRows.some((row) => runtimeAttachmentLabel(row) === "Attached Live") ? "good" : "muted"} />
                  <MetricCard label="Temp Paper Enabled" value={formatShortNumber(temporaryPaperRuntimeIntegrity.enabled_in_app_count ?? temporaryPaperStrategyRows.length)} tone={Number(temporaryPaperRuntimeIntegrity.enabled_in_app_count ?? temporaryPaperStrategyRows.length ?? 0) > 0 ? "warn" : "good"} />
                  <MetricCard label="Temp Paper Loaded" value={formatShortNumber(temporaryPaperRuntimeIntegrity.loaded_in_runtime_count ?? 0)} tone={Number(temporaryPaperRuntimeIntegrity.loaded_in_runtime_count ?? 0) > 0 ? "good" : "muted"} />
                  <MetricCard label="Temp Paper Audit-Only" value={formatShortNumber(temporaryPaperRuntimeIntegrity.snapshot_only_count ?? 0)} tone={Number(temporaryPaperRuntimeIntegrity.snapshot_only_count ?? 0) > 0 ? "warn" : "good"} />
                </div>
                <div className="notice-strip compact">
                  <div>ATP benchmark lanes remain explicit benchmark truth. ATP candidate lanes remain separate candidate-staged identities. Temporary paper rows are secondary audit/runtime-integrity surfaces, not the main operator path.</div>
                  <div>Current temp-paper integrity: {formatValue(temporaryPaperRuntimeIntegrity.mismatch_status ?? "UNKNOWN")} | Enabled {formatShortNumber(temporaryPaperRuntimeIntegrity.enabled_in_app_count ?? 0)} | Loaded {formatShortNumber(temporaryPaperRuntimeIntegrity.loaded_in_runtime_count ?? 0)}.</div>
                </div>
              </Section>

              <Section title="Desktop Startup" subtitle="Backend bind, chosen URL, and ownership">
                <StartupPanel
                  desktopState={desktopState}
                  busyAction={busyAction}
                  onRetryStart={() => void runCommand("start-dashboard", () => api.startDashboard())}
                />
              </Section>

              <Section title="Paper Runtime Launch" subtitle="Backend-owned safe auto-recovery for stopped paper runtime, with explicit manual reasons when restart is unsafe">
                <PaperStartupPanel
                  metrics={paperStartupMetrics}
                  stateLabel={paperStartupStateLabel}
                  reason={paperStartupReasonText}
                  actionLabel={paperStartupActionLabel}
                  actionDescription={paperStartupActionDescription}
                  busyAction={busyAction}
                  canRunLiveActions={canRunLiveActions}
                  onStartDashboard={() => void runCommand("start-dashboard", () => api.startDashboard())}
                  onStartPaper={() => void runCommand("start-paper", () => api.runDashboardAction("start-paper"), { requiresLive: true })}
                  onRestartPaperWithTempPaper={() =>
                    void runCommand("restart-paper-with-temp-paper", () => api.runDashboardAction("restart-paper-with-temp-paper"), {
                      confirmMessage: "Restart Runtime + Temp Paper will restart the paper soak with all enabled temporary paper lanes included. Proceed?",
                      requiresLive: true,
                    })
                  }
                  onAuthGateCheck={() => void runCommand("auth-gate-check", () => api.runDashboardAction("auth-gate-check"), { requiresLive: true })}
                  onCompletePreSessionReview={() =>
                    void runCommand("complete-pre-session-review", () => api.runDashboardAction("complete-pre-session-review"), { requiresLive: true })
                  }
                />
              </Section>

              <Section title="Paper Soak Continuity" subtitle="Restart-safe restore truth, soak health, and continuity state for the current paper runtime">
                <div className="badge-row">
                  <Badge label="RESTART-SAFE" tone="good" />
                  <Badge label={formatValue(restoreValidationLastResult)} tone={restoreValidationUnresolvedIssueCount > 0 ? "danger" : restoreValidationLastResult === "SAFE_CLEANUP_READY" ? "warn" : restoreValidationLastResult === "READY" ? "good" : "muted"} />
                  {paperSoakHealthy ? <Badge label="HEALTHY SOAK" tone="good" /> : <Badge label="DEGRADED SOAK" tone="warn" />}
                </div>
                <div className="metric-grid">
                  {paperSoakContinuityMetrics.map((item) => (
                    <MetricCard key={item.label} label={item.label} value={item.value} tone={item.tone ?? statusTone(item.value)} />
                  ))}
                </div>
                <div className="status-line">{formatValue(paperSoakContinuityStatusLine)}</div>
                <div className="status-line">{formatValue(paperSoakContinuityDetailLine)}</div>
                {restoreValidationActiveIssueRows.length ? (
                  <div className="notice-strip compact">
                    {restoreValidationActiveIssueRows.map((row) => (
                      <div key={String(row.lane_id ?? row.display_name ?? row.symbol ?? "restore-issue")}>
                        <strong>{formatValue(row.display_name ?? row.lane_id)}:</strong> restore result {formatValue(row.restore_result)}.{" "}
                        {row.restore_recommended_action ? `Next action: ${formatValue(row.restore_recommended_action)}` : ""}
                      </div>
                    ))}
                  </div>
                ) : restoreValidationSafeCleanupCount > 0 ? (
                  <div className="notice-strip compact">
                    Restart restore applied {restoreValidationSafeCleanupCount} safe cleanup {restoreValidationSafeCleanupCount === 1 ? "action" : "actions"} automatically and returned to tradable readiness without duplicate submission/fill activity.
                  </div>
                ) : null}
              </Section>

              <Section title="Paper Soak Validation" subtitle="Deterministic MGC paper-engine validation scenarios for duplicate suppression, restart/restore, reconciliation, and fault handling">
                <div className="metric-grid">
                  {paperSoakValidationMetrics.map((item) => (
                    <MetricCard key={String(item.label)} label={String(item.label)} value={item.value} tone={item.tone} />
                  ))}
                </div>
                <div className="status-line">{formatValue(paperSoakValidation.summary_line ?? "No paper soak validation artifact is available yet.")}</div>
                <div className="notice-strip compact">
                  <div>Operator path: {formatValue(paperSoakValidation.operator_path ?? "mgc-v05l probationary-paper-soak-validate")}</div>
                  <div>Allowed scope: {formatValue(asRecord(paperSoakValidation.allowed_scope).symbol ?? "MGC")} / {formatValue(asRecord(paperSoakValidation.allowed_scope).timeframe ?? "5m")} / {formatValue(asRecord(paperSoakValidation.allowed_scope).mode ?? "PAPER")}</div>
                </div>
                <DataTable
                  columns={[
                    { key: "scenario_id", label: "Scenario" },
                    { key: "status", label: "Result", render: (row) => formatValue(row.status ?? "UNKNOWN") },
                    { key: "detail", label: "Outcome" },
                    { key: "runtime_phase", label: "Phase", render: (row) => formatValue(asRecord(row.summary).runtime_phase ?? "UNKNOWN") },
                    { key: "strategy_state", label: "State", render: (row) => formatValue(asRecord(row.summary).strategy_state ?? "UNKNOWN") },
                  ]}
                  rows={paperSoakValidationScenarioRows}
                  emptyLabel="No deterministic paper soak validation scenarios have been recorded yet."
                />
              </Section>

              <Section title="Live Shadow Runtime" subtitle="Actual MGC runtime running on real finalized broker bars and real broker/account truth in SHADOW / NO-SUBMIT mode">
                <div className="metric-grid">
                  {shadowLiveMetrics.map((item) => (
                    <MetricCard key={String(item.label)} label={String(item.label)} value={item.value} tone={item.tone} />
                  ))}
                </div>
                <div className="status-line">{formatValue(shadowLiveSummary.summary_line ?? "No live shadow summary artifact is available yet.")}</div>
                <div className="notice-strip compact">
                  <div>Operator path: {formatValue(shadowLiveSummary.operator_path ?? "mgc-v05l probationary-live-shadow")}</div>
                  <div>Allowed scope: {formatValue(asRecord(shadowLiveSummary.allowed_scope).symbol ?? "MGC")} / {formatValue(asRecord(shadowLiveSummary.allowed_scope).timeframe ?? "5m")} / {formatValue(asRecord(shadowLiveSummary.allowed_scope).mode ?? "LIVE_SHADOW_NO_SUBMIT")}</div>
                  <div>Latest signal: {formatValue(shadowLatestSignalSummary.long_entry_source ?? shadowLatestSignalSummary.short_entry_source ?? "None")}</div>
                  <div>Latest shadow intent: {formatValue(shadowLatestIntent.intent_type ?? "None")}</div>
                </div>
                <div className="notice-strip compact">
                  <div>Pending stage: {formatValue(shadowLiveSummary.pending_stage ?? "IDLE")}</div>
                  <div>Pending reason: {formatValue(shadowLiveSummary.pending_reason ?? "None")}</div>
                  <div>Reconcile trigger: {formatValue(shadowLiveSummary.reconcile_trigger_source ?? "None")}</div>
                  <div>Broker reconciliation: {formatValue(shadowBrokerTruthSummary.reconciliation_status ?? "UNKNOWN")}</div>
                </div>
              </Section>

              <Section title="Live Strategy Pilot" subtitle="First tightly gated real MGC live-submit runtime path with fill-only state transitions and fail-closed broker-truth handling">
                <div className="metric-grid">
                  {liveStrategyPilotMetrics.map((item) => (
                    <MetricCard key={String(item.label)} label={String(item.label)} value={item.value} tone={item.tone} />
                  ))}
                </div>
                <div className="status-line">{formatValue(liveStrategyPilotSummary.summary_line ?? "No live strategy pilot summary artifact is available yet.")}</div>
                <div className="notice-strip compact">
                  <div>Operator path: {formatValue(liveStrategyPilotSummary.operator_path ?? "mgc-v05l probationary-live-strategy-pilot")}</div>
                  <div>Allowed scope: {formatValue(asRecord(liveStrategyPilotSummary.allowed_scope).symbol ?? "MGC")} / {formatValue(asRecord(liveStrategyPilotSummary.allowed_scope).timeframe ?? "5m")} / {formatValue(asRecord(liveStrategyPilotSummary.allowed_scope).mode ?? "LIVE_STRATEGY_PILOT")}</div>
                  <div>Latest bar: {formatValue(liveStrategyPilotLatestBar.bar_id ?? "None")}</div>
                  <div>Latest signal: {formatValue(liveStrategyPilotLatestSignal.long_entry_source ?? liveStrategyPilotLatestSignal.short_entry_source ?? "None")}</div>
                </div>
                <div className="notice-strip compact">
                  <div>Latest intent: {formatValue(liveStrategyPilotLatestIntent.intent_type ?? "None")}</div>
                  <div>Submit attempted: {formatTimestamp(liveStrategyPilotSummary.submit_attempted_at)}</div>
                  <div>Broker ack: {formatTimestamp(liveStrategyPilotSummary.broker_ack_at)}</div>
                  <div>Broker fill: {formatTimestamp(liveStrategyPilotSummary.broker_fill_at)}</div>
                </div>
                <div className="notice-strip compact">
                  <div>Cycle status: {formatValue(liveStrategyPilotCycle.cycle_status ?? liveStrategyPilotSummary.cycle_status ?? "waiting_for_entry")}</div>
                  <div>Entry leg: {formatValue(liveStrategyPilotCycleEntry.intent_type ?? "None")} @ {formatTimestamp(liveStrategyPilotCycleEntry.submit_attempted_at)}</div>
                  <div>Exit leg: {formatValue(liveStrategyPilotCycleExit.intent_type ?? "None")} @ {formatTimestamp(liveStrategyPilotCycleExit.submit_attempted_at)}</div>
                  <div>Flat restore: {formatTimestamp(liveStrategyPilotCycle.flat_restore_confirmation_time)}</div>
                </div>
                <div className="notice-strip compact">
                  <div>Pending reason: {formatValue(liveStrategyPilotSummary.pending_reason ?? "None")}</div>
                  <div>Reconcile trigger: {formatValue(liveStrategyPilotSummary.reconcile_trigger_source ?? "None")}</div>
                  <div>Rearm required: {liveStrategyPilotSummary.pilot_rearm_required ? "YES" : "NO"}</div>
                  <div>Cycle result: {formatValue(liveStrategyPilotCycle.final_result ?? "In Progress")}</div>
                </div>
                <div className="notice-strip compact">
                  <div>Warm-up: {formatValue(liveStrategyPilotGate.warmup_complete ? "COMPLETE" : `${formatValue(liveStrategyPilotGate.warmup_bars_loaded ?? 0)}/${formatValue(liveStrategyPilotGate.warmup_bars_required ?? 0)}`)}</div>
                  <div>Gate blocker: {formatValue(liveStrategyPilotGate.blocker ?? "None")}</div>
                  <div>Cycle blocker: {formatValue(liveStrategyPilotCycle.blocker ?? "None")}</div>
                  <div>Re-arm action: {formatValue(liveStrategyPilotCycle.rearm_action ?? "None")}</div>
                </div>
                <div className="metric-grid">
                  {liveStrategyPilotSignalMetrics.map((item) => (
                    <MetricCard key={String(item.label)} label={String(item.label)} value={item.value} tone={item.tone} />
                  ))}
                </div>
                <div className="status-line">{formatValue(liveStrategyPilotSignalObservability.summary_line ?? "No live signal observability summary is available yet.")}</div>
                <div className="notice-strip compact">
                  <div>Top bull failure: {formatValue(asRecord(liveStrategyPilotBullTopFailures[0]).predicate ?? "None")} ({formatValue(asRecord(liveStrategyPilotBullTopFailures[0]).count ?? 0)})</div>
                  <div>Top Asia VWAP failure: {formatValue(asRecord(liveStrategyPilotAsiaTopFailures[0]).predicate ?? "None")} ({formatValue(asRecord(liveStrategyPilotAsiaTopFailures[0]).count ?? 0)})</div>
                  <div>Top bear failure: {formatValue(asRecord(liveStrategyPilotBearTopFailures[0]).predicate ?? "None")} ({formatValue(asRecord(liveStrategyPilotBearTopFailures[0]).count ?? 0)})</div>
                  <div>Latest bars-since setup: L {formatValue(liveStrategyPilotSignalAntiChurn.lastBarsSinceLongSetup ?? "—")} / S {formatValue(liveStrategyPilotSignalAntiChurn.lastBarsSinceShortSetup ?? "—")}</div>
                </div>
                <div className="metric-grid">
                  {signalSelectivityMetrics.map((item) => (
                    <MetricCard key={String(item.label)} label={String(item.label)} value={item.value} tone={item.tone} />
                  ))}
                </div>
                <div className="status-line">{formatValue(signalSelectivityAnalysis.summary_line ?? "No signal selectivity analysis artifact is available yet.")}</div>
                <div className="notice-strip compact">
                  <div>Quant top bull blocker: {formatValue(asRecord(signalSelectivityBullTop[0]).predicate ?? "None")} ({formatValue(asRecord(signalSelectivityBullTop[0]).count ?? 0)})</div>
                  <div>Quant top Asia blocker: {formatValue(asRecord(signalSelectivityAsiaTop[0]).predicate ?? "None")} ({formatValue(asRecord(signalSelectivityAsiaTop[0]).count ?? 0)})</div>
                  <div>Quant top bear blocker: {formatValue(asRecord(signalSelectivityBearTop[0]).predicate ?? "None")} ({formatValue(asRecord(signalSelectivityBearTop[0]).count ?? 0)})</div>
                  <div>Bear Snap range ladder: {formatValue(signalSelectivityRangeLadder.summary_line ?? "Unavailable")}</div>
                </div>
                <DataTable
                  columns={[
                    { key: "bar_end_ts", label: "Completed Bar", render: (row) => formatTimestamp(row.bar_end_ts) },
                    { key: "session_classification", label: "Session", render: (row) => formatValue(row.session_classification ?? "UNKNOWN") },
                    { key: "why_no_trade", label: "Why No Trade", render: (row) => formatValue(row.why_no_trade ?? "Unavailable") },
                    { key: "bear_snap_location_ok", label: "bearSnapLoc", render: (row) => formatValue(row.bear_snap_location_ok ?? false) },
                    { key: "recentLongSetup", label: "recentLongSetup", render: (row) => formatValue(row.recentLongSetup ?? false) },
                    { key: "barsSinceLongSetup", label: "barsSinceLongSetup", render: (row) => formatValue(row.barsSinceLongSetup ?? "—") },
                    { key: "recentShortSetup", label: "recentShortSetup", render: (row) => formatValue(row.recentShortSetup ?? false) },
                    { key: "barsSinceShortSetup", label: "barsSinceShortSetup", render: (row) => formatValue(row.barsSinceShortSetup ?? "—") },
                    { key: "entries", label: "Raw -> Final", render: (row) => `L ${formatValue(row.longEntryRaw ?? false)} -> ${formatValue(row.longEntry ?? false)} | S ${formatValue(row.shortEntryRaw ?? false)} -> ${formatValue(row.shortEntry ?? false)}` },
                  ]}
                  rows={liveStrategyPilotRecentNoTradeRows}
                  emptyLabel="No persisted completed-bar signal observability rows are available yet."
                />
              </Section>

              <Section title="Broker Truth Shadow Validation" subtitle="Read-only live-shadow confirmation of normalized Schwab order-status, open-order, position, and account-health payload truth used by the real MGC runtime">
                <div className="metric-grid">
                  {paperBrokerTruthShadowMetrics.map((item) => (
                    <MetricCard key={String(item.label)} label={String(item.label)} value={item.value} tone={item.tone} />
                  ))}
                </div>
                <div className="status-line">{formatValue(paperBrokerTruthShadowValidation.summary_line ?? "No broker-truth shadow validation artifact is available yet.")}</div>
                <div className="notice-strip compact">
                  <div>Operator path: {formatValue(paperBrokerTruthShadowValidation.operator_path ?? "mgc-v05l probationary-broker-truth-shadow-validate")}</div>
                  <div>Allowed scope: {formatValue(asRecord(paperBrokerTruthShadowValidation.allowed_scope).symbol ?? "MGC")} / {formatValue(asRecord(paperBrokerTruthShadowValidation.allowed_scope).timeframe ?? "5m")} / {formatValue(asRecord(paperBrokerTruthShadowValidation.allowed_scope).mode ?? "READ_ONLY_LIVE_SHADOW")}</div>
                  <div>Missing or ambiguous fields: {formatShortNumber(asArray(paperBrokerTruthShadowSummary.missing_or_ambiguous_fields).length)}</div>
                </div>
                <DataTable
                  columns={[
                    { key: "schema_name", label: "Schema" },
                    { key: "classification", label: "Classification", render: (row) => formatValue(row.classification ?? "UNKNOWN") },
                    { key: "issues", label: "Issues", render: (row) => formatValue(row.issues ?? "none") },
                    { key: "required_count", label: "Required", render: (row) => formatValue(row.required_count ?? 0) },
                    { key: "optional_count", label: "Optional", render: (row) => formatValue(row.optional_count ?? 0) },
                  ]}
                  rows={paperBrokerTruthShadowComponentRows}
                  emptyLabel="No broker-truth validation components are available yet."
                />
              </Section>

              <Section title="Live Timing Summary" subtitle="Actual MGC runtime timing truth across completed-bar evaluation, intent persistence, broker submit/ack/fill timing, and fail-closed pending stages">
                <div className="metric-grid">
                  {paperLiveTimingMetrics.map((item) => (
                    <MetricCard key={String(item.label)} label={String(item.label)} value={item.value} tone={item.tone} />
                  ))}
                </div>
                <div className="status-line">{formatValue(paperLiveTimingSummary.summary_line ?? "No live timing summary artifact is available yet.")}</div>
                <div className="notice-strip compact">
                  <div>Pending since: {formatTimestamp(paperLiveTimingSummary.pending_since)}</div>
                  <div>Pending reason: {formatValue(paperLiveTimingSummary.pending_reason ?? "None")}</div>
                  <div>Reconcile trigger: {formatValue(paperLiveTimingSummary.reconcile_trigger_source ?? "None")}</div>
                  <div>Broker truth order: {asArray(paperLiveTimingBrokerTruth.decision_order).map((value) => String(value)).join(" -> ") || "direct_order_status -> open_orders -> position_truth -> fill_truth"}</div>
                </div>
              </Section>

              <Section title="Live Timing Validation" subtitle="Deterministic MGC live-timing boundary validation for submit, ack, fill, broker reconnect, rejection, and fill-driven exit semantics">
                <div className="metric-grid">
                  {paperLiveTimingValidationMetrics.map((item) => (
                    <MetricCard key={String(item.label)} label={String(item.label)} value={item.value} tone={item.tone} />
                  ))}
                </div>
                <div className="status-line">{formatValue(paperLiveTimingValidation.summary_line ?? "No live timing validation artifact is available yet.")}</div>
                <div className="notice-strip compact">
                  <div>Operator path: {formatValue(paperLiveTimingValidation.operator_path ?? "mgc-v05l probationary-live-timing-validate")}</div>
                  <div>Allowed scope: {formatValue(asRecord(paperLiveTimingValidation.allowed_scope).symbol ?? "MGC")} / {formatValue(asRecord(paperLiveTimingValidation.allowed_scope).timeframe ?? "5m")} / {formatValue(asRecord(paperLiveTimingValidation.allowed_scope).mode ?? "PAPER_RUNTIME_WITH_LIVE_TIMING_BOUNDARY")}</div>
                </div>
                <DataTable
                  columns={[
                    { key: "scenario_id", label: "Scenario" },
                    { key: "status", label: "Result", render: (row) => formatValue(row.status ?? "UNKNOWN") },
                    { key: "detail", label: "Outcome" },
                    { key: "pending_stage", label: "Stage", render: (row) => formatValue(asRecord(row.summary).pending_stage ?? "UNKNOWN") },
                    { key: "runtime_phase", label: "Phase", render: (row) => formatValue(asRecord(row.summary).runtime_phase ?? "UNKNOWN") },
                  ]}
                  rows={paperLiveTimingValidationScenarioRows}
                  emptyLabel="No live timing validation scenarios have been recorded yet."
                />
              </Section>

              <Section title="Exit Parity Summary" subtitle="Current MGC exit-family truth, reason priority, stop references, break-even state, and pending-vs-confirmed exit status">
                <div className="metric-grid">
                  {paperExitParityMetrics.map((item) => (
                    <MetricCard key={String(item.label)} label={String(item.label)} value={item.value} tone={item.tone} />
                  ))}
                </div>
                <div className="status-line">{formatValue(paperExitParitySummary.summary_line ?? "No exit parity summary artifact is available yet.")}</div>
                <div className="notice-strip compact">
                  <div>Active long stop ref: {formatValue(exitParityStopRefs.active_long_stop_ref ?? "None")}</div>
                  <div>Active short stop ref: {formatValue(exitParityStopRefs.active_short_stop_ref ?? "None")}</div>
                  <div>Long BE armed: {formatValue(exitParityBreakEven.long_break_even_armed ?? false)}</div>
                  <div>Short BE armed: {formatValue(exitParityBreakEven.short_break_even_armed ?? false)}</div>
                </div>
              </Section>

              <Section title="Extended Paper Soak" subtitle="Longer-running MGC paper runtime with restart injection and drift detection across READY, pending, in-position, post-exit, and degraded watchdog checkpoints">
                <div className="metric-grid">
                  {paperSoakExtendedMetrics.map((item) => (
                    <MetricCard key={String(item.label)} label={String(item.label)} value={item.value} tone={item.tone} />
                  ))}
                </div>
                <div className="status-line">{formatValue(paperSoakExtended.summary_line ?? "No extended paper soak artifact is available yet.")}</div>
                <div className="notice-strip compact">
                  <div>Operator path: {formatValue(paperSoakExtended.operator_path ?? "mgc-v05l probationary-paper-soak-extended")}</div>
                  <div>Allowed scope: {formatValue(asRecord(paperSoakExtended.allowed_scope).symbol ?? "MGC")} / {formatValue(asRecord(paperSoakExtended.allowed_scope).timeframe ?? "5m")} / {formatValue(asRecord(paperSoakExtended.allowed_scope).mode ?? "PAPER")}</div>
                </div>
                <DataTable
                  columns={[
                    { key: "checkpoint_id", label: "Checkpoint" },
                    { key: "trigger_state", label: "Trigger" },
                    { key: "drift_detected", label: "Drift", render: (row) => (row.drift_detected === true ? "YES" : "NO") },
                    { key: "duplicate_action_prevention_held", label: "Duplicate Actions Held", render: (row) => (row.duplicate_action_prevention_held === true ? "YES" : "NO") },
                    { key: "restore_result", label: "Restore Result", render: (row) => formatValue(row.restore_result ?? "UNKNOWN") },
                  ]}
                  rows={paperSoakExtendedCheckpointRows}
                  emptyLabel="No extended paper soak checkpoints have been recorded yet."
                />
              </Section>

              <Section title="Unattended Paper Soak" subtitle="Operational MGC paper soak with longer session progression, restart injection, drift detection, and fail-closed checkpoint summaries">
                <div className="metric-grid">
                  {paperSoakUnattendedMetrics.map((item) => (
                    <MetricCard key={String(item.label)} label={String(item.label)} value={item.value} tone={item.tone} />
                  ))}
                </div>
                <div className="status-line">{formatValue(paperSoakUnattended.summary_line ?? "No unattended paper soak artifact is available yet.")}</div>
                <div className="notice-strip compact">
                  <div>Operator path: {formatValue(paperSoakUnattended.operator_path ?? "mgc-v05l probationary-paper-soak-unattended")}</div>
                  <div>Allowed scope: {formatValue(asRecord(paperSoakUnattended.allowed_scope).symbol ?? "MGC")} / {formatValue(asRecord(paperSoakUnattended.allowed_scope).timeframe ?? "5m")} / {formatValue(asRecord(paperSoakUnattended.allowed_scope).mode ?? "PAPER")}</div>
                </div>
                <DataTable
                  columns={[
                    { key: "checkpoint_id", label: "Checkpoint" },
                    { key: "trigger_state", label: "Trigger" },
                    { key: "drift_detected", label: "Drift", render: (row) => (row.drift_detected === true ? "YES" : "NO") },
                    { key: "duplicate_action_prevention_held", label: "Duplicate Actions Held", render: (row) => (row.duplicate_action_prevention_held === true ? "YES" : "NO") },
                    { key: "summary_alignment_held", label: "State Aligned", render: (row) => (row.summary_alignment_held === true ? "YES" : "NO") },
                    { key: "restore_result", label: "Restore Result", render: (row) => formatValue(row.restore_result ?? "UNKNOWN") },
                  ]}
                  rows={paperSoakUnattendedCheckpointRows}
                  emptyLabel="No unattended paper soak checkpoints have been recorded yet."
                />
              </Section>

              <Section title="Local Operator Auth" subtitle="Touch ID-backed local operator identity for sensitive desktop actions, kept separate from broker auth">
                <div className="badge-row">
                  <Badge label="LOCAL AUTH" tone={localOperatorAuth.auth_session_active ? "good" : localOperatorAuth.auth_available ? "warn" : "muted"} />
                  <Badge label="BROKER AUTH SEPARATE" tone="warn" />
                  {localOperatorAuth.secret_protection?.wrapper_ready ? <Badge label="KEYCHAIN WRAPPER READY" tone="good" /> : <Badge label="SECRET WRAPPER DEFERRED" tone="warn" />}
                </div>
                <div className="metric-grid">
                  {localAuthMetrics.map((item) => (
                    <MetricCard key={item.label} label={item.label} value={item.value} tone={item.tone} />
                  ))}
                </div>
                <div className="status-line">
                  {formatValue(
                    localOperatorAuth.auth_session_active
                      ? `Local operator authenticated as ${localOperatorAuth.local_operator_identity ?? "local_touch_id_operator"} until ${formatTimestamp(localOperatorAuth.auth_session_expires_at)}.`
                      : localOperatorAuth.last_auth_detail ??
                          "Sensitive same-underlying and dangerous operator actions require fresh local Touch ID authentication on macOS.",
                  )}
                </div>
                <div className="notice-strip">
                  <div>Touch ID protects local operator-sensitive actions in this desktop app. It does not replace or modify Schwab OAuth.</div>
                  <div>{formatValue(localOperatorAuth.secret_protection?.detail ?? "Keychain-backed secret wrapper status is unavailable.")}</div>
                  {Object.keys(latestLocalAuthEvent).length ? (
                    <div>
                      Latest local auth event: {formatValue(latestLocalAuthEvent.event_type)} at {formatTimestamp(latestLocalAuthEvent.occurred_at)}.
                    </div>
                  ) : null}
                </div>
                <div className="action-row inline">
                  <button
                    className="panel-button"
                    disabled={busyAction !== null || localOperatorAuth.auth_available !== true}
                    onClick={() =>
                      void runCommand("authenticate-local-operator", () =>
                        api.authenticateLocalOperator("Authenticate local operator access for this desktop session."),
                      )
                    }
                  >
                    Authenticate Touch ID
                  </button>
                  <button
                    className="panel-button subtle"
                    disabled={busyAction !== null || localOperatorAuth.auth_session_active !== true}
                    onClick={() => void runCommand("clear-local-operator-auth-session", () => api.clearLocalOperatorAuthSession())}
                  >
                    Clear Local Auth Session
                  </button>
                </div>
              </Section>

              <Section title="Sunday Open Preflight" subtitle="Default-visible operator readiness verdict">
                <PreflightPanel model={preflight} />
              </Section>

              <Section title="Strategy Runtime Truth" subtitle="Configured standalone strategy identities, runtime instances, and current runtime-state coverage">
                <div className="badge-row">
                  <Badge label="RUNTIME TRUTH" tone={truthBadgeTone("RUNTIME TRUTH")} />
                  <Badge label="PAPER" tone={truthBadgeTone("PAPER")} />
                  {desktopState?.source.mode === "snapshot_fallback" ? <Badge label="SNAPSHOT FALLBACK" tone={truthBadgeTone("SNAPSHOT FALLBACK")} /> : null}
                </div>
                <div className="metric-grid">
                  {strategyRuntimeMetrics.map((item) => (
                    <MetricCard key={item.label} label={item.label} value={item.value} tone={item.tone} />
                  ))}
                </div>
                <div className="status-line">
                  {formatValue(
                    strategyRuntimeSummary.summary_line ??
                      "Runtime truth shows whether standalone strategies are configured, instantiated, state-loaded, and able to process completed bars.",
                  )}
                </div>
                <div className="notice-strip">
                  {asArray<string>(strategyRuntimeSummary.notes).length ? (
                    asArray<string>(strategyRuntimeSummary.notes).map((item) => <div key={item}>{item}</div>)
                  ) : (
                    <>
                      <div>Standalone strategy identity is the runtime unit. Different instruments are surfaced and tracked separately.</div>
                      <div>Same-underlying execution/netting arbitration remains constrained and is surfaced explicitly when present.</div>
                    </>
                  )}
                </div>
              </Section>

              <Section title="Same-Underlying Conflicts" subtitle="Explicit operator warning workflow for standalone strategies sharing the same instrument">
                <div className="badge-row">
                  <Badge label="RUNTIME TRUTH" tone={truthBadgeTone("RUNTIME TRUTH")} />
                  <Badge label="STRATEGY LEDGER" tone={truthBadgeTone("STRATEGY LEDGER")} />
                  {sameUnderlyingBlockingConflictRows.length > 0 ? <Badge label="OPERATOR ACTION REQUIRED" tone="danger" /> : null}
                </div>
                <div className="metric-grid">
                  {sameUnderlyingConflictMetrics.map((item) => (
                    <MetricCard key={item.label} label={item.label} value={item.value} tone={item.tone} />
                  ))}
                </div>
                <div className="status-line">
                  {sortedSameUnderlyingConflictRows.length
                    ? "Same-underlying overlap is surfaced explicitly. The app is not auto-merging, auto-netting, or auto-arbitrating these strategies."
                    : "No same-underlying conflicts are surfaced in the current snapshot."}
                </div>
                <div className="notice-strip">
                  {sameUnderlyingConflictNotes.length ? (
                    sameUnderlyingConflictNotes.map((item) => <div key={item}>{item}</div>)
                  ) : (
                    <>
                      <div>Observational conflicts mean multiple standalone strategies share an instrument, but no overlapping exposure or pending-order state is surfaced yet.</div>
                      <div>Blocking conflicts mean same-underlying exposure, pending orders, or broker/runtime overlap is already present and requires operator review.</div>
                    </>
                  )}
                  {sameUnderlyingHoldingRows.length ? (
                    <div>
                      Instruments on hold: {sameUnderlyingHoldingRows.map((row) => String(row.instrument ?? "")).filter(Boolean).join(", ")}.
                      New entries are intentionally blocked there, but exits and existing exposure still require manual review.
                    </div>
                  ) : null}
                  {sameUnderlyingExpiredRows.length ? (
                    <div>
                      Hold expired: {sameUnderlyingExpiredRows.map((row) => String(row.instrument ?? "")).filter(Boolean).join(", ")}.
                      The conflict is still visible, but the entry hold is no longer effective until the operator re-applies it.
                    </div>
                  ) : null}
                  {sameUnderlyingStaleRows.length ? (
                    <div>
                      Reopened conflicts: {sameUnderlyingStaleRows.map((row) => String(row.instrument ?? "")).filter(Boolean).join(", ")}.
                      A prior acknowledgement no longer matches the current overlap state and should be reviewed again.
                    </div>
                  ) : null}
                  {Object.keys(sameUnderlyingLatestEvent).length ? (
                    <div>
                      Latest conflict activity: {formatValue(sameUnderlyingLatestEvent.event_type)} on {formatValue(sameUnderlyingLatestEvent.instrument)} at{" "}
                      {formatTimestamp(sameUnderlyingLatestEvent.occurred_at)}.
                    </div>
                  ) : null}
                  {sortedSameUnderlyingConflictRows.length ? (
                    <div>
                      Instruments affected: {sortedSameUnderlyingConflictRows.map((row) => String(row.instrument ?? "")).filter(Boolean).join(", ")}
                    </div>
                  ) : null}
                </div>
              </Section>

              <Section title="Portfolio P&L" subtitle="Strategy-ledger portfolio truth from paper/runtime state, kept separate from broker account truth">
                <div className="badge-row">
                  <Badge label="STRATEGY LEDGER" tone={truthBadgeTone("STRATEGY LEDGER")} />
                  <Badge label="PAPER" tone={truthBadgeTone("PAPER")} />
                  {desktopState?.source.mode === "snapshot_fallback" ? <Badge label="SNAPSHOT FALLBACK" tone={truthBadgeTone("SNAPSHOT FALLBACK")} /> : null}
                </div>
                <div className="metric-grid">
                  {portfolioSnapshotMetrics.map((item) => (
                    <MetricCard key={item.label} label={item.label} value={item.value} tone={statusTone(item.value)} />
                  ))}
                </div>
                <div className="status-line">
                  {formatValue(
                    strategyPortfolioSnapshot.summary_line ??
                      "Derived from the current strategy-performance ledger; unrealized values remain explicit when trusted marks are missing.",
                  )}
                </div>
                <div className="status-line">
                  Approved/admitted paper totals and TEMP PAPER totals are shown separately here. Combined totals include both.
                </div>
                {asArray<string>(strategyPortfolioSnapshot.unrealized_missing_strategies).length ? (
                  <div className="notice-strip">
                    <div>Unpriced unrealized component remains explicit. Trusted open-position marks are missing for:</div>
                    <div>{asArray<string>(strategyPortfolioSnapshot.unrealized_missing_strategies).join(", ")}</div>
                  </div>
                ) : null}
              </Section>

              <Section title="Replay / Backtest Truth" subtitle="Latest replay/backtest run summary, explicitly separate from runtime paper state and broker truth">
                <div className="badge-row">
                  <Badge label="REPLAY" tone={truthBadgeTone("REPLAY")} />
                  <Badge label="BACKTEST" tone={truthBadgeTone("REPLAY")} />
                </div>
                <div className="metric-grid">
                  {replaySummaryMetrics.map((item) => (
                    <MetricCard key={item.label} label={item.label} value={item.value} tone={statusTone(item.value)} />
                  ))}
                </div>
                <div className="status-line">
                  {playbackReplaySummaryAvailable
                    ? "Replay summary payload is available and is being rendered as replay/backtest truth, not broker truth."
                    : formatValue(playback.note ?? "The latest historical playback artifact does not expose the standalone replay summary contract yet.")}
                </div>
                <div className="notice-strip">
                  {playbackReplaySummaryAvailable ? (
                    <>
                      <div>Standalone strategies included: {formatValue(asArray<string>(playbackAggregateSummary.standalone_strategy_ids).join(", ") || "None")}</div>
                      <div>{formatValue(playbackAggregateSummary.pnl_unavailable_reason ?? "Priced replay P&L is available for the latest run.")}</div>
                    </>
                  ) : (
                    <div>Replay summary contract unavailable in the latest artifact; fallback trigger-validation rows are still shown on the Replay page.</div>
                  )}
                </div>
              </Section>

              <Section title="Research History Capture" subtitle="Daily operational append status for forever-retained research-history bars">
                <div className="badge-row">
                  <Badge label="RESEARCH HISTORY" tone={truthBadgeTone("REPLAY")} />
                  <Badge label="DAILY CAPTURE" tone="muted" />
                  <Badge
                    label={sentenceCase(researchCaptureRunStatus.replace(/_/g, " "))}
                    tone={researchCaptureTone(researchCaptureRunStatus, researchCaptureFreshnessState)}
                  />
                  {researchCaptureFreshnessState !== "current" ? (
                    <Badge
                      label={sentenceCase(researchCaptureFreshnessState.replace(/_/g, " "))}
                      tone={researchCaptureTone(researchCaptureRunStatus, researchCaptureFreshnessState)}
                    />
                  ) : null}
                </div>
                <div className="metric-grid">
                  {researchCaptureMetrics.map((item) => (
                    <MetricCard key={item.label} label={item.label} value={item.value} tone={item.tone ?? statusTone(item.value)} />
                  ))}
                </div>
                <div className="status-line">
                  {formatValue(
                    researchCapture.status_line ?? "No daily research-history capture status is currently available.",
                  )}
                </div>
                {researchCaptureFreshnessState === "stale" || researchCaptureFreshnessState === "no_run" ? (
                  <div className="notice-strip compact">
                    {researchCaptureFreshnessState === "no_run"
                      ? "No daily research-history capture run has been recorded yet."
                      : `Daily research-history capture is stale. Last attempted run was ${formatTimestamp(researchCapture.last_attempted_at)}.`}
                  </div>
                ) : null}
                {researchCaptureFailedSymbols.length ? (
                  <div className="notice-strip compact">
                    Failed symbols:{" "}
                    {researchCaptureFailedSymbols
                      .map((row) => `${formatValue(row.symbol)} (${formatValue(row.failure_code ?? "error")})`)
                      .join(", ")}
                  </div>
                ) : null}
                {researchCaptureTargetRows.length ? (
                  <div className="table-shell">
                    <table className="data-table research-capture-table">
                      <thead>
                        <tr>
                          <th>Symbol</th>
                          <th>Class</th>
                          <th>TF</th>
                          <th>Last Captured Bar</th>
                          <th>Status</th>
                          <th>Failure</th>
                        </tr>
                      </thead>
                      <tbody>
                        {researchCaptureTargetRows.map((row) => (
                          <tr key={`${String(row.capture_class ?? "")}:${String(row.symbol ?? "")}:${String(row.timeframe ?? "")}`}>
                            <td>{formatValue(row.symbol)}</td>
                            <td>{formatValue(row.capture_class)}</td>
                            <td>{formatValue(row.timeframe)}</td>
                            <td>{formatTimestamp(row.last_captured_bar_end_ts)}</td>
                            <td>
                              <Badge
                                label={sentenceCase(String(row.status ?? "unknown").replace(/_/g, " "))}
                                tone={researchCaptureTone(row.status, row.status === "success" ? "current" : researchCaptureFreshnessState)}
                              />
                            </td>
                            <td>{formatValue(row.failure_code ? `${row.failure_code}: ${row.failure_detail ?? ""}` : "—")}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : null}
              </Section>

              <Section title="Broker Portfolio Truth" subtitle="Schwab broker account truth kept separate from strategy-ledger and replay truth">
                <div className="badge-row">
                  <Badge label="LIVE BROKER" tone={truthBadgeTone("LIVE BROKER")} />
                  {desktopState?.source.mode === "snapshot_fallback" ? <Badge label="SNAPSHOT FALLBACK" tone={truthBadgeTone("SNAPSHOT FALLBACK")} /> : null}
                </div>
                <div className="metric-grid">
                  {brokerSummaryMetrics.map((item) => (
                    <MetricCard key={item.label} label={item.label} value={item.value} tone={item.tone ?? statusTone(item.value)} />
                  ))}
                </div>
                <div className="status-line">{strategyBrokerDriftSummary}</div>
              </Section>

              <Section title="Operator Alerts" subtitle="Compact active and recent notifications from runtime, reconciliation, health, and recovery paths">
                <div className="badge-row">
                  <Badge label="ALERT ROUTING" tone="muted" />
                  {operatorActiveAlertRows.length ? <Badge label="ACTIVE" tone="warn" /> : <Badge label="QUIET" tone="good" />}
                </div>
                <div className="metric-grid">
                  {operatorAlertMetrics.map((item) => (
                    <MetricCard key={item.label} label={item.label} value={item.value} tone={item.tone ?? statusTone(item.value)} />
                  ))}
                </div>
                <div className="status-line">{operatorAlertsStatusLine}</div>
                {operatorActiveAlertRows.length ? (
                  <div className="notice-strip compact">
                    {operatorActiveAlertRows.map((row, index) => (
                      <div key={`${String(row.dedup_key ?? row.code ?? row.title ?? "active-alert")}-${index}`}>
                        <strong>{formatValue(row.title ?? row.code ?? "Alert")}:</strong> {formatValue(row.message)}{" "}
                        {row.recommended_action ? `Next action: ${formatValue(row.recommended_action)}` : ""}
                      </div>
                    ))}
                  </div>
                ) : null}
                {operatorRecentAlertRows.length ? (
                  <div className="table-shell">
                    <table className="data-table">
                      <thead>
                        <tr>
                          <th>Time</th>
                          <th>Severity</th>
                          <th>Category</th>
                          <th>Alert</th>
                          <th>Next Action</th>
                        </tr>
                      </thead>
                      <tbody>
                        {operatorRecentAlertRows.map((row, index) => (
                          <tr key={`${String(row.alert_id ?? row.dedup_key ?? row.code ?? row.title ?? "recent-alert")}-${index}`}>
                            <td>{formatTimestamp(row.occurred_at ?? row.logged_at)}</td>
                            <td>
                              <Badge label={formatValue(row.severity ?? "INFO")} tone={alertSeverityTone(row.severity)} />
                            </td>
                            <td>{formatValue(row.category ?? "runtime")}</td>
                            <td>{formatValue(row.message ?? row.title ?? "Alert")}</td>
                            <td>{formatValue(row.recommended_action ?? "—")}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="notice-strip compact">No persisted alerts have been emitted yet in the current runtime snapshot.</div>
                )}
              </Section>

              <Section title="Runtime / Readiness Context" subtitle="Immediate operator context across the runtime, transport, and preflight layer">
                <div className="metric-grid">
                  {readinessCards.map((item) => (
                    <MetricCard key={item.label} label={item.label} value={formatValue(item.value)} tone={item.tone ?? statusTone(item.value)} />
                  ))}
                </div>
                <div className="status-line">{formatValue(runtimeReadiness.status_line ?? "No runtime readiness summary is available.")}</div>
                <div className="status-line">{formatValue(runtimeSupervisorStatusLine)}</div>
                <div className="status-line">{formatValue(runtimeSupervisorActionLine)}</div>
                <div className="status-line">{formatValue(heartbeatStatusLine)}</div>
                <div className="status-line">{formatValue(orderTimeoutStatusLine)}</div>
                {paperRuntimeRestartSuppressed || paperRuntimeRecovery.manual_action_required === true ? (
                  <div className="notice-strip compact">
                    <div>
                      <strong>Runtime Supervisor:</strong> {formatValue(paperRuntimeRecoveryMessage || paperRuntimeRecoveryState)}{" "}
                      {paperRuntimeRecoveryNextAction ? `Next action: ${formatValue(paperRuntimeRecoveryNextAction)}` : ""}
                    </div>
                  </div>
                ) : null}
                {heartbeatActiveIssueRows.length ? (
                  <div className="notice-strip compact">
                    {heartbeatActiveIssueRows.map((row) => (
                      <div key={String(row.lane_id ?? row.display_name ?? row.symbol ?? "heartbeat-issue")}>
                        <strong>{formatValue(row.display_name ?? row.lane_id)}:</strong> {formatValue(row.reason ?? row.classification ?? row.status)}{" "}
                        {row.recommended_action ? `Next action: ${formatValue(row.recommended_action)}` : ""}
                      </div>
                    ))}
                  </div>
                ) : heartbeatLastStatus === "SAFE_REPAIR" ? (
                  <div className="notice-strip compact">
                    Safe repair was applied on the last heartbeat reconcile. {formatValue(heartbeatReason ?? "State drift was repaired automatically.")}{" "}
                    {heartbeatRecommendedAction ? `Next action: ${formatValue(heartbeatRecommendedAction)}` : ""}
                  </div>
                ) : null}
                {orderTimeoutActiveIssueRows.length ? (
                  <div className="notice-strip compact">
                    {orderTimeoutActiveIssueRows.map((row) => (
                      <div key={String(row.order_intent_id ?? row.display_name ?? row.symbol ?? "timeout-issue")}>
                        <strong>{formatValue(row.display_name ?? row.symbol ?? row.order_intent_id)}:</strong> {formatValue(row.reason ?? row.status)}{" "}
                        {row.recommended_action ? `Next action: ${formatValue(row.recommended_action)}` : ""}
                      </div>
                    ))}
                  </div>
                ) : orderTimeoutLastStatus === "SAFE_REPAIR" ? (
                  <div className="notice-strip compact">
                    Pending-order timeout automation applied a safe cleanup. {formatValue(orderTimeoutReason ?? "Low-ambiguity stale pending state was repaired automatically.")}{" "}
                    {orderTimeoutRecommendedAction ? `Next action: ${formatValue(orderTimeoutRecommendedAction)}` : ""}
                  </div>
                ) : null}
                <div className="readiness-state-grid">
                  <div className={`readiness-state-card ${loadedNotEligibleRows.length ? "warn" : "good"}`}>
                    <div className="readiness-state-title">Loaded, Not Eligible</div>
                    <div className="readiness-state-body">
                      {loadedNotEligibleRows.length
                        ? "These lanes are loaded in runtime but not currently tradable. This is usually a wait state, not a fault."
                        : "No lanes are currently loaded-but-not-eligible."}
                    </div>
                  </div>
                  <div className={`readiness-state-card ${runtimeUpButLaneHalted ? "warn" : "good"}`}>
                    <div className="readiness-state-title">Halted By Lane Risk</div>
                    <div className="readiness-state-body">
                      {runtimeUpButLaneHalted
                        ? "These lanes are running inside the runtime, but lane-local risk logic is intentionally preventing new entries."
                        : "No lane-local risk halt is currently active."}
                    </div>
                  </div>
                  <div className={`readiness-state-card ${reconcilingLaneRows.length ? "danger" : "good"}`}>
                    <div className="readiness-state-title">Frozen By Reconciliation</div>
                    <div className="readiness-state-body">
                      {reconcilingLaneRows.length
                        ? "Reconciliation is unresolved for one or more lanes, so new entries stay frozen until state is clean."
                        : "No lanes are currently frozen by reconciliation."}
                    </div>
                  </div>
                  <div className={`readiness-state-card ${faultedLaneRows.length || runtimeBlockingFaultRows.length ? "danger" : "good"}`}>
                    <div className="readiness-state-title">Faulted</div>
                    <div className="readiness-state-body">
                      {faultedLaneRows.length || runtimeBlockingFaultRows.length
                        ? "A true fault is active. This is separate from harmless coexistence or normal not-eligible wait states."
                        : "No lanes are currently faulted."}
                    </div>
                  </div>
                  <div className={`readiness-state-card ${readinessDegradedFeeds.length ? "warn" : "good"}`}>
                    <div className="readiness-state-title">Informational Only</div>
                    <div className="readiness-state-body">
                      {informationalOnlyLaneRows.length || readinessDegradedFeeds.length
                        ? "These issues stay visible, but they do not by themselves make the lane non-tradable."
                        : "No informational-only degradation is currently surfaced."}
                    </div>
                  </div>
                </div>
                {readinessLaneStatusRows.length ? (
                  <div className="table-shell">
                    <table className="data-table">
                      <thead>
                        <tr>
                          <th>Lane</th>
                          <th>Symbol</th>
                          <th>Loaded</th>
                          <th>Tradability</th>
                          <th>Reason</th>
                          <th>Next Action</th>
                        </tr>
                      </thead>
                      <tbody>
                        {readinessLaneStatusRows.map((row) => (
                          <tr key={String(row.lane_id ?? row.display_name ?? row.symbol ?? "lane-status-row")}>
                            <td>{formatValue(row.display_name ?? row.lane_id)}</td>
                            <td>{formatValue(row.symbol)}</td>
                            <td>{row.loaded_in_runtime === true ? "Yes" : "No"}</td>
                            <td>
                              <Badge label={laneTradabilityLabel(row.tradability_status)} tone={laneTradabilityTone(row.tradability_status)} />
                            </td>
                            <td>{formatValue(row.tradability_reason)}</td>
                            <td>{formatValue(row.next_action)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : null}
                {laneHaltRecoveryRows.length ? (
                  <div className="readiness-operator-panel">
                    <div className="readiness-operator-header">
                      <div className="subsection-title">Lane Halt Recovery</div>
                      <Badge label="LANE-LOCAL HALTS" tone="warn" />
                    </div>
                    <div className="readiness-operator-grid">
                      {laneHaltRecoveryRows.map((row) => (
                        <div key={row.laneId} className="readiness-operator-card">
                          <div className="readiness-operator-card-header">
                            <div className="readiness-operator-name">{row.laneLabel}</div>
                            <div className="badge-row compact">
                              <Badge label={row.symbolLabel} tone="muted" />
                              {row.canForceSessionOverride ? <Badge label="OVERRIDE ELIGIBLE" tone="warn" /> : null}
                            </div>
                          </div>
                          <div className="readiness-operator-detail"><strong>State:</strong> {row.riskStateLabel}</div>
                          <div className="readiness-operator-detail"><strong>Reason:</strong> {row.haltReasonLabel}</div>
                          <div className="readiness-operator-detail"><strong>Latched:</strong> {row.latchedLabel}</div>
                          <div className="readiness-operator-detail"><strong>Effective Trading State:</strong> {row.effectiveTradingStateLabel}</div>
                          <div className="readiness-operator-detail"><strong>Lane Roster Signal:</strong> {row.rawEligibilityStateLabel}</div>
                          <div className="readiness-operator-detail"><strong>Clear Action:</strong> {row.clearActionLabel}</div>
                          <div className="readiness-operator-detail"><strong>Latch vs Session:</strong> {row.latchVsSessionDetail}</div>
                          <div className="readiness-operator-detail"><strong>Recovery:</strong> {row.recoveryDetail}</div>
                          <div className="readiness-operator-detail"><strong>Same-Session Override:</strong> {row.supportedOverrideLabel}</div>
                          {row.canForceSessionOverride ? (
                            <div className="action-row inline">
                              <ControlButton
                                label="Force Lane Resume (Session Override)"
                                danger
                                onClick={() =>
                                  void runCommand(
                                    "paper-force-lane-resume-session-override",
                                    () =>
                                      api.runDashboardAction("paper-force-lane-resume-session-override", {
                                        lane_id: row.laneId,
                                        lane_name: row.laneLabel,
                                        symbol: row.symbolLabel,
                                        halt_reason: row.haltReasonRaw,
                                        override_note: "Operator-confirmed same-session resume despite realized-loser session halt.",
                                        session_override_confirmed: true,
                                      }),
                                    {
                                      requiresLive: true,
                                      confirmMessage: `Force a same-session resume override for ${row.laneLabel} (${row.symbolLabel})? This bypasses the realized-loser session protection for this lane until the next session reset.`,
                                    },
                                  )
                                }
                                busyAction={busyAction}
                                disabled={!canRunLiveActions || localOperatorAuth.auth_available !== true}
                              />
                            </div>
                          ) : null}
                        </div>
                      ))}
                    </div>
                    <div className="notice-strip compact">
                      {anyLaneNextSessionResetRequired ? (
                        <>
                          <div><strong>Operator truth:</strong> For `lane_realized_loser_limit_per_session`, these lanes are done for the current session. The default recovery path is automatic next-session reset, not manual same-session restore.</div>
                          <div><strong>What happens at reset:</strong> The runtime clears the stale session-bound halt latch automatically when the next valid session boundary is reached.</div>
                          <div><strong>What does not happen:</strong> The app does not auto-override active same-session realized-loss protection.</div>
                          <div><strong>Session override path:</strong> `Force Lane Resume (Session Override)` is a separate audited operator action. It clears the lane latch and bypasses the realized-loser session gate for that lane only until the next session reset.</div>
                        </>
                      ) : (
                        <>
                          <div><strong>Operator path:</strong> 1. `Clear Risk Halts` clears the active lane-level risk halt. 2. `Resume Entries` re-enables entries if the runtime is still intentionally halted afterward.</div>
                          <div>Use this path when a lane is in `HALTED_DEGRADATION` and the clear action does not require a session reset.</div>
                        </>
                      )}
                    </div>
                    <div className="action-row inline">
                      <ControlButton
                        label="Clear Risk Halts"
                        onClick={() => void runCommand("paper-clear-risk-halts", () => api.runDashboardAction("paper-clear-risk-halts"), { requiresLive: true })}
                        busyAction={busyAction}
                        disabled={!canRunLiveActions || !laneHaltRecoveryRows.length}
                      />
                      <ControlButton
                        label="Resume Entries"
                        onClick={() => void runCommand("paper-resume-entries", () => api.runDashboardAction("paper-resume-entries"), { requiresLive: true })}
                        busyAction={busyAction}
                        disabled={!canRunLiveActions || !(paperReadiness.runtime_running === true)}
                      />
                    </div>
                  </div>
                ) : null}
                {sessionOverrideRows.length ? (
                  <div className="readiness-operator-panel">
                    <div className="readiness-operator-header">
                      <div className="subsection-title">Session Overrides Active</div>
                      <Badge label="OVERRIDDEN THIS SESSION" tone="warn" />
                    </div>
                    <div className="readiness-operator-grid">
                      {sessionOverrideRows.map((row) => (
                        <div key={row.laneId} className="readiness-operator-card">
                          <div className="readiness-operator-card-header">
                            <div className="readiness-operator-name">{row.laneLabel}</div>
                            <div className="badge-row compact">
                              <Badge label={row.symbolLabel} tone="muted" />
                              <Badge label="OVERRIDDEN THIS SESSION" tone="warn" />
                            </div>
                          </div>
                          <div className="readiness-operator-detail"><strong>Reason:</strong> {row.reasonLabel}</div>
                          <div className="readiness-operator-detail"><strong>Applied:</strong> {row.appliedAtLabel}</div>
                          <div className="readiness-operator-detail"><strong>Operator:</strong> {row.appliedByLabel}</div>
                          <div className="readiness-operator-detail"><strong>Scope:</strong> {row.scopeLabel}</div>
                          <div className="readiness-operator-detail"><strong>Note:</strong> {row.noteLabel}</div>
                        </div>
                      ))}
                    </div>
                    <div className="notice-strip compact">
                      <div><strong>Operator truth:</strong> This lane has an active same-session override. Normal realized-loser session policy resumes automatically at the next session reset.</div>
                    </div>
                  </div>
                ) : null}
                {runtimeFaultDetailRows.length ? (
                  <div className="readiness-operator-panel">
                    <div className="readiness-operator-header">
                      <div className="subsection-title">Runtime Faults</div>
                      <Badge label="TRUE BLOCKERS" tone="danger" />
                    </div>
                    <div className="readiness-operator-grid">
                      {runtimeFaultDetailRows.map((row, index) => (
                        <div key={`${row.title}-${index}`} className="readiness-operator-card danger">
                          <div className="readiness-operator-card-header">
                            <div className="readiness-operator-name">{row.title}</div>
                            <Badge label={row.severityLabel} tone="danger" />
                          </div>
                          <div className="readiness-operator-detail"><strong>Detail:</strong> {row.detailText}</div>
                          {row.recommendationText ? <div className="readiness-operator-detail"><strong>Recommendation:</strong> {row.recommendationText}</div> : null}
                          <div className="readiness-operator-detail"><strong>Fault handling:</strong> {row.staleOnlyClear}</div>
                        </div>
                      ))}
                    </div>
                    <div className="action-row inline">
                      <ControlButton
                        label="Clear Fault"
                        onClick={() => void runCommand("paper-clear-fault", () => api.runDashboardAction("paper-clear-fault"), { requiresLive: true })}
                        busyAction={busyAction}
                        disabled={!canRunLiveActions || !runtimeFaultDetailRows.length}
                      />
                    </div>
                  </div>
                ) : null}
                <div className="notice-strip">
                  <div>True runtime blockers: {runtimeFaultSummary}</div>
                  <div>Degraded informational feeds: {degradedFeedSummary}</div>
                  <div>Lane-local degradation halts: {haltedLaneSummary}</div>
                  {atpeLongLaneHalts.length ? (
                    <div>
                      ATPE long MES/MNQ are halted because the lane realized loser limit per session was reached. The runtime is still up; these are lane-local session-bound halts that auto-clear at the next session reset unless you deliberately use the audited same-session override.
                    </div>
                  ) : null}
                  {runtimeBlockingFaultRows.some((row) => String(row.code ?? "") === "DECISION_WITHOUT_INTENT") ? (
                    <div>`DECISION_WITHOUT_INTENT` remains a fault item. `Clear Fault` is only appropriate if it is stale after you verify the missing intent condition is no longer real.</div>
                  ) : null}
                </div>
              </Section>

              <Section title="Strategy Risk Context" subtitle="At-a-glance strategy-ledger exposure and drawdown context">
                <div className="metric-grid">
                  {homeMetrics.map((item) => (
                    <MetricCard key={item.label} label={item.label} value={item.value} tone={statusTone(item.value)} />
                  ))}
                </div>
                <div className="status-line">{formatValue(portfolio.status_line)}</div>
              </Section>

              <Section title="Market Context" subtitle="Trustworthy context, not decorative clutter">
                <div className="market-strip">
                  {asArray<JsonRecord>(marketContext.items).map((item) => (
                    <div key={String(item.label)} className="market-tile">
                      <div className="market-label">{formatValue(item.label)}</div>
                      <div className="market-value">{formatValue(item.value_label ?? item.value)}</div>
                      <Badge label={item.status_label ?? item.status ?? "Unknown"} tone={statusTone(item.status_label ?? item.status)} />
                      <div className="market-note">{formatValue(item.note ?? item.reason)}</div>
                    </div>
                  ))}
                </div>
              </Section>

              <Section title="Instrument Rollup Preview" subtitle="Compact view of active surfaced instruments">
                <DataTable
                  columns={[
                    { key: "instrument", label: "Symbol" },
                    { key: "realized_pnl", label: "Today Realized", render: (row) => formatMaybePnL(row.realized_pnl) },
                    { key: "unrealized_pnl", label: "Unrealized", render: (row) => formatMaybePnL(row.unrealized_pnl) },
                    { key: "net_pnl", label: "Net", render: (row) => formatMaybePnL(row.net_pnl) },
                    { key: "current_session_max_drawdown", label: "Session Max DD", render: (row) => formatMaybePnL(row.current_session_max_drawdown) },
                    { key: "active_position_count", label: "Positions" },
                    { key: "active_signal_count", label: "Signals" },
                    { key: "blocked_lane_count", label: "Blocked" },
                    { key: "latest_activity_timestamp", label: "Latest Activity", render: (row) => formatTimestamp(row.latest_activity_timestamp) },
                    { key: "warning_summary", label: "Warnings" },
                  ]}
                  rows={instrumentRollup}
                  emptyLabel="No instrument rollup rows are currently available."
                />
              </Section>

              <Section title="Active Lane Preview" subtitle="Can the lane fire, and if not, why not?">
                <DataTable
                  columns={[
                    { key: "display_name", label: "Lane" },
                    { key: "instrument", label: "Symbol" },
                    { key: "classification", label: "Classification" },
                    { key: "session", label: "Current Session" },
                    { key: "blocked", label: "Eligible", render: (row) => (row.blocked ? "No" : "Yes") },
                    { key: "warning_summary", label: "Blocker / Warning" },
                    { key: "latest_timestamp", label: "Latest Activity", render: (row) => formatTimestamp(row.latest_timestamp) },
                  ]}
                  rows={laneRows}
                  emptyLabel="No active lane preview rows are currently available."
                />
              </Section>

              <Section title="Current Active Positions" subtitle="Actual open exposure">
                <DataTable
                  columns={[
                    { key: "symbol", label: "Symbol", render: (row) => formatValue(row.symbol ?? row.instrument) },
                    { key: "side", label: "Side" },
                    { key: "quantity", label: "Qty", render: (row) => formatValue(row.quantity ?? row.qty) },
                    { key: "realized", label: "Realized", render: (row) => formatMaybePnL(row.realized ?? row.realized_pnl) },
                    { key: "unrealized", label: "Unrealized", render: (row) => formatMaybePnL(row.unrealized ?? row.unrealized_pnl) },
                    { key: "net", label: "Net", render: (row) => formatMaybePnL(row.net ?? row.net_pnl) },
                    { key: "exit", label: "Exit", render: (row) => formatValue(row.exit ?? row.active_exit) },
                    { key: "warnings", label: "Warnings", render: (row) => formatValue(row.warnings) },
                  ]}
                  rows={currentPositions}
                  emptyLabel="No active positions are currently open."
                />
              </Section>
            </>
          ) : null}

          {!loading && page === "runtime" ? (
            <>
              <Section title="Startup / Bind" subtitle="Local dashboard URL, chosen port, and startup errors">
                <StartupPanel
                  desktopState={desktopState}
                  busyAction={busyAction}
                  onRetryStart={() => void runCommand("start-dashboard", () => api.startDashboard())}
                />
              </Section>

              <Section title="Paper Runtime Launch" subtitle="Backend-owned runtime recovery status for the current paper soak session">
                <PaperStartupPanel
                  metrics={paperStartupMetrics}
                  stateLabel={paperStartupStateLabel}
                  reason={paperStartupReasonText}
                  actionLabel={paperStartupActionLabel}
                  actionDescription={paperStartupActionDescription}
                  busyAction={busyAction}
                  canRunLiveActions={canRunLiveActions}
                  onStartDashboard={() => void runCommand("start-dashboard", () => api.startDashboard())}
                  onStartPaper={() => void runCommand("start-paper", () => api.runDashboardAction("start-paper"), { requiresLive: true })}
                  onRestartPaperWithTempPaper={() =>
                    void runCommand("restart-paper-with-temp-paper", () => api.runDashboardAction("restart-paper-with-temp-paper"), {
                      confirmMessage: "Restart Runtime + Temp Paper will restart the paper soak with all enabled temporary paper lanes included. Proceed?",
                      requiresLive: true,
                    })
                  }
                  onAuthGateCheck={() => void runCommand("auth-gate-check", () => api.runDashboardAction("auth-gate-check"), { requiresLive: true })}
                  onCompletePreSessionReview={() =>
                    void runCommand("complete-pre-session-review", () => api.runDashboardAction("complete-pre-session-review"), { requiresLive: true })
                  }
                />
              </Section>

              <Section title="Sunday Open Preflight" subtitle="Operator readiness before the Sunday open">
                <PreflightPanel model={preflight} />
              </Section>

              <Section title="Process Control" subtitle="Local Python service lifecycle">
                <div className="control-grid">
                  <ControlButton label="Start Dashboard/API" onClick={() => void runCommand("start-dashboard", () => api.startDashboard())} busyAction={busyAction} />
                  <ControlButton
                    label="Stop Dashboard/API"
                    onClick={() =>
                      void runCommand("stop-dashboard", () => api.stopDashboard(), {
                        confirmMessage: "Stop Dashboard/API will take the operator surface offline. Proceed?",
                      })
                    }
                    busyAction={busyAction}
                    danger
                  />
                  <ControlButton
                    label="Restart Dashboard/API"
                    onClick={() =>
                      void runCommand("restart-dashboard", () => api.restartDashboard(), {
                        confirmMessage: "Restart Dashboard/API will interrupt live operator refresh briefly. Proceed?",
                      })
                    }
                    busyAction={busyAction}
                    danger
                  />
                  <ControlButton
                    label="Start Runtime"
                    onClick={() => void runCommand("start-paper", () => api.runDashboardAction("start-paper"), { requiresLive: true })}
                    busyAction={busyAction}
                    disabled={!canRunLiveActions}
                  />
                  <ControlButton
                    label="Restart Runtime + Temp Paper"
                    onClick={() =>
                      void runCommand("restart-paper-with-temp-paper", () => api.runDashboardAction("restart-paper-with-temp-paper"), {
                        confirmMessage: "Restart Runtime + Temp Paper will restart the paper soak with all enabled temporary paper lanes included. Proceed?",
                        requiresLive: true,
                      })
                    }
                    busyAction={busyAction}
                    disabled={!canRunLiveActions}
                  />
                  <ControlButton
                    label="Stop Runtime"
                    onClick={() =>
                      void runCommand("stop-paper", () => api.runDashboardAction("stop-paper"), {
                        confirmMessage: "Stop Runtime will stop the current paper runtime. Proceed?",
                        requiresLive: true,
                      })
                    }
                    busyAction={busyAction}
                    disabled={!canRunLiveActions}
                    danger
                  />
                  <ControlButton
                    label="Emergency Halt Entries"
                    onClick={() =>
                      void runCommand("paper-halt-entries", () => api.runDashboardAction("paper-halt-entries"), {
                        confirmMessage: "Emergency Halt will stop new paper entries immediately. Proceed?",
                        requiresLive: true,
                      })
                    }
                    busyAction={busyAction}
                    disabled={!canRunLiveActions}
                    danger
                  />
                  <ControlButton
                    label="Resume Entries"
                    onClick={() => void runCommand("paper-resume-entries", () => api.runDashboardAction("paper-resume-entries"), { requiresLive: true })}
                    busyAction={busyAction}
                    disabled={!canRunLiveActions}
                  />
                  <ControlButton
                    label="Auth Gate Check"
                    onClick={() => void runCommand("auth-gate-check", () => api.runDashboardAction("auth-gate-check"), { requiresLive: true })}
                    busyAction={busyAction}
                    disabled={!canRunLiveActions}
                  />
                </div>
              </Section>

              <Section title="Temp-Paper Runtime Integrity" subtitle="Enabled temporary paper lanes must also be present in the running paper runtime">
                <div className="metric-grid">
                  {temporaryPaperIntegrityMetrics.map((item) => (
                    <MetricCard key={item.label} label={item.label} value={item.value} tone={item.tone} />
                  ))}
                </div>
                <div className="status-line">{formatValue(temporaryPaperRuntimeIntegrity.summary_line ?? "No temporary paper integrity summary is available.")}</div>
                <div className="notice-strip">
                  <div>{formatValue(temporaryPaperRuntimeIntegrity.note ?? "Snapshot-only temporary paper rows remain visible for audit, but they are not live runtime lanes.")}</div>
                  <div>Missing lane ids: {temporaryPaperMissingLaneIds.length ? temporaryPaperMissingLaneIds.join(", ") : "None"}</div>
                  <div>Required start flags: {temporaryPaperStartFlags.length ? temporaryPaperStartFlags.join(" ") : "None"}</div>
                  {temporaryPaperUnresolvedLaneIds.length ? <div>Unmapped lane ids: {temporaryPaperUnresolvedLaneIds.join(", ")}</div> : null}
                </div>
                <DataTable
                  columns={[
                    { key: "display_name", label: "Temporary Paper Lane" },
                    { key: "enabled_in_app", label: "Enabled In App", render: (row) => formatValue(row.enabled_in_app ?? false) },
                    { key: "runtime_instance_present", label: "Loaded In Runtime", render: (row) => formatValue(row.runtime_instance_present ?? false) },
                    { key: "runtime_state_loaded", label: "Runtime State Loaded", render: (row) => formatValue(row.runtime_state_loaded ?? false) },
                    { key: "truth_label", label: "Truth" },
                    { key: "start_flag", label: "Start Flag", render: (row) => formatValue(row.start_flag ?? "Unavailable") },
                    { key: "last_update_timestamp", label: "Latest Snapshot", render: (row) => formatTimestamp(row.last_update_timestamp) },
                  ]}
                  rows={temporaryPaperIntegrityRows}
                  emptyLabel="No temporary paper strategies are currently surfaced."
                />
              </Section>

              <Section title="Paper Capture Integrity" subtitle="Live runtime truth versus stale strategy-ledger timestamps">
                <div className="metric-grid">
                  {paperCaptureIntegrityMetrics.map((item) => (
                    <MetricCard key={item.label} label={item.label} value={item.value} tone={item.tone} />
                  ))}
                </div>
                <div className="status-line">
                  {paperRuntimeStopped
                    ? "The paper soak is currently stopped. Strategy rows can remain visible from persisted lane databases, but they are not refreshing live."
                    : captureHealthLabel === "CAPTURE HEALTHY"
                      ? "The paper runtime is live and the latest strategy activity is tracking current runtime updates."
                      : "Live operator visibility is degraded. Review runtime status, stale rows, and temp-paper mismatch before trusting recent activity timestamps."}
                </div>
                <div className="notice-strip">
                  <div>Latest runtime update: {latestRuntimeCaptureTimestamp ? formatRelativeAge(latestRuntimeCaptureTimestamp) : "Unavailable"}.</div>
                  <div>Latest strategy activity: {latestStrategyActivityTimestamp ? formatRelativeAge(latestStrategyActivityTimestamp) : "Unavailable"}.</div>
                  <div>Stale strategy rows: {staleStrategyRows.length ? staleStrategyRows.slice(0, 6).map((row) => String(row.strategy_name ?? row.display_name ?? row.lane_id ?? "")).join(", ") : "None"}.</div>
                </div>
              </Section>

              <Section title="Runtime Identity" subtitle="Current dashboard process and bridge state">
                <div className="metric-grid">
                  <MetricCard label="Backend URL" value={desktopState?.backendUrl ?? "Unavailable"} />
                  <MetricCard label="Preferred URL" value={formatValue(startup?.preferredUrl)} />
                  <MetricCard label="Chosen Port" value={formatValue(startup?.chosenPort)} />
                  <MetricCard label="Ownership" value={ownershipLabel(startup?.ownership)} />
                  <MetricCard label="Backend State" value={desktopState?.backend.label ?? "Unknown"} tone={statusTone(desktopState?.backend.label)} />
                  <MetricCard label="Source Mode" value={desktopState?.source.label ?? "Unknown"} tone={statusTone(desktopState?.source.label)} />
                  <MetricCard label="Port Policy" value={startup?.allowPortFallback ? "Explicit fallback enabled" : "Fixed preferred port"} />
                  <MetricCard label="Failure Kind" value={startupFailureLabel(desktopState?.backend.startupFailureKind)} tone={statusTone(desktopState?.backend.startupFailureKind)} />
                  <MetricCard label="Build Hash" value={formatValue(dashboardMeta.build_stamp ?? dashboardMeta.version_label)} />
                  <MetricCard label="Server PID" value={formatValue(dashboardMeta.server_pid ?? desktopState?.backend.pid)} />
                  <MetricCard label="Started At" value={formatTimestamp(dashboardMeta.server_started_at)} />
                  <MetricCard label="Retry Count" value={formatValue(desktopState?.backend.retryCount)} />
                  <MetricCard label="Next Retry" value={formatTimestamp(desktopState?.backend.nextRetryAt)} />
                  <MetricCard label="Manager Owned" value={formatValue(desktopState?.backend.managerOwned)} />
                  <MetricCard label="Info Files" value={infoFiles.length ? `${infoFiles.length} tracked` : "None"} />
                </div>
              </Section>

              <Section title="Health / Readiness" subtitle="Live health checks or latest snapshot truth">
                <div className="metric-grid">
                  <MetricCard label="Dashboard Health Status" value={formatValue(desktopState?.health?.status ?? desktopState?.backend.healthStatus)} tone={statusTone(desktopState?.health?.status ?? desktopState?.backend.healthStatus)} />
                  <MetricCard label="Dashboard Probe Ready" value={formatValue(desktopState?.health?.ready ?? false)} />
                  <MetricCard label="Entries Enabled" value={formatValue(global.entries_enabled ?? paperReadiness.entries_enabled)} />
                  <MetricCard label="Broker / Auth" value={formatValue(global.auth_label ?? runtimeValues.auth_readiness)} />
                  <MetricCard label="Market Data" value={formatValue(global.market_data_label)} />
                  <MetricCard label="Persistence" value={formatValue(asRecord(desktopState?.health?.checks).operator_surface_loadable?.detail ?? "Loaded through dashboard snapshot")} />
                </div>
                <JsonBlock value={desktopState?.health ?? runtimeReadiness} />
              </Section>

              <Section title="Manager Output" subtitle="Recent dashboard launch/stop lines from the Electron-owned process manager">
                <CodeBlock lines={desktopState?.manager.recentOutput ?? []} emptyLabel="No managed process output has been captured in this desktop session." />
              </Section>
            </>
          ) : null}

          {!loading && page === "calendar" ? (
            <>
              <Section
                title="P&L Calendar"
                subtitle="Historical performance workspace with provenance-safe source selection and direct routing into strategy detail"
                className="calendar-page-section"
                headerClassName="section-header-tight"
              >
                <div className="calendar-toolbar">
                  <div className="calendar-period-group" role="group" aria-label="Calendar period">
                    {[
                      ["monthly", "Monthly"],
                      ["weekly", "Weekly"],
                      ["quarterly", "Quarterly"],
                      ["ytd", "YTD"],
                      ["custom", "Custom"],
                    ].map(([value, label]) => (
                      <button
                        key={value}
                        className={`calendar-pill ${calendarPeriod === value ? "active" : ""}`}
                        onClick={() => setCalendarPeriod(value as PnlCalendarPeriod)}
                        data-calendar-period={value}
                      >
                        {label}
                      </button>
                    ))}
                  </div>
                  <div className="calendar-mode-row">
                    <div className="calendar-view-group" role="group" aria-label="Calendar view">
                      {[
                        ["calendar", "Calendar"],
                        ["line", "Line Graph"],
                        ["bar", "Bar"],
                      ].map(([value, label]) => (
                        <button
                          key={value}
                          className={`calendar-pill ${effectiveCalendarViewMode === value ? "active" : ""}`}
                          onClick={() => setCalendarViewMode(value as PnlCalendarViewMode)}
                          disabled={calendarPeriod === "ytd" && value === "calendar"}
                          data-calendar-view-toggle={value}
                        >
                          {label}
                        </button>
                      ))}
                    </div>
                    <div className="calendar-nav-group">
                      <button
                        className="calendar-nav-button"
                        onClick={() =>
                          setCalendarAnchorDate((current) =>
                            calendarPeriod === "weekly"
                              ? addDays(current, -7)
                              : calendarPeriod === "quarterly"
                                ? addMonths(current, -3)
                                : addMonths(current, -1),
                          )
                        }
                        aria-label="Previous period"
                      >
                        &lt;
                      </button>
                      <div className="calendar-period-label">{calendarPeriodTitle}</div>
                      <button
                        className="calendar-nav-button"
                        onClick={() =>
                          setCalendarAnchorDate((current) =>
                            calendarPeriod === "weekly"
                              ? addDays(current, 7)
                              : calendarPeriod === "quarterly"
                                ? addMonths(current, 3)
                                : addMonths(current, 1),
                          )
                        }
                        aria-label="Next period"
                      >
                        &gt;
                      </button>
                    </div>
                  </div>
                  <div className="calendar-filter-row">
                    <label className="settings-field compact">
                      <span>Truth Basis</span>
                      <select value={calendarSource} onChange={(event) => setCalendarSource(event.target.value as PnlCalendarSource)} data-calendar-source>
                        <option value="all">All Accounts</option>
                        <option value="live">Live</option>
                        <option value="paper">Paper</option>
                        <option value="benchmark_replay">Benchmark / Replay</option>
                        <option value="research_execution">Research Execution</option>
                      </select>
                    </label>
                    {calendarPeriod === "custom" ? (
                      <div className="calendar-custom-range">
                        <label className="settings-field compact">
                          <span>From</span>
                          <input type="date" value={calendarCustomStart} onChange={(event) => setCalendarCustomStart(event.target.value)} />
                        </label>
                        <label className="settings-field compact">
                          <span>To</span>
                          <input type="date" value={calendarCustomEnd} onChange={(event) => setCalendarCustomEnd(event.target.value)} />
                        </label>
                      </div>
                    ) : null}
                    <div className="calendar-toolbar-actions">
                      <button
                        className="panel-button subtle"
                        onClick={() =>
                          void api.copyText([
                            "date,gross_pnl,trade_count,cumulative_pnl",
                            ...calendarDayPoints.map((point) => `${point.date},${point.pnl.toFixed(2)},${point.tradeCount},${point.cumulative.toFixed(2)}`),
                          ].join("\n"))
                        }
                      >
                        Copy CSV
                      </button>
                    </div>
                  </div>
                </div>

                <div className="notice-strip compact calendar-provenance-strip">
                  <div><strong>Source:</strong> {calendarSourceSelection.selectedSourceLabel}</div>
                  <div>{calendarSourceSelection.note}</div>
                </div>

                {calendarAlertRows.length ? (
                  <div className="calendar-alert-strip">
                    {calendarAlertRows.map((alert) => (
                      <div key={alert.label} className={`calendar-alert-card ${alert.tone}`}>
                        <div className="calendar-alert-label">{alert.label}</div>
                        <div className="calendar-alert-note">{alert.note}</div>
                      </div>
                    ))}
                  </div>
                ) : null}

                <div className="calendar-kpi-strip">
                  {calendarKpis.map((metric) => (
                    <MetricCard key={metric.label} label={metric.label} value={metric.value} tone={metric.tone} />
                  ))}
                </div>

                {effectiveCalendarViewMode === "calendar" ? (
                  <div className="calendar-board-shell" data-calendar-surface="monthly">
                    <div className="calendar-weekday-row">
                      {["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"].map((label) => (
                        <div key={label} className="calendar-weekday-cell">{label}</div>
                      ))}
                    </div>
                    <div className={`calendar-grid ${calendarPeriod === "weekly" ? "weekly" : ""}`}>
                      {calendarGridDays.map((dateKey) => {
                        const point = calendarDayPointMap.get(dateKey) ?? null;
                        const isSelected = selectedCalendarDay === dateKey;
                        const isToday = dateKey === new Date().toISOString().slice(0, 10);
                        const outsideMonth = calendarPeriod === "monthly" && !dateKey.startsWith(startOfMonth(calendarAnchorDate).slice(0, 7));
                        const weekend = isWeekendDate(dateKey);
                        return (
                          <button
                            key={dateKey}
                            className={[
                              "calendar-day-cell",
                              point ? (point.pnl >= 0 ? "positive" : "negative") : weekend ? "weekend" : "flat",
                              isSelected ? "selected" : "",
                              isToday ? "today" : "",
                              outsideMonth ? "outside-month" : "",
                            ].filter(Boolean).join(" ")}
                            onClick={() => setSelectedCalendarDay(dateKey)}
                            data-calendar-day={dateKey}
                          >
                            <div className="calendar-day-number">{dateFromKey(dateKey).getDate()}</div>
                            {weekend && !point ? (
                              <div className="calendar-day-weekend">Weekend</div>
                            ) : (
                              <>
                                <div className={`calendar-day-pnl ${point ? pnlTone(point.pnl) : "muted"}`}>{point ? formatSignedCompactWhole(point.pnl) : "—"}</div>
                                <div className="calendar-day-meta">
                                  <span>
                                    {point
                                      ? point.tradeCount > 0
                                        ? `${point.tradeCount}T`
                                        : point.coveredSources.length
                                          ? "Covered"
                                          : "0T"
                                      : "No trades"}
                                  </span>
                                  <span>{point ? `cum:${formatSignedCompactWhole(point.cumulative)}` : ""}</span>
                                </div>
                              </>
                            )}
                          </button>
                        );
                      })}
                    </div>
                  </div>
                ) : (
                  <CalendarHistoryChart
                    mode={effectiveCalendarViewMode}
                    title={effectiveCalendarViewMode === "line" ? "Cumulative + Daily Progression" : "Daily P&L Bars"}
                    subtitle={effectiveCalendarViewMode === "line" ? "Cumulative curve uses completed day totals; daily series stays provenance-safe to the selected source." : "Daily P&L bars for the currently selected source and period."}
                    points={calendarDayPoints}
                  />
                )}

                {selectedCalendarDayPoint ? (
                  <CalendarDayDetailPanel
                    day={selectedCalendarDayPoint}
                    sourceLabel={`${calendarSourceSelection.selectedSourceLabel} • ${calendarPeriodTitle}`}
                    onClose={() => setSelectedCalendarDay(null)}
                    onOpenStrategy={openCalendarContributionStrategy}
                  />
                ) : null}
              </Section>
            </>
          ) : null}

          {!loading && page === "strategies" ? (
            <>
              <Section
                title="Strategy Deep-Dive Workspace"
                subtitle="Select one strategy lane, inspect core performance, and verify current runtime posture without opening the analysis lab first"
                className="strategy-deep-dive-section"
                headerClassName="section-header-tight"
              >
                {selectedWorkspaceRow ? (
                  <div className="local-workspace-strip">
                    <div className="local-workspace-copy">
                      <div className="local-workspace-kicker">Selected Workspace</div>
                      <div className="local-workspace-title">{compactBranchLabel(selectedWorkspaceRow)}</div>
                      <div className="local-workspace-meta">
                        <Badge label={laneClassLabel(selectedWorkspaceRow)} tone={paperStrategyClassTone(selectedWorkspaceRow)} />
                        <Badge label={selectedWorkspaceDesignation} tone={selectedWorkspaceRow?.benchmark_designation ? "warn" : selectedWorkspaceRow?.candidate_designation ? "good" : "muted"} />
                        <Badge label={runtimeAttachmentLabel(selectedWorkspaceRow)} tone={runtimeAttachmentLabel(selectedWorkspaceRow) === "Attached Live" ? "good" : runtimeAttachmentLabel(selectedWorkspaceRow) === "Audit Only" ? "warn" : "muted"} />
                        <span>{selectedWorkspaceInstrument || "No instrument"}</span>
                        <span>Exec {formatValue(selectedWorkspaceRow?.execution_timeframe ?? "1m")}</span>
                        <span>Context {selectedWorkspaceContextTimes}</span>
                        <span>Last eval {formatTimestamp(selectedWorkspaceRow?.last_execution_bar_evaluated_at)}</span>
                      </div>
                    </div>
                    <div className="local-workspace-actions">
                      <button className="panel-button subtle" onClick={() => selectWorkspaceLane(String(selectedWorkspaceRow?.lane_id ?? ""), { navigateTo: "market" })}>
                        Open Trade Entry
                      </button>
                      <button
                        className="panel-button subtle"
                        onClick={() => {
                          window.location.hash = "#/diagnostics";
                          setPage("diagnostics");
                        }}
                      >
                        Open Evidence
                      </button>
                    </div>
                  </div>
                ) : null}
                {playback.available === true ? (
                  <div className="notice-strip compact strategy-context-strip">
                    <div><strong>Historical Playback</strong> {playbackSyncLabel}</div>
                    <div><strong>Run</strong> {playbackRunStampLabel}</div>
                    <div><strong>Study Catalog</strong> {formatShortNumber(playbackStudyItems.length)} catalog rows / {formatShortNumber(playbackLatestStudyItems.length)} latest unique studies</div>
                    <div><strong>Strategy Coverage</strong> {formatShortNumber(playbackStrategyCount)} strategies / {formatShortNumber(playbackLaneCount)} lanes</div>
                    <div><strong>Where To Inspect</strong> Strategy Analysis Lab below carries the ranked replay/back-cast lanes and linked study surface.</div>
                  </div>
                ) : null}
                <div className="strategy-focus-layout">
                  <div className="strategy-focus-sidebar">
                    <label className="settings-field">
                      <span>Select Strategy</span>
                      <select value={selectedWorkspaceLaneId} onChange={(event) => selectWorkspaceLane(event.target.value)}>
                        {dashboardRosterRows.map((row) => (
                          <option key={String(row.lane_id ?? row.branch)} value={String(row.lane_id ?? "")}>
                            {compactBranchLabel(row)}
                          </option>
                        ))}
                      </select>
                    </label>
                    {calendarContextLabel ? (
                      <div className="notice-strip compact strategy-context-strip">
                        <div><strong>Calendar Context</strong></div>
                        <div>{calendarContextLabel}</div>
                      </div>
                    ) : null}
                    <div className="metric-grid compact strategy-metric-grid">
                      <MetricCard label="Sharpe Proxy" value={formatValue(selectedWorkspacePerformanceRow?.operator_interpretation_state ?? "—")} />
                      <MetricCard label="Win Rate" value={formatValue(selectedWorkspacePerformanceRow?.operator_interpretation ?? "Sparse history")} />
                      <MetricCard label="Trade Count" value={formatShortNumber(selectedWorkspacePerformanceRow?.trade_count ?? selectedWorkspaceTrades.length)} />
                      <MetricCard label="Avg Trade P&L" value={renderPnlValue(selectedWorkspaceTrades.length ? selectedWorkspaceTrades.reduce((sum, row) => sum + (Number(row.realized_pnl ?? 0) || 0), 0) / selectedWorkspaceTrades.length : 0)} tone={pnlTone(selectedWorkspaceTrades.length ? selectedWorkspaceTrades.reduce((sum, row) => sum + (Number(row.realized_pnl ?? 0) || 0), 0) / selectedWorkspaceTrades.length : 0)} />
                      <MetricCard label="Status" value={formatValue(selectedWorkspaceRow?.strategy_status ?? selectedWorkspacePerformanceRow?.status ?? "Unknown")} tone={statusTone(selectedWorkspaceRow?.strategy_status ?? selectedWorkspacePerformanceRow?.status)} />
                      <MetricCard label="30D Est P&L" value={renderPnlValue(selectedWorkspacePerformanceRow?.cumulative_pnl ?? selectedWorkspacePerformanceRow?.realized_pnl)} tone={pnlTone(selectedWorkspacePerformanceRow?.cumulative_pnl ?? selectedWorkspacePerformanceRow?.realized_pnl)} />
                      <MetricCard label="Participation" value={formatValue(selectedWorkspaceRow?.participation_policy ?? "Unavailable")} />
                      <MetricCard label="Can Add More" value={selectedWorkspaceRow?.additional_entry_allowed === true ? "YES" : "NO"} tone={selectedWorkspaceRow?.additional_entry_allowed === true ? "good" : "muted"} />
                    </div>
                  </div>
                  <div className="strategy-focus-main">
                    <TrendPanel
                      title="Equity Curve"
                      subtitle="Strategy-level realized equity progression from the persisted trade ledger."
                      points={selectedWorkspaceEquityCurve}
                      tone={pnlTone(selectedWorkspacePerformanceRow?.realized_pnl)}
                      footer={`Execution ${formatValue(selectedWorkspaceRow?.execution_timeframe ?? "1m")} | Context ${(asArray<string>(selectedWorkspaceRow?.context_timeframes).join(" / ") || "5m")} | Last eval ${formatTimestamp(selectedWorkspaceRow?.last_execution_bar_evaluated_at)}`}
                      className="strategy-primary-trend"
                    />
                    <div>
                      <h3 className="subsection-title">Trade Log</h3>
                      <DataTable
                        columns={[
                          { key: "entry_timestamp", label: "Date", render: (row) => formatTimestamp(row.entry_timestamp ?? row.exit_timestamp) },
                          { key: "side", label: "Side", render: (row) => formatValue(row.side) },
                          { key: "entry_price", label: "Entry", render: (row) => formatValue(row.entry_price) },
                          { key: "exit_price", label: "Exit", render: (row) => formatValue(row.exit_price) },
                          { key: "realized_pnl", label: "P&L", render: (row) => renderPnlValue(row.realized_pnl) },
                          { key: "exit_reason", label: "Reason", render: (row) => formatValue(row.exit_reason) },
                        ]}
                        rows={selectedWorkspaceTrades}
                        emptyLabel="No trade log rows are available for the selected lane yet."
                      />
                    </div>
                  </div>
                </div>
                {selectedWorkspacePlaybackStudyItem ? (
                  <div className="strategy-playback-preview">
                    <div className="notice-strip compact strategy-context-strip">
                      <div><strong>Historical Study Preview</strong> {formatValue(selectedWorkspacePlaybackStudyItem.label ?? selectedWorkspacePlaybackStudyItem.strategy_id ?? "Loaded")}</div>
                      <div><strong>Coverage</strong> {strategyLaneDateRangeLabel(selectedWorkspacePlaybackCoverage)}</div>
                      <div><strong>Closed Trades</strong> {formatShortNumber(asArray<JsonRecord>(selectedWorkspacePlaybackSummary.closed_trade_breakdown).length)}</div>
                      <div><strong>Net P&L</strong> {renderPnlValue(selectedWorkspacePlaybackSummary.net_pnl ?? selectedWorkspacePlaybackSummary.realized_pnl)}</div>
                    </div>
                    {selectedWorkspacePlaybackCanRenderStudy ? (
                      <ReplayStrategyStudy studies={[{ study_key: String(selectedWorkspacePlaybackStudyItem.study_key ?? "workspace"), label: String(selectedWorkspacePlaybackStudyItem.label ?? "Replay Study"), study: selectedWorkspacePlaybackStudy }]} />
                    ) : (
                      <div className="placeholder-note">Summary and trade economics are loaded for this playback lane; the full bar-by-bar study will appear here as soon as the linked artifact finishes loading.</div>
                    )}
                  </div>
                ) : null}
              </Section>

              <Section title="Strategy Analysis Lab" subtitle="Full ranked analysis, comparison, study, runtime, and diagnostics remain available here when you need the deeper evidence model">
                {strategyAnalysis.available === true ? (
                  <UnifiedStrategyAnalysis
                    analysis={strategyAnalysis}
                    replayStudyItems={playbackLatestStudyItems}
                    preferredStrategyKey={String(selectedWorkspacePerformanceRow?.strategy_key ?? "")}
                    studyPanel={(
                      <>
                        <div className="notice-strip compact">
                          Deep-dive keeps the latest study/equity visualization close to strategy selection. Replay-only artifacts stay secondary when not available.
                        </div>
                        {playbackStudyAvailable ? (
                          <ReplayStrategyStudy studies={[{ study_key: playbackSelectedStudyKey || "latest", label: "Latest replay study", study: playbackStudy }]} />
                        ) : (
                          <div className="placeholder-note">No replay study artifact is attached to the currently selected strategy.</div>
                        )}
                      </>
                    )}
                    runtimePanel={(
                      <div className="notice-strip compact">
                        <div><strong>Execution cadence:</strong> 1m decision surface with completed higher-timeframe context.</div>
                        <div><strong>Paper runtime:</strong> {formatValue(paperReadiness.runtime_phase ?? "STOPPED")} | <strong>Entries enabled:</strong> {formatValue(global.entries_enabled ?? paperReadiness.entries_enabled)}</div>
                      </div>
                    )}
                    diagnosticsPanel={(
                      <div className="notice-strip compact">
                        <div>Strategy diagnostics stay available here, but raw artifact navigation is demoted behind Evidence.</div>
                        <div>Use the roster and lane-level status below when you need runtime attachment truth alongside strategy analysis.</div>
                      </div>
                    )}
                  />
                ) : (
                  <div className="placeholder-note">Strategy analysis is not yet available from the current snapshot.</div>
                )}
              </Section>

              <Section title="Standalone Strategy Lens" subtitle="Filter the strategy surfaces by standalone identity, family, instrument, runtime state, status, and ambiguity">
                <div className="badge-row">
                  <Badge label="RUNTIME TRUTH" tone={truthBadgeTone("RUNTIME TRUTH")} />
                  <Badge label="STRATEGY LEDGER" tone={truthBadgeTone("STRATEGY LEDGER")} />
                  <Badge label="PAPER" tone={truthBadgeTone("PAPER")} />
                </div>
                <div className="ticket-grid">
                  <label className="settings-field">
                    <span>Standalone Strategy</span>
                    <select value={strategyLensIdentityFilter} onChange={(event) => setStrategyLensIdentityFilter(event.target.value)}>
                      <option value="">All standalone strategies</option>
                      {strategyLensIdentityOptions.map((item) => (
                        <option key={item} value={item}>
                          {item}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="settings-field">
                    <span>Family</span>
                    <select value={strategyLensFamilyFilter} onChange={(event) => setStrategyLensFamilyFilter(event.target.value)}>
                      <option value="">All families</option>
                      {strategyLensFamilyOptions.map((item) => (
                        <option key={item} value={item}>
                          {item}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="settings-field">
                    <span>Instrument</span>
                    <select value={strategyLensInstrumentFilter} onChange={(event) => setStrategyLensInstrumentFilter(event.target.value)}>
                      <option value="">All instruments</option>
                      {strategyLensInstrumentOptions.map((item) => (
                        <option key={item} value={item}>
                          {item}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="settings-field">
                    <span>Status</span>
                    <select value={strategyLensStatusFilter} onChange={(event) => setStrategyLensStatusFilter(event.target.value)}>
                      <option value="">All statuses</option>
                      {strategyLensStatusOptions.map((item) => (
                        <option key={item} value={item}>
                          {item}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="settings-field">
                    <span>Runtime State</span>
                    <select value={strategyLensRuntimeStateFilter} onChange={(event) => setStrategyLensRuntimeStateFilter(event.target.value)}>
                      <option value="">All runtime states</option>
                      <option value="READY">Ready</option>
                      <option value="INSTANCE_ONLY">Instance only</option>
                      <option value="SURFACED_ONLY">Surfaced only</option>
                      <option value="MISSING">Missing</option>
                    </select>
                  </label>
                  <label className="settings-field">
                    <span>Ambiguity</span>
                    <select value={strategyLensAmbiguityFilter} onChange={(event) => setStrategyLensAmbiguityFilter(event.target.value)}>
                      <option value="">All rows</option>
                      <option value="AMBIGUOUS">Same-underlying ambiguity</option>
                      <option value="CLEAR">No ambiguity</option>
                    </select>
                  </label>
                </div>
              </Section>

              <Section title="Temp-Paper Runtime Integrity" subtitle="Temporary paper rows should never appear live unless the running paper soak actually loaded them">
                <div className="metric-grid">
                  {temporaryPaperIntegrityMetrics.map((item) => (
                    <MetricCard key={item.label} label={item.label} value={item.value} tone={item.tone} />
                  ))}
                </div>
                <div className="status-line">{formatValue(temporaryPaperRuntimeIntegrity.summary_line ?? "No temporary paper integrity summary is available.")}</div>
                <div className="notice-strip">
                  <div>{formatValue(temporaryPaperRuntimeIntegrity.note ?? "No temporary paper integrity note is available.")}</div>
                  <div>Missing lane ids: {temporaryPaperMissingLaneIds.length ? temporaryPaperMissingLaneIds.join(", ") : "None"}</div>
                  <div>Required start flags: {temporaryPaperStartFlags.length ? temporaryPaperStartFlags.join(" ") : "None"}</div>
                  {temporaryPaperUnresolvedLaneIds.length ? <div>Unmapped lane ids: {temporaryPaperUnresolvedLaneIds.join(", ")}</div> : null}
                </div>
                <div className="action-row inline">
                  <button
                    className="panel-button"
                    disabled={busyAction !== null || !canRunLiveActions}
                    onClick={() =>
                      void runCommand("restart-paper-with-temp-paper", () => api.runDashboardAction("restart-paper-with-temp-paper"), {
                        confirmMessage: "Restart Runtime + Temp Paper will restart the paper soak with all enabled temporary paper lanes included. Proceed?",
                        requiresLive: true,
                      })
                    }
                  >
                    {busyAction === "restart-paper-with-temp-paper" ? "Restarting..." : "Restart Runtime + Temp Paper"}
                  </button>
                </div>
                <DataTable
                  columns={[
                    { key: "lane_id", label: "Lane ID" },
                    { key: "display_name", label: "Display Name" },
                    { key: "enabled_in_app", label: "Enabled In App", render: (row) => formatValue(row.enabled_in_app ?? false) },
                    { key: "runtime_instance_present", label: "Loaded In Runtime", render: (row) => formatValue(row.runtime_instance_present ?? false) },
                    { key: "runtime_state_loaded", label: "Runtime State Loaded", render: (row) => formatValue(row.runtime_state_loaded ?? false) },
                    { key: "truth_label", label: "Truth" },
                    { key: "start_flag", label: "Start Flag", render: (row) => formatValue(row.start_flag ?? "Unavailable") },
                  ]}
                  rows={temporaryPaperIntegrityRows}
                  emptyLabel="No temporary paper integrity rows are available."
                />
              </Section>

              <Section title="Paper Capture Integrity" subtitle="Are recent strategy rows truly live, stale, or snapshot-only?">
                <div className="metric-grid">
                  {paperCaptureIntegrityMetrics.map((item) => (
                    <MetricCard key={item.label} label={item.label} value={item.value} tone={item.tone} />
                  ))}
                </div>
                <div className="status-line">
                  {paperRuntimeStopped
                    ? "Paper soak is stopped. Frozen strategy timestamps reflect the last persisted runtime update, not ongoing live evaluation."
                    : "Use this integrity summary to distinguish live runtime capture from stale strategy-ledger rows and snapshot-only temporary paper rows."}
                </div>
                <div className="notice-strip">
                  <div>Latest runtime update: {latestRuntimeCaptureTimestamp ? formatTimestamp(latestRuntimeCaptureTimestamp) : "Unavailable"}</div>
                  <div>Latest strategy activity: {latestStrategyActivityTimestamp ? formatTimestamp(latestStrategyActivityTimestamp) : "Unavailable"}</div>
                  <div>Rows older than the latest runtime update: {formatShortNumber(staleStrategyRows.length)}</div>
                </div>
              </Section>

              <Section title="Standalone Strategy Registry" subtitle="One row per standalone strategy identity, regardless of family origin">
                <DataTable
                  columns={[
                    { key: "standalone_strategy_id", label: "Standalone Strategy", render: (row) => standaloneStrategyLabel(row) },
                    { key: "display_name", label: "Operator Label" },
                    { key: "paper_strategy_class", label: "Class", render: (row) => <Badge label={paperStrategyClassLabel(row)} tone={paperStrategyClassTone(row)} /> },
                    { key: "strategy_family", label: "Strategy Family", render: (row) => formatValue(row.strategy_family ?? row.family ?? row.source_family) },
                    { key: "instrument", label: "Instrument", render: (row) => formatValue(row.instrument ?? row.scope_summary) },
                    { key: "enabled", label: "Configured / Enabled", render: (row) => formatValue(row.enabled ?? false) },
                    { key: "runtime_instance_present", label: "Runtime Instance", render: (row) => formatValue(row.runtime_instance_present ?? false) },
                    { key: "runtime_state_loaded", label: "Runtime State Loaded", render: (row) => formatValue(row.runtime_state_loaded ?? false) },
                    { key: "can_process_bars", label: "Can Process Bars", render: (row) => formatValue(row.can_process_bars ?? false) },
                    { key: "config_source", label: "Config Source", render: (row) => formatValue(row.config_source ?? "Unavailable") },
                    { key: "current_session", label: "Current Session", render: () => formatValue(paperReadiness.current_detected_session ?? "Unknown") },
                    { key: "same_underlying_ambiguity", label: "Ambiguity", render: (row) => <Badge label={sameUnderlyingLabel(row)} tone={row.same_underlying_ambiguity ? "warn" : "good"} /> },
                    {
                      key: "same_underlying_conflict",
                      label: "Conflict",
                      render: (row) =>
                        row.same_underlying_conflict_present ? (
                          <Badge label={sameUnderlyingConflictLabel(row)} tone={sameUnderlyingConflictTone(row.same_underlying_conflict_severity)} />
                        ) : (
                          "—"
                        ),
                    },
                    {
                      key: "same_underlying_conflict_review_state",
                      label: "Review",
                      render: (row) =>
                        row.same_underlying_conflict_present ? (
                          <Badge label={formatValue(row.same_underlying_conflict_review_state ?? "OPEN")} tone={sameUnderlyingReviewTone(row.same_underlying_conflict_review_state)} />
                        ) : (
                          "—"
                        ),
                    },
                    {
                      key: "same_underlying_entry_block_effective",
                      label: "Entry Hold",
                      render: (row) =>
                        row.same_underlying_conflict_present ? (
                          <Badge
                            label={row.same_underlying_entry_block_effective ? "HELD" : row.same_underlying_conflict_review_state === "HOLD_EXPIRED" ? "HOLD EXPIRED" : "CLEAR"}
                            tone={row.same_underlying_entry_block_effective ? "danger" : row.same_underlying_conflict_review_state === "HOLD_EXPIRED" ? "warn" : "good"}
                          />
                        ) : (
                          "—"
                        ),
                    },
                    { key: "same_underlying_ambiguity_note", label: "Note", render: (row) => formatValue(row.same_underlying_ambiguity_note) },
                  ]}
                  rows={filteredRuntimeRegistryRows}
                  emptyLabel="No strategy registry rows are available yet."
                />
              </Section>

              <Section title="Same-Underlying Conflict Table" subtitle="One row per instrument conflict group, with explicit severity and operator workflow">
                <div className="notice-strip">
                  <div>Observational rows mean multiple standalone strategies share an instrument but no overlapping exposure or pending-order state is surfaced yet.</div>
                  <div>Blocking rows mean same-underlying exposure, pending orders, or broker/runtime overlap is already present and must be reviewed manually.</div>
                  <div>The app is not automatically arbitrating, netting, flattening, or choosing between these same-underlying strategies in this phase.</div>
                </div>
                <DataTable
                  columns={[
                    { key: "instrument", label: "Instrument" },
                    { key: "severity", label: "Severity", render: (row) => <Badge label={formatValue(row.severity)} tone={sameUnderlyingConflictTone(row.severity)} /> },
                    { key: "conflict_kind", label: "Conflict Kind" },
                    {
                      key: "review_state_status",
                      label: "Review State",
                      render: (row) => <Badge label={formatValue(row.review_state_status ?? "OPEN")} tone={sameUnderlyingReviewTone(row.review_state_status)} />,
                    },
                    {
                      key: "hold_new_entries",
                      label: "Entry Hold",
                      render: (row) => (
                        <Badge
                          label={row.hold_new_entries === true ? "NEW ENTRIES HELD" : row.hold_expired === true ? "HOLD EXPIRED" : "CLEAR"}
                          tone={row.hold_new_entries === true ? "danger" : row.hold_expired === true ? "warn" : "good"}
                        />
                      ),
                    },
                    {
                      key: "mode",
                      label: "Mode",
                      render: (row) => (
                        <Badge
                          label={row.execution_risk === true ? "EXECUTION-RELEVANT" : "OBSERVATIONAL"}
                          tone={row.execution_risk === true ? "danger" : "warn"}
                        />
                      ),
                    },
                    { key: "operator_action_required", label: "Operator Action", render: (row) => formatValue(row.operator_action_required ?? false) },
                    { key: "overlap_scope", label: "Overlap Scope", render: (row) => formatValue(row.overlap_scope) },
                    { key: "standalone_strategy_ids", label: "Standalone Strategies", render: (row) => formatValue(row.standalone_strategy_ids) },
                    { key: "broker_overlap_present", label: "Broker Overlap", render: (row) => formatValue(row.broker_overlap_present ?? false) },
                    { key: "in_position_overlap_present", label: "In Position", render: (row) => formatValue(row.in_position_overlap_present ?? false) },
                    { key: "pending_order_overlap_present", label: "Pending Orders", render: (row) => formatValue(row.pending_order_overlap_present ?? false) },
                    { key: "reconciliation_state", label: "Reconciliation", render: (row) => formatValue(row.reconciliation_state ?? "Unavailable") },
                    {
                      key: "review_state_status",
                      label: "Review State",
                      render: (row) => <Badge label={formatValue(row.review_state_status ?? "OPEN")} tone={sameUnderlyingReviewTone(row.review_state_status)} />,
                    },
                    {
                      key: "hold_new_entries",
                      label: "Entry Hold",
                      render: (row) => (
                        <Badge
                          label={row.hold_new_entries === true ? "HELD" : row.hold_expired === true ? "HOLD EXPIRED" : "CLEAR"}
                          tone={row.hold_new_entries === true ? "danger" : row.hold_expired === true ? "warn" : "good"}
                        />
                      ),
                    },
                    { key: "conflict_reason", label: "Reason" },
                    {
                      key: "select",
                      label: "Detail",
                      render: (row) => (
                        <button
                          className="panel-button"
                          disabled={String(row.instrument ?? "") === String(selectedSameUnderlyingConflict?.instrument ?? "")}
                          onClick={() => setSelectedSameUnderlyingConflictInstrument(String(row.instrument ?? ""))}
                        >
                          {String(row.instrument ?? "") === String(selectedSameUnderlyingConflict?.instrument ?? "") ? "Selected" : "View"}
                        </button>
                      ),
                    },
                  ]}
                  rows={sortedSameUnderlyingConflictRows}
                  emptyLabel="No same-underlying conflicts are surfaced in the current snapshot."
                />
                {selectedSameUnderlyingConflict ? (
                  <div className="startup-panel">
                    <div className="subsection-title">Selected Same-Underlying Conflict</div>
                    <div className="metric-grid">
                      <MetricCard label="Instrument" value={formatValue(selectedSameUnderlyingConflict.instrument)} />
                      <MetricCard label="Severity" value={formatValue(selectedSameUnderlyingConflict.severity)} tone={sameUnderlyingConflictTone(selectedSameUnderlyingConflict.severity)} />
                      <MetricCard label="Conflict Kind" value={formatValue(selectedSameUnderlyingConflict.conflict_kind)} />
                      <MetricCard label="Review State" value={formatValue(selectedSameUnderlyingConflict.review_state_status ?? "OPEN")} tone={sameUnderlyingReviewTone(selectedSameUnderlyingConflict.review_state_status)} />
                      <MetricCard label="Operator Action Required" value={formatValue(selectedSameUnderlyingConflict.operator_action_required ?? false)} tone={selectedSameUnderlyingConflict.operator_action_required ? "danger" : "warn"} />
                      <MetricCard label="Overlap Scope" value={formatValue(selectedSameUnderlyingConflict.overlap_scope)} />
                      <MetricCard label="Entry Hold Effective" value={formatValue(selectedSameUnderlyingConflict.entry_hold_effective ?? false)} tone={selectedSameUnderlyingConflict.entry_hold_effective ? "danger" : "good"} />
                      <MetricCard label="Hold Expired" value={formatValue(selectedSameUnderlyingConflict.hold_expired ?? false)} tone={selectedSameUnderlyingConflict.hold_expired ? "warn" : "good"} />
                      <MetricCard label="Hold Expires At" value={formatTimestamp(selectedSameUnderlyingConflict.hold_expires_at)} />
                      <MetricCard label="Broker Overlap" value={formatValue(selectedSameUnderlyingConflict.broker_overlap_present ?? false)} tone={selectedSameUnderlyingConflict.broker_overlap_present ? "danger" : "good"} />
                      <MetricCard label="In-Position Overlap" value={formatValue(selectedSameUnderlyingConflict.in_position_overlap_present ?? false)} tone={selectedSameUnderlyingConflict.in_position_overlap_present ? "danger" : "good"} />
                      <MetricCard label="Pending-Order Overlap" value={formatValue(selectedSameUnderlyingConflict.pending_order_overlap_present ?? false)} tone={selectedSameUnderlyingConflict.pending_order_overlap_present ? "danger" : "good"} />
                      <MetricCard label="Reconciliation" value={formatValue(selectedSameUnderlyingConflict.reconciliation_state ?? "Unavailable")} tone={selectedSameUnderlyingConflict.reconciliation_clear === false ? "danger" : "good"} />
                      <MetricCard label="Standalone Strategies" value={formatShortNumber(asArray<string>(selectedSameUnderlyingConflict.standalone_strategy_ids).length)} />
                    </div>
                    <div className="notice-strip">
                      <div>{formatValue(selectedSameUnderlyingConflict.conflict_reason)}</div>
                      <div>Review state: {formatValue(selectedSameUnderlyingConflict.review_state_status ?? "OPEN")}.</div>
                      {selectedSameUnderlyingConflict.acknowledged ? (
                        <div>
                          Acknowledged by {formatValue(selectedSameUnderlyingConflict.acknowledged_by)} at {formatTimestamp(selectedSameUnderlyingConflict.acknowledged_at)}.
                        </div>
                      ) : null}
                      {selectedSameUnderlyingConflict.acknowledgement_note ? (
                        <div>Acknowledgement note: {formatValue(selectedSameUnderlyingConflict.acknowledgement_note)}</div>
                      ) : null}
                      {selectedSameUnderlyingConflict.hold_new_entries ? (
                        <div>
                          New entry hold: {formatValue(selectedSameUnderlyingConflict.hold_reason)}. Exits remain allowed and existing exposure is not auto-resolved.
                        </div>
                      ) : null}
                      {selectedSameUnderlyingConflict.hold_expired ? (
                        <div>
                          Hold expired at {formatTimestamp(selectedSameUnderlyingConflict.hold_expired_at)}. {formatValue(selectedSameUnderlyingConflict.hold_state_reason)}
                        </div>
                      ) : null}
                      {selectedSameUnderlyingConflict.override_observational_only ? (
                        <div>Observational-only override: {formatValue(selectedSameUnderlyingConflict.override_reason)}</div>
                      ) : null}
                      {selectedSameUnderlyingConflict.reopened_reason ? (
                        <div>Auto-reopened: {formatValue(selectedSameUnderlyingConflict.reopened_reason)}</div>
                      ) : null}
                      {asArray<string>(selectedSameUnderlyingConflict.operator_workflow).map((item) => (
                        <div key={item}>{item}</div>
                      ))}
                    </div>
                    <div className="startup-panel">
                      <div className="subsection-title">Conflict Review Workflow</div>
                      <div className="status-line">Acknowledge keeps the conflict visible. Observational-only marks it reviewed without implying arbitration. Hold New Entries blocks new entry intents on this instrument only; exits remain allowed. These controls require local operator authentication.</div>
                      <div className="status-line">
                        Local auth status: {formatValue(localOperatorAuth.auth_session_active ? "Authenticated" : localOperatorAuth.auth_available ? "Touch ID required" : "Unavailable")} | Method:{" "}
                        {formatValue(localOperatorAuth.auth_method ?? "NONE")} | Last result: {formatValue(localOperatorAuth.last_auth_result ?? "NONE")}
                      </div>
                      <div className="workflow-form">
                        <label className="workflow-field">
                          <span className="workflow-label">Operator Label (optional note label)</span>
                          <input
                            className="workflow-input"
                            value={sameUnderlyingOperatorLabel}
                            onChange={(event) => setSameUnderlyingOperatorLabel(event.target.value)}
                            placeholder={localOperatorAuth.local_operator_identity ?? "manual operator"}
                          />
                        </label>
                        <label className="workflow-field">
                          <span className="workflow-label">Hold Expires At</span>
                          <input
                            className="workflow-input"
                            type="datetime-local"
                            value={sameUnderlyingHoldExpiresAt}
                            onChange={(event) => setSameUnderlyingHoldExpiresAt(event.target.value)}
                          />
                        </label>
                        <label className="workflow-field workflow-field-wide">
                          <span className="workflow-label">Review Note / Reason</span>
                          <textarea
                            className="workflow-textarea"
                            value={sameUnderlyingReviewNote}
                            onChange={(event) => setSameUnderlyingReviewNote(event.target.value)}
                            placeholder="Why this overlap is understood, why entries are held, or why the review is being reset."
                            rows={3}
                          />
                        </label>
                      </div>
                      <div className="action-row inline">
                        <button
                          className="panel-button"
                          disabled={busyAction !== null}
                          onClick={() =>
                            void runCommand(
                              `same-underlying-acknowledge-${String(selectedSameUnderlyingConflict.instrument ?? "")}`,
                              () => api.runDashboardAction("same-underlying-acknowledge", sameUnderlyingActionPayload()),
                              { requiresLive: true },
                            )
                          }
                        >
                          Acknowledge
                        </button>
                        <button
                          className="panel-button subtle"
                          disabled={busyAction !== null}
                          onClick={() =>
                            void runCommand(
                              `same-underlying-observational-${String(selectedSameUnderlyingConflict.instrument ?? "")}`,
                              () => api.runDashboardAction("same-underlying-mark-observational", sameUnderlyingActionPayload()),
                              { requiresLive: true },
                            )
                          }
                        >
                          Mark Observational-Only
                        </button>
                        <button
                          className="danger-button"
                          disabled={busyAction !== null}
                          onClick={() =>
                            void runCommand(
                              `same-underlying-hold-${String(selectedSameUnderlyingConflict.instrument ?? "")}`,
                              () => api.runDashboardAction("same-underlying-hold-entries", sameUnderlyingActionPayload()),
                              {
                                requiresLive: true,
                                confirmMessage: `Hold new entries on ${formatValue(selectedSameUnderlyingConflict.instrument)}? Exits will remain allowed.`,
                              },
                            )
                          }
                        >
                          Hold New Entries
                        </button>
                        <button
                          className="panel-button subtle"
                          disabled={busyAction !== null || selectedSameUnderlyingConflict.hold_new_entries !== true}
                          onClick={() =>
                            void runCommand(
                              `same-underlying-clear-hold-${String(selectedSameUnderlyingConflict.instrument ?? "")}`,
                              () => api.runDashboardAction("same-underlying-clear-hold", sameUnderlyingActionPayload()),
                              { requiresLive: true },
                            )
                          }
                        >
                          Clear Hold
                        </button>
                        <button
                          className="panel-button subtle"
                          disabled={busyAction !== null}
                          onClick={() =>
                            void runCommand(
                              `same-underlying-reset-${String(selectedSameUnderlyingConflict.instrument ?? "")}`,
                              () => api.runDashboardAction("same-underlying-reset-review", sameUnderlyingActionPayload()),
                              { requiresLive: true },
                            )
                          }
                        >
                          Reset Review
                        </button>
                      </div>
                    </div>
                    <DataTable
                      columns={[
                        { key: "standalone_strategy_id", label: "Standalone Strategy" },
                        { key: "strategy_family", label: "Family" },
                        { key: "current_strategy_status", label: "Status" },
                        { key: "position_side", label: "Position Side" },
                        { key: "eligible_now", label: "Eligible Now", render: (row) => formatValue(row.eligible_now ?? "Unavailable") },
                        { key: "pending_order_present", label: "Pending Order", render: (row) => formatValue(row.pending_order_present ?? false) },
                        { key: "open_broker_order_id", label: "Open Broker Order Id", render: (row) => formatValue(row.open_broker_order_id) },
                        { key: "latest_fault_or_blocker", label: "Fault / Blocker", render: (row) => formatValue(row.latest_fault_or_blocker) },
                      ]}
                      rows={asArray<JsonRecord>(selectedSameUnderlyingConflict.strategies)}
                      emptyLabel="No standalone strategies are attached to this conflict row."
                    />
                    <div className="startup-panel">
                      <div className="subsection-title">Recent Same-Underlying Events</div>
                      <div className="status-line">
                        Review events, auto-reopens, hold expiry, and blocked new-entry events for {formatValue(selectedSameUnderlyingConflict.instrument)}.
                      </div>
                      <DataTable
                        columns={[
                          { key: "occurred_at", label: "Occurred", render: (row) => formatTimestamp(row.occurred_at) },
                          { key: "event_type", label: "Event Type" },
                          { key: "trigger", label: "Trigger", render: (row) => (row.automatic === true ? "Automatic" : "Operator") },
                          { key: "local_operator_identity", label: "Local Operator", render: (row) => formatValue(row.local_operator_identity ?? row.operator_label) },
                          { key: "auth_method", label: "Auth", render: (row) => formatValue(row.auth_method ?? "NONE") },
                          { key: "blocked_standalone_strategy_id", label: "Blocked Strategy", render: (row) => formatValue(row.blocked_standalone_strategy_id) },
                          { key: "review_state_status", label: "Review State", render: (row) => formatValue(row.review_state_status) },
                          { key: "entry_hold_effective", label: "Entry Hold", render: (row) => formatValue(row.entry_hold_effective ?? false) },
                          { key: "note", label: "Reason / Note", render: (row) => formatValue(row.note ?? row.blocked_reason ?? row.hold_state_reason) },
                        ]}
                        rows={selectedSameUnderlyingConflictEvents}
                        emptyLabel="No same-underlying events are recorded yet for this instrument."
                      />
                    </div>
                  </div>
                ) : null}
              </Section>

              <Section title="Same-Underlying Review Events" subtitle="Operator-triggered actions, automatic reopen/expiry, and blocked-entry control events">
                <div className="workflow-form">
                  <label className="workflow-field">
                    <span className="workflow-label">Instrument Filter</span>
                    <select className="workflow-input" value={sameUnderlyingEventInstrumentFilter} onChange={(event) => setSameUnderlyingEventInstrumentFilter(event.target.value)}>
                      <option value="">All instruments</option>
                      {sameUnderlyingEventInstrumentOptions.map((value) => (
                        <option key={value} value={value}>
                          {value}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="workflow-field">
                    <span className="workflow-label">Event Type Filter</span>
                    <select className="workflow-input" value={sameUnderlyingEventTypeFilter} onChange={(event) => setSameUnderlyingEventTypeFilter(event.target.value)}>
                      <option value="">All event types</option>
                      {sameUnderlyingEventTypeOptions.map((value) => (
                        <option key={value} value={value}>
                          {value}
                        </option>
                      ))}
                    </select>
                  </label>
                </div>
                <DataTable
                  columns={[
                    { key: "occurred_at", label: "Occurred", render: (row) => formatTimestamp(row.occurred_at) },
                    { key: "instrument", label: "Instrument" },
                    { key: "event_type", label: "Event Type" },
                    { key: "trigger", label: "Trigger", render: (row) => (row.automatic === true ? "Automatic" : "Operator") },
                    { key: "local_operator_identity", label: "Local Operator", render: (row) => formatValue(row.local_operator_identity ?? row.operator_label) },
                    { key: "auth_method", label: "Auth", render: (row) => formatValue(row.auth_method ?? "NONE") },
                    { key: "blocked_standalone_strategy_id", label: "Blocked Strategy", render: (row) => formatValue(row.blocked_standalone_strategy_id) },
                    { key: "review_state_status", label: "Review State", render: (row) => formatValue(row.review_state_status) },
                    { key: "entry_hold_effective", label: "Entry Hold", render: (row) => formatValue(row.entry_hold_effective ?? false) },
                    { key: "note", label: "Reason / Note", render: (row) => formatValue(row.note ?? row.blocked_reason ?? row.hold_state_reason) },
                  ]}
                  rows={filteredSameUnderlyingEventRows}
                  emptyLabel="No same-underlying workflow events are available for the current filters."
                />
              </Section>

              <Section title="Live Eligibility" subtitle="Separate correct gating from runtime failure at the standalone strategy identity level">
                <DataTable
                  columns={[
                    { key: "standalone_strategy_id", label: "Standalone Strategy", render: (row) => standaloneStrategyLabel(row) },
                    { key: "strategy_name", label: "Lane / Strategy" },
                    { key: "paper_strategy_class", label: "Class", render: (row) => <Badge label={paperStrategyClassLabel(row)} tone={paperStrategyClassTone(row)} /> },
                    { key: "family", label: "Family", render: (row) => formatValue(row.family ?? row.source_family) },
                    { key: "instrument", label: "Symbol" },
                    { key: "runtime_instance_present", label: "Runtime Instance", render: (row) => formatValue(row.runtime_instance_present ?? false) },
                    { key: "runtime_state_loaded", label: "Runtime State Loaded", render: (row) => formatValue(row.runtime_state_loaded ?? false) },
                    { key: "current_session", label: "Current Session", render: (row) => formatValue(row.current_session ?? row.session ?? row.current_detected_session) },
                    { key: "eligible_now", label: "Eligible Now", render: (row) => formatValue(row.eligible_now ?? false) },
                    { key: "auditable_now", label: "Auditable Now", render: (row) => formatValue(row.auditable_now ?? false) },
                    { key: "performance_row_present", label: "Performance Row", render: (row) => formatValue(row.performance_row_present ?? false) },
                    { key: "current_strategy_status", label: "Current Status" },
                    { key: "latest_fault_or_blocker", label: "Blocker Reason", render: (row) => formatValue(row.latest_fault_or_blocker ?? row.audit_reason) },
                    { key: "same_underlying_ambiguity", label: "Ambiguity", render: (row) => <Badge label={sameUnderlyingLabel(row)} tone={row.same_underlying_ambiguity ? "warn" : "good"} /> },
                    {
                      key: "same_underlying_conflict",
                      label: "Conflict",
                      render: (row) =>
                        row.same_underlying_conflict_present ? (
                          <Badge label={sameUnderlyingConflictLabel(row)} tone={sameUnderlyingConflictTone(row.same_underlying_conflict_severity)} />
                        ) : (
                          "—"
                        ),
                    },
                    {
                      key: "same_underlying_conflict_review_state",
                      label: "Review",
                      render: (row) =>
                        row.same_underlying_conflict_present ? (
                          <Badge label={formatValue(row.same_underlying_conflict_review_state ?? "OPEN")} tone={sameUnderlyingReviewTone(row.same_underlying_conflict_review_state)} />
                        ) : (
                          "—"
                        ),
                    },
                    {
                      key: "same_underlying_entry_block_effective",
                      label: "Entry Hold",
                      render: (row) =>
                        row.same_underlying_conflict_present ? (
                          <Badge
                            label={row.same_underlying_entry_block_effective ? "HELD" : row.same_underlying_conflict_review_state === "HOLD_EXPIRED" ? "HOLD EXPIRED" : "CLEAR"}
                            tone={row.same_underlying_entry_block_effective ? "danger" : row.same_underlying_conflict_review_state === "HOLD_EXPIRED" ? "warn" : "good"}
                          />
                        ) : (
                          "—"
                        ),
                    },
                  ]}
                  rows={filteredSignalIntentFillAuditRows}
                  emptyLabel="No live eligibility rows are available."
                />
              </Section>

              <Section title="Strategy Performance" subtitle="Per-standalone-strategy ledger truth with explicit pricing availability notes">
                <div className="metric-grid">
                  <MetricCard label="Strategies Surfaced" value={formatShortNumber(filteredStrategyPerformanceRows.length)} />
                  <MetricCard label="Trade Log Rows" value={formatShortNumber(filteredStrategyTradeLogRows.length)} />
                  <MetricCard label="Attribution Families" value={formatShortNumber(strategyAttributionRows.length)} />
                </div>
                <DataTable
                  columns={[
                    { key: "standalone_strategy_id", label: "Standalone Strategy", render: (row) => standaloneStrategyLabel(row) },
                    { key: "strategy_name", label: "Strategy" },
                    { key: "paper_strategy_class", label: "Class", render: (row) => <Badge label={paperStrategyClassLabel(row)} tone={paperStrategyClassTone(row)} /> },
                    { key: "instrument", label: "Instrument" },
                    { key: "runtime_instance_present", label: "Runtime Instance", render: (row) => formatValue(row.runtime_instance_present ?? false) },
                    { key: "runtime_state_loaded", label: "Runtime State Loaded", render: (row) => formatValue(row.runtime_state_loaded ?? false) },
                    { key: "status", label: "Status" },
                    { key: "realized_pnl", label: "Realized P&L", render: (row) => formatMaybePnL(row.realized_pnl) },
                    { key: "unrealized_pnl", label: "Unrealized P&L", render: (row) => formatMaybePnL(row.unrealized_pnl) },
                    { key: "day_pnl", label: "Day P&L", render: (row) => formatMaybePnL(row.day_pnl) },
                    { key: "cumulative_pnl", label: "Cumulative P&L", render: (row) => formatMaybePnL(row.cumulative_pnl) },
                    { key: "max_drawdown", label: "Max Drawdown", render: (row) => formatMaybePnL(row.max_drawdown) },
                    { key: "trade_count", label: "Trade Count" },
                    { key: "pnl_unavailable_reason", label: "P&L Availability", render: (row) => formatValue(row.pnl_unavailable_reason ?? "Exact / fully priced") },
                    {
                      key: "same_underlying_conflict",
                      label: "Conflict",
                      render: (row) =>
                        row.same_underlying_conflict_present ? (
                          <Badge label={sameUnderlyingConflictLabel(row)} tone={sameUnderlyingConflictTone(row.same_underlying_conflict_severity)} />
                        ) : (
                          "—"
                        ),
                    },
                    {
                      key: "same_underlying_conflict_review_state",
                      label: "Review",
                      render: (row) =>
                        row.same_underlying_conflict_present ? (
                          <Badge label={formatValue(row.same_underlying_conflict_review_state ?? "OPEN")} tone={sameUnderlyingReviewTone(row.same_underlying_conflict_review_state)} />
                        ) : (
                          "—"
                        ),
                    },
                    {
                      key: "same_underlying_entry_block_effective",
                      label: "Entry Hold",
                      render: (row) =>
                        row.same_underlying_conflict_present ? (
                          <Badge
                            label={row.same_underlying_entry_block_effective ? "HELD" : row.same_underlying_conflict_review_state === "HOLD_EXPIRED" ? "HOLD EXPIRED" : "CLEAR"}
                            tone={row.same_underlying_entry_block_effective ? "danger" : row.same_underlying_conflict_review_state === "HOLD_EXPIRED" ? "warn" : "good"}
                          />
                        ) : (
                          "—"
                        ),
                    },
                    { key: "same_underlying_ambiguity_note", label: "Same-Underlying Note", render: (row) => formatValue(row.same_underlying_ambiguity_note) },
                    { key: "latest_activity_timestamp", label: "Latest Activity", render: (row) => formatTimestamp(row.latest_activity_timestamp) },
                  ]}
                  rows={filteredStrategyPerformanceRows}
                  emptyLabel="No strategy-performance rows are available yet."
                />
                {strategyPerformanceLimitations.length ? (
                  <div className="notice-strip">
                    {strategyPerformanceLimitations.map((item) => (
                      <div key={item}>{item}</div>
                    ))}
                  </div>
                ) : null}
              </Section>

              <Section title="Expected Fire / Historical Cadence" subtitle="Descriptive completed-bar history only; not a prediction">
                <div className="notice-strip">
                  <div>The current engine evaluates completed bars only. No decisions are made on partial bars.</div>
                  <div>Expected fire cadence and likely windows are historical/operator-facing statistics from available persisted lane history.</div>
                </div>
                <DataTable
                  columns={[
                    { key: "standalone_strategy_id", label: "Standalone Strategy", render: (row) => standaloneStrategyLabel(row) },
                    { key: "strategy_name", label: "Strategy" },
                    { key: "instrument", label: "Instrument" },
                    { key: "trade_count", label: "Trades" },
                    { key: "entry_count", label: "Entries" },
                    { key: "total_signal_count", label: "Signals", render: (row) => formatValue(row.total_signal_count ?? "Unavailable") },
                    { key: "session_bucket_summary", label: "Session Mix" },
                    { key: "day_of_week_summary", label: "Day Mix" },
                    { key: "most_common_session_bucket", label: "Common Bucket" },
                    { key: "most_likely_next_window", label: "Most Likely Next Window" },
                    { key: "expected_fire_cadence", label: "Cadence" },
                    { key: "median_bars_between_entries_label", label: "Median Bars Between Entries" },
                    { key: "median_elapsed_between_entries_label", label: "Median Time Between Entries" },
                    { key: "last_fire_timestamp", label: "Last Fire", render: (row) => formatTimestamp(row.last_fire_timestamp) },
                    { key: "days_since_last_fire", label: "Days Since Last Fire", render: (row) => formatValue(row.days_since_last_fire ?? "Unavailable") },
                    { key: "operator_interpretation", label: "Why No Trades Yet?" },
                  ]}
                  rows={filteredStrategyExecutionLikelihoodRows}
                  emptyLabel="No historical cadence rows are available yet."
                />
                {strategyExecutionLikelihoodNotes.length ? (
                  <div className="notice-strip">
                    {strategyExecutionLikelihoodNotes.map((item) => (
                      <div key={item}>{item}</div>
                    ))}
                  </div>
                ) : null}
              </Section>

              <Section title="Signal / Intent / Fill Audit" subtitle="Explain whether nothing happened, a setup was gated, an intent is still waiting on fill persistence, or UI surfacing may be lagging">
                <div className="notice-strip">
                  <div>Completed-bar only: this audit reads persisted finalized-bar artifacts only. No partial-bar or current-bar logic is used.</div>
                  <div>Use this when asking “Did we miss a setup overnight?” or “Was the lane simply quiet?”</div>
                  <div>Eligible row means the lane is currently surfaced in the operator universe. Auditable row means persisted audit artifacts exist. Performance row present means the strategy P&amp;L ledger is surfacing that lane separately.</div>
                </div>
                <div className="metric-grid">
                  <MetricCard label="Lanes Audited" value={formatShortNumber(signalIntentFillAuditSummary.lane_count ?? signalIntentFillAuditRows.length)} />
                  <MetricCard label="No Setup Observed" value={formatShortNumber(asRecord(signalIntentFillAuditSummary.verdict_counts).NO_SETUP_OBSERVED)} />
                  <MetricCard label="Setup Gated" value={formatShortNumber(asRecord(signalIntentFillAuditSummary.verdict_counts).SETUP_GATED)} tone="warn" />
                  <MetricCard label="Intent No Fill Yet" value={formatShortNumber(asRecord(signalIntentFillAuditSummary.verdict_counts).INTENT_NO_FILL_YET)} tone="warn" />
                  <MetricCard label="Filled" value={formatShortNumber(asRecord(signalIntentFillAuditSummary.verdict_counts).FILLED)} tone="good" />
                  <MetricCard label="Surfacing Mismatch Suspected" value={formatShortNumber(asRecord(signalIntentFillAuditSummary.verdict_counts).SURFACING_MISMATCH_SUSPECTED)} tone="danger" />
                </div>
                <div className="ticket-grid">
                  <label className="settings-field">
                    <span>Standalone Strategy</span>
                    <select value={auditStrategyIdentityFilter} onChange={(event) => setAuditStrategyIdentityFilter(event.target.value)}>
                      <option value="">All standalone strategies</option>
                      {signalIntentFillAuditStrategyOptions.map((item) => (
                        <option key={item} value={item}>
                          {item}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="settings-field">
                    <span>Family</span>
                    <select value={auditFamilyFilter} onChange={(event) => setAuditFamilyFilter(event.target.value)}>
                      <option value="">All families</option>
                      {signalIntentFillAuditFamilyOptions.map((item) => (
                        <option key={item} value={item}>
                          {item}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="settings-field">
                    <span>Instrument</span>
                    <select value={auditInstrumentFilter} onChange={(event) => setAuditInstrumentFilter(event.target.value)}>
                      <option value="">All instruments</option>
                      {signalIntentFillAuditInstrumentOptions.map((item) => (
                        <option key={item} value={item}>
                          {item}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="settings-field">
                    <span>Session</span>
                    <select value={auditSessionFilter} onChange={(event) => setAuditSessionFilter(event.target.value)}>
                      <option value="">All sessions</option>
                      {signalIntentFillAuditSessionOptions.map((item) => (
                        <option key={item} value={item}>
                          {item}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="settings-field">
                    <span>Verdict</span>
                    <select value={auditVerdictFilter} onChange={(event) => setAuditVerdictFilter(event.target.value)}>
                      <option value="">All verdicts</option>
                      {signalIntentFillAuditVerdictOptions.map((item) => (
                        <option key={item} value={item}>
                          {item}
                        </option>
                      ))}
                    </select>
                  </label>
                </div>
                <DataTable
                  columns={[
                    { key: "standalone_strategy_id", label: "Standalone Strategy", render: (row) => standaloneStrategyLabel(row) },
                    { key: "strategy_name", label: "Lane / Strategy" },
                    { key: "family", label: "Family", render: (row) => formatValue(row.family ?? row.source_family) },
                    { key: "instrument", label: "Instrument" },
                    { key: "runtime_instance_present", label: "Runtime Instance", render: (row) => formatValue(row.runtime_instance_present ?? false) },
                    { key: "runtime_state_loaded", label: "Runtime State Loaded", render: (row) => formatValue(row.runtime_state_loaded ?? false) },
                    { key: "current_session", label: "Current Session" },
                    { key: "eligible_now", label: "Eligible Now", render: (row) => formatValue(row.eligible_now ?? "Unavailable") },
                    { key: "auditable_now", label: "Auditable Now", render: (row) => formatValue(row.auditable_now ?? false) },
                    { key: "performance_row_present", label: "Performance Row Present", render: (row) => formatValue(row.performance_row_present ?? false) },
                    { key: "trade_log_present", label: "Trade Log Present", render: (row) => formatValue(row.trade_log_present ?? false) },
                    { key: "last_processed_bar_end_ts", label: "Last Processed Bar", render: (row) => formatTimestamp(row.last_processed_bar_end_ts) },
                    { key: "last_actionable_signal_timestamp", label: "Last Actionable Signal", render: (row) => row.last_actionable_signal_timestamp ? `${formatTimestamp(row.last_actionable_signal_timestamp)} • ${formatValue(row.last_actionable_signal_family)}` : "None in window" },
                    { key: "last_intent_timestamp", label: "Last Intent", render: (row) => row.last_intent_timestamp ? `${formatTimestamp(row.last_intent_timestamp)} • ${formatValue(row.last_intent_type)}` : "None in window" },
                    { key: "last_fill_timestamp", label: "Last Fill", render: (row) => row.last_fill_timestamp ? `${formatTimestamp(row.last_fill_timestamp)} • ${formatValue(row.last_fill_price)}` : "None in window" },
                    { key: "current_strategy_status", label: "Current Status" },
                    {
                      key: "same_underlying_conflict",
                      label: "Conflict",
                      render: (row) =>
                        row.same_underlying_conflict_present ? (
                          <Badge label={sameUnderlyingConflictLabel(row)} tone={sameUnderlyingConflictTone(row.same_underlying_conflict_severity)} />
                        ) : (
                          "—"
                        ),
                    },
                    { key: "same_underlying_ambiguity_note", label: "Same-Underlying Note", render: (row) => formatValue(row.same_underlying_ambiguity_note) },
                    { key: "audit_verdict", label: "Audit Verdict", render: (row) => <Badge label={formatValue(row.audit_verdict)} tone={auditVerdictTone(row.audit_verdict)} /> },
                    { key: "audit_reason", label: "Reason" },
                    {
                      key: "select",
                      label: "Detail",
                      render: (row) => (
                        <button
                          className="panel-button"
                          disabled={standaloneStrategyId(row) === standaloneStrategyId(selectedSignalIntentFillAuditRow)}
                          onClick={() => setSelectedAuditStrategyKey(standaloneStrategyId(row))}
                        >
                          {standaloneStrategyId(row) === standaloneStrategyId(selectedSignalIntentFillAuditRow) ? "Selected" : "View"}
                        </button>
                      ),
                    },
                  ]}
                  rows={filteredSignalIntentFillAuditRows}
                  emptyLabel="No signal / intent / fill audit rows are available yet."
                />
                {selectedSignalIntentFillAuditRow ? (
                  <div className="startup-panel">
                    <div className="subsection-title">Selected Audit Detail</div>
                    <div className="metric-grid">
                      <MetricCard label="Standalone Strategy" value={standaloneStrategyLabel(selectedSignalIntentFillAuditRow)} />
                      <MetricCard label="Lane" value={formatValue(selectedSignalIntentFillAuditRow.strategy_name)} />
                      <MetricCard label="Instrument" value={formatValue(selectedSignalIntentFillAuditRow.instrument)} />
                      <MetricCard label="Verdict" value={formatValue(selectedSignalIntentFillAuditRow.audit_verdict)} tone={auditVerdictTone(selectedSignalIntentFillAuditRow.audit_verdict)} />
                      <MetricCard label="Current Status" value={formatValue(selectedSignalIntentFillAuditRow.current_strategy_status)} />
                      <MetricCard label="Runtime Instance" value={formatValue(selectedSignalIntentFillAuditRow.runtime_instance_present ?? false)} tone={selectedSignalIntentFillAuditRow.runtime_instance_present ? "good" : "warn"} />
                      <MetricCard label="Runtime State Loaded" value={formatValue(selectedSignalIntentFillAuditRow.runtime_state_loaded ?? false)} tone={selectedSignalIntentFillAuditRow.runtime_state_loaded ? "good" : "warn"} />
                      <MetricCard label="Config Source" value={formatValue(selectedSignalIntentFillAuditRow.config_source ?? "Unavailable")} />
                      <MetricCard label="Eligible Now" value={formatValue(selectedSignalIntentFillAuditRow.eligible_now ?? "Unavailable")} tone={selectedSignalIntentFillAuditRow.eligible_now === true ? "good" : selectedSignalIntentFillAuditRow.eligible_now === false ? "warn" : "muted"} />
                      <MetricCard label="Auditable Now" value={formatValue(selectedSignalIntentFillAuditRow.auditable_now ?? false)} tone={selectedSignalIntentFillAuditRow.auditable_now === true ? "good" : "warn"} />
                      <MetricCard label="Performance Row Present" value={formatValue(selectedSignalIntentFillAuditRow.performance_row_present ?? false)} tone={selectedSignalIntentFillAuditRow.performance_row_present ? "good" : "warn"} />
                      <MetricCard label="Trade Log Present" value={formatValue(selectedSignalIntentFillAuditRow.trade_log_present ?? false)} tone={selectedSignalIntentFillAuditRow.trade_log_present ? "good" : "warn"} />
                      <MetricCard label="Processed Bars In Window" value={formatShortNumber(selectedSignalIntentFillAuditRow.bar_count_in_window)} />
                      <MetricCard label="Actionable Signals In Window" value={formatShortNumber(selectedSignalIntentFillAuditRow.actionable_entry_signal_count)} />
                      <MetricCard label="Intents In Window" value={formatShortNumber(selectedSignalIntentFillAuditRow.total_intent_count)} />
                      <MetricCard label="Fills In Window" value={formatShortNumber(selectedSignalIntentFillAuditRow.total_fill_count)} />
                      <MetricCard
                        label="Same-Underlying Conflict"
                        value={formatValue(selectedSignalIntentFillAuditRow.same_underlying_conflict_severity ?? "CLEAR")}
                        tone={selectedSignalIntentFillAuditRow.same_underlying_conflict_present ? sameUnderlyingConflictTone(selectedSignalIntentFillAuditRow.same_underlying_conflict_severity) : "good"}
                      />
                    </div>
                    <div className="notice-strip">
                      <div>{formatValue(selectedSignalIntentFillAuditRow.audit_reason)}</div>
                      <div>{formatValue(selectedSignalIntentFillAuditRow.operator_explanation)}</div>
                      {selectedSignalIntentFillAuditRow.same_underlying_conflict_present ? (
                        <div>
                          Same-underlying conflict: {formatValue(selectedSignalIntentFillAuditRow.same_underlying_conflict_reason)} ({formatValue(selectedSignalIntentFillAuditRow.same_underlying_conflict_overlap_scope)}).
                        </div>
                      ) : null}
                      {selectedSignalIntentFillAuditRow.same_underlying_latest_entry_blocked_at ? (
                        <div>
                          Latest blocked entry event: {formatTimestamp(selectedSignalIntentFillAuditRow.same_underlying_latest_entry_blocked_at)}.{" "}
                          {formatValue(selectedSignalIntentFillAuditRow.same_underlying_latest_entry_blocked_reason)}
                        </div>
                      ) : null}
                    </div>
                    <div className="metric-grid">
                      <MetricCard label="Entries Enabled" value={formatValue(selectedSignalIntentFillAuditRow.entries_enabled)} tone={selectedSignalIntentFillAuditRow.entries_enabled === true ? "good" : "warn"} />
                      <MetricCard label="Operator Halt" value={formatValue(selectedSignalIntentFillAuditRow.operator_halt)} tone={selectedSignalIntentFillAuditRow.operator_halt === true ? "danger" : "good"} />
                      <MetricCard label="Same-Underlying Entry Hold" value={formatValue(selectedSignalIntentFillAuditRow.same_underlying_entry_hold ?? selectedSignalIntentFillAuditRow.same_underlying_entry_block_effective ?? false)} tone={(selectedSignalIntentFillAuditRow.same_underlying_entry_hold ?? selectedSignalIntentFillAuditRow.same_underlying_entry_block_effective) ? "danger" : "good"} />
                      <MetricCard label="Conflict Review State" value={formatValue(selectedSignalIntentFillAuditRow.same_underlying_conflict_review_state ?? "OPEN")} tone={sameUnderlyingReviewTone(selectedSignalIntentFillAuditRow.same_underlying_conflict_review_state)} />
                      <MetricCard label="Warmup Complete" value={formatValue(selectedSignalIntentFillAuditRow.warmup_complete)} tone={selectedSignalIntentFillAuditRow.warmup_complete === true ? "good" : "warn"} />
                      <MetricCard label="Position Side" value={formatValue(selectedSignalIntentFillAuditRow.position_side)} />
                      <MetricCard label="Open Broker Order Id" value={formatValue(selectedSignalIntentFillAuditRow.open_broker_order_id)} />
                      <MetricCard label="Latest Fault / Blocker" value={formatValue(selectedSignalIntentFillAuditRow.latest_fault_or_blocker)} tone={selectedSignalIntentFillAuditRow.latest_fault_or_blocker ? "warn" : "good"} />
                    </div>
                    <div className="ticket-grid">
                      <div>
                        <div className="subsection-title">Latest Signal Packet Summary</div>
                        <JsonBlock value={asRecord(selectedSignalIntentFillAuditRow.latest_signal_packet_summary)} />
                      </div>
                      <div>
                        <div className="subsection-title">Latest Gating State</div>
                        <JsonBlock value={asRecord(selectedSignalIntentFillAuditRow.latest_gating_state)} />
                      </div>
                      <div>
                        <div className="subsection-title">Latest Intent Summary</div>
                        <JsonBlock value={asRecord(selectedSignalIntentFillAuditRow.latest_intent_summary)} />
                      </div>
                      <div>
                        <div className="subsection-title">Latest Fill Summary</div>
                        <JsonBlock value={asRecord(selectedSignalIntentFillAuditRow.latest_fill_summary)} />
                      </div>
                    </div>
                    <div className="notice-strip">
                      <div>Strategy performance row surfaced: {selectedSignalIntentFillAuditRow.strategy_performance_row_exists ? "Yes" : "No"}.</div>
                      <div>Trade-log rows surfaced: {selectedSignalIntentFillAuditRow.trade_log_rows_exist ? `${formatShortNumber(selectedSignalIntentFillAuditRow.trade_log_row_count)} row(s)` : "No"}.</div>
                      <div>Eligible now: {formatValue(selectedSignalIntentFillAuditRow.eligible_now ?? "Unavailable")} | Auditable now: {formatValue(selectedSignalIntentFillAuditRow.auditable_now ?? false)}.</div>
                      <div>Inspection window: {formatTimestamp(selectedSignalIntentFillAuditRow.inspection_start_ts)} to {formatTimestamp(selectedSignalIntentFillAuditRow.inspection_end_ts)}.</div>
                    </div>
                  </div>
                ) : null}
                {signalIntentFillAuditNotes.length ? (
                  <div className="notice-strip">
                    {signalIntentFillAuditNotes.map((item) => (
                      <div key={item}>{item}</div>
                    ))}
                  </div>
                ) : null}
              </Section>

              <Section title="Strategy Trade Log" subtitle="Closed trades with strategy, instrument, and signal-family attribution">
                <div className="ticket-grid">
                  <label className="settings-field">
                    <span>Standalone Strategy</span>
                    <select value={strategyFilterKey} onChange={(event) => setStrategyFilterKey(event.target.value)}>
                      <option value="">All standalone strategies</option>
                      {strategyFilterOptions.map((item) => (
                        <option key={item.key} value={item.key}>
                          {item.label}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="settings-field">
                    <span>Family</span>
                    <select value={strategyFamilyFilter} onChange={(event) => setStrategyFamilyFilter(event.target.value)}>
                      <option value="">All families</option>
                      {strategyFamilyOptions.map((item) => (
                        <option key={item} value={item}>
                          {item}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="settings-field">
                    <span>Instrument</span>
                    <select value={strategyInstrumentFilter} onChange={(event) => setStrategyInstrumentFilter(event.target.value)}>
                      <option value="">All instruments</option>
                      {strategyInstrumentOptions.map((item) => (
                        <option key={item} value={item}>
                          {item}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="settings-field">
                    <span>Signal family</span>
                    <select value={strategySignalFamilyFilter} onChange={(event) => setStrategySignalFamilyFilter(event.target.value)}>
                      <option value="">All families</option>
                      {strategySignalFamilyOptions.map((item) => (
                        <option key={item} value={item}>
                          {item}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="settings-field">
                    <span>Date</span>
                    <select value={strategyTradeDateFilter} onChange={(event) => setStrategyTradeDateFilter(event.target.value)}>
                      <option value="">All dates</option>
                      {strategyTradeDateOptions.map((item) => (
                        <option key={item} value={item}>
                          {item}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="settings-field">
                    <span>Session</span>
                    <select value={strategyTradeSessionFilter} onChange={(event) => setStrategyTradeSessionFilter(event.target.value)}>
                      <option value="">All sessions</option>
                      {strategyTradeSessionOptions.map((item) => (
                        <option key={item} value={item}>
                          {item}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="settings-field">
                    <span>Status</span>
                    <select value={strategyTradeStatusFilter} onChange={(event) => setStrategyTradeStatusFilter(event.target.value)}>
                      <option value="">All statuses</option>
                      {strategyTradeStatusOptions.map((item) => (
                        <option key={item} value={item}>
                          {item}
                        </option>
                      ))}
                    </select>
                  </label>
                </div>
                <DataTable
                  columns={[
                    { key: "standalone_strategy_id", label: "Standalone Strategy", render: (row) => standaloneStrategyLabel(row) },
                    { key: "strategy_name", label: "Strategy" },
                    { key: "family", label: "Family", render: (row) => formatValue(row.family ?? row.source_family) },
                    { key: "instrument", label: "Instrument" },
                    { key: "signal_family_label", label: "Signal Family" },
                    { key: "side", label: "Side" },
                    { key: "entry_session_phase", label: "Session", render: (row) => formatValue(row.entry_session_phase ?? row.exit_session_phase) },
                    { key: "status", label: "Status" },
                    { key: "entry_timestamp", label: "Entry", render: (row) => formatTimestamp(row.entry_timestamp) },
                    { key: "exit_timestamp", label: "Exit", render: (row) => formatTimestamp(row.exit_timestamp) },
                    { key: "entry_price", label: "Entry Px" },
                    { key: "exit_price", label: "Exit Px" },
                    { key: "realized_pnl", label: "Realized P&L", render: (row) => formatMaybePnL(row.realized_pnl) },
                    { key: "exit_reason", label: "Exit Reason" },
                  ]}
                  rows={filteredStrategyTradeLogRows}
                  emptyLabel="No strategy trade-log rows match the current filters."
                />
              </Section>

              <Section title="Attribution Summary" subtitle="Roll up realized P&L by operator-facing attribution family with standalone strategy visibility preserved">
                <DataTable
                  columns={[
                    { key: "standalone_strategy_ids", label: "Standalone Strategies", render: (row) => formatValue(row.standalone_strategy_ids) },
                    { key: "family_label", label: "Signal Family" },
                    { key: "trade_count", label: "Trades" },
                    { key: "wins", label: "Wins" },
                    { key: "losses", label: "Losses" },
                    { key: "realized_pnl", label: "Realized P&L", render: (row) => formatMaybePnL(row.realized_pnl) },
                    { key: "latest_trade_timestamp", label: "Latest Trade", render: (row) => formatTimestamp(row.latest_trade_timestamp) },
                    { key: "source_families", label: "Exact Source Labels", render: (row) => formatValue(row.source_families) },
                  ]}
                  rows={strategyAttributionRows}
                  emptyLabel="No attribution summary is available yet."
                />
              </Section>
            </>
          ) : null}

          {!loading && page === "positions" ? (
            <>
              <Section
                title="Live P&L Workspace"
                subtitle="Live and paper account monitoring with current exposure, curves, and order activity in one operator surface"
                className="pnl-workspace-section"
                headerClassName="section-header-tight"
              >
                <div className="split-panel pnl-account-panels">
                  <div className="account-panel-shell">
                    <div className="subsection-title">Live Account</div>
                    <div className="metric-grid account-metric-grid">
                      <MetricCard label="Net Liq" value={formatCompactMetric(productionBalances.liquidation_value)} />
                      <MetricCard label="Buying Power" value={formatCompactMetric(productionBalances.buying_power)} />
                      <MetricCard label="Day P&L" value={renderPnlValue(productionTotals.total_current_day_pnl)} tone={pnlTone(productionTotals.total_current_day_pnl)} />
                      <MetricCard label="Open P&L" value={renderPnlValue(productionTotals.total_open_pnl)} tone={pnlTone(productionTotals.total_open_pnl)} />
                      <MetricCard label="Open Positions" value={formatShortNumber(productionPositions.length)} />
                      <MetricCard label="Working Orders" value={formatShortNumber(productionOpenOrders.length)} tone={productionOpenOrders.length ? "warn" : "muted"} />
                    </div>
                    <DataTable
                      columns={[
                        { key: "symbol", label: "Symbol", render: (row) => formatValue(row.symbol) },
                        { key: "quantity", label: "Qty", render: (row) => formatValue(row.quantity) },
                        { key: "average_price", label: "Avg", render: (row) => formatValue(row.average_price ?? row.averagePrice) },
                        { key: "market_value", label: "Mkt Value", render: (row) => formatMaybePnL(row.market_value) },
                        { key: "current_day_profit_loss", label: "Day P&L", render: (row) => formatMaybePnL(row.current_day_profit_loss ?? row.currentDayProfitLoss) },
                        { key: "current_day_profit_loss_percentage", label: "Day %", render: (row) => formatValue(row.current_day_profit_loss_percentage ?? row.currentDayProfitLossPercentage) },
                      ]}
                      rows={productionPositions.slice(0, 5)}
                      emptyLabel="No live-account positions are open."
                    />
                  </div>
                  <div className="account-panel-shell">
                    <div className="subsection-title">Paper / Simulated</div>
                    <div className="metric-grid account-metric-grid">
                      <MetricCard label="Paper Realized" value={renderPnlValue(combinedStrategyPortfolioSnapshot.total_realized_pnl)} tone={pnlTone(combinedStrategyPortfolioSnapshot.total_realized_pnl)} />
                      <MetricCard label="Paper Open P&L" value={renderPnlValue(combinedStrategyPortfolioSnapshot.total_unrealized_pnl)} tone={pnlTone(combinedStrategyPortfolioSnapshot.total_unrealized_pnl)} />
                      <MetricCard label="Paper Day P&L" value={renderPnlValue(combinedStrategyPortfolioSnapshot.total_day_pnl)} tone={pnlTone(combinedStrategyPortfolioSnapshot.total_day_pnl)} />
                      <MetricCard label="Open Positions" value={formatShortNumber(sortedPositionsRows.filter((row) => row.paperRows.some((paperRow) => String(paperRow.position_side ?? "").toUpperCase() !== "FLAT")).length)} />
                      <MetricCard label="Tracked Strategies" value={formatShortNumber(combinedStrategyPortfolioSnapshot.active_strategy_count)} />
                      <MetricCard label="Recent Fills" value={formatShortNumber(closedStrategyTradeRows.length)} />
                    </div>
                    <DataTable
                      columns={[
                        { key: "branch", label: "Strategy", render: (row) => formatValue(row.branch ?? row.strategy_name ?? row.lane_id) },
                        { key: "instrument", label: "Symbol", render: (row) => formatValue(row.instrument) },
                        { key: "position_side", label: "Side", render: (row) => formatValue(row.position_side ?? row.net_side ?? "FLAT") },
                        { key: "realized_pnl", label: "Realized", render: (row) => formatMaybePnL(row.realized_pnl) },
                        { key: "unrealized_pnl", label: "Open P&L", render: (row) => formatMaybePnL(row.unrealized_pnl) },
                        { key: "latest_activity_timestamp", label: "Latest", render: (row) => formatTimestamp(row.latest_activity_timestamp) },
                      ]}
                      rows={strategyPerformanceRows.slice(0, 5)}
                      emptyLabel="No paper strategy positions are currently surfaced."
                    />
                  </div>
                </div>
                <div className="split-panel pnl-chart-grid">
                  <TrendPanel
                    title="Live Account Intraday Curve"
                    subtitle={liveIntradayCurveRows.length > 1 ? "Derived from live session activity with current broker day-P&L anchor." : "Current broker payload is snapshot-heavy; line is anchored from the latest available live session truth."}
                    points={liveIntradayCurveRows}
                    tone={pnlTone(productionTotals.total_current_day_pnl)}
                    footer={`Orders ${formatTimestamp(brokerOrdersTimestamp)} | Fills ${formatTimestamp(brokerFillsTimestamp)} | Quotes ${formatTimestamp(brokerQuotesTimestamp)}`}
                    className="pnl-trend-panel"
                  />
                  <TrendPanel
                    title="Paper / Simulated Equity Curve"
                    subtitle={paperIntradayCurveRows.length > 1 ? "Built from today’s realized trade ledger so the operator can see paper progression without opening raw artifacts." : "No intraday paper trade ladder has been recorded yet in the current session."}
                    points={paperIntradayCurveRows}
                    tone={pnlTone(combinedStrategyPortfolioSnapshot.total_day_pnl)}
                    footer={`Runtime ${formatTimestamp(paperRuntimeTimestamp)} | Last fill ${formatTimestamp(closedStrategyTradeRows[0]?.exit_timestamp ?? closedStrategyTradeRows[0]?.entry_timestamp)}`}
                    className="pnl-trend-panel"
                  />
                </div>
                <div className="split-panel pnl-table-grid">
                  <div className="table-panel-shell">
                    <h3 className="subsection-title">Working Orders</h3>
                    <DataTable
                      columns={[
                        { key: "entered_time", label: "Time", render: (row) => formatTimestamp(row.entered_time ?? row.updated_at) },
                        { key: "symbol", label: "Symbol", render: (row) => formatValue(row.symbol) },
                        { key: "order_type", label: "Type", render: (row) => formatValue(row.order_type ?? row.orderType) },
                        { key: "instruction", label: "Side", render: (row) => formatValue(row.instruction ?? row.side) },
                        { key: "quantity", label: "Qty", render: (row) => formatValue(row.quantity) },
                        { key: "status", label: "Status", render: (row) => <Badge label={formatValue(row.status ?? "Working")} tone={statusTone(row.status)} /> },
                      ]}
                      rows={productionOpenOrders.slice(0, 8)}
                      emptyLabel="No broker working orders are currently open."
                    />
                  </div>
                  <div className="table-panel-shell">
                    <h3 className="subsection-title">Recent Fills</h3>
                    <DataTable
                      columns={[
                        { key: "updated_at", label: "Time", render: (row) => formatTimestamp(row.updated_at ?? row.occurred_at) },
                        { key: "symbol", label: "Symbol", render: (row) => formatValue(row.symbol) },
                        { key: "instruction", label: "Side", render: (row) => formatValue(row.instruction ?? row.side) },
                        { key: "quantity", label: "Qty", render: (row) => formatValue(row.quantity) },
                        { key: "price", label: "Price", render: (row) => formatValue(row.price ?? row.execution_price) },
                      ]}
                      rows={productionRecentFills.slice(0, 8)}
                      emptyLabel="No recent broker fills are available."
                    />
                  </div>
                </div>
              </Section>

              <Section title="Positions Monitor" subtitle="One row per symbol, separate source-of-truth modes, and deeper detail in a side drawer">
                <div className="positions-workbench">
                  <div className="positions-toolbar">
                    <div className="positions-mode-switch">
                      {(Object.keys(POSITIONS_VIEW_LABELS) as PositionsViewMode[]).map((mode) => (
                        <button
                          key={mode}
                          className={`positions-mode-button ${positionsViewMode === mode ? "active" : ""}`}
                          onClick={() => {
                            setPositionsViewMode(mode);
                            setPositionsLayoutEditorOpen(false);
                            setOpenPositionsMenuRowId(null);
                          }}
                        >
                          {POSITIONS_VIEW_LABELS[mode]}
                        </button>
                      ))}
                    </div>
                    <div className="positions-toolbar-actions">
                      <button className="panel-button subtle" onClick={() => setPositionsLayoutEditorOpen((current) => !current)}>
                        {positionsLayoutEditorOpen ? "Hide Layout Tools" : "Customize Grid"}
                      </button>
                      <button className="panel-button subtle" onClick={saveCurrentPositionsLayout}>
                        Save Layout
                      </button>
                    </div>
                  </div>

                  <div className="positions-summary-strip">
                    {positionsSummaryMetrics.map((item) => (
                      <MetricMini key={item.label} label={item.label} value={item.value} tone={item.tone} />
                    ))}
                  </div>

                  {positionsViewMode !== "paper" ? (
                    <div className={`positions-freshness-banner ${brokerFreshnessTone}`}>
                      <div className="positions-freshness-title">Broker Feed • {brokerPositionsFreshnessState}</div>
                      <div className="positions-freshness-body">
                        {brokerFreshnessItems.map((item) => (
                          <span key={item.label} className={`positions-freshness-item ${item.state.toLowerCase()}`}>
                            <span className="positions-freshness-state">{item.state}</span>
                            <strong>{item.label}</strong> {item.text}
                          </span>
                        ))}
                        <span>{brokerFreshnessDetail}</span>
                      </div>
                    </div>
                  ) : null}

                  <div className="positions-total-line">
                    {positionsTotalsLine.map((item) => (
                      <div key={item.label} className={`positions-total-chip ${item.tone ?? "muted"}`}>
                        <span className="positions-total-label">{item.label}</span>
                        <span className="positions-total-value">{item.value}</span>
                      </div>
                    ))}
                  </div>

                  {positionsLayoutEditorOpen ? (
                    <div className="positions-layout-editor">
                      <div className="positions-layout-header">
                        <div>
                          <div className="section-title small">Grid Layout</div>
                          <div className="section-subtitle">Show/hide columns, reorder the grid, and save named layouts per mode.</div>
                        </div>
                        <button className="panel-button subtle" onClick={resetPositionsColumns}>
                          Reset Default
                        </button>
                      </div>
                      <div className="positions-layout-saved">
                        <span className="metric-label">Saved Layouts</span>
                        <div className="positions-layout-chip-row">
                          {Object.keys(savedPositionLayouts).length ? (
                            Object.keys(savedPositionLayouts).map((layoutName) => (
                              <button key={layoutName} className="positions-layout-chip" onClick={() => loadSavedPositionsLayout(layoutName)}>
                                {layoutName}
                              </button>
                            ))
                          ) : (
                            <span className="placeholder-note">No named layouts saved for this mode yet.</span>
                          )}
                        </div>
                      </div>
                      <div className="positions-layout-list">
                        {POSITIONS_AVAILABLE_COLUMNS[positionsViewMode].map((columnId) => {
                          const column = positionsMonitorColumns[columnId];
                          const visible = normalizedCurrentColumns.includes(columnId);
                          return (
                            <div key={columnId} className="positions-layout-row">
                              <label className="positions-layout-toggle">
                                <input
                                  type="checkbox"
                                  checked={visible}
                                  disabled={column?.hideable === false}
                                  onChange={() => togglePositionsColumn(columnId)}
                                />
                                <span>{column?.label ?? columnId}</span>
                              </label>
                              <div className="positions-layout-move">
                                <button className="panel-button subtle" disabled={!visible || columnId === "symbol"} onClick={() => movePositionsColumn(columnId, -1)}>
                                  Left
                                </button>
                                <button className="panel-button subtle" disabled={!visible || columnId === "symbol"} onClick={() => movePositionsColumn(columnId, 1)}>
                                  Right
                                </button>
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  ) : null}

                  {sameUnderlyingBlockingConflictRows.length ? (
                    <div className="positions-inline-note">
                      {formatShortNumber(sameUnderlyingBlockingConflictRows.length)} symbols carry conflict markers. Use the `Conflict` column or row actions for detail.
                    </div>
                  ) : null}

                  <PositionsMonitorGrid
                    columns={visiblePositionsColumns}
                    rows={sortedPositionsRows}
                    sort={currentPositionsSort}
                    selectedRowId={selectedPositionsRow?.id ?? ""}
                    totalsByColumnId={positionsColumnTotals}
                    onSort={updatePositionsSort}
                    onSelectRow={(row) => setSelectedPositionsRowId(row.id)}
                    onRowContextMenu={(row) => {
                      setSelectedPositionsRowId(row.id);
                      setOpenPositionsMenuRowId(row.id);
                    }}
                    emptyLabel={
                      positionsViewMode === "broker"
                        ? "No broker positions are available in the current source-of-record snapshot."
                        : positionsViewMode === "paper"
                          ? "No paper or experimental symbols are currently surfaced."
                          : "No broker or paper symbols are currently available."
                    }
                  />

                  <div className="positions-inline-note">
                    {positionsViewMode === "combined"
                      ? "Combined mode keeps broker and paper values side by side. It does not silently merge them into one unlabeled truth."
                      : positionsViewMode === "broker"
                        ? "Broker mode reflects broker/account truth only."
                        : "Paper mode reflects paper and experimental strategy-ledger truth only."}
                  </div>
                </div>
              </Section>

              {!(positionsViewMode === "broker" && filteredClosedTradeRows.length === 0) ? (
              <Section title="Closed Trades" subtitle="Collapsed by default so the monitor grid stays dense and focused">
                <details className="positions-collapsed-card" open={positionsClosedTradesOpen} onToggle={(event) => setPositionsClosedTradesOpen((event.target as HTMLDetailsElement).open)}>
                  <summary>
                    Closed Trades
                    <span>{filteredClosedTradeRows.length ? `${formatShortNumber(visibleClosedTradeRows.length)} of ${formatShortNumber(filteredClosedTradeRows.length)} rows` : positionsViewMode === "broker" ? "No broker closed-trade ledger" : "No rows"}</span>
                  </summary>
                  <div className="positions-collapsed-body">
                    {positionsViewMode !== "broker" ? (
                      <div className="positions-closed-toolbar">
                        <label className="settings-field compact">
                          <span>Class</span>
                          <select value={positionsClosedTradesClassFilter} onChange={(event) => setPositionsClosedTradesClassFilter(event.target.value as "all" | "paper" | "experimental")}>
                            <option value="all">All</option>
                            <option value="paper">Paper</option>
                            <option value="experimental">Experimental</option>
                          </select>
                        </label>
                        <label className="settings-field compact">
                          <span>Rows</span>
                          <select value={String(positionsClosedTradesPageSize)} onChange={(event) => setPositionsClosedTradesPageSize(Number(event.target.value))}>
                            {[25, 50, 100].map((value) => (
                              <option key={value} value={value}>
                                {value}
                              </option>
                            ))}
                          </select>
                        </label>
                        <div className="positions-inline-note">
                          Showing {formatShortNumber(visibleClosedTradeRows.length)} of {formatShortNumber(filteredClosedTradeRows.length)} filtered rows.
                        </div>
                      </div>
                    ) : null}
                    <DataTable
                      columns={[
                        { key: "exit_timestamp", label: "Time", render: (row) => formatTimestamp(row.exit_timestamp ?? row.entry_timestamp) },
                        { key: "instrument", label: "Symbol", render: (row) => formatValue(row.instrument ?? row.symbol) },
                        { key: "side", label: "Side" },
                        { key: "entry_price", label: "Entry", render: (row) => formatCompactPrice(row.entry_price) },
                        { key: "exit_price", label: "Exit", render: (row) => formatCompactPrice(row.exit_price) },
                        { key: "realized_pnl", label: "Realized P/L", render: (row) => renderPnlValue(row.realized_pnl) },
                        { key: "strategy_name", label: "Strategy" },
                        { key: "class", label: "Class", render: (row) => <Badge label={tradeTopLevelClass(row)} tone={tradeTopLevelClass(row) === "EXPERIMENTAL" ? "warn" : "muted"} /> },
                        { key: "exit_reason", label: "Exit Reason" },
                      ]}
                      rows={visibleClosedTradeRows}
                      emptyLabel={
                        positionsViewMode === "broker"
                          ? "No broker closed-trade ledger is currently available in this snapshot."
                          : "No closed paper or experimental trades are currently available."
                      }
                    />
                    {filteredClosedTradeRows.length > visibleClosedTradeRows.length ? (
                      <div className="positions-closed-more">
                        <button className="panel-button subtle" onClick={() => setPositionsClosedTradesPageSize((current) => current + 25)}>
                          Show 25 More
                        </button>
                      </div>
                    ) : null}
                  </div>
                </details>
              </Section>
              ) : null}

              {selectedPositionsRow ? (
                <PositionsDrawer
                  open={positionsDrawerOpen}
                  row={selectedPositionsRow}
                  tab={positionsDrawerTab}
                  onClose={() => setPositionsDrawerOpen(false)}
                  onChangeTab={setPositionsDrawerTab}
                />
              ) : null}

              <Section title="Manual Order Ticket" subtitle="Thinkorswim-style review / confirm / send flow with explicit broker audit trail">
                {!productionLinkEnabled() ? (
                  <div className="placeholder-note">
                    Manual order entry is disabled until the Schwab production-link feature flags are turned on.
                  </div>
                ) : (
                  <>
                    <div className="metric-grid">
                      <MetricCard label="Pilot Mode" value={formatValue(productionPilotMode.label ?? (productionCapabilities.manual_live_pilot === true ? "MANUAL LIVE PILOT ACTIVE" : "MANUAL LIVE PILOT OFF"))} tone={statusTone(productionPilotMode.enabled === true ? "ready" : "blocked")} />
                      <MetricCard label="Manual Submit" value={productionManualSubmitStatusLabel} tone={productionManualSubmitEnabled ? "good" : "warn"} />
                      <MetricCard label="Dry-Run Preview" value={productionCapabilities.manual_order_preview === true ? "Enabled" : "Disabled"} tone={statusTone(productionCapabilities.manual_order_preview === true ? "ready" : "blocked")} />
                      <MetricCard label="Auth" value={formatValue(asRecord(productionHealth.auth_healthy).label)} tone={statusTone(asRecord(productionHealth.auth_healthy).label)} />
                      <MetricCard label="Broker" value={formatValue(asRecord(productionHealth.broker_reachable).label)} tone={statusTone(asRecord(productionHealth.broker_reachable).label)} />
                      <MetricCard label="Account" value={formatValue(asRecord(productionHealth.account_selected).label)} tone={statusTone(asRecord(productionHealth.account_selected).label)} />
                      <MetricCard label="Orders Fresh" value={formatValue(asRecord(productionHealth.orders_fresh).label)} tone={statusTone(asRecord(productionHealth.orders_fresh).label)} />
                      <MetricCard label="Submit Safety" value={productionFeatureFlags.live_order_submit_enabled === true ? "MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED=1" : "MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED=0"} tone={statusTone(productionFeatureFlags.live_order_submit_enabled === true ? "ready" : "blocked")} />
                      <MetricCard label="Advanced TIF UI" value={advancedTifTicketSupport ? "Dry-Run Enabled" : "Disabled"} tone={statusTone(advancedTifTicketSupport ? "ready" : "blocked")} />
                      <MetricCard label="OCO UI" value={ocoTicketSupport ? "Dry-Run Enabled" : "Disabled"} tone={statusTone(ocoTicketSupport ? "ready" : "blocked")} />
                      <MetricCard label="Trailing Live Submit" value={trailingLiveSubmitEnabled ? "Enabled" : "Disabled"} tone={statusTone(trailingLiveSubmitEnabled ? "ready" : "blocked")} />
                      <MetricCard label="Close-Order Live Submit" value={closeOrderLiveSubmitEnabled ? "Enabled" : "Disabled"} tone={statusTone(closeOrderLiveSubmitEnabled ? "ready" : "blocked")} />
                      <MetricCard label="Futures Live Submit" value={futuresLiveSubmitEnabled ? "Enabled" : "Disabled"} tone={statusTone(futuresLiveSubmitEnabled ? "ready" : "blocked")} />
                      <MetricCard label="Selected Type Status" value={formatValue(selectedManualVerificationStatusText)} tone={selectedManualVerification.live_verified === true ? "good" : "warn"} />
                      <MetricCard label="Next Verification Step" value={formatValue(nextLiveVerificationStep.label ?? nextLiveVerificationStep.verification_key)} tone={statusTone(nextLiveVerificationStep.blocked === true ? "warn" : "ready")} />
                      <MetricCard label="Replace" value={productionReplaceStatusText} tone={productionReplaceEnabled ? "good" : "warn"} />
                    </div>
                    <div className="notice-strip">
                      <div>Manual live submit gate: {productionManualSubmitStatusDetail}</div>
                      <div>Selected type gate ({formatValue(selectedManualVerificationKey ?? "None")}): {selectedManualVerificationDetail}</div>
                      <div>Replace gate: {productionReplaceStatusDetail}</div>
                    </div>
                    <div className="split-panel">
                      <div>
                        <h3 className="subsection-title">Live Manual Pilot Runbook</h3>
                        <div className="metric-grid">
                          <MetricCard label="Pilot Readiness" value={formatValue(productionPilotReadiness.label ?? (productionPilotReadiness.submit_eligible === true ? "LIVE MANUAL PILOT READY" : "LIVE MANUAL PILOT BLOCKED"))} tone={statusTone(productionPilotReadiness.submit_eligible === true ? "ready" : "blocked")} />
                          <MetricCard label="Pilot Symbol" value={formatValue(preferredPilotSymbol || "None")} tone={statusTone(preferredPilotSymbol ? "ready" : "blocked")} />
                          <MetricCard label="Allowed Route" value={`${formatValue(productionPilotOpenRoute.operator_label ?? "BUY_TO_OPEN")} / ${formatValue(productionPilotCloseRoute.operator_label ?? "SELL_TO_CLOSE")}`} tone="good" />
                          <MetricCard label="Whitelist" value={manualSymbolWhitelist.length ? manualSymbolWhitelist.join(", ") : "None"} tone={statusTone(manualSymbolWhitelist.length ? "ready" : "blocked")} />
                          <MetricCard label="Qty Cap" value={formatValue(productionPilotLockedPolicy.max_quantity ?? manualMaxQuantity)} tone="good" />
                          <MetricCard label="Allowed Type" value={formatValue(productionPilotLockedPolicy.submit_order_type ?? productionPilotLockedPolicy.order_type ?? "LIMIT")} tone="good" />
                          <MetricCard label="Entry Eligible Now" value={pilotRunbookEntryEligible ? "YES" : "NO"} tone={statusTone(pilotRunbookEntryEligible ? "ready" : "blocked")} />
                          <MetricCard label="Close Eligible Now" value={pilotRunbookCloseEligible ? "YES" : "NO"} tone={statusTone(pilotRunbookCloseEligible ? "ready" : "blocked")} />
                          <MetricCard label="Broker Health" value={formatValue(asRecord(productionHealth.broker_reachable).label)} tone={statusTone(asRecord(productionHealth.broker_reachable).label)} />
                          <MetricCard label="Auth Health" value={formatValue(asRecord(productionHealth.auth_healthy).label)} tone={statusTone(asRecord(productionHealth.auth_healthy).label)} />
                          <MetricCard label="Account Health" value={formatValue(asRecord(productionHealth.account_selected).label)} tone={statusTone(asRecord(productionHealth.account_selected).label)} />
                          <MetricCard label="Reconciliation" value={formatValue(productionReconciliation.label ?? productionReconciliation.status)} tone={statusTone(productionReconciliation.label ?? productionReconciliation.status)} />
                          <MetricCard label="Submit Eligible" value={productionPilotReadiness.submit_eligible === true ? "YES" : "NO"} tone={statusTone(productionPilotReadiness.submit_eligible === true ? "ready" : "blocked")} />
                        </div>
                        <div className="status-line">
                          Locked route: {formatValue(productionPilotLockedPolicy.asset_class ?? "STOCK")} {formatValue(productionPilotLockedPolicy.submit_order_type ?? "LIMIT")} | qty {formatValue(productionPilotLockedPolicy.max_quantity ?? "1")} | TIF {formatValue(productionPilotLockedPolicy.time_in_force ?? "DAY")} | session {formatValue(productionPilotLockedPolicy.session ?? "NORMAL")} | regular hours only {productionPilotLockedPolicy.regular_hours_only === false ? "No" : "Yes"}.
                        </div>
                        <div className="status-line">
                          Open route: {formatValue(productionPilotOpenRoute.operator_label ?? "BUY_TO_OPEN")} via {formatValue(productionPilotOpenRoute.intent_type ?? "MANUAL_LIVE_PILOT")} / {formatValue(productionPilotOpenRoute.side ?? "BUY")}. Close route: {formatValue(productionPilotCloseRoute.operator_label ?? "SELL_TO_CLOSE")} via {formatValue(productionPilotCloseRoute.intent_type ?? "FLATTEN")} / {formatValue(productionPilotCloseRoute.side ?? "SELL")}.
                        </div>
                        <div className="status-line">
                          Submit eligibility reason: {formatValue(productionPilotReadiness.detail ?? productionManualSubmitStatusDetail)}.
                        </div>
                        <div className="status-line">
                          Entry route: {pilotRunbookEntryEligible ? "eligible now" : "not eligible"} | {formatValue(pilotRunbookEntryDetail)}.
                        </div>
                        <div className="status-line">
                          Close route: {pilotRunbookCloseEligible ? "eligible now" : "not eligible"} | {formatValue(pilotRunbookCloseDetail)}.
                        </div>
                        <div className="status-line">
                          Expected lifecycle: submit_requested {"->"} ACKNOWLEDGED {"->"} WORKING or FILLED. A completed SELL_TO_CLOSE cycle then requires flat broker confirmation, reconciliation CLEAR, and passive refresh/restart proof with no extra submit.
                        </div>
                        <div className="status-line">
                          Ticket presets below load the proven live-manual pilot route directly into the standard manual ticket. No ad hoc proof harness is required for the validated stock pilot path.
                        </div>
                        {productionPilotReadiness.blocked_reason ? (
                          <div className="status-line">
                            Current blocker: {formatValue(productionPilotReadiness.blocked_reason)}.
                          </div>
                        ) : null}
                        <div className="action-row inline">
                          <button
                            className="panel-button subtle"
                            disabled={busyAction !== null || !preferredPilotSymbol}
                            onClick={() =>
                              setManualOrderForm((current) => ({
                                ...current,
                                symbol: preferredPilotSymbol,
                                assetClass: "STOCK",
                                structureType: "SINGLE",
                                intentType: "MANUAL_LIVE_PILOT",
                                side: "BUY",
                                quantity: String(productionPilotLockedPolicy.max_quantity ?? "1"),
                                orderType: String(productionPilotLockedPolicy.submit_order_type ?? productionPilotLockedPolicy.order_type ?? "LIMIT"),
                                timeInForce: String(productionPilotLockedPolicy.time_in_force ?? "DAY"),
                                session: String(productionPilotLockedPolicy.session ?? "NORMAL"),
                                reviewConfirmed: false,
                              }))
                            }
                          >
                            Set BUY_TO_OPEN Ticket
                          </button>
                          <button
                            className="panel-button subtle"
                            disabled={busyAction !== null || !selectedProductionPositionIsLongOneShare}
                            onClick={() =>
                              setManualOrderForm((current) => ({
                                ...current,
                                symbol: selectedProductionSymbol,
                                assetClass: "STOCK",
                                structureType: "SINGLE",
                                intentType: "FLATTEN",
                                side: "SELL",
                                quantity: "1",
                                orderType: String(productionPilotLockedPolicy.submit_order_type ?? productionPilotLockedPolicy.order_type ?? "LIMIT"),
                                timeInForce: String(productionPilotLockedPolicy.time_in_force ?? "DAY"),
                                session: String(productionPilotLockedPolicy.session ?? "NORMAL"),
                                reviewConfirmed: false,
                              }))
                            }
                          >
                            Set SELL_TO_CLOSE Ticket
                          </button>
                        </div>
                      </div>
                      <div>
                        <h3 className="subsection-title">Last Completed Live Cycle</h3>
                        <div className="metric-grid">
                          <MetricCard label="Buy Order ID" value={formatValue(productionPilotCycle.buy_order_id ?? productionPilotCycleBuy.broker_order_id ?? "None")} />
                          <MetricCard label="Close Order ID" value={formatValue(productionPilotCycle.close_order_id ?? productionPilotCycleClose.broker_order_id ?? "None")} />
                          <MetricCard label="Buy Fill" value={formatCompactPrice(productionPilotCycleBuy.fill_price)} tone={statusTone(productionPilotCycleBuy.fill_price ? "ready" : "muted")} />
                          <MetricCard label="Close Fill" value={formatCompactPrice(productionPilotCycleClose.fill_price)} tone={statusTone(productionPilotCycleClose.fill_price ? "ready" : "muted")} />
                          <MetricCard label="Flat Confirmed" value={productionPilotCycleFlat.confirmed === true ? "YES" : "NO"} tone={statusTone(productionPilotCycleFlat.confirmed === true ? "ready" : "blocked")} />
                          <MetricCard label="Reconcile Clean" value={productionPilotCycleReconciliation.confirmed === true ? "YES" : "NO"} tone={statusTone(productionPilotCycleReconciliation.confirmed === true ? "ready" : "blocked")} />
                          <MetricCard label="Passive Refresh" value={productionPilotCyclePassive.passive_refresh_held === true ? "HELD" : productionPilotCyclePassive.passive_refresh_held === false ? "FAILED" : "Unknown"} tone={statusTone(productionPilotCyclePassive.passive_refresh_held === true ? "ready" : productionPilotCyclePassive.passive_refresh_held === false ? "blocked" : "muted")} />
                          <MetricCard label="Cycle Completed" value={formatTimestamp(productionPilotCycle.cycle_completed_at)} />
                        </div>
                        <div className="status-line">
                          Last completed pilot cycle: BUY {formatValue(productionPilotCycleBuy.broker_order_id ?? "None")} ack {formatTimestamp(productionPilotCycleBuy.acknowledged_at)} fill {formatTimestamp(productionPilotCycleBuy.fill_timestamp ?? productionPilotCycleBuy.filled_at)}.
                        </div>
                        <div className="status-line">
                          Close leg: SELL_TO_CLOSE {formatValue(productionPilotCycleClose.broker_order_id ?? "None")} ack {formatTimestamp(productionPilotCycleClose.acknowledged_at)} fill {formatTimestamp(productionPilotCycleClose.fill_timestamp ?? productionPilotCycleClose.filled_at)}.
                        </div>
                        <div className="status-line">
                          Flat confirmation: {productionPilotCycleFlat.confirmed === true ? "Broker/manual state is flat." : "Flat confirmation not yet recorded."} Reconciliation: {formatValue(productionPilotCycleReconciliation.status ?? productionPilotCycleReconciliation.confirmed)}.
                        </div>
                        <div className="status-line">
                          Passive refresh proof: {productionPilotCyclePassive.passive_refresh_held === true ? "No extra submit was created on refresh/restart." : productionPilotCyclePassive.passive_refresh_held === false ? "Refresh proof failed." : "No passive refresh proof recorded yet."}
                        </div>
                      </div>
                    </div>
                    <div className="split-panel">
                      <div>
                        <h3 className="subsection-title">Manual Live Order Health</h3>
                        <div className="metric-grid">
                          <MetricCard label="Lifecycle" value={formatValue(productionManualLiveOrderSummary.status ?? "HEALTHY")} tone={statusTone(String(productionManualLiveOrderSummary.status ?? "HEALTHY").includes("REVIEW") ? "warn" : "ready")} />
                          <MetricCard label="Open Manual Orders" value={String(productionManualLiveOrderSummary.open_manual_order_count ?? 0)} tone={statusTone(Number(productionManualLiveOrderSummary.open_manual_order_count ?? 0) > 0 ? "warn" : "ready")} />
                          <MetricCard label="Overdue ACK" value={String(productionManualLiveOrderSummary.overdue_ack_count ?? 0)} tone={statusTone(Number(productionManualLiveOrderSummary.overdue_ack_count ?? 0) > 0 ? "warn" : "ready")} />
                          <MetricCard label="Overdue Fill" value={String(productionManualLiveOrderSummary.overdue_fill_count ?? 0)} tone={statusTone(Number(productionManualLiveOrderSummary.overdue_fill_count ?? 0) > 0 ? "warn" : "ready")} />
                          <MetricCard label="Manual Review" value={String(productionManualLiveOrderSummary.manual_review_required_count ?? 0)} tone={statusTone(Number(productionManualLiveOrderSummary.manual_review_required_count ?? 0) > 0 ? "danger" : "good")} />
                          <MetricCard label="Safe Cleanup" value={String(productionManualLiveOrderSummary.safe_cleanup_count ?? 0)} tone={statusTone(Number(productionManualLiveOrderSummary.safe_cleanup_count ?? 0) > 0 ? "good" : "muted")} />
                        </div>
                        <div className="status-line">
                          Last lifecycle check: {formatTimestamp(productionManualLiveOrders.checked_at)}. ACK timeout {formatValue(productionManualSafetyConstraints.manual_order_ack_timeout_seconds)}s, fill timeout {formatValue(productionManualSafetyConstraints.manual_order_fill_timeout_seconds)}s, reconcile grace {formatValue(productionManualSafetyConstraints.manual_order_reconcile_grace_seconds)}s, post-ack broker-confirm grace {formatValue(productionManualSafetyConstraints.manual_order_post_ack_grace_seconds)}s.
                        </div>
                        <div className="status-line">
                          {productionActiveManualLiveOrders.length
                            ? `${productionActiveManualLiveOrders.length} live manual order${productionActiveManualLiveOrders.length === 1 ? "" : "s"} still need tracking.`
                            : "No active manual live orders currently require tracking."}
                        </div>
                      </div>
                      <div>
                        <h3 className="subsection-title">Current Manual Live Orders</h3>
                        <DataTable
                          columns={[
                            { key: "broker_order_id", label: "Broker Order ID" },
                            { key: "symbol", label: "Symbol" },
                            { key: "intent_type", label: "Intent" },
                            { key: "side", label: "Side" },
                            { key: "quantity", label: "Qty" },
                            { key: "lifecycle_state", label: "Lifecycle" },
                            { key: "broker_order_status", label: "Broker Status" },
                            { key: "direct_status_last_outcome", label: "Broker Confirm" },
                            { key: "terminal_resolution", label: "Terminal Proof" },
                            { key: "cancel_resolution", label: "Cancel Proof" },
                            { key: "recommended_action", label: "Next Action" },
                          ]}
                          rows={productionActiveManualLiveOrders}
                          emptyLabel="No active manual live orders currently require follow-through."
                        />
                      </div>
                    </div>
                    <div className="split-panel">
                      <div>
                        <h3 className="subsection-title">Manual Live Order Proof</h3>
                        <div className="metric-grid">
                          <MetricCard label="Pilot Status" value={formatValue(productionPilotMode.label ?? "Unknown")} tone={statusTone(productionPilotMode.enabled === true ? "ready" : "blocked")} />
                          <MetricCard label="Last Scenario" value={formatValue(productionManualValidationLatest.scenario_type ?? "None")} />
                          <MetricCard label="Occurred" value={formatTimestamp(productionManualValidationLatest.occurred_at)} />
                          <MetricCard label="Last Lifecycle" value={formatValue(asRecord(productionManualValidationLatest.payload).lifecycle_transition_observed ? `${formatValue(asRecord(asRecord(productionManualValidationLatest.payload).lifecycle_transition_observed).from ?? "None")} -> ${formatValue(asRecord(asRecord(productionManualValidationLatest.payload).lifecycle_transition_observed).to ?? "None")}` : asRecord(productionManualValidationLatest.payload).restore_result ?? "None")} />
                          <MetricCard label="Dup Action Guard" value={asRecord(productionManualValidationLatest.payload).duplicate_action_prevention_held === false ? "FAILED" : "HELD"} tone={asRecord(productionManualValidationLatest.payload).duplicate_action_prevention_held === false ? "danger" : "good"} />
                        </div>
                        <div className="status-line">
                          Pilot scope: {formatValue(productionPilotLockedPolicy.asset_class ?? productionPilotScope.asset_class ?? productionPilotMode.scope?.asset_class ?? "STOCK")} {formatValue(productionPilotLockedPolicy.submit_order_type ?? productionPilotLockedPolicy.order_type ?? productionPilotScope.submit_order_type ?? productionPilotScope.order_type ?? productionPilotMode.scope?.submit_order_type ?? productionPilotMode.scope?.order_type ?? "LIMIT")} | max qty {formatValue(productionPilotLockedPolicy.max_quantity ?? productionPilotScope.max_quantity ?? productionPilotMode.scope?.max_quantity ?? "1")} | TIF {formatValue(productionPilotLockedPolicy.time_in_force ?? productionPilotScope.time_in_force ?? productionPilotMode.scope?.time_in_force ?? "DAY")} | session {formatValue(productionPilotLockedPolicy.session ?? productionPilotScope.session ?? productionPilotMode.scope?.session ?? "NORMAL")}.
                        </div>
                        <div className="status-line">
                          Last manual live order: {formatValue(asRecord(productionLastManualOrder.result).broker_order_id ?? asRecord(productionLastManualOrder.request).symbol ?? "None")} | Lifecycle {formatValue(productionRecentManualLiveOrders[0]?.lifecycle_state ?? "None")} | Review required {productionRecentManualLiveOrders[0]?.manual_action_required === true ? "Yes" : "No"}.
                        </div>
                        <div className="status-line">
                          Broker confirmation: {formatValue(productionRecentManualLiveOrders[0]?.direct_status_last_outcome ?? "None")} | {formatValue(productionRecentManualLiveOrders[0]?.direct_status_last_detail ?? "No direct broker order-status check recorded yet.")}.
                        </div>
                        <div className="status-line">
                          Post-ack grace: start {formatTimestamp(productionRecentManualLiveOrders[0]?.post_ack_grace_started_at)} | expires {formatTimestamp(productionRecentManualLiveOrders[0]?.post_ack_grace_expires_at)}.
                        </div>
                        <div className="status-line">
                          Open-order evidence: last seen {formatTimestamp(productionRecentManualLiveOrders[0]?.last_open_order_observed_at)} | first seen {formatTimestamp(productionRecentManualLiveOrders[0]?.first_open_order_observed_at)}.
                        </div>
                        <div className="status-line">
                          Fill / position evidence: first fill {formatTimestamp(productionRecentManualLiveOrders[0]?.first_fill_observed_at)} | first position {formatTimestamp(productionRecentManualLiveOrders[0]?.first_position_observed_at)}.
                        </div>
                        <div className="status-line">
                          Terminal classification: {formatValue(productionRecentManualLiveOrders[0]?.terminal_resolution ?? "None")} | {formatValue(productionRecentManualLiveOrders[0]?.terminal_resolution_detail ?? "No broker-backed terminal resolution recorded yet.")}.
                        </div>
                        <div className="status-line">
                          Cancel disposition: {formatValue(productionRecentManualLiveOrders[0]?.cancel_resolution ?? "None")} | {formatValue(productionRecentManualLiveOrders[0]?.cancel_resolution_detail ?? "No cancel-specific broker proof recorded.")}.
                        </div>
                        {asRecord(productionLastManualOrder.result).ok === false ? (
                          <div className="status-line">
                            Last submit failure: {formatValue(asRecord(productionLastManualOrder.result).error ?? "Broker validation rejected the manual live order before broker-order creation.")}.
                          </div>
                        ) : null}
                        <div className="status-line">
                          Last broker refresh {formatTimestamp(productionLastOrdersRefreshAt)} | ack {formatTimestamp(productionRecentManualLiveOrders[0]?.acknowledged_at)} | fill {formatTimestamp(productionRecentManualLiveOrders[0]?.filled_at)} | cancel {formatTimestamp(productionRecentManualLiveOrders[0]?.cancel_requested_at ?? productionRecentManualLiveOrders[0]?.cancelled_at)}.
                        </div>
                      </div>
                      <div>
                        <h3 className="subsection-title">Recent Proof Events</h3>
                        <DataTable
                          columns={[
                            { key: "scenario_type", label: "Scenario" },
                            { key: "occurred_at", label: "Time", render: (row) => formatTimestamp(row.occurred_at) },
                            {
                              key: "payload",
                              label: "Result",
                              render: (row) =>
                                formatValue(
                                  asRecord(row.payload).restore_result ??
                                    asRecord(asRecord(row.payload).lifecycle_transition_observed).to ??
                                    asRecord(asRecord(row.payload).timeout_state).classification ??
                                    "Recorded",
                                ),
                            },
                          ]}
                          rows={productionManualValidationEvents.slice(0, 6)}
                          emptyLabel="No manual live proof events have been recorded yet."
                        />
                      </div>
                    </div>
                    <div>
                      <h3 className="subsection-title">Recent Manual Live Activity</h3>
                        <DataTable
                          columns={[
                            { key: "broker_order_id", label: "Broker Order ID" },
                            { key: "symbol", label: "Symbol" },
                            { key: "intent_type", label: "Intent" },
                            { key: "lifecycle_state", label: "Lifecycle" },
                            { key: "direct_status_last_outcome", label: "Broker Confirm" },
                            { key: "terminal_resolution", label: "Terminal Proof" },
                            { key: "cancel_resolution", label: "Cancel Proof" },
                            { key: "submitted_at", label: "Submitted", render: (row) => formatTimestamp(row.submitted_at) },
                            { key: "issue_detail", label: "Detail", render: (row) => formatValue(row.issue_detail ?? row.terminal_resolution_detail ?? row.direct_status_last_detail ?? row.cancel_resolution_detail) },
                          ]}
                          rows={productionRecentManualLiveOrders.slice(0, 8)}
                          emptyLabel="No recent manual live-order activity is currently recorded."
                        />
                    </div>
                    <div className="notice-strip">
                      <div>This ticket submits live broker requests through the isolated Schwab production-link path. Broker truth remains separate from the frozen paper runtime.</div>
                      <div>Manual live flow only: operator auth + confirmation are required, and autonomous strategy submission is still intentionally disabled.</div>
                      <div>Supported manual asset classes: {manualAssetClasses.join(", ") || "None configured"}.</div>
                      <div>Reviewable order types for {manualOrderForm.assetClass}: {manualOrderTypes.join(", ") || "None configured"}.</div>
                      <div>Live-enabled order types for {manualOrderForm.assetClass}: {liveEnabledOrderTypesForAsset.join(", ") || "None live-enabled"}.</div>
                      <div>Dry-run-only order types for {manualOrderForm.assetClass}: {dryRunOnlyOrderTypesForAsset.join(", ") || "None"}.</div>
                      <div>Supported TIF values: {manualTimeInForceOptions.join(", ") || "None configured"}.</div>
                      <div>Supported session values: {manualSessionOptions.join(", ") || "None configured"}.</div>
                      <div>Symbol whitelist: {manualSymbolWhitelist.length ? manualSymbolWhitelist.join(", ") : "No symbols whitelisted; submit stays blocked."}</div>
                      <div>Max quantity: {manualMaxQuantity}. Only `DAY` + `NORMAL` is enabled in the first live-order safety mode.</div>
                      <div>{advancedTifTicketSupport ? "EXTO / GTC_EXTO review is available in dry-run mode only." : "EXTO / GTC_EXTO review is disabled for this environment."}</div>
                      <div>{ocoTicketSupport ? "OCO review is available in dry-run mode only." : "OCO review is disabled for this environment."}</div>
                      <div>{extExtoLiveSubmitEnabled ? "EXTO / GTC_EXTO live submit is enabled." : "EXTO / GTC_EXTO live submit remains disabled pending live broker verification."}</div>
                      <div>{ocoLiveSubmitEnabled ? "OCO live submit is enabled." : "OCO live submit remains disabled pending live broker verification."}</div>
                      <div>{trailingLiveSubmitEnabled ? "Trailing live submit is enabled." : "Trailing live submit remains disabled pending live broker verification."}</div>
                      <div>{closeOrderLiveSubmitEnabled ? "Market-on-close / limit-on-close live submit is enabled." : "Market-on-close / limit-on-close live submit remains disabled pending live broker verification."}</div>
                      <div>{futuresLiveSubmitEnabled ? "Futures live submit is enabled." : "Futures live submit remains disabled pending live broker verification."}</div>
                      <div>{productionCapabilities.sell_short === true ? "SELL_SHORT is enabled for this environment." : "SELL_SHORT remains disabled until account permissions and Schwab product support are live-verified."}</div>
                      {!productionReconciliationFresh ? <div>Reconciliation is not clear: {formatValue(productionReconciliation.detail)}</div> : null}
                    </div>
                    {firstLiveStockLimitActive ? (
                      <div className="notice-strip">
                        <div>First live order path: STOCK LIMIT only.</div>
                        <div>Readiness now: {firstLiveStockLimitReadyNow ? "READY TO SUBMIT" : "BLOCKED"}.</div>
                        <div>Required flags: {asArray<string>(firstLiveStockLimitTest.required_flags).join(", ") || "None published"}.</div>
                        <div>Required whitelist: {asArray<string>(asRecord(firstLiveStockLimitTest.required_config).manual_symbol_whitelist).join(", ") || "Set one safe stock symbol before submit."}.</div>
                        <div>Required account state: {asArray<string>(firstLiveStockLimitTest.required_account_state).join(" | ") || "See diagnostics"}.</div>
                        <div>Required freshness state: {asArray<string>(firstLiveStockLimitTest.required_freshness_state).join(" | ") || "See diagnostics"}.</div>
                        <div>Submit path: {asArray<string>(firstLiveStockLimitTest.submit_path).join(" -> ") || "Open Manual Order Ticket"}.</div>
                        <div>Cancel path: {asArray<string>(firstLiveStockLimitTest.cancel_path).join(" -> ") || "Cancel Open Order after WORKING status."}.</div>
                        <div>Post-submit reconciliation: {asArray<string>(firstLiveStockLimitTest.expected_reconciliation_checks).join(" | ") || "Reconciliation must remain CLEAR."}.</div>
                      </div>
                    ) : null}
                    {manualAdvancedDryRun ? (
                      <div className="notice-strip">
                        <div>Advanced order review mode is active for this ticket.</div>
                        <div>Dry-run payload review is supported.</div>
                        <div>Live submit is intentionally disabled for {manualOrderForm.structureType === "OCO" ? "OCO" : manualAdvancedMode} in this phase.</div>
                      </div>
                    ) : null}
                    <div className="status-line">
                      Broker state: {formatValue(asRecord(productionHealth.broker_reachable).label)}. Auth: {formatValue(asRecord(productionHealth.auth_healthy).label)}. Account selected: {formatValue(asRecord(productionHealth.account_selected).label)}.
                    </div>
                    <div className="status-line">
                      Structure: {manualOrderForm.structureType}. Mode: {manualAdvancedMode}. Replace is {productionReplaceEnabled ? "enabled for this environment." : "disabled until Schwab replace semantics are live-verified."}
                    </div>
                    <div className="status-line">
                      Intent type: {manualIntentTypeLabel(manualOrderForm.intentType)}. Operator note: {manualOrderForm.operatorNote.trim() || "None"}.
                    </div>
                    <div className="status-line">
                      Verification key: {formatValue(selectedManualVerificationKey)} | Previewable: {formatValue(selectedManualVerification.previewable)} | Live enabled: {formatValue(selectedManualVerification.live_enabled)} | Live verified: {formatValue(selectedManualVerification.live_verified)}.
                    </div>
                    {firstLiveStockLimitActive ? (
                      <div className="status-line">
                        First live STOCK LIMIT checklist: quantity 1, DAY, NORMAL, whitelisted symbol, review confirmed, selected live Schwab account, fresh balances/positions/orders, reconciliation CLEAR, regular US market hours.
                      </div>
                    ) : null}
                    {selectedManualVerification.blocker_reason ? (
                      <div className="status-line">Selected type blocker: {formatValue(selectedManualVerification.blocker_reason)}.</div>
                    ) : null}
                    <div className="settings-field">
                      <span>Broker account</span>
                      <select
                        value={manualOrderForm.accountHash}
                        disabled={busyAction !== null || !productionLinkEnabled()}
                        onChange={(event) => {
                          const nextHash = event.target.value;
                          setManualOrderForm((current) => ({ ...current, accountHash: nextHash }));
                          void runCommand("select-broker-account", () => api.runProductionLinkAction("select-account", { account_hash: nextHash }), { requiresLive: true });
                        }}
                      >
                        {productionAccounts.map((account) => (
                          <option key={String(account.account_hash)} value={String(account.account_hash)}>
                            {maskAccountNumber(account.account_number)} / {formatValue(account.account_type)}
                          </option>
                        ))}
                      </select>
                    </div>
                    <div className="ticket-grid">
                      <label className="settings-field">
                        <span>Symbol</span>
                        <input disabled={busyAction !== null} value={manualOrderForm.symbol} onChange={(event) => setManualOrderForm((current) => ({ ...current, symbol: event.target.value.toUpperCase() }))} />
                      </label>
                      <label className="settings-field">
                        <span>Order structure</span>
                        <select disabled={busyAction !== null} value={manualOrderForm.structureType} onChange={(event) => setManualOrderForm((current) => ({ ...current, structureType: event.target.value }))}>
                          <option value="SINGLE">Single</option>
                          <option value="OCO">OCO</option>
                        </select>
                      </label>
                      <label className="settings-field">
                        <span>Asset class</span>
                        <select disabled={busyAction !== null} value={manualOrderForm.assetClass} onChange={(event) => setManualOrderForm((current) => ({ ...current, assetClass: event.target.value }))}>
                          {manualAssetClasses.map((assetClass) => (
                            <option key={assetClass} value={assetClass}>
                              {sentenceCase(assetClass.toLowerCase())}
                            </option>
                          ))}
                        </select>
                      </label>
                      <label className="settings-field">
                        <span>Intent type</span>
                        <select disabled={busyAction !== null} value={manualOrderForm.intentType} onChange={(event) => setManualOrderForm((current) => ({ ...current, intentType: event.target.value }))}>
                          {["MANUAL_LIVE_PILOT", "FLATTEN", "ENTRY", "EXIT", "ADJUSTMENT", "MANUAL"].map((intentType) => (
                            <option key={intentType} value={intentType}>
                              {manualIntentTypeLabel(intentType)}
                            </option>
                          ))}
                        </select>
                      </label>
                      <label className="settings-field">
                        <span>Quantity</span>
                        <input disabled={busyAction !== null} value={manualOrderForm.quantity} onChange={(event) => setManualOrderForm((current) => ({ ...current, quantity: event.target.value }))} />
                      </label>
                      <label className="settings-field">
                        <span>Time in force</span>
                        <select disabled={busyAction !== null} value={manualOrderForm.timeInForce} onChange={(event) => setManualOrderForm((current) => ({ ...current, timeInForce: event.target.value }))}>
                          {manualTimeInForceOptions.map((timeInForce) => (
                            <option key={timeInForce} value={timeInForce}>
                              {timeInForce}
                            </option>
                          ))}
                        </select>
                      </label>
                      <label className="settings-field">
                        <span>Session</span>
                        <select disabled={busyAction !== null} value={manualOrderForm.session} onChange={(event) => setManualOrderForm((current) => ({ ...current, session: event.target.value }))}>
                          {manualSessionOptions.map((session) => (
                            <option key={session} value={session}>
                              {session}
                            </option>
                          ))}
                        </select>
                      </label>
                      <label className="settings-field">
                        <span>Operator note</span>
                        <input disabled={busyAction !== null} value={manualOrderForm.operatorNote} onChange={(event) => setManualOrderForm((current) => ({ ...current, operatorNote: event.target.value }))} />
                      </label>
                      {manualOrderForm.structureType === "SINGLE" ? (
                        <>
                          <label className="settings-field">
                            <span>Side</span>
                            <select disabled={busyAction !== null} value={manualOrderForm.side} onChange={(event) => setManualOrderForm((current) => ({ ...current, side: event.target.value }))}>
                              {manualSideOptions.map((side) => (
                                <option key={side} value={side}>
                                  {sentenceCase(side.replace(/_/g, " ").toLowerCase())}
                                </option>
                              ))}
                            </select>
                          </label>
                          <label className="settings-field">
                            <span>Order type</span>
                            <select disabled={busyAction !== null} value={manualOrderForm.orderType} onChange={(event) => setManualOrderForm((current) => ({ ...current, orderType: event.target.value }))}>
                              {manualOrderTypes.map((orderType) => (
                                <option key={orderType} value={orderType}>
                                  {sentenceCase(orderType.replace(/_/g, "-").toLowerCase())}
                                </option>
                              ))}
                            </select>
                          </label>
                          {["LIMIT", "STOP_LIMIT", "LIMIT_ON_CLOSE"].includes(manualOrderForm.orderType) ? (
                            <label className="settings-field">
                              <span>{manualOrderForm.orderType === "LIMIT_ON_CLOSE" ? "Limit-on-close price" : "Limit price"}</span>
                              <input disabled={busyAction !== null} value={manualOrderForm.limitPrice} onChange={(event) => setManualOrderForm((current) => ({ ...current, limitPrice: event.target.value }))} />
                            </label>
                          ) : null}
                          {["STOP", "STOP_LIMIT"].includes(manualOrderForm.orderType) ? (
                            <label className="settings-field">
                              <span>Stop price</span>
                              <input disabled={busyAction !== null} value={manualOrderForm.stopPrice} onChange={(event) => setManualOrderForm((current) => ({ ...current, stopPrice: event.target.value }))} />
                            </label>
                          ) : null}
                          {["TRAIL_STOP", "TRAIL_STOP_LIMIT"].includes(manualOrderForm.orderType) ? (
                            <>
                              <label className="settings-field">
                                <span>Trail mode</span>
                                <select disabled={busyAction !== null} value={manualOrderForm.trailValueType} onChange={(event) => setManualOrderForm((current) => ({ ...current, trailValueType: event.target.value }))}>
                                  <option value="AMOUNT">Amount</option>
                                  <option value="PERCENT">Percent</option>
                                </select>
                              </label>
                              <label className="settings-field">
                                <span>Trail value</span>
                                <input disabled={busyAction !== null} value={manualOrderForm.trailValue} onChange={(event) => setManualOrderForm((current) => ({ ...current, trailValue: event.target.value }))} />
                              </label>
                              <label className="settings-field">
                                <span>Trail trigger basis</span>
                                <select disabled={busyAction !== null} value={manualOrderForm.trailTriggerBasis} onChange={(event) => setManualOrderForm((current) => ({ ...current, trailTriggerBasis: event.target.value }))}>
                                  <option value="LAST">LAST</option>
                                  <option value="BID">BID</option>
                                  <option value="ASK">ASK</option>
                                  <option value="MARK">MARK</option>
                                </select>
                              </label>
                              {manualOrderForm.orderType === "TRAIL_STOP_LIMIT" ? (
                                <label className="settings-field">
                                  <span>Trail limit offset</span>
                                  <input disabled={busyAction !== null} value={manualOrderForm.trailLimitOffset} onChange={(event) => setManualOrderForm((current) => ({ ...current, trailLimitOffset: event.target.value }))} />
                                </label>
                              ) : null}
                            </>
                          ) : null}
                        </>
                      ) : null}
                    </div>
                    {manualOrderForm.structureType === "OCO" ? (
                      <div className="split-panel">
                        {manualOrderForm.ocoLegs.map((leg, index) => (
                          <div key={leg.legLabel}>
                            <h3 className="subsection-title">{leg.legLabel}</h3>
                            <div className="ticket-grid">
                              <label className="settings-field">
                                <span>Side</span>
                                <select disabled={busyAction !== null} value={leg.side} onChange={(event) => updateOcoLeg(index, "side", event.target.value)}>
                                  {manualSideOptions.map((side) => (
                                    <option key={side} value={side}>
                                      {sentenceCase(side.replace(/_/g, " ").toLowerCase())}
                                    </option>
                                  ))}
                                </select>
                              </label>
                              <label className="settings-field">
                                <span>Quantity</span>
                                <input disabled={busyAction !== null} value={leg.quantity} onChange={(event) => updateOcoLeg(index, "quantity", event.target.value)} />
                              </label>
                              <label className="settings-field">
                                <span>Order type</span>
                                <select disabled={busyAction !== null} value={leg.orderType} onChange={(event) => updateOcoLeg(index, "orderType", event.target.value)}>
                                  {manualOrderTypes.map((orderType) => (
                                    <option key={orderType} value={orderType}>
                                      {sentenceCase(orderType.replace(/_/g, "-").toLowerCase())}
                                    </option>
                                  ))}
                                </select>
                              </label>
                              {["LIMIT", "STOP_LIMIT", "LIMIT_ON_CLOSE"].includes(leg.orderType) ? (
                                <label className="settings-field">
                                  <span>{leg.orderType === "LIMIT_ON_CLOSE" ? "Limit-on-close price" : "Limit price"}</span>
                                  <input disabled={busyAction !== null} value={leg.limitPrice} onChange={(event) => updateOcoLeg(index, "limitPrice", event.target.value)} />
                                </label>
                              ) : null}
                              {["STOP", "STOP_LIMIT"].includes(leg.orderType) ? (
                                <label className="settings-field">
                                  <span>Stop price</span>
                                  <input disabled={busyAction !== null} value={leg.stopPrice} onChange={(event) => updateOcoLeg(index, "stopPrice", event.target.value)} />
                                </label>
                              ) : null}
                              {["TRAIL_STOP", "TRAIL_STOP_LIMIT"].includes(leg.orderType) ? (
                                <>
                                  <label className="settings-field">
                                    <span>Trail mode</span>
                                    <select disabled={busyAction !== null} value={leg.trailValueType} onChange={(event) => updateOcoLeg(index, "trailValueType", event.target.value)}>
                                      <option value="AMOUNT">Amount</option>
                                      <option value="PERCENT">Percent</option>
                                    </select>
                                  </label>
                                  <label className="settings-field">
                                    <span>Trail value</span>
                                    <input disabled={busyAction !== null} value={leg.trailValue} onChange={(event) => updateOcoLeg(index, "trailValue", event.target.value)} />
                                  </label>
                                  <label className="settings-field">
                                    <span>Trail trigger basis</span>
                                    <select disabled={busyAction !== null} value={leg.trailTriggerBasis} onChange={(event) => updateOcoLeg(index, "trailTriggerBasis", event.target.value)}>
                                      <option value="LAST">LAST</option>
                                      <option value="BID">BID</option>
                                      <option value="ASK">ASK</option>
                                      <option value="MARK">MARK</option>
                                    </select>
                                  </label>
                                  {leg.orderType === "TRAIL_STOP_LIMIT" ? (
                                    <label className="settings-field">
                                      <span>Trail limit offset</span>
                                      <input disabled={busyAction !== null} value={leg.trailLimitOffset} onChange={(event) => updateOcoLeg(index, "trailLimitOffset", event.target.value)} />
                                    </label>
                                  ) : null}
                                </>
                              ) : null}
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : null}
                    <label className="settings-toggle">
                      <input
                        type="checkbox"
                        checked={manualOrderForm.reviewConfirmed}
                        disabled={busyAction !== null}
                        onChange={(event) => setManualOrderForm((current) => ({ ...current, reviewConfirmed: event.target.checked }))}
                      />
                      <span>Review confirmed. Do not autosend without this explicit confirmation.</span>
                    </label>
                    <div className="status-line">
                      Preview: {manualOrderForm.structureType === "OCO" ? "OCO" : manualOrderForm.orderType} {manualOrderForm.structureType === "OCO" ? "review" : manualOrderForm.side} {manualOrderForm.quantity} {manualOrderForm.symbol || "SYMBOL"} on {maskAccountNumber(selectedManualAccount?.account_number)}
                    </div>
                    <div className="status-line">Current manual broker submit gate: {productionManualSubmitEnabled ? "healthy" : "blocked"}.</div>
                    <div className="status-line">Selected symbol: {manualOrderForm.symbol.trim() || "None"} | Asset class: {manualOrderForm.assetClass} | Structure: {manualOrderForm.structureType} | Mode: {manualAdvancedMode}.</div>
                    <div className="status-line">{manualOrderForm.structureType === "OCO" ? "OCO relationship: when one leg fills, the other leg should be canceled." : "Single-order review path."}</div>
                    {manualPreviewBlockers.length ? (
                      <div className="notice-strip">
                        <div>Dry-run preview is currently blocked.</div>
                        {manualPreviewBlockers.map((item) => (
                          <div key={`preview-${item}`}>{item}</div>
                        ))}
                      </div>
                    ) : null}
                    {manualOrderGateBlockers.length ? (
                      <div className="notice-strip">
                        <div>Manual live broker submission is currently gated off.</div>
                        {manualOrderGateBlockers.map((item) => (
                          <div key={item}>{item}</div>
                        ))}
                      </div>
                    ) : null}
                    {productionManualSafetyWarnings.length ? (
                      <div className="notice-strip">
                        {productionManualSafetyWarnings.map((item) => (
                          <div key={item}>{item}</div>
                        ))}
                      </div>
                    ) : null}
                    <div className="action-row inline">
                      <button
                        className="panel-button subtle"
                        disabled={busyAction !== null || manualPreviewBlockers.length > 0}
                        onClick={() =>
                          void runCommand(
                            "preview-broker-order",
                            () => api.runProductionLinkAction("preview-order", buildManualOrderPayload()),
                            {
                              requiresLive: true,
                              confirmMessage: `Build a dry-run payload preview for ${manualOrderForm.structureType === "OCO" ? "OCO" : manualAdvancedMode} ${manualOrderForm.symbol || "SYMBOL"} without sending a live order?`,
                            },
                          )
                        }
                      >
                        {busyAction === "preview-broker-order" ? "Building Preview..." : "Build Dry-Run Payload"}
                      </button>
                      <button
                        className="panel-button"
                        disabled={busyAction !== null || !productionManualSubmitEnabled || manualOrderGateBlockers.length > 0}
                        onClick={() =>
                          void runCommand(
                            "submit-broker-order",
                            () => api.runProductionLinkAction("submit-order", buildManualOrderPayload()),
                            {
                              requiresLive: true,
                              confirmMessage: `Final live order confirmation: ${manualOrderForm.orderType} ${manualOrderForm.side} ${manualOrderForm.quantity} ${manualOrderForm.symbol || "SYMBOL"} on ${maskAccountNumber(selectedManualAccount?.account_number)}. This is a real Schwab production-link order.`,
                            },
                          )
                        }
                      >
                        {busyAction === "submit-broker-order" ? "Sending..." : "Review / Confirm / Send"}
                      </button>
                      <button
                        className="panel-button subtle"
                        disabled={busyAction !== null || !productionManualSubmitEnabled || !selectedProductionPosition}
                        onClick={() =>
                          void runCommand(
                            "flatten-broker-position",
                            () =>
                              api.runProductionLinkAction("flatten-position", {
                                account_hash: manualOrderForm.accountHash,
                                symbol: selectedProductionPosition?.symbol,
                                asset_class: selectedProductionPosition?.asset_class,
                                quantity: selectedProductionPosition?.quantity,
                                side: selectedProductionPosition?.side,
                              }),
                            { requiresLive: true, confirmMessage: "Submit a flatten market order for the selected broker position?" },
                          )
                        }
                      >
                        Flatten Selected Position
                      </button>
                    </div>
                    <div className="split-panel">
                      {firstLiveStockLimitActive ? (
                        <div>
                          <h3 className="subsection-title">First Live STOCK LIMIT Readiness</h3>
                          <DataTable
                            columns={[
                              { key: "label", label: "Requirement" },
                              { key: "ok", label: "Status", render: (row) => (row.ok === true ? "PASS" : "BLOCKED") },
                              { key: "detail", label: "Detail" },
                            ]}
                            rows={firstLiveStockLimitReadiness.map((item) => ({
                              label: item.label,
                              ok: item.ok,
                              detail: item.detail,
                            }))}
                            emptyLabel="No first-live stock-limit readiness checks are available."
                          />
                        </div>
                      ) : null}
                    </div>
                    <div className="split-panel">
                      <div>
                        <h3 className="subsection-title">Interpreted Review Summary</h3>
                        <JsonBlock
                          value={{
                            structure_type: manualOrderForm.structureType,
                            advanced_mode: manualAdvancedMode,
                            selected_verification_key: selectedManualVerificationKey,
                            selected_verification_status: selectedManualVerification,
                            next_live_verification_step: nextLiveVerificationStep,
                            near_term_verification_runbooks: nearTermLiveVerificationRunbooks,
                            first_live_stock_limit_test: firstLiveStockLimitActive ? firstLiveStockLimitTest : null,
                            first_live_stock_limit_readiness: firstLiveStockLimitActive ? firstLiveStockLimitReadiness : null,
                            first_live_stock_limit_ready_now: firstLiveStockLimitActive ? firstLiveStockLimitReadyNow : null,
                            dry_run_supported: manualPreviewBlockers.length === 0,
                            live_submit_disabled_reason:
                              manualOrderGateBlockers.length > 0 ? manualOrderGateBlockers : null,
                            selected_account: selectedManualAccount?.account_hash ?? null,
                            payload_request: buildManualOrderPayload(),
                          }}
                        />
                      </div>
                      <div>
                        <h3 className="subsection-title">Last Dry-Run Payload</h3>
                        <JsonBlock value={productionLastManualOrderPreview || null} />
                      </div>
                    </div>
                    <div className="split-panel">
                      <div>
                        <h3 className="subsection-title">Cancel Open Order</h3>
                        <label className="settings-field">
                          <span>Broker order ID</span>
                          <input disabled={busyAction !== null} value={cancelOrderId} onChange={(event) => setCancelOrderId(event.target.value)} />
                        </label>
                        <button
                          className="panel-button subtle"
                          disabled={busyAction !== null || !productionManualCancelEnabled || !cancelOrderId.trim()}
                          onClick={() =>
                            void runCommand(
                              "cancel-broker-order",
                              () => api.runProductionLinkAction("cancel-order", { account_hash: manualOrderForm.accountHash, broker_order_id: cancelOrderId.trim() }),
                              { requiresLive: true, confirmMessage: "Cancel this open broker order?" },
                            )
                          }
                        >
                          Cancel Open Order
                        </button>
                      </div>
                      <div>
                        <h3 className="subsection-title">Replace Order</h3>
                        {!productionReplaceEnabled ? (
                          <div className="status-line">Replace stays disabled in this phase until Schwab replace semantics are live-verified.</div>
                        ) : null}
                        <label className="settings-field">
                          <span>Broker order ID</span>
                          <input disabled={busyAction !== null || !productionReplaceEnabled} value={replaceOrderId} onChange={(event) => setReplaceOrderId(event.target.value)} />
                        </label>
                        <button
                          className="panel-button subtle"
                          disabled={busyAction !== null || !productionManualSubmitEnabled || !productionReplaceEnabled || !replaceOrderId.trim()}
                          onClick={() =>
                            void runCommand(
                              "replace-broker-order",
                              () => api.runProductionLinkAction("replace-order", { ...buildManualOrderPayload(), broker_order_id: replaceOrderId.trim() }),
                              { requiresLive: true, confirmMessage: "Replace this broker order with the ticket details shown above?" },
                            )
                          }
                        >
                          Replace / Modify
                        </button>
                      </div>
                    </div>
                  </>
                )}
              </Section>

              <Section title="Broker Orders and Fills" subtitle="Persisted broker acknowledgements, working orders, fills, cancels, rejects, and recent events">
                <div className="metric-grid">
                  <MetricCard label="Open Orders" value={String(productionOpenOrders.length)} />
                  <MetricCard label="Recent Fills" value={String(productionRecentFills.length)} />
                  <MetricCard label="Recent Events" value={String(productionEvents.length)} />
                  <MetricCard label="Missing Local Orders" value={String(productionReconciliationCategories.missing_local_orders ?? 0)} tone={statusTone(Number(productionReconciliationCategories.missing_local_orders ?? 0) > 0 ? "warn" : "ready")} />
                  <MetricCard label="Missing Broker Orders" value={String(productionReconciliationCategories.missing_broker_orders ?? 0)} tone={statusTone(Number(productionReconciliationCategories.missing_broker_orders ?? 0) > 0 ? "warn" : "ready")} />
                  <MetricCard label="Position Mismatches" value={String(productionReconciliationCategories.position_mismatches ?? 0)} tone={statusTone(Number(productionReconciliationCategories.position_mismatches ?? 0) > 0 ? "warn" : "ready")} />
                </div>
                <div className={`positions-freshness-banner ${freshnessToneFromState(ordersFillsFreshnessState)} compact`}>
                  <div className="positions-freshness-title">Orders / Fills • {ordersFillsFreshnessState}</div>
                  <div className="positions-freshness-body">
                    <span className={`positions-freshness-item ${ordersFillsFreshnessState.toLowerCase()}`}>
                      <span className="positions-freshness-state">{ordersFillsFreshnessState}</span>
                      <strong>Orders</strong> {brokerOrdersTimestamp ? `${formatTimestamp(brokerOrdersTimestamp)} • ${formatRelativeAge(brokerOrdersTimestamp)}` : "Unavailable"}
                    </span>
                    <span className={`positions-freshness-item ${ordersFillsFreshnessState.toLowerCase()}`}>
                      <span className="positions-freshness-state">{ordersFillsFreshnessState}</span>
                      <strong>Fills</strong> {brokerFillsTimestamp ? `${formatTimestamp(brokerFillsTimestamp)} • ${formatRelativeAge(brokerFillsTimestamp)}` : "Unavailable"}
                    </span>
                    <span>
                      {ordersFillsFreshnessState === "SNAPSHOT"
                        ? "Snapshot-only broker order history. Do not treat this block as a live order monitor."
                        : ordersFillsFreshnessState === "STALE"
                          ? "Broker orders/fills are behind the live feed. Use the timestamps here before trusting the state."
                          : brokerPollingActive
                            ? `Broker order lifecycle is refreshing on the ${POSITIONS_PAGE_BROKER_REFRESH_SECONDS}s broker poll cadence.`
                            : "Broker order lifecycle is live-capable, but this page is not currently driving the active poll loop."}
                    </span>
                  </div>
                </div>
                {brokerOrdersOrFillsAvailable ? (
                  <>
                    <div className="split-panel">
                      <div>
                        <h3 className="subsection-title">Open Orders</h3>
                        <DataTable
                          columns={[
                            { key: "broker_order_id", label: "Broker Order ID" },
                            { key: "symbol", label: "Symbol" },
                            { key: "instruction", label: "Side / Instruction" },
                            { key: "quantity", label: "Qty" },
                            { key: "order_type", label: "Type" },
                            { key: "status", label: "Status" },
                            { key: "updated_at", label: "Updated", render: (row) => formatTimestamp(row.updated_at) },
                            { key: "limit_price", label: "Limit" },
                            { key: "stop_price", label: "Stop" },
                          ]}
                          rows={productionOpenOrders}
                          emptyLabel="No persisted open broker orders are currently available."
                        />
                      </div>
                      <div>
                        <h3 className="subsection-title">Recent Fills</h3>
                        <DataTable
                          columns={[
                            { key: "broker_order_id", label: "Broker Order ID" },
                            { key: "symbol", label: "Symbol" },
                            { key: "instruction", label: "Instruction" },
                            { key: "filled_quantity", label: "Filled" },
                            { key: "status", label: "Status" },
                            { key: "updated_at", label: "Updated", render: (row) => formatTimestamp(row.updated_at) },
                          ]}
                          rows={productionRecentFills}
                          emptyLabel="No persisted broker fills are currently available."
                        />
                      </div>
                    </div>
                    <DataTable
                      columns={[
                        { key: "occurred_at", label: "Time", render: (row) => formatTimestamp(row.occurred_at) },
                        { key: "event_type", label: "Event Type" },
                        { key: "broker_order_id", label: "Broker Order ID" },
                        { key: "status", label: "Status" },
                        { key: "message", label: "Message" },
                        { key: "source", label: "Source" },
                      ]}
                      rows={productionEvents}
                      emptyLabel="No persisted broker order events are available yet."
                    />
                  </>
                ) : (
                  <div className="placeholder-note">No persisted broker orders, fills, or order events are currently available in this source path.</div>
                )}
              </Section>
            </>
          ) : null}

          {!loading && page === "market" ? (
            <>
              <Section
                title="Trade Entry Workspace"
                subtitle="Manual routing, current symbol truth, and explicit live-vs-paper account context"
                className="trade-entry-section"
                headerClassName="section-header-tight"
              >
                {selectedWorkspaceRow ? (
                  <div className="local-workspace-strip">
                    <div className="local-workspace-copy">
                      <div className="local-workspace-kicker">Selected Strategy Context</div>
                      <div className="local-workspace-title">{compactBranchLabel(selectedWorkspaceRow)}</div>
                      <div className="local-workspace-meta">
                        <Badge label={laneClassLabel(selectedWorkspaceRow)} tone={paperStrategyClassTone(selectedWorkspaceRow)} />
                        <Badge label={selectedWorkspaceDesignation} tone={selectedWorkspaceRow?.benchmark_designation ? "warn" : selectedWorkspaceRow?.candidate_designation ? "good" : "muted"} />
                        <Badge label={runtimeAttachmentLabel(selectedWorkspaceRow)} tone={runtimeAttachmentLabel(selectedWorkspaceRow) === "Attached Live" ? "good" : runtimeAttachmentLabel(selectedWorkspaceRow) === "Audit Only" ? "warn" : "muted"} />
                        <span>{selectedWorkspaceInstrument || "No instrument"}</span>
                        <span>Exec {formatValue(selectedWorkspaceRow?.execution_timeframe ?? "1m")}</span>
                        <span>Context {selectedWorkspaceContextTimes}</span>
                        <span>Last eval {formatTimestamp(selectedWorkspaceRow?.last_execution_bar_evaluated_at)}</span>
                      </div>
                    </div>
                    <div className="local-workspace-actions">
                      <button className="panel-button subtle" onClick={() => selectWorkspaceLane(String(selectedWorkspaceRow?.lane_id ?? ""), { navigateTo: "strategies" })}>
                        Open Deep-Dive
                      </button>
                      <button
                        className="panel-button subtle"
                        onClick={() => {
                          window.location.hash = "#/diagnostics";
                          setPage("diagnostics");
                        }}
                      >
                        Open Evidence
                      </button>
                    </div>
                  </div>
                ) : null}
                <div className="trade-entry-layout">
                  <div className="trade-entry-ticket-shell">
                    <div className="subsection-title">Order Ticket</div>
                    <div className="ticket-grid trade-entry-ticket-grid">
                      <label className="settings-field">
                        <span>Account</span>
                        <select
                          disabled={busyAction !== null}
                          value={manualOrderForm.accountHash}
                          onChange={(event) => setManualOrderForm((current) => ({ ...current, accountHash: event.target.value }))}
                        >
                          {productionAccounts.map((row) => (
                            <option key={String(row.account_hash)} value={String(row.account_hash)}>
                              {maskAccountNumber(row.account_number)} / {formatValue(row.account_type)}
                            </option>
                          ))}
                        </select>
                      </label>
                      <label className="settings-field">
                        <span>Symbol</span>
                        <input disabled={busyAction !== null} value={manualOrderForm.symbol} onChange={(event) => setManualOrderForm((current) => ({ ...current, symbol: event.target.value.toUpperCase() }))} />
                      </label>
                      <label className="settings-field">
                        <span>Intent / Tag</span>
                        <select disabled={busyAction !== null} value={manualOrderForm.intentType} onChange={(event) => setManualOrderForm((current) => ({ ...current, intentType: event.target.value }))}>
                          {["MANUAL_LIVE_PILOT", "FLATTEN", "ENTRY", "EXIT", "ADJUSTMENT", "MANUAL"].map((intentType) => (
                            <option key={intentType} value={intentType}>
                              {manualIntentTypeLabel(intentType)}
                            </option>
                          ))}
                        </select>
                      </label>
                      <label className="settings-field">
                        <span>Side</span>
                        <select disabled={busyAction !== null} value={manualOrderForm.side} onChange={(event) => setManualOrderForm((current) => ({ ...current, side: event.target.value }))}>
                          {manualSideOptions.map((side) => (
                            <option key={side} value={side}>
                              {sentenceCase(side.replace(/_/g, " ").toLowerCase())}
                            </option>
                          ))}
                        </select>
                      </label>
                      <label className="settings-field">
                        <span>Quantity</span>
                        <input disabled={busyAction !== null} value={manualOrderForm.quantity} onChange={(event) => setManualOrderForm((current) => ({ ...current, quantity: event.target.value }))} />
                      </label>
                      <label className="settings-field">
                        <span>Order Type</span>
                        <select disabled={busyAction !== null} value={manualOrderForm.orderType} onChange={(event) => setManualOrderForm((current) => ({ ...current, orderType: event.target.value }))}>
                          {manualOrderTypes.map((orderType) => (
                            <option key={orderType} value={orderType}>
                              {sentenceCase(orderType.replace(/_/g, "-").toLowerCase())}
                            </option>
                          ))}
                        </select>
                      </label>
                      {["LIMIT", "STOP_LIMIT", "LIMIT_ON_CLOSE"].includes(manualOrderForm.orderType) ? (
                        <label className="settings-field">
                          <span>Limit Price</span>
                          <input disabled={busyAction !== null} value={manualOrderForm.limitPrice} onChange={(event) => setManualOrderForm((current) => ({ ...current, limitPrice: event.target.value }))} />
                        </label>
                      ) : null}
                      {["STOP", "STOP_LIMIT"].includes(manualOrderForm.orderType) ? (
                        <label className="settings-field">
                          <span>Stop Price</span>
                          <input disabled={busyAction !== null} value={manualOrderForm.stopPrice} onChange={(event) => setManualOrderForm((current) => ({ ...current, stopPrice: event.target.value }))} />
                        </label>
                      ) : null}
                      <label className="settings-field">
                        <span>Time In Force</span>
                        <select disabled={busyAction !== null} value={manualOrderForm.timeInForce} onChange={(event) => setManualOrderForm((current) => ({ ...current, timeInForce: event.target.value }))}>
                          {manualTimeInForceOptions.map((timeInForce) => (
                            <option key={timeInForce} value={timeInForce}>
                              {timeInForce}
                            </option>
                          ))}
                        </select>
                      </label>
                      <label className="settings-field">
                        <span>Session</span>
                        <select disabled={busyAction !== null} value={manualOrderForm.session} onChange={(event) => setManualOrderForm((current) => ({ ...current, session: event.target.value }))}>
                          {manualSessionOptions.map((session) => (
                            <option key={session} value={session}>
                              {session}
                            </option>
                          ))}
                        </select>
                      </label>
                      <label className="settings-field workflow-field-wide">
                        <span>Operator Note</span>
                        <input disabled={busyAction !== null} value={manualOrderForm.operatorNote} onChange={(event) => setManualOrderForm((current) => ({ ...current, operatorNote: event.target.value }))} />
                      </label>
                    </div>
                    <label className="settings-toggle trade-entry-confirmation">
                      <input
                        type="checkbox"
                        checked={manualOrderForm.reviewConfirmed}
                        disabled={busyAction !== null}
                        onChange={(event) => setManualOrderForm((current) => ({ ...current, reviewConfirmed: event.target.checked }))}
                      />
                      <span>Review confirmed. Do not send without this explicit confirmation.</span>
                    </label>
                    <div className="action-row inline trade-entry-primary-actions">
                      <ControlButton
                        label="Build Dry-Run Payload"
                        onClick={() =>
                          void runCommand(
                            "preview-broker-order",
                            () => api.runProductionLinkAction("preview-order", buildManualOrderPayload()),
                            {
                              requiresLive: true,
                              confirmMessage: `Build a dry-run payload preview for ${manualOrderForm.orderType} ${manualOrderForm.side} ${manualOrderForm.quantity} ${manualOrderForm.symbol || "SYMBOL"}?`,
                            },
                          )
                        }
                        busyAction={busyAction}
                        disabled={manualPreviewBlockers.length > 0}
                      />
                      <ControlButton
                        label="Review / Confirm / Send"
                        onClick={() =>
                          void runCommand(
                            "submit-broker-order",
                            () => api.runProductionLinkAction("submit-order", buildManualOrderPayload()),
                            {
                              requiresLive: true,
                              confirmMessage: `Final live order confirmation: ${manualOrderForm.orderType} ${manualOrderForm.side} ${manualOrderForm.quantity} ${manualOrderForm.symbol || "SYMBOL"} on ${maskAccountNumber(selectedManualAccount?.account_number)}.`,
                            },
                          )
                        }
                        busyAction={busyAction}
                        disabled={!productionManualSubmitEnabled || manualOrderGateBlockers.length > 0}
                      />
                    </div>
                    {manualPreviewBlockers.length || manualOrderGateBlockers.length ? (
                      <details className="secondary-evidence-shell ticket-gate-shell">
                        <summary className="secondary-evidence-summary">
                          <span className="secondary-evidence-title">Ticket Gate Details</span>
                          <span className="secondary-evidence-note">
                            {Array.from(new Set([...manualPreviewBlockers, ...manualOrderGateBlockers])).length} active guardrails
                          </span>
                        </summary>
                        <div className="section-card secondary-evidence-card">
                          <div className="notice-strip compact">
                            {Array.from(new Set([...manualPreviewBlockers, ...manualOrderGateBlockers])).map((item) => (
                              <div key={item}>{item}</div>
                            ))}
                          </div>
                        </div>
                      </details>
                    ) : null}
                  </div>
                  <div className="trade-entry-market-shell">
                    <div className="trade-entry-quote-shell">
                      <div className="subsection-title">Quote Box</div>
                      <div className="metric-grid trade-entry-quote-grid">
                      <MetricCard label="Symbol" value={tradeEntrySymbol || "None selected"} />
                      <MetricCard label="Last" value={formatValue(tradeEntryQuoteRow?.last_price ?? tradeEntryQuoteRow?.mark ?? tradeEntryQuoteRow?.price ?? "Unavailable")} />
                      <MetricCard label="Bid" value={formatValue(tradeEntryQuoteRow?.bid_price ?? tradeEntryQuoteRow?.bid ?? "Unavailable")} />
                      <MetricCard label="Ask" value={formatValue(tradeEntryQuoteRow?.ask_price ?? tradeEntryQuoteRow?.ask ?? "Unavailable")} />
                      <MetricCard label="Net Change" value={formatValue(tradeEntryQuoteRow?.net_change ?? tradeEntryQuoteRow?.quoteTrend ?? "Unavailable")} tone={statusTone(tradeEntryQuoteRow?.net_change)} />
                      <MetricCard label="Quote Timestamp" value={formatTimestamp(tradeEntryQuoteRow?.updated_at ?? productionQuotes.updated_at)} />
                    </div>
                    </div>
                    <div className="notice-strip compact trade-entry-account-note">
                      <div>Live account: {selectedProductionAccount ? `${maskAccountNumber(selectedProductionAccount.account_number)} / ${formatValue(selectedProductionAccount.account_type)}` : "No live account selected"}.</div>
                      <div>Paper account: shared paper runtime truth stays separate from broker routing and remains strategy-attributed.</div>
                    </div>
                    <div className="trade-entry-position-shell">
                      <div className="subsection-title">Current Position & Attribution</div>
                      <div className="metric-grid trade-entry-position-grid">
                        <MetricCard label="Broker Position" value={selectedProductionPosition ? `${formatValue(selectedProductionPosition.side)} ${formatValue(selectedProductionPosition.quantity)} ${formatValue(selectedProductionPosition.symbol)}` : "Flat / None selected"} tone={selectedProductionPosition ? "warn" : "good"} />
                        <MetricCard label="Paper Strategy Rows" value={formatShortNumber(tradeEntryPaperRows.length)} tone={tradeEntryPaperRows.length ? "good" : "muted"} />
                        <MetricCard label="Strategy Tag" value={manualIntentTypeLabel(manualOrderForm.intentType)} />
                        <MetricCard label="Participation" value={formatValue(tradeEntryPaperRows[0]?.participation_policy ?? "Unavailable")} />
                        <MetricCard label="Net Side" value={formatValue(tradeEntryPaperRows[0]?.net_side ?? tradeEntryPaperRows[0]?.position_side ?? "FLAT")} />
                        <MetricCard label="Can Add More" value={formatValue(tradeEntryPaperRows[0]?.additional_entry_allowed ?? tradeEntryPaperRows[0]?.can_add_more ?? false)} tone={(tradeEntryPaperRows[0]?.additional_entry_allowed ?? tradeEntryPaperRows[0]?.can_add_more) ? "good" : "muted"} />
                      </div>
                      <DataTable
                        columns={[
                          { key: "strategy_name", label: "Strategy" },
                          { key: "paper_strategy_class", label: "Class", render: (row) => <Badge label={paperStrategyClassLabel(row)} tone={paperStrategyClassTone(row)} /> },
                          { key: "current_strategy_status", label: "Status", render: (row) => formatValue(row.current_strategy_status ?? row.status) },
                          { key: "latest_activity_timestamp", label: "Last Activity", render: (row) => formatTimestamp(row.latest_activity_timestamp) },
                        ]}
                        rows={tradeEntryPaperRows.slice(0, 4)}
                        emptyLabel="No paper strategy attribution rows are attached to the selected symbol."
                      />
                    </div>
                    <div className="trade-entry-fills-shell">
                      <h3 className="subsection-title">Recent Fills</h3>
                      <DataTable
                        columns={[
                          { key: "broker_order_id", label: "Order ID" },
                          { key: "symbol", label: "Symbol" },
                          { key: "instruction", label: "Instruction" },
                          { key: "filled_quantity", label: "Filled" },
                          { key: "updated_at", label: "Updated", render: (row) => formatTimestamp(row.updated_at) },
                        ]}
                        rows={productionRecentFills.filter((row) => !tradeEntrySymbol || String(row.symbol ?? "").trim().toUpperCase() === tradeEntrySymbol).slice(0, 6)}
                        emptyLabel="No recent fills match the current symbol filter."
                      />
                    </div>
                  </div>
                </div>
                <div className="split-panel pnl-table-grid">
                  <div className="table-panel-shell">
                    <h3 className="subsection-title">Working Orders</h3>
                    <DataTable
                      columns={[
                        { key: "broker_order_id", label: "Order ID" },
                        { key: "symbol", label: "Symbol" },
                        { key: "instruction", label: "Instruction" },
                        { key: "quantity", label: "Qty" },
                        { key: "status", label: "Status" },
                        { key: "updated_at", label: "Updated", render: (row) => formatTimestamp(row.updated_at) },
                      ]}
                      rows={productionOpenOrders.filter((row) => !tradeEntrySymbol || String(row.symbol ?? "").trim().toUpperCase() === tradeEntrySymbol).slice(0, 8)}
                      emptyLabel="No working broker orders match the current symbol filter."
                    />
                  </div>
                  <div className="table-panel-shell trade-entry-working-summary">
                    <div className="subsection-title">Symbol Activity</div>
                    <div className="metric-grid compact trade-entry-activity-grid">
                      <MetricCard label="Recent Fills" value={formatShortNumber(productionRecentFills.filter((row) => !tradeEntrySymbol || String(row.symbol ?? "").trim().toUpperCase() === tradeEntrySymbol).length)} tone={productionRecentFills.length ? "good" : "muted"} />
                      <MetricCard label="Working Orders" value={formatShortNumber(productionOpenOrders.filter((row) => !tradeEntrySymbol || String(row.symbol ?? "").trim().toUpperCase() === tradeEntrySymbol).length)} tone={productionOpenOrders.length ? "warn" : "muted"} />
                      <MetricCard label="Quote Updated" value={formatTimestamp(tradeEntryQuoteRow?.updated_at ?? productionQuotes.updated_at)} />
                      <MetricCard label="Broker Position" value={selectedProductionPosition ? "OPEN" : "FLAT"} tone={selectedProductionPosition ? "warn" : "good"} />
                    </div>
                  </div>
                </div>
              </Section>
            </>
          ) : null}

          {!loading && page === "replay" ? (
            <>
              <Section title="Operator Results Board" subtitle="Operator-first results workspace with separate results, comparison, study, runtime, and diagnostics views">
                <div className="badge-row">
                  <Badge label="RESULTS FIRST" tone="good" />
                  <Badge label="PROVENANCE EXPLICIT" tone="muted" />
                  <Badge label="REPLAY + PAPER + RESEARCH" tone="muted" />
                </div>
                {strategyAnalysis.available === true ? (
                  <UnifiedStrategyAnalysis
                    analysis={strategyAnalysis}
                    replayStudyItems={playbackLatestStudyItems}
                    studyPanel={(
                      <>
                        <div className="badge-row">
                          <Badge label="REPLAY STUDY" tone={playbackStudyAvailable ? "good" : "muted"} />
                          <Badge label={playbackStudyMode} tone={playbackStudyModeToneValue} />
                          <Badge label={playbackStudyArtifactFound ? "STUDY AVAILABLE" : "STUDY NOT AVAILABLE"} tone={playbackStudyArtifactFound ? "good" : "warn"} />
                        </div>
                        <div className="notice-strip compact">
                          Chart and study detail live here so the default landing board stays focused on ranked results.
                        </div>
                        <div className="metric-grid strategy-study-status-grid compact">
                          <MetricCard label="Study Available" value={playbackStudyArtifactFound ? "YES" : "NO"} tone={playbackStudyArtifactFound ? "good" : "warn"} />
                          <MetricCard label="Run Loaded" value={playbackStudyRunLoaded ? "YES" : "NO"} tone={playbackStudyRunLoaded ? "good" : "muted"} />
                          <MetricCard label="Artifact Timeframe" value={playbackStudyBaseTimeframe} />
                          <MetricCard label="Structural Timeframe" value={playbackStudyStructuralTimeframe} />
                          <MetricCard label="Execution Timeframe" value={playbackStudyExecutionTimeframe} />
                          <MetricCard label="Truth Mode" value={playbackStudyTruthMode} />
                          <MetricCard label="Timing Detail" value={playbackStudyTimingLabelValue} tone={statusTone(playbackStudyTimingLabelValue)} />
                          <MetricCard label="View Mode" value={playbackStudyMode} tone={playbackStudyModeToneValue} />
                        </div>
                        {playbackStudyAvailable ? (
                          <>
                            <ReplayStrategyStudy studies={playbackLatestStudyItems.length ? playbackLatestStudyItems : [{ study_key: "latest", label: "Latest replay study", study: playbackStudy }]} />
                            <div className="notice-strip compact">
                              {formatValue(
                                playbackStudySummary.pnl_unavailable_reason ??
                                  "Lower-panel pricing is derived from persisted fills, current position state, and replay bar closes when point-value truth is available.",
                              )}
                            </div>
                          </>
                        ) : (
                          <div className="notice-strip strategy-study-empty">
                            <div>Study detail is replay-only and needs strategy-study artifacts from a completed playback run.</div>
                            <div>Required artifact: the latest run must publish `*.strategy_study.json` and `*.strategy_study.md` for the loaded replay.</div>
                            <div>Until then, results ranking remains available and Diagnostics still exposes trigger-validation detail.</div>
                          </div>
                        )}
                      </>
                    )}
                    runtimePanel={(
                      <>
                        <div className="notice-strip compact">
                          <div><strong>Attachment:</strong> {desktopState?.backendUrl ? "Desktop attached to backend/API." : "Desktop is not attached to the backend/API yet."}</div>
                          <div><strong>Auth Gate:</strong> {formatValue(global.auth_label)}</div>
                          <div><strong>Paper Soak State:</strong> {formatValue(paperStartupStateLabel)}</div>
                        </div>
                        <PaperStartupPanel
                          metrics={paperStartupMetrics}
                          stateLabel={paperStartupStateLabel}
                          reason={paperStartupReasonText}
                          actionLabel={paperStartupActionLabel}
                          actionDescription={paperStartupActionDescription}
                          busyAction={busyAction}
                          canRunLiveActions={canRunLiveActions}
                          onStartDashboard={() => void runCommand("start-dashboard", () => api.startDashboard())}
                          onStartPaper={() => void runCommand("start-paper", () => api.runDashboardAction("start-paper"), { requiresLive: true })}
                          onRestartPaperWithTempPaper={() =>
                            void runCommand("restart-paper-with-temp-paper", () => api.runDashboardAction("restart-paper-with-temp-paper"), {
                              confirmMessage: "Restart Runtime + Temp Paper will restart the paper soak with all enabled temporary paper lanes included. Proceed?",
                              requiresLive: true,
                            })
                          }
                          onAuthGateCheck={() => void runCommand("auth-gate-check", () => api.runDashboardAction("auth-gate-check"), { requiresLive: true })}
                          onCompletePreSessionReview={() =>
                            void runCommand("complete-pre-session-review", () => api.runDashboardAction("complete-pre-session-review"), { requiresLive: true })
                          }
                        />
                        <div className="control-grid">
                          <ControlButton
                            label="Start Runtime"
                            onClick={() => void runCommand("start-paper", () => api.runDashboardAction("start-paper"), { requiresLive: true })}
                            busyAction={busyAction}
                            disabled={!canRunLiveActions}
                          />
                          <ControlButton
                            label="Restart Runtime + Temp Paper"
                            onClick={() =>
                              void runCommand("restart-paper-with-temp-paper", () => api.runDashboardAction("restart-paper-with-temp-paper"), {
                                confirmMessage: "Restart Runtime + Temp Paper will restart the paper soak with all enabled temporary paper lanes included. Proceed?",
                                requiresLive: true,
                              })
                            }
                            busyAction={busyAction}
                            disabled={!canRunLiveActions}
                          />
                          <ControlButton
                            label="Stop Runtime"
                            onClick={() =>
                              void runCommand("stop-paper", () => api.runDashboardAction("stop-paper"), {
                                confirmMessage: "Stop Runtime will stop the current paper runtime. Proceed?",
                                requiresLive: true,
                              })
                            }
                            busyAction={busyAction}
                            disabled={!canRunLiveActions}
                            danger
                          />
                          <ControlButton
                            label="Auth Gate Check"
                            onClick={() => void runCommand("auth-gate-check", () => api.runDashboardAction("auth-gate-check"), { requiresLive: true })}
                            busyAction={busyAction}
                            disabled={!canRunLiveActions}
                          />
                        </div>
                      </>
                    )}
                    diagnosticsPanel={(
                      <>
                        <div className="badge-row">
                          <Badge label="TRIGGER VALIDATION" tone="muted" />
                          <Badge label={asArray<JsonRecord>(playbackLatestRun.rows).length ? `${formatShortNumber(asArray<JsonRecord>(playbackLatestRun.rows).length)} CHECKS` : "NO CHECKS"} tone={asArray<JsonRecord>(playbackLatestRun.rows).length ? "good" : "muted"} />
                        </div>
                        <div className="notice-strip compact">
                          Trigger validation and other low-level diagnostics stay here so raw blocker counts do not dominate the operator landing board.
                        </div>
                        <DataTable
                          columns={[
                            { key: "symbol", label: "Symbol" },
                            { key: "lane_family", label: "Lane / Family" },
                            { key: "bars_processed", label: "Bars" },
                            { key: "signals_seen", label: "Signals" },
                            { key: "intents_created", label: "Intents" },
                            { key: "fills_created", label: "Fills" },
                            { key: "first_trigger_timestamp", label: "First Signal", render: (row) => formatTimestamp(row.first_trigger_timestamp) },
                            { key: "first_intent_timestamp", label: "First Intent", render: (row) => formatTimestamp(row.first_intent_timestamp) },
                            { key: "first_fill_timestamp", label: "First Fill", render: (row) => formatTimestamp(row.first_fill_timestamp) },
                            { key: "result_status", label: "Result" },
                            { key: "block_or_fault_reason", label: "Block Reason" },
                          ]}
                          rows={asArray<JsonRecord>(playbackLatestRun.rows)}
                          emptyLabel="No trigger-validation rows are available in the latest playback snapshot."
                        />
                      </>
                    )}
                  />
                ) : (
                  <div className="notice-strip strategy-study-empty">
                    <div>No unified strategy analysis rows are available yet.</div>
                    <div>This surface activates after the dashboard loads replay studies and/or paper strategy performance truth.</div>
                  </div>
                )}
              </Section>

              <Section title="Historical Playback Context" subtitle="Latest managed playback run and replay-level context for the operator board">
                <div className="badge-row">
                  <Badge label="REPLAY" tone={truthBadgeTone("REPLAY")} />
                  <Badge label="BACKTEST" tone={truthBadgeTone("REPLAY")} />
                </div>
                <div className="metric-grid">
                  <MetricCard label="Available" value={formatValue(playback.available)} tone={statusTone(playback.available ? "available" : "unavailable")} />
                  <MetricCard label="Run Stamp" value={formatValue(playbackLatestRun.run_stamp)} />
                  <MetricCard label="Run Timestamp" value={formatTimestamp(playbackLatestRun.run_timestamp)} />
                  <MetricCard label="Bars Processed" value={formatShortNumber(playbackLatestRun.bars_processed)} />
                  <MetricCard label="Signals Seen" value={formatShortNumber(playbackLatestRun.signals_seen)} />
                  <MetricCard label="Intents Created" value={formatShortNumber(playbackLatestRun.intents_created)} />
                  <MetricCard label="Fills Created" value={formatShortNumber(playbackLatestRun.fills_created)} />
                  <MetricCard label="Symbols" value={formatValue(playbackLatestRun.symbols)} />
                </div>
                <div className="action-row inline">
                  <button className="panel-button subtle" onClick={() => void runCommand("open-playback-summary", () => openArtifact(api, desktopState, playbackArtifacts.summary))}>
                    Export Replay Output
                  </button>
                  <button className="panel-button subtle" onClick={() => void runCommand("open-trigger-report", () => openArtifact(api, desktopState, playbackArtifacts.trigger_report_markdown ?? playbackArtifacts.trigger_report_json))}>
                    Export Trigger Report
                  </button>
                </div>
                <div className="notice-strip strategy-study-entry">
                  <div className="strategy-study-entry-head">
                    <div>
                      <div className="strategy-study-entry-title">Replay Strategy Study</div>
                      <div className="strategy-study-entry-copy">
                        Available after a replay/historical playback run with strategy-study artifacts.
                      </div>
                    </div>
                    <div className="badge-row strategy-study-entry-badges">
                      <Badge label="REPLAY ONLY" tone="muted" />
                      <Badge label={playbackStudyMode} tone={playbackStudyModeToneValue} />
                      <Badge label={playbackStudyArtifactFound ? "STUDY AVAILABLE" : "STUDY NOT AVAILABLE"} tone={playbackStudyArtifactFound ? "good" : "warn"} />
                    </div>
                  </div>
                  <div className="metric-grid strategy-study-status-grid">
                    <MetricCard label="Run Loaded" value={playbackStudyRunLoaded ? "YES" : "NO"} tone={playbackStudyRunLoaded ? "good" : "muted"} />
                    <MetricCard label="Study Available" value={playbackStudyArtifactFound ? "YES" : "NO"} tone={playbackStudyArtifactFound ? "good" : "warn"} />
                    <MetricCard label="Base Timeframe" value={playbackStudyBaseTimeframe} />
                    <MetricCard label="Timing Detail" value={playbackStudyTimingLabelValue} tone={statusTone(playbackStudyTimingLabelValue)} />
                    <MetricCard label="View Mode" value={playbackStudyMode} tone={playbackStudyModeToneValue} />
                  </div>
                  <div className="action-row">
                    <button
                      className="panel-button subtle"
                      disabled={!playbackArtifacts.strategy_study_json}
                      onClick={() => void runCommand("open-strategy-study-json", () => openArtifact(api, desktopState, playbackArtifacts.strategy_study_json))}
                    >
                      Export Study JSON
                    </button>
                    <button
                      className="panel-button subtle"
                      disabled={!playbackArtifacts.strategy_study_json && !playbackArtifacts.strategy_study_markdown}
                      onClick={() => void runCommand("open-strategy-study-md", () => openArtifact(api, desktopState, playbackArtifacts.strategy_study_markdown ?? playbackArtifacts.strategy_study_json))}
                    >
                      Export Study Summary
                    </button>
                  </div>
                </div>
                {playbackReplaySummaryAvailable ? (
                  <>
                    <div className="notice-strip">
                      <div>This latest replay artifact includes the standalone multi-strategy replay summary contract.</div>
                      <div>Included standalone strategies: {formatValue(asArray<string>(playbackAggregateSummary.standalone_strategy_ids).join(", ") || "None")}</div>
                      <div>{formatValue(playbackAggregateSummary.pnl_unavailable_reason ?? "Aggregate priced replay P&L is available for this run.")}</div>
                    </div>
                    <div className="metric-grid">
                      <MetricCard label="Standalone Strategies" value={formatShortNumber(playbackAggregateSummary.standalone_strategy_count)} />
                      <MetricCard label="Order Intents" value={formatShortNumber(playbackAggregateSummary.order_intents)} />
                      <MetricCard label="Fills" value={formatShortNumber(playbackAggregateSummary.fills)} />
                      <MetricCard label="Entries" value={formatShortNumber(playbackAggregateSummary.entries)} />
                      <MetricCard label="Exits" value={formatShortNumber(playbackAggregateSummary.exits)} />
                      <MetricCard label="Realized P&L" value={formatMaybePnL(playbackAggregateSummary.realized_pnl)} />
                      <MetricCard label="Unrealized P&L" value={formatMaybePnL(playbackAggregateSummary.unrealized_pnl)} />
                      <MetricCard label="Cumulative P&L" value={formatMaybePnL(playbackAggregateSummary.cumulative_pnl)} />
                    </div>
                    <DataTable
                      columns={[
                        { key: "standalone_strategy_id", label: "Standalone Strategy", render: (row) => standaloneStrategyLabel(row) },
                        { key: "strategy_family", label: "Family" },
                        { key: "instrument", label: "Instrument" },
                        { key: "processed_bars", label: "Processed Bars" },
                        { key: "order_intents", label: "Order Intents" },
                        { key: "fills", label: "Fills" },
                        { key: "entries", label: "Entries" },
                        { key: "exits", label: "Exits" },
                        { key: "final_position_side", label: "Final Position" },
                        { key: "final_strategy_status", label: "Final Status" },
                        { key: "realized_pnl", label: "Realized P&L", render: (row) => formatMaybePnL(row.realized_pnl) },
                        { key: "pnl_unavailable_reason", label: "P&L Note", render: (row) => formatValue(row.pnl_unavailable_reason ?? "Exact / fully priced") },
                      ]}
                      rows={playbackPerStrategySummaries}
                      emptyLabel="No per-strategy replay summaries were published for the latest playback run."
                    />
                  </>
                ) : (
                  <div className="notice-strip">
                    <div>The latest playback artifact does not include the standalone replay summary contract yet.</div>
                    <div>Diagnostics still exposes trigger-validation rows, but replay per-strategy and aggregate portfolio summaries are unavailable in this artifact.</div>
                  </div>
                )}
              </Section>
            </>
          ) : null}

          {!loading && page === "logs" ? (
            <>
              <Section title="Runtime Events" subtitle="Recent dashboard actions and surfaced runtime events">
                <DataTable
                  columns={[
                    { key: "timestamp", label: "Time", render: (row) => formatTimestamp(row.timestamp) },
                    { key: "action_label", label: "Event" },
                    { key: "ok", label: "Status", render: (row) => (row.ok ? "OK" : "Failed") },
                    { key: "output", label: "Output", render: (row) => formatValue(row.output ?? row.message) },
                  ]}
                  rows={actionLog}
                  emptyLabel="No recent runtime events were found."
                />
              </Section>

              <Section title="Local Operator Auth Events" subtitle="Touch ID auth results, sensitive-action authorization, and denied/canceled local operator events">
                <div className="metric-grid">
                  <MetricCard label="Latest Event" value={formatValue(latestLocalAuthEvent.event_type ?? "None")} tone={statusTone(latestLocalAuthEvent.event_type)} />
                  <MetricCard label="Latest Event At" value={formatTimestamp(latestLocalAuthEvent.occurred_at)} />
                  <MetricCard label="Session Active" value={formatValue(localOperatorAuth.auth_session_active ? "ACTIVE" : "INACTIVE")} tone={localOperatorAuth.auth_session_active ? "good" : "warn"} />
                  <MetricCard label="Identity" value={formatValue(localOperatorAuth.local_operator_identity ?? "Unauthenticated")} tone={localOperatorAuth.auth_session_active ? "good" : "muted"} />
                </div>
                <DataTable
                  columns={[
                    { key: "occurred_at", label: "Occurred", render: (row) => formatTimestamp(row.occurred_at) },
                    { key: "event_type", label: "Event Type" },
                    { key: "action", label: "Action", render: (row) => formatValue(row.action) },
                    { key: "instrument", label: "Instrument", render: (row) => formatValue(row.instrument) },
                    { key: "local_operator_identity", label: "Local Operator", render: (row) => formatValue(row.local_operator_identity) },
                    { key: "auth_method", label: "Method", render: (row) => formatValue(row.auth_method ?? "NONE") },
                    { key: "trigger", label: "Trigger", render: (row) => (row.automatic === true ? "Automatic" : "Operator") },
                    { key: "note", label: "Reason / Note", render: (row) => formatValue(row.note) },
                  ]}
                  rows={localAuthEvents}
                  emptyLabel="No local operator auth events are available yet."
                />
              </Section>

              <Section title="Signals / Intents / Fills" subtitle="Latest paper-run event surfaces">
                <div className="split-panel">
                  <div>
                    <h3 className="subsection-title">Latest Intents</h3>
                    <JsonBlock value={paper.latest_intents ?? []} />
                  </div>
                  <div>
                    <h3 className="subsection-title">Latest Fills</h3>
                    <JsonBlock value={paper.latest_fills ?? []} />
                  </div>
                </div>
              </Section>
            </>
          ) : null}

          {!loading && page === "configuration" ? (
            <>
              <Section title="Runtime Config" subtitle="Read-only first pass">
                <div className="metric-grid">
                  <MetricCard label="Mode" value={formatValue(global.mode_label)} />
                  <MetricCard label="Entries Enabled" value={formatValue(global.entries_enabled ?? paperReadiness.entries_enabled)} />
                  <MetricCard label="Artifacts Path" value={formatValue("/outputs/operator_dashboard")} />
                  <MetricCard label="Dashboard URL" value={formatValue(desktopState?.backendUrl)} />
                </div>
              </Section>

              <Section title="Broker / Auth" subtitle="Auth state and refresh posture">
                <div className="metric-grid">
                  <MetricCard label="Auth Label" value={formatValue(global.auth_label)} tone={statusTone(global.auth_label)} />
                  <MetricCard label="Runtime Health" value={formatValue(global.runtime_health_label)} tone={statusTone(global.runtime_health_label)} />
                  <MetricCard label="Latest Check" value={formatTimestamp(actionLog[0]?.timestamp)} />
                </div>
              </Section>

              <Section title="Production Link" subtitle="Schwab production connectivity, account selection, and broker source-of-record boundary">
                <div className="metric-grid">
                  <MetricCard label="Production Link" value={formatValue(productionLink.label ?? productionLink.status)} tone={statusTone(productionLink.label ?? productionLink.status)} />
                  <MetricCard label="Selected Account" value={selectedProductionAccount ? `${maskAccountNumber(selectedProductionAccount.account_number)} / ${formatValue(selectedProductionAccount.account_type)}` : "None"} />
                  <MetricCard label="API Base URL" value={formatValue(productionDiagnostics.trader_api_base_url)} />
                  <MetricCard label="Broker DB" value={formatValue(productionDiagnostics.database_path)} />
                  <MetricCard label="Auth" value={formatValue(asRecord(productionLink.auth).label)} tone={statusTone(asRecord(productionLink.auth).label)} />
                  <MetricCard label="Last Live Fetch" value={formatTimestamp(productionDiagnostics.last_live_fetch_at)} />
                  <MetricCard label="Broker Reachability" value={formatValue(asRecord(productionHealth.broker_reachable).label)} tone={statusTone(asRecord(productionHealth.broker_reachable).label)} />
                  <MetricCard label="Positions Fresh" value={formatValue(asRecord(productionHealth.positions_fresh).label)} tone={statusTone(asRecord(productionHealth.positions_fresh).label)} />
                  <MetricCard label="Orders Fresh" value={formatValue(asRecord(productionHealth.orders_fresh).label)} tone={statusTone(asRecord(productionHealth.orders_fresh).label)} />
                  <MetricCard label="Fills / Events" value={formatValue(asRecord(productionHealth.fills_events_fresh).label)} tone={statusTone(asRecord(productionHealth.fills_events_fresh).label)} />
                  <MetricCard label="Reconciliation Fresh" value={formatValue(asRecord(productionHealth.reconciliation_fresh).label)} tone={statusTone(asRecord(productionHealth.reconciliation_fresh).label)} />
                  <MetricCard label="Manual Submit Safety" value={productionFeatureFlags.live_order_submit_enabled === true ? "Explicitly Enabled" : "Disabled"} tone={statusTone(productionFeatureFlags.live_order_submit_enabled === true ? "ready" : "blocked")} />
                  <MetricCard label="Next Live Verification" value={formatValue(nextLiveVerificationStep.label ?? nextLiveVerificationStep.verification_key)} tone={statusTone(asRecord(nextLiveVerificationStep).blocked === true ? "warn" : "ready")} />
                  <MetricCard label="Advanced TIF UI" value={advancedTifTicketSupport ? "Dry-Run Enabled" : "Disabled"} tone={statusTone(advancedTifTicketSupport ? "ready" : "blocked")} />
                  <MetricCard label="OCO UI" value={ocoTicketSupport ? "Dry-Run Enabled" : "Disabled"} tone={statusTone(ocoTicketSupport ? "ready" : "blocked")} />
                  <MetricCard label="EXTO Live Submit" value={extExtoLiveSubmitEnabled ? "Enabled" : "Disabled"} tone={statusTone(extExtoLiveSubmitEnabled ? "ready" : "blocked")} />
                  <MetricCard label="OCO Live Submit" value={ocoLiveSubmitEnabled ? "Enabled" : "Disabled"} tone={statusTone(ocoLiveSubmitEnabled ? "ready" : "blocked")} />
                  <MetricCard label="Live-Verified Types" value={liveVerifiedOrderKeys.length ? liveVerifiedOrderKeys.join(", ") : "None"} />
                  <MetricCard label="Supported Asset Classes" value={manualAssetClasses.join(", ")} />
                  <MetricCard label="Supported Order Types" value={manualOrderTypes.join(", ")} />
                  <MetricCard label="Dry-Run Order Types" value={supportedManualDryRunOrderTypes.join(", ")} />
                  <MetricCard label="Supported TIF Values" value={manualTimeInForceOptions.join(", ")} />
                  <MetricCard label="Supported Sessions" value={manualSessionOptions.join(", ")} />
                  <MetricCard label="Replace Path" value={productionReplaceEnabled ? "Enabled" : "Disabled until live-verified"} tone={productionReplaceEnabled ? "good" : "warn"} />
                </div>
                {productionEndpointUncertainty.length ? (
                  <div className="notice-strip">
                    {productionEndpointUncertainty.map((item) => (
                      <div key={item}>{item}</div>
                    ))}
                  </div>
                ) : null}
                <div className="split-panel">
                  <div>
                    <h3 className="subsection-title">Live Verification Matrix</h3>
                    <JsonBlock
                      value={{
                        reviewable: orderTypeMatrixByAssetClass,
                        live_enabled: liveEnabledOrderTypesByAssetClass,
                        dry_run_only: dryRunOnlyOrderTypesByAssetClass,
                        verification_status: orderTypeLiveVerificationMatrix,
                      }}
                    />
                  </div>
                  <div>
                    <h3 className="subsection-title">Verification Sequence And Gates</h3>
                    <JsonBlock
                      value={{
                        live_verified_order_keys: liveVerifiedOrderKeys,
                        next_step: nextLiveVerificationStep,
                        sequence: orderTypeLiveVerificationSequence,
                        near_term_runbooks: nearTermLiveVerificationRunbooks,
                        stock_market_live_submit: productionCapabilities.stock_market_live_submit === true,
                        stock_limit_live_submit: productionCapabilities.stock_limit_live_submit === true,
                        stock_stop_live_submit: productionCapabilities.stock_stop_live_submit === true,
                        stock_stop_limit_live_submit: productionCapabilities.stock_stop_limit_live_submit === true,
                        advanced_tif_ui: advancedTifTicketSupport,
                        oco_ui: ocoTicketSupport,
                        ext_exto_live_submit: extExtoLiveSubmitEnabled,
                        oco_live_submit: ocoLiveSubmitEnabled,
                        trailing_live_submit: trailingLiveSubmitEnabled,
                        close_order_live_submit: closeOrderLiveSubmitEnabled,
                        futures_live_submit: futuresLiveSubmitEnabled,
                      }}
                    />
                  </div>
                </div>
                <div className="action-row inline">
                  <button className="panel-button" disabled={busyAction !== null || !productionLinkEnabled()} onClick={() => void runCommand("refresh-broker-state", () => api.runProductionLinkAction("refresh", {}), { requiresLive: true })}>
                    Refresh Broker State
                  </button>
                  <button className="panel-button subtle" disabled={busyAction !== null || !productionLinkEnabled()} onClick={() => void runCommand("reconcile-broker-state", () => api.runProductionLinkAction("reconcile", {}), { requiresLive: true })}>
                    Reconcile Broker State
                  </button>
                </div>
              </Section>
            </>
          ) : null}

          {!loading && page === "diagnostics" ? (
            <>
              <Section title="Diagnostics Summary" subtitle="Supportability without cluttering the default operator path">
                <div className="action-row inline">
                  <button className="panel-button" onClick={() => void runCommand("copy-diagnostics", () => api.copyText(diagnosticsSummary))}>
                    Copy Diagnostics Summary
                  </button>
                  <button className="panel-button" disabled={!desktopState?.desktopLogPath} onClick={() => void runCommand("open-desktop-log", () => api.openPath(desktopState?.desktopLogPath ?? ""))}>
                    Open Electron Log
                  </button>
                  <button className="panel-button" disabled={!desktopState?.backendLogPath} onClick={() => void runCommand("open-backend-log", () => api.openPath(desktopState?.backendLogPath ?? ""))}>
                    Open Backend Log
                  </button>
                  <button className="panel-button" disabled={!desktopState?.runtimeLogPath} onClick={() => void runCommand("open-runtime-log", () => api.openPath(desktopState?.runtimeLogPath ?? ""))}>
                    Open Runtime Log
                  </button>
                </div>
                <JsonBlock value={diagnosticsSummary} />
              </Section>

              <Section title="Local Operator Auth Diagnostics" subtitle="Touch ID session truth, local secret wrapper state, and recent local auth events">
                <div className="metric-grid">
                  {localAuthMetrics.map((item) => (
                    <MetricCard key={item.label} label={item.label} value={item.value} tone={item.tone} />
                  ))}
                </div>
                <div className="split-panel">
                  <div>
                    <h3 className="subsection-title">Local Auth State</h3>
                    <JsonBlock value={desktopState?.localAuth ?? null} />
                  </div>
                  <div>
                    <h3 className="subsection-title">Latest Local Auth Event</h3>
                    <JsonBlock value={latestLocalAuthEvent} />
                  </div>
                </div>
                <div>
                  <h3 className="subsection-title">Recent Local Auth Events</h3>
                  <DataTable
                    columns={[
                      { key: "occurred_at", label: "Occurred", render: (row) => formatTimestamp(row.occurred_at) },
                      { key: "event_type", label: "Event Type" },
                      { key: "action", label: "Action", render: (row) => formatValue(row.action) },
                      { key: "instrument", label: "Instrument", render: (row) => formatValue(row.instrument) },
                      { key: "local_operator_identity", label: "Local Operator", render: (row) => formatValue(row.local_operator_identity) },
                      { key: "auth_method", label: "Method", render: (row) => formatValue(row.auth_method ?? "NONE") },
                      { key: "note", label: "Reason / Note", render: (row) => formatValue(row.note) },
                    ]}
                    rows={localAuthEvents.slice(0, 20)}
                    emptyLabel="No local auth events are available yet."
                  />
                </div>
              </Section>

              <Section title="Sunday Open Quick Check" subtitle="Operator-facing checklist grounded in the current app state">
                <div className="split-panel">
                  <div>
                    <h3 className="subsection-title">Healthy At The Open</h3>
                    <div className="status-line">
                      Banner should read {desktopState?.source.label ?? "Source Unknown"} | {desktopState?.backend.label ?? "Backend Unknown"}.
                    </div>
                    <div className="status-line">Preflight verdict should be {preflight.verdict}.</div>
                    <div className="status-line">
                      Runtime should show Dashboard Health Status {formatValue(desktopState?.health?.status ?? desktopState?.backend.healthStatus)} and
                      Dashboard Probe Ready {formatValue(desktopState?.health?.ready)}.
                    </div>
                    <div className="status-line">
                      Normal Sunday-open non-fire reasons are wrong_session and no_new_completed_bar until the correct session and first completed 5-minute bar are in place.
                    </div>
                  </div>
                  <div>
                    <h3 className="subsection-title">Runbook</h3>
                    <CodeBlock lines={sundayRunbookPath ? [sundayRunbookPath] : []} emptyLabel="Runbook path could not be derived from the current desktop state." />
                    <div className="action-row inline">
                      <button className="panel-button" disabled={!sundayRunbookPath} onClick={() => void runCommand("open-sunday-runbook", () => api.openPath(sundayRunbookPath ?? ""))}>
                        Open Sunday Runbook
                      </button>
                    </div>
                  </div>
                </div>
              </Section>

              <Section title="Diagnostics / Evidence" subtitle="Dense by design, separate from default operator flow">
                <div className="split-panel">
                  <div>
                    <h3 className="subsection-title">Operator Surface</h3>
                    <JsonBlock value={operatorSurface} />
                  </div>
                  <div>
                    <h3 className="subsection-title">Runtime Info Files</h3>
                    <CodeBlock lines={infoFiles} emptyLabel="No runtime info files are currently tracked." />
                  </div>
                </div>
              </Section>

              <Section title="Runtime Health Context" subtitle="Latest state that support should ask for first">
                <div className="metric-grid">
                  <MetricCard label="Source Mode" value={desktopState?.source.label ?? "Unknown"} tone={statusTone(desktopState?.source.label)} />
                  <MetricCard label="Backend State" value={desktopState?.backend.label ?? "Unknown"} tone={statusTone(desktopState?.backend.label)} />
                  <MetricCard label="Current Session" value={formatValue(paperReadiness.current_detected_session ?? paperReadiness.runtime_phase ?? global.current_session_date)} />
                  <MetricCard label="Runtime Freshness" value={formatValue(global.stale ? "STALE" : formatRelativeAge(global.last_update_timestamp ?? desktopState?.refreshedAt))} tone={statusTone(global.stale ? "stale" : "fresh")} />
                  <MetricCard label="Eligible Lanes" value={`${laneEligibilityRows.filter((row) => row.eligible_now === true).length}/${laneEligibilityRows.length || 0}`} />
                  <MetricCard label="Lane Risk Rows" value={`${laneRiskRows.length}`} />
                </div>
              </Section>

              <Section title="Same-Underlying Diagnostics" subtitle="Review-state control truth, expiry enforcement, and blocked-entry audit trail">
                <div className="metric-grid">
                  <MetricCard label="Active Holds" value={formatShortNumber(sameUnderlyingConflictSummary.holding_count ?? sameUnderlyingHoldingRows.length)} tone={sameUnderlyingHoldingRows.length > 0 ? "danger" : "good"} />
                  <MetricCard label="Hold Expired" value={formatShortNumber(sameUnderlyingConflictSummary.hold_expired_count ?? sameUnderlyingExpiredRows.length)} tone={sameUnderlyingExpiredRows.length > 0 ? "warn" : "good"} />
                  <MetricCard label="Latest Workflow Event" value={formatValue(sameUnderlyingLatestEvent.event_type ?? "None")} tone={statusTone(sameUnderlyingLatestEvent.event_type)} />
                  <MetricCard label="Latest Workflow Event At" value={formatTimestamp(sameUnderlyingLatestEvent.occurred_at)} />
                  <MetricCard label="Latest Entry Block Event" value={formatValue(sameUnderlyingLatestEntryBlockedEvent.blocked_standalone_strategy_id ?? "None")} tone={statusTone(sameUnderlyingLatestEntryBlockedEvent.blocked_standalone_strategy_id)} />
                  <MetricCard label="Latest Entry Block At" value={formatTimestamp(sameUnderlyingLatestEntryBlockedEvent.occurred_at)} />
                </div>
                <div className="split-panel">
                  <div>
                    <h3 className="subsection-title">Latest Same-Underlying Workflow Event</h3>
                    <JsonBlock value={sameUnderlyingLatestEvent} />
                  </div>
                  <div>
                    <h3 className="subsection-title">Latest Entry Blocked By Hold</h3>
                    <JsonBlock value={sameUnderlyingLatestEntryBlockedEvent} />
                  </div>
                </div>
                <div>
                  <h3 className="subsection-title">Recent Same-Underlying Events</h3>
                  <DataTable
                    columns={[
                      { key: "occurred_at", label: "Occurred", render: (row) => formatTimestamp(row.occurred_at) },
                      { key: "instrument", label: "Instrument" },
                      { key: "event_type", label: "Event Type" },
                      { key: "trigger", label: "Trigger", render: (row) => (row.automatic === true ? "Automatic" : "Operator") },
                      { key: "local_operator_identity", label: "Local Operator", render: (row) => formatValue(row.local_operator_identity ?? row.operator_label) },
                      { key: "auth_method", label: "Auth", render: (row) => formatValue(row.auth_method ?? "NONE") },
                      { key: "blocked_standalone_strategy_id", label: "Blocked Strategy", render: (row) => formatValue(row.blocked_standalone_strategy_id) },
                      { key: "review_state_status", label: "Review State", render: (row) => formatValue(row.review_state_status) },
                      { key: "entry_hold_effective", label: "Entry Hold", render: (row) => formatValue(row.entry_hold_effective ?? false) },
                      { key: "note", label: "Reason / Note", render: (row) => formatValue(row.note ?? row.blocked_reason ?? row.hold_state_reason) },
                    ]}
                    rows={filteredSameUnderlyingEventRows.slice(0, 20)}
                    emptyLabel="No same-underlying events are available yet."
                  />
                </div>
              </Section>

              <Section title="Production Link Diagnostics" subtitle="Broker account truth, reconciliation, and recent broker events">
                <div className="metric-grid">
                  <MetricCard label="Broker Status" value={formatValue(productionLink.label ?? productionLink.status)} tone={statusTone(productionLink.label ?? productionLink.status)} />
                  <MetricCard label="Selected Account Hash" value={formatValue(productionConnection.selected_account_hash ?? productionDiagnostics.selected_account_hash)} />
                  <MetricCard label="Selected Account" value={selectedProductionAccount ? maskAccountNumber(selectedProductionAccount.account_number) : "None"} />
                  <MetricCard label="Attached Mode" value={formatValue(productionDiagnostics.attached_mode)} />
                  <MetricCard label="Reconciliation" value={formatValue(productionReconciliation.label ?? productionReconciliation.status)} tone={statusTone(productionReconciliation.label ?? productionReconciliation.status)} />
                  <MetricCard label="Last Broker Error" value={formatValue(productionDiagnostics.last_error)} tone={statusTone(productionDiagnostics.last_error)} />
                  <MetricCard label="Last Balances Refresh" value={formatTimestamp(productionDiagnostics.last_balances_refresh_at)} />
                  <MetricCard label="Last Positions Refresh" value={formatTimestamp(productionDiagnostics.last_positions_refresh_at)} />
                  <MetricCard label="Last Orders Refresh" value={formatTimestamp(productionDiagnostics.last_orders_refresh_at)} />
                  <MetricCard label="Last Fills Refresh" value={formatTimestamp(productionDiagnostics.last_fills_refresh_at)} />
                  <MetricCard label="Last Reconciliation" value={formatTimestamp(productionDiagnostics.last_reconciliation_at)} />
                  <MetricCard label="Live Submit" value={productionFeatureFlags.live_order_submit_enabled === true ? "Enabled" : "Disabled"} tone={statusTone(productionFeatureFlags.live_order_submit_enabled === true ? "ready" : "blocked")} />
                  <MetricCard label="Next Verification Step" value={formatValue(nextLiveVerificationStep.label ?? nextLiveVerificationStep.verification_key)} tone={statusTone(nextLiveVerificationStep.blocked === true ? "warn" : "ready")} />
                  <MetricCard label="Live-Verified Types" value={liveVerifiedOrderKeys.length ? liveVerifiedOrderKeys.join(", ") : "None"} />
                </div>
                <div className="split-panel">
                  <div>
                    <h3 className="subsection-title">Endpoint Confidence</h3>
                    <JsonBlock
                      value={{
                        live_verified_endpoint_paths: productionDiagnostics.live_verified_endpoint_paths ?? [],
                        implemented_endpoint_paths: productionDiagnostics.implemented_endpoint_paths ?? [],
                        endpoint_uncertainty: productionEndpointUncertainty,
                      }}
                    />
                  </div>
                  <div>
                    <h3 className="subsection-title">Last Manual Order</h3>
                    <JsonBlock
                      value={{
                        request: productionDiagnostics.last_manual_order_request ?? productionLastManualOrder.request ?? null,
                        result: productionDiagnostics.last_manual_order_result ?? productionLastManualOrder.result ?? null,
                        lifecycle_readiness: productionOrderLifecycleReadiness || null,
                      }}
                    />
                  </div>
                  <div>
                    <h3 className="subsection-title">Last Manual Preview</h3>
                    <JsonBlock value={productionDiagnostics.last_manual_order_preview ?? productionLastManualOrderPreview ?? null} />
                  </div>
                </div>
                <div className="split-panel">
                  <div>
                    <h3 className="subsection-title">Order-Type Verification Matrix</h3>
                    <JsonBlock
                      value={{
                        live_verified_order_keys: liveVerifiedOrderKeys,
                        next_step: nextLiveVerificationStep,
                        sequence: orderTypeLiveVerificationSequence,
                        matrix: orderTypeLiveVerificationMatrix,
                      }}
                    />
                  </div>
                  <div>
                    <h3 className="subsection-title">Near-Term Verification Runbooks</h3>
                    <JsonBlock value={nearTermLiveVerificationRunbooks} />
                  </div>
                </div>
                <div className="split-panel">
                  <div>
                    <h3 className="subsection-title">First Live Stock Limit Test</h3>
                    <JsonBlock value={firstLiveStockLimitTest} />
                  </div>
                  <div>
                    <h3 className="subsection-title">First Live Stock Limit Operator Path</h3>
                    <JsonBlock
                      value={{
                        required_flags: asArray<string>(firstLiveStockLimitTest.required_flags),
                        required_whitelist: asArray<string>(asRecord(firstLiveStockLimitTest.required_config).manual_symbol_whitelist),
                        required_account_state: asArray<string>(firstLiveStockLimitTest.required_account_state),
                        required_freshness_state: asArray<string>(firstLiveStockLimitTest.required_freshness_state),
                        submit_path: asArray<string>(firstLiveStockLimitTest.submit_path),
                        cancel_path: asArray<string>(firstLiveStockLimitTest.cancel_path),
                        post_submit_reconciliation_checks: asArray<string>(firstLiveStockLimitTest.expected_reconciliation_checks),
                        post_submit_ui_checks: asArray<string>(firstLiveStockLimitTest.expected_post_submit_checks),
                      }}
                    />
                  </div>
                </div>
                <div className="split-panel">
                  <div>
                    <h3 className="subsection-title">First Live Stock Limit Readiness</h3>
                    <JsonBlock
                      value={{
                        ready_now: firstLiveStockLimitReadyNow,
                        checks: firstLiveStockLimitReadiness,
                        current_submit_blockers: firstLiveStockLimitActive ? manualOrderGateBlockers : [],
                      }}
                    />
                  </div>
                  <div>
                    <h3 className="subsection-title">Exact First Live Stock Limit Operator Path</h3>
                    <JsonBlock
                      value={{
                        submit_path: asArray<string>(firstLiveStockLimitTest.submit_path),
                        cancel_path: asArray<string>(firstLiveStockLimitTest.cancel_path),
                        post_submit_ui_checks: asArray<string>(firstLiveStockLimitTest.expected_post_submit_checks),
                        post_submit_reconciliation_checks: asArray<string>(firstLiveStockLimitTest.expected_reconciliation_checks),
                        replace_expectation: firstLiveStockLimitTest.replace_expectation,
                        cancel_expectation: firstLiveStockLimitTest.cancel_expectation,
                      }}
                    />
                  </div>
                </div>
                <div className="split-panel">
                  <div>
                    <h3 className="subsection-title">Manual Order Safety</h3>
                    <JsonBlock
                      value={{
                        selected_symbol: manualOrderForm.symbol.trim().toUpperCase() || null,
                        selected_asset_class: manualOrderForm.assetClass,
                        selected_structure_type: manualOrderForm.structureType,
                        selected_order_type: manualOrderForm.structureType === "OCO" ? "OCO" : manualOrderForm.orderType,
                        selected_verification_key: selectedManualVerificationKey,
                        selected_verification_status: selectedManualVerification,
                        selected_advanced_mode: manualAdvancedMode,
                        dry_run_supported: manualPreviewBlockers.length === 0,
                        dry_run_only_types_for_asset: dryRunOnlyOrderTypesForAsset,
                        live_enabled_types_for_asset: liveEnabledOrderTypesForAsset,
                        submit_enabled: productionManualSubmitEnabled && manualOrderGateBlockers.length === 0,
                        required_field_completeness: {
                          symbol: Boolean(manualOrderForm.symbol.trim()),
                          quantity: Number(manualOrderForm.quantity) > 0,
                          limit_price: Boolean(manualOrderForm.limitPrice.trim()),
                          stop_price: Boolean(manualOrderForm.stopPrice.trim()),
                          trail_value: Boolean(manualOrderForm.trailValue.trim()),
                          trail_trigger_basis: Boolean(manualOrderForm.trailTriggerBasis.trim()),
                          trail_limit_offset: Boolean(manualOrderForm.trailLimitOffset.trim()),
                        },
                        preview_blockers: manualPreviewBlockers,
                        blockers: manualOrderGateBlockers,
                        warnings: productionManualSafetyWarnings,
                        constraints: productionManualSafetyConstraints,
                        advanced_flags: {
                          advanced_tif_ticket_support: advancedTifTicketSupport,
                          oco_ticket_support: ocoTicketSupport,
                          ext_exto_live_submit_enabled: extExtoLiveSubmitEnabled,
                          oco_live_submit_enabled: ocoLiveSubmitEnabled,
                        },
                      }}
                    />
                  </div>
                  <div>
                    <h3 className="subsection-title">Reconciliation Payload</h3>
                    <JsonBlock value={productionReconciliation.payload ?? null} />
                  </div>
                </div>
                <JsonBlock value={productionLink} />
              </Section>

              {settings.showDiagnostics ? (
                <Section title="Raw Dashboard Payload" subtitle="Current transport payload for inspection">
                  <JsonBlock value={dashboard} />
                </Section>
              ) : null}
            </>
          ) : null}

          {!loading && page === "settings" ? (
            <>
              <Section title="Local Operator Auth" subtitle="macOS Touch ID-backed local operator identity for sensitive actions in this desktop app">
                <div className="metric-grid">
                  {localAuthMetrics.map((item) => (
                    <MetricCard key={item.label} label={item.label} value={item.value} tone={item.tone} />
                  ))}
                </div>
                <div className="notice-strip">
                  <div>Local operator auth protects sensitive desktop actions. Schwab OAuth remains separate and unchanged.</div>
                  <div>{formatValue(localOperatorAuth.last_auth_detail ?? localOperatorAuth.secret_protection?.detail)}</div>
                  <div>{formatValue(localOperatorAuth.secret_protection?.detail ?? "Local secret wrapper status unavailable.")}</div>
                </div>
                <div className="action-row inline">
                  <button
                    className="panel-button"
                    disabled={busyAction !== null || localOperatorAuth.auth_available !== true}
                    onClick={() =>
                      void runCommand("authenticate-local-operator", () =>
                        api.authenticateLocalOperator("Authenticate local operator access for this desktop session."),
                      )
                    }
                  >
                    Authenticate Touch ID
                  </button>
                  <button
                    className="panel-button subtle"
                    disabled={busyAction !== null || localOperatorAuth.auth_session_active !== true}
                    onClick={() => void runCommand("clear-local-operator-auth-session", () => api.clearLocalOperatorAuthSession())}
                  >
                    Clear Local Auth Session
                  </button>
                  <button
                    className="panel-button"
                    disabled={!desktopState?.localAuth.artifacts.state_path}
                    onClick={() => void runCommand("open-local-auth-state", () => api.openPath(desktopState?.localAuth.artifacts.state_path ?? ""))}
                  >
                    Open Local Auth State
                  </button>
                </div>
              </Section>

              <Section title="Desktop Preferences" subtitle="Application-level preferences stored locally in the renderer">
                <div className="settings-grid">
                  <label className="settings-field">
                    <span>Auto-refresh interval</span>
                    <select
                      value={settings.refreshSeconds}
                      onChange={(event) =>
                        setSettings((current) => ({
                          ...current,
                          refreshSeconds: Number(event.target.value),
                        }))
                      }
                    >
                      {[0, 5, 10, 15, 30, 60].map((value) => (
                        <option key={value} value={value}>
                          {value === 0 ? "Manual only" : `${value} seconds`}
                        </option>
                      ))}
                    </select>
                  </label>

                  <label className="settings-field">
                    <span>Default page</span>
                    <select
                      value={settings.defaultPage}
                      onChange={(event) =>
                        setSettings((current) => ({
                          ...current,
                          defaultPage: event.target.value as PageId,
                        }))
                      }
                    >
                      {NAV_ITEMS.map((item) => (
                        <option key={item.id} value={item.id}>
                          {item.label}
                        </option>
                      ))}
                    </select>
                  </label>

                  <label className="settings-toggle">
                    <input
                      type="checkbox"
                      checked={settings.showDiagnostics}
                      onChange={(event) =>
                        setSettings((current) => ({
                          ...current,
                          showDiagnostics: event.target.checked,
                        }))
                      }
                    />
                    <span>Show raw payload diagnostics panels</span>
                  </label>

                  <div className="settings-actions">
                    <button className="panel-button" onClick={() => setSettings(DEFAULT_SETTINGS)}>
                      Reset Preferences
                    </button>
                    <button className="panel-button" onClick={() => void runCommand("open-runtime-log", () => api.openPath(desktopState?.runtimeLogPath ?? ""))}>
                      Open Runtime Log
                    </button>
                    <button className="panel-button" onClick={() => void runCommand("open-desktop-log", () => api.openPath(desktopState?.desktopLogPath ?? ""))}>
                      Open Electron Log
                    </button>
                  </div>
                </div>
              </Section>
            </>
          ) : null}
        </main>
      </div>
    </div>
  );
}

function Section(props: { title: string; subtitle?: string; children: ReactNode; className?: string; headerClassName?: string }) {
  if (currentSectionPageContext && PRIMARY_WORKSTATION_PAGES.has(currentSectionPageContext) && EVIDENCE_ONLY_SECTION_TITLES.has(props.title)) {
    return null;
  }
  const demoted = DEMOTED_PRIMARY_SECTION_TITLES.has(props.title);
  if (demoted) {
    return (
      <details className="secondary-evidence-shell">
        <summary className="secondary-evidence-summary">
          <span className="secondary-evidence-title">{props.title}</span>
          <span className="secondary-evidence-note">{props.subtitle ?? "Supporting evidence"}</span>
        </summary>
        <section className={`section-card secondary-evidence-card ${props.className ?? ""}`.trim()} data-section-title={props.title}>
          <div className={`section-header ${props.headerClassName ?? ""}`.trim()}>
            <div>
              <div className="section-title">{props.title}</div>
              {props.subtitle ? <div className="section-subtitle">{props.subtitle}</div> : null}
            </div>
          </div>
          {props.children}
        </section>
      </details>
    );
  }
  return (
    <section className={`section-card ${props.className ?? ""}`.trim()} data-section-title={props.title}>
      <div className={`section-header ${props.headerClassName ?? ""}`.trim()}>
        <div>
          <div className="section-title">{props.title}</div>
          {props.subtitle ? <div className="section-subtitle">{props.subtitle}</div> : null}
        </div>
      </div>
      {props.children}
    </section>
  );
}

function TrendPanel(props: {
  title: string;
  subtitle: string;
  points: Array<{ timestamp: string; value: number }>;
  tone?: Tone;
  footer?: string;
  className?: string;
}) {
  const width = 420;
  const height = 180;
  const values = props.points.map((point) => point.value).filter((value) => Number.isFinite(value));
  const min = values.length ? Math.min(...values) : 0;
  const max = values.length ? Math.max(...values) : 0;
  const range = max - min || 1;
  const path = props.points
    .map((point, index) => {
      const x = props.points.length === 1 ? width / 2 : (index / Math.max(props.points.length - 1, 1)) * (width - 24) + 12;
      const y = height - (((point.value - min) / range) * (height - 36) + 18);
      return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(" ");
  return (
    <div className={`trend-panel ${props.className ?? ""}`.trim()}>
      <div className="trend-panel-header">
        <div>
          <div className="subsection-title">{props.title}</div>
          <div className="section-subtitle">{props.subtitle}</div>
        </div>
        <Badge label={props.points.length > 1 ? "LIVE SERIES" : "SNAPSHOT"} tone={props.tone ?? "muted"} />
      </div>
      <div className="trend-panel-shell">
        {props.points.length ? (
          <svg className="trend-panel-svg" viewBox={`0 0 ${width} ${height}`} role="img" aria-label={props.title}>
            <line x1="12" y1={height - 18} x2={width - 12} y2={height - 18} className="trend-panel-axis" />
            <line x1="12" y1="18" x2="12" y2={height - 18} className="trend-panel-axis" />
            <path d={path} className={`trend-panel-line ${props.tone ?? "muted"}`} />
          </svg>
        ) : (
          <div className="placeholder-note">No intraday points are available yet.</div>
        )}
      </div>
      <div className="trend-panel-footer">
        <span>{props.points.length ? `${formatShortNumber(props.points.length)} points` : "0 points"}</span>
        <span>{props.footer ?? "Waiting for fresh activity."}</span>
      </div>
    </div>
  );
}

function CalendarHistoryChart(props: {
  mode: "line" | "bar";
  title: string;
  subtitle: string;
  points: CalendarDayPoint[];
}) {
  const width = 1080;
  const height = 340;
  const marginLeft = 44;
  const marginRight = 26;
  const marginTop = 28;
  const marginBottom = 44;
  const plotWidth = width - marginLeft - marginRight;
  const plotHeight = height - marginTop - marginBottom;
  if (!props.points.length) {
    return (
      <div className="calendar-chart-shell">
        <div className="calendar-chart-header">
          <div>
            <div className="subsection-title">{props.title}</div>
            <div className="section-subtitle">{props.subtitle}</div>
          </div>
        </div>
        <div className="placeholder-note">No closed-trade daily history is available for this source and period yet.</div>
      </div>
    );
  }
  const cumulativeValues = props.points.map((point) => point.cumulative);
  const dailyValues = props.points.map((point) => point.pnl);
  const cumulativeMin = Math.min(...cumulativeValues, 0);
  const cumulativeMax = Math.max(...cumulativeValues, 0);
  const dailyMin = Math.min(...dailyValues, 0);
  const dailyMax = Math.max(...dailyValues, 0);
  const cumulativeRange = Math.max(cumulativeMax - cumulativeMin, 1);
  const dailyRange = Math.max(dailyMax - dailyMin, 1);
  const xForIndex = (index: number) => marginLeft + (plotWidth * index) / Math.max(props.points.length - 1, 1);
  const cumulativeY = (value: number) => marginTop + ((cumulativeMax - value) / cumulativeRange) * plotHeight;
  const dailyY = (value: number) => marginTop + ((dailyMax - value) / dailyRange) * plotHeight;
  const zeroY = dailyY(0);
  const cumulativePath = props.points
    .map((point, index) => `${index === 0 ? "M" : "L"} ${xForIndex(index).toFixed(2)} ${cumulativeY(point.cumulative).toFixed(2)}`)
    .join(" ");
  const dailyPath = props.points
    .map((point, index) => `${index === 0 ? "M" : "L"} ${xForIndex(index).toFixed(2)} ${dailyY(point.pnl).toFixed(2)}`)
    .join(" ");
  const tickIndices = props.points.length <= 6 ? props.points.map((_, index) => index) : [0, Math.floor(props.points.length * 0.25), Math.floor(props.points.length * 0.5), Math.floor(props.points.length * 0.75), props.points.length - 1];

  return (
    <div className="calendar-chart-shell" data-calendar-view={props.mode}>
      <div className="calendar-chart-header">
        <div>
          <div className="subsection-title">{props.title}</div>
          <div className="section-subtitle">{props.subtitle}</div>
        </div>
        <div className="calendar-chart-legend">
          {props.mode === "line" ? (
            <>
              <span className="calendar-legend-item"><span className="calendar-legend-swatch cumulative" /> Cumulative P&amp;L</span>
              <span className="calendar-legend-item"><span className="calendar-legend-swatch daily" /> Daily P&amp;L</span>
            </>
          ) : (
            <span className="calendar-legend-item"><span className="calendar-legend-swatch bars" /> Daily P&amp;L</span>
          )}
        </div>
      </div>
      <svg className="calendar-history-svg" viewBox={`0 0 ${width} ${height}`} role="img" aria-label={props.title}>
        <line x1={marginLeft} y1={marginTop} x2={marginLeft} y2={height - marginBottom} className="calendar-axis-line" />
        <line x1={marginLeft} y1={height - marginBottom} x2={width - marginRight} y2={height - marginBottom} className="calendar-axis-line" />
        <line x1={marginLeft} y1={zeroY} x2={width - marginRight} y2={zeroY} className="calendar-zero-line" />
        {tickIndices.map((index) => (
          <g key={`tick-${index}`}>
            <line x1={xForIndex(index)} y1={marginTop} x2={xForIndex(index)} y2={height - marginBottom} className="calendar-grid-line" />
            <text x={xForIndex(index)} y={height - 14} className="calendar-axis-label" textAnchor="middle">
              {dateFromKey(props.points[index]?.date ?? props.points[0]?.date ?? "").toLocaleString(undefined, { month: "short", day: "numeric" })}
            </text>
          </g>
        ))}
        {props.mode === "line" ? (
          <>
            <path d={cumulativePath} className="calendar-line cumulative" />
            <path d={dailyPath} className="calendar-line daily" />
            {props.points.map((point, index) => (
              <circle key={point.date} cx={xForIndex(index)} cy={cumulativeY(point.cumulative)} r={3} className="calendar-point cumulative" />
            ))}
          </>
        ) : (
          props.points.map((point, index) => {
            const x = xForIndex(index);
            const y = dailyY(point.pnl);
            const barWidth = Math.max(Math.min(plotWidth / Math.max(props.points.length, 1) - 8, 20), 6);
            const barHeight = Math.max(Math.abs(zeroY - y), 2);
            const top = point.pnl >= 0 ? y : zeroY;
            return (
              <rect
                key={point.date}
                x={x - barWidth / 2}
                y={top}
                width={barWidth}
                height={barHeight}
                rx={3}
                className={`calendar-bar ${point.pnl >= 0 ? "positive" : "negative"}`}
              />
            );
          })
        )}
      </svg>
    </div>
  );
}

function CalendarDayDetailPanel(props: {
  day: CalendarDayPoint;
  sourceLabel: string;
  onClose: () => void;
  onOpenStrategy: (contribution: CalendarStrategyContribution) => void;
}) {
  return (
    <div className="calendar-day-detail" data-selected-calendar-day={props.day.date}>
      <div className="calendar-day-detail-header">
        <div>
          <div className="page-eyebrow">Selected Day</div>
          <div className="section-title">{formatLongDate(props.day.date)}</div>
          <div className="section-subtitle">{props.sourceLabel}</div>
        </div>
        <button className="panel-button subtle" onClick={props.onClose}>Close</button>
      </div>
      <div className="metric-grid calendar-day-detail-metrics">
        <MetricCard label="Gross P&L" value={formatCompactCurrency(props.day.pnl)} tone={pnlTone(props.day.pnl)} />
        <MetricCard label="Trade Count" value={formatShortNumber(props.day.tradeCount)} tone="muted" />
        <MetricCard label="Cumulative P&L" value={formatCompactCurrency(props.day.cumulative)} tone={pnlTone(props.day.cumulative)} />
      </div>
      {props.day.tradeCount === 0 && props.day.coveredSources.length ? (
        <div className="notice-strip compact">
          <div><strong>Historical Coverage</strong> {props.day.coveredSources.map((source) => source.replace(/_/g, " ")).join(" + ")}</div>
          <div>No closed trades were published for this covered back-cast day.</div>
        </div>
      ) : null}
      <div className="calendar-contribution-table-shell">
        <table className="data-table calendar-contribution-table">
          <thead>
            <tr>
              <th>Strategy</th>
              <th>Source</th>
              <th>Trades</th>
              <th>P&amp;L</th>
            </tr>
          </thead>
          <tbody>
            {props.day.contributions.map((contribution) => (
              <tr
                key={`${contribution.source}:${contribution.strategyId}:${contribution.laneId ?? ""}`}
                className="is-clickable"
                onClick={() => props.onOpenStrategy(contribution)}
              >
                <td>{contribution.strategyName}</td>
                <td><Badge label={contribution.source.replace(/_/g, " ")} tone={contribution.source === "paper" ? "muted" : contribution.source === "live" ? "good" : "warn"} /></td>
                <td>{formatShortNumber(contribution.tradeCount)}</td>
                <td>{renderPnlValue(contribution.pnl)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function PositionsMonitorGrid(props: {
  columns: PositionsMonitorColumn[];
  rows: PositionsMonitorRow[];
  sort: PositionsSortState;
  selectedRowId: string;
  emptyLabel: string;
  totalsByColumnId?: Record<string, ReactNode>;
  onSort: (columnId: string) => void;
  onSelectRow: (row: PositionsMonitorRow) => void;
  onRowContextMenu: (row: PositionsMonitorRow) => void;
}) {
  const priorityColumns = new Set([
    "symbol",
    "qty",
    "mark",
    "dayPnl",
    "openPnl",
    "realizedPnl",
    "marketValue",
    "brokerOpenPnl",
    "paperOpenPnl",
    "combinedRealized",
    "netValue",
  ]);
  const pnlColumns = new Set([
    "dayPnl",
    "openPnl",
    "realizedPnl",
    "ytdPnl",
    "brokerOpenPnl",
    "paperOpenPnl",
    "brokerRealized",
    "paperRealized",
    "combinedRealized",
    "maxDrawdown",
  ]);
  if (!props.rows.length) {
    return <div className="placeholder-note">{props.emptyLabel}</div>;
  }
  return (
    <div className="monitor-table-shell">
      <table className="monitor-table">
        <thead>
          <tr>
            {props.columns.map((column) => (
              <th
                key={column.id}
                className={[
                  column.sticky ? "sticky-col" : "",
                  priorityColumns.has(column.id) ? "priority-col" : "",
                  pnlColumns.has(column.id) ? "pnl-col" : "",
                  `col-${column.id}`,
                ].filter(Boolean).join(" ")}
              >
                <button className="monitor-sort-button" onClick={() => props.onSort(column.id)}>
                  <span>{column.label}</span>
                  <span className="monitor-sort-indicator">
                    {props.sort.columnId === column.id ? (props.sort.direction === "asc" ? "↑" : "↓") : "↕"}
                  </span>
                </button>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {props.rows.map((row) => (
            <tr
              key={row.id}
              className={[
                props.selectedRowId === row.id ? "selected" : "",
                row.isSpreadParent ? "spread-parent-row" : "",
                row.isSpreadLeg ? "spread-leg-row" : "",
              ].filter(Boolean).join(" ") || undefined}
              onClick={() => props.onSelectRow(row)}
              onContextMenu={(event) => {
                event.preventDefault();
                props.onRowContextMenu(row);
              }}
            >
              {props.columns.map((column) => (
                <td
                  key={column.id}
                  className={[
                    column.sticky ? "sticky-col" : "",
                    column.align === "right" ? "align-right" : "",
                    priorityColumns.has(column.id) ? "priority-col" : "",
                    pnlColumns.has(column.id) ? "pnl-col" : "",
                    `col-${column.id}`,
                  ].filter(Boolean).join(" ")}
                >
                  {column.render(row)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
        {props.totalsByColumnId ? (
          <tfoot>
            <tr className="monitor-totals-row">
              {props.columns.map((column) => (
                <td
                  key={column.id}
                  className={[
                    column.sticky ? "sticky-col" : "",
                    column.align === "right" ? "align-right" : "",
                    priorityColumns.has(column.id) ? "priority-col" : "",
                    pnlColumns.has(column.id) ? "pnl-col" : "",
                    `col-${column.id}`,
                  ].filter(Boolean).join(" ")}
                >
                  {props.totalsByColumnId?.[column.id] ?? ""}
                </td>
              ))}
            </tr>
          </tfoot>
        ) : null}
      </table>
    </div>
  );
}

function PositionsDrawer(props: {
  open: boolean;
  row: PositionsMonitorRow;
  tab: PositionsDrawerTab;
  onClose: () => void;
  onChangeTab: (tab: PositionsDrawerTab) => void;
}) {
  if (!props.open) {
    return null;
  }

  const tabs: Array<{ id: PositionsDrawerTab; label: string }> = [
    { id: "summary", label: "Summary" },
    { id: "trades", label: "Trades" },
    { id: "orders", label: "Orders" },
    { id: "attribution", label: "Attribution" },
    { id: "margin", label: "Margin" },
    { id: "conflict", label: "Conflict" },
    { id: "activity", label: "Activity" },
    { id: "instrument", label: "Instrument" },
  ];
  const row = props.row;
  const sourceBadges = row.sourceBadges ?? sourceBadgesForRow(row);
  const sourceBreakdownRows = [
    row.brokerRows.length
      ? {
          source: "BROKER",
          rows: row.brokerRows.length,
          qty: row.brokerQty,
          openPnl: row.brokerOpenPnl,
          realized: row.brokerRealized,
          status: row.brokerRows[0]?.side ?? "—",
        }
      : null,
    row.approvedPaperRows.length
      ? {
          source: "PAPER",
          rows: row.approvedPaperRows.length,
          qty: row.paperQty,
          openPnl: sumNullable(row.approvedPaperRows.map((paperRow) => numericOrNull(paperRow.unrealized_pnl))),
          realized: sumNullable(row.approvedPaperRows.map((paperRow) => numericOrNull(paperRow.realized_pnl))),
          status: row.approvedPaperRows[0]?.status ?? "—",
        }
      : null,
    row.experimentalRows.length
      ? {
          source: "EXPERIMENTAL",
          rows: row.experimentalRows.length,
          qty: row.experimentalRows.reduce((sum, paperRow) => sum + quantityFromPaperRow(paperRow), 0),
          openPnl: sumNullable(row.experimentalRows.map((paperRow) => numericOrNull(paperRow.unrealized_pnl))),
          realized: sumNullable(row.experimentalRows.map((paperRow) => numericOrNull(paperRow.realized_pnl))),
          status: row.experimentalRows[0]?.status ?? "—",
        }
      : null,
  ].filter(Boolean) as JsonRecord[];
  const optionLegRows: JsonRecord[] = (row.childRows ?? []).map((childRow, index) => ({
    id: `${row.id}-leg-${index}`,
    symbol: childRow.symbol,
    description: childRow.description,
    side: childRow.exposureMarker ?? "—",
    qty: childRow.brokerQty,
    avgPrice: childRow.brokerAvgPrice,
    mark: childRow.brokerMark,
    openPnl: childRow.brokerOpenPnl,
  }));
  const activityRows = [
    ...row.closedTrades.map((tradeRow) => ({
      time: tradeRow.exit_timestamp ?? tradeRow.entry_timestamp,
      type: "TRADE",
      detail: `${formatValue(tradeRow.side)} ${formatValue(tradeRow.strategy_name)} ${formatCompactPnL(tradeRow.realized_pnl)}`,
    })),
    ...row.brokerEvents.map((eventRow) => ({
      time: eventRow.occurred_at ?? eventRow.updated_at,
      type: "BROKER EVENT",
      detail: `${formatValue(eventRow.event_type)} ${formatValue(eventRow.status)} ${formatValue(eventRow.message)}`,
    })),
  ].sort((left, right) => parseTimestampMs(right.time) - parseTimestampMs(left.time));

  return (
    <>
      <button className="positions-drawer-scrim" aria-label="Close position drawer" onClick={props.onClose} />
      <aside className="positions-drawer">
        <div className="positions-drawer-header">
          <div>
            <div className="section-subtitle">Position Detail</div>
            <div className="section-title">{row.displaySymbol ?? row.symbol}</div>
            <div className="status-line">{row.displayDescription ?? row.description}</div>
            <div className="badge-row compact">
              {sourceBadges.map((badge) => (
                <Badge key={badge} label={badge} tone={badge === "BROKER" ? "good" : badge === "EXPERIMENTAL" ? "warn" : "muted"} />
              ))}
              {row.exposureMarker ? <span className={`monitor-flag ${row.exposureMarker.toLowerCase()}`}>{row.exposureMarker}</span> : null}
            </div>
            {row.spreadLabel ? <div className="positions-inline-note">{row.spreadLabel}</div> : null}
          </div>
          <button className="panel-button subtle" onClick={props.onClose}>
            Close
          </button>
        </div>
        <div className="positions-drawer-tabs">
          {tabs.map((tab) => (
            <button key={tab.id} className={`positions-drawer-tab ${props.tab === tab.id ? "active" : ""}`} onClick={() => props.onChangeTab(tab.id)}>
              {tab.label}
            </button>
          ))}
        </div>
        <div className="positions-drawer-body">
          {props.tab === "summary" ? (
            <>
              <div className="metric-grid">
                <MetricCard label="Broker Qty" value={formatCompactMetric(row.brokerQty, 4)} />
                <MetricCard label="Paper Qty" value={formatCompactMetric(row.paperQty, 0)} />
                <MetricCard label="Broker Open P/L" value={renderPnlValue(row.brokerOpenPnl)} tone={pnlTone(row.brokerOpenPnl)} />
                <MetricCard label="Paper Open P/L" value={renderPnlValue(row.paperOpenPnl)} tone={pnlTone(row.paperOpenPnl)} />
                <MetricCard label="Paper Realized" value={renderPnlValue(row.paperRealized)} tone={pnlTone(row.paperRealized)} />
                <MetricCard label="Exposure" value={row.exposureMarker ?? "—"} tone={row.exposureMarker === "BOTH" ? "warn" : "muted"} />
                <MetricCard label="Conflict" value={row.conflict} tone={statusTone(row.conflictState)} />
                <MetricCard label="Last Activity" value={formatCompactTimestamp(row.lastActivity)} />
                <MetricCard label="Status" value={row.currentStatus} tone={statusTone(row.currentStatus)} />
              </div>
              <h3 className="subsection-title">Source Breakdown</h3>
              <DataTable
                columns={[
                  { key: "source", label: "Source" },
                  { key: "rows", label: "Rows" },
                  { key: "qty", label: "Qty", render: (sourceRow) => formatCompactMetric(sourceRow.qty, sourceRow.source === "BROKER" ? 4 : 0) },
                  { key: "openPnl", label: "Open P/L", render: (sourceRow) => renderPnlValue(sourceRow.openPnl) },
                  { key: "realized", label: "Realized", render: (sourceRow) => renderPnlValue(sourceRow.realized) },
                  { key: "status", label: "Status" },
                ]}
                rows={sourceBreakdownRows}
                emptyLabel="No source rows are attached to this symbol."
              />
              {optionLegRows.length ? (
                <>
                  <h3 className="subsection-title">Option Legs</h3>
                  <DataTable
                    columns={[
                      { key: "symbol", label: "Symbol" },
                      { key: "description", label: "Description" },
                      { key: "side", label: "Side" },
                      { key: "qty", label: "Qty", render: (legRow) => formatCompactMetric(legRow.qty, 4) },
                      { key: "avgPrice", label: "Avg", render: (legRow) => formatCompactPrice(legRow.avgPrice) },
                      { key: "mark", label: "Mark", render: (legRow) => formatCompactPrice(legRow.mark) },
                      { key: "openPnl", label: "Open P/L", render: (legRow) => renderPnlValue(legRow.openPnl) },
                    ]}
                    rows={optionLegRows}
                    emptyLabel="No option legs are attached to this spread."
                  />
                </>
              ) : null}
            </>
          ) : null}

          {props.tab === "trades" ? (
            <DataTable
              columns={[
                { key: "exit_timestamp", label: "Time", render: (tradeRow) => formatTimestamp(tradeRow.exit_timestamp ?? tradeRow.entry_timestamp) },
                { key: "side", label: "Side" },
                { key: "entry_price", label: "Entry", render: (tradeRow) => formatCompactPrice(tradeRow.entry_price) },
                { key: "exit_price", label: "Exit", render: (tradeRow) => formatCompactPrice(tradeRow.exit_price) },
                { key: "realized_pnl", label: "Realized P/L", render: (tradeRow) => renderPnlValue(tradeRow.realized_pnl) },
                { key: "strategy_name", label: "Strategy" },
                { key: "exit_reason", label: "Exit Reason" },
              ]}
              rows={row.closedTrades}
              emptyLabel="No closed trades are attached to this symbol."
            />
          ) : null}

          {props.tab === "orders" ? (
            <>
              <DataTable
                columns={[
                  { key: "broker_order_id", label: "Broker Order ID" },
                  { key: "status", label: "Status" },
                  { key: "instruction", label: "Instruction" },
                  { key: "quantity", label: "Qty" },
                  { key: "updated_at", label: "Updated", render: (orderRow) => formatTimestamp(orderRow.updated_at) },
                ]}
                rows={row.brokerOrders}
                emptyLabel="No broker orders are attached to this symbol."
              />
              <DataTable
                columns={[
                  { key: "broker_order_id", label: "Broker Order ID" },
                  { key: "status", label: "Status" },
                  { key: "instruction", label: "Instruction" },
                  { key: "filled_quantity", label: "Filled" },
                  { key: "updated_at", label: "Updated", render: (fillRow) => formatTimestamp(fillRow.updated_at) },
                ]}
                rows={row.brokerFills}
                emptyLabel="No broker fills are attached to this symbol."
              />
            </>
          ) : null}

          {props.tab === "attribution" ? (
            <DataTable
              columns={[
                { key: "strategy_name", label: "Strategy" },
                { key: "paper_strategy_class", label: "Class", render: (paperRow) => <Badge label={tradeTopLevelClass(paperRow)} tone={tradeTopLevelClass(paperRow) === "EXPERIMENTAL" ? "warn" : "muted"} /> },
                { key: "position_side", label: "Position" },
                { key: "realized_pnl", label: "Realized P/L", render: (paperRow) => renderPnlValue(paperRow.realized_pnl) },
                { key: "unrealized_pnl", label: "Open P/L", render: (paperRow) => renderPnlValue(paperRow.unrealized_pnl) },
                { key: "trade_count", label: "Trades", render: (paperRow) => formatCompactMetric(paperRow.trade_count, 0) },
                { key: "latest_activity_timestamp", label: "Last Activity", render: (paperRow) => formatTimestamp(paperRow.latest_activity_timestamp) },
              ]}
              rows={row.paperRows}
              emptyLabel="No paper or experimental attribution rows are attached to this symbol."
            />
          ) : null}

          {props.tab === "margin" ? (
            <>
              <div className="metric-grid">
                <MetricCard label="Market Value" value={formatCompactMetric(row.brokerMarketValue)} />
                <MetricCard label="Margin / BP Effect" value={formatCompactMetric(row.brokerMarginEffect)} />
                <MetricCard label="Delta" value={formatCompactMetric(row.brokerDelta, 4)} />
                <MetricCard label="Theta" value={formatCompactMetric(row.brokerTheta, 4)} />
              </div>
              <div className="notice-strip">
                <div>Broker margin and buying-power fields stay broker-only. Paper values are not silently merged into these cards.</div>
                <div>Use this panel to explain what is live broker impact versus paper-only strategy exposure.</div>
              </div>
              <JsonBlock value={row.brokerRows[0]?.raw_payload ?? { note: "No broker payload is attached to this symbol." }} />
            </>
          ) : null}

          {props.tab === "conflict" ? (
            <DataTable
              columns={[
                { key: "instrument", label: "Instrument" },
                { key: "severity", label: "Severity" },
                { key: "review_state_status", label: "Review State" },
                { key: "broker_overlap_present", label: "Broker Overlap", render: (conflictRow) => formatValue(conflictRow.broker_overlap_present) },
                { key: "hold_new_entries", label: "Entry Hold", render: (conflictRow) => formatValue(conflictRow.hold_new_entries) },
                { key: "note", label: "Note", render: (conflictRow) => formatValue(conflictRow.note ?? conflictRow.same_underlying_note) },
              ]}
              rows={row.sameUnderlyingRows}
              emptyLabel="No same-underlying conflicts are attached to this symbol."
            />
          ) : null}

          {props.tab === "activity" ? (
            <DataTable
              columns={[
                { key: "time", label: "Time", render: (activityRow) => formatTimestamp(activityRow.time) },
                { key: "type", label: "Type" },
                { key: "detail", label: "Detail" },
              ]}
              rows={activityRows}
              emptyLabel="No recent activity is attached to this symbol."
            />
          ) : null}

          {props.tab === "instrument" ? (
            <JsonBlock
              value={{
                symbol: row.symbol,
                description: row.description,
                spread: row.isSpreadParent
                  ? {
                      label: row.spreadLabel,
                      leg_count: row.childRows?.length ?? 0,
                      legs: row.childRows?.map((childRow) => ({
                        symbol: childRow.symbol,
                        description: childRow.description,
                        side: childRow.exposureMarker,
                        qty: childRow.brokerQty,
                        mark: childRow.brokerMark,
                        open_pnl: childRow.brokerOpenPnl,
                      })),
                    }
                  : null,
                broker: row.brokerRows[0] ?? null,
                paper: row.paperRows[0] ?? null,
              }}
            />
          ) : null}
        </div>
      </aside>
    </>
  );
}

function MetricCard(props: { label: string; value: ReactNode; tone?: "good" | "warn" | "danger" | "muted" }) {
  return (
    <div className={`metric-card ${props.tone ?? "muted"}`}>
      <div className="metric-label">{props.label}</div>
      <div className="metric-value">{props.value}</div>
    </div>
  );
}

function MetricMini(props: { label: string; value: ReactNode; tone?: "good" | "warn" | "danger" | "muted" }) {
  return (
    <div className={`metric-mini ${props.tone ?? "muted"}`}>
      <div className="metric-mini-label">{props.label}</div>
      <div className="metric-mini-value">{props.value}</div>
    </div>
  );
}

function Badge(props: { label: string; tone?: "good" | "warn" | "danger" | "muted" }) {
  return <span className={`badge ${props.tone ?? "muted"}`}>{sentenceCase(props.label).toUpperCase()}</span>;
}

function ControlButton(props: { label: string; onClick: () => void; busyAction: string | null; danger?: boolean; disabled?: boolean }) {
  return (
    <button className={`control-button ${props.danger ? "danger" : ""}`} disabled={props.busyAction !== null || props.disabled} onClick={props.onClick}>
      {props.label}
    </button>
  );
}

function JsonBlock(props: { value: unknown }) {
  return <pre className="json-block">{typeof props.value === "string" ? props.value : JSON.stringify(props.value, null, 2)}</pre>;
}

function CodeBlock(props: { lines: string[]; emptyLabel: string }) {
  return <pre className="json-block">{props.lines.length ? props.lines.join("\n") : props.emptyLabel}</pre>;
}

function StartupPanel(props: {
  desktopState: DesktopState | null;
  busyAction: string | null;
  onRetryStart: () => void;
}) {
  const startup = props.desktopState?.startup;
  const backend = props.desktopState?.backend;
  const canRetry = backend?.state !== "healthy" && props.busyAction === null;

  return (
    <div className="startup-panel">
      <div className="metric-grid">
        <MetricCard label="Preferred URL" value={formatValue(startup?.preferredUrl)} />
        <MetricCard label="Chosen URL" value={formatValue(startup?.chosenUrl ?? props.desktopState?.backendUrl)} />
        <MetricCard label="Chosen Port" value={formatValue(startup?.chosenPort)} />
        <MetricCard label="Ownership" value={ownershipLabel(startup?.ownership)} tone={statusTone(ownershipLabel(startup?.ownership))} />
        <MetricCard label="Startup State" value={backend?.label ?? "Unknown"} tone={statusTone(backend?.label)} />
        <MetricCard label="Port Policy" value={startup?.allowPortFallback ? "Explicit fallback enabled" : "Fixed preferred port"} />
        <MetricCard label="Failure Kind" value={startupFailureLabel(backend?.startupFailureKind)} tone={statusTone(backend?.startupFailureKind)} />
        <MetricCard label="Backend Health Reachable" value={backend?.healthReachable ? "YES" : "NO"} tone={backend?.healthReachable ? "good" : "warn"} />
        <MetricCard label="/api/dashboard Timed Out" value={backend?.dashboardApiTimedOut ? "YES" : "NO"} tone={backend?.dashboardApiTimedOut ? "warn" : "good"} />
        <MetricCard label="Stale Listener Detected" value={backend?.staleListenerDetected ? "YES" : "NO"} tone={backend?.staleListenerDetected ? "warn" : "good"} />
        <MetricCard label="Port Conflict Detected" value={backend?.portConflictDetected ? "YES" : "NO"} tone={backend?.portConflictDetected ? "warn" : "good"} />
        <MetricCard label="Managed Exit Code" value={formatValue(props.desktopState?.manager.lastExitCode)} />
        <MetricCard label="Managed Exit Signal" value={formatValue(props.desktopState?.manager.lastExitSignal)} />
        <MetricCard label="Next Retry" value={formatTimestamp(backend?.nextRetryAt)} />
      </div>
      {backend?.lastError ? <div className="startup-error">Latest bind/start error: {backend.lastError}</div> : null}
      {startup?.recommendedAction || backend?.actionHint ? (
        <div className="startup-hint">Next Action: {startup?.recommendedAction ?? backend?.actionHint}</div>
      ) : null}
      <div className="action-row inline">
        <button className="panel-button" disabled={!canRetry} onClick={props.onRetryStart}>
          Retry Start
        </button>
      </div>
      <div className="startup-events">
        <div className="subsection-title">Recent Backend Events</div>
        <CodeBlock lines={startup?.recentEvents ?? []} emptyLabel="No startup events have been recorded in this desktop session." />
      </div>
    </div>
  );
}

function PreflightPanel(props: { model: PreflightModel }) {
  return (
    <div className="preflight-panel">
      <div className={`preflight-verdict ${statusTone(props.model.verdict)}`}>
        <div className="preflight-verdict-label">Verdict</div>
        <div className="preflight-verdict-value">{props.model.verdict}</div>
      </div>
      <div className="preflight-grid">
        {props.model.checks.map((check) => (
          <div key={check.key} className={`preflight-check ${check.status}`}>
            <div className="preflight-check-header">
              <span className="preflight-check-label">{check.label}</span>
              <Badge label={check.status === "pass" ? "PASS" : check.status === "warn" ? "WARN" : "FAIL"} tone={statusTone(check.status === "pass" ? "ready" : check.status)} />
            </div>
            <div className="preflight-check-value">{check.value}</div>
            <div className="preflight-check-detail">{check.detail}</div>
          </div>
        ))}
      </div>
      {props.model.blockers.length ? (
        <div className="preflight-blockers">
          <div className="preflight-list-title">Blockers</div>
          {props.model.blockers.map((item) => (
            <div key={item} className="preflight-list-item">
              {item}
            </div>
          ))}
        </div>
      ) : null}
      {!props.model.blockers.length && props.model.warnings.length ? (
        <div className="preflight-blockers warn">
          <div className="preflight-list-title">Warnings</div>
          {props.model.warnings.map((item) => (
            <div key={item} className="preflight-list-item">
              {item}
            </div>
          ))}
        </div>
      ) : null}
      {props.model.informational.length ? (
        <div className="preflight-blockers info">
          <div className="preflight-list-title">Informational Feed Degradation</div>
          {props.model.informational.map((item) => (
            <div key={item} className="preflight-list-item">
              {item}
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
}

function PaperStartupPanel(props: {
  metrics: Array<{ label: string; value: string; tone?: Tone }>;
  stateLabel: string;
  reason: string;
  actionLabel: string;
  actionDescription: string;
  busyAction: string | null;
  canRunLiveActions: boolean;
  onStartDashboard: () => void;
  onStartPaper: () => void;
  onRestartPaperWithTempPaper: () => void;
  onAuthGateCheck: () => void;
  onCompletePreSessionReview: () => void;
}) {
  let primaryAction = props.onStartPaper;
  let primaryButtonLabel = props.actionLabel;
  let disabled = props.busyAction !== null || props.actionLabel === "Runtime Active" || props.actionLabel === "Auto-Restart In Progress";
  if (props.actionLabel === "Start Dashboard/API") {
    primaryAction = props.onStartDashboard;
  } else if (props.actionLabel === "Restart Runtime + Temp Paper") {
    primaryAction = props.onRestartPaperWithTempPaper;
    disabled = disabled || !props.canRunLiveActions;
  } else if (props.actionLabel === "Auth Gate Check") {
    primaryAction = props.onAuthGateCheck;
    disabled = disabled || !props.canRunLiveActions;
  } else if (props.actionLabel === "Complete Pre-Session Review") {
    primaryAction = props.onCompletePreSessionReview;
    disabled = disabled || !props.canRunLiveActions;
  } else {
    disabled = disabled || !props.canRunLiveActions;
  }

  return (
    <div className="startup-panel">
      <div className="badge-row">
        <Badge label={props.stateLabel} tone={statusTone(props.stateLabel)} />
        <Badge label={props.actionLabel} tone={statusTone(props.actionLabel)} />
      </div>
      <div className="metric-grid">
        {props.metrics.map((item) => (
          <MetricCard key={item.label} label={item.label} value={item.value} tone={item.tone} />
        ))}
      </div>
      <div className="status-line">{props.reason}</div>
      <div className="notice-strip">
        <div>{props.actionDescription}</div>
        <div>If enabled temp-paper lanes exist, the runtime start path includes them automatically.</div>
      </div>
      <div className="action-row inline">
        <button className="panel-button" disabled={disabled} onClick={primaryAction}>
          {primaryButtonLabel}
        </button>
      </div>
    </div>
  );
}

function DataTable(props: {
  columns: Array<{ key: string; label: string; render?: (row: JsonRecord) => ReactNode }>;
  rows: JsonRecord[];
  emptyLabel: string;
  onRowClick?: (row: JsonRecord) => void;
  rowKey?: (row: JsonRecord, index: number) => string;
  selectedRowKey?: string;
}) {
  const rows = useMemo(() => props.rows ?? [], [props.rows]);
  if (!rows.length) {
    return <div className="placeholder-note">{props.emptyLabel}</div>;
  }
  return (
    <div className="table-shell">
      <table className="data-table">
        <thead>
          <tr>
            {props.columns.map((column) => (
              <th key={column.key}>{column.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => {
            const resolvedRowKey = props.rowKey ? props.rowKey(row, index) : String(row.id ?? row.lane_id ?? row.trade_id ?? row.symbol ?? index);
            return (
            <tr
              key={resolvedRowKey}
              className={`${props.onRowClick ? "is-clickable" : ""} ${props.selectedRowKey && resolvedRowKey === props.selectedRowKey ? "is-selected" : ""}`.trim()}
              onClick={props.onRowClick ? () => props.onRowClick?.(row) : undefined}
            >
              {props.columns.map((column) => (
                <td key={column.key}>{column.render ? column.render(row) : formatValue(row[column.key])}</td>
              ))}
            </tr>
          );
          })}
        </tbody>
      </table>
    </div>
  );
}

function ReplayStrategyStudy(props: { studies: JsonRecord[] }) {
  const [heightPreset, setHeightPreset] = useState<StudyHeightPreset>("standard");
  const [selectedStudyKey, setSelectedStudyKey] = useState<string>("");
  const [selectedStrategyId, setSelectedStrategyId] = useState<string>("all");
  const [selectedCandidateId, setSelectedCandidateId] = useState<string>("all");
  const [selectedStudyMode, setSelectedStudyMode] = useState<string>("all");
  const [selectedEntryModel, setSelectedEntryModel] = useState<string>("all");
  const [pnlMode, setPnlMode] = useState<StudyPnlMode>("cumulative_total");
  const [showExecutionDetail, setShowExecutionDetail] = useState(true);
  const studies = useMemo(
    () =>
      asArray<JsonRecord>(props.studies)
        .map((item, index) => {
          const study = asRecord(item.study ?? item);
          const meta = asRecord(study.meta);
          return {
            index,
            studyKey: String(item.study_key ?? meta.study_id ?? `study-${index}`),
            label: String(
              item.label ??
                meta.study_id ??
                meta.strategy_id ??
                meta.strategy_family ??
                study.standalone_strategy_id ??
                study.strategy_family ??
                `Study ${index + 1}`,
            ),
            strategyId: String(item.strategy_id ?? meta.strategy_id ?? study.standalone_strategy_id ?? study.strategy_family ?? "").trim(),
            candidateId: String(item.candidate_id ?? meta.candidate_id ?? "").trim(),
            studyMode: String(item.study_mode ?? meta.study_mode ?? "baseline_parity_mode").trim(),
            entryModel: String(item.entry_model ?? meta.entry_model ?? "BASELINE_NEXT_BAR_OPEN").trim(),
            activeEntryModel: String(item.active_entry_model ?? meta.active_entry_model ?? item.entry_model ?? meta.entry_model ?? "BASELINE_NEXT_BAR_OPEN").trim(),
            supportedEntryModels: asArray(item.supported_entry_models ?? meta.supported_entry_models).map((value) => String(value ?? "").trim()).filter(Boolean),
            entryModelSupported:
              item.entry_model_supported === undefined
                ? meta.entry_model_supported !== false
                : item.entry_model_supported === true,
            executionTruthEmitter: String(item.execution_truth_emitter ?? meta.execution_truth_emitter ?? "").trim(),
            lifecycleTruthClass: String(item.lifecycle_truth_class ?? meta.lifecycle_truth_class ?? "").trim(),
            pnlTruthBasis: String(item.pnl_truth_basis ?? meta.pnl_truth_basis ?? "BASELINE_FILL_TRUTH").trim(),
            intrabarExecutionAuthoritative:
              item.intrabar_execution_authoritative === true || meta.intrabar_execution_authoritative === true,
            authoritativeEntryTruthAvailable:
              item.authoritative_entry_truth_available === true || meta.authoritative_entry_truth_available === true,
            authoritativeExitTruthAvailable:
              item.authoritative_exit_truth_available === true || meta.authoritative_exit_truth_available === true,
            authoritativeTradeLifecycleAvailable:
              item.authoritative_trade_lifecycle_available === true || meta.authoritative_trade_lifecycle_available === true,
            truthProvenance: asRecord(item.truth_provenance ?? meta.truth_provenance),
            unsupportedReason: String(item.unsupported_reason ?? meta.unsupported_reason ?? "").trim(),
            scopeLabel: String(item.scope_label ?? "").trim(),
            coverageStart: String(item.coverage_start ?? meta.coverage_start ?? asRecord(meta.coverage_range).start_timestamp ?? "").trim(),
            coverageEnd: String(item.coverage_end ?? meta.coverage_end ?? asRecord(meta.coverage_range).end_timestamp ?? "").trim(),
            item,
            study,
            meta,
          };
        })
        .filter((item) => asArray<JsonRecord>(item.study.bars ?? item.study.rows).length > 0),
    [props.studies],
  );

  const strategyOptions = useMemo(
    () => Array.from(new Set(studies.map((item) => item.strategyId).filter(Boolean))).sort((left, right) => left.localeCompare(right)),
    [studies],
  );
  const candidateOptions = useMemo(
    () => Array.from(new Set(studies.map((item) => item.candidateId).filter(Boolean))).sort((left, right) => left.localeCompare(right)),
    [studies],
  );
  const studyModeOptions = useMemo(
    () => Array.from(new Set(studies.map((item) => item.studyMode).filter(Boolean))).sort((left, right) => left.localeCompare(right)),
    [studies],
  );
  const entryModelOptions = useMemo(
    () => Array.from(new Set(studies.map((item) => item.entryModel).filter(Boolean))).sort((left, right) => left.localeCompare(right)),
    [studies],
  );
  const filteredStudies = useMemo(
    () =>
      studies.filter((item) => {
        if (selectedStrategyId !== "all" && item.strategyId !== selectedStrategyId) {
          return false;
        }
        if (selectedCandidateId !== "all" && item.candidateId !== selectedCandidateId) {
          return false;
        }
        if (selectedStudyMode !== "all" && item.studyMode !== selectedStudyMode) {
          return false;
        }
        if (selectedEntryModel !== "all" && item.entryModel !== selectedEntryModel) {
          return false;
        }
        return true;
      }),
    [selectedCandidateId, selectedEntryModel, selectedStrategyId, selectedStudyMode, studies],
  );
  const unsupportedStudiesForSelectedEntryModel = useMemo(
    () =>
      studies.filter((item) => {
        if (selectedStrategyId !== "all" && item.strategyId !== selectedStrategyId) {
          return false;
        }
        if (selectedCandidateId !== "all" && item.candidateId !== selectedCandidateId) {
          return false;
        }
        if (selectedStudyMode !== "all" && item.studyMode !== selectedStudyMode) {
          return false;
        }
        if (selectedEntryModel === "all") {
          return false;
        }
        return !item.supportedEntryModels.includes(selectedEntryModel);
      }),
    [selectedCandidateId, selectedEntryModel, selectedStrategyId, selectedStudyMode, studies],
  );

  useEffect(() => {
    if (!filteredStudies.length) {
      setSelectedStudyKey("");
      return;
    }
    if (!selectedStudyKey || !filteredStudies.some((item) => item.studyKey === selectedStudyKey)) {
      setSelectedStudyKey(filteredStudies[0].studyKey);
    }
  }, [filteredStudies, selectedStudyKey]);

  const selectedStudyItem = filteredStudies.find((item) => item.studyKey === selectedStudyKey) ?? filteredStudies[0] ?? null;
  const selectedStudy = selectedStudyItem?.study ?? {};
  const meta = asRecord(selectedStudy.meta);
  const summary = asRecord(selectedStudy.summary);
  const atpSummary = asRecord(summary.atp_summary);
  const timeframeTruth = asRecord(meta.timeframe_truth);
  const availableBars = useMemo(
    () =>
      asArray<JsonRecord>(selectedStudy.bars ?? selectedStudy.rows).map((row) => ({
        barId: String(row.bar_id ?? ""),
        timestamp: String(row.timestamp ?? ""),
        startTimestamp: String(row.start_timestamp ?? ""),
        endTimestamp: String(row.end_timestamp ?? row.timestamp ?? ""),
        open: studyNumber(row.open),
        high: studyNumber(row.high),
        low: studyNumber(row.low),
        close: studyNumber(row.close),
        vwap: studyNumber(row.session_vwap),
        realized: studyNumber(row.cumulative_realized_pnl),
        openPnl: studyNumber(row.unrealized_pnl),
        total: studyNumber(row.cumulative_total_pnl),
        positionSide: String(row.position_side ?? "FLAT").toUpperCase(),
        entryMarker: row.entry_marker === true,
        exitMarker: row.exit_marker === true,
        fillMarker: row.fill_marker === true,
        entryEligible: row.entry_eligible === true,
        entryBlocked: row.entry_blocked === true,
        blockerCode: String(row.blocker_code ?? "").trim(),
        biasState: String(row.current_bias_state ?? "").trim(),
        pullbackState: String(row.current_pullback_state ?? "").trim(),
        continuationState: String(row.continuation_state ?? "").trim(),
        positionPhase: String(row.position_phase ?? "").trim(),
        legacyEntryEligible: row.legacy_entry_eligible === true || row.entry_eligible === true,
        legacyEntryBlocked: row.legacy_entry_blocked === true || row.entry_blocked === true,
        legacyBlockerCode: String(row.legacy_blocker_code ?? row.blocker_code ?? "").trim(),
        atpEntryState: String(row.atp_entry_state ?? "").trim(),
        atpEntryReady: row.atp_entry_ready === true,
        atpEntryBlocked: row.atp_entry_blocked === true,
        atpEntryBlockerCode: String(row.atp_entry_blocker_code ?? "").trim(),
        atpTimingState: String(row.atp_timing_state ?? "").trim(),
        atpTimingConfirmed: row.atp_timing_confirmed === true,
        atpTimingExecutable: row.atp_timing_executable === true,
        atpTimingBlockerCode: String(row.atp_timing_blocker_code ?? "").trim(),
        atpBlockerCode: String(row.atp_blocker_code ?? "").trim(),
        vwapEntryQualityState: String(row.vwap_entry_quality_state ?? "").trim(),
      })),
    [selectedStudy],
  );
  const pnlPoints = useMemo(
    () =>
      asArray<JsonRecord>(selectedStudy.pnl_points).map((point) => ({
        pointId: String(point.point_id ?? ""),
        barId: String(point.bar_id ?? ""),
        timestamp: String(point.timestamp ?? ""),
        realized: studyNumber(point.cumulative_realized),
        openPnl: studyNumber(point.unrealized_pnl),
        total: studyNumber(point.cumulative_total),
      })),
    [selectedStudy.pnl_points],
  );
  const tradeEvents = useMemo(
    () =>
      asArray<JsonRecord>(selectedStudy.trade_events).map((event) => ({
        eventId: String(event.event_id ?? ""),
        linkedBarId: String(event.linked_bar_id ?? ""),
        linkedSubbarId: String(event.linked_subbar_id ?? ""),
        eventType: String(event.event_type ?? ""),
        executionEventType: String(event.execution_event_type ?? ""),
        side: String(event.side ?? ""),
        family: String(event.family ?? ""),
        reason: String(event.reason ?? ""),
        sourceResolution: String(event.source_resolution ?? "BAR_CONTEXT"),
        decisionContextTimestamp: String(event.decision_context_timestamp ?? ""),
        eventTimestamp: String(event.event_timestamp ?? ""),
        entryModel: String(event.entry_model ?? meta.entry_model ?? ""),
        eventPrice: studyNumber(event.event_price),
        vwapAtEvent: studyNumber(event.vwap_at_event),
        acceptanceState: String(event.acceptance_state ?? ""),
        invalidationReason: String(event.invalidation_reason ?? ""),
        truthAuthority: String(event.truth_authority ?? ""),
      })),
    [meta.entry_model, selectedStudy.trade_events],
  );
  const executionSlices = useMemo(
    () =>
      asArray<JsonRecord>(selectedStudy.execution_slices).map((slice) => ({
        sliceId: String(slice.slice_id ?? ""),
        linkedBarId: String(slice.linked_bar_id ?? ""),
        timestamp: String(slice.timestamp ?? ""),
        startTimestamp: String(slice.start_timestamp ?? ""),
        endTimestamp: String(slice.end_timestamp ?? slice.timestamp ?? ""),
        close: studyNumber(slice.close),
        high: studyNumber(slice.high),
        low: studyNumber(slice.low),
      })),
    [selectedStudy.execution_slices],
  );

  const minAvailableDate = availableBars[0] ? studyDateLabel(availableBars[0].timestamp) : "";
  const maxAvailableDate = availableBars.length ? studyDateLabel(availableBars[availableBars.length - 1].timestamp) : "";
  const [rangeStart, setRangeStart] = useState("");
  const [rangeEnd, setRangeEnd] = useState("");

  useEffect(() => {
    setRangeStart(minAvailableDate);
    setRangeEnd(maxAvailableDate);
  }, [selectedStudyKey, minAvailableDate, maxAvailableDate]);

  const effectiveRangeStart = rangeStart || minAvailableDate;
  const effectiveRangeEnd = rangeEnd || maxAvailableDate;
  const bars = useMemo(
    () => availableBars.filter((row) => studyDateInRange(row.timestamp, effectiveRangeStart, effectiveRangeEnd)),
    [availableBars, effectiveRangeEnd, effectiveRangeStart],
  );
  const pnlPointsInRange = useMemo(
    () => pnlPoints.filter((point) => studyDateInRange(point.timestamp, effectiveRangeStart, effectiveRangeEnd)),
    [effectiveRangeEnd, effectiveRangeStart, pnlPoints],
  );
  const eventsInRange = useMemo(
    () => tradeEvents.filter((event) => studyDateInRange(event.eventTimestamp || event.decisionContextTimestamp, effectiveRangeStart, effectiveRangeEnd)),
    [effectiveRangeEnd, effectiveRangeStart, tradeEvents],
  );
  const executionSlicesInRange = useMemo(
    () => executionSlices.filter((slice) => studyDateInRange(slice.timestamp, effectiveRangeStart, effectiveRangeEnd)),
    [effectiveRangeEnd, effectiveRangeStart, executionSlices],
  );

  if (!selectedStudyItem || !availableBars.length) {
    return (
      <div className="placeholder-note">
        {selectedEntryModel !== "all" && unsupportedStudiesForSelectedEntryModel.length
          ? `No loaded studies support ${selectedEntryModel} for the current strategy / candidate / mode filters.`
          : "No strategy-study rows are available for this replay run."}
      </div>
    );
  }

  if (!bars.length) {
    return (
      <div className="study-surface study-empty-surface">
        <div className="study-toolbar">
          <div className="study-workbench-controls">
            <label className="study-select-field">
              <span>Strategy</span>
              <select value={selectedStrategyId} onChange={(event) => setSelectedStrategyId(event.target.value)}>
                <option value="all">All strategies</option>
                {strategyOptions.map((option) => (
                  <option key={option} value={option}>{option}</option>
                ))}
              </select>
            </label>
            <label className="study-select-field">
              <span>Candidate</span>
              <select value={selectedCandidateId} onChange={(event) => setSelectedCandidateId(event.target.value)}>
                <option value="all">All candidates</option>
                {candidateOptions.map((option) => (
                  <option key={option} value={option}>{option}</option>
                ))}
              </select>
            </label>
            <label className="study-select-field">
              <span>Lane</span>
              <select value={selectedStudyMode} onChange={(event) => setSelectedStudyMode(event.target.value)}>
                <option value="all">All modes</option>
                {studyModeOptions.map((option) => (
                  <option key={option} value={option}>{option}</option>
                ))}
              </select>
            </label>
            <label className="study-select-field">
              <span>Entry Model</span>
              <select value={selectedEntryModel} onChange={(event) => setSelectedEntryModel(event.target.value)}>
                <option value="all">All entry models</option>
                {entryModelOptions.map((option) => (
                  <option key={option} value={option}>{option}</option>
                ))}
              </select>
            </label>
            <label className="study-select-field">
              <span>Study</span>
              <select value={selectedStudyKey} onChange={(event) => setSelectedStudyKey(event.target.value)}>
                {filteredStudies.map((item) => (
                  <option key={item.studyKey} value={item.studyKey}>{item.label}</option>
                ))}
              </select>
            </label>
          </div>
        </div>
          <div className="notice-strip strategy-study-empty">
          <div>{filteredStudies.length ? `${filteredStudies.length} filtered studies found.` : "No studies match the current strategy / candidate / mode filters."}</div>
          {selectedEntryModel !== "all" && unsupportedStudiesForSelectedEntryModel.length ? (
            <div>
              {`${unsupportedStudiesForSelectedEntryModel.length} filtered studies do not support ${selectedEntryModel}. Unsupported combinations stay explicit and are not silently normalized.`}
            </div>
          ) : null}
          <div>No bars fall inside the selected date range.</div>
          <div>Reset the visible range to {minAvailableDate || "the full study"} through {maxAvailableDate || "the end of the run"} to restore the chart.</div>
        </div>
      </div>
    );
  }

  const eventsByBarId = new Map<string, Array<(typeof tradeEvents)[number]>>();
  eventsInRange.forEach((event) => {
    const key = event.linkedBarId;
    if (!key) {
      return;
    }
    const bucket = eventsByBarId.get(key) ?? [];
    bucket.push(event);
    eventsByBarId.set(key, bucket);
  });
  const pnlByBarId = new Map(pnlPointsInRange.map((point) => [point.barId, point]));
  const hasExecutionDetail = executionSlices.length > 0;
  const hasAtpData = bars.some((row) => row.biasState || row.pullbackState || row.atpEntryState || row.atpTimingState || row.continuationState);
  const hasPositionPhase = bars.some((row) => row.positionPhase);
  const layout = resolveStudyLayout(bars.length, heightPreset);
  const width = layout.surfaceWidth;
  const marginLeft = 58;
  const marginRight = 18;
  const panePaddingTop = layout.panePaddingTop;
  const panePaddingBottom = layout.panePaddingBottom;
  const topPaneHeight = layout.topPaneHeight;
  const lowerPaneHeight = layout.lowerPaneHeight;
  const paneGap = layout.paneGap;
  const executionStripHeight = showExecutionDetail && hasExecutionDetail ? 62 : 0;
  const totalHeight = topPaneHeight + lowerPaneHeight + paneGap;
  const svgHeight = totalHeight + 26;
  const plotWidth = Math.max(width - marginLeft - marginRight, 1);
  const topPlotHeight = topPaneHeight - panePaddingTop - panePaddingBottom - (executionStripHeight ? executionStripHeight + 12 : 0);
  const executionStripTop = panePaddingTop + topPlotHeight + 12;
  const lowerPlotHeight = lowerPaneHeight - panePaddingTop - panePaddingBottom;
  const priceRows = bars.filter((row) => row.high !== null && row.low !== null && row.open !== null && row.close !== null);
  const priceValues = priceRows.flatMap((row) => [row.high as number, row.low as number, row.vwap].filter((value): value is number => value !== null));
  const priceMinRaw = priceValues.length ? Math.min(...priceValues) : 0;
  const priceMaxRaw = priceValues.length ? Math.max(...priceValues) : 1;
  const pricePadding = Math.max((priceMaxRaw - priceMinRaw) * 0.08, 0.25);
  const priceMin = priceMinRaw - pricePadding;
  const priceMax = priceMaxRaw + pricePadding;
  const pnlSeries = bars.map((row) => studyPnlValue(pnlMode, pnlByBarId.get(row.barId), row));
  const pnlValues = pnlSeries.filter((value): value is number => value !== null);
  const pnlMinRaw = pnlValues.length ? Math.min(...pnlValues, 0) : -1;
  const pnlMaxRaw = pnlValues.length ? Math.max(...pnlValues, 0) : 1;
  const pnlPadding = Math.max((pnlMaxRaw - pnlMinRaw) * 0.12, 1);
  const pnlMin = pnlMinRaw - pnlPadding;
  const pnlMax = pnlMaxRaw + pnlPadding;
  const legacyBlockerSummary = asArray<JsonRecord>(summary.most_common_legacy_blocker_codes ?? summary.most_common_blocker_codes)
    .slice(0, 3)
    .map((row) => `${formatValue(row.code)} (${formatShortNumber(row.count)})`)
    .join(" • ");
  const atpBlockerSummary = asArray<JsonRecord>(atpSummary.top_atp_blocker_codes)
    .slice(0, 3)
    .map((row) => `${formatValue(row.code)} (${formatShortNumber(row.count)})`)
    .join(" • ");
  const tickIndices = studyTickIndices(bars.length, 6);
  const xForIndex = (index: number) => marginLeft + (plotWidth * (bars.length <= 1 ? 0.5 : index / (bars.length - 1)));
  const priceY = (value: number) => panePaddingTop + ((priceMax - value) / Math.max(priceMax - priceMin, 0.0001)) * topPlotHeight;
  const lowerTop = topPaneHeight + paneGap;
  const pnlY = (value: number) => lowerTop + panePaddingTop + ((pnlMax - value) / Math.max(pnlMax - pnlMin, 0.0001)) * lowerPlotHeight;
  const zeroY = pnlY(0);
  const vwapPath = buildStudyPath(
    bars.map((row, index) => ({
      x: xForIndex(index),
      y: row.vwap === null ? null : priceY(row.vwap),
    })),
  );
  const pnlPath = buildStudyPath(
    bars.map((row, index) => {
      const value = studyPnlValue(pnlMode, pnlByBarId.get(row.barId), row);
      return { x: xForIndex(index), y: value === null ? null : pnlY(value) };
    }),
  );
  const candleWidth = Math.max(4, Math.min(12, (plotWidth / Math.max(bars.length, 1)) * 0.65));
  const executionCoords = buildExecutionSliceCoords({
    bars,
    executionSlices: executionSlicesInRange,
    xForIndex,
  });
  const executionValues = executionCoords.flatMap((item) => [item.high, item.low, item.close].filter((value): value is number => value !== null));
  const executionMinRaw = executionValues.length ? Math.min(...executionValues) : priceMinRaw;
  const executionMaxRaw = executionValues.length ? Math.max(...executionValues) : priceMaxRaw;
  const executionPadding = Math.max((executionMaxRaw - executionMinRaw) * 0.15, 0.12);
  const executionMin = executionMinRaw - executionPadding;
  const executionMax = executionMaxRaw + executionPadding;
  const executionY = (value: number) =>
    executionStripTop + ((executionMax - value) / Math.max(executionMax - executionMin, 0.0001)) * Math.max(executionStripHeight - 10, 1);
  const executionPath = buildStudyPath(
    executionCoords.map((item) => ({
      x: item.x,
      y: item.close === null ? null : executionY(item.close),
    })),
  );
  const firstVisibleTimestamp = bars[0]?.timestamp ?? "";
  const carryInBar =
    firstVisibleTimestamp
      ? [...availableBars].reverse().find((row) => String(row.timestamp) < firstVisibleTimestamp) ?? null
      : null;
  const carryInPnlPoint =
    firstVisibleTimestamp
      ? [...pnlPoints].reverse().find((point) => String(point.timestamp) < firstVisibleTimestamp) ?? null
      : null;
  const carryInPositionActive = !!carryInBar && carryInBar.positionSide !== "FLAT";
  const carryInRealized = carryInPnlPoint?.realized ?? carryInBar?.realized ?? null;
  const carryInTotal = carryInPnlPoint?.total ?? carryInBar?.total ?? null;
  const carryInSummary = carryInPositionActive || carryInRealized !== null || carryInTotal !== null
    ? [
        carryInPositionActive ? `Carry-in ${carryInBar?.positionSide} position` : "No carry-in position",
        carryInRealized !== null ? `Closed baseline ${formatMaybePnL(carryInRealized)}` : null,
        carryInTotal !== null ? `Total baseline ${formatMaybePnL(carryInTotal)}` : null,
      ]
        .filter((value): value is string => Boolean(value))
        .join(" • ")
    : "No carry-in state before the visible range";
  const studyEventCoords = buildStudyEventCoords({
    bars,
    tradeEvents: eventsInRange,
    executionCoords,
    xForIndex,
    topMarkerY: 36,
    intrabarMarkerY: executionStripHeight > 0 ? executionStripTop - 12 : 36,
  });
  const biasPercent = asRecord(atpSummary.bias_state_percent);
  const pullbackPercent = asRecord(atpSummary.pullback_state_percent);
  const timingAvailable = atpSummary.timing_available === true;
  const biasMixSummary = `${formatShortNumber(studyNumber(biasPercent.LONG_BIAS) ?? 0)} / ${formatShortNumber(studyNumber(biasPercent.SHORT_BIAS) ?? 0)} / ${formatShortNumber(studyNumber(biasPercent.NEUTRAL) ?? 0)}`;
  const pullbackMixSummary = `${formatShortNumber(studyNumber(pullbackPercent.NORMAL_PULLBACK) ?? 0)} / ${formatShortNumber(studyNumber(pullbackPercent.STRETCHED_PULLBACK) ?? 0)} / ${formatShortNumber(studyNumber(pullbackPercent.VIOLENT_PULLBACK_DISQUALIFY) ?? 0)} / ${formatShortNumber(studyNumber(pullbackPercent.NO_PULLBACK) ?? 0)}`;
  const conversionSummary = timingAvailable
    ? `${formatShortNumber(studyNumber(atpSummary.ready_to_timing_confirmed_percent) ?? 0)}% -> ${formatShortNumber(studyNumber(atpSummary.timing_confirmed_to_executed_percent) ?? 0)}%`
    : "Timing unavailable";
  const studySurfaceStyle = {
    "--study-shell-min-height": `${layout.shellMinHeight}px`,
    "--study-pane-price-min-height": `${layout.topPaneHeight}px`,
    "--study-pane-lower-min-height": `${layout.lowerPaneHeight}px`,
  } as CSSProperties;

  return (
    <div className={`study-surface study-surface-${heightPreset} study-workstation`} style={studySurfaceStyle}>
      <div className="study-toolbar">
        <div className="study-workbench-controls">
          <label className="study-select-field">
            <span>Study</span>
            <select value={selectedStudyKey} onChange={(event) => setSelectedStudyKey(event.target.value)}>
              {filteredStudies.map((item) => (
                <option key={item.studyKey} value={item.studyKey}>{item.label}</option>
              ))}
            </select>
          </label>
          <label className="study-select-field">
            <span>Entry Model</span>
            <select value={selectedEntryModel} onChange={(event) => setSelectedEntryModel(event.target.value)}>
              <option value="all">All entry models</option>
              {entryModelOptions.map((option) => (
                <option key={option} value={option}>{option}</option>
              ))}
            </select>
          </label>
          <label className="study-select-field study-date-field">
            <span>Start</span>
            <input type="date" value={effectiveRangeStart} min={minAvailableDate} max={effectiveRangeEnd || maxAvailableDate} onChange={(event) => setRangeStart(event.target.value)} />
          </label>
          <label className="study-select-field study-date-field">
            <span>End</span>
            <input type="date" value={effectiveRangeEnd} min={effectiveRangeStart || minAvailableDate} max={maxAvailableDate} onChange={(event) => setRangeEnd(event.target.value)} />
          </label>
          <button type="button" className="study-ghost-button" onClick={() => {
            setRangeStart(minAvailableDate);
            setRangeEnd(maxAvailableDate);
          }}>
            Max Range
          </button>
        </div>
        <div className="study-toolbar-actions">
          <div className="study-mode-control" role="group" aria-label="Strategy study pnl mode">
            {([
              ["cumulative_total", "Total"],
              ["cumulative_realized", "Closed"],
              ["unrealized", "Open"],
            ] as const).map(([mode, label]) => (
              <button
                key={mode}
                type="button"
                className={`study-size-button ${pnlMode === mode ? "active" : ""}`}
                onClick={() => setPnlMode(mode)}
              >
                {label}
              </button>
            ))}
          </div>
          <div className="study-size-control" role="group" aria-label="Strategy study size">
            {(["compact", "standard", "expanded"] as const).map((preset) => (
              <button
                key={preset}
                type="button"
                className={`study-size-button ${heightPreset === preset ? "active" : ""}`}
                onClick={() => setHeightPreset(preset)}
              >
                {sentenceCase(preset)}
              </button>
            ))}
          </div>
          <label className="study-toggle">
            <input
              type="checkbox"
              checked={showExecutionDetail}
              disabled={!hasExecutionDetail}
              onChange={(event) => setShowExecutionDetail(event.target.checked)}
            />
            <span>Execution detail</span>
          </label>
        </div>
      </div>
      <div className="study-meta-strip">
        <span><strong>Strategy</strong> {formatValue(meta.strategy_id ?? selectedStudy.standalone_strategy_id ?? selectedStudy.strategy_family)}</span>
        <span><strong>Candidate</strong> {formatValue(selectedStudyItem.candidateId || meta.candidate_id || "—")}</span>
        <span><strong>Scope</strong> {formatValue(selectedStudyItem.scopeLabel || meta.study_mode || "Legacy Benchmark")}</span>
        <span><strong>Run</strong> {formatValue(selectedStudyItem.item.run_stamp ?? selectedStudy.run_metadata?.run_stamp)}</span>
        <span><strong>Mode</strong> {formatValue(meta.study_mode ?? "baseline_parity_mode")}</span>
        <span><strong>Entry Model</strong> {formatValue(meta.active_entry_model ?? meta.entry_model ?? selectedStudyItem.activeEntryModel ?? selectedStudyItem.entryModel ?? "BASELINE_NEXT_BAR_OPEN")}</span>
        <span><strong>Emitter</strong> {formatValue(meta.execution_truth_emitter ?? selectedStudyItem.executionTruthEmitter ?? "baseline_parity_emitter")}</span>
        <span><strong>Lifecycle</strong> {formatValue(meta.lifecycle_truth_class ?? selectedStudyItem.lifecycleTruthClass ?? "BASELINE_PARITY_ONLY")}</span>
        <span><strong>Entry Support</strong> {formatValue(meta.entry_model_supported === false ? "Unsupported" : "Supported")}</span>
        <span><strong>Structural</strong> {formatValue(timeframeTruth.structural_signal_timeframe ?? meta.context_resolution ?? selectedStudy.timeframe)}</span>
        <span><strong>Execution</strong> {formatValue(timeframeTruth.execution_timeframe ?? meta.execution_resolution ?? (hasExecutionDetail ? "Available" : "None"))}</span>
        <span><strong>Artifact</strong> {formatValue(timeframeTruth.artifact_timeframe ?? selectedStudy.timeframe)}</span>
        <span><strong>Execution Role</strong> {formatValue(timeframeTruth.execution_timeframe_role ?? "matches_signal_evaluation")}</span>
        <span><strong>Intrabar Truth</strong> {formatValue(meta.intrabar_execution_authoritative === true ? "Authoritative" : "Not authoritative")}</span>
        <span><strong>Entry Truth</strong> {formatValue(meta.authoritative_entry_truth_available === true ? "Authoritative" : "Baseline/none")}</span>
        <span><strong>Exit Truth</strong> {formatValue(meta.authoritative_exit_truth_available === true ? "Authoritative" : "Baseline/none")}</span>
        <span><strong>Lifecycle Records</strong> {formatValue(meta.authoritative_trade_lifecycle_available === true ? "Authoritative" : "Not available")}</span>
        <span><strong>P&amp;L Basis</strong> {formatValue(meta.pnl_truth_basis ?? selectedStudyItem.pnlTruthBasis ?? "BASELINE_FILL_TRUTH")}</span>
        <span><strong>Provenance</strong> {formatValue(asRecord(meta.truth_provenance).run_lane ?? selectedStudyItem.truthProvenance.run_lane ?? "UNKNOWN")}</span>
        <span><strong>Supported Models</strong> {formatValue(asArray(meta.supported_entry_models ?? selectedStudyItem.supportedEntryModels).join(", ") || "BASELINE_NEXT_BAR_OPEN")}</span>
        <span><strong>Coverage</strong> {formatValue(selectedStudyItem.coverageStart || meta.coverage_start || "—")} {"->"} {formatValue(selectedStudyItem.coverageEnd || meta.coverage_end || "—")}</span>
        <span><strong>Visible</strong> {bars.length} / {availableBars.length} bars</span>
      </div>
      <div className="study-summary-grid compact">
        <div className="study-summary-card">
          <div className="metric-label">Visible P&amp;L</div>
          <div className="metric-value">{formatMaybePnL(studyPnlValue(pnlMode, pnlByBarId.get(bars[bars.length - 1]?.barId), bars[bars.length - 1]))}</div>
        </div>
        <div className="study-summary-card">
          <div className="metric-label">Carry-In</div>
          <div className="metric-value">{carryInSummary}</div>
        </div>
        <div className="study-summary-card">
          <div className="metric-label">Trades</div>
          <div className="metric-value">{formatShortNumber(summary.total_trades)}</div>
        </div>
        <div className="study-summary-card">
          <div className="metric-label">Legacy Blockers</div>
          <div className="metric-value">{legacyBlockerSummary || "None"}</div>
        </div>
        <div className="study-summary-card">
          <div className="metric-label">ATP Bias</div>
          <div className="metric-value">{biasMixSummary}</div>
        </div>
        <div className="study-summary-card">
          <div className="metric-label">ATP Pullback</div>
          <div className="metric-value">{pullbackMixSummary}</div>
        </div>
        <div className="study-summary-card">
          <div className="metric-label">ATP Timing</div>
          <div className="metric-value">{conversionSummary}</div>
        </div>
        <div className="study-summary-card">
          <div className="metric-label">ATP Blockers</div>
          <div className="metric-value">{atpBlockerSummary || "None"}</div>
        </div>
      </div>
      <div className="study-chart-shell">
        <div className="study-chart-scroll">
          <svg
            className="study-svg"
            viewBox={`0 0 ${width} ${svgHeight}`}
            width={width}
            height={svgHeight}
            style={{ width: `${width}px`, height: `${svgHeight}px` }}
            role="img"
            aria-label="Replay strategy study workstation"
          >
            <rect x={0} y={0} width={width} height={topPaneHeight} rx={16} className="study-pane study-pane-price" />
            <rect x={0} y={lowerTop} width={width} height={lowerPaneHeight} rx={16} className="study-pane study-pane-lower" />
            <line x1={marginLeft} y1={zeroY} x2={width - marginRight} y2={zeroY} className="study-zero-line" />
            <text x={18} y={36} className="study-pane-label">Price / Decisions</text>
            <text x={18} y={lowerTop + 36} className="study-pane-label">Rolling P&amp;L</text>

            {tickIndices.map((index) => (
              <g key={`grid-${index}`}>
                <line x1={xForIndex(index)} y1={14} x2={xForIndex(index)} y2={totalHeight - 8} className="study-grid-line" />
                <text x={xForIndex(index)} y={totalHeight + 14} className="study-axis-label" textAnchor="middle">
                  {studyAxisLabel(bars[index]?.timestamp)}
                </text>
              </g>
            ))}

            {bars.map((row, index) => {
              const currentX = xForIndex(index);
              const nextX = xForIndex(Math.min(index + 1, bars.length - 1));
              const bandWidth = Math.max(nextX - currentX, 8);
              return (
                <rect
                  key={`bias-${row.barId || index}`}
                  x={currentX - bandWidth / 2}
                  y={10}
                  width={bandWidth}
                  height={10}
                  className={`study-bias-strip ${studyBiasClass(row.biasState)}`}
                />
              );
            })}

            {bars.map((row, index) => {
              if (row.positionSide === "FLAT") {
                return null;
              }
              const nextX = xForIndex(Math.min(index + 1, bars.length - 1));
              const currentX = xForIndex(index);
              const bandWidth = Math.max(nextX - currentX, 8);
              return (
                <rect
                  key={`band-${row.barId || index}`}
                  x={currentX - bandWidth / 2}
                  y={lowerTop + 8}
                  width={bandWidth}
                  height={lowerPaneHeight - 16}
                  className={row.positionSide === "LONG" ? "study-position-band long" : "study-position-band short"}
                />
              );
            })}

            {bars.map((row, index) => {
              if (row.high === null || row.low === null || row.open === null || row.close === null) {
                return null;
              }
              const x = xForIndex(index);
              const openY = priceY(row.open);
              const closeY = priceY(row.close);
              const highY = priceY(row.high);
              const lowY = priceY(row.low);
              const candleY = Math.min(openY, closeY);
              const candleHeight = Math.max(Math.abs(closeY - openY), 2);
              const toneClass = row.close >= row.open ? "up" : "down";
              const tooltip = studyBarTooltip(row, eventsByBarId.get(row.barId) ?? []);
              return (
                <g key={`candle-${row.barId || index}`}>
                  <title>{tooltip}</title>
                  <line x1={x} y1={highY} x2={x} y2={lowY} className={`study-wick ${toneClass}`} />
                  <rect x={x - candleWidth / 2} y={candleY} width={candleWidth} height={candleHeight} className={`study-candle ${toneClass}`} rx={3} />
                  {row.fillMarker ? <circle cx={x} cy={closeY} r={3} className="study-marker fill" /> : null}
                  {row.continuationState === "CONTINUATION_TRIGGER_CONFIRMED" ? (
                    <rect x={x - 4} y={22} width={8} height={8} className="study-marker continuation" transform={`rotate(45 ${x} 26)`} />
                  ) : null}
                  {row.atpTimingConfirmed ? <circle cx={x} cy={18} r={3} className="study-marker atp-confirmed" /> : null}
                </g>
              );
            })}

            {vwapPath ? <path d={vwapPath} className="study-line vwap" /> : null}
            {pnlPath ? <path d={pnlPath} className={`study-line ${studyPnlClass(pnlMode)}`} /> : null}

            {studyEventCoords.map((event) => (
              <g key={`trade-event-${event.eventId}`} className={`study-trade-event ${studyEventSourceClass(event.sourceResolution)}`}>
                <title>{studyEventTooltip(event)}</title>
                {studyEventShape(event.eventType) === "diamond" ? (
                  <rect
                    x={event.x - 4}
                    y={event.y - 4}
                    width={8}
                    height={8}
                    className={`study-event-marker ${studyEventToneClass(event.eventType)} ${studyEventSourceClass(event.sourceResolution)}`}
                    transform={`rotate(45 ${event.x} ${event.y})`}
                  />
                ) : studyEventShape(event.eventType) === "square" ? (
                  <rect
                    x={event.x - 4}
                    y={event.y - 4}
                    width={8}
                    height={8}
                    rx={2}
                    className={`study-event-marker ${studyEventToneClass(event.eventType)} ${studyEventSourceClass(event.sourceResolution)}`}
                  />
                ) : (
                  <circle
                    cx={event.x}
                    cy={event.y}
                    r={3.8}
                    className={`study-event-marker ${studyEventToneClass(event.eventType)} ${studyEventSourceClass(event.sourceResolution)}`}
                  />
                )}
              </g>
            ))}

            {showExecutionDetail && executionStripHeight > 0 ? (
              <>
                <rect x={marginLeft} y={executionStripTop - 6} width={plotWidth} height={executionStripHeight + 6} rx={10} className="study-execution-strip" />
                <text x={width - marginRight} y={executionStripTop - 10} className="study-axis-label" textAnchor="end">Execution detail</text>
                {executionPath ? <path d={executionPath} className="study-line execution" /> : null}
                {executionCoords.map((slice) =>
                  slice.close === null || slice.high === null || slice.low === null ? null : (
                    <g key={slice.sliceId}>
                      <title>{`Execution slice ${formatTimestamp(slice.timestamp)} | Linked bar ${slice.linkedBarId}`}</title>
                      <line x1={slice.x} y1={executionY(slice.high)} x2={slice.x} y2={executionY(slice.low)} className="study-execution-wick" />
                      <circle cx={slice.x} cy={executionY(slice.close)} r={1.8} className="study-execution-point" />
                    </g>
                  ),
                )}
              </>
            ) : null}

            {bars.map((row, index) => {
              const currentX = xForIndex(index);
              const nextX = xForIndex(Math.min(index + 1, bars.length - 1));
              const bandWidth = Math.max(nextX - currentX, 8);
              return (
                <rect
                  key={`pullback-${row.barId || index}`}
                  x={currentX - bandWidth / 2}
                  y={lowerTop + 10}
                  width={bandWidth}
                  height={10}
                  className={`study-pullback-band ${studyPullbackClass(row.pullbackState)}`}
                />
              );
            })}

            {bars.map((row, index) => {
              const x = xForIndex(index);
              const pnlValue = studyPnlValue(pnlMode, pnlByBarId.get(row.barId), row);
              const pointY = pnlValue === null ? null : pnlY(pnlValue);
              const rowEvents = eventsByBarId.get(row.barId) ?? [];
              const hasEntryEvent = rowEvents.some((event) =>
                event.eventType.includes("ENTRY_EXECUTED") || event.eventType.includes("ENTRY_FILL"),
              );
              const hasExitEvent = rowEvents.some((event) =>
                event.eventType.includes("EXIT_EXECUTED") || event.eventType.includes("EXIT_FILL"),
              );
              return (
                <g key={`event-${row.barId || index}`}>
                  {row.legacyEntryEligible ? <rect x={x - 2} y={lowerTop + lowerPaneHeight - 76} width={4} height={10} rx={2} className="study-event eligible" /> : null}
                  {row.legacyEntryBlocked ? <rect x={x - 2} y={lowerTop + lowerPaneHeight - 92} width={4} height={10} rx={2} className="study-event blocked" /> : null}
                  {row.atpEntryReady ? <circle cx={x} cy={lowerTop + lowerPaneHeight - 50} r={4} className="study-event atp-ready" /> : null}
                  {row.atpEntryBlocked ? <circle cx={x} cy={lowerTop + lowerPaneHeight - 50} r={4} className="study-event atp-blocked" /> : null}
                  {row.atpTimingState ? <circle cx={x} cy={lowerTop + lowerPaneHeight - 28} r={4} className={`study-event ${studyTimingClass(row.atpTimingState)}`} /> : null}
                  {hasPositionPhase && row.positionPhase ? <rect x={x - 3} y={lowerTop + 30} width={6} height={6} rx={2} className="study-event position-phase" /> : null}
                  {pointY !== null && hasEntryEvent ? <circle cx={x} cy={pointY} r={3} className="study-marker entry" /> : null}
                  {pointY !== null && hasExitEvent ? <circle cx={x} cy={pointY} r={3} className="study-marker exit" /> : null}
                </g>
              );
            })}
            {carryInPositionActive ? (
              <g className="study-carry-in-marker">
                <title>{carryInSummary}</title>
                <line x1={marginLeft} y1={lowerTop + 18} x2={marginLeft} y2={lowerTop + lowerPaneHeight - 18} className="study-carry-in-line" />
                <circle cx={marginLeft} cy={lowerTop + 18} r={4} className={carryInBar?.positionSide === "LONG" ? "study-marker entry" : "study-marker exit"} />
              </g>
            ) : null}
          </svg>
        </div>
      </div>
      <div className="status-line">
        {meta.unsupported_reason ? `Unsupported entry-model combination: ${formatValue(meta.unsupported_reason)}` : hasAtpData
          ? "Bar-context strategy state stays separate from execution-detail truth. Tooltips label source resolution, decision-context time, and event time so replay bars and intrabar slices are not collapsed into one timestamp."
          : "This study currently exposes bar-context truth only. Execution-detail controls stay visible, but intrabar overlays remain omitted until the artifact publishes execution slices."}
      </div>
    </div>
  );
}

type StrategyAnalysisTab = "results" | "compare" | "study" | "runtime" | "diagnostics";

function UnifiedStrategyAnalysis({
  analysis,
  replayStudyItems,
  preferredStrategyKey,
  studyPanel,
  runtimePanel,
  diagnosticsPanel,
}: {
  analysis: JsonRecord;
  replayStudyItems: JsonRecord[];
  preferredStrategyKey?: string;
  studyPanel?: ReactNode;
  runtimePanel?: ReactNode;
  diagnosticsPanel?: ReactNode;
}) {
  const resultsBoard = asRecord(analysis.results_board);
  const catalogRows = asArray<JsonRecord>(asRecord(analysis.catalog).rows);
  const detailsByStrategyKey = asRecord(analysis.details_by_strategy_key);
  const allLanes = useMemo(
    () =>
      Object.values(detailsByStrategyKey).flatMap((detail) => asArray<JsonRecord>(asRecord(detail).lanes)),
    [detailsByStrategyKey],
  );
  const laneById = useMemo(() => {
    const lookup = new Map<string, JsonRecord>();
    allLanes.forEach((lane) => {
      const key = String(lane.lane_id ?? "");
      if (key && !lookup.has(key)) {
        lookup.set(key, lane);
      }
    });
    return lookup;
  }, [allLanes]);
  const replayStudyItemByKey = useMemo(() => {
    const lookup = new Map<string, JsonRecord>();
    for (const item of replayStudyItems) {
      const studyKey = String(item.study_key ?? "").trim();
      if (studyKey && !lookup.has(studyKey)) {
        lookup.set(studyKey, item);
      }
    }
    return lookup;
  }, [replayStudyItems]);
  const boardRows = asArray<JsonRecord>(resultsBoard.rows);
  const discovery = asRecord(resultsBoard.discovery);
  const strategyOptions = asArray<JsonRecord>(discovery.strategies);
  const laneOptions = asArray<JsonRecord>(discovery.lanes);
  const sourceTypeOptions = asArray<JsonRecord>(discovery.source_types);
  const candidateStatusOptions = asArray<JsonRecord>(discovery.candidate_statuses);
  const lifecycleTruthOptions = asArray<JsonRecord>(discovery.lifecycle_truth_classes);
  const sortFieldOptions = asArray<JsonRecord>(resultsBoard.sort_fields);
  const runScopeOptions = asArray<JsonRecord>(resultsBoard.run_scope_presets);
  const rankLimitOptions = asArray<JsonRecord>(resultsBoard.rank_limit_options);
  const savedViews = asArray<JsonRecord>(resultsBoard.saved_views);
  const dateWindowOptions = asArray<JsonRecord>(resultsBoard.date_windows);
  const defaults = asRecord(resultsBoard.defaults);
  const defaultStrategyKey = String(defaults.strategy_key ?? analysis.default_strategy_key ?? strategyOptions[0]?.id ?? catalogRows[0]?.strategy_key ?? "all");
  const preferredDefaultStrategyKey = useMemo(
    () => strategyAnalysisPreferredStrategyKey(boardRows, defaultStrategyKey),
    [boardRows, defaultStrategyKey],
  );
  const [activeTab, setActiveTab] = useState<StrategyAnalysisTab>("results");
  const [selectedSavedViewId, setSelectedSavedViewId] = useState("");
  const [selectedStrategyKey, setSelectedStrategyKey] = useState(preferredDefaultStrategyKey || "all");
  const [selectedLaneFilterId, setSelectedLaneFilterId] = useState(String(defaults.lane_id ?? "all"));
  const [selectedSourceType, setSelectedSourceType] = useState(String(defaults.source_type ?? "all"));
  const [selectedCandidateStatus, setSelectedCandidateStatus] = useState(String(defaults.candidate_status ?? "all"));
  const [selectedLifecycleTruthClass, setSelectedLifecycleTruthClass] = useState(String(defaults.lifecycle_truth_class ?? "all"));
  const [selectedDateWindow, setSelectedDateWindow] = useState(String(defaults.date_window ?? "all_dates"));
  const [selectedRunScope, setSelectedRunScope] = useState(String(defaults.run_scope ?? "top"));
  const [selectedSortField, setSelectedSortField] = useState(String(defaults.sort_field ?? "net_pnl"));
  const [selectedRankLimit, setSelectedRankLimit] = useState(String(defaults.rank_limit ?? "10"));
  const boardRowByLaneId = useMemo(() => {
    const lookup = new Map<string, JsonRecord>();
    boardRows.forEach((row) => {
      const key = String(row.lane_id ?? row.id ?? "");
      if (key && !lookup.has(key)) {
        lookup.set(key, row);
      }
    });
    return lookup;
  }, [boardRows]);

  useEffect(() => {
    if (!strategyOptions.length) {
      return;
    }
    if (
      !selectedStrategyKey
      || (selectedStrategyKey !== "all" && !strategyOptions.some((row) => String(row.id ?? "") === selectedStrategyKey))
    ) {
      setSelectedStrategyKey(preferredDefaultStrategyKey);
    }
  }, [preferredDefaultStrategyKey, selectedStrategyKey, strategyOptions]);

  useEffect(() => {
    if (!preferredStrategyKey || preferredStrategyKey === "all") {
      return;
    }
    if (!strategyOptions.some((row) => String(row.id ?? "") === preferredStrategyKey)) {
      return;
    }
    setSelectedStrategyKey(preferredStrategyKey);
  }, [preferredStrategyKey, strategyOptions]);

  const selectedSavedView = asRecord(savedViews.find((row) => String(row.id ?? "") === selectedSavedViewId));
  const activeSavedViewSourceTypes = useMemo(
    () => asArray(selectedSavedView.source_type_list).map((value) => String(value ?? "")).filter(Boolean),
    [selectedSavedView],
  );
  const visibleLaneOptions = useMemo(
    () =>
      laneOptions.filter((row) =>
        selectedStrategyKey === "all"
          ? true
          : String(row.strategy_key ?? "") === selectedStrategyKey,
      ),
    [laneOptions, selectedStrategyKey],
  );

  useEffect(() => {
    if (selectedLaneFilterId !== "all" && !visibleLaneOptions.some((row) => String(row.id ?? "") === selectedLaneFilterId)) {
      setSelectedLaneFilterId("all");
    }
  }, [selectedLaneFilterId, visibleLaneOptions]);

  const filteredBoardRows = useMemo(
    () =>
      boardRows.filter((row) => {
        const rowStrategyKey = String(row.strategy_key ?? "");
        const rowLaneId = String(row.lane_id ?? row.id ?? "");
        const rowSourceType = String(row.source_type ?? "");
        const rowCandidateStatus = String(row.candidate_status_id ?? asRecord(row.candidate_status).id ?? "");
        const rowLifecycleTruth = String(row.lifecycle_truth_class ?? asRecord(row.lifecycle_truth).class ?? "");
        if (selectedStrategyKey !== "all" && rowStrategyKey !== selectedStrategyKey) {
          return false;
        }
        if (selectedLaneFilterId !== "all" && rowLaneId !== selectedLaneFilterId) {
          return false;
        }
        if (selectedSourceType !== "all" && rowSourceType !== selectedSourceType) {
          return false;
        }
        if (selectedCandidateStatus !== "all" && rowCandidateStatus !== selectedCandidateStatus) {
          return false;
        }
        if (selectedLifecycleTruthClass !== "all" && rowLifecycleTruth !== selectedLifecycleTruthClass) {
          return false;
        }
        if (!strategyAnalysisMatchesDateWindow(row, selectedDateWindow)) {
          return false;
        }
        if (activeSavedViewSourceTypes.length && !activeSavedViewSourceTypes.includes(rowSourceType)) {
          return false;
        }
        return true;
      }),
    [
      activeSavedViewSourceTypes,
      boardRows,
      selectedCandidateStatus,
      selectedDateWindow,
      selectedLaneFilterId,
      selectedLifecycleTruthClass,
      selectedSourceType,
      selectedStrategyKey,
    ],
  );
  const sortFieldLookup = useMemo(() => {
    const lookup = new Map<string, JsonRecord>();
    sortFieldOptions.forEach((row) => {
      const key = String(row.id ?? "");
      if (key) {
        lookup.set(key, row);
      }
    });
    return lookup;
  }, [sortFieldOptions]);
  const runScopeLookup = useMemo(() => {
    const lookup = new Map<string, JsonRecord>();
    runScopeOptions.forEach((row) => {
      const key = String(row.id ?? "");
      if (key) {
        lookup.set(key, row);
      }
    });
    return lookup;
  }, [runScopeOptions]);
  const effectiveRunScope = runScopeLookup.get(selectedRunScope) ?? asRecord(runScopeOptions[0]);
  const effectiveSortField = selectedRunScope === "latest"
    ? "latest_update_timestamp"
    : selectedRunScope === "lowest_drawdown"
      ? "max_drawdown"
      : selectedSortField;
  const effectiveSortMeta = sortFieldLookup.get(effectiveSortField) ?? asRecord(sortFieldOptions[0]);
  const runScopeUnavailableReason = strategyAnalysisRunScopeUnavailableReason({
    runScope: selectedRunScope,
    sortField: effectiveSortField,
    sortFieldMeta: effectiveSortMeta,
  });
  const rankedBoardRows = useMemo(() => {
    if (runScopeUnavailableReason) {
      return [];
    }
    const rows = [...filteredBoardRows];
    const direction = selectedRunScope === "lowest_drawdown"
      ? "asc"
      : String(effectiveSortMeta.default_direction ?? "desc");
    rows.sort((left, right) => strategyAnalysisCompareBoardRows(left, right, effectiveSortField, direction));
    let limit = selectedRankLimit === "all" ? null : Number(selectedRankLimit);
    if (selectedRunScope === "latest") {
      limit = 1;
    }
    if (limit !== null && Number.isFinite(limit) && limit > 0) {
      return rows.slice(0, limit);
    }
    return rows;
  }, [
    effectiveSortField,
    effectiveSortMeta,
    filteredBoardRows,
    runScopeUnavailableReason,
    selectedRankLimit,
    selectedRunScope,
  ]);
  const defaultVisibleLaneId = String(
    strategyAnalysisPreferredVisibleLaneId(
      rankedBoardRows,
      replayStudyItems,
      String(
        asRecord(detailsByStrategyKey[selectedStrategyKey]).default_lane_id
          ?? resultsBoard.default_row_id
          ?? "",
      ),
    ),
  );
  const [selectedLaneId, setSelectedLaneId] = useState(defaultVisibleLaneId);

  useEffect(() => {
    if (!rankedBoardRows.length) {
      return;
    }
    if (!selectedLaneId || !rankedBoardRows.some((row) => String(row.lane_id ?? "") === selectedLaneId)) {
      setSelectedLaneId(defaultVisibleLaneId);
    }
  }, [defaultVisibleLaneId, rankedBoardRows, selectedLaneId]);

  const selectedLane = laneById.get(selectedLaneId) ?? rankedBoardRows[0] ?? null;
  const selectedStrategyKeyForDetail = String(selectedLane?.strategy_key ?? (selectedStrategyKey === "all" ? defaultStrategyKey : selectedStrategyKey) ?? "");
  const strategyDetail = asRecord(detailsByStrategyKey[selectedStrategyKeyForDetail]);
  const strategyIdentity = asRecord(strategyDetail.strategy_identity);
  const lanes = asArray<JsonRecord>(strategyDetail.lanes);
  const comparisonPresets = asArray<JsonRecord>(strategyDetail.comparison_presets);
  const comparisonOptions = useMemo(
    () =>
      lanes
        .filter((lane) => String(lane.lane_id ?? "") !== selectedLaneId)
        .map((lane) => ({
          laneId: String(lane.lane_id ?? ""),
          label: `${formatValue(lane.lane_label ?? lane.lane_type)} | ${formatValue(lane.display_name ?? lane.strategy_label ?? lane.strategy_key)}`,
        })),
    [lanes, selectedLaneId],
  );
  const [comparisonLaneId, setComparisonLaneId] = useState("");

  useEffect(() => {
    if (!comparisonOptions.length) {
      if (comparisonLaneId) {
        setComparisonLaneId("");
      }
      return;
    }
    if (!comparisonLaneId || !comparisonOptions.some((option) => option.laneId === comparisonLaneId)) {
      const recommendedPreset = comparisonPresets.find((preset) => String(preset.left_lane_id ?? "") === selectedLaneId);
      const recommendedLaneId = String(recommendedPreset?.right_lane_id ?? comparisonOptions[0]?.laneId ?? "");
      setComparisonLaneId(recommendedLaneId);
    }
  }, [comparisonLaneId, comparisonOptions, comparisonPresets, selectedLaneId]);

  const comparisonLane = comparisonLaneId ? laneById.get(comparisonLaneId) ?? null : null;
  const selectedReplayStudyKey = String(asRecord(asRecord(selectedLane?.evidence).bars).ref?.study_key ?? "");
  const selectedReplayStudyItem = selectedReplayStudyKey ? replayStudyItemByKey.get(selectedReplayStudyKey) ?? null : null;
  const [selectedReplayStudyLoaded, setSelectedReplayStudyLoaded] = useState<JsonRecord | null>(null);
  useEffect(() => {
    const artifactTarget = String(asRecord(selectedReplayStudyItem?.artifact_paths).strategy_study_json ?? "").trim();
    const backendUrl = String(desktopState?.backendUrl ?? "").trim();
    if (!selectedReplayStudyKey || !artifactTarget || !backendUrl) {
      setSelectedReplayStudyLoaded(null);
      return;
    }
    let cancelled = false;
    const artifactUrl = artifactTarget.startsWith("/api/")
      ? new URL(artifactTarget.replace(/^\//, ""), backendUrl).toString()
      : artifactTarget;
    void fetch(artifactUrl)
      .then((response) => {
        if (!response.ok) {
          throw new Error(`Replay study fetch failed (${response.status})`);
        }
        return response.json() as Promise<JsonRecord>;
      })
      .then((payload) => {
        if (!cancelled) {
          setSelectedReplayStudyLoaded(asRecord(payload));
        }
      })
      .catch(() => {
        if (!cancelled) {
          setSelectedReplayStudyLoaded(null);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [selectedReplayStudyItem, selectedReplayStudyKey]);
  const selectedReplayStudyPayload = selectedReplayStudyLoaded ?? asRecord(selectedReplayStudyItem?.study_preview);
  const selectedReplayStudyRows = asArray<JsonRecord>(selectedReplayStudyPayload.bars ?? selectedReplayStudyPayload.rows);
  const selectedReplayStudy = selectedReplayStudyKey && selectedReplayStudyPayload
    ? {
        study_key: selectedReplayStudyKey,
        label: String(selectedReplayStudyItem?.label ?? selectedReplayStudyItem?.strategy_id ?? "Replay Study"),
        study: selectedReplayStudyPayload,
      }
    : null;
  const comparisonMetrics = useMemo(
    () => (selectedLane && comparisonLane ? strategyComparisonMetricRows(selectedLane, comparisonLane) : []),
    [comparisonLane, selectedLane],
  );
  const selectedBoardRow = selectedLaneId ? boardRowByLaneId.get(selectedLaneId) ?? null : null;
  const selectedLaneCandidateLabel = selectedLane
    ? formatValue(asRecord(selectedLane.candidate_status).label ?? selectedLane.candidate_status_label ?? "—")
    : "—";
  const selectedLaneDateRangeLabel = selectedLane ? strategyLaneDateRangeLabel(asRecord(selectedLane.date_range)) : "—";
  const selectedLaneProvenanceLabel = selectedLane
    ? formatValue(selectedLane.source_label ?? selectedLane.source_lane ?? "—")
    : "—";
  const selectedLaneLifecycleLabel = selectedLane
    ? strategyLifecycleTruthLabel(asRecord(selectedLane.lifecycle_truth))
    : "Lifecycle Detail Not Available";
  const selectedLaneTitle = selectedLane
    ? `${formatValue(selectedLane.lane_label ?? selectedLane.lane_type)} | ${formatValue(selectedLane.display_name ?? selectedLane.strategy_label ?? selectedLane.strategy_key)}`
    : "Choose a result row";
  const selectedLaneLatestTrade = selectedBoardRow
    ? formatValue(selectedBoardRow.latest_trade_summary_label ?? "Unavailable")
    : "Unavailable";
  const selectedLaneSessionEvidence = asArray<JsonRecord>(asRecord(asRecord(selectedLane?.evidence).session_evidence).preview_rows);
  const comparisonLaneSessionEvidence = asArray<JsonRecord>(asRecord(asRecord(comparisonLane?.evidence).session_evidence).preview_rows);
  const selectedTradeFamilyBreakdownMetric = asRecord(asRecord(selectedLane?.metrics).trade_family_breakdown);
  const comparisonTradeFamilyBreakdownMetric = asRecord(asRecord(comparisonLane?.metrics).trade_family_breakdown);
  const selectedTradeFamilyBreakdown = asArray<JsonRecord>(selectedTradeFamilyBreakdownMetric.value);
  const comparisonTradeFamilyBreakdown = asArray<JsonRecord>(comparisonTradeFamilyBreakdownMetric.value);
  const reportStatusLabel = strategyAnalysisReportStatus(selectedLane, rankedBoardRows.length);
  const pnlStatusLabel = strategyAnalysisPnlStatus(selectedLane, rankedBoardRows.length);
  const tradeTruthQualityLabel = strategyAnalysisTradeTruthQuality(selectedLane);
  const comparableStatusLabel = strategyAnalysisComparableStatus(selectedLane, lanes);
  const recommendedNextActionLabel = strategyAnalysisRecommendedNextAction(selectedLane, rankedBoardRows.length, lanes);
  const unavailableOperatorMessage = strategyAnalysisUnavailableOperatorMessage(selectedLane, rankedBoardRows.length);
  const resultsEmptyStateMessage = strategyAnalysisResultsEmptyStateMessage({
    visibleRowCount: rankedBoardRows.length,
    runScopeUnavailableReason,
  });
  const analysisTabs: Array<{ id: StrategyAnalysisTab; label: string; note: string }> = [
    { id: "results", label: "Results", note: "Ranked board and selectors" },
    { id: "compare", label: "Compare", note: "Baseline, candidate, and lane deltas" },
    { id: "study", label: "Study", note: "Charts and study detail" },
    { id: "runtime", label: "Runtime", note: "Runtime health and actions" },
    { id: "diagnostics", label: "Diagnostics", note: "Trigger validation and raw evidence" },
  ];

  if (!strategyOptions.length) {
    return <div className="placeholder-note">No strategy analysis rows are available yet.</div>;
  }

  return (
    <>
      <div className="results-board-tabs" role="tablist" aria-label="Operator analytics sections">
        {analysisTabs.map((tab) => (
          <button
            key={tab.id}
            type="button"
            role="tab"
            aria-selected={activeTab === tab.id}
            className={`panel-button results-board-tab ${activeTab === tab.id ? "active" : ""}`}
            onClick={() => setActiveTab(tab.id)}
            title={tab.note}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {activeTab === "results" ? (
        <div className="results-board-panel">
          <div className="metric-grid">
            <MetricCard label="Strategy" value={formatValue(strategyIdentity.display_name ?? (selectedStrategyKey === "all" ? "All strategies" : selectedStrategyKey))} />
            <MetricCard label="Lane / Candidate" value={`${selectedLaneTitle} | ${selectedLaneCandidateLabel}`} tone="good" />
            <MetricCard label="Date Range" value={selectedLaneDateRangeLabel} />
            <MetricCard label="Report Status" value={reportStatusLabel} tone={reportStatusLabel === "Report Ready" ? "good" : "warn"} />
            <MetricCard label="P/L Status" value={pnlStatusLabel} tone={strategyMetricAvailable(selectedLane, "net_pnl") ? "good" : "warn"} />
            <MetricCard label="Trade Truth Quality" value={tradeTruthQualityLabel} tone={strategyLifecycleTruthTone(asRecord(selectedLane?.lifecycle_truth))} />
            <MetricCard label="Comparable to Baseline?" value={comparableStatusLabel} tone={comparableStatusLabel === "Yes" || comparableStatusLabel === "This is the baseline" ? "good" : "muted"} />
            <MetricCard label="Recommended Next Action" value={recommendedNextActionLabel} tone="muted" />
          </div>
          <div className="action-row inline">
            <button className="panel-button" disabled={!selectedLane} onClick={() => setActiveTab("compare")}>
              View Report
            </button>
            <button className="panel-button subtle" disabled={!selectedLane} onClick={() => setActiveTab("study")}>
              View Summary
            </button>
            <button className="panel-button subtle" disabled={!selectedLane} onClick={() => setActiveTab("diagnostics")}>
              View Evidence
            </button>
            <button
              className="panel-button subtle"
              disabled={!selectedLane}
              onClick={() => setActiveTab("compare")}
            >
              Compare Selected
            </button>
          </div>
          <div className="results-view-buttons">
            {savedViews.map((view) => {
              const viewId = String(view.id ?? "");
              const available = view.available !== false;
              const active = selectedSavedViewId === viewId;
              return (
                <button
                  key={viewId}
                  className={`panel-button ${active ? "active" : ""}`}
                  disabled={!available}
                  title={available ? formatValue(view.note ?? "") : formatValue(view.unavailable_reason ?? "This view is currently unavailable.")}
                  onClick={() => {
                    if (!available) {
                      return;
                    }
                    setSelectedSavedViewId(viewId);
                    if (view.strategy_key) {
                      setSelectedStrategyKey(String(view.strategy_key ?? "all"));
                    }
                    setSelectedLaneFilterId("all");
                    setSelectedSourceType(String(view.source_type ?? "all"));
                    setSelectedCandidateStatus(String(view.candidate_status ?? "all"));
                    setSelectedLifecycleTruthClass(String(view.lifecycle_truth_class ?? "all"));
                    setSelectedDateWindow("all_dates");
                    setSelectedRunScope(String(view.run_scope ?? "top"));
                    setSelectedSortField(String(view.sort_field ?? "net_pnl"));
                    setSelectedRankLimit(String(view.rank_limit ?? "10"));
                  }}
                >
                  {formatValue(view.label)}
                </button>
              );
            })}
          </div>
          <div className="study-toolbar">
            <div className="study-workbench-controls">
              <label className="study-select-field">
                <span>Strategy</span>
                <select value={selectedStrategyKey} onChange={(event) => setSelectedStrategyKey(event.target.value)}>
                  <option value="all">All strategies</option>
                  {strategyOptions.map((row) => (
                    <option key={String(row.id ?? "")} value={String(row.id ?? "")}>
                      {formatValue(row.label ?? row.id)} | {formatValue(row.instrument ?? "MULTI")} {row.has_data === true ? "" : "(No data yet)"}
                    </option>
                  ))}
                </select>
              </label>
              <label className="study-select-field">
                <span>Lane / Candidate</span>
                <select value={selectedLaneFilterId} onChange={(event) => setSelectedLaneFilterId(event.target.value)}>
                  <option value="all">All discovered lanes</option>
                  {visibleLaneOptions.map((lane) => (
                    <option key={String(lane.id ?? "")} value={String(lane.id ?? "")}>
                      {formatValue(lane.label ?? lane.id)} {lane.has_data === true ? "" : "(No data yet)"}
                    </option>
                  ))}
                </select>
              </label>
              <label className="study-select-field">
                <span>Source Type</span>
                <select value={selectedSourceType} onChange={(event) => setSelectedSourceType(event.target.value)}>
                  <option value="all">All source types</option>
                  {sourceTypeOptions.map((row) => (
                    <option key={String(row.id ?? "")} value={String(row.id ?? "")}>
                      {formatValue(row.label ?? row.id)}
                    </option>
                  ))}
                </select>
              </label>
              <label className="study-select-field">
                <span>Candidate Status</span>
                <select value={selectedCandidateStatus} onChange={(event) => setSelectedCandidateStatus(event.target.value)}>
                  <option value="all">All statuses</option>
                  {candidateStatusOptions.map((row) => (
                    <option key={String(row.id ?? "")} value={String(row.id ?? "")}>
                      {formatValue(row.label ?? row.id)}
                    </option>
                  ))}
                </select>
              </label>
              <label className="study-select-field">
                <span>Lifecycle Truth</span>
                <select value={selectedLifecycleTruthClass} onChange={(event) => setSelectedLifecycleTruthClass(event.target.value)}>
                  <option value="all">All lifecycle classes</option>
                  {lifecycleTruthOptions.map((row) => (
                    <option key={String(row.id ?? "")} value={String(row.id ?? "")}>
                      {formatValue(row.label ?? row.id)}
                    </option>
                  ))}
                </select>
              </label>
              <label className="study-select-field">
                <span>Date Window</span>
                <select value={selectedDateWindow} onChange={(event) => setSelectedDateWindow(event.target.value)}>
                  {dateWindowOptions.map((row) => (
                    <option key={String(row.id ?? "")} value={String(row.id ?? "")}>
                      {formatValue(row.label ?? row.id)}
                    </option>
                  ))}
                </select>
              </label>
              <label className="study-select-field">
                <span>Run Scope</span>
                <select value={selectedRunScope} onChange={(event) => setSelectedRunScope(event.target.value)}>
                  {runScopeOptions.map((row) => (
                    <option key={String(row.id ?? "")} value={String(row.id ?? "")} disabled={row.available === false}>
                      {formatValue(row.label ?? row.id)}
                    </option>
                  ))}
                </select>
              </label>
              <label className="study-select-field">
                <span>Sort Field</span>
                <select value={selectedSortField} onChange={(event) => setSelectedSortField(event.target.value)}>
                  {sortFieldOptions.map((row) => (
                    <option key={String(row.id ?? "")} value={String(row.id ?? "")} disabled={row.available === false}>
                      {formatValue(row.label ?? row.id)}
                    </option>
                  ))}
                </select>
              </label>
              <label className="study-select-field">
                <span>Rank Limit</span>
                <select value={selectedRankLimit} onChange={(event) => setSelectedRankLimit(event.target.value)}>
                  {rankLimitOptions.map((row) => (
                    <option key={String(row.id ?? "")} value={String(row.id ?? "")}>
                      {formatValue(row.label ?? row.id)}
                    </option>
                  ))}
                </select>
              </label>
            </div>
          </div>
          <div className="notice-strip compact">
            <div><strong>Report View:</strong> Compact ranked results first, with evidence drill-down kept in separate tabs.</div>
            <div><strong>Sort:</strong> {formatValue(effectiveSortMeta.label ?? effectiveSortField)} | <strong>Scope:</strong> {formatValue(effectiveRunScope.label ?? selectedRunScope)}</div>
            <div><strong>Saved View:</strong> {formatValue(selectedSavedView.label ?? "Custom")}</div>
            {selectedSavedView.unavailable_reason ? <div><strong>Saved View Note:</strong> {formatValue(selectedSavedView.unavailable_reason)}</div> : null}
            {unavailableOperatorMessage ? <div><strong>Operator Note:</strong> {unavailableOperatorMessage}</div> : null}
          </div>
          <DataTable
            columns={[
              { key: "strategy_display_name", label: "Strategy", render: (row) => formatValue(row.strategy_display_name ?? row.strategy_key) },
              { key: "lane_label", label: "Lane", render: (row) => formatValue(row.lane_label ?? row.source_type) },
              { key: "candidate_status", label: "Candidate", render: (row) => formatValue(row.candidate_status_label ?? asRecord(row.candidate_status).label ?? "—") },
              { key: "run_study_identity", label: "Run / Study", render: (row) => formatValue(row.run_study_identity ?? row.display_name ?? row.strategy_key) },
              { key: "date_range_label", label: "Date Range", render: (row) => formatValue(row.date_range_label ?? "—") },
              { key: "trade_count", label: "Trades", render: (row) => strategyMetricLabel(row, "trade_count") },
              { key: "net_pnl", label: "Net P/L", render: (row) => strategyMetricLabel(row, "net_pnl") },
              { key: "average_trade", label: "Avg Trade", render: (row) => strategyMetricLabel(row, "average_trade") },
              { key: "profit_factor", label: "Profit Factor", render: (row) => strategyMetricLabel(row, "profit_factor") },
              { key: "max_drawdown", label: "Max Drawdown", render: (row) => strategyMetricLabel(row, "max_drawdown") },
              { key: "win_rate", label: "Win Rate", render: (row) => strategyMetricLabel(row, "win_rate") },
              { key: "latest_trade_summary_label", label: "Latest Trade", render: (row) => formatValue(row.latest_trade_summary_label ?? "Unavailable") },
              { key: "lifecycle_truth", label: "Lifecycle Truth", render: (row) => strategyLifecycleTruthLabel(asRecord(row.lifecycle_truth)) },
              { key: "source_lane", label: "Provenance", render: (row) => formatValue(row.source_label ?? row.source_lane ?? "—") },
              {
                key: "comparison_status",
                label: "Comparison Status",
                render: (row) => {
                  const rowStrategyKey = String(row.strategy_key ?? "");
                  const rowDetail = asRecord(detailsByStrategyKey[rowStrategyKey]);
                  const rowLanes = asArray<JsonRecord>(rowDetail.lanes);
                  return strategyAnalysisComparableStatus(row, rowLanes);
                },
              },
              {
                key: "comparison_recommendation",
                label: "Recommendation",
                render: (row) => {
                  const rowStrategyKey = String(row.strategy_key ?? "");
                  const rowDetail = asRecord(detailsByStrategyKey[rowStrategyKey]);
                  const rowLanes = asArray<JsonRecord>(rowDetail.lanes);
                  return strategyAnalysisRecommendedNextAction(row, 1, rowLanes);
                },
              },
            ]}
            rows={rankedBoardRows}
            emptyLabel={resultsEmptyStateMessage}
            onRowClick={(row) => {
              const nextStrategyKey = String(row.strategy_key ?? "");
              if (nextStrategyKey) {
                setSelectedStrategyKey(nextStrategyKey);
              }
              setSelectedLaneId(String(row.lane_id ?? row.id ?? ""));
              setActiveTab("compare");
            }}
            rowKey={(row) => String(row.lane_id ?? row.id ?? "")}
            selectedRowKey={selectedLaneId}
          />
          <div className="notice-strip compact">
            <div><strong>What changed?</strong> Select a row to open the in-app report view with active benchmark/candidate context.</div>
            <div><strong>What should I inspect next?</strong> Open Summary for the chart/study view or Open Evidence for raw proof and trigger validation.</div>
            <div><strong>Latest trade</strong> {selectedLaneLatestTrade}</div>
          </div>
        </div>
      ) : null}

      {activeTab === "compare" ? (
        selectedLane ? (
          <div className="results-board-panel">
            <div className="study-toolbar">
              <div className="study-workbench-controls">
                <label className="study-select-field">
                  <span>Inspect Lane</span>
                  <select value={selectedLaneId} onChange={(event) => setSelectedLaneId(event.target.value)}>
                    {lanes.map((lane) => (
                      <option key={String(lane.lane_id ?? "")} value={String(lane.lane_id ?? "")}>
                        {formatValue(lane.lane_label ?? lane.lane_type)} | {formatValue(lane.display_name ?? lane.strategy_label)}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="study-select-field">
                  <span>Compare To</span>
                  <select value={comparisonLaneId} onChange={(event) => setComparisonLaneId(event.target.value)}>
                    <option value="">None</option>
                    {comparisonOptions.map((option) => (
                      <option key={option.laneId} value={option.laneId}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </label>
              </div>
            </div>
            <div className="study-meta-strip">
              <span><strong>Strategy</strong> {formatValue(strategyIdentity.display_name ?? selectedStrategyKeyForDetail)}</span>
              <span><strong>Selected lane</strong> {selectedLaneTitle}</span>
              <span><strong>Compared to</strong> {formatValue(comparisonLane?.lane_label ?? "Choose a lane")}</span>
              <span><strong>Date range</strong> {selectedLaneDateRangeLabel}</span>
              <span><strong>Provenance</strong> {selectedLaneProvenanceLabel}</span>
            </div>
            <div className="metric-grid">
              <MetricCard label="Candidate" value={selectedLaneCandidateLabel} tone="good" />
              <MetricCard label="Lifecycle Truth" value={selectedLaneLifecycleLabel} tone={strategyLifecycleTruthTone(asRecord(selectedLane.lifecycle_truth))} />
              <MetricCard label="Net P/L Delta" value={strategyComparisonMetricCell(comparisonMetrics, "net_pnl", "delta")} />
              <MetricCard label="Avg Trade Delta" value={strategyComparisonMetricCell(comparisonMetrics, "average_trade", "delta")} />
              <MetricCard label="PF Delta" value={strategyComparisonMetricCell(comparisonMetrics, "profit_factor", "delta")} />
              <MetricCard label="Max Drawdown Delta" value={strategyComparisonMetricCell(comparisonMetrics, "max_drawdown", "delta")} />
            </div>
            <div className="notice-strip compact">
              <div><strong>Selected lifecycle:</strong> {selectedLaneLifecycleLabel}</div>
              <div><strong>Selected provenance:</strong> {formatValue(asRecord(selectedLane.source_of_truth).primary_artifact ?? selectedLane.source_lane ?? "—")}</div>
              <div><strong>Compared provenance:</strong> {formatValue(asRecord(comparisonLane?.source_of_truth).primary_artifact ?? comparisonLane?.source_lane ?? "Choose a lane")}</div>
              <div><strong>Semantics:</strong> {formatValue(asRecord(selectedLane.mode_truth).execution_semantics ?? "—")}</div>
            </div>
            {comparisonLane ? (
              <>
                <DataTable
                  columns={[
                    { key: "label", label: "Metric" },
                    { key: "left", label: formatValue(selectedLane.lane_label), render: (row) => formatValue(row.left) },
                    { key: "right", label: formatValue(comparisonLane.lane_label), render: (row) => formatValue(row.right) },
                    { key: "delta", label: "Delta", render: (row) => formatValue(row.delta) },
                  ]}
                  rows={comparisonMetrics}
                  emptyLabel="No comparable metrics are available for these lanes."
                />
                <div className="split-panel">
                  <div>
                    <h3 className="subsection-title">Session Contribution</h3>
                    <JsonBlock
                      value={{
                        selected_lane: selectedLaneSessionEvidence,
                        comparison_lane: comparisonLaneSessionEvidence,
                        note: selectedLaneSessionEvidence.length || comparisonLaneSessionEvidence.length
                          ? "Session evidence rows are shown as published."
                          : "Session contribution detail is unavailable for one or both lanes.",
                      }}
                    />
                  </div>
                  <div>
                    <h3 className="subsection-title">Family Contribution</h3>
                    <JsonBlock
                      value={{
                        selected_lane: selectedTradeFamilyBreakdownMetric.available === true ? selectedTradeFamilyBreakdown : selectedTradeFamilyBreakdownMetric.reason ?? "Unavailable",
                        comparison_lane: comparisonTradeFamilyBreakdownMetric.available === true ? comparisonTradeFamilyBreakdown : comparisonTradeFamilyBreakdownMetric.reason ?? "Unavailable",
                      }}
                    />
                  </div>
                </div>
              </>
            ) : (
              <div className="placeholder-note">Choose a comparison lane to see deltas, session contribution, and family contribution.</div>
            )}
            <div className="action-row inline">
              <button className="panel-button subtle" onClick={() => setActiveTab("results")}>
                Back To Results
              </button>
              <button className="panel-button subtle" onClick={() => setActiveTab("study")}>
                Open Study
              </button>
              <button className="panel-button subtle" onClick={() => setActiveTab("diagnostics")}>
                Open Diagnostics
              </button>
            </div>
          </div>
        ) : (
          <div className="placeholder-note">Select a result row on the Results tab to open a comparison view.</div>
        )
      ) : null}

      {activeTab === "study" ? (
        <div className="results-board-panel">
          <div className="study-meta-strip">
            <span><strong>Strategy</strong> {formatValue(strategyIdentity.display_name ?? selectedStrategyKeyForDetail ?? "—")}</span>
            <span><strong>Selected lane</strong> {selectedLaneTitle}</span>
            <span><strong>Candidate</strong> {selectedLaneCandidateLabel}</span>
            <span><strong>Lifecycle</strong> {selectedLaneLifecycleLabel}</span>
            <span><strong>Provenance</strong> {selectedLaneProvenanceLabel}</span>
          </div>
          {selectedLane?.source_lane === "historical_playback" && selectedReplayStudy && selectedReplayStudyRows.length > 0 ? (
            <>
              <div className="notice-strip compact">
                <div><strong>Study Available:</strong> Linked replay study published for the selected lane.</div>
                <div><strong>Timing Detail:</strong> Study view keeps ATP and execution detail separate from baseline bar context.</div>
              </div>
              <ReplayStrategyStudy studies={[selectedReplayStudy]} />
            </>
          ) : selectedLane?.source_lane === "historical_playback" && selectedReplayStudy ? (
            <div className="placeholder-note">Replay study metadata is attached to this lane and the full artifact is loading.</div>
          ) : studyPanel ?? <div className="placeholder-note">No study surface is available for the selected lane yet.</div>}
        </div>
      ) : null}

      {activeTab === "runtime" ? (
        <div className="results-board-panel">
          <div className="study-meta-strip">
            <span><strong>Strategy</strong> {formatValue(strategyIdentity.display_name ?? selectedStrategyKeyForDetail ?? "—")}</span>
            <span><strong>Selected lane</strong> {selectedLaneTitle}</span>
            <span><strong>Latest status</strong> {selectedLane ? strategyMetricValue(selectedLane, "latest_status") : "Unavailable"}</span>
            <span><strong>Last update</strong> {selectedLane ? strategyMetricValue(selectedLane, "latest_update_timestamp") : "Unavailable"}</span>
            <span><strong>Runtime health</strong> {selectedLane ? formatValue(asRecord(selectedLane.runtime_health).label ?? "N/A") : "N/A"}</span>
          </div>
          {selectedLane ? (
            <div className="metric-grid">
              <MetricCard label="Runtime Health" value={formatValue(asRecord(selectedLane.runtime_health).label ?? "N/A")} tone={statusTone(asRecord(selectedLane.runtime_health).label)} />
              <MetricCard label="Lifecycle Truth" value={selectedLaneLifecycleLabel} tone={strategyLifecycleTruthTone(asRecord(selectedLane.lifecycle_truth))} />
              <MetricCard label="Timeframe Truth" value={strategyLaneTimeframeLabel(asRecord(selectedLane.timeframe_truth))} />
              <MetricCard label="Source Lane" value={formatValue(selectedLane.source_lane ?? "—")} />
              <MetricCard label="Latest Trade" value={selectedLaneLatestTrade} />
              <MetricCard label="Date Range" value={selectedLaneDateRangeLabel} />
            </div>
          ) : null}
          {runtimePanel ?? <div className="placeholder-note">Runtime actions are unavailable because no runtime panel was provided.</div>}
        </div>
      ) : null}

      {activeTab === "diagnostics" ? (
        <div className="results-board-panel">
          {selectedLane ? (
            <>
              <div className="study-meta-strip">
                <span><strong>Strategy</strong> {formatValue(strategyIdentity.display_name ?? selectedStrategyKeyForDetail ?? "—")}</span>
                <span><strong>Selected lane</strong> {selectedLaneTitle}</span>
                <span><strong>Lifecycle</strong> {selectedLaneLifecycleLabel}</span>
                <span><strong>Provenance</strong> {formatValue(asRecord(selectedLane.source_of_truth).primary_artifact ?? selectedLane.source_lane ?? "—")}</span>
                <span><strong>Latest trade</strong> {selectedLaneLatestTrade}</span>
              </div>
              <div className="split-panel">
                <div>
                  <h3 className="subsection-title">Lane Metrics</h3>
                  <JsonBlock value={asRecord(selectedLane.metrics)} />
                </div>
                <div>
                  <h3 className="subsection-title">Provenance</h3>
                  <JsonBlock
                    value={{
                      source_of_truth: selectedLane.source_of_truth ?? {},
                      lifecycle_truth: selectedLane.lifecycle_truth ?? {},
                      mode_truth: selectedLane.mode_truth ?? {},
                      timeframe_truth: selectedLane.timeframe_truth ?? {},
                      runtime_health: selectedLane.runtime_health ?? {},
                      provenance: selectedLane.provenance ?? {},
                    }}
                  />
                </div>
              </div>
              <div className="split-panel">
                <div>
                  <h3 className="subsection-title">Evidence Availability</h3>
                  <JsonBlock value={selectedLane.evidence ?? {}} />
                </div>
                <div>
                  <h3 className="subsection-title">Latest Trade Summary</h3>
                  <JsonBlock value={asRecord(asRecord(selectedLane.metrics).latest_trade_summary).value ?? null} />
                </div>
              </div>
              {selectedLane.source_lane !== "historical_playback" ? (
                <div className="split-panel">
                  <div>
                    <h3 className="subsection-title">Recent Bars / Signals</h3>
                    <JsonBlock
                      value={{
                        bars: asRecord(asRecord(selectedLane.evidence).bars).preview_rows ?? [],
                        signals: asRecord(asRecord(selectedLane.evidence).signals).preview_rows ?? [],
                      }}
                    />
                  </div>
                  <div>
                    <h3 className="subsection-title">Intents / Fills / Snapshots</h3>
                    <JsonBlock
                      value={{
                        order_intents: asRecord(asRecord(selectedLane.evidence).order_intents).preview_rows ?? [],
                        fills: asRecord(asRecord(selectedLane.evidence).fills).preview_rows ?? [],
                        state_snapshots: asRecord(asRecord(selectedLane.evidence).state_snapshots).preview_rows ?? [],
                        session_evidence: asRecord(asRecord(selectedLane.evidence).session_evidence).preview_rows ?? [],
                        readiness_artifacts: asRecord(asRecord(selectedLane.evidence).readiness_artifacts).preview_rows ?? [],
                        trade_lifecycle: asRecord(asRecord(selectedLane.evidence).trade_lifecycle).preview_rows ?? [],
                      }}
                    />
                  </div>
                </div>
              ) : null}
            </>
          ) : null}
          {diagnosticsPanel ?? <div className="placeholder-note">Diagnostics are unavailable because no diagnostics panel was provided.</div>}
        </div>
      ) : null}
    </>
  );
}

type StudyPnlMode = "cumulative_total" | "cumulative_realized" | "unrealized";

type StudyHeightPreset = "compact" | "standard" | "expanded";

function strategyMetricValue(lane: JsonRecord, metricKey: string): string {
  const metric = asRecord(asRecord(lane.metrics)[metricKey]);
  if (metric.available === true) {
    return formatValue(metric.value);
  }
  return metric.reason ? `Unavailable: ${formatValue(metric.reason)}` : "Unavailable";
}

function strategyMetricLabel(lane: JsonRecord, metricKey: string): string {
  const metric = asRecord(asRecord(lane.metrics)[metricKey]);
  if (metric.available !== true) {
    return metric.reason ? `Unavailable: ${formatValue(metric.reason)}` : "Unavailable";
  }
  if (metricKey.includes("pnl") || metricKey === "max_drawdown" || metricKey === "average_trade") {
    return formatMaybePnL(metric.value);
  }
  if (metricKey === "win_rate") {
    const value = Number(metric.value);
    return Number.isFinite(value) ? `${value.toFixed(1)}%` : formatValue(metric.value);
  }
  return formatValue(metric.value);
}

function strategyLaneTimeframeLabel(timeframeTruth: JsonRecord): string {
  if (timeframeTruth.structural_signal_timeframe || timeframeTruth.execution_timeframe || timeframeTruth.artifact_timeframe) {
    return [
      `S ${formatValue(timeframeTruth.structural_signal_timeframe ?? "—")}`,
      `E ${formatValue(timeframeTruth.execution_timeframe ?? "—")}`,
      `A ${formatValue(timeframeTruth.artifact_timeframe ?? "—")}`,
    ].join(" | ");
  }
  return formatValue(timeframeTruth.unavailable_reason ?? "Unavailable");
}

function strategyLaneDateRangeLabel(dateRange: JsonRecord): string {
  const start = formatValue(dateRange.start_timestamp ?? "—");
  const end = formatValue(dateRange.end_timestamp ?? "—");
  return `${start} -> ${end}`;
}

function strategyLifecycleTruthLabel(lifecycleTruth: JsonRecord): string {
  const lifecycleClass = String(lifecycleTruth.class ?? "").trim();
  const reason = String(lifecycleTruth.reason ?? "").trim();
  if (!lifecycleClass) {
    return reason ? `Lifecycle Detail Not Available: ${reason}` : "Lifecycle Detail Not Available";
  }
  return lifecycleClass;
}

function strategyLifecycleTruthTone(lifecycleTruth: JsonRecord): "good" | "warn" | "danger" | "muted" {
  const lifecycleClass = String(lifecycleTruth.class ?? "").trim().toUpperCase();
  if (lifecycleClass === "FULL_LIFECYCLE_TRUTH") {
    return "good";
  }
  if (lifecycleClass === "HYBRID_ENTRY_BASELINE_EXIT_TRUTH" || lifecycleClass === "BASELINE_ONLY") {
    return "warn";
  }
  if (lifecycleClass === "UNSUPPORTED") {
    return "danger";
  }
  return "muted";
}

function strategyComparisonMetricRows(leftLane: JsonRecord, rightLane: JsonRecord): JsonRecord[] {
  const keys = [
    "net_pnl",
    "realized_pnl",
    "open_pnl",
    "trade_count",
    "long_trades",
    "short_trades",
    "winners",
    "losers",
    "win_rate",
    "average_trade",
    "profit_factor",
    "max_drawdown",
  ];
  return keys.map((key) => {
    const leftMetric = asRecord(asRecord(leftLane.metrics)[key]);
    const rightMetric = asRecord(asRecord(rightLane.metrics)[key]);
    const leftValue = leftMetric.available === true ? leftMetric.value : leftMetric.reason ?? "Unavailable";
    const rightValue = rightMetric.available === true ? rightMetric.value : rightMetric.reason ?? "Unavailable";
    let delta: string = "Unavailable";
    if (leftMetric.available === true && rightMetric.available === true) {
      const leftNumber = Number(leftMetric.value);
      const rightNumber = Number(rightMetric.value);
      if (Number.isFinite(leftNumber) && Number.isFinite(rightNumber)) {
        const difference = rightNumber - leftNumber;
        delta = key.includes("pnl") || key === "average_trade" || key === "max_drawdown" ? formatMaybePnL(difference) : formatValue(difference);
      }
    }
    return {
      key,
      label: sentenceCase(key.split("_").join(" ")),
      left: key.includes("pnl") || key === "average_trade" || key === "max_drawdown"
        ? (leftMetric.available === true ? formatMaybePnL(leftMetric.value) : formatValue(leftValue))
        : key === "win_rate" && leftMetric.available === true
          ? `${Number(leftMetric.value).toFixed(1)}%`
          : formatValue(leftValue),
      right: key.includes("pnl") || key === "average_trade" || key === "max_drawdown"
        ? (rightMetric.available === true ? formatMaybePnL(rightMetric.value) : formatValue(rightValue))
        : key === "win_rate" && rightMetric.available === true
          ? `${Number(rightMetric.value).toFixed(1)}%`
          : formatValue(rightValue),
      delta,
    };
  });
}

function strategyComparisonMetricCell(rows: JsonRecord[], key: string, field: "left" | "right" | "delta"): string {
  const row = rows.find((item) => String(item.key ?? "") === key);
  return row ? formatValue(row[field]) : "Unavailable";
}

function strategyMetricAvailable(lane: JsonRecord | null | undefined, metricKey: string): boolean {
  return asRecord(asRecord(lane?.metrics)[metricKey]).available === true;
}

function strategyAnalysisIsReportableLane(lane: JsonRecord | null | undefined): boolean {
  return ["net_pnl", "average_trade", "profit_factor", "max_drawdown", "trade_count"].every((key) => strategyMetricAvailable(lane, key));
}

function strategyAnalysisReplayStudyKey(lane: JsonRecord | null | undefined): string {
  return String(asRecord(asRecord(lane?.evidence).bars).ref?.study_key ?? "").trim();
}

function strategyAnalysisIsHistoricalReplayLane(lane: JsonRecord | null | undefined): boolean {
  const sourceLane = String(lane?.source_lane ?? lane?.lane_type ?? "").trim();
  return sourceLane === "historical_playback" || sourceLane === "benchmark_replay";
}

function strategyAnalysisRuntimeLaneIsSparse(lane: JsonRecord | null | undefined): boolean {
  if (!lane) {
    return true;
  }
  const sourceLane = String(lane.source_lane ?? lane.lane_type ?? "").trim();
  if (strategyAnalysisIsHistoricalReplayLane(lane)) {
    return false;
  }
  if (!strategyAnalysisIsReportableLane(lane)) {
    return true;
  }
  const tradeCountMetric = asRecord(asRecord(lane.metrics).trade_count);
  const tradeCount = Number(tradeCountMetric.value ?? 0);
  if (!Number.isFinite(tradeCount) || tradeCount <= 0) {
    return true;
  }
  return sourceLane === "paper_runtime" && tradeCount < 3;
}

function strategyAnalysisPreferredVisibleLaneId(
  rankedBoardRows: JsonRecord[],
  replayStudyItems: JsonRecord[],
  fallbackLaneId: string,
): string {
  const replayStudyKeys = new Set(
    replayStudyItems
      .map((item) => String(item.study_key ?? "").trim())
      .filter(Boolean),
  );
  const firstRankedRow = rankedBoardRows[0] ?? null;
  const historicalRow = rankedBoardRows.find((row) => {
    const studyKey = strategyAnalysisReplayStudyKey(row);
    return strategyAnalysisIsHistoricalReplayLane(row) && studyKey && replayStudyKeys.has(studyKey) && strategyAnalysisIsReportableLane(row);
  });
  if (historicalRow && strategyAnalysisRuntimeLaneIsSparse(firstRankedRow)) {
    return String(historicalRow.lane_id ?? historicalRow.id ?? fallbackLaneId ?? "");
  }
  return String(firstRankedRow?.lane_id ?? firstRankedRow?.id ?? fallbackLaneId ?? "");
}

function strategyAnalysisPreferredStrategyKey(boardRows: JsonRecord[], fallbackStrategyKey: string): string {
  const reportableRow = boardRows.find((row) => strategyAnalysisIsReportableLane(row));
  const firstDataRow = boardRows[0];
  return String(reportableRow?.strategy_key ?? firstDataRow?.strategy_key ?? fallbackStrategyKey ?? "all");
}

function strategyAnalysisReportStatus(lane: JsonRecord | null, visibleRowCount: number): string {
  if (!visibleRowCount) {
    return "No reportable run loaded";
  }
  if (!lane) {
    return "Select a report row";
  }
  if (strategyAnalysisIsReportableLane(lane)) {
    return "Report Ready";
  }
  return "Evidence Loaded, Report Partial";
}

function strategyAnalysisPnlStatus(lane: JsonRecord | null, visibleRowCount: number): string {
  if (!visibleRowCount) {
    return "No reportable run loaded";
  }
  if (!lane) {
    return "Select a result row";
  }
  if (strategyMetricAvailable(lane, "net_pnl")) {
    return "Priced P/L Available";
  }
  const sourceLane = String(lane.source_lane ?? lane.lane_type ?? "").trim();
  if (sourceLane === "historical_playback" || sourceLane === "benchmark_replay") {
    return "Replay loaded, but priced closed-trade path is incomplete";
  }
  if (sourceLane === "paper_runtime") {
    return "Paper lane attached, but insufficient trade truth for P/L";
  }
  return "P/L unavailable for this lane";
}

function strategyAnalysisTradeTruthQuality(lane: JsonRecord | null): string {
  const lifecycleClass = String(asRecord(lane?.lifecycle_truth).class ?? "").trim().toUpperCase();
  if (lifecycleClass === "FULL_LIFECYCLE_TRUTH") {
    return "Full trade truth";
  }
  if (lifecycleClass === "HYBRID_ENTRY_BASELINE_EXIT_TRUTH") {
    return "Hybrid trade truth";
  }
  if (lifecycleClass === "BASELINE_ONLY") {
    return "Baseline-only trade truth";
  }
  if (lifecycleClass === "UNSUPPORTED") {
    return "Unsupported trade truth";
  }
  return "Lifecycle Detail Not Available";
}

function strategyAnalysisComparableStatus(lane: JsonRecord | null, lanes: JsonRecord[]): string {
  if (!lane) {
    return "Unknown";
  }
  const laneType = String(lane.lane_type ?? "").trim();
  const hasBenchmarkLane = lanes.some((row) => String(row.lane_type ?? "").trim() === "benchmark_replay");
  if (laneType === "benchmark_replay") {
    return "This is the baseline";
  }
  if (hasBenchmarkLane) {
    return "Yes";
  }
  return "No benchmark loaded";
}

function strategyAnalysisRecommendedNextAction(lane: JsonRecord | null, visibleRowCount: number, lanes: JsonRecord[]): string {
  if (!visibleRowCount) {
    return "Load a reportable lane";
  }
  if (!lane) {
    return "Select a result row";
  }
  if (strategyAnalysisIsReportableLane(lane)) {
    return strategyAnalysisComparableStatus(lane, lanes) === "Yes" ? "Open Report" : "Open Evidence";
  }
  const sourceLane = String(lane.source_lane ?? lane.lane_type ?? "").trim();
  if (sourceLane === "historical_playback" || sourceLane === "benchmark_replay") {
    return "Run a priced replay";
  }
  if (sourceLane === "paper_runtime") {
    return "Inspect evidence";
  }
  return "Load a reportable lane";
}

function strategyAnalysisUnavailableOperatorMessage(lane: JsonRecord | null, visibleRowCount: number): string | null {
  if (!visibleRowCount) {
    return "No reportable run loaded. Load a reportable lane, run a priced replay, or inspect evidence.";
  }
  if (!lane) {
    return "Select a result row to understand report availability.";
  }
  if (strategyMetricAvailable(lane, "net_pnl")) {
    return null;
  }
  const sourceLane = String(lane.source_lane ?? lane.lane_type ?? "").trim();
  if (sourceLane === "historical_playback" || sourceLane === "benchmark_replay") {
    return "Replay loaded, but priced closed-trade path is incomplete. Run a priced replay or inspect evidence.";
  }
  if (sourceLane === "paper_runtime") {
    return "Paper lane attached, but insufficient trade truth for P/L. Inspect evidence or load a more reportable lane.";
  }
  return "This lane is loaded, but it does not publish enough truth for an operator report yet. Inspect evidence.";
}

function strategyAnalysisResultsEmptyStateMessage({
  visibleRowCount,
  runScopeUnavailableReason,
}: {
  visibleRowCount: number;
  runScopeUnavailableReason: string | null;
}): string {
  if (runScopeUnavailableReason) {
    return runScopeUnavailableReason;
  }
  if (!visibleRowCount) {
    return "No reportable run loaded. Load a reportable lane, run a priced replay, or inspect evidence.";
  }
  return "No reportable rows match the current filters. Adjust the view or inspect evidence.";
}

function strategyAnalysisCompareBoardRows(
  left: JsonRecord,
  right: JsonRecord,
  sortField: string,
  direction: string,
): number {
  const leftValue = strategyAnalysisSortValue(left, sortField);
  const rightValue = strategyAnalysisSortValue(right, sortField);
  if (leftValue === null && rightValue === null) {
    return String(left.strategy_display_name ?? left.strategy_key ?? "").localeCompare(String(right.strategy_display_name ?? right.strategy_key ?? ""));
  }
  if (leftValue === null) {
    return 1;
  }
  if (rightValue === null) {
    return -1;
  }
  if (leftValue === rightValue) {
    return String(left.run_study_identity ?? left.display_name ?? "").localeCompare(String(right.run_study_identity ?? right.display_name ?? ""));
  }
  return direction === "asc" ? leftValue - rightValue : rightValue - leftValue;
}

function strategyAnalysisSortValue(row: JsonRecord, sortField: string): number | null {
  const value = asRecord(row.sort_values)[sortField];
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function strategyAnalysisMatchesDateWindow(row: JsonRecord, dateWindow: string): boolean {
  if (dateWindow === "all_dates") {
    return true;
  }
  const latestTimestampSeconds = strategyAnalysisSortValue(row, "latest_update_timestamp");
  if (latestTimestampSeconds === null) {
    return false;
  }
  const nowSeconds = Date.now() / 1000;
  const ageSeconds = nowSeconds - latestTimestampSeconds;
  if (dateWindow === "recent_7d") {
    return ageSeconds <= 7 * 24 * 60 * 60;
  }
  if (dateWindow === "recent_30d") {
    return ageSeconds <= 30 * 24 * 60 * 60;
  }
  if (dateWindow === "recent_90d") {
    return ageSeconds <= 90 * 24 * 60 * 60;
  }
  return true;
}

function strategyAnalysisRunScopeUnavailableReason({
  runScope,
  sortField,
  sortFieldMeta,
}: {
  runScope: string;
  sortField: string;
  sortFieldMeta: JsonRecord;
}): string | null {
  if (sortFieldMeta.available === false) {
    if (runScope === "lowest_drawdown") {
      return "Lowest-drawdown ranking is unavailable because no visible row publishes supported max-drawdown truth.";
    }
    if (runScope === "latest") {
      return "Latest ranking is unavailable because no visible row publishes a sortable latest timestamp.";
    }
    return `Sorting by ${formatValue(sortFieldMeta.label ?? sortField)} is unavailable because the current rows do not publish that metric truth.`;
  }
  return null;
}

function resolveStudyLayout(rowCount: number, preset: StudyHeightPreset): {
  surfaceWidth: number;
  shellMinHeight: number;
  topPaneHeight: number;
  lowerPaneHeight: number;
  panePaddingTop: number;
  panePaddingBottom: number;
  paneGap: number;
} {
  switch (preset) {
    case "compact":
      return {
        surfaceWidth: Math.max(960, rowCount * 10),
        shellMinHeight: 560,
        topPaneHeight: 290,
        lowerPaneHeight: 210,
        panePaddingTop: 18,
        panePaddingBottom: 28,
        paneGap: 20,
      };
    case "expanded":
      return {
        surfaceWidth: Math.max(1120, rowCount * 14),
        shellMinHeight: 780,
        topPaneHeight: 430,
        lowerPaneHeight: 300,
        panePaddingTop: 24,
        panePaddingBottom: 34,
        paneGap: 26,
      };
    case "standard":
    default:
      return {
        surfaceWidth: Math.max(1040, rowCount * 12),
        shellMinHeight: 660,
        topPaneHeight: 360,
        lowerPaneHeight: 250,
        panePaddingTop: 22,
        panePaddingBottom: 32,
        paneGap: 24,
      };
  }
}

function studyBiasClass(value: string): string {
  const normalized = value.toUpperCase();
  if (normalized === "LONG_BIAS") {
    return "long";
  }
  if (normalized === "SHORT_BIAS") {
    return "short";
  }
  return "neutral";
}

function studyPullbackClass(value: string): string {
  const normalized = value.toUpperCase();
  if (normalized === "NORMAL_PULLBACK") {
    return "normal";
  }
  if (normalized === "STRETCHED_PULLBACK") {
    return "stretched";
  }
  if (normalized === "VIOLENT_PULLBACK_DISQUALIFY") {
    return "violent";
  }
  return "none";
}

function studyTimingClass(value: string): string {
  const normalized = value.toUpperCase();
  if (normalized === "ATP_TIMING_CONFIRMED") {
    return "atp-confirmed";
  }
  if (normalized === "ATP_TIMING_WAITING") {
    return "atp-waiting";
  }
  if (normalized === "ATP_TIMING_CHASE_RISK") {
    return "atp-chase";
  }
  if (normalized === "ATP_TIMING_INVALIDATED") {
    return "atp-invalidated";
  }
  return "atp-unavailable";
}

function studyNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function studyTickIndices(length: number, desiredCount: number): number[] {
  if (length <= 0) {
    return [];
  }
  if (length === 1) {
    return [0];
  }
  const indices = new Set<number>();
  for (let step = 0; step < desiredCount; step += 1) {
    indices.add(Math.round((step / Math.max(desiredCount - 1, 1)) * (length - 1)));
  }
  return Array.from(indices).sort((left, right) => left - right);
}

function studyAxisLabel(timestamp: string | undefined): string {
  if (!timestamp) {
    return "-";
  }
  const value = new Date(timestamp);
  if (Number.isNaN(value.getTime())) {
    return timestamp;
  }
  return value.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
}

function buildStudyPath(points: Array<{ x: number; y: number | null }>): string | null {
  const segments: string[] = [];
  let drawing = false;
  points.forEach((point) => {
    if (point.y === null) {
      drawing = false;
      return;
    }
    segments.push(`${drawing ? "L" : "M"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`);
    drawing = true;
  });
  return segments.length ? segments.join(" ") : null;
}

function studyDateLabel(timestamp: string | undefined): string {
  if (!timestamp) {
    return "";
  }
  return timestamp.slice(0, 10);
}

function studyDateInRange(timestamp: string | undefined, start: string, end: string): boolean {
  const value = studyDateLabel(timestamp);
  if (!value) {
    return false;
  }
  if (start && value < start) {
    return false;
  }
  if (end && value > end) {
    return false;
  }
  return true;
}

function studyPnlValue(pnlMode: StudyPnlMode, point: { realized: number | null; openPnl: number | null; total: number | null } | undefined, bar: { realized: number | null; openPnl: number | null; total: number | null }): number | null {
  switch (pnlMode) {
    case "cumulative_realized":
      return point?.realized ?? bar.realized;
    case "unrealized":
      return point?.openPnl ?? bar.openPnl;
    case "cumulative_total":
    default:
      return point?.total ?? bar.total ?? point?.realized ?? bar.realized;
  }
}

function studyPnlClass(pnlMode: StudyPnlMode): string {
  if (pnlMode === "unrealized") {
    return "open";
  }
  if (pnlMode === "cumulative_realized") {
    return "realized";
  }
  return "total";
}

function buildExecutionSliceCoords(args: {
  bars: Array<{ barId: string }>;
  executionSlices: Array<{ sliceId: string; linkedBarId: string; timestamp: string; close: number | null; high: number | null; low: number | null }>;
  xForIndex: (index: number) => number;
}): Array<{ sliceId: string; linkedBarId: string; timestamp: string; x: number; close: number | null; high: number | null; low: number | null }> {
  const barIndexById = new Map(args.bars.map((bar, index) => [bar.barId, index]));
  const grouped = new Map<string, Array<(typeof args.executionSlices)[number]>>();
  args.executionSlices.forEach((slice) => {
    const bucket = grouped.get(slice.linkedBarId) ?? [];
    bucket.push(slice);
    grouped.set(slice.linkedBarId, bucket);
  });
  const coords: Array<{ sliceId: string; linkedBarId: string; timestamp: string; x: number; close: number | null; high: number | null; low: number | null }> = [];
  grouped.forEach((group, barId) => {
    const barIndex = barIndexById.get(barId);
    if (barIndex === undefined) {
      return;
    }
    const centerX = args.xForIndex(barIndex);
    const nextX = args.xForIndex(Math.min(barIndex + 1, args.bars.length - 1));
    const span = Math.max(nextX - centerX, 8);
    const ordered = [...group].sort((left, right) => String(left.timestamp).localeCompare(String(right.timestamp)));
    ordered.forEach((slice, index) => {
      const x = centerX - span / 2 + ((index + 0.5) / Math.max(ordered.length, 1)) * span;
      coords.push({
        sliceId: slice.sliceId,
        linkedBarId: barId,
        timestamp: slice.timestamp,
        x,
        close: slice.close,
        high: slice.high,
        low: slice.low,
      });
    });
  });
  return coords.sort((left, right) => String(left.timestamp).localeCompare(String(right.timestamp)));
}

function studyBarTooltip(
  bar: {
    timestamp: string;
    biasState: string;
    pullbackState: string;
    continuationState: string;
    blockerCode: string;
    atpTimingState: string;
    vwapEntryQualityState: string;
  },
  events: Array<{
    eventType: string;
    executionEventType: string;
    sourceResolution: string;
    decisionContextTimestamp: string;
    eventTimestamp: string;
    family: string;
    reason: string;
    entryModel: string;
    eventPrice: number | null;
    vwapAtEvent: number | null;
    acceptanceState: string;
    invalidationReason: string;
    truthAuthority: string;
  }>,
): string {
  const lines = [
    `Bar ${formatTimestamp(bar.timestamp)}`,
  ];
  if (bar.biasState) {
    lines.push(`Bias: ${bar.biasState}`);
  }
  if (bar.pullbackState) {
    lines.push(`Pullback: ${bar.pullbackState}`);
  }
  if (bar.continuationState) {
    lines.push(`Continuation: ${bar.continuationState}`);
  }
  if (bar.atpTimingState) {
    lines.push(`Timing: ${bar.atpTimingState}`);
  }
  if (bar.vwapEntryQualityState) {
    lines.push(`VWAP quality: ${bar.vwapEntryQualityState}`);
  }
  if (bar.blockerCode) {
    lines.push(`Legacy blocker: ${bar.blockerCode}`);
  }
  events.slice(0, 4).forEach((event) => {
    lines.push(
      `${event.eventType}${event.executionEventType ? ` (${event.executionEventType})` : ""} | ${event.sourceResolution} | context ${formatTimestamp(event.decisionContextTimestamp)} | event ${formatTimestamp(event.eventTimestamp)}${event.entryModel ? ` | ${event.entryModel}` : ""}${event.family ? ` | ${event.family}` : ""}${event.reason ? ` | ${event.reason}` : ""}${event.eventPrice !== null ? ` | px ${formatShortNumber(event.eventPrice)}` : ""}${event.vwapAtEvent !== null ? ` | VWAP ${formatShortNumber(event.vwapAtEvent)}` : ""}${event.acceptanceState ? ` | ${event.acceptanceState}` : ""}${event.invalidationReason ? ` | invalidation ${event.invalidationReason}` : ""}`,
    );
  });
  return lines.join("\n");
}

function buildStudyEventCoords(args: {
  bars: Array<{ barId: string }>;
  tradeEvents: Array<{
    eventId: string;
    linkedBarId: string;
    linkedSubbarId: string;
    eventType: string;
    executionEventType: string;
    side: string;
    family: string;
    reason: string;
    sourceResolution: string;
    decisionContextTimestamp: string;
    eventTimestamp: string;
    entryModel: string;
    eventPrice: number | null;
    vwapAtEvent: number | null;
    acceptanceState: string;
    invalidationReason: string;
    truthAuthority: string;
  }>;
  executionCoords: Array<{ sliceId: string; linkedBarId: string; timestamp: string; x: number }>;
  xForIndex: (index: number) => number;
  topMarkerY: number;
  intrabarMarkerY: number;
}): Array<{
  eventId: string;
  eventType: string;
  executionEventType: string;
  sourceResolution: string;
  side: string;
  family: string;
  reason: string;
  decisionContextTimestamp: string;
  eventTimestamp: string;
  entryModel: string;
  eventPrice: number | null;
  vwapAtEvent: number | null;
  acceptanceState: string;
  invalidationReason: string;
  truthAuthority: string;
  x: number;
  y: number;
}> {
  const barIndexById = new Map(args.bars.map((bar, index) => [bar.barId, index]));
  const executionXByTimestamp = new Map(args.executionCoords.map((item) => [item.timestamp, item.x]));
  return args.tradeEvents.flatMap((event) => {
    const barIndex = barIndexById.get(event.linkedBarId);
    const fallbackX = barIndex === undefined ? null : args.xForIndex(barIndex);
    const intrabarX =
      executionXByTimestamp.get(event.linkedSubbarId) ??
      executionXByTimestamp.get(event.eventTimestamp) ??
      fallbackX;
    const x = event.sourceResolution === "INTRABAR" ? intrabarX : fallbackX;
    if (x === null) {
      return [];
    }
    return [
      {
        eventId: event.eventId,
        eventType: event.eventType,
        executionEventType: event.executionEventType,
        sourceResolution: event.sourceResolution,
        side: event.side,
        family: event.family,
        reason: event.reason,
        decisionContextTimestamp: event.decisionContextTimestamp,
        eventTimestamp: event.eventTimestamp,
        entryModel: event.entryModel,
        eventPrice: event.eventPrice,
        vwapAtEvent: event.vwapAtEvent,
        acceptanceState: event.acceptanceState,
        invalidationReason: event.invalidationReason,
        truthAuthority: event.truthAuthority,
        x,
        y: event.sourceResolution === "INTRABAR" ? args.intrabarMarkerY : args.topMarkerY,
      },
    ];
  });
}

function studyEventShape(eventType: string): "circle" | "square" | "diamond" {
  const normalized = eventType.toUpperCase();
  if (normalized.includes("BLOCKED")) {
    return "square";
  }
  if (normalized.includes("TIMING") || normalized.includes("INTENT")) {
    return "diamond";
  }
  return "circle";
}

function studyEventToneClass(eventType: string): string {
  const normalized = eventType.toUpperCase();
  if (normalized.includes("EXIT")) {
    return "exit";
  }
  if (normalized.includes("INVALIDATED") || normalized.includes("CHASE_RISK")) {
    return "blocked";
  }
  if (normalized.includes("BLOCKED")) {
    return "blocked";
  }
  if (normalized.includes("TIMING")) {
    return "timing";
  }
  if (normalized.includes("READY") || normalized.includes("ELIGIBLE")) {
    return "eligible";
  }
  return "entry";
}

function studyEventSourceClass(sourceResolution: string): string {
  return sourceResolution === "INTRABAR" ? "intrabar" : "bar-context";
}

function studyEventTooltip(event: {
  eventType: string;
  sourceResolution: string;
  decisionContextTimestamp: string;
  eventTimestamp: string;
  executionEventType: string;
  family: string;
  reason: string;
  side: string;
  entryModel: string;
  eventPrice: number | null;
  vwapAtEvent: number | null;
  acceptanceState: string;
  invalidationReason: string;
  truthAuthority: string;
}): string {
  return [
    event.eventType,
    event.executionEventType ? `Execution event: ${event.executionEventType}` : null,
    `Source: ${event.sourceResolution}`,
    event.entryModel ? `Entry model: ${event.entryModel}` : null,
    `Decision context: ${formatTimestamp(event.decisionContextTimestamp)}`,
    `Event time: ${formatTimestamp(event.eventTimestamp)}`,
    event.side ? `Side: ${event.side}` : null,
    event.family ? `Family: ${event.family}` : null,
    event.reason ? `Reason: ${event.reason}` : null,
    event.eventPrice !== null ? `Price: ${formatShortNumber(event.eventPrice)}` : null,
    event.vwapAtEvent !== null ? `VWAP: ${formatShortNumber(event.vwapAtEvent)}` : null,
    event.acceptanceState ? `Acceptance: ${event.acceptanceState}` : null,
    event.invalidationReason ? `Invalidation: ${event.invalidationReason}` : null,
    event.truthAuthority ? `Truth: ${event.truthAuthority}` : null,
  ]
    .filter((value): value is string => Boolean(value))
    .join("\n");
}
