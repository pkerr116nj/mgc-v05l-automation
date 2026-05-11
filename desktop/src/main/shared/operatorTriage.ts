type JsonRecord = Record<string, unknown>;

export type TradeAuthorityState = "Enabled" | "Blocked";
export type PositionPostureState = "Flat" | "In Position";
export type OutagePostureState = "None" | "Review" | "Critical";
export type ConnectionPostureState = "Live" | "Snapshot" | "Unavailable";
export type RuntimePostureState = "Ready" | "Degraded" | "Reconciling" | "Faulted";
export type OperatorTriageGateStatus = "pass" | "fail";
export type OperatorTriageGateKey =
  | "market-data"
  | "broker-authority"
  | "reconciliation"
  | "runtime-readiness"
  | "operator-authority";
export type OperatorTriageTruthSourceKind =
  | "broker_authoritative"
  | "operator_surface_strategy_ledger"
  | "stitched_payload_derived"
  | "inferred_fallback"
  | "placeholder"
  | "unavailable";
export type OperatorTriageTruthSourceAuthority = "stitched" | "fallback" | "placeholder" | "unavailable" | "mixed";
export type OperatorTriageTruthSourceFamily = "broker" | "operator_surface" | "dashboard" | "desktop" | "mixed" | "placeholder" | "unknown";
export type OperatorTriageTruthAuthorityLevel = "authoritative" | "derived" | "fallback" | "placeholder" | "unavailable";
export type OperatorTriageContractOrigin = "client_shim" | "backend_canonical";

export interface OperatorTriageTruthSource {
  kind: OperatorTriageTruthSourceKind;
  authority: OperatorTriageTruthSourceAuthority;
  source_family: OperatorTriageTruthSourceFamily;
  authority_level: OperatorTriageTruthAuthorityLevel;
  contract_origin: OperatorTriageContractOrigin;
  label: string;
  is_fallback: boolean;
  source_paths: string[];
  detail: string | null;
}

export interface OperatorTriageDominantBlocker {
  code: string;
  label: string;
}

export interface OperatorTriageRootCause {
  layer: "market_data" | "broker" | "reconciliation" | "runtime" | "operator_authority" | "alerts" | "unknown";
  code: string;
  detail: string;
}

export interface OperatorTriageHardGate {
  key: OperatorTriageGateKey;
  label: string;
  status: OperatorTriageGateStatus;
  reason: string;
  checked_at: string | null;
  source_timestamp: string | null;
  freshness_seconds: number | null;
  truth_source: OperatorTriageTruthSource;
}

export interface OperatorTriageCurrentExposure {
  truth_source: OperatorTriageTruthSource;
  symbol: string | null;
  side: string | null;
  quantity: number | null;
  average_price: number | null;
  mark_price: number | null;
  as_of: string | null;
  risk_state: string | null;
}

export interface OperatorTriageTodayPnL {
  truth_source: OperatorTriageTruthSource;
  realized: number | null;
  unrealized: number | null;
  net: number | null;
  session_drawdown: number | null;
  as_of: string | null;
}

export interface OperatorTriageFallback {
  truth_source: OperatorTriageTruthSource;
  runbook_path: string | null;
  broker_desk_phone: string | null;
  runbook_truth_source: OperatorTriageTruthSource;
  broker_desk_phone_truth_source: OperatorTriageTruthSource;
}

export interface OperatorTriageOperatorAuthority {
  truth_source: OperatorTriageTruthSource;
  auth_available: boolean;
  session_active: boolean;
  session_expires_at: string | null;
  sensitive_actions_allowed: boolean;
  blocker: string | null;
}

export interface OperatorTriage {
  pilot_symbol: string;
  paper_trade_authority: TradeAuthorityState;
  paper_trade_allowed: boolean;
  paper_trade_block_reason: string | null;
  live_trade_authority: TradeAuthorityState;
  live_trade_allowed: boolean;
  live_trade_block_reason: string | null;
  paper_runtime_ready: boolean;
  live_runtime_ready: boolean;
  paper_bridge_allowed: boolean;
  live_bridge_allowed: boolean;
  paper_readiness_source: string | null;
  paper_readiness_timestamp: string | null;
  session_eligible_count: number;
  live_capable_count: number;
  waiting_for_bar_count: number;
  market_data_stale_count: number;
  no_setup_count: number;
  actionable_now_count: number;
  true_blocked_count: number;
  advisory_fault_count: number;
  blocking_fault_count: number;
  position_posture: PositionPostureState;
  outage_posture: OutagePostureState;
  connection_posture: ConnectionPostureState;
  runtime_posture: RuntimePostureState;
  track_b_paper_status_code: string | null;
  track_b_paper_status_message: string | null;
  track_b_legacy_market_data_note: string | null;
  verdict_sentence: string;
  dominant_blocker: OperatorTriageDominantBlocker;
  root_cause: OperatorTriageRootCause;
  hard_gates: OperatorTriageHardGate[];
  current_exposure: OperatorTriageCurrentExposure;
  today_pnl: OperatorTriageTodayPnL;
  fallback: OperatorTriageFallback;
  operator_authority: OperatorTriageOperatorAuthority;
}

export interface OperatorTriageContract {
  operator_triage: OperatorTriage;
}

export interface OperatorLaneSemantics {
  cadence_state: string | null;
  cadence_reason: string | null;
  latest_hard_blocker: string | null;
  live_capable: boolean;
  actionable_this_bar: boolean;
  true_blocked: boolean;
}

export interface OperatorTriageInput {
  desktopSourceMode?: string | null;
  desktopRefreshedAt?: unknown;
  dashboardGeneratedAt?: unknown;
  global?: JsonRecord | null;
  operatorSurface?: JsonRecord | null;
  runtimeReadiness?: JsonRecord | null;
  runtimeValues?: JsonRecord | null;
  paperReadiness?: JsonRecord | null;
  trackBPaperTrading?: JsonRecord | null;
  portfolio?: JsonRecord | null;
  laneRows?: JsonRecord[] | null;
  currentPositions?: JsonRecord[] | null;
  productionLinkEnabled: boolean;
  productionLink?: JsonRecord | null;
  productionCapabilities?: JsonRecord | null;
  productionPilotScope?: JsonRecord | null;
  productionLastManualOrderPreview?: JsonRecord | null;
  productionPositions?: JsonRecord[] | null;
  productionReconciliation?: JsonRecord | null;
  productionHealth?: JsonRecord | null;
  productionDiagnostics?: JsonRecord | null;
  productionBalances?: JsonRecord | null;
  localOperatorAuth?: JsonRecord | null;
  operatorActiveAlertRows?: JsonRecord[] | null;
  operatorRecentAlertRows?: JsonRecord[] | null;
  sameUnderlyingConflictSummary?: JsonRecord | null;
}

const NY_TIMESTAMP_FORMATTER = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York",
  year: "numeric",
  month: "numeric",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
  second: "2-digit",
});

function asArray<T>(value: unknown): T[] {
  return Array.isArray(value) ? (value as T[]) : [];
}

function asRecord(value: unknown): JsonRecord {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as JsonRecord) : {};
}

const CADENCE_STATE_CODES = new Set([
  "WAITING_FOR_BAR_CLOSE",
  "BAR_RECEIVED_NOT_PROCESSED_YET",
  "BAR_PROCESSED_CURRENT",
  "READY_NO_SETUP",
  "NO_SETUP_OBSERVED",
  "NO_NEW_COMPLETED_BAR",
]);

function normalizedUpperToken(value: unknown): string {
  return String(value ?? "").trim().toUpperCase();
}

function isRiskHaltState(value: unknown): boolean {
  const riskState = normalizedUpperToken(value);
  return Boolean(riskState) && !["", "OK", "CLEAR", "READY"].includes(riskState);
}

export function deriveOperatorLaneSemantics(row: JsonRecord | null | undefined): OperatorLaneSemantics {
  const record = asRecord(row);
  const latestGatingState = asRecord(record.latest_gating_state);
  const cadenceState = normalizedUpperToken(record.bar_state || record.fireability_classification || record.current_signal_state) || null;
  const cadenceReason = String(record.bar_state_reason ?? record.audit_reason ?? record.eligibility_detail ?? "").trim() || null;
  const firstTrueBlocker = String(record.first_true_blocker ?? "").trim() || null;
  const effectiveReason = String(
    record.effective_readiness_eligibility_reason
      ?? record.eligibility_reason
      ?? latestGatingState.latest_fault_or_blocker
      ?? record.latest_fault_or_blocker
      ?? "",
  ).trim() || null;
  const marketDataStale = record.market_data_stale === true;
  const routeReady = record.route_ready !== false;
  const governanceAllowed = record.governance_allowed !== false;
  const entriesEnabled = record.entries_enabled !== false;
  const operatorHalt = record.operator_halt === true;
  const riskHalt = isRiskHaltState(record.risk_state ?? latestGatingState.risk_state);
  const sessionEligible = record.session_eligible === true;
  const actionableThisBar = record.can_fire_now === true || record.actionable_now === true || record.eligible_now === true;
  const staleRuntime = record.runtime_stale_effective === true || record.runtime_stale_observed === true || record.data_fresh === false;

  const hardBlockerCandidate =
    marketDataStale
      ? "market_data_stale"
      : operatorHalt
        ? "operator_halt"
        : riskHalt
          ? (String(record.halt_reason ?? "").trim() || "risk_halt")
          : !routeReady
            ? "route_not_ready"
            : !governanceAllowed || !entriesEnabled
              ? "governance_disabled"
              : firstTrueBlocker
                ? firstTrueBlocker
                : effectiveReason;
  const normalizedHardBlocker = hardBlockerCandidate ? normalizedUpperToken(hardBlockerCandidate) : "";
  const latestHardBlocker = normalizedHardBlocker && !CADENCE_STATE_CODES.has(normalizedHardBlocker)
    ? hardBlockerCandidate
    : null;
  const trueBlocked = Boolean(
    record.true_blocked === true
    || record.blocked_lane === true
    || latestHardBlocker
    || (record.session_eligible === false && normalizedUpperToken(effectiveReason) === "WRONG_SESSION"),
  );
  const liveCapable = Boolean(
    sessionEligible
    && routeReady
    && governanceAllowed
    && entriesEnabled
    && !operatorHalt
    && !riskHalt
    && !marketDataStale
    && !staleRuntime
    && !trueBlocked,
  );
  return {
    cadence_state: cadenceState,
    cadence_reason: cadenceReason,
    latest_hard_blocker: latestHardBlocker,
    live_capable: liveCapable,
    actionable_this_bar: actionableThisBar,
    true_blocked: trueBlocked,
  };
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "Unavailable";
  }
  if (typeof value === "boolean") {
    return value ? "Yes" : "No";
  }
  if (Array.isArray(value)) {
    return value.length ? value.join(", ") : "None";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

function formatTimestamp(value: unknown): string {
  if (!value || typeof value !== "string") {
    return "Unavailable";
  }
  const parsed = Date.parse(value);
  if (Number.isNaN(parsed)) {
    return value;
  }
  return `${NY_TIMESTAMP_FORMATTER.format(new Date(parsed))} ET`;
}

function formatRelativeAge(value: unknown): string {
  if (!value || typeof value !== "string") {
    return "Unavailable";
  }
  const parsed = Date.parse(value);
  if (Number.isNaN(parsed)) {
    return value;
  }
  const deltaMs = Date.now() - parsed;
  const absoluteSeconds = Math.max(0, Math.round(deltaMs / 1000));
  if (absoluteSeconds < 60) {
    return `${absoluteSeconds}s ago`;
  }
  const minutes = Math.round(absoluteSeconds / 60);
  if (minutes < 60) {
    return `${minutes}m ago`;
  }
  const hours = Math.round(minutes / 60);
  if (hours < 48) {
    return `${hours}h ago`;
  }
  const days = Math.round(hours / 24);
  return `${days}d ago`;
}

function textOrFallback(value: unknown, fallback: string): string {
  if (value === null || value === undefined) {
    return fallback;
  }
  const text = String(value).trim();
  return text ? text : fallback;
}

function normalizeScopeSymbol(value: unknown): string {
  return String(value ?? "")
    .trim()
    .toUpperCase()
    .replace(/^\//, "");
}

function symbolMatchesScope(candidate: unknown, scopeSymbol: string): boolean {
  const normalizedScope = normalizeScopeSymbol(scopeSymbol);
  const normalizedCandidate = normalizeScopeSymbol(candidate);
  if (!normalizedScope || !normalizedCandidate) {
    return false;
  }
  return normalizedCandidate === normalizedScope || normalizedCandidate.startsWith(normalizedScope);
}

function firstNonEmptyString(...values: unknown[]): string | null {
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (text) {
      return text;
    }
  }
  return null;
}

function numericOrNull(value: unknown): number | null {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const numeric = Number(value);
  return Number.isNaN(numeric) ? null : numeric;
}

interface TrackBPaperReadinessSummary {
  available: boolean;
  code: string | null;
  message: string | null;
  ready: boolean;
  evaluating: boolean;
  waitingForCompletedBar: boolean;
  blockedFeatureContext: boolean;
  blockedLiveExecution: boolean;
  diagnosticStale: boolean;
  reviewRequired: boolean;
  legacyMarketDataNote: string | null;
}

function summarizeTrackBPaperReadiness(payload: JsonRecord): TrackBPaperReadinessSummary {
  const startup = asRecord(payload.startup_readiness_diagnostic);
  const zeroActivity = asRecord(payload.zero_activity_diagnostic);
  const phase1Gc = asRecord(payload.phase1_gc_readiness);
  const completedAudit = asRecord(zeroActivity.completed_decision_bar_audit);
  const instruments = Object.values(asRecord(startup.instruments)).map((row) => asRecord(row));
  const configured = instruments.length > 0;
  const paperAllowedRows = instruments.filter((row) => row.paper_evaluation_allowed === true);
  const liveBlockedRows = instruments.filter((row) => row.live_execution_approved === false);
  const featureBlockedRows = instruments.filter((row) => row.context_ready === false || row.feature_context_ready === false);
  const reviewRequired = payload.critical === true || Number(payload.review_required_count ?? 0) > 0;
  const diagnosticStale = zeroActivity.stale === true || String(zeroActivity.diagnosis_classification ?? "") === "STALE_DIAGNOSTIC";
  const phase1GcReadyForWatch = phase1Gc.ready_for_guarded_paper_watch === true && phase1Gc.live_money_eligible !== true;
  const auditClassification = String(completedAudit.classification ?? "").trim().toUpperCase();
  const latestMonitorVerdict = String(zeroActivity.latest_monitor_verdict ?? "").trim().toUpperCase();
  const signalsSeen = Number(zeroActivity.signals_seen ?? 0) || 0;
  const recentEvaluated = Number(zeroActivity.recent_cycles_evaluated ?? 0) || 0;
  const waitingForCompletedBar =
    latestMonitorVerdict.includes("NO_NEW_COMPLETED_5M_BAR")
    || latestMonitorVerdict.includes("WAITING_NEW_COMPLETED_BAR")
    || auditClassification === "EVALUATING_EACH_COMPLETED_BAR";
  const evaluating = paperAllowedRows.length > 0 && (
    auditClassification === "EVALUATING_EACH_COMPLETED_BAR"
    || recentEvaluated > 0
    || Number(zeroActivity.strategies_evaluated ?? 0) > 0
  );
  const ready = (paperAllowedRows.length > 0 && !diagnosticStale && !reviewRequired) || (phase1GcReadyForWatch && !reviewRequired);
  const blockedLiveExecution = configured && liveBlockedRows.length > 0 && paperAllowedRows.length === 0;
  const blockedFeatureContext = configured && featureBlockedRows.length > 0 && paperAllowedRows.length === 0;
  let code: string | null = null;
  let message: string | null = null;
  if (!configured && payload.available !== true && !phase1GcReadyForWatch) {
    code = null;
    message = null;
  } else if (reviewRequired) {
    code = "TRACK_B_PAPER_REVIEW_REQUIRED";
    message = "Track B PAPER review is required before interpreting autonomous trading status as clean.";
  } else if (phase1GcReadyForWatch) {
    code = "GC_PHASE1_READY_FOR_GUARDED_PAPER_WATCH";
    message = "GC Phase-1 candidate is ready for guarded PAPER watch after current monday-live preflight; legacy lifecycle diagnostics remain read-only context.";
  } else if (diagnosticStale) {
    code = "TRACK_B_PAPER_DIAGNOSTIC_STALE";
    message = "Track B PAPER diagnostic is stale; refresh or inspect the latest monitor artifact.";
  } else if (blockedLiveExecution) {
    const names = liveBlockedRows.map((row) => String(row.instrument ?? row.instrument_family ?? "")).filter(Boolean);
    code = "TRACK_B_PAPER_BLOCKED_LIVE_EXECUTION";
    message = `Track B PAPER blocked: live execution freshness failed${names.length ? ` for ${names.join(", ")}` : ""}.`;
  } else if (blockedFeatureContext) {
    const names = featureBlockedRows.map((row) => String(row.instrument ?? row.instrument_family ?? "")).filter(Boolean);
    code = "TRACK_B_PAPER_BLOCKED_FEATURE_CONTEXT";
    message = `Track B PAPER blocked: feature context not ready${names.length ? ` for ${names.join(", ")}` : ""}.`;
  } else if (evaluating && signalsSeen === 0) {
    code = "TRACK_B_PAPER_READY_NO_SIGNAL";
    message = "Track B PAPER evaluating live decision bars; no trade signals observed.";
  } else if (waitingForCompletedBar && ready) {
    code = "TRACK_B_PAPER_WAITING_NEW_COMPLETED_BAR";
    message = "Track B PAPER ready; waiting for the next completed decision bar.";
  } else if (ready) {
    code = "TRACK_B_PAPER_EVALUATING";
    message = "Track B PAPER live path is ready for eligible strategy evaluation.";
  }
  return {
    available: Boolean(code),
    code,
    message,
    ready,
    evaluating,
    waitingForCompletedBar,
    blockedFeatureContext,
    blockedLiveExecution,
    diagnosticStale,
    reviewRequired,
    legacyMarketDataNote: code
      ? "Legacy paper market-data status is separate from Track B Databento Live execution readiness."
      : null,
  };
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

function truthFreshnessLabel(updatedAt: unknown, fallbackUpdatedAt?: unknown): string {
  const timestamp = typeof updatedAt === "string" && updatedAt.trim()
    ? updatedAt
    : typeof fallbackUpdatedAt === "string" && fallbackUpdatedAt.trim()
      ? fallbackUpdatedAt
      : null;
  if (!timestamp) {
    return "Unavailable";
  }
  return `${formatRelativeAge(timestamp)} • ${formatTimestamp(timestamp)}`;
}

function gateFreshnessLabel(updatedAt: unknown, fallbackUpdatedAt?: unknown): string | null {
  const label = truthFreshnessLabel(updatedAt, fallbackUpdatedAt);
  return label === "Unavailable" ? null : label;
}

function connectionPostureLabel(sourceMode: string | undefined): ConnectionPostureState {
  switch (sourceMode) {
    case "live_api":
      return "Live";
    case "snapshot_fallback":
    case "attached_snapshot_bridge":
    case "degraded_reconnecting":
      return "Snapshot";
    case "backend_down":
    default:
      return "Unavailable";
  }
}

function combineAuthorities(authorities: OperatorTriageTruthSourceAuthority[]): OperatorTriageTruthSourceAuthority {
  const filtered = authorities.filter(Boolean);
  if (!filtered.length) {
    return "unavailable";
  }
  const unique = new Set(filtered);
  return unique.size === 1 ? filtered[0] : "mixed";
}

function timestampOrNull(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value : null;
}

function freshnessSeconds(sourceTimestamp: string | null, checkedAt: string | null): number | null {
  const effectiveTimestamp = sourceTimestamp ?? checkedAt;
  if (!effectiveTimestamp) {
    return null;
  }
  const sourceMs = Date.parse(effectiveTimestamp);
  const checkedMs = checkedAt ? Date.parse(checkedAt) : Date.now();
  if (!Number.isFinite(sourceMs) || !Number.isFinite(checkedMs)) {
    return null;
  }
  return Math.max(0, Math.round((checkedMs - sourceMs) / 1000));
}

function buildTruthSource(input: {
  kind: OperatorTriageTruthSourceKind;
  authority: OperatorTriageTruthSourceAuthority;
  sourceFamily: OperatorTriageTruthSourceFamily;
  authorityLevel: OperatorTriageTruthAuthorityLevel;
  contractOrigin?: OperatorTriageContractOrigin;
  label: string;
  isFallback?: boolean;
  sourcePaths?: string[];
  detail?: string | null;
}): OperatorTriageTruthSource {
  return {
    kind: input.kind,
    authority: input.authority,
    source_family: input.sourceFamily,
    authority_level: input.authorityLevel,
    contract_origin: input.contractOrigin ?? "client_shim",
    label: input.label,
    is_fallback: input.isFallback === true,
    source_paths: input.sourcePaths ?? [],
    detail: input.detail ?? null,
  };
}

function sourcePathList(...values: Array<unknown>): string[] {
  return values
    .map((value) => String(value ?? "").trim())
    .filter((value, index, all) => value.length > 0 && all.indexOf(value) === index);
}

function hardGateLayerForKey(key: OperatorTriageGateKey): OperatorTriageRootCause["layer"] {
  switch (key) {
    case "market-data":
      return "market_data";
    case "broker-authority":
      return "broker";
    case "reconciliation":
      return "reconciliation";
    case "runtime-readiness":
      return "runtime";
    case "operator-authority":
      return "operator_authority";
    default:
      return "unknown";
  }
}

function hardGateCodeForKey(key: OperatorTriageGateKey): string {
  return key.replace(/-/g, "_");
}

function buildExposureSummary(
  input: OperatorTriageInput,
  pilotSymbol: string,
  productionReconciliation: JsonRecord,
): OperatorTriageCurrentExposure {
  const operatorSurface = asRecord(input.operatorSurface);
  const currentActivePositions = asRecord(operatorSurface.current_active_positions);
  const currentActivePositionSources = asRecord(currentActivePositions.field_sources);
  const productionPositions = asArray<JsonRecord>(input.productionPositions);
  const currentPositions = asArray<JsonRecord>(input.currentPositions);
  const laneRows = asArray<JsonRecord>(input.laneRows);
  const scopedBrokerPositions = productionPositions.filter((row) => symbolMatchesScope(row.symbol, pilotSymbol));
  const scopedBrokerQuantity = sumNullable(scopedBrokerPositions.map((row) => numericOrNull(row.quantity))) ?? 0;
  const scopedBrokerPosition = scopedBrokerPositions[0] ?? null;
  const scopedPaperPosition =
    currentPositions.find((row) => {
      const explicitOpen = row.open_position === true;
      const side = String(row.side ?? row.position_side ?? "").trim().toUpperCase();
      const quantity = numericOrNull(row.quantity ?? row.qty ?? row.current_quantity);
      return explicitOpen || side === "LONG" || side === "SHORT" || (quantity !== null && Math.abs(quantity) > 0);
    })
    ?? laneRows.find((row) => row.open_position === true)
    ?? null;

  if (scopedBrokerPositions.length && Math.abs(scopedBrokerQuantity) > 0) {
    const absoluteQuantity = Math.abs(scopedBrokerQuantity);
    const weightedAverageCost = weightedAverage(
      scopedBrokerPositions.map((row) => ({
        value: numericOrNull(row.average_cost),
        weight: numericOrNull(row.quantity),
      })),
    );
    const mark = averageNullable(scopedBrokerPositions.map((row) => numericOrNull(row.mark_price)));
    const timestamp = latestTimestamp(scopedBrokerPositions.flatMap((row) => [row.fetched_at, row.quote_fetched_at]));
    return {
      truth_source: buildTruthSource({
        kind: "broker_authoritative",
        authority: "stitched",
        sourceFamily: "broker",
        authorityLevel: "authoritative",
        label: "Broker Truth",
        sourcePaths: sourcePathList(
          "production_link.portfolio.positions[].symbol",
          "production_link.portfolio.positions[].quantity",
          "production_link.portfolio.positions[].average_cost",
          "production_link.portfolio.positions[].mark_price",
          "production_link.portfolio.positions[].fetched_at",
          "production_link.portfolio.positions[].quote_fetched_at",
        ),
        detail: "Exposure fields are broker-authoritative within the current client-shim contract; the contract itself is still built from stitched broker payloads rather than a backend-published operator_triage object.",
      }),
      symbol: String((scopedBrokerPosition?.symbol ?? pilotSymbol) || "Unknown"),
      side: String(scopedBrokerPosition?.side ?? (scopedBrokerQuantity < 0 ? "SHORT" : "LONG")).trim().toUpperCase(),
      quantity: absoluteQuantity,
      average_price: weightedAverageCost,
      mark_price: mark,
      as_of: timestamp,
      risk_state: formatValue(productionReconciliation.label ?? productionReconciliation.status ?? "Unknown"),
    };
  }

  if (scopedPaperPosition) {
    const quantity = numericOrNull(scopedPaperPosition.quantity ?? scopedPaperPosition.qty ?? scopedPaperPosition.current_quantity);
    const side = firstNonEmptyString(
      scopedPaperPosition.side,
      scopedPaperPosition.position_side,
      quantity !== null && quantity < 0 ? "SHORT" : quantity !== null && quantity > 0 ? "LONG" : "",
    ) ?? "LONG";
    return {
      truth_source: buildTruthSource({
        kind: "inferred_fallback",
        authority: "fallback",
        sourceFamily: "operator_surface",
        authorityLevel: "fallback",
        label: currentPositions.includes(scopedPaperPosition) ? "Paper Runtime Fallback" : "Lane Fallback",
        isFallback: true,
        sourcePaths: currentPositions.includes(scopedPaperPosition)
          ? sourcePathList(
              asRecord(currentActivePositionSources.rows).field_path,
              "operator_surface.current_active_positions.rows[].instrument",
              "operator_surface.current_active_positions.rows[].side",
              "operator_surface.current_active_positions.rows[].quantity",
              "operator_surface.current_active_positions.rows[].avg_entry_price",
              "operator_surface.current_active_positions.rows[].mark_price",
            )
          : sourcePathList(
              "operator_surface.lane_rows[].instrument",
              "operator_surface.lane_rows[].open_position",
            ),
        detail: "Broker-authoritative exposure was unavailable, so posture and values fall back to operator-surface paper/runtime hints.",
      }),
      symbol: formatValue(scopedPaperPosition.instrument ?? scopedPaperPosition.symbol ?? pilotSymbol),
      side: formatValue(side).toUpperCase(),
      quantity: quantity !== null ? Math.abs(quantity) : numericOrNull(scopedPaperPosition.quantity ?? scopedPaperPosition.qty),
      average_price: numericOrNull(
        scopedPaperPosition.avg_entry_price
        ?? scopedPaperPosition.average_entry_price
        ?? scopedPaperPosition.entry_price,
      ),
      mark_price: numericOrNull(
        scopedPaperPosition.mark_price
        ?? scopedPaperPosition.last_mark
        ?? scopedPaperPosition.current_mark,
      ),
      as_of: timestampOrNull(
        scopedPaperPosition.latest_timestamp
        ?? scopedPaperPosition.latest_activity_timestamp,
      ),
      risk_state: formatValue(
        scopedPaperPosition.active_exit
        ?? scopedPaperPosition.warning_summary
        ?? "Monitoring",
      ),
    };
  }

  return {
    truth_source: buildTruthSource({
      kind: "stitched_payload_derived",
      authority: "stitched",
      sourceFamily: "mixed",
      authorityLevel: "derived",
      label: pilotSymbol ? `Scoped To ${pilotSymbol}` : "No Live Exposure",
      sourcePaths: sourcePathList("operator_scope.symbol", "desktop_state.refreshedAt", "dashboard.generated_at"),
      detail: "No open exposure was found in broker or operator-surface fallback rows; flat posture is inferred from the stitched operator payload.",
    }),
    symbol: pilotSymbol || null,
    side: null,
    quantity: 0,
    average_price: null,
    mark_price: null,
    as_of: latestTimestamp([input.desktopRefreshedAt, input.dashboardGeneratedAt]),
    risk_state: "No open exposure",
  };
}

function resolvePnlField(
  primaryValue: unknown,
  fallbackValue: unknown,
  primaryPath: string,
  fallbackPath: string,
): { value: number | null; authority: OperatorTriageTruthSourceAuthority; path: string; fallbackUsed: boolean } {
  const primaryNumeric = numericOrNull(primaryValue);
  if (primaryNumeric !== null) {
    return { value: primaryNumeric, authority: "stitched", path: primaryPath, fallbackUsed: false };
  }
  const fallbackNumeric = numericOrNull(fallbackValue);
  if (fallbackNumeric !== null) {
    return { value: fallbackNumeric, authority: "fallback", path: fallbackPath, fallbackUsed: true };
  }
  return { value: null, authority: "unavailable", path: fallbackPath, fallbackUsed: true };
}

function buildTodayPnLSummary(portfolio: JsonRecord, asOf: string | null): OperatorTriageTodayPnL {
  const portfolioValues = asRecord(portfolio.values);
  const fieldSources = asRecord(portfolio.field_sources);
  const realized = resolvePnlField(
    portfolio.daily_realized_pnl,
    portfolioValues.daily_realized_pnl,
    "operator_surface.operator_metrics_portfolio.daily_realized_pnl",
    "operator_surface.operator_metrics_portfolio.values.daily_realized_pnl",
  );
  const unrealized = resolvePnlField(
    portfolio.daily_unrealized_pnl,
    portfolioValues.daily_unrealized_pnl,
    "operator_surface.operator_metrics_portfolio.daily_unrealized_pnl",
    "operator_surface.operator_metrics_portfolio.values.daily_unrealized_pnl",
  );
  const net = resolvePnlField(
    portfolio.daily_net_pnl,
    portfolioValues.daily_net_pnl,
    "operator_surface.operator_metrics_portfolio.daily_net_pnl",
    "operator_surface.operator_metrics_portfolio.values.daily_net_pnl",
  );
  const drawdown = resolvePnlField(
    portfolio.intraday_max_drawdown,
    portfolioValues.intraday_max_drawdown,
    "operator_surface.operator_metrics_portfolio.intraday_max_drawdown",
    "operator_surface.operator_metrics_portfolio.values.intraday_max_drawdown",
  );
  const sourceAuthority = combineAuthorities([realized.authority, unrealized.authority, net.authority, drawdown.authority]);
  const fallbackUsed = realized.fallbackUsed || unrealized.fallbackUsed || net.fallbackUsed || drawdown.fallbackUsed;
  return {
    truth_source: buildTruthSource({
      kind: "operator_surface_strategy_ledger",
      authority: sourceAuthority,
      sourceFamily: "operator_surface",
      authorityLevel: fallbackUsed ? "fallback" : "derived",
      label: fallbackUsed
        ? "Strategy Ledger Fallback"
        : "Strategy Ledger",
      isFallback: fallbackUsed,
      sourcePaths: sourcePathList(
        asRecord(fieldSources.daily_realized_pnl).field_path,
        asRecord(fieldSources.daily_unrealized_pnl).field_path,
        asRecord(fieldSources.daily_net_pnl).field_path,
        asRecord(fieldSources.intraday_max_drawdown).field_path,
        realized.path,
        unrealized.path,
        net.path,
        drawdown.path,
      ),
      detail: "Today P&L comes from operator-surface / strategy-ledger payloads, not broker-authoritative account truth. When primary fields are null, the contract falls back to operator-surface values.* fields.",
    }),
    realized: realized.value,
    unrealized: unrealized.value,
    net: net.value,
    session_drawdown: drawdown.value,
    as_of: asOf,
  };
}

export function buildOperatorTriageContract(input: OperatorTriageInput): OperatorTriageContract {
  const global = asRecord(input.global);
  const operatorSurface = asRecord(input.operatorSurface);
  const runtimeReadiness = asRecord(input.runtimeReadiness);
  const runtimeValues = asRecord(input.runtimeValues);
  const paperReadiness = asRecord(input.paperReadiness);
  const trackBPaperTrading = asRecord(input.trackBPaperTrading);
  const trackBPaperStatus = summarizeTrackBPaperReadiness(trackBPaperTrading);
  const portfolio = asRecord(input.portfolio);
  const productionLink = asRecord(input.productionLink);
  const productionHealth = asRecord(input.productionHealth);
  const productionReconciliation = asRecord(input.productionReconciliation);
  const productionDiagnostics = asRecord(input.productionDiagnostics);
  const productionCapabilities = asRecord(input.productionCapabilities);
  const productionPilotScope = asRecord(input.productionPilotScope);
  const productionLastManualOrderPreview = asRecord(input.productionLastManualOrderPreview);
  const productionBalances = asRecord(input.productionBalances);
  const localOperatorAuth = asRecord(input.localOperatorAuth);
  const operatorActiveAlertRows = asArray<JsonRecord>(input.operatorActiveAlertRows);
  const operatorRecentAlertRows = asArray<JsonRecord>(input.operatorRecentAlertRows);
  const sameUnderlyingConflictSummary = asRecord(input.sameUnderlyingConflictSummary);
  const productionOperatorStatus = asRecord(productionLink.operator_status);
  const productionFuturesPilotStatus = asRecord(productionLink.futures_pilot_status);
  const productionOperatorLocalAuth = asRecord(
    productionOperatorStatus.local_operator_auth ?? productionFuturesPilotStatus.local_operator_auth ?? productionLink.local_operator_auth,
  );

  const pilotSymbol = normalizeScopeSymbol(
    asRecord(productionOperatorStatus.locked_pilot_policy).symbol
    ?? asRecord(productionOperatorStatus.allowed_scope).symbol
    ?? productionPilotScope.symbol
    ?? asRecord(productionCapabilities.manual_live_pilot_scope).symbol
    ?? asRecord(productionLastManualOrderPreview.request).symbol
    ?? "MGC",
  );
  const checkedAt = latestTimestamp([input.desktopRefreshedAt, input.dashboardGeneratedAt]);
  const operatorSurfaceAsOf = timestampOrNull(operatorSurface.generated_at) ?? checkedAt;
  const operatingMode = String(global.mode ?? global.mode_label ?? "").trim().toUpperCase();
  const paperMode = operatingMode === "PAPER" || global.live_disabled === true;
  const laneStatusSummary = asRecord(paperReadiness.lane_status_summary);
  const paperReadinessSource = firstNonEmptyString(runtimeValues.paper_readiness_source, paperReadiness.paper_readiness_source);
  const paperReadinessTimestamp = timestampOrNull(
    runtimeValues.paper_readiness_timestamp ?? paperReadiness.paper_readiness_timestamp ?? paperReadiness.generated_at,
  );
  const paperRuntimePhase = String(
    paperReadiness.runtime_phase
    ?? paperReadiness.phase
    ?? paperReadiness.status
    ?? runtimeValues.runtime_recovery_state
    ?? runtimeReadiness.runtime_status
    ?? "",
  ).trim().toUpperCase();
  const paperRuntimeReadyRaw = runtimeValues.paper_runtime_ready ?? paperReadiness.paper_runtime_ready;
  const paperRuntimeReadyKnown = typeof paperRuntimeReadyRaw === "boolean";
  const sessionEligibleCount = numericOrNull(
    runtimeValues.session_eligible_count
    ?? runtimeValues.session_eligible_lanes_count
    ?? paperReadiness.session_eligible_count
    ?? laneStatusSummary.session_eligible_lanes_count,
  ) ?? 0;
  const waitingForBarCount = numericOrNull(
    runtimeValues.waiting_for_bar_count
    ?? runtimeValues.waiting_for_completed_bar_count
    ?? paperReadiness.waiting_for_bar_count
    ?? laneStatusSummary.waiting_for_completed_bar_count,
  ) ?? 0;
  const marketDataStaleCount = numericOrNull(
    runtimeValues.market_data_stale_count
    ?? paperReadiness.market_data_stale_count
    ?? laneStatusSummary.market_data_stale_count,
  ) ?? 0;
  const noSetupCount = numericOrNull(
    runtimeValues.no_setup_count
    ?? paperReadiness.no_setup_count
    ?? laneStatusSummary.no_setup_count,
  ) ?? 0;
  const actionableNowCount = numericOrNull(
    runtimeValues.actionable_now_count
    ?? paperReadiness.actionable_now_count
    ?? laneStatusSummary.actionable_now_count,
  ) ?? 0;
  const trueBlockedCount = numericOrNull(
    runtimeValues.true_blocked_count
    ?? runtimeValues.blocked_lanes_count
    ?? paperReadiness.true_blocked_count
    ?? laneStatusSummary.blocked_lanes_count,
  ) ?? 0;
  const liveCapableCount = numericOrNull(
    runtimeValues.live_capable_count
    ?? paperReadiness.live_capable_count
    ?? laneStatusSummary.live_capable_count,
  ) ?? Math.max(0, sessionEligibleCount - trueBlockedCount);
  const advisoryFaultCount = numericOrNull(
    runtimeValues.advisory_fault_count
    ?? runtimeValues.advisory_faults_count
    ?? paperReadiness.advisory_fault_count,
  ) ?? 0;
  const blockingFaultCount = numericOrNull(
    runtimeValues.blocking_fault_count
    ?? runtimeValues.blocking_faults_count
    ?? paperReadiness.blocking_fault_count,
  ) ?? 0;
  const inferredPaperRuntimeReady =
    !paperRuntimeReadyKnown
    && blockingFaultCount === 0
    && runtimeReadiness.blocking_faults_active !== true
    && (paperReadiness.runtime_running === true || paperRuntimePhase === "RUNNING")
    && paperReadiness.entries_enabled !== false
    && runtimeReadiness.entries_enabled !== false;
  const paperRuntimeReady = paperRuntimeReadyKnown ? paperRuntimeReadyRaw === true : inferredPaperRuntimeReady;
  const authoritativePaperTradeBlockReason = firstNonEmptyString(
    runtimeValues.paper_trade_block_reason,
    paperReadiness.paper_trade_block_reason,
  );
  const legacyMarketDataBlockReason =
    authoritativePaperTradeBlockReason === "paper_market_data_stale_or_unavailable";
  const authoritativePaperTradeAllowedRaw = runtimeValues.paper_trade_allowed ?? paperReadiness.paper_trade_allowed;
  const authoritativePaperTradeAllowedKnown = typeof authoritativePaperTradeAllowedRaw === "boolean";
  const authoritativePaperTradeAllowed = paperMode && trackBPaperStatus.ready && legacyMarketDataBlockReason
    ? true
    : authoritativePaperTradeAllowedKnown
      ? authoritativePaperTradeAllowedRaw === true
      : false;

  const currentExposure = buildExposureSummary(input, pilotSymbol, productionReconciliation);
  const todayPnL = buildTodayPnLSummary(portfolio, operatorSurfaceAsOf);
  const positionPosture: PositionPostureState =
    currentExposure.quantity !== null && Math.abs(currentExposure.quantity) > 0 ? "In Position" : "Flat";

  const legacyMarketDataPass =
    String(global.market_data_status ?? global.market_data_label ?? runtimeReadiness.market_data_readiness ?? "").trim().toUpperCase() === "LIVE"
    && global.stale !== true;
  const marketDataPass = paperMode && trackBPaperStatus.ready ? true : legacyMarketDataPass;
  const brokerReachable = asRecord(productionHealth.broker_reachable).ok === true;
  const brokerAuthHealthy = asRecord(productionHealth.auth_healthy).ok === true;
  const brokerAccountSelected = asRecord(productionHealth.account_selected).ok === true;
  const brokerPositionsFresh = asRecord(productionHealth.positions_fresh).ok === true;
  const brokerQuotesFresh = asRecord(productionHealth.quotes_fresh).ok === true;
  const brokerRouteBlockers = [
    ...asArray<string>(productionFuturesPilotStatus.preview_blockers),
    ...asArray<string>(productionFuturesPilotStatus.live_submit_blockers),
  ].filter((value) => !/local operator auth/i.test(String(value)));
  const liveBrokerAuthorityPass =
    input.productionLinkEnabled
    && brokerReachable
    && brokerAuthHealthy
    && brokerAccountSelected
    && brokerPositionsFresh
    && brokerQuotesFresh
    && brokerRouteBlockers.length === 0;
  const brokerAuthorityPass = paperMode || liveBrokerAuthorityPass;
  const reconciliationStatusToken = normalizedUpperToken(
    productionReconciliation.status
      ?? productionReconciliation.label
      ?? productionReconciliation.detail
      ?? global.reconciliation_status,
  );
  const reconciliationStructuredClear =
    productionReconciliation.blocked !== true
    && Number(productionReconciliation.mismatch_count ?? 0) === 0;
  const reconciliationHasAuthoritativeSnapshot =
    productionReconciliation.blocked !== undefined
    || productionReconciliation.mismatch_count !== undefined
    || Boolean(reconciliationStatusToken);
  const reconciliationAuthorityAvailable =
    input.productionLinkEnabled || reconciliationHasAuthoritativeSnapshot;
  const reconciliationPass =
    reconciliationAuthorityAvailable
    && reconciliationStructuredClear
    && (
      reconciliationHasAuthoritativeSnapshot
        ? !["BLOCKED", "DIRTY", "FAIL", "FAILED", "MISMATCH", "RECONCILING"].includes(reconciliationStatusToken)
        : ["CLEAN", "CLEAR"].includes(normalizedUpperToken(global.reconciliation_status))
    );
  const reconciliationFailReason = firstNonEmptyString(
    !reconciliationAuthorityAvailable ? "Production link is disabled." : null,
    productionReconciliation.blocked === true
      ? String(productionReconciliation.detail ?? productionReconciliation.status ?? "Reconciliation is blocked.")
      : null,
    Number(productionReconciliation.mismatch_count ?? 0) > 0
      ? `Broker reconciliation reported ${Number(productionReconciliation.mismatch_count ?? 0)} mismatch${Number(productionReconciliation.mismatch_count ?? 0) === 1 ? "" : "es"}.`
      : null,
    ["BLOCKED", "DIRTY", "FAIL", "FAILED", "MISMATCH", "RECONCILING"].includes(reconciliationStatusToken)
      ? String(productionReconciliation.detail ?? productionReconciliation.status ?? global.reconciliation_status ?? "Reconciliation is not clear.")
      : null,
    textOrFallback(global.reconciliation_status, "Reconciliation is not clear."),
  );
  const runtimePosture: RuntimePostureState = paperMode
    ? (
        blockingFaultCount > 0
          ? "Faulted"
          : String(runtimeValues.runtime_recovery_state ?? "").trim().toUpperCase() === "RECONCILING"
            ? "Reconciling"
            : paperRuntimeReady
              ? "Ready"
              : "Degraded"
      )
    : (
        runtimeReadiness.blocking_faults_active === true
        || String(global.runtime_health_label ?? global.runtime_health ?? "").trim().toUpperCase() === "FAULTED"
          ? "Faulted"
          : productionReconciliation.blocked === true || String(runtimeValues.runtime_recovery_state ?? "").trim().toUpperCase() === "RECONCILING"
            ? "Reconciling"
            : runtimeReadiness.paper_enabled === true && String(runtimeReadiness.runtime_status ?? "").trim().toUpperCase() === "RUNNING" && runtimeReadiness.entries_enabled === true
              ? "Ready"
              : "Degraded"
      );
  const runtimePass = runtimePosture === "Ready";
  const operatorSessionActive = productionOperatorLocalAuth.auth_session_active === true || localOperatorAuth.auth_session_active === true;
  const operatorSensitiveActionsAllowed =
    productionOperatorLocalAuth.entry_allowed !== false
    && productionOperatorLocalAuth.flatten_allowed !== false
    && productionOperatorLocalAuth.replace_allowed !== false;
  const liveOperatorAuthorityPass =
    productionOperatorLocalAuth.available !== false
    && (productionOperatorLocalAuth.ready === true || operatorSessionActive)
    && operatorSensitiveActionsAllowed;
  const operatorAuthorityPass = paperMode || liveOperatorAuthorityPass;

  const hardGates: OperatorTriageHardGate[] = [
    {
      key: "market-data",
      label: paperMode && trackBPaperStatus.available ? "Track B Live Market Data" : "Market Data",
      status: marketDataPass ? "pass" : "fail",
      reason: paperMode && trackBPaperStatus.available
        ? textOrFallback(
            trackBPaperStatus.message,
            legacyMarketDataPass
              ? "Track B PAPER Live execution readiness is available."
              : "Legacy paper market data is unavailable; Track B Live path status is reported separately.",
          )
        : marketDataPass
          ? "Live market data is available."
          : textOrFallback(global.market_data_label ?? global.market_data_status, "Live market data is unavailable."),
      checked_at: checkedAt,
      source_timestamp: timestampOrNull(global.last_update_timestamp) ?? timestampOrNull(input.desktopRefreshedAt),
      freshness_seconds: freshnessSeconds(
        timestampOrNull(global.last_update_timestamp) ?? timestampOrNull(input.desktopRefreshedAt),
        checkedAt,
      ),
      truth_source: buildTruthSource({
        kind: "stitched_payload_derived",
        authority: "stitched",
        sourceFamily: "dashboard",
        authorityLevel: "derived",
        label: "Dashboard Market Data Status",
        sourcePaths: [
          "global.market_data_status",
          "global.market_data_label",
          "operator_surface.runtime_readiness.market_data_readiness",
          "track_b_paper_trading.phase1_gc_readiness",
          "track_b_paper_trading.startup_readiness_diagnostic",
          "track_b_paper_trading.zero_activity_diagnostic",
          "global.last_update_timestamp",
        ],
        detail: paperMode && trackBPaperStatus.available
          ? "Paper-mode market-data posture uses Track B Databento Live execution readiness when Track B PAPER diagnostics are present; legacy paper market-data freshness is labeled separately."
          : "Hard-gate status is derived from stitched dashboard freshness and market-data readiness fields.",
      }),
    },
    {
      key: "broker-authority",
      label: "Broker Authority",
      status: brokerAuthorityPass ? "pass" : "fail",
      reason: brokerAuthorityPass
        ? paperMode
          ? "Broker authority does not hard-block supervised paper mode."
          : "Broker truth is live, selected, and fresh."
        : firstNonEmptyString(
            !input.productionLinkEnabled ? "Production link is disabled." : null,
            brokerRouteBlockers[0],
            brokerReachable ? null : asRecord(productionHealth.broker_reachable).detail,
            brokerAuthHealthy ? null : asRecord(productionHealth.auth_healthy).detail,
            brokerAccountSelected ? null : asRecord(productionHealth.account_selected).detail,
            brokerPositionsFresh ? null : asRecord(productionHealth.positions_fresh).detail,
            brokerQuotesFresh ? null : asRecord(productionHealth.quotes_fresh).detail,
            productionLink.detail,
          ) ?? "Broker authority is unavailable.",
      checked_at: checkedAt,
      source_timestamp: latestTimestamp([
        productionDiagnostics.last_positions_refresh_at,
        productionDiagnostics.last_quotes_refresh_at,
        productionBalances.fetched_at,
        ...asArray<JsonRecord>(input.productionPositions).flatMap((row) => [row.fetched_at, row.quote_fetched_at]),
      ]),
      freshness_seconds: freshnessSeconds(
        latestTimestamp([
          productionDiagnostics.last_positions_refresh_at,
          productionDiagnostics.last_quotes_refresh_at,
          productionBalances.fetched_at,
          ...asArray<JsonRecord>(input.productionPositions).flatMap((row) => [row.fetched_at, row.quote_fetched_at]),
        ]),
        checkedAt,
      ),
      truth_source: buildTruthSource({
        kind: "stitched_payload_derived",
        authority: "stitched",
        sourceFamily: "broker",
        authorityLevel: "derived",
        label: "Broker Health Stitch",
        sourcePaths: [
          "production_link.health.broker_reachable",
          "production_link.health.auth_healthy",
          "production_link.health.account_selected",
          "production_link.health.positions_fresh",
          "production_link.health.quotes_fresh",
          "production_link.futures_pilot_status.preview_blockers",
          "production_link.futures_pilot_status.live_submit_blockers",
          "production_link.diagnostics.last_positions_refresh_at",
          "production_link.diagnostics.last_quotes_refresh_at",
        ],
        detail: "Broker authority is stitched from health checks, route blockers, and broker refresh timestamps.",
      }),
    },
    {
      key: "reconciliation",
      label: "Reconciliation",
      status: reconciliationPass ? "pass" : "fail",
      reason: reconciliationPass
        ? "Broker reconciliation is clear."
        : (reconciliationFailReason || "Reconciliation is not clear."),
      checked_at: checkedAt,
      source_timestamp: timestampOrNull(productionReconciliation.created_at) ?? timestampOrNull(input.desktopRefreshedAt),
      freshness_seconds: freshnessSeconds(
        timestampOrNull(productionReconciliation.created_at) ?? timestampOrNull(input.desktopRefreshedAt),
        checkedAt,
      ),
      truth_source: buildTruthSource({
        kind: "stitched_payload_derived",
        authority: "stitched",
        sourceFamily: "mixed",
        authorityLevel: "derived",
        label: "Reconciliation Stitch",
        sourcePaths: [
          "production_link.reconciliation.blocked",
          "production_link.reconciliation.mismatch_count",
          "production_link.reconciliation.detail",
          "production_link.reconciliation.created_at",
          "global.reconciliation_status",
        ],
        detail: "Reconciliation is derived from the broker reconciliation payload plus dashboard reconciliation status.",
      }),
    },
    {
      key: "runtime-readiness",
      label: "Runtime Readiness",
      status: runtimePass ? "pass" : "fail",
      reason: runtimePass
        ? "Runtime is ready and entries are enabled."
        : textOrFallback(runtimeReadiness.status_line ?? runtimeValues.runtime_recovery_message, "Runtime is not ready."),
      checked_at: checkedAt,
      source_timestamp: timestampOrNull(operatorSurface.generated_at ?? paperReadiness.generated_at) ?? timestampOrNull(input.desktopRefreshedAt),
      freshness_seconds: freshnessSeconds(
        timestampOrNull(operatorSurface.generated_at ?? paperReadiness.generated_at) ?? timestampOrNull(input.desktopRefreshedAt),
        checkedAt,
      ),
      truth_source: buildTruthSource({
        kind: "stitched_payload_derived",
        authority: "stitched",
        sourceFamily: "operator_surface",
        authorityLevel: "derived",
        label: "Runtime Readiness Stitch",
        sourcePaths: [
          "operator_surface.runtime_readiness.runtime_status",
          "operator_surface.runtime_readiness.paper_enabled",
          "operator_surface.runtime_readiness.entries_enabled",
          "operator_surface.runtime_readiness.values.runtime_recovery_state",
          "operator_surface.generated_at",
          "paper.readiness.generated_at",
        ],
        detail: "Runtime posture is stitched from operator-surface runtime readiness and recovery state.",
      }),
    },
    {
      key: "operator-authority",
      label: "Operator Authority",
      status: operatorAuthorityPass ? "pass" : "fail",
      reason: operatorAuthorityPass
        ? paperMode
          ? "Local operator authority is only required for live-sensitive actions."
          : "Local operator authority is active for sensitive actions."
        : firstNonEmptyString(
            productionOperatorLocalAuth.blocker,
            productionOperatorLocalAuth.entry_blocked_reason,
            productionOperatorLocalAuth.flatten_blocked_reason,
            productionOperatorLocalAuth.next_action_detail,
            productionOperatorLocalAuth.detail,
            localOperatorAuth.last_auth_detail,
          ) ?? "Sensitive actions are not currently authorized.",
      checked_at: checkedAt,
      source_timestamp: timestampOrNull(
        productionOperatorLocalAuth.auth_session_expires_at ?? productionOperatorLocalAuth.authenticated_at ?? localOperatorAuth.last_authenticated_at,
      ) ?? timestampOrNull(input.desktopRefreshedAt),
      freshness_seconds: freshnessSeconds(
        timestampOrNull(
          productionOperatorLocalAuth.auth_session_expires_at ?? productionOperatorLocalAuth.authenticated_at ?? localOperatorAuth.last_authenticated_at,
        ) ?? timestampOrNull(input.desktopRefreshedAt),
        checkedAt,
      ),
      truth_source: buildTruthSource({
        kind: "stitched_payload_derived",
        authority: "stitched",
        sourceFamily: "mixed",
        authorityLevel: "derived",
        label: "Operator Auth Stitch",
        sourcePaths: [
          "production_link.operator_status.local_operator_auth",
          "production_link.futures_pilot_status.local_operator_auth",
          "production_link.local_operator_auth",
          "desktop_state.localAuth",
        ],
        detail: "Operator authority is stitched from production local-auth state plus desktop local-auth session state.",
      }),
    },
  ];

  const liveRuntimeReady = runtimePass;
  const paperTradeAuthority: TradeAuthorityState =
    authoritativePaperTradeAllowedKnown
      ? (authoritativePaperTradeAllowed ? "Enabled" : "Blocked")
      : (hardGates.every((row) => row.status === "pass") ? "Enabled" : "Blocked");
  const liveTradeAuthority: TradeAuthorityState = (
    legacyMarketDataPass
    && liveBrokerAuthorityPass
    && reconciliationPass
    && liveRuntimeReady
    && liveOperatorAuthorityPass
  ) ? "Enabled" : "Blocked";
  const paperTradeAllowed = paperTradeAuthority === "Enabled";
  const liveTradeAllowed = liveTradeAuthority === "Enabled";
  const activeTradeAuthority = paperMode ? paperTradeAuthority : liveTradeAuthority;
  const firstFailingGate = hardGates.find((row) => row.status === "fail") ?? null;
  const liveTradeBlockReason = liveTradeAllowed
    ? null
    : (firstFailingGate?.reason ?? "Live trade authority is unavailable.");
  const connectionPosture = connectionPostureLabel(input.desktopSourceMode ?? undefined);
  const outagePosture: OutagePostureState =
    activeTradeAuthority === "Blocked"
      ? positionPosture === "In Position"
        ? "Critical"
        : "Review"
      : operatorActiveAlertRows.length > 0 || Number(sameUnderlyingConflictSummary.blocking_unacknowledged_count ?? 0) > 0
        ? "Review"
        : "None";
  const dominantBlocker: OperatorTriageDominantBlocker =
    activeTradeAuthority === "Blocked"
      ? positionPosture === "In Position"
        ? { code: "critical_outage_with_open_exposure", label: "Critical outage with open exposure" }
        : paperMode
          ? { code: "paper_trade_authority_blocked", label: "Paper trade authority blocked" }
          : { code: "live_trade_authority_blocked", label: "Live trade authority blocked" }
      : outagePosture === "Review"
        ? { code: "review_active_warnings", label: "Review active warnings" }
        : { code: "no_active_blocker", label: "No active blocker" };
  const rootCause: OperatorTriageRootCause = paperMode && trackBPaperStatus.available
    ? {
        layer: "runtime",
        code: textOrFallback(trackBPaperStatus.code, "track_b_paper_status"),
        detail: textOrFallback(trackBPaperStatus.message, "Track B PAPER status is available."),
      }
    : paperMode && !paperTradeAllowed
    ? {
        layer: "runtime",
        code: textOrFallback(authoritativePaperTradeBlockReason, "paper_trade_blocked"),
        detail: textOrFallback(authoritativePaperTradeBlockReason, "Paper trade authority is unavailable."),
      }
    : firstFailingGate
      ? {
          layer: hardGateLayerForKey(firstFailingGate.key),
          code: hardGateCodeForKey(firstFailingGate.key),
          detail: `${firstFailingGate.label}: ${firstFailingGate.reason}`,
        }
      : operatorActiveAlertRows[0]
        ? {
            layer: "alerts",
            code: "active_alert_review",
            detail: formatValue(operatorActiveAlertRows[0].message ?? operatorActiveAlertRows[0].title ?? "Review active alerts."),
          }
        : {
            layer: "unknown",
            code: "no_hard_gate_failure",
            detail: "No hard-gate failure.",
          };
  const verdictSentence =
    paperMode && trackBPaperStatus.message
      ? trackBPaperStatus.message
      :
    activeTradeAuthority === "Enabled"
      ? positionPosture === "In Position"
        ? paperMode
          ? "Paper stack healthy. In position. Paper trade authority enabled."
          : "System healthy. In position. Live trade authority enabled."
        : paperMode
          ? "Paper stack healthy. Flat. Paper trade authority enabled."
          : "System healthy. Flat. Live trade authority enabled."
      : positionPosture === "In Position"
        ? paperMode
          ? "Critical outage. Position open, but paper trade authority is blocked. Use paper fallback procedure now."
          : "Critical outage. Position open, but live trade authority is blocked. Use fallback procedure now."
        : paperMode
          ? "Flat but blocked. Paper trade authority is not currently available."
          : "Flat but blocked. Live trade authority is not currently available.";

  const fallbackRunbookPath = typeof asRecord(productionFuturesPilotStatus.outside_sandbox_live_validation).runbook_path === "string"
    ? (asRecord(productionFuturesPilotStatus.outside_sandbox_live_validation).runbook_path as string)
    : null;

  return {
    operator_triage: {
      pilot_symbol: pilotSymbol,
      paper_trade_authority: paperTradeAuthority,
      paper_trade_allowed: paperTradeAllowed,
      paper_trade_block_reason: paperTradeAllowed
        ? null
        : legacyMarketDataBlockReason
          ? "legacy_paper_market_data_stale_or_unavailable"
          : authoritativePaperTradeBlockReason ?? "Paper trade authority is unavailable.",
      live_trade_authority: liveTradeAuthority,
      live_trade_allowed: liveTradeAllowed,
      live_trade_block_reason: liveTradeBlockReason,
      paper_runtime_ready: paperRuntimeReady,
      live_runtime_ready: liveRuntimeReady,
      paper_bridge_allowed: paperTradeAllowed,
      live_bridge_allowed: liveTradeAllowed,
      paper_readiness_source: paperReadinessSource,
      paper_readiness_timestamp: paperReadinessTimestamp,
      track_b_paper_status_code: trackBPaperStatus.code,
      track_b_paper_status_message: trackBPaperStatus.message,
      track_b_legacy_market_data_note: trackBPaperStatus.ready && !legacyMarketDataPass ? trackBPaperStatus.legacyMarketDataNote : null,
      session_eligible_count: sessionEligibleCount,
      live_capable_count: liveCapableCount,
      waiting_for_bar_count: waitingForBarCount,
      market_data_stale_count: marketDataStaleCount,
      no_setup_count: noSetupCount,
      actionable_now_count: actionableNowCount,
      true_blocked_count: trueBlockedCount,
      advisory_fault_count: advisoryFaultCount,
      blocking_fault_count: blockingFaultCount,
      position_posture: positionPosture,
      outage_posture: outagePosture,
      connection_posture: connectionPosture,
      runtime_posture: runtimePosture,
      verdict_sentence: verdictSentence,
      dominant_blocker: dominantBlocker,
      root_cause: rootCause,
      hard_gates: hardGates,
      current_exposure: currentExposure,
      today_pnl: todayPnL,
      fallback: {
        truth_source: buildTruthSource({
          kind: "stitched_payload_derived",
          authority: fallbackRunbookPath ? "mixed" : "placeholder",
          sourceFamily: fallbackRunbookPath ? "mixed" : "placeholder",
          authorityLevel: fallbackRunbookPath ? "fallback" : "placeholder",
          label: fallbackRunbookPath ? "Mixed Fallback Data" : "Fallback Placeholder Data",
          isFallback: true,
          sourcePaths: [
            "production_link.futures_pilot_status.outside_sandbox_live_validation.runbook_path",
          ],
          detail: "Fallback procedure data is only partially real today: runbook path may be published, broker desk phone is still a placeholder.",
        }),
        runbook_path: fallbackRunbookPath,
        broker_desk_phone: null,
        runbook_truth_source: buildTruthSource({
          kind: fallbackRunbookPath ? "stitched_payload_derived" : "unavailable",
          authority: fallbackRunbookPath ? "stitched" : "unavailable",
          sourceFamily: fallbackRunbookPath ? "mixed" : "unknown",
          authorityLevel: fallbackRunbookPath ? "fallback" : "unavailable",
          label: fallbackRunbookPath ? "Published Runbook Path" : "Runbook Path Unavailable",
          isFallback: true,
          sourcePaths: ["production_link.futures_pilot_status.outside_sandbox_live_validation.runbook_path"],
          detail: fallbackRunbookPath
            ? "Runbook path is present in the current stitched production payload."
            : "No runbook path is currently published in the payload.",
        }),
        broker_desk_phone_truth_source: buildTruthSource({
          kind: "placeholder",
          authority: "placeholder",
          sourceFamily: "placeholder",
          authorityLevel: "placeholder",
          label: "Broker Desk Phone Placeholder",
          isFallback: true,
          sourcePaths: [],
          detail: "Broker desk phone is not supplied by the current payload and remains a placeholder/config hook.",
        }),
      },
      operator_authority: {
        truth_source: buildTruthSource({
          kind: "stitched_payload_derived",
          authority: "stitched",
          sourceFamily: "mixed",
          authorityLevel: "derived",
          label: "Operator Authority Stitch",
          sourcePaths: [
            "production_link.operator_status.local_operator_auth",
            "production_link.futures_pilot_status.local_operator_auth",
            "production_link.local_operator_auth",
            "desktop_state.localAuth",
          ],
          detail: "Operator authority combines production local-auth posture with desktop local-auth session state.",
        }),
        auth_available: productionOperatorLocalAuth.available !== false,
        session_active: operatorSessionActive,
        session_expires_at: typeof (productionOperatorLocalAuth.auth_session_expires_at ?? null) === "string"
          ? (productionOperatorLocalAuth.auth_session_expires_at as string)
          : null,
        sensitive_actions_allowed: operatorSensitiveActionsAllowed,
        blocker: firstNonEmptyString(
          productionOperatorLocalAuth.blocker,
          productionOperatorLocalAuth.entry_blocked_reason,
          productionOperatorLocalAuth.flatten_blocked_reason,
          productionOperatorLocalAuth.next_action_detail,
          productionOperatorLocalAuth.detail,
          localOperatorAuth.last_auth_detail,
        ),
      },
    },
  };
}
