export type JsonRecord = Record<string, unknown>;

export type OperatorNotificationSeverity = "info" | "warning" | "critical" | "trade";

export type OperatorNotificationDeliveryStatus =
  | "pending"
  | "delivered"
  | "failed"
  | "suppressed_policy"
  | "suppressed_quiet_hours"
  | "suppressed_throttle"
  | "suppressed_duplicate";

export const TRACK_B_NOTIFICATION_EVENT_TYPES = [
  "market_data_disconnected",
  "market_data_reconnected",
  "broker_disconnected",
  "broker_reconnected",
  "runtime_down",
  "runtime_recovered",
  "readiness_changed",
  "paper_order_submitted",
  "paper_fill_received",
  "trade_opened",
  "trade_closed",
  "lifecycle_reconciliation_blocked",
  "duplicate_writer_detected",
  "stale_authority_truth_artifact",
  "guardian_control_safe_state_hard_block",
  "backend_ui_degraded_runtime_active",
  "test_notification",
] as const;

export type OperatorNotificationEventType = typeof TRACK_B_NOTIFICATION_EVENT_TYPES[number];

export interface OperatorNotificationEvent {
  event_id: string;
  event_type: OperatorNotificationEventType;
  severity: OperatorNotificationSeverity;
  title: string;
  body: string;
  timestamp: string;
  source_component: string;
  dedupe_key: string;
  throttle_seconds: number;
  metadata: JsonRecord;
  delivery_status: OperatorNotificationDeliveryStatus;
}

export interface OperatorNotificationPolicy {
  enabled: boolean;
  enabled_event_types: Record<string, boolean>;
  severity_threshold: OperatorNotificationSeverity;
  quiet_hours: {
    enabled: boolean;
    start: string;
    end: string;
  };
  trade_alerts_always_on: boolean;
  default_throttle_seconds: number;
  dedupe_window_seconds: number;
}

export interface OperatorNotificationSnapshot {
  generated_at: string | null;
  market_data_connected: boolean | null;
  broker_connected: boolean | null;
  runtime_active: boolean | null;
  readiness_key: string;
  readiness_label: string;
  lifecycle_blocked: boolean;
  lifecycle_block_reason: string | null;
  duplicate_writer_detected: boolean;
  stale_authority_truth_artifact: boolean;
  hard_blocked: boolean;
  hard_block_reason: string | null;
  backend_ui_degraded_runtime_active: boolean;
  backend_ui_degraded_reason: string | null;
  paper_orders: JsonRecord[];
  paper_fills: JsonRecord[];
}

export interface OperatorNotificationPolicyDecision {
  allowed: boolean;
  status: OperatorNotificationDeliveryStatus;
  reason: string;
}

export const DEFAULT_NOTIFICATION_POLICY: OperatorNotificationPolicy = {
  enabled: true,
  enabled_event_types: Object.fromEntries(TRACK_B_NOTIFICATION_EVENT_TYPES.map((type) => [type, true])),
  severity_threshold: "info",
  quiet_hours: {
    enabled: false,
    start: "22:00",
    end: "07:00",
  },
  trade_alerts_always_on: true,
  default_throttle_seconds: 60,
  dedupe_window_seconds: 300,
};

const SEVERITY_RANK: Record<OperatorNotificationSeverity, number> = {
  info: 0,
  warning: 1,
  critical: 2,
  trade: 3,
};

const TRADE_EVENT_TYPES = new Set<OperatorNotificationEventType>([
  "paper_order_submitted",
  "paper_fill_received",
  "trade_opened",
  "trade_closed",
]);

export function normalizeNotificationPolicy(input: unknown): OperatorNotificationPolicy {
  const raw = isRecord(input) ? input : {};
  const quietHours = isRecord(raw.quiet_hours) ? raw.quiet_hours : {};
  const enabledEventTypes = {
    ...DEFAULT_NOTIFICATION_POLICY.enabled_event_types,
    ...(isRecord(raw.enabled_event_types) ? raw.enabled_event_types : {}),
  };
  return {
    enabled: typeof raw.enabled === "boolean" ? raw.enabled : DEFAULT_NOTIFICATION_POLICY.enabled,
    enabled_event_types: Object.fromEntries(
      TRACK_B_NOTIFICATION_EVENT_TYPES.map((type) => [type, enabledEventTypes[type] !== false]),
    ),
    severity_threshold: isSeverity(raw.severity_threshold)
      ? raw.severity_threshold
      : DEFAULT_NOTIFICATION_POLICY.severity_threshold,
    quiet_hours: {
      enabled: typeof quietHours.enabled === "boolean" ? quietHours.enabled : DEFAULT_NOTIFICATION_POLICY.quiet_hours.enabled,
      start: normalizeClockValue(quietHours.start, DEFAULT_NOTIFICATION_POLICY.quiet_hours.start),
      end: normalizeClockValue(quietHours.end, DEFAULT_NOTIFICATION_POLICY.quiet_hours.end),
    },
    trade_alerts_always_on: typeof raw.trade_alerts_always_on === "boolean"
      ? raw.trade_alerts_always_on
      : DEFAULT_NOTIFICATION_POLICY.trade_alerts_always_on,
    default_throttle_seconds: positiveInteger(raw.default_throttle_seconds, DEFAULT_NOTIFICATION_POLICY.default_throttle_seconds),
    dedupe_window_seconds: positiveInteger(raw.dedupe_window_seconds, DEFAULT_NOTIFICATION_POLICY.dedupe_window_seconds),
  };
}

export function notificationPolicyDecision(
  event: OperatorNotificationEvent,
  policyInput: unknown,
  recentEvents: readonly OperatorNotificationEvent[],
  now = new Date(),
): OperatorNotificationPolicyDecision {
  const policy = normalizeNotificationPolicy(policyInput);
  const tradeAlwaysOn = policy.trade_alerts_always_on && TRADE_EVENT_TYPES.has(event.event_type);
  if (!policy.enabled && !tradeAlwaysOn) {
    return { allowed: false, status: "suppressed_policy", reason: "Notifications are globally disabled." };
  }
  if (policy.enabled_event_types[event.event_type] === false && !tradeAlwaysOn) {
    return { allowed: false, status: "suppressed_policy", reason: `${event.event_type} notifications are disabled.` };
  }
  if (SEVERITY_RANK[event.severity] < SEVERITY_RANK[policy.severity_threshold] && !tradeAlwaysOn) {
    return { allowed: false, status: "suppressed_policy", reason: `Severity ${event.severity} is below threshold ${policy.severity_threshold}.` };
  }
  if (policy.quiet_hours.enabled && !tradeAlwaysOn && quietHoursActive(policy.quiet_hours.start, policy.quiet_hours.end, now)) {
    return { allowed: false, status: "suppressed_quiet_hours", reason: "Quiet hours are active." };
  }
  const throttleSeconds = Math.max(0, event.throttle_seconds || policy.default_throttle_seconds);
  const duplicateWindowSeconds = Math.max(0, policy.dedupe_window_seconds);
  for (const recent of recentEvents) {
    if (recent.dedupe_key !== event.dedupe_key) {
      continue;
    }
    const ageSeconds = (now.getTime() - Date.parse(recent.timestamp)) / 1000;
    if (!Number.isFinite(ageSeconds) || ageSeconds < 0) {
      continue;
    }
    if (duplicateWindowSeconds > 0 && ageSeconds <= duplicateWindowSeconds && recent.delivery_status === "delivered") {
      return { allowed: false, status: "suppressed_duplicate", reason: "Duplicate notification is inside the dedupe window." };
    }
    if (throttleSeconds > 0 && ageSeconds <= throttleSeconds) {
      return { allowed: false, status: "suppressed_throttle", reason: "Notification is inside the throttle window." };
    }
  }
  return { allowed: true, status: "pending", reason: "Allowed by notification policy." };
}

export function buildNotificationSnapshot(input: {
  dashboard: JsonRecord | null;
  trackBStatus: JsonRecord | null;
  backendState?: string | null;
  backendLabel?: string | null;
  sourceMode?: string | null;
  sourceLabel?: string | null;
}): OperatorNotificationSnapshot {
  const dashboard = input.dashboard ?? {};
  const global = asRecord(dashboard.global);
  const paper = asRecord(dashboard.paper);
  const readiness = asRecord(paper.readiness);
  const operatorSurface = asRecord(dashboard.operator_surface);
  const runtimeReadiness = asRecord(operatorSurface.runtime_readiness);
  const runtimeValues = { ...asRecord(runtimeReadiness.values), ...asRecord(readiness.values) };
  const startupControlPlane = asRecord(dashboard.startup_control_plane);
  const productionLink = asRecord(dashboard.production_link);
  const productionHealth = asRecord(productionLink.health);
  const brokerReachable = asRecord(productionHealth.broker_reachable);
  const paperOrders = asArray(asRecord(paper).latest_intents).concat(asArray(asRecord(paper).latest_orders));
  const paperFills = asArray(asRecord(paper).latest_fills);
  const trackBStatus = input.trackBStatus ?? {};
  const hardBlock = hardBlockReason(startupControlPlane, runtimeValues, trackBStatus);
  const runtimeActive = runtimeIsActive(runtimeValues, readiness, input.backendState ?? null);
  const backendDegraded =
    runtimeActive === true
    && (String(input.sourceMode ?? "").includes("snapshot")
      || String(input.backendState ?? "").includes("degraded")
      || String(input.backendState ?? "").includes("backend_down"));
  return {
    generated_at: stringOrNull(dashboard.generated_at ?? operatorSurface.generated_at ?? readiness.generated_at),
    market_data_connected: marketDataConnected(global, runtimeValues, startupControlPlane),
    broker_connected: brokerConnected(productionLink, brokerReachable, runtimeValues),
    runtime_active: runtimeActive,
    readiness_key: readinessKey(startupControlPlane, runtimeValues, readiness),
    readiness_label: readinessLabel(startupControlPlane, runtimeValues, readiness),
    lifecycle_blocked: lifecycleBlocked(startupControlPlane, runtimeValues, readiness),
    lifecycle_block_reason: stringOrNull(startupControlPlane.primary_reason ?? runtimeValues.paper_trade_block_reason),
    duplicate_writer_detected: textSearchFlag(trackBStatus, /duplicate.*writer|writer.*duplicate/i),
    stale_authority_truth_artifact: staleAuthorityTruthArtifact(startupControlPlane, runtimeValues, trackBStatus),
    hard_blocked: hardBlock !== null,
    hard_block_reason: hardBlock,
    backend_ui_degraded_runtime_active: backendDegraded,
    backend_ui_degraded_reason: backendDegraded
      ? stringOrNull(input.sourceLabel ?? input.backendLabel ?? "Backend/UI degraded while runtime remains active.")
      : null,
    paper_orders: paperOrders,
    paper_fills: paperFills,
  };
}

export function deriveNotificationEvents(input: {
  previous: OperatorNotificationSnapshot | null;
  current: OperatorNotificationSnapshot;
  timestamp?: string;
}): OperatorNotificationEvent[] {
  const previous = input.previous;
  const current = input.current;
  const timestamp = input.timestamp ?? new Date().toISOString();
  if (!previous) {
    return [];
  }
  const events: OperatorNotificationEvent[] = [];
  if (previous.market_data_connected === true && current.market_data_connected === false) {
    events.push(makeEvent("market_data_disconnected", "critical", "Market data disconnected", "Track B execution market data is not connected.", timestamp, "market_data", "market-data", 120, {}));
  }
  if (previous.market_data_connected === false && current.market_data_connected === true) {
    events.push(makeEvent("market_data_reconnected", "info", "Market data reconnected", "Track B execution market data is connected again.", timestamp, "market_data", "market-data", 120, {}));
  }
  if (previous.broker_connected === true && current.broker_connected === false) {
    events.push(makeEvent("broker_disconnected", "critical", "Broker disconnected", "Broker-backed connectivity is no longer healthy.", timestamp, "broker", "broker", 120, {}));
  }
  if (previous.broker_connected === false && current.broker_connected === true) {
    events.push(makeEvent("broker_reconnected", "info", "Broker reconnected", "Broker-backed connectivity is healthy again.", timestamp, "broker", "broker", 120, {}));
  }
  if (previous.runtime_active === true && current.runtime_active === false) {
    events.push(makeEvent("runtime_down", "critical", "Track B runtime down", "The Track B PAPER runtime is no longer active.", timestamp, "runtime", "runtime", 120, {}));
  }
  if (previous.runtime_active === false && current.runtime_active === true) {
    events.push(makeEvent("runtime_recovered", "info", "Track B runtime recovered", "The Track B PAPER runtime is active again.", timestamp, "runtime", "runtime", 120, {}));
  }
  if (previous.readiness_key !== current.readiness_key) {
    events.push(makeEvent("readiness_changed", "warning", "Track B readiness changed", current.readiness_label, timestamp, "readiness", `readiness:${current.readiness_key}`, 60, {
      previous_readiness_key: previous.readiness_key,
      readiness_key: current.readiness_key,
    }));
  }
  pushBooleanTransition(events, previous, current, "lifecycle_blocked", "lifecycle_reconciliation_blocked", "critical", "Lifecycle/reconciliation blocked", current.lifecycle_block_reason ?? "Track B lifecycle or reconciliation is blocked.", timestamp, "reconciliation");
  pushBooleanTransition(events, previous, current, "duplicate_writer_detected", "duplicate_writer_detected", "critical", "Duplicate writer detected", "Track B detected a duplicate-writer condition.", timestamp, "runtime_authority");
  pushBooleanTransition(events, previous, current, "stale_authority_truth_artifact", "stale_authority_truth_artifact", "warning", "Stale authority/truth artifact", "An authority or broker-truth artifact is stale.", timestamp, "runtime_authority");
  pushBooleanTransition(events, previous, current, "hard_blocked", "guardian_control_safe_state_hard_block", "critical", "Track B hard block", current.hard_block_reason ?? "Guardian, Control Plane, or Safe-State hard block is active.", timestamp, "control_plane");
  pushBooleanTransition(events, previous, current, "backend_ui_degraded_runtime_active", "backend_ui_degraded_runtime_active", "warning", "Backend/UI degraded", current.backend_ui_degraded_reason ?? "Backend/UI is degraded while runtime remains active.", timestamp, "desktop_backend");

  const previousOrders = new Set(previous.paper_orders.map(orderIdentity).filter(Boolean));
  for (const order of current.paper_orders) {
    const identity = orderIdentity(order);
    if (identity && !previousOrders.has(identity) && brokerBackedOrderEvidence(order)) {
      events.push(orderSubmittedEvent(order, timestamp));
    }
  }
  const previousFills = new Set(previous.paper_fills.map(fillIdentity).filter(Boolean));
  for (const fill of current.paper_fills) {
    const identity = fillIdentity(fill);
    if (!identity || previousFills.has(identity) || !brokerBackedTradeEvidence(fill)) {
      continue;
    }
    events.push(fillReceivedEvent(fill, timestamp));
    const intentType = String(fill.intent_type ?? fill.side ?? "").toUpperCase();
    if (intentType.includes("_TO_OPEN")) {
      events.push(tradeOpenedEvent(fill, timestamp));
    } else if (intentType.includes("_TO_CLOSE")) {
      events.push(tradeClosedEvent(fill, timestamp));
    }
  }
  return events;
}

export function brokerBackedTradeEvidence(row: JsonRecord): boolean {
  const status = String(row.order_status ?? row.status ?? "").toUpperCase();
  const hasFillIdentity = Boolean(row.exec_id ?? row.execution_id ?? row.perm_id ?? row.fill_id);
  const hasBrokerOrderIdentity = Boolean(row.broker_order_id ?? row.broker_order_status ?? row.order_ref ?? row.ibkr_order_id);
  return status.includes("FILL") && hasFillIdentity && hasBrokerOrderIdentity;
}

export function brokerBackedOrderEvidence(row: JsonRecord): boolean {
  const submitted = Boolean(row.submitted_at ?? row.acknowledged_at ?? row.broker_order_id);
  const hasBrokerIdentity = Boolean(row.broker_order_id ?? row.perm_id ?? row.order_ref ?? row.ibkr_order_id);
  return submitted && hasBrokerIdentity;
}

export function makeTestNotificationEvent(timestamp = new Date().toISOString()): OperatorNotificationEvent {
  return makeEvent(
    "test_notification",
    "info",
    "Track B notification test",
    "Native macOS notifications are advisory only and do not change broker/runtime state.",
    timestamp,
    "operator_notifications",
    `test:${timestamp}`,
    0,
    { advisory_only: true, mutates_broker_state: false },
  );
}

function pushBooleanTransition(
  events: OperatorNotificationEvent[],
  previous: OperatorNotificationSnapshot,
  current: OperatorNotificationSnapshot,
  field: keyof Pick<OperatorNotificationSnapshot, "lifecycle_blocked" | "duplicate_writer_detected" | "stale_authority_truth_artifact" | "hard_blocked" | "backend_ui_degraded_runtime_active">,
  eventType: OperatorNotificationEventType,
  severity: OperatorNotificationSeverity,
  title: string,
  body: string,
  timestamp: string,
  sourceComponent: string,
): void {
  if (previous[field] === false && current[field] === true) {
    events.push(makeEvent(eventType, severity, title, body, timestamp, sourceComponent, eventType, 180, {}));
  }
}

function makeEvent(
  eventType: OperatorNotificationEventType,
  severity: OperatorNotificationSeverity,
  title: string,
  body: string,
  timestamp: string,
  sourceComponent: string,
  dedupeKey: string,
  throttleSeconds: number,
  metadata: JsonRecord,
): OperatorNotificationEvent {
  return {
    event_id: `${eventType}:${dedupeKey}:${timestamp}`,
    event_type: eventType,
    severity,
    title,
    body,
    timestamp,
    source_component: sourceComponent,
    dedupe_key: `${eventType}:${dedupeKey}`,
    throttle_seconds: throttleSeconds,
    metadata,
    delivery_status: "pending",
  };
}

function orderSubmittedEvent(order: JsonRecord, timestamp: string): OperatorNotificationEvent {
  const symbol = formatSymbol(order);
  const qty = String(order.quantity ?? order.qty ?? "?");
  const side = String(order.intent_type ?? order.side ?? "PAPER order");
  return makeEvent("paper_order_submitted", "trade", "PAPER order submitted", `${side} ${qty} ${symbol}`, timestamp, "broker_backed_paper_orders", orderIdentity(order) ?? `${symbol}:${timestamp}`, 30, {
    broker_order_id: order.broker_order_id ?? null,
    order_intent_id: order.order_intent_id ?? null,
  });
}

function fillReceivedEvent(fill: JsonRecord, timestamp: string): OperatorNotificationEvent {
  const symbol = formatSymbol(fill);
  const qty = String(fill.quantity ?? fill.qty ?? "?");
  const price = String(fill.fill_price ?? fill.price ?? "?");
  const side = String(fill.intent_type ?? fill.side ?? "PAPER fill");
  return makeEvent("paper_fill_received", "trade", "PAPER fill received", `${side} ${qty} ${symbol} @ ${price}`, timestamp, "broker_backed_paper_fills", fillIdentity(fill) ?? `${symbol}:${timestamp}`, 15, {
    broker_order_id: fill.broker_order_id ?? null,
    fill_id: fill.fill_id ?? null,
    exec_id: fill.exec_id ?? fill.execution_id ?? null,
    perm_id: fill.perm_id ?? null,
  });
}

function tradeOpenedEvent(fill: JsonRecord, timestamp: string): OperatorNotificationEvent {
  return tradeEvent("trade_opened", "Trade opened", fill, timestamp);
}

function tradeClosedEvent(fill: JsonRecord, timestamp: string): OperatorNotificationEvent {
  return tradeEvent("trade_closed", "Trade closed", fill, timestamp);
}

function tradeEvent(eventType: OperatorNotificationEventType, title: string, fill: JsonRecord, timestamp: string): OperatorNotificationEvent {
  const symbol = formatSymbol(fill);
  const qty = String(fill.quantity ?? fill.qty ?? "?");
  const price = String(fill.fill_price ?? fill.price ?? "?");
  const side = String(fill.intent_type ?? fill.side ?? "?");
  const realized = fill.realized_pnl ?? fill.realized_pl ?? fill.pnl;
  const pnlSuffix = realized === undefined || realized === null || realized === "" ? "" : `, realized P&L ${String(realized)}`;
  return makeEvent(eventType, "trade", title, `${side} ${qty} ${symbol} @ ${price}${pnlSuffix}`, timestamp, "broker_backed_paper_fills", fillIdentity(fill) ?? `${symbol}:${timestamp}`, 15, {
    broker_order_id: fill.broker_order_id ?? null,
    fill_id: fill.fill_id ?? null,
    realized_pnl: realized ?? null,
  });
}

function orderIdentity(row: JsonRecord): string | null {
  return stringOrNull(row.broker_order_id ?? row.order_intent_id ?? row.perm_id ?? row.order_ref);
}

function fillIdentity(row: JsonRecord): string | null {
  return stringOrNull(row.exec_id ?? row.execution_id ?? row.perm_id ?? row.fill_id ?? row.broker_order_id);
}

function marketDataConnected(global: JsonRecord, runtimeValues: JsonRecord, startupControlPlane: JsonRecord): boolean | null {
  const readiness = String(runtimeValues.market_data_readiness ?? global.market_data_status ?? global.market_data_label ?? "").toUpperCase();
  const staleCount = Number(runtimeValues.market_data_stale_count ?? 0);
  const dependency = dependencyByKey(startupControlPlane, "market_data_connectivity");
  if (dependency) {
    const state = String(dependency.state ?? "").toUpperCase();
    if (state === "READY") {
      return true;
    }
    if (state === "BLOCKED" || state === "DEGRADED") {
      return false;
    }
  }
  if (staleCount > 0 || /STALE|DISCONNECTED|UNAVAILABLE|DOWN|FAILED/.test(readiness)) {
    return false;
  }
  if (/LIVE|READY|CONNECTED|OK|FRESH/.test(readiness)) {
    return true;
  }
  return null;
}

function brokerConnected(productionLink: JsonRecord, brokerReachable: JsonRecord, runtimeValues: JsonRecord): boolean | null {
  if (typeof brokerReachable.ok === "boolean") {
    return brokerReachable.ok;
  }
  const text = String(productionLink.status ?? productionLink.connection ?? runtimeValues.auth_readiness ?? "").toUpperCase();
  if (/DISCONNECTED|UNAVAILABLE|DOWN|FAILED|STALE/.test(text)) {
    return false;
  }
  if (/CONNECTED|READY|OK|HEALTHY|ACTIVE/.test(text)) {
    return true;
  }
  return null;
}

function runtimeIsActive(runtimeValues: JsonRecord, readiness: JsonRecord, backendState: string | null): boolean | null {
  const status = String(runtimeValues.runtime_status ?? runtimeValues.runtime_recovery_state ?? readiness.runtime_status ?? "").toUpperCase();
  if (String(backendState ?? "").includes("backend_down")) {
    return false;
  }
  if (/STOPPED|DOWN|FAILED|DEAD|EXITED|OFFLINE/.test(status)) {
    return false;
  }
  if (/RUNNING|ACTIVE|READY|RECOVERED/.test(status) || readiness.runtime_running === true) {
    return true;
  }
  return null;
}

function readinessKey(startupControlPlane: JsonRecord, runtimeValues: JsonRecord, readiness: JsonRecord): string {
  return String(
    startupControlPlane.overall_state
      ?? startupControlPlane.primary_reason_code
      ?? runtimeValues.paper_trade_block_reason
      ?? readiness.paper_trade_block_reason
      ?? runtimeValues.paper_trade_allowed
      ?? "UNKNOWN",
  );
}

function readinessLabel(startupControlPlane: JsonRecord, runtimeValues: JsonRecord, readiness: JsonRecord): string {
  return String(
    startupControlPlane.summary_line
      ?? startupControlPlane.primary_reason
      ?? runtimeValues.paper_trade_block_reason
      ?? readiness.paper_trade_block_reason
      ?? "Track B readiness state changed.",
  );
}

function lifecycleBlocked(startupControlPlane: JsonRecord, runtimeValues: JsonRecord, readiness: JsonRecord): boolean {
  const counts = asRecord(startupControlPlane.counts);
  const reconciliationDependency = dependencyByKey(startupControlPlane, "reconciliation");
  const blockReason = String(runtimeValues.paper_trade_block_reason ?? readiness.paper_trade_block_reason ?? "").toLowerCase();
  return (
    Number(counts.reconciliation_required ?? 0) > 0
    || String(reconciliationDependency?.state ?? "").toUpperCase() === "BLOCKED"
    || blockReason.includes("reconciliation")
    || blockReason.includes("lifecycle")
  );
}

function staleAuthorityTruthArtifact(startupControlPlane: JsonRecord, runtimeValues: JsonRecord, trackBStatus: JsonRecord): boolean {
  const text = JSON.stringify({
    primary_reason_code: startupControlPlane.primary_reason_code,
    primary_reason: startupControlPlane.primary_reason,
    runtime_values: {
      authoritative_runtime_truth: runtimeValues.authoritative_runtime_truth,
      paper_readiness_source: runtimeValues.paper_readiness_source,
    },
    track_b_status: {
      dashboard_is_not_authority: trackBStatus.dashboard_is_not_authority,
      secondary_blockers: trackBStatus.secondary_blockers,
    },
  });
  return /stale.*(authority|truth|artifact)|(authority|truth).*stale/i.test(text);
}

function hardBlockReason(startupControlPlane: JsonRecord, runtimeValues: JsonRecord, trackBStatus: JsonRecord): string | null {
  const text = JSON.stringify({
    primary_reason_code: startupControlPlane.primary_reason_code,
    primary_reason: startupControlPlane.primary_reason,
    paper_trade_block_reason: runtimeValues.paper_trade_block_reason,
    track_b_safety_primary_warning: trackBStatus.track_b_safety_primary_warning,
    track_b_safety_warnings: trackBStatus.track_b_safety_warnings,
  });
  if (/guardian|control plane|control_plane|safe-state|safe_state|hard block|hard_block/i.test(text)) {
    return String(startupControlPlane.primary_reason ?? runtimeValues.paper_trade_block_reason ?? trackBStatus.track_b_safety_primary_warning ?? "Hard block active.");
  }
  return null;
}

function dependencyByKey(startupControlPlane: JsonRecord, key: string): JsonRecord | null {
  for (const dependency of asArray(startupControlPlane.dependencies)) {
    if (String(dependency.key ?? "") === key) {
      return dependency;
    }
  }
  return null;
}

function quietHoursActive(start: string, end: string, now: Date): boolean {
  const nowMinutes = now.getHours() * 60 + now.getMinutes();
  const startMinutes = clockMinutes(start);
  const endMinutes = clockMinutes(end);
  if (startMinutes === endMinutes) {
    return true;
  }
  if (startMinutes < endMinutes) {
    return nowMinutes >= startMinutes && nowMinutes < endMinutes;
  }
  return nowMinutes >= startMinutes || nowMinutes < endMinutes;
}

function clockMinutes(value: string): number {
  const match = /^(\d{2}):(\d{2})$/.exec(value);
  if (!match) {
    return 0;
  }
  return Math.min(23, Number(match[1])) * 60 + Math.min(59, Number(match[2]));
}

function normalizeClockValue(value: unknown, fallback: string): string {
  const text = String(value ?? "").trim();
  return /^\d{2}:\d{2}$/.test(text) ? text : fallback;
}

function positiveInteger(value: unknown, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? Math.round(parsed) : fallback;
}

function isSeverity(value: unknown): value is OperatorNotificationSeverity {
  return value === "info" || value === "warning" || value === "critical" || value === "trade";
}

function textSearchFlag(record: JsonRecord, pattern: RegExp): boolean {
  return pattern.test(JSON.stringify(record));
}

function formatSymbol(row: JsonRecord): string {
  return String(row.symbol ?? row.instrument ?? "UNKNOWN");
}

function asRecord(value: unknown): JsonRecord {
  return isRecord(value) ? value : {};
}

function asArray(value: unknown): JsonRecord[] {
  return Array.isArray(value) ? value.filter(isRecord) : [];
}

function isRecord(value: unknown): value is JsonRecord {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function stringOrNull(value: unknown): string | null {
  const text = String(value ?? "").trim();
  return text ? text : null;
}
