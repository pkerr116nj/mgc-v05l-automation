type JsonRecord = Record<string, unknown>;

export type LivePnlLaneSortKey =
  | "strategy"
  | "symbol"
  | "verdict"
  | "live_capable"
  | "actionable_this_bar"
  | "cadence_state"
  | "hard_blocker"
  | "signals"
  | "intents"
  | "fills"
  | "last_signal"
  | "last_intent"
  | "last_fill"
  | "latest_activity";

export type LivePnlLaneSortDirection = "asc" | "desc";

export interface LivePnlLaneSortSpec {
  key: LivePnlLaneSortKey;
  direction: LivePnlLaneSortDirection;
}

type SortValueKind = "text" | "number" | "boolean" | "timestamp";

interface SortValueDescriptor {
  kind: SortValueKind;
  value: string | number | boolean | null;
}

function asRecord(value: unknown): JsonRecord {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as JsonRecord) : {};
}

function textValue(value: unknown): SortValueDescriptor {
  const text = String(value ?? "").trim();
  return { kind: "text", value: text ? text.toLocaleLowerCase() : null };
}

function numberValue(value: unknown): SortValueDescriptor {
  const numeric = Number(value);
  return { kind: "number", value: Number.isFinite(numeric) ? numeric : null };
}

function booleanValue(value: unknown): SortValueDescriptor {
  if (value === true || value === false) {
    return { kind: "boolean", value };
  }
  return { kind: "boolean", value: null };
}

function timestampValue(value: unknown): SortValueDescriptor {
  if (typeof value !== "string" || !value.trim()) {
    return { kind: "timestamp", value: null };
  }
  const parsed = Date.parse(value);
  return { kind: "timestamp", value: Number.isFinite(parsed) ? parsed : null };
}

function livePnlLaneSortValue(row: JsonRecord, key: LivePnlLaneSortKey): SortValueDescriptor {
  switch (key) {
    case "strategy":
      return textValue(row.strategy_name ?? row.standalone_strategy_id ?? row.lane_id);
    case "symbol":
      return textValue(row.instrument ?? row.symbol);
    case "verdict":
      return textValue(row.audit_verdict);
    case "live_capable":
      return booleanValue(row.live_capable);
    case "actionable_this_bar":
      return booleanValue(row.actionable_this_bar);
    case "cadence_state":
      return textValue(row.cadence_state ?? row.bar_state);
    case "hard_blocker":
      return textValue(row.latest_hard_blocker);
    case "signals":
      return numberValue(row.actionable_entry_signal_count);
    case "intents":
      return numberValue(row.total_intent_count);
    case "fills":
      return numberValue(row.total_fill_count);
    case "last_signal":
      return timestampValue(row.last_actionable_signal_timestamp);
    case "last_intent":
      return timestampValue(row.last_intent_timestamp);
    case "last_fill":
      return timestampValue(row.last_fill_timestamp);
    case "latest_activity":
      return timestampValue(
        row.latest_activity_timestamp
          ?? asRecord(row.strategy_performance_summary).latest_activity_timestamp
          ?? row.last_update_timestamp,
      );
  }
}

function compareSortValues(
  left: SortValueDescriptor,
  right: SortValueDescriptor,
  direction: LivePnlLaneSortDirection,
): number {
  if (left.value == null && right.value == null) {
    return 0;
  }
  if (left.value == null) {
    return 1;
  }
  if (right.value == null) {
    return -1;
  }
  const order = direction === "asc" ? 1 : -1;
  switch (left.kind) {
    case "text":
      return String(left.value).localeCompare(String(right.value)) * order;
    case "number":
    case "timestamp":
      return (Number(left.value) - Number(right.value)) * order;
    case "boolean": {
      const leftRank = left.value === true ? 1 : 0;
      const rightRank = right.value === true ? 1 : 0;
      return (leftRank - rightRank) * order;
    }
  }
}

export function sortLivePnlLaneRows(
  rows: JsonRecord[],
  spec: LivePnlLaneSortSpec | null,
): JsonRecord[] {
  const sourceRows = Array.isArray(rows) ? rows : [];
  if (!spec) {
    return [...sourceRows];
  }
  return sourceRows
    .map((row, index) => ({ row, index }))
    .sort((left, right) => {
      const compared = compareSortValues(
        livePnlLaneSortValue(left.row, spec.key),
        livePnlLaneSortValue(right.row, spec.key),
        spec.direction,
      );
      return compared !== 0 ? compared : left.index - right.index;
    })
    .map(({ row }) => row);
}

