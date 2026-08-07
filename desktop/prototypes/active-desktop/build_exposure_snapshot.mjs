import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const prototypeRoot = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = process.env.OBSERVATORY_REPO_ROOT
  ? path.resolve(process.env.OBSERVATORY_REPO_ROOT)
  : path.resolve(prototypeRoot, "../../..");
const outputPath = process.env.OBSERVATORY_EXPOSURE_OUTPUT_PATH
  ? path.resolve(process.env.OBSERVATORY_EXPOSURE_OUTPUT_PATH)
  : path.join(prototypeRoot, "exposure_snapshot.generated.mjs");

const sources = Object.freeze({
  managedPositions: "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
  openOrderTruth: "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
  reconciliation: "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
});

function readJson(relativePath) {
  const artifactPath = path.join(repoRoot, relativePath);
  if (!fs.existsSync(artifactPath)) {
    return { artifactPath: relativePath, status: "MISSING", data: null, error: "source_artifact_missing" };
  }
  try {
    return { artifactPath: relativePath, status: "LOADED", data: JSON.parse(fs.readFileSync(artifactPath, "utf8")), error: null };
  } catch (error) {
    return { artifactPath: relativePath, status: "INVALID", data: null, error: error instanceof Error ? error.message : String(error) };
  }
}

function stringField(data, keys) {
  if (!data || typeof data !== "object") return null;
  for (const key of keys) {
    const value = data[key];
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return null;
}

function timestampField(data) {
  return stringField(data, ["generated_at", "authority_cycle_generated_at", "latest_record_at"]);
}

function parseQuantity(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function normalizeSide(row, quantity) {
  const explicit = stringField(row, ["side", "direction"]);
  if (explicit) {
    const normalized = explicit.toUpperCase();
    if (normalized.includes("SHORT") || normalized === "SELL") return "SHORT";
    if (normalized.includes("LONG") || normalized === "BUY") return "LONG";
  }
  if (typeof quantity === "number") {
    if (quantity < 0) return "SHORT";
    if (quantity > 0) return "LONG";
  }
  return "UNKNOWN";
}

function ageSeconds(openedAt, asOf) {
  if (!openedAt || !asOf) return null;
  const opened = Date.parse(openedAt);
  const generated = Date.parse(asOf);
  if (!Number.isFinite(opened) || !Number.isFinite(generated)) return null;
  return Math.max(0, Math.round((generated - opened) / 1000));
}

function positionRows(data) {
  if (!data || typeof data !== "object") return [];
  for (const key of ["managed_positions", "positions", "rows", "current_positions"]) {
    if (Array.isArray(data[key])) return data[key];
  }
  return [];
}

function normalizePosition(row, generatedAt) {
  const rawQuantity = parseQuantity(row.quantity ?? row.qty ?? row.owned_qty ?? row.position_qty ?? row.net_quantity);
  const side = normalizeSide(row, rawQuantity);
  const quantity = rawQuantity === null ? null : Math.abs(rawQuantity);
  const openedAt = stringField(row, ["opened_at", "entry_time", "entry_timestamp", "created_at", "first_opened_at"]);
  return {
    instrument: stringField(row, ["instrument", "symbol", "underlying"]) || "UNKNOWN",
    contract: stringField(row, ["contract", "local_symbol", "contract_key"]) || "UNKNOWN",
    side,
    quantity,
    opened_at: openedAt,
    age_seconds: ageSeconds(openedAt, generatedAt),
    managed_status: stringField(row, ["managed_status", "attribution_status", "status"]) || stringField(row, ["classification"]) || "UNKNOWN",
    lifecycle_id: stringField(row, ["lifecycle_id"]) || null,
    trade_id: stringField(row, ["trade_id", "source_trade_id"]) || null,
    strategy_id: stringField(row, ["strategy_id", "lane_id"]) || null,
  };
}

function orderCount(data) {
  if (!data || typeof data !== "object") return null;
  for (const key of ["open_order_count", "unknown_order_count"]) {
    const value = parseQuantity(data[key]);
    if (value !== null) return value;
  }
  for (const key of ["open_orders", "broker_open_orders", "orders", "unknown_orders"]) {
    if (Array.isArray(data[key])) return data[key].length;
  }
  return null;
}

function freshness(data) {
  if (!data || typeof data !== "object") return {};
  return {
    generated_at: timestampField(data),
    fresh_until: data.fresh_until ?? data.expires_at ?? null,
    producer_fresh: data.fresh ?? null,
    classification: stringField(data, ["classification", "status"]),
  };
}

function visualState(positions) {
  if (!positions.length) {
    return {
      exposure_state: "FLAT",
      display_class: "FLAT",
      visual_material: "exceptionally clear river; no exposure weight",
      exposure_age_seconds: 0,
    };
  }

  const longCount = positions.filter((position) => position.side === "LONG").length;
  const shortCount = positions.filter((position) => position.side === "SHORT").length;
  const knownAges = positions.map((position) => position.age_seconds).filter((age) => typeof age === "number");
  const exposureAge = knownAges.length ? Math.min(...knownAges) : null;
  const isFresh = exposureAge !== null && exposureAge < 900;

  if (positions.length === 1 && longCount === 1) {
    return {
      exposure_state: isFresh ? "ONE_NEW_LONG" : "ONE_SETTLED_LONG",
      display_class: "LIGHT_EXPOSURE",
      visual_material: isFresh ? "modest fresh exposure bloom settling into the river" : "modest integrated exposure density",
      exposure_age_seconds: exposureAge,
    };
  }
  if (positions.length === 1 && shortCount === 1) {
    return {
      exposure_state: "ONE_NEW_SHORT",
      display_class: "LIGHT_EXPOSURE",
      visual_material: isFresh ? "modest fresh exposure bloom settling into the river" : "modest integrated exposure density",
      exposure_age_seconds: exposureAge,
    };
  }
  if (longCount > 0 && shortCount > 0) {
    return {
      exposure_state: "MULTIPLE_MIXED_POSITIONS",
      display_class: "MIXED_EXPOSURE",
      visual_material: "combined material weight for mixed current positions without side severity coloring",
      exposure_age_seconds: exposureAge,
    };
  }
  return {
    exposure_state: "BUILDING_SAME_DIRECTION_EXPOSURE",
    display_class: "HEAVY_EXPOSURE",
    visual_material: "deeper same-direction exposure weight without implying risk severity",
    exposure_age_seconds: exposureAge,
  };
}

function buildSnapshot() {
  const managedPositions = readJson(sources.managedPositions);
  const openOrderTruth = readJson(sources.openOrderTruth);
  const reconciliation = readJson(sources.reconciliation);
  const generatedAt = timestampField(managedPositions.data) || timestampField(openOrderTruth.data) || timestampField(reconciliation.data);

  const positions = managedPositions.data
    ? positionRows(managedPositions.data).map((row) => normalizePosition(row, generatedAt))
    : [];
  const aggregate = {
    position_count: positions.length,
    long_count: positions.filter((position) => position.side === "LONG").length,
    short_count: positions.filter((position) => position.side === "SHORT").length,
    gross_contracts: positions.reduce((total, position) => total + (typeof position.quantity === "number" ? position.quantity : 0), 0),
  };
  const sourceProblems = [managedPositions, openOrderTruth, reconciliation].filter((source) => source.status !== "LOADED");
  const visual = managedPositions.status === "LOADED"
    ? visualState(positions)
    : {
      exposure_state: "UNKNOWN",
      display_class: managedPositions.status === "INVALID" ? "INVALID" : "UNKNOWN",
      visual_material: "exposure source unavailable; no exposure state inferred",
      exposure_age_seconds: null,
    };

  return {
    schema_version: "observatory_exposure_snapshot_v1",
    generated_at: generatedAt || null,
    source_kind: "PREPARED_MANAGED_EXPOSURE_ARTIFACTS",
    model_status: sourceProblems.length ? "VALID_WITH_WARNINGS" : "READY",
    source_artifacts: {
      managed_positions: managedPositions.artifactPath,
      open_order_truth: openOrderTruth.artifactPath,
      reconciliation: reconciliation.artifactPath,
    },
    source_status: {
      managed_positions: managedPositions.status,
      open_order_truth: openOrderTruth.status,
      reconciliation: reconciliation.status,
    },
    positions,
    aggregate,
    pending_order_count: orderCount(openOrderTruth.data),
    reconciliation_status: reconciliation.data ? stringField(reconciliation.data, ["classification", "reconciliation_status"]) : reconciliation.status,
    freshness: {
      managed_positions: freshness(managedPositions.data),
      open_order_truth: freshness(openOrderTruth.data),
      reconciliation: freshness(reconciliation.data),
    },
    visual,
    guardrails: {
      display_only: true,
      trading_input: false,
      broker_authority: false,
      runtime_authority: false,
      strategy_input: false,
    },
  };
}

const snapshot = buildSnapshot();
fs.writeFileSync(
  outputPath,
  `export const exposureSnapshot = ${JSON.stringify(snapshot, null, 2)};\n`,
  "utf8",
);
console.log(JSON.stringify({
  ok: true,
  output: path.relative(repoRoot, outputPath),
  positions: snapshot.aggregate.position_count,
  display_class: snapshot.visual.display_class,
}, null, 2));
