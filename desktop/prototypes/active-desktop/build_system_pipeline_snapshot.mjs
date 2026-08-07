import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const prototypeRoot = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = process.env.OBSERVATORY_REPO_ROOT
  ? path.resolve(process.env.OBSERVATORY_REPO_ROOT)
  : path.resolve(prototypeRoot, "../../..");
const outputPath = process.env.OBSERVATORY_PIPELINE_OUTPUT_PATH
  ? path.resolve(process.env.OBSERVATORY_PIPELINE_OUTPUT_PATH)
  : path.join(prototypeRoot, "system_pipeline_snapshot.generated.mjs");

const sources = Object.freeze({
  marketData: "outputs/reports/phase1_databento_live_runtime_candles/latest_phase1_databento_live_listener_status.json",
  runtime: "outputs/track_b_execution_core/runtime_truth/latest_runtime_environment_truth.json",
  brokerTws: "outputs/operator_dashboard/runtime/latest_broker_session_authority.json",
  tradeEvidence: "outputs/track_b_execution_core/managed_exit_service/latest_managed_exit_service_status.json",
  crr: "outputs/track_b_execution_core/research_analytics/canonical_research_record/canonical_research_record_validation_report.json",
  prospective: "outputs/track_b_execution_core/research_analytics/prospective_nq_cohort_monitor/validation_report.json",
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
  return stringField(data, ["generated_at", "latest_record_at", "authority_source_timestamp"]);
}

function freshnessFields(data) {
  if (!data || typeof data !== "object") return {};
  return {
    current_lag_seconds: data.current_lag_seconds ?? null,
    latest_durable_completed_bar_ts: data.latest_durable_completed_bar_ts ?? null,
    fresh_until: data.fresh_until ?? null,
    expires_at: data.expires_at ?? data.broker_position_lease?.expires_at ?? data.broker_open_order_lease?.expires_at ?? null,
    producer_fresh: data.fresh ?? data.broker_position_lease?.fresh ?? data.broker_open_order_lease?.fresh ?? null,
  };
}

function visualMaterialForDisplayClass(displayClass) {
  return {
    HEALTHY: { texture: "HEALTHY", treatment: "clear lens; uninterrupted flow" },
    VALID_WITH_WARNINGS: { texture: "LOW_CONFIDENCE_DATA", treatment: "soft haze; evidence usable with visible caveats" },
    NOT_READY: { texture: "LOW_CONFIDENCE_DATA", treatment: "dim held lens; no inferred health" },
    STALE: { texture: "STALE_DATA", treatment: "sediment tone; producer evidence is stale" },
    EVIDENCE_UNRELIABLE: { texture: "BROKER_DISCONNECTED", treatment: "thinned fractured lens; source declares unreliable evidence" },
    DEGRADED: { texture: "LOW_CONFIDENCE_DATA", treatment: "amber-haze lens; producer declares degraded or mismatched state" },
    INVALID: { texture: "STALE_DATA", treatment: "opaque sediment; source invalid or unreadable" },
    UNKNOWN: { texture: "LOW_CONFIDENCE_DATA", treatment: "neutral haze; mapping unsupported" },
  }[displayClass] || { texture: "LOW_CONFIDENCE_DATA", treatment: "neutral haze; mapping unsupported" };
}

function classifyDisplay({ label, status }) {
  const normalized = String(status || "UNKNOWN").toUpperCase();
  if (normalized === "MISSING") return { displayClass: "UNKNOWN", mapping: "unsupported" };
  if (normalized === "INVALID") return { displayClass: "INVALID", mapping: "direct_normalization" };
  if (normalized === "STALE") return { displayClass: "STALE", mapping: "direct_normalization" };

  if (label === "Market Data" && normalized === "PHASE1_DATABENTO_LIVE_LISTENER_RUNNING") {
    return { displayClass: "HEALTHY", mapping: "direct_normalization" };
  }
  if (label === "Regime / Context" && normalized === "NOT_READY") {
    return { displayClass: "NOT_READY", mapping: "pass_through" };
  }
  if (label === "Magic Runtime" && normalized === "COMMIT_MISMATCH") {
    return { displayClass: "DEGRADED", mapping: "direct_normalization" };
  }
  if (label === "Magic Runtime" && normalized === "STALE_RUNTIME_TRUTH") {
    return { displayClass: "STALE", mapping: "direct_normalization" };
  }
  if (label === "Broker / TWS" && normalized === "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE") {
    return { displayClass: "EVIDENCE_UNRELIABLE", mapping: "direct_normalization" };
  }
  if (label === "Trade Evidence" && normalized === "NO_ELIGIBLE_EXITS") {
    return { displayClass: "HEALTHY", mapping: "direct_normalization" };
  }
  if (label === "Trade Evidence" && normalized === "TRACK_B_MANAGED_EXIT_SERVICE_POST_BROKER_MUTATION_REFRESH_RUNNING") {
    return { displayClass: "HEALTHY", mapping: "direct_normalization" };
  }
  if (label === "Trade Evidence" && normalized === "APPLY_SUCCEEDED") {
    return { displayClass: "HEALTHY", mapping: "direct_normalization" };
  }
  if ((label === "CRR / Research" || label === "Prospective Validation") && normalized === "VALID_WITH_WARNINGS") {
    return { displayClass: "VALID_WITH_WARNINGS", mapping: "pass_through" };
  }
  if (normalized.includes("DEGRADED")) return { displayClass: "DEGRADED", mapping: "direct_normalization" };
  if (normalized.includes("WARNING")) return { displayClass: "VALID_WITH_WARNINGS", mapping: "direct_normalization" };
  return { displayClass: "UNKNOWN", mapping: "unsupported" };
}

function stage({ label, source, status, generatedAt, freshness, detail, semantics, event = false }) {
  const producerStatus = status || "UNKNOWN";
  const classification = classifyDisplay({ label, status: producerStatus });
  const material = visualMaterialForDisplayClass(classification.displayClass);
  return {
    label,
    texture: material.texture,
    event,
    producer_status: producerStatus,
    display_class: classification.displayClass,
    display_mapping: classification.mapping,
    visual_material: material.treatment,
    source_artifact: source,
    generated_at: generatedAt || null,
    freshness: freshness || {},
    detail: detail || null,
    semantics: semantics || null,
  };
}

function buildSnapshot() {
  const marketData = readJson(sources.marketData);
  const runtime = readJson(sources.runtime);
  const brokerTws = readJson(sources.brokerTws);
  const tradeEvidence = readJson(sources.tradeEvidence);
  const crr = readJson(sources.crr);
  const prospective = readJson(sources.prospective);

  const marketDataStatus = marketData.data
    ? stringField(marketData.data, ["final_classification", "required_symbol_readiness_status", "provider_status"])
    : marketData.status;
  const runtimeStatus = runtime.data ? stringField(runtime.data, ["classification", "runtime_status"]) : runtime.status;
  const brokerStatus = brokerTws.data ? stringField(brokerTws.data, ["classification"]) : brokerTws.status;
  const tradeEvidenceStatus = tradeEvidence.data ? stringField(tradeEvidence.data, ["classification"]) : tradeEvidence.status;
  const crrStatus = crr.data ? stringField(crr.data, ["status"]) : crr.status;
  const prospectiveStatus = prospective.data ? stringField(prospective.data, ["status"]) : prospective.status;

  const stages = [
    stage({
      label: "Market Data",
      source: marketData.artifactPath,
      status: marketDataStatus,
      generatedAt: timestampField(marketData.data),
      freshness: freshnessFields(marketData.data),
      detail: marketData.data ? `required=${marketData.data.required_symbol_readiness_status || "UNKNOWN"}` : marketData.error,
      semantics: "Phase-1 listener reports live Databento runtime candle feed state and required-symbol freshness.",
    }),
    stage({
      label: "Regime / Context",
      source: null,
      status: "NOT_READY",
      generatedAt: null,
      freshness: {},
      detail: "No dedicated current Regime Monitor health artifact was found for this slice.",
      semantics: "Unsupported for real pipeline display until a current Regime Monitor health producer exists.",
    }),
    stage({
      label: "Magic Runtime",
      source: runtime.artifactPath,
      status: runtimeStatus,
      generatedAt: timestampField(runtime.data),
      freshness: freshnessFields(runtime.data),
      detail: runtime.data ? stringField(runtime.data, ["classification"]) : runtime.error,
      semantics: "Runtime Environment Truth classification is passed through as producer-authored process/state evidence.",
    }),
    stage({
      label: "Broker / TWS",
      source: brokerTws.artifactPath,
      status: brokerStatus,
      generatedAt: timestampField(brokerTws.data),
      freshness: freshnessFields(brokerTws.data),
      detail: brokerTws.data ? stringField(brokerTws.data, ["callback_missing_reason", "classification"]) : brokerTws.error,
      semantics: "Broker Session Authority reports read-only broker/TWS connection and callback evidence quality.",
    }),
    stage({
      label: "Trade Evidence",
      source: tradeEvidence.artifactPath,
      status: tradeEvidenceStatus,
      generatedAt: timestampField(tradeEvidence.data),
      freshness: freshnessFields(tradeEvidence.data),
      detail: tradeEvidence.data ? `eligible_exits=${tradeEvidence.data.eligible_count ?? "UNKNOWN"}` : tradeEvidence.error,
      semantics: "Managed Exit service status is used only as current managed lifecycle supervision evidence.",
    }),
    stage({
      label: "CRR / Research",
      source: crr.artifactPath,
      status: crrStatus,
      generatedAt: timestampField(crr.data),
      freshness: freshnessFields(crr.data),
      detail: crr.data ? `records=${crr.data.counts?.canonical_research_records ?? "UNKNOWN"}` : crr.error,
      semantics: "CRR validation report status is diagnostic research-readiness evidence only.",
    }),
    stage({
      label: "Prospective Validation",
      source: prospective.artifactPath,
      status: prospectiveStatus,
      generatedAt: timestampField(prospective.data),
      freshness: freshnessFields(prospective.data),
      detail: prospective.data ? `prospective_trades=${prospective.data.prospective_trade_count ?? "UNKNOWN"}` : prospective.error,
      semantics: "Prospective monitor validation status is producer-authored research checkpoint evidence only.",
    }),
  ];

  return {
    schema_version: "observatory_system_pipeline_snapshot_v1",
    generated_at: stages.map((item) => item.generated_at).filter(Boolean).sort().at(-1) || null,
    model_status: stages.some((item) => ["UNKNOWN", "INVALID", "STALE", "NOT_READY", "EVIDENCE_UNRELIABLE", "DEGRADED"].includes(item.display_class))
      ? "VALID_WITH_WARNINGS"
      : "READY",
    source_kind: "PREPARED_PRODUCER_STATUS_ARTIFACTS",
    guardrails: {
      display_only: true,
      broker_authority: false,
      runtime_authority: false,
      trading_gate: false,
      strategy_input: false,
      readiness_computation: false,
    },
    stages,
  };
}

const snapshot = buildSnapshot();
fs.writeFileSync(
  outputPath,
  `export const systemPipelineSnapshot = ${JSON.stringify(snapshot, null, 2)};\n`,
  "utf8",
);
console.log(JSON.stringify({ ok: true, output: path.relative(repoRoot, outputPath), stages: snapshot.stages.length }, null, 2));
