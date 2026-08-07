export type ResearchReadModelName =
  | "research_questions_and_investigations_v1"
  | "research_evidence_coverage_v1"
  | "research_checkpoints_v1"
  | "research_roadmap_v1";

export type ResearchModelStatus = "HEALTHY" | "VALID_WITH_WARNINGS" | "STALE" | "MISSING" | "INVALID" | "NOT_READY";

export interface ResearchReadModelEnvelope {
  schema_version: ResearchReadModelName;
  generated_at: string;
  model_status: ResearchModelStatus;
  source: "fixture";
  guardrails: {
    diagnostic_only: true;
    production_recommendation: false;
    trading_gate: false;
    broker_authority: false;
    runtime_authority: false;
  };
  deterministic_fingerprint: string;
  [key: string]: unknown;
}

export interface ResearchReadModelResult {
  ok: boolean;
  model_name: ResearchReadModelName;
  model_status: ResearchModelStatus;
  payload: ResearchReadModelEnvelope | null;
  error: string | null;
}

const GENERATED_AT = "2026-08-06T12:00:00.000Z";

const GUARDRAILS = {
  diagnostic_only: true,
  production_recommendation: false,
  trading_gate: false,
  broker_authority: false,
  runtime_authority: false,
} as const;

const MODEL_NAMES: readonly ResearchReadModelName[] = [
  "research_questions_and_investigations_v1",
  "research_evidence_coverage_v1",
  "research_checkpoints_v1",
  "research_roadmap_v1",
];

export function researchReadModelNames(): readonly ResearchReadModelName[] {
  return MODEL_NAMES;
}

export function isResearchReadModelName(value: string): value is ResearchReadModelName {
  return MODEL_NAMES.includes(value as ResearchReadModelName);
}

export function fixtureResearchReadModel(name: ResearchReadModelName): ResearchReadModelEnvelope {
  switch (name) {
    case "research_questions_and_investigations_v1":
      return withFingerprint({
        schema_version: name,
        generated_at: GENERATED_AT,
        model_status: "VALID_WITH_WARNINGS",
        source: "fixture",
        guardrails: GUARDRAILS,
        investigations: [
          {
            investigation_id: "INV-004",
            title: "NQ Qualified Performance Attribution",
            question: "What explains NQ's qualified P&L contribution?",
            confidence: "PARTIAL",
            conclusion_status: "INCONCLUSIVE",
            latest_evidence_timestamp: "2026-08-06T12:00:00+00:00",
            source_paths: ["outputs/track_b_execution_core/research_analytics/investigations/INV-004/investigation.json"],
            source_fingerprints: ["fixture_inv_004_fingerprint"],
            contradictory_evidence: [
              "NQ has negative rolling windows despite positive aggregate qualified contribution.",
              "Sparse path/excursion evidence limits entry-versus-exit attribution.",
            ],
          },
          {
            investigation_id: "INV-006",
            title: "August NQ Performance Regime Attribution",
            question: "Why were qualified NQ results concentrated in August 2026?",
            confidence: "PARTIAL",
            conclusion_status: "PARTIALLY_SUPPORTED",
            latest_evidence_timestamp: "2026-08-06T12:00:00+00:00",
            source_paths: ["outputs/track_b_execution_core/research_analytics/investigations/INV-006/investigation.json"],
            source_fingerprints: ["fixture_inv_006_fingerprint"],
            contradictory_evidence: [
              "Temporal repository milestones do not prove causality.",
              "Structured market context is incomplete, so market-regime explanations are limited.",
            ],
          },
        ],
      });
    case "research_evidence_coverage_v1":
      return withFingerprint({
        schema_version: name,
        generated_at: GENERATED_AT,
        model_status: "VALID_WITH_WARNINGS",
        source: "fixture",
        guardrails: GUARDRAILS,
        coverage_items: [
          { key: "crr", label: "CRR readiness", status: "HEALTHY", count: 1338, detail: "Fixture CRR row count is available." },
          { key: "eligibility", label: "Research eligibility", status: "VALID_WITH_WARNINGS", count: 1338, detail: "Fixture eligibility includes source-confirmed exclusions." },
          { key: "ra8", label: "RA8 path coverage", status: "STALE", count: 526, detail: "Fixture path coverage remains partial." },
          { key: "prospective_context", label: "Prospective context", status: "MISSING", count: 0, detail: "Real context producer is not integrated in fixture Phase 1." },
          { key: "roadmap", label: "Structured roadmap", status: "INVALID", count: 0, detail: "No governed roadmap producer exists." },
        ],
      });
    case "research_checkpoints_v1":
      return withFingerprint({
        schema_version: name,
        generated_at: GENERATED_AT,
        model_status: "VALID_WITH_WARNINGS",
        source: "fixture",
        guardrails: GUARDRAILS,
        checkpoint_identity: "cohort_id + checkpoint_trade_count",
        prospective_start: "2026-08-07 00:00:00 America/New_York",
        cohorts: [
          { cohort_id: "all_prospective_qualified_nq", prospective_trade_count: 0, validation_state: "NOT_READY", latest_delta: null },
          { cohort_id: "nq_globex_participation_long", prospective_trade_count: 10, validation_state: "CHECKPOINT_REACHED", latest_delta: { average_pnl_delta: 125.5, win_rate_delta: 0.04 } },
          { cohort_id: "nq_us_participation_long", prospective_trade_count: 20, validation_state: "CHECKPOINT_REACHED", latest_delta: { average_pnl_delta: -80.25, win_rate_delta: -0.02 } },
        ],
        checkpoint_history: [
          { cohort_id: "nq_globex_participation_long", checkpoint_trade_count: 10, confidence: "LOW", contradictory_evidence: ["Thin sample remains below later checkpoints."] },
          { cohort_id: "nq_us_participation_long", checkpoint_trade_count: 10, confidence: "LOW", contradictory_evidence: ["Early cohort mix may not match discovery baseline."] },
          { cohort_id: "nq_us_participation_long", checkpoint_trade_count: 20, confidence: "PARTIAL", contradictory_evidence: ["Tail concentration remains visible."] },
        ],
      });
    case "research_roadmap_v1":
      return withFingerprint({
        schema_version: name,
        generated_at: GENERATED_AT,
        model_status: "NOT_READY",
        source: "fixture",
        guardrails: GUARDRAILS,
        reason: "GOVERNED_ROADMAP_PRODUCER_NOT_AVAILABLE",
        milestones: [],
      });
  }
}

export function fixtureResearchReadModelResult(name: string): ResearchReadModelResult {
  if (!isResearchReadModelName(name)) {
    return {
      ok: false,
      model_name: "research_roadmap_v1",
      model_status: "INVALID",
      payload: null,
      error: `Unsupported research read-model: ${name}`,
    };
  }
  const payload = fixtureResearchReadModel(name);
  const validation = validateResearchReadModel(name, payload);
  return {
    ok: validation.ok,
    model_name: name,
    model_status: validation.status,
    payload: validation.ok ? payload : null,
    error: validation.error,
  };
}

export function validateResearchReadModel(expectedName: ResearchReadModelName, payload: unknown): { ok: boolean; status: ResearchModelStatus; error: string | null } {
  if (!isRecord(payload)) {
    return { ok: false, status: "MISSING", error: "Read-model payload is missing." };
  }
  if (payload.schema_version !== expectedName) {
    return { ok: false, status: "INVALID", error: `Unsupported schema version: ${String(payload.schema_version)}` };
  }
  const status = String(payload.model_status || "INVALID") as ResearchModelStatus;
  if (!["HEALTHY", "VALID_WITH_WARNINGS", "STALE", "MISSING", "INVALID", "NOT_READY"].includes(status)) {
    return { ok: false, status: "INVALID", error: `Unsupported model status: ${status}` };
  }
  return { ok: true, status, error: null };
}

export function buildFixtureTriage(results: readonly ResearchReadModelResult[]): Record<string, unknown> {
  const okResults = results.filter((result) => result.ok && result.payload);
  const failedResults = results.filter((result) => !result.ok);
  const investigations = okResults
    .flatMap((result) => (result.payload?.investigations as unknown[]) || [])
    .filter(isRecord);
  const coverage = okResults
    .flatMap((result) => (result.payload?.coverage_items as unknown[]) || [])
    .filter(isRecord);
  const checkpoints = okResults
    .flatMap((result) => (result.payload?.checkpoint_history as unknown[]) || [])
    .filter(isRecord);

  return {
    what_changed: {
      newest_investigations: investigations.slice(0, 2),
      latest_checkpoint_deltas: checkpoints.slice(-2),
    },
    what_needs_attention: {
      evidence: coverage.filter((item) => ["STALE", "MISSING", "INVALID"].includes(String(item.status))),
      contradictory_evidence: investigations.flatMap((item) => (Array.isArray(item.contradictory_evidence) ? item.contradictory_evidence : [])),
      failed_models: failedResults.map((result) => ({ model_name: result.model_name, error: result.error })),
    },
    what_remains_credible: investigations.map((item) => ({
      investigation_id: item.investigation_id,
      confidence: item.confidence,
      conclusion_status: item.conclusion_status,
    })),
    roadmap: okResults.find((result) => result.model_name === "research_roadmap_v1")?.payload ?? null,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function withFingerprint<T extends Omit<ResearchReadModelEnvelope, "deterministic_fingerprint">>(payload: T): T & { deterministic_fingerprint: string } {
  return { ...payload, deterministic_fingerprint: stableFingerprint(payload) };
}

function stableFingerprint(payload: unknown): string {
  const material = stableStringify(payload);
  let hash = 2166136261;
  for (let index = 0; index < material.length; index += 1) {
    hash ^= material.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return `fixture_${(hash >>> 0).toString(16).padStart(8, "0")}`;
}

function stableStringify(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(stableStringify).join(",")}]`;
  }
  if (isRecord(value)) {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}
