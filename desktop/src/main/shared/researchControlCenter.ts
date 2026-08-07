import fs from "node:fs";
import path from "node:path";

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
  source: "fixture" | "prepared_artifacts";
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

interface EvidenceCoverageOptions {
  repoRoot?: string;
  artifactRoot?: string;
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

export function researchReadModelResult(name: string, options: EvidenceCoverageOptions = {}): ResearchReadModelResult {
  if (!isResearchReadModelName(name)) {
    return unsupportedReadModelResult(name);
  }
  if (name === "research_evidence_coverage_v1") {
    return researchEvidenceCoverageReadModelResult(options);
  }
  return fixtureResearchReadModelResult(name);
}

export function researchEvidenceCoverageReadModelResult(options: EvidenceCoverageOptions = {}): ResearchReadModelResult {
  const modelName: ResearchReadModelName = "research_evidence_coverage_v1";
  try {
    const payload = buildResearchEvidenceCoverageReadModel(options);
    const validation = validateResearchReadModel(modelName, payload);
    return {
      ok: validation.ok,
      model_name: modelName,
      model_status: validation.status,
      payload: validation.ok ? payload : null,
      error: validation.error,
    };
  } catch (error) {
    return {
      ok: false,
      model_name: modelName,
      model_status: "INVALID",
      payload: null,
      error: error instanceof Error ? error.message : String(error),
    };
  }
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

function unsupportedReadModelResult(name: string): ResearchReadModelResult {
  return {
    ok: false,
    model_name: "research_roadmap_v1",
    model_status: "INVALID",
    payload: null,
    error: `Unsupported research read-model: ${name}`,
  };
}

const SUPPORTED_SOURCE_SCHEMAS = {
  crr_validation: "canonical_research_record_validation_report_v1",
  research_eligibility: "research_eligibility_summary_v1",
  prospective_context_coverage: "prospective_market_context_coverage_audit_v1",
  research_evidence_explorer: "research_evidence_explorer_v1",
} as const;

const SOURCE_RELATIVE_PATHS: Record<keyof typeof SUPPORTED_SOURCE_SCHEMAS, string> = {
  crr_validation: path.join("canonical_research_record", "canonical_research_record_validation_report.json"),
  research_eligibility: path.join("research_eligibility", "research_eligibility_summary.json"),
  prospective_context_coverage: path.join("prospective_nq_cohort_monitor", "context_coverage_audit.json"),
  research_evidence_explorer: path.join("research_evidence_explorer", "research_evidence_explorer_v1.json"),
};

function defaultArtifactRoot(repoRoot: string): string {
  return path.join(repoRoot, "outputs", "track_b_execution_core", "research_analytics");
}

function buildResearchEvidenceCoverageReadModel(options: EvidenceCoverageOptions): ResearchReadModelEnvelope {
  const repoRoot = options.repoRoot || process.env.MGC_REPO_ROOT || process.cwd();
  const artifactRoot = options.artifactRoot || defaultArtifactRoot(repoRoot);
  const sources = Object.entries(SOURCE_RELATIVE_PATHS).map(([sourceId, relativePath]) =>
    readPreparedSource(artifactRoot, sourceId as keyof typeof SUPPORTED_SOURCE_SCHEMAS, relativePath),
  );
  const invalid = sources.find((source) => source.status === "INVALID");
  if (invalid) {
    throw new Error(`Unsupported source schema for ${invalid.source_id}: ${String(invalid.schema_version || "missing")}`);
  }

  const crr = sourceData(sources, "crr_validation");
  const eligibility = sourceData(sources, "research_eligibility");
  const context = sourceData(sources, "prospective_context_coverage");
  const explorer = sourceData(sources, "research_evidence_explorer");
  const crrCounts = isRecord(crr?.counts) ? crr.counts : {};
  const crrReadiness = Array.isArray(crr?.upstream_readiness) ? crr.upstream_readiness.filter(isRecord) : [];
  const crrRowCount = numberValue(crrCounts.canonical_research_records);
  const crrMissingByLayer = isRecord(crr?.missing_by_layer) ? crr.missing_by_layer : {};
  const crrBrokenByLayer = isRecord(crr?.broken_by_layer) ? crr.broken_by_layer : {};
  const explorerPopulation = isRecord(explorer?.population) ? explorer.population : {};
  const explorerCoverage = isRecord(explorerPopulation.coverage) ? explorerPopulation.coverage : {};
  const eligibilityCounts = isRecord(eligibility?.classification_counts) ? eligibility.classification_counts : {};
  const contextFields = isRecord(context?.fields) ? context.fields : {};

  const ra8Missing = numberValue(crrMissingByLayer.ra8);
  const ra8Available = crrRowCount === null || ra8Missing === null ? null : crrRowCount - ra8Missing;
  const contextCoverage = ["gre", "crfd", "vwap_relationship", "avwap_relationship", "opening_range_position"].map((field) =>
    coverageFromContextField(field, isRecord(contextFields[field]) ? contextFields[field] : null),
  );

  const warnings = [
    ...sources.filter((source) => source.status === "MISSING").map((source) => `missing_source:${source.source_id}`),
    ...sources.filter((source) => source.status === "VALID_WITH_WARNINGS").map((source) => `source_warning:${source.source_id}`),
    ...stringArray(explorer?.warnings),
    ...stringArray(crr?.refresh_guidance).map((value) => `crr_refresh_guidance:${value}`),
  ];

  const payload = {
    schema_version: "research_evidence_coverage_v1" as const,
    generated_at: latestGeneratedAt(sources),
    model_status: sources.some((source) => source.status !== "HEALTHY") || warnings.length > 0 ? "VALID_WITH_WARNINGS" as const : "HEALTHY" as const,
    source: "prepared_artifacts" as const,
    guardrails: GUARDRAILS,
    source_artifacts: sources.map(({ data: _data, ...source }) => source),
    source_fingerprints: Object.fromEntries(sources.map((source) => [source.source_id, source.fingerprint ?? null])),
    crr: {
      status: crr?.status ?? sourceStatus(sources, "crr_validation"),
      row_count: crrRowCount,
      expected_completed_count: numberValue(crrCounts.expected_completed_canonical_records),
      broken_join_count: numberValue(crrCounts.broken_join_count),
      missing_join_count: numberValue(crrCounts.missing_join_count),
      reconciliation_mismatch_count: numberValue(crrCounts.reconciliation_mismatch_count),
      upstream_readiness: crrReadiness.map((item) => ({
        source_name: item.source_name ?? null,
        readiness_classification: item.readiness_classification ?? null,
        row_count: numberValue(item.row_count),
        exact_join_count: numberValue(item.exact_join_count),
        missing_join_count: numberValue(item.missing_join_count),
        broken_join_count: numberValue(item.broken_join_count),
        coverage_percentage: numberValue(item.coverage_percentage),
        artifact_path: item.artifact_path ?? null,
      })),
    },
    research_eligibility: {
      total_count: numberValue(eligibility?.eligibility_record_count),
      input_crr_count: numberValue(eligibility?.input_crr_count),
      qualified_count: numberValue(eligibilityCounts.ELIGIBLE_WITH_LIMITATIONS),
      excluded_confirmed_anomaly_count: numberValue(eligibilityCounts.EXCLUDED_CONFIRMED_SOURCE_INTEGRITY_ANOMALY),
      review_required_count: Array.isArray(eligibility?.review_queue) ? eligibility.review_queue.length : null,
    },
    evidence_coverage: {
      ra8: {
        available_count: numberValue(explorerCoverage.ra8_exact_count) ?? ra8Available,
        missing_count: numberValue(explorerCoverage.ra8_missing_count) ?? ra8Missing,
        coverage_rate: numberValue(explorerCoverage.ra8_coverage_rate) ?? percentage(ra8Available, crrRowCount),
        status: ra8Missing && ra8Missing > 0 ? "VALID_WITH_WARNINGS" : "HEALTHY",
        source: "CRR validation and Research Evidence Explorer producer counts",
      },
      mfe: unavailableMetric("mfe", "No producer-authored aggregate MFE coverage count is available in the supported read-model sources."),
      mae: unavailableMetric("mae", "No producer-authored aggregate MAE coverage count is available in the supported read-model sources."),
      giveback: unavailableMetric("giveback", "No producer-authored aggregate giveback coverage count is available in the supported read-model sources."),
      contract_point_value_provenance: unavailableMetric("contract_point_value_provenance", "No producer-authored aggregate contract point-value provenance count is available in the supported read-model sources."),
      prospective_market_context: contextCoverage,
      broken_join_layers: crrBrokenByLayer,
    },
    coverage_items: [
      {
        key: "crr",
        label: "CRR readiness",
        status: crr?.status ?? sourceStatus(sources, "crr_validation"),
        count: crrRowCount,
        detail: `${numberValue(crrCounts.broken_join_count) ?? "unknown"} broken joins; ${numberValue(crrCounts.reconciliation_mismatch_count) ?? "unknown"} reconciliation mismatches.`,
      },
      {
        key: "research_eligibility",
        label: "Research eligibility",
        status: sourceStatus(sources, "research_eligibility"),
        count: numberValue(eligibility?.eligibility_record_count),
        detail: `${numberValue(eligibilityCounts.ELIGIBLE_WITH_LIMITATIONS) ?? "unknown"} eligible with limitations; ${numberValue(eligibilityCounts.EXCLUDED_CONFIRMED_SOURCE_INTEGRITY_ANOMALY) ?? "unknown"} source-confirmed exclusions.`,
      },
      {
        key: "ra8",
        label: "RA8 path coverage",
        status: ra8Missing && ra8Missing > 0 ? "VALID_WITH_WARNINGS" : "HEALTHY",
        count: ra8Available,
        total_count: crrRowCount,
        detail: `${ra8Available ?? "unknown"} exact finalized captures; ${ra8Missing ?? "unknown"} missing from CRR validation.`,
      },
      {
        key: "prospective_context",
        label: "Prospective context",
        status: sourceStatus(sources, "prospective_context_coverage"),
        count: contextCoverage.filter((item) => item.status !== "ABSENT").length,
        total_count: contextCoverage.length,
        detail: "Producer-declared context availability from the prospective NQ coverage audit.",
      },
      {
        key: "mfe_mae_giveback",
        label: "MFE/MAE/giveback",
        status: "NOT_READY",
        count: null,
        detail: "Aggregate coverage counts are not producer-authored in the supported RCC Phase 2 sources.",
      },
    ],
    warnings: Array.from(new Set(warnings)),
  };
  return withFingerprint(payload);
}

function readPreparedSource(artifactRoot: string, sourceId: keyof typeof SUPPORTED_SOURCE_SCHEMAS, relativePath: string) {
  const artifactPath = path.join(artifactRoot, relativePath);
  const sourceRelativePath = path.join("outputs", "track_b_execution_core", "research_analytics", relativePath);
  if (!fs.existsSync(artifactPath)) {
    return {
      source_id: sourceId,
      path: sourceRelativePath,
      schema_version: null,
      generated_at: null,
      fingerprint: null,
      status: "MISSING" as const,
      error: "source_artifact_missing",
      data: null,
    };
  }
  try {
    const data = JSON.parse(fs.readFileSync(artifactPath, "utf8")) as unknown;
    if (!isRecord(data)) {
      return { source_id: sourceId, path: sourceRelativePath, schema_version: null, generated_at: null, fingerprint: null, status: "INVALID" as const, error: "source_payload_not_object", data: null };
    }
    const schemaVersion = typeof data.schema_version === "string" ? data.schema_version : null;
    if (schemaVersion !== SUPPORTED_SOURCE_SCHEMAS[sourceId]) {
      return { source_id: sourceId, path: sourceRelativePath, schema_version: schemaVersion, generated_at: stringValue(data.generated_at), fingerprint: stringValue(data.deterministic_fingerprint) ?? stringValue(data.fingerprint), status: "INVALID" as const, error: "unsupported_schema_version", data };
    }
    return {
      source_id: sourceId,
      path: sourceRelativePath,
      schema_version: schemaVersion,
      generated_at: stringValue(data.generated_at),
      fingerprint: stringValue(data.deterministic_fingerprint) ?? stringValue(data.fingerprint) ?? derivedSourceFingerprint(data),
      status: producerStatus(sourceId, data),
      error: null,
      data,
    };
  } catch (error) {
    return { source_id: sourceId, path: sourceRelativePath, schema_version: null, generated_at: null, fingerprint: null, status: "INVALID" as const, error: error instanceof Error ? error.message : String(error), data: null };
  }
}

function sourceData(sources: readonly ReturnType<typeof readPreparedSource>[], sourceId: string): Record<string, unknown> | null {
  const data = sources.find((source) => source.source_id === sourceId)?.data;
  return isRecord(data) ? data : null;
}

function sourceStatus(sources: readonly ReturnType<typeof readPreparedSource>[], sourceId: string): ResearchModelStatus {
  return sources.find((source) => source.source_id === sourceId)?.status ?? "MISSING";
}

function producerStatus(sourceId: keyof typeof SUPPORTED_SOURCE_SCHEMAS, data: Record<string, unknown>): ResearchModelStatus {
  if (sourceId === "crr_validation" && data.status === "VALID_WITH_WARNINGS") {
    return "VALID_WITH_WARNINGS";
  }
  return "HEALTHY";
}

function coverageFromContextField(field: string, value: Record<string, unknown> | null): Record<string, unknown> {
  if (!value) {
    return { field, status: "MISSING", available_count: null, missing_count: null, coverage_rate: null };
  }
  const discovery = isRecord(value.discovery_coverage) ? value.discovery_coverage : {};
  return {
    field,
    status: value.status ?? "MISSING",
    available_count: numberValue(discovery.available_count),
    missing_count: numberValue(discovery.missing_count),
    total_count: numberValue(discovery.total_count),
    coverage_rate: numberValue(discovery.coverage_rate),
    source_artifact: value.source_artifact ?? null,
    implementation_requirement: value.implementation_requirement ?? null,
  };
}

function unavailableMetric(key: string, detail: string): Record<string, unknown> {
  return { key, status: "NOT_READY", available_count: null, missing_count: null, coverage_rate: null, detail };
}

function latestGeneratedAt(sources: readonly ReturnType<typeof readPreparedSource>[]): string {
  const generated = sources.map((source) => source.generated_at).filter((value): value is string => Boolean(value)).sort();
  return generated[generated.length - 1] || GENERATED_AT;
}

function numberValue(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function stringValue(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value : null;
}

function stringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];
}

function percentage(numerator: number | null, denominator: number | null): number | null {
  if (numerator === null || denominator === null || denominator <= 0) {
    return null;
  }
  return Number((numerator / denominator).toFixed(6));
}

function derivedSourceFingerprint(payload: unknown): string {
  return `source_${stableFingerprint(payload).replace(/^fixture_/, "")}`;
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
