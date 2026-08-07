import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

import {
  buildFixtureTriage,
  fixtureResearchReadModel,
  fixtureResearchReadModelResult,
  researchEvidenceCoverageReadModelResult,
  researchReadModelResult,
  researchReadModelNames,
  validateResearchReadModel,
  type ResearchReadModelResult,
} from "./researchControlCenter";

test("bounded read-model fixtures parse and have deterministic fingerprints", () => {
  for (const name of researchReadModelNames()) {
    const first = fixtureResearchReadModel(name);
    const second = fixtureResearchReadModel(name);
    assert.equal(first.schema_version, name);
    assert.equal(first.source, "fixture");
    assert.equal(first.deterministic_fingerprint, second.deterministic_fingerprint);
    assert.equal(first.guardrails.broker_authority, false);
    assert.equal(first.guardrails.runtime_authority, false);
    assert.equal(first.guardrails.production_recommendation, false);
    assert.equal(first.guardrails.trading_gate, false);
  }
});

test("unsupported schema versions are rejected explicitly", () => {
  const result = validateResearchReadModel("research_evidence_coverage_v1", {
    ...fixtureResearchReadModel("research_evidence_coverage_v1"),
    schema_version: "research_evidence_coverage_v2",
  });
  assert.equal(result.ok, false);
  assert.equal(result.status, "INVALID");
  assert.match(result.error ?? "", /Unsupported schema version/);
});

test("model-level fail-soft keeps valid views when one read-model fails", () => {
  const valid = fixtureResearchReadModelResult("research_questions_and_investigations_v1");
  const invalid: ResearchReadModelResult = {
    ok: false,
    model_name: "research_evidence_coverage_v1",
    model_status: "INVALID",
    payload: null,
    error: "fixture failure",
  };
  const triage = buildFixtureTriage([valid, invalid]);
  const changed = triage.what_changed as Record<string, unknown>;
  const attention = triage.what_needs_attention as Record<string, unknown>;
  assert.ok(Array.isArray(changed.newest_investigations));
  assert.equal((changed.newest_investigations as unknown[]).length, 2);
  assert.deepEqual(attention.failed_models, [{ model_name: "research_evidence_coverage_v1", error: "fixture failure" }]);
});

test("Investigation confidence and contradictory evidence pass through unchanged", () => {
  const model = fixtureResearchReadModel("research_questions_and_investigations_v1");
  const investigations = model.investigations as Array<Record<string, unknown>>;
  assert.equal(investigations[0].confidence, "PARTIAL");
  assert.equal(investigations[0].conclusion_status, "INCONCLUSIVE");
  assert.deepEqual(investigations[0].contradictory_evidence, [
    "NQ has negative rolling windows despite positive aggregate qualified contribution.",
    "Sparse path/excursion evidence limits entry-versus-exit attribution.",
  ]);
});

test("fixture models do not synthesize Claim records", () => {
  const serialized = JSON.stringify(researchReadModelNames().map((name) => fixtureResearchReadModel(name)));
  assert.equal(serialized.includes('"claims"'), false);
  assert.equal(serialized.includes('"claim_id"'), false);
});

test("Roadmap remains NOT_READY until a governed producer exists", () => {
  const roadmap = fixtureResearchReadModel("research_roadmap_v1");
  assert.equal(roadmap.model_status, "NOT_READY");
  assert.equal(roadmap.reason, "GOVERNED_ROADMAP_PRODUCER_NOT_AVAILABLE");
});

test("checkpoint fixtures include zero, ten, and twenty trade states", () => {
  const model = fixtureResearchReadModel("research_checkpoints_v1");
  const cohorts = model.cohorts as Array<Record<string, unknown>>;
  const counts = cohorts.map((cohort) => cohort.prospective_trade_count).sort((a, b) => Number(a) - Number(b));
  assert.deepEqual(counts, [0, 10, 20]);
  assert.equal(model.checkpoint_identity, "cohort_id + checkpoint_trade_count");
});

test("RCC renderer and preload do not expose filesystem or operational mutation authority", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const preload = fs.readFileSync(path.join(repoRoot, "src/main/researchPreload.ts"), "utf8");
  const page = fs.readFileSync(path.join(repoRoot, "src/renderer/pages/research/ResearchControlCenterPage.tsx"), "utf8");
  assert.match(preload, /research-control-center:get-read-model/);
  assert.doesNotMatch(preload, /desktop:start-dashboard|desktop:stop-dashboard|desktop:run-dashboard-action|desktop:run-production-link-action/);
  assert.doesNotMatch(page, /from "node:fs"|from 'node:fs'|readFile|writeFile|fetch\(|XMLHttpRequest/);
  assert.doesNotMatch(page, /placeOrder|cancelOrder|globalCancel|submit_order|flatten_position/);
});

test("RCC business logic avoids broker runtime imports and hard-coded local paths", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const files = [
    "src/main/shared/researchControlCenter.ts",
    "src/main/researchPreload.ts",
    "src/main/researchWindow.ts",
    "src/renderer/pages/research/ResearchControlCenterPage.tsx",
  ];
  const combined = files.map((file) => fs.readFileSync(path.join(repoRoot, file), "utf8")).join("\n");
  assert.doesNotMatch(combined, /ibapi|mgc_v05l\.broker|mgc_v05l\.runtime|mgc_v05l\.strategy/);
  assert.doesNotMatch(combined, /\/Users\//);
  assert.doesNotMatch(combined, /open -a|osascript|xdg-open|start /);
  assert.doesNotMatch(combined.replace(/production_recommendation/g, ""), /recommend|should adopt|should change|promote|retire/i);
});

test("real evidence coverage read-model parses supported prepared artifacts", () => {
  const artifactRoot = makeEvidenceCoverageArtifacts();
  const result = researchEvidenceCoverageReadModelResult({ artifactRoot });
  assert.equal(result.ok, true);
  assert.equal(result.model_name, "research_evidence_coverage_v1");
  assert.equal(result.model_status, "VALID_WITH_WARNINGS");
  const payload = result.payload as Record<string, unknown>;
  assert.equal(payload.source, "prepared_artifacts");
  assert.equal(payload.generated_at, "2026-08-06T12:00:00+00:00");
  const crr = payload.crr as Record<string, unknown>;
  assert.equal(crr.row_count, 10);
  assert.equal(crr.broken_join_count, 0);
  const eligibility = payload.research_eligibility as Record<string, unknown>;
  assert.equal(eligibility.total_count, 10);
  assert.equal(eligibility.qualified_count, 8);
  assert.equal(eligibility.excluded_confirmed_anomaly_count, 2);
});

test("real evidence coverage preserves source fingerprints and producer statuses", () => {
  const artifactRoot = makeEvidenceCoverageArtifacts();
  const result = researchEvidenceCoverageReadModelResult({ artifactRoot });
  assert.equal(result.ok, true);
  const payload = result.payload as Record<string, unknown>;
  const sourceFingerprints = payload.source_fingerprints as Record<string, unknown>;
  assert.equal(sourceFingerprints.research_eligibility, "eligibility_fixture_fp");
  assert.equal(sourceFingerprints.prospective_context_coverage, "context_fixture_fp");
  const sources = payload.source_artifacts as Array<Record<string, unknown>>;
  const crr = sources.find((source) => source.source_id === "crr_validation");
  assert.equal(crr?.status, "VALID_WITH_WARNINGS");
});

test("real evidence coverage exposes producer-authored counts without confidence recomputation", () => {
  const artifactRoot = makeEvidenceCoverageArtifacts();
  const result = researchEvidenceCoverageReadModelResult({ artifactRoot });
  assert.equal(result.ok, true);
  const payload = result.payload as Record<string, unknown>;
  const evidence = payload.evidence_coverage as Record<string, unknown>;
  const ra8 = evidence.ra8 as Record<string, unknown>;
  assert.equal(ra8.available_count, 4);
  assert.equal(ra8.missing_count, 6);
  assert.equal(ra8.coverage_rate, 0.4);
  const serialized = JSON.stringify(payload);
  assert.doesNotMatch(serialized, /confidence|claim_id|conclusion_id/);
});

test("unsupported prepared source schema invalidates only the evidence coverage model", () => {
  const artifactRoot = makeEvidenceCoverageArtifacts({
    crr_validation: { schema_version: "canonical_research_record_validation_report_v2" },
  });
  const result = researchEvidenceCoverageReadModelResult({ artifactRoot });
  assert.equal(result.ok, false);
  assert.equal(result.model_status, "INVALID");
  assert.match(result.error ?? "", /Unsupported source schema/);

  const questions = researchReadModelResult("research_questions_and_investigations_v1", { artifactRoot });
  assert.equal(questions.ok, true);
  assert.equal(questions.payload?.source, "fixture");
});

test("missing prepared source is fail-soft for the affected evidence subsection", () => {
  const artifactRoot = makeEvidenceCoverageArtifacts({}, ["prospective_context_coverage"]);
  const result = researchEvidenceCoverageReadModelResult({ artifactRoot });
  assert.equal(result.ok, true);
  assert.equal(result.model_status, "VALID_WITH_WARNINGS");
  const payload = result.payload as Record<string, unknown>;
  const sources = payload.source_artifacts as Array<Record<string, unknown>>;
  const missing = sources.find((source) => source.source_id === "prospective_context_coverage");
  assert.equal(missing?.status, "MISSING");
  const coverageItems = payload.coverage_items as Array<Record<string, unknown>>;
  const context = coverageItems.find((item) => item.key === "prospective_context");
  assert.equal(context?.status, "MISSING");
});

test("real evidence coverage read-model is deterministic across repeated generation", () => {
  const artifactRoot = makeEvidenceCoverageArtifacts();
  const first = researchEvidenceCoverageReadModelResult({ artifactRoot });
  const second = researchEvidenceCoverageReadModelResult({ artifactRoot });
  assert.equal(first.ok, true);
  assert.equal(second.ok, true);
  assert.equal(first.payload?.deterministic_fingerprint, second.payload?.deterministic_fingerprint);
  assert.deepEqual(first.payload, second.payload);
});

function makeEvidenceCoverageArtifacts(overrides: Record<string, Record<string, unknown>> = {}, omit: string[] = []): string {
  const root = fs.mkdtempSync(path.join(process.env.TMPDIR || "/tmp", "rcc-evidence-"));
  const artifacts: Record<string, { relativePath: string; payload: Record<string, unknown> }> = {
    crr_validation: {
      relativePath: "canonical_research_record/canonical_research_record_validation_report.json",
      payload: {
        schema_version: "canonical_research_record_validation_report_v1",
        generated_at: "2026-08-06T06:02:38.742548+00:00",
        status: "VALID_WITH_WARNINGS",
        counts: {
          canonical_research_records: 10,
          expected_completed_canonical_records: 10,
          broken_join_count: 0,
          missing_join_count: 6,
          reconciliation_mismatch_count: 0,
        },
        missing_by_layer: { ra8: 6 },
        broken_by_layer: {},
        refresh_guidance: ["ra8_source_coverage_limit"],
        upstream_readiness: [
          {
            source_name: "ctol",
            readiness_classification: "READY",
            row_count: 10,
            exact_join_count: 10,
            missing_join_count: 0,
            broken_join_count: 0,
            coverage_percentage: 1,
            artifact_path: "outputs/track_b_execution_core/trade_outcome_layer/canonical_trade_outcomes.jsonl",
          },
        ],
      },
    },
    research_eligibility: {
      relativePath: "research_eligibility/research_eligibility_summary.json",
      payload: {
        schema_version: "research_eligibility_summary_v1",
        generated_at: "2026-08-06T12:00:00+00:00",
        deterministic_fingerprint: "eligibility_fixture_fp",
        eligibility_record_count: 10,
        input_crr_count: 10,
        classification_counts: {
          ELIGIBLE_WITH_LIMITATIONS: 8,
          EXCLUDED_CONFIRMED_SOURCE_INTEGRITY_ANOMALY: 2,
        },
        review_queue: [],
        source_confirmed_anomalies: [{ id: "a" }, { id: "b" }],
      },
    },
    prospective_context_coverage: {
      relativePath: "prospective_nq_cohort_monitor/context_coverage_audit.json",
      payload: {
        schema_version: "prospective_market_context_coverage_audit_v1",
        generated_at: "2026-08-06T12:00:00+00:00",
        deterministic_fingerprint: "context_fixture_fp",
        fields: {
          gre: {
            status: "AVAILABLE_ONLY_AS_VALIDITY_OR_UNAVAILABLE_STATE",
            source_artifact: "CRR enrichment_ref.context_validity_summary.gre_validity_classification",
            implementation_requirement: "research producer change",
            discovery_coverage: { available_count: 3, missing_count: 7, total_count: 10, coverage_rate: 0.3 },
          },
          crfd: {
            status: "ABSENT",
            source_artifact: "not present in CRR v1",
            implementation_requirement: "runtime producer change",
            discovery_coverage: { available_count: 0, missing_count: 10, total_count: 10, coverage_rate: 0 },
          },
        },
      },
    },
    research_evidence_explorer: {
      relativePath: "research_evidence_explorer/research_evidence_explorer_v1.json",
      payload: {
        schema_version: "research_evidence_explorer_v1",
        generated_at: "2026-08-06T12:00:00+00:00",
        deterministic_fingerprint: "explorer_fixture_fp",
        population: {
          coverage: {
            ra8_exact_count: 4,
            ra8_missing_count: 6,
            ra8_coverage_rate: 0.4,
          },
        },
        warnings: ["ra8_finalized_path_coverage_partial"],
      },
    },
  };

  for (const [key, artifact] of Object.entries(artifacts)) {
    if (omit.includes(key)) {
      continue;
    }
    const target = path.join(root, artifact.relativePath);
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.writeFileSync(target, `${JSON.stringify({ ...artifact.payload, ...(overrides[key] || {}) }, null, 2)}\n`, "utf8");
  }
  return root;
}
