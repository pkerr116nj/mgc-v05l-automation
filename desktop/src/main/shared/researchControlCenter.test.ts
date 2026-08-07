import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

import {
  buildFixtureTriage,
  fixtureResearchReadModel,
  fixtureResearchReadModelResult,
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
