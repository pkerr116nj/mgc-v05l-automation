# IP-001: Canonical Research Record v1

## Status

Draft implementation plan.

This plan is governed by `docs/architecture/decisions/ADR-001-canonical-research-record.md`.

It is a planning artifact only. It does not implement CRR, change runtime behavior, change broker behavior, change Managed Exit behavior, change strategy logic, create trading gates, or create production recommendations.

## Purpose

Define the smallest complete implementation slice for Canonical Research Record (CRR) v1.

CRR v1 creates a versioned JSONL research contract that makes completed Track B PAPER trades reconstructable from existing artifacts while preserving all authority boundaries.

CRR is derived and never authoritative.

## Governing Inputs

- `ENGINEERING_PROCESS.md`
- `PROJECT_PRINCIPLES.md`
- `SYSTEM_OVERVIEW.md`
- `docs/architecture/track-b-architectural-invariants.md`
- `docs/architecture/baselines/BA-001-current-research-platform.md`
- `docs/architecture/proposals/DP-001-canonical-research-record.md`
- `docs/architecture/reviews/RR-001-DP-001-canonical-research-record.md`
- `docs/architecture/decisions/ADR-001-canonical-research-record.md`
- `docs/epics/EPIC-002-research-platform.md`
- `docs/architecture/research-analytics-platform-vision.md`
- `outputs/reports/research_data_inventory/research_data_inventory.md`
- `outputs/reports/research_data_inventory/research_data_flow_diagram.md`

## 1. Current Source Modules And Artifacts To Reuse

### Source Modules

- `src/mgc_v05l/execution_core/track_b_strategy_performance_attachment.py`
  - Produces `canonical_trade_records.jsonl`.
  - Owns completed-trade identity, entry/exit anchors, and `path_capture` references.
- `src/mgc_v05l/execution_core/track_b_trade_outcome_layer.py`
  - Produces CTOL `canonical_trade_outcomes.jsonl`.
  - Owns completed-trade outcome metrics, paired/closed filtering, and CTOL data-quality flags.
  - Contains reusable logic for completed-paired eligibility and canonical-path lookup.
- `src/mgc_v05l/execution_core/track_b_trade_outcome_enrichment.py`
  - Produces CTOE `canonical_trade_outcome_enrichment.jsonl`.
  - Owns context enrichment and context-validity classifications.
- `src/mgc_v05l/execution_core/track_b_trade_decision_attribution.py`
  - Produces RA3 `canonical_trade_decision_attribution.jsonl`.
  - Owns deterministic entry/exit attribution and preserves `UNKNOWN`.
- `src/mgc_v05l/execution_core/track_b_canonical_trade_path_layer.py`
  - Produces RA7 `canonical_trade_paths.jsonl`.
  - Owns normalized path summaries, path coverage, and path provenance.
- `src/mgc_v05l/execution_core/track_b_offline_research_experiment_engine.py`
  - Existing example of building a research-facing population from CTOL, CTOE, and canonical paths.
  - Useful as a consumption pattern, not as the CRR source of authority.

### Source Artifacts

- `outputs/track_b_execution_core/strategy_performance/canonical_trade_records.jsonl`
- `outputs/track_b_execution_core/trade_outcome_layer/canonical_trade_outcomes.jsonl`
- `outputs/track_b_execution_core/trade_outcome_enrichment/canonical_trade_outcome_enrichment.jsonl`
- `outputs/track_b_execution_core/research_analytics/canonical_trade_path_layer/canonical_trade_paths.jsonl`
- `outputs/track_b_execution_core/research_analytics/live_trade_path_accumulator/finalized_trade_path_capture.jsonl`
- `outputs/track_b_execution_core/research_analytics/ra3_trade_decision_attribution/canonical_trade_decision_attribution.jsonl`

Operational artifacts may be referenced as provenance, but CRR v1 should not embed operational truth or use it as trade-metric authority.

## 2. First Implementation Blocker Or Missing Contract

The first blocker is the lack of one public, reusable join-resolution contract for completed-trade research reconstruction.

Current join logic exists, but it is distributed across private helpers and layer-specific indexes:

- CTOL has completed-paired filtering and canonical-path lookup.
- RA3 has canonical-record lookup from outcomes.
- RA7 has retained-path lookup and path provenance.
- CTOE has context-validity joins.
- REF has a merged research population builder.

CRR must not independently invent competing joins. The first implementation step should therefore expose or wrap a small shared research join resolver that reuses those established semantics and returns explicit join quality.

This is a bounded contract problem, not a reason to redesign CTOL, CTOE, RA7, RA8, RA3, or REF.

## 3. Proposed Source-Code Files To Add Or Change

### Add

- `src/mgc_v05l/execution_core/track_b_canonical_research_record.py`
  - Pure research builder, resolver orchestration, validation, summary, and markdown rendering.
  - No broker/runtime/Managed Exit imports.
- `src/mgc_v05l/app/track_b_canonical_research_record.py`
  - CLI wrapper for generation, validation, and sample reconstruction export.
- `tests/unit/execution_core/test_track_b_canonical_research_record.py`
  - Focused tests for schema, joins, validation, guardrails, determinism, and isolation.

### Add Or Expose If Needed

- A small public resolver helper inside `track_b_canonical_research_record.py`, or a new internal research-only helper such as:
  - `src/mgc_v05l/execution_core/track_b_research_join_resolution.py`

Prefer a CRR-local adapter first if the public API can call existing builders and indexes without changing upstream behavior. Add a shared helper only if tests show duplicated join logic would otherwise be unavoidable.

### Do Not Change

- Runtime launcher or submit paths.
- Broker refreshers or IBKR API code.
- Managed Exit authority.
- Guardian, Safe-State, readiness, reconciliation, or trading-gate modules.
- Strategy definitions or lane configuration.

## 4. Proposed Artifact Schema And Minimum V1 Fields

### Output Location

ADR-001 accepts:

`outputs/track_b_execution_core/research_analytics/canonical_research_record/canonical_research_records.jsonl`

Companion artifacts:

- `canonical_research_record_validation_report.json`
- `canonical_research_record_validation_report.md`
- `canonical_research_record_schema.json`
- `canonical_research_record_summary.md`
- Optional sample reconstruction: `sample_trade_reconstruction.json`

### Minimum Row Shape

```json
{
  "schema_version": "canonical_research_record_v1",
  "generated_at": "ISO-8601",
  "research_record_id": "crr_<stable_hash>",
  "trade_identity": {
    "trade_id": null,
    "source_trade_id": null,
    "lifecycle_id": null,
    "instrument": null,
    "contract": null,
    "con_id": null,
    "side": null,
    "quantity": null
  },
  "entry_anchor": {
    "entry_time": null,
    "entry_price": null,
    "entry_order_id": null,
    "entry_perm_id": null,
    "entry_exec_id": null,
    "strategy_id": null,
    "lane_id": null,
    "source": "canonical_trade_records",
    "cache_reconciled": false
  },
  "exit_anchor": {
    "exit_time": null,
    "exit_price": null,
    "exit_order_id": null,
    "exit_perm_id": null,
    "exit_exec_id": null,
    "exit_policy": null,
    "exit_reason": null,
    "source": "canonical_trade_records",
    "cache_reconciled": false
  },
  "outcome_ref": {
    "trade_outcome_id": null,
    "path": null,
    "fingerprint": null,
    "join_quality": "MISSING"
  },
  "enrichment_ref": {
    "trade_outcome_id": null,
    "path": null,
    "context_validity_summary": {},
    "join_quality": "MISSING"
  },
  "path_ref": {
    "capture_id": null,
    "canonical_trade_path_id": null,
    "path_status": null,
    "path_fingerprint": null,
    "join_quality": "MISSING"
  },
  "attribution_ref": {
    "trade_decision_attribution_id": null,
    "entry_attribution_status": null,
    "exit_attribution_status": null,
    "join_quality": "MISSING"
  },
  "join_quality": {
    "overall": "INCOMPLETE",
    "exact": [],
    "tolerance": [],
    "missing": [],
    "broken": [],
    "migration_debt": []
  },
  "source_provenance": [],
  "reconciliation": {
    "status": "NOT_VALIDATED",
    "mismatches": []
  },
  "guardrails": {
    "diagnostic_only": true,
    "production_recommendation": false,
    "trading_gate": false
  },
  "deterministic_fingerprint": "sha256"
}
```

### Minimum V1 Materialized Cache Fields

Only these cache values should be materialized in v1:

- completed-trade identity fields from Canonical Trade Records
- entry and exit anchor fields from Canonical Trade Records
- `trade_outcome_id` and key outcome metrics from CTOL as a lightweight outcome summary
- context validity summary from CTOE
- path status and identifiers from RA7/RA8
- attribution status and identifiers from RA3

Do not embed full path samples, full operational snapshots, full context payloads, or upstream records.

## 5. Join-Resolution Reuse Strategy

Preferred resolution order:

1. Start with completed paired Canonical Trade Records.
2. Use CTOL's completed-paired semantics for eligibility.
3. Resolve CTOL outcomes by exact source identifiers where present.
4. If exact source identifiers are unavailable, reuse CTOL/RA3 timestamp-and-contract tolerance semantics and mark the join as `TOLERANCE`.
5. Resolve CTOE by `trade_outcome_id`.
6. Resolve RA3 by `trade_outcome_id` first, then existing source-trade semantics.
7. Resolve RA7 by `trade_outcome_id`, `source_trade_id`, `capture_id`, or already emitted canonical path identifiers where available.
8. Resolve RA8 finalized capture only as a path-source reference, preferably via `capture_id`.

Join quality values:

- `EXACT`: stable identifier match.
- `TOLERANCE`: documented timestamp/contract/lane tolerance match.
- `MISSING`: required source evidence is absent or unavailable.
- `BROKEN`: referential evidence exists but resolves inconsistently or violates the join contract.

Every tolerance join must include:

- source artifacts involved
- fields used
- tolerance used
- why exact linkage was unavailable
- migration-debt note

## 6. Provenance And Versioning Model

Each CRR row should include per-source provenance entries:

- `source_name`
- `source_artifact_path`
- `source_schema_version`
- `source_generated_at`
- `source_fingerprint` or `source_hash`
- `record_identifier`
- `resolution_method`
- `resolution_quality`

File-level fingerprints should use SHA-256. Record-level fingerprints should hash normalized JSON payloads excluding `generated_at` and other run-time-only metadata.

CRR schema version:

- `canonical_research_record_v1`

Validation report schema version:

- `canonical_research_record_validation_report_v1`

## 7. Reconciliation And Validation Design

Validation should run as part of CRR generation and be callable independently.

Checks:

- Row count equals number of completed paired Canonical Trade Records selected for CRR.
- Materialized identity and anchor fields match Canonical Trade Records.
- CTOL joined rows reconcile instrument, side, entry/exit timestamps, and key P&L cache values.
- CTOE joined rows reconcile `trade_outcome_id` and context-validity fields.
- RA7 joined rows reconcile `trade_outcome_id`, `source_trade_id`, `capture_id` where present, path status, and path fingerprint.
- RA3 joined rows reconcile `trade_outcome_id` and attribution id.
- Missing joins and broken joins are counted separately.
- Tolerance joins are reported as migration debt.
- Guardrails are present and unchanged on every row.
- Deterministic regeneration produces identical row fingerprints for the same source set.

Validation result classes:

- `VALID`
- `VALID_WITH_WARNINGS`
- `INVALID_BROKEN_JOIN`
- `INVALID_RECONCILIATION_MISMATCH`
- `INVALID_SCHEMA`

Validation failure should fail the CRR CLI command by default when a broken join or reconciliation mismatch is found. Missing joins and tolerance joins may produce `VALID_WITH_WARNINGS` when they are explicitly reported.

## 8. Failure And Fail-Closed Behavior

CRR generation is research-only and fail-closed with respect to its own artifact publication.

Rules:

- If Canonical Trade Records are missing, write no CRR rows and fail validation.
- If CTOL is missing, generate CRR rows only if explicitly allowed in validation-only degraded mode; otherwise fail before publication.
- If CTOE, RA3, RA7, or RA8 evidence is missing, emit `MISSING` joins and validation warnings rather than fabricating fields.
- If expected referential evidence conflicts, emit `BROKEN` and fail validation.
- If output write fails, no runtime or broker behavior is affected.
- No CRR failure may stop, start, authorize, or block trading.

## 9. Test Plan

Focused unit tests:

- Create one CRR row from exact Canonical Trade Record, CTOL, CTOE, RA7, and RA3 fixture.
- Missing path evidence emits `MISSING`, not fabricated MFE/MAE.
- Tolerance CTOL join is marked `TOLERANCE` and migration debt is recorded.
- Broken RA7 path join fails validation.
- Materialized entry/exit cache mismatch fails validation.
- Guardrails are preserved.
- Deterministic fingerprint is stable across repeated runs with the same input.
- Source artifact provenance includes schema version, generated timestamp, path, and hash where available.
- Runtime/broker import boundary scan remains clean.
- CLI writes JSONL, schema, summary, and validation report.

Validation commands for implementation phase:

- Focused CRR tests.
- Existing focused CTOL/CTOE/RA3/RA7 tests if reusable helpers are touched.
- `py_compile` or `compileall` for new modules.
- JSON/JSONL parse for generated CRR artifacts.
- `git diff --check`.
- AST/import boundary scan for broker/runtime/strategy imports.

## 10. Representative Sample-Trade Reconstruction Test

Add a deterministic fixture representing one completed trade with:

- Canonical Trade Record identity and entry/exit anchors.
- CTOL outcome with `trade_outcome_id`.
- CTOE enrichment with context-validity flags.
- RA7 path summary with `canonical_trade_path_id`, path status, and fingerprint.
- RA3 attribution with entry and exit statuses.

Expected assertion:

- One CRR row is produced.
- `research_record_id` is stable.
- All joins are `EXACT`.
- Entry and exit anchor cache values reconcile to Canonical Trade Records.
- CTOL outcome values reconcile to the outcome fixture.
- Context validity is summarized without embedding full CTOE payload.
- Path and attribution references are lightweight.
- Source provenance can reconstruct the fixture chain.
- Guardrails remain diagnostic-only.

Also include a second fixture where path evidence is absent to prove missing evidence remains missing.

## 11. Incremental Implementation Phases

### Phase 1: Resolver And Schema Skeleton

- Add CRR module and CLI.
- Load source artifacts.
- Emit schema, empty/degraded validation, and source provenance inventory.
- No broad joins beyond Canonical Trade Record eligibility.

### Phase 2: Exact Joins And Row Generation

- Build CRR rows from completed paired Canonical Trade Records.
- Add exact CTOL, CTOE, RA7, and RA3 joins.
- Add deterministic fingerprints and guardrails.

### Phase 3: Tolerance/Missing/Broken Join Classification

- Reuse existing tolerance semantics where exact identifiers are unavailable.
- Emit join-quality classification.
- Add migration-debt entries.
- Fail validation on broken joins.

### Phase 4: Reconciliation Report

- Compare materialized cache values against source artifacts.
- Publish JSON and Markdown validation reports.
- Add representative sample reconstruction report.

### Phase 5: Integration Validation

- Run against current local artifacts.
- Parse generated JSONL.
- Confirm no runtime/broker/Managed Exit imports.
- Report counts, join-quality distribution, warnings, and first blocker if any.

## 12. Explicit Non-Goals

- Do not alter Canonical Trade Records.
- Do not alter CTOL or CTOE semantics.
- Do not alter RA7, RA8, RA3, CAE, or REF behavior except by calling or lightly exposing existing resolution helpers if needed.
- Do not write to broker, runtime, Managed Exit, Guardian, Safe-State, readiness, reconciliation, strategy, or trading-gate paths.
- Do not create production recommendations.
- Do not infer missing evidence.
- Do not embed full path samples.
- Do not choose DuckDB, Parquet, PostgreSQL, or a UI framework.
- Do not reorganize generated outputs.

## 13. Runtime/Research Isolation Proof

CRR v1 should prove isolation by:

- Importing only research/output parsing modules and standard-library helpers.
- Having no imports from IBKR broker clients, runtime launcher, strategy submission, Managed Exit actuator, Guardian, Safe-State, readiness, or reconciliation authority modules.
- Writing only under `outputs/track_b_execution_core/research_analytics/canonical_research_record/`.
- Including guardrails on every row and summary.
- Having tests that scan the CRR source and CLI for prohibited imports and broker/action vocabulary.
- Keeping CRR out of startup, runtime, Managed Exit, and broker refresh paths.

## 14. Rollback And Cleanup Approach

Rollback is simple because CRR v1 is additive and research-only.

Cleanup steps:

- Remove the CRR source module and CLI if implementation is abandoned.
- Remove CRR tests.
- Remove generated CRR artifacts under the CRR output directory.
- Leave upstream CTOL, CTOE, RA7, RA8, RA3, CAE, REF, runtime, and broker artifacts untouched.

No data migration or broker cleanup is required.

## 15. Estimated Implementation Cost And Expected Product ROI

Estimated implementation cost:

- Small-to-medium.
- Expected code footprint: one execution-core module, one CLI wrapper, one focused test file, and generated research artifacts.
- Expected implementation time: one bounded implementation phase plus validation.

Expected ROI:

- High research-platform ROI.
- Reduces repeated artifact-path and join rediscovery.
- Makes missing, tolerance, and broken joins visible.
- Gives future research scripts, CAE/REF consumers, investigations, and analytics applications one stable research-facing contract.
- Preserves existing investment in RA, CTOL, CTOE, CAE, REF, and Canonical Trade Records.

## 16. Stop Conditions

Stop implementation if any of these are encountered:

- CRR requires changing runtime, broker, Managed Exit, Guardian, Safe-State, readiness, reconciliation, or strategy behavior.
- CRR requires treating research artifacts as current broker/runtime truth.
- Existing join logic cannot be reused or exposed without a broader upstream refactor.
- Source artifacts lack enough identifiers to distinguish missing joins from broken joins.
- Validation finds reconciliation mismatches that imply upstream authoritative artifacts are inconsistent.
- Any implementation path would fabricate missing evidence.
- Any implementation path would create production recommendations or trading gates.

At a stop condition, publish the first blocker and return to Architecture Track rather than broadening implementation.
