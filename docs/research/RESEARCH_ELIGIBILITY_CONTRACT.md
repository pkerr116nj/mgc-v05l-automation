# Research Eligibility Contract

Status: Draft

## Purpose

This contract defines deterministic record-level research eligibility for historical
Canonical Research Record rows. It changes research population membership only.
It does not alter canonical history, broker state, runtime authority, Managed
Exit authority, readiness, reconciliation, Guardian, Safe-State, strategy logic,
or trading gates.

## Classifications

- `ELIGIBLE_ORDINARY_STRATEGY_EVIDENCE`: source evidence supports ordinary
  completed strategy evidence with no known source-integrity exclusion.
- `ELIGIBLE_WITH_LIMITATIONS`: source evidence is usable for research, but known
  limitations such as missing RA8, missing MFE/MAE, missing contract-economics
  provenance, or context gaps must remain visible.
- `EXCLUDED_CONFIRMED_SOURCE_INTEGRITY_ANOMALY`: source-confirmed
  transformation, price-domain, identity, or duplicate execution-evidence defect.
- `EXCLUDED_CONFIRMED_DEVELOPMENT_OR_LEAK_TEST_ARTIFACT`: source-confirmed
  development, leak-test, or non-representative test artifact.
- `EXCLUDED_CONFIRMED_OPERATIONAL_OR_LIFECYCLE_ANOMALY`: source-confirmed
  operational or lifecycle defect that invalidates ordinary research use.
- `EXCLUDED_CONFIRMED_PNL_OR_DATA_QUALITY_INVALID`: source-confirmed P&L or
  data-quality invalidity.
- `REVIEW_REQUIRED_INSUFFICIENT_EVIDENCE`: available evidence is insufficient to
  determine whether the row is ordinary strategy evidence or source-invalid.
- `NOT_APPLICABLE`: the source row is outside the completed-trade research scope.

## Record Shape

Each eligibility record must include:

- `research_record_id`
- `classification`
- `evidence_basis`
- `supporting_artifact_paths`
- `supporting_ids`
- `confidence`
- `limitations`
- `contradictory_evidence`
- `review_required`
- `source_fingerprint`
- `generated_at`
- `deterministic_fingerprint`
- `guardrails`

Guardrails are always:

- `diagnostic_only=true`
- `production_recommendation=false`
- `trading_gate=false`

## Classification Rules

- One eligibility record is produced for every CRR row.
- Only source-confirmed defects may be excluded.
- The five INV-001 source-confirmed anomalies are excluded as
  `EXCLUDED_CONFIRMED_SOURCE_INTEGRITY_ANOMALY`.
- Missing RA8 alone does not make a record ineligible.
- Missing independent contract point-value provenance remains visible as a
  limitation.
- Unresolved historical records default to `ELIGIBLE_WITH_LIMITATIONS` when they
  have usable CRR/CTOL/CTOE evidence and no source-confirmed exclusion.
- Records with broken joins or missing required outcome evidence become
  `REVIEW_REQUIRED_INSUFFICIENT_EVIDENCE`.
- No classification may be inferred from P&L magnitude alone.
- Historical source rows are never rewritten, deleted, or silently hidden.

## Population Views

- `FULL_HISTORICAL`: all CRR rows.
- `SOURCE_INTEGRITY_QUALIFIED`: excludes only source-confirmed source-integrity
  anomalies.
- `ORDINARY_STRATEGY_EVIDENCE`: includes rows classified as ordinary strategy
  evidence or eligible with documented limitations.
- `REVIEW_REQUIRED`: rows that require unresolved research review.

Every view must report inclusion and exclusion counts, metrics, missingness, and
RA8 coverage. Reports must identify the selected population view explicitly.

## Authority Boundary

Research eligibility is a derived research artifact. It has no broker, runtime,
Managed Exit, strategy, Guardian, Safe-State, readiness, reconciliation, or
trading-gate authority.
