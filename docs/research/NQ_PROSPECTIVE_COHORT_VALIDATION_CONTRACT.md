# NQ Prospective Cohort Validation Contract

Status: Draft

## Purpose

Freeze the INV-005 and INV-006 NQ discovery definitions and evaluate later qualified NQ trades out of sample.

## Frozen Discovery Population

The discovery population is all `SOURCE_INTEGRITY_QUALIFIED` NQ trades through `2026-08-06` inclusive.

The prospective start is `2026-08-07 00:00:00 America/New_York`.

Discovery definitions are immutable for this validation cycle. Cohorts must not be redefined after observing prospective outcomes.

## Guardrails

- `diagnostic_only=true`
- `production_recommendation=false`
- `trading_gate=false`
- `live_money_eligible=false`

## Population Rules

Prospective rows must be NQ trades at or after the prospective start, must be research-eligible as `ELIGIBLE_ORDINARY_STRATEGY_EVIDENCE` or `ELIGIBLE_WITH_LIMITATIONS`, must have valid exact CRR layers, must have no broken joins, and must include realized P&L proxy.

Research eligibility remains authoritative for inclusion. Source-confirmed anomalies remain excluded. Missing context remains missing.

## Frozen Baselines

The monitor freezes at least:

- NQ GLOBEX Participation Long: `PAPER_ACTIVE_EVIDENCE_NQ_GLOBEX_PARTICIPATION_LONG_V1`, side `LONG`, session `GLOBEX`.
- NQ US Participation Long: `PAPER_ACTIVE_EVIDENCE_NQ_US_PARTICIPATION_LONG_V1`, side `LONG`, session `US`.
- INV-005 peer-level definitions that met the minimum sample threshold.

Each baseline preserves cohort definition, discovery trade count, date range, P&L proxy metrics, concentration, rolling windows where supported, RA8 coverage, missingness, source fingerprints, and deterministic fingerprint.

## Validation States

- `NOT_ENOUGH_PROSPECTIVE_DATA`
- `EARLY_POSITIVE_CONFIRMATION`
- `EARLY_MIXED_EVIDENCE`
- `EARLY_NEGATIVE_DIVERGENCE`
- `PROSPECTIVE_CONFIRMATION_STRENGTHENING`
- `PROSPECTIVE_CONFIRMATION_WEAKENING`
- `INCONCLUSIVE`

Fewer than 10 prospective trades remains `NOT_ENOUGH_PROSPECTIVE_DATA`. Ten to 19 trades is early descriptive evidence only. Twenty or more trades allows stronger descriptive comparison. Fifty or more trades allows rolling-window comparison.

No state is a production recommendation.

## Checkpoints

Deterministic checkpoints occur at 10, 20, 50, and 100 prospective trades. Checkpoint history is append-only and reproducible, preserving population fingerprint, metrics, comparison with discovery, contradictory evidence, missingness, confidence, validation state, and generated timestamp.

## Authority Boundary

This contract authorizes no strategy, sizing, runtime, broker, readiness, Guardian, Safe-State, Managed Exit, or trading-gate change.
