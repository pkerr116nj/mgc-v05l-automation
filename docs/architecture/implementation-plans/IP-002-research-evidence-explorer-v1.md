# IP-002: Research Evidence Explorer v1

Status: Draft

## 1. First Research Question

Research Evidence Explorer v1 answers one bounded question:

How do the characteristics of the best completed trades differ from the worst completed trades?

The first slice is descriptive and deterministic. It does not infer causality, recommend production changes, or modify trading behavior.

## 2. Source Contract

The source contract is Canonical Research Record v1 only, plus source references already present in each CRR row.

Inputs:

- `outputs/track_b_execution_core/research_analytics/canonical_research_record/canonical_research_records.jsonl`
- `outputs/track_b_execution_core/research_analytics/canonical_research_record/canonical_research_record_validation_report.json`

The presentation layer consumes prepared Research Evidence Explorer artifacts. It must not independently reimplement CRR joins or research calculations.

## 3. Population Definition

Include CRR rows where:

- CRR validation is `VALID` or `VALID_WITH_WARNINGS`.
- Required upstream readiness rows are `READY`.
- The CRR row has no broken joins.
- CTOL, CTOE, RA7, and RA3 joins are exact.
- `outcome_summary.realized_pnl_proxy` is present and numeric.

RA8 finalized path coverage is optional. Missing RA8 evidence is retained as explicit source coverage limitation.

## 4. Best And Worst Definitions

Primary comparison:

- Top decile by realized P&L proxy.
- Bottom decile by realized P&L proxy.

This global comparison is portfolio-outcome analysis. It must visibly disclose
that raw realized P&L proxy can reflect instrument, multiplier, and quantity
differences and therefore is not automatically a strategy-quality comparison.

Decile assignment is deterministic:

- Sort by realized P&L proxy, then research record id.
- Use `ceil(n * 0.10)` as the boundary count, with a minimum of one trade.
- Include ties at the boundary value and report the final cohort size.

Baseline comparisons:

- Winners versus losers using realized P&L proxy greater than zero.
- Long versus short using CRR trade identity side.

Comparability-controlled view:

- Standardized within-instrument realized P&L percentile.
- Include instruments only when the instrument sample size meets the declared
  minimum threshold.
- Show sample size and excluded instruments.
- Do not invent per-contract, multiplier, or quantity normalization where CRR
  does not provide reliable contract economics.

## 5. Required Deterministic Metrics

For each cohort, where available:

- trade count
- percentage of full population
- total, average, and median realized P&L proxy
- win rate where applicable
- average and median duration
- average and median MFE
- average and median MAE
- average and median giveback
- long/short mix
- instrument distribution
- strategy distribution
- session distribution
- regime distribution
- exit-reason distribution
- RA8 coverage count and percentage
- missing-field counts

Top-versus-bottom deltas are computed for numeric summary metrics only.
Within-instrument controlled metrics are reported separately from the global
portfolio-outcome comparison.

## 6. Data Quality And Coverage Display

Every artifact must show:

- CRR validation status.
- Required source readiness.
- RA8 source coverage warning.
- Missing optional metric counts.
- Exclusions and reasons.
- Guardrails.

Missing optional fields remain missing. No values may be inferred or fabricated.

## 7. Analytics Output Schema

The v1 analytics artifact contains:

- schema version
- generated timestamp
- source CRR path and fingerprint
- source CRR validation status
- population definition
- exclusions
- cohort definitions
- metric definitions
- cohort metrics
- cohort deltas
- comparability-controlled within-instrument analysis
- distributions
- trade drill-down rows
- validation report
- deterministic fingerprint
- guardrails

## 8. Minimum Presentation Screens

The first presentation is a static HTML artifact generated from the prepared analytics JSON.

Screens:

1. Overview: population, coverage, top/bottom summaries, readiness, warnings.
2. Cohort Comparison: top/bottom, winners/losers, long/short tables.
3. Distributions: P&L, MFE, MAE, duration, giveback.
4. Trade Drill-Down: selected CRR row details, anchors, attribution, context, path status, provenance.

## 9. Drill-Down Behavior

Trade drill-down is read-only. It displays precomputed rows from the analytics artifact:

- identity
- entry and exit anchors
- outcome summary
- context validity summary
- attribution status
- RA7 path status
- RA8 source coverage status
- source provenance references
- explicit missing fields

## 10. Refresh Model

The explorer is refreshed by rerunning its offline CLI after CRR generation.

It does not invoke broker, runtime, Managed Exit, Guardian, Safe-State, readiness, reconciliation, or strategy code.

## 11. Hosting Recommendation

Initial development should use generated static artifacts only.

Recommended future deployment is a standalone internal read-only service on Atlas behind Caddy, consuming precomputed artifacts. The trading runtime must not depend on this presentation layer.

## 12. Security And Read-Only Boundaries

The explorer has no broker authority, runtime authority, strategy authority, research promotion authority, or trading-gate authority.

It reports evidence. It does not decide, submit, cancel, flatten, relabel lifecycle records, or change runtime state.

## 13. Tests

Focused tests must cover:

- deterministic cohort assignment
- top/bottom decile boundaries
- duplicate/tie handling
- missing optional-field handling
- RA8 not required
- RA8 coverage reporting
- metric calculations
- sample-size reporting
- exclusions reporting
- deterministic artifact fingerprints
- guardrails
- presentation reads prepared artifacts only
- prohibited broker/runtime imports and mutation vocabulary
- no recommendation or trading-gate outputs

## 14. Incremental Phases

Phase 1:

- Analytics model and artifact generation from CRR.
- Markdown and JSON reports.
- Validation report.

Phase 2:

- Static HTML presentation from prepared analytics artifacts.
- Smoke-testable local preview as a file.

Phase 3:

- Future service wrapper only if needed, using the same prepared artifacts.

## 15. Explicit Non-Goals

- No generic dashboard.
- No AI-generated conclusions.
- No production recommendations.
- No strategy changes.
- No exit tuning.
- No tolerance joins.
- No RA8 historical recovery.
- No broker/runtime/service deployment.

## 16. Stop Conditions

Stop if:

- The first question cannot be answered deterministically from CRR.
- CRR semantics must change.
- Required metrics cannot be traced to CRR fields.
- A new broad UI framework is required.
- The presentation layer would need direct broker or runtime access.

## 17. Expected ROI

This slice creates the first product-facing research surface over CRR. It should reduce artifact spelunking, make best/worst trade evidence visible, and expose RA8 limitations without blocking the first useful analysis.
