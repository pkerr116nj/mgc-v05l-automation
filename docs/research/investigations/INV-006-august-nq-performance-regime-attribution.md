# INV-006: August NQ Performance Regime Attribution

Status: Draft

## Purpose

Why were qualified NQ results and top winners concentrated in August 2026, especially week 32?

## Current Conclusion

`PARTIALLY_SUPPORTED` with `PARTIAL` confidence.

Active population view: `SOURCE_INTEGRITY_QUALIFIED`.

Source-confirmed anomalies surfaced for comparison: `5`.

Review-required records: `0`.

These findings are descriptive, non-causal, and carry no production authority.

## Generated Evidence

- `investigation_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-006/investigation.json`
- `investigation_md`: `outputs/track_b_execution_core/research_analytics/investigations/INV-006/investigation.md`
- `population_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-006/population.json`
- `evidence_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-006/evidence.json`
- `validation_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-006/validation_report.json`
- `validation_md`: `outputs/track_b_execution_core/research_analytics/investigations/INV-006/validation_report.md`
- `period_comparison_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-006/period_comparison.json`
- `composition_attribution_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-006/composition_attribution.json`
- `peer_period_comparisons_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-006/peer_period_comparisons.json`
- `tail_sensitivity_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-006/tail_sensitivity.json`
- `milestone_change_audit_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-006/milestone_change_audit.json`
- `context_coverage_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-006/context_coverage.json`
- `hypothesis_results_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-006/hypothesis_results.json`
- `contradictory_evidence_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-006/contradictory_evidence.json`
- `august_nq_attribution_html`: `outputs/track_b_execution_core/research_analytics/investigations/INV-006/august_nq_attribution.html`

## Findings

- August concentration is classified as LIKE_FOR_LIKE_PERFORMANCE_IMPROVEMENT.
- August contains 29 qualified NQ trades; week 32 contains 28; top-20 winners in August/week32 are 19/18.
- 6 of 6 supported like-for-like period cells have higher average P&L in the later period.

## Limitations

- Structured market-context coverage is incomplete and unavailable fields are not reconstructed.
- RA8/path evidence is optional and sparse, so period attribution cannot explain path mechanics.
- Raw P&L proxy is descriptive and not normalized risk economics.
- Milestone timing is evidence of temporal proximity only, not causality.

## Guardrails

- `diagnostic_only=true`
- `production_recommendation=false`
- `trading_gate=false`
