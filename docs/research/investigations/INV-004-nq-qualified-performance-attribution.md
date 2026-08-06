# INV-004: NQ Qualified Performance Attribution

Status: Draft

## Purpose

What explains NQ's approximately +907,653 qualified P&L contribution?

## Current Conclusion

`INCONCLUSIVE` with `PARTIAL` confidence.

Active population view: `SOURCE_INTEGRITY_QUALIFIED`.

Source-confirmed anomalies surfaced for comparison: `5`.

Review-required records: `0`.

These findings are descriptive, non-causal, and carry no production authority.

## Generated Evidence

- `investigation_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-004/investigation.json`
- `investigation_md`: `outputs/track_b_execution_core/research_analytics/investigations/INV-004/investigation.md`
- `population_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-004/population.json`
- `evidence_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-004/evidence.json`
- `validation_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-004/validation_report.json`
- `validation_md`: `outputs/track_b_execution_core/research_analytics/investigations/INV-004/validation_report.md`
- `performance_summary_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-004/performance_summary.json`
- `concentration_by_dimension_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-004/concentration_by_dimension.json`
- `tail_sensitivity_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-004/tail_sensitivity.json`
- `rolling_windows_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-004/rolling_windows.json`
- `controlled_comparisons_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-004/controlled_comparisons.json`
- `top_bottom_trades_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-004/top_bottom_trades.json`
- `contradictory_evidence_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-004/contradictory_evidence.json`
- `nq_performance_attribution_html`: `outputs/track_b_execution_core/research_analytics/investigations/INV-004/nq_performance_attribution.html`

## Findings

- NQ contributes 907653.32 qualified P&L proxy across 155 trades; breadth is classified as HIGHLY_CONCENTRATED.
- NQ does not remain positive after excluding top 10% winners: -95136.68 versus full total 907653.32.
- Controlled NQ comparisons produced 2 minimum-sample cells; sparse cells were excluded rather than pooled.

## Limitations

- Independent contract point-value provenance is missing in CRR v1.
- MFE, MAE, giveback, and full path evidence remain sparse, so exit-quality conclusions are limited.
- Concentration and period attribution are descriptive and do not establish causality.
- Raw P&L proxy is not a normalized risk or multiplier-adjusted measure.

## Guardrails

- `diagnostic_only=true`
- `production_recommendation=false`
- `trading_gate=false`
