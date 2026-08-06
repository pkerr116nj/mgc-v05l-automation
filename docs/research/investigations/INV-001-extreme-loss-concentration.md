# INV-001: Extreme Loss Concentration

Status: Draft

## Purpose

What explains the unusually large bottom-decile losses?

## Current Conclusion

`PARTIALLY_SUPPORTED` with `PARTIAL` confidence.

Active population view: `SOURCE_INTEGRITY_QUALIFIED`.

Source-confirmed anomalies surfaced for comparison: `5`.

Review-required records: `0`.

These findings are descriptive, non-causal, and carry no production authority.

## Generated Evidence

- `investigation_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/investigation.json`
- `investigation_md`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/investigation.md`
- `population_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/population.json`
- `evidence_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/evidence.json`
- `validation_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/validation_report.json`
- `validation_md`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/validation_report.md`
- `extreme_loss_inventory_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/extreme_loss_inventory.json`
- `loss_concentration_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/loss_concentration.json`
- `loss_classification_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/loss_classification.json`
- `population_sensitivity_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/population_sensitivity.json`
- `loss_attribution_html`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/loss_attribution.html`
- `extreme_trade_pnl_reconciliation_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/extreme_trade_pnl_reconciliation.json`
- `extreme_trade_execution_lineage_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/extreme_trade_execution_lineage.json`
- `extreme_trade_classification_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/extreme_trade_classification.json`
- `extreme_trade_population_impact_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/extreme_trade_population_impact.json`
- `extreme_trade_forensic_review_html`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/extreme_trade_forensic_review.html`
- `forensic_validation_report_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/forensic_validation_report.json`
- `forensic_validation_report_md`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/forensic_validation_report.md`
- `anomaly_source_trace_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/anomaly_source_trace.json`
- `anomaly_root_cause_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/anomaly_root_cause.json`
- `anomaly_repair_plan_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/anomaly_repair_plan.json`
- `anomaly_before_after_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/anomaly_before_after.json`
- `anomaly_root_cause_review_html`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/anomaly_root_cause_review.html`

## Findings

- Aggregate negative results are materially affected by extreme losses: bottom 1% total P&L proxy -248946.48, bottom 5% total P&L proxy -685277.32.
- Supported exclusions materially change the aggregate result.
- Extreme-loss forensic audit found 0 source-backed price-scale anomalies, 0 duplicate/reused execution-evidence anomalies, and 20 unresolved records among the worst 20; no records were removed from the population.

## Limitations

- Raw P&L comparability can reflect instrument, multiplier, and quantity differences.
- RA8 path evidence is partial.
- Extreme-loss classification remains insufficient where source context is absent.
- Contract point values are not independently sourced in CRR v1 and are treated as unresolved when absent.

## Guardrails

- `diagnostic_only=true`
- `production_recommendation=false`
- `trading_gate=false`
