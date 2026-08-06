# INV-005: NQ Winner Concentration And Peer-Cohort Analysis

Status: Draft

## Purpose

Do NQ's largest winners belong to repeatable peer cohorts, or are they isolated, fragile outcomes?

## Current Conclusion

`PARTIALLY_SUPPORTED` with `PARTIAL` confidence.

Active population view: `SOURCE_INTEGRITY_QUALIFIED`.

Source-confirmed anomalies surfaced for comparison: `5`.

Review-required records: `0`.

These findings are descriptive, non-causal, and carry no production authority.

## Generated Evidence

- `investigation_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-005/investigation.json`
- `investigation_md`: `outputs/track_b_execution_core/research_analytics/investigations/INV-005/investigation.md`
- `population_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-005/population.json`
- `evidence_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-005/evidence.json`
- `validation_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-005/validation_report.json`
- `validation_md`: `outputs/track_b_execution_core/research_analytics/investigations/INV-005/validation_report.md`
- `top_winner_cohorts_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-005/top_winner_cohorts.json`
- `peer_cohort_assignments_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-005/peer_cohort_assignments.json`
- `peer_cohort_metrics_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-005/peer_cohort_metrics.json`
- `recurring_traits_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-005/recurring_traits.json`
- `focal_trade_peer_comparisons_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-005/focal_trade_peer_comparisons.json`
- `breadth_fragility_classification_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-005/breadth_fragility_classification.json`
- `contradictory_evidence_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-005/contradictory_evidence.json`
- `nq_winner_peer_analysis_html`: `outputs/track_b_execution_core/research_analytics/investigations/INV-005/nq_winner_peer_analysis.html`

## Findings

- Overall NQ winner evidence is classified as POSITIVE_BUT_TAIL_DEPENDENT.
- 20 of 20 usable top-winner peer cohorts remain positive after removing the focal winner.
- Top winners most frequently share strategy PAPER_ACTIVE_EVIDENCE_NQ_GLOBEX_PARTICIPATION_LONG_V1 (15 of 20 top winners).

## Limitations

- Independent contract point-value provenance is missing in CRR v1.
- RA8/path evidence is optional and sparse, so peer analysis does not explain entry/exit path quality.
- Peer groups are formed from available CRR fields only; unavailable setup/context fields remain unavailable.
- Raw P&L proxy is descriptive and not normalized risk economics.

## Guardrails

- `diagnostic_only=true`
- `production_recommendation=false`
- `trading_gate=false`
