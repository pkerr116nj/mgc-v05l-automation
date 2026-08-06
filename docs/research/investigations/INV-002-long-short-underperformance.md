# INV-002: Long Short Underperformance Control

Status: Draft

## Purpose

Does long-side underperformance persist after controlling for instrument, session, strategy, and time period?

## Current Conclusion

`INCONCLUSIVE` with `PARTIAL` confidence.

Active population view: `SOURCE_INTEGRITY_QUALIFIED`.

Source-confirmed anomalies surfaced for comparison: `5`.

Review-required records: `0`.

These findings are descriptive, non-causal, and carry no production authority.

## Generated Evidence

- `investigation_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-002/investigation.json`
- `investigation_md`: `outputs/track_b_execution_core/research_analytics/investigations/INV-002/investigation.md`
- `population_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-002/population.json`
- `evidence_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-002/evidence.json`
- `validation_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-002/validation_report.json`
- `validation_md`: `outputs/track_b_execution_core/research_analytics/investigations/INV-002/validation_report.md`

## Findings

- Controlled cells are mixed: 6 long-worse cells and 11 short-worse cells.

## Limitations

- Sparse cells are excluded rather than pooled.
- Raw P&L proxy is not normalized by contract economics.
- Side may be confounded by strategy, instrument, and time period.

## Guardrails

- `diagnostic_only=true`
- `production_recommendation=false`
- `trading_gate=false`
