# INV-001: Extreme Loss Concentration

Status: Draft

## Purpose

What explains the unusually large bottom-decile losses?

## Current Conclusion

`PARTIALLY_SUPPORTED` with `PARTIAL` confidence.

These findings are descriptive, non-causal, and carry no production authority.

## Generated Evidence

- `investigation_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/investigation.json`
- `investigation_md`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/investigation.md`
- `population_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/population.json`
- `evidence_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/evidence.json`
- `validation_json`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/validation_report.json`
- `validation_md`: `outputs/track_b_execution_core/research_analytics/investigations/INV-001/validation_report.md`

## Findings

- Aggregate negative results are materially affected by extreme losses: bottom 1% total P&L proxy -3212581.96, bottom 5% total P&L proxy -3692935.06.

## Limitations

- Raw P&L comparability can reflect instrument, multiplier, and quantity differences.
- RA8 path evidence is partial.
- Extreme-loss classification remains insufficient where source context is absent.

## Guardrails

- `diagnostic_only=true`
- `production_recommendation=false`
- `trading_gate=false`
