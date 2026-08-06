# Investigation Record Contract

Status: Draft

## Purpose

Investigation Records preserve reproducible research reasoning over prepared
Track B research artifacts.

They do not grant production authority. They do not alter broker, runtime,
Managed Exit, Guardian, Safe-State, readiness, reconciliation, strategy, or
trading-gate behavior.

## Record Shape

Each record contains:

- `investigation_id`
- `title`
- `status`
- `question`
- `rationale`
- `source_artifacts`
- `source_fingerprints`
- `generated_at`
- `population`
- `population_definition`
- `filters_and_exclusions`
- `methodology`
- `metrics`
- `evidence`
- `contradictory_evidence`
- `findings`
- `limitations`
- `confidence`
- `conclusion_status`
- `unresolved_questions`
- `follow_up_candidates`
- `guardrails`

Guardrails are always:

- `diagnostic_only=true`
- `production_recommendation=false`
- `trading_gate=false`

## Conclusion Status

Allowed conclusion statuses:

- `SUPPORTED`
- `PARTIALLY_SUPPORTED`
- `UNSUPPORTED`
- `INCONCLUSIVE`
- `SUPERSEDED`

Conclusion status is research status only. It is not production approval,
strategy authority, or trading authority.

## Artifact Locations

Generated evidence artifacts remain under:

`outputs/track_b_execution_core/research_analytics/investigations/`

Durable investigation summaries live under:

`docs/research/investigations/`

Generated evidence may be regenerated. Durable summaries preserve the research
question, result status, artifact references, limitations, and follow-up
candidates.

## Evidence Rules

- Missing evidence remains missing.
- Descriptive findings do not establish causality.
- Source fingerprints must be recorded when available.
- Filters, exclusions, and sample thresholds must be explicit.
- No investigation may infer contract economics or path evidence from absent
  source fields.
- RA8 coverage may be optional when the investigation question does not require
  path evidence, but the limitation must remain visible.

## Authority Boundary

Investigation Records report research evidence. They do not decide, submit,
cancel, flatten, relabel lifecycle state, change readiness, create gates, or
recommend production behavior.
