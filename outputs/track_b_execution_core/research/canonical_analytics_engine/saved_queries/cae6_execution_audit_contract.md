# CAE6 Execution Audit Contract

- Schema version: `cae6_canonical_analytics_execution_record_v1`
- Execution logs are diagnostic JSONL records.
- Fingerprints exclude generation timestamps so equivalent query/result payloads are reproducible.
- Execution records preserve input artifact refs, input hashes, dimensions, metrics, filters, validity rules, sample classes, and guardrails.
- Guardrails remain `diagnostic_only=true`, `production_recommendation=false`, `trading_gate=false`.
