# CAE6 Result Provenance Report

- Query id: `expectancy_by_strategy`
- Saved query hash: `e93a9074b71271d864bf12778709d8405e1dfaaea15f9e050fde6b401c92b1b2`
- Engine version: `track_b_canonical_analytics_engine_v1`

## Inputs

- `canonical_trade_outcomes`: `outputs/track_b_execution_core/trade_outcome_layer/canonical_trade_outcomes.jsonl`
  - sha256: `dd5b4aeb295a06b9b1703bc244724ccd7092278d59481b45ab6eab112a903287`
- `trade_outcome_enrichment`: `outputs/track_b_execution_core/trade_outcome_enrichment/canonical_trade_outcome_enrichment.jsonl`
  - sha256: `1ec533311a0c4e4542297315d3cac1e747ecc21ed34428dfbb7a4aee006e3047`

## Query Shape

- Dimensions: `strategy`
- Metrics: `trade_count, win_rate, expectancy_proxy, sample_class`
- Context validity rules: ``
