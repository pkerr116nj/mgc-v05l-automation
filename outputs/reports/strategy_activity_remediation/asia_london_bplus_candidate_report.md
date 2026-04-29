# Asia-London B+ Candidate Report

Classification: `ASIA_LONDON_SCORE_MODEL_PROMISING`

## Why this is not dead

The archived Asia/London candidate system already contains several scientifically judgeable variants:
- `LONG__segment_forced_long_v5_dip_reclaim_or_bar8__base`
- `SHORT__segment_forced_short_v2_reclaim_fail_or_bar7__base`
- `LONG__segment_forced_long_v6_contextual_fallback__base`

Those variants have:
- trade counts from `1190` to `2374`
- positive expectancy
- profit factors from `1.60` to `2.55`
- probation-level scientific validation, not outright rejection

## Why B+ is the right framing

The simplification study explicitly found:
- overall conclusion: `possible_but_needs_more_evidence`
- provisional survivor: `LONG__segment_forced_long_v5_dip_reclaim_or_bar8__base`

That is exactly the kind of evidence that supports a B+/score-model remediation path:
- active enough to evaluate
- still explainable
- not dependent on a single perfect A+ configuration

## Best current B+ proxy

`LONG__segment_forced_long_v5_dip_reclaim_or_bar8__base`
- trade count: `2374`
- average P&L: `30.67`
- win rate: `43.47%`
- profit factor: `1.81`
- average winner: `157.80`
- average loser: `-67.54`
- max drawdown: `6138.0`
- evidence status: scientifically judgeable / provisional survivor

## Why not promote immediately

The family still has real risks:
- train-bias concerns remain in the validation stack
- exact live-predicate pass/fail traces are not yet persisted
- current live strict gate and archived candidate-system surfaces are not one-to-one

So the right action is:
- keep Asia/London alive
- treat it as `PROBABILISTIC_REMEDIATION`
- build a non-execution score model and deeper predicate telemetry first
