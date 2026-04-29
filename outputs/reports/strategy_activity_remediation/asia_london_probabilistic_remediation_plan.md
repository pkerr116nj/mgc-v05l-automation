# Asia-London Probabilistic Remediation Plan

## Goal

Turn Asia/London from an over-gated strict A+ filter into a research-backed score model that can decide whether B+ participation is viable for paper evaluation.

## Principles

- do not change live or paper execution logic in this pass
- do not force trades
- do not optimize to one threshold
- keep the score model simple and explainable

## Proposed research model

### Score features

Start with features already visible in the archive:
- entry family quality
- fallback timing quality
- contextual fallback branch presence
- setup close location
- setup VWAP displacement
- setup return points
- setup range points
- side choice
- volatility-floor context

### Bucket structure

- `A+`
  - current strict gate / strongest confidence
- `A`
  - near-perfect archival surface such as `LONG__segment_forced_long_v6_contextual_fallback__base`
- `B+`
  - strong but not perfect reclaim-style surfaces such as `LONG__segment_forced_long_v5_dip_reclaim_or_bar8__base`
- `B`
  - weaker but still positive simplified surfaces
- `Rejected`
  - insufficient evidence or clearly inferior proxies

### Promotion standard for B+

B+ can be considered viable if:
- trade frequency is sufficient for paper evaluation
- expectancy remains positive after realistic embedded costs
- drawdown and tail loss remain acceptable
- results are not concentrated in one small segment
- the score model remains simple enough to explain

## Immediate next research tasks

1. Persist raw live predicate traces for every Asia/London bar.
2. Reconstruct exact pass/fail lineage for the current strict gate.
3. Build a bar-level near-miss table against the strict live gate.
4. Compare score buckets on dev vs holdout windows before any runtime change.

## Current recommendation

Keep Asia/London in `PROBABILISTIC_REMEDIATION`, not `NOT_READY` and not `REJECT`.
