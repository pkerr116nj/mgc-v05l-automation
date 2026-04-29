# Asia-London Predicate Contribution Report

Asia/London should not be classified as dead simply because the current strict live gate produced zero candidates. The archived validation layer shows scientifically judgeable candidate surfaces with real trade counts, positive expectancy, and probation-level evidence.

## Revised interpretation

- current strict gate is over-narrow
- zero A+ candidates in the live probationary stack does not imply zero opportunity
- the right next step is probabilistic remediation and score-model research, not demotion or kill

## What the archive can prove

The archive preserves:
- full trade streams for multiple Asia/London candidate surfaces
- validation metrics
- simplification comparisons
- evidence that removing some hard gating improves robustness without destroying expectancy

The archive does **not** preserve:
- boolean pass/fail traces for every live predicate on every bar
- exact per-bar near-miss lineage against the current strict live A+ gate

So this report is a proxy contribution analysis, not a complete raw-predicate replay.

## Strongest contribution signals

### ATP-style bias gate

Evidence:
- simplification pass says long `v5 dip reclaim / bar8 / base` improved train-bias and selection-bias behavior when the ATP-style gating/contextual branching was removed
- this is direct evidence that the current hard gate is likely too strict

Interpretation:
- good candidate to downgrade from hard gate to score contributor

### Contextual fallback logic

Evidence:
- tuned `LONG__segment_forced_long_v6_contextual_fallback__base` has the strongest expectancy and PF in the archive
- but its parameter-surface status is weaker than ideal

Interpretation:
- useful signal feature, but too complex to be the only admissible hard gate
- better as a positive score contribution than a binary veto

### Reclaim vs fixed breakout/breakdown

Evidence:
- `LONG__segment_forced_long_v5_dip_reclaim_or_bar8__base` outperforms the simpler `long_v4_breakout_or_bar7` proxy
- `SHORT__segment_forced_short_v2_reclaim_fail_or_bar7__base` outperforms the simpler `short_v4_breakdown_or_bar7` proxy

Interpretation:
- reclaim-style features add value
- missing a perfect reclaim should not necessarily zero the trade if other quality features remain favorable

### Vol-floor strictness

Evidence:
- the ES-only `vol_floor_1p25` live strict branch remains insufficient evidence

Interpretation:
- this should not stay as the sole hard admissibility standard for the whole family

## Bottom line

The best current reading is:
- `ASIA_LONDON_STRICT_GATE_TOO_NARROW`
- `ASIA_LONDON_SCORE_MODEL_PROMISING`

That is strong enough to justify a non-execution B+/score-model research pass, but not strong enough to change live paper rules in this pass.
