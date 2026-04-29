# Strategy Activity Remediation Summary

Overall classification: `STRATEGY_ACTIVITY_REMEDIATION_PARTIAL`

## Why partial

This pass is strong enough to prioritize the next 1-2 week paper/live-readiness effort, but not strong enough to fully close the Asia/London research question.

What is now clear:
- the near-term broker-path paper focus should stay on the already active early-session families
- the midday families are no longer classed as caller-policy failures; they are post-fix revalidation candidates waiting for a fresh real signal
- Asia/London should move into probabilistic remediation, not kill/demotion

What remains incomplete:
- exact bar-level predicate pass/fail frequencies for the current live Asia/London gate are not persisted
- exact near-miss lineage versus the current strict live gate is unavailable in the current archive
- fresh post-fix midday routed-entry proof has not occurred yet

## Priority buckets

### `PAPER_ACTIVE_CANDIDATE`
- `index_ny_early_core_us_early`
- `gc_all_lanes_us_early`
- `gold_forced_session_baseline_v2` london-early lane

### `POST_FIX_REVALIDATION`
- `index_ny_early_core_us_midday`
- `gc_all_lanes_us_midday`

### `PROBABILISTIC_REMEDIATION`
- `asia_london_participation`

### `RESEARCH_ONLY`
- `gc_all_lanes_asia_early`
- `index_ny_early_core_us_late`

## Key conclusion

Do not spread near-term live-readiness effort evenly across every family.

The fastest path to broker-path evidence is:
1. keep harvesting early-session paper evidence
2. capture the next real midday signal post-fix
3. treat Asia/London as a score-model research program, not a dead strategy
