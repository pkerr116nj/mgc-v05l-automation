# London-Open Post-04:00 Transition Shadow Observer

`gc_mgc_london_open_post_04_transition_shadow_v1` is a diagnostic-only paper-shadow observer for the GC/MGC London-open post-04:00 transition candidate.

It is not a runtime lane, not route-capable, and not live-money eligible. It does not change canonical session labels and does not widen global London-open policy.

## Eligibility

- Observed session label must remain `LONDON_OPEN`.
- Completed bar timestamp must be strictly after `04:00` and no later than `04:10` America/New_York.
- Exact `04:00` boundary rows are rejected.
- Instrument must be `GC` or `MGC`.
- Direction is long-only.
- Range regime must be `range_normal` or `range_expanded`.
- `range_compressed` is rejected.
- Required market-data provenance must be `DATABENTO_REALTIME_PHASE1`.
- Phase-1 artifact must be present and fresh.
- Exact structural predicate fields from the source breakout/retest/hold rule must be present and pass.

## Output

The observer writes only shadow artifacts:

- `latest_state.json`
- `shadow_diagnostic_history.jsonl`

Payloads are explicitly non-authoritative and include:

- `order_intent_created: false`
- `route_attempted: false`
- `broker_state_mutated: false`
- `lifecycle_mutated: false`
- `route_capable: false`
- `submit_capable: false`
- `live_money_eligible: false`

## Non-Goals

- No runtime activation.
- No Track B activation.
- No broker, order, route, or lifecycle mutation.
- No canonical session-label change.
- No global session widening.
- No route-capable paper lane.
- No migration of additional strategy families.
