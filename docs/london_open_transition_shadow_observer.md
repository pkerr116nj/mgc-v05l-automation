# London-Open Post-04:00 Transition Shadow Observer

`gc_mgc_london_open_post_04_transition_shadow_v1` is a diagnostic-only paper-shadow observer for the GC/MGC London-open post-04:00 transition candidate.

It is not a runtime lane, not route-capable, and not live-money eligible. It does not change canonical session labels and does not widen global London-open policy.

The manual integration path is read-only by default. It reads persisted Phase-1 candle artifacts from disk and never connects this candidate to active Track B routing or lifecycle paths.

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

## Manual Command

Read-only preflight/check mode, with no shadow artifact writes:

```bash
./.venv/bin/python -m mgc_v05l.app.london_open_transition_shadow_manual --check
```

Explicit one-shot shadow write mode:

```bash
./.venv/bin/python -m mgc_v05l.app.london_open_transition_shadow_manual --write-shadow
```

Default input artifact:

```text
outputs/track_b_execution_core/phase1_runtime_market_data/GC/5m/latest_runtime_candles.json
```

To inspect MGC instead, pass the artifact explicitly:

```bash
./.venv/bin/python -m mgc_v05l.app.london_open_transition_shadow_manual \
  --check \
  --artifact outputs/track_b_execution_core/phase1_runtime_market_data/MGC/5m/latest_runtime_candles.json
```

The preflight report includes the input path, freshness status, provenance status, readiness status, observed bar timestamp, accept/reject decision, reject reason, and non-routing flags. `--check` does not create or append shadow artifacts.

## Output

When explicitly run with `--write-shadow`, the observer writes only shadow artifacts:

- `latest_state.json`
- `shadow_diagnostic_history.jsonl`

Payloads are explicitly non-authoritative and include:

- `order_intent_created: false`
- `route_attempted: false`
- `broker_state_mutated: false`
- `lifecycle_mutated: false`
- `route_capable: false`
- `submit_capable: false`
- `observer_only: true`
- `live_money_eligible: false`
- `broker_mutation: false`
- `lifecycle_mutation: false`

## Non-Goals

- No runtime activation.
- No Track B activation.
- No broker, order, route, or lifecycle mutation.
- No canonical session-label change.
- No global session widening.
- No route-capable paper lane.
- No `OrderIntent` creation.
- No route attempts.
- No active runtime wiring.
- No migration of additional strategy families.
