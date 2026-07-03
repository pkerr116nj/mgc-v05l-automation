# O15 Safe-State Policy Contract

## Purpose

Safe-State limit evaluation must distinguish active managed exposure, open broker orders, and broker mutation event history. It must not infer exposure limits from research-only or context-only symbol lists.

## Policy Resolution Order

1. Explicit Safe-State risk policy config:
   - `config/track_b_safe_state_risk_policy.json`
2. Embedded policy inside active runtime profile artifact:
   - `outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json`
3. Conservative legacy config fallback:
   - dataclass defaults on `TrackBRuntimeSafeStateEnvelopeConfig`

Fallback is allowed for tests/minimal profiles but is explicitly marked with `legacy_fallback_used=true`.

## Canonical Policy Fields

- `policy_source`
- `profile_id`
- `runtime_roster_symbols`
- `runtime_roster_lane_count`
- `limit_policy_version`
- `legacy_fallback_used`

## Canonical Limits

- `max_active_managed_exposures`
- `max_open_broker_orders`
- `max_broker_mutation_events_per_runtime_generation`
- `max_submits_per_symbol_per_window`
- `max_broker_mutation_attempts_per_window`
- `max_failed_broker_mutations_per_window`
- `max_duplicate_intent_attempts`
- `max_managed_open_positions_per_strategy_lane`
- `max_consecutive_lifecycle_reconciliation_disagreements`
- `max_recovery_attempts_per_runtime_generation`

## Counter Semantics

- `active_managed_exposure_count`: current active managed positions/exposures.
- `open_broker_order_count`: current active broker/open-order truth rows.
- `broker_mutation_events_per_runtime_generation`: active managed/open-order event rows attributed to or falling back for the current runtime generation.
- `orders_per_runtime_generation_id`: deprecated compatibility mirror for older readers.

## Safety Behavior

- Exceeding `max_active_managed_exposures` hard-blocks through `SAFE_STATE_POSITION_LIMIT_HIT`.
- Exceeding open broker order or broker mutation event limits hard-blocks through `SAFE_STATE_BROKER_MUTATION_LIMIT_HIT`.
- Broker/order/lifecycle hard stops remain separate from research/context universes.
- No policy source grants broker mutation authority.

## Non-Authority Sources

These are not Safe-State exposure authorities:

- research data universe
- live market-data/context symbol lists
- CMC context providers
- CRFD/GRE research providers
