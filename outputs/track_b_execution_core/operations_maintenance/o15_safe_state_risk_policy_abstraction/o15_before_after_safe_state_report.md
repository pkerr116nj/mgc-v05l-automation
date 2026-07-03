# O15 Before/After Safe-State Report

## Before

O13/O14 found:

- `max_orders_per_runtime_generation_id=4` was a legacy hardcoded fallback.
- `orders_per_runtime_generation_id` was semantically overloaded.
- The counter behaved like exposure/order-state accounting rather than a clean broker mutation event counter.
- Runtime profile/roster evidence existed but was not surfaced in Safe-State policy provenance.

## After

Safe-State now emits:

- explicit risk policy metadata;
- active runtime profile attribution;
- runtime roster symbols;
- runtime roster lane count;
- separate counters for active exposure, open broker orders, and broker mutation events;
- deprecated compatibility marker for `orders_per_runtime_generation_id`.

## Latest Safe-State Refresh

- generated_at: `2026-07-03T15:40:21.333018+00:00`
- classification: `SAFE_STATE_NORMAL`
- policy_source: `legacy_config_fallback`
- profile_id: `mnq_mes_full_session_active_evidence`
- runtime_roster_lane_count: `71`
- runtime_roster_symbols: `ES, GC, MES, MGC, MNQ, NQ, ZB, ZF, ZN, ZT`
- active_managed_exposure_count: `0`
- open_broker_order_count: `0`
- broker_mutation_events_per_runtime_generation: `0`
- tripped_limits: `[]`

## Latest Operational Certification

- classification: `PLATFORM_NOT_CERTIFIED`
- Safe-State checks: `PASS`
- Managed Exit: `PASS`
- Runtime: `WARN`
- Broker/open-order/Guardian freshness: `FAIL`

The Safe-State risk policy abstraction repaired the limit semantics and provenance. The remaining certification failure is a separate freshness issue in broker/open-order/Guardian artifacts.
