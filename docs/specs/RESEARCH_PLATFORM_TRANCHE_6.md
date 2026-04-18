# Research Platform Tranche 6

## Summary

This tranche shifts the platform posture from "ATP plus one proof" toward "shared research infrastructure with multiple tenants."

The main changes are:

- cross-family research analytics are now served through one shared contract
- approved quant is a fuller platform tenant, not only a starter review script
- the application reads combined research analytics instead of assuming ATP is the default analytics root
- published research analytics can surface as read-only strategy-analysis lanes
- approved quant platform review now carries explicit hashed review-config lineage

## What Changed

### 1. Multi-family analytics contract

The shared analytics layer now publishes and serves a family-aware contract with:

- `strategy_key` as the app-facing cross-family selection key
- `strategy_family`
- `family_label`
- `strategy_label`

The combined payload supports:

- one family
- many families
- cross-family aggregation without pretending family-local ids are globally unique

### 2. Dashboard/app bridge

The dashboard no longer reads only the ATP analytics root.

It now consumes:

- `outputs/research_platform/analytics/atp_companion`
- `outputs/research_platform/analytics/approved_quant`

through one combined backend payload and one `/api/research-analytics/...` path.

The renderer now uses family-aware analytics selection keys and labels, so approved quant and ATP can coexist without silent id collisions.

### 3. Strategy analysis

Published research analytics can now appear in `strategy_analysis` as a read-only lane type:

- `research_analytics`

This gives non-ATP tenants a path into the existing strategy-analysis surfaces without inventing replay-only compatibility logic.

### 4. Approved quant lineage

`approved_quant_platform_review` now carries a structured hashed review config covering:

- required timeframes
- execution model
- registry publishing
- analytics publishing

This improves reproducibility and run identity without forcing approved quant into ATP config shapes.

## Runtime Notes

### ATP

Warm ATP historical analysis remains effectively solved:

- bounded optimized warm wall time remains sub-second when source inventory is hot

### Approved quant

Approved quant uses the same shared path:

- source discovery
- generic trade-scope bundles
- registry
- analytics publishing
- app-serving contract

On the bounded windows used in this tranche, approved quant still produced zero trades, so the platform proof is structural and lineage-driven, not a rich trade-content proof yet.

### Cold inventory recommendation

The next cold-path gain is unlikely to come from more Python-side caching.

The current evidence still says the remaining cold cost is mostly the SQLite grouped coverage query itself. If cold refresh becomes important again, the next real lever is a durable coverage index/materialization strategy, not more app-level caching layers.

## Platform Boundary

Now validated beyond ATP:

- `source_context`
- `scope_bundles`
- `registry`
- `analytics`
- shared config hashing helpers

Still tenant-specific:

- ATP substrate and ATP outcome engine
- approved quant feature/signal/exit semantics
- family-local strategy definitions and evaluation logic

## Next Family Recommendation

The cleanest next migration target after approved quant is still the warehouse historical evaluator path.

Why:

- it already has explicit dataset contracts
- it already thinks in durable materialized layers
- it benefits from the shared source inventory, registry, analytics, and app-facing serving contract
- migrating it would validate the platform against a warehouse-first tenant, not another ATP-adjacent flow
