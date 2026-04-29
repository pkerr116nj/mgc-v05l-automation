# Post-Fix Midday Revalidation

Classification:
- `index_ny_early_core_us_midday`: `MIDDAY_ROUTE_FIXED_NO_FRESH_SIGNAL`
- `gc_all_lanes_us_midday`: `MIDDAY_ROUTE_FIXED_NO_FRESH_SIGNAL`

## What changed

The old midday suppressor was the bridge caller-policy guard:
- `BLOCKED_NOT_SENT_TO_BROKER: Paper strategy bridge rejected a non-manual caller path.`

That caller-path block has already been fixed. The current live lane artifacts now show:
- `route_destination=ibkr_paper_bridge_submit_capable`
- `submit_failure=null`
- reconciliation status `CLEAN`
- broker truth account health `HEALTHY`

## What is proven now

For both midday families, the runtime is no longer stuck at the old caller-policy layer.

Representative current lane evidence:
- `es_1x_ny_early_core__us_midday_long`
  - last processed bar: `2026-04-29 13:27 ET`
  - route destination: `ibkr_paper_bridge_submit_capable`
  - bridge proxy mode: `ES_SIGNAL_ROUTED_TO_MES_PHASE1`
  - submit failure: `null`
  - reconciliation: `CLEAN`
- `mnq_1x_ny_early_core__us_midday_long`
  - route destination: `ibkr_paper_bridge_submit_capable`
  - bridge proxy mode: `MNQ_SIGNAL_DIRECT_PHASE1`
  - submit failure: `null`
- `gc_1x_all_lanes__us_midday_short`
  - route destination: `ibkr_paper_bridge_submit_capable`
  - bridge proxy mode: `GC_SIGNAL_ROUTED_TO_MGC_PHASE1`
  - submit failure: `null`

## What is not proven yet

There was no fresh post-fix midday actionable entry during this pass.

So the correct conclusion is not “midday still blocked,” and not yet “fully proven broker-path routing on a new midday signal.” It is:
- route policy fix is in place
- current live lane state is healthy and submit-capable
- the next real midday actionable signal is needed to prove fresh routed intent and broker truth

## Operational meaning

Midday belongs in `POST_FIX_REVALIDATION`, not in rule relaxation.
