# IBKR Runtime Route-Miss Fix

- classification: `IBKR_ROUTE_MISSING_BUG_FIXED`
- historical 08:29 ET fills remain local-only route-miss rows and are not reclassified as broker-path performance
- future submit-capable entry and exit signals now use the IBKR bridge runtime broker path
- bridge-blocked events now fail closed as `BLOCKED_NOT_SENT_TO_BROKER` instead of creating `paper-*` fills
- internal-only lanes still create local fills, but remain explicitly labeled internal simulation

## Focus Lanes
- `es_1x_ny_early_core__us_early_long`
- `mes_1x_ny_early_core__us_early_long`
- `nq_1x_ny_early_core__us_early_long`
- `mnq_1x_ny_early_core__us_early_long`

## Root Cause
- runtime builder: `_build_probationary_paper_lanes`
- historical path: submit-capable lanes were still constructed with ExecutionEngine(broker=PaperBroker())
- local fill materializer: StrategyEngine._apply_due_replay_fills() only runs for PaperBroker lanes, which created the paper-* fills
- runtime gap: inventory/adapter status had been ported, but the live probationary paper runtime dispatcher had not been switched to an IBKR bridge-aware broker

## Proof
- focused runtime route-fix slice: `8 passed`
- submit-capable entries route to the bridge broker and do not create local fills first
- submit-capable exits route to the bridge broker and do not create local fills first
- bridge-blocked submits create no fake local fills
- internal-only lanes retain explicit local-only behavior
