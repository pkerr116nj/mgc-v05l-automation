# Paper/Live Readiness Simplification Plan

Classification: `READINESS_RULE_SIMPLIFICATION_PLAN_READY`

## Goal
Make the working IBKR paper stack understandable enough to run hands-off, without weakening live-money protections or deleting working code in this pass.

## What The Audit Found
- The system has a real working core, but it is buried under overlapping readiness layers:
  - startup attachment and launch allowance
  - supervised paper operability
  - operator-surface runtime readiness
  - Electron operational readiness
  - Electron trade authority triage
  - per-lane fireability
  - bridge preflight
- The biggest recent false-block themes were not strategy rules:
  - live authority bleeding into paper authority
  - WATCH exceptions counted as blocking runtime faults
  - stale runtime/snapshot state overriding healthy current runtime
  - submit-capable lanes silently falling back to `PaperBroker`
  - caller-policy rejecting approved paper-runtime callers before real preflight
  - route-ready vs actionable-now vs session-eligible being presented as if they were one concept

## Simplified Target Model
### PAPER
1. `paper_monitor_ready`
2. `data_ready`
3. `strategy_eligible`
4. `intent_generated`
5. `governance_allowed`
6. `exposure_allowed`
7. `broker_reconciled`
8. `bridge_submit_allowed`

### LIVE
1. all paper gates
2. `live_authority_ready`
3. `live_risk_allowed`
4. `live_account_allowed`

Everything else should be advisory, explanatory, or diagnostic.

## Recommended Gate Layers
### Keep as core hard gates
- Dashboard/API attached
- Paper runtime running and not halted
- Market/data freshness
- Strategy governance
- Exposure attribution and caps
- Broker/ledger reconciliation
- IBKR bridge caller/environment lock
- Live operator authority and live account gates

### Merge or rename
- `eligible_to_trade_count` should become a clearer broad paper-readiness metric, not a catch-all label for “ready now”
- `actionable_now_count` should remain strict and momentary
- `UNCLASSIFIED` phase label should be displayed as informational next to broad session, not as a blocker by implication
- `WATCH` and `REVIEW` should never share the same mental bucket as `BLOCKED`
- `runtime_recovery_state` and `fault_state` should be presented separately from lane readiness counts

### Deprecate from critical paper routing
- Silent `PaperBroker` fallback for submit-capable lanes
- Duplicate browser/Electron semantic drift where both re-express the same readiness differently
- Stale snapshot fallback overriding healthy live/attached runtime truth
- Legacy local-paper route ambiguity for supported broker-path families

## Proposed Ownership Model
- `operator_dashboard.py`
  - authoritative paper lane fireability, session logic, paper operability snapshot
- `operator_surface.py`
  - normalized compact runtime/readiness contract for downstream UIs
- `operatorTriage.ts`
  - live vs paper authority synthesis only; do not re-invent lane semantics here
- `operationalReadiness.ts`
  - attachment/startup/top-level app usability only
- `ibkr_paper_strategy_bridge.py`
  - final paper submit preflight and broker route lock
- `probationary_runtime.py`
  - strategy execution/runtime dispatch, but not alternate truth semantics

## Immediate Cleanup Priorities
1. Collapse paper-top-level decisions onto the paper model:
   - paper usable
   - paper runtime ready
   - paper bridge allowed
2. Leave live authority as a separate parallel contract.
3. Standardize lane operator taxonomy:
   - out of session
   - waiting for bar
   - no setup
   - actionable
   - blocked by gate
4. Remove silent legacy route fallback for all submit-capable supported lanes.
5. Treat snapshot fallback as evidence, not as equivalent to attached live truth.

## What To Keep Stable
- PAPER-only lock
- account/host/port lock
- monitor health gate
- governance and exposure caps
- reconciliation fail-closed behavior
- route-aware broker wrapper
- live authority separation

## Safe Sequencing
1. Freeze semantics:
   - one authoritative paper-readiness contract
   - one authoritative live-authority contract
2. De-duplicate labels and card names.
3. Remove legacy fallback path usage from supported broker-path lanes.
4. Only then consider deleting redundant code paths.

## Conclusion
The system does not need a rewrite. It needs a narrower contract:
- one paper launch/operability contract
- one live-authority contract
- one lane fireability model
- one bridge preflight model

That is enough to simplify operator understanding while preserving the working IBKR paper stack.
