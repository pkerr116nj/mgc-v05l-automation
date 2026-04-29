# Strategy Activity Instrumentation

This pass adds instrumentation only. No strategy rules were changed, no orders were placed, and live trading was not enabled.

## Asia/London

Live observational tracing is now attached to `AsiaLondonParticipationStrategyEngine._evaluate_signals` in `/Users/patrick/Documents/MGC-v05l-automation/src/mgc_v05l/app/asia_london_participation_runtime.py`.

For each evaluated Asia/London decision bar, the runtime now persists:

- timestamp
- lane id
- instrument
- session label
- predicate pass/fail map
- predicate values where available
- strict gate pass/fail
- near-miss score
- passed predicate count
- observational score bucket: `A+`, `A`, `B+`, `B`, or `rejected`
- whether the current strict gate would trade
- whether the research bucket would count as a candidate

These traces are observational only. They do not change live or paper execution decisions.

## Midday Route Proof

Post-fix route proof tracing is now attached to `_IbkrPaperBridgeRuntimeBroker.submit_order` in `/Users/patrick/Documents/MGC-v05l-automation/src/mgc_v05l/app/probationary_runtime.py`.

For `__us_midday__` submit-capable lanes, the broker wrapper now persists:

- route-attempt context
- caller metadata
- bridge preflight rows
- bridge invocation classification
- exact real gate blocker when blocked
- broker order id and broker-truth snapshots when available

Current baseline status remains:

- `MIDDAY_ROUTE_PROOF_WAITING_FOR_SIGNAL`

That means the instrumentation is live, but the next real actionable midday signal is still required to populate fresh bridge invocation and broker-truth evidence.

## Verification

Focused instrumentation slice:

- `3 passed`
- `114 deselected`

Covered checks:

- Asia/London predicate and near-miss trace persistence
- Midday blocked route-proof trace persistence
- Midday successful bridge-invoked trace persistence
