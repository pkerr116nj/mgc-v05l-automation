# OPERATOR_RUNBOOK_PILOT_V1

## Lane Scope

This runbook governs a narrow operator-controlled live pilot lane.

It does not define:
- legacy benchmark reproduction rules
- research platform defaults
- paper-runtime strategy semantics

Operational truth in this runbook is production-safety-oriented and intentionally narrower than research or benchmark analysis surfaces.

## Pre-Entry Checklist

- Confirm `Live Manual Pilot Runbook` shows pilot readiness as ready
- Confirm the symbol is whitelisted
- Confirm qty is `1`
- Confirm route is `BUY_TO_OPEN`
- Confirm broker/account/auth health are green
- Confirm reconciliation is `CLEAR`
- Confirm no same-symbol unresolved broker/manual order is active
- Confirm regular US market hours are open

## Submit Expectations

- Submit path: `Review / Confirm / Send`
- Expected lifecycle:
  - `submit_requested`
  - `ACKNOWLEDGED`
  - `WORKING` or `FILLED`
- A broker order id should appear after ack
- State should advance only on actual broker truth

## Post-Fill Checks

- Confirm lifecycle is `FILLED`
- Confirm broker/manual position shows the expected live position
- Confirm no duplicate submit was created
- Confirm refresh stays passive

## Close Checklist

- Use `Set SELL_TO_CLOSE Ticket`
- Confirm selected broker position is `LONG 1`
- Confirm route is `SELL_TO_CLOSE`
- Expected lifecycle:
  - `submit_requested`
  - `ACKNOWLEDGED`
  - `FILLED`
  - flat confirmed
- After fill, confirm:
  - broker/manual state is flat
  - reconciliation is `CLEAR`
  - no same-symbol open broker/manual order remains

## What RECONCILING Means

- Broker/manual truth is not strong enough yet to classify the order or position safely
- Stop and do nothing until the mismatch is resolved or classified
- Do not send another live order for the same symbol while this is active

## What REJECTED Means

- Schwab or broker truth produced a terminal non-fill state
- The order did not become an active filled position
- Review the broker/order details before retrying

## When To Stop And Do Nothing

- Reconciliation is not `CLEAR`
- Broker/auth/account/orders freshness is not healthy
- A same-symbol order is unresolved
- Broker truth is missing, conflicting, or stale
- The route falls outside the narrow pilot scope

## How To Confirm Passive Refresh / Restart Behavior

- Refresh the operator surface after submit/fill/close
- Confirm no extra `submit_requested` event was created
- Confirm lifecycle and position state restore correctly from persisted truth
- Confirm passive refresh/restart proof remains held in the pilot cycle summary
