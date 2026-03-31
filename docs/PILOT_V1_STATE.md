# PILOT_V1_STATE

## Governance Note

This document records the exact narrow live-pilot scope that has been proven so far.

It is not:
- a statement of research defaults
- a statement of legacy benchmark semantics
- a declaration that the platform is production-enabled beyond this pilot slice

## What Is Proven Live

- Real manual-live `BUY_TO_OPEN` on Schwab completed submit, ack, and fill.
- Real manual-live `SELL_TO_CLOSE` on Schwab completed submit, ack, and fill.
- Broker/manual state returned to flat after the close.
- Reconciliation and readiness returned to `CLEAR`.
- Refresh/restart stayed passive with no autonomous follow-on submit.
- No autonomous live strategy submission is enabled.

## Exact Narrow Scope

- Asset class: `STOCK`
- Order type: `LIMIT`
- Quantity: `1` share only
- Symbol universe: whitelist only
- Time in force: `DAY`
- Session: `NORMAL`
- Hours: regular US market hours only
- Open route: `MANUAL_LIVE_PILOT + BUY`
- Close route: `FLATTEN + SELL`
- `clientOrderId` omitted only for the exact proven narrow stock pilot shape

## Exact Allowed Route

- `BUY_TO_OPEN`:
  - `intent_type = MANUAL_LIVE_PILOT`
  - `side = BUY`
  - `asset_class = STOCK`
  - `order_type = LIMIT`
  - `quantity = 1`
  - whitelisted symbol only
- `SELL_TO_CLOSE`:
  - `intent_type = FLATTEN`
  - `side = SELL`
  - existing broker `LONG 1` position required
  - `asset_class = STOCK`
  - `order_type = LIMIT`
  - `quantity = 1`

## Exact Required Gates

- Local operator auth ready
- Live Schwab account selected
- Broker reachable
- Orders / balances / positions freshness in bounds
- Reconciliation `CLEAR`
- No unresolved same-symbol broker/manual order ambiguity
- Route must remain inside the narrow scope above

## Exact Operator UI Path

- `Positions > Manual Order Ticket`
- Use `Set BUY_TO_OPEN Ticket` for the proven open route
- Use `Set SELL_TO_CLOSE Ticket` for the proven close route
- Send through `Review / Confirm / Send`
- Inspect compact state in:
  - `Live Manual Pilot Runbook`
  - `Last Completed Live Cycle`

## Exact Known Non-Goals

- No autonomous live strategy entries or exits
- No multi-share live pilot trading
- No multi-symbol widening beyond the whitelist
- No new order types
- No guessed undocumented Schwab payload broadening

## Exact Remaining Limitations

- Pilot remains intentionally narrow and operator-driven
- Unsupported order types remain preview-only or blocked
- Any unresolved broker/order/position ambiguity still fail-closes into reconciliation/review
- Temporary proof scripts may remain in `/tmp` for engineering replay/debug only; they are not the operator workflow
