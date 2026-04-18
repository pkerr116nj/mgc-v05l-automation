# PILOT_V1_STATE

## Governance Note

This document records historical proven pilot scope and distinguishes it from the current active live-test target.

It is not:
- a statement of research defaults
- a statement of legacy benchmark semantics
- a declaration that the platform is production-enabled beyond this pilot slice

## Historical Proven Live Scope

- Real manual-live `BUY_TO_OPEN` on Schwab completed submit, ack, and fill.
- Real manual-live `SELL_TO_CLOSE` on Schwab completed submit, ack, and fill.
- Broker/manual state returned to flat after the close.
- Reconciliation and readiness returned to `CLEAR`.
- Refresh/restart stayed passive with no autonomous follow-on submit.
- No autonomous live strategy submission is enabled.

The stock pilot proof below is historical record only. It is not the current active manual live-test lane.

## Historical Narrow Scope

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

## Historical Allowed Route

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

## Historical Required Gates

- Local operator auth ready
- Live Schwab account selected
- Broker reachable
- Orders / balances / positions freshness in bounds
- Reconciliation `CLEAR`
- No unresolved same-symbol broker/manual order ambiguity
- Route must remain inside the narrow scope above

## Historical Operator UI Path

- `Positions > Manual Order Ticket`
- Use `Set BUY_TO_OPEN Ticket` for the proven open route
- Use `Set SELL_TO_CLOSE Ticket` for the proven close route
- Send through `Review / Confirm / Send`
- Inspect compact state in:
- `Historical Stock Pilot Record`
- `Last Completed Live Cycle`

## Current Active Live-Test Lane

- Current active lane: narrow manual futures lane
- Asset class: `FUTURE`
- Symbol scope: whitelist-controlled valid futures symbols only
- Order type: `LIMIT`
- Quantity: `1` contract only
- Time in force: `DAY`
- Session: `NORMAL`
- Open route: `MANUAL_LIVE_FUTURES_PILOT + BUY`
- Close route: `FLATTEN + SELL`
- `clientOrderId` omission remains scoped only to the approved manual futures lane
- `ANYTIME` remains disabled/deferred

Operator-facing current runbook:
- [`MANUAL_FUTURES_PILOT_RUNBOOK.md`](/Users/patrick/Documents/MGC-v05l-automation/docs/MANUAL_FUTURES_PILOT_RUNBOOK.md)

Current status boundary:
- Proven inside sandbox: narrow futures lane policy, whitelist-controlled symbol authorization, and operator-facing preview/live-gate observability
- Not yet proven from sandbox: end-to-end live Schwab open/fill/flatten/return-to-flat cycle

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
