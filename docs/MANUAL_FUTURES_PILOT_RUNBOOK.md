# Manual Futures Pilot Runbook

This runbook covers the durable narrow manual futures pilot lane only.

Allowed lane:
- `asset_class=FUTURE`
- `symbol=<WHITELISTED_FUTURES_SYMBOL>`
- `order_type=LIMIT`
- `quantity=1`
- `time_in_force=DAY`
- `session=NORMAL`
- open only: `MANUAL_LIVE_FUTURES_PILOT / BUY_TO_OPEN`
- close only: `FLATTEN / SELL_TO_CLOSE`
- `clientOrderId` omission is allowed only for this exact approved futures lane

Symbol scope:
- futures-only
- whitelist-controlled
- requires a configured futures market-data mapping for the selected symbol

Not allowed here:
- `ANYTIME`
- additional symbols beyond the configured whitelist
- multi-contract sizing
- market orders
- stop or stop-limit orders
- autonomous routing
- any stock-pilot change

## Launch Path

1. Start or confirm the existing service-first host.
   Command if needed:
   ```bash
   bash scripts/run_headless_supervised_paper_service.sh --wait-timeout-seconds 120
   ```
2. Verify host health.
   ```bash
   curl -sS http://127.0.0.1:8790/health
   ```
3. Prime local operator auth before preview or submit.
   ```bash
   bash scripts/run_local_operator_auth.sh
   ```
4. Use the existing operator surface at `http://127.0.0.1:8790/`.
   Operator path: `Positions > Manual Order Ticket`

## Preconditions

- Local operator auth is active and unexpired.
- Selected live Schwab account is the intended live account.
- Broker reachable is green.
- Auth healthy is green.
- Balances freshness is in bounds.
- Positions freshness is in bounds.
- Orders freshness is in bounds.
- Reconciliation is `CLEAR`.
- Futures pilot status shows no live-submit blockers.
- Selected symbol is explicitly whitelisted and mapped in the futures pilot config.
- Exact lane remains `FUTURE / LIMIT / 1 / DAY / NORMAL`.

## Open Sequence

1. Build the ticket exactly as:
   - `MANUAL_LIVE_FUTURES_PILOT`
   - `BUY`
   - `FUTURE`
   - `<WHITELISTED_FUTURES_SYMBOL>`
   - `LIMIT`
   - `1`
   - `DAY`
   - `NORMAL`
2. Preview first.
3. Require all of these in the preview output:
   - `action_phase = OPEN_PREVIEW`
   - `allowing_rule = MANUAL_FUTURES_PILOT_TIME_SESSION_POLICY`
   - `symbol_authorization.allowed = true`
   - `live_submit_enabled = true`
   - `live_submit_blockers = []`
4. Submit only after explicit operator review confirmation.
5. Capture:
   - broker order id
   - acknowledgement
   - lifecycle progression
   - broker/manual position truth

## Close Sequence

1. Confirm broker truth shows an existing `LONG 1` position in the same whitelisted futures symbol.
2. Build the ticket exactly as:
   - `FLATTEN`
   - `SELL`
   - `FUTURE`
   - `<WHITELISTED_FUTURES_SYMBOL>`
   - `LIMIT`
   - `1`
   - `DAY`
   - `NORMAL`
3. Preview first.
4. Require all of these in the preview output:
   - `action_phase = FLATTEN_PREVIEW`
   - `allowing_rule = MANUAL_FUTURES_PILOT_TIME_SESSION_POLICY`
   - `symbol_authorization.allowed = true`
   - `live_submit_enabled = true`
   - `live_submit_blockers = []`
5. Submit only after explicit operator review confirmation.
6. Capture:
   - broker order id
   - acknowledgement
   - fill
   - flat confirmation
   - reconciliation returning `CLEAR`

## Surfaces To Check

- `futures_pilot_status`
- `futures_pilot_policy_snapshot`
- `runtime_state.last_manual_order_preview`
- `runtime_state.last_manual_order`
- `manual_live_orders.recent_rows`
- `broker_state_snapshot.positions_by_symbol`
- `orders.open_rows`
- `reconciliation`
- `local_operator_auth`

## Pass Criteria

- Preview allowed for the selected whitelisted open lane.
- Open submit accepted by Schwab with broker order id.
- Broker/manual truth reaches `LONG 1` in the selected symbol.
- Preview allowed for the selected whitelisted flatten lane.
- Flatten submit accepted by Schwab with broker order id.
- Broker/manual truth returns to flat.
- Reconciliation returns `CLEAR`.
- No same-symbol unresolved ambiguity remains.
- No stuck or residual open order/manual lifecycle remains.

## Abort Criteria

- Any live-submit blocker appears before submit.
- Broker order id is missing after submit acknowledgement.
- Broker/account/auth freshness becomes degraded before the next irreversible step.
- Same-symbol ambiguity appears between broker and tracked manual state.
- Open leg does not resolve to broker-truth-backed `LONG 1`.
- Close leg does not restore flat state and `CLEAR` reconciliation.

## Proof Boundary

Proven from sandbox:
- The whitelist-controlled one-lot manual futures lane is durable and explicitly defined.
- The allowing rule is the scoped manual futures pilot time/session policy.
- Preview/live-gate observability is present for the approved lane.

Not yet proven from sandbox:
- Real Schwab broker acceptance, fill, flatten, and return-to-flat behavior.
- End-to-end live cycle completion against the real broker.

Do not claim end-to-end live success until the outside-sandbox operator run completes successfully.
