# Track B PAPER Round-Trip Proof Runbook

## Purpose And Scope

This runbook is for supervised Track B PAPER runtime round-trip proofs only. It is not a live-money procedure, not a `paper_proof` procedure, and not a broad recovery playbook.

The proof objective is:

```text
clean preflight -> one runtime writer -> entry -> broker fill -> auto-adoption
-> OPEN_MANAGED -> managed exit -> close fill -> CLOSED_FLAT -> clean shared truth
```

Use execution_core authority artifacts only. Dashboard projections are display-only and must not be used as runtime, readiness, routing, or recovery authority.

Hard boundaries:

- PAPER only.
- `live_money_eligible=false`.
- Exactly one runtime writer.
- No broad cancel.
- No broad flatten.
- No `paper_proof`.
- No dashboard projection as authority.

## Preflight Checklist

Run the shared truth refresh before any runtime/proof attempt:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.track_b_shared_truth_refresh_cli \
  --repo-root /Users/patrick/Dev/MGC-v05l-automation \
  --runtime-start-preflight
```

Expected clean classifications:

- Open Order Truth: `NO_OPEN_ORDERS`
- Managed Order Registry: `NO_MANAGED_ORDERS`
- Position Truth: `CLEAN_FLAT_READY`
- Runtime Environment Truth: `RUNTIME_DOWN_CLEAN`
- Managed Position Registry: `NO_MANAGED_POSITIONS`
- Reconciliation: `TRACK_B_PAPER_BROKER_RECONCILED`
- Broker Truth Lease: `ACTIVE`
- `live_money_eligible=false`

Also confirm Phase-1 runtime candles are fresh for:

- MGC 1m
- MGC 5m
- MNQ 1m
- MNQ 5m

If the session is closed or in a known Globex halt, stale bars should classify as `MARKET_CLOSED_NO_FRESH_BARS`. That is clear and expected, but it still blocks runtime trading. Do not retry the proof until Globex is open and fresh Phase-1 bars are writing again.

## Runtime Launch Procedure

Use the repaired direct supervisor launcher:

```bash
bash scripts/run_probationary_paper_soak.sh --background
```

The launcher now runs shared-truth runtime-start preflight before spawning the runtime. If shared truth is not clean, it writes launch status `SHARED_TRUTH_PREFLIGHT_BLOCKED` and does not start the child process.

Expected launch/convergence evidence:

- Live PID.
- Dev-root cwd.
- Current HEAD/source commit.
- Runtime command marker includes `mgc_v05l.app.main probationary-paper-soak`.
- Two advancing `paper_runtime_truth.json` samples.
- `heartbeat_state=HEALTHY`.
- `freshness_state=FRESH`.
- `writer_authority=SINGLE_WRITER`.
- Lane count `17`.
- Mule enabled if the current proof expects mule lanes.
- Canonical readiness `READY_SUBMIT_CAPABLE`.
- `paper_trade_allowed=true`.
- `live_money_eligible=false`.

## Expected Successful Round Trip

Observe one mule or B+ event through the full chain:

1. Entry `OrderIntent` created.
2. Position-management manifest exists before submit.
3. Broker submit/fill observed.
4. Shared broker-backed entry auto-adoption updates manifest and lifecycle.
5. Lifecycle becomes valid `OPEN_MANAGED`.
6. Managed Position Registry sees the open managed position.
7. Managed exit fires through the runtime exit path.
8. Broker-position-before-close guard passes.
9. Managed Order Registry sees the close order state.
10. Close fills.
11. Lifecycle becomes `CLOSED_FLAT`.
12. Open Order Truth returns `NO_OPEN_ORDERS`.
13. Position Truth returns `CLEAN_FLAT_READY`.
14. Runtime Environment Truth remains healthy or clean down.
15. Managed Position Registry returns `NO_MANAGED_POSITIONS`.
16. Reconciliation remains `TRACK_B_PAPER_BROKER_RECONCILED`.

## Failure Classifications And Operator Actions

`MARKET_CLOSED_NO_FRESH_BARS`

- Meaning: Phase-1 producer may be healthy, but no fresh bars are expected because the futures session is closed or in Globex halt.
- Action: wait for Globex reopen and fresh MGC/MNQ 1m/5m bars.
- Do not start runtime for a proof.

`SHARED_TRUTH_PREFLIGHT_BLOCKED`

- Meaning: one or more execution_core authority services are not clean.
- Action: read the blocker summary and authority artifact paths. Do not launch runtime.

`runtime_exited_after_preflight`

- Meaning: runtime passed its earliest preflight but exited before sustained runtime-truth convergence.
- Action: inspect launch status, runtime log, and stop provenance. Do not retry blindly.

`paper_reconciliation_mismatch`

- Meaning: runtime observed broker/lifecycle/ownership contradiction.
- Action: stop proof observation, refresh shared truth, and perform scoped read-only audit. Remediate only with explicit authorization.

Suspicious order state

- Examples: sentinel filled quantity, missing remaining quantity, open close order with no execDetails, marketable limit not filling beyond threshold.
- Action: do not submit a second close. Use Managed Order Registry and Order Adjustment Planner. If state is contradictory in TWS/API, prefer operator-assisted evidence capture.

Duplicate close risk

- Meaning: an existing working close may already cover the position.
- Action: do not replace while live. Replacement is allowed only after terminal cancel/inactive/fill is confirmed.

Runtime down with broker exposure

- Meaning: runtime is not alive and broker position exists.
- Action: do not restart. Refresh shared truth and classify whether the position is OPEN_MANAGED, adoption-required, review-required, or unmanaged exposure.

API/TWS divergence

- Meaning: TWS GUI, API open orders, execution callbacks, and broker truth disagree.
- Action: capture evidence first. Manual TWS cancel may be appropriate only for the exact suspicious order. Replacement requires terminal confirmation first.

## Managed Order Handling

Prefer modify-in-place over cancel/replace when the order is clean and eligible:

- Same account.
- Same contract/conId.
- Same order id/perm id.
- Same action.
- Same quantity.
- No duplicate close order.
- No suspicious sentinel state.

Never submit a replacement while an existing close order is still live. Suspicious sentinel orders default to review-required or manual/TWS path, not automatic replacement.

Manual TWS intervention rules:

- Use only for the exact suspicious order.
- Capture evidence before and after.
- Do not touch unrelated symbols or orders.
- After terminal cancel/fill, refresh Open Order Truth, Managed Order Registry, Position Truth, reconciliation, and lifecycle before any next action.

## Safe Cleanup Procedure

Cleanup is scoped remediation only:

1. Refresh broker truth, open orders, lifecycle, ownership, reconciliation, and shared truth.
2. Confirm exact target position and contract.
3. Confirm no existing working close order for that contract/action/quantity.
4. Repair/adopt lifecycle only when broker-backed evidence matches.
5. Submit at most one guarded PAPER close for the exact position.
6. If the close becomes working or unknown, stop and report order id/perm/status. Do not submit another close.
7. Confirm final flat:
   - broker positions `0`
   - open broker orders `0`
   - lifecycle open/review `0/0`
   - unresolved ownership `0`
   - Open Order Truth `NO_OPEN_ORDERS`
   - Managed Order Registry `NO_MANAGED_ORDERS`
   - Position Truth `CLEAN_FLAT_READY`
   - Runtime Environment Truth `RUNTIME_DOWN_CLEAN` or active healthy single writer
   - Managed Position Registry `NO_MANAGED_POSITIONS`
   - Reconciliation `TRACK_B_PAPER_BROKER_RECONCILED`
   - Broker Truth Lease `ACTIVE`

## Artifact Map

Shared truth and execution_core authority:

- `outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json`
- `outputs/track_b_execution_core/managed_orders/latest_managed_orders.json`
- `outputs/track_b_execution_core/managed_orders/latest_order_adjustment_plan.json`
- `outputs/track_b_execution_core/position_truth/latest_position_truth.json`
- `outputs/track_b_execution_core/runtime_truth/latest_runtime_environment_truth.json`
- `outputs/track_b_execution_core/managed_positions/latest_managed_positions.json`
- `outputs/track_b_execution_core/managed_orders/managed_order_events.jsonl`
- `outputs/track_b_execution_core/open_order_truth/open_order_truth_events.jsonl`
- `outputs/track_b_execution_core/position_truth/track_b_trade_outcome_events.jsonl`

Runtime and launch:

- `outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper_launch_status.json`
- `outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid`
- `outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid.json`
- `outputs/probationary_pattern_engine/paper_session/runtime/paper_runtime_truth.json`
- `outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.log`
- `outputs/probationary_pattern_engine/paper_session/runtime/market_data_transport_failure.json`

Phase-1 market data:

- `outputs/track_b_execution_core/phase1_runtime_market_data/MGC/1m/latest_runtime_candles.json`
- `outputs/track_b_execution_core/phase1_runtime_market_data/MGC/5m/latest_runtime_candles.json`
- `outputs/track_b_execution_core/phase1_runtime_market_data/MNQ/1m/latest_runtime_candles.json`
- `outputs/track_b_execution_core/phase1_runtime_market_data/MNQ/5m/latest_runtime_candles.json`
- `outputs/reports/phase1_databento_live_runtime_candles/latest_phase1_databento_live_listener_status.json`
- `outputs/reports/phase1_databento_live_runtime_candles/latest_phase1_databento_live_supervisor_status.json`

Lifecycle, manifests, ownership, reconciliation:

- `outputs/track_b_execution_core/position_management_manifests/`
- `outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json`
- `outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_paper_trade_summary.json`
- `outputs/track_b_execution_core/submit_ownership/`
- `outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json`
- `outputs/operator_dashboard/runtime/latest_broker_truth_lease.json`

Dashboard projections:

- `outputs/operator_dashboard/runtime/latest_track_b_position_truth.json`
- `outputs/operator_dashboard/runtime/latest_track_b_runtime_environment_truth.json`
- `outputs/operator_dashboard/runtime/latest_track_b_open_order_truth.json`
- `outputs/operator_dashboard/runtime/latest_track_b_managed_positions.json`
- `outputs/operator_dashboard/runtime/latest_track_b_managed_orders.json`

These dashboard paths are projection-only and are prohibited as authority inputs.

## Explicitly Prohibited Actions

- Broad flatten.
- Broad cancel.
- `paper_proof`.
- Live-money route.
- Duplicate runtime writers.
- Restarting runtime while broker exposure is unmanaged.
- Replacing a close order while the prior close is still live.
- Treating dashboard projection artifacts as runtime authority.
- Ignoring `live_money_eligible=true` if it ever appears.

## Future V2 Improvements

- Operator-authorized managed modify-in-place execution.
- Supervisor/self-healing integration with shared-truth runtime-start preflight.
- Richer API/TWS divergence state machine.
- Notification sidecar escalation for suspicious order and runtime-down-with-exposure states.
- Session-aware proof scheduling around Globex reopen and daily maintenance halt.
