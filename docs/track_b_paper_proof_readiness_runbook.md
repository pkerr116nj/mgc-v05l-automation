# Track B Paper Proof Readiness Runbook

This runbook is the final mechanical checklist before running a Track B paper proof. It does not replace `paper_proof_cli` gates. It exists so the operator can see one clear readiness verdict and one clear next action before any broker submit is attempted.

Track B Phase 1 has passed the full PAPER open/guarded-close/flat lifecycle.
Phase 2 may execute PAPER trades through Track B-controlled paths when explicit
operator/config flags request that handoff and every readiness/submit gate
passes. Default strategy, listener, replay, operator-status, and UI flows remain
no-submit. Live-money execution is not implemented.

## Broker-State Stop Condition

Do not run `paper_proof_cli` for `DUM882026` / `MGC-202606` while any same-account/same-contract broker order remains working, ambiguous, or `PendingCancel`.

That state is operationally blocked even if there is no known economic exposure. Track B development may continue, but same account/contract proof submits must wait for terminal/clean broker state.

## Command Order

1. Run read-only recovery status:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.recovery_status_cli \
  --mode PAPER \
  --host 127.0.0.1 \
  --port 7497 \
  --client-id 17077 \
  --account-id DUM882026 \
  --contract-key MGC-202606 \
  --broker-order-id 1 \
  --perm-id 736787312 \
  --output-root outputs/track_b_execution_core/recovery_status
```

Stop unless `final_readiness_verdict` is `READY_FOR_PAPER_PROOF`.

2. Run read-only preflight:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.preflight_cli \
  --mode PAPER \
  --host 127.0.0.1 \
  --port 7497 \
  --client-id 17077 \
  --account-id DUM882026 \
  --contract-key MGC-202606 \
  --market-data-mode DELAYED \
  --output-root outputs/track_b_execution_core/preflight
```

Stop unless `final_readiness_verdict` is `READY_FOR_PAPER_PROOF`, account and contract match exactly, position is flat, and proof-contract open orders are clean.

3. Run Databento quote diagnostic/current quote check if current quote-derived pricing will be used.

Historical Databento diagnostics prove provider plumbing and parsing, not current executable quote readiness. A current Databento quote can support paper execution diagnostics, but it does not make the run live-money ready by itself.

4. Optionally run the no-submit readiness summary over the produced reports.

```bash
./.venv/bin/python -m mgc_v05l.execution_core.readiness_summary_cli \
  --recovery-report-json <RECOVERY_REPORT_JSON> \
  --preflight-report-json <PREFLIGHT_REPORT_JSON> \
  --proof-timing-status ACTIVE_SESSION \
  --quote-report-json <QUOTE_REPORT_JSON> \
  --output-root outputs/track_b_execution_core/readiness_summary
```

This command does not submit, cancel, place orders, or connect to a broker. It only summarizes report JSON.

5. Run `paper_proof_cli` only after recovery/preflight are clean, proof timing is active, and pricing is explicitly approved.

## Paper Proof Lifecycle

Track B paper proof now owns the explicit open/close proof lifecycle inside `paper_proof_cli`. Manual cleanup is a fallback only.

The lifecycle is intentionally narrow:

- Open proof submits one PAPER `LMT DAY` order only after explicit submit flags, active timing, clean read-only preflight, and pricing gates pass.
- If the open proof fills and broker truth shows exactly `+1` for the default BUY proof, no working same-contract orders, and the configured PAPER account/contract, Track B may submit one close-only `SELL 1` order.
- If position is not exactly the expected one-lot state, Track B refuses the close and reports `BLOCKED_POSITION_NOT_EXPECTED`.
- If any working same-contract broker order exists before close, Track B refuses the close and reports `BLOCKED_WORKING_ORDER_EXISTS`.
- If the close fills and final broker truth is flat with no working same-contract orders, the lifecycle reports `PROOF_COMPLETE_FLAT`.
- If broker truth is already flat before Track B submits its guarded close, and there are no working same-contract orders, the lifecycle reports `PROOF_FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE` / `TRACK_B_PAPER_PROOF_FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE`. This is not a passed proof because Track B did not own complete close provenance, but it is more precise than a generic position-not-expected blocker.
- `AMBIGUOUS_MANUAL_REVIEW_REQUIRED` is reserved for cases where broker truth or callbacks are not sufficient to prove the safe next action.

The proof report includes lifecycle fields such as `proof_lifecycle_status`, `close_only_guard_reports`, `flat_after_close_guard_reports`, `open_submit_diagnostics`, and `close_submit_diagnostics`. These are operator artifacts; the UI/dashboard must display them only and must not submit.

## Track B Paper Proof Live Validation Lessons

On May 4, 2026, Track B ran two real PAPER proof attempts for `DUM882026` / `MGC-202606` / `MGCM6`. Both proved useful lifecycle edges, and both ended fail-closed before the current code fixes were in place.

Attempt 1:

- Run artifact:
  `outputs/track_b_execution_core/paper_proof/proof_runs/paper_proof_bf69bca798bf4c6093036b8955b495ec/proof_report.json`
- `paper_proof_cli` submitted a PAPER `BUY 1` MGC limit order.
- TWS filled the open order, but the proof report saw no `openOrder`, `orderStatus`, or `execDetails` callbacks inside the callback wait window.
- Track B classified the run as `TRACK_B_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED`.
- Because the proof runner did not yet have post-submit broker-truth close reconciliation, cleanup required close tooling/manual flatten outside the Track B proof lifecycle.
- Fix applied afterward: `31b81b3be1` added callback-gap reconciliation, guarded close-only lifecycle handling, lifecycle status artifacts, and final flat verification inside the Track B proof path.

Attempt 2:

- Run artifact:
  `outputs/track_b_execution_core/paper_proof/proof_runs/paper_proof_0bc4f15438f64868a09c70fe9faa3a14/proof_report.json`
- `paper_proof_cli` submitted a PAPER `BUY 1` MGC limit order.
- TWS/API callbacks recorded the open order and `execDetails`; the report reached `proof_lifecycle_status=OPEN_FILLED`.
- The proof then failed during position-truth reconciliation because the IBKR position callback contract shape did not match Track B's allowlist correlation logic.
- Track B classified the run as `TRACK_B_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED` with `failure_or_ambiguity=missing positionEnd callback`, after the reader callback path hit `IbkrPaperCorrelationError: contract callback did not match allowlist`.
- Recovery after cleanup showed `RECOVERY_READY_CLEAN`, `position_quantity=0`, `working_order_count=0`, and `final_readiness_verdict=READY_FOR_PAPER_PROOF`.
- Fix applied afterward: `2f57118dd2` tightened MGC contract callback correlation for legitimate IBKR callback forms and captures callback errors as reportable artifacts instead of allowing the reader callback path to die silently.

Current code now addresses these specific live-validation findings:

- Callback gap reconciliation: after an open submit, the proof lifecycle can use broker position/open-order truth to decide whether a close-only order is safe.
- Guarded close-only lifecycle: Track B submits close only when broker truth shows the exact expected one-lot position and no working same-contract orders.
- MGC contract callback correlation: IBKR callbacks may match exact `conId`, exact `localSymbol`, or exact futures symbol/security type/currency/contract-month/multiplier when noncritical fields are missing.
- Callback error capture: IBKR reader callback exceptions are captured in `callback_errors` / `broker_callback_errors` instead of crashing without an artifact.

Final validation:

- Run artifact:
  `outputs/track_b_execution_core/paper_proof/proof_runs/paper_proof_222fcd070b9d453c9bb9d0061b975889/proof_report.json`
- Readiness was green with a realtime Databento quote before the proof command.
- `paper_proof_cli` used TWS PAPER client id `17086`.
- The open PAPER `BUY 1` MGC limit order filled with execution id `0000e1a7.69f8fef6.01.01`.
- The guarded close-only PAPER `SELL 1` MGC limit order filled with execution id `0000e1a7.69f8fef8.01.01`.
- The final broker position callback matched the allowlisted `MGC-202606` / `MGCM6` contract, including `conId=712565978`, `lastTradeDateOrContractMonth=20260626`, `multiplier=10`, and `tradingClass=MGC`.
- `flat_after_close_guard_reports` recorded `flat_clean=true`, `observed_signed_quantity=0`, no working orders, `submit_allowed=false`, and `live_money_readiness=false`.
- Final TWS state was flat.
- Classification was `TRACK_B_PAPER_PROOF_PASSED` with `proof_lifecycle_status=PROOF_COMPLETE_FLAT`.

Resolved issue status:

- The attempt-1 callback gap is resolved by `31b81b3be1`, which added broker-truth reconciliation and guarded close-only lifecycle handling.
- The attempt-2 MGC position callback correlation failure is resolved by `2f57118dd2`, which accepts legitimate exact MGC callback forms and captures callback errors as artifacts.
- The final validation run above proves the Track B PAPER proof lifecycle can now open, verify, close, and end flat under the current guarded path.

Phase 2 strategy-paper validation race:

- Run artifact:
  `outputs/track_b_execution_core/paper_proof/paper_proof_9eec720a567843748cc4988b14b29748/paper_proof_report.json`
- Strategy runner artifact:
  `outputs/track_b_execution_core/track_b_strategy_paper_runner/latest_track_b_strategy_paper_runner_report.json`
- The strategy rule emitted a LONG signal and readiness was green with `READY_FOR_PAPER_PROOF`.
- The strategy paper runner invoked the Track B paper-proof lifecycle with explicit PAPER submit flags and manual open/close limit prices.
- Track B submitted the open PAPER `BUY 1` MGC limit order.
- The proof lifecycle observed `openOrder` / `orderStatus` for the BUY, but did not receive `execDetails` within the callback wait window.
- During the post-open callback-gap guard, broker position truth showed `0` while the same BUY order still appeared working as `Submitted` with remaining quantity `1.0`.
- The proof report contains no Track B-owned close provenance: `close_intent=null`, `close_submit_attempt=null`, and `close_fill=null`.
- TWS ended flat, but the flat state was not proven by a complete Track B open/close/final-flat chain.
- Correct outcome: review-required, not passed. Current code classifies this pattern as `TRACK_B_PAPER_PROOF_FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE` when broker truth is flat without working orders, and remains `AMBIGUOUS_MANUAL_REVIEW_REQUIRED` / `BLOCKED_WORKING_ORDER_EXISTS` when a same-contract order still appears working.

This race is an expected fail-closed broker-state pattern, not a reason to loosen the close guard. If position truth, order truth, and fill callbacks disagree, Track B must preserve the contradiction in artifacts and refuse another automated broker mutation.

### Readiness Before Any Future Proof

Run the no-submit readiness check runner first, or run its component checks manually:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.track_b_readiness_check_runner_cli \
  --mode PAPER \
  --host 127.0.0.1 \
  --port 7497 \
  --client-id 17077 \
  --account-id DUM882026 \
  --contract-key MGC-202606 \
  --broker-order-id 1 \
  --perm-id 736787312 \
  --market-data-mode DELAYED \
  --databento-continuous-symbol MGC.v.0 \
  --dataset GLBX.MDP3 \
  --allowlisted-local-symbol MGCM6 \
  --tick-size 0.1 \
  --exchange COMEX \
  --currency USD \
  --proof-timing-status ACTIVE_SESSION \
  --quote-provider-mode REALTIME \
  --max-wait-cycles 10 \
  --wait-poll-seconds 15 \
  --output-root outputs/track_b_execution_core/track_b_readiness_check_runner
```

Proceed to paper proof review only when recovery, preflight, proof timing, and realtime quote readiness are clean. Clean readiness still does not submit.

### Running Paper Proof

Run `paper_proof_cli` only as an explicit operator action, with submit flags and current approved pricing context. The proof lifecycle remains PAPER-only and uses `paper_proof_cli` as the only Track B submit surface.

If manual paper-only prices are used, both open and close prices must be supplied and acknowledged through the existing flags. Do not use historical/fallback data as readiness.

### Recovering If Ambiguous

If a proof returns `TRACK_B_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED`:

- Stop. Do not run another open proof.
- Inspect `proof_report.json`, especially:
  - `proof_lifecycle_status`
  - `failure_or_ambiguity`
  - `required_manual_action`
  - `open_submit_diagnostics`
  - `close_submit_diagnostics`
  - `close_only_guard_reports`
  - `flat_after_close_guard_reports`
  - `broker_callback_errors`
- Verify TWS position and working orders manually.
- Use Track B close/flatten proof lifecycle only if it is explicitly available for the current state and its guards pass. Otherwise use manual TWS cleanup as fallback.
- After any cleanup, rerun read-only recovery and preflight before considering any future proof.

### Verifying Flat

A clean post-proof or post-cleanup state requires:

- `recovery_status` classification `RECOVERY_READY_CLEAN`
- position quantity `0`
- working order count `0`
- `final_readiness_verdict=READY_FOR_PAPER_PROOF`
- no unresolved same-account/same-contract order blocker

### Known Failure Classes And Artifacts

- Missing submit callbacks:
  Inspect `open_submit_diagnostics` / `close_submit_diagnostics` for `openOrder_seen`, `orderStatus_seen`, `execDetails_seen`, `completedOrder_seen`, `broker_order_id_allocated`, and `error_callbacks_after_submit`.
- Position not exactly expected before close:
  Inspect `close_only_guard_reports`; expected blocker is `BLOCKED_POSITION_NOT_EXPECTED`.
- Working same-contract order before close:
  Inspect `close_only_guard_reports`; expected blocker is `BLOCKED_WORKING_ORDER_EXISTS`.
- Flat before Track B close provenance:
  Inspect `close_only_guard_reports`; expected classification is `TRACK_B_PAPER_PROOF_FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE`. Verify broker activity and rerun read-only recovery before any further PAPER submit. Do not relabel this as `TRACK_B_PAPER_PROOF_PASSED` unless Track B artifacts include the complete open/close/final-flat proof chain.
- Contradictory broker truth after open callback gap:
  Inspect `open_submit_diagnostics`, `close_only_guard_reports`, and proof payload close fields. The pattern is `execDetails_seen=false`, broker position flat or unexpected, same-contract open order still appearing working, and no `close_intent` / `close_submit_attempt` / `close_fill`. Correct response is manual review: verify TWS position, flatten manually if needed, run `recovery_status_cli`, and proceed only after `RECOVERY_READY_CLEAN`.
- Final not flat after close:
  Inspect `flat_after_close_guard_reports`; do not send another close order automatically.
- IBKR callback contract-correlation errors:
  Inspect `broker_callback_errors` in the proof report and raw callback contract fields stored under position artifacts.
- Broker recovery/preflight blockers:
  Inspect recovery/preflight report JSON before any new proof attempt.

## Stop Conditions

Do not proceed if any of these are true:

- `BLOCKED_UNRESOLVED_BROKER_ORDER`: wait for terminal broker order state, then rerun recovery/preflight.
- `BLOCKED_NON_FLAT_POSITION`: flatten or reconcile the account before proof.
- `BLOCKED_OUTSIDE_ACTIVE_SESSION`: wait for an active exchange session.
- `BLOCKED_UNKNOWN_PROOF_TIMING`: provide active-session timing evidence or implement a reviewed session calendar guard.
- `BLOCKED_CONTRACT_OR_ACCOUNT_MISMATCH`: fix the explicit account/contract configuration.
- `BLOCKED_MARKET_DATA_MODE_OR_QUOTE_UNAVAILABLE`: obtain a usable current quote or use explicitly acknowledged manual open/close limit prices for paper-only proof.
- `AMBIGUOUS_MANUAL_REVIEW_REQUIRED`: stop and review TWS/API/broker state manually.

## Readiness Meanings

- Paper proof readiness means Track B may run an explicitly confirmed PAPER proof if all proof CLI flags and gates still pass.
- Quote availability means market data can support pricing diagnostics; it is not broker authority.
- Live-money readiness is not proven by delayed IBKR data, historical Databento diagnostics, or a paper proof.

IBKR broker truth remains authority for account, contract qualification, positions, open orders, order status, fills, and final reconciliation. Databento continuous symbols are market-data selectors only. The IBKR allowlist remains execution authority.

## Phase 2 Strategy Signal Wiring

After the successful Track B Phase 1 paper-proof lifecycle, Phase 2 begins with
a no-submit strategy-rule runner. This runner evaluates realtime Databento
quote/candle evidence and may write a Track B signal batch for shadow/listener
review. Its default mode is deliberately separate from paper proof readiness
and does not call `paper_proof_cli`.

Current Phase 2 chain:

```text
Databento realtime quote/event
-> track_b_strategy_rule_runner
-> strategy_signal_adapter
-> candle_signal_producer
-> signal_batch_writer
-> shadow_listener / operator_status / Track B Status UI
```

The first real rule is `mgc_ema_momentum_reclaim_long_v1`, an auditable
MGC-only EMA momentum reclaim rule. It consumes realtime Databento quote/candle
evidence plus explicit precomputed EMA momentum/VWAP fields on the event
metadata. It emits LONG only when:

- the input event is for `MGC-202606`;
- the referenced quote report proves `quote_provider_mode=REALTIME`;
- `realtime_quote_received=true`;
- `current_quote_available=true`;
- `close` reclaims VWAP after `prior_close` was below VWAP;
- `momentum_turning_positive=true`;
- `momentum_norm` and `momentum_acceleration` meet configured thresholds
  defaulting to zero;
- `--rule-mode MGC_EMA_MOMENTUM_RECLAIM_LONG --emit-signal` is explicitly
  supplied.

Without `--emit-signal`, the runner writes HUMAN_REVIEW / NO_SIGNAL artifacts
and no listener inbox work. Historical/fallback Databento evidence remains
diagnostic only and cannot become a strategy signal without an explicit fixture
demo flag. Missing feature fields or failed conditions produce NO_SIGNAL
artifacts, not execution. The older `DEMO_LONG_ONLY` mode remains available as
an explicit wiring proof, but it is not the default Phase 2 rule. A strategy
signal remains evidence/proposal input only by default.

Phase 2 includes an explicit handoff from a strategy rule to readiness and
`paper_proof_cli`, and that handoff remains opt-in and artifact-driven:

- explicit PAPER mode/account/contract/lane context;
- explicit strategy-runner handoff flag/config;
- clean read-only recovery and preflight;
- realtime current quote evidence accepted by readiness;
- explicit paper submit confirmation flags owned by the Track B submit path;
- durable artifacts for strategy decision, readiness, submit diagnostics,
  lifecycle state, fills or blockers, guarded close/flatten state, and final
  broker-state classification.

The UI must not initiate hidden submits or infer authority from a healthy
strategy/listener/operator-status report. Any future UI control must call an
explicit Track B API/CLI and display the resulting report. Live-money readiness
remains false unless a future live-readiness phase explicitly changes it.

The first controlled handoff boundary is `track_b_strategy_paper_runner`:

```text
realtime Databento event
-> track_b_strategy_rule_runner
-> track_b_readiness_check_runner
-> paper_proof_cli / paper proof lifecycle
```

It defaults to dry-run/no-submit. PAPER proof is invoked only when all explicit
submit gates are present:

- `--mode PAPER`
- `--submit-paper`
- `--confirm-paper-submit`
- `--quantity 1`
- `--manual-open-limit-price`
- `--manual-close-limit-price`

The runner refuses non-PAPER mode, missing quantity, missing manual prices, and
missing submit confirmation flags before strategy/readiness/proof stages can
mutate broker state. It reports `PAPER_READY_NO_SUBMIT_REQUESTED` when strategy
signal and readiness are green but submit flags are absent. If proof is invoked,
the report records proof classification, proof report path, final flat status
when available, `submit_attempted=true`, and `live_money_readiness=false`.
