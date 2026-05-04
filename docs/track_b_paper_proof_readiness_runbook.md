# Track B Paper Proof Readiness Runbook

This runbook is the final mechanical checklist before running a Track B paper proof. It does not replace `paper_proof_cli` gates. It exists so the operator can see one clear readiness verdict and one clear next action before any broker submit is attempted.

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
- `AMBIGUOUS_MANUAL_REVIEW_REQUIRED` is reserved for cases where broker truth or callbacks are not sufficient to prove the safe next action.

The proof report includes lifecycle fields such as `proof_lifecycle_status`, `close_only_guard_reports`, `flat_after_close_guard_reports`, `open_submit_diagnostics`, and `close_submit_diagnostics`. These are operator artifacts; the UI/dashboard must display them only and must not submit.

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
