# Track B Paper Proof Readiness Runbook

This runbook is the final mechanical checklist before running a Track B paper proof. It does not replace `paper_proof_cli` gates. It exists so the operator can see one clear readiness verdict and one clear next action before any broker submit is attempted.

## Current Stop Condition

Do not run `paper_proof_cli` for `DUM882026` / `MGC-202606` while broker order `1` / permId `736787312` remains `PendingCancel`.

That state is operationally blocked, not current economic exposure, because the known broker position is flat. Track B development may continue, but same account/contract proof submits must wait for terminal/clean broker state.

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
