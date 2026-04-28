# IBKR Manual Paper Submit Test Design

## Classification

`IBKR_MANUAL_PAPER_SUBMIT_DESIGN_READY`

## Purpose

This is a design-only note for the safest possible first IBKR manual paper submit/cancel test.

It does **not**:

- implement submit
- place an order
- stage an order
- transmit an order
- connect ATP/GC
- connect strategy execution
- modify live execution behavior

Core question:

`What exact manual approval, submit, verify, cancel, and audit guardrails must exist before we allow one IBKR paper order test?`

## Commit Summary

- classification: `IBKR_MANUAL_PAPER_SUBMIT_DESIGN_READY`
- no submit logic was implemented
- no order was placed
- no order was staged
- no strategy linkage was added
- the required flow is preview-first
- manual CLI only
- approval requires exact preview digest plus typed confirmation phrase
- environment lock remains `PAPER / 127.0.0.1 / 7497`
- initial order scope is one-lot `MGC 202606` `LMT DAY` only
- immediate broker-truth verification is required after submit
- mandatory cancel verification by exact order id is required
- append-only audit logging is required for preview, approval, submit, open-order verification, cancel, and cancel verification
- ATP/GC strategy paths and any scheduler must not be able to reach a submit-capable object

## Design Summary

- classification: `IBKR_MANUAL_PAPER_SUBMIT_DESIGN_READY`
- this pass is design-only
- submit is not implemented in this pass
- no order was placed
- no order was staged
- no strategy was connected
- the first allowed test must use the committed preview harness as the gating entrypoint
- paper only, TWS only, manual CLI only
- required lock is `mode=PAPER`, `host=127.0.0.1`, `port=7497`
- live port `7496` must fail closed
- IB Gateway ports `4001` and `4002` must fail closed
- the first test must allow one order maximum
- `MGC 202606` is preferred over `GC 202606`
- `LMT` only, `DAY` only, `qty <= 1`
- preview digest and typed confirmation must both be required
- preview invalidation must block submit if any material field changes
- open-order verification after submit and after cancel is mandatory
- append-only audit logging is mandatory

## Current Basis

This design assumes:

- paper TWS is already verified at `127.0.0.1:7497`
- account, position, open-order, and contract truth already work in read-only mode
- the committed preview harness already generates deterministic preview digests
- delayed quote context is available when permissioned and clearly labeled
- no submit-capable object exists today

Reference context:

- [IBKR paper order safety design](/Users/patrick/Documents/MGC-v05l-automation/docs/ibkr_paper_order_safety_design.md)
- [IBKR paper order state machine](/Users/patrick/Documents/MGC-v05l-automation/docs/ibkr_paper_order_state_machine.md)
- [IBKR paper order guardrail checklist](/Users/patrick/Documents/MGC-v05l-automation/docs/ibkr_paper_order_guardrail_checklist.md)
- [IBKR paper order preview report](/Users/patrick/Documents/MGC-v05l-automation/outputs/reports/ibkr_paper_order_preview/ibkr_paper_order_preview_report.md)
- [IBKR manual paper submit/cancel state machine](/Users/patrick/Documents/MGC-v05l-automation/docs/ibkr_manual_paper_submit_cancel_state_machine.md)
- [IBKR manual paper submit release checklist](/Users/patrick/Documents/MGC-v05l-automation/docs/ibkr_manual_paper_submit_release_checklist.md)

## Scope

The first submit-capable test must stay as narrow as possible.

Allowed scope:

- paper only
- TWS only
- manual CLI only
- one order maximum
- one contract maximum
- one account only
- `MGC 202606` first
- `GC 202606` only if `MGC 202606` is already proven cleanly
- `LMT` only
- `DAY` only
- `qty = 1` maximum
- no market orders
- no stop orders
- no bracket orders
- no OCO
- no automation
- no scheduler
- no ATP/GC linkage
- no strategy-generated order path

## Required Environment Lock

Before a submit flow can even begin, the future harness must prove:

- `mode=PAPER`
- `host=127.0.0.1`
- `port=7497`
- TWS handshake completed successfully
- selected account id is known and displayed
- selected account id matches the explicitly expected account if one is provided
- account truth is fresh
- open-order baseline is fresh
- contract qualification is fresh

Fail-closed cases:

- live port `7496`
- IB Gateway live port `4001`
- IB Gateway paper port `4002`
- any unknown port
- non-local host
- account mismatch
- duplicate client id
- stale connection state
- stale open-order baseline

## Required Manual Approval Gate

The preview harness must remain the first step.

Required flow:

1. operator runs preview command
2. preview report is generated
3. preview digest is displayed
4. operator re-runs the dedicated submit command with the exact preview digest and the typed confirmation phrase
5. the submit command re-checks all guardrails
6. only then may a single paper order be sent

Required typed confirmation content:

- account id
- mode
- host
- port
- contract
- action
- quantity
- order type
- limit price
- time in force
- delayed quote warning
- preview digest

Recommended confirmation phrase:

`APPROVE IBKR PAPER SUBMIT account=<ACCOUNT_ID> mode=PAPER host=127.0.0.1 port=7497 contract=<LOCAL_SYMBOL> action=<BUY_OR_SELL> qty=1 type=LMT price=<LIMIT_PRICE> tif=DAY digest=<PREVIEW_DIGEST> delayed_quote_ack=yes`

Approval rules:

- phrase must match the preview exactly
- digest must match the preview exactly
- approval is single-use
- approval expires on TTL such as 60 seconds
- approval fails closed if preview payload changes
- approval fails closed if open-order baseline changes
- approval fails closed if account id changes
- approval fails closed if contract qualification changes

## Required Preview Content Before Submit

The preview that feeds the first submit test must display:

- account id
- mode / host / port / client id
- server version if available
- connection timestamp
- contract symbol
- local symbol
- expiry
- exchange
- multiplier
- qualified contract identifier if available
- action
- quantity
- order type
- limit price
- time in force
- current delayed quote if available, labeled `DELAYED`
- explicit note when live market data is unavailable
- estimated notional
- estimated tick value
- open-order baseline count
- all guardrail checks with pass/fail
- exact preview digest
- statement that no order was submitted, staged, or transmitted

## Recommended First Test Shape

The safest first test should be:

- instrument: `MGC 202606`
- action: `BUY` or `SELL`, operator-selected
- quantity: `1`
- order type: `LMT`
- time in force: `DAY`
- price: deliberately non-marketable relative to the delayed quote
- operator objective: validate submit visibility and cancel verification, not fill behavior

Why this shape is safest:

- `MGC` is smaller than `GC`
- `qty=1` caps exposure
- non-marketable `LMT` reduces fill risk
- `DAY` avoids overnight persistence
- manual cancel validates the full lifecycle while minimizing unintended execution risk

## Required End-to-End Test Flow

The first allowed paper test must follow this exact sequence:

1. Connect and verify environment lock.
2. Read account, positions, and open orders.
3. Require open orders baseline.
4. Qualify `MGC 202606`.
5. Generate preview.
6. Require typed approval.
7. Submit one paper limit order only if approved.
8. Immediately read open orders.
9. Cancel the order.
10. Verify cancel status.
11. Write audit log for preview, approval, submit, open-order verification, cancel, and cancel verification.

No step may be skipped.

## Submit Guardrails

Before a future paper submit is allowed, all of the following must still be true at submit time:

- preview digest matches
- account id matches
- mode / host / port still match
- client id matches the preview session or the explicitly approved submit session rules
- contract still qualifies as the same contract
- quantity still equals `1`
- order type still equals `LMT`
- time in force still equals `DAY`
- limit price still equals the preview value
- open-order baseline is still fresh
- operator acknowledged delayed quote limitations
- no unexpected open orders appeared since preview

If any one of those changes, approval must invalidate.

## Verification Guardrails After Submit

The first paper submit test is not successful just because a broker call returns.

Required verification immediately after submit:

- re-read open orders
- confirm one new open order appears
- confirm broker order id exists
- confirm account id matches
- confirm contract matches the preview
- confirm quantity matches
- confirm order type matches
- confirm limit price matches
- confirm time in force matches
- confirm order status is captured

If any mismatch occurs:

- fail closed
- classify the test as failed
- do not assume the order is safe or canceled

## Cancel Guardrails

The first allowed test must include cancel verification.

Required cancel controls:

- cancel by exact broker order id only
- operator sees cancel preview before request
- operator gives separate cancel confirmation
- cancel request is audited
- open orders are re-read after cancel
- final state must be proven by broker truth

Acceptable cancel verification outcomes:

- order no longer appears in open orders
- or status is explicitly shown as canceled by the broker truth path

Unacceptable outcomes:

- assume cancel worked without truth
- rely on local intent only
- rely on stale open-order state

## Audit Requirements

Every step must be append-only logged.

Required audit events:

- environment lock checked
- account truth read
- position truth read
- open-order baseline read
- contract qualified
- preview generated
- approval attempted
- approval accepted or rejected
- submit attempted
- submit acknowledged or rejected
- open-order verification attempted
- open-order verification result
- cancel requested
- cancel acknowledged or rejected
- cancel verification attempted
- cancel verification result

Required audit fields:

- timestamp
- session id
- account id
- mode
- host
- port
- client id
- symbol
- expiry
- qualified contract identifier
- action
- quantity
- order type
- limit price
- tif
- delayed quote warning status
- preview digest
- typed confirmation hash or redacted record
- broker order id if present
- verification result
- error code and message if any

## Failure Behavior

The first submit-capable implementation must fail closed on:

- account mismatch
- live port
- unknown port
- quantity greater than `1`
- non-whitelisted contract
- market order attempt
- missing limit price
- digest mismatch
- stale open-order baseline
- cancel verification that cannot prove final state

Additional fail-closed cases:

- duplicate client id
- connection drop after approval
- contract re-qualification mismatch
- unexpected open orders appearing after preview
- broker rejection after submit

## Separation From Strategy

The first manual submit test must remain unreachable from strategy code.

Required architecture rules:

- manual CLI only
- no ATP/GC linkage
- no strategy import path to submit-capable code
- no scheduler path
- no shadow-ledger bridge
- no operator dashboard shortcut in the first phase
- no shared generic submit interface exposed to strategy packages

Required enforcement:

- submit-capable code lives under a separate module tree
- the preview harness may be reused, but strategy code must still not gain a submit-capable object
- a strategy-boundary test must fail if strategy packages import the submit module tree

## Minimum Acceptance Criteria Before Implementation

Before submit code is allowed, the future implementation should prove:

- preview gate works
- exact digest approval works
- typed phrase approval works
- non-marketable one-lot `MGC 202606` `LMT DAY` submit can be observed in open-order truth
- cancel can be requested manually
- cancel can be verified by broker truth
- audit trail is complete
- strategy boundary remains intact

## Go/No-Go Rule

The repo must not be allowed to submit even one IBKR paper order until:

- the preview-first gate is implemented
- the approval phrase and digest lock are implemented
- all fail-closed cases above are tested
- submit verification is implemented
- cancel verification is implemented
- audit logging is implemented
- strategy separation tests are green
- the release checklist is fully complete

If any one of those is missing, the correct result is no submit implementation.
