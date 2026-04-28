# IBKR Paper Order Safety Design

## Classification

`IBKR_PAPER_ORDER_DESIGN_READY`

## Purpose

This is a design-only note for the safest possible IBKR paper-order harness.

It does **not**:

- implement order placement
- add submit code
- place an order
- stage an order
- connect ATP/GC strategy execution
- alter live execution behavior
- weaken the existing read-only verifier

Core question:

`What exact guardrails must exist before this repo is allowed to place even a single IBKR paper order?`

## Design Summary

- classification: `IBKR_PAPER_ORDER_DESIGN_READY`
- no order placement was implemented
- no order staging was implemented
- no strategy linkage was implemented
- live execution behavior was not touched
- read-only verifier behavior was not weakened
- any future submit capability must be manual-only and separate from strategy execution
- required environment lock is `PAPER / 127.0.0.1 / 7497`
- live port `7496` and IB Gateway live port `4001` must fail closed
- order workflow must default to preview-only
- typed approval must be bound to an exact preview digest
- allowed contract scope is hard-whitelisted `GC` and `MGC` only
- initial quantity cap is `qty <= 1`
- delayed-data-aware pricing controls are required
- post-submit and post-cancel open-order verification are required
- append-only audit logging is required
- ATP/GC strategy logic and any scheduler must not be able to import or construct a submit-capable object

## Current Ground Truth

The design assumes the current verified paper-only IBKR state:

- TWS paper is confirmed on `127.0.0.1:7497`
- `mode=PAPER`
- `host=127.0.0.1`
- `port=7497`
- `read_only=true` in the current verifier path
- account, positions, open orders, and contract truth work in read-only mode
- `GC 202606` and `MGC 202606` qualification work
- market data is delayed-only because of entitlement/settings behavior
- no orders have been placed or staged so far
- no ATP/GC strategy execution is connected

Reference context:

- [IBKR read-only connection report](/Users/patrick/Documents/MGC-v05l-automation/outputs/reports/ibkr_read_only_verification/ibkr_read_only_connection_report.md)
- [IBKR market-data diagnostic report](/Users/patrick/Documents/MGC-v05l-automation/outputs/reports/ibkr_market_data_diagnostic/ibkr_market_data_diagnostic_report.md)
- [IBKR paper order state machine](/Users/patrick/Documents/MGC-v05l-automation/docs/ibkr_paper_order_state_machine.md)
- [IBKR paper order guardrail checklist](/Users/patrick/Documents/MGC-v05l-automation/docs/ibkr_paper_order_guardrail_checklist.md)

## Design Decision

Before the repo is allowed to place even a single IBKR paper order, the order path must be built as a separate, manual-only harness with its own fail-closed environment lock, approval gate, preview screen, audit log, and cancel workflow.

The strategy stack must have no callable path to submit, transmit, modify, or cancel broker orders.

## Non-Negotiable Safety Principles

- paper only
- TWS only
- `mode=PAPER` only
- `host=127.0.0.1` only
- `port=7497` only
- never connect to live TWS port `7496`
- never connect to IB Gateway paper/live ports in this phase
- no strategy automation
- no ATP/GC execution linkage
- no scheduler
- no unattended runtime
- fail closed on any ambiguity

## Proposed Architecture

The future paper-order harness should exist behind a separate manual command path only.

Suggested separation:

- manual CLI entrypoint: `mgc_v05l.app.ibkr_paper_order_harness`
- future harness logic: `mgc_v05l.execution.ibkr_paper_order_harness`
- future shared models: `mgc_v05l.execution.ibkr_paper_order_models`

The strategy tree must not import these modules.

Recommended code boundary:

- keep read-only verification and account truth modules unchanged
- place any future submit-capable code in a new package that is not imported by ATP/GC, shadow ledger, or scheduling code
- require a manual operator command to start the harness
- require a separate runtime capability object such as `PaperOrderSubmitCapability`
- do not construct that capability anywhere except inside the manual CLI command after all guardrails pass

That means a strategy can observe truth, but it cannot ever gain a submit-capable object.

## Environment Lock

The future harness must repeat and strengthen the current read-only lock before any order workflow begins.

Required checks:

- configured mode must equal `PAPER`
- configured host must equal `127.0.0.1`
- configured port must equal `7497`
- reject `7496`
- reject `4001`
- reject `4002`
- reject any unknown port
- reject any non-local host
- reject if TWS handshake is incomplete
- reject if account list is empty
- reject if selected account id is missing
- reject if selected account appears live or unexpected
- reject if account id shown to the operator does not match the account id approved for the session

The harness should show the selected account id before any preview workflow continues.

Fail-closed examples:

- `mode=PAPER` with `port=7496`: reject
- `mode=LIVE` with `port=7497`: reject
- `mode=PAPER` with `port=4002`: reject in this phase
- duplicate client id during setup: reject
- disconnected or stale TWS session state: reject

## Manual Approval Gate

The default mode must be preview-only.

The harness must support two explicit phases:

1. `preview`
2. `submit-paper-order`

`preview` may never transmit.

`submit-paper-order` may only be available after preview output is shown and the operator provides an exact confirmation phrase or an explicit CLI approval flag that reproduces the order details.

Required approval payload:

- account id
- mode
- host
- port
- client id
- contract symbol
- expiry
- action
- quantity
- order type
- limit price if applicable
- time in force
- transmit behavior

Recommended confirmation phrase:

`APPROVE IBKR PAPER ORDER account=<ACCOUNT_ID> mode=PAPER host=127.0.0.1 port=7497 contract=<LOCAL_SYMBOL> action=<BUY_OR_SELL> qty=<QTY> type=<ORDER_TYPE> price=<PRICE_OR_NONE>`

Approval rules:

- the phrase must match the generated preview exactly
- any mismatch invalidates approval
- approval expires after a short TTL such as 60 seconds
- approval is single-use
- approval is bound to one preview digest
- approval is invalid if connection state, account id, or open-order baseline changes after preview

## Order Preview Requirements

Before any possible submit, the harness must display:

- account id
- mode, host, port, client id
- server version if available
- connection timestamp
- contract details
- exchange
- local symbol
- expiry
- multiplier
- action
- quantity
- order type
- limit price if applicable
- time in force
- `transmit` flag behavior
- estimated notional
- estimated tick value
- current delayed quote if available, clearly labeled `DELAYED`
- an explicit note that live market data is unavailable when that is true
- current open-order count before submit
- preview digest or fingerprint used for approval binding

Preview wording must make the delayed-data limitation obvious.

Example required warning:

- `Quote source: DELAYED data only. Live market data is not currently available. Any limit price decision is operator-reviewed and delayed-data-aware.`

## Initial Allowed Order Scope

First submit-capable phase must be smaller than the full paper trading universe.

Allowed scope:

- paper only
- manual test order only
- one contract maximum
- whitelist only `GC` and `MGC`
- default to `MGC 202606` as the smallest practical test instrument
- no market orders
- no stop orders
- no bracket orders
- no OCO
- no attached profit/stop legs
- no overnight automation
- no scheduler
- no strategy-generated orders
- no batch orders
- no multi-account routing

Initial permitted order type:

- `LMT` only

Initial pricing policy:

- default to a deliberately non-marketable limit price
- require an extra `delayed-data-aware` acknowledgment if live data is unavailable
- if `transmit=false` preview is later supported safely, treat it as a separate explicit mode and not as a substitute for approval

## Safety Controls

The harness must enforce the following controls before any future transmit is possible:

- max quantity guard of `1`
- allowed contracts whitelist for `GC 202606` and `MGC 202606` only
- reject any unknown symbol, expiry, exchange, or multiplier mismatch
- reject any account not explicitly selected and echoed in preview
- reject if connection state is stale, reconnecting, or unknown
- reject if account summary cannot be refreshed
- reject if open-order baseline cannot be refreshed
- reject if contract qualification must be inferred rather than proven
- reject if market data is unavailable unless the operator explicitly chooses a delayed-data-aware path
- reject if open orders already exist and the operator did not explicitly acknowledge them
- reject if the client id is already in use
- reject if the TWS session flips environments mid-workflow
- reject if preview digest no longer matches the submission request

### Cancel Utility Design

The first submit-capable phase must include a manual cancel utility design before any paper submit is allowed.

Required cancel controls:

- cancel by exact broker order id only
- show preview before cancel
- show account id, contract, status, remaining quantity, and original order details
- require a separate cancel confirmation phrase
- confirm cancel by reading open orders after request
- classify unresolved cancels as failed, not assumed successful

### Post-Action Verification

After any future submit:

- re-read open orders
- confirm the expected order id appears
- confirm action, quantity, order type, limit price, and status match preview
- record whether the order is transmitted, pending, inactive, or rejected

After any future cancel:

- re-read open orders
- confirm the targeted order no longer appears as open
- if still present, classify as `CANCEL_REJECTED` or `STALE_OPEN_ORDER_VIEW`

## Audit Log

Every operator action must create an append-only audit record.

Required events:

- environment lock evaluation
- preview generated
- approval requested
- approval denied
- approval accepted
- submit attempted
- submit rejected
- submit acknowledged
- open order confirmed
- cancel requested
- cancel acknowledged
- cancel confirmed
- connection lost
- stale view detected

Required fields:

- timestamp
- session id
- account id
- mode
- host
- port
- client id
- contract identity
- action
- quantity
- order type
- limit price
- time in force
- delayed-data status
- preview digest
- operator confirmation string hash
- resulting broker order id if any
- result classification
- error code and message if any

Audit logs should live outside strategy reporting and must never be silently suppressed.

## Separation From Strategy

This is the most important architectural rule.

The order harness must be unreachable from ATP/GC or any automated strategy path.

Required separation rules:

- ATP/GC strategy logic cannot import a submit-capable module
- no signal-to-order bridge
- no shadow-ledger execution linkage
- no automated entry or exit path
- no scheduler integration
- no operator dashboard one-click submit in this phase
- no reuse of generic execution interfaces that strategies already hold

Recommended enforcement:

- submit-capable interfaces live in a separate module tree
- strategy-facing execution interfaces remain read-only or abstracted without submit methods
- a dedicated test must fail if strategy packages import the submit-capable module tree
- the manual harness requires a `manual_only=True` construction path that no strategy code can satisfy

## Required State Machine

The harness must implement only the following explicit workflow states:

- `DISCONNECTED`
- `CONNECTED_READ_ONLY`
- `PREVIEW_READY`
- `APPROVAL_REQUIRED`
- `APPROVED_FOR_PAPER_SUBMIT`
- `SUBMITTED`
- `OPEN_ORDER_CONFIRMED`
- `CANCEL_REQUESTED`
- `CANCEL_CONFIRMED`
- `REJECTED_OR_FAILED`

The state machine is defined in [ibkr_paper_order_state_machine.md](/Users/patrick/Documents/MGC-v05l-automation/docs/ibkr_paper_order_state_machine.md).

No hidden or implicit submit state is allowed.

## Failure Behavior

Required responses:

- TWS disconnected: return to `DISCONNECTED`, clear approval token, fail closed
- duplicate client id: fail closed before preview
- contract qualification failure: fail closed, no preview
- account mismatch: fail closed, no preview
- market data unavailable: allow preview only; require delayed-data-aware acknowledgment before any future limit-order submit
- permission error: treat as market-data-limited, not as permission to guess pricing
- order rejected: record rejection, verify open-order state, move to `REJECTED_OR_FAILED`
- partial fill in future paper testing: immediately freeze further automation, require manual cancel/close handling, classify separately
- cancel rejected: remain in `REJECTED_OR_FAILED` until open-order truth proves otherwise
- stale open-order view: fail closed and force refresh before any new action

## Minimum Test Plan Before Any Future Submit Implementation

Before a submit-capable patch is allowed, the repo should have tests for:

- `PAPER + 7497` passes
- `PAPER + 7496` fails
- `LIVE + 7497` fails
- `PAPER + 4002` fails in this phase
- unknown port fails
- non-local host fails
- duplicate client id fails safely
- live-looking or unexpected account fails safely
- preview digest mismatch fails
- approval TTL expiry fails
- missing explicit confirmation phrase fails
- max quantity violation fails
- unknown contract fails
- market order attempt fails
- strategy import boundary fails
- open-order refresh failure blocks submit
- cancel verification failure blocks further actions

## Recommended First Paper Test Shape

When implementation is eventually allowed, the safest first paper test should be:

- instrument: `MGC 202606`
- quantity: `1`
- order type: `LMT`
- time in force: `DAY`
- price: deliberately non-marketable relative to the current delayed quote
- operator workflow: preview, explicit approval, submit, verify open order, cancel, verify cancel

That flow limits risk while still validating the full paper order lifecycle.

## Release Gate

The repo must not be allowed to place even a single IBKR paper order until all of the following are true:

- the environment lock is implemented and tested
- the manual approval gate is implemented and tested
- preview-only is the default behavior
- the submit path is manual-only and unreachable from strategy code
- the whitelist and max-quantity guards are implemented
- audit logging is implemented
- open-order verification after submit and cancel is implemented
- cancel utility exists
- strategy separation tests exist
- the guardrail checklist is signed off

If any one of those is missing, the correct outcome is:

- `IBKR_PAPER_ORDER_DESIGN_NEEDS_REVIEW` for incomplete design
- no order implementation work
