# IBKR Paper Order State Machine

## Purpose

This state machine defines the only allowed workflow for a future manual IBKR paper-order harness.

It is design-only. It does not authorize implementation or order submission.

## States

### `DISCONNECTED`

Meaning:

- no trusted TWS session
- no valid account truth
- no valid preview
- no approval token

Allowed transitions:

- to `CONNECTED_READ_ONLY` after successful paper-only handshake on `127.0.0.1:7497`
- to `REJECTED_OR_FAILED` if handshake fails or environment lock fails

### `CONNECTED_READ_ONLY`

Meaning:

- TWS paper handshake succeeded
- account id is known
- contract truth may be queried
- no order preview exists yet
- no submit rights exist yet

Required checks:

- `mode=PAPER`
- `host=127.0.0.1`
- `port=7497`
- selected account id displayed
- duplicate client id not present
- account truth and open-order truth fresh enough for the session

Allowed transitions:

- to `PREVIEW_READY` after contract qualification and baseline truth refresh succeed
- to `DISCONNECTED` if TWS drops
- to `REJECTED_OR_FAILED` on account mismatch, stale state, or qualification failure

### `PREVIEW_READY`

Meaning:

- the harness can build a preview
- no approval has been granted
- submit is still impossible

Required output:

- account id
- mode, host, port, client id
- contract details
- action
- quantity
- order type
- limit price if applicable
- time in force
- estimated notional and tick value
- delayed quote, if available, labeled delayed
- live market-data unavailable warning, if true
- preview digest

Allowed transitions:

- to `APPROVAL_REQUIRED` after preview is rendered
- to `REJECTED_OR_FAILED` if preview inputs cannot be proven safe
- to `DISCONNECTED` if session drops

### `APPROVAL_REQUIRED`

Meaning:

- preview exists
- submit remains impossible until exact approval arrives

Required approval rules:

- exact confirmation phrase or explicit CLI approval flag
- approval bound to preview digest
- approval bound to account id, mode, host, port, contract, action, quantity, order type, and price
- approval expires on TTL

Allowed transitions:

- to `APPROVED_FOR_PAPER_SUBMIT` on valid approval
- to `REJECTED_OR_FAILED` on mismatch, expiry, or malformed approval
- to `DISCONNECTED` if session drops

### `APPROVED_FOR_PAPER_SUBMIT`

Meaning:

- all guards passed
- a single paper submit attempt is authorized for one preview digest only

Required re-checks immediately before any future submit:

- same account id
- same connection state
- same contract qualification
- same open-order baseline freshness
- same preview digest

Allowed transitions:

- to `SUBMITTED` if the broker acknowledges the submit request
- to `REJECTED_OR_FAILED` if any re-check fails
- to `DISCONNECTED` if session drops

### `SUBMITTED`

Meaning:

- a future paper order request has been sent to TWS
- success is not assumed

Required follow-up:

- read open orders
- read order status
- verify order id, action, quantity, type, price, and tif

Allowed transitions:

- to `OPEN_ORDER_CONFIRMED` when open-order truth matches the preview
- to `REJECTED_OR_FAILED` if rejected, unverified, or mismatched
- to `DISCONNECTED` if session drops before truth can be verified

### `OPEN_ORDER_CONFIRMED`

Meaning:

- the future paper order is visible in open-order truth
- operator may choose to cancel

Allowed transitions:

- to `CANCEL_REQUESTED` on explicit cancel request
- to `REJECTED_OR_FAILED` if the open-order view becomes stale or contradictory
- to `DISCONNECTED` if session drops

### `CANCEL_REQUESTED`

Meaning:

- a cancel action has been manually requested for a known broker order id

Required checks:

- exact order id known
- original account id still matches
- current open-order truth still shows the target order
- separate cancel confirmation received

Allowed transitions:

- to `CANCEL_CONFIRMED` when open-order truth confirms cancellation
- to `REJECTED_OR_FAILED` on cancel reject, stale view, or mismatch
- to `DISCONNECTED` if session drops

### `CANCEL_CONFIRMED`

Meaning:

- open-order truth no longer shows the target order as open

Allowed transitions:

- to `CONNECTED_READ_ONLY` after cleanup for a new manual session
- to `DISCONNECTED` if session ends

### `REJECTED_OR_FAILED`

Meaning:

- something unsafe, ambiguous, or rejected occurred
- no further submit attempts are allowed in the current session

Required behavior:

- clear approval token
- keep audit trail
- require a full return to `CONNECTED_READ_ONLY` with fresh truth before any new preview

Allowed transitions:

- to `CONNECTED_READ_ONLY` only after a full safety reset and fresh truth refresh
- to `DISCONNECTED` if session ends

## Transition Table

| From | Trigger | To | Fail-Closed Condition |
| --- | --- | --- | --- |
| `DISCONNECTED` | paper TWS handshake succeeds | `CONNECTED_READ_ONLY` | environment mismatch |
| `CONNECTED_READ_ONLY` | contract and truth refresh succeed | `PREVIEW_READY` | account mismatch or stale truth |
| `PREVIEW_READY` | preview rendered | `APPROVAL_REQUIRED` | unsafe preview inputs |
| `APPROVAL_REQUIRED` | exact approval validated | `APPROVED_FOR_PAPER_SUBMIT` | digest mismatch or TTL expiry |
| `APPROVED_FOR_PAPER_SUBMIT` | future submit acknowledged | `SUBMITTED` | state changed before submit |
| `SUBMITTED` | open-order truth matches preview | `OPEN_ORDER_CONFIRMED` | reject or mismatch |
| `OPEN_ORDER_CONFIRMED` | explicit cancel requested | `CANCEL_REQUESTED` | target order not provable |
| `CANCEL_REQUESTED` | open-order truth confirms cancel | `CANCEL_CONFIRMED` | cancel reject or stale view |
| `CANCEL_CONFIRMED` | session reset | `CONNECTED_READ_ONLY` | cleanup incomplete |
| any state | disconnect | `DISCONNECTED` | no trusted session |
| any state | safety violation | `REJECTED_OR_FAILED` | fail closed |

## Explicit Prohibitions

- no direct `CONNECTED_READ_ONLY -> SUBMITTED`
- no direct `PREVIEW_READY -> SUBMITTED`
- no direct `APPROVAL_REQUIRED -> SUBMITTED`
- no hidden auto-approval
- no retry loop that preserves approval after failure
- no strategy-driven transition into any submit-capable state

## Design Consequence

If a strategy cannot create `APPROVED_FOR_PAPER_SUBMIT`, it cannot place an order.

That is the required safety property.

