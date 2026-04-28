# IBKR Manual Paper Submit/Cancel State Machine

## Purpose

This state machine defines the narrowest allowed workflow for the first single-order IBKR paper submit/cancel test.

It is design-only. It does not authorize submit implementation.

## States

### `DISCONNECTED`

Meaning:

- no trusted TWS paper session
- no usable preview
- no approval token
- no trusted broker truth

Allowed transitions:

- to `CONNECTED_LOCKED` after paper-only handshake and environment lock pass
- to `FAILED_CLOSED` on any mismatch

### `CONNECTED_LOCKED`

Meaning:

- paper TWS handshake succeeded
- `mode=PAPER`, `host=127.0.0.1`, `port=7497` are proven
- selected account id is known
- submit is still impossible

Required checks:

- account id displayed
- duplicate client id absent
- account truth fresh
- positions truth fresh
- open-order baseline fresh

Allowed transitions:

- to `PREVIEW_GENERATED` after `MGC 202606` qualification and preview success
- to `FAILED_CLOSED` on stale truth or account mismatch
- to `DISCONNECTED` if session drops

### `PREVIEW_GENERATED`

Meaning:

- a preview exists
- exact digest exists
- submit is still impossible

Required preview contents:

- account id
- mode / host / port / client id
- contract details
- action
- quantity
- order type
- limit price
- tif
- delayed quote warning
- open-order baseline
- digest

Allowed transitions:

- to `APPROVAL_REQUIRED` after preview is displayed
- to `FAILED_CLOSED` if preview cannot be proven safe
- to `DISCONNECTED` if session drops

### `APPROVAL_REQUIRED`

Meaning:

- preview exists
- no submit may happen until exact approval is validated

Required approval inputs:

- exact preview digest
- exact typed confirmation phrase
- delayed quote acknowledgment

Allowed transitions:

- to `APPROVED_ONCE` on exact match
- to `FAILED_CLOSED` on mismatch, expiry, or changed preview
- to `DISCONNECTED` if session drops

### `APPROVED_ONCE`

Meaning:

- one paper submit attempt is authorized for one preview only

Required re-checks before any future submit:

- same account id
- same mode / host / port
- same contract
- same qty
- same order type
- same limit price
- same tif
- same digest
- same open-order baseline freshness

Allowed transitions:

- to `SUBMIT_REQUESTED` if all re-checks still pass
- to `FAILED_CLOSED` on any mismatch
- to `DISCONNECTED` if session drops

### `SUBMIT_REQUESTED`

Meaning:

- a future paper submit request has been sent
- success is not assumed

Required follow-up:

- immediately re-read open orders
- verify one matching order appears

Allowed transitions:

- to `OPEN_ORDER_VERIFIED` on broker-truth confirmation
- to `FAILED_CLOSED` on rejection or mismatch
- to `DISCONNECTED` if session drops

### `OPEN_ORDER_VERIFIED`

Meaning:

- the paper order is now visible in broker open-order truth

Required checks:

- broker order id known
- order details match preview
- status captured

Allowed transitions:

- to `CANCEL_REQUESTED` on explicit manual cancel action
- to `FAILED_CLOSED` on stale or contradictory truth
- to `DISCONNECTED` if session drops

### `CANCEL_REQUESTED`

Meaning:

- a manual cancel was requested for a known broker order id

Required checks:

- exact order id still open
- separate cancel confirmation received
- cancel request audited

Allowed transitions:

- to `CANCEL_VERIFYING` after cancel request is sent
- to `FAILED_CLOSED` on cancel-request mismatch
- to `DISCONNECTED` if session drops

### `CANCEL_VERIFYING`

Meaning:

- cancel request was sent
- final state is not yet trusted

Required follow-up:

- re-read open orders
- confirm the order disappeared or is explicitly canceled

Allowed transitions:

- to `CANCEL_VERIFIED` when final state is proven
- to `FAILED_CLOSED` when final state cannot be proven
- to `DISCONNECTED` if session drops

### `CANCEL_VERIFIED`

Meaning:

- broker truth proves the paper order is no longer open

Allowed transitions:

- to `CONNECTED_LOCKED` after cleanup
- to `DISCONNECTED` when session ends

### `FAILED_CLOSED`

Meaning:

- a safety, verification, or truth condition failed

Required behavior:

- invalidate approval
- preserve audit trail
- block any further submit attempts in the session

Allowed transitions:

- to `CONNECTED_LOCKED` only after a full reset and fresh truth refresh
- to `DISCONNECTED` when session ends

## Transition Table

| From | Trigger | To | Fail-Closed Condition |
| --- | --- | --- | --- |
| `DISCONNECTED` | paper TWS handshake succeeds | `CONNECTED_LOCKED` | environment mismatch |
| `CONNECTED_LOCKED` | truth + qualification + preview succeed | `PREVIEW_GENERATED` | stale truth or mismatch |
| `PREVIEW_GENERATED` | preview shown | `APPROVAL_REQUIRED` | unsafe preview |
| `APPROVAL_REQUIRED` | digest + phrase validated | `APPROVED_ONCE` | mismatch or expiry |
| `APPROVED_ONCE` | final re-check passes | `SUBMIT_REQUESTED` | any field changed |
| `SUBMIT_REQUESTED` | open-order truth confirms order | `OPEN_ORDER_VERIFIED` | reject or mismatch |
| `OPEN_ORDER_VERIFIED` | manual cancel requested | `CANCEL_REQUESTED` | target not provable |
| `CANCEL_REQUESTED` | cancel sent | `CANCEL_VERIFYING` | cancel request mismatch |
| `CANCEL_VERIFYING` | broker truth proves final state | `CANCEL_VERIFIED` | final state unproven |
| `CANCEL_VERIFIED` | cleanup reset | `CONNECTED_LOCKED` | cleanup incomplete |
| any state | disconnect | `DISCONNECTED` | no trusted session |
| any state | safety violation | `FAILED_CLOSED` | fail closed |

## Explicit Prohibitions

- no direct `PREVIEW_GENERATED -> SUBMIT_REQUESTED`
- no direct `APPROVAL_REQUIRED -> SUBMIT_REQUESTED`
- no multi-order loop
- no approval reuse
- no auto-cancel assumption
- no strategy-driven path into any submit-capable state

## Safety Property

If the operator cannot produce the exact preview digest and typed phrase for the unchanged preview, the single paper submit cannot happen.

