# IBKR Unattended Paper Order Design

- classification: `IBKR_UNATTENDED_PAPER_DESIGN_NEEDS_REVIEW`
- scope: design only
- implementation status: no unattended submit logic implemented
- strategy linkage: none
- scheduler linkage: none
- live trading: not enabled

## Objective

Design a paper-only unattended order harness that can place and close `MGC` paper orders without typed approval or human TWS dialog handling, while preserving hard fail-closed boundaries and keeping the path completely separate from strategy execution.

## Core Answer

Removing manual approval is only acceptable if all of the following are true at the same time:

1. The harness is locked to `mode=PAPER`, `host=127.0.0.1`, `port=7497`, and `account=DUM882026`, with fail-closed behavior on any mismatch.
2. The unattended path is a separate command surface, explicitly enabled by an operator-only flag such as `--unattended-paper`, and unreachable from ATP/GC, any strategy caller, or any scheduler.
3. TWS is configured so API paper orders do not stop on precaution dialogs. If TWS still blocks on a warning dialog, the harness must fail closed and refuse unattended mode.
4. The submitted order uses the exact qualified futures contract object and a minimal futures-safe order payload.
5. Post-submit broker truth is verified using `orderStatus`, `openOrder`, `execDetails`, `completedOrder`, `positions`, and `permId` correlation rather than local order id alone.
6. The harness enforces one active order max, one position max, no pyramiding, no long-to-short flip, and no second order unless it is an explicit close/flatten step inside the same paper-only state machine.

## Classification Rationale

`IBKR_UNATTENDED_PAPER_DESIGN_NEEDS_REVIEW` is the right classification for this design pass because the manual paper open-close loop is proven, but unattended paper still depends on a TWS configuration assumption that has not yet been proven in a dedicated no-dialog paper rest/cancel run. The missing proof is not architectural uncertainty inside the repo; it is confirmation that the exact TWS paper profile can accept API orders without precaution dialogs while preserving the intended safety envelope.

## Required Environment Lock

The unattended harness must hard-fail unless all of these match:

- `mode=PAPER`
- `host=127.0.0.1`
- `port=7497`
- `account=DUM882026`
- `read_only=false` for the unattended submit path itself

Fail closed on:

- live TWS port `7496`
- IB Gateway live port `4001`
- IB Gateway paper port `4002`
- any unknown port
- any account mismatch
- any ambiguity in managed-account discovery
- any inability to verify paper mode through configured mode plus port plus explicit operator configuration

The current read-only verifier remains separate and should still be runnable with `read_only=true`.

## Unattended Command Boundary

The unattended paper path must be separate from the manual harness and separate from any future strategy bridge.

Required boundary:

- dedicated CLI path such as `mgc_v05l.app.ibkr_unattended_paper_order`
- explicit operator flag such as `--unattended-paper`
- explicit `--paper-only` or equivalent lock in addition to normal environment checks
- no import path that exposes a submit-capable object to ATP/GC modules
- no import path that exposes a submit-capable object to scheduler modules
- no shared helper that silently upgrades a read-only path into a submit-capable path

Runtime caller checks must fail closed for:

- `strategy`
- `atp`
- `scheduler`
- `autonomous`
- any caller path not matching the dedicated unattended paper CLI

Every unattended run must write an append-only audit log, even when blocked before connect.

## TWS / API Assumptions

The design depends on these TWS assumptions being true for the unattended paper workstation:

### Required

- `Global Configuration > API > Settings > Enable ActiveX and Socket Clients` is enabled.
- `Global Configuration > API > Settings > Socket Port` is `7497`.
- `Read-Only API` is disabled for the unattended submit path. Official IBKR docs note that read-only mode blocks API order handling and even order information visibility in ways that are incompatible with unattended submit verification.

### Required for No-Dialog Operation

- `Global Configuration > API > Precautions > Bypass Order Precautions for API orders` is enabled.

### Operationally Recommended

- the TWS paper session uses a dedicated profile for unattended paper only
- the unattended paper profile is not used for live trading
- TWS lock / restart behavior is configured so the API session does not silently stall during unattended windows

### Fail-Closed Assumption

If a precaution or confirmation dialog still appears despite the expected TWS settings, unattended mode is not allowed to continue. The harness must:

- wait only for broker callbacks, not for human input
- classify the run as blocked or ambiguous when `openOrder` / `orderStatus` truth does not appear within the configured window
- report that a manual TWS dialog likely blocked unattended operation
- stop without sending a second order

## Order Construction Rules

Unattended paper orders must reuse the proven manual paper contract and order construction rules.

### Contract

- `MGC` only
- friendly display label may remain `MGC 202606`
- submitted payload must use the exact qualified contract:
- `conId=712565978`
- `lastTradeDateOrContractMonth=20260626`
- `localSymbol=MGCM6`
- exact qualified `exchange`, `currency`, `multiplier`, and `tradingClass`
- no shorthand expiry mixed with `conId`

### Order

- `qty <= 1`
- `LMT` only
- `DAY` only
- no market orders initially
- no bracket/OCO initially
- no unsupported stock-routing fields
- no `eTradeOnly`
- no `firmQuoteOnly`
- no `nbboPriceCap`
- no `auctionStrategy`
- no `discretionaryAmt`
- `outsideRth=false` unless a later design explicitly changes that for paper futures

### Allowed Price Modes

- unattended rest/cancel mode: near-market but non-marketable limit
- unattended fill mode: marketable limit derived from delayed quote
- unattended close mode: marketable `SELL` limit derived below delayed bid/last

Far-away placeholder prices remain forbidden.

## Broker-Truth Verification Model

The unattended harness must not rely on local order id alone.

The identity and verification model must correlate on:

- `account_id`
- `permId` when available
- exact `conId`
- exact `localSymbol`
- exact expiry
- action
- quantity
- fill / callback time window

Required broker-truth sources:

- `orderStatus`
- `openOrder`
- `reqOpenOrders`
- `reqAllOpenOrders`
- `reqExecutions`
- `reqCompletedOrders` when supported
- `reqPositions`
- `reqAccountUpdates` and `updatePortfolio` as supplementary truth

The harness should treat `permId` as the highest-confidence durable identifier after submit.

## Failure Handling

### Rejected Order

- stop immediately
- classify with exact IBKR error code and message
- no retry
- no second order

### No Fill

- attempt cancel
- verify cancel with broker truth
- report remaining exact position

### Ambiguous Status

- stop
- classify as requiring manual TWS review
- report order id, perm id, latest callback evidence, and current position snapshot

### Working Order Left Behind

- report exact identifiers:
- order id
- perm id
- contract
- status
- quantity remaining
- no follow-on order

### Disconnect After Submit

- reconnect read-only first
- reconcile current open orders, executions, completed orders, and positions
- do not allow a second unattended order until state is reconciled cleanly

### Position Mismatch

- fail closed
- report whether execution truth says filled while position truth is stale, missing, or contradictory
- require operator review before any further unattended run

## Safety Limits

The unattended paper harness must enforce all of these:

- one active `MGC` order max
- one exact `MGC` position max
- no pyramiding
- no flip from long to short
- no second order unless explicitly in close/flatten mode inside the same paper-only workflow
- daily paper order count cap
- kill-switch file such as `var/ibkr_unattended_paper.disabled`
- explicit runtime halt flag such as `--kill-switch-path`

Utility design allowed later, still paper only:

- `cancel-all-open-mgc`
- `flatten-one-mgc`

Both utilities should be separate explicit commands, not implicit recovery side effects.

## Transition Plan

### Step 1

Unattended paper rest/cancel test.

Goal:

- prove no-dialog TWS behavior
- verify openOrder / orderStatus / cancel loop
- verify a deliberately resting order is visible in TWS while it is still working
- record operator confirmation of TWS visibility before cancel

### Step 2

Unattended paper fill test.

Goal:

- prove marketable-limit fill with no human handling
- verify executions and completed-order correlation

### Step 3

Unattended paper open-close test.

Goal:

- prove the same paper loop now works without typed approval or TWS dialog handling

### Step 4

Paper strategy-intent bridge, still not autonomous strategy execution.

Goal:

- allow a reviewed intent envelope to call the unattended paper harness without exposing raw broker submit primitives to strategy code

### Step 5

Only later consider a live manual / supervised path.

Goal:

- entirely separate design and approval process

## Recommended First Acceptance Gate

Before any implementation is allowed, require one paper-only acceptance checklist:

- confirm TWS paper profile uses `7497`
- confirm unattended paper profile is not live-capable
- confirm `Read-Only API` is off for unattended submit testing
- confirm `Bypass Order Precautions for API orders` is on
- prove one unattended paper rest/cancel cycle with no dialog
- prove one unattended paper fill cycle with exact broker-truth correlation

For the resting-order acceptance step, require all of the following:

- submit one non-marketable near-market `LMT DAY` order
- verify API `openOrder` / `orderStatus` shows a working / submitted order
- pause long enough for operator visual confirmation that the order is visible in TWS Orders / Activity
- record `visible_in_tws=true/false`
- cancel the order
- verify API cancel truth
- allow operator confirmation that the order disappeared or showed canceled in TWS

Use these acceptance classifications:

- `WORKING_ORDER_API_AND_TWS_VISIBLE`
- `WORKING_ORDER_API_VISIBLE_TWS_NOT_CONFIRMED`
- `WORKING_ORDER_FILLED_BEFORE_VISUAL_CONFIRMATION`
- `WORKING_ORDER_REJECTED`
- `WORKING_ORDER_CANCEL_FAILED`

Fast marketable fills should not be mistaken for missing visibility. TWS resting-order visibility is only expected when the order actually rests.

Only after that should unattended paper submit code be written.

## Sources

- [IBKR API Initial Setup](https://interactivebrokers.github.io/tws-api/initial_setup.html)
- [IBKR Campus TWS API Docs: Order Precautions](https://ibkrcampus.com/campus/ibkr-api-page/twsapi-doc/)
- [IBKR Third Party API Platforms](https://interactivebrokers.github.io/tws-api/third_party.html)
- [IBKR Order Submission Notes](https://interactivebrokers.github.io/tws-api/order_submission.html)
