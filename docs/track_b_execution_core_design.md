# Track B Execution Core Design

## Status

- Track: B
- Document type: design proposal for review
- Implementation status: not started
- Scope: clean paper execution spine only

This document defines a separate execution core for one narrow milestone: submit and close one 1-lot micro futures trade through the IBKR paper route, then write a durable proof report. It is intentionally not a continuation of the current app dashboard, cache, snapshot, canary, or strategy research architecture.

## Purpose

Track B builds a clean, testable execution core with this path:

```text
canonical bar/event
-> strategy decision
-> order intent
-> execution safety gate
-> IBKR paper submit
-> fill
-> ledger
-> reconciliation
-> close/flatten
```

The first milestone is a command-line PAPER route harness that:

1. Connects to the configured IBKR paper account.
2. Verifies the account, contract allowlist, broker flat state, and open orders.
3. Submits one 1-lot micro futures paper open order.
4. Proves the fill from IBKR broker callbacks and broker truth.
5. Submits the matching close/flatten order.
6. Proves the account is flat with no open orders.
7. Writes an append-only event ledger and a proof report.

## Milestone One Acceptance Criteria

Milestone one has exactly three terminal classifications.

### `TRACK_B_PAPER_PROOF_PASSED`

This classification requires all of the following:

- Explicit configured IBKR paper account.
- Exact allowlisted contract.
- Pre-open broker position is flat for the exact contract.
- Pre-open broker open orders are zero for the account and exact contract.
- Pre-open reconciliation is `CLEAN`.
- Real open order is submitted to IBKR paper.
- Broker order id is captured.
- `permId` is captured when IBKR provides it.
- Real IBKR open fill is observed through broker callbacks and/or execution truth.
- Post-open broker position is exactly `+1` or `-1`, matching the open intent.
- Close order is submitted to IBKR paper.
- Real IBKR close fill is observed through broker callbacks and/or execution truth.
- Final broker position is flat for the exact contract.
- Final broker open orders are zero for the account and exact contract.
- Final reconciliation is `CLEAN`.
- Complete append-only event ledger is written.
- Proof report JSON and Markdown are written.

### `TRACK_B_PAPER_PROOF_BLOCKED`

This classification is used when the harness stops before creating ambiguous broker exposure, or when it performs a clean cancellation of an unfilled open order and proves it is safe to stop.

Examples:

- Invalid config.
- Missing account id.
- No exact paper account match in managed accounts.
- Unallowlisted contract.
- Stale, missing, crossed, locked, or ambiguous quote before submit.
- Risk gate rejection before broker submit.
- Broker unavailable before submit.
- Missing `nextValidId` before submit.
- Clean cancel after an unfilled open order, followed by broker flat/open-orders-clean reconciliation.

### `TRACK_B_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED`

This classification is used after a broker action has been attempted and Track B cannot prove the resulting broker state.

Examples:

- Submit sent but callback truth is incomplete.
- Unknown order status.
- Possible TWS dialog/API block.
- Cancel status uncertain.
- Position or open-order mismatch after submit.
- Close order uncertainty.
- Broker/ledger disagreement.
- Fill observed but position reconciliation does not match.

## Run State Machine

Every run must move through legal states only. Each state transition must be written to the durable ledger before the harness takes the next dependent action.

Legal states:

- `CREATED`
- `CONFIG_VALIDATED`
- `BROKER_CONNECTED`
- `PRE_OPEN_RECONCILED`
- `OPEN_INTENT_CREATED`
- `OPEN_GATE_PASSED`
- `OPEN_SUBMITTED`
- `OPEN_FILLED`
- `POST_OPEN_RECONCILED`
- `CLOSE_INTENT_CREATED`
- `CLOSE_GATE_PASSED`
- `CLOSE_SUBMITTED`
- `CLOSE_FILLED`
- `POST_CLOSE_RECONCILED`
- `FINAL_RECONCILED`
- `PROOF_PASSED`
- `BLOCKED`
- `AMBIGUOUS_MANUAL_REVIEW_REQUIRED`

Legal transitions:

| From | To |
| --- | --- |
| `CREATED` | `CONFIG_VALIDATED` |
| `CREATED` | `BLOCKED` |
| `CONFIG_VALIDATED` | `BROKER_CONNECTED` |
| `CONFIG_VALIDATED` | `BLOCKED` |
| `BROKER_CONNECTED` | `PRE_OPEN_RECONCILED` |
| `BROKER_CONNECTED` | `BLOCKED` |
| `PRE_OPEN_RECONCILED` | `OPEN_INTENT_CREATED` |
| `PRE_OPEN_RECONCILED` | `BLOCKED` |
| `OPEN_INTENT_CREATED` | `OPEN_GATE_PASSED` |
| `OPEN_INTENT_CREATED` | `BLOCKED` |
| `OPEN_GATE_PASSED` | `OPEN_SUBMITTED` |
| `OPEN_GATE_PASSED` | `BLOCKED` |
| `OPEN_SUBMITTED` | `OPEN_FILLED` |
| `OPEN_SUBMITTED` | `BLOCKED` |
| `OPEN_SUBMITTED` | `AMBIGUOUS_MANUAL_REVIEW_REQUIRED` |
| `OPEN_FILLED` | `POST_OPEN_RECONCILED` |
| `OPEN_FILLED` | `AMBIGUOUS_MANUAL_REVIEW_REQUIRED` |
| `POST_OPEN_RECONCILED` | `CLOSE_INTENT_CREATED` |
| `POST_OPEN_RECONCILED` | `AMBIGUOUS_MANUAL_REVIEW_REQUIRED` |
| `CLOSE_INTENT_CREATED` | `CLOSE_GATE_PASSED` |
| `CLOSE_INTENT_CREATED` | `BLOCKED` |
| `CLOSE_GATE_PASSED` | `CLOSE_SUBMITTED` |
| `CLOSE_GATE_PASSED` | `BLOCKED` |
| `CLOSE_SUBMITTED` | `CLOSE_FILLED` |
| `CLOSE_SUBMITTED` | `AMBIGUOUS_MANUAL_REVIEW_REQUIRED` |
| `CLOSE_FILLED` | `POST_CLOSE_RECONCILED` |
| `CLOSE_FILLED` | `AMBIGUOUS_MANUAL_REVIEW_REQUIRED` |
| `POST_CLOSE_RECONCILED` | `FINAL_RECONCILED` |
| `POST_CLOSE_RECONCILED` | `AMBIGUOUS_MANUAL_REVIEW_REQUIRED` |
| `FINAL_RECONCILED` | `PROOF_PASSED` |
| `FINAL_RECONCILED` | `AMBIGUOUS_MANUAL_REVIEW_REQUIRED` |

No broker action may occur unless the immediately prior required state exists in the durable ledger. Specifically, submit cannot occur without `OPEN_GATE_PASSED` or `CLOSE_GATE_PASSED`, and close cannot occur without `POST_OPEN_RECONCILED`.

## Event Taxonomy

The JSONL ledger must use explicit event types. Required event types for milestone one:

- `run_started`
- `config_loaded`
- `config_validated`
- `broker_connected`
- `account_validated`
- `contract_qualified`
- `quote_observed`
- `pricing_decision_created`
- `broker_position_observed`
- `broker_open_orders_observed`
- `reconciliation_created`
- `signal_event_created`
- `order_intent_created`
- `gate_decision_created`
- `submit_attempt_created`
- `broker_order_observed`
- `fill_event_created`
- `cancel_attempt_created`
- `cancel_status_observed`
- `proof_report_written`
- `run_blocked`
- `run_ambiguous_manual_review_required`
- `run_passed`

Every event must include `event_id`, `run_id`, `sequence`, `event_type`, `created_at`, `payload_sha256`, and `payload`. Events that are caused by a previous event must include `causation_id`. Events that belong to the same order lifecycle must include a stable `correlation_id`.

## Non-Goals

Track B must not include:

- Electron UI.
- Schwab dependency.
- Legacy local paper fill path.
- Snapshot fallback as authority.
- ATP global blockers.
- Phase 1 MGC-only hidden gates.
- Broad strategy research framework.
- Production live-money route.

## Authority Model

The execution core has one strict authority order:

1. IBKR broker truth is authoritative for broker account, positions, open orders, order status, and executions.
2. Track B append-only ledger is authoritative for what Track B attempted, observed, and correlated.
3. Proof reports are derived artifacts only.
4. Snapshots and caches are never authority.

If broker truth and the ledger disagree, the system fails closed. It may write a reconciliation result and operator recovery instructions, but it must not create a second order while the prior state is ambiguous.

## Canonical Event Placeholder

The first proof signal may be synthetic and operator-invoked. That is acceptable for milestone one because the goal is to prove the execution spine, not strategy alpha.

Even when synthetic, the proof signal must produce a real durable `SignalEvent`, followed by a real durable `OrderIntent`. Future strategy integration should replace only the `SignalEvent` source. It must not replace, bypass, or fork the order intent, gate, IBKR paper submit, fill, ledger, reconciliation, or close/flatten path.

## Future Strategy Integration Boundary

Future strategy integration may only replace the synthetic `SignalEvent` source.

Strategy code must not:

- Call `ibkr_paper_adapter.submit_limit_order()` or any broker submit method directly.
- Bypass `risk_gate`.
- Write broker events directly.
- Write ledger events that claim broker truth without broker adapter evidence.
- Read Track A dashboard, snapshot, cache, monitor, or local paper artifacts as authority.
- Import Track B adapter internals to create a private route.

All future strategy decisions must enter through:

```text
SignalEvent -> OrderIntent -> risk_gate -> IBKR paper adapter -> FillEvent -> ledger -> reconcile
```

## Mobile / iOS / iPadOS Portability Constraint

Magic is a personal trading system, not a commercial multi-user SaaS product.

The execution core must remain headless and service-friendly. It must not depend on Electron, desktop UI state, local dashboard cache, browser DOM, or macOS-only UI assumptions.

Milestone one still runs locally against TWS paper on `127.0.0.1:7497`.

Future mobile access should be through a secure Magic service/API, not by having iOS or iPadOS talk directly to TWS or IBKR. The broker-facing process should remain local to the machine running TWS/IB Gateway, or become a local broker agent near that broker session.

iPhone and iPad clients should be treated as operator clients for monitoring, approval, and limited command submission. Mobile clients must never be execution authority.

All dangerous actions must still pass server-side through:

```text
SignalEvent -> OrderIntent -> risk_gate -> adapter -> ledger -> reconcile
```

Track B outputs, status, and proof reports should remain JSON-serializable and API-friendly.

Future UI direction should prefer responsive web/PWA or a thin client first. Native iOS and iPadOS can be considered later.

No mobile implementation is part of Track B slice 2.

## Proposed Module Layout

```text
src/mgc_v05l/execution_core/
  __init__.py
  models.py
  risk_gate.py
  ibkr_paper_adapter.py
  ledger.py
  reconcile.py
  harness.py

tests/unit/execution_core/
  test_models.py
  test_risk_gate.py
  test_ledger.py
  test_reconcile.py
  test_harness_flow.py
  test_ibkr_paper_adapter_mapping.py

tests/integration/execution_core/
  test_paper_harness_fixture_flow.py
```

Optional later files, only after the first milestone proves the spine:

```text
src/mgc_v05l/execution_core/
  config.py
  contracts.py
  proof_report.py
```

For the first pass, keep these small or inline in the required modules unless separation is clearly needed.

## Required Models

All models should be plain dataclasses or frozen dataclasses with explicit JSON serialization. IDs must be generated at the boundary where the durable event is created and then carried forward as causation/correlation fields.

### `SignalEvent`

Represents the durable strategy decision event after a canonical bar/event is evaluated.

Required fields:

- `signal_event_id`
- `run_id`
- `source_event_id`
- `bar_id`
- `strategy_id`
- `symbol`
- `contract_key`
- `decision`
- `side`
- `quantity`
- `reason`
- `occurred_at`
- `input_digest`
- `metadata`

The first harness can use a deliberately simple proof signal such as `TRACK_B_PAPER_PROOF_OPEN_LONG`. It should still write a real `SignalEvent` so future strategy decisions plug into the same seam.

### `OrderIntent`

Represents a broker-independent order request produced from a signal.

Required fields:

- `order_intent_id`
- `signal_event_id`
- `run_id`
- `intent_kind`: `OPEN` or `CLOSE`
- `account_id`
- `symbol`
- `contract_key`
- `action`: `BUY` or `SELL`
- `quantity`
- `order_type`
- `limit_price`
- `time_in_force`
- `paper_only`
- `created_at`
- `reason`

First milestone constraints:

- `paper_only` must be true.
- `quantity` must be exactly `1`.
- `order_type` must be `LMT`.
- `time_in_force` must be `DAY`.
- Market orders are rejected.

### `SubmitAttempt`

Represents one attempt to transmit an order intent to IBKR paper.

Required fields:

- `submit_attempt_id`
- `order_intent_id`
- `run_id`
- `account_id`
- `broker`
- `environment`
- `pre_submit_reconciliation_id`
- `open_order_baseline_event_id`
- `request_digest`
- `state`
- `submitted_at`
- `broker_order_id`
- `perm_id`
- `failure_reason`

There must be at most one active submit attempt per unresolved order intent. A failed or ambiguous attempt cannot be retried automatically in the first milestone.

### `CancelAttempt`

Represents one attempt to cancel a broker order that was submitted by the Track B harness but did not fill inside the configured timeout.

Required fields:

- `cancel_attempt_id`
- `run_id`
- `submit_attempt_id`
- `broker_order_id`
- `perm_id`
- `account_id`
- `contract_key`
- `requested_at`
- `observed_cancel_status`
- `confirmed_at`
- `failure_reason`
- `raw`

Cancel invariants:

- One cancel attempt maximum is allowed for an unfilled open order in milestone one.
- No replacement order may be submitted in milestone one.
- Ambiguous cancel status requires manual TWS review.
- No second close/flatten order may be sent if status is ambiguous.
- Raw broker payloads should be retained where practical, including `orderStatus`, `openOrder`, error, and completed-order observations.

### `BrokerOrder`

Represents IBKR broker order truth normalized into Track B.

Required fields:

- `broker_order_event_id`
- `run_id`
- `submit_attempt_id`
- `account_id`
- `broker_order_id`
- `perm_id`
- `client_id`
- `contract_key`
- `action`
- `quantity`
- `order_type`
- `limit_price`
- `status`
- `filled_quantity`
- `remaining_quantity`
- `average_fill_price`
- `observed_at`
- `raw`

Correlation must use account, contract identity, action, quantity, time window, `broker_order_id`, and `perm_id` when present. It must not rely on local order id alone.

### `FillEvent`

Represents IBKR execution truth normalized into Track B.

Required fields:

- `fill_event_id`
- `run_id`
- `submit_attempt_id`
- `order_intent_id`
- `account_id`
- `broker_order_id`
- `perm_id`
- `execution_id`
- `contract_key`
- `action`
- `quantity`
- `price`
- `filled_at`
- `raw`

A fill event is durable only when it can be correlated to the submitted intent and exact allowlisted contract.

### `PositionState`

Represents a position observation from either broker truth or the ledger-derived state.

Required fields:

- `position_state_id`
- `run_id`
- `source`: `BROKER` or `LEDGER`
- `account_id`
- `contract_key`
- `signed_quantity`
- `average_price`
- `open_order_ids`
- `observed_at`
- `source_event_ids`
- `raw`

Broker `PositionState` is used for authority. Ledger `PositionState` is used for comparison and attribution.

### `ReconciliationResult`

Represents one comparison between broker truth and ledger state.

Required fields:

- `reconciliation_id`
- `run_id`
- `stage`: `PRE_OPEN`, `POST_OPEN`, `PRE_CLOSE`, `POST_CLOSE`, `FINAL`
- `status`: `CLEAN`, `BLOCKED`, or `AMBIGUOUS`
- `account_id`
- `contract_key`
- `expected_signed_quantity`
- `broker_position_state_id`
- `ledger_position_state_id`
- `broker_open_order_ids`
- `ledger_open_order_ids`
- `fill_event_ids`
- `issues`
- `required_action`
- `created_at`

Only `CLEAN` allows the harness to continue.

## `risk_gate.py`

`risk_gate.py` owns paper-only safety checks. It should be pure and easy to unit test.

Inputs:

- Environment config.
- Contract allowlist.
- Intended `OrderIntent`.
- Latest broker `PositionState`.
- Latest broker open orders.
- Latest reconciliation result.
- Ledger summary for the current run.

Required checks:

- Mode is `PAPER`.
- Host is `127.0.0.1`.
- Port is `7497`.
- Account exactly matches the configured paper account.
- Account id is present and explicit.
- No account is inferred from the first managed account.
- Config account, order intent account, broker callback account, fill account, and reconciliation account all match exactly.
- Live ports `7496`, `4001`, and `4002` fail closed.
- Contract is explicitly allowlisted by symbol, expiry, exchange, currency, multiplier, and, when known, `con_id` and local symbol.
- Quantity is exactly `1` for the first milestone.
- Order type is `LMT`.
- Time in force is `DAY`.
- No bracket, OCO, parent, child, or algorithmic order fields.
- No market order.
- No open broker orders before opening.
- Broker is flat before opening.
- Broker has exactly the expected 1-lot position before closing.
- Reconciliation is `CLEAN` before every submit.
- Any missing, stale, or contradictory broker truth fails closed.

Outputs:

- `GateDecision` value with `passed`, `blocking_reason`, `checks`, and `event_payload`.

The gate must not read dashboard files, global ATP state, Schwab state, or Track A local paper ledgers.

## Marketable Limit Pricing

The first milestone uses marketable but bounded limit orders. The goal is to prove that the IBKR paper route can submit, fill, reconcile, and close. The goal is not price improvement.

Quote source:

- Quotes must come from the IBKR paper adapter for the exact allowlisted contract.
- The pricing function may use bid, ask, last, and timestamp fields returned by IBKR callbacks.
- No Schwab, dashboard, cache, snapshot, local paper, or research-derived quote may price the proof order.

Required quote freshness:

- The quote must have an observed timestamp from the current harness run.
- The default maximum quote age is 30 seconds.
- The proof report must include quote observed time, age, bid, ask, last, computed mid, tick size, selected limit, and distance checks.

Bid/ask validation:

- Bid and ask must both be present, numeric, and positive.
- Ask must be greater than bid.
- Locked quotes fail closed.
- Crossed quotes fail closed.
- If last is present, it must be positive and must not contradict bid/ask by more than the configured maximum distance.

Tick-size rounding:

- Contract tick size must come from the allowlist or IBKR contract details.
- Limit prices must be rounded to the nearest valid tick in the conservative direction:
  - Buy limits round up to a valid tick.
  - Sell limits round down to a valid tick.
- If tick size is missing, zero, or ambiguous, pricing fails closed.

Buy limit formula:

```text
raw_buy_limit = ask + fill_offset_ticks * tick_size
buy_limit = round_up_to_tick(raw_buy_limit)
```

Sell limit formula:

```text
raw_sell_limit = bid - fill_offset_ticks * tick_size
sell_limit = round_down_to_tick(raw_sell_limit)
```

Default `fill_offset_ticks` is `1`. The offset exists only to make the paper order marketable and bounded.

Maximum allowed distance:

- Compute `mid = (bid + ask) / 2`.
- If last is present and inside the spread, last may be used as an additional reference.
- The selected limit must be within the smaller of:
  - `max_distance_ticks * tick_size`
  - `max_distance_percent` of mid
- Initial defaults: `max_distance_ticks = 10`, `max_distance_percent = 0.25%`.
- If mid cannot be computed, pricing fails closed.

Missing, stale, crossed, locked, or ambiguous quote behavior:

- Missing bid or ask: fail closed.
- Missing timestamp: fail closed.
- Stale quote: fail closed.
- Crossed quote: fail closed.
- Locked quote: fail closed.
- Non-positive quote value: fail closed.
- Missing or ambiguous tick size: fail closed.
- Contradictory bid/ask/last relationship: fail closed.

Market orders remain forbidden. A marketable limit order is still a limit order with an explicit bounded price.

## `ibkr_paper_adapter.py`

`ibkr_paper_adapter.py` owns the IBKR paper transport boundary. It should be the only Track B module that imports optional `ibapi` transport classes.

Required public surface:

```text
connect()
disconnect()
managed_accounts()
require_account(account_id)
qualify_contract(contract_allowlist_entry)
snapshot_positions(account_id, contract_key)
snapshot_open_orders(account_id)
snapshot_executions(account_id, since)
submit_limit_order(order_intent)
cancel_order(broker_order_id)
wait_for_order_update(submit_attempt_id, timeout)
wait_for_fill(submit_attempt_id, timeout)
```

Adapter principles:

- Paper-only config is explicit at construction.
- Submit-capable mode is impossible unless `mode=PAPER`, `host=127.0.0.1`, and `port=7497`.
- The adapter reports raw callback payloads but returns Track B model objects.
- The adapter records `nextValidId` and allocates broker order ids only after IBKR seeds them.
- The adapter handles `openOrder`, `orderStatus`, `execDetails`, `completedOrder`, positions, and account callbacks.
- The adapter does not render reports, read dashboard snapshots, or inspect strategy/canary state.
- The adapter does not synthesize fills.

The first implementation can reuse low-level IBKR package loading patterns from Track A, but should not import the large Track A harness modules directly.

## Broker Callback Correlation Rules

Track B must correlate IBKR callbacks and execution truth to a `SubmitAttempt` using broker and contract evidence, not local assumptions.

Required matching fields:

- `account_id`
- `contract_key`
- `action`
- `quantity`
- `broker_order_id` when known
- `permId` when known
- `execution_id` for fills
- Time window from `submit_attempt.created_at`

Correlation rules:

- Local order id alone is insufficient.
- `broker_order_id` alone is insufficient if account, contract, action, or quantity do not match.
- `permId` strengthens correlation but does not override account or contract mismatch.
- `execution_id` must be unique in the run ledger before a fill is accepted.
- Callback payloads outside the configured submit time window must be ignored or classified as ambiguous if they conflict with expected state.
- Any callback account mismatch fails closed or becomes ambiguous if a submit has already been sent.
- Any fill that cannot be tied to the exact `SubmitAttempt` remains broker evidence but does not become a Track B `FillEvent`.

## TWS Paper Readiness

The harness must prove TWS paper readiness before any submit attempt.

Required explicit configuration:

- Host is explicitly `127.0.0.1`.
- Port is explicitly `7497`.
- Client id is explicitly supplied.
- Paper account id is explicitly supplied.
- Missing account id fails closed.

Managed account validation:

- The IBKR managed account list must contain the configured paper account exactly.
- Multiple managed accounts are allowed only if the configured paper account is present and selected explicitly.
- No implicit first-account selection is allowed.
- The harness must reject any attempt to infer the account from the first managed account.

Callback readiness:

- Missing `nextValidId` before submit readiness fails closed.
- Missing `orderStatus` after submit must be detected and reported.
- Missing `openOrder` after submit must be detected and reported.
- Missing `execDetails` for an expected fill must be detected and reported.
- Missing position refresh after fill must be detected and reported.

TWS dialog or API-block suspicion:

- If submit is sent but expected broker callbacks do not arrive within timeout, the harness must classify the state as ambiguous.
- If IBKR errors, missing callbacks, or unchanged open-order/position truth suggest a TWS precaution dialog or API block, the harness fails closed.
- Suspected TWS dialog/API-block conditions must appear in the proof report with observed callbacks, missing callbacks, IBKR errors, and required manual TWS review action.

## Account Safety

The harness must reject:

- Missing account id.
- Multiple managed accounts with no exact configured match.
- Any attempt to infer account from the first managed account.
- Account mismatch between config and managed accounts.
- Account mismatch between config and order intent.
- Account mismatch between order intent and broker order callbacks.
- Account mismatch between broker order callbacks and fill callbacks.
- Account mismatch between fills and reconciliation snapshots.

Every durable model that carries account context must carry the explicit configured paper account id. Any account mismatch fails closed before another broker action is attempted.

## Operator Preflight Checklist

Before running the real IBKR paper proof, the human operator must verify:

- TWS paper is open.
- TWS is logged into the intended paper account.
- API access is enabled.
- Socket port `7497` is enabled.
- Read-only API mode is disabled if submitting paper orders.
- No modal dialog is blocking API activity.
- The selected client id is not already in conflict.
- Market data is available for the selected contract.
- Account is flat, or expected flat, before proof.
- No existing open orders are present for the account and selected contract.
- The operator understands this is paper only.

## `ledger.py`

`ledger.py` owns append-only durable event writing and replay.

Proposed storage:

```text
outputs/track_b_execution_core/
  <run_id>/
    events.jsonl
    proof_report.json
    proof_report.md
```

Event envelope:

```json
{
  "event_id": "...",
  "run_id": "...",
  "sequence": 1,
  "event_type": "order_intent_created",
  "created_at": "...",
  "causation_id": "...",
  "correlation_id": "...",
  "payload_sha256": "...",
  "payload": {}
}
```

Ledger invariants:

- Every state transition writes an event before continuing.
- Every submit attempt has a durable event id before transmission.
- Every broker observation is written with raw callback payload where practical.
- Replaying the ledger reconstructs Track B's view without reading reports.
- Reports are derived from ledger plus final broker reconciliation.
- Existing Track A `var/paper_strategy_position_ledger.json` is not read.

## `reconcile.py`

`reconcile.py` compares ledger-derived state to broker truth for one account and one exact contract.

Required stages:

- `PRE_OPEN`: broker flat, no open orders, ledger flat.
- `POST_OPEN`: broker long/short exactly 1 according to the open intent, no unresolved open orders after fill.
- `PRE_CLOSE`: broker position exactly matches the ledger-owned open position, no open orders.
- `POST_CLOSE`: broker flat, no open orders, ledger flat after close fill.
- `FINAL`: same as `POST_CLOSE`, with all submit attempts terminal.

Failure classes:

- `BROKER_UNAVAILABLE`
- `ACCOUNT_MISMATCH`
- `CONTRACT_MISMATCH`
- `OPEN_ORDER_UNCERTAINTY`
- `POSITION_MISMATCH`
- `MISSING_FILL`
- `UNOWNED_POSITION`
- `AMBIGUOUS_BROKER_STATUS`
- `LEDGER_CORRUPTION`

Any failure returns `BLOCKED` or `AMBIGUOUS`; the harness stops before any additional submit.

## `harness.py`

`harness.py` is the first operator-facing entrypoint for Track B.

## Configuration Schema

Milestone one config is explicit. Missing fields fail closed.

| Field | Allowed values | Forbidden values / notes |
| --- | --- | --- |
| `mode` | `PAPER` | Any live, production, blank, or unknown mode |
| `host` | `127.0.0.1` | Any non-local host |
| `port` | `7497` | `7496`, `4001`, `4002`, blank, or unknown ports |
| `client_id` | Explicit positive integer | Missing, zero, negative, non-integer, known-conflicting client id |
| `account_id` | Explicit configured IBKR paper account id | Missing, blank, inferred, first managed account |
| `contract_key` | Exact key in Track B allowlist | Missing, blank, unknown, dynamic rollover key |
| `side` | `BUY` or `SELL` | Blank, hold/no-action, unsupported side |
| `quantity` | `1` | `0`, negative, fractional, greater than `1` |
| `order_type` | `LMT` | `MKT`, stop, bracket, OCO, algo, blank |
| `time_in_force` | `DAY` | GTC, IOC, FOK, blank |
| `fill_offset_ticks` | Positive decimal or integer, default `1` | Negative, zero, non-numeric, unbounded |
| `max_quote_age_seconds` | Positive number, default `30` | Zero, negative, missing when no default is provided |
| `max_distance_ticks` | Positive number, default `10` | Zero, negative, unbounded |
| `max_distance_percent` | Positive decimal percent, default `0.25` | Zero, negative, unbounded |
| `open_fill_timeout_seconds` | Positive number | Zero, negative, unbounded |
| `close_fill_timeout_seconds` | Positive number | Zero, negative, unbounded |
| `cancel_timeout_seconds` | Positive number | Zero, negative, unbounded |
| `output_root` | Path under repo-controlled outputs or an explicit operator path | Missing, unwritable, Track A var/output authority path |
| `confirm_paper_only` | Explicit true | Missing, false, implicit confirmation |

Forbidden config behavior:

- Inferring account from IBKR managed account order.
- Inferring contract by front-month lookup or automatic rollover.
- Loading Schwab, dashboard, monitor, ATP, canary, or Track A local paper paths.
- Enabling live-money submission.

Proposed command:

```text
python -m mgc_v05l.execution_core.harness paper-proof \
  --account-id DUM882026 \
  --contract-key MGC-202606 \
  --side BUY \
  --quantity 1 \
  --host 127.0.0.1 \
  --port 7497 \
  --client-id <paper-client-id> \
  --confirm-paper-only
```

The exact CLI shape can be adjusted during implementation, but the command must be explicit, operator-invoked, and paper-only. No scheduler, dashboard, strategy runtime, or canary path should be able to import and call it as a submit shortcut.

### First Milestone Flow

1. Create `run_id` and append `run_started`.
2. Load paper config and contract allowlist.
3. Connect to IBKR paper.
4. Discover managed accounts and require the configured paper account.
5. Qualify the exact allowlisted contract.
6. Capture broker positions and open orders.
7. Reconcile `PRE_OPEN`.
8. Create proof `SignalEvent`.
9. Create open `OrderIntent`.
10. Run `risk_gate` for open.
11. Build bounded marketable limit price from fresh IBKR bid/ask.
12. Append `SubmitAttempt` before broker transmission.
13. Submit one limit order intended to fill in paper.
14. Wait for broker order truth and fill truth.
15. Append `BrokerOrder` and `FillEvent`.
16. Reconcile `POST_OPEN`.
17. Create close `OrderIntent`.
18. Run `risk_gate` for close.
19. Build bounded marketable close limit price from fresh IBKR bid/ask.
20. Append close `SubmitAttempt` before broker transmission.
21. Submit one opposite-side limit order intended to close.
22. Wait for broker order truth and fill truth.
23. Append close `BrokerOrder` and `FillEvent`.
24. Reconcile `POST_CLOSE`.
25. Reconcile `FINAL`.
26. Write proof report JSON and Markdown from the ledger.
27. Disconnect.

If the open order does not fill within the configured timeout but broker truth clearly shows a working order, the harness may issue exactly one cancel for that broker order id, append a durable `CancelAttempt`, reconcile flat, and mark the milestone failed. It must not submit a replacement order. If cancel status or order status is ambiguous, it must stop and require manual TWS review.

If the close order is ambiguous, the harness must not submit another close/flatten order. It writes the ambiguity and required manual action.

## Proof Report

The proof report should include:

- Run id.
- Environment: mode, host, port, client id.
- Account id.
- Contract allowlist entry.
- Quote source, quote freshness, bid, ask, last, mid, tick size, limit formula, selected limit price, and distance checks.
- Event id chain.
- Open intent id, submit attempt id, broker order id, perm id, execution id, fill price, fill time.
- Close intent id, submit attempt id, broker order id, perm id, execution id, fill price, fill time.
- Cancel attempt id and cancel status if an unfilled open order required cancellation.
- Pre-open, post-open, pre-close, post-close, and final reconciliation ids.
- Final broker position state.
- Final open-order state.
- Any IBKR errors observed.
- Missing callback summary, including missing `nextValidId`, `orderStatus`, `openOrder`, `execDetails`, or position refresh when applicable.
- Suspected TWS dialog/API-block conditions and required manual review action when applicable.
- Final classification:
  - `TRACK_B_PAPER_PROOF_PASSED`
  - `TRACK_B_PAPER_PROOF_BLOCKED`
  - `TRACK_B_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED`

## Proof Report Schema

`proof_report.json` must be derived from ledger events plus final broker reconciliation. It must not be manually assembled from transient runtime state.

Required JSON fields:

- `schema_version`
- `classification`
- `run_id`
- `generated_at`
- `environment`
- `configured_account_id`
- `validated_account_id`
- `contract_key`
- `contract`
- `state_machine`
- `event_counts`
- `event_ids`
- `config`
- `quotes`
- `pricing_decisions`
- `pre_open_reconciliation`
- `open_intent`
- `open_submit_attempt`
- `open_broker_order`
- `open_fill`
- `post_open_reconciliation`
- `close_intent`
- `close_submit_attempt`
- `close_broker_order`
- `close_fill`
- `post_close_reconciliation`
- `final_reconciliation`
- `cancel_attempts`
- `broker_errors`
- `missing_callbacks`
- `account_checks`
- `risk_gate_checks`
- `failure_or_ambiguity`
- `required_manual_action`
- `ledger_path`
- `markdown_report_path`

Required Markdown sections:

- `# Track B Paper Proof Report`
- `## Classification`
- `## Environment`
- `## Account Validation`
- `## Contract`
- `## Quote And Pricing`
- `## Event Chain`
- `## Open Order`
- `## Post-Open Reconciliation`
- `## Close Order`
- `## Final Reconciliation`
- `## Cancel Attempt`, when applicable
- `## Broker Errors And Missing Callbacks`
- `## Failure Or Ambiguity`, when applicable
- `## Required Manual Action`, when applicable
- `## Ledger Files`

## Contract Allowlist

The first implementation should use a small explicit allowlist, not a hidden Phase 1 policy gate.

Example shape:

```json
{
  "MGC-202606": {
    "symbol": "MGC",
    "security_type": "FUT",
    "exchange": "COMEX",
    "currency": "USD",
    "contract_month": "202606",
    "expiry": "20260626",
    "local_symbol": "MGCM6",
    "con_id": 712565978,
    "multiplier": "10",
    "max_quantity": 1
  }
}
```

The allowlist can live in code for the first reviewed implementation if that keeps the milestone small, but it must be visible in Track B and covered by tests. It should not import `ibkr_phase1_futures_scope.py`.

The contract allowlist may be static for milestone one. Any expiry, `localSymbol`, or `conId` change must be operator-reviewed before use. No automatic rollover or dynamic contract discovery is allowed in milestone one.

## Existing Track A Code That Can Be Reused Safely

Safe reuse means importing isolated IBKR broker helpers or copying tiny neutral ideas into Track B with tests. It does not mean importing Track A route orchestration or old `src/mgc_v05l/execution/*` modules.

Milestone-one rule:

- Track B may import isolated `src/mgc_v05l/brokers/ibkr/*` transport/model helpers only if the imported helper does not import Track A execution, dashboard, monitor, ATP, Schwab, canary, or local paper fill modules.
- Track B may copy tiny neutral ideas from old execution modules with tests.
- Track B must not import old `src/mgc_v05l/execution/*` modules in milestone one.

- `src/mgc_v05l/brokers/ibkr/ibkr_models.py`: raw IBKR value shapes are reusable as transport-facing records.
- `src/mgc_v05l/brokers/ibkr/ibkr_session.py`: session state and `nextValidId` seeding are reusable if constructed with paper-only config.
- `src/mgc_v05l/brokers/ibkr/ibkr_order_identity.py`: `IbkrOrderIdAllocator` is reusable.
- `src/mgc_v05l/brokers/ibkr/ibkr_contract_resolver.py`: pure contract normalization is reusable behind Track B's explicit allowlist.
- `src/mgc_v05l/brokers/ibkr/ibkr_callback_adapter.py`: read-only callback normalization is a useful pattern and may be reused or extended, but Track B needs submit-aware callbacks too.
- `src/mgc_v05l/brokers/ibkr/ibkr_tws_transport.py`: optional `ibapi` loading and bridge construction pattern is reusable, but Track B should create its own submit-capable adapter surface.
- Selected helper ideas from `src/mgc_v05l/execution/ibkr_manual_paper_submit.py`: raw futures `Contract` construction, minimal futures `LMT DAY` order fields, callback names, and quote-pricing guardrail concepts are useful. Copy or re-express only the tiny neutral pieces with tests; do not import the module directly.
- Existing IBKR guardrail docs under `docs/ibkr_*`: useful safety requirements reference.
- Existing IBKR unit tests: useful examples for callback normalization, truth snapshots, order id seeding, and fail-closed behavior.

## Import / Dependency Boundary

Milestone one has an explicit import boundary.

Allowed imports:

- Python standard library.
- Local Track B modules under `mgc_v05l.execution_core`.
- Isolated `mgc_v05l.brokers.ibkr.*` modules only after confirming they do not import Track A execution, dashboard, monitor, ATP, Schwab, canary, or local paper fill modules.

Forbidden imports:

- `mgc_v05l.execution.*`
- `mgc_v05l.app.*`
- `mgc_v05l.strategy.*`
- `mgc_v05l.market_data.schwab*`
- `mgc_v05l.research.*`
- `validation_layer.*`
- Desktop code.
- Existing Track A `var/` or `outputs/` ledgers, snapshots, dashboard artifacts, monitor artifacts, governance artifacts, ATP artifacts, canary artifacts, Schwab artifacts, or local paper fill artifacts.

Milestone one requires an import-boundary unit test that scans Track B modules and fails if forbidden imports are introduced.

## Track A Code That Must Not Be Reused

These modules caused or preserve authority confusion for Track B's purpose. They should not be imported by `src/mgc_v05l/execution_core/`.

- `src/mgc_v05l/execution/paper_broker.py`: deterministic local paper fills are explicitly out of scope.
- `src/mgc_v05l/execution/ibkr_paper_strategy_bridge.py`: strategy bridge mixes route policy, Phase 1 targets, monitor state, manual harness delegation, and submit authority.
- `src/mgc_v05l/execution/ibkr_paper_strategy_executor.py`: executor depends on monitor status, dashboard snapshots, governance, exposure checks, and Track A ledgers.
- `src/mgc_v05l/execution/ibkr_paper_strategy_monitor.py`: monitor writes and reads persistent strategy ledgers, PnL snapshots, dashboard backend gates, and local fallbacks.
- `src/mgc_v05l/execution/ibkr_paper_strategy_porting.py`: strategy inventory and lane-routing policy are not part of the execution core.
- `src/mgc_v05l/execution/ibkr_paper_strategy_governance.py`: governance and lane approval are Track A concerns.
- `src/mgc_v05l/execution/ibkr_paper_strategy_exposure.py`: exposure policy is coupled to Track A paper lanes.
- `src/mgc_v05l/execution/ibkr_phase1_futures_scope.py`: first Track B contract scope must be an explicit allowlist, not a hidden Phase 1 gate.
- `src/mgc_v05l/execution/broker_truth.py`: do not import directly in milestone one; define Track B-local proof models instead.
- `src/mgc_v05l/execution/broker_requests.py`: do not import directly in milestone one; define Track B-local order intent and adapter request shapes instead.
- Any old execution module that touches Track A paper ledgers, dashboard snapshots, governance, ATP, canary, Schwab, or local paper fills.
- `src/mgc_v05l/strategy/*`: strategy state machine and strategy-side reconciliation are too coupled to the current runtime.
- `src/mgc_v05l/app/probationary_runtime.py`: includes canary lanes, runtime submit bridge coupling, and local fill/synthetic fallback paths.
- `src/mgc_v05l/app/operator_dashboard.py` and `desktop/*`: UI, API snapshots, cache freshness, and dashboard authority are non-goals.
- `src/mgc_v05l/market_data/schwab*` and Schwab config/auth modules: no Schwab dependency.
- `src/mgc_v05l/research/*` and `src/validation_layer/*`: broad research framework is out of scope.
- Existing `var/paper_strategy_position_ledger.json`, `var/per_strategy_paper_status.json`, and `outputs/operator_dashboard/*`: not authority for Track B.

## Additional Production Safety Constraints

These constraints are not all required for the first fake-adapter or read-only
preflight slices, but they are required design boundaries before Track B becomes
an automated production-capable execution core.

### Futures Contract Rollover Safety

- Logical symbols may resolve to candidate contracts, but the executable contract must remain explicit and allowlisted.
- Milestone one allows no automatic rollover.
- Default `roll_cutoff_days` is `14` calendar days before last trade date or expiration.
- New entries are blocked inside the roll window.
- Existing positions inside the roll window require operator review.
- `contract_month`, `expiry`, `local_symbol`, `exchange`, `currency`, `multiplier`, and `con_id` must be internally consistent.
- Broker/front-month convention mismatch fails closed.

### Session Calendar And Holidays

- Track B must not use wall-clock-only market-open assumptions.
- An exchange session calendar is required before automated trading.
- Holiday, early close, and Globex maintenance handling are required before automated trading.
- Durable timestamps are stored in UTC; display timezone is a separate presentation concern.

### Time Authority

- Durable timestamps are stored in UTC.
- Broker, market, and system timestamps are normalized before comparison.
- String timestamp comparisons are forbidden.
- Clock skew should be detected and reported.

### Market Data Authority

- Quotes price orders.
- Bars drive strategy decisions.
- Broker truth drives positions, orders, and fills.
- Dashboard, cache, and snapshot files are never authority.

### Order Policy

- Milestone one allows single-contract `LMT` `DAY` orders only.
- `MKT`, stops, brackets, OCO, parent/child, algo, combo/spread, options, and futures options are forbidden until separately designed.

### Position Ownership

- Track B may only close positions it opened and correlated.
- Unowned broker positions block automation.
- Manual intervention must be recorded and cannot produce a clean proof `PASS`.

### Client ID And Account Isolation

- Account id must be explicit.
- First-account inference is forbidden.
- Read-only preflight and submit proof should use separate client ID ranges.
- Client ID collision fails closed.

### Restart, Reconnect, And Idempotency

- Unresolved prior runs require broker reconciliation and manual review.
- Track B must not automatically resume submit after a crash.
- Only one active run is allowed per account and contract.
- Duplicate submit for the same intent is forbidden.

### Error Severity Taxonomy

- Broker errors must be classified as informational, warning, blocking, ambiguous, or fatal.
- Blocking and ambiguous errors stop automation.

### Risk Envelope

- Milestone one quantity is exactly `1`.
- Milestone one permits one open and one close maximum.
- Future production design requires per-instrument max quantity, notional, loss, and session-attempt limits.

### Mobile / API Safety

- Mobile and iPad clients are operator clients only.
- Server/core remains execution authority.
- All dangerous actions pass the server-side risk gate.
- Future service design must not expose an unauthenticated LAN submit endpoint.

### Secrets And Reports

- Credentials must not be committed to git.
- Local config should be ignored unless an explicitly scrubbed template is being versioned.
- Proof reports may contain account and order data and should not be committed unless intentionally scrubbed.

### Data Retention

- Runtime logs and generated research outputs require a retention policy.
- Huge outputs should not be committed.
- Track B proof ledgers are retained locally but reviewed before sharing.

## Test Plan

Unit tests:

- Import-boundary test rejects forbidden Track A imports from `mgc_v05l.execution_core`.
- Model validation rejects missing IDs, naive datetimes, non-paper orders, invalid quantities, and unsupported order types.
- Risk gate rejects live ports, non-local hosts, account mismatch, unallowlisted contracts, market orders, quantity greater than 1, existing open orders, and non-flat pre-open broker state.
- Pricing rejects missing, stale, locked, crossed, non-positive, far-away, or ambiguous quotes and forbidden market orders.
- Account safety rejects missing account id, implicit first-account selection, and account mismatch across config, intent, callbacks, fills, and reconciliation.
- Ledger appends stable event envelopes and replays position state deterministically.
- Ledger appends durable cancel attempts and proves no replacement order was submitted after cancel.
- Reconciliation returns `CLEAN` only for exact expected broker/ledger alignment.
- Reconciliation fails closed for missing fills, open-order mismatch, unowned broker position, and ambiguous status.
- IBKR adapter mapping converts fake callbacks into `BrokerOrder`, `FillEvent`, and `PositionState` without synthesizing fills.
- IBKR adapter readiness reports missing `nextValidId`, `orderStatus`, `openOrder`, `execDetails`, and position refresh.

Fixture integration tests:

- Full open-close paper-proof flow with a fake adapter.
- Open fill succeeds but post-open reconciliation mismatches, so close is blocked.
- Open order rests and is canceled once, then proof report is blocked.
- Close submit becomes ambiguous, so no second close order is sent.
- Suspected TWS dialog/API-block condition is reported and fails closed.

## Failure / Recovery Matrix

| Failure condition | Classification | Allowed next action | Forbidden next action | Required proof-report content |
| --- | --- | --- | --- | --- |
| Missing config | `TRACK_B_PAPER_PROOF_BLOCKED` | Fix config and start a new run | Submit order | Missing fields and validation checks |
| Wrong host/port | `TRACK_B_PAPER_PROOF_BLOCKED` | Fix host/port and start a new run | Connect or submit | Configured host/port and expected `127.0.0.1:7497` |
| Account mismatch | `TRACK_B_PAPER_PROOF_BLOCKED` before submit, otherwise `TRACK_B_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED` | Inspect account config and broker account truth | Submit another order | Config account, managed accounts, callback accounts, fill accounts |
| No `nextValidId` | `TRACK_B_PAPER_PROOF_BLOCKED` | Fix TWS/API readiness and start a new run | Allocate order id or submit | Missing callback summary and TWS readiness evidence |
| Missing quote | `TRACK_B_PAPER_PROOF_BLOCKED` | Restore market data and start a new run | Price or submit | Quote request, missing fields, contract key |
| Stale quote | `TRACK_B_PAPER_PROOF_BLOCKED` | Refresh quote and start a new run | Price or submit from stale quote | Quote timestamp, age, max age |
| Crossed/locked quote | `TRACK_B_PAPER_PROOF_BLOCKED` | Wait for clean quote and start a new run | Price or submit | Bid, ask, last, validation reason |
| Risk gate rejection | `TRACK_B_PAPER_PROOF_BLOCKED` | Fix rejected precondition and start a new run | Submit order | All gate checks and blocking reason |
| Broker unavailable before submit | `TRACK_B_PAPER_PROOF_BLOCKED` | Restore broker connection and start a new run | Submit order | Connection errors and missing readiness callbacks |
| Open order submitted but no fill | `TRACK_B_PAPER_PROOF_BLOCKED` if order is cleanly canceled, otherwise `TRACK_B_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED` | One cancel attempt if broker truth clearly shows working order | Replacement open order | Submit attempt, broker order observations, fill timeout |
| Open order cancel confirmed | `TRACK_B_PAPER_PROOF_BLOCKED` | Stop run and inspect report | Replacement order | Cancel attempt, cancel status, final flat/open-orders-clean reconciliation |
| Open order cancel ambiguous | `TRACK_B_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED` | Manual TWS review | Replacement order or close order | Cancel attempt, observed statuses, missing callbacks |
| Submit sent but no `orderStatus`/`openOrder` | `TRACK_B_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED` | Manual TWS review | Second submit | Submit attempt, missing callback summary, TWS/API-block suspicion |
| Fill observed but position mismatch | `TRACK_B_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED` | Manual TWS/account review | Close/flatten automation unless explicitly proven safe later | Fill event, broker position state, reconciliation issues |
| Post-open reconciliation mismatch | `TRACK_B_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED` | Manual TWS/account review | Close submit through harness | Post-open reconciliation payload and broker/ledger diff |
| Close submit ambiguous | `TRACK_B_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED` | Manual TWS review | Second close/flatten order | Close submit attempt, callback evidence, open orders, position state |
| Close fill missing | `TRACK_B_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED` | Manual TWS review | Second close/flatten order | Close order status, executions, missing fill evidence |
| Final reconciliation mismatch | `TRACK_B_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED` | Manual TWS/account review | Mark passed or submit another order | Final reconciliation payload, broker state, ledger state |

Real IBKR paper test:

- Manual/operator invoked only after design and implementation review.
- Uses configured paper account and allowlisted contract.
- Produces `TRACK_B_PAPER_PROOF_PASSED` only if final broker state is flat with no open orders.

## Implementation Plan For Paper Route Harness

1. Add `src/mgc_v05l/execution_core/models.py` with the required dataclasses, JSON serialization, and ID helper conventions.
2. Add `ledger.py` with append-only JSONL writing, replay, and proof-report input helpers.
3. Add `risk_gate.py` with pure fail-closed checks and unit tests.
4. Add bounded marketable limit pricing as pure logic inside `risk_gate.py` or a small Track B-local helper.
5. Add `reconcile.py` with proof-specific broker-vs-ledger comparison and unit tests.
6. Add a fake adapter test double inside tests to drive the full harness without IBKR.
7. Add `ibkr_paper_adapter.py` with optional `ibapi` loading, paper-only config validation, callback collection, request methods, limit submit, cancel, and broker truth snapshots.
8. Add `harness.py` CLI flow using dependency injection so tests can run with the fake adapter.
9. Add fixture integration tests for the full open-close flow and the main failure modes.
10. Only after tests and review, run the CLI against TWS paper for the one real proof trade.

No implementation should begin until this design is reviewed or explicitly approved.
