# Track B Central Trade Registry / Lifecycle Ledger

Status: design proposal, no live wiring

Scope: canonical registry for broker-backed Track B PAPER trade lifecycles. The registry records immutable event chains and derives current lifecycle state from those events. It does not submit, cancel, close, liquidate, restart, recover, or mutate broker state.

## Goal

Create one canonical trade registry that records each broker-backed trade lifecycle as one event chain:

`entry intent -> order submitted -> fill -> managed position -> exit intent -> close order -> close fill -> reconciliation`

The registry replaces scattered lifecycle interpretation with a durable event log plus deterministic projections. Local artifacts may create pending/review events, but only broker-backed evidence can advance a trade into broker-submitted or broker-filled states.

## Principles

- The event log is append-only. Corrections are explicit events, not in-place rewrites.
- A `trade_id` identifies the complete trade lifecycle. A `lifecycle_id` identifies the managed position ownership chain within that trade.
- Local intent is not broker evidence.
- Broker-backed submit/fill/close evidence requires broker identifiers where applicable: `order_id`, `client_id`, `perm_id`, and `exec_id`.
- Broker truth beats local artifacts for order and position facts.
- Managed exits use the original filled contract identity. Entry contract resolver recommendations never rewrite exit contract identity.
- Aggregate placeholders such as `MULTIPLE`, `MISSING`, `UNKNOWN`, or blank account values are diagnostic unless exact lifecycle identity cannot resolve.
- Recovery, adoption, and manual close events are explicit lifecycle events with source artifacts and operator/safety context.
- `TrackBTruthSnapshot` is consumed as read-only authority context. The registry does not make live submit decisions.

## Identity Model

### `trade_id`

Stable identifier for the end-to-end trade lifecycle.

Recommended format:

```text
trade_{account_id}|{conId}|{lane_id}|{entry_action}|{entry_perm_id or order_id}|{entry_timestamp}
```

Rules:

- If entry `perm_id` is known, include it.
- If only broker submit is known and fill is not yet known, use `order_id` plus `client_id`.
- If no broker identifiers exist, do not create a broker-backed `trade_id`; create a pending local-intent event only.
- `trade_id` must not be based solely on symbol, bar time, UI row, or local file path.

### `lifecycle_id`

Stable identifier for managed ownership and exit policy attachment.

Rules:

- Created when broker-backed entry fill is adopted into lifecycle ownership.
- Must preserve `trade_id`.
- Must carry `lane_id`, thesis `strategy_id`, account, `conId`, `localSymbol`, expiry, side/action, qty, entry `perm_id`, and entry `exec_id`.
- May be recovered after restart only from broker-backed fill evidence plus reconciliation.
- Missing `lifecycle_id` blocks managed exits until repaired or manually classified.

Relationship:

```text
trade_id 1 -> 1 lifecycle_id for normal managed entries
trade_id 1 -> many lifecycle_id only for explicit partial/adoption correction cases
lifecycle_id -> trade_id required
```

## Immutable Event Model

```python
class TrackBTradeEventType(StrEnum):
    ENTRY_INTENT_CREATED = "ENTRY_INTENT_CREATED"
    ENTRY_SUBMIT_ATTEMPTED = "ENTRY_SUBMIT_ATTEMPTED"
    ENTRY_ORDER_SUBMITTED = "ENTRY_ORDER_SUBMITTED"
    ENTRY_ORDER_CANCELLED = "ENTRY_ORDER_CANCELLED"
    ENTRY_FILL_RECEIVED = "ENTRY_FILL_RECEIVED"
    ENTRY_FILL_ADOPTED = "ENTRY_FILL_ADOPTED"
    MANAGED_POSITION_OPENED = "MANAGED_POSITION_OPENED"
    MANAGED_EXIT_POLICY_ATTACHED = "MANAGED_EXIT_POLICY_ATTACHED"
    EXIT_INTENT_CREATED = "EXIT_INTENT_CREATED"
    EXIT_SUBMIT_ATTEMPTED = "EXIT_SUBMIT_ATTEMPTED"
    CLOSE_ORDER_SUBMITTED = "CLOSE_ORDER_SUBMITTED"
    CLOSE_ORDER_CANCELLED = "CLOSE_ORDER_CANCELLED"
    CLOSE_FILL_RECEIVED = "CLOSE_FILL_RECEIVED"
    RECONCILED_FLAT = "RECONCILED_FLAT"
    RECONCILIATION_BLOCKED = "RECONCILIATION_BLOCKED"
    RECOVERY_ADOPTED = "RECOVERY_ADOPTED"
    RECOVERY_REJECTED = "RECOVERY_REJECTED"
    MANUAL_CLOSE_OBSERVED = "MANUAL_CLOSE_OBSERVED"
    MANUAL_CLOSE_RECONCILED = "MANUAL_CLOSE_RECONCILED"
    LOCAL_ARTIFACT_RETIRED = "LOCAL_ARTIFACT_RETIRED"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"


@dataclass(frozen=True)
class TrackBTradeEvent:
    event_id: str
    event_type: TrackBTradeEventType
    event_ts: datetime
    trade_id: str | None
    lifecycle_id: str | None
    runtime_generation_id: str | None
    lane_id: str | None
    thesis_strategy_id: str | None
    account_id: str | None
    symbol: str | None
    con_id: int | None
    local_symbol: str | None
    expiry: str | None
    side: str | None
    action: str | None
    qty: Decimal | None
    order_id: str | None
    client_id: str | None
    perm_id: str | None
    exec_id: str | None
    price: Decimal | None
    source_component: str
    source_artifact_path: str
    source_artifact_ts: datetime | None
    truth_snapshot_id: str | None
    broker_backed: bool
    reason_codes: tuple[str, ...]
    metadata: Mapping[str, Any]
```

## Registry Schema

```python
@dataclass(frozen=True)
class TrackBTradeRegistry:
    schema_version: Literal["track_b_trade_registry_v1"]
    generated_at: datetime
    registry_id: str
    event_log_path: str
    trade_count: int
    open_trade_count: int
    review_required_count: int
    trades: tuple[TrackBTradeRecord, ...]
    source_truth_snapshot_id: str | None


@dataclass(frozen=True)
class TrackBTradeRecord:
    trade_id: str
    lifecycle_id: str | None
    current_state: TrackBTradeState
    lane_id: str
    thesis_strategy_id: str
    account_id: str
    symbol: str
    con_id: int
    local_symbol: str
    expiry: str
    entry_action: str
    exit_action: str | None
    qty: Decimal
    entry_order_id: str | None
    entry_client_id: str | None
    entry_perm_id: str | None
    entry_exec_id: str | None
    close_order_id: str | None
    close_client_id: str | None
    close_perm_id: str | None
    close_exec_id: str | None
    entry_price: Decimal | None
    close_price: Decimal | None
    realized_pnl: Decimal | None
    managed_exit_policy_id: str | None
    submit_intent_ownership_id: str | None
    reconciliation_id: str | None
    first_event_ts: datetime
    last_event_ts: datetime
    event_ids: tuple[str, ...]
    source_artifact_paths: tuple[str, ...]
    review_required: bool
    reason_codes: tuple[str, ...]


class TrackBTradeState(StrEnum):
    LOCAL_INTENT_ONLY = "LOCAL_INTENT_ONLY"
    SUBMIT_ATTEMPTED = "SUBMIT_ATTEMPTED"
    ENTRY_ORDER_WORKING = "ENTRY_ORDER_WORKING"
    ENTRY_CANCELLED_NO_FILL = "ENTRY_CANCELLED_NO_FILL"
    OPEN_UNADOPTED_FILL = "OPEN_UNADOPTED_FILL"
    OPEN_MANAGED = "OPEN_MANAGED"
    OPEN_MANAGED_EXIT_DUE = "OPEN_MANAGED_EXIT_DUE"
    CLOSE_ORDER_WORKING = "CLOSE_ORDER_WORKING"
    CLOSED_FLAT_BROKER_BACKED = "CLOSED_FLAT_BROKER_BACKED"
    CLOSED_FLAT_MANUAL_RECONCILED = "CLOSED_FLAT_MANUAL_RECONCILED"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
```

## Broker-Backed Evidence Rules

Broker-backed submit/order evidence:

- `ENTRY_ORDER_SUBMITTED` and `CLOSE_ORDER_SUBMITTED` require `order_id` and `client_id`.
- If a broker returns `perm_id` at order time, preserve it.
- Local submit-attempt artifacts without broker ids remain `ENTRY_SUBMIT_ATTEMPTED` or `EXIT_SUBMIT_ATTEMPTED`.

Broker-backed fill evidence:

- `ENTRY_FILL_RECEIVED` and `CLOSE_FILL_RECEIVED` require `exec_id`.
- `perm_id` is required when the broker/API supplies it for the instrument/order.
- Fill event must include account, `conId`, `localSymbol`, action, qty, price, and timestamp.
- Local `paper-*` rows without `perm_id`/`exec_id` cannot create or close broker-backed trades.

Manual close evidence:

- Manual close is not inferred from lifecycle flat alone.
- It requires broker truth showing the position flat plus execution/fill evidence or reconciliation classification explaining the manual close.
- Manual close events preserve the original lifecycle identity and mark `source_component=broker_reconciliation` or `operator_review`.

## Event Chain Requirements

### Entry Intent

Required:

- `runtime_generation_id`
- `lane_id`
- thesis `strategy_id`
- side/action
- qty
- source artifact path/timestamp

Creates:

- `ENTRY_INTENT_CREATED`
- no broker-backed `trade_id` unless broker ids already exist

### Order Submitted

Required:

- `order_id`
- `client_id`
- account
- `conId`
- `localSymbol`
- expiry
- action
- qty

Creates or updates:

- broker-backed pending `trade_id`
- `ENTRY_ORDER_SUBMITTED`

### Entry Fill

Required:

- `order_id`
- `client_id`
- `perm_id` where available
- `exec_id`
- account
- `conId`
- `localSymbol`
- action
- qty
- fill price

Creates:

- `ENTRY_FILL_RECEIVED`
- `trade_id` if not already assigned

### Managed Position

Required:

- `trade_id`
- `lifecycle_id`
- entry `perm_id`/`exec_id`
- exact account
- exact contract identity
- managed exit policy id

Creates:

- `ENTRY_FILL_ADOPTED`
- `MANAGED_POSITION_OPENED`
- `MANAGED_EXIT_POLICY_ATTACHED`

### Exit Intent / Close Order / Close Fill

Required:

- exact `trade_id`
- exact `lifecycle_id`
- exact account
- original filled `conId`, `localSymbol`, expiry
- close action and qty
- entry `perm_id`/`exec_id`

Close fill requires:

- close `order_id`
- close `client_id`
- close `perm_id`
- close `exec_id`

Creates:

- `EXIT_INTENT_CREATED`
- `CLOSE_ORDER_SUBMITTED`
- `CLOSE_FILL_RECEIVED`
- `RECONCILED_FLAT` when broker/lifecycle reconciliation confirms flat

## Derived Current State

Current state is derived by folding immutable events by `trade_id`.

Rules:

- No event is deleted or rewritten.
- Later events can supersede earlier local-only events.
- Broker-backed close fill plus clean reconciliation derives `CLOSED_FLAT_BROKER_BACKED`.
- Broker flat without close fill derives `REVIEW_REQUIRED` unless manual close reconciliation evidence exists.
- Entry fill without lifecycle adoption derives `OPEN_UNADOPTED_FILL`.
- Close intent without close order derives `OPEN_MANAGED_EXIT_DUE` or `REVIEW_REQUIRED` depending on blocker.
- Entry order cancelled without fill derives `ENTRY_CANCELLED_NO_FILL`.

## Linkages

### Reconciliation Linkage

Each registry projection records:

- latest broker/lifecycle reconciliation artifact path
- reconciliation generated timestamp
- reconciliation classification
- blocker ids/reason codes
- broker position count and lifecycle position count used

`RECONCILED_FLAT` requires reconciliation evidence, not lifecycle projection alone.

### Managed Exit Linkage

Managed exit events carry:

- `managed_exit_policy_id`
- hold/exit policy version
- due timestamp or completed-bar count
- close intent id
- close order id/client id when submitted
- blocker if not submitted

Wrong/missing policy mappings become `REVIEW_REQUIRED` events, not silent projection edits.

### Submit-Intent Ownership Linkage

Submit-intent ownership rows link local submit attempts to broker effects:

- `submit_intent_ownership_id`
- submit attempt id
- expected order/action/qty
- broker order id if observed
- broker fill ids if observed
- timeout/no-broker-effect classification
- superseded/retired classification

Stale local submit-intent artifacts can be retired only by explicit `LOCAL_ARTIFACT_RETIRED` events referencing broker/lifecycle truth.

### Recovery / Adoption / Manual Close Events

Recovery/adoption/manual events are first-class:

- `RECOVERY_ADOPTED`: restart recovered a broker-backed lifecycle row from exact fill/position truth.
- `RECOVERY_REJECTED`: candidate row lacked exact broker-backed evidence.
- `MANUAL_CLOSE_OBSERVED`: broker truth/fills show operator or external close.
- `MANUAL_CLOSE_RECONCILED`: lifecycle and broker flat after manual close review.
- `LOCAL_ARTIFACT_RETIRED`: stale/no-broker-effect local artifact is no longer current exposure.

Each requires source artifact path, truth snapshot id, and reason codes.

## Consumption Of TrackBTruthSnapshot

The registry consumes `TrackBTruthSnapshot` as read-only authority context:

- broker truth section: open broker orders and broker positions
- lifecycle section: open lifecycle positions, exact owner rows, aggregate placeholders
- reconciliation section: clean/dirty/flat status
- broker-backed evidence section: fill evidence validity
- contract status section: new-entry eligibility vs close-only exit allowance
- Safe-State and Control Plane sections: authority context for event diagnostics only
- conflicts: create `REVIEW_REQUIRED` events when truth conflicts affect a trade

The registry must not use the snapshot to mutate live state. It only records and derives.

## Simulation Harness Coverage

The lifecycle simulation harness should produce synthetic registry event chains for:

- clean full lifecycle
- passive entry cancel
- entry fill not adopted
- managed exit policy wrong bar count
- missing lifecycle id
- lane/thesis mismatch
- aggregate `MULTIPLE` with exact row valid
- stale Control Plane
- Safe-State blocked
- planner snapshot mismatch
- scoped cleanup extra fields
- local paper artifact without `perm_id`/`exec_id`
- contract close-only: new entry blocked, exit allowed

Expected registry outcomes:

| Harness scenario | Registry expectation |
| --- | --- |
| `clean_full_lifecycle` | one `CLOSED_FLAT_BROKER_BACKED` trade |
| `passive_entry_cancel` | one `ENTRY_CANCELLED_NO_FILL` chain, no broker-backed fill |
| `entry_fill_not_adopted` | `OPEN_UNADOPTED_FILL` plus review-required reconciliation event |
| `managed_exit_policy_wrong_bar_count` | `OPEN_MANAGED_EXIT_DUE` plus policy conflict review event |
| `managed_exit_due_missing_lifecycle_id` | review-required missing lifecycle identity |
| `lane_id_vs_thesis_strategy_id_mismatch` | review-required identity mismatch |
| `managed_exit_close_identity_contract_mismatch` | review-required exact lifecycle identity mismatch |
| `aggregate_account_multiple_exact_row_valid` | exact owner resolved; no review solely from `MULTIPLE` |
| `stale_control_plane_snapshot` | review-required authority context; no broker fact inferred |
| `safe_state_submit_blocked` | submit blocked context attached to event chain |
| `planner_snapshot_mismatch` | planner/supervisor review event attached |
| `scoped_cleanup_extra_diagnostic_fields` | hard identity match, diagnostic fields ignored |
| `local_paper_artifact_without_broker_ids` | local-only event, no broker-backed trade state |
| `contract_close_only_new_entry_blocked_exit_allowed` | entry blocked, lifecycle exit allowed on original contract |

## Migration Path From Current Artifacts

1. **Spec only:** land this design.
2. **Pure model module:** add dataclasses and deterministic fold/projection functions.
3. **Fixture-backed importer:** import current artifacts without writing live outputs:
   - `outputs/track_b_execution_core/paper_trade_ledger/track_b_paper_trade_ledger.jsonl`
   - `latest_track_b_paper_trade_summary.json`
   - `latest_track_b_live_position_status.json`
   - managed lifecycle reports under `track_b_strategy_managed_paper_lifecycle`
   - managed position/order registries
   - submit-intent ownership JSONL
   - broker reconciliation report
   - broker truth snapshots
   - `TrackBTruthSnapshot`
4. **Simulation-backed tests:** generate synthetic registry events from lifecycle harness scenarios.
5. **Read-only report:** emit registry projection JSON under a new `central_trade_registry` output path.
6. **Operator display adoption:** display registry projection as diagnostic/current-state read model.
7. **Governance/managed-exit adoption:** only after parity, consume registry identity for managed closes.
8. **Legacy artifact deprecation:** keep old summaries as projections, not authority.

## Test Plan

Model tests:

- Missing broker ids cannot create broker-backed submit/fill events.
- `trade_id` is stable across entry submit, fill, managed position, close fill, and reconciliation.
- `lifecycle_id` must link to exactly one active managed owner unless an explicit correction event exists.
- Exact lifecycle identity permits close when aggregate account is `MULTIPLE`.
- True account, contract, side, or qty mismatch derives `REVIEW_REQUIRED`.
- Entry close-only contract blocks new entry but permits exit on original filled contract.
- Manual close events require broker truth/reconciliation evidence.
- Recovery adoption requires broker-backed fill or broker position truth.

Projection tests:

- Clean full lifecycle derives `CLOSED_FLAT_BROKER_BACKED`.
- Passive cancel derives `ENTRY_CANCELLED_NO_FILL`.
- Entry fill not adopted derives `OPEN_UNADOPTED_FILL`.
- Close fill without clean reconciliation remains review-required.
- Stale local submit-intent with no broker effect can be retired only by explicit event.

Snapshot integration tests:

- `TrackBTruthSnapshot` conflict classifications produce registry review events.
- Broker truth beats local artifact rows.
- Local `paper-*` artifacts without `perm_id`/`exec_id` remain local-only.
- Safe-State submit blocked is recorded as context, not as a broker fact.
- Stale Control Plane is recorded as context, not as broker/lifecycle state.

Simulation tests:

- Every lifecycle harness scenario maps to the expected registry state table above.
- Dry-run and simulated-live harness modes produce identical registry event chains.

## Non-Goals For Initial Implementation

- No broker adapter calls.
- No runtime restart.
- No submit/cancel/close/liquidation behavior.
- No governance/exposure/managed-exit wiring.
- No replacement of current ledger artifacts until read-only parity is proven.
