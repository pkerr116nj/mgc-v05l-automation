# Track B Lifecycle Simulation Harness

## Purpose

The Track B lifecycle simulation harness exercises the strategy-to-reconciliation handoff contract with synthetic broker-backed evidence. It does not rely on live market timing, does not call IBKR, and does not place, cancel, modify, close, or liquidate real broker orders.

The harness exists to prove that dry-run and simulated-live paths validate the same identity and authority contract before production code is allowed to rely on that handoff.

## Typed Contexts

Each context carries the required Track B identity and authority envelope:

- `lane_id`
- `thesis_strategy_id`
- `lifecycle_id` where applicable
- `account_id`
- `conId`
- `localSymbol`
- `expiry`
- `side` and `action`
- `qty`
- `order_id` and `client_id` where applicable
- `perm_id` and `exec_id` for broker-backed fill evidence
- `runtime_generation_id`
- `control_plane_snapshot_id`
- `safe_state_snapshot_id`
- `contract_resolver_status`
- `source_artifact_path`
- `source_artifact_timestamp`

The implemented contexts are:

- `EntryIntentContext`
- `BrokerOrderContext`
- `FillEvidenceContext`
- `ManagedPositionContext`
- `ExitIntentContext`
- `CloseFillContext`
- `ReconciliationContext`

## Shared Validation Boundary

`validate_lifecycle_simulation_scenario` is the single validation function for both:

- `DRY_RUN`
- `SIMULATED_LIVE_PATH`

Both modes call the same identity checks and the existing Track B pre-action snapshot validator. This prevents a repeat of dry-run passing while the live bridge path fails on a different authority interpretation.

## Fail-Closed Conditions

The harness fails closed when any required identity or authority field is missing or incoherent, including:

- missing `lifecycle_id` for managed exits
- missing or mismapped managed-exit policy/bar count
- close intent identity that does not match the exact lifecycle owner
- missing broker-backed `perm_id` or `exec_id`
- local paper artifacts trying to imply fills
- stale Control Plane snapshot
- Safe-State submit block
- planner snapshot mismatch
- lane/thesis identity mismatch
- close-only contract selected for a new entry

Aggregate placeholders such as `MULTIPLE` are diagnostic only. They cannot block an exact lifecycle close when the broker-backed lifecycle row resolves the true account, contract, side, and quantity.

## Contract Resolver Rule

The contract resolver gate applies to new entries. Existing lifecycle exits use the original filled contract. A close-only contract status blocks the simulated new entry while still allowing validation of the exit identity on the original lifecycle contract.

## Scenario Matrix

| Scenario | Expected outcome |
| --- | --- |
| `clean_full_lifecycle` | Entry submit, broker-backed fill, managed hold, timebox exit, close fill, reconciled flat |
| `passive_entry_cancel` | Terminal cancelled before fill |
| `entry_fill_not_adopted` | Broker-backed fill exists but lifecycle does not adopt it |
| `managed_exit_policy_wrong_bar_count` | Fails closed when the exit policy maps to the wrong completed-bar count |
| `managed_exit_due_missing_lifecycle_id` | Fails closed on missing lifecycle identity |
| `lane_id_vs_thesis_strategy_id_mismatch` | Fails closed on strategy/lane mismatch |
| `managed_exit_close_identity_contract_mismatch` | Fails closed when close identity does not match the original lifecycle contract |
| `aggregate_account_multiple_exact_row_valid` | Passes using exact lifecycle row; aggregate `MULTIPLE` is diagnostic |
| `stale_control_plane_snapshot` | Fails closed before lifecycle handoff |
| `safe_state_submit_blocked` | Fails closed on Safe-State submit authority |
| `planner_snapshot_mismatch` | Fails closed on planner/control-plane mismatch |
| `scoped_cleanup_extra_diagnostic_fields` | Passes when hard identity matches and extra fields are diagnostic |
| `local_paper_artifact_without_broker_ids` | Fails closed because local artifacts are not broker-backed fills |
| `contract_close_only_new_entry_blocked_exit_allowed` | New entry blocked; lifecycle exit remains allowed on original filled contract |

## Safety Invariants

- `broker_mutation_allowed=false`
- `lifecycle_mutation_allowed=false`
- `ibkr_mutation_allowed=false`
- `simulated_only=true`
- no IBKR adapter import
- no live-money path
- no `paper_proof` path
- no direct submit/cancel/close/liquidation behavior
