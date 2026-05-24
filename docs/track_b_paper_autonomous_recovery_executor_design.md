# Track B PAPER Autonomous Recovery Executor Design

## Purpose

Track B PAPER is a bounded autonomous failure-discovery environment. The recovery executor path must therefore expose abnormal behavior, preserve evidence, and recommend bounded next steps without turning routine PAPER failures into permanent human-gated holds.

Version 1 is a dry-run planner only. It does not start or stop runtimes, restart producers, submit orders, cancel orders, replace orders, modify orders, close positions, flatten exposure, or mutate lifecycle artifacts.

## Authority Boundary

The planner consumes execution_core authority artifacts only. Dashboard/operator projections are display-only and must never be used as routing, restart, readiness, order, broker, or lifecycle authority.

Every future autonomous recovery executor must use the execution_core Control Plane Snapshot as the single coherent pre-action evidence packet. The snapshot must be captured immediately before the action boundary and must prove that Shared Truth, Runtime Supervisor Authority, PAPER Recovery Policy, and the dry-run Autonomous Recovery Plan were built from one coherent generation.

Required pre-action packet:

- `outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json`

Reusable validator:

- `mgc_v05l.execution_core.track_b_pre_action_snapshot_validator`

Dry-run executor envelope:

- `mgc_v05l.execution_core.track_b_paper_autonomous_recovery_executor`
- audit directory: `outputs/track_b_execution_core/paper_autonomous_recovery/executor_attempts/`

Executor boundary rule:

- future executors may only execute from a coherent Control Plane Snapshot;
- the snapshot must be captured immediately before action;
- no executor may reassemble scattered local evidence from individual artifacts;
- if the snapshot is missing, stale, or not coherent, the dry-run planner must classify `PLAN_BLOCKED_STALE_EVIDENCE`;
- future executor apply paths must call `validate_track_b_pre_action_snapshot(...)` before any runtime restart or broker/lifecycle mutation;
- dashboard projections of the snapshot are display-only and must never be consumed as authority.

Primary inputs:

- Control Plane Snapshot
- PAPER Recovery Policy
- Recovery Budget Ledger
- Runtime Supervisor Authority
- Runtime Resume Semantics
- Self-Recover Rules
- Crash Loop Protection
- Agent Health
- Proof Readiness
- Open Order Truth
- Managed Order Registry
- Order Adjustment Planner
- Position Truth
- Managed Position Registry
- Broker Reconciliation and Broker Lease
- Lifecycle State Matrix

Authority output:

- `outputs/track_b_execution_core/paper_autonomous_recovery/latest_paper_autonomous_recovery_plan.json`

The output is a dry-run plan. Every proposed action has `execution_enabled=false`.

## Plan Classifications

- `NO_ACTION_NEEDED`: no recovery action is currently planned.
- `WAIT_MARKET_CLOSED`: market/session is closed and no fresh bars are expected.
- `PLAN_RUNTIME_RETRY`: a future executor could start a bounded PAPER runtime retry after a final evidence refresh.
- `PLAN_EVIDENCE_REFRESH`: authority artifacts should be refreshed before recovery decisions.
- `PLAN_MARKET_DATA_RESTART`: a future executor could restart only the Phase-1 market-data producer.
- `PLAN_SCOPED_POSITION_CLEANUP`: exact broker exposure identity is known; a future scoped cleanup plan may be prepared.
- `PLAN_MANAGED_ORDER_MODIFY`: an existing working managed close order is a modify-in-place candidate.
- `PLAN_TARGETED_CANCEL_REPLACE`: old order is terminal and exact position remains open; targeted cancel/replace may be planned.
- `PLAN_QUARANTINE_OBSERVE_ONLY`: preserve evidence and observe; do not mutate.
- `PLAN_HARD_UNSAFE_HOLD`: hard invariant violation.
- `PLAN_BLOCKED_BUDGET_EXHAUSTED`: bounded recovery budget is exhausted.
- `PLAN_BLOCKED_STALE_EVIDENCE`: authority evidence is stale or missing.
- `PLAN_BLOCKED_IDENTITY_AMBIGUITY`: identity is too ambiguous for autonomous recovery planning.

## Proposed Action Schema

Each proposed action includes:

- `action_id`
- `action_type`
- `target_identity`
- `reason`
- `required_preconditions`
- `prohibited_actions`
- `budget_key`
- `remaining_budget`
- `would_mutate_broker`
- `would_mutate_lifecycle`
- `would_restart_runtime`
- `execution_enabled=false`

The `would_*` fields describe future executor behavior. They are not permissions in v1.

## Executor Attempt Envelope

The v1 executor framework is dry-run only. It calls `validate_track_b_pre_action_snapshot(...)`, checks a file-backed recovery budget ledger, and writes audit artifacts. It never starts a runtime and never mutates broker, order, or lifecycle state.

Budget authority:

- `outputs/track_b_execution_core/recovery_budget/latest_recovery_budget_ledger.json`
- `outputs/track_b_execution_core/recovery_budget/recovery_budget_events.jsonl`

The ledger is the persistent accounting source for per-agent/per-action
recovery budgets. It keys attempts by agent id, action type, target identity
hash, runtime generation when known, and stop/failure classification. Crash Loop
Protection, PAPER Recovery Policy, the planner, and dry-run executor may display
budget state from this ledger; future apply-enabled executors must validate the
same ledger immediately after capturing a fresh Control Plane Snapshot.

Each attempt includes:

- `recovery_attempt_id`
- `control_plane_snapshot_id`
- `shared_truth_generation_id`
- `action_type`
- `target_identity`
- `budget_key`
- Recovery Budget Ledger source path and remaining budget
- `pre_action_validation`
- `pre_action_evidence`
- `post_action_evidence` placeholder
- `execution_enabled=false`
- `would_mutate_runtime`
- `would_mutate_broker`
- `would_mutate_lifecycle`
- `prohibited_actions`
- `result_classification`

Executor audit artifacts:

- per-attempt JSON under `outputs/track_b_execution_core/paper_autonomous_recovery/executor_attempts/`
- latest dry-run report at `outputs/track_b_execution_core/paper_autonomous_recovery/executor_attempts/latest_paper_autonomous_recovery_executor_attempt.json`
- append-only event log at `outputs/track_b_execution_core/paper_autonomous_recovery/executor_attempts/paper_autonomous_recovery_executor_events.jsonl`

## Disabled RUNTIME_RETRY Adapter

`RUNTIME_RETRY` is the first candidate action adapter for future enablement because it does not directly mutate broker/order/lifecycle state. In v1 it is wired only as a disabled boundary:

- `adapter_name=RUNTIME_RETRY_DISABLED_V1`
- `adapter_enabled=false`
- `execution_enabled=false`
- `blocked_reason=ADAPTER_DISABLED`
- `would_execute_command` records the intended launcher command
- `apply_result.executed=false`

Before the disabled adapter can report `ADAPTER_DISABLED`, it must pass two hard
pre-apply gates:

- `validate_track_b_pre_action_snapshot(...)` must return
  `PRE_ACTION_SNAPSHOT_VALID`.
- Recovery Budget Ledger must provide a matching `RUNTIME_RETRY` budget entry
  for `agent_id=track_b_paper_runtime` and the target identity hash, with
  `attempts_remaining > 0`, no exhausted budget, no active cooldown, and no
  quarantine requirement.

Budget gate classifications:

- `BUDGET_GATE_PASS`
- `BUDGET_GATE_BLOCKED_EXHAUSTED`
- `BUDGET_GATE_BLOCKED_COOLDOWN`
- `BUDGET_GATE_BLOCKED_QUARANTINE`
- `BUDGET_GATE_BLOCKED_MISSING`

Future apply-enabled runtime retry must use a two-phase Recovery Budget Ledger
event lifecycle:

- `BUDGET_ATTEMPT_RESERVED`: reserve budget immediately before apply after
  snapshot and budget gates pass.
- `BUDGET_ATTEMPT_RELEASED`: release the reservation if apply is abandoned
  before execution.
- `BUDGET_ATTEMPT_CONSUMED_SUCCESS`: consume the reservation after a successful
  apply attempt.
- `BUDGET_ATTEMPT_CONSUMED_FAILURE`: consume the reservation after a failed
  apply attempt.
- `BUDGET_ATTEMPT_EXPIRED`: release an abandoned stale reservation.

Reservation events must include `recovery_attempt_id`,
`control_plane_snapshot_id`, `shared_truth_generation_id`, `action_type`,
`budget_key`, `agent_id`, and `created_at`. Consumed, released, and expired
events must link back to `reservation_id`. Duplicate active reservations for the
same recovery attempt are invalid. A consumed event without a prior reservation
is invalid.

The disabled v1 adapter includes `would_record_budget_event` as an audit
preview only. It does not append to
`outputs/track_b_execution_core/recovery_budget/recovery_budget_events.jsonl`,
and budget consumption remains reserved for a future explicitly apply-enabled
executor.

The disabled adapter also includes a Recovery Budget transaction simulation
preview. The simulator validates reserve/follow-up ordering end to end without
appending events:

- `TRANSACTION_DRY_RUN_READY`
- `TRANSACTION_BLOCKED_NO_RESERVATION`
- `TRANSACTION_BLOCKED_DUPLICATE_ACTIVE_RESERVATION`
- `TRANSACTION_BLOCKED_INVALID_ORDERING`
- `TRANSACTION_BLOCKED_BUDGET_EXHAUSTED`
- `TRANSACTION_SIMULATED_RELEASED`
- `TRANSACTION_SIMULATED_CONSUMED_SUCCESS`
- `TRANSACTION_SIMULATED_CONSUMED_FAILURE`
- `TRANSACTION_SIMULATED_EXPIRED`

Future apply-enabled runtime retry must pass this transaction ordering model:
reserve before consume/release/expire, no duplicate active reservation for the
same `recovery_attempt_id`, no second consume after release/consume/expire, and
no consumed success/failure event without a known reservation. The preview
fields use `would_append=false` and `append_enabled=false`; dry-run adapters may
show both a reserve+success and reserve+failure path, but they must not write
either path to the budget event log.

### RUNTIME_RETRY Apply Transaction Lifecycle

The disabled adapter now defines the future apply transaction lifecycle without
enabling execution. The lifecycle is emitted under
`action_adapter.runtime_retry_transaction` and is still read-only:

- `RUNTIME_RETRY_TRANSACTION_DRY_RUN_READY`: pre-action validations are ready to
  preview a transaction.
- `RUNTIME_RETRY_TRANSACTION_RESERVED`: budget reservation event preview is
  valid.
- `RUNTIME_RETRY_TRANSACTION_APPLY_DISABLED`: the sequence reached the apply
  boundary, but execution remains disabled.
- `RUNTIME_RETRY_TRANSACTION_RELEASED`: release event preview for an abandoned
  reservation.
- `RUNTIME_RETRY_TRANSACTION_CONSUMED_SUCCESS`: success consumption event
  preview.
- `RUNTIME_RETRY_TRANSACTION_CONSUMED_FAILURE`: failure consumption event
  preview.
- `RUNTIME_RETRY_TRANSACTION_ABORTED_PRE_ACTION`: snapshot, generation, budget,
  or reservation validation blocked before apply.
- `RUNTIME_RETRY_TRANSACTION_ABORTED_POST_ACTION`: post-action verification
  failure classification reserved for future apply-enabled mode.

Required lifecycle fields:

- `recovery_attempt_id`
- `reservation_id`
- `control_plane_snapshot_id`
- `shared_truth_generation_id`
- `previous_runtime_generation_id`
- `proposed_next_runtime_generation_id`
- `budget_key`
- `budget_event_sequence_preview`
- `launch_command_preview`
- `post_action_verification_plan`
- `append_enabled=false`
- `execution_enabled=false`

The sequence is:

1. Validate the Control Plane Snapshot with
   `validate_track_b_pre_action_snapshot(...)`.
2. Validate Runtime Resume v2 generation posture:
   `resume_action_policy=NEW_RUNTIME_GENERATION_ALLOWED`, a non-empty
   `proposed_next_runtime_generation_id`, `generation_reuse_allowed=false`, and
   `must_start_new_generation=true`.
3. Validate Recovery Budget Ledger attempts, cooldown, and quarantine state.
4. Preview `BUDGET_ATTEMPT_RESERVED` with the proposed runtime generation id.
5. Stop at `RUNTIME_RETRY_TRANSACTION_APPLY_DISABLED`; no process is started.
6. Preview post-action verification checks for future convergence proof.
7. Preview success/failure consumption and release events linked to the same
   `reservation_id`.
8. Write immutable executor audit artifacts only.

The placeholder config flag `enable_runtime_retry_adapter` may be recorded for audit, but it does not enable execution. A second, explicitly reviewed enablement boundary is required before this adapter may start a runtime.

Broker/order mutation adapters remain later and stricter:

- scoped position cleanup
- managed order modify-in-place
- targeted cancel/replace

Those adapters must add exact broker/order/lifecycle identity revalidation, scoped mutation authorization, and stronger post-action verification before they can move beyond dry-run.

## Decision Rules

1. Live-money eligibility, duplicate runtime writers, and broad ambiguity are hard unsafe holds.
2. Market closed/no fresh bars expected maps to `WAIT_MARKET_CLOSED`.
3. Budget exhaustion from Recovery Budget Ledger maps to
   `PLAN_BLOCKED_BUDGET_EXHAUSTED` or quarantine-observe posture.
4. Stale, missing, or incoherent Control Plane Snapshot evidence maps to `PLAN_BLOCKED_STALE_EVIDENCE`.
5. Phase-1 producer down while the proof window is open and shared truth is clean maps to `PLAN_MARKET_DATA_RESTART`.
6. Suspicious, duplicate, or identity-ambiguous order state maps to quarantine or identity ambiguity, not mutation.
7. Managed-order modify is planned only when Order Adjustment Planner says `MODIFY_IN_PLACE_ELIGIBLE`.
8. Targeted cancel/replace is planned only when Order Adjustment Planner says `TARGETED_CANCEL_REPLACE_REQUIRED`.
9. Exact broker exposure maps to scoped cleanup only when PAPER Recovery Policy says `SCOPED_RECOVERY_ELIGIBLE` and shared position identity is available.
10. Clean proof-ready truth plus PAPER Recovery Policy `AUTONOMOUS_RETRY_ELIGIBLE` maps to `PLAN_RUNTIME_RETRY`.

## Hard Invariants

The planner must never weaken these boundaries:

- no live-money route
- no broad cancel or flatten
- no duplicate runtime writers
- no hidden recovery
- no stale evidence as mutation permission
- no dashboard projection authority
- no paper_proof bypass
- no pretending suspicious state is clean
- no broker mutation without exact identity
- no runtime retry without Recovery Budget Ledger evidence

## v2 Executor Boundary

A future executor can use this plan as one input, but must still revalidate shared truth immediately before any action. Mutation-capable execution should be split into narrow adapters:

- runtime retry executor
- market-data producer restart executor
- scoped position cleanup executor
- managed order modify-in-place executor
- targeted cancel/replace executor

Each adapter should keep its own explicit authorization and audit artifact and must reuse the shared authority evidence gates.

Before any adapter executes, it must capture a fresh Control Plane Snapshot and carry these fields into its audit artifact:

- `control_plane_snapshot_id`
- `shared_truth_refresh_generation_id`
- `snapshot_coherence_status`
- `supervisor_decision_id`
- `supervisor_classification`

The adapter must stop before action if `snapshot_coherence_status` is not `COHERENT`, if the snapshot is stale, or if the snapshot identity does not match the dry-run plan being executed.

The adapter must also stop if `validate_track_b_pre_action_snapshot(...)` returns any result other than `PRE_ACTION_SNAPSHOT_VALID`. Validator blocks include stale/missing/incoherent snapshot, plan mismatch, supervisor mismatch, hard invariant violation, and target identity mismatch.
