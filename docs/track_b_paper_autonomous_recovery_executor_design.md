# Track B PAPER Autonomous Recovery Executor Design

## Purpose

Track B PAPER is a bounded autonomous failure-discovery environment. The recovery executor path must therefore expose abnormal behavior, preserve evidence, and recommend bounded next steps without turning routine PAPER failures into permanent human-gated holds.

Version 1 is a dry-run planner only. It does not start or stop runtimes, restart producers, submit orders, cancel orders, replace orders, modify orders, close positions, flatten exposure, or mutate lifecycle artifacts.

## Authority Boundary

The planner consumes execution_core authority artifacts only. Dashboard/operator projections are display-only and must never be used as routing, restart, readiness, order, broker, or lifecycle authority.

Every future autonomous recovery executor must use the execution_core Control Plane Snapshot as the single coherent pre-action evidence packet. The snapshot must be captured immediately before the action boundary and must prove that Shared Truth, Runtime Supervisor Authority, PAPER Recovery Policy, and the dry-run Autonomous Recovery Plan were built from one coherent generation.

Required pre-action packet:

- `outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json`

Executor boundary rule:

- future executors may only execute from a coherent Control Plane Snapshot;
- the snapshot must be captured immediately before action;
- no executor may reassemble scattered local evidence from individual artifacts;
- if the snapshot is missing, stale, or not coherent, the dry-run planner must classify `PLAN_BLOCKED_STALE_EVIDENCE`;
- dashboard projections of the snapshot are display-only and must never be consumed as authority.

Primary inputs:

- Control Plane Snapshot
- PAPER Recovery Policy
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

## Decision Rules

1. Live-money eligibility, duplicate runtime writers, and broad ambiguity are hard unsafe holds.
2. Market closed/no fresh bars expected maps to `WAIT_MARKET_CLOSED`.
3. Budget exhaustion maps to `PLAN_BLOCKED_BUDGET_EXHAUSTED`.
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
- no runtime retry without bounded budget

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
