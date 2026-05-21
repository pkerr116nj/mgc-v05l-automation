# Track B Runtime Convergence Slice 4: Stale PID and Duplicate Writer Guard

Date: 2026-05-21

Scope: PAPER-only launcher/status convergence. This slice does not change strategy logic, submit readiness authority, restart authority, broker behavior, or lifecycle behavior.

## Goal

Use Slice 3 runtime generation evidence to make stale PID cleanup and duplicate-writer prevention deterministic before a supervised Track B PAPER launch claims or creates a runtime writer.

## Contract

The launcher/status flows now classify runtime ownership from:

- `outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid.json`
- `outputs/probationary_pattern_engine/paper_session/runtime/paper_runtime_truth.json`
- `outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json`
- `outputs/probationary_pattern_engine/paper_session/operator_status.json`
- `outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json`

The shared helper `classify_runtime_launch_guard` is pure and side-effect free. It reports whether an existing PID can be accepted, whether stale PID metadata can be cleaned, or whether launch must block because a live/wrong-root/duplicate writer is present.

## Classifications

- `LAUNCH_PID_ACCEPTED`: live PID metadata is fresh, root-correct, and generation-aligned.
- `LAUNCH_NO_PID_METADATA`: no PID metadata exists; launcher may proceed.
- `LAUNCH_STALE_PID_CLEANUP_ALLOWED`: PID metadata is stale/dead, broker reconciliation is clean, and cleanup may remove stale PID metadata before launch.
- `LAUNCH_STALE_PID_CLEANUP_BLOCKED`: stale/dead PID metadata exists but broker state is not clean.
- `LAUNCH_CONFLICTING_WRITER_BLOCKED`: duplicate writer or live generation mismatch evidence exists.
- `LAUNCH_WRONG_ROOT_BLOCKED`: live PID points outside the expected Dev root.
- `LAUNCH_ZOMBIE_PID_REJECTED`: PID resolves to a zombie process and is not accepted as runtime truth.

## Safety Boundaries

- No broker/order/lifecycle mutation is introduced.
- Stale PID cleanup is allowed only for dead/stale PID metadata when broker reconciliation is `TRACK_B_PAPER_BROKER_RECONCILED`.
- A live wrong-root or generation-mismatched runtime blocks launch loudly instead of being killed.
- Status output remains evidence-only for readiness in this slice.

## Launcher Behavior

`scripts/run_headless_supervised_paper_service.sh` now writes a launch guard artifact at:

`outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid.json.launch_guard.json`

Before launching, it:

1. Classifies existing PID metadata with process/root/generation evidence.
2. Accepts only a matching live runtime.
3. Cleans stale PID metadata only when broker reconciliation is clean.
4. Blocks launch on duplicate writer, wrong-root runtime, zombie PID, or unsafe cleanup state.

## Status Behavior

`scripts/show_headless_supervised_paper_status.sh` now surfaces:

- `paper_runtime_launch_guard_classification`
- `paper_runtime_launch_guard_cleanup_allowed`
- `paper_runtime_launch_guard_launch_allowed`
- `paper_runtime_launch_guard_blockers`
- freshness classifications for runtime truth, config-in-force, and operator status artifacts

These fields are display/evidence only and do not override canonical readiness.

## Recommended Slice 5

Converge heartbeat ownership into canonical readiness as evidence, still fail-closed, then add restart-budget/cooldown semantics so self-healing can make deterministic restart decisions without duplicate writer risk.
