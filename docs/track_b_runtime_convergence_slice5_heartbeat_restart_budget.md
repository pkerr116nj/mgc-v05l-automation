# Track B Runtime Convergence Slice 5: Heartbeat Evidence and Restart Budget

Date: 2026-05-21

Scope: PAPER-only readiness/self-healing convergence. This slice does not change submit authority, broker behavior, lifecycle behavior, strategy thresholds, session policy, or automatic restart execution.

## Runtime Truth Evidence in Canonical Readiness

Canonical readiness now carries `runtime_truth_heartbeat` from:

`outputs/probationary_pattern_engine/paper_session/runtime/paper_runtime_truth.json`

The evidence includes runtime instance, restart generation, heartbeat state, freshness, writer authority, source commit, lane count, B+ threshold, and test-mule enablement.

This is evidence-only:

- `readiness_authority=false`
- `restart_authority=false`
- stale heartbeat produces a warning, not a submit decision by itself
- healthy heartbeat cannot override runtime-down, stale ingestion, reconciliation, broker lease, market data, or route-authority gates

## Restart Budget / Cooldown Contract

Self-healing health now includes `restart_control` with:

- `restart_attempt_count`
- `restart_window_seconds`
- `max_restart_attempts`
- `cooldown_until`
- `cooldown_active`
- `crash_loop_state`
- `max_restart_budget_exhausted`
- `restart_allowed`

Deterministic classifications:

- `RESTART_ALLOWED`
- `RESTART_COOLDOWN_ACTIVE`
- `RESTART_BUDGET_EXHAUSTED`
- `RESTART_BLOCKED_DUPLICATE_WRITER`
- `RESTART_BLOCKED_RECONCILIATION`
- `RESTART_NOT_NEEDED_HEALTHY`

`auto_restart_allowed` remains false unless the existing self-healing classification is restart eligible and the restart-control classification is `RESTART_ALLOWED`.

## Non-Goals

- No automatic restart execution changes.
- No submit authority changes.
- No broker/order/lifecycle mutation.
- No runtime restart required for this slice.

## Recommended Slice 6

Persist restart-attempt history as an append-only, generation-aware event ledger, then wire self-healing repair executors to consult `restart_control` before any future automatic restart action.
