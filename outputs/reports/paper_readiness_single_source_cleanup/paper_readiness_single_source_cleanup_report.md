## Classification
`PAPER_READINESS_SINGLE_SOURCE_FIXED`

## Outcome
Cleanup pass 1 is in place without rewriting the system.

`src/mgc_v05l/app/operator_dashboard.py:_paper_readiness_payload` is now the authoritative source for paper lane readiness/fireability. `src/mgc_v05l/app/operator_dashboard.py:_supervised_paper_operability_payload` remains the top-level paper usability contract. Transport and desktop layers now consume and summarize those contracts instead of independently re-deciding whether paper trading is blocked.

## What Changed
- `src/mgc_v05l/app/operator_dashboard.py`
  - publishes explicit paper-authority fields from `_paper_readiness_payload`
  - computes `paper_trade_allowed`, `paper_trade_block_reason`, `paper_readiness_source`, `paper_readiness_timestamp`
  - publishes explicit counts: `session_eligible_count`, `waiting_for_bar_count`, `no_setup_count`, `actionable_now_count`, `true_blocked_count`, `advisory_fault_count`, `blocking_fault_count`
  - passes `_supervised_paper_operability_payload` into operator-surface assembly so paper usability can be transported without reclassification
- `src/mgc_v05l/app/operator_surface.py`
  - `_build_runtime_readiness` now transports authoritative paper status instead of re-deriving it from `fault_state`
  - `blocking_faults_active` only reflects true blocking fault rows, not generic global fault labels
  - exposes transported paper-authority fields and counts to downstream consumers
- `desktop/src/main/shared/operatorTriage.ts`
  - PAPER mode now prefers authoritative paper authority when present
  - still preserves live authority separately for LIVE mode
  - exposes `paper_trade_allowed`, `paper_trade_block_reason`, `live_trade_allowed`, `live_trade_block_reason`, source/timestamp, and the new lane counts
  - falls back sensibly for older partial paper payloads instead of fail-closing just because `paper_runtime_ready` is absent
- `desktop/src/main/shared/operationalReadiness.ts`
  - no longer re-blocks supervised paper when authoritative paper readiness already says paper is allowed
  - keeps LIVE-specific blocking separate

## Authority Boundary
- `_paper_readiness_payload`: authoritative paper lane readiness and fireability
- `_supervised_paper_operability_payload`: authoritative top-level supervised-paper usability
- `_build_runtime_readiness`: transport/summary only
- `operatorTriage.ts` and `operationalReadiness.ts`: mode-aware display and app-level authority presentation only
- `src/mgc_v05l/execution/ibkr_paper_strategy_bridge.py`: final order preflight authority before any actual IBKR submission

## Duplicate Paper Blocking Removed
- PAPER mode no longer gets re-blocked downstream because of:
  - live authority being unavailable
  - advisory/WATCH-only fault rows
  - stale global `fault_state` labels when the authoritative paper contract is healthy
- Legacy/partial payloads now infer paper runtime readiness from actual running paper state instead of requiring an explicit `paper_runtime_ready=true` field to avoid a false block

## Current Projected Snapshot
Source artifacts:
- `outputs/operator_dashboard/runtime/headless_supervised_paper_dashboard.json`
- `outputs/operator_dashboard/runtime/headless_supervised_paper_operability.json`

Live dashboard note:
- a direct `curl http://127.0.0.1:8790/api/dashboard` failed during this pass, so the snapshot artifact is a projected post-fix readiness view derived from the persisted headless paper artifacts and current code semantics

Projected paper-authority values from the persisted headless artifact:
- `paper_trade_allowed=true`
- `paper_trade_block_reason=null`
- `paper_readiness_source=src/mgc_v05l/app/operator_dashboard.py:_paper_readiness_payload`
- `paper_readiness_timestamp=2026-04-29T17:31:46.573435+00:00`
- `paper_runtime_ready=true`
- `session_eligible_count=9`
- `waiting_for_bar_count=9`
- `no_setup_count=11`
- `actionable_now_count=0`
- `true_blocked_count=0`
- `advisory_fault_count=9`
- `blocking_fault_count=0`

## Verification
- `./.venv/bin/pytest -q tests/unit/test_mgc_v05l_operator_surface.py`
  - `5 passed`
- `npm run typecheck:main --prefix desktop`
  - passed
- `npm run build:main --prefix desktop`
  - passed
- `node --test --test-name-pattern "paper mode does not let live broker and operator auth gates block supervised paper usability" desktop/dist/main/runtime.test.js`
  - passed
- `node --test --test-name-pattern "paper mode keeps live authority blocked while allowing supervised paper authority" desktop/dist/main/runtime.test.js`
  - passed
- `node --test desktop/dist/main/runtime.test.js`
  - `27 passed`, `3 failed`
  - remaining failures are the same unrelated pre-existing startup timing tests:
    - `snapshot-first startup returns persisted state without waiting for live dashboard attach`
    - `prepareDesktopForLaunch schedules service warmup without blocking the app`
    - `prepareDesktopForLaunch skips automatic warmup in sandboxed launch contexts`

## Conclusion
The duplicate/conflicting paper-readiness decisions are removed for the main backend-to-Electron path.

This pass did not place orders, did not change strategy logic, and did not weaken live-money protections.
