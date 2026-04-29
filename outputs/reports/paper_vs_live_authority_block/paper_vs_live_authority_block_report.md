`PAPER_BLOCK_REMOVED_LIVE_AUTHORITY_SEPARATION_FIXED`

The paper block shown in the Electron app had two separate causes:

1. `src/mgc_v05l/app/operator_surface.py` treated every paper exception row as a blocking runtime fault.
2. `desktop/src/main/shared/operatorTriage.ts` collapsed the final verdict into a single live-style authority state, even in `PAPER` mode.

That combination produced the wrong operator story:
- runtime looked `RUNNING`
- paper stack/control plane looked usable
- but `Runtime Readiness` failed from `faults=9`
- and the Electron app surfaced a live-only blocker message in `PAPER` mode

**Exact blocking decision path**
- Runtime fault promotion: `src/mgc_v05l/app/operator_surface.py`
- Paper/live authority collapse: `desktop/src/main/shared/operatorTriage.ts`
- Paper-mode wording/rendering: `desktop/src/renderer/App.tsx`

**What the nine faults actually were**
- Source file: `outputs/operator_dashboard/paper_exceptions_snapshot.json`
- Observed at: `2026-04-29`
- Severity counts:
  - `BLOCKING=0`
  - `ACTION=0`
  - `INFO=0`
  - `WATCH=9`
- All nine rows were `DECISION_WITHOUT_INTENT` review items for midday lanes.
- These were advisory review rows, not hard paper-runtime faults.

**Before fix**
- `outputs/operator_dashboard/dashboard_api_snapshot.json`
- generated_at: `2026-04-29T17:30:34.234223+00:00`
- `operator_surface.runtime_readiness.blocking_faults_count = 9`
- `operator_surface.runtime_readiness.blocking_faults_active = true`
- `status_line = runtime=RUNNING | paper=ENABLED | entries=ENABLED | auth=READY | market_data=LIVE | faults=9 | runtime_recovery=RUNNING | restart_budget=0/3 | session_eligible=9 | waiting_bar=9 | no_setup=11 | actionable=0 | blocked=0 | bootstrap_issues=0`

At the same time, the control-plane artifacts already said paper was usable:
- `outputs/operator_dashboard/runtime/operator_dashboard_readiness.json`
  - `launch_allowed = true`
  - `control_plane.paper_runtime_ready = true`
- `outputs/operator_dashboard/supervised_paper_operability_snapshot.json`
  - `app_usable_for_supervised_paper = true`

That mismatch was the bug.

**Fix applied**
- `src/mgc_v05l/app/operator_surface.py`
  - only `severity == BLOCKING` contributes to `blocking_faults`
  - non-blocking rows are now surfaced as `advisory_faults`
  - `blocking_faults_active` now reflects true blocking faults, not WATCH rows
- `desktop/src/main/shared/operatorTriage.ts`
  - added separate `paper_trade_authority` and `live_trade_authority`
  - added `paper_runtime_ready`, `live_runtime_ready`, `paper_bridge_allowed`, `live_bridge_allowed`
  - paper mode now derives its verdict from paper gates, not live-only authority
- `desktop/src/renderer/App.tsx`
  - paper-mode messaging is now mode-specific
  - `Paper Trading Blocked` / `paper trade authority blocked` are used where applicable
  - live-only wording is no longer shown as the paper blocker

**After fix**
- operator dashboard restarted to republish snapshots from the patched code
- `outputs/operator_dashboard/dashboard_api_snapshot.json`
- generated_at: `2026-04-29T17:31:53.661924+00:00`
- published runtime-readiness now shows:
  - `blocking_faults_count = 0`
  - `advisory_faults_count = 9`
  - `blocking_faults_active = false`
  - `status_line = runtime=RUNNING | paper=ENABLED | entries=ENABLED | auth=READY | market_data=STALE | faults=0 | advisory=9 | runtime_recovery=RUNNING | restart_budget=0/3 | session_eligible=9 | waiting_bar=9 | no_setup=11 | actionable=0 | blocked=0 | bootstrap_issues=0`

**Important current nuance**
- The bogus paper block is removed.
- The current post-restart snapshot now shows `market_data=STALE`.
- That is a separate paper-mode gate and should be treated as a real paper readiness/input issue if it persists.
- It is no longer the case that WATCH-only exception rows or missing live-money authority are globally blocking paper mode.

**Verification**
- Python backend slice: `tests/unit/test_mgc_v05l_operator_surface.py` -> `4 passed`
- Electron main-process compile surface: `npm run typecheck:main --prefix desktop` -> passed
- Focused Electron runtime contract test file:
  - compiled temp harness executed
  - relevant paper/live separation tests passed:
    - `paper mode does not let live broker and operator auth gates block supervised paper usability`
    - `paper mode keeps live authority blocked while allowing supervised paper authority`
    - `paper mode still hard-blocks on current runtime faults with paper-specific wording`
    - `live mode still requires live trade authority`
- full `runtime.test.js` temp run finished with unrelated pre-existing failures in startup timing tests, but the new authority-split tests passed

**Conclusion**
- Paper mode no longer requires live trade authority.
- WATCH-only/review exceptions no longer keep `Runtime Readiness` fail-closed.
- Live-money protections remain fail-closed and separate.
- If the Electron app still shows paper blocked after refresh, the remaining published reason is now the paper-side `market_data=STALE` state, not the old live-authority/runtime-fault mismatch.
