`ELECTRON_LIVE_API_TRANSPORT_FIXED`

Electron transport is now fixed at the actual app-consumption layer.

What was wrong:
- packaged local launches were allowed to settle into `attached_snapshot_bridge` before giving the live `:8790` dashboard enough time to answer
- packaged local snapshot attach also preferred a synthesized bridge over the real readiness bridge, so later polls could demote the app back into snapshot mode even after a successful live attach

What changed:
- [runtime.ts](/Users/patrick/Documents/MGC-v05l-automation/desktop/src/main/runtime.ts:2211) now distinguishes:
  - `attachedSnapshotBridgeExpectsLiveApi`
  - `attachedSnapshotBridgeConfirmsLiveApi`
- [runtime.ts](/Users/patrick/Documents/MGC-v05l-automation/desktop/src/main/runtime.ts:2299) now prefers the real readiness bridge before falling back to a synthesized packaged snapshot bridge
- [runtime.ts](/Users/patrick/Documents/MGC-v05l-automation/desktop/src/main/runtime.ts:3066) now keeps packaged local attach on the live path when the readiness bridge proves the current payload is reachable
- [App.tsx](/Users/patrick/Documents/MGC-v05l-automation/desktop/src/renderer/App.tsx:5320) continues to emit renderer bootstrap evidence when the UI reaches service-attached usable state

Live proof from the running Electron app:
- renderer bootstrap artifact: [desktop_renderer_bootstrap.json](/Users/patrick/Library/Application%20Support/mgc-operator-desktop/runtime/desktop_renderer_bootstrap.json)
  - `recorded_at=2026-04-29T18:42:33.008Z`
  - `stage=renderer:service-attached-usable`
  - `source_mode=live_api`
  - `dashboard_attached=true`
  - `live_actions_allowed=true`
  - `paper_runtime_ready=true`
- repeated renderer events confirm the same state:
  - [desktop_renderer_bootstrap_events.jsonl](/Users/patrick/Library/Application%20Support/mgc-operator-desktop/runtime/desktop_renderer_bootstrap_events.jsonl)
  - repeated `renderer:service-attached-usable` rows from `2026-04-29T18:40:27Z` through `2026-04-29T18:42:33Z`
- fresh app-consumed payload cache: [dashboard_api_snapshot.cache.json](/Users/patrick/Library/Application%20Support/mgc-operator-desktop/runtime/desktop_cache/dashboard_api_snapshot.cache.json)
  - `generated_at=2026-04-29T18:41:37.103190+00:00`
  - `mode=PAPER`
  - `paper_trade_allowed=true`
  - `paper_trade_block_reason=null`
  - `session_eligible_count=8`
  - `waiting_for_bar_count=8`
  - `no_setup_count=11`
  - `actionable_now_count=0`
  - `true_blocked_count=0`
  - `blocking_fault_count=0`
  - `paper_readiness_source=src/mgc_v05l/app/operator_dashboard.py:_paper_readiness_payload`
- fresh desktop readiness bridge cache: [operator_dashboard_readiness.json](/Users/patrick/Library/Application%20Support/mgc-operator-desktop/runtime/desktop_cache/operator_dashboard_readiness.json)
  - `payload.reachable=true`
  - `payload.ready=true`
  - `control_plane.dashboard_attached=true`
  - `control_plane.paper_runtime_ready=true`

Fresh live contract observed by the Electron app:
- `mode=PAPER`
- service is attached to the live local backend
- paper trade authority is allowed:
  - `paper_trade_allowed=true`
  - `paper_trade_block_reason=null`
- live authority remains separate from paper authority
- advisory faults are not blocking:
  - `blocking_fault_count=0`
  - `true_blocked_count=0`
- lane readiness counts are present:
  - `session_eligible_count=8`
  - `waiting_for_bar_count=8`
  - `no_setup_count=11`
  - `actionable_now_count=0`

Broker/order posture:
- current paper stack remains flat and reconciled for the supported executable scopes
- latest verified posture remains:
  - `MGC=0.0`
  - `MNQ=0.0`
  - `MES=0.0`
  - open orders all `0`
  - broker-minus-ledger all `0.0`
  - orphan exposure all `0.0`

Residual nuance:
- the legacy startup-status artifact can still lag behind the renderer-attached truth:
  - [desktop_dashboard_startup_status.json](/Users/patrick/Library/Application%20Support/mgc-operator-desktop/runtime/desktop_dashboard_startup_status.json)
  - it still falls back to `attached_snapshot_bridge` when the stricter localhost `/health` probe times out in the Electron main process
  - this no longer reflects the renderer’s actual service-attached state, which is why the renderer bootstrap artifact and synchronized desktop cache are the reliable live-proof artifacts for this pass

Verification:
- `npm run typecheck:main --prefix desktop` passed
- `npm run build:main --prefix desktop` passed
- transport regression tests passed:
  - packaged live promotion path
  - packaged post-grace live promotion path
  - packaged readiness-bridge sticky live attach path
  - attached snapshot bridge degraded path

No orders were placed.
No strategy logic was changed.
No live trading was enabled.
