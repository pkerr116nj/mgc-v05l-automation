# Track B Research / Offline Diagnostics Policy

Track B research and offline diagnostic artifacts are evidence for analysis, replay, and forensic review. They are not shared truth, broker truth, runtime market-data truth, readiness truth, routing authority, restart authority, or recovery authority.

## Required Metadata

Research/offline diagnostic JSON reports should include these labels when practical:

- `research_only=true`
- `offline_diagnostic=true`
- `not_runtime_authority=true`
- `not_broker_truth=true`
- `not_market_data_runtime_truth=true`
- `not_routing_authority=true`
- `source_category=research/offline`

The shared helper is:

- `mgc_v05l.execution_core.track_b_research_offline_metadata`

## Inventory

Primary research/offline roots:

- `outputs/track_b_research/*`
- `outputs/research_platform/*`
- `outputs/research_runtime_bridge/*`
- `outputs/research_warehouse/*`
- `outputs/reports/*research*`

Known offline diagnostic readers that may inspect operator/dashboard history:

- `src/mgc_v05l/research/asia_drift/data_continuity_audit.py`
- `src/mgc_v05l/research/asia_drift/cross_asset_trade_mapping.py`
- `src/mgc_v05l/execution_core/track1_signal_handoff_breakpoint_audit.py`

These paths are offline/forensic only. Any dashboard/operator rows they read are historical samples and must not be promoted to active Track B authority.

## Consumption Rule

Active runtime, launch, restart, readiness, broker/order, lifecycle, and autonomous recovery code must consume execution_core shared authority artifacts, especially Control Plane Snapshot, Shared Truth Refresh, Runtime Supervisor Authority, Position Truth, Open Order Truth, Managed Order Registry, Managed Position Registry, and Broker Reconciliation / Lease evidence.

Research/offline artifacts may be consumed only when a caller is explicitly in replay/offline/research mode. No active PAPER runtime decision may be based on a `latest_*` research artifact.

## Prohibited

- No runtime start/restart authorization from research/offline artifacts.
- No broker submit/cancel/close/replace/modify authorization from research/offline artifacts.
- No lifecycle/ledger repair authorization from research/offline artifacts.
- No market-data freshness or broker truth classification from research/offline artifacts.
- No dashboard projection as authority.

## SS-LCA-012 Status

`SS-LCA-012` is resolved/narrowed by standard metadata labels and static hot-path checks. Remaining work is future hygiene: continue adding labels to older research reports as they are touched, and prefer cold/archive research roots for forensic replay rather than active dashboard paths.
