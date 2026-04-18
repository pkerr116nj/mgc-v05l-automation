# Research Platform Tranche 4

This tranche retires the new top platform bottleneck identified in tranche 3 and continues the application bridge.

## What changed

- Added a persistent project-level source inventory/catalog in [source_context.py](/Users/patrick/Documents/MGC-v05l-automation/src/mgc_v05l/research/platform/source_context.py).
- Source discovery now reuses persisted SQLite file signatures and cached coverage rows instead of rescanning grouped `bars` coverage on every ATP bounded run.
- Warm inventory hits skip dataset/DuckDB rewrites entirely when underlying SQLite signatures are unchanged.
- The Strategy History Review page now prefers published research analytics without eagerly loading replay-study artifacts when analytics already cover the selected lane.
- Full-history ATP review now carries an explicit structured review config in manifests and registry identity.

## Source inventory design

The source inventory is project-level infrastructure, not ATP-local logic.

- Root: `outputs/research_platform/source_context/inventory`
- Raw row cache: `source_inventory_rows.jsonl`
- Query bundles: `datasets/source_inventory_rows/...`
- Query catalog: `catalog.duckdb`
- Manifest: `manifest.json`

Cache invalidation is driven by SQLite file signatures:

- `sqlite_path`
- `file_size_bytes`
- `file_mtime_ns`

If all signatures match the prior manifest, discovery returns from the persisted inventory immediately.

If any signature changes, only the affected SQLite files are rescanned and the inventory is refreshed.

## Runtime effect

Bounded optimized ATP smoke:

- tranche 3 wall: `29.238194s`
- tranche 4 wall: `0.290952s`
- tranche 3 source discovery: `28.927150s`
- tranche 4 source discovery: `0.002520s`

The remaining bounded-run time is now mostly intrinsic ATP scope work, plus registry/analytics publishing, not architectural source-discovery overhead.

## Application bridge

The app is now closer to analytics-native ATP historical review:

- P/L Calendar uses published `daily_pnl`
- main Strategy Deep Dive uses published summary/equity/drawdown/blotter/exit/session datasets
- Strategy History Review now avoids replay-study fetches when research analytics already cover the lane

Replay-linked fallback remains only for lanes that do not yet have published analytics truth.

## Non-ATP next step

The next clean migration family should be a strategy family that already has:

- stable historical trade rows
- clear lane identity
- recurring comparative review questions

Good candidates:

- approved quant replay families
- probationary paper/research families with explicit standalone strategy ids

Do not generalize ATP candidate/timing semantics into shared platform primitives first. Keep:

- source inventory
- registry/catalog
- analytics publishing
- dataset bundles

as project-level layers, while leaving ATP feature/candidate/outcome semantics strategy-specific.
