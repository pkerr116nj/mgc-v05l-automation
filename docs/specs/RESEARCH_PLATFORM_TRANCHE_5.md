# Research Platform Tranche 5

This tranche hardened the remaining source-inventory cold path and took the first non-ATP family onto the shared platform.

## What changed

- Source inventory refresh is now request-scoped instead of always repo-wide.
- Discovery slices are stored under:
  - `outputs/research_platform/source_context/inventory/slices/<scope_hash>/...`
- Cold refreshes now scan only the requested symbol/timeframe slice for the changed SQLite DB.
- Warm lookups still return from the persisted inventory in milliseconds.
- Approved quant now has a first platform path through:
  - generic trade scope bundles
  - shared registry registration
  - shared analytics publishing

## Cold-path note

Current invalidation remains SQLite-signature based:

- `sqlite_path`
- `file_size_bytes`
- `file_mtime_ns`

Granularity is now:

- per SQLite file
- per requested symbol/timeframe scope

This means the cold path is no longer forced to rebuild a repo-wide coverage catalog when only one strategy family needs a narrow source slice.

## Approved Quant starter path

The first non-ATP migration target is approved quant replay/baseline review because it already has:

- explicit lane specs
- stable strategy identity
- reusable historical trade truth
- no dependence on ATP candidate/timing semantics

Artifacts:

- generic scope bundles under `outputs/research_platform/strategy_scopes/approved_quant`
- registry targets under `strategy_family=approved_quant`
- analytics datasets under `outputs/research_platform/analytics/approved_quant`

This proves the current platform layers are reusable beyond ATP:

- source/context discovery
- generic scope bundles
- registry/catalog
- analytics publishing

## Boundary check

Project-level and now validated beyond ATP:

- source inventory/catalog
- generic scope bundles
- registry/catalog
- analytics publishing

Still ATP-proven only:

- ATP feature bundles
- ATP candidate/timing layers
- ATP outcome substrate

Do not elevate ATP candidate/timing/session semantics into project-level primitives yet.
