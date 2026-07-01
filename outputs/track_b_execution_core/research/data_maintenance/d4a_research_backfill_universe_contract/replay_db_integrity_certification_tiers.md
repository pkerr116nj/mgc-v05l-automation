# D4A Replay DB Integrity Certification Tiers

Generated: 2026-07-01

Scope: documentation for bounded research-data preflight. This does not run a refresh, mutate DBs, or change runtime behavior.

## Problem

D4 showed that full SQLite integrity checks on `mgc_v05l.replay.sqlite3` can be too slow for an interactive preflight. The DB is about 63 GiB and contains about 24.7M `bars` rows. A weekly maintenance workflow needs bounded checks that catch common failure modes quickly, while still preserving an option for a full off-hours integrity scan.

## Tier 0: Metadata Readability

Use for every dry-run.

Checks:

- DB file exists.
- DB file size readable.
- `pragma schema_version` returns.
- required tables exist:
  - `bars`
  - `market_data_ingest_runs`
  - `market_data_bar_provenance`
- row counts return for required tables.
- max ingest coverage timestamp returns.

Expected runtime:

- seconds.

Failure classification:

- `REPLAY_DB_METADATA_NOT_READABLE`

## Tier 1: Weekly Bounded Consistency

Use before routine weekly refresh.

Checks:

- all Tier 0 checks,
- ingest-run status distribution,
- latest timestamp by configured research symbol from ingest metadata,
- target missing range does not overlap already complete range,
- sampled bar-table count by target symbol/date range,
- sampled duplicate-key check on target/missing range,
- source/provenance presence for sampled partitions.

Expected runtime:

- seconds to a few minutes, depending on indexes.

Failure classification:

- `REPLAY_DB_BOUNDED_CONSISTENCY_REVIEW_REQUIRED`

## Tier 2: Target-Range Pre-Mutation Validation

Use before any approved catch-up/backfill that will write research data.

Checks:

- all Tier 1 checks,
- duplicate-key check for the exact target range,
- table/index presence for the write path,
- free-space check for staging plus DB growth,
- transaction mode check,
- checkpoint/resume metadata check.

Expected runtime:

- bounded by target range, not full DB size.

Failure classification:

- `REPLAY_DB_TARGET_RANGE_NOT_CERTIFIED`

## Tier 3: Full SQLite Integrity

Use off-hours or scheduled maintenance.

Checks:

- `pragma quick_check`
- `pragma integrity_check`

Expected runtime:

- may be long for 63 GiB DB.

Failure classification:

- `REPLAY_DB_FULL_INTEGRITY_FAILED`

Operational note:

- Tier 3 should not be mandatory for every weekly interactive dry-run. It should run on an off-hours cadence or before major schema/storage changes.

## D4A Recommendation

Implement Tier 0/Tier 1 as the weekly preflight gate, with Tier 2 required before a write-enabled catch-up. Keep Tier 3 as a scheduled/off-hours certification task.

