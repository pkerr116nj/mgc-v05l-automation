# Track B Missed-Opportunity Replay Coverage Gap Report

Generated: 2026-05-26T10:39:05.427163+00:00

Classification: `MGC_20260525_REPLAY_BACKFILL_MATERIALIZED`

## May 25 MGC Backfill

- Source: `DATABENTO_TARGETED_BACKFILL_VIA_TRACK_B_DATA_MAINTENANCE`
- 1m rows fetched for May 25: `305`
- 5m replay rows available for May 25: `61`
- 5m window: `2026-05-25T01:45:00+00:00` to `2026-05-25T06:45:00+00:00`
- Derived replay path: `outputs/reports/entry_acceptance_research/full_history_batch/warehouse_historical_evaluator_partitions/MGC/2026Q2/datasets/derived_bars_5m/symbol=MGC/year=2026/shard_id=2026Q2/bars.parquet`

## Maintenance Gap

- Existing derived replay coverage previously stopped at 2026-04-22.
- Existing rolling data-maintenance report was stale at 2026-05-04.
- The Databento key exists in `.env.local`, but maintenance subprocesses need `set -a` before sourcing so the key is exported.
- Weekly maintenance should separately validate replay/archive coverage, not just rolling hot-path readiness.

No broker, live-money, or paper_proof path was used.
