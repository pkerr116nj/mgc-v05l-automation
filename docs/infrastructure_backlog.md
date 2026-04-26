# Infrastructure Backlog

## 1. Completed Data Foundation Milestones

- Canonical `1m` six-core coverage is complete for `GC,MGC,ES,MES,NQ,MNQ` from `2020-01-01T18:01:00-05:00` through `2026-04-21T23:59:00-04:00`.
- Phase A parquet warehouse is complete for `GC,MGC,ES,MES,NQ,MNQ`.
- Phase A derived timeframes are complete:
  - `5m`
  - `15m`
  - `60m`
  - `240m`
  - `daily`
- Final six-core integrity audit is healthy.
- `analysis_allowed=true` on the final six-core integrity audit.

## 2. Phase B Derived Timeframe Backlog

- Add `3m`.
- Add `7m`.
- Add `10m`.
- Add `12m`.
- Add `30m`.
- Add `120m`.
- Expand tests for nonstandard intervals.
- Rebuild the six-core parquet warehouse after Phase B timeframe support lands.
- Rerun six-core integrity audit after the Phase B rebuild.

## 3. Weekly Maintenance Backlog

- Add Saturday incremental canonical update workflow.
- Add impacted-window derived refresh workflow.
- Add maintenance report output.
- Make maintenance runs idempotent.
- Make maintenance runs resumable.

## 4. Data Warehouse Evolution

- Evaluate DuckDB/Parquet as the primary research scan layer.
- Keep SQLite as the canonical landing truth.

## 5. Raw-Contract Archive / Roll Ledger

- Add parent symbology discovery.
- Add raw-contract archive mode.
- Add point-in-time continuous mapping ledger.

## 6. Batch/DBN/ZSTD Future Optimization

- Run live batch validation in a scratch target.
- Add DBN/ZSTD staged parsing.
- Keep NDJSON staged fallback available.

## 7. Open Warnings / Non-Blocking Notes

- Raw storage `schwab_history` coexistence months remain observable but non-blocking.
- Research read-surface contamination remains zero.

## 8. Broker / Portfolio Truth Evolution

- Future architecture should allow multiple broker feeds and multiple portfolio-truth sources.
- The current forward execution and portfolio-truth path remains TradeStation-only until Stage 1B read-only validation is completed and accepted.
