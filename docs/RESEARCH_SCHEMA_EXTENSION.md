# Research Schema Extension

## Purpose

This additive schema extension gives the current SQLite persistence layer enough structure to support upcoming momentum-change research without changing production strategy behavior.

It adds durable storage for:
- raw OHLCV bar history
- continuous derived features
- per-bar signal evaluations
- experiment run metadata
- trade outcomes

## Why Ticker and Nullable CUSIP Are Included

- `ticker` is included directly in raw market-data identity so the schema can support MGC now and extend to other liquid instruments later without needing a platform redesign.
- `cusip` is nullable because some instruments, especially futures and research feeds, may not have a usable CUSIP at ingestion time.
- the schema also stores `asset_class` so ticker collisions across asset types remain manageable later.

## Backend and Portability

- SQLite remains the current backend.
- The schema stays practical and additive to the existing SQLAlchemy stack.
- Constraints and indexes were chosen to remain portable later rather than relying on Postgres-only behavior.

## Scope Boundary

This extension does not:
- add new momentum-trigger logic
- change the current MGC v0.5l strategy behavior
- change Schwab auth or live behavior
- turn the codebase into a generalized quant platform
