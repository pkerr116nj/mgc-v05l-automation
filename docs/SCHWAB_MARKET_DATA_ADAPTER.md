# Schwab Market Data Adapter Notes

## Purpose

The goal of this layer is to add Schwab market-data ingestion without changing the replay-first strategy core or creating a separate internal market-data model.

Replay remains intact and continues to serve as:
- deterministic test mode
- deterministic debug mode
- a future candidate for seeding from broker-sourced historical data

## Architecture Fit

The shared ingestion shape is:
1. external data source
2. symbol/timeframe mapping
3. raw-response normalization
4. shared internal `Bar` model
5. session classification and validation
6. strategy engine or persistence consumers

This means:
- historical backfill and live ingestion both target the same internal `Bar`
- replay bars and Schwab bars are intended to look the same once normalized
- strategy logic does not need a different interface for Schwab data

## Current Modules

- `src/mgc_v05l/market_data/schwab_models.py`
  - auth/config placeholders
  - symbol/timeframe mapping config
  - raw field mapping config
  - request and client protocol scaffolding

- `src/mgc_v05l/market_data/schwab_adapter.py`
  - symbol mapping
  - timeframe mapping
  - raw payload normalization into internal `Bar` objects

- `src/mgc_v05l/market_data/historical_service.py`
  - historical backfill service scaffold

- `src/mgc_v05l/market_data/live_feed.py`
  - live polling service scaffold
  - live streaming service interface scaffold

- `src/mgc_v05l/market_data/gateway.py`
  - shared gateway across replay, historical backfill, and live ingestion

## Complete vs Placeholder

Complete in this stage:
- internal adapter boundary
- mapping layer
- normalization layer
- shared `Bar` model targeting
- historical/live service scaffolding
- optional persistence hookup for normalized incoming bars

Placeholder pending official Schwab API confirmation:
- exact auth/token flow
- exact endpoint names
- exact raw payload field names
- exact polling/stream mechanics
- any transport/client implementation details

## Important Constraint

This stage does not:
- implement live order execution
- change strategy entry/exit behavior
- remove replay support
- guess undocumented Schwab API specifics
