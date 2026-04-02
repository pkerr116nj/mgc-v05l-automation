# Schwab Market Data Adapter Notes

## Purpose

This layer adds a real Schwab auth and historical market-data path without changing the shared strategy core or introducing a separate strategy-specific market-data model.

Replay remains intact and continues to serve as:
- deterministic test mode
- deterministic debug mode
- a future candidate for seeding from broker-sourced historical data

## Architecture Fit

The shared ingestion shape is:
1. external data source
2. explicit symbol mapping
3. raw-response normalization
4. shared internal `Bar` model
5. session classification and validation
6. persistence and strategy consumers

This means:
- replay bars and Schwab historical bars target the same internal `Bar`
- the strategy core does not need a separate Schwab-facing interface
- historical backfill can feed the same persistence and downstream consumers used by replay

## Implemented in This Stage

- OAuth authorization-code support
  - auth URL construction
  - auth-code exchange
  - refresh-token flow
  - local token persistence for development
- confirmed Schwab `GET /pricehistory`
  - configurable request parameters
  - epoch-millisecond handling
  - candle normalization into internal `Bar`
  - ordering and missing-field validation
- confirmed Schwab `GET /quotes`
  - normalized quote results kept outside strategy decisions
- explicit symbol mapping
  - internal symbol -> Schwab historical symbol
  - internal symbol -> Schwab quote symbol

## Key Modules

- `src/mgc_v05l/market_data/schwab_auth.py`
  - OAuth client
  - local token store
  - env-backed auth config loading

- `src/mgc_v05l/market_data/schwab_http.py`
  - stdlib JSON transport
  - real `/pricehistory` client
  - real `/quotes` client

- `src/mgc_v05l/market_data/schwab_adapter.py`
  - explicit symbol mapping
  - time normalization
  - `/pricehistory` candle normalization into internal bars
  - quote-response normalization

- `src/mgc_v05l/market_data/historical_service.py`
  - historical fetch service with optional bar persistence

- `src/mgc_v05l/market_data/quote_service.py`
  - quote inspection path separated from strategy logic

## Still Placeholder

- live Schwab polling
- live Schwab streaming
- live order execution
- reconciliation workflow expansion
- final confirmed MGC futures symbol formatting on Schwab

## Important Constraints

- strategy entry and exit behavior are unchanged
- replay support is unchanged
- experimental causal momentum research is unchanged and still isolated
- secrets and token files must remain local and uncommitted
