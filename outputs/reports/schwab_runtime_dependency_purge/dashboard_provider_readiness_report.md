# Dashboard Provider Readiness

## Semantics

- Broker readiness is derived from the IBKR paper monitor/runtime, not from Schwab auth.
- Market-data readiness is provider-specific and should distinguish Databento primary from Schwab fallback.
- Schwab failure should surface as fallback-specific unavailability unless Schwab is explicitly selected as the active provider.
- Local cache/replay readiness remains separate from broker and market-data readiness.

## Current matrix

- Dashboard bootstrap status: `ready`
- Dashboard reduced mode: `False`
- IBKR paper monitor status: `ready`
- Databento primary status: `configured`
- Schwab fallback status: `optional`
- Schwab fallback allowed: `True`
- Schwab required for runtime: `False`
