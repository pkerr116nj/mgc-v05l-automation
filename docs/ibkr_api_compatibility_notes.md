# IBKR API Compatibility Notes For Track B

## Order Construction

- Track B proof orders are `LMT` `DAY` quantity `1` only.
- Keep IBKR `Order` objects minimal:
  - `action`
  - `totalQuantity`
  - `orderType = LMT`
  - `lmtPrice`
  - `tif = DAY`
  - `account` when required for paper account safety
  - `transmit = True` only inside the fully gated paper proof path
- Do not use deprecated or unsupported IBKR order attributes:
  - `eTradeOnly` / error `10268`
  - `firmQuoteOnly` / error `10269`
  - `nbboPriceCap` / error `10270`
- Deprecated attribute errors must be captured as broker rejection evidence and classified as ambiguous manual review unless a later design gives a cleaner blocking classification.

## Connection Handshake

- Use the direct `EWrapper` / `EClient` pattern for IBKR bridges.
- Wait passively for the initial `nextValidId` callback after `connect`.
- Do not call `reqIds(-1)` before the initial `nextValidId` has been received.

## Delayed Market Data

- Use `reqMarketDataType(3)` before `reqMktData` when delayed data is expected.
- `marketDataType=3` confirms delayed mode.
- Delayed data is acceptable for paper proof only when explicitly labeled.
- Delayed data blocks live-money readiness.

## Informational Warnings

- IBKR farm-status messages `2104`, `2106`, and `2158` are informational unless accompanied by a blocking condition.

## Track B Safety

- No `MKT` orders in the proof path.
- No live-money mode.
- No Track A dashboard, cache, or snapshot authority.
