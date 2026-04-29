# IBKR MES/ES Quote Diagnostic

- classification: `IBKR_MES_ES_QUOTES_DELAYED_ONLY`
- generated_at: `2026-04-29T11:37:49.163300+00:00`
- MGC comparison quote available: `True`
- MES any quote: `True`
- ES any quote: `True`

## Contract Check

- `MGC` ok: `True`
  contract: `MGCM6 / conId=712565978 / exchange=COMEX / expiry=20260626`
  detail: Contract details received from TWS.
- `MES` ok: `True`
  contract: `MESM6 / conId=770561194 / exchange=CME / expiry=20260618`
  detail: Contract details received from TWS.
- `ES` ok: `True`
  contract: `ESM6 / conId=649180678 / exchange=CME / expiry=20260618`
  detail: Contract details received from TWS.

## Comparison

- MGC response codes: `[10168]`
- MES response codes: `[354, 10167]`
- ES response codes: `[10168]`

## Interpretation

- MES/ES returned delayed quote data only. LMT pricing can use delayed quotes with explicit delayed labeling.
