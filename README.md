# mgc-v05l-automation

External automation engine for MGC v0.5l with a replay-first strategy core, SQLite persistence, and a Schwab market-data integration path that normalizes into the same internal bar model used by replay.

## Current Architecture

- `replay` remains the deterministic test and debug path
- Schwab historical data flows through explicit symbol mapping and normalization into the same internal `Bar` type
- strategy logic stays broker-agnostic and unchanged by the market-data adapter
- live order execution is still not implemented
- experimental causal momentum research remains isolated from production signals

## Install

```bash
python3.11 -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
```

## Required Environment Variables

Schwab auth uses environment variables and local token storage. Do not commit secrets.

```bash
export SCHWAB_APP_KEY="your-app-key"
export SCHWAB_APP_SECRET="your-app-secret"
export SCHWAB_CALLBACK_URL="http://127.0.0.1:8182/callback"
export SCHWAB_TOKEN_FILE=".local/schwab/tokens.json"
```

`.env.example` shows the expected variable names. The default local token file path is gitignored.

## Replay Usage

```bash
mgc-v05l replay \
  --config config/base.yaml \
  --config config/replay.yaml \
  --csv /path/to/mgc_replay.csv
```

Replay uses the locked fill policy `NEXT_BAR_OPEN` and prints a JSON summary.

## Schwab Local Auth Flow

1. Create and export the required env vars above.
2. Generate the authorization URL:

```bash
mgc-v05l schwab-auth-url
```

3. Open the printed URL in a browser and complete the Schwab authorization-code flow.
4. Exchange the returned code:

```bash
mgc-v05l schwab-exchange-code --code "<returned-auth-code>"
```

5. Refresh the local token later if needed:

```bash
mgc-v05l schwab-refresh-token
```

Local token files are stored at `.local/schwab/tokens.json` by default, or at the path given by `SCHWAB_TOKEN_FILE`.

## Fetch Historical Bars

Use explicit symbol mapping. Do not assume MGC contract formatting in code.

One-off symbol override:

```bash
mgc-v05l schwab-fetch-history \
  --config config/base.yaml \
  --config config/replay.yaml \
  --internal-symbol MGC \
  --historical-symbol "<confirmed-schwab-history-symbol>" \
  --period-type day \
  --period 1 \
  --frequency-type minute \
  --frequency 5 \
  --start-date-ms 1741903200000 \
  --end-date-ms 1741903800000
```

Optional local mapping file:

```bash
cp config/schwab.local.example.json config/schwab.local.json
```

Then fill in the confirmed Schwab futures symbols and run:

```bash
mgc-v05l schwab-fetch-history \
  --config config/base.yaml \
  --config config/replay.yaml \
  --schwab-config config/schwab.local.json \
  --internal-symbol MGC \
  --period-type day \
  --period 1 \
  --frequency-type minute \
  --frequency 5
```

Add `--persist` to save normalized bars into the configured SQLite database.

## Fetch Quotes

Quote fetching is separate from strategy decisions for now.

```bash
mgc-v05l schwab-fetch-quote \
  --config config/base.yaml \
  --config config/replay.yaml \
  --internal-symbol MGC \
  --quote-symbol "<confirmed-schwab-quote-symbol>"
```

## Persistence

Strategy and replay persistence is stored in SQLite using the configured `database_url`.

Core tables include:
- `bars`
- `features`
- `signals`
- `strategy_state_snapshots`
- `order_intents`
- `fills`
- `processed_bars`

## Tests

```bash
PYTHONPATH=src .venv/bin/pytest tests/unit tests/integration -q
PYTHONPYCACHEPREFIX=.pycache python3 -m compileall src tests docs
```

## Notes

- Schwab historical `/pricehistory` and `/quotes` support are implemented through the adapter boundary
- live Schwab ingestion and live order execution remain deferred
- the symbol format for MGC futures on Schwab must remain explicitly configured until verified from real calls
- secrets and token files must not be committed

Additional docs:
- [Developer Runbook](/Users/patrick/Documents/MGC-v05l-automation/docs/DEVELOPER_RUNBOOK.md)
- [Schwab Market Data Adapter Notes](/Users/patrick/Documents/MGC-v05l-automation/docs/SCHWAB_MARKET_DATA_ADAPTER.md)
