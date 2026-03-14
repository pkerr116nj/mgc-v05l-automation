# Developer Runbook

## Environment Setup

1. Create a Python 3.11+ virtual environment.
2. Activate it.
3. Install the package and test dependencies.

```bash
python3.11 -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
```

## Required Environment Variables

```bash
export SCHWAB_APP_KEY="your-app-key"
export SCHWAB_APP_SECRET="your-app-secret"
export SCHWAB_CALLBACK_URL="http://127.0.0.1:8182/callback"
export SCHWAB_TOKEN_FILE=".local/schwab/tokens.json"
```

Do not commit secrets or local token files.

## Run Tests

```bash
PYTHONPATH=src .venv/bin/pytest tests/unit tests/integration -q
PYTHONPYCACHEPREFIX=.pycache python3 -m compileall src tests docs
```

## Run Replay

```bash
mgc-v05l replay \
  --config config/base.yaml \
  --config config/replay.yaml \
  --csv /path/to/mgc_replay.csv
```

Replay prints a JSON summary to stdout.

## Run Experimental Causal Momentum Report

This path is research-only and does not affect production strategy behavior.

```bash
mgc-v05l research-causal-report \
  --config config/base.yaml \
  --config config/replay.yaml \
  --csv /path/to/mgc_replay.csv \
  --output /path/to/causal_momentum_report.csv
```

## Schwab Auth Flow

Generate the authorization URL:

```bash
mgc-v05l schwab-auth-url
```

Exchange the returned auth code:

```bash
mgc-v05l schwab-exchange-code --code "<returned-auth-code>"
```

Refresh later if needed:

```bash
mgc-v05l schwab-refresh-token
```

## Fetch Historical Bars

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
  --frequency 5
```

Or use a local mapping file based on `config/schwab.local.example.json`:

```bash
cp config/schwab.local.example.json config/schwab.local.json
mgc-v05l schwab-fetch-history \
  --config config/base.yaml \
  --config config/replay.yaml \
  --schwab-config config/schwab.local.json \
  --internal-symbol MGC \
  --period-type day \
  --period 1 \
  --frequency-type minute \
  --frequency 5 \
  --persist
```

`--persist` saves normalized bars into the configured SQLite database.

## Fetch Quotes

```bash
mgc-v05l schwab-fetch-quote \
  --config config/base.yaml \
  --config config/replay.yaml \
  --internal-symbol MGC \
  --quote-symbol "<confirmed-schwab-quote-symbol>"
```

Quote fetching is for inspection only and does not feed strategy decisions yet.

## Persistence Storage

Persistence is stored in SQLite at the configured `database_url`.

Common local files:
- replay: `./mgc_v05l.replay.sqlite3`
- paper: `./mgc_v05l.paper.sqlite3`
- base/default: `./mgc_v05l.sqlite3`
- Schwab local token file: `.local/schwab/tokens.json`

## Inspect Results

Current inspection paths:
- replay CLI JSON summary on stdout
- historical bar JSON output from `schwab-fetch-history`
- quote JSON output from `schwab-fetch-quote`
- SQLite tables for bars, features, signals, state snapshots, order intents, fills, and processed bars

Example SQLite inspection:

```bash
sqlite3 ./mgc_v05l.replay.sqlite3 ".tables"
sqlite3 ./mgc_v05l.replay.sqlite3 "select * from bars limit 5;"
sqlite3 ./mgc_v05l.replay.sqlite3 "select * from processed_bars limit 5;"
sqlite3 ./mgc_v05l.replay.sqlite3 "select * from order_intents limit 5;"
sqlite3 ./mgc_v05l.replay.sqlite3 "select * from fills limit 5;"
```

## Known Limitations

- live broker implementation is still deferred beyond the abstract interface and paper broker
- live Schwab polling and streaming remain placeholders pending confirmed endpoint details
- the exact Schwab futures symbol format for MGC must be configured explicitly
- quote fetching is implemented for inspection only and is not part of strategy decisions
- config loading still uses the current flat YAML strategy files plus optional JSON mapping for Schwab symbols
