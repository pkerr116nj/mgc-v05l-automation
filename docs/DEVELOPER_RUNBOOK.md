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

## Run Tests

Full suite:

```bash
PYTHONPATH=src .venv/bin/pytest tests/unit tests/integration -q
```

Compile check:

```bash
PYTHONPYCACHEPREFIX=.pycache python3 -m compileall src tests
```

## Run Replay

Use the replay CLI with the base and replay configs:

```bash
PYTHONPATH=src .venv/bin/python -m mgc_v05l.app.main replay \
  --config config/base.yaml \
  --config config/replay.yaml \
  --csv /path/to/mgc_replay.csv
```

Installed script form:

```bash
mgc-v05l replay --csv /path/to/mgc_replay.csv
```

Replay prints a JSON summary to stdout.

## Run Experimental Causal Momentum Report

This does not affect production strategy decisions.

```bash
PYTHONPATH=src .venv/bin/python -m mgc_v05l.app.main research-causal-report \
  --config config/base.yaml \
  --config config/replay.yaml \
  --csv /path/to/mgc_replay.csv \
  --output /path/to/causal_momentum_report.csv
```

The output CSV includes:
- timestamps
- OHLCV
- ATR
- smoothed price
- first derivative
- second derivative
- normalized slope and curvature
- momentum-shape boolean flags

## Persistence Storage

Persistence is stored in SQLite at the configured `database_url`.

Default examples:
- replay: `./mgc_v05l.replay.sqlite3`
- paper: `./mgc_v05l.paper.sqlite3`
- base/default: `./mgc_v05l.sqlite3`

## Inspect Results

Current inspection paths:
- replay CLI JSON summary on stdout
- SQLite tables for bars, features, signals, state snapshots, order intents, fills, and processed bars
- optional causal momentum report CSV

Example SQLite inspection:

```bash
sqlite3 ./mgc_v05l.replay.sqlite3 ".tables"
sqlite3 ./mgc_v05l.replay.sqlite3 "select * from processed_bars limit 5;"
sqlite3 ./mgc_v05l.replay.sqlite3 "select * from order_intents limit 5;"
sqlite3 ./mgc_v05l.replay.sqlite3 "select * from fills limit 5;"
```

## Known Limitations

- no live broker implementation yet beyond the interface
- no full reconciliation workflow yet
- no structured production logging pipeline yet beyond persisted artifacts and CLI summaries
- research features are isolated and not used by production strategy decisions
- config loading expects the current flat YAML file style
