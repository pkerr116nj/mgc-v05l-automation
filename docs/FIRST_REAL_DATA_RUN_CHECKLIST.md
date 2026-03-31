# First Real Data Run Checklist

Use this checklist for the first real-data research run in the current repo. This is operator guidance only.

## 1. Prerequisites

- Python venv created and dependencies installed
- Schwab app credentials available locally:
  - `SCHWAB_APP_KEY`
  - `SCHWAB_APP_SECRET`
  - `SCHWAB_CALLBACK_URL`
- local token path set or default token path available:
  - `SCHWAB_TOKEN_FILE` or `.local/schwab/tokens.json`
- confirmed Schwab symbol mapping for the target instrument
- writable SQLite path from:
  - `config/base.yaml`
  - `config/replay.yaml`

Success check:
- `mgc-v05l schwab-auth-url` prints a valid auth URL
- auth code exchange succeeds and token file exists locally

Failure check:
- missing env vars
- auth URL does not open the retail Schwab flow
- token exchange/refresh fails

## 2. High-Level Sequence

1. Complete Schwab auth and token setup.
2. Fetch historical bars from Schwab with the confirmed symbol mapping.
3. Normalize and persist bars into SQLite.
4. Create the first experiment run.
5. Compute and persist EMA research features.
6. Run the research evaluator and structure-label path.
7. Run the research report/export.
8. Generate the research visualization HTML artifact.

Success check:
- each step completes without exceptions

Failure check:
- zero bars returned
- persistence tables remain empty
- report/visualization rows are zero unexpectedly

## 3. Verify Persistence

Verify these tables are populated for the run:

- `bars`
- `derived_features`
- `signal_evaluations`
- `experiment_runs`

Minimum practical checks:

- `experiment_runs` has the new `experiment_run_id`
- `bars` count is nonzero for the target `ticker` and `timeframe`
- `derived_features` count is nonzero for the run
- `signal_evaluations` count is nonzero for the run

Success check:
- counts are nonzero and roughly aligned by bar count

Failure check:
- run exists but `derived_features` or `signal_evaluations` are zero
- row counts are badly mismatched

## 4. Run The Research Report

```bash
mgc-v05l research-ema-eval-report \
  --config config/base.yaml \
  --config config/replay.yaml \
  --experiment-run-id <RUN_ID> \
  --output /tmp/ema_eval_report.csv
```

Success check:
- terminal JSON summary prints
- `rows` is nonzero
- CSV file is created

Failure check:
- `rows: 0` when you expected populated research data
- missing output file

## 5. Run The Research Visualization

```bash
mgc-v05l research-ema-viz \
  --config config/base.yaml \
  --config config/replay.yaml \
  --experiment-run-id <RUN_ID> \
  --ticker MGC \
  --timeframe 5m \
  --output /tmp/mgc_ema_viz.html
```

Success check:
- HTML artifact is created
- chart shows candles, overlays, and research markers

Failure check:
- empty chart
- no rows loaded for the selected run/ticker/timeframe

## 6. Final Operator Checkpoint

The first real-data run is in good shape if:

- Schwab auth/token flow succeeded
- historical bars were persisted
- experiment run exists
- `derived_features` and `signal_evaluations` are populated
- report/export returns nonzero rows
- visualization artifact renders expected price and research overlays
