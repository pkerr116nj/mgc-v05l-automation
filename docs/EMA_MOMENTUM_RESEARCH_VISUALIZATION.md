# EMA Momentum Research Visualization

This visualization layer is a lightweight historical inspection tool for persisted MGC research data. It is meant for debugging and sanity checking, not for live trading or production decision-making.

## What It Shows

Main price panel:

- candles from persisted `bars`
- VWAP overlay when available from `derived_features`
- smoothed-close overlay when available
- markers for:
  - `trigger_long_math`
  - `trigger_short_math`
  - `structure_long_candidate`
  - `structure_short_candidate`
- optional detail markers for:
  - `compression_long` / `compression_short`
  - `reclaim_long` / `failure_short`
  - `separation_long` / `separation_short`

Lower feature panels:

- `momentum_norm`
- `momentum_acceleration`
- `signed_impulse`
- `smoothed_signed_impulse`

## How To Run It

```bash
mgc-v05l research-ema-viz \
  --config config/base.yaml \
  --config config/replay.yaml \
  --experiment-run-id 1 \
  --ticker MGC \
  --timeframe 5m \
  --output /tmp/mgc_ema_viz.html
```

Optional filters:

- `--start-timestamp`
- `--end-timestamp`
- `--limit`

The output is a local HTML artifact that can be opened in a browser.

## Why It Exists

This tool is for visually checking whether the persisted EMA momentum features and research labels line up with price structure in a believable way. It helps compare:

- price action
- EMA/derivative feature behavior
- math-trigger labels
- structure-label sequences

## Explicitly Not Included

- live charting
- real-time streaming
- production dashboards
- backtesting
- production signal gating

This layer remains research-only.
