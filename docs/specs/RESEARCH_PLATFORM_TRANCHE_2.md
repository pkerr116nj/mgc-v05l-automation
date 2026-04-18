# Research Platform Tranche 2

## What changed

This tranche moves the ATP research platform from "persisted ATP substrate exists" toward a broader reusable platform with:

- project-level source/context services
- wider ATP runner reuse of persisted scope truth
- a durable experiment registry
- first application-facing research analytics outputs

## Updated dependency flow

Current shared path after tranche 2:

1. raw bars and source selection
2. symbol-context bundle
3. ATP feature bundle
4. ATP scope bundle
5. overlays / runner-specific experiment logic
6. experiment registry
7. app-facing research analytics datasets
8. app/operator payload consumption

## New platform boundary

Project-level shared layers:

- `src/mgc_v05l/research/platform/source_context.py`
- `src/mgc_v05l/research/platform/datasets.py`
- `src/mgc_v05l/research/platform/registry.py`
- `src/mgc_v05l/research/platform/analytics.py`

ATP-specific layers on top:

- `src/mgc_v05l/research/trend_participation/substrate.py`
- `src/mgc_v05l/research/trend_participation/outcome_engine.py`
- ATP review / governance / shaping runners under `src/mgc_v05l/app/atp_*`

## App-facing analytics layer

The research platform now publishes a stable analytics payload under:

- `outputs/research_platform/analytics/atp_companion/manifest.json`
- `outputs/research_platform/analytics/atp_companion/app_payload.json`

Datasets currently published:

- `strategy_catalog`
- `daily_pnl`
- `strategy_summaries`
- `equity_curve`
- `drawdown_curve`
- `trade_blotter`
- `exit_reason_breakdown`
- `session_breakdown`

These are intended to power:

- P/L Calendar
- Strategy Deep Dive
- one / many / all strategy aggregation

The application should consume these datasets or the derived app payload rather than reconstructing research logic from raw bundles.

## Remaining debt

- source/context loading is platformized, but not every adjacent strategy family consumes it yet
- ATP runners now reuse bundles more broadly, but not all research families are registered
- the registry is real, but backfill across older ATP runs is still pending
- analytics are first-class enough for app consumption, but deeper app integration remains future work
- some legacy MGC-centric naming still exists outside the research platform layers
