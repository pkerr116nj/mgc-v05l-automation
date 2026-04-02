# Active Trend Participation Engine

Research and strategy-development module for interpretable intraday market-direction participation.

## Current Benchmark

- Current ATP reference candidate:
  `ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only`
- Internal label:
  `ATP_COMPANION_V1_ASIA_US`
- Executable ATP coverage:
  `ASIA + US`
- Diagnostic-only ATP coverage:
  `LONDON`
- Benchmark note:
  [ATP Companion Baseline v1 Benchmark](./specs/ATP_COMPANION_BASELINE_V1_BENCHMARK.md)
- App-facing status:
  tracked paper strategy plus continuous live-data paper runtime, paper only, not live
- Operator note:
  [ATP Companion Baseline v1 Paper Tracking](./specs/ATP_COMPANION_BASELINE_V1_PAPER_TRACKING.md)
- Runbook:
  [ATP Companion Baseline v1 Paper Runtime Runbook](./specs/ATP_COMPANION_BASELINE_V1_PAPER_RUNTIME_RUNBOOK.md)

## Intent

- Baseline ATP benchmark lane: `5m` structural context plus `1m` timing detail
- Research execution lane: may vary structural and execution timeframe truth explicitly by study artifact
- Initial instruments: `MES`, `MNQ`
- Runtime priority: lower than existing live strategies
- Conflict handling: higher-priority strategy wins, but shadow signals remain logged
- Participation bias: active by default in v1; trade frequency is trimmed only if post-cost quality fails

## Mode-Aware Assumptions

- Structural signal timeframe:
  lane-specific, but baseline ATP benchmark truth remains `5m`
- Execution timeframe:
  lane-specific, with ATP benchmark timing detail currently `1m`
- Artifact/reporting timeframe:
  must be recorded explicitly in study and report outputs
- Replay fill convention:
  baseline-parity lane remains `NEXT_BAR_OPEN`; research execution studies must not silently reuse that label for richer execution truth

## Project Structure

- `src/mgc_v05l/research/trend_participation/storage.py`
  Handles SQLite import, normalization, gap detection, resampling, Parquet materialization, and DuckDB view registration.
- `src/mgc_v05l/research/trend_participation/features.py`
  Builds interpretable 5m structural states with embedded 1m agreement context.
- `src/mgc_v05l/research/trend_participation/patterns.py`
  Searches only bounded pattern families:
  `pullback_continuation`, `breakout_continuation`, `pause_resume`, `failed_countertrend_resumption`.
- `src/mgc_v05l/research/trend_participation/backtest.py`
  Applies conservative 1m execution with slippage, fees, no decision-bar fills, and stop-first same-bar conflicts.
- `src/mgc_v05l/research/trend_participation/conflict.py`
  Encodes `no_conflict`, `agreement`, `soft_conflict`, and `hard_conflict_cooldown`.
- `src/mgc_v05l/research/trend_participation/report.py`
  Writes JSON/Markdown promotion-style reports and shortlist outputs.
- `src/mgc_v05l/research/trend_participation/engine.py`
  Orchestrates the full research flow.
- `src/mgc_v05l/research/trend_participation/canary.py`
  Packages the narrower Phase 5 salvage lanes as paper-only experimental canaries with isolated metrics, kill-switch metadata, and lane-local JSONL artifacts for dashboard validation.

## Durable Storage Design

- Historical bars:
  Parquet under `outputs/reports/trend_participation_engine/raw_bars/`
- Derived features:
  Parquet under `outputs/reports/trend_participation_engine/features/`
- Signals:
  Parquet under `outputs/reports/trend_participation_engine/signals/`
- Trades:
  Parquet under `outputs/reports/trend_participation_engine/trades/`
- Research query layer:
  DuckDB file at `outputs/reports/trend_participation_engine/warehouse/trend_participation.duckdb`
- State/metadata:
  JSON manifest at `outputs/reports/trend_participation_engine/manifests/storage_manifest.json`

## Core Schemas

### Raw Bars

- `instrument`
- `timeframe`
- `start_ts`
- `end_ts`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `session_label`
- `session_segment`
- `source`

### Features

- `decision_ts`
- `trend_state`
- `pullback_state`
- `expansion_state`
- `bar_anatomy`
- `momentum_persistence`
- `reference_state`
- `volatility_range_state`
- `mtf_agreement_state`
- `regime_bucket`
- `volatility_bucket`
- `direction_bias`

### Signals

- `variant_id`
- `family`
- `side`
- `decision_ts`
- `conflict_outcome`
- `live_eligible`
- `shadow_only`
- `block_reason`

### Trades

- `variant_id`
- `instrument`
- `entry_ts`
- `exit_ts`
- `entry_price`
- `exit_price`
- `pnl_points`
- `pnl_cash`
- `mfe_points`
- `mae_points`
- `exit_reason`

## Feature Families

- trend/slope state
- pullback depth
- expansion/compression
- bar anatomy
- momentum persistence
- distance from local references
- volatility-normalized range
- multi-timeframe agreement

The module prefers discrete or semi-discrete labels over opaque continuous transformations.

## Active Participation Rules

- Multiple opportunities per session are expected.
- One active position per instrument per candidate variant is enforced.
- Controlled re-entry is allowed after a fresh setup/reset state appears.
- Local cooldowns are configurable but intentionally light in v1.
- Overtrading is diagnosed after cost-adjusted evaluation, not suppressed up front.

## Validation Rules

- Walk-forward evaluation only
- Separate long and short variants
- Total trades and trades per day are treated as first-class outputs
- Session/regime/volatility bucket breakdowns included
- Slippage and fees included
- No decision-bar fill fantasy
- Same-bar stop/target conflicts resolve to stop first

## Conflict Policy

- `no_conflict`: live-eligible
- `agreement`: higher-priority strategy still owns execution; Active Trend Participation Engine logs shadow only
- `soft_conflict`: conflicting directional overlap; shadow only
- `hard_conflict_cooldown`: explicit higher-priority cooldown/hold; shadow only

## CLI

```bash
mgc-v05l research-trend-participation \
  --source-sqlite /path/to/bars.sqlite3 \
  --output-dir outputs/reports/trend_participation_engine \
  --instruments MES MNQ
```

```bash
mgc-v05l research-trend-participation-canary-package \
  --source-sqlite /path/to/bars.sqlite3 \
  --output-dir outputs/probationary_quant_canaries/active_trend_participation_engine \
  --instruments MES MNQ
```

## Wrapper Scripts

- `scripts/backfill_trend_participation_engine.sh`
- `scripts/update_trend_participation_engine.sh`
- `scripts/run_trend_participation_engine.sh`
- `scripts/package_active_trend_participation_canary.sh`
- `scripts/disable_active_trend_participation_canary.sh`
- `scripts/enable_active_trend_participation_canary.sh`

## Dependency Note

Parquet and DuckDB materialization require the research extras:

```bash
pip install -e ".[dev,research]"
```
