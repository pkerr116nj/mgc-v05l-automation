# Project Status

## Current Build State

The current v0.5l build is a shared strategy platform with distinct replay, research, paper, and tightly gated live provenance lanes:
- typed settings and locked implementation decisions
- completed-bar session classification
- feature calculation, signal generation, state tracking, risk, and family-specific exits
- deterministic replay execution with `NEXT_BAR_OPEN` fills
- SQLite persistence for replay safety and restart inspection
- a paper broker only
- lane-level participation policy, including explicit single-entry and staged same-direction participation modes

## Implemented

Production-path implementation currently includes:
- typed domain models and enums
- typed settings validation
- session clock and bar model
- feature engine and swing tracking
- Bull Snap, Bear Snap, and Asia VWAP reclaim signal modules
- entry resolution with VWAP precedence
- risk and exit engines
- invariants and fill-driven state machine transitions
- strategy engine orchestration
- staged participation state tracking with explicit entry-leg persistence
- replay CSV ingestion
- processed-bar persistence and duplicate suppression
- order intent persistence
- fill persistence
- replay runner and CLI

## Deferred

Still intentionally deferred:
- live broker integration beyond the abstract interface
- full reconciliation workflow
- production monitoring/alerting depth
- operator controls beyond current scaffolding
- any change to current strategy rules from research-only features

## Research-Only / Experimental

The following are explicitly research-only:
- causal momentum-shape features in `src/mgc_v05l/research/`
- replay report export for those features

These modules do not participate in current production entry or exit decisions.

## Provenance Architecture

Replay remains one provenance lane, not the governing platform identity. Shared architecture now supports replay, research, paper, and future live execution under the same core state/execution model, with lane-level controls for:
1. participation policy
2. session restrictions
3. approved entry sources
4. persistence and artifacts

Frozen benchmark lanes may still keep explicit single-entry or fixed-session assumptions in their own lane configs. ATP candidate paper/research lanes are now allowed to use staged participation when their configs explicitly opt into it; that support is no longer treated as out of scope for the shared paper path.

Replay execution today is still:
1. replay CSV ingest
2. session classification
3. feature computation
4. signal evaluation
5. state/risk/exit evaluation
6. intent creation
7. deterministic paper execution on next bar open
8. fill-driven state transition
9. persistence of bars, features, signals, intents, fills, and state snapshots

## Major Modules

- `src/mgc_v05l/market_data/`: bars, session clock, replay feed, processed-bar tracking
- `src/mgc_v05l/indicators/`: ATR/EMA/VWAP/features and swings
- `src/mgc_v05l/signals/`: Bull Snap, Bear Snap, Asia VWAP reclaim, entry resolution
- `src/mgc_v05l/strategy/`: state machine, invariants, risk, exits, orchestration
- `src/mgc_v05l/execution/`: order models, paper broker, replay execution engine, broker interface
- `src/mgc_v05l/persistence/`: SQLite schema, repositories, state snapshots
- `src/mgc_v05l/app/`: bootstrap, container, runner, CLI
- `src/mgc_v05l/research/`: isolated experimental feature analysis

## Test Coverage

Current test coverage is focused on:
- locked settings and typed models
- session classification and feature calculations
- signal behavior and anti-churn/cooldown handling
- risk, break-even, and exit priorities
- invariants and fill transitions
- persistence and replay fill behavior
- replay end-to-end path
- experimental causal momentum causality and synthetic-sequence behavior

## Replay Path

Replay can be run through:
- `mgc-v05l replay --csv /path/to/replay.csv`
- or `python -m mgc_v05l.app.main replay ...`

Replay input must use the locked columns:
- `timestamp,open,high,low,close,volume`

## Recommended Next Build Stages

Recommended next work, in order:
1. observability polish for replay and paper runs
2. structured audit/log outputs and inspectable summaries
3. fault and mismatch handling hardening without expanding live integration yet
4. paper-mode soak and restart-restore validation
5. only after that, carefully staged live-broker integration
