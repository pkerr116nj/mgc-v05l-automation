# ATP Companion Baseline v1 Paper Tracking

## Purpose

This note explains how the current ATP companion benchmark appears in the app as a tracked paper strategy and how that tracked view changes once a live-attached paper runtime is running.

Tracked strategy:
- `ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only`
- internal label: `ATP_COMPANION_V1_ASIA_US`
- strategy id: `atp_companion_v1_asia_us`

## Registration Vs Runtime

Static tracked registration means:
- the strategy is enumerated in the app automatically
- benchmark identity, config source, and persisted paper history are visible
- the app can still show `RECONCILING` from persisted truth after restart even if no runtime is attached

Live-attached paper runtime means:
- the tracked strategy is backed by an active continuous paper runtime
- latest processed bar time, runtime heartbeat, runtime attachment, and stale-data health are updated near real time
- signals, intents, fills, state snapshots, reconciliation rows, and faults are inspectable from the app-facing layer

## What The App Shows

Tracked strategy list:
- display name
- environment
- current tracked status
- enabled / disabled
- current session segment
- current position side
- realized paper P/L
- open paper P/L if supportable
- last update timestamp

Tracked strategy detail:
- benchmark description
- current config identity
- latest tracked state snapshot
- runtime attached / not attached
- runtime heartbeat age
- stale-data flag
- recent bars
- recent signals
- recent order intents
- recent fills
- recent state snapshots
- recent reconciliation events
- recent fault events
- cumulative paper metrics
- last trade summary

## Telemetry Meaning

- `READY` means the tracked paper strategy is enabled and waiting for eligible paper setups.
- `IN_POSITION` means a persisted paper position is currently open.
- `RECONCILING` means restart restoration or reconciliation truth is still uncertain and the app is showing the persisted benchmark state conservatively.
- `FAULT` means persisted fault evidence is present.
- `DISABLED` means the tracked strategy is registered but entries are not currently enabled.

Runtime-health fields:
- `runtime_attached` means the tracked strategy currently has an active paper runtime bound to the registry entry.
- `runtime_heartbeat_age_seconds` measures how stale the attached runtime heartbeat is.
- `data_stale` means the runtime heartbeat or data freshness is no longer current enough to trust a healthy `READY` state.
- `duplicate_bar_suppression_count` is an operator-facing count of ignored duplicate completed bars during continuous polling.

Operator controls available from the tracked strategy surface:
- start paper runtime
- stop paper runtime
- halt new entries
- resume entries
- flatten and halt
- stop after current cycle

## Live Boundary

This tracked strategy is not live trading.

It is:
- paper only
- backed by persisted paper/runtime truth
- intended for observability and paper-soak validation

It is not:
- live order routing
- authorization to widen production scope
- a replacement for the legacy v0.5l locked baseline

## Runtime Entry Point

The tracked strategy becomes live-attached in paper mode through the benchmark runtime overlay:
- shared paper config: `config/probationary_pattern_engine_paper.yaml`
- start script: `scripts/run_probationary_paper_soak.sh`
- stop script: `scripts/stop_probationary_paper_soak.sh`
- operator control path: `scripts/run_probationary_operator_control.sh --shared-strategy-identity ATP_COMPANION_V1_ASIA_US`
- shared operator-control file: `outputs/probationary_pattern_engine/paper_session/runtime/operator_control.json`

## Connected Environment Bootstrap

Before treating the tracked strategy as live-attached paper runtime truth, verify:
- Schwab bootstrap env is present through `.local/schwab_env.sh`
- token store exists at `.local/schwab/tokens.json`
- Schwab config exists at `config/schwab.local.json`
- auth gate passes through `bash scripts/run_schwab_auth_gate.sh`

Connected paper monitoring means:
- the tracked strategy can continue to appear even if the generic probationary paper supervisor is also running
- ATP benchmark controls remain isolated through the benchmark-specific control file
- the tracked view is allowed to fall back to persisted lane-local artifacts when the generic paper lane registry does not currently enumerate the ATP benchmark lane

## Closed-Market Truth

During a closed market:
- `runtime_attached` can still be true if the benchmark runtime heartbeat is current
- `data_stale` is expected once the last finalized bar ages out
- `RECONCILING` is the correct conservative app-facing status even when the benchmark runtime itself is healthy, because current market data is no longer fresh enough to claim `READY`
- no new paper trades are required for telemetry validation; attachment, controls, persistence, and stale-data truth are still valid soak evidence

## Pre-Live Requirement

This tracked paper strategy surface is the required pre-live observability stage for ATP companion work.

Future ATP research should branch from the frozen benchmark and use this tracked paper surface to validate:
- restart safety
- paper telemetry truth
- session behavior
- paper performance stability

See also:
- [ATP Companion Baseline v1 Benchmark](./ATP_COMPANION_BASELINE_V1_BENCHMARK.md)
- [ATP Companion Baseline v1 Paper Runtime Runbook](./ATP_COMPANION_BASELINE_V1_PAPER_RUNTIME_RUNBOOK.md)
