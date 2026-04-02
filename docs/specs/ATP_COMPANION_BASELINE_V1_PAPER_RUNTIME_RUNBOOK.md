# ATP Companion Baseline v1 Paper Runtime Runbook

## Purpose

This runbook covers the continuously running paper runtime for:
- `ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only`
- internal label: `ATP_COMPANION_V1_ASIA_US`

This runtime is:
- paper only
- single strategy
- single symbol
- backed by live/current market data

This runtime is not:
- live order routing
- a semantic change to the frozen ATP benchmark
- permission to widen production scope

## Launch

Connected-environment prerequisites:

```bash
bash scripts/run_schwab_auth_gate.sh
```

Verified local prerequisites for this runtime:
- `config/schwab.local.json`
- `.local/schwab_env.sh`
- `.local/schwab/tokens.json`
- repo virtualenv under `.venv`

Start the benchmark paper runtime:

```bash
bash scripts/run_probationary_paper_soak.sh --background
```

Stop the benchmark paper runtime:

```bash
bash scripts/stop_probationary_paper_soak.sh
```

Send operator controls:

```bash
bash scripts/run_probationary_operator_control.sh --action halt_entries --shared-strategy-identity ATP_COMPANION_V1_ASIA_US
bash scripts/run_probationary_operator_control.sh --action resume_entries --shared-strategy-identity ATP_COMPANION_V1_ASIA_US
bash scripts/run_probationary_operator_control.sh --action flatten_and_halt --shared-strategy-identity ATP_COMPANION_V1_ASIA_US
bash scripts/run_probationary_operator_control.sh --action stop_after_cycle --shared-strategy-identity ATP_COMPANION_V1_ASIA_US
```

Recommended startup order:
1. Run `bash scripts/run_schwab_auth_gate.sh`
2. Start the ATP paper runtime
3. Start the operator dashboard
4. Confirm the tracked strategy row is present and attached before leaving the runtime unattended

## What “Paper Mode” Means Here

- current market data is consumed continuously
- ATP benchmark signals are evaluated under the frozen benchmark rules
- intents and fills are persisted as paper artifacts only
- state transitions remain fill-driven
- no live broker order submission is performed

## Confirm It Is Attached

In the app tracked-strategy detail, confirm:
- strategy id is `atp_companion_v1_asia_us`
- status is `READY`, `IN_POSITION`, `RECONCILING`, or `FAULT`
- `runtime_attached = true`
- runtime heartbeat age is current
- `data_stale = false` during an active market session
- `data_stale = true` is expected during a closed market once the last finalized bar ages out
- latest processed bar time is updating
- lane count remains `1`
- observed instruments remain `["MGC"]`

## How To Read Health And Reconciliation Flags

- `READY`: runtime is attached, entries are enabled, and no open paper position exists
- `IN_POSITION`: runtime is attached and a paper position is open
- `RECONCILING`: restart restore, stale heartbeat, or uncertain runtime attachment means persisted truth is being shown conservatively
- `FAULT`: persisted fault evidence exists and operator review is required
- `data_stale = true`: current bar data or runtime heartbeat is stale; fail closed and investigate
- `duplicate_bar_suppression_count`: duplicate completed bars have been ignored instead of reprocessed

## Inspectable Audit Surfaces

The tracked strategy detail view exposes recent:
- processed bars
- signals
- order intents
- fills
- state snapshots
- faults
- reconciliation events

Use those app-facing surfaces before inspecting raw SQLite or JSONL artifacts directly.

Primary persisted ATP benchmark artifacts:
- `outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_asia_us/operator_status.json`
- `outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_asia_us/processed_bars.jsonl`
- `outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_asia_us/signals.jsonl`
- `outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_asia_us/order_intents.jsonl`
- `outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_asia_us/fills.jsonl`
- `outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_asia_us/reconciliation_events.jsonl`
- `outputs/operator_dashboard/paper_tracked_strategies_snapshot.json`
- `outputs/operator_dashboard/paper_tracked_strategy_details_snapshot.json`

## Restart Behavior

After process restart:
- persisted runtime state is reloaded
- the tracked strategy remains visible from persisted truth
- if runtime reattachment is uncertain, the app shows `RECONCILING`
- duplicate bar suppression prevents already-processed finalized bars from being replayed into new paper events

The ATP benchmark runtime also uses a dedicated operator-control file:
- `outputs/probationary_pattern_engine/paper_session/runtime/operator_control.json`

That isolates ATP halt/resume/flatten/stop commands from any separately running generic paper supervisor on the same machine.

## Operator Control Interpretation

- `halt_entries`: stop new ATP paper entries, keep runtime attached, preserve current flat/open state truthfully
- `resume_entries`: clear operator halt and allow new ATP paper entries again
- `flatten_and_halt`: if flat, the runtime reports `Runtime halted and already flat`; if a paper position is open, it flattens first and then halts
- `stop_after_cycle`: halt new entries and exit the runtime at the next safe cycle boundary; this is the preferred controlled shutdown path for soak sessions

## Connected Soak Notes

Connected soak evidence from the March 28, 2026 readiness pass showed:
- auth gate passed against the real Schwab environment
- ATP benchmark runtime attached successfully on real/current market-data wiring
- the dashboard tracked row updated from persisted runtime truth
- `halt_entries`, `resume_entries`, `flatten_and_halt`, and `stop_after_cycle` all worked through the dedicated ATP control path
- the session occurred on a Saturday, so stale-data behavior was expected and useful to verify

## Scope Guardrails

Still out of scope:
- live broker order execution
- London ATP execution
- promotion/add logic
- exit redesign
- multi-symbol orchestration
- portfolio management
