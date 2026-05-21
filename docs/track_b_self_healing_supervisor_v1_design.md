# Track B Self-Healing Supervisor V1 Design

## Purpose

Track B PAPER needs a supervisor that can make stale or dead operational agents impossible to miss, and eventually restart safe sidecars without creating broker/order risk. This first slice is deliberately read-only. It defines the health contract and a classifier that says whether self-healing is ready, degraded, restart-eligible, blocked, operator-required, or unsafe because broker/lifecycle truth is ambiguous.

The supervisor must never submit, cancel, close, flatten, mutate lifecycle, invoke `paper_proof`, or grant live-money eligibility.

## Authority Boundary

Authoritative inputs:

- Dev-root PID/root/process validation.
- Canonical Track B readiness full artifact.
- Broker-truth lease.
- Phase-1 broker reconciliation.
- Agent heartbeat/status artifacts.
- PAPER safety flags, especially `live_money_eligible=false`.

Diagnostic-only inputs:

- Dashboard display snapshots.
- Legacy compact readiness panels.
- Startup/operability presentation snapshots.
- Human-facing dashboard cards.

Presentation artifacts may explain a degraded state, but they must not be restart or submit authority.

## Health Contract Artifact

Default artifact path:

`outputs/operator_dashboard/runtime/latest_track_b_self_healing_health.json`

The artifact schema is defined by `track_b_self_healing_health_v1` in:

`src/mgc_v05l/execution_core/track_b_self_healing_supervisor.py`

This first implementation exposes:

- Static agent registry.
- Read-only artifact/PID aggregation.
- Pure health classification.
- Explicit restart candidates.
- Explicit restart blockers.

It does not execute restarts.

## Agent Registry

| Agent | Required | Restart Eligible | Expected Process | PID/Root Validation | Heartbeat/Artifact Paths | TTL | Operator-Required States |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Phase-1 candle supervisor/listener | Yes | Yes | `mgc_v05l.execution_core.phase1_databento_live_runtime_candles --mode service` | `var/phase1_databento_live_candles_service.pid`, `var/phase1_databento_live_candles_child.pid`, cwd must be Dev root | `outputs/reports/phase1_databento_live_runtime_candles/latest_phase1_databento_live_listener_status.json`, `latest_phase1_databento_live_supervisor_status.json` | 180s | Databento env/auth missing |
| Broker truth refresher | Yes | Yes | `mgc_v05l.app.ibkr_broker_truth_refresher --service --read-only` | `var/track_b_broker_truth_refresh_service.pid`, cwd must be Dev root | `outputs/reports/ibkr_broker_truth_refresh/latest_broker_truth_refresh_status.json`, `outputs/operator_dashboard/runtime/latest_broker_truth_lease.json` | 150s | TWS/manual review required |
| Operator readiness refresher | Yes | Yes | `mgc_v05l.app.track_b_operator_readiness_refresher --service` | `var/track_b_operator_readiness_refresh_service.pid`, `var/track_b_operator_readiness_refresh_child.pid`, cwd must be Dev root | `var/track_b_operator_readiness_refresh_heartbeat.json`, `outputs/reports/track_b_operator_readiness_refresher/latest_track_b_operator_readiness_refresher_status.json`, canonical readiness | 180s | Readiness refresh operator-required |
| PAPER runtime | Yes | No in V1 | `mgc_v05l.app.main probationary-paper-soak` | `outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid`, cwd must be Dev root | `outputs/probationary_pattern_engine/paper_session/operator_status.json`, canonical readiness | 180s | Open managed position or runtime restart approval required |
| Operator dashboard/backend | No | Yes | `mgc_v05l.app.operator_dashboard` | `outputs/operator_dashboard/runtime/operator_dashboard.pid`, cwd must be Dev root | `outputs/operator_dashboard/runtime/operator_dashboard.json`, `operator_dashboard_readiness.json` | 180s | Port conflict |

## Classifications

- `SELF_HEALING_READY`: required agents are healthy, artifacts are fresh, and broker/lifecycle state is safe.
- `DEGRADED_RECOVERABLE`: a required agent is degraded, but this slice does not yet authorize auto-restart.
- `AUTO_RESTART_ELIGIBLE`: at least one required restart-eligible sidecar is unhealthy and no global blocker is present.
- `AUTO_RESTART_BLOCKED`: restart candidates exist, but a global safety/root/duplication blocker prevents automation.
- `OPERATOR_REQUIRED`: a health source or contract state requires human review.
- `UNSAFE_BROKER_STATE`: broker/lifecycle truth is ambiguous or unsafe.

## Global Auto-Restart Blockers

Auto-restart is blocked if any of these are true:

- Unknown broker open orders.
- Lifecycle review-required state.
- Broker/reconciliation mismatch.
- `live_money_eligible=true`.
- Wrong-root process.
- Duplicate conflicting runtimes.

The PAPER runtime is not auto-restart eligible in V1. A future slice may add explicit operator-approved runtime restart plans after broker/lifecycle state is proven safe and flat.

## Intended V1 Behavior

The V1 supervisor may produce an artifact and dashboard/status warnings. It may recommend that a sidecar is restart-eligible, but it must not execute the restart. Runtime restart remains operator-approved only.

## Recommended Second Slice

Add a CLI/status script that writes `latest_track_b_self_healing_health.json`, surfaces it in Execution Truth, and optionally restarts only sidecars whose contracts are `AUTO_RESTART_ELIGIBLE`. Keep PAPER runtime restart behind an explicit operator approval gate.
