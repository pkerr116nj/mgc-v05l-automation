# Track B Runtime Operability Contract

This contract keeps Track B PAPER runtime status and recovery decisions tied to one explicit authority map. It is read-only and does not change strategy logic, active PAPER configuration, broker state, lifecycle state, or live-money eligibility.

## Canonical Authority

| Surface | Path | Role |
| --- | --- | --- |
| Active PAPER config | `outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json` | Only active lane/config source in force. |
| Runtime truth | `outputs/probationary_pattern_engine/paper_session/runtime/paper_runtime_truth.json` | Current PAPER runtime process, generation, lane count, heartbeat, and writer truth. |
| Canonical readiness | `outputs/operator_dashboard/runtime/latest_canonical_readiness.json` | Canonical readiness detail consumed by operators and recovery. |
| Broker/lifecycle reconciliation | `outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json` | Broker/lifecycle reconciliation truth consumed by readiness. |

## Diagnostic Or Projection Only

| Surface | Path | Role |
| --- | --- | --- |
| Canonical readiness summary | `outputs/operator_dashboard/runtime/latest_canonical_readiness_summary.json` | Compact display projection. |
| Runtime supervisor authority | `outputs/track_b_execution_core/runtime_supervisor/latest_runtime_supervisor_authority.json` | Read-only supervisor advisory. It cannot execute recovery. |
| Hourly recovery audit | `outputs/track_b_execution_core/runtime_recovery/latest_hourly_runtime_recovery_audit.json` | Scheduler and recovery audit evidence. |
| Operator dashboard readiness | `outputs/operator_dashboard/runtime/operator_dashboard_readiness.json` | Dashboard display projection. The dashboard is not on the runtime critical path. |

## Quarantined Deprecated Surfaces

Legacy launch metadata under `outputs/probationary_pattern_engine/paper_session/runtime/_quarantine_stale_launch_metadata_20260522_ee53089/` is stale/deprecated. It cannot imply submit authority, runtime health, current lane count, active config, or recovery eligibility.

## Readiness States

| State | Meaning |
| --- | --- |
| `READY_SUBMIT_CAPABLE` | Safety gates are clean and guarded PAPER runtime can submit through approved authority. |
| `READY_DIAGNOSTIC_ONLY` | Runtime may be observable, but cannot be treated as submit-capable. |
| `BLOCKED_SAFETY` | A hard PAPER safety invariant failed. |
| `BLOCKED_INFRASTRUCTURE` | A required service or dependency is not ready. |
| `BLOCKED_CONFIG` | Active PAPER config or root/config truth is invalid. |
| `BLOCKED_STALE_TRUTH` | An authority artifact is stale, missing, or contradictory. |

## Freshness Policy

Authority artifacts can block runtime when stale or missing. Diagnostic projections can warn but cannot grant or remove submit authority. Replay, research, historical recovery, and report artifacts are evidence only and cannot block runtime startup unless explicitly promoted into the authority map.

## Operator Command

```bash
scripts/track_b_runtime_operability_status.sh
```

The command writes and prints `outputs/track_b_execution_core/runtime_operability/latest_runtime_operability_contract.json`.
