# Track B Runtime Convergence Slice 3: Runtime Generation Metadata

Slice 3 adds deterministic runtime identity evidence across the supervised PAPER launcher and runtime artifacts. It does not change submit readiness, restart authority, strategy behavior, broker behavior, or canonical readiness decisions.

## Artifacts

| Artifact | Producer | Purpose | Authority |
| --- | --- | --- | --- |
| `outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid.json` | `scripts/run_headless_supervised_paper_service.sh`, then active PAPER runtime | Launcher/runtime PID metadata with `runtime_instance_id`, `restart_generation`, root, commit, and config fingerprint | Evidence only |
| `outputs/probationary_pattern_engine/paper_session/runtime/paper_runtime_truth.json` | Active PAPER runtime | Heartbeat truth plus lane count, B+ threshold, mule flag, generation, and PID metadata path | Evidence only |
| `outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json` | Active PAPER runtime | Active config/lane stack plus shared generation fields | Existing display/config evidence |
| `outputs/probationary_pattern_engine/paper_session/operator_status.json` | Active PAPER runtime | Runtime/lane status plus shared generation fields | Existing runtime status evidence |

## Generation Contract

The launcher creates these fields before process start:

- `runtime_instance_id`
- `restart_generation`
- `launch_started_at`
- `launcher_pid`
- `expected_project_root`
- `config_fingerprint`

The wrapper passes them to the runtime through `MGC_TRACK_B_*` environment variables. The runtime writes the same `runtime_instance_id` and `restart_generation` into `paper_runtime_truth.json`, `paper_config_in_force.json`, `operator_status.json`, and the PID metadata artifact.

## Detection Added

`scripts/show_headless_supervised_paper_status.sh` now reports evidence-only generation fields:

- `paper_runtime_pid_metadata_state`
- `paper_runtime_generation_mismatches`
- `paper_runtime_generation_duplicate_writer_state`
- `paper_runtime_generation_runtime_instance_id`
- `paper_runtime_generation_restart_generation`

These fields identify stale PID metadata, dead/zombie/wrong-root PID evidence, config/runtime truth mismatches, operator status/runtime truth mismatches, and duplicate writer indicators. They do not override canonical readiness in this slice.

## Non-Goals

- No runtime restart behavior changes.
- No broker/order/lifecycle mutation.
- No submit authority changes.
- No canonical readiness authority changes.
- No strategy, threshold, or session policy changes.

## Recommended Slice 4

Use the generation evidence to harden stale PID elimination and duplicate-writer prevention in launcher/status flows, still before granting self-healing restart authority. The next slice should make stale PID cleanup deterministic while preserving broker/reconciliation fail-closed gates.
