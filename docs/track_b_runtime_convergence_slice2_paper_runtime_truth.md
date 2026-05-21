# Track B Runtime Convergence Slice 2 Paper Runtime Truth

Date: 2026-05-21

Scope: PAPER-only heartbeat/truth artifact for the Track B PAPER runtime. This slice adds evidence only. It does not change submit readiness authority, self-healing restart decisions, broker/lifecycle behavior, strategy thresholds, session policy, or route authority.

## Artifact

Path:

`outputs/probationary_pattern_engine/paper_session/runtime/paper_runtime_truth.json`

Producer:

- `ProbationaryPaperSupervisor` for the active multi-lane Track B PAPER runtime.
- `ProbationaryPaperRunner` for legacy/single-lane PAPER runs.

Consumers in this slice:

- `scripts/show_headless_supervised_paper_status.sh` surfaces it as `paper_runtime_truth_*` evidence-only fields.
- `track_b_self_healing_supervisor` reads it as an optional non-required artifact on the `paper_runtime` agent.

It is not canonical readiness authority yet.

## Required Fields

- `runtime_instance_id`
- `service_name = track_b_paper_runtime`
- `producer_pid`
- `producer_root`
- `generated_at`
- `last_success_at`
- `freshness_ttl_seconds`
- `freshness_state`
- `heartbeat_state`
- `writer_authority`
- `source_commit`
- `config_fingerprint`
- `runtime_mode = PAPER`
- `restart_generation`
- `duplicate_writer_detection`
- `stale_reason`
- `recovery_state`
- `lane_count`
- `b_plus_threshold`
- `test_mule_enabled`

Additional safety labels:

- `paper_only=true`
- `live_money_eligible=false`
- `submit_authority=false`
- `readiness_authority=false`
- `restart_authority=false`

## Runtime Instance ID

The runtime creates one `runtime_instance_id` per process object:

`track-b-paper-runtime-{started_at_utc}-{pid}`

Slice 3 should lift this into launcher-owned runtime metadata so the PID file, launchctl label, config stack, operator status, and runtime truth all share one generation ID.

## Config Fingerprint

The artifact hashes the current `paper_config_in_force.json` payload with SHA-256 after JSON key sorting. This is evidence that the runtime truth record was generated against a concrete active config stack. It does not yet replace the existing launcher/config command-line checks.

## Evidence-Only Consumption

Headless status adds:

- `paper_runtime_truth_artifact`
- `paper_runtime_truth_evidence_only=true`
- `paper_runtime_truth_present`
- `paper_runtime_truth_freshness_state`
- `paper_runtime_truth_heartbeat_state`
- `paper_runtime_truth_writer_authority`
- `paper_runtime_truth_runtime_instance_id`
- `paper_runtime_truth_lane_count`
- `paper_runtime_truth_b_plus_threshold`
- `paper_runtime_truth_test_mule_enabled`

Self-healing adds the artifact under `agents.paper_runtime.artifacts` with `required=false`.

Missing, stale, wrong-root, or duplicate-writer truth is visible as evidence in this slice, but it does not yet change canonical readiness or restart eligibility.

## Next Slice

Recommended Slice 3: stale PID and generation convergence.

- Create launcher-owned runtime metadata beside `probationary_paper.pid`.
- Generate `runtime_instance_id` and `restart_generation` before process start.
- Pass the generation into the runtime environment.
- Add the same generation to `paper_runtime_truth.json`, `operator_status.json`, and `paper_config_in_force.json`.
- Teach status/self-healing to flag PID-only evidence as insufficient, still without changing submit authority until validated.
