# Track B Runtime Convergence Slice 1 Contract

Date: 2026-05-21

Scope: PAPER-only inventory and shared contract. This slice does not change restart behavior, broker behavior, lifecycle behavior, strategy thresholds, or session policy.

## Objective

Track B needs one operational truth language for runtime liveness, heartbeat freshness, writer ownership, duplicate runtime detection, and restart eligibility. Today those facts are spread across launcher artifacts, canonical readiness, self-healing health, dashboard display state, broker-truth refresh state, operator-readiness refresh state, Phase-1 data artifacts, and reconciliation gates.

The target hierarchy is:

1. Authoritative runtime/service heartbeat and PID/root ownership.
2. Canonical readiness consumes authoritative service truth and broker/data safety truth.
3. Self-healing consumes canonical readiness plus explicit restart-safety gates.
4. Dashboard displays canonical/self-healing truth and labels stale derived display state as display-only.

## Artifact Inventory

| Artifact | Producer | Consumers | Authority | Freshness / TTL | PID / Root Semantics | Duplicate Writer Detection | Schema |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid` | `scripts/run_probationary_paper_soak.sh`, canonical wrapper launched by `scripts/run_headless_supervised_paper_service.sh` | stop script, launcher, canonical readiness, self-healing, dashboard/status | Authoritative candidate for PAPER runtime PID, only after process command/cwd validation | No timestamp; liveness is process probe | PID must be live, non-zombie, command contains `mgc_v05l.app.main probationary-paper-soak`, cwd must be Dev root | Indirect via process scan/self-healing duplicate runtime count | Plain PID, no schema |
| `outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper_launch_status.json` | `scripts/run_probationary_paper_soak.sh --background` | operators/status/debug | Derived launch attempt evidence | `generated_at`; no shared TTL | Stores launched child PID and cwd/repo root | No | Ad hoc JSON |
| `outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid.launchctl_label` | `scripts/run_headless_supervised_paper_service.sh` | operators/status/debug | Derived launcher label | No timestamp | launchctl label only, not runtime PID authority | No | Plain text |
| `outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.wrapper.pid` | legacy/nohup wrapper path | stop script, launcher cleanup | Deprecated/legacy fallback marker | No timestamp | wrapper PID, not runtime child PID | No | Plain PID |
| `outputs/probationary_pattern_engine/paper_session/runtime/paper_runtime_config_paths.txt` | runtime launcher and soak script | launcher/status/canonical checks | Authoritative requested config stack for current launch attempt | No timestamp; validity tied to runtime PID/config check | Must match active runtime command | Helps detect wrong config writer | Plain text |
| `outputs/probationary_pattern_engine/paper_session/operator_status.json` | active PAPER runtime | preflight, dashboard, canonical readiness, self-healing | Authoritative runtime/lane status when PID/root also validate | `generated_at` / `updated_at`, commonly 180s | May include source runtime PID; must not override PID/root guard | Source PID can expose stale writer, but duplicate detection is external | Ad hoc JSON |
| `outputs/probationary_pattern_engine/paper_session/runtime/track_b_runtime_heartbeat.jsonl` | decision journal | operators/research diagnostics | Diagnostic runtime cycle heartbeat | Append-only records; no single latest contract | Does not own process | No | `track_b_runtime_heartbeat_v1` |
| `outputs/operator_dashboard/runtime/latest_canonical_readiness.json` | `mgc_v05l.app.track_b_canonical_readiness` via launcher/status/refresher | governance, status, self-healing, dashboard | Canonical submit/readiness authority | `generated_at`; internal TTLs: broker 150s, reconciliation 180s, market data 180s | Includes root guard summary; does not itself own PID | Uses root guard and duplicate runtime counts from inputs | Canonical readiness JSON, state set documented in code |
| `outputs/operator_dashboard/runtime/latest_canonical_readiness_summary.json` | launcher/status wrapper around canonical readiness CLI | operators/status/launcher | Derived display/launch summary | `generated_at` implicit from source; no independent authority | No PID authority | No | Compact ad hoc JSON |
| `outputs/operator_dashboard/runtime/latest_track_b_self_healing_health.json` | `mgc_v05l.app.track_b_self_healing_supervisor` / status script | dashboard, self-healing CLI, operators | Restart planning authority, not submit authority | Per-agent artifact TTLs: broker 150s, readiness 180s, runtime 180s, Phase-1 180s | Probes PID files and process cwd/command | Reads broker safety duplicate count and agent process state | `track_b_self_healing_health_v1` |
| `outputs/operator_dashboard/runtime/self_healing_recovery_audit.jsonl` | self-healing apply mode | operators/dashboard | Restart attempt audit | Append-only; cooldown window uses recent rows | No PID authority | No | `track_b_self_healing_restart_attempt_v1` |
| `var/track_b_broker_truth_refresh_service.pid` | `scripts/start-track-b-broker-truth-refresh` | status script, self-healing | Broker truth refresher process PID candidate | No timestamp; process probe required | PID must run from Dev root; command should match read-only refresher | Duplicate prevention in start script, not unified | Plain PID |
| `var/track_b_broker_truth_refresh_heartbeat.json` | `mgc_v05l.app.ibkr_broker_truth_refresher --service` | self-healing/status | Authoritative refresher heartbeat | `generated_at`; self-healing TTL 150s | No process ownership by itself | No | `track_b_broker_truth_refresh_heartbeat_v1` |
| `outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_refresh_status.json` | broker truth refresher | broker truth lease, canonical readiness, self-healing | Authoritative last-good broker truth status, while fresh | `generated_at`, `last_success_at`, `completed_at`; default 150s | No PID authority | No | Ad hoc JSON with last-success preservation |
| `outputs/operator_dashboard/runtime/latest_broker_truth_lease.json` | `mgc_v05l.app.track_b_broker_truth_lease` | canonical readiness, launcher/status, self-healing | Derived broker safety lease authority | `generated_at`, broker truth/reconciliation ages; entry TTL 300s and exit TTL 900s per lease design | No PID authority | No | `track_b_broker_truth_lease_v1` |
| `var/track_b_operator_readiness_refresh_service.pid` / `var/track_b_operator_readiness_refresh_child.pid` | `scripts/start-track-b-operator-readiness-refresh` / refresher supervisor | status script, self-healing | Readiness refresher PID candidates | No timestamp; process probe required | service and child PID are separate; both need root/command checks | Duplicate blocked by start script unless override | Plain PID |
| `var/track_b_operator_readiness_refresh_heartbeat.json` | operator readiness refresher | status script, self-healing | Authoritative refresher heartbeat | `generated_at`; status script reports heartbeat age; self-healing TTL 180s | No process ownership by itself | No | `track_b_operator_readiness_refresher_heartbeat_v1` |
| `outputs/reports/track_b_operator_readiness_refresher/latest_track_b_operator_readiness_refresher_status.json` | operator readiness refresher | status script, self-healing, dashboard | Derived readiness refresh status | `generated_at`, `last_refresh_finished_at`; TTL 180s | No PID authority | No | Ad hoc JSON |
| `outputs/track_b_execution_core/phase1_runtime_market_data/{SYMBOL}/{TF}/latest_runtime_candles.json` | Phase-1 Databento runtime candle service / seed | live feed, runtime lanes, readiness, diagnostics | Authoritative market data for own instrument/timeframe when provenance and freshness pass | latest bar age threshold commonly 180s for routing | No PID authority | Per-symbol stale state, no writer authority contract | Ad hoc JSON |
| `outputs/reports/phase1_databento_live_runtime_candles/latest_phase1_databento_live_listener_status.json` | Phase-1 candle supervisor/listener | self-healing, status, diagnostics | Authoritative Phase-1 listener state | `latest_record_at` / `generated_at`; self-healing TTL 180s | PID separate in `var/phase1_databento_live_candles_*.pid` | Start/status scripts detect service/child only | Ad hoc JSON |
| `outputs/reports/phase1_databento_live_runtime_candles/latest_phase1_databento_live_supervisor_status.json` | Phase-1 supervisor | self-healing/status | Authoritative Phase-1 supervisor state | `generated_at`; TTL 180s | PID separate | Start/status scripts detect service/child only | Ad hoc JSON |
| `outputs/operator_dashboard/paper_runtime_recovery.json` | `operator_dashboard.py` | dashboard endpoint `/api/operator-artifact/paper-runtime-recovery` | Active display state only | `generated_at`/attempt timestamps; no submit/restart authority | No PID authority | No | Ad hoc JSON |
| `outputs/operator_dashboard/runtime/headless_supervised_paper_status.json` | `scripts/show_headless_supervised_paper_status.sh` | operators/launcher | Derived launch/status contract | Generated on demand; no standing TTL | Reads dashboard endpoints plus canonical readiness; does not own PID | No | Ad hoc JSON |
| `outputs/operator_dashboard/runtime/headless_supervised_paper_service_startup.json` | `scripts/run_headless_supervised_paper_service.sh` | operators/launcher | Derived startup result | `generated_at`; no runtime heartbeat authority | Reports launch result after PID/root/config checks | No | Ad hoc JSON |
| `outputs/operator_dashboard/runtime/operator_dashboard.pid` / `operator_dashboard_readiness.json` / `operator_dashboard.json` | `scripts/run_operator_dashboard.sh` and dashboard service | headless status, self-healing, operators | Dashboard/backend liveness only; not submit authority | readiness heartbeat fields from script; optional 180s self-healing TTL | PID must be Dev-root dashboard process | No unified duplicate detection | Ad hoc JSON |

## Convergence Problems

1. PID files have no generation, timestamp, root, command, or schema. Consumers must rediscover those facts differently.
2. Runtime liveness is split between process PID, `operator_status.json`, headless status, dashboard readiness, and canonical readiness.
3. Freshness TTLs are repeated in multiple modules: broker truth 150s, reconciliation 180s, market data 180s, self-healing per-agent TTLs, status scripts, and lane-local data gates.
4. `generated_at`, `updated_at`, `last_success_at`, `completed_at`, `latest_record_at`, and latest bar timestamps all act as clocks, but each consumer chooses its own precedence.
5. Derived display artifacts can outlive their source and be misread as current truth unless explicitly labeled display-only.
6. Launch success can be confused with durable runtime health unless a live PID, command, cwd, config stack, and first heartbeat all converge.
7. Duplicate writer detection exists as process scans and reconciliation counts, but no shared writer-authority field exists across artifacts.
8. Restart eligibility is calculated by self-healing, while readiness and dashboard use separate blocker vocabularies.
9. Phase-1 stale data is instrument-scoped in runtime behavior, but readiness/status summaries can still collapse it into global freshness language.
10. Launchctl label, wrapper PID, runtime PID, and operator status source PID are separate identities with no shared runtime generation ID.

## Unified Contract

Slice 1 adds shared pure primitives in:

`src/mgc_v05l/execution_core/track_b_runtime_truth_contract.py`

Schema version:

`track_b_runtime_truth_contract_v1`

Required fields:

- `runtime_instance_id`
- `service_name`
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
- `runtime_mode`
- `restart_generation`
- `duplicate_writer_detection`
- `stale_reason`
- `recovery_state`

Allowed common states:

- Freshness: `FRESH`, `STALE`, `MISSING_TIMESTAMP`, `FUTURE_TIMESTAMP`, `MISSING_ARTIFACT`
- Heartbeat: `HEALTHY`, `PROCESS_DOWN`, `ARTIFACT_MISSING`, `ARTIFACT_STALE`, `WRONG_ROOT`, `COMMAND_MISMATCH`, `UNKNOWN`
- Writer authority: `SINGLE_WRITER`, `DUPLICATE_WRITER_DETECTED`, `NO_ACTIVE_WRITER`
- Recovery: `OBSERVE_ONLY`, `RESTART_ELIGIBLE`, `RECOVERY_BLOCKED`, `OPERATOR_REQUIRED`

The contract is PAPER-only in this repo slice. Validation rejects `runtime_mode != PAPER`.

## Convergence Hierarchy

1. Service producer writes a normalized heartbeat/truth payload with PID, root, generated time, source commit, config fingerprint, runtime instance ID, and restart generation.
2. Canonical readiness reads normalized service truth plus broker lease, reconciliation, lane quarantine, and own-instrument data freshness. It remains submit authority.
3. Self-healing reads canonical readiness and normalized service truth. It remains restart authority only after broker/reconciliation/lifecycle gates are clean.
4. Dashboard reads canonical readiness and self-healing. It never overrides canonical truth with stale presentation snapshots.
5. Derived status artifacts include `source_artifact`, `source_generated_at`, and `derived_from_schema_version` if they summarize an authoritative artifact.

## Future Slices

Slice 2: Heartbeat convergence.

- Add normalized runtime truth payload for PAPER runtime, broker truth refresher, operator readiness refresher, Phase-1 candle supervisor, and dashboard backend.
- Keep existing artifacts in place, but include the normalized block in each producer output.
- Add latest heartbeat paths and schema checks without changing restart behavior.

Slice 3: Stale PID and generation elimination.

- Add `runtime_instance_id` and `restart_generation` to launcher wrapper, PID-adjacent metadata, `operator_status.json`, and canonical readiness.
- Treat PID-only evidence as insufficient for READY states.

Slice 4: Duplicate writer prevention.

- Add a single writer lock/lease with root, PID, generation, source commit, and config fingerprint.
- Fail launch before writing shared runtime artifacts when another valid writer owns the generation.

Slice 5: Canonical readiness stabilization.

- Refactor canonical readiness to consume normalized service truth blocks and emit one stale vocabulary.
- Preserve existing readiness states but standardize blocker source codes.

Slice 6: Restart budget and cooldown convergence.

- Move cooldown/restart budget into normalized recovery fields and audit rows.
- Ensure self-healing applies one restart budget per service/generation.

Slice 7: Launchctl KeepAlive integration.

- Add a launchd plist or deterministic service wrapper with `KeepAlive` after normalized truth and duplicate writer prevention are stable.
- Keep broker/lifecycle safety gates outside launchd.

Slice 8: Runtime data freshness by instrument.

- Normalize Phase-1 candle freshness into per-instrument truth records.
- Prevent global stale wording from hiding instrument-scoped readiness.

## Recommended Slice 2

Start with heartbeat convergence for the PAPER runtime only:

1. Add a normalized `paper_runtime_truth.json` beside `probationary_paper.pid`.
2. Populate it from the supervised launcher/runtime with `runtime_instance_id`, PID, Dev root, source commit, config fingerprint, generation, and heartbeat/freshness states.
3. Make headless status and self-healing read it as additional evidence only.
4. Do not change submit readiness or restart decisions until the new truth artifact is proven stable.
