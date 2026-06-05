"""Pure Track B startup/reload phase classifier.

This module reads existing Track B artifacts and returns an evidence-only
startup phase projection. It does not start, stop, restart, submit, cancel,
close, flatten, or mutate broker/runtime state.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "track_b_startup_phase_status_v1"

PRECHECK_ACCEPTED = "PRECHECK_ACCEPTED"
PROCESS_SPAWNED = "PROCESS_SPAWNED"
PROCESS_IDENTIFIED = "PROCESS_IDENTIFIED"
PROFILE_LOADED = "PROFILE_LOADED"
LANES_LOADED = "LANES_LOADED"
MARKET_DATA_FEED_OBSERVED = "MARKET_DATA_FEED_OBSERVED"
RUNTIME_INGESTION_ADVANCING = "RUNTIME_INGESTION_ADVANCING"
AUTHORITY_REFRESHED = "AUTHORITY_REFRESHED"
READINESS_EVALUATED = "READINESS_EVALUATED"
SUBMIT_CAPABLE = "SUBMIT_CAPABLE"

PROCESS_STARTED_PROFILE_PENDING = "PROCESS_STARTED_PROFILE_PENDING"
PROCESS_STARTED_LANES_PENDING = "PROCESS_STARTED_LANES_PENDING"
STARTED_DEGRADED_RUNTIME_INGESTION_STALE = "STARTED_DEGRADED_RUNTIME_INGESTION_STALE"
STARTED_NOT_SUBMIT_CAPABLE_AUTHORITY_PENDING = "STARTED_NOT_SUBMIT_CAPABLE_AUTHORITY_PENDING"
STARTED_DIAGNOSTIC_ONLY_MARKET_CLOSED = "STARTED_DIAGNOSTIC_ONLY_MARKET_CLOSED"
CONTROL_PLANE_SNAPSHOT_START_BLOCKED = "CONTROL_PLANE_SNAPSHOT_START_BLOCKED"

PHASE_SEQUENCE: tuple[str, ...] = (
    PRECHECK_ACCEPTED,
    PROCESS_SPAWNED,
    PROCESS_IDENTIFIED,
    PROFILE_LOADED,
    LANES_LOADED,
    MARKET_DATA_FEED_OBSERVED,
    RUNTIME_INGESTION_ADVANCING,
    AUTHORITY_REFRESHED,
    READINESS_EVALUATED,
    SUBMIT_CAPABLE,
)

SAFETY_INVARIANTS: tuple[str, ...] = (
    "startup_phase_artifact_grants_no_submit_authority",
    "startup_phase_artifact_allows_no_broker_mutation",
    "startup_phase_artifact_invokes_no_paper_proof",
    "startup_phase_artifact_creates_no_live_money_eligibility",
    "submit_capable_observation_requires_canonical_readiness",
)

DEFAULT_RUNTIME_TRUTH_PATH = (
    Path("outputs") / "probationary_pattern_engine" / "paper_session" / "runtime" / "paper_runtime_truth.json"
)
DEFAULT_PID_METADATA_PATH = (
    Path("outputs") / "probationary_pattern_engine" / "paper_session" / "runtime" / "probationary_paper.pid.json"
)
DEFAULT_CONFIG_IN_FORCE_PATH = (
    Path("outputs") / "probationary_pattern_engine" / "paper_session" / "runtime" / "paper_config_in_force.json"
)
DEFAULT_LAUNCH_STATUS_PATH = (
    Path("outputs") / "probationary_pattern_engine" / "paper_session" / "runtime" / "probationary_paper_launch_status.json"
)
DEFAULT_OPERATOR_STATUS_PATH = (
    Path("outputs") / "probationary_pattern_engine" / "paper_session" / "operator_status.json"
)
DEFAULT_PHASE1_LISTENER_STATUS_PATH = (
    Path("outputs")
    / "reports"
    / "phase1_databento_live_runtime_candles"
    / "latest_phase1_databento_live_listener_status.json"
)
DEFAULT_AUTHORITY_REFRESH_PATH = (
    Path("outputs") / "track_b_execution_core" / "authority_refresh" / "latest_authority_refresh_heartbeat.json"
)
DEFAULT_CANONICAL_READINESS_PATH = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
)
DEFAULT_BROKER_RECONCILIATION_PATH = (
    Path("outputs")
    / "reports"
    / "track_b_paper_broker_reconciliation"
    / "latest_track_b_paper_broker_reconciliation.json"
)


@dataclass(frozen=True)
class TrackBStartupPhaseClassifierConfig:
    repo_root: Path
    runtime_truth_path: Path = DEFAULT_RUNTIME_TRUTH_PATH
    pid_metadata_path: Path = DEFAULT_PID_METADATA_PATH
    config_in_force_path: Path = DEFAULT_CONFIG_IN_FORCE_PATH
    launch_status_path: Path = DEFAULT_LAUNCH_STATUS_PATH
    operator_status_path: Path = DEFAULT_OPERATOR_STATUS_PATH
    phase1_listener_status_path: Path = DEFAULT_PHASE1_LISTENER_STATUS_PATH
    authority_refresh_path: Path = DEFAULT_AUTHORITY_REFRESH_PATH
    canonical_readiness_path: Path = DEFAULT_CANONICAL_READINESS_PATH
    broker_reconciliation_path: Path = DEFAULT_BROKER_RECONCILIATION_PATH

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def classify_track_b_startup_phase(
    *,
    artifacts: Mapping[str, Mapping[str, Any]] | None = None,
    config: TrackBStartupPhaseClassifierConfig | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    generated_at = _ensure_utc(now or datetime.now(UTC))
    payloads = _artifact_payloads(config=config, artifacts=artifacts)

    runtime_truth = payloads["runtime_truth"]
    pid_metadata = payloads["pid_metadata"]
    config_in_force = payloads["config_in_force"]
    launch_status = payloads["launch_status"]
    operator_status = payloads["operator_status"]
    phase1 = payloads["phase1_listener_status"]
    authority_refresh = payloads["authority_refresh"]
    canonical = payloads["canonical_readiness"]
    reconciliation = payloads["broker_reconciliation"]

    precheck = _precheck_accepted(reconciliation=reconciliation, canonical=canonical)
    process_spawned = _process_spawned(runtime_truth=runtime_truth, pid_metadata=pid_metadata)
    process_identified = process_spawned and _process_identified(runtime_truth=runtime_truth, pid_metadata=pid_metadata)
    profile_loaded = process_identified and _profile_loaded(config_in_force=config_in_force, runtime_truth=runtime_truth)
    lanes_loaded = profile_loaded and _lanes_loaded(
        operator_status=operator_status,
        runtime_truth=runtime_truth,
        config_in_force=config_in_force,
    )
    market_data_observed = lanes_loaded and _market_data_feed_observed(phase1=phase1, canonical=canonical)
    runtime_ingestion_advancing = market_data_observed and _runtime_ingestion_advancing(
        operator_status=operator_status,
        canonical=canonical,
    )
    market_closed = _market_closed(canonical=canonical, phase1=phase1)
    authority_refreshed = runtime_ingestion_advancing and _authority_refreshed(authority_refresh=authority_refresh)
    readiness_evaluated = authority_refreshed and _readiness_evaluated(canonical=canonical)
    canonical_submit_capable = _canonical_submit_capable(canonical)
    submit_capable = readiness_evaluated and canonical_submit_capable

    completed = {
        PRECHECK_ACCEPTED: precheck,
        PROCESS_SPAWNED: process_spawned,
        PROCESS_IDENTIFIED: process_identified,
        PROFILE_LOADED: profile_loaded,
        LANES_LOADED: lanes_loaded,
        MARKET_DATA_FEED_OBSERVED: market_data_observed,
        RUNTIME_INGESTION_ADVANCING: runtime_ingestion_advancing,
        AUTHORITY_REFRESHED: authority_refreshed,
        READINESS_EVALUATED: readiness_evaluated,
        SUBMIT_CAPABLE: submit_capable,
    }
    phase = _deepest_phase(completed)
    classification = _classification(
        launch_status=launch_status,
        runtime_truth=runtime_truth,
        phase=phase,
        process_spawned=process_spawned,
        process_identified=process_identified,
        profile_loaded=profile_loaded,
        lanes_loaded=lanes_loaded,
        market_data_observed=market_data_observed,
        runtime_ingestion_advancing=runtime_ingestion_advancing,
        authority_refreshed=authority_refreshed,
        market_closed=market_closed,
        submit_capable=submit_capable,
    )
    blockers = _blockers(
        completed=completed,
        classification=classification,
        canonical=canonical,
        authority_refresh=authority_refresh,
        operator_status=operator_status,
        phase1=phase1,
        launch_status=launch_status,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "classification": classification,
        "phase": phase,
        "next_expected_phase": _next_expected_phase(completed),
        "completed_phases": [phase_name for phase_name in PHASE_SEQUENCE if completed[phase_name]],
        "phases": [
            {
                "phase": phase_name,
                "state": "PASSED" if completed[phase_name] else "WAITING",
            }
            for phase_name in PHASE_SEQUENCE
        ],
        "current_blockers": blockers,
        "submit_authority": False,
        "startup_grants_submit_authority": False,
        "submit_authority_source": "canonical_readiness_only",
        "canonical_readiness_submit_capable": canonical_submit_capable,
        "broker_mutation_allowed": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "broad_flatten_allowed": False,
        "global_flatten_allowed": False,
        "safety_invariants": list(SAFETY_INVARIANTS),
        "evidence": {
            "runtime_process_alive": process_spawned,
            "process_identified": process_identified,
            "profile_loaded": profile_loaded,
            "lanes_loaded": lanes_loaded,
            "market_data_feed_observed": market_data_observed,
            "runtime_ingestion_advancing": runtime_ingestion_advancing,
            "authority_refreshed": authority_refreshed,
            "readiness_evaluated": readiness_evaluated,
            "market_closed_or_scheduled_halt": market_closed,
            "canonical_readiness": _canonical_state(canonical),
            "authority_refresh_classification": _classification_text(authority_refresh),
            "launch_status_classification": _classification_text(launch_status),
        },
    }


def _artifact_payloads(
    *,
    config: TrackBStartupPhaseClassifierConfig | None,
    artifacts: Mapping[str, Mapping[str, Any]] | None,
) -> dict[str, dict[str, Any]]:
    if artifacts is not None:
        return {
            "runtime_truth": _mapping(artifacts.get("runtime_truth")),
            "pid_metadata": _mapping(artifacts.get("pid_metadata")),
            "config_in_force": _mapping(artifacts.get("config_in_force")),
            "launch_status": _mapping(artifacts.get("launch_status")),
            "operator_status": _mapping(artifacts.get("operator_status")),
            "phase1_listener_status": _mapping(artifacts.get("phase1_listener_status")),
            "authority_refresh": _mapping(artifacts.get("authority_refresh")),
            "canonical_readiness": _mapping(artifacts.get("canonical_readiness")),
            "broker_reconciliation": _mapping(artifacts.get("broker_reconciliation")),
        }
    if config is None:
        raise ValueError("Either artifacts or config is required.")
    return {
        "runtime_truth": _read_json(config.resolve(config.runtime_truth_path)),
        "pid_metadata": _read_json(config.resolve(config.pid_metadata_path)),
        "config_in_force": _read_json(config.resolve(config.config_in_force_path)),
        "launch_status": _read_json(config.resolve(config.launch_status_path)),
        "operator_status": _read_json(config.resolve(config.operator_status_path)),
        "phase1_listener_status": _read_json(config.resolve(config.phase1_listener_status_path)),
        "authority_refresh": _read_json(config.resolve(config.authority_refresh_path)),
        "canonical_readiness": _read_json(config.resolve(config.canonical_readiness_path)),
        "broker_reconciliation": _read_json(config.resolve(config.broker_reconciliation_path)),
    }


def _precheck_accepted(*, reconciliation: Mapping[str, Any], canonical: Mapping[str, Any]) -> bool:
    if reconciliation.get("restart_allowed") is True or reconciliation.get("precheck_accepted") is True:
        return True
    reconciliation_state = _classification_text(reconciliation)
    canonical_state = _canonical_state(canonical)
    if reconciliation_state in {"TRACK_B_PAPER_BROKER_RECONCILED", "BROKER_LIFECYCLE_RECONCILED"}:
        return True
    return canonical_state not in {"", "NOT_READY_WRONG_ROOT", "NOT_READY_CONFIG"}


def _process_spawned(*, runtime_truth: Mapping[str, Any], pid_metadata: Mapping[str, Any]) -> bool:
    return any(
        value is True
        for value in (
            _runtime_value(runtime_truth, "process_alive"),
            _runtime_value(runtime_truth, "runtime_alive"),
            _runtime_value(runtime_truth, "running"),
            _runtime_value(runtime_truth, "pid_alive"),
            pid_metadata.get("process_alive"),
            pid_metadata.get("running"),
        )
    ) or bool(
        _runtime_value(runtime_truth, "producer_pid")
        or _runtime_value(runtime_truth, "pid")
        or pid_metadata.get("producer_pid")
        or pid_metadata.get("pid")
    )


def _process_identified(*, runtime_truth: Mapping[str, Any], pid_metadata: Mapping[str, Any]) -> bool:
    if not _identity_fields_match(runtime_truth, pid_metadata, "runtime_instance_id"):
        return False
    if not _identity_fields_match(runtime_truth, pid_metadata, "source_commit"):
        return False
    if not _identity_fields_match(runtime_truth, pid_metadata, "restart_generation"):
        return False
    if not _pid_fields_match(runtime_truth, pid_metadata):
        return False
    runtime_instance_id = _first_text(
        _runtime_value(runtime_truth, "runtime_instance_id"),
        pid_metadata.get("runtime_instance_id"),
    )
    source_commit = _first_text(_runtime_value(runtime_truth, "source_commit"), pid_metadata.get("source_commit"))
    root = _first_text(
        _runtime_value(runtime_truth, "producer_root"),
        _runtime_value(runtime_truth, "root"),
        _runtime_value(runtime_truth, "process_root"),
        pid_metadata.get("producer_root"),
        pid_metadata.get("root"),
    )
    restart_generation = _first_text(
        _runtime_value(runtime_truth, "restart_generation"),
        pid_metadata.get("restart_generation"),
    )
    bad_heartbeat = str(_runtime_value(runtime_truth, "heartbeat_state") or "").upper() in {
        "PROCESS_DOWN",
        "WRONG_ROOT",
        "STALE",
    }
    return bool(runtime_instance_id and source_commit and root and restart_generation and not bad_heartbeat)


def _profile_loaded(*, config_in_force: Mapping[str, Any], runtime_truth: Mapping[str, Any]) -> bool:
    config_profile = _first_text(
        config_in_force.get("profile_id"),
        config_in_force.get("runtime_profile"),
    )
    runtime_profile = _first_text(
        _runtime_value(runtime_truth, "profile_id"),
        _runtime_value(runtime_truth, "runtime_profile"),
    )
    config_fingerprint = _first_text(config_in_force.get("config_fingerprint"))
    runtime_fingerprint = _first_text(_runtime_value(runtime_truth, "config_fingerprint"))
    has_config_profile_evidence = bool(config_profile or config_fingerprint)
    if not has_config_profile_evidence:
        return False
    if config_profile and runtime_profile and config_profile != runtime_profile:
        return False
    if config_fingerprint and runtime_fingerprint and config_fingerprint != runtime_fingerprint:
        return False
    profile_id = _first_text(
        config_profile,
        runtime_profile,
    )
    fingerprint = _first_text(config_fingerprint, runtime_fingerprint)
    exclusive = config_in_force.get("probationary_paper_runtime_exclusive_config")
    forbidden_overlay = config_in_force.get("review_overlay_active") is True or config_in_force.get("forbidden_overlay_active") is True
    return bool((profile_id or fingerprint) and exclusive is not False and not forbidden_overlay)


def _lanes_loaded(
    *,
    operator_status: Mapping[str, Any],
    runtime_truth: Mapping[str, Any],
    config_in_force: Mapping[str, Any],
) -> bool:
    lane_count = _lane_count(operator_status)
    runtime_lane_count = _int_or_none(_runtime_value(runtime_truth, "lane_count"))
    expected = _int_or_none(
        operator_status.get("expected_lane_count")
        or config_in_force.get("expected_lane_count")
        or config_in_force.get("lane_count")
        or _runtime_value(runtime_truth, "expected_lane_count")
    )
    if expected is not None and expected > 0:
        if lane_count >= expected:
            return True
        return runtime_lane_count is not None and runtime_lane_count >= expected
    return lane_count > 0


def _market_data_feed_observed(*, phase1: Mapping[str, Any], canonical: Mapping[str, Any]) -> bool:
    if _market_closed(canonical=canonical, phase1=phase1):
        return True
    if phase1.get("fresh") is True or phase1.get("market_data_fresh") is True:
        return True
    classification = _classification_text(phase1)
    if classification in {"READY_FOR_PROOF", "PHASE1_FEED_HEALTHY", "PHASE1_DATA_READY"}:
        return True
    return bool(phase1.get("latest_record_at") or phase1.get("latest_bar_at"))


def _runtime_ingestion_advancing(*, operator_status: Mapping[str, Any], canonical: Mapping[str, Any]) -> bool:
    if _market_closed(canonical=canonical, phase1={}):
        return True
    if operator_status.get("runtime_ingestion_fresh") is True or operator_status.get("ingestion_advancing") is True:
        return True
    if operator_status.get("runtime_ingestion_stale") is True:
        return False
    lanes = _lanes(operator_status)
    if not lanes:
        return False
    for lane in lanes:
        if lane.get("runtime_ingestion_fresh") is True or lane.get("ingestion_advancing") is True:
            return True
        processed = _first_text(lane.get("last_processed_bar_end_ts"), lane.get("latest_bar_at"))
        evaluated = _first_text(lane.get("last_execution_bar_evaluated_at"), lane.get("last_completed_context_bars_at"))
        if processed and evaluated and processed == evaluated:
            return True
    return False


def _authority_refreshed(*, authority_refresh: Mapping[str, Any]) -> bool:
    classification = _classification_text(authority_refresh)
    return classification in {"AUTHORITY_REFRESHED", "AUTHORITY_REFRESH_SKIPPED_NOT_DUE"} and (
        authority_refresh.get("fresh") is not False
    )


def _readiness_evaluated(*, canonical: Mapping[str, Any]) -> bool:
    return bool(_canonical_state(canonical))


def _canonical_submit_capable(canonical: Mapping[str, Any]) -> bool:
    return _canonical_state(canonical) == "READY_SUBMIT_CAPABLE" and canonical.get("submit_allowed") is not False


def _market_closed(*, canonical: Mapping[str, Any], phase1: Mapping[str, Any]) -> bool:
    market_state = str(canonical.get("market_schedule_state") or "").upper()
    canonical_state = _canonical_state(canonical)
    phase1_classification = _classification_text(phase1)
    phase1_reason = str(phase1.get("phase1_session_reason") or phase1.get("reason") or "").upper()
    return (
        canonical.get("readiness_block_is_scheduled_halt") is True
        or canonical_state in {"WAITING_FOR_MARKET_REOPEN", "READY_TO_START_DIAGNOSTIC_ONLY"}
        or market_state in {"SCHEDULED_MARKET_HALT", "WEEKEND_GLOBEX_HALT_BEFORE_SUNDAY_REOPEN"}
        or phase1_classification == "MARKET_CLOSED_NO_FRESH_BARS"
        or phase1_reason == "MARKET_CLOSED_NO_FRESH_BARS"
    )


def _deepest_phase(completed: Mapping[str, bool]) -> str:
    deepest = PRECHECK_ACCEPTED
    for phase in PHASE_SEQUENCE:
        if completed.get(phase):
            deepest = phase
        else:
            break
    return deepest


def _next_expected_phase(completed: Mapping[str, bool]) -> str | None:
    for phase in PHASE_SEQUENCE:
        if not completed.get(phase):
            return phase
    return None


def _classification(
    *,
    launch_status: Mapping[str, Any],
    runtime_truth: Mapping[str, Any],
    phase: str,
    process_spawned: bool,
    process_identified: bool,
    profile_loaded: bool,
    lanes_loaded: bool,
    market_data_observed: bool,
    runtime_ingestion_advancing: bool,
    authority_refreshed: bool,
    market_closed: bool,
    submit_capable: bool,
) -> str:
    if _launch_control_plane_snapshot_start_blocked(launch_status, runtime_truth=runtime_truth):
        return CONTROL_PLANE_SNAPSHOT_START_BLOCKED
    if submit_capable:
        return SUBMIT_CAPABLE
    if process_spawned and not process_identified:
        return PROCESS_STARTED_PROFILE_PENDING
    if process_identified and not profile_loaded:
        return PROCESS_STARTED_PROFILE_PENDING
    if profile_loaded and not lanes_loaded:
        return PROCESS_STARTED_LANES_PENDING
    if lanes_loaded and market_closed and not submit_capable:
        return STARTED_DIAGNOSTIC_ONLY_MARKET_CLOSED
    if market_data_observed and not runtime_ingestion_advancing:
        return STARTED_DEGRADED_RUNTIME_INGESTION_STALE
    if runtime_ingestion_advancing and not authority_refreshed:
        return STARTED_NOT_SUBMIT_CAPABLE_AUTHORITY_PENDING
    return phase


def _blockers(
    *,
    completed: Mapping[str, bool],
    classification: str,
    canonical: Mapping[str, Any],
    authority_refresh: Mapping[str, Any],
    operator_status: Mapping[str, Any],
    phase1: Mapping[str, Any],
    launch_status: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if classification == CONTROL_PLANE_SNAPSHOT_START_BLOCKED:
        return [_launch_control_plane_blocker(launch_status)]
    next_phase = _next_expected_phase(completed)
    if next_phase is None:
        return []
    detail_by_phase = {
        PROCESS_SPAWNED: "Runtime process/PID evidence is not available.",
        PROCESS_IDENTIFIED: "Runtime process identity is incomplete or stale.",
        PROFILE_LOADED: "Runtime profile/config fingerprint is not loaded.",
        LANES_LOADED: "Expected lanes are not visible in operator status.",
        MARKET_DATA_FEED_OBSERVED: "Phase-1 market-data feed has not been observed.",
        RUNTIME_INGESTION_ADVANCING: "Phase-1 feed is visible but runtime lane ingestion is stale.",
        AUTHORITY_REFRESHED: "Authority refresh heartbeat is missing or stale.",
        READINESS_EVALUATED: "Canonical readiness has not evaluated startup state.",
        SUBMIT_CAPABLE: "Canonical readiness has not classified READY_SUBMIT_CAPABLE.",
    }
    blocker = {
        "code": classification,
        "phase": next_phase,
        "detail": detail_by_phase.get(next_phase, "Startup phase is waiting for required evidence."),
    }
    if next_phase == AUTHORITY_REFRESHED:
        blocker["authority_refresh_classification"] = _classification_text(authority_refresh)
    if next_phase == RUNTIME_INGESTION_ADVANCING:
        blocker["operator_status_lane_count"] = _lane_count(operator_status)
    if next_phase == MARKET_DATA_FEED_OBSERVED:
        blocker["phase1_classification"] = _classification_text(phase1)
    if next_phase == SUBMIT_CAPABLE:
        blocker["canonical_readiness"] = _canonical_state(canonical)
    return [blocker]


def _launch_control_plane_snapshot_start_blocked(
    launch_status: Mapping[str, Any],
    *,
    runtime_truth: Mapping[str, Any],
) -> bool:
    if _classification_text(launch_status) != CONTROL_PLANE_SNAPSHOT_START_BLOCKED:
        return False
    launch_generated_at = _parse_datetime(launch_status.get("generated_at"))
    runtime_generated_at = _parse_datetime(runtime_truth.get("generated_at"))
    if launch_generated_at and runtime_generated_at and runtime_generated_at >= launch_generated_at:
        return False
    if launch_status.get("final_pid_alive") is True:
        return False
    if launch_status.get("first_truth_generated_at") or launch_status.get("second_truth_generated_at"):
        return False
    return str(launch_status.get("child_exit_code") or "") == "2"


def _launch_control_plane_blocker(launch_status: Mapping[str, Any]) -> dict[str, Any]:
    snapshot = _mapping(launch_status.get("control_plane_snapshot"))
    detail = _first_text(
        launch_status.get("detail"),
        snapshot.get("top_line_status"),
        "Control Plane Snapshot start preflight blocked before runtime profile/config load.",
    )
    blocker = {
        "code": CONTROL_PLANE_SNAPSHOT_START_BLOCKED,
        "phase": PROFILE_LOADED,
        "detail": detail,
        "source": "control_plane_snapshot_start_preflight",
        "launch_child_exit_code": launch_status.get("child_exit_code"),
        "control_plane_snapshot_classification": snapshot.get("classification"),
        "runtime_supervisor_classification": snapshot.get("runtime_supervisor_classification"),
        "proof_window_status": snapshot.get("proof_window_status"),
    }
    blocking_agent = _extract_field_from_detail(detail, "primary_blocking_agent_id")
    blocking_reason = _extract_field_from_detail(detail, "primary_blocking_reason")
    if blocking_agent:
        blocker["primary_blocking_agent_id"] = blocking_agent
    if blocking_reason:
        blocker["primary_blocking_reason"] = blocking_reason
    return blocker


def _lane_count(operator_status: Mapping[str, Any]) -> int:
    lanes = _lanes(operator_status)
    if lanes:
        return len(lanes)
    for key in ("lane_count", "active_lane_count", "eligible_lane_count"):
        value = _int_or_none(operator_status.get(key))
        if value is not None:
            return value
    return 0


def _lanes(operator_status: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = operator_status.get("lanes")
    if isinstance(raw, Mapping):
        return [dict(item) for item in raw.values() if isinstance(item, Mapping)]
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        return [dict(item) for item in raw if isinstance(item, Mapping)]
    return []


def _canonical_state(canonical: Mapping[str, Any]) -> str:
    return str(canonical.get("canonical_readiness") or canonical.get("classification") or canonical.get("state") or "").strip()


def _classification_text(payload: Mapping[str, Any]) -> str:
    return str(payload.get("classification") or payload.get("state") or "").strip()


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _runtime_mapping(runtime_truth: Mapping[str, Any]) -> Mapping[str, Any]:
    nested = runtime_truth.get("runtime")
    return nested if isinstance(nested, Mapping) else {}


def _runtime_value(runtime_truth: Mapping[str, Any], key: str) -> Any:
    if key in runtime_truth:
        return runtime_truth.get(key)
    return _runtime_mapping(runtime_truth).get(key)


def _identity_fields_match(
    runtime_truth: Mapping[str, Any],
    pid_metadata: Mapping[str, Any],
    key: str,
) -> bool:
    runtime_value = _first_text(_runtime_value(runtime_truth, key))
    pid_value = _first_text(pid_metadata.get(key))
    return not (runtime_value and pid_value and runtime_value != pid_value)


def _pid_fields_match(runtime_truth: Mapping[str, Any], pid_metadata: Mapping[str, Any]) -> bool:
    runtime_pid = _int_or_none(_runtime_value(runtime_truth, "producer_pid") or _runtime_value(runtime_truth, "pid"))
    pid_metadata_pid = _int_or_none(pid_metadata.get("producer_pid") or pid_metadata.get("pid"))
    return not (runtime_pid is not None and pid_metadata_pid is not None and runtime_pid != pid_metadata_pid)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _extract_field_from_detail(detail: str, key: str) -> str:
    marker = f"{key}="
    if marker not in detail:
        return ""
    tail = detail.split(marker, 1)[1].strip()
    if tail.startswith('"'):
        return tail.split('"', 2)[1] if '"' in tail[1:] else tail.strip('"')
    return tail.split(maxsplit=1)[0].strip().strip(",;")


def _int_or_none(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _ensure_utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
