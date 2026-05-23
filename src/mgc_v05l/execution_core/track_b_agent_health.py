"""Read-only Track B PAPER agent health contract authority.

Agent Health authority lives in execution_core. Dashboard artifacts are
projections and must not be used as runtime, readiness, restart, broker, or
routing authority. This v1 contract classifies registered agents only; it never
starts, stops, restarts, submits, cancels, replaces, closes, or flattens
anything.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_agent_registry import (
    DEFAULT_AGENT_REGISTRY_ARTIFACT,
    TrackBAgentRegistryConfig,
    build_track_b_agent_registry,
    write_track_b_agent_registry,
)
from mgc_v05l.execution_core.track_b_runtime_environment_truth import (
    RUNTIME_ACTIVE_OBSERVATION_ONLY,
    RUNTIME_ACTIVE_TRADE_CAPABLE,
    RUNTIME_DOWN_CLEAN,
    RUNTIME_DOWN_WITH_BROKER_EXPOSURE,
)
from mgc_v05l.market_data.phase1_market_session import MARKET_CLOSED_NO_FRESH_BARS


AGENT_HEALTH_READY = "AGENT_HEALTH_READY"
AGENT_HEALTH_DEGRADED = "AGENT_HEALTH_DEGRADED"
AGENT_HEALTH_BLOCKING = "AGENT_HEALTH_BLOCKING"
AGENT_HEALTH_UNKNOWN = "AGENT_HEALTH_UNKNOWN"

HEALTHY = "HEALTHY"
DEGRADED = "DEGRADED"
STALE = "STALE"
MISSING = "MISSING"
STOPPED_EXPECTED = "STOPPED_EXPECTED"
STOPPED_UNEXPECTED = "STOPPED_UNEXPECTED"
NOT_APPLICABLE = "NOT_APPLICABLE"

REQUIRED_RUNNING = "required_running"
REQUIRED_FRESH_ARTIFACT = "required_fresh_artifact"
ON_DEMAND = "on_demand"
DIAGNOSTIC_OPTIONAL = "diagnostic_optional"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_AGENT_HEALTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "agent_health" / "latest_agent_health.json"
)
DEFAULT_DASHBOARD_AGENT_HEALTH_PROJECTION = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_track_b_agent_health.json"
)
DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "runtime_truth" / "latest_runtime_environment_truth.json"
)
DEFAULT_PHASE1_READINESS_ARTIFACT = (
    Path("outputs") / "reports" / "phase1_runtime_data_readiness" / "latest_phase1_runtime_data_readiness.json"
)
DEFAULT_ARTIFACT_MAX_AGE_SECONDS = 300.0


@dataclass(frozen=True)
class TrackBAgentHealthConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_AGENT_HEALTH_ARTIFACT
    dashboard_projection_path: Path | None = DEFAULT_DASHBOARD_AGENT_HEALTH_PROJECTION
    agent_registry_path: Path = DEFAULT_AGENT_REGISTRY_ARTIFACT
    runtime_environment_truth_path: Path = DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT
    phase1_readiness_path: Path = DEFAULT_PHASE1_READINESS_ARTIFACT
    artifact_max_age_seconds: float = DEFAULT_ARTIFACT_MAX_AGE_SECONDS

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_agent_health(
    *,
    config: TrackBAgentHealthConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    registry_path = config.resolve(config.agent_registry_path)
    registry = _read_json(registry_path)
    if not registry:
        registry_config = TrackBAgentRegistryConfig(repo_root=config.repo_root, dashboard_projection_path=None)
        registry = build_track_b_agent_registry(config=registry_config, now=actual_now)
        write_track_b_agent_registry(config=registry_config, payload=registry)

    runtime_environment_truth = _read_json(config.resolve(config.runtime_environment_truth_path))
    phase1_readiness = _read_json(config.resolve(config.phase1_readiness_path))
    agents = _list(registry.get("agents"))
    health_rows = [
        _agent_health_row(
            agent=agent,
            config=config,
            now=actual_now,
            runtime_environment_truth=runtime_environment_truth,
            phase1_readiness=phase1_readiness,
        )
        for agent in agents
    ]
    classification = _overall_classification(health_rows)
    return {
        "schema_version": "track_b_agent_health_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "submit_authority": False,
        "broker_mutation": False,
        "lifecycle_mutation": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "authority_owner": "execution_core",
        "projection_only": False,
        "classification": classification,
        "agents": health_rows,
        "agent_registry": {
            "classification": registry.get("classification"),
            "generated_at": registry.get("generated_at"),
            "artifact_path": str(registry_path),
        },
        "summary": {
            "classification": classification,
            "agent_count": len(health_rows),
            "healthy_count": sum(1 for row in health_rows if row.get("status") == HEALTHY),
            "degraded_count": sum(1 for row in health_rows if row.get("status") == DEGRADED),
            "stale_count": sum(1 for row in health_rows if row.get("status") == STALE),
            "missing_count": sum(1 for row in health_rows if row.get("status") == MISSING),
            "stopped_expected_count": sum(1 for row in health_rows if row.get("status") == STOPPED_EXPECTED),
            "stopped_unexpected_count": sum(1 for row in health_rows if row.get("status") == STOPPED_UNEXPECTED),
            "blocking_for_proof_count": sum(1 for row in health_rows if row.get("blocking_for_proof") is True),
            "blocking_for_runtime_submit_count": sum(
                1 for row in health_rows if row.get("blocking_for_runtime_submit") is True
            ),
        },
        "artifact_paths": {
            "authority": str(config.resolve(config.output_path)),
            "dashboard_projection": None
            if config.dashboard_projection_path is None
            else str(config.resolve(config.dashboard_projection_path)),
            "agent_registry": str(registry_path),
            "runtime_environment_truth": str(config.resolve(config.runtime_environment_truth_path)),
            "phase1_readiness": str(config.resolve(config.phase1_readiness_path)),
        },
    }


def write_track_b_agent_health(
    *,
    config: TrackBAgentHealthConfig,
    payload: Mapping[str, Any],
) -> Path:
    authority_path = config.resolve(config.output_path)
    _write_json_atomic(authority_path, dict(payload))
    if config.dashboard_projection_path is not None:
        _write_json_atomic(
            config.resolve(config.dashboard_projection_path),
            build_dashboard_agent_health_projection(authority_payload=payload, authority_path=authority_path),
        )
    return authority_path


def build_dashboard_agent_health_projection(*, authority_payload: Mapping[str, Any], authority_path: Path) -> dict[str, Any]:
    return {
        **dict(authority_payload),
        "schema_version": "track_b_agent_health_dashboard_projection_v1",
        "projection_only": True,
        "not_routing_authority": True,
        "source_authority_path": str(authority_path),
        "authority_owner": "execution_core",
        "operator_dashboard_display_only": True,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write the read-only Track B PAPER agent health authority artifact.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_AGENT_HEALTH_ARTIFACT)
    parser.add_argument("--dashboard-projection-path", type=Path, default=DEFAULT_DASHBOARD_AGENT_HEALTH_PROJECTION)
    parser.add_argument("--no-dashboard-projection", action="store_true")
    parser.add_argument("--artifact-max-age-seconds", type=float, default=DEFAULT_ARTIFACT_MAX_AGE_SECONDS)
    parser.add_argument("--json", action="store_true", help="Print the full health artifact as JSON.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBAgentHealthConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
        dashboard_projection_path=None if bool(args.no_dashboard_projection) else Path(args.dashboard_projection_path),
        artifact_max_age_seconds=float(args.artifact_max_age_seconds),
    )
    payload = build_track_b_agent_health(config=config)
    authority_path = write_track_b_agent_health(config=config, payload=payload)
    if bool(args.json):
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            json.dumps(
                {
                    "classification": payload.get("classification"),
                    "summary": payload.get("summary"),
                    "authority_path": str(authority_path),
                    "read_only": True,
                    "paper_proof_invoked": False,
                    "live_money_eligible": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
    return 0 if payload.get("classification") in {AGENT_HEALTH_READY, AGENT_HEALTH_DEGRADED} else 2


def _agent_health_row(
    *,
    agent: Mapping[str, Any],
    config: TrackBAgentHealthConfig,
    now: datetime,
    runtime_environment_truth: Mapping[str, Any],
    phase1_readiness: Mapping[str, Any],
) -> dict[str, Any]:
    agent_id = str(agent.get("agent_id") or "")
    expected_state = _expected_state(agent)
    if agent_id == "track_b_paper_runtime":
        status, reason = _runtime_status(runtime_environment_truth)
        primary_path = str(config.resolve(config.runtime_environment_truth_path))
        artifact_status = _artifact_status(config.resolve(config.runtime_environment_truth_path), now=now, max_age_seconds=config.artifact_max_age_seconds)
    elif agent_id == "phase1_databento_live_candles":
        status, reason = _phase1_status(phase1_readiness)
        primary_path = str(config.resolve(config.phase1_readiness_path))
        artifact_status = _artifact_status(config.resolve(config.phase1_readiness_path), now=now, max_age_seconds=config.artifact_max_age_seconds)
        if status == HEALTHY and artifact_status["status"] == STALE and _phase1_market_closed(phase1_readiness):
            artifact_status = {**artifact_status, "status": HEALTHY}
    elif expected_state == ON_DEMAND:
        status, reason = HEALTHY, "on_demand_agent_invoked_by_callers"
        primary_path = None
        artifact_status = {"status": NOT_APPLICABLE, "last_seen_at": None, "freshness_age_seconds": None}
    elif expected_state == DIAGNOSTIC_OPTIONAL:
        primary_path = _primary_path(agent)
        artifact_status = _optional_artifact_status(primary_path, now=now, max_age_seconds=config.artifact_max_age_seconds)
        status = artifact_status["status"]
        reason = "diagnostic_optional"
    else:
        primary_path = _primary_path(agent)
        artifact_status = _required_artifact_status(primary_path, now=now, max_age_seconds=config.artifact_max_age_seconds)
        status = artifact_status["status"]
        reason = artifact_status["reason"]

    required_for_proof = agent.get("required_for_proof") is True
    required_for_runtime_submit = agent.get("required_for_runtime_submit") is True
    blocking_for_proof = required_for_proof and status in {MISSING, STALE, STOPPED_UNEXPECTED}
    blocking_for_runtime_submit = required_for_runtime_submit and status in {MISSING, STALE, STOPPED_UNEXPECTED}
    if expected_state == DIAGNOSTIC_OPTIONAL:
        blocking_for_proof = False
        blocking_for_runtime_submit = False
    return {
        "agent_id": agent_id,
        "display_name": agent.get("display_name"),
        "category": agent.get("category"),
        "health_contract_id": agent.get("health_contract_id"),
        "expected_state": expected_state,
        "status": status,
        "heartbeat_path": agent.get("heartbeat_artifact_path"),
        "primary_artifact_path": primary_path,
        "max_age_seconds": None if expected_state in {ON_DEMAND, DIAGNOSTIC_OPTIONAL} else config.artifact_max_age_seconds,
        "last_seen_at": artifact_status.get("last_seen_at"),
        "freshness_age_seconds": artifact_status.get("freshness_age_seconds"),
        "required_for_proof": required_for_proof,
        "required_for_runtime_submit": required_for_runtime_submit,
        "blocking_for_proof": blocking_for_proof,
        "blocking_for_runtime_submit": blocking_for_runtime_submit,
        "evidence_paths": _evidence_paths(agent=agent, primary_path=primary_path),
        "reason": reason,
    }


def _expected_state(agent: Mapping[str, Any]) -> str:
    agent_id = str(agent.get("agent_id") or "")
    category = str(agent.get("category") or "")
    if agent_id == "track_b_paper_runtime":
        return REQUIRED_RUNNING
    if category == "diagnostic" or agent_id in {"position_truth_notifier", "self_healing_supervisor"}:
        return DIAGNOSTIC_OPTIONAL
    if agent_id in {"shared_truth_refresh"}:
        return ON_DEMAND
    return REQUIRED_FRESH_ARTIFACT


def _runtime_status(runtime_environment_truth: Mapping[str, Any]) -> tuple[str, str]:
    classification = str(runtime_environment_truth.get("classification") or "")
    if classification in {RUNTIME_ACTIVE_TRADE_CAPABLE, RUNTIME_ACTIVE_OBSERVATION_ONLY}:
        return HEALTHY, classification
    if classification == RUNTIME_DOWN_CLEAN:
        return STOPPED_EXPECTED, classification
    if classification == RUNTIME_DOWN_WITH_BROKER_EXPOSURE:
        return STOPPED_UNEXPECTED, classification
    if not runtime_environment_truth:
        return MISSING, "runtime_environment_truth_missing"
    return DEGRADED, classification or "runtime_environment_truth_not_clean"


def _phase1_status(phase1_readiness: Mapping[str, Any]) -> tuple[str, str]:
    if not phase1_readiness:
        return MISSING, "phase1_readiness_missing"
    if _phase1_market_closed(phase1_readiness):
        return HEALTHY, MARKET_CLOSED_NO_FRESH_BARS
    rows = _list(phase1_readiness.get("rows"))
    if rows and all(row.get("runtime_candles_ready") is True for row in rows):
        return HEALTHY, "phase1_runtime_candles_ready"
    return DEGRADED, _first_phase1_reason(phase1_readiness)


def _phase1_market_closed(phase1_readiness: Mapping[str, Any]) -> bool:
    market_session = _mapping(phase1_readiness.get("market_session"))
    if market_session.get("classification") == MARKET_CLOSED_NO_FRESH_BARS:
        return True
    for row in _list(phase1_readiness.get("rows")):
        for check_group_name in ("candle_checks", "feature_checks"):
            for check in _mapping(row.get(check_group_name)).values():
                if isinstance(check, Mapping) and check.get("reason") == MARKET_CLOSED_NO_FRESH_BARS:
                    return True
    return False


def _first_phase1_reason(phase1_readiness: Mapping[str, Any]) -> str:
    for row in _list(phase1_readiness.get("rows")):
        for key in ("runtime_candles_block_reason", "derived_features_block_reason"):
            reason = str(row.get(key) or "")
            if reason and reason != "READY":
                return reason
    return "phase1_runtime_data_not_ready"


def _primary_path(agent: Mapping[str, Any]) -> str | None:
    heartbeat = agent.get("heartbeat_artifact_path")
    if heartbeat:
        return str(heartbeat)
    paths = _list(agent.get("expected_artifact_paths"))
    return str(paths[0]) if paths else None


def _required_artifact_status(path_text: str | None, *, now: datetime, max_age_seconds: float) -> dict[str, Any]:
    if not path_text:
        return {"status": MISSING, "reason": "primary_artifact_path_missing", "last_seen_at": None, "freshness_age_seconds": None}
    return _artifact_status(Path(path_text), now=now, max_age_seconds=max_age_seconds)


def _optional_artifact_status(path_text: str | None, *, now: datetime, max_age_seconds: float) -> dict[str, Any]:
    if not path_text:
        return {"status": NOT_APPLICABLE, "reason": "diagnostic_optional_no_primary_artifact", "last_seen_at": None, "freshness_age_seconds": None}
    status = _artifact_status(Path(path_text), now=now, max_age_seconds=max_age_seconds)
    if status["status"] == MISSING:
        return {**status, "status": NOT_APPLICABLE, "reason": "diagnostic_optional_artifact_missing"}
    return status


def _artifact_status(path: Path, *, now: datetime, max_age_seconds: float) -> dict[str, Any]:
    payload = _read_json(path)
    if not payload:
        return {"status": MISSING, "reason": "artifact_missing_or_invalid", "last_seen_at": None, "freshness_age_seconds": None}
    last_seen = _parse_time(payload.get("generated_at") or payload.get("updated_at"))
    if last_seen is None:
        return {"status": DEGRADED, "reason": "artifact_timestamp_missing", "last_seen_at": None, "freshness_age_seconds": None}
    age = max(0.0, (now - last_seen).total_seconds())
    if age > max_age_seconds:
        return {"status": STALE, "reason": "artifact_stale", "last_seen_at": last_seen.isoformat(), "freshness_age_seconds": age}
    return {"status": HEALTHY, "reason": "artifact_fresh", "last_seen_at": last_seen.isoformat(), "freshness_age_seconds": age}


def _evidence_paths(*, agent: Mapping[str, Any], primary_path: str | None) -> list[str]:
    paths = []
    if primary_path:
        paths.append(primary_path)
    for path in _list(agent.get("expected_artifact_paths")):
        text = str(path)
        if text not in paths:
            paths.append(text)
    return paths


def _overall_classification(rows: Sequence[Mapping[str, Any]]) -> str:
    if not rows:
        return AGENT_HEALTH_UNKNOWN
    if any(row.get("blocking_for_proof") is True or row.get("blocking_for_runtime_submit") is True for row in rows):
        return AGENT_HEALTH_BLOCKING
    statuses = {str(row.get("status") or "") for row in rows}
    if statuses & {DEGRADED, STALE, MISSING, STOPPED_UNEXPECTED}:
        return AGENT_HEALTH_DEGRADED
    return AGENT_HEALTH_READY


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


if __name__ == "__main__":
    raise SystemExit(main())
