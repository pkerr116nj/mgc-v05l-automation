"""Read-only Track B PAPER crash-loop protection authority.

Crash Loop Protection authority lives in execution_core. Dashboard artifacts
are projections and must not be used as runtime, readiness, restart, broker, or
routing authority. This v1 policy recommends only; it never starts, stops,
restarts, submits, cancels, replaces, closes, or flattens anything.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_agent_health import DEFAULT_AGENT_HEALTH_ARTIFACT
from mgc_v05l.execution_core.track_b_agent_registry import DEFAULT_AGENT_REGISTRY_ARTIFACT
from mgc_v05l.execution_core.track_b_runtime_environment_truth import DEFAULT_RUNTIME_ENVIRONMENT_EVENTS
from mgc_v05l.execution_core.track_b_self_recover_rules import (
    DEFAULT_SELF_RECOVER_RULES_ARTIFACT,
    WAIT_MARKET_CLOSED,
)
from mgc_v05l.market_data.phase1_market_session import MARKET_CLOSED_NO_FRESH_BARS


NO_CRASH_LOOP = "NO_CRASH_LOOP"
RESTART_COOLDOWN_ACTIVE = "RESTART_COOLDOWN_ACTIVE"
REPEATED_RUNTIME_FAILURE = "REPEATED_RUNTIME_FAILURE"
REPEATED_MARKET_DATA_FAILURE = "REPEATED_MARKET_DATA_FAILURE"
REPEATED_BROKER_LEASE_FAILURE = "REPEATED_BROKER_LEASE_FAILURE"
OPERATOR_ACK_REQUIRED = "OPERATOR_ACK_REQUIRED"
INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CRASH_LOOP_PROTECTION_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "crash_loop_protection" / "latest_crash_loop_protection.json"
)
DEFAULT_CRASH_LOOP_EVENTS = (
    Path("outputs") / "track_b_execution_core" / "crash_loop_protection" / "crash_loop_events.jsonl"
)
DEFAULT_DASHBOARD_CRASH_LOOP_PROTECTION_PROJECTION = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_track_b_crash_loop_protection.json"
)
DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "runtime_truth" / "latest_runtime_environment_truth.json"
)
DEFAULT_RUNTIME_STOP_PROVENANCE_ARTIFACT = (
    Path("outputs") / "probationary_pattern_engine" / "paper_session" / "runtime" / "latest_runtime_stop_provenance.json"
)
DEFAULT_LAUNCH_STATUS_ARTIFACT = (
    Path("outputs")
    / "probationary_pattern_engine"
    / "paper_session"
    / "runtime"
    / "probationary_paper_launch_status.json"
)
DEFAULT_LAUNCH_STATUS_HISTORY = (
    Path("outputs")
    / "probationary_pattern_engine"
    / "paper_session"
    / "runtime"
    / "probationary_paper_launch_status_history.jsonl"
)
DEFAULT_BROKER_LEASE_ARTIFACT = Path("outputs") / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json"

RUNTIME_PRE_CONVERGENCE_FAILURES = {
    "RUNTIME_EXITED_AFTER_PREFLIGHT",
    "RUNTIME_EXITED_AFTER_INITIAL_TRUTH",
    "RUNTIME_TRUTH_NOT_CONVERGED",
    "LAUNCH_VERIFIER_STOPPED_CHILD_AFTER_TIMEOUT",
    "PROBATIONARY_PAPER_BACKGROUND_START_FAILED",
    "runtime_exited_after_preflight",
    "runtime_exited_after_initial_truth",
    "launch_verifier_timeout_before_sustained_runtime_truth",
}
BROKER_LEASE_FAILURES = {
    "ACTIVE_DEGRADED_REFRESH_FAILING",
    "BROKER_TRUTH_LEASE_EXPIRED",
    "OPERATOR_REQUIRED",
    "INVALIDATED_CONTRADICTION",
}


@dataclass(frozen=True)
class TrackBCrashLoopProtectionConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_CRASH_LOOP_PROTECTION_ARTIFACT
    event_log_path: Path = DEFAULT_CRASH_LOOP_EVENTS
    dashboard_projection_path: Path | None = DEFAULT_DASHBOARD_CRASH_LOOP_PROTECTION_PROJECTION
    agent_registry_path: Path = DEFAULT_AGENT_REGISTRY_ARTIFACT
    agent_health_path: Path = DEFAULT_AGENT_HEALTH_ARTIFACT
    runtime_environment_truth_path: Path = DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT
    self_recover_rules_path: Path = DEFAULT_SELF_RECOVER_RULES_ARTIFACT
    stop_provenance_path: Path = DEFAULT_RUNTIME_STOP_PROVENANCE_ARTIFACT
    launch_status_path: Path = DEFAULT_LAUNCH_STATUS_ARTIFACT
    launch_status_history_path: Path = DEFAULT_LAUNCH_STATUS_HISTORY
    runtime_truth_history_path: Path = DEFAULT_RUNTIME_ENVIRONMENT_EVENTS
    broker_lease_path: Path = DEFAULT_BROKER_LEASE_ARTIFACT
    restart_window_seconds: float = 900.0
    cooldown_seconds: float = 300.0
    max_restarts_per_window: int = 2
    max_same_stop_reason_per_window: int = 2
    max_same_stop_source_per_window: int = 3

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_crash_loop_protection(
    *,
    config: TrackBCrashLoopProtectionConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    inputs = {
        "agent_registry": _read_json(config.resolve(config.agent_registry_path)),
        "agent_health": _read_json(config.resolve(config.agent_health_path)),
        "runtime_environment_truth": _read_json(config.resolve(config.runtime_environment_truth_path)),
        "self_recover_rules": _read_json(config.resolve(config.self_recover_rules_path)),
        "broker_lease": _read_json(config.resolve(config.broker_lease_path)),
    }
    events = _collect_history_events(config=config, now=actual_now)
    window_events = _window_events(events=events, now=actual_now, window_seconds=config.restart_window_seconds)
    decision = _classify_crash_loop(config=config, inputs=inputs, events=window_events, now=actual_now)
    event_state = _event_state(decision=decision, events=window_events)
    return {
        "schema_version": "track_b_crash_loop_protection_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "recommendation_only": True,
        "submit_authority": False,
        "broker_mutation": False,
        "lifecycle_mutation": False,
        "runtime_restart_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "classification": decision["classification"],
        "crash_loop_classification": decision["classification"],
        "restart_blocked": decision["restart_blocked"],
        "restart_allowed_by_crash_loop_policy": not decision["restart_blocked"],
        "reason": decision["reason"],
        "restart_window_seconds": config.restart_window_seconds,
        "max_restarts_per_window": config.max_restarts_per_window,
        "repeated_same_stop_reason_count": decision["same_reason_count"],
        "repeated_same_stop_source_count": decision["same_source_count"],
        "cooldown_until": decision["cooldown_until"],
        "operator_ack_required": decision["operator_ack_required"],
        "history": {
            "event_count": len(events),
            "window_event_count": len(window_events),
            "events": window_events,
        },
        "input_evidence": _input_evidence(inputs),
        "event_state": event_state,
        "input_artifacts": {
            "agent_registry": str(config.resolve(config.agent_registry_path)),
            "agent_health": str(config.resolve(config.agent_health_path)),
            "runtime_environment_truth": str(config.resolve(config.runtime_environment_truth_path)),
            "self_recover_rules": str(config.resolve(config.self_recover_rules_path)),
            "stop_provenance": str(config.resolve(config.stop_provenance_path)),
            "launch_status": str(config.resolve(config.launch_status_path)),
            "launch_status_history": str(config.resolve(config.launch_status_history_path)),
            "runtime_truth_history": str(config.resolve(config.runtime_truth_history_path)),
            "broker_lease": str(config.resolve(config.broker_lease_path)),
        },
        "artifact_paths": {
            "authority": str(config.resolve(config.output_path)),
            "event_log": str(config.resolve(config.event_log_path)),
            "dashboard_projection": None
            if config.dashboard_projection_path is None
            else str(config.resolve(config.dashboard_projection_path)),
        },
        "todo_v2": [
            "Persist per-agent restart attempts with explicit runtime resume ids.",
            "Define resume semantics for expected-clean shutdown versus failed convergence.",
            "Attach operator acknowledgement ids before clearing OPERATOR_ACK_REQUIRED.",
        ],
    }


def write_track_b_crash_loop_protection(
    *,
    config: TrackBCrashLoopProtectionConfig,
    payload: Mapping[str, Any],
    now: datetime | None = None,
) -> tuple[Path, list[dict[str, Any]]]:
    authority_path = config.resolve(config.output_path)
    event_log_path = config.resolve(config.event_log_path)
    previous = _read_json(authority_path)
    events = build_crash_loop_events(previous=previous, current=payload, now=now)
    _write_json_atomic(authority_path, dict(payload))
    if config.dashboard_projection_path is not None:
        _write_json_atomic(
            config.resolve(config.dashboard_projection_path),
            build_dashboard_crash_loop_projection(authority_payload=payload, authority_path=authority_path),
        )
    if events:
        event_log_path.parent.mkdir(parents=True, exist_ok=True)
        with event_log_path.open("a", encoding="utf-8") as handle:
            for event in events:
                handle.write(json.dumps(event, sort_keys=True) + "\n")
    return authority_path, events


def build_dashboard_crash_loop_projection(*, authority_payload: Mapping[str, Any], authority_path: Path) -> dict[str, Any]:
    return {
        **dict(authority_payload),
        "schema_version": "track_b_crash_loop_protection_dashboard_projection_v1",
        "projection_only": True,
        "not_routing_authority": True,
        "source_authority_path": str(authority_path),
        "authority_owner": "execution_core",
        "operator_dashboard_display_only": True,
    }


def build_crash_loop_events(
    *,
    previous: Mapping[str, Any] | None,
    current: Mapping[str, Any],
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    previous_state = _mapping((previous or {}).get("event_state"))
    current_state = _mapping(current.get("event_state"))
    if previous_state == current_state:
        return []
    actual_now = _ensure_utc(now or datetime.now(UTC))
    return [
        {
            "schema_version": "track_b_crash_loop_event_v1",
            "event_type": "CRASH_LOOP_CLASSIFICATION_CHANGED",
            "generated_at": actual_now.isoformat(),
            "previous_classification": previous_state.get("classification"),
            "classification": current_state.get("classification"),
            "previous_signature": previous_state.get("signature"),
            "signature": current_state.get("signature"),
            "read_only": True,
            "paper_proof_invoked": False,
            "live_money_eligible": False,
        }
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write read-only Track B PAPER crash-loop protection policy.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_CRASH_LOOP_PROTECTION_ARTIFACT)
    parser.add_argument("--event-log-path", type=Path, default=DEFAULT_CRASH_LOOP_EVENTS)
    parser.add_argument("--dashboard-projection-path", type=Path, default=DEFAULT_DASHBOARD_CRASH_LOOP_PROTECTION_PROJECTION)
    parser.add_argument("--no-dashboard-projection", action="store_true")
    parser.add_argument("--restart-window-seconds", type=float, default=900.0)
    parser.add_argument("--cooldown-seconds", type=float, default=300.0)
    parser.add_argument("--max-restarts-per-window", type=int, default=2)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBCrashLoopProtectionConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
        event_log_path=Path(args.event_log_path),
        dashboard_projection_path=None if bool(args.no_dashboard_projection) else Path(args.dashboard_projection_path),
        restart_window_seconds=float(args.restart_window_seconds),
        cooldown_seconds=float(args.cooldown_seconds),
        max_restarts_per_window=int(args.max_restarts_per_window),
    )
    payload = build_track_b_crash_loop_protection(config=config)
    authority_path, _ = write_track_b_crash_loop_protection(config=config, payload=payload)
    if bool(args.json):
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            json.dumps(
                {
                    "classification": payload.get("classification"),
                    "restart_blocked": payload.get("restart_blocked"),
                    "reason": payload.get("reason"),
                    "cooldown_until": payload.get("cooldown_until"),
                    "authority_path": str(authority_path),
                    "read_only": True,
                    "paper_proof_invoked": False,
                    "live_money_eligible": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
    return 0 if payload.get("restart_blocked") is not True else 2


def _classify_crash_loop(
    *,
    config: TrackBCrashLoopProtectionConfig,
    inputs: Mapping[str, Mapping[str, Any]],
    events: list[dict[str, Any]],
    now: datetime,
) -> dict[str, Any]:
    unsafe = [event for event in events if event.get("broker_safe_at_stop") is False]
    if len(unsafe) >= 2:
        return _decision(
            OPERATOR_ACK_REQUIRED,
            True,
            "Repeated unsafe runtime stops with broker_safe_at_stop=false.",
            events=events,
            now=now,
            config=config,
            operator_ack_required=True,
        )
    if _market_closed(inputs):
        return _decision(NO_CRASH_LOOP, False, "Market is closed; no fresh bars expected.", events=events, now=now, config=config)
    if not events:
        return _decision(INSUFFICIENT_HISTORY, False, "No recent crash-loop history is available.", events=events, now=now, config=config)
    broker_lease_failures = [event for event in events if event.get("classification") in BROKER_LEASE_FAILURES or event.get("stop_reason") in BROKER_LEASE_FAILURES]
    broker_lease_class = _broker_lease_classification(inputs["broker_lease"])
    if len(broker_lease_failures) >= 2 or broker_lease_class in BROKER_LEASE_FAILURES:
        return _decision(
            REPEATED_BROKER_LEASE_FAILURE,
            True,
            "Broker lease refresh is repeatedly degraded or failing.",
            events=events,
            now=now,
            config=config,
        )
    runtime_failures = [event for event in events if _runtime_pre_convergence_failure(event)]
    if len(runtime_failures) >= config.max_restarts_per_window:
        return _decision(
            RESTART_COOLDOWN_ACTIVE,
            True,
            "Repeated runtime failures before sustained convergence within restart window.",
            events=events,
            now=now,
            config=config,
        )
    market_data_failures = [event for event in events if "MARKET_DATA" in str(event.get("classification") or event.get("stop_reason") or "")]
    if len(market_data_failures) >= config.max_restarts_per_window:
        return _decision(
            REPEATED_MARKET_DATA_FAILURE,
            True,
            "Repeated market-data producer failures within restart window.",
            events=events,
            now=now,
            config=config,
        )
    return _decision(NO_CRASH_LOOP, False, "No crash loop detected in restart window.", events=events, now=now, config=config)


def _decision(
    classification: str,
    restart_blocked: bool,
    reason: str,
    *,
    events: list[dict[str, Any]],
    now: datetime,
    config: TrackBCrashLoopProtectionConfig,
    operator_ack_required: bool = False,
) -> dict[str, Any]:
    latest = _latest_event(events)
    same_reason_count = _same_value_count(events, "stop_reason", latest.get("stop_reason"))
    same_source_count = _same_value_count(events, "stop_source", latest.get("stop_source"))
    cooldown_until = None
    if classification == RESTART_COOLDOWN_ACTIVE:
        base = _parse_time(latest.get("generated_at") or latest.get("observed_at")) or now
        cooldown_until = (base + timedelta(seconds=config.cooldown_seconds)).isoformat()
    return {
        "classification": classification,
        "restart_blocked": restart_blocked,
        "reason": reason,
        "same_reason_count": same_reason_count,
        "same_source_count": same_source_count,
        "cooldown_until": cooldown_until,
        "operator_ack_required": operator_ack_required or classification == OPERATOR_ACK_REQUIRED,
    }


def _collect_history_events(*, config: TrackBCrashLoopProtectionConfig, now: datetime) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for payload in (
        _read_json(config.resolve(config.stop_provenance_path)),
        _read_json(config.resolve(config.launch_status_path)),
    ):
        event = _event_from_payload(payload)
        if event:
            events.append(event)
    for path in (config.resolve(config.launch_status_history_path), config.resolve(config.runtime_truth_history_path)):
        for payload in _read_jsonl(path):
            event = _event_from_payload(payload)
            if event:
                events.append(event)
    return sorted(_dedupe_events(events), key=lambda item: str(item.get("generated_at") or item.get("observed_at") or ""))


def _event_from_payload(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    if not payload:
        return None
    provenance = _mapping(payload.get("stop_provenance")) or payload
    classification = str(payload.get("classification") or provenance.get("classification") or "")
    stop_reason = str(provenance.get("stop_reason") or payload.get("stop_reason") or classification)
    stop_source = str(provenance.get("stop_source") or payload.get("stop_source") or "")
    generated_at = (
        provenance.get("observed_at")
        or payload.get("generated_at")
        or payload.get("observed_at")
        or provenance.get("requested_at")
    )
    if not (classification or stop_reason or stop_source):
        return None
    return {
        "generated_at": generated_at,
        "classification": classification or None,
        "stop_reason": stop_reason or None,
        "stop_source": stop_source or None,
        "runtime_instance_id": provenance.get("runtime_instance_id") or payload.get("runtime_instance_id"),
        "restart_generation": provenance.get("restart_generation") or payload.get("restart_generation"),
        "source_commit": provenance.get("source_commit") or payload.get("source_commit"),
        "expected_cleanup": provenance.get("expected_cleanup"),
        "broker_safe_at_stop": provenance.get("broker_safe_at_stop"),
        "control_action_id": provenance.get("control_action_id"),
    }


def _window_events(*, events: list[dict[str, Any]], now: datetime, window_seconds: float) -> list[dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    for event in events:
        parsed = _parse_time(event.get("generated_at"))
        if parsed is None:
            kept.append(event)
            continue
        if (now - parsed).total_seconds() <= window_seconds:
            kept.append(event)
    return kept


def _runtime_pre_convergence_failure(event: Mapping[str, Any]) -> bool:
    return str(event.get("classification") or "") in RUNTIME_PRE_CONVERGENCE_FAILURES or str(event.get("stop_reason") or "") in RUNTIME_PRE_CONVERGENCE_FAILURES


def _market_closed(inputs: Mapping[str, Mapping[str, Any]]) -> bool:
    self_recover = inputs["self_recover_rules"]
    if self_recover.get("recommendation") == WAIT_MARKET_CLOSED or self_recover.get("reason") == MARKET_CLOSED_NO_FRESH_BARS:
        return True
    agent_health = inputs["agent_health"]
    for agent in _list(agent_health.get("agents")):
        if agent.get("agent_id") == "phase1_databento_live_candles" and agent.get("reason") == MARKET_CLOSED_NO_FRESH_BARS:
            return True
    return False


def _broker_lease_classification(payload: Mapping[str, Any]) -> str:
    return str(payload.get("classification") or payload.get("lease_state") or "")


def _input_evidence(inputs: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "agent_registry_classification": _classification(inputs["agent_registry"]),
        "agent_health_classification": _classification(inputs["agent_health"]),
        "runtime_environment_truth_classification": _classification(inputs["runtime_environment_truth"]),
        "self_recover_recommendation": inputs["self_recover_rules"].get("recommendation"),
        "broker_lease_classification": _broker_lease_classification(inputs["broker_lease"]),
    }


def _event_state(*, decision: Mapping[str, Any], events: list[dict[str, Any]]) -> dict[str, Any]:
    signature_parts = [
        str(decision.get("classification") or ""),
        str(decision.get("restart_blocked") or False),
        str(len(events)),
        str(decision.get("cooldown_until") or ""),
        str(decision.get("operator_ack_required") or False),
    ]
    return {
        "classification": decision.get("classification"),
        "restart_blocked": decision.get("restart_blocked"),
        "signature": "|".join(signature_parts),
    }


def _latest_event(events: list[dict[str, Any]]) -> dict[str, Any]:
    return events[-1] if events else {}


def _same_value_count(events: list[dict[str, Any]], key: str, value: Any) -> int:
    if value in (None, ""):
        return 0
    return sum(1 for event in events if event.get(key) == value)


def _dedupe_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    kept: list[dict[str, Any]] = []
    for event in events:
        signature = "|".join(str(event.get(key) or "") for key in ("generated_at", "classification", "stop_reason", "runtime_instance_id"))
        if signature in seen:
            continue
        seen.add(signature)
        kept.append(event)
    return kept


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    rows: list[dict[str, Any]] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, Mapping):
            rows.append(dict(payload))
    return rows


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


def _classification(payload: Mapping[str, Any]) -> str:
    return str(payload.get("classification") or "")


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


if __name__ == "__main__":
    raise SystemExit(main())
