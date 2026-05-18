"""Produce the Track B shared readiness-maintenance supervisor decision artifact."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_readiness_maintenance_supervisor import (
    DEFAULT_DECISION_ARTIFACT,
    classify_readiness_maintenance,
    write_maintenance_supervisor_decision,
)
from mgc_v05l.execution_core.track_b_readiness_state import REPO_ROOT

READY_EXIT_STATES = {"OBSERVING", "RECOVERED"}
DEGRADED_EXIT_STATES = {"DEGRADED", "REPAIRING_RECOMMENDED"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="track-b-readiness-maintenance-supervisor")
    parser.add_argument("--repo-root", default=str(REPO_ROOT), help="Repository root to read artifacts from.")
    parser.add_argument(
        "--output-path",
        default=None,
        help="Supervisor decision JSON path. Relative paths are resolved under --repo-root.",
    )
    parser.add_argument("--json", action="store_true", help="Print compact supervisor summary as JSON.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = Path(args.repo_root).expanduser().resolve()
    output_path = _resolve_output_path(repo_root, args.output_path)
    inputs = gather_supervisor_inputs(repo_root=repo_root)
    decision = classify_readiness_maintenance(inputs)
    decision = {
        **decision,
        "source_artifact_timestamps": _source_artifact_timestamps(inputs),
        "source_artifacts": _source_artifact_paths(inputs),
        "live_money_eligible": False,
    }
    write_maintenance_supervisor_decision(output_path=output_path, decision=decision)
    summary = compact_supervisor_summary(decision)
    print_summary(summary, as_json=args.json)
    return exit_code_for_state(str(summary["supervisor_state"]))


def gather_supervisor_inputs(*, repo_root: Path) -> dict[str, Any]:
    paths = _artifact_paths(repo_root)
    canonical = _read_json(paths["canonical_readiness"])
    ibkr_connectivity = _read_json(paths["ibkr_connectivity"])
    broker_truth = _read_json(paths["broker_truth"])
    broker_truth_lease = _read_json(paths["broker_truth_lease"])
    reconciliation = _read_json(paths["reconciliation"])
    operator_status = _read_json(paths["runtime_health"])
    lane_quarantine = _read_json(paths["lane_quarantine"])
    market_data = _read_json(paths["market_data"])
    root_guard = _mapping(canonical.get("root_guard_summary"))

    runtime = _runtime_input(operator_status=operator_status, canonical=canonical)
    return {
        "generated_at": canonical.get("generated_at") or broker_truth.get("generated_at") or ibkr_connectivity.get("generated_at"),
        "canonical_readiness": canonical,
        "ibkr_connectivity": _with_available(ibkr_connectivity, paths["ibkr_connectivity"]),
        "broker_truth": _with_available(broker_truth or _mapping(canonical.get("broker_truth")), paths["broker_truth"]),
        "broker_truth_lease": _with_available(
            broker_truth_lease or _mapping(canonical.get("broker_truth_lease")),
            paths["broker_truth_lease"],
        ),
        "market_data": _with_available(market_data or _mapping(canonical.get("market_data")), paths["market_data"]),
        "runtime": _with_available(runtime, paths["runtime_health"]),
        "lane_quarantine": _with_available(lane_quarantine or _mapping(canonical.get("lane_quarantine")), paths["lane_quarantine"]),
        "reconciliation": _with_available(reconciliation or _mapping(canonical.get("phase1_reconciliation")), paths["reconciliation"]),
        "root_guard": root_guard,
        "artifact_pressure": _read_json(paths["artifact_pressure"]),
        "_source_paths": {key: str(path) for key, path in paths.items()},
        "_source_timestamps": {key: _artifact_timestamp(path, _read_json(path)) for key, path in paths.items()},
    }


def compact_supervisor_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "supervisor_state": str(payload.get("supervisor_state") or "BLOCKED"),
        "recommended_actions": list(payload.get("recommended_actions") or []),
        "action_scope": list(payload.get("action_scope") or []),
        "blockers": _codes(payload.get("blockers")),
        "warnings": _codes(payload.get("warnings")),
        "retry_count": int(payload.get("retry_count") or 0),
        "cooldown_until": payload.get("cooldown_until"),
        "operator_action_required": payload.get("operator_action_required") is True,
        "submit_block_required": payload.get("submit_block_required") is True,
        "canonical_readiness": payload.get("canonical_readiness"),
        "live_money_eligible": False,
    }


def print_summary(summary: Mapping[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(dict(summary), indent=2, sort_keys=True))
        return
    for key in (
        "supervisor_state",
        "recommended_actions",
        "action_scope",
        "blockers",
        "warnings",
        "retry_count",
        "cooldown_until",
        "operator_action_required",
        "submit_block_required",
        "canonical_readiness",
        "live_money_eligible",
    ):
        print(f"{key}={_format_summary_value(summary.get(key))}")


def exit_code_for_state(state: str) -> int:
    if state in READY_EXIT_STATES:
        return 0
    if state in DEGRADED_EXIT_STATES:
        return 1
    return 2


def _artifact_paths(repo_root: Path) -> dict[str, Path]:
    runtime_root = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    runtime_dir = runtime_root / "runtime"
    reports_root = repo_root / "outputs" / "reports"
    return {
        "canonical_readiness": repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json",
        "broker_truth_lease": repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json",
        "ibkr_connectivity": reports_root / "ibkr_connectivity_watchdog" / "latest_ibkr_connectivity_watchdog.json",
        "broker_truth": reports_root / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json",
        "reconciliation": reports_root
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json",
        "runtime_health": runtime_root / "operator_status.json",
        "lane_quarantine": runtime_dir / "paper_lane_quarantine_status.json",
        "market_data": runtime_dir / "market_data_transport_probe.json",
        "artifact_pressure": repo_root
        / "outputs"
        / "operator_dashboard"
        / "runtime"
        / "latest_artifact_pressure_status.json",
    }


def _resolve_output_path(repo_root: Path, output_path: str | None) -> Path:
    if output_path:
        path = Path(output_path).expanduser()
        return path if path.is_absolute() else repo_root / path
    return repo_root / DEFAULT_DECISION_ARTIFACT


def _runtime_input(*, operator_status: Mapping[str, Any], canonical: Mapping[str, Any]) -> dict[str, Any]:
    canonical_runtime = _mapping(canonical.get("runtime"))
    if not operator_status:
        return canonical_runtime
    health = _mapping(operator_status.get("health"))
    return {
        **canonical_runtime,
        "classification": operator_status.get("classification") or operator_status.get("status"),
        "running": operator_status.get("running") is True or bool(operator_status.get("source_runtime_pid")),
        "healthy": operator_status.get("healthy") is True or health.get("runtime_ok") is True or health.get("market_data_ok") is True,
        "eligible_lane_count": operator_status.get("usable_lane_count")
        or operator_status.get("eligible_lane_count")
        or canonical_runtime.get("eligible_lane_count"),
        "loaded_lane_count": operator_status.get("paper_lane_count")
        or operator_status.get("loaded_lane_count")
        or canonical_runtime.get("loaded_lane_count"),
        "live_money_eligible": operator_status.get("live_money_eligible") is True,
    }


def _with_available(payload: Mapping[str, Any], path: Path) -> dict[str, Any]:
    return {**dict(payload), "available": bool(payload), "source_path": str(path)}


def _source_artifact_timestamps(inputs: Mapping[str, Any]) -> dict[str, Any]:
    timestamps = _mapping(inputs.get("_source_timestamps"))
    return {key: value for key, value in timestamps.items() if value is not None}


def _source_artifact_paths(inputs: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(inputs.get("_source_paths"))


def _artifact_timestamp(path: Path, payload: Mapping[str, Any]) -> str | None:
    if payload.get("generated_at"):
        return str(payload["generated_at"])
    if payload.get("completed_at"):
        return str(payload["completed_at"])
    if not path.exists():
        return None
    try:
        return str(path.stat().st_mtime)
    except OSError:
        return None


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _codes(rows: Any) -> list[str]:
    if not isinstance(rows, list):
        return []
    codes: list[str] = []
    for row in rows:
        if isinstance(row, Mapping):
            code = str(row.get("code") or "").strip()
            if code:
                codes.append(code)
    return codes


def _format_summary_value(value: Any) -> str:
    if isinstance(value, list):
        return ",".join(str(item) for item in value) if value else "none"
    if value is None:
        return "none"
    return str(value)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


if __name__ == "__main__":
    raise SystemExit(main())
