"""Produce the Track B canonical PAPER readiness artifact.

This CLI is intentionally independent of the operator dashboard server. It
delegates readiness logic to execution_core.track_b_readiness_state so launch
scripts, watchdogs, and operators can publish the canonical state while the
dashboard remains a consumer.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_readiness_state import (
    DEFAULT_CANONICAL_READINESS_ARTIFACT,
    REPO_ROOT,
    write_canonical_readiness_artifact,
)

READY_EXIT_STATES = {"READY_SUBMIT_CAPABLE", "READY_OBSERVATION_ONLY", "READY_TO_START_DIAGNOSTIC_ONLY"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="track-b-canonical-readiness")
    parser.add_argument("--repo-root", default=str(REPO_ROOT), help="Repository root to read artifacts from.")
    parser.add_argument(
        "--expected-root",
        default=None,
        help="Expected active Track B root. Defaults to the execution_core configured Dev root.",
    )
    parser.add_argument(
        "--output-path",
        default=None,
        help="Canonical readiness JSON path. Relative paths are resolved under --repo-root.",
    )
    parser.add_argument(
        "--summary-output-path",
        default=None,
        help="Compact canonical readiness summary JSON path. Relative paths are resolved under --repo-root.",
    )
    parser.add_argument("--json", action="store_true", help="Print the compact readiness summary as JSON.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = Path(args.repo_root).expanduser().resolve()
    expected_root = Path(args.expected_root).expanduser().resolve() if args.expected_root else None
    output_path = _resolve_output_path(repo_root, args.output_path)
    payload = write_canonical_readiness_artifact(
        repo_root=repo_root,
        expected_root=expected_root,
        output_path=output_path,
    )
    summary = compact_readiness_summary(payload)
    summary_output_path = _resolve_optional_output_path(repo_root, args.summary_output_path)
    if summary_output_path is not None:
        _write_json_atomic(summary_output_path, summary)
    print_summary(summary, as_json=args.json)
    return exit_code_for_classification(str(summary["classification"]))


def _resolve_output_path(repo_root: Path, output_path: str | None) -> Path:
    if output_path:
        path = Path(output_path).expanduser()
        return path if path.is_absolute() else repo_root / path
    return repo_root / DEFAULT_CANONICAL_READINESS_ARTIFACT


def _resolve_optional_output_path(repo_root: Path, output_path: str | None) -> Path | None:
    if not output_path:
        return None
    path = Path(output_path).expanduser()
    return path if path.is_absolute() else repo_root / path


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def compact_readiness_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    broker_truth = _mapping(payload.get("broker_truth"))
    broker_truth_lease = _mapping(payload.get("broker_truth_lease"))
    broker_session_allowed_uses = _mapping(payload.get("broker_session_allowed_uses"))
    phase1_reconciliation = _mapping(payload.get("phase1_reconciliation"))
    execution_core_shared_truth = _mapping(payload.get("execution_core_shared_truth"))
    shared_truth_classifications = _mapping(execution_core_shared_truth.get("classifications"))
    proof_readiness = _mapping(execution_core_shared_truth.get("proof_readiness"))
    runtime = _mapping(payload.get("runtime"))
    lane_quarantine = _mapping(payload.get("lane_quarantine"))
    root_guard = _mapping(payload.get("root_guard_summary"))
    return {
        "generated_at": payload.get("generated_at"),
        "classification": str(payload.get("canonical_readiness") or payload.get("state") or "NOT_READY_CONFIG"),
        "runtime_start_allowed": payload.get("runtime_start_allowed") is True,
        "submit_allowed": payload.get("submit_allowed") is True,
        "blockers": _codes(payload.get("readiness_blockers")),
        "warnings": _codes(payload.get("readiness_warnings")),
        "market_schedule_state": payload.get("market_schedule_state"),
        "stale_market_data_expected": payload.get("stale_market_data_expected") is True,
        "next_expected_reopen_time": payload.get("next_expected_reopen_time"),
        "market_data_grace_until": payload.get("market_data_grace_until"),
        "readiness_block_is_scheduled_halt": payload.get("readiness_block_is_scheduled_halt") is True,
        "root_match": root_guard.get("root_match") is True,
        "broker_truth_fresh": broker_truth.get("fresh") is True,
        "broker_truth_lease_state": broker_truth_lease.get("lease_state") or "BROKER_TRUTH_LEASE_MISSING",
        "broker_lease_degraded_diagnostic": payload.get("broker_lease_degraded_diagnostic") is True,
        "broker_truth_lease_age_seconds": broker_truth_lease.get("age_seconds"),
        "broker_truth_lease_entry_seconds_remaining": broker_truth_lease.get("entry_seconds_remaining"),
        "broker_session_authority_classification": payload.get("broker_session_authority_classification")
        or "BROKER_SESSION_AUTHORITY_MISSING",
        "broker_session_connection_mode": payload.get("broker_session_connection_mode") or "UNKNOWN",
        "broker_session_allowed_uses": broker_session_allowed_uses,
        "broker_session_authority_blockers": _codes(payload.get("broker_session_authority_blockers")),
        "callback_ownership_attribution": payload.get("callback_ownership_attribution"),
        "broker_session_submit_alignment": payload.get("broker_session_submit_alignment") or "UNKNOWN",
        "proof_readiness_classification": proof_readiness.get("classification"),
        "shared_truth_open_order_truth": shared_truth_classifications.get("Open Order Truth"),
        "shared_truth_managed_order_registry": shared_truth_classifications.get("Managed Order Registry"),
        "shared_truth_order_adjustment_planner": shared_truth_classifications.get("Order Adjustment Planner"),
        "shared_truth_position_truth": shared_truth_classifications.get("Position Truth"),
        "shared_truth_runtime_environment_truth": shared_truth_classifications.get("Runtime Environment Truth"),
        "shared_truth_managed_position_registry": shared_truth_classifications.get("Managed Position Registry"),
        "shared_truth_broker_lease": shared_truth_classifications.get("Broker Truth Lease"),
        "reconciliation_state": phase1_reconciliation.get("classification")
        or "TRACK_B_PAPER_BROKER_RECONCILIATION_UNKNOWN",
        "eligible_lane_count": int(runtime.get("eligible_lane_count") or 0),
        "quarantine_count": int(lane_quarantine.get("quarantine_count") or 0),
    }


def print_summary(summary: Mapping[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(dict(summary), indent=2, sort_keys=True))
        return
    for key in (
        "classification",
        "runtime_start_allowed",
        "submit_allowed",
        "blockers",
        "warnings",
        "market_schedule_state",
        "stale_market_data_expected",
        "next_expected_reopen_time",
        "market_data_grace_until",
        "readiness_block_is_scheduled_halt",
        "root_match",
        "broker_truth_fresh",
        "broker_truth_lease_state",
        "broker_lease_degraded_diagnostic",
        "broker_truth_lease_age_seconds",
        "broker_truth_lease_entry_seconds_remaining",
        "broker_session_authority_classification",
        "broker_session_connection_mode",
        "broker_session_allowed_uses",
        "broker_session_authority_blockers",
        "callback_ownership_attribution",
        "broker_session_submit_alignment",
        "proof_readiness_classification",
        "shared_truth_open_order_truth",
        "shared_truth_managed_order_registry",
        "shared_truth_order_adjustment_planner",
        "shared_truth_position_truth",
        "shared_truth_runtime_environment_truth",
        "shared_truth_managed_position_registry",
        "shared_truth_broker_lease",
        "reconciliation_state",
        "eligible_lane_count",
        "quarantine_count",
    ):
        print(f"{key}={_format_summary_value(summary.get(key))}")


def exit_code_for_classification(classification: str) -> int:
    if classification in READY_EXIT_STATES:
        return 0
    if classification == "DEGRADED_NO_SUBMIT":
        return 1
    return 2


def _codes(rows: Any) -> list[str]:
    if not isinstance(rows, list):
        return []
    codes = []
    for row in rows:
        if isinstance(row, Mapping):
            code = str(row.get("code") or "").strip()
            if code:
                codes.append(code)
    return codes


def _format_summary_value(value: Any) -> str:
    if isinstance(value, list):
        return ",".join(str(item) for item in value) if value else "none"
    return str(value)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


if __name__ == "__main__":
    raise SystemExit(main())
