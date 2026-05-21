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

READY_EXIT_STATES = {"READY_SUBMIT_CAPABLE", "READY_OBSERVATION_ONLY"}


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
    phase1_reconciliation = _mapping(payload.get("phase1_reconciliation"))
    runtime = _mapping(payload.get("runtime"))
    lane_quarantine = _mapping(payload.get("lane_quarantine"))
    root_guard = _mapping(payload.get("root_guard_summary"))
    return {
        "generated_at": payload.get("generated_at"),
        "classification": str(payload.get("canonical_readiness") or payload.get("state") or "NOT_READY_CONFIG"),
        "blockers": _codes(payload.get("readiness_blockers")),
        "warnings": _codes(payload.get("readiness_warnings")),
        "root_match": root_guard.get("root_match") is True,
        "broker_truth_fresh": broker_truth.get("fresh") is True,
        "broker_truth_lease_state": broker_truth_lease.get("lease_state") or "BROKER_TRUTH_LEASE_MISSING",
        "broker_truth_lease_age_seconds": broker_truth_lease.get("age_seconds"),
        "broker_truth_lease_entry_seconds_remaining": broker_truth_lease.get("entry_seconds_remaining"),
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
        "blockers",
        "warnings",
        "root_match",
        "broker_truth_fresh",
        "broker_truth_lease_state",
        "broker_truth_lease_age_seconds",
        "broker_truth_lease_entry_seconds_remaining",
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
