"""Weekly data maintenance command surface.

This wraps the Track B weekly maintenance orchestrator with explicit status,
dry-run, approved apply, and backfill-planning commands. Backfill commands are
plan-only in this module; they do not fetch market data or mutate broker state.
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_weekly_maintenance_orchestrator import (
    DEFAULT_OUTPUT_PATH,
    DEFAULT_RETENTION_POLICY_PATH,
    REPO_ROOT,
    TrackBWeeklyMaintenanceOrchestratorConfig,
    build_track_b_weekly_maintenance_orchestrator,
    write_track_b_weekly_maintenance_orchestrator,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Weekly maintenance status, dry-run, apply, and backfill planning.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--retention-policy", type=Path, default=DEFAULT_RETENTION_POLICY_PATH)
    parser.add_argument("--json", action="store_true", help="Print full JSON payload.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    status = subparsers.add_parser("status", help="Write and print weekly maintenance status.")
    status.add_argument("--no-write", action="store_true", help="Print status without writing report artifacts.")

    dry_run = subparsers.add_parser("dry-run", help="Run weekly maintenance dry-run checks and write report artifacts.")
    dry_run.add_argument("--no-write", action="store_true", help="Print dry-run without writing report artifacts.")

    apply = subparsers.add_parser("apply", help="Write approved weekly maintenance report artifacts.")
    apply.add_argument("--approved", action="store_true", help="Required acknowledgement for apply mode.")

    backfill = subparsers.add_parser("backfill", help="Plan a historical data backfill window without running it.")
    backfill.add_argument("--from", dest="from_date", required=True)
    backfill.add_argument("--to", dest="to_date", required=True)
    backfill.add_argument("--dry-run", action="store_true", required=True)
    backfill.add_argument("--no-write", action="store_true", help="Print plan without writing report artifacts.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "apply" and not bool(args.approved):
        print(json.dumps({"error": "APPROVAL_REQUIRED", "required_flag": "--approved"}, indent=2, sort_keys=True))
        return 2

    repo_root = Path(args.repo_root).expanduser().resolve()
    config = TrackBWeeklyMaintenanceOrchestratorConfig(
        repo_root=repo_root,
        output_path=Path(args.output_path),
        retention_policy_path=Path(args.retention_policy),
        force=True,
        run_retention_maintenance_dry_run=args.command in {"dry-run", "apply"},
    )
    payload = build_track_b_weekly_maintenance_orchestrator(config=config)
    payload["command"] = f"weekly-maintenance {args.command}"
    payload["apply_approved"] = bool(getattr(args, "approved", False))
    payload["backfill_execution_invoked"] = False
    payload["broker_mutation_allowed"] = False
    payload["runtime_restart"] = False
    if args.command == "backfill":
        payload["requested_backfill"] = _backfill_plan(from_date=args.from_date, to_date=args.to_date)

    written: dict[str, Path] = {}
    if not bool(getattr(args, "no_write", False)):
        written = write_track_b_weekly_maintenance_orchestrator(config=config, payload=payload)

    summary = _summary(payload=payload, written=written, config=config)
    print(json.dumps(payload if bool(args.json) else summary, indent=2, sort_keys=True))
    return 0


def _backfill_plan(*, from_date: str, to_date: str) -> dict[str, Any]:
    start = _parse_date_start(from_date)
    end = _parse_date_end(to_date)
    return {
        "from": start.isoformat(),
        "to": end.isoformat(),
        "dry_run": True,
        "execute_backfill": False,
        "recommended_command": (
            "weekly-maintenance backfill "
            f"--from {start.date().isoformat()} --to {end.date().isoformat()} --dry-run"
        ),
        "large_backfill_requires_explicit_approval": True,
    }


def _parse_date_start(value: str) -> datetime:
    parsed = date.fromisoformat(value)
    return datetime.combine(parsed, time(0, 0), tzinfo=UTC)


def _parse_date_end(value: str) -> datetime:
    parsed = date.fromisoformat(value)
    return datetime.combine(parsed, time(23, 59, 59), tzinfo=UTC)


def _summary(
    *,
    payload: Mapping[str, Any],
    written: Mapping[str, Path],
    config: TrackBWeeklyMaintenanceOrchestratorConfig,
) -> dict[str, Any]:
    return {
        "command": payload.get("command"),
        "overall_classification": payload.get("overall_classification"),
        "completion_status": payload.get("completion_status"),
        "last_two_weekly_runs_missed": _mapping(payload.get("backfill_tracking")).get("last_two_weekly_runs_missed"),
        "recommended_backfill_date_ranges": _mapping(payload.get("backfill_tracking")).get("recommended_backfill_date_ranges"),
        "generated_artifact_retention_classification": _mapping(payload.get("generated_artifact_retention_status")).get("classification"),
        "retention_violation_count": _mapping(payload.get("generated_artifact_retention_status")).get("violation_count"),
        "pass_fail_summary": payload.get("pass_fail_summary"),
        "report_path": str(written.get("json") or config.resolve(config.output_path)),
        "dry_run_only": payload.get("dry_run_only"),
        "broker_mutation_allowed": False,
        "runtime_restart": False,
    }


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


if __name__ == "__main__":
    raise SystemExit(main())
