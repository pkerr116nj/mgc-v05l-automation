"""Replay-first runner for Asia Drift v1 Phase 2 research artifacts."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..research.asia_drift import RECOVERY_CONFIRMED, run_asia_drift_phase2


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE_DB = REPO_ROOT / "mgc_v05l.replay.sqlite3"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "asia_drift_v1_phase2"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="asia-drift-v1-phase2")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="Replay SQLite database path.")
    parser.add_argument(
        "--instrument",
        action="append",
        dest="instruments",
        default=None,
        help="Instrument to include. May be supplied multiple times. Defaults to MGC.",
    )
    parser.add_argument("--start-ts", default=None, help="Optional ISO-8601 lower bound on bar end_ts.")
    parser.add_argument("--end-ts", default=None, help="Optional ISO-8601 upper bound on bar end_ts.")
    parser.add_argument("--calibration-profile", default=RECOVERY_CONFIRMED, help="Asia Drift pullback/invalidation profile.")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional output directory. Defaults to a timestamped folder under outputs/reports/asia_drift_v1_phase2.",
    )
    return parser


def run_asia_drift_v1_phase2(
    *,
    source_db: Path,
    instruments: tuple[str, ...] = ("MGC",),
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
    output_dir: Path | None = None,
    calibration_profile_name: str = RECOVERY_CONFIRMED,
) -> dict[str, Any]:
    resolved_output_dir = (
        output_dir.resolve()
        if output_dir is not None
        else (DEFAULT_OUTPUT_ROOT / datetime.now(UTC).strftime("%Y%m%d_%H%M%S")).resolve()
    )
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    run = run_asia_drift_phase2(
        source_sqlite_path=source_db.resolve(),
        output_dir=resolved_output_dir,
        instruments=instruments,
        start_ts=start_ts,
        end_ts=end_ts,
        calibration_profile_name=calibration_profile_name,
    )
    return {
        "output_dir": str(resolved_output_dir),
        "phase1_root_dir": str(run.phase1_run.artifacts.root_dir),
        "phase2_summary_json_path": str(run.artifacts.summary_json_path),
        "phase2_summary_markdown_path": str(run.artifacts.summary_markdown_path),
        "phase2_diagnostics_json_path": str(run.artifacts.diagnostics_json_path),
        "entry_setups_path": str(run.artifacts.entry_setups_path),
        "entry_evaluations_path": str(run.artifacts.entry_evaluations_path),
        "trade_records_path": str(run.artifacts.trade_records_path),
        "storage_manifest_path": str(run.artifacts.storage_manifest_path),
        "entry_setup_count": len(run.entry_setups),
        "entry_evaluation_count": len(run.entry_evaluations),
        "trade_record_count": len(run.trade_records),
        "calibration_profile": calibration_profile_name,
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    start_ts = datetime.fromisoformat(args.start_ts) if args.start_ts else None
    end_ts = datetime.fromisoformat(args.end_ts) if args.end_ts else None
    payload = run_asia_drift_v1_phase2(
        source_db=Path(args.source_db),
        instruments=tuple((args.instruments or ["MGC"])),
        start_ts=start_ts,
        end_ts=end_ts,
        output_dir=Path(args.output_dir) if args.output_dir else None,
        calibration_profile_name=args.calibration_profile,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
