"""Runner for Asia Drift v1 pullback/invalidation calibration comparison."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..research.asia_drift.calibration import DEFAULT_COMPARISON_PROFILES, run_calibration_comparison


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE_DB = REPO_ROOT / "mgc_v05l.replay.sqlite3"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "asia_drift_v1_calibration"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="asia-drift-v1-calibration")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="Replay SQLite database path.")
    parser.add_argument("--instrument", action="append", dest="instruments", default=None)
    parser.add_argument("--start-ts", default=None)
    parser.add_argument("--end-ts", default=None)
    parser.add_argument("--profile", action="append", dest="profiles", default=None)
    parser.add_argument("--output-dir", default=None)
    return parser


def run_asia_drift_v1_calibration(
    *,
    source_db: Path,
    instruments: tuple[str, ...] = ("MGC",),
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
    profiles: tuple[str, ...] = DEFAULT_COMPARISON_PROFILES,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    resolved_output_dir = (
        output_dir.resolve()
        if output_dir is not None
        else (DEFAULT_OUTPUT_ROOT / datetime.now(UTC).strftime("%Y%m%d_%H%M%S")).resolve()
    )
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    result = run_calibration_comparison(
        source_sqlite_path=source_db.resolve(),
        output_dir=resolved_output_dir,
        instruments=instruments,
        start_ts=start_ts,
        end_ts=end_ts,
        profile_names=profiles,
    )
    artifacts = result["artifacts"]
    return {
        "output_dir": str(resolved_output_dir),
        "comparison_json_path": artifacts["comparison_json_path"],
        "comparison_markdown_path": artifacts["comparison_markdown_path"],
        "comparison_rows_path": artifacts["comparison_rows_path"],
        "storage_manifest_path": artifacts["storage_manifest_path"],
        "recommended_next_default": result["comparison"]["recommendation"]["recommended_next_default"],
        "recommended_diagnostic_ceiling": result["comparison"]["recommendation"]["recommended_diagnostic_ceiling"],
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_asia_drift_v1_calibration(
        source_db=Path(args.source_db),
        instruments=tuple(args.instruments or ["MGC"]),
        start_ts=datetime.fromisoformat(args.start_ts) if args.start_ts else None,
        end_ts=datetime.fromisoformat(args.end_ts) if args.end_ts else None,
        profiles=tuple(args.profiles or DEFAULT_COMPARISON_PROFILES),
        output_dir=Path(args.output_dir) if args.output_dir else None,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
