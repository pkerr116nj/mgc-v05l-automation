"""Runner for Asia Drift compression-then-continuation detector research."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..research.asia_drift import (
    DEFAULT_WIDER_REPLAY_WINDOWS,
    AsiaDriftReplayWindow,
    DIAGNOSTIC_INSTRUMENTS,
    PRIMARY_INSTRUMENTS,
    run_compression_continuation_detector,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE_DB = REPO_ROOT / "mgc_v05l.replay.archive.sqlite3"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "asia_drift_v1_compression_continuation"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="asia-drift-v1-compression-continuation")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="Replay SQLite database path.")
    parser.add_argument(
        "--instrument",
        action="append",
        dest="instruments",
        default=None,
        help="Primary/comparative instrument. Defaults to MGC,GC,MES,ES. May be supplied multiple times.",
    )
    parser.add_argument(
        "--diagnostic-instrument",
        action="append",
        dest="diagnostic_instruments",
        default=None,
        help="Optional diagnostic-only instrument, e.g. NQ or MNQ. May be supplied multiple times.",
    )
    parser.add_argument(
        "--window",
        action="append",
        dest="windows",
        default=None,
        help="Window in label,start_ts,end_ts form. May be supplied multiple times.",
    )
    parser.add_argument("--output-dir", default=None)
    return parser


def run_asia_drift_v1_compression_continuation(
    *,
    source_db: Path,
    instruments: tuple[str, ...] = PRIMARY_INSTRUMENTS,
    diagnostic_instruments: tuple[str, ...] = (),
    windows: tuple[AsiaDriftReplayWindow, ...] = DEFAULT_WIDER_REPLAY_WINDOWS,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    resolved_output_dir = (
        output_dir.resolve()
        if output_dir is not None
        else (DEFAULT_OUTPUT_ROOT / datetime.now(UTC).strftime("%Y%m%d_%H%M%S")).resolve()
    )
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    result = run_compression_continuation_detector(
        source_sqlite_path=source_db.resolve(),
        output_dir=resolved_output_dir,
        instruments=instruments,
        diagnostic_instruments=diagnostic_instruments,
        windows=windows,
    )
    artifacts = result["artifacts"]
    return {
        "output_dir": str(resolved_output_dir),
        "summary_json_path": artifacts["summary_json_path"],
        "summary_markdown_path": artifacts["summary_markdown_path"],
        "comparison_json_path": artifacts["comparison_json_path"],
        "comparison_markdown_path": artifacts["comparison_markdown_path"],
        "funnel_json_path": artifacts["funnel_json_path"],
        "funnel_markdown_path": artifacts["funnel_markdown_path"],
        "maturation_json_path": artifacts["maturation_json_path"],
        "maturation_markdown_path": artifacts["maturation_markdown_path"],
        "per_bar_rows_path": artifacts["per_bar_rows_path"],
        "compression_windows_path": artifacts["compression_windows_path"],
        "continuation_confirmations_path": artifacts["continuation_confirmations_path"],
        "false_breaks_path": artifacts["false_breaks_path"],
        "missed_target_audit_path": artifacts["missed_target_audit_path"],
        "session_stage_rows_path": artifacts["session_stage_rows_path"],
        "candidate_lifecycle_rows_path": artifacts["candidate_lifecycle_rows_path"],
        "candidate_discovery_manifest_path": artifacts["candidate_discovery_manifest_path"],
        "pending_unresolved_manifest_path": artifacts["pending_unresolved_manifest_path"],
        "confirmed_rejection_manifest_path": artifacts["confirmed_rejection_manifest_path"],
        "contamination_breakdown_path": artifacts["contamination_breakdown_path"],
        "session_summary_path": artifacts["session_summary_path"],
        "storage_manifest_path": artifacts["storage_manifest_path"],
        "recommendation": result["payload"]["recommendation"],
    }


def _parse_windows(raw_windows: list[str] | None) -> tuple[AsiaDriftReplayWindow, ...]:
    if not raw_windows:
        return DEFAULT_WIDER_REPLAY_WINDOWS
    windows: list[AsiaDriftReplayWindow] = []
    for raw in raw_windows:
        label, start_ts, end_ts = raw.split(",", 2)
        windows.append(
            AsiaDriftReplayWindow(
                label=label,
                start_ts=datetime.fromisoformat(start_ts),
                end_ts=datetime.fromisoformat(end_ts),
            )
        )
    return tuple(windows)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_asia_drift_v1_compression_continuation(
        source_db=Path(args.source_db),
        instruments=tuple(args.instruments or PRIMARY_INSTRUMENTS),
        diagnostic_instruments=tuple(args.diagnostic_instruments or ()),
        windows=_parse_windows(args.windows),
        output_dir=Path(args.output_dir) if args.output_dir else None,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
