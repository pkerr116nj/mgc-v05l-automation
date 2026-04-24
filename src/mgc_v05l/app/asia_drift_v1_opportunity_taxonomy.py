"""Runner for Asia Drift v1 opportunity taxonomy over wider replay samples."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..research.asia_drift import (
    DEFAULT_WIDER_REPLAY_WINDOWS,
    AsiaDriftReplayWindow,
    run_asia_drift_opportunity_taxonomy,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE_DB = REPO_ROOT / "mgc_v05l.replay.archive.sqlite3"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "asia_drift_v1_opportunity_taxonomy"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="asia-drift-v1-opportunity-taxonomy")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="Replay SQLite database path.")
    parser.add_argument("--instrument", default="MGC", help="Primary instrument. Defaults to MGC.")
    parser.add_argument(
        "--reference-instrument",
        action="append",
        dest="reference_instruments",
        default=None,
        help="Optional reference instrument, e.g. GC. May be supplied multiple times.",
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


def run_asia_drift_v1_opportunity_taxonomy(
    *,
    source_db: Path,
    instrument: str = "MGC",
    reference_instruments: tuple[str, ...] = (),
    windows: tuple[AsiaDriftReplayWindow, ...] = DEFAULT_WIDER_REPLAY_WINDOWS,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    resolved_output_dir = (
        output_dir.resolve()
        if output_dir is not None
        else (DEFAULT_OUTPUT_ROOT / datetime.now(UTC).strftime("%Y%m%d_%H%M%S")).resolve()
    )
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    result = run_asia_drift_opportunity_taxonomy(
        source_sqlite_path=source_db.resolve(),
        output_dir=resolved_output_dir,
        primary_instrument=instrument,
        windows=windows,
        reference_instruments=reference_instruments,
    )
    artifacts = result["artifacts"]
    return {
        "output_dir": str(resolved_output_dir),
        "summary_json_path": artifacts["summary_json_path"],
        "summary_markdown_path": artifacts["summary_markdown_path"],
        "window_rows_path": artifacts["window_rows_path"],
        "session_manifest_path": artifacts["session_manifest_path"],
        "session_rows_path": artifacts["session_rows_path"],
        "missed_audit_path": artifacts["missed_audit_path"],
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
    payload = run_asia_drift_v1_opportunity_taxonomy(
        source_db=Path(args.source_db),
        instrument=args.instrument,
        reference_instruments=tuple(args.reference_instruments or ()),
        windows=_parse_windows(args.windows),
        output_dir=Path(args.output_dir) if args.output_dir else None,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
