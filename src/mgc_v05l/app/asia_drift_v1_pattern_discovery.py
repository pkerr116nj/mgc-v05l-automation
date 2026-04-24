"""Runner for Asia Drift broad pattern discovery."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..research.asia_drift import (
    DEFAULT_PATTERN_DISCOVERY_INSTRUMENTS,
    DEFAULT_WIDER_REPLAY_WINDOWS,
    AsiaDriftReplayWindow,
    run_asia_drift_pattern_discovery,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE_DB = REPO_ROOT / "mgc_v05l.replay.archive.sqlite3"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "asia_drift_v1_pattern_discovery"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="asia-drift-v1-pattern-discovery")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="Replay SQLite database path.")
    parser.add_argument(
        "--instrument",
        action="append",
        dest="instruments",
        default=None,
        help="Instrument to include in the discovery pass. Defaults to MGC,GC,MES,ES,MNQ,NQ. May be supplied multiple times.",
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


def run_asia_drift_v1_pattern_discovery(
    *,
    source_db: Path,
    instruments: tuple[str, ...] = DEFAULT_PATTERN_DISCOVERY_INSTRUMENTS,
    windows: tuple[AsiaDriftReplayWindow, ...] = DEFAULT_WIDER_REPLAY_WINDOWS,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    resolved_output_dir = (
        output_dir.resolve()
        if output_dir is not None
        else (DEFAULT_OUTPUT_ROOT / datetime.now(UTC).strftime("%Y%m%d_%H%M%S")).resolve()
    )
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    result = run_asia_drift_pattern_discovery(
        source_sqlite_path=source_db.resolve(),
        output_dir=resolved_output_dir,
        instruments=instruments,
        windows=windows,
    )
    artifacts = result["artifacts"]
    return {
        "output_dir": str(resolved_output_dir),
        "summary_json_path": artifacts["summary_json_path"],
        "summary_markdown_path": artifacts["summary_markdown_path"],
        "session_manifest_path": artifacts["session_manifest_path"],
        "session_rows_path": artifacts["session_rows_path"],
        "feature_rows_path": artifacts["feature_rows_path"],
        "top_patterns_path": artifacts["top_patterns_path"],
        "rejected_patterns_path": artifacts["rejected_patterns_path"],
        "research_queue_path": artifacts["research_queue_path"],
        "cross_asset_events_path": artifacts["cross_asset_events_path"],
        "window_rows_path": artifacts["window_rows_path"],
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
    payload = run_asia_drift_v1_pattern_discovery(
        source_db=Path(args.source_db),
        instruments=tuple(args.instruments or DEFAULT_PATTERN_DISCOVERY_INSTRUMENTS),
        windows=_parse_windows(args.windows),
        output_dir=Path(args.output_dir) if args.output_dir else None,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
