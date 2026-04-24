"""Runner for Asia Drift v1 refined shallow prefill persistence comparison."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from ..research.asia_drift import (
    DEFAULT_PREFILL_COMPARISON_PROFILES,
    DEFAULT_REFINED_REPLAY_WINDOWS,
    AsiaDriftReplayWindow,
    run_prefill_persistence_comparison,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE_DB = REPO_ROOT / "mgc_v05l.replay.archive.sqlite3"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "asia_drift_v1_prefill_persistence"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="asia-drift-v1-prefill-persistence")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB))
    parser.add_argument("--instrument", default="MGC")
    parser.add_argument("--reference-instrument", action="append", dest="reference_instruments", default=None)
    parser.add_argument("--profile", action="append", dest="profiles", default=None)
    parser.add_argument("--window", action="append", dest="windows", default=None)
    parser.add_argument("--output-dir", default=None)
    return parser


def run_asia_drift_v1_prefill_persistence(
    *,
    source_db: Path,
    instrument: str = "MGC",
    reference_instruments: tuple[str, ...] = (),
    profiles: tuple[str, ...] = DEFAULT_PREFILL_COMPARISON_PROFILES,
    windows: tuple[AsiaDriftReplayWindow, ...] = DEFAULT_REFINED_REPLAY_WINDOWS,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    resolved_output_dir = (output_dir or DEFAULT_OUTPUT_ROOT).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    result = run_prefill_persistence_comparison(
        source_sqlite_path=source_db.resolve(),
        output_dir=resolved_output_dir,
        primary_instrument=instrument,
        windows=windows,
        reference_instruments=reference_instruments,
        profile_names=profiles,
    )
    artifacts = result["artifacts"]
    return {
        "output_dir": str(resolved_output_dir),
        "comparison_json_path": artifacts["comparison_json_path"],
        "comparison_markdown_path": artifacts["comparison_markdown_path"],
        "comparison_rows_path": artifacts["comparison_rows_path"],
        "storage_manifest_path": artifacts["storage_manifest_path"],
        "recommendation": result["payload"]["recommendation"],
    }


def _parse_windows(raw_windows: list[str] | None) -> tuple[AsiaDriftReplayWindow, ...]:
    if not raw_windows:
        return DEFAULT_REFINED_REPLAY_WINDOWS
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


from datetime import datetime


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_asia_drift_v1_prefill_persistence(
        source_db=Path(args.source_db),
        instrument=args.instrument,
        reference_instruments=tuple(args.reference_instruments or ()),
        profiles=tuple(args.profiles or DEFAULT_PREFILL_COMPARISON_PROFILES),
        windows=_parse_windows(args.windows),
        output_dir=Path(args.output_dir) if args.output_dir else None,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
