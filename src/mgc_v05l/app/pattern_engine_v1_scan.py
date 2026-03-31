"""Operator-facing Pattern Engine v1 scan for replay OHLCV history."""

from __future__ import annotations

import json
from pathlib import Path

from ..research.pattern_engine import build_pattern_engine_v1_report, write_pattern_engine_v1_report


def build_and_write_pattern_engine_v1_scan(*, summary_path: Path, ticker: str = "MGC", timeframe: str = "5m") -> dict[str, str]:
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    replay_db_path = Path(summary["replay_db_path"])
    prefix = Path(str(summary_path).removesuffix(".summary.json"))
    detail_path = prefix.with_suffix(".pattern_engine_v1_detail.csv")
    family_summary_path = prefix.with_suffix(".pattern_engine_v1_family_summary.csv")
    sequence_summary_path = prefix.with_suffix(".pattern_engine_v1_sequence_summary.csv")
    summary_json_path = prefix.with_suffix(".pattern_engine_v1_summary.json")

    matches, family_rows, sequence_rows, summary_payload = build_pattern_engine_v1_report(
        replay_db_path=replay_db_path,
        ticker=ticker,
        timeframe=timeframe,
    )
    write_pattern_engine_v1_report(
        detail_path=detail_path,
        family_summary_path=family_summary_path,
        sequence_summary_path=sequence_summary_path,
        summary_json_path=summary_json_path,
        matches=matches,
        family_rows=family_rows,
        sequence_rows=sequence_rows,
        summary_payload=summary_payload,
    )
    return {
        "pattern_engine_v1_detail_path": str(detail_path),
        "pattern_engine_v1_family_summary_path": str(family_summary_path),
        "pattern_engine_v1_sequence_summary_path": str(sequence_summary_path),
        "pattern_engine_v1_summary_path": str(summary_json_path),
    }


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summary_path", type=Path)
    parser.add_argument("--ticker", default="MGC")
    parser.add_argument("--timeframe", default="5m")
    args = parser.parse_args()

    outputs = build_and_write_pattern_engine_v1_scan(
        summary_path=args.summary_path,
        ticker=args.ticker,
        timeframe=args.timeframe,
    )
    print(json.dumps(outputs, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
