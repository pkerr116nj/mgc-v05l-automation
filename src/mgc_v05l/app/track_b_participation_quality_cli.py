"""Offline CLI/demo runner for Track B participation-quality classification."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_participation_quality import (
    INPUT_MODE_OFFLINE_EVALUATION,
    INPUT_MODE_REPLAY_RESEARCH,
    INPUT_MODE_RUNTIME_DECISION,
    INPUT_MODE_TEST,
    LOW_CONFIDENCE_THIN_DATA,
    RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE,
    SOURCE_CATEGORY_RESEARCH,
    SOURCE_CATEGORY_TEST_FIXTURE,
    build_participation_quality_state,
    track_b_input_context,
    write_participation_quality_state,
)

LATEST_STATE_FILENAME = "latest_participation_quality_state.json"
SAFE_INPUT_MODES = (INPUT_MODE_OFFLINE_EVALUATION, INPUT_MODE_TEST, INPUT_MODE_REPLAY_RESEARCH)
SAFE_SOURCE_CATEGORIES = (SOURCE_CATEGORY_TEST_FIXTURE, SOURCE_CATEGORY_RESEARCH)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the Track B participation-quality classifier against an offline JSON candle fixture. "
            "Writes quality context only; no broker, strategy, lifecycle, or reconciliation paths are invoked."
        )
    )
    parser.add_argument("--candle-json", required=True, type=Path, help="Offline candle fixture/input JSON path.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for latest_participation_quality_state.json.")
    parser.add_argument("--instrument", help="Optional instrument override for demo fixtures.")
    parser.add_argument("--candle-timeframe", default="5m")
    parser.add_argument("--input-mode", choices=SAFE_INPUT_MODES, default=INPUT_MODE_OFFLINE_EVALUATION)
    parser.add_argument("--input-source-category", choices=SAFE_SOURCE_CATEGORIES, default=SOURCE_CATEGORY_TEST_FIXTURE)
    parser.add_argument("--now", help="Optional UTC-ish ISO timestamp for deterministic offline demos/tests.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = _read_payload(args.candle_json)
    if args.instrument:
        payload = {**payload, "instrument": args.instrument}

    output_path = args.output_dir / LATEST_STATE_FILENAME
    now = _parse_now(args.now)
    if _claims_research_runtime_truth(payload, args.candle_json):
        report = build_participation_quality_state(
            payload,
            now=now,
            candle_timeframe=args.candle_timeframe,
            input_mode=INPUT_MODE_RUNTIME_DECISION,
            input_source_path=args.candle_json,
            input_source_category=SOURCE_CATEGORY_RESEARCH,
        )
        report = {**report, "artifact_path": str(output_path.resolve())}
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        _print_summary(report, output_path=output_path, refused=True)
        return 2

    report = write_participation_quality_state(
        payload,
        output_path=output_path,
        now=now,
        candle_timeframe=args.candle_timeframe,
        input_mode=args.input_mode,
        input_source_path=args.candle_json,
        input_source_category=args.input_source_category,
    )
    _print_summary(report, output_path=output_path, refused=False)
    return 0


def _read_payload(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return {"candles": payload, "test_fixture": True}
    if not isinstance(payload, dict):
        raise SystemExit("--candle-json must contain a JSON object or a candle array.")
    return dict(payload)


def _claims_research_runtime_truth(payload: Mapping[str, Any], source_path: Path) -> bool:
    input_context = track_b_input_context(payload)
    if input_context.get("input_source_path") is None:
        input_context = track_b_input_context(payload, input_source_path=source_path)
    return (
        input_context["input_mode"] == INPUT_MODE_RUNTIME_DECISION
        and input_context["input_source_category"] == SOURCE_CATEGORY_RESEARCH
    )


def _parse_now(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _print_summary(report: Mapping[str, Any], *, output_path: Path, refused: bool) -> None:
    print(
        json.dumps(
            {
                "classification": report.get("participation_state"),
                "confidence_state": report.get("confidence_state"),
                "confidence": report.get("confidence"),
                "long_hold_quality": report.get("long_hold_quality"),
                "short_hold_quality": report.get("short_hold_quality"),
                "input_mode": report.get("input_mode"),
                "input_source_category": report.get("input_source_category"),
                "source_provenance_status": report.get("source_provenance_status"),
                "confidence_failure_reasons": report.get("confidence_failure_reasons", []),
                "refused_research_runtime_truth": refused,
                "wrote_state": output_path.exists(),
                "state_path": str(output_path.resolve()),
                "offline_only": True,
                "strategy_authority": report.get("strategy_authority"),
                "runtime_trade_eligible": report.get("runtime_trade_eligible"),
                "broker_state_mutated": report.get("broker_state_mutated"),
                "submit_attempted": report.get("submit_attempted"),
                "cancel_attempted": report.get("cancel_attempted"),
                "close_attempted": report.get("close_attempted"),
                "place_order_attempted": report.get("place_order_attempted"),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
