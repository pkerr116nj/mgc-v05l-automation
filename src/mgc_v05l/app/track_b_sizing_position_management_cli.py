"""Offline CLI for Track B sizing/position-management advisory context."""

from __future__ import annotations

import argparse
import json
import sys
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_sizing_position_management import (
    NO_POSITION,
    NOT_ALLOWED_V1,
    PRODUCER_NAME,
    SCHEMA_VERSION,
    UNKNOWN,
    build_sizing_position_management_state,
)


LATEST_SIZING_POSITION_MANAGEMENT_FILENAME = "latest_sizing_position_management_context.json"
MALFORMED_INPUT = "MALFORMED_INPUT"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate an offline Track B sizing/position-management JSON payload and write advisory context. "
            "No broker, runtime, lane, strategy, order, or lifecycle authority is invoked."
        )
    )
    parser.add_argument("--input-json", required=True, type=Path, help="Explicit offline sizing input payload.")
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="Directory for latest_sizing_position_management_context.json.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_path = args.output_dir / LATEST_SIZING_POSITION_MANAGEMENT_FILENAME

    try:
        payload = _load_payload(args.input_json)
        payload = _with_offline_defaults(payload, input_path=args.input_json)
        report = build_sizing_position_management_state(payload)
        report = {
            **report,
            "input_source_path": payload["input_source_path"],
            "input_source_category": payload["input_source_category"],
            "input_mode": payload["input_mode"],
            "test_fixture": payload["test_fixture"],
        }
    except (OSError, JSONDecodeError, SystemExit) as exc:
        report = _fail_closed_report(input_path=args.input_json, reason=str(exc) or MALFORMED_INPUT)

    report = {**report, "artifact_path": str(output_path.resolve())}
    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(_summary(report, output_path=output_path), sort_keys=True))
    return 2 if report["failure_reasons"] else 0


def _load_payload(path: Path) -> Mapping[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise SystemExit(f"input JSON must be an object: {path}")
    return payload


def _with_offline_defaults(payload: Mapping[str, Any], *, input_path: Path) -> dict[str, Any]:
    updated = dict(payload)
    updated.setdefault("input_source_path", str(input_path))
    updated.setdefault("input_source_category", "TEST_FIXTURE")
    updated.setdefault("input_mode", "TEST")
    updated.setdefault("test_fixture", True)
    return updated


def _fail_closed_report(*, input_path: Path, reason: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "producer": PRODUCER_NAME,
        "initial_size_context": NO_POSITION,
        "in_position_size_context": UNKNOWN,
        "add_size_context": NOT_ALLOWED_V1,
        "confidence": 0.0,
        "sizing_reasons": ["FAIL_CLOSED_MALFORMED_INPUT"],
        "management_reasons": ["FAIL_CLOSED_MALFORMED_INPUT"],
        "warning_reasons": [],
        "failure_reasons": [MALFORMED_INPUT, reason],
        "strategy_family": UNKNOWN,
        "exit_profile": UNKNOWN,
        "instrument": UNKNOWN,
        "timeframe": UNKNOWN,
        "source_contexts": {},
        "input_source_path": str(input_path),
        "input_source_category": "TEST_FIXTURE",
        "input_mode": "TEST",
        "test_fixture": True,
        "strategy_authority": False,
        "broker_state_mutated": False,
        "submit_attempted": False,
        "order_intent_created": False,
        "lifecycle_mutated": False,
        "runtime_trade_eligible": False,
    }


def _summary(report: Mapping[str, Any], *, output_path: Path) -> dict[str, Any]:
    return {
        "schema_version": report.get("schema_version"),
        "producer": report.get("producer"),
        "initial_size_context": report.get("initial_size_context"),
        "in_position_size_context": report.get("in_position_size_context"),
        "add_size_context": report.get("add_size_context"),
        "confidence": report.get("confidence"),
        "failure_reasons": report.get("failure_reasons", []),
        "strategy_family": report.get("strategy_family"),
        "exit_profile": report.get("exit_profile"),
        "instrument": report.get("instrument"),
        "timeframe": report.get("timeframe"),
        "strategy_authority": report.get("strategy_authority"),
        "broker_state_mutated": report.get("broker_state_mutated"),
        "submit_attempted": report.get("submit_attempted"),
        "order_intent_created": report.get("order_intent_created"),
        "lifecycle_mutated": report.get("lifecycle_mutated"),
        "runtime_trade_eligible": report.get("runtime_trade_eligible"),
        "offline_only": True,
        "output_path": str(output_path.resolve()),
        "wrote_artifact": output_path.exists(),
    }


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
