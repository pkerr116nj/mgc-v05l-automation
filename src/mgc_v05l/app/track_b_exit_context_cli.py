"""Offline CLI for Track B exit context evaluation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_exit_context import build_exit_context_state

LATEST_EXIT_CONTEXT_FILENAME = "latest_exit_context_state.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate an offline Track B exit context fixture and write advisory JSON output. No broker or runtime authority."
    )
    parser.add_argument("--input-json", required=True, type=Path, help="Offline JSON fixture or input payload.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for latest_exit_context_state.json.")
    parser.add_argument("--now", help="Optional ISO timestamp used for deterministic freshness evaluation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = _load_payload(args.input_json)
    payload = _with_offline_defaults(payload, input_path=args.input_json)
    report = build_exit_context_state(payload, now=args.now)
    output_path = args.output_dir / LATEST_EXIT_CONTEXT_FILENAME
    summary = _summary(report, output_path=output_path)

    if report["failure_reasons"]:
        print(json.dumps(summary, sort_keys=True))
        return 2

    args.output_dir.mkdir(parents=True, exist_ok=True)
    report = {**report, "artifact_path": str(output_path.resolve())}
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(_summary(report, output_path=output_path), sort_keys=True))
    return 0


def _load_payload(path: Path) -> Mapping[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
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


def _summary(report: Mapping[str, Any], *, output_path: Path) -> dict[str, Any]:
    return {
        "schema_version": report.get("schema_version"),
        "producer": report.get("producer"),
        "exit_profile_context": report.get("exit_profile_context"),
        "exit_urgency_context": report.get("exit_urgency_context"),
        "hold_quality_context": report.get("hold_quality_context"),
        "reduce_size_context": report.get("reduce_size_context"),
        "scale_up_context": report.get("scale_up_context"),
        "confidence": report.get("confidence"),
        "failure_reasons": report.get("failure_reasons", []),
        "strategy_authority": report.get("strategy_authority"),
        "broker_state_mutated": report.get("broker_state_mutated"),
        "submit_attempted": report.get("submit_attempted"),
        "order_intent_created": report.get("order_intent_created"),
        "lifecycle_mutated": report.get("lifecycle_mutated"),
        "runtime_trade_eligible": report.get("runtime_trade_eligible"),
        "output_path": str(output_path.resolve()),
        "wrote_artifact": bool(report.get("artifact_path")),
    }


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
