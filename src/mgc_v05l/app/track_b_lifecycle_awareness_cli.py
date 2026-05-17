"""Offline CLI for Track B lifecycle awareness evaluation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_lifecycle_awareness import build_lifecycle_awareness_state

LATEST_LIFECYCLE_AWARENESS_FILENAME = "latest_lifecycle_awareness_state.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate an offline Track B lifecycle awareness JSON payload and write advisory state. "
            "No broker, runtime, strategy, or lifecycle authority is invoked."
        )
    )
    parser.add_argument("--input-json", required=True, type=Path, help="Explicit offline lifecycle-awareness input payload.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for latest_lifecycle_awareness_state.json.")
    parser.add_argument("--now", help="Optional ISO timestamp used for deterministic freshness evaluation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = _load_payload(args.input_json)
    payload = _with_offline_defaults(payload, input_path=args.input_json)
    report = build_lifecycle_awareness_state(payload, now=args.now)
    output_path = args.output_dir / LATEST_LIFECYCLE_AWARENESS_FILENAME
    report = {**report, "artifact_path": str(output_path.resolve())}

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(_summary(report, output_path=output_path), sort_keys=True))
    return 2 if report["failure_reasons"] else 0


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
        "lifecycle_awareness_state": report.get("lifecycle_awareness_state"),
        "hold_quality_context": report.get("hold_quality_context"),
        "exit_urgency_context": report.get("exit_urgency_context"),
        "reduce_size_context": report.get("reduce_size_context"),
        "add_size_context": report.get("add_size_context"),
        "patience_context": report.get("patience_context"),
        "confidence": report.get("confidence"),
        "failure_reasons": report.get("failure_reasons", []),
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
