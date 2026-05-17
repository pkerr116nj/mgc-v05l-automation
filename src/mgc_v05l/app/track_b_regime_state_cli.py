"""Offline CLI for Track B regime/session context evaluation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_regime_state import build_regime_session_context_state


LATEST_REGIME_SESSION_FILENAME = "latest_regime_session_context.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate an offline Track B regime/session JSON payload and write advisory context. "
            "No broker, runtime, lane, strategy, or lifecycle authority is invoked."
        )
    )
    parser.add_argument("--input-json", required=True, type=Path, help="Explicit offline regime/session input payload.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for latest_regime_session_context.json.")
    parser.add_argument("--now", help="Optional ISO timestamp used for deterministic freshness evaluation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = _load_payload(args.input_json)
    payload = _with_offline_defaults(payload, input_path=args.input_json)
    report = build_regime_session_context_state(payload, now=args.now)
    output_path = args.output_dir / LATEST_REGIME_SESSION_FILENAME
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


def _summary(report: Mapping[str, Any], *, output_path: Path) -> dict[str, Any]:
    return {
        "schema_version": report.get("schema_version"),
        "producer": report.get("producer"),
        "market_regime_state": report.get("market_regime_state"),
        "volatility_range_state": report.get("volatility_range_state"),
        "trend_chop_state": report.get("trend_chop_state"),
        "liquidity_state": report.get("liquidity_state"),
        "directional_context": report.get("directional_context"),
        "session_bucket": report.get("session_bucket"),
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
