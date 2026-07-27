"""CLI for RA8 live trade path accumulation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_live_trade_path_accumulator import (
    DEFAULT_CADENCE_SECONDS,
    DEFAULT_CANONICAL_TRADE_RECORDS,
    DEFAULT_DURABLE_CANDLE_ROOT,
    DEFAULT_MANAGED_POSITIONS,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_RUNTIME_CANDLE_ROOT,
    run_live_trade_path_accumulator_cadence_once,
    run_live_trade_path_accumulator_cadence_service,
    run_live_trade_path_accumulator,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Accumulate and finalize research-only live trade paths.")
    parser.add_argument(
        "command",
        choices=[
            "accumulate-open-paths",
            "finalize-closed-paths",
            "repair-finalized-paths",
            "status",
            "publish-ra8-artifacts",
            "run-cadence-once",
            "service",
        ],
    )
    parser.add_argument("--managed-positions-path", type=Path, default=DEFAULT_MANAGED_POSITIONS)
    parser.add_argument("--canonical-records-path", type=Path, default=DEFAULT_CANONICAL_TRADE_RECORDS)
    parser.add_argument("--runtime-candle-root", type=Path, default=DEFAULT_RUNTIME_CANDLE_ROOT)
    parser.add_argument("--durable-candle-root", type=Path, default=DEFAULT_DURABLE_CANDLE_ROOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--repair-finalized", action="store_true")
    parser.add_argument("--finalization-grace-seconds", type=int, default=120)
    parser.add_argument("--cadence-seconds", type=float, default=DEFAULT_CADENCE_SECONDS)
    parser.add_argument("--max-iterations", type=int, help="Optional bounded service iterations for tests/manual validation.")
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "run-cadence-once":
        result = run_live_trade_path_accumulator_cadence_once(
            managed_positions_path=args.managed_positions_path,
            canonical_records_path=args.canonical_records_path,
            runtime_candle_root=args.runtime_candle_root,
            durable_candle_root=args.durable_candle_root,
            output_dir=args.output_dir,
            finalization_grace_seconds=args.finalization_grace_seconds,
            cadence_seconds=args.cadence_seconds,
            now=args.now,
        )
        print(json.dumps(_cadence_summary(result.status, result.status_path, result.lock_path), sort_keys=True))
        return 0 if result.status.get("classification") != "RA8_CADENCE_FAILED" else 2
    if args.command == "service":
        result = run_live_trade_path_accumulator_cadence_service(
            managed_positions_path=args.managed_positions_path,
            canonical_records_path=args.canonical_records_path,
            runtime_candle_root=args.runtime_candle_root,
            durable_candle_root=args.durable_candle_root,
            output_dir=args.output_dir,
            finalization_grace_seconds=args.finalization_grace_seconds,
            cadence_seconds=args.cadence_seconds,
            max_iterations=args.max_iterations,
        )
        print(json.dumps(_cadence_summary(result.status, result.status_path, result.lock_path), sort_keys=True))
        return 0 if result.status.get("classification") != "RA8_CADENCE_FAILED" else 2
    result = run_live_trade_path_accumulator(
        managed_positions_path=args.managed_positions_path,
        canonical_records_path=args.canonical_records_path,
        runtime_candle_root=args.runtime_candle_root,
        durable_candle_root=args.durable_candle_root,
        output_dir=args.output_dir,
        accumulate_open_paths=args.command in {"accumulate-open-paths", "publish-ra8-artifacts"},
        finalize_closed_paths=args.command in {"finalize-closed-paths", "repair-finalized-paths", "publish-ra8-artifacts"},
        repair_finalized=args.repair_finalized or args.command == "repair-finalized-paths",
        finalization_grace_seconds=args.finalization_grace_seconds,
        now=args.now,
    )
    status = result.status
    print(
        json.dumps(
            {
                "schema_version": status.get("schema_version"),
                "classification": status.get("classification"),
                "open_accumulator_count": status.get("open_accumulator_count"),
                "finalized_path_count": status.get("finalized_path_count"),
                "closed_canonical_record_count": status.get("closed_canonical_record_count"),
                "accumulated_open_path_updates": status.get("accumulated_open_path_updates"),
                "newly_finalized_path_count": status.get("newly_finalized_path_count"),
                "deferred_finalization_count": status.get("deferred_finalization_count"),
                "repaired_finalized_path_count": status.get("repaired_finalized_path_count"),
                "complete_finalized_count": status.get("coverage", {}).get("complete_finalized_count"),
                "partial_entry_missing_count": status.get("coverage", {}).get("partial_entry_missing_count"),
                "partial_exit_missing_count": status.get("coverage", {}).get("partial_exit_missing_count"),
                "timebox_ready_count": status.get("readiness", {}).get("timebox_ready_count"),
                "trailing_ready_count": status.get("readiness", {}).get("trailing_ready_count"),
                "status_path": str(result.status_path),
                "open_path": str(result.open_path),
                "finalized_path": str(result.finalized_path),
                "diagnostic_only": status.get("diagnostic_only"),
                "production_recommendation": status.get("production_recommendation"),
                "trading_gate": status.get("trading_gate"),
            },
            sort_keys=True,
        )
    )
    return 0


def _cadence_summary(status: dict, status_path: Path, lock_path: Path) -> dict:
    return {
        "schema_version": status.get("schema_version"),
        "classification": status.get("classification"),
        "cadence_seconds": status.get("cadence_seconds"),
        "last_successful_run_at": status.get("last_successful_run_at"),
        "last_duration_seconds": status.get("last_duration_seconds"),
        "last_rows_updated": status.get("last_rows_updated"),
        "last_open_accumulator_count": status.get("last_open_accumulator_count"),
        "last_finalized_path_count": status.get("last_finalized_path_count"),
        "last_error": status.get("last_error"),
        "status_path": str(status_path),
        "lock_path": str(lock_path),
        "diagnostic_only": status.get("diagnostic_only"),
        "production_recommendation": status.get("production_recommendation"),
        "trading_gate": status.get("trading_gate"),
    }


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
