"""Runner for Asia Drift market/trade data continuity audit."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from ..research.asia_drift import run_data_continuity_audit


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_REPLAY_DB = REPO_ROOT / "mgc_v05l.replay.archive.sqlite3"
DEFAULT_WAREHOUSE_ROOT = REPO_ROOT / "outputs" / "research_platform" / "warehouse" / "historical_evaluator"
DEFAULT_MULTI_YEAR_DIR = REPO_ROOT / "outputs" / "reports" / "asia_drift_v1_multi_year_discovery" / "full_20240101_20260421"
DEFAULT_CROSS_ASSET_DIR = REPO_ROOT / "outputs" / "reports" / "asia_drift_v1_cross_asset_confirmation" / "full_20240101_20260421"
DEFAULT_RUNTIME_BRIDGE_DIR = REPO_ROOT / "outputs" / "research_runtime_bridge" / "default_warehouse_paper"
DEFAULT_OPERATOR_DASHBOARD_DIR = REPO_ROOT / "outputs" / "operator_dashboard"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "asia_drift_v1_data_continuity_audit" / "full_20240101_20260421"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="asia-drift-v1-data-continuity-audit")
    parser.add_argument("--replay-db", default=str(DEFAULT_REPLAY_DB))
    parser.add_argument("--warehouse-root", default=str(DEFAULT_WAREHOUSE_ROOT))
    parser.add_argument("--multi-year-dir", default=str(DEFAULT_MULTI_YEAR_DIR))
    parser.add_argument("--cross-asset-dir", default=str(DEFAULT_CROSS_ASSET_DIR))
    parser.add_argument("--runtime-bridge-dir", default=str(DEFAULT_RUNTIME_BRIDGE_DIR))
    parser.add_argument("--operator-dashboard-dir", default=str(DEFAULT_OPERATOR_DASHBOARD_DIR))
    parser.add_argument(
        "--instrument",
        action="append",
        dest="instruments",
        default=None,
        help="Instrument to include in the audit. May be supplied multiple times. Defaults to MGC,GC,ES,MES,NQ,MNQ.",
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser


def run_asia_drift_v1_data_continuity_audit(
    *,
    replay_db: Path,
    warehouse_root: Path,
    multi_year_dir: Path,
    cross_asset_dir: Path,
    runtime_bridge_dir: Path | None = None,
    operator_dashboard_dir: Path | None = None,
    instruments: tuple[str, ...] | None = None,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    resolved_output_dir = output_dir.resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    result = run_data_continuity_audit(
        output_dir=resolved_output_dir,
        replay_db_path=replay_db.resolve(),
        warehouse_root=warehouse_root.resolve(),
        multi_year_output_dir=multi_year_dir.resolve(),
        cross_asset_output_dir=cross_asset_dir.resolve(),
        runtime_bridge_dir=runtime_bridge_dir.resolve() if runtime_bridge_dir is not None else None,
        operator_dashboard_dir=operator_dashboard_dir.resolve() if operator_dashboard_dir is not None else None,
        instruments=tuple(instruments or ("MGC", "GC", "ES", "MES", "NQ", "MNQ")),
    )
    artifacts = result["artifacts"]
    return {
        "output_dir": str(resolved_output_dir),
        "summary_json_path": artifacts["summary_json_path"],
        "summary_md_path": artifacts["summary_md_path"],
        "replay_coverage_csv": artifacts["replay_coverage_csv"],
        "replay_gap_csv": artifacts["replay_gap_csv"],
        "replay_monthly_csv": artifacts["replay_monthly_csv"],
        "trade_coverage_json": artifacts["trade_coverage_json"],
        "alignment_json": artifacts["alignment_json"],
        "root_cause_json": artifacts["root_cause_json"],
        "repair_plan_json": artifacts["repair_plan_json"],
        "storage_manifest_path": artifacts["storage_manifest_path"],
        "verdict": result["payload"]["root_cause_summary"]["verdict"],
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_asia_drift_v1_data_continuity_audit(
        replay_db=Path(args.replay_db),
        warehouse_root=Path(args.warehouse_root),
        multi_year_dir=Path(args.multi_year_dir),
        cross_asset_dir=Path(args.cross_asset_dir),
        runtime_bridge_dir=Path(args.runtime_bridge_dir) if args.runtime_bridge_dir else None,
        operator_dashboard_dir=Path(args.operator_dashboard_dir) if args.operator_dashboard_dir else None,
        instruments=tuple(args.instruments) if args.instruments else None,
        output_dir=Path(args.output_dir),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
