"""Runner for Asia Drift cross-asset trade mapping research."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..research.asia_drift import run_cross_asset_trade_mapping


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CROSS_ASSET_DIR = REPO_ROOT / "outputs" / "reports" / "asia_drift_v1_cross_asset_confirmation" / "full_20240101_20260421"
DEFAULT_MULTI_YEAR_DIR = REPO_ROOT / "outputs" / "reports" / "asia_drift_v1_multi_year_discovery" / "full_20240101_20260421"
DEFAULT_WAREHOUSE_ROOT = REPO_ROOT / "outputs" / "research_platform" / "warehouse" / "historical_evaluator"
DEFAULT_RUNTIME_BRIDGE_DIR = REPO_ROOT / "outputs" / "research_runtime_bridge" / "default_warehouse_paper"
DEFAULT_OPERATOR_DASHBOARD_DIR = REPO_ROOT / "outputs" / "operator_dashboard"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "asia_drift_v1_cross_asset_trade_mapping"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="asia-drift-v1-cross-asset-trade-mapping")
    parser.add_argument("--cross-asset-dir", default=str(DEFAULT_CROSS_ASSET_DIR))
    parser.add_argument("--multi-year-dir", default=str(DEFAULT_MULTI_YEAR_DIR))
    parser.add_argument("--warehouse-root", default=str(DEFAULT_WAREHOUSE_ROOT))
    parser.add_argument("--runtime-bridge-dir", default=str(DEFAULT_RUNTIME_BRIDGE_DIR))
    parser.add_argument("--operator-dashboard-dir", default=str(DEFAULT_OPERATOR_DASHBOARD_DIR))
    parser.add_argument("--recent-session-count", type=int, default=20)
    parser.add_argument("--output-dir", default=None)
    return parser


def run_asia_drift_v1_cross_asset_trade_mapping(
    *,
    cross_asset_dir: Path,
    multi_year_dir: Path,
    warehouse_root: Path | None,
    runtime_bridge_dir: Path | None,
    operator_dashboard_dir: Path | None,
    recent_session_count: int,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    resolved_output_dir = (
        output_dir.resolve()
        if output_dir is not None
        else (DEFAULT_OUTPUT_ROOT / datetime.now(UTC).strftime("%Y%m%d_%H%M%S")).resolve()
    )
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    result = run_cross_asset_trade_mapping(
        output_dir=resolved_output_dir,
        cross_asset_output_dir=cross_asset_dir.resolve(),
        multi_year_output_dir=multi_year_dir.resolve(),
        warehouse_root=warehouse_root.resolve() if warehouse_root is not None else None,
        runtime_bridge_dir=runtime_bridge_dir.resolve() if runtime_bridge_dir is not None else None,
        operator_dashboard_dir=operator_dashboard_dir.resolve() if operator_dashboard_dir is not None else None,
        recent_session_count=recent_session_count,
    )
    artifacts = result["artifacts"]
    return {
        "output_dir": str(resolved_output_dir),
        "summary_json_path": artifacts["summary_json_path"],
        "summary_md_path": artifacts["summary_md_path"],
        "mapped_trades_path": artifacts["mapped_trades_path"],
        "tier_summary_json_path": artifacts["tier_summary_json_path"],
        "counterfactual_json_path": artifacts["counterfactual_json_path"],
        "recent_signal_json_path": artifacts["recent_signal_json_path"],
        "inventory_path": artifacts["inventory_path"],
        "insufficient_json_path": artifacts["insufficient_json_path"],
        "recommendation": result["payload"]["recommendation"],
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_asia_drift_v1_cross_asset_trade_mapping(
        cross_asset_dir=Path(args.cross_asset_dir),
        multi_year_dir=Path(args.multi_year_dir),
        warehouse_root=Path(args.warehouse_root) if args.warehouse_root else None,
        runtime_bridge_dir=Path(args.runtime_bridge_dir) if args.runtime_bridge_dir else None,
        operator_dashboard_dir=Path(args.operator_dashboard_dir) if args.operator_dashboard_dir else None,
        recent_session_count=args.recent_session_count,
        output_dir=Path(args.output_dir) if args.output_dir else None,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
