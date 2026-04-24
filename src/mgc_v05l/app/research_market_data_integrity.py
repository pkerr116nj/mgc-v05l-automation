"""CLI runner for research-grade market-data integrity auditing and maintenance."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from ..config_models import load_settings_from_files
from ..market_data.research_data_integrity import (
    execute_research_market_data_backfill,
    rebuild_canonical_warehouse_surfaces,
    rematerialize_trade_artifacts,
    run_research_data_integrity_audit,
)
from ..market_data.provider_config import load_market_data_providers_config

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_WAREHOUSE_ROOT = REPO_ROOT / "outputs" / "research_platform" / "warehouse" / "historical_evaluator"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "research_market_data_integrity"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="research-market-data-integrity")
    parser.add_argument(
        "--mode",
        choices=("audit", "backfill", "warehouse-rebuild", "trade-rematerialize"),
        default="audit",
        help="Run dry-run audit, execute canonical backfill, rebuild warehouse surfaces, or re-materialize trade artifacts.",
    )
    parser.add_argument("--replay-db", default=None, help="Optional replay SQLite path override.")
    parser.add_argument("--warehouse-root", default=str(DEFAULT_WAREHOUSE_ROOT), help="Warehouse historical evaluator root.")
    parser.add_argument("--provider-config", default=None, help="Optional provider config override.")
    parser.add_argument("--output-dir", default=None, help="Optional report output directory.")
    parser.add_argument(
        "--config",
        action="append",
        default=None,
        help="Settings config path. Defaults to config/base.yaml + config/replay.yaml.",
    )
    parser.add_argument("--start-date", default="2024-01-01", help="Start date for backfill planning.")
    parser.add_argument("--start", default=None, help="Explicit ISO start timestamp for execution modes.")
    parser.add_argument("--end", default=None, help="Explicit ISO end timestamp for audit/maintenance execution modes.")
    parser.add_argument("--symbol", action="append", default=None, help="Optional symbol override for execution modes.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = load_settings_from_files(args.config or [Path("config/base.yaml"), Path("config/replay.yaml")])
    replay_db_path = Path(args.replay_db) if args.replay_db else Path(str(settings.database_url).removeprefix("sqlite:///"))
    warehouse_root = Path(args.warehouse_root)
    end_ts = _parse_timestamp(args.end) if args.end else None
    provider_cfg = load_market_data_providers_config(args.provider_config)
    symbols = [str(item).strip().upper() for item in (args.symbol or provider_cfg.databento.pilot_symbols.keys())]

    if args.mode == "audit":
        output_dir = Path(args.output_dir) if args.output_dir else DEFAULT_OUTPUT_ROOT / _output_stamp(end_ts=end_ts)
        result = run_research_data_integrity_audit(
            output_dir=output_dir,
            replay_db_path=replay_db_path,
            warehouse_root=warehouse_root,
            settings=settings,
            provider_config=args.provider_config,
            start_date=args.start_date,
            end_timestamp=end_ts,
        )
        print(json.dumps(_json_ready(result), indent=2, sort_keys=True))
        return 0

    if args.mode == "backfill":
        if args.start is None or args.end is None:
            raise SystemExit("--mode backfill requires --start and --end.")
        result = execute_research_market_data_backfill(
            replay_db_path=replay_db_path,
            provider_config=args.provider_config,
            config_paths=args.config or [Path("config/base.yaml"), Path("config/replay.yaml")],
            symbols=symbols,
            start_ts=_parse_timestamp(args.start),
            end_ts=_parse_timestamp(args.end),
        )
        print(json.dumps(_json_ready(result), indent=2, sort_keys=True))
        return 0

    if args.mode == "warehouse-rebuild":
        if args.end is None:
            raise SystemExit("--mode warehouse-rebuild requires --end.")
        start_ts = _parse_timestamp(args.start) if args.start else datetime.fromisoformat("2024-01-01T18:00:00-05:00")
        result = rebuild_canonical_warehouse_surfaces(
            warehouse_root=warehouse_root,
            replay_db_path=replay_db_path,
            instruments=symbols,
            start_ts=start_ts,
            end_ts=_parse_timestamp(args.end),
        )
        print(json.dumps(_json_ready(result), indent=2, sort_keys=True))
        return 0

    if args.mode == "trade-rematerialize":
        if args.end is None:
            raise SystemExit("--mode trade-rematerialize requires --end.")
        start_ts = _parse_timestamp(args.start) if args.start else datetime.fromisoformat("2024-01-01T18:00:00-05:00")
        result = rematerialize_trade_artifacts(
            warehouse_root=warehouse_root,
            replay_db_path=replay_db_path,
            start_ts=start_ts,
            end_ts=_parse_timestamp(args.end),
        )
        print(json.dumps(_json_ready(result), indent=2, sort_keys=True))
        return 0

    raise SystemExit(f"Unsupported mode: {args.mode}")


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _output_stamp(*, end_ts: datetime | None) -> str:
    if end_ts is None:
        return "latest"
    return f"full_20240101_{end_ts.strftime('%Y%m%d')}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


if __name__ == "__main__":
    raise SystemExit(main())
