"""CLI runner for research-grade market-data integrity auditing and maintenance."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from ..config_models import load_settings_from_files
from ..market_data.research_data_integrity import (
    PHASE_A_WAREHOUSE_DERIVED_TIMEFRAMES,
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
    parser.add_argument("--symbols", default=None, help="Optional comma-separated symbol scope.")
    parser.add_argument(
        "--with-gap-repair",
        action="store_true",
        help="For backfill mode, run gap repair after canonical 1m fetch. Disabled by default.",
    )
    parser.add_argument(
        "--derive-timeframe",
        action="append",
        default=None,
        help="For backfill mode, explicitly derive canonical higher timeframes after fetch (e.g. --derive-timeframe 5m).",
    )
    parser.add_argument(
        "--phase-timeout-seconds",
        type=float,
        default=20.0,
        help="Timeout guard for expensive audit phases such as duplicate/overlap scans.",
    )
    parser.add_argument(
        "--skip-warehouse-checks",
        action="store_true",
        help="For audit mode, validate replay/canonical fetch state only and skip warehouse/trade alignment checks.",
    )
    parser.add_argument(
        "--derived-timeframes",
        default=None,
        help="For warehouse-rebuild mode, optional comma-separated derived timeframe override. Defaults to the Phase A set.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = load_settings_from_files(args.config or [Path("config/base.yaml"), Path("config/replay.yaml")])
    replay_db_path = Path(args.replay_db) if args.replay_db else Path(str(settings.database_url).removeprefix("sqlite:///"))
    warehouse_root = Path(args.warehouse_root)
    end_ts = _parse_timestamp(args.end) if args.end else None
    provider_cfg = load_market_data_providers_config(args.provider_config)
    symbols = _resolve_symbols(args, provider_cfg=provider_cfg)

    if args.mode == "audit":
        output_dir = Path(args.output_dir) if args.output_dir else DEFAULT_OUTPUT_ROOT / _output_stamp(end_ts=end_ts, symbols=symbols)
        result = run_research_data_integrity_audit(
            output_dir=output_dir,
            replay_db_path=replay_db_path,
            warehouse_root=warehouse_root,
            settings=settings,
            provider_config=args.provider_config,
            start_date=args.start_date,
            end_timestamp=end_ts,
            symbols=symbols,
            phase_timeout_seconds=args.phase_timeout_seconds,
            skip_warehouse_checks=bool(args.skip_warehouse_checks),
            progress_callback=_stderr_progress_callback,
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
            run_gap_repair=bool(args.with_gap_repair),
            derive_timeframes=args.derive_timeframe or (),
            progress_callback=_stderr_backfill_callback,
        )
        print(json.dumps(_json_ready(result), indent=2, sort_keys=True))
        return 0

    if args.mode == "warehouse-rebuild":
        if args.end is None:
            raise SystemExit("--mode warehouse-rebuild requires --end.")
        start_ts = _parse_timestamp(args.start) if args.start else None
        derived_timeframes = _resolve_warehouse_derived_timeframes(args)
        result = rebuild_canonical_warehouse_surfaces(
            warehouse_root=warehouse_root,
            replay_db_path=replay_db_path,
            instruments=symbols,
            start_ts=start_ts,
            end_ts=_parse_timestamp(args.end),
            derived_timeframes=derived_timeframes,
            progress_callback=_stderr_warehouse_rebuild_callback,
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


def _output_stamp(*, end_ts: datetime | None, symbols: list[str] | None = None) -> str:
    symbol_suffix = ""
    if symbols:
        symbol_suffix = "_" + "_".join(symbol.lower() for symbol in symbols)
    if end_ts is None:
        return f"latest{symbol_suffix}"
    return f"full_20240101_{end_ts.strftime('%Y%m%d')}{symbol_suffix}"


def _resolve_symbols(args: argparse.Namespace, *, provider_cfg: Any) -> list[str]:
    resolved = [str(item).strip().upper() for item in (args.symbol or [])]
    if args.symbols:
        resolved.extend(str(item).strip().upper() for item in str(args.symbols).split(",") if str(item).strip())
    if resolved:
        return sorted(dict.fromkeys(resolved))
    return [str(item).strip().upper() for item in provider_cfg.databento.pilot_symbols.keys()]


def _resolve_warehouse_derived_timeframes(args: argparse.Namespace) -> list[str]:
    if not args.derived_timeframes:
        return list(PHASE_A_WAREHOUSE_DERIVED_TIMEFRAMES)
    requested = [str(item).strip().lower() for item in str(args.derived_timeframes).split(",") if str(item).strip()]
    allowed = set(PHASE_A_WAREHOUSE_DERIVED_TIMEFRAMES)
    invalid = [timeframe for timeframe in requested if timeframe not in allowed]
    if invalid:
        raise SystemExit(
            f"--derived-timeframes only supports the Phase A set: {', '.join(PHASE_A_WAREHOUSE_DERIVED_TIMEFRAMES)}. "
            f"Invalid: {', '.join(sorted(invalid))}"
        )
    return list(dict.fromkeys(requested))


def _stderr_progress_callback(event: dict[str, Any]) -> None:
    phase = event.get("phase")
    if event.get("event") == "start":
        detail = event.get("detail") or {}
        detail_suffix = f" detail={json.dumps(detail, sort_keys=True)}" if detail else ""
        print(f"[research-market-data-integrity] phase={phase} status=running{detail_suffix}", file=sys.stderr, flush=True)
        return
    status = event.get("status")
    duration = event.get("duration_seconds")
    reason = event.get("reason")
    reason_suffix = f" reason={reason}" if reason else ""
    print(
        f"[research-market-data-integrity] phase={phase} status={status} duration_seconds={duration}{reason_suffix}",
        file=sys.stderr,
        flush=True,
    )


def _stderr_backfill_callback(event: dict[str, Any]) -> None:
    symbol = event.get("symbol")
    label = event.get("label")
    status = event.get("status")
    detail = event.get("detail") or {}
    detail_suffix = f" detail={json.dumps(detail, sort_keys=True)}" if detail else ""
    print(
        f"[research-market-data-backfill] symbol={symbol} label={label} status={status}{detail_suffix}",
        file=sys.stderr,
        flush=True,
    )


def _stderr_warehouse_rebuild_callback(event: dict[str, Any]) -> None:
    symbol = event.get("symbol") or "-"
    timeframe = event.get("timeframe") or "-"
    shard_id = event.get("shard_id") or "-"
    label = event.get("label")
    status = event.get("status")
    detail = event.get("detail") or {}
    detail_suffix = f" detail={json.dumps(detail, sort_keys=True)}" if detail else ""
    print(
        f"[research-market-data-warehouse] symbol={symbol} timeframe={timeframe} shard={shard_id} label={label} status={status}{detail_suffix}",
        file=sys.stderr,
        flush=True,
    )


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
