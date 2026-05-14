"""Forced-session research sweep for stock-index futures using gold-aligned session windows."""

from __future__ import annotations

import argparse
import bisect
import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ..config_models import load_settings_from_files
from ..research.trend_participation.models import ResearchBar
from ..research.trend_participation.storage import load_sqlite_bars, normalize_and_check_bars, resample_bars_from_1m
from .gc_mgc_segment_forced_session_long_research import (
    _evaluate_forced_session as _evaluate_forced_long_session,
)
from .gc_mgc_segment_forced_session_long_research import (
    _summarize_trades as _summarize_long_trades,
)
from .gc_mgc_segment_forced_session_long_research import build_variant_specs as build_long_variant_specs
from .gc_mgc_segment_forced_session_short_research import (
    _evaluate_forced_session as _evaluate_forced_short_session,
)
from .gc_mgc_segment_forced_session_short_research import (
    _summarize_trades as _summarize_short_trades,
)
from .gc_mgc_segment_forced_session_short_research import build_variant_specs as build_short_variant_specs
from .gc_mgc_segment_regime_research import (
    DEFAULT_CONFIG_PATHS,
    _parse_date,
    _resolve_sqlite_database_path,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "index_futures_forced_session_research"
DEFAULT_SYMBOLS: tuple[str, ...] = ("ES", "MES", "NQ", "MNQ")
NEW_YORK = ZoneInfo("America/New_York")
SESSION_RESET_TIME = time(18, 0)
VALID_SEGMENTS: tuple[str, ...] = (
    "SESSION_OPEN",
    "ASIA_EARLY",
    "ASIA_LATE",
    "LONDON_EARLY",
    "LONDON_LATE",
    "US_EARLY",
    "US_MIDDAY",
    "US_LATE",
)
VALID_SIDES: tuple[str, ...] = ("LONG", "SHORT")
DEFAULT_LONG_VARIANT_IDS: tuple[str, ...] = (
    "segment_forced_long_v1_breakout_or_bar6",
    "segment_forced_long_v2_dip_reclaim_or_bar7",
    "segment_forced_long_v4_breakout_or_bar7",
    "segment_forced_long_v5_dip_reclaim_or_bar8",
    "segment_forced_long_v6_contextual_fallback",
)
DEFAULT_SHORT_VARIANT_IDS: tuple[str, ...] = (
    "segment_forced_short_v1_breakdown_or_bar6",
    "segment_forced_short_v2_reclaim_fail_or_bar7",
    "segment_forced_short_v4_breakdown_or_bar7",
    "segment_forced_short_v5_contextual_breakdown",
)


@dataclass(frozen=True)
class SegmentSessionContext:
    trade_date: date
    segment_bars: list[ResearchBar]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="index-futures-forced-session-research")
    parser.add_argument("--symbol", action="append", default=None, help="Symbol to evaluate. Defaults to ES/MES/NQ/MNQ.")
    parser.add_argument("--segment-id", action="append", default=None, help="Optional segment filter.")
    parser.add_argument("--side", action="append", default=None, help="Optional side filter: LONG and/or SHORT.")
    parser.add_argument("--start-date", default=None, help="Optional inclusive trade-date filter.")
    parser.add_argument("--end-date", default=None, help="Optional inclusive trade-date filter.")
    parser.add_argument("--output-dir", default=None, help="Optional output directory override.")
    parser.add_argument("--config", action="append", default=None, help="Config path, may be supplied multiple times.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_index_futures_forced_session_research(
        symbols=args.symbol or list(DEFAULT_SYMBOLS),
        segment_ids=args.segment_id,
        sides=args.side,
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        config_paths=args.config,
    )
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


def run_index_futures_forced_session_research(
    *,
    symbols: list[str] | tuple[str, ...] | None = None,
    segment_ids: list[str] | tuple[str, ...] | None = None,
    sides: list[str] | tuple[str, ...] | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    output_dir: str | Path | None = None,
    config_paths: list[str] | list[Path] | tuple[str | Path, ...] | None = None,
) -> dict[str, Any]:
    resolved_symbols = tuple(sorted({str(symbol).strip().upper() for symbol in (symbols or DEFAULT_SYMBOLS) if str(symbol).strip()}))
    resolved_segments = tuple(
        segment
        for segment in VALID_SEGMENTS
        if not segment_ids or segment in {str(item).strip().upper() for item in segment_ids if str(item).strip()}
    )
    requested_sides = {str(item).strip().upper() for item in (sides or VALID_SIDES) if str(item).strip()}
    resolved_sides = tuple(side for side in VALID_SIDES if side in requested_sides)
    if not resolved_segments:
        raise ValueError("No valid segment_ids selected.")
    if not resolved_sides:
        raise ValueError("No valid sides selected.")

    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    settings = load_settings_from_files([Path(path) for path in (config_paths or DEFAULT_CONFIG_PATHS)])
    sqlite_path = _resolve_sqlite_database_path(settings.database_url)
    start_day = _parse_date(start_date)
    end_day = _parse_date(end_date)

    long_specs = tuple(spec for spec in build_long_variant_specs(selected_ids=list(DEFAULT_LONG_VARIANT_IDS)) if spec.decision_timeframe == "3m")
    short_specs = tuple(spec for spec in build_short_variant_specs(selected_ids=list(DEFAULT_SHORT_VARIANT_IDS)) if spec.decision_timeframe == "3m")

    symbol_reports: dict[str, Any] = {}
    for symbol in resolved_symbols:
        one_minute = load_sqlite_bars(
            sqlite_path=sqlite_path,
            instrument=symbol,
            timeframe="1m",
            data_source="historical_1m_canonical",
            start_ts=_load_start_timestamp(start_day),
            end_ts=_load_end_timestamp(end_day),
        )
        normalized_1m, quality_issues = normalize_and_check_bars(bars=one_minute, timeframe="1m")
        bars_3m = resample_bars_from_1m(bars_1m=normalized_1m, target_timeframe="3m")
        segment_contexts = {
            segment_id: _build_segment_session_contexts(
                decision_bars=bars_3m,
                segment_id=segment_id,
                start_day=start_day,
                end_day=end_day,
            )
            for segment_id in resolved_segments
        }
        variant_reports: dict[str, Any] = {}
        for segment_id in resolved_segments:
            if "LONG" in resolved_sides:
                for spec in long_specs:
                    variant_id = f"{segment_id}__LONG__{spec.variant_id}"
                    sessions = [
                        _evaluate_forced_long_session(
                            symbol=symbol,
                            trade_day=context.trade_date,
                            segment_id=segment_id,
                            segment_bars=context.segment_bars,
                            spec=spec,
                        )
                        for context in segment_contexts[segment_id]
                    ]
                    variant_reports[variant_id] = {
                        "segment_id": segment_id,
                        "side": "LONG",
                        "description": spec.description,
                        "trade_summary": _summarize_long_trades(sessions),
                        "sessions": [asdict(row) for row in sessions],
                    }
            if "SHORT" in resolved_sides:
                for spec in short_specs:
                    variant_id = f"{segment_id}__SHORT__{spec.variant_id}"
                    sessions = [
                        _evaluate_forced_short_session(
                            symbol=symbol,
                            trade_day=context.trade_date,
                            segment_id=segment_id,
                            segment_bars=context.segment_bars,
                            spec=spec,
                        )
                        for context in segment_contexts[segment_id]
                    ]
                    variant_reports[variant_id] = {
                        "segment_id": segment_id,
                        "side": "SHORT",
                        "description": spec.description,
                        "trade_summary": _summarize_short_trades(sessions),
                        "sessions": [asdict(row) for row in sessions],
                    }
        symbol_reports[symbol] = {
            "bar_count_1m": len(normalized_1m),
            "bar_count_3m": len(bars_3m),
            "data_quality_issues": [asdict(issue) for issue in quality_issues],
            "variants": variant_reports,
        }

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study_id": "index_futures_forced_session_research",
        "definition": {
            "symbols": list(resolved_symbols),
            "segments": list(resolved_segments),
            "sides": list(resolved_sides),
            "session_reset_time_et": "18:00",
            "session_windows_et": {
                "SESSION_OPEN": "18:00-19:00",
                "ASIA_EARLY": "19:00-20:30",
                "ASIA_LATE": "20:30-03:00",
                "LONDON_EARLY": "03:00-05:30",
                "LONDON_LATE": "05:30-08:20",
                "US_EARLY": "08:20-11:00",
                "US_MIDDAY": "11:00-13:30",
                "US_LATE": "13:30-16:00",
            },
            "common_window_note": "For apples-to-apples ES/MES/NQ/MNQ comparison, use the common coverage window shared by the selected symbols.",
            "start_date": start_day.isoformat() if start_day else None,
            "end_date": end_day.isoformat() if end_day else None,
        },
        "database_path": str(sqlite_path),
        "symbol_reports": symbol_reports,
        "pair_rankings": _pair_rankings(symbol_reports=symbol_reports),
        "all_symbol_ranking": _all_symbol_ranking(symbol_reports=symbol_reports),
    }
    json_path = resolved_output_dir / "index_futures_forced_session_research.json"
    markdown_path = resolved_output_dir / "index_futures_forced_session_research.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "index_futures_forced_session_research",
        "artifact_paths": {"json": str(json_path), "markdown": str(markdown_path)},
        "pair_rankings": payload["pair_rankings"],
        "all_symbol_ranking": payload["all_symbol_ranking"][:10],
    }


def stock_index_trade_date_for_timestamp(timestamp: datetime) -> date:
    local_dt = timestamp.astimezone(NEW_YORK)
    if local_dt.timetz().replace(tzinfo=None) >= SESSION_RESET_TIME:
        return local_dt.date() + timedelta(days=1)
    return local_dt.date()


def label_stock_index_segment(timestamp: datetime) -> str | None:
    local_dt = timestamp.astimezone(NEW_YORK)
    local_time = local_dt.timetz().replace(tzinfo=None)
    if time(18, 0) < local_time < time(19, 0):
        return "SESSION_OPEN"
    if time(19, 0) <= local_time < time(20, 30):
        return "ASIA_EARLY"
    if time(20, 30) <= local_time or local_time < time(3, 0):
        return "ASIA_LATE"
    if time(3, 0) <= local_time < time(5, 30):
        return "LONDON_EARLY"
    if time(5, 30) <= local_time < time(8, 20):
        return "LONDON_LATE"
    if time(8, 20) <= local_time < time(11, 0):
        return "US_EARLY"
    if time(11, 0) <= local_time < time(13, 30):
        return "US_MIDDAY"
    if time(13, 30) <= local_time < time(16, 0):
        return "US_LATE"
    return None


def _build_segment_session_contexts(
    *,
    decision_bars: list[ResearchBar],
    segment_id: str,
    start_day: date | None,
    end_day: date | None,
) -> list[SegmentSessionContext]:
    grouped: dict[date, list[ResearchBar]] = defaultdict(list)
    local_bars = [(bar.end_ts.astimezone(NEW_YORK), bar) for bar in decision_bars]
    local_times = [local_dt for local_dt, _ in local_bars]
    for trade_day in sorted({stock_index_trade_date_for_timestamp(local_dt) for local_dt, _ in local_bars}):
        if start_day is not None and trade_day < start_day:
            continue
        if end_day is not None and trade_day > end_day:
            continue
        start_dt, end_dt = _segment_window_for_trade_date(trade_day=trade_day, segment_id=segment_id)
        left = bisect.bisect_left(local_times, start_dt)
        right = bisect.bisect_left(local_times, end_dt)
        bars = [bar for _, bar in local_bars[left:right] if label_stock_index_segment(bar.end_ts) == segment_id]
        if bars:
            grouped[trade_day].extend(bars)
    return [SegmentSessionContext(trade_date=day, segment_bars=grouped[day]) for day in sorted(grouped)]


def _segment_window_for_trade_date(*, trade_day: date, segment_id: str) -> tuple[datetime, datetime]:
    windows = {
        "SESSION_OPEN": (time(18, 0), time(19, 0)),
        "ASIA_EARLY": (time(19, 0), time(20, 30)),
        "ASIA_LATE": (time(20, 30), time(3, 0)),
        "LONDON_EARLY": (time(3, 0), time(5, 30)),
        "LONDON_LATE": (time(5, 30), time(8, 20)),
        "US_EARLY": (time(8, 20), time(11, 0)),
        "NY_EARLY": (time(8, 20), time(11, 0)),
        "US_MIDDAY": (time(11, 0), time(13, 30)),
        "US_LATE": (time(13, 30), time(16, 0)),
    }
    start_time, end_time = windows[segment_id]
    base_day = trade_day - timedelta(days=1) if start_time >= SESSION_RESET_TIME else trade_day
    start_dt = datetime.combine(base_day, start_time, tzinfo=NEW_YORK)
    end_dt = datetime.combine(base_day, end_time, tzinfo=NEW_YORK)
    if end_time <= start_time:
        end_dt += timedelta(days=1)
    return start_dt, end_dt


def _pair_rankings(*, symbol_reports: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    pair_map = {
        "ES_MES": ("ES", "MES"),
        "NQ_MNQ": ("NQ", "MNQ"),
    }
    rankings: dict[str, list[dict[str, Any]]] = {}
    for pair_id, symbols in pair_map.items():
        shared_variants = set(symbol_reports.get(symbols[0], {}).get("variants", {}).keys())
        for symbol in symbols[1:]:
            shared_variants &= set(symbol_reports.get(symbol, {}).get("variants", {}).keys())
        rows: list[dict[str, Any]] = []
        for variant_id in sorted(shared_variants):
            variant_payloads = [symbol_reports[symbol]["variants"][variant_id] for symbol in symbols]
            summaries = [payload["trade_summary"] for payload in variant_payloads]
            rows.append(
                {
                    "variant_id": variant_id,
                    "segment_id": variant_payloads[0]["segment_id"],
                    "side": variant_payloads[0]["side"],
                    "description": variant_payloads[0]["description"],
                    "symbols": list(symbols),
                    "min_average_net_pnl_points": min(float(summary.get("average_net_pnl_points") or 0.0) for summary in summaries),
                    "min_net_profit_factor": min(float(summary.get("net_profit_factor") or 0.0) for summary in summaries),
                    "min_entered_trade_count": min(int(summary.get("entered_trade_count") or 0) for summary in summaries),
                    "symbol_metrics": {
                        symbol: {
                            "entered_trade_count": summaries[index].get("entered_trade_count"),
                            "average_net_pnl_points": summaries[index].get("average_net_pnl_points"),
                            "net_profit_factor": summaries[index].get("net_profit_factor"),
                        }
                        for index, symbol in enumerate(symbols)
                    },
                }
            )
        rows.sort(
            key=lambda row: (
                float(row["min_average_net_pnl_points"]),
                float(row["min_net_profit_factor"]),
                int(row["min_entered_trade_count"]),
            ),
            reverse=True,
        )
        rankings[pair_id] = rows
    return rankings


def _all_symbol_ranking(*, symbol_reports: dict[str, Any]) -> list[dict[str, Any]]:
    symbols = tuple(symbol_reports.keys())
    if not symbols:
        return []
    shared_variants = set(symbol_reports[symbols[0]].get("variants", {}).keys())
    for symbol in symbols[1:]:
        shared_variants &= set(symbol_reports[symbol].get("variants", {}).keys())
    rows: list[dict[str, Any]] = []
    for variant_id in sorted(shared_variants):
        variant_payloads = [symbol_reports[symbol]["variants"][variant_id] for symbol in symbols]
        summaries = [payload["trade_summary"] for payload in variant_payloads]
        rows.append(
            {
                "variant_id": variant_id,
                "segment_id": variant_payloads[0]["segment_id"],
                "side": variant_payloads[0]["side"],
                "description": variant_payloads[0]["description"],
                "symbols": list(symbols),
                "min_average_net_pnl_points": min(float(summary.get("average_net_pnl_points") or 0.0) for summary in summaries),
                "min_net_profit_factor": min(float(summary.get("net_profit_factor") or 0.0) for summary in summaries),
                "min_entered_trade_count": min(int(summary.get("entered_trade_count") or 0) for summary in summaries),
                "symbol_metrics": {
                    symbol: {
                        "entered_trade_count": summaries[index].get("entered_trade_count"),
                        "average_net_pnl_points": summaries[index].get("average_net_pnl_points"),
                        "net_profit_factor": summaries[index].get("net_profit_factor"),
                    }
                    for index, symbol in enumerate(symbols)
                },
            }
        )
    rows.sort(
        key=lambda row: (
            float(row["min_average_net_pnl_points"]),
            float(row["min_net_profit_factor"]),
            int(row["min_entered_trade_count"]),
        ),
        reverse=True,
    )
    return rows


def _load_start_timestamp(start_day: date | None) -> datetime | None:
    if start_day is None:
        return None
    return datetime.combine(start_day - timedelta(days=1), SESSION_RESET_TIME, tzinfo=NEW_YORK)


def _load_end_timestamp(end_day: date | None) -> datetime | None:
    if end_day is None:
        return None
    return datetime.combine(end_day, time(23, 59), tzinfo=NEW_YORK)


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Index Futures Forced Session Research",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Database: `{payload['database_path']}`",
        f"- Symbols: `{', '.join(payload['definition']['symbols'])}`",
        f"- Window: `{payload['definition']['start_date']} -> {payload['definition']['end_date']}`",
        "",
        "## Pair Rankings",
        "",
    ]
    for pair_id, rows in payload["pair_rankings"].items():
        lines.append(f"### {pair_id}")
        lines.append("")
        for row in rows[:5]:
            lines.append(
                f"- `{row['variant_id']}`: min avg net `{row['min_average_net_pnl_points']}`, min PF `{row['min_net_profit_factor']}`, min trades `{row['min_entered_trade_count']}`"
            )
        lines.append("")
    lines.append("## All-Symbol Ranking")
    lines.append("")
    for row in payload["all_symbol_ranking"][:10]:
        lines.append(
            f"- `{row['variant_id']}`: min avg net `{row['min_average_net_pnl_points']}`, min PF `{row['min_net_profit_factor']}`, min trades `{row['min_entered_trade_count']}`"
        )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
