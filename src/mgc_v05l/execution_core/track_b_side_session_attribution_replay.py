"""Read-only side/session attribution replay for Track B PAPER trades.

The replay consumes strategy-performance canonical trade records and retained
Phase-1 candle artifacts. It writes analytics-only attribution reports and a
forward candle-path capture snapshot. It does not import broker/runtime modules,
does not mutate lifecycle or Managed Exit state, and is not trade authority.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.bounded_jsonl import BoundedJsonlConfig, write_bounded_jsonl


DEFAULT_PERFORMANCE_ROOT = Path("outputs") / "track_b_execution_core" / "strategy_performance"
DEFAULT_CANONICAL_TRADES = DEFAULT_PERFORMANCE_ROOT / "canonical_trade_records.jsonl"
DEFAULT_PHASE1_ROOT = Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data"
DEFAULT_OUTPUT_DIR = DEFAULT_PERFORMANCE_ROOT / "side_session_attribution"
DEFAULT_SUMMARY_PATH = DEFAULT_OUTPUT_DIR / "latest_side_session_attribution_summary.json"
DEFAULT_REPORT_PATH = DEFAULT_OUTPUT_DIR / "side_session_attribution_report.md"
DEFAULT_TRADE_REPLAY_PATH = DEFAULT_OUTPUT_DIR / "side_session_trade_replay.jsonl"
DEFAULT_FORWARD_CAPTURE_PATH = DEFAULT_OUTPUT_DIR / "forward_path_capture.jsonl"
SCHEMA_VERSION = "track_b_side_session_attribution_replay_v1"
HORIZONS_MINUTES = (5, 10, 15, 30, 60)
PATH_CAPTURE_JSONL_CONFIG = BoundedJsonlConfig(
    max_row_bytes=256 * 1024,
    max_file_bytes=16 * 1024 * 1024,
    max_depth=6,
    max_items=256,
    max_string_chars=4096,
)


@dataclass(frozen=True)
class SideSessionAttributionResult:
    summary_path: Path
    report_path: Path
    trade_replay_path: Path
    forward_capture_path: Path
    summary: dict[str, Any]


def build_side_session_attribution_replay(
    *,
    repo_root: Path | str = Path("."),
    canonical_trades_path: Path | str = DEFAULT_CANONICAL_TRADES,
    phase1_root: Path | str = DEFAULT_PHASE1_ROOT,
    output_dir: Path | str = DEFAULT_OUTPUT_DIR,
    write_artifacts: bool = True,
    now: datetime | None = None,
) -> SideSessionAttributionResult:
    root = Path(repo_root)
    generated_at = _ensure_utc(now or datetime.now(UTC))
    canonical = _resolve(root, Path(canonical_trades_path))
    phase1 = _resolve(root, Path(phase1_root))
    output = _resolve(root, Path(output_dir))
    trades = [
        row
        for row in _read_jsonl(canonical)
        if row.get("event_type") == "CANONICAL_TRADE_RECORD"
        and row.get("pairing_status") == "PAIRED"
        and row.get("trade_status") == "CLOSED"
    ]
    replay_rows = [
        _replay_trade(row, phase1_root=phase1, generated_at=generated_at)
        for row in trades
    ]
    summary = _build_summary(replay_rows=replay_rows, generated_at=generated_at, canonical_path=canonical)
    forward_capture = _build_forward_capture(
        replay_rows=replay_rows,
        phase1_root=phase1,
        generated_at=generated_at,
    )
    summary_path = output / "latest_side_session_attribution_summary.json"
    report_path = output / "side_session_attribution_report.md"
    trade_replay_path = output / "side_session_trade_replay.jsonl"
    forward_capture_path = output / "forward_path_capture.jsonl"
    if write_artifacts:
        output.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        report_path.write_text(_markdown_report(summary), encoding="utf-8")
        write_bounded_jsonl(trade_replay_path, replay_rows, config=PATH_CAPTURE_JSONL_CONFIG)
        write_bounded_jsonl(forward_capture_path, forward_capture, config=PATH_CAPTURE_JSONL_CONFIG)
    return SideSessionAttributionResult(
        summary_path=summary_path,
        report_path=report_path,
        trade_replay_path=trade_replay_path,
        forward_capture_path=forward_capture_path,
        summary=summary,
    )


def _replay_trade(row: Mapping[str, Any], *, phase1_root: Path, generated_at: datetime) -> dict[str, Any]:
    symbol = str(row.get("symbol") or "").strip().upper()
    entry_time = _parse_time(row.get("entry_time"))
    exit_time = _parse_time(row.get("exit_time"))
    entry_price = _decimal(row.get("entry_price"))
    exit_price = _decimal(row.get("exit_price"))
    side = str(row.get("side") or "").strip().upper()
    candles, candle_path = _load_trade_candles(phase1_root=phase1_root, symbol=symbol, start=entry_time, end=exit_time)
    path = _path_metrics(
        candles=candles,
        side=side,
        entry_price=entry_price,
        exit_price=exit_price,
        entry_time=entry_time,
        exit_time=exit_time,
    )
    current_exit_pnl = _pnl_points(side=side, entry_price=entry_price, exit_price=exit_price)
    horizons = {
        f"{minutes}m": _horizon_pnl(
            candles=candles,
            side=side,
            entry_price=entry_price,
            entry_time=entry_time,
            minutes=minutes,
        )
        for minutes in HORIZONS_MINUTES
    }
    available_horizon_values = [value for value in horizons.values() if value.get("available")]
    best_horizon = max(
        (value for value in available_horizon_values if value.get("pnl_points") is not None),
        key=lambda value: _decimal(value.get("pnl_points")) or Decimal("-999999999"),
        default=None,
    )
    mfe = _decimal(path.get("mfe_points"))
    profitable_before_exit = bool(mfe is not None and mfe > 0)
    gave_back = bool(
        profitable_before_exit
        and current_exit_pnl is not None
        and (current_exit_pnl <= 0 or current_exit_pnl < mfe * Decimal("0.25"))
    )
    best_pnl = _decimal(best_horizon.get("pnl_points")) if best_horizon else None
    current_vs_best = current_exit_pnl - best_pnl if current_exit_pnl is not None and best_pnl is not None else None
    return {
        "schema_version": SCHEMA_VERSION,
        "event_type": "SIDE_SESSION_ATTRIBUTION_TRADE_REPLAY",
        "generated_at": generated_at.isoformat(),
        "trade_id": row.get("trade_id"),
        "lifecycle_id": row.get("lifecycle_id"),
        "lane_id": row.get("lane_id"),
        "symbol": symbol,
        "local_symbol": row.get("local_symbol"),
        "session_label": row.get("session_label"),
        "side": side,
        "cohort": _cohort(row),
        "entry_time": row.get("entry_time"),
        "exit_time": row.get("exit_time"),
        "entry_price": _str_decimal(entry_price),
        "exit_price": _str_decimal(exit_price),
        "current_exit_pnl_points": _str_decimal(current_exit_pnl),
        "current_exit_pnl_currency": row.get("realized_pnl_currency"),
        "horizon_pnl": horizons,
        "best_fixed_horizon": best_horizon,
        "current_exit_vs_best_horizon_points": _str_decimal(current_vs_best),
        "current_exit_improved_vs_best_horizon": bool(current_vs_best is not None and current_vs_best > 0),
        "current_exit_worsened_vs_best_horizon": bool(current_vs_best is not None and current_vs_best < 0),
        "profitable_before_current_exit": profitable_before_exit,
        "profitable_before_exit_but_gave_back": gave_back,
        "entry_quality_classification": _entry_quality(path=path, current_exit_pnl=current_exit_pnl),
        "exit_policy_classification": _exit_quality(path=path, current_exit_pnl=current_exit_pnl, best_horizon=best_horizon),
        "candle_path_status": "AVAILABLE" if candles else "NO_PHASE1_CANDLE_COVERAGE",
        "candle_count": len(candles),
        "candle_source": str(candle_path) if candle_path else None,
        "entry_to_exit_path": _entry_to_exit_path(candles=candles, side=side, entry_price=entry_price),
        **path,
    }


def _path_metrics(
    *,
    candles: Sequence[Mapping[str, Any]],
    side: str,
    entry_price: Decimal | None,
    exit_price: Decimal | None,
    entry_time: datetime | None,
    exit_time: datetime | None,
) -> dict[str, Any]:
    if not candles or entry_price is None:
        return {
            "mfe_points": None,
            "mae_points": None,
            "time_to_mfe_seconds": None,
            "time_to_mae_seconds": None,
            "path_bar_count": len(candles),
        }
    mfe: Decimal | None = None
    mae: Decimal | None = None
    mfe_time: datetime | None = None
    mae_time: datetime | None = None
    for candle in candles:
        high = _decimal(candle.get("high"))
        low = _decimal(candle.get("low"))
        ts = _parse_time(candle.get("bar_end") or candle.get("timestamp"))
        if high is None or low is None:
            continue
        if side == "SHORT":
            favorable = entry_price - low
            adverse = entry_price - high
        else:
            favorable = high - entry_price
            adverse = low - entry_price
        if mfe is None or favorable > mfe:
            mfe = favorable
            mfe_time = ts
        if mae is None or adverse < mae:
            mae = adverse
            mae_time = ts
    return {
        "mfe_points": _str_decimal(mfe),
        "mae_points": _str_decimal(mae),
        "time_to_mfe_seconds": _seconds_between(entry_time, mfe_time),
        "time_to_mae_seconds": _seconds_between(entry_time, mae_time),
        "path_bar_count": len(candles),
        "exit_price_in_path": exit_price is not None and bool(candles),
    }


def _horizon_pnl(
    *,
    candles: Sequence[Mapping[str, Any]],
    side: str,
    entry_price: Decimal | None,
    entry_time: datetime | None,
    minutes: int,
) -> dict[str, Any]:
    if not candles or entry_price is None or entry_time is None:
        return {"available": False, "pnl_points": None, "price": None, "bar_end": None}
    target = entry_time + timedelta(minutes=minutes)
    selected: Mapping[str, Any] | None = None
    for candle in candles:
        ts = _parse_time(candle.get("bar_end") or candle.get("timestamp"))
        if ts and ts >= target:
            selected = candle
            break
    if selected is None:
        return {"available": False, "pnl_points": None, "price": None, "bar_end": None}
    price = _decimal(selected.get("close"))
    return {
        "available": price is not None,
        "pnl_points": _str_decimal(_pnl_points(side=side, entry_price=entry_price, exit_price=price)),
        "price": _str_decimal(price),
        "bar_end": selected.get("bar_end") or selected.get("timestamp"),
    }


def _entry_to_exit_path(
    *,
    candles: Sequence[Mapping[str, Any]],
    side: str,
    entry_price: Decimal | None,
) -> list[dict[str, Any]]:
    if not candles or entry_price is None:
        return []
    points: list[dict[str, Any]] = []
    for candle in candles:
        high = _decimal(candle.get("high"))
        low = _decimal(candle.get("low"))
        close = _decimal(candle.get("close"))
        if high is None or low is None:
            continue
        if side == "SHORT":
            favorable = entry_price - low
            adverse = entry_price - high
        else:
            favorable = high - entry_price
            adverse = low - entry_price
        points.append(
            {
                "bar_end": candle.get("bar_end") or candle.get("timestamp"),
                "high": _str_decimal(high),
                "low": _str_decimal(low),
                "close": _str_decimal(close),
                "close_pnl_points": _str_decimal(_pnl_points(side=side, entry_price=entry_price, exit_price=close)),
                "favorable_excursion_points": _str_decimal(favorable),
                "adverse_excursion_points": _str_decimal(adverse),
            }
        )
    return points


def _build_summary(*, replay_rows: Sequence[Mapping[str, Any]], generated_at: datetime, canonical_path: Path) -> dict[str, Any]:
    cohorts: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    lanes: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in replay_rows:
        cohorts[str(row.get("cohort") or "other")].append(row)
        lanes[str(row.get("lane_id") or "UNKNOWN")].append(row)
    lane_summary = {lane: _group_metrics(rows) for lane, rows in sorted(lanes.items())}
    entry_bad = [
        {"lane_id": lane, **metrics}
        for lane, metrics in lane_summary.items()
        if metrics["trade_count"] >= 3 and metrics["entry_bad_count"] >= max(2, metrics["trade_count"] // 2)
    ]
    exit_bad = [
        {"lane_id": lane, **metrics}
        for lane, metrics in lane_summary.items()
        if metrics["trade_count"] >= 3 and metrics["exit_policy_bad_count"] >= max(2, metrics["trade_count"] // 3)
    ]
    needs_more = [
        {"lane_id": lane, **metrics}
        for lane, metrics in lane_summary.items()
        if metrics["trade_count"] < 5 or metrics["path_available_count"] == 0
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "analytics_only": True,
        "broker_authority": False,
        "runtime_authority": False,
        "source_artifacts": {"canonical_trade_records": str(canonical_path)},
        "total_trades": len(replay_rows),
        "path_available_count": sum(1 for row in replay_rows if row.get("candle_path_status") == "AVAILABLE"),
        "path_missing_count": sum(1 for row in replay_rows if row.get("candle_path_status") != "AVAILABLE"),
        "cohorts": {cohort: _group_metrics(rows) for cohort, rows in sorted(cohorts.items())},
        "lanes": lane_summary,
        "lanes_where_entry_looks_bad": sorted(entry_bad, key=lambda row: (row["path_available_count"], row["realized_pnl_points"])),
        "lanes_where_exit_policy_looks_bad": sorted(exit_bad, key=lambda row: (row["exit_policy_bad_count"], row["realized_pnl_points"])),
        "lanes_needing_more_data": sorted(needs_more, key=lambda row: (row["path_available_count"], row["trade_count"])),
    }


def _group_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pnl = [_decimal(row.get("current_exit_pnl_points")) for row in rows]
    pnl = [item for item in pnl if item is not None]
    return {
        "trade_count": len(rows),
        "win_count": sum(1 for value in pnl if value > 0),
        "loss_count": sum(1 for value in pnl if value < 0),
        "realized_pnl_points": _str_decimal(sum(pnl, Decimal("0"))),
        "average_realized_pnl_points": _str_decimal(_avg(pnl)),
        "average_mfe_points": _str_decimal(_avg([_decimal(row.get("mfe_points")) for row in rows])),
        "average_mae_points": _str_decimal(_avg([_decimal(row.get("mae_points")) for row in rows])),
        "average_time_to_mfe_seconds": _avg_float([row.get("time_to_mfe_seconds") for row in rows]),
        "average_time_to_mae_seconds": _avg_float([row.get("time_to_mae_seconds") for row in rows]),
        "profitable_before_exit_count": sum(1 for row in rows if row.get("profitable_before_current_exit") is True),
        "gave_back_count": sum(1 for row in rows if row.get("profitable_before_exit_but_gave_back") is True),
        "current_exit_worsened_vs_best_horizon_count": sum(1 for row in rows if row.get("current_exit_worsened_vs_best_horizon") is True),
        "current_exit_improved_vs_best_horizon_count": sum(1 for row in rows if row.get("current_exit_improved_vs_best_horizon") is True),
        "entry_bad_count": sum(1 for row in rows if row.get("entry_quality_classification") == "ENTRY_LOOKS_BAD"),
        "exit_policy_bad_count": sum(1 for row in rows if row.get("exit_policy_classification") == "EXIT_POLICY_LOOKS_BAD"),
        "path_available_count": sum(1 for row in rows if row.get("candle_path_status") == "AVAILABLE"),
        "path_missing_count": sum(1 for row in rows if row.get("candle_path_status") != "AVAILABLE"),
        "horizon_availability": dict(
            Counter(
                horizon
                for row in rows
                for horizon, payload in (row.get("horizon_pnl") or {}).items()
                if isinstance(payload, Mapping) and payload.get("available")
            )
        ),
    }


def _build_forward_capture(
    *,
    replay_rows: Sequence[Mapping[str, Any]],
    phase1_root: Path,
    generated_at: datetime,
) -> list[dict[str, Any]]:
    symbols = sorted({str(row.get("symbol") or "").upper() for row in replay_rows if row.get("symbol")})
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        for timeframe in ("1m", "5m"):
            path = phase1_root / symbol / timeframe / "latest_runtime_candles.json"
            payload = _read_json(path)
            bars = payload.get("bars") if isinstance(payload, Mapping) else []
            if not isinstance(bars, list):
                bars = []
            rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "event_type": "FORWARD_PATH_CANDLE_CAPTURE",
                    "generated_at": generated_at.isoformat(),
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "source_artifact_path": str(path),
                    "bar_count": len(bars),
                    "first_bar_end": (bars[0].get("bar_end") if bars and isinstance(bars[0], Mapping) else None),
                    "latest_bar_end": (bars[-1].get("bar_end") if bars and isinstance(bars[-1], Mapping) else None),
                    "bars": bars[-240:],
                    "analytics_only": True,
                }
            )
    return rows


def _markdown_report(summary: Mapping[str, Any]) -> str:
    lines = [
        "# Side/Session Attribution Replay",
        "",
        f"Generated: {summary.get('generated_at')}",
        "",
        "## Coverage",
        "",
        _table(
            ["Metric", "Value"],
            [
                ["trades", summary.get("total_trades")],
                ["path available", summary.get("path_available_count")],
                ["path missing", summary.get("path_missing_count")],
            ],
        ),
        "",
        "## Cohorts",
        "",
        _table(
            ["Cohort", "Trades", "W/L", "P&L pts", "Avg MFE", "Avg MAE", "Gave Back", "Exit Bad", "Path"],
            [
                [
                    cohort,
                    metrics.get("trade_count"),
                    f"{metrics.get('win_count')}/{metrics.get('loss_count')}",
                    metrics.get("realized_pnl_points"),
                    metrics.get("average_mfe_points"),
                    metrics.get("average_mae_points"),
                    metrics.get("gave_back_count"),
                    metrics.get("exit_policy_bad_count"),
                    f"{metrics.get('path_available_count')}/{metrics.get('trade_count')}",
                ]
                for cohort, metrics in summary.get("cohorts", {}).items()
            ],
        ),
        "",
        "## Entry Looks Bad",
        "",
        _lane_table(summary.get("lanes_where_entry_looks_bad", [])[:20]),
        "",
        "## Exit Policy Looks Bad",
        "",
        _lane_table(summary.get("lanes_where_exit_policy_looks_bad", [])[:20]),
        "",
        "## Needs More Data",
        "",
        _lane_table(summary.get("lanes_needing_more_data", [])[:30]),
        "",
        "Notes: this report uses retained Phase-1 candles only. Missing paths are reported as data gaps, not inferred.",
    ]
    return "\n".join(lines) + "\n"


def _lane_table(rows: Sequence[Mapping[str, Any]]) -> str:
    return _table(
        ["Lane", "Trades", "W/L", "P&L pts", "EntryBad", "ExitBad", "Path"],
        [
            [
                row.get("lane_id"),
                row.get("trade_count"),
                f"{row.get('win_count')}/{row.get('loss_count')}",
                row.get("realized_pnl_points"),
                row.get("entry_bad_count"),
                row.get("exit_policy_bad_count"),
                f"{row.get('path_available_count')}/{row.get('trade_count')}",
            ]
            for row in rows
        ],
    )


def _table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    return "\n".join(
        [
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join(["---"] * len(headers)) + " |",
            *["| " + " | ".join(str(item) for item in row) + " |" for row in rows],
        ]
    )


def _entry_quality(*, path: Mapping[str, Any], current_exit_pnl: Decimal | None) -> str:
    mfe = _decimal(path.get("mfe_points"))
    mae = _decimal(path.get("mae_points"))
    if mfe is None or mae is None:
        return "ENTRY_PATH_UNKNOWN"
    if mfe <= 0 and current_exit_pnl is not None and current_exit_pnl <= 0:
        return "ENTRY_LOOKS_BAD"
    if abs(mae) > max(mfe * Decimal("2"), Decimal("0.01")) and current_exit_pnl is not None and current_exit_pnl <= 0:
        return "ENTRY_LOOKS_BAD"
    return "ENTRY_PATH_ACCEPTABLE"


def _exit_quality(*, path: Mapping[str, Any], current_exit_pnl: Decimal | None, best_horizon: Mapping[str, Any] | None) -> str:
    mfe = _decimal(path.get("mfe_points"))
    best = _decimal(best_horizon.get("pnl_points")) if best_horizon else None
    if mfe is None or current_exit_pnl is None:
        return "EXIT_PATH_UNKNOWN"
    if mfe > 0 and current_exit_pnl <= 0:
        return "EXIT_POLICY_LOOKS_BAD"
    if best is not None and best > 0 and current_exit_pnl < best * Decimal("0.5"):
        return "EXIT_POLICY_LOOKS_BAD"
    return "EXIT_PATH_ACCEPTABLE"


def _cohort(row: Mapping[str, Any]) -> str:
    session = str(row.get("session_label") or "").upper()
    side = str(row.get("side") or "").upper()
    if session == "LONDON_LATE" and side == "SHORT":
        return "london_late_shorts"
    if session == "US" and side == "SHORT":
        return "us_shorts"
    if session == "US" and side == "LONG":
        return "us_longs"
    if session == "GLOBEX":
        return "globex_longs_shorts"
    return "other"


def _load_trade_candles(*, phase1_root: Path, symbol: str, start: datetime | None, end: datetime | None) -> tuple[list[Mapping[str, Any]], Path | None]:
    if not symbol or start is None or end is None:
        return [], None
    path = phase1_root / symbol / "1m" / "latest_runtime_candles.json"
    payload = _read_json(path)
    bars = payload.get("bars") if isinstance(payload, Mapping) else []
    if not isinstance(bars, list):
        return [], path
    selected: list[Mapping[str, Any]] = []
    for bar in bars:
        if not isinstance(bar, Mapping):
            continue
        ts = _parse_time(bar.get("bar_end") or bar.get("timestamp"))
        if ts is None:
            continue
        if start <= ts <= end:
            selected.append(bar)
    return selected, path


def _read_jsonl(path: Path) -> list[Mapping[str, Any]]:
    if not path.exists():
        return []
    rows: list[Mapping[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, Mapping):
            rows.append(row)
    return rows


def _read_json(path: Path) -> Any:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _resolve(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else root / path


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _seconds_between(start: datetime | None, end: datetime | None) -> int | None:
    if start is None or end is None:
        return None
    return int((end - start).total_seconds())


def _decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _str_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value.normalize()) if value != value.to_integral() else str(value.quantize(Decimal("1")))


def _avg(values: Sequence[Decimal | None]) -> Decimal | None:
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return sum(clean, Decimal("0")) / Decimal(len(clean))


def _avg_float(values: Sequence[Any]) -> float | None:
    clean = [Decimal(str(value)) for value in values if value is not None]
    if not clean:
        return None
    return float(sum(clean, Decimal("0")) / Decimal(len(clean)))


def _pnl_points(*, side: str, entry_price: Decimal | None, exit_price: Decimal | None) -> Decimal | None:
    if entry_price is None or exit_price is None:
        return None
    if side == "SHORT":
        return entry_price - exit_price
    return exit_price - entry_price


def _main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", default=".")
    args = parser.parse_args()
    result = build_side_session_attribution_replay(repo_root=Path(args.repo_root))
    print(
        json.dumps(
            {
                "classification": "SIDE_SESSION_ATTRIBUTION_REPLAY_READY",
                "summary_path": str(result.summary_path),
                "report_path": str(result.report_path),
                "trade_replay_path": str(result.trade_replay_path),
                "forward_capture_path": str(result.forward_capture_path),
                "total_trades": result.summary.get("total_trades"),
                "path_available_count": result.summary.get("path_available_count"),
                "path_missing_count": result.summary.get("path_missing_count"),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main())
