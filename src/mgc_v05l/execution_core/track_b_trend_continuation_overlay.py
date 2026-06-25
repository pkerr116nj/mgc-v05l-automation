"""Shadow-only Trend Continuation Overlay for Track B PAPER trades.

This module reads persisted analytics artifacts and scores whether completed
active-participation trades displayed continuation behavior after entry. It is
analytics-only: no broker authority, no runtime authority, no Managed Exit
authority, and no strategy gating.
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
DEFAULT_SIDE_SESSION_REPLAY = DEFAULT_PERFORMANCE_ROOT / "side_session_attribution" / "side_session_trade_replay.jsonl"
DEFAULT_FORWARD_CAPTURE = DEFAULT_PERFORMANCE_ROOT / "side_session_attribution" / "forward_path_capture.jsonl"
DEFAULT_PHASE1_ROOT = Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data"
DEFAULT_OUTPUT_DIR = Path("outputs") / "track_b_execution_core" / "trend_continuation_overlay"
DEFAULT_SUMMARY_PATH = DEFAULT_OUTPUT_DIR / "latest_trend_continuation_overlay_summary.json"
DEFAULT_SCORES_PATH = DEFAULT_OUTPUT_DIR / "trend_continuation_trade_scores.jsonl"
DEFAULT_REPORT_PATH = DEFAULT_OUTPUT_DIR / "trend_continuation_overlay_report.md"

SCHEMA_VERSION = "track_b_trend_continuation_overlay_v1"
OUTPUT_STATES = (
    "CONTINUATION_CONFIRMED",
    "CONTINUATION_WEAK",
    "IMPULSE_ONLY",
    "REVERSAL_RISK",
    "CHOP_OR_NO_EDGE",
    "INSUFFICIENT_PATH_DATA",
)
HORIZONS_MINUTES = (5, 10, 15, 30, 60)
PATH_JSONL_CONFIG = BoundedJsonlConfig(
    max_row_bytes=256 * 1024,
    max_file_bytes=16 * 1024 * 1024,
    max_depth=6,
    max_items=256,
    max_string_chars=4096,
)


@dataclass(frozen=True)
class TrendContinuationOverlayResult:
    summary_path: Path
    scores_path: Path
    report_path: Path
    summary: dict[str, Any]


def build_trend_continuation_overlay(
    *,
    repo_root: Path | str = Path("."),
    canonical_trades_path: Path | str = DEFAULT_CANONICAL_TRADES,
    side_session_replay_path: Path | str = DEFAULT_SIDE_SESSION_REPLAY,
    forward_capture_path: Path | str = DEFAULT_FORWARD_CAPTURE,
    phase1_root: Path | str = DEFAULT_PHASE1_ROOT,
    output_dir: Path | str = DEFAULT_OUTPUT_DIR,
    write_artifacts: bool = True,
    now: datetime | None = None,
) -> TrendContinuationOverlayResult:
    root = Path(repo_root)
    generated_at = _ensure_utc(now or datetime.now(UTC))
    canonical = _resolve(root, Path(canonical_trades_path))
    side_replay = _resolve(root, Path(side_session_replay_path))
    forward_capture = _resolve(root, Path(forward_capture_path))
    phase1 = _resolve(root, Path(phase1_root))
    output = _resolve(root, Path(output_dir))

    side_rows = _side_replay_by_key(_read_jsonl(side_replay))
    capture_rows = _forward_capture_by_symbol(_read_jsonl(forward_capture))
    trades = [
        row
        for row in _read_jsonl(canonical)
        if row.get("event_type") == "CANONICAL_TRADE_RECORD"
        and row.get("pairing_status") == "PAIRED"
        and row.get("trade_status") == "CLOSED"
    ]
    scores = [
        _score_trade(
            trade,
            side_replay=side_rows.get(_trade_key(trade)),
            phase1_root=phase1,
            forward_capture=capture_rows,
            generated_at=generated_at,
        )
        for trade in trades
    ]
    summary = _build_summary(scores=scores, generated_at=generated_at, canonical_path=canonical)
    summary_path = output / "latest_trend_continuation_overlay_summary.json"
    scores_path = output / "trend_continuation_trade_scores.jsonl"
    report_path = output / "trend_continuation_overlay_report.md"
    if write_artifacts:
        output.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        write_bounded_jsonl(scores_path, scores, config=PATH_JSONL_CONFIG)
        report_path.write_text(_markdown_report(summary), encoding="utf-8")
    return TrendContinuationOverlayResult(
        summary_path=summary_path,
        scores_path=scores_path,
        report_path=report_path,
        summary=summary,
    )


def _score_trade(
    trade: Mapping[str, Any],
    *,
    side_replay: Mapping[str, Any] | None,
    phase1_root: Path,
    forward_capture: Mapping[tuple[str, str], Sequence[Mapping[str, Any]]],
    generated_at: datetime,
) -> dict[str, Any]:
    symbol = str(trade.get("symbol") or "").upper()
    side = str(trade.get("side") or "").upper()
    entry_time = _parse_time(trade.get("entry_time"))
    exit_time = _parse_time(trade.get("exit_time"))
    entry_price = _decimal(trade.get("entry_price"))
    exit_price = _decimal(trade.get("exit_price"))
    candles, candle_source = _trade_candles(
        trade,
        side_replay=side_replay,
        phase1_root=phase1_root,
        forward_capture=forward_capture,
    )
    horizons = _horizon_pnl(candles=candles, side=side, entry_price=entry_price, entry_time=entry_time)
    features = _continuation_features(
        candles=candles,
        side=side,
        entry_price=entry_price,
        exit_price=exit_price,
        entry_time=entry_time,
        side_replay=side_replay,
        horizons=horizons,
    )
    state = _classify_state(features)
    return {
        "schema_version": SCHEMA_VERSION,
        "event_type": "TREND_CONTINUATION_TRADE_SCORE",
        "generated_at": generated_at.isoformat(),
        "analytics_only": True,
        "broker_authority": False,
        "runtime_authority": False,
        "managed_exit_authority": False,
        "trade_id": trade.get("trade_id"),
        "lifecycle_id": trade.get("lifecycle_id"),
        "lane_id": trade.get("lane_id"),
        "strategy_id": trade.get("strategy_id"),
        "strategy_family": trade.get("strategy_family"),
        "variant_id": trade.get("variant_id"),
        "entry_thesis": trade.get("entry_thesis"),
        "session_label": trade.get("session_label"),
        "cohort": _cohort(trade),
        "symbol": symbol,
        "local_symbol": trade.get("local_symbol"),
        "side": side,
        "entry_time": trade.get("entry_time"),
        "exit_time": trade.get("exit_time"),
        "entry_price": _str_decimal(entry_price),
        "exit_price": _str_decimal(exit_price),
        "exit_policy": trade.get("exit_policy"),
        "realized_pnl_points": _str_decimal(_pnl_points(side=side, entry_price=entry_price, exit_price=exit_price)),
        "continuation_state": state,
        "continuation_score": _str_decimal(features.get("continuation_score")),
        "features": {key: _jsonable_decimal(value) for key, value in features.items() if key != "horizon_pnl"},
        "horizon_pnl": horizons,
        "path_status": "AVAILABLE" if candles else "INSUFFICIENT_PATH_DATA",
        "path_bar_count": len(candles),
        "path_source": candle_source,
        "source_refs": {
            "canonical_trade_records": str(DEFAULT_CANONICAL_TRADES),
            "side_session_replay": str(DEFAULT_SIDE_SESSION_REPLAY) if side_replay else None,
        },
    }


def _continuation_features(
    *,
    candles: Sequence[Mapping[str, Any]],
    side: str,
    entry_price: Decimal | None,
    exit_price: Decimal | None,
    entry_time: datetime | None,
    side_replay: Mapping[str, Any] | None,
    horizons: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    if len(candles) < 3 or entry_price is None:
        return {
            "continuation_score": Decimal("0"),
            "path_bar_count": len(candles),
            "insufficient_reason": "fewer_than_3_path_bars_or_missing_entry_price",
            "horizon_pnl": horizons,
        }

    closes = [_decimal(candle.get("close")) for candle in candles]
    highs = [_decimal(candle.get("high")) for candle in candles]
    lows = [_decimal(candle.get("low")) for candle in candles]
    ranges = [(high - low) for high, low in zip(highs, lows, strict=False) if high is not None and low is not None]
    close_moves = [
        _directional_delta(side=side, newer=closes[index], older=closes[index - 1])
        for index in range(1, len(closes))
        if closes[index] is not None and closes[index - 1] is not None
    ]
    directional_close_move = _directional_delta(side=side, newer=closes[-1], older=closes[0]) if closes[-1] is not None and closes[0] is not None else None
    persistence_ratio = _ratio([move > 0 for move in close_moves])
    structure_ratio = _structure_ratio(side=side, highs=highs, lows=lows)
    body_strength = _avg([_body_strength(candle) for candle in candles])
    close_location = _avg([_close_location(candle, side=side) for candle in candles])
    range_expansion = _range_expansion_ratio(ranges)
    vwap_alignment = _vwap_alignment(candles=candles, side=side)
    path_metrics = _path_metrics(candles=candles, side=side, entry_price=entry_price, entry_time=entry_time)
    current_pnl = _pnl_points(side=side, entry_price=entry_price, exit_price=exit_price)
    side_mfe = _decimal(side_replay.get("mfe_points")) if side_replay else None
    side_mae = _decimal(side_replay.get("mae_points")) if side_replay else None
    mfe = side_mfe if side_mfe is not None else path_metrics["mfe_points"]
    mae = side_mae if side_mae is not None else path_metrics["mae_points"]
    giveback = (mfe - current_pnl) if mfe is not None and current_pnl is not None else None
    giveback_ratio = giveback / mfe if giveback is not None and mfe and mfe > 0 else None
    best_horizon = _best_horizon(horizons)
    best_horizon_pnl = _decimal(best_horizon.get("pnl_points")) if best_horizon else None
    current_exit_vs_best = current_pnl - best_horizon_pnl if current_pnl is not None and best_horizon_pnl is not None else None
    score = _continuation_score(
        directional_close_move=directional_close_move,
        persistence_ratio=persistence_ratio,
        structure_ratio=structure_ratio,
        body_strength=body_strength,
        close_location=close_location,
        range_expansion=range_expansion,
        vwap_alignment=vwap_alignment,
        mfe=mfe,
        mae=mae,
        current_pnl=current_pnl,
        giveback_ratio=giveback_ratio,
    )
    return {
        "continuation_score": score,
        "path_bar_count": len(candles),
        "trend_direction_alignment": _alignment(directional_close_move),
        "directional_close_move_points": directional_close_move,
        "close_to_close_persistence_ratio": persistence_ratio,
        "structure_ratio": structure_ratio,
        "body_strength": body_strength,
        "close_location_score": close_location,
        "range_expansion_ratio": range_expansion,
        "vwap_anchor_alignment": vwap_alignment,
        "mfe_points": mfe,
        "mae_points": mae,
        "pullback_depth_points": abs(mae) if mae is not None else None,
        "time_to_mfe_seconds": _decimal(side_replay.get("time_to_mfe_seconds")) if side_replay and side_replay.get("time_to_mfe_seconds") is not None else path_metrics["time_to_mfe_seconds"],
        "time_to_mae_seconds": _decimal(side_replay.get("time_to_mae_seconds")) if side_replay and side_replay.get("time_to_mae_seconds") is not None else path_metrics["time_to_mae_seconds"],
        "giveback_from_mfe_to_exit_points": giveback,
        "giveback_from_mfe_to_exit_ratio": giveback_ratio,
        "current_exit_pnl_points": current_pnl,
        "best_fixed_horizon": best_horizon,
        "current_exit_vs_best_horizon_points": current_exit_vs_best,
        "current_exit_improved_vs_best_horizon": bool(current_exit_vs_best is not None and current_exit_vs_best > 0),
        "current_exit_worsened_vs_best_horizon": bool(current_exit_vs_best is not None and current_exit_vs_best < 0),
        "horizon_pnl": horizons,
    }


def _continuation_score(
    *,
    directional_close_move: Decimal | None,
    persistence_ratio: Decimal | None,
    structure_ratio: Decimal | None,
    body_strength: Decimal | None,
    close_location: Decimal | None,
    range_expansion: Decimal | None,
    vwap_alignment: Decimal | None,
    mfe: Decimal | None,
    mae: Decimal | None,
    current_pnl: Decimal | None,
    giveback_ratio: Decimal | None,
) -> Decimal:
    score = Decimal("0")
    if directional_close_move is not None and directional_close_move > 0:
        score += Decimal("2")
    if persistence_ratio is not None:
        if persistence_ratio >= Decimal("0.65"):
            score += Decimal("2")
        elif persistence_ratio >= Decimal("0.50"):
            score += Decimal("1")
    if structure_ratio is not None:
        if structure_ratio >= Decimal("0.60"):
            score += Decimal("1.5")
        elif structure_ratio >= Decimal("0.45"):
            score += Decimal("0.5")
    if body_strength is not None and body_strength >= Decimal("0.50"):
        score += Decimal("1")
    if close_location is not None and close_location >= Decimal("0.65"):
        score += Decimal("1")
    if range_expansion is not None and range_expansion >= Decimal("1.05"):
        score += Decimal("0.5")
    if vwap_alignment is not None and vwap_alignment >= Decimal("0.65"):
        score += Decimal("0.5")
    if mfe is not None and mfe > 0:
        score += Decimal("1")
    if mfe is not None and mae is not None and mfe > 0 and abs(mae) <= mfe:
        score += Decimal("0.5")
    if current_pnl is not None and current_pnl > 0:
        score += Decimal("0.5")
    if giveback_ratio is not None and giveback_ratio > Decimal("0.75"):
        score -= Decimal("1")
    return score


def _classify_state(features: Mapping[str, Any]) -> str:
    if int(features.get("path_bar_count") or 0) < 3:
        return "INSUFFICIENT_PATH_DATA"
    score = _decimal(features.get("continuation_score")) or Decimal("0")
    persistence = _decimal(features.get("close_to_close_persistence_ratio"))
    structure = _decimal(features.get("structure_ratio"))
    mfe = _decimal(features.get("mfe_points"))
    mae = _decimal(features.get("mae_points"))
    pnl = _decimal(features.get("current_exit_pnl_points"))
    giveback_ratio = _decimal(features.get("giveback_from_mfe_to_exit_ratio"))
    alignment = str(features.get("trend_direction_alignment") or "")

    if (
        mfe is not None
        and mae is not None
        and mfe > abs(mae) * Decimal("2")
        and giveback_ratio is not None
        and giveback_ratio >= Decimal("0.70")
    ):
        return "IMPULSE_ONLY"
    if alignment == "AGAINST_TRADE" and pnl is not None and pnl <= 0:
        return "REVERSAL_RISK"
    if mfe is not None and mae is not None and mfe <= 0 and abs(mae) > 0:
        return "REVERSAL_RISK"
    if score >= Decimal("6"):
        return "CONTINUATION_CONFIRMED"
    if score >= Decimal("4"):
        return "CONTINUATION_WEAK"
    if (persistence is not None and persistence < Decimal("0.45")) and (structure is not None and structure < Decimal("0.45")):
        return "CHOP_OR_NO_EDGE"
    return "CHOP_OR_NO_EDGE"


def _build_summary(*, scores: Sequence[Mapping[str, Any]], generated_at: datetime, canonical_path: Path) -> dict[str, Any]:
    cohorts: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    lanes: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    symbols: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in scores:
        cohorts[str(row.get("cohort") or "other")].append(row)
        lanes[str(row.get("lane_id") or "UNKNOWN")].append(row)
        symbols[str(row.get("symbol") or "UNKNOWN")].append(row)
    lane_summary = {lane: _group_metrics(rows) for lane, rows in sorted(lanes.items())}
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "classification": "TREND_CONTINUATION_OVERLAY_READY",
        "analytics_only": True,
        "broker_authority": False,
        "runtime_authority": False,
        "managed_exit_authority": False,
        "source_artifacts": {"canonical_trade_records": str(canonical_path)},
        "state_definitions": list(OUTPUT_STATES),
        "total_trades": len(scores),
        "path_available_count": sum(1 for row in scores if row.get("path_status") == "AVAILABLE"),
        "path_missing_count": sum(1 for row in scores if row.get("path_status") != "AVAILABLE"),
        "cohorts": {cohort: _group_metrics(rows) for cohort, rows in sorted(cohorts.items())},
        "lanes": lane_summary,
        "symbols": {symbol: _group_metrics(rows) for symbol, rows in sorted(symbols.items())},
        "gc_mgc_comparison": {symbol: _group_metrics(symbols.get(symbol, [])) for symbol in ("GC", "MGC")},
        "nq_short_side_behavior": _group_metrics(
            [row for row in scores if str(row.get("symbol")) == "NQ" and str(row.get("side")) == "SHORT"]
        ),
        "winners_vs_losers": {
            "winners": _group_metrics([row for row in scores if (_decimal(row.get("realized_pnl_points")) or Decimal("0")) > 0]),
            "losers": _group_metrics([row for row in scores if (_decimal(row.get("realized_pnl_points")) or Decimal("0")) < 0]),
        },
        "lanes_best_candidates_for_future_overlay_integration": _best_overlay_candidates(lane_summary),
        "lanes_where_continuation_fails": _continuation_failures(lane_summary),
        "lanes_needing_more_path_data": _needs_more_path(lane_summary),
        "research_questions": _research_questions(cohorts),
    }


def _group_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pnl = [_decimal(row.get("realized_pnl_points")) for row in rows]
    pnl = [value for value in pnl if value is not None]
    scores = [_decimal(row.get("continuation_score")) for row in rows]
    scores = [value for value in scores if value is not None]
    states = Counter(str(row.get("continuation_state") or "UNKNOWN") for row in rows)
    return {
        "trade_count": len(rows),
        "path_available_count": sum(1 for row in rows if row.get("path_status") == "AVAILABLE"),
        "path_missing_count": sum(1 for row in rows if row.get("path_status") != "AVAILABLE"),
        "win_count": sum(1 for value in pnl if value > 0),
        "loss_count": sum(1 for value in pnl if value < 0),
        "realized_pnl_points": _str_decimal(sum(pnl, Decimal("0"))),
        "average_realized_pnl_points": _str_decimal(_avg(pnl)),
        "average_continuation_score": _str_decimal(_avg(scores)),
        "state_counts": dict(states),
        "confirmed_or_weak_count": states.get("CONTINUATION_CONFIRMED", 0) + states.get("CONTINUATION_WEAK", 0),
        "impulse_or_reversal_count": states.get("IMPULSE_ONLY", 0) + states.get("REVERSAL_RISK", 0),
        "average_persistence_ratio": _str_decimal(_avg([_decimal((row.get("features") or {}).get("close_to_close_persistence_ratio")) for row in rows])),
        "average_structure_ratio": _str_decimal(_avg([_decimal((row.get("features") or {}).get("structure_ratio")) for row in rows])),
        "average_giveback_ratio": _str_decimal(_avg([_decimal((row.get("features") or {}).get("giveback_from_mfe_to_exit_ratio")) for row in rows])),
        "current_exit_worsened_vs_best_horizon_count": sum(1 for row in rows if (row.get("features") or {}).get("current_exit_worsened_vs_best_horizon") is True),
        "current_exit_improved_vs_best_horizon_count": sum(1 for row in rows if (row.get("features") or {}).get("current_exit_improved_vs_best_horizon") is True),
    }


def _best_overlay_candidates(lane_summary: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows = [
        {"lane_id": lane, **metrics}
        for lane, metrics in lane_summary.items()
        if int(metrics.get("path_available_count") or 0) >= 10
        and int(metrics.get("confirmed_or_weak_count") or 0) >= max(5, int(metrics.get("path_available_count") or 0) // 2)
    ]
    return sorted(rows, key=lambda row: (row.get("average_continuation_score") or "0", row.get("realized_pnl_points") or "0"), reverse=True)[:20]


def _continuation_failures(lane_summary: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows = [
        {"lane_id": lane, **metrics}
        for lane, metrics in lane_summary.items()
        if int(metrics.get("path_available_count") or 0) >= 10
        and int(metrics.get("impulse_or_reversal_count") or 0) >= max(5, int(metrics.get("path_available_count") or 0) // 2)
    ]
    return sorted(rows, key=lambda row: (row.get("impulse_or_reversal_count") or 0, row.get("realized_pnl_points") or "0"), reverse=True)[:20]


def _needs_more_path(lane_summary: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows = [
        {"lane_id": lane, **metrics}
        for lane, metrics in lane_summary.items()
        if int(metrics.get("path_available_count") or 0) < 10
    ]
    return sorted(rows, key=lambda row: (row.get("path_available_count") or 0, row.get("trade_count") or 0))[:30]


def _research_questions(cohorts: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, Any]:
    return {
        "winners_show_stronger_continuation_than_losers": _winner_loser_read([row for rows in cohorts.values() for row in rows]),
        "us_longs_fail_because_continuation_weak_or_reversing": _cohort_read(cohorts.get("us_longs", [])),
        "london_late_shorts_show_confirmed_continuation": _cohort_read(cohorts.get("london_late_shorts", [])),
        "current_exits_cut_continuation_too_early_or_hold_too_long": _exit_timing_read([row for rows in cohorts.values() for row in rows]),
    }


def _winner_loser_read(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    winners = [row for row in rows if (_decimal(row.get("realized_pnl_points")) or Decimal("0")) > 0]
    losers = [row for row in rows if (_decimal(row.get("realized_pnl_points")) or Decimal("0")) < 0]
    return {
        "winner_metrics": _group_metrics(winners),
        "loser_metrics": _group_metrics(losers),
        "status": "NEEDS_PATH_DATA" if not winners or not losers or _group_metrics(rows)["path_available_count"] == 0 else "COMPARABLE",
    }


def _cohort_read(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    metrics = _group_metrics(rows)
    path_count = int(metrics.get("path_available_count") or 0)
    if path_count == 0:
        read = "NEEDS_PATH_DATA"
    elif int(metrics.get("confirmed_or_weak_count") or 0) > int(metrics.get("impulse_or_reversal_count") or 0):
        read = "CONTINUATION_PRESENT"
    else:
        read = "CONTINUATION_WEAK_OR_REVERSING"
    return {"status": read, "metrics": metrics}


def _exit_timing_read(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    metrics = _group_metrics(rows)
    worsened = int(metrics.get("current_exit_worsened_vs_best_horizon_count") or 0)
    improved = int(metrics.get("current_exit_improved_vs_best_horizon_count") or 0)
    if int(metrics.get("path_available_count") or 0) == 0:
        status = "NEEDS_PATH_DATA"
    elif worsened > improved:
        status = "CURRENT_EXIT_OFTEN_WORSE_THAN_FIXED_HORIZON"
    elif improved > worsened:
        status = "CURRENT_EXIT_OFTEN_IMPROVES_ON_FIXED_HORIZON"
    else:
        status = "MIXED_OR_FLAT"
    return {"status": status, "metrics": metrics}


def _trade_candles(
    trade: Mapping[str, Any],
    *,
    side_replay: Mapping[str, Any] | None,
    phase1_root: Path,
    forward_capture: Mapping[tuple[str, str], Sequence[Mapping[str, Any]]],
) -> tuple[list[Mapping[str, Any]], str | None]:
    path_rows = list(side_replay.get("entry_to_exit_path") or []) if side_replay else []
    if path_rows:
        return [_normalize_path_bar(row) for row in path_rows], "side_session_replay.entry_to_exit_path"
    symbol = str(trade.get("symbol") or "").upper()
    start = _parse_time(trade.get("entry_time"))
    end = _parse_time(trade.get("exit_time"))
    phase1_path = phase1_root / symbol / "1m" / "latest_runtime_candles.json"
    bars = _window_bars((_read_json(phase1_path).get("bars") or []), start=start, end=end)
    if bars:
        return bars, str(phase1_path)
    capture_bars = _window_bars(forward_capture.get((symbol, "1m"), []), start=start, end=end)
    if capture_bars:
        return capture_bars, "forward_path_capture.1m"
    return [], None


def _normalize_path_bar(row: Mapping[str, Any]) -> dict[str, Any]:
    close = row.get("close")
    return {
        "bar_end": row.get("bar_end") or row.get("timestamp"),
        "open": row.get("open") or close,
        "high": row.get("high"),
        "low": row.get("low"),
        "close": close,
        "vwap": row.get("vwap"),
    }


def _window_bars(rows: Sequence[Mapping[str, Any]], *, start: datetime | None, end: datetime | None) -> list[Mapping[str, Any]]:
    if start is None or end is None:
        return []
    selected: list[Mapping[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        ts = _parse_time(row.get("bar_end") or row.get("timestamp"))
        if ts is not None and start <= ts <= end:
            selected.append(row)
    return selected


def _path_metrics(
    *,
    candles: Sequence[Mapping[str, Any]],
    side: str,
    entry_price: Decimal,
    entry_time: datetime | None,
) -> dict[str, Decimal | None]:
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
        favorable = entry_price - low if side == "SHORT" else high - entry_price
        adverse = entry_price - high if side == "SHORT" else low - entry_price
        if mfe is None or favorable > mfe:
            mfe = favorable
            mfe_time = ts
        if mae is None or adverse < mae:
            mae = adverse
            mae_time = ts
    return {
        "mfe_points": mfe,
        "mae_points": mae,
        "time_to_mfe_seconds": Decimal(_seconds_between(entry_time, mfe_time)) if _seconds_between(entry_time, mfe_time) is not None else None,
        "time_to_mae_seconds": Decimal(_seconds_between(entry_time, mae_time)) if _seconds_between(entry_time, mae_time) is not None else None,
    }


def _horizon_pnl(
    *,
    candles: Sequence[Mapping[str, Any]],
    side: str,
    entry_price: Decimal | None,
    entry_time: datetime | None,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for minutes in HORIZONS_MINUTES:
        key = f"{minutes}m"
        if not candles or entry_price is None or entry_time is None:
            result[key] = {"available": False, "pnl_points": None, "price": None, "bar_end": None}
            continue
        target = entry_time + timedelta(minutes=minutes)
        selected = next(
            (candle for candle in candles if (_parse_time(candle.get("bar_end") or candle.get("timestamp")) or datetime.min.replace(tzinfo=UTC)) >= target),
            None,
        )
        price = _decimal(selected.get("close")) if selected else None
        result[key] = {
            "available": price is not None,
            "pnl_points": _str_decimal(_pnl_points(side=side, entry_price=entry_price, exit_price=price)),
            "price": _str_decimal(price),
            "bar_end": selected.get("bar_end") or selected.get("timestamp") if selected else None,
        }
    return result


def _best_horizon(horizons: Mapping[str, Mapping[str, Any]]) -> Mapping[str, Any] | None:
    available = [row for row in horizons.values() if row.get("available") and row.get("pnl_points") is not None]
    return max(available, key=lambda row: _decimal(row.get("pnl_points")) or Decimal("-999999999"), default=None)


def _directional_delta(*, side: str, newer: Decimal | None, older: Decimal | None) -> Decimal | None:
    if newer is None or older is None:
        return None
    return older - newer if side == "SHORT" else newer - older


def _structure_ratio(*, side: str, highs: Sequence[Decimal | None], lows: Sequence[Decimal | None]) -> Decimal | None:
    checks: list[bool] = []
    for index in range(1, min(len(highs), len(lows))):
        high = highs[index]
        prev_high = highs[index - 1]
        low = lows[index]
        prev_low = lows[index - 1]
        if high is None or prev_high is None or low is None or prev_low is None:
            continue
        if side == "SHORT":
            checks.append(high <= prev_high and low <= prev_low)
        else:
            checks.append(high >= prev_high and low >= prev_low)
    return _ratio(checks)


def _body_strength(candle: Mapping[str, Any]) -> Decimal | None:
    open_price = _decimal(candle.get("open"))
    close = _decimal(candle.get("close"))
    high = _decimal(candle.get("high"))
    low = _decimal(candle.get("low"))
    if open_price is None or close is None or high is None or low is None:
        return None
    candle_range = high - low
    if candle_range <= 0:
        return None
    return abs(close - open_price) / candle_range


def _close_location(candle: Mapping[str, Any], *, side: str) -> Decimal | None:
    close = _decimal(candle.get("close"))
    high = _decimal(candle.get("high"))
    low = _decimal(candle.get("low"))
    if close is None or high is None or low is None:
        return None
    candle_range = high - low
    if candle_range <= 0:
        return None
    return (high - close) / candle_range if side == "SHORT" else (close - low) / candle_range


def _range_expansion_ratio(ranges: Sequence[Decimal]) -> Decimal | None:
    if len(ranges) < 4:
        return None
    baseline = _avg(ranges[: min(3, len(ranges) // 2)])
    later = _avg(ranges[min(3, len(ranges) // 2) :])
    if baseline is None or later is None or baseline <= 0:
        return None
    return later / baseline


def _vwap_alignment(*, candles: Sequence[Mapping[str, Any]], side: str) -> Decimal | None:
    checks: list[bool] = []
    for candle in candles:
        close = _decimal(candle.get("close"))
        vwap = _decimal(candle.get("vwap"))
        if close is None or vwap is None:
            continue
        checks.append(close <= vwap if side == "SHORT" else close >= vwap)
    return _ratio(checks)


def _alignment(value: Decimal | None) -> str:
    if value is None:
        return "UNKNOWN"
    if value > 0:
        return "ALIGNED_WITH_TRADE"
    if value < 0:
        return "AGAINST_TRADE"
    return "FLAT"


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


def _side_replay_by_key(rows: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str, str, str], Mapping[str, Any]]:
    return {_trade_key(row): row for row in rows if row.get("event_type") == "SIDE_SESSION_ATTRIBUTION_TRADE_REPLAY"}


def _trade_key(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row.get("trade_id") or ""),
        str(row.get("lifecycle_id") or ""),
        str(row.get("lane_id") or ""),
        str(row.get("entry_time") or ""),
    )


def _forward_capture_by_symbol(rows: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str], Sequence[Mapping[str, Any]]]:
    captures: dict[tuple[str, str], Sequence[Mapping[str, Any]]] = {}
    for row in rows:
        if row.get("event_type") != "FORWARD_PATH_CANDLE_CAPTURE":
            continue
        symbol = str(row.get("symbol") or "").upper()
        timeframe = str(row.get("timeframe") or "")
        bars = row.get("bars") if isinstance(row.get("bars"), list) else []
        captures[(symbol, timeframe)] = bars
    return captures


def _markdown_report(summary: Mapping[str, Any]) -> str:
    lines = [
        "# Trend Continuation Overlay",
        "",
        f"Generated: {summary.get('generated_at')}",
        "",
        "Analytics-only shadow overlay. It does not create signals, gate runtime, or own broker/Managed Exit authority.",
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
            ["Cohort", "Trades", "Path", "W/L", "P&L pts", "Avg score", "States"],
            [
                [
                    cohort,
                    metrics.get("trade_count"),
                    f"{metrics.get('path_available_count')}/{metrics.get('trade_count')}",
                    f"{metrics.get('win_count')}/{metrics.get('loss_count')}",
                    metrics.get("realized_pnl_points"),
                    metrics.get("average_continuation_score"),
                    metrics.get("state_counts"),
                ]
                for cohort, metrics in summary.get("cohorts", {}).items()
            ],
        ),
        "",
        "## Research Reads",
        "",
        "```json",
        json.dumps(summary.get("research_questions") or {}, indent=2, sort_keys=True),
        "```",
        "",
        "## Future Overlay Candidates",
        "",
        _lane_table(summary.get("lanes_best_candidates_for_future_overlay_integration", [])[:20]),
        "",
        "## Continuation Failures",
        "",
        _lane_table(summary.get("lanes_where_continuation_fails", [])[:20]),
        "",
        "## Needs More Path Data",
        "",
        _lane_table(summary.get("lanes_needing_more_path_data", [])[:30]),
        "",
    ]
    return "\n".join(lines) + "\n"


def _lane_table(rows: Sequence[Mapping[str, Any]]) -> str:
    return _table(
        ["Lane", "Trades", "Path", "W/L", "P&L pts", "Avg score", "States"],
        [
            [
                row.get("lane_id"),
                row.get("trade_count"),
                f"{row.get('path_available_count')}/{row.get('trade_count')}",
                f"{row.get('win_count')}/{row.get('loss_count')}",
                row.get("realized_pnl_points"),
                row.get("average_continuation_score"),
                row.get("state_counts"),
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


def _decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _jsonable_decimal(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _str_decimal(value)
    if isinstance(value, Mapping):
        return {key: _jsonable_decimal(nested) for key, nested in value.items()}
    if isinstance(value, list):
        return [_jsonable_decimal(item) for item in value]
    return value


def _str_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value.normalize()) if value != value.to_integral() else str(value.quantize(Decimal("1")))


def _avg(values: Sequence[Decimal | None]) -> Decimal | None:
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return sum(clean, Decimal("0")) / Decimal(len(clean))


def _ratio(values: Sequence[bool]) -> Decimal | None:
    if not values:
        return None
    return Decimal(sum(1 for value in values if value)) / Decimal(len(values))


def _seconds_between(start: datetime | None, end: datetime | None) -> int | None:
    if start is None or end is None:
        return None
    return int((end - start).total_seconds())


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
    result = build_trend_continuation_overlay(repo_root=Path(args.repo_root))
    print(
        json.dumps(
            {
                "classification": "TREND_CONTINUATION_OVERLAY_READY",
                "summary_path": str(result.summary_path),
                "scores_path": str(result.scores_path),
                "report_path": str(result.report_path),
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
