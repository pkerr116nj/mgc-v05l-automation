"""Research-only futures strategy discovery program built on persisted bars."""

from __future__ import annotations

import json
import math
import sqlite3
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import fmean, median, pstdev
from typing import Any

from ..domain.models import Bar

DEFAULT_FUTURES_UNIVERSE: tuple[str, ...] = (
    "6A",
    "6B",
    "6E",
    "6J",
    "CL",
    "ES",
    "GC",
    "HG",
    "MBT",
    "MES",
    "MGC",
    "MNQ",
    "NG",
    "NQ",
    "PL",
    "QC",
    "YM",
    "ZB",
    "ZF",
    "ZN",
    "ZQ",
    "ZT",
)
HIGHER_TIMEFRAMES: tuple[str, ...] = ("60m", "240m", "720m", "1440m")
SESSION_ORDER: tuple[str, ...] = ("ASIA", "LONDON", "US", "UNKNOWN")
REGIME_ORDER: tuple[str, ...] = ("UP", "DOWN", "NEUTRAL", "UNCLASSIFIED")


@dataclass(frozen=True)
class CandidateSpec:
    candidate_id: str
    family: str
    direction: str
    gated: bool
    variant: str
    interpretability_level: str
    description: str
    feature_classes: tuple[str, ...]
    entry_logic: str
    exit_logic: str
    translation_feasibility: str
    hold_bars: int
    stop_r: float
    target_r: float
    params: dict[str, float]


@dataclass(frozen=True)
class TradeRecord:
    candidate_id: str
    family: str
    direction: str
    symbol: str
    entry_ts: str
    exit_ts: str
    entry_session: str
    regime_label: str
    r_multiple: float
    holding_bars: int
    exit_reason: str


@dataclass(frozen=True)
class SymbolCandidateSummary:
    symbol: str
    trade_count: int
    expectancy_r: float
    win_rate: float
    profit_factor: float
    max_drawdown_r: float
    avg_holding_bars: float
    top_3_positive_share: float
    walk_forward_positive_ratio: float


@dataclass(frozen=True)
class CandidateSummary:
    candidate_id: str
    family: str
    direction: str
    gated: bool
    variant: str
    rank_score: float
    trade_count: int
    expectancy_r: float
    win_rate: float
    avg_win_r: float
    avg_loss_r: float
    profit_factor: float
    max_drawdown_r: float
    sharpe_proxy: float
    avg_holding_bars: float
    positive_symbol_share: float
    median_symbol_expectancy_r: float
    walk_forward_positive_ratio: float
    cost_expectancy_r_005: float
    cost_expectancy_r_010: float
    top_3_positive_share: float
    returns_without_top_1_r: float
    regime_gating_lift_r: float | None
    regime_gating_lift_positive_symbol_share: float | None
    parameter_neighbor_stability: float | None
    best_symbols: tuple[str, ...]
    dominant_sessions: tuple[str, ...]
    dominant_regimes: tuple[str, ...]
    interpretability_level: str
    description: str
    feature_classes: tuple[str, ...]
    entry_logic: str
    exit_logic: str
    translation_feasibility: str
    per_symbol: tuple[SymbolCandidateSummary, ...]


@dataclass(frozen=True)
class StrategyResearchArtifacts:
    json_path: Path
    markdown_path: Path
    report: dict[str, Any]


@dataclass(frozen=True)
class _FrameSeries:
    bars: list[Bar]
    opens: list[float]
    highs: list[float]
    lows: list[float]
    closes: list[float]
    ranges: list[float]
    timestamps: list[datetime]
    prefix_closes: list[float]
    prefix_ranges: list[float]
    prefix_abs_returns: list[float]

    @classmethod
    def from_bars(cls, bars: list[Bar]) -> "_FrameSeries":
        opens = [float(bar.open) for bar in bars]
        highs = [float(bar.high) for bar in bars]
        lows = [float(bar.low) for bar in bars]
        closes = [float(bar.close) for bar in bars]
        ranges = [max(high - low, 1e-9) for high, low in zip(highs, lows, strict=False)]
        timestamps = [bar.end_ts for bar in bars]

        prefix_closes = [0.0]
        prefix_ranges = [0.0]
        prefix_abs_returns = [0.0]
        for index, close in enumerate(closes):
            prefix_closes.append(prefix_closes[-1] + close)
            prefix_ranges.append(prefix_ranges[-1] + ranges[index])
            abs_return = 0.0 if index == 0 else abs(close - closes[index - 1])
            prefix_abs_returns.append(prefix_abs_returns[-1] + abs_return)
        return cls(
            bars=bars,
            opens=opens,
            highs=highs,
            lows=lows,
            closes=closes,
            ranges=ranges,
            timestamps=timestamps,
            prefix_closes=prefix_closes,
            prefix_ranges=prefix_ranges,
            prefix_abs_returns=prefix_abs_returns,
        )

    def mean_close(self, idx: int, window: int) -> float | None:
        start = idx - window + 1
        if start < 0:
            return None
        total = self.prefix_closes[idx + 1] - self.prefix_closes[start]
        return total / float(window)

    def mean_range(self, idx: int, window: int) -> float | None:
        start = idx - window + 1
        if start < 0:
            return None
        total = self.prefix_ranges[idx + 1] - self.prefix_ranges[start]
        return max(total / float(window), 1e-9)

    def max_high_prior(self, idx: int, window: int) -> float | None:
        start = idx - window
        if start < 0 or idx <= 0:
            return None
        return max(self.highs[start:idx])

    def min_low_prior(self, idx: int, window: int) -> float | None:
        start = idx - window
        if start < 0 or idx <= 0:
            return None
        return min(self.lows[start:idx])

    def max_close_prior(self, idx: int, window: int) -> float | None:
        start = idx - window
        if start < 0 or idx <= 0:
            return None
        return max(self.closes[start:idx])

    def min_close_prior(self, idx: int, window: int) -> float | None:
        start = idx - window
        if start < 0 or idx <= 0:
            return None
        return min(self.closes[start:idx])

    def range_span(self, idx: int, window: int) -> float | None:
        start = idx - window + 1
        if start < 0:
            return None
        return max(self.highs[start : idx + 1]) - min(self.lows[start : idx + 1])

    def slope(self, idx: int, window: int) -> float | None:
        start = idx - window
        if start < 0:
            return None
        norm = self.mean_range(idx, min(window, 12))
        if norm is None:
            return None
        return (self.closes[idx] - self.closes[start]) / norm

    def efficiency(self, idx: int, window: int) -> float | None:
        start = idx - window
        if start < 0:
            return None
        path = self.prefix_abs_returns[idx + 1] - self.prefix_abs_returns[start + 1]
        if path <= 0:
            return None
        return abs(self.closes[idx] - self.closes[start]) / path

    def distance_to_mean(self, idx: int, window: int) -> float | None:
        mean_close = self.mean_close(idx, window)
        norm = self.mean_range(idx, min(window, 12))
        if mean_close is None or norm is None:
            return None
        return (self.closes[idx] - mean_close) / norm

    def compression_ratio(self, idx: int, range_window: int, norm_window: int) -> float | None:
        span = self.range_span(idx, range_window)
        norm = self.mean_range(idx, norm_window)
        if span is None or norm is None:
            return None
        return span / max(norm * float(range_window), 1e-9)


def run_quant_futures_research(
    *,
    database_path: str | Path,
    execution_timeframe: str = "5m",
    symbols: tuple[str, ...] | None = None,
    output_dir: str | Path | None = None,
) -> StrategyResearchArtifacts:
    resolved_database_path = Path(database_path).resolve()
    resolved_output_dir = Path(output_dir or Path.cwd() / "outputs" / "reports" / "quant_futures_research").resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    selected_symbols = symbols or _available_symbols(resolved_database_path, execution_timeframe)
    specs = _build_candidate_specs()

    candidate_trades: dict[str, list[TradeRecord]] = {spec.candidate_id: [] for spec in specs}
    candidate_per_symbol: dict[str, dict[str, list[TradeRecord]]] = {
        spec.candidate_id: defaultdict(list) for spec in specs
    }

    symbol_family_best: dict[str, dict[str, tuple[float, str]]] = defaultdict(dict)
    universe_notes = []
    for symbol in selected_symbols:
        symbol_payload = _load_symbol_payload(
            database_path=resolved_database_path,
            symbol=symbol,
            execution_timeframe=execution_timeframe,
        )
        if symbol_payload is None:
            universe_notes.append(f"{symbol}: skipped because one or more required timeframes were unavailable.")
            continue

        execution = symbol_payload["execution"]
        feature_rows = _build_feature_rows(
            execution=execution,
            higher=symbol_payload["higher"],
            alignments=symbol_payload["alignments"],
        )
        for spec in specs:
            trades = _simulate_candidate(
                spec=spec,
                symbol=symbol,
                execution=execution,
                features=feature_rows,
            )
            candidate_trades[spec.candidate_id].extend(trades)
            candidate_per_symbol[spec.candidate_id][symbol].extend(trades)
            expectancy = _expectancy([trade.r_multiple for trade in trades])
            prior = symbol_family_best[symbol].get(spec.family)
            if prior is None or expectancy > prior[0]:
                symbol_family_best[symbol][spec.family] = (expectancy, spec.candidate_id)

    summaries = _build_candidate_summaries(
        specs=specs,
        candidate_trades=candidate_trades,
        candidate_per_symbol=candidate_per_symbol,
        selected_symbols=selected_symbols,
    )
    family_rollup = _build_family_rollup(summaries)
    longer_horizon_report = _build_longer_horizon_importance_report(summaries)
    instrument_affinities = _build_instrument_affinities(symbol_family_best, summaries)
    decision_shortlist = _build_decision_shortlist(summaries, instrument_affinities)

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "database_path": str(resolved_database_path),
        "execution_timeframe": execution_timeframe,
        "symbols_tested": list(selected_symbols),
        "phase_1_research_design": _phase_1_design(execution_timeframe=execution_timeframe, symbols=selected_symbols),
        "broad_discovery": {
            "universe_notes": universe_notes,
            "longer_horizon_importance": longer_horizon_report,
            "family_rollup": family_rollup,
            "instrument_affinities": instrument_affinities,
            "candidate_rankings": [asdict(summary) for summary in summaries],
            "dead_end_areas": _build_dead_end_areas(family_rollup),
            "current_pattern_engine_relationship": _build_pattern_engine_relationship(
                family_rollup=family_rollup,
                instrument_affinities=instrument_affinities,
            ),
        },
        "promotion_shortlist": decision_shortlist,
    }

    json_path = resolved_output_dir / "quant_futures_research_program.json"
    markdown_path = resolved_output_dir / "quant_futures_research_program.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(report).strip() + "\n", encoding="utf-8")
    return StrategyResearchArtifacts(
        json_path=json_path,
        markdown_path=markdown_path,
        report=report,
    )


def _available_symbols(database_path: Path, execution_timeframe: str) -> tuple[str, ...]:
    connection = sqlite3.connect(database_path)
    try:
        rows = connection.execute(
            "select ticker from bars where timeframe = ? group by ticker order by ticker",
            (execution_timeframe,),
        ).fetchall()
    finally:
        connection.close()
    found = [str(row[0]) for row in rows if str(row[0]) in DEFAULT_FUTURES_UNIVERSE]
    return tuple(found)


def _load_symbol_payload(
    *,
    database_path: Path,
    symbol: str,
    execution_timeframe: str,
) -> dict[str, Any] | None:
    all_timeframes = (execution_timeframe,) + HIGHER_TIMEFRAMES
    connection = sqlite3.connect(database_path)
    try:
        bars_by_timeframe: dict[str, list[Bar]] = {}
        for timeframe in all_timeframes:
            rows = connection.execute(
                """
                select bar_id, symbol, timeframe, start_ts, end_ts, open, high, low, close, volume,
                       is_final, session_asia, session_london, session_us, session_allowed
                from bars
                where ticker = ?
                  and timeframe = ?
                order by end_ts
                """,
                (symbol, timeframe),
            ).fetchall()
            if not rows:
                return None
            bars_by_timeframe[timeframe] = [
                Bar(
                    bar_id=str(row[0]),
                    symbol=str(row[1]),
                    timeframe=str(row[2]),
                    start_ts=datetime.fromisoformat(str(row[3])),
                    end_ts=datetime.fromisoformat(str(row[4])),
                    open=_to_float_decimal(row[5]),
                    high=_to_float_decimal(row[6]),
                    low=_to_float_decimal(row[7]),
                    close=_to_float_decimal(row[8]),
                    volume=int(row[9]),
                    is_final=bool(row[10]),
                    session_asia=bool(row[11]),
                    session_london=bool(row[12]),
                    session_us=bool(row[13]),
                    session_allowed=bool(row[14]),
                )
                for row in rows
            ]
    finally:
        connection.close()

    execution = _FrameSeries.from_bars(bars_by_timeframe[execution_timeframe])
    higher = {timeframe: _FrameSeries.from_bars(bars_by_timeframe[timeframe]) for timeframe in HIGHER_TIMEFRAMES}
    alignments = {
        timeframe: _align_timestamps(execution.timestamps, higher_frame.timestamps)
        for timeframe, higher_frame in higher.items()
    }
    return {"execution": execution, "higher": higher, "alignments": alignments}


def _to_float_decimal(value: Any):
    from decimal import Decimal

    return Decimal(str(value))


def _align_timestamps(execution_ts: list[datetime], higher_ts: list[datetime]) -> list[int]:
    alignment = [-1] * len(execution_ts)
    higher_index = -1
    for index, timestamp in enumerate(execution_ts):
        while higher_index + 1 < len(higher_ts) and higher_ts[higher_index + 1] <= timestamp:
            higher_index += 1
        alignment[index] = higher_index
    return alignment


def _build_feature_rows(
    *,
    execution: _FrameSeries,
    higher: dict[str, _FrameSeries],
    alignments: dict[str, list[int]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for idx, bar in enumerate(execution.bars):
        risk_unit = execution.mean_range(idx, 20)
        if risk_unit is None:
            rows.append({"ready": False})
            continue

        slope_60 = _aligned_metric(higher["60m"], alignments["60m"][idx], "slope", 8)
        slope_240 = _aligned_metric(higher["240m"], alignments["240m"][idx], "slope", 6)
        slope_720 = _aligned_metric(higher["720m"], alignments["720m"][idx], "slope", 3)
        slope_1440 = _aligned_metric(higher["1440m"], alignments["1440m"][idx], "slope", 20)
        eff_240 = _aligned_metric(higher["240m"], alignments["240m"][idx], "efficiency", 6)
        eff_720 = _aligned_metric(higher["720m"], alignments["720m"][idx], "efficiency", 3)
        dist_60 = _aligned_metric(higher["60m"], alignments["60m"][idx], "distance_to_mean", 8)
        dist_240 = _aligned_metric(higher["240m"], alignments["240m"][idx], "distance_to_mean", 6)
        dist_1440 = _aligned_metric(higher["1440m"], alignments["1440m"][idx], "distance_to_mean", 20)
        compression_60 = _aligned_metric(higher["60m"], alignments["60m"][idx], "compression_ratio", (4, 12))

        prior_high_20 = execution.max_high_prior(idx, 20)
        prior_low_20 = execution.min_low_prior(idx, 20)
        prior_close_high_12 = execution.max_close_prior(idx, 12)
        prior_close_low_12 = execution.min_close_prior(idx, 12)
        compression_5 = execution.compression_ratio(idx, 12, 20)
        mean_5 = execution.mean_close(idx, 10)
        if (
            slope_60 is None
            or slope_240 is None
            or slope_720 is None
            or slope_1440 is None
            or eff_240 is None
            or eff_720 is None
            or dist_60 is None
            or dist_240 is None
            or dist_1440 is None
            or compression_60 is None
            or prior_high_20 is None
            or prior_low_20 is None
            or prior_close_high_12 is None
            or prior_close_low_12 is None
            or compression_5 is None
            or mean_5 is None
            or idx == 0
        ):
            rows.append({"ready": False})
            continue

        close = execution.closes[idx]
        open_ = execution.opens[idx]
        high = execution.highs[idx]
        low = execution.lows[idx]
        prev_close = execution.closes[idx - 1]
        close_pos = (close - low) / max(high - low, 1e-9)
        pullback_from_high = (prior_close_high_12 - close) / risk_unit
        bounce_from_low = (close - prior_close_low_12) / risk_unit
        breakout_up = (close - prior_high_20) / risk_unit
        breakout_down = (prior_low_20 - close) / risk_unit
        regime_up = slope_240 > 0.60 and slope_720 > 0.25 and slope_1440 > 0.0 and eff_240 > 0.45
        regime_down = slope_240 < -0.60 and slope_720 < -0.25 and slope_1440 < 0.0 and eff_240 > 0.45
        regime_neutral = not regime_up and not regime_down and abs(slope_240) < 0.95
        extended_up = dist_240 > 1.10 or dist_1440 > 1.25
        extended_down = dist_240 < -1.10 or dist_1440 < -1.25
        session_label = _session_label(bar)

        rows.append(
            {
                "ready": True,
                "risk_unit": risk_unit,
                "close_pos": close_pos,
                "body_r": abs(close - open_) / risk_unit,
                "bar_range_r": (high - low) / risk_unit,
                "prev_close_delta_r": (close - prev_close) / risk_unit,
                "slope_60": slope_60,
                "slope_240": slope_240,
                "slope_720": slope_720,
                "slope_1440": slope_1440,
                "eff_240": eff_240,
                "eff_720": eff_720,
                "dist_60": dist_60,
                "dist_240": dist_240,
                "dist_1440": dist_1440,
                "compression_60": compression_60,
                "compression_5": compression_5,
                "pullback_from_high": pullback_from_high,
                "bounce_from_low": bounce_from_low,
                "breakout_up": breakout_up,
                "breakout_down": breakout_down,
                "failed_breakout_short": high > prior_high_20 and close < prior_high_20 and close_pos < 0.40,
                "failed_breakout_long": low < prior_low_20 and close > prior_low_20 and close_pos > 0.60,
                "regime_up": regime_up,
                "regime_down": regime_down,
                "regime_neutral": regime_neutral,
                "extended_up": extended_up,
                "extended_down": extended_down,
                "session_label": session_label,
                "regime_label": "UP" if regime_up else "DOWN" if regime_down else "NEUTRAL" if regime_neutral else "UNCLASSIFIED",
                "close_above_mean_5": close > mean_5,
                "close_below_mean_5": close < mean_5,
            }
        )
    return rows


def _aligned_metric(
    frame: _FrameSeries,
    idx: int,
    metric: str,
    arg: int | tuple[int, int],
) -> float | None:
    if idx < 0:
        return None
    method = getattr(frame, metric)
    if isinstance(arg, tuple):
        return method(idx, *arg)
    return method(idx, arg)


def _simulate_candidate(
    *,
    spec: CandidateSpec,
    symbol: str,
    execution: _FrameSeries,
    features: list[dict[str, Any]],
) -> list[TradeRecord]:
    trades: list[TradeRecord] = []
    next_available_index = 0
    for index, feature in enumerate(features):
        if index < next_available_index or index + 1 >= len(execution.bars):
            continue
        if not feature.get("ready"):
            continue
        if not _signal_matches(spec, feature):
            continue

        entry_index = index + 1
        entry_price = execution.opens[entry_index]
        risk = max(float(feature["risk_unit"]), 1e-6)
        stop_price = entry_price - spec.stop_r * risk if spec.direction == "LONG" else entry_price + spec.stop_r * risk
        target_price = entry_price + spec.target_r * risk if spec.direction == "LONG" else entry_price - spec.target_r * risk
        exit_index, exit_price, exit_reason = _resolve_exit(
            direction=spec.direction,
            execution=execution,
            entry_index=entry_index,
            hold_bars=spec.hold_bars,
            stop_price=stop_price,
            target_price=target_price,
        )
        signed_move = exit_price - entry_price if spec.direction == "LONG" else entry_price - exit_price
        r_multiple = signed_move / risk
        trades.append(
            TradeRecord(
                candidate_id=spec.candidate_id,
                family=spec.family,
                direction=spec.direction,
                symbol=symbol,
                entry_ts=execution.timestamps[entry_index].isoformat(),
                exit_ts=execution.timestamps[exit_index].isoformat(),
                entry_session=str(features[index]["session_label"]),
                regime_label=str(features[index]["regime_label"]),
                r_multiple=r_multiple,
                holding_bars=max(exit_index - entry_index + 1, 1),
                exit_reason=exit_reason,
            )
        )
        next_available_index = exit_index + 1
    return trades


def _signal_matches(spec: CandidateSpec, feature: dict[str, Any]) -> bool:
    params = spec.params
    if spec.family == "trend_pullback_hybrid":
        if spec.gated and not feature[f"regime_{'up' if spec.direction == 'LONG' else 'down'}"]:
            return False
        if spec.direction == "LONG":
            return (
                feature["slope_60"] >= params["slope_60_min"]
                and params["pullback_min"] <= feature["pullback_from_high"] <= params["pullback_max"]
                and feature["close_pos"] >= params["close_pos_min"]
                and feature["prev_close_delta_r"] >= params["trigger_delta_min"]
                and feature["dist_60"] <= params["dist_60_max"]
                and feature["close_above_mean_5"]
            )
        return (
            feature["slope_60"] <= -params["slope_60_min"]
            and params["pullback_min"] <= feature["bounce_from_low"] <= params["pullback_max"]
            and feature["close_pos"] <= 1.0 - params["close_pos_min"]
            and feature["prev_close_delta_r"] <= -params["trigger_delta_min"]
            and feature["dist_60"] >= -params["dist_60_max"]
            and feature["close_below_mean_5"]
        )

    if spec.family == "breakout_acceptance":
        if spec.gated and not feature[f"regime_{'up' if spec.direction == 'LONG' else 'down'}"]:
            return False
        if spec.direction == "LONG":
            return (
                feature["compression_60"] <= params["compression_60_max"]
                and feature["compression_5"] <= params["compression_5_max"]
                and feature["breakout_up"] >= params["breakout_min"]
                and feature["close_pos"] >= params["close_pos_min"]
                and feature["slope_60"] >= params["slope_60_min"]
            )
        return (
            feature["compression_60"] <= params["compression_60_max"]
            and feature["compression_5"] <= params["compression_5_max"]
            and feature["breakout_down"] >= params["breakout_min"]
            and feature["close_pos"] <= 1.0 - params["close_pos_min"]
            and feature["slope_60"] <= -params["slope_60_min"]
        )

    if spec.family == "regime_conditioned_mean_reversion":
        if spec.gated:
            if spec.direction == "LONG" and not (feature["regime_neutral"] or feature["regime_up"]):
                return False
            if spec.direction == "SHORT" and not (feature["regime_neutral"] or feature["regime_down"]):
                return False
        if spec.direction == "LONG":
            return (
                feature["dist_60"] <= -params["stretch_min"]
                and feature["dist_240"] >= -params["trend_floor"]
                and feature["close_pos"] >= params["close_pos_min"]
                and feature["prev_close_delta_r"] >= params["trigger_delta_min"]
            )
        return (
            feature["dist_60"] >= params["stretch_min"]
            and feature["dist_240"] <= params["trend_floor"]
            and feature["close_pos"] <= 1.0 - params["close_pos_min"]
            and feature["prev_close_delta_r"] <= -params["trigger_delta_min"]
        )

    if spec.family == "failed_move_reversal":
        if spec.direction == "LONG":
            regime_ok = feature["extended_down"] or feature["regime_neutral"] or feature["regime_down"]
            if spec.gated and not regime_ok:
                return False
            return (
                feature["failed_breakout_long"]
                and feature["close_pos"] >= params["close_pos_min"]
                and feature["body_r"] >= params["body_r_min"]
                and feature["dist_240"] <= -params["dist_240_extreme"]
            )
        regime_ok = feature["extended_up"] or feature["regime_neutral"] or feature["regime_up"]
        if spec.gated and not regime_ok:
            return False
        return (
            feature["failed_breakout_short"]
            and feature["close_pos"] <= 1.0 - params["close_pos_min"]
            and feature["body_r"] >= params["body_r_min"]
            and feature["dist_240"] >= params["dist_240_extreme"]
        )

    return False


def _resolve_exit(
    *,
    direction: str,
    execution: _FrameSeries,
    entry_index: int,
    hold_bars: int,
    stop_price: float,
    target_price: float,
) -> tuple[int, float, str]:
    last_index = min(entry_index + hold_bars - 1, len(execution.bars) - 1)
    for index in range(entry_index, last_index + 1):
        high = execution.highs[index]
        low = execution.lows[index]
        if direction == "LONG":
            stop_hit = low <= stop_price
            target_hit = high >= target_price
            if stop_hit and target_hit:
                return index, stop_price, "stop_first_conflict"
            if stop_hit:
                return index, stop_price, "stop"
            if target_hit:
                return index, target_price, "target"
        else:
            stop_hit = high >= stop_price
            target_hit = low <= target_price
            if stop_hit and target_hit:
                return index, stop_price, "stop_first_conflict"
            if stop_hit:
                return index, stop_price, "stop"
            if target_hit:
                return index, target_price, "target"
    return last_index, execution.closes[last_index], "time_exit"


def _build_candidate_summaries(
    *,
    specs: list[CandidateSpec],
    candidate_trades: dict[str, list[TradeRecord]],
    candidate_per_symbol: dict[str, dict[str, list[TradeRecord]]],
    selected_symbols: tuple[str, ...],
) -> list[CandidateSummary]:
    specs_by_id = {spec.candidate_id: spec for spec in specs}
    summaries: list[CandidateSummary] = []
    symbol_count = max(len(selected_symbols), 1)

    sibling_groups: dict[tuple[str, str, bool], list[str]] = defaultdict(list)
    pair_groups: dict[tuple[str, str, str], dict[bool, str]] = defaultdict(dict)
    for spec in specs:
        sibling_groups[(spec.family, spec.direction, spec.gated)].append(spec.candidate_id)
        pair_groups[(spec.family, spec.direction, spec.variant)][spec.gated] = spec.candidate_id

    expectancy_cache = {candidate_id: _expectancy([trade.r_multiple for trade in trades]) for candidate_id, trades in candidate_trades.items()}
    positive_symbol_share_cache = {
        candidate_id: (
            sum(1 for symbol in selected_symbols if _expectancy([trade.r_multiple for trade in candidate_per_symbol[candidate_id].get(symbol, [])]) > 0.0)
            / float(symbol_count)
        )
        for candidate_id in candidate_trades
    }

    for spec in specs:
        trades = candidate_trades[spec.candidate_id]
        per_symbol_rows = []
        for symbol in selected_symbols:
            symbol_trades = candidate_per_symbol[spec.candidate_id].get(symbol, [])
            if not symbol_trades:
                continue
            r_values = [trade.r_multiple for trade in symbol_trades]
            per_symbol_rows.append(
                SymbolCandidateSummary(
                    symbol=symbol,
                    trade_count=len(symbol_trades),
                    expectancy_r=_expectancy(r_values),
                    win_rate=_win_rate(r_values),
                    profit_factor=_profit_factor(r_values),
                    max_drawdown_r=_max_drawdown(r_values),
                    avg_holding_bars=_avg([trade.holding_bars for trade in symbol_trades]),
                    top_3_positive_share=_top_positive_share(r_values, 3),
                    walk_forward_positive_ratio=_walk_forward_positive_ratio(symbol_trades),
                )
            )
        per_symbol_rows.sort(key=lambda row: (-row.expectancy_r, -row.trade_count, row.symbol))

        r_values = [trade.r_multiple for trade in trades]
        expectancy = _expectancy(r_values)
        avg_win = _avg([value for value in r_values if value > 0.0])
        avg_loss = _avg([value for value in r_values if value < 0.0])
        median_symbol_expectancy = _avg([row.expectancy_r for row in per_symbol_rows])
        session_counts = CounterLike(trade.entry_session for trade in trades)
        regime_counts = CounterLike(trade.regime_label for trade in trades)
        walk_forward_positive_ratio = _walk_forward_positive_ratio(trades)
        cost_005 = _expectancy([value - 0.05 for value in r_values]) if r_values else 0.0
        cost_010 = _expectancy([value - 0.10 for value in r_values]) if r_values else 0.0

        ungated_id = pair_groups[(spec.family, spec.direction, spec.variant)].get(False)
        gated_id = pair_groups[(spec.family, spec.direction, spec.variant)].get(True)
        gating_lift_r = None
        gating_lift_positive_symbol_share = None
        if spec.gated and ungated_id is not None:
            gating_lift_r = expectancy - expectancy_cache[ungated_id]
            gating_lift_positive_symbol_share = positive_symbol_share_cache[spec.candidate_id] - positive_symbol_share_cache[ungated_id]
        elif not spec.gated and gated_id is not None:
            gating_lift_r = expectancy_cache[gated_id] - expectancy
            gating_lift_positive_symbol_share = positive_symbol_share_cache[gated_id] - positive_symbol_share_cache[spec.candidate_id]

        sibling_ids = sibling_groups[(spec.family, spec.direction, spec.gated)]
        parameter_neighbor_stability = None
        if len(sibling_ids) > 1:
            sibling_expectancies = [expectancy_cache[item] for item in sibling_ids]
            center = fmean(sibling_expectancies)
            spread = pstdev(sibling_expectancies) if len(sibling_expectancies) > 1 else 0.0
            parameter_neighbor_stability = 0.0 if center == 0.0 else max(0.0, 1.0 - min(abs(spread / center), 2.0))

        rank_score = _rank_score(
            expectancy=expectancy,
            positive_symbol_share=positive_symbol_share_cache[spec.candidate_id],
            walk_forward_positive_ratio=walk_forward_positive_ratio,
            trade_count=len(trades),
            max_drawdown=_max_drawdown(r_values),
            cost_005=cost_005,
            top_3_positive_share=_top_positive_share(r_values, 3),
            parameter_neighbor_stability=parameter_neighbor_stability,
            gated=spec.gated,
        )
        summaries.append(
            CandidateSummary(
                candidate_id=spec.candidate_id,
                family=spec.family,
                direction=spec.direction,
                gated=spec.gated,
                variant=spec.variant,
                rank_score=rank_score,
                trade_count=len(trades),
                expectancy_r=expectancy,
                win_rate=_win_rate(r_values),
                avg_win_r=avg_win,
                avg_loss_r=avg_loss,
                profit_factor=_profit_factor(r_values),
                max_drawdown_r=_max_drawdown(r_values),
                sharpe_proxy=_sharpe_proxy(r_values),
                avg_holding_bars=_avg([trade.holding_bars for trade in trades]),
                positive_symbol_share=positive_symbol_share_cache[spec.candidate_id],
                median_symbol_expectancy_r=median_symbol_expectancy,
                walk_forward_positive_ratio=walk_forward_positive_ratio,
                cost_expectancy_r_005=cost_005,
                cost_expectancy_r_010=cost_010,
                top_3_positive_share=_top_positive_share(r_values, 3),
                returns_without_top_1_r=sum(sorted(r_values)[:-1]) if len(r_values) > 1 else sum(r_values),
                regime_gating_lift_r=gating_lift_r,
                regime_gating_lift_positive_symbol_share=gating_lift_positive_symbol_share,
                parameter_neighbor_stability=parameter_neighbor_stability,
                best_symbols=tuple(row.symbol for row in per_symbol_rows[:5]),
                dominant_sessions=tuple(key for key, _ in session_counts.most_common(2)),
                dominant_regimes=tuple(key for key, _ in regime_counts.most_common(2)),
                interpretability_level=spec.interpretability_level,
                description=spec.description,
                feature_classes=spec.feature_classes,
                entry_logic=spec.entry_logic,
                exit_logic=spec.exit_logic,
                translation_feasibility=spec.translation_feasibility,
                per_symbol=tuple(per_symbol_rows),
            )
        )

    summaries.sort(key=lambda row: (-row.rank_score, -row.expectancy_r, -row.positive_symbol_share, row.candidate_id))
    return summaries


def _build_family_rollup(summaries: list[CandidateSummary]) -> list[dict[str, Any]]:
    grouped: dict[str, list[CandidateSummary]] = defaultdict(list)
    for row in summaries:
        grouped[row.family].append(row)
    payload = []
    for family, rows in grouped.items():
        best = max(rows, key=lambda item: item.rank_score)
        payload.append(
            {
                "family": family,
                "best_candidate_id": best.candidate_id,
                "best_rank_score": best.rank_score,
                "best_expectancy_r": best.expectancy_r,
                "positive_symbol_share": best.positive_symbol_share,
                "walk_forward_positive_ratio": best.walk_forward_positive_ratio,
                "dominant_sessions": list(best.dominant_sessions),
                "dominant_regimes": list(best.dominant_regimes),
                "best_symbols": list(best.best_symbols),
                "summary": best.description,
            }
        )
    payload.sort(key=lambda row: (-row["best_rank_score"], row["family"]))
    return payload


def _build_longer_horizon_importance_report(summaries: list[CandidateSummary]) -> dict[str, Any]:
    pair_rows = []
    grouped: dict[tuple[str, str, str], dict[bool, CandidateSummary]] = defaultdict(dict)
    for row in summaries:
        grouped[(row.family, row.direction, row.variant)][row.gated] = row
    lifts = []
    positive_symbol_lifts = []
    for key, pair in grouped.items():
        gated = pair.get(True)
        ungated = pair.get(False)
        if gated is None or ungated is None:
            continue
        expectancy_lift = gated.expectancy_r - ungated.expectancy_r
        positive_symbol_lift = gated.positive_symbol_share - ungated.positive_symbol_share
        lifts.append(expectancy_lift)
        positive_symbol_lifts.append(positive_symbol_lift)
        pair_rows.append(
            {
                "family": key[0],
                "direction": key[1],
                "variant": key[2],
                "gated_candidate": gated.candidate_id,
                "ungated_candidate": ungated.candidate_id,
                "expectancy_lift_r": expectancy_lift,
                "positive_symbol_share_lift": positive_symbol_lift,
                "gated_rank_score": gated.rank_score,
                "ungated_rank_score": ungated.rank_score,
            }
        )
    pair_rows.sort(key=lambda row: (-row["expectancy_lift_r"], row["family"], row["direction"], row["variant"]))
    return {
        "average_expectancy_lift_r": _avg(lifts),
        "median_expectancy_lift_r": _median(lifts),
        "average_positive_symbol_share_lift": _avg(positive_symbol_lifts),
        "pairwise_results": pair_rows,
        "conclusion": (
            "Longer-horizon gating improved the average candidate."
            if _avg(lifts) > 0.0
            else "Longer-horizon gating did not improve the average candidate in this first pass."
        ),
    }


def _build_instrument_affinities(
    symbol_family_best: dict[str, dict[str, tuple[float, str]]],
    summaries: list[CandidateSummary],
) -> list[dict[str, Any]]:
    summary_by_id = {row.candidate_id: row for row in summaries}
    payload = []
    for symbol, family_rows in symbol_family_best.items():
        ranked = []
        for family, (expectancy, candidate_id) in family_rows.items():
            summary = summary_by_id[candidate_id]
            ranked.append(
                {
                    "family": family,
                    "candidate_id": candidate_id,
                    "expectancy_r": expectancy,
                    "direction": summary.direction,
                    "gated": summary.gated,
                }
            )
        ranked.sort(key=lambda row: (-row["expectancy_r"], row["family"]))
        best = ranked[0] if ranked else None
        payload.append(
            {
                "symbol": symbol,
                "best_fit_family": best["family"] if best is not None else "none",
                "best_fit_candidate": best["candidate_id"] if best is not None else None,
                "ranked_family_fits": ranked,
            }
        )
    payload.sort(key=lambda row: row["symbol"])
    return payload


def _build_decision_shortlist(
    summaries: list[CandidateSummary],
    instrument_affinities: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    shortlist = []
    by_family_direction: set[tuple[str, str]] = set()
    qualified = [
        row
        for row in summaries
        if row.expectancy_r > 0.03
        and row.positive_symbol_share >= 0.40
        and row.walk_forward_positive_ratio >= 0.50
        and row.cost_expectancy_r_005 > 0.0
    ]
    source_rows = qualified or [row for row in summaries if row.expectancy_r > 0.0][:6]
    for row in source_rows:
        family_key = (row.family, row.direction)
        if family_key in by_family_direction:
            continue
        if len(shortlist) >= 6:
            break
        by_family_direction.add(family_key)
        symbol_rows = [item for item in instrument_affinities if item["best_fit_candidate"] == row.candidate_id]
        shortlist.append(
            {
                "candidate_id": row.candidate_id,
                "plain_english_description": row.description,
                "family": row.family,
                "direction": row.direction,
                "instruments": [item["symbol"] for item in symbol_rows[:8]] or list(row.best_symbols),
                "timeframes": {
                    "regime": ["240m", "720m", "1440m"],
                    "setup": ["60m"],
                    "execution": ["5m"],
                },
                "feature_classes": list(row.feature_classes),
                "entry_logic": row.entry_logic,
                "exit_logic": row.exit_logic,
                "holding_profile": f"Typical holding time {round(row.avg_holding_bars, 2)} bars; hard max {next(spec.hold_bars for spec in _build_candidate_specs() if spec.candidate_id == row.candidate_id)} bars.",
                "regime_dependence": {
                    "dominant_regimes": list(row.dominant_regimes),
                    "regime_gating_helped": row.regime_gating_lift_r is not None and row.regime_gating_lift_r > 0.0,
                    "gating_lift_r": row.regime_gating_lift_r,
                },
                "walk_forward_results": {
                    "positive_window_ratio": row.walk_forward_positive_ratio,
                    "positive_symbol_share": row.positive_symbol_share,
                },
                "promotion_readiness": (
                    "advance_to_second_pass"
                    if row.expectancy_r > 0.05 and row.positive_symbol_share >= 0.50 and row.walk_forward_positive_ratio >= 0.60
                    else "watchlist_only"
                ),
                "robustness_notes": {
                    "cost_005_expectancy_r": row.cost_expectancy_r_005,
                    "cost_010_expectancy_r": row.cost_expectancy_r_010,
                    "top_3_positive_share": row.top_3_positive_share,
                    "parameter_neighbor_stability": row.parameter_neighbor_stability,
                },
                "interpretability_level": row.interpretability_level,
                "translation_feasibility": row.translation_feasibility,
            }
        )
    return shortlist


def _build_dead_end_areas(family_rollup: list[dict[str, Any]]) -> list[dict[str, Any]]:
    dead = []
    for row in family_rollup:
        if row["best_expectancy_r"] > 0.0 and row["best_rank_score"] >= 25.0:
            continue
        dead.append(
            {
                "family": row["family"],
                "reason": "Best first-pass candidate was too weak on combined expectancy, breadth, or stability to justify immediate promotion work.",
            }
        )
    return dead


def _build_pattern_engine_relationship(
    *,
    family_rollup: list[dict[str, Any]],
    instrument_affinities: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    relationships = []
    family_map = {row["family"]: row for row in family_rollup}
    trend = family_map.get("trend_pullback_hybrid")
    breakout = family_map.get("breakout_acceptance")
    failed = family_map.get("failed_move_reversal")
    if trend is not None:
        trend_is_gated = _candidate_id_is_gated(str(trend["best_candidate_id"]))
        relationships.append(
            {
                "theme": "pause_resume_continuation",
                "assessment": (
                    "First-pass quant rules confirm the continuation intuition, but the hard regime filters used here were not the clean winner."
                    if not trend_is_gated
                    else "First-pass quant rules partially confirm the existing pause/resume intuition."
                ),
                "detail": (
                    "The stronger continuation result came from an ungated but still multi-horizon feature set, which suggests the next pass should soften regime context into a score rather than a hard allow/deny gate."
                    if not trend_is_gated
                    else "Longer-horizon directional context improved the continuation family enough to justify keeping it central in the next pass."
                ),
            }
        )
    if breakout is not None:
        breakout_is_gated = _candidate_id_is_gated(str(breakout["best_candidate_id"]))
        relationships.append(
            {
                "theme": "breakout_retest_hold",
                "assessment": "First-pass quant breakout logic suggests the current symbolic breakout families may compress into a smaller set of compression plus acceptance variables.",
                "detail": (
                    "Compression plus directional regime context explained more of the first-pass breakout behavior than any symbolic lane naming did."
                    if breakout_is_gated
                    else "Breakout behavior was visible, but the current regime gate appears too rigid and should become softer in the next pass."
                ),
            }
        )
    if failed is not None:
        failed_is_gated = _candidate_id_is_gated(str(failed["best_candidate_id"]))
        relationships.append(
            {
                "theme": "failed_move_reversal",
                "assessment": (
                    "The first-pass reversal rules work best when they are framed as failed excursion plus longer-horizon extension."
                    if failed_is_gated
                    else "The first-pass reversal rules only weakly improved over ungated reversal logic."
                ),
                "detail": (
                    "That points toward replacing brittle symbolic thresholds with excursion-failure state variables."
                    if failed_is_gated
                    else "The next pass should keep excursion-failure variables but reconsider how much hard higher-timeframe opposition is required."
                ),
            }
        )
    if not relationships:
        relationships.append(
            {
                "theme": "pattern_engine_priors",
                "assessment": "No strong quantitative confirmation emerged in this initial pass.",
                "detail": "The existing symbolic work should remain hypothesis input rather than operating truth until further discovery runs improve transfer.",
            }
        )
    return relationships


def _phase_1_design(*, execution_timeframe: str, symbols: tuple[str, ...]) -> dict[str, Any]:
    return {
        "architecture": {
            "research_store": "Use the persisted multi-timeframe SQLite bars as the canonical source of truth.",
            "dataset_layer": "Build a research-only loader that aligns 5m execution bars with 60m setup bars and 240m/720m/1440m regime bars.",
            "feature_layer": "Compute explicit multi-horizon state variables rather than symbolic families first.",
            "candidate_layer": "Start with interpretable rule templates across trend, breakout, mean reversion, and failed-move families.",
            "evaluation_layer": "Evaluate each candidate as a trading system with next-bar entry, volatility-scaled stop/target exits, cost sensitivity, walk-forward slices, and cross-instrument portability scoring.",
            "reporting_layer": "Write JSON plus Markdown artifacts with candidate rankings, dead ends, family summaries, and promotion-ready shortlists.",
        },
        "data_assumptions": {
            "execution_timeframe": execution_timeframe,
            "required_timeframes": [execution_timeframe, "60m", "240m", "720m", "1440m"],
            "universe": list(symbols),
            "assumption_notes": [
                "Intraday discovery uses the stored 5m bars for broad coverage.",
                "Longer-horizon context comes from already-derived higher timeframe bars in the same database.",
                "This first pass uses normalized R-multiples for cross-instrument comparability instead of instrument-specific dollar PnL.",
            ],
        },
        "feature_families": [
            "Long-horizon trend slope across 240m, 720m, and 1440m bars.",
            "Directional efficiency ratios to separate trend from noisy drift.",
            "Distance from higher-timeframe moving means as extension or pullback state.",
            "Compression and expansion on both 60m setup bars and 5m execution bars.",
            "Breakout distance versus prior local range.",
            "Failed excursion logic: new local extremes that close back inside range.",
            "Execution timing from 5m bar close position, short-term delta, and session label.",
        ],
        "target_and_trade_definition": {
            "entry": "Signal on bar close, enter next bar open.",
            "risk_unit": "5m average range over the prior 20 bars.",
            "exit": "Volatility-scaled stop, volatility-scaled target, or time exit at family-specific max hold.",
            "target_type": "Trading-system outcome in R, not classifier accuracy.",
        },
        "validation_framework": {
            "cross_instrument": "Require evidence across multiple futures, not only one home symbol.",
            "walk_forward": "Score sequential performance windows on the chronologically ordered trade stream.",
            "cost_sensitivity": "Subtract fixed 0.05R and 0.10R trade frictions.",
            "concentration": "Measure top-trade contribution and performance after removing the top winner.",
            "regime_ablation": "Compare longer-horizon-gated and ungated versions of the same candidate family.",
            "parameter_sensitivity": "Compare base and tight threshold variants within each family and side.",
        },
        "model_classes_to_test": [
            "Explicit rules first.",
            "Additive scorecards second if rules show a stable state basis but need smoother ranking.",
            "Sparse linear or shallow-tree classifiers only after rule templates identify credible state variables.",
            "Opaque models only if they materially outperform the explicit structures and remain diagnosable.",
        ],
        "ranking_framework": {
            "primary": [
                "Expectancy in R.",
                "Positive symbol share.",
                "Sequential walk-forward stability.",
                "Drawdown and return concentration penalties.",
                "Cost survival.",
                "Mild-threshold neighbor stability.",
            ],
            "secondary": [
                "Trade count sufficiency.",
                "Holding time practicality.",
                "Interpretability and rule-translation feasibility.",
                "Incremental value of longer-horizon gating.",
            ],
        },
        "recommended_order_of_operations": [
            "Start with explicit longer-horizon state variables and rule templates.",
            "Run a broad family scan across the futures universe.",
            "Keep both gated and ungated versions to quantify regime value rather than assume it.",
            "Promote only the best family-direction variants into a second pass with richer exits or additive scorecards.",
            "Revisit current symbolic pattern families only after identifying which state variables actually matter.",
        ],
    }


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Quant Futures Research Program",
        "",
        "## Phase 1 Research Design",
    ]
    design = report["phase_1_research_design"]
    lines.append("- Architecture:")
    for key, value in design["architecture"].items():
        lines.append(f"  - {key}: {value}")
    lines.append("- Data assumptions:")
    lines.append(f"  - Execution timeframe: {design['data_assumptions']['execution_timeframe']}")
    lines.append(f"  - Required timeframes: {', '.join(design['data_assumptions']['required_timeframes'])}")
    lines.append(f"  - Universe: {', '.join(design['data_assumptions']['universe'])}")
    for note in design["data_assumptions"]["assumption_notes"]:
        lines.append(f"  - {note}")
    lines.append("- Feature emphasis:")
    for item in design["feature_families"]:
        lines.append(f"  - {item}")
    lines.append("- Validation:")
    for key, value in design["validation_framework"].items():
        lines.append(f"  - {key}: {value}")
    lines.append("- Candidate model order:")
    for item in design["model_classes_to_test"]:
        lines.append(f"  - {item}")
    lines.append("- Implementation order:")
    for item in design["recommended_order_of_operations"]:
        lines.append(f"  - {item}")
    lines.extend(["", "## Broad Discovery", ""])

    longer_horizon = report["broad_discovery"]["longer_horizon_importance"]
    lines.append(
        f"- Longer-horizon gating average expectancy lift: {round(longer_horizon['average_expectancy_lift_r'], 4)}R"
    )
    lines.append(
        f"- Longer-horizon gating average positive-symbol-share lift: {round(longer_horizon['average_positive_symbol_share_lift'], 4)}"
    )
    lines.append(f"- Conclusion: {longer_horizon['conclusion']}")
    lines.append("")
    lines.append("### Best Families")
    for row in report["broad_discovery"]["family_rollup"][:6]:
        lines.append(
            f"- {row['family']}: best={row['best_candidate_id']}, score={round(row['best_rank_score'], 2)}, "
            f"expectancy={round(row['best_expectancy_r'], 4)}R, positive_symbol_share={round(row['positive_symbol_share'], 4)}, "
            f"dominant_sessions={', '.join(row['dominant_sessions'])}, best_symbols={', '.join(row['best_symbols'])}"
        )
    lines.append("")
    lines.append("### Dead Ends")
    dead_ends = report["broad_discovery"]["dead_end_areas"]
    if dead_ends:
        for row in dead_ends:
            lines.append(f"- {row['family']}: {row['reason']}")
    else:
        lines.append("- No major family was an obvious dead end in the first pass.")
    lines.append("")
    lines.append("### Pattern Engine Relationship")
    for row in report["broad_discovery"]["current_pattern_engine_relationship"]:
        lines.append(f"- {row['theme']}: {row['assessment']} {row['detail']}")
    lines.append("")
    lines.append("## Promotion Shortlist")
    for row in report["promotion_shortlist"]:
        lines.append(
            f"- {row['candidate_id']}: {row['plain_english_description']} "
            f"Instruments={', '.join(row['instruments'])}. "
            f"Entry={row['entry_logic']} Exit={row['exit_logic']} "
            f"Walk-forward positive ratio={round(row['walk_forward_results']['positive_window_ratio'], 4)} "
            f"Readiness={row['promotion_readiness']} "
            f"Interpretability={row['interpretability_level']} Translation={row['translation_feasibility']}."
        )
    return "\n".join(lines)


def _build_candidate_specs() -> list[CandidateSpec]:
    specs: list[CandidateSpec] = []
    family_descriptions = {
        "trend_pullback_hybrid": "Cross-timeframe continuation: long-horizon regime sets direction, 60m setup looks for a controlled pullback, and 5m closes supply the trigger.",
        "breakout_acceptance": "Compression-to-expansion breakout with longer-horizon directional context and a clean 5m acceptance close.",
        "regime_conditioned_mean_reversion": "Short-term stretch reversion, but only when the broader context is neutral enough or trend-compatible enough to allow mean reversion to work.",
        "failed_move_reversal": "Failed excursion reversal after a local breakout or breakdown closes back into range, preferably inside a longer-horizon extension state.",
    }
    entry_logic = {
        "trend_pullback_hybrid": "Require higher-timeframe directional agreement, a medium-horizon pullback, and a short-horizon reclaim or resumption bar.",
        "breakout_acceptance": "Require multi-bar compression, then a directional breakout close that holds through the short-horizon bar close.",
        "regime_conditioned_mean_reversion": "Fade short-term overextension only when broader regime conditions do not strongly oppose the reversion.",
        "failed_move_reversal": "Enter after a fresh excursion fails and the bar closes back inside the prior range with conviction.",
    }
    exit_logic = {
        "trend_pullback_hybrid": "1.1R stop, 2.0R target, or time exit after the continuation window expires.",
        "breakout_acceptance": "1.0R stop, 1.8R target, or time exit if acceptance stalls.",
        "regime_conditioned_mean_reversion": "0.9R stop, 1.2R target, or short holding-period time exit.",
        "failed_move_reversal": "1.0R stop, 1.5R target, or time exit if the rejection does not continue quickly.",
    }
    shared_features = {
        "trend_pullback_hybrid": ("multi_horizon_slope", "directional_efficiency", "pullback_depth", "5m_reclaim_trigger"),
        "breakout_acceptance": ("regime_slope", "compression_state", "breakout_distance", "5m_acceptance_close"),
        "regime_conditioned_mean_reversion": ("distance_to_mean", "regime_filter", "short_horizon_reversal_trigger"),
        "failed_move_reversal": ("excursion_failure", "extension_state", "reversal_close_position"),
    }
    params_by_family_variant = {
        "trend_pullback_hybrid": {
            "base": {"slope_60_min": 0.20, "pullback_min": 0.60, "pullback_max": 2.10, "close_pos_min": 0.57, "trigger_delta_min": 0.05, "dist_60_max": 1.10},
            "tight": {"slope_60_min": 0.30, "pullback_min": 0.75, "pullback_max": 1.80, "close_pos_min": 0.62, "trigger_delta_min": 0.10, "dist_60_max": 0.95},
        },
        "breakout_acceptance": {
            "base": {"compression_60_max": 1.05, "compression_5_max": 0.95, "breakout_min": 0.20, "close_pos_min": 0.62, "slope_60_min": 0.15},
            "tight": {"compression_60_max": 0.90, "compression_5_max": 0.82, "breakout_min": 0.30, "close_pos_min": 0.68, "slope_60_min": 0.20},
        },
        "regime_conditioned_mean_reversion": {
            "base": {"stretch_min": 1.20, "trend_floor": 0.50, "close_pos_min": 0.55, "trigger_delta_min": 0.02},
            "tight": {"stretch_min": 1.45, "trend_floor": 0.35, "close_pos_min": 0.60, "trigger_delta_min": 0.05},
        },
        "failed_move_reversal": {
            "base": {"close_pos_min": 0.60, "body_r_min": 0.20, "dist_240_extreme": 0.80},
            "tight": {"close_pos_min": 0.66, "body_r_min": 0.28, "dist_240_extreme": 1.05},
        },
    }
    hold_stop_target = {
        "trend_pullback_hybrid": (24, 1.10, 2.00),
        "breakout_acceptance": (18, 1.00, 1.80),
        "regime_conditioned_mean_reversion": (8, 0.90, 1.20),
        "failed_move_reversal": (12, 1.00, 1.50),
    }
    for family in family_descriptions:
        hold_bars, stop_r, target_r = hold_stop_target[family]
        for direction in ("LONG", "SHORT"):
            for gated in (False, True):
                for variant, params in params_by_family_variant[family].items():
                    candidate_id = f"{family}.{direction.lower()}.{'gated' if gated else 'ungated'}.{variant}"
                    specs.append(
                        CandidateSpec(
                            candidate_id=candidate_id,
                            family=family,
                            direction=direction,
                            gated=gated,
                            variant=variant,
                            interpretability_level="Level 1: directly interpretable",
                            description=family_descriptions[family],
                            feature_classes=shared_features[family],
                            entry_logic=entry_logic[family],
                            exit_logic=exit_logic[family],
                            translation_feasibility="High",
                            hold_bars=hold_bars,
                            stop_r=stop_r,
                            target_r=target_r,
                            params=params,
                        )
                    )
    return specs


def _rank_score(
    *,
    expectancy: float,
    positive_symbol_share: float,
    walk_forward_positive_ratio: float,
    trade_count: int,
    max_drawdown: float,
    cost_005: float,
    top_3_positive_share: float,
    parameter_neighbor_stability: float | None,
    gated: bool,
) -> float:
    expectancy_score = _clip((expectancy + 0.10) / 0.45, 0.0, 1.0)
    breadth_score = _clip(positive_symbol_share, 0.0, 1.0)
    stability_score = _clip(walk_forward_positive_ratio, 0.0, 1.0)
    trade_count_score = _clip(trade_count / 90.0, 0.0, 1.0)
    drawdown_penalty = _clip(max_drawdown / 18.0, 0.0, 1.0)
    cost_score = _clip((cost_005 + 0.05) / 0.40, 0.0, 1.0)
    concentration_penalty = _clip((top_3_positive_share - 0.45) / 0.55, 0.0, 1.0)
    neighbor_score = parameter_neighbor_stability if parameter_neighbor_stability is not None else 0.5
    gating_bonus = 0.05 if gated else 0.0
    score = (
        100.0
        * (
            0.28 * expectancy_score
            + 0.22 * breadth_score
            + 0.18 * stability_score
            + 0.10 * trade_count_score
            + 0.10 * cost_score
            + 0.07 * neighbor_score
            + gating_bonus
            - 0.10 * drawdown_penalty
            - 0.10 * concentration_penalty
        )
    )
    return round(score, 4)


def _session_label(bar: Bar) -> str:
    if bar.session_asia:
        return "ASIA"
    if bar.session_london:
        return "LONDON"
    if bar.session_us:
        return "US"
    return "UNKNOWN"


def _expectancy(values: list[float]) -> float:
    return _avg(values)


def _avg(values: list[float | int]) -> float:
    clean = [float(value) for value in values]
    return round(fmean(clean), 6) if clean else 0.0


def _median(values: list[float]) -> float:
    clean = [float(value) for value in values]
    return round(float(median(clean)), 6) if clean else 0.0


def _win_rate(values: list[float]) -> float:
    return round(sum(1 for value in values if value > 0.0) / float(len(values)), 6) if values else 0.0


def _profit_factor(values: list[float]) -> float:
    gains = sum(value for value in values if value > 0.0)
    losses = -sum(value for value in values if value < 0.0)
    if gains <= 0.0 and losses <= 0.0:
        return 0.0
    if losses <= 0.0:
        return 999.0
    return round(gains / losses, 6)


def _max_drawdown(values: list[float]) -> float:
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return round(max_drawdown, 6)


def _sharpe_proxy(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    sigma = pstdev(values)
    if sigma <= 0.0:
        return 0.0
    return round(fmean(values) / sigma * math.sqrt(len(values)), 6)


def _top_positive_share(values: list[float], count: int) -> float:
    positives = sorted((value for value in values if value > 0.0), reverse=True)
    total = sum(positives)
    if total <= 0.0:
        return 0.0
    return round(sum(positives[:count]) / total, 6)


def _walk_forward_positive_ratio(trades: list[TradeRecord]) -> float:
    if not trades:
        return 0.0
    ordered = sorted(trades, key=lambda row: row.entry_ts)
    bucket_size = max(len(ordered) // 4, 1)
    positive = 0
    total = 0
    for start in range(0, len(ordered), bucket_size):
        bucket = ordered[start : start + bucket_size]
        if not bucket:
            continue
        total += 1
        if _expectancy([row.r_multiple for row in bucket]) > 0.0:
            positive += 1
    return round(positive / float(total), 6) if total else 0.0


def _clip(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _candidate_id_is_gated(candidate_id: str) -> bool:
    parts = candidate_id.split(".")
    return len(parts) >= 3 and parts[2] == "gated"


class CounterLike:
    def __init__(self, values):
        counts: dict[str, int] = defaultdict(int)
        for value in values:
            counts[str(value)] += 1
        self._counts = counts

    def most_common(self, limit: int) -> list[tuple[str, int]]:
        rows = list(self._counts.items())
        rows.sort(key=lambda item: (-item[1], item[0]))
        return rows[:limit]
