"""Subclass diagnostics for the MGC impulse burst continuation family."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .mgc_impulse_burst_continuation_research import (
    COMMON_CONTEXT_TIMEFRAME,
    COMMON_DETECTION_TIMEFRAME,
    COMMON_SYMBOL,
    COMMON_WINDOW_DESCRIPTION,
    OUTPUT_DIR,
    Bar,
    _build_latest_context_lookup,
    _dominant_bucket_from_counter,
    _load_bars,
    _max_drawdown,
    _profit_factor,
    _rolling_atr,
    _rolling_mean,
    _rolling_realized_vol,
    _survives_without_top,
    _top_trade_share,
)
from .mgc_impulse_burst_continuation_second_pass import (
    REFINEMENT_SPECS,
    EntryTrade,
    _build_direct_trade_with_refined_exit,
    _detect_refined_impulse_event,
    _decision_bucket_second_pass,
    _find_bar_index_by_timestamp,
    _passes_context_filter,
)


TARGET_VARIANT = "breadth_plus_agreement_combo"
WINDOW_SIZE = 8


@dataclass(frozen=True)
class ClassifiedTrade:
    subclass_bucket: str
    trade: EntryTrade
    prior_run_10_norm: float
    prior_run_20_norm: float
    compression_ratio: float
    micro_breakout: bool


SUBCLASS_BUCKETS = (
    "FRESH_LAUNCH_FROM_COMPRESSION",
    "CONTINUATION_AFTER_SHALLOW_DRIFT",
    "LATE_EXTENSION_CHASE",
    "REVERSAL_BURST",
    "SPIKE_DOMINATED_OTHER",
)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_impulse_burst_subclass_diagnostics(symbol=args.symbol)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mgc-impulse-burst-subclass-diagnostics")
    parser.add_argument("--symbol", default=COMMON_SYMBOL)
    return parser


def run_impulse_burst_subclass_diagnostics(*, symbol: str) -> dict[str, Any]:
    spec = next(spec for spec in REFINEMENT_SPECS if spec.variant_name == TARGET_VARIANT)
    one_minute_bars = _load_bars(symbol=symbol, timeframe=COMMON_DETECTION_TIMEFRAME)
    five_minute_bars = _load_bars(symbol=symbol, timeframe=COMMON_CONTEXT_TIMEFRAME)
    overlap_start = max(one_minute_bars[0].timestamp, five_minute_bars[0].timestamp)
    overlap_end = min(one_minute_bars[-1].timestamp, five_minute_bars[-1].timestamp)
    one_minute = [bar for bar in one_minute_bars if overlap_start <= bar.timestamp <= overlap_end]
    five_minute = [bar for bar in five_minute_bars if overlap_start <= bar.timestamp <= overlap_end]

    atr_1m = _rolling_atr(one_minute, length=14)
    rv_1m = _rolling_realized_vol(one_minute, length=20)
    vol_baseline_1m = _rolling_mean([bar.volume for bar in one_minute], length=20)
    atr_5m = _rolling_atr(five_minute, length=14)
    context_lookup = _build_latest_context_lookup(one_minute=one_minute, five_minute=five_minute)

    classified = _collect_classified_trades(
        bars_1m=one_minute,
        bars_5m=five_minute,
        atr_1m=atr_1m,
        rv_1m=rv_1m,
        vol_baseline_1m=vol_baseline_1m,
        atr_5m=atr_5m,
        context_lookup=context_lookup,
        spec=spec,
    )
    subclass_rows = [_subclass_row(bucket=bucket, trades=[row for row in classified if row.subclass_bucket == bucket]) for bucket in SUBCLASS_BUCKETS]
    diagnosis = _build_diagnosis(subclass_rows)
    payload = {
        "symbol": symbol,
        "family_name": "impulse_burst_continuation",
        "base_variant": TARGET_VARIANT,
        "sample_start_date": overlap_start.isoformat(),
        "sample_end_date": overlap_end.isoformat(),
        "history_window_type": COMMON_WINDOW_DESCRIPTION,
        "exact_subclass_rules_used": _subclass_rules(),
        "subclass_results": subclass_rows,
        "diagnosis": diagnosis,
    }
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUTPUT_DIR / "mgc_impulse_burst_subclass_diagnostics.json"
    md_path = OUTPUT_DIR / "mgc_impulse_burst_subclass_diagnostics.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "mgc_impulse_burst_subclass_diagnostics",
        "artifact_paths": {"json": str(json_path), "markdown": str(md_path)},
        "diagnosis": diagnosis,
    }


def _collect_classified_trades(
    *,
    bars_1m: list[Bar],
    bars_5m: list[Bar],
    atr_1m: list[float | None],
    rv_1m: list[float | None],
    vol_baseline_1m: list[float | None],
    atr_5m: list[float | None],
    context_lookup: list[int | None],
    spec: Any,
) -> list[ClassifiedTrade]:
    rows: list[ClassifiedTrade] = []
    index = max(WINDOW_SIZE, 20)
    next_available_index = index
    while index < len(bars_1m) - 9:
        if index < next_available_index:
            index += 1
            continue
        impulse = _detect_refined_impulse_event(
            bars=bars_1m,
            atr_1m=atr_1m,
            rv_1m=rv_1m,
            vol_baseline_1m=vol_baseline_1m,
            index=index,
            spec=spec,
        )
        if impulse is None:
            index += 1
            continue
        if not _passes_context_filter(
            bars_5m=bars_5m,
            atr_5m=atr_5m,
            context_lookup=context_lookup,
            one_minute_index=index,
            direction=impulse["direction"],
            threshold=0.35,
            body_share_min=2.0 / 3.0,
        ):
            index += WINDOW_SIZE
            continue
        trade = _build_direct_trade_with_refined_exit(bars=bars_1m, signal_index=index, impulse=impulse, fast_failure_exit=False)
        if trade is None:
            index += WINDOW_SIZE
            continue
        direction_sign = 1.0 if trade.direction == "LONG" else -1.0
        prior_run_10_norm = _prior_run_norm(bars_1m, atr_1m, rv_1m, index=index, lookback=10, direction_sign=direction_sign)
        prior_run_20_norm = _prior_run_norm(bars_1m, atr_1m, rv_1m, index=index, lookback=20, direction_sign=direction_sign)
        compression_ratio = _pre_burst_compression_ratio(bars_1m, index=index)
        micro_breakout = _micro_breakout(bars_1m, index=index, direction=trade.direction)
        bucket = _classify_subclass(
            prior_run_10_norm=prior_run_10_norm,
            prior_run_20_norm=prior_run_20_norm,
            compression_ratio=compression_ratio,
            micro_breakout=micro_breakout,
            largest_bar_share=trade.largest_bar_share,
        )
        rows.append(
            ClassifiedTrade(
                subclass_bucket=bucket,
                trade=trade,
                prior_run_10_norm=prior_run_10_norm,
                prior_run_20_norm=prior_run_20_norm,
                compression_ratio=compression_ratio,
                micro_breakout=micro_breakout,
            )
        )
        exit_index = _find_bar_index_by_timestamp(bars_1m, trade.exit_ts)
        next_available_index = max(index + WINDOW_SIZE, exit_index + 1)
        index += WINDOW_SIZE
    return rows


def _prior_run_norm(
    bars: list[Bar],
    atr_1m: list[float | None],
    rv_1m: list[float | None],
    *,
    index: int,
    lookback: int,
    direction_sign: float,
) -> float:
    start = index - lookback
    if start < 0:
        return 0.0
    net = direction_sign * (bars[index].close - bars[start].close)
    atr_value = atr_1m[index] or 0.0
    rv_value = rv_1m[index] or 0.0
    scale = max(atr_value * math.sqrt(lookback), rv_value * math.sqrt(lookback), 0.1)
    return round(net / scale, 4)


def _pre_burst_compression_ratio(bars: list[Bar], *, index: int) -> float:
    prior_8 = bars[max(0, index - 8) : index]
    prior_20 = bars[max(0, index - 20) : index]
    if not prior_8 or not prior_20:
        return 1.0
    range_8 = max(bar.high for bar in prior_8) - min(bar.low for bar in prior_8)
    range_20 = max(bar.high for bar in prior_20) - min(bar.low for bar in prior_20)
    return round(range_8 / max(range_20, 0.1), 4)


def _micro_breakout(bars: list[Bar], *, index: int, direction: str) -> bool:
    prior = bars[max(0, index - 8) : index]
    if not prior:
        return False
    current = bars[index]
    if direction == "LONG":
        return current.close > max(bar.high for bar in prior)
    return current.close < min(bar.low for bar in prior)


def _classify_subclass(
    *,
    prior_run_10_norm: float,
    prior_run_20_norm: float,
    compression_ratio: float,
    micro_breakout: bool,
    largest_bar_share: float,
) -> str:
    if prior_run_10_norm <= -0.60 or prior_run_20_norm <= -0.85:
        return "REVERSAL_BURST"
    if prior_run_20_norm >= 1.40 and prior_run_10_norm >= 0.80:
        return "LATE_EXTENSION_CHASE"
    if micro_breakout and compression_ratio <= 0.65 and prior_run_10_norm < 0.80:
        return "FRESH_LAUNCH_FROM_COMPRESSION"
    if 0.15 <= prior_run_10_norm < 0.80 and compression_ratio <= 1.10:
        return "CONTINUATION_AFTER_SHALLOW_DRIFT"
    if largest_bar_share >= 0.40:
        return "SPIKE_DOMINATED_OTHER"
    return "SPIKE_DOMINATED_OTHER"


def _subclass_row(*, bucket: str, trades: list[ClassifiedTrade]) -> dict[str, Any]:
    pnls = [row.trade.pnl for row in trades]
    phase_counts = Counter(row.trade.time_of_day_phase for row in trades)
    dominant_phase, dominant_share = _dominant_bucket_from_counter(phase_counts)
    return {
        "subclass_bucket": bucket,
        "trades": len(trades),
        "realized_pnl": round(sum(pnls), 4),
        "avg_trade": round(statistics.fmean(pnls), 4) if pnls else None,
        "median_trade": round(statistics.median(pnls), 4) if pnls else None,
        "profit_factor": _profit_factor(pnls),
        "max_drawdown": _max_drawdown(pnls),
        "top_1_contribution": _top_trade_share(pnls, top_n=1),
        "top_3_contribution": _top_trade_share(pnls, top_n=3),
        "survives_without_top_1": _survives_without_top(pnls, top_n=1),
        "survives_without_top_3": _survives_without_top(pnls, top_n=3),
        "false_start_rate": round(sum(1 for row in trades if row.trade.false_start) / len(trades), 4) if trades else None,
        "first_2_bars_continuation_success_rate": round(sum(1 for row in trades if not row.trade.first_2_bars_failure) / len(trades), 4) if trades else None,
        "dominant_time_of_day_phase": dominant_phase,
        "dominant_time_of_day_phase_share": dominant_share,
        "avg_prior_run_10_norm": round(statistics.fmean([row.prior_run_10_norm for row in trades]), 4) if trades else None,
        "avg_prior_run_20_norm": round(statistics.fmean([row.prior_run_20_norm for row in trades]), 4) if trades else None,
        "avg_compression_ratio": round(statistics.fmean([row.compression_ratio for row in trades]), 4) if trades else None,
    }


def _build_diagnosis(rows: list[dict[str, Any]]) -> dict[str, Any]:
    nonempty = [row for row in rows if row["trades"] > 0]
    total_trades = sum(row["trades"] for row in nonempty)
    min_clean_trades = max(12, int(math.ceil(total_trades * 0.08))) if total_trades else 12
    carrier = max(nonempty, key=lambda row: row["realized_pnl"], default=None)
    poison = min(nonempty, key=lambda row: row["realized_pnl"], default=None)
    negative_median_source = max(
        [row for row in nonempty if (row["median_trade"] or 0) < 0],
        key=lambda row: row["trades"],
        default=None,
    )
    clean = next(
        (
            row
            for row in sorted(nonempty, key=lambda row: (-(row["profit_factor"] or 0), row["top_3_contribution"] or 999999))
            if row["trades"] >= min_clean_trades
            and (row["profit_factor"] or 0) >= 1.10
            and (row["median_trade"] or 0) >= 0
            and (row["top_3_contribution"] or 999999) < 170
            and row["survives_without_top_1"]
        ),
        None,
    )
    thin_positive = next(
        (
            row
            for row in sorted(nonempty, key=lambda row: (-row["realized_pnl"], -row["trades"]))
            if row["trades"] < min_clean_trades
            and (row["realized_pnl"] or 0) > 0
            and (row["median_trade"] or 0) >= 0
        ),
        None,
    )
    return {
        "carrier_subclass": carrier["subclass_bucket"] if carrier else None,
        "poison_subclass": poison["subclass_bucket"] if poison else None,
        "negative_median_trade_main_source": negative_median_source["subclass_bucket"] if negative_median_source else None,
        "clean_subclass_candidate": clean["subclass_bucket"] if clean else None,
        "thin_positive_subclass_candidate": thin_positive["subclass_bucket"] if thin_positive else None,
        "clean_subclass_min_trades_required": min_clean_trades,
        "worth_narrowing_to_one_subclass": clean is not None,
        "family_verdict": (
            "NARROW_TO_CLEAN_SUBCLASS"
            if clean is not None
            else "CONCEPT_STILL_TOO_MIXED"
        ),
    }


def _subclass_rules() -> dict[str, Any]:
    return {
        "base_variant": TARGET_VARIANT,
        "prior_run_size": {
            "prior_run_10_norm": "signed net move over prior 10 1m bars / max(ATR14*sqrt(10), realized-vol20*sqrt(10))",
            "prior_run_20_norm": "signed net move over prior 20 1m bars / max(ATR14*sqrt(20), realized-vol20*sqrt(20))",
        },
        "pre_burst_compression_vs_expansion": "prior 8-bar range / prior 20-bar range",
        "local_launch_state_rules": {
            "REVERSAL_BURST": "prior_run_10_norm <= -0.60 or prior_run_20_norm <= -0.85",
            "LATE_EXTENSION_CHASE": "prior_run_20_norm >= 1.40 and prior_run_10_norm >= 0.80",
            "FRESH_LAUNCH_FROM_COMPRESSION": "micro breakout from prior 8-bar range, compression_ratio <= 0.65, and prior_run_10_norm < 0.80",
            "CONTINUATION_AFTER_SHALLOW_DRIFT": "0.15 <= prior_run_10_norm < 0.80 and compression_ratio <= 1.10",
            "SPIKE_DOMINATED_OTHER": "fallback bucket, including remaining high-concentration or ambiguous bursts",
        },
        "post_entry_follow_through_fields": {
            "first_1_bar_failure": "first post-entry bar does not continue at least 10% of burst size and remains flat/against direction",
            "first_2_bars_failure": "first two post-entry bars do not continue at least 20% of burst size and remain flat/against direction",
        },
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# MGC Impulse Burst Subclass Diagnostics",
        "",
        f"Base variant: {payload['base_variant']}",
        f"Family verdict: {payload['diagnosis']['family_verdict']}",
        "",
        "## Subclass Results",
        "",
    ]
    for row in payload["subclass_results"]:
        lines.append(
            f"- {row['subclass_bucket']}: trades={row['trades']}, pnl={row['realized_pnl']}, avg={row['avg_trade']}, "
            f"median={row['median_trade']}, pf={row['profit_factor']}, dd={row['max_drawdown']}, "
            f"top3={row['top_3_contribution']}, false_start={row['false_start_rate']}, first2_success={row['first_2_bars_continuation_success_rate']}"
        )
    lines.extend(["", "## Diagnosis", ""])
    for key, value in payload["diagnosis"].items():
        lines.append(f"- {key}: {value}")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
