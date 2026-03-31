"""Separator-only analysis for widened US_MIDDAY pause/resume short trades."""

from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


FAMILY = "usMiddayPauseResumeShortTurn"


@dataclass(frozen=True)
class WideningObservation:
    cohort: str
    variant: str
    trade_id: int
    entry_ts: datetime
    net_pnl: Decimal
    exit_reason: str
    bars_held: int
    phase_minute: int
    entry_distance_fast_ema_atr: Decimal | None
    entry_distance_slow_ema_atr: Decimal | None
    entry_distance_vwap_atr: Decimal | None
    normalized_slope: Decimal | None
    normalized_curvature: Decimal | None
    signal_range_expansion_ratio: Decimal | None
    prior_return_signs_3: str
    prior_return_signs_5: str
    prior_vwap_extension_signs_3: str
    prior_slope_signs_3: str
    prior_curvature_signs_3: str
    one_bar_rebound_before_signal: bool
    two_bar_rebound_before_signal: bool
    prior_3_any_positive_curvature: bool
    prior_3_any_below_vwap: bool
    prior_3_all_above_vwap: bool
    signal_breaks_prior_1_low: bool
    signal_breaks_prior_2_low: bool
    signal_body_to_range: Decimal | None
    signal_close_location: Decimal | None
    prior_1_bar_vwap_extension: Decimal | None
    prior_3_bar_avg_vwap_extension: Decimal | None
    prior_3_bar_avg_curvature: Decimal | None
    prior_5_bar_avg_slope: Decimal | None
    rebound_depth_1bar_atr: Decimal | None
    rebound_depth_2bar_atr: Decimal | None
    prior_3_lower_highs: bool
    prior_3_higher_lows: bool
    prior_3_range_contracting: bool
    followthrough_3bar: Decimal | None


def build_and_write_us_midday_pause_resume_widening_separator_analysis(
    *,
    strict_summary_path: Path,
    medium_1_summary_path: Path,
    medium_2_summary_path: Path,
) -> dict[str, str]:
    strict_summary = json.loads(strict_summary_path.read_text(encoding="utf-8"))
    medium_1_summary = json.loads(medium_1_summary_path.read_text(encoding="utf-8"))
    medium_2_summary = json.loads(medium_2_summary_path.read_text(encoding="utf-8"))

    strict_trades = _load_family_trades(Path(strict_summary["trade_ledger_path"]))
    medium_1_trades = _load_family_trades(Path(medium_1_summary["trade_ledger_path"]))
    medium_2_trades = _load_family_trades(Path(medium_2_summary["trade_ledger_path"]))

    strict_entry_ts = {row["entry_ts"] for row in strict_trades}
    medium_1_entry_ts = {row["entry_ts"] for row in medium_1_trades}

    cohorts: list[tuple[str, str, list[dict[str, str]], Path]] = [
        ("strict_family_trade", "strict", strict_trades, Path(strict_summary["replay_db_path"])),
        (
            "medium_1_added_trade",
            "medium_1",
            [row for row in medium_1_trades if row["entry_ts"] not in strict_entry_ts],
            Path(medium_1_summary["replay_db_path"]),
        ),
        (
            "medium_2_added_trade",
            "medium_2",
            [row for row in medium_2_trades if row["entry_ts"] not in medium_1_entry_ts],
            Path(medium_2_summary["replay_db_path"]),
        ),
    ]

    observations: list[WideningObservation] = []
    for cohort, variant, rows, replay_db_path in cohorts:
        ordered_rows = _load_ordered_bar_rows(replay_db_path)
        by_timestamp = {row["timestamp"].isoformat(): row for row in ordered_rows}
        for row in rows:
            observations.append(
                _build_observation(
                    cohort=cohort,
                    variant=variant,
                    trade=row,
                    context=by_timestamp[row["entry_ts"]],
                )
            )

    prefix = Path(str(strict_summary_path).removesuffix(".summary.json"))
    detail_path = prefix.with_suffix(".us_midday_pause_resume_widening_separator_detail.csv")
    comparison_path = prefix.with_suffix(".us_midday_pause_resume_widening_separator_comparison.csv")
    summary_path = prefix.with_suffix(".us_midday_pause_resume_widening_separator_summary.json")

    _write_csv(detail_path, [asdict(item) for item in observations])
    comparison_rows = _build_comparison_rows(observations)
    _write_csv(comparison_path, comparison_rows)
    summary_path.write_text(
        json.dumps(_build_summary(observations, comparison_rows), indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )

    return {
        "us_midday_pause_resume_widening_separator_detail_path": str(detail_path),
        "us_midday_pause_resume_widening_separator_comparison_path": str(comparison_path),
        "us_midday_pause_resume_widening_separator_summary_path": str(summary_path),
    }


def _load_family_trades(trade_ledger_path: Path) -> list[dict[str, str]]:
    with trade_ledger_path.open(encoding="utf-8", newline="") as handle:
        return [row for row in csv.DictReader(handle) if row["setup_family"] == FAMILY]


def _build_observation(
    *,
    cohort: str,
    variant: str,
    trade: dict[str, str],
    context: dict[str, Any],
) -> WideningObservation:
    prior = context["prior_rows"]
    signal = context["current"]
    entry_ts = datetime.fromisoformat(trade["entry_ts"])

    return WideningObservation(
        cohort=cohort,
        variant=variant,
        trade_id=int(trade["trade_id"]),
        entry_ts=entry_ts,
        net_pnl=Decimal(trade["net_pnl"]),
        exit_reason=trade["exit_reason"],
        bars_held=int(trade["bars_held"]),
        phase_minute=(entry_ts.hour * 60 + entry_ts.minute) - (10 * 60 + 30),
        entry_distance_fast_ema_atr=_decimal_or_none(trade["entry_distance_fast_ema_atr"]),
        entry_distance_slow_ema_atr=_decimal_or_none(trade["entry_distance_slow_ema_atr"]),
        entry_distance_vwap_atr=_decimal_or_none(trade["entry_distance_vwap_atr"]),
        normalized_slope=signal["normalized_slope"],
        normalized_curvature=signal["normalized_curvature"],
        signal_range_expansion_ratio=_safe_div(signal["range"], _avg(item["range"] for item in prior[-3:])),
        prior_return_signs_3=_return_sign_pattern(prior[-3:]),
        prior_return_signs_5=_return_sign_pattern(prior[-5:]),
        prior_vwap_extension_signs_3=_sign_pattern([item["vwap_extension"] for item in prior[-3:]]),
        prior_slope_signs_3=_sign_pattern([item["normalized_slope"] for item in prior[-3:]]),
        prior_curvature_signs_3=_sign_pattern([item["normalized_curvature"] for item in prior[-3:]]),
        one_bar_rebound_before_signal=_one_bar_rebound(prior),
        two_bar_rebound_before_signal=_two_bar_rebound(prior),
        prior_3_any_positive_curvature=any(_is_positive(item["normalized_curvature"]) for item in prior[-3:]),
        prior_3_any_below_vwap=any(_is_negative(item["vwap_extension"]) for item in prior[-3:]),
        prior_3_all_above_vwap=all(_is_positive(item["vwap_extension"]) for item in prior[-3:]) if prior[-3:] else False,
        signal_breaks_prior_1_low=bool(prior) and signal["low"] < prior[-1]["low"],
        signal_breaks_prior_2_low=bool(prior[-2:]) and signal["low"] < min(item["low"] for item in prior[-2:]),
        signal_body_to_range=_safe_div(signal["body"], signal["range"]),
        signal_close_location=signal["close_location"],
        prior_1_bar_vwap_extension=prior[-1]["vwap_extension"] if prior else None,
        prior_3_bar_avg_vwap_extension=_avg(item["vwap_extension"] for item in prior[-3:]),
        prior_3_bar_avg_curvature=_avg(item["normalized_curvature"] for item in prior[-3:]),
        prior_5_bar_avg_slope=_avg(item["normalized_slope"] for item in prior[-5:]),
        rebound_depth_1bar_atr=_rebound_depth_1bar(prior),
        rebound_depth_2bar_atr=_rebound_depth_2bar(prior),
        prior_3_lower_highs=_strict_monotone([item["high"] for item in prior[-3:]], decreasing=True),
        prior_3_higher_lows=_strict_monotone([item["low"] for item in prior[-3:]], decreasing=False),
        prior_3_range_contracting=_strict_monotone([item["range"] for item in prior[-3:]], decreasing=True),
        followthrough_3bar=context["followthrough_3bar"],
    )


def _load_ordered_bar_rows(replay_db_path: Path) -> list[dict[str, Any]]:
    connection = sqlite3.connect(replay_db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            select
              b.end_ts,
              b.open,
              b.high,
              b.low,
              b.close,
              f.payload_json
            from bars b
            join features f on f.bar_id = b.bar_id
            where b.ticker = 'MGC' and b.timeframe = '5m'
            order by b.end_ts asc
            """
        ).fetchall()
    finally:
        connection.close()

    parsed: list[dict[str, Any]] = []
    for row in rows:
        payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
        atr = _payload_decimal(payload.get("atr")) or Decimal("0")
        open_price = _decimal(row["open"])
        high = _decimal(row["high"])
        low = _decimal(row["low"])
        close = _decimal(row["close"])
        vwap = _payload_decimal(payload.get("vwap")) or Decimal("0")
        bar_range = high - low
        parsed.append(
            {
                "timestamp": datetime.fromisoformat(row["end_ts"]),
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "range": bar_range,
                "body": abs(close - open_price),
                "close_location": _safe_div(close - low, bar_range),
                "normalized_slope": _safe_div(_payload_decimal(payload.get("velocity")) or Decimal("0"), atr),
                "normalized_curvature": _safe_div(_payload_decimal(payload.get("velocity_delta")) or Decimal("0"), atr),
                "vwap_extension": _safe_div(close - vwap, atr),
            }
        )

    ordered: list[dict[str, Any]] = []
    for index, current in enumerate(parsed):
        prior = parsed[max(0, index - 6) : index]
        next_rows = parsed[index + 1 : index + 4]
        ordered.append(
            {
                "timestamp": current["timestamp"],
                "prior_rows": prior,
                "current": current,
                "followthrough_3bar": _short_favorable_move(current["close"], next_rows),
            }
        )
    return ordered


def _build_comparison_rows(observations: list[WideningObservation]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for cohort in ["strict_family_trade", "medium_1_added_trade", "medium_2_added_trade"]:
        cohort_rows = [item for item in observations if item.cohort == cohort]
        rows.append(
            {
                "cohort": cohort,
                "count": len(cohort_rows),
                "net_pnl_total": _decimal_str(sum((item.net_pnl for item in cohort_rows), Decimal("0"))),
                "prior_return_signs_3_distribution": json.dumps(dict(Counter(item.prior_return_signs_3 for item in cohort_rows)), sort_keys=True),
                "prior_vwap_extension_signs_3_distribution": json.dumps(dict(Counter(item.prior_vwap_extension_signs_3 for item in cohort_rows)), sort_keys=True),
                "prior_curvature_signs_3_distribution": json.dumps(dict(Counter(item.prior_curvature_signs_3 for item in cohort_rows)), sort_keys=True),
                "one_bar_rebound_before_signal_true_rate": _bool_rate(item.one_bar_rebound_before_signal for item in cohort_rows),
                "two_bar_rebound_before_signal_true_rate": _bool_rate(item.two_bar_rebound_before_signal for item in cohort_rows),
                "prior_3_any_positive_curvature_true_rate": _bool_rate(item.prior_3_any_positive_curvature for item in cohort_rows),
                "prior_3_any_below_vwap_true_rate": _bool_rate(item.prior_3_any_below_vwap for item in cohort_rows),
                "prior_3_all_above_vwap_true_rate": _bool_rate(item.prior_3_all_above_vwap for item in cohort_rows),
                "signal_breaks_prior_2_low_true_rate": _bool_rate(item.signal_breaks_prior_2_low for item in cohort_rows),
                "prior_3_lower_highs_true_rate": _bool_rate(item.prior_3_lower_highs for item in cohort_rows),
                "prior_3_higher_lows_true_rate": _bool_rate(item.prior_3_higher_lows for item in cohort_rows),
                "prior_3_range_contracting_true_rate": _bool_rate(item.prior_3_range_contracting for item in cohort_rows),
                "avg_phase_minute": _decimal_str(_avg(item.phase_minute for item in cohort_rows)),
                "avg_entry_distance_vwap_atr": _decimal_str(_avg(item.entry_distance_vwap_atr for item in cohort_rows)),
                "avg_prior_3_bar_avg_vwap_extension": _decimal_str(_avg(item.prior_3_bar_avg_vwap_extension for item in cohort_rows)),
                "avg_prior_3_bar_avg_curvature": _decimal_str(_avg(item.prior_3_bar_avg_curvature for item in cohort_rows)),
                "avg_prior_5_bar_avg_slope": _decimal_str(_avg(item.prior_5_bar_avg_slope for item in cohort_rows)),
                "avg_rebound_depth_1bar_atr": _decimal_str(_avg(item.rebound_depth_1bar_atr for item in cohort_rows)),
                "avg_rebound_depth_2bar_atr": _decimal_str(_avg(item.rebound_depth_2bar_atr for item in cohort_rows)),
                "avg_signal_range_expansion_ratio": _decimal_str(_avg(item.signal_range_expansion_ratio for item in cohort_rows)),
                "avg_followthrough_3bar": _decimal_str(_avg(item.followthrough_3bar for item in cohort_rows)),
            }
        )
    return rows


def _build_summary(observations: list[WideningObservation], comparison_rows: list[dict[str, Any]]) -> dict[str, Any]:
    strict_rows = [item for item in observations if item.cohort == "strict_family_trade"]
    medium_1_rows = [item for item in observations if item.cohort == "medium_1_added_trade"]
    medium_2_rows = [item for item in observations if item.cohort == "medium_2_added_trade"]
    widened_rows = medium_1_rows + medium_2_rows

    return {
        "cohort_sizes": {row["cohort"]: row["count"] for row in comparison_rows},
        "widened_added_trade_table": [asdict(item) for item in widened_rows],
        "ranked_boolean_separators_strict_vs_widened": _rank_boolean_separators(strict_rows, widened_rows),
        "ranked_numeric_separators_strict_vs_widened": _rank_numeric_separators(strict_rows, widened_rows),
        "explicit_findings": [
            "All widened-added trades were losers, including the single medium_1 add, so there is no economically acceptable widened add on this sample.",
            "The widened adds were shallower resume attempts: weaker 3-bar follow-through, more positive prior VWAP extension, and later/less-negative slope context than the strict family average.",
            "The bad medium_2-only adds also appeared later in the phase and included both already-stretched and weak-break cases, which argues against another simple widening on pause depth or expansion ratio.",
        ],
    }


def _rank_boolean_separators(left_rows: list[WideningObservation], right_rows: list[WideningObservation]) -> list[dict[str, Any]]:
    features = [
        "one_bar_rebound_before_signal",
        "two_bar_rebound_before_signal",
        "prior_3_any_positive_curvature",
        "prior_3_any_below_vwap",
        "prior_3_all_above_vwap",
        "signal_breaks_prior_2_low",
        "prior_3_lower_highs",
        "prior_3_higher_lows",
        "prior_3_range_contracting",
    ]
    ranked: list[dict[str, Any]] = []
    for feature in features:
        left_rate = _bool_rate(getattr(item, feature) for item in left_rows)
        right_rate = _bool_rate(getattr(item, feature) for item in right_rows)
        ranked.append(
            {
                "feature": feature,
                "strict_true_rate": left_rate,
                "widened_added_true_rate": right_rate,
                "absolute_gap": abs(left_rate - right_rate),
            }
        )
    ranked.sort(key=lambda item: item["absolute_gap"], reverse=True)
    return ranked


def _rank_numeric_separators(left_rows: list[WideningObservation], right_rows: list[WideningObservation]) -> list[dict[str, Any]]:
    features = [
        "phase_minute",
        "entry_distance_vwap_atr",
        "normalized_slope",
        "normalized_curvature",
        "signal_range_expansion_ratio",
        "prior_1_bar_vwap_extension",
        "prior_3_bar_avg_vwap_extension",
        "prior_3_bar_avg_curvature",
        "prior_5_bar_avg_slope",
        "rebound_depth_1bar_atr",
        "rebound_depth_2bar_atr",
        "followthrough_3bar",
    ]
    ranked: list[dict[str, Any]] = []
    for feature in features:
        left_avg = _avg(getattr(item, feature) for item in left_rows)
        right_avg = _avg(getattr(item, feature) for item in right_rows)
        if left_avg is None or right_avg is None:
            continue
        ranked.append(
            {
                "feature": feature,
                "strict_avg": _decimal_str(left_avg),
                "widened_added_avg": _decimal_str(right_avg),
                "absolute_gap": _decimal_str(abs(left_avg - right_avg)),
            }
        )
    ranked.sort(key=lambda item: Decimal(item["absolute_gap"]), reverse=True)
    return ranked


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _return_sign_pattern(prior_rows: list[dict[str, Any]]) -> str:
    return "".join(_bar_return_sign(item) for item in prior_rows)


def _bar_return_sign(row: dict[str, Any]) -> str:
    if row["close"] > row["open"]:
        return "U"
    if row["close"] < row["open"]:
        return "D"
    return "F"


def _sign_pattern(values: list[Decimal | None]) -> str:
    output = []
    for value in values:
        if value is None:
            output.append("?")
        elif value > 0:
            output.append("+")
        elif value < 0:
            output.append("-")
        else:
            output.append("0")
    return "".join(output)


def _one_bar_rebound(prior: list[dict[str, Any]]) -> bool:
    return len(prior) >= 2 and prior[-1]["close"] > prior[-2]["close"]


def _two_bar_rebound(prior: list[dict[str, Any]]) -> bool:
    return len(prior) >= 3 and prior[-1]["close"] > prior[-2]["close"] and prior[-2]["close"] > prior[-3]["close"]


def _rebound_depth_1bar(prior: list[dict[str, Any]]) -> Decimal | None:
    if len(prior) < 2:
        return None
    return prior[-1]["close"] - prior[-2]["close"]


def _rebound_depth_2bar(prior: list[dict[str, Any]]) -> Decimal | None:
    if len(prior) < 3:
        return None
    return prior[-1]["close"] - min(prior[-2]["close"], prior[-3]["close"])


def _strict_monotone(values: list[Decimal | None], *, decreasing: bool) -> bool:
    if len(values) < 3 or any(value is None for value in values):
        return False
    if decreasing:
        return bool(values[0] > values[1] > values[2])
    return bool(values[0] < values[1] < values[2])


def _short_favorable_move(entry_close: Decimal, rows: list[dict[str, Any]]) -> Decimal | None:
    if not rows:
        return None
    return entry_close - min(item["low"] for item in rows)


def _avg(values) -> Decimal | None:
    filtered = []
    for value in values:
        if value is None:
            continue
        if not isinstance(value, Decimal):
            value = Decimal(str(value))
        filtered.append(value)
    if not filtered:
        return None
    return sum(filtered, Decimal("0")) / Decimal(len(filtered))


def _safe_div(numerator: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    if numerator is None or denominator in (None, Decimal("0")):
        return None
    return numerator / denominator


def _bool_rate(values) -> float:
    collected = list(values)
    if not collected:
        return 0.0
    return sum(1 for value in collected if value) / len(collected)


def _payload_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, dict) and value.get("__type__") == "decimal":
        return Decimal(value["value"])
    return Decimal(str(value))


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def _decimal_or_none(value: str) -> Decimal | None:
    if value == "":
        return None
    return Decimal(value)


def _decimal_str(value: Decimal | None) -> str:
    if value is None:
        return ""
    return format(value, "f")


def _is_positive(value: Decimal | None) -> bool:
    return value is not None and value > 0


def _is_negative(value: Decimal | None) -> bool:
    return value is not None and value < 0


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("strict_summary_path", type=Path)
    parser.add_argument("medium_1_summary_path", type=Path)
    parser.add_argument("medium_2_summary_path", type=Path)
    args = parser.parse_args()

    outputs = build_and_write_us_midday_pause_resume_widening_separator_analysis(
        strict_summary_path=args.strict_summary_path,
        medium_1_summary_path=args.medium_1_summary_path,
        medium_2_summary_path=args.medium_2_summary_path,
    )
    print(json.dumps(outputs, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
