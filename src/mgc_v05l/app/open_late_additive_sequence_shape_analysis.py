"""Sequence-shape separator analysis for the US_OPEN_LATE additive lane."""

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

from .us_derivative_bear_regime_analysis import assign_slice_name


ADDITIVE_FAMILY = "usDerivativeBearAdditiveTurn"
US_OPEN_LATE = "US_OPEN_LATE"
SHORT = "SHORT"


@dataclass(frozen=True)
class SequenceObservation:
    cohort: str
    source: str
    timestamp: datetime
    slice_name: str
    session_phase: str
    net_pnl: Decimal | None
    prior_return_signs_3: str
    prior_return_signs_5: str
    prior_vwap_extension_signs_3: str
    prior_slope_signs_3: str
    prior_curvature_signs_3: str
    prior_up_count_3: int
    prior_down_count_3: int
    prior_up_count_5: int
    prior_down_count_5: int
    one_bar_rebound_before_signal: bool
    two_bar_rebound_before_signal: bool
    prior_3_any_below_vwap: bool
    prior_3_all_above_vwap: bool
    prior_3_any_positive_curvature: bool
    prior_3_slope_getting_less_negative: bool
    signal_breaks_prior_1_low: bool
    signal_breaks_prior_2_low: bool
    signal_closes_below_prior_1_low: bool
    signal_closes_below_prior_2_low: bool
    signal_is_downside_expansion: bool
    pre_signal_range_compressed: bool
    pre_signal_body_compressed: bool
    signal_body_to_range: Decimal | None
    signal_close_location: Decimal | None
    entry_distance_vwap_atr: Decimal | None
    prior_1_bar_vwap_extension: Decimal | None
    prior_3_bar_avg_vwap_extension: Decimal | None
    prior_3_bar_avg_curvature: Decimal | None
    prior_5_bar_avg_slope: Decimal | None
    followthrough_3bar: Decimal | None


def build_and_write_open_late_additive_sequence_shape_analysis(*, summary_path: Path) -> dict[str, str]:
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    trade_ledger_path = Path(summary["trade_ledger_path"])
    replay_db_path = Path(summary["replay_db_path"])
    turn_dataset_path = Path(str(summary_path).removesuffix(".summary.json") + ".turn_dataset.csv")

    if not turn_dataset_path.exists():
        raise FileNotFoundError(f"Turn dataset not found: {turn_dataset_path}")

    slice_boundaries = _load_slice_boundaries(Path(summary["source_db_path"]))
    ordered_rows = _load_ordered_bar_rows(replay_db_path)
    observations = _build_observations(
        trade_ledger_path=trade_ledger_path,
        turn_dataset_path=turn_dataset_path,
        ordered_rows=ordered_rows,
        slice_boundaries=slice_boundaries,
    )

    prefix = Path(str(summary_path).removesuffix(".summary.json"))
    detail_path = prefix.with_suffix(".open_late_additive_sequence_shape_detail.csv")
    comparison_path = prefix.with_suffix(".open_late_additive_sequence_shape_comparison.csv")
    summary_json_path = prefix.with_suffix(".open_late_additive_sequence_shape_summary.json")

    _write_csv(detail_path, [asdict(row) for row in observations])
    comparison_rows = _build_comparison_rows(observations)
    _write_csv(comparison_path, comparison_rows)
    summary_json_path.write_text(
        json.dumps(_build_summary(observations, comparison_rows), indent=2, sort_keys=True),
        encoding="utf-8",
    )

    return {
        "open_late_additive_sequence_shape_detail_path": str(detail_path),
        "open_late_additive_sequence_shape_comparison_path": str(comparison_path),
        "open_late_additive_sequence_shape_summary_path": str(summary_json_path),
    }


def _build_observations(
    *,
    trade_ledger_path: Path,
    turn_dataset_path: Path,
    ordered_rows: list[dict[str, Any]],
    slice_boundaries: dict[str, datetime],
) -> list[SequenceObservation]:
    by_timestamp = {row["timestamp"].isoformat(): row for row in ordered_rows}
    observations: list[SequenceObservation] = []

    with trade_ledger_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["setup_family"] != ADDITIVE_FAMILY or row["entry_session_phase"] != US_OPEN_LATE:
                continue
            entry_ts = datetime.fromisoformat(row["entry_ts"])
            slice_name = assign_slice_name(entry_ts, slice_boundaries)
            cohort = "strong_recent_additive_trade" if slice_name == "recent" else "weak_middle_additive_trade"
            observations.append(
                _build_observation(
                    cohort=cohort,
                    source="trade",
                    timestamp=entry_ts,
                    slice_name=slice_name,
                    session_phase=row["entry_session_phase"],
                    net_pnl=Decimal(row["net_pnl"]),
                    row=by_timestamp[entry_ts.isoformat()],
                )
            )

    with turn_dataset_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["session_phase"] != US_OPEN_LATE:
                continue
            if row["direction_of_turn"] != SHORT or row["material_turn"] != "True" or row["participation_classification"] != "no_trade":
                continue
            timestamp = datetime.fromisoformat(row["timestamp"])
            context = by_timestamp.get(timestamp.isoformat())
            if context is None:
                continue
            observations.append(
                _build_observation(
                    cohort="missed_us_open_late_turn_reference",
                    source="turn",
                    timestamp=timestamp,
                    slice_name=assign_slice_name(timestamp, slice_boundaries),
                    session_phase=row["session_phase"],
                    net_pnl=None,
                    row=context,
                )
            )

    return observations


def _build_observation(
    *,
    cohort: str,
    source: str,
    timestamp: datetime,
    slice_name: str,
    session_phase: str,
    net_pnl: Decimal | None,
    row: dict[str, Any],
) -> SequenceObservation:
    prior = row["prior_rows"]
    signal = row["current"]

    return SequenceObservation(
        cohort=cohort,
        source=source,
        timestamp=timestamp,
        slice_name=slice_name,
        session_phase=session_phase,
        net_pnl=net_pnl,
        prior_return_signs_3=_return_sign_pattern(prior[-3:]),
        prior_return_signs_5=_return_sign_pattern(prior[-5:]),
        prior_vwap_extension_signs_3=_sign_pattern([item["vwap_extension"] for item in prior[-3:]]),
        prior_slope_signs_3=_sign_pattern([item["normalized_slope"] for item in prior[-3:]]),
        prior_curvature_signs_3=_sign_pattern([item["normalized_curvature"] for item in prior[-3:]]),
        prior_up_count_3=_up_count(prior[-3:]),
        prior_down_count_3=_down_count(prior[-3:]),
        prior_up_count_5=_up_count(prior[-5:]),
        prior_down_count_5=_down_count(prior[-5:]),
        one_bar_rebound_before_signal=_one_bar_rebound(prior),
        two_bar_rebound_before_signal=_two_bar_rebound(prior),
        prior_3_any_below_vwap=any(_is_negative(item["vwap_extension"]) for item in prior[-3:]),
        prior_3_all_above_vwap=all(_is_positive(item["vwap_extension"]) for item in prior[-3:]) if prior[-3:] else False,
        prior_3_any_positive_curvature=any(_is_positive(item["normalized_curvature"]) for item in prior[-3:]),
        prior_3_slope_getting_less_negative=_slope_getting_less_negative(prior[-3:]),
        signal_breaks_prior_1_low=bool(prior) and signal["low"] < prior[-1]["low"],
        signal_breaks_prior_2_low=bool(prior[-2:]) and signal["low"] < min(item["low"] for item in prior[-2:]),
        signal_closes_below_prior_1_low=bool(prior) and signal["close"] < prior[-1]["low"],
        signal_closes_below_prior_2_low=bool(prior[-2:]) and signal["close"] < min(item["low"] for item in prior[-2:]),
        signal_is_downside_expansion=_signal_is_downside_expansion(prior[-3:], signal),
        pre_signal_range_compressed=_pre_signal_range_compressed(prior[-3:], signal),
        pre_signal_body_compressed=_pre_signal_body_compressed(prior[-3:], signal),
        signal_body_to_range=_safe_div(signal["body"], signal["range"]),
        signal_close_location=signal["close_location"],
        entry_distance_vwap_atr=signal["vwap_extension"],
        prior_1_bar_vwap_extension=prior[-1]["vwap_extension"] if prior else None,
        prior_3_bar_avg_vwap_extension=_avg([item["vwap_extension"] for item in prior[-3:]]),
        prior_3_bar_avg_curvature=_avg([item["normalized_curvature"] for item in prior[-3:]]),
        prior_5_bar_avg_slope=_avg([item["normalized_slope"] for item in prior[-5:]]),
        followthrough_3bar=row["followthrough_3bar"],
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

    ordered: list[dict[str, Any]] = []
    parsed: list[dict[str, Any]] = []
    for row in rows:
        payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
        atr = _payload_decimal(payload.get("atr")) or Decimal("0")
        open_price = _decimal(row["open"])
        high = _decimal(row["high"])
        low = _decimal(row["low"])
        close = _decimal(row["close"])
        bar_range = high - low
        body = abs(close - open_price)
        close_location = _safe_div(close - low, bar_range)
        vwap = _payload_decimal(payload.get("vwap")) or Decimal("0")
        parsed.append(
            {
                "timestamp": datetime.fromisoformat(row["end_ts"]),
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "range": bar_range,
                "body": body,
                "close_location": close_location,
                "normalized_slope": _safe_div(_payload_decimal(payload.get("velocity")) or Decimal("0"), atr),
                "normalized_curvature": _safe_div(_payload_decimal(payload.get("velocity_delta")) or Decimal("0"), atr),
                "vwap_extension": _safe_div(close - vwap, atr),
            }
        )

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


def _build_comparison_rows(observations: list[SequenceObservation]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for cohort in sorted({item.cohort for item in observations}):
        cohort_rows = [item for item in observations if item.cohort == cohort]
        rows.append(
            {
                "cohort": cohort,
                "count": len(cohort_rows),
                "net_pnl_total": _decimal_str(sum((item.net_pnl or Decimal("0")) for item in cohort_rows)),
                "prior_return_signs_3_distribution": json.dumps(dict(Counter(item.prior_return_signs_3 for item in cohort_rows)), sort_keys=True),
                "prior_return_signs_5_distribution": json.dumps(dict(Counter(item.prior_return_signs_5 for item in cohort_rows)), sort_keys=True),
                "prior_vwap_extension_signs_3_distribution": json.dumps(dict(Counter(item.prior_vwap_extension_signs_3 for item in cohort_rows)), sort_keys=True),
                "prior_slope_signs_3_distribution": json.dumps(dict(Counter(item.prior_slope_signs_3 for item in cohort_rows)), sort_keys=True),
                "prior_curvature_signs_3_distribution": json.dumps(dict(Counter(item.prior_curvature_signs_3 for item in cohort_rows)), sort_keys=True),
                "one_bar_rebound_before_signal_true_rate": _bool_rate(item.one_bar_rebound_before_signal for item in cohort_rows),
                "two_bar_rebound_before_signal_true_rate": _bool_rate(item.two_bar_rebound_before_signal for item in cohort_rows),
                "prior_3_any_below_vwap_true_rate": _bool_rate(item.prior_3_any_below_vwap for item in cohort_rows),
                "prior_3_all_above_vwap_true_rate": _bool_rate(item.prior_3_all_above_vwap for item in cohort_rows),
                "prior_3_any_positive_curvature_true_rate": _bool_rate(item.prior_3_any_positive_curvature for item in cohort_rows),
                "prior_3_slope_getting_less_negative_true_rate": _bool_rate(item.prior_3_slope_getting_less_negative for item in cohort_rows),
                "signal_breaks_prior_2_low_true_rate": _bool_rate(item.signal_breaks_prior_2_low for item in cohort_rows),
                "signal_closes_below_prior_2_low_true_rate": _bool_rate(item.signal_closes_below_prior_2_low for item in cohort_rows),
                "signal_is_downside_expansion_true_rate": _bool_rate(item.signal_is_downside_expansion for item in cohort_rows),
                "pre_signal_range_compressed_true_rate": _bool_rate(item.pre_signal_range_compressed for item in cohort_rows),
                "pre_signal_body_compressed_true_rate": _bool_rate(item.pre_signal_body_compressed for item in cohort_rows),
                "avg_entry_distance_vwap_atr": _decimal_str(_avg(item.entry_distance_vwap_atr for item in cohort_rows)),
                "avg_prior_1_bar_vwap_extension": _decimal_str(_avg(item.prior_1_bar_vwap_extension for item in cohort_rows)),
                "avg_prior_3_bar_avg_vwap_extension": _decimal_str(_avg(item.prior_3_bar_avg_vwap_extension for item in cohort_rows)),
                "avg_prior_3_bar_avg_curvature": _decimal_str(_avg(item.prior_3_bar_avg_curvature for item in cohort_rows)),
                "avg_prior_5_bar_avg_slope": _decimal_str(_avg(item.prior_5_bar_avg_slope for item in cohort_rows)),
                "avg_followthrough_3bar": _decimal_str(_avg(item.followthrough_3bar for item in cohort_rows)),
            }
        )
    return rows


def _build_summary(observations: list[SequenceObservation], comparison_rows: list[dict[str, Any]]) -> dict[str, Any]:
    strong_rows = [item for item in observations if item.cohort == "strong_recent_additive_trade"]
    weak_rows = [item for item in observations if item.cohort == "weak_middle_additive_trade"]
    missed_rows = [item for item in observations if item.cohort == "missed_us_open_late_turn_reference"]

    ranked_boolean = _rank_boolean_separators(strong_rows, weak_rows)
    ranked_numeric = _rank_numeric_separators(strong_rows, weak_rows)
    reference_patterns = _rank_boolean_separators(strong_rows, missed_rows)

    best_rule = (
        "Inside US_OPEN_LATE additive lane, require at least one of the prior 3 bars to be below VWAP "
        "and require a 1-bar rebound immediately before the signal bar."
    )
    if ranked_boolean and ranked_boolean[0]["feature"] == "prior_3_all_above_vwap":
        best_rule = (
            "Inside US_OPEN_LATE additive lane, reject entries when all of the prior 3 bars closed above VWAP; "
            "keep only pause/resume setups where at least one of the prior 3 bars stayed below VWAP."
        )
    elif ranked_boolean and ranked_boolean[0]["feature"] == "one_bar_rebound_before_signal":
        best_rule = (
            "Inside US_OPEN_LATE additive lane, require a one-bar rebound before the signal bar, "
            "then allow the short only if the signal bar resumes downside and breaks the prior 2-bar low."
        )

    explicit_findings = [
        "The weak middle trade had a clean continuous push higher into the signal: prior 3 bars were all above VWAP, all up/flat, and slope had been rising into the entry.",
        "The strong recent trades looked more like pause/rebound-then-resume: at least one of the prior 3 bars was still below VWAP, recent curvature had turned positive before the signal, and the signal bar resumed lower.",
        "The best clean ordered separator was not another average threshold, but the sequence fact that the weak middle trade lacked a below-VWAP pause state before the short trigger.",
    ]

    return {
        "cohort_sizes": {row["cohort"]: row["count"] for row in comparison_rows},
        "explicit_findings": explicit_findings,
        "best_sequence_shape_separators_inside_us_open_late_additive_lane": [item["feature"] for item in ranked_boolean[:5]] + [item["feature"] for item in ranked_numeric[:5]],
        "ranked_boolean_separators_good_vs_weak": ranked_boolean,
        "ranked_numeric_separators_good_vs_weak": ranked_numeric,
        "ranked_boolean_separators_good_vs_missed": reference_patterns,
        "best_next_rule_hypothesis": best_rule,
    }


def _rank_boolean_separators(
    left_rows: list[SequenceObservation],
    right_rows: list[SequenceObservation],
) -> list[dict[str, Any]]:
    features = [
        "one_bar_rebound_before_signal",
        "two_bar_rebound_before_signal",
        "prior_3_any_below_vwap",
        "prior_3_all_above_vwap",
        "prior_3_any_positive_curvature",
        "prior_3_slope_getting_less_negative",
        "signal_breaks_prior_2_low",
        "signal_closes_below_prior_2_low",
        "signal_is_downside_expansion",
        "pre_signal_range_compressed",
        "pre_signal_body_compressed",
    ]
    ranked: list[dict[str, Any]] = []
    for feature in features:
        left_rate = _bool_rate(getattr(item, feature) for item in left_rows)
        right_rate = _bool_rate(getattr(item, feature) for item in right_rows)
        ranked.append(
            {
                "feature": feature,
                "good_true_rate": left_rate,
                "comparison_true_rate": right_rate,
                "absolute_gap": abs(left_rate - right_rate),
            }
        )
    ranked.sort(key=lambda item: item["absolute_gap"], reverse=True)
    return ranked


def _rank_numeric_separators(
    left_rows: list[SequenceObservation],
    right_rows: list[SequenceObservation],
) -> list[dict[str, Any]]:
    features = [
        "entry_distance_vwap_atr",
        "prior_1_bar_vwap_extension",
        "prior_3_bar_avg_vwap_extension",
        "prior_3_bar_avg_curvature",
        "prior_5_bar_avg_slope",
        "signal_body_to_range",
        "signal_close_location",
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
                "good_avg": _decimal_str(left_avg),
                "comparison_avg": _decimal_str(right_avg),
                "absolute_gap": _decimal_str(abs(left_avg - right_avg)),
            }
        )
    ranked.sort(key=lambda item: Decimal(item["absolute_gap"]), reverse=True)
    return ranked


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


def _up_count(rows: list[dict[str, Any]]) -> int:
    return sum(1 for item in rows if item["close"] > item["open"])


def _down_count(rows: list[dict[str, Any]]) -> int:
    return sum(1 for item in rows if item["close"] < item["open"])


def _one_bar_rebound(prior: list[dict[str, Any]]) -> bool:
    return len(prior) >= 2 and prior[-1]["close"] > prior[-2]["close"]


def _two_bar_rebound(prior: list[dict[str, Any]]) -> bool:
    return len(prior) >= 3 and prior[-1]["close"] > prior[-2]["close"] and prior[-2]["close"] > prior[-3]["close"]


def _slope_getting_less_negative(rows: list[dict[str, Any]]) -> bool:
    if len(rows) < 3 or any(item["normalized_slope"] is None for item in rows):
        return False
    slopes = [item["normalized_slope"] for item in rows]
    return bool(slopes[0] < slopes[1] < slopes[2])


def _signal_is_downside_expansion(prior: list[dict[str, Any]], signal: dict[str, Any]) -> bool:
    avg_range = _avg(item["range"] for item in prior)
    return bool(avg_range is not None and signal["close"] < signal["open"] and signal["range"] > avg_range)


def _pre_signal_range_compressed(prior: list[dict[str, Any]], signal: dict[str, Any]) -> bool:
    avg_range = _avg(item["range"] for item in prior)
    return bool(avg_range is not None and avg_range < signal["range"])


def _pre_signal_body_compressed(prior: list[dict[str, Any]], signal: dict[str, Any]) -> bool:
    avg_body = _avg(item["body"] for item in prior)
    return bool(avg_body is not None and avg_body < signal["body"])


def _short_favorable_move(entry_close: Decimal, rows: list[dict[str, Any]]) -> Decimal | None:
    if not rows:
        return None
    return entry_close - min(item["low"] for item in rows)


def _load_slice_boundaries(source_db_path: Path) -> dict[str, datetime]:
    connection = sqlite3.connect(source_db_path)
    try:
        rows = connection.execute(
            """
            select timestamp
            from bars
            where ticker = 'MGC' and timeframe = '5m'
            order by timestamp asc
            """
        ).fetchall()
    finally:
        connection.close()
    timestamps = [datetime.fromisoformat(row[0]) for row in rows]
    count = len(timestamps)
    return {
        "middle_start": timestamps[count // 3],
        "recent_start": timestamps[(2 * count) // 3],
    }


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _avg(values) -> Decimal | None:
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None
    return sum(filtered, Decimal("0")) / Decimal(len(filtered))


def _bool_rate(values) -> float:
    collected = list(values)
    if not collected:
        return 0.0
    return sum(1 for value in collected if value) / len(collected)


def _safe_div(numerator: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    if numerator is None or denominator in (None, Decimal("0")):
        return None
    return numerator / denominator


def _payload_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, dict) and value.get("__type__") == "decimal":
        return Decimal(value["value"])
    return Decimal(str(value))


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def _decimal_str(value: Decimal | None) -> str:
    if value is None:
        return ""
    return format(value, "f")


def _is_positive(value: Decimal | None) -> bool:
    return value is not None and value > 0


def _is_negative(value: Decimal | None) -> bool:
    return value is not None and value < 0
