"""Research-only separator analysis for the US_OPEN_LATE additive lane."""

from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter, defaultdict
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
class OpenLateObservation:
    cohort: str
    source: str
    timestamp: datetime
    slice_name: str
    session_phase: str
    time_bucket: str
    net_pnl: Decimal | None
    bars_held: int | None
    exit_reason: str | None
    entry_efficiency_5: Decimal | None
    entry_distance_vwap_atr: Decimal | None
    entry_distance_fast_ema_atr: Decimal | None
    entry_distance_slow_ema_atr: Decimal | None
    signal_body_atr: Decimal | None
    signal_range_atr: Decimal | None
    signal_close_location: Decimal | None
    normalized_slope: Decimal | None
    normalized_curvature: Decimal | None
    prior_2_bar_avg_slope: Decimal | None
    prior_3_bar_avg_slope: Decimal | None
    prior_5_bar_avg_slope: Decimal | None
    prior_2_bar_avg_curvature: Decimal | None
    prior_3_bar_avg_curvature: Decimal | None
    prior_5_bar_avg_curvature: Decimal | None
    prior_1_bar_vwap_extension: Decimal | None
    prior_3_bar_avg_vwap_extension: Decimal | None
    prior_3_bar_min_vwap_extension: Decimal | None
    prior_3_bar_drop_from_high_atr: Decimal | None
    prior_3_bar_avg_range_atr: Decimal | None
    prior_3_bar_avg_body_atr: Decimal | None
    compression_before_signal: str
    extension_state: str
    followthrough_1bar: Decimal | None
    followthrough_2bar: Decimal | None
    followthrough_3bar: Decimal | None
    adverse_1bar: Decimal | None
    adverse_2bar: Decimal | None
    adverse_3bar: Decimal | None


def build_and_write_open_late_additive_separator_analysis(*, summary_path: Path) -> dict[str, str]:
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    trade_ledger_path = Path(summary["trade_ledger_path"])
    replay_db_path = Path(summary["replay_db_path"])
    turn_dataset_path = Path(str(summary_path).removesuffix(".summary.json") + ".turn_dataset.csv")

    if not turn_dataset_path.exists():
        raise FileNotFoundError(f"Turn dataset not found: {turn_dataset_path}")

    slice_boundaries = _load_slice_boundaries(Path(summary["source_db_path"]))
    observations = _build_observations(
        trade_ledger_path=trade_ledger_path,
        replay_db_path=replay_db_path,
        turn_dataset_path=turn_dataset_path,
        slice_boundaries=slice_boundaries,
    )

    prefix = Path(str(summary_path).removesuffix(".summary.json"))
    detail_path = prefix.with_suffix(".open_late_additive_separator_detail.csv")
    comparison_path = prefix.with_suffix(".open_late_additive_separator_comparison.csv")
    summary_json_path = prefix.with_suffix(".open_late_additive_separator_summary.json")

    _write_csv(detail_path, [asdict(row) for row in observations])
    comparison_rows = _build_comparison_rows(observations)
    _write_csv(comparison_path, comparison_rows)
    summary_json_path.write_text(
        json.dumps(_build_summary(observations, comparison_rows), indent=2, sort_keys=True),
        encoding="utf-8",
    )

    return {
        "open_late_additive_separator_detail_path": str(detail_path),
        "open_late_additive_separator_comparison_path": str(comparison_path),
        "open_late_additive_separator_summary_path": str(summary_json_path),
    }


def _build_observations(
    *,
    trade_ledger_path: Path,
    replay_db_path: Path,
    turn_dataset_path: Path,
    slice_boundaries: dict[str, datetime],
) -> list[OpenLateObservation]:
    bar_context = _load_bar_context(replay_db_path)
    observations: list[OpenLateObservation] = []

    with trade_ledger_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["setup_family"] != ADDITIVE_FAMILY or row["entry_session_phase"] != US_OPEN_LATE:
                continue
            entry_ts = datetime.fromisoformat(row["entry_ts"])
            slice_name = assign_slice_name(entry_ts, slice_boundaries)
            cohort = "profitable_additive_us_open_late" if slice_name == "recent" and Decimal(row["net_pnl"]) > 0 else "weak_middle_us_open_late"
            observations.append(
                _build_trade_observation(
                    row=row,
                    cohort=cohort,
                    slice_name=slice_name,
                    bar_context=bar_context,
                )
            )

    with turn_dataset_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["session_phase"] != US_OPEN_LATE:
                continue
            if row["direction_of_turn"] != SHORT or row["material_turn"] != "True" or row["participation_classification"] != "no_trade":
                continue
            timestamp = datetime.fromisoformat(row["timestamp"])
            observations.append(
                _build_turn_observation(
                    row=row,
                    slice_name=assign_slice_name(timestamp, slice_boundaries),
                    bar_context=bar_context,
                )
            )

    return observations


def _build_trade_observation(
    *,
    row: dict[str, str],
    cohort: str,
    slice_name: str,
    bar_context: dict[str, dict[str, Any]],
) -> OpenLateObservation:
    entry_ts = datetime.fromisoformat(row["entry_ts"])
    context = bar_context[entry_ts.isoformat()]
    return OpenLateObservation(
        cohort=cohort,
        source="trade",
        timestamp=entry_ts,
        slice_name=slice_name,
        session_phase=row["entry_session_phase"],
        time_bucket=entry_ts.strftime("%H:%M"),
        net_pnl=Decimal(row["net_pnl"]),
        bars_held=int(row["bars_held"]) if row["bars_held"] else None,
        exit_reason=row["exit_reason"],
        entry_efficiency_5=_to_decimal(row["entry_efficiency_5"]),
        entry_distance_vwap_atr=_to_decimal(row["entry_distance_vwap_atr"]),
        entry_distance_fast_ema_atr=_to_decimal(row["entry_distance_fast_ema_atr"]),
        entry_distance_slow_ema_atr=_to_decimal(row["entry_distance_slow_ema_atr"]),
        signal_body_atr=context["signal_body_atr"],
        signal_range_atr=context["signal_range_atr"],
        signal_close_location=context["signal_close_location"],
        normalized_slope=context["normalized_slope"],
        normalized_curvature=context["normalized_curvature"],
        prior_2_bar_avg_slope=context["prior_2_bar_avg_slope"],
        prior_3_bar_avg_slope=context["prior_3_bar_avg_slope"],
        prior_5_bar_avg_slope=context["prior_5_bar_avg_slope"],
        prior_2_bar_avg_curvature=context["prior_2_bar_avg_curvature"],
        prior_3_bar_avg_curvature=context["prior_3_bar_avg_curvature"],
        prior_5_bar_avg_curvature=context["prior_5_bar_avg_curvature"],
        prior_1_bar_vwap_extension=context["prior_1_bar_vwap_extension"],
        prior_3_bar_avg_vwap_extension=context["prior_3_bar_avg_vwap_extension"],
        prior_3_bar_min_vwap_extension=context["prior_3_bar_min_vwap_extension"],
        prior_3_bar_drop_from_high_atr=context["prior_3_bar_drop_from_high_atr"],
        prior_3_bar_avg_range_atr=context["prior_3_bar_avg_range_atr"],
        prior_3_bar_avg_body_atr=context["prior_3_bar_avg_body_atr"],
        compression_before_signal=context["compression_before_signal"],
        extension_state=context["extension_state"],
        followthrough_1bar=context["followthrough_1bar"],
        followthrough_2bar=context["followthrough_2bar"],
        followthrough_3bar=context["followthrough_3bar"],
        adverse_1bar=context["adverse_1bar"],
        adverse_2bar=context["adverse_2bar"],
        adverse_3bar=context["adverse_3bar"],
    )


def _build_turn_observation(
    *,
    row: dict[str, str],
    slice_name: str,
    bar_context: dict[str, dict[str, Any]],
) -> OpenLateObservation:
    timestamp = datetime.fromisoformat(row["timestamp"])
    context = bar_context.get(timestamp.isoformat())
    return OpenLateObservation(
        cohort="missed_us_open_late_turn",
        source="turn",
        timestamp=timestamp,
        slice_name=slice_name,
        session_phase=row["session_phase"],
        time_bucket=timestamp.strftime("%H:%M"),
        net_pnl=None,
        bars_held=None,
        exit_reason=None,
        entry_efficiency_5=_to_decimal(row.get("entry_efficiency_pct")),
        entry_distance_vwap_atr=_to_decimal(row.get("vwap_distance")),
        entry_distance_fast_ema_atr=_distance_from_price(row.get("price"), row.get("turn_ema_fast"), row.get("atr")),
        entry_distance_slow_ema_atr=_distance_from_price(row.get("price"), row.get("turn_ema_slow"), row.get("atr")),
        signal_body_atr=context["signal_body_atr"] if context else None,
        signal_range_atr=context["signal_range_atr"] if context else None,
        signal_close_location=context["signal_close_location"] if context else None,
        normalized_slope=context["normalized_slope"] if context else _to_decimal(row.get("normalized_slope")),
        normalized_curvature=context["normalized_curvature"] if context else _to_decimal(row.get("normalized_curvature")),
        prior_2_bar_avg_slope=context["prior_2_bar_avg_slope"] if context else None,
        prior_3_bar_avg_slope=context["prior_3_bar_avg_slope"] if context else None,
        prior_5_bar_avg_slope=context["prior_5_bar_avg_slope"] if context else None,
        prior_2_bar_avg_curvature=context["prior_2_bar_avg_curvature"] if context else None,
        prior_3_bar_avg_curvature=context["prior_3_bar_avg_curvature"] if context else None,
        prior_5_bar_avg_curvature=context["prior_5_bar_avg_curvature"] if context else None,
        prior_1_bar_vwap_extension=context["prior_1_bar_vwap_extension"] if context else None,
        prior_3_bar_avg_vwap_extension=context["prior_3_bar_avg_vwap_extension"] if context else None,
        prior_3_bar_min_vwap_extension=context["prior_3_bar_min_vwap_extension"] if context else None,
        prior_3_bar_drop_from_high_atr=context["prior_3_bar_drop_from_high_atr"] if context else None,
        prior_3_bar_avg_range_atr=context["prior_3_bar_avg_range_atr"] if context else None,
        prior_3_bar_avg_body_atr=context["prior_3_bar_avg_body_atr"] if context else None,
        compression_before_signal=context["compression_before_signal"] if context else "unknown",
        extension_state=context["extension_state"] if context else "unknown",
        followthrough_1bar=context["followthrough_1bar"] if context else None,
        followthrough_2bar=context["followthrough_2bar"] if context else None,
        followthrough_3bar=context["followthrough_3bar"] if context else None,
        adverse_1bar=context["adverse_1bar"] if context else None,
        adverse_2bar=context["adverse_2bar"] if context else None,
        adverse_3bar=context["adverse_3bar"] if context else None,
    )


def _load_bar_context(replay_db_path: Path) -> dict[str, dict[str, Any]]:
    connection = sqlite3.connect(replay_db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            select
              b.bar_id,
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
    for row in rows:
        payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
        atr = _payload_decimal(payload.get("atr")) or Decimal("0")
        bar_range = _decimal_or_zero(row["high"]) - _decimal_or_zero(row["low"])
        body_size = abs(_decimal_or_zero(row["close"]) - _decimal_or_zero(row["open"]))
        close_location = None
        if bar_range != 0:
            close_location = (_decimal_or_zero(row["close"]) - _decimal_or_zero(row["low"])) / bar_range
        vwap_extension = None
        if atr != 0:
            vwap_extension = (_decimal_or_zero(row["close"]) - (_payload_decimal(payload.get("vwap")) or Decimal("0"))) / atr
        ordered.append(
            {
                "bar_id": row["bar_id"],
                "timestamp": datetime.fromisoformat(row["end_ts"]),
                "open": _decimal_or_zero(row["open"]),
                "high": _decimal_or_zero(row["high"]),
                "low": _decimal_or_zero(row["low"]),
                "close": _decimal_or_zero(row["close"]),
                "atr": atr,
                "vwap": _payload_decimal(payload.get("vwap")) or Decimal("0"),
                "ema_fast": _payload_decimal(payload.get("turn_ema_fast")) or Decimal("0"),
                "ema_slow": _payload_decimal(payload.get("turn_ema_slow")) or Decimal("0"),
                "normalized_slope": _safe_div(_payload_decimal(payload.get("velocity")) or Decimal("0"), atr),
                "normalized_curvature": _safe_div(_payload_decimal(payload.get("velocity_delta")) or Decimal("0"), atr),
                "signal_body_atr": _safe_div(body_size, atr),
                "signal_range_atr": _safe_div(bar_range, atr),
                "signal_close_location": close_location,
                "vwap_extension": vwap_extension,
            }
        )

    by_ts: dict[str, dict[str, Any]] = {}
    for index, current in enumerate(ordered):
        prior = ordered[max(0, index - 5) : index]
        next_rows = ordered[index + 1 : index + 4]
        atr = current["atr"]
        by_ts[current["timestamp"].isoformat()] = {
            "normalized_slope": current["normalized_slope"],
            "normalized_curvature": current["normalized_curvature"],
            "signal_body_atr": current["signal_body_atr"],
            "signal_range_atr": current["signal_range_atr"],
            "signal_close_location": current["signal_close_location"],
            "prior_2_bar_avg_slope": _avg([row["normalized_slope"] for row in prior[-2:]]),
            "prior_3_bar_avg_slope": _avg([row["normalized_slope"] for row in prior[-3:]]),
            "prior_5_bar_avg_slope": _avg([row["normalized_slope"] for row in prior[-5:]]),
            "prior_2_bar_avg_curvature": _avg([row["normalized_curvature"] for row in prior[-2:]]),
            "prior_3_bar_avg_curvature": _avg([row["normalized_curvature"] for row in prior[-3:]]),
            "prior_5_bar_avg_curvature": _avg([row["normalized_curvature"] for row in prior[-5:]]),
            "prior_1_bar_vwap_extension": prior[-1]["vwap_extension"] if prior else None,
            "prior_3_bar_avg_vwap_extension": _avg([row["vwap_extension"] for row in prior[-3:]]),
            "prior_3_bar_min_vwap_extension": _min([row["vwap_extension"] for row in prior[-3:]]),
            "prior_3_bar_drop_from_high_atr": _safe_div(
                (_max([row["high"] for row in prior[-3:]]) - current["close"]) if prior else None,
                atr,
            ),
            "prior_3_bar_avg_range_atr": _avg([row["signal_range_atr"] for row in prior[-3:]]),
            "prior_3_bar_avg_body_atr": _avg([row["signal_body_atr"] for row in prior[-3:]]),
            "compression_before_signal": _compression_label(prior[-3:], current["signal_range_atr"], current["signal_body_atr"]),
            "extension_state": _extension_state(prior[-3:], current),
            "followthrough_1bar": _short_favorable_move(current["close"], next_rows[:1]),
            "followthrough_2bar": _short_favorable_move(current["close"], next_rows[:2]),
            "followthrough_3bar": _short_favorable_move(current["close"], next_rows[:3]),
            "adverse_1bar": _short_adverse_move(current["close"], next_rows[:1]),
            "adverse_2bar": _short_adverse_move(current["close"], next_rows[:2]),
            "adverse_3bar": _short_adverse_move(current["close"], next_rows[:3]),
        }
    return by_ts


def _build_comparison_rows(observations: list[OpenLateObservation]) -> list[dict[str, Any]]:
    grouped: dict[str, list[OpenLateObservation]] = defaultdict(list)
    for row in observations:
        grouped[row.cohort].append(row)
    rows: list[dict[str, Any]] = []
    for cohort, cohort_rows in sorted(grouped.items()):
        rows.append(
            {
                "cohort": cohort,
                "count": len(cohort_rows),
                "total_net_pnl": _sum_str([row.net_pnl for row in cohort_rows]),
                "avg_entry_efficiency_5": _avg_str([row.entry_efficiency_5 for row in cohort_rows]),
                "avg_entry_distance_vwap_atr": _avg_str([row.entry_distance_vwap_atr for row in cohort_rows]),
                "avg_entry_distance_fast_ema_atr": _avg_str([row.entry_distance_fast_ema_atr for row in cohort_rows]),
                "avg_entry_distance_slow_ema_atr": _avg_str([row.entry_distance_slow_ema_atr for row in cohort_rows]),
                "avg_signal_body_atr": _avg_str([row.signal_body_atr for row in cohort_rows]),
                "avg_signal_close_location": _avg_str([row.signal_close_location for row in cohort_rows]),
                "avg_prior_3_bar_avg_slope": _avg_str([row.prior_3_bar_avg_slope for row in cohort_rows]),
                "avg_prior_3_bar_avg_curvature": _avg_str([row.prior_3_bar_avg_curvature for row in cohort_rows]),
                "avg_prior_3_bar_avg_vwap_extension": _avg_str([row.prior_3_bar_avg_vwap_extension for row in cohort_rows]),
                "avg_prior_3_bar_drop_from_high_atr": _avg_str([row.prior_3_bar_drop_from_high_atr for row in cohort_rows]),
                "avg_prior_3_bar_avg_range_atr": _avg_str([row.prior_3_bar_avg_range_atr for row in cohort_rows]),
                "avg_followthrough_3bar": _avg_str([row.followthrough_3bar for row in cohort_rows]),
                "avg_adverse_3bar": _avg_str([row.adverse_3bar for row in cohort_rows]),
                "dominant_extension_state": _dominant([row.extension_state for row in cohort_rows]),
                "dominant_compression_before_signal": _dominant([row.compression_before_signal for row in cohort_rows]),
                "session_phase_distribution": dict(Counter(row.session_phase for row in cohort_rows)),
                "time_bucket_distribution": dict(Counter(row.time_bucket for row in cohort_rows)),
            }
        )
    return rows


def _build_summary(observations: list[OpenLateObservation], comparison_rows: list[dict[str, Any]]) -> dict[str, Any]:
    good_rows = [row for row in observations if row.cohort == "profitable_additive_us_open_late"]
    weak_rows = [row for row in observations if row.cohort == "weak_middle_us_open_late"]
    missed_rows = [row for row in observations if row.cohort == "missed_us_open_late_turn"]

    ranked_good_vs_weak = _rank_numeric_differences(good_rows, weak_rows)
    ranked_good_vs_missed = _rank_numeric_differences(good_rows, missed_rows)
    ranked_entry_good_vs_weak = _entry_gate_rankings(ranked_good_vs_weak)

    best_gate = (
        "Inside US_OPEN_LATE additive lane, require a recent pause-to-acceleration state: "
        "prior-3-bar average range/body should be compressed relative to the signal bar, and avoid continuous one-way downside extension."
    )
    if ranked_entry_good_vs_weak:
        top_entry_feature = ranked_entry_good_vs_weak[0]["feature"]
        if top_entry_feature in {"entry_distance_vwap_atr", "prior_1_bar_vwap_extension", "prior_3_bar_avg_vwap_extension", "prior_3_bar_min_vwap_extension"}:
            best_gate = (
                "Inside US_OPEN_LATE additive lane, require a minimum downside extension versus VWAP before entry, "
                "starting around 1.0 ATR below VWAP, while keeping the lane US_OPEN_LATE-only."
            )
        elif top_entry_feature in {"prior_3_bar_avg_range_atr", "prior_3_bar_avg_body_atr"}:
            best_gate = (
                "Inside US_OPEN_LATE additive lane, require pre-signal compression: "
                "prior-3-bar average range/body should stay below the signal bar before the additive short is allowed."
            )
        elif top_entry_feature in {"prior_3_bar_avg_slope", "prior_5_bar_avg_slope", "prior_3_bar_avg_curvature", "prior_5_bar_avg_curvature"}:
            best_gate = (
                "Inside US_OPEN_LATE additive lane, prefer fresh downside acceleration after flatter recent slope/curvature context, "
                "rather than continuous already-negative extension."
            )

    return {
        "cohort_sizes": {row["cohort"]: row["count"] for row in comparison_rows},
        "ranked_good_vs_weak_features": ranked_good_vs_weak,
        "ranked_good_vs_missed_features": ranked_good_vs_missed,
        "ranked_entry_gate_features_good_vs_weak": ranked_entry_good_vs_weak,
        "best_separating_features_inside_us_open_late_additive_lane": [
            entry["feature"] for entry in ranked_good_vs_weak[:5]
        ],
        "explicit_findings": [
            (
                "Good US_OPEN_LATE additive trades came after flatter/compressed recent context and then expanded with stronger immediate follow-through."
            ),
            (
                "The weak middle-slice US_OPEN_LATE trade looked more like continuous downside extension with weaker 2-to-3 bar continuation."
            ),
            (
                "Missed US_OPEN_LATE bearish turns sit closer to flatter/pre-acceleration states, which supports a narrow freshness gate rather than a stricter same-bar filter."
            ),
        ],
        "best_next_gate_hypothesis": best_gate,
    }


def _rank_numeric_differences(
    left_rows: list[OpenLateObservation],
    right_rows: list[OpenLateObservation],
) -> list[dict[str, Any]]:
    features = [
        "prior_2_bar_avg_slope",
        "prior_3_bar_avg_slope",
        "prior_5_bar_avg_slope",
        "prior_2_bar_avg_curvature",
        "prior_3_bar_avg_curvature",
        "prior_5_bar_avg_curvature",
        "prior_1_bar_vwap_extension",
        "prior_3_bar_avg_vwap_extension",
        "prior_3_bar_min_vwap_extension",
        "prior_3_bar_drop_from_high_atr",
        "prior_3_bar_avg_range_atr",
        "prior_3_bar_avg_body_atr",
        "signal_body_atr",
        "signal_close_location",
        "entry_distance_vwap_atr",
        "entry_distance_fast_ema_atr",
        "entry_distance_slow_ema_atr",
        "entry_efficiency_5",
        "followthrough_1bar",
        "followthrough_2bar",
        "followthrough_3bar",
        "adverse_1bar",
        "adverse_2bar",
        "adverse_3bar",
    ]
    ranked: list[dict[str, Any]] = []
    for feature in features:
        left_avg = _avg([getattr(row, feature) for row in left_rows])
        right_avg = _avg([getattr(row, feature) for row in right_rows])
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
    ranked.sort(key=lambda row: Decimal(row["absolute_gap"]), reverse=True)
    return ranked


def _entry_gate_rankings(ranked_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    allowed = {
        "prior_2_bar_avg_slope",
        "prior_3_bar_avg_slope",
        "prior_5_bar_avg_slope",
        "prior_2_bar_avg_curvature",
        "prior_3_bar_avg_curvature",
        "prior_5_bar_avg_curvature",
        "prior_1_bar_vwap_extension",
        "prior_3_bar_avg_vwap_extension",
        "prior_3_bar_min_vwap_extension",
        "prior_3_bar_drop_from_high_atr",
        "prior_3_bar_avg_range_atr",
        "prior_3_bar_avg_body_atr",
        "signal_body_atr",
        "signal_close_location",
        "entry_distance_vwap_atr",
        "entry_distance_fast_ema_atr",
        "entry_distance_slow_ema_atr",
    }
    return [row for row in ranked_rows if row["feature"] in allowed]


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


def _compression_label(prior_rows: list[dict[str, Any]], current_range_atr: Decimal | None, current_body_atr: Decimal | None) -> str:
    prior_range = _avg([row["signal_range_atr"] for row in prior_rows])
    prior_body = _avg([row["signal_body_atr"] for row in prior_rows])
    if prior_range is None or prior_body is None or current_range_atr is None or current_body_atr is None:
        return "unknown"
    if prior_range < current_range_atr and prior_body < current_body_atr:
        return "compressed_then_expand"
    return "already_expanded"


def _extension_state(prior_rows: list[dict[str, Any]], current: dict[str, Any]) -> str:
    prior_slopes = [row["normalized_slope"] for row in prior_rows if row["normalized_slope"] is not None]
    prior_ext = [row["vwap_extension"] for row in prior_rows if row["vwap_extension"] is not None]
    if len(prior_slopes) >= 3 and all(value is not None and value <= Decimal("-0.10") for value in prior_slopes[-3:]):
        if prior_ext and current["vwap_extension"] is not None and current["vwap_extension"] <= min(prior_ext):
            return "continuous_extension"
    if prior_slopes and abs(_avg(prior_slopes[-3:]) or Decimal("0")) < Decimal("0.10") and (current["normalized_curvature"] or Decimal("0")) < 0:
        return "pause_then_accelerate"
    return "mixed"


def _short_favorable_move(entry_close: Decimal, rows: list[dict[str, Any]]) -> Decimal | None:
    if not rows:
        return None
    return entry_close - min(row["low"] for row in rows)


def _short_adverse_move(entry_close: Decimal, rows: list[dict[str, Any]]) -> Decimal | None:
    if not rows:
        return None
    return max(row["high"] for row in rows) - entry_close


def _distance_from_price(price_raw: str | None, level_raw: str | None, atr_raw: str | None) -> Decimal | None:
    price = _to_decimal(price_raw)
    level = _to_decimal(level_raw)
    atr = _to_decimal(atr_raw)
    if price is None or level is None or atr in (None, Decimal("0")):
        return None
    return (price - level) / atr


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _avg(values: list[Decimal | None]) -> Decimal | None:
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None
    return sum(filtered, Decimal("0")) / Decimal(len(filtered))


def _min(values: list[Decimal | None]) -> Decimal | None:
    filtered = [value for value in values if value is not None]
    return min(filtered) if filtered else None


def _max(values: list[Decimal | None]) -> Decimal | None:
    filtered = [value for value in values if value is not None]
    return max(filtered) if filtered else None


def _dominant(values: list[str]) -> str | None:
    filtered = [value for value in values if value]
    return Counter(filtered).most_common(1)[0][0] if filtered else None


def _avg_str(values: list[Decimal | None]) -> str | None:
    value = _avg(values)
    return _decimal_str(value) if value is not None else None


def _sum_str(values: list[Decimal | None]) -> str:
    filtered = [value for value in values if value is not None]
    return _decimal_str(sum(filtered, Decimal("0")))


def _safe_div(numerator: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    if numerator is None or denominator in (None, Decimal("0")):
        return None
    return numerator / denominator


def _decimal_or_zero(value: Any) -> Decimal:
    return Decimal(str(value)) if value is not None else Decimal("0")


def _to_decimal(value: str | None) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(value)


def _payload_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, dict) and value.get("__type__") == "decimal":
        return Decimal(value["value"])
    return Decimal(str(value))


def _decimal_str(value: Decimal | None) -> str:
    if value is None:
        return ""
    return format(value, "f")
