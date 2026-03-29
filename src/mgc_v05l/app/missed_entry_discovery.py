"""Full-clock missed-turn discovery for new MGC entry family research."""

from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from .replay_turn_research import build_and_write_replay_turn_research


RESEARCH_PHASES = (
    "SESSION_RESET_1800",
    "ASIA_EARLY",
    "ASIA_LATE",
    "LONDON_OPEN",
    "LONDON_LATE",
    "US_PREOPEN_OPENING",
    "US_CASH_OPEN_IMPULSE",
    "US_OPEN_LATE",
    "US_MIDDAY",
    "US_LATE",
)

SHORT = "SHORT"
LONG = "LONG"


@dataclass(frozen=True)
class OrderedBarContext:
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    atr: Decimal
    vwap: Decimal | None
    turn_ema_fast: Decimal | None
    turn_ema_slow: Decimal | None
    normalized_slope: Decimal
    normalized_curvature: Decimal
    range_expansion_ratio: Decimal | None
    volatility_regime: str


@dataclass(frozen=True)
class MissedTurnObservation:
    timestamp: datetime
    session_phase: str
    direction_of_turn: str
    participation_classification: str
    local_turn_type: str
    signal_family_if_any: str | None
    move_5bar: Decimal
    move_10bar: Decimal
    move_20bar: Decimal
    mfe_20bar: Decimal
    mae_20bar: Decimal
    atr: Decimal
    vwap_distance_atr: Decimal | None
    vwap_bucket: str
    ema_relation: str
    slope_bucket: str
    curvature_bucket: str
    derivative_bucket: str
    expansion_state: str
    recent_path_shape: str
    followthrough_quality: str
    volatility_regime: str
    prior_return_signs_3: str
    prior_return_signs_5: str
    prior_vwap_extension_signs_3: str
    prior_curvature_signs_3: str
    one_bar_rebound_before_signal: bool
    two_bar_rebound_before_signal: bool
    prior_3_any_below_vwap: bool
    prior_3_all_above_vwap: bool
    prior_3_any_positive_curvature: bool
    signal_close_location: Decimal | None
    signal_body_to_range: Decimal | None


def build_and_write_missed_entry_discovery(*, summary_path: Path) -> dict[str, str]:
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    turn_dataset_path = Path(str(summary_path).removesuffix(".summary.json") + ".turn_dataset.csv")
    if not turn_dataset_path.exists():
        build_and_write_replay_turn_research(summary_path)

    replay_db_path = Path(summary["replay_db_path"])
    observations = _build_missed_turn_observations(
        turn_dataset_path=turn_dataset_path,
        replay_db_path=replay_db_path,
    )

    prefix = Path(str(summary_path).removesuffix(".summary.json"))
    detail_path = prefix.with_suffix(".missed_entry_discovery_detail.csv")
    session_map_path = prefix.with_suffix(".missed_entry_session_phase_map.csv")
    cluster_path = prefix.with_suffix(".missed_entry_candidate_clusters.csv")
    summary_json_path = prefix.with_suffix(".missed_entry_discovery_summary.json")

    detail_rows = [_observation_row(observation) for observation in observations]
    session_rows = _build_session_phase_rows(observations)
    cluster_rows = _build_candidate_family_rows(observations)
    summary_payload = _build_summary(observations, session_rows, cluster_rows)

    _write_csv(detail_path, detail_rows)
    _write_csv(session_map_path, session_rows)
    _write_csv(cluster_path, cluster_rows)
    summary_json_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "missed_entry_discovery_detail_path": str(detail_path),
        "missed_entry_session_phase_map_path": str(session_map_path),
        "missed_entry_candidate_clusters_path": str(cluster_path),
        "missed_entry_discovery_summary_path": str(summary_json_path),
    }


def _build_missed_turn_observations(
    *,
    turn_dataset_path: Path,
    replay_db_path: Path,
) -> list[MissedTurnObservation]:
    ordered_bars = _load_ordered_bar_context(replay_db_path)
    bar_index = {bar.timestamp.isoformat(): index for index, bar in enumerate(ordered_bars)}
    observations: list[MissedTurnObservation] = []

    with turn_dataset_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["material_turn"] != "True":
                continue
            if row["participation_classification"] == "good_entry":
                continue
            if row["session_phase"] not in RESEARCH_PHASES:
                continue
            turn_timestamp = datetime.fromisoformat(row["timestamp"])
            index = bar_index.get(turn_timestamp.isoformat())
            if index is None:
                continue
            current = ordered_bars[index]
            prior = ordered_bars[max(0, index - 6) : index]
            observations.append(
                MissedTurnObservation(
                    timestamp=turn_timestamp,
                    session_phase=row["session_phase"],
                    direction_of_turn=row["direction_of_turn"],
                    participation_classification=row["participation_classification"],
                    local_turn_type=row["local_turn_type"],
                    signal_family_if_any=row.get("signal_family_if_any") or None,
                    move_5bar=Decimal(row["move_5bar"]),
                    move_10bar=Decimal(row["move_10bar"]),
                    move_20bar=Decimal(row["move_20bar"]),
                    mfe_20bar=Decimal(row["mfe_20bar"]),
                    mae_20bar=Decimal(row["mae_20bar"]),
                    atr=Decimal(row["atr"]),
                    vwap_distance_atr=_safe_div(_decimal(row["vwap_distance"]), Decimal(row["atr"])),
                    vwap_bucket=_vwap_bucket(_safe_div(_decimal(row["vwap_distance"]), Decimal(row["atr"]))),
                    ema_relation=_ema_relation(current.close, current.turn_ema_fast, current.turn_ema_slow),
                    slope_bucket=row["slope_bucket"],
                    curvature_bucket=row["curvature_bucket"],
                    derivative_bucket=row["derivative_bucket"],
                    expansion_state=_expansion_state(current=current, prior=prior),
                    recent_path_shape=_recent_path_shape(direction=row["direction_of_turn"], prior=prior),
                    followthrough_quality=_followthrough_quality(move_5bar=Decimal(row["move_5bar"]), atr=Decimal(row["atr"])),
                    volatility_regime=row["volatility_regime"] or current.volatility_regime,
                    prior_return_signs_3=_return_sign_pattern(prior[-3:]),
                    prior_return_signs_5=_return_sign_pattern(prior[-5:]),
                    prior_vwap_extension_signs_3=_sign_pattern(_vwap_extensions(prior[-3:])),
                    prior_curvature_signs_3=_sign_pattern(item.normalized_curvature for item in prior[-3:]),
                    one_bar_rebound_before_signal=_one_bar_rebound(row["direction_of_turn"], prior),
                    two_bar_rebound_before_signal=_two_bar_rebound(row["direction_of_turn"], prior),
                    prior_3_any_below_vwap=any(_is_negative(value) for value in _vwap_extensions(prior[-3:])),
                    prior_3_all_above_vwap=all(_is_positive(value) for value in _vwap_extensions(prior[-3:])) if prior[-3:] else False,
                    prior_3_any_positive_curvature=any(item.normalized_curvature > 0 for item in prior[-3:]),
                    signal_close_location=_signal_close_location(current),
                    signal_body_to_range=_signal_body_to_range(current),
                )
            )

    return observations


def _load_ordered_bar_context(replay_db_path: Path) -> list[OrderedBarContext]:
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

    ordered: list[OrderedBarContext] = []
    for row in rows:
        payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
        atr = _payload_decimal(payload.get("atr")) or Decimal("0")
        bar_range = Decimal(str(row["high"])) - Decimal(str(row["low"]))
        range_expansion_ratio = _safe_div(bar_range, atr)
        ordered.append(
            OrderedBarContext(
                timestamp=datetime.fromisoformat(row["end_ts"]),
                open=Decimal(str(row["open"])),
                high=Decimal(str(row["high"])),
                low=Decimal(str(row["low"])),
                close=Decimal(str(row["close"])),
                atr=atr,
                vwap=_payload_decimal(payload.get("vwap")),
                turn_ema_fast=_payload_decimal(payload.get("turn_ema_fast")),
                turn_ema_slow=_payload_decimal(payload.get("turn_ema_slow")),
                normalized_slope=_safe_div(_payload_decimal(payload.get("velocity")) or Decimal("0"), atr) or Decimal("0"),
                normalized_curvature=_safe_div(_payload_decimal(payload.get("velocity_delta")) or Decimal("0"), atr) or Decimal("0"),
                range_expansion_ratio=range_expansion_ratio,
                volatility_regime=_volatility_regime(payload, range_expansion_ratio),
            )
        )
    return ordered


def _build_session_phase_rows(observations: list[MissedTurnObservation]) -> list[dict[str, Any]]:
    grouped: dict[str, list[MissedTurnObservation]] = defaultdict(list)
    for observation in observations:
        grouped[observation.session_phase].append(observation)

    rows: list[dict[str, Any]] = []
    for phase in RESEARCH_PHASES:
        phase_rows = grouped.get(phase, [])
        long_rows = [row for row in phase_rows if row.direction_of_turn == LONG]
        short_rows = [row for row in phase_rows if row.direction_of_turn == SHORT]
        rows.append(
            {
                "session_phase": phase,
                "missed_turn_count": len(phase_rows),
                "estimated_value_move10": _decimal_str(sum((row.move_10bar for row in phase_rows), Decimal("0"))),
                "estimated_value_mfe20": _decimal_str(sum((row.mfe_20bar for row in phase_rows), Decimal("0"))),
                "avg_mfe20": _decimal_str(_avg(row.mfe_20bar for row in phase_rows)),
                "long_missed_turn_count": len(long_rows),
                "short_missed_turn_count": len(short_rows),
                "top_long_path_shape": _top_label(long_rows, "recent_path_shape"),
                "top_short_path_shape": _top_label(short_rows, "recent_path_shape"),
                "top_long_derivative_bucket": _top_label(long_rows, "derivative_bucket"),
                "top_short_derivative_bucket": _top_label(short_rows, "derivative_bucket"),
            }
        )
    rows.sort(key=lambda row: Decimal(row["estimated_value_mfe20"]), reverse=True)
    return rows


def _build_candidate_family_rows(observations: list[MissedTurnObservation]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[MissedTurnObservation]] = defaultdict(list)
    for observation in observations:
        grouped[(observation.direction_of_turn, observation.session_phase, observation.recent_path_shape)].append(observation)

    rows: list[dict[str, Any]] = []
    for (direction, session_phase, recent_path_shape), family_rows in grouped.items():
        count = len(family_rows)
        if count < 20:
            continue
        rows.append(
            {
                "direction": direction,
                "session_phase": session_phase,
                "recent_path_shape": recent_path_shape,
                "missed_turn_count": count,
                "estimated_value_move10": _decimal_str(sum((row.move_10bar for row in family_rows), Decimal("0"))),
                "estimated_value_mfe20": _decimal_str(sum((row.mfe_20bar for row in family_rows), Decimal("0"))),
                "avg_move10": _decimal_str(_avg(row.move_10bar for row in family_rows)),
                "avg_mfe20": _decimal_str(_avg(row.mfe_20bar for row in family_rows)),
                "avg_mae20": _decimal_str(_avg(row.mae_20bar for row in family_rows)),
                "dominant_vwap_bucket": _top_label(family_rows, "vwap_bucket"),
                "dominant_vwap_bucket_share": _top_share(family_rows, "vwap_bucket"),
                "dominant_ema_relation": _top_label(family_rows, "ema_relation"),
                "dominant_ema_relation_share": _top_share(family_rows, "ema_relation"),
                "dominant_derivative_bucket": _top_label(family_rows, "derivative_bucket"),
                "dominant_derivative_bucket_share": _top_share(family_rows, "derivative_bucket"),
                "dominant_expansion_state": _top_label(family_rows, "expansion_state"),
                "dominant_expansion_state_share": _top_share(family_rows, "expansion_state"),
                "dominant_followthrough_quality": _top_label(family_rows, "followthrough_quality"),
                "dominant_followthrough_quality_share": _top_share(family_rows, "followthrough_quality"),
                "dominant_volatility_regime": _top_label(family_rows, "volatility_regime"),
                "dominant_volatility_regime_share": _top_share(family_rows, "volatility_regime"),
                "coherence_score": _decimal_str(_coherence_score(family_rows)),
                "noise_trap_flag": _noise_trap_flag(
                    count=count,
                    session_phase=session_phase,
                    coherence_score=_coherence_score(family_rows),
                    recent_path_shape=recent_path_shape,
                ),
            }
        )
    rows.sort(
        key=lambda row: (
            row["noise_trap_flag"] == "false",
            Decimal(row["estimated_value_mfe20"]),
            row["missed_turn_count"],
        ),
        reverse=True,
    )
    return rows


def _build_summary(
    observations: list[MissedTurnObservation],
    session_rows: list[dict[str, Any]],
    cluster_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    top_session_count = sorted(session_rows, key=lambda row: row["missed_turn_count"], reverse=True)[:5]
    top_session_value = sorted(session_rows, key=lambda row: Decimal(row["estimated_value_mfe20"]), reverse=True)[:5]
    long_clusters = [row for row in cluster_rows if row["direction"] == LONG and row["noise_trap_flag"] == "false"][:5]
    short_clusters = [row for row in cluster_rows if row["direction"] == SHORT and row["noise_trap_flag"] == "false"][:5]
    candidate_families = _top_candidate_families(cluster_rows)
    noise_traps = [row for row in cluster_rows if row["noise_trap_flag"] == "true"][:5]

    return {
        "missed_turn_count": len(observations),
        "top_session_phases_by_missed_count": top_session_count,
        "top_session_phases_by_estimated_value": top_session_value,
        "top_long_side_missed_clusters": long_clusters,
        "top_short_side_missed_clusters": short_clusters,
        "top_candidate_entry_families": candidate_families,
        "likely_noise_or_overfit_traps": noise_traps,
        "ranked_missed_turn_opportunity_summary": [
            "US_MIDDAY and LONDON_LATE are the biggest unexploited opportunity pools by both raw count and estimated 20-bar favorable excursion.",
            "The most coherent missed clusters are not tiny exotic patterns; they are repeatable pause/resume reversal shapes that recur across London late and US midday.",
            "US_OPEN_LATE remains economically real but small relative to full-clock missed opportunity, so it should not be the center of the next discovery branch.",
        ],
    }


def _top_candidate_families(cluster_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    viable = [row for row in cluster_rows if row["noise_trap_flag"] == "false"]
    shortlisted = viable[:3]
    results: list[dict[str, Any]] = []
    for row in shortlisted:
        why = (
            f"{row['session_phase']} {row['direction'].lower()} {row['recent_path_shape']} turns were frequent "
            f"({row['missed_turn_count']} missed turns) and high value ({row['estimated_value_mfe20']} total 20-bar MFE), "
            f"with the cluster anchored by {row['dominant_derivative_bucket']} and {row['dominant_ema_relation']} structure."
        )
        risk = (
            f"Main risk: follow-through skew is still {row['dominant_followthrough_quality']}, so a naive entry family here can overfire "
            f"into chop unless the eventual A/B keeps a narrow structural qualifier."
        )
        results.append(
            {
                "family_label": f"{row['session_phase']} {row['direction']} {row['recent_path_shape']}",
                "why_promising": why,
                "main_risk": risk,
            }
        )
    return results


def _observation_row(observation: MissedTurnObservation) -> dict[str, Any]:
    payload = asdict(observation)
    payload["timestamp"] = observation.timestamp.isoformat()
    for key, value in list(payload.items()):
        if isinstance(value, Decimal):
            payload[key] = str(value)
    return payload


def _recent_path_shape(direction: str, prior: list[OrderedBarContext]) -> str:
    if len(prior) < 3:
        return "other"
    return_signs = _return_sign_pattern(prior[-3:])
    vwap_extensions = _vwap_extensions(prior[-3:])
    all_above_vwap = all(_is_positive(value) for value in vwap_extensions)
    all_below_vwap = all(_is_negative(value) for value in vwap_extensions)
    any_positive_curvature = any(item.normalized_curvature > 0 for item in prior[-3:])
    any_negative_curvature = any(item.normalized_curvature < 0 for item in prior[-3:])

    if direction == SHORT:
        if return_signs == "UUU" and all_above_vwap:
            return "continuous_up_into_reversal"
        if return_signs in {"UDU", "DUU", "UUD"} and any_positive_curvature:
            return "pause_rebound_resume_short"
        if all_above_vwap and any_negative_curvature:
            return "late_stretch_short"
        return return_signs

    if return_signs == "DDD" and all_below_vwap:
        return "continuous_down_into_reversal"
    if return_signs in {"DUD", "UDD", "DDU"} and any_negative_curvature:
        return "pause_pullback_resume_long"
    if all_below_vwap and any_positive_curvature:
        return "late_stretch_long"
    return return_signs


def _ema_relation(price: Decimal, turn_ema_fast: Decimal | None, turn_ema_slow: Decimal | None) -> str:
    if turn_ema_fast is None or turn_ema_slow is None:
        return "na"
    if price > turn_ema_fast > turn_ema_slow:
        return "above_both_fast_gt_slow"
    if price < turn_ema_fast < turn_ema_slow:
        return "below_both_fast_lt_slow"
    if turn_ema_fast > turn_ema_slow and price < turn_ema_fast:
        return "pullback_above_slow"
    if turn_ema_fast < turn_ema_slow and price > turn_ema_fast:
        return "rebound_below_slow"
    return "mixed"


def _expansion_state(*, current: OrderedBarContext, prior: list[OrderedBarContext]) -> str:
    prior_values = [item.range_expansion_ratio for item in prior[-3:] if item.range_expansion_ratio is not None]
    prior_avg = _avg(prior_values)
    if current.range_expansion_ratio is not None and current.range_expansion_ratio >= Decimal("1.25"):
        if prior_avg is not None and prior_avg < Decimal("1.0"):
            return "fresh_expansion"
        return "already_expanded"
    if prior_avg is not None and prior_avg >= Decimal("1.25"):
        return "post_expansion"
    return "not_expanded"


def _followthrough_quality(*, move_5bar: Decimal, atr: Decimal) -> str:
    normalized = _safe_div(move_5bar, atr) or Decimal("0")
    if normalized >= Decimal("1.50"):
        return "strong"
    if normalized >= Decimal("0.75"):
        return "moderate"
    return "weak"


def _one_bar_rebound(direction: str, prior: list[OrderedBarContext]) -> bool:
    if len(prior) < 2:
        return False
    if direction == SHORT:
        return prior[-1].close > prior[-2].close
    return prior[-1].close < prior[-2].close


def _two_bar_rebound(direction: str, prior: list[OrderedBarContext]) -> bool:
    if len(prior) < 3:
        return False
    if direction == SHORT:
        return prior[-1].close > prior[-2].close and prior[-2].close > prior[-3].close
    return prior[-1].close < prior[-2].close and prior[-2].close < prior[-3].close


def _return_sign_pattern(rows: list[OrderedBarContext]) -> str:
    output: list[str] = []
    for row in rows:
        if row.close > row.open:
            output.append("U")
        elif row.close < row.open:
            output.append("D")
        else:
            output.append("F")
    return "".join(output)


def _sign_pattern(values: Iterable[Decimal | None]) -> str:
    output: list[str] = []
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


def _vwap_extensions(rows: list[OrderedBarContext]) -> list[Decimal | None]:
    return [_safe_div(row.close - row.vwap, row.atr) if row.vwap is not None else None for row in rows]


def _vwap_bucket(value: Decimal | None) -> str:
    if value is None:
        return "na"
    if value <= Decimal("-1.0"):
        return "<=-1atr"
    if value <= Decimal("-0.25"):
        return "-1to-.25atr"
    if value < Decimal("0.25"):
        return "near_vwap"
    if value < Decimal("1.0"):
        return ".25to1atr"
    return ">=1atr"


def _signal_close_location(row: OrderedBarContext) -> Decimal | None:
    return _safe_div(row.close - row.low, row.high - row.low)


def _signal_body_to_range(row: OrderedBarContext) -> Decimal | None:
    return _safe_div(abs(row.close - row.open), row.high - row.low)


def _coherence_score(rows: list[MissedTurnObservation]) -> Decimal:
    shares = [
        _top_share(rows, "ema_relation"),
        _top_share(rows, "vwap_bucket"),
        _top_share(rows, "derivative_bucket"),
        _top_share(rows, "expansion_state"),
    ]
    return _avg(Decimal(str(share)) for share in shares if share is not None) or Decimal("0")


def _noise_trap_flag(*, count: int, session_phase: str, coherence_score: Decimal, recent_path_shape: str) -> str:
    if session_phase in {"SESSION_RESET_1800", "US_OPEN_LATE", "US_PREOPEN_OPENING"} and count < 60:
        return "true"
    if count < 25:
        return "true"
    if coherence_score < Decimal("0.38"):
        return "true"
    if recent_path_shape in {"UUU", "DDD", "UUD", "DDU", "DUU", "UDD"} and count < 40:
        return "true"
    return "false"


def _top_label(rows: list[Any], attribute: str) -> str:
    if not rows:
        return ""
    counter = Counter(getattr(row, attribute) if hasattr(row, attribute) else row[attribute] for row in rows)
    return counter.most_common(1)[0][0]


def _top_share(rows: list[Any], attribute: str) -> float:
    if not rows:
        return 0.0
    counter = Counter(getattr(row, attribute) if hasattr(row, attribute) else row[attribute] for row in rows)
    return counter.most_common(1)[0][1] / len(rows)


def _volatility_regime(payload: dict[str, Any], range_expansion_ratio: Decimal | None) -> str:
    vol_ratio = _payload_decimal(payload.get("vol_ratio"))
    if range_expansion_ratio is not None and range_expansion_ratio >= Decimal("1.25"):
        return "HIGH"
    if vol_ratio is not None and vol_ratio >= Decimal("1.20"):
        return "HIGH"
    return "NORMAL"


def _payload_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, dict) and value.get("__type__") == "decimal":
        return Decimal(value["value"])
    return Decimal(str(value))


def _decimal(value: str | None) -> Decimal | None:
    if value in {None, ""}:
        return None
    return Decimal(value)


def _safe_div(numerator: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def _avg(values: Iterable[Decimal | None]) -> Decimal | None:
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None
    return sum(filtered, Decimal("0")) / Decimal(len(filtered))


def _decimal_str(value: Decimal | None) -> str:
    if value is None:
        return ""
    return str(value.quantize(Decimal("0.0001")) if value != value.to_integral_value() else value.quantize(Decimal("1")))


def _is_positive(value: Decimal | None) -> bool:
    return value is not None and value > 0


def _is_negative(value: Decimal | None) -> bool:
    return value is not None and value < 0


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
