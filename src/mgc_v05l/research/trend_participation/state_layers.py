"""Explicit ATP v1 bias and pullback state layers."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from statistics import median
from typing import Any, Iterable, Sequence

from .models import ResearchBar

LONG_BIAS = "LONG_BIAS"
SHORT_BIAS = "SHORT_BIAS"
NEUTRAL = "NEUTRAL"

NO_PULLBACK = "NO_PULLBACK"
NORMAL_PULLBACK = "NORMAL_PULLBACK"
STRETCHED_PULLBACK = "STRETCHED_PULLBACK"
VIOLENT_PULLBACK_DISQUALIFY = "VIOLENT_PULLBACK_DISQUALIFY"

SHALLOW = "SHALLOW"
STANDARD = "STANDARD"
DEEP_ACCEPTABLE = "DEEP_ACCEPTABLE"
ABNORMAL = "ABNORMAL"

EMA_FAST_SPAN = 4
EMA_SLOW_SPAN = 9
BIAS_MIN_SCORE = 3
SLOW_SLOPE_MIN_ATR = 0.05
PERSISTENCE_UP_THRESHOLD = 2
PERSISTENCE_DOWN_THRESHOLD = -2
EXTENSION_OVERSTRETCHED_ATR = 2.5
PULLBACK_LOOKBACK_BARS = 6
PULLBACK_MIN_RESET_ATR = 0.35
PULLBACK_MIN_RESET_IMPULSE = 0.20
PULLBACK_STANDARD_ATR = 0.75
PULLBACK_STANDARD_IMPULSE = 0.45
PULLBACK_STANDARD_REFERENCE = 0.80
PULLBACK_STRETCHED_MULTIPLE = 1.35
PULLBACK_STRETCHED_ATR = 1.15
PULLBACK_STRETCHED_IMPULSE = 0.60
PULLBACK_STRETCHED_REFERENCE = 1.10
PULLBACK_DISQUALIFY_MULTIPLE = 1.15
PULLBACK_DISQUALIFY_ATR = 1.50
PULLBACK_DISQUALIFY_IMPULSE = 0.75
PULLBACK_STRUCTURE_DAMAGE_RETRACE = 0.62
PULLBACK_VIOLENT_VELOCITY = 1.10
PULLBACK_VIOLENT_RANGE_EXPANSION = 1.35
PULLBACK_EXTREME_RANGE_EXPANSION = 1.90


@dataclass(frozen=True)
class BiasAssessment:
    state: str
    score: int
    long_score: int
    short_score: int
    reasons: tuple[str, ...]
    long_blockers: tuple[str, ...]
    short_blockers: tuple[str, ...]
    fast_ema: float
    slow_ema: float
    slow_ema_slope_norm: float
    session_vwap: float
    directional_persistence_score: int
    trend_extension_norm: float


@dataclass(frozen=True)
class PullbackAssessment:
    state: str
    envelope_state: str
    reason: str | None
    depth_points: float
    depth_score: float
    violence_score: float
    min_reset_depth: float
    standard_depth: float
    stretched_depth: float
    disqualify_depth: float
    retracement_ratio: float
    countertrend_velocity_norm: float
    countertrend_range_expansion: float
    structure_damage: bool
    reference_displacement: float


def classify_bias(
    *,
    bars: Sequence[ResearchBar],
    index: int,
    fast_ema: float,
    slow_ema: float,
    prev_slow_ema: float | None,
    session_vwap: float,
    atr: float,
) -> BiasAssessment:
    bar = bars[index]
    atr_floor = max(float(atr), 1e-9)
    previous_slow_ema = prev_slow_ema if prev_slow_ema is not None else slow_ema
    slope_norm = (slow_ema - previous_slow_ema) / atr_floor
    persistence_score = _directional_persistence_score(bars=bars, index=index, window=4)

    fast_above_slow = fast_ema > slow_ema
    fast_below_slow = fast_ema < slow_ema
    slope_up = slope_norm >= SLOW_SLOPE_MIN_ATR
    slope_down = slope_norm <= -SLOW_SLOPE_MIN_ATR
    close_above_vwap = bar.close >= session_vwap
    close_below_vwap = bar.close <= session_vwap
    persistence_up = persistence_score >= PERSISTENCE_UP_THRESHOLD
    persistence_down = persistence_score <= PERSISTENCE_DOWN_THRESHOLD

    long_score = sum((fast_above_slow, slope_up, close_above_vwap, persistence_up))
    short_score = sum((fast_below_slow, slope_down, close_below_vwap, persistence_down))

    long_extension_norm = max(bar.close - max(fast_ema, session_vwap), 0.0) / atr_floor
    short_extension_norm = max(min(fast_ema, session_vwap) - bar.close, 0.0) / atr_floor
    long_overstretched = long_extension_norm >= EXTENSION_OVERSTRETCHED_ATR
    short_overstretched = short_extension_norm >= EXTENSION_OVERSTRETCHED_ATR

    long_blockers = _bias_blockers(
        fast_relation_ok=fast_above_slow,
        slope_ok=slope_up,
        vwap_ok=close_above_vwap,
        persistence_ok=persistence_up,
        overstretched=long_overstretched,
        side="LONG",
    )
    short_blockers = _bias_blockers(
        fast_relation_ok=fast_below_slow,
        slope_ok=slope_down,
        vwap_ok=close_below_vwap,
        persistence_ok=persistence_down,
        overstretched=short_overstretched,
        side="SHORT",
    )

    state = NEUTRAL
    score = 0
    if long_score >= BIAS_MIN_SCORE and short_score <= 1 and not (long_overstretched and long_score == BIAS_MIN_SCORE):
        state = LONG_BIAS
        reasons = tuple(
            label
            for label, condition in (
                ("ema_aligned_up", fast_above_slow),
                ("slow_ema_rising", slope_up),
                ("close_above_vwap", close_above_vwap),
                ("directional_persistence_up", persistence_up),
            )
            if condition
        )
        if long_overstretched:
            reasons = reasons + ("upside_extension_elevated",)
        score = long_score
    elif short_score >= BIAS_MIN_SCORE and long_score <= 1 and not (short_overstretched and short_score == BIAS_MIN_SCORE):
        state = SHORT_BIAS
        reasons = tuple(
            label
            for label, condition in (
                ("ema_aligned_down", fast_below_slow),
                ("slow_ema_falling", slope_down),
                ("close_below_vwap", close_below_vwap),
                ("directional_persistence_down", persistence_down),
            )
            if condition
        )
        if short_overstretched:
            reasons = reasons + ("downside_extension_elevated",)
        score = -short_score
    elif long_score > short_score:
        reasons = tuple(long_blockers[:3]) or ("mixed_long_evidence",)
    elif short_score > long_score:
        reasons = tuple(short_blockers[:3]) or ("mixed_short_evidence",)
    else:
        reasons = ("mixed_directional_evidence",)

    trend_extension_norm = (
        long_extension_norm
        if state == LONG_BIAS
        else short_extension_norm if state == SHORT_BIAS else max(long_extension_norm, short_extension_norm)
    )
    return BiasAssessment(
        state=state,
        score=score,
        long_score=long_score,
        short_score=short_score,
        reasons=reasons,
        long_blockers=tuple(long_blockers),
        short_blockers=tuple(short_blockers),
        fast_ema=fast_ema,
        slow_ema=slow_ema,
        slow_ema_slope_norm=slope_norm,
        session_vwap=session_vwap,
        directional_persistence_score=persistence_score,
        trend_extension_norm=trend_extension_norm,
    )


def classify_pullback(
    *,
    bars: Sequence[ResearchBar],
    index: int,
    bias: BiasAssessment,
    atr: float,
) -> PullbackAssessment:
    atr_floor = max(float(atr), 1e-9)
    if bias.state == NEUTRAL or index <= 0:
        return PullbackAssessment(
            state=NO_PULLBACK,
            envelope_state=SHALLOW,
            reason="bias_neutral",
            depth_points=0.0,
            depth_score=0.0,
            violence_score=0.0,
            min_reset_depth=0.0,
            standard_depth=0.0,
            stretched_depth=0.0,
            disqualify_depth=0.0,
            retracement_ratio=0.0,
            countertrend_velocity_norm=0.0,
            countertrend_range_expansion=0.0,
            structure_damage=False,
            reference_displacement=0.0,
        )

    window = list(bars[max(0, index - PULLBACK_LOOKBACK_BARS + 1) : index + 1])
    bar = bars[index]
    previous_close = bars[index - 1].close
    earlier_close = bars[index - 2].close if index >= 2 else previous_close

    if bias.state == LONG_BIAS:
        impulse_extreme = max(item.high for item in window)
        impulse_counter_extreme = min(item.low for item in window)
        depth_points = max(impulse_extreme - bar.close, 0.0)
        reference_displacement = max(max(bias.fast_ema - bar.close, 0.0), max(bias.session_vwap - bar.close, 0.0))
        countertrend_velocity_norm = max(
            max(previous_close - bar.close, 0.0) / atr_floor,
            max(earlier_close - bar.close, 0.0) / (2.0 * atr_floor),
        )
        countertrend_range_expansion = _countertrend_range_expansion(
            bars=bars,
            index=index,
            atr=atr_floor,
            countertrend_is_down=True,
        )
        structure_damage = bar.close < bias.slow_ema and bar.close < bias.session_vwap
    else:
        impulse_extreme = min(item.low for item in window)
        impulse_counter_extreme = max(item.high for item in window)
        depth_points = max(bar.close - impulse_extreme, 0.0)
        reference_displacement = max(max(bar.close - bias.fast_ema, 0.0), max(bar.close - bias.session_vwap, 0.0))
        countertrend_velocity_norm = max(
            max(bar.close - previous_close, 0.0) / atr_floor,
            max(bar.close - earlier_close, 0.0) / (2.0 * atr_floor),
        )
        countertrend_range_expansion = _countertrend_range_expansion(
            bars=bars,
            index=index,
            atr=atr_floor,
            countertrend_is_down=False,
        )
        structure_damage = bar.close > bias.slow_ema and bar.close > bias.session_vwap

    impulse_size = max(abs(impulse_extreme - impulse_counter_extreme), atr_floor)
    retracement_ratio = depth_points / impulse_size
    if retracement_ratio < PULLBACK_STRUCTURE_DAMAGE_RETRACE:
        structure_damage = False

    min_reset_depth = max(PULLBACK_MIN_RESET_IMPULSE * impulse_size, PULLBACK_MIN_RESET_ATR * atr_floor)
    standard_depth = max(
        min_reset_depth,
        PULLBACK_STANDARD_IMPULSE * impulse_size,
        PULLBACK_STANDARD_ATR * atr_floor,
        PULLBACK_STANDARD_REFERENCE * reference_displacement,
    )
    stretched_depth = max(
        standard_depth * PULLBACK_STRETCHED_MULTIPLE,
        PULLBACK_STRETCHED_IMPULSE * impulse_size,
        PULLBACK_STRETCHED_ATR * atr_floor,
        PULLBACK_STRETCHED_REFERENCE * reference_displacement,
    )
    disqualify_depth = max(
        stretched_depth * PULLBACK_DISQUALIFY_MULTIPLE,
        PULLBACK_DISQUALIFY_IMPULSE * impulse_size,
        PULLBACK_DISQUALIFY_ATR * atr_floor,
    )
    violence_score = max(
        countertrend_velocity_norm,
        max(countertrend_range_expansion - 1.0, 0.0),
        reference_displacement / atr_floor,
        2.0 if structure_damage else 0.0,
    )

    if depth_points < min_reset_depth:
        state = NO_PULLBACK
        envelope_state = SHALLOW
        reason = "insufficient_retracement"
    elif structure_damage:
        state = VIOLENT_PULLBACK_DISQUALIFY
        envelope_state = ABNORMAL
        reason = "structure_damage"
    elif depth_points > stretched_depth:
        state = VIOLENT_PULLBACK_DISQUALIFY
        envelope_state = ABNORMAL
        reason = "retracement_exceeds_stretched_envelope"
    elif countertrend_velocity_norm >= PULLBACK_VIOLENT_VELOCITY and countertrend_range_expansion >= PULLBACK_VIOLENT_RANGE_EXPANSION:
        state = VIOLENT_PULLBACK_DISQUALIFY
        envelope_state = ABNORMAL
        reason = "countertrend_velocity_range_expansion"
    elif countertrend_range_expansion >= PULLBACK_EXTREME_RANGE_EXPANSION:
        state = VIOLENT_PULLBACK_DISQUALIFY
        envelope_state = ABNORMAL
        reason = "countertrend_range_expansion_extreme"
    elif depth_points <= standard_depth:
        state = NORMAL_PULLBACK
        envelope_state = STANDARD
        reason = None
    else:
        state = STRETCHED_PULLBACK
        envelope_state = DEEP_ACCEPTABLE
        reason = None

    return PullbackAssessment(
        state=state,
        envelope_state=envelope_state,
        reason=reason,
        depth_points=depth_points,
        depth_score=depth_points / max(standard_depth, atr_floor),
        violence_score=violence_score,
        min_reset_depth=min_reset_depth,
        standard_depth=standard_depth,
        stretched_depth=stretched_depth,
        disqualify_depth=disqualify_depth,
        retracement_ratio=retracement_ratio,
        countertrend_velocity_norm=countertrend_velocity_norm,
        countertrend_range_expansion=countertrend_range_expansion,
        structure_damage=structure_damage,
        reference_displacement=reference_displacement,
    )


def summarize_atp_state_diagnostics(feature_rows: Iterable[Any]) -> dict[str, Any]:
    rows = list(feature_rows)
    total = len(rows)
    bias_rows = [row for row in rows if getattr(row, "atp_bias_state", NEUTRAL) != NEUTRAL]
    session_keys = sorted({str(getattr(row, "session_segment", "UNKNOWN")) for row in rows})

    payload = {
        "bar_count": total,
        "bias_state_percent": _percentages(
            Counter(str(getattr(row, "atp_bias_state", NEUTRAL)) for row in rows),
            total=total,
        ),
        "pullback_state_percent_on_bias_bars": _percentages(
            Counter(str(getattr(row, "atp_pullback_state", NO_PULLBACK)) for row in bias_rows),
            total=len(bias_rows),
        ),
        "session_breakdown": {},
        "directional_persistence_breakdown": _percentages(
            Counter(str(getattr(row, "momentum_persistence", "UNKNOWN")) for row in rows),
            total=total,
        ),
        "top_violent_pullback_reasons": _top_reasons(
            str(getattr(row, "atp_pullback_reason", "") or "")
            for row in bias_rows
            if str(getattr(row, "atp_pullback_state", "")) == VIOLENT_PULLBACK_DISQUALIFY
        ),
        "top_neutral_reasons": _top_reasons(
            reason
            for row in rows
            if str(getattr(row, "atp_bias_state", NEUTRAL)) == NEUTRAL
            for reason in (getattr(row, "atp_bias_reasons", ()) or ())
        ),
    }
    for session in session_keys:
        session_rows = [row for row in rows if str(getattr(row, "session_segment", "UNKNOWN")) == session]
        session_bias_rows = [row for row in session_rows if str(getattr(row, "atp_bias_state", NEUTRAL)) != NEUTRAL]
        payload["session_breakdown"][session] = {
            "bar_count": len(session_rows),
            "bias_state_percent": _percentages(
                Counter(str(getattr(row, "atp_bias_state", NEUTRAL)) for row in session_rows),
                total=len(session_rows),
            ),
            "pullback_state_percent_on_bias_bars": _percentages(
                Counter(str(getattr(row, "atp_pullback_state", NO_PULLBACK)) for row in session_bias_rows),
                total=len(session_bias_rows),
            ),
        }
    return payload


def render_atp_state_diagnostics_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# ATP Phase 1 State Diagnostics",
        "",
        f"- Bars: {summary.get('bar_count')}",
        "",
        "## Bias State Percent",
    ]
    for key, value in sorted((summary.get("bias_state_percent") or {}).items()):
        lines.append(f"- {key}: {value}%")
    lines.extend(["", "## Pullback State Percent On Bias Bars"])
    for key, value in sorted((summary.get("pullback_state_percent_on_bias_bars") or {}).items()):
        lines.append(f"- {key}: {value}%")
    lines.extend(["", "## Session Breakdown"])
    for session, payload in sorted((summary.get("session_breakdown") or {}).items()):
        lines.append(f"- {session}: bars={payload.get('bar_count')}")
        for key, value in sorted((payload.get("bias_state_percent") or {}).items()):
            lines.append(f"  - bias {key}: {value}%")
        for key, value in sorted((payload.get("pullback_state_percent_on_bias_bars") or {}).items()):
            lines.append(f"  - pullback {key}: {value}%")
    lines.extend(["", "## Top Neutral Reasons"])
    neutral_reasons = summary.get("top_neutral_reasons") or []
    if not neutral_reasons:
        lines.append("- none")
    else:
        for row in neutral_reasons:
            lines.append(f"- {row['reason']}: {row['count']}")
    lines.extend(["", "## Top Violent Pullback Reasons"])
    violent_reasons = summary.get("top_violent_pullback_reasons") or []
    if not violent_reasons:
        lines.append("- none")
    else:
        for row in violent_reasons:
            lines.append(f"- {row['reason']}: {row['count']}")
    return "\n".join(lines) + "\n"


def latest_atp_state_summary(feature: Any | None) -> dict[str, Any]:
    if feature is None:
        return {
            "bias_state": NEUTRAL,
            "bias_reasons": [],
            "pullback_state": NO_PULLBACK,
            "pullback_envelope_state": SHALLOW,
            "pullback_depth_score": 0.0,
            "pullback_violence_score": 0.0,
            "pullback_reason": "missing_feature_state",
            "standard_pullback_envelope": {
                "min_reset_depth": 0.0,
                "standard_depth": 0.0,
                "stretched_depth": 0.0,
                "disqualify_depth": 0.0,
            },
        }
    return {
        "bias_state": str(getattr(feature, "atp_bias_state", NEUTRAL)),
        "bias_score": int(getattr(feature, "atp_bias_score", 0) or 0),
        "bias_reasons": list(getattr(feature, "atp_bias_reasons", ()) or ()),
        "long_bias_blockers": list(getattr(feature, "atp_long_bias_blockers", ()) or ()),
        "short_bias_blockers": list(getattr(feature, "atp_short_bias_blockers", ()) or ()),
        "pullback_state": str(getattr(feature, "atp_pullback_state", NO_PULLBACK)),
        "pullback_envelope_state": str(getattr(feature, "atp_pullback_envelope_state", SHALLOW)),
        "pullback_reason": getattr(feature, "atp_pullback_reason", None),
        "pullback_depth_score": round(float(getattr(feature, "atp_pullback_depth_score", 0.0) or 0.0), 4),
        "pullback_violence_score": round(float(getattr(feature, "atp_pullback_violence_score", 0.0) or 0.0), 4),
        "standard_pullback_envelope": {
            "min_reset_depth": round(float(getattr(feature, "atp_pullback_min_reset_depth", 0.0) or 0.0), 4),
            "standard_depth": round(float(getattr(feature, "atp_pullback_standard_depth", 0.0) or 0.0), 4),
            "stretched_depth": round(float(getattr(feature, "atp_pullback_stretched_depth", 0.0) or 0.0), 4),
            "disqualify_depth": round(float(getattr(feature, "atp_pullback_disqualify_depth", 0.0) or 0.0), 4),
        },
    }


def _bias_blockers(
    *,
    fast_relation_ok: bool,
    slope_ok: bool,
    vwap_ok: bool,
    persistence_ok: bool,
    overstretched: bool,
    side: str,
) -> list[str]:
    prefix = str(side).upper()
    blockers: list[str] = []
    if not fast_relation_ok:
        blockers.append(f"{prefix}_EMA_ALIGNMENT_FAILED")
    if not slope_ok:
        blockers.append(f"{prefix}_SLOW_SLOPE_FAILED")
    if not vwap_ok:
        blockers.append(f"{prefix}_VWAP_LOCATION_FAILED")
    if not persistence_ok:
        blockers.append(f"{prefix}_PERSISTENCE_FAILED")
    if overstretched:
        blockers.append(f"{prefix}_REFERENCE_EXTENSION_OVERSTRETCHED")
    return blockers


def _directional_persistence_score(*, bars: Sequence[ResearchBar], index: int, window: int) -> int:
    start = max(1, index - window + 1)
    score = 0
    for cursor in range(start, index + 1):
        current = bars[cursor].close
        previous = bars[cursor - 1].close
        if current > previous:
            score += 1
        elif current < previous:
            score -= 1
    return score


def _countertrend_range_expansion(
    *,
    bars: Sequence[ResearchBar],
    index: int,
    atr: float,
    countertrend_is_down: bool,
) -> float:
    window = list(bars[max(0, index - 1) : index + 1])
    expansions = []
    for bar in window:
        if countertrend_is_down and bar.close > bar.open:
            continue
        if not countertrend_is_down and bar.close < bar.open:
            continue
        expansions.append(bar.range_points / max(atr, 1e-9))
    return max(expansions) if expansions else 0.0


def _percentages(counter: Counter[str], *, total: int) -> dict[str, float]:
    if total <= 0:
        return {}
    return {
        key: round((count / total) * 100.0, 2)
        for key, count in sorted(counter.items())
    }


def _top_reasons(reasons: Iterable[str]) -> list[dict[str, Any]]:
    counter = Counter(reason for reason in reasons if reason)
    return [
        {"reason": reason, "count": count}
        for reason, count in counter.most_common(5)
    ]


def rolling_ema(values: Sequence[float], *, span: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (float(span) + 1.0)
    ema_values = [float(values[0])]
    for value in values[1:]:
        ema_values.append(alpha * float(value) + (1.0 - alpha) * ema_values[-1])
    return ema_values


def rolling_atr(bars: Sequence[ResearchBar], *, window: int = 8) -> list[float]:
    if not bars:
        return []
    true_ranges: list[float] = []
    for index, bar in enumerate(bars):
        if index == 0:
            true_ranges.append(bar.range_points)
            continue
        previous_close = bars[index - 1].close
        true_ranges.append(
            max(
                bar.high - bar.low,
                abs(bar.high - previous_close),
                abs(bar.low - previous_close),
            )
        )
    atr_values: list[float] = []
    for index in range(len(true_ranges)):
        window_values = true_ranges[max(0, index - window + 1) : index + 1]
        atr_values.append(float(median(window_values)))
    return atr_values
