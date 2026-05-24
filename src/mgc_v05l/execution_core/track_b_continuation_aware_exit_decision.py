"""Dry-run continuation-aware exit decision builder for Track B PAPER.

This module is deliberately pure: it does not submit, cancel, replace, modify,
close, flatten, mutate lifecycle state, or call broker/order planner APIs.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence


TIME_PLUS_CONTINUATION_EXIT_V1 = "TIME_PLUS_CONTINUATION_EXIT_V1"

HOLD_MINIMUM_WINDOW = "HOLD_MINIMUM_WINDOW"
HOLD_CONTINUATION_CONFIRMED = "HOLD_CONTINUATION_CONFIRMED"
EXIT_DECAY_DETECTED = "EXIT_DECAY_DETECTED"
EXIT_REVERSAL_DETECTED = "EXIT_REVERSAL_DETECTED"
EXIT_STAGNATION = "EXIT_STAGNATION"
EXIT_HARD_MAX_DURATION = "EXIT_HARD_MAX_DURATION"
EXIT_SAFE_STATE_OVERRIDE = "EXIT_SAFE_STATE_OVERRIDE"
EXIT_LIFECYCLE_UNSAFE = "EXIT_LIFECYCLE_UNSAFE"
INSUFFICIENT_DATA_HOLD_OR_FALLBACK = "INSUFFICIENT_DATA_HOLD_OR_FALLBACK"

SUPPORTED_STRATEGY_IDS = {
    "asian_drift_v1",
    "ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
}


_PROFILE_DEFAULTS: dict[str, dict[str, Any]] = {
    "ASIA_DRIFT_MEDIUM_LEASH": {
        "family_profile_id": "ASIA_DRIFT_MEDIUM_LEASH",
        "strategy_family": "asia_drift",
        "minimum_hold_minutes": 10,
        "continuation_extension_minutes": 15,
        "hard_max_hold_minutes": 35,
        "decay_threshold": Decimal("0.58"),
        "reversal_threshold": Decimal("0.65"),
        "stagnation_threshold": Decimal("0.62"),
    },
    "PAUSE_RESUME_MEDIUM_SHORT_LEASH": {
        "family_profile_id": "PAUSE_RESUME_MEDIUM_SHORT_LEASH",
        "strategy_family": "pause_resume",
        "minimum_hold_minutes": 10,
        "continuation_extension_minutes": 15,
        "hard_max_hold_minutes": 30,
        "decay_threshold": Decimal("0.52"),
        "reversal_threshold": Decimal("0.60"),
        "stagnation_threshold": Decimal("0.58"),
    },
}

_STRATEGY_PROFILE = {
    "asian_drift_v1": "ASIA_DRIFT_MEDIUM_LEASH",
    "ASIA_EARLY_PAUSE_RESUME_SHORT_V1": "PAUSE_RESUME_MEDIUM_SHORT_LEASH",
}


def build_time_plus_continuation_exit_decision(
    *,
    strategy_id: str,
    symbol: str,
    side: str,
    entry_time: datetime | str | None = None,
    current_time: datetime | str | None = None,
    completed_5m_candles: Sequence[Mapping[str, Any]] | None = None,
    position_age_minutes: int | float | Decimal | None = None,
    mfe: int | float | Decimal | str | None = None,
    mae: int | float | Decimal | str | None = None,
    unrealized_pnl: int | float | Decimal | str | None = None,
    microtrend_state: Mapping[str, Any] | str | None = None,
    participation_state: Mapping[str, Any] | str | None = None,
    safe_state_classification: str | None = "SAFE_STATE_NORMAL",
    lifecycle_reconciliation_classification: str | None = "CLEAN",
    family_profile_id: str | None = None,
) -> dict[str, Any]:
    """Build a dry-run exit decision for the first P0 continuation-aware slice."""

    actual_strategy_id = str(strategy_id or "").strip()
    profile = _profile_for(strategy_id=actual_strategy_id, family_profile_id=family_profile_id)
    actual_current_time = _parse_time(current_time) or datetime.now(UTC)
    actual_entry_time = _parse_time(entry_time)
    age_minutes = _position_age_minutes(
        explicit=position_age_minutes,
        entry_time=actual_entry_time,
        current_time=actual_current_time,
    )
    candles = tuple(completed_5m_candles or ())
    side_normalized = _normalize_side(side)
    mfe_decimal = _decimal_or_none(mfe)
    mae_decimal = _decimal_or_none(mae)
    pnl_decimal = _decimal_or_none(unrealized_pnl)

    scores = _scores(
        side=side_normalized,
        candles=candles,
        microtrend_state=microtrend_state,
        participation_state=participation_state,
        mfe=mfe_decimal,
        mae=mae_decimal,
        unrealized_pnl=pnl_decimal,
        age_minutes=age_minutes,
        profile=profile,
    )
    evidence = {
        "strategy_id": actual_strategy_id,
        "symbol": str(symbol or "").upper(),
        "side": side_normalized,
        "family_profile_id": profile["family_profile_id"],
        "strategy_family": profile["strategy_family"],
        "completed_5m_candle_count": len(candles),
        "position_age_minutes": _json_decimal(age_minutes),
        "mfe": _json_decimal(mfe_decimal),
        "mae": _json_decimal(mae_decimal),
        "unrealized_pnl": _json_decimal(pnl_decimal),
        "safe_state_classification": safe_state_classification,
        "lifecycle_reconciliation_classification": lifecycle_reconciliation_classification,
        "microtrend_state": microtrend_state,
        "participation_state": participation_state,
    }

    exit_state, reason = _classify(
        profile=profile,
        age_minutes=age_minutes,
        candles=candles,
        safe_state_classification=safe_state_classification,
        lifecycle_reconciliation_classification=lifecycle_reconciliation_classification,
        scores=scores,
    )
    min_remaining = _remaining(profile["minimum_hold_minutes"], age_minutes)
    hard_max_remaining = _remaining(profile["hard_max_hold_minutes"], age_minutes)
    return {
        "exit_policy_id": TIME_PLUS_CONTINUATION_EXIT_V1,
        "schema_version": "track_b_continuation_aware_exit_decision_v1",
        "strategy_id": actual_strategy_id,
        "symbol": str(symbol or "").upper(),
        "side": side_normalized,
        "family_profile_id": profile["family_profile_id"],
        "strategy_family": profile["strategy_family"],
        "dry_run_only": True,
        "not_order_authority": True,
        "not_lifecycle_authority": True,
        "dashboard_projection_consumed": False,
        "broker_mutation_allowed": False,
        "lifecycle_mutation_allowed": False,
        "should_request_close": False,
        "exit_state": exit_state,
        "reason": reason,
        "evidence_summary": evidence,
        "min_hold_remaining_minutes": _json_decimal(min_remaining),
        "hard_max_remaining_minutes": _json_decimal(hard_max_remaining),
        "continuation_quality_state": scores["continuation_quality_state"],
        "continuation_score": _json_decimal(scores["continuation_score"]),
        "decay_score": _json_decimal(scores["decay_score"]),
        "reversal_score": _json_decimal(scores["reversal_score"]),
        "stagnation_score": _json_decimal(scores["stagnation_score"]),
        "close_intent_preview": _close_intent_preview(
            strategy_id=actual_strategy_id,
            symbol=str(symbol or "").upper(),
            side=side_normalized,
            exit_state=exit_state,
            reason=reason,
        ),
    }


def _profile_for(*, strategy_id: str, family_profile_id: str | None) -> dict[str, Any]:
    requested = str(family_profile_id or "").strip().upper()
    profile_id = requested or _STRATEGY_PROFILE.get(strategy_id) or "ASIA_DRIFT_MEDIUM_LEASH"
    profile = _PROFILE_DEFAULTS.get(profile_id)
    if profile is None:
        profile = _PROFILE_DEFAULTS["ASIA_DRIFT_MEDIUM_LEASH"]
    return dict(profile)


def _classify(
    *,
    profile: Mapping[str, Any],
    age_minutes: Decimal | None,
    candles: Sequence[Mapping[str, Any]],
    safe_state_classification: str | None,
    lifecycle_reconciliation_classification: str | None,
    scores: Mapping[str, Decimal | str],
) -> tuple[str, str]:
    safe_state = str(safe_state_classification or "").upper()
    if safe_state and safe_state not in {"SAFE_STATE_NORMAL", "NORMAL", "CLEAN"}:
        return (
            EXIT_SAFE_STATE_OVERRIDE,
            f"Safe-State classification {safe_state_classification} overrides continuation-aware exit planning.",
        )

    lifecycle = str(lifecycle_reconciliation_classification or "").upper()
    if lifecycle and lifecycle not in {"CLEAN", "RECONCILED", "TRACK_B_PAPER_BROKER_RECONCILED", "SAFE"}:
        return (
            EXIT_LIFECYCLE_UNSAFE,
            f"Lifecycle/reconciliation classification {lifecycle_reconciliation_classification} is not clean.",
        )

    hard_max = Decimal(int(profile["hard_max_hold_minutes"]))
    if age_minutes is not None and age_minutes >= hard_max:
        return EXIT_HARD_MAX_DURATION, "Hard maximum hold duration reached in dry-run exit preview."

    if len(candles) < 2:
        return (
            INSUFFICIENT_DATA_HOLD_OR_FALLBACK,
            "At least two completed 5m candles are required for continuation-aware exit preview.",
        )

    minimum = Decimal(int(profile["minimum_hold_minutes"]))
    if age_minutes is None or age_minutes < minimum:
        return HOLD_MINIMUM_WINDOW, "Position is inside the minimum hold window."

    if Decimal(scores["reversal_score"]) >= Decimal(profile["reversal_threshold"]):
        return EXIT_REVERSAL_DETECTED, "Adverse reversal pressure detected in dry-run exit preview."
    if Decimal(scores["decay_score"]) >= Decimal(profile["decay_threshold"]):
        return EXIT_DECAY_DETECTED, "Directional continuation pressure is decaying in dry-run exit preview."
    if Decimal(scores["stagnation_score"]) >= Decimal(profile["stagnation_threshold"]):
        return EXIT_STAGNATION, "Position is stagnating after the minimum hold window."
    if Decimal(scores["continuation_score"]) >= Decimal("0.55"):
        return HOLD_CONTINUATION_CONFIRMED, "Aligned continuation remains confirmed inside the bounded leash."
    return EXIT_DECAY_DETECTED, "Continuation quality is below hold threshold after the minimum hold window."


def _scores(
    *,
    side: str,
    candles: Sequence[Mapping[str, Any]],
    microtrend_state: Mapping[str, Any] | str | None,
    participation_state: Mapping[str, Any] | str | None,
    mfe: Decimal | None,
    mae: Decimal | None,
    unrealized_pnl: Decimal | None,
    age_minutes: Decimal | None,
    profile: Mapping[str, Any],
) -> dict[str, Decimal | str]:
    candle_alignment = _candle_alignment(side=side, candles=candles[-3:])
    state_bias = _state_bias(microtrend_state) + _state_bias(participation_state)
    reversal_state_pressure = _reversal_state_pressure(microtrend_state) + _reversal_state_pressure(participation_state)
    pnl_bias = Decimal("0")
    if unrealized_pnl is not None:
        pnl_bias += Decimal("0.12") if unrealized_pnl > 0 else Decimal("-0.12") if unrealized_pnl < 0 else Decimal("0")
    if mfe is not None and mae is not None and mfe > 0:
        adverse_ratio = abs(mae) / max(abs(mfe), Decimal("0.01"))
        if adverse_ratio <= Decimal("0.35"):
            pnl_bias += Decimal("0.10")
        elif adverse_ratio >= Decimal("0.85"):
            pnl_bias -= Decimal("0.16")

    continuation = _clamp(Decimal("0.50") + candle_alignment + state_bias + pnl_bias)
    reversal = _clamp(
        Decimal("0.50") - candle_alignment + reversal_state_pressure + _negative_pnl_pressure(unrealized_pnl)
    )
    decay = _clamp(Decimal("0.55") - continuation + _mfe_decay_pressure(mfe=mfe, unrealized_pnl=unrealized_pnl))
    stagnation = Decimal("0")
    if age_minutes is not None:
        extension_start = Decimal(int(profile["minimum_hold_minutes"])) + Decimal(int(profile["continuation_extension_minutes"]))
        if age_minutes >= extension_start:
            stagnation += Decimal("0.48")
    if mfe is None or mfe <= Decimal("0"):
        stagnation += Decimal("0.18")
    if abs(candle_alignment) < Decimal("0.12"):
        stagnation += Decimal("0.18")
    stagnation = _clamp(stagnation)

    if reversal >= Decimal("0.65"):
        quality = "REVERSAL_PRESSURE"
    elif decay >= Decimal("0.58"):
        quality = "DECAYING"
    elif continuation >= Decimal("0.68"):
        quality = "STRONG_ALIGNED_CONTINUATION"
    elif continuation >= Decimal("0.55"):
        quality = "ALIGNED_CONTINUATION"
    else:
        quality = "WEAK_OR_STAGNANT"
    return {
        "continuation_score": continuation,
        "decay_score": decay,
        "reversal_score": reversal,
        "stagnation_score": stagnation,
        "continuation_quality_state": quality,
    }


def _candle_alignment(*, side: str, candles: Sequence[Mapping[str, Any]]) -> Decimal:
    if not candles:
        return Decimal("0")
    side_sign = Decimal("-1") if side == "SHORT" else Decimal("1")
    scores: list[Decimal] = []
    for candle in candles:
        open_price = _decimal_or_none(candle.get("open") or candle.get("o"))
        close_price = _decimal_or_none(candle.get("close") or candle.get("c"))
        high_price = _decimal_or_none(candle.get("high") or candle.get("h"))
        low_price = _decimal_or_none(candle.get("low") or candle.get("l"))
        if open_price is None or close_price is None:
            continue
        candle_range = Decimal("1")
        if high_price is not None and low_price is not None and high_price > low_price:
            candle_range = high_price - low_price
        body = (close_price - open_price) * side_sign
        scores.append(_clamp(body / max(candle_range, Decimal("0.01")), lower=Decimal("-0.35"), upper=Decimal("0.35")))
    if not scores:
        return Decimal("0")
    return sum(scores, Decimal("0")) / Decimal(len(scores))


def _state_bias(state: Mapping[str, Any] | str | None) -> Decimal:
    if state is None:
        return Decimal("0")
    values: list[str] = []
    if isinstance(state, Mapping):
        values = [str(value).upper() for value in state.values()]
    else:
        values = [str(state).upper()]
    joined = " ".join(values)
    if any(token in joined for token in ("REVERSAL", "ADVERSE", "OPPOSITE")):
        return Decimal("-0.28")
    if any(token in joined for token in ("DECAY", "WEAK", "FAILED", "FADING")):
        return Decimal("-0.12")
    if any(token in joined for token in ("STRONG", "ALIGNED", "CONTINUATION", "PARTICIPATING")):
        return Decimal("0.18")
    return Decimal("0")


def _reversal_state_pressure(state: Mapping[str, Any] | str | None) -> Decimal:
    if state is None:
        return Decimal("0")
    values: list[str]
    if isinstance(state, Mapping):
        values = [str(value).upper() for value in state.values()]
    else:
        values = [str(state).upper()]
    joined = " ".join(values)
    if any(token in joined for token in ("REVERSAL", "ADVERSE", "OPPOSITE")):
        return Decimal("0.22")
    return Decimal("0")


def _negative_pnl_pressure(unrealized_pnl: Decimal | None) -> Decimal:
    if unrealized_pnl is None or unrealized_pnl >= 0:
        return Decimal("0")
    return Decimal("0.12")


def _mfe_decay_pressure(*, mfe: Decimal | None, unrealized_pnl: Decimal | None) -> Decimal:
    if mfe is None or mfe <= 0 or unrealized_pnl is None:
        return Decimal("0")
    giveback = (mfe - unrealized_pnl) / max(abs(mfe), Decimal("0.01"))
    if giveback >= Decimal("0.70"):
        return Decimal("0.20")
    if giveback >= Decimal("0.45"):
        return Decimal("0.10")
    return Decimal("0")


def _close_intent_preview(*, strategy_id: str, symbol: str, side: str, exit_state: str, reason: str) -> dict[str, Any]:
    close_action = "BUY" if side == "SHORT" else "SELL"
    return {
        "dry_run_only": True,
        "not_order_authority": True,
        "not_lifecycle_authority": True,
        "strategy_id": strategy_id,
        "symbol": symbol,
        "position_side": side,
        "would_order_action": close_action,
        "would_close_reason": exit_state,
        "reason": reason,
        "would_submit": False,
        "would_cancel": False,
        "would_replace": False,
        "would_modify": False,
        "would_flatten": False,
    }


def _position_age_minutes(
    *,
    explicit: int | float | Decimal | None,
    entry_time: datetime | None,
    current_time: datetime,
) -> Decimal | None:
    explicit_decimal = _decimal_or_none(explicit)
    if explicit_decimal is not None:
        return explicit_decimal
    if entry_time is None:
        return None
    return Decimal(str(max(0.0, (current_time - entry_time).total_seconds() / 60.0)))


def _parse_time(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _normalize_side(side: str) -> str:
    raw = str(side or "").strip().upper()
    if raw in {"SELL", "SHORT"}:
        return "SHORT"
    return "LONG"


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _remaining(target: int, actual: Decimal | None) -> Decimal | None:
    if actual is None:
        return None
    return max(Decimal("0"), Decimal(target) - actual)


def _clamp(value: Decimal, *, lower: Decimal = Decimal("0"), upper: Decimal = Decimal("1")) -> Decimal:
    return min(upper, max(lower, value))


def _json_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value.normalize(), "f")
