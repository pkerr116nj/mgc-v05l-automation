"""PAPER-only operational maturation helpers for sparse probationary lanes."""

from __future__ import annotations

from typing import Any


OPERATIONAL_MATURATION_PROFILE = "PAPER_ONLY_OPERATIONAL_MATURATION_V1"


def operational_maturation_params(lane_spec: Any) -> dict[str, Any]:
    params = dict(getattr(lane_spec, "runtime_overlay_params", {}) or {})
    if not params.get("operational_maturation_mode"):
        return {}
    if str(params.get("operational_maturation_profile") or "") != OPERATIONAL_MATURATION_PROFILE:
        return {}
    if not bool(getattr(lane_spec, "paper_only", True)):
        return {}
    if params.get("paper_only") is False:
        return {}
    if bool(getattr(lane_spec, "live_money_eligible", False)) or bool(params.get("live_money_eligible", False)):
        return {}
    return params


def operational_maturation_enabled(lane_spec: Any) -> bool:
    return bool(operational_maturation_params(lane_spec))


def operational_entry_reason(
    *,
    lane_spec: Any,
    segment_bars: list[Any],
    current_index: int,
    setup_bar_count: int,
    tick_size: float,
) -> str | None:
    params = operational_maturation_params(lane_spec)
    if not params:
        return None
    if current_index < setup_bar_count:
        return None

    forced_bar = _coerce_int(params.get("operational_maturation_forced_entry_bar"), default=5)
    forced_bar = max(setup_bar_count + 1, min(forced_bar, 12))
    entry_index = forced_bar - 1
    catchup_bars = max(0, min(_coerce_int(params.get("operational_maturation_entry_catchup_bars"), default=0), 3))
    if current_index < entry_index or current_index > entry_index + catchup_bars:
        return None

    min_setup_range_ticks = max(
        0,
        min(_coerce_int(params.get("operational_maturation_min_setup_range_ticks"), default=2), 20),
    )
    if min_setup_range_ticks > 0 and not _setup_range_passes(
        segment_bars=segment_bars,
        setup_bar_count=setup_bar_count,
        tick_size=tick_size,
        min_setup_range_ticks=min_setup_range_ticks,
    ):
        return None

    return f"operational_maturation_timed_bar{forced_bar}_catchup{catchup_bars}"


def _setup_range_passes(
    *,
    segment_bars: list[Any],
    setup_bar_count: int,
    tick_size: float,
    min_setup_range_ticks: int,
) -> bool:
    if len(segment_bars) < setup_bar_count:
        return False
    setup_bars = segment_bars[:setup_bar_count]
    setup_high = max(float(candidate.high) for candidate in setup_bars)
    setup_low = min(float(candidate.low) for candidate in setup_bars)
    return (setup_high - setup_low) >= float(tick_size) * float(min_setup_range_ticks)


def _coerce_int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
