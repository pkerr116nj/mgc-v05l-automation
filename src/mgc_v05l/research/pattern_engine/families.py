"""Pattern Engine v1 family specifications."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .dataset import PatternEngineContext
from .phases import PhaseSequenceMatch, PhaseStep
from .primitives import PatternPrimitivePoint


@dataclass(frozen=True)
class PatternFamilySpec:
    name: str
    description: str
    matcher: Callable[[list[PatternEngineContext], list[PatternPrimitivePoint], int], PhaseSequenceMatch | None]


def default_pattern_family_specs() -> tuple[PatternFamilySpec, ...]:
    return (
        PatternFamilySpec("pause_pullback_resume_long", "Upside setup, one-bar pullback, then upside resumption.", _match_pause_pullback_resume_long),
        PatternFamilySpec("pause_rebound_resume_short", "Downside setup, one-bar rebound, then downside resumption.", _match_pause_rebound_resume_short),
        PatternFamilySpec("breakout_retest_hold", "Breakout, retest of the breakout level, then hold.", _match_breakout_retest_hold),
        PatternFamilySpec("failed_move_reversal", "Failed move through prior extremes that reverses back through the level.", _match_failed_move_reversal),
    )


def _match_pause_pullback_resume_long(
    contexts: list[PatternEngineContext],
    primitives: list[PatternPrimitivePoint],
    index: int,
) -> PhaseSequenceMatch | None:
    if index < 2:
        return None
    current = primitives[index]
    previous = primitives[index - 1]
    setup = primitives[index - 2]
    if setup.slope_state not in {"SLOPE_POS", "SLOPE_FLAT"}:
        return None
    if current.curvature_state != "CURVATURE_POS":
        return None
    if current.expansion_state == "EXPANDED":
        return None
    if previous.pullback_state != "ONE_BAR_PULLBACK":
        return None
    if current.breakout_state != "BREAK_ABOVE_PRIOR_HIGH":
        return None
    if current.ema_location_state not in {"REBOUND_BELOW_SLOW", "ABOVE_BOTH_FAST_GT_SLOW"}:
        return None
    return _sequence("pause_pullback_resume_long", "LONG", contexts, index, (("setup", index - 2, setup), ("pullback", index - 1, previous), ("resumption", index, current), ("confirmation", index, current)))


def _match_pause_rebound_resume_short(
    contexts: list[PatternEngineContext],
    primitives: list[PatternPrimitivePoint],
    index: int,
) -> PhaseSequenceMatch | None:
    if index < 2:
        return None
    current = primitives[index]
    previous = primitives[index - 1]
    setup = primitives[index - 2]
    if setup.slope_state not in {"SLOPE_NEG", "SLOPE_FLAT"}:
        return None
    if current.curvature_state != "CURVATURE_NEG":
        return None
    if current.expansion_state == "EXPANDED":
        return None
    if previous.pullback_state != "ONE_BAR_REBOUND":
        return None
    if current.breakout_state != "BREAK_BELOW_PRIOR_LOW":
        return None
    if current.ema_location_state not in {"REBOUND_ABOVE_SLOW", "BELOW_BOTH_FAST_LT_SLOW"}:
        return None
    return _sequence("pause_rebound_resume_short", "SHORT", contexts, index, (("setup", index - 2, setup), ("rebound", index - 1, previous), ("resumption", index, current), ("confirmation", index, current)))


def _match_breakout_retest_hold(
    contexts: list[PatternEngineContext],
    primitives: list[PatternPrimitivePoint],
    index: int,
) -> PhaseSequenceMatch | None:
    if index < 2:
        return None
    current_context = contexts[index]
    previous_context = contexts[index - 1]
    current = primitives[index]
    previous = primitives[index - 1]
    if previous.breakout_state == "BREAK_ABOVE_PRIOR_HIGH":
        if current_context.low <= previous_context.high and current_context.close >= previous_context.high:
            return _sequence("breakout_retest_hold", "LONG", contexts, index, (("breakout", index - 1, previous), ("retest", index, current), ("hold", index, current)))
    if previous.breakout_state == "BREAK_BELOW_PRIOR_LOW":
        if current_context.high >= previous_context.low and current_context.close <= previous_context.low:
            return _sequence("breakout_retest_hold", "SHORT", contexts, index, (("breakout", index - 1, previous), ("retest", index, current), ("hold", index, current)))
    return None


def _match_failed_move_reversal(
    contexts: list[PatternEngineContext],
    primitives: list[PatternPrimitivePoint],
    index: int,
) -> PhaseSequenceMatch | None:
    if index < 1:
        return None
    current = primitives[index]
    previous = primitives[index - 1]
    if current.failure_state == "FAILED_UP_BREAK" and current.curvature_state == "CURVATURE_NEG":
        return _sequence("failed_move_reversal", "SHORT", contexts, index, (("failed_move", index - 1, previous), ("reversal", index, current), ("confirmation", index, current)))
    if current.failure_state == "FAILED_DOWN_BREAK" and current.curvature_state == "CURVATURE_POS":
        return _sequence("failed_move_reversal", "LONG", contexts, index, (("failed_move", index - 1, previous), ("reversal", index, current), ("confirmation", index, current)))
    return None


def _sequence(
    family_name: str,
    direction: str,
    contexts: list[PatternEngineContext],
    index: int,
    steps: tuple[tuple[str, int, PatternPrimitivePoint], ...],
) -> PhaseSequenceMatch:
    return PhaseSequenceMatch(
        family_name=family_name,
        direction=direction,
        session_phase=contexts[index].session_phase,
        anchor_timestamp=contexts[index].timestamp.isoformat(),
        steps=tuple(
            PhaseStep(
                phase_name=phase_name,
                timestamp=contexts[phase_index].timestamp.isoformat(),
                primitive_signature=_primitive_signature(primitive),
            )
            for phase_name, phase_index, primitive in steps
        ),
    )


def _primitive_signature(primitive: PatternPrimitivePoint) -> str:
    return "|".join(
        [
            primitive.slope_state,
            primitive.curvature_state,
            primitive.expansion_state,
            primitive.ema_location_state,
            primitive.breakout_state,
        ]
    )
