"""Track B managed exit strategy roster.

Exit profiles are reusable managed-lifecycle instructions. They do not submit
orders by themselves; apply-capable boundaries consume a resolved profile,
build a close-intent preview, validate shared authority, and only then call the
guarded broker path.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping


PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1 = "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
MNQ_SNAP_TURN_TIMEBOX_3X5M_V1 = "MNQ_SNAP_TURN_TIMEBOX_3X5M_V1"
TIMEBOXED_3X5M_MANAGED_LIMIT_CLOSE_V1 = "timeboxed_3x5m_managed_limit_close_v1"


@dataclass(frozen=True)
class TrackBExitProfile:
    exit_strategy_id: str
    exit_profile_id: str
    managed_exit_policy_id: str
    instrument_family: str
    strategy_family: str
    order_type: str
    required_completed_5m_bars: int
    price_offset_ticks: int
    tick_size: str
    profile_explanation: str
    paper_only: bool = True
    live_money_eligible: bool = False
    paper_proof_allowed: bool = False
    broad_cancel_allowed: bool = False
    global_flatten_allowed: bool = False

    def to_json_dict(self) -> dict[str, object]:
        return {
            "exit_strategy_id": self.exit_strategy_id,
            "exit_profile_id": self.exit_profile_id,
            "managed_exit_policy_id": self.managed_exit_policy_id,
            "instrument_family": self.instrument_family,
            "strategy_family": self.strategy_family,
            "order_type": self.order_type,
            "required_completed_5m_bars": self.required_completed_5m_bars,
            "price_offset_ticks": self.price_offset_ticks,
            "tick_size": self.tick_size,
            "profile_explanation": self.profile_explanation,
            "paper_only": self.paper_only,
            "live_money_eligible": self.live_money_eligible,
            "paper_proof_allowed": self.paper_proof_allowed,
            "broad_cancel_allowed": self.broad_cancel_allowed,
            "global_flatten_allowed": self.global_flatten_allowed,
        }


EXIT_PROFILE_ROSTER: Mapping[str, TrackBExitProfile] = {
    MNQ_SNAP_TURN_TIMEBOX_3X5M_V1: TrackBExitProfile(
        exit_strategy_id=TIMEBOXED_3X5M_MANAGED_LIMIT_CLOSE_V1,
        exit_profile_id=MNQ_SNAP_TURN_TIMEBOX_3X5M_V1,
        managed_exit_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
        instrument_family="MNQ",
        strategy_family="snap_turn",
        order_type="LMT",
        required_completed_5m_bars=3,
        price_offset_ticks=2,
        tick_size="0.25",
        profile_explanation=(
            "PAPER diagnostic snap-turn close profile: after three completed 5m bars, "
            "submit the managed lifecycle close as an exact SELL/BUY limit order for the owned position."
        ),
    )
}


def resolve_track_b_exit_profile(exit_profile_id: str) -> TrackBExitProfile:
    try:
        return EXIT_PROFILE_ROSTER[exit_profile_id]
    except KeyError as exc:
        raise ValueError(f"Unknown Track B exit profile: {exit_profile_id}") from exc


def close_action_for_position_side(side: str) -> str:
    normalized = str(side or "").strip().upper()
    if normalized in {"LONG", "BUY", "BUY_TO_OPEN"}:
        return "SELL"
    if normalized in {"SHORT", "SELL", "SELL_TO_OPEN"}:
        return "BUY"
    raise ValueError(f"Unsupported managed exit side: {side}")


def close_limit_from_profile(*, latest_price: str | None, side: str, profile: TrackBExitProfile) -> str | None:
    if latest_price in {None, ""}:
        return None
    latest = Decimal(str(latest_price))
    tick = Decimal(str(profile.tick_size))
    offset = tick * Decimal(max(profile.price_offset_ticks, 0))
    raw = latest - offset if close_action_for_position_side(side) == "SELL" else latest + offset
    rounded = (raw / tick).to_integral_value() * tick
    return format(rounded.normalize(), "f")
