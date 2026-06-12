"""Track B managed exit strategy roster.

Exit profiles are reusable managed-lifecycle instructions. They do not submit
orders by themselves; apply-capable boundaries consume a resolved profile,
build a close-intent preview, validate shared authority, and only then call the
guarded broker path.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping


PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1 = "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1 = "FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1"
MNQ_SNAP_TURN_TIMEBOX_3X5M_V1 = "MNQ_SNAP_TURN_TIMEBOX_3X5M_V1"
MNQ_CHANGEOVER_0300_LONG_TIMEBOX_6H_V1 = "MNQ_CHANGEOVER_0300_LONG_TIMEBOX_6H_V1"
MES_CHANGEOVER_0300_LONG_TIMEBOX_6H_V1 = "MES_CHANGEOVER_0300_LONG_TIMEBOX_6H_V1"
MNQ_CHANGEOVER_0700_LONG_TIMEBOX_4H_V1 = "MNQ_CHANGEOVER_0700_LONG_TIMEBOX_4H_V1"
MES_CHANGEOVER_0700_LONG_TIMEBOX_4H_V1 = "MES_CHANGEOVER_0700_LONG_TIMEBOX_4H_V1"
MNQ_US_SESSION_CONTINUATION_TIMEBOX_2H_V1 = "MNQ_US_SESSION_CONTINUATION_TIMEBOX_2H_V1"
MES_US_SESSION_CONTINUATION_TIMEBOX_2H_V1 = "MES_US_SESSION_CONTINUATION_TIMEBOX_2H_V1"
MNQ_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1 = "MNQ_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1"
MES_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1 = "MES_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1"
MNQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1 = "MNQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1"
MES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1 = "MES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1"
MNQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_V1 = "MNQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_V1"
MES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_V1 = "MES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_V1"
MNQ_GLOBEX_REOPEN_FIRST_CANDLE_TIMEBOX_60M_SHADOW_V1 = "MNQ_GLOBEX_REOPEN_FIRST_CANDLE_TIMEBOX_60M_SHADOW_V1"
MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1 = "MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1"
MGC_FORCED_SESSION_SEGMENT_TIMEBOX_3X5M_V1 = "MGC_FORCED_SESSION_SEGMENT_TIMEBOX_3X5M_V1"
TIMEBOXED_3X5M_MANAGED_LIMIT_CLOSE_V1 = "timeboxed_3x5m_managed_limit_close_v1"
TIMEBOXED_MANAGED_LIMIT_CLOSE_V1 = "timeboxed_managed_limit_close_v1"
ACTIVE_EVIDENCE_MANAGED_CLOSE_OFFSET_TICKS = 8
ACTIVE_EVIDENCE_MANAGED_CLOSE_MAX_SLIPPAGE_TICKS = 16
ACTIVE_EVIDENCE_MANAGED_CLOSE_REPRICE_ESCALATION_TICKS = 4
ACTIVE_EVIDENCE_MANAGED_CLOSE_WIDEN_AFTER_SECONDS = 60
ACTIVE_EVIDENCE_MANAGED_CLOSE_STALE_AFTER_SECONDS = 120


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
    max_slippage_ticks: int | None = None
    reprice_escalation_ticks: int = 0
    stale_reference_seconds: int | None = None
    widen_reference_seconds: int | None = None
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
            "max_slippage_ticks": self.max_slippage_ticks,
            "reprice_escalation_ticks": self.reprice_escalation_ticks,
            "stale_reference_seconds": self.stale_reference_seconds,
            "widen_reference_seconds": self.widen_reference_seconds,
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
    ),
    MNQ_CHANGEOVER_0300_LONG_TIMEBOX_6H_V1: TrackBExitProfile(
        exit_strategy_id=TIMEBOXED_MANAGED_LIMIT_CLOSE_V1,
        exit_profile_id=MNQ_CHANGEOVER_0300_LONG_TIMEBOX_6H_V1,
        managed_exit_policy_id="CHANGEOVER_0300_LONG_TIMEBOX_6H_EXIT_V1",
        instrument_family="MNQ",
        strategy_family="changeover_continuation",
        order_type="LMT",
        required_completed_5m_bars=72,
        price_offset_ticks=2,
        tick_size="0.25",
        profile_explanation=(
            "PAPER evidence-generation changeover close profile: after seventy-two completed 5m bars "
            "(about six hours), submit the managed lifecycle close as an exact opposite-side limit order "
            "for the owned MNQ position."
        ),
    ),
    MES_CHANGEOVER_0300_LONG_TIMEBOX_6H_V1: TrackBExitProfile(
        exit_strategy_id=TIMEBOXED_MANAGED_LIMIT_CLOSE_V1,
        exit_profile_id=MES_CHANGEOVER_0300_LONG_TIMEBOX_6H_V1,
        managed_exit_policy_id="CHANGEOVER_0300_LONG_TIMEBOX_6H_EXIT_V1",
        instrument_family="MES",
        strategy_family="changeover_continuation",
        order_type="LMT",
        required_completed_5m_bars=72,
        price_offset_ticks=2,
        tick_size="0.25",
        profile_explanation=(
            "PAPER evidence-generation changeover close profile: after seventy-two completed 5m bars "
            "(about six hours), submit the managed lifecycle close as an exact opposite-side limit order "
            "for the owned MES position."
        ),
    ),
    MNQ_CHANGEOVER_0700_LONG_TIMEBOX_4H_V1: TrackBExitProfile(
        exit_strategy_id=TIMEBOXED_MANAGED_LIMIT_CLOSE_V1,
        exit_profile_id=MNQ_CHANGEOVER_0700_LONG_TIMEBOX_4H_V1,
        managed_exit_policy_id="CHANGEOVER_0700_LONG_TIMEBOX_4H_EXIT_V1",
        instrument_family="MNQ",
        strategy_family="changeover_continuation",
        order_type="LMT",
        required_completed_5m_bars=48,
        price_offset_ticks=2,
        tick_size="0.25",
        profile_explanation=(
            "PAPER evidence-generation Europe-to-US changeover close profile: after forty-eight "
            "completed 5m bars (about four hours), submit the managed lifecycle close as an exact "
            "opposite-side limit order for the owned MNQ position."
        ),
    ),
    MES_CHANGEOVER_0700_LONG_TIMEBOX_4H_V1: TrackBExitProfile(
        exit_strategy_id=TIMEBOXED_MANAGED_LIMIT_CLOSE_V1,
        exit_profile_id=MES_CHANGEOVER_0700_LONG_TIMEBOX_4H_V1,
        managed_exit_policy_id="CHANGEOVER_0700_LONG_TIMEBOX_4H_EXIT_V1",
        instrument_family="MES",
        strategy_family="changeover_continuation",
        order_type="LMT",
        required_completed_5m_bars=48,
        price_offset_ticks=2,
        tick_size="0.25",
        profile_explanation=(
            "PAPER evidence-generation Europe-to-US changeover close profile: after forty-eight "
            "completed 5m bars (about four hours), submit the managed lifecycle close as an exact "
            "opposite-side limit order for the owned MES position."
        ),
    ),
    MNQ_US_SESSION_CONTINUATION_TIMEBOX_2H_V1: TrackBExitProfile(
        exit_strategy_id=TIMEBOXED_MANAGED_LIMIT_CLOSE_V1,
        exit_profile_id=MNQ_US_SESSION_CONTINUATION_TIMEBOX_2H_V1,
        managed_exit_policy_id="US_SESSION_CONTINUATION_TIMEBOX_2H_EXIT_V1",
        instrument_family="MNQ",
        strategy_family="us_session_continuation",
        order_type="LMT",
        required_completed_5m_bars=24,
        price_offset_ticks=2,
        tick_size="0.25",
        profile_explanation=(
            "PAPER evidence-generation US-session continuation close profile: after twenty-four "
            "completed 5m bars (about two hours), submit the managed lifecycle close as an exact "
            "opposite-side limit order for the owned MNQ position."
        ),
    ),
    MES_US_SESSION_CONTINUATION_TIMEBOX_2H_V1: TrackBExitProfile(
        exit_strategy_id=TIMEBOXED_MANAGED_LIMIT_CLOSE_V1,
        exit_profile_id=MES_US_SESSION_CONTINUATION_TIMEBOX_2H_V1,
        managed_exit_policy_id="US_SESSION_CONTINUATION_TIMEBOX_2H_EXIT_V1",
        instrument_family="MES",
        strategy_family="us_session_continuation",
        order_type="LMT",
        required_completed_5m_bars=24,
        price_offset_ticks=2,
        tick_size="0.25",
        profile_explanation=(
            "PAPER evidence-generation US-session continuation close profile: after twenty-four "
            "completed 5m bars (about two hours), submit the managed lifecycle close as an exact "
            "opposite-side limit order for the owned MES position."
        ),
    ),
    MNQ_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1: TrackBExitProfile(
        exit_strategy_id=TIMEBOXED_MANAGED_LIMIT_CLOSE_V1,
        exit_profile_id=MNQ_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
        managed_exit_policy_id="US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        instrument_family="MNQ",
        strategy_family="paper_active_evidence",
        order_type="LMT",
        required_completed_5m_bars=12,
        price_offset_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_OFFSET_TICKS,
        tick_size="0.25",
        max_slippage_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_MAX_SLIPPAGE_TICKS,
        reprice_escalation_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_REPRICE_ESCALATION_TICKS,
        stale_reference_seconds=ACTIVE_EVIDENCE_MANAGED_CLOSE_STALE_AFTER_SECONDS,
        widen_reference_seconds=ACTIVE_EVIDENCE_MANAGED_CLOSE_WIDEN_AFTER_SECONDS,
        profile_explanation=(
            "PAPER active-evidence US-session close profile: after twelve completed 5m bars "
            "(about one hour), submit the managed lifecycle close as an exact opposite-side "
            "limit order for the owned MNQ position."
        ),
    ),
    MES_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1: TrackBExitProfile(
        exit_strategy_id=TIMEBOXED_MANAGED_LIMIT_CLOSE_V1,
        exit_profile_id=MES_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
        managed_exit_policy_id="US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        instrument_family="MES",
        strategy_family="paper_active_evidence",
        order_type="LMT",
        required_completed_5m_bars=12,
        price_offset_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_OFFSET_TICKS,
        tick_size="0.25",
        max_slippage_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_MAX_SLIPPAGE_TICKS,
        reprice_escalation_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_REPRICE_ESCALATION_TICKS,
        stale_reference_seconds=ACTIVE_EVIDENCE_MANAGED_CLOSE_STALE_AFTER_SECONDS,
        widen_reference_seconds=ACTIVE_EVIDENCE_MANAGED_CLOSE_WIDEN_AFTER_SECONDS,
        profile_explanation=(
            "PAPER active-evidence US-session close profile: after twelve completed 5m bars "
            "(about one hour), submit the managed lifecycle close as an exact opposite-side "
            "limit order for the owned MES position."
        ),
    ),
    MNQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1: TrackBExitProfile(
        exit_strategy_id=TIMEBOXED_MANAGED_LIMIT_CLOSE_V1,
        exit_profile_id=MNQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
        managed_exit_policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        instrument_family="MNQ",
        strategy_family="paper_active_evidence",
        order_type="LMT",
        required_completed_5m_bars=3,
        price_offset_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_OFFSET_TICKS,
        tick_size="0.25",
        max_slippage_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_MAX_SLIPPAGE_TICKS,
        reprice_escalation_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_REPRICE_ESCALATION_TICKS,
        stale_reference_seconds=ACTIVE_EVIDENCE_MANAGED_CLOSE_STALE_AFTER_SECONDS,
        widen_reference_seconds=ACTIVE_EVIDENCE_MANAGED_CLOSE_WIDEN_AFTER_SECONDS,
        profile_explanation=(
            "PAPER active-evidence test close profile: after three completed 5m bars "
            "(about fifteen minutes), submit the managed lifecycle close as an exact "
            "opposite-side limit order for the owned MNQ position."
        ),
    ),
    MES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1: TrackBExitProfile(
        exit_strategy_id=TIMEBOXED_MANAGED_LIMIT_CLOSE_V1,
        exit_profile_id=MES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
        managed_exit_policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        instrument_family="MES",
        strategy_family="paper_active_evidence",
        order_type="LMT",
        required_completed_5m_bars=3,
        price_offset_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_OFFSET_TICKS,
        tick_size="0.25",
        max_slippage_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_MAX_SLIPPAGE_TICKS,
        reprice_escalation_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_REPRICE_ESCALATION_TICKS,
        stale_reference_seconds=ACTIVE_EVIDENCE_MANAGED_CLOSE_STALE_AFTER_SECONDS,
        widen_reference_seconds=ACTIVE_EVIDENCE_MANAGED_CLOSE_WIDEN_AFTER_SECONDS,
        profile_explanation=(
            "PAPER active-evidence test close profile: after three completed 5m bars "
            "(about fifteen minutes), submit the managed lifecycle close as an exact "
            "opposite-side limit order for the owned MES position."
        ),
    ),
    MNQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_V1: TrackBExitProfile(
        exit_strategy_id=TIMEBOXED_MANAGED_LIMIT_CLOSE_V1,
        exit_profile_id=MNQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
        managed_exit_policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        instrument_family="MNQ",
        strategy_family="paper_active_evidence",
        order_type="LMT",
        required_completed_5m_bars=12,
        price_offset_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_OFFSET_TICKS,
        tick_size="0.25",
        max_slippage_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_MAX_SLIPPAGE_TICKS,
        reprice_escalation_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_REPRICE_ESCALATION_TICKS,
        stale_reference_seconds=ACTIVE_EVIDENCE_MANAGED_CLOSE_STALE_AFTER_SECONDS,
        widen_reference_seconds=ACTIVE_EVIDENCE_MANAGED_CLOSE_WIDEN_AFTER_SECONDS,
        profile_explanation=(
            "PAPER active-evidence Globex close profile: after twelve completed 5m bars "
            "(about one hour), submit the managed lifecycle close as an exact opposite-side "
            "limit order for the owned MNQ position."
        ),
    ),
    MES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_V1: TrackBExitProfile(
        exit_strategy_id=TIMEBOXED_MANAGED_LIMIT_CLOSE_V1,
        exit_profile_id=MES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
        managed_exit_policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        instrument_family="MES",
        strategy_family="paper_active_evidence",
        order_type="LMT",
        required_completed_5m_bars=12,
        price_offset_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_OFFSET_TICKS,
        tick_size="0.25",
        max_slippage_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_MAX_SLIPPAGE_TICKS,
        reprice_escalation_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_REPRICE_ESCALATION_TICKS,
        stale_reference_seconds=ACTIVE_EVIDENCE_MANAGED_CLOSE_STALE_AFTER_SECONDS,
        widen_reference_seconds=ACTIVE_EVIDENCE_MANAGED_CLOSE_WIDEN_AFTER_SECONDS,
        profile_explanation=(
            "PAPER active-evidence Globex close profile: after twelve completed 5m bars "
            "(about one hour), submit the managed lifecycle close as an exact opposite-side "
            "limit order for the owned MES position."
        ),
    ),
    MNQ_GLOBEX_REOPEN_FIRST_CANDLE_TIMEBOX_60M_SHADOW_V1: TrackBExitProfile(
        exit_strategy_id=TIMEBOXED_MANAGED_LIMIT_CLOSE_V1,
        exit_profile_id=MNQ_GLOBEX_REOPEN_FIRST_CANDLE_TIMEBOX_60M_SHADOW_V1,
        managed_exit_policy_id="GLOBEX_REOPEN_FIRST_CANDLE_60M_TIMEBOX_SHADOW_EXIT_V1",
        instrument_family="MNQ",
        strategy_family="globex_reopen_first_candle_continuation_shadow",
        order_type="LMT",
        required_completed_5m_bars=12,
        price_offset_ticks=2,
        tick_size="0.25",
        profile_explanation=(
            "Shadow-only Globex reopen first-candle benchmark profile: observe the hypothetical "
            "opposite-side MNQ limit close after twelve completed 5m bars, about one hour, without "
            "broker or lifecycle authority."
        ),
    ),
    MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1: TrackBExitProfile(
        exit_strategy_id=TIMEBOXED_3X5M_MANAGED_LIMIT_CLOSE_V1,
        exit_profile_id=MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1,
        managed_exit_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
        instrument_family="MGC",
        strategy_family="gold_session_strategy_diagnostic",
        order_type="LMT",
        required_completed_5m_bars=3,
        price_offset_ticks=2,
        tick_size="0.1",
        profile_explanation=(
            "PAPER diagnostic MGC close profile: after three completed 5m bars, "
            "submit the managed lifecycle close as an exact opposite-side limit order for the owned MGC position."
        ),
    ),
    MGC_FORCED_SESSION_SEGMENT_TIMEBOX_3X5M_V1: TrackBExitProfile(
        exit_strategy_id=TIMEBOXED_3X5M_MANAGED_LIMIT_CLOSE_V1,
        exit_profile_id=MGC_FORCED_SESSION_SEGMENT_TIMEBOX_3X5M_V1,
        managed_exit_policy_id=FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1,
        instrument_family="MGC",
        strategy_family="gold_forced_session_baseline_v2",
        order_type="LMT",
        required_completed_5m_bars=3,
        price_offset_ticks=2,
        tick_size="0.1",
        profile_explanation=(
            "PAPER forced-session MGC close profile: after three completed 5m bars, "
            "submit the managed lifecycle close as an exact opposite-side limit order for the owned MGC position."
        ),
    ),
}


def resolve_track_b_exit_profile(exit_profile_id: str) -> TrackBExitProfile:
    try:
        return EXIT_PROFILE_ROSTER[exit_profile_id]
    except KeyError as exc:
        raise ValueError(f"Unknown Track B exit profile: {exit_profile_id}") from exc


def resolve_track_b_exit_profile_for_position(*, instrument_family: str, managed_exit_policy_id: str) -> TrackBExitProfile:
    normalized_instrument = str(instrument_family or "").strip().upper()
    normalized_policy = str(managed_exit_policy_id or "").strip()
    matches = [
        profile
        for profile in EXIT_PROFILE_ROSTER.values()
        if profile.instrument_family == normalized_instrument and profile.managed_exit_policy_id == normalized_policy
    ]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise ValueError(
            f"No Track B exit profile for instrument={normalized_instrument or 'UNKNOWN'} "
            f"policy={normalized_policy or 'UNKNOWN'}"
        )
    raise ValueError(
        f"Ambiguous Track B exit profile for instrument={normalized_instrument} policy={normalized_policy}: "
        + ", ".join(sorted(profile.exit_profile_id for profile in matches))
    )


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
    priced = managed_close_limit_from_reference(
        reference_price=latest_price,
        close_action=close_action_for_position_side(side),
        tick_size=profile.tick_size,
        base_offset_ticks=profile.price_offset_ticks,
        max_slippage_ticks=profile.max_slippage_ticks,
        reprice_escalation_ticks=profile.reprice_escalation_ticks,
        stale_reference_seconds=profile.stale_reference_seconds,
        widen_reference_seconds=profile.widen_reference_seconds,
    )
    return priced["limit_price"] if priced["classification"] == "MANAGED_CLOSE_PRICED" else None


def managed_close_limit_from_reference(
    *,
    reference_price: Any,
    close_action: str,
    tick_size: str,
    base_offset_ticks: int,
    max_slippage_ticks: int | None = None,
    reprice_attempts: int = 0,
    reprice_escalation_ticks: int = 0,
    reference_age_seconds: float | None = None,
    stale_reference_seconds: int | None = None,
    widen_reference_seconds: int | None = None,
) -> dict[str, Any]:
    reference = _decimal_or_none(reference_price)
    tick = _decimal_or_none(tick_size)
    if reference is None or tick is None or tick <= Decimal("0"):
        return _managed_close_pricing_block(
            blocker="MANAGED_CLOSE_REFERENCE_MISSING",
            reference_price=reference_price,
            reference_age_seconds=reference_age_seconds,
            base_offset_ticks=base_offset_ticks,
            max_slippage_ticks=max_slippage_ticks,
            reprice_attempts=reprice_attempts,
        )
    if (
        stale_reference_seconds is not None
        and reference_age_seconds is not None
        and reference_age_seconds > float(stale_reference_seconds)
    ):
        return _managed_close_pricing_block(
            blocker="MANAGED_CLOSE_REFERENCE_STALE",
            reference_price=str(reference),
            reference_age_seconds=reference_age_seconds,
            base_offset_ticks=base_offset_ticks,
            max_slippage_ticks=max_slippage_ticks,
            reprice_attempts=reprice_attempts,
        )
    action = str(close_action or "").strip().upper()
    if action not in {"SELL", "BUY"}:
        return _managed_close_pricing_block(
            blocker="MANAGED_CLOSE_ACTION_UNSUPPORTED",
            reference_price=str(reference),
            reference_age_seconds=reference_age_seconds,
            base_offset_ticks=base_offset_ticks,
            max_slippage_ticks=max_slippage_ticks,
            reprice_attempts=reprice_attempts,
        )

    offset_ticks = Decimal(max(int(base_offset_ticks), 0))
    if (
        widen_reference_seconds is not None
        and reference_age_seconds is not None
        and reference_age_seconds > float(widen_reference_seconds)
    ):
        offset_ticks += Decimal(max(int(reprice_escalation_ticks), 0))
    offset_ticks += Decimal(max(int(reprice_attempts), 0) * max(int(reprice_escalation_ticks), 0))
    max_ticks = Decimal(max(int(max_slippage_ticks), 0)) if max_slippage_ticks is not None else offset_ticks
    if max_slippage_ticks is not None:
        offset_ticks = min(offset_ticks, max_ticks)

    offset = tick * offset_ticks
    raw = reference - offset if action == "SELL" else reference + offset
    rounded = (raw / tick).to_integral_value() * tick
    return {
        "classification": "MANAGED_CLOSE_PRICED",
        "limit_price": format(rounded.normalize(), "f"),
        "close_action": action,
        "reference_price": format(reference.normalize(), "f"),
        "reference_age_seconds": reference_age_seconds,
        "marketable_limit_offset_ticks": float(offset_ticks),
        "max_slippage_ticks": float(max_ticks),
        "reprice_attempts": max(int(reprice_attempts), 0),
        "reprice_escalation_ticks": max(int(reprice_escalation_ticks), 0),
        "stale_reference_blocker": None,
    }


def _managed_close_pricing_block(
    *,
    blocker: str,
    reference_price: Any,
    reference_age_seconds: float | None,
    base_offset_ticks: int,
    max_slippage_ticks: int | None,
    reprice_attempts: int,
) -> dict[str, Any]:
    return {
        "classification": "MANAGED_CLOSE_PRICING_BLOCKED",
        "limit_price": None,
        "reference_price": reference_price,
        "reference_age_seconds": reference_age_seconds,
        "marketable_limit_offset_ticks": None,
        "max_slippage_ticks": float(max_slippage_ticks) if max_slippage_ticks is not None else None,
        "base_offset_ticks": base_offset_ticks,
        "reprice_attempts": max(int(reprice_attempts), 0),
        "stale_reference_blocker": blocker,
        "block_reason": blocker,
    }


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
