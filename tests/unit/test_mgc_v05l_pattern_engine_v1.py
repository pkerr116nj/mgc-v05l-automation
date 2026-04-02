from datetime import datetime
from decimal import Decimal

from mgc_v05l.research.pattern_engine.dataset import PatternEngineContext
from mgc_v05l.research.pattern_engine.families import default_pattern_family_specs
from mgc_v05l.research.pattern_engine.primitives import build_pattern_primitive_points


def _context(
    timestamp: str,
    *,
    open_: str,
    high: str,
    low: str,
    close: str,
    slope: str,
    curvature: str,
    fast: str,
    slow: str,
    range_ratio: str = "1.00",
) -> PatternEngineContext:
    return PatternEngineContext(
        bar_id=timestamp,
        timestamp=datetime.fromisoformat(timestamp),
        symbol="MGC",
        timeframe="5m",
        open=Decimal(open_),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=100,
        session_phase="US_MIDDAY",
        atr=Decimal("2"),
        vol_ratio=Decimal("1.0"),
        vwap=Decimal("100"),
        turn_ema_fast=Decimal(fast),
        turn_ema_slow=Decimal(slow),
        normalized_slope=Decimal(slope),
        normalized_curvature=Decimal(curvature),
        range_expansion_ratio=Decimal(range_ratio),
        body_to_range=Decimal("0.6"),
        close_location=Decimal("0.8"),
        vwap_distance_atr=Decimal("0.5"),
        rolling_high_10=Decimal("110"),
        rolling_low_10=Decimal("95"),
        distance_from_high_10_atr=Decimal("2"),
        distance_from_low_10_atr=Decimal("2"),
    )


def test_pause_pullback_resume_long_family_matches_phase_sequence() -> None:
    contexts = [
        _context("2026-03-17T11:00:00-04:00", open_="100", high="103", low="99", close="102", slope="0.10", curvature="0.05", fast="101", slow="100"),
        _context("2026-03-17T11:05:00-04:00", open_="102", high="102.5", low="100.5", close="101", slope="0.02", curvature="-0.05", fast="101.2", slow="101.5"),
        _context("2026-03-17T11:10:00-04:00", open_="101", high="104", low="100.8", close="103.5", slope="0.05", curvature="0.25", fast="103.2", slow="103"),
    ]
    primitives = build_pattern_primitive_points(contexts)
    family = next(spec for spec in default_pattern_family_specs() if spec.name == "pause_pullback_resume_long")
    match = family.matcher(contexts, primitives, 2)
    assert match is not None
    assert [step.phase_name for step in match.steps] == ["setup", "pullback", "resumption", "confirmation"]


def test_failed_move_reversal_family_matches_short_reversal() -> None:
    contexts = [
        _context("2026-03-17T11:00:00-04:00", open_="100", high="101", low="99", close="100.5", slope="0.05", curvature="0.05", fast="100.2", slow="100.1"),
        _context("2026-03-17T11:05:00-04:00", open_="100.5", high="103", low="100", close="102.8", slope="0.25", curvature="0.20", fast="101.5", slow="101"),
        _context("2026-03-17T11:10:00-04:00", open_="102.8", high="103.1", low="100.5", close="101.0", slope="-0.10", curvature="-0.30", fast="101.8", slow="101.2"),
    ]
    primitives = build_pattern_primitive_points(contexts)
    family = next(spec for spec in default_pattern_family_specs() if spec.name == "failed_move_reversal")
    match = family.matcher(contexts, primitives, 2)
    assert match is not None
    assert match.direction == "SHORT"
