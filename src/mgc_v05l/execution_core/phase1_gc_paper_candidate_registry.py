"""GC-only Phase-1 PAPER candidate registry.

This is an eligibility registry, not a submit bypass. A listed strategy may only
submit through the existing guarded PAPER route after live feed, governance,
monitor, account, broker flat/open-order, and review gates pass.
"""

from __future__ import annotations

from typing import Final

CHOSEN_GC_STRATEGY_ID: Final[str] = "gc_1x_asia_london_participation__asia_london_long_v5"
PHASE1_GC_GUARDED_PAPER_ELIGIBLE_STRATEGY_IDS: Final[frozenset[str]] = frozenset(
    {CHOSEN_GC_STRATEGY_ID}
)


def is_phase1_gc_guarded_paper_eligible_strategy(*, strategy_id: str, instrument: str) -> bool:
    return (
        str(instrument or "").strip().upper() == "GC"
        and str(strategy_id or "").strip() in PHASE1_GC_GUARDED_PAPER_ELIGIBLE_STRATEGY_IDS
    )
