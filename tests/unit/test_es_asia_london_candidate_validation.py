from __future__ import annotations

from mgc_v05l.app.es_asia_london_candidate_validation import DEFAULT_FLOORS, DEFAULT_LONG_VARIANTS


def test_validation_defaults_focus_on_es_long_candidate_ladder() -> None:
    assert DEFAULT_FLOORS == (1.0, 1.25, 1.5, 1.75)
    assert DEFAULT_LONG_VARIANTS == (
        "segment_forced_long_v6_contextual_fallback",
        "segment_forced_long_v5_dip_reclaim_or_bar8",
    )
