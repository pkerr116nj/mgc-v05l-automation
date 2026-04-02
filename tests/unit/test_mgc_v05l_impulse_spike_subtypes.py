from types import SimpleNamespace

from mgc_v05l.app.mgc_impulse_spike_subtypes import _classify_spike_subtype


def test_classify_spike_subtype_marks_good_ignition() -> None:
    subtype = _classify_spike_subtype(
        trade=SimpleNamespace(
            favorable_excursion_first_1_bar=20.0,
            favorable_excursion_first_2_bars=60.0,
            adverse_excursion_first_2_bars=20.0,
        ),
        impulse={"contributing_breadth": 0.5, "largest_bar_share": 0.32},
        body_to_range_quality=0.58,
    )
    assert subtype == "GOOD_IGNITION_SPIKE"


def test_classify_spike_subtype_marks_bad_trap() -> None:
    subtype = _classify_spike_subtype(
        trade=SimpleNamespace(
            favorable_excursion_first_1_bar=6.0,
            favorable_excursion_first_2_bars=18.0,
            adverse_excursion_first_2_bars=50.0,
        ),
        impulse={"contributing_breadth": 0.375, "largest_bar_share": 0.36},
        body_to_range_quality=0.58,
    )
    assert subtype == "BAD_SPIKE_TRAP"
