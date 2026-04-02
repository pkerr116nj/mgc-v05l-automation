from mgc_v05l.app.mgc_impulse_paper_admission_plan import (
    DISPLAY_NAME,
    FAMILY_NAME,
    LANE_ID,
    LONG_SOURCE_ID,
    SESSION_RESTRICTION,
    SHORT_SOURCE_ID,
    _paper_admission_wiring_plan,
)


def test_paper_admission_plan_uses_distinct_side_specific_source_ids() -> None:
    wiring = _paper_admission_wiring_plan()
    lane_entry = wiring["exact_config_entries_to_add"]["paper_lane_entry"]

    assert lane_entry["lane_id"] == LANE_ID
    assert lane_entry["display_name"] == DISPLAY_NAME
    assert lane_entry["long_sources"] == [LONG_SOURCE_ID]
    assert lane_entry["short_sources"] == [SHORT_SOURCE_ID]
    assert LONG_SOURCE_ID != SHORT_SOURCE_ID


def test_paper_admission_plan_requires_explicit_all_sessions_marker() -> None:
    wiring = _paper_admission_wiring_plan()
    lane_entry = wiring["exact_config_entries_to_add"]["paper_lane_entry"]

    assert lane_entry["session_restriction"] == SESSION_RESTRICTION
    assert FAMILY_NAME == "impulse_burst_continuation"
