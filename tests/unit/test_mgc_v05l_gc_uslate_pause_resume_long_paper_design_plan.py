from __future__ import annotations

import json

from mgc_v05l.app.gc_uslate_pause_resume_long_paper_design_plan import (
    DISPLAY_NAME,
    LANE_ID,
    OUTPUT_JSON,
    build_and_write_gc_uslate_paper_design_plan,
)


def test_gc_uslate_paper_design_plan_writes_expected_lane_shape() -> None:
    result = build_and_write_gc_uslate_paper_design_plan()
    payload = json.loads(OUTPUT_JSON.read_text(encoding="utf-8"))

    assert result["decision_bucket"] == "HOLD_AS_NEXT_ADDITION_CANDIDATE"
    assert payload["admission_design_shape"]["future_lane_identity"]["lane_id"] == LANE_ID
    assert payload["admission_design_shape"]["future_lane_identity"]["display_name"] == DISPLAY_NAME
    assert payload["admission_design_shape"]["future_lane_identity"]["session_restriction"] == "US_LATE"
    assert payload["admission_design_shape"]["architecture_fit"]["future_change_shape"] == "config_only_likely_sufficient"


def test_gc_uslate_paper_design_plan_records_current_blocker() -> None:
    build_and_write_gc_uslate_paper_design_plan()
    payload = json.loads(OUTPUT_JSON.read_text(encoding="utf-8"))

    assert payload["decision_framework"]["decision_bucket"] == "HOLD_AS_NEXT_ADDITION_CANDIDATE"
    assert "Concentration fragility" in payload["direct_answers"]["single_biggest_reason_to_wait"]
    assert payload["operational_prerequisites"]["concentration_sanity_requirements_before_future_admission_attempt"]["current_blocking_state"]["survives_without_top_1"] is False
    assert payload["operational_prerequisites"]["concentration_sanity_requirements_before_future_admission_attempt"]["current_blocking_state"]["survives_without_top_3"] is False


def test_gc_uslate_paper_design_plan_uses_current_gc_architecture_proof() -> None:
    build_and_write_gc_uslate_paper_design_plan()
    payload = json.loads(OUTPUT_JSON.read_text(encoding="utf-8"))

    current_gc_branch_names = payload["current_reference_state"]["current_approved_gc_branch_names"]
    assert "GC / asiaEarlyNormalBreakoutRetestHoldTurn" in current_gc_branch_names
    assert payload["admission_design_shape"]["architecture_fit"]["fits_current_metals_multi_lane_paper_architecture"] is True
    assert "config/design work only" in payload["direct_answers"]["can_it_likely_be_added_later_with_config_design_work_only_or_would_runtime_changes_be_needed"]
