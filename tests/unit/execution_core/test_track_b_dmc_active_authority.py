from __future__ import annotations

from mgc_v05l.execution_core.track_b_dmc_active_authority import (
    active_authority_classification,
    active_authority_rows,
    is_active_authority_payload,
    is_active_authority_row,
)


def test_empty_payload_is_active_authority() -> None:
    assert is_active_authority_payload({}) is True


def test_row_flags_mark_inactive_authority() -> None:
    assert is_active_authority_row({"historical_only": True}) is False
    assert is_active_authority_row({"diagnostic_only": True}) is False
    assert is_active_authority_row({"current_scope_active": False}) is False
    assert is_active_authority_row({"invalidated_by_current_truth": True}) is False


def test_dmc_metadata_flags_mark_inactive_authority() -> None:
    assert is_active_authority_payload({"dmc_metadata": {"diagnostic_only": True}}) is False
    assert is_active_authority_payload({"metadata": {"current_scope_active": False}}) is False


def test_current_truth_invalidation_marks_payload_inactive_authority() -> None:
    payload = {
        "classification": "BROKER_POSITION_GUARDIAN_HARD_HOLD",
        "current_truth_invalidation": {
            "current_scope_active": False,
            "diagnostic_only": True,
            "invalidated_by_current_truth": True,
        },
    }

    assert is_active_authority_payload(payload) is False


def test_active_authority_rows_filters_only_inactive_rows() -> None:
    rows = [
        {"id": "active"},
        {"id": "historical", "historical_only": True},
        {"id": "diagnostic", "diagnostic_only": True},
        {"id": "inactive", "current_scope_active": False},
        {"id": "invalidated", "invalidated_by_current_truth": True},
    ]

    assert active_authority_rows(rows) == [{"id": "active"}]


def test_active_authority_classification_hides_inactive_payload_classification() -> None:
    assert active_authority_classification({"classification": "READY"}) == "READY"
    assert (
        active_authority_classification(
            {"classification": "HARD_HOLD", "current_truth_invalidation": {"diagnostic_only": True}},
            inactive_value="",
        )
        == ""
    )
