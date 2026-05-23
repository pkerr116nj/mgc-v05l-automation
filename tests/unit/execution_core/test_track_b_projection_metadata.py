from __future__ import annotations

import inspect
from pathlib import Path

from mgc_v05l.execution_core.track_b_agent_health import build_dashboard_agent_health_projection
from mgc_v05l.execution_core.track_b_agent_registry import build_dashboard_agent_registry_projection
from mgc_v05l.execution_core.track_b_control_plane_snapshot import build_dashboard_control_plane_snapshot_projection
from mgc_v05l.execution_core.track_b_crash_loop_protection import build_dashboard_crash_loop_projection
from mgc_v05l.execution_core.track_b_managed_order_registry import build_dashboard_managed_order_projection
from mgc_v05l.execution_core.track_b_managed_position_registry import build_dashboard_managed_position_projection
from mgc_v05l.execution_core.track_b_open_order_truth import build_dashboard_open_order_truth_projection
from mgc_v05l.execution_core.track_b_paper_recovery_policy import build_dashboard_paper_recovery_policy_projection
from mgc_v05l.execution_core.track_b_position_truth_monitor import build_dashboard_position_truth_projection
from mgc_v05l.execution_core.track_b_pre_action_snapshot_validator import validate_track_b_pre_action_snapshot
from mgc_v05l.execution_core.track_b_projection_metadata import (
    EXECUTION_CORE_AUTHORITY,
    MISSING_SOURCE_AUTHORITY_PATH,
    build_projection_metadata,
)
from mgc_v05l.execution_core.track_b_runtime_environment_truth import build_dashboard_runtime_environment_projection
from mgc_v05l.execution_core.track_b_runtime_resume_semantics import build_dashboard_runtime_resume_projection
from mgc_v05l.execution_core.track_b_runtime_supervisor_authority import (
    build_dashboard_runtime_supervisor_projection,
)
from mgc_v05l.execution_core.track_b_self_recover_rules import build_dashboard_self_recover_projection


def test_projection_metadata_marks_missing_source_as_degraded_diagnostic_only() -> None:
    metadata = build_projection_metadata()

    assert metadata["projection_only"] is True
    assert metadata["not_routing_authority"] is True
    assert metadata["dashboard_projection_authority"] is False
    assert metadata["source_authority"] == EXECUTION_CORE_AUTHORITY
    assert metadata["projection_metadata_complete"] is False
    assert metadata["projection_degraded"] is True
    assert metadata["diagnostic_only"] is True
    assert metadata["degraded_reason"] == MISSING_SOURCE_AUTHORITY_PATH


def test_all_track_b_control_plane_dashboard_projections_include_ownership_metadata() -> None:
    payload = {
        "classification": "TEST_CLASSIFICATION",
        "control_plane_snapshot_id": "snapshot-test",
    }
    builders = [
        build_dashboard_agent_registry_projection,
        build_dashboard_agent_health_projection,
        build_dashboard_open_order_truth_projection,
        build_dashboard_managed_order_projection,
        build_dashboard_position_truth_projection,
        build_dashboard_runtime_environment_projection,
        build_dashboard_managed_position_projection,
        build_dashboard_self_recover_projection,
        build_dashboard_crash_loop_projection,
        build_dashboard_runtime_resume_projection,
        build_dashboard_paper_recovery_policy_projection,
        build_dashboard_runtime_supervisor_projection,
        build_dashboard_control_plane_snapshot_projection,
    ]

    for builder in builders:
        authority_path = Path("outputs/track_b_execution_core") / builder.__name__ / "latest.json"
        projection = builder(authority_payload=payload, authority_path=authority_path)
        assert projection["projection_only"] is True
        assert projection["not_routing_authority"] is True
        assert projection["dashboard_projection_authority"] is False
        assert projection["source_authority"] == EXECUTION_CORE_AUTHORITY
        assert projection["source_authority_path"] == str(authority_path)
        assert projection["source_authority_paths"] == [str(authority_path)]
        assert projection["authority_owner"] == "execution_core"
        assert projection["operator_dashboard_display_only"] is True
        assert projection["projection_metadata_complete"] is True
        assert projection["projection_degraded"] is False
        assert projection["degraded_reason"] is None

    control_plane_projection = build_dashboard_control_plane_snapshot_projection(
        authority_payload=payload,
        authority_path=Path("outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json"),
    )
    assert control_plane_projection["generated_from_control_plane_snapshot_id"] == "snapshot-test"
    assert control_plane_projection["control_plane_snapshot_required"] is True


def test_pre_action_snapshot_validator_does_not_consume_dashboard_projection_as_authority() -> None:
    source = inspect.getsource(validate_track_b_pre_action_snapshot)

    assert "outputs/operator_dashboard/runtime/latest_track_b_control_plane_snapshot.json" not in source
    assert "latest_track_b_runtime_supervisor_authority.json" not in source
