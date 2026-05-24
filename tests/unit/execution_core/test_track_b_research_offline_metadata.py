from __future__ import annotations

from pathlib import Path

from mgc_v05l.execution_core.track_b_research_offline_metadata import (
    RESEARCH_OFFLINE_SOURCE_CATEGORY,
    apply_research_offline_metadata,
    build_research_offline_metadata,
)


def test_research_offline_metadata_marks_artifact_non_authoritative() -> None:
    metadata = build_research_offline_metadata(
        producer="unit_test_research_diagnostic",
        source_paths=["outputs/operator_dashboard/paper_latest_fills_snapshot.json"],
        notes=["historical sample"],
    )

    assert metadata["research_only"] is True
    assert metadata["offline_diagnostic"] is True
    assert metadata["not_runtime_authority"] is True
    assert metadata["not_broker_truth"] is True
    assert metadata["not_market_data_runtime_truth"] is True
    assert metadata["not_routing_authority"] is True
    assert metadata["dashboard_projection_consumed_as_authority"] is False
    assert metadata["source_category"] == RESEARCH_OFFLINE_SOURCE_CATEGORY


def test_apply_research_offline_metadata_copies_payload() -> None:
    original = {"classification": "RESEARCH_SIGNAL_PRESENT"}

    labeled = apply_research_offline_metadata(
        original,
        producer="unit_test_research_diagnostic",
        source_paths=[Path("outputs/track_b_research/latest.json")],
    )

    assert original == {"classification": "RESEARCH_SIGNAL_PRESENT"}
    assert labeled["classification"] == "RESEARCH_SIGNAL_PRESENT"
    assert labeled["research_offline_metadata"]["not_runtime_authority"] is True
    assert labeled["research_offline_metadata"]["source_paths"] == ["outputs/track_b_research/latest.json"]


def test_active_execution_core_hot_paths_do_not_consume_research_artifacts() -> None:
    repo = Path(__file__).resolve().parents[3]
    hot_path_modules = [
        "src/mgc_v05l/execution_core/track_b_control_plane_snapshot.py",
        "src/mgc_v05l/execution_core/track_b_runtime_supervisor_authority.py",
        "src/mgc_v05l/execution_core/track_b_pre_action_snapshot_validator.py",
        "src/mgc_v05l/execution_core/track_b_shared_truth_refresh_cli.py",
        "src/mgc_v05l/execution_core/track_b_paper_proof_readiness.py",
        "src/mgc_v05l/execution_core/track_b_runtime_resume_semantics.py",
        "src/mgc_v05l/execution_core/track_b_self_recover_rules.py",
        "src/mgc_v05l/execution_core/track_b_open_order_truth.py",
        "src/mgc_v05l/execution_core/track_b_position_truth_monitor.py",
        "src/mgc_v05l/execution_core/track_b_managed_order_registry.py",
        "src/mgc_v05l/execution_core/track_b_managed_position_registry.py",
    ]
    forbidden = ("outputs/track_b_research", "outputs/research_", "outputs/reports/approved_branch_research")

    offenders = []
    for module in hot_path_modules:
        text = (repo / module).read_text(encoding="utf-8")
        if any(token in text for token in forbidden):
            offenders.append(module)

    assert offenders == []
