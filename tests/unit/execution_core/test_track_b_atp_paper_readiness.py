from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_atp_paper_readiness import (
    AtpPaperReadinessConfig,
    build_atp_lifecycle_mapping_shadow,
    build_atp_paper_readiness_report,
    create_atp_paper_readiness_report,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_atp_readiness_ranks_mgc_candidate_as_lifecycle_mapping_gap(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "outputs/track_b_execution_core/research_shadow/latest_atp_trend_participation_shadow.json",
        {
            "candidates": [
                {
                    "candidate_id": "atp_mgc",
                    "strategy_id": "atp_companion_v1__paper_mgc_asia_us",
                    "instrument": "MGC",
                    "direction": "LONG",
                    "session_regime": "ASIA/US",
                    "strategy_family": "active_trend_participation_engine",
                    "shadow_classification": "ATP_SHADOW_CANDIDATE_LIVE_STRATEGIES_SILENT",
                    "score": 0.84,
                    "confidence": "HIGH",
                    "failed_gates": [],
                    "paper_only": True,
                    "non_approved": True,
                    "source_authority_path": "/tmp/mgc.json",
                    "source_candle_timestamp": "2026-05-26T10:45:00+00:00",
                }
            ]
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/research_shadow/latest_missed_opportunity_forward_outcomes.json",
        {
            "outcomes": [
                {
                    "source": "ATP_TREND_PARTICIPATION_SHADOW",
                    "candidate_id": "atp_mgc",
                    "outcome_classification": "UNCLEAR",
                    "outcome_reason": "UNCLEAR_FUTURE_BARS_NOT_YET_AVAILABLE",
                }
            ]
        },
    )

    report = build_atp_paper_readiness_report(
        config=AtpPaperReadinessConfig(repo_root=tmp_path),
        now=datetime(2026, 5, 26, tzinfo=UTC),
    )

    row = report["ranked_candidates"][0]
    assert row["readiness_classification"] == "PAPER_CANDIDATE_NEEDS_LIFECYCLE_MAPPING"
    assert row["structural_gates"]["exit_profile_exists"] is True
    assert row["structural_gates"]["lifecycle_mapping_exists"] is False
    assert "track_b_lifecycle_mapping_required" in row["missing_gates"]
    assert row["submit_allowed"] is False
    assert row["not_order_authority"] is True
    mapping = report["atp_lifecycle_mapping_shadow"]["mapped_candidates"][0]
    assert mapping["lifecycle_mapping_status"] == "ATP_LIFECYCLE_MAPPING_MAPPED_SHADOW_ONLY"
    assert mapping["mapped_exit_profile_id"] == "MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1"
    assert mapping["lifecycle_authority"] is False
    assert mapping["submit_allowed"] is False
    assert report["recommended_first_atp_candidate_for_guarded_paper"]["candidate_id"] == "atp_mgc"


def test_atp_readiness_requires_exit_profile_for_unmapped_instrument(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "outputs/track_b_execution_core/research_shadow/latest_atp_trend_participation_shadow.json",
        {
            "candidates": [
                {
                    "candidate_id": "atp_pl",
                    "strategy_id": "atp_companion_v1__paper_pl_asia_us",
                    "instrument": "PL",
                    "direction": "LONG",
                    "session_regime": "ASIA/US",
                    "shadow_classification": "ATP_SHADOW_CANDIDATE_LIVE_STRATEGIES_SILENT",
                    "score": 0.8,
                    "confidence": "HIGH",
                    "failed_gates": [],
                    "paper_only": True,
                    "non_approved": True,
                }
            ]
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/research_shadow/latest_missed_opportunity_forward_outcomes.json",
        {"outcomes": []},
    )

    report = build_atp_paper_readiness_report(
        config=AtpPaperReadinessConfig(repo_root=tmp_path),
        now=datetime(2026, 5, 26, tzinfo=UTC),
    )

    row = report["ranked_candidates"][0]
    assert row["readiness_classification"] == "PAPER_CANDIDATE_NEEDS_EXIT_PROFILE"
    assert "managed_exit_profile_required" in row["missing_gates"]
    assert row["broker_mutation_allowed"] is False
    mapping = report["atp_lifecycle_mapping_shadow"]["mapped_candidates"][0]
    assert mapping["lifecycle_mapping_status"] == "ATP_LIFECYCLE_MAPPING_NEEDS_EXIT_PROFILE"
    assert mapping["mapped_exit_profile_id"] is None


def test_atp_lifecycle_mapping_keeps_rejected_candidates_non_actionable() -> None:
    shadow = build_atp_lifecycle_mapping_shadow(
        atp_shadow={
            "candidates": [
                {
                    "candidate_id": "atp_reject",
                    "strategy_id": "atp_companion_v1__paper_mgc_asia_us",
                    "instrument": "MGC",
                    "direction": "LONG",
                    "shadow_classification": "ATP_SHADOW_NO_CANDIDATE",
                    "paper_only": True,
                }
            ]
        },
        now=datetime(2026, 5, 26, tzinfo=UTC),
    )

    row = shadow["mapped_candidates"][0]
    assert row["lifecycle_mapping_status"] == "ATP_LIFECYCLE_MAPPING_NON_ACTIONABLE"
    assert row["not_lifecycle_authority"] is True
    assert row["broker_mutation_allowed"] is False


def test_atp_readiness_writes_json_and_markdown(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "outputs/track_b_execution_core/research_shadow/latest_atp_trend_participation_shadow.json",
        {"candidates": []},
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/research_shadow/latest_missed_opportunity_forward_outcomes.json",
        {"outcomes": []},
    )

    json_path, md_path, report = create_atp_paper_readiness_report(
        config=AtpPaperReadinessConfig(repo_root=tmp_path),
        now=datetime(2026, 5, 26, tzinfo=UTC),
    )

    assert json_path.exists()
    assert md_path.exists()
    assert (
        tmp_path
        / "outputs/track_b_execution_core/research_shadow/latest_atp_lifecycle_mapping_shadow.json"
    ).exists()
    assert (
        tmp_path
        / "outputs/track_b_execution_core/research_shadow/atp_lifecycle_mapping_shadow_events.jsonl"
    ).exists()
    assert report["classification"] == "ATP_PAPER_READINESS_READY"
    assert report["submit_allowed"] is False
