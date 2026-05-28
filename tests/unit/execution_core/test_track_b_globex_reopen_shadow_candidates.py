from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_globex_reopen_shadow_candidates import (
    GLOBEX_REOPEN_MNQ_1M_STRONG_GREEN_SECOND_CANDLE_CONFIRM_SHADOW_V1,
    GLOBEX_REOPEN_SHADOW_CANDIDATES_READY,
    build_globex_reopen_shadow_candidate_report,
)


NOW = datetime(2026, 5, 28, 18, 0, tzinfo=UTC)


def test_globex_reopen_candidate_registers_shadow_only_metadata(tmp_path: Path) -> None:
    _write_replay_evidence(tmp_path)

    report = build_globex_reopen_shadow_candidate_report(repo_root=tmp_path, now=NOW)

    assert report["classification"] == GLOBEX_REOPEN_SHADOW_CANDIDATES_READY
    assert report["submit_allowed"] is False
    assert report["broker_authority"] is False
    assert report["lifecycle_authority"] is False
    row = report["candidates"][0]
    assert row["candidate_id"] == GLOBEX_REOPEN_MNQ_1M_STRONG_GREEN_SECOND_CANDLE_CONFIRM_SHADOW_V1
    assert row["classification"] == "SHADOW_CANDIDATE"
    assert row["symbol"] == "MNQ"
    assert row["benchmark_hold_minutes"] == 60
    assert row["no_paper_authority_yet"] is True
    assert row["entry_creation_allowed"] is False
    assert row["submit_allowed"] is False
    assert row["broker_authority"] is False
    assert row["lifecycle_authority"] is False


def test_globex_reopen_candidate_has_position_intent_and_hold_exit_policy(tmp_path: Path) -> None:
    _write_replay_evidence(tmp_path)

    report = build_globex_reopen_shadow_candidate_report(repo_root=tmp_path, now=NOW)
    row = report["candidates"][0]

    assert row["position_intent_validation"]["valid"] is True
    assert row["position_intent"]["trade_thesis"]["thesis_type"] == "TREND_PARTICIPATION"
    assert row["position_intent"]["hold_policy"]["max_hold_policy"] == "12_COMPLETED_5M_BARS"
    assert row["position_intent"]["order_policy"]["submit_authority"] is False
    assert row["hold_exit_policy_validation"]["valid"] is True
    assert row["hold_exit_policy"]["hold_policy_id"] == "GLOBEX_REOPEN_FIRST_CANDLE_60M_TIMEBOX_HOLD_SHADOW_V1"
    assert row["hold_exit_policy"]["submit_allowed"] is False
    assert row["hold_exit_policy"]["lifecycle_authority"] is False


def test_globex_reopen_candidate_links_replay_evidence_without_promoting_authority(tmp_path: Path) -> None:
    _write_replay_evidence(tmp_path)

    report = build_globex_reopen_shadow_candidate_report(repo_root=tmp_path, now=NOW)
    evidence = report["candidates"][0]["replay_evidence"]

    assert evidence["evidence_linkage_status"] == "LINKED"
    assert evidence["source_artifact_classification"] == "shadow candidate"
    assert evidence["source_authority_status"] == "research_only_not_promoted"
    assert evidence["paper_authority_promoted"] is False
    assert evidence["best_mnq_trigger"]["hold_minutes"] == 60


def test_operator_surface_exposes_globex_shadow_without_authority() -> None:
    from mgc_v05l.app.operator_surface import _build_track_b_trading_authority_surface

    surface = _build_track_b_trading_authority_surface(paper={"config_in_force": {"lanes": []}})

    assert surface["broker_authoritative_count"] == 0
    assert surface["globex_reopen_shadow_candidate_count"] == 1
    assert surface["globex_reopen_shadow_operator_visibility"]["authority_note"] == "shadow_only_no_broker_or_lifecycle_authority"
    row = surface["globex_reopen_shadow_candidates"][0]
    assert row["candidate_id"] == GLOBEX_REOPEN_MNQ_1M_STRONG_GREEN_SECOND_CANDLE_CONFIRM_SHADOW_V1
    assert row["submit_allowed"] is False
    assert row["broker_authority"] is False
    assert row["lifecycle_authority"] is False


def _write_replay_evidence(repo_root: Path) -> None:
    path = (
        repo_root
        / "outputs"
        / "reports"
        / "globex_reopen_first_candle_continuation"
        / "globex_reopen_first_candle_continuation_research.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "globex_reopen_first_candle_continuation_research_v1",
        "mode": "globex_reopen_first_candle_continuation_research",
        "lane_classification": {
            "classification": "shadow candidate",
            "authority_status": "research_only_not_promoted",
            "paper_authority_promoted": False,
            "best_mnq_trigger": {
                "timeframe": "1m",
                "hold_minutes": 60,
                "sample_count": 219,
                "net_continuation_rate": 0.5251,
                "average_net_directional_points": 2.6484,
            },
        },
        "simple_filter_summaries": {
            "second_candle_confirmation": [
                {
                    "timeframe": "1m",
                    "hold_minutes": 60,
                    "second_candle_hold_confirmed": True,
                    "sample_count": 127,
                    "net_continuation_rate": 0.5827,
                    "average_net_directional_points": 8.2323,
                }
            ]
        },
        "artifact_paths": {"json": str(path)},
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
