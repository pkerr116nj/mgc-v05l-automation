from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution_core.track_b_london_active_evidence_shadow_coverage import (
    build_london_active_evidence_shadow_report,
)


def _write_runtime_bars(root: Path, symbol: str, bars: list[dict[str, object]]) -> None:
    path = root / "outputs/track_b_execution_core/phase1_runtime_market_data" / symbol / "1m" / "latest_runtime_candles.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "source": "DATABENTO_REALTIME_PHASE1",
                "realtime_feed_confirmed": True,
                "bars": bars,
            }
        ),
        encoding="utf-8",
    )


def _clean_status() -> dict[str, object]:
    return {
        "readiness": {
            "canonical_state": "READY_SUBMIT_CAPABLE",
            "ready_submit_capable": True,
            "submit_allowed": True,
            "blockers": [],
            "market_schedule_state": "MARKET_OPEN_EXPECT_FRESH_BARS",
        },
        "broker_lifecycle": {
            "reconciliation_classification": "TRACK_B_PAPER_BROKER_RECONCILED",
        },
        "registry_truth_diagnostics": {
            "classification": "TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE",
            "broker_open_order_count": 0,
            "lifecycle_open_position_count": 0,
            "track_b_managed_futures_position_count": 0,
            "current_scope_review_required_count": 0,
        },
    }


def test_london_shadow_cannot_submit_or_mutate_broker(tmp_path: Path) -> None:
    _write_runtime_bars(
        tmp_path,
        "MNQ",
        [
            {
                "bar_start": "2026-06-01T06:59:00+00:00",
                "bar_end": "2026-06-01T07:00:00+00:00",
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100,
                "volume": 10,
                "completed": True,
            },
            {
                "bar_start": "2026-06-01T07:00:00+00:00",
                "bar_end": "2026-06-01T07:01:00+00:00",
                "open": 100,
                "high": 103,
                "low": 100,
                "close": 102,
                "volume": 20,
                "completed": True,
            },
        ],
    )

    report = build_london_active_evidence_shadow_report(repo_root=tmp_path, paper_stack_status=_clean_status())

    assert report["broker_authoritative_lane_activation"] is False
    assert report["submit_allowed"] is False
    assert report["broker_mutation_allowed"] is False
    for row in report["rows"]:
        assert row["broker_action"]["broker_authority"] is False
        assert row["broker_action"]["submit_allowed"] is False
        assert row["broker_action"]["submit_attempted"] is False
        assert row["broker_action"]["route_created"] is False
        assert row["broker_action"]["broker_mutation_allowed"] is False


def test_london_shadow_produces_would_be_candidates_and_intents_only(tmp_path: Path) -> None:
    _write_runtime_bars(
        tmp_path,
        "MNQ",
        [
            {
                "bar_start": "2026-06-01T06:59:00+00:00",
                "bar_end": "2026-06-01T07:00:00+00:00",
                "open": 100,
                "high": 100,
                "low": 99,
                "close": 100,
                "volume": 10,
                "completed": True,
            },
            {
                "bar_start": "2026-06-01T07:00:00+00:00",
                "bar_end": "2026-06-01T07:01:00+00:00",
                "open": 100,
                "high": 103,
                "low": 100,
                "close": 102,
                "volume": 20,
                "completed": True,
            },
        ],
    )

    report = build_london_active_evidence_shadow_report(repo_root=tmp_path, paper_stack_status=_clean_status())
    mnq_open_long = next(row for row in report["rows"] if row["lane_id"] == "mnq_london_open_active_evidence_long_shadow")

    assert mnq_open_long["bars_evaluated"] == 2
    assert mnq_open_long["would_be_candidate_count"] >= 1
    assert mnq_open_long["would_be_intent_count"] == mnq_open_long["would_be_candidate_count"]
    assert mnq_open_long["hypothetical_trade_id_birth_readiness"]["trade_id_created"] is False
    assert mnq_open_long["broker_action"]["route_created"] is False


def test_registry_truth_checked_in_hypothetical_mode(tmp_path: Path) -> None:
    _write_runtime_bars(
        tmp_path,
        "MES",
        [
            {
                "bar_start": "2026-06-01T09:29:00+00:00",
                "bar_end": "2026-06-01T09:30:00+00:00",
                "open": 5000,
                "high": 5001,
                "low": 4999,
                "close": 5000,
                "volume": 10,
                "completed": True,
            },
            {
                "bar_start": "2026-06-01T09:30:00+00:00",
                "bar_end": "2026-06-01T09:31:00+00:00",
                "open": 5000,
                "high": 5001,
                "low": 4990,
                "close": 4994,
                "volume": 20,
                "completed": True,
            },
        ],
    )
    status = _clean_status()
    status["registry_truth_diagnostics"] = {
        "classification": "TRACK_B_DIAGNOSTICS_CONFLICT_CURRENT_SCOPE",
        "broker_open_order_count": 0,
        "lifecycle_open_position_count": 0,
        "track_b_managed_futures_position_count": 0,
        "current_scope_review_required_count": 1,
    }

    report = build_london_active_evidence_shadow_report(repo_root=tmp_path, paper_stack_status=status)
    mes_late_short = next(row for row in report["rows"] if row["lane_id"] == "mes_london_late_active_evidence_short_shadow")

    assert mes_late_short["would_be_candidate_count"] >= 1
    assert mes_late_short["registry_truth_entry_exposure_hypothetical"]["allowed"] is False
    assert "current_scope_review_required" in mes_late_short["registry_truth_entry_exposure_hypothetical"]["reason_codes"]
    assert mes_late_short["broker_action"]["submit_attempted"] is False


def test_london_windows_classified_correctly(tmp_path: Path) -> None:
    _write_runtime_bars(
        tmp_path,
        "MNQ",
        [
            {
                "bar_start": "2026-06-01T06:59:00+00:00",
                "bar_end": "2026-06-01T07:00:00+00:00",
                "open": 100,
                "high": 100,
                "low": 99,
                "close": 100,
                "volume": 10,
                "completed": True,
            },
            {
                "bar_start": "2026-06-01T09:29:00+00:00",
                "bar_end": "2026-06-01T09:30:00+00:00",
                "open": 105,
                "high": 106,
                "low": 104,
                "close": 105,
                "volume": 10,
                "completed": True,
            },
        ],
    )

    report = build_london_active_evidence_shadow_report(repo_root=tmp_path, paper_stack_status=_clean_status())
    mnq_open_long = next(row for row in report["rows"] if row["lane_id"] == "mnq_london_open_active_evidence_long_shadow")
    mnq_late_long = next(row for row in report["rows"] if row["lane_id"] == "mnq_london_late_active_evidence_long_shadow")

    assert mnq_open_long["bars_evaluated"] == 1
    assert mnq_late_long["bars_evaluated"] == 1


def test_existing_london_candidate_comparison_is_diagnostic_only(tmp_path: Path) -> None:
    report = build_london_active_evidence_shadow_report(repo_root=tmp_path, paper_stack_status=_clean_status())

    assert report["existing_london_candidate_comparison"]
    assert all(row["diagnostic_only"] is True for row in report["existing_london_candidate_comparison"])
