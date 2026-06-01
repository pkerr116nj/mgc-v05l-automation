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


def test_shadow_selector_collapses_candidate_spam_to_one_trade(tmp_path: Path) -> None:
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
            {
                "bar_start": "2026-06-01T07:01:00+00:00",
                "bar_end": "2026-06-01T07:02:00+00:00",
                "open": 102,
                "high": 104,
                "low": 102,
                "close": 103,
                "volume": 20,
                "completed": True,
            },
            {
                "bar_start": "2026-06-01T07:02:00+00:00",
                "bar_end": "2026-06-01T07:03:00+00:00",
                "open": 103,
                "high": 105,
                "low": 103,
                "close": 104,
                "volume": 20,
                "completed": True,
            },
        ],
    )

    report = build_london_active_evidence_shadow_report(repo_root=tmp_path, paper_stack_status=_clean_status())
    selector = report["shadow_trade_selector"]

    assert selector["selected_hypothetical_trade"]["symbol"] == "MNQ"
    assert selector["selected_hypothetical_trade"]["direction"] == "LONG"
    assert selector["selected_hypothetical_trade"]["confirmation_cluster"]["confirmed"] is True
    assert selector["selected_hypothetical_trade"]["broker_action"]["route_created"] is False
    assert selector["rejected_candidate_count"] >= 2
    assert {row["reason"] for row in selector["rejected_candidates"]} == {
        "same_london_session_reentry_after_timebox_not_allowed"
    }


def test_shadow_selector_blocks_direct_long_short_flip(tmp_path: Path) -> None:
    _write_runtime_bars(
        tmp_path,
        "MNQ",
        [
            {"bar_start": "2026-06-01T06:59:00+00:00", "bar_end": "2026-06-01T07:00:00+00:00", "open": 100, "high": 100, "low": 99, "close": 100, "volume": 10, "completed": True},
            {"bar_start": "2026-06-01T07:00:00+00:00", "bar_end": "2026-06-01T07:01:00+00:00", "open": 100, "high": 103, "low": 100, "close": 102, "volume": 20, "completed": True},
            {"bar_start": "2026-06-01T07:01:00+00:00", "bar_end": "2026-06-01T07:02:00+00:00", "open": 102, "high": 104, "low": 102, "close": 103, "volume": 20, "completed": True},
            {"bar_start": "2026-06-01T07:02:00+00:00", "bar_end": "2026-06-01T07:03:00+00:00", "open": 103, "high": 105, "low": 103, "close": 104, "volume": 20, "completed": True},
                {"bar_start": "2026-06-01T07:03:00+00:00", "bar_end": "2026-06-01T07:04:00+00:00", "open": 104, "high": 104, "low": 99, "close": 99, "volume": 20, "completed": True},
                {"bar_start": "2026-06-01T07:04:00+00:00", "bar_end": "2026-06-01T07:05:00+00:00", "open": 99, "high": 99, "low": 96, "close": 97, "volume": 20, "completed": True},
                {"bar_start": "2026-06-01T07:05:00+00:00", "bar_end": "2026-06-01T07:06:00+00:00", "open": 97, "high": 97, "low": 94, "close": 95, "volume": 20, "completed": True},
                {"bar_start": "2026-06-01T07:06:00+00:00", "bar_end": "2026-06-01T07:07:00+00:00", "open": 95, "high": 95, "low": 92, "close": 93, "volume": 20, "completed": True},
                {"bar_start": "2026-06-01T07:07:00+00:00", "bar_end": "2026-06-01T07:08:00+00:00", "open": 93, "high": 93, "low": 90, "close": 91, "volume": 20, "completed": True},
            ],
        )

    report = build_london_active_evidence_shadow_report(repo_root=tmp_path, paper_stack_status=_clean_status())
    reasons = {row["reason"] for row in report["shadow_trade_selector"]["rejected_candidates"]}

    assert "no_direct_long_short_flip_after_shadow_entry" in reasons


def test_shadow_selector_enforces_combined_mnq_mes_conflict_group(tmp_path: Path) -> None:
    bars = [
        {"bar_start": "2026-06-01T06:59:00+00:00", "bar_end": "2026-06-01T07:00:00+00:00", "open": 100, "high": 100, "low": 99, "close": 100, "volume": 10, "completed": True},
        {"bar_start": "2026-06-01T07:00:00+00:00", "bar_end": "2026-06-01T07:01:00+00:00", "open": 100, "high": 103, "low": 100, "close": 102, "volume": 20, "completed": True},
        {"bar_start": "2026-06-01T07:01:00+00:00", "bar_end": "2026-06-01T07:02:00+00:00", "open": 102, "high": 104, "low": 102, "close": 103, "volume": 20, "completed": True},
        {"bar_start": "2026-06-01T07:02:00+00:00", "bar_end": "2026-06-01T07:03:00+00:00", "open": 103, "high": 105, "low": 103, "close": 104, "volume": 20, "completed": True},
    ]
    _write_runtime_bars(tmp_path, "MNQ", bars)
    _write_runtime_bars(tmp_path, "MES", bars)

    report = build_london_active_evidence_shadow_report(repo_root=tmp_path, paper_stack_status=_clean_status())
    selector = report["shadow_trade_selector"]

    assert selector["selected_hypothetical_trade"]["symbol"] == "MNQ"
    assert selector["conflict_group"] == "equity_index_mnq_mes_london_shadow"
    assert "mnq_mes_conflict_group_already_selected" in {row["reason"] for row in selector["rejected_candidates"]}


def test_shadow_trade_id_cannot_be_used_for_live_submit(tmp_path: Path) -> None:
    _write_runtime_bars(
        tmp_path,
        "MNQ",
        [
            {"bar_start": "2026-06-01T06:59:00+00:00", "bar_end": "2026-06-01T07:00:00+00:00", "open": 100, "high": 100, "low": 99, "close": 100, "volume": 10, "completed": True},
            {"bar_start": "2026-06-01T07:00:00+00:00", "bar_end": "2026-06-01T07:01:00+00:00", "open": 100, "high": 103, "low": 100, "close": 102, "volume": 20, "completed": True},
            {"bar_start": "2026-06-01T07:01:00+00:00", "bar_end": "2026-06-01T07:02:00+00:00", "open": 102, "high": 104, "low": 102, "close": 103, "volume": 20, "completed": True},
            {"bar_start": "2026-06-01T07:02:00+00:00", "bar_end": "2026-06-01T07:03:00+00:00", "open": 103, "high": 105, "low": 103, "close": 104, "volume": 20, "completed": True},
        ],
    )

    report = build_london_active_evidence_shadow_report(repo_root=tmp_path, paper_stack_status=_clean_status())
    selected = report["shadow_trade_selector"]["selected_hypothetical_trade"]

    assert selected["trade_id"].startswith("shadow_london_")
    assert selected["trade_id_namespace"] == "SHADOW_ONLY_NOT_LIVE_SUBMIT_ELIGIBLE"
    assert selected["can_be_used_for_live_submit"] is False
    assert selected["broker_action"]["submit_attempted"] is False
    assert selected["broker_action"]["route_created"] is False
