from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_missed_opportunity_discovery import (
    ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_V1,
    MissedOpportunityDiscoveryConfig,
    build_asian_drift_late_join_missing_anchor_shadow_family,
    build_atp_trend_participation_shadow,
    build_missed_opportunity_forward_outcomes,
    build_near_miss_scored_shadow,
    build_shadow_candidate_maturation,
    build_timestamp_locked_forward_evidence,
    compute_shadow_forward_outcome,
    create_missed_opportunity_discovery_layer,
    rank_b_grade_missed_winner_families,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_near_miss_scoring_is_general_not_atp_specific() -> None:
    review = {
        "rejection_attribution": {
            "near_miss_examples": [
                {
                    "strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
                    "failed_predicates": ["signal_retests_and_holds_breakout_level"],
                    "gate_distance": 1,
                    "hypothetical_direction": "LONG",
                },
                {
                    "strategy_id": "MNQ_FIRST_BULL_SNAP_TURN_V1",
                    "failed_predicates": ["bull_snap_reversal_bar", "bull_snap_velocity_ok"],
                    "gate_distance": 2,
                    "hypothetical_direction": "LONG",
                },
            ]
        },
        "forward_outcome_simulation": {
            "simulations": [
                {
                    "strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
                    "outcome_classification": "MISSED_WINNER",
                }
            ]
        },
    }

    payload = build_near_miss_scored_shadow(
        review=review,
        overfiltering={},
        now=datetime(2026, 5, 26, tzinfo=UTC),
        min_b_score=0.75,
    )

    assert payload["scoring_scope"] == "ALL_TRACK_B_STRATEGY_FAMILIES"
    strategy_ids = {row["strategy_id"] for row in payload["scored_candidates"]}
    assert "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1" in strategy_ids
    assert "MNQ_FIRST_BULL_SNAP_TURN_V1" in strategy_ids
    assert all(row["submit_allowed"] is False for row in payload["scored_candidates"])


def test_atp_shadow_uses_phase1_candles_without_authority(tmp_path: Path) -> None:
    config_path = tmp_path / "config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "probationary_paper_lanes_json: '[{\"lane_id\":\"atp_companion_v1_gc_asia_us\","
        "\"standalone_strategy_id\":\"atp_companion_v1__paper_gc_asia_us\","
        "\"strategy_family\":\"active_trend_participation_engine\","
        "\"strategy_identity_root\":\"ATP_COMPANION_V1\","
        "\"symbol\":\"GC\",\"long_sources\":[\"trend\"],\"short_sources\":[],"
        "\"quality_bucket_policy\":\"MEDIUM_HIGH_ONLY\","
        "\"experimental_status\":\"paper_candidate\",\"paper_only\":true,\"non_approved\":true}]'\n",
        encoding="utf-8",
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/phase1_runtime_market_data/MGC/5m/latest_runtime_candles.json",
        {
            "symbol": "MGC",
            "timeframe": "5m",
            "realtime_feed_confirmed": True,
            "bars": [
                {"bar_end": "2026-05-26T00:00:00+00:00", "high": 100, "low": 99, "close": 100},
                {"bar_end": "2026-05-26T00:05:00+00:00", "high": 101, "low": 100, "close": 101},
                {"bar_end": "2026-05-26T00:10:00+00:00", "high": 102, "low": 101, "close": 102},
                {"bar_end": "2026-05-26T00:15:00+00:00", "high": 103, "low": 102, "close": 103},
                {"bar_end": "2026-05-26T00:20:00+00:00", "high": 104, "low": 103, "close": 104},
                {"bar_end": "2026-05-26T00:25:00+00:00", "high": 105, "low": 104, "close": 105},
            ],
        },
    )

    payload = build_atp_trend_participation_shadow(
        repo_root=tmp_path,
        phase1_root=tmp_path / "outputs/track_b_execution_core/phase1_runtime_market_data",
        active_roster=set(),
        review={},
        shared_context={"control_plane_classification": "CONTROL_PLANE_SNAPSHOT_READY"},
        now=datetime(2026, 5, 26, tzinfo=UTC),
        min_score=0.5,
    )

    assert payload["candidate_count"] == 1
    assert payload["candidates"][0]["shadow_only"] is True
    assert payload["candidates"][0]["broker_mutation_allowed"] is False
    assert payload["candidates"][0]["shadow_classification"] in {
        "ATP_SHADOW_CANDIDATE_LIVE_STRATEGIES_SILENT",
        "ATP_SHADOW_LOW_CONFIDENCE",
    }


def test_forward_outcome_classifies_favorable_candidate_as_missed_winner() -> None:
    candidate = {"timestamp": "2026-05-26T00:00:00+00:00", "direction": "LONG", "reference_price": 100}
    candles = [
        {"candle_timestamp": "2026-05-26T00:00:00+00:00", "high": 100.5, "low": 99.5, "close": 100},
        {"candle_timestamp": "2026-05-26T00:05:00+00:00", "high": 103, "low": 100.5, "close": 102.5},
        {"candle_timestamp": "2026-05-26T00:10:00+00:00", "high": 104, "low": 102, "close": 103.5},
        {"candle_timestamp": "2026-05-26T00:15:00+00:00", "high": 105, "low": 103, "close": 104},
    ]

    outcome = compute_shadow_forward_outcome(candidate=candidate, candles=candles)

    assert outcome["outcome_classification"] == "MISSED_WINNER"
    assert outcome["profit_harvest_exit_helped"] is True


def test_shadow_candidate_maturation_preserves_and_matures_pending_atp_candidate(tmp_path: Path) -> None:
    phase1_root = tmp_path / "outputs/track_b_execution_core/phase1_runtime_market_data"
    candidate = {
        "candidates": [
            {
                "candidate_id": "atp_mgc",
                "strategy_id": "atp_companion_v1__paper_mgc_asia",
                "strategy_family": "active_trend_participation_engine",
                "shadow_classification": "ATP_SHADOW_CANDIDATE_LIVE_STRATEGIES_SILENT",
                "instrument": "MGC",
                "direction": "LONG",
                "source_candle_timestamp": "2026-05-26T00:00:00+00:00",
                "source_authority_path": "/tmp/mgc.json",
                "score": 0.8,
                "confidence": "HIGH",
            }
        ]
    }
    _write_json(
        phase1_root / "MGC/5m/latest_runtime_candles.json",
        {
            "bars": [
                {"bar_end": "2026-05-26T00:00:00+00:00", "high": 100.5, "low": 99.5, "close": 100},
                {"bar_end": "2026-05-26T00:05:00+00:00", "high": 103, "low": 100.5, "close": 102},
            ]
        },
    )
    first = build_shadow_candidate_maturation(
        atp_shadow=candidate,
        near_miss_shadow={},
        phase1_root=phase1_root,
        now=datetime(2026, 5, 26, 0, 5, tzinfo=UTC),
    )
    assert first["candidate_count"] == 1
    assert first["partially_matured_count"] == 1

    new_candidate = {
        "candidates": [
            {
                "candidate_id": "atp_new",
                "strategy_id": "atp_companion_v1__paper_mgc_later",
                "strategy_family": "active_trend_participation_engine",
                "shadow_classification": "ATP_SHADOW_CANDIDATE_LIVE_STRATEGIES_SILENT",
                "instrument": "MGC",
                "direction": "LONG",
                "source_candle_timestamp": "2026-05-26T00:25:00+00:00",
                "score": 0.82,
                "confidence": "HIGH",
            }
        ]
    }
    _write_json(
        phase1_root / "MGC/5m/latest_runtime_candles.json",
        {
            "bars": [
                {"bar_end": "2026-05-26T00:00:00+00:00", "high": 100.5, "low": 99.5, "close": 100},
                {"bar_end": "2026-05-26T00:05:00+00:00", "high": 103, "low": 100.5, "close": 102},
                {"bar_end": "2026-05-26T00:10:00+00:00", "high": 104, "low": 102, "close": 103},
                {"bar_end": "2026-05-26T00:15:00+00:00", "high": 105, "low": 103, "close": 104},
                {"bar_end": "2026-05-26T00:20:00+00:00", "high": 106, "low": 104, "close": 105},
                {"bar_end": "2026-05-26T00:25:00+00:00", "high": 107, "low": 105, "close": 106},
                {"bar_end": "2026-05-26T00:30:00+00:00", "high": 108, "low": 106, "close": 107},
                {"bar_end": "2026-05-26T00:35:00+00:00", "high": 109, "low": 107, "close": 108},
                {"bar_end": "2026-05-26T00:40:00+00:00", "high": 110, "low": 108, "close": 109},
                {"bar_end": "2026-05-26T00:45:00+00:00", "high": 111, "low": 109, "close": 110},
                {"bar_end": "2026-05-26T00:50:00+00:00", "high": 112, "low": 110, "close": 111},
                {"bar_end": "2026-05-26T00:55:00+00:00", "high": 113, "low": 111, "close": 112},
                {"bar_end": "2026-05-26T01:00:00+00:00", "high": 114, "low": 112, "close": 113},
            ]
        },
    )
    second = build_shadow_candidate_maturation(
        atp_shadow=new_candidate,
        near_miss_shadow={},
        phase1_root=phase1_root,
        prior_maturation=first,
        now=datetime(2026, 5, 26, 0, 30, tzinfo=UTC),
    )
    ids = {row["candidate_id"] for row in second["records"]}
    assert ids == {"atp_mgc", "atp_new"}
    old = next(row for row in second["records"] if row["candidate_id"] == "atp_mgc")
    assert old["maturation_status"] == "FINAL"
    assert old["outcome_classification"] == "MISSED_WINNER"
    new = next(row for row in second["records"] if row["candidate_id"] == "atp_new")
    assert new["maturation_status"] in {"PARTIALLY_MATURED", "PENDING"}
    assert second["broker_mutation_allowed"] is False


def test_shadow_candidate_maturation_dedupes_existing_candidate(tmp_path: Path) -> None:
    phase1_root = tmp_path / "outputs/track_b_execution_core/phase1_runtime_market_data"
    _write_json(
        phase1_root / "MGC/5m/latest_runtime_candles.json",
        {"bars": [{"bar_end": "2026-05-26T00:00:00+00:00", "high": 100, "low": 99, "close": 100}]},
    )
    shadow = {
        "candidates": [
            {
                "candidate_id": "atp_mgc",
                "strategy_id": "atp_companion_v1__paper_mgc_asia",
                "shadow_classification": "ATP_SHADOW_CANDIDATE_LIVE_STRATEGIES_SILENT",
                "instrument": "MGC",
                "direction": "LONG",
                "source_candle_timestamp": "2026-05-26T00:00:00+00:00",
            }
        ]
    }
    first = build_shadow_candidate_maturation(
        atp_shadow=shadow,
        near_miss_shadow={},
        phase1_root=phase1_root,
        now=datetime(2026, 5, 26, tzinfo=UTC),
    )
    second = build_shadow_candidate_maturation(
        atp_shadow=shadow,
        near_miss_shadow={},
        phase1_root=phase1_root,
        prior_maturation=first,
        now=datetime(2026, 5, 26, 0, 5, tzinfo=UTC),
    )
    assert second["candidate_count"] == 1
    assert second["records"][0]["seen_count"] == 2


def test_forward_outcome_classifies_profit_harvest_giveback_as_missed_winner() -> None:
    candidate = {"timestamp": "2026-05-26T00:00:00+00:00", "direction": "LONG", "reference_price": 100}
    candles = [
        {"candle_timestamp": "2026-05-26T00:00:00+00:00", "high": 100.5, "low": 99.5, "close": 100},
        {"candle_timestamp": "2026-05-26T00:05:00+00:00", "high": 106, "low": 99, "close": 104},
        {"candle_timestamp": "2026-05-26T00:10:00+00:00", "high": 105, "low": 101, "close": 103},
        {"candle_timestamp": "2026-05-26T00:15:00+00:00", "high": 103, "low": 98, "close": 99},
    ]

    outcome = compute_shadow_forward_outcome(candidate=candidate, candles=candles)

    assert outcome["outcome_classification"] == "MISSED_WINNER"
    assert outcome["simple_timebox_exit_helped"] is False
    assert outcome["profit_harvest_exit_helped"] is True


def test_forward_outcome_classifies_adverse_candidate_as_avoided_loser() -> None:
    candidate = {"timestamp": "2026-05-26T00:00:00+00:00", "direction": "LONG", "reference_price": 100}
    candles = [
        {"candle_timestamp": "2026-05-26T00:00:00+00:00", "high": 100.5, "low": 99.5, "close": 100},
        {"candle_timestamp": "2026-05-26T00:05:00+00:00", "high": 100.2, "low": 96, "close": 97},
        {"candle_timestamp": "2026-05-26T00:10:00+00:00", "high": 98, "low": 95, "close": 96},
        {"candle_timestamp": "2026-05-26T00:15:00+00:00", "high": 97, "low": 94, "close": 95},
    ]

    outcome = compute_shadow_forward_outcome(candidate=candidate, candles=candles)

    assert outcome["outcome_classification"] == "AVOIDED_LOSER"
    assert outcome["simple_timebox_exit_helped"] is False


def test_forward_outcome_is_unclear_with_incomplete_future_bars() -> None:
    candidate = {"timestamp": "2026-05-26T00:00:00+00:00", "direction": "LONG", "reference_price": 100}
    candles = [
        {"candle_timestamp": "2026-05-26T00:00:00+00:00", "high": 100.5, "low": 99.5, "close": 100}
    ]

    outcome = compute_shadow_forward_outcome(candidate=candidate, candles=candles)

    assert outcome["outcome_classification"] == "UNCLEAR"


def test_forward_outcome_rejects_candidate_outside_candle_window() -> None:
    candidate = {"timestamp": "2026-05-25T00:00:00+00:00", "direction": "LONG", "reference_price": 100}
    candles = [
        {"candle_timestamp": "2026-05-26T00:00:00+00:00", "high": 110, "low": 99, "close": 108},
        {"candle_timestamp": "2026-05-26T00:05:00+00:00", "high": 112, "low": 107, "close": 111},
    ]

    outcome = compute_shadow_forward_outcome(candidate=candidate, candles=candles)

    assert outcome["outcome_classification"] == "UNCLEAR"
    assert outcome["outcome_reason"] == "candidate_timestamp_outside_candle_window"


def test_forward_outcome_builder_tracks_b_grade_candidates_without_authority(tmp_path: Path) -> None:
    near_miss = {
        "scored_candidates": [
            {
                "strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
                "approval_grade": "B",
                "generated_at": "2026-05-26T00:00:00+00:00",
                "hypothetical_direction": "LONG",
                "near_miss_score": 0.88,
            }
        ]
    }
    _write_json(
        tmp_path / "MGC/5m/latest_runtime_candles.json",
        {
            "symbol": "MGC",
            "bars": [
                {"bar_end": "2026-05-26T00:00:00+00:00", "high": 100, "low": 99, "close": 100},
                {"bar_end": "2026-05-26T00:05:00+00:00", "high": 103, "low": 100, "close": 102},
            ],
        },
    )

    payload = build_missed_opportunity_forward_outcomes(
        atp_shadow={"candidates": []},
        near_miss_shadow=near_miss,
        phase1_root=tmp_path,
        now=datetime(2026, 5, 26, tzinfo=UTC),
    )

    assert payload["candidate_count"] == 1
    assert payload["submit_allowed"] is False
    assert payload["broker_mutation_allowed"] is False
    assert payload["outcomes"][0]["not_lifecycle_authority"] is True


def test_timestamp_locked_evidence_fails_closed_for_old_candidate_outside_window(tmp_path: Path) -> None:
    near_miss = {
        "scored_candidates": [
            {
                "strategy_id": "asian_drift_v1",
                "approval_grade": "B",
                "generated_at": "2026-05-25T00:00:00+00:00",
                "hypothetical_direction": "LONG",
                "near_miss_score": 0.88,
            }
        ]
    }
    _write_json(
        tmp_path / "MGC/5m/latest_runtime_candles.json",
        {
            "symbol": "MGC",
            "bars": [
                {"bar_end": "2026-05-26T00:00:00+00:00", "high": 110, "low": 99, "close": 108},
                {"bar_end": "2026-05-26T00:05:00+00:00", "high": 112, "low": 107, "close": 111},
            ],
        },
    )

    evidence = build_timestamp_locked_forward_evidence(
        atp_shadow={"candidates": []},
        near_miss_shadow=near_miss,
        phase1_root=tmp_path,
        now=datetime(2026, 5, 26, tzinfo=UTC),
    )
    outcomes = build_missed_opportunity_forward_outcomes(
        atp_shadow={"candidates": []},
        near_miss_shadow=near_miss,
        phase1_root=tmp_path,
        now=datetime(2026, 5, 26, tzinfo=UTC),
        timestamp_locked_evidence=evidence,
    )

    assert evidence["evidence"][0]["timestamp_lock_classification"] == "UNCLEAR_CANDIDATE_OUTSIDE_REPLAY_WINDOW"
    assert evidence["evidence"][0]["timestamp_locked_candle_window"] == []
    assert outcomes["outcomes"][0]["outcome_classification"] == "UNCLEAR"
    assert outcomes["outcomes"][0]["outcome_reason"] == "UNCLEAR_CANDIDATE_OUTSIDE_REPLAY_WINDOW"


def test_timestamp_locked_evidence_scores_candidate_with_valid_future_bars(tmp_path: Path) -> None:
    near_miss = {
        "scored_candidates": [
            {
                "strategy_id": "asian_drift_v1",
                "approval_grade": "B",
                "generated_at": "2026-05-26T00:00:00+00:00",
                "hypothetical_direction": "LONG",
                "near_miss_score": 0.88,
            }
        ]
    }
    _write_json(
        tmp_path / "MGC/5m/latest_runtime_candles.json",
        {
            "symbol": "MGC",
            "bars": [
                {"bar_end": "2026-05-26T00:00:00+00:00", "high": 100, "low": 99, "close": 100},
                {"bar_end": "2026-05-26T00:05:00+00:00", "high": 104, "low": 100, "close": 103},
                {"bar_end": "2026-05-26T00:10:00+00:00", "high": 105, "low": 102, "close": 104},
            ],
        },
    )

    evidence = build_timestamp_locked_forward_evidence(
        atp_shadow={"candidates": []},
        near_miss_shadow=near_miss,
        phase1_root=tmp_path,
        now=datetime(2026, 5, 26, tzinfo=UTC),
    )
    outcomes = build_missed_opportunity_forward_outcomes(
        atp_shadow={"candidates": []},
        near_miss_shadow=near_miss,
        phase1_root=tmp_path,
        now=datetime(2026, 5, 26, tzinfo=UTC),
        timestamp_locked_evidence=evidence,
    )

    assert evidence["evidence"][0]["timestamp_lock_classification"] == "TIMESTAMP_LOCKED_FORWARD_EVIDENCE_READY"
    assert evidence["evidence"][0]["forward_window_availability"]["5m"] is True
    assert outcomes["outcomes"][0]["outcome_classification"] == "MISSED_WINNER"


def test_replay_backed_timestamp_locked_evidence_scores_historical_candidate(tmp_path: Path) -> None:
    import pandas as pd

    near_miss = {
        "scored_candidates": [
            {
                "strategy_id": "asian_drift_v1",
                "approval_grade": "B",
                "generated_at": "2026-05-25T01:47:00+00:00",
                "hypothetical_direction": "LONG",
                "near_miss_score": 0.88,
            }
        ]
    }
    replay_path = (
        tmp_path
        / "MGC/2026Q2/datasets/derived_bars_5m/symbol=MGC/year=2026/shard_id=2026Q2/bars.parquet"
    )
    replay_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "symbol": "MGC",
                "timeframe": "5m",
                "bar_ts": "2026-05-25T01:45:00+00:00",
                "open": 100,
                "high": 100,
                "low": 99,
                "close": 100,
                "volume": 1,
            },
            {
                "symbol": "MGC",
                "timeframe": "5m",
                "bar_ts": "2026-05-25T01:50:00+00:00",
                "open": 100,
                "high": 104,
                "low": 100,
                "close": 103,
                "volume": 1,
            },
            {
                "symbol": "MGC",
                "timeframe": "5m",
                "bar_ts": "2026-05-25T01:55:00+00:00",
                "open": 103,
                "high": 105,
                "low": 102,
                "close": 104,
                "volume": 1,
            },
        ]
    ).to_parquet(replay_path)

    evidence = build_timestamp_locked_forward_evidence(
        atp_shadow={"candidates": []},
        near_miss_shadow=near_miss,
        phase1_root=tmp_path / "phase1",
        historical_replay_root=tmp_path,
        now=datetime(2026, 5, 26, tzinfo=UTC),
    )
    outcomes = build_missed_opportunity_forward_outcomes(
        atp_shadow={"candidates": []},
        near_miss_shadow=near_miss,
        phase1_root=tmp_path / "phase1",
        now=datetime(2026, 5, 26, tzinfo=UTC),
        timestamp_locked_evidence=evidence,
    )

    assert evidence["evidence"][0]["timestamp_lock_classification"] == "TIMESTAMP_LOCKED_FORWARD_EVIDENCE_READY"
    assert evidence["evidence"][0]["replay_source_used"] is True
    assert outcomes["outcomes"][0]["outcome_classification"] == "MISSED_WINNER"


def test_replay_lookup_blocks_mismatched_instrument_and_timeframe(tmp_path: Path) -> None:
    import pandas as pd

    near_miss = {
        "scored_candidates": [
            {
                "strategy_id": "asian_drift_v1",
                "approval_grade": "B",
                "generated_at": "2026-05-25T01:47:00+00:00",
                "hypothetical_direction": "LONG",
                "near_miss_score": 0.88,
            }
        ]
    }
    replay_path = (
        tmp_path
        / "MGC/2026Q2/datasets/derived_bars_5m/symbol=MGC/year=2026/shard_id=2026Q2/bars.parquet"
    )
    replay_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "symbol": "GC",
                "timeframe": "1m",
                "bar_ts": "2026-05-25T01:50:00+00:00",
                "open": 100,
                "high": 104,
                "low": 100,
                "close": 103,
                "volume": 1,
            }
        ]
    ).to_parquet(replay_path)

    evidence = build_timestamp_locked_forward_evidence(
        atp_shadow={"candidates": []},
        near_miss_shadow=near_miss,
        phase1_root=tmp_path / "phase1",
        historical_replay_root=tmp_path,
        now=datetime(2026, 5, 26, tzinfo=UTC),
    )

    assert evidence["evidence"][0]["timestamp_lock_classification"] == "UNCLEAR_NO_REPLAY_OR_PHASE1_CANDLES_AVAILABLE"
    assert evidence["evidence"][0]["timestamp_locked_candle_window"] == []


def test_rank_b_grade_missed_winners_extracts_late_join_drift_traits() -> None:
    near_miss = {
        "scored_candidates": [
            {
                "strategy_id": "asian_drift_v1",
                "approval_grade": "B",
                "generated_at": f"2026-05-26T00:0{index}:00+00:00",
                "near_miss_score": 0.88,
                "threshold_passed": True,
                "failed_critical_gates": [],
                "failed_noncritical_gates": ["missing_18_00_et_session_anchor_context"],
                "failed_predicates": ["missing_18_00_et_session_anchor_context"],
            }
            for index in range(3)
        ]
    }
    outcomes = {
        "outcomes": [
            {
                "candidate_id": f"asian_drift_v1:2026-05-26T00:0{index}:00+00:00",
                "strategy_id": "asian_drift_v1",
                "strategy_family": "asian_drift",
                "instrument": "MGC",
                "direction": "LONG",
                "timestamp": f"2026-05-26T00:0{index}:00+00:00",
                "score": 0.88,
                "approval_grade": "B",
                "outcome_classification": "MISSED_WINNER",
                "mfe": 5.0,
                "mae": -1.0,
                "net_movement": -0.5,
                "profit_harvest_exit_helped": True,
                "simple_timebox_exit_helped": False,
                "forward_windows": {"15m": {"complete": True, "mfe": 5.0, "net_movement": 3.0}},
            }
            for index in range(3)
        ]
    }

    ranked = rank_b_grade_missed_winner_families(
        near_miss_shadow=near_miss,
        forward_outcomes=outcomes,
        atp_shadow={"candidates": []},
    )

    assert ranked[0]["classification"] == "B_GRADE_PROMISING_SHADOW"
    assert ranked[0]["behavioral_hypothesis"] == "LATE_JOIN_DRIFT_CONTINUATION_BEHAVIOR"
    assert ranked[0]["common_failed_predicates"] == ["missing_18_00_et_session_anchor_context"]
    assert ranked[0]["promotion_allowed"] is False
    assert ranked[0]["submit_allowed"] is False


def test_persistent_late_join_missing_anchor_shadow_family_tracks_forward_evidence() -> None:
    near_miss = {
        "scored_candidates": [
            {
                "strategy_id": "asian_drift_v1",
                "approval_grade": "B",
                "generated_at": "2026-05-25T01:47:23+00:00",
                "hypothetical_direction": "LONG",
                "hypothetical_score": 4.7,
                "near_miss_score": 0.88,
                "failed_critical_gates": [],
                "failed_noncritical_gates": ["missing_18_00_et_session_anchor_context"],
                "failed_predicates": ["missing_18_00_et_session_anchor_context"],
            }
        ]
    }
    evidence = {
        "evidence": [
            {
                "candidate": {
                    "strategy_id": "asian_drift_v1",
                    "timestamp": "2026-05-25T01:47:23+00:00",
                },
                "timestamp_lock_classification": "TIMESTAMP_LOCKED_FORWARD_EVIDENCE_READY",
                "source_authority_path": "/tmp/replay/MGC/5m/bars.parquet",
            }
        ]
    }
    outcomes = {
        "outcomes": [
            {
                "strategy_id": "asian_drift_v1",
                "timestamp": "2026-05-25T01:47:23+00:00",
                "outcome_classification": "MISSED_WINNER",
                "mfe": 6.5,
                "mae": -3.0,
                "net_movement": 1.1,
                "profit_harvest_exit_helped": True,
                "forward_windows": {"30m": {"complete": True, "mfe": 6.5, "mae": -3.0}},
            }
        ]
    }

    shadow = build_asian_drift_late_join_missing_anchor_shadow_family(
        near_miss_shadow=near_miss,
        timestamp_locked_evidence=evidence,
        forward_outcomes=outcomes,
        now=datetime(2026, 5, 26, tzinfo=UTC),
    )

    assert shadow["shadow_strategy_id"] == ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_V1
    assert shadow["candidate_count"] == 1
    assert shadow["candidates"][0]["forward_outcome_classification"] == "MISSED_WINNER"
    assert shadow["candidates"][0]["missing_anchor_allowed_only_in_shadow"] is True
    assert shadow["promotion_gate_status"] == "PROMOTION_PAUSED_COLLECT_FORWARD_EVIDENCE"
    assert shadow["explainable_rule_basis"]["a_grade_asian_drift_rules_unchanged"] is True
    assert shadow["submit_allowed"] is False
    assert shadow["not_order_authority"] is True
    assert shadow["not_lifecycle_authority"] is True


def test_create_writes_discovery_artifacts(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "outputs/track_b_execution_core/diagnostics/latest_strategy_underperformance_review.json",
        {
            "strategy_inventory": ["asian_drift_v1"],
            "rejection_attribution": {
                "near_miss_examples": [
                    {
                        "strategy_id": "asian_drift_v1",
                        "failed_predicates": ["missing_18_00_et_session_anchor_context"],
                        "gate_distance": 1,
                    }
                ]
            },
            "forward_outcome_simulation": {},
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/diagnostics/latest_overfiltering_missing_regime_audit.json",
        {},
    )
    _write_json(tmp_path / "config/track_b_guarded_paper_roster.json", {"enabled_strategy_ids": ["asian_drift_v1"]})
    config = MissedOpportunityDiscoveryConfig(repo_root=tmp_path)

    report_json, report_md, report = create_missed_opportunity_discovery_layer(
        config=config,
        now=datetime(2026, 5, 26, tzinfo=UTC),
    )

    assert report_json.exists()
    assert report_md.exists()
    assert report["submit_allowed"] is False
    assert (tmp_path / "outputs/track_b_execution_core/research_shadow/latest_near_miss_scored_shadow.json").exists()
    assert (
        tmp_path / "outputs/track_b_execution_core/research_shadow/latest_timestamp_locked_forward_evidence.json"
    ).exists()
    assert (
        tmp_path / "outputs/track_b_execution_core/research_shadow/latest_shadow_candidate_maturation.json"
    ).exists()
    assert (tmp_path / "outputs/track_b_execution_core/research_shadow/latest_missed_opportunity_forward_outcomes.json").exists()
    assert (
        tmp_path
        / "outputs/track_b_execution_core/research_shadow/latest_asian_drift_late_join_missing_anchor_long_shadow.json"
    ).exists()
