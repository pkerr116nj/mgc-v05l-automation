from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_strategy_underperformance_review import (
    StrategyUnderperformanceReviewConfig,
    build_strategy_underperformance_review,
    create_strategy_underperformance_review,
    simulate_forward_outcome,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _append_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_forward_simulator_classifies_missed_winner() -> None:
    candles = [
        {"bar_end": "2026-05-25T00:00:00+00:00", "open": 100, "high": 101, "low": 99.5, "close": 100},
        {"bar_end": "2026-05-25T00:05:00+00:00", "open": 100, "high": 104, "low": 99.8, "close": 103},
        {"bar_end": "2026-05-25T00:10:00+00:00", "open": 103, "high": 106, "low": 102, "close": 105},
    ]

    result = simulate_forward_outcome(
        candles=candles,
        timestamp="2026-05-25T00:00:00+00:00",
        direction="LONG",
        entry_price=100,
    )

    assert result["outcome_classification"] == "MISSED_WINNER"
    assert result["mfe"] == 6


def test_underperformance_review_builds_taxonomy_and_reports(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "outputs/track_b_execution_core/p0_observe_only/latest_p0_observe_only_loop.json",
        {
            "classification": "P0_GUARDED_PAPER_LOOP_NO_CANDIDATE",
            "completed_iterations": 2,
            "enabled_strategy_ids": ["asian_drift_v1", "MNQ_FIRST_BULL_SNAP_TURN_V1"],
        },
    )
    _append_jsonl(
        tmp_path / "outputs/track_b_execution_core/p0_observe_only/p0_observe_only_loop_events.jsonl",
        [
            {
                "generated_at": "2026-05-25T00:00:00+00:00",
                "per_strategy": [
                    {
                        "strategy_id": "asian_drift_v1",
                        "decision": "NO_SIGNAL",
                        "failed_predicates": ["state_is_entry_eligible", "entry_ready"],
                        "signal_emitted": False,
                    },
                    {
                        "strategy_id": "MNQ_FIRST_BULL_SNAP_TURN_V1",
                        "decision": "NO_SIGNAL",
                        "failed_predicates": ["bull_snap_reversal_bar"],
                        "signal_emitted": False,
                    },
                ],
            }
        ],
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/diagnostics/latest_track_b_no_signal_attribution_rollup.json",
        {
            "evaluated_decision_bar_records": [
                {
                    "strategy_id": "asian_drift_v1",
                    "failed_predicates": ["missing_18_00_et_session_anchor_context"],
                    "failed_predicates_count": 1,
                }
            ]
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/diagnostics/latest_p0_near_miss_predicate_pressure_audit.json",
        {
            "asian_drift": {
                "late_join_strong_drift_count": 1,
                "max_drift_score": 4.7,
                "late_join_events": [
                    {
                        "generated_at": "2026-05-25T00:00:00+00:00",
                        "score": 4.7,
                    }
                ],
            },
            "overall": {"classifications": ["P0_NO_TRADE_ANCHOR_POLICY_TOO_STRICT"]},
        },
    )
    for symbol in ("MGC", "MNQ"):
        _write_json(
            tmp_path
            / f"outputs/track_b_execution_core/phase1_runtime_market_data/{symbol}/5m/latest_runtime_candles.json",
            {
                "bars": [
                    {"bar_end": "2026-05-25T00:00:00+00:00", "open": 100, "high": 101, "low": 99.5, "close": 100},
                    {"bar_end": "2026-05-25T00:05:00+00:00", "open": 100, "high": 104, "low": 99.8, "close": 103},
                    {"bar_end": "2026-05-25T00:10:00+00:00", "open": 103, "high": 106, "low": 102, "close": 105},
                ]
            },
        )
    _append_jsonl(
        tmp_path / "outputs/track_b_execution_core/paper_trade_ledger/track_b_paper_trade_ledger.jsonl",
        [
            {
                "strategy_id": "asian_drift_v1",
                "contract_key": "MGC-202606",
                "entry_timestamp": "2026-05-25T00:00:00+00:00",
                "entry_fill_price": "100",
                "entry_order_id": "1",
                "exit_order_id": "2",
                "exit_fill_confirmed": True,
                "exit_fill_price": None,
                "close_bridge_classification": "KNOWN_MANAGED_EXIT_ORDER_FILLED_CLOSE_PERSISTENCE_GAP",
            }
        ],
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_paper_trade_summary.json",
        {"broker_backed_trade_count": 1},
    )
    _write_json(
        tmp_path / "config/track_b_guarded_paper_roster.json",
        {
            "enabled_strategy_ids": [
                "asian_drift_v1",
                "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            ]
        },
    )
    _write_json(
        tmp_path / "outputs/reports/entry_acceptance_research/full_history_batch/combined_cross_instrument_summary.json",
        {
            "acceptance_class_counts": {
                "EXACT_STRUCTURAL_MATCH": 10,
                "NEAR_STRUCTURAL_MATCH": 70,
            },
            "baseline_episodes": 8,
            "near_gte_0_80_episodes": 56,
            "near_to_baseline_episode_ratio": 7.0,
            "aggregate_metrics": {
                "baseline": {
                    "24b_next_bar_open_0_5_cost": {
                        "average_return": 0.2,
                        "profit_factor_proxy": 1.1,
                        "win_rate": 0.48,
                        "avg_mfe": 3.0,
                        "avg_mae": -2.0,
                    }
                },
                "near_gte_0_80": {
                    "24b_next_bar_open_0_5_cost": {
                        "average_return": -0.1,
                        "profit_factor_proxy": 0.9,
                        "win_rate": 0.44,
                        "avg_mfe": 4.0,
                        "avg_mae": -5.0,
                    },
                    "12b_next_bar_open_0_5_cost": {
                        "average_return": -0.2,
                        "profit_factor_proxy": 0.8,
                        "win_rate": 0.42,
                        "avg_mfe": 2.0,
                        "avg_mae": -3.0,
                    },
                },
            },
        },
    )
    decision_path = tmp_path / "docs/track_b_entry_acceptance_candidate_universe_decision.md"
    decision_path.parent.mkdir(parents=True, exist_ok=True)
    decision_path.write_text("EXACT_BASELINE_RETAINED\nNEAR_EXPANSION_PARKED\n", encoding="utf-8")
    atp_config = tmp_path / "config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us.yaml"
    atp_config.parent.mkdir(parents=True, exist_ok=True)
    atp_config.write_text(
        "probationary_paper_lanes_json: '[{\"lane_id\":\"atp_companion_v1_gc_asia_us\","
        "\"standalone_strategy_id\":\"atp_companion_v1__paper_gc_asia_us\","
        "\"strategy_family\":\"active_trend_participation_engine\","
        "\"strategy_identity_root\":\"ATP_COMPANION_V1\","
        "\"symbol\":\"GC\",\"quality_bucket_policy\":\"MEDIUM_HIGH_ONLY\","
        "\"experimental_status\":\"paper_candidate\",\"paper_only\":true,\"non_approved\":true}]'\n",
        encoding="utf-8",
    )

    config = StrategyUnderperformanceReviewConfig(repo_root=tmp_path)
    report = build_strategy_underperformance_review(
        config=config,
        now=datetime(2026, 5, 25, tzinfo=UTC),
    )

    assert report["broker_mutation_allowed"] is False
    assert report["paper_proof_allowed"] is False
    assert report["dashboard_projection_consumed"] is False
    assert report["rejection_attribution"]["total_one_gate_away"] >= 2
    assert report["forward_outcome_simulation"]["outcome_counts"]["MISSED_WINNER"] >= 1
    assert any(
        item["root_cause"] == "ANCHOR_POLICY_TOO_STRICT_FOR_LATE_JOIN_DRIFT"
        for item in report["top_underperformance_root_causes"]
    )
    assert any(
        item["action"] in {"ADD_SHADOW_VARIANT", "ADD_NEW_REGIME_FAMILY"}
        for item in report["ranked_remediation_board"]
    )
    assert report["exit_position_management_attribution"]["attribution_counts"][
        "ENTRY_GOOD_ORDER_MANAGEMENT_BAD"
    ] == 1
    promotion = report["high_quality_strategy_promotion_audit"]
    assert promotion["primary_large_sample_family_match"] == "TRACK_B_ENTRY_ACCEPTANCE_ASIA_EARLY_BREAKOUT_RETEST_HOLD"
    assert "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1" in promotion["primary_finding"]
    assert report["threshold_pressure_analysis"]["authoritative_threshold_recommendation"] == "KEEP_80_AUTHORITATIVE"
    assert report["threshold_pressure_analysis"]["expected_trade_frequency_increase"]["near_gte_0_80_vs_exact"] == 7.0


def test_create_writes_json_and_markdown(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "outputs/track_b_execution_core/p0_observe_only/latest_p0_observe_only_loop.json",
        {"enabled_strategy_ids": ["asian_drift_v1"]},
    )
    _append_jsonl(
        tmp_path / "outputs/track_b_execution_core/p0_observe_only/p0_observe_only_loop_events.jsonl",
        [],
    )
    config = StrategyUnderperformanceReviewConfig(repo_root=tmp_path)

    result = create_strategy_underperformance_review(
        config=config,
        now=datetime(2026, 5, 25, tzinfo=UTC),
    )

    assert result.report_json.exists()
    assert result.report_md.exists()
    assert "Track B Strategy Underperformance Review v1" in result.report_md.read_text(encoding="utf-8")
