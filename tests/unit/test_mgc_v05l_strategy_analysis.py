from __future__ import annotations

from mgc_v05l.app.strategy_analysis import (
    LANE_TYPE_BENCHMARK_REPLAY,
    LANE_TYPE_HISTORICAL_PLAYBACK,
    LANE_TYPE_PAPER_RUNTIME,
    LANE_TYPE_RESEARCH_EXECUTION,
    build_strategy_analysis_payload,
)


def test_strategy_analysis_builds_unified_lanes_and_comparison_presets() -> None:
    strategy_key = "active_trend_participation_engine__MGC"
    baseline_study = {
        "contract_version": "strategy_study_v3",
        "symbol": "MGC",
        "standalone_strategy_id": strategy_key,
        "strategy_family": "active_trend_participation_engine",
        "timeframe": "5m",
        "meta": {
            "study_id": "baseline-study",
            "strategy_id": strategy_key,
            "strategy_family": "active_trend_participation_engine",
            "study_mode": "baseline_parity_mode",
            "entry_model": "BASELINE_NEXT_BAR_OPEN",
            "pnl_truth_basis": "BASELINE_FILL_TRUTH",
            "timeframe_truth": {
                "structural_signal_timeframe": "5m",
                "execution_timeframe": "5m",
                "artifact_timeframe": "5m",
                "execution_timeframe_role": "matches_signal_evaluation",
            },
        },
        "summary": {
            "bar_count": 2,
            "total_trades": 2,
            "long_trades": 1,
            "short_trades": 1,
            "winners": 1,
            "losers": 1,
            "cumulative_realized_pnl": "50",
            "cumulative_total_pnl": "50",
            "max_drawdown": "15",
            "session_level_behavior": [{"session_phase": "US", "bar_count": 2, "entry_marked_bars": 1}],
            "atp_summary": {"available": True, "top_atp_blocker_codes": [{"code": "ATP_NO_PULLBACK", "count": 1}]},
        },
        "bars": [
            {"bar_id": "bar-1", "timestamp": "2026-03-22T13:35:00-04:00", "strategy_status": "READY"},
            {"bar_id": "bar-2", "timestamp": "2026-03-22T13:40:00-04:00", "strategy_status": "READY"},
        ],
        "trade_events": [
            {"event_type": "ENTRY_FILL", "event_timestamp": "2026-03-22T13:35:00-04:00", "family": "ATP Companion", "side": "LONG"},
            {"event_type": "EXIT_FILL", "event_timestamp": "2026-03-22T13:40:00-04:00", "family": "ATP Companion", "side": "LONG"},
        ],
        "pnl_points": [{"timestamp": "2026-03-22T13:40:00-04:00", "realized": "50", "open_pnl": "0", "total": "50"}],
        "execution_slices": [],
    }
    research_study = {
        **baseline_study,
        "meta": {
            **baseline_study["meta"],
            "study_id": "research-study",
            "study_mode": "research_execution_mode",
            "entry_model": "EXECUTION_DETAIL",
            "pnl_truth_basis": "ENRICHED_EXECUTION_TRUTH",
            "authoritative_intrabar_available": True,
            "entry_model_capabilities": [
                {
                    "subject": "active_trend_participation_engine",
                    "supported_entry_models": ["BASELINE_NEXT_BAR_OPEN", "CURRENT_CANDLE_VWAP"],
                    "execution_truth_emitter": "atp_phase3_timing_emitter",
                    "authoritative_intrabar_available": True,
                }
            ],
            "authoritative_execution_events": [{"execution_event_type": "ENTRY_EXECUTED"}],
            "authoritative_trade_lifecycle_records": [{"trade_id": "shadow-1"}],
            "timeframe_truth": {
                "structural_signal_timeframe": "5m",
                "execution_timeframe": "1m",
                "artifact_timeframe": "5m",
                "execution_timeframe_role": "execution_detail_only",
            },
        },
        "execution_slices": [{"slice_id": "slice-1", "linked_bar_id": "bar-2", "timestamp": "2026-03-22T13:37:00-04:00"}],
    }
    payload = build_strategy_analysis_payload(
        historical_playback={
            "note": "Historical playback results stay separate from paper-runtime truth.",
            "study_catalog": {
                "items": [
                    {
                        "study_key": "baseline-study",
                        "label": "MGC / ATP / baseline",
                        "run_stamp": "replay-run-1",
                        "run_timestamp": "2026-03-22T18:00:00+00:00",
                        "symbol": "MGC",
                        "strategy_id": strategy_key,
                        "strategy_family": "active_trend_participation_engine",
                        "study_mode": "baseline_parity_mode",
                        "coverage_start": "2026-03-22T13:35:00-04:00",
                        "coverage_end": "2026-03-22T13:40:00-04:00",
                        "artifact_paths": {"strategy_study_json": "/tmp/baseline.strategy_study.json"},
                        "summary": baseline_study["summary"],
                        "study": baseline_study,
                    },
                    {
                        "study_key": "research-study",
                        "label": "MGC / ATP / research",
                        "run_stamp": "replay-run-2",
                        "run_timestamp": "2026-03-22T19:00:00+00:00",
                        "symbol": "MGC",
                        "strategy_id": strategy_key,
                        "strategy_family": "active_trend_participation_engine",
                        "study_mode": "research_execution_mode",
                        "coverage_start": "2026-03-22T13:35:00-04:00",
                        "coverage_end": "2026-03-22T13:40:00-04:00",
                        "artifact_paths": {"strategy_study_json": "/tmp/research.strategy_study.json"},
                        "summary": research_study["summary"],
                        "study": research_study,
                    },
                ]
            },
        },
        paper={
            "status": {"session_date": "2026-03-22", "stale": False},
            "raw_operator_status": {
                "lanes": [
                    {
                        "lane_id": "atp_companion_v1_asia_us",
                        "database_url": None,
                        "timeframe": "5m",
                    }
                ]
            },
            "strategy_performance": {
                "rows": [
                    {
                        "lane_id": "atp_companion_v1_asia_us",
                        "standalone_strategy_id": strategy_key,
                        "strategy_name": "ATP Companion Baseline v1",
                        "instrument": "MGC",
                        "strategy_family": "active_trend_participation_engine",
                        "source_family": "active_trend_participation_engine",
                        "cumulative_pnl": "45",
                        "realized_pnl": "40",
                        "unrealized_pnl": "5",
                        "trade_count": 2,
                        "max_drawdown": "12",
                        "entries_by_session_bucket": {"ASIA": 1, "US": 1},
                        "status": "READY",
                        "latest_activity_timestamp": "2026-03-22T18:03:00+00:00",
                        "history_start_timestamp": "2026-03-20T18:03:00+00:00",
                        "history_end_timestamp": "2026-03-22T18:03:00+00:00",
                    }
                ],
                "trade_log": [
                    {
                        "standalone_strategy_id": strategy_key,
                        "status": "CLOSED",
                        "exit_timestamp": "2026-03-22T18:02:00+00:00",
                        "signal_family_label": "ATP Companion",
                        "realized_pnl": "25",
                    },
                    {
                        "standalone_strategy_id": strategy_key,
                        "status": "CLOSED",
                        "exit_timestamp": "2026-03-21T18:02:00+00:00",
                        "signal_family_label": "ATP Companion",
                        "realized_pnl": "15",
                    },
                ],
                "attribution": {
                    "rows": [
                        {
                            "family_label": "ATP Companion",
                            "trade_count": 2,
                            "wins": 1,
                            "losses": 1,
                            "realized_pnl": "40",
                            "latest_trade_timestamp": "2026-03-22T18:02:00+00:00",
                            "source_families": ["active_trend_participation_engine"],
                            "standalone_strategy_ids": [strategy_key],
                        }
                    ]
                },
            },
            "tracked_strategies": {
                "rows": [
                    {
                        "strategy_id": "atp_companion_v1_asia_us",
                        "display_name": "ATP Companion Baseline v1",
                        "status": "READY",
                        "status_reason": "Tracked strategy is enabled and waiting for eligible paper setups.",
                        "runtime_attached": True,
                        "data_stale": False,
                        "last_update_timestamp": "2026-03-22T18:03:00+00:00",
                        "open_pnl": "5",
                        "trade_count": 2,
                        "long_trade_count": 2,
                        "short_trade_count": 0,
                        "winner_count": 1,
                        "loser_count": 1,
                        "win_rate": "50",
                        "average_trade_pnl": "20",
                        "profit_factor": "1.6667",
                        "max_drawdown": "12",
                        "cumulative_pnl": "45",
                        "realized_pnl": "40",
                        "benchmark_designation": "CURRENT_ATP_COMPANION_BENCHMARK",
                        "config_source": "config/probationary_pattern_engine_paper_atp_companion_v1_asia_us.yaml",
                        "observed_instruments": ["MGC"],
                        "current_session_segment": "US",
                        "last_trade_summary": {"exit_timestamp": "2026-03-22T18:02:00+00:00", "realized_pnl": "25"},
                        "health_flags": {"runtime_attached": True},
                    }
                ],
                "details_by_strategy_id": {
                    "atp_companion_v1_asia_us": {
                        "constituent_lanes": [{"lane_id": "atp_companion_v1_asia_us"}],
                        "artifacts": {},
                        "recent_bars": [{"end_ts": "2026-03-22T18:00:00+00:00"}],
                        "recent_signals": [{"signal_timestamp": "2026-03-22T17:55:00+00:00"}],
                        "recent_order_intents": [{"created_at": "2026-03-22T17:56:00+00:00"}],
                        "recent_fills": [{"fill_timestamp": "2026-03-22T17:57:00+00:00"}],
                        "recent_state_snapshots": [{"updated_at": "2026-03-22T17:58:00+00:00"}],
                    }
                },
            },
        },
        generated_at="2026-03-22T18:05:00+00:00",
    )

    assert payload["available"] is True
    assert payload["default_strategy_key"] == strategy_key
    detail = payload["details_by_strategy_key"][strategy_key]
    lane_types = {lane["lane_type"] for lane in detail["lanes"]}
    assert lane_types == {
        LANE_TYPE_BENCHMARK_REPLAY,
        LANE_TYPE_RESEARCH_EXECUTION,
        LANE_TYPE_PAPER_RUNTIME,
    }
    benchmark_lane = next(lane for lane in detail["lanes"] if lane["lane_type"] == LANE_TYPE_BENCHMARK_REPLAY)
    research_lane = next(lane for lane in detail["lanes"] if lane["lane_type"] == LANE_TYPE_RESEARCH_EXECUTION)
    paper_lane = next(lane for lane in detail["lanes"] if lane["lane_type"] == LANE_TYPE_PAPER_RUNTIME)
    assert benchmark_lane["lifecycle_truth"]["class"] == "BASELINE_ONLY"
    assert research_lane["lifecycle_truth"]["class"] == "FULL_LIFECYCLE_TRUTH"
    assert paper_lane["lifecycle_truth"]["class"] == "FULL_LIFECYCLE_TRUTH"
    assert benchmark_lane["metrics"]["profit_factor"]["available"] is False
    assert benchmark_lane["metrics"]["profit_factor"]["reason"] == "Replay artifact does not expose a complete closed-trade path that can be priced for profit factor."
    assert paper_lane["runtime_health"]["attached"] is True
    assert paper_lane["runtime_health"]["healthy"] is True
    assert paper_lane["metrics"]["trade_family_breakdown"]["available"] is True
    assert paper_lane["metrics"]["trade_family_breakdown"]["value"][0]["family"] == "ATP Companion"
    comparison_types = {row["comparison_type"] for row in detail["comparison_presets"]}
    assert comparison_types == {
        "benchmark_vs_paper_runtime",
        "baseline_parity_vs_research_execution",
    }
    benchmark_vs_paper = next(
        row for row in detail["comparison_presets"] if row["comparison_type"] == "benchmark_vs_paper_runtime"
    )
    assert benchmark_vs_paper["left_lane"]["lane_type"] == LANE_TYPE_BENCHMARK_REPLAY
    assert benchmark_vs_paper["right_lane"]["lane_type"] == LANE_TYPE_PAPER_RUNTIME
    assert benchmark_vs_paper["left_lane"]["lifecycle_truth"]["class"] == "BASELINE_ONLY"
    assert benchmark_vs_paper["right_lane"]["lifecycle_truth"]["class"] == "FULL_LIFECYCLE_TRUTH"
    assert benchmark_vs_paper["left_lane"]["primary_truth_source"] == "strategy_study_v3"
    assert benchmark_vs_paper["right_lane"]["primary_truth_source"] == "paper_strategy_performance_snapshot"

    results_board = payload["results_board"]
    assert results_board["defaults"]["sort_field"] == "net_pnl"
    assert results_board["defaults"]["run_scope"] == "top"
    assert {row["id"] for row in results_board["sort_fields"]} >= {
        "net_pnl",
        "average_trade",
        "profit_factor",
        "max_drawdown",
        "trade_count",
        "latest_update_timestamp",
    }
    assert any(row["id"] == "top_10_net_pnl" and row["available"] is True for row in results_board["saved_views"])
    assert any(row["id"] == "replay_vs_paper_selected_strategy" and row["available"] is True for row in results_board["saved_views"])
    reportable_row = next(row for row in results_board["rows"] if row["lane_type"] == LANE_TYPE_PAPER_RUNTIME)
    assert reportable_row["metrics"]["net_pnl"]["available"] is True
    assert reportable_row["metrics"]["average_trade"]["available"] is True
    assert reportable_row["metrics"]["profit_factor"]["available"] is True
    assert reportable_row["metrics"]["max_drawdown"]["available"] is True
    assert reportable_row["metrics"]["trade_count"]["available"] is True


def test_strategy_analysis_results_board_discovers_registry_and_candidate_lane_selectors() -> None:
    strategy_key = "alpha_strategy__MGC"
    baseline_study = {
        "contract_version": "strategy_study_v3",
        "symbol": "MGC",
        "standalone_strategy_id": strategy_key,
        "strategy_family": "alpha_strategy",
        "timeframe": "5m",
        "meta": {
            "study_id": "alpha-baseline",
            "strategy_id": strategy_key,
            "strategy_family": "alpha_strategy",
            "study_mode": "baseline_parity_mode",
            "entry_model": "BASELINE_NEXT_BAR_OPEN",
            "pnl_truth_basis": "BASELINE_FILL_TRUTH",
        },
        "summary": {
            "bar_count": 2,
            "total_trades": 1,
            "cumulative_realized_pnl": "12",
            "cumulative_total_pnl": "12",
            "max_drawdown": "4",
        },
        "bars": [{"bar_id": "bar-1", "timestamp": "2026-03-20T13:35:00-04:00"}],
        "trade_events": [],
        "pnl_points": [{"timestamp": "2026-03-20T13:40:00-04:00", "realized": "12", "open_pnl": "0", "total": "12"}],
        "execution_slices": [],
    }
    research_study = {
        **baseline_study,
        "meta": {
            **baseline_study["meta"],
            "study_id": "alpha-candidate",
            "study_mode": "research_execution_mode",
            "entry_model": "CURRENT_CANDLE_VWAP",
            "pnl_truth_basis": "ENRICHED_EXECUTION_TRUTH",
            "candidate_id": "alpha_candidate_v2",
            "authoritative_execution_events": [{"execution_event_type": "ENTRY_EXECUTED"}],
            "authoritative_trade_lifecycle_records": [{"trade_id": "trade-1"}],
        },
    }
    payload = build_strategy_analysis_payload(
        historical_playback={
            "study_catalog": {
                "items": [
                    {
                        "study_key": "alpha-baseline",
                        "label": "Alpha baseline",
                        "run_stamp": "run-1",
                        "run_timestamp": "2026-03-20T18:00:00+00:00",
                        "symbol": "MGC",
                        "strategy_id": strategy_key,
                        "strategy_family": "alpha_strategy",
                        "study_mode": "baseline_parity_mode",
                        "summary": baseline_study["summary"],
                        "study": baseline_study,
                    },
                    {
                        "study_key": "alpha-candidate",
                        "label": "Alpha candidate",
                        "run_stamp": "run-2",
                        "run_timestamp": "2026-03-20T19:00:00+00:00",
                        "symbol": "MGC",
                        "strategy_id": strategy_key,
                        "strategy_family": "alpha_strategy",
                        "study_mode": "research_execution_mode",
                        "candidate_id": "alpha_candidate_v2",
                        "summary": research_study["summary"],
                        "study": research_study,
                    },
                ]
            }
        },
        paper={"strategy_performance": {"rows": [], "trade_log": []}},
        runtime_registry={
            "rows": [
                {
                    "standalone_strategy_id": "runtime_only__GC",
                    "display_name": "Runtime Only / GC",
                    "instrument": "GC",
                    "strategy_family": "runtime_only_family",
                    "lane_id": "runtime_only_gc",
                }
            ]
        },
        lane_registry={
            "rows": [
                {
                    "lane_id": "candidate_lane_gc",
                    "display_name": "Candidate Lane GC",
                    "instrument": "GC",
                    "family": "candidate_lane_family",
                    "admission_state": "canary",
                }
            ]
        },
        generated_at="2026-03-20T19:05:00+00:00",
    )

    results_board = payload["results_board"]
    strategy_options = {row["id"]: row for row in results_board["discovery"]["strategies"]}
    assert strategy_options[strategy_key]["has_data"] is True
    assert strategy_options["runtime_only__GC"]["has_data"] is False
    assert "runtime_registry" in strategy_options["runtime_only__GC"]["source_types"]
    assert "lane_registry" in {row["id"] for row in results_board["discovery"]["source_types"]}
    assert "runtime_registry" in {row["id"] for row in results_board["discovery"]["source_types"]}

    lane_options = {row["id"]: row for row in results_board["discovery"]["lanes"]}
    assert lane_options["runtime_registry:runtime_only_gc"]["has_data"] is False
    assert lane_options["runtime_registry:runtime_only_gc"]["candidate_status_id"] == "CONFIGURED_RUNTIME"
    assert any(row["candidate_status_id"] == "RESEARCH_CANDIDATE" for row in results_board["rows"])
    assert any(row["candidate_status_id"] == "BENCHMARK_REFERENCE" for row in results_board["rows"])

    candidate_statuses = {row["id"] for row in results_board["discovery"]["candidate_statuses"]}
    assert {"BENCHMARK_REFERENCE", "RESEARCH_CANDIDATE", "CONFIGURED_RUNTIME", "CANARY"} <= candidate_statuses


def test_strategy_analysis_results_board_marks_profit_factor_saved_view_unavailable_without_supported_truth() -> None:
    strategy_key = "replay_only__GC"
    payload = build_strategy_analysis_payload(
        historical_playback={
            "study_catalog": {
                "items": [
                    {
                        "study_key": "replay-only",
                        "label": "Replay only",
                        "run_stamp": "run-1",
                        "run_timestamp": "2026-03-20T18:00:00+00:00",
                        "symbol": "GC",
                        "strategy_id": strategy_key,
                        "strategy_family": "replay_only",
                        "study_mode": "baseline_parity_mode",
                        "summary": {"bar_count": 1, "total_trades": 0},
                        "study": {
                            "contract_version": "strategy_study_v3",
                            "symbol": "GC",
                            "standalone_strategy_id": strategy_key,
                            "strategy_family": "replay_only",
                            "timeframe": "5m",
                            "meta": {
                                "study_id": "replay-only",
                                "strategy_id": strategy_key,
                                "study_mode": "baseline_parity_mode",
                                "pnl_truth_basis": "BASELINE_FILL_TRUTH",
                            },
                            "summary": {"bar_count": 1, "total_trades": 0},
                            "bars": [{"bar_id": "bar-1", "timestamp": "2026-03-20T13:35:00-04:00"}],
                            "trade_events": [],
                            "pnl_points": [],
                            "execution_slices": [],
                        },
                    }
                ]
            }
        },
        paper={"strategy_performance": {"rows": [], "trade_log": []}},
        generated_at="2026-03-20T18:05:00+00:00",
    )

    saved_views = {row["id"]: row for row in payload["results_board"]["saved_views"]}
    assert saved_views["top_10_profit_factor"]["available"] is False
    assert "profit-factor truth" in saved_views["top_10_profit_factor"]["unavailable_reason"]


def test_strategy_analysis_results_board_automatically_picks_up_new_registry_strategy_identity() -> None:
    baseline_payload = build_strategy_analysis_payload(
        historical_playback={"study_catalog": {"items": []}},
        paper={"strategy_performance": {"rows": [], "trade_log": []}},
        generated_at="2026-03-20T18:05:00+00:00",
    )
    updated_payload = build_strategy_analysis_payload(
        historical_playback={"study_catalog": {"items": []}},
        paper={"strategy_performance": {"rows": [], "trade_log": []}},
        runtime_registry={
            "rows": [
                {
                    "standalone_strategy_id": "new_runtime_identity__SI",
                    "display_name": "New Runtime Identity / SI",
                    "instrument": "SI",
                    "strategy_family": "silver_runtime",
                    "lane_id": "new_runtime_identity_si",
                }
            ]
        },
        generated_at="2026-03-20T18:05:00+00:00",
    )

    baseline_ids = {row["id"] for row in baseline_payload["results_board"]["discovery"]["strategies"]}
    updated_ids = {row["id"] for row in updated_payload["results_board"]["discovery"]["strategies"]}
    assert "new_runtime_identity__SI" not in baseline_ids
    assert "new_runtime_identity__SI" in updated_ids


def test_strategy_analysis_marks_legacy_replay_artifacts_as_historical_playback() -> None:
    payload = build_strategy_analysis_payload(
        historical_playback={
            "study_catalog": {
                "items": [
                    {
                        "study_key": "legacy-study",
                        "label": "Legacy study",
                        "run_stamp": "legacy-run",
                        "run_timestamp": "2026-03-20T18:00:00+00:00",
                        "symbol": "GC",
                        "strategy_family": "legacy_strategy_family",
                        "artifact_paths": {"strategy_study_json": "/tmp/legacy.strategy_study.json"},
                        "summary": {"bar_count": 1, "total_trades": 0},
                        "study": {
                            "contract_version": "strategy_study_v3",
                            "symbol": "GC",
                            "strategy_family": "legacy_strategy_family",
                            "timeframe": "5m",
                            "meta": {"study_id": "legacy-study", "timeframe_truth": {"artifact_timeframe": "5m"}},
                            "summary": {"bar_count": 1, "total_trades": 0},
                            "bars": [{"bar_id": "bar-1", "timestamp": "2026-03-20T13:35:00-04:00"}],
                            "trade_events": [],
                            "pnl_points": [],
                            "execution_slices": [],
                        },
                    }
                ]
            }
        },
        paper={"strategy_performance": {"rows": [], "trade_log": []}},
        generated_at="2026-03-20T18:05:00+00:00",
    )

    strategy_key = payload["default_strategy_key"]
    detail = payload["details_by_strategy_key"][strategy_key]
    lane = detail["lanes"][0]
    assert lane["lane_type"] == LANE_TYPE_HISTORICAL_PLAYBACK
    assert lane["lane_label"] == "Historical Playback"
    assert lane["lifecycle_truth"]["class"] == "BASELINE_ONLY"
    assert lane["metrics"]["latest_trade_summary"]["available"] is False
    assert lane["metrics"]["trade_count"]["value"] == 0


def test_strategy_analysis_marks_hybrid_and_unsupported_research_truth_classes() -> None:
    strategy_key = "legacy_runtime__MGC"
    payload = build_strategy_analysis_payload(
        historical_playback={
            "study_catalog": {
                "items": [
                    {
                        "study_key": "hybrid-study",
                        "label": "Hybrid research study",
                        "run_stamp": "hybrid-run",
                        "run_timestamp": "2026-03-24T18:00:00+00:00",
                        "symbol": "MGC",
                        "strategy_id": strategy_key,
                        "strategy_family": "legacy_runtime",
                        "study_mode": "research_execution_mode",
                        "coverage_start": "2026-03-24T01:00:00+00:00",
                        "coverage_end": "2026-03-24T01:20:00+00:00",
                        "artifact_paths": {"strategy_study_json": "/tmp/hybrid.strategy_study.json"},
                        "summary": {"bar_count": 4, "total_trades": 1},
                        "study": {
                            "contract_version": "strategy_study_v3",
                            "symbol": "MGC",
                            "standalone_strategy_id": strategy_key,
                            "strategy_family": "legacy_runtime",
                            "timeframe": "5m",
                            "meta": {
                                "study_id": "hybrid-study",
                                "strategy_id": strategy_key,
                                "study_mode": "research_execution_mode",
                                "entry_model": "CURRENT_CANDLE_VWAP",
                                "pnl_truth_basis": "HYBRID_ENTRY_BASELINE_EXIT_TRUTH",
                                "unsupported_reason": None,
                                "entry_model_capabilities": [{"subject": "asiaVWAPLongSignal"}],
                            },
                            "summary": {"bar_count": 4, "total_trades": 1},
                            "bars": [{"bar_id": "bar-1", "timestamp": "2026-03-24T01:05:00+00:00"}],
                            "trade_events": [],
                            "pnl_points": [],
                            "execution_slices": [],
                        },
                    },
                    {
                        "study_key": "unsupported-study",
                        "label": "Unsupported research study",
                        "run_stamp": "unsupported-run",
                        "run_timestamp": "2026-03-24T19:00:00+00:00",
                        "symbol": "MGC",
                        "strategy_id": strategy_key,
                        "strategy_family": "legacy_runtime",
                        "study_mode": "research_execution_mode",
                        "coverage_start": "2026-03-24T13:00:00+00:00",
                        "coverage_end": "2026-03-24T13:05:00+00:00",
                        "artifact_paths": {"strategy_study_json": "/tmp/unsupported.strategy_study.json"},
                        "summary": {"bar_count": 1, "total_trades": 0},
                        "study": {
                            "contract_version": "strategy_study_v3",
                            "symbol": "MGC",
                            "standalone_strategy_id": strategy_key,
                            "strategy_family": "legacy_runtime",
                            "timeframe": "5m",
                            "meta": {
                                "study_id": "unsupported-study",
                                "strategy_id": strategy_key,
                                "study_mode": "research_execution_mode",
                                "entry_model": "CURRENT_CANDLE_VWAP",
                                "pnl_truth_basis": "UNSUPPORTED_ENTRY_MODEL",
                                "unsupported_reason": "CURRENT_CANDLE_VWAP is not implemented for this family.",
                                "entry_model_capabilities": [{"subject": "firstBullSnapTurn"}],
                            },
                            "summary": {"bar_count": 1, "total_trades": 0},
                            "bars": [{"bar_id": "bar-1", "timestamp": "2026-03-24T13:05:00+00:00"}],
                            "trade_events": [],
                            "pnl_points": [],
                            "execution_slices": [],
                        },
                    },
                ]
            }
        },
        paper={"strategy_performance": {"rows": [], "trade_log": []}},
        generated_at="2026-03-24T19:05:00+00:00",
    )

    detail = payload["details_by_strategy_key"][strategy_key]
    lifecycle_by_study = {
        lane["run_identity"]["study_key"]: lane["lifecycle_truth"]["class"]
        for lane in detail["lanes"]
        if lane["lane_type"] == LANE_TYPE_RESEARCH_EXECUTION
    }
    assert lifecycle_by_study == {
        "hybrid-study": "HYBRID_ENTRY_BASELINE_EXIT_TRUTH",
        "unsupported-study": "UNSUPPORTED",
    }


def test_strategy_analysis_supports_replay_profit_factor_when_closed_trade_prices_are_persisted() -> None:
    strategy_key = "breakout_engine__MGC"
    study = {
        "contract_version": "strategy_study_v3",
        "symbol": "MGC",
        "point_value": "10",
        "standalone_strategy_id": strategy_key,
        "strategy_family": "breakout_engine",
        "timeframe": "5m",
        "meta": {
            "study_id": "priced-study",
            "strategy_id": strategy_key,
            "study_mode": "baseline_parity_mode",
            "entry_model": "BASELINE_NEXT_BAR_OPEN",
            "pnl_truth_basis": "BASELINE_FILL_TRUTH",
        },
        "summary": {
            "bar_count": 4,
            "total_trades": 2,
            "cumulative_realized_pnl": "30",
            "cumulative_total_pnl": "30",
        },
        "bars": [
            {"bar_id": "bar-1", "start_timestamp": "2026-03-22T13:30:00-04:00", "end_timestamp": "2026-03-22T13:35:00-04:00", "session_phase": "US"},
            {"bar_id": "bar-2", "start_timestamp": "2026-03-22T13:35:00-04:00", "end_timestamp": "2026-03-22T13:40:00-04:00", "session_phase": "US"},
            {"bar_id": "bar-3", "start_timestamp": "2026-03-22T13:40:00-04:00", "end_timestamp": "2026-03-22T13:45:00-04:00", "session_phase": "US"},
            {"bar_id": "bar-4", "start_timestamp": "2026-03-22T13:45:00-04:00", "end_timestamp": "2026-03-22T13:50:00-04:00", "session_phase": "US"},
        ],
        "trade_events": [
            {"event_id": "e1", "event_type": "ENTRY_FILL", "linked_bar_id": "bar-1", "event_timestamp": "2026-03-22T13:35:00-04:00", "family": "Breakout A", "side": "LONG", "event_price": "100"},
            {"event_id": "e2", "event_type": "EXIT_FILL", "linked_bar_id": "bar-2", "event_timestamp": "2026-03-22T13:40:00-04:00", "family": "Breakout A", "side": "LONG", "event_price": "105", "reason": "TARGET"},
            {"event_id": "e3", "event_type": "ENTRY_FILL", "linked_bar_id": "bar-3", "event_timestamp": "2026-03-22T13:45:00-04:00", "family": "Breakout B", "side": "LONG", "event_price": "110"},
            {"event_id": "e4", "event_type": "EXIT_FILL", "linked_bar_id": "bar-4", "event_timestamp": "2026-03-22T13:50:00-04:00", "family": "Breakout B", "side": "LONG", "event_price": "108", "reason": "STOP"},
        ],
        "pnl_points": [],
        "execution_slices": [],
    }
    payload = build_strategy_analysis_payload(
        historical_playback={
            "study_catalog": {
                "items": [
                    {
                        "study_key": "priced-study",
                        "label": "Priced replay",
                        "run_stamp": "priced-run",
                        "run_timestamp": "2026-03-22T18:00:00+00:00",
                        "symbol": "MGC",
                        "strategy_id": strategy_key,
                        "strategy_family": "breakout_engine",
                        "study_mode": "baseline_parity_mode",
                        "summary": study["summary"],
                        "study": study,
                    }
                ]
            }
        },
        paper={"strategy_performance": {"rows": [], "trade_log": []}},
        generated_at="2026-03-22T18:05:00+00:00",
    )

    lane = payload["details_by_strategy_key"][strategy_key]["lanes"][0]
    assert lane["metrics"]["profit_factor"]["available"] is True
    assert lane["metrics"]["profit_factor"]["value"] == "2.5"
    assert lane["metrics"]["trade_family_breakdown"]["available"] is True
    assert {row["family"] for row in lane["metrics"]["trade_family_breakdown"]["value"]} == {"Breakout A", "Breakout B"}
    assert lane["metrics"]["latest_trade_summary"]["available"] is True
    assert lane["metrics"]["latest_trade_summary"]["value"]["family"] == "Breakout B"


def test_strategy_analysis_preserves_paper_trade_lifecycle_preview_without_lane_db() -> None:
    strategy_key = "paper_preview__MGC"
    payload = build_strategy_analysis_payload(
        historical_playback={"study_catalog": {"items": []}},
        paper={
            "status": {"session_date": "2026-03-22", "stale": False},
            "raw_operator_status": {"lanes": [{"lane_id": "paper_preview_lane", "database_url": None, "timeframe": "5m"}]},
            "strategy_performance": {
                "rows": [
                    {
                        "lane_id": "paper_preview_lane",
                        "standalone_strategy_id": strategy_key,
                        "strategy_name": "Paper Preview Strategy",
                        "instrument": "MGC",
                        "strategy_family": "preview_family",
                        "source_family": "preview_family",
                        "cumulative_pnl": "10",
                        "realized_pnl": "10",
                        "trade_count": 1,
                    }
                ],
                "trade_log": [],
            },
            "tracked_strategies": {
                "rows": [
                    {
                        "strategy_id": "paper_preview_lane",
                        "display_name": "Paper Preview Strategy",
                        "status": "READY",
                        "runtime_attached": True,
                        "last_update_timestamp": "2026-03-22T18:03:00+00:00",
                        "truth_provenance": {"run_lane": "PAPER_RUNTIME"},
                    }
                ],
                "details_by_strategy_id": {
                    "paper_preview_lane": {
                        "constituent_lanes": [{"lane_id": "paper_preview_lane"}],
                        "recent_trades": [
                            {
                                "trade_id": "trade-1",
                                "entry_timestamp": "2026-03-22T17:56:00+00:00",
                                "exit_timestamp": "2026-03-22T18:01:00+00:00",
                                "signal_family": "Preview Family",
                                "realized_pnl": "10",
                            }
                        ],
                    }
                },
            },
        },
        generated_at="2026-03-22T18:05:00+00:00",
    )

    lane = payload["details_by_strategy_key"][strategy_key]["lanes"][0]
    assert lane["evidence"]["trade_lifecycle"]["available"] is True
    assert lane["evidence"]["trade_lifecycle"]["preview_rows"][0]["trade_id"] == "trade-1"
    assert lane["source_of_truth"]["truth_provenance"]["run_lane"] == "PAPER_RUNTIME"


def test_strategy_analysis_supports_replay_profit_factor_from_closed_trade_breakdown_variants() -> None:
    strategy_key = "variant_breakout_engine__MGC"
    study = {
        "contract_version": "strategy_study_v3",
        "symbol": "MGC",
        "point_value": "10",
        "standalone_strategy_id": strategy_key,
        "strategy_family": "variant_breakout_engine",
        "timeframe": "5m",
        "meta": {
            "study_id": "variant-priced-study",
            "strategy_id": strategy_key,
            "study_mode": "baseline_parity_mode",
            "entry_model": "BASELINE_NEXT_BAR_OPEN",
            "pnl_truth_basis": "BASELINE_FILL_TRUTH",
        },
        "summary": {
            "bar_count": 4,
            "total_trades": 2,
            "closed_trade_breakdown": [
                {
                    "trade_id": "t1",
                    "family_label": "Breakout A",
                    "direction": "LONG",
                    "entry_ts": "2026-03-22T13:35:00-04:00",
                    "exit_ts": "2026-03-22T13:40:00-04:00",
                    "entry_price": "100",
                    "exit_price": "105",
                    "pnl_points": "5",
                    "primary_exit_reason": "TARGET",
                },
                {
                    "trade_id": "t2",
                    "family_label": "Breakout B",
                    "direction": "LONG",
                    "entry_ts": "2026-03-22T13:45:00-04:00",
                    "exit_ts": "2026-03-22T13:50:00-04:00",
                    "entry_price": "110",
                    "exit_price": "108",
                    "pnl_points": "-2",
                    "primary_exit_reason": "STOP",
                },
            ],
        },
        "bars": [
            {"bar_id": "bar-1", "start_timestamp": "2026-03-22T13:30:00-04:00", "end_timestamp": "2026-03-22T13:35:00-04:00", "session_phase": "US"},
            {"bar_id": "bar-2", "start_timestamp": "2026-03-22T13:35:00-04:00", "end_timestamp": "2026-03-22T13:40:00-04:00", "session_phase": "US"},
            {"bar_id": "bar-3", "start_timestamp": "2026-03-22T13:40:00-04:00", "end_timestamp": "2026-03-22T13:45:00-04:00", "session_phase": "US"},
            {"bar_id": "bar-4", "start_timestamp": "2026-03-22T13:45:00-04:00", "end_timestamp": "2026-03-22T13:50:00-04:00", "session_phase": "US"},
        ],
        "trade_events": [],
        "pnl_points": [],
        "execution_slices": [],
    }
    payload = build_strategy_analysis_payload(
        historical_playback={
            "study_catalog": {
                "items": [
                    {
                        "study_key": "variant-priced-study",
                        "label": "Variant priced replay",
                        "run_stamp": "variant-priced-run",
                        "run_timestamp": "2026-03-22T18:00:00+00:00",
                        "symbol": "MGC",
                        "strategy_id": strategy_key,
                        "strategy_family": "variant_breakout_engine",
                        "study_mode": "baseline_parity_mode",
                        "summary": study["summary"],
                        "study": study,
                    }
                ]
            }
        },
        paper={"strategy_performance": {"rows": [], "trade_log": []}},
        generated_at="2026-03-22T18:05:00+00:00",
    )

    lane = payload["details_by_strategy_key"][strategy_key]["lanes"][0]
    assert lane["metrics"]["profit_factor"]["available"] is True
    assert lane["metrics"]["profit_factor"]["value"] == "2.5"
    assert lane["metrics"]["latest_trade_summary"]["value"]["family"] == "Breakout B"
    assert lane["metrics"]["latest_trade_summary"]["value"]["truth_source"] == "closed_trade_breakdown"


def test_strategy_analysis_derives_paper_breakdowns_from_complete_recent_trade_preview() -> None:
    strategy_key = "paper_complete_preview__MGC"
    payload = build_strategy_analysis_payload(
        historical_playback={"study_catalog": {"items": []}},
        paper={
            "status": {"session_date": "2026-03-22", "stale": False},
            "strategy_performance": {"rows": [], "trade_log": []},
            "tracked_strategies": {
                "rows": [
                    {
                        "strategy_id": strategy_key,
                        "display_name": "Paper Complete Preview",
                        "status": "READY",
                        "runtime_attached": True,
                        "last_update_timestamp": "2026-03-22T18:03:00+00:00",
                        "trade_count": 2,
                        "cumulative_pnl": "10",
                        "realized_pnl": "10",
                    }
                ],
                "details_by_strategy_id": {
                    strategy_key: {
                        "constituent_lanes": [{"lane_id": strategy_key}],
                        "recent_trades": [
                            {
                                "trade_id": "trade-2",
                                "entry_timestamp": "2026-03-22T10:35:00-04:00",
                                "exit_timestamp": "2026-03-22T10:40:00-04:00",
                                "family": "US Resume",
                                "side": "SHORT",
                                "realized_pnl": "-10",
                                "exit_reason": "STOP",
                            },
                            {
                                "trade_id": "trade-1",
                                "entry_timestamp": "2026-03-21T18:05:00-04:00",
                                "exit_timestamp": "2026-03-21T18:10:00-04:00",
                                "family": "Asia Breakout",
                                "side": "LONG",
                                "realized_pnl": "20",
                                "exit_reason": "TARGET",
                            },
                        ],
                    }
                },
            },
        },
        generated_at="2026-03-22T18:05:00+00:00",
    )

    lane = payload["details_by_strategy_key"][strategy_key]["lanes"][0]
    assert lane["metrics"]["profit_factor"]["available"] is True
    assert lane["metrics"]["profit_factor"]["value"] == "2"
    assert lane["metrics"]["trade_family_breakdown"]["available"] is True
    assert {row["family"] for row in lane["metrics"]["trade_family_breakdown"]["value"]} == {
        "Asia Breakout",
        "US Resume",
    }
    assert lane["metrics"]["session_breakdown"]["available"] is True
    assert {row["session"] for row in lane["metrics"]["session_breakdown"]["value"]} == {
        "ASIA_EARLY",
        "US_MIDDAY",
    }
    assert lane["metrics"]["latest_trade_summary"]["value"]["trade_id"] == "trade-2"


def test_strategy_analysis_prefers_exact_tracked_paper_breakdowns_over_preview_inference() -> None:
    strategy_key = "paper_exact_summary__MGC"
    payload = build_strategy_analysis_payload(
        historical_playback={"study_catalog": {"items": []}},
        paper={
            "status": {"session_date": "2026-03-22", "stale": False},
            "strategy_performance": {"rows": [], "trade_log": []},
            "tracked_strategies": {
                "rows": [
                    {
                        "strategy_id": strategy_key,
                        "display_name": "Paper Exact Summary",
                        "status": "READY",
                        "runtime_attached": True,
                        "last_update_timestamp": "2026-03-22T18:03:00+00:00",
                        "trade_count": 5,
                        "cumulative_pnl": "10",
                        "realized_pnl": "10",
                        "session_breakdown": [
                            {"session": "US_LATE", "trade_count": 3, "wins": 2, "losses": 1, "realized_pnl": "15"}
                        ],
                        "trade_family_breakdown": [
                            {"family": "ATP Companion", "trade_count": 5, "wins": 3, "losses": 2, "realized_pnl": "10"}
                        ],
                    }
                ],
                "details_by_strategy_id": {
                    strategy_key: {
                        "constituent_lanes": [{"lane_id": strategy_key}],
                        "recent_trades": [
                            {
                                "trade_id": "trade-2",
                                "entry_timestamp": "2026-03-22T10:35:00-04:00",
                                "exit_timestamp": "2026-03-22T10:40:00-04:00",
                                "family": "Partial Preview",
                                "side": "SHORT",
                                "realized_pnl": "-10",
                            }
                        ],
                    }
                },
            },
        },
        generated_at="2026-03-22T18:05:00+00:00",
    )

    lane = payload["details_by_strategy_key"][strategy_key]["lanes"][0]
    assert lane["metrics"]["trade_family_breakdown"]["available"] is True
    assert lane["metrics"]["trade_family_breakdown"]["value"][0]["family"] == "ATP Companion"
    assert lane["metrics"]["session_breakdown"]["available"] is True
    assert lane["metrics"]["session_breakdown"]["value"][0]["session"] == "US_LATE"
