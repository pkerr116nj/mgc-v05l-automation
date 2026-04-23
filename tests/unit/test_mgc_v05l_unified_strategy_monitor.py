from __future__ import annotations

from mgc_v05l.app.unified_strategy_monitor import build_unified_strategy_monitor


def test_unified_strategy_monitor_builds_grouping_selection_and_quality_views() -> None:
    payload = build_unified_strategy_monitor(
        catalog_rows=[
            {
                "strategy_key": "bull_snap__MGC",
                "display_name": "Bull Snap",
                "instrument": "MGC",
                "strategy_family": "bull_snap",
                "lane_presence": {},
                "source_types": ["benchmark_replay", "paper_runtime"],
                "discovery_sources": ["historical_playback", "paper_runtime"],
            }
        ],
        details_by_strategy_key={},
        evidence_lanes=[
            {
                "strategy_key": "bull_snap__MGC",
                "lane_id": "benchmark_lane",
                "display_name": "Bull Snap Replay",
                "strategy_family": "bull_snap",
                "instrument": "MGC",
                "lane_type": "benchmark_replay",
                "lifecycle_truth": {"class": "BASELINE_ONLY"},
                "runtime_health": {"label": "Historical", "attached": False},
                "metrics": {
                    "realized_pnl": {"available": True, "value": 30.0},
                    "open_pnl": {"available": False, "reason": "Replay open pnl unavailable."},
                    "net_pnl": {"available": True, "value": 30.0},
                    "trade_count": {"available": True, "value": 1},
                    "win_rate": {"available": True, "value": 100.0},
                    "average_trade": {"available": True, "value": 30.0},
                    "max_drawdown": {"available": True, "value": 5.0},
                    "profit_factor": {"available": True, "value": 2.0},
                    "latest_trade_summary": {"value": {"exit_timestamp": "2026-04-21T10:34:00-04:00"}},
                    "latest_update_timestamp": {"available": True, "value": "2026-04-21T10:34:00-04:00"},
                },
                "run_identity": {"study_key": "benchmark-study"},
                "evidence": {},
            },
            {
                "strategy_key": "bull_snap__MGC",
                "lane_id": "paper_lane",
                "paper_lane_id": "paper_lane",
                "display_name": "Bull Snap Paper",
                "strategy_family": "bull_snap",
                "instrument": "MGC",
                "lane_type": "paper_runtime",
                "lifecycle_truth": {"class": "FULL_LIFECYCLE_TRUTH"},
                "runtime_health": {"label": "READY", "attached": True},
                "metrics": {
                    "realized_pnl": {"available": True, "value": 25.0},
                    "open_pnl": {"available": True, "value": 5.0},
                    "net_pnl": {"available": True, "value": 30.0},
                    "trade_count": {"available": True, "value": 1},
                    "win_rate": {"available": True, "value": 100.0},
                    "average_trade": {"available": True, "value": 25.0},
                    "max_drawdown": {"available": True, "value": 4.0},
                    "profit_factor": {"available": True, "value": 1.8},
                    "latest_trade_summary": {"value": {"exit_timestamp": "2026-04-21T11:18:00-04:00"}},
                    "latest_update_timestamp": {"available": True, "value": "2026-04-22T11:30:00+00:00"},
                },
                "position_side": "FLAT",
                "evidence": {},
            },
        ],
        historical_playback={
            "study_catalog": {
                "items": [
                    {
                        "study_key": "benchmark-study",
                        "study": {
                            "pnl_points": [
                                {"timestamp": "2026-04-21T10:34:00-04:00", "realized": 30.0, "total": 30.0}
                            ],
                            "trade_events": [
                                {"event_type": "EXIT_FILL", "event_timestamp": "2026-04-21T10:34:00-04:00", "realized_pnl": 30.0}
                            ],
                        },
                    }
                ]
            }
        },
        paper={
            "strategy_performance": {
                "trade_log": [
                    {
                        "lane_id": "paper_lane",
                        "standalone_strategy_id": "bull_snap__MGC",
                        "status": "CLOSED",
                        "exit_timestamp": "2026-04-21T11:18:00-04:00",
                        "realized_pnl": 25.0,
                    }
                ]
            },
            "tracked_strategies": {"rows": [], "details_by_strategy_id": {}},
            "temporary_paper_strategies": {"rows": []},
        },
        runtime_registry={"rows": []},
        lane_registry={
            "rows": [
                {
                    "lane_id": "paper_lane",
                    "runtime_instance_present": True,
                    "runtime_presence": "ACTIVE_RUNTIME",
                    "runtime_presence_tone": "good",
                    "monitoring_summary": "READY",
                }
            ]
        },
        research_analytics={},
        generated_at="2026-04-22T12:00:00+00:00",
    )

    assert payload["available"] is True
    assert payload["view_modes"]["navigation_model"]["top_level_modes"] == [
        "comparison",
        "charts",
        "leaderboard",
        "rollup",
        "data_quality",
    ]
    assert payload["selection_contract"]["default_selection_behavior"]["mode"] == "select_all_visible_lanes"
    assert payload["sort_contract"]["default_sort"]["within_groups_only"] is True
    assert payload["aggregate_rules"]["metric_rules"]["win_rate"]["mode"] == "recomputed"
    assert payload["grouping"]["default_group_keys"] == ["strategy_class", "instrument", "family"]
    assert payload["grouping"]["group_tree"] == payload["grouping"]["default_group_tree"]
    assert payload["grouping"]["default_group_tree"]
    assert payload["leaderboard_views"]["rankings"]["realized_pnl"]
    assert payload["rollup_views"]["by_lane_id"]["paper_lane"]["daily"]
    assert "metric_support_by_lane" in payload["data_quality_report"]
    assert "low_confidence_strategy_keys" in payload["identity_exceptions"]
    replay_row = next(row for row in payload["comparison_rows"] if row["lane_id"] == "benchmark_lane")
    paper_row = next(row for row in payload["comparison_rows"] if row["lane_id"] == "paper_lane")
    assert replay_row["status"]["freshness_state"] == "snapshot"
    assert replay_row["stale"] is False
    assert paper_row["status"]["freshness_state"] == "fresh"
    assert paper_row["runtime_attached"] is True
    assert paper_row["status"]["runtime_health"] == "healthy"


def test_unified_strategy_monitor_handles_mixed_lane_inventory_with_actionable_quality_gaps() -> None:
    payload = build_unified_strategy_monitor(
        catalog_rows=[
            {
                "strategy_key": "dup_alpha__MGC",
                "display_name": "Alpha Session",
                "instrument": "MGC",
                "strategy_family": "alpha_family",
                "lane_presence": {},
                "source_types": ["benchmark_replay", "paper_runtime"],
                "discovery_sources": ["historical_playback", "paper_runtime"],
            },
            {
                "strategy_key": "dup_beta__MGC",
                "display_name": "Alpha Session",
                "instrument": "MGC",
                "strategy_family": "beta_family",
                "lane_presence": {},
                "source_types": ["paper_runtime"],
                "discovery_sources": ["paper_runtime"],
            },
        ],
        details_by_strategy_key={},
        evidence_lanes=[
            {
                "strategy_key": "dup_alpha__MGC",
                "lane_id": "replay_alpha",
                "display_name": "Alpha Session",
                "strategy_family": "alpha_family",
                "instrument": "MGC",
                "lane_type": "benchmark_replay",
                "lifecycle_truth": {"class": "BASELINE_ONLY"},
                "runtime_health": {"label": "Historical", "attached": False},
                "metrics": {
                    "realized_pnl": {"available": True, "value": 40.0},
                    "open_pnl": {"available": False, "reason": "Replay open pnl unavailable."},
                    "net_pnl": {"available": True, "value": 40.0},
                    "trade_count": {"available": True, "value": 2},
                    "win_rate": {"available": True, "value": 50.0},
                    "average_trade": {"available": True, "value": 20.0},
                    "max_drawdown": {"available": True, "value": 10.0},
                    "profit_factor": {"available": True, "value": 1.5},
                    "latest_update_timestamp": {"available": True, "value": "2026-04-18T10:34:00-04:00"},
                },
                "run_identity": {"study_key": "alpha-study"},
                "evidence": {},
            },
            {
                "strategy_key": "dup_alpha__MGC",
                "lane_id": "paper_alpha",
                "paper_lane_id": "paper_alpha",
                "display_name": "Alpha Session",
                "strategy_family": "alpha_family",
                "instrument": "MGC",
                "lane_type": "paper_runtime",
                "lifecycle_truth": {"class": "FULL_LIFECYCLE_TRUTH"},
                "runtime_health": {"label": "READY", "attached": True},
                "metrics": {
                    "realized_pnl": {"available": True, "value": 60.0},
                    "open_pnl": {"available": True, "value": 5.0},
                    "net_pnl": {"available": True, "value": 65.0},
                    "trade_count": {"available": True, "value": 3},
                    "win_rate": {"available": True, "value": 66.7},
                    "average_trade": {"available": True, "value": 20.0},
                    "max_drawdown": {"available": True, "value": 8.0},
                    "profit_factor": {"available": True, "value": 1.9},
                    "latest_update_timestamp": {"available": True, "value": "2026-04-21T11:18:00-04:00"},
                },
                "position_side": "LONG",
                "evidence": {},
            },
            {
                "strategy_key": "dup_beta__MGC",
                "lane_id": "paper_beta",
                "paper_lane_id": "paper_beta",
                "display_name": "Alpha Session",
                "strategy_family": "beta_family",
                "instrument": "MGC",
                "lane_type": "paper_runtime",
                "lifecycle_truth": {"class": "FULL_LIFECYCLE_TRUTH"},
                "runtime_health": {"label": "DEGRADED", "attached": True},
                "metrics": {
                    "realized_pnl": {"available": True, "value": -15.0},
                    "open_pnl": {"available": True, "value": -2.0},
                    "net_pnl": {"available": True, "value": -17.0},
                    "trade_count": {"available": True, "value": 1},
                    "win_rate": {"available": True, "value": 0.0},
                    "average_trade": {"available": True, "value": -15.0},
                    "max_drawdown": {"available": True, "value": 17.0},
                    "profit_factor": {"available": True, "value": 0.0},
                    "latest_update_timestamp": {"available": True, "value": "2026-04-21T12:18:00-04:00"},
                },
                "position_side": "FLAT",
                "evidence": {},
            },
        ],
        historical_playback={
            "study_catalog": {
                "items": [
                    {
                        "study_key": "alpha-study",
                        "study": {
                            "pnl_points": [
                                {"timestamp": "2026-04-17T10:34:00-04:00", "realized": 10.0, "total": 10.0},
                                {"timestamp": "2026-04-18T10:34:00-04:00", "realized": 40.0, "total": 40.0},
                            ],
                            "trade_events": [
                                {"event_type": "EXIT_FILL", "event_timestamp": "2026-04-17T10:34:00-04:00", "realized_pnl": 10.0},
                                {"event_type": "EXIT_FILL", "event_timestamp": "2026-04-18T10:34:00-04:00", "realized_pnl": 30.0},
                            ],
                        },
                    }
                ]
            }
        },
        paper={
            "strategy_performance": {
                "trade_log": [
                    {
                        "lane_id": "paper_alpha",
                        "standalone_strategy_id": "dup_alpha__MGC",
                        "status": "CLOSED",
                        "exit_timestamp": "2026-04-20T11:18:00-04:00",
                        "realized_pnl": 25.0,
                    },
                    {
                        "lane_id": "paper_alpha",
                        "standalone_strategy_id": "dup_alpha__MGC",
                        "status": "CLOSED",
                        "exit_timestamp": "2026-04-21T11:18:00-04:00",
                        "realized_pnl": 35.0,
                    },
                    {
                        "lane_id": "paper_beta",
                        "standalone_strategy_id": "dup_beta__MGC",
                        "status": "CLOSED",
                        "exit_timestamp": "2026-04-21T12:18:00-04:00",
                        "realized_pnl": -15.0,
                    },
                ]
            },
            "tracked_strategies": {"rows": [], "details_by_strategy_id": {}},
            "temporary_paper_strategies": {"rows": []},
        },
        runtime_registry={"rows": []},
        lane_registry={"rows": []},
        research_analytics={},
        generated_at="2026-04-22T12:00:00+00:00",
    )

    comparison_rows = payload["comparison_rows"]
    assert len(comparison_rows) == 3
    top_groups = payload["grouping"]["default_group_tree"]
    assert top_groups
    assert any(group["group_key"] == "strategy_class" for group in top_groups)
    quality = payload["data_quality_report"]
    assert "replay_alpha" in quality["missing_metric_support"]["open_pnl"]
    assert payload["leaderboard_views"]["rankings"]["realized_pnl"][0]["lane_id"] == "paper_alpha"
    weekly_rollup = payload["rollup_views"]["by_lane_id"]["paper_alpha"]["weekly"]
    assert weekly_rollup
    identity = payload["identity_exceptions"]
    assert identity["possible_duplicates"]
    assert identity["naming_mismatches"] == []


def test_unified_strategy_monitor_low_confidence_identity_report_avoids_obvious_instrument_scoped_keys() -> None:
    payload = build_unified_strategy_monitor(
        catalog_rows=[],
        details_by_strategy_key={},
        evidence_lanes=[
            {
                "strategy_key": "approved_quant::phase2c.failed.core4_plus_qc.no_us.baseline::6E",
                "lane_id": "research_6e",
                "display_name": "Failed Move No US Reversal Short / 6E",
                "strategy_family": "failed_move_reversal",
                "instrument": "6E",
                "lane_type": "research_execution",
                "lifecycle_truth": {"class": "BASELINE_ONLY"},
                "runtime_health": {"label": "Historical", "attached": False},
                "metrics": {
                    "realized_pnl": {"available": True, "value": 10.0},
                    "open_pnl": {"available": False, "reason": "Research open pnl unavailable."},
                    "net_pnl": {"available": True, "value": 10.0},
                    "trade_count": {"available": True, "value": 1},
                    "latest_update_timestamp": {"available": True, "value": "2026-04-21T10:34:00-04:00"},
                },
                "evidence": {},
            },
            {
                "strategy_key": "atp_companion_v1_asia_us",
                "lane_id": "paper_atp",
                "display_name": "ATP Companion Baseline v1 / Asia+US",
                "strategy_family": "atp_companion",
                "instrument": "UNKNOWN",
                "lane_type": "paper_runtime",
                "lifecycle_truth": {"class": "FULL_LIFECYCLE_TRUTH"},
                "runtime_health": {"label": "DISABLED", "attached": False},
                "metrics": {
                    "realized_pnl": {"available": True, "value": 0.0},
                    "open_pnl": {"available": False, "reason": "Open pnl unavailable."},
                    "net_pnl": {"available": True, "value": 0.0},
                    "trade_count": {"available": True, "value": 0},
                    "latest_update_timestamp": {"available": True, "value": "2026-04-21T10:34:00-04:00"},
                },
                "evidence": {},
            },
        ],
        historical_playback={},
        paper={"strategy_performance": {"trade_log": []}, "tracked_strategies": {"rows": [], "details_by_strategy_id": {}}, "temporary_paper_strategies": {"rows": []}},
        runtime_registry={"rows": []},
        lane_registry={"rows": []},
        research_analytics={},
        generated_at="2026-04-22T12:00:00+00:00",
    )

    low_conf = payload["identity_exceptions"]["low_confidence_strategy_keys"]
    low_conf_lane_ids = {entry["lane_id"] for entry in low_conf}
    assert "paper_atp" in low_conf_lane_ids
    assert "research_6e" not in low_conf_lane_ids
