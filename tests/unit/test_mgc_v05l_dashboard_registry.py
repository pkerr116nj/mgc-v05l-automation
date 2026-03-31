"""Tests for the dashboard-facing lane registry."""

from __future__ import annotations

from mgc_v05l.app.dashboard_registry import build_dashboard_lane_registry


def test_dashboard_lane_registry_auto_surfaces_new_admitted_lane_without_template_changes() -> None:
    registry = build_dashboard_lane_registry(
        approved_quant_baselines={
            "rows": [
                {
                    "lane_id": "phase2c.breakout.metals_only.us_unknown.baseline",
                    "lane_name": "breakout_metals_us_unknown_continuation",
                    "probation_status": "normal",
                    "promotion_state": "operator_baseline_candidate",
                    "approved_exit_label": "time_stop_only.h24",
                    "approved_scope": {
                        "symbols": ["GC", "MGC", "HG", "PL"],
                        "allowed_sessions": ["US", "UNKNOWN"],
                    },
                    "post_cost_monitoring_read": {"label": "stable_positive_post_cost"},
                    "symbol_attribution_summary": ["GC +0.100R (4)"],
                    "session_attribution_summary": ["US +0.120R (5)"],
                }
            ]
        },
        paper_approved_models={
            "rows": [
                {
                    "lane_id": "legacy_gc_long",
                    "branch": "GC / usLatePauseResumeLongTurn",
                    "source_family": "usLatePauseResumeLongTurn",
                    "instrument": "GC",
                    "session_restriction": "US_LATE",
                    "enabled": True,
                    "state": "ENABLED",
                    "side": "LONG",
                    "signal_count": 4,
                    "blocked_count": 1,
                    "intent_count": 2,
                    "fill_count": 1,
                    "open_position": False,
                    "chain_state": "CLEAN",
                    "latest_activity_type": "SIGNAL",
                    "realized_pnl": "25.0",
                    "unrealized_pnl": "0.0",
                    "risk_state": "OK",
                },
                {
                    "lane_id": "legacy_pl_short",
                    "branch": "PL / asiaEarlyPauseResumeShortTurn",
                    "source_family": "asiaEarlyPauseResumeShortTurn",
                    "instrument": "PL",
                    "session_restriction": "ASIA_EARLY",
                    "enabled": True,
                    "state": "ENABLED",
                    "side": "SHORT",
                    "signal_count": 1,
                    "blocked_count": 0,
                    "intent_count": 1,
                    "fill_count": 1,
                    "open_position": True,
                    "chain_state": "OPEN",
                    "latest_activity_type": "FILL",
                    "realized_pnl": "10.0",
                    "unrealized_pnl": "5.0",
                    "risk_state": "OK",
                },
            ]
        },
        paper_non_approved_lanes={
            "rows": [
                {
                    "lane_id": "canary_gc_once",
                    "display_name": "GC Canary Once",
                    "instrument": "GC",
                    "session_restriction": "US",
                    "lane_mode": "PAPER_EXECUTION_CANARY",
                    "is_canary": True,
                    "position_side": "FLAT",
                    "open_position": False,
                    "fired": True,
                    "signal_count": 1,
                    "intent_count": 1,
                    "fill_count": 1,
                    "entry_state": "COMPLETE",
                    "exit_state": "COMPLETE",
                    "lifecycle_state": "ENTRY_AND_EXIT_COMPLETE",
                    "latest_activity_timestamp": "2026-03-21T14:00:00-04:00",
                }
            ]
        },
    )

    sections = {section["key"]: section for section in registry["sections"]}
    assert registry["section_order"] == ["approved_quant", "admitted_paper", "canary"]
    approved_quant_rows = sections["approved_quant"]["rows"]
    assert len(approved_quant_rows) == 4
    assert approved_quant_rows[0]["standalone_strategy_id"].endswith("__GC")
    assert approved_quant_rows[1]["standalone_strategy_id"].endswith("__MGC")
    assert sections["admitted_paper"]["summary_metrics"][0]["value"] == "2"
    admitted_names = [row["display_name"] for row in sections["admitted_paper"]["rows"]]
    assert "GC / usLatePauseResumeLongTurn" in admitted_names
    assert "PL / asiaEarlyPauseResumeShortTurn" in admitted_names
    assert sections["admitted_paper"]["rows"][0]["standalone_strategy_id"]
    assert sections["canary"]["title"] == "Temporary Paper Strategies"
    assert sections["canary"]["rows"][0]["display_name"] == "GC Canary Once"
