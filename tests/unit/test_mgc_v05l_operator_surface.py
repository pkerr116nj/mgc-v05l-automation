"""Tests for the authoritative operator surface payload."""

from __future__ import annotations

from mgc_v05l.app.operator_surface import build_operator_surface


def test_operator_surface_exposes_exact_contract_and_rollup_integrity() -> None:
    surface = build_operator_surface(
        generated_at="2026-03-21T12:00:00+00:00",
        global_payload={
            "paper_label": "RUNNING",
            "current_session_date": "2026-03-21",
            "market_data_label": "LIVE",
            "runtime_health_label": "HEALTHY",
            "fault_state": "CLEAR",
            "desk_clean_label": "DESK CLEAN",
            "paper_run_ready_label": "READY",
            "last_processed_bar_timestamp": "2026-03-21T10:05:00-04:00",
            "last_update_timestamp": "2026-03-21T10:05:05-04:00",
        },
        auth_status={"runtime_ready": True},
        paper={
            "running": True,
            "status": {
                "entries_enabled": True,
                "operator_halt": False,
                "last_processed_bar_end_ts": "2026-03-21T10:05:00-04:00",
                "last_update_ts": "2026-03-21T10:05:05-04:00",
            },
            "readiness": {
                "runtime_phase": "RUNNING",
                "operator_halt": False,
                "entries_enabled": True,
            },
            "exceptions": {"exceptions": []},
            "performance": {
                "session_date": "2026-03-21",
                "current_session_date": "2026-03-21",
                "realized_pnl": "10.0",
                "unrealized_pnl": "2.0",
                "total_pnl": "12.0",
                "session_metrics": {
                    "open_trade_count": 1,
                },
            },
            "session_shape": {
                "max_intraday_drawdown": "5.0",
            },
            "position": {
                "instrument": "GC",
                "side": "LONG",
                "unrealized_pnl": "2.0",
                "latest_bar_close": "3010.5",
            },
            "full_blotter_rows": [
                {
                    "instrument": "GC",
                    "setup_family": "usLatePauseResumeLongTurn",
                    "entry_ts": "2026-03-21T09:35:00-04:00",
                    "exit_ts": "2026-03-21T09:40:00-04:00",
                    "net_pnl": "10.0",
                }
            ],
            "approved_models": {
                "details_by_branch": {
                    "GC / usLatePauseResumeLongTurn": {"unresolved_intent_count": 0, "open_position": True},
                },
                "rows": [
                    {
                        "lane_id": "legacy_gc_long",
                        "branch": "GC / usLatePauseResumeLongTurn",
                        "instrument": "GC",
                        "source_family": "usLatePauseResumeLongTurn",
                        "session_restriction": "US_LATE",
                        "enabled": True,
                        "side": "LONG",
                        "decision_count": 1,
                        "signal_count": 1,
                        "intent_count": 0,
                        "blocked_count": 0,
                        "open_position": True,
                        "latest_activity_timestamp": "2026-03-21T09:35:00-04:00",
                        "realized_pnl": "10.0",
                        "unrealized_pnl": "2.0",
                        "risk_state": "OK",
                    }
                ],
            },
            "non_approved_lanes": {
                "rows": [
                    {
                        "lane_id": "canary_gc",
                        "display_name": "GC Canary Once",
                        "instrument": "GC",
                        "session_restriction": "US",
                        "lane_mode": "PAPER_EXECUTION_CANARY",
                        "position_side": "FLAT",
                        "state": "ENABLED",
                        "fired": True,
                        "entry_completed": True,
                        "exit_completed": True,
                        "latest_activity_timestamp": "2026-03-21T09:40:00-04:00",
                        "risk_state": "OK",
                    }
                ]
            },
        },
        approved_quant_baselines={
            "rows": [
                {
                    "lane_id": "phase2c.breakout.metals_only.us_unknown.baseline",
                    "lane_name": "breakout_metals_us_unknown_continuation",
                    "probation_status": "normal",
                    "promotion_state": "operator_baseline_candidate",
                    "latest_signal_timestamp": "2026-03-21T09:30:00-04:00",
                    "approved_exit_label": "time_stop_only.h24",
                    "approved_scope": {
                        "family": "breakout_continuation",
                        "direction": "LONG",
                        "symbols": ["GC", "MGC"],
                        "allowed_sessions": ["US", "UNKNOWN"],
                    },
                    "warning_flags": [],
                    "unknown_session_warning": {"flag": False},
                    "post_cost_monitoring_read": {"label": "stable_positive_post_cost"},
                }
            ]
        },
        market_context={"feed_label": "INDEX FEED LIVE", "feed_state": "LIVE", "symbols": [], "note": "ok"},
        treasury_curve={"feed_label": "TREASURY LIVE", "feed_state": "LIVE", "rows": [], "coverage_note": "ok"},
    )

    assert set(surface) >= {
        "runtime_readiness",
        "operator_metrics_portfolio",
        "operator_metrics_by_instrument",
        "current_active_positions",
        "active_instrument_surface",
        "secondary_context",
        "market_context",
        "rollup_integrity",
    }
    assert surface["runtime_readiness"]["paper_enabled"] is True
    assert surface["runtime_readiness"]["entries_enabled"] is True
    assert surface["operator_metrics_portfolio"]["daily_realized_pnl"] == "10.0"
    assert surface["operator_metrics_portfolio"]["daily_unrealized_pnl"] == "2.0"
    assert surface["operator_metrics_portfolio"]["daily_net_pnl"] == "12.0"
    assert surface["operator_metrics_portfolio"]["active_lanes_count"] == 3
    assert surface["operator_metrics_portfolio"]["realized_pnl_horizons"]["today"]["value"] == "10.0"
    assert surface["operator_metrics_portfolio"]["realized_pnl_horizons"]["lifetime"]["available"] is False
    assert surface["active_instrument_surface"]["active_instruments_count"] == 2
    assert surface["active_instrument_surface"]["classification_counts"] == {
        "approved_quant": 1,
        "admitted_paper": 1,
        "temporary_paper": 0,
        "canary": 1,
    }
    assert surface["active_instrument_surface"]["classification_row_counts"] == {
        "approved_quant": 2,
        "admitted_paper": 1,
        "temporary_paper": 0,
        "canary": 1,
    }

    lane_rows = surface["active_instrument_surface"]["rows"]
    assert any(row["classification_tag"] == "approved_quant" and row["instrument"] == "GC" for row in lane_rows)
    assert any(row["classification_tag"] == "approved_quant" and row["instrument"] == "MGC" for row in lane_rows)
    assert any(row["display_name"] == "GC / usLatePauseResumeLongTurn" for row in lane_rows)
    assert any(row["display_name"] == "GC Canary Once" for row in lane_rows)

    instrument_rows = {row["instrument"]: row for row in surface["operator_metrics_by_instrument"]["rows"]}
    assert instrument_rows["GC"]["realized_pnl"] == "10.0"
    assert instrument_rows["GC"]["unrealized_pnl"] == "2.0"
    assert instrument_rows["GC"]["net_pnl"] == "12.0"
    assert instrument_rows["GC"]["realized_pnl_horizons"]["today"]["value"] == "10.0"
    assert instrument_rows["GC"]["active_lane_count"] == 3
    assert instrument_rows["MGC"]["net_pnl"] == "0"
    assert surface["current_active_positions"]["open_position_count"] == 1
    assert surface["current_active_positions"]["rows"][0]["instrument"] == "GC"
    assert surface["rollup_integrity"]["realized_pnl_reconciliation"]["reconciles"] is True
    assert surface["rollup_integrity"]["unrealized_pnl_reconciliation"]["reconciles"] is True
    assert surface["rollup_integrity"]["net_pnl_reconciliation"]["reconciles"] is True
    assert surface["rollup_integrity"]["position_count_reconciliation"]["reconciles"] is True

    summary = {row["label"]: row["value"] for row in surface["lane_universe"]["cards"]}
    assert summary["Approved Quant"] == "1"
    assert summary["Admitted Paper"] == "1"
    assert summary["Canary"] == "1"

    secondary_context = surface["secondary_context"]
    assert secondary_context["status_counts"]["stale"] >= 1
    assert secondary_context["mgc_key_quote"]["status"] == "stale"
    assert secondary_context["vix"]["status"] == "unavailable"
    assert secondary_context["major_equity_indices"]["status"] == "unavailable"
    assert secondary_context["treasury_curve_current"]["status"] == "unavailable"
    assert secondary_context["treasury_curve_prior"]["status"] == "unavailable"
    assert len(secondary_context["items"]) == 5
    assert secondary_context["items"][0]["label"] == "MGC Key Quote"
    assert secondary_context["items"][1]["label"] == "Major Equity Indices"
    assert secondary_context["items"][3]["label"] == "Treasury Curve Current"
    assert secondary_context["items"][4]["label"] == "Treasury Curve Prior"


def test_operator_surface_hardens_context_semantics_for_thin_comparisons_and_invalid_prior() -> None:
    surface = build_operator_surface(
        generated_at="2026-03-21T12:00:00+00:00",
        global_payload={
            "paper_label": "RUNNING",
            "current_session_date": "2026-03-21",
            "market_data_label": "LIVE",
            "runtime_health_label": "HEALTHY",
            "fault_state": "CLEAR",
            "desk_clean_label": "DESK CLEAN",
            "paper_run_ready_label": "READY",
            "last_processed_bar_timestamp": "2026-03-21T10:05:00-04:00",
            "last_update_timestamp": "2026-03-21T10:05:05-04:00",
        },
        auth_status={"runtime_ready": True},
        paper={
            "running": True,
            "status": {
                "entries_enabled": True,
                "operator_halt": False,
                "freshness": "FRESH",
                "last_processed_bar_end_ts": "2026-03-21T10:05:00-04:00",
                "last_update_ts": "2026-03-21T10:05:05-04:00",
            },
            "readiness": {"runtime_phase": "RUNNING", "operator_halt": False, "entries_enabled": True},
            "exceptions": {"exceptions": []},
            "performance": {"session_date": "2026-03-21", "current_session_date": "2026-03-21", "realized_pnl": "0", "unrealized_pnl": "0", "total_pnl": "0"},
            "session_shape": {"max_intraday_drawdown": "0"},
            "position": {"latest_bar_close": "3010.5"},
            "approved_models": {"details_by_branch": {}, "rows": []},
            "non_approved_lanes": {"rows": []},
        },
        approved_quant_baselines={"rows": []},
        market_context={
            "feed_label": "INDEX FEED LIVE",
            "feed_state": "LIVE",
            "updated_at": "2026-03-21T10:06:00-04:00",
            "note": "Direct Schwab /quotes fetch.",
            "symbols": [
                {
                    "label": "DJIA",
                    "current_value": "45577.47",
                    "percent_change": "0.00%",
                    "value_state": "LIVE",
                    "field_states": {"percent_change": {"available": True, "source_field": "netPercentChange"}},
                },
                {
                    "label": "SPX",
                    "current_value": "6506.48",
                    "percent_change": "0.00%",
                    "value_state": "LIVE",
                    "field_states": {"percent_change": {"available": True, "source_field": "netPercentChange"}},
                },
                {
                    "label": "NDX",
                    "current_value": "23898.15",
                    "percent_change": "0.00%",
                    "value_state": "LIVE",
                    "field_states": {"percent_change": {"available": True, "source_field": "netPercentChange"}},
                },
                {
                    "label": "RUT",
                    "current_value": "2438.45",
                    "percent_change": "0.00%",
                    "value_state": "LIVE",
                    "field_states": {"percent_change": {"available": True, "source_field": "netPercentChange"}},
                },
                {
                    "label": "VIX",
                    "current_value": "26.78",
                    "percent_change": "0.00%",
                    "value_state": "LIVE",
                    "field_states": {"percent_change": {"available": True, "source_field": "netPercentChange"}},
                },
            ],
        },
        treasury_curve={
            "feed_label": "TREASURY LIVE",
            "feed_state": "LIVE",
            "updated_at": "2026-03-21T10:06:00-04:00",
            "coverage_note": "Direct Schwab /quotes fetch.",
            "rows": [
                {
                    "tenor": "3M",
                    "current_yield": "3.618",
                    "prior_yield": "3.618",
                    "current_state": "LIVE",
                },
                {
                    "tenor": "10Y",
                    "current_yield": "4.391",
                    "prior_yield": "4.391",
                    "current_state": "LIVE",
                },
            ],
        },
    )

    secondary = surface["secondary_context"]
    assert secondary["major_equity_indices"]["status"] == "live_thin_comparison"
    assert secondary["major_equity_indices"]["reference_value"] is None
    assert secondary["major_equity_indices"]["reason_code"] == "thin_provider_comparison"
    assert secondary["vix"]["status"] == "live_thin_comparison"
    assert secondary["vix"]["reference_value"] is None
    assert secondary["treasury_curve_current"]["status"] == "live"
    assert secondary["treasury_curve_prior"]["status"] == "live_no_valid_prior"
    assert secondary["treasury_curve_prior"]["value"] == "No valid prior snapshot"
    assert secondary["treasury_curve_prior"]["reference_value"] is None
    assert secondary["status_counts"]["live"] == 2
    assert secondary["status_counts"]["live_thin_comparison"] == 2
    assert secondary["status_counts"]["live_no_valid_prior"] == 1
    assert "thin_cmp=2" in secondary["status_line"]
    assert "no_valid_prior=1" in secondary["status_line"]


def test_operator_surface_marks_experimental_canary_rows_without_hiding_canary_counts() -> None:
    surface = build_operator_surface(
        generated_at="2026-03-23T23:00:00+00:00",
        global_payload={
            "paper_label": "RUNNING",
            "current_session_date": "2026-03-23",
            "market_data_label": "LIVE",
            "runtime_health_label": "HEALTHY",
            "fault_state": "CLEAR",
            "desk_clean_label": "DESK CLEAN",
            "paper_run_ready_label": "READY",
            "last_processed_bar_timestamp": "2026-03-23T19:45:00-04:00",
            "last_update_timestamp": "2026-03-23T19:45:05-04:00",
        },
        auth_status={"runtime_ready": True},
        paper={
            "running": True,
            "status": {
                "entries_enabled": True,
                "operator_halt": False,
                "last_processed_bar_end_ts": "2026-03-23T19:45:00-04:00",
                "last_update_ts": "2026-03-23T19:45:05-04:00",
            },
            "readiness": {
                "runtime_phase": "RUNNING",
                "operator_halt": False,
                "entries_enabled": True,
            },
            "exceptions": {"exceptions": []},
            "performance": {
                "session_date": "2026-03-23",
                "current_session_date": "2026-03-23",
                "realized_pnl": "0",
                "unrealized_pnl": "0",
                "total_pnl": "0",
                "session_metrics": {"open_trade_count": 0},
            },
            "session_shape": {"max_intraday_drawdown": "0"},
            "position": {"side": "FLAT", "latest_bar_close": "0"},
            "approved_models": {"details_by_branch": {}, "rows": []},
            "non_approved_lanes": {
                "rows": [
                    {
                        "lane_id": "atpe_long_medium_high_canary",
                        "display_name": "ATPE Long Medium+High Canary",
                        "instrument": "MES/MNQ",
                        "state": "ENABLED",
                        "experimental_status": "experimental_canary",
                        "paper_only": True,
                        "side": "LONG",
                        "session_restriction": "ALL",
                        "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
                        "latest_activity_timestamp": "2026-03-23T19:45:00-04:00",
                        "last_update_timestamp": "2026-03-23T19:45:00-04:00",
                        "fired": True,
                        "recent_signal_count": 12,
                        "recent_event_count": 3,
                        "kill_switch_active": False,
                        "metrics_net_pnl_cash": "18.75",
                        "metrics_max_drawdown": "42.5",
                        "allow_block_override_summary": {
                            "allowed": 9,
                            "blocked": 3,
                            "top_override_reason": "paper_only_experimental_canary",
                            "label": "allowed=9 blocked=3 override=paper_only_experimental_canary",
                        },
                        "risk_state": "OK",
                    }
                ]
            },
        },
        approved_quant_baselines={"rows": []},
        market_context={"feed_label": "INDEX FEED LIVE", "feed_state": "LIVE", "symbols": [], "note": "ok"},
        treasury_curve={"feed_label": "TREASURY LIVE", "feed_state": "LIVE", "rows": [], "coverage_note": "ok"},
    )

    row = surface["active_instrument_surface"]["rows"][0]
    assert row["classification"] == "Experimental Paper Strategy"
    assert row["classification_tag"] == "temporary_paper"
    assert row["active_exit"] == "paper_only.MEDIUM_HIGH_ONLY"
    assert row["current_net_pnl"] == "18.75"
    assert row["current_session_max_drawdown"] == "42.5"
    assert row["family"] == "Temporary Paper / MEDIUM_HIGH_ONLY"
    assert "Paper Only | Experimental | Non-Approved" in row["warning_summary"]
    assert surface["active_instrument_surface"]["classification_counts"]["temporary_paper"] == 1
    assert surface["active_instrument_surface"]["classification_row_counts"]["temporary_paper"] == 1
