from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution.ibkr_paper_strategy_porting import (
    IbkrPaperStrategyPortingConfig,
    lane_submit_bridge_adapter,
    run_ibkr_paper_strategy_porting,
    submit_capable_lane_adapters,
    write_ibkr_paper_strategy_porting_artifacts,
)


def _config(tmp_path: Path) -> IbkrPaperStrategyPortingConfig:
    return IbkrPaperStrategyPortingConfig(
        repo_root=tmp_path,
        output_dir=Path("outputs") / "reports" / "ibkr_strategy_porting",
        dashboard_snapshot_path=Path("outputs") / "operator_dashboard" / "dashboard_api_snapshot.json",
        signal_audit_path=Path("outputs") / "operator_dashboard" / "paper_signal_intent_fill_audit_snapshot.json",
        strategy_performance_path=Path("outputs") / "operator_dashboard" / "paper_strategy_performance_snapshot.json",
        ledger_path=Path("var") / "paper_strategy_position_ledger.json",
        bridge_audit_path=Path("outputs") / "reports" / "ibkr_strategy_porting" / "ibkr_paper_strategy_bridge_porting_audit.jsonl",
    )


def _write_monitor(tmp_path: Path, **overrides: object) -> None:
    payload = {
        "classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
        "monitor_running": True,
        "health_classification": "HEALTHY",
        "stale": False,
        "submit_allowed": True,
        "broker_position_quantity": 1.0,
        "ledger_position_quantity": 1.0,
        "open_order_count": 0,
        "last_successful_broker_refresh": "2026-04-28T19:38:25.796168+00:00",
    }
    payload.update(overrides)
    path = tmp_path / "var" / "paper_strategy_monitor_runtime_status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_ledger(tmp_path: Path) -> None:
    path = tmp_path / "var" / "paper_strategy_position_ledger.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "positions": [
                    {
                        "strategy_id": "ATP_COMPANION_V1_ASIA_US",
                        "symbol": "MGC",
                        "quantity": 1.0,
                        "side": "LONG",
                        "average_entry_price": 4586.7,
                    }
                ],
                "orphan_positions": [],
            }
        ),
        encoding="utf-8",
    )


def _write_dashboard(tmp_path: Path) -> None:
    path = tmp_path / "outputs" / "operator_dashboard" / "dashboard_api_snapshot.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"paper": {"status": {"stale": False}, "readiness": {"current_detected_session": "US_LATE"}}}), encoding="utf-8")


def _write_signal_audit(tmp_path: Path) -> None:
    path = tmp_path / "outputs" / "operator_dashboard" / "paper_signal_intent_fill_audit_snapshot.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "rows": [
            {
                "id": "asia_london_participation_core_v1__GC",
                "lane_id": "gc_1x_asia_london_participation__asia_london_long_v5",
                "instrument": "GC",
                "family": "asia_london_participation_core_v1",
                "current_strategy_status": "READY",
                "entries_enabled": True,
                "eligible_now": False,
                "audit_verdict": "INSUFFICIENT_HISTORY",
                "last_actionable_signal_family": None,
                "last_actionable_signal_timestamp": None,
                "last_recent_long_setup": False,
                "last_recent_short_setup": False,
                "last_intent_type": None,
                "last_fill_timestamp": None,
            },
            {
                "id": "asia_london_participation_core_v1__MGC",
                "lane_id": "mgc_1x_asia_london_participation__asia_london_long_v5",
                "instrument": "MGC",
                "family": "asia_london_participation_core_v1",
                "current_strategy_status": "READY",
                "entries_enabled": True,
                "eligible_now": False,
                "audit_verdict": "INSUFFICIENT_HISTORY",
                "last_actionable_signal_family": None,
                "last_actionable_signal_timestamp": None,
                "last_recent_long_setup": False,
                "last_recent_short_setup": False,
                "last_intent_type": None,
                "last_fill_timestamp": None,
            },
            {
                "id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
                "lane_id": "gc_1x_all_lanes__london_early_long",
                "instrument": "GC",
                "family": "gold_forced_session_baseline_v2",
                "current_strategy_status": "READY",
                "entries_enabled": True,
                "eligible_now": False,
                "audit_verdict": "EXIT_RECENTLY_FILLED",
                "last_actionable_signal_family": "londonEarlyLongV5",
                "last_actionable_signal_timestamp": "2026-04-29T03:04:00-04:00",
                "last_recent_long_setup": False,
                "last_recent_short_setup": False,
                "last_intent_type": "SELL_TO_CLOSE",
                "last_fill_timestamp": "2026-04-29T03:08:00-04:00",
            },
            {
                "id": "ATP_COMPANION_V1_ASIA_US",
                "lane_id": "atp_companion_v1_asia_us",
                "instrument": "MGC",
                "family": "active_trend_participation_engine",
                "current_strategy_status": "READY",
                "entries_enabled": True,
                "eligible_now": False,
                "audit_verdict": "FILLED",
                "last_actionable_signal_family": "atp_pullback_long",
                "last_actionable_signal_timestamp": "2026-04-28T14:30:00-04:00",
                "last_recent_long_setup": False,
                "last_recent_short_setup": False,
                "last_intent_type": "BUY_TO_OPEN",
                "last_fill_timestamp": "2026-04-28T14:34:38-04:00",
            },
            {
                "id": "index_futures_ny_intraday_forced_core_v2__NQ",
                "lane_id": "nq_1x_ny_early_core__us_late_long",
                "instrument": "NQ",
                "family": "index_futures_ny_intraday_forced_core_v2",
                "current_strategy_status": "READY",
                "entries_enabled": True,
                "eligible_now": True,
                "audit_verdict": "READY",
                "last_actionable_signal_family": "nyLateLong",
                "last_actionable_signal_timestamp": "2026-04-28T14:00:00-04:00",
                "last_recent_long_setup": True,
                "last_recent_short_setup": False,
                "last_intent_type": None,
                "last_fill_timestamp": None,
            },
        ]
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_strategy_performance(tmp_path: Path) -> None:
    path = tmp_path / "outputs" / "operator_dashboard" / "paper_strategy_performance_snapshot.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "rows": [
            {
                "lane_id": "gc_1x_asia_london_participation__asia_london_long_v5",
                "instrument": "GC",
                "strategy_family": "asia_london_participation_core_v1",
                "standalone_strategy_id": "asia_london_participation_core_v1__GC",
                "position_side": "FLAT",
                "status": "READY",
            },
            {
                "lane_id": "mgc_1x_asia_london_participation__asia_london_long_v5",
                "instrument": "MGC",
                "strategy_family": "asia_london_participation_core_v1",
                "standalone_strategy_id": "asia_london_participation_core_v1__MGC",
                "position_side": "FLAT",
                "status": "READY",
            },
            {
                "lane_id": "gc_1x_all_lanes__london_early_long",
                "instrument": "GC",
                "strategy_family": "gold_forced_session_baseline_v2",
                "standalone_strategy_id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
                "position_side": "FLAT",
                "status": "READY",
            },
            {
                "lane_id": "atp_companion_v1_asia_us",
                "instrument": "MGC",
                "strategy_family": "active_trend_participation_engine",
                "standalone_strategy_id": "ATP_COMPANION_V1_ASIA_US",
                "position_side": "FLAT",
                "status": "READY",
            },
            {
                "lane_id": "nq_1x_ny_early_core__us_late_long",
                "instrument": "NQ",
                "strategy_family": "index_futures_ny_intraday_forced_core_v2",
                "standalone_strategy_id": "index_futures_ny_intraday_forced_core_v2__NQ",
                "position_side": "FLAT",
                "status": "READY",
            },
        ]
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_restored_track_b_lanes_are_wired_for_ibkr_paper_submit() -> None:
    expected_targets = {
        "gc_1x_all_lanes__ny_early_short": "GC",
        "mgc_1x_all_lanes__asia_early_long": "MGC",
        "mgc_1x_all_lanes__asia_early_short": "MGC",
        "mgc_1x_all_lanes__london_early_long": "MGC",
        "mgc_1x_all_lanes__ny_early_short": "MGC",
        "mgc_1x_all_lanes__us_early_short": "MGC",
        "mgc_1x_all_lanes__us_midday_short": "MGC",
    }

    for lane_id, expected_symbol in expected_targets.items():
        adapter = lane_submit_bridge_adapter(lane_id=lane_id)

        assert adapter is not None
        assert adapter["current_order_destination"] == "ibkr_paper_bridge_submit_capable"
        assert adapter["source_instrument"] == expected_symbol
        assert adapter["bridge_execution_target"]["symbol"] == expected_symbol


def test_mgc_forced_session_us_early_lane_uses_explicit_migration_metadata() -> None:
    adapter = lane_submit_bridge_adapter(lane_id="mgc_1x_all_lanes__us_early_short")

    assert adapter is not None
    assert adapter["lane_id"] == "mgc_1x_all_lanes__us_early_short"
    assert adapter["source_instrument"] == "MGC"
    assert adapter["bridge_execution_target"]["symbol"] == "MGC"
    assert adapter["legacy_lane_id"] == "mgc_1x_all_lanes__ny_early_short"
    assert adapter["canonical_session"] == "US_EARLY"
    assert adapter["legacy_session"] == "NY_EARLY"
    assert adapter["lane_id_migration"] == {
        "canonical_lane_id": "mgc_1x_all_lanes__us_early_short",
        "legacy_lane_id": "mgc_1x_all_lanes__ny_early_short",
        "strategy_family": "gold_forced_session_baseline_v2",
        "package_id": "mgc_1x_all_lanes",
        "instrument": "MGC",
        "canonical_session": "US_EARLY",
        "legacy_session": "NY_EARLY",
        "source_variant": "nyEarlyShortV2",
        "source_artifact": (
            "outputs/reports/gc_mgc_forced_session_candidate_admission_archive_v4/"
            "mgc_1x_all_lanes.paper_package.json"
        ),
        "migration_reason": (
            "Candidate archive v4 renamed the NY_EARLY forced-session lane to the current "
            "gold segment label US_EARLY while retaining the nyEarlyShortV2 signal source."
        ),
        "review_status": "EXPLICIT_LANE_ID_MIGRATION_REVIEWED",
    }


def test_mgc_forced_session_lane_migration_does_not_enable_wildcards() -> None:
    adapters = submit_capable_lane_adapters()

    assert lane_submit_bridge_adapter(lane_id="mgc_1x_all_lanes__us_early_short") is not None
    assert lane_submit_bridge_adapter(lane_id="mgc_1x_all_lanes__us_early_long") is None
    assert lane_submit_bridge_adapter(lane_id="mgc_1x_all_lanes__london_early_short") is None
    assert lane_submit_bridge_adapter(lane_id="mgc_1x_all_lanes__us_midday_long") is None
    assert lane_submit_bridge_adapter(lane_id="mgc_10x_all_lanes_gc_equivalent__us_early_short") is None
    assert "mgc_1x_all_lanes__us_early_short" in adapters
    assert "mgc_1x_all_lanes__ny_early_short" in adapters


def test_batch1_active_evidence_lanes_use_validated_contract_bridge_targets() -> None:
    expected_targets = {
        "mgc_us_active_participation_long": ("MGC", "MGCQ6", 732156883, "20260827", "10", "0.1"),
        "gc_globex_active_participation_short": ("GC", "GCQ6", 732156872, "20260827", "100", "0.1"),
        "nq_london_open_active_participation_long": ("NQ", "NQU6", 770561204, "20260918", "20", "0.25"),
        "es_london_late_active_participation_short": ("ES", "ESU6", 649180671, "20260918", "50", "0.25"),
        "zt_us_active_participation_long": ("ZT", "ZTU6", 842590391, "20260930", "2000", "0.00390625"),
        "zf_globex_active_participation_short": ("ZF", "ZFU6", 842590380, "20260930", "1000", "0.0078125"),
        "zn_london_open_active_participation_long": ("ZN", "ZNU6", 840227361, "20260921", "1000", "0.015625"),
        "zb_london_late_active_participation_short": ("ZB", "ZBU6", 840227357, "20260921", "1000", "0.03125"),
    }

    for lane_id, (symbol, local_symbol, con_id, expiry, multiplier, min_tick) in expected_targets.items():
        adapter = lane_submit_bridge_adapter(lane_id=lane_id)

        assert adapter is not None
        assert adapter["current_order_destination"] == "ibkr_paper_bridge_submit_capable"
        assert adapter["source_instrument"] == symbol
        assert adapter["entry_execution_intent"] == "PARTICIPATE_NOW"
        assert adapter["entry_execution_policy"] == "MARKETABLE_LIMIT_FROM_RUNTIME_TAPE"
        assert adapter["entry_marketable_limit_offset_ticks"] == 4
        target = adapter["bridge_execution_target"]
        assert target["symbol"] == symbol
        assert target["local_symbol"] == local_symbol
        assert target["con_id"] == con_id
        assert target["expiry"] == expiry
        assert target["multiplier"] == multiplier
        assert target["min_tick"] == min_tick

    for lane_id in (
        "btc_us_active_participation_long",
        "mbt_london_open_active_participation_short",
        "eth_us_active_participation_long",
        "met_london_open_active_participation_short",
        "sol_us_active_participation_long",
        "msl_london_open_active_participation_short",
    ):
        assert lane_submit_bridge_adapter(lane_id=lane_id) is None


def test_dormant_paper_config_lanes_are_wired_for_ibkr_paper_submit() -> None:
    expected_targets = {
        "atp_companion_v1_asia_us": "MGC",
        "atp_companion_v1_asia_us_5m": "MGC",
        "atp_companion_v1_gc_asia_promotion_1_075r_favorable_only": "GC",
        "atp_companion_v1_gc_asia_promotion_1_075r_favorable_only_5m": "GC",
        "atp_companion_v1_gc_asia_us": "GC",
        "atp_companion_v1_gc_asia_us_5m": "GC",
        "atp_companion_v1_gc_asia_us_loosened_backfill_20260416": "GC",
        "atp_companion_v1_gc_asia_us_production_track": "GC",
        "atp_companion_v1_gc_asia_us_production_track_5m": "GC",
        "atp_companion_v1_gc_asia_us_production_track_selective_v1": "GC",
        "atp_companion_v1_gc_asia_us_selective_v1": "GC",
        "atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only": "MGC",
        "atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only_5m": "MGC",
        "atp_companion_v1_mgc_asia_promotion_edge_v1": "MGC",
        "atp_companion_v1_pl_asia_us": "PL",
        "atp_companion_v1_pl_asia_us_5m": "PL",
        "atp_companion_v1_pl_asia_us_risk_shaped_v1": "PL",
        "gc_1x_all_lanes__ny_late_short": "GC",
        "gc_asia_early_normal_breakout_retest_hold_long": "GC",
        "mgc_asia_early_normal_breakout_retest_hold_long": "MGC",
        "mgc_asia_early_pause_resume_short": "MGC",
        "mgc_us_late_pause_resume_long": "MGC",
        "pl_us_late_pause_resume_long": "PL",
    }

    for lane_id, expected_symbol in expected_targets.items():
        adapter = lane_submit_bridge_adapter(lane_id=lane_id)

        assert adapter is not None
        assert adapter["current_order_destination"] == "ibkr_paper_bridge_submit_capable"
        assert adapter["source_instrument"] == expected_symbol
        assert adapter["bridge_execution_target"]["symbol"] == expected_symbol
        assert adapter["bridge_execution_target"]


def test_builds_inventory_and_intent_rows_for_live_paper_lanes(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    artifacts = run_ibkr_paper_strategy_porting(config=_config(tmp_path))

    assert artifacts.classification == "IBKR_PAPER_STRATEGY_PORT_READY"
    assert artifacts.adapter_classification == "STRATEGY_INTENT_ADAPTER_READY"
    assert len(artifacts.inventory_rows) == 5
    atp = next(row for row in artifacts.inventory_rows if row["strategy_id"] == "atp_companion_v1_asia_us")
    assert atp["current_position_state"] == "LONG"
    assert atp["current_order_destination"] == "ibkr_paper_bridge_adopted_position"
    gc = next(row for row in artifacts.inventory_rows if row["strategy_id"] == "gc_1x_asia_london_participation__asia_london_long_v5")
    assert gc["current_order_destination"] == "ibkr_paper_bridge_submit_capable"
    assert gc["bridge_adapter_ready"] is True
    london = next(row for row in artifacts.inventory_rows if row["strategy_id"] == "gc_1x_all_lanes__london_early_long")
    assert london["current_order_destination"] == "ibkr_paper_bridge_submit_capable"
    assert london["bridge_adapter_ready"] is True
    mgc = next(row for row in artifacts.intent_rows if row["strategy_id"] == "mgc_1x_asia_london_participation__asia_london_long_v5")
    assert mgc["action"] == "NO_ACTION"
    assert mgc["bridge_submit_capable"] is True
    assert mgc["bridge_execution_target"]["symbol"] == "MGC"
    gc_intent = next(row for row in artifacts.intent_rows if row["strategy_id"] == "gc_1x_asia_london_participation__asia_london_long_v5")
    assert gc_intent["bridge_submit_capable"] is True
    assert gc_intent["bridge_execution_target"]["symbol"] == "GC"
    nq = next(row for row in artifacts.intent_rows if row["strategy_id"] == "nq_1x_ny_early_core__us_late_long")
    assert nq["bridge_submit_capable"] is True
    assert nq["bridge_execution_target"]["symbol"] == "NQ"
    assert nq["action"] == "BUY"
    assert nq["can_route_to_ibkr_now"] is True


def test_missing_full_size_lane_adapter_fails_closed(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    signal_path = tmp_path / "outputs" / "operator_dashboard" / "paper_signal_intent_fill_audit_snapshot.json"
    signal_payload = json.loads(signal_path.read_text(encoding="utf-8"))
    signal_payload["rows"].append(
        {
            "id": "index_futures_ny_intraday_forced_core_v2__ES_UNKNOWN",
            "lane_id": "es_1x_unknown_lane__us_midday_long",
            "instrument": "ES",
            "family": "index_futures_ny_intraday_forced_core_v2",
            "current_strategy_status": "READY",
            "entries_enabled": True,
            "eligible_now": True,
            "audit_verdict": "READY",
            "last_actionable_signal_family": "nyMiddayLong",
            "last_actionable_signal_timestamp": "2026-04-28T13:00:00-04:00",
            "last_recent_long_setup": True,
            "last_recent_short_setup": False,
            "last_intent_type": None,
            "last_fill_timestamp": None,
        }
    )
    signal_path.write_text(json.dumps(signal_payload), encoding="utf-8")
    performance_path = tmp_path / "outputs" / "operator_dashboard" / "paper_strategy_performance_snapshot.json"
    performance_payload = json.loads(performance_path.read_text(encoding="utf-8"))
    performance_payload["rows"].append(
        {
            "lane_id": "es_1x_unknown_lane__us_midday_long",
            "instrument": "ES",
            "strategy_family": "index_futures_ny_intraday_forced_core_v2",
            "standalone_strategy_id": "index_futures_ny_intraday_forced_core_v2__ES_UNKNOWN",
            "position_side": "FLAT",
            "status": "READY",
        }
    )
    performance_path.write_text(json.dumps(performance_payload), encoding="utf-8")

    artifacts = run_ibkr_paper_strategy_porting(config=_config(tmp_path))

    row = next(row for row in artifacts.intent_rows if row["strategy_id"] == "es_1x_unknown_lane__us_midday_long")
    assert row["bridge_submit_capable"] is False
    assert "lane_not_yet_submit_ported" in row["route_blockers"]
    assert "strategy_lane_not_yet_submit_ported" in row["route_blockers"]
    assert row["can_route_to_ibkr_now"] is False


def test_rates_scope_does_not_imply_strategy_or_lane_approval(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    signal_path = tmp_path / "outputs" / "operator_dashboard" / "paper_signal_intent_fill_audit_snapshot.json"
    signal_payload = json.loads(signal_path.read_text(encoding="utf-8"))
    signal_payload["rows"].append(
        {
            "id": "rates_research_only__ZN",
            "lane_id": "zn_1x_rates_research_only__us_midday_long",
            "instrument": "ZN",
            "family": "rates_research_only",
            "current_strategy_status": "READY",
            "entries_enabled": True,
            "eligible_now": True,
            "audit_verdict": "READY",
            "last_actionable_signal_family": "ratesResearchOnly",
            "last_actionable_signal_timestamp": "2026-04-28T13:00:00-04:00",
            "last_recent_long_setup": True,
            "last_recent_short_setup": False,
            "last_intent_type": None,
            "last_fill_timestamp": None,
        }
    )
    signal_path.write_text(json.dumps(signal_payload), encoding="utf-8")
    performance_path = tmp_path / "outputs" / "operator_dashboard" / "paper_strategy_performance_snapshot.json"
    performance_payload = json.loads(performance_path.read_text(encoding="utf-8"))
    performance_payload["rows"].append(
        {
            "lane_id": "zn_1x_rates_research_only__us_midday_long",
            "instrument": "ZN",
            "strategy_family": "rates_research_only",
            "standalone_strategy_id": "rates_research_only__ZN",
            "position_side": "FLAT",
            "status": "READY",
        }
    )
    performance_path.write_text(json.dumps(performance_payload), encoding="utf-8")

    artifacts = run_ibkr_paper_strategy_porting(config=_config(tmp_path))

    row = next(row for row in artifacts.intent_rows if row["strategy_id"] == "zn_1x_rates_research_only__us_midday_long")
    assert row["bridge_submit_capable"] is False
    assert row["bridge_execution_target"] == {}
    assert "lane_not_yet_submit_ported" in row["route_blockers"]
    assert "strategy_lane_not_yet_submit_ported" in row["route_blockers"]
    assert row["can_route_to_ibkr_now"] is False


def test_write_porting_artifacts(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    config = _config(tmp_path)
    artifacts = run_ibkr_paper_strategy_porting(config=config)
    write_ibkr_paper_strategy_porting_artifacts(config=config, artifacts=artifacts)

    output_dir = tmp_path / "outputs" / "reports" / "ibkr_strategy_porting"
    assert (output_dir / "ibkr_live_paper_strategy_inventory.csv").exists()
    assert (output_dir / "ibkr_strategy_intent_adapter_report.md").exists()
    assert (output_dir / "ibkr_paper_strategy_bridge_porting_report.json").exists()
    assert (output_dir / "ibkr_paper_strategy_bridge_porting_report.md").exists()
    assert (output_dir / "per_strategy_ibkr_paper_status.csv").exists()
    assert (output_dir / "per_strategy_order_intent_examples.jsonl").exists()
