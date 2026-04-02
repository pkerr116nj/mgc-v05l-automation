from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

import mgc_v05l.app.approved_quant_lanes.probation as probation_module
from mgc_v05l.app.approved_quant_lanes.evaluator import _resolve_lane_exit, lane_rejection_reason
from mgc_v05l.app.approved_quant_lanes.probation import _build_lane_status
from mgc_v05l.app.approved_quant_lanes.runtime_boundary import (
    APPROVED_QUANT_BASELINE_RUNTIME_CONTRACT_VERSION,
    approved_quant_research_dependencies,
)
from mgc_v05l.app.approved_quant_lanes.specs import (
    BREAKOUT_METALS_US_UNKNOWN_CONTINUATION,
    FAILED_MOVE_NO_US_REVERSAL_SHORT,
    approved_quant_lane_scope_fingerprint,
    approved_quant_lane_specs,
)
from mgc_v05l.domain.models import Bar
from mgc_v05l.research.quant_futures import _FrameSeries


def _trade(
    *,
    lane_id: str,
    lane_name: str,
    symbol: str,
    session_label: str,
    index: int,
    gross_r: float,
) -> dict[str, object]:
    ts = datetime(2026, 3, 20, 14, 0, tzinfo=UTC) + timedelta(minutes=5 * index)
    return {
        "lane_id": lane_id,
        "lane_name": lane_name,
        "variant_id": lane_id,
        "symbol": symbol,
        "session_label": session_label,
        "signal_timestamp": ts.isoformat(),
        "entry_timestamp": ts.isoformat(),
        "exit_timestamp": (ts + timedelta(minutes=10)).isoformat(),
        "direction": "LONG" if "breakout" in lane_id else "SHORT",
        "entry_price": 100.0,
        "stop_price": 99.0,
        "target_price": 101.5,
        "exit_price": 100.5,
        "exit_reason": "time",
        "holding_bars": 2,
        "gross_r": gross_r,
        "net_r_cost_020": gross_r - 0.20,
        "net_r_cost_025": gross_r - 0.25,
        "mae_r": -0.3,
        "mfe_r": 0.8,
        "bars_to_mfe": 1,
        "bars_to_mae": 1,
    }


def test_approved_quant_lane_specs_preserve_authoritative_scope() -> None:
    specs = {spec.lane_id: spec for spec in approved_quant_lane_specs()}
    assert tuple(specs["phase2c.breakout.metals_only.us_unknown.baseline"].symbols) == ("GC", "MGC", "HG", "PL")
    assert tuple(specs["phase2c.breakout.metals_only.us_unknown.baseline"].allowed_sessions) == ("US", "UNKNOWN")
    assert tuple(specs["phase2c.breakout.metals_only.us_unknown.baseline"].excluded_sessions) == ("ASIA", "LONDON")
    assert specs["phase2c.breakout.metals_only.us_unknown.baseline"].hold_bars == 24
    assert specs["phase2c.breakout.metals_only.us_unknown.baseline"].target_r is None
    assert specs["phase2c.breakout.metals_only.us_unknown.baseline"].exit_style == "time_stop_only"
    assert tuple(specs["phase2c.breakout.metals_only.us_unknown.baseline"].permanent_exclusions) == (
        "6J",
        "LONDON",
        "broad_fx_metals_breakout",
        "cross_universe_breakout",
    )
    assert tuple(specs["phase2c.failed.core4_plus_qc.no_us.baseline"].symbols) == ("CL", "ES", "6E", "6J", "QC")
    assert tuple(specs["phase2c.failed.core4_plus_qc.no_us.baseline"].allowed_sessions) == ("ASIA", "LONDON", "UNKNOWN")
    assert tuple(specs["phase2c.failed.core4_plus_qc.no_us.baseline"].excluded_sessions) == ("US",)
    assert specs["phase2c.failed.core4_plus_qc.no_us.baseline"].target_r == 1.5
    assert specs["phase2c.failed.core4_plus_qc.no_us.baseline"].exit_style == "target_stop_time_plus_structure"
    assert specs["phase2c.failed.core4_plus_qc.no_us.baseline"].structural_invalidation_r == 0.45
    assert tuple(specs["phase2c.failed.core4_plus_qc.no_us.baseline"].permanent_exclusions) == (
        "US",
        "ZT",
        "soft_reversal_score_required",
        "broad_failed_move_family",
    )


def test_approved_quant_lane_scope_fingerprints_fail_loudly_on_scope_drift() -> None:
    specs = {spec.lane_id: spec for spec in approved_quant_lane_specs()}
    assert approved_quant_lane_scope_fingerprint(specs["phase2c.breakout.metals_only.us_unknown.baseline"]) == (
        "c44dd8343e6a0052822c5c183b2e76e26d026a59d82c8a0d7174b11aa0c4c218"
    )
    assert approved_quant_lane_scope_fingerprint(specs["phase2c.failed.core4_plus_qc.no_us.baseline"]) == (
        "b76dd8bdd067ddc2aa07a764bc3dd4c046dfe22fb2b11d0527b57638a5619e30"
    )


def test_approved_quant_runtime_boundary_declares_research_dependencies() -> None:
    dependencies = {dependency.dependency_id: dependency for dependency in approved_quant_research_dependencies()}
    assert APPROVED_QUANT_BASELINE_RUNTIME_CONTRACT_VERSION == "approved_quant_baseline_runtime_v1"
    assert dependencies["phase2a_symbol_store_builder"].import_path == (
        "mgc_v05l.research.quant_futures_phase2a._build_symbol_store"
    )
    assert dependencies["phase2a_symbol_store_builder"].required_parameters == (
        "database_path",
        "execution_timeframe",
        "symbols",
    )
    assert dependencies["quant_exit_resolver"].import_path == "mgc_v05l.research.quant_futures._resolve_exit"
    assert dependencies["quant_frame_series"].import_path == "mgc_v05l.research.quant_futures._FrameSeries"


def test_lane_rejection_reason_uses_only_approved_baseline_rules() -> None:
    breakout_reason = lane_rejection_reason(
        spec=BREAKOUT_METALS_US_UNKNOWN_CONTINUATION,
        session_label="LONDON",
        feature={
            "regime_up": True,
            "compression_60": 0.5,
            "compression_5": 0.5,
            "breakout_up": 0.5,
            "close_pos": 0.8,
            "slope_60": 0.4,
        },
    )
    failed_reason = lane_rejection_reason(
        spec=FAILED_MOVE_NO_US_REVERSAL_SHORT,
        session_label="ASIA",
        feature={
            "failed_breakout_short": False,
            "dist_240": 1.5,
            "close_pos": 0.2,
            "body_r": 0.5,
        },
    )
    assert breakout_reason == "session_excluded"
    assert failed_reason == "failed_breakout_missing"


def test_breakout_lane_status_suspends_when_breadth_and_concentration_fail() -> None:
    trades = [
        _trade(
            lane_id=BREAKOUT_METALS_US_UNKNOWN_CONTINUATION.lane_id,
            lane_name=BREAKOUT_METALS_US_UNKNOWN_CONTINUATION.lane_name,
            symbol="GC",
            session_label="UNKNOWN",
            index=index,
            gross_r=0.35,
        )
        for index in range(30)
    ]
    status = _build_lane_status(
        spec=BREAKOUT_METALS_US_UNKNOWN_CONTINUATION,
        trades=trades,
        generated_at="2026-03-20T00:00:00+00:00",
        approval_date="2026-03-20",
        approval_reference={"expectancy_net_020_r": 0.15, "expectancy_net_025_r": 0.10},
    )
    assert status["probation_status"] == "suspend"
    assert status["baseline_status"] == "suspended"


def test_failed_lane_status_flags_core_integrity_when_qc_carries_all_edge() -> None:
    trades = [
        _trade(
            lane_id=FAILED_MOVE_NO_US_REVERSAL_SHORT.lane_id,
            lane_name=FAILED_MOVE_NO_US_REVERSAL_SHORT.lane_name,
            symbol="QC",
            session_label="LONDON",
            index=index,
            gross_r=0.45,
        )
        for index in range(25)
    ]
    status = _build_lane_status(
        spec=FAILED_MOVE_NO_US_REVERSAL_SHORT,
        trades=trades,
        generated_at="2026-03-20T00:00:00+00:00",
        approval_date="2026-03-20",
        approval_reference={"expectancy_net_020_r": 0.20, "expectancy_net_025_r": 0.15},
    )
    assert status["core_integrity_flag"] is False
    assert status["probation_status"] == "suspend"


def test_breakout_exit_is_now_time_stop_only_without_target() -> None:
    execution = _FrameSeries.from_bars(
        [
            _bar_model(index=0, open_=100.0, high=100.2, low=99.8, close=100.0),
            _bar_model(index=1, open_=100.0, high=101.9, low=99.9, close=101.2),
            _bar_model(index=2, open_=101.2, high=102.0, low=101.0, close=101.5),
        ]
    )
    exit_index, exit_price, exit_reason, _, target_price = _resolve_lane_exit(
        spec=BREAKOUT_METALS_US_UNKNOWN_CONTINUATION,
        execution=execution,
        features=[{"session_label": "US"} for _ in execution.bars],
        entry_index=1,
        entry_price=100.0,
        risk=1.0,
    )
    assert target_price is None
    assert exit_index == 2
    assert exit_reason == "time_exit"
    assert exit_price == execution.closes[2]


def test_failed_move_exit_uses_structural_invalidation_refinement() -> None:
    execution = _FrameSeries.from_bars(
        [
            _bar_model(index=0, open_=100.0, high=100.2, low=99.8, close=100.0),
            _bar_model(index=1, open_=100.0, high=100.3, low=99.7, close=99.8),
            _bar_model(index=2, open_=99.8, high=100.5, low=99.6, close=100.46),
            _bar_model(index=3, open_=100.46, high=100.7, low=100.1, close=100.4),
        ]
    )
    exit_index, exit_price, exit_reason, _, target_price = _resolve_lane_exit(
        spec=FAILED_MOVE_NO_US_REVERSAL_SHORT,
        execution=execution,
        features=[{"session_label": "LONDON"} for _ in execution.bars],
        entry_index=1,
        entry_price=100.0,
        risk=1.0,
    )
    assert target_price == 98.5
    assert exit_index == 2
    assert exit_reason == "structural_invalidation"
    assert exit_price == execution.closes[2]


def test_probation_snapshot_includes_operator_legible_status_and_scope(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def fake_build_symbol_store_for_approved_lanes(**_: object) -> dict[str, dict[str, object]]:
        return {}

    def fake_evaluate_approved_lane(*, spec, symbol_store):  # type: ignore[no-untyped-def]
        del symbol_store
        trades = []
        if spec.lane_id == BREAKOUT_METALS_US_UNKNOWN_CONTINUATION.lane_id:
            trades = []
            for symbol_index, symbol in enumerate(("GC", "MGC", "HG", "PL")):
                trades.extend(
                    _trade(
                        lane_id=spec.lane_id,
                        lane_name=spec.lane_name,
                        symbol=symbol,
                        session_label="US",
                        index=(symbol_index * 10) + index,
                        gross_r=0.45,
                    )
                    for index in range(3)
                )
        else:
            trades = []
            for symbol_index, symbol in enumerate(("CL", "ES", "6E", "6J", "QC")):
                trades.extend(
                    _trade(
                        lane_id=spec.lane_id,
                        lane_name=spec.lane_name,
                        symbol=symbol,
                        session_label="LONDON",
                        index=(symbol_index * 10) + index,
                        gross_r=0.40,
                    )
                    for index in range(3)
                )
        signals = [
            {
                "lane_id": trade["lane_id"],
                "lane_name": trade["lane_name"],
                "variant_id": trade["variant_id"],
                "symbol": trade["symbol"],
                "session_label": trade["session_label"],
                "signal_timestamp": trade["signal_timestamp"],
                "entry_timestamp_planned": trade["entry_timestamp"],
                "direction": trade["direction"],
                "signal_passed_flag": True,
                "rejection_reason_code": None,
                "rule_snapshot": {"session_label": trade["session_label"]},
            }
            for trade in trades
        ]
        return {
            "spec": {"lane_id": spec.lane_id},
            "signals": signals,
            "trades": trades,
            "rejected_reason_counts": {},
        }

    monkeypatch.setattr(probation_module, "build_symbol_store_for_approved_lanes", fake_build_symbol_store_for_approved_lanes)
    monkeypatch.setattr(probation_module, "evaluate_approved_lane", fake_evaluate_approved_lane)

    artifacts = probation_module.run_approved_quant_baseline_probation(
        database_path=tmp_path / "dummy.sqlite3",
        output_dir=tmp_path / "probation",
    )
    snapshot = json.loads(artifacts.snapshot_json_path.read_text(encoding="utf-8"))
    current_status = json.loads(artifacts.current_status_json_path.read_text(encoding="utf-8"))

    assert snapshot["runtime_contract_version"] == APPROVED_QUANT_BASELINE_RUNTIME_CONTRACT_VERSION
    assert snapshot["boundary"]["adapter_module"] == "mgc_v05l.app.approved_quant_lanes.runtime_boundary"
    assert snapshot["rows"][0]["lane_classification"] == "approved_baseline_lane"
    assert snapshot["rows"][0]["promotion_state"] == "operator_baseline_candidate"
    assert snapshot["rows"][0]["post_cost_monitoring_read"]["label"] == "stable_positive_post_cost"
    assert snapshot["rows"][0]["active_exit_logic"]["exit_style"] == "time_stop_only"
    assert snapshot["rows"][0]["approved_scope"]["symbols"] == ["GC", "MGC", "HG", "PL"]
    assert snapshot["rows"][0]["approved_scope"]["exit_style"] == "time_stop_only"
    assert snapshot["rows"][0]["approved_scope"]["target_r"] is None
    assert "approval_baseline_reference" in snapshot["rows"][0]
    assert "drift_vs_approval_baseline_cost_020" in snapshot["rows"][0]
    assert "unknown_session_warning" in snapshot["rows"][0]
    assert "APPROVED BASELINE" in snapshot["rows"][0]["operator_status_line"]
    assert current_status["freeze_mode"] == "logic_frozen_monitoring_only"
    assert current_status["approved_lane_count"] == 2
    assert current_status["first_formal_review_checkpoint"]["cadence"] == "every_5_trading_days_during_probation"
    assert current_status["lanes"][0]["active_exit_logic"]["exit_style"] == "time_stop_only"

    markdown = artifacts.snapshot_markdown_path.read_text(encoding="utf-8")
    assert "Runtime contract: approved_quant_baseline_runtime_v1" in markdown
    assert "APPROVED BASELINE" in markdown
    current_status_markdown = artifacts.current_status_markdown_path.read_text(encoding="utf-8")
    assert "Current Active Baseline Status" in current_status_markdown
    assert "First Formal Review Checkpoint" in current_status_markdown

    breakout_daily_files = sorted((tmp_path / "probation" / "lanes" / BREAKOUT_METALS_US_UNKNOWN_CONTINUATION.lane_id / "daily").glob("*.json"))
    breakout_weekly_files = sorted((tmp_path / "probation" / "lanes" / BREAKOUT_METALS_US_UNKNOWN_CONTINUATION.lane_id / "weekly").glob("*.json"))
    assert breakout_daily_files
    assert breakout_weekly_files
    daily_payload = json.loads(breakout_daily_files[0].read_text(encoding="utf-8"))
    weekly_payload = json.loads(breakout_weekly_files[0].read_text(encoding="utf-8"))
    assert "symbol_attribution" in daily_payload
    assert "session_attribution" in daily_payload
    assert "approval_baseline_reference" in daily_payload
    assert "drift_vs_approval_baseline_cost_025" in daily_payload
    assert "unknown_session_warning" in daily_payload
    assert "warning_flags" in daily_payload
    assert "slice_weakness_flag" in weekly_payload
    assert "active_exit_logic" in weekly_payload


def _bar_model(*, index: int, open_: float, high: float, low: float, close: float) -> Bar:
    start = datetime(2026, 3, 20, 14, 0, tzinfo=UTC) + timedelta(minutes=5 * index)
    end = start + timedelta(minutes=5)
    return Bar(
        bar_id=f"bar-{index}",
        symbol="TEST",
        timeframe="5m",
        start_ts=start,
        end_ts=end,
        open=Decimal(str(open_)),
        high=Decimal(str(high)),
        low=Decimal(str(low)),
        close=Decimal(str(close)),
        volume=100,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=True,
        session_allowed=True,
    )
