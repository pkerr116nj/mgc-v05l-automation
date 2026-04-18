from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import json

import mgc_v05l.research.approved_quant_exit_research as exit_research
from mgc_v05l.app.approved_quant_lanes.specs import approved_quant_lane_specs
from mgc_v05l.domain.models import Bar
from mgc_v05l.research.quant_futures import _FrameSeries


def _bar(*, symbol: str, index: int, open_: float, high: float, low: float, close: float) -> Bar:
    start = datetime(2026, 3, 20, 12, 0, tzinfo=UTC) + timedelta(minutes=5 * index)
    end = start + timedelta(minutes=5)
    return Bar(
        bar_id=f"{symbol}-{index}",
        symbol=symbol,
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


def _execution_for_direction(symbol: str, direction: str) -> _FrameSeries:
    if direction == "LONG":
        bars = [
            _bar(symbol=symbol, index=0, open_=100.0, high=100.2, low=99.8, close=100.0),
            _bar(symbol=symbol, index=1, open_=100.0, high=100.4, low=99.9, close=100.3),
            _bar(symbol=symbol, index=2, open_=100.0, high=100.7, low=99.7, close=100.6),
            _bar(symbol=symbol, index=3, open_=100.6, high=101.0, low=100.4, close=100.9),
            _bar(symbol=symbol, index=4, open_=100.9, high=101.2, low=100.7, close=101.0),
            _bar(symbol=symbol, index=5, open_=101.0, high=101.95, low=100.9, close=101.85),
            _bar(symbol=symbol, index=6, open_=101.85, high=102.0, low=101.2, close=101.4),
            _bar(symbol=symbol, index=7, open_=101.4, high=101.5, low=100.8, close=100.9),
        ]
    else:
        bars = [
            _bar(symbol=symbol, index=0, open_=100.0, high=100.2, low=99.8, close=100.0),
            _bar(symbol=symbol, index=1, open_=100.0, high=100.1, low=99.7, close=99.8),
            _bar(symbol=symbol, index=2, open_=100.0, high=100.2, low=99.6, close=99.7),
            _bar(symbol=symbol, index=3, open_=99.7, high=99.8, low=99.2, close=99.3),
            _bar(symbol=symbol, index=4, open_=99.3, high=99.5, low=99.0, close=99.1),
            _bar(symbol=symbol, index=5, open_=99.1, high=99.2, low=98.4, close=98.5),
            _bar(symbol=symbol, index=6, open_=98.5, high=99.0, low=98.3, close=98.8),
            _bar(symbol=symbol, index=7, open_=98.8, high=99.3, low=98.6, close=99.0),
        ]
    return _FrameSeries.from_bars(bars)


def _features_for_lane(*, lane_id: str) -> list[dict[str, object]]:
    session_labels = ["US", "US", "US", "US", "LONDON", "LONDON", "LONDON", "LONDON"]
    if "failed" in lane_id:
        session_labels = ["LONDON", "LONDON", "LONDON", "LONDON", "US", "US", "US", "US"]
    rows: list[dict[str, object]] = []
    for index, session_label in enumerate(session_labels):
        row: dict[str, object] = {
            "ready": index == 1,
            "risk_unit": 1.0,
            "session_label": session_label,
            "close_pos": 0.8 if "breakout" in lane_id else 0.2,
        }
        if "breakout" in lane_id:
            row.update(
                {
                    "regime_up": True,
                    "compression_60": 0.5,
                    "compression_5": 0.5,
                    "breakout_up": 0.5,
                    "slope_60": 0.4,
                }
            )
        else:
            row.update(
                {
                    "failed_breakout_short": True,
                    "dist_240": 1.4,
                    "body_r": 0.5,
                }
            )
        rows.append(row)
    return rows


def _synthetic_symbol_store() -> dict[str, dict[str, object]]:
    store: dict[str, dict[str, object]] = {}
    for spec in approved_quant_lane_specs():
        for symbol in spec.symbols:
            store[symbol] = {
                "execution": _execution_for_direction(symbol, spec.direction),
                "features": _features_for_lane(lane_id=spec.lane_id),
            }
    return store


def test_exit_variant_score_rewards_robust_post_cost_exits() -> None:
    strong = exit_research._exit_variant_score(
        summary={
            "expectancy_net_020_r": 0.12,
            "expectancy_net_025_r": 0.07,
            "walk_forward_positive_ratio": 0.80,
            "avg_giveback_from_peak_r": 0.20,
        },
        robustness={
            "leave_one_symbol_out_positive_ratio_025": 0.80,
            "leave_one_session_out_positive_ratio_025": 0.60,
            "concentration": {"dominant_symbol_share_of_total_abs_r_025": 0.30},
        },
        parameter_sensitivity={"positive_ratio_cost_025": 0.66},
        variant={"complexity_points": 1},
    )
    weak = exit_research._exit_variant_score(
        summary={
            "expectancy_net_020_r": -0.02,
            "expectancy_net_025_r": -0.05,
            "walk_forward_positive_ratio": 0.20,
            "avg_giveback_from_peak_r": 0.95,
        },
        robustness={
            "leave_one_symbol_out_positive_ratio_025": 0.20,
            "leave_one_session_out_positive_ratio_025": 0.0,
            "concentration": {"dominant_symbol_share_of_total_abs_r_025": 0.85},
        },
        parameter_sensitivity={"positive_ratio_cost_025": 0.10},
        variant={"complexity_points": 3},
    )
    assert strong > weak


def test_run_approved_quant_exit_research_writes_lane_reports(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(exit_research, "build_approved_quant_symbol_store", lambda **_: _synthetic_symbol_store())

    artifacts = exit_research.run_approved_quant_exit_research(
        database_path=tmp_path / "dummy.sqlite3",
        output_dir=tmp_path / "exit_report",
    )

    report = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert report["lane_count"] == 2
    assert {lane["lane_id"] for lane in report["lane_reports"]} == {
        "phase2c.breakout.metals_only.us_unknown.baseline",
        "phase2c.failed.core4_plus_qc.no_us.baseline",
    }
    for lane in report["lane_reports"]:
        assert lane["control_exit_id"].endswith(".approved_control")
        assert lane["entry_diagnostic"]["raw_entry_count"] > 0
        families = {row["variant"]["family"] for row in lane["ranked_exit_variants"]}
        assert "approved_control" in families
        assert "time_stop_only" in families
        assert "session_boundary" in families
        assert "checkpoint_no_traction" in families
        assert isinstance(lane["recommendation"]["current_approved_exit_should_remain_unchanged"], bool)

    markdown = artifacts.markdown_path.read_text(encoding="utf-8")
    assert "# Approved Quant Exit Research" in markdown
    assert "Top exit variants:" in markdown


def test_simulate_trade_checkpoint_no_traction_arms_and_trails() -> None:
    spec = approved_quant_lane_specs()[0]
    execution = _execution_for_direction("GC", "LONG")
    features = _features_for_lane(lane_id=spec.lane_id)
    entry = exit_research.ApprovedLaneEntryCandidate(
        lane_id=spec.lane_id,
        lane_name=spec.lane_name,
        symbol="GC",
        session_label="US",
        signal_index=1,
        entry_index=2,
        signal_ts=execution.timestamps[1].isoformat(),
        entry_ts=execution.timestamps[2].isoformat(),
        entry_price=execution.opens[2],
        risk=1.0,
        direction="LONG",
        rule_snapshot={},
    )
    variant = exit_research.ExitVariantSpec(
        lane_id=spec.lane_id,
        exit_id="test.checkpoint_no_traction",
        family="checkpoint_no_traction",
        description="test",
        hold_bars=spec.hold_bars,
        stop_r=spec.stop_r,
        target_r=None,
        checkpoint_arm_r=0.8,
        checkpoint_lock_r=0.35,
        checkpoint_trail_r=0.25,
        no_traction_abort_bars=2,
        no_traction_min_favorable_r=0.25,
    )

    trade = exit_research._simulate_trade(
        spec=spec,
        variant=variant,
        entry=entry,
        execution=execution,
        features=features,
    )

    assert trade.exit_reason == "checkpoint_stop"
    assert trade.exit_price > trade.entry_price


def test_simulate_trade_no_traction_abort_triggers_before_checkpoint() -> None:
    spec = approved_quant_lane_specs()[0]
    execution = _FrameSeries.from_bars(
        [
            _bar(symbol="GC", index=0, open_=100.0, high=100.2, low=99.8, close=100.0),
            _bar(symbol="GC", index=1, open_=100.0, high=100.1, low=99.9, close=100.0),
            _bar(symbol="GC", index=2, open_=100.0, high=100.1, low=99.8, close=99.95),
            _bar(symbol="GC", index=3, open_=99.95, high=100.15, low=99.7, close=99.9),
            _bar(symbol="GC", index=4, open_=99.9, high=100.1, low=99.6, close=99.85),
        ]
    )
    features = _features_for_lane(lane_id=spec.lane_id)
    entry = exit_research.ApprovedLaneEntryCandidate(
        lane_id=spec.lane_id,
        lane_name=spec.lane_name,
        symbol="GC",
        session_label="US",
        signal_index=1,
        entry_index=2,
        signal_ts=execution.timestamps[1].isoformat(),
        entry_ts=execution.timestamps[2].isoformat(),
        entry_price=execution.opens[2],
        risk=1.0,
        direction="LONG",
        rule_snapshot={},
    )
    variant = exit_research.ExitVariantSpec(
        lane_id=spec.lane_id,
        exit_id="test.checkpoint_no_traction",
        family="checkpoint_no_traction",
        description="test",
        hold_bars=spec.hold_bars,
        stop_r=spec.stop_r,
        target_r=None,
        checkpoint_arm_r=0.8,
        checkpoint_lock_r=0.35,
        checkpoint_trail_r=0.25,
        no_traction_abort_bars=2,
        no_traction_min_favorable_r=0.25,
    )

    trade = exit_research._simulate_trade(
        spec=spec,
        variant=variant,
        entry=entry,
        execution=execution,
        features=features,
    )

    assert trade.exit_reason == "no_traction_abort"
