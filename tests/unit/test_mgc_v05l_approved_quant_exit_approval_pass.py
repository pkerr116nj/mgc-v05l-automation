from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from mgc_v05l.domain.models import Bar
from mgc_v05l.research.approved_quant_exit_approval_pass import (
    _exit_approval_score,
    run_approved_quant_exit_approval_pass,
)
from mgc_v05l.research.quant_futures import _FrameSeries

import mgc_v05l.research.approved_quant_exit_approval_pass as approval_pass


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
    breakout_symbols = ("GC", "MGC", "HG", "PL")
    failed_symbols = ("CL", "ES", "6E", "6J", "QC")
    store: dict[str, dict[str, object]] = {}
    for symbol in breakout_symbols:
        store[symbol] = {
            "execution": _execution_for_direction(symbol, "LONG"),
            "features": _features_for_lane(lane_id="phase2c.breakout.metals_only.us_unknown.baseline"),
        }
    for symbol in failed_symbols:
        store[symbol] = {
            "execution": _execution_for_direction(symbol, "SHORT"),
            "features": _features_for_lane(lane_id="phase2c.failed.core4_plus_qc.no_us.baseline"),
        }
    return store


def test_exit_approval_score_rewards_tougher_cost_and_robustness() -> None:
    strong = _exit_approval_score(
        assessment={
            "candidate": {
                "cost_expectancy_r_030": 0.08,
                "cost_expectancy_r_035": 0.03,
                "concentration": {"dominant_symbol_share_of_total_abs_r": 0.30},
            },
            "improvement_vs_control": {"cost_expectancy_r_030": 0.04},
            "leave_one_symbol_out_positive_ratio_030": 1.0,
            "leave_one_session_out_positive_ratio_030": 0.75,
            "perturbation": {"positive_ratio_cost_030": 0.80},
        },
        candidate_variant={"complexity_points": 1},
    )
    weak = _exit_approval_score(
        assessment={
            "candidate": {
                "cost_expectancy_r_030": -0.02,
                "cost_expectancy_r_035": -0.05,
                "concentration": {"dominant_symbol_share_of_total_abs_r": 0.88},
            },
            "improvement_vs_control": {"cost_expectancy_r_030": 0.0},
            "leave_one_symbol_out_positive_ratio_030": 0.25,
            "leave_one_session_out_positive_ratio_030": 0.0,
            "perturbation": {"positive_ratio_cost_030": 0.20},
        },
        candidate_variant={"complexity_points": 3},
    )
    assert strong > weak


def test_run_approved_quant_exit_approval_pass_writes_verdicts(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(approval_pass, "build_approved_quant_symbol_store", lambda **_: _synthetic_symbol_store())

    artifacts = run_approved_quant_exit_approval_pass(
        database_path=tmp_path / "dummy.sqlite3",
        output_dir=tmp_path / "exit_approval",
    )

    report = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert set(report["exit_approval_verdicts"]) == {
        "phase2c.breakout.metals_only.us_unknown.baseline",
        "phase2c.failed.core4_plus_qc.no_us.baseline",
    }
    breakout = report["exit_approval_verdicts"]["phase2c.breakout.metals_only.us_unknown.baseline"]
    failed = report["exit_approval_verdicts"]["phase2c.failed.core4_plus_qc.no_us.baseline"]
    assert breakout["candidate_exit_id"].endswith(".time_stop_only.h24")
    assert failed["candidate_exit_id"].endswith(".structure_invalidation.r045")
    assert isinstance(breakout["replace_current_approved_exit"], bool)
    assert isinstance(failed["current_exit_should_remain_unchanged"], bool)

    markdown = artifacts.markdown_path.read_text(encoding="utf-8")
    assert "# Approved Quant Exit Approval Pass" in markdown
    assert "Candidate exit:" in markdown
