from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from mgc_v05l.app.approved_quant_lanes.evaluator import evaluate_approved_lane
from mgc_v05l.app.approved_quant_lanes.specs import ApprovedQuantLaneSpec, approved_quant_lane_specs
from mgc_v05l.domain.models import Bar
from mgc_v05l.research.quant_futures import _FrameSeries
from validation_layer.config.defaults import build_debug_config
from validation_layer.layer1.adapters import build_strategy_backtest_from_approved_quant_lane
from validation_layer.layer1.pilot import run_approved_quant_layer1_pilot_rerun
from validation_layer.orchestration.pipeline import run_validation_pipeline


def _approved_quant_symbol_store() -> dict[str, dict[str, object]]:
    store: dict[str, dict[str, object]] = {}
    for spec in approved_quant_lane_specs():
        for symbol in spec.symbols:
            store[symbol] = {
                "execution": _execution_frame(symbol=symbol, direction=spec.direction),
                "features": _feature_rows(spec=spec),
            }
    return store


def _execution_frame(*, symbol: str, direction: str) -> _FrameSeries:
    if direction == "LONG":
        bars = [
            _bar(symbol=symbol, index=0, open_=100.0, high=100.2, low=99.8, close=100.0, session="US"),
            _bar(symbol=symbol, index=1, open_=100.0, high=100.3, low=99.9, close=100.2, session="US"),
            _bar(symbol=symbol, index=2, open_=100.2, high=100.8, low=100.0, close=100.6, session="US"),
            _bar(symbol=symbol, index=3, open_=100.6, high=101.2, low=100.4, close=101.0, session="US"),
            _bar(symbol=symbol, index=4, open_=101.0, high=101.4, low=100.8, close=101.2, session="UNKNOWN"),
            _bar(symbol=symbol, index=5, open_=101.2, high=101.5, low=100.9, close=101.1, session="UNKNOWN"),
            _bar(symbol=symbol, index=6, open_=101.1, high=101.3, low=100.7, close=100.9, session="UNKNOWN"),
            _bar(symbol=symbol, index=7, open_=100.9, high=101.0, low=100.5, close=100.7, session="UNKNOWN"),
        ]
    else:
        bars = [
            _bar(symbol=symbol, index=0, open_=100.0, high=100.2, low=99.8, close=100.0, session="LONDON"),
            _bar(symbol=symbol, index=1, open_=100.0, high=100.1, low=99.7, close=99.8, session="LONDON"),
            _bar(symbol=symbol, index=2, open_=99.8, high=99.9, low=99.1, close=99.3, session="LONDON"),
            _bar(symbol=symbol, index=3, open_=99.3, high=99.4, low=98.6, close=98.9, session="LONDON"),
            _bar(symbol=symbol, index=4, open_=98.9, high=99.0, low=98.3, close=98.5, session="UNKNOWN"),
            _bar(symbol=symbol, index=5, open_=98.5, high=98.8, low=98.1, close=98.4, session="UNKNOWN"),
            _bar(symbol=symbol, index=6, open_=98.4, high=98.9, low=98.2, close=98.7, session="UNKNOWN"),
            _bar(symbol=symbol, index=7, open_=98.7, high=99.0, low=98.5, close=98.8, session="UNKNOWN"),
        ]
    return _FrameSeries.from_bars(bars)


def _bar(
    *,
    symbol: str,
    index: int,
    open_: float,
    high: float,
    low: float,
    close: float,
    session: str,
) -> Bar:
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
        session_asia=session == "ASIA",
        session_london=session == "LONDON",
        session_us=session == "US",
        session_allowed=True,
    )


def _feature_rows(*, spec: ApprovedQuantLaneSpec) -> list[dict[str, object]]:
    sessions = ["US", "US", "US", "US", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"]
    if spec.direction == "SHORT":
        sessions = ["LONDON", "LONDON", "LONDON", "LONDON", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"]
    rows: list[dict[str, object]] = []
    for index, session in enumerate(sessions):
        row: dict[str, object] = {
            "ready": index == 1,
            "risk_unit": 1.0,
            "session_label": session,
            "close_pos": 0.78 if spec.direction == "LONG" else 0.20,
        }
        if spec.direction == "LONG":
            row.update(
                {
                    "regime_up": True,
                    "compression_60": 0.50,
                    "compression_5": 0.45,
                    "breakout_up": 0.55,
                    "slope_60": 0.35,
                }
            )
        else:
            row.update(
                {
                    "failed_breakout_short": True,
                    "dist_240": 1.40,
                    "body_r": 0.45,
                }
            )
        rows.append(row)
    return rows


def test_approved_quant_adapter_builds_layer1_ready_backtest() -> None:
    spec = approved_quant_lane_specs()[0]
    symbol_store = _approved_quant_symbol_store()
    evaluated_lane = evaluate_approved_lane(spec=spec, symbol_store=symbol_store)

    backtest = build_strategy_backtest_from_approved_quant_lane(
        spec=spec,
        evaluated_lane=evaluated_lane,
        symbol_store=symbol_store,
        execution_timeframe="5m",
        data_version="approved_quant_layer1_test_v1",
        code_version="test",
        cost_basis_r=0.25,
    )
    report = run_validation_pipeline(backtest, build_debug_config())
    module_map = {result.module_name: result for result in report.module_results}

    assert backtest.provenance is not None
    assert backtest.provenance.producer_id == "approved_quant_lanes.evaluate_approved_lane"
    assert backtest.provenance.execution_assumptions.fill_policy == "next_bar_open_after_signal"
    assert module_map["layer1_prerequisites"].status == "pass"
    assert module_map["layer1_prerequisites"].metrics["insufficient_evidence"] is False
    assert max(abs(point.position) for point in backtest.position_series) > 1.0
    assert backtest.metadata["scope_fingerprint"]
    assert len(backtest.trades) == len(spec.symbols)


def test_approved_quant_adapter_rejects_lossy_trade_payload() -> None:
    spec = approved_quant_lane_specs()[0]
    symbol_store = _approved_quant_symbol_store()
    evaluated_lane = evaluate_approved_lane(spec=spec, symbol_store=symbol_store)
    broken_payload = deepcopy(evaluated_lane)
    del broken_payload["trades"][0]["signal_timestamp"]

    with pytest.raises(ValueError, match="signal_timestamp"):
        build_strategy_backtest_from_approved_quant_lane(
            spec=spec,
            evaluated_lane=broken_payload,
            symbol_store=symbol_store,
            execution_timeframe="5m",
            data_version="approved_quant_layer1_test_v1",
            code_version="test",
            cost_basis_r=0.25,
        )


def test_approved_quant_layer1_pilot_rerun_writes_validation_artifacts(tmp_path: Path) -> None:
    artifacts = run_approved_quant_layer1_pilot_rerun(output_dir=tmp_path / "approved_quant_layer1")
    manifest = json.loads(artifacts.manifest_path.read_text(encoding="utf-8"))

    assert manifest["pilot_count"] == len(approved_quant_lane_specs())
    for report_paths in artifacts.reports.values():
        payload = json.loads(Path(report_paths["validation_report_json"]).read_text(encoding="utf-8"))
        module_names = {row["module_name"]: row for row in payload["module_results"]}
        assert module_names["layer1_prerequisites"]["status"] == "pass"
