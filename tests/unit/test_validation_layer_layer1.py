from __future__ import annotations

import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from validation_layer.config.defaults import build_debug_config
from validation_layer.config.schemas import ModuleFlags, ValidationConfig
from validation_layer.data.contracts import EquityPoint, PositionPoint, StrategyBacktest, TradeRecord
from validation_layer.layer1.pilot import run_layer1_pilot_rerun
from validation_layer.orchestration.pipeline import run_validation_pipeline


def _ts(index: int) -> datetime:
    base = datetime(2026, 4, 1, 9, 30, tzinfo=ZoneInfo("America/New_York"))
    return base + timedelta(minutes=5 * index)


def _strategy_without_layer1_provenance() -> StrategyBacktest:
    trades = (
        TradeRecord(
            entry_time=_ts(1),
            exit_time=_ts(2),
            direction="long",
            qty=1.0,
            gross_pnl=10.0,
            net_pnl=10.0,
            mae=-1.0,
            mfe=12.0,
            holding_bars=1,
            holding_minutes=5.0,
            session_label="us",
        ),
    )
    equity_curve = (
        EquityPoint(timestamp=_ts(0), equity=100_000.0),
        EquityPoint(timestamp=_ts(2), equity=100_010.0),
    )
    position_series = (
        PositionPoint(timestamp=_ts(0), position=0.0),
        PositionPoint(timestamp=_ts(1), position=1.0),
        PositionPoint(timestamp=_ts(2), position=0.0),
    )
    return StrategyBacktest(
        strategy_name="missing_layer1",
        symbol="MGC",
        timeframe="5m",
        parameters={},
        bar_data=(),
        trades=trades,
        equity_curve=equity_curve,
        position_series=position_series,
    )


def test_pipeline_reports_insufficient_evidence_when_layer1_provenance_is_missing() -> None:
    config = ValidationConfig(
        module_flags=ModuleFlags(
            layer1_prerequisites=True,
            stationarity=False,
            entropy=False,
            canonical_performance=False,
            drawdown=False,
            walkforward=False,
            cross_validation=False,
            split_comparison=False,
            trade_normalization=False,
            training_bias=False,
            selection_bias=False,
            parameter_surface=False,
            bootstrap=False,
            monte_carlo=False,
            confidence_tests=False,
            overlap=False,
        )
    )
    report = run_validation_pipeline(_strategy_without_layer1_provenance(), config)
    prerequisites = next(result for result in report.module_results if result.module_name == "layer1_prerequisites")

    assert prerequisites.status == "warn"
    assert prerequisites.metrics["insufficient_evidence"] is True
    assert report.overall_status == "insufficient_evidence"


def test_layer1_pilot_rerun_writes_backtests_and_validation_reports(tmp_path) -> None:
    artifacts = run_layer1_pilot_rerun(output_dir=tmp_path / "layer1_pilot")
    manifest = json.loads(artifacts.manifest_path.read_text(encoding="utf-8"))

    assert manifest["pilot_count"] == 2
    for report_paths in artifacts.reports.values():
        assert json.loads(open(report_paths["strategy_backtest_json"], encoding="utf-8").read())
        payload = json.loads(open(report_paths["validation_report_json"], encoding="utf-8").read())
        module_names = {row["module_name"]: row for row in payload["module_results"]}
        assert module_names["layer1_prerequisites"]["status"] == "pass"
