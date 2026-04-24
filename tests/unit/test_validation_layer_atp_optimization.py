from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.research.trend_participation.atp_promotion_add_review import default_atp_promotion_add_candidates
from mgc_v05l.research.trend_participation.models import ResearchBar

from validation_layer.config.defaults import build_debug_config
from validation_layer.data.contracts import SplitWindow, ValidationSubject
from validation_layer.layer1.atp_optimization import (
    build_optimization_run_from_atp_window_history,
    build_strategy_backtest_from_atp_candidate_history,
)
from validation_layer.layer1.pilot import run_atp_promotion_add_optimization_pilot_rerun
from validation_layer.orchestration.pipeline import run_validation_pipeline


def _split_rows() -> list[dict[str, object]]:
    candidate_ids = [candidate.candidate_id for candidate in default_atp_promotion_add_candidates()]
    objectives = [
        {candidate_ids[0]: -30.0, candidate_ids[1]: 10.0, candidate_ids[2]: 18.0},
        {candidate_ids[0]: -22.0, candidate_ids[1]: 12.0, candidate_ids[2]: 16.0},
        {candidate_ids[0]: -28.0, candidate_ids[1]: 8.0, candidate_ids[2]: 14.0},
        {candidate_ids[0]: -18.0, candidate_ids[1]: 4.0, candidate_ids[2]: 6.0},
    ]
    rows: list[dict[str, object]] = []
    for index, objective_map in enumerate(objectives, start=1):
        candidates = []
        for candidate_id, objective_value in objective_map.items():
            candidates.append(
                {
                    "candidate_id": candidate_id,
                    "candidate_label": candidate_id,
                    "net_pnl_cash": 1000.0 + objective_value,
                    "trade_count": 12,
                    "average_trade_pnl_cash": 25.0 + objective_value / 10.0,
                    "profit_factor": 1.1 + objective_value / 100.0,
                    "max_drawdown": 90.0 - objective_value / 5.0,
                    "objective_value": objective_value,
                    "average_trade_pnl_cash_delta": objective_value / 10.0,
                    "profit_factor_delta": objective_value / 100.0,
                    "max_drawdown_delta": -objective_value / 5.0,
                }
            )
        rows.append(
            {
                "split_id": f"window_{index}",
                "window_label": f"2026-03-{index:02d}->2026-03-{index:02d}",
                "bars_processed": 400,
                "baseline_metrics": {"net_pnl_cash": 1000.0},
                "candidates": candidates,
            }
        )
    return rows


def _candidate_rows(candidate_id: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    start = datetime(2026, 3, 1, 0, 0, tzinfo=UTC)
    for index in range(4):
        entry_ts = start + timedelta(days=index, minutes=5)
        exit_ts = entry_ts + timedelta(minutes=15)
        rows.append(
            {
                "added": index % 2 == 0,
                "candidate_id": candidate_id,
                "candidate_label": candidate_id,
                "instrument": "MGC",
                "variant_id": "trend_participation.atp_v1_long_pullback_continuation.long.base",
                "entry_ts": entry_ts,
                "decision_ts": entry_ts - timedelta(minutes=1),
                "exit_ts": exit_ts,
                "position_entry_price": 100.0 + index,
                "position_exit_price": 101.5 + index,
                "trade_pnl_cash": 18.0 + index,
                "pnl_cash": 22.0 + index,
                "add_pnl_cash": 4.0 if index % 2 == 0 else 0.0,
                "add_pnl_points": 0.8 if index % 2 == 0 else 0.0,
                "add_entry_ts": entry_ts + timedelta(minutes=3) if index % 2 == 0 else None,
                "add_exit_ts": exit_ts if index % 2 == 0 else None,
                "add_entry_price": 100.8 + index if index % 2 == 0 else None,
                "add_trigger_price": 100.7 + index if index % 2 == 0 else None,
                "add_price_quality_state": "VWAP_FAVORABLE" if index % 2 == 0 else None,
                "add_reason": "PROMOTION_1_EARNED" if index % 2 == 0 else "PROMOTION_NOT_EARNED",
                "evidence_truth": "strong_replay_derived",
                "modeled_exit_dependency": "inherits_frozen_baseline_exit",
                "depends_on_weak_evidence": False,
                "hold_minutes": 15.0,
                "bars_held_1m": 15,
                "add_hold_minutes": 12.0 if index % 2 == 0 else 0.0,
                "side": "LONG",
                "session_segment": "ASIA" if index < 2 else "US",
                "mfe_points": 2.5,
                "mae_points": 0.9,
                "family": "atp_v1_long_pullback_continuation",
                "exit_reason": "trend_failure",
            }
        )
    return rows


def _price_bars() -> list[ResearchBar]:
    bars: list[ResearchBar] = []
    start = datetime(2026, 3, 1, 0, 0, tzinfo=UTC)
    for index in range(40):
        end_ts = start + timedelta(minutes=index + 1)
        bars.append(
            ResearchBar(
                instrument="MGC",
                timeframe="1m",
                start_ts=end_ts - timedelta(minutes=1),
                end_ts=end_ts,
                open=100.0 + index * 0.1,
                high=100.2 + index * 0.1,
                low=99.9 + index * 0.1,
                close=100.1 + index * 0.1,
                volume=100,
                session_label="ASIA" if index < 20 else "US",
                session_segment="ASIA" if index < 20 else "US",
                source="synthetic",
                provenance="test",
            )
        )
    return bars


def _split_windows(count: int) -> list[SplitWindow]:
    start = datetime(2026, 3, 1, tzinfo=UTC)
    return [
        SplitWindow(
            split_id=f"window_{index + 1}",
            fold_index=index,
            role="evaluation",
            train_start=start + timedelta(days=index),
            train_end=start + timedelta(days=index, hours=23),
            test_start=start + timedelta(days=index),
            test_end=start + timedelta(days=index, hours=23),
        )
        for index in range(count)
    ]


def test_build_optimization_run_from_window_history_preserves_ranked_trials() -> None:
    candidates = default_atp_promotion_add_candidates()
    run = build_optimization_run_from_atp_window_history(
        strategy_name="atp_promotion_add::promotion_1_075r_favorable_only::mgc",
        candidates=candidates,
        chosen_candidate_id="promotion_1_075r_favorable_only",
        split_rows=_split_rows(),
    )

    assert run.chosen_parameters["candidate_id"] == "promotion_1_075r_favorable_only"
    assert run.objective_name == "net_pnl_cash_delta_vs_frozen_baseline"
    assert len(run.trials) == 12
    ranks = [trial.rank for trial in run.trials if trial.split_id == "window_1"]
    assert sorted(ranks) == [1, 2, 3]


def test_strategy_backtest_from_candidate_history_attaches_optimization_linkage() -> None:
    candidates = {candidate.candidate_id: candidate for candidate in default_atp_promotion_add_candidates()}
    run = build_optimization_run_from_atp_window_history(
        strategy_name="atp_promotion_add::promotion_1_075r_favorable_only::mgc",
        candidates=tuple(candidates.values()),
        chosen_candidate_id="promotion_1_075r_favorable_only",
        split_rows=_split_rows(),
    )
    backtest = build_strategy_backtest_from_atp_candidate_history(
        strategy_name=run.strategy_name,
        candidate=candidates["promotion_1_075r_favorable_only"],
        candidate_rows=_candidate_rows("promotion_1_075r_favorable_only"),
        price_bars=_price_bars(),
        split_windows=_split_windows(4),
        data_version="test_windows",
        code_version="test",
        source_sqlite_path="mgc_v05l.replay.sqlite3",
        optimization_run=run,
        artifact_references={"optimization_history": "synthetic_history"},
    )

    assert backtest.provenance is not None
    assert backtest.provenance.optimization_linkage is not None
    assert backtest.provenance.optimization_linkage.objective_name == "net_pnl_cash_delta_vs_frozen_baseline"
    assert backtest.position_series[-1].position == 0.0


def test_pipeline_fires_optimization_modules_with_attached_run() -> None:
    candidates = {candidate.candidate_id: candidate for candidate in default_atp_promotion_add_candidates()}
    run = build_optimization_run_from_atp_window_history(
        strategy_name="atp_promotion_add::promotion_1_075r_favorable_only::mgc",
        candidates=tuple(candidates.values()),
        chosen_candidate_id="promotion_1_075r_favorable_only",
        split_rows=_split_rows(),
    )
    backtest = build_strategy_backtest_from_atp_candidate_history(
        strategy_name=run.strategy_name,
        candidate=candidates["promotion_1_075r_favorable_only"],
        candidate_rows=_candidate_rows("promotion_1_075r_favorable_only"),
        price_bars=_price_bars(),
        split_windows=_split_windows(4),
        data_version="test_windows",
        code_version="test",
        source_sqlite_path="mgc_v05l.replay.sqlite3",
        optimization_run=run,
    )

    report = run_validation_pipeline(
        ValidationSubject(strategy_backtest=backtest, optimization_run=run),
        build_debug_config(),
    )
    module_names = {result.module_name: result for result in report.module_results}

    assert "train_bias" in module_names
    assert "selection_bias" in module_names
    assert "cscv_pbo" in module_names
    assert module_names["train_bias"].metrics["training_bias_score"] is not None
    assert module_names["selection_bias"].metrics["selection_bias_score"] is not None
    assert module_names["cscv_pbo"].metrics["cscv_pbo_score"] is not None


def test_pipeline_reports_insufficient_evidence_when_optimization_history_is_too_thin() -> None:
    candidates = default_atp_promotion_add_candidates()
    split_rows = _split_rows()[:1]
    run = build_optimization_run_from_atp_window_history(
        strategy_name="atp_promotion_add::promotion_1_075r_favorable_only::mgc",
        candidates=candidates,
        chosen_candidate_id="promotion_1_075r_favorable_only",
        split_rows=split_rows,
    )
    backtest = build_strategy_backtest_from_atp_candidate_history(
        strategy_name=run.strategy_name,
        candidate={candidate.candidate_id: candidate for candidate in candidates}["promotion_1_075r_favorable_only"],
        candidate_rows=_candidate_rows("promotion_1_075r_favorable_only"),
        price_bars=_price_bars(),
        split_windows=_split_windows(1),
        data_version="thin_windows",
        code_version="test",
        source_sqlite_path="mgc_v05l.replay.sqlite3",
        optimization_run=run,
    )

    report = run_validation_pipeline(ValidationSubject(strategy_backtest=backtest, optimization_run=run), build_debug_config())
    module_names = {result.module_name: result for result in report.module_results}

    assert module_names["selection_bias"].status == "warn"
    assert module_names["cscv_pbo"].status == "warn"
    assert module_names["cscv_pbo"].metrics["insufficient_evidence"] is True


def test_atp_promotion_add_optimization_pilot_writes_combined_artifacts(monkeypatch, tmp_path: Path) -> None:
    candidates = {candidate.candidate_id: candidate for candidate in default_atp_promotion_add_candidates()}
    run = build_optimization_run_from_atp_window_history(
        strategy_name="atp_promotion_add::promotion_1_075r_favorable_only::mgc",
        candidates=tuple(candidates.values()),
        chosen_candidate_id="promotion_1_075r_favorable_only",
        split_rows=_split_rows(),
    )
    backtest = build_strategy_backtest_from_atp_candidate_history(
        strategy_name=run.strategy_name,
        candidate=candidates["promotion_1_075r_favorable_only"],
        candidate_rows=_candidate_rows("promotion_1_075r_favorable_only"),
        price_bars=_price_bars(),
        split_windows=_split_windows(4),
        data_version="test_windows",
        code_version="test",
        source_sqlite_path="mgc_v05l.replay.sqlite3",
        optimization_run=run,
        artifact_references={"optimization_history": "synthetic_history"},
    )

    class _Bundle:
        def __init__(self):
            self.strategy_backtest = backtest
            self.optimization_run = run
            self.history_payload = {"window_count": 4, "strategy_name": run.strategy_name}

    monkeypatch.setattr(
        "validation_layer.layer1.pilot.rerun_atp_promotion_add_validation_bundle",
        lambda **_: _Bundle(),
    )

    artifacts = run_atp_promotion_add_optimization_pilot_rerun(output_dir=tmp_path / "pilot")
    manifest = json.loads(artifacts.manifest_path.read_text(encoding="utf-8"))
    report_paths = manifest["rows"][0]["report_paths"]

    assert manifest["pilot_count"] == 1
    assert json.loads(Path(report_paths["strategy_backtest_json"]).read_text(encoding="utf-8"))
    assert json.loads(Path(report_paths["optimization_run_json"]).read_text(encoding="utf-8"))
    payload = json.loads(Path(report_paths["validation_report_json"]).read_text(encoding="utf-8"))
    module_names = {row["module_name"]: row for row in payload["module_results"]}
    assert "train_bias" in module_names
    assert "selection_bias" in module_names
    assert "cscv_pbo" in module_names
