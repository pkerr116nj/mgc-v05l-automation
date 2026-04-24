from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

from validation_layer.data.contracts import EquityPoint, PositionPoint, PriceBar, StrategyBacktest, ValidationSubject
from validation_layer.layer1.runtime_sqlite_adapter import RuntimeSQLiteLaneSource, build_runtime_sqlite_strategy_backtest
from validation_layer.orchestration.live_universe_audit import (
    LiveUniverseItem,
    _atp_config_match_score,
    _taxonomy_for_row,
    run_live_candidate_universe_audit,
)
from validation_layer.reporting.models import ScorecardEntry, ValidationModuleResult, ValidationReport, ValidationScorecard


def test_runtime_sqlite_adapter_builds_backtest_from_runtime_db(tmp_path: Path) -> None:
    database_path = tmp_path / "runtime.sqlite3"
    connection = sqlite3.connect(database_path)
    try:
        connection.executescript(
            """
            create table bars (
                bar_id text primary key,
                symbol text,
                start_ts text,
                end_ts text,
                open real,
                high real,
                low real,
                close real,
                volume real,
                session_asia integer,
                session_london integer,
                session_us integer,
                data_source text
            );
            create table processed_bars (
                lane_id text,
                bar_id text,
                standalone_strategy_id text
            );
            create table strategy_state_snapshots (
                lane_id text,
                snapshot_ts text
            );
            create table trade_outcomes (
                trade_id integer,
                experiment_run_id text,
                entry_bar_id text,
                exit_bar_id text,
                ticker text,
                timeframe text,
                side text,
                entry_family text,
                entry_reason text,
                entry_price real,
                exit_price real,
                size real,
                bars_held integer,
                pnl real,
                mae real,
                mfe real,
                exit_reason text,
                quality_score_at_entry real,
                size_recommendation_at_entry real,
                created_at text
            );
            create table fills (fill_id integer);
            create table order_intents (intent_id integer);
            """
        )
        bar_times = [
            datetime(2026, 4, 20, 10, 0, tzinfo=UTC),
            datetime(2026, 4, 20, 10, 1, tzinfo=UTC),
            datetime(2026, 4, 20, 10, 2, tzinfo=UTC),
        ]
        for index, end_ts in enumerate(bar_times):
            start_ts = end_ts - timedelta(minutes=1)
            bar_id = f"MGC|1m|{end_ts.isoformat()}"
            connection.execute(
                """
                insert into bars values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    bar_id,
                    "MGC",
                    start_ts.isoformat(),
                    end_ts.isoformat(),
                    100.0 + index,
                    101.0 + index,
                    99.0 + index,
                    100.5 + index,
                    10.0 + index,
                    1,
                    0,
                    0,
                    "test_feed",
                ),
            )
            connection.execute(
                "insert into processed_bars values (?, ?, ?)",
                ("lane_alpha", bar_id, "alpha_strategy"),
            )
        connection.execute(
            "insert into strategy_state_snapshots values (?, ?)",
            ("lane_alpha", bar_times[-1].isoformat()),
        )
        connection.execute(
            """
            insert into trade_outcomes values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "exp-1",
                f"MGC|1m|{bar_times[0].isoformat()}",
                f"MGC|1m|{bar_times[2].isoformat()}",
                "MGC",
                "1m",
                "long",
                "trend_participation",
                "pullback_long",
                100.5,
                102.5,
                1.0,
                2,
                12.5,
                -1.0,
                2.5,
                "target_hit",
                0.75,
                1.0,
                bar_times[2].isoformat(),
            ),
        )
        connection.execute("insert into fills values (1)")
        connection.execute("insert into order_intents values (1)")
        connection.commit()
    finally:
        connection.close()

    backtest = build_runtime_sqlite_strategy_backtest(
        RuntimeSQLiteLaneSource(
            lane_id="lane_alpha",
            display_name="Lane Alpha",
            runtime_kind="approved_quant_strategy_engine",
            lane_mode="STANDARD",
            symbol="MGC",
            allowed_sessions=("ASIA",),
            source_family="approved_quant",
            strategy_family="approved_quant",
            strategy_identity_root="ALPHA",
            scope_kind="paper_runtime",
            config_path=tmp_path / "config.json",
            database_path=database_path,
            artifacts_dir=tmp_path / "artifacts",
            execution_timeframe="1m",
            point_value=10.0,
            trade_size=1.0,
        ),
        code_version="test",
    )

    assert backtest.strategy_name == "approved_quant::lane_alpha"
    assert len(backtest.bar_data) == 3
    assert len(backtest.trades) == 1
    assert backtest.trades[0].validation_metadata is not None
    assert backtest.provenance is not None
    assert backtest.provenance.producer_id == "probationary_runtime.sqlite_lane_artifacts"


def test_atp_config_match_prefers_runtime_consistent_lane_spec() -> None:
    item = LiveUniverseItem(
        item_id="lane::atp_companion_v1_pl_asia_us",
        item_type="lane",
        lane_id="atp_companion_v1_pl_asia_us",
        display_name="ATP Companion Candidate v1 — PL / Asia + US Executable, London Diagnostic-Only",
        runtime_kind="atp_companion_benchmark_paper",
        source_family="active_trend_participation_engine",
        symbol="PL",
        scope_kind="paper_runtime",
        config_path=Path("config/runtime.json"),
        database_path=Path("runtime.sqlite3"),
        artifacts_dir=Path("/tmp/atp_companion_v1_pl_asia_us"),
        metadata={
            "display_name": "ATP Companion Candidate v1 — PL / Asia + US Executable, London Diagnostic-Only",
            "standalone_strategy_id": "atp_companion_v1__paper_pl_asia_us",
            "runtime_kind": "atp_companion_benchmark_paper",
            "lane_mode": "ATP_COMPANION_BENCHMARK",
            "strategy_family": "active_trend_participation_engine",
            "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
        },
    )
    matching_spec = {
        "display_name": item.display_name,
        "symbol": "PL",
        "standalone_strategy_id": "atp_companion_v1__paper_pl_asia_us",
        "runtime_kind": "atp_companion_benchmark_paper",
        "lane_mode": "ATP_COMPANION_BENCHMARK",
        "strategy_family": "active_trend_participation_engine",
        "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
        "artifacts_dir": "/tmp/atp_companion_v1_pl_asia_us",
    }
    drifted_spec = {
        "display_name": item.display_name + " [3m]",
        "symbol": "PL",
        "standalone_strategy_id": "atp_companion_v1__paper_pl_asia_us",
        "runtime_kind": "atp_companion_benchmark_paper",
        "lane_mode": "ATP_COMPANION_CANDIDATE",
        "strategy_family": "active_trend_participation_engine",
        "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
        "artifacts_dir": "/tmp/elsewhere",
    }

    assert _atp_config_match_score(item, matching_spec) > _atp_config_match_score(item, drifted_spec)


def test_taxonomy_keeps_zero_trade_rejects_as_insufficient_evidence() -> None:
    row = {
        "evaluation_status": "completed",
        "operational_only_signal": False,
        "sample_evidence_status": "no_realized_trade_sample",
        "optimization_history_status": "unavailable",
        "overall_status": "reject",
    }
    assert _taxonomy_for_row(row) == "insufficient_evidence"


def test_taxonomy_keeps_operational_live_pilots_separate() -> None:
    row = {
        "evaluation_status": "blocked",
        "operational_only_signal": True,
        "sample_evidence_status": "unknown_sample",
        "optimization_history_status": "unavailable",
        "overall_status": None,
    }
    assert _taxonomy_for_row(row) == "operationally_useful_not_scientifically_supported"


def test_live_universe_audit_writes_taxonomy_summary_for_zero_trade_candidate(tmp_path: Path, monkeypatch) -> None:
    item = LiveUniverseItem(
        item_id="lane::sample_zero_trade",
        item_type="lane",
        lane_id="sample_zero_trade",
        display_name="Sample Zero Trade Lane",
        runtime_kind="approved_quant_strategy_engine",
        source_family="approved_quant",
        symbol="MGC",
        scope_kind="paper_runtime",
        config_path=tmp_path / "runtime.json",
        database_path=tmp_path / "runtime.sqlite3",
        artifacts_dir=tmp_path / "artifacts",
        metadata={},
    )

    strategy = StrategyBacktest(
        strategy_name="sample_zero_trade",
        symbol="MGC",
        timeframe="1m",
        parameters={"lane_id": "sample_zero_trade"},
        bar_data=(
            PriceBar(timestamp=datetime(2026, 4, 20, 10, 0, tzinfo=UTC), open=1, high=1, low=1, close=1),
            PriceBar(timestamp=datetime(2026, 4, 20, 10, 1, tzinfo=UTC), open=1, high=1, low=1, close=1),
        ),
        trades=(),
        equity_curve=(EquityPoint(timestamp=datetime(2026, 4, 20, 10, 0, tzinfo=UTC), equity=100000.0),),
        position_series=(PositionPoint(timestamp=datetime(2026, 4, 20, 10, 0, tzinfo=UTC), position=0.0, exposure=0.0),),
        metadata={},
    )

    report = ValidationReport(
        subject_type="strategy",
        subject_name="sample_zero_trade",
        overall_status="reject",
        module_results=(
            ValidationModuleResult(
                module_name="trade_normalization",
                status="fail",
                summary="Normalization shows 0.00 per trade.",
                metrics={"capital_efficiency_score": 0.0},
                diagnostics={},
                artifacts={},
                recommendations=[],
            ),
            ValidationModuleResult(
                module_name="bootstrap",
                status="warn",
                summary="Insufficient evidence: broader trade sample needed.",
                metrics={"insufficient_evidence": True, "bootstrap_confidence_score": 0.0},
                diagnostics={},
                artifacts={},
                recommendations=[],
            ),
            ValidationModuleResult(
                module_name="monte_carlo",
                status="warn",
                summary="Insufficient evidence: broader trade sample needed.",
                metrics={"insufficient_evidence": True, "path_fragility_score": 0.0},
                diagnostics={},
                artifacts={},
                recommendations=[],
            ),
            ValidationModuleResult(
                module_name="market_permutation",
                status="warn",
                summary="Insufficient evidence: position lineage aligned to bars is missing.",
                metrics={"insufficient_evidence": True, "market_permutation_score": 0.0},
                diagnostics={},
                artifacts={},
                recommendations=[],
            ),
        ),
        scorecard=ValidationScorecard(
            composite_score=0.0,
            entries=(ScorecardEntry("trade_normalization", "fail", "capital_efficiency_score", 0.0, {}),),
            pass_count=0,
            warn_count=3,
            fail_count=1,
            error_count=0,
            evidence_coverage=1.0,
        ),
        blocking_issues=("trade_normalization failed",),
        warnings=(),
        next_actions=("Collect more trades.",),
    )

    monkeypatch.setattr(
        "validation_layer.orchestration.live_universe_audit.build_live_candidate_universe_inventory",
        lambda: [item],
    )
    monkeypatch.setattr(
        "validation_layer.orchestration.live_universe_audit._build_subject_for_item",
        lambda _item: (ValidationSubject(strategy_backtest=strategy), {"strategy_backtest": strategy}),
    )
    monkeypatch.setattr(
        "validation_layer.orchestration.live_universe_audit.run_validation_pipeline",
        lambda subject, config: report,
    )

    result = run_live_candidate_universe_audit(output_dir=tmp_path / "audit")
    summary = json.loads(Path(result["summary_json_path"]).read_text(encoding="utf-8"))

    assert summary["taxonomy_counts"]["insufficient_evidence"] == 1
    assert summary["clear_failures"] == []
