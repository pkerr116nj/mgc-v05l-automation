from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from validation_layer import build_debug_config, run_validation_pipeline
from validation_layer.data.contracts import ValidationSubject
from validation_layer.layer1.asia_london_family import (
    build_asia_london_candidate_bundle,
    load_asia_london_family_source,
)
from validation_layer.layer1.pilot import run_asia_london_participation_family_pilot_rerun


VARIANT_LONG_V5 = "LONG__segment_forced_long_v5_dip_reclaim_or_bar8__base"
VARIANT_SHORT_V2 = "SHORT__segment_forced_short_v2_reclaim_fail_or_bar7__base"
VARIANT_LONG_V6 = "LONG__segment_forced_long_v6_contextual_fallback__base"
VARIANT_ES = "LONG__segment_forced_long_v6_contextual_fallback__vol_floor_1p25"


def _session(symbol: str, variant_id: str, side: str, day_offset: int, pnl_points: float) -> dict[str, object]:
    entry = datetime(2024, 1, 2, 0, 15, tzinfo=UTC) + timedelta(days=day_offset)
    exit_time = entry + timedelta(minutes=45)
    return {
        "symbol": symbol,
        "trade_date": entry.date().isoformat(),
        "side": side,
        "variant_id": variant_id,
        "entered": True,
        "entry_reason": "synthetic",
        "entry_bar_number": 2,
        "entry_end_ts": entry.isoformat(),
        "entry_price": 100.0,
        "stop_price": 99.5 if side == "LONG" else 100.5,
        "exit_bar_number": 6,
        "exit_end_ts": exit_time.isoformat(),
        "exit_price": 100.0 + pnl_points,
        "exit_reason": "time_stop",
        "exit_session_phase": "LONDON_LATE",
        "pnl_points": pnl_points + 0.1,
        "net_pnl_points": pnl_points,
        "gross_r_multiple": 1.0,
        "net_r_multiple": 0.8,
        "mae_points": 0.2,
        "mfe_points": max(0.2, abs(pnl_points) + 0.1),
        "setup_return_points": 0.3,
        "setup_range_points": 0.8,
        "setup_close_location": 0.7,
        "setup_vwap_displacement": 0.2,
        "notes": (),
    }


def _variant_payload(symbol: str, variant_key: str, side: str, total_points: float, entered_trade_count: int) -> dict[str, object]:
    variant_id = variant_key.split("__")[1]
    sessions = [
        _session(symbol, variant_id, side, index, total_points / entered_trade_count)
        for index in range(entered_trade_count)
    ]
    average_points = total_points / entered_trade_count
    return {
        "variant_id": variant_id,
        "side": side,
        "gate_mode": "base",
        "description": variant_key,
        "trade_summary": {
            "average_net_pnl_points": average_points,
            "median_net_pnl_points": average_points,
            "entered_trade_count": entered_trade_count,
            "entered_trade_days": entered_trade_count,
            "net_profit_factor": max(0.6, 1.0 + average_points / 5.0),
            "max_drawdown_points": abs(min(total_points, 0.0)) + 1.0,
            "net_win_rate": 0.55,
            "total_net_pnl_points": total_points,
        },
        "sessions": sessions,
    }


def _optimization_payload(symbols: tuple[str, ...], per_symbol_objectives: dict[str, dict[str, float]]) -> dict[str, object]:
    symbol_reports: dict[str, object] = {}
    for symbol in symbols:
        symbol_reports[symbol] = {
            "variants": {
                VARIANT_LONG_V5: _variant_payload(symbol, VARIANT_LONG_V5, "LONG", per_symbol_objectives[symbol][VARIANT_LONG_V5], 3),
                VARIANT_SHORT_V2: _variant_payload(symbol, VARIANT_SHORT_V2, "SHORT", per_symbol_objectives[symbol][VARIANT_SHORT_V2], 3),
                VARIANT_LONG_V6: _variant_payload(symbol, VARIANT_LONG_V6, "LONG", per_symbol_objectives[symbol][VARIANT_LONG_V6], 3),
            }
        }
    return {
        "study_id": "asia-london-test",
        "generated_at": datetime(2026, 4, 23, 12, 0, tzinfo=UTC).isoformat(),
        "database_path": "/tmp/test.sqlite3",
        "definition": {
            "start_date": "2024-01-01",
            "end_date": "2026-04-22",
            "hold_segments": ["ASIA_EARLY", "ASIA_LATE", "LONDON_EARLY", "LONDON_LATE"],
            "optimization_modes": ["base"],
            "symbols": list(symbols),
        },
        "pair_rankings": {},
        "symbol_reports": symbol_reports,
    }


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _build_test_source(tmp_path: Path) -> dict[str, Path]:
    candidate_system = {
        "generated_at": datetime(2026, 4, 23, 12, 0, tzinfo=UTC).isoformat(),
        "candidate_system": {
            "candidate_id": "asia_london_participation_core_v1",
            "family_name": "asia_london_participation_core_v1",
            "lane_sequence": [
                {"lane_id": "ASIA_LONDON_LONG_V5", "symbol_group": "GC_MGC", "source_variant": VARIANT_LONG_V5, "side": "LONG"},
                {"lane_id": "ASIA_LONDON_SHORT_V2", "symbol_group": "GC_MGC", "source_variant": VARIANT_SHORT_V2, "side": "SHORT"},
                {"lane_id": "ASIA_LONDON_LONG_V6", "symbol_group": "NQ_MNQ", "source_variant": VARIANT_LONG_V6, "side": "LONG"},
                {"lane_id": "ASIA_LONDON_LONG_V5", "symbol_group": "NQ_MNQ", "source_variant": VARIANT_LONG_V5, "side": "LONG"},
                {"lane_id": "ASIA_LONDON_SHORT_V2", "symbol_group": "NQ_MNQ", "source_variant": VARIANT_SHORT_V2, "side": "SHORT"},
                {"lane_id": "ASIA_LONDON_LONG_V6_VOL_FLOOR_125", "symbol_group": "ES_ONLY", "source_variant": VARIANT_ES, "side": "LONG"},
            ],
        },
        "source_artifacts": {},
        "scenario_reports": [],
    }
    admission_plan = {
        "candidate_id": "asia_london_participation_core_v1",
        "family_name": "asia_london_participation_core_v1",
        "admission_status": "PACKAGE_READY_AND_RUNTIME_WIRING_REQUIRED",
        "combined_package": {},
        "package_scenarios": [{"symbol": "GC"}, {"symbol": "MGC"}, {"symbol": "NQ"}, {"symbol": "MNQ"}, {"symbol": "ES"}],
    }
    gc_mgc_payload = _optimization_payload(
        ("GC", "MGC"),
        {
            "GC": {VARIANT_LONG_V5: 5.0, VARIANT_SHORT_V2: -2.0, VARIANT_LONG_V6: 2.0},
            "MGC": {VARIANT_LONG_V5: 4.0, VARIANT_SHORT_V2: -1.0, VARIANT_LONG_V6: 1.0},
        },
    )
    nq_mnq_payload = _optimization_payload(
        ("NQ", "MNQ"),
        {
            "NQ": {VARIANT_LONG_V5: 3.0, VARIANT_SHORT_V2: 1.0, VARIANT_LONG_V6: 6.0},
            "MNQ": {VARIANT_LONG_V5: 2.0, VARIANT_SHORT_V2: 0.5, VARIANT_LONG_V6: 5.0},
        },
    )
    es_validation = {
        "generated_at": datetime(2026, 4, 23, 12, 0, tzinfo=UTC).isoformat(),
        "study_id": "es-summary",
        "definition": {},
        "artifact_paths": {},
        "ranked_candidates": [
            {
                "variant_key": VARIANT_ES,
                "variant_id": "segment_forced_long_v6_contextual_fallback",
                "gate_mode": "vol_floor_1p25",
                "average_net_pnl_points": 1.2,
                "entered_trade_count": 42,
                "net_profit_factor": 1.1,
                "max_drawdown_points": 8.0,
                "total_net_pnl_points": 50.0,
            }
        ],
    }
    return {
        "candidate_system_json": _write_json(tmp_path / "candidate_system.json", candidate_system),
        "admission_plan_json": _write_json(tmp_path / "admission_plan.json", admission_plan),
        "gc_mgc_optimization_json": _write_json(tmp_path / "gc_mgc_optimization.json", gc_mgc_payload),
        "nq_mnq_optimization_json": _write_json(tmp_path / "nq_mnq_optimization.json", nq_mnq_payload),
        "es_validation_json": _write_json(tmp_path / "es_validation.json", es_validation),
    }


def test_asia_london_adapter_maps_provenance_and_optimization(tmp_path: Path) -> None:
    paths = _build_test_source(tmp_path)
    source = load_asia_london_family_source(**paths)

    bundle = build_asia_london_candidate_bundle(
        source=source,
        variant_key=VARIANT_LONG_V5,
        strategy_name="asia_london_participation::long_v5",
        code_version="test",
    )

    assert bundle.strategy_backtest is not None
    assert bundle.optimization_run is not None
    assert bundle.support_status == "scientifically_judgeable"
    assert bundle.strategy_backtest.provenance is not None
    assert bundle.strategy_backtest.provenance.producer_family == "asia_london_participation_core_v1"
    assert bundle.strategy_backtest.provenance.optimization_linkage is not None
    assert bundle.strategy_backtest.parameters["symbols"] == ("GC", "MGC", "NQ", "MNQ")
    assert len(bundle.optimization_run.trials) == 12
    assert sorted({trial.split_id for trial in bundle.optimization_run.trials}) == ["GC", "MGC", "MNQ", "NQ"]


def test_asia_london_es_candidate_stays_insufficient_when_only_summary_exists(tmp_path: Path) -> None:
    paths = _build_test_source(tmp_path)
    source = load_asia_london_family_source(**paths)

    bundle = build_asia_london_candidate_bundle(
        source=source,
        variant_key=VARIANT_ES,
        strategy_name="asia_london_participation::es_only",
        code_version="test",
    )

    assert bundle.strategy_backtest is None
    assert bundle.optimization_run is None
    assert bundle.support_status == "insufficient_evidence"
    assert "session_lineage_unavailable" in bundle.evidence_missing


def test_asia_london_pipeline_integration_fires_bias_modules(tmp_path: Path) -> None:
    paths = _build_test_source(tmp_path)
    source = load_asia_london_family_source(**paths)
    bundle = build_asia_london_candidate_bundle(
        source=source,
        variant_key=VARIANT_SHORT_V2,
        strategy_name="asia_london_participation::short_v2",
        code_version="test",
    )

    report = run_validation_pipeline(
        ValidationSubject(strategy_backtest=bundle.strategy_backtest, optimization_run=bundle.optimization_run),
        build_debug_config(),
    )
    module_map = {result.module_name: result for result in report.module_results}

    assert module_map["train_bias"].metrics["training_bias_score"] is not None
    assert module_map["selection_bias"].metrics["selection_bias_score"] is not None
    assert module_map["cscv_pbo"].metrics["cscv_pbo_score"] is not None


def test_asia_london_family_pilot_writes_summary_artifacts(tmp_path: Path) -> None:
    paths = _build_test_source(tmp_path)
    artifacts = run_asia_london_participation_family_pilot_rerun(output_dir=tmp_path / "out", **paths)

    manifest = json.loads(artifacts.manifest_path.read_text(encoding="utf-8"))
    summary = json.loads((artifacts.root_dir / "asia_london_family_summary.json").read_text(encoding="utf-8"))

    assert manifest["pilot_count"] == 4
    assert summary["family_name"] == "asia_london_participation_core_v1"
    assert summary["core_surface_judgeability"] == "scientifically_judgeable"
    assert any(row["judgeability"] == "insufficient_evidence" for row in summary["rows"])
    assert any(row["optimization_history_attached"] for row in summary["rows"])
