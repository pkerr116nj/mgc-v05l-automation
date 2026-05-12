from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mgc_v05l.app.phase1_ticker_readiness_matrix import (
    PHASE1_TICKER_ORDER,
    Phase1TickerReadinessMatrixConfig,
    REPO_ROOT,
    build_phase1_ticker_readiness_matrix,
    write_phase1_ticker_readiness_matrix_artifacts,
)

NOW = datetime(2026, 5, 9, 14, 0, tzinfo=timezone.utc)


def _write_market_data_config(root: Path) -> None:
    path = root / "config" / "market_data_providers.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "databento": {
                    "pilot_symbols": {
                        symbol: {
                            "request_symbol": f"{symbol}.v.0",
                            "schema_by_timeframe": {"1m": "ohlcv-1m"},
                            "asset_class": "future",
                        }
                        for symbol in PHASE1_TICKER_ORDER
                    }
                }
            }
        ),
        encoding="utf-8",
    )


def _write_governance(root: Path, *, approve_gc: bool = False) -> None:
    path = root / "var" / "per_strategy_paper_status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    gc_row = {
        "strategy_id": "gc_1x_asia_london_participation__asia_london_long_v5",
        "instrument": "GC",
        "submit_allowed": True,
        "strategy_status": "WATCHLIST",
    }
    if approve_gc:
        gc_row |= {
            "strategy_approved": True,
            "paper_strategy_approved": True,
            "approved_phase1_strategy": True,
            "strategy_status": "PROBATION_ACTIVE",
        }
    path.write_text(
        json.dumps(
            {
                "strategies": [
                    gc_row,
                    {
                        "strategy_id": "nq_1x_asia_london_participation__asia_london_long_v5",
                        "instrument": "NQ",
                        "submit_allowed": True,
                        "strategy_status": "WATCHLIST",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )


def _write_runtime_artifacts(
    root: Path,
    *,
    symbol: str,
    historical_seed_ready: bool = False,
    realtime_feed_confirmed: bool = True,
    include_features: bool = True,
) -> None:
    for timeframe in ("1m", "3m", "5m"):
        artifact_specs = [("phase1_runtime_market_data", "latest_runtime_candles.json", "bars")]
        if include_features:
            artifact_specs.append(("phase1_runtime_features", "latest_runtime_features.json", "features"))
        for base, filename, row_key in artifact_specs:
            path = root / "outputs" / "track_b_execution_core" / base / symbol / timeframe / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(
                    {
                        "generated_at": NOW.isoformat(),
                        "source_id": "databento_live:test",
                        "symbol": symbol,
                        "timeframe": timeframe,
                            "completed_candles_only": True,
                            "historical_seed_ready": historical_seed_ready,
                            "realtime_feed_confirmed": realtime_feed_confirmed,
                            row_key: [
                                {
                                    "bar_start": (NOW.replace(second=0, microsecond=0) - {
                                        "1m": timedelta(minutes=1),
                                        "3m": timedelta(minutes=3),
                                        "5m": timedelta(minutes=5),
                                    }[timeframe]).isoformat(),
                                    "bar_end": NOW.isoformat(),
                                    "open": 100.0,
                                    "high": 101.0,
                                    "low": 99.5,
                                    "close": 100.5,
                                    "volume": 100,
                                }
                            ],
                        }
                    ),
                encoding="utf-8",
            )


def _config(root: Path) -> Phase1TickerReadinessMatrixConfig:
    return Phase1TickerReadinessMatrixConfig(
        repo_root=root,
        output_dir=Path("outputs") / "reports" / "phase1_ticker_readiness_matrix",
        now=NOW,
    )


def _repo_config_with_tmp_artifacts(root: Path) -> Phase1TickerReadinessMatrixConfig:
    return Phase1TickerReadinessMatrixConfig(
        repo_root=REPO_ROOT,
        market_data_config_path=root / "config" / "market_data_providers.json",
        governance_status_path=root / "var" / "per_strategy_paper_status.json",
        runtime_candle_dir=root / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data",
        feature_state_dir=root / "outputs" / "track_b_execution_core" / "phase1_runtime_features",
        output_dir=root / "outputs" / "reports" / "phase1_ticker_readiness_matrix",
        now=NOW,
    )


def test_matrix_includes_every_approved_phase1_ticker(tmp_path: Path) -> None:
    _write_market_data_config(tmp_path)
    _write_governance(tmp_path)

    artifacts = build_phase1_ticker_readiness_matrix(config=_config(tmp_path))

    assert [row["approved_phase1_symbol"] for row in artifacts.rows] == list(PHASE1_TICKER_ORDER)
    assert artifacts.report["approved_phase1_tickers"] == list(PHASE1_TICKER_ORDER)
    assert "RTY" not in {row["approved_phase1_symbol"] for row in artifacts.rows}
    assert "AAPL" not in {row["approved_phase1_symbol"] for row in artifacts.rows}


def test_matrix_distinguishes_full_size_micro_and_rates_contracts(tmp_path: Path) -> None:
    _write_market_data_config(tmp_path)
    artifacts = build_phase1_ticker_readiness_matrix(config=_config(tmp_path))
    rows = {row["approved_phase1_symbol"]: row for row in artifacts.rows}

    assert rows["GC"]["full_size_contract"] is True
    assert rows["GC"]["micro_contract"] is False
    assert rows["GC"]["rates_contract"] is False
    assert rows["GC"]["multiplier"] == "100"
    assert rows["MGC"]["full_size_contract"] is False
    assert rows["MGC"]["micro_contract"] is True
    assert rows["MGC"]["multiplier"] == "10"
    assert rows["NQ"]["multiplier"] == "20"
    assert rows["MNQ"]["multiplier"] == "2"
    assert rows["ES"]["multiplier"] == "50"
    assert rows["MES"]["multiplier"] == "5"
    assert rows["PL"]["full_size_contract"] is True
    assert rows["PL"]["micro_contract"] is False
    assert rows["PL"]["rates_contract"] is False
    assert rows["PL"]["exchange"] == "NYMEX"
    assert rows["PL"]["multiplier"] == "50"
    for symbol in ("ZT", "ZF", "ZN", "ZB"):
        assert rows[symbol]["rates_contract"] is True
        assert rows[symbol]["exchange"] == "CBOT"


def test_strategy_approval_defaults_false_and_blocks_submit(tmp_path: Path) -> None:
    _write_market_data_config(tmp_path)
    _write_governance(tmp_path)

    artifacts = build_phase1_ticker_readiness_matrix(config=_config(tmp_path))
    rows = {row["approved_phase1_symbol"]: row for row in artifacts.rows}

    assert rows["GC"]["governance_visible"] is True
    assert rows["GC"]["lane_adapter_present"] is True
    assert rows["GC"]["strategy_approved"] is False
    assert rows["GC"]["can_submit"] is False
    assert rows["GC"]["block_reason"] == "NO_APPROVED_STRATEGY"
    assert rows["GC"]["runtime_data_block_reason"] == "RUNTIME_CANDLES_MISSING"
    assert rows["NQ"]["block_reason"] == "NO_APPROVED_STRATEGY"


def test_rates_scope_is_visible_but_not_route_approved_without_adapter(tmp_path: Path) -> None:
    _write_market_data_config(tmp_path)

    artifacts = build_phase1_ticker_readiness_matrix(config=_config(tmp_path))
    rows = {row["approved_phase1_symbol"]: row for row in artifacts.rows}

    for symbol in ("ZT", "ZF", "ZN", "ZB"):
        assert rows[symbol]["source_symbol_supported"] is True
        assert rows[symbol]["executable_target_supported"] is True
        assert rows[symbol]["contract_metadata_present"] is True
        assert rows[symbol]["market_data_ready"] is True
        assert rows[symbol]["lane_adapter_present"] is False
        assert rows[symbol]["strategy_approved"] is False
        assert rows[symbol]["can_submit"] is False
        assert rows[symbol]["block_reason"] == "LANE_ADAPTER_MISSING"


def test_live_money_false_and_quantity_cap_constant_for_all_rows(tmp_path: Path) -> None:
    _write_market_data_config(tmp_path)

    artifacts = build_phase1_ticker_readiness_matrix(config=_config(tmp_path))

    assert all(row["live_money_eligible"] is False for row in artifacts.rows)
    assert {row["quantity_cap"] for row in artifacts.rows} == {1.0}


def test_runtime_data_ready_does_not_create_submit_permission_without_strategy_approval(tmp_path: Path) -> None:
    _write_market_data_config(tmp_path)
    _write_governance(tmp_path)
    _write_runtime_artifacts(tmp_path, symbol="GC")

    artifacts = build_phase1_ticker_readiness_matrix(config=_config(tmp_path))
    rows = {row["approved_phase1_symbol"]: row for row in artifacts.rows}

    assert rows["GC"]["runtime_candles_ready"] is True
    assert rows["GC"]["derived_features_ready"] is True
    assert rows["GC"]["runtime_data_block_reason"] == "READY"
    assert rows["GC"]["strategy_approved"] is False
    assert rows["GC"]["can_submit"] is False
    assert rows["GC"]["block_reason"] == "NO_APPROVED_STRATEGY"


def test_historical_seed_does_not_create_live_runtime_or_submit_readiness(tmp_path: Path) -> None:
    _write_market_data_config(tmp_path)
    _write_governance(tmp_path)
    _write_runtime_artifacts(tmp_path, symbol="GC", historical_seed_ready=True, realtime_feed_confirmed=False)

    artifacts = build_phase1_ticker_readiness_matrix(config=_config(tmp_path))
    rows = {row["approved_phase1_symbol"]: row for row in artifacts.rows}

    assert rows["GC"]["historical_seed_ready"] is True
    assert rows["GC"]["realtime_feed_confirmed"] is False
    assert rows["GC"]["runtime_candles_ready"] is False
    assert rows["GC"]["runtime_data_block_reason"] == "REALTIME_FEED_NOT_CONFIRMED"
    assert rows["GC"]["can_submit"] is False
    assert rows["GC"]["block_reason"] == "NO_APPROVED_STRATEGY"
    assert rows["GC"]["paper_candidate_visible"] is True
    assert rows["GC"]["paper_candidate_approved"] is True
    assert rows["GC"]["paper_watch_ready"] is False
    assert rows["GC"]["paper_watch_block_reason"] in {
        "REALTIME_FEED_NOT_CONFIRMED",
        "GC_PAPER_CANDIDATE_NOT_EVALUATION_READY",
    }


def test_gc_candidate_can_become_paper_watch_ready_only_after_mocked_live_route_gates(tmp_path: Path) -> None:
    _write_market_data_config(tmp_path)
    _write_governance(tmp_path, approve_gc=True)
    _write_runtime_artifacts(tmp_path, symbol="GC")

    artifacts = build_phase1_ticker_readiness_matrix(config=_repo_config_with_tmp_artifacts(tmp_path))
    rows = {row["approved_phase1_symbol"]: row for row in artifacts.rows}

    assert rows["GC"]["paper_candidate_visible"] is True
    assert rows["GC"]["paper_candidate_strategy_id"] == "gc_1x_asia_london_participation__asia_london_long_v5"
    assert rows["GC"]["paper_candidate_approved"] is True
    assert rows["GC"]["paper_candidate_evaluation_ready"] is True
    assert rows["GC"]["strategy_approved"] is True
    assert rows["GC"]["guarded_route_authorized"] is True
    assert rows["GC"]["paper_watch_ready"] is True
    assert rows["GC"]["can_submit"] is True
    assert rows["GC"]["block_reason"] == "READY"
    assert rows["GC"]["live_money_eligible"] is False
    assert all(rows[symbol]["paper_candidate_visible"] is False for symbol in PHASE1_TICKER_ORDER if symbol != "GC")
    assert all(rows[symbol]["can_submit"] is False for symbol in PHASE1_TICKER_ORDER if symbol != "GC")


def test_gc_candidate_uses_in_engine_feature_contract_when_generic_features_missing(tmp_path: Path) -> None:
    _write_market_data_config(tmp_path)
    _write_governance(tmp_path, approve_gc=True)
    _write_runtime_artifacts(tmp_path, symbol="GC", include_features=False)

    artifacts = build_phase1_ticker_readiness_matrix(config=_repo_config_with_tmp_artifacts(tmp_path))
    rows = {row["approved_phase1_symbol"]: row for row in artifacts.rows}

    assert rows["GC"]["runtime_candles_ready"] is True
    assert rows["GC"]["derived_features_ready"] is False
    assert rows["GC"]["external_feature_artifacts_required"] is False
    assert rows["GC"]["external_feature_artifacts_ready"] is True
    assert rows["GC"]["runtime_data_ready_for_candidate"] is True
    assert rows["GC"]["paper_candidate_feature_contract_status"] == "IN_ENGINE_BAR_DERIVED_CONTEXT"
    assert rows["GC"]["paper_candidate_required_runtime_feature_artifacts"] == []
    assert rows["GC"]["paper_watch_ready"] is True
    assert rows["GC"]["can_submit"] is True
    assert rows["GC"]["live_money_eligible"] is False
    assert all(rows[symbol]["paper_candidate_visible"] is False for symbol in PHASE1_TICKER_ORDER if symbol != "GC")
    assert all(rows[symbol]["can_submit"] is False for symbol in PHASE1_TICKER_ORDER if symbol != "GC")


def test_strategy_approved_symbol_is_blocked_when_runtime_data_missing(tmp_path: Path) -> None:
    _write_market_data_config(tmp_path)
    _write_governance(tmp_path)
    governance_path = tmp_path / "var" / "per_strategy_paper_status.json"
    payload = json.loads(governance_path.read_text(encoding="utf-8"))
    payload["strategies"][0]["strategy_approved"] = True
    governance_path.write_text(json.dumps(payload), encoding="utf-8")

    artifacts = build_phase1_ticker_readiness_matrix(config=_config(tmp_path))
    rows = {row["approved_phase1_symbol"]: row for row in artifacts.rows}

    assert rows["GC"]["lane_adapter_present"] is True
    assert rows["GC"]["strategy_approved"] is True
    assert rows["GC"]["runtime_candles_ready"] is False
    assert rows["GC"]["can_submit"] is False
    assert rows["GC"]["block_reason"] == "RUNTIME_DATA_NOT_READY"


def test_writes_matrix_artifacts_without_archive_truth(tmp_path: Path) -> None:
    _write_market_data_config(tmp_path)

    config = _config(tmp_path)
    artifacts = build_phase1_ticker_readiness_matrix(config=config)
    write_phase1_ticker_readiness_matrix_artifacts(config=config, artifacts=artifacts)

    output_dir = tmp_path / "outputs" / "reports" / "phase1_ticker_readiness_matrix"
    payload = json.loads((output_dir / "latest_phase1_ticker_readiness_matrix.json").read_text(encoding="utf-8"))
    assert payload["archive_artifact_used"] is False
    assert (output_dir / "latest_phase1_ticker_readiness_matrix.csv").exists()
    assert (output_dir / "latest_phase1_ticker_readiness_matrix.md").exists()
