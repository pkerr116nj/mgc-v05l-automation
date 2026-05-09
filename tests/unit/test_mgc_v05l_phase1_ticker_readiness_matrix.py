from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.phase1_ticker_readiness_matrix import (
    PHASE1_TICKER_ORDER,
    Phase1TickerReadinessMatrixConfig,
    build_phase1_ticker_readiness_matrix,
    write_phase1_ticker_readiness_matrix_artifacts,
)


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


def _write_governance(root: Path) -> None:
    path = root / "var" / "per_strategy_paper_status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "strategies": [
                    {
                        "strategy_id": "gc_1x_asia_london_participation__asia_london_long_v5",
                        "instrument": "GC",
                        "submit_allowed": True,
                        "strategy_status": "WATCHLIST",
                    },
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


def _config(root: Path) -> Phase1TickerReadinessMatrixConfig:
    return Phase1TickerReadinessMatrixConfig(
        repo_root=root,
        output_dir=Path("outputs") / "reports" / "phase1_ticker_readiness_matrix",
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
