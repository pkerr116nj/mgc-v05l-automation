from __future__ import annotations

import inspect
import json
from pathlib import Path

from mgc_v05l.execution_core import track_b_side_session_attribution_replay as replay


def test_replay_computes_long_path_horizons_and_giveback(tmp_path: Path) -> None:
    _write_canonical(
        tmp_path,
        [
            _trade(
                lane_id="gc_us_active_participation_long",
                symbol="GC",
                side="LONG",
                session_label="US",
                entry_price="100",
                exit_price="99",
                entry_time="2026-06-24T00:00:00Z",
                exit_time="2026-06-24T00:20:00Z",
            )
        ],
    )
    _write_1m_candles(
        tmp_path,
        "GC",
        [
            ("2026-06-24T00:05:00Z", "104", "101", "103"),
            ("2026-06-24T00:10:00Z", "105", "100", "104"),
            ("2026-06-24T00:15:00Z", "102", "98", "99"),
            ("2026-06-24T00:20:00Z", "101", "97", "99"),
        ],
    )

    result = replay.build_side_session_attribution_replay(repo_root=tmp_path, write_artifacts=True)
    row = _read_jsonl(result.trade_replay_path)[0]

    assert row["cohort"] == "us_longs"
    assert row["mfe_points"] == "5"
    assert row["mae_points"] == "-3"
    assert row["time_to_mfe_seconds"] == 600
    assert row["time_to_mae_seconds"] == 1200
    assert row["entry_to_exit_path"][0]["close_pnl_points"] == "3"
    assert row["horizon_pnl"]["5m"]["pnl_points"] == "3"
    assert row["horizon_pnl"]["15m"]["pnl_points"] == "-1"
    assert row["profitable_before_exit_but_gave_back"] is True
    assert row["exit_policy_classification"] == "EXIT_POLICY_LOOKS_BAD"
    assert result.summary["cohorts"]["us_longs"]["exit_policy_bad_count"] == 1


def test_replay_computes_short_path_and_entry_bad(tmp_path: Path) -> None:
    _write_canonical(
        tmp_path,
        [
            _trade(
                lane_id="nq_us_active_participation_short",
                symbol="NQ",
                side="SHORT",
                session_label="US",
                entry_price="100",
                exit_price="103",
                entry_time="2026-06-24T00:00:00Z",
                exit_time="2026-06-24T00:10:00Z",
            )
        ],
    )
    _write_1m_candles(
        tmp_path,
        "NQ",
        [
            ("2026-06-24T00:05:00Z", "103", "101", "102"),
            ("2026-06-24T00:10:00Z", "104", "102", "103"),
        ],
    )

    result = replay.build_side_session_attribution_replay(repo_root=tmp_path)
    row = _read_jsonl(result.trade_replay_path)[0]

    assert row["cohort"] == "us_shorts"
    assert row["mfe_points"] == "-1"
    assert row["mae_points"] == "-4"
    assert row["entry_quality_classification"] == "ENTRY_LOOKS_BAD"
    lane = result.summary["lanes"]["nq_us_active_participation_short"]
    assert lane["entry_bad_count"] == 1
    assert result.summary["lanes_where_entry_looks_bad"] == []


def test_forward_capture_keeps_recent_1m_and_5m_candles(tmp_path: Path) -> None:
    _write_canonical(
        tmp_path,
        [
            _trade(
                lane_id="mbt_globex_active_participation_long",
                symbol="MBT",
                side="LONG",
                session_label="GLOBEX",
                entry_price="100",
                exit_price="101",
                entry_time="2026-06-24T00:00:00Z",
                exit_time="2026-06-24T00:05:00Z",
            )
        ],
    )
    _write_1m_candles(tmp_path, "MBT", [("2026-06-24T00:05:00Z", "101", "99", "101")])
    _write_candle_file(tmp_path, "MBT", "5m", [("2026-06-24T00:05:00Z", "101", "99", "101")])

    result = replay.build_side_session_attribution_replay(repo_root=tmp_path)
    captures = _read_jsonl(result.forward_capture_path)

    assert {(row["symbol"], row["timeframe"]) for row in captures} == {("MBT", "1m"), ("MBT", "5m")}
    assert all(row["event_type"] == "FORWARD_PATH_CANDLE_CAPTURE" for row in captures)
    assert all(row["bars"] for row in captures)


def test_forward_capture_retains_sixty_minute_path_window(tmp_path: Path) -> None:
    _write_canonical(
        tmp_path,
        [
            _trade(
                lane_id="met_us_active_participation_long",
                symbol="MET",
                side="LONG",
                session_label="US",
                entry_price="100",
                exit_price="101",
                entry_time="2026-06-24T00:00:00Z",
                exit_time="2026-06-24T00:05:00Z",
            )
        ],
    )
    bars = [(f"2026-06-24T00:{minute:02d}:00Z", "101", "99", "100") for minute in range(60)]
    _write_1m_candles(tmp_path, "MET", bars)

    result = replay.build_side_session_attribution_replay(repo_root=tmp_path)
    row = next(item for item in _read_jsonl(result.forward_capture_path) if item["timeframe"] == "1m")

    assert row["bar_count"] == 60
    assert len(row["bars"]) == 60


def test_module_has_no_broker_runtime_or_strategy_mutation_imports() -> None:
    source = inspect.getsource(replay)
    forbidden = (
        "ib_insync",
        "ibapi",
        "socket",
        "subprocess",
        "ibkr_paper_strategy_bridge",
        "track_b_managed_exit_service",
        "track_b_managed_exit_actuator",
        "probationary_runtime",
    )
    for token in forbidden:
        assert token not in source


def _trade(
    *,
    lane_id: str,
    symbol: str,
    side: str,
    session_label: str,
    entry_price: str,
    exit_price: str,
    entry_time: str,
    exit_time: str,
) -> dict[str, object]:
    return {
        "event_type": "CANONICAL_TRADE_RECORD",
        "pairing_status": "PAIRED",
        "trade_status": "CLOSED",
        "trade_id": f"trade_{lane_id}",
        "lifecycle_id": f"life_{lane_id}",
        "lane_id": lane_id,
        "symbol": symbol,
        "local_symbol": f"{symbol}U6",
        "session_label": session_label,
        "side": side,
        "entry_time": entry_time,
        "exit_time": exit_time,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "realized_pnl_currency": "0",
    }


def _write_canonical(tmp_path: Path, rows: list[dict[str, object]]) -> None:
    path = tmp_path / "outputs" / "track_b_execution_core" / "strategy_performance" / "canonical_trade_records.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _write_1m_candles(tmp_path: Path, symbol: str, rows: list[tuple[str, str, str, str]]) -> None:
    _write_candle_file(tmp_path, symbol, "1m", rows)


def _write_candle_file(tmp_path: Path, symbol: str, timeframe: str, rows: list[tuple[str, str, str, str]]) -> None:
    bars = [
        {
            "bar_end": end,
            "completed": True,
            "high": high,
            "low": low,
            "close": close,
        }
        for end, high, low, close in rows
    ]
    path = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "phase1_runtime_market_data"
        / symbol
        / timeframe
        / "latest_runtime_candles.json"
    )
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"bars": bars}), encoding="utf-8")


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
