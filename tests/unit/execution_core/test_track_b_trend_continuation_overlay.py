from __future__ import annotations

import inspect
import json
from pathlib import Path

from mgc_v05l.execution_core import track_b_trend_continuation_overlay as overlay


def test_bullish_continuation_long(tmp_path: Path) -> None:
    _write_canonical(
        tmp_path,
        [
            _trade(
                lane_id="es_us_active_participation_long",
                symbol="ES",
                side="LONG",
                session_label="US",
                entry_price="100",
                exit_price="106",
            )
        ],
    )
    _write_side_replay(
        tmp_path,
        "es_us_active_participation_long",
        "ES",
        "LONG",
        [
            _bar("2026-06-24T00:05:00Z", "100", "102", "100", "102"),
            _bar("2026-06-24T00:10:00Z", "102", "104", "101", "104"),
            _bar("2026-06-24T00:15:00Z", "104", "106", "103", "106"),
        ],
        mfe="6",
        mae="0",
    )

    result = overlay.build_trend_continuation_overlay(repo_root=tmp_path)
    row = _read_jsonl(result.scores_path)[0]

    assert row["continuation_state"] == "CONTINUATION_CONFIRMED"
    assert row["features"]["trend_direction_alignment"] == "ALIGNED_WITH_TRADE"
    assert row["cohort"] == "us_longs"


def test_bearish_continuation_short(tmp_path: Path) -> None:
    _write_canonical(
        tmp_path,
        [
            _trade(
                lane_id="nq_london_late_active_participation_short",
                symbol="NQ",
                side="SHORT",
                session_label="LONDON_LATE",
                entry_price="100",
                exit_price="94",
            )
        ],
    )
    _write_side_replay(
        tmp_path,
        "nq_london_late_active_participation_short",
        "NQ",
        "SHORT",
        [
            _bar("2026-06-24T00:05:00Z", "100", "100", "98", "98"),
            _bar("2026-06-24T00:10:00Z", "98", "99", "96", "96"),
            _bar("2026-06-24T00:15:00Z", "96", "97", "94", "94"),
        ],
        mfe="6",
        mae="0",
    )

    result = overlay.build_trend_continuation_overlay(repo_root=tmp_path)
    row = _read_jsonl(result.scores_path)[0]

    assert row["continuation_state"] == "CONTINUATION_CONFIRMED"
    assert row["features"]["structure_ratio"] == "1"
    assert row["cohort"] == "london_late_shorts"


def test_impulse_only_failure(tmp_path: Path) -> None:
    _write_canonical(
        tmp_path,
        [
            _trade(
                lane_id="gc_us_active_participation_long",
                symbol="GC",
                side="LONG",
                session_label="US",
                entry_price="100",
                exit_price="100",
            )
        ],
    )
    _write_side_replay(
        tmp_path,
        "gc_us_active_participation_long",
        "GC",
        "LONG",
        [
            _bar("2026-06-24T00:05:00Z", "100", "110", "100", "108"),
            _bar("2026-06-24T00:10:00Z", "108", "109", "101", "101"),
            _bar("2026-06-24T00:15:00Z", "101", "102", "99", "100"),
        ],
        mfe="10",
        mae="-1",
    )

    result = overlay.build_trend_continuation_overlay(repo_root=tmp_path)
    row = _read_jsonl(result.scores_path)[0]

    assert row["continuation_state"] == "IMPULSE_ONLY"
    assert row["features"]["giveback_from_mfe_to_exit_ratio"] == "1"


def test_chop_or_no_edge(tmp_path: Path) -> None:
    _write_canonical(
        tmp_path,
        [
            _trade(
                lane_id="mgc_globex_active_participation_long",
                symbol="MGC",
                side="LONG",
                session_label="GLOBEX",
                entry_price="100",
                exit_price="100",
            )
        ],
    )
    _write_side_replay(
        tmp_path,
        "mgc_globex_active_participation_long",
        "MGC",
        "LONG",
        [
            _bar("2026-06-24T00:05:00Z", "100", "101", "99", "100"),
            _bar("2026-06-24T00:10:00Z", "100", "101", "99", "100"),
            _bar("2026-06-24T00:15:00Z", "100", "101", "99", "100"),
        ],
        mfe="1",
        mae="-1",
    )

    result = overlay.build_trend_continuation_overlay(repo_root=tmp_path)
    row = _read_jsonl(result.scores_path)[0]

    assert row["continuation_state"] == "CHOP_OR_NO_EDGE"


def test_insufficient_path_data(tmp_path: Path) -> None:
    _write_canonical(
        tmp_path,
        [
            _trade(
                lane_id="mbt_us_active_participation_long",
                symbol="MBT",
                side="LONG",
                session_label="US",
                entry_price="100",
                exit_price="101",
            )
        ],
    )

    result = overlay.build_trend_continuation_overlay(repo_root=tmp_path)
    row = _read_jsonl(result.scores_path)[0]

    assert row["continuation_state"] == "INSUFFICIENT_PATH_DATA"
    assert row["path_status"] == "INSUFFICIENT_PATH_DATA"


def test_module_has_no_broker_runtime_or_managed_exit_imports() -> None:
    source = inspect.getsource(overlay)
    forbidden = (
        "ib_insync",
        "ibapi",
        "socket",
        "subprocess",
        "ibkr_paper_strategy_bridge",
        "track_b_managed_exit_service",
        "track_b_managed_exit_actuator",
        "probationary_runtime",
        "placeOrder",
        "cancel_order",
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
) -> dict[str, object]:
    return {
        "event_type": "CANONICAL_TRADE_RECORD",
        "pairing_status": "PAIRED",
        "trade_status": "CLOSED",
        "trade_id": f"trade_{lane_id}",
        "lifecycle_id": f"life_{lane_id}",
        "lane_id": lane_id,
        "strategy_id": f"strategy_{lane_id}",
        "strategy_family": "paper_active_evidence",
        "variant_id": f"{lane_id}_v1",
        "entry_thesis": "generic_active_participation",
        "symbol": symbol,
        "local_symbol": f"{symbol}U6",
        "session_label": session_label,
        "side": side,
        "entry_time": "2026-06-24T00:00:00Z",
        "exit_time": "2026-06-24T00:15:00Z",
        "entry_price": entry_price,
        "exit_price": exit_price,
        "exit_policy": "TEST_TIMEBOX_EXIT",
    }


def _write_canonical(tmp_path: Path, rows: list[dict[str, object]]) -> None:
    path = tmp_path / "outputs" / "track_b_execution_core" / "strategy_performance" / "canonical_trade_records.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _write_side_replay(
    tmp_path: Path,
    lane_id: str,
    symbol: str,
    side: str,
    bars: list[dict[str, object]],
    *,
    mfe: str,
    mae: str,
) -> None:
    row = {
        "event_type": "SIDE_SESSION_ATTRIBUTION_TRADE_REPLAY",
        "trade_id": f"trade_{lane_id}",
        "lifecycle_id": f"life_{lane_id}",
        "lane_id": lane_id,
        "symbol": symbol,
        "side": side,
        "entry_time": "2026-06-24T00:00:00Z",
        "exit_time": "2026-06-24T00:15:00Z",
        "mfe_points": mfe,
        "mae_points": mae,
        "time_to_mfe_seconds": 900,
        "time_to_mae_seconds": 300,
        "entry_to_exit_path": bars,
    }
    path = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "strategy_performance"
        / "side_session_attribution"
        / "side_session_trade_replay.jsonl"
    )
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(row) + "\n", encoding="utf-8")


def _bar(end: str, open_price: str, high: str, low: str, close: str) -> dict[str, object]:
    return {
        "bar_end": end,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
    }


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
