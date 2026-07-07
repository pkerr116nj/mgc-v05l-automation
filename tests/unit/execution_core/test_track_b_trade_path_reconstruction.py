from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_trade_path_reconstruction import (
    build_trade_path_reconstruction_summary,
    build_trade_path_reconstructions,
    merge_retained_path_captures,
    run_trade_path_reconstruction,
)


NOW = datetime(2026, 7, 7, 12, 0, tzinfo=UTC)


def test_reconstructs_entry_path_and_mfe_mae_for_long_trade() -> None:
    rows = build_trade_path_reconstructions(
        [_outcome("t1", side="LONG", entry_price=100.0, exit_price=102.0)],
        replay_rows=[_replay("t1", side="LONG")],
        generated_at=NOW,
    )

    row = rows[0]
    assert row["entry_to_exit_path_status"] == "AVAILABLE"
    assert row["mfe_points"] == 5.0
    assert row["mae_points"] == -2.0
    assert row["time_to_mfe_seconds"] == 600
    assert row["time_to_mae_seconds"] == 120
    assert row["profitable_before_exit"] is True
    assert row["counterfactual_readiness"]["mfe_mae"] == "READY"
    assert row["diagnostic_only"] is True
    assert row["production_recommendation"] is False
    assert row["trading_gate"] is False


def test_reconstructs_post_exit_forward_windows() -> None:
    rows = build_trade_path_reconstructions(
        [_outcome("t1", side="LONG", entry_price=100.0, exit_price=102.0, exit_time="2026-07-07T01:00:00Z")],
        replay_rows=[_replay("t1", side="LONG")],
        forward_capture_rows=[
            _forward_capture(
                "GC",
                [
                    ("2026-07-07T01:05:00Z", 104.0, 101.0, 103.0),
                    ("2026-07-07T01:30:00Z", 106.0, 102.0, 105.0),
                    ("2026-07-07T02:00:00Z", 107.0, 103.0, 106.0),
                ],
            )
        ],
        generated_at=NOW,
    )

    windows = rows[0]["post_exit_forward_windows"]
    assert windows["15m"]["available"] is True
    assert windows["15m"]["pnl_points"] == 3.0
    assert windows["60m"]["available"] is True
    assert windows["60m"]["best_favorable_points"] == 7.0
    assert rows[0]["counterfactual_readiness"]["timebox_grid"] == "READY"


def test_missing_market_data_is_reported_without_failure() -> None:
    rows = build_trade_path_reconstructions([_outcome("t1")], generated_at=NOW)
    row = rows[0]

    assert row["entry_to_exit_path_status"] == "MISSING"
    assert "missing_entry_to_exit_path" in row["data_quality_flags"]
    assert row["counterfactual_readiness"]["timebox_grid"] == "BLOCKED_MISSING_ENTRY_PATH_OR_FORWARD_PATH"


def test_summary_reports_coverage_and_lane_readiness() -> None:
    rows = build_trade_path_reconstructions(
        [
            _outcome("t1", lane="lane_a", entry_price=100.0, exit_price=102.0),
            _outcome("t2", lane="lane_b", entry_price=100.0, exit_price=99.0),
        ],
        replay_rows=[_replay("t1")],
        generated_at=NOW,
    )
    summary = build_trade_path_reconstruction_summary(rows, generated_at=NOW)

    assert summary["overall"]["completed_trades"] == 2
    assert summary["overall"]["entry_to_exit_path_available_count"] == 1
    assert summary["overall"]["mfe_coverage"] == 0.5
    assert summary["lane_path_coverage"][0]["lane_id"] == "lane_a"


def test_retained_path_capture_is_reused_when_current_replay_missing() -> None:
    first = build_trade_path_reconstructions(
        [_outcome("t1")],
        replay_rows=[_replay("t1")],
        generated_at=NOW,
    )
    retained = merge_retained_path_captures([], first, generated_at=NOW)
    second = build_trade_path_reconstructions(
        [_outcome("t1")],
        retained_path_rows=retained,
        generated_at=NOW,
    )

    assert len(retained) == 1
    assert second[0]["entry_to_exit_path_status"] == "AVAILABLE"
    assert second[0]["entry_to_exit_path_source"] == "RETAINED_CAPTURE"
    assert second[0]["mfe_points"] == 5.0


def test_retained_path_capture_is_idempotent() -> None:
    rows = build_trade_path_reconstructions([_outcome("t1")], replay_rows=[_replay("t1")], generated_at=NOW)
    first = merge_retained_path_captures([], rows, generated_at=NOW)
    second = merge_retained_path_captures(first, rows, generated_at=NOW)

    assert len(first) == 1
    assert len(second) == 1
    assert first[0]["deterministic_fingerprint"] == second[0]["deterministic_fingerprint"]


def test_run_writes_json_and_jsonl(tmp_path: Path) -> None:
    outcomes = tmp_path / "outcomes.jsonl"
    replay = tmp_path / "replay.jsonl"
    forward = tmp_path / "forward.jsonl"
    outcomes.write_text(json.dumps(_outcome("t1")) + "\n", encoding="utf-8")
    replay.write_text(json.dumps(_replay("t1")) + "\n", encoding="utf-8")
    forward.write_text(json.dumps(_forward_capture("GC", [("2026-07-07T01:05:00Z", 104.0, 101.0, 103.0)])) + "\n", encoding="utf-8")

    result = run_trade_path_reconstruction(
        outcomes_path=outcomes,
        side_session_replay_path=replay,
        forward_capture_path=forward,
        output_dir=tmp_path / "out",
        now=NOW,
    )

    assert json.loads(result.summary_json_path.read_text())["schema_version"] == "trade_path_reconstruction_summary_v1"
    assert len([json.loads(line) for line in result.path_jsonl_path.read_text().splitlines() if line.strip()]) == 1
    assert len([json.loads(line) for line in result.retained_path_capture_path.read_text().splitlines() if line.strip()]) == 1


def test_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_trade_path_reconstruction.py"),
        Path("src/mgc_v05l/app/track_b_trade_path_reconstruction.py"),
    ]
    forbidden_import_roots = (
        "mgc_v05l.execution.",
        "mgc_v05l.strategy",
        "mgc_v05l.app.ibkr",
        "ibapi",
        "ib_insync",
    )
    forbidden_call_names = {"submit", "cancel", "modify", "placeOrder", "create_order_intent", "mutate_lifecycle", "flatten"}
    violations: list[str] = []
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith(forbidden_import_roots):
                        violations.append(f"{path}:{alias.name}")
            elif isinstance(node, ast.ImportFrom) and node.module:
                if node.module.startswith(forbidden_import_roots):
                    violations.append(f"{path}:{node.module}")
            elif isinstance(node, ast.Call):
                call_name = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else None
                if call_name in forbidden_call_names:
                    violations.append(f"{path}:{call_name}")
    assert violations == []


def _outcome(
    trade_id: str,
    *,
    lane: str = "gc_us_active_participation_long",
    side: str = "LONG",
    entry_price: float = 100.0,
    exit_price: float = 102.0,
    exit_time: str = "2026-07-07T00:20:00Z",
) -> dict:
    return {
        "trade_outcome_id": f"outcome_{trade_id}",
        "lane_id": lane,
        "strategy_id": "strategy",
        "instrument": "GC",
        "contract": "GCQ6",
        "side": side,
        "session_at_entry": "US",
        "entry_time": "2026-07-07T00:00:00Z",
        "exit_time": exit_time,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "realized_points": exit_price - entry_price if side == "LONG" else entry_price - exit_price,
        "realized_pnl_proxy": 200.0,
        "source_refs": {"source_trade_id": trade_id},
    }


def _replay(trade_id: str, *, side: str = "LONG") -> dict:
    return {
        "trade_id": trade_id,
        "symbol": "GC",
        "lane_id": "gc_us_active_participation_long",
        "side": side,
        "entry_time": "2026-07-07T00:00:00Z",
        "exit_time": "2026-07-07T00:20:00Z",
        "time_to_mfe_seconds": 600,
        "time_to_mae_seconds": 120,
        "entry_to_exit_path": [
            {
                "bar_end": "2026-07-07T00:02:00Z",
                "high": "101",
                "low": "98",
                "close": "99",
                "favorable_excursion_points": "1",
                "adverse_excursion_points": "-2",
                "close_pnl_points": "-1",
            },
            {
                "bar_end": "2026-07-07T00:10:00Z",
                "high": "105",
                "low": "100",
                "close": "104",
                "favorable_excursion_points": "5",
                "adverse_excursion_points": "0",
                "close_pnl_points": "4",
            },
        ],
    }


def _forward_capture(symbol: str, bars: list[tuple[str, float, float, float]]) -> dict:
    return {
        "event_type": "FORWARD_PATH_CANDLE_CAPTURE",
        "symbol": symbol,
        "timeframe": "1m",
        "bars": [
            {
                "bar_end": end,
                "high": high,
                "low": low,
                "close": close,
            }
            for end, high, low, close in bars
        ],
    }
