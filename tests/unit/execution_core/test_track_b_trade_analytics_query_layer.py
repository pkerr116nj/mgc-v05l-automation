from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_trade_analytics_query_layer import (
    build_trade_analytics_scorecard,
    run_trade_analytics_query_layer,
)


NOW = datetime(2026, 7, 2, 12, 0, tzinfo=UTC)


def test_paired_trade_construction() -> None:
    report = build_trade_analytics_scorecard(
        [_trade("lane_a", pnl=3.0), _trade("lane_a", pnl=-1.0), _trade("lane_b", pairing="UNPAIRED_ENTRY", pnl=None)],
        pairing_summary={"total_entries": 3, "total_exits": 2, "paired_trades": 2, "pairing_rate": 2 / 3},
        generated_at=NOW,
    )

    assert report["overall"]["canonical_record_count"] == 3
    assert report["overall"]["paired_trade_count"] == 2
    assert report["overall"]["pairing_rate"] == 2 / 3


def test_strategy_scorecard_aggregation() -> None:
    report = build_trade_analytics_scorecard(
        [_trade("lane_a", pnl=3.0), _trade("lane_a", pnl=-1.0), _trade("lane_b", pnl=2.0)],
        generated_at=NOW,
    )
    groups = {row["key"]: row for row in report["views"]["strategy_performance_view"]["groups"]}

    assert groups["lane_a"]["trade_count"] == 2
    assert groups["lane_a"]["expectancy_proxy"] == 1.0
    assert groups["lane_a"]["win_rate"] == 0.5


def test_exit_quality_aggregation() -> None:
    report = build_trade_analytics_scorecard(
        [_trade("lane_a", pnl=3.0, exit_reason="TIMEBOX"), _trade("lane_b", pnl=-4.0, exit_reason="TIMEBOX")],
        generated_at=NOW,
    )
    groups = {row["key"]: row for row in report["views"]["exit_quality_view"]["groups"]}

    assert groups["TIMEBOX"]["trade_count"] == 2
    assert groups["TIMEBOX"]["expectancy_proxy"] == -0.5


def test_data_quality_warnings() -> None:
    report = build_trade_analytics_scorecard(
        [_trade("lane_a", pnl=None, mfe=None, mae=None)],
        pairing_summary={"unpaired_entry_count": 1, "unpaired_exit_count": 0},
        generated_at=NOW,
    )

    warnings = report["views"]["data_quality_view"]["warnings"]
    assert "missing_pnl_proxy" in warnings
    assert "missing_mfe_mae" in warnings
    assert "unpaired_trade_records_present" in warnings


def test_missing_optional_joins_degrade_gracefully() -> None:
    report = build_trade_analytics_scorecard([_trade("lane_a", pnl=1.0)], crfd_rows=[], generated_at=NOW)

    assert report["views"]["regime_crfd_join_view"]["group_count"] == 0
    assert "CRFD/regime joins unavailable." in report["limitations"]


def test_empty_trade_dataset_handled_safely(tmp_path: Path) -> None:
    records = tmp_path / "records.jsonl"
    records.write_text("", encoding="utf-8")
    result = run_trade_analytics_query_layer(
        canonical_records_path=records,
        pairing_summary_path=tmp_path / "missing.json",
        side_session_replay_path=tmp_path / "missing_replay.jsonl",
        blocked_intents_path=tmp_path / "missing_blocked.jsonl",
        crfd_rows_path=tmp_path / "missing_crfd.jsonl",
        output_dir=tmp_path / "out",
        now=NOW,
    )

    assert result.report["overall"]["canonical_record_count"] == 0
    assert result.json_path.exists()
    assert result.contract_path.exists()


def test_trade_analytics_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_trade_analytics_query_layer.py"),
        Path("src/mgc_v05l/app/track_b_trade_analytics_query_layer.py"),
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


def _trade(
    lane: str,
    *,
    pnl: float | None,
    pairing: str = "PAIRED",
    mfe: float | None = 5.0,
    mae: float | None = -2.0,
    exit_reason: str = "TIMEBOX",
) -> dict:
    return {
        "event_type": "CANONICAL_TRADE_RECORD",
        "trade_status": "CLOSED" if pairing == "PAIRED" else "OPEN_OR_UNPAIRED",
        "pairing_status": pairing,
        "lane_id": lane,
        "strategy_id": f"{lane}_strategy",
        "symbol": "GC",
        "side": "LONG",
        "entry_time": "2026-07-01T10:00:00Z",
        "exit_time": "2026-07-01T10:30:00Z" if pairing == "PAIRED" else None,
        "realized_pnl_points": pnl,
        "mfe_points": mfe,
        "mae_points": mae,
        "hold_seconds": 1800,
        "session_label": "LONDON",
        "exit_reason": exit_reason,
        "exit_policy": exit_reason,
        "trade_id": f"{lane}_{pnl}_{pairing}",
    }
