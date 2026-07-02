from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_trade_outcome_layer import (
    build_trade_outcome_summary,
    build_trade_outcomes,
    run_trade_outcome_layer,
)


NOW = datetime(2026, 7, 2, 12, 0, tzinfo=UTC)


def test_paired_trade_becomes_outcome_record() -> None:
    outcomes = build_trade_outcomes([_trade("lane_a", side="LONG", entry=100.0, exit=104.0)], generated_at=NOW)

    assert len(outcomes) == 1
    outcome = outcomes[0]
    assert outcome["schema_version"] == "track_b_canonical_trade_outcome_v1"
    assert outcome["lane_id"] == "lane_a"
    assert outcome["realized_points"] == 4.0
    assert outcome["diagnostic_only"] is True


def test_long_realized_points_from_prices_when_source_points_missing() -> None:
    row = _trade("lane_a", side="LONG", entry=100.0, exit=104.0, realized_points=None)
    outcomes = build_trade_outcomes([row], generated_at=NOW)

    assert outcomes[0]["realized_points"] == 4.0
    assert "realized_points_computed_from_prices" in outcomes[0]["data_quality_flags"]


def test_short_realized_points_from_prices_when_source_points_missing() -> None:
    row = _trade("lane_a", side="SHORT", entry=100.0, exit=94.0, realized_points=None)
    outcomes = build_trade_outcomes([row], generated_at=NOW)

    assert outcomes[0]["realized_points"] == 6.0
    assert "realized_points_computed_from_prices" in outcomes[0]["data_quality_flags"]


def test_missing_pnl_proxy_fields_are_flagged() -> None:
    row = _trade("lane_a", side="LONG", entry=100.0, exit=104.0, currency=None)
    outcomes = build_trade_outcomes([row], generated_at=NOW)

    assert outcomes[0]["realized_pnl_proxy"] is None
    assert "missing_pnl_proxy" in outcomes[0]["data_quality_flags"]


def test_missing_mfe_mae_are_flagged() -> None:
    row = _trade("lane_a", side="LONG", entry=100.0, exit=104.0, mfe=None, mae=None)
    outcomes = build_trade_outcomes([row], generated_at=NOW)

    assert outcomes[0]["mfe_points"] is None
    assert outcomes[0]["mae_points"] is None
    assert "missing_mfe" in outcomes[0]["data_quality_flags"]
    assert "missing_mae" in outcomes[0]["data_quality_flags"]


def test_unpaired_records_excluded_and_reported_in_summary() -> None:
    records = [_trade("lane_a", side="LONG", entry=100.0, exit=104.0), _trade("lane_b", pairing="UNPAIRED_ENTRY")]
    outcomes = build_trade_outcomes(records, generated_at=NOW)
    summary = build_trade_outcome_summary(outcomes, canonical_records=records, generated_at=NOW)

    assert len(outcomes) == 1
    assert summary["overall"]["incomplete_or_unpaired_count"] == 1
    assert summary["data_quality"]["unpaired_or_incomplete_examples"][0]["lane_id"] == "lane_b"


def test_summary_aggregation() -> None:
    records = [
        _trade("lane_a", side="LONG", entry=100.0, exit=104.0, realized_points=4.0, currency=40.0, mfe=8.0, mae=-2.0),
        _trade("lane_b", side="SHORT", entry=100.0, exit=96.0, realized_points=4.0, currency=20.0, mfe=5.0, mae=-1.0),
    ]
    outcomes = build_trade_outcomes(records, generated_at=NOW)
    summary = build_trade_outcome_summary(outcomes, canonical_records=records, generated_at=NOW)

    assert summary["overall"]["outcome_count"] == 2
    assert summary["overall"]["average_realized_points"] == 4.0
    assert summary["overall"]["average_pnl_proxy"] == 30.0
    assert summary["coverage"]["strategy_count"] == 2


def test_empty_dataset_handled_safely(tmp_path: Path) -> None:
    records = tmp_path / "records.jsonl"
    records.write_text("", encoding="utf-8")
    result = run_trade_outcome_layer(
        canonical_records_path=records,
        side_session_replay_path=tmp_path / "missing_replay.jsonl",
        crfd_rows_path=tmp_path / "missing_crfd.jsonl",
        output_dir=tmp_path / "out",
        now=NOW,
    )

    assert result.outcomes == []
    assert result.summary["overall"]["outcome_count"] == 0
    assert result.outcomes_path.exists()
    assert result.summary_path.exists()


def test_trade_outcome_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_trade_outcome_layer.py"),
        Path("src/mgc_v05l/app/track_b_trade_outcome_layer.py"),
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
    side: str = "LONG",
    entry: float = 100.0,
    exit: float = 104.0,
    realized_points: float | None = 4.0,
    currency: float | None = 40.0,
    mfe: float | None = 6.0,
    mae: float | None = -2.0,
    pairing: str = "PAIRED",
) -> dict:
    return {
        "event_type": "CANONICAL_TRADE_RECORD",
        "trade_status": "CLOSED" if pairing == "PAIRED" else "OPEN_OR_UNPAIRED",
        "pairing_status": pairing,
        "lane_id": lane,
        "strategy_id": f"{lane}_strategy",
        "symbol": "GC",
        "local_symbol": "GCQ6",
        "side": side,
        "entry_time": "2026-07-01T10:00:00Z",
        "exit_time": "2026-07-01T10:30:00Z" if pairing == "PAIRED" else None,
        "entry_price": entry,
        "exit_price": exit,
        "realized_pnl_points": realized_points,
        "realized_pnl_currency": currency,
        "mfe_points": mfe,
        "mae_points": mae,
        "hold_seconds": 1800,
        "session_label": "LONDON",
        "exit_reason": "TIMEBOX",
        "exit_policy": "TIMEBOX",
        "trade_id": f"{lane}_{side}_{realized_points}_{pairing}",
    }
