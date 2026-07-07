from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_ctol_ctoe_refresh_controller import (
    build_refresh_diagnosis,
    run_ctol_ctoe_refresh_controller,
)


NOW = datetime(2026, 7, 7, 6, 0, tzinfo=UTC)


def test_detects_stale_ctol_ctoe_when_canonical_records_advance(tmp_path: Path) -> None:
    records = tmp_path / "canonical_trade_records.jsonl"
    ctol = tmp_path / "ctol.jsonl"
    ctoe = tmp_path / "ctoe.jsonl"
    _write_jsonl(records, [_record("a", "2026-07-07T05:00:00Z"), _record("b", "2026-07-07T05:30:00Z")])
    _write_jsonl(ctol, [_outcome("a", "2026-07-07T05:00:00Z")])
    _write_jsonl(ctoe, [_outcome("a", "2026-07-07T05:00:00Z")])

    diagnosis = build_refresh_diagnosis(
        canonical_records_path=records,
        ctol_rows_path=ctol,
        ctoe_rows_path=ctoe,
        generated_at=NOW,
    )

    assert diagnosis["staleness"]["ctol_stale"] is True
    assert "ctol_row_count_lags_completed_canonical_records" in diagnosis["staleness"]["ctol_stale_reasons"]
    assert diagnosis["staleness"]["ctoe_stale"] is True
    assert "ctoe_row_count_lags_completed_canonical_records" in diagnosis["staleness"]["ctoe_stale_reasons"]


def test_noop_when_ctol_ctoe_current(tmp_path: Path) -> None:
    records = tmp_path / "records.jsonl"
    output = tmp_path / "out"
    ctol_dir = tmp_path / "ctol"
    ctoe_dir = tmp_path / "ctoe"
    ctol_dir.mkdir()
    ctoe_dir.mkdir()
    _write_jsonl(records, [_record("a", "2026-07-07T05:00:00Z")])
    _write_jsonl(ctol_dir / "canonical_trade_outcomes.jsonl", [_outcome("a", "2026-07-07T05:00:00Z")])
    _write_jsonl(ctoe_dir / "canonical_trade_outcome_enrichment.jsonl", [_outcome("a", "2026-07-07T05:00:00Z")])

    result = run_ctol_ctoe_refresh_controller(
        canonical_records_path=records,
        side_session_replay_path=tmp_path / "missing_replay.jsonl",
        crfd_rows_path=tmp_path / "missing_crfd.jsonl",
        ctol_output_dir=ctol_dir,
        ctoe_output_dir=ctoe_dir,
        output_dir=output,
        refresh_if_stale=True,
        now=NOW,
    )

    assert result.status["classification"] == "CURRENT_NO_OP"
    assert result.status["refreshed"]["ctol"] is False
    assert result.status["refreshed"]["ctoe"] is False


def test_active_open_position_excluded_from_staleness_count(tmp_path: Path) -> None:
    records = tmp_path / "records.jsonl"
    ctol = tmp_path / "ctol.jsonl"
    ctoe = tmp_path / "ctoe.jsonl"
    _write_jsonl(records, [_record("a", "2026-07-07T05:00:00Z"), _record("open", None, paired=False)])
    _write_jsonl(ctol, [_outcome("a", "2026-07-07T05:00:00Z")])
    _write_jsonl(ctoe, [_outcome("a", "2026-07-07T05:00:00Z")])

    diagnosis = build_refresh_diagnosis(
        canonical_records_path=records,
        ctol_rows_path=ctol,
        ctoe_rows_path=ctoe,
        generated_at=NOW,
    )

    assert diagnosis["source_state"]["completed_paired_record_count"] == 1
    assert diagnosis["source_state"]["active_open_positions_excluded"] == 1
    assert diagnosis["staleness"]["ctol_stale"] is False


def test_refresh_order_ctol_before_ctoe(tmp_path: Path) -> None:
    records = tmp_path / "records.jsonl"
    ctol_dir = tmp_path / "ctol"
    ctoe_dir = tmp_path / "ctoe"
    _write_jsonl(records, [_record("a", "2026-07-07T05:00:00Z")])

    result = run_ctol_ctoe_refresh_controller(
        canonical_records_path=records,
        side_session_replay_path=tmp_path / "missing_replay.jsonl",
        crfd_rows_path=tmp_path / "missing_crfd.jsonl",
        ctol_output_dir=ctol_dir,
        ctoe_output_dir=ctoe_dir,
        output_dir=tmp_path / "out",
        refresh_if_stale=True,
        now=NOW,
    )

    completed = [action["action"] for action in result.status["actions"] if action["status"] == "COMPLETE"]
    assert completed[:2] == ["refresh_ctol", "refresh_ctoe"]
    assert result.status["after"]["staleness"]["ctol_stale"] is False
    assert result.status["after"]["staleness"]["ctoe_stale"] is False


def test_guardrails_and_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_ctol_ctoe_refresh_controller.py"),
        Path("src/mgc_v05l/app/track_b_ctol_ctoe_refresh_controller.py"),
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


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _record(trade_id: str, exit_time: str | None, *, paired: bool = True) -> dict:
    return {
        "trade_id": trade_id,
        "lane_id": "lane_a",
        "symbol": "GC",
        "local_symbol": "GCQ6",
        "side": "LONG",
        "entry_time": "2026-07-07T04:45:00Z",
        "exit_time": exit_time,
        "entry_price": 100.0,
        "exit_price": 101.0 if exit_time else None,
        "realized_pnl_points": 1.0 if exit_time else None,
        "realized_pnl_currency": 100.0 if exit_time else None,
        "qty": 1,
        "pairing_status": "PAIRED" if paired else "UNPAIRED_ENTRY",
        "trade_status": "CLOSED" if paired else "OPEN_OR_UNPAIRED",
    }


def _outcome(trade_id: str, exit_time: str) -> dict:
    return {
        "trade_outcome_id": f"outcome_{trade_id}",
        "instrument": "GC",
        "contract": "GCQ6",
        "entry_time": "2026-07-07T04:45:00Z",
        "exit_time": exit_time,
        "realized_pnl_proxy": 100.0,
    }
