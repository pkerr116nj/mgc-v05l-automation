from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_live_trade_path_accumulator import (
    accumulate_open_trade_paths,
    finalize_closed_trade_paths,
    run_live_trade_path_accumulator,
)


NOW = datetime(2026, 7, 7, 12, 10, tzinfo=UTC)


def test_open_trade_accumulates_samples_over_multiple_invocations(tmp_path: Path) -> None:
    root = tmp_path / "candles"
    _write_candles(root, "GC", [_bar("2026-07-07T12:01:00Z", high=101, low=99), _bar("2026-07-07T12:02:00Z", high=102, low=98)])
    rows, updates = accumulate_open_trade_paths(
        [],
        managed_positions=_managed_positions(),
        runtime_candle_root=root,
        generated_at=NOW,
    )
    _write_candles(root, "GC", [_bar("2026-07-07T12:02:00Z", high=102, low=98), _bar("2026-07-07T12:03:00Z", high=103, low=97)])
    rows, updates_2 = accumulate_open_trade_paths(
        rows,
        managed_positions=_managed_positions(),
        runtime_candle_root=root,
        generated_at=NOW,
    )

    assert updates == 1
    assert updates_2 == 1
    assert len(rows) == 1
    assert rows[0]["path_sample_count"] == 3
    assert rows[0]["status"] == "OPEN_ACCUMULATING"
    assert rows[0]["path_coverage_status"] == "OPEN_ACCUMULATING"


def test_no_duplicate_samples_on_repeated_invocation(tmp_path: Path) -> None:
    root = tmp_path / "candles"
    _write_candles(root, "GC", [_bar("2026-07-07T12:01:00Z"), _bar("2026-07-07T12:02:00Z")])
    rows, _ = accumulate_open_trade_paths([], managed_positions=_managed_positions(), runtime_candle_root=root, generated_at=NOW)
    rows, updates = accumulate_open_trade_paths(rows, managed_positions=_managed_positions(), runtime_candle_root=root, generated_at=NOW)

    assert updates == 0
    assert rows[0]["path_sample_count"] == 2


def test_closed_trade_finalizes_complete_path_and_mfe_mae(tmp_path: Path) -> None:
    root = tmp_path / "candles"
    _write_candles(root, "GC", [_bar("2026-07-07T12:01:00Z", high=101, low=99), _bar("2026-07-07T12:02:00Z", high=104, low=98)])
    open_rows, _ = accumulate_open_trade_paths([], managed_positions=_managed_positions(), runtime_candle_root=root, generated_at=NOW)

    finalized, count = finalize_closed_trade_paths(
        open_rows,
        previous_finalized=[],
        canonical_records=[_closed_record()],
        generated_at=NOW,
    )

    assert count == 1
    assert finalized[0]["path_coverage_status"] == "COMPLETE"
    assert finalized[0]["mfe"] == 4.0
    assert finalized[0]["mae"] == -2.0
    assert finalized[0]["counterfactual_ready"]["timebox"] is True
    assert finalized[0]["counterfactual_ready"]["trailing"] is True


def test_partial_classifications_are_reported() -> None:
    open_rows = [
        _open_row(
            [
                _sample("2026-07-07T12:02:00Z", high=102, low=99),
                _sample("2026-07-07T12:03:00Z", high=103, low=98),
            ]
        )
    ]
    finalized, _ = finalize_closed_trade_paths(open_rows, previous_finalized=[], canonical_records=[_closed_record()], generated_at=NOW)
    assert finalized[0]["path_coverage_status"] == "PARTIAL_ENTRY_MISSING"

    open_rows = [_open_row([_sample("2026-07-07T12:01:00Z", high=102, low=99)])]
    finalized, _ = finalize_closed_trade_paths(open_rows, previous_finalized=[], canonical_records=[_closed_record()], generated_at=NOW, repair_finalized=True)
    assert finalized[0]["path_coverage_status"] == "PARTIAL_EXIT_MISSING"

    open_rows = [
        _open_row(
            [
                _sample("2026-07-07T12:01:00Z", high=101, low=99),
                _sample("2026-07-07T12:05:00Z", high=104, low=98),
            ]
        )
    ]
    finalized, _ = finalize_closed_trade_paths(open_rows, previous_finalized=[], canonical_records=[_closed_record(exit_time="2026-07-07T12:05:00Z")], generated_at=NOW, repair_finalized=True)
    assert finalized[0]["path_coverage_status"] == "PARTIAL_INTERNAL_GAP"


def test_run_writes_json_jsonl_and_reports(tmp_path: Path) -> None:
    root = tmp_path / "candles"
    _write_candles(root, "GC", [_bar("2026-07-07T12:01:00Z"), _bar("2026-07-07T12:02:00Z")])
    managed = tmp_path / "managed.json"
    canonical = tmp_path / "canonical.jsonl"
    managed.write_text(json.dumps(_managed_positions()), encoding="utf-8")
    canonical.write_text(json.dumps(_closed_record()) + "\n", encoding="utf-8")

    result = run_live_trade_path_accumulator(
        managed_positions_path=managed,
        canonical_records_path=canonical,
        runtime_candle_root=root,
        output_dir=tmp_path / "out",
        now=NOW,
    )

    assert json.loads(result.status_path.read_text())["schema_version"] == "ra8_path_accumulator_status_v1"
    assert len([json.loads(line) for line in result.open_path.read_text().splitlines() if line.strip()]) == 1
    assert len([json.loads(line) for line in result.finalized_path.read_text().splitlines() if line.strip()]) == 1
    assert result.contract_path.exists()
    assert result.finalization_report_path.exists()


def test_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_live_trade_path_accumulator.py"),
        Path("src/mgc_v05l/app/track_b_live_trade_path_accumulator.py"),
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


def _managed_positions() -> dict:
    return {
        "managed_positions": [
            {
                "trade_id": "trade_open_gc",
                "lifecycle_id": "life_gc",
                "symbol": "GC",
                "side": "LONG",
                "entry_time": "2026-07-07T12:00:30Z",
                "entry_price": "100",
                "quantity": "1",
                "classification": "OPEN_MANAGED_MATCHED",
            }
        ]
    }


def _closed_record(*, exit_time: str = "2026-07-07T12:02:00Z") -> dict:
    return {
        "event_type": "CANONICAL_TRADE_RECORD",
        "trade_id": "trade_open_gc",
        "symbol": "GC",
        "side": "LONG",
        "entry_time": "2026-07-07T12:00:30Z",
        "exit_time": exit_time,
        "entry_price": 100.0,
        "exit_price": 102.0,
        "quantity": 1,
        "trade_status": "CLOSED",
    }


def _open_row(samples: list[dict]) -> dict:
    return {
        "accumulator_id": "open_trade_path_x",
        "accumulator_key": "trade_open_gc|life_gc|GC|GC|LONG|2026-07-07T12:00:30Z",
        "source_trade_id": "trade_open_gc",
        "managed_position_id": "life_gc",
        "lifecycle_id": "life_gc",
        "instrument": "GC",
        "contract": "GC",
        "side": "LONG",
        "entry_time": "2026-07-07T12:00:30Z",
        "entry_price": 100.0,
        "quantity": 1,
        "path_samples": samples,
    }


def _write_candles(root: Path, symbol: str, bars: list[dict]) -> None:
    path = root / symbol / "1m" / "latest_runtime_candles.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"bars": bars}), encoding="utf-8")


def _bar(end: str, *, high: float = 101.0, low: float = 99.0) -> dict:
    return {
        "bar_start": (datetime.fromisoformat(end.replace("Z", "+00:00")) - __import__("datetime").timedelta(minutes=1)).isoformat(),
        "bar_end": end,
        "open": 100.0,
        "high": high,
        "low": low,
        "close": 100.0,
        "volume": 1,
        "completed": True,
    }


def _sample(end: str, *, high: float = 101.0, low: float = 99.0) -> dict:
    return {
        "bar_start": (datetime.fromisoformat(end.replace("Z", "+00:00")) - __import__("datetime").timedelta(minutes=1)).isoformat(),
        "bar_end": end.replace("Z", "+00:00"),
        "high": high,
        "low": low,
        "close": 100.0,
    }
