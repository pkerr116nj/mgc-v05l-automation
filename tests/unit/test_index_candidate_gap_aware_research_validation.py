from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from mgc_v05l.app.index_candidate_gap_aware_research_validation import (
    IndexCandidateValidationConfig,
    _annotate_trade,
    _derive_decision_bars,
    _load_research_1m_rows,
    _parse_targets,
    _robustness_classification,
    _session_mask_from_report,
    _summarize_trade_rows,
)


def _write_partition(root: Path, *, symbol: str = "MNQ") -> Path:
    directory = (
        root
        / "outputs"
        / "reports"
        / "trend_participation_engine"
        / "raw_bars"
        / "databento_minute_backfill"
        / f"symbol={symbol}"
        / "year=2019"
        / "month=05"
    )
    directory.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "symbol": symbol,
            "timeframe": "1m",
            "bar_start": datetime(2019, 5, 6, 13, minute, tzinfo=timezone.utc),
            "bar_end": datetime(2019, 5, 6, 13, minute + 1, tzinfo=timezone.utc),
            "open": 10.0 + minute,
            "high": 11.0 + minute,
            "low": 9.0 + minute,
            "close": 10.5 + minute,
            "volume": 1.0,
            "research_artifact": True,
            "runtime_artifact": False,
            "archive_artifact": False,
        }
        for minute in range(5)
    ]
    path = directory / "bars.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path)
    return path


def test_loads_only_research_parquet_and_derives_completed_five_minute_bars(tmp_path: Path) -> None:
    _write_partition(tmp_path, symbol="MNQ")
    config = IndexCandidateValidationConfig(
        repo_root=Path.cwd(),
        output_root=tmp_path / "outputs" / "reports" / "trend_participation_engine",
    )

    rows = _load_research_1m_rows(config=config, symbol="MNQ")
    bars = _derive_decision_bars(symbol="MNQ", rows=rows, config=config)

    assert len(rows) == 5
    assert len(bars) == 1
    assert bars[0].symbol == "MNQ"
    assert bars[0].timeframe == "5m"
    assert bars[0].is_final is True
    assert bars[0].end_ts.isoformat() == "2019-05-06T13:05:00+00:00"


def test_session_mask_marks_trade_eligible_or_ineligible() -> None:
    report = {
        "session_coverage": [
            {
                "date": "2019-05-06",
                "session": "US",
                "eligible_for_replay": True,
                "exclusion_reason": None,
                "total_bars": 400,
                "active_minutes": 400,
                "largest_intra_session_gap_minutes": 0,
                "suspicious_gap_count": 0,
            }
        ]
    }

    row = _annotate_trade(
        {
            "entry_ts": "2019-05-06T14:00:00+00:00",
            "net_pnl": "10",
            "gross_pnl": "10",
            "setup_family": "usDerivativeBearTurn",
        },
        session_mask=_session_mask_from_report(report),
    )

    assert row["eligible_for_replay"] is True
    assert row["entry_session"] == "US"
    assert row["entry_month"] == "2019-05"


def test_metrics_and_classification_are_research_only_and_gap_aware() -> None:
    rows = [
        {"net_pnl": "20", "gross_pnl": "20"},
        {"net_pnl": "-5", "gross_pnl": "-5"},
        {"net_pnl": "10", "gross_pnl": "10"},
    ]

    summary = _summarize_trade_rows(rows)

    assert summary["trade_count"] == 3
    assert summary["net_pnl"] == 25.0
    assert summary["profit_factor"] == 6.0
    assert _robustness_classification(symbol="MNQ", rows=[]) == "NEEDS_ADAPTER_OR_DATA_WORK"


def test_target_parser_requires_explicit_symbol_family_pairs() -> None:
    assert _parse_targets(["MNQ:usDerivativeBearTurn"]) == (("MNQ", "usDerivativeBearTurn"),)


def test_validation_module_has_no_broker_or_paper_proof_paths() -> None:
    source = Path("src/mgc_v05l/app/index_candidate_gap_aware_research_validation.py").read_text(encoding="utf-8")

    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "paper_proof" not in source
    assert "ibkr" not in source.lower()
    assert "TWS" not in source
    assert "live_money_eligible\": True" not in json.dumps({"source": source})
