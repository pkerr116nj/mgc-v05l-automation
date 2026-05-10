from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from mgc_v05l.app import databento_research_minute_backfill_audit as audit
from mgc_v05l.app.databento_research_minute_backfill import SOURCE
from mgc_v05l.app.databento_research_minute_backfill_audit import (
    ResearchBackfillAuditConfig,
    audit_research_minute_backfill,
)


NOW = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)


def _config(tmp_path: Path, **overrides: Any) -> ResearchBackfillAuditConfig:
    values: dict[str, Any] = {
        "repo_root": tmp_path,
        "output_root": Path("outputs") / "reports" / "trend_participation_engine",
        "symbol": "MGC",
        "now": NOW,
    }
    values.update(overrides)
    return ResearchBackfillAuditConfig(**values)


def _partition_dir(tmp_path: Path, *, year: int, month: int, symbol: str = "MGC") -> Path:
    return (
        tmp_path
        / "outputs"
        / "reports"
        / "trend_participation_engine"
        / "raw_bars"
        / "databento_minute_backfill"
        / f"symbol={symbol}"
        / f"year={year:04d}"
        / f"month={month:02d}"
    )


def _write_metadata(
    tmp_path: Path,
    *,
    year: int,
    month: int,
    status: str = "COMPLETE",
    row_count: int = 0,
    symbol: str = "MGC",
    runtime_artifact: bool = False,
) -> Path:
    directory = _partition_dir(tmp_path, year=year, month=month, symbol=symbol)
    directory.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": NOW.isoformat(),
        "source": SOURCE,
        "symbol": symbol,
        "timeframe": "1m",
        "requested_start": f"{year:04d}-{month:02d}-01T00:00:00+00:00",
        "requested_end": f"{year:04d}-{month:02d}-28T23:59:59+00:00",
        "actual_start": None,
        "actual_end": None,
        "row_count": row_count,
        "status": status,
        "research_artifact": True,
        "runtime_artifact": runtime_artifact,
        "archive_artifact": False,
        "can_submit": False,
        "live_money_eligible": False,
    }
    path = directory / "partition_metadata.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _write_parquet(tmp_path: Path, *, year: int, month: int, bar_ends: list[datetime], symbol: str = "MGC") -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq

    directory = _partition_dir(tmp_path, year=year, month=month, symbol=symbol)
    directory.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "symbol": symbol,
            "timeframe": "1m",
            "bar_end": bar_end,
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "research_artifact": True,
            "runtime_artifact": False,
            "archive_artifact": False,
        }
        for bar_end in bar_ends
    ]
    table = pa.Table.from_pylist(rows)
    path = directory / "bars.parquet"
    pq.write_table(table, path)
    return path


def test_audit_accepts_consistent_research_partitions_and_empty_months(tmp_path: Path) -> None:
    bars = [
        datetime(2019, 12, 2, 0, 0, tzinfo=timezone.utc),
        datetime(2019, 12, 2, 0, 1, tzinfo=timezone.utc),
        datetime(2019, 12, 2, 0, 10, tzinfo=timezone.utc),
    ]
    _write_parquet(tmp_path, year=2019, month=12, bar_ends=bars)
    _write_metadata(tmp_path, year=2019, month=12, row_count=3)
    _write_metadata(tmp_path, year=2010, month=6, status="EMPTY_NO_ROWS")

    result = audit_research_minute_backfill(config=_config(tmp_path))

    assert result.report["final_classification"] == "MGC_2010_TO_2020_RESEARCH_DATA_QUALITY_ACCEPTABLE"
    assert result.report["complete_partition_count"] == 1
    assert result.report["empty_partition_count"] == 1
    assert result.report["failed_partition_count"] == 0
    assert result.report["total_row_count"] == 3
    assert result.report["schema_consistent"] is True
    assert result.report["research_artifact"] is True
    assert result.report["runtime_artifact"] is False
    assert result.report["can_submit"] is False
    assert result.report["live_money_eligible"] is False
    assert result.report["suspicious_gap_count"] == 1
    assert result.report["session_coverage"]
    assert result.report["session_coverage_summary"]["row_count"] == len(result.report["session_coverage"])
    assert result.report_paths[0].exists()
    assert result.report_paths[1].exists()
    assert result.report_paths[2].exists()


def test_session_coverage_reports_replay_eligibility_fields(tmp_path: Path) -> None:
    bars = [datetime(2019, 12, 2, 14, minute, tzinfo=timezone.utc) for minute in range(60)]
    _write_parquet(tmp_path, year=2019, month=12, bar_ends=bars)
    _write_metadata(tmp_path, year=2019, month=12, row_count=len(bars))

    result = audit_research_minute_backfill(config=_config(tmp_path))

    row = next(item for item in result.report["session_coverage"] if item["session"] == "US")
    assert set(row) >= {
        "symbol",
        "date",
        "session",
        "total_bars",
        "active_minutes",
        "largest_intra_session_gap_minutes",
        "suspicious_gap_count",
        "first_bar",
        "last_bar",
        "eligible_for_replay",
        "exclusion_reason",
    }
    assert row["symbol"] == "MGC"
    assert row["date"] == "2019-12-02"
    assert row["total_bars"] == 60
    assert row["active_minutes"] == 60
    assert row["largest_intra_session_gap_minutes"] == 0
    assert row["suspicious_gap_count"] == 0
    assert row["eligible_for_replay"] is False
    assert row["exclusion_reason"] == "ACTIVE_MINUTES_BELOW_75_PERCENT"


def test_audit_flags_2020_partitions_and_runtime_artifact_metadata(tmp_path: Path) -> None:
    _write_parquet(tmp_path, year=2020, month=1, bar_ends=[datetime(2020, 1, 2, 0, 0, tzinfo=timezone.utc)])
    _write_metadata(tmp_path, year=2020, month=1, row_count=1, runtime_artifact=True)

    result = audit_research_minute_backfill(config=_config(tmp_path))

    assert result.report["final_classification"] == "MGC_2010_TO_2020_RESEARCH_DATA_QUALITY_REVIEW_REQUIRED"
    assert result.report["no_2020_plus_partition_exists"] is False
    assert result.report["artifact_flags_ok"] is False
    assert any("runtime_artifact" in item for item in result.report["artifact_flag_failures"])


def test_gap_classifier_separates_maintenance_weekend_and_provider_holes() -> None:
    assert (
        audit._classify_gap(
            datetime(2019, 12, 2, 21, 59, tzinfo=timezone.utc),
            datetime(2019, 12, 2, 23, 0, tzinfo=timezone.utc),
        )
        == "EXPECTED_DAILY_MAINTENANCE_GAP"
    )
    assert (
        audit._classify_gap(
            datetime(2019, 12, 2, 10, 0, tzinfo=timezone.utc),
            datetime(2019, 12, 2, 15, 0, tzinfo=timezone.utc),
        )
        == "PROVIDER_OR_DATA_HOLE"
    )
    assert (
        audit._classify_gap(
            datetime(2019, 12, 6, 21, 59, tzinfo=timezone.utc),
            datetime(2019, 12, 8, 23, 0, tzinfo=timezone.utc),
        )
        == "HOLIDAY_OR_WEEKEND_GAP"
    )


def test_audit_module_has_no_broker_or_paper_proof_paths() -> None:
    source = Path(audit.__file__).read_text(encoding="utf-8")

    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "paper_proof" not in source
    assert "ibkr" not in source.lower()
    assert "TWS" not in source
