from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from mgc_v05l.app import databento_research_minute_backfill as backfill
from mgc_v05l.app.databento_research_minute_backfill import (
    DEFAULT_START_DATE,
    ResearchMinuteBackfillConfig,
    run_research_minute_backfill,
)


NOW = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)


class _FakeBackfillClient:
    def __init__(self, *, fail: bool = False, empty: bool = False) -> None:
        self.fail = fail
        self.empty = empty
        self.requests: list[dict[str, Any]] = []

    def get_range_json_lines(
        self,
        *,
        dataset: str,
        request_symbol: str,
        schema_name: str,
        start: datetime,
        end: datetime | None,
        stype_in: str,
        stype_out: str,
        encoding: str,
        compression: str,
        pretty_px: bool,
        pretty_ts: bool,
        map_symbols: bool,
        limit: int | None,
    ) -> list[dict[str, Any]]:
        self.requests.append(
            {
                "dataset": dataset,
                "request_symbol": request_symbol,
                "schema_name": schema_name,
                "start": start,
                "end": end,
                "stype_in": stype_in,
                "stype_out": stype_out,
                "encoding": encoding,
                "compression": compression,
                "pretty_px": pretty_px,
                "pretty_ts": pretty_ts,
                "map_symbols": map_symbols,
                "limit": limit,
            }
        )
        if self.fail:
            raise RuntimeError("provider unavailable")
        if self.empty:
            return []
        return [
            {
                "ts_event": start.replace(hour=0, minute=1, second=0, microsecond=0).isoformat(),
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "volume": 12,
                "symbol": request_symbol.replace(".v.0", "M0"),
                "instrument_id": 123,
            },
            {
                "ts_event": start.replace(hour=0, minute=2, second=0, microsecond=0).isoformat(),
                "open": 10.5,
                "high": 11.5,
                "low": 10.0,
                "close": 11.0,
                "volume": 13,
                "symbol": request_symbol.replace(".v.0", "M0"),
                "instrument_id": 123,
            },
        ]


def _config(tmp_path: Path, **overrides: Any) -> ResearchMinuteBackfillConfig:
    values: dict[str, Any] = {
        "repo_root": tmp_path,
        "output_root": Path("outputs") / "reports" / "trend_participation_engine",
        "symbols": ("MGC", "MNQ", "MES"),
        "start_date": DEFAULT_START_DATE,
        "end_date": date(2010, 8, 15),
        "chunk": "monthly",
        "now": NOW,
        "env_file": tmp_path / ".env.missing",
    }
    values.update(overrides)
    return ResearchMinuteBackfillConfig(**values)


def test_dry_run_uses_2010_start_and_all_three_symbols(tmp_path: Path) -> None:
    result = run_research_minute_backfill(config=_config(tmp_path, dry_run=True))

    assert result.report["final_verdict"] == "RESEARCH_MINUTE_BACKFILL_DRY_RUN_APPROVAL_REQUIRED"
    assert result.report["symbols"] == ["MGC", "MNQ", "MES"]
    assert result.report["requested_start"] == "2010-06-06T00:00:00+00:00"
    assert result.report["partitions_would_fetch"] == result.report["chunk_count"]
    assert result.report["approval_required"] is True
    assert result.report["scope_estimate"]["estimated_1m_bar_count"] > 0
    assert result.report["research_artifact"] is True
    assert result.report["runtime_artifact"] is False
    assert result.report["can_submit"] is False
    assert result.report["live_money_eligible"] is False


def test_monthly_chunking_can_be_limited_for_smoke_runs(tmp_path: Path) -> None:
    result = run_research_minute_backfill(config=_config(tmp_path, dry_run=True, max_chunks=2))

    assert result.report["chunk_count"] == 2
    starts = [row["requested_start"] for row in result.report["partition_reports"]]
    assert starts == ["2010-06-06T00:00:00+00:00", "2010-07-01T00:00:00+00:00"]


def test_default_loaded_history_boundary_skips_2020_plus_duplicate_downloads(tmp_path: Path) -> None:
    result = run_research_minute_backfill(
        config=_config(tmp_path, symbols=("MGC",), end_date=date(2026, 5, 10), dry_run=True)
    )

    assert result.report["loaded_history_start_date"] == "2020-01-01"
    assert result.report["loaded_history_overlap_policy"] == "SKIP_EXISTING_LOADED_HISTORY"
    assert result.report["loaded_history_overlap_skipped"] is True
    assert result.report["requested_end"] == "2026-05-10T23:59:59+00:00"
    assert result.report["planned_fetch_end"] == "2020-01-01T00:00:00+00:00"
    assert result.report["partitions_would_fetch"] == result.report["chunk_count"]
    assert all(row["requested_start"] < "2020-01-01T00:00:00+00:00" for row in result.report["partition_reports"])
    assert result.report["partition_reports"][-1]["requested_end"] == "2020-01-01T00:00:00+00:00"


def test_real_run_writes_atp_research_parquet_manifest_and_duckdb_view(tmp_path: Path) -> None:
    client = _FakeBackfillClient()
    result = run_research_minute_backfill(
        config=_config(tmp_path, symbols=("MGC",), end_date=date(2010, 6, 10), max_chunks=1),
        client=client,
    )

    assert result.report["final_verdict"] == "RESEARCH_MINUTE_BACKFILL_COMPLETE_OR_RESUMED"
    assert result.report["storage_architecture"] == "ATP_RESEARCH_PARQUET_DUCKDB_JSON_MANIFEST"
    partition = (
        tmp_path
        / "outputs"
        / "reports"
        / "trend_participation_engine"
        / "raw_bars"
        / "databento_minute_backfill"
        / "symbol=MGC"
        / "year=2010"
        / "month=06"
        / "bars.parquet"
    )
    manifest = tmp_path / "outputs" / "reports" / "trend_participation_engine" / "manifests" / "databento_research_minute_backfill_manifest.json"
    duckdb_path = tmp_path / "outputs" / "reports" / "trend_participation_engine" / "warehouse" / "trend_participation.duckdb"
    assert partition.exists()
    assert manifest.exists()
    manifest_payload = json.loads(manifest.read_text(encoding="utf-8"))
    assert manifest_payload["research_artifact"] is True
    assert manifest_payload["runtime_artifact"] is False
    assert manifest_payload["can_submit"] is False
    assert result.report["duckdb_registration_status"] in {"REGISTERED", "DUCKDB_NOT_INSTALLED"}
    assert result.report["duckdb_registered"] is duckdb_path.exists()


def test_resume_skips_complete_partition_without_force(tmp_path: Path) -> None:
    first_client = _FakeBackfillClient()
    second_client = _FakeBackfillClient()
    config = _config(tmp_path, symbols=("MGC",), end_date=date(2010, 6, 10), max_chunks=1)

    first = run_research_minute_backfill(config=config, client=first_client)
    second = run_research_minute_backfill(config=config, client=second_client)

    assert first.report["partitions_written"] == 1
    assert second.report["partitions_written"] == 0
    assert second.report["partitions_skipped"] == 1
    assert len(second_client.requests) == 0


def test_empty_chunk_writes_metadata_without_empty_parquet(tmp_path: Path) -> None:
    client = _FakeBackfillClient(empty=True)
    result = run_research_minute_backfill(
        config=_config(tmp_path, symbols=("MGC",), end_date=date(2010, 6, 10), max_chunks=1),
        client=client,
    )

    partition = (
        tmp_path
        / "outputs"
        / "reports"
        / "trend_participation_engine"
        / "raw_bars"
        / "databento_minute_backfill"
        / "symbol=MGC"
        / "year=2010"
        / "month=06"
        / "bars.parquet"
    )
    metadata = partition.with_name("partition_metadata.json")
    metadata_payload = json.loads(metadata.read_text(encoding="utf-8"))

    assert result.report["partitions_written"] == 0
    assert result.report["partitions_empty"] == 1
    assert partition.exists() is False
    assert metadata_payload["status"] == "EMPTY_NO_ROWS"
    assert metadata_payload["runtime_artifact"] is False


def test_force_overwrites_complete_partition(tmp_path: Path) -> None:
    first_client = _FakeBackfillClient()
    second_client = _FakeBackfillClient()
    base = _config(tmp_path, symbols=("MGC",), end_date=date(2010, 6, 10), max_chunks=1)
    forced = _config(tmp_path, symbols=("MGC",), end_date=date(2010, 6, 10), max_chunks=1, force=True)

    run_research_minute_backfill(config=base, client=first_client)
    second = run_research_minute_backfill(config=forced, client=second_client)

    assert second.report["partitions_written"] == 1
    assert len(second_client.requests) == 1


def test_missing_credentials_fail_closed_for_real_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABENTO_API_KEY", raising=False)

    result = run_research_minute_backfill(config=_config(tmp_path, symbols=("MGC",), end_date=date(2010, 6, 10), max_chunks=1))

    assert result.report["final_verdict"] == "RESEARCH_MINUTE_BACKFILL_BLOCKED"
    assert result.report["primary_blocker"] == "DATABENTO_API_KEY_MISSING"
    assert result.report["runtime_artifact"] is False
    assert result.report["can_submit"] is False


def test_provider_error_fails_closed_without_runtime_mutation(tmp_path: Path) -> None:
    result = run_research_minute_backfill(
        config=_config(tmp_path, symbols=("MGC",), end_date=date(2010, 6, 10), max_chunks=1),
        client=_FakeBackfillClient(fail=True),
    )

    assert result.report["final_verdict"] == "RESEARCH_MINUTE_BACKFILL_PARTIAL_OR_BLOCKED"
    assert result.report["provider_error_count"] == 1
    assert result.report["runtime_preflight_dashboard_truth"] is False
    assert result.report["paper_trade_allowed"] is False


def test_configured_research_universe_allows_d4a_symbols(tmp_path: Path) -> None:
    result = run_research_minute_backfill(
        config=_config(
            tmp_path,
            symbols=("GC", "MGC", "ES", "MES", "NQ", "MNQ", "ZT", "ZF", "ZN", "ZB", "PL"),
            research_universe_symbols=("GC", "MGC", "ES", "MES", "NQ", "MNQ", "ZT", "ZF", "ZN", "ZB", "PL"),
            start_date=date(2026, 4, 22),
            end_date=date(2026, 7, 1),
            loaded_history_start_date=None,
            dry_run=True,
        )
    )

    assert result.report["final_verdict"] == "RESEARCH_MINUTE_BACKFILL_DRY_RUN_APPROVAL_REQUIRED"
    assert result.report["symbols"] == ["GC", "MGC", "ES", "MES", "NQ", "MNQ", "ZT", "ZF", "ZN", "ZB", "PL"]
    assert result.report["research_universe"]["unknown_symbols_rejected"] is True
    assert result.report["scope_estimate"]["approval_reason"] == "REQUESTED_RANGE_EXCEEDS_ROUTINE_WEEKLY_POLICY"


def test_rejects_symbols_outside_configured_research_universe(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Unsupported research symbols"):
        run_research_minute_backfill(
            config=_config(
                tmp_path,
                symbols=("CL",),
                research_universe_symbols=("GC", "MGC"),
                dry_run=True,
            )
        )


def test_requires_explicit_end_date_for_scope_control(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="requires an explicit --end-date"):
        run_research_minute_backfill(config=_config(tmp_path, end_date=None, dry_run=True))


def test_unapproved_large_real_refresh_fails_closed_before_provider_call(tmp_path: Path) -> None:
    client = _FakeBackfillClient()

    result = run_research_minute_backfill(
        config=_config(
            tmp_path,
            symbols=("GC",),
            research_universe_symbols=("GC",),
            start_date=date(2026, 4, 22),
            end_date=date(2026, 7, 1),
            loaded_history_start_date=None,
        ),
        client=client,
    )

    assert result.report["final_verdict"] == "RESEARCH_MINUTE_BACKFILL_APPROVAL_REQUIRED"
    assert result.report["primary_blocker"] == "OPERATOR_APPROVAL_REQUIRED_FOR_LARGE_RESEARCH_REFRESH"
    assert result.report["approval_required"] is True
    assert client.requests == []


def test_backfill_module_has_no_broker_or_paper_proof_paths() -> None:
    source = Path(backfill.__file__).read_text(encoding="utf-8")

    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "paper_proof" not in source
    assert "ibkr" not in source.lower()
    assert "TWS" not in source
