from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.phase1_databento_historical_seed import (
    Phase1HistoricalSeedConfig,
    build_phase1_databento_historical_seed,
)
from mgc_v05l.execution_core.phase1_runtime_ticker_registry import PHASE1_RUNTIME_TICKER_ORDER

NOW = datetime(2026, 5, 10, 16, 0, tzinfo=timezone.utc)


class FakeHistoricalClient:
    def __init__(self, records: list[dict[str, Any]] | None = None, *, raise_error: bool = False) -> None:
        self.records = records or _records()
        self.raise_error = raise_error
        self.requests: list[dict[str, Any]] = []

    def get_range_json_lines(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.requests.append(dict(kwargs))
        if self.raise_error:
            raise RuntimeError("historical server unavailable")
        return list(self.records)


def _records(count: int = 30) -> list[dict[str, Any]]:
    start = NOW - timedelta(minutes=count)
    rows: list[dict[str, Any]] = []
    for index in range(count):
        ts = start + timedelta(minutes=index + 1)
        px = 100.0 + index
        rows.append(
            {
                "ts_event": ts.isoformat(),
                "open": px,
                "high": px + 1.0,
                "low": px - 1.0,
                "close": px + 0.5,
                "volume": 10 + index,
            }
        )
    return rows


def _config(root: Path, **overrides: object) -> Phase1HistoricalSeedConfig:
    values = {
        "repo_root": root,
        "report_dir": Path("outputs") / "reports" / "phase1_databento_historical_seed",
        "env_file": root / ".env.missing",
        "now": NOW,
    }
    values.update(overrides)
    return Phase1HistoricalSeedConfig(**values)


def test_bounded_30_day_window_and_all_10_symbols_are_requested(tmp_path: Path) -> None:
    client = FakeHistoricalClient()

    result = build_phase1_databento_historical_seed(config=_config(tmp_path), client=client)

    assert len(client.requests) == 10
    assert {request["request_symbol"] for request in client.requests} == {f"{symbol}.v.0" for symbol in PHASE1_RUNTIME_TICKER_ORDER}
    assert all(request["dataset"] == "GLBX.MDP3" for request in client.requests)
    assert all(request["schema_name"] == "ohlcv-1m" for request in client.requests)
    assert all(request["end"] == NOW.replace(second=0, microsecond=0) for request in client.requests)
    assert all(request["start"] == NOW.replace(second=0, microsecond=0) - timedelta(days=30) for request in client.requests)
    assert result.report["phase1_symbol_count"] == 10
    assert result.report["historical_seed_ready_count"] == 10
    assert result.report["final_classification"] == "SUNDAY_HISTORICAL_SEED_READY"


def test_seed_artifacts_include_runtime_provenance_and_do_not_enable_submit(tmp_path: Path) -> None:
    result = build_phase1_databento_historical_seed(
        config=_config(tmp_path, symbols=("GC",)),
        client=FakeHistoricalClient(),
    )

    assert result.report["realtime_feed_confirmed"] is False
    assert result.report["research_artifact_used"] is False
    assert result.report["archive_artifact_used"] is False
    assert result.report["can_submit"] is False
    assert result.report["live_money_eligible"] is False
    one_minute = tmp_path / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data" / "GC" / "1m" / "latest_runtime_candles.json"
    five_minute = tmp_path / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data" / "GC" / "5m" / "latest_runtime_candles.json"
    three_minute = tmp_path / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data" / "GC" / "3m" / "latest_runtime_candles.json"
    assert one_minute.exists()
    assert five_minute.exists()
    assert three_minute.exists()
    payload = json.loads(one_minute.read_text(encoding="utf-8"))
    assert payload["source"] == "DATABENTO_HISTORICAL_SEED"
    assert payload["source_id"] == "DATABENTO_HISTORICAL_SEED"
    assert payload["symbol"] == "GC"
    assert payload["instrument"] == "GC"
    assert payload["timeframe"] == "1m"
    assert payload["bar_count"] == 30
    assert payload["last_completed_bar_ts"] == NOW.isoformat()
    assert payload["historical_seed_ready"] is True
    assert payload["realtime_feed_confirmed"] is False
    assert payload["research_artifact_used"] is False
    assert payload["archive_artifact_used"] is False
    assert payload["can_submit"] is False
    assert payload["live_money_eligible"] is False


def test_missing_credentials_fail_closed_without_client(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("DATABENTO_API_KEY", raising=False)

    result = build_phase1_databento_historical_seed(config=_config(tmp_path, symbols=("GC",)))

    assert result.report["final_classification"] == "DATABENTO_HISTORICAL_SEED_BLOCKED"
    assert result.report["primary_blocker"] == "DATABENTO_API_KEY_MISSING"
    assert result.report["historical_seed_ready"] is False
    assert result.report["can_submit"] is False
    assert result.artifacts_written == []


def test_unavailable_historical_server_fails_closed(tmp_path: Path) -> None:
    result = build_phase1_databento_historical_seed(
        config=_config(tmp_path, symbols=("GC",)),
        client=FakeHistoricalClient(raise_error=True),
    )

    assert result.report["final_classification"] == "SUNDAY_HISTORICAL_SEED_PARTIAL_OR_BLOCKED"
    row = result.report["rows"][0]
    assert row["historical_seed_ready"] is False
    assert row["block_reason"] == "DATABENTO_HISTORICAL_FETCH_FAILED"
    assert row["can_submit"] is False
    assert row["live_money_eligible"] is False


def test_no_broker_or_paper_proof_terms_in_seed_module() -> None:
    path = Path("src/mgc_v05l/execution_core/phase1_databento_historical_seed.py")
    text = path.read_text(encoding="utf-8")

    assert "placeOrder" not in text
    assert "cancelOrder" not in text
    assert "paper_proof" not in text
