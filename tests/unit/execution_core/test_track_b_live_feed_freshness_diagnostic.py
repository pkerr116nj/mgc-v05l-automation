from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_live_feed_freshness_diagnostic import (
    build_track_b_live_feed_freshness_diagnostic,
)


def now() -> datetime:
    return datetime(2026, 5, 6, 12, 0, tzinfo=UTC)


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def monitor(tmp_path: Path) -> None:
    root = tmp_path / "outputs" / "track_b_execution_core" / "track_b_shadow_monitor"
    write_json(
        root / "latest_track_b_shadow_monitor_report.json",
        {
            "monitor_mode": "PAPER",
            "runtime_decision_source": "DATABENTO_LIVE_ARTIFACT",
            "monitor_verdict": "TRACK_B_SHADOW_MONITOR_OK_NO_SIGNAL",
            "instrument_reports": [
                {
                    "instrument_family": "MGC",
                    "contract_key": "MGC-202606",
                    "local_symbol": "MGCM6",
                    "databento_continuous_symbol": "MGC.v.0",
                    "dataset": "GLBX.MDP3",
                    "live_feed_pid": 111,
                    "runtime_chain_wired": True,
                    "enabled_strategies": ["TEST_MGC_STRATEGY_V1"],
                },
                {
                    "instrument_family": "MNQ",
                    "contract_key": "MNQ-202606",
                    "local_symbol": "MNQM6",
                    "databento_continuous_symbol": "MNQ.v.0",
                    "dataset": "GLBX.MDP3",
                    "live_feed_pid": 222,
                    "runtime_chain_wired": True,
                    "enabled_strategies": ["TEST_MNQ_STRATEGY_V1"],
                },
            ],
        },
    )
    write_json(root / "latest_track_b_shadow_monitor_heartbeat.json", {"generated_at": now().isoformat(), "monitor_running": True})


def live_artifacts(tmp_path: Path, family: str, *, latest_1m: str, latest_5m: str, connected: bool = True) -> None:
    symbol = family.lower()
    root = tmp_path / "outputs" / "track_b_execution_core" / "databento_live_runtime_feed"
    local = "MGCM6" if family == "MGC" else "MNQM6"
    contract = "MGC-202606" if family == "MGC" else "MNQ-202606"
    continuous = "MGC.v.0" if family == "MGC" else "MNQ.v.0"
    write_json(
        root / f"latest_live_{symbol}_1m_candles.json",
        {
            "contract_key": contract,
            "local_symbol": local,
            "databento_continuous_symbol": continuous,
            "dataset": "GLBX.MDP3",
            "last_candle_timestamp": latest_1m,
            "candle_timestamp": latest_1m,
            "candles": [{"candle_timestamp": latest_1m}],
        },
    )
    write_json(
        root / f"latest_live_{symbol}_completed_5m_candles.json",
        {
            "generated_at": now().isoformat(),
            "candles": [{"candle_timestamp": latest_5m}],
            "bars_available": 1,
        },
    )
    write_json(
        root / f"latest_databento_live_runtime_feed_{symbol}_heartbeat.json",
        {
            "generated_at": now().isoformat(),
            "live_feed_connected": connected,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "latest_record_ts_event": latest_1m,
            "latest_1m_timestamp": latest_1m,
            "latest_completed_5m_timestamp": latest_5m,
        },
    )
    write_json(
        root / f"latest_databento_live_runtime_feed_{symbol}_report.json",
        {
            "contract_key": contract,
            "local_symbol": local,
            "databento_continuous_symbol": continuous,
            "dataset": "GLBX.MDP3",
            "live_feed_connected": connected,
            "latest_live_1m_candles_path": str(root / f"latest_live_{symbol}_1m_candles.json"),
            "latest_live_completed_5m_candles_path": str(root / f"latest_live_{symbol}_completed_5m_candles.json"),
        },
    )


def test_live_feed_freshness_diagnostic_reports_per_instrument_stale_and_fresh(tmp_path: Path) -> None:
    monitor(tmp_path)
    live_artifacts(tmp_path, "MGC", latest_1m="2026-05-06T11:55:00+00:00", latest_5m="2026-05-06T11:55:00+00:00")
    live_artifacts(tmp_path, "MNQ", latest_1m="2026-05-06T11:59:00+00:00", latest_5m="2026-05-06T11:55:00+00:00")

    result = build_track_b_live_feed_freshness_diagnostic(repo_root=tmp_path, output_root=tmp_path / "diag", now=now())

    assert result.report["diagnosis_classification"] == "STALE_LIVE_FEED"
    assert result.report["stale_instruments"] == ["MGC"]
    assert result.report["fresh_instruments"] == ["MNQ"]
    by_family = {item["instrument_family"]: item for item in result.report["instrument_reports"]}
    assert by_family["MGC"]["transport_connected"] is True
    assert by_family["MGC"]["execution_fresh"] is False
    assert "latest 1m candle age" in str(by_family["MGC"]["reason_for_stale_verdict"])
    assert by_family["MNQ"]["execution_fresh"] is True
    assert result.report["http_backfill_can_satisfy_execution_freshness"] is False
    assert result.report_json.exists()


def test_live_feed_freshness_diagnostic_detects_read_write_path_mismatch(tmp_path: Path) -> None:
    monitor(tmp_path)
    live_artifacts(tmp_path, "MGC", latest_1m="2026-05-06T11:59:00+00:00", latest_5m="2026-05-06T11:55:00+00:00")
    report = tmp_path / "outputs" / "track_b_execution_core" / "databento_live_runtime_feed" / "latest_databento_live_runtime_feed_mgc_report.json"
    payload = json.loads(report.read_text(encoding="utf-8"))
    payload["latest_live_1m_candles_path"] = "outputs/track_b_execution_core/databento_live_runtime_feed/not_the_reader.json"
    report.write_text(json.dumps(payload), encoding="utf-8")

    result = build_track_b_live_feed_freshness_diagnostic(repo_root=tmp_path, output_root=tmp_path / "diag", now=now())

    mgc = next(item for item in result.report["instrument_reports"] if item["instrument_family"] == "MGC")
    assert mgc["read_write_path_mismatch"] is True
