from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_asian_drift_state import (
    TrackBAsianDriftStateVerdict,
    write_track_b_asian_drift_state_snapshot,
)
from mgc_v05l.execution_core.track_b_asian_drift_state_cli import main as asian_drift_state_cli_main


def aware_now() -> datetime:
    return datetime(2026, 5, 4, 23, 15, tzinfo=timezone.utc)


def state_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "instrument_family": "MGC",
        "strategy_id": "asian_drift_v1",
        "lane_id": "mgc_example_long_lmt_day",
        "timeframe": "5m",
        "candle_timestamp": aware_now().isoformat(),
        "observed_at": aware_now().isoformat(),
        "open": "4575.0",
        "high": "4575.6",
        "low": "4574.8",
        "close": "4575.4",
        "asia_drift_state": "NO_TRADE",
        "asia_drift_regime": "NO_TRADE",
        "hypothetical_entry_ready": False,
        "entry_window_open": True,
        "in_scope": True,
        "session_timeout": False,
        "feature_version": "asia_drift_v1_phase1",
        "calibration_profile": "recovery_confirmed",
        "quote_provider_mode": "REALTIME",
        "realtime_quote_received": True,
        "current_quote_available": True,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }
    payload.update(overrides)
    return payload


def test_valid_state_snapshot_writes_latest_artifacts(tmp_path: Path) -> None:
    result = write_track_b_asian_drift_state_snapshot(
        state_payload=state_payload(),
        output_root=tmp_path / "asian_drift_state",
        builder_id="asian-drift-state-valid",
        now=aware_now(),
    )

    assert result.verdict == TrackBAsianDriftStateVerdict.WROTE_SNAPSHOT
    assert result.snapshot_json is not None
    assert result.snapshot_json.exists()
    assert result.report["asian_drift_state_ready"] is True
    assert result.report["asian_drift_watch_verdict"] == "ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION"
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False
    latest = tmp_path / "asian_drift_state" / "latest_asian_drift_5m_state_snapshot.json"
    assert latest.exists()
    snapshot = json.loads(latest.read_text(encoding="utf-8"))
    assert snapshot["strategy_id"] == "asian_drift_v1"
    assert snapshot["asian_drift_state_ready"] is True


def test_missing_state_fields_reports_not_ready_for_tonight(tmp_path: Path) -> None:
    payload = state_payload()
    payload.pop("asia_drift_state")

    result = write_track_b_asian_drift_state_snapshot(
        state_payload=payload,
        output_root=tmp_path / "asian_drift_state",
        builder_id="asian-drift-state-missing",
        now=aware_now(),
    )

    assert result.verdict == TrackBAsianDriftStateVerdict.BLOCKED_INVALID_STATE
    assert result.snapshot_json is None
    assert result.report["asian_drift_watch_verdict"] == "ASIAN_DRIFT_NOT_READY_FOR_TONIGHT"
    assert result.report["asian_drift_state_ready"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_cli_writes_valid_state_snapshot(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    state_json = tmp_path / "state.json"
    state_json.write_text(json.dumps(state_payload()), encoding="utf-8")

    exit_code = asian_drift_state_cli_main(
        [
            "--state-json",
            str(state_json),
            "--output-root",
            str(tmp_path / "asian_drift_state"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["asian_drift_state_builder_verdict"] == "TRACK_B_ASIAN_DRIFT_STATE_WROTE_SNAPSHOT"
    assert output["asian_drift_state_ready"] is True
    assert Path(output["latest_asian_drift_state_snapshot_path"]).exists()
