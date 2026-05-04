from __future__ import annotations

import inspect
import json
from datetime import datetime, timezone
from pathlib import Path

import mgc_v05l.execution_core.track_b_data_maintenance as data_maintenance_module
from mgc_v05l.execution_core.track_b_data_maintenance import (
    TrackBDataMaintenanceVerdict,
    maintain_track_b_mgc_1m_history,
)
from mgc_v05l.execution_core.track_b_data_maintenance_cli import main as data_maintenance_cli_main
from mgc_v05l.execution_core.track_b_feature_builder import (
    TrackBFeatureBuilderVerdict,
    build_track_b_mgc_feature_event,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 4, 15, 30, tzinfo=timezone.utc)


def bars(*timestamps: str) -> list[dict[str, object]]:
    return [
        {
            "candle_timestamp": timestamp,
            "open": str(4570 + index),
            "high": str(4570 + index),
            "low": str(4570 + index),
            "close": str(4570 + index),
            "volume": "1",
        }
        for index, timestamp in enumerate(timestamps)
    ]


def history_payload(candles: list[dict[str, object]]) -> dict[str, object]:
    return {
        "source_id": "unit_test_history",
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "instrument_family": "MGC",
        "strategy_id": "track_b_example_gold_shadow_v1",
        "lane_id": "mgc_example_long_lmt_day",
        "dataset": "GLBX.MDP3",
        "databento_continuous_symbol": "MGC.v.0",
        "symbol": "MGCM6",
        "timeframe": "1m",
        "candles": candles,
    }


def test_initial_backfill_creates_latest_good_history(tmp_path: Path) -> None:
    result = maintain_track_b_mgc_1m_history(
        incoming_history_payload=history_payload(
            bars(
                "2026-05-04T15:26:00+00:00",
                "2026-05-04T15:27:00+00:00",
                "2026-05-04T15:28:00+00:00",
                "2026-05-04T15:29:00+00:00",
                "2026-05-04T15:30:00+00:00",
            )
        ),
        output_root=tmp_path / "maintenance",
        min_bars=3,
        export_bars=5,
        maintenance_id="maintenance-initial",
        now=aware_now(),
    )

    assert result.verdict == TrackBDataMaintenanceVerdict.UPDATED_HISTORY_READY
    assert result.latest_good_history_json is not None
    assert result.latest_good_history_json.exists()
    assert result.store_json.exists()
    assert result.report["bars_available"] == 5
    assert result.report["gap_count"] == 0
    assert result.report["history_ready"] is True
    assert result.report["latest_good_history_path"] == str(result.latest_good_history_json)
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_incremental_append_adds_newer_bars_and_dedupes(tmp_path: Path) -> None:
    output_root = tmp_path / "maintenance"
    first = maintain_track_b_mgc_1m_history(
        incoming_history_payload=history_payload(
            bars(
                "2026-05-04T15:26:00+00:00",
                "2026-05-04T15:27:00+00:00",
                "2026-05-04T15:28:00+00:00",
            )
        ),
        output_root=output_root,
        min_bars=3,
        export_bars=5,
        maintenance_id="maintenance-first",
        now=datetime(2026, 5, 4, 15, 28, tzinfo=timezone.utc),
    )
    assert first.verdict == TrackBDataMaintenanceVerdict.UPDATED_HISTORY_READY

    second = maintain_track_b_mgc_1m_history(
        incoming_history_payload=history_payload(
            bars(
                "2026-05-04T15:28:00+00:00",
                "2026-05-04T15:29:00+00:00",
                "2026-05-04T15:30:00+00:00",
            )
        ),
        output_root=output_root,
        min_bars=3,
        export_bars=5,
        maintenance_id="maintenance-second",
        now=aware_now(),
    )

    assert second.verdict == TrackBDataMaintenanceVerdict.UPDATED_HISTORY_READY
    assert second.report["bars_available"] == 5
    assert second.report["duplicate_count"] == 1
    assert second.report["last_bar_timestamp"] == "2026-05-04T15:30:00+00:00"


def test_out_of_order_bars_are_sorted(tmp_path: Path) -> None:
    result = maintain_track_b_mgc_1m_history(
        incoming_history_payload=history_payload(
            bars(
                "2026-05-04T15:30:00+00:00",
                "2026-05-04T15:28:00+00:00",
                "2026-05-04T15:29:00+00:00",
            )
        ),
        output_root=tmp_path / "maintenance",
        min_bars=3,
        export_bars=3,
        maintenance_id="maintenance-sorted",
        now=aware_now(),
    )

    assert result.verdict == TrackBDataMaintenanceVerdict.UPDATED_HISTORY_READY
    assert result.latest_good_history is not None
    assert [item["candle_timestamp"] for item in result.latest_good_history["candles"]] == [
        "2026-05-04T15:28:00+00:00",
        "2026-05-04T15:29:00+00:00",
        "2026-05-04T15:30:00+00:00",
    ]


def test_gaps_are_detected_and_block_readiness(tmp_path: Path) -> None:
    result = maintain_track_b_mgc_1m_history(
        incoming_history_payload=history_payload(
            bars(
                "2026-05-04T15:26:00+00:00",
                "2026-05-04T15:28:00+00:00",
                "2026-05-04T15:30:00+00:00",
            )
        ),
        output_root=tmp_path / "maintenance",
        min_bars=3,
        export_bars=3,
        maintenance_id="maintenance-gap",
        now=aware_now(),
    )

    assert result.verdict == TrackBDataMaintenanceVerdict.UPDATED_HISTORY_STALE_OR_INSUFFICIENT
    assert result.report["gap_count"] == 2
    assert result.report["history_ready"] is False
    assert "gaps" in str(result.report["primary_blocker"])


def test_stale_or_insufficient_history_blocks(tmp_path: Path) -> None:
    stale = maintain_track_b_mgc_1m_history(
        incoming_history_payload=history_payload(
            bars(
                "2026-05-04T15:00:00+00:00",
                "2026-05-04T15:01:00+00:00",
                "2026-05-04T15:02:00+00:00",
            )
        ),
        output_root=tmp_path / "maintenance_stale",
        min_bars=3,
        export_bars=3,
        max_history_age_seconds=60,
        maintenance_id="maintenance-stale",
        now=aware_now(),
    )
    insufficient = maintain_track_b_mgc_1m_history(
        incoming_history_payload=history_payload(bars("2026-05-04T15:30:00+00:00")),
        output_root=tmp_path / "maintenance_short",
        min_bars=3,
        export_bars=3,
        maintenance_id="maintenance-short",
        now=aware_now(),
    )

    assert stale.verdict == TrackBDataMaintenanceVerdict.UPDATED_HISTORY_STALE_OR_INSUFFICIENT
    assert "stale" in str(stale.report["primary_blocker"])
    assert insufficient.verdict == TrackBDataMaintenanceVerdict.UPDATED_HISTORY_STALE_OR_INSUFFICIENT
    assert "requires at least 3" in str(insufficient.report["primary_blocker"])


def test_latest_good_history_can_feed_feature_builder_when_current_quote_is_separate(tmp_path: Path) -> None:
    result = maintain_track_b_mgc_1m_history(
        incoming_history_payload=history_payload(
            bars(
                "2026-05-04T15:28:00+00:00",
                "2026-05-04T15:29:00+00:00",
                "2026-05-04T15:30:00+00:00",
            )
        ),
        output_root=tmp_path / "maintenance",
        min_bars=3,
        export_bars=3,
        maintenance_id="maintenance-feature",
        now=aware_now(),
    )
    assert result.latest_good_history is not None
    payload = dict(result.latest_good_history)
    payload["quote_provider_mode"] = "REALTIME"
    payload["realtime_quote_received"] = True
    payload["current_quote_available"] = True

    feature = build_track_b_mgc_feature_event(
        source_event_payload=payload,
        source_event_path=result.latest_good_history_json,
        expected_account_id="DUM882026",
        output_root=tmp_path / "feature_builder",
        now=aware_now(),
    )

    assert feature.verdict == TrackBFeatureBuilderVerdict.WROTE_FEATURE_EVENT
    assert feature.report["submit_attempted"] is False
    assert feature.report["live_money_readiness"] is False


def test_data_maintenance_cli_writes_latest_artifacts(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    history_json = tmp_path / "history.json"
    history_json.write_text(
        json.dumps(
            history_payload(
                bars(
                    "2026-05-04T15:28:00+00:00",
                    "2026-05-04T15:29:00+00:00",
                    "2026-05-04T15:30:00+00:00",
                )
            )
        ),
        encoding="utf-8",
    )

    exit_code = data_maintenance_cli_main(
        [
            "--history-json",
            str(history_json),
            "--expected-account-id",
            "DUM882026",
            "--strategy-id",
            "track_b_example_gold_shadow_v1",
            "--lane-id",
            "mgc_example_long_lmt_day",
            "--min-bars",
            "3",
            "--export-bars",
            "3",
            "--output-root",
            str(tmp_path / "maintenance_cli"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code in {0, 2}
    assert output["data_maintenance_verdict"].startswith("TRACK_B_DATA_MAINTENANCE_")
    assert (tmp_path / "maintenance_cli" / "latest_track_b_data_maintenance_report.json").exists()
    assert (tmp_path / "maintenance_cli" / "latest_good_mgc_1m_history.json").exists()


def test_no_broker_or_proof_paths_are_invoked_by_data_maintenance() -> None:
    source = inspect.getsource(data_maintenance_module)

    assert "run_paper_proof" not in source
    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "Ibkr" not in source
