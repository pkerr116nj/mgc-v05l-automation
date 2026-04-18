from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.app.published_strategy_exit_probe import PublishedExitProbeSpec
from mgc_v05l.app.published_strategy_exit_probe import publish_published_strategy_exit_probe_study
from mgc_v05l.app.published_strategy_exit_probe import run_published_strategy_exit_probe


def _bar(*, index: int, open_: float, high: float, low: float, close: float) -> dict[str, object]:
    ts = datetime(2026, 4, 1, 12, 0, tzinfo=UTC) + timedelta(minutes=index)
    return {
        "bar_id": f"GC|1m|{ts.isoformat()}",
        "timestamp": ts.isoformat(),
        "end_timestamp": ts.isoformat(),
        "start_timestamp": (ts - timedelta(minutes=1)).isoformat(),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
    }


def test_run_published_strategy_exit_probe_writes_report(tmp_path: Path) -> None:
    bars = [
        _bar(index=0, open_=100.0, high=100.1, low=99.9, close=100.0),
        _bar(index=1, open_=100.0, high=100.2, low=99.9, close=100.1),
        _bar(index=2, open_=100.1, high=100.3, low=100.0, close=100.2),
        _bar(index=3, open_=100.2, high=100.7, low=100.15, close=100.6),
        _bar(index=4, open_=100.6, high=100.8, low=100.45, close=100.5),
        _bar(index=5, open_=100.5, high=100.55, low=100.1, close=100.2),
    ]
    study_path = tmp_path / "sample.strategy_study.json"
    payload = {
        "standalone_strategy_id": "sample_gc_strategy",
        "symbol": "GC",
        "point_value": 100,
        "bars": bars,
        "summary": {
            "closed_trade_breakdown": [
                {
                    "trade_id": "1",
                    "side": "LONG",
                    "family": "sample_family",
                    "entry_timestamp": bars[2]["end_timestamp"],
                    "exit_timestamp": bars[5]["end_timestamp"],
                    "entry_price": 100.1,
                    "exit_price": 100.2,
                    "realized_pnl": 10.0,
                    "exit_reason": "time_exit",
                    "entry_session_phase": "US",
                }
            ]
        },
    }
    study_path.write_text(json.dumps(payload), encoding="utf-8")

    result = run_published_strategy_exit_probe(
        study_json_paths=[study_path],
        report_dir=tmp_path / "report",
        spec=PublishedExitProbeSpec(
            checkpoint_arm_r=0.8,
            checkpoint_lock_r=0.35,
            checkpoint_trail_r=0.25,
            no_traction_abort_bars=2,
            no_traction_min_favorable_r=0.25,
            risk_lookback_bars=3,
            risk_range_floor=0.1,
        ),
    )

    report = json.loads(Path(result["report_json_path"]).read_text(encoding="utf-8"))
    assert report["results"][0]["baseline"]["trade_count"] == 1
    assert report["results"][0]["candidate"]["trade_count"] == 1
    assert "checkpoint_stop" in report["results"][0]["candidate"]["exit_reason_counts"]


def test_publish_published_strategy_exit_probe_study_writes_parallel_artifacts(tmp_path: Path) -> None:
    bars = [
        _bar(index=0, open_=100.0, high=100.1, low=99.9, close=100.0),
        _bar(index=1, open_=100.0, high=100.2, low=99.9, close=100.1),
        _bar(index=2, open_=100.1, high=100.3, low=100.0, close=100.2),
        _bar(index=3, open_=100.2, high=100.7, low=100.15, close=100.6),
        _bar(index=4, open_=100.6, high=100.8, low=100.45, close=100.5),
        _bar(index=5, open_=100.5, high=100.55, low=100.1, close=100.2),
    ]
    study_path = tmp_path / "sample.strategy_study.json"
    payload = {
        "standalone_strategy_id": "sample_gc_strategy",
        "symbol": "GC",
        "point_value": 100,
        "bars": bars,
        "summary": {
            "bar_count": len(bars),
            "closed_trade_breakdown": [
                {
                    "trade_id": "1",
                    "side": "LONG",
                    "family": "sample_family",
                    "entry_timestamp": bars[2]["end_timestamp"],
                    "exit_timestamp": bars[5]["end_timestamp"],
                    "entry_price": 100.1,
                    "exit_price": 100.2,
                    "realized_pnl": 10.0,
                    "exit_reason": "time_exit",
                    "entry_session_phase": "US",
                }
            ]
        },
    }
    study_path.write_text(json.dumps(payload), encoding="utf-8")

    result = publish_published_strategy_exit_probe_study(
        source_study_json_path=study_path,
        report_dir=tmp_path / "report",
        historical_playback_dir=tmp_path / "historical_playback",
        study_suffix="_checkpoint_no_traction_v1",
        label_suffix=" [Checkpoint + No-Traction v1]",
        spec=PublishedExitProbeSpec(
            checkpoint_arm_r=0.8,
            checkpoint_lock_r=0.35,
            checkpoint_trail_r=0.25,
            no_traction_abort_bars=2,
            no_traction_min_favorable_r=0.25,
            risk_lookback_bars=3,
            risk_range_floor=0.1,
        ),
    )

    published_payload = json.loads(Path(result["strategy_study_json_path"]).read_text(encoding="utf-8"))
    assert published_payload["standalone_strategy_id"] == "sample_gc_strategy_checkpoint_no_traction_v1"
    assert published_payload["summary"]["total_trades"] == 1
    assert "published_strategy_exit_probe_source_study" in (published_payload.get("meta") or {}).get("truth_provenance", {})
    manifest_payload = json.loads(Path(result["historical_playback_manifest_path"]).read_text(encoding="utf-8"))
    assert any(
        str(row.get("strategy_id") or "") == "sample_gc_strategy_checkpoint_no_traction_v1"
        for row in list(manifest_payload.get("studies") or [])
    )
