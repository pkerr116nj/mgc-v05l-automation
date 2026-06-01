from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from mgc_v05l.execution_core.track_b_session_anchor_resolver import (
    SessionAnchorReasonCode,
    SessionAnchorStatus,
    SessionAnchorType,
    TrackBSessionAnchorConfig,
    produce_session_anchor,
    resolve_session_anchor,
)


NY = ZoneInfo("America/New_York")


def test_us_0930_open_resolved_from_runtime_bars(tmp_path: Path) -> None:
    _write_runtime_bars(
        tmp_path,
        "MNQ",
        [
            _bar("2026-06-01T13:30:00+00:00", "2026-06-01T13:31:00+00:00", open_="30420.25"),
            _bar("2026-06-01T13:31:00+00:00", "2026-06-01T13:32:00+00:00", open_="30425.00"),
        ],
    )

    result = resolve_session_anchor(
        "MNQ",
        SessionAnchorType.US_0930_OPEN,
        datetime(2026, 6, 1, 14, 0, tzinfo=NY),
        config=TrackBSessionAnchorConfig(repo_root=tmp_path),
    )

    assert result.status == SessionAnchorStatus.READY
    assert result.reason_code == SessionAnchorReasonCode.ANCHOR_READY_FROM_RUNTIME
    assert result.reference_price == "30420.25"
    assert result.session_date_et == "2026-06-01"
    assert result.to_dict()["broker_mutation_allowed"] is False
    assert result.to_dict()["paper_proof_invoked"] is False


def test_anchor_survives_rolling_runtime_window_by_reading_canonical_artifact(tmp_path: Path) -> None:
    config = TrackBSessionAnchorConfig(repo_root=tmp_path)
    _write_runtime_bars(tmp_path, "MNQ", [_bar("2026-06-01T13:30:00+00:00", "2026-06-01T13:31:00+00:00", open_="30420.25")])
    produced = produce_session_anchor(
        "MNQ",
        "US_0930_OPEN",
        datetime(2026, 6, 1, 14, 0, tzinfo=NY),
        config=config,
    )
    assert produced.status == SessionAnchorStatus.READY
    _write_runtime_bars(tmp_path, "MNQ", [_bar("2026-06-01T16:48:00+00:00", "2026-06-01T16:49:00+00:00", open_="30500")])

    result = resolve_session_anchor(
        "MNQ",
        "US_0930_OPEN",
        datetime(2026, 6, 1, 14, 30, tzinfo=NY),
        config=config,
    )

    assert result.status == SessionAnchorStatus.READY
    assert result.reason_code == SessionAnchorReasonCode.ANCHOR_READY_FROM_CANONICAL_ARTIFACT
    assert result.reference_price == "30420.25"


def test_stale_prior_day_anchor_rejected(tmp_path: Path) -> None:
    config = TrackBSessionAnchorConfig(repo_root=tmp_path)
    path = tmp_path / "outputs/track_b_execution_core/session_anchors/MNQ/2026-06-01/US_0930_OPEN.json"
    _write_json(
        path,
        {
            "schema_version": "track_b_session_anchor_v1",
            "status": "READY",
            "anchor_type": "US_0930_OPEN",
            "symbol": "MNQ",
            "session_date_et": "2026-05-30",
            "anchor_time_utc": "2026-05-30T13:30:00+00:00",
            "reference_price": "30000",
            "reference_price_field": "open",
            "bar": _bar("2026-05-30T13:30:00+00:00", "2026-05-30T13:31:00+00:00", open_="30000"),
        },
    )

    result = resolve_session_anchor(
        "MNQ",
        "US_0930_OPEN",
        datetime(2026, 6, 1, 14, 0, tzinfo=NY),
        config=config,
    )

    assert result.status == SessionAnchorStatus.STALE
    assert result.reason_code == SessionAnchorReasonCode.ANCHOR_SESSION_DATE_MISMATCH


def test_missing_anchor_returns_not_ready_with_searched_paths(tmp_path: Path) -> None:
    result = resolve_session_anchor(
        "MES",
        "US_0930_OPEN",
        datetime(2026, 6, 1, 14, 0, tzinfo=NY),
        config=TrackBSessionAnchorConfig(repo_root=tmp_path),
    )

    assert result.status == SessionAnchorStatus.NOT_READY
    assert result.reason_code == SessionAnchorReasonCode.ANCHOR_BAR_NOT_FOUND
    assert result.not_ready_reason == "ANCHOR_BAR_NOT_FOUND"
    assert len(result.searched_source_paths) >= 4


def test_boundary_containment_start_inclusive_end_exclusive(tmp_path: Path) -> None:
    _write_runtime_bars(
        tmp_path,
        "MNQ",
        [_bar("2026-06-01T13:30:00+00:00", "2026-06-01T13:31:00+00:00", open_="30420.25")],
    )

    result = resolve_session_anchor(
        "MNQ",
        "US_0930_OPEN",
        datetime(2026, 6, 1, 10, 0, tzinfo=NY),
        config=TrackBSessionAnchorConfig(repo_root=tmp_path),
    )

    assert result.status == SessionAnchorStatus.READY
    assert result.bar is not None
    assert result.bar["bar_start"] == "2026-06-01T13:30:00+00:00"


def test_legacy_exact_end_compatibility(tmp_path: Path) -> None:
    _write_runtime_bars(
        tmp_path,
        "MNQ",
        [_bar("2026-06-01T13:29:00+00:00", "2026-06-01T13:30:00+00:00", open_="30419.00")],
    )

    result = resolve_session_anchor(
        "MNQ",
        "US_0930_OPEN",
        datetime(2026, 6, 1, 10, 0, tzinfo=NY),
        config=TrackBSessionAnchorConfig(repo_root=tmp_path),
    )

    assert result.status == SessionAnchorStatus.READY
    assert result.reference_price == "30419.00"


def test_dst_timezone_handling_for_us_anchor(tmp_path: Path) -> None:
    _write_runtime_bars(
        tmp_path,
        "MNQ",
        [_bar("2026-12-01T14:30:00+00:00", "2026-12-01T14:31:00+00:00", open_="25000")],
    )

    result = resolve_session_anchor(
        "MNQ",
        "US_0930_OPEN",
        datetime(2026, 12, 1, 10, 0, tzinfo=NY),
        config=TrackBSessionAnchorConfig(repo_root=tmp_path),
    )

    assert result.status == SessionAnchorStatus.READY
    assert result.anchor_time_utc is not None
    assert result.anchor_time_utc.isoformat() == "2026-12-01T14:30:00+00:00"


def test_naive_timestamp_invalid_for_authority(tmp_path: Path) -> None:
    result = resolve_session_anchor(
        "MNQ",
        "US_0930_OPEN",
        datetime(2026, 6, 1, 10, 0),
        config=TrackBSessionAnchorConfig(repo_root=tmp_path),
    )

    assert result.status == SessionAnchorStatus.NOT_READY
    assert result.reason_code == SessionAnchorReasonCode.NOT_READY_INVALID_TIMESTAMP


def test_gap_backfill_can_recover_anchor_when_runtime_window_rolled(tmp_path: Path) -> None:
    _write_runtime_bars(tmp_path, "MES", [_bar("2026-06-01T16:48:00+00:00", "2026-06-01T16:49:00+00:00", open_="7600")])
    _write_gap_bars(
        tmp_path,
        "MES",
        "us_session_reference",
        [_bar("2026-06-01T13:30:00+00:00", "2026-06-01T13:31:00+00:00", open_="7588.50")],
    )

    result = resolve_session_anchor(
        "MES",
        "US_0930_OPEN",
        datetime(2026, 6, 1, 14, 0, tzinfo=NY),
        config=TrackBSessionAnchorConfig(repo_root=tmp_path),
    )

    assert result.status == SessionAnchorStatus.READY
    assert result.reason_code == SessionAnchorReasonCode.ANCHOR_RECOVERED_FROM_PHASE1_GAP_BACKFILL
    assert result.reference_price == "7588.50"


def _bar(start: str, end: str, *, open_: str) -> dict[str, object]:
    return {
        "bar_start": start,
        "bar_end": end,
        "open": open_,
        "high": open_,
        "low": open_,
        "close": open_,
        "volume": "100",
        "completed": True,
    }


def _write_runtime_bars(tmp_path: Path, symbol: str, bars: list[dict[str, object]]) -> None:
    _write_json(
        tmp_path / f"outputs/track_b_execution_core/phase1_runtime_market_data/{symbol}/1m/latest_runtime_candles.json",
        {"source": "DATABENTO_REALTIME_PHASE1", "generated_at": "2026-06-01T14:01:00+00:00", "bars": bars},
    )


def _write_gap_bars(tmp_path: Path, symbol: str, subdir: str, bars: list[dict[str, object]]) -> None:
    _write_json(
        tmp_path / f"outputs/track_b_execution_core/phase1_runtime_market_data_gap_backfill/{subdir}/{symbol}/1m/latest_runtime_candles.json",
        {"source": "DATABENTO_HISTORICAL_SEED", "generated_at": "2026-06-01T14:01:00+00:00", "bars": bars},
    )


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
