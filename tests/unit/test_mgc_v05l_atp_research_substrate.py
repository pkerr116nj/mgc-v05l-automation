from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.research.trend_participation.features import build_feature_states
from mgc_v05l.research.trend_participation.models import ResearchBar
from mgc_v05l.research.trend_participation.substrate import ensure_atp_feature_bundle, ensure_atp_scope_bundle


def _bar(*, instrument: str, timeframe: str, minute_offset: int, minutes: int, open_: float, high: float, low: float, close: float) -> ResearchBar:
    start = datetime(2026, 1, 5, 14, 0, tzinfo=UTC) + timedelta(minutes=minute_offset)
    end = start + timedelta(minutes=minutes)
    return ResearchBar(
        instrument=instrument,
        timeframe=timeframe,
        start_ts=start,
        end_ts=end,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=100,
        session_label="US_MIDDAY",
        session_segment="US",
        source="synthetic",
        provenance="unit_test",
    )


def _eligible_phase2_setup_and_1m_followthrough() -> tuple[list[ResearchBar], list[ResearchBar]]:
    bars_5m = [
        _bar(instrument="GC", timeframe="5m", minute_offset=idx * 5, minutes=5, open_=100.0 + idx * 0.4, high=100.35 + idx * 0.4, low=99.9 + idx * 0.4, close=100.25 + idx * 0.4)
        for idx in range(8)
    ] + [
        _bar(instrument="GC", timeframe="5m", minute_offset=40, minutes=5, open_=103.0, high=103.05, low=102.4, close=102.55),
        _bar(instrument="GC", timeframe="5m", minute_offset=45, minutes=5, open_=102.55, high=103.48, low=102.5, close=103.36),
    ]
    bars_1m = [
        _bar(instrument="GC", timeframe="1m", minute_offset=51, minutes=1, open_=103.12, high=103.22, low=103.05, close=103.16),
        _bar(instrument="GC", timeframe="1m", minute_offset=52, minutes=1, open_=103.18, high=103.82, low=103.32, close=103.70),
        _bar(instrument="GC", timeframe="1m", minute_offset=53, minutes=1, open_=103.72, high=103.9, low=103.55, close=103.86),
        _bar(instrument="GC", timeframe="1m", minute_offset=54, minutes=1, open_=103.84, high=104.02, low=103.74, close=103.98),
    ]
    return bars_5m, bars_1m


def test_atp_substrate_bundles_persist_and_reload(tmp_path: Path) -> None:
    bars_5m, bars_1m = _eligible_phase2_setup_and_1m_followthrough()
    feature_rows = build_feature_states(bars_5m=bars_5m, bars_1m=bars_1m)
    rolling_scope_feature_rows = [row for row in feature_rows if row.decision_ts == bars_5m[-1].end_ts]

    feature_bundle = ensure_atp_feature_bundle(
        bundle_root=tmp_path,
        source_db=tmp_path / "replay.sqlite3",
        symbol="GC",
        selected_sources={"1m": {"data_source": "synthetic"}, "5m": {"data_source": "synthetic"}},
        start_timestamp=bars_1m[0].end_ts,
        end_timestamp=bars_1m[-1].end_ts,
        feature_rows=rolling_scope_feature_rows,
    )
    assert feature_bundle.manifest_path.exists()

    scope_bundle = ensure_atp_scope_bundle(
        bundle_root=tmp_path,
        source_db=tmp_path / "replay.sqlite3",
        symbol="GC",
        selected_sources={"1m": {"data_source": "synthetic"}, "5m": {"data_source": "synthetic"}},
        start_timestamp=bars_1m[0].end_ts,
        end_timestamp=bars_1m[-1].end_ts,
        allowed_sessions=("US",),
        point_value=100.0,
        bars_1m=bars_1m,
        feature_bundle=feature_bundle,
    )
    assert scope_bundle.manifest_path.exists()
    assert scope_bundle.dataset_paths["trade_records"].endswith(".parquet")

    cached_feature_bundle = ensure_atp_feature_bundle(
        bundle_root=tmp_path,
        source_db=tmp_path / "replay.sqlite3",
        symbol="GC",
        selected_sources={"1m": {"data_source": "synthetic"}, "5m": {"data_source": "synthetic"}},
        start_timestamp=bars_1m[0].end_ts,
        end_timestamp=bars_1m[-1].end_ts,
        feature_rows=[],
    )
    cached_scope_bundle = ensure_atp_scope_bundle(
        bundle_root=tmp_path,
        source_db=tmp_path / "replay.sqlite3",
        symbol="GC",
        selected_sources={"1m": {"data_source": "synthetic"}, "5m": {"data_source": "synthetic"}},
        start_timestamp=bars_1m[0].end_ts,
        end_timestamp=bars_1m[-1].end_ts,
        allowed_sessions=("US",),
        point_value=100.0,
        bars_1m=bars_1m,
        feature_bundle=cached_feature_bundle,
    )

    assert cached_feature_bundle.bundle_id == feature_bundle.bundle_id
    assert cached_scope_bundle.bundle_id == scope_bundle.bundle_id
    assert len(cached_feature_bundle.feature_rows) == len(feature_bundle.feature_rows)
    assert len(cached_scope_bundle.trade_rows) == len(scope_bundle.trade_rows)
