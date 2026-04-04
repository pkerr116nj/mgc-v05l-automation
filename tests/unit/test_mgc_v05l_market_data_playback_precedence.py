from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.domain.models import Bar
from mgc_v05l.market_data.bar_models import build_bar_id
from mgc_v05l.market_data.sqlite_playback import SQLiteHistoricalBarSource
from mgc_v05l.persistence.db import build_engine
from mgc_v05l.persistence.repositories import RepositorySet


def _build_settings(tmp_path: Path):
    overlay_path = tmp_path / "overlay.yaml"
    overlay_path.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{tmp_path / "playback.sqlite3"}"\n',
        encoding="utf-8",
    )
    return load_settings_from_files([Path("config/base.yaml"), overlay_path])


def _bar(symbol: str, end_ts: datetime) -> Bar:
    return Bar(
        bar_id=build_bar_id(symbol, "1m", end_ts),
        symbol=symbol,
        timeframe="1m",
        start_ts=end_ts - timedelta(minutes=1),
        end_ts=end_ts,
        open=Decimal("1"),
        high=Decimal("2"),
        low=Decimal("0.5"),
        close=Decimal("1.5"),
        volume=10,
        is_final=True,
        session_asia=True,
        session_london=False,
        session_us=False,
        session_allowed=True,
    )


def test_sqlite_playback_prefers_canonical_historical_source(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    base = datetime(2026, 2, 3, 18, 1, tzinfo=ZoneInfo("America/New_York"))
    repositories.bars.save(_bar("MGC", base), data_source="historical_1m_canonical")
    repositories.bars.save(_bar("MGC", base + timedelta(minutes=1)), data_source="historical_1m_canonical")
    repositories.bars.save(_bar("MGC", base + timedelta(minutes=10)), data_source="schwab_history")

    loaded = SQLiteHistoricalBarSource(tmp_path / "playback.sqlite3", settings).load_bars(
        symbol="MGC",
        source_timeframe="1m",
        target_timeframe="1m",
    )

    assert loaded.data_source == "historical_1m_canonical"
    assert len(loaded.playback_bars) == 2
