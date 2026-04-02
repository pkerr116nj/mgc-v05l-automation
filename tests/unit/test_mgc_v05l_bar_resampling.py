from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.domain.models import Bar
from mgc_v05l.market_data.bar_builder import BarBuilder
from mgc_v05l.research.bar_resampling import build_resampled_bars


def _build_settings(tmp_path: Path):
    overlay_path = tmp_path / "overlay.yaml"
    overlay_path.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{tmp_path / "resample.sqlite3"}"\n',
        encoding="utf-8",
    )
    return load_settings_from_files([Path("config/base.yaml"), overlay_path])


def _bar(end_ts: datetime, open_: str, high: str, low: str, close: str, volume: int) -> Bar:
    return Bar(
        bar_id=f"MGC|1m|{end_ts.isoformat()}",
        symbol="MGC",
        timeframe="1m",
        start_ts=end_ts.replace(second=0, microsecond=0) - timedelta(minutes=1),
        end_ts=end_ts.replace(second=0, microsecond=0),
        open=Decimal(open_),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=volume,
        is_final=True,
        session_asia=False,
        session_london=True,
        session_us=False,
        session_allowed=True,
    )


def test_build_resampled_bars_aggregates_full_three_minute_buckets(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    builder = BarBuilder(settings)
    ny = ZoneInfo("America/New_York")
    source = [
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 1, tzinfo=ny), "100", "101", "99", "100.5", 10)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 2, tzinfo=ny), "100.5", "102", "100", "101.5", 12)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 3, tzinfo=ny), "101.5", "103", "101", "102.5", 14)),
    ]

    result = build_resampled_bars(source, target_timeframe="3m", bar_builder=builder)

    assert result.skipped_bucket_count == 0
    assert len(result.bars) == 1
    bar = result.bars[0]
    assert bar.timeframe == "3m"
    assert bar.open == Decimal("100")
    assert bar.high == Decimal("103")
    assert bar.low == Decimal("99")
    assert bar.close == Decimal("102.5")
    assert bar.volume == 36


def test_build_resampled_bars_skips_incomplete_bucket(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    builder = BarBuilder(settings)
    ny = ZoneInfo("America/New_York")
    source = [
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 1, tzinfo=ny), "100", "101", "99", "100.5", 10)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 2, tzinfo=ny), "100.5", "102", "100", "101.5", 12)),
    ]

    result = build_resampled_bars(source, target_timeframe="3m", bar_builder=builder)

    assert result.skipped_bucket_count == 1
    assert result.bars == []


def test_build_resampled_bars_supports_hour_alias_target(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    builder = BarBuilder(settings)
    ny = ZoneInfo("America/New_York")
    source = [
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 1, tzinfo=ny), "100", "101", "99", "100.5", 10)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 2, tzinfo=ny), "100.5", "102", "100", "101.5", 12)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 3, tzinfo=ny), "101.5", "103", "101", "102.5", 14)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 4, tzinfo=ny), "102.5", "104", "102", "103.5", 16)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 5, tzinfo=ny), "103.5", "105", "103", "104.5", 18)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 6, tzinfo=ny), "104.5", "106", "104", "105.5", 20)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 7, tzinfo=ny), "105.5", "107", "105", "106.5", 22)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 8, tzinfo=ny), "106.5", "108", "106", "107.5", 24)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 9, tzinfo=ny), "107.5", "109", "107", "108.5", 26)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 10, tzinfo=ny), "108.5", "110", "108", "109.5", 28)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 11, tzinfo=ny), "109.5", "111", "109", "110.5", 30)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 12, tzinfo=ny), "110.5", "112", "110", "111.5", 32)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 13, tzinfo=ny), "111.5", "113", "111", "112.5", 34)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 14, tzinfo=ny), "112.5", "114", "112", "113.5", 36)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 15, tzinfo=ny), "113.5", "115", "113", "114.5", 38)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 16, tzinfo=ny), "114.5", "116", "114", "115.5", 40)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 17, tzinfo=ny), "115.5", "117", "115", "116.5", 42)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 18, tzinfo=ny), "116.5", "118", "116", "117.5", 44)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 19, tzinfo=ny), "117.5", "119", "117", "118.5", 46)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 20, tzinfo=ny), "118.5", "120", "118", "119.5", 48)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 21, tzinfo=ny), "119.5", "121", "119", "120.5", 50)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 22, tzinfo=ny), "120.5", "122", "120", "121.5", 52)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 23, tzinfo=ny), "121.5", "123", "121", "122.5", 54)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 24, tzinfo=ny), "122.5", "124", "122", "123.5", 56)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 25, tzinfo=ny), "123.5", "125", "123", "124.5", 58)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 26, tzinfo=ny), "124.5", "126", "124", "125.5", 60)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 27, tzinfo=ny), "125.5", "127", "125", "126.5", 62)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 28, tzinfo=ny), "126.5", "128", "126", "127.5", 64)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 29, tzinfo=ny), "127.5", "129", "127", "128.5", 66)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 30, tzinfo=ny), "128.5", "130", "128", "129.5", 68)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 31, tzinfo=ny), "129.5", "131", "129", "130.5", 70)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 32, tzinfo=ny), "130.5", "132", "130", "131.5", 72)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 33, tzinfo=ny), "131.5", "133", "131", "132.5", 74)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 34, tzinfo=ny), "132.5", "134", "132", "133.5", 76)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 35, tzinfo=ny), "133.5", "135", "133", "134.5", 78)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 36, tzinfo=ny), "134.5", "136", "134", "135.5", 80)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 37, tzinfo=ny), "135.5", "137", "135", "136.5", 82)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 38, tzinfo=ny), "136.5", "138", "136", "137.5", 84)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 39, tzinfo=ny), "137.5", "139", "137", "138.5", 86)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 40, tzinfo=ny), "138.5", "140", "138", "139.5", 88)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 41, tzinfo=ny), "139.5", "141", "139", "140.5", 90)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 42, tzinfo=ny), "140.5", "142", "140", "141.5", 92)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 43, tzinfo=ny), "141.5", "143", "141", "142.5", 94)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 44, tzinfo=ny), "142.5", "144", "142", "143.5", 96)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 45, tzinfo=ny), "143.5", "145", "143", "144.5", 98)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 46, tzinfo=ny), "144.5", "146", "144", "145.5", 100)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 47, tzinfo=ny), "145.5", "147", "145", "146.5", 102)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 48, tzinfo=ny), "146.5", "148", "146", "147.5", 104)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 49, tzinfo=ny), "147.5", "149", "147", "148.5", 106)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 50, tzinfo=ny), "148.5", "150", "148", "149.5", 108)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 51, tzinfo=ny), "149.5", "151", "149", "150.5", 110)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 52, tzinfo=ny), "150.5", "152", "150", "151.5", 112)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 53, tzinfo=ny), "151.5", "153", "151", "152.5", 114)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 54, tzinfo=ny), "152.5", "154", "152", "153.5", 116)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 55, tzinfo=ny), "153.5", "155", "153", "154.5", 118)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 56, tzinfo=ny), "154.5", "156", "154", "155.5", 120)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 57, tzinfo=ny), "155.5", "157", "155", "156.5", 122)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 58, tzinfo=ny), "156.5", "158", "156", "157.5", 124)),
        builder.normalize(_bar(datetime(2026, 3, 16, 6, 59, tzinfo=ny), "157.5", "159", "157", "158.5", 126)),
        builder.normalize(_bar(datetime(2026, 3, 16, 7, 0, tzinfo=ny), "158.5", "160", "158", "159.5", 128)),
    ]

    result = build_resampled_bars(source, target_timeframe="1h", bar_builder=builder)

    assert result.skipped_bucket_count == 0
    assert len(result.bars) == 1
    assert result.bars[0].timeframe == "60m"
