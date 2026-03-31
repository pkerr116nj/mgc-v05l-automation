from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from mgc_v05l.app.gc_mgc_london_open_acceptance_continuation_research import (
    StoredBarContext,
    StoredFeature,
    _evaluate_symbol,
)
from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.domain.models import Bar


def _bar(symbol: str, end_ts: datetime, open_: str, high: str, low: str, close: str) -> Bar:
    return Bar(
        bar_id=f"{symbol}|5m|{end_ts.astimezone(ZoneInfo('UTC')).isoformat()}",
        symbol=symbol,
        timeframe="5m",
        start_ts=end_ts - timedelta(minutes=5),
        end_ts=end_ts,
        open=Decimal(open_),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=100,
        is_final=True,
        session_asia=False,
        session_london=True,
        session_us=False,
        session_allowed=True,
    )


def test_sibling_branch_admits_london_open_acceptance_shape_without_mutating_current_family() -> None:
    settings = load_settings_from_files(
        [
            "config/base.yaml",
            "config/live.yaml",
            "config/probationary_pattern_engine.yaml",
        ]
    )
    tz = ZoneInfo("America/New_York")
    contexts = [
        StoredBarContext(
            bar=_bar("GC", datetime(2026, 3, 24, 2, 55, tzinfo=tz), "4398.0", "4400.0", "4397.0", "4399.0"),
            feature=StoredFeature(atr=Decimal("10"), velocity=Decimal("1.0"), bar_range=Decimal("1.5")),
        ),
        StoredBarContext(
            bar=_bar("GC", datetime(2026, 3, 24, 3, 0, tzinfo=tz), "4399.0", "4402.0", "4398.8", "4401.0"),
            feature=StoredFeature(atr=Decimal("10"), velocity=Decimal("5.0"), bar_range=Decimal("2.0")),
        ),
        StoredBarContext(
            bar=_bar("GC", datetime(2026, 3, 24, 3, 5, tzinfo=tz), "4402.2", "4407.5", "4402.0", "4406.0"),
            feature=StoredFeature(atr=Decimal("10"), velocity=Decimal("6.0"), bar_range=Decimal("5.5")),
        ),
    ]

    rows = _evaluate_symbol(contexts=contexts, settings=settings)

    assert len(rows) == 1
    row = rows[0]
    assert row.signal_timestamp == "2026-03-24T03:05:00-04:00"
    assert row.baseline_current_family_passed is False
    assert row.baseline_detail["breakout_bar_slope_is_flat"] is False
    assert row.baseline_detail["breakout_bar_expansion_is_normal"] is False
    assert row.sibling_branch_passed is True
    assert row.sibling_detail["breakout_normalized_slope_in_range"] is True
    assert row.sibling_detail["breakout_range_expansion_ratio_in_range"] is True
    assert row.sibling_detail["signal_bar_low_greater_equal_breakout_high"] is True
