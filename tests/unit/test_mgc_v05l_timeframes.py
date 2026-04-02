from __future__ import annotations

import pytest

from mgc_v05l.market_data.timeframes import normalize_timeframe_label, timeframe_aliases, timeframe_minutes


def test_normalize_timeframe_label_supports_hour_aliases() -> None:
    assert normalize_timeframe_label("60m") == "60m"
    assert normalize_timeframe_label("1h") == "60m"
    assert normalize_timeframe_label("2H") == "120m"


def test_timeframe_minutes_and_aliases_are_consistent() -> None:
    assert timeframe_minutes("4h") == 240
    assert timeframe_aliases("240m") == ["240m", "4h"]


def test_normalize_timeframe_label_rejects_unsupported_formats() -> None:
    with pytest.raises(ValueError):
        normalize_timeframe_label("1d")
