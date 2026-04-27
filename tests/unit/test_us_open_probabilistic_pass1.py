from __future__ import annotations

import csv
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from mgc_v05l.research.trend_participation.models import ResearchBar
from mgc_v05l.research.us_open_follow_through import probabilistic_pass1 as pass1


NY = ZoneInfo("America/New_York")


def _bar(symbol: str, start_local: datetime, open_: float, high: float, low: float, close: float, volume: int = 10, timeframe: str = "1m") -> ResearchBar:
    start_ts = start_local.astimezone(NY)
    end_ts = (start_local + timedelta(minutes=int(timeframe.rstrip("m")))).astimezone(NY)
    return ResearchBar(
        instrument=symbol,
        timeframe=timeframe,
        start_ts=start_ts,
        end_ts=end_ts,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        session_label="TEST",
        session_segment="TEST",
        source="test",
    )


def _one_minute_day(symbol: str, session_date: datetime, *, open_drive_up: bool) -> list[ResearchBar]:
    rows: list[ResearchBar] = []
    rows.append(_bar(symbol, session_date.replace(hour=4, minute=0), 100.0, 100.2, 99.8, 100.1))
    rows.append(_bar(symbol, session_date.replace(hour=9, minute=29), 100.1, 100.2, 99.9, 100.0))
    opening_levels = [(100.0, 100.3), (100.3, 100.6), (100.6, 101.0)] if open_drive_up else [(100.0, 99.7), (99.7, 99.4), (99.4, 99.0)]
    rows.append(_bar(symbol, session_date.replace(hour=9, minute=30), opening_levels[0][0], max(opening_levels[0]) + 0.1, min(opening_levels[0]) - 0.1, opening_levels[0][1]))
    rows.append(_bar(symbol, session_date.replace(hour=9, minute=45), opening_levels[1][0], max(opening_levels[1]) + 0.1, min(opening_levels[1]) - 0.1, opening_levels[1][1]))
    rows.append(_bar(symbol, session_date.replace(hour=9, minute=59), opening_levels[2][0], max(opening_levels[2]) + 0.1, min(opening_levels[2]) - 0.1, opening_levels[2][1]))
    rows.append(_bar(symbol, session_date.replace(hour=10, minute=29), opening_levels[2][1], opening_levels[2][1] + (0.4 if open_drive_up else 0.6), opening_levels[2][1] + (-0.2 if open_drive_up else -0.4), opening_levels[2][1] + (0.3 if open_drive_up else -0.3)))
    rows.append(_bar(symbol, session_date.replace(hour=10, minute=59), opening_levels[2][1] + (0.3 if open_drive_up else -0.3), opening_levels[2][1] + (0.7 if open_drive_up else 0.1), opening_levels[2][1] + (0.1 if open_drive_up else -0.7), opening_levels[2][1] + (0.5 if open_drive_up else -0.5)))
    rows.append(_bar(symbol, session_date.replace(hour=11, minute=59), opening_levels[2][1] + (0.5 if open_drive_up else -0.5), opening_levels[2][1] + (1.0 if open_drive_up else 0.2), opening_levels[2][1] + (0.2 if open_drive_up else -1.0), opening_levels[2][1] + (0.7 if open_drive_up else -0.7)))
    rows.append(_bar(symbol, session_date.replace(hour=15, minute=29), opening_levels[2][1] + (0.7 if open_drive_up else -0.7), opening_levels[2][1] + (1.1 if open_drive_up else 0.2), opening_levels[2][1] + (0.3 if open_drive_up else -1.1), opening_levels[2][1] + (0.8 if open_drive_up else -0.8)))
    rows.append(_bar(symbol, session_date.replace(hour=15, minute=59), opening_levels[2][1] + (0.8 if open_drive_up else -0.8), opening_levels[2][1] + (1.2 if open_drive_up else 0.3), opening_levels[2][1] + (0.4 if open_drive_up else -1.2), opening_levels[2][1] + (0.9 if open_drive_up else -0.9)))
    return rows


def _resample_rows(symbol: str, base_date: datetime) -> tuple[list[ResearchBar], list[ResearchBar], list[ResearchBar]]:
    bars_5m = [
        _bar(symbol, base_date.replace(hour=9, minute=45), 100.0, 100.8, 99.9, 100.7, timeframe="5m"),
        _bar(symbol, base_date.replace(hour=9, minute=50), 100.7, 100.95, 100.5, 100.85, timeframe="5m"),
        _bar(symbol, base_date.replace(hour=9, minute=55), 100.85, 101.1, 100.75, 101.0, timeframe="5m"),
        _bar(symbol, base_date.replace(hour=10, minute=0), 101.0, 101.2, 100.9, 101.1, timeframe="5m"),
    ]
    bars_15m = [
        _bar(symbol, base_date.replace(hour=9, minute=15), 99.8, 100.5, 99.7, 100.4, timeframe="15m"),
        _bar(symbol, base_date.replace(hour=9, minute=30), 100.4, 100.9, 100.3, 100.8, timeframe="15m"),
        _bar(symbol, base_date.replace(hour=9, minute=45), 100.8, 101.1, 100.7, 101.0, timeframe="15m"),
        _bar(symbol, base_date.replace(hour=10, minute=0), 101.0, 101.3, 100.9, 101.2, timeframe="15m"),
    ]
    bars_60m = [
        _bar(symbol, base_date.replace(hour=7, minute=0), 99.0, 100.0, 98.9, 99.8, timeframe="60m"),
        _bar(symbol, base_date.replace(hour=8, minute=0), 99.8, 100.6, 99.7, 100.4, timeframe="60m"),
        _bar(symbol, base_date.replace(hour=9, minute=0), 100.4, 101.2, 100.3, 101.0, timeframe="60m"),
        _bar(symbol, base_date.replace(hour=10, minute=0), 101.0, 101.4, 100.9, 101.2, timeframe="60m"),
    ]
    return bars_5m, bars_15m, bars_60m


def test_run_probabilistic_pass1_writes_expected_artifacts(tmp_path: Path, monkeypatch) -> None:
    dev_date = datetime(2024, 6, 3, tzinfo=NY)
    holdout_date = datetime(2025, 6, 2, tzinfo=NY)
    bars_by_dataset: dict[tuple[str, str], list[ResearchBar]] = {}
    raw_by_symbol: dict[str, dict[str, list[float | datetime]]] = {}
    for symbol, is_up in (("ES", True), ("NQ", True)):
        bars_1m = _one_minute_day(symbol, dev_date, open_drive_up=is_up) + _one_minute_day(symbol, holdout_date, open_drive_up=is_up)
        bars_5m_dev, bars_15m_dev, bars_60m_dev = _resample_rows(symbol, dev_date)
        bars_5m_hold, bars_15m_hold, bars_60m_hold = _resample_rows(symbol, holdout_date)
        bars_by_dataset[(symbol, "1m")] = bars_1m
        bars_by_dataset[(symbol, "5m")] = bars_5m_dev + bars_5m_hold
        bars_by_dataset[(symbol, "15m")] = bars_15m_dev + bars_15m_hold
        bars_by_dataset[(symbol, "60m")] = bars_60m_dev + bars_60m_hold
        ordered = sorted(bars_1m, key=lambda item: item.end_ts)
        raw_by_symbol[symbol] = {
            "timestamps": [bar.end_ts for bar in ordered],
            "highs": [bar.high for bar in ordered],
            "lows": [bar.low for bar in ordered],
            "closes": [bar.close for bar in ordered],
        }

    def _fake_load_warehouse_bars(*, dataset_name: str, symbol: str, timeframe: str, **kwargs):
        del kwargs
        key = (symbol, timeframe)
        if dataset_name == "raw_bars_1m":
            return bars_by_dataset[key]
        return bars_by_dataset[key]

    monkeypatch.setattr(pass1, "_load_warehouse_bars", _fake_load_warehouse_bars)
    monkeypatch.setattr(pass1, "_load_raw_outcome_series", lambda **kwargs: raw_by_symbol[kwargs["symbol"]])
    monkeypatch.setattr(pass1, "load_vol_regime_rows", lambda warehouse_root: [
        {
            "vix_trade_date": "2024-06-03",
            "vix_asof_ts": "2024-06-02T20:15:00+00:00",
            "vix_close": 14.2,
            "vix_change_abs": -0.5,
            "vix_change_pct": -0.03,
            "vix_level_bucket": "LOW",
            "vix_change_bucket": "DOWN",
            "vix_combined_bucket": "LOW_DOWN",
        },
        {
            "vix_trade_date": "2025-06-02",
            "vix_asof_ts": "2025-06-01T20:15:00+00:00",
            "vix_close": 18.5,
            "vix_change_abs": 0.4,
            "vix_change_pct": 0.02,
            "vix_level_bucket": "MID",
            "vix_change_bucket": "UP",
            "vix_combined_bucket": "MID_UP",
        },
    ])

    result = pass1.run_probabilistic_pass1(
        warehouse_root=tmp_path / "warehouse",
        output_dir=tmp_path / "outputs",
        symbols=("ES", "NQ"),
        start_ts=datetime.fromisoformat("2024-01-01T00:00:00-05:00"),
        end_ts=datetime.fromisoformat("2025-12-31T23:59:00-05:00"),
    )

    assert result["summary"]["row_counts"]["candidate_rows"] == 4
    assert Path(result["artifacts"]["candidate_dataset_csv"]).exists()
    assert Path(result["artifacts"]["outcome_dataset_csv"]).exists()
    assert Path(result["artifacts"]["summary_markdown"]).exists()

    with Path(result["artifacts"]["pooled_summary_csv"]).open("r", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert {row["sample_split"] for row in rows} == {"development", "holdout"}

    with Path(result["artifacts"]["vix_conditioned_summary_csv"]).open("r", encoding="utf-8") as handle:
        vix_rows = list(csv.DictReader(handle))
    assert any(row["vix_level_bucket"] == "LOW" for row in vix_rows)
    assert any(row["vix_level_bucket"] == "MID" for row in vix_rows)


def test_cross_index_confirmation_and_directional_labels_are_present(tmp_path: Path, monkeypatch) -> None:
    date_local = datetime(2025, 6, 2, tzinfo=NY)
    es_bars = _one_minute_day("ES", date_local, open_drive_up=True)
    nq_bars = _one_minute_day("NQ", date_local, open_drive_up=True)
    bars_5m_es, bars_15m_es, bars_60m_es = _resample_rows("ES", date_local)
    bars_5m_nq, bars_15m_nq, bars_60m_nq = _resample_rows("NQ", date_local)
    ordered_es = sorted(es_bars, key=lambda item: item.end_ts)
    ordered_nq = sorted(nq_bars, key=lambda item: item.end_ts)
    lookup = {
        ("ES", "1m"): es_bars,
        ("ES", "5m"): bars_5m_es,
        ("ES", "15m"): bars_15m_es,
        ("ES", "60m"): bars_60m_es,
        ("NQ", "1m"): nq_bars,
        ("NQ", "5m"): bars_5m_nq,
        ("NQ", "15m"): bars_15m_nq,
        ("NQ", "60m"): bars_60m_nq,
    }
    raw_lookup = {
        "ES": {"timestamps": [bar.end_ts for bar in ordered_es], "highs": [bar.high for bar in ordered_es], "lows": [bar.low for bar in ordered_es], "closes": [bar.close for bar in ordered_es]},
        "NQ": {"timestamps": [bar.end_ts for bar in ordered_nq], "highs": [bar.high for bar in ordered_nq], "lows": [bar.low for bar in ordered_nq], "closes": [bar.close for bar in ordered_nq]},
    }
    monkeypatch.setattr(pass1, "_load_warehouse_bars", lambda *, dataset_name, symbol, timeframe, **kwargs: lookup[(symbol, timeframe)])
    monkeypatch.setattr(pass1, "_load_raw_outcome_series", lambda **kwargs: raw_lookup[kwargs["symbol"]])
    monkeypatch.setattr(pass1, "load_vol_regime_rows", lambda warehouse_root: [])
    result = pass1.run_probabilistic_pass1(
        warehouse_root=tmp_path / "warehouse",
        output_dir=tmp_path / "outputs",
        symbols=("ES", "NQ"),
        start_ts=datetime.fromisoformat("2025-01-01T00:00:00-05:00"),
        end_ts=datetime.fromisoformat("2025-12-31T23:59:00-05:00"),
    )
    with Path(result["artifacts"]["candidate_dataset_csv"]).open("r", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert all(row["cross_index_confirmation"] == "True" for row in rows)
    assert all(row["direction"] == "UP" for row in rows)
