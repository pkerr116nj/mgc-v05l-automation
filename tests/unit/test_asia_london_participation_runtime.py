from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import mgc_v05l.app.asia_london_participation_runtime as asia_runtime
from mgc_v05l.domain.models import Bar


def _bar(symbol: str, end_ts: datetime, *, open_px: str, high_px: str, low_px: str, close_px: str, volume: int = 100) -> Bar:
    return Bar(
        bar_id=f"{symbol}|5m|{end_ts.astimezone(ZoneInfo('UTC')).isoformat()}",
        symbol=symbol,
        timeframe="5m",
        start_ts=end_ts - timedelta(minutes=5),
        end_ts=end_ts,
        open=Decimal(open_px),
        high=Decimal(high_px),
        low=Decimal(low_px),
        close=Decimal(close_px),
        volume=volume,
        is_final=True,
        session_asia=True,
        session_london=False,
        session_us=False,
        session_allowed=True,
    )


def test_asia_london_live_observation_writes_predicate_and_near_miss_artifacts(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("MGC_ASIA_LONDON_INSTRUMENTATION_DIR", str(tmp_path))
    definition = asia_runtime.ASIA_LONDON_RUNTIME_BY_SOURCE[asia_runtime.GC_ASIA_LONDON_LONG_V5_SOURCE]
    start = datetime(2026, 4, 29, 18, 5, tzinfo=ZoneInfo("America/New_York"))
    segment_bars = [
        _bar("GC", start + timedelta(minutes=5 * index), open_px="100.0", high_px="100.5", low_px="99.8", close_px="100.2")
        for index in range(9)
    ]
    # Make the final bar a dip-reclaim candidate on the strict latest-bar decision.
    segment_bars[-2] = _bar("GC", start + timedelta(minutes=35), open_px="100.2", high_px="100.4", low_px="99.9", close_px="100.0")
    segment_bars[-1] = _bar("GC", start + timedelta(minutes=40), open_px="100.0", high_px="100.8", low_px="100.0", close_px="100.7")
    row = asia_runtime._build_asia_london_live_observation(  # noqa: SLF001
        lane_id="gc_1x_asia_london_participation_long",
        definition=definition,
        current_bar=segment_bars[-1],
        session_label=asia_runtime.ENTRY_SEGMENT,
        segment_bars=segment_bars,
        strict_gate_pass=True,
        strict_gate_fail_reason=None,
        entry_index=len(segment_bars) - 1,
        entry_reason="dip_reclaim_or_bar8",
        floor_reason=None,
    )

    asia_runtime._record_asia_london_live_observation(row)  # noqa: SLF001

    predicate_rows = [
        json.loads(line)
        for line in (tmp_path / "asia_london_live_predicate_trace.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert predicate_rows[-1]["candidate_score_bucket"] == "A+"
    assert predicate_rows[-1]["current_strict_candidate"] is True
    near_miss_rows = list(csv.DictReader((tmp_path / "asia_london_live_near_miss_trace.csv").open(encoding="utf-8")))
    assert near_miss_rows[-1]["candidate_score_bucket"] == "A+"
    score_rows = list(csv.DictReader((tmp_path / "asia_london_score_bucket_live_observation.csv").open(encoding="utf-8")))
    assert score_rows[-1]["research_score_candidate"] == "True"
