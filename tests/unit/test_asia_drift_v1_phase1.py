from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from mgc_v05l.research.asia_drift import (
    ASIA_DRIFT_BUILD,
    ASIA_DRIFT_LONG,
    ASIA_DRIFT_PRE_HANDOFF,
    STRICT_CURRENT,
    DISQUALIFYING_PULLBACK,
    NORMAL_PULLBACK,
    STATE_ENTRY_ARMED,
    STATE_THESIS_INVALIDATED,
    derive_session_scope,
    run_asia_drift_phase1_from_bars,
)
from mgc_v05l.research.trend_participation.models import ResearchBar


NY = ZoneInfo("America/New_York")


def _bar(*, end_ts: datetime, open_: float, high: float, low: float, close: float, instrument: str = "MGC") -> ResearchBar:
    return ResearchBar(
        instrument=instrument,
        timeframe="5m",
        start_ts=end_ts - timedelta(minutes=5),
        end_ts=end_ts,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=100,
        session_label="ASIA",
        session_segment="ASIA",
        source="synthetic",
        provenance="unit_test",
    )


def _uptrend_session_with_pullback() -> list[ResearchBar]:
    base = datetime(2026, 3, 2, 18, 0, tzinfo=NY)
    bars: list[ResearchBar] = []
    price = 100.0
    for index in range(10):
        if index < 8:
            open_ = price + 0.22
            close = open_ + 0.38
            low = open_ - 0.08
            high = close + 0.06
        elif index == 8:
            open_ = price + 0.08
            close = open_ - 0.26
            low = close - 0.10
            high = open_ + 0.05
        else:
            open_ = price + 0.04
            close = open_ - 0.24
            low = close - 0.10
            high = open_ + 0.04
        bars.append(_bar(end_ts=base + timedelta(minutes=index * 5), open_=open_, high=high, low=low, close=close))
        price = close
    return bars


def _uptrend_session_with_break() -> list[ResearchBar]:
    bars = _uptrend_session_with_pullback()
    base = datetime(2026, 3, 2, 18, 0, tzinfo=NY)
    bars.append(
        _bar(
            end_ts=base + timedelta(minutes=50),
            open_=bars[-1].close - 0.10,
            high=bars[-1].close + 0.05,
            low=bars[-1].close - 1.90,
            close=bars[-1].close - 1.55,
        )
    )
    return bars


def _uptrend_session_with_pullback_first_bar_at_1805() -> list[ResearchBar]:
    return [
        ResearchBar(
            instrument=bar.instrument,
            timeframe=bar.timeframe,
            start_ts=bar.start_ts + timedelta(minutes=5),
            end_ts=bar.end_ts + timedelta(minutes=5),
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            session_label=bar.session_label,
            session_segment=bar.session_segment,
            source=bar.source,
            provenance=bar.provenance,
        )
        for bar in _uptrend_session_with_pullback()
    ]


def _chop_session() -> list[ResearchBar]:
    base = datetime(2026, 3, 2, 18, 0, tzinfo=NY)
    prices = [100.0, 100.2, 99.95, 100.18, 99.92, 100.16, 99.90, 100.14, 99.88, 100.12]
    bars: list[ResearchBar] = []
    for index, close in enumerate(prices):
        open_ = prices[index - 1] if index > 0 else 100.0
        high = max(open_, close) + 0.18
        low = min(open_, close) - 0.18
        bars.append(_bar(end_ts=base + timedelta(minutes=index * 5), open_=open_, high=high, low=low, close=close))
    return bars


def test_session_scope_tags_boundaries() -> None:
    build_tag = derive_session_scope(
        instrument="MGC",
        timeframe="5m",
        bar_end_ts=datetime(2026, 3, 2, 18, 0, tzinfo=NY),
        anchor_observed=True,
    )
    pre_handoff_tag = derive_session_scope(
        instrument="MGC",
        timeframe="5m",
        bar_end_ts=datetime(2026, 3, 3, 0, 30, tzinfo=NY),
        anchor_observed=True,
    )
    timeout_tag = derive_session_scope(
        instrument="MGC",
        timeframe="5m",
        bar_end_ts=datetime(2026, 3, 3, 2, 56, tzinfo=NY),
        anchor_observed=True,
    )

    assert build_tag.in_scope is True
    assert build_tag.subphase == ASIA_DRIFT_BUILD
    assert build_tag.entry_window_open is True
    assert pre_handoff_tag.in_scope is True
    assert pre_handoff_tag.subphase == ASIA_DRIFT_PRE_HANDOFF
    assert timeout_tag.in_scope is False
    assert timeout_tag.session_timeout is True


def test_feature_engine_identifies_long_drift_on_orderly_session(tmp_path: Path) -> None:
    run = run_asia_drift_phase1_from_bars(output_dir=tmp_path / "phase1_long", bars_5m=_uptrend_session_with_pullback())
    tradable_rows = [row for row in run.feature_rows if row.regime == ASIA_DRIFT_LONG]
    assert tradable_rows
    strongest = max(tradable_rows, key=lambda row: row.long_drift_score)

    assert strongest.regime == ASIA_DRIFT_LONG
    assert strongest.long_drift_score > strongest.short_drift_score
    assert strongest.efficiency_ratio_12 > 0.5
    assert any(row.pullback_state in {NORMAL_PULLBACK, "STRETCHED_BUT_VALID"} for row in tradable_rows)


def test_feature_engine_treats_first_1805_bar_as_anchor_observed(tmp_path: Path) -> None:
    run = run_asia_drift_phase1_from_bars(
        output_dir=tmp_path / "phase1_long_1805_anchor",
        bars_5m=_uptrend_session_with_pullback_first_bar_at_1805(),
    )

    assert any(row.anchor_observed for row in run.feature_rows)
    assert any(row.regime == ASIA_DRIFT_LONG for row in run.feature_rows)


def test_feature_engine_rejects_chop_session(tmp_path: Path) -> None:
    run = run_asia_drift_phase1_from_bars(output_dir=tmp_path / "phase1_chop", bars_5m=_chop_session())
    last = run.feature_rows[-1]

    assert last.regime == "NO_TRADE"
    assert last.chop_veto is True


def test_pullback_classifier_distinguishes_valid_vs_disqualifying(tmp_path: Path) -> None:
    valid_run = run_asia_drift_phase1_from_bars(output_dir=tmp_path / "phase1_valid_pb", bars_5m=_uptrend_session_with_pullback())
    invalid_run = run_asia_drift_phase1_from_bars(output_dir=tmp_path / "phase1_invalid_pb", bars_5m=_uptrend_session_with_break())

    assert any(
        row.pullback_state in {NORMAL_PULLBACK, "STRETCHED_BUT_VALID"}
        for row in valid_run.feature_rows
        if row.regime == ASIA_DRIFT_LONG
    )
    assert invalid_run.feature_rows[-1].pullback_state == DISQUALIFYING_PULLBACK


def test_state_machine_reaches_entry_armed_then_invalidated(tmp_path: Path) -> None:
    run = run_asia_drift_phase1_from_bars(
        output_dir=tmp_path / "phase1_states",
        bars_5m=_uptrend_session_with_break(),
        calibration_profile_name=STRICT_CURRENT,
    )
    states = [row.state for row in run.state_rows]

    assert STATE_ENTRY_ARMED in states
    assert STATE_THESIS_INVALIDATED in states
    assert states.index(STATE_ENTRY_ARMED) < states.index(STATE_THESIS_INVALIDATED)


def test_artifact_generation_writes_summary_shapes(tmp_path: Path) -> None:
    run = run_asia_drift_phase1_from_bars(output_dir=tmp_path / "phase1_artifacts", bars_5m=_uptrend_session_with_pullback())
    summary_payload = json.loads(run.artifacts.summary_json_path.read_text(encoding="utf-8"))
    candidate_manifest = json.loads(run.artifacts.candidate_manifest_path.read_text(encoding="utf-8"))

    assert run.artifacts.feature_rows_path.exists()
    assert run.artifacts.state_rows_path.exists()
    assert "headline_counts" in summary_payload
    assert "regime_counts" in summary_payload
    assert "candidate_manifest" in summary_payload
    assert "promising_sessions" in candidate_manifest
