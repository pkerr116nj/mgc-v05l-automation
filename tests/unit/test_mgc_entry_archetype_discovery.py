from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mgc_v05l.app.mgc_entry_archetype_discovery import (
    DiscoveryConfig,
    MinuteBar,
    _build_event_candidates,
    _build_nested_cohorts,
    _extract_pre_entry_features,
    _forward_outcomes,
    _session_key,
    run_mgc_entry_archetype_discovery,
)


def _bar(ts: datetime, open_px: float, high: float, low: float, close: float) -> MinuteBar:
    return MinuteBar(symbol="MGC", ts=ts, open=open_px, high=high, low=low, close=close, volume=1.0)


def _steady_bars(count: int = 90) -> list[MinuteBar]:
    start = datetime(2019, 1, 2, 14, 0, tzinfo=timezone.utc)
    bars: list[MinuteBar] = []
    price = 100.0
    for offset in range(count):
        price += 0.05
        bars.append(_bar(start + timedelta(minutes=offset), price - 0.02, price + 0.08, price - 0.08, price + 0.03))
    return bars


def test_forward_outcomes_use_only_bars_after_decision() -> None:
    bars = [
        _bar(datetime(2019, 1, 2, 14, 0, tzinfo=timezone.utc), 99.0, 101.0, 98.0, 100.0),
        _bar(datetime(2019, 1, 2, 14, 1, tzinfo=timezone.utc), 100.0, 100.4, 99.8, 100.2),
        _bar(datetime(2019, 1, 2, 14, 2, tzinfo=timezone.utc), 100.2, 101.2, 100.1, 101.0),
    ]

    returns, mfes, maes = _forward_outcomes(session_bars=bars, index=0, direction="LONG", horizons=(1, 2))

    assert returns[1] == 0.2
    assert returns[2] == 1.0
    assert mfes[2] == 1.2
    assert maes[2] == 0.2


def test_pre_entry_features_do_not_change_when_future_bars_change() -> None:
    bars = _steady_bars(75)
    baseline = _extract_pre_entry_features(session_bars=bars, index=65, direction="LONG")
    changed = list(bars)
    changed[70] = _bar(changed[70].ts, 250.0, 300.0, 200.0, 275.0)

    after_future_mutation = _extract_pre_entry_features(session_bars=changed, index=65, direction="LONG")

    assert after_future_mutation == baseline


def test_event_candidate_builder_filters_to_eligible_sessions_and_builds_both_directions() -> None:
    bars = _steady_bars(90)
    key = _session_key(symbol="MGC", bar_end=bars[0].ts)
    eligible = {
        key: {
            "symbol": "MGC",
            "date": key[1],
            "session": key[2],
            "active_ratio": 0.9,
            "largest_intra_session_gap_minutes": 0,
            "suspicious_gap_count": 0,
            "eligible_for_replay": True,
        }
    }

    events = _build_event_candidates(bars=bars, eligible_sessions=eligible, horizons=(15, 30, 60), lookbacks=(5, 15, 30, 60))

    assert events
    assert {event.direction for event in events} == {"LONG", "SHORT"}
    assert all(event.eligible_for_replay for event in events)
    assert all(event.decision_ts < bars[-1].ts for event in events)


def test_nested_cohorts_are_capped_and_control_excludes_top_and_adverse_ids() -> None:
    bars = _steady_bars(120)
    key = _session_key(symbol="MGC", bar_end=bars[0].ts)
    eligible = {
        key: {
            "symbol": "MGC",
            "date": key[1],
            "session": key[2],
            "active_ratio": 0.9,
            "largest_intra_session_gap_minutes": 0,
            "suspicious_gap_count": 0,
            "eligible_for_replay": True,
        }
    }
    events = _build_event_candidates(bars=bars, eligible_sessions=eligible, horizons=(15, 30, 60), lookbacks=(5, 15, 30, 60))

    cohorts = _build_nested_cohorts(events=events, cohort_sizes=(5, 10, 20), random_control_count=12, random_seed=1)

    assert len(cohorts["favorable_top5"]) == 5
    assert len(cohorts["favorable_top10"]) == 10
    assert {event.event_id for event in cohorts["favorable_top5"]}.issubset({event.event_id for event in cohorts["favorable_top10"]})
    excluded = {event.event_id for event in cohorts["favorable_top20"]} | {event.event_id for event in cohorts["adverse_bottom20"]}
    assert not ({event.event_id for event in cohorts["random_control"]} & excluded)


def test_run_discovery_writes_research_only_summary(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw_bars" / "databento_minute_backfill" / "symbol=MGC"
    partition = raw_root / "year=2019" / "month=01"
    partition.mkdir(parents=True)
    bars = _steady_bars(125)

    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "symbol": "MGC",
                    "bar_end": bar.ts,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                    "research_artifact": True,
                    "runtime_artifact": False,
                }
                for bar in bars
            ]
        ),
        partition / "bars.parquet",
    )
    key = _session_key(symbol="MGC", bar_end=bars[0].ts)
    quality_path = tmp_path / "quality.json"
    quality_path.write_text(
        json.dumps(
            {
                "session_coverage_summary": {"eligible_count": 1, "row_count": 1},
                "suspicious_gap_count": 0,
                "session_coverage": [
                    {
                        "symbol": "MGC",
                        "date": key[1],
                        "session": key[2],
                        "active_ratio": 1.0,
                        "largest_intra_session_gap_minutes": 0,
                        "suspicious_gap_count": 0,
                        "eligible_for_replay": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    summary = run_mgc_entry_archetype_discovery(
        config=DiscoveryConfig(
            output_root=tmp_path / "reports",
            raw_bars_root=raw_root,
            quality_audit_path=quality_path,
            canonical_sqlite_path=tmp_path / "missing.sqlite3",
            cohort_sizes=(5, 10, 20),
            random_control_count=10,
        )
    )

    assert summary["final_classification"] == "MGC_FULL_DATASET_ENTRY_ARCHETYPE_DISCOVERY_COMPLETE"
    assert summary["runtime_artifact"] is False
    assert summary["can_submit"] is False
    assert summary["live_money_eligible"] is False
    assert Path(summary["artifact_paths"]["summary_json"]).exists()
