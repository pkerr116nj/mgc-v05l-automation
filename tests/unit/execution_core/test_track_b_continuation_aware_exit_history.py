from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_continuation_aware_exit_history import (
    CONTINUATION_EXIT_HISTORY_EMPTY,
    CONTINUATION_EXIT_HISTORY_MALFORMED,
    CONTINUATION_EXIT_HISTORY_PARTIAL,
    CONTINUATION_EXIT_HISTORY_READY,
    TrackBContinuationAwareExitHistoryConfig,
    build_continuation_aware_exit_history,
    write_continuation_aware_exit_history,
)


def test_empty_history_is_calm_and_non_authority(tmp_path: Path) -> None:
    payload = _history(tmp_path)

    assert payload["history_classification"] == CONTINUATION_EXIT_HISTORY_EMPTY
    assert payload["strategy_count"] == 0
    assert payload["total_events"] == 0
    assert payload["by_strategy"] == []
    assert payload["dry_run_only"] is True
    assert payload["not_order_authority"] is True
    assert payload["not_lifecycle_authority"] is True
    assert payload["not_routing_authority"] is True


def test_single_asian_drift_hold_event(tmp_path: Path) -> None:
    _append_event(tmp_path, _preview("asian_drift_v1", "HOLD_CONTINUATION_CONFIRMED"))

    payload = _history(tmp_path)

    assert payload["history_classification"] == CONTINUATION_EXIT_HISTORY_READY
    assert payload["strategy_count"] == 1
    assert payload["total_events"] == 1
    assert payload["lookback_event_count"] == 1
    assert payload["latest_strategy_id"] == "asian_drift_v1"
    row = payload["by_strategy"][0]
    assert row["strategy_id"] == "asian_drift_v1"
    assert row["symbol"] == "MGC"
    assert row["latest_exit_state"] == "HOLD_CONTINUATION_CONFIRMED"
    assert row["hold_count"] == 1
    assert row["exit_preview_count"] == 0
    assert "diagnostic dry-run only" in row["latest_operator_summary"]


def test_mixed_hold_exit_and_insufficient_data_counts(tmp_path: Path) -> None:
    _append_event(tmp_path, _preview("asian_drift_v1", "HOLD_CONTINUATION_CONFIRMED"))
    _append_event(tmp_path, _preview("asian_drift_v1", "EXIT_DECAY_DETECTED"))
    _append_event(tmp_path, _preview("ASIA_EARLY_PAUSE_RESUME_SHORT_V1", "INSUFFICIENT_DATA_HOLD_OR_FALLBACK"))
    _append_event(tmp_path, _preview("ASIA_EARLY_PAUSE_RESUME_SHORT_V1", "EXIT_SAFE_STATE_OVERRIDE"))

    payload = _history(tmp_path)

    assert payload["history_classification"] == CONTINUATION_EXIT_HISTORY_READY
    assert payload["strategy_count"] == 2
    rows = {row["strategy_id"]: row for row in payload["by_strategy"]}
    assert rows["asian_drift_v1"]["hold_count"] == 1
    assert rows["asian_drift_v1"]["exit_preview_count"] == 1
    assert rows["ASIA_EARLY_PAUSE_RESUME_SHORT_V1"]["insufficient_data_count"] == 1
    assert rows["ASIA_EARLY_PAUSE_RESUME_SHORT_V1"]["hard_override_count"] == 1


def test_malformed_jsonl_row_does_not_crash_rollup(tmp_path: Path) -> None:
    _append_event(tmp_path, _preview("asian_drift_v1", "HOLD_CONTINUATION_CONFIRMED"))
    _event_log(tmp_path).write_text(_event_log(tmp_path).read_text(encoding="utf-8") + "{nope\n", encoding="utf-8")

    payload = _history(tmp_path)

    assert payload["history_classification"] == CONTINUATION_EXIT_HISTORY_PARTIAL
    assert payload["total_events"] == 1
    assert payload["malformed_row_count"] == 1
    assert payload["malformed_rows_sample"][0]["line_number"] == 2


def test_malformed_only_history_is_reported(tmp_path: Path) -> None:
    _event_log(tmp_path).parent.mkdir(parents=True, exist_ok=True)
    _event_log(tmp_path).write_text("{not json\n", encoding="utf-8")

    payload = _history(tmp_path)

    assert payload["history_classification"] == CONTINUATION_EXIT_HISTORY_MALFORMED
    assert payload["total_events"] == 0
    assert payload["malformed_row_count"] == 1


def test_should_request_close_fixture_remains_diagnostic_only(tmp_path: Path) -> None:
    _append_event(
        tmp_path,
        {
            **_preview("asian_drift_v1", "HOLD_CONTINUATION_CONFIRMED"),
            "should_request_close": True,
            "dry_run_only": False,
            "not_order_authority": False,
            "not_lifecycle_authority": False,
        },
    )

    payload = _history(tmp_path)

    assert payload["by_strategy"][0]["exit_preview_count"] == 1
    assert payload["by_strategy"][0]["recent_states"][0]["should_request_close"] is True
    assert payload["dry_run_only"] is True
    assert payload["not_order_authority"] is True
    assert payload["not_lifecycle_authority"] is True
    assert payload["broker_mutation_allowed"] is False
    assert payload["lifecycle_mutation_allowed"] is False


def test_bounded_lookback_keeps_latest_events(tmp_path: Path) -> None:
    base = datetime(2026, 5, 24, 12, 0, tzinfo=UTC)
    for index in range(12):
        _append_event(
            tmp_path,
            _preview(
                "asian_drift_v1",
                "HOLD_CONTINUATION_CONFIRMED",
                generated_at=(base + timedelta(minutes=index)).isoformat(),
            ),
        )

    payload = _history(tmp_path, lookback_event_count=5)

    assert payload["total_events"] == 12
    assert payload["lookback_event_count"] == 5
    assert payload["by_strategy"][0]["recent_states"][0]["generated_at"] == "2026-05-24T12:07:00+00:00"
    assert payload["latest_generated_at"] == "2026-05-24T12:11:00+00:00"


def test_zero_lookback_keeps_count_but_no_strategy_rows(tmp_path: Path) -> None:
    _append_event(tmp_path, _preview("asian_drift_v1", "HOLD_CONTINUATION_CONFIRMED"))

    payload = _history(tmp_path, lookback_event_count=0)

    assert payload["history_classification"] == CONTINUATION_EXIT_HISTORY_READY
    assert payload["total_events"] == 1
    assert payload["lookback_event_count"] == 0
    assert payload["strategy_count"] == 0
    assert payload["by_strategy"] == []


def test_latest_preview_used_when_event_log_empty(tmp_path: Path) -> None:
    _latest_preview(tmp_path).parent.mkdir(parents=True, exist_ok=True)
    _latest_preview(tmp_path).write_text(
        json.dumps(_preview("asian_drift_v1", "HOLD_CONTINUATION_CONFIRMED")),
        encoding="utf-8",
    )

    payload = _history(tmp_path)

    assert payload["history_classification"] == CONTINUATION_EXIT_HISTORY_READY
    assert payload["total_events"] == 1
    assert payload["latest_strategy_id"] == "asian_drift_v1"


def test_dashboard_projection_is_not_consumed(tmp_path: Path) -> None:
    projection = tmp_path / "outputs/operator_dashboard/runtime/latest_continuation_aware_exit_history.json"
    projection.parent.mkdir(parents=True, exist_ok=True)
    projection.write_text(
        json.dumps({"history_classification": "CONTINUATION_EXIT_HISTORY_READY", "total_events": 999}),
        encoding="utf-8",
    )

    payload = _history(tmp_path)

    assert payload["history_classification"] == CONTINUATION_EXIT_HISTORY_EMPTY
    assert payload["total_events"] == 0
    assert payload["dashboard_projection_consumed"] is False


def test_writer_emits_json_artifact(tmp_path: Path) -> None:
    _append_event(tmp_path, _preview("asian_drift_v1", "HOLD_CONTINUATION_CONFIRMED"))
    config = _config(tmp_path)
    payload = build_continuation_aware_exit_history(config=config)

    path = write_continuation_aware_exit_history(config=config, payload=payload)

    assert path.exists()
    assert json.loads(path.read_text(encoding="utf-8"))["history_classification"] == CONTINUATION_EXIT_HISTORY_READY


def _history(tmp_path: Path, *, lookback_event_count: int = 100) -> dict:
    return build_continuation_aware_exit_history(
        config=_config(tmp_path, lookback_event_count=lookback_event_count),
        now=datetime(2026, 5, 24, 13, 0, tzinfo=UTC),
    )


def _config(tmp_path: Path, *, lookback_event_count: int = 100) -> TrackBContinuationAwareExitHistoryConfig:
    return TrackBContinuationAwareExitHistoryConfig(
        repo_root=tmp_path,
        lookback_event_count=lookback_event_count,
    )


def _append_event(tmp_path: Path, payload: dict) -> None:
    path = _event_log(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def _preview(
    strategy_id: str,
    exit_state: str,
    *,
    generated_at: str = "2026-05-24T12:00:00+00:00",
) -> dict:
    return {
        "generated_at": generated_at,
        "strategy_id": strategy_id,
        "symbol": "MGC" if strategy_id == "asian_drift_v1" else "MNQ",
        "exit_policy_id": "TIME_PLUS_CONTINUATION_EXIT_V1",
        "exit_profile_id": "ASIAN_DRIFT_CONTINUATION_LONG_LEASH_V1"
        if strategy_id == "asian_drift_v1"
        else "ASIA_EARLY_PAUSE_RESUME_SHORT_MEDIUM_LEASH_V1",
        "exit_state": exit_state,
        "continuation_quality_state": "STRONG_ALIGNED_CONTINUATION",
        "should_request_close": False,
        "dry_run_only": True,
        "not_order_authority": True,
        "not_lifecycle_authority": True,
    }


def _event_log(tmp_path: Path) -> Path:
    return (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "continuation_aware_exit"
        / "continuation_aware_exit_previews.jsonl"
    )


def _latest_preview(tmp_path: Path) -> Path:
    return (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "continuation_aware_exit"
        / "latest_continuation_aware_exit_preview.json"
    )
