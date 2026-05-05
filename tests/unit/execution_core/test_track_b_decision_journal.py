from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from mgc_v05l.execution_core.track_b_decision_journal import (
    TrackBDecisionJournalConfig,
    record_track_b_decision_journal_cycle,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 5, 8, 45, tzinfo=timezone.utc)


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def strategy_report(
    tmp_path: Path,
    strategy_id: str,
    *,
    conditions: dict[str, bool | None],
    emitted: bool = False,
    direction: str | None = None,
    input_event: dict[str, object] | None = None,
) -> Path:
    event_path = None
    if input_event is not None:
        event_path = write_json(tmp_path / "events" / f"{strategy_id}.json", input_event)
    report_path = tmp_path / "strategy_reports" / f"{strategy_id}.json"
    return write_json(
        report_path,
        {
            "generated_at": aware_now().isoformat(),
            "strategy_registry_id": strategy_id,
            "strategy_registry_rule_mode": strategy_id,
            "strategy_id": strategy_id,
            "rule_mode": strategy_id,
            "decision": direction or "NO_SIGNAL",
            "decision_reason": "unit decision",
            "signal_emitted": emitted,
            "signal_direction": direction,
            "signal_source": strategy_id,
            "real_strategy_signal": emitted,
            "rule_conditions": conditions,
            "rule_blockers": [f"{key}=false_or_missing" for key, value in conditions.items() if value is not True],
            "input_event_path": None if event_path is None else str(event_path),
            "report_json_path": str(report_path),
        },
    )


def runtime_report(
    tmp_path: Path,
    *,
    cycle_id: str = "cycle-001",
    strategies: list[dict[str, object]],
    candidate_signals: list[dict[str, object]] | None = None,
    suppressed_signals: list[dict[str, object]] | None = None,
    verdict: str = "TRACK_B_MULTI_STRATEGY_RUNTIME_NO_SIGNAL_NO_MUTATION",
) -> tuple[dict[str, object], Path]:
    report_path = tmp_path / "cycle" / cycle_id / "track_b_multi_strategy_runtime_cycle_report.json"
    payload = {
        "generated_at": aware_now().isoformat(),
        "track_b_multi_strategy_runtime_cycle_id": cycle_id,
        "multi_strategy_runtime_cycle_verdict": verdict,
        "evaluated_strategies": strategies,
        "candidate_signals": candidate_signals or [],
        "suppressed_signals": suppressed_signals or [],
        "arbitration_result": {},
        "readiness_invoked": False,
        "paper_proof_invoked": False,
        "submit_attempted": False,
        "broker_state_mutated": False,
        "live_money_readiness": False,
    }
    write_json(report_path, payload)
    return payload, report_path


def strategy_summary(
    strategy_id: str,
    report_path: Path | None,
    *,
    verdict: str = "NO_SIGNAL_NO_MUTATION",
    emitted: bool = False,
    direction: str | None = None,
    blocker: str | None = None,
) -> dict[str, object]:
    return {
        "strategy_id": strategy_id,
        "rule_mode": strategy_id,
        "strategy_runtime_verdict": verdict,
        "decision": direction or "NO_SIGNAL",
        "signal_emitted": emitted,
        "signal_direction": direction,
        "signal_source": strategy_id,
        "real_strategy_signal": emitted,
        "primary_blocker": blocker,
        "report_json_path": None if report_path is None else str(report_path),
        "paper_eligible": True,
        "live_money_eligible": False,
    }


def journal_config(**overrides: object) -> TrackBDecisionJournalConfig:
    payload = {
        "near_miss_score_threshold": Decimal("0.80"),
        "near_miss_distance_threshold": Decimal("0.05"),
        "no_setup_aggregate_interval_minutes": 15,
        "max_bytes": 1024 * 1024,
        "rotated_keep": 2,
    }
    payload.update(overrides)
    return TrackBDecisionJournalConfig(**payload)  # type: ignore[arg-type]


def test_boring_no_setup_cycles_do_not_create_full_journal_spam(tmp_path: Path) -> None:
    report_path = strategy_report(
        tmp_path,
        "FIRST_BULL_SNAP_TURN_V1",
        conditions={f"predicate_{index}": index < 2 for index in range(12)},
    )
    report, runtime_path = runtime_report(
        tmp_path,
        strategies=[strategy_summary("FIRST_BULL_SNAP_TURN_V1", report_path)],
    )

    result = record_track_b_decision_journal_cycle(
        runtime_cycle_report=report,
        runtime_cycle_report_json=runtime_path,
        output_root=tmp_path / "journal",
        config=journal_config(),
        now=aware_now(),
    )

    assert read_jsonl(result.active_journal_jsonl) == []
    assert len(read_jsonl(result.heartbeat_jsonl)) == 1
    assert result.summary["full_records_written"] == 0
    assert result.summary["ordinary_no_setup_aggregate_updates"] == 1


def test_repeated_no_setup_evaluations_aggregate_into_one_summary(tmp_path: Path) -> None:
    report_path = strategy_report(
        tmp_path,
        "FIRST_BEAR_SNAP_TURN_V1",
        conditions={f"predicate_{index}": False for index in range(12)},
    )
    report, runtime_path = runtime_report(
        tmp_path,
        strategies=[strategy_summary("FIRST_BEAR_SNAP_TURN_V1", report_path)],
    )
    root = tmp_path / "journal"

    record_track_b_decision_journal_cycle(
        runtime_cycle_report=report,
        runtime_cycle_report_json=runtime_path,
        output_root=root,
        config=journal_config(),
        now=aware_now(),
    )
    record_track_b_decision_journal_cycle(
        runtime_cycle_report=report,
        runtime_cycle_report_json=runtime_path,
        output_root=root,
        config=journal_config(),
        now=aware_now(),
    )

    aggregates = json.loads((root / "track_b_no_setup_aggregates.json").read_text(encoding="utf-8"))
    assert len(aggregates) == 1
    assert next(iter(aggregates.values()))["count"] == 2


def test_near_miss_writes_full_compact_record(tmp_path: Path) -> None:
    input_event = {
        "candle_timestamp": aware_now().isoformat(),
        "metadata": {"feature_diagnostics": {"close_location": "0.70"}},
    }
    conditions = {f"predicate_{index}": True for index in range(11)}
    conditions["bull_snap_close_strong"] = False
    report_path = strategy_report(
        tmp_path,
        "FIRST_BULL_SNAP_TURN_V1",
        conditions=conditions,
        input_event=input_event,
    )
    report, runtime_path = runtime_report(
        tmp_path,
        strategies=[strategy_summary("FIRST_BULL_SNAP_TURN_V1", report_path)],
    )

    result = record_track_b_decision_journal_cycle(
        runtime_cycle_report=report,
        runtime_cycle_report_json=runtime_path,
        output_root=tmp_path / "journal",
        config=journal_config(),
        now=aware_now(),
    )

    records = read_jsonl(result.active_journal_jsonl)
    assert len(records) == 1
    assert records[0]["journal_tier"] == "TIER_2_NEAR_MISS"
    assert records[0]["nearest_failed_predicate"]["predicate"] == "bull_snap_close_strong"
    assert records[0]["nearest_failed_predicate"]["distance_to_pass"] == "0.02"


def test_signal_writes_full_decision_record(tmp_path: Path) -> None:
    report_path = strategy_report(
        tmp_path,
        "FIRST_BEAR_SNAP_TURN_V1",
        conditions={f"predicate_{index}": True for index in range(12)},
        emitted=True,
        direction="SHORT",
    )
    report, runtime_path = runtime_report(
        tmp_path,
        verdict="TRACK_B_MULTI_STRATEGY_RUNTIME_SIGNAL_READY_NO_SUBMIT",
        strategies=[strategy_summary("FIRST_BEAR_SNAP_TURN_V1", report_path, verdict="SIGNAL_READY_NO_SUBMIT", emitted=True, direction="SHORT")],
        candidate_signals=[{"strategy_id": "FIRST_BEAR_SNAP_TURN_V1", "signal_direction": "SHORT"}],
    )

    result = record_track_b_decision_journal_cycle(
        runtime_cycle_report=report,
        runtime_cycle_report_json=runtime_path,
        output_root=tmp_path / "journal",
        config=journal_config(),
        now=aware_now(),
    )

    records = read_jsonl(result.active_journal_jsonl)
    assert records[0]["journal_tier"] == "TIER_3_SIGNAL_TRADE_DECISION"
    assert records[0]["signal_emitted"] is True
    assert records[0]["strategy_report_json"].endswith("FIRST_BEAR_SNAP_TURN_V1.json")


def test_suppression_writes_abnormal_record(tmp_path: Path) -> None:
    report_path = strategy_report(
        tmp_path,
        "FIRST_BULL_SNAP_TURN_V1",
        conditions={f"predicate_{index}": False for index in range(12)},
    )
    report, runtime_path = runtime_report(
        tmp_path,
        verdict="TRACK_B_MULTI_STRATEGY_RUNTIME_ARBITRATION_BLOCKED",
        strategies=[strategy_summary("FIRST_BULL_SNAP_TURN_V1", report_path)],
        suppressed_signals=[{"strategy_id": "FIRST_BULL_SNAP_TURN_V1"}],
    )

    result = record_track_b_decision_journal_cycle(
        runtime_cycle_report=report,
        runtime_cycle_report_json=runtime_path,
        output_root=tmp_path / "journal",
        config=journal_config(),
        now=aware_now(),
    )

    records = read_jsonl(result.active_journal_jsonl)
    assert records[0]["journal_tier"] == "TIER_4_ABNORMAL_SAFETY"
    assert records[0]["suppressed_signal_count"] == 1


def test_not_ready_after_evaluable_transition_writes_abnormal_record(tmp_path: Path) -> None:
    root = tmp_path / "journal"
    report_path = strategy_report(
        tmp_path,
        "FIRST_BULL_SNAP_TURN_V1",
        conditions={f"predicate_{index}": False for index in range(12)},
    )
    report, runtime_path = runtime_report(
        tmp_path,
        strategies=[strategy_summary("FIRST_BULL_SNAP_TURN_V1", report_path)],
    )
    record_track_b_decision_journal_cycle(
        runtime_cycle_report=report,
        runtime_cycle_report_json=runtime_path,
        output_root=root,
        config=journal_config(),
        now=aware_now(),
    )
    not_ready_report, not_ready_path = runtime_report(
        tmp_path,
        cycle_id="cycle-not-ready",
        strategies=[
            strategy_summary(
                "FIRST_BULL_SNAP_TURN_V1",
                None,
                verdict="NOT_READY",
                blocker="FIRST_BULL_SNAP_TURN_V1 input envelope was not supplied.",
            )
        ],
    )

    result = record_track_b_decision_journal_cycle(
        runtime_cycle_report=not_ready_report,
        runtime_cycle_report_json=not_ready_path,
        output_root=root,
        config=journal_config(),
        now=aware_now(),
    )

    records = read_jsonl(result.active_journal_jsonl)
    assert len(records) == 1
    assert records[0]["journal_tier"] == "TIER_4_ABNORMAL_SAFETY"
    assert records[0]["primary_blocker"] == "FIRST_BULL_SNAP_TURN_V1 input envelope was not supplied."


def test_full_record_contains_reconstruction_artifact_pointers(tmp_path: Path) -> None:
    input_event = {"candle_timestamp": aware_now().isoformat(), "metadata": {"feature_diagnostics": {"close_location": "0.74"}}}
    report_path = strategy_report(
        tmp_path,
        "FIRST_BULL_SNAP_TURN_V1",
        conditions={f"predicate_{index}": True for index in range(12)},
        emitted=True,
        direction="LONG",
        input_event=input_event,
    )
    report, runtime_path = runtime_report(
        tmp_path,
        verdict="TRACK_B_MULTI_STRATEGY_RUNTIME_SIGNAL_READY_NO_SUBMIT",
        strategies=[strategy_summary("FIRST_BULL_SNAP_TURN_V1", report_path, verdict="SIGNAL_READY_NO_SUBMIT", emitted=True, direction="LONG")],
        candidate_signals=[{"strategy_id": "FIRST_BULL_SNAP_TURN_V1", "signal_direction": "LONG"}],
    )

    result = record_track_b_decision_journal_cycle(
        runtime_cycle_report=report,
        runtime_cycle_report_json=runtime_path,
        output_root=tmp_path / "journal",
        config=journal_config(),
        now=aware_now(),
    )

    record = read_jsonl(result.active_journal_jsonl)[0]
    assert record["runtime_cycle_report_json"] == str(runtime_path)
    assert record["runtime_cycle_report_sha256"]
    assert record["strategy_report_json"] == str(report_path)
    assert record["strategy_report_sha256"]
    assert record["input_event_path"]
    assert record["input_event_sha256"]


def test_journal_rotation_compresses_and_retains_bounded_files(tmp_path: Path) -> None:
    root = tmp_path / "journal"
    active = root / "track_b_decision_journal.jsonl"
    root.mkdir()
    active.write_text("x" * 200, encoding="utf-8")
    report_path = strategy_report(
        tmp_path,
        "FIRST_BEAR_SNAP_TURN_V1",
        conditions={f"predicate_{index}": True for index in range(12)},
        emitted=True,
        direction="SHORT",
    )
    report, runtime_path = runtime_report(
        tmp_path,
        strategies=[strategy_summary("FIRST_BEAR_SNAP_TURN_V1", report_path, verdict="SIGNAL_READY_NO_SUBMIT", emitted=True, direction="SHORT")],
    )

    result = record_track_b_decision_journal_cycle(
        runtime_cycle_report=report,
        runtime_cycle_report_json=runtime_path,
        output_root=root,
        config=journal_config(max_bytes=100, rotated_keep=1),
        now=aware_now(),
    )

    rotated = sorted(root.glob("track_b_decision_journal.*.jsonl.gz"))
    assert len(rotated) == 1
    with gzip.open(rotated[0], "rt", encoding="utf-8") as handle:
        assert "x" in handle.read()
    assert read_jsonl(result.active_journal_jsonl)[0]["journal_tier"] == "TIER_3_SIGNAL_TRADE_DECISION"
