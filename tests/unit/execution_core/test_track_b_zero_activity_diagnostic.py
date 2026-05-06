from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_zero_activity_diagnostic import build_track_b_zero_activity_diagnostic


def now() -> datetime:
    return datetime(2026, 5, 6, 6, 30, tzinfo=timezone.utc)


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def runtime_report(
    tmp_path: Path,
    cycle_id: str,
    *,
    strategies: list[dict[str, object]],
    candidates: list[dict[str, object]] | None = None,
    suppressed: list[dict[str, object]] | None = None,
    handoff: bool = False,
) -> Path:
    return write_json(
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "track_b_multi_strategy_runtime_cycle"
        / cycle_id
        / "track_b_multi_strategy_runtime_cycle_report.json",
        {
            "generated_at": now().isoformat(),
            "track_b_multi_strategy_runtime_cycle_id": cycle_id,
            "multi_strategy_runtime_cycle_verdict": "TRACK_B_MULTI_STRATEGY_RUNTIME_NO_SIGNAL_NO_MUTATION",
            "evaluated_strategies": strategies,
            "candidate_signals": candidates or [],
            "suppressed_signals": suppressed or [],
            "arbitration_result": {"primary_blocker": "conflicting signals"} if suppressed else {},
            "paper_runner_report_path": "paper-runner.json" if handoff else None,
            "paper_proof_invoked": handoff,
            "submit_attempted": handoff,
            "broker_state_mutated": handoff,
            "live_money_readiness": False,
        },
    )


def monitor_report(
    tmp_path: Path,
    cycle: int,
    *,
    verdict: str = "TRACK_B_SHADOW_MONITOR_OK_NO_SIGNAL",
    runtime_path: Path | None = None,
    evaluated: int = 1,
    primary_blocker: str | None = None,
) -> Path:
    path = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "track_b_shadow_monitor"
        / f"track_b_shadow_monitor_unit_cycle_{cycle}"
        / "track_b_shadow_monitor_report.json"
    )
    payload = {
        "completed_at": now().isoformat(),
        "cycle_index": cycle,
        "cycle_id": f"cycle-{cycle}",
        "mode": "PAPER",
        "runtime_decision_source": "DATABENTO_LIVE_ARTIFACT",
        "monitor_verdict": verdict,
        "primary_blocker": primary_blocker,
        "evaluated_strategy_count": evaluated,
        "candidate_signals": [],
        "suppressed_signals": [],
        "submit_allowed": False,
        "submit_attempted": False,
        "paper_proof_invoked": False,
        "broker_state_mutated": False,
        "live_money_readiness": False,
        "instrument_reports": [
            {
                "instrument_family": "MGC",
                "enabled_strategies": ["TEST_STRATEGY_V1"],
                "live_feed_connected": True,
                "live_feed_status": "LIVE_FEED_STRATEGY_READY",
                "live_feed_strategy_ready": True,
                "fresh_for_execution": "STALE" not in verdict,
                "multi_strategy_runtime_cycle_report_path": None if runtime_path is None else str(runtime_path),
            }
        ],
    }
    write_json(path, payload)
    write_json(path.parent.parent / "latest_track_b_shadow_monitor_report.json", payload)
    write_json(
        path.parent.parent / "latest_track_b_shadow_monitor_heartbeat.json",
        {
            "generated_at": now().isoformat(),
            "monitor_running": True,
            "last_monitor_verdict": verdict,
        },
    )
    return path


def compact_summaries(tmp_path: Path, *, trade_count: int = 0) -> None:
    root = tmp_path / "outputs" / "track_b_execution_core" / "paper_trade_ledger"
    write_json(
        root / "latest_track_b_paper_trade_summary.json",
        {
            "as_of": now().isoformat(),
            "paper_trades_attempted_count": trade_count,
            "completed_trade_count": trade_count,
            "open_position_count": 0,
        },
    )
    write_json(root / "latest_track_b_live_position_status.json", {"as_of": now().isoformat(), "open_position_count": 0})
    write_json(root / "latest_track_b_pnl_summary.json", {"as_of": now().isoformat(), "total_realized_pnl_today": "0"})
    (root / "track_b_paper_trade_ledger.jsonl").write_text("{not valid and must not be read}\n", encoding="utf-8")


def strategy(strategy_id: str, verdict: str, *, signal: bool = False, blocker: str | None = None) -> dict[str, object]:
    return {
        "strategy_id": strategy_id,
        "strategy_runtime_verdict": verdict,
        "decision": "SIGNAL" if signal else ("NOT_READY" if "NOT_READY" in verdict else "NO_SIGNAL"),
        "signal_emitted": signal,
        "primary_blocker": blocker,
        "rule_blockers": ["predicate_a=false_or_missing"] if "NO_SIGNAL" in verdict else [],
    }


def test_diagnostic_classifies_normal_repeated_no_signal(tmp_path: Path) -> None:
    compact_summaries(tmp_path)
    runtime = runtime_report(tmp_path, "runtime-1", strategies=[strategy("TEST_STRATEGY_V1", "TEST_NO_SIGNAL_NO_MUTATION")])
    monitor_report(tmp_path, 1, runtime_path=runtime)

    result = build_track_b_zero_activity_diagnostic(repo_root=tmp_path, output_root=tmp_path / "diag", now=now())

    assert result.report["diagnosis_classification"] == "NORMAL_NO_SIGNAL"
    assert result.report["cycle_summary"]["recent_evaluation_cycle_count"] == 1
    assert result.report["per_strategy_recent_result_counts"]["TEST_STRATEGY_V1"]["no_signal"] == 1


def test_diagnostic_classifies_no_recent_evaluation_cycles_as_stuck(tmp_path: Path) -> None:
    compact_summaries(tmp_path)
    monitor_report(
        tmp_path,
        1,
        verdict="TRACK_B_SHADOW_MONITOR_HEARTBEAT_NO_NEW_COMPLETED_BAR",
        evaluated=0,
        primary_blocker="no new completed bar",
    )

    result = build_track_b_zero_activity_diagnostic(repo_root=tmp_path, output_root=tmp_path / "diag", now=now())

    assert result.report["diagnosis_classification"] == "STUCK_BEFORE_EVALUATION"


def test_diagnostic_classifies_repeated_not_ready_as_inputs_not_ready(tmp_path: Path) -> None:
    compact_summaries(tmp_path)
    runtime = runtime_report(
        tmp_path,
        "runtime-not-ready",
        strategies=[strategy("TEST_STRATEGY_V1", "TEST_NOT_READY", blocker="missing explicit envelope")],
    )
    monitor_report(tmp_path, 1, runtime_path=runtime)

    result = build_track_b_zero_activity_diagnostic(repo_root=tmp_path, output_root=tmp_path / "diag", now=now())

    assert result.report["diagnosis_classification"] == "INPUTS_NOT_READY"
    assert result.report["top_not_ready_reasons"][0]["reason"] == "missing explicit envelope"


def test_diagnostic_classifies_stale_live_feed(tmp_path: Path) -> None:
    compact_summaries(tmp_path)
    monitor_report(
        tmp_path,
        1,
        verdict="TRACK_B_SHADOW_MONITOR_NOT_READY_STALE_RUNTIME_CONTEXT",
        evaluated=0,
        primary_blocker="Track B runtime candle context is stale",
    )

    result = build_track_b_zero_activity_diagnostic(repo_root=tmp_path, output_root=tmp_path / "diag", now=now())

    assert result.report["diagnosis_classification"] == "STALE_LIVE_FEED"


def test_diagnostic_classifies_signal_suppressed_by_arbitration(tmp_path: Path) -> None:
    compact_summaries(tmp_path)
    runtime = runtime_report(
        tmp_path,
        "runtime-suppressed",
        strategies=[strategy("TEST_STRATEGY_V1", "TEST_SIGNAL_READY_NO_SUBMIT", signal=True)],
        candidates=[{"strategy_id": "TEST_STRATEGY_V1"}],
        suppressed=[{"strategy_id": "TEST_STRATEGY_V1", "reason": "conflicting LONG/SHORT signals"}],
    )
    monitor_report(tmp_path, 1, runtime_path=runtime)

    result = build_track_b_zero_activity_diagnostic(repo_root=tmp_path, output_root=tmp_path / "diag", now=now())

    assert result.report["diagnosis_classification"] == "SIGNAL_SUPPRESSED"


def test_diagnostic_classifies_signal_blocked_before_paper_handoff(tmp_path: Path) -> None:
    compact_summaries(tmp_path)
    runtime = runtime_report(
        tmp_path,
        "runtime-signal",
        strategies=[strategy("TEST_STRATEGY_V1", "TEST_SIGNAL_READY_NO_SUBMIT", signal=True)],
        candidates=[{"strategy_id": "TEST_STRATEGY_V1"}],
    )
    monitor_report(tmp_path, 1, runtime_path=runtime)

    result = build_track_b_zero_activity_diagnostic(repo_root=tmp_path, output_root=tmp_path / "diag", now=now())

    assert result.report["diagnosis_classification"] == "PAPER_HANDOFF_BLOCKED"


def test_diagnostic_detects_stale_ledger_after_handoff(tmp_path: Path) -> None:
    compact_summaries(tmp_path, trade_count=0)
    runtime = runtime_report(
        tmp_path,
        "runtime-handoff",
        strategies=[strategy("TEST_STRATEGY_V1", "TEST_SIGNAL_READY_NO_SUBMIT", signal=True)],
        candidates=[{"strategy_id": "TEST_STRATEGY_V1"}],
        handoff=True,
    )
    monitor_report(tmp_path, 1, runtime_path=runtime)

    result = build_track_b_zero_activity_diagnostic(repo_root=tmp_path, output_root=tmp_path / "diag", now=now())

    assert result.report["diagnosis_classification"] == "LEDGER_WRITE_BLOCKED"
    assert result.report["ledger_summary"]["full_paper_trade_ledger_scanned"] is False

