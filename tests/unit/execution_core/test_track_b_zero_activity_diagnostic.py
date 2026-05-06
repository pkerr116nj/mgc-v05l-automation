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
    latest_completed_5m_timestamp: str = "2026-05-06T06:30:00+00:00",
    paper_evaluation_allowed: bool = True,
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
                "paper_evaluation_allowed": paper_evaluation_allowed,
                "latest_completed_5m_timestamp": latest_completed_5m_timestamp,
                "multi_strategy_runtime_cycle_report_path": None if runtime_path is None else str(runtime_path),
                "strategy_verdicts": [strategy("TEST_STRATEGY_V1", "TEST_NO_SIGNAL_NO_MUTATION")] if evaluated else [],
                "candidate_signals": [],
                "suppressed_signals": [],
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


def completed_5m_artifact(tmp_path: Path, instrument_family: str, timestamps: list[str]) -> Path:
    return write_json(
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "databento_live_runtime_feed"
        / f"latest_live_{instrument_family.lower()}_completed_5m_candles.json",
        {
            "schema_version": f"track_b_databento_live_{instrument_family.lower()}_completed_5m_candles_v1",
            "bars_available": len(timestamps),
            "candles": [
                {
                    "candle_timestamp": timestamp,
                    "open": "100",
                    "high": "101",
                    "low": "99",
                    "close": "100.5",
                    "volume": "10",
                }
                for timestamp in timestamps
            ],
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        },
    )


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


def test_diagnostic_classifies_no_new_completed_bar_heartbeat_separately(tmp_path: Path) -> None:
    compact_summaries(tmp_path)
    runtime = runtime_report(tmp_path, "runtime-1", strategies=[strategy("TEST_STRATEGY_V1", "TEST_NO_SIGNAL_NO_MUTATION")])
    monitor_report(tmp_path, 1, runtime_path=runtime)
    for cycle in range(2, 8):
        monitor_report(
            tmp_path,
            cycle,
            verdict="TRACK_B_SHADOW_MONITOR_HEARTBEAT_NO_NEW_COMPLETED_BAR",
            evaluated=0,
            primary_blocker=None,
        )

    result = build_track_b_zero_activity_diagnostic(repo_root=tmp_path, output_root=tmp_path / "diag", now=now())

    assert result.report["diagnosis_classification"] == "NO_NEW_COMPLETED_5M_BAR_HEARTBEAT"
    assert result.report["cycle_summary"]["recent_evaluation_cycle_count"] == 1
    assert result.report["cycle_summary"]["recent_heartbeat_only_cycle_count"] == 6


def test_completed_decision_bar_audit_allows_normal_no_signal_when_each_bar_evaluated(tmp_path: Path) -> None:
    compact_summaries(tmp_path)
    completed_5m_artifact(tmp_path, "MGC", ["2026-05-06T06:30:00+00:00"])
    runtime = runtime_report(tmp_path, "runtime-1", strategies=[strategy("TEST_STRATEGY_V1", "TEST_NO_SIGNAL_NO_MUTATION")])
    monitor_report(tmp_path, 1, runtime_path=runtime, latest_completed_5m_timestamp="2026-05-06T06:30:00+00:00")
    for cycle in range(2, 5):
        monitor_report(
            tmp_path,
            cycle,
            verdict="TRACK_B_SHADOW_MONITOR_HEARTBEAT_NO_NEW_COMPLETED_BAR",
            evaluated=0,
            latest_completed_5m_timestamp="2026-05-06T06:30:00+00:00",
        )

    result = build_track_b_zero_activity_diagnostic(repo_root=tmp_path, output_root=tmp_path / "diag", now=now())

    audit = result.report["completed_decision_bar_audit"]
    assert audit["classification"] == "EVALUATING_EACH_COMPLETED_BAR"
    assert audit["instruments"]["MGC"]["decision_bars_actually_evaluated"] == 1
    assert result.report["diagnosis_classification"] == "NORMAL_NO_SIGNAL"
    assert (tmp_path / "diag" / "latest_track_b_completed_decision_bar_evaluation_audit.json").exists()


def test_completed_decision_bar_audit_detects_lagging_live_bars(tmp_path: Path) -> None:
    compact_summaries(tmp_path)
    completed_5m_artifact(
        tmp_path,
        "MGC",
        ["2026-05-06T06:30:00+00:00", "2026-05-06T06:35:00+00:00"],
    )
    runtime = runtime_report(tmp_path, "runtime-1", strategies=[strategy("TEST_STRATEGY_V1", "TEST_NO_SIGNAL_NO_MUTATION")])
    monitor_report(tmp_path, 1, runtime_path=runtime, latest_completed_5m_timestamp="2026-05-06T06:30:00+00:00")
    monitor_report(
        tmp_path,
        2,
        verdict="TRACK_B_SHADOW_MONITOR_HEARTBEAT_NO_NEW_COMPLETED_BAR",
        evaluated=0,
        latest_completed_5m_timestamp="2026-05-06T06:30:00+00:00",
    )

    result = build_track_b_zero_activity_diagnostic(
        repo_root=tmp_path,
        output_root=tmp_path / "diag",
        now=datetime(2026, 5, 6, 6, 40, tzinfo=timezone.utc),
    )

    audit = result.report["completed_decision_bar_audit"]
    assert audit["classification"] == "EVALUATION_LAGGING_LIVE_BARS"
    assert audit["instruments"]["MGC"]["decision_bars_skipped"] == 1
    assert result.report["diagnosis_classification"] == "EVALUATION_LAGGING_LIVE_BARS"


def test_diagnostic_classifies_intermittent_live_execution_failures(tmp_path: Path) -> None:
    compact_summaries(tmp_path)
    runtime = runtime_report(tmp_path, "runtime-1", strategies=[strategy("TEST_STRATEGY_V1", "TEST_NO_SIGNAL_NO_MUTATION")])
    monitor_report(tmp_path, 1, runtime_path=runtime)
    for cycle in range(2, 6):
        monitor_report(
            tmp_path,
            cycle,
            verdict="TRACK_B_SHADOW_MONITOR_NOT_READY_STALE_RUNTIME_CONTEXT",
            evaluated=0,
            primary_blocker="Track B runtime candle context is stale: latest 1m candle age exceeds max.",
        )
    monitor_report(
        tmp_path,
        6,
        verdict="TRACK_B_SHADOW_MONITOR_HEARTBEAT_NO_NEW_COMPLETED_BAR",
        evaluated=0,
        primary_blocker=None,
    )

    result = build_track_b_zero_activity_diagnostic(repo_root=tmp_path, output_root=tmp_path / "diag", now=now())

    assert result.report["diagnosis_classification"] == "LIVE_EXECUTION_INTERMITTENT"
    assert result.report["cycle_summary"]["recent_stale_cycle_count"] == 4
    assert result.report["cycle_summary"]["recent_evaluation_cycle_count"] == 1


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
