from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.app.operator_dashboard import OperatorDashboardService
from mgc_v05l.app.research_runtime_bridge import (
    BRIDGE_MODE_PROSPECTIVE,
    BRIDGE_MODE_REPLAY,
    review_runtime_bridge_anomaly,
    run_bridge,
)
from mgc_v05l.research.trend_participation.storage import materialize_parquet_dataset


def _materialize_bridge_fixture(warehouse_root: Path) -> None:
    materialize_parquet_dataset(
        warehouse_root / "datasets" / "lane_entries" / "symbol=GC" / "year=2024" / "shard_id=2024Q1" / "entries.parquet",
        [
            {
                "entry_id": "entry-1",
                "candidate_id": "candidate-1",
                "lane_id": "gc_asia_early_normal_breakout_retest_hold_turn__GC",
                "strategy_key": "gc_asia_early_normal_breakout_retest_hold_turn__GC",
                "family": "asiaEarlyNormalBreakoutRetestHoldTurn",
                "symbol": "GC",
                "shard_id": "2024Q1",
                "side": "LONG",
                "entry_ts": "2024-01-02T10:00:00+00:00",
                "entry_price": 2000.0,
                "provenance_tag": "lane_entries:gc:2024Q1",
            }
        ],
    )
    materialize_parquet_dataset(
        warehouse_root / "datasets" / "lane_entries" / "symbol=MGC" / "year=2024" / "shard_id=2024Q1" / "entries.parquet",
        [
            {
                "entry_id": "entry-2",
                "candidate_id": "candidate-2",
                "lane_id": "mgc_asia_early_pause_resume_short_turn__MGC",
                "strategy_key": "mgc_asia_early_pause_resume_short_turn__MGC",
                "family": "asiaEarlyPauseResumeShortTurn",
                "symbol": "MGC",
                "shard_id": "2024Q1",
                "side": "SHORT",
                "entry_ts": "2024-01-03T11:00:00+00:00",
                "entry_price": 2100.0,
                "provenance_tag": "lane_entries:mgc:2024Q1",
            }
        ],
    )
    materialize_parquet_dataset(
        warehouse_root / "datasets" / "lane_closed_trades" / "symbol=GC" / "year=2024" / "shard_id=2024Q1" / "closed_trades.parquet",
        [
            {
                "trade_id": "gc_asia_early_normal_breakout_retest_hold_turn__GC:trade-1",
                "entry_id": "entry-1",
                "candidate_id": "candidate-1",
                "lane_id": "gc_asia_early_normal_breakout_retest_hold_turn__GC",
                "strategy_key": "gc_asia_early_normal_breakout_retest_hold_turn__GC",
                "family": "asiaEarlyNormalBreakoutRetestHoldTurn",
                "symbol": "GC",
                "shard_id": "2024Q1",
                "side": "LONG",
                "execution_model": "PROBATIONARY_5M_CONTEXT_1M_EXECUTABLE_VWAP",
                "entry_ts": "2024-01-02T10:00:00+00:00",
                "exit_ts": "2024-01-02T10:15:00+00:00",
                "entry_price": 2000.0,
                "exit_price": 2001.5,
                "pnl": 150.0,
                "pnl_points": 1.5,
                "point_value": 100.0,
                "hold_minutes": 15,
                "vwap_quality": "VWAP_FAVORABLE",
                "exit_reason": "LONG_TIME_EXIT",
                "win_flag": True,
                "provenance_tag": "lane_closed_trades:gc:2024Q1",
            }
        ],
    )
    materialize_parquet_dataset(
        warehouse_root / "datasets" / "lane_closed_trades" / "symbol=MGC" / "year=2024" / "shard_id=2024Q1" / "closed_trades.parquet",
        [
            {
                "trade_id": "mgc_asia_early_pause_resume_short_turn__MGC:trade-1",
                "entry_id": "entry-2",
                "candidate_id": "candidate-2",
                "lane_id": "mgc_asia_early_pause_resume_short_turn__MGC",
                "strategy_key": "mgc_asia_early_pause_resume_short_turn__MGC",
                "family": "asiaEarlyPauseResumeShortTurn",
                "symbol": "MGC",
                "shard_id": "2024Q1",
                "side": "SHORT",
                "execution_model": "PROBATIONARY_5M_CONTEXT_1M_EXECUTABLE_VWAP",
                "entry_ts": "2024-01-03T11:00:00+00:00",
                "exit_ts": "2024-01-03T11:20:00+00:00",
                "entry_price": 2100.0,
                "exit_price": 2098.0,
                "pnl": 20.0,
                "pnl_points": 2.0,
                "point_value": 10.0,
                "hold_minutes": 20,
                "vwap_quality": "VWAP_FAVORABLE",
                "exit_reason": "SHORT_TIME_EXIT",
                "win_flag": True,
                "provenance_tag": "lane_closed_trades:mgc:2024Q1",
            }
        ],
    )


def test_research_runtime_bridge_replays_selected_warehouse_trades(tmp_path: Path) -> None:
    warehouse_root = tmp_path / "warehouse"
    output_dir = tmp_path / "bridge"
    _materialize_bridge_fixture(warehouse_root)

    result = run_bridge(
        warehouse_root=warehouse_root,
        output_dir=output_dir,
        selected_lane_ids=[
            "gc_asia_early_normal_breakout_retest_hold_turn__GC",
            "mgc_asia_early_pause_resume_short_turn__MGC",
        ],
        mode=BRIDGE_MODE_REPLAY,
    )

    snapshot = json.loads(Path(result["snapshot_path"]).read_text(encoding="utf-8"))
    assert snapshot["available"] is True
    assert snapshot["paper_only"] is True
    assert snapshot["live_execution_enabled"] is False
    assert snapshot["summary"]["selected_lane_count"] == 2
    assert snapshot["summary"]["intent_count"] == 4
    assert snapshot["summary"]["fill_count"] == 4
    assert snapshot["summary"]["closed_position_count"] == 2
    assert snapshot["summary"]["open_position_count"] == 0
    assert snapshot["summary"]["reconciliation_issue_count"] == 0
    assert snapshot["summary"]["realized_pnl_cash"] == 170.0
    assert len(snapshot["lane_rows"]) == 2
    assert {row["lane_id"] for row in snapshot["lane_rows"]} == {
        "gc_asia_early_normal_breakout_retest_hold_turn__GC",
        "mgc_asia_early_pause_resume_short_turn__MGC",
    }
    assert all(row["runtime_status"] == "READY" for row in snapshot["lane_rows"])
    assert Path(result["db_path"]).exists()

    intents_rows = [json.loads(line) for line in (output_dir / "order_intents.jsonl").read_text(encoding="utf-8").splitlines()]
    fills_rows = [json.loads(line) for line in (output_dir / "fills.jsonl").read_text(encoding="utf-8").splitlines()]
    trades_rows = [json.loads(line) for line in (output_dir / "trades.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(intents_rows) == 4
    assert len(fills_rows) == 4
    assert len(trades_rows) == 2
    assert {row["phase"] for row in intents_rows} == {"entry", "exit"}
    assert {row["side"] for row in trades_rows} == {"LONG", "SHORT"}


def test_research_runtime_bridge_prospective_lifecycle_advances_across_runtime_cycles(tmp_path: Path) -> None:
    warehouse_root = tmp_path / "warehouse"
    output_dir = tmp_path / "bridge"
    _materialize_bridge_fixture(warehouse_root)

    first = run_bridge(warehouse_root=warehouse_root, output_dir=output_dir, mode=BRIDGE_MODE_PROSPECTIVE, reset_state=True)
    first_snapshot = json.loads(Path(first["snapshot_path"]).read_text(encoding="utf-8"))
    assert first_snapshot["summary"]["pending_intent_count"] == 2
    assert first_snapshot["summary"]["fill_count"] == 0
    assert first_snapshot["summary"]["open_position_count"] == 0
    assert first_snapshot["summary"]["bridge_mode"] == BRIDGE_MODE_PROSPECTIVE
    assert len(first_snapshot["pending_intents"]) == 2
    assert first_snapshot["cycle_policy"]["stale_after_cycles"] == 2
    assert first_snapshot["cycle_policy"]["expire_after_cycles"] == 3

    second = run_bridge(warehouse_root=warehouse_root, output_dir=output_dir, mode=BRIDGE_MODE_PROSPECTIVE)
    second_snapshot = json.loads(Path(second["snapshot_path"]).read_text(encoding="utf-8"))
    assert second_snapshot["summary"]["pending_intent_count"] == 0
    assert second_snapshot["summary"]["fill_count"] == 2
    assert second_snapshot["summary"]["open_position_count"] == 2
    assert len(second_snapshot["open_positions"]) == 2

    third = run_bridge(warehouse_root=warehouse_root, output_dir=output_dir, mode=BRIDGE_MODE_PROSPECTIVE)
    third_snapshot = json.loads(Path(third["snapshot_path"]).read_text(encoding="utf-8"))
    assert third_snapshot["summary"]["pending_intent_count"] == 2
    assert third_snapshot["summary"]["fill_count"] == 2
    assert third_snapshot["summary"]["closed_position_count"] == 0
    assert any(str(row.get("phase")) == "exit" for row in third_snapshot["pending_intents"])

    fourth = run_bridge(warehouse_root=warehouse_root, output_dir=output_dir, mode=BRIDGE_MODE_PROSPECTIVE)
    fourth_snapshot = json.loads(Path(fourth["snapshot_path"]).read_text(encoding="utf-8"))
    assert fourth_snapshot["summary"]["pending_intent_count"] == 0
    assert fourth_snapshot["summary"]["fill_count"] == 4
    assert fourth_snapshot["summary"]["closed_position_count"] == 2
    assert fourth_snapshot["summary"]["open_position_count"] == 0
    assert fourth_snapshot["summary"]["reconciliation_issue_count"] == 0
    assert fourth_snapshot["summary"]["runtime_event_count"] == 8
    assert fourth_snapshot["summary"]["runtime_event_severity_counts"]["INFO"] == 8
    assert fourth_snapshot["summary"]["active_alert_count"] == 0
    assert fourth_snapshot["summary"]["unreviewed_anomaly_count"] == 0
    assert fourth_snapshot["anomalies"]["active_count"] == 0
    assert len(fourth_snapshot["recent_closed_positions"]) == 2
    assert fourth_snapshot["cadence"]["cycle_state"] == "SETTLED"
    assert fourth_snapshot["cadence"]["bridge_cycle_index"] == 4


def test_research_runtime_bridge_prospective_stale_pending_intents_raise_operator_alerts(tmp_path: Path) -> None:
    warehouse_root = tmp_path / "warehouse"
    output_dir = tmp_path / "bridge"
    _materialize_bridge_fixture(warehouse_root)

    run_bridge(warehouse_root=warehouse_root, output_dir=output_dir, mode=BRIDGE_MODE_PROSPECTIVE, reset_state=True)
    run_bridge(
        warehouse_root=warehouse_root,
        output_dir=output_dir,
        mode=BRIDGE_MODE_PROSPECTIVE,
        operator_halt=True,
    )
    stale = run_bridge(
        warehouse_root=warehouse_root,
        output_dir=output_dir,
        mode=BRIDGE_MODE_PROSPECTIVE,
        operator_halt=True,
    )
    stale_snapshot = json.loads(Path(stale["snapshot_path"]).read_text(encoding="utf-8"))
    assert stale_snapshot["summary"]["pending_intent_count"] == 2
    assert stale_snapshot["summary"]["active_alert_count"] == 2
    assert stale_snapshot["summary"]["unreviewed_anomaly_count"] == 2
    assert stale_snapshot["alerts"]["count"] == 2
    assert all(str(row.get("classification")) == "STALE_PENDING_INTENT" for row in stale_snapshot["alerts"]["active_rows"])
    assert all(str(row.get("review_state")) == "UNREVIEWED" for row in stale_snapshot["alerts"]["active_rows"])

    expired = run_bridge(
        warehouse_root=warehouse_root,
        output_dir=output_dir,
        mode=BRIDGE_MODE_PROSPECTIVE,
        operator_halt=True,
    )
    expired_snapshot = json.loads(Path(expired["snapshot_path"]).read_text(encoding="utf-8"))
    assert expired_snapshot["summary"]["pending_intent_count"] == 0
    assert expired_snapshot["summary"]["active_alert_count"] == 2
    assert expired_snapshot["summary"]["runtime_event_severity_counts"]["WARN"] >= 2
    assert expired_snapshot["anomalies"]["severity_counts"]["WARN"] == 2
    assert any(str(row.get("event_type")) == "INTENT_EXPIRED" for row in expired_snapshot["runtime_events"]["recent_rows"])
    intent_rows = [json.loads(line) for line in (output_dir / "order_intents.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    assert sum(1 for row in intent_rows if str(row.get("lifecycle_state")) == "EXPIRED") == 2


def test_research_runtime_bridge_cycle_runner_advances_until_settled(tmp_path: Path) -> None:
    warehouse_root = tmp_path / "warehouse"
    output_dir = tmp_path / "bridge"
    _materialize_bridge_fixture(warehouse_root)

    result = run_bridge(
        warehouse_root=warehouse_root,
        output_dir=output_dir,
        mode=BRIDGE_MODE_PROSPECTIVE,
        reset_state=True,
        cycle_count=6,
        poll_interval_seconds=0,
        stop_when_settled=True,
    )

    snapshot = json.loads(Path(result["snapshot_path"]).read_text(encoding="utf-8"))
    cadence_state = json.loads((output_dir / "cadence_state.json").read_text(encoding="utf-8"))
    assert snapshot["summary"]["status"] == "READY"
    assert snapshot["summary"]["closed_position_count"] == 2
    assert cadence_state["cycle_state"] == "SETTLED"
    assert cadence_state["bridge_cycle_index"] == 4
    assert cadence_state["scheduler_mode"] == "LOCAL_INTERVAL_LOOP"


def test_research_runtime_bridge_anomaly_review_updates_snapshot_and_review_log(tmp_path: Path) -> None:
    warehouse_root = tmp_path / "warehouse"
    output_dir = tmp_path / "bridge"
    _materialize_bridge_fixture(warehouse_root)

    run_bridge(warehouse_root=warehouse_root, output_dir=output_dir, mode=BRIDGE_MODE_PROSPECTIVE, reset_state=True)
    run_bridge(warehouse_root=warehouse_root, output_dir=output_dir, mode=BRIDGE_MODE_PROSPECTIVE, operator_halt=True)
    run_bridge(warehouse_root=warehouse_root, output_dir=output_dir, mode=BRIDGE_MODE_PROSPECTIVE, operator_halt=True)
    run_bridge(warehouse_root=warehouse_root, output_dir=output_dir, mode=BRIDGE_MODE_PROSPECTIVE, operator_halt=True)

    review_result = review_runtime_bridge_anomaly(
        output_dir=output_dir,
        anomaly_key="stale-pending:entry-1:prospective_entry_intent",
        operator_label="test operator",
        note="Reviewed in paper mode.",
        review_state="ACKNOWLEDGED",
    )

    snapshot = json.loads(Path(review_result["snapshot_path"]).read_text(encoding="utf-8"))
    reviewed_alert = next(row for row in snapshot["alerts"]["recent_rows"] if row.get("dedup_key") == "stale-pending:entry-1:prospective_entry_intent")
    review_rows = [json.loads(line) for line in (output_dir / "operator_review_events.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    assert reviewed_alert["review_state"] == "ACKNOWLEDGED"
    assert reviewed_alert["acknowledged"] is True
    assert snapshot["anomalies"]["unreviewed_count"] == 1
    assert snapshot["anomalies"]["acknowledged_count"] == 1
    acknowledged_queue_row = next(row for row in snapshot["anomaly_queue"]["recent_rows"] if row.get("dedup_key") == "stale-pending:entry-1:prospective_entry_intent")
    assert acknowledged_queue_row["queue_status"] == "ACKNOWLEDGED_PENDING"
    assert acknowledged_queue_row["needs_attention_now"] is False
    assert review_rows[0]["review_state"] == "ACKNOWLEDGED"
    assert review_rows[0]["operator_label"] == "test operator"

    resolve_result = review_runtime_bridge_anomaly(
        output_dir=output_dir,
        anomaly_key="stale-pending:entry-1:prospective_entry_intent",
        operator_label="test operator",
        note="Resolved after verification.",
        review_state="RESOLVED",
    )
    resolved_snapshot = json.loads(Path(resolve_result["snapshot_path"]).read_text(encoding="utf-8"))
    resolved_alert = next(row for row in resolved_snapshot["alerts"]["recent_rows"] if row.get("dedup_key") == "stale-pending:entry-1:prospective_entry_intent")
    assert resolved_alert["review_state"] == "RESOLVED"
    assert resolved_alert["active"] is False
    assert resolved_snapshot["anomaly_queue"]["resolved_count"] >= 1


def test_research_runtime_bridge_acknowledged_stale_anomaly_escalates_when_still_unresolved(tmp_path: Path) -> None:
    warehouse_root = tmp_path / "warehouse"
    output_dir = tmp_path / "bridge"
    _materialize_bridge_fixture(warehouse_root)

    run_bridge(warehouse_root=warehouse_root, output_dir=output_dir, mode=BRIDGE_MODE_PROSPECTIVE, reset_state=True)
    run_bridge(warehouse_root=warehouse_root, output_dir=output_dir, mode=BRIDGE_MODE_PROSPECTIVE, operator_halt=True)
    run_bridge(warehouse_root=warehouse_root, output_dir=output_dir, mode=BRIDGE_MODE_PROSPECTIVE, operator_halt=True)

    review_runtime_bridge_anomaly(
        output_dir=output_dir,
        anomaly_key="stale-pending:entry-1:prospective_entry_intent",
        operator_label="test operator",
        note="Acknowledged pending intent while monitoring paper mode.",
        review_state="ACKNOWLEDGED",
    )

    escalated = run_bridge(
        warehouse_root=warehouse_root,
        output_dir=output_dir,
        mode=BRIDGE_MODE_PROSPECTIVE,
        operator_halt=True,
    )
    escalated_snapshot = json.loads(Path(escalated["snapshot_path"]).read_text(encoding="utf-8"))
    escalated_alert = next(row for row in escalated_snapshot["alerts"]["recent_rows"] if row.get("dedup_key") == "stale-pending:entry-1:prospective_entry_intent")

    assert escalated_alert["classification"] == "UNRESOLVED_PENDING_INTENT_ESCALATED"
    assert escalated_alert["severity"] == "ERROR"
    assert escalated_alert["review_state"] == "ACKNOWLEDGED"
    assert escalated_alert["escalated"] is True
    assert escalated_snapshot["anomalies"]["escalated_count"] >= 1
    assert escalated_snapshot["anomaly_queue"]["escalated_count"] >= 1


def test_research_runtime_bridge_reviewed_pending_anomaly_stays_visible_without_counting_as_attention_now(tmp_path: Path) -> None:
    warehouse_root = tmp_path / "warehouse"
    output_dir = tmp_path / "bridge"
    _materialize_bridge_fixture(warehouse_root)

    run_bridge(warehouse_root=warehouse_root, output_dir=output_dir, mode=BRIDGE_MODE_PROSPECTIVE, reset_state=True)
    run_bridge(warehouse_root=warehouse_root, output_dir=output_dir, mode=BRIDGE_MODE_PROSPECTIVE, operator_halt=True)
    run_bridge(warehouse_root=warehouse_root, output_dir=output_dir, mode=BRIDGE_MODE_PROSPECTIVE, operator_halt=True)

    reviewed_result = review_runtime_bridge_anomaly(
        output_dir=output_dir,
        anomaly_key="stale-pending:entry-1:prospective_entry_intent",
        operator_label="test operator",
        note="Reviewed while monitoring unresolved state.",
        review_state="REVIEWED",
    )
    reviewed_snapshot = json.loads(Path(reviewed_result["snapshot_path"]).read_text(encoding="utf-8"))
    reviewed_alert = next(row for row in reviewed_snapshot["alerts"]["recent_rows"] if row.get("dedup_key") == "stale-pending:entry-1:prospective_entry_intent")
    reviewed_queue_row = next(row for row in reviewed_snapshot["anomaly_queue"]["recent_rows"] if row.get("dedup_key") == "stale-pending:entry-1:prospective_entry_intent")

    assert reviewed_alert["review_state"] == "REVIEWED"
    assert reviewed_queue_row["queue_status"] == "REVIEWED_PENDING"
    assert reviewed_queue_row["needs_attention_now"] is False
    assert reviewed_snapshot["anomalies"]["reviewed_pending_count"] >= 1


def test_operator_dashboard_research_runtime_bridge_supervisor_supports_manual_cycle_and_start_stop(tmp_path: Path) -> None:
    warehouse_root = tmp_path / "warehouse"
    bridge_root = tmp_path / "outputs" / "research_runtime_bridge" / "default_warehouse_paper"
    _materialize_bridge_fixture(warehouse_root)
    run_bridge(warehouse_root=warehouse_root, output_dir=bridge_root, mode=BRIDGE_MODE_PROSPECTIVE, reset_state=True)

    service = OperatorDashboardService(tmp_path)

    run_now = service._run_research_runtime_bridge_supervisor_cycle_now({})  # noqa: SLF001
    assert run_now["ok"] is True
    supervisor_state = json.loads((bridge_root / "supervisor_state.json").read_text(encoding="utf-8"))
    assert supervisor_state["last_successful_cycle_index"] >= 2
    assert supervisor_state["status"] in {"RUNNING", "SETTLED", "ATTENTION_REQUIRED", "BLOCKED"}

    start_result = service._start_research_runtime_bridge_supervisor({"poll_interval_seconds": 1})  # noqa: SLF001
    assert start_result["ok"] is True
    time.sleep(1.2)
    stop_result = service._stop_research_runtime_bridge_supervisor({})  # noqa: SLF001
    assert stop_result["ok"] is True
    stopped_state = json.loads((bridge_root / "supervisor_state.json").read_text(encoding="utf-8"))
    assert stopped_state["desired_state"] == "STOPPED"
    assert stopped_state["status"] == "STOPPED"
    event_rows = [json.loads(line) for line in (bridge_root / "supervisor_events.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    assert any(row["event_type"] == "SUPERVISOR_STARTED" for row in event_rows)
    assert any(row["event_type"] == "SUPERVISOR_STOPPED" for row in event_rows)


def test_operator_dashboard_service_host_autostarts_research_runtime_bridge_supervisor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    warehouse_root = tmp_path / "warehouse"
    bridge_root = tmp_path / "outputs" / "research_runtime_bridge" / "default_warehouse_paper"
    _materialize_bridge_fixture(warehouse_root)
    run_bridge(warehouse_root=warehouse_root, output_dir=bridge_root, mode=BRIDGE_MODE_PROSPECTIVE, reset_state=True)
    monkeypatch.setenv("MGC_SERVICE_HOST_AUTOSTART_RESEARCH_RUNTIME_BRIDGE_SUPERVISOR", "1")

    service = OperatorDashboardService(tmp_path)
    service._autostart_research_runtime_bridge_supervisor_if_enabled()  # noqa: SLF001
    time.sleep(0.4)

    supervisor_state = json.loads((bridge_root / "supervisor_state.json").read_text(encoding="utf-8"))
    assert supervisor_state["desired_state"] == "RUNNING"
    assert supervisor_state["status"] in {"STARTING", "RUNNING", "WAITING", "SETTLED", "ATTENTION_REQUIRED", "BLOCKED"}
    event_rows = [
        json.loads(line)
        for line in (bridge_root / "supervisor_events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert any(
        row["event_type"] == "SUPERVISOR_STARTED" and row.get("operator_label") == "service-first host"
        for row in event_rows
    )

    stop_result = service._stop_research_runtime_bridge_supervisor({})  # noqa: SLF001
    assert stop_result["ok"] is True


def test_operator_dashboard_surfaces_research_runtime_bridge_snapshot(tmp_path: Path) -> None:
    bridge_root = tmp_path / "outputs" / "research_runtime_bridge" / "default_warehouse_paper"
    bridge_root.mkdir(parents=True, exist_ok=True)
    snapshot_path = bridge_root / "bridge_snapshot.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "available": True,
                "generated_at": datetime.now(UTC).isoformat(),
                "bridge_mode": "PAPER_DRY_RUN_ONLY",
                "paper_only": True,
                "live_execution_enabled": False,
                "summary": {
                    "status": "READY",
                    "summary_line": "Bridge ready.",
                },
                "operator_status": {
                    "status": "PAPER_READY",
                    "label": "Paper Runtime Bridge",
                    "operator_action_required": False,
                },
                "artifacts": {
                    "root_dir": str(bridge_root),
                },
                "lane_rows": [],
                "recent_intents": [],
                "recent_fills": [],
                "recent_closed_positions": [],
                "open_positions": [],
                "pending_intents": [],
                "alerts": {"recent_rows": [], "count": 0},
                "reconciliation": {"recent_rows": [], "anomaly_rows": [], "count": 0},
                "runtime_events": {"recent_rows": [], "count": 0, "severity_counts": {"INFO": 0, "WARN": 0, "ERROR": 0}},
                "operator_reviews": {"recent_rows": [], "count": 0},
                "anomalies": {"active_count": 0, "unreviewed_count": 0, "acknowledged_count": 0, "unresolved_count": 0, "needs_attention_now_count": 0, "resolved_count": 0, "acknowledged_pending_count": 0, "reviewed_pending_count": 0, "severity_counts": {"WARN": 0, "ERROR": 0}, "counts_by_classification": {}, "review_state_counts": {"UNREVIEWED": 0, "ACKNOWLEDGED": 0, "REVIEWED": 0, "RESOLVED": 0, "INFO_ONLY": 0}, "reconciliation_issue_count": 0},
                "anomaly_queue": {"recent_rows": [], "count": 0, "needs_attention_now_count": 0, "unresolved_count": 0, "review_pending_count": 0, "acknowledged_count": 0, "acknowledged_pending_count": 0, "reviewed_count": 0, "reviewed_pending_count": 0, "resolved_count": 0, "escalated_count": 0, "counts_by_queue_status": {"NEEDS_ATTENTION_NOW": 0, "ACKNOWLEDGED_PENDING": 0, "REVIEWED_PENDING": 0, "ESCALATED": 0, "RESOLVED": 0}},
                "cycle_policy": {"stale_after_cycles": 2, "expire_after_cycles": 3, "min_fill_delay_cycles": 1},
                "cadence": {"cycle_state": "SETTLED", "poll_interval_seconds": 30},
            }
        ),
        encoding="utf-8",
    )

    service = OperatorDashboardService(tmp_path)
    payload = service._research_runtime_bridge_payload(generated_at=datetime.now(UTC).isoformat())  # noqa: SLF001

    assert payload["available"] is True
    assert payload["paper_only"] is True
    assert payload["artifacts"]["snapshot"] == "/api/operator-artifact/research-runtime-bridge"
    assert payload["artifacts"]["intents"] == "/api/operator-artifact/research-runtime-bridge-intents"
    assert payload["artifacts"]["runtime_events"] == "/api/operator-artifact/research-runtime-bridge-runtime-events"
    assert payload["artifacts"]["cadence_state"] == "/api/operator-artifact/research-runtime-bridge-cadence-state"
    assert payload["artifacts"]["operator_reviews"] == "/api/operator-artifact/research-runtime-bridge-operator-reviews"
    assert payload["artifacts"]["supervisor_state"] == "/api/operator-artifact/research-runtime-bridge-supervisor-state"
    assert payload["artifacts"]["supervisor_events"] == "/api/operator-artifact/research-runtime-bridge-supervisor-events"
