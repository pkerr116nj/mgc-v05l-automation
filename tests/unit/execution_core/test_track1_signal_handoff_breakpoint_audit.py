from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track1_signal_handoff_breakpoint_audit import (
    build_track1_signal_handoff_breakpoint_audit,
)


NOW = datetime(2026, 5, 7, 12, 0, tzinfo=UTC)


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def seed_preflight(tmp_path: Path) -> Path:
    return write_json(
        tmp_path / "outputs" / "track_b_execution_core" / "diagnostics" / "latest_track1_trading_stop_preflight.json",
        {
            "breakpoint": {
                "last_known_track1_trade": {
                    "timestamp": "2026-04-29T08:35:00-04:00",
                    "path": "outputs/operator_dashboard/paper_strategy_trade_log_snapshot.json",
                    "strategy_ids": ["firstBullSnapTurn"],
                },
                "last_known_strategy_signal": {
                    "timestamp": "2026-05-01T04:31:29.459796+00:00",
                    "path": "outputs/operator_dashboard/paper_session_close_reviews/latest.json",
                },
            }
        },
    )


def build(tmp_path: Path):
    return build_track1_signal_handoff_breakpoint_audit(
        repo_root=tmp_path,
        output_root=tmp_path / "outputs" / "track_b_execution_core" / "diagnostics",
        preflight_path=seed_preflight(tmp_path).relative_to(tmp_path),
        now=NOW,
    )


def seed_trade_log(tmp_path: Path) -> None:
    write_json(
        tmp_path / "outputs" / "operator_dashboard" / "paper_strategy_trade_log_snapshot.json",
        {
            "rows": [
                {
                    "trade_id": "trade-1",
                    "lane_id": "firstBullSnapTurn",
                    "strategy_key": "firstBullSnapTurn",
                    "instrument": "MNQ",
                    "side": "LONG",
                    "entry_timestamp": "2026-04-29T08:30:00-04:00",
                    "exit_timestamp": "2026-04-29T08:35:00-04:00",
                    "entry_price": "100",
                    "exit_price": "101",
                    "status": "CLOSED",
                }
            ]
        },
    )


def seed_signal_review(tmp_path: Path, *, intent_count: int = 0, fill_count: int = 0, halt_reason: str | None = None) -> None:
    write_json(
        tmp_path / "outputs" / "operator_dashboard" / "paper_session_close_reviews" / "latest.json",
        {
            "rows": [
                {
                    "lane_id": "firstBullSnapTurn",
                    "instrument": "MNQ",
                    "session_verdict": "SIGNAL_NO_FILL",
                    "signal_count": 1,
                    "intent_count": intent_count,
                    "fill_count": fill_count,
                    "evidence_chain_status": "BROKEN",
                    "latest_event_timestamp": "2026-04-30T12:30:00+00:00",
                    "risk_state": "OK",
                    "latest_halt_reason": halt_reason,
                }
            ]
        },
    )


def test_signals_present_but_no_handoff_artifact_classifies_intent_not_created(tmp_path: Path) -> None:
    seed_trade_log(tmp_path)
    seed_signal_review(tmp_path)
    write_json(tmp_path / "outputs" / "operator_dashboard" / "paper_latest_intents_snapshot.json", {"rows": []})

    result = build(tmp_path)

    assert result.report["classification"] == "HANDOFF_INTENT_NOT_CREATED"
    assert result.report["missing_link"] == "signal_to_intent"
    assert result.report["bounded_policy"]["broker_commands_invoked"] is False


def test_handoff_disabled_config_classifies_config(tmp_path: Path) -> None:
    seed_trade_log(tmp_path)
    seed_signal_review(tmp_path)
    write_json(tmp_path / "config" / "paper.json", {"paper_on_signal": True, "enabled": False})

    result = build(tmp_path)

    assert result.report["classification"] == "HANDOFF_DISABLED_BY_CONFIG"


def test_runner_invoked_but_no_lifecycle_classifies_lifecycle_routing(tmp_path: Path) -> None:
    seed_trade_log(tmp_path)
    seed_signal_review(tmp_path, intent_count=1)
    write_json(tmp_path / "outputs" / "operator_dashboard" / "paper_latest_intents_snapshot.json", {"rows": [{"intent_id": "i1"}]})
    write_json(tmp_path / "outputs" / "operator_dashboard" / "paper_latest_fills_snapshot.json", {"rows": []})

    result = build(tmp_path)

    assert result.report["classification"] == "LIFECYCLE_ROUTING_BROKEN"


def test_broker_readiness_block_classifies_broker_block(tmp_path: Path) -> None:
    seed_trade_log(tmp_path)
    seed_signal_review(tmp_path, halt_reason="broker_not_ready")

    result = build(tmp_path)

    assert result.report["classification"] == "BROKER_READINESS_BLOCKED"


def test_session_review_only_signals_are_labeled_as_review_risk(tmp_path: Path) -> None:
    seed_trade_log(tmp_path)
    seed_signal_review(tmp_path)

    result = build(tmp_path)

    assert result.report["post_trade_signal_summary"]["session_review_only_risk"] is True
    assert result.report["classification"] == "HANDOFF_INTENT_NOT_CREATED"


def test_no_broker_mutation_paths_are_invoked(tmp_path: Path) -> None:
    result = build(tmp_path)

    policy = result.report["bounded_policy"]
    assert policy["broker_commands_invoked"] is False
    assert policy["paper_proof_cli_invoked"] is False
    assert policy["submit_cancel_place_order_invoked"] is False
    assert policy["broker_state_mutated"] is False
