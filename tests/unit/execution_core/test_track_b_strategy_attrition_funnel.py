from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core import track_b_strategy_attrition_funnel as funnel


NOW = datetime(2026, 6, 10, 12, 30, tzinfo=UTC)


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_event_writer_emits_valid_jsonl(tmp_path: Path) -> None:
    event_path = Path("outputs/track_b_execution_core/strategy_attrition_funnel/strategy_funnel_events.jsonl")

    result = funnel.record_strategy_funnel_events(
        [
            funnel.build_strategy_funnel_event(
                timestamp=NOW,
                lane_id="mnq_lane",
                strategy_family="regular_family",
                profile="paper_profile",
                stage="candidate_generated",
                pass_fail="PASS",
                candidate_id="candidate-1",
                source_component="unit_test",
            )
        ],
        repo_root=tmp_path,
        event_path=event_path,
    )

    rows = _read_jsonl(result.event_path)
    assert result.events_written == 1
    assert rows[0]["schema_version"] == "track_b_strategy_attrition_funnel_event_v1"
    assert rows[0]["timestamp"] == NOW.isoformat()
    assert rows[0]["lane_id"] == "mnq_lane"
    assert rows[0]["stage"] == "candidate_generated"
    assert rows[0]["pass_fail"] == "PASS"


def test_writer_failure_is_diagnostic_only(monkeypatch, tmp_path: Path) -> None:
    def boom(*args, **kwargs):  # noqa: ANN002, ANN003
        raise OSError("disk unavailable")

    monkeypatch.setattr(funnel, "record_strategy_funnel_events", boom)

    result = funnel.try_record_strategy_funnel_events(
        [
            funnel.build_strategy_funnel_event(
                timestamp=NOW,
                stage="governance_fail",
                pass_fail="FAIL",
                lane_id="mnq_lane",
                source_component="unit_test",
            )
        ],
        repo_root=tmp_path,
    )

    assert result is None


def test_no_trade_diagnostic_derives_candidate_and_governance_events() -> None:
    events = funnel.events_from_no_trade_diagnostic(
        {
            "generated_at": NOW.isoformat(),
            "bar_timestamp": NOW.isoformat(),
            "lane_id": "mes_lane",
            "symbol": "MES",
            "session": "LONDON_OPEN",
            "session_allowed": True,
            "setup_detected": True,
            "final_decision": "GOVERNANCE_BLOCKED",
            "blocker_reason": "control_plane_not_ready",
            "order_intent_id": "intent-1",
            "runtime_identity": {"strategy_family": "regular_family"},
        }
    )

    by_stage = {event["stage"]: event for event in events}
    assert by_stage["session_eligible"]["pass_fail"] == "PASS"
    assert by_stage["candidate_generated"]["pass_fail"] == "PASS"
    assert by_stage["rule_pass"]["pass_fail"] == "PASS"
    assert by_stage["governance_fail"]["blocker_classification"] == "GOVERNANCE_BLOCKED"


def test_blocked_submit_and_fill_and_adoption_events_are_derived() -> None:
    blocked = funnel.events_from_blocked_strategy_intent(
        {
            "created_at": NOW.isoformat(),
            "lane_id": "mnq_lane",
            "strategy_family": "paper_active_evidence",
            "instrument": "MNQ",
            "order_intent_id": "intent-1",
            "bar_id": "MNQ|1m|2026-06-10T12:30:00Z",
            "submit_attempted": True,
            "blocker_classification": "PRE_SUBMIT_GATE_BLOCKED",
            "exact_blocker_reason": "control_plane_not_ready",
        }
    )
    filled = funnel.events_from_filled_bridge_result(
        {
            "created_at": NOW.isoformat(),
            "lane_id": "mnq_lane",
            "strategy_family": "paper_active_evidence",
            "instrument": "MNQ",
            "order_intent_id": "intent-1",
            "broker_order_id": "94",
            "broker_backed_entry_auto_adoption": "BROKER_BACKED_ENTRY_AUTO_ADOPTED",
            "lifecycle_id": "life-1",
            "trade_id": "trade-1",
        }
    )

    assert {event["stage"] for event in blocked} >= {
        "candidate_generated",
        "rule_pass",
        "governance_fail",
        "submit_attempted",
    }
    assert {event["stage"] for event in filled} == {"broker_accepted", "fill_observed", "managed_adopted"}


def test_managed_registry_and_service_emit_exit_events() -> None:
    registry_events = funnel.events_from_managed_position_registry(
        {
            "generated_at": NOW.isoformat(),
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "exit_due": True,
                    "lane_id": "mnq_lane",
                    "instrument": "MNQ",
                    "lifecycle_id": "life-1",
                    "trade_id": "trade-1",
                }
            ],
        }
    )
    service_events = funnel.events_from_managed_exit_service_status(
        {
            "generated_at": NOW.isoformat(),
            "classification": "APPLY_SUCCEEDED",
            "submit_attempted": True,
            "submitted_count": 1,
            "broker_state_mutated": True,
            "execution_plan": {
                "executable_intents": [
                    {
                        "exit_intent_id": "exit-1",
                        "lane_id": "mnq_lane",
                        "instrument": "MNQ",
                        "lifecycle_id": "life-1",
                        "trade_id": "trade-1",
                    }
                ]
            },
        }
    )

    assert {event["stage"] for event in registry_events} == {"managed_adopted", "exit_due"}
    assert {event["stage"] for event in service_events} == {"exit_submitted", "exit_filled"}


def test_summary_counts_active_regular_and_noisemaker_lanes(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "outputs/probationary_pattern_engine/paper_session/runtime"
    runtime_dir.mkdir(parents=True)
    (runtime_dir / "paper_config_in_force.json").write_text(
        json.dumps(
            {
                "profile": "test_profile",
                "active_lane_ids": ["regular_lane", "noisy_active_participation_short"],
                "lanes": [
                    {"lane_id": "regular_lane", "strategy_family": "regular_family"},
                    {
                        "lane_id": "noisy_active_participation_short",
                        "strategy_family": "paper_active_evidence",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    event_path = tmp_path / "outputs/track_b_execution_core/strategy_attrition_funnel/strategy_funnel_events.jsonl"
    funnel.record_strategy_funnel_events(
        [
            funnel.build_strategy_funnel_event(
                timestamp=NOW,
                lane_id="regular_lane",
                strategy_family="regular_family",
                stage="governance_fail",
                pass_fail="FAIL",
                blocker_classification="control_plane_not_ready",
                source_component="unit_test",
            ),
            funnel.build_strategy_funnel_event(
                timestamp=NOW,
                lane_id="noisy_active_participation_short",
                strategy_family="paper_active_evidence",
                stage="fill_observed",
                pass_fail="PASS",
                source_component="unit_test",
            ),
        ],
        repo_root=tmp_path,
    )

    summary = funnel.summarize_strategy_attrition_funnel(
        repo_root=tmp_path,
        now=NOW,
        include_legacy_artifacts=False,
    )

    assert summary["active_profile_lane_count"] == 2
    assert summary["regular_strategy_active_count"] == 1
    assert summary["noisemaker_active_participation_active_count"] == 1
    assert summary["regular_enabled_and_blocked"] == ["regular_lane"]
    by_lane = {row["lane_id"]: row for row in summary["lanes"]}
    assert by_lane["regular_lane"]["windows"]["24h"]["label"] == "GOVERNANCE_BLOCKED"
    assert by_lane["noisy_active_participation_short"]["windows"]["24h"]["label"] == "TRADING_OK"
