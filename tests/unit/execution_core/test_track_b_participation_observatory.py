from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_participation_observatory import (
    TrackBParticipationObservatoryConfig,
    build_track_b_participation_observatory,
    write_track_b_participation_observatory,
)


NOW = datetime(2026, 6, 1, 15, 0, tzinfo=UTC)


def test_out_of_window_bar_classified_correctly(tmp_path: Path) -> None:
    bar_ts = "2026-06-01T22:00:00+00:00"
    lane = _lane(bar_ts=bar_ts, allowed_session_match=False, eligibility_reason="wrong_session")
    _write_fixture(tmp_path, [lane], {"MNQ": [bar_ts]})

    report = _build(tmp_path)
    row = report["bar_evaluations"][0]

    assert row["stage_results"]["SESSION_ELIGIBLE"] is False
    assert row["first_fail_reason"] == "wrong_session"
    assert report["session_coverage"]["out_of_window_lane_count"] == 1


def test_data_not_ready_classified_correctly(tmp_path: Path) -> None:
    bar_ts = "2026-06-01T14:00:00+00:00"
    lane = _lane(
        bar_ts=bar_ts,
        market_data_not_ready=True,
        market_data_blocker_reason="paper_lane_phase1_runtime_artifact_not_ready",
    )
    _write_fixture(tmp_path, [lane], {"MNQ": [bar_ts]})

    row = _build(tmp_path)["bar_evaluations"][0]

    assert row["stage_results"]["DATA_READY"] is False
    assert row["first_fail_reason"] == "paper_lane_phase1_runtime_artifact_not_ready"


def test_predicates_fail_with_first_fail_reason(tmp_path: Path) -> None:
    bar_ts = "2026-06-01T14:00:00+00:00"
    lane = _lane(
        bar_ts=bar_ts,
        rule_report={"classification": "TRACK_B_PAPER_ACTIVE_EVIDENCE_NO_SIGNAL", "primary_blocker": "close_below_vwap"},
    )
    _write_fixture(tmp_path, [lane], {"MNQ": [bar_ts]})

    row = _build(tmp_path)["bar_evaluations"][0]

    assert row["stage_results"]["PREDICATES_EVALUATED"] is True
    assert row["stage_results"]["CANDIDATE_CREATED"] is False
    assert row["first_fail_reason"] == "close_below_vwap"


def test_session_anchor_not_ready_is_surfaced_as_predicate_blocker(tmp_path: Path) -> None:
    bar_ts = "2026-06-01T14:00:00+00:00"
    lane = _lane(
        bar_ts=bar_ts,
        rule_report={
            "classification": "TRACK_B_PAPER_ACTIVE_EVIDENCE_NO_SIGNAL",
            "primary_blocker": "SESSION_ANCHOR_NOT_READY",
            "session_anchor_status": "NOT_READY",
            "session_anchor_reason_code": "ANCHOR_BAR_NOT_FOUND",
            "session_anchor_source": None,
            "session_anchor_source_artifact_path": None,
        },
    )
    _write_fixture(tmp_path, [lane], {"MNQ": [bar_ts]})

    report = _build(tmp_path)
    row = report["bar_evaluations"][0]

    assert row["stage_results"]["PREDICATES_EVALUATED"] is True
    assert row["first_fail_reason"] == "SESSION_ANCHOR_NOT_READY"
    assert row["session_anchor_status"] == "NOT_READY"
    assert row["session_anchor_reason_code"] == "ANCHOR_BAR_NOT_FOUND"
    assert row["predicate_results"]["session_anchor_status"] == "NOT_READY"
    assert report["lane_reports"][0]["latest_session_anchor_status"] == "NOT_READY"


def test_session_anchor_ready_does_not_report_legacy_missing_open_blocker(tmp_path: Path) -> None:
    bar_ts = "2026-06-01T14:00:00+00:00"
    lane = _lane(
        bar_ts=bar_ts,
        rule_report={
            "classification": "TRACK_B_PAPER_ACTIVE_EVIDENCE_SIGNAL",
            "primary_blocker": None,
            "session_open_price": "21000",
            "session_anchor_status": "READY",
            "session_anchor_reason_code": "ANCHOR_RECOVERED_FROM_PHASE1_GAP_BACKFILL",
            "session_anchor_source": "RECOVERED_PHASE1_1M",
            "session_anchor_source_artifact_path": "/tmp/session-anchor.json",
        },
    )
    _write_fixture(tmp_path, [lane], {"MNQ": [bar_ts]})

    row = _build(tmp_path)["bar_evaluations"][0]

    assert row["candidate_created"] is True
    assert row["first_fail_reason"] != "us_session_open_bar_missing"
    assert row["session_anchor_status"] == "READY"
    assert row["predicate_results"]["session_anchor_reason_code"] == "ANCHOR_RECOVERED_FROM_PHASE1_GAP_BACKFILL"


def test_predicates_pass_but_candidate_missing_is_flagged(tmp_path: Path) -> None:
    bar_ts = "2026-06-01T14:00:00+00:00"
    lane = _lane(
        bar_ts=bar_ts,
        rule_report={
            "classification": "TRACK_B_PAPER_ACTIVE_EVIDENCE_NO_SIGNAL",
            "primary_blocker": "candidate_not_emitted",
            "predicate_results": {"above_vwap": True, "above_session_open": True},
        },
    )
    _write_fixture(tmp_path, [lane], {"MNQ": [bar_ts]})

    report = _build(tmp_path)

    assert _hidden_codes(report) == ["PREDICATES_PASS_NO_CANDIDATE"]
    assert report["classification"] == "PARTICIPATION_OBSERVATORY_REVIEW_REQUIRED"


def test_candidate_to_intent_blocker_recorded(tmp_path: Path) -> None:
    bar_ts = "2026-06-01T14:00:00+00:00"
    lane = _lane(bar_ts=bar_ts, rule_report={"classification": "TRACK_B_PAPER_ACTIVE_EVIDENCE_SIGNAL"})
    _write_fixture(tmp_path, [lane], {"MNQ": [bar_ts]})

    report = _build(tmp_path)
    row = report["bar_evaluations"][0]

    assert row["candidate_created"] is True
    assert row["intent_created"] is False
    assert "CANDIDATE_NO_INTENT" in _hidden_codes(report)


def test_intent_to_submit_blocker_recorded(tmp_path: Path) -> None:
    bar_ts = "2026-06-01T14:00:00+00:00"
    lane = _lane(
        bar_ts=bar_ts,
        rule_report={"classification": "TRACK_B_PAPER_ACTIVE_EVIDENCE_SIGNAL"},
        latest_live_strategy_intent={
            "trade_id": "trade-intent-blocked",
            "created_at": bar_ts,
            "submit_suppressed": True,
            "submit_gate_blocker": "BRIDGE_EXPOSURE_GATE_BLOCKED",
        },
    )
    _write_fixture(tmp_path, [lane], {"MNQ": [bar_ts]})

    report = _build(tmp_path)
    row = report["bar_evaluations"][0]

    assert row["intent_created"] is True
    assert row["submit_status"] == "SUBMIT_SUPPRESSED"
    assert row["gate_blocker"] == "BRIDGE_EXPOSURE_GATE_BLOCKED"
    assert "INTENT_NO_SUBMIT" in _hidden_codes(report)


def test_submit_fill_and_lifecycle_adoption_path_recorded(tmp_path: Path) -> None:
    bar_ts = "2026-06-01T14:00:00+00:00"
    lane = _lane(
        bar_ts=bar_ts,
        rule_report={"classification": "TRACK_B_PAPER_ACTIVE_EVIDENCE_SIGNAL"},
        latest_live_strategy_intent={
            "trade_id": "trade-filled",
            "created_at": bar_ts,
            "submitted_at": bar_ts,
            "submit_attempted": True,
            "broker_order_id": "17",
            "perm_id": "1955790757",
            "exec_id": "0000e1a7.6a2bc09f.01.01",
            "filled_at": bar_ts,
            "lifecycle_adopted": True,
        },
    )
    _write_fixture(tmp_path, [lane], {"MNQ": [bar_ts]})

    row = _build(tmp_path)["bar_evaluations"][0]

    assert row["stage_results"]["SUBMIT_ATTEMPTED"] is True
    assert row["stage_results"]["BROKER_ACK"] is True
    assert row["stage_results"]["FILL"] is True
    assert row["stage_results"]["LIFECYCLE_ADOPTED"] is True
    assert row["first_fail_reason"] == "FUNNEL_COMPLETE"


def test_simulated_paper_orders_are_not_counted_as_broker_submits_or_fills(tmp_path: Path) -> None:
    bar_ts = "2026-06-01T07:20:00+00:00"
    lane = _lane(
        bar_ts=bar_ts,
        lane_id="mnq_london_open_active_participation_long",
        rule_report={
            "classification": "TRACK_B_PAPER_ACTIVE_EVIDENCE_SIGNAL",
            "broker_authoritative_envelope_classification": "BROKER_AUTHORITATIVE_ENVELOPE_READY_DRY_RUN",
            "broker_authoritative_envelope_path": "outputs/track_b_execution_core/london_open_active_evidence/latest_mnq_london_open_active_participation_long_event_envelope.json",
        },
        latest_live_strategy_intent={
            "trade_id": "paper-only-trade",
            "created_at": bar_ts,
            "submitted_at": bar_ts,
            "submit_attempted": True,
            "broker_order_id": "paper-MNQ|1m|2026-06-01T07:20:00+00:00|BUY_TO_OPEN",
            "filled_at": bar_ts,
        },
    )
    lane["fill_count"] = 90
    _write_fixture(tmp_path, [lane], {"MNQ": [bar_ts]})

    report = _build(tmp_path)
    row = report["bar_evaluations"][0]
    lane_report = report["lane_reports"][0]

    assert row["stage_results"]["SUBMIT_ATTEMPTED"] is False
    assert row["stage_results"]["BROKER_ACK"] is False
    assert row["stage_results"]["FILL"] is False
    assert row["simulated_paper_submit"] is True
    assert row["simulated_paper_fill"] is True
    assert row["broker_authoritative_envelope_produced"] is True
    assert lane_report["submit_attempt_count"] == 0
    assert lane_report["fill_count"] == 0
    assert lane_report["raw_lane_fill_count"] == 90
    assert lane_report["simulated_paper_fill_count"] == 1
    assert report["conversion"]["fill_count"] == 0
    assert report["conversion"]["simulated_paper_fill_count"] == 1
    assert report["conversion"]["broker_authoritative_envelope_count"] == 1


def test_stale_broker_event_envelope_is_not_counted_current(tmp_path: Path) -> None:
    bar_ts = "2026-06-01T07:20:00+00:00"
    lane = _lane(
        bar_ts=bar_ts,
        lane_id="mnq_london_open_active_participation_long",
        rule_report={
            "classification": "TRACK_B_PAPER_ACTIVE_EVIDENCE_SIGNAL",
            "broker_event_envelope_classification": "BROKER_EVENT_ENVELOPE_READY_DRY_RUN",
            "broker_event_envelope_path": "outputs/track_b_execution_core/london_open_active_evidence/latest_mnq_london_open_active_participation_long_event_envelope.json",
            "broker_event_envelope_source_candle_timestamp": "2026-05-28T07:20:00+00:00",
        },
        latest_live_strategy_intent={"trade_id": "trade-stale-envelope", "created_at": bar_ts},
    )
    _write_fixture(tmp_path, [lane], {"MNQ": [bar_ts]})

    report = _build(tmp_path)

    assert report["bar_evaluations"][0]["broker_authoritative_envelope_produced"] is False
    assert report["lane_reports"][0]["broker_envelope_count"] == 0
    assert report["conversion"]["broker_authoritative_envelope_count"] == 0


def test_lane_silent_during_active_window_flagged(tmp_path: Path) -> None:
    bar_ts = "2026-06-01T14:00:00+00:00"
    lane = _lane(
        bar_ts=bar_ts,
        rule_report={"classification": "TRACK_B_PAPER_ACTIVE_EVIDENCE_NO_SIGNAL", "primary_blocker": "close_below_reference"},
    )
    _write_fixture(tmp_path, [lane], {"MNQ": [bar_ts]})

    report = _build(tmp_path)

    assert report["lane_reports"][0]["noise_silence_classification"] == "LANE_SILENT_DURING_ACTIVE_WINDOW"
    assert report["noise_silence"]["silent_active_lanes"] == ["mnq_us_active_participation_long"]


def test_report_aggregation_and_bounded_event_stream(tmp_path: Path) -> None:
    bar_ts = "2026-06-01T14:00:00+00:00"
    lane = _lane(bar_ts=bar_ts, rule_report={"classification": "TRACK_B_PAPER_ACTIVE_EVIDENCE_SIGNAL"})
    _write_fixture(tmp_path, [lane], {"MNQ": [bar_ts]})
    config = TrackBParticipationObservatoryConfig(
        repo_root=tmp_path,
        output_json_path=Path("out/latest.json"),
        output_md_path=Path("out/latest.md"),
        event_jsonl_path=Path("out/events.jsonl"),
        write_event_stream=True,
        max_event_stream_rows=1,
    )
    report = build_track_b_participation_observatory(config=config, now=NOW)

    json_path, md_path = write_track_b_participation_observatory(config=config, report=report)

    assert json.loads(json_path.read_text(encoding="utf-8"))["schema_version"] == "track_b_participation_observatory_v1"
    assert "Track B Participation Observatory" in md_path.read_text(encoding="utf-8")
    assert len((tmp_path / "out/events.jsonl").read_text(encoding="utf-8").splitlines()) == 1
    assert report["conversion"]["candidate_count"] == 1


def _build(tmp_path: Path) -> dict[str, object]:
    return build_track_b_participation_observatory(
        config=TrackBParticipationObservatoryConfig(repo_root=tmp_path, max_bars_per_lane=5),
        now=NOW,
    )


def _lane(
    *,
    bar_ts: str,
    lane_id: str = "mnq_us_active_participation_long",
    symbol: str = "MNQ",
    allowed_session_match: bool = True,
    eligibility_reason: str = "eligible",
    market_data_not_ready: bool = False,
    market_data_blocker_reason: str | None = None,
    rule_report: dict[str, object] | None = None,
    latest_live_strategy_intent: dict[str, object] | None = None,
) -> dict[str, object]:
    report = {
        "classification": "TRACK_B_PAPER_ACTIVE_EVIDENCE_NO_SIGNAL",
        "primary_blocker": "no_signal",
        "current_bar_end_et": bar_ts,
    }
    report.update(rule_report or {})
    return {
        "lane_id": lane_id,
        "symbol": symbol,
        "paper_only": True,
        "source_family": "paper_active_evidence",
        "strategy_identity_root": f"PAPER_ACTIVE_EVIDENCE_{symbol}_{lane_id.upper()}_V1",
        "allowed_session_match": allowed_session_match,
        "eligible_now": allowed_session_match,
        "eligibility_reason": eligibility_reason,
        "current_detected_session": "US_MIDDAY" if allowed_session_match else "OUT_OF_WINDOW",
        "last_processed_bar_end_ts": bar_ts,
        "latest_completed_bar_end_ts": bar_ts,
        "last_execution_bar_evaluated_at": bar_ts,
        "last_completed_context_bars_at": bar_ts,
        "market_data_not_ready": market_data_not_ready,
        "market_data_blocker_reason": market_data_blocker_reason,
        "latest_track_b_rule_runner_classification": report["classification"],
        "latest_track_b_rule_runner_primary_blocker": report.get("primary_blocker"),
        "latest_track_b_rule_runner_report": report,
        "latest_live_strategy_intent": latest_live_strategy_intent or {},
        "intent_count": 0,
        "fill_count": 0,
    }


def _write_fixture(tmp_path: Path, lanes: list[dict[str, object]], bars_by_symbol: dict[str, list[str]]) -> None:
    _write_json(
        tmp_path / "outputs/probationary_pattern_engine/paper_session/operator_status.json",
        {"generated_at": NOW.isoformat(), "lanes": lanes, "active_lane_ids": [lane["lane_id"] for lane in lanes]},
    )
    for symbol, bars in bars_by_symbol.items():
        _write_json(
            tmp_path / f"outputs/track_b_execution_core/phase1_runtime_market_data/{symbol}/1m/latest_runtime_candles.json",
            {"bars": [{"bar_end": bar_ts, "close": "100.0"} for bar_ts in bars]},
        )


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _hidden_codes(report: dict[str, object]) -> list[str]:
    return [str(row["code"]) for row in report["hidden_blockers"]]  # type: ignore[index]
