from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_position_truth_notifier import (
    TrackBPositionTruthNotificationConfig,
    build_notification_payload,
    process_position_truth_notifications,
)

NOW = datetime(2026, 5, 22, 10, 20, tzinfo=UTC)


def test_notifier_reads_execution_core_event_log_and_writes_jsonl(tmp_path: Path) -> None:
    config = TrackBPositionTruthNotificationConfig(
        repo_root=tmp_path,
        enable_macos_notifications=False,
    )
    _append_event(
        config.resolve(config.event_log_path),
        {
            "generated_at": NOW.isoformat(),
            "event_type": "BROKER_POSITION_REQUIRES_ADOPTION",
            "symbol": "MGC",
            "classification": "BROKER_POSITION_REQUIRES_ADOPTION",
            "detail": "Broker exposure needs adoption.",
            "state": {"broker_quantity": "1"},
        },
    )

    notifications = process_position_truth_notifications(config=config, now=NOW)

    assert len(notifications) == 1
    assert config.resolve(config.event_log_path) == (
        tmp_path / "outputs" / "track_b_execution_core" / "position_truth" / "track_b_trade_outcome_events.jsonl"
    )
    notification_lines = config.resolve(config.notification_log_path).read_text(encoding="utf-8").splitlines()
    assert len(notification_lines) == 1
    notification = json.loads(notification_lines[0])
    assert notification["source_event_type"] == "BROKER_POSITION_REQUIRES_ADOPTION"
    assert notification["delivery_method"] == "jsonl_only"
    assert notification["read_only"] is True
    assert notification["broker_mutation"] is False
    assert notification["live_money_eligible"] is False


def test_notifier_deduplicates_by_signature_with_rate_limit(tmp_path: Path) -> None:
    config = TrackBPositionTruthNotificationConfig(
        repo_root=tmp_path,
        enable_macos_notifications=False,
        rate_limit_seconds=300.0,
    )
    event = {
        "generated_at": NOW.isoformat(),
        "event_type": "SUSPICIOUS_ORDER_DETECTED",
        "symbol": "MNQ",
        "classification": "CLOSE_ORDER_SUSPICIOUS",
        "detail": "Suspicious close order.",
        "state": {"open_order_ids": ["27"], "suspicious_order_reasons": ["27:sentinel_filled_quantity"]},
    }
    _append_event(config.resolve(config.event_log_path), event)
    assert len(process_position_truth_notifications(config=config, now=NOW)) == 1

    # Simulate the same event being unread again after an operator resets offset.
    state = json.loads(config.resolve(config.state_path).read_text(encoding="utf-8"))
    state["event_log_offset"] = 0
    config.resolve(config.state_path).write_text(json.dumps(state), encoding="utf-8")

    assert process_position_truth_notifications(config=config, now=NOW + timedelta(seconds=60)) == []


def test_notifier_allows_repeat_after_rate_limit(tmp_path: Path) -> None:
    config = TrackBPositionTruthNotificationConfig(
        repo_root=tmp_path,
        enable_macos_notifications=False,
        rate_limit_seconds=30.0,
    )
    event = {
        "generated_at": NOW.isoformat(),
        "event_type": "RECONCILIATION_BLOCKED",
        "symbol": None,
        "classification": "BROKER_TRUTH_SETTLEMENT_TIMEOUT",
        "detail": "Reconciliation blocked.",
    }
    _append_event(config.resolve(config.event_log_path), event)
    assert len(process_position_truth_notifications(config=config, now=NOW)) == 1

    state = json.loads(config.resolve(config.state_path).read_text(encoding="utf-8"))
    state["event_log_offset"] = 0
    config.resolve(config.state_path).write_text(json.dumps(state), encoding="utf-8")

    assert len(process_position_truth_notifications(config=config, now=NOW + timedelta(seconds=31))) == 1


def test_notifier_ignores_low_signal_position_change(tmp_path: Path) -> None:
    config = TrackBPositionTruthNotificationConfig(repo_root=tmp_path, enable_macos_notifications=False)
    _append_event(
        config.resolve(config.event_log_path),
        {
            "generated_at": NOW.isoformat(),
            "event_type": "POSITION_STATE_CHANGED",
            "symbol": "MGC",
            "classification": "FLAT_CLEAN",
            "detail": "No exposure.",
        },
    )

    assert process_position_truth_notifications(config=config, now=NOW) == []
    assert not config.resolve(config.notification_log_path).exists()


def test_build_notification_payload_for_closed_flat() -> None:
    payload = build_notification_payload(
        event={
            "generated_at": NOW.isoformat(),
            "event_type": "LIFECYCLE_CLOSED_FLAT",
            "symbol": "MNQ",
            "classification": "FLAT_CLEAN",
            "detail": "Lifecycle closed flat.",
        },
        now=NOW,
    )

    assert payload is not None
    assert payload["title"] == "Track B PAPER MNQ: LIFECYCLE_CLOSED_FLAT"
    assert payload["submit_authority"] is False
    assert payload["paper_proof_invoked"] is False


def test_custom_notifier_receives_payload_without_broker_authority(tmp_path: Path) -> None:
    config = TrackBPositionTruthNotificationConfig(repo_root=tmp_path)
    _append_event(
        config.resolve(config.event_log_path),
        {
            "generated_at": NOW.isoformat(),
            "event_type": "RUNTIME_STOPPED_WITH_BROKER_EXPOSURE",
            "symbol": None,
            "classification": "RUNTIME_STOPPED_WITH_BROKER_EXPOSURE",
            "detail": "Runtime stopped with exposure.",
        },
    )
    delivered: list[dict[str, object]] = []

    notifications = process_position_truth_notifications(
        config=config,
        now=NOW,
        notifier=lambda payload: delivered.append(dict(payload)) is None or True,
    )

    assert len(notifications) == 1
    assert delivered[0]["broker_mutation"] is False
    assert delivered[0]["lifecycle_mutation"] is False


def _append_event(path: Path, event: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")
