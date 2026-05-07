from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_paper_trade_ledger import update_track_b_paper_trade_ledger_from_runner_report
from mgc_v05l.execution_core.track_b_submit_transmission_diagnostic import (
    run_track_b_submit_transmission_diagnostic,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 7, 13, 5, tzinfo=timezone.utc)


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def test_submit_transmission_diagnostic_classifies_app_only_position_from_unfilled_entry(tmp_path: Path) -> None:
    latest_intent = write_json(
        tmp_path / "strategy_trade_intents" / "latest_track_b_strategy_trade_intent.json",
        {
            "intent_id": "track_b_strategy_trade_intent_001",
            "intent_classification": "STRATEGY_TRADE_INTENT_CREATED",
            "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "lifecycle_mode": "STRATEGY_MANAGED",
        },
    )
    lifecycle_path = write_json(
        tmp_path / "managed" / "track_b_strategy_managed_paper_lifecycle_report.json",
        {
            "lifecycle_id": "strategy_managed_001",
            "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "strategy_managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
            "entry_intent": {
                "signal_id": "track_b_strategy_trade_intent_001",
                "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
                "contract_key": "MGC-202606",
                "local_symbol": "MGCM6",
                "side": "SHORT",
                "order_action": "SELL",
                "quantity": 1,
                "entry_limit_price": "4756.6",
            },
            "entry_submit_attempt": None,
            "entry_fill": None,
            "submit_attempted": False,
            "broker_state_mutated": False,
            "review_required": True,
            "final_position_status": "REVIEW_REQUIRED",
        },
    )
    runner_report = {
        "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
        "mode": "PAPER",
        "runtime_decision_source": "DATABENTO_LIVE_ARTIFACT",
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "local_symbol": "MGCM6",
        "quantity": 1,
        "strategy_trade_intent_created": True,
        "managed_lifecycle_invoked": True,
        "managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
        "managed_lifecycle_report_path": str(lifecycle_path),
        "submit_attempted": False,
        "broker_state_mutated": False,
        "paper_proof_invoked": False,
    }
    runner_path = write_json(
        tmp_path / "track_b_strategy_paper_runner" / "latest_track_b_strategy_paper_runner_report.json",
        runner_report,
    )
    ledger = update_track_b_paper_trade_ledger_from_runner_report(
        runner_report=runner_report,
        runner_report_json=runner_path,
        output_root=tmp_path / "paper_trade_ledger",
        now=aware_now(),
    )

    report = run_track_b_submit_transmission_diagnostic(
        runner_report_json=runner_path,
        latest_intent_json=latest_intent,
        ledger_jsonl=ledger.ledger_jsonl,
        output_root=tmp_path / "paper_trade_ledger",
        diagnostic_json=tmp_path / "diagnostics" / "latest_track_b_submit_transmission_diagnostic.json",
        diagnostic_md=tmp_path / "diagnostics" / "latest_track_b_submit_transmission_diagnostic.md",
        now=aware_now(),
    )

    assert report["classification"] == "APP_ONLY_POSITION_FROM_UNFILLED_ENTRY"
    assert report["strategy_trade_intent_created"] is True
    assert report["managed_lifecycle_invoked"] is True
    assert report["entry_submit_attempt_recorded"] is False
    assert report["ibkr_adapter_invoked"] is False
    assert report["place_order_called"] is False
    assert report["fill_callback_received"] is False
    assert report["submit_attempted"] is False
    assert report["broker_state_mutated"] is False
    assert report["broker_mutation_attempted_by_diagnostic"] is False
    assert report["paper_proof_cli_invoked_by_diagnostic"] is False
    assert report["post_summary"]["open_position_count"] == 0
    assert report["post_summary"]["managed_strategy_trade_count"] == 0

    position_status = json.loads((tmp_path / "paper_trade_ledger" / "latest_track_b_live_position_status.json").read_text())
    assert position_status["open_position_count"] == 0
    assert position_status["positions_by_instrument"] == {}


def test_submit_transmission_diagnostic_confirms_broker_backed_fill(tmp_path: Path) -> None:
    latest_intent = write_json(
        tmp_path / "strategy_trade_intents" / "latest_track_b_strategy_trade_intent.json",
        {
            "intent_id": "track_b_strategy_trade_intent_002",
            "intent_classification": "STRATEGY_TRADE_INTENT_CREATED",
            "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
        },
    )
    lifecycle_path = write_json(
        tmp_path / "managed" / "track_b_strategy_managed_paper_lifecycle_report.json",
        {
            "lifecycle_id": "strategy_managed_002",
            "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "strategy_managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
            "entry_intent": {
                "signal_id": "track_b_strategy_trade_intent_002",
                "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
                "contract_key": "MGC-202606",
                "local_symbol": "MGCM6",
                "side": "SHORT",
                "order_action": "SELL",
                "quantity": 1,
                "entry_limit_price": "4756.6",
            },
            "entry_submit_attempt": {
                "broker_order_id": "401",
                "submitted_at": "2026-05-07T13:00:01+00:00",
                "submit_diagnostics": {"order_transmit_flag": True, "place_order_called": True},
            },
            "entry_fill": {
                "broker_order_id": "401",
                "filled_at": "2026-05-07T13:00:02+00:00",
                "price": "4756.5",
                "quantity": 1,
            },
            "submit_attempted": True,
            "broker_state_mutated": True,
            "review_required": False,
            "final_position_status": "OPEN_MANAGED",
        },
    )
    runner_report = {
        "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
        "contract_key": "MGC-202606",
        "local_symbol": "MGCM6",
        "quantity": 1,
        "strategy_trade_intent_created": True,
        "managed_lifecycle_invoked": True,
        "managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
        "managed_lifecycle_report_path": str(lifecycle_path),
        "submit_attempted": True,
        "broker_state_mutated": True,
        "paper_proof_invoked": False,
    }
    runner_path = write_json(
        tmp_path / "track_b_strategy_paper_runner" / "latest_track_b_strategy_paper_runner_report.json",
        runner_report,
    )
    ledger = update_track_b_paper_trade_ledger_from_runner_report(
        runner_report=runner_report,
        runner_report_json=runner_path,
        output_root=tmp_path / "paper_trade_ledger",
        now=aware_now(),
    )

    report = run_track_b_submit_transmission_diagnostic(
        runner_report_json=runner_path,
        latest_intent_json=latest_intent,
        ledger_jsonl=ledger.ledger_jsonl,
        output_root=tmp_path / "paper_trade_ledger",
        diagnostic_json=tmp_path / "diagnostics" / "latest_track_b_submit_transmission_diagnostic.json",
        diagnostic_md=tmp_path / "diagnostics" / "latest_track_b_submit_transmission_diagnostic.md",
        now=aware_now(),
    )

    assert report["classification"] == "BROKER_BACKED_POSITION_CONFIRMED"
    assert report["entry_submit_attempt_recorded"] is True
    assert report["order_id_assigned"] is True
    assert report["fill_callback_received"] is True
    assert report["post_summary"]["open_position_count"] == 1
