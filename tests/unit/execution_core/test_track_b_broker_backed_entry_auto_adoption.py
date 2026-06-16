from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_broker_backed_entry_auto_adoption import (
    AUTO_ADOPTION_APPLIED,
    AUTO_ADOPTION_BLOCKED_NO_BROKER_EFFECT,
    AUTO_ADOPTION_INCOMPLETE,
    auto_adopt_broker_backed_entry,
)
from mgc_v05l.execution_core.track_b_lifecycle_state_transition import (
    BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE,
)
from mgc_v05l.execution_core.track_b_position_management_manifest import (
    create_or_update_position_management_manifest,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 22, 12, 1, tzinfo=timezone.utc)


def _filled_entry(symbol: str = "MGC") -> dict[str, object]:
    contracts = {
        "MGC": {"local_symbol": "MGCM6", "con_id": 712565978, "fill_price": "4522.0"},
        "MNQ": {"local_symbol": "MNQM6", "con_id": 770561201, "fill_price": "29510.75"},
        "ES": {"local_symbol": "ESU6", "con_id": 779841727, "fill_price": "5961.25"},
    }
    contract = contracts.get(symbol, contracts["MNQ"])
    return {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "strategy_id": f"track_b_paper_execution_test_mule_v1__{symbol.lower()}",
        "lane_id": f"track_b_paper_execution_test_mule_v1__{symbol.lower()}",
        "instrument": symbol,
        "symbol": symbol,
        "action": "BUY",
        "quantity": 1,
        "order_intent_id": f"{symbol}|1m|2026-05-22T12:01:00Z|BUY_TO_OPEN",
        "intent_type": "BUY_TO_OPEN",
        "decision_bar_timestamp": "2026-05-22T12:01:00+00:00",
        "broker_order_id": "1",
        "account_id": "DUM882026",
        "perm_id": 347072597,
        "client_id": 11086,
        "exec_id": "exec-1",
        "local_symbol": contract["local_symbol"],
        "con_id": contract["con_id"],
        "contract_key": f"{symbol}-202606",
        "fill_price": contract["fill_price"],
        "fill_timestamp": "2026-05-22T12:02:29.507461+00:00",
        "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        "paper_proof_invoked": False,
        "live_money_readiness": False,
        "review_required": False,
    }


def test_mgc_mule_broker_fill_auto_adopts_open_managed(tmp_path: Path) -> None:
    result = auto_adopt_broker_backed_entry(
        entry_fill_evidence=_filled_entry("MGC"),
        paper_trade_ledger_output_root=tmp_path / "ledger",
        position_management_manifest_root=tmp_path / "manifests",
        managed_lifecycle_output_root=tmp_path / "managed",
        now=aware_now(),
    )

    assert result.classification == AUTO_ADOPTION_APPLIED
    assert result.lifecycle_report_path is not None
    assert result.lifecycle_report_path.exists()
    lifecycle = json.loads(result.lifecycle_report_path.read_text(encoding="utf-8"))
    assert lifecycle["paper_lifecycle_classification"] == "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED"
    live = result.ledger_result.live_position_status
    live_position = next(iter(live["positions_by_instrument"].values()))
    assert live_position["managed_exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"


def test_mnq_mule_broker_fill_auto_adopts_open_managed(tmp_path: Path) -> None:
    result = auto_adopt_broker_backed_entry(
        entry_fill_evidence=_filled_entry("MNQ"),
        paper_trade_ledger_output_root=tmp_path / "ledger",
        position_management_manifest_root=tmp_path / "manifests",
        managed_lifecycle_output_root=tmp_path / "managed",
        now=aware_now(),
    )

    assert result.classification == AUTO_ADOPTION_APPLIED
    assert result.manifest_path is not None
    assert result.manifest_path.exists()
    assert result.ledger_result.live_position_status["open_position_count"] == 1


def test_no_broker_effect_stays_terminal_without_lifecycle(tmp_path: Path) -> None:
    payload = {
        **_filled_entry("MGC"),
        "submit_sent": False,
        "broker_effect_classification": "PRE_SUBMIT_BLOCKED_NO_BROKER_EFFECT",
        "broker_order_id": None,
        "perm_id": None,
        "fill_price": None,
        "fill_timestamp": None,
    }

    result = auto_adopt_broker_backed_entry(
        entry_fill_evidence=payload,
        paper_trade_ledger_output_root=tmp_path / "ledger",
        position_management_manifest_root=tmp_path / "manifests",
        managed_lifecycle_output_root=tmp_path / "managed",
        now=aware_now(),
    )

    assert result.classification == AUTO_ADOPTION_BLOCKED_NO_BROKER_EFFECT
    assert not (tmp_path / "managed").exists()
    assert not (tmp_path / "ledger" / "latest_track_b_live_position_status.json").exists()


def test_incomplete_broker_evidence_does_not_open_managed(tmp_path: Path) -> None:
    payload = {**_filled_entry("MNQ"), "fill_timestamp": None}

    result = auto_adopt_broker_backed_entry(
        entry_fill_evidence=payload,
        paper_trade_ledger_output_root=tmp_path / "ledger",
        position_management_manifest_root=tmp_path / "manifests",
        managed_lifecycle_output_root=tmp_path / "managed",
        now=aware_now(),
    )

    assert result.classification == AUTO_ADOPTION_INCOMPLETE
    assert result.transition_classification == BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE
    assert result.lifecycle_report_path is None


def test_stale_manifest_status_cannot_veto_broker_effect_observed_entry(tmp_path: Path) -> None:
    manifests = tmp_path / "manifests"
    payload = {
        **_filled_entry("ES"),
        "action": "SELL",
        "intent_type": "SELL_TO_OPEN",
        "order_intent_id": "ES|1m|2026-06-16T10:30:00Z|SELL_TO_OPEN",
        "broker_order_id": None,
        "perm_id": None,
        "exec_id": None,
        "broker_effect_classification": "BROKER_EFFECT_OBSERVED_AFTER_REJECTION",
        "broker_effect_observation_id": "broker-effect:ESU6:DUM882026:-1",
    }
    create_or_update_position_management_manifest(
        entry_intent_id=str(payload["order_intent_id"]),
        lane_id=str(payload["lane_id"]),
        strategy_id=str(payload["strategy_id"]),
        instrument_family="ES",
        contract_key="ES-202609",
        local_symbol="ESU6",
        con_id=779841727,
        side="SHORT",
        quantity=1,
        managed_exit_policy_id=None,
        lifecycle_status=BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE,
        output_root=manifests,
        now=aware_now(),
    )

    result = auto_adopt_broker_backed_entry(
        entry_fill_evidence=payload,
        paper_trade_ledger_output_root=tmp_path / "ledger",
        position_management_manifest_root=manifests,
        managed_lifecycle_output_root=tmp_path / "managed",
        now=aware_now(),
    )

    assert result.classification == AUTO_ADOPTION_APPLIED
    assert result.transition_classification == "OPEN_MANAGED"
    assert result.ledger_result.trade_record["final_position_status"] == "OPEN_MANAGED"
    assert result.manifest_path is not None
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["lifecycle_status"] == "OPEN_MANAGED"
