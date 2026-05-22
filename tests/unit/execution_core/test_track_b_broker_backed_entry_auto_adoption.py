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


def aware_now() -> datetime:
    return datetime(2026, 5, 22, 12, 1, tzinfo=timezone.utc)


def _filled_entry(symbol: str = "MGC") -> dict[str, object]:
    is_mgc = symbol == "MGC"
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
        "perm_id": 347072597 if is_mgc else 347072610,
        "client_id": 11086 if is_mgc else 11154,
        "exec_id": "exec-1",
        "local_symbol": "MGCM6" if is_mgc else "MNQM6",
        "con_id": 712565978 if is_mgc else 770561201,
        "contract_key": f"{symbol}-202606",
        "fill_price": "4522.0" if is_mgc else "29510.75",
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
