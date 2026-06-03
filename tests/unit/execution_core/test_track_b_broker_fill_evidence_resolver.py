from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution_core.track_b_broker_fill_evidence_resolver import (
    AMBIGUOUS_EXECUTION,
    MISSING_EXEC_ID,
    RESOLVED,
    BrokerFillEvidenceRequest,
    resolve_broker_backed_fill_evidence,
)


def test_exec_id_missing_from_submit_intent_but_found_in_bridge_report_adopts(tmp_path: Path) -> None:
    _write_submit_ownership(tmp_path, exec_id=None)
    _write_bridge_report(tmp_path, exec_id="exec-1")

    result = resolve_broker_backed_fill_evidence(repo_root=tmp_path, request=_request())

    assert result.classification == RESOLVED
    assert result.broker_backed_evidence_valid is True
    assert result.evidence is not None
    assert result.evidence["exec_id"] == "exec-1"
    assert result.evidence["perm_id"] == "1955790757"
    assert result.evidence["con_id"] == "770561201"


def test_exec_id_missing_everywhere_is_review_required(tmp_path: Path) -> None:
    _write_submit_ownership(tmp_path, exec_id=None)
    _write_bridge_report(tmp_path, exec_id=None)

    result = resolve_broker_backed_fill_evidence(repo_root=tmp_path, request=_request())

    assert result.classification == MISSING_EXEC_ID
    assert result.broker_backed_evidence_valid is False
    assert "EXEC_ID_NOT_FOUND" in result.reason_codes


def test_multiple_matching_exec_ids_are_review_required(tmp_path: Path) -> None:
    _write_submit_ownership(tmp_path, exec_id=None)
    _write_bridge_report(tmp_path, exec_id="exec-1", extra_exec_ids=["exec-2"])

    result = resolve_broker_backed_fill_evidence(repo_root=tmp_path, request=_request())

    assert result.classification == AMBIGUOUS_EXECUTION
    assert result.broker_backed_evidence_valid is False
    assert "MULTIPLE_MATCHING_EXEC_IDS" in result.reason_codes


def test_perm_id_mismatch_blocks(tmp_path: Path) -> None:
    _write_bridge_report(tmp_path, exec_id="exec-1", perm_id=999)

    result = resolve_broker_backed_fill_evidence(repo_root=tmp_path, request=_request())

    assert result.classification == MISSING_EXEC_ID
    assert any(row["reason"] == "perm_id_mismatch" for row in result.rejected)


def test_contract_mismatch_blocks(tmp_path: Path) -> None:
    _write_bridge_report(tmp_path, exec_id="exec-1", local_symbol="MESM6", con_id=770561194)

    result = resolve_broker_backed_fill_evidence(repo_root=tmp_path, request=_request())

    assert result.classification == MISSING_EXEC_ID
    assert any(row["reason"] in {"con_id_mismatch", "local_symbol_mismatch"} for row in result.rejected)


def test_account_mismatch_blocks(tmp_path: Path) -> None:
    _write_bridge_report(tmp_path, exec_id="exec-1", account_id="OTHER")

    result = resolve_broker_backed_fill_evidence(repo_root=tmp_path, request=_request())

    assert result.classification == MISSING_EXEC_ID
    assert any(row["reason"] == "account_mismatch" for row in result.rejected)


def test_synthetic_bridge_fill_trade_id_can_prove_submit_owner_when_broker_identity_exact(tmp_path: Path) -> None:
    _write_bridge_fill_lifecycle_report(
        tmp_path,
        source_trade_id="trade_bridge_fill_MNQ_1m_2026-06-03T02_47_00Z_SELL_TO_OPEN",
        lifecycle_id="bridge_fill_MNQ|1m|2026-06-03T02:47:00Z|SELL_TO_OPEN",
        exec_id="0000e1a7.6a2ecf0d.01.01",
    )

    result = resolve_broker_backed_fill_evidence(repo_root=tmp_path, request=_request())

    assert result.classification == RESOLVED
    assert result.broker_backed_evidence_valid is True
    assert result.evidence is not None
    assert result.evidence["trade_id"] == "trade_mnq_current"
    assert result.evidence["source_trade_id"] == "trade_bridge_fill_MNQ_1m_2026-06-03T02_47_00Z_SELL_TO_OPEN"
    assert result.evidence["exec_id"] == "0000e1a7.6a2ecf0d.01.01"


def test_synthetic_bridge_fill_trade_id_still_blocks_when_broker_identity_not_exact(tmp_path: Path) -> None:
    _write_bridge_fill_lifecycle_report(
        tmp_path,
        source_trade_id="trade_bridge_fill_MNQ_1m_2026-06-03T02_47_00Z_SELL_TO_OPEN",
        lifecycle_id="bridge_fill_MNQ|1m|2026-06-03T02:47:00Z|SELL_TO_OPEN",
        exec_id="0000e1a7.6a2ecf0d.01.01",
        perm_id=999,
    )

    result = resolve_broker_backed_fill_evidence(repo_root=tmp_path, request=_request())

    assert result.classification == MISSING_EXEC_ID
    assert any(row["reason"] == "trade_id_mismatch" for row in result.rejected)


def test_broker_position_alone_cannot_create_broker_backed_fill(tmp_path: Path) -> None:
    path = tmp_path / "outputs/reports/ibkr_read_only_verification/ibkr_positions_snapshot.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "positions": [
                    {
                        "account_id": "DUM882026",
                        "symbol": "MNQ",
                        "local_symbol": "MNQM6",
                        "con_id": 770561201,
                        "quantity": "1",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    result = resolve_broker_backed_fill_evidence(repo_root=tmp_path, request=_request())

    assert result.classification == MISSING_EXEC_ID
    assert result.matches == ()


def _request() -> BrokerFillEvidenceRequest:
    return BrokerFillEvidenceRequest(
        trade_id="trade_mnq_current",
        submit_intent_id="submit_owner_mnq_current",
        order_id="1",
        client_id=11127,
        perm_id=1955790757,
        con_id=770561201,
        local_symbol="MNQM6",
        account_id="DUM882026",
        action="BUY",
        qty=1,
        symbol="MNQ",
    )


def _write_submit_ownership(tmp_path: Path, *, exec_id: str | None) -> None:
    path = tmp_path / "outputs/track_b_execution_core/submit_intent_ownership/track_b_submit_intent_ownership.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "extra": {"trade_id": "trade_mnq_current"},
        "ownership_intent_id": "submit_owner_mnq_current",
        "broker_order_id": "1",
        "client_id": 11127,
        "perm_id": 1955790757,
        "exec_id": exec_id,
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "action": "BUY",
        "qty": 1,
    }
    path.write_text(json.dumps(row) + "\n", encoding="utf-8")


def _write_bridge_report(
    tmp_path: Path,
    *,
    exec_id: str | None,
    extra_exec_ids: list[str] | None = None,
    perm_id: int = 1955790757,
    account_id: str = "DUM882026",
    local_symbol: str = "MNQM6",
    con_id: int = 770561201,
) -> None:
    path = tmp_path / "outputs/reports/ibkr_runtime_route_dispatch/mnq_us_active_participation_long/ibkr_paper_strategy_bridge_report.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    executions = []
    if exec_id is not None:
        executions.append(
            {
                "execution_id": exec_id,
                "account_id": account_id,
                "symbol": "MNQ",
                "quantity": 1,
                "side": "BOT",
                "price": "30437.00",
                "executed_at": "2026-06-01T13:36:09+00:00",
            }
        )
    for item in extra_exec_ids or []:
        executions.append(
            {
                "execution_id": item,
                "account_id": account_id,
                "symbol": "MNQ",
                "quantity": 1,
                "side": "BOT",
                "price": "30437.25",
                "executed_at": "2026-06-01T13:36:10+00:00",
            }
        )
    payload = {
        "selected_account_id": account_id,
        "intent": {"symbol": "MNQ", "quantity": 1, "action": "BUY", "order_intent_id": "submit_owner_mnq_current"},
        "qualified_contract_report": {
            "qualified_contract": {
                "symbol": "MNQ",
                "local_symbol": local_symbol,
                "con_id": con_id,
                "expiry": "20260618",
            }
        },
        "delegated_result": {
            "report": {
                "submit_cancel_lifecycle": {
                    "latest_order_status": {
                        "order_id": 1,
                        "client_id": 11127,
                        "perm_id": perm_id,
                    },
                    "executions_after_submit": executions,
                }
            }
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_bridge_fill_lifecycle_report(
    tmp_path: Path,
    *,
    source_trade_id: str,
    lifecycle_id: str,
    exec_id: str | None,
    perm_id: int = 1955790757,
) -> None:
    path = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
        / lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "trade_id": source_trade_id,
        "lifecycle_id": lifecycle_id,
        "account_id": "DUM882026",
        "instrument_family": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "entry_intent": {
            "trade_id": source_trade_id,
            "lifecycle_id": lifecycle_id,
            "order_action": "BUY",
            "quantity": 1,
            "account_id": "DUM882026",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
        },
        "entry_submit_attempt": {
            "broker_order_id": "1",
            "client_id": 11127,
            "perm_id": perm_id,
        },
        "entry_fill": {
            "broker_order_id": "1",
            "execution_id": exec_id,
            "perm_id": perm_id,
            "price": "30437.00",
            "quantity": "1",
            "filled_at": "2026-06-03T02:49:10.058184+00:00",
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
