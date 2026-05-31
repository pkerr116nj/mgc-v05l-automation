from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from mgc_v05l.execution_core.track_b_central_trade_registry import TradeCurrentState, TradeEventType
from mgc_v05l.execution_core.track_b_live_trade_registry import (
    append_live_trade_registry_event,
    broker_backed_fill_has_required_ids,
    make_live_trade_registry_event,
)
from mgc_v05l.execution_core.track_b_trade_registry_reconstruction import (
    TradeRegistryReconstructionConfig,
    reconstruct_trade_registry_from_artifacts,
)


NOW = datetime(2026, 5, 31, 21, 55, tzinfo=UTC)


def test_live_trade_registry_appends_and_reconstructs_one_trade_chain(tmp_path):
    repo_root = tmp_path
    source_path = str(repo_root / "outputs/track_b_execution_core/strategy_bridge/bridge_report.json")
    base = {
        "trade_id": "trade_active_evidence_1",
        "lifecycle_id": "life_active_evidence_1",
        "lane_id": "mnq_globex_active_participation_long",
        "thesis_strategy_id": "PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_LONG_V1",
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "con_id": 770561201,
        "local_symbol": "MNQM6",
        "expiry": "202606",
        "side": "LONG",
        "action": "BUY",
        "qty": Decimal("1"),
        "source_artifact_path": source_path,
        "generated_at": NOW,
    }
    for event_type, extra in (
        (TradeEventType.ENTRY_INTENT_CREATED, {}),
        (TradeEventType.ENTRY_ORDER_SUBMITTED, {"order_id": "101", "client_id": "17086"}),
        (
            TradeEventType.ENTRY_FILL_BROKER_BACKED,
            {"order_id": "101", "client_id": "17086", "perm_id": "2047", "exec_id": "exec-1", "price": "30380.25"},
        ),
        (
            TradeEventType.LIFECYCLE_OPEN_MANAGED,
            {"order_id": "101", "client_id": "17086", "perm_id": "2047", "exec_id": "exec-1", "price": "30380.25"},
        ),
    ):
        append_live_trade_registry_event(
            repo_root=repo_root,
            event=make_live_trade_registry_event(event_type=event_type, **base, **extra),
        )

    report = reconstruct_trade_registry_from_artifacts(
        config=TradeRegistryReconstructionConfig(repo_root=repo_root),
        now=NOW,
    )

    assert len(report.records) == 1
    record = report.records[0]
    assert record.trade_id == "trade_active_evidence_1"
    assert record.current_state == TradeCurrentState.OPEN_MANAGED
    assert record.broker_backed_entry is True
    assert record.ownership_identity is not None
    assert record.ownership_identity.lifecycle_id == "life_active_evidence_1"


def test_broker_backed_fill_requires_perm_and_exec_id():
    assert broker_backed_fill_has_required_ids(perm_id="2047", exec_id="exec-1") is True
    assert broker_backed_fill_has_required_ids(perm_id="2047", exec_id=None) is False
    assert broker_backed_fill_has_required_ids(perm_id=None, exec_id="exec-1") is False
