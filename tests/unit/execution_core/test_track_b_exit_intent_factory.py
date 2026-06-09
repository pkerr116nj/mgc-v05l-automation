from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_exit_intent_factory import (
    TrackBExitIntentFactoryConfig,
    build_track_b_exit_intent_factory_report,
)


NOW = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)


def test_timebox_full_close_long_creates_sell_intent(tmp_path: Path) -> None:
    report = _report(tmp_path, [_selection(side="LONG", qty="2", strategy_type="timebox_close")])

    intent = report["exit_intents"][0]
    assert report["classification"] == "EXIT_INTENT_FACTORY_INTENTS_READY"
    assert intent["position_side"] == "LONG"
    assert intent["close_action"] == "SELL"
    assert intent["owned_qty"] == "2"
    assert intent["close_qty"] == "2"
    assert intent["remaining_qty_after"] == "0"
    assert intent["exit_type"] == "TIMEBOX_CLOSE"


def test_timebox_full_close_short_creates_buy_intent(tmp_path: Path) -> None:
    report = _report(tmp_path, [_selection(side="SHORT", qty="1", strategy_type="timebox_close")])

    intent = report["exit_intents"][0]
    assert intent["position_side"] == "SHORT"
    assert intent["close_action"] == "BUY"
    assert intent["close_qty"] == "1"
    assert intent["remaining_qty_after"] == "0"


def test_partial_close_intent_requires_explicit_quantity_source(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        [
            _selection(
                action="REDUCE",
                strategy_type="partial_scale_out",
                qty="3",
                reduce_qty="1",
                close_qty_source="RISK_POLICY",
            )
        ],
    )

    intent = report["exit_intents"][0]
    assert intent["exit_type"] == "PARTIAL_SCALE_OUT"
    assert intent["close_qty"] == "1"
    assert intent["remaining_qty_after"] == "2"
    assert intent["close_qty_source"] == "RISK_POLICY"
    assert intent["allow_partial"] is True
    assert intent["partial_policy_supported"] is True


def test_partial_close_without_quantity_source_emits_no_intent(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        [_selection(action="REDUCE", strategy_type="partial_scale_out", qty="3", reduce_qty="1")],
    )

    assert report["classification"] == "EXIT_INTENT_FACTORY_BLOCKED"
    assert report["intent_count"] == 0
    assert report["blocked_selections"][0]["reason"] == "partial_close_qty_source_missing"


def test_reversal_emits_close_only_full_close_intent(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        [_selection(action="REVERSE_CONSIDER", side="LONG", qty="2", strategy_type="reversal_close_only")],
    )

    intent = report["exit_intents"][0]
    assert intent["exit_type"] == "REVERSAL_EXIT"
    assert intent["close_action"] == "SELL"
    assert intent["close_qty"] == "2"
    assert intent["remaining_qty_after"] == "0"
    assert intent["allow_reverse"] is False


def test_hold_selection_emits_no_intent(tmp_path: Path) -> None:
    report = _report(tmp_path, [_selection(action="HOLD", strategy_type="timebox_close")])

    assert report["classification"] == "EXIT_INTENT_FACTORY_BLOCKED"
    assert report["intent_count"] == 0
    assert report["blocked_selections"][0]["reason"] == "hold_decision_does_not_create_exit_intent"


def test_idempotency_key_is_stable(tmp_path: Path) -> None:
    selection = _selection(strategy_type="timebox_close", side="SHORT", qty="1")

    first = _report(tmp_path, [selection])
    second = _report(tmp_path, [selection])

    assert first["exit_intents"][0]["idempotency_key"] == second["exit_intents"][0]["idempotency_key"]


def test_attribution_status_and_ids_are_preserved(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        [
            _selection(
                lifecycle_id="lifecycle_1",
                trade_id="trade_1",
                strategy_id="strategy_1",
                lane_id="lane_1",
            )
        ],
    )

    intent = report["exit_intents"][0]
    assert intent["lifecycle_id"] == "lifecycle_1"
    assert intent["trade_id"] == "trade_1"
    assert intent["strategy_id"] == "strategy_1"
    assert intent["lane_id"] == "lane_1"
    assert report["exit_intent_metadata"][0]["attribution_status"] == "ATTRIBUTED"


def test_unattributed_broker_scoped_position_still_creates_intent(tmp_path: Path) -> None:
    report = _report(tmp_path, [_selection(attribution_status="UNATTRIBUTED")])

    intent = report["exit_intents"][0]
    assert report["exit_intent_metadata"][0]["attribution_status"] == "UNATTRIBUTED"
    assert intent["lifecycle_id"] is None
    assert intent["trade_id"] is None
    assert intent["close_action"] == "BUY"


def test_execution_domain_and_account_are_preserved(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        [_selection(execution_domain="TRACK_B_LIVE", account_id="DU12345", local_symbol="MNQM6", instrument="MNQ")],
    )

    intent = report["exit_intents"][0]
    assert intent["execution_domain"] == "TRACK_B_LIVE"
    assert intent["account_id"] == "DU12345"
    assert intent["local_symbol"] == "MNQM6"
    assert intent["instrument"] == "MNQ"


def test_read_only_flags_confirm_no_authority_or_execution_integration(tmp_path: Path) -> None:
    report = _report(tmp_path, [_selection()])

    assert report["read_only"] is True
    assert report["broker_state_mutated"] is False
    assert report["submit_attempted"] is False
    assert report["cancel_attempted"] is False
    assert report["close_attempted"] is False
    assert report["service_started"] is False
    assert report["runtime_restarted"] is False
    assert report["authority_validated"] is False
    assert report["actuator_integrated"] is False


def test_flat_selector_source_produces_flat_factory_report(tmp_path: Path) -> None:
    report = _report(tmp_path, [], selector_classification="EXIT_STRATEGY_SELECTOR_FLAT")

    assert report["classification"] == "EXIT_INTENT_FACTORY_FLAT"
    assert report["intent_count"] == 0


def _report(
    tmp_path: Path,
    selected: list[dict],
    *,
    selector_classification: str = "EXIT_STRATEGY_SELECTOR_SELECTED",
) -> dict:
    return build_track_b_exit_intent_factory_report(
        config=TrackBExitIntentFactoryConfig(repo_root=tmp_path),
        now=NOW,
        input_overrides={
            "exit_strategy_selector": {
                "schema_version": "track_b_exit_strategy_selector_report_v1",
                "generated_at": NOW.isoformat(),
                "classification": selector_classification,
                "selected_count": len(selected),
                "selected_decisions": selected,
                "diagnostic_rows": [],
                "live_money_eligible": False,
                "paper_proof_invoked": False,
            }
        },
    )


def _selection(
    *,
    selection_id: str = "selection_1",
    decision_id: str = "decision_1",
    strategy_type: str = "timebox_close",
    action: str = "FULL_CLOSE",
    execution_domain: str = "TRACK_B_PAPER",
    account_id: str = "DUM882026",
    con_id: int = 770561194,
    local_symbol: str = "MESM6",
    instrument: str = "MES",
    side: str = "SHORT",
    qty: str = "1",
    reduce_qty: str | None = None,
    close_qty_source: str | None = None,
    attribution_status: str = "ATTRIBUTED",
    lifecycle_id: str | None = "lifecycle_1",
    trade_id: str | None = "trade_1",
    strategy_id: str | None = "strategy_1",
    lane_id: str | None = "lane_1",
) -> dict:
    if attribution_status == "UNATTRIBUTED":
        lifecycle_id = None
        trade_id = None
        strategy_id = None
        lane_id = None
    position = {
        "schema_version": "track_b_position_state_v1",
        "execution_domain": execution_domain,
        "account_id": account_id,
        "con_id": con_id,
        "local_symbol": local_symbol,
        "instrument": instrument,
        "side": side,
        "qty": qty,
        "owned_qty": qty if attribution_status == "ATTRIBUTED" else None,
        "attribution_status": attribution_status,
        "lifecycle_id": lifecycle_id,
        "trade_id": trade_id,
        "strategy_id": strategy_id,
        "lane_id": lane_id,
        "current_scope": True,
        "source_artifact_refs": [],
        "diagnostic_rows": [],
    }
    decision = {
        "schema_version": "track_b_exit_decision_v1",
        "decision_id": decision_id,
        "action": action,
        "execution_domain": execution_domain,
        "account_id": account_id,
        "con_id": con_id,
        "local_symbol": local_symbol,
        "instrument": instrument,
        "side": side,
        "qty": qty,
        "reason": "timebox_or_exit_due",
        "priority": 50,
        "urgency": "NORMAL",
        "source_policy_id": "POLICY_1",
        "position": position,
        "source_artifact_refs": [],
        "diagnostics": [],
    }
    if reduce_qty is not None:
        decision["reduce_qty"] = reduce_qty
    if close_qty_source is not None:
        decision["close_qty_source"] = close_qty_source
    return {
        "schema_version": "track_b_exit_strategy_selector_v1",
        "selection_id": selection_id,
        "strategy_type": strategy_type,
        "priority": 80,
        "decision_id": decision_id,
        "execution_domain": execution_domain,
        "account_id": account_id,
        "con_id": con_id,
        "local_symbol": local_symbol,
        "instrument": instrument,
        "side": side,
        "qty": qty,
        "action": action,
        "reason": "timebox_or_exit_due",
        "close_only": True,
        "entry_allowed": False,
        "flip_allowed": False,
        "selected_decision": decision,
        "source_artifact_refs": [],
        "diagnostics": [],
    }
