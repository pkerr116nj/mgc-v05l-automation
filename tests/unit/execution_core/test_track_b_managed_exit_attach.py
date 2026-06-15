from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

import mgc_v05l.execution_core.track_b_managed_exit_attach as attach_module
from mgc_v05l.execution_core.track_b_central_trade_registry import TradeEventType
from mgc_v05l.execution_core.track_b_live_trade_registry import (
    append_live_trade_registry_event,
    make_live_trade_registry_event,
)
from mgc_v05l.execution_core.track_b_managed_exit_attach import (
    MANAGED_EXIT_APPLY_DISABLED,
    MANAGED_EXIT_BLOCKED_DUPLICATE_CLOSE_ORDER,
    MANAGED_EXIT_BLOCKED_POSITION_MISMATCH,
    MANAGED_EXIT_NOT_YET_ELIGIBLE,
    MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE,
    TrackBManagedExitAttachConfig,
    build_track_b_managed_exit_attach_plan,
)


NOW = datetime(2026, 5, 25, 7, 48, tzinfo=timezone.utc)


def test_plan_ready_when_3x5m_elapsed(tmp_path: Path) -> None:
    config = _seed(tmp_path, completed_bars=3)

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE
    assert payload["timebox_exit_eligible"] is True
    assert payload["apply_boundary_classification"] == MANAGED_EXIT_APPLY_DISABLED
    assert payload["close_intent_preview"]["order_action"] == "SELL"
    assert payload["close_intent_preview"]["quantity"] == 1
    assert payload["close_intent_preview"]["expiry"] == config.expiry
    assert payload["close_intent_preview"]["would_submit"] is False
    assert payload["runtime_pricing_reference"]["pricing_source"] == "DATABENTO_RUNTIME"
    assert payload["runtime_pricing_reference"]["reference_age_seconds"] == 60.0
    assert payload["close_pricing_policy"]["classification"] == "MANAGED_CLOSE_PRICED"
    assert payload["close_pricing_policy"]["marketable_execution_required"] is True
    assert payload["close_pricing_policy"]["passive_execution_allowed"] is False
    assert payload["exit_execution_class"] == "EXIT_CLASS_RISK_REDUCING"
    assert payload["risk_reducing_exit_execution"] is True
    assert payload["broker_state_mutated"] is False
    assert payload["exit_roster_compatible"] is True
    assert payload["exit_strategy_id"] == "timeboxed_3x5m_managed_limit_close_v1"


def test_plan_blocks_close_price_when_runtime_market_data_is_stale(tmp_path: Path) -> None:
    config = _seed(tmp_path, completed_bars=3)
    _write_json(
        tmp_path / "outputs/track_b_execution_core/phase1_runtime_market_data" / config.instrument_family / "1m/latest_runtime_candles.json",
        {
            "generated_at": "2026-05-25T07:40:00+00:00",
            "bars": [{"bar_end": "2026-05-25T07:40:00+00:00", "close": "29965.5"}],
        },
    )

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["close_pricing_policy"]["classification"] == "MANAGED_CLOSE_PRICING_BLOCKED"
    assert payload["close_pricing_policy"]["stale_reference_blocker"] == "MANAGED_CLOSE_REFERENCE_STALE"
    assert payload["close_intent_preview"]["submit_allowed"] is False
    assert "Current executable close price is unavailable." in payload["blockers"]


def test_buy_to_close_without_bid_ask_uses_fresh_close_plus_aggressive_paper_offset(tmp_path: Path) -> None:
    config = _seed(
        tmp_path,
        completed_bars=3,
        config_overrides={"side": "SHORT"},
        position_overrides={"quantity": "-1", "side": "SHORT"},
    )

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    policy = payload["close_pricing_policy"]
    assert policy["classification"] == "MANAGED_CLOSE_PRICED"
    assert policy["close_action"] == "BUY"
    assert policy["reference_price_kind"] == "close"
    assert policy["limit_price"] == "30564.75"
    assert policy["aggressive_paper_fallback"] is True
    assert policy["marketable_execution_required"] is True
    assert policy["passive_execution_allowed"] is False
    assert policy["marketable_limit_offset_ticks"] == 2397.0
    assert payload["close_intent_preview"]["close_limit_price"] == "30564.75"


def test_sell_to_close_without_bid_ask_uses_fresh_close_minus_aggressive_paper_offset(tmp_path: Path) -> None:
    config = _seed(tmp_path, completed_bars=3)

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    policy = payload["close_pricing_policy"]
    assert policy["classification"] == "MANAGED_CLOSE_PRICED"
    assert policy["close_action"] == "SELL"
    assert policy["reference_price_kind"] == "close"
    assert policy["limit_price"] == "29366.25"
    assert policy["aggressive_paper_fallback"] is True
    assert policy["marketable_execution_required"] is True
    assert policy["passive_execution_allowed"] is False
    assert policy["marketable_limit_offset_ticks"] == 2397.0


def test_attach_prefers_ask_for_buy_to_close_when_bid_ask_available(tmp_path: Path) -> None:
    config = _seed(
        tmp_path,
        completed_bars=3,
        config_overrides={"side": "SHORT"},
        position_overrides={"quantity": "-1", "side": "SHORT"},
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/phase1_runtime_market_data" / config.instrument_family / "1m/latest_runtime_candles.json",
        {
            "generated_at": NOW.isoformat(),
            "bars": [{"bar_end": "2026-05-25T07:47:00+00:00", "bid_price": "29965.25", "ask_price": "29966.25", "close": "29965.5"}],
        },
    )

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    policy = payload["close_pricing_policy"]
    assert policy["classification"] == "MANAGED_CLOSE_PRICED"
    assert policy["reference_price_kind"] == "ask_price"
    assert policy["reference_price"] == "29966.25"
    assert policy["limit_price"] == "29966.25"
    assert policy["aggressive_paper_fallback"] is False


def test_not_eligible_before_3x5m(tmp_path: Path) -> None:
    config = _seed(tmp_path, completed_bars=2)

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_NOT_YET_ELIGIBLE
    assert payload["timebox_exit_eligible"] is False
    assert payload["close_intent_preview"]["submit_allowed"] is False


def test_blocked_on_position_mismatch(tmp_path: Path) -> None:
    config = _seed(tmp_path, completed_bars=3, position_overrides={"local_symbol": "WRONG"})

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_BLOCKED_POSITION_MISMATCH
    assert payload["position_identity_verified"] is False
    assert payload["broker_state_mutated"] is False


def test_blocked_on_duplicate_close_order(tmp_path: Path) -> None:
    config = _seed(
        tmp_path,
        completed_bars=12,
        managed_order_overrides={
            "classification": "WORKING_CLOSE_ORDER",
            "managed_orders": [
                {
                    "working": True,
                    "local_symbol": "MNQM6",
                    "action": "SELL",
                    "quantity": "1",
                }
            ],
        },
    )

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_BLOCKED_DUPLICATE_CLOSE_ORDER
    assert payload["duplicate_close_order_detected"] is True


def test_blocked_when_lifecycle_already_has_prior_close_submit(tmp_path: Path) -> None:
    config = _seed(
        tmp_path,
        completed_bars=3,
        config_overrides={"apply": True, "operator_authorized_managed_exit": True},
    )
    lifecycle_path = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
        / config.lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    lifecycle = json.loads(lifecycle_path.read_text(encoding="utf-8"))
    lifecycle["close_submit_attempt"] = {
        "broker_order_id": "36",
        "broker_state_mutated": True,
        "submit_attempted": True,
        "submitted": False,
    }
    _write_json(lifecycle_path, lifecycle)

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_BLOCKED_DUPLICATE_CLOSE_ORDER
    assert payload["apply_enabled"] is False
    assert payload["submit_attempted"] is False
    assert payload["broker_state_mutated"] is False
    assert payload["duplicate_close_order_detected"] is True
    assert "broker order 36" in payload["prior_lifecycle_close_submit_blocker"]


def test_prior_lifecycle_close_fill_is_diagnostic_when_broker_risk_still_open_and_v11_allows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _seed(
        tmp_path,
        completed_bars=12,
        config_overrides={"apply": True, "operator_authorized_managed_exit": True},
        open_order_overrides={"classification": "NO_OPEN_ORDERS", "unknown_open_order_count": 0},
    )
    lifecycle_path = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
        / config.lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    lifecycle = json.loads(lifecycle_path.read_text(encoding="utf-8"))
    lifecycle["close_fill"] = {
        "broker_order_id": "84",
        "perm_id": "629785906",
        "execution_id": "exec-old-close",
        "price": "7418.75",
        "quantity": "1",
        "filled_at": "2026-06-08T09:23:54+00:00",
    }
    _write_json(lifecycle_path, lifecycle)

    class Result:
        report_json = lifecycle_path
        report = {
            **lifecycle,
            "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
            "final_position_status": "OPEN_MANAGED",
            "broker_state_mutated": False,
            "submit_attempted": False,
        }

    called: dict[str, Any] = {}

    def fake_maintain(**kwargs: Any) -> Result:
        called["maintain"] = kwargs
        return Result()

    monkeypatch.setattr(attach_module, "maintain_open_track_b_strategy_managed_paper_lifecycle", fake_maintain)

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED"
    assert payload["apply_enabled"] is True
    assert payload["duplicate_close_order_detected"] is False
    assert payload["prior_lifecycle_close_submit_blocker"] is None
    assert "close fill" in payload["prior_lifecycle_close_stale_diagnostic"]
    assert payload["exit_authority_contract"]["decision"]["decision"] in {"ALLOWED", "DEGRADED_ALLOWED"}
    assert called["maintain"]["existing_lifecycle_report"]["close_fill"]["broker_order_id"] == "84"


def test_previous_attach_guard_review_state_can_retry_when_broker_identity_matches(tmp_path: Path) -> None:
    config = _seed(tmp_path, completed_bars=3)
    lifecycle_path = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
        / config.lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    lifecycle = json.loads(lifecycle_path.read_text(encoding="utf-8"))
    lifecycle.update(
        {
            "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
            "final_position_status": "REVIEW_REQUIRED",
            "broker_state_mutated": False,
            "close_intent": None,
            "close_submit_attempt": None,
            "primary_blocker": "Strategy-managed PAPER lifecycle requires latest decision bar source DATABENTO_LIVE_ARTIFACT.",
        }
    )
    _write_json(lifecycle_path, lifecycle)

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] != MANAGED_EXIT_BLOCKED_POSITION_MISMATCH
    assert payload["lifecycle_identity_verified"] is True


def test_unmutated_positive_quantity_review_can_retry_after_signed_quantity_fix(tmp_path: Path) -> None:
    config = _seed(
        tmp_path,
        completed_bars=3,
        config_overrides={
            "side": "SHORT",
        },
        position_overrides={
            "quantity": "-1",
            "side": "SHORT",
        },
    )
    lifecycle_path = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
        / config.lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    lifecycle = json.loads(lifecycle_path.read_text(encoding="utf-8"))
    lifecycle.update(
        {
            "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
            "final_position_status": "REVIEW_REQUIRED",
            "broker_state_mutated": False,
            "close_intent": None,
            "close_submit_attempt": None,
            "primary_blocker": "Strategy-managed PAPER lifecycle requires positive configured quantity.",
        }
    )
    _write_json(lifecycle_path, lifecycle)

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] != MANAGED_EXIT_BLOCKED_POSITION_MISMATCH
    assert payload["position_identity_verified"] is True
    assert payload["lifecycle_identity_verified"] is True
    assert payload["close_intent_preview"]["order_action"] == "BUY"
    assert payload["close_intent_preview"]["quantity"] == 1


def test_unmutated_close_authorization_review_can_retry_when_close_intent_matches(tmp_path: Path) -> None:
    config = _seed(tmp_path, completed_bars=3)
    lifecycle_path = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
        / config.lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    lifecycle = json.loads(lifecycle_path.read_text(encoding="utf-8"))
    lifecycle.update(
        {
            "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
            "final_position_status": "REVIEW_REQUIRED",
            "broker_state_mutated": False,
            "primary_blocker": "Planner does not reference the active snapshot id.",
            "close_intent": {
                "lifecycle_id": config.lifecycle_id,
                "strategy_id": config.strategy_id,
                "local_symbol": config.local_symbol,
                "con_id": config.con_id,
                "order_action": "SELL",
                "quantity": 1,
                "managed_exit_policy_id": config.managed_exit_policy_id,
            },
            "close_submit_attempt": {
                "submitted": False,
                "broker_state_mutated": False,
                "classification": "STRATEGY_SUBMIT_BLOCKED_SUPERVISOR",
            },
        }
    )
    _write_json(lifecycle_path, lifecycle)

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] != MANAGED_EXIT_BLOCKED_POSITION_MISMATCH
    assert payload["lifecycle_identity_verified"] is True


def test_apply_disabled_without_both_flags(tmp_path: Path) -> None:
    config = TrackBManagedExitAttachConfig(**{**_seed(tmp_path, completed_bars=3).__dict__, "apply": True})

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_APPLY_DISABLED
    assert payload["apply_enabled"] is False
    assert payload["submit_attempted"] is False
    assert payload["broker_state_mutated"] is False


def test_apply_updates_paper_trade_ledger_projection(tmp_path: Path, monkeypatch) -> None:
    config = TrackBManagedExitAttachConfig(
        **{
            **_seed(tmp_path, completed_bars=3).__dict__,
            "apply": True,
            "operator_authorized_managed_exit": True,
        }
    )
    lifecycle_report_path = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
        / config.lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    lifecycle = json.loads(lifecycle_report_path.read_text(encoding="utf-8"))
    closed_lifecycle = {
        **lifecycle,
        **{
            "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_CLOSED_FLAT",
            "final_position_status": "CLOSED_FLAT",
            "broker_state_mutated": True,
            "submit_attempted": True,
            "close_intent": {
                "lifecycle_id": config.lifecycle_id,
                "strategy_id": config.strategy_id,
                "local_symbol": config.local_symbol,
                "con_id": config.con_id,
                "order_action": "SELL",
                "quantity": 1,
                "close_limit_price": "29965",
                "managed_exit_policy_id": config.managed_exit_policy_id,
            },
            "close_submit_attempt": {
                "submitted": True,
                "broker_state_mutated": True,
                "broker_order_id": "34",
            },
            "close_fill": {
                "broker_order_id": "34",
                "perm_id": "917751476",
                "execution_id": "exec-34",
                "price": "29965",
                "quantity": "1",
                "filled_at": "2026-05-25T07:48:00+00:00",
            },
        },
    }
    calls: dict[str, Any] = {}

    class Result:
        report_json = lifecycle_report_path
        report = closed_lifecycle

    class LedgerResult:
        trade_record_written = True
        ledger_jsonl = tmp_path / "ledger.jsonl"
        trade_summary_json = tmp_path / "summary.json"
        live_position_status_json = tmp_path / "live.json"
        pnl_summary_json = tmp_path / "pnl.json"
        trade_summary = {"open_position_count": 0}
        trade_record = {"realized_pnl": "82.5"}

    def fake_maintain(**kwargs: Any) -> Result:
        calls["maintain"] = kwargs
        return Result()

    def fake_ledger_update(**kwargs: Any) -> LedgerResult:
        calls["ledger_update"] = kwargs
        return LedgerResult()

    monkeypatch.setattr(attach_module, "maintain_open_track_b_strategy_managed_paper_lifecycle", fake_maintain)
    monkeypatch.setattr(attach_module, "update_track_b_paper_trade_ledger_from_runner_report", fake_ledger_update)

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == "TRACK_B_STRATEGY_PAPER_CLOSED_FLAT"
    assert calls["ledger_update"]["runner_report"]["managed_lifecycle_invoked"] is True
    assert calls["ledger_update"]["runner_report"]["managed_lifecycle_report_path"] == str(lifecycle_report_path)
    assert payload["apply_result"]["paper_trade_ledger_update"]["trade_record_written"] is True
    assert payload["apply_result"]["paper_trade_ledger_update"]["open_position_count"] == 0
    assert payload["apply_result"]["paper_trade_ledger_update"]["realized_pnl_by_lifecycle"] == ["82.5"]


def test_apply_uses_registry_born_trade_id_for_managed_exit_identity(tmp_path: Path, monkeypatch) -> None:
    config = TrackBManagedExitAttachConfig(
        **{
            **_seed(tmp_path, completed_bars=3).__dict__,
            "apply": True,
            "operator_authorized_managed_exit": True,
        }
    )
    lifecycle_report_path = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
        / config.lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    lifecycle = json.loads(lifecycle_report_path.read_text(encoding="utf-8"))
    lifecycle["trade_id"] = "placeholder_strategy_lifecycle_trade_id"
    lifecycle["entry_intent"]["trade_id"] = "placeholder_strategy_lifecycle_trade_id"
    _write_json(lifecycle_report_path, lifecycle)
    registry_trade_id = "trade_registry_born_exact_owner"
    source_path = str(tmp_path / "outputs/track_b_execution_core/strategy_bridge/bridge_report.json")
    registry_base = {
        "trade_id": registry_trade_id,
        "lifecycle_id": config.lifecycle_id,
        "lane_id": config.lane_id,
        "thesis_strategy_id": config.strategy_id,
        "account_id": config.account_id,
        "symbol": config.instrument_family,
        "con_id": config.con_id,
        "local_symbol": config.local_symbol,
        "expiry": config.expiry,
        "side": config.side,
        "action": "BUY",
        "qty": Decimal("1"),
        "source_artifact_path": source_path,
        "generated_at": NOW,
    }
    for event_type, extra in (
        (TradeEventType.ENTRY_INTENT_CREATED, {}),
        (TradeEventType.ENTRY_ORDER_SUBMITTED, {"order_id": "33", "client_id": "17086"}),
        (
            TradeEventType.ENTRY_FILL_BROKER_BACKED,
            {"order_id": "33", "client_id": "17086", "perm_id": "perm-33", "exec_id": "exec-33", "price": "29923.75"},
        ),
        (
            TradeEventType.LIFECYCLE_OPEN_MANAGED,
            {"order_id": "33", "client_id": "17086", "perm_id": "perm-33", "exec_id": "exec-33", "price": "29923.75"},
        ),
    ):
        append_live_trade_registry_event(
            repo_root=tmp_path,
            event=make_live_trade_registry_event(event_type=event_type, **registry_base, **extra),
        )

    class Result:
        report_json = lifecycle_report_path
        report = {
            **lifecycle,
            "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
            "final_position_status": "OPEN_MANAGED",
        }

    seen: dict[str, Any] = {}

    def fake_maintain(**kwargs: Any) -> Result:
        seen["existing_lifecycle_report"] = kwargs["existing_lifecycle_report"]
        return Result()

    monkeypatch.setattr(attach_module, "maintain_open_track_b_strategy_managed_paper_lifecycle", fake_maintain)

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["broker_state_mutated"] is False
    assert seen["existing_lifecycle_report"]["trade_id"] == registry_trade_id
    assert seen["existing_lifecycle_report"]["entry_intent"]["trade_id"] == registry_trade_id


def test_aggregate_close_persists_all_lifecycle_units(tmp_path: Path, monkeypatch) -> None:
    config = _seed(tmp_path, completed_bars=3)
    unit_ids = ["lifecycle-46", "lifecycle-47", "lifecycle-48"]
    units = []
    for index, lifecycle_id in enumerate(unit_ids, start=1):
        report_path = (
            tmp_path
            / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
            / lifecycle_id
            / "track_b_strategy_managed_paper_lifecycle_report.json"
        )
        report = _lifecycle_payload(
            config=config,
            lifecycle_id=lifecycle_id,
            order_id=str(45 + index),
            price=str(29900 + index),
        )
        _write_json(report_path, report)
        units.append(
            {
                "lifecycle_id": lifecycle_id,
                "entry_order_id": str(45 + index),
                "quantity": "1",
                "paper_lifecycle_report_path": str(report_path),
            }
        )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "exit_due": True,
                    "attention_required": False,
                    "symbol": "MNQ",
                    "contract_key": config.contract_key,
                    "local_symbol": config.local_symbol,
                    "con_id": config.con_id,
                    "quantity": "3",
                    "aggregate_qty": "-3",
                    "side": "SHORT",
                    "strategy_id": config.strategy_id,
                    "lifecycle_id": unit_ids[-1],
                    "managed_exit_policy_id": config.managed_exit_policy_id,
                    "lifecycle_units": units,
                    "lifecycle_position": {
                        "account_id": config.account_id,
                        "instrument_family": "MNQ",
                        "contract_key": config.contract_key,
                        "local_symbol": config.local_symbol,
                        "con_id": config.con_id,
                        "quantity": "3",
                        "aggregate_qty": "-3",
                        "side": "SHORT",
                        "strategy_id": config.strategy_id,
                        "lifecycle_id": unit_ids[-1],
                        "managed_exit_policy_id": config.managed_exit_policy_id,
                        "lifecycle_units": units,
                        "paper_lifecycle_report_path": str(
                            tmp_path
                            / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
                            / unit_ids[-1]
                            / "track_b_strategy_managed_paper_lifecycle_report.json"
                        ),
                    },
                }
            ],
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json",
        {
            "positions_by_instrument": {
                config.contract_key: {
                    "account_id": config.account_id,
                    "contract_key": config.contract_key,
                    "local_symbol": config.local_symbol,
                    "con_id": config.con_id,
                    "quantity": "3",
                    "aggregate_qty": "-3",
                    "side": "SHORT",
                    "lifecycle_units": units,
                }
            }
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/position_truth/latest_position_truth.json",
        {"broker_positions": [{"account_id": config.account_id, "local_symbol": config.local_symbol, "con_id": config.con_id, "quantity": "-3"}]},
    )
    apply_config = TrackBManagedExitAttachConfig(
        **{
            **config.__dict__,
            "apply": True,
            "operator_authorized_managed_exit": True,
        }
    )
    latest_path = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
        / unit_ids[-1]
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    closed_lifecycle = {
        **json.loads(latest_path.read_text(encoding="utf-8")),
        "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_CLOSED_FLAT",
        "final_position_status": "CLOSED_FLAT",
        "broker_state_mutated": True,
        "submit_attempted": True,
        "close_intent": {"quantity": 3, "order_action": "BUY", "managed_exit_policy_id": config.managed_exit_policy_id},
        "close_submit_attempt": {"submitted": True, "broker_state_mutated": True, "broker_order_id": "55"},
        "close_fill": {"broker_order_id": "55", "price": "29915", "quantity": "3", "filled_at": NOW.isoformat()},
    }
    calls: list[str] = []

    class Result:
        report_json = latest_path
        report = closed_lifecycle

    class LedgerResult:
        trade_record_written = True
        ledger_jsonl = tmp_path / "ledger.jsonl"
        trade_summary_json = tmp_path / "summary.json"
        live_position_status_json = tmp_path / "live.json"
        pnl_summary_json = tmp_path / "pnl.json"
        trade_summary = {"open_position_count": 0}
        trade_record = {"realized_pnl": "1"}

    def fake_maintain(**kwargs: Any) -> Result:
        return Result()

    def fake_ledger_update(**kwargs: Any) -> LedgerResult:
        calls.append(str(kwargs["runner_report"]["managed_lifecycle_report_path"]))
        return LedgerResult()

    monkeypatch.setattr(attach_module, "maintain_open_track_b_strategy_managed_paper_lifecycle", fake_maintain)
    monkeypatch.setattr(attach_module, "update_track_b_paper_trade_ledger_from_runner_report", fake_ledger_update)

    payload = build_track_b_managed_exit_attach_plan(config=apply_config, now=NOW)

    assert payload["aggregate_exit_group"]["coordinated_lifecycle_close_required"] is True
    assert payload["apply_result"]["paper_trade_ledger_update"]["aggregate_lifecycle_close_persistence"]["synced_lifecycle_count"] == 3
    assert len(calls) == 3
    for lifecycle_id in unit_ids:
        report_path = (
            tmp_path
            / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
            / lifecycle_id
            / "track_b_strategy_managed_paper_lifecycle_report.json"
        )
        synced = json.loads(report_path.read_text(encoding="utf-8"))
        assert synced["final_position_status"] == "CLOSED_FLAT"
        assert synced["close_fill"]["aggregate_order_quantity"] == "3"
        assert synced["close_fill"]["quantity"] == "1"
        assert synced["aggregate_managed_exit_close"]["unit_count"] == 3


def test_no_live_money_or_paper_proof_route(tmp_path: Path) -> None:
    config = _seed(tmp_path, completed_bars=3, snapshot_overrides={"live_money_eligible": True})

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == "MANAGED_EXIT_BLOCKED_CONTROL_PLANE"
    assert payload["paper_only"] is True
    assert payload["paper_proof_invoked"] is False
    assert payload["broad_cancel_allowed"] is False
    assert payload["global_flatten_allowed"] is False


def test_exit_due_close_allows_unhealthy_runtime_identity_with_degraded_close_authority(tmp_path: Path) -> None:
    config = _seed(
        tmp_path,
        completed_bars=3,
        snapshot_overrides={
            "classification": "CONTROL_PLANE_SNAPSHOT_STALE_OR_MIXED",
            "shared_truth_coherence_status": "STALE_OR_MIXED",
            "runtime_supervisor_classification": "PROCESS_STARTED_PROFILE_PENDING",
            "safe_to_start_runtime": False,
        },
        open_order_overrides={"classification": "NO_OPEN_ORDERS", "unknown_open_order_count": 0},
    )

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE
    assert payload["apply_enabled"] is False
    assert payload["broker_session_allowed_uses"]["managed_risk_reducing_close"] is True
    assert payload["risk_reducing_close_connection_mode"] == "RISK_REDUCING_CLOSE_CAPABLE_ORDER_STATUS_DEGRADED"
    assert payload["close_intent_preview"]["order_action"] == "SELL"
    assert payload["close_intent_preview"]["submit_allowed"] is False
    assert payload["broker_state_mutated"] is False


def test_exact_exit_due_close_allows_entry_oriented_control_plane_block_with_close_authority(tmp_path: Path) -> None:
    config = _seed(
        tmp_path,
        completed_bars=3,
        snapshot_overrides={
            "classification": "CONTROL_PLANE_SNAPSHOT_BLOCKED",
            "shared_truth_coherence_status": "COHERENT",
            "runtime_supervisor_classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
            "open_order_truth_classification": "NO_OPEN_ORDERS",
            "managed_order_registry_classification": "NO_MANAGED_ORDERS",
            "operator_explanation": "Entry runtime state is blocked while managed close authority is exact.",
        },
        open_order_overrides={"classification": "NO_OPEN_ORDERS", "unknown_open_order_count": 0},
    )

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE
    assert payload["broker_session_allowed_uses"]["managed_risk_reducing_close"] is True
    assert payload["close_intent_preview"]["order_action"] == "SELL"
    assert payload["submit_attempted"] is False
    assert payload["broker_state_mutated"] is False


def test_exact_exit_due_close_allows_stale_or_mixed_control_plane_with_close_authority(tmp_path: Path) -> None:
    config = _seed(
        tmp_path,
        completed_bars=3,
        snapshot_overrides={
            "classification": "CONTROL_PLANE_SNAPSHOT_STALE_OR_MIXED",
            "shared_truth_coherence_status": "COHERENT",
            "runtime_supervisor_classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
            "blockers": [
                {
                    "code": "agent_health_blocks_runtime_submit",
                    "detail": "Agent Health blocks entry/runtime submit.",
                }
            ],
        },
        open_order_overrides={"classification": "NO_OPEN_ORDERS", "unknown_open_order_count": 0},
    )

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE
    assert payload["apply_enabled"] is False
    assert payload["broker_session_allowed_uses"]["managed_risk_reducing_close"] is True
    assert payload["close_intent_preview"]["order_action"] == "SELL"
    assert payload["close_intent_preview"]["submit_allowed"] is False
    assert payload["submit_attempted"] is False
    assert payload["broker_state_mutated"] is False


def test_exact_exit_due_close_allows_start_preflight_block_with_close_authority(tmp_path: Path) -> None:
    config = _seed(
        tmp_path,
        completed_bars=3,
        snapshot_overrides={
            "classification": "CONTROL_PLANE_SNAPSHOT_START_BLOCKED",
            "shared_truth_coherence_status": "COHERENT",
            "runtime_supervisor_classification": "PROCESS_STARTED_PROFILE_PENDING",
            "primary_blocking_reason": "Runtime profile has not loaded; entries are blocked.",
        },
        open_order_overrides={"classification": "NO_OPEN_ORDERS", "unknown_open_order_count": 0},
    )

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE
    assert payload["close_intent_preview"]["order_action"] == "SELL"
    assert payload["submit_attempted"] is False
    assert payload["broker_state_mutated"] is False


def test_exact_exit_due_close_allows_degraded_managed_order_topline_with_exact_row(tmp_path: Path) -> None:
    config = _seed(
        tmp_path,
        completed_bars=3,
        snapshot_overrides={
            "classification": "CONTROL_PLANE_SNAPSHOT_BLOCKED",
            "shared_truth_coherence_status": "COHERENT",
            "runtime_supervisor_classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
            "open_order_truth_classification": "NO_OPEN_ORDERS",
            "managed_order_registry_classification": "ORDER_STATE_UNKNOWN_REVIEW_REQUIRED",
        },
        open_order_overrides={"classification": "NO_OPEN_ORDERS", "unknown_open_order_count": 0},
        managed_order_overrides={"classification": "ORDER_STATE_UNKNOWN_REVIEW_REQUIRED"},
    )

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE
    assert payload["managed_order_registry_classification"] == "ORDER_STATE_UNKNOWN_REVIEW_REQUIRED"
    assert payload["broker_session_allowed_uses"]["managed_risk_reducing_close"] is True
    assert payload["close_intent_preview"]["order_action"] == "SELL"
    assert payload["submit_attempted"] is False
    assert payload["broker_state_mutated"] is False


def test_exact_exit_due_close_allows_degraded_managed_order_topline_without_exact_row_when_v11_allows(tmp_path: Path) -> None:
    config = _seed(
        tmp_path,
        completed_bars=3,
        snapshot_overrides={
            "classification": "CONTROL_PLANE_SNAPSHOT_BLOCKED",
            "shared_truth_coherence_status": "COHERENT",
            "runtime_supervisor_classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
            "open_order_truth_classification": "NO_OPEN_ORDERS",
            "managed_order_registry_classification": "ORDER_STATE_UNKNOWN_REVIEW_REQUIRED",
        },
        open_order_overrides={"classification": "NO_OPEN_ORDERS", "unknown_open_order_count": 0},
        managed_order_overrides={
            "classification": "ORDER_STATE_UNKNOWN_REVIEW_REQUIRED",
            "managed_orders": [
                {
                    "working": False,
                    "local_symbol": "MESM6",
                    "action": "SELL",
                    "quantity": "1",
                    "classification": "POSITION_WITHOUT_CLOSE_ORDER",
                }
            ],
        },
    )

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE
    assert payload["submit_attempted"] is False
    assert payload["broker_state_mutated"] is False
    assert payload["exit_authority_contract"]["decision"]["decision"] in {"ALLOWED", "DEGRADED_ALLOWED"}


def test_exact_exit_due_close_demotes_stale_control_plane_guardian_hard_hold(tmp_path: Path) -> None:
    config = _seed(
        tmp_path,
        completed_bars=3,
        snapshot_overrides={
            "classification": "CONTROL_PLANE_SNAPSHOT_BLOCKED",
            "shared_truth_coherence_status": "COHERENT",
            "runtime_supervisor_classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
            "open_order_truth_classification": "NO_OPEN_ORDERS",
            "managed_order_registry_classification": "NO_MANAGED_ORDERS",
            "top_line_classification": "BROKER_POSITION_GUARDIAN_HARD_HOLD",
        },
        open_order_overrides={"classification": "NO_OPEN_ORDERS", "unknown_open_order_count": 0},
    )

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE
    assert payload["blockers"] == []
    assert payload["submit_attempted"] is False
    assert payload["broker_state_mutated"] is False


def test_exit_due_close_allows_unhealthy_runtime_identity_without_legacy_bsa_close_flag_when_v11_allows(tmp_path: Path) -> None:
    config = _seed(
        tmp_path,
        completed_bars=3,
        snapshot_overrides={
            "classification": "CONTROL_PLANE_SNAPSHOT_STALE_OR_MIXED",
            "shared_truth_coherence_status": "STALE_OR_MIXED",
            "runtime_supervisor_classification": "PROCESS_STARTED_PROFILE_PENDING",
            "safe_to_start_runtime": False,
        },
        open_order_overrides={"classification": "NO_OPEN_ORDERS", "unknown_open_order_count": 0},
        broker_session_authority_overrides={
            "allowed_uses": {
                "new_entry": False,
                "managed_risk_reducing_close": False,
                "broker_observed_adoption_diagnosis": True,
                "fill_callback_adoption": False,
                "status_diagnostic": True,
            },
            "degraded_exact_risk_reducing_close_context": {"ready": False},
        },
    )

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE
    assert payload["apply_enabled"] is False
    assert payload["submit_attempted"] is False
    assert payload["broker_state_mutated"] is False
    assert payload["broker_session_allowed_uses"]["managed_risk_reducing_close"] is False
    assert payload["exit_authority_contract"]["decision"]["decision"] in {"ALLOWED", "DEGRADED_ALLOWED"}


def test_mgc_forced_session_exit_profile_builds_managed_close_preview(tmp_path: Path) -> None:
    config = _seed(
        tmp_path,
        completed_bars=3,
        config_overrides={
            "strategy_id": "gc_mgc_forced_session_baseline_v2__mgc_1x_all_lanes__asia_early_long",
            "lane_id": "mgc_1x_all_lanes__asia_early_long",
            "lifecycle_id": "reserved_submit_mgc_1x_all_lanes_asia_early_long_test",
            "instrument_family": "MGC",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "con_id": 712565978,
            "expiry": "20260626",
            "managed_exit_policy_id": "FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1",
            "exit_profile_id": "MGC_FORCED_SESSION_SEGMENT_TIMEBOX_3X5M_V1",
            "tick_size": "0.1",
        },
    )

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE
    assert payload["managed_exit_policy_id"] == "FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1"
    assert payload["exit_profile_id"] == "MGC_FORCED_SESSION_SEGMENT_TIMEBOX_3X5M_V1"
    assert payload["close_intent_preview"]["order_action"] == "SELL"
    assert payload["close_intent_preview"]["would_submit"] is False
    assert payload["broker_state_mutated"] is False


def test_auto_selects_current_exit_due_managed_position_instead_of_stale_default_target(tmp_path: Path) -> None:
    current_lifecycle_id = "reserved_submit_mnq_1x_asia_london_participation_asia_london_long_v6"
    strategy_id = "asia_london_participation_core_v1__mnq_1x_asia_london_participation__asia_london_long_v6"
    config = _seed(
        tmp_path,
        completed_bars=3,
        config_overrides={
            "strategy_id": strategy_id,
            "lane_id": "mnq_1x_asia_london_participation__asia_london_long_v6",
            "lifecycle_id": current_lifecycle_id,
        },
    )
    stale_config = TrackBManagedExitAttachConfig(repo_root=tmp_path, refresh_control_plane=False)
    lifecycle_path = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
        / current_lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "exit_due": True,
                    "attention_required": False,
                    "symbol": "MNQ",
                    "contract_key": "MNQ-202606",
                    "local_symbol": "MNQM6",
                    "con_id": 770561201,
                    "quantity": "1",
                    "side": "LONG",
                    "strategy_id": strategy_id,
                    "lifecycle_id": current_lifecycle_id,
                    "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
                    "lifecycle_position": {
                        "account_id": "DUM882026",
                        "instrument_family": "MNQ",
                        "contract_key": "MNQ-202606",
                        "local_symbol": "MNQM6",
                        "con_id": 770561201,
                        "quantity": "1",
                        "side": "LONG",
                        "strategy_id": strategy_id,
                        "lifecycle_id": current_lifecycle_id,
                        "paper_lifecycle_report_path": str(lifecycle_path),
                    },
                }
            ],
        },
    )

    payload = build_track_b_managed_exit_attach_plan(config=stale_config, now=NOW)

    assert config.lifecycle_id == current_lifecycle_id
    assert payload["classification"] == MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE
    assert payload["target_identity"]["lifecycle_id"] == current_lifecycle_id
    assert payload["target_identity"]["strategy_id"] == strategy_id
    assert payload["target_identity"]["lane_id"] == "mnq_1x_asia_london_participation__asia_london_long_v6"
    assert payload["close_intent_preview"]["lifecycle_id"] == current_lifecycle_id
    assert payload["managed_exit_due_automation"]["classification"] == "MANAGED_EXIT_DUE_READY_FOR_APPLY"


def test_current_scope_selected_position_accepts_bridge_fill_lifecycle_report_identity(tmp_path: Path) -> None:
    current_lifecycle_id = "reserved_submit_mes_globex_active_participation_long_current"
    bridge_lifecycle_id = "bridge_fill_MES|1m|2026-06-08T06:55:00Z|BUY_TO_OPEN"
    strategy_id = "mes_globex_active_participation_long"
    config = _seed(
        tmp_path,
        completed_bars=12,
        config_overrides={
            "strategy_id": strategy_id,
            "lane_id": strategy_id,
            "lifecycle_id": current_lifecycle_id,
            "instrument_family": "MES",
            "contract_key": "MES-202606",
            "local_symbol": "MESM6",
            "con_id": 770561194,
            "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
            "exit_profile_id": "MES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_V1",
        },
    )
    bridge_path = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
        / bridge_lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    bridge_report = _lifecycle_payload(config=config, lifecycle_id=bridge_lifecycle_id, order_id="2", price="7400.5")
    bridge_report["contract_key"] = "202606"
    bridge_report["managed_exit_policy_id"] = "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    _write_json(bridge_path, bridge_report)
    _write_json(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "exit_due": True,
                    "attention_required": False,
                    "symbol": "MES",
                    "contract_key": "MES-202606",
                    "local_symbol": "MESM6",
                    "con_id": 770561194,
                    "quantity": "1",
                    "side": "LONG",
                    "strategy_id": strategy_id,
                    "lifecycle_id": current_lifecycle_id,
                    "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
                    "lifecycle_position": {
                        "account_id": "DUM882026",
                        "instrument_family": "MES",
                        "contract_key": "MES-202606",
                        "local_symbol": "MESM6",
                        "con_id": 770561194,
                        "quantity": "1",
                        "side": "LONG",
                        "strategy_id": strategy_id,
                        "lifecycle_id": current_lifecycle_id,
                        "paper_lifecycle_report_path": str(bridge_path),
                        "lifecycle_units": [
                            {
                                "lifecycle_id": current_lifecycle_id,
                                "local_symbol": "MESM6",
                                "con_id": 770561194,
                                "quantity": "1",
                            }
                        ],
                    },
                }
            ],
        },
    )

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] != MANAGED_EXIT_BLOCKED_POSITION_MISMATCH
    assert payload["lifecycle_identity_verified"] is True
    assert payload["source_artifact_paths"]["lifecycle_report"] == str(bridge_path)
    assert payload["target_identity"]["lifecycle_id"] == current_lifecycle_id


def test_apply_uses_current_scope_identity_when_bridge_fill_report_is_selected(
    tmp_path: Path,
    monkeypatch,
) -> None:
    current_lifecycle_id = "reserved_submit_mes_globex_active_participation_long_current"
    current_trade_id = "trade_current_scope_exact_owner"
    bridge_lifecycle_id = "bridge_fill_MES|1m|2026-06-08T06:55:00Z|BUY_TO_OPEN"
    strategy_id = "mes_globex_active_participation_long"
    config = _seed(
        tmp_path,
        completed_bars=12,
        config_overrides={
            "apply": True,
            "operator_authorized_managed_exit": True,
            "strategy_id": strategy_id,
            "lane_id": strategy_id,
            "lifecycle_id": current_lifecycle_id,
            "instrument_family": "MES",
            "contract_key": "MES-202606",
            "local_symbol": "MESM6",
            "con_id": 770561194,
            "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
            "exit_profile_id": "MES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_V1",
        },
    )
    bridge_path = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
        / bridge_lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    bridge_report = _lifecycle_payload(config=config, lifecycle_id=bridge_lifecycle_id, order_id="2", price="7400.5")
    bridge_report["trade_id"] = "trade_bridge_fill_stale_identity"
    bridge_report["entry_intent"]["trade_id"] = "trade_bridge_fill_stale_identity"
    bridge_report["managed_exit_policy_id"] = "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    _write_json(bridge_path, bridge_report)
    _write_json(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "exit_due": True,
                    "attention_required": False,
                    "symbol": "MES",
                    "contract_key": "MES-202606",
                    "local_symbol": "MESM6",
                    "con_id": 770561194,
                    "quantity": "1",
                    "side": "LONG",
                    "strategy_id": strategy_id,
                    "lifecycle_id": current_lifecycle_id,
                    "trade_id": current_trade_id,
                    "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
                    "lifecycle_position": {
                        "account_id": "DUM882026",
                        "instrument_family": "MES",
                        "contract_key": "MES-202606",
                        "local_symbol": "MESM6",
                        "con_id": 770561194,
                        "quantity": "1",
                        "side": "LONG",
                        "strategy_id": strategy_id,
                        "lifecycle_id": current_lifecycle_id,
                        "trade_id": current_trade_id,
                        "paper_lifecycle_report_path": str(bridge_path),
                        "lifecycle_units": [
                            {
                                "lifecycle_id": current_lifecycle_id,
                                "trade_id": current_trade_id,
                                "local_symbol": "MESM6",
                                "con_id": 770561194,
                                "quantity": "1",
                            }
                        ],
                    },
                }
            ],
        },
    )

    class Result:
        report_json = bridge_path
        report = {
            "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
            "final_position_status": "OPEN_MANAGED",
            "submit_attempted": False,
            "broker_state_mutated": False,
        }

    seen: dict[str, Any] = {}

    def fake_maintain(**kwargs: Any) -> Result:
        seen["existing_lifecycle_report"] = kwargs["existing_lifecycle_report"]
        return Result()

    monkeypatch.setattr(attach_module, "maintain_open_track_b_strategy_managed_paper_lifecycle", fake_maintain)

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["apply_enabled"] is True
    assert seen["existing_lifecycle_report"]["lifecycle_id"] == current_lifecycle_id
    assert seen["existing_lifecycle_report"]["trade_id"] == current_trade_id
    assert seen["existing_lifecycle_report"]["entry_intent"]["lifecycle_id"] == current_lifecycle_id
    assert seen["existing_lifecycle_report"]["entry_intent"]["trade_id"] == current_trade_id


def test_apply_blocks_with_broker_unavailable_retryable_before_lifecycle_submit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _seed(
        tmp_path,
        completed_bars=12,
        config_overrides={
            "apply": True,
            "operator_authorized_managed_exit": True,
        },
    )
    _write_broker_retryable_unavailable(tmp_path, account_id=config.account_id)
    called = False

    def fake_maintain(**kwargs: Any) -> object:
        nonlocal called
        called = True
        raise AssertionError("managed lifecycle submit path should not be reached")

    monkeypatch.setattr(attach_module, "maintain_open_track_b_strategy_managed_paper_lifecycle", fake_maintain)

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert called is False
    assert payload["classification"] == "MANAGED_EXIT_BLOCKED_BROKER_UNAVAILABLE_RETRYABLE"
    assert payload["apply_boundary_classification"] == "broker_unavailable_retryable"
    assert payload["broker_availability"]["classification"] == "BROKER_UNAVAILABLE_RETRYABLE"
    assert payload["submit_attempted"] is False
    assert payload["broker_state_mutated"] is False
    assert payload["exit_authority_contract"]["decision"]["decision"] in {"ALLOWED", "DEGRADED_ALLOWED"}


def test_broker_availability_unknown_is_diagnostic_when_v11_exact_close_allowed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _seed(
        tmp_path,
        completed_bars=12,
        config_overrides={
            "apply": True,
            "operator_authorized_managed_exit": True,
        },
    )

    class Result:
        report_json = (
            tmp_path
            / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
            / config.lifecycle_id
            / "track_b_strategy_managed_paper_lifecycle_report.json"
        )
        report = {
            "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_CLOSE_SUBMITTED",
            "broker_state_mutated": True,
            "submit_attempted": True,
            "close_submit_attempt": {
                "broker_order_id": "94",
                "perm_id": "1871410001",
                "action": "SELL",
                "quantity": "1",
                "local_symbol": config.local_symbol,
                "con_id": config.con_id,
            },
        }

    def fake_broker_availability(**kwargs: Any) -> Mapping[str, Any]:
        return {
            "classification": "BROKER_AVAILABILITY_DIAGNOSTIC_UNKNOWN",
            "diagnostic": "publication artifact stale",
        }

    def fake_maintain(**kwargs: Any) -> Result:
        return Result()

    monkeypatch.setattr(attach_module, "build_broker_availability_report", fake_broker_availability)
    monkeypatch.setattr(attach_module, "maintain_open_track_b_strategy_managed_paper_lifecycle", fake_maintain)

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == "TRACK_B_STRATEGY_PAPER_CLOSE_SUBMITTED"
    assert payload["apply_enabled"] is True
    assert payload["broker_availability_blocker"] is None
    assert payload["broker_availability_diagnostic_blocker"] == "broker_availability_unknown"
    assert payload["submit_attempted"] is True
    assert payload["broker_state_mutated"] is True


def test_aggregate_lifecycle_blocker_is_diagnostic_when_v11_exact_close_allowed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _seed(
        tmp_path,
        completed_bars=12,
        config_overrides={
            "apply": True,
            "operator_authorized_managed_exit": True,
            "aggregate_lifecycle_units": (
                {
                    "lifecycle_id": "stale_unit_a",
                    "trade_id": "trade-stale-a",
                    "quantity": "1",
                    "paper_lifecycle_report_path": str(tmp_path / "missing-a.json"),
                },
                {
                    "lifecycle_id": "stale_unit_b",
                    "trade_id": "trade-stale-b",
                    "quantity": "1",
                    "paper_lifecycle_report_path": str(tmp_path / "missing-b.json"),
                },
            ),
        },
    )

    class Result:
        report_json = (
            tmp_path
            / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
            / config.lifecycle_id
            / "track_b_strategy_managed_paper_lifecycle_report.json"
        )
        report = {
            "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_CLOSE_SUBMITTED",
            "broker_state_mutated": True,
            "submit_attempted": True,
            "close_submit_attempt": {
                "broker_order_id": "95",
                "perm_id": "1871410002",
                "action": "SELL",
                "quantity": "1",
                "local_symbol": config.local_symbol,
                "con_id": config.con_id,
            },
        }

    monkeypatch.setattr(
        attach_module,
        "maintain_open_track_b_strategy_managed_paper_lifecycle",
        lambda **kwargs: Result(),
    )

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    diagnostics = payload["exit_authority_contract"]["legacy_diagnostics"]
    assert payload["classification"] == "TRACK_B_STRATEGY_PAPER_CLOSE_SUBMITTED"
    assert payload["apply_enabled"] is True
    assert payload["submit_attempted"] is True
    assert payload["broker_state_mutated"] is True
    assert diagnostics["aggregate_lifecycle_blocker"]
    assert diagnostics["aggregate_lifecycle_blocker_diagnostic"] == diagnostics["aggregate_lifecycle_blocker"]


def test_auto_selected_mgc_due_position_resolves_mgc_exit_profile_not_default_mnq(tmp_path: Path) -> None:
    current_lifecycle_id = "reserved_submit_mgc_1x_all_lanes_asia_early_short"
    strategy_id = "gc_mgc_forced_session_baseline_v2__mgc_1x_all_lanes__asia_early_short"
    _seed(
        tmp_path,
        completed_bars=3,
        config_overrides={
            "strategy_id": strategy_id,
            "lane_id": "mgc_1x_all_lanes__asia_early_short",
            "lifecycle_id": current_lifecycle_id,
            "instrument_family": "MGC",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "con_id": 712565978,
            "expiry": "20260626",
            "side": "SHORT",
            "managed_exit_policy_id": "FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1",
        },
        position_overrides={
            "symbol": "MGC",
            "quantity": "-1",
            "side": "SHORT",
        },
    )
    stale_default_config = TrackBManagedExitAttachConfig(repo_root=tmp_path, refresh_control_plane=False)
    lifecycle_path = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
        / current_lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "exit_due": True,
                    "attention_required": False,
                    "symbol": "MGC",
                    "contract_key": "MGC-202606",
                    "local_symbol": "MGCM6",
                    "con_id": 712565978,
                    "quantity": "-1",
                    "side": "SHORT",
                    "strategy_id": strategy_id,
                    "lifecycle_id": current_lifecycle_id,
                    "managed_exit_policy_id": "FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1",
                    "lifecycle_position": {
                        "account_id": "DUM882026",
                        "instrument_family": "MGC",
                        "contract_key": "MGC-202606",
                        "local_symbol": "MGCM6",
                        "con_id": 712565978,
                        "quantity": "-1",
                        "side": "SHORT",
                        "strategy_id": strategy_id,
                        "lifecycle_id": current_lifecycle_id,
                        "managed_exit_policy_id": "FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1",
                        "paper_lifecycle_report_path": str(lifecycle_path),
                    },
                }
            ],
        },
    )

    payload = build_track_b_managed_exit_attach_plan(config=stale_default_config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE
    assert payload["target_identity"]["lifecycle_id"] == current_lifecycle_id
    assert payload["managed_exit_policy_id"] == "FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1"
    assert payload["exit_profile_id"] == "MGC_FORCED_SESSION_SEGMENT_TIMEBOX_3X5M_V1"
    assert payload["close_intent_preview"]["exit_profile_id"] == "MGC_FORCED_SESSION_SEGMENT_TIMEBOX_3X5M_V1"
    assert payload["close_intent_preview"]["order_action"] == "BUY"
    assert payload["close_intent_preview"]["quantity"] == 1
    assert payload["target_identity"]["quantity"] == "1"


def test_explicit_mgc_retry_target_does_not_auto_select_due_mnq_position(tmp_path: Path) -> None:
    mgc_lifecycle_id = "reserved_submit_mgc_1x_all_lanes_asia_early_short"
    mgc_strategy_id = "gc_mgc_forced_session_baseline_v2__mgc_1x_all_lanes__asia_early_short"
    config = _seed(
        tmp_path,
        completed_bars=3,
        config_overrides={
            "strategy_id": mgc_strategy_id,
            "lane_id": "mgc_1x_all_lanes__asia_early_short",
            "lifecycle_id": mgc_lifecycle_id,
            "instrument_family": "MGC",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "con_id": 712565978,
            "expiry": "20260626",
            "side": "SHORT",
            "managed_exit_policy_id": "FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1",
        },
        position_overrides={
            "symbol": "MGC",
            "quantity": "-1",
            "side": "SHORT",
        },
    )
    lifecycle_path = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
        / mgc_lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    lifecycle = json.loads(lifecycle_path.read_text(encoding="utf-8"))
    lifecycle.update(
        {
            "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
            "final_position_status": "REVIEW_REQUIRED",
            "broker_state_mutated": False,
            "close_intent": None,
            "close_submit_attempt": None,
            "primary_blocker": "Strategy-managed PAPER lifecycle requires positive configured quantity.",
        }
    )
    _write_json(lifecycle_path, lifecycle)
    _write_json(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "exit_due": True,
                    "attention_required": False,
                    "symbol": "MNQ",
                    "contract_key": "MNQ-202606",
                    "local_symbol": "MNQM6",
                    "con_id": 770561201,
                    "quantity": "1",
                    "side": "LONG",
                    "strategy_id": "asia_london_participation_core_v1__mnq_1x_asia_london_participation__asia_london_long_v6",
                    "lifecycle_id": "reserved_submit_mnq_1x_asia_london_participation_asia_london_long_v6",
                    "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
                }
            ],
        },
    )

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE
    assert payload["selected_managed_position"] == {}
    assert payload["target_identity"]["lifecycle_id"] == mgc_lifecycle_id
    assert payload["target_identity"]["contract"] == "MGCM6"
    assert payload["exit_profile_id"] == "MGC_FORCED_SESSION_SEGMENT_TIMEBOX_3X5M_V1"
    assert payload["close_intent_preview"]["order_action"] == "BUY"


def test_auto_selected_aggregate_multiple_account_uses_broker_backed_account(tmp_path: Path) -> None:
    config = _seed(tmp_path, completed_bars=3)
    _write_exit_due_managed_position(
        tmp_path,
        config=config,
        lifecycle_account_id="MULTIPLE",
        lifecycle_unit_account_id=None,
        broker_account_id=config.account_id,
        include_broker_position_con_id=False,
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/position_truth/latest_position_truth.json",
        {
            "broker_positions": [
                {
                    "account_id": config.account_id,
                    "local_symbol": config.local_symbol,
                    "quantity": "1.0",
                    "symbol": config.instrument_family,
                }
            ],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    stale_default_config = TrackBManagedExitAttachConfig(repo_root=tmp_path, refresh_control_plane=False)

    payload = build_track_b_managed_exit_attach_plan(config=stale_default_config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE
    assert payload["target_identity"]["account_id"] == config.account_id
    assert payload["expected_post_action_evidence"]["same_account"] == config.account_id
    assert payload["position_identity_verified"] is True
    assert payload["lifecycle_identity_verified"] is True


def test_auto_selected_lifecycle_account_mismatch_is_attribution_diagnostic_when_broker_account_matches(tmp_path: Path) -> None:
    config = _seed(tmp_path, completed_bars=3)
    lifecycle_path = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
        / config.lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    lifecycle = json.loads(lifecycle_path.read_text(encoding="utf-8"))
    lifecycle["account_id"] = "OTHER_ACCOUNT"
    _write_json(lifecycle_path, lifecycle)
    _write_exit_due_managed_position(
        tmp_path,
        config=config,
        lifecycle_account_id="OTHER_ACCOUNT",
        lifecycle_unit_account_id="OTHER_ACCOUNT",
        broker_account_id=config.account_id,
    )
    stale_default_config = TrackBManagedExitAttachConfig(repo_root=tmp_path, refresh_control_plane=False)

    payload = build_track_b_managed_exit_attach_plan(config=stale_default_config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE
    assert payload["position_identity_verified"] is True
    assert payload["lifecycle_identity_verified"] is False
    assert payload["broker_state_mutated"] is False
    assert payload["exit_authority_contract"]["decision"]["decision"] in {"ALLOWED", "DEGRADED_ALLOWED"}


def test_auto_selected_missing_account_fails_closed(tmp_path: Path) -> None:
    config = _seed(tmp_path, completed_bars=3)
    _write_exit_due_managed_position(
        tmp_path,
        config=config,
        lifecycle_account_id="MULTIPLE",
        lifecycle_unit_account_id=None,
        broker_account_id=None,
    )
    stale_default_config = TrackBManagedExitAttachConfig(repo_root=tmp_path, refresh_control_plane=False)

    payload = build_track_b_managed_exit_attach_plan(config=stale_default_config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_BLOCKED_POSITION_MISMATCH
    assert payload["target_identity"]["account_id"] == ""
    assert payload["position_identity_verified"] is False
    assert payload["broker_state_mutated"] is False


def _seed(
    tmp_path: Path,
    *,
    completed_bars: int,
    position_overrides: Mapping[str, Any] | None = None,
    managed_order_overrides: Mapping[str, Any] | None = None,
    open_order_overrides: Mapping[str, Any] | None = None,
    broker_session_authority_overrides: Mapping[str, Any] | None = None,
    snapshot_overrides: Mapping[str, Any] | None = None,
    config_overrides: Mapping[str, Any] | None = None,
) -> TrackBManagedExitAttachConfig:
    config = TrackBManagedExitAttachConfig(
        repo_root=tmp_path,
        refresh_control_plane=False,
        **dict(config_overrides or {}),
    )
    lifecycle_path = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
        / config.lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    lifecycle = {
        "schema_version": "track_b_strategy_managed_paper_lifecycle_v1",
        "lifecycle_id": config.lifecycle_id,
        "strategy_id": config.strategy_id,
        "instrument_family": config.instrument_family,
        "contract_key": config.contract_key,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "account_id": config.account_id,
        "expected_account_id": config.expected_account_id,
        "mode": "PAPER",
        "managed_exit_policy_id": config.managed_exit_policy_id,
        "managed_exit_policy_max_completed_5m_bars": config.required_completed_5m_bars,
        "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
        "final_position_status": "OPEN_MANAGED",
        "entry_intent": {
            "side": "LONG",
            "quantity": 1,
            "managed_exit_policy_id": config.managed_exit_policy_id,
        },
        "entry_fill": {
            "price": "29923.75",
            "quantity": "1",
            "filled_at": "2026-05-25T07:26:12+00:00",
            "broker_order_id": "33",
        },
    }
    _write_json(lifecycle_path, lifecycle)
    position = {
        "account_id": config.account_id,
        "instrument_family": config.instrument_family,
        "contract_key": config.contract_key,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "quantity": "1",
        "side": "LONG",
        "strategy_id": config.strategy_id,
        "lifecycle_id": config.lifecycle_id,
        "entry_timestamp": "2026-05-25T07:26:12+00:00",
        "managed_exit_policy_id": config.managed_exit_policy_id,
        "paper_lifecycle_report_path": str(lifecycle_path),
    }
    position.update(dict(position_overrides or {}))
    _write_json(
        tmp_path / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json",
        {
            "schema_version": "track_b_live_position_status_v1",
            "open_position_count": 1,
            "open_order_count": 0,
            "positions_by_instrument": {config.contract_key: position},
            "source_artifact_paths": [str(lifecycle_path)],
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "CONTROL_PLANE_SNAPSHOT_BLOCKED",
            "control_plane_snapshot_id": "snapshot-1",
            "shared_truth_refresh_generation_id": "generation-1",
            "shared_truth_coherence_status": "COHERENT",
            "runtime_supervisor_classification": "SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME",
            "open_order_truth_classification": "BROKER_POSITION_WITHOUT_CLOSE_ORDER",
            "managed_order_registry_classification": "POSITION_WITHOUT_CLOSE_ORDER",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            **dict(snapshot_overrides or {}),
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/safe_state/latest_runtime_safe_state_envelope.json",
        {
            "safe_state_classification": "SAFE_STATE_NORMAL",
            "submit_allowed": False,
            "broker_mutation_allowed": True,
            "observe_only": False,
            "tripped_limits": [],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        {"classification": "BROKER_POSITION_WITHOUT_CLOSE_ORDER", **dict(open_order_overrides or {})},
    )
    position_truth_position = {
        "account_id": config.account_id,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "quantity": "1",
        "symbol": config.instrument_family,
    }
    position_truth_position.update(dict(position_overrides or {}))
    _write_json(
        tmp_path / "outputs/track_b_execution_core/position_truth/latest_position_truth.json",
        {"broker_positions": [position_truth_position], "live_money_eligible": False, "paper_proof_invoked": False},
    )
    managed_orders = {
        "classification": "POSITION_WITHOUT_CLOSE_ORDER",
        "managed_orders": [
            {
                "working": False,
                "local_symbol": config.local_symbol,
                "action": "SELL",
                "quantity": "1",
                "classification": "POSITION_WITHOUT_CLOSE_ORDER",
            }
        ],
    }
    managed_orders.update(dict(managed_order_overrides or {}))
    _write_json(tmp_path / "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json", managed_orders)
    broker_session_authority = {
        "schema_version": "track_b_broker_session_authority_v1",
        "classification": "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE",
        "connection_mode": "ORDER_STATUS_UNRELIABLE",
        "allowed_uses": {
            "new_entry": False,
            "managed_risk_reducing_close": True,
            "broker_observed_adoption_diagnosis": True,
            "fill_callback_adoption": False,
            "status_diagnostic": True,
        },
        "risk_reducing_close_connection_mode": "RISK_REDUCING_CLOSE_CAPABLE_ORDER_STATUS_DEGRADED",
        "degraded_exact_risk_reducing_close_context": {
            "ready": True,
            "broker_positions_present": True,
            "current_scope_lifecycle_positions_match_broker": True,
            "broker_open_orders_zero": True,
            "unknown_open_orders_zero": True,
            "broker_lifecycle_reconciled": True,
            "no_lifecycle_open_order": True,
        },
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "broad_flatten_allowed": False,
        "global_flatten_allowed": False,
    }
    broker_session_authority.update(dict(broker_session_authority_overrides or {}))
    _write_json(
        tmp_path / "outputs/operator_dashboard/runtime/latest_broker_session_authority.json",
        broker_session_authority,
    )
    bars = [
        {"bar_end": f"2026-05-25T07:{30 + idx * 5:02d}:00+00:00", "close": "29960"}
        for idx in range(completed_bars)
    ]
    _write_json(
        tmp_path / "outputs/track_b_execution_core/phase1_runtime_market_data" / config.instrument_family / "5m/latest_runtime_candles.json",
        {"bars": bars},
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/phase1_runtime_market_data" / config.instrument_family / "1m/latest_runtime_candles.json",
        {"bars": [{"bar_end": "2026-05-25T07:47:00+00:00", "close": "29965.5"}]},
    )
    _write_broker_available(tmp_path, account_id=config.account_id)
    return config


def _write_broker_available(tmp_path: Path, *, account_id: str = "DUM882026") -> None:
    broker_root = tmp_path / "outputs/reports/ibkr_read_only_verification"
    _write_json(
        broker_root / "ibkr_positions_snapshot.json",
        {
            "ok": True,
            "positions_complete": True,
            "generated_at": NOW.isoformat(),
            "selected_account_id": account_id,
            "positions": [],
        },
    )
    _write_json(
        broker_root / "ibkr_open_orders_snapshot.json",
        {
            "ok": True,
            "open_orders_complete": True,
            "generated_at": NOW.isoformat(),
            "selected_account_id": account_id,
            "open_orders": [],
        },
    )


def _write_broker_retryable_unavailable(tmp_path: Path, *, account_id: str = "DUM882026") -> None:
    broker_root = tmp_path / "outputs/reports/ibkr_read_only_verification"
    _write_json(
        broker_root / "ibkr_broker_truth_latest_attempt_status.json",
        {
            "classification": "BROKER_TRUTH_REFRESH_FAILED",
            "generated_at": NOW.isoformat(),
            "last_failure": True,
            "last_success": False,
            "last_error": "TWS paper API error 502: Couldn't connect to TWS.",
            "mode": "PAPER",
            "account": account_id,
        },
    )
    _write_json(
        broker_root / "ibkr_read_only_connection_report.json",
        {
            "classification": "IBKR_READ_ONLY_BLOCKED",
            "generated_at": NOW.isoformat(),
            "detail": "TWS paper API error 502: Couldn't connect to TWS.",
        },
    )


def _write_exit_due_managed_position(
    tmp_path: Path,
    *,
    config: TrackBManagedExitAttachConfig,
    lifecycle_account_id: str | None,
    lifecycle_unit_account_id: str | None,
    broker_account_id: str | None,
    include_broker_position_con_id: bool = True,
) -> None:
    lifecycle_path = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
        / config.lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    lifecycle_unit = {
        "account_id": lifecycle_unit_account_id,
        "con_id": config.con_id,
        "contract_key": config.contract_key,
        "entry_order_id": "33",
        "entry_perm_id": 2047276068,
        "entry_exec_id": "0000e1a7.6a2870c7.01.01",
        "instrument_family": config.instrument_family,
        "lane_id": config.lane_id,
        "lifecycle_id": config.lifecycle_id,
        "local_symbol": config.local_symbol,
        "managed_exit_policy_id": config.managed_exit_policy_id,
        "paper_lifecycle_report_path": str(lifecycle_path),
        "quantity": "1",
        "side": config.side,
        "strategy_id": config.strategy_id,
    }
    broker_position = {
        "average_cost": "149618.75",
        "expiry": config.expiry,
        "local_symbol": config.local_symbol,
        "quantity": "1",
        "symbol": config.instrument_family,
    }
    if include_broker_position_con_id:
        broker_position["con_id"] = config.con_id
    if broker_account_id is not None:
        broker_position["account_id"] = broker_account_id
    _write_json(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "exit_due": True,
                    "attention_required": False,
                    "symbol": config.instrument_family,
                    "contract_key": config.contract_key,
                    "local_symbol": config.local_symbol,
                    "con_id": config.con_id,
                    "quantity": "1",
                    "side": config.side,
                    "strategy_id": config.strategy_id,
                    "lane_id": config.lane_id,
                    "lifecycle_id": config.lifecycle_id,
                    "managed_exit_policy_id": config.managed_exit_policy_id,
                    "broker_position": broker_position,
                    "lifecycle_units": [lifecycle_unit],
                    "lifecycle_position": {
                        "account_id": lifecycle_account_id,
                        "instrument_family": config.instrument_family,
                        "contract_key": config.contract_key,
                        "local_symbol": config.local_symbol,
                        "con_id": config.con_id,
                        "quantity": "1",
                        "side": config.side,
                        "strategy_id": config.strategy_id,
                        "lane_id": config.lane_id,
                        "lifecycle_id": config.lifecycle_id,
                        "managed_exit_policy_id": config.managed_exit_policy_id,
                        "lifecycle_units": [lifecycle_unit],
                        "paper_lifecycle_report_path": str(lifecycle_path),
                    },
                }
            ],
        },
    )


def _lifecycle_payload(
    *,
    config: TrackBManagedExitAttachConfig,
    lifecycle_id: str,
    order_id: str,
    price: str,
) -> dict[str, Any]:
    return {
        "schema_version": "track_b_strategy_managed_paper_lifecycle_v1",
        "lifecycle_id": lifecycle_id,
        "strategy_id": config.strategy_id,
        "instrument_family": config.instrument_family,
        "contract_key": config.contract_key,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "account_id": config.account_id,
        "expected_account_id": config.expected_account_id,
        "mode": "PAPER",
        "managed_exit_policy_id": config.managed_exit_policy_id,
        "managed_exit_policy_max_completed_5m_bars": config.required_completed_5m_bars,
        "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
        "final_position_status": "OPEN_MANAGED",
        "entry_intent": {
            "side": "SHORT",
            "quantity": 1,
            "managed_exit_policy_id": config.managed_exit_policy_id,
        },
        "entry_fill": {
            "price": price,
            "quantity": "1",
            "filled_at": "2026-05-25T07:26:12+00:00",
            "broker_order_id": order_id,
        },
    }


def test_reconciled_lifecycle_identity_supplies_missing_broker_con_id_for_any_symbol(tmp_path: Path) -> None:
    _write_reconciliation(
        tmp_path,
        broker_position={
            "account_id": "DUM882026",
            "symbol": "ZC",
            "local_symbol": "ZCN6",
            "quantity": "1",
        },
        lifecycle_position={
            "account_id": "MULTIPLE",
            "track_b_root": "ZC",
            "local_symbol": "ZCN6",
            "con_id": 123456789,
            "quantity": "1",
        },
    )
    config = TrackBManagedExitAttachConfig(
        repo_root=tmp_path,
        account_id="DUM882026",
        instrument_family="ZC",
        contract_key="ZC-202607",
        local_symbol="ZCN6",
        con_id=123456789,
        quantity=1,
    )

    ok, reason = attach_module._position_identity_matches(  # noqa: SLF001
        config=config,
        position_truth={"broker_positions": []},
        live_position_status={},
    )

    assert ok is True
    assert reason == "Position identity matches."


def test_reconciled_lifecycle_identity_preserves_true_account_mismatch_block(tmp_path: Path) -> None:
    _write_reconciliation(
        tmp_path,
        broker_position={
            "account_id": "OTHER",
            "symbol": "BTC",
            "local_symbol": "BTC-PERP",
            "quantity": "1",
        },
        lifecycle_position={
            "account_id": "MULTIPLE",
            "track_b_root": "BTC",
            "local_symbol": "BTC-PERP",
            "con_id": 987654321,
            "quantity": "1",
        },
    )
    config = TrackBManagedExitAttachConfig(
        repo_root=tmp_path,
        account_id="DUM882026",
        instrument_family="BTC",
        contract_key="BTC-PERP",
        local_symbol="BTC-PERP",
        con_id=987654321,
        quantity=1,
    )

    ok, reason = attach_module._position_identity_matches(  # noqa: SLF001
        config=config,
        position_truth={"broker_positions": []},
        live_position_status={},
    )

    assert ok is False
    assert reason == "No exact active broker/lifecycle position matches account, contract, conId, and quantity."


def _write_reconciliation(
    repo_root: Path,
    *,
    broker_position: dict[str, Any],
    lifecycle_position: dict[str, Any],
) -> None:
    _write_json(
        repo_root
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json",
        {
            "track_b_broker_positions": [broker_position],
            "track_b_lifecycle_positions": [lifecycle_position],
        },
    )


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
