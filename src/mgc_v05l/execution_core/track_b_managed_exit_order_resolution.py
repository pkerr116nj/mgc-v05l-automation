"""Resolve disappeared known managed exit orders from read-only broker truth.

This module is artifact-only. It never connects to IBKR and never submits or
cancels orders. Its job is to bridge a specific managed-exit gap: a known exit
order is no longer present in broker open orders, and fresh broker truth shows
whether the associated lifecycle position is flat, still open, partially
reduced, or ambiguous.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Mapping

from .models import to_jsonable
from .track_b_paper_broker_reconciliation import PAPER_ACCOUNT, ReconciliationConfig, reconcile_track_b_paper_broker_truth
from .track_b_paper_trade_ledger import update_track_b_paper_trade_ledger_from_filled_bridge_result


KNOWN_MANAGED_EXIT_ORDER_FILLED_CLOSE_PERSISTENCE_GAP = (
    "KNOWN_MANAGED_EXIT_ORDER_FILLED_CLOSE_PERSISTENCE_GAP"
)
KNOWN_MANAGED_EXIT_ORDER_CANCELLED_CLEANLY = "KNOWN_MANAGED_EXIT_ORDER_CANCELLED_CLEANLY"
KNOWN_MANAGED_EXIT_ORDER_EXPIRED_OR_GONE_POSITION_STILL_OPEN = (
    "KNOWN_MANAGED_EXIT_ORDER_EXPIRED_OR_GONE_POSITION_STILL_OPEN"
)
KNOWN_MANAGED_EXIT_ORDER_DISAPPEARED_REVIEW_REQUIRED = (
    "KNOWN_MANAGED_EXIT_ORDER_DISAPPEARED_REVIEW_REQUIRED"
)
KNOWN_MANAGED_EXIT_ORDER_PARTIAL_FILL_REVIEW_REQUIRED = (
    "KNOWN_MANAGED_EXIT_ORDER_PARTIAL_FILL_REVIEW_REQUIRED"
)
KNOWN_MANAGED_EXIT_ORDER_STILL_WORKING = "KNOWN_MANAGED_EXIT_ORDER_STILL_WORKING"

DEFAULT_REPORT_DIR = Path("outputs") / "reports" / "track_b_managed_exit_order_resolution"
DEFAULT_KNOWN_MANAGED_EXIT_STATE_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "managed_exit_orders"
    / "latest_known_managed_exit_orders.json"
)


@dataclass(frozen=True)
class ManagedExitOrderResolutionConfig:
    repo_root: Path
    lifecycle_id: str
    broker_order_id: str
    client_id: int | None = None
    perm_id: int | None = None
    account_id: str = PAPER_ACCOUNT
    symbol: str | None = None
    local_symbol: str | None = None
    con_id: int | None = None
    apply: bool = False
    report_dir: Path = DEFAULT_REPORT_DIR
    known_managed_exit_state_path: Path = DEFAULT_KNOWN_MANAGED_EXIT_STATE_PATH
    reconciliation_report_path: Path | None = None

    @property
    def resolved_report_dir(self) -> Path:
        return self.report_dir if self.report_dir.is_absolute() else self.repo_root / self.report_dir

    @property
    def resolved_known_managed_exit_state_path(self) -> Path:
        path = self.known_managed_exit_state_path
        return path if path.is_absolute() else self.repo_root / path


def resolve_known_managed_exit_order_disappearance(
    *,
    config: ManagedExitOrderResolutionConfig,
    now: datetime | None = None,
    reconciliation_runner: Callable[[ReconciliationConfig], Mapping[str, Any]] | None = None,
    broker_truth_refresh: Callable[[], Mapping[str, Any]] | None = None,
    ledger_update_runner: Callable[..., Any] = update_track_b_paper_trade_ledger_from_filled_bridge_result,
) -> dict[str, Any]:
    """Resolve one exact known managed exit order disappearance."""

    actual_now = now or datetime.now(UTC)
    if broker_truth_refresh is not None:
        broker_truth_refresh()
    reconciliation = _run_reconciliation(config=config, reconciliation_runner=reconciliation_runner)
    target = _target_lifecycle_position(config=config, reconciliation=reconciliation)
    known_order = _known_exit_order(config=config, reconciliation=reconciliation, target=target)
    current_open_order = _current_broker_order(config=config, reconciliation=reconciliation)
    report = _base_report(
        config=config,
        now=actual_now,
        reconciliation=reconciliation,
        target=target,
        known_order=known_order,
        current_open_order=current_open_order,
    )
    if current_open_order is not None:
        report["classification"] = KNOWN_MANAGED_EXIT_ORDER_STILL_WORKING
        report["detail"] = "The requested managed exit order still appears in broker open orders."
        _write_report(config, report)
        return report
    validation_error = _validate_identity(config=config, target=target, known_order=known_order)
    if validation_error:
        report["classification"] = KNOWN_MANAGED_EXIT_ORDER_DISAPPEARED_REVIEW_REQUIRED
        report["detail"] = validation_error
        _write_report(config, report)
        return report

    broker_position = _broker_position_for_target(reconciliation=reconciliation, target=target)
    lifecycle_qty = _abs_decimal(target.get("quantity"))
    broker_qty = _abs_decimal(broker_position.get("quantity") if broker_position else "0")
    open_order_count = int(reconciliation.get("track_b_broker_open_order_count") or 0)
    unknown_order_count = int(reconciliation.get("unknown_broker_open_order_count") or 0)
    if unknown_order_count:
        report["classification"] = KNOWN_MANAGED_EXIT_ORDER_DISAPPEARED_REVIEW_REQUIRED
        report["detail"] = "Unknown broker open orders are present; disappeared managed exit cannot be inferred."
        _write_report(config, report)
        return report
    if open_order_count:
        report["classification"] = KNOWN_MANAGED_EXIT_ORDER_DISAPPEARED_REVIEW_REQUIRED
        report["detail"] = "Other broker open orders are present; disappeared managed exit cannot be inferred."
        _write_report(config, report)
        return report
    if broker_qty == Decimal("0"):
        report["classification"] = KNOWN_MANAGED_EXIT_ORDER_FILLED_CLOSE_PERSISTENCE_GAP
        report["detail"] = "Broker truth is flat for the exact lifecycle contract; lifecycle close persistence is required."
        close_payloads = _filled_close_payloads(
            config=config,
            target=target,
            known_order=known_order,
            now=actual_now,
        )
        close_payload = close_payloads[-1] if close_payloads else _filled_close_payload(
            config=config,
            target=target,
            known_order=known_order,
            now=actual_now,
        )
        report["filled_bridge_result"] = close_payload
        if len(close_payloads) > 1:
            report["aggregate_lifecycle_close_persistence"] = {
                "enabled": True,
                "lifecycle_count": len(close_payloads),
                "lifecycle_ids": [payload.get("lifecycle_id") for payload in close_payloads],
            }
        if config.apply:
            ledger_results = []
            for index, payload in enumerate(close_payloads or [close_payload], start=1):
                lifecycle_id = _safe_filename(str(payload.get("lifecycle_id") or config.lifecycle_id or index))
                suffix = "" if len(close_payloads) <= 1 else f"_{index}_{lifecycle_id}"
                filled_path = config.resolved_report_dir / f"known_managed_exit_order_close_fill_result{suffix}.json"
                _write_json(filled_path, payload)
                ledger_results.append(
                    ledger_update_runner(
                        filled_bridge_result=payload,
                        filled_bridge_result_json=filled_path,
                        output_root=config.repo_root / "outputs" / "track_b_execution_core" / "paper_trade_ledger",
                        now=actual_now,
                    )
                )
            _clear_pending_order_state(config=config, known_order=known_order, classification=report["classification"], now=actual_now)
            primary_ledger_result = ledger_results[-1] if ledger_results else None
            report["lifecycle_close"] = {
                "persisted": any(bool(getattr(item, "trade_record_written", False)) for item in ledger_results),
                "persisted_count": sum(1 for item in ledger_results if getattr(item, "trade_record_written", False)),
                "ledger_jsonl": str(getattr(primary_ledger_result, "ledger_jsonl", "")) if primary_ledger_result else "",
                "live_position_status_json": str(getattr(primary_ledger_result, "live_position_status_json", ""))
                if primary_ledger_result
                else "",
                "trade_record": getattr(primary_ledger_result, "trade_record", None) if primary_ledger_result else None,
                "trade_records": [getattr(item, "trade_record", None) for item in ledger_results],
            }
        _write_report(config, report)
        return report
    if lifecycle_qty is not None and broker_qty is not None and broker_qty < lifecycle_qty:
        report["classification"] = KNOWN_MANAGED_EXIT_ORDER_PARTIAL_FILL_REVIEW_REQUIRED
        report["detail"] = "Broker position was reduced but not flat; partial managed-exit handling requires review."
        _write_report(config, report)
        return report
    report["classification"] = KNOWN_MANAGED_EXIT_ORDER_EXPIRED_OR_GONE_POSITION_STILL_OPEN
    report["detail"] = "Known managed exit order disappeared, but broker position remains open."
    if config.apply:
        _clear_pending_order_state(config=config, known_order=known_order, classification=report["classification"], now=actual_now)
    _write_report(config, report)
    return report


def _run_reconciliation(
    *,
    config: ManagedExitOrderResolutionConfig,
    reconciliation_runner: Callable[[ReconciliationConfig], Mapping[str, Any]] | None,
) -> Mapping[str, Any]:
    report_path = config.reconciliation_report_path or (
        config.repo_root
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )
    recon_config = ReconciliationConfig(repo_root=config.repo_root, report_path=report_path)
    runner = reconciliation_runner or (lambda cfg: reconcile_track_b_paper_broker_truth(config=cfg))
    return runner(recon_config)


def _target_lifecycle_position(
    *,
    config: ManagedExitOrderResolutionConfig,
    reconciliation: Mapping[str, Any],
) -> dict[str, Any] | None:
    for row in reconciliation.get("track_b_lifecycle_positions") or []:
        if isinstance(row, Mapping) and str(row.get("lifecycle_id") or "") == config.lifecycle_id:
            return dict(row)
    for row in (reconciliation.get("position_match_report") or {}).get("unmatched_lifecycle_positions") or []:
        if isinstance(row, Mapping) and str(row.get("lifecycle_id") or "") == config.lifecycle_id:
            return dict(row)
    return None


def _known_exit_order(
    *,
    config: ManagedExitOrderResolutionConfig,
    reconciliation: Mapping[str, Any],
    target: Mapping[str, Any] | None,
) -> dict[str, Any]:
    for row in reconciliation.get("known_managed_exit_orders") or []:
        if isinstance(row, Mapping) and str(row.get("broker_order_id") or "") == str(config.broker_order_id):
            return dict(row)
    persisted = _load_json(config.resolved_known_managed_exit_state_path)
    for row in persisted.get("known_managed_exit_orders") or []:
        if isinstance(row, Mapping) and str(row.get("broker_order_id") or "") == str(config.broker_order_id):
            return dict(row)
    attach_order = _managed_exit_attach_known_order(config=config, target=target)
    if attach_order:
        return attach_order
    restore = _restore_known_order(config=config, target=target)
    if restore:
        return restore
    return {
        "source": "OPERATOR_SUPPLIED_MANAGED_EXIT_IDENTITY",
        "lifecycle_id": config.lifecycle_id,
        "broker_order_id": str(config.broker_order_id),
        "client_id": config.client_id,
        "perm_id": config.perm_id,
        "symbol": config.symbol or (target or {}).get("track_b_root") or (target or {}).get("instrument_family"),
        "local_symbol": config.local_symbol or (target or {}).get("local_symbol"),
        "con_id": config.con_id if config.con_id is not None else (target or {}).get("con_id"),
        "quantity": (target or {}).get("quantity") or "1",
        "action": "SELL" if str((target or {}).get("side") or "").upper() == "LONG" else "BUY",
        "order_intent_id": None,
        "strategy_id": (target or {}).get("strategy_id"),
        "lane_id": (target or {}).get("lane_id"),
    }


def _managed_exit_attach_known_order(
    *,
    config: ManagedExitOrderResolutionConfig,
    target: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    path = (
        config.repo_root
        / "outputs"
        / "track_b_execution_core"
        / "managed_exit_attach"
        / "latest_managed_exit_attach_plan.json"
    )
    payload = _load_json(path)
    if not payload:
        return None
    apply_result = payload.get("apply_result")
    close_intent = apply_result.get("close_intent") if isinstance(apply_result, Mapping) else {}
    if not isinstance(close_intent, Mapping):
        close_intent = {}
    preview = payload.get("close_intent_preview")
    if not isinstance(preview, Mapping):
        preview = {}
    payload_lifecycle_id = str(
        payload.get("lifecycle_id") or close_intent.get("lifecycle_id") or preview.get("lifecycle_id") or ""
    )
    aggregate_group = payload.get("aggregate_exit_group") if isinstance(payload.get("aggregate_exit_group"), Mapping) else {}
    aggregate_lifecycle_ids = {str(item) for item in aggregate_group.get("lifecycle_ids") or []}
    if payload_lifecycle_id != config.lifecycle_id and config.lifecycle_id not in aggregate_lifecycle_ids:
        return None
    close_submit = apply_result
    if isinstance(apply_result, Mapping):
        close_submit = apply_result.get("close_submit_attempt")
    if not isinstance(close_submit, Mapping):
        return None
    if str(close_submit.get("broker_order_id") or "") != str(config.broker_order_id):
        return None
    if not _managed_exit_attach_payload_matches_target(
        config=config,
        target=target,
        payload=payload,
        preview={**dict(close_intent), **dict(preview)},
    ):
        return None
    return {
        "source": "TRACK_B_MANAGED_EXIT_ATTACH_APPLY_ARTIFACT",
        "source_artifact_path": str(path),
        "lifecycle_id": config.lifecycle_id,
        "strategy_id": payload.get("strategy_id") or close_intent.get("strategy_id") or preview.get("strategy_id") or (target or {}).get("strategy_id"),
        "lane_id": payload.get("lane_id") or close_intent.get("lane_id") or preview.get("lane_id") or (target or {}).get("lane_id"),
        "order_intent_id": close_submit.get("submit_attempt_id")
        or close_intent.get("order_intent_id")
        or preview.get("order_intent_id")
        or f"{preview.get('symbol') or (target or {}).get('track_b_root') or config.symbol}|managed_exit_attach|{config.broker_order_id}",
        "broker_order_id": str(close_submit.get("broker_order_id") or config.broker_order_id),
        "client_id": config.client_id or close_submit.get("client_id"),
        "perm_id": config.perm_id or close_submit.get("perm_id"),
        "symbol": config.symbol or payload.get("symbol") or close_intent.get("symbol") or (target or {}).get("track_b_root"),
        "local_symbol": config.local_symbol or payload.get("local_symbol") or close_intent.get("local_symbol") or preview.get("local_symbol") or (target or {}).get("local_symbol"),
        "con_id": config.con_id if config.con_id is not None else payload.get("con_id") or close_intent.get("con_id") or preview.get("con_id") or (target or {}).get("con_id"),
        "action": close_intent.get("order_action") or preview.get("order_action") or ("SELL" if str((target or {}).get("side") or "").upper() == "LONG" else "BUY"),
        "quantity": close_intent.get("quantity") or preview.get("quantity") or (target or {}).get("quantity") or "1",
        "order_type": close_intent.get("order_type") or preview.get("order_type"),
        "limit_price": close_intent.get("close_limit_price") or preview.get("close_limit_price"),
        "exit_reason": close_intent.get("close_reason") or preview.get("close_reason"),
        "exit_profile_id": preview.get("exit_profile_id"),
        "fill_price": close_submit.get("fill_price"),
        "fill_timestamp": close_submit.get("fill_timestamp"),
        "review_required": close_submit.get("review_required"),
        "primary_blocker": close_submit.get("primary_blocker"),
    }


def _managed_exit_attach_payload_matches_target(
    *,
    config: ManagedExitOrderResolutionConfig,
    target: Mapping[str, Any] | None,
    payload: Mapping[str, Any],
    preview: Mapping[str, Any],
) -> bool:
    target_local = str(config.local_symbol or (target or {}).get("local_symbol") or "").upper()
    payload_local = str(payload.get("local_symbol") or preview.get("local_symbol") or "").upper()
    if target_local and payload_local and target_local != payload_local:
        return False
    target_con_id = _int_or_none(config.con_id if config.con_id is not None else (target or {}).get("con_id"))
    payload_con_id = _int_or_none(payload.get("con_id") or preview.get("con_id"))
    if target_con_id is not None and payload_con_id is not None and target_con_id != payload_con_id:
        return False
    target_symbol = str(config.symbol or (target or {}).get("track_b_root") or (target or {}).get("instrument_family") or "").upper()
    payload_symbol = str(payload.get("symbol") or preview.get("symbol") or "").upper()
    if target_symbol and payload_symbol and target_symbol != payload_symbol:
        return False
    return True


def _restore_known_order(
    *,
    config: ManagedExitOrderResolutionConfig,
    target: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    paths = _restore_paths(config=config, target=target)
    for path in paths:
        payload = _load_json(path)
        pre = payload.get("pre_restore_state_summary") if isinstance(payload.get("pre_restore_state_summary"), Mapping) else {}
        restored = payload.get("restored_state_summary") if isinstance(payload.get("restored_state_summary"), Mapping) else {}
        intent = pre.get("latest_order_intent") if isinstance(pre.get("latest_order_intent"), Mapping) else {}
        if str(intent.get("broker_order_id") or "") != str(config.broker_order_id):
            continue
        if not _restore_payload_matches_target(config=config, target=target, payload=payload, intent=intent):
            continue
        lane_id = str(payload.get("lane_id") or intent.get("lane_id") or (target or {}).get("lane_id") or "")
        return {
            "source": "TRACK_B_RUNTIME_RESTORE_VALIDATION",
            "source_artifact_path": str(path),
            "lifecycle_id": config.lifecycle_id,
            "strategy_id": (target or {}).get("strategy_id") or intent.get("standalone_strategy_id"),
            "lane_id": lane_id or None,
            "order_intent_id": intent.get("order_intent_id"),
            "broker_order_id": str(intent.get("broker_order_id") or config.broker_order_id),
            "client_id": config.client_id,
            "perm_id": config.perm_id,
            "symbol": config.symbol or intent.get("instrument") or payload.get("instrument") or (target or {}).get("track_b_root"),
            "local_symbol": config.local_symbol or (target or {}).get("local_symbol"),
            "con_id": config.con_id if config.con_id is not None else (target or {}).get("con_id"),
            "action": "SELL" if str((target or {}).get("side") or "").upper() == "LONG" else "BUY",
            "quantity": intent.get("quantity") or (target or {}).get("quantity") or "1",
            "exit_reason": intent.get("reason_code"),
            "submitted_at": intent.get("submitted_at"),
            "acknowledged_at": intent.get("acknowledged_at"),
            "fill_timestamp": restored.get("latest_fill_timestamp"),
            "fill_price": _latest_fill_price(config=config, target={**dict(target or {}), "lane_id": lane_id}),
        }
    return None


def _restore_paths(
    *,
    config: ManagedExitOrderResolutionConfig,
    target: Mapping[str, Any] | None,
) -> list[Path]:
    lanes_root = config.repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes"
    lane_id = str((target or {}).get("lane_id") or "")
    if lane_id:
        return [lanes_root / lane_id / "restore_validation_latest.json"]
    try:
        return sorted(lanes_root.glob("*/restore_validation_latest.json"))
    except OSError:
        return []


def _restore_payload_matches_target(
    *,
    config: ManagedExitOrderResolutionConfig,
    target: Mapping[str, Any] | None,
    payload: Mapping[str, Any],
    intent: Mapping[str, Any],
) -> bool:
    symbol = str(config.symbol or (target or {}).get("track_b_root") or (target or {}).get("instrument_family") or "").upper()
    payload_symbol = str(payload.get("instrument") or intent.get("instrument") or intent.get("symbol") or "").upper()
    if symbol and payload_symbol and symbol != payload_symbol:
        return False
    entry_intent_id = str(((payload.get("pre_restore_state_summary") or {}).get("latest_fill") or {}).get("order_intent_id") or "")
    if entry_intent_id and config.lifecycle_id != f"bridge_fill_{entry_intent_id}":
        return False
    strategy = str((target or {}).get("strategy_id") or "")
    restored_strategy = str(intent.get("standalone_strategy_id") or "")
    if strategy and restored_strategy and strategy != restored_strategy:
        return False
    return True


def _latest_fill_price(
    *,
    config: ManagedExitOrderResolutionConfig,
    target: Mapping[str, Any] | None,
) -> Any:
    fills = _load_json(config.repo_root / "outputs" / "operator_dashboard" / "paper_latest_fills_snapshot.json")
    lane_id = str((target or {}).get("lane_id") or "")
    for row in fills.get("rows") or []:
        if not isinstance(row, Mapping):
            continue
        if str(row.get("broker_order_id") or "") != str(config.broker_order_id):
            continue
        if str(row.get("intent_type") or "").upper() not in {"SELL_TO_CLOSE", "BUY_TO_CLOSE"}:
            continue
        if lane_id and str(row.get("lane_id") or "") != lane_id:
            continue
        return row.get("fill_price")
    return None


def _current_broker_order(
    *,
    config: ManagedExitOrderResolutionConfig,
    reconciliation: Mapping[str, Any],
) -> dict[str, Any] | None:
    for row in reconciliation.get("track_b_broker_open_orders") or []:
        if isinstance(row, Mapping) and str(row.get("broker_order_id") or row.get("order_id") or "") == str(config.broker_order_id):
            return dict(row)
    return None


def _validate_identity(
    *,
    config: ManagedExitOrderResolutionConfig,
    target: Mapping[str, Any] | None,
    known_order: Mapping[str, Any],
) -> str | None:
    if target is None:
        return f"Lifecycle id {config.lifecycle_id} was not found in Track B lifecycle positions."
    if (
        str(known_order.get("source") or "") == "OPERATOR_SUPPLIED_MANAGED_EXIT_IDENTITY"
        and (known_order.get("client_id") in {None, ""} or known_order.get("perm_id") in {None, ""})
    ):
        return "Known managed exit order metadata is incomplete; refusing to infer lifecycle close."
    checks = [
        ("symbol", config.symbol, target.get("track_b_root") or target.get("instrument_family")),
        ("local_symbol", config.local_symbol, target.get("local_symbol")),
        ("con_id", config.con_id, target.get("con_id")),
        ("broker_order_id", config.broker_order_id, known_order.get("broker_order_id")),
        ("client_id", config.client_id, known_order.get("client_id")),
        ("perm_id", config.perm_id, known_order.get("perm_id")),
    ]
    for name, expected, actual in checks:
        if expected in {None, ""}:
            continue
        if name == "con_id":
            if _int_or_none(expected) != _int_or_none(actual):
                return f"{name} mismatch: expected {expected}, saw {actual}."
        elif str(expected).strip().upper() != str(actual or "").strip().upper():
            return f"{name} mismatch: expected {expected}, saw {actual}."
    order_lifecycle_id = str(known_order.get("lifecycle_id") or "")
    if order_lifecycle_id and order_lifecycle_id != config.lifecycle_id:
        return f"lifecycle_id mismatch: expected {config.lifecycle_id}, saw {order_lifecycle_id}."
    return None


def _broker_position_for_target(
    *,
    reconciliation: Mapping[str, Any],
    target: Mapping[str, Any],
) -> dict[str, Any] | None:
    target_local = str(target.get("local_symbol") or "").upper()
    target_con_id = _int_or_none(target.get("con_id"))
    target_root = str(target.get("track_b_root") or target.get("instrument_family") or "").upper()
    for row in reconciliation.get("track_b_broker_positions") or []:
        if not isinstance(row, Mapping):
            continue
        local = str(row.get("local_symbol") or "").upper()
        con_id = _int_or_none(row.get("con_id"))
        root = str(row.get("track_b_root") or row.get("symbol") or "").upper()
        if target_con_id is not None and con_id is not None and con_id != target_con_id:
            continue
        if target_local and local and local != target_local:
            continue
        if target_root and root and root != target_root:
            continue
        return dict(row)
    return None


def _filled_close_payload(
    *,
    config: ManagedExitOrderResolutionConfig,
    target: Mapping[str, Any],
    known_order: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    side = str(target.get("side") or "").upper()
    action = str(known_order.get("action") or ("SELL" if side == "LONG" else "BUY")).upper()
    symbol = str(known_order.get("symbol") or target.get("track_b_root") or target.get("instrument_family") or "").upper()
    expiry = _expiry_from_contract_key(target.get("contract_key"))
    lane_id = target.get("lane_id") or known_order.get("lane_id")
    fill_price = known_order.get("fill_price")
    fill_timestamp = known_order.get("fill_timestamp") or now.isoformat()
    lifecycle_id = str(target.get("lifecycle_id") or config.lifecycle_id)
    return {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "bridge_classification": KNOWN_MANAGED_EXIT_ORDER_FILLED_CLOSE_PERSISTENCE_GAP,
        "intent_type": "SELL_TO_CLOSE" if action == "SELL" else "BUY_TO_CLOSE",
        "action": action,
        "instrument": symbol,
        "symbol": symbol,
        "strategy_id": target.get("strategy_id"),
        "lane_id": lane_id,
        "lifecycle_id": lifecycle_id,
        "order_intent_id": known_order.get("order_intent_id") or f"{symbol}|managed_exit_disappeared|{config.broker_order_id}",
        "account_id": target.get("account_id"),
        "broker_account_id": config.account_id,
        "quantity": _positive_quantity(target.get("quantity") or known_order.get("quantity") or "1"),
        "fill_price": fill_price,
        "fill_price_source": "UNKNOWN_BROKER_POSITION_FLAT" if fill_price in {None, ""} else "BROKER_FILL_EVIDENCE",
        "realized_pnl_unknown": fill_price in {None, ""},
        "fill_timestamp": fill_timestamp,
        "broker_order_id": str(config.broker_order_id),
        "perm_id": config.perm_id if config.perm_id is not None else known_order.get("perm_id"),
        "client_id": config.client_id if config.client_id is not None else known_order.get("client_id"),
        "exec_id": known_order.get("exec_id") or known_order.get("execution_id"),
        "execution_id": known_order.get("exec_id") or known_order.get("execution_id"),
        "local_symbol": target.get("local_symbol") or known_order.get("local_symbol"),
        "con_id": target.get("con_id") or known_order.get("con_id"),
        "exit_order_type": known_order.get("order_type"),
        "exit_limit_price": known_order.get("limit_price"),
        "exit_stop_price": known_order.get("stop_price"),
        "exit_tif": known_order.get("tif"),
        "exit_reason": known_order.get("exit_reason"),
        "exit_urgency": known_order.get("exit_urgency"),
        "hard_exit": known_order.get("hard_exit"),
        "source_bridge_report_path": known_order.get("bridge_report_path"),
        "contract": {
            "symbol": symbol,
            "local_symbol": target.get("local_symbol") or known_order.get("local_symbol"),
            "expiry": expiry,
            "qualified_contract_identifier": target.get("con_id") or known_order.get("con_id"),
        },
        "source": "KNOWN_MANAGED_EXIT_ORDER_DISAPPEARANCE_RESOLVER",
        "source_artifact_path": known_order.get("source_artifact_path"),
        "paper_proof_invoked": False,
        "live_money_readiness": False,
        "review_required": False,
        "created_at": now.isoformat(),
    }


def _filled_close_payloads(
    *,
    config: ManagedExitOrderResolutionConfig,
    target: Mapping[str, Any],
    known_order: Mapping[str, Any],
    now: datetime,
) -> list[dict[str, Any]]:
    units = [dict(item) for item in target.get("lifecycle_units") or [] if isinstance(item, Mapping)]
    if not units:
        return [_filled_close_payload(config=config, target=target, known_order=known_order, now=now)]
    aggregate_lifecycle_ids = {
        str(item.get("lifecycle_id") or "")
        for item in units
        if str(item.get("lifecycle_id") or "").strip()
    }
    payloads: list[dict[str, Any]] = []
    for unit in units:
        unit_target = {
            **dict(target),
            **unit,
            "lifecycle_id": unit.get("lifecycle_id") or unit.get("entry_intent_id") or target.get("lifecycle_id"),
            "quantity": unit.get("quantity") or "1",
            "side": unit.get("side") or target.get("side"),
            "strategy_id": unit.get("strategy_id") or target.get("strategy_id"),
            "lane_id": unit.get("lane_id") or target.get("lane_id"),
            "account_id": unit.get("account_id") or target.get("account_id"),
            "local_symbol": unit.get("local_symbol") or target.get("local_symbol"),
            "con_id": unit.get("con_id") or target.get("con_id"),
            "contract_key": unit.get("contract_key") or target.get("contract_key"),
        }
        payload = _filled_close_payload(config=config, target=unit_target, known_order=known_order, now=now)
        payload["aggregate_lifecycle_close"] = {
            "enabled": True,
            "source_lifecycle_id": config.lifecycle_id,
            "aggregate_lifecycle_ids": sorted(aggregate_lifecycle_ids),
            "aggregate_order_quantity": str(known_order.get("quantity") or target.get("quantity") or len(units)),
            "unit_count": len(units),
        }
        payloads.append(payload)
    return payloads


def _clear_pending_order_state(
    *,
    config: ManagedExitOrderResolutionConfig,
    known_order: Mapping[str, Any],
    classification: str,
    now: datetime,
) -> None:
    path = config.resolved_known_managed_exit_state_path
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "track_b_known_managed_exit_orders_v1",
        "generated_at": now.isoformat(),
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "known_managed_exit_orders": [],
        "resolved_known_managed_exit_orders": [
            {
                **dict(known_order),
                "managed_order_status": classification,
                "resolved_at": now.isoformat(),
            }
        ],
    }
    _write_json(path, payload)


def _base_report(
    *,
    config: ManagedExitOrderResolutionConfig,
    now: datetime,
    reconciliation: Mapping[str, Any],
    target: Mapping[str, Any] | None,
    known_order: Mapping[str, Any],
    current_open_order: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return {
        "schema_version": "track_b_managed_exit_order_resolution_v1",
        "generated_at": now.isoformat(),
        "classification": KNOWN_MANAGED_EXIT_ORDER_DISAPPEARED_REVIEW_REQUIRED,
        "detail": None,
        "apply": bool(config.apply),
        "broker_mutation_attempted": False,
        "broker_mutation_performed": False,
        "forbidden_routes": ["broad_cancel", "reqGlobalCancel", "paper_proof", "live_money"],
        "account_id": config.account_id,
        "requested_identity": {
            "lifecycle_id": config.lifecycle_id,
            "broker_order_id": config.broker_order_id,
            "client_id": config.client_id,
            "perm_id": config.perm_id,
            "symbol": config.symbol,
            "local_symbol": config.local_symbol,
            "con_id": config.con_id,
        },
        "target_lifecycle_position": _jsonable(target),
        "known_exit_order": _jsonable(known_order),
        "current_broker_open_order": _jsonable(current_open_order),
        "reconciliation_summary": {
            "classification": reconciliation.get("classification"),
            "broker_reconciled": reconciliation.get("broker_reconciled"),
            "broker_position_count": reconciliation.get("track_b_broker_position_count"),
            "lifecycle_position_count": reconciliation.get("lifecycle_open_position_count"),
            "open_order_count": reconciliation.get("track_b_broker_open_order_count"),
            "unknown_open_order_count": reconciliation.get("unknown_broker_open_order_count"),
            "review_required_count": reconciliation.get("review_required_count"),
            "live_money_eligible": reconciliation.get("live_money_eligible"),
            "paper_proof_invoked": reconciliation.get("paper_proof_invoked"),
        },
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }


def _expiry_from_contract_key(value: Any) -> str | None:
    text = str(value or "")
    if "-" not in text:
        return None
    return text.split("-", 1)[1]


def _abs_decimal(value: Any) -> Decimal | None:
    try:
        return abs(Decimal(str(value)))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _positive_quantity(value: Any) -> str:
    parsed = _abs_decimal(value)
    if parsed is None:
        return str(value or "1")
    return str(parsed.normalize())


def _int_or_none(value: Any) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _jsonable(value: Any) -> Any:
    return to_jsonable(value)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _report_path(config: ManagedExitOrderResolutionConfig) -> Path:
    return config.resolved_report_dir / "track_b_managed_exit_order_resolution_report.json"


def _write_report(config: ManagedExitOrderResolutionConfig, report: Mapping[str, Any]) -> None:
    _write_json(_report_path(config), report)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(to_jsonable(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _safe_filename(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value)[:180] or "lifecycle"
