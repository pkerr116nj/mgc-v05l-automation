"""Manual operator IBKR paper strategy-intent bridge."""

from __future__ import annotations

import inspect
import hashlib
import json
import os
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from ..app.shared_strategy_identities import get_shared_strategy_identity
from ..brokers.ibkr import IbkrClient, IbkrSession, build_default_ibkr_order_id_policy
from ..domain.enums import OrderIntentType
from .ibkr_manual_paper_submit import (
    _SEVERE_CONNECTION_ERROR_CODES,
    IbkrManualPaperSubmitCollector,
    IbkrManualPaperSubmitConfig,
    IbkrManualPaperSubmitTransport,
    _build_callback_timeline,
    _exact_contract_position_quantity,
    artifact_stem_for_test_mode,
    frozen_preview_path_for_config,
    _probe_delayed_quote_context,
    _qualify_futures_contract,
    _refresh_open_orders_snapshot,
    _refresh_positions_snapshot,
    run_ibkr_manual_paper_submit_test,
    write_ibkr_manual_paper_submit_artifacts,
)
from .ibkr_phase1_futures_scope import (
    phase1_execution_target_for_source,
    phase1_execution_target_for_symbol,
)
from .ibkr_paper_order_preview import (
    _FORBIDDEN_CALLER_PREFIXES,
    _FORBIDDEN_CALLER_SUBSTRINGS,
    evaluate_paper_preview_environment_lock,
)
from .ibkr_paper_strategy_exposure import evaluate_paper_strategy_exposure_gate
from .ibkr_paper_strategy_governance import load_paper_strategy_governance_status
from .ibkr_paper_strategy_monitor import load_paper_strategy_monitor_status
from .ibkr_paper_strategy_porting import lane_submit_bridge_adapter
from .ibkr_position_reconciliation import (
    _collect_account_truth,
    _collect_exact_contract_context,
    _collect_managed_account_context,
)
from .ibkr_read_only_verifier import _wait_for_connection_ready, IbkrReadOnlyApiTransportConfig
from .track_b_phase1_submit_authority import evaluate_phase1_broker_reconciliation_submit_gate
from ..execution_core.track_b_exit_safety import (
    ExitAttemptPolicy,
    classify_exit_attempt_policy,
    classify_exit_urgency,
)
from ..execution_core.track_b_paper_broker_reconciliation import (
    ReconciliationConfig,
    reconcile_track_b_paper_broker_truth,
)

_EXPECTED_MODE = "PAPER"
_EXPECTED_HOST = "127.0.0.1"
_EXPECTED_PORT = 7497
_EXPECTED_ACCOUNT_ID = "DUM882026"
_EXPECTED_SYMBOL = "MGC"
_EXPECTED_CONTRACT_MONTH = "202606"
_EXPECTED_EXACT_EXPIRY = "20260626"
_EXPECTED_CON_ID = 712565978
_EXPECTED_LOCAL_SYMBOL = "MGCM6"
_EXPECTED_EXCHANGE = "COMEX"
_EXPECTED_CURRENCY = "USD"
_EXPECTED_MULTIPLIER = "10"
_EXPECTED_ORDER_TYPE = "LMT"
_EXPECTED_TIF = "DAY"
_EXPECTED_QUANTITY = 1.0
_DEFAULT_TIMEOUT_SECONDS = 15.0
_DEFAULT_KILL_SWITCH_PATH = Path("var") / "ibkr_paper_strategy_bridge.disabled"
_SCHEMA_PATH = Path("strategy_order_intent_schema.json")
_MANUAL_HARNESS_CLIENT_ID_OFFSET = 1000
_SUPPORTED_STRATEGY_IDS = {
    "ATP_COMPANION_V1_ASIA_US",
    "ATP_COMPANION_V1_GC_ASIA_US",
    "ATP_COMPANION_V1_GC_ASIA_US_PRODUCTION_TRACK",
    "gc_1x_asia_london_participation__asia_london_long_v5",
}
_ALLOWED_LIMIT_PRICE_MODELS = {
    "DELAYED_ASK_PLUS_1T_MARKETABLE_BUY",
    "DELAYED_BID_MINUS_1T_MARKETABLE_SELL",
    "DELAYED_BID_MINUS_1T_RESTING_BUY",
}
_ENTRY_MARKETABLE_LIMIT_MODELS = {
    "DELAYED_ASK_PLUS_1T_MARKETABLE_BUY",
    "DELAYED_BID_MINUS_1T_MARKETABLE_SELL",
}
_ENTRY_POLICY_PASSIVE_LIMIT = "PASSIVE_LIMIT"
_ENTRY_POLICY_MARKETABLE_RUNTIME = "MARKETABLE_LIMIT_FROM_RUNTIME_TAPE"
_ENTRY_POLICY_AGGRESSIVE_WITH_CAP = "AGGRESSIVE_ENTRY_LIMIT_WITH_CAP"
_ENTRY_POLICY_BLOCK_IF_ONLY_DELAYED = "BLOCK_IF_ONLY_DELAYED_QUOTE"
_ENTRY_INTENT_PARTICIPATE_NOW = "PARTICIPATE_NOW"
_ENTRY_INTENT_RESTING_PULLBACK_LIMIT = "RESTING_PULLBACK_LIMIT"
_ENTRY_INTENT_DYNAMIC_LIMIT_WITH_CHASE_CAP = "DYNAMIC_LIMIT_WITH_CHASE_CAP"
_ENTRY_INTENT_PASSIVE_ONLY = "PASSIVE_ONLY"
_ENTRY_EXECUTION_INTENTS = {
    _ENTRY_INTENT_PARTICIPATE_NOW,
    _ENTRY_INTENT_RESTING_PULLBACK_LIMIT,
    _ENTRY_INTENT_DYNAMIC_LIMIT_WITH_CHASE_CAP,
    _ENTRY_INTENT_PASSIVE_ONLY,
}
_ENTRY_RUNTIME_PRICE_SOURCE = "RUNTIME_DATABENTO_1M_CLOSE"
_ENTRY_DELAYED_DIAGNOSTIC_SOURCE = "IBKR_DELAYED_DIAGNOSTIC_ONLY"
_ENTRY_STRATEGY_DEFINED_LIMIT_SOURCE = "STRATEGY_DEFINED_ENTRY_LIMIT"
_ENTRY_RESTING_RUNTIME_PULLBACK_SOURCE = "RUNTIME_DATABENTO_1M_PULLBACK_LIMIT"
_ENTRY_RUNTIME_CANDLE_MAX_AGE_SECONDS = 180.0
_ENTRY_RUNTIME_LIMIT_OFFSET_TICKS = 1.0
_ENTRY_AGGRESSIVE_LIMIT_OFFSET_TICKS = 2.0
_ENTRY_MAX_LIMIT_OFFSET_TICKS = 4.0
_ENTRY_PARTICIPATE_TIMEOUT_SECONDS = 60.0
_ENTRY_DYNAMIC_TIMEOUT_SECONDS = 180.0
_ENTRY_RESTING_TIMEOUT_SECONDS = 300.0
_ENTRY_PASSIVE_TIMEOUT_SECONDS = 300.0
_ENTRY_RUNTIME_CANDLE_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "phase1_runtime_market_data"
    / "{symbol}"
    / "1m"
    / "latest_runtime_candles.json"
)
_SCHEMA_ACTIONS = {"BUY", "SELL", "HOLD", "EXIT", "NO_ACTION"}
_ARTIFACT_STEM = "ibkr_paper_strategy_bridge"
_KNOWN_MANAGED_EXIT_STATE_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "managed_exit_orders"
    / "latest_known_managed_exit_orders.json"
)
_APPROVED_RUNTIME_CALLER_PATHS = {
    "probationary_paper_runtime_lane",
    "supervised_paper_runtime_bridge",
    "ibkr_paper_strategy_executor",
}
_LEAK_TEST_CALLER_PATH = "track_b_paper_leak_test_apply"
_LEAK_TEST_AUTHORIZATION_ARTIFACT_TYPE = "TRACK_B_PAPER_LEAK_TEST_AUTHORIZATION"
_LEAK_TEST_AUTHORIZATION_DIGEST_FIELDS = (
    "artifact_type",
    "account_id",
    "mode",
    "lane_id",
    "strategy_id",
    "symbol",
    "local_symbol",
    "expiry",
    "con_id",
    "action",
    "exit_action",
    "qty",
    "repo_root",
    "git_head",
    "created_at",
    "expires_at",
    "safety_snapshot",
)
_APPROVED_CALLER_PATHS = {"manual_strategy_bridge_cli", _LEAK_TEST_CALLER_PATH, *_APPROVED_RUNTIME_CALLER_PATHS}
_APPROVED_RUNTIME_CALLER_TYPES = {
    "supervised_paper_runtime",
    "supervised_paper_executor",
}
_APPROVED_RUNTIME_CALLER_MODULE_PREFIXES = (
    "mgc_v05l.app.probationary_runtime",
    "mgc_v05l.app.headless_supervised_paper",
)
_APPROVED_RUNTIME_STRATEGY_ENGINE_MODULE_PREFIXES = (
    "mgc_v05l.strategy.strategy_engine",
)
_BRIDGE_ADDITIONAL_FORBIDDEN_CALLER_PREFIXES = (
    "mgc_v05l.live",
    "mgc_v05l.execution.live_strategy_broker",
)
_DEPRECATED_SUBMIT_ROOT_FRAGMENTS = (
    "/Users/patrick/Documents/MGC-v05l-automation",
    "/Users/patrick/Documents/",
    "/Mobile Documents/",
    "/iCloud",
)


class IbkrPaperStrategyBridgeError(RuntimeError):
    """Raised when the IBKR paper strategy bridge fails closed."""


@dataclass(frozen=True)
class IbkrPaperStrategyOrderIntent:
    strategy_id: str
    symbol: str
    contract_month: str
    action: str
    quantity: float
    order_type: str
    limit_price_model: str
    time_in_force: str
    reason: str
    timestamp: str
    risk_tags: tuple[str, ...]
    paper_only: bool
    intent_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "intent_id": self.intent_id or self.default_intent_id,
            "strategy_id": self.strategy_id,
            "symbol": self.symbol,
            "contract_month": self.contract_month,
            "action": self.action,
            "quantity": self.quantity,
            "order_type": self.order_type,
            "limit_price_model": self.limit_price_model,
            "time_in_force": self.time_in_force,
            "reason": self.reason,
            "timestamp": self.timestamp,
            "risk_tags": list(self.risk_tags),
            "paper_only": self.paper_only,
        }

    @property
    def default_intent_id(self) -> str:
        return f"{self.strategy_id}|{self.symbol}|{self.action}|{self.timestamp}"


@dataclass(frozen=True)
class IbkrPaperStrategyBridgeConfig:
    repo_root: Path
    mode: str
    host: str
    port: int
    client_id: int
    account_id: str
    strategy_id: str
    symbol: str
    contract_month: str
    action: str
    quantity: float
    order_type: str
    limit_price_model: str
    time_in_force: str
    reason: str
    risk_tags: tuple[str, ...]
    paper_only: bool
    submit: bool = False
    timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS
    kill_switch_path: Path = _DEFAULT_KILL_SWITCH_PATH
    daily_order_cap: int = 1
    caller_path: str = "manual_strategy_bridge_cli"
    output_dir: Path | None = None
    prepare_manual_submit_bundle: bool = False
    approval_digest: str | None = None
    approval_phrase: str | None = None
    manual_frozen_preview_path: Path | None = None
    caller_metadata: dict[str, Any] | None = None
    leak_test_authorization_path: Path | None = None
    leak_test_authorization_digest: str | None = None


@dataclass(frozen=True)
class IbkrPaperStrategyBridgeArtifacts:
    classification: str
    report: dict[str, Any]
    audit_events: list[dict[str, Any]]

    @property
    def exit_code(self) -> int:
        return 0 if self.classification != "PAPER_STRATEGY_INTENT_BLOCKED" else 1


@dataclass
class _Runtime:
    session: IbkrSession
    client: IbkrClient
    collector: IbkrManualPaperSubmitCollector
    transport: IbkrManualPaperSubmitTransport


def _bridge_phase1_target(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    intent: IbkrPaperStrategyOrderIntent,
) -> dict[str, Any]:
    metadata = dict(config.caller_metadata or {})
    lane_adapter = lane_submit_bridge_adapter(lane_id=intent.strategy_id)
    target = dict(lane_adapter.get("bridge_execution_target") or {}) if lane_adapter is not None else {}
    approved = bool(target)
    if not target:
        try:
            executable_proxy = str(
                metadata.get("executable_proxy")
                or config.symbol
                or intent.symbol
                or ""
            ).strip().upper()
            target = dict(
                phase1_execution_target_for_symbol(
                    executable_proxy,
                    contract_month=str(config.contract_month or intent.contract_month or "").strip() or None,
                )
            )
            approved = True
        except KeyError:
            try:
                target = dict(
                    phase1_execution_target_for_source(
                        str(
                            metadata.get("source_instrument")
                            or (lane_adapter or {}).get("source_instrument")
                            or config.symbol
                            or intent.symbol
                            or ""
                        ).strip().upper()
                    )
                    or {}
                )
                approved = bool(target)
            except KeyError:
                target = {}
    symbol = str(target.get("symbol") or config.symbol or intent.symbol or "").strip().upper()
    contract_month = str(target.get("contract_month") or config.contract_month or intent.contract_month or "").strip()
    caller_path = str(config.caller_path or "").strip()
    route_destination = str(
        metadata.get("route_destination")
        or (lane_adapter or {}).get("current_order_destination")
        or ""
    ).strip()
    source_symbol = str(
        metadata.get("source_instrument")
        or (lane_adapter or {}).get("source_instrument")
        or intent.symbol
        or config.symbol
        or ""
    ).strip().upper()
    supervised_runtime_route = (
        caller_path in _APPROVED_RUNTIME_CALLER_PATHS
        and route_destination == "ibkr_paper_bridge_submit_capable"
        and _runtime_caller_metadata_is_authorized(
            caller_metadata=metadata,
            expected_strategy_id=config.strategy_id,
            expected_lane_id=config.strategy_id,
            expected_executable_proxy=symbol or None,
            expected_action=config.action,
        )
    )
    return {
        **target,
        "approved": approved,
        "symbol": symbol,
        "contract_month": contract_month,
        "source_symbol": source_symbol,
        "route_destination": route_destination,
        "lane_adapter_present": lane_adapter is not None,
        "lane_adapter": dict(lane_adapter or {}),
        "supervised_runtime_route": supervised_runtime_route,
        "friendly_label": str(target.get("friendly_label") or f"{symbol} {contract_month}".strip()).strip(),
    }


def _phase1_target_detail_label(target: dict[str, Any]) -> str:
    symbol = str(target.get("symbol") or "").strip().upper()
    contract_month = str(target.get("contract_month") or "").strip()
    if symbol and contract_month:
        return f"{symbol} {contract_month}"
    if symbol:
        return symbol
    return "the approved phase-1 execution target"


def _phase1_target_is_configured(target: dict[str, Any]) -> bool:
    return bool(target.get("approved")) and bool(str(target.get("symbol") or "").strip())


def _deprecated_submit_root_detail(repo_root: Path) -> str | None:
    try:
        normalized = str(Path(repo_root).expanduser().resolve())
    except OSError:
        normalized = str(Path(repo_root).expanduser())
    for fragment in _DEPRECATED_SUBMIT_ROOT_FRAGMENTS:
        if fragment in normalized:
            return (
                "Submit-capable PAPER bridge calls must not originate from deprecated "
                f"or cloud-synced repo roots; resolved repo_root={normalized}."
            )
    return None


def _exact_contract_matches_phase1_target(
    *,
    contract: dict[str, Any],
    target: dict[str, Any],
) -> bool:
    if str(contract.get("broker_symbol") or "").strip().upper() != str(target.get("symbol") or "").strip().upper():
        return False
    expected_expiry = str(target.get("expiry") or "").strip()
    expected_contract_month = str(target.get("contract_month") or "").strip()
    actual_expiry = str(contract.get("expiry") or "").strip()
    if expected_expiry and actual_expiry != expected_expiry:
        return False
    if not expected_expiry and expected_contract_month and actual_expiry and not actual_expiry.startswith(expected_contract_month):
        return False
    expected_con_id = target.get("con_id")
    actual_con_id = contract.get("con_id")
    if expected_con_id is not None and actual_con_id is not None and int(actual_con_id) != int(expected_con_id):
        return False
    expected_local_symbol = str(target.get("local_symbol") or "").strip().upper()
    actual_local_symbol = str(contract.get("local_symbol") or "").strip().upper()
    if expected_local_symbol and actual_local_symbol and actual_local_symbol != expected_local_symbol:
        return False
    expected_exchange = str(target.get("exchange") or "").strip().upper()
    actual_exchange = str(contract.get("exchange") or "").strip().upper()
    if expected_exchange and actual_exchange and actual_exchange != expected_exchange:
        return False
    expected_currency = str(target.get("currency") or "").strip().upper()
    actual_currency = str(contract.get("currency") or "").strip().upper()
    if expected_currency and actual_currency and actual_currency != expected_currency:
        return False
    expected_multiplier = str(target.get("multiplier") or "").strip()
    actual_multiplier = str(contract.get("multiplier") or "").strip()
    if expected_multiplier and actual_multiplier and actual_multiplier != expected_multiplier:
        return False
    return True


def _monitor_exact_contract_matches_target(
    *,
    monitor_exact_contract: dict[str, Any],
    target: dict[str, Any],
) -> bool:
    symbol = str(target.get("symbol") or "").strip().upper()
    if not symbol:
        return False
    if str(monitor_exact_contract.get("symbol") or "").strip().upper() != symbol:
        return False
    expected_expiry = str(target.get("expiry") or "").strip()
    expected_contract_month = str(target.get("contract_month") or "").strip()
    actual_expiry = str(monitor_exact_contract.get("expiry") or "").strip()
    if expected_expiry and actual_expiry != expected_expiry:
        return False
    if not expected_expiry and expected_contract_month and actual_expiry and not actual_expiry.startswith(expected_contract_month):
        return False
    expected_con_id = target.get("con_id")
    actual_con_id = monitor_exact_contract.get("con_id")
    if expected_con_id is not None and actual_con_id not in (None, "") and int(actual_con_id) != int(expected_con_id):
        return False
    expected_local_symbol = str(target.get("local_symbol") or "").strip().upper()
    actual_local_symbol = str(monitor_exact_contract.get("local_symbol") or "").strip().upper()
    if expected_local_symbol and actual_local_symbol and actual_local_symbol != expected_local_symbol:
        return False
    return True


def strategy_order_intent_schema() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "IBKR Paper Strategy Order Intent",
        "type": "object",
        "required": [
            "strategy_id",
            "symbol",
            "contract_month",
            "action",
            "quantity",
            "order_type",
            "limit_price_model",
            "time_in_force",
            "reason",
            "timestamp",
            "risk_tags",
            "paper_only",
        ],
        "properties": {
            "intent_id": {"type": "string"},
            "strategy_id": {"type": "string"},
            "symbol": {"type": "string"},
            "contract_month": {"type": "string"},
            "action": {"type": "string", "enum": sorted(_SCHEMA_ACTIONS)},
            "quantity": {"type": "number", "minimum": 0},
            "order_type": {"type": "string"},
            "limit_price_model": {"type": ["string", "null"]},
            "time_in_force": {"type": "string"},
            "reason": {"type": "string"},
            "timestamp": {"type": "string", "format": "date-time"},
            "risk_tags": {"type": "array", "items": {"type": "string"}},
            "paper_only": {"type": "boolean", "const": True},
            "current_strategy_state": {"type": ["object", "null"]},
            "contract_target": {"type": ["object", "null"]},
            "signal_id": {"type": ["string", "null"]},
        },
        "additionalProperties": False,
    }


def run_ibkr_paper_strategy_bridge(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    transport_factory: Callable[..., Any] = IbkrManualPaperSubmitTransport,
    module_loader: Callable[[str], Any] | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    stack_provider: Callable[[], list[Any]] = inspect.stack,
) -> IbkrPaperStrategyBridgeArtifacts:
    started_at = datetime.now(timezone.utc)
    audit_events: list[dict[str, Any]] = []
    intent = _build_intent(config)
    environment_lock = evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port)
    caller_gate = evaluate_strategy_bridge_caller(
        caller_path=config.caller_path,
        caller_metadata=dict(config.caller_metadata or {}),
        stack_provider=stack_provider,
    )
    monitor_status = load_paper_strategy_monitor_status(repo_root=config.repo_root)
    governance_status = load_paper_strategy_governance_status(repo_root=config.repo_root, strategy_id=config.strategy_id)
    governance_row = dict(governance_status.get("selected_strategy") or {})
    metadata = dict(config.caller_metadata or {})
    exposure_status = evaluate_paper_strategy_exposure_gate(
        repo_root=config.repo_root,
        strategy_id=_exposure_strategy_id_for_bridge(config=config),
        bridge_strategy_id=str(governance_row.get("bridge_strategy_id") or "").strip() or None,
        action=config.action,
        intent_type=str(metadata.get("intent_type") or "").strip().upper() or None,
        quantity=config.quantity,
        executable_symbol=config.symbol,
        account_id=str(metadata.get("account_id") or config.account_id or "").strip() or None,
        con_id=_int_or_none(metadata.get("con_id")),
        local_symbol=str(metadata.get("local_symbol") or "").strip() or None,
        lifecycle_id=str(
            metadata.get("lifecycle_id")
            or metadata.get("position_lifecycle_id")
            or metadata.get("managed_lifecycle_id")
            or ""
        ).strip()
        or None,
    )
    bridge_audit_history = _load_bridge_audit_history(config.output_dir)
    runtime: _Runtime | None = None
    _record_bridge_audit(
        audit_events,
        event_type="intent_received",
        detail="Strategy bridge received one paper strategy intent.",
        config=config,
        extra={"intent": intent.to_dict(), "caller_metadata": dict(config.caller_metadata or {})},
    )
    static_checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=environment_lock,
        caller_gate=caller_gate,
        monitor_status=monitor_status,
        governance_status=governance_status,
        exposure_status=exposure_status,
    )
    static_failures = [row for row in static_checks if row.get("blocking") and not row.get("passed")]
    if static_failures:
        detail = str(static_failures[0].get("detail") or "Paper strategy bridge static preflight failed closed.")
        _record_bridge_audit(
            audit_events,
            event_type="preflight_blocked",
            detail=detail,
            config=config,
            extra={"failing_checks": static_failures},
        )
        return IbkrPaperStrategyBridgeArtifacts(
            classification="PAPER_STRATEGY_INTENT_BLOCKED",
            report={
                "classification": "PAPER_STRATEGY_INTENT_BLOCKED",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "started_at": started_at.isoformat(),
                "environment": {
                    "mode": config.mode,
                    "host": config.host,
                    "port": config.port,
                    "client_id": config.client_id,
                    "account_id": config.account_id,
                    "read_only_preflight": True,
                    "timeout_seconds": config.timeout_seconds,
                },
                "connection_diagnostics": _bridge_connection_diagnostics(config=config),
                "intent": intent.to_dict(),
                "caller_gate": caller_gate,
                "caller_metadata": dict(config.caller_metadata or {}),
                "environment_lock_check": environment_lock,
                "paper_strategy_monitor_status": monitor_status,
                "paper_strategy_governance_status": governance_status,
                "paper_strategy_exposure_status": exposure_status,
                "preflight_checks": static_checks,
                "detail": detail,
                "errors": [],
            },
            audit_events=audit_events,
        )
    try:
        runtime = _build_runtime(config=config, transport_factory=transport_factory, module_loader=module_loader)
        runtime.transport.connect()
        _start_runtime(runtime)
        if not _wait_for_connection_ready(
            transport=runtime.transport,
            collector=runtime.collector,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        ):
            latest_error = runtime.collector.latest_error(codes=_SEVERE_CONNECTION_ERROR_CODES) or runtime.collector.latest_error()
            raise IbkrPaperStrategyBridgeError(
                (
                    f"IBKR paper strategy bridge handshake failed: {latest_error['message']}"
                    if latest_error is not None
                    else f"IBKR paper strategy bridge handshake did not complete within {config.timeout_seconds:.1f}s."
                )
            )
        selected_account_id = _collect_managed_account_context(
            config=_position_like_config(config),
            runtime=runtime,
            sleep_fn=sleep_fn,
        )
        exact_contract, exact_contract_report = _collect_exact_contract_context(
            config=_position_like_config(config),
            runtime=runtime,
            sleep_fn=sleep_fn,
        )
        account_truth, _provider_snapshot = _collect_account_truth(
            config=_position_like_config(config),
            runtime=runtime,
            selected_account_id=selected_account_id,
            sleep_fn=sleep_fn,
        )
        positions = _refresh_positions_snapshot(
            runtime=runtime,
            config=_position_like_config(config),
            selected_account_id=selected_account_id,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        open_orders = _refresh_open_orders_snapshot(
            runtime=runtime,
            config=_position_like_config(config),
            selected_account_id=selected_account_id,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        expected_target = _bridge_phase1_target(config=config, intent=intent)
        qualified_contract_report = _qualify_futures_contract(
            transport=runtime.transport,
            collector=runtime.collector,
            symbol=str(expected_target.get("symbol") or intent.symbol or "").strip().upper(),
            expiry=str(
                expected_target.get("expiry")
                or expected_target.get("contract_month")
                or intent.contract_month
                or ""
            ).strip(),
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        quote_context = _probe_delayed_quote_context(
            transport=runtime.transport,
            collector=runtime.collector,
            contract=qualified_contract_report["qualified_contract_object"],
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        current_position_quantity = _exact_contract_position_quantity(
            positions_snapshot=positions,
            contract_report={"qualified_contract": qualified_contract_report.get("qualified_contract") or {}},
        )
        phase1_gate = _phase1_reconciliation_gate_for_bridge(config=config, intent=intent)
        exit_attempt_policy = _exit_attempt_policy_for_bridge(
            config=config,
            intent=intent,
            history_events=bridge_audit_history,
            current_position_quantity=current_position_quantity,
            open_orders=open_orders,
            phase1_gate=phase1_gate,
        )
        entry_attempt_memory = _build_entry_attempt_memory(
            history_events=bridge_audit_history,
            config=config,
            intent=intent,
        )
        entry_execution_pricing = _entry_execution_pricing_for_bridge(
            config=config,
            intent=intent,
            quote_context=quote_context,
            qualified_contract_report=qualified_contract_report,
            entry_attempt_memory=entry_attempt_memory,
            exit_attempt_policy=exit_attempt_policy,
        )
        dynamic_checks = _build_preflight_checks(
            config=config,
            intent=intent,
            selected_account_id=selected_account_id,
            open_orders=open_orders,
            current_position_quantity=current_position_quantity,
            quote_context=quote_context,
            exact_contract_report=exact_contract_report,
            qualified_contract_report=qualified_contract_report,
            audit_events=audit_events,
            exit_attempt_policy=exit_attempt_policy,
            entry_execution_pricing=entry_execution_pricing,
        )
        preflight_checks = [*static_checks, *dynamic_checks]
        blocking_failures = [row for row in preflight_checks if row.get("blocking") and not row.get("passed")]
        if blocking_failures:
            detail = str(blocking_failures[0].get("detail") or "Paper strategy bridge preflight failed closed.")
            classification = "PAPER_STRATEGY_INTENT_BLOCKED"
            _record_bridge_audit(
                audit_events,
                event_type="preflight_blocked",
                detail=detail,
                config=config,
                extra={"failing_checks": blocking_failures},
            )
            report = _build_report(
                config=config,
                classification=classification,
                started_at=started_at,
                intent=intent,
                caller_gate=caller_gate,
                environment_lock=environment_lock,
                account_truth=account_truth,
                selected_account_id=selected_account_id,
                paper_strategy_governance_status=governance_status,
                positions=positions,
                open_orders=open_orders,
                quote_context=quote_context,
                paper_strategy_monitor_status=monitor_status,
                paper_strategy_exposure_status=exposure_status,
                exact_contract_report=exact_contract_report,
                qualified_contract_report=qualified_contract_report,
                current_position_quantity=current_position_quantity,
                preflight_checks=preflight_checks,
                prepared_submit_bundle=None,
                delegated_result=None,
                exit_attempt_policy=exit_attempt_policy,
                entry_attempt_memory=entry_attempt_memory,
                entry_execution_pricing=entry_execution_pricing,
                callback_timeline_event_count=len(_build_callback_timeline(runtime)),
                errors=list(runtime.collector.errors),
                connection_diagnostics=_bridge_connection_diagnostics(config=config, runtime=runtime),
            )
            return IbkrPaperStrategyBridgeArtifacts(classification=classification, report=report, audit_events=audit_events)

        delegated_result = None
        prepared_submit_bundle = None
        known_managed_exit_order_persistence = None
        classification = "PAPER_STRATEGY_BRIDGE_READY"
        _record_bridge_audit(
            audit_events,
            event_type="intent_ready",
            detail="Paper strategy bridge preflight passed and the intent is ready for operator-controlled paper submit.",
            config=config,
            extra={"current_position_quantity": current_position_quantity},
        )
        if config.prepare_manual_submit_bundle:
            prepared_submit_bundle = _prepare_manual_submit_bundle(
                config=config,
                intent=intent,
                exit_attempt_policy=exit_attempt_policy,
                entry_execution_pricing=entry_execution_pricing,
            )
            _record_bridge_audit(
                audit_events,
                event_type="manual_submit_bundle_prepared",
                detail="Paper strategy bridge prepared a frozen manual submit bundle for the current strategy intent.",
                config=config,
                extra={
                    "bundle_path": prepared_submit_bundle.get("frozen_preview_path"),
                    "preview_digest": prepared_submit_bundle.get("preview_digest"),
                    "entry_execution_pricing": entry_execution_pricing,
                },
            )
        if config.submit:
            delegated_result = _delegate_to_manual_harness(
                config=config,
                intent=intent,
                exit_attempt_policy=exit_attempt_policy,
                entry_execution_pricing=entry_execution_pricing,
            )
            classification = _map_delegate_classification(delegated_result)
            known_managed_exit_order_persistence = _persist_known_managed_exit_order_after_submit(
                config=config,
                intent=intent,
                delegated_result=delegated_result,
                qualified_contract_report=qualified_contract_report,
                entry_execution_pricing=entry_execution_pricing,
                exit_attempt_policy=exit_attempt_policy,
            )
            _record_bridge_audit(
                audit_events,
                event_type="delegated_manual_harness_completed",
                detail="Paper strategy bridge delegated to the proven manual paper harness.",
                config=config,
                extra={
                    "delegated_classification": delegated_result.get("classification"),
                    "exit_attempt_policy": exit_attempt_policy.to_json_dict(),
                    "entry_attempt_memory": entry_attempt_memory,
                    "entry_execution_pricing": entry_execution_pricing,
                    "caller_metadata": dict(config.caller_metadata or {}),
                    "intent": intent.to_dict(),
                    "known_managed_exit_order_persistence": known_managed_exit_order_persistence,
                },
            )
            if known_managed_exit_order_persistence:
                _record_bridge_audit(
                    audit_events,
                    event_type="known_managed_exit_order_persisted",
                    detail="The bridge persisted a known managed exit order after a close submit reached broker order identity.",
                    config=config,
                    extra={"known_managed_exit_order_persistence": known_managed_exit_order_persistence},
                )
        report = _build_report(
            config=config,
            classification=classification,
            started_at=started_at,
            intent=intent,
            caller_gate=caller_gate,
            environment_lock=environment_lock,
            paper_strategy_monitor_status=monitor_status,
            paper_strategy_governance_status=governance_status,
            paper_strategy_exposure_status=exposure_status,
            account_truth=account_truth,
            selected_account_id=selected_account_id,
            positions=positions,
            open_orders=open_orders,
            quote_context=quote_context,
            exact_contract_report=exact_contract_report,
            qualified_contract_report=qualified_contract_report,
            current_position_quantity=current_position_quantity,
            preflight_checks=preflight_checks,
            prepared_submit_bundle=prepared_submit_bundle,
            delegated_result=delegated_result,
            exit_attempt_policy=exit_attempt_policy,
            entry_attempt_memory=entry_attempt_memory,
            entry_execution_pricing=entry_execution_pricing,
            callback_timeline_event_count=len(_build_callback_timeline(runtime)),
            errors=list(runtime.collector.errors),
            connection_diagnostics=_bridge_connection_diagnostics(config=config, runtime=runtime),
        )
        if known_managed_exit_order_persistence:
            report["known_managed_exit_order_persistence"] = known_managed_exit_order_persistence
        return IbkrPaperStrategyBridgeArtifacts(classification=classification, report=report, audit_events=audit_events)
    except Exception as exc:
        classification = "PAPER_STRATEGY_INTENT_BLOCKED"
        _record_bridge_audit(
            audit_events,
            event_type="bridge_failed_closed",
            detail=str(exc),
            config=config,
        )
        report = {
            "classification": classification,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "started_at": started_at.isoformat(),
            "environment": {
                "mode": config.mode,
                "host": config.host,
                "port": config.port,
                "client_id": config.client_id,
                "account_id": config.account_id,
                "read_only_preflight": True,
                "timeout_seconds": config.timeout_seconds,
            },
            "connection_diagnostics": _bridge_connection_diagnostics(config=config, runtime=runtime, exc=exc),
            "intent": intent.to_dict(),
            "caller_gate": caller_gate,
            "caller_metadata": dict(config.caller_metadata or {}),
            "environment_lock_check": environment_lock,
            "paper_strategy_monitor_status": monitor_status,
            "paper_strategy_governance_status": governance_status,
            "paper_strategy_exposure_status": exposure_status,
            "detail": str(exc),
            "errors": [] if runtime is None else list(runtime.collector.errors),
        }
        return IbkrPaperStrategyBridgeArtifacts(classification=classification, report=report, audit_events=audit_events)
    finally:
        if runtime is not None:
            try:
                runtime.transport.disconnect()
            except Exception:
                pass


def write_ibkr_paper_strategy_bridge_artifacts(
    *,
    output_dir: Path,
    artifacts: IbkrPaperStrategyBridgeArtifacts,
) -> None:
    reports_dir = Path(output_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / f"{_ARTIFACT_STEM}_report.json").write_text(
        json.dumps(artifacts.report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / f"{_ARTIFACT_STEM}_report.md").write_text(
        render_ibkr_paper_strategy_bridge_markdown(artifacts.report),
        encoding="utf-8",
    )
    with (reports_dir / f"{_ARTIFACT_STEM}_audit.jsonl").open("a", encoding="utf-8") as handle:
        for row in artifacts.audit_events:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")
    (reports_dir / "per_strategy_paper_status_summary.json").write_text(
        json.dumps(_per_strategy_status_summary(artifacts.report), indent=2, sort_keys=True),
        encoding="utf-8",
    )


def render_ibkr_paper_strategy_bridge_markdown(report: dict[str, Any]) -> str:
    environment = dict(report.get("environment") or {})
    intent = dict(report.get("intent") or {})
    exact_contract = dict(report.get("qualified_contract_report", {}).get("qualified_contract") or {})
    current_position_quantity = report.get("current_position_quantity")
    monitor_status = dict(report.get("paper_strategy_monitor_status") or {})
    governance_status = dict(report.get("paper_strategy_governance_status") or {})
    governance_row = dict(governance_status.get("selected_strategy") or {})
    exposure_status = dict(report.get("paper_strategy_exposure_status") or {})
    lines = [
        "# IBKR Paper Strategy Bridge Report",
        "",
        f"- classification: `{report.get('classification')}`",
        f"- environment: `{environment.get('mode')} / {environment.get('host')} / {environment.get('port')}`",
        f"- account: `{report.get('selected_account_id')}`",
        f"- strategy id: `{intent.get('strategy_id')}`",
        f"- executable symbol: `{intent.get('symbol')}`",
        f"- action / qty / type / tif: `{intent.get('action')} / {intent.get('quantity')} / {intent.get('order_type')} / {intent.get('time_in_force')}`",
        f"- limit price model: `{intent.get('limit_price_model')}`",
        f"- exact qualified contract: `{exact_contract.get('broker_symbol') or exact_contract.get('internal_symbol')} {exact_contract.get('expiry')}` / `conId={exact_contract.get('con_id')}` / `localSymbol={exact_contract.get('local_symbol')}`",
        f"- current exact position quantity: `{current_position_quantity}`",
        f"- current open-order count: `{report.get('open_orders', {}).get('open_order_count')}`",
        f"- paper strategy monitor classification: `{monitor_status.get('classification')}`",
        f"- strategy submit allowed: `{monitor_status.get('submit_allowed')}`",
        f"- governance classification: `{governance_status.get('classification')}`",
        f"- governance strategy status: `{governance_row.get('strategy_status')}`",
        f"- governance submit allowed: `{governance_status.get('submit_allowed')}`",
        f"- exposure gate classification: `{exposure_status.get('classification')}`",
        f"- exposure submit allowed: `{exposure_status.get('submit_allowed')}`",
        "",
        "## Summary",
        "",
    ]
    classification = str(report.get("classification") or "")
    if classification == "PAPER_STRATEGY_BRIDGE_READY":
        lines.append("- manual operator preflight passed and the paper strategy lane is ready for explicit operator-controlled submit through the shared IBKR paper bridge.")
    elif classification == "PAPER_STRATEGY_ENTRY_MISSED_CANCELLED_ACCEPTED":
        lines.append("- a passive/resting entry window ended without a fill; this is recorded as an accepted miss rather than a strategy execution failure.")
    elif classification == "PAPER_STRATEGY_INTENT_BLOCKED":
        lines.append("- preflight failed closed. Review the failing guardrails before attempting any paper submit.")
    else:
        lines.append("- the bridge delegated into the proven paper harness; review the delegated result for final broker truth and position reconciliation.")
    lines.append("")
    lines.append("## Guardrails")
    lines.append("")
    for row in list(report.get("preflight_checks") or []):
        lines.append(f"- `{row.get('name')}`: `{'PASS' if row.get('passed') else 'FAIL'}` — {row.get('detail')}")
    prepared = report.get("prepared_submit_bundle")
    if isinstance(prepared, dict):
        lines.append("")
        lines.append("## Prepared Submit Bundle")
        lines.append("")
        lines.append(f"- bundle classification: `{prepared.get('classification')}`")
        lines.append(f"- frozen preview path: `{prepared.get('frozen_preview_path')}`")
        lines.append(f"- preview digest: `{prepared.get('preview_digest')}`")
        lines.append(f"- expected approval phrase: `{prepared.get('expected_approval_phrase')}`")
    delegated = report.get("delegated_result")
    if isinstance(delegated, dict):
        lines.append("")
        lines.append("## Delegated Result")
        lines.append("")
        lines.append(f"- delegated classification: `{delegated.get('classification')}`")
        lines.append(f"- delegated detail: {delegated.get('detail') or delegated.get('report', {}).get('lifecycle', {}).get('detail')}")
    return "\n".join(lines)


def write_strategy_order_intent_schema_file(*, repo_root: Path) -> Path:
    path = Path(repo_root) / _SCHEMA_PATH
    path.write_text(json.dumps(strategy_order_intent_schema(), indent=2, sort_keys=True), encoding="utf-8")
    return path


def evaluate_strategy_bridge_caller(
    *,
    caller_path: str,
    caller_metadata: dict[str, Any] | None = None,
    stack_provider: Callable[[], list[Any]] = inspect.stack,
) -> dict[str, Any]:
    normalized_caller = str(caller_path or "").strip()
    runtime_caller = normalized_caller in _APPROVED_RUNTIME_CALLER_PATHS
    runtime_metadata_authorized = runtime_caller and _runtime_caller_metadata_is_authorized(
        caller_metadata=dict(caller_metadata or {}),
    )
    stack_modules: list[str] = []
    for frame in stack_provider():
        module_name = str(getattr(getattr(frame, "frame", None), "f_globals", {}).get("__name__", "") or "").strip()
        if module_name:
            stack_modules.append(module_name)
    forbidden: list[str] = []
    for module_name in stack_modules:
        if runtime_caller and module_name.startswith(_APPROVED_RUNTIME_CALLER_MODULE_PREFIXES):
            continue
        if runtime_metadata_authorized and module_name.startswith(_APPROVED_RUNTIME_STRATEGY_ENGINE_MODULE_PREFIXES):
            continue
        if (
            module_name.startswith(_BRIDGE_ADDITIONAL_FORBIDDEN_CALLER_PREFIXES)
            or module_name.startswith(_FORBIDDEN_CALLER_PREFIXES)
            or any(fragment in module_name for fragment in _FORBIDDEN_CALLER_SUBSTRINGS)
            or "scheduler" in module_name
        ):
            forbidden.append(module_name)
    caller_allowed = normalized_caller in _APPROVED_CALLER_PATHS
    passed = caller_allowed and not forbidden
    return {
        "caller_path": normalized_caller,
        "passed": passed,
        "fail_closed": not passed,
        "forbidden_callers_detected": forbidden,
        "detail": (
            "Paper strategy bridge caller path is approved for supervised paper routing."
            if passed
            else (
                "Paper strategy bridge rejected a non-manual caller path."
                if not caller_allowed
                else f"Paper strategy bridge detected forbidden caller frames: {', '.join(forbidden)}"
            )
        ),
    }


def _runtime_caller_metadata_is_authorized(
    *,
    caller_metadata: dict[str, Any],
    expected_strategy_id: str | None = None,
    expected_lane_id: str | None = None,
    expected_executable_proxy: str | None = None,
    expected_action: str | None = None,
) -> bool:
    metadata = dict(caller_metadata or {})
    strategy_id = str(metadata.get("strategy_id") or "").strip()
    lane_id = str(metadata.get("lane_id") or "").strip()
    executable_proxy = str(metadata.get("executable_proxy") or "").strip().upper()
    intent_action = str(metadata.get("intent_action") or "").strip().upper()
    return (
        str(metadata.get("caller_type") or "").strip() in _APPROVED_RUNTIME_CALLER_TYPES
        and metadata.get("paper_only") is True
        and str(metadata.get("mode") or "").strip().upper() == _EXPECTED_MODE
        and str(metadata.get("host") or "").strip() == _EXPECTED_HOST
        and int(metadata.get("port") or 0) == _EXPECTED_PORT
        and str(metadata.get("account_id") or "").strip() == _EXPECTED_ACCOUNT_ID
        and strategy_id != ""
        and lane_id != ""
        and str(metadata.get("source_instrument") or "").strip().upper() != ""
        and executable_proxy != ""
        and str(metadata.get("route_destination") or "").strip() == "ibkr_paper_bridge_submit_capable"
        and str(metadata.get("bridge_proxy_mode") or "").strip() != ""
        and intent_action != ""
        and str(metadata.get("intent_type") or "").strip().upper() != ""
        and (expected_strategy_id is None or strategy_id == expected_strategy_id)
        and (expected_lane_id is None or lane_id == expected_lane_id)
        and (expected_executable_proxy is None or executable_proxy == expected_executable_proxy)
        and (expected_action is None or intent_action == expected_action)
    )


def _runtime_caller_metadata_check(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    intent: IbkrPaperStrategyOrderIntent,
) -> dict[str, Any]:
    caller_path = str(config.caller_path or "").strip()
    if caller_path not in _APPROVED_RUNTIME_CALLER_PATHS:
        return _check(
            "approved_runtime_caller_metadata",
            True,
            True,
            "Runtime caller metadata is only required for approved non-manual supervised paper callers.",
        )
    metadata = dict(config.caller_metadata or {})
    if not metadata:
        return _check(
            "approved_runtime_caller_metadata",
            False,
            True,
            "Approved supervised paper runtime callers must provide explicit caller metadata.",
        )
    passed = _runtime_caller_metadata_is_authorized(
        caller_metadata=metadata,
        expected_strategy_id=config.strategy_id,
        expected_lane_id=config.strategy_id,
        expected_executable_proxy=str(_bridge_phase1_target(config=config, intent=intent).get("symbol") or config.symbol).strip().upper(),
        expected_action=config.action,
    )
    return _check(
        "approved_runtime_caller_metadata",
        passed,
        True,
        (
            "Approved supervised paper runtime caller metadata is present and matches the paper bridge environment lock."
            if passed
            else "Approved supervised paper runtime caller metadata is missing or does not match the required PAPER / 127.0.0.1 / 7497 / DUM882026 route context."
        ),
    )


def _leak_test_authorization_digest_payload(authorization: dict[str, Any]) -> dict[str, Any]:
    return {field: authorization.get(field) for field in _LEAK_TEST_AUTHORIZATION_DIGEST_FIELDS}


def _leak_test_authorization_digest(authorization: dict[str, Any]) -> str:
    payload = json.dumps(
        _leak_test_authorization_digest_payload(authorization),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _load_leak_test_authorization(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _parse_iso_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _leak_test_authorization_check(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    intent: IbkrPaperStrategyOrderIntent,
) -> dict[str, Any]:
    caller_path = str(config.caller_path or "").strip()
    if caller_path != _LEAK_TEST_CALLER_PATH:
        return _check(
            "leak_test_authorization",
            True,
            True,
            "Leak-test authorization is only required for the dedicated leak-test caller path.",
        )
    authorization = _load_leak_test_authorization(config.leak_test_authorization_path)
    if not authorization:
        return _check(
            "leak_test_authorization",
            False,
            True,
            "Dedicated leak-test bridge callers require a readable leak-test authorization artifact.",
        )
    expected_digest = _leak_test_authorization_digest(authorization)
    supplied_digest = str(config.leak_test_authorization_digest or "").strip()
    artifact_digest = str(authorization.get("digest") or "").strip()
    if not supplied_digest or supplied_digest != artifact_digest or artifact_digest != expected_digest:
        return _check(
            "leak_test_authorization",
            False,
            True,
            "Dedicated leak-test bridge caller authorization digest is missing or mismatched.",
        )
    expires_at = _parse_iso_datetime(authorization.get("expires_at"))
    if expires_at is None or expires_at.astimezone(timezone.utc) <= datetime.now(timezone.utc):
        return _check(
            "leak_test_authorization",
            False,
            True,
            "Dedicated leak-test bridge caller authorization is expired.",
        )
    metadata = dict(config.caller_metadata or {})
    intent_type = str(metadata.get("intent_type") or "").strip().upper()
    expected_action = str(intent.action or config.action or "").strip().upper()
    authorization_action_field = "exit_action" if intent_type.endswith("_TO_CLOSE") else "action"
    expected = {
        "artifact_type": _LEAK_TEST_AUTHORIZATION_ARTIFACT_TYPE,
        "account_id": _EXPECTED_ACCOUNT_ID,
        "mode": _EXPECTED_MODE,
        "lane_id": config.strategy_id,
        "symbol": str(config.symbol or intent.symbol or "").strip().upper(),
        "expiry": str(config.contract_month or intent.contract_month or "").strip(),
        authorization_action_field: expected_action,
        "qty": 1,
        "repo_root": str(config.repo_root),
    }
    mismatches = [key for key, value in expected.items() if authorization.get(key) != value]
    if float(authorization.get("qty") or 0) != float(config.quantity or intent.quantity or 0):
        mismatches.append("quantity")
    if str(authorization.get("local_symbol") or "") != str(metadata.get("local_symbol") or ""):
        mismatches.append("local_symbol")
    if mismatches:
        return _check(
            "leak_test_authorization",
            False,
            True,
            f"Dedicated leak-test bridge caller authorization identity mismatch: {', '.join(mismatches)}.",
        )
    safety = dict(authorization.get("safety_snapshot") or {})
    if safety.get("live_money_eligible") is not False or safety.get("paper_proof_invoked") is not False:
        return _check(
            "leak_test_authorization",
            False,
            True,
            "Dedicated leak-test bridge caller authorization safety snapshot is not PAPER-only safe.",
        )
    return _check(
        "leak_test_authorization",
        True,
        True,
        "Dedicated leak-test bridge caller supplied a valid lane-specific authorization artifact.",
    )


def _exposure_strategy_id_for_bridge(*, config: IbkrPaperStrategyBridgeConfig) -> str:
    metadata = dict(config.caller_metadata or {})
    caller_path = str(config.caller_path or "").strip()
    intent_type = str(metadata.get("intent_type") or "").strip().upper()
    if caller_path == _LEAK_TEST_CALLER_PATH and intent_type in {"SELL_TO_CLOSE", "BUY_TO_CLOSE"}:
        lifecycle_owner_strategy_id = str(metadata.get("strategy_id") or "").strip()
        if lifecycle_owner_strategy_id:
            return lifecycle_owner_strategy_id
    return str(config.strategy_id or "").strip()


def _authorized_supervised_runtime_route_check(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    intent: IbkrPaperStrategyOrderIntent,
) -> dict[str, Any]:
    metadata_check = _runtime_caller_metadata_check(config=config, intent=intent)
    caller_path = str(config.caller_path or "").strip()
    route_authorized = caller_path in _APPROVED_RUNTIME_CALLER_PATHS and bool(metadata_check.get("passed"))
    return {
        "passed": route_authorized,
        "detail": str(metadata_check.get("detail") or ""),
        "metadata_check": metadata_check,
    }


def _build_runtime(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    transport_factory: Callable[..., Any],
    module_loader: Callable[[str], Any] | None,
) -> _Runtime:
    session = IbkrSession(
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        account_id=config.account_id,
        gateway_mode="paper",
        read_only=True,
        order_id_policy=build_default_ibkr_order_id_policy(client_id=config.client_id, live_orders_enabled=False),
    )
    client = IbkrClient(session=session)
    collector = IbkrManualPaperSubmitCollector(client)
    transport = transport_factory(
        client=client,
        collector=collector,
        config=IbkrReadOnlyApiTransportConfig(
            host=config.host,
            port=config.port,
            client_id=config.client_id,
            read_only=True,
        ),
        module_loader=module_loader,
    )
    return _Runtime(session=session, client=client, collector=collector, transport=transport)


def _start_runtime(runtime: _Runtime) -> None:
    thread = threading.Thread(target=runtime.transport.run_loop, name="ibkr_paper_strategy_bridge_loop", daemon=True)
    thread.start()


def _build_intent(config: IbkrPaperStrategyBridgeConfig) -> IbkrPaperStrategyOrderIntent:
    return IbkrPaperStrategyOrderIntent(
        strategy_id=str(config.strategy_id or "").strip(),
        symbol=str(config.symbol or "").strip().upper(),
        contract_month=str(config.contract_month or "").strip(),
        action=str(config.action or "").strip().upper(),
        quantity=float(config.quantity),
        order_type=str(config.order_type or "").strip().upper(),
        limit_price_model=str(config.limit_price_model or "").strip().upper(),
        time_in_force=str(config.time_in_force or "").strip().upper(),
        reason=str(config.reason or "").strip(),
        timestamp=datetime.now(timezone.utc).isoformat(),
        risk_tags=tuple(str(tag or "").strip() for tag in config.risk_tags if str(tag or "").strip()),
        paper_only=bool(config.paper_only),
        intent_id=str(uuid.uuid4()),
    )


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _build_preflight_checks(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    intent: IbkrPaperStrategyOrderIntent,
    selected_account_id: str,
    open_orders: dict[str, Any],
    current_position_quantity: float | None,
    quote_context: dict[str, Any],
    exact_contract_report: dict[str, Any],
    qualified_contract_report: dict[str, Any],
    audit_events: list[dict[str, Any]],
    exit_attempt_policy: ExitAttemptPolicy | None = None,
    entry_execution_pricing: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    if exit_attempt_policy is None:
        exit_attempt_policy = classify_exit_attempt_policy(
            history_events=[],
            lifecycle_id=None,
            intent_type=str((config.caller_metadata or {}).get("intent_type") or "").strip().upper() or None,
            action=intent.action,
            hard_exit=False,
            broker_position_quantity=current_position_quantity,
            broker_reconciled=True,
            open_order_count=int(open_orders.get("open_order_count") or 0),
        )
    pricing = dict(entry_execution_pricing or {})
    runtime_pricing_source = (
        str(pricing.get("execution_price_source") or "").strip().upper() == _ENTRY_RUNTIME_PRICE_SOURCE
    )
    expected_target = _bridge_phase1_target(config=config, intent=intent)
    expected_label = _phase1_target_detail_label(expected_target)
    checks = [
        _check("account_match", selected_account_id == config.account_id == _EXPECTED_ACCOUNT_ID, True, "Paper bridge account must match DUM882026 exactly."),
        _check("daily_order_cap", _submitted_order_count(audit_events) < int(config.daily_order_cap), True, f"Daily bridge order cap is {config.daily_order_cap}."),
        _check("no_working_orders", int(open_orders.get("open_order_count") or 0) == 0, True, "No working broker order is allowed before a strategy bridge intent can submit."),
        _check(
            "delayed_quote_freshness",
            runtime_pricing_source or _quote_is_fresh(quote_context),
            not runtime_pricing_source,
            (
                "Delayed broker quote freshness is diagnostic because runtime/Databento supplied the execution price."
                if runtime_pricing_source
                else "A fresh delayed quote is required before strategy bridge submit is allowed when no runtime execution price is selected."
            ),
        ),
        _check(
            "exact_qualified_contract",
            _qualified_contract_is_exact(qualified_contract_report, expected_target=expected_target),
            True,
            f"Submitted contract must match the approved phase-1 execution target {expected_label}.",
        ),
        _check("strategy_allowed_state", True, True, "The bridge remains manual-only and the shadow ledger stays separate from broker execution."),
    ]
    if _is_close_intent(config=config, intent=intent):
        checks.append(
            _check(
                "exit_execution_price_source",
                not bool(pricing.get("block_submit")),
                True,
                str(pricing.get("block_reason") or "")
                or (
                    f"Exit execution pricing uses {pricing.get('execution_price_source')} with urgency {pricing.get('exit_urgency')}."
                    if pricing
                    else "Exit execution pricing did not produce a blocking price-source condition."
                ),
            )
        )
        checks.append(
            _check(
                "broker_position_present_for_close",
                not exit_attempt_policy.broker_flat,
                True,
                (
                    "Broker truth shows an exact contract position available for this close intent."
                    if not exit_attempt_policy.broker_flat
                    else "Broker truth reports the exact contract already flat; no close order may be submitted."
                ),
            )
        )
        checks.append(
            _check(
                "broker_lifecycle_reconciled_for_exit",
                not exit_attempt_policy.broker_lifecycle_mismatch,
                True,
                (
                    "Broker/lifecycle reconciliation is clean for this close intent."
                    if not exit_attempt_policy.broker_lifecycle_mismatch
                    else "Broker/lifecycle reconciliation is not clean; close submission fails closed."
                ),
            )
        )
        checks.append(
            _check(
                "exit_attempt_policy_allows_submit",
                not exit_attempt_policy.block_submit,
                True,
                exit_attempt_policy.block_reason
                or f"Exit execution policy {exit_attempt_policy.execution_policy} allows guarded PAPER submit.",
            )
        )
    else:
        pricing = dict(entry_execution_pricing or {})
        checks.append(
            _check(
                "entry_execution_price_source",
                not bool(pricing.get("block_submit")),
                True,
                (
                    str(pricing.get("block_reason") or "")
                    or (
                        f"Entry execution pricing uses {pricing.get('execution_price_source')} with runtime freshness diagnostics."
                        if pricing
                        else "Entry execution pricing is not required for this legacy preview-only route."
                    )
                ),
            )
        )
        checks.append(
            _check(
                "position_gate_buy_to_open",
                True,
                True,
                "BUY intents are governed by per-strategy exposure attribution; aggregate broker flat is not required when stacking is explicitly allowed.",
            )
        )
    _record_bridge_audit(
        audit_events,
        event_type="preflight_evaluated",
        detail="Paper strategy bridge preflight guardrails evaluated.",
        config=config,
        extra={
            "checks": checks,
            "current_position_quantity": current_position_quantity,
            "exact_contract": exact_contract_report.get("exact_contract"),
            "entry_execution_pricing": entry_execution_pricing,
        },
    )
    return checks


def _build_static_preflight_checks(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    intent: IbkrPaperStrategyOrderIntent,
    environment_lock: dict[str, Any],
    caller_gate: dict[str, Any],
    monitor_status: dict[str, Any],
    governance_status: dict[str, Any],
    exposure_status: dict[str, Any],
) -> list[dict[str, Any]]:
    monitor_exact_contract = dict(monitor_status.get("exact_contract") or {})
    monitor_health = str(monitor_status.get("health_classification") or monitor_status.get("monitor_health") or "").strip().upper()
    monitor_account_matches = str(monitor_status.get("account_id") or "").strip() == config.account_id
    governance_row = dict(governance_status.get("selected_strategy") or {})
    expected_target = _bridge_phase1_target(config=config, intent=intent)
    expected_label = _phase1_target_detail_label(expected_target)
    lane_adapter = dict(expected_target.get("lane_adapter") or {})
    runtime_route = _authorized_supervised_runtime_route_check(config=config, intent=intent)
    leak_test_authorization = _leak_test_authorization_check(config=config, intent=intent)
    phase1_reconciliation_gate = _phase1_reconciliation_gate_for_bridge(config=config, intent=intent)
    monitor_contract_matches = _monitor_exact_contract_matches_target(
        monitor_exact_contract=monitor_exact_contract,
        target=expected_target,
    )
    monitor_authority = _paper_strategy_monitor_authority_for_route(
        monitor_status=monitor_status,
        expected_target=expected_target,
        strategy_id=intent.strategy_id,
    )
    monitor_contract_gate_required = (not config.submit) or not bool(runtime_route.get("passed"))
    monitor_detail = _paper_strategy_monitor_submit_gate_detail(monitor_status)
    monitor_authority_detail = str(monitor_authority.get("detail") or "")
    governance_detail = str(
        governance_status.get("detail")
        or (
            "Paper strategy governance allows submit."
            if governance_status.get("submit_allowed")
            else f"Paper strategy governance blocked submit: {', '.join(list(governance_status.get('block_reasons') or [])) or 'unknown_reason'}"
        )
    )
    exposure_detail = str(
        exposure_status.get("detail")
        or (
            "Paper strategy exposure attribution allows submit."
            if exposure_status.get("submit_allowed")
            else f"Paper strategy exposure attribution blocked submit: {', '.join(list(exposure_status.get('block_reasons') or [])) or 'unknown_reason'}"
        )
    )
    governance_submit_allowed = bool(governance_status.get("submit_allowed")) or _governance_exit_override_allowed(
        config=config,
        intent=intent,
        governance_status=governance_status,
        phase1_reconciliation_gate=phase1_reconciliation_gate,
        exposure_status=exposure_status,
    )
    if governance_submit_allowed and not bool(governance_status.get("submit_allowed")):
        governance_detail = (
            "Paper strategy governance entry-readiness blocker is bypassed for a supervised PAPER exit only; "
            "Phase-1 broker reconciliation and owning-strategy exposure gates remain required."
        )
    caller_path = str(config.caller_path or "").strip()
    leak_test_authorized = caller_path == _LEAK_TEST_CALLER_PATH and bool(leak_test_authorization.get("passed"))
    deprecated_root_detail = _deprecated_submit_root_detail(Path(config.repo_root))
    phase1_reconciliation_check = _phase1_reconciliation_gate_check(
        config=config,
        phase1_reconciliation_gate=phase1_reconciliation_gate,
    )
    return [
        _check("approved_paper_caller_path", caller_gate["passed"], True, caller_gate["detail"]),
        dict(runtime_route.get("metadata_check") or _runtime_caller_metadata_check(config=config, intent=intent)),
        leak_test_authorization,
        _check(
            "deprecated_submit_root_block",
            (not config.submit) or deprecated_root_detail is None,
            True,
            deprecated_root_detail or "Submit-capable PAPER bridge repo_root is not a deprecated Documents/iCloud path.",
        ),
        _check("paper_environment_lock", environment_lock["passed"], True, str(environment_lock.get("port_policy") or environment_lock.get("detail") or "Environment lock failed.")),
        _check("paper_only_intent", bool(intent.paper_only), True, "Intent must remain explicitly paper-only."),
        _check("strategy_allowlist", intent.strategy_id in _SUPPORTED_STRATEGY_IDS or bool(lane_adapter), True, "Only the ATP Companion baseline and explicitly ported paper strategy lane identities are allowed in the paper bridge."),
        _check(
            "executable_contract_whitelist",
            _phase1_target_is_configured(expected_target) and intent.symbol == str(expected_target.get("symbol") or "").strip().upper(),
            True,
            (
                f"Executable contract must match the approved phase-1 execution target {expected_label}."
                if _phase1_target_is_configured(expected_target)
                else "No approved phase-1 execution target exists for this supervised paper route."
            ),
        ),
        _check(
            "contract_month_lock",
            _phase1_target_is_configured(expected_target) and intent.contract_month == str(expected_target.get("contract_month") or "").strip(),
            True,
            (
                f"Executable contract month must match the approved phase-1 execution target {expected_label}."
                if _phase1_target_is_configured(expected_target)
                else "No approved phase-1 execution target exists for this supervised paper route."
            ),
        ),
        _check(
            "selected_lane_adapter_present",
            bool(lane_adapter) if intent.strategy_id not in {"ATP_COMPANION_V1_ASIA_US", "ATP_COMPANION_V1_GC_ASIA_US", "ATP_COMPANION_V1_GC_ASIA_US_PRODUCTION_TRACK"} else True,
            True,
            "Non-ATP paper strategy lanes require an explicit bridge adapter before submit-capable routing is allowed.",
        ),
        _check("quantity_cap", float(intent.quantity) == _EXPECTED_QUANTITY, True, "Quantity must equal exactly one contract."),
        _check("order_type_lock", intent.order_type == _EXPECTED_ORDER_TYPE, True, "Only LMT orders are allowed in the phase-1 paper bridge."),
        _check("tif_lock", intent.time_in_force == _EXPECTED_TIF, True, "Only DAY time-in-force is allowed in the phase-1 paper bridge."),
        _check("limit_price_model_lock", intent.limit_price_model in _ALLOWED_LIMIT_PRICE_MODELS, True, "Limit-price model must be one of the explicitly allowed paper bridge models."),
        _check("kill_switch_inactive", not Path(config.kill_switch_path).exists(), True, f"Kill-switch path {config.kill_switch_path} must not exist."),
        _check(
            "paper_strategy_monitor_runtime_present",
            (not config.submit) or (not bool(monitor_authority.get("authoritative"))) or bool(monitor_status),
            True,
            (
                "Scoped legacy paper monitor runtime status is present."
                if bool(monitor_authority.get("authoritative"))
                else monitor_authority_detail
            ),
        ),
        _check(
            "paper_strategy_monitor_running",
            (not config.submit) or (not bool(monitor_authority.get("authoritative"))) or bool(monitor_status.get("monitor_running")),
            True,
            (
                "Scoped legacy paper monitor service is actively running."
                if bool(monitor_authority.get("authoritative"))
                else monitor_authority_detail
            ),
        ),
        _check(
            "paper_strategy_monitor_health",
            (not config.submit) or (not bool(monitor_authority.get("authoritative"))) or monitor_health == "HEALTHY",
            True,
            (
                f"Scoped legacy paper strategy monitor health is {monitor_health or 'UNKNOWN'}."
                if bool(monitor_authority.get("authoritative"))
                else monitor_authority_detail
            ),
        ),
        _check(
            "paper_strategy_monitor_account_match",
            (not config.submit) or (not bool(monitor_authority.get("authoritative"))) or monitor_account_matches,
            True,
            (
                "Scoped legacy paper strategy monitor account matches DUM882026."
                if bool(monitor_authority.get("authoritative"))
                else monitor_authority_detail
            ),
        ),
        _check(
            "paper_strategy_monitor_contract_match",
            (not config.submit)
            or (not bool(monitor_authority.get("authoritative")))
            or ((not monitor_contract_gate_required) or monitor_contract_matches),
            True,
            (
                monitor_authority_detail
                if not bool(monitor_authority.get("authoritative"))
                else f"Submit-capable paper strategy orders require the live monitor exact contract to match the approved phase-1 execution target {expected_label}."
                if monitor_contract_gate_required
                else f"Approved supervised PAPER route resolved executable target {expected_label}; stale global monitor exact-contract snapshots do not veto current route preflight."
            ),
        ),
        _check(
            "manual_harness_bundle_present_for_submit",
            (not config.submit) or caller_path in _APPROVED_RUNTIME_CALLER_PATHS or leak_test_authorized or (
                config.manual_frozen_preview_path is not None
                and config.approval_digest is not None
                and config.approval_phrase is not None
            ),
            True,
            (
                "Approved supervised paper runtime callers may submit through the bridge without a manual frozen preview bundle."
                if caller_path in _APPROVED_RUNTIME_CALLER_PATHS
                else "Dedicated leak-test caller supplied a valid short-lived authorization; the bridge will prepare and record an internal frozen preview bundle."
                if leak_test_authorized
                else "Bridge submit requires a manual-harness frozen preview path plus the exact approval digest and approval phrase."
            ),
        ),
        _check(
            "paper_strategy_submit_gate",
            (not config.submit)
            or (not bool(monitor_authority.get("authoritative")))
            or _paper_strategy_monitor_submit_gate_passed(monitor_status),
            True,
            monitor_detail if bool(monitor_authority.get("authoritative")) else monitor_authority_detail,
        ),
        phase1_reconciliation_check,
        _check(
            "paper_strategy_governance_runtime_present",
            (not config.submit) or bool(governance_status),
            True,
            "Submit-capable paper strategy orders require a live per-strategy governance status file.",
        ),
        _check(
            "paper_strategy_governance_strategy_present",
            (not config.submit) or bool(governance_row),
            True,
            f"Submit-capable paper strategy orders require a governance row for strategy identity {config.strategy_id}.",
        ),
        _check(
            "paper_strategy_governance_status_allowed",
            (not config.submit)
            or str(governance_row.get("strategy_status") or "").strip().upper() not in {"PAUSED", "DISABLED", "KILL_CANDIDATE"},
            True,
            "Submit-capable paper strategy orders require the strategy governance status to stay out of PAUSED / DISABLED / KILL_CANDIDATE.",
        ),
        _check(
            "paper_strategy_governance_submit_gate",
            (not config.submit) or governance_submit_allowed,
            True,
            governance_detail,
        ),
        _check(
            "paper_strategy_exposure_gate",
            (not config.submit) or bool(exposure_status.get("submit_allowed")),
            True,
            exposure_detail,
        ),
    ]


def _governance_exit_override_allowed(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    intent: IbkrPaperStrategyOrderIntent,
    governance_status: dict[str, Any],
    phase1_reconciliation_gate: dict[str, Any],
    exposure_status: dict[str, Any],
) -> bool:
    if not config.submit:
        return False
    caller_path = str(config.caller_path or "").strip()
    if caller_path not in _APPROVED_RUNTIME_CALLER_PATHS:
        return False
    metadata = dict(config.caller_metadata or {})
    intent_type = str(metadata.get("intent_type") or "").strip().upper()
    action = str(intent.action or config.action or "").strip().upper()
    if intent_type not in {"SELL_TO_CLOSE", "BUY_TO_CLOSE"} and action != "EXIT":
        return False
    block_reasons = {str(reason).strip() for reason in governance_status.get("block_reasons") or [] if str(reason).strip()}
    if not block_reasons or not block_reasons.issubset({"backend_or_source_not_live_ready"}):
        return False
    if not bool(phase1_reconciliation_gate.get("ready")):
        return False
    if not bool(exposure_status.get("submit_allowed")):
        return False
    if not _runtime_exit_override_identity_is_current(config=config):
        return False
    selected = dict(governance_status.get("selected_strategy") or {})
    if str(selected.get("strategy_status") or "").strip().upper() in {"PAUSED", "DISABLED", "KILL_CANDIDATE"}:
        return False
    return True


def _phase1_reconciliation_gate_for_bridge(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    intent: IbkrPaperStrategyOrderIntent,
) -> dict[str, Any]:
    gate = dict(evaluate_phase1_broker_reconciliation_submit_gate(repo_root=config.repo_root))
    gate.setdefault("stale_reconciliation_refresh_attempted", False)
    gate.setdefault("stale_reconciliation_refresh_result", None)
    gate.setdefault("refreshed_reconciliation_age", None)
    gate.setdefault("exit_allowed_after_refresh", False)
    gate.setdefault("exit_block_reason", None)
    if not config.submit or not _is_close_intent(config=config, intent=intent):
        return gate
    reasons = {
        str(reason or "").strip()
        for reason in list(gate.get("block_reasons") or [])
        if str(reason or "").strip()
    }
    if "phase1_broker_reconciliation_stale" not in reasons:
        return gate
    non_stale_reasons = sorted(reasons - {"phase1_broker_reconciliation_stale"})
    if non_stale_reasons:
        gate["exit_block_reason"] = "stale_reconciliation_plus_non_stale_blockers"
        return gate
    if not _runtime_exit_override_identity_is_current(config=config):
        gate["exit_block_reason"] = "runtime_identity_not_current_for_stale_reconciliation_refresh"
        return gate

    refresh_result = _refresh_phase1_broker_reconciliation_artifacts(config=config)
    refreshed_gate = dict(evaluate_phase1_broker_reconciliation_submit_gate(repo_root=config.repo_root))
    refreshed_gate["stale_reconciliation_refresh_attempted"] = True
    refreshed_gate["stale_reconciliation_refresh_result"] = refresh_result
    refreshed_gate["refreshed_reconciliation_age"] = refreshed_gate.get("age_seconds")
    refreshed_gate["exit_allowed_after_refresh"] = bool(refreshed_gate.get("ready"))
    refreshed_gate["exit_block_reason"] = (
        None
        if refreshed_gate.get("ready")
        else ",".join(str(reason) for reason in list(refreshed_gate.get("block_reasons") or []))
        or str(refresh_result.get("classification") or "stale_reconciliation_refresh_failed")
    )
    if refreshed_gate.get("ready"):
        refreshed_gate["detail"] = (
            "Stale Phase-1 broker reconciliation was refreshed read-only and is now clean for this managed exit; "
            "route/governance/exposure gates still apply."
        )
    return refreshed_gate


def _refresh_phase1_broker_reconciliation_artifacts(*, config: IbkrPaperStrategyBridgeConfig) -> dict[str, Any]:
    try:
        from ..app.ibkr_broker_truth_refresher import BrokerTruthRefreshConfig, run_broker_truth_refresh_once

        broker_status = run_broker_truth_refresh_once(
            config=BrokerTruthRefreshConfig(
                repo_root=config.repo_root,
                output_dir=config.repo_root / "outputs" / "reports" / "ibkr_read_only_verification",
                status_path=config.repo_root
                / "outputs"
                / "reports"
                / "ibkr_read_only_verification"
                / "ibkr_broker_truth_refresh_status.json",
                var_status_path=config.repo_root / "var" / "ibkr_broker_truth_refresh_status.json",
                mode="PAPER",
                host=config.host,
                port=int(config.port),
                client_id=11080,
                account_id=config.account_id,
                read_only=True,
                timeout_seconds=max(float(config.timeout_seconds), 8.0),
            )
        )
        reconciliation = reconcile_track_b_paper_broker_truth(
            config=ReconciliationConfig(
                repo_root=config.repo_root,
                ledger_root=config.repo_root / "outputs" / "track_b_execution_core" / "paper_trade_ledger",
                broker_truth_root=config.repo_root / "outputs" / "reports" / "ibkr_read_only_verification",
                report_path=config.repo_root
                / "outputs"
                / "reports"
                / "track_b_paper_broker_reconciliation"
                / "latest_track_b_paper_broker_reconciliation.json",
                account=config.account_id,
            )
        )
    except Exception as exc:  # pragma: no cover - explicit unit tests patch the refresh path
        return {
            "classification": "TRACK_B_PHASE1_RECONCILIATION_REFRESH_FAILED",
            "error_type": type(exc).__name__,
            "error": str(exc),
            "live_money_eligible": False,
        }
    return {
        "classification": (
            "TRACK_B_PHASE1_RECONCILIATION_REFRESH_CLEAN"
            if reconciliation.get("broker_reconciled") is True
            else "TRACK_B_PHASE1_RECONCILIATION_REFRESH_BLOCKED"
        ),
        "broker_truth_refresh_classification": broker_status.get("classification"),
        "broker_truth_refresh_generated_at": broker_status.get("generated_at"),
        "reconciliation_classification": reconciliation.get("classification"),
        "reconciliation_generated_at": reconciliation.get("generated_at"),
        "broker_reconciled": reconciliation.get("broker_reconciled"),
        "review_required_count": reconciliation.get("review_required_count"),
        "track_b_broker_open_order_count": reconciliation.get("track_b_broker_open_order_count"),
        "track_b_broker_position_count": reconciliation.get("track_b_broker_position_count"),
        "live_money_eligible": False,
    }


def _phase1_reconciliation_gate_check(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    phase1_reconciliation_gate: dict[str, Any],
) -> dict[str, Any]:
    check = _check(
        "phase1_broker_reconciliation_submit_gate",
        (not config.submit) or bool(phase1_reconciliation_gate.get("ready")),
        True,
        str(phase1_reconciliation_gate.get("detail") or "Current Phase-1 broker reconciliation is required."),
    )
    for key in (
        "stale_reconciliation_refresh_attempted",
        "stale_reconciliation_refresh_result",
        "refreshed_reconciliation_age",
        "exit_allowed_after_refresh",
        "exit_block_reason",
    ):
        check[key] = phase1_reconciliation_gate.get(key)
    return check


def _runtime_exit_override_identity_is_current(*, config: IbkrPaperStrategyBridgeConfig) -> bool:
    metadata = dict(config.caller_metadata or {})
    runtime_pid = _int_or_none(metadata.get("runtime_pid") or metadata.get("source_runtime_pid"))
    if runtime_pid is None or runtime_pid != os.getpid():
        return False
    runtime_cwd = str(metadata.get("runtime_cwd") or metadata.get("source_runtime_cwd") or "").strip()
    if not runtime_cwd:
        return False
    try:
        return Path(runtime_cwd).resolve() == Path(config.repo_root).resolve()
    except OSError:
        return False


def _paper_strategy_monitor_authority_for_route(
    *,
    monitor_status: dict[str, Any],
    expected_target: dict[str, Any],
    strategy_id: str,
) -> dict[str, Any]:
    exact_contract = dict(monitor_status.get("exact_contract") or {})
    scoped = (
        bool(monitor_status)
        and str(monitor_status.get("strategy_id") or "").strip() == str(strategy_id or "").strip()
        and _monitor_exact_contract_matches_target(monitor_exact_contract=exact_contract, target=expected_target)
    )
    if scoped:
        return {
            "authoritative": True,
            "detail": "Legacy paper strategy monitor is scoped to this exact route and may participate as an additional gate.",
        }
    return {
        "authoritative": False,
        "detail": (
            "Legacy paper strategy monitor is diagnostic for this route; current Phase-1 broker reconciliation "
            "is the authoritative open-position/open-order/review-required gate."
        ),
    }


def _paper_strategy_monitor_submit_gate_passed(monitor_status: dict[str, Any]) -> bool:
    if bool(monitor_status.get("submit_allowed")):
        return True
    detail = str(monitor_status.get("detail") or "").strip()
    block_reasons = [str(reason or "").strip() for reason in list(monitor_status.get("block_reasons") or []) if str(reason or "").strip()]
    health = str(monitor_status.get("health_classification") or monitor_status.get("monitor_health") or "").strip().upper()
    broker_quantity = float(
        monitor_status.get("broker_position_quantity")
        if monitor_status.get("broker_position_quantity") is not None
        else monitor_status.get("current_broker_mgc_position")
        if monitor_status.get("current_broker_mgc_position") is not None
        else 0.0
    )
    open_order_count = int(
        monitor_status.get("open_order_count")
        if monitor_status.get("open_order_count") is not None
        else monitor_status.get("open_mgc_orders")
        if monitor_status.get("open_mgc_orders") is not None
        else 0
    )
    broker_ledger_match = str(monitor_status.get("broker_ledger_match") or "").strip().upper()
    preserved_flat_ownership_detail = (
        "Preserved ATP ownership on the reconciled flat paper position" in detail
    )
    return (
        preserved_flat_ownership_detail
        and not block_reasons
        and bool(monitor_status.get("monitor_running"))
        and health == "HEALTHY"
        and broker_quantity == 0.0
        and open_order_count == 0
        and broker_ledger_match in {"", "MATCH"}
    )


def _paper_strategy_monitor_submit_gate_detail(monitor_status: dict[str, Any]) -> str:
    detail = str(monitor_status.get("detail") or "").strip()
    if _paper_strategy_monitor_submit_gate_passed(monitor_status):
        if "Preserved ATP ownership on the reconciled flat paper position" in detail:
            return (
                "Paper strategy monitor allows submit; preserved ATP ownership detail on a "
                "reconciled flat paper position is informational only."
            )
        return detail or "Paper strategy monitor allows submit."
    return detail or (
        f"Paper strategy monitor blocked submit: {', '.join(list(monitor_status.get('block_reasons') or [])) or 'unknown_reason'}"
    )


def _delegate_to_manual_harness(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    intent: IbkrPaperStrategyOrderIntent,
    exit_attempt_policy: ExitAttemptPolicy,
    entry_execution_pricing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manual_frozen_preview_path = config.manual_frozen_preview_path
    approval_digest = config.approval_digest
    approval_phrase = config.approval_phrase
    runtime_route = _authorized_supervised_runtime_route_check(config=config, intent=intent)
    supervised_runtime_route = bool(runtime_route.get("passed"))
    caller_path = str(config.caller_path or "").strip()
    leak_test_route = caller_path == _LEAK_TEST_CALLER_PATH and bool(
        _leak_test_authorization_check(config=config, intent=intent).get("passed")
    )
    if manual_frozen_preview_path is None or approval_digest is None or approval_phrase is None:
        if not supervised_runtime_route and not leak_test_route:
            raise IbkrPaperStrategyBridgeError(
                "Bridge submit delegation requires a previously generated manual-harness frozen preview plus the exact approval digest and approval phrase."
            )
        prepared = _prepare_manual_submit_bundle(
            config=config,
            intent=intent,
            exit_attempt_policy=exit_attempt_policy,
            entry_execution_pricing=entry_execution_pricing,
            stack_provider=(lambda: []),
        )
        manual_frozen_preview_path = Path(str(prepared.get("frozen_preview_path") or ""))
        approval_digest = str(prepared.get("preview_digest") or "")
        approval_phrase = str(prepared.get("expected_approval_phrase") or "")
        if not manual_frozen_preview_path.exists() or not approval_digest or not approval_phrase:
            raise IbkrPaperStrategyBridgeError(
                "Approved supervised PAPER runtime/leak-test route could not prepare a valid internal frozen preview bundle."
            )
    test_mode = "PAPER_FILL_TEST" if intent.action == "BUY" else "PAPER_CLOSE_TEST"
    delegated_output_dir = (Path(config.output_dir) / "delegated_manual_harness") if config.output_dir is not None else None
    expected_target = _bridge_phase1_target(config=config, intent=intent)
    limit_override = _entry_limit_override(entry_execution_pricing)
    fill_timeout_seconds = _bridge_fill_timeout_seconds(
        exit_attempt_policy=exit_attempt_policy,
        entry_execution_pricing=entry_execution_pricing,
    )
    limit_offset_ticks = _bridge_limit_offset_ticks(
        exit_attempt_policy=exit_attempt_policy,
        entry_execution_pricing=entry_execution_pricing,
    )
    manual_config = IbkrManualPaperSubmitConfig(
        repo_root=config.repo_root,
        mode=config.mode,
        host=config.host,
        port=config.port,
        client_id=_manual_harness_client_id(config.client_id),
        account_id=config.account_id,
        symbol=str(expected_target.get("symbol") or intent.symbol or "").strip().upper(),
        expiry=str(expected_target.get("contract_month") or intent.contract_month or "").strip(),
        action=intent.action,
        quantity=float(intent.quantity),
        order_type=_EXPECTED_ORDER_TYPE,
        limit_price=limit_override,
        time_in_force=_EXPECTED_TIF,
        test_mode=test_mode,
        timeout_seconds=float(config.timeout_seconds),
        fill_timeout_seconds=fill_timeout_seconds,
        post_approval_observation_seconds=75.0,
        fill_limit_offset_ticks=limit_offset_ticks,
        manual_confirmation_timeout_seconds=90.0,
        caller_path="manual_cli",
        submit=True,
        approval_digest=approval_digest,
        approval_phrase=approval_phrase,
        output_dir=delegated_output_dir,
        frozen_preview_path=Path(manual_frozen_preview_path),
        diagnostic_dry_run=False,
        execution_pricing_context=entry_execution_pricing if limit_override is not None else None,
    )
    manual_confirmation_fn = _supervised_runtime_manual_confirmation if supervised_runtime_route else None
    delegated = run_ibkr_manual_paper_submit_test(
        config=manual_config,
        stack_provider=(lambda: []) if supervised_runtime_route else inspect.stack,
        manual_confirmation_fn=manual_confirmation_fn,
    )
    return {
        "classification": delegated.classification,
        "detail": delegated.report.get("lifecycle", {}).get("detail"),
        "report": delegated.report,
        "exit_attempt_policy": exit_attempt_policy.to_json_dict(),
        "entry_execution_pricing": entry_execution_pricing,
    }


def _supervised_runtime_manual_confirmation(
    *,
    timeout_seconds: float,
    order_id: int,
    preview_digest: str,
) -> dict[str, Any]:
    del timeout_seconds
    return {
        "state": "SUBMIT_SENT_AWAITING_TWS_MANUAL_CONFIRMATION",
        "operator_outcome": "approved",
        "response_text": None,
        "detail": (
            "Approved supervised PAPER runtime route skipped the legacy manual-harness TTY prompt; "
            "broker truth still verifies the submitted paper order before success."
        ),
        "timed_out": False,
        "order_id": int(order_id),
        "preview_digest": str(preview_digest),
        "source": "approved_supervised_paper_runtime_route",
    }


def _map_delegate_classification(delegated_result: dict[str, Any] | None) -> str:
    if not isinstance(delegated_result, dict):
        return "PAPER_STRATEGY_BRIDGE_READY"
    entry_execution_pricing = dict(delegated_result.get("entry_execution_pricing") or {})
    entry_intent = str(entry_execution_pricing.get("entry_execution_intent") or "").strip().upper()
    delegated_classification = str(delegated_result.get("classification") or "").strip().upper()
    delegated_report = dict(delegated_result.get("report") or {})
    delegated_lifecycle = dict(
        delegated_report.get("submit_cancel_lifecycle")
        or delegated_report.get("lifecycle")
        or {}
    )
    delegated_status = str(delegated_lifecycle.get("status") or "").strip().lower()
    if delegated_classification.endswith("_PASSED") or delegated_status in {"filled", "filled_flat", "passed"}:
        return "PAPER_STRATEGY_ORDER_FILLED"
    if "NOT_FILLED_CANCELLED" in delegated_classification or delegated_status in {
        "fill_timeout_cancelled",
        "partial_fill_cancelled",
        "manual_confirmation_timeout_order_cancelled",
    }:
        if entry_intent in {_ENTRY_INTENT_RESTING_PULLBACK_LIMIT, _ENTRY_INTENT_PASSIVE_ONLY}:
            return "PAPER_STRATEGY_ENTRY_MISSED_CANCELLED_ACCEPTED"
        return "PAPER_STRATEGY_ORDER_NOT_FILLED_CANCELLED"
    if "REJECTED" in delegated_classification:
        return "PAPER_STRATEGY_ORDER_REJECTED"
    if "WORKING" in delegated_classification or delegated_status in {"working_submitted", "submitted"}:
        return "PAPER_STRATEGY_ORDER_WORKING"
    if "BLOCKED" in delegated_classification or delegated_status in {"blocked", "approval_blocked", "preview_only"}:
        return "PAPER_STRATEGY_INTENT_BLOCKED"
    if delegated_status in {
        "manual_confirmation_timeout",
        "manual_confirmation_unavailable",
        "manual_confirmation_rejected_no_order",
        "manual_confirmation_rejected_order_cancelled",
        "submit_verification_failed",
        "approval_invalidated",
        "unknown_needs_review",
    }:
        return "PAPER_STRATEGY_NEEDS_MANUAL_REVIEW"
    return "PAPER_STRATEGY_RECONCILIATION_FAILED"


def _build_report(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    classification: str,
    started_at: datetime,
    intent: IbkrPaperStrategyOrderIntent,
    caller_gate: dict[str, Any],
    environment_lock: dict[str, Any],
    paper_strategy_monitor_status: dict[str, Any],
    paper_strategy_governance_status: dict[str, Any],
    paper_strategy_exposure_status: dict[str, Any],
    account_truth: dict[str, Any],
    selected_account_id: str,
    positions: dict[str, Any],
    open_orders: dict[str, Any],
    quote_context: dict[str, Any],
    exact_contract_report: dict[str, Any],
    qualified_contract_report: dict[str, Any],
    current_position_quantity: float | None,
    preflight_checks: list[dict[str, Any]],
    prepared_submit_bundle: dict[str, Any] | None,
    delegated_result: dict[str, Any] | None,
    exit_attempt_policy: ExitAttemptPolicy,
    entry_attempt_memory: dict[str, Any],
    entry_execution_pricing: dict[str, Any],
    callback_timeline_event_count: int,
    errors: list[dict[str, Any]],
    connection_diagnostics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    strategy_identity = _resolve_strategy_identity(intent.strategy_id)
    return {
        "classification": classification,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "started_at": started_at.isoformat(),
        "environment": {
            "mode": config.mode,
            "host": config.host,
            "port": config.port,
            "client_id": config.client_id,
            "account_id": config.account_id,
            "read_only_preflight": True,
            "timeout_seconds": config.timeout_seconds,
        },
        "connection_diagnostics": connection_diagnostics or _bridge_connection_diagnostics(config=config),
        "intent": intent.to_dict(),
        "strategy_identity": strategy_identity,
        "selected_account_id": selected_account_id,
        "caller_gate": caller_gate,
        "caller_metadata": dict(config.caller_metadata or {}),
        "environment_lock_check": environment_lock,
        "paper_strategy_monitor_status": paper_strategy_monitor_status,
        "paper_strategy_governance_status": paper_strategy_governance_status,
        "paper_strategy_exposure_status": paper_strategy_exposure_status,
        "account_truth": account_truth,
        "positions": positions,
        "open_orders": open_orders,
        "quote_context": quote_context,
        "exact_contract_report": exact_contract_report,
        "qualified_contract_report": {
            "ok": qualified_contract_report.get("ok"),
            "detail": qualified_contract_report.get("detail"),
            "qualified_contract": qualified_contract_report.get("qualified_contract"),
            "qualified_contract_identifier": qualified_contract_report.get("qualified_contract_identifier"),
            "api_contract_details": qualified_contract_report.get("api_contract_details"),
        },
        "current_position_quantity": current_position_quantity,
        "preflight_checks": preflight_checks,
        "exit_attempt_policy": exit_attempt_policy.to_json_dict(),
        "entry_attempt_memory": entry_attempt_memory,
        "entry_execution_pricing": entry_execution_pricing,
        "exit_execution_pricing": entry_execution_pricing if bool(entry_execution_pricing.get("is_close")) else None,
        "prepared_submit_bundle": prepared_submit_bundle,
        "delegated_result": delegated_result,
        "callback_timeline_event_count": callback_timeline_event_count,
        "errors": errors,
    }


def _bridge_connection_diagnostics(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    runtime: _Runtime | None = None,
    exc: BaseException | None = None,
) -> dict[str, Any]:
    collector = getattr(runtime, "collector", None) if runtime is not None else None
    transport = getattr(runtime, "transport", None) if runtime is not None else None
    session = getattr(runtime, "session", None) if runtime is not None else None
    session_state = getattr(session, "state", None) if session is not None else None
    errors = list(getattr(collector, "errors", []) or []) if collector is not None else []
    latest_error = None
    latest_error_fn = getattr(collector, "latest_error", None) if collector is not None else None
    if callable(latest_error_fn):
        try:
            latest_error = latest_error_fn(codes=_SEVERE_CONNECTION_ERROR_CODES) or latest_error_fn()
        except Exception:
            latest_error = None
    return {
        "config_source": "IbkrPaperStrategyBridgeConfig",
        "mode": config.mode,
        "host": config.host,
        "port": int(config.port),
        "client_id": int(config.client_id),
        "account_id": config.account_id,
        "caller_path": config.caller_path,
        "caller_type": str((config.caller_metadata or {}).get("caller_type") or "").strip() or None,
        "read_only": True,
        "read_only_preflight": True,
        "timeout_seconds": float(config.timeout_seconds),
        "session_read_only": getattr(session_state, "read_only", None),
        "session_gateway_mode": getattr(session_state, "gateway_mode", None),
        "session_live_orders_enabled": bool(getattr(getattr(session, "order_id_policy", None), "live_orders_enabled", False)),
        "transport_class": type(transport).__name__ if transport is not None else None,
        "collector_class": type(collector).__name__ if collector is not None else None,
        "server_version": _safe_transport_call(transport, "server_version"),
        "tws_connection_time": _safe_transport_call(transport, "tws_connection_time"),
        "connection_error_type": type(exc).__name__ if exc is not None else None,
        "connection_error_message": str(exc) if exc is not None else None,
        "latest_error": latest_error,
        "error_count": len(errors),
    }


def _safe_transport_call(transport: Any, method_name: str) -> Any:
    method = getattr(transport, method_name, None)
    if not callable(method):
        return None
    try:
        return method()
    except Exception:
        return None


def _resolve_strategy_identity(strategy_id: str) -> dict[str, Any]:
    try:
        identity = get_shared_strategy_identity(strategy_id)
    except KeyError:
        return {"strategy_id": strategy_id, "known_identity": False}
    return {
        "strategy_id": identity.identity_id,
        "display_name": identity.display_name,
        "strategy_family": identity.strategy_family,
        "strategy_identity_root": identity.strategy_identity_root,
        "source_symbol": identity.symbol,
        "allowed_sessions": list(identity.allowed_sessions),
        "known_identity": True,
    }


def _submitted_order_count(audit_events: list[dict[str, Any]]) -> int:
    return sum(1 for row in audit_events if str(row.get("event_type") or "").strip() == "delegated_manual_harness_completed")


def _persist_known_managed_exit_order_after_submit(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    intent: IbkrPaperStrategyOrderIntent,
    delegated_result: dict[str, Any] | None,
    qualified_contract_report: dict[str, Any],
    entry_execution_pricing: dict[str, Any],
    exit_attempt_policy: ExitAttemptPolicy | None,
) -> dict[str, Any] | None:
    if not config.submit or not _is_close_intent(config=config, intent=intent):
        return None
    delegated = dict(delegated_result or {})
    delegated_report = dict(delegated.get("report") or {})
    lifecycle = dict(
        delegated_report.get("submit_cancel_lifecycle")
        or delegated_report.get("lifecycle")
        or delegated.get("submit_cancel_lifecycle")
        or {}
    )
    status = str(lifecycle.get("status") or delegated.get("status") or "").strip().lower()
    if status in {
        "filled",
        "filled_flat",
        "partial_fill_cancelled",
        "fill_timeout_cancelled",
        "manual_confirmation_timeout_order_cancelled",
        "manual_confirmation_rejected_no_order",
        "rejected",
        "cancel_verification_failed",
    }:
        return None
    broker_order_id = _submitted_broker_order_id(delegated=delegated, delegated_report=delegated_report, lifecycle=lifecycle)
    if broker_order_id is None:
        return None
    metadata = dict(config.caller_metadata or {})
    qualified = dict(qualified_contract_report.get("qualified_contract") or {})
    open_after_submit = dict(lifecycle.get("open_order_after_submit") or {})
    client_id = _int_or_none(
        lifecycle.get("client_id")
        or delegated_report.get("client_id")
        or delegated.get("client_id")
        or open_after_submit.get("client_id")
        or config.client_id
    )
    perm_id = _int_or_none(
        lifecycle.get("submitted_perm_id")
        or lifecycle.get("perm_id")
        or delegated_report.get("submitted_perm_id")
        or delegated_report.get("perm_id")
        or delegated.get("submitted_perm_id")
        or delegated.get("perm_id")
    )
    now = datetime.now(timezone.utc)
    state_path = _known_managed_exit_state_path(config.repo_root)
    existing = _load_known_managed_exit_state(state_path)
    order_row = {
        "managed_order_status": "KNOWN_MANAGED_EXIT_ORDER_WORKING",
        "source": "IBKR_PAPER_STRATEGY_BRIDGE_DELEGATED_CLOSE_SUBMIT",
        "source_artifact_path": str(_bridge_report_path(config)),
        "lifecycle_id": str(
            metadata.get("lifecycle_id")
            or metadata.get("position_lifecycle_id")
            or metadata.get("managed_lifecycle_id")
            or ""
        )
        or None,
        "strategy_id": str(metadata.get("strategy_id") or _exposure_strategy_id_for_bridge(config=config) or "").strip() or None,
        "lane_id": str(metadata.get("lane_id") or config.strategy_id or "").strip() or None,
        "account_id": str(metadata.get("account_id") or config.account_id or "").strip() or None,
        "symbol": str(qualified.get("symbol") or config.symbol or intent.symbol or "").strip().upper() or None,
        "local_symbol": str(metadata.get("local_symbol") or qualified.get("local_symbol") or "").strip() or None,
        "expiry": str(qualified.get("expiry") or config.contract_month or intent.contract_month or "").strip() or None,
        "con_id": _int_or_none(metadata.get("con_id") or qualified.get("con_id")),
        "action": str(intent.action or config.action or "").strip().upper() or None,
        "qty": float(config.quantity or intent.quantity or 0.0),
        "quantity": float(config.quantity or intent.quantity or 0.0),
        "order_type": str(config.order_type or intent.order_type or "").strip().upper() or None,
        "limit_price": entry_execution_pricing.get("limit_price"),
        "stop_price": entry_execution_pricing.get("stop_price"),
        "tif": str(config.time_in_force or intent.time_in_force or "").strip().upper() or None,
        "broker_order_id": str(broker_order_id),
        "client_id": client_id,
        "perm_id": perm_id,
        "submitted_at": now.isoformat(),
        "exit_reason": str(config.reason or intent.reason or "").strip() or None,
        "exit_urgency": None if exit_attempt_policy is None else exit_attempt_policy.exit_urgency,
        "hard_exit": None if exit_attempt_policy is None else exit_attempt_policy.hard_exit,
        "discretionary_exit": None if exit_attempt_policy is None else exit_attempt_policy.discretionary_exit,
        "delegated_classification": delegated.get("classification"),
        "delegated_status": status or None,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
    }
    rows = [
        dict(row)
        for row in list(existing.get("known_managed_exit_orders") or [])
        if str(row.get("broker_order_id") or row.get("order_id") or "") != str(broker_order_id)
    ]
    rows.append(order_row)
    payload = {
        "schema_version": "track_b_known_managed_exit_orders_v1",
        "generated_at": now.isoformat(),
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "known_managed_exit_orders": rows,
        "resolved_known_managed_exit_orders": list(existing.get("resolved_known_managed_exit_orders") or []),
    }
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "persisted": True,
        "path": str(state_path),
        "broker_order_id": str(broker_order_id),
        "client_id": client_id,
        "perm_id": perm_id,
        "lifecycle_id": order_row["lifecycle_id"],
        "managed_order_status": order_row["managed_order_status"],
    }


def _submitted_broker_order_id(
    *,
    delegated: dict[str, Any],
    delegated_report: dict[str, Any],
    lifecycle: dict[str, Any],
) -> int | None:
    return _int_or_none(
        lifecycle.get("submitted_order_id")
        or lifecycle.get("order_id")
        or lifecycle.get("broker_order_id")
        or delegated_report.get("submitted_order_id")
        or delegated_report.get("order_id")
        or delegated.get("submitted_order_id")
        or delegated.get("broker_order_id")
    )


def _known_managed_exit_state_path(repo_root: Path) -> Path:
    return repo_root / _KNOWN_MANAGED_EXIT_STATE_PATH


def _bridge_report_path(config: IbkrPaperStrategyBridgeConfig) -> Path:
    output_dir = Path(config.output_dir or Path("outputs") / "reports" / _ARTIFACT_STEM)
    if not output_dir.is_absolute():
        output_dir = config.repo_root / output_dir
    return output_dir / f"{_ARTIFACT_STEM}_report.json"


def _load_known_managed_exit_state(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_bridge_audit_history(output_dir: Path | None) -> list[dict[str, Any]]:
    if output_dir is None:
        return []
    path = Path(output_dir) / f"{_ARTIFACT_STEM}_audit.jsonl"
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                try:
                    row = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict):
                    rows.append(row)
    except OSError:
        return []
    return rows


def _exit_attempt_policy_for_bridge(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    intent: IbkrPaperStrategyOrderIntent,
    history_events: list[dict[str, Any]],
    current_position_quantity: float | None,
    open_orders: dict[str, Any],
    phase1_gate: dict[str, Any],
) -> ExitAttemptPolicy:
    metadata = dict(config.caller_metadata or {})
    intent_type = str(metadata.get("intent_type") or "").strip().upper()
    lifecycle_id = str(
        metadata.get("lifecycle_id")
        or metadata.get("position_lifecycle_id")
        or metadata.get("managed_lifecycle_id")
        or ""
    ).strip() or None
    hard_exit = _hard_exit_from_metadata(config=config, metadata=metadata)
    discretionary_exit = _bool_or_none(metadata.get("discretionary_exit"))
    reason_values = _exit_reason_values_from_metadata(config=config, metadata=metadata)
    return classify_exit_attempt_policy(
        history_events=history_events,
        lifecycle_id=lifecycle_id,
        intent_type=intent_type,
        action=intent.action,
        hard_exit=hard_exit,
        discretionary_exit=discretionary_exit,
        exit_reasons=reason_values,
        exit_reason_source="bridge_caller_metadata",
        broker_position_quantity=current_position_quantity,
        broker_reconciled=bool(phase1_gate.get("ready")),
        open_order_count=int(open_orders.get("open_order_count") or 0),
    )


def _build_entry_attempt_memory(
    *,
    history_events: list[dict[str, Any]],
    config: IbkrPaperStrategyBridgeConfig,
    intent: IbkrPaperStrategyOrderIntent,
) -> dict[str, Any]:
    if not _is_entry_intent(config=config, intent=intent):
        return {
            "entry_attempt_count": 0,
            "not_filled_cancelled_count": 0,
            "last_cancel_reason": None,
            "same_setup_retry_count": 0,
            "working_order_duplicate_block": False,
        }
    metadata = dict(config.caller_metadata or {})
    lane_id = str(metadata.get("lane_id") or config.strategy_id or intent.strategy_id or "").strip()
    setup_family = str(
        metadata.get("signal_family")
        or metadata.get("setup_family")
        or metadata.get("entry_family")
        or config.reason
        or ""
    ).strip()
    attempted: list[dict[str, Any]] = []
    cancelled: list[dict[str, Any]] = []
    same_setup = 0
    for row in history_events:
        if str(row.get("event_type") or "").strip() != "delegated_manual_harness_completed":
            continue
        row_intent = dict(row.get("intent") or {})
        row_metadata = dict(row.get("caller_metadata") or {})
        row_strategy = str(row_intent.get("strategy_id") or row.get("strategy_id") or "").strip()
        row_lane = str(row_metadata.get("lane_id") or row_strategy or "").strip()
        row_intent_type = str(row_metadata.get("intent_type") or "").strip().upper()
        if row_strategy != intent.strategy_id or row_lane != lane_id:
            continue
        if row_intent_type not in {"BUY_TO_OPEN", "SELL_TO_OPEN", ""}:
            continue
        row_action = str(row_intent.get("action") or row.get("action") or "").strip().upper()
        if row_action != intent.action:
            continue
        attempted.append(dict(row))
        row_setup = str(
            row_metadata.get("signal_family")
            or row_metadata.get("setup_family")
            or row_metadata.get("entry_family")
            or row_intent.get("reason")
            or ""
        ).strip()
        if setup_family and row_setup == setup_family:
            same_setup += 1
        classification = str(row.get("delegated_classification") or "").strip().upper()
        if "NOT_FILLED_CANCELLED" in classification:
            cancelled.append(dict(row))
    last_cancel = cancelled[-1] if cancelled else {}
    return {
        "strategy_id": intent.strategy_id,
        "lane_id": lane_id,
        "entry_family": setup_family or None,
        "entry_attempt_count": len(attempted),
        "not_filled_cancelled_count": len(cancelled),
        "last_cancel_reason": (
            str(last_cancel.get("delegated_classification") or "").strip() or None
        ),
        "same_setup_retry_count": same_setup,
        "working_order_duplicate_block": False,
    }


def _entry_execution_pricing_for_bridge(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    intent: IbkrPaperStrategyOrderIntent,
    quote_context: dict[str, Any],
    qualified_contract_report: dict[str, Any],
    entry_attempt_memory: dict[str, Any] | None = None,
    exit_attempt_policy: ExitAttemptPolicy | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    if _is_close_intent(config=config, intent=intent):
        if exit_attempt_policy is None:
            metadata = dict(config.caller_metadata or {})
            exit_attempt_policy = classify_exit_attempt_policy(
                history_events=[],
                lifecycle_id=str(metadata.get("lifecycle_id") or "") or None,
                intent_type=str(metadata.get("intent_type") or "").strip().upper() or None,
                action=intent.action,
                hard_exit=_hard_exit_from_metadata(config=config, metadata=metadata),
                exit_reasons=_exit_reason_values_from_metadata(config=config, metadata=metadata),
                exit_reason_source="bridge_caller_metadata",
            )
        return _exit_execution_pricing_for_bridge(
            config=config,
            intent=intent,
            quote_context=quote_context,
            qualified_contract_report=qualified_contract_report,
            exit_attempt_policy=exit_attempt_policy,
            now=now,
        )
    if not _is_entry_intent(config=config, intent=intent):
        return {
            "is_entry": False,
            "execution_policy": "NOT_ENTRY",
            "block_submit": False,
            "block_reason": None,
            "live_money_eligible": False,
            "entry_attempt_memory": dict(entry_attempt_memory or {}),
        }
    now = now or datetime.now(timezone.utc)
    policy = _entry_execution_policy(config=config, intent=intent)
    min_tick = _qualified_contract_min_tick(qualified_contract_report) or 0.25
    action = str(intent.action or "").strip().upper()
    delayed_bid = _float_or_none(quote_context.get("bid_price"))
    delayed_ask = _float_or_none(quote_context.get("ask_price"))
    delayed_last = _float_or_none(quote_context.get("last_price"))
    delayed_reference = delayed_ask if action == "BUY" else delayed_bid
    if delayed_reference is None:
        delayed_reference = delayed_last
    delayed_quote_limit = None
    if delayed_reference is not None:
        offset = _ENTRY_RUNTIME_LIMIT_OFFSET_TICKS * float(min_tick)
        delayed_quote_limit = (
            float(delayed_reference) + offset
            if action == "BUY"
            else float(delayed_reference) - offset
        )
        delayed_quote_limit = _round_price_to_tick(delayed_quote_limit, float(min_tick))
    runtime_snapshot = _load_entry_runtime_market_snapshot(
        repo_root=config.repo_root,
        symbol=str(intent.symbol or config.symbol or "").strip().upper(),
        now=now,
    )
    runtime_price = _float_or_none(runtime_snapshot.get("runtime_last_or_close"))
    runtime_fresh = bool(runtime_snapshot.get("runtime_data_fresh")) and runtime_price is not None
    marketable_policy = policy in {
        _ENTRY_POLICY_MARKETABLE_RUNTIME,
        _ENTRY_POLICY_AGGRESSIVE_WITH_CAP,
        _ENTRY_POLICY_BLOCK_IF_ONLY_DELAYED,
    }
    entry_intent = _entry_execution_intent(config=config, intent=intent)
    strategy_limit_price = _strategy_entry_limit_price(config=config)
    pullback_offset_points = _strategy_entry_pullback_offset_points(config=config, min_tick=float(min_tick))
    fill_timeout_seconds = _entry_fill_timeout_seconds(config=config, entry_intent=entry_intent)
    selected_limit = None
    execution_price_source = None
    block_submit = False
    block_reason = None
    limit_offset_ticks = _ENTRY_RUNTIME_LIMIT_OFFSET_TICKS
    if policy == _ENTRY_POLICY_AGGRESSIVE_WITH_CAP:
        limit_offset_ticks = min(_ENTRY_AGGRESSIVE_LIMIT_OFFSET_TICKS, _ENTRY_MAX_LIMIT_OFFSET_TICKS)
    if entry_intent in {_ENTRY_INTENT_RESTING_PULLBACK_LIMIT, _ENTRY_INTENT_PASSIVE_ONLY}:
        if strategy_limit_price is not None:
            selected_limit = _round_price_to_tick(float(strategy_limit_price), float(min_tick))
            execution_price_source = _ENTRY_STRATEGY_DEFINED_LIMIT_SOURCE
        elif runtime_fresh and pullback_offset_points is not None:
            selected_limit = (
                float(runtime_price) - float(pullback_offset_points)
                if action == "BUY"
                else float(runtime_price) + float(pullback_offset_points)
            )
            selected_limit = _round_price_to_tick(selected_limit, float(min_tick))
            execution_price_source = _ENTRY_RESTING_RUNTIME_PULLBACK_SOURCE
        elif entry_intent == _ENTRY_INTENT_RESTING_PULLBACK_LIMIT:
            block_submit = True
            block_reason = "RESTING_ENTRY_LIMIT_NOT_DEFINED"
            execution_price_source = "UNKNOWN"
        else:
            block_submit = True
            block_reason = "PASSIVE_ENTRY_LIMIT_NOT_DEFINED"
            execution_price_source = "UNKNOWN"
    elif marketable_policy and runtime_fresh:
        selected_limit = (
            float(runtime_price) + limit_offset_ticks * float(min_tick)
            if action == "BUY"
            else float(runtime_price) - limit_offset_ticks * float(min_tick)
        )
        selected_limit = _round_price_to_tick(selected_limit, float(min_tick))
        execution_price_source = _ENTRY_RUNTIME_PRICE_SOURCE
    elif marketable_policy:
        block_submit = True
        block_reason = "DELAYED_QUOTE_NOT_EXECUTION_SAFE"
        execution_price_source = _ENTRY_DELAYED_DIAGNOSTIC_SOURCE
    elif delayed_quote_limit is not None:
        execution_price_source = "IBKR_DELAYED_PASSIVE_DIAGNOSTIC"
    else:
        execution_price_source = "UNKNOWN"
    selected_limit_vs_runtime = (
        None
        if selected_limit is None or runtime_price is None
        else _signed_limit_distance(action=action, limit_price=selected_limit, reference_price=runtime_price)
    )
    selected_limit_vs_delayed_ask = (
        None
        if selected_limit is None or delayed_ask is None
        else float(selected_limit) - float(delayed_ask)
    )
    delayed_limit_vs_runtime = (
        None
        if delayed_quote_limit is None or runtime_price is None
        else _signed_limit_distance(action=action, limit_price=delayed_quote_limit, reference_price=runtime_price)
    )
    delayed_limit_vs_delayed_ask = (
        None
        if delayed_quote_limit is None or delayed_ask is None
        else float(delayed_quote_limit) - float(delayed_ask)
    )
    marketable_by_runtime = (
        selected_limit_vs_runtime is not None and float(selected_limit_vs_runtime) >= 0.0
    )
    marketable_by_delayed = (
        (selected_limit_vs_delayed_ask is not None and float(selected_limit_vs_delayed_ask) >= 0.0)
        if action == "BUY"
        else (
            delayed_bid is not None
            and selected_limit is not None
            and float(delayed_bid) - float(selected_limit) >= 0.0
        )
    )
    delayed_candidate_marketable_by_runtime = (
        delayed_limit_vs_runtime is not None and float(delayed_limit_vs_runtime) >= 0.0
    )
    delayed_candidate_marketable_by_delayed = (
        (delayed_limit_vs_delayed_ask is not None and float(delayed_limit_vs_delayed_ask) >= 0.0)
        if action == "BUY"
        else (
            delayed_bid is not None
            and delayed_quote_limit is not None
            and float(delayed_bid) - float(delayed_quote_limit) >= 0.0
        )
    )
    return {
        "is_entry": True,
        "entry_execution_intent": entry_intent,
        "execution_policy": policy,
        "execution_price_source": execution_price_source,
        "broker_quote_type": _broker_quote_type(quote_context),
        "delayed_bid": delayed_bid,
        "delayed_ask": delayed_ask,
        "delayed_last": delayed_last,
        "runtime_last_or_close": runtime_price,
        "runtime_candle_timestamp": runtime_snapshot.get("runtime_candle_timestamp"),
        "runtime_data_age_seconds": runtime_snapshot.get("runtime_data_age_seconds"),
        "runtime_data_fresh": runtime_fresh,
        "runtime_source_artifact_path": runtime_snapshot.get("source_artifact_path"),
        "limit_price": selected_limit,
        "strategy_defined_limit_price": strategy_limit_price,
        "pullback_offset_points": pullback_offset_points,
        "limit_vs_runtime_price_points": selected_limit_vs_runtime,
        "limit_vs_broker_delayed_ask_points": selected_limit_vs_delayed_ask,
        "marketable_by_runtime_context": marketable_by_runtime,
        "marketable_by_delayed_quote": marketable_by_delayed,
        "delayed_quote_limit_price": delayed_quote_limit,
        "delayed_quote_limit_vs_runtime_price_points": delayed_limit_vs_runtime,
        "delayed_quote_limit_vs_broker_delayed_ask_points": delayed_limit_vs_delayed_ask,
        "delayed_quote_limit_marketable_by_runtime_context": delayed_candidate_marketable_by_runtime,
        "delayed_quote_limit_marketable_by_delayed_quote": delayed_candidate_marketable_by_delayed,
        "limit_offset_ticks": limit_offset_ticks if selected_limit is not None else None,
        "max_limit_offset_ticks": _ENTRY_MAX_LIMIT_OFFSET_TICKS,
        "fill_timeout_seconds": fill_timeout_seconds,
        "working_window_seconds": fill_timeout_seconds,
        "cancel_on": _entry_cancel_reasons(entry_intent),
        "passive_miss_is_failure": entry_intent not in {
            _ENTRY_INTENT_RESTING_PULLBACK_LIMIT,
            _ENTRY_INTENT_PASSIVE_ONLY,
        },
        "replace_policy": (
            "recompute_or_replace_only_while_setup_valid_with_max_chase_budget"
            if entry_intent == _ENTRY_INTENT_DYNAMIC_LIMIT_WITH_CHASE_CAP
            else None
        ),
        "block_submit": block_submit,
        "block_reason": block_reason,
        "live_money_eligible": False,
        "entry_attempt_memory": dict(entry_attempt_memory or {}),
    }


def _exit_execution_pricing_for_bridge(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    intent: IbkrPaperStrategyOrderIntent,
    quote_context: dict[str, Any],
    qualified_contract_report: dict[str, Any],
    exit_attempt_policy: ExitAttemptPolicy,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    min_tick = _qualified_contract_min_tick(qualified_contract_report) or 0.25
    action = str(intent.action or "").strip().upper()
    delayed_bid = _float_or_none(quote_context.get("bid_price"))
    delayed_ask = _float_or_none(quote_context.get("ask_price"))
    delayed_last = _float_or_none(quote_context.get("last_price"))
    delayed_reference = delayed_bid if action == "SELL" else delayed_ask
    if delayed_reference is None:
        delayed_reference = delayed_last
    delayed_quote_limit = None
    if delayed_reference is not None:
        offset = float(exit_attempt_policy.limit_offset_ticks) * float(min_tick)
        delayed_quote_limit = (
            float(delayed_reference) - offset
            if action == "SELL"
            else float(delayed_reference) + offset
        )
        delayed_quote_limit = _round_price_to_tick(delayed_quote_limit, float(min_tick))
    runtime_snapshot = _load_entry_runtime_market_snapshot(
        repo_root=config.repo_root,
        symbol=str(intent.symbol or config.symbol or "").strip().upper(),
        now=now,
    )
    runtime_price = _float_or_none(runtime_snapshot.get("runtime_last_or_close"))
    runtime_fresh = bool(runtime_snapshot.get("runtime_data_fresh")) and runtime_price is not None
    selected_limit = None
    execution_price_source = "UNKNOWN"
    block_submit = False
    block_reason = None
    if runtime_fresh:
        offset = float(exit_attempt_policy.limit_offset_ticks) * float(min_tick)
        selected_limit = (
            float(runtime_price) - offset
            if action == "SELL"
            else float(runtime_price) + offset
        )
        selected_limit = _round_price_to_tick(selected_limit, float(min_tick))
        execution_price_source = _ENTRY_RUNTIME_PRICE_SOURCE
    elif exit_attempt_policy.hard_exit:
        block_submit = True
        block_reason = "LOW_CONFIDENCE_PRICE_SOURCE"
        execution_price_source = _ENTRY_DELAYED_DIAGNOSTIC_SOURCE
    elif delayed_quote_limit is not None:
        execution_price_source = "IBKR_DELAYED_PASSIVE_DIAGNOSTIC"
    limit_vs_runtime = (
        None
        if selected_limit is None or runtime_price is None
        else _signed_limit_distance(action=action, limit_price=selected_limit, reference_price=runtime_price)
    )
    limit_vs_delayed = (
        None
        if selected_limit is None or delayed_reference is None
        else _signed_limit_distance(action=action, limit_price=selected_limit, reference_price=delayed_reference)
    )
    delayed_limit_vs_runtime = (
        None
        if delayed_quote_limit is None or runtime_price is None
        else _signed_limit_distance(action=action, limit_price=delayed_quote_limit, reference_price=runtime_price)
    )
    delayed_limit_vs_delayed = (
        None
        if delayed_quote_limit is None or delayed_reference is None
        else _signed_limit_distance(action=action, limit_price=delayed_quote_limit, reference_price=delayed_reference)
    )
    return {
        "is_entry": False,
        "is_close": True,
        "exit_urgency": exit_attempt_policy.exit_urgency,
        "hard_exit": exit_attempt_policy.hard_exit,
        "hard_exit_reason_matches": list(exit_attempt_policy.hard_exit_reason_matches),
        "exit_reason_source": exit_attempt_policy.exit_reason_source,
        "selected_exit_policy": exit_attempt_policy.execution_policy,
        "timeout_seconds": exit_attempt_policy.fill_timeout_seconds,
        "fill_timeout_seconds": exit_attempt_policy.fill_timeout_seconds,
        "escalation_level": exit_attempt_policy.escalation_level,
        "limit_offset_ticks": exit_attempt_policy.limit_offset_ticks,
        "execution_price_source": execution_price_source,
        "broker_quote_type": _broker_quote_type(quote_context),
        "delayed_bid": delayed_bid,
        "delayed_ask": delayed_ask,
        "delayed_last": delayed_last,
        "runtime_reference_price": runtime_price,
        "runtime_last_or_close": runtime_price,
        "runtime_candle_timestamp": runtime_snapshot.get("runtime_candle_timestamp"),
        "runtime_data_age_seconds": runtime_snapshot.get("runtime_data_age_seconds"),
        "runtime_data_fresh": runtime_fresh,
        "runtime_source_artifact_path": runtime_snapshot.get("source_artifact_path"),
        "limit_price": selected_limit,
        "limit_vs_runtime_price_points": limit_vs_runtime,
        "limit_vs_broker_delayed_quote_points": limit_vs_delayed,
        "marketable_by_runtime_context": limit_vs_runtime is not None and float(limit_vs_runtime) >= 0.0,
        "marketable_by_delayed_quote": limit_vs_delayed is not None and float(limit_vs_delayed) >= 0.0,
        "delayed_quote_limit_price": delayed_quote_limit,
        "delayed_quote_limit_vs_runtime_price_points": delayed_limit_vs_runtime,
        "delayed_quote_limit_vs_broker_delayed_quote_points": delayed_limit_vs_delayed,
        "delayed_quote_limit_marketable_by_runtime_context": delayed_limit_vs_runtime is not None and float(delayed_limit_vs_runtime) >= 0.0,
        "delayed_quote_limit_marketable_by_delayed_quote": delayed_limit_vs_delayed is not None and float(delayed_limit_vs_delayed) >= 0.0,
        "block_submit": block_submit,
        "block_reason": block_reason,
        "low_confidence_price_source": bool(block_submit),
        "live_money_eligible": False,
    }


def _is_close_intent(*, config: IbkrPaperStrategyBridgeConfig, intent: IbkrPaperStrategyOrderIntent) -> bool:
    metadata = dict(config.caller_metadata or {})
    intent_type = str(metadata.get("intent_type") or "").strip().upper()
    action = str(intent.action or "").strip().upper()
    if intent_type in {"SELL_TO_CLOSE", "BUY_TO_CLOSE"}:
        return True
    if intent_type in {"BUY_TO_OPEN", "SELL_TO_OPEN"}:
        return False
    return action in {"EXIT"} or (action == "SELL" and intent_type == "")


def _is_entry_intent(*, config: IbkrPaperStrategyBridgeConfig, intent: IbkrPaperStrategyOrderIntent) -> bool:
    metadata = dict(config.caller_metadata or {})
    intent_type = str(metadata.get("intent_type") or "").strip().upper()
    action = str(intent.action or "").strip().upper()
    if intent_type in {"BUY_TO_OPEN", "SELL_TO_OPEN"}:
        return True
    if intent_type in {"SELL_TO_CLOSE", "BUY_TO_CLOSE"}:
        return False
    return action == "BUY"


def _entry_execution_policy(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    intent: IbkrPaperStrategyOrderIntent,
) -> str:
    metadata = dict(config.caller_metadata or {})
    lane_adapter = lane_submit_bridge_adapter(lane_id=intent.strategy_id)
    policy = str(
        metadata.get("entry_execution_policy")
        or metadata.get("entry_order_policy")
        or (dict(lane_adapter or {}).get("entry_execution_policy") if lane_adapter else "")
        or ""
    ).strip().upper()
    if policy in {
        _ENTRY_POLICY_PASSIVE_LIMIT,
        _ENTRY_POLICY_MARKETABLE_RUNTIME,
        _ENTRY_POLICY_AGGRESSIVE_WITH_CAP,
        _ENTRY_POLICY_BLOCK_IF_ONLY_DELAYED,
    }:
        return policy
    entry_intent = _entry_execution_intent(config=config, intent=intent)
    if entry_intent == _ENTRY_INTENT_PARTICIPATE_NOW:
        return _ENTRY_POLICY_MARKETABLE_RUNTIME
    if entry_intent == _ENTRY_INTENT_DYNAMIC_LIMIT_WITH_CHASE_CAP:
        return _ENTRY_POLICY_AGGRESSIVE_WITH_CAP
    if entry_intent in {_ENTRY_INTENT_RESTING_PULLBACK_LIMIT, _ENTRY_INTENT_PASSIVE_ONLY}:
        return _ENTRY_POLICY_PASSIVE_LIMIT
    if str(intent.limit_price_model or "").strip().upper() in _ENTRY_MARKETABLE_LIMIT_MODELS:
        return _ENTRY_POLICY_MARKETABLE_RUNTIME
    return _ENTRY_POLICY_PASSIVE_LIMIT


def _entry_execution_intent(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    intent: IbkrPaperStrategyOrderIntent,
) -> str:
    metadata = dict(config.caller_metadata or {})
    lane_adapter = lane_submit_bridge_adapter(lane_id=intent.strategy_id)
    value = str(
        metadata.get("entry_execution_intent")
        or metadata.get("entry_intent")
        or (dict(lane_adapter or {}).get("entry_execution_intent") if lane_adapter else "")
        or ""
    ).strip().upper()
    if value in _ENTRY_EXECUTION_INTENTS:
        return value
    if _entry_has_resting_price_metadata(config=config):
        return _ENTRY_INTENT_RESTING_PULLBACK_LIMIT
    if str(intent.limit_price_model or "").strip().upper() in _ENTRY_MARKETABLE_LIMIT_MODELS:
        return _ENTRY_INTENT_PARTICIPATE_NOW
    return _ENTRY_INTENT_PASSIVE_ONLY


def _entry_has_resting_price_metadata(*, config: IbkrPaperStrategyBridgeConfig) -> bool:
    metadata = dict(config.caller_metadata or {})
    return any(
        _float_or_none(metadata.get(key)) is not None
        for key in (
            "strategy_entry_limit_price",
            "entry_limit_price",
            "open_limit_price",
            "limit_price",
            "entry_pullback_offset_points",
            "entry_pullback_offset_ticks",
        )
    )


def _strategy_entry_limit_price(*, config: IbkrPaperStrategyBridgeConfig) -> float | None:
    metadata = dict(config.caller_metadata or {})
    for key in (
        "strategy_entry_limit_price",
        "entry_limit_price",
        "open_limit_price",
        "limit_price",
    ):
        value = _float_or_none(metadata.get(key))
        if value is not None and value > 0.0:
            return value
    return None


def _strategy_entry_pullback_offset_points(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    min_tick: float,
) -> float | None:
    metadata = dict(config.caller_metadata or {})
    point_offset = _float_or_none(metadata.get("entry_pullback_offset_points"))
    if point_offset is not None and point_offset > 0.0:
        return point_offset
    tick_offset = _float_or_none(metadata.get("entry_pullback_offset_ticks"))
    if tick_offset is not None and tick_offset > 0.0:
        return float(tick_offset) * float(min_tick)
    return None


def _entry_fill_timeout_seconds(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    entry_intent: str,
) -> float:
    metadata = dict(config.caller_metadata or {})
    explicit = _float_or_none(
        metadata.get("entry_working_window_seconds")
        or metadata.get("entry_fill_timeout_seconds")
        or metadata.get("signal_validity_seconds")
    )
    if explicit is not None and explicit > 0:
        return float(explicit)
    if entry_intent == _ENTRY_INTENT_PARTICIPATE_NOW:
        return _ENTRY_PARTICIPATE_TIMEOUT_SECONDS
    if entry_intent == _ENTRY_INTENT_DYNAMIC_LIMIT_WITH_CHASE_CAP:
        return _ENTRY_DYNAMIC_TIMEOUT_SECONDS
    if entry_intent == _ENTRY_INTENT_RESTING_PULLBACK_LIMIT:
        return _ENTRY_RESTING_TIMEOUT_SECONDS
    if entry_intent == _ENTRY_INTENT_PASSIVE_ONLY:
        return _ENTRY_PASSIVE_TIMEOUT_SECONDS
    return _ENTRY_PARTICIPATE_TIMEOUT_SECONDS


def _entry_cancel_reasons(entry_intent: str) -> list[str]:
    reasons = [
        "signal_invalidated",
        "session_or_regime_changed",
        "broker_lifecycle_mismatch",
        "severe_stale_data",
        "open_order_risk",
    ]
    if entry_intent == _ENTRY_INTENT_DYNAMIC_LIMIT_WITH_CHASE_CAP:
        reasons.append("max_chase_budget_exhausted")
    if entry_intent == _ENTRY_INTENT_PARTICIPATE_NOW:
        reasons.append("short_participation_timeout")
    else:
        reasons.append("working_window_expired")
    return reasons


def _entry_limit_override(entry_execution_pricing: dict[str, Any] | None) -> float | None:
    pricing = dict(entry_execution_pricing or {})
    if bool(pricing.get("block_submit")):
        return None
    if str(pricing.get("execution_price_source") or "").strip().upper() not in {
        _ENTRY_RUNTIME_PRICE_SOURCE,
        _ENTRY_STRATEGY_DEFINED_LIMIT_SOURCE,
        _ENTRY_RESTING_RUNTIME_PULLBACK_SOURCE,
    }:
        return None
    return _float_or_none(pricing.get("limit_price"))


def _qualified_contract_min_tick(qualified_contract_report: dict[str, Any]) -> float | None:
    details = list(qualified_contract_report.get("api_contract_details") or [])
    if details:
        tick = _float_or_none(dict(details[0]).get("min_tick"))
        if tick is not None:
            return tick
    contract = dict(qualified_contract_report.get("qualified_contract") or {})
    return _float_or_none(contract.get("min_tick"))


def _load_entry_runtime_market_snapshot(
    *,
    repo_root: Path,
    symbol: str,
    now: datetime,
) -> dict[str, Any]:
    normalized_symbol = str(symbol or "").strip().upper()
    path = Path(repo_root) / Path(str(_ENTRY_RUNTIME_CANDLE_PATH).format(symbol=normalized_symbol))
    if not path.exists():
        return {
            "source_artifact_path": str(path),
            "runtime_last_or_close": None,
            "runtime_candle_timestamp": None,
            "runtime_data_age_seconds": None,
            "runtime_data_fresh": False,
            "detail": "Runtime 1m candle artifact is missing.",
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "source_artifact_path": str(path),
            "runtime_last_or_close": None,
            "runtime_candle_timestamp": None,
            "runtime_data_age_seconds": None,
            "runtime_data_fresh": False,
            "detail": f"Runtime 1m candle artifact could not be read: {exc}",
        }
    bars = [dict(row) for row in list(payload.get("bars") or []) if isinstance(row, dict)]
    latest_bar = bars[-1] if bars else {}
    candle_ts = (
        latest_bar.get("bar_end")
        or payload.get("last_completed_bar_ts")
        or payload.get("last_candle_timestamp")
        or latest_bar.get("timestamp")
        or latest_bar.get("bar_start")
        or payload.get("generated_at")
    )
    parsed_ts = _parse_datetime(candle_ts)
    if parsed_ts is not None and parsed_ts.tzinfo is None:
        parsed_ts = parsed_ts.replace(tzinfo=timezone.utc)
    age_seconds = (
        max(0.0, (now - parsed_ts).total_seconds())
        if parsed_ts is not None
        else None
    )
    runtime_price = _float_or_none(
        latest_bar.get("last")
        if latest_bar.get("last") is not None
        else latest_bar.get("close")
        if latest_bar.get("close") is not None
        else payload.get("last")
        if payload.get("last") is not None
        else payload.get("close")
    )
    return {
        "source_artifact_path": str(path),
        "runtime_last_or_close": runtime_price,
        "runtime_candle_timestamp": None if candle_ts is None else str(candle_ts),
        "runtime_data_age_seconds": age_seconds,
        "runtime_data_fresh": bool(age_seconds is not None and age_seconds <= _ENTRY_RUNTIME_CANDLE_MAX_AGE_SECONDS),
        "generated_at": payload.get("generated_at"),
        "source": payload.get("source") or payload.get("source_id") or "DATABENTO_LIVE_RUNTIME",
        "timeframe": payload.get("timeframe") or "1m",
    }


def _broker_quote_type(quote_context: dict[str, Any]) -> str:
    label = str(quote_context.get("quote_source_label") or "").strip().upper()
    if label in {"DELAYED", "DELAYED_FROZEN", "FROZEN"}:
        return "delayed"
    if label == "LIVE":
        return "live"
    return "unknown"


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _round_price_to_tick(price: float, min_tick: float) -> float:
    if min_tick <= 0.0:
        return float(price)
    ticks = round(float(price) / float(min_tick))
    return round(ticks * float(min_tick), 8)


def _signed_limit_distance(*, action: str, limit_price: float, reference_price: float) -> float:
    normalized_action = str(action or "").strip().upper()
    if normalized_action == "SELL":
        return float(reference_price) - float(limit_price)
    return float(limit_price) - float(reference_price)


def _hard_exit_from_metadata(*, config: IbkrPaperStrategyBridgeConfig, metadata: dict[str, Any]) -> bool:
    explicit = _bool_or_none(metadata.get("hard_exit"))
    urgency = classify_exit_urgency(
        explicit_hard_exit=explicit,
        reason_values=_exit_reason_values_from_metadata(config=config, metadata=metadata),
        reason_source="bridge_caller_metadata",
    )
    return bool(urgency["hard_exit"])


def _exit_reason_values_from_metadata(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    metadata: dict[str, Any],
) -> tuple[Any, ...]:
    return (
        config.reason,
        metadata.get("exit_reason"),
        metadata.get("reason"),
        metadata.get("reason_code"),
        metadata.get("close_reason"),
        metadata.get("primary_reason"),
        metadata.get("all_true_reasons"),
        metadata.get("exit_family"),
        metadata.get("risk_tags"),
        tuple(config.risk_tags),
        metadata.get("order_intent_id"),
    )


def _bool_or_none(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None


def _manual_harness_client_id(client_id: int) -> int:
    return int(client_id) + _MANUAL_HARNESS_CLIENT_ID_OFFSET


def _bridge_fill_timeout_seconds(
    *,
    exit_attempt_policy: ExitAttemptPolicy,
    entry_execution_pricing: dict[str, Any] | None,
) -> float:
    pricing = dict(entry_execution_pricing or {})
    if bool(pricing.get("is_entry")):
        return float(pricing.get("fill_timeout_seconds") or _ENTRY_PARTICIPATE_TIMEOUT_SECONDS)
    return float(exit_attempt_policy.fill_timeout_seconds)


def _bridge_limit_offset_ticks(
    *,
    exit_attempt_policy: ExitAttemptPolicy,
    entry_execution_pricing: dict[str, Any] | None,
) -> float:
    pricing = dict(entry_execution_pricing or {})
    if bool(pricing.get("is_entry")):
        return float(pricing.get("limit_offset_ticks") or _ENTRY_RUNTIME_LIMIT_OFFSET_TICKS)
    return float(exit_attempt_policy.limit_offset_ticks)


def _prepare_manual_submit_bundle(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    intent: IbkrPaperStrategyOrderIntent,
    exit_attempt_policy: ExitAttemptPolicy,
    entry_execution_pricing: dict[str, Any] | None = None,
    stack_provider: Callable[[], list[Any]] = inspect.stack,
) -> dict[str, Any]:
    test_mode = _intent_test_mode(intent)
    delegated_output_dir = (Path(config.output_dir) / "prepared_manual_harness") if config.output_dir is not None else None
    expected_target = _bridge_phase1_target(config=config, intent=intent)
    limit_override = _entry_limit_override(entry_execution_pricing)
    fill_timeout_seconds = _bridge_fill_timeout_seconds(
        exit_attempt_policy=exit_attempt_policy,
        entry_execution_pricing=entry_execution_pricing,
    )
    limit_offset_ticks = _bridge_limit_offset_ticks(
        exit_attempt_policy=exit_attempt_policy,
        entry_execution_pricing=entry_execution_pricing,
    )
    manual_config = IbkrManualPaperSubmitConfig(
        repo_root=config.repo_root,
        mode=config.mode,
        host=config.host,
        port=config.port,
        client_id=_manual_harness_client_id(config.client_id),
        account_id=config.account_id,
        symbol=str(expected_target.get("symbol") or intent.symbol or "").strip().upper(),
        expiry=str(expected_target.get("contract_month") or intent.contract_month or "").strip(),
        action=intent.action,
        quantity=float(intent.quantity),
        order_type=_EXPECTED_ORDER_TYPE,
        limit_price=limit_override,
        time_in_force=_EXPECTED_TIF,
        test_mode=test_mode,
        timeout_seconds=float(config.timeout_seconds),
        fill_timeout_seconds=fill_timeout_seconds,
        post_approval_observation_seconds=75.0,
        fill_limit_offset_ticks=limit_offset_ticks,
        manual_confirmation_timeout_seconds=90.0,
        caller_path="manual_cli",
        submit=False,
        output_dir=delegated_output_dir,
        frozen_preview_path=None,
        diagnostic_dry_run=False,
        execution_pricing_context=entry_execution_pricing if limit_override is not None else None,
    )
    artifacts = run_ibkr_manual_paper_submit_test(config=manual_config, stack_provider=stack_provider)
    if delegated_output_dir is not None:
        write_ibkr_manual_paper_submit_artifacts(output_dir=delegated_output_dir, artifacts=artifacts)
    frozen_preview_path = frozen_preview_path_for_config(manual_config)
    preview = dict(artifacts.report.get("preview") or {})
    preview_digest = preview.get("preview_digest")
    expected_phrase = preview.get("expected_approval_phrase")
    if frozen_preview_path is None or not Path(frozen_preview_path).exists() or not preview_digest or not expected_phrase:
        raise IbkrPaperStrategyBridgeError("Paper strategy bridge could not prepare a valid frozen manual submit bundle.")
    return {
        "classification": artifacts.classification,
        "detail": artifacts.report.get("submit_cancel_lifecycle", {}).get("detail"),
        "artifact_stem": artifact_stem_for_test_mode(test_mode),
        "output_dir": None if delegated_output_dir is None else str(delegated_output_dir),
        "frozen_preview_path": None if frozen_preview_path is None else str(frozen_preview_path),
        "preview_digest": preview_digest,
        "expected_approval_phrase": expected_phrase,
        "exit_attempt_policy": exit_attempt_policy.to_json_dict(),
        "entry_execution_pricing": entry_execution_pricing,
    }


def _intent_test_mode(intent: IbkrPaperStrategyOrderIntent) -> str:
    action = str(intent.action or "").strip().upper()
    price_model = str(intent.limit_price_model or "").strip().upper()
    if action == "SELL":
        return "PAPER_CLOSE_TEST"
    if price_model == "DELAYED_BID_MINUS_1T_RESTING_BUY":
        return "PAPER_RESTING_TEST"
    return "PAPER_FILL_TEST"


def _per_strategy_status_summary(report: dict[str, Any]) -> dict[str, Any]:
    intent = dict(report.get("intent") or {})
    delegated_result = dict(report.get("delegated_result") or {})
    prepared = dict(report.get("prepared_submit_bundle") or {})
    return {
        "generated_at": report.get("generated_at"),
        "strategy_id": intent.get("strategy_id"),
        "symbol": intent.get("symbol"),
        "action": intent.get("action"),
        "quantity": intent.get("quantity"),
        "classification": report.get("classification"),
        "selected_account_id": report.get("selected_account_id"),
        "current_position_quantity": report.get("current_position_quantity"),
        "open_order_count": dict(report.get("open_orders") or {}).get("open_order_count"),
        "prepared_submit_bundle": {
            "classification": prepared.get("classification"),
            "frozen_preview_path": prepared.get("frozen_preview_path"),
            "preview_digest": prepared.get("preview_digest"),
        }
        if prepared
        else None,
        "delegated_classification": delegated_result.get("classification"),
        "needs_manual_review": report.get("classification") == "PAPER_STRATEGY_NEEDS_MANUAL_REVIEW",
    }


def _quote_is_fresh(quote_context: dict[str, Any]) -> bool:
    updated_at = _parse_datetime(quote_context.get("updated_at"))
    if updated_at is None:
        return False
    age_seconds = max(0.0, (datetime.now(timezone.utc) - updated_at).total_seconds())
    return age_seconds <= 30.0 and str(quote_context.get("quote_source_label") or "").strip().upper() in {
        "DELAYED",
        "DELAYED_FROZEN",
        "FROZEN",
        "LIVE",
    }


def _qualified_contract_is_exact(
    qualified_contract_report: dict[str, Any],
    expected_target: dict[str, Any],
) -> bool:
    contract = dict(qualified_contract_report.get("qualified_contract") or {})
    return _exact_contract_matches_phase1_target(contract=contract, target=expected_target)


def _check(name: str, passed: bool, blocking: bool, detail: str) -> dict[str, Any]:
    return {"name": name, "passed": bool(passed), "blocking": bool(blocking), "detail": detail}


def _record_bridge_audit(
    audit_events: list[dict[str, Any]],
    *,
    event_type: str,
    detail: str,
    config: IbkrPaperStrategyBridgeConfig,
    extra: dict[str, Any] | None = None,
) -> None:
    audit_events.append(
        {
            "event_type": event_type,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "mode": config.mode,
            "host": config.host,
            "port": config.port,
            "client_id": config.client_id,
            "strategy_id": config.strategy_id,
            "symbol": config.symbol,
            "contract_month": config.contract_month,
            "action": config.action,
            "caller_metadata": dict(config.caller_metadata or {}),
            "detail": detail,
            **dict(extra or {}),
        }
    )


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _position_like_config(config: IbkrPaperStrategyBridgeConfig) -> Any:
    expected_target = _bridge_phase1_target(config=config, intent=_build_intent(config))
    return type(
        "PaperStrategyBridgePositionConfig",
        (),
        {
            "repo_root": config.repo_root,
            "mode": config.mode,
            "host": config.host,
            "port": config.port,
            "client_id": config.client_id,
            "read_only": True,
            "account_id": config.account_id,
            "symbol": str(expected_target.get("symbol") or config.symbol or "").strip().upper(),
            "contract_month": str(expected_target.get("contract_month") or config.contract_month or "").strip(),
            "exact_expiry": str(expected_target.get("expiry") or "").strip(),
            "con_id": expected_target.get("con_id"),
            "local_symbol": str(expected_target.get("local_symbol") or "").strip().upper(),
            "security_type": "FUT",
            "exchange": str(expected_target.get("exchange") or _EXPECTED_EXCHANGE).strip().upper(),
            "currency": str(expected_target.get("currency") or _EXPECTED_CURRENCY).strip().upper(),
            "multiplier": str(expected_target.get("multiplier") or _EXPECTED_MULTIPLIER).strip(),
            "timeout_seconds": config.timeout_seconds,
        },
    )()
