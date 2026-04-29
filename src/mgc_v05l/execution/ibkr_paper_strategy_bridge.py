"""Manual operator IBKR paper strategy-intent bridge."""

from __future__ import annotations

import inspect
import json
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
    _qualify_mgc_contract,
    _refresh_open_orders_snapshot,
    _refresh_positions_snapshot,
    run_ibkr_manual_paper_submit_test,
    write_ibkr_manual_paper_submit_artifacts,
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
_SCHEMA_ACTIONS = {"BUY", "SELL", "HOLD", "EXIT", "NO_ACTION"}
_ARTIFACT_STEM = "ibkr_paper_strategy_bridge"
_APPROVED_RUNTIME_CALLER_PATHS = {
    "probationary_paper_runtime_lane",
    "supervised_paper_runtime_bridge",
    "ibkr_paper_strategy_executor",
}
_APPROVED_CALLER_PATHS = {"manual_strategy_bridge_cli", *_APPROVED_RUNTIME_CALLER_PATHS}
_APPROVED_RUNTIME_CALLER_TYPES = {
    "supervised_paper_runtime",
    "supervised_paper_executor",
}
_APPROVED_RUNTIME_CALLER_MODULE_PREFIXES = (
    "mgc_v05l.app.probationary_runtime",
    "mgc_v05l.app.headless_supervised_paper",
)
_BRIDGE_ADDITIONAL_FORBIDDEN_CALLER_PREFIXES = (
    "mgc_v05l.live",
    "mgc_v05l.execution.live_strategy_broker",
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
    caller_gate = evaluate_strategy_bridge_caller(caller_path=config.caller_path, stack_provider=stack_provider)
    monitor_status = load_paper_strategy_monitor_status(repo_root=config.repo_root)
    governance_status = load_paper_strategy_governance_status(repo_root=config.repo_root, strategy_id=config.strategy_id)
    governance_row = dict(governance_status.get("selected_strategy") or {})
    exposure_status = evaluate_paper_strategy_exposure_gate(
        repo_root=config.repo_root,
        strategy_id=config.strategy_id,
        bridge_strategy_id=str(governance_row.get("bridge_strategy_id") or "").strip() or None,
        action=config.action,
        quantity=config.quantity,
        executable_symbol=config.symbol,
    )
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
                    "read_only_preflight": True,
                },
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
            selected_account_id=selected_account_id,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        open_orders = _refresh_open_orders_snapshot(
            runtime=runtime,
            selected_account_id=selected_account_id,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        qualified_contract_report = _qualify_mgc_contract(
            transport=runtime.transport,
            collector=runtime.collector,
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
                exact_contract_report=exact_contract_report,
                qualified_contract_report=qualified_contract_report,
                current_position_quantity=current_position_quantity,
                preflight_checks=preflight_checks,
                prepared_submit_bundle=None,
                delegated_result=None,
                callback_timeline_event_count=len(_build_callback_timeline(runtime)),
                errors=list(runtime.collector.errors),
            )
            return IbkrPaperStrategyBridgeArtifacts(classification=classification, report=report, audit_events=audit_events)

        delegated_result = None
        prepared_submit_bundle = None
        classification = "PAPER_STRATEGY_BRIDGE_READY"
        _record_bridge_audit(
            audit_events,
            event_type="intent_ready",
            detail="Paper strategy bridge preflight passed and the intent is ready for operator-controlled paper submit.",
            config=config,
            extra={"current_position_quantity": current_position_quantity},
        )
        if config.prepare_manual_submit_bundle:
            prepared_submit_bundle = _prepare_manual_submit_bundle(config=config, intent=intent)
            _record_bridge_audit(
                audit_events,
                event_type="manual_submit_bundle_prepared",
                detail="Paper strategy bridge prepared a frozen manual submit bundle for the current strategy intent.",
                config=config,
                extra={
                    "bundle_path": prepared_submit_bundle.get("frozen_preview_path"),
                    "preview_digest": prepared_submit_bundle.get("preview_digest"),
                },
            )
        if config.submit:
            delegated_result = _delegate_to_manual_harness(
                config=config,
                intent=intent,
            )
            classification = _map_delegate_classification(delegated_result)
            _record_bridge_audit(
                audit_events,
                event_type="delegated_manual_harness_completed",
                detail="Paper strategy bridge delegated to the proven manual paper harness.",
                config=config,
                extra={"delegated_classification": delegated_result.get("classification")},
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
            callback_timeline_event_count=len(_build_callback_timeline(runtime)),
            errors=list(runtime.collector.errors),
        )
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
                "read_only_preflight": True,
            },
            "intent": intent.to_dict(),
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
        f"- exact qualified contract: `MGC {exact_contract.get('expiry')} / conId={exact_contract.get('con_id')} / localSymbol={exact_contract.get('local_symbol')}`",
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
    stack_provider: Callable[[], list[Any]] = inspect.stack,
) -> dict[str, Any]:
    normalized_caller = str(caller_path or "").strip()
    runtime_caller = normalized_caller in _APPROVED_RUNTIME_CALLER_PATHS
    stack_modules: list[str] = []
    for frame in stack_provider():
        module_name = str(getattr(getattr(frame, "frame", None), "f_globals", {}).get("__name__", "") or "").strip()
        if module_name:
            stack_modules.append(module_name)
    forbidden: list[str] = []
    for module_name in stack_modules:
        if runtime_caller and module_name.startswith(_APPROVED_RUNTIME_CALLER_MODULE_PREFIXES):
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
    passed = (
        str(metadata.get("caller_type") or "").strip() in _APPROVED_RUNTIME_CALLER_TYPES
        and metadata.get("paper_only") is True
        and str(metadata.get("mode") or "").strip().upper() == _EXPECTED_MODE
        and str(metadata.get("host") or "").strip() == _EXPECTED_HOST
        and int(metadata.get("port") or 0) == _EXPECTED_PORT
        and str(metadata.get("account_id") or "").strip() == _EXPECTED_ACCOUNT_ID
        and str(metadata.get("strategy_id") or "").strip() == config.strategy_id
        and str(metadata.get("lane_id") or "").strip() == config.strategy_id
        and str(metadata.get("source_instrument") or "").strip().upper() != ""
        and str(metadata.get("executable_proxy") or "").strip().upper() == config.symbol
        and str(metadata.get("route_destination") or "").strip() == "ibkr_paper_bridge_submit_capable"
        and str(metadata.get("bridge_proxy_mode") or "").strip() != ""
        and str(metadata.get("intent_action") or "").strip().upper() == config.action
        and str(metadata.get("intent_type") or "").strip().upper() != ""
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
) -> list[dict[str, Any]]:
    checks = [
        _check("account_match", selected_account_id == config.account_id == _EXPECTED_ACCOUNT_ID, True, "Paper bridge account must match DUM882026 exactly."),
        _check("daily_order_cap", _submitted_order_count(audit_events) < int(config.daily_order_cap), True, f"Daily bridge order cap is {config.daily_order_cap}."),
        _check("no_working_orders", int(open_orders.get("open_order_count") or 0) == 0, True, "No working broker order is allowed before a strategy bridge intent can submit."),
        _check("delayed_quote_freshness", _quote_is_fresh(quote_context), True, "A fresh delayed quote is required before strategy bridge submit is allowed."),
        _check("exact_qualified_contract", _qualified_contract_is_exact(qualified_contract_report), True, "Submitted contract must match exact qualified MGC 20260626 details."),
        _check("strategy_allowed_state", True, True, "The bridge remains manual-only and the shadow ledger stays separate from broker execution."),
    ]
    if intent.action == "BUY":
        checks.append(
            _check(
                "position_gate_buy_to_open",
                True,
                True,
                "BUY intents are governed by per-strategy exposure attribution; aggregate broker flat is not required when stacking is explicitly allowed.",
            )
        )
    else:
        checks.append(
            _check(
                "position_gate_sell_to_close",
                True,
                True,
                "SELL and EXIT intents are governed by per-strategy exposure attribution; aggregate broker quantity alone is not treated as ownership proof.",
            )
        )
    _record_bridge_audit(
        audit_events,
        event_type="preflight_evaluated",
        detail="Paper strategy bridge preflight guardrails evaluated.",
        config=config,
        extra={"checks": checks, "current_position_quantity": current_position_quantity, "exact_contract": exact_contract_report.get("exact_contract")},
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
    lane_adapter = lane_submit_bridge_adapter(lane_id=intent.strategy_id)
    monitor_contract_matches = (
        str(monitor_exact_contract.get("symbol") or "").strip().upper() == config.symbol
        and str(monitor_exact_contract.get("expiry") or "").strip() == _EXPECTED_EXACT_EXPIRY
        and int(monitor_exact_contract.get("con_id") or 0) == _EXPECTED_CON_ID
        and str(monitor_exact_contract.get("local_symbol") or "").strip().upper() == _EXPECTED_LOCAL_SYMBOL
    )
    monitor_detail = str(
        monitor_status.get("detail")
        or (
            "Paper strategy monitor allows submit."
            if monitor_status.get("submit_allowed")
            else f"Paper strategy monitor blocked submit: {', '.join(list(monitor_status.get('block_reasons') or [])) or 'unknown_reason'}"
        )
    )
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
    caller_path = str(config.caller_path or "").strip()
    return [
        _check("approved_paper_caller_path", caller_gate["passed"], True, caller_gate["detail"]),
        _runtime_caller_metadata_check(config=config, intent=intent),
        _check("paper_environment_lock", environment_lock["passed"], True, str(environment_lock.get("port_policy") or environment_lock.get("detail") or "Environment lock failed.")),
        _check("paper_only_intent", bool(intent.paper_only), True, "Intent must remain explicitly paper-only."),
        _check("strategy_allowlist", intent.strategy_id in _SUPPORTED_STRATEGY_IDS or lane_adapter is not None, True, "Only the ATP Companion baseline and explicitly ported paper strategy lane identities are allowed in the paper bridge."),
        _check("executable_contract_whitelist", intent.symbol == _EXPECTED_SYMBOL, True, "Phase 1 executable contract is MGC only, even when the source strategy lane is GC."),
        _check("contract_month_lock", intent.contract_month == _EXPECTED_CONTRACT_MONTH, True, "Only MGC 202606 is allowed in the paper bridge."),
        _check(
            "selected_lane_adapter_present",
            (lane_adapter is not None) if intent.strategy_id not in {"ATP_COMPANION_V1_ASIA_US", "ATP_COMPANION_V1_GC_ASIA_US", "ATP_COMPANION_V1_GC_ASIA_US_PRODUCTION_TRACK"} else True,
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
            (not config.submit) or bool(monitor_status),
            True,
            "Submit-capable paper strategy orders require a live paper strategy monitor runtime status file.",
        ),
        _check(
            "paper_strategy_monitor_running",
            (not config.submit) or bool(monitor_status.get("monitor_running")),
            True,
            "Submit-capable paper strategy orders require the paper strategy monitor service to be actively running.",
        ),
        _check(
            "paper_strategy_monitor_health",
            (not config.submit) or monitor_health == "HEALTHY",
            True,
            f"Submit-capable paper strategy orders require a HEALTHY paper strategy monitor, not {monitor_health or 'UNKNOWN'}.",
        ),
        _check(
            "paper_strategy_monitor_account_match",
            (not config.submit) or monitor_account_matches,
            True,
            "Submit-capable paper strategy orders require the live monitor account to match DUM882026.",
        ),
        _check(
            "paper_strategy_monitor_contract_match",
            (not config.submit) or monitor_contract_matches,
            True,
            "Submit-capable paper strategy orders require the live monitor exact contract to match MGC 20260626 / conId 712565978 / localSymbol MGCM6.",
        ),
        _check(
            "manual_harness_bundle_present_for_submit",
            (not config.submit) or caller_path in _APPROVED_RUNTIME_CALLER_PATHS or (
                config.manual_frozen_preview_path is not None
                and config.approval_digest is not None
                and config.approval_phrase is not None
            ),
            True,
            (
                "Approved supervised paper runtime callers may submit through the bridge without a manual frozen preview bundle."
                if caller_path in _APPROVED_RUNTIME_CALLER_PATHS
                else "Bridge submit requires a manual-harness frozen preview path plus the exact approval digest and approval phrase."
            ),
        ),
        _check(
            "paper_strategy_submit_gate",
            (not config.submit) or bool(monitor_status.get("submit_allowed")),
            True,
            monitor_detail,
        ),
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
            (not config.submit) or bool(governance_status.get("submit_allowed")),
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


def _delegate_to_manual_harness(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    intent: IbkrPaperStrategyOrderIntent,
) -> dict[str, Any]:
    if config.manual_frozen_preview_path is None or config.approval_digest is None or config.approval_phrase is None:
        raise IbkrPaperStrategyBridgeError(
            "Bridge submit delegation requires a previously generated manual-harness frozen preview plus the exact approval digest and approval phrase."
        )
    test_mode = "PAPER_FILL_TEST" if intent.action == "BUY" else "PAPER_CLOSE_TEST"
    delegated_output_dir = (Path(config.output_dir) / "delegated_manual_harness") if config.output_dir is not None else None
    manual_config = IbkrManualPaperSubmitConfig(
        repo_root=config.repo_root,
        mode=config.mode,
        host=config.host,
        port=config.port,
        client_id=_manual_harness_client_id(config.client_id),
        account_id=config.account_id,
        symbol=_EXPECTED_SYMBOL,
        expiry=_EXPECTED_CONTRACT_MONTH,
        action=intent.action,
        quantity=float(intent.quantity),
        order_type=_EXPECTED_ORDER_TYPE,
        limit_price=None,
        time_in_force=_EXPECTED_TIF,
        test_mode=test_mode,
        timeout_seconds=float(config.timeout_seconds),
        fill_timeout_seconds=8.0,
        post_approval_observation_seconds=15.0,
        fill_limit_offset_ticks=1.0,
        manual_confirmation_timeout_seconds=90.0,
        caller_path="manual_cli",
        submit=True,
        approval_digest=config.approval_digest,
        approval_phrase=config.approval_phrase,
        output_dir=delegated_output_dir,
        frozen_preview_path=Path(config.manual_frozen_preview_path),
        diagnostic_dry_run=False,
    )
    delegated = run_ibkr_manual_paper_submit_test(config=manual_config)
    return {
        "classification": delegated.classification,
        "detail": delegated.report.get("lifecycle", {}).get("detail"),
        "report": delegated.report,
    }


def _map_delegate_classification(delegated_result: dict[str, Any] | None) -> str:
    if not isinstance(delegated_result, dict):
        return "PAPER_STRATEGY_BRIDGE_READY"
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
    callback_timeline_event_count: int,
    errors: list[dict[str, Any]],
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
            "read_only_preflight": True,
        },
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
        "prepared_submit_bundle": prepared_submit_bundle,
        "delegated_result": delegated_result,
        "callback_timeline_event_count": callback_timeline_event_count,
        "errors": errors,
    }


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


def _manual_harness_client_id(client_id: int) -> int:
    return int(client_id) + _MANUAL_HARNESS_CLIENT_ID_OFFSET


def _prepare_manual_submit_bundle(
    *,
    config: IbkrPaperStrategyBridgeConfig,
    intent: IbkrPaperStrategyOrderIntent,
) -> dict[str, Any]:
    test_mode = _intent_test_mode(intent)
    delegated_output_dir = (Path(config.output_dir) / "prepared_manual_harness") if config.output_dir is not None else None
    manual_config = IbkrManualPaperSubmitConfig(
        repo_root=config.repo_root,
        mode=config.mode,
        host=config.host,
        port=config.port,
        client_id=_manual_harness_client_id(config.client_id),
        account_id=config.account_id,
        symbol=_EXPECTED_SYMBOL,
        expiry=_EXPECTED_CONTRACT_MONTH,
        action=intent.action,
        quantity=float(intent.quantity),
        order_type=_EXPECTED_ORDER_TYPE,
        limit_price=None,
        time_in_force=_EXPECTED_TIF,
        test_mode=test_mode,
        timeout_seconds=float(config.timeout_seconds),
        fill_timeout_seconds=8.0,
        post_approval_observation_seconds=15.0,
        fill_limit_offset_ticks=1.0,
        manual_confirmation_timeout_seconds=90.0,
        caller_path="manual_cli",
        submit=False,
        output_dir=delegated_output_dir,
        frozen_preview_path=None,
        diagnostic_dry_run=False,
    )
    artifacts = run_ibkr_manual_paper_submit_test(config=manual_config)
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
        "LIVE",
    }


def _qualified_contract_is_exact(qualified_contract_report: dict[str, Any]) -> bool:
    contract = dict(qualified_contract_report.get("qualified_contract") or {})
    return (
        str(contract.get("broker_symbol") or "").strip().upper() == _EXPECTED_SYMBOL
        and str(contract.get("expiry") or "").strip() == _EXPECTED_EXACT_EXPIRY
        and int(contract.get("con_id") or 0) == _EXPECTED_CON_ID
        and str(contract.get("local_symbol") or "").strip().upper() == _EXPECTED_LOCAL_SYMBOL
    )


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
            "symbol": _EXPECTED_SYMBOL,
            "contract_month": _EXPECTED_CONTRACT_MONTH,
            "exact_expiry": _EXPECTED_EXACT_EXPIRY,
            "con_id": _EXPECTED_CON_ID,
            "local_symbol": _EXPECTED_LOCAL_SYMBOL,
            "security_type": "FUT",
            "exchange": _EXPECTED_EXCHANGE,
            "currency": _EXPECTED_CURRENCY,
            "multiplier": _EXPECTED_MULTIPLIER,
            "timeout_seconds": config.timeout_seconds,
        },
    )()
