"""IBKR TWS paper read-only market-data diagnostics."""

from __future__ import annotations

import csv
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from ..brokers.ibkr import (
    IbkrClient,
    IbkrSession,
    build_default_ibkr_order_id_policy,
)
from .ibkr_read_only_verifier import (
    IbkrReadOnlyApiTransport,
    IbkrReadOnlyApiTransportConfig,
    IbkrReadOnlyProbeCollector,
    IbkrReadOnlyVerificationError,
    _resolve_selected_account_id,
    _run_contract_qualification_checks,
    _tws_manual_check_message,
    _wait_for_connection_ready,
    _wait_for_event,
    evaluate_ibkr_environment_lock,
)

_PERMISSION_ERROR_CODES = {354, 10168}
_DELAYED_ONLY_ERROR_CODES = {10167}
_BID_TICK_IDS = (1, 66)
_ASK_TICK_IDS = (2, 67)
_LAST_TICK_IDS = (4, 68)
_CLOSE_TICK_IDS = (9, 75)


@dataclass(frozen=True)
class IbkrMarketDataDiagnosticConfig:
    repo_root: Path
    mode: str
    host: str
    port: int
    client_id: int
    read_only: bool
    account_id: str | None = None
    timeout_seconds: float = 12.0
    gc_expiry: str = "202606"
    mgc_expiry: str = "202606"


@dataclass(frozen=True)
class IbkrMarketDataDiagnosticArtifacts:
    classification: str
    report: dict[str, Any]
    mode_probe_rows: list[dict[str, Any]]

    @property
    def exit_code(self) -> int:
        return 0 if self.classification != "IBKR_MARKET_DATA_BLOCKED" else 1


@dataclass(frozen=True)
class _ProbeMode:
    label: str
    market_data_type_requested: int
    request_type: str = "snapshot"


_PROBE_MODES = (
    _ProbeMode(label="live_snapshot", market_data_type_requested=1),
    _ProbeMode(label="delayed_snapshot", market_data_type_requested=3),
    _ProbeMode(label="frozen_snapshot", market_data_type_requested=2),
    _ProbeMode(label="delayed_frozen_snapshot", market_data_type_requested=4),
)


def run_ibkr_market_data_diagnostic(
    *,
    config: IbkrMarketDataDiagnosticConfig,
    transport_factory: Callable[..., Any] = IbkrReadOnlyApiTransport,
    module_loader: Callable[[str], Any] | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> IbkrMarketDataDiagnosticArtifacts:
    started_at = datetime.now(timezone.utc)
    environment_lock = evaluate_ibkr_environment_lock(
        mode=config.mode,
        host=config.host,
        port=config.port,
        read_only=config.read_only,
    )
    if not environment_lock["passed"]:
        return _blocked_artifacts(
            config=config,
            started_at=started_at,
            environment_lock=environment_lock,
            detail="Configured environment lock does not match the required TWS paper read-only lock.",
        )

    runtime: _DiagnosticRuntime | None = None
    try:
        runtime = _build_runtime(config=config, transport_factory=transport_factory, module_loader=module_loader)
        runtime.transport.connect()
        _start_runtime(runtime, timeout_seconds=config.timeout_seconds, sleep_fn=sleep_fn)
        runtime.client.request_managed_accounts()
        runtime.transport.req_managed_accounts()
        if not _wait_for_event(
            runtime.collector.managed_accounts_ready,
            collector=runtime.collector,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        ):
            latest_error = runtime.collector.latest_error()
            detail = (
                f"TWS paper API did not expose managed accounts within {config.timeout_seconds:.1f}s."
                if latest_error is None
                else f"TWS paper API error {latest_error['code']}: {latest_error['message']}"
            )
            return _blocked_artifacts(
                config=config,
                started_at=started_at,
                environment_lock=environment_lock,
                detail=detail,
            )
        selected_account_id = _resolve_selected_account_id(
            requested_account_id=config.account_id,
            managed_accounts=runtime.client.connection_state().managed_accounts,
        )
        contract_report = _run_contract_qualification_checks(
            transport=runtime.transport,
            collector=runtime.collector,
            timeout_seconds=config.timeout_seconds,
            gc_expiry=config.gc_expiry,
            mgc_expiry=config.mgc_expiry,
            sleep_fn=sleep_fn,
        )
        if not contract_report.get("ok"):
            return _blocked_artifacts(
                config=config,
                started_at=started_at,
                environment_lock=environment_lock,
                detail="Contract qualification did not complete cleanly, so market-data probing was not safe to continue.",
                contract_report=contract_report,
                selected_account_id=selected_account_id,
                connection_check={
                    "connected": runtime.session.state.connected,
                    "host": config.host,
                    "port": config.port,
                    "client_id": config.client_id,
                    "server_version": runtime.transport.server_version(),
                    "tws_connection_time": runtime.transport.tws_connection_time(),
                    "connection_timestamp": (
                        runtime.session.state.connected_at.isoformat()
                        if runtime.session.state.connected_at is not None
                        else started_at.isoformat()
                    ),
                },
            )

        probe_rows = _run_mode_probes(
            transport=runtime.transport,
            collector=runtime.collector,
            contract_report=contract_report,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        classification = classify_market_data_diagnostic(
            probe_rows=probe_rows,
            contract_report=contract_report,
            connected=bool(runtime.session.state.connected),
        )
        diagnostic_note = build_market_data_diagnostic_note(
            classification=classification,
            probe_rows=probe_rows,
            contract_report=contract_report,
            host=config.host,
            port=config.port,
        )
        report = {
            "classification": classification,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "started_at": started_at.isoformat(),
            "connection_check": {
                "connected": runtime.session.state.connected,
                "host": config.host,
                "port": config.port,
                "client_id": config.client_id,
                "server_version": runtime.transport.server_version(),
                "tws_connection_time": runtime.transport.tws_connection_time(),
                "connection_timestamp": (
                    runtime.session.state.connected_at.isoformat()
                    if runtime.session.state.connected_at is not None
                    else started_at.isoformat()
                ),
            },
            "environment_lock_check": {
                **environment_lock,
                "account_id": selected_account_id,
            },
            "contract_qualification_check": contract_report,
            "mode_probes": probe_rows,
            "diagnostic_note": diagnostic_note,
            "errors": list(runtime.collector.errors),
            "next_manual_checks": diagnostic_note["manual_next_checks"],
        }
        return IbkrMarketDataDiagnosticArtifacts(
            classification=classification,
            report=report,
            mode_probe_rows=probe_rows,
        )
    except Exception as exc:
        return _blocked_artifacts(
            config=config,
            started_at=started_at,
            environment_lock=environment_lock,
            detail=str(exc),
        )
    finally:
        if runtime is not None:
            try:
                runtime.transport.disconnect()
            except Exception:
                pass


def classify_market_data_diagnostic(
    *,
    probe_rows: list[dict[str, Any]],
    contract_report: dict[str, Any],
    connected: bool,
) -> str:
    if not connected or not contract_report.get("ok"):
        return "IBKR_MARKET_DATA_BLOCKED"
    if not probe_rows:
        return "IBKR_MARKET_DATA_BLOCKED"
    live_success = any(
        row.get("any_tick_returned")
        and row.get("market_data_type_requested") in {1, 2}
        and row.get("response_indication") == "data_returned"
        for row in probe_rows
    )
    delayed_success = any(
        row.get("any_tick_returned")
        and row.get("response_indication") == "delayed_only"
        for row in probe_rows
    )
    any_ticks = any(row.get("any_tick_returned") for row in probe_rows)
    all_permission = all(
        row.get("response_indication") in {"no_permission", "delayed_only"}
        for row in probe_rows
    )
    any_permission = any(
        row.get("response_indication") in {"no_permission", "delayed_only"}
        for row in probe_rows
    )
    if live_success:
        return "IBKR_MARKET_DATA_CONNECTED"
    if delayed_success and not live_success:
        return "IBKR_MARKET_DATA_DELAYED_ONLY"
    if not any_ticks and all_permission:
        return "IBKR_MARKET_DATA_PERMISSION_BLOCKED"
    if any_ticks or any_permission:
        return "IBKR_MARKET_DATA_PARTIAL"
    return "IBKR_MARKET_DATA_BLOCKED"


def build_market_data_diagnostic_note(
    *,
    classification: str,
    probe_rows: list[dict[str, Any]],
    contract_report: dict[str, Any],
    host: str,
    port: int,
) -> dict[str, Any]:
    api_path_works = bool(contract_report.get("ok"))
    any_ticks = any(row.get("any_tick_returned") for row in probe_rows)
    any_delayed_only = any(row.get("response_indication") == "delayed_only" for row in probe_rows)
    any_no_permission = any(row.get("response_indication") == "no_permission" for row in probe_rows)
    if classification in {"IBKR_MARKET_DATA_DELAYED_ONLY", "IBKR_MARKET_DATA_PERMISSION_BLOCKED"} or any_delayed_only or any_no_permission:
        likely_cause = "The remaining gap appears to be market-data entitlement or TWS settings, not the read-only connection path."
    elif classification == "IBKR_MARKET_DATA_CONNECTED":
        likely_cause = "The TWS/API market-data path is working for these contracts in read-only mode."
    elif classification == "IBKR_MARKET_DATA_PARTIAL":
        likely_cause = "The read-only code path is working, but market-data availability is mixed across modes and looks more like session/entitlement behavior than order-path code."
    else:
        likely_cause = "The diagnostic could not prove the market-data path cleanly, so TWS/API session state still needs checking."
    manual_next_checks = [
        (
            f"Confirm TWS paper is still on {host}:{port} with API socket access enabled, localhost allowed, "
            "and Read-Only API enabled if your TWS build exposes that option."
        ),
        "Check IBKR market-data subscriptions or delayed-data availability for COMEX GC and MGC futures in the paper session.",
        "In TWS, verify market-data permissions/settings for futures are active and not blocked by another competing market-data session.",
    ]
    if any_delayed_only:
        manual_next_checks.append("TWS reported delayed-only behavior, so verify whether delayed futures data is enabled and whether live futures subscriptions are missing.")
    if not any_ticks:
        manual_next_checks.append("Because no usable bid/ask/last/close ticks were returned, inspect TWS market-data panels for GC/MGC directly to confirm whether quotes are visible there.")
    summary_lines = [
        f"TWS/API path works: {'yes' if api_path_works else 'no'}",
        likely_cause,
        f"The project can proceed with account/position/open-order truth without market data: {'yes' if api_path_works else 'no'}",
    ]
    if any_ticks:
        summary_lines.append("At least one mode returned usable bid/ask/last/close ticks.")
    else:
        summary_lines.append("No probe mode returned usable bid/ask/last/close ticks.")
    return {
        "tws_api_path_works": api_path_works,
        "issue_appears_entitlement_or_settings": classification in {"IBKR_MARKET_DATA_DELAYED_ONLY", "IBKR_MARKET_DATA_PERMISSION_BLOCKED"} or any_delayed_only or any_no_permission,
        "project_can_proceed_with_truth_without_market_data": api_path_works,
        "likely_cause": likely_cause,
        "summary_lines": summary_lines,
        "manual_next_checks": manual_next_checks,
    }


def write_ibkr_market_data_diagnostic_artifacts(
    *,
    output_dir: Path,
    artifacts: IbkrMarketDataDiagnosticArtifacts,
) -> None:
    reports_dir = Path(output_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "ibkr_market_data_diagnostic_report.json").write_text(
        json.dumps(artifacts.report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / "ibkr_market_data_diagnostic_report.md").write_text(
        render_ibkr_market_data_diagnostic_markdown(artifacts.report),
        encoding="utf-8",
    )
    with (reports_dir / "ibkr_market_data_mode_probe.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "contract_symbol",
                "contract_local_symbol",
                "requested_expiry",
                "probe_label",
                "request_type",
                "market_data_type_requested",
                "market_data_type_reported",
                "response_code",
                "response_message",
                "response_indication",
                "any_bid_tick",
                "any_ask_tick",
                "any_last_tick",
                "any_close_tick",
                "any_tick_returned",
                "bid_price",
                "ask_price",
                "last_price",
                "close_price",
                "status",
            ],
        )
        writer.writeheader()
        for row in artifacts.mode_probe_rows:
            writer.writerow(
                {
                    "contract_symbol": row.get("contract_symbol"),
                    "contract_local_symbol": row.get("contract_local_symbol"),
                    "requested_expiry": row.get("requested_expiry"),
                    "probe_label": row.get("probe_label"),
                    "request_type": row.get("request_type"),
                    "market_data_type_requested": row.get("market_data_type_requested"),
                    "market_data_type_reported": row.get("market_data_type_reported"),
                    "response_code": row.get("response_code"),
                    "response_message": row.get("response_message"),
                    "response_indication": row.get("response_indication"),
                    "any_bid_tick": row.get("any_bid_tick"),
                    "any_ask_tick": row.get("any_ask_tick"),
                    "any_last_tick": row.get("any_last_tick"),
                    "any_close_tick": row.get("any_close_tick"),
                    "any_tick_returned": row.get("any_tick_returned"),
                    "bid_price": row.get("bid_price"),
                    "ask_price": row.get("ask_price"),
                    "last_price": row.get("last_price"),
                    "close_price": row.get("close_price"),
                    "status": row.get("status"),
                }
            )


def render_ibkr_market_data_diagnostic_markdown(report: dict[str, Any]) -> str:
    connection = dict(report.get("connection_check") or {})
    environment = dict(report.get("environment_lock_check") or {})
    contract_report = dict(report.get("contract_qualification_check") or {})
    note = dict(report.get("diagnostic_note") or {})
    probes = list(report.get("mode_probes") or [])
    live_permission_failures = [
        row
        for row in probes
        if row.get("probe_label") == "live_snapshot"
        and row.get("response_indication") in {"no_permission", "delayed_only"}
        and not row.get("any_tick_returned")
    ]
    delayed_successes = [
        row
        for row in probes
        if row.get("probe_label") == "delayed_snapshot" and row.get("any_tick_returned")
    ]
    delayed_fallbacks = [
        row
        for row in probes
        if row.get("probe_label") in {"frozen_snapshot", "delayed_frozen_snapshot"}
        and row.get("market_data_type_reported") == 3
        and row.get("any_tick_returned")
    ]
    lines = [
        "# IBKR Market-Data Diagnostic Report",
        "",
        f"- classification: `{report.get('classification')}`",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- host: `{connection.get('host')}`",
        f"- port: `{connection.get('port')}`",
        f"- client_id: `{connection.get('client_id')}`",
        f"- server_version: `{connection.get('server_version')}`",
        "",
        "## Safety",
        "",
        f"- environment lock: `mode={environment.get('configured_mode')}, host={environment.get('configured_host')}, port={environment.get('configured_port')}, read_only={str(environment.get('read_only')).lower()}`",
        "- no orders were placed",
        "- no staged orders were created",
        "- no strategy execution was connected",
        "- no ATP/GC integration was connected",
        "- ATP/GC and live execution behavior were not touched",
        "",
        "## Qualification",
        "",
        f"- contract qualification ok: `{contract_report.get('ok')}`",
    ]
    for row in contract_report.get("contracts", []):
        lines.append(
            f"- {row.get('symbol')} {row.get('requested_expiry')}: `{'ok' if row.get('ok') else 'not_ok'}`"
        )
    lines.extend(
        [
            "",
            "## Verification Summary",
            "",
            "- live snapshots failed with permission-style responses"
            if live_permission_failures
            else "- live snapshots did not show permission-style failures",
            "- delayed snapshots returned usable bid/ask/last/close ticks for both contracts"
            if len(delayed_successes) >= 2
            else "- delayed snapshots did not return usable bid/ask/last/close ticks for both contracts",
            "- frozen/delayed-frozen requests fell back to delayed type 3"
            if len(delayed_fallbacks) >= 2
            else "- frozen/delayed-frozen requests did not both fall back to delayed type 3",
            (
                "- this points to a market-data entitlement/settings gap rather than a code/request-mode problem"
                if note.get("issue_appears_entitlement_or_settings")
                else "- the remaining gap does not clearly point to an entitlement/settings issue"
            ),
            (
                "- account/position/open-order truth can proceed without live market data"
                if note.get("project_can_proceed_with_truth_without_market_data")
                else "- account/position/open-order truth should not proceed until market-data blocking is resolved"
            ),
            "",
            "## Diagnostic Note",
            "",
        ]
    )
    for line in note.get("summary_lines", []):
        lines.append(f"- {line}")
    lines.extend(
        [
            "",
            "## Mode Probes",
            "",
        ]
    )
    for row in probes:
        lines.append(
            f"- {row.get('probe_label')} {row.get('contract_symbol')} {row.get('requested_expiry')}: "
            f"type={row.get('market_data_type_requested')} response={row.get('response_code')} "
            f"indication={row.get('response_indication')} ticks={row.get('any_tick_returned')}"
        )
    lines.extend(
        [
            "",
            "## Manual Checks",
            "",
        ]
    )
    for item in note.get("manual_next_checks", []):
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


@dataclass
class _DiagnosticRuntime:
    session: IbkrSession
    client: IbkrClient
    collector: IbkrReadOnlyProbeCollector
    transport: Any


def _build_runtime(
    *,
    config: IbkrMarketDataDiagnosticConfig,
    transport_factory: Callable[..., Any],
    module_loader: Callable[[str], Any] | None,
) -> _DiagnosticRuntime:
    session = IbkrSession(
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        account_id=config.account_id,
        gateway_mode="paper",
        read_only=config.read_only,
        order_id_policy=build_default_ibkr_order_id_policy(
            client_id=config.client_id,
            live_orders_enabled=False,
        ),
    )
    client = IbkrClient(session=session)
    collector = IbkrReadOnlyProbeCollector(client)
    transport = transport_factory(
        client=client,
        collector=collector,
        config=IbkrReadOnlyApiTransportConfig(
            host=config.host,
            port=config.port,
            client_id=config.client_id,
            read_only=config.read_only,
        ),
        module_loader=module_loader,
    )
    return _DiagnosticRuntime(session=session, client=client, collector=collector, transport=transport)


def _start_runtime(
    runtime: _DiagnosticRuntime,
    *,
    timeout_seconds: float,
    sleep_fn: Callable[[float], None],
) -> None:
    import threading

    thread = threading.Thread(target=runtime.transport.run_loop, name="ibkr_market_data_diag_loop", daemon=True)
    thread.start()
    if not _wait_for_connection_ready(
        transport=runtime.transport,
        collector=runtime.collector,
        timeout_seconds=timeout_seconds,
        sleep_fn=sleep_fn,
    ):
        latest_error = runtime.collector.latest_error()
        if latest_error is None:
            raise IbkrReadOnlyVerificationError(
                f"TWS paper API did not finish the initial handshake within {timeout_seconds:.1f}s."
            )
        raise IbkrReadOnlyVerificationError(
            f"TWS paper API error {latest_error['code']}: {latest_error['message']}"
        )


def _run_mode_probes(
    *,
    transport: Any,
    collector: IbkrReadOnlyProbeCollector,
    contract_report: dict[str, Any],
    timeout_seconds: float,
    sleep_fn: Callable[[float], None],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    request_id = 5100
    for contract_row in contract_report.get("contracts", []):
        if not contract_row.get("ok"):
            continue
        qualified = dict(contract_row.get("qualified_contract") or {})
        contract = _MarketDataContract(
            symbol=str(contract_row.get("symbol") or ""),
            local_symbol=str(qualified.get("local_symbol") or ""),
            expiry=str(contract_row.get("requested_expiry") or ""),
            broker_symbol=str(qualified.get("broker_symbol") or ""),
            exchange=str(qualified.get("exchange") or ""),
            currency=str(qualified.get("currency") or ""),
            security_type=str(qualified.get("security_type") or ""),
            multiplier=_optional_text(qualified.get("multiplier")),
            trading_class=_optional_text(qualified.get("trading_class")),
        )
        for probe_mode in _PROBE_MODES:
            request_id += 1
            rows.append(
                _probe_market_data_mode(
                    transport=transport,
                    collector=collector,
                    contract=contract,
                    probe_mode=probe_mode,
                    request_id=request_id,
                    timeout_seconds=timeout_seconds,
                    sleep_fn=sleep_fn,
                )
            )
    return rows


@dataclass(frozen=True)
class _MarketDataContract:
    symbol: str
    local_symbol: str
    expiry: str
    broker_symbol: str
    exchange: str
    currency: str
    security_type: str
    multiplier: str | None
    trading_class: str | None


def _probe_market_data_mode(
    *,
    transport: Any,
    collector: IbkrReadOnlyProbeCollector,
    contract: _MarketDataContract,
    probe_mode: _ProbeMode,
    request_id: int,
    timeout_seconds: float,
    sleep_fn: Callable[[float], None],
) -> dict[str, Any]:
    event = collector.market_data_event(request_id)
    transport.req_market_data_type(market_data_type=probe_mode.market_data_type_requested)
    transport.req_market_data_snapshot(
        request_id=request_id,
        contract=_raw_contract_payload(contract),
    )
    event_ok = _wait_for_event(
        event,
        collector=collector,
        timeout_seconds=timeout_seconds,
        sleep_fn=sleep_fn,
    )
    raw_row = dict(collector.market_data_rows.get(request_id, {}))
    related_errors = [
        error
        for error in collector.errors
        if error.get("request_id") == request_id
    ]
    latest_error = related_errors[-1] if related_errors else None
    tick_summary = _extract_tick_summary(raw_row)
    response_indication = _classify_probe_response(
        latest_error=latest_error,
        tick_summary=tick_summary,
        market_data_type_reported=raw_row.get("market_data_type"),
        event_ok=event_ok,
    )
    status = "usable_ticks" if tick_summary["any_tick_returned"] else "no_ticks"
    return {
        "contract_symbol": contract.symbol,
        "contract_local_symbol": contract.local_symbol,
        "requested_expiry": contract.expiry,
        "probe_label": probe_mode.label,
        "request_type": probe_mode.request_type,
        "market_data_type_requested": probe_mode.market_data_type_requested,
        "market_data_type_reported": raw_row.get("market_data_type"),
        "response_code": None if latest_error is None else latest_error.get("code"),
        "response_message": None if latest_error is None else latest_error.get("message"),
        "response_indication": response_indication,
        "any_bid_tick": tick_summary["any_bid_tick"],
        "any_ask_tick": tick_summary["any_ask_tick"],
        "any_last_tick": tick_summary["any_last_tick"],
        "any_close_tick": tick_summary["any_close_tick"],
        "any_tick_returned": tick_summary["any_tick_returned"],
        "bid_price": tick_summary["bid_price"],
        "ask_price": tick_summary["ask_price"],
        "last_price": tick_summary["last_price"],
        "close_price": tick_summary["close_price"],
        "status": status,
        "raw_market_data": raw_row,
        "errors": related_errors,
    }


def _raw_contract_payload(contract: _MarketDataContract) -> Any:
    from ..brokers.ibkr import IbkrQualifiedContract

    return IbkrQualifiedContract(
        internal_symbol=contract.symbol,
        broker_symbol=contract.broker_symbol,
        local_symbol=contract.local_symbol,
        security_type=contract.security_type,
        exchange=contract.exchange,
        currency=contract.currency,
        expiry=contract.expiry,
        multiplier=contract.multiplier,
        trading_class=contract.trading_class,
        con_id=None,
        metadata=None,
    )


def _extract_tick_summary(raw_row: dict[str, Any]) -> dict[str, Any]:
    tick_prices = dict(raw_row.get("tick_prices") or {})
    bid_price = _first_price(tick_prices, _BID_TICK_IDS)
    ask_price = _first_price(tick_prices, _ASK_TICK_IDS)
    last_price = _first_price(tick_prices, _LAST_TICK_IDS)
    close_price = _first_price(tick_prices, _CLOSE_TICK_IDS)
    return {
        "any_bid_tick": bid_price is not None,
        "any_ask_tick": ask_price is not None,
        "any_last_tick": last_price is not None,
        "any_close_tick": close_price is not None,
        "any_tick_returned": any(value is not None for value in (bid_price, ask_price, last_price, close_price)),
        "bid_price": bid_price,
        "ask_price": ask_price,
        "last_price": last_price,
        "close_price": close_price,
    }


def _classify_probe_response(
    *,
    latest_error: dict[str, Any] | None,
    tick_summary: dict[str, Any],
    market_data_type_reported: Any,
    event_ok: bool,
) -> str:
    code = None if latest_error is None else int(latest_error.get("code"))
    if tick_summary["any_tick_returned"]:
        if market_data_type_reported in {3, 4} or code in _DELAYED_ONLY_ERROR_CODES:
            return "delayed_only"
        return "data_returned"
    if code in _DELAYED_ONLY_ERROR_CODES:
        return "delayed_only"
    if code in _PERMISSION_ERROR_CODES:
        return "no_permission"
    if latest_error is not None or event_ok:
        return "unavailable"
    return "unavailable"


def _first_price(tick_prices: dict[str, Any], tick_ids: tuple[int, ...]) -> Any:
    for tick_id in tick_ids:
        key = str(int(tick_id))
        if key in tick_prices:
            return tick_prices[key]
    return None


def _optional_text(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _blocked_artifacts(
    *,
    config: IbkrMarketDataDiagnosticConfig,
    started_at: datetime,
    environment_lock: dict[str, Any],
    detail: str,
    contract_report: dict[str, Any] | None = None,
    selected_account_id: str | None = None,
    connection_check: dict[str, Any] | None = None,
) -> IbkrMarketDataDiagnosticArtifacts:
    report = {
        "classification": "IBKR_MARKET_DATA_BLOCKED",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "started_at": started_at.isoformat(),
        "connection_check": connection_check
        or {
            "connected": False,
            "host": config.host,
            "port": config.port,
            "client_id": config.client_id,
            "server_version": None,
            "tws_connection_time": None,
            "connection_timestamp": started_at.isoformat(),
        },
        "environment_lock_check": {
            **environment_lock,
            "account_id": selected_account_id or config.account_id,
        },
        "contract_qualification_check": contract_report
        or {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "ok": False,
            "contracts": [],
            "detail": detail,
        },
        "mode_probes": [],
        "diagnostic_note": {
            "tws_api_path_works": False,
            "issue_appears_entitlement_or_settings": False,
            "project_can_proceed_with_truth_without_market_data": False,
            "likely_cause": detail,
            "summary_lines": [detail],
            "manual_next_checks": [_tws_manual_check_message(config.host, config.port)],
        },
        "errors": [],
        "next_manual_checks": [_tws_manual_check_message(config.host, config.port)],
    }
    return IbkrMarketDataDiagnosticArtifacts(
        classification="IBKR_MARKET_DATA_BLOCKED",
        report=report,
        mode_probe_rows=[],
    )
