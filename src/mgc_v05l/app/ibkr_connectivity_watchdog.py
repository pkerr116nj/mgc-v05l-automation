"""Read-only TWS/IBKR connectivity watchdog for Track B PAPER."""

from __future__ import annotations

import argparse
import json
import socket
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

from ..execution_core.ibkr_readonly_transport import (
    IbkrReadOnlyTimeoutError,
    IbkrReadOnlyTransportConfig,
    IbkrReadOnlyTwsTransport,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_PATH = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "ibkr_connectivity_watchdog"
    / "latest_ibkr_connectivity_watchdog.json"
)
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 7497
DEFAULT_ACCOUNT = "DUM882026"
WATCHDOG_CLIENT_ID_BASE = 9800
WATCHDOG_CLIENT_ID_SPAN = 100
PROBE_CONTRACT_KEY = "MGC-202606"

CONNECTED_READ_ONLY = "IBKR_CONNECTED_READ_ONLY"
CONNECTED_BUT_INCOMPLETE = "IBKR_CONNECTED_BUT_INCOMPLETE"
TWS_NOT_LISTENING = "TWS_NOT_LISTENING"
IBKR_HANDSHAKE_TIMEOUT = "IBKR_HANDSHAKE_TIMEOUT"
MANAGED_ACCOUNTS_TIMEOUT = "MANAGED_ACCOUNTS_TIMEOUT"
POSITIONS_TIMEOUT = "POSITIONS_TIMEOUT"
OPEN_ORDERS_TIMEOUT = "OPEN_ORDERS_TIMEOUT"
API_MODAL_OR_BLOCKED_SUSPECTED = "API_MODAL_OR_BLOCKED_SUSPECTED"
CLIENT_ID_COLLISION_SUSPECTED = "CLIENT_ID_COLLISION_SUSPECTED"


@dataclass(frozen=True)
class TcpProbeResult:
    listening: bool
    latency_ms: float | None
    failure_reason: str | None = None


@dataclass(frozen=True)
class IbkrConnectivityWatchdogConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_OUTPUT_PATH
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    account_id: str = DEFAULT_ACCOUNT
    client_id: int | None = None
    timeout_seconds: float = 8.0
    read_only: bool = True


def default_watchdog_client_id(*, now_fn: Callable[[], datetime] | None = None) -> int:
    now = (now_fn or (lambda: datetime.now(timezone.utc)))()
    return WATCHDOG_CLIENT_ID_BASE + (int(now.timestamp()) % WATCHDOG_CLIENT_ID_SPAN)


def tcp_port_probe(*, host: str, port: int, timeout_seconds: float) -> TcpProbeResult:
    started = time.monotonic()
    try:
        with socket.create_connection((host, int(port)), timeout=float(timeout_seconds)):
            latency_ms = (time.monotonic() - started) * 1000.0
            return TcpProbeResult(listening=True, latency_ms=round(latency_ms, 3))
    except ConnectionRefusedError:
        return TcpProbeResult(
            listening=False,
            latency_ms=None,
            failure_reason=f"connection refused for {host}:{port}",
        )
    except TimeoutError:
        return TcpProbeResult(
            listening=False,
            latency_ms=None,
            failure_reason=f"timeout connecting to {host}:{port}",
        )
    except OSError as exc:
        return TcpProbeResult(
            listening=False,
            latency_ms=None,
            failure_reason=f"{type(exc).__name__}: {exc}",
        )


def run_ibkr_connectivity_watchdog(
    *,
    config: IbkrConnectivityWatchdogConfig,
    tcp_checker: Callable[..., TcpProbeResult] = tcp_port_probe,
    transport_factory: Callable[[], Any] | None = None,
    now_fn: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    now_fn = now_fn or (lambda: datetime.now(timezone.utc))
    generated_at = now_fn().astimezone(timezone.utc).isoformat()
    client_id = int(config.client_id or default_watchdog_client_id(now_fn=now_fn))
    started = time.monotonic()
    tcp_result = tcp_checker(
        host=config.host,
        port=int(config.port),
        timeout_seconds=float(config.timeout_seconds),
    )
    checks: dict[str, Any] = {
        "tcp_port": {
            "ok": bool(tcp_result.listening),
            "host": config.host,
            "port": int(config.port),
            "latency_ms": tcp_result.latency_ms,
            "failure_reason": tcp_result.failure_reason,
        }
    }
    if not tcp_result.listening:
        return _watchdog_report(
            config=config,
            generated_at=generated_at,
            client_id=client_id,
            classification=TWS_NOT_LISTENING,
            checks=checks,
            latency_ms=_elapsed_ms(started),
            failure_reason=tcp_result.failure_reason,
            diagnostics={},
        )

    transport = (
        transport_factory()
        if transport_factory is not None
        else IbkrReadOnlyTwsTransport(
            config=IbkrReadOnlyTransportConfig(request_timeout_seconds=float(config.timeout_seconds))
        )
    )
    diagnostics: dict[str, Any] = {}
    failure_reason: str | None = None
    classification = CONNECTED_BUT_INCOMPLETE
    try:
        transport.connect(host=config.host, port=int(config.port), client_id=client_id, readonly=bool(config.read_only))
        next_valid_id = transport.next_valid_id()
        diagnostics = _diagnostics(transport)
        checks["connect_ack"] = {"ok": diagnostics.get("connect_ack_received") is True}
        checks["next_valid_id"] = {"ok": next_valid_id is not None, "value": next_valid_id}

        accounts = tuple(str(account) for account in transport.managed_accounts())
        checks["managed_accounts"] = {
            "ok": bool(accounts),
            "accounts": list(accounts),
            "requested_account_present": str(config.account_id) in accounts,
        }
        if str(config.account_id) not in accounts:
            failure_reason = f"managed accounts did not include {config.account_id}"
            return _watchdog_report(
                config=config,
                generated_at=generated_at,
                client_id=client_id,
                classification=CONNECTED_BUT_INCOMPLETE,
                checks=checks,
                latency_ms=_elapsed_ms(started),
                failure_reason=failure_reason,
                diagnostics=_diagnostics(transport),
            )

        transport.snapshot_position(
            run_id="ibkr-connectivity-watchdog",
            account_id=str(config.account_id),
            contract_key=PROBE_CONTRACT_KEY,
            observed_at=now_fn(),
        )
        checks["positions_complete"] = {"ok": True}

        transport.snapshot_open_orders(
            account_id=str(config.account_id),
            contract_key=PROBE_CONTRACT_KEY,
            observed_at=now_fn(),
        )
        checks["open_orders_complete"] = {"ok": True}

        diagnostics = _diagnostics(transport)
        classification = CONNECTED_READ_ONLY if _all_required_checks_pass(checks) else CONNECTED_BUT_INCOMPLETE
        if classification == CONNECTED_BUT_INCOMPLETE:
            failure_reason = "IBKR read-only callbacks completed but one or more required checks were incomplete"
    except IbkrReadOnlyTimeoutError as exc:
        diagnostics = _diagnostics(transport)
        failure_reason = str(exc)
        classification = _classify_timeout(failure_reason=failure_reason, diagnostics=diagnostics)
        _record_timeout_check(checks=checks, failure_reason=failure_reason)
    except Exception as exc:  # pragma: no cover - exact external IBKR exceptions vary.
        diagnostics = _diagnostics(transport)
        failure_reason = f"{type(exc).__name__}: {exc}"
        classification = _classify_transport_error(failure_reason=failure_reason, diagnostics=diagnostics)
    finally:
        try:
            transport.disconnect()
        except Exception:
            pass

    return _watchdog_report(
        config=config,
        generated_at=generated_at,
        client_id=client_id,
        classification=classification,
        checks=checks,
        latency_ms=_elapsed_ms(started),
        failure_reason=failure_reason,
        diagnostics=diagnostics,
    )


def write_watchdog_report(*, output_path: Path, report: dict[str, Any]) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="track-b-ibkr-connectivity-watchdog")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--account-id", default=DEFAULT_ACCOUNT)
    parser.add_argument("--client-id", type=int, default=None)
    parser.add_argument("--timeout-seconds", type=float, default=8.0)
    parser.add_argument("--output-path", type=Path, default=None)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = Path(args.repo_root)
    output_path = (
        Path(args.output_path)
        if args.output_path is not None
        else repo_root / "outputs" / "reports" / "ibkr_connectivity_watchdog" / "latest_ibkr_connectivity_watchdog.json"
    )
    config = IbkrConnectivityWatchdogConfig(
        repo_root=repo_root,
        output_path=output_path,
        host=str(args.host or "").strip(),
        port=int(args.port),
        account_id=str(args.account_id or "").strip(),
        client_id=args.client_id,
        timeout_seconds=float(args.timeout_seconds),
        read_only=True,
    )
    report = run_ibkr_connectivity_watchdog(config=config)
    write_watchdog_report(output_path=config.output_path, report=report)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(_compact_summary(report))
    return 0 if report.get("classification") == CONNECTED_READ_ONLY else 1


def _watchdog_report(
    *,
    config: IbkrConnectivityWatchdogConfig,
    generated_at: str,
    client_id: int,
    classification: str,
    checks: dict[str, Any],
    latency_ms: float,
    failure_reason: str | None,
    diagnostics: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "track_b_ibkr_connectivity_watchdog_v1",
        "service": "track_b_ibkr_connectivity_watchdog",
        "generated_at": generated_at,
        "classification": classification,
        "host": config.host,
        "port": int(config.port),
        "account": config.account_id,
        "client_id": int(client_id),
        "client_id_policy": {
            "mode": "rotating_high_range" if config.client_id is None else "explicit",
            "range_start": WATCHDOG_CLIENT_ID_BASE,
            "range_end": WATCHDOG_CLIENT_ID_BASE + WATCHDOG_CLIENT_ID_SPAN - 1,
            "avoids_known_track_b_sidecars": True,
        },
        "read_only": True,
        "paper_only": True,
        "submit_authority": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "latency_ms": latency_ms,
        "failure_reason": failure_reason,
        "checks": checks,
        "transport_diagnostics": diagnostics,
        "recommendations": _recommendations_for(classification),
        "future_supervised_repair_policy": _future_supervised_repair_policy(),
    }


def _classify_timeout(*, failure_reason: str, diagnostics: dict[str, Any]) -> str:
    lowered = failure_reason.lower()
    if _has_error_code(diagnostics, 326):
        return CLIENT_ID_COLLISION_SUSPECTED
    if "managedaccounts" in lowered:
        return MANAGED_ACCOUNTS_TIMEOUT
    if "positionend" in lowered:
        return POSITIONS_TIMEOUT
    if "openorderend" in lowered:
        return OPEN_ORDERS_TIMEOUT
    if _modal_or_blocked_suspected(diagnostics):
        return API_MODAL_OR_BLOCKED_SUSPECTED
    return IBKR_HANDSHAKE_TIMEOUT


def _classify_transport_error(*, failure_reason: str, diagnostics: dict[str, Any]) -> str:
    if _has_error_code(diagnostics, 326) or "client id" in failure_reason.lower():
        return CLIENT_ID_COLLISION_SUSPECTED
    if _modal_or_blocked_suspected(diagnostics):
        return API_MODAL_OR_BLOCKED_SUSPECTED
    return IBKR_HANDSHAKE_TIMEOUT


def _modal_or_blocked_suspected(diagnostics: dict[str, Any]) -> bool:
    if _has_error_code(diagnostics, 502) or _has_error_code(diagnostics, 504):
        return True
    causes = " ".join(str(item) for item in diagnostics.get("suspected_causes") or ())
    lowered = causes.lower()
    return "modal" in lowered or "api disabled" in lowered or "not accepting clients" in lowered


def _has_error_code(diagnostics: dict[str, Any], code: int) -> bool:
    for error in diagnostics.get("ibkr_errors") or ():
        try:
            if int(error.get("error_code")) == int(code):
                return True
        except (AttributeError, TypeError, ValueError):
            continue
    return False


def _record_timeout_check(*, checks: dict[str, Any], failure_reason: str) -> None:
    lowered = failure_reason.lower()
    if "managedaccounts" in lowered:
        checks["managed_accounts"] = {"ok": False, "failure_reason": failure_reason}
    elif "positionend" in lowered:
        checks["positions_complete"] = {"ok": False, "failure_reason": failure_reason}
    elif "openorderend" in lowered:
        checks["open_orders_complete"] = {"ok": False, "failure_reason": failure_reason}
    elif "nextvalidid" in lowered:
        checks["next_valid_id"] = {"ok": False, "failure_reason": failure_reason}
    else:
        checks["handshake"] = {"ok": False, "failure_reason": failure_reason}


def _all_required_checks_pass(checks: dict[str, Any]) -> bool:
    required = (
        "tcp_port",
        "connect_ack",
        "next_valid_id",
        "managed_accounts",
        "positions_complete",
        "open_orders_complete",
    )
    for name in required:
        check = checks.get(name)
        if not isinstance(check, dict) or check.get("ok") is not True:
            return False
    managed = checks.get("managed_accounts")
    return isinstance(managed, dict) and managed.get("requested_account_present") is True


def _diagnostics(transport: Any) -> dict[str, Any]:
    diagnostics_report = getattr(transport, "diagnostics_report", None)
    if not callable(diagnostics_report):
        return {}
    try:
        payload = diagnostics_report()
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _elapsed_ms(started: float) -> float:
    return round((time.monotonic() - started) * 1000.0, 3)


def _recommendations_for(classification: str) -> list[str]:
    if classification == CONNECTED_READ_ONLY:
        return [
            "Keep broker-truth refresher running as the canonical broker-truth producer.",
            "Use canonical readiness to decide submit capability; this watchdog is diagnostic only.",
        ]
    if classification == TWS_NOT_LISTENING:
        return [
            "Confirm TWS is running and logged into PAPER.",
            "Confirm TWS paper API socket is configured for 127.0.0.1:7497.",
            "Keep submit blocked until broker truth refresh is fresh and Phase-1 reconciliation is clean.",
        ]
    if classification == CLIENT_ID_COLLISION_SUSPECTED:
        return [
            "Retry with a different high-range watchdog client id.",
            "Inspect existing IBKR API clients before changing runtime services.",
        ]
    if classification in {IBKR_HANDSHAKE_TIMEOUT, MANAGED_ACCOUNTS_TIMEOUT, API_MODAL_OR_BLOCKED_SUSPECTED}:
        return [
            "Inspect TWS for API prompts, modal dialogs, or disabled API settings.",
            "Do not restart runtime solely from this watchdog result.",
            "Keep submit blocked through canonical readiness until broker truth is fresh.",
        ]
    return [
        "Preserve last-good broker truth; do not replace it with incomplete data.",
        "Retry read-only watchdog after the next broker-truth refresh interval.",
        "Keep submit blocked through canonical readiness until complete broker truth is available.",
    ]


def _future_supervised_repair_policy() -> dict[str, Any]:
    return {
        "retry": "Allowed for read-only checks with bounded backoff.",
        "rotate_client_id": "Allowed inside the watchdog high client-id range after suspected collision.",
        "operator_alert": "Required for repeated modal/API-block suspicions or TWS not listening.",
        "restart_tws": "Not allowed by this app; requires explicit operator action outside this watchdog.",
        "block_submit": "Canonical readiness must block submit while broker truth is stale or incomplete.",
        "auto_repair_enabled": False,
    }


def _compact_summary(report: dict[str, Any]) -> str:
    checks = dict(report.get("checks") or {})
    return "\n".join(
        [
            f"classification={report.get('classification')}",
            f"host={report.get('host')}",
            f"port={report.get('port')}",
            f"account={report.get('account')}",
            f"client_id={report.get('client_id')}",
            f"tcp_listening={dict(checks.get('tcp_port') or {}).get('ok')}",
            f"connect_ack={dict(checks.get('connect_ack') or {}).get('ok')}",
            f"managed_accounts={dict(checks.get('managed_accounts') or {}).get('ok')}",
            f"positions_complete={dict(checks.get('positions_complete') or {}).get('ok')}",
            f"open_orders_complete={dict(checks.get('open_orders_complete') or {}).get('ok')}",
            f"failure_reason={report.get('failure_reason')}",
        ]
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
