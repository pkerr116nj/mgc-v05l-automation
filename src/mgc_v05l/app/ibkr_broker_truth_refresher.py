"""Read-only periodic IBKR broker-truth refresh service for Track B PAPER."""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from ..execution.ibkr_read_only_verifier import (
    IbkrReadOnlyVerificationArtifacts,
    IbkrReadOnlyVerificationConfig,
    verify_ibkr_read_only_connection,
    write_ibkr_read_only_artifacts,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_read_only_verification"
DEFAULT_STATUS_PATH = DEFAULT_OUTPUT_DIR / "ibkr_broker_truth_refresh_status.json"
DEFAULT_VAR_STATUS_PATH = REPO_ROOT / "var" / "ibkr_broker_truth_refresh_status.json"
DEFAULT_REFRESH_SECONDS = 60.0
DEFAULT_CLIENT_ID = 9077


@dataclass(frozen=True)
class BrokerTruthRefreshConfig:
    repo_root: Path = REPO_ROOT
    output_dir: Path = DEFAULT_OUTPUT_DIR
    status_path: Path = DEFAULT_STATUS_PATH
    var_status_path: Path = DEFAULT_VAR_STATUS_PATH
    refresh_seconds: float = DEFAULT_REFRESH_SECONDS
    mode: str = "PAPER"
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = DEFAULT_CLIENT_ID
    account_id: str = "DUM882026"
    read_only: bool = True
    timeout_seconds: float = 8.0
    skip_market_data_probe: bool = True
    skip_duplicate_client_id_probe: bool = True
    gc_expiry: str = "202606"
    mgc_expiry: str = "202606"


def refresh_seconds_from_env(env: dict[str, str] | None = None) -> float:
    env = env or os.environ
    raw_value = str(env.get("TRACK_B_BROKER_TRUTH_REFRESH_SECONDS") or "").strip()
    if not raw_value:
        return DEFAULT_REFRESH_SECONDS
    try:
        value = float(raw_value)
    except ValueError:
        return DEFAULT_REFRESH_SECONDS
    return value if value > 0 else DEFAULT_REFRESH_SECONDS


def run_broker_truth_refresh_once(
    *,
    config: BrokerTruthRefreshConfig,
    verifier: Callable[..., IbkrReadOnlyVerificationArtifacts] = verify_ibkr_read_only_connection,
    artifact_writer: Callable[..., None] = write_ibkr_read_only_artifacts,
    now_fn: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    now_fn = now_fn or (lambda: datetime.now(timezone.utc))
    started_at = now_fn()
    artifacts: IbkrReadOnlyVerificationArtifacts | None = None
    error: str | None = None
    try:
        artifacts = verifier(config=_verification_config(config))
        artifact_writer(output_dir=config.output_dir, artifacts=artifacts)
    except Exception as exc:  # pragma: no cover - exercised with explicit unit fake
        error = str(exc)
    completed_at = now_fn()
    status = build_broker_truth_refresh_status(
        config=config,
        started_at=started_at,
        completed_at=completed_at,
        artifacts=artifacts,
        error=error,
    )
    write_broker_truth_refresh_status(status_path=config.status_path, var_status_path=config.var_status_path, status=status)
    return status


def run_broker_truth_refresh_service(
    *,
    config: BrokerTruthRefreshConfig,
    max_cycles: int = 0,
    sleep_fn: Callable[[float], None] = time.sleep,
    verifier: Callable[..., IbkrReadOnlyVerificationArtifacts] = verify_ibkr_read_only_connection,
    artifact_writer: Callable[..., None] = write_ibkr_read_only_artifacts,
) -> dict[str, Any]:
    cycle = 0
    latest_status: dict[str, Any] = {
        "classification": "BROKER_TRUTH_REFRESH_NOT_RUN",
        "service_mode": "loop",
        "refresh_seconds": float(config.refresh_seconds),
    }
    while max_cycles <= 0 or cycle < max_cycles:
        cycle += 1
        latest_status = run_broker_truth_refresh_once(
            config=config,
            verifier=verifier,
            artifact_writer=artifact_writer,
        )
        latest_status["cycle"] = cycle
        write_broker_truth_refresh_status(
            status_path=config.status_path,
            var_status_path=config.var_status_path,
            status=latest_status,
        )
        if max_cycles > 0 and cycle >= max_cycles:
            break
        sleep_fn(float(config.refresh_seconds))
    return latest_status


def build_broker_truth_refresh_status(
    *,
    config: BrokerTruthRefreshConfig,
    started_at: datetime,
    completed_at: datetime,
    artifacts: IbkrReadOnlyVerificationArtifacts | None,
    error: str | None,
) -> dict[str, Any]:
    generated_at = completed_at.astimezone(timezone.utc).isoformat()
    positions = dict(artifacts.positions_snapshot) if artifacts is not None else {}
    open_orders = dict(artifacts.open_orders_snapshot) if artifacts is not None else {}
    connection = dict(artifacts.connection_report) if artifacts is not None else {}
    positions_complete = positions.get("positions_complete") is True
    open_orders_complete = open_orders.get("open_orders_complete") is True
    benign_account_unsubscribe_ignored = _benign_account_unsubscribe_after_complete_truth(
        artifacts=artifacts,
        connection=connection,
        positions=positions,
        open_orders=open_orders,
        positions_complete=positions_complete,
        open_orders_complete=open_orders_complete,
        error=error,
    )
    success = (
        artifacts is not None
        and (artifacts.classification != "IBKR_READ_ONLY_BLOCKED" or benign_account_unsubscribe_ignored)
        and positions.get("ok") is True
        and open_orders.get("ok") is True
        and positions_complete
        and open_orders_complete
        and error is None
    )
    position_count = int(positions.get("position_count") or 0)
    open_order_count = int(open_orders.get("open_order_count") or 0)
    age_seconds = max((datetime.now(timezone.utc) - completed_at.astimezone(timezone.utc)).total_seconds(), 0.0)
    last_error = error or _connection_detail(connection) if not success else None
    return {
        "schema_version": "track_b_broker_truth_refresh_status_v1",
        "classification": "BROKER_TRUTH_REFRESH_READY" if success else "BROKER_TRUTH_REFRESH_FAILED",
        "service": "track_b_ibkr_broker_truth_refresh",
        "read_only": bool(config.read_only),
        "mode": config.mode,
        "account": positions.get("selected_account_id") or open_orders.get("selected_account_id") or config.account_id,
        "host": config.host,
        "port": int(config.port),
        "client_id": int(config.client_id),
        "refresh_seconds": float(config.refresh_seconds),
        "generated_at": generated_at,
        "latest_refresh_time": generated_at if success else None,
        "last_success_at": generated_at if success else None,
        "last_failure_at": generated_at if not success else None,
        "last_success": bool(success),
        "last_failure": not bool(success),
        "last_error": last_error,
        "age_seconds": age_seconds,
        "positions_complete": positions_complete,
        "open_orders_complete": open_orders_complete,
        "position_count": position_count,
        "open_order_count": open_order_count,
        "benign_account_unsubscribe_ignored": benign_account_unsubscribe_ignored,
        "positions_snapshot_path": str(config.output_dir / "ibkr_positions_snapshot.json"),
        "open_orders_snapshot_path": str(config.output_dir / "ibkr_open_orders_snapshot.json"),
        "connection_report_path": str(config.output_dir / "ibkr_read_only_connection_report.json"),
        "started_at": started_at.astimezone(timezone.utc).isoformat(),
        "completed_at": generated_at,
        "verifier_classification": artifacts.classification if artifacts is not None else "NOT_RUN",
        "submit_authority": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }


def _benign_account_unsubscribe_after_complete_truth(
    *,
    artifacts: IbkrReadOnlyVerificationArtifacts | None,
    connection: dict[str, Any],
    positions: dict[str, Any],
    open_orders: dict[str, Any],
    positions_complete: bool,
    open_orders_complete: bool,
    error: str | None,
) -> bool:
    if artifacts is None or error is not None:
        return False
    if artifacts.classification != "IBKR_READ_ONLY_BLOCKED":
        return False
    if positions.get("ok") is not True or open_orders.get("ok") is not True:
        return False
    if not positions_complete or not open_orders_complete:
        return False
    checks = (
        dict(connection.get("connection_check") or {}),
        dict(connection.get("account_truth_check") or {}),
        dict(connection.get("position_truth_check") or {}),
        dict(connection.get("open_order_truth_check") or {}),
    )
    if any(check.get("ok") is False or check.get("connected") is False for check in checks):
        return False
    severe_errors = []
    benign_unsubscribe_seen = False
    for row in list(connection.get("errors") or []):
        if not isinstance(row, dict):
            severe_errors.append(row)
            continue
        try:
            code = int(row.get("code"))
        except (TypeError, ValueError):
            severe_errors.append(row)
            continue
        message = str(row.get("message") or "")
        if code == 2100 and "unsubscribed from account data" in message.lower():
            benign_unsubscribe_seen = True
            continue
        if code in {2104, 2106, 2158} and "connection is OK" in message:
            continue
        severe_errors.append(row)
    return benign_unsubscribe_seen and not severe_errors


def write_broker_truth_refresh_status(*, status_path: Path, var_status_path: Path, status: dict[str, Any]) -> None:
    _write_json_atomically(status_path, status)
    _write_json_atomically(var_status_path, status)


def load_broker_truth_refresh_status(*, status_path: Path = DEFAULT_STATUS_PATH) -> dict[str, Any]:
    try:
        payload = json.loads(status_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "classification": "BROKER_TRUTH_REFRESH_STATUS_MISSING",
            "path": str(status_path),
            "available": False,
            "live_money_eligible": False,
        }
    if not isinstance(payload, dict):
        return {
            "classification": "BROKER_TRUTH_REFRESH_STATUS_MALFORMED",
            "path": str(status_path),
            "available": False,
            "live_money_eligible": False,
        }
    return {**payload, "available": True, "path": str(status_path)}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="track-b-broker-truth-refresh")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Run one read-only broker-truth refresh cycle.")
    mode.add_argument("--service", action="store_true", help="Run periodic read-only broker-truth refresh cycles.")
    mode.add_argument("--status", action="store_true", help="Print the latest broker-truth refresh status artifact.")
    parser.add_argument("--refresh-seconds", type=float, default=refresh_seconds_from_env())
    parser.add_argument("--mode", default="PAPER")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7497)
    parser.add_argument("--client-id", type=int, default=DEFAULT_CLIENT_ID)
    parser.add_argument("--account-id", default="DUM882026")
    parser.add_argument("--read-only", action="store_true", help="Required. Fails closed if omitted.")
    parser.add_argument("--timeout-seconds", type=float, default=8.0)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--status-path", type=Path, default=DEFAULT_STATUS_PATH)
    parser.add_argument("--var-status-path", type=Path, default=DEFAULT_VAR_STATUS_PATH)
    parser.add_argument("--max-cycles", type=int, default=0)
    parser.add_argument("--gc-expiry", default="202606")
    parser.add_argument("--mgc-expiry", default="202606")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = BrokerTruthRefreshConfig(
        repo_root=REPO_ROOT,
        output_dir=Path(args.output_dir),
        status_path=Path(args.status_path),
        var_status_path=Path(args.var_status_path),
        refresh_seconds=float(args.refresh_seconds),
        mode=str(args.mode or "").strip().upper(),
        host=str(args.host or "").strip(),
        port=int(args.port),
        client_id=int(args.client_id),
        account_id=str(args.account_id or "").strip(),
        read_only=bool(args.read_only),
        timeout_seconds=float(args.timeout_seconds),
        gc_expiry=str(args.gc_expiry or "").strip(),
        mgc_expiry=str(args.mgc_expiry or "").strip(),
    )
    if args.status:
        print(json.dumps(load_broker_truth_refresh_status(status_path=config.status_path), indent=2, sort_keys=True))
        return 0
    if not config.read_only:
        print(json.dumps({"classification": "BROKER_TRUTH_REFRESH_BLOCKED", "reason": "read_only_flag_required"}, indent=2))
        return 2
    status = (
        run_broker_truth_refresh_service(config=config, max_cycles=int(args.max_cycles))
        if args.service
        else run_broker_truth_refresh_once(config=config)
    )
    print(json.dumps(status, indent=2, sort_keys=True))
    return 0 if status.get("classification") == "BROKER_TRUTH_REFRESH_READY" else 1


def _verification_config(config: BrokerTruthRefreshConfig) -> IbkrReadOnlyVerificationConfig:
    return IbkrReadOnlyVerificationConfig(
        repo_root=config.repo_root,
        mode=config.mode,
        host=config.host,
        port=int(config.port),
        client_id=int(config.client_id),
        account_id=config.account_id,
        read_only=bool(config.read_only),
        timeout_seconds=float(config.timeout_seconds),
        probe_market_data=not bool(config.skip_market_data_probe),
        probe_duplicate_client_id=not bool(config.skip_duplicate_client_id_probe),
        gc_expiry=config.gc_expiry,
        mgc_expiry=config.mgc_expiry,
    )


def _connection_detail(connection: dict[str, Any]) -> str | None:
    for key in ("account_truth_check", "position_truth_check", "open_order_truth_check", "connection_check"):
        section = connection.get(key)
        if isinstance(section, dict) and section.get("detail"):
            return str(section.get("detail"))
    return None


def _write_json_atomically(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
