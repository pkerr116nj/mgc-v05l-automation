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

from mgc_v05l.execution_core.track_b_live_market_data_symbols import active_phase1_runtime_symbols

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
DEFAULT_HEARTBEAT_PATH = REPO_ROOT / "var" / "track_b_broker_truth_refresh_heartbeat.json"
DEFAULT_REFRESH_SECONDS = 60.0
DEFAULT_CLIENT_ID = 9077


@dataclass(frozen=True)
class BrokerTruthRefreshConfig:
    repo_root: Path = REPO_ROOT
    output_dir: Path = DEFAULT_OUTPUT_DIR
    status_path: Path = DEFAULT_STATUS_PATH
    var_status_path: Path = DEFAULT_VAR_STATUS_PATH
    heartbeat_path: Path | None = None
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
    skip_reconnect_check_when_active_exposure: bool = True
    refresh_lease_artifact: bool = True
    gc_expiry: str = "202606"
    mgc_expiry: str = "202606"



def _default_allowed_instruments(repo_root: Path) -> tuple[str, ...]:
    try:
        return active_phase1_runtime_symbols(repo_root / "config" / "track_b_live_market_data_symbols.yaml")
    except Exception:
        return active_phase1_runtime_symbols()


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
    except Exception as exc:  # pragma: no cover - exercised with explicit unit fake
        error = str(exc)
    completed_at = now_fn()
    attempt_dir = config.output_dir / "latest_attempt"
    attempt_config = BrokerTruthRefreshConfig(**{**config.__dict__, "output_dir": attempt_dir})
    attempt_status = build_broker_truth_refresh_status(
        config=config if _artifacts_are_complete_success(artifacts=artifacts, error=error) else attempt_config,
        started_at=started_at,
        completed_at=completed_at,
        artifacts=artifacts,
        error=error,
    )
    attempt_status_path = config.output_dir / "ibkr_broker_truth_latest_attempt_status.json"
    if attempt_status["classification"] == "BROKER_TRUTH_REFRESH_READY":
        if artifacts is not None:
            artifact_writer(output_dir=config.output_dir, artifacts=artifacts)
        status = _status_with_attempt_metadata(
            status=attempt_status,
            attempt_status=attempt_status,
            attempt_status_path=attempt_status_path,
        )
    else:
        if artifacts is not None:
            artifact_writer(output_dir=attempt_dir, artifacts=artifacts)
        previous_status = _read_json(config.status_path)
        status = _preserve_last_successful_status(
            previous_status=previous_status,
            attempt_status=attempt_status,
            attempt_status_path=attempt_status_path,
        )
    _write_json_atomically(attempt_status_path, attempt_status)
    write_broker_truth_refresh_status(status_path=config.status_path, var_status_path=config.var_status_path, status=status)
    lease_status = _refresh_broker_truth_lease_if_enabled(config=config, current_time=completed_at.isoformat())
    if lease_status:
        status = {**status, "broker_truth_lease_refresh": lease_status}
        write_broker_truth_refresh_status(status_path=config.status_path, var_status_path=config.var_status_path, status=status)
    _write_broker_truth_heartbeat(config=config, status=status, cycle=None)
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
        _write_broker_truth_heartbeat(config=config, status=latest_status, cycle=cycle)
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
    age_seconds = 0.0
    freshness_threshold_seconds = _freshness_threshold_seconds(config.refresh_seconds)
    fresh = bool(success and age_seconds <= freshness_threshold_seconds)
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
        "freshness_threshold_seconds": freshness_threshold_seconds,
        "fresh": fresh,
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


def _artifacts_are_complete_success(*, artifacts: IbkrReadOnlyVerificationArtifacts | None, error: str | None) -> bool:
    if artifacts is None or error is not None:
        return False
    status = build_broker_truth_refresh_status(
        config=BrokerTruthRefreshConfig(output_dir=Path(".")),
        started_at=datetime.now(timezone.utc),
        completed_at=datetime.now(timezone.utc),
        artifacts=artifacts,
        error=error,
    )
    return status.get("classification") == "BROKER_TRUTH_REFRESH_READY"


def _status_with_attempt_metadata(
    *,
    status: dict[str, Any],
    attempt_status: dict[str, Any],
    attempt_status_path: Path,
) -> dict[str, Any]:
    return {
        **status,
        "latest_attempt_status_path": str(attempt_status_path),
        "latest_attempt_status": _compact_attempt_status(attempt_status),
        "last_successful_broker_truth": _last_successful_broker_truth(status),
    }


def _preserve_last_successful_status(
    *,
    previous_status: dict[str, Any],
    attempt_status: dict[str, Any],
    attempt_status_path: Path,
) -> dict[str, Any]:
    if not _is_successful_canonical_status(previous_status):
        return _status_with_attempt_metadata(
            status=attempt_status,
            attempt_status=attempt_status,
            attempt_status_path=attempt_status_path,
        )
    preserved = {
        **previous_status,
        "classification": "BROKER_TRUTH_REFRESH_LAST_SUCCESS_PRESERVED",
        "last_failure": True,
        "last_failure_at": attempt_status.get("generated_at"),
        "last_error": attempt_status.get("last_error") or attempt_status.get("verifier_classification"),
        "latest_attempt_status_path": str(attempt_status_path),
        "latest_attempt_status": _compact_attempt_status(attempt_status),
    }
    preserved["last_successful_broker_truth"] = _last_successful_broker_truth(preserved)
    return preserved


def _is_successful_canonical_status(status: dict[str, Any]) -> bool:
    if not isinstance(status, dict):
        return False
    return (
        status.get("last_success") is True
        and status.get("read_only") is True
        and status.get("positions_complete") is True
        and status.get("open_orders_complete") is True
        and bool(status.get("positions_snapshot_path"))
        and bool(status.get("open_orders_snapshot_path"))
    )


def _compact_attempt_status(status: dict[str, Any]) -> dict[str, Any]:
    return {
        "classification": status.get("classification"),
        "generated_at": status.get("generated_at"),
        "last_success": status.get("last_success") is True,
        "last_failure": status.get("last_failure") is True,
        "last_error": status.get("last_error"),
        "positions_complete": status.get("positions_complete") is True,
        "open_orders_complete": status.get("open_orders_complete") is True,
        "position_count": status.get("position_count"),
        "open_order_count": status.get("open_order_count"),
        "positions_snapshot_path": status.get("positions_snapshot_path"),
        "open_orders_snapshot_path": status.get("open_orders_snapshot_path"),
        "verifier_classification": status.get("verifier_classification"),
        "submit_authority": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }


def _last_successful_broker_truth(status: dict[str, Any]) -> dict[str, Any]:
    return {
        "classification": status.get("classification"),
        "generated_at": status.get("generated_at"),
        "latest_refresh_time": status.get("latest_refresh_time"),
        "last_success_at": status.get("last_success_at"),
        "account": status.get("account"),
        "positions_complete": status.get("positions_complete") is True,
        "open_orders_complete": status.get("open_orders_complete") is True,
        "position_count": status.get("position_count"),
        "open_order_count": status.get("open_order_count"),
        "positions_snapshot_path": status.get("positions_snapshot_path"),
        "open_orders_snapshot_path": status.get("open_orders_snapshot_path"),
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


def _refresh_broker_truth_lease_if_enabled(*, config: BrokerTruthRefreshConfig, current_time: str | None = None) -> dict[str, Any]:
    if not config.refresh_lease_artifact:
        return {}
    try:
        from mgc_v05l.app.track_b_broker_truth_lease import (
            compact_lease_summary,
            gather_lease_inputs,
        )
        from mgc_v05l.execution_core.track_b_broker_truth_lease import (
            DEFAULT_LEASE_ARTIFACT,
            DEFAULT_LEASE_HISTORY,
            classify_broker_truth_lease,
            preserve_invalidated_previous_lease_diagnostic,
            write_broker_truth_lease,
        )
        from mgc_v05l.execution_core.track_b_broker_session_authority import (
            DEFAULT_BROKER_SESSION_AUTHORITY_ARTIFACT,
            DEFAULT_BROKER_SESSION_AUTHORITY_HISTORY,
            build_broker_session_authority,
            write_broker_session_authority,
        )
        from mgc_v05l.execution_core.track_b_broker_authority_ownership import (
            DEFAULT_BROKER_AUTHORITY_OWNERSHIP_ARTIFACT,
            build_broker_authority_ownership_status,
            load_broker_authority_ownership_status,
            write_broker_authority_ownership_status,
        )

        repo_root = Path(config.repo_root).expanduser().resolve()
        output_path = repo_root / DEFAULT_LEASE_ARTIFACT
        history_path = repo_root / DEFAULT_LEASE_HISTORY
        authority_output_path = repo_root / DEFAULT_BROKER_SESSION_AUTHORITY_ARTIFACT
        authority_history_path = repo_root / DEFAULT_BROKER_SESSION_AUTHORITY_HISTORY
        ownership_output_path = repo_root / DEFAULT_BROKER_AUTHORITY_OWNERSHIP_ARTIFACT
        paths = {
            "broker_truth_status": config.status_path,
            "latest_attempt": config.output_dir / "ibkr_broker_truth_latest_attempt_status.json",
            "reconciliation": repo_root
            / "outputs"
            / "reports"
            / "track_b_paper_broker_reconciliation"
            / "latest_track_b_paper_broker_reconciliation.json",
            "lifecycle": repo_root
            / "outputs"
            / "track_b_execution_core"
            / "paper_trade_ledger"
            / "latest_track_b_live_position_status.json",
            "order_state": repo_root
            / "outputs"
            / "track_b_execution_core"
            / "paper_trade_ledger"
            / "latest_track_b_paper_trade_summary.json",
            "open_order_truth": repo_root
            / "outputs"
            / "track_b_execution_core"
            / "open_order_truth"
            / "latest_open_order_truth.json",
            "canonical_readiness": repo_root
            / "outputs"
            / "operator_dashboard"
            / "runtime"
            / "latest_canonical_readiness.json",
            "maintenance_supervisor": repo_root
            / "outputs"
            / "operator_dashboard"
            / "runtime"
            / "latest_maintenance_supervisor_decision.json",
            "output": output_path,
            "history": history_path,
        }
        inputs = gather_lease_inputs(
            repo_root=repo_root,
            account_id=config.account_id,
            allowed_instruments=list(_default_allowed_instruments(repo_root)),
            current_time=current_time,
            policy={
                "max_entry_age_seconds": 300.0,
                "max_exit_age_seconds": 900.0,
                "degraded_refresh_grace_seconds": 120.0,
            },
            paths=paths,
        )
        source_timestamp = current_time or datetime.now(timezone.utc).isoformat()
        authority_generation_id = _broker_authority_generation_id(source_timestamp)
        inputs["authority_generation_id"] = authority_generation_id
        inputs["authority_writer"] = "ibkr_broker_truth_refresher"
        inputs["authority_source_timestamp"] = source_timestamp
        lease = classify_broker_truth_lease(inputs)
        lease = preserve_invalidated_previous_lease_diagnostic(
            lease=lease,
            previous_lease=_read_json(output_path),
            broker_truth=dict(inputs.get("last_successful_broker_truth") or {}),
            open_order_truth=_read_json(
                repo_root / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json"
            ),
        )
        write_broker_truth_lease(output_path=output_path, lease=lease, history_path=history_path)
        authority = build_broker_session_authority(
            lease=lease,
            generated_at=source_timestamp,
            source_lease_path=output_path,
            active_track_b_exposure=_active_track_b_exposure_present(repo_root),
        )
        write_broker_session_authority(
            output_path=authority_output_path,
            authority=authority,
            history_path=authority_history_path,
        )
        ownership_status = build_broker_authority_ownership_status(
            repo_root=repo_root,
            lease=lease,
            broker_session_authority=authority,
            generated_at=source_timestamp,
            writer_pid=os.getpid(),
            service_label="track_b_ibkr_broker_truth_refresh",
            existing_status=load_broker_authority_ownership_status(repo_root),
        )
        write_broker_authority_ownership_status(
            repo_root=repo_root,
            status=ownership_status,
            output_path=ownership_output_path,
        )
        summary = compact_lease_summary(lease)
        return {
            "ok": True,
            "authority_generation_id": authority_generation_id,
            "authority_writer": "ibkr_broker_truth_refresher",
            "authority_source_timestamp": source_timestamp,
            **summary,
            "broker_session_authority_classification": authority.get("classification"),
            "broker_session_authority_path": str(authority_output_path),
            "broker_authority_ownership_classification": ownership_status.get("classification"),
            "broker_authority_ownership_path": str(ownership_output_path),
        }
    except Exception as exc:  # pragma: no cover - defensive status path
        return {"ok": False, "error": str(exc), "live_money_eligible": False}


def _write_broker_truth_heartbeat(*, config: BrokerTruthRefreshConfig, status: dict[str, Any], cycle: int | None) -> None:
    if config.heartbeat_path is None:
        return
    now = datetime.now(timezone.utc).isoformat()
    heartbeat = {
        "schema_version": "track_b_broker_truth_refresh_heartbeat_v1",
        "generated_at": now,
        "service": "track_b_ibkr_broker_truth_refresh",
        "pid": os.getpid(),
        "repo_root": str(Path(config.repo_root).expanduser().resolve()),
        "mode": config.mode,
        "account": config.account_id,
        "client_id": int(config.client_id),
        "read_only": bool(config.read_only),
        "refresh_seconds": float(config.refresh_seconds),
        "cycle": cycle if cycle is not None else status.get("cycle"),
        "status_generated_at": status.get("generated_at"),
        "classification": status.get("classification"),
        "last_success": status.get("last_success") is True,
        "last_failure": status.get("last_failure") is True,
        "fresh": status.get("fresh") is True,
        "broker_truth_lease_refresh": status.get("broker_truth_lease_refresh")
        if isinstance(status.get("broker_truth_lease_refresh"), dict)
        else {},
        "submit_authority": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }
    _write_json_atomically(config.heartbeat_path, heartbeat)


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
    return _with_current_freshness({**payload, "available": True, "path": str(status_path)})


def _with_current_freshness(payload: dict[str, Any]) -> dict[str, Any]:
    source_classification = str(payload.get("classification") or "BROKER_TRUTH_REFRESH_STATUS_UNKNOWN")
    age_seconds = _payload_age_seconds(payload.get("generated_at"))
    try:
        refresh_seconds = float(payload.get("refresh_seconds") or DEFAULT_REFRESH_SECONDS)
    except (TypeError, ValueError):
        refresh_seconds = DEFAULT_REFRESH_SECONDS
    freshness_threshold_seconds = _freshness_threshold_seconds(refresh_seconds)
    fresh = bool(
        source_classification in {"BROKER_TRUTH_REFRESH_READY", "BROKER_TRUTH_REFRESH_LAST_SUCCESS_PRESERVED"}
        and payload.get("last_success") is True
        and payload.get("positions_complete") is True
        and payload.get("open_orders_complete") is True
        and age_seconds is not None
        and age_seconds <= freshness_threshold_seconds
    )
    classification = "BROKER_TRUTH_REFRESH_FRESH" if fresh else source_classification
    if source_classification in {"BROKER_TRUTH_REFRESH_READY", "BROKER_TRUTH_REFRESH_LAST_SUCCESS_PRESERVED"} and not fresh:
        classification = "BROKER_TRUTH_REFRESH_STALE"
    last_successful = payload.get("last_successful_broker_truth")
    latest_attempt = payload.get("latest_attempt_status")
    return {
        **payload,
        "source_classification": source_classification,
        "classification": classification,
        "age_seconds": age_seconds,
        "freshness_threshold_seconds": freshness_threshold_seconds,
        "fresh": fresh,
        "last_successful_broker_truth": _with_truth_age(last_successful, refresh_seconds)
        if isinstance(last_successful, dict)
        else _with_truth_age(_last_successful_broker_truth(payload), refresh_seconds),
        "latest_attempt_status": _with_truth_age(latest_attempt, refresh_seconds)
        if isinstance(latest_attempt, dict)
        else latest_attempt,
    }


def _with_truth_age(payload: dict[str, Any], refresh_seconds: float) -> dict[str, Any]:
    generated_at = payload.get("generated_at") or payload.get("last_success_at") or payload.get("latest_refresh_time")
    age_seconds = _payload_age_seconds(generated_at)
    threshold = _freshness_threshold_seconds(refresh_seconds)
    return {
        **payload,
        "age_seconds": age_seconds,
        "freshness_threshold_seconds": threshold,
        "fresh": bool(age_seconds is not None and age_seconds <= threshold),
    }


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _payload_age_seconds(value: object) -> float | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max((datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds(), 0.0)


def _freshness_threshold_seconds(refresh_seconds: float) -> float:
    try:
        value = float(refresh_seconds)
    except (TypeError, ValueError):
        value = DEFAULT_REFRESH_SECONDS
    if value <= 0:
        value = DEFAULT_REFRESH_SECONDS
    return max(value * 2.5, value + 30.0)


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
    parser.add_argument("--heartbeat-path", type=Path, default=DEFAULT_HEARTBEAT_PATH)
    parser.add_argument("--no-lease-refresh", action="store_true")
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
        heartbeat_path=Path(args.heartbeat_path) if args.heartbeat_path else None,
        refresh_lease_artifact=not bool(args.no_lease_refresh),
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
    active_exposure = _active_track_b_exposure_present(config.repo_root)
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
        probe_reconnect=not (bool(config.skip_reconnect_check_when_active_exposure) and active_exposure),
        gc_expiry=config.gc_expiry,
        mgc_expiry=config.mgc_expiry,
    )


def _active_track_b_exposure_present(repo_root: Path) -> bool:
    lease = _read_json(repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json")
    reconciliation = _read_json(
        repo_root
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )
    lifecycle = _read_json(
        repo_root / "outputs" / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_live_position_status.json"
    )
    counts = (
        lease.get("track_b_broker_position_count"),
        reconciliation.get("track_b_broker_position_count"),
        reconciliation.get("lifecycle_open_position_count"),
        reconciliation.get("current_scope_lifecycle_open_position_count"),
        lifecycle.get("open_position_count"),
        lifecycle.get("current_scope_lifecycle_open_position_count"),
    )
    for value in counts:
        try:
            if int(value or 0) > 0:
                return True
        except (TypeError, ValueError):
            continue
    for row in list(lease.get("positions") or []) + list(reconciliation.get("track_b_broker_positions") or []):
        if isinstance(row, dict):
            try:
                if abs(float(row.get("quantity") or row.get("qty") or 0)) > 1e-9:
                    return True
            except (TypeError, ValueError):
                continue
    return False


def _connection_detail(connection: dict[str, Any]) -> str | None:
    for key in ("account_truth_check", "position_truth_check", "open_order_truth_check", "connection_check"):
        section = connection.get(key)
        if isinstance(section, dict) and section.get("detail"):
            return str(section.get("detail"))
    return None


def _broker_authority_generation_id(source_timestamp: str) -> str:
    compact = (
        source_timestamp.replace("-", "")
        .replace(":", "")
        .replace("+00:00", "Z")
        .replace(".", "")
    )
    return f"ibkr-broker-truth-refresher-{compact}"


def _write_json_atomically(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Use a unique temp file in the same directory to avoid collisions across
    # concurrent writers and to keep os.replace() atomic on the same filesystem.
    import os
    import tempfile

    serialized = json.dumps(payload, indent=2, sort_keys=True)
    fd: int | None = None
    temp_path: Path | None = None
    try:
        fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
        temp_path = Path(temp_name)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        fd = None
        os.replace(str(temp_path), str(path))
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        if temp_path is not None and temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
