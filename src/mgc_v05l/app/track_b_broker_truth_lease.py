"""Read-only Track B broker-truth lease producer CLI."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_broker_truth_lease import (
    DEFAULT_LEASE_ARTIFACT,
    DEFAULT_LEASE_HISTORY,
    classify_broker_truth_lease,
    preserve_invalidated_previous_lease_diagnostic,
    write_broker_truth_lease,
)
from mgc_v05l.execution_core.track_b_live_market_data_symbols import active_phase1_runtime_symbols
from mgc_v05l.execution_core.track_b_readiness_state import REPO_ROOT

READY_EXIT_STATES = {"ACTIVE", "ACTIVE_DEGRADED_REFRESH_FAILING"}
DEGRADED_EXIT_STATES = {"EXPIRED_BLOCK_NEW_ENTRIES", "EXPIRED_EXITS_ONLY"}
DEFAULT_BROKER_TRUTH_STATUS = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json"
)
DEFAULT_RECONCILIATION = (
    Path("outputs")
    / "reports"
    / "track_b_paper_broker_reconciliation"
    / "latest_track_b_paper_broker_reconciliation.json"
)
DEFAULT_LIFECYCLE_SUMMARY = (
    Path("outputs") / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_live_position_status.json"
)
DEFAULT_ORDER_STATE = (
    Path("outputs") / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_paper_trade_summary.json"
)
DEFAULT_OPEN_ORDER_TRUTH = (
    Path("outputs") / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json"
)
DEFAULT_CANONICAL_READINESS = Path("outputs") / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
DEFAULT_MAINTENANCE_SUPERVISOR = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_maintenance_supervisor_decision.json"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="track-b-broker-truth-lease")
    parser.add_argument("--repo-root", default=str(REPO_ROOT), help="Repository root to read artifacts from.")
    parser.add_argument("--account-id", default="DUM882026", help="Expected PAPER account id.")
    parser.add_argument(
        "--allowed-instrument",
        action="append",
        default=None,
        help="Allowed Track B instrument. May be repeated. Defaults to MGC/MNQ/GC.",
    )
    parser.add_argument("--current-time", default=None, help="Evaluation time as ISO-8601. Defaults to now.")
    parser.add_argument("--max-entry-age-seconds", type=float, default=300.0)
    parser.add_argument("--max-exit-age-seconds", type=float, default=900.0)
    parser.add_argument("--degraded-refresh-grace-seconds", type=float, default=120.0)
    parser.add_argument("--broker-truth-status-path", default=None)
    parser.add_argument("--latest-attempt-path", default=None)
    parser.add_argument("--reconciliation-path", default=None)
    parser.add_argument("--lifecycle-path", default=None)
    parser.add_argument("--order-state-path", default=None)
    parser.add_argument("--open-order-truth-path", default=None)
    parser.add_argument("--canonical-readiness-path", default=None)
    parser.add_argument("--maintenance-supervisor-path", default=None)
    parser.add_argument("--output-path", default=None)
    parser.add_argument("--history-path", default=None)
    parser.add_argument("--no-history", action="store_true", help="Write only the latest lease artifact.")
    parser.add_argument("--json", action="store_true", help="Print compact JSON summary.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = Path(args.repo_root).expanduser().resolve()
    paths = _resolve_paths(repo_root=repo_root, args=args)
    inputs = gather_lease_inputs(
        repo_root=repo_root,
        account_id=str(args.account_id),
        allowed_instruments=list(args.allowed_instrument or _default_allowed_instruments(repo_root)),
        current_time=args.current_time,
        policy={
            "max_entry_age_seconds": float(args.max_entry_age_seconds),
            "max_exit_age_seconds": float(args.max_exit_age_seconds),
            "degraded_refresh_grace_seconds": float(args.degraded_refresh_grace_seconds),
        },
        paths=paths,
    )
    lease = classify_broker_truth_lease(inputs)
    existing_hot_lease = _read_json(paths["output"])
    lease = preserve_invalidated_previous_lease_diagnostic(
        lease=lease,
        previous_lease=existing_hot_lease,
        broker_truth=_mapping(inputs.get("last_successful_broker_truth")),
        open_order_truth=_mapping(inputs.get("open_order_truth")),
    )
    hot_authority_write_skipped = _hot_authority_write_should_be_skipped(
        repo_root=repo_root,
        output_path=paths["output"],
        existing_lease=existing_hot_lease,
        current_time=str(inputs["current_time"]),
    )
    if hot_authority_write_skipped:
        lease = {
            **lease,
            "diagnostic_only": True,
            "hot_authority_write_skipped": True,
            "hot_authority_writer": existing_hot_lease.get("authority_writer"),
            "hot_authority_generation_id": existing_hot_lease.get("authority_generation_id"),
        }
    else:
        write_broker_truth_lease(
            output_path=paths["output"],
            lease=lease,
            history_path=None if bool(args.no_history) else paths["history"],
        )
    summary = compact_lease_summary(lease)
    summary["hot_authority_write_skipped"] = hot_authority_write_skipped
    print_summary(summary, as_json=bool(args.json))
    return exit_code_for_state(str(summary["lease_state"]))


def gather_lease_inputs(
    *,
    repo_root: Path,
    account_id: str,
    allowed_instruments: Sequence[str],
    current_time: str | None,
    policy: Mapping[str, Any],
    paths: Mapping[str, Path],
) -> dict[str, Any]:
    broker_status = _read_json(paths["broker_truth_status"])
    latest_attempt = _read_json(paths["latest_attempt"]) or _mapping(broker_status.get("latest_attempt_status"))
    last_success = _broker_truth_with_snapshots(
        repo_root=repo_root,
        broker_status=broker_status,
        broker_truth=_mapping(broker_status.get("last_successful_broker_truth")) or broker_status,
    )
    reconciliation = _read_json(paths["reconciliation"])
    lifecycle = _read_json(paths["lifecycle"])
    order_state = _read_json(paths["order_state"])
    open_order_truth = _read_json(paths["open_order_truth"])
    canonical = _read_json(paths["canonical_readiness"])
    maintenance = _read_json(paths["maintenance_supervisor"])

    return {
        "account_id": account_id,
        "allowed_instruments": list(allowed_instruments),
        "current_time": current_time or _default_current_time(),
        "policy": dict(policy),
        "last_successful_broker_truth": last_success,
        "latest_attempt_status": latest_attempt,
        "reconciliation": reconciliation,
        "lifecycle": _lifecycle_summary(lifecycle),
        "order_state": _order_state_summary(order_state, reconciliation),
        "open_order_truth": open_order_truth,
        "canonical_readiness": canonical,
        "maintenance_supervisor": maintenance,
        "source_artifact_paths": {key: str(path) for key, path in paths.items()},
        "source_artifact_timestamps": {
            key: value
            for key, value in {
                "broker_truth_status": _artifact_timestamp(paths["broker_truth_status"], broker_status),
                "latest_attempt": _artifact_timestamp(paths["latest_attempt"], latest_attempt),
                "reconciliation": _artifact_timestamp(paths["reconciliation"], reconciliation),
                "lifecycle": _artifact_timestamp(paths["lifecycle"], lifecycle),
                "order_state": _artifact_timestamp(paths["order_state"], order_state),
                "open_order_truth": _artifact_timestamp(paths["open_order_truth"], open_order_truth),
                "canonical_readiness": _artifact_timestamp(paths["canonical_readiness"], canonical),
                "maintenance_supervisor": _artifact_timestamp(paths["maintenance_supervisor"], maintenance),
            }.items()
            if value is not None
        },
    }


def compact_lease_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "lease_state": str(payload.get("lease_state") or "OPERATOR_REQUIRED"),
        "account_id": payload.get("account_id"),
        "generated_at": payload.get("generated_at"),
        "authority_generation_id": payload.get("authority_generation_id"),
        "authority_writer": payload.get("authority_writer"),
        "authority_source_timestamp": payload.get("authority_source_timestamp"),
        "broker_session_owner": payload.get("broker_session_owner"),
        "client_id": _mapping(payload.get("broker_session_owner")).get("client_id"),
        "position_snapshot_timestamp": payload.get("position_snapshot_timestamp"),
        "open_order_snapshot_timestamp": payload.get("open_order_snapshot_timestamp"),
        "callback_timestamps": payload.get("callback_timestamps"),
        "allowed_uses": payload.get("allowed_uses"),
        "valid_until": payload.get("valid_until"),
        "entry_valid_until": payload.get("entry_valid_until"),
        "exit_valid_until": payload.get("exit_valid_until"),
        "submit_entry_allowed": payload.get("submit_entry_allowed") is True,
        "submit_exit_allowed": payload.get("submit_exit_allowed") is True,
        "warnings": _codes(payload.get("warnings")),
        "blockers": _codes(payload.get("blockers")),
        "contradictions": _codes(payload.get("contradiction_details")),
        "operator_action_required": payload.get("operator_action_required") is True,
        "live_money_eligible": False,
    }


def print_summary(summary: Mapping[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(dict(summary), indent=2, sort_keys=True))
        return
    for key in (
        "lease_state",
        "account_id",
        "generated_at",
        "authority_generation_id",
        "authority_writer",
        "authority_source_timestamp",
        "client_id",
        "position_snapshot_timestamp",
        "open_order_snapshot_timestamp",
        "valid_until",
        "entry_valid_until",
        "exit_valid_until",
        "submit_entry_allowed",
        "submit_exit_allowed",
        "warnings",
        "blockers",
        "contradictions",
        "operator_action_required",
        "live_money_eligible",
    ):
        print(f"{key}={_format_summary_value(summary.get(key))}")


def exit_code_for_state(state: str) -> int:
    if state in READY_EXIT_STATES:
        return 0
    if state in DEGRADED_EXIT_STATES:
        return 1
    return 2



def _default_allowed_instruments(repo_root: Path) -> tuple[str, ...]:
    try:
        return active_phase1_runtime_symbols(repo_root / "config" / "track_b_live_market_data_symbols.yaml")
    except Exception:
        return active_phase1_runtime_symbols()


def _resolve_paths(*, repo_root: Path, args: argparse.Namespace) -> dict[str, Path]:
    return {
        "broker_truth_status": _resolve_path(repo_root, args.broker_truth_status_path, DEFAULT_BROKER_TRUTH_STATUS),
        "latest_attempt": _resolve_path(
            repo_root,
            args.latest_attempt_path,
            Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_latest_attempt_status.json",
        ),
        "reconciliation": _resolve_path(repo_root, args.reconciliation_path, DEFAULT_RECONCILIATION),
        "lifecycle": _resolve_path(repo_root, args.lifecycle_path, DEFAULT_LIFECYCLE_SUMMARY),
        "order_state": _resolve_path(repo_root, args.order_state_path, DEFAULT_ORDER_STATE),
        "open_order_truth": _resolve_path(repo_root, args.open_order_truth_path, DEFAULT_OPEN_ORDER_TRUTH),
        "canonical_readiness": _resolve_path(repo_root, args.canonical_readiness_path, DEFAULT_CANONICAL_READINESS),
        "maintenance_supervisor": _resolve_path(repo_root, args.maintenance_supervisor_path, DEFAULT_MAINTENANCE_SUPERVISOR),
        "output": _resolve_path(repo_root, args.output_path, DEFAULT_LEASE_ARTIFACT),
        "history": _resolve_path(repo_root, args.history_path, DEFAULT_LEASE_HISTORY),
    }


def _resolve_path(repo_root: Path, configured: str | None, default: Path) -> Path:
    path = Path(configured).expanduser() if configured else default
    return path if path.is_absolute() else repo_root / path


def _hot_authority_write_should_be_skipped(
    *,
    repo_root: Path,
    output_path: Path,
    existing_lease: Mapping[str, Any],
    current_time: str,
) -> bool:
    if output_path.resolve() != (repo_root / DEFAULT_LEASE_ARTIFACT).resolve():
        return False
    if str(existing_lease.get("authority_writer") or "") != "ibkr_broker_truth_refresher":
        return False
    if str(existing_lease.get("lease_state") or "").upper() not in {"ACTIVE", "ACTIVE_DEGRADED_REFRESH_FAILING"}:
        return False
    valid_until = existing_lease.get("entry_valid_until") or existing_lease.get("valid_until")
    seconds_remaining = _seconds_until(valid_until, current_time)
    return bool(seconds_remaining is not None and seconds_remaining > 0)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _seconds_until(value: Any, current_time: str) -> float | None:
    target = _parse_iso(value)
    now = _parse_iso(current_time)
    if target is None or now is None:
        return None
    return (target - now).total_seconds()


def _parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _broker_truth_with_snapshots(
    *,
    repo_root: Path,
    broker_status: Mapping[str, Any],
    broker_truth: Mapping[str, Any],
) -> dict[str, Any]:
    result = dict(broker_truth)
    positions_snapshot = _read_json(_resolve_snapshot_path(repo_root, result.get("positions_snapshot_path") or broker_status.get("positions_snapshot_path")))
    open_orders_snapshot = _read_json(
        _resolve_snapshot_path(repo_root, result.get("open_orders_snapshot_path") or broker_status.get("open_orders_snapshot_path"))
    )
    connection_report = _read_json(
        _resolve_snapshot_path(repo_root, result.get("connection_report_path") or broker_status.get("connection_report_path"))
    )
    if positions_snapshot:
        result.setdefault("positions", positions_snapshot.get("positions") or [])
        result.setdefault("positions_complete", positions_snapshot.get("positions_complete"))
        result.setdefault("positions_generated_at", positions_snapshot.get("generated_at"))
        result.setdefault("client_id", positions_snapshot.get("client_id"))
    if open_orders_snapshot:
        result.setdefault("open_orders", open_orders_snapshot.get("open_orders") or [])
        result.setdefault("open_orders_complete", open_orders_snapshot.get("open_orders_complete"))
        result.setdefault("open_orders_generated_at", open_orders_snapshot.get("generated_at"))
        result.setdefault("client_id", open_orders_snapshot.get("client_id"))
    if connection_report:
        connection_check = _mapping(connection_report.get("connection_check"))
        result.setdefault("connection_check", connection_check)
        result.setdefault("server_version", connection_check.get("server_version"))
        submit_session_readiness = _submit_session_readiness_from_connection_report(connection_report)
        if submit_session_readiness:
            result.setdefault("submit_session_readiness", submit_session_readiness)
    return result


def _resolve_snapshot_path(repo_root: Path, value: Any) -> Path:
    path = Path(str(value or ""))
    if not str(path):
        return repo_root / "__missing__"
    return path if path.is_absolute() else repo_root / path


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _submit_session_readiness_from_connection_report(payload: Mapping[str, Any]) -> dict[str, Any]:
    connection_check = _mapping(payload.get("connection_check"))
    if not connection_check:
        return {}
    return {
        "source": "ibkr_read_only_connection_report",
        "client_id": connection_check.get("client_id"),
        "connected": connection_check.get("connected") is True,
        "server_version": connection_check.get("server_version"),
        "connection_started_at": connection_check.get("connection_timestamp") or payload.get("started_at"),
    }


def _lifecycle_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not payload:
        return {}
    open_positions = payload.get("open_positions") or payload.get("positions") or payload.get("track_b_lifecycle_positions") or []
    return {
        **dict(payload),
        "open_positions": list(open_positions) if isinstance(open_positions, list) else [],
        "open_position_count": payload.get("open_position_count")
        or payload.get("lifecycle_open_position_count")
        or len(open_positions if isinstance(open_positions, list) else []),
    }


def _order_state_summary(payload: Mapping[str, Any], reconciliation: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **dict(payload),
        "unknown_open_order_count": payload.get("unknown_open_order_count")
        or reconciliation.get("unknown_broker_open_order_count")
        or 0,
        "lifecycle_open_order_count": payload.get("lifecycle_open_order_count")
        or reconciliation.get("lifecycle_open_order_count")
        or 0,
        "unresolved_intent_count": payload.get("unresolved_intent_count")
        or reconciliation.get("unresolved_submit_intent_ownership_count")
        or 0,
    }


def _artifact_timestamp(path: Path, payload: Mapping[str, Any]) -> str | None:
    if payload.get("generated_at"):
        return str(payload["generated_at"])
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()
    except OSError:
        return None


def _default_current_time() -> str:
    return datetime.now(timezone.utc).isoformat()


def _codes(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    codes: list[str] = []
    for row in value:
        if isinstance(row, Mapping):
            code = row.get("code")
            if code is not None:
                codes.append(str(code))
        elif row is not None:
            codes.append(str(row))
    return codes


def _format_summary_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return ",".join(str(item) for item in value) if value else "none"
    if value is None:
        return ""
    return str(value)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
