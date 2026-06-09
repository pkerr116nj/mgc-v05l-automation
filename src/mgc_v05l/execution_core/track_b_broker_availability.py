"""Read-only Track B broker availability contract.

Broker availability is an execution-boundary fact. This module deliberately
does not evaluate strategy authority, lifecycle authority, entry authority, or
exit authority; it only classifies whether the configured broker endpoint is
currently usable for broker-bound operations.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.models import JsonSerializable, TrackBModelError, require_aware_datetime, to_jsonable
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_exit_authority_contract import ExecutionDomain, SourceArtifactRef


REPO_ROOT = Path(__file__).resolve().parents[3]
BROKER_AVAILABILITY_SCHEMA_VERSION = "track_b_broker_availability_v1"

DEFAULT_BROKER_AVAILABILITY_PATH = (
    Path("outputs") / "track_b_execution_core" / "broker_availability" / "latest_broker_availability.json"
)
DEFAULT_BROKER_TRUTH_STATUS_PATH = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_latest_attempt_status.json"
)
DEFAULT_CONNECTION_REPORT_PATH = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_read_only_connection_report.json"
)
DEFAULT_POSITIONS_SNAPSHOT_PATH = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_positions_snapshot.json"
)
DEFAULT_OPEN_ORDERS_SNAPSHOT_PATH = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_open_orders_snapshot.json"
)


class BrokerAvailabilityClassification(str, Enum):
    AVAILABLE = "BROKER_AVAILABLE"
    UNAVAILABLE_RETRYABLE = "BROKER_UNAVAILABLE_RETRYABLE"
    UNAVAILABLE_FATAL = "BROKER_UNAVAILABLE_FATAL"
    UNKNOWN = "BROKER_AVAILABILITY_UNKNOWN"


@dataclass(frozen=True)
class BrokerAvailability(JsonSerializable):
    execution_domain: ExecutionDomain | str
    account_id: str
    endpoint_host: str
    endpoint_port: int
    classification: BrokerAvailabilityClassification | str
    connected: bool
    account_visible: bool
    positions_readable: bool
    open_orders_readable: bool
    submit_capability_observed_or_unknown: bool | None
    last_success_at: datetime | None
    last_failure_at: datetime | None
    failure_code: str | int | None
    failure_message: str | None
    retryable: bool
    generated_at: datetime
    source_artifact_refs: tuple[SourceArtifactRef | Mapping[str, Any], ...]
    schema_version: str = BROKER_AVAILABILITY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "execution_domain", _normalize_execution_domain(self.execution_domain))
        object.__setattr__(self, "account_id", _required_text(self.account_id, "account_id"))
        object.__setattr__(self, "endpoint_host", _required_text(self.endpoint_host, "endpoint_host"))
        port = int(self.endpoint_port)
        if port <= 0:
            raise TrackBModelError("endpoint_port must be positive.")
        object.__setattr__(self, "endpoint_port", port)
        object.__setattr__(self, "classification", _normalize_classification(self.classification))
        if self.last_success_at is not None:
            object.__setattr__(self, "last_success_at", require_aware_datetime(self.last_success_at, "last_success_at"))
        if self.last_failure_at is not None:
            object.__setattr__(self, "last_failure_at", require_aware_datetime(self.last_failure_at, "last_failure_at"))
        object.__setattr__(self, "generated_at", require_aware_datetime(self.generated_at, "generated_at"))
        object.__setattr__(
            self,
            "source_artifact_refs",
            tuple(_normalize_source_ref(row) for row in self.source_artifact_refs),
        )


@dataclass(frozen=True)
class BrokerAvailabilityReportConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_BROKER_AVAILABILITY_PATH
    broker_truth_status_path: Path = DEFAULT_BROKER_TRUTH_STATUS_PATH
    connection_report_path: Path = DEFAULT_CONNECTION_REPORT_PATH
    positions_snapshot_path: Path = DEFAULT_POSITIONS_SNAPSHOT_PATH
    open_orders_snapshot_path: Path = DEFAULT_OPEN_ORDERS_SNAPSHOT_PATH
    execution_domain: ExecutionDomain | str = ExecutionDomain.TRACK_B_PAPER
    account_id: str = "DUM882026"
    endpoint_host: str = "127.0.0.1"
    endpoint_port: int = 7497
    stale_after_seconds: float = 150.0

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_broker_availability_report(
    *,
    config: BrokerAvailabilityReportConfig,
    now: datetime | None = None,
    input_overrides: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    actual_now = require_aware_datetime(now or datetime.now(UTC), "now")
    inputs = _inputs(config=config, overrides=input_overrides or {})
    availability = classify_broker_availability(config=config, inputs=inputs, now=actual_now)
    payload = availability.to_json_dict()
    payload.update(
        {
            "read_only": True,
            "broker_state_mutated": False,
            "submit_attempted": False,
            "cancel_attempted": False,
            "close_attempted": False,
            "service_started": False,
            "runtime_restarted": False,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "strategy_authority_evaluated": False,
            "strategy_authority_failed": False,
            "entry_authority_evaluated": False,
            "entry_authority_failed": False,
            "exit_authority_evaluated": False,
            "exit_authority_failed": False,
            "lifecycle_validation_failed": False,
            "source_classifications": {
                "broker_truth_status": inputs["broker_truth_status"].get("classification"),
                "connection_report": inputs["connection_report"].get("classification"),
            },
            "source_artifact_paths": {
                "broker_truth_status": str(config.resolve(config.broker_truth_status_path)),
                "connection_report": str(config.resolve(config.connection_report_path)),
                "positions_snapshot": str(config.resolve(config.positions_snapshot_path)),
                "open_orders_snapshot": str(config.resolve(config.open_orders_snapshot_path)),
            },
        }
    )
    return payload


def classify_broker_availability(
    *,
    config: BrokerAvailabilityReportConfig,
    inputs: Mapping[str, Mapping[str, Any]],
    now: datetime,
) -> BrokerAvailability:
    actual_now = require_aware_datetime(now, "now")
    source_refs = _source_artifact_refs(config=config, inputs=inputs)
    broker_status = dict(inputs.get("broker_truth_status") or {})
    connection_report = dict(inputs.get("connection_report") or {})
    positions_snapshot = dict(inputs.get("positions_snapshot") or {})
    open_orders_snapshot = dict(inputs.get("open_orders_snapshot") or {})

    failure_code, failure_message, failure_at = _latest_failure(broker_status=broker_status, connection_report=connection_report)
    fatal_reason = _fatal_scope_reason(
        config=config,
        broker_status=broker_status,
        connection_report=connection_report,
        positions_snapshot=positions_snapshot,
        open_orders_snapshot=open_orders_snapshot,
    )
    last_success_at = _parse_datetime(
        broker_status.get("last_success_at")
        or broker_status.get("latest_refresh_time")
        or broker_status.get("generated_at")
        or connection_report.get("generated_at")
    )
    last_failure_at = failure_at or _parse_datetime(broker_status.get("last_failure_at"))
    if fatal_reason:
        return _availability(
            config=config,
            classification=BrokerAvailabilityClassification.UNAVAILABLE_FATAL,
            connected=False,
            account_visible=False,
            positions_readable=False,
            open_orders_readable=False,
            last_success_at=last_success_at,
            last_failure_at=last_failure_at,
            failure_code=failure_code,
            failure_message=fatal_reason,
            retryable=False,
            now=actual_now,
            source_refs=source_refs,
        )

    if _is_retryable_failure(failure_code=failure_code, failure_message=failure_message, broker_status=broker_status):
        return _availability(
            config=config,
            classification=BrokerAvailabilityClassification.UNAVAILABLE_RETRYABLE,
            connected=False,
            account_visible=False,
            positions_readable=False,
            open_orders_readable=False,
            last_success_at=last_success_at,
            last_failure_at=last_failure_at or actual_now,
            failure_code=failure_code,
            failure_message=failure_message,
            retryable=True,
            now=actual_now,
            source_refs=source_refs,
        )

    positions_readable = _positions_readable(broker_status=broker_status, positions_snapshot=positions_snapshot)
    open_orders_readable = _open_orders_readable(broker_status=broker_status, open_orders_snapshot=open_orders_snapshot)
    connected = _connected(
        broker_status=broker_status,
        connection_report=connection_report,
        positions_readable=positions_readable,
        open_orders_readable=open_orders_readable,
    )
    account_visible = _account_visible(config=config, broker_status=broker_status, positions_snapshot=positions_snapshot, open_orders_snapshot=open_orders_snapshot)
    source_success_at = last_success_at or _latest_snapshot_time(positions_snapshot, open_orders_snapshot)
    if connected and account_visible and positions_readable and open_orders_readable and not _stale(source_success_at, now=actual_now, max_age=config.stale_after_seconds):
        return _availability(
            config=config,
            classification=BrokerAvailabilityClassification.AVAILABLE,
            connected=True,
            account_visible=True,
            positions_readable=True,
            open_orders_readable=True,
            last_success_at=source_success_at,
            last_failure_at=last_failure_at,
            failure_code=None,
            failure_message=None,
            retryable=False,
            now=actual_now,
            source_refs=source_refs,
        )

    return _availability(
        config=config,
        classification=BrokerAvailabilityClassification.UNKNOWN,
        connected=connected,
        account_visible=account_visible,
        positions_readable=positions_readable,
        open_orders_readable=open_orders_readable,
        last_success_at=last_success_at,
        last_failure_at=last_failure_at,
        failure_code=failure_code,
        failure_message=failure_message or "broker availability artifacts are missing, stale, or incomplete",
        retryable=True,
        now=actual_now,
        source_refs=source_refs,
    )


def run_broker_availability_report(
    *,
    config: BrokerAvailabilityReportConfig,
    now: datetime | None = None,
    write: bool = True,
) -> dict[str, Any]:
    payload = build_broker_availability_report(config=config, now=now)
    if write:
        write_broker_availability_report(config=config, payload=payload)
    return payload


def write_broker_availability_report(*, config: BrokerAvailabilityReportConfig, payload: Mapping[str, Any]) -> Path:
    return write_json_atomic(config.resolve(config.output_path), to_jsonable(dict(payload)))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="track-b-broker-availability")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_BROKER_AVAILABILITY_PATH)
    parser.add_argument("--broker-truth-status-path", type=Path, default=DEFAULT_BROKER_TRUTH_STATUS_PATH)
    parser.add_argument("--connection-report-path", type=Path, default=DEFAULT_CONNECTION_REPORT_PATH)
    parser.add_argument("--positions-snapshot-path", type=Path, default=DEFAULT_POSITIONS_SNAPSHOT_PATH)
    parser.add_argument("--open-orders-snapshot-path", type=Path, default=DEFAULT_OPEN_ORDERS_SNAPSHOT_PATH)
    parser.add_argument("--account-id", default="DUM882026")
    parser.add_argument("--execution-domain", default=ExecutionDomain.TRACK_B_PAPER.value)
    parser.add_argument("--endpoint-host", default="127.0.0.1")
    parser.add_argument("--endpoint-port", type=int, default=7497)
    parser.add_argument("--stale-after-seconds", type=float, default=150.0)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-write", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = BrokerAvailabilityReportConfig(
        repo_root=args.repo_root,
        output_path=args.output_path,
        broker_truth_status_path=args.broker_truth_status_path,
        connection_report_path=args.connection_report_path,
        positions_snapshot_path=args.positions_snapshot_path,
        open_orders_snapshot_path=args.open_orders_snapshot_path,
        account_id=args.account_id,
        execution_domain=args.execution_domain,
        endpoint_host=args.endpoint_host,
        endpoint_port=args.endpoint_port,
        stale_after_seconds=args.stale_after_seconds,
    )
    payload = run_broker_availability_report(config=config, write=not args.no_write)
    if args.json or args.no_write:
        print(json.dumps(to_jsonable(payload), indent=2, sort_keys=True))
    else:
        print(f"{payload['classification']} connected={payload['connected']} account_visible={payload['account_visible']}")
    return 0


def _availability(
    *,
    config: BrokerAvailabilityReportConfig,
    classification: BrokerAvailabilityClassification,
    connected: bool,
    account_visible: bool,
    positions_readable: bool,
    open_orders_readable: bool,
    last_success_at: datetime | None,
    last_failure_at: datetime | None,
    failure_code: str | int | None,
    failure_message: str | None,
    retryable: bool,
    now: datetime,
    source_refs: tuple[SourceArtifactRef, ...],
) -> BrokerAvailability:
    return BrokerAvailability(
        execution_domain=config.execution_domain,
        account_id=config.account_id,
        endpoint_host=config.endpoint_host,
        endpoint_port=config.endpoint_port,
        classification=classification,
        connected=connected,
        account_visible=account_visible,
        positions_readable=positions_readable,
        open_orders_readable=open_orders_readable,
        submit_capability_observed_or_unknown=None,
        last_success_at=last_success_at,
        last_failure_at=last_failure_at,
        failure_code=failure_code,
        failure_message=failure_message,
        retryable=retryable,
        generated_at=now,
        source_artifact_refs=source_refs,
    )


def _inputs(
    *,
    config: BrokerAvailabilityReportConfig,
    overrides: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {
        "broker_truth_status": dict(
            overrides.get("broker_truth_status") or _read_json(config.resolve(config.broker_truth_status_path))
        ),
        "connection_report": dict(
            overrides.get("connection_report") or _read_json(config.resolve(config.connection_report_path))
        ),
        "positions_snapshot": dict(
            overrides.get("positions_snapshot") or _read_json(config.resolve(config.positions_snapshot_path))
        ),
        "open_orders_snapshot": dict(
            overrides.get("open_orders_snapshot") or _read_json(config.resolve(config.open_orders_snapshot_path))
        ),
    }


def _source_artifact_refs(
    *,
    config: BrokerAvailabilityReportConfig,
    inputs: Mapping[str, Mapping[str, Any]],
) -> tuple[SourceArtifactRef, ...]:
    refs: list[SourceArtifactRef] = []
    paths = {
        "broker_truth_status": config.broker_truth_status_path,
        "connection_report": config.connection_report_path,
        "positions_snapshot": config.positions_snapshot_path,
        "open_orders_snapshot": config.open_orders_snapshot_path,
    }
    for name, path in paths.items():
        payload = inputs.get(name) or {}
        refs.append(
            SourceArtifactRef(
                name=name,
                path=str(config.resolve(path)),
                generated_at=_parse_datetime(payload.get("generated_at")),
                authority_layer="execution_boundary",
            )
        )
    return tuple(refs)


def _latest_failure(
    *,
    broker_status: Mapping[str, Any],
    connection_report: Mapping[str, Any],
) -> tuple[str | int | None, str | None, datetime | None]:
    latest_attempt = broker_status.get("latest_attempt_status")
    if isinstance(latest_attempt, Mapping) and latest_attempt.get("last_failure"):
        code, message = _error_from_mapping(latest_attempt)
        return code, message, _parse_datetime(latest_attempt.get("last_failure_at") or latest_attempt.get("generated_at"))
    if broker_status.get("last_failure") and not broker_status.get("last_success"):
        code, message = _error_from_mapping(broker_status)
        return code, message, _parse_datetime(broker_status.get("last_failure_at") or broker_status.get("generated_at"))
    if connection_report.get("classification") in {"IBKR_READ_ONLY_BLOCKED", "IBKR_READ_ONLY_FAILED"}:
        code, message = _error_from_mapping(connection_report)
        return code, message, _parse_datetime(connection_report.get("generated_at"))
    return None, None, None


def _error_from_mapping(payload: Mapping[str, Any]) -> tuple[str | int | None, str | None]:
    latest_error = payload.get("latest_error")
    if isinstance(latest_error, Mapping):
        return latest_error.get("code"), _optional_text(latest_error.get("message"))
    errors = payload.get("errors")
    if isinstance(errors, list) and errors:
        for row in errors:
            if isinstance(row, Mapping) and row.get("code") not in {2100, 2104, 2106, 2158}:
                return row.get("code"), _optional_text(row.get("message"))
    text = payload.get("last_error") or payload.get("detail")
    if text:
        return _extract_error_code(str(text)), str(text)
    return None, None


def _fatal_scope_reason(
    *,
    config: BrokerAvailabilityReportConfig,
    broker_status: Mapping[str, Any],
    connection_report: Mapping[str, Any],
    positions_snapshot: Mapping[str, Any],
    open_orders_snapshot: Mapping[str, Any],
) -> str | None:
    mode = str(broker_status.get("mode") or connection_report.get("mode") or "").strip().upper()
    if mode and _normalize_execution_domain(config.execution_domain) == ExecutionDomain.TRACK_B_PAPER and mode != "PAPER":
        return f"wrong broker mode for TRACK_B_PAPER: {mode}"
    account_values = [
        broker_status.get("account"),
        broker_status.get("account_id"),
        broker_status.get("selected_account_id"),
        positions_snapshot.get("account"),
        positions_snapshot.get("account_id"),
        positions_snapshot.get("selected_account_id"),
        open_orders_snapshot.get("account"),
        open_orders_snapshot.get("account_id"),
        open_orders_snapshot.get("selected_account_id"),
    ]
    visible_accounts = {str(value).strip() for value in account_values if str(value or "").strip()}
    if visible_accounts and config.account_id not in visible_accounts:
        return f"expected account {config.account_id} not visible in broker availability artifacts"
    environment_lock = connection_report.get("environment_lock_check")
    if isinstance(environment_lock, Mapping) and environment_lock.get("account_type") == "LIVE":
        return "live account detected in TRACK_B_PAPER broker availability check"
    return None


def _is_retryable_failure(
    *,
    failure_code: str | int | None,
    failure_message: str | None,
    broker_status: Mapping[str, Any],
) -> bool:
    code = str(failure_code or "").strip()
    message = str(failure_message or "").lower()
    if code == "502" or "error 502" in message or "couldn't connect to tws" in message:
        return True
    latest_attempt = broker_status.get("latest_attempt_status")
    if isinstance(latest_attempt, Mapping) and latest_attempt.get("classification") == "BROKER_TRUTH_REFRESH_FAILED":
        return True
    return False


def _connected(
    *,
    broker_status: Mapping[str, Any],
    connection_report: Mapping[str, Any],
    positions_readable: bool,
    open_orders_readable: bool,
) -> bool:
    if connection_report.get("classification") == "IBKR_READ_ONLY_CONNECTED":
        return True
    if broker_status.get("verifier_classification") == "IBKR_READ_ONLY_CONNECTED":
        return True
    if positions_readable and open_orders_readable:
        return True
    return bool(broker_status.get("last_success") and broker_status.get("positions_complete") and broker_status.get("open_orders_complete"))


def _account_visible(
    *,
    config: BrokerAvailabilityReportConfig,
    broker_status: Mapping[str, Any],
    positions_snapshot: Mapping[str, Any],
    open_orders_snapshot: Mapping[str, Any],
) -> bool:
    for payload in (broker_status, positions_snapshot, open_orders_snapshot):
        for key in ("account", "account_id", "selected_account_id"):
            if str(payload.get(key) or "").strip() == config.account_id:
                return True
    for row in positions_snapshot.get("positions") or []:
        if isinstance(row, Mapping) and str(row.get("account_id") or row.get("account") or "").strip() == config.account_id:
            return True
    return False


def _positions_readable(*, broker_status: Mapping[str, Any], positions_snapshot: Mapping[str, Any]) -> bool:
    if broker_status.get("positions_complete") is True:
        return True
    if positions_snapshot.get("positions_complete") is True or positions_snapshot.get("ok") is True:
        return "positions" in positions_snapshot
    return "positions" in positions_snapshot and positions_snapshot.get("position_count") is not None


def _open_orders_readable(*, broker_status: Mapping[str, Any], open_orders_snapshot: Mapping[str, Any]) -> bool:
    if broker_status.get("open_orders_complete") is True:
        return True
    if open_orders_snapshot.get("open_orders_complete") is True or open_orders_snapshot.get("ok") is True:
        return "open_orders" in open_orders_snapshot
    return "open_orders" in open_orders_snapshot and open_orders_snapshot.get("open_order_count") is not None


def _stale(value: datetime | None, *, now: datetime, max_age: float) -> bool:
    if value is None:
        return True
    return (now - value).total_seconds() > max_age


def _latest_snapshot_time(*payloads: Mapping[str, Any]) -> datetime | None:
    values = [_parse_datetime(payload.get("generated_at")) for payload in payloads]
    values = [value for value in values if value is not None]
    return max(values) if values else None


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _extract_error_code(text: str) -> str | None:
    for token in text.replace(":", " ").split():
        if token.isdigit():
            return token
    return None


def _normalize_execution_domain(value: ExecutionDomain | str) -> ExecutionDomain:
    try:
        return value if isinstance(value, ExecutionDomain) else ExecutionDomain(str(value).strip().upper())
    except ValueError as exc:
        raise TrackBModelError("execution_domain must be TRACK_B_PAPER or TRACK_B_LIVE.") from exc


def _normalize_classification(value: BrokerAvailabilityClassification | str) -> BrokerAvailabilityClassification:
    try:
        return value if isinstance(value, BrokerAvailabilityClassification) else BrokerAvailabilityClassification(str(value).strip().upper())
    except ValueError as exc:
        raise TrackBModelError("broker availability classification is invalid.") from exc


def _normalize_source_ref(value: SourceArtifactRef | Mapping[str, Any]) -> SourceArtifactRef:
    if isinstance(value, SourceArtifactRef):
        return value
    return SourceArtifactRef(**dict(value))


def _required_text(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise TrackBModelError(f"{field_name} is required.")
    return text


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
