"""Read-only Track B shared-truth refresh CLI.

This command refreshes execution_core authority artifacts only. Dashboard
artifacts are never consumed as authority and dashboard projections are not
written by this shared refresh path.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .track_b_broker_truth_lease import (
    DEFAULT_LEASE_ARTIFACT,
    DEFAULT_LEASE_HISTORY,
    classify_broker_truth_lease,
    write_broker_truth_lease,
)
from .track_b_managed_order_registry import (
    NO_MANAGED_ORDERS,
    TrackBManagedOrderRegistryConfig,
    build_track_b_managed_order_registry,
    write_track_b_managed_order_registry,
)
from .track_b_managed_position_registry import (
    NO_MANAGED_POSITIONS,
    TrackBManagedPositionRegistryConfig,
    build_track_b_managed_position_registry,
    write_track_b_managed_position_registry,
)
from .track_b_open_order_truth import (
    NO_OPEN_ORDERS,
    TrackBOpenOrderTruthConfig,
    build_track_b_open_order_truth,
    write_track_b_open_order_truth,
)
from .track_b_paper_broker_reconciliation import (
    PAPER_ACCOUNT,
    PHASE1_RUNTIME_TICKER_ORDER,
)
from .track_b_position_truth_monitor import TrackBPositionTruthMonitorConfig, build_track_b_position_truth, write_track_b_position_truth
from .track_b_runtime_environment_truth import (
    RUNTIME_ACTIVE_OBSERVATION_ONLY,
    RUNTIME_ACTIVE_TRADE_CAPABLE,
    RUNTIME_DOWN_CLEAN,
    TrackBRuntimeEnvironmentTruthConfig,
    build_track_b_runtime_environment_truth,
    write_track_b_runtime_environment_truth,
)


DEFAULT_BROKER_TRUTH_STATUS = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json"
)
DEFAULT_BROKER_TRUTH_LATEST_ATTEMPT = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_latest_attempt_status.json"
)
DEFAULT_LIVE_POSITION_STATUS = (
    Path("outputs") / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_live_position_status.json"
)
DEFAULT_TRADE_SUMMARY = (
    Path("outputs") / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_paper_trade_summary.json"
)
DEFAULT_RECONCILIATION_ARTIFACT = (
    Path("outputs")
    / "reports"
    / "track_b_paper_broker_reconciliation"
    / "latest_track_b_paper_broker_reconciliation.json"
)


@dataclass(frozen=True)
class TrackBSharedTruthRefreshConfig:
    repo_root: Path
    account: str = PAPER_ACCOUNT
    symbols: tuple[str, ...] = PHASE1_RUNTIME_TICKER_ORDER
    broker_truth_status_path: Path = DEFAULT_BROKER_TRUTH_STATUS
    broker_truth_latest_attempt_path: Path = DEFAULT_BROKER_TRUTH_LATEST_ATTEMPT
    live_position_status_path: Path = DEFAULT_LIVE_POSITION_STATUS
    trade_summary_path: Path = DEFAULT_TRADE_SUMMARY
    broker_lease_path: Path = DEFAULT_LEASE_ARTIFACT
    broker_lease_history_path: Path | None = DEFAULT_LEASE_HISTORY
    broker_lease_max_entry_age_seconds: float = 300.0
    broker_lease_max_exit_age_seconds: float = 900.0
    broker_lease_degraded_refresh_grace_seconds: float = 120.0

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def refresh_track_b_shared_truth(
    *,
    config: TrackBSharedTruthRefreshConfig,
    now: datetime | None = None,
    pid_running: Callable[[int], bool] | None = None,
    process_root_resolver: Callable[[int], Path | None] | None = None,
    source_commit_resolver: Callable[[Path], str | None] | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))

    open_order_config = TrackBOpenOrderTruthConfig(repo_root=config.repo_root, dashboard_projection_path=None)
    open_order_truth = build_track_b_open_order_truth(config=open_order_config, now=actual_now)
    open_order_path, _ = write_track_b_open_order_truth(config=open_order_config, payload=open_order_truth, now=actual_now)

    managed_order_config = TrackBManagedOrderRegistryConfig(repo_root=config.repo_root, dashboard_projection_path=None)
    managed_order_registry = build_track_b_managed_order_registry(config=managed_order_config, now=actual_now)
    managed_order_path, _ = write_track_b_managed_order_registry(
        config=managed_order_config,
        payload=managed_order_registry,
        now=actual_now,
    )

    position_config = TrackBPositionTruthMonitorConfig(repo_root=config.repo_root, dashboard_projection_path=None)
    position_truth = build_track_b_position_truth(config=position_config, now=actual_now)
    position_path, _ = write_track_b_position_truth(config=position_config, payload=position_truth, now=actual_now)

    runtime_config = TrackBRuntimeEnvironmentTruthConfig(repo_root=config.repo_root, dashboard_projection_path=None)
    runtime_environment_truth = build_track_b_runtime_environment_truth(
        config=runtime_config,
        now=actual_now,
        pid_running=pid_running,
        process_root_resolver=process_root_resolver,
        source_commit_resolver=source_commit_resolver,
    )
    runtime_path, _ = write_track_b_runtime_environment_truth(
        config=runtime_config,
        payload=runtime_environment_truth,
        now=actual_now,
    )

    managed_position_config = TrackBManagedPositionRegistryConfig(repo_root=config.repo_root, dashboard_projection_path=None)
    managed_position_registry = build_track_b_managed_position_registry(config=managed_position_config, now=actual_now)
    managed_position_path, _ = write_track_b_managed_position_registry(
        config=managed_position_config,
        payload=managed_position_registry,
        now=actual_now,
    )

    # Managed Order Registry enriches lower-level Open Order Truth with managed
    # position context, while Position Truth and Managed Position Registry also
    # consume Managed Order Registry evidence. One bounded convergence pass keeps
    # the command one-shot even when prior artifacts were stale.
    managed_order_registry = build_track_b_managed_order_registry(config=managed_order_config, now=actual_now)
    managed_order_path, _ = write_track_b_managed_order_registry(
        config=managed_order_config,
        payload=managed_order_registry,
        now=actual_now,
    )
    position_truth = build_track_b_position_truth(config=position_config, now=actual_now)
    position_path, _ = write_track_b_position_truth(config=position_config, payload=position_truth, now=actual_now)
    runtime_environment_truth = build_track_b_runtime_environment_truth(
        config=runtime_config,
        now=actual_now,
        pid_running=pid_running,
        process_root_resolver=process_root_resolver,
        source_commit_resolver=source_commit_resolver,
    )
    runtime_path, _ = write_track_b_runtime_environment_truth(
        config=runtime_config,
        payload=runtime_environment_truth,
        now=actual_now,
    )
    managed_position_registry = build_track_b_managed_position_registry(config=managed_position_config, now=actual_now)
    managed_position_path, _ = write_track_b_managed_position_registry(
        config=managed_position_config,
        payload=managed_position_registry,
        now=actual_now,
    )

    reconciliation = _read_json(config.resolve(DEFAULT_RECONCILIATION_ARTIFACT))
    broker_lease = _refresh_broker_lease(config=config, reconciliation=reconciliation, now=actual_now)
    services = [
        _service_row("Open Order Truth", open_order_truth, open_order_path),
        _service_row("Managed Order Registry", managed_order_registry, managed_order_path),
        _service_row("Position Truth", position_truth, position_path, summary_key="overall_classification"),
        _service_row("Runtime Environment Truth", runtime_environment_truth, runtime_path),
        _service_row("Managed Position Registry", managed_position_registry, managed_position_path),
        _reconciliation_row(config=config, reconciliation=reconciliation),
        _broker_lease_row(config=config, broker_lease=broker_lease),
    ]
    warnings = _warnings(services=services, payloads={
        "open_order_truth": open_order_truth,
        "managed_order_registry": managed_order_registry,
        "position_truth": position_truth,
        "runtime_environment_truth": runtime_environment_truth,
        "managed_position_registry": managed_position_registry,
        "reconciliation": reconciliation,
        "broker_lease": broker_lease,
    })
    blockers = _unsafe_blockers(
        open_order_truth=open_order_truth,
        managed_order_registry=managed_order_registry,
        position_truth=position_truth,
        runtime_environment_truth=runtime_environment_truth,
        managed_position_registry=managed_position_registry,
        reconciliation=reconciliation,
        broker_lease=broker_lease,
    )
    exit_code = 2 if blockers else 0
    return {
        "schema_version": "track_b_shared_truth_refresh_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "services": services,
        "classifications": {str(row["service"]): row.get("classification") for row in services},
        "artifact_paths": {str(row["service"]): row.get("artifact_path") for row in services if row.get("artifact_path")},
        "warnings": warnings,
        "unsafe_blockers": blockers,
        "exit_code": exit_code,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh read-only Track B PAPER shared-truth authority artifacts.")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[3]))
    parser.add_argument("--account", default=PAPER_ACCOUNT)
    parser.add_argument("--symbols", default=",".join(PHASE1_RUNTIME_TICKER_ORDER))
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a compact table.")
    parser.add_argument("--no-broker-lease-history", action="store_true", help="Skip broker lease history append.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = Path(args.repo_root).expanduser().resolve()
    symbols = tuple(symbol.strip().upper() for symbol in str(args.symbols).split(",") if symbol.strip())
    config = TrackBSharedTruthRefreshConfig(
        repo_root=repo_root,
        account=str(args.account),
        symbols=symbols,
        broker_lease_history_path=None if bool(args.no_broker_lease_history) else DEFAULT_LEASE_HISTORY,
    )
    result = refresh_track_b_shared_truth(config=config)
    if bool(args.json):
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print_classification_table(result)
    return int(result.get("exit_code") or 0)


def print_classification_table(result: Mapping[str, Any]) -> None:
    print("Track B Shared Truth Refresh")
    print(f"generated_at: {result.get('generated_at')}")
    print("")
    print(f"{'Service':<30} {'Classification':<40} Artifact")
    print(f"{'-' * 30} {'-' * 40} {'-' * 8}")
    for row in _list(result.get("services")):
        print(f"{str(row.get('service') or ''):<30} {str(row.get('classification') or ''):<40} {row.get('artifact_path') or ''}")
    warnings = _list(result.get("warnings"))
    blockers = _list(result.get("unsafe_blockers"))
    if warnings:
        print("")
        print("Warnings:")
        for warning in warnings:
            print(f"- {warning.get('code')}: {warning.get('detail')}")
    if blockers:
        print("")
        print("Unsafe blockers:")
        for blocker in blockers:
            print(f"- {blocker.get('code')}: {blocker.get('detail')}")


def _refresh_broker_lease(
    *,
    config: TrackBSharedTruthRefreshConfig,
    reconciliation: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    broker_status = _read_json(config.resolve(config.broker_truth_status_path))
    latest_attempt = _read_json(config.resolve(config.broker_truth_latest_attempt_path)) or _mapping(
        broker_status.get("latest_attempt_status")
    )
    if not broker_status and not reconciliation:
        return {}
    live_position_status = _read_json(config.resolve(config.live_position_status_path))
    trade_summary = _read_json(config.resolve(config.trade_summary_path))
    lease = classify_broker_truth_lease(
        {
            "account_id": config.account,
            "allowed_instruments": list(config.symbols),
            "current_time": now.isoformat(),
            "policy": {
                "max_entry_age_seconds": float(config.broker_lease_max_entry_age_seconds),
                "max_exit_age_seconds": float(config.broker_lease_max_exit_age_seconds),
                "degraded_refresh_grace_seconds": float(config.broker_lease_degraded_refresh_grace_seconds),
            },
            "last_successful_broker_truth": _mapping(broker_status.get("last_successful_broker_truth")) or broker_status,
            "latest_attempt_status": latest_attempt,
            "reconciliation": reconciliation,
            "lifecycle": _lifecycle_summary(live_position_status),
            "order_state": _order_state_summary(trade_summary, reconciliation),
            "source_artifact_paths": {
                "broker_truth_status": str(config.resolve(config.broker_truth_status_path)),
                "latest_attempt": str(config.resolve(config.broker_truth_latest_attempt_path)),
                "reconciliation": str(config.resolve(DEFAULT_RECONCILIATION_ARTIFACT)),
                "lifecycle": str(config.resolve(config.live_position_status_path)),
                "order_state": str(config.resolve(config.trade_summary_path)),
            },
            "source_artifact_timestamps": {
                key: value
                for key, value in {
                    "broker_truth_status": _artifact_timestamp(config.resolve(config.broker_truth_status_path), broker_status),
                    "latest_attempt": _artifact_timestamp(config.resolve(config.broker_truth_latest_attempt_path), latest_attempt),
                    "reconciliation": _artifact_timestamp(config.resolve(DEFAULT_RECONCILIATION_ARTIFACT), reconciliation),
                    "lifecycle": _artifact_timestamp(config.resolve(config.live_position_status_path), live_position_status),
                    "order_state": _artifact_timestamp(config.resolve(config.trade_summary_path), trade_summary),
                }.items()
                if value is not None
            },
        }
    )
    write_broker_truth_lease(
        output_path=config.resolve(config.broker_lease_path),
        lease=lease,
        history_path=None if config.broker_lease_history_path is None else config.resolve(config.broker_lease_history_path),
    )
    return lease


def _service_row(
    service: str,
    payload: Mapping[str, Any],
    artifact_path: Path,
    *,
    summary_key: str | None = None,
) -> dict[str, Any]:
    summary = _mapping(payload.get("summary"))
    classification = payload.get("classification")
    if summary_key is not None:
        classification = summary.get(summary_key) or classification
    return {
        "service": service,
        "classification": classification,
        "generated_at": payload.get("generated_at"),
        "artifact_path": str(artifact_path),
    }


def _reconciliation_row(*, config: TrackBSharedTruthRefreshConfig, reconciliation: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "service": "Reconciliation",
        "classification": reconciliation.get("classification") or "MISSING",
        "generated_at": reconciliation.get("generated_at"),
        "artifact_path": str(config.resolve(DEFAULT_RECONCILIATION_ARTIFACT)),
    }


def _broker_lease_row(*, config: TrackBSharedTruthRefreshConfig, broker_lease: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "service": "Broker Truth Lease",
        "classification": broker_lease.get("lease_state") or "MISSING",
        "generated_at": broker_lease.get("generated_at"),
        "artifact_path": str(config.resolve(config.broker_lease_path)),
    }


def _warnings(*, services: Sequence[Mapping[str, Any]], payloads: Mapping[str, Mapping[str, Any]]) -> list[dict[str, str]]:
    warnings: list[dict[str, str]] = []
    for row in services:
        if row.get("classification") in {"MISSING", "ORDER_STATE_UNKNOWN_REVIEW_REQUIRED", "STALE_MANAGED_POSITION_EVIDENCE"}:
            warnings.append(
                {
                    "code": f"{str(row.get('service') or 'service').lower().replace(' ', '_')}_attention",
                    "detail": f"{row.get('service')} classification is {row.get('classification')}.",
                }
            )
    for name, payload in payloads.items():
        stale_sources = _stale_sources(payload)
        if stale_sources:
            warnings.append(
                {
                    "code": f"{name}_stale_sources",
                    "detail": f"{name} reports stale source evidence: {', '.join(stale_sources)}.",
                }
            )
    return _dedupe_codes(warnings)


def _unsafe_blockers(
    *,
    open_order_truth: Mapping[str, Any],
    managed_order_registry: Mapping[str, Any],
    position_truth: Mapping[str, Any],
    runtime_environment_truth: Mapping[str, Any],
    managed_position_registry: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    broker_lease: Mapping[str, Any],
) -> list[dict[str, str]]:
    blockers: list[dict[str, str]] = []
    position_class = str(_mapping(position_truth.get("summary")).get("overall_classification") or "")
    runtime_class = str(runtime_environment_truth.get("classification") or "")
    managed_position_class = str(managed_position_registry.get("classification") or "")
    managed_order_class = str(managed_order_registry.get("classification") or "")
    open_order_class = str(open_order_truth.get("classification") or "")
    reconciliation_class = str(reconciliation.get("classification") or "")
    lease_state = str(broker_lease.get("lease_state") or "")

    if open_order_class not in {NO_OPEN_ORDERS, "OPEN_CLOSE_ORDER_WORKING", "OPEN_ENTRY_ORDER_WORKING"}:
        blockers.append({"code": "open_order_truth_blocked", "detail": f"Open Order Truth is {open_order_class}."})
    if managed_order_class not in {NO_MANAGED_ORDERS, "WORKING_CLOSE_ORDER", "WORKING_ENTRY_ORDER", "CLOSE_ORDER_MODIFIABLE"}:
        blockers.append({"code": "managed_order_registry_blocked", "detail": f"Managed Order Registry is {managed_order_class}."})
    if position_class and position_class != "CLEAN_FLAT_READY":
        blockers.append({"code": "position_truth_attention_required", "detail": f"Position Truth is {position_class}."})
    if runtime_class not in {RUNTIME_DOWN_CLEAN, RUNTIME_ACTIVE_TRADE_CAPABLE, RUNTIME_ACTIVE_OBSERVATION_ONLY}:
        blockers.append({"code": "runtime_environment_blocked", "detail": f"Runtime Environment Truth is {runtime_class}."})
    if managed_position_class not in {NO_MANAGED_POSITIONS, "OPEN_MANAGED_MATCHED", "OPEN_MANAGED_EXIT_DUE", "OPEN_MANAGED_CLOSE_WORKING"}:
        if not (managed_position_class == "STALE_MANAGED_POSITION_EVIDENCE" and position_class == "CLEAN_FLAT_READY"):
            blockers.append(
                {"code": "managed_position_registry_blocked", "detail": f"Managed Position Registry is {managed_position_class}."}
            )
    if reconciliation_class and reconciliation_class != "TRACK_B_PAPER_BROKER_RECONCILED":
        blockers.append({"code": "reconciliation_blocked", "detail": f"Reconciliation is {reconciliation_class}."})
    if lease_state.startswith("INVALIDATED"):
        blockers.append({"code": "broker_lease_invalidated", "detail": f"Broker Truth Lease is {lease_state}."})
    if lease_state == "OPERATOR_REQUIRED" and position_class != "CLEAN_FLAT_READY":
        blockers.append({"code": "broker_lease_operator_required", "detail": "Broker Truth Lease requires operator attention."})
    return blockers


def _stale_sources(payload: Mapping[str, Any]) -> list[str]:
    sources = _mapping(payload.get("source_freshness")).get("stale_sources")
    if isinstance(sources, list):
        return [str(item) for item in sources if str(item)]
    evidence_stale = []
    for key, value in payload.items():
        if key.endswith("_stale_or_missing") and value is True:
            evidence_stale.append(key)
    return evidence_stale


def _lifecycle_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
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
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat()
    except OSError:
        return None


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _ensure_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _dedupe_codes(rows: Sequence[Mapping[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    result: list[dict[str, str]] = []
    for row in rows:
        code = str(row.get("code") or "")
        if not code or code in seen:
            continue
        seen.add(code)
        result.append({"code": code, "detail": str(row.get("detail") or "")})
    return result


if __name__ == "__main__":
    raise SystemExit(main())
