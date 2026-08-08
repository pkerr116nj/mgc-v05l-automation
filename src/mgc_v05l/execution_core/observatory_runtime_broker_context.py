"""Read-only runtime and broker display context for The Observatory."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping


SCHEMA_VERSION = "observatory_runtime_broker_context_v1"
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "observatory"
    / "runtime_broker_context"
    / "latest_runtime_broker_context.json"
)

RUNTIME_TRUTH_PATH = Path("outputs/track_b_execution_core/runtime_truth/latest_runtime_environment_truth.json")
BROKER_AUTHORITY_PATH = Path("outputs/operator_dashboard/runtime/latest_broker_session_authority.json")
OPEN_ORDER_TRUTH_PATH = Path("outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json")
MANAGED_POSITIONS_PATH = Path("outputs/track_b_execution_core/managed_positions/latest_managed_positions.json")
RECONCILIATION_PATH = Path("outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json")


def build_runtime_broker_context(*, repo_root: Path = REPO_ROOT, generated_at: datetime | None = None) -> dict[str, Any]:
    now = _coerce_datetime(generated_at) or datetime.now(UTC)
    runtime = _read_json(repo_root, RUNTIME_TRUTH_PATH)
    broker = _read_json(repo_root, BROKER_AUTHORITY_PATH)
    open_orders = _read_json(repo_root, OPEN_ORDER_TRUTH_PATH)
    managed_positions = _read_json(repo_root, MANAGED_POSITIONS_PATH)
    reconciliation = _read_json(repo_root, RECONCILIATION_PATH)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now.isoformat(),
        "producer": "observatory_runtime_broker_context",
        "status": _overall_status(runtime, broker, open_orders, managed_positions, reconciliation),
        "source_contract": "PREPARED_RUNTIME_BROKER_ARTIFACTS",
        "runtime": _runtime_section(runtime),
        "broker": _broker_section(broker, open_orders, managed_positions, reconciliation),
        "sources": {
            "runtime_truth": runtime["source_artifact"],
            "broker_session_authority": broker["source_artifact"],
            "open_order_truth": open_orders["source_artifact"],
            "managed_positions": managed_positions["source_artifact"],
            "reconciliation": reconciliation["source_artifact"],
        },
        "guardrails": {
            "display_only": True,
            "trading_input": False,
            "broker_authority": False,
            "runtime_authority": False,
            "strategy_input": False,
            "submit_authority_computation": False,
            "readiness_computation": False,
            "reconciliation_computation": False,
        },
    }
    payload["deterministic_fingerprint"] = _fingerprint(payload)
    return payload


def write_runtime_broker_context(snapshot: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output_path


def _runtime_section(source: Mapping[str, Any]) -> dict[str, Any]:
    data = source.get("data") if isinstance(source.get("data"), Mapping) else {}
    runtime = data.get("runtime") if isinstance(data.get("runtime"), Mapping) else {}
    pid_metadata = data.get("pid_metadata") if isinstance(data.get("pid_metadata"), Mapping) else {}
    startup = data.get("runtime_startup_progress") if isinstance(data.get("runtime_startup_progress"), Mapping) else {}
    return {
        "source_status": source["status"],
        "source_artifact": source["source_artifact"],
        "schema_version": data.get("schema_version"),
        "classification": data.get("classification"),
        "generated_at": data.get("generated_at"),
        "fresh_until": data.get("fresh_until"),
        "mode": data.get("mode"),
        "source_pid": data.get("source_pid"),
        "loaded_commit": runtime.get("commit") or runtime.get("loaded_commit") or pid_metadata.get("commit"),
        "commit_coherence": runtime.get("commit_coherence") or data.get("commit_coherence"),
        "profile": runtime.get("profile") or data.get("profile"),
        "active_lane_count": runtime.get("lane_count") or runtime.get("active_lane_count") or data.get("lane_count"),
        "active_strategy_count": runtime.get("strategy_count") or data.get("strategy_count"),
        "trading_loop_entered": runtime.get("trading_loop_entered") or startup.get("trading_loop_entered"),
        "submit_authority": data.get("submit_authority"),
        "submit_authority_source": "producer_authored" if "submit_authority" in data else "missing",
    }


def _broker_section(
    broker_source: Mapping[str, Any],
    open_order_source: Mapping[str, Any],
    managed_position_source: Mapping[str, Any],
    reconciliation_source: Mapping[str, Any],
) -> dict[str, Any]:
    broker = broker_source.get("data") if isinstance(broker_source.get("data"), Mapping) else {}
    open_orders = open_order_source.get("data") if isinstance(open_order_source.get("data"), Mapping) else {}
    managed_positions = managed_position_source.get("data") if isinstance(managed_position_source.get("data"), Mapping) else {}
    reconciliation = reconciliation_source.get("data") if isinstance(reconciliation_source.get("data"), Mapping) else {}
    return {
        "broker_session_source_status": broker_source["status"],
        "broker_session_source_artifact": broker_source["source_artifact"],
        "schema_version": broker.get("schema_version"),
        "broker_session_classification": broker.get("classification"),
        "broker_session_generated_at": broker.get("generated_at") or broker.get("authority_source_timestamp"),
        "mode": broker.get("mode") or open_orders.get("mode") or managed_positions.get("mode"),
        "server_version": broker.get("server_version"),
        "lease_state": broker.get("lease_state") or reconciliation.get("broker_truth_lease_state"),
        "position_snapshot_timestamp": broker.get("position_snapshot_timestamp"),
        "open_order_snapshot_timestamp": broker.get("open_order_snapshot_timestamp"),
        "callback_missing_reason": broker.get("callback_missing_reason") or reconciliation.get("callback_missing_reason"),
        "broker_truth_reliable_for_position": reconciliation.get("broker_truth_reliable_for_position"),
        "broker_truth_reliable_for_order_status": reconciliation.get("broker_truth_reliable_for_order_status"),
        "open_order_truth_classification": open_orders.get("classification") or reconciliation.get("open_order_truth_classification"),
        "open_order_count": _count(open_orders.get("broker_open_orders"), reconciliation.get("track_b_broker_open_order_count")),
        "unknown_open_order_count": reconciliation.get("unknown_broker_open_order_count"),
        "broker_position_count": reconciliation.get("track_b_broker_position_count") or _count(open_orders.get("broker_positions")),
        "managed_position_classification": managed_positions.get("classification"),
        "managed_position_count": managed_positions.get("managed_position_count") or _count(managed_positions.get("managed_positions")),
        "review_required_count": reconciliation.get("review_required_count"),
        "reconciliation_classification": reconciliation.get("classification"),
        "reconciliation_confidence": reconciliation.get("reconciliation_confidence"),
        "submit_authority": open_orders.get("submit_authority") if "submit_authority" in open_orders else managed_positions.get("submit_authority"),
        "submit_authority_source": "producer_authored"
        if "submit_authority" in open_orders or "submit_authority" in managed_positions
        else "missing",
    }


def _read_json(repo_root: Path, relative_path: Path) -> dict[str, Any]:
    path = _resolve(repo_root, relative_path)
    display_path = _display_path(repo_root, path)
    if not path.exists():
        return {"status": "MISSING", "source_artifact": display_path, "data": None, "error": "source_artifact_missing"}
    try:
        return {"status": "LOADED", "source_artifact": display_path, "data": json.loads(path.read_text(encoding="utf-8")), "error": None}
    except json.JSONDecodeError as exc:
        return {"status": "INVALID", "source_artifact": display_path, "data": None, "error": str(exc)}


def _overall_status(*sources: Mapping[str, Any]) -> str:
    statuses = {source.get("status") for source in sources}
    if "INVALID" in statuses:
        return "INVALID_SOURCE"
    if "MISSING" in statuses:
        return "VALID_WITH_WARNINGS"
    return "READY"


def _count(value: Any, fallback: Any = None) -> int | None:
    if isinstance(value, list):
        return len(value)
    if isinstance(fallback, int):
        return fallback
    return None


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _display_path(repo_root: Path, path: Path) -> str:
    try:
        return str(path.relative_to(repo_root))
    except ValueError:
        return str(path)


def _coerce_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _fingerprint(payload: Mapping[str, Any]) -> str:
    stable = {key: value for key, value in payload.items() if key != "deterministic_fingerprint"}
    return hashlib.sha256(json.dumps(stable, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build Observatory runtime/broker display context from prepared artifacts.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args(list(argv) if argv is not None else None)
    snapshot = build_runtime_broker_context(repo_root=args.repo_root)
    output_path = _resolve(args.repo_root, args.output_json)
    write_runtime_broker_context(snapshot, output_path)
    print(json.dumps({"status": snapshot["status"], "output_json": str(output_path)}, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
