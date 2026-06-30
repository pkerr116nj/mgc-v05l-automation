"""Bounded JSON snapshot writers for hot current-state artifacts."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.models import to_jsonable


DEFAULT_MAX_SNAPSHOT_BYTES = int(os.environ.get("MGC_HOT_PATH_SNAPSHOT_MAX_BYTES", str(5 * 1024 * 1024)))
DEFAULT_TARGET_SNAPSHOT_BYTES = int(os.environ.get("MGC_HOT_PATH_SNAPSHOT_TARGET_BYTES", str(1024 * 1024)))
DEFAULT_MAX_DEPTH = int(os.environ.get("MGC_HOT_PATH_SNAPSHOT_MAX_DEPTH", "5"))
DEFAULT_MAX_ITEMS = int(os.environ.get("MGC_HOT_PATH_SNAPSHOT_MAX_ITEMS", "64"))
DEFAULT_MAX_STRING_CHARS = int(os.environ.get("MGC_HOT_PATH_SNAPSHOT_MAX_STRING_CHARS", "4096"))


@dataclass(frozen=True)
class BoundedSnapshotConfig:
    max_bytes: int = DEFAULT_MAX_SNAPSHOT_BYTES
    target_bytes: int = DEFAULT_TARGET_SNAPSHOT_BYTES
    max_depth: int = DEFAULT_MAX_DEPTH
    max_items: int = DEFAULT_MAX_ITEMS
    max_string_chars: int = DEFAULT_MAX_STRING_CHARS
    indent: int | None = 2
    sort_keys: bool = True


@dataclass(frozen=True)
class BoundedSnapshotWriteResult:
    path: Path
    diagnostic_path: Path | None
    original_bytes: int
    written_bytes: int
    degraded: bool
    omitted_sections: tuple[str, ...]


def write_bounded_snapshot_json(
    path: Path,
    payload: Mapping[str, Any],
    *,
    config: BoundedSnapshotConfig | None = None,
    diagnostic_path: Path | None = None,
) -> BoundedSnapshotWriteResult:
    """Atomically write a bounded current-state JSON snapshot.

    The writer never appends. If the original payload is too large, it writes a
    compact degraded snapshot with source metadata preserved and a sidecar
    diagnostic describing the truncation.
    """

    actual = config or BoundedSnapshotConfig()
    if actual.max_bytes <= 0:
        raise ValueError("max_bytes must be positive")
    if actual.target_bytes <= 0:
        raise ValueError("target_bytes must be positive")
    path.parent.mkdir(parents=True, exist_ok=True)
    original_payload = to_jsonable(dict(payload))
    original_blob = _json_bytes(original_payload, config=actual)
    original_bytes = len(original_blob)
    omitted_sections: tuple[str, ...] = ()
    degraded = original_bytes > actual.max_bytes
    output_payload = original_payload
    if degraded:
        output_payload, omitted_sections = _degraded_snapshot(
            original_payload,
            original_bytes=original_bytes,
            config=actual,
        )
        output_blob = _json_bytes(output_payload, config=actual)
        if len(output_blob) > actual.max_bytes:
            output_payload, omitted_sections = _minimal_snapshot(
                original_payload,
                original_bytes=original_bytes,
                config=actual,
                prior_omitted=omitted_sections,
            )
    written_blob = _json_bytes(output_payload, config=actual)
    if len(written_blob) > actual.max_bytes:
        raise ValueError(
            f"bounded snapshot still exceeds max_bytes: {len(written_blob)} > {actual.max_bytes}"
        )
    _atomic_write_bytes(path, written_blob)
    actual_diagnostic_path: Path | None = None
    if degraded:
        actual_diagnostic_path = diagnostic_path or path.with_name(f"{path.stem}_bounded_snapshot_diagnostic.json")
        _write_diagnostic(
            actual_diagnostic_path,
            snapshot_path=path,
            original_bytes=original_bytes,
            written_bytes=len(written_blob),
            config=actual,
            omitted_sections=omitted_sections,
        )
    return BoundedSnapshotWriteResult(
        path=path,
        diagnostic_path=actual_diagnostic_path,
        original_bytes=original_bytes,
        written_bytes=len(written_blob),
        degraded=degraded,
        omitted_sections=omitted_sections,
    )


def _degraded_snapshot(
    payload: Any,
    *,
    original_bytes: int,
    config: BoundedSnapshotConfig,
) -> tuple[dict[str, Any], tuple[str, ...]]:
    if not isinstance(payload, dict):
        return _minimal_snapshot(payload, original_bytes=original_bytes, config=config, prior_omitted=())
    omitted: list[str] = []
    degraded: dict[str, Any] = {}
    preserve_keys = {
        "schema_version",
        "artifact_type",
        "generated_at",
        "updated_at",
        "created_at",
        "runtime_instance_id",
        "runtime_pid",
        "git_head",
        "strategy_id",
        "strategy_count",
        "lane_id",
        "lane_count",
        "strategy_family",
        "instrument",
        "symbol",
        "side",
        "selected_account_id",
        "current_position_quantity",
        "broker_effect_classification",
        "control_plane_snapshot_id",
        "shared_truth_generation_id",
        "callback_timeline_event_count",
        "dashboard_projection_consumed",
        "runtime_supervised",
        "bridge_direct_invocation",
        "classification",
        "dashboard_classification",
        "dashboard_status",
        "routing_policy_classification",
        "paper_monitor_health",
        "paper_monitor_stale",
        "legacy_monitor_authority",
        "submit_capable_count",
        "active_count",
        "blocked_count",
        "warning_count",
        "error_count",
        "order_intent_id",
        "intent_type",
        "signal_id",
        "signal_timestamp",
        "decision_bar_timestamp",
        "bar_id",
        "source_artifact",
        "source_artifact_bar_id",
        "route_target",
        "intended_lifecycle_mode",
        "submit_allowed",
        "submit_attempt_id",
        "submit_attempted",
        "blocker_classification",
        "exact_blocker_reason",
        "monitor_running",
        "health_classification",
        "bridge_allowed",
        "broker_refresh_timestamp",
        "broker_refresh_freshness",
        "account",
        "contract",
        "bridge_classification",
        "bridge_detail",
        "paper_proof_invoked",
        "live_money_readiness",
        "active_lane_ids",
        "healthy_lane_ids",
        "quarantined_lane_ids",
        "health",
        "strategy_status",
        "position_side",
        "entries_enabled",
        "operator_halt",
        "paper_lane_count",
        "executable_lane_count",
        "enabled_lane_count",
        "usable_lane_count",
        "faulted_lane_count",
        "desk_risk_state",
        "desk_risk_reason",
        "last_processed_bar_end_ts",
        "current_detected_session",
    }
    compact_dict_keys = {
        "account_truth",
        "caller_gate",
        "caller_metadata",
        "connection_diagnostics",
        "entry_attempt_memory",
        "entry_execution_pricing",
        "environment",
        "environment_lock_check",
        "exact_contract_report",
        "exit_attempt_policy",
        "exit_execution_pricing",
        "futures_contract_resolver_status",
        "intent",
        "open_orders",
        "paper_strategy_exposure_status",
        "paper_strategy_governance_status",
        "paper_strategy_monitor_status",
        "positions",
        "pre_action_snapshot_validation",
        "qualified_contract_report",
        "quote_context",
        "runtime_control_plane_authorization",
        "strategy_identity",
    }
    compact_list_keys = {
        "errors",
        "live_trade_registry_events",
        "preflight_checks",
    }
    for key, value in payload.items():
        if key in preserve_keys:
            degraded[key] = _compact_value(value, depth=0, config=config)
        elif key in compact_dict_keys and isinstance(value, dict):
            degraded[key] = _compact_value(value, depth=0, config=config)
        elif key in compact_list_keys and isinstance(value, list):
            degraded[key] = [_compact_value(row, depth=0, config=config) for row in value[: config.max_items]]
            if len(value) > config.max_items:
                omitted.append(f"{key}[{config.max_items}:]")
        elif key == "delegated_result" and isinstance(value, dict):
            degraded[key] = _compact_bridge_delegated_result(value, config=config)
        elif key == "prepared_submit_bundle" and isinstance(value, dict):
            degraded[key] = _compact_prepared_submit_bundle(value, config=config)
        elif key in {
            "active_rows",
            "blocked_rows",
            "ibkr_routed_rows",
            "internal_only_rows",
            "paused_or_disabled_rows",
            "performance_rows",
            "rows",
            "strategies",
            "strategy_rows",
        } and isinstance(value, list):
            degraded[key] = [_compact_dashboard_row(row, config=config) for row in value[: config.max_items]]
            if len(value) > config.max_items:
                omitted.append(f"{key}[{config.max_items}:]")
        elif key == "lanes" and isinstance(value, list):
            degraded[key] = [_compact_lane(row, config=config) for row in value[: config.max_items]]
            if len(value) > config.max_items:
                omitted.append(f"lanes[{config.max_items}:]")
        elif key in {"summary", "counts", "top_findings", "warnings", "errors"}:
            degraded[key] = _compact_value(value, depth=0, config=config)
        elif key.endswith("_path") or key.endswith("_paths"):
            degraded[key] = _compact_value(value, depth=0, config=config)
        else:
            omitted.append(str(key))
    degraded["_bounded_snapshot"] = {
        "degraded": True,
        "generated_at": datetime.now(UTC).isoformat(),
        "original_bytes": original_bytes,
        "max_bytes": config.max_bytes,
        "target_bytes": config.target_bytes,
        "omitted_section_count": len(omitted),
        "omitted_sections": omitted[: config.max_items],
    }
    return degraded, tuple(omitted)


def _minimal_snapshot(
    payload: Any,
    *,
    original_bytes: int,
    config: BoundedSnapshotConfig,
    prior_omitted: tuple[str, ...],
) -> tuple[dict[str, Any], tuple[str, ...]]:
    source = payload if isinstance(payload, dict) else {}
    minimal = {
        "schema_version": source.get("schema_version"),
        "generated_at": source.get("generated_at") or source.get("updated_at"),
        "updated_at": source.get("updated_at"),
        "runtime_instance_id": source.get("runtime_instance_id"),
        "runtime_pid": source.get("runtime_pid"),
        "git_head": source.get("git_head"),
        "classification": source.get("classification"),
        "selected_account_id": source.get("selected_account_id"),
        "current_position_quantity": source.get("current_position_quantity"),
        "broker_effect_classification": source.get("broker_effect_classification"),
        "intent": _compact_value(source.get("intent"), depth=0, config=config) if isinstance(source.get("intent"), dict) else {},
        "strategy_identity": _compact_value(source.get("strategy_identity"), depth=0, config=config) if isinstance(source.get("strategy_identity"), dict) else {},
        "environment": _compact_value(source.get("environment"), depth=0, config=config) if isinstance(source.get("environment"), dict) else {},
        "caller_metadata": _compact_value(source.get("caller_metadata"), depth=0, config=config) if isinstance(source.get("caller_metadata"), dict) else {},
        "qualified_contract_report": _compact_value(source.get("qualified_contract_report"), depth=0, config=config) if isinstance(source.get("qualified_contract_report"), dict) else {},
        "exact_contract_report": _compact_value(source.get("exact_contract_report"), depth=0, config=config) if isinstance(source.get("exact_contract_report"), dict) else {},
        "entry_execution_pricing": _compact_value(source.get("entry_execution_pricing"), depth=0, config=config) if isinstance(source.get("entry_execution_pricing"), dict) else {},
        "delegated_result": _compact_bridge_delegated_result(source.get("delegated_result"), config=config) if isinstance(source.get("delegated_result"), dict) else None,
        "active_lane_ids": source.get("active_lane_ids") if isinstance(source.get("active_lane_ids"), list) else [],
        "paper_lane_count": source.get("paper_lane_count"),
        "health": source.get("health") if isinstance(source.get("health"), dict) else {},
        "strategy_status": source.get("strategy_status"),
        "position_side": source.get("position_side"),
        "_bounded_snapshot": {
            "degraded": True,
            "minimal": True,
            "generated_at": datetime.now(UTC).isoformat(),
            "original_bytes": original_bytes,
            "max_bytes": config.max_bytes,
            "target_bytes": config.target_bytes,
            "omitted_section_count": len(prior_omitted),
            "omitted_sections": list(prior_omitted[: config.max_items]),
        },
    }
    return minimal, prior_omitted


def _compact_lane(row: Any, *, config: BoundedSnapshotConfig) -> Any:
    if not isinstance(row, Mapping):
        return _compact_value(row, depth=0, config=config)
    keep = {
        "lane_id",
        "display_name",
        "symbol",
        "strategy_family",
        "runtime_kind",
        "session_restriction",
        "position_side",
        "strategy_status",
        "entries_enabled",
        "operator_halt",
        "fault_code",
        "risk_state",
        "halt_reason",
        "unblock_action",
        "intent_count",
        "fill_count",
        "open_order_count",
        "internal_position_qty",
        "broker_position_qty",
        "entry_timestamp",
        "last_processed_bar_end_ts",
        "startup_reconciliation_classification",
        "quarantine_state",
        "quarantined",
    }
    compact = {key: _compact_value(value, depth=0, config=config) for key, value in row.items() if key in keep}
    omitted = sorted(str(key) for key in row if key not in keep)
    if omitted:
        compact["_bounded_snapshot_omitted_keys"] = omitted[: config.max_items]
        compact["_bounded_snapshot_omitted_key_count"] = len(omitted)
    return compact


def _compact_dashboard_row(row: Any, *, config: BoundedSnapshotConfig) -> Any:
    if not isinstance(row, Mapping):
        return _compact_value(row, depth=0, config=config)
    keep = {
        "strategy_id",
        "bridge_strategy_id",
        "lane_id",
        "display_name",
        "instrument",
        "symbol",
        "side",
        "session",
        "strategy_family",
        "strategy_status",
        "current_routing_mode",
        "current_order_destination",
        "execution_mode",
        "submit_allowed",
        "strategy_approved",
        "paper_strategy_approved",
        "approved_phase1_strategy",
        "ibkr_bridge_submit_capable",
        "paper_candidate_scope",
        "live_money_eligible",
        "paper_proof_invoked",
        "submit_block_reasons",
        "pause_reasons",
        "backend_source_readiness_detail",
        "broker_session_authority_classification",
        "broker_session_connection_mode",
        "broker_session_submit_alignment",
        "phase1_broker_reconciliation_gate",
        "routing_policy_classification",
        "orders_today",
        "orders_this_week",
        "realized_pnl",
        "unrealized_pnl",
        "drawdown",
    }
    compact = {key: _compact_value(value, depth=0, config=config) for key, value in row.items() if key in keep}
    omitted = sorted(str(key) for key in row if key not in keep)
    if omitted:
        compact["_bounded_snapshot_omitted_keys"] = omitted[: config.max_items]
        compact["_bounded_snapshot_omitted_key_count"] = len(omitted)
    return compact


def _compact_bridge_delegated_result(value: Any, *, config: BoundedSnapshotConfig) -> Any:
    if not isinstance(value, Mapping):
        return _compact_value(value, depth=0, config=config)
    keep = {
        "classification",
        "broker_effect_classification",
        "entry_execution_pricing",
        "execution_id",
        "fill_price",
        "filled_quantity",
        "order_id",
        "perm_id",
        "status",
        "submitted_order_id",
    }
    compact = {key: _compact_value(item, depth=0, config=config) for key, item in value.items() if key in keep}
    report = value.get("report")
    if isinstance(report, Mapping):
        compact["report"] = _compact_bridge_delegate_report(report, config=config)
    omitted = sorted(str(key) for key in value if key not in keep and key != "report")
    if omitted:
        compact["_bounded_snapshot_omitted_keys"] = omitted[: config.max_items]
        compact["_bounded_snapshot_omitted_key_count"] = len(omitted)
    return compact


def _compact_bridge_delegate_report(report: Mapping[str, Any], *, config: BoundedSnapshotConfig) -> dict[str, Any]:
    keep = {
        "classification",
        "generated_at",
        "submit_cancel_lifecycle",
        "latest_order_status",
        "open_order_after_submit",
        "open_order_after_cancel",
        "execution_truth",
        "preview_payload",
        "order_state",
        "warning_text",
        "why_held",
        "reject_reason",
        "advanced_reject_json",
    }
    compact = {key: _compact_value(item, depth=0, config=config) for key, item in report.items() if key in keep}
    omitted = sorted(str(key) for key in report if key not in keep)
    if omitted:
        compact["_bounded_snapshot_omitted_keys"] = omitted[: config.max_items]
        compact["_bounded_snapshot_omitted_key_count"] = len(omitted)
    return compact


def _compact_prepared_submit_bundle(value: Any, *, config: BoundedSnapshotConfig) -> Any:
    if not isinstance(value, Mapping):
        return _compact_value(value, depth=0, config=config)
    keep = {
        "frozen_preview_path",
        "preview_digest",
        "bundle_path",
        "approval_digest",
        "approval_phrase_present",
        "intent",
        "contract",
        "order",
    }
    compact = {key: _compact_value(item, depth=0, config=config) for key, item in value.items() if key in keep}
    omitted = sorted(str(key) for key in value if key not in keep)
    if omitted:
        compact["_bounded_snapshot_omitted_keys"] = omitted[: config.max_items]
        compact["_bounded_snapshot_omitted_key_count"] = len(omitted)
    return compact


def _compact_value(value: Any, *, depth: int, config: BoundedSnapshotConfig) -> Any:
    value = to_jsonable(value)
    if depth >= config.max_depth:
        return _summary(value)
    if isinstance(value, dict):
        items = list(value.items())
        compact = {
            str(key): _compact_value(item, depth=depth + 1, config=config)
            for key, item in items[: config.max_items]
        }
        if len(items) > config.max_items:
            compact["_bounded_snapshot_omitted_item_count"] = len(items) - config.max_items
        return compact
    if isinstance(value, list):
        compact_list = [_compact_value(item, depth=depth + 1, config=config) for item in value[: config.max_items]]
        if len(value) > config.max_items:
            compact_list.append({"_bounded_snapshot_omitted_item_count": len(value) - config.max_items})
        return compact_list
    if isinstance(value, str) and len(value) > config.max_string_chars:
        return value[: config.max_string_chars] + f"...[truncated {len(value) - config.max_string_chars} chars]"
    return value


def _summary(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return {"_bounded_snapshot_summary": "dict", "item_count": len(value)}
    if isinstance(value, list):
        return {"_bounded_snapshot_summary": "list", "item_count": len(value)}
    text = str(value)
    return {"_bounded_snapshot_summary": type(value).__name__, "preview": text[:128]}


def _write_diagnostic(
    path: Path,
    *,
    snapshot_path: Path,
    original_bytes: int,
    written_bytes: int,
    config: BoundedSnapshotConfig,
    omitted_sections: tuple[str, ...],
) -> None:
    payload = {
        "schema_version": "bounded_snapshot_diagnostic_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "snapshot_path": str(snapshot_path),
        "classification": "BOUNDED_SNAPSHOT_DEGRADED",
        "original_bytes": original_bytes,
        "written_bytes": written_bytes,
        "max_bytes": config.max_bytes,
        "target_bytes": config.target_bytes,
        "omitted_section_count": len(omitted_sections),
        "omitted_sections": list(omitted_sections[: config.max_items]),
        "diagnostic_only": True,
    }
    blob = _json_bytes(payload, config=BoundedSnapshotConfig(max_bytes=max(config.max_bytes, 65536)))
    _atomic_write_bytes(path, blob)


def _json_bytes(payload: Any, *, config: BoundedSnapshotConfig) -> bytes:
    return (json.dumps(payload, sort_keys=config.sort_keys, indent=config.indent, default=str) + "\n").encode("utf-8")


def _atomic_write_bytes(path: Path, blob: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with tmp.open("wb") as handle:
            handle.write(blob)
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)
