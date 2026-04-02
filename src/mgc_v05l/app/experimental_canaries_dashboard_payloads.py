"""Dashboard payload helpers for experimental paper-only canaries."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any


def load_experimental_canaries_snapshot(snapshot_path: Path) -> dict[str, Any]:
    if not snapshot_path.exists():
        return {
            "generated_at": None,
            "status": "missing",
            "rows": [],
            "scope_label": "No experimental canary snapshot available yet.",
            "operator_summary_line": "No experimental canary snapshot available yet.",
            "kill_switch": {"active": False, "path": None, "operator_action": "No canary package has been generated yet."},
            "artifacts": _default_artifacts(snapshot_path),
        }

    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    payload.setdefault("status", "available")
    payload.setdefault("rows", [])
    payload.setdefault("artifacts", _default_artifacts(snapshot_path))
    kill_switch = payload.get("kill_switch") or {}
    payload["kill_switch"] = {
        "active": bool(kill_switch.get("active")),
        "path": kill_switch.get("path"),
        "operator_action": kill_switch.get("operator_action") or "Use the enable/disable scripts to control canary visibility.",
    }
    normalized_rows = [_normalize_canary_row(row, generated_at=payload.get("generated_at"), global_kill_switch=payload["kill_switch"]) for row in payload["rows"]]
    payload["rows"] = normalized_rows
    enabled_count = sum(1 for row in normalized_rows if row.get("enabled"))
    payload["enabled_count"] = enabled_count
    payload["disabled_count"] = len(normalized_rows) - enabled_count
    payload["operator_summary_line"] = " | ".join(row["operator_status_line"] for row in normalized_rows) if normalized_rows else payload.get("operator_summary_line")
    return payload


def _default_artifacts(snapshot_path: Path) -> dict[str, Any]:
    root_dir = snapshot_path.parent
    return {
        "snapshot_json_path": str(snapshot_path.resolve()),
        "snapshot_markdown_path": str((root_dir / "experimental_canaries_snapshot.md").resolve()),
        "operator_summary_path": str((root_dir / "operator_summary.md").resolve()),
    }


def _normalize_canary_row(row: dict[str, Any], *, generated_at: str | None, global_kill_switch: dict[str, Any]) -> dict[str, Any]:
    artifacts = row.get("artifacts") or {}
    operator_status = _read_json(Path(str(artifacts.get("operator_status") or "")))
    signal_rows = _read_jsonl(Path(str(artifacts.get("signals") or "")))
    event_rows = _read_jsonl(Path(str(artifacts.get("events") or "")))
    allowed_count = sum(1 for item in signal_rows if bool(item.get("signal_passed_flag")))
    blocked_count = sum(1 for item in signal_rows if str(item.get("decision") or "").lower() == "blocked")
    override_counter = Counter(str(item.get("override_reason") or "") for item in signal_rows if item.get("override_reason"))
    top_override_reason = override_counter.most_common(1)[0][0] if override_counter else None
    kill_switch_active = bool(operator_status.get("kill_switch_active")) or bool(global_kill_switch.get("active"))
    enabled = bool(operator_status.get("enabled")) and not kill_switch_active
    lane_name = str(row.get("lane_name") or row.get("display_name") or row.get("lane_id") or "unknown_canary")
    quality_policy = str(row.get("quality_bucket_policy") or "-")
    side = str(row.get("side") or "-")
    metrics = row.get("metrics") or {}
    latest_atp_state = row.get("latest_atp_state") or operator_status.get("latest_atp_state") or {}
    latest_atp_entry_state = row.get("latest_atp_entry_state") or operator_status.get("latest_atp_entry_state") or {}
    latest_atp_timing_state = row.get("latest_atp_timing_state") or operator_status.get("latest_atp_timing_state") or {}
    last_update_timestamp = (
        operator_status.get("generated_at")
        or row.get("generated_at")
        or generated_at
    )
    warning_flags = []
    if kill_switch_active:
        warning_flags.append("kill_switch_active")
    if blocked_count and not enabled:
        warning_flags.append("signals_blocked")
    allow_block_override_label = (
        f"allowed={allowed_count} blocked={blocked_count}"
        + (f" override={top_override_reason}" if top_override_reason else "")
    )
    return {
        **row,
        "lane_name": lane_name,
        "display_name": lane_name,
        "enabled": enabled,
        "disabled": not enabled,
        "kill_switch_active": kill_switch_active,
        "paper_only": True,
        "experimental_status": str(row.get("experimental_status") or "experimental_canary"),
        "quality_bucket_policy": quality_policy,
        "side": side,
        "last_update_timestamp": last_update_timestamp,
        "recent_signal_count": len(signal_rows),
        "recent_event_count": len(event_rows),
        "recent_allowed_signal_count": allowed_count,
        "recent_blocked_signal_count": blocked_count,
        "allow_block_override_summary": {
            "allowed": allowed_count,
            "blocked": blocked_count,
            "top_override_reason": top_override_reason,
            "label": allow_block_override_label,
        },
        "metrics_net_pnl_cash": metrics.get("net_pnl_cash"),
        "metrics_max_drawdown": metrics.get("max_drawdown"),
        "latest_atp_state": latest_atp_state,
        "latest_atp_entry_state": latest_atp_entry_state,
        "latest_atp_timing_state": latest_atp_timing_state,
        "atp_bias_state": latest_atp_state.get("bias_state"),
        "atp_pullback_state": latest_atp_state.get("pullback_state"),
        "atp_pullback_reason": latest_atp_state.get("pullback_reason"),
        "atp_pullback_depth_score": latest_atp_state.get("pullback_depth_score"),
        "atp_pullback_violence_score": latest_atp_state.get("pullback_violence_score"),
        "atp_entry_state": latest_atp_entry_state.get("entry_state"),
        "atp_primary_blocker": latest_atp_entry_state.get("primary_blocker"),
        "atp_continuation_trigger_state": latest_atp_entry_state.get("continuation_trigger_state"),
        "atp_timing_state": latest_atp_timing_state.get("timing_state"),
        "atp_vwap_price_quality_state": latest_atp_timing_state.get("vwap_price_quality_state"),
        "atp_timing_blocker": latest_atp_timing_state.get("primary_blocker"),
        "operator_status_line": (
            f"{lane_name} | {'ENABLED' if enabled else 'DISABLED'} | "
            f"{quality_policy} | bias={latest_atp_state.get('bias_state') or '-'} | "
            f"pullback={latest_atp_state.get('pullback_state') or '-'} | "
            f"entry={latest_atp_entry_state.get('entry_state') or '-'} | "
            f"timing={latest_atp_timing_state.get('timing_state') or '-'} | "
            f"blocker={latest_atp_timing_state.get('primary_blocker') or latest_atp_entry_state.get('primary_blocker') or '-'} | "
            f"signals={len(signal_rows)} | events={len(event_rows)} | {allow_block_override_label}"
        ),
        "warning_flags": warning_flags,
        "warning_summary": (
            "Kill switch active; paper-only canary suppressed."
            if kill_switch_active
            else f"Paper Only | Experimental Canary | {allow_block_override_label}"
        ),
        "operator_status": operator_status,
    }


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rows.append(json.loads(line))
    except (OSError, json.JSONDecodeError):
        return []
    return rows
