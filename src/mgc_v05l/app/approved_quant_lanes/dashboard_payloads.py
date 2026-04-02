"""Dashboard payload helpers for approved quant baselines."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .runtime_boundary import (
    APPROVED_QUANT_BASELINE_RUNTIME_CONTRACT_VERSION,
    approved_quant_research_dependency_rows,
)


def load_approved_quant_baselines_snapshot(snapshot_path: Path) -> dict[str, Any]:
    if not snapshot_path.exists():
        return {
            "generated_at": None,
            "status": "missing",
            "rows": [],
            "summary_line": "No approved quant baseline probation snapshot available yet.",
            "operator_summary_line": "No approved quant baseline probation snapshot available yet.",
            "runtime_contract_version": APPROVED_QUANT_BASELINE_RUNTIME_CONTRACT_VERSION,
            "boundary": {
                "adapter_module": "mgc_v05l.app.approved_quant_lanes.runtime_boundary",
                "contract_version": APPROVED_QUANT_BASELINE_RUNTIME_CONTRACT_VERSION,
                "research_dependencies": approved_quant_research_dependency_rows(),
            },
            "artifacts": _default_artifacts(snapshot_path),
        }

    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    payload.setdefault("status", "available")
    payload.setdefault("runtime_contract_version", APPROVED_QUANT_BASELINE_RUNTIME_CONTRACT_VERSION)
    payload.setdefault("artifacts", _default_artifacts(snapshot_path))
    payload.setdefault(
        "boundary",
        {
            "adapter_module": "mgc_v05l.app.approved_quant_lanes.runtime_boundary",
            "contract_version": APPROVED_QUANT_BASELINE_RUNTIME_CONTRACT_VERSION,
            "research_dependencies": approved_quant_research_dependency_rows(),
        },
    )
    for row in payload.get("rows", []):
        row.setdefault("lane_classification", "approved_baseline_lane")
        row.setdefault("promotion_state", row.get("baseline_status", "operator_baseline_candidate"))
        approved_scope = row.get("approved_scope", {})
        row.setdefault(
            "active_exit_logic",
            {
                "exit_style": approved_scope.get("exit_style"),
                "hold_bars": approved_scope.get("hold_bars"),
                "stop_r": approved_scope.get("stop_r"),
                "target_r": approved_scope.get("target_r"),
                "structural_invalidation_r": approved_scope.get("structural_invalidation_r"),
            },
        )
        row.setdefault("warning_flags", [])
        row.setdefault(
            "unknown_session_warning",
            {
                "flag": False,
                "label": "unknown_session_unavailable",
                "trade_share": 0.0,
                "abs_pnl_share_cost_020": 0.0,
            },
        )
        row.setdefault("slice_weakness_flag", False)
        row.setdefault("drift_vs_approval_baseline_cost_020", 0.0)
        row.setdefault("drift_vs_approval_baseline_cost_025", 0.0)
        row.setdefault("latest_weekly_summary", _load_latest_weekly_summary(row))
        row.setdefault("symbol_attribution_summary", _attribution_summary(row["latest_weekly_summary"], key="symbol"))
        row.setdefault("session_attribution_summary", _attribution_summary(row["latest_weekly_summary"], key="session_label"))
        row.setdefault(
            "post_cost_monitoring_read",
            _post_cost_monitoring_read(
                rolling_020=float(row.get("rolling_expectancy_cost_020", 0.0) or 0.0),
                rolling_025=float(row.get("rolling_expectancy_cost_025", 0.0) or 0.0),
            ),
        )
        row.setdefault(
            "approved_exit_label",
            _approved_exit_label(row.get("active_exit_logic", {})),
        )
        row.setdefault(
            "operator_status_line",
            (
                f"APPROVED BASELINE | {row.get('lane_name', row.get('lane_id', 'unknown_lane'))} | "
                f"probation={str(row.get('probation_status', 'unknown')).upper()} | "
                f"promotion={str(row.get('promotion_state', row.get('baseline_status', 'unknown'))).upper()} | "
                f"post_cost={row['post_cost_monitoring_read']['label']}"
            ),
        )
    payload.setdefault("operator_summary_line", " | ".join(row["operator_status_line"] for row in payload.get("rows", [])))
    return payload


def _default_artifacts(snapshot_path: Path) -> dict[str, Any]:
    root_dir = snapshot_path.parent
    return {
        "snapshot_json_path": str(snapshot_path.resolve()),
        "current_status_json_path": str((root_dir / "current_active_baseline_status.json").resolve()),
        "current_status_markdown_path": str((root_dir / "current_active_baseline_status.md").resolve()),
    }


def _load_latest_weekly_summary(row: dict[str, Any]) -> dict[str, Any]:
    weekly_dir_raw = ((row.get("artifacts") or {}).get("weekly_dir")) or None
    if not weekly_dir_raw:
        return {
            "status": "missing",
            "symbol_attribution": [],
            "session_attribution": [],
            "warning_flags": [],
        }
    weekly_dir = Path(str(weekly_dir_raw))
    if not weekly_dir.exists():
        return {
            "status": "missing",
            "symbol_attribution": [],
            "session_attribution": [],
            "warning_flags": [],
        }
    weekly_files = sorted(weekly_dir.glob("*.json"))
    if not weekly_files:
        return {
            "status": "missing",
            "symbol_attribution": [],
            "session_attribution": [],
            "warning_flags": [],
        }
    payload = json.loads(weekly_files[-1].read_text(encoding="utf-8"))
    payload.setdefault("status", "available")
    payload.setdefault("symbol_attribution", [])
    payload.setdefault("session_attribution", [])
    payload.setdefault("warning_flags", [])
    return payload


def _attribution_summary(payload: dict[str, Any], *, key: str) -> list[str]:
    rows = list(payload.get("symbol_attribution" if key == "symbol" else "session_attribution", []))
    label_key = "symbol" if key == "symbol" else "session_label"
    summary = []
    for row in rows[:3]:
        label = row.get(label_key, "-")
        net = float(row.get("net_r_020_total", 0.0) or 0.0)
        trades = int(row.get("trade_count", 0) or 0)
        summary.append(f"{label} {net:+.3f}R ({trades})")
    return summary


def _approved_exit_label(active_exit_logic: dict[str, Any]) -> str:
    exit_style = str(active_exit_logic.get("exit_style") or "")
    hold_bars = active_exit_logic.get("hold_bars")
    if exit_style == "time_stop_only" and hold_bars == 24:
        return "time_stop_only.h24"
    if active_exit_logic.get("structural_invalidation_r") is not None:
        return f"structural_invalidation_r={active_exit_logic['structural_invalidation_r']}"
    return exit_style or "unknown_exit"


def _post_cost_monitoring_read(*, rolling_020: float, rolling_025: float) -> dict[str, Any]:
    if rolling_025 >= 0.0:
        label = "stable_positive_post_cost"
    elif rolling_020 >= 0.0:
        label = "mixed_cost_read"
    else:
        label = "negative_post_cost"
    return {
        "label": label,
        "rolling_expectancy_cost_020": rolling_020,
        "rolling_expectancy_cost_025": rolling_025,
    }
