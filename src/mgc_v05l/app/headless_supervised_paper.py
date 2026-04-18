from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _dependency_map(startup_control_plane: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = {}
    for row in list(startup_control_plane.get("dependencies") or []):
        if isinstance(row, dict):
            key = str(row.get("key") or "").strip()
            if key:
                rows[key] = row
    return rows


def _dependency_summary(row: dict[str, Any], *, fallback_label: str) -> dict[str, Any]:
    return {
        "label": str(row.get("label") or fallback_label),
        "state": str(row.get("state") or "UNKNOWN").upper(),
        "reason": str(row.get("reason") or row.get("summary_line") or "").strip() or None,
        "reason_code": str(row.get("reason_code") or "").strip() or None,
        "next_action": str(row.get("next_action_label") or "").strip() or None,
        "operator_action_required": bool(row.get("operator_action_required")),
        "authoritative_artifact": str(row.get("authoritative_artifact") or "").strip() or None,
        "authoritative_artifact_label": str(row.get("authoritative_artifact_label") or "").strip() or None,
    }


def build_headless_supervised_paper_contract(
    *,
    health_payload: dict[str, Any] | None,
    startup_control_plane: dict[str, Any] | None,
    supervised_paper_operability: dict[str, Any] | None,
    dashboard_info: dict[str, Any] | None = None,
) -> dict[str, Any]:
    generated_at = _utc_now_iso()
    health = _as_dict(health_payload)
    startup = _as_dict(startup_control_plane)
    operability = _as_dict(supervised_paper_operability)
    dashboard = _as_dict(dashboard_info)

    dependencies = _dependency_map(startup)
    backend_dep = _dependency_summary(dependencies.get("dashboard_backend", {}), fallback_label="Dashboard / Backend")
    auth_dep = _dependency_summary(dependencies.get("schwab_connectivity", {}), fallback_label="Schwab Connectivity / Auth")
    paper_dep = _dependency_summary(dependencies.get("paper_runtime", {}), fallback_label="Paper Runtime")
    reconciliation_dep = _dependency_summary(dependencies.get("reconciliation", {}), fallback_label="Reconciliation Needed")

    health_ready = bool(health.get("ready"))
    startup_ready = str(startup.get("overall_state") or "").upper() == "READY"
    launch_allowed = bool(startup.get("launch_allowed"))
    dashboard_attached = bool(operability.get("dashboard_attached"))
    paper_runtime_ready = bool(operability.get("paper_runtime_ready"))
    runtime_running = bool(operability.get("runtime_running"))
    backend_dependency_ready = backend_dep["state"] == "READY"
    backend_attached = (
        health_ready
        and dashboard_attached
        and backend_dependency_ready
    )
    usable = bool(operability.get("app_usable_for_supervised_paper"))
    usable = (
        usable
        and backend_attached
        and runtime_running
        and paper_runtime_ready
    )

    unusable_reason = (
        str(operability.get("unusable_reason") or "").strip()
        or str(startup.get("primary_reason") or "").strip()
        or backend_dep.get("reason")
        or "Headless supervised paper host is not yet usable."
    )
    unusable_reason_code = (
        str(operability.get("unusable_reason_code") or "").strip()
        or str(startup.get("primary_reason_code") or "").strip()
        or str(backend_dep.get("reason_code") or "").strip()
        or "not_usable"
    )

    if not health_ready:
        unusable_reason = "Dashboard/backend health is not currently reachable."
        unusable_reason_code = "backend_health_unreachable"
    if usable:
        overall_state = "USABLE"
    elif not health_ready:
        overall_state = "BACKEND_UNAVAILABLE"
    elif not backend_attached:
        overall_state = "ATTACH_INCOMPLETE"
    elif not runtime_running or not paper_runtime_ready:
        overall_state = "PAPER_RUNTIME_UNAVAILABLE"
    else:
        overall_state = str(operability.get("state") or startup.get("overall_state") or "UNUSABLE").upper()
        if overall_state == "USABLE":
            overall_state = "UNUSABLE"
    primary_next_action = (
        str(operability.get("primary_next_action") or "").strip()
        or str(startup.get("primary_next_action_label") or "").strip()
        or backend_dep.get("next_action")
        or "Refresh"
    )

    contract = {
        "generated_at": generated_at,
        "mode": "HEADLESS_SUPERVISED_PAPER",
        "paper_only": True,
        "production_host_model": "backend_operator_service_plus_supervised_paper_runtime",
        "app_usable_for_supervised_paper": usable,
        "overall_state": overall_state,
        "unusable_reason": None if usable else unusable_reason,
        "unusable_reason_code": None if usable else unusable_reason_code,
        "primary_next_action": primary_next_action,
        "operator_action_required": not usable,
        "summary_line": (
            "Headless supervised paper host is usable."
            if usable
            else f"Headless supervised paper host is not usable: {unusable_reason}"
        ),
        "backend": {
            "attached": backend_attached,
            "health_ready": health_ready,
            "startup_ready": startup_ready,
            "launch_allowed": launch_allowed,
            "dashboard_api_url": str(dashboard.get("dashboard_api_url") or "").strip() or None,
            "health_url": str(dashboard.get("health_url") or "").strip() or None,
            "pid": dashboard.get("pid"),
            "instance_id": dashboard.get("instance_id"),
            "status": str(health.get("status") or "").strip() or None,
            "phase": str(health.get("phase") or "").strip() or None,
            "phase_detail": str(health.get("phase_detail") or "").strip() or None,
            "dependency": backend_dep,
        },
        "auth": {
            "usable": auth_dep["state"] == "READY",
            "dependency": auth_dep,
        },
        "paper_runtime": {
            "running": runtime_running,
            "ready": paper_runtime_ready,
            "usable": usable and paper_runtime_ready,
            "phase": str(operability.get("paper_runtime_phase") or "").strip() or None,
            "entries_enabled": bool(operability.get("entries_enabled")),
            "operator_halt": bool(operability.get("operator_halt")),
            "usable_lane_count": int(operability.get("usable_lane_count") or 0),
            "eligible_to_trade_count": int(operability.get("eligible_to_trade_count") or 0),
            "halted_lane_count": int(operability.get("halted_lane_count") or 0),
            "dependency": paper_dep,
        },
        "reconciliation": {
            "needed": reconciliation_dep["state"] == "RECONCILIATION_REQUIRED",
            "dependency": reconciliation_dep,
        },
        "operator_ui": {
            "packaged_desktop_required_for_uptime": False,
            "optional_attach_path": "Packaged desktop app may attach to the already-running backend if it launches successfully.",
            "fallback_if_ui_unavailable": "Use this headless status contract plus dashboard health/readiness artifacts to supervise paper operation.",
        },
        "evidence": {
            "startup_control_plane_artifact": "/api/operator-artifact/startup-control-plane",
            "supervised_paper_operability_artifact": "/api/operator-artifact/supervised-paper-operability",
            "dashboard_info_file": str((Path("outputs/operator_dashboard/runtime/operator_dashboard.json")).resolve()),
        },
    }
    return contract


def render_headless_supervised_paper_markdown(contract: dict[str, Any]) -> str:
    lines = [
        "# Headless Supervised Paper Status",
        "",
        f"- Generated: `{contract.get('generated_at')}`",
        f"- Mode: `{contract.get('mode')}`",
        f"- Usable: `{contract.get('app_usable_for_supervised_paper')}`",
        f"- Overall State: `{contract.get('overall_state')}`",
        f"- Summary: {contract.get('summary_line')}",
        f"- Primary Next Action: `{contract.get('primary_next_action')}`",
        "",
        "## Backend",
        f"- Attached: `{contract.get('backend', {}).get('attached')}`",
        f"- Health Ready: `{contract.get('backend', {}).get('health_ready')}`",
        f"- Startup Ready: `{contract.get('backend', {}).get('startup_ready')}`",
        f"- Launch Allowed: `{contract.get('backend', {}).get('launch_allowed')}`",
        f"- Backend Reason: {contract.get('backend', {}).get('dependency', {}).get('reason') or 'n/a'}",
        "",
        "## Paper Runtime",
        f"- Running: `{contract.get('paper_runtime', {}).get('running')}`",
        f"- Ready: `{contract.get('paper_runtime', {}).get('ready')}`",
        f"- Usable: `{contract.get('paper_runtime', {}).get('usable')}`",
        f"- Phase: `{contract.get('paper_runtime', {}).get('phase')}`",
        f"- Paper Reason: {contract.get('paper_runtime', {}).get('dependency', {}).get('reason') or 'n/a'}",
        "",
        "## Auth / Reconciliation",
        f"- Auth Usable: `{contract.get('auth', {}).get('usable')}`",
        f"- Auth Reason: {contract.get('auth', {}).get('dependency', {}).get('reason') or 'n/a'}",
        f"- Reconciliation Needed: `{contract.get('reconciliation', {}).get('needed')}`",
        f"- Reconciliation Reason: {contract.get('reconciliation', {}).get('dependency', {}).get('reason') or 'n/a'}",
        "",
        "## Operator UI",
        "- Packaged desktop is optional; it is not required for uptime.",
        f"- Fallback: {contract.get('operator_ui', {}).get('fallback_if_ui_unavailable')}",
        "",
    ]
    return "\n".join(lines) + "\n"


def write_headless_supervised_paper_artifacts(
    *,
    contract: dict[str, Any],
    output_path: str | None = None,
    markdown_path: str | None = None,
) -> None:
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(contract, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if markdown_path:
        path = Path(markdown_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render_headless_supervised_paper_markdown(contract), encoding="utf-8")
