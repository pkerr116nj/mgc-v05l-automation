"""Shared metadata for Track B dashboard/operator projections.

Dashboard artifacts are display-only projections. They must never become
runtime, readiness, restart, broker, lifecycle, or routing authority.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable


EXECUTION_CORE_AUTHORITY = "execution_core_authority"
MISSING_SOURCE_AUTHORITY_PATH = "missing_source_authority_path"


def build_projection_metadata(
    *,
    source_authority_path: str | Path | None = None,
    source_authority_paths: Iterable[str | Path] | None = None,
    generated_from_control_plane_snapshot_id: str | None = None,
    control_plane_snapshot_required: bool = False,
    diagnostic_only: bool = False,
) -> dict[str, Any]:
    """Return standard non-authority metadata for dashboard/status projections."""

    paths = [str(path) for path in (source_authority_paths or []) if str(path)]
    source_path = str(source_authority_path) if source_authority_path is not None else None
    if source_path and not paths:
        paths = [source_path]
    metadata_complete = bool(source_path or paths)
    projection_degraded = not metadata_complete
    return {
        "projection_only": True,
        "not_routing_authority": True,
        "dashboard_projection_authority": False,
        "source_authority": EXECUTION_CORE_AUTHORITY,
        "source_authority_path": source_path,
        "source_authority_paths": paths,
        "authority_owner": "execution_core",
        "operator_dashboard_display_only": True,
        "generated_from_control_plane_snapshot_id": generated_from_control_plane_snapshot_id,
        "control_plane_snapshot_required": bool(control_plane_snapshot_required),
        "projection_metadata_complete": metadata_complete,
        "projection_degraded": projection_degraded,
        "diagnostic_only": bool(diagnostic_only or projection_degraded),
        "degraded_reason": MISSING_SOURCE_AUTHORITY_PATH if projection_degraded else None,
    }


__all__ = [
    "EXECUTION_CORE_AUTHORITY",
    "MISSING_SOURCE_AUTHORITY_PATH",
    "build_projection_metadata",
]
