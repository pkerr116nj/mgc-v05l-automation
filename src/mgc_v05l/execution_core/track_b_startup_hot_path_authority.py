"""Bounded Track B PAPER startup authority.

This artifact is the runtime startup hot path. It intentionally consumes only
current-state PAPER evidence and does not rebuild shared truth, proof readiness,
or the control-plane snapshot.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_paper_minimal_startup import (
    REPO_ROOT,
    TrackBPaperMinimalStartupConfig,
    build_track_b_paper_minimal_startup,
)


DEFAULT_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "startup_hot_path_authority"
    / "latest_startup_hot_path_authority.json"
)


@dataclass(frozen=True)
class TrackBStartupHotPathAuthorityConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_OUTPUT_PATH
    minimal_startup_output_path: Path = TrackBPaperMinimalStartupConfig.output_path

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_startup_hot_path_authority(
    *,
    config: TrackBStartupHotPathAuthorityConfig | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    config = config or TrackBStartupHotPathAuthorityConfig()
    actual_now = _ensure_utc(now or datetime.now(UTC))
    started_at = time.perf_counter()
    substages: list[dict[str, Any]] = []

    minimal_config = TrackBPaperMinimalStartupConfig(
        repo_root=config.repo_root,
        output_path=config.minimal_startup_output_path,
    )
    minimal = _timed_substage(
        substages,
        "bounded_minimal_startup_authority",
        lambda: build_track_b_paper_minimal_startup(config=minimal_config, now=actual_now),
    )
    classification = (
        "STARTUP_HOT_PATH_AUTHORITY_ALLOWED"
        if minimal.get("allowed") is True
        else "STARTUP_HOT_PATH_AUTHORITY_BLOCKED"
    )
    return {
        "schema_version": "track_b_startup_hot_path_authority_v1",
        "generated_at": actual_now.isoformat(),
        "classification": classification,
        "allowed": minimal.get("allowed") is True,
        "read_only": True,
        "broker_state_mutated": False,
        "submit_attempted": False,
        "cancel_attempted": False,
        "close_attempted": False,
        "bounded_current_state_only": True,
        "full_shared_truth_refresh_invoked": False,
        "control_plane_snapshot_build_invoked": False,
        "proof_readiness_refresh_invoked": False,
        "historical_manifest_scan_invoked": False,
        "lifecycle_report_tree_scan_invoked": False,
        "startup_minimal_classification": minimal.get("classification"),
        "startup_minimal_allowed": minimal.get("allowed") is True,
        "broker_position_count": minimal.get("broker_position_count"),
        "broker_open_order_count": minimal.get("broker_open_order_count"),
        "unknown_open_order_count": minimal.get("unknown_open_order_count"),
        "configured_instruments": list(minimal.get("configured_instruments") or []),
        "profile_overlay": minimal.get("profile_overlay"),
        "blockers": list(minimal.get("blockers") or []),
        "warnings": list(minimal.get("warnings") or []),
        "diagnostics_only_categories": list(minimal.get("diagnostics_only_categories") or []),
        "source_artifact_refs": {
            "startup_hot_path_authority": str(config.resolve(config.output_path)),
            "paper_minimal_startup": str(minimal_config.resolve(minimal_config.output_path)),
            **dict(minimal.get("source_artifact_refs") or {}),
        },
        "substage_durations": substages,
        "slowest_substage": _slowest_substage(substages),
        "duration_seconds": round(max(time.perf_counter() - started_at, 0.0), 6),
    }


def write_track_b_startup_hot_path_authority(
    *,
    config: TrackBStartupHotPathAuthorityConfig | None = None,
    payload: dict[str, Any],
) -> Path:
    config = config or TrackBStartupHotPathAuthorityConfig()
    path = config.resolve(config.output_path)
    write_json_atomic(path, payload)
    return path


def _timed_substage(
    substages: list[dict[str, Any]],
    name: str,
    fn: Any,
) -> Any:
    started_at = time.perf_counter()
    try:
        return fn()
    finally:
        substages.append(
            {
                "substage": name,
                "duration_seconds": round(max(time.perf_counter() - started_at, 0.0), 6),
                "current_hot_path_required": True,
                "bounded_current_state_only": True,
                "scans_historical_artifacts": False,
                "invokes_shared_truth_refresh": False,
                "invokes_control_plane_snapshot": False,
                "invokes_proof_readiness": False,
                "touches_broker_tws": False,
                "broker_state_mutated": False,
            }
        )


def _slowest_substage(substages: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not substages:
        return None
    return max(substages, key=lambda row: float(row.get("duration_seconds") or 0.0))


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    config = TrackBStartupHotPathAuthorityConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output) if args.output else DEFAULT_OUTPUT_PATH,
    )
    payload = build_track_b_startup_hot_path_authority(config=config)
    path = write_track_b_startup_hot_path_authority(config=config, payload=payload)
    print(json.dumps({"classification": payload.get("classification"), "allowed": payload.get("allowed"), "output_path": str(path)}))
    return 0 if payload.get("allowed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
