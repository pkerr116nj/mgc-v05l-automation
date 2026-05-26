from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution_core.track_b_runtime_authority_resolver import (
    RUNTIME_AUTHORITY_DUPLICATE_WRITER,
    RUNTIME_AUTHORITY_GENERATION_MISMATCH,
    RUNTIME_AUTHORITY_MISSING,
    RUNTIME_AUTHORITY_STALE_LEGACY_IGNORED,
    RUNTIME_AUTHORITY_WRONG_ROOT,
    RuntimeAuthorityResolverConfig,
    resolve_track_b_runtime_authority,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_current_artifacts(
    root: Path,
    *,
    generation: str = "gen-test",
    loop_generation: str | None = "gen-test",
    control_plane_classification: str = "CONTROL_PLANE_SNAPSHOT_READY",
    safe_state_classification: str = "SAFE_STATE_NORMAL",
    duplicate_writer: bool = False,
) -> None:
    _write_json(
        root / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
        {
            "classification": control_plane_classification,
            "shared_truth_coherence_status": "COHERENT",
            "runtime_supervisor_classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
            "safe_state_runtime_generation_id": generation,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "agent_health_has_duplicate_writer": duplicate_writer,
        },
    )
    _write_json(
        root / "outputs/track_b_execution_core/safe_state/latest_runtime_safe_state_envelope.json",
        {
            "classification": safe_state_classification,
            "safe_state_classification": safe_state_classification,
            "runtime_generation_id": generation,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write_json(
        root / "outputs/track_b_execution_core/p0_observe_only/latest_p0_observe_only_loop.json",
        {
            "loop_mode": "guarded-paper",
            "runtime_generation_id": loop_generation or generation,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write_json(
        root / "outputs/track_b_execution_core/agent_health/latest_agent_health.json",
        {
            "classification": "AGENT_HEALTH_OK",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write_json(
        root / "outputs/operator_dashboard/runtime/latest_canonical_readiness.json",
        {"canonical_readiness": "NOT_READY_DEPENDENCY", "runtime_pid": 999999},
    )


def _process_rows(root: Path, *, count: int = 1, root_matches: bool = True) -> tuple[dict[str, object], ...]:
    rows = []
    for idx in range(count):
        rows.append(
            {
                "pid": 80000 + idx,
                "command": (
                    "python -m mgc_v05l.execution_core.track_b_p0_observe_only_loop "
                    f"--repo-root {root} --mode guarded-paper"
                ),
                "root_matches": root_matches,
                "wrong_root": not root_matches,
                "pid_active": True,
            }
        )
    return tuple(rows)


def test_stale_legacy_pid_ignored_when_current_guarded_loop_is_valid(tmp_path: Path) -> None:
    _write_current_artifacts(tmp_path)

    result = resolve_track_b_runtime_authority(
        RuntimeAuthorityResolverConfig(repo_root=tmp_path),
        legacy_operator_status={"source_runtime_pid": 999999},
        process_rows_provider=lambda root: _process_rows(root),
        pid_running=lambda pid: pid != 999999,
    )

    assert result["valid"] is True
    assert result["classification"] == RUNTIME_AUTHORITY_STALE_LEGACY_IGNORED
    assert result["runtime_pid"] == 80000
    assert result["legacy_stale_ignored"] is True
    assert "stale_legacy_runtime_pid_ignored" in result["diagnostics"]
    assert result["dashboard_projection_consumed"] is False


def test_stale_legacy_pid_blocks_when_no_current_authority_exists(tmp_path: Path) -> None:
    result = resolve_track_b_runtime_authority(
        RuntimeAuthorityResolverConfig(repo_root=tmp_path),
        legacy_operator_status={"source_runtime_pid": 999999},
        process_rows_provider=lambda _root: (),
        pid_running=lambda _pid: False,
    )

    assert result["valid"] is False
    assert result["classification"] == RUNTIME_AUTHORITY_MISSING
    assert "runtime_pid_not_active" in result["blockers"]
    assert "runtime_not_verified_from_dev_root" in result["blockers"]


def test_wrong_root_blocks_runtime_authority(tmp_path: Path) -> None:
    _write_current_artifacts(tmp_path)

    result = resolve_track_b_runtime_authority(
        RuntimeAuthorityResolverConfig(repo_root=tmp_path),
        process_rows_provider=lambda root: _process_rows(root, root_matches=False),
        pid_running=lambda _pid: True,
    )

    assert result["valid"] is False
    assert result["classification"] == RUNTIME_AUTHORITY_WRONG_ROOT
    assert "runtime_not_verified_from_dev_root" in result["blockers"]


def test_duplicate_writer_blocks_runtime_authority(tmp_path: Path) -> None:
    _write_current_artifacts(tmp_path)

    result = resolve_track_b_runtime_authority(
        RuntimeAuthorityResolverConfig(repo_root=tmp_path),
        process_rows_provider=lambda root: _process_rows(root, count=2),
        pid_running=lambda _pid: True,
    )

    assert result["valid"] is False
    assert result["classification"] == RUNTIME_AUTHORITY_DUPLICATE_WRITER
    assert "duplicate_runtime_submitters" in result["blockers"]


def test_generation_mismatch_blocks_runtime_authority(tmp_path: Path) -> None:
    _write_current_artifacts(tmp_path, generation="gen-current", loop_generation="gen-old")

    result = resolve_track_b_runtime_authority(
        RuntimeAuthorityResolverConfig(repo_root=tmp_path),
        process_rows_provider=lambda root: _process_rows(root),
        pid_running=lambda _pid: True,
    )

    assert result["valid"] is False
    assert result["classification"] == RUNTIME_AUTHORITY_GENERATION_MISMATCH
    assert "runtime_generation_mismatch" in result["blockers"]


def test_control_plane_block_blocks_runtime_authority(tmp_path: Path) -> None:
    _write_current_artifacts(tmp_path, control_plane_classification="CONTROL_PLANE_SNAPSHOT_BLOCKED")

    result = resolve_track_b_runtime_authority(
        RuntimeAuthorityResolverConfig(repo_root=tmp_path),
        process_rows_provider=lambda root: _process_rows(root),
        pid_running=lambda _pid: True,
    )

    assert result["valid"] is False
    assert "control_plane_not_ready" in result["blockers"]
