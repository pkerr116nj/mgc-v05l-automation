from __future__ import annotations

from pathlib import Path

from mgc_v05l.app.track_b_self_healing_supervisor import render_track_b_self_healing_status, run_track_b_self_healing_status
from tests.unit.execution_core.test_track_b_self_healing_supervisor import NOW, _process_probe, _write_runtime_artifacts


def test_status_writer_writes_and_renders_summary(tmp_path: Path) -> None:
    _write_runtime_artifacts(tmp_path)
    output = tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_self_healing_health.json"

    health = run_track_b_self_healing_status(
        repo_root=tmp_path,
        expected_root=tmp_path,
        output_path=output,
        write=True,
        process_probe=_process_probe(tmp_path),
        now=NOW,
    )
    rendered = render_track_b_self_healing_status(health)

    assert output.exists()
    assert "classification=SELF_HEALING_READY" in rendered
    assert "broker_truth_refresher: health=HEALTHY" in rendered
    assert "auto_restart_allowed=false" in rendered
    assert health["generated_at"]
