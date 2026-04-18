from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.probationary_runtime import run_probationary_paper_soak_validation


def test_paper_soak_validation_proves_staged_participation_end_to_end(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    override = tmp_path / "paper_soak_override.yaml"
    override.write_text(
        "\n".join(
            [
                'mode: "paper"',
                f'database_url: "sqlite:///{tmp_path / "paper_soak.sqlite3"}"',
                f'probationary_artifacts_dir: "{artifacts_dir}"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = run_probationary_paper_soak_validation([Path("config/base.yaml"), override])

    payload = json.loads(Path(result.artifact_path).read_text(encoding="utf-8"))
    scenarios = {row["scenario_id"]: row for row in payload["scenarios"]}

    assert payload["summary"]["result"] == "PASS"
    assert scenarios["staged_same_direction_participation"]["status"] == "PASS"
    assert scenarios["staged_partial_exit_preserves_remaining_exposure"]["status"] == "PASS"
    assert scenarios["restart_restores_staged_position_without_duplicate_bar_processing"]["status"] == "PASS"

    staged_summary = scenarios["staged_same_direction_participation"]["summary"]["position_state"]
    assert staged_summary["open_entry_leg_count"] == 2
    assert staged_summary["open_add_count"] == 1
    assert staged_summary["additional_entry_allowed"] is False

    partial_exit_summary = scenarios["staged_partial_exit_preserves_remaining_exposure"]["summary"]["position_state"]
    assert partial_exit_summary["side"] == "LONG"
    assert partial_exit_summary["internal_qty"] == 1
    assert partial_exit_summary["open_entry_leg_count"] == 1

    restart_summary = scenarios["restart_restores_staged_position_without_duplicate_bar_processing"]["summary"]["position_state"]
    assert restart_summary["internal_qty"] == 3
    assert restart_summary["open_entry_leg_count"] == 3
