"""CLI integration coverage for replay research utilities."""

import json
from pathlib import Path

from mgc_v05l.app.main import main


def test_research_causal_report_cli_writes_output(tmp_path: Path, capsys) -> None:
    base_config = tmp_path / "base.yaml"
    replay_config = tmp_path / "replay.yaml"
    report_csv = tmp_path / "report.csv"
    replay_db = tmp_path / "cli.sqlite3"
    base_config.write_text(Path("config/base.yaml").read_text(encoding="utf-8"), encoding="utf-8")
    replay_config.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{replay_db}"\n',
        encoding="utf-8",
    )
    replay_csv = tmp_path / "replay.csv"
    replay_csv.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2026-03-13T18:00:00-04:00,100,101,99,100,100\n"
        "2026-03-13T18:05:00-04:00,100,103,99,102,100\n"
        "2026-03-13T18:10:00-04:00,102,104,101,103,100\n",
        encoding="utf-8",
    )

    exit_code = main(
        [
            "research-causal-report",
            "--config",
            str(base_config),
            "--config",
            str(replay_config),
            "--csv",
            str(replay_csv),
            "--output",
            str(report_csv),
        ]
    )
    stdout = capsys.readouterr().out
    payload = json.loads(stdout)

    assert exit_code == 0
    assert payload["rows"] == 3
    assert payload["research_only"] is True
    assert report_csv.exists()


def test_probationary_operator_control_cli_queues_resume_entries(tmp_path: Path, capsys) -> None:
    control_path = tmp_path / "paper_artifacts" / "operator_control.json"
    override_config = tmp_path / "override.yaml"
    override_config.write_text(
        (
            f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"\n'
            f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"\n'
            "probationary_paper_runtime_exclusive_config: true\n"
            f'probationary_operator_control_path: "{control_path}"\n'
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "probationary-operator-control",
            "--config",
            "config/base.yaml",
            "--config",
            "config/live.yaml",
            "--config",
            "config/probationary_pattern_engine.yaml",
            "--config",
            "config/probationary_pattern_engine_paper.yaml",
            "--config",
            str(override_config),
            "--action",
            "resume_entries",
        ]
    )
    stdout = capsys.readouterr().out
    payload = json.loads(stdout)
    control_payload = json.loads(control_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert payload["action"] == "resume_entries"
    assert payload["status"] == "pending"
    assert payload["control_path"] == str(control_path)
    assert control_payload["action"] == "resume_entries"
    assert control_payload["status"] == "pending"
