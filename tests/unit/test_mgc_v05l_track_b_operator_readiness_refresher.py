from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Sequence

from mgc_v05l.app.track_b_operator_readiness_refresher import (
    RefreshConfig,
    _refresh_commands,
    read_status,
    refresh_once,
)


def test_refresh_commands_are_read_only_and_cover_dashboard_artifacts(tmp_path: Path) -> None:
    commands = _refresh_commands(repo_root=tmp_path, preflight_mode="monday-live")
    names = [name for name, _command in commands]
    flattened = " ".join(part for _name, command in commands for part in command)

    assert names == [
        "phase1_runtime_data_readiness",
        "phase1_ticker_readiness_matrix",
        "track_b_paper_preflight",
    ]
    assert "--mode monday-live" in flattened
    assert "phase1_runtime_data_readiness" in flattened
    assert "phase1_ticker_readiness_matrix" in flattened
    assert "track_b_paper_preflight.sh" in flattened
    assert "placeOrder" not in flattened
    assert "cancelOrder" not in flattened
    assert "reqGlobalCancel" not in flattened
    assert "paper_proof" not in flattened


def test_refresh_once_writes_status_and_keeps_submit_authority_false(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def fake_runner(command: Sequence[str], _repo_root: Path, _timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(list(command))
        return subprocess.CompletedProcess(list(command), 0, stdout="ok", stderr="")

    status_path = tmp_path / "latest_status.json"
    payload = refresh_once(
        config=RefreshConfig(repo_root=tmp_path, status_path=status_path),
        runner=fake_runner,
    )

    assert len(calls) == 3
    assert payload["classification"] == "TRACK_B_OPERATOR_READINESS_REFRESH_READY"
    assert payload["last_success"] is True
    assert payload["submit_authority"] is False
    assert payload["paper_proof_invoked"] is False
    assert payload["live_money_eligible"] is False
    assert "latest_track_b_paper_preflight.json" in payload["refreshed_artifacts"]["track_b_paper_preflight"]
    assert json.loads(status_path.read_text(encoding="utf-8"))["classification"] == payload["classification"]


def test_refresh_once_fails_closed_when_a_refresh_command_fails(tmp_path: Path) -> None:
    def fake_runner(command: Sequence[str], _repo_root: Path, _timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        name = " ".join(command)
        return subprocess.CompletedProcess(list(command), 1 if "track_b_paper_preflight" in name else 0, stdout="", stderr="blocked")

    payload = refresh_once(
        config=RefreshConfig(repo_root=tmp_path, status_path=tmp_path / "status.json"),
        runner=fake_runner,
    )

    assert payload["classification"] == "TRACK_B_OPERATOR_READINESS_REFRESH_FAILED"
    assert payload["last_success"] is False
    assert payload["last_success_at"] is None
    assert payload["submit_authority"] is False
    assert payload["commands"][-1]["returncode"] == 1


def test_missing_status_is_safe_and_non_authoritative(tmp_path: Path) -> None:
    payload = read_status(status_path=tmp_path / "missing.json")

    assert payload["classification"] == "TRACK_B_OPERATOR_READINESS_REFRESH_STATUS_MISSING"
    assert payload["refresh_running"] is False
    assert payload["submit_authority"] is False
    assert payload["paper_proof_invoked"] is False
    assert payload["live_money_eligible"] is False
