from __future__ import annotations

import subprocess
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_post_broker_mutation_refresh import (
    POST_BROKER_MUTATION_REFRESH_DEGRADED,
    POST_BROKER_MUTATION_REFRESH_SKIPPED,
    POST_BROKER_MUTATION_REFRESH_SUCCEEDED,
    PostBrokerMutationRefreshConfig,
    post_position_order_change_refresh,
)


NOW = datetime(2026, 6, 10, 10, 45, tzinfo=UTC)


def test_post_broker_mutation_refresh_runs_ods_after_current_scope_authority(tmp_path: Path) -> None:
    calls: list[str] = []

    def _runner(command, repo_root, timeout):
        calls.append(_module_name(command))
        return subprocess.CompletedProcess(list(command), 0, stdout='{"ok": true}', stderr="")

    payload = post_position_order_change_refresh(
        config=PostBrokerMutationRefreshConfig(repo_root=tmp_path, python_executable="python"),
        trigger="entry_fill_observed",
        mutation_report={"classification": "PAPER_STRATEGY_ORDER_FILLED", "broker_state_mutated": True},
        now=NOW,
        command_runner=_runner,
        write=False,
    )

    assert payload["classification"] == POST_BROKER_MUTATION_REFRESH_SUCCEEDED
    assert calls == [
        "mgc_v05l.app.ibkr_broker_truth_refresher",
        "mgc_v05l.execution_core.track_b_paper_broker_reconciliation",
        "mgc_v05l.execution_core.track_b_shared_truth_refresh_cli",
        "mgc_v05l.app.track_b_canonical_readiness",
        "mgc_v05l.execution_core.track_b_current_scope_state",
        "mgc_v05l.execution_core.track_b_control_plane_snapshot",
        "mgc_v05l.execution_core.track_b_operator_decision_surface",
    ]
    assert payload["steps"][-1]["name"] == "operator_decision_surface"
    assert payload["live_money_eligible"] is False
    assert payload["paper_proof_invoked"] is False
    assert payload["global_cancel_allowed"] is False


def test_control_plane_failure_is_diagnostic_when_other_refreshes_succeed(tmp_path: Path) -> None:
    def _runner(command, repo_root, timeout):
        module = _module_name(command)
        return subprocess.CompletedProcess(list(command), 2 if module.endswith("track_b_control_plane_snapshot") else 0)

    payload = post_position_order_change_refresh(
        config=PostBrokerMutationRefreshConfig(repo_root=tmp_path, python_executable="python"),
        trigger="close_fill_observed",
        now=NOW,
        command_runner=_runner,
        write=False,
    )

    assert payload["classification"] == POST_BROKER_MUTATION_REFRESH_SUCCEEDED
    assert payload["diagnostic_failure_count"] == 1
    assert payload["required_failure_count"] == 0


def test_required_refresh_timeout_is_degraded_but_does_not_raise(tmp_path: Path) -> None:
    def _runner(command, repo_root, timeout):
        module = _module_name(command)
        if module.endswith("track_b_shared_truth_refresh_cli"):
            raise subprocess.TimeoutExpired(command, timeout, output="", stderr="hung")
        return subprocess.CompletedProcess(list(command), 0)

    payload = post_position_order_change_refresh(
        config=PostBrokerMutationRefreshConfig(repo_root=tmp_path, python_executable="python", timeout_seconds=1.0),
        trigger="managed_close_fill_observed",
        now=NOW,
        command_runner=_runner,
        write=False,
    )

    assert payload["classification"] == POST_BROKER_MUTATION_REFRESH_DEGRADED
    assert payload["required_failure_count"] == 1
    assert payload["first_failing_step"]["name"] == "shared_truth"
    assert payload["first_failing_step"]["returncode"] == 124


def test_non_paper_or_wrong_account_refresh_is_skipped(tmp_path: Path) -> None:
    payload = post_position_order_change_refresh(
        config=PostBrokerMutationRefreshConfig(repo_root=tmp_path, account_id="LIVE123"),
        trigger="entry_fill_observed",
        now=NOW,
        command_runner=lambda command, repo_root, timeout: (_ for _ in ()).throw(AssertionError("no commands")),
        write=False,
    )

    assert payload["classification"] == POST_BROKER_MUTATION_REFRESH_SKIPPED
    assert payload["safety_blockers"] == ["wrong_paper_account"]
    assert payload["step_count"] == 0


def _module_name(command) -> str:
    command = list(command)
    return command[command.index("-m") + 1]
