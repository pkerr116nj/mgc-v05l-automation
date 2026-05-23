from __future__ import annotations

import json
import os
import signal
import subprocess
from datetime import UTC, datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "run_probationary_paper_soak.sh"
OVERLAY = REPO_ROOT / "config" / "probationary_pattern_engine_paper_mnq_mgc_plus_mnq_us_intraday_review.yaml"
NOW = datetime(2026, 5, 22, 18, 0, tzinfo=UTC)


def _base_env(tmp_path: Path, fake_python: Path, *, broker_exposure: bool = False) -> dict[str, str]:
    shared_truth_root = tmp_path / "shared_truth"
    _seed_shared_truth_preflight_root(shared_truth_root, broker_exposure=broker_exposure)
    env = os.environ.copy()
    env.update(
        {
            "MGC_PROBATIONARY_PAPER_RUNTIME_DIR": str(tmp_path / "runtime"),
            "MGC_PROBATIONARY_PAPER_LAUNCH_PYTHON_BIN": str(fake_python),
            "MGC_PROBATIONARY_PAPER_BACKGROUND_VERIFY_ATTEMPTS": "80",
            "MGC_PROBATIONARY_PAPER_BACKGROUND_VERIFY_POLL_SECONDS": "0.05",
            "MGC_PROBATIONARY_PAPER_BACKGROUND_OBSERVATION_WINDOW_SECONDS": "0",
            "REQUIRE_SCHWAB_AUTH": "false",
            "MARKET_DATA_PRIMARY": "databento",
            "BROKER_TRUTH_PROVIDER": "ibkr",
            "EXECUTION_PROVIDER": "ibkr",
            "MGC_TRACK_B_SHARED_TRUTH_PREFLIGHT_REPO_ROOT": str(shared_truth_root),
        }
    )
    return env


def _command(tmp_path: Path) -> list[str]:
    runtime_dir = tmp_path / "runtime"
    return [
        "bash",
        str(SCRIPT),
        "--background",
        "--pid-file",
        str(runtime_dir / "probationary_paper.pid"),
        "--log-file",
        str(runtime_dir / "probationary_paper.log"),
        "--config-paths-file",
        str(runtime_dir / "paper_runtime_config_paths.txt"),
        "--launch-status-file",
        str(runtime_dir / "probationary_paper_launch_status.json"),
        "--config",
        str(REPO_ROOT / "config" / "base.yaml"),
        "--config",
        str(REPO_ROOT / "config" / "live.yaml"),
        "--config",
        str(REPO_ROOT / "config" / "probationary_pattern_engine.yaml"),
        "--config",
        str(REPO_ROOT / "config" / "headless_supervised_paper_runtime.yaml"),
        "--config",
        str(REPO_ROOT / "config" / "probationary_pattern_engine_paper.yaml"),
        "--config",
        str(OVERLAY),
        "--schwab-config",
        str(REPO_ROOT / "config" / "schwab.local.json"),
    ]


def _write_fake_python(path: Path, body: str) -> None:
    path.write_text("#!/usr/bin/env bash\n" + body, encoding="utf-8")
    path.chmod(0o755)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _seed_shared_truth_preflight_root(root: Path, *, broker_exposure: bool = False) -> None:
    now = datetime.now(UTC)
    positions = (
        [{"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "1.0", "account": "DUM882026"}]
        if broker_exposure
        else []
    )
    broker_reconciled = not broker_exposure
    _write_json(
        root / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        {
            "generated_at": now.isoformat(),
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED"
            if broker_reconciled
            else "BROKER_TRUTH_SETTLEMENT_TIMEOUT",
            "broker_reconciled": broker_reconciled,
            "live_money_eligible": False,
            "track_b_broker_positions": positions,
            "track_b_broker_open_orders": [],
            "track_b_lifecycle_positions": [],
            "unknown_broker_open_orders": [],
            "known_managed_exit_orders": [],
            "unresolved_submit_intent_ownership_records": [],
            "track_b_broker_position_count": len(positions),
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "review_required_count": 0,
            "unresolved_submit_intent_ownership_count": 0,
            "lifecycle_open_position_count": 0,
            "lifecycle_open_order_count": 0,
            "position_match_report": {
                "state": "BROKER_AND_LIFECYCLE_FLAT"
                if broker_reconciled
                else "BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH",
                "matched": broker_reconciled,
            },
            "blockers": [] if broker_reconciled else [{"code": "broker_position_without_lifecycle"}],
        },
    )
    broker_status = {
        "classification": "BROKER_TRUTH_REFRESH_READY",
        "account": "DUM882026",
        "generated_at": now.isoformat(),
        "positions_complete": True,
        "open_orders_complete": True,
        "positions": positions,
        "open_orders": [],
        "live_money_eligible": False,
    }
    _write_json(
        root / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_refresh_status.json",
        {**broker_status, "last_successful_broker_truth": broker_status, "latest_attempt_status": broker_status},
    )
    _write_json(root / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_latest_attempt_status.json", broker_status)
    _write_json(
        root / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json",
        {
            "generated_at": now.isoformat(),
            "open_position_count": 0,
            "open_order_count": 0,
            "open_positions": [],
            "review_required_positions": [],
            "live_money_eligible": False,
        },
    )
    _write_json(
        root / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_paper_trade_summary.json",
        {
            "generated_at": now.isoformat(),
            "review_required_count": 0,
            "unknown_open_order_count": 0,
            "unresolved_intent_count": 0,
            "live_money_eligible": False,
        },
    )


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _truth_write_snippet(generated_at_expr: str = "$(date -u +%Y-%m-%dT%H:%M:%SZ)", extra: str = "") -> str:
    return f"""
runtime_dir="${{MGC_PROBATIONARY_PAPER_RUNTIME_DIR}}"
mkdir -p "${{runtime_dir}}"
generated_at="{generated_at_expr}"
cat > "${{runtime_dir}}/paper_runtime_truth.json" <<EOF
{{
  "producer_pid": $$,
  "generated_at": "${{generated_at}}",
  "heartbeat_state": "HEALTHY",
  "freshness_state": "FRESH",
  "writer_authority": "SINGLE_WRITER",
  "runtime_mode": "PAPER",
  "live_money_eligible": false{extra}
}}
EOF
"""


def test_background_launch_writes_pid_only_after_child_survives(tmp_path: Path) -> None:
    fake_python = tmp_path / "fake_python"
    _write_fake_python(
        fake_python,
        _truth_write_snippet()
        + """
sleep 1.1
"""
        + _truth_write_snippet()
        + """
sleep 30
""",
    )

    result = subprocess.run(
        _command(tmp_path),
        cwd=REPO_ROOT,
        env=_base_env(tmp_path, fake_python),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    pid = None
    try:
        assert result.returncode == 0, result.stderr + result.stdout
        pid_file = tmp_path / "runtime" / "probationary_paper.pid"
        status_file = tmp_path / "runtime" / "probationary_paper_launch_status.json"
        config_paths_file = tmp_path / "runtime" / "paper_runtime_config_paths.txt"
        pid = int(pid_file.read_text(encoding="utf-8").strip())
        assert _pid_is_alive(pid)
        status = json.loads(status_file.read_text(encoding="utf-8"))
        assert status["classification"] == "PROBATIONARY_PAPER_BACKGROUND_STARTED"
        assert status["pid"] == pid
        assert status["sustained_convergence_confirmed"] is True
        assert status["final_pid_alive"] is True
        assert status["first_truth_generated_at"]
        assert status["second_truth_generated_at"]
        assert status["second_truth_generated_at"] != status["first_truth_generated_at"]
        assert status["repo_root"] == str(REPO_ROOT)
        assert status["cwd"] == str(REPO_ROOT)
        assert status["paper_only"] is True
        assert status["live_money_eligible"] is False
        assert status["paper_proof_invoked"] is False
        assert status["submit_authority"] is False
        assert status["python_bin"] == str(fake_python)
        assert str(OVERLAY) in status["config_paths"]
        assert str(OVERLAY) in config_paths_file.read_text(encoding="utf-8")
        shared_truth_preflight = json.loads(
            (tmp_path / "runtime" / "shared_truth_runtime_start_preflight.json").read_text(encoding="utf-8")
        )
        assert shared_truth_preflight["runtime_start_preflight"]["classification"] == "SHARED_TRUTH_PREFLIGHT_CLEAN"
    finally:
        if pid is not None and _pid_is_alive(pid):
            os.kill(pid, signal.SIGTERM)


def test_background_launch_blocks_before_runtime_when_shared_truth_not_clean(tmp_path: Path) -> None:
    fake_python = tmp_path / "fake_python"
    marker = tmp_path / "fake_python_invoked"
    _write_fake_python(fake_python, f"touch {marker}\nsleep 30\n")

    result = subprocess.run(
        _command(tmp_path),
        cwd=REPO_ROOT,
        env=_base_env(tmp_path, fake_python, broker_exposure=True),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    status_file = tmp_path / "runtime" / "probationary_paper_launch_status.json"
    assert result.returncode != 0
    assert not marker.exists()
    status = json.loads(status_file.read_text(encoding="utf-8"))
    assert status["classification"] == "SHARED_TRUTH_PREFLIGHT_BLOCKED"
    assert "Position Truth is ATTENTION_REQUIRED" in status["detail"]
    shared_truth_preflight = json.loads(
        (tmp_path / "runtime" / "shared_truth_runtime_start_preflight.json").read_text(encoding="utf-8")
    )
    assert shared_truth_preflight["runtime_start_preflight"]["classification"] == "SHARED_TRUTH_PREFLIGHT_BLOCKED"


def test_background_launch_rejects_alive_child_without_runtime_truth(tmp_path: Path) -> None:
    fake_python = tmp_path / "fake_python"
    _write_fake_python(fake_python, "sleep 30\n")

    result = subprocess.run(
        _command(tmp_path),
        cwd=REPO_ROOT,
        env=_base_env(tmp_path, fake_python),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    pid_file = tmp_path / "runtime" / "probationary_paper.pid"
    status_file = tmp_path / "runtime" / "probationary_paper_launch_status.json"
    assert result.returncode != 0
    assert not pid_file.exists()
    status = json.loads(status_file.read_text(encoding="utf-8"))
    assert status["classification"] == "LAUNCH_VERIFIER_STOPPED_CHILD_AFTER_TIMEOUT"
    assert status["sustained_convergence_confirmed"] is False
    assert status["final_pid_alive"] is True
    assert status["terminated_by_launch_verifier"] is True
    assert status["termination_signal"] == "TERM"
    assert status["termination_reason"] == "launch_verifier_timeout_before_sustained_runtime_truth"
    assert status["stop_provenance"]["stop_source"] == "launcher"
    assert status["stop_provenance"]["stop_reason"] == "launch_verifier_timeout_before_sustained_runtime_truth"
    assert status["stop_provenance"]["expected_cleanup"] is True
    assert status["stop_provenance"]["broker_safe_at_stop"] is False
    assert status["live_money_eligible"] is False
    assert status["submit_authority"] is False


def test_background_launch_rejects_non_advancing_runtime_truth(tmp_path: Path) -> None:
    fake_python = tmp_path / "fake_python"
    _write_fake_python(fake_python, _truth_write_snippet() + "sleep 30\n")

    result = subprocess.run(
        _command(tmp_path),
        cwd=REPO_ROOT,
        env=_base_env(tmp_path, fake_python),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    pid_file = tmp_path / "runtime" / "probationary_paper.pid"
    status_file = tmp_path / "runtime" / "probationary_paper_launch_status.json"
    assert result.returncode != 0
    assert not pid_file.exists()
    status = json.loads(status_file.read_text(encoding="utf-8"))
    assert status["classification"] == "LAUNCH_VERIFIER_STOPPED_CHILD_AFTER_TIMEOUT"
    assert status["first_truth_generated_at"]
    assert status["second_truth_generated_at"] is None
    assert status["sustained_convergence_confirmed"] is False
    assert status["final_pid_alive"] is True
    assert status["terminated_by_launch_verifier"] is True
    assert status["termination_signal"] == "TERM"


def test_background_launch_rejects_duplicate_writer_truth(tmp_path: Path) -> None:
    fake_python = tmp_path / "fake_python"
    _write_fake_python(
        fake_python,
        _truth_write_snippet(
            "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
            ',\n  "duplicate_writer_detection": {"duplicate_writer_detected": true}',
        )
        + """
sleep 1.1
"""
        + _truth_write_snippet(
            "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
            ',\n  "duplicate_writer_detection": {"duplicate_writer_detected": true}',
        )
        + "sleep 30\n",
    )

    result = subprocess.run(
        _command(tmp_path),
        cwd=REPO_ROOT,
        env=_base_env(tmp_path, fake_python),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    pid_file = tmp_path / "runtime" / "probationary_paper.pid"
    status_file = tmp_path / "runtime" / "probationary_paper_launch_status.json"
    assert result.returncode != 0
    assert not pid_file.exists()
    status = json.loads(status_file.read_text(encoding="utf-8"))
    assert status["classification"] == "LAUNCH_VERIFIER_STOPPED_CHILD_AFTER_TIMEOUT"
    assert status["sustained_convergence_confirmed"] is False
    assert status["terminated_by_launch_verifier"] is True


def test_background_launch_classifies_child_exit_after_preflight(tmp_path: Path) -> None:
    fake_python = tmp_path / "fake_python"
    _write_fake_python(
        fake_python,
        """
echo "Probationary paper runtime artifact preflight: {\\\"pid\\\": $$, \\\"runtime_ready\\\": true}"
exit 0
""",
    )

    result = subprocess.run(
        _command(tmp_path),
        cwd=REPO_ROOT,
        env=_base_env(tmp_path, fake_python),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    pid_file = tmp_path / "runtime" / "probationary_paper.pid"
    status_file = tmp_path / "runtime" / "probationary_paper_launch_status.json"
    assert result.returncode != 0
    assert not pid_file.exists()
    status = json.loads(status_file.read_text(encoding="utf-8"))
    assert status["classification"] == "RUNTIME_EXITED_AFTER_PREFLIGHT"
    assert status["live_money_eligible"] is False


def test_background_launch_classifies_exit_after_initial_truth(tmp_path: Path) -> None:
    fake_python = tmp_path / "fake_python"
    _write_fake_python(
        fake_python,
        _truth_write_snippet()
        + """
sleep 0.2
exit 0
""",
    )

    result = subprocess.run(
        _command(tmp_path),
        cwd=REPO_ROOT,
        env=_base_env(tmp_path, fake_python),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    pid_file = tmp_path / "runtime" / "probationary_paper.pid"
    status_file = tmp_path / "runtime" / "probationary_paper_launch_status.json"
    assert result.returncode != 0
    assert not pid_file.exists()
    status = json.loads(status_file.read_text(encoding="utf-8"))
    assert status["classification"] == "RUNTIME_EXITED_AFTER_INITIAL_TRUTH"
    assert status["first_truth_generated_at"]
    assert status["second_truth_generated_at"] is None
    assert status["final_pid_alive"] is False
    assert status["stop_provenance"]["stop_source"] == "runtime_internal"
    assert status["stop_provenance"]["expected_cleanup"] is False


def test_background_launch_reports_failed_child_without_stale_pid(tmp_path: Path) -> None:
    fake_python = tmp_path / "fake_python"
    _write_fake_python(fake_python, 'echo "fake launch failed"\nexit 42\n')

    result = subprocess.run(
        _command(tmp_path),
        cwd=REPO_ROOT,
        env=_base_env(tmp_path, fake_python),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    pid_file = tmp_path / "runtime" / "probationary_paper.pid"
    status_file = tmp_path / "runtime" / "probationary_paper_launch_status.json"
    log_file = tmp_path / "runtime" / "probationary_paper.log"
    assert result.returncode != 0
    assert not pid_file.exists()
    status = json.loads(status_file.read_text(encoding="utf-8"))
    assert status["classification"] == "PROBATIONARY_PAPER_BACKGROUND_START_FAILED"
    assert status["child_exit_code"] == 42
    assert status["live_money_eligible"] is False
    assert status["submit_authority"] is False
    assert "fake launch failed" in log_file.read_text(encoding="utf-8")
