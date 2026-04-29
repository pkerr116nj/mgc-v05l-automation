from __future__ import annotations

import os
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_common_env_loads_app_support_schwab_env_when_repo_local_file_is_missing(tmp_path: Path) -> None:
    home = tmp_path / "home"
    runtime_root = home / "Library" / "Application Support" / "mgc_v05l" / "research_daily_capture_runtime"
    runtime_root.mkdir(parents=True, exist_ok=True)
    token_path = runtime_root / "tokens.json"
    token_path.write_text("{}", encoding="utf-8")
    env_path = runtime_root / "schwab_env.sh"
    env_path.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "export SCHWAB_APP_KEY='app-key'",
                "export SCHWAB_APP_SECRET='app-secret'",
                "export SCHWAB_CALLBACK_URL='https://127.0.0.1:8182/callback'",
                f"export SCHWAB_TOKEN_FILE='{token_path}'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["HOME"] = str(home)
    env.pop("SCHWAB_APP_KEY", None)
    env.pop("SCHWAB_APP_SECRET", None)
    env.pop("SCHWAB_CALLBACK_URL", None)
    env.pop("SCHWAB_TOKEN_FILE", None)

    completed = subprocess.run(
        [
            "bash",
            "-lc",
            "source scripts/common_env.sh >/dev/null; "
            "printf '%s\\n' \"$MGC_BOOTSTRAP_SCHWAB_ENV_SOURCE_KIND|$MGC_BOOTSTRAP_SCHWAB_ENV_SOURCE_PATH|$SCHWAB_TOKEN_FILE|$MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_STATUS\"",
        ],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    source_kind, source_path, resolved_token_path, auth_status = completed.stdout.strip().split("|")
    assert source_kind == "app_support_research_runtime"
    assert source_path == str(env_path)
    assert resolved_token_path == str(token_path)
    assert auth_status == "ready"


def test_common_env_defaults_make_schwab_optional_for_ibkr_paper_runtime(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["HOME"] = str(home)
    env.pop("SCHWAB_APP_KEY", None)
    env.pop("SCHWAB_APP_SECRET", None)
    env.pop("SCHWAB_CALLBACK_URL", None)
    env.pop("SCHWAB_TOKEN_FILE", None)
    env.pop("MARKET_DATA_PRIMARY", None)
    env.pop("MARKET_DATA_FALLBACK", None)
    env.pop("BROKER_TRUTH_PROVIDER", None)
    env.pop("EXECUTION_PROVIDER", None)
    env.pop("ALLOW_SCHWAB_FALLBACK", None)
    env.pop("REQUIRE_SCHWAB_AUTH", None)

    completed = subprocess.run(
        [
            "bash",
            "-lc",
            "source scripts/common_env.sh >/dev/null; "
            "printf '%s\\n' \"$MARKET_DATA_PRIMARY|$MARKET_DATA_FALLBACK|$BROKER_TRUTH_PROVIDER|$EXECUTION_PROVIDER|$ALLOW_SCHWAB_FALLBACK|$REQUIRE_SCHWAB_AUTH|$MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_STATUS|$MGC_OPERATOR_DASHBOARD_REDUCED_MODE\"",
        ],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    (
        market_data_primary,
        market_data_fallback,
        broker_truth_provider,
        execution_provider,
        allow_schwab_fallback,
        require_schwab_auth,
        auth_status,
        reduced_mode,
    ) = completed.stdout.strip().split("|")
    assert market_data_primary == "databento"
    assert market_data_fallback == "schwab"
    assert broker_truth_provider == "ibkr"
    assert execution_provider == "ibkr"
    assert allow_schwab_fallback == "true"
    assert require_schwab_auth == "false"
    assert auth_status == "fallback_unavailable"
    assert reduced_mode == "0"


def test_research_market_data_integrity_runner_sources_project_dotenv(tmp_path: Path) -> None:
    dotenv_path = REPO_ROOT / ".env"
    original_env = dotenv_path.read_text(encoding="utf-8") if dotenv_path.exists() else None
    stub_path = tmp_path / "stub_python.sh"
    stub_path.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "printf 'DATABENTO_API_KEY present: %s\\n' \"$( [[ -n \"${DATABENTO_API_KEY:-}\" ]] && echo True || echo False )\"",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    stub_path.chmod(0o755)

    try:
        dotenv_path.write_text('export DATABENTO_API_KEY=\"TEST_VALUE\"\\n', encoding="utf-8")
        env = os.environ.copy()
        env.pop("DATABENTO_API_KEY", None)
        env["RESEARCH_MARKET_DATA_INTEGRITY_PYTHON_BIN"] = str(stub_path)
        completed = subprocess.run(
            [
                "bash",
                "scripts/run_research_market_data_integrity.sh",
                "--mode",
                "audit",
                "--symbols",
                "GC",
                "--end",
                "2026-04-21T23:59:00-04:00",
            ],
            cwd=REPO_ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
    finally:
        if original_env is None:
            dotenv_path.unlink(missing_ok=True)
        else:
            dotenv_path.write_text(original_env, encoding="utf-8")

    assert completed.returncode == 0, completed.stderr
    assert "DATABENTO_API_KEY present: True" in completed.stdout


def test_research_market_data_integrity_runner_backfill_fails_loudly_without_key(tmp_path: Path) -> None:
    dotenv_path = REPO_ROOT / ".env"
    original_env = dotenv_path.read_text(encoding="utf-8") if dotenv_path.exists() else None
    stub_path = tmp_path / "stub_python.sh"
    stub_path.write_text("#!/usr/bin/env bash\nexit 99\n", encoding="utf-8")
    stub_path.chmod(0o755)

    try:
        dotenv_path.write_text("", encoding="utf-8")
        env = os.environ.copy()
        env.pop("DATABENTO_API_KEY", None)
        env["RESEARCH_MARKET_DATA_INTEGRITY_PYTHON_BIN"] = str(stub_path)
        completed = subprocess.run(
            [
                "bash",
                "scripts/run_research_market_data_integrity.sh",
                "--mode",
                "backfill",
                "--symbols",
                "GC",
                "--start",
                "2024-01-01T18:00:00-05:00",
                "--end",
                "2026-04-21T23:59:00-04:00",
            ],
            cwd=REPO_ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
    finally:
        if original_env is None:
            dotenv_path.unlink(missing_ok=True)
        else:
            dotenv_path.write_text(original_env, encoding="utf-8")

    assert completed.returncode == 1
    assert "requires DATABENTO_API_KEY" in completed.stderr
