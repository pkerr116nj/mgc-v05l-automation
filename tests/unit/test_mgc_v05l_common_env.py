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
