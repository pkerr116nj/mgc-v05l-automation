from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from mgc_v05l.paths import (
    ALLOW_ARCHIVED_ROOT_FOR_TESTS_ENV,
    CONFIG_DIR,
    DOCS_DIR,
    OUTPUTS_DIR,
    PROJECT_ROOT,
    PROJECT_ROOT_ENV_VARS,
    ProjectRootError,
    path_from_root,
    resolve_project_root,
)


def _make_repo_root(path: Path) -> Path:
    (path / ".git").mkdir(parents=True)
    (path / "pyproject.toml").write_text("[project]\nname = 'test-root'\n", encoding="utf-8")
    (path / "src" / "mgc_v05l").mkdir(parents=True)
    return path


def _clean_env(**updates: str) -> dict[str, str]:
    env = dict(os.environ)
    for name in PROJECT_ROOT_ENV_VARS:
        env.pop(name, None)
    env.pop(ALLOW_ARCHIVED_ROOT_FOR_TESTS_ENV, None)
    env.update(updates)
    return env


def test_project_root_resolves_from_env_var(tmp_path: Path) -> None:
    expected = _make_repo_root(tmp_path / "relocatable")

    resolved = resolve_project_root(env=_clean_env(TRACK_B_PROJECT_ROOT=str(expected)))

    assert resolved == expected.resolve()


def test_project_root_resolves_from_repo_marker(tmp_path: Path) -> None:
    expected = _make_repo_root(tmp_path / "repo")
    nested = expected / "src" / "mgc_v05l" / "app"
    nested.mkdir(parents=True, exist_ok=True)

    resolved = resolve_project_root(anchor=nested / "module.py", env=_clean_env())

    assert resolved == expected.resolve()


def test_derived_paths_are_under_project_root() -> None:
    assert CONFIG_DIR == PROJECT_ROOT / "config"
    assert OUTPUTS_DIR == PROJECT_ROOT / "outputs"
    assert DOCS_DIR == PROJECT_ROOT / "docs"
    assert path_from_root("outputs", "reports") == PROJECT_ROOT / "outputs" / "reports"


def test_archived_documents_root_is_rejected() -> None:
    archived = Path("/Users/patrick/Documents/MGC-v05l-automation")

    with pytest.raises(ProjectRootError, match="Refusing archived"):
        resolve_project_root(env=_clean_env(TRACK_B_PROJECT_ROOT=str(archived)), require_markers=False)


def test_archived_do_not_use_documents_root_is_rejected() -> None:
    archived = Path("/Users/patrick/Documents/MGC-v05l-automation_ARCHIVED_DO_NOT_USE_20260519")

    with pytest.raises(ProjectRootError, match="Refusing archived"):
        resolve_project_root(env=_clean_env(TRACK_B_PROJECT_ROOT=str(archived)), require_markers=False)


def test_archived_documents_root_requires_explicit_test_override() -> None:
    archived = Path("/Users/patrick/Documents/MGC-v05l-automation")

    resolved = resolve_project_root(
        env=_clean_env(
            TRACK_B_PROJECT_ROOT=str(archived),
            MGC_V05L_ALLOW_ARCHIVED_ROOT_FOR_TESTS="1",
        ),
        require_markers=False,
    )

    assert resolved == archived.resolve()


def test_preflight_script_uses_repo_relative_root() -> None:
    script = PROJECT_ROOT / "scripts" / "track_b_paper_preflight.sh"
    text = script.read_text(encoding="utf-8")

    assert 'TRACK_B_PROJECT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.."' in text
    assert 'REPO_ROOT="/Users/patrick/Dev/MGC-v05l-automation"' not in text
    completed = subprocess.run(
        ["bash", str(script), "--help"],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0
    assert "Usage:" in completed.stderr
