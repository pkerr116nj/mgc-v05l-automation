"""Canonical project-root and path helpers for Track B runtime tooling."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

PROJECT_ROOT_ENV_VARS = ("TRACK_B_PROJECT_ROOT", "MGC_V05L_PROJECT_ROOT")
TEST_FALLBACK_ENV = "MGC_V05L_TEST_PROJECT_ROOT_FALLBACK"
ALLOW_ARCHIVED_ROOT_FOR_TESTS_ENV = "MGC_V05L_ALLOW_ARCHIVED_ROOT_FOR_TESTS"
REPO_MARKER_PATHS = (
    Path(".git"),
    Path("pyproject.toml"),
    Path("src") / "mgc_v05l",
)
ARCHIVED_ROOT_FRAGMENTS = (
    "Documents/MGC-v05l-automation",
    "Mobile Documents",
    "iCloud",
)


class ProjectRootError(RuntimeError):
    """Raised when a safe project root cannot be resolved."""


def resolve_project_root(
    *,
    anchor: Path | str | None = None,
    env: dict[str, str] | None = None,
    require_markers: bool = True,
) -> Path:
    """Resolve the canonical project root from env, repo markers, or test fallback."""

    environ = os.environ if env is None else env
    explicit = _explicit_project_root(environ)
    if explicit is not None:
        raw_root = explicit.expanduser()
        root = raw_root.resolve()
        _validate_project_root(
            root=root,
            require_markers=require_markers,
            environ=environ,
            raw_root=raw_root,
        )
        return root

    for candidate in _candidate_roots(anchor):
        if _has_repo_markers(candidate):
            root = candidate.resolve()
            _validate_project_root(
                root=root,
                require_markers=require_markers,
                environ=environ,
                raw_root=candidate,
            )
            return root

    fallback = str(environ.get(TEST_FALLBACK_ENV) or "").strip()
    if fallback:
        raw_root = Path(fallback).expanduser()
        root = raw_root.resolve()
        _validate_project_root(
            root=root,
            require_markers=False,
            environ=environ,
            raw_root=raw_root,
        )
        return root

    raise ProjectRootError("Could not resolve project root from env, repo markers, or test fallback.")


def path_from_root(*parts: str | os.PathLike[str], root: Path | str | None = None) -> Path:
    """Return an absolute path under the project root."""

    base = Path(root).expanduser().resolve() if root is not None else PROJECT_ROOT
    return base.joinpath(*map(Path, parts))


def _explicit_project_root(environ: dict[str, str]) -> Path | None:
    for name in PROJECT_ROOT_ENV_VARS:
        value = str(environ.get(name) or "").strip()
        if value:
            return Path(value)
    return None


def _candidate_roots(anchor: Path | str | None) -> Iterable[Path]:
    anchors: list[Path] = []
    if anchor is not None:
        anchors.append(Path(anchor))
    anchors.extend([Path.cwd(), Path(__file__)])
    seen: set[Path] = set()
    for raw_anchor in anchors:
        resolved = raw_anchor.expanduser().resolve()
        start = resolved if resolved.is_dir() else resolved.parent
        for candidate in (start, *start.parents):
            if candidate in seen:
                continue
            seen.add(candidate)
            yield candidate


def _has_repo_markers(path: Path) -> bool:
    return all(path.joinpath(marker).exists() for marker in REPO_MARKER_PATHS)


def is_archived_project_root(path: Path | str) -> bool:
    """Return whether a raw or resolved path references an archived Documents/iCloud root."""

    return _is_archived_root(Path(path)) or _is_archived_root(Path(path).expanduser())


def _validate_project_root(
    *,
    root: Path,
    require_markers: bool,
    environ: dict[str, str],
    raw_root: Path | None = None,
) -> None:
    path_for_archive_check = raw_root or root
    if (
        (is_archived_project_root(path_for_archive_check) or is_archived_project_root(root))
        and str(environ.get(ALLOW_ARCHIVED_ROOT_FOR_TESTS_ENV) or "").strip() != "1"
    ):
        raise ProjectRootError(f"Refusing archived Documents/iCloud project root: {path_for_archive_check}")
    if require_markers and not _has_repo_markers(root):
        markers = ", ".join(str(marker) for marker in REPO_MARKER_PATHS)
        raise ProjectRootError(f"Project root {root} is missing required markers: {markers}")


def _is_archived_root(path: Path) -> bool:
    normalized = str(path)
    return any(fragment in normalized for fragment in ARCHIVED_ROOT_FRAGMENTS)


PROJECT_ROOT = resolve_project_root(anchor=Path(__file__))
CONFIG_DIR = PROJECT_ROOT / "config"
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
VAR_DIR = PROJECT_ROOT / "var"
DOCS_DIR = PROJECT_ROOT / "docs"
