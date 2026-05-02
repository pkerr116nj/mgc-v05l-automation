from __future__ import annotations

import ast
from pathlib import Path


FORBIDDEN_IMPORT_PREFIXES = (
    "mgc_v05l.execution.",
    "mgc_v05l.app",
    "mgc_v05l.strategy",
    "mgc_v05l.market_data.schwab",
    "mgc_v05l.research",
    "validation_layer",
)


def test_execution_core_does_not_import_forbidden_track_a_modules() -> None:
    package_root = Path("src/mgc_v05l/execution_core")
    violations: list[str] = []

    for path in sorted(package_root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            imported: list[str] = []
            if isinstance(node, ast.Import):
                imported = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                imported = [node.module]
            for module in imported:
                if _is_forbidden_import(module):
                    violations.append(f"{path}: {module}")

    assert violations == []


def _is_forbidden_import(module: str) -> bool:
    if module == "mgc_v05l.execution":
        return True
    return module.startswith(FORBIDDEN_IMPORT_PREFIXES)
