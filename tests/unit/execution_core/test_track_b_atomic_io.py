from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from mgc_v05l.execution_core.track_b_atomic_io import unique_atomic_temp_path, write_json_atomic


def test_unique_atomic_temp_path_changes_for_same_target(tmp_path: Path) -> None:
    target = tmp_path / "latest.json"

    paths = {unique_atomic_temp_path(target) for _ in range(8)}

    assert len(paths) == 8
    assert all(path.parent == target.parent for path in paths)
    assert all(path.name.startswith(".latest.json.") for path in paths)


def test_parallel_atomic_json_writes_do_not_collide(tmp_path: Path) -> None:
    target = tmp_path / "latest.json"

    def write(index: int) -> None:
        write_json_atomic(target, {"index": index, "payload": "ok"})

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(write, range(32)))

    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["payload"] == "ok"
    assert isinstance(payload["index"], int)
    assert not list(tmp_path.glob(".latest.json.*.tmp"))
