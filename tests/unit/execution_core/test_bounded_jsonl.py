from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from mgc_v05l.execution_core.bounded_jsonl import BoundedJsonlConfig, append_bounded_jsonl, write_bounded_jsonl


def _read_rows(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_append_bounded_jsonl_caps_large_nested_row(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    payload = {
        "schema_version": "test_v1",
        "classification": "BLOCKED",
        "lane_id": "mes_london_open_active_participation_long",
        "diagnostics": {"rows": [{"payload": "x" * 5000} for _ in range(100)]},
    }

    append_bounded_jsonl(
        path,
        payload,
        config=BoundedJsonlConfig(max_row_bytes=1200, max_file_bytes=10_000, max_archives=2),
    )

    encoded = path.read_bytes()
    assert len(encoded) <= 1200
    row = _read_rows(path)[0]
    assert row["_bounded_jsonl_truncated"] is True
    assert row["_bounded_jsonl_original_bytes"] > len(encoded)
    assert row["classification"] == "BLOCKED"
    assert row["lane_id"] == "mes_london_open_active_participation_long"


def test_append_bounded_jsonl_rotates_and_compresses_by_size(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    config = BoundedJsonlConfig(max_row_bytes=500, max_file_bytes=180, max_archives=2)

    append_bounded_jsonl(path, {"seq": 1, "payload": "a" * 80}, config=config)
    append_bounded_jsonl(path, {"seq": 2, "payload": "b" * 80}, config=config)

    archives = list(tmp_path.glob("events.*.jsonl.gz"))
    assert len(archives) == 1
    with gzip.open(archives[0], "rt", encoding="utf-8") as handle:
        archived_rows = [json.loads(line) for line in handle if line.strip()]
    assert archived_rows[0]["seq"] == 1
    assert _read_rows(path)[0]["seq"] == 2


def test_append_bounded_jsonl_prunes_old_archives(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    config = BoundedJsonlConfig(max_row_bytes=500, max_file_bytes=100, max_archives=1)

    for seq in range(4):
        append_bounded_jsonl(path, {"seq": seq, "payload": "x" * 80}, config=config)

    assert len(list(tmp_path.glob("events.*.jsonl.gz"))) == 1


def test_write_bounded_jsonl_keeps_recent_rows_within_file_cap(tmp_path: Path) -> None:
    path = tmp_path / "lane_events.jsonl"
    rows = [{"seq": seq, "payload": "x" * 60} for seq in range(20)]

    write_bounded_jsonl(path, rows, config=BoundedJsonlConfig(max_row_bytes=500, max_file_bytes=260))

    written = _read_rows(path)
    assert [row["seq"] for row in written] == [17, 18, 19]


def test_bounded_jsonl_refuses_latest_json_paths(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=r"\.jsonl"):
        append_bounded_jsonl(tmp_path / "latest_current_state.json", {"ok": True})
