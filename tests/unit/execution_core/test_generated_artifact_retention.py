from __future__ import annotations

import gzip
import json
from pathlib import Path

from mgc_v05l.execution_core.generated_artifact_retention import (
    CHECK_WARN,
    ROTATED,
    SKIPPED_PROTECTED,
    SKIPPED_UNSAFE_PATH,
    RetentionConfig,
    RetentionPolicy,
    check_generated_artifacts,
    maintain_generated_artifacts,
)


def test_rotation_preserves_tail_archives_and_recreates_live_file(tmp_path: Path) -> None:
    target = tmp_path / "outputs/reports/paper_strategy_monitor/paper_strategy_monitor_audit.jsonl"
    _write_lines(target, line_count=6000, payload_size=400)
    policy = _policy(
        streams=[
            {
                "stream_id": "paper_strategy_monitor",
                "patterns": ["outputs/reports/paper_strategy_monitor/paper_strategy_monitor_audit.jsonl"],
                "max_file_size_mb": 1,
                "keep_latest_tail_mb": 1,
                "max_archives_per_stream": 5,
                "compression_enabled": True,
                "recreate_live_file": True,
            }
        ]
    )

    payload = maintain_generated_artifacts(config=RetentionConfig(repo_root=tmp_path), policy=policy, apply=True)

    action = payload["actions"][0]
    assert action["status"] == ROTATED
    assert target.exists()
    assert target.stat().st_size == 0
    tail_path = target.with_name("paper_strategy_monitor_audit.recent_tail.jsonl")
    assert tail_path.exists()
    tail_text = tail_path.read_text(encoding="utf-8")
    assert '"i": 5999' in tail_text
    assert '"i": 0' not in tail_text
    archive_path = Path(action["archive_path"])
    assert archive_path.exists()
    with gzip.open(archive_path, "rt", encoding="utf-8") as handle:
        archive_text = handle.read()
    assert '"i": 0' in archive_text
    assert '"i": 5999' in archive_text


def test_dry_run_reports_without_mutating(tmp_path: Path) -> None:
    target = tmp_path / "outputs/track_b_execution_core/no_trade_diagnostics/no_trade_diagnostics.jsonl"
    _write_lines(target, line_count=4000, payload_size=350)
    before = target.read_text(encoding="utf-8")
    policy = _policy(
        streams=[
            {
                "stream_id": "no_trade_diagnostics",
                "patterns": ["outputs/track_b_execution_core/no_trade_diagnostics/no_trade_diagnostics.jsonl"],
                "max_file_size_mb": 1,
                "keep_latest_tail_mb": 1,
                "max_archives_per_stream": 5,
            }
        ]
    )

    payload = maintain_generated_artifacts(config=RetentionConfig(repo_root=tmp_path), policy=policy, apply=False)

    assert payload["dry_run"] is True
    assert payload["actions"][0]["status"] == "WOULD_ROTATE"
    assert target.read_text(encoding="utf-8") == before
    assert not (tmp_path / "outputs/archive/no_trade_diagnostics").exists()


def test_protected_current_registry_and_reconciliation_are_preserved(tmp_path: Path) -> None:
    registry = tmp_path / "outputs/track_b_execution_core/trade_registry/live_trade_events.jsonl"
    reconciliation = (
        tmp_path / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"
    )
    _write_lines(registry, line_count=4000, payload_size=350)
    reconciliation.parent.mkdir(parents=True, exist_ok=True)
    reconciliation.write_text(json.dumps({"classification": "KEEP"}) + (" " * 1_200_000), encoding="utf-8")
    policy = _policy(
        protected_globs=[
            "outputs/track_b_execution_core/trade_registry/**",
            "outputs/reports/track_b_paper_broker_reconciliation/**",
        ],
        streams=[
            {
                "stream_id": "broad_runtime",
                "patterns": ["outputs/**/*.jsonl", "outputs/**/*.json"],
                "max_file_size_mb": 1,
                "keep_latest_tail_mb": 1,
                "max_archives_per_stream": 5,
            }
        ],
    )

    payload = maintain_generated_artifacts(config=RetentionConfig(repo_root=tmp_path), policy=policy, apply=True)

    statuses = {item["relative_path"]: item["status"] for item in payload["actions"]}
    assert statuses["outputs/track_b_execution_core/trade_registry/live_trade_events.jsonl"] == SKIPPED_PROTECTED
    assert (
        statuses["outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"]
        == SKIPPED_PROTECTED
    )
    assert registry.exists()
    assert reconciliation.exists()
    assert not (tmp_path / "outputs/archive/broad_runtime").exists()


def test_forbidden_paths_are_never_rotated_even_if_configured(tmp_path: Path) -> None:
    config_file = tmp_path / "config/generated_artifact_retention.json"
    config_file.parent.mkdir(parents=True, exist_ok=True)
    config_file.write_text("x" * 1_200_000, encoding="utf-8")
    policy = _policy(
        streams=[
            {
                "stream_id": "bad_policy",
                "patterns": ["config/*.json"],
                "max_file_size_mb": 1,
                "keep_latest_tail_mb": 1,
                "max_archives_per_stream": 5,
            }
        ]
    )

    payload = maintain_generated_artifacts(config=RetentionConfig(repo_root=tmp_path), policy=policy, apply=True)

    assert payload["actions"][0]["status"] == SKIPPED_UNSAFE_PATH
    assert config_file.exists()
    assert config_file.stat().st_size == 1_200_000
    assert not (tmp_path / "outputs/archive/bad_policy").exists()


def test_check_reports_hot_path_thresholds_and_top_large_files(tmp_path: Path) -> None:
    target = tmp_path / "outputs/probationary_pattern_engine/paper_session/live_timing_events.jsonl"
    other = tmp_path / "outputs/reports/other_large.csv"
    _write_lines(target, line_count=4000, payload_size=350)
    other.parent.mkdir(parents=True, exist_ok=True)
    other.write_text("z" * 1_100_000, encoding="utf-8")
    policy = _policy(
        streams=[
            {
                "stream_id": "probationary_pattern_engine",
                "patterns": ["outputs/probationary_pattern_engine/**/*.jsonl"],
                "max_file_size_mb": 1,
                "keep_latest_tail_mb": 1,
                "max_archives_per_stream": 5,
            }
        ]
    )

    payload = check_generated_artifacts(config=RetentionConfig(repo_root=tmp_path, top_limit=5), policy=policy)

    assert payload["classification"] == CHECK_WARN
    assert payload["violation_count"] == 1
    assert payload["violations"][0]["relative_path"] == (
        "outputs/probationary_pattern_engine/paper_session/live_timing_events.jsonl"
    )
    top_paths = [item["relative_path"] for item in payload["top_large_files"]]
    assert "outputs/reports/other_large.csv" in top_paths
    assert "outputs/probationary_pattern_engine/paper_session/live_timing_events.jsonl" in top_paths


def _policy(*, streams: list[dict], protected_globs: list[str] | None = None) -> RetentionPolicy:
    return RetentionPolicy.from_mapping(
        {
            "max_file_size_mb": 1,
            "keep_latest_tail_mb": 1,
            "max_archives_per_stream": 5,
            "compression_enabled": True,
            "generated_roots": ["outputs", "logs"],
            "forbidden_roots": ["src", "tests", "docs", "scripts", "config", "var"],
            "protected_globs": protected_globs or [],
            "streams": streams,
        }
    )


def _write_lines(path: Path, *, line_count: int, payload_size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for index in range(line_count):
            handle.write(json.dumps({"i": index, "payload": "x" * payload_size}) + "\n")
