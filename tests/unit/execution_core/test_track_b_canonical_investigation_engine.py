from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_canonical_investigation_engine import (
    attach_reference,
    create_investigation,
    investigation_fingerprint,
    publish_ie1_artifacts,
    summarize_investigation,
    validate_investigation,
    write_investigation,
)


NOW = datetime(2026, 7, 5, 12, 0, tzinfo=UTC)


def test_create_investigation(tmp_path: Path) -> None:
    result = _create(tmp_path)

    assert result.investigation["schema_version"] == "ie1_canonical_investigation_v1"
    assert result.investigation["status"] == "OPEN"
    assert result.investigation["diagnostic_only"] is True
    assert result.investigation_path.exists()
    assert result.summary_path.exists()


def test_attach_saved_query(tmp_path: Path) -> None:
    result = _create(tmp_path)

    updated = attach_reference(
        result.investigation,
        reference_type="saved_query",
        reference_id="expectancy_by_strategy",
        artifact_path="saved_queries.jsonl",
        now="2026-07-05T12:01:00Z",
    )
    summary = summarize_investigation(updated)

    assert summary["reference_count"] == 1
    assert summary["attached_queries"][0]["reference_id"] == "expectancy_by_strategy"
    assert summary["latest_timeline_event"]["event_type"] == "QUERY_EXECUTED"


def test_attach_insight(tmp_path: Path) -> None:
    result = _create(tmp_path)

    updated = attach_reference(result.investigation, reference_type="insight", reference_id="insight_1", now="2026-07-05T12:01:00Z")
    summary = summarize_investigation(updated)

    assert summary["attached_insights"][0]["reference_id"] == "insight_1"
    assert summary["latest_timeline_event"]["event_type"] == "INSIGHT_ATTACHED"


def test_attach_morning_brief(tmp_path: Path) -> None:
    result = _create(tmp_path)

    updated = attach_reference(result.investigation, reference_type="morning_brief", reference_id="morning_brief_abc", now="2026-07-05T12:01:00Z")

    assert updated["timeline"][-1]["event_type"] == "MORNING_BRIEF_REFERENCED"


def test_timeline_ordering(tmp_path: Path) -> None:
    result = _create(tmp_path)

    updated = attach_reference(result.investigation, reference_type="insight", reference_id="late", now="2026-07-05T12:03:00Z")
    updated = attach_reference(updated, reference_type="diff_result", reference_id="early", now="2026-07-05T12:02:00Z")

    timestamps = [event["timestamp"] for event in updated["timeline"]]
    assert timestamps == sorted(timestamps)


def test_fingerprint_stability() -> None:
    summary = {
        "investigation_id": "inv",
        "title": "Title",
        "hypothesis": "Hypothesis",
        "current_status": "OPEN",
        "updated_at": "2026-07-05T12:00:00Z",
    }
    changed_time = dict(summary, updated_at="2026-07-05T13:00:00Z")

    assert investigation_fingerprint(summary) == investigation_fingerprint(changed_time)


def test_guardrails_preserved(tmp_path: Path) -> None:
    result = _create(tmp_path)
    validation = validate_investigation(result.investigation)

    assert validation["valid"] is True
    assert result.summary["diagnostic_only"] is True
    assert result.summary["production_recommendation"] is False
    assert result.summary["trading_gate"] is False


def test_publish_ie1_artifacts(tmp_path: Path) -> None:
    paths = publish_ie1_artifacts(output_dir=tmp_path, now=NOW)

    for path in paths.values():
        assert Path(path).exists()
    json.loads(Path(paths["schema"]).read_text(encoding="utf-8"))
    json.loads(Path(paths["sample"]).read_text(encoding="utf-8"))


def test_write_investigation_round_trip(tmp_path: Path) -> None:
    result = _create(tmp_path)
    written = write_investigation(result.investigation, output_dir=tmp_path)

    assert written.summary["deterministic_fingerprint"] == result.summary["deterministic_fingerprint"]


def test_investigation_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_canonical_investigation_engine.py"),
        Path("src/mgc_v05l/app/track_b_canonical_investigation_engine.py"),
    ]
    forbidden_import_roots = (
        "mgc_v05l.execution.",
        "mgc_v05l.strategy",
        "mgc_v05l.app.ibkr",
        "ibapi",
        "ib_insync",
    )
    forbidden_call_names = {"submit", "cancel", "modify", "placeOrder", "create_order_intent", "mutate_lifecycle", "flatten", "global_cancel"}
    violations: list[str] = []
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith(forbidden_import_roots):
                        violations.append(f"{path}:{alias.name}")
            elif isinstance(node, ast.ImportFrom) and node.module:
                if node.module.startswith(forbidden_import_roots):
                    violations.append(f"{path}:{node.module}")
            elif isinstance(node, ast.Call):
                call_name = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else None
                if call_name in forbidden_call_names:
                    violations.append(f"{path}:{call_name}")
    assert violations == []


def _create(tmp_path: Path):
    return create_investigation(
        title="Investigate strategy session expectancy",
        description="Durable research workspace for a diagnostic hypothesis.",
        hypothesis="Session context may explain strategy expectancy variation.",
        owner="operator",
        tags=["expectancy", "session"],
        investigation_id="inv_test",
        now=NOW,
        output_dir=tmp_path,
    )
