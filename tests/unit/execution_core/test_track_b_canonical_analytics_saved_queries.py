from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from mgc_v05l.execution_core.track_b_canonical_analytics_engine import CanonicalAnalyticsQuery
from mgc_v05l.execution_core.track_b_canonical_analytics_saved_queries import (
    SAVED_QUERIES_JSONL,
    build_saved_query,
    load_saved_queries,
    preset_saved_queries,
    publish_saved_query_artifacts,
    run_saved_query,
    save_saved_query,
    validate_saved_query,
)


NOW = datetime(2026, 7, 3, 12, 0, tzinfo=UTC)


def test_save_valid_query(tmp_path: Path) -> None:
    saved = _saved_query("valid", CanonicalAnalyticsQuery(name="q", dimensions=("strategy",), metrics=("trade_count",)))
    path = save_saved_query(saved, output_dir=tmp_path)

    rows = load_saved_queries(path)
    assert path == tmp_path / SAVED_QUERIES_JSONL
    assert [row.saved_query_id for row in rows] == ["valid"]


def test_reject_invalid_dimension(tmp_path: Path) -> None:
    saved = _saved_query("bad_dim", CanonicalAnalyticsQuery(name="q", dimensions=("not_a_dimension",), metrics=("trade_count",)))

    with pytest.raises(ValueError):
        save_saved_query(saved, output_dir=tmp_path)


def test_reject_invalid_metric(tmp_path: Path) -> None:
    saved = _saved_query("bad_metric", CanonicalAnalyticsQuery(name="q", dimensions=("strategy",), metrics=("not_a_metric",)))

    with pytest.raises(ValueError):
        save_saved_query(saved, output_dir=tmp_path)


def test_stale_identifier_enters_repair_state() -> None:
    saved = _saved_query("stale", CanonicalAnalyticsQuery(name="q", dimensions=("strategy",), metrics=("not_a_metric",)))
    validation = validate_saved_query(saved)

    assert validation.status == "REPAIR_REQUIRED"
    assert validation.repair_required is True
    assert validation.stale_identifiers == ("metric:not_a_metric",)


def test_scope_binding_snapshot_and_inherit_serialize() -> None:
    inherit = _saved_query("inherit", CanonicalAnalyticsQuery(name="q"), scope_binding="inherit")
    snapshot = _saved_query("snapshot", CanonicalAnalyticsQuery(name="q"), scope_binding="snapshot")

    assert inherit.to_record()["scope_binding"] == "inherit"
    assert snapshot.to_record()["scope_binding"] == "snapshot"
    assert validate_saved_query(snapshot).status == "VALID"


def test_diagnostic_guardrails_preserved() -> None:
    saved = _saved_query("guardrails", CanonicalAnalyticsQuery(name="q"))
    record = saved.to_record()

    assert record["diagnostic_only"] is True
    assert record["production_recommendation"] is False
    assert record["trading_gate"] is False


def test_preset_queries_validate_against_live_catalog() -> None:
    presets = preset_saved_queries(now=NOW)

    assert len(presets) == 7
    assert all(validate_saved_query(preset).status == "VALID" for preset in presets)


def test_publish_artifacts_and_run_saved_query(tmp_path: Path) -> None:
    published = publish_saved_query_artifacts(output_dir=tmp_path, now=NOW)
    outcomes = tmp_path / "outcomes.jsonl"
    enrichments = tmp_path / "enrichments.jsonl"
    outcomes.write_text(json.dumps(_outcome("a", 10.0)) + "\n" + json.dumps(_outcome("b", -4.0)) + "\n", encoding="utf-8")
    enrichments.write_text(json.dumps(_enrichment("a")) + "\n" + json.dumps(_enrichment("b")) + "\n", encoding="utf-8")
    saved = {query.saved_query_id: query for query in load_saved_queries(published["saved_queries_path"])}["expectancy_by_strategy"]

    run_result = run_saved_query(saved, outcomes_path=outcomes, enrichments_path=enrichments)

    assert published["summary"]["preset_count"] == 7
    assert run_result.validation.status == "VALID"
    assert run_result.result is not None
    assert run_result.result["summary"]["matched_count"] == 2
    assert run_result.result["diagnostic_only"] is True


def test_saved_query_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_canonical_analytics_saved_queries.py"),
        Path("src/mgc_v05l/app/track_b_canonical_analytics_saved_queries.py"),
    ]
    forbidden_import_roots = (
        "mgc_v05l.execution.",
        "mgc_v05l.strategy",
        "mgc_v05l.app.ibkr",
        "ibapi",
        "ib_insync",
    )
    allowed_imports = {
        "mgc_v05l.execution_core.track_b_canonical_analytics_engine",
        "mgc_v05l.execution_core.track_b_canonical_analytics_saved_queries",
        "mgc_v05l.execution_core.track_b_core_expectancy_analytics",
        "mgc_v05l.execution_core.track_b_trade_outcome_enrichment",
        "mgc_v05l.execution_core.track_b_trade_outcome_layer",
    }
    forbidden_call_names = {"submit", "cancel", "modify", "placeOrder", "create_order_intent", "mutate_lifecycle", "flatten", "global_cancel"}
    violations: list[str] = []
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith(forbidden_import_roots) and alias.name not in allowed_imports:
                        violations.append(f"{path}:{alias.name}")
            elif isinstance(node, ast.ImportFrom) and node.module:
                if node.module.startswith(forbidden_import_roots) and node.module not in allowed_imports:
                    violations.append(f"{path}:{node.module}")
            elif isinstance(node, ast.Call):
                call_name = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else None
                if call_name in forbidden_call_names:
                    violations.append(f"{path}:{call_name}")
    assert violations == []


def _saved_query(saved_query_id: str, query: CanonicalAnalyticsQuery, *, scope_binding: str = "inherit"):
    return build_saved_query(
        saved_query_id=saved_query_id,
        name=saved_query_id,
        description="unit test",
        tags=("unit",),
        query=query,
        scope_binding=scope_binding,
        created_at=NOW,
        updated_at=NOW,
    )


def _outcome(trade_id: str, pnl: float) -> dict:
    return {
        "trade_outcome_id": trade_id,
        "strategy_id": "strategy_a",
        "lane_id": "lane_a",
        "session_at_entry": "LONDON",
        "instrument": "GC",
        "contract": "GCQ6",
        "side": "LONG",
        "entry_time": "2026-07-01T10:00:00Z",
        "realized_points": pnl,
        "realized_pnl_proxy": pnl,
        "hold_seconds": 120,
        "data_quality_flags": [],
    }


def _enrichment(trade_id: str) -> dict:
    return {
        "trade_outcome_id": trade_id,
        "vix_regime": "NORMAL",
        "vix_percentile": 0.4,
        "market_context_validity_classification": "VALID",
        "gre_label": "LONG",
        "gre_confidence": 64,
        "gre_validity_classification": "VALID",
        "crfd_validity_classification": "VALID",
        "data_quality_flags": [],
    }
