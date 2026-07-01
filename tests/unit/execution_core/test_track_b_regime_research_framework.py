from __future__ import annotations

import ast
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_gold_regime_engine import GoldRegimePlugin, RegimeEngineContext
from mgc_v05l.execution_core.track_b_regime_engine_interface import RegimePluginDescriptor
from mgc_v05l.execution_core.track_b_regime_research_framework import (
    build_artifact_plan,
    build_future_plugin_examples,
    build_regime_research_framework_contract,
    validate_plugin_report_against_framework,
    write_regime_research_framework_documents,
)


NOW = datetime(2026, 6, 30, 17, 0, tzinfo=UTC)


def test_framework_contract_lists_reusable_pipeline_steps() -> None:
    contract = build_regime_research_framework_contract()

    steps = [item["step"] for item in contract["pipeline"]]
    assert steps == [
        "Regime Plugin",
        "Validation Logger",
        "Validation Store",
        "Scorecard Generator",
        "Research Analyzer",
        "Historical Backfill Runner",
    ]
    assert contract["authority_boundary"]["broker_authority"] is False
    assert contract["compatibility"]["gre_outputs_unchanged"] is True


def test_artifact_plan_is_generic_by_plugin_descriptor() -> None:
    plan = build_artifact_plan(
        RegimePluginDescriptor(
            engine_name="Nasdaq Regime Engine",
            plugin_id="NRE",
            instrument_family="Nasdaq futures",
        )
    )

    assert plan.output_namespace == "research/nre_regime_engine"
    assert plan.latest_report == "latest_nre_regime_engine.json"
    assert plan.validation_rows == "nre_validation_rows.jsonl"
    assert plan.backfill_observations == "nre_backfill_observations.jsonl"


def test_future_plugin_examples_are_illustrations_only_except_gre() -> None:
    examples = build_future_plugin_examples()
    by_plugin = {item["descriptor"]["plugin_id"]: item for item in examples}

    assert set(by_plugin) == {"GRE", "NRE", "ERE", "TRE"}
    assert by_plugin["GRE"]["implementation_status"] == "implemented_diagnostic_mvp"
    assert by_plugin["NRE"]["implementation_status"] == "illustration_only"
    assert by_plugin["ERE"]["implementation_status"] == "illustration_only"
    assert by_plugin["TRE"]["implementation_status"] == "illustration_only"


def test_gre_plugin_implements_generic_descriptor_and_output_contract() -> None:
    report = GoldRegimePlugin().evaluate(
        RegimeEngineContext(
            instrument="GOLD",
            symbols=("GC", "MGC"),
            candles_by_symbol_timeframe={"GC": {"1m": tuple(_bars(30, 1.0)), "5m": tuple(_bars(12, 3.0))}},
            source_refs={"fixture": "synthetic"},
            analytics={},
            generated_at=NOW,
        )
    )

    assert GoldRegimePlugin.descriptor.plugin_id == "GRE"
    compatibility = validate_plugin_report_against_framework(report, descriptor=GoldRegimePlugin.descriptor)
    assert compatibility["compatible"] is True
    assert compatibility["descriptor_issues"] == []


def test_framework_documents_are_written(tmp_path: Path) -> None:
    output_root = tmp_path / "outputs" / "track_b_execution_core"

    paths = write_regime_research_framework_documents(output_root=output_root, now=NOW)

    for value in paths.values():
        assert Path(value).exists()
    payload = json.loads(Path(paths["json"]).read_text(encoding="utf-8"))
    assert payload["diagnostic_only"] is True
    assert payload["future_plugin_examples"]


def test_regime_research_framework_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_regime_research_framework.py"),
        Path("src/mgc_v05l/execution_core/track_b_regime_engine_interface.py"),
    ]
    forbidden_import_roots = (
        "mgc_v05l.execution.",
        "mgc_v05l.strategy",
        "mgc_v05l.app.ibkr",
        "ibapi",
        "ib_insync",
    )
    forbidden_call_names = {"submit", "cancel", "placeOrder", "create_order_intent", "mutate_lifecycle", "flatten"}
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
                call_name: str | None = None
                if isinstance(node.func, ast.Name):
                    call_name = node.func.id
                elif isinstance(node.func, ast.Attribute):
                    call_name = node.func.attr
                if call_name in forbidden_call_names:
                    violations.append(f"{path}:{call_name}")
    assert violations == []


def _bars(count: int, step: float) -> list[dict]:
    rows: list[dict] = []
    for idx in range(count):
        close = 3300.0 + idx * step
        rows.append(
            {
                "timestamp": NOW.replace(minute=0) + timedelta(minutes=idx),
                "open": close - step * 0.5,
                "high": close + abs(step),
                "low": close - abs(step),
                "close": close,
                "volume": 100 + idx,
            }
        )
    return rows
