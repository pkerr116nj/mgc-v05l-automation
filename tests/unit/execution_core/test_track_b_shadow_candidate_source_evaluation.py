from __future__ import annotations

import ast
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_shadow_candidate_source_evaluation import build_shadow_candidate_source_evaluation


NOW = datetime(2026, 7, 2, 5, 0, tzinfo=UTC)
START = datetime(2026, 6, 30, 10, 0, tzinfo=UTC)


def test_candidate_adapter_selection() -> None:
    report = _report()
    rec = report["canonical_recommendation"]

    assert rec["canonical_shadow_observation_candidate_source_v1"] == "gold_only_timestamped_strategy_intent_candidates"
    assert report["safety_contract"]["runtime_hook"] is False
    assert report["safety_contract"]["trading_gate"] is False


def test_join_quality_accounting() -> None:
    report = _report()
    populations = {row["population_id"]: row for row in report["candidate_populations"]}

    blocked = populations["blocked_intent_diagnostics"]
    assert blocked["join_quality"]["gre_join_rate"] == 1.0
    assert blocked["join_quality"]["crfd_join_rate"] == 0.5
    assert blocked["join_quality"]["both_join_rate"] == 0.5


def test_representativeness_calculations() -> None:
    report = _report()
    populations = {row["population_id"]: row for row in report["candidate_populations"]}

    crfd = populations["generated_crfd_observation_points"]
    assert crfd["criteria"]["observation_count"] == 2
    assert crfd["criteria"]["gold_coverage"] == 1.0
    assert crfd["representativeness"]["score"] >= 0.0


def test_recommendation_generation_mentions_future_reuse() -> None:
    report = _report()

    evolution = report["canonical_recommendation"]["expected_future_evolution"]
    assert any("NRE" in item and "TRE" in item for item in evolution)


def test_shadow_candidate_source_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_shadow_candidate_source_evaluation.py"),
        Path("src/mgc_v05l/app/track_b_shadow_candidate_source_evaluation.py"),
    ]
    forbidden_import_roots = (
        "mgc_v05l.execution.",
        "mgc_v05l.strategy",
        "mgc_v05l.app.ibkr",
        "ibapi",
        "ib_insync",
    )
    forbidden_call_names = {"submit", "cancel", "modify", "placeOrder", "create_order_intent", "mutate_lifecycle", "flatten"}
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


def _report() -> dict:
    return build_shadow_candidate_source_evaluation(
        observation_rows=[_observation("GC", "LONG", joined=True), _observation("MNQ", "SHORT", joined=False)],
        historical_rows=[_historical("LONG"), _historical("SHORT", ts=START + timedelta(minutes=5))],
        crfd_rows=[_crfd("above_avwap"), _crfd("below_avwap", ts=START + timedelta(minutes=5))],
        post_governance_rows=[{"contract": "GC", "symbol": "GC", "intended_direction": "LONG", "lane_id": "gc_london_open_long"}],
        generated_at=NOW,
        observation_rows_path="obs.jsonl",
        historical_rows_path="hist.jsonl",
        crfd_rows_path="crfd.jsonl",
    )


def _observation(contract: str, direction: str, *, joined: bool) -> dict:
    return {
        "generated_at": START.isoformat(),
        "symbol": contract,
        "contract": contract,
        "session": "LONDON",
        "strategy_id": f"{contract.lower()}_strategy",
        "lane_id": f"{contract.lower()}_lane",
        "intended_direction": direction,
        "gre_label": direction,
        "gre_confidence": 70,
        "vwap_relation": "above_vwap",
        "avwap_globex_session_open_18et_relation": "above_avwap",
        "source_refs": {"crfd_observation_time": START.isoformat() if joined else None},
    }


def _historical(label: str, *, ts: datetime = START) -> dict:
    return {
        "gre_generated_at": ts.isoformat(),
        "contract": "GC",
        "regime_label": label,
        "directional_bias": "BULLISH" if label == "LONG" else "BEARISH",
        "confidence": 70,
    }


def _crfd(relation: str, *, ts: datetime = START) -> dict:
    return {
        "observation_time": ts.isoformat(),
        "contract": "GC",
        "session": "LONDON",
        "vwap_relation": "above_vwap",
        "avwap_relation_globex_session_open_18et": relation,
    }
