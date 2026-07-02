from __future__ import annotations

import ast
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_gre_shadow_zero_allow_diagnosis import build_zero_allow_diagnosis


NOW = datetime(2026, 7, 2, 4, 30, tzinfo=UTC)
START = datetime(2026, 6, 30, 10, 0, tzinfo=UTC)


def test_reason_breakdown_generation() -> None:
    report = build_zero_allow_diagnosis(
        [
            _obs("INSUFFICIENT_EVIDENCE", "GRE_GOLD_ONLY_SYMBOL_UNSUPPORTED", contract="MNQ"),
            _obs("WOULD_BLOCK", "GRE_OR_GLOBEX_AVWAP_NOT_CONFIRMING_DIRECTION", contract="GC", gre_label="TRANSITION"),
            _obs("INSUFFICIENT_EVIDENCE", "GLOBEX_AVWAP_UNAVAILABLE", contract="GC", avwap="unavailable"),
        ],
        historical_rows=[_hist("LONG")],
        crfd_rows=[_crfd("above_avwap")],
        historical_simulation=_simulation(),
        generated_at=NOW,
        observation_rows_path="obs.jsonl",
        historical_rows_path="hist.jsonl",
        crfd_rows_path="crfd.jsonl",
        historical_simulation_path="sim.json",
    )

    counts = report["offline_observation_diagnosis"]["failure_reason_counts"]
    assert counts["unsupported_symbol"] == 1
    assert counts["gre_not_long_or_short"] == 1
    assert counts["avwap_unavailable"] == 1


def test_offline_vs_historical_comparison() -> None:
    report = build_zero_allow_diagnosis(
        [_obs("WOULD_BLOCK", "GRE_OR_GLOBEX_AVWAP_NOT_CONFIRMING_DIRECTION", gre_label="TRANSITION")],
        historical_rows=[_hist("LONG"), _hist("SHORT", ts=START + timedelta(minutes=5))],
        crfd_rows=[_crfd("above_avwap"), _crfd("below_avwap", ts=START + timedelta(minutes=5))],
        historical_simulation=_simulation(accepted=2, rejected=0),
        generated_at=NOW,
        observation_rows_path="obs.jsonl",
        historical_rows_path="hist.jsonl",
        crfd_rows_path="crfd.jsonl",
        historical_simulation_path="sim.json",
    )

    comparison = report["offline_vs_historical_comparison"]
    assert comparison["offline_allow_count"] == 0
    assert comparison["historical_allow_count"] == 2
    assert comparison["offline_is_representative_of_r18"] is False


def test_missing_field_accounting() -> None:
    report = build_zero_allow_diagnosis(
        [
            _obs("INSUFFICIENT_EVIDENCE", "INTENDED_DIRECTION_UNAVAILABLE", direction=None),
            _obs("INSUFFICIENT_EVIDENCE", "GLOBEX_AVWAP_UNAVAILABLE", avwap="unavailable"),
        ],
        historical_rows=[],
        crfd_rows=[],
        historical_simulation={},
        generated_at=NOW,
        observation_rows_path="obs.jsonl",
        historical_rows_path="hist.jsonl",
        crfd_rows_path="crfd.jsonl",
        historical_simulation_path="sim.json",
    )

    offline = report["offline_observation_diagnosis"]
    assert offline["intended_direction_missing_count"] == 1
    assert offline["missing_or_unavailable_globex_avwap_count"] == 1


def test_low_sample_and_representativeness_warning() -> None:
    report = build_zero_allow_diagnosis(
        [],
        historical_rows=[_hist("LONG")],
        crfd_rows=[_crfd("above_avwap")],
        historical_simulation=_simulation(),
        generated_at=NOW,
        observation_rows_path="obs.jsonl",
        historical_rows_path="hist.jsonl",
        crfd_rows_path="crfd.jsonl",
        historical_simulation_path="sim.json",
    )

    assert report["conclusions"]["primary_driver"] == "no_observations"
    assert report["offline_vs_historical_comparison"]["representativeness_reasons"]
    assert report["recommendations"]["production_gate_recommended"] is False


def test_zero_allow_diagnosis_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_gre_shadow_zero_allow_diagnosis.py"),
        Path("src/mgc_v05l/app/track_b_gre_shadow_zero_allow_diagnosis.py"),
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


def _obs(
    result: str,
    reason: str,
    *,
    contract: str = "GC",
    gre_label: str = "LONG",
    direction: str | None = "LONG",
    avwap: str = "below_avwap",
) -> dict:
    return {
        "shadow_gate_result": result,
        "shadow_gate_reason": reason,
        "contract": contract,
        "session": "LONDON",
        "gre_label": gre_label,
        "gre_confidence": 44,
        "intended_direction": direction,
        "vwap_relation": "above_vwap",
        "avwap_globex_session_open_18et_relation": avwap,
        "source_refs": {"candidate_source": "blocked_strategy_intents.jsonl"},
    }


def _hist(label: str, *, ts: datetime = START) -> dict:
    return {
        "gre_generated_at": ts.isoformat(),
        "contract": "GC",
        "regime_label": label,
        "directional_bias": "BULLISH" if label == "LONG" else "BEARISH" if label == "SHORT" else "MIXED",
        "confidence": 70,
        "validation_status": "VALIDATED",
    }


def _crfd(relation: str, *, ts: datetime = START) -> dict:
    return {
        "observation_time": ts.isoformat(),
        "contract": "GC",
        "session": "LONDON",
        "vwap_relation": "above_vwap",
        "avwap_relation_globex_session_open_18et": relation,
    }


def _simulation(*, accepted: int = 1, rejected: int = 0) -> dict:
    return {
        "policy_matrix": {
            "avwap_confirmation_globex_session_open_18et": {
                "accepted_count": accepted,
                "rejected_count": rejected,
                "acceptance_rate": accepted / max(accepted + rejected, 1),
                "expectancy_proxy": 1.0,
                "net_filter_value": 0.5,
            }
        }
    }
