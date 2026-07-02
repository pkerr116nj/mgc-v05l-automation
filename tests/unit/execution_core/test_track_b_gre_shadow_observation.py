from __future__ import annotations

import ast
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from mgc_v05l.execution_core import track_b_gre_shadow_observation as observation
from mgc_v05l.execution_core.track_b_gre_shadow_observation import (
    _CrfdIndex,
    build_shadow_observation_row,
    evaluate_shadow_policy,
    run_gre_shadow_observation_generator,
)


NOW = datetime(2026, 7, 1, 17, 0, tzinfo=UTC)
GRE_TS = datetime(2026, 7, 1, 16, 55, tzinfo=UTC)


def test_would_allow_case() -> None:
    row = build_shadow_observation_row(
        _candidate(direction="LONG"),
        gre_payload=_gre("LONG"),
        crfd_index=_CrfdIndex([_crfd("above_avwap")]),
        generated_at=NOW,
        gre_generated_at=GRE_TS,
        sequence=1,
        gre_path="gre.json",
        crfd_rows_path="crfd.jsonl",
    )

    assert row["shadow_gate_result"] == "WOULD_ALLOW"
    assert row["shadow_gate_reason"] == "GRE_LONG_CONFIRMED_BY_GLOBEX_AVWAP"
    assert row["production_effect"] is False
    assert row["diagnostic_only"] is True


def test_would_block_case() -> None:
    row = build_shadow_observation_row(
        _candidate(direction="LONG"),
        gre_payload=_gre("LONG"),
        crfd_index=_CrfdIndex([_crfd("below_avwap")]),
        generated_at=NOW,
        gre_generated_at=GRE_TS,
        sequence=1,
        gre_path="gre.json",
        crfd_rows_path="crfd.jsonl",
    )

    assert row["shadow_gate_result"] == "WOULD_BLOCK"
    assert row["shadow_gate_reason"] == "GRE_OR_GLOBEX_AVWAP_NOT_CONFIRMING_DIRECTION"


def test_insufficient_evidence_case() -> None:
    result, reason = evaluate_shadow_policy(
        gre_label="LONG",
        intended_direction="LONG",
        contract="GC",
        avwap_relation="unavailable",
        gre_payload=_gre("LONG"),
    )

    assert result == "INSUFFICIENT_EVIDENCE"
    assert reason == "GLOBEX_AVWAP_UNAVAILABLE"


def test_no_candidate_artifacts_produces_safe_zero_summary(tmp_path: Path) -> None:
    gre_path = tmp_path / "gre.json"
    crfd_path = tmp_path / "crfd.jsonl"
    gre_path.write_text(json.dumps(_gre("LONG")), encoding="utf-8")
    crfd_path.write_text("", encoding="utf-8")

    result = run_gre_shadow_observation_generator(
        output_root=tmp_path / "outputs" / "track_b_execution_core",
        now=NOW,
        gre_path=gre_path,
        crfd_rows_path=crfd_path,
        candidate_paths=[tmp_path / "missing.json"],
    )

    assert result.report["observation_count"] == 0
    assert result.report["candidate_count"] == 0
    assert result.rows_path.exists()
    assert result.summary_json_path.exists()
    assert "No candidate strategy-intent diagnostics" in "\n".join(result.report["source_notes"])


def test_production_effect_and_diagnostic_only_are_forced() -> None:
    row = build_shadow_observation_row(
        {"contract": "GC", "intended_direction": "SHORT", "production_effect": True, "diagnostic_only": False},
        gre_payload=_gre("SHORT"),
        crfd_index=_CrfdIndex([_crfd("below_avwap")]),
        generated_at=NOW,
        gre_generated_at=GRE_TS,
        sequence=1,
        gre_path="gre.json",
        crfd_rows_path="crfd.jsonl",
    )

    assert row["production_effect"] is False
    assert row["diagnostic_only"] is True
    assert "broker_authority" not in row
    assert "runtime_authority" not in row
    assert "strategy_authority" not in row


def test_bounded_jsonl_writer_used(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    gre_path = tmp_path / "gre.json"
    crfd_path = tmp_path / "crfd.jsonl"
    candidate_path = tmp_path / "candidate.json"
    gre_path.write_text(json.dumps(_gre("LONG")), encoding="utf-8")
    crfd_path.write_text(json.dumps(_crfd("above_avwap")) + "\n", encoding="utf-8")
    candidate_path.write_text(json.dumps(_candidate(direction="LONG")), encoding="utf-8")
    called: dict[str, Path] = {}

    def fake_write(path: Path, rows: object, *, config: object | None = None) -> Path:
        called["path"] = path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(list(rows)[0]) + "\n", encoding="utf-8")
        return path

    monkeypatch.setattr(observation, "write_bounded_jsonl", fake_write)

    result = run_gre_shadow_observation_generator(
        output_root=tmp_path / "outputs" / "track_b_execution_core",
        now=NOW,
        gre_path=gre_path,
        crfd_rows_path=crfd_path,
        candidate_paths=[candidate_path],
    )

    assert called["path"] == result.rows_path
    assert result.report["writer"]["observations"] == "bounded_jsonl"
    assert result.report["observation_count"] == 1


def test_shadow_observation_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_gre_shadow_observation.py"),
        Path("src/mgc_v05l/app/track_b_gre_shadow_observation.py"),
    ]
    forbidden_import_roots = (
        "mgc_v05l.execution.",
        "mgc_v05l.strategy",
        "mgc_v05l.app.ibkr",
        "ibapi",
        "ib_insync",
    )
    forbidden_call_names = {"submit", "cancel", "placeOrder", "create_order_intent", "mutate_lifecycle", "flatten", "close"}
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


def _gre(label: str) -> dict:
    return {
        "generated_at": GRE_TS.isoformat(),
        "regime_label": label,
        "confidence": 72,
        "directional_bias": "BULLISH" if label == "LONG" else "BEARISH" if label == "SHORT" else "MIXED",
        "diagnostic_only": True,
    }


def _crfd(relation: str) -> dict:
    return {
        "observation_time": (GRE_TS - timedelta(minutes=1)).isoformat(),
        "contract": "GC",
        "session": "LONDON",
        "vwap_relation": "above_vwap" if relation == "above_avwap" else "below_vwap",
        "has_avwap_globex_session_open_18et": relation != "unavailable",
        "avwap_relation_globex_session_open_18et": relation,
    }


def _candidate(*, direction: str) -> dict:
    return {
        "strategy_id": "gold_test_strategy",
        "lane_id": "gc_test_lane",
        "contract": "GC",
        "side": "BUY" if direction == "LONG" else "SELL",
        "intended_direction": direction,
        "session": "LONDON",
    }
