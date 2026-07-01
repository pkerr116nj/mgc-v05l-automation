from __future__ import annotations

import ast
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_gre_historical_scorecard_deep_dive import (
    AVWAP_ANCHORS,
    build_gre_historical_scorecard_deep_dive,
    run_gre_historical_scorecard_deep_dive,
)


NOW = datetime(2026, 7, 1, 17, 0, tzinfo=UTC)
START = datetime(2026, 6, 30, 10, 0, tzinfo=UTC)


def test_regime_aggregation() -> None:
    report = build_gre_historical_scorecard_deep_dive(
        [_row("LONG", 70, 4.0), _row("LONG", 80, -2.0), _row("SHORT", 65, -5.0, direction_correct=True)],
        crfd_rows=_crfd_rows(),
        generated_at=NOW,
        rows_path="rows.jsonl",
        crfd_rows_path="crfd.jsonl",
    )

    long_metrics = report["regime_label_analysis"]["LONG"]
    assert long_metrics["count"] == 2
    assert long_metrics["validated_count"] == 2
    assert long_metrics["average_confidence"] == 75.0
    assert long_metrics["average_forward_return"]["60m"] == 1.0
    assert report["regime_label_analysis"]["SHORT"]["direction_correctness_rate"] == 1.0


def test_confidence_band_aggregation() -> None:
    report = build_gre_historical_scorecard_deep_dive(
        [_row("LONG", 25, 1.0), _row("LONG", 45, 2.0), _row("LONG", 75, 3.0)],
        crfd_rows=_crfd_rows(),
        generated_at=NOW,
        rows_path="rows.jsonl",
        crfd_rows_path="crfd.jsonl",
    )

    assert report["confidence_band_analysis"]["20-40"]["count"] == 1
    assert report["confidence_band_analysis"]["40-60"]["average_forward_return"]["60m"] == 2.0
    assert report["confidence_band_analysis"]["60-80"]["median_forward_return"]["60m"] == 3.0


def test_session_breakout() -> None:
    report = build_gre_historical_scorecard_deep_dive(
        [_row("LONG", 70, 3.0, ts=START), _row("SHORT", 60, -2.0, ts=START + timedelta(minutes=5))],
        crfd_rows=_crfd_rows(session="LONDON"),
        generated_at=NOW,
        rows_path="rows.jsonl",
        crfd_rows_path="crfd.jsonl",
    )

    assert report["session_analysis"]["LONDON"]["count"] == 2
    assert report["session_analysis"]["LONDON"]["regime_distribution"] == {"LONG": 1, "SHORT": 1}


def test_vwap_and_avwap_diagnostic_grouping() -> None:
    rows = [_row("LONG", 70, 2.0, ts=START), _row("SHORT", 70, -3.0, ts=START + timedelta(minutes=5))]
    crfd = _crfd_rows(vwap_relations=("above_vwap", "below_vwap"), avwap_relation="above_avwap")
    report = build_gre_historical_scorecard_deep_dive(
        rows,
        crfd_rows=crfd,
        generated_at=NOW,
        rows_path="rows.jsonl",
        crfd_rows_path="crfd.jsonl",
    )

    assert report["vwap_relation_analysis"]["above_vwap"]["count"] == 1
    assert report["vwap_relation_analysis"]["below_vwap"]["count"] == 1
    for anchor in AVWAP_ANCHORS:
        assert report["anchored_vwap_analysis"][anchor]["availability_count"] == 2
        assert report["anchored_vwap_analysis"][anchor]["relation_counts"]["above_avwap"] == 2


def test_failure_case_selection() -> None:
    rows = [
        _row("LONG", 70, -12.0),
        _row("SHORT", 70, 15.0, ts=START + timedelta(minutes=5)),
        _row("TRANSITION", 45, 18.0, ts=START + timedelta(minutes=10)),
        _row("CHOP", 50, -16.0, ts=START + timedelta(minutes=15)),
    ]
    report = build_gre_historical_scorecard_deep_dive(
        rows,
        crfd_rows=_crfd_rows(count=4),
        generated_at=NOW,
        rows_path="rows.jsonl",
        crfd_rows_path="crfd.jsonl",
    )

    failures = report["failure_analysis"]
    assert failures["worst_false_long_cases"][0]["forward_60m"] == -12.0
    assert failures["worst_false_short_cases"][0]["forward_60m"] == 15.0
    assert failures["transition_cases_with_strong_followthrough"][0]["forward_60m"] == 18.0
    assert failures["chop_cases_that_trended_anyway"][0]["forward_60m"] == -16.0


def test_empty_sample_is_safe(tmp_path: Path) -> None:
    rows_path = tmp_path / "rows.jsonl"
    crfd_path = tmp_path / "crfd.jsonl"
    rows_path.write_text("", encoding="utf-8")
    crfd_path.write_text("", encoding="utf-8")

    result = run_gre_historical_scorecard_deep_dive(
        output_root=tmp_path / "outputs" / "track_b_execution_core",
        now=NOW,
        rows_path=rows_path,
        crfd_rows_path=crfd_path,
    )

    assert result.report["overall"]["observation_count"] == 0
    assert result.report["gate_research_recommendations"]["ready_for_shadow_gate_simulation"] is False
    assert result.json_path.exists()
    assert result.failure_case_review_path.exists()


def test_gre_deep_dive_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_gre_historical_scorecard_deep_dive.py"),
        Path("src/mgc_v05l/app/track_b_gre_historical_scorecard_deep_dive.py"),
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
                call_name = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else None
                if call_name in forbidden_call_names:
                    violations.append(f"{path}:{call_name}")
    assert violations == []


def _row(
    regime: str,
    confidence: int,
    forward_60m: float,
    *,
    ts: datetime = START,
    direction_correct: bool | None = None,
) -> dict:
    return {
        "gre_generated_at": ts.isoformat(),
        "contract": "GC",
        "session": "ASIA",
        "regime_label": regime,
        "confidence": confidence,
        "directional_bias": "BULLISH" if regime == "LONG" else "BEARISH" if regime == "SHORT" else "MIXED",
        "validation_status": "VALIDATED",
        "forward_returns": {"5m": forward_60m / 4, "15m": forward_60m / 2, "30m": forward_60m * 0.75, "60m": forward_60m},
        "mfe": abs(forward_60m) + 1,
        "mae": -abs(forward_60m) / 2,
        "direction_correctness": direction_correct,
        "positive_evidence": [{"feature": "trend_persistence_1m"}],
        "negative_evidence": [],
        "conflicts": [],
        "missing_providers": [],
    }


def _crfd_rows(
    *,
    session: str = "ASIA",
    count: int = 2,
    vwap_relations: tuple[str, ...] = ("above_vwap", "above_vwap"),
    avwap_relation: str = "below_avwap",
) -> list[dict]:
    rows: list[dict] = []
    for idx in range(count):
        ts = START + timedelta(minutes=idx * 5)
        relation = vwap_relations[min(idx, len(vwap_relations) - 1)]
        row = {
            "observation_time": ts.isoformat(),
            "contract": "GC",
            "session": session,
            "session_label": f"{session}_TEST",
            "vwap_relation": relation,
            "distance_from_vwap_points": 1.0,
            "has_anchored_vwap": True,
        }
        for anchor in AVWAP_ANCHORS:
            row[f"has_avwap_{anchor}"] = True
            row[f"avwap_relation_{anchor}"] = avwap_relation
            row[f"distance_from_avwap_{anchor}_points"] = 2.0
        rows.append(row)
    return rows
