from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_gold_regime_engine import GOLD_REGIME_OUTPUT_DIR
from mgc_v05l.execution_core.track_b_gre_validation_logger import VALIDATED
from mgc_v05l.execution_core.track_b_gre_validation_scorecard import (
    LATEST_GRE_SCORECARD_JSON,
    LATEST_GRE_SCORECARD_MD,
    build_gre_validation_scorecard,
    run_gre_validation_scorecard,
)


NOW = datetime(2026, 6, 30, 15, 0, tzinfo=UTC)


def test_empty_history_scorecard_is_research_too_early() -> None:
    scorecard = build_gre_validation_scorecard([], generated_at=NOW, rows_path="rows.jsonl")

    assert scorecard["overall_metrics"]["total_observations"] == 0
    assert scorecard["readiness_assessment"]["classification"] == "RESEARCH_TOO_EARLY"
    assert scorecard["regime_breakdown"]["LONG"]["observation_count"] == 0
    assert scorecard["largest_unknowns"] == ["No GRE validation history yet."]


def test_mixed_pending_and_validated_counts() -> None:
    rows = [
        _row("LONG", 72, "VALIDATED", {"5m": 1.0, "60m": 2.0}, direction_correctness=True),
        _row("SHORT", 55, "PENDING_FORWARD_DATA", {}),
        _row("CHOP", 35, "INSUFFICIENT_DATA", {}),
    ]

    scorecard = build_gre_validation_scorecard(rows, generated_at=NOW, rows_path="rows.jsonl")

    assert scorecard["overall_metrics"]["total_observations"] == 3
    assert scorecard["overall_metrics"]["validated_observations"] == 1
    assert scorecard["overall_metrics"]["pending_observations"] == 1
    assert scorecard["overall_metrics"]["insufficient_observations"] == 1


def test_regime_aggregation_and_direction_correctness() -> None:
    rows = [
        _row("LONG", 70, VALIDATED, {"5m": 1.0, "15m": 2.0, "30m": 3.0, "60m": 4.0}, direction_correctness=True, mfe=5.0, mae=-1.0),
        _row("LONG", 80, VALIDATED, {"5m": -1.0, "15m": 1.0, "30m": 2.0, "60m": -2.0}, direction_correctness=False, mfe=2.0, mae=-3.0),
    ]

    scorecard = build_gre_validation_scorecard(rows, generated_at=NOW, rows_path="rows.jsonl")
    long_metrics = scorecard["regime_breakdown"]["LONG"]

    assert long_metrics["observation_count"] == 2
    assert long_metrics["average_confidence"] == 75.0
    assert long_metrics["confidence_range"] == {"min": 70.0, "max": 80.0}
    assert long_metrics["average_forward_return"]["60m"] == 1.0
    assert long_metrics["average_mfe"] == 3.5
    assert long_metrics["average_mae"] == -2.0
    assert long_metrics["direction_correctness_rate"] == 0.5


def test_confidence_band_aggregation() -> None:
    rows = [
        _row("LONG", 10, VALIDATED, {"60m": 1.0}, mfe=2.0, mae=-0.5),
        _row("SHORT", 45, VALIDATED, {"60m": -2.0}, mfe=3.0, mae=-1.0),
        _row("TRANSITION", 85, VALIDATED, {"60m": 0.5}, mfe=None, mae=None),
    ]

    scorecard = build_gre_validation_scorecard(rows, generated_at=NOW, rows_path="rows.jsonl")

    assert scorecard["confidence_bands"]["0-20"]["observations"] == 1
    assert scorecard["confidence_bands"]["40-60"]["observations"] == 1
    assert scorecard["confidence_bands"]["80-100"]["observations"] == 1
    assert scorecard["confidence_bands"]["40-60"]["average_outcome"] == -2.0


def test_descriptive_feature_ranking_and_missing_provider_counts() -> None:
    rows = [
        _row("LONG", 70, VALIDATED, {"60m": 3.0}, positive_feature="London Late"),
        _row("LONG", 72, VALIDATED, {"60m": 2.0}, positive_feature="London Late"),
        _row("SHORT", 62, VALIDATED, {"60m": -1.0}, positive_feature="Trend Persistence"),
    ]

    scorecard = build_gre_validation_scorecard(rows, generated_at=NOW, rows_path="rows.jsonl")

    assert scorecard["feature_effectiveness"][0]["feature"] == "London Late"
    assert scorecard["feature_effectiveness"][0]["observation_count"] == 2
    assert scorecard["missing_provider_analysis"][0]["provider"] == "VWAP unavailable in GRE MVP"
    assert scorecard["missing_provider_analysis"][0]["count"] == 3


def test_readiness_assessment_thresholds() -> None:
    early = build_gre_validation_scorecard([_row("LONG", 70, VALIDATED, {"60m": 1.0}) for _ in range(10)], generated_at=NOW, rows_path="rows.jsonl")
    shadow = build_gre_validation_scorecard([_row("LONG", 70, VALIDATED, {"60m": 1.0}) for _ in range(80)], generated_at=NOW, rows_path="rows.jsonl")
    gate_research = build_gre_validation_scorecard(
        [_row("LONG", 70, VALIDATED, {"60m": 1.0}, direction_correctness=True) for _ in range(150)],
        generated_at=NOW,
        rows_path="rows.jsonl",
    )

    assert early["readiness_assessment"]["classification"] == "RESEARCH_TOO_EARLY"
    assert shadow["readiness_assessment"]["classification"] == "SUFFICIENT_FOR_SHADOW"
    assert gate_research["readiness_assessment"]["classification"] == "READY_FOR_GATE_RESEARCH"


def test_scorecard_runner_writes_bounded_outputs(tmp_path: Path) -> None:
    output_root = tmp_path / "outputs" / "track_b_execution_core"
    scorecard_dir = output_root / GOLD_REGIME_OUTPUT_DIR
    scorecard_dir.mkdir(parents=True)
    rows_path = scorecard_dir / "gre_validation_rows.jsonl"
    rows_path.write_text(json.dumps(_row("LONG", 70, VALIDATED, {"60m": 1.0})) + "\n", encoding="utf-8")

    result = run_gre_validation_scorecard(output_root=output_root, now=NOW)

    assert result.json_path == scorecard_dir / LATEST_GRE_SCORECARD_JSON
    assert result.markdown_path == scorecard_dir / LATEST_GRE_SCORECARD_MD
    assert result.json_path.exists()
    assert result.markdown_path.exists()
    assert result.scorecard["diagnostic_only"] is True


def test_gre_scorecard_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_gre_validation_scorecard.py"),
        Path("src/mgc_v05l/app/track_b_gre_validation_scorecard.py"),
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


def _row(
    label: str,
    confidence: int,
    status: str,
    forward: dict[str, float],
    *,
    direction_correctness: bool | None = None,
    mfe: float | None = None,
    mae: float | None = None,
    positive_feature: str = "Trend Persistence",
) -> dict:
    return {
        "schema_version": "track_b_gre_validation_row_v1",
        "generated_at": NOW.isoformat(),
        "gre_generated_at": NOW.isoformat(),
        "instrument": "GOLD",
        "contract": "GC",
        "session": "LONDON_LATE",
        "regime_label": label,
        "confidence": confidence,
        "directional_bias": "BULLISH" if label == "LONG" else "BEARISH" if label == "SHORT" else "MIXED",
        "diagnostic_only": True,
        "positive_evidence": [{"feature": positive_feature, "points": 8, "explanation": positive_feature}],
        "negative_evidence": [],
        "conflicts": [],
        "missing_providers": [
            "VWAP unavailable in GRE MVP",
            "anchored VWAP unavailable in GRE MVP",
        ],
        "forward_returns": forward,
        "mfe": mfe,
        "mae": mae,
        "best_observed_holding_window": "60m" if forward else None,
        "direction_correctness": direction_correctness,
        "validation_status": status,
    }
