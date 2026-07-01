from __future__ import annotations

import ast
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_gre_avwap_evidence_research import (
    AVWAP_ANCHORS,
    build_gre_avwap_evidence_research,
    classify_confirmation_conflict,
    classify_distance_band,
    run_gre_avwap_evidence_research,
)


NOW = datetime(2026, 7, 1, 17, 0, tzinfo=UTC)
START = datetime(2026, 6, 30, 10, 0, tzinfo=UTC)


def test_avwap_anchor_grouping() -> None:
    report = build_gre_avwap_evidence_research(
        [_row("LONG", 6.0), _row("SHORT", -4.0, ts=START + timedelta(minutes=5))],
        crfd_rows=_crfd_rows(relations=("above_avwap", "below_avwap")),
        generated_at=NOW,
        rows_path="rows.jsonl",
        crfd_rows_path="crfd.jsonl",
    )

    for anchor in AVWAP_ANCHORS:
        assert report["anchor_analysis"][anchor]["availability_count"] == 2
        assert report["anchor_analysis"][anchor]["relation_distribution"]["above_avwap"] == 1
        assert report["anchor_analysis"][anchor]["relation_distribution"]["below_avwap"] == 1


def test_confirmation_conflict_classification() -> None:
    anchor = "london_open"
    assert classify_confirmation_conflict({"regime_label": "LONG", f"avwap_relation_{anchor}": "above_avwap"}, anchor=anchor) == "confirmation"
    assert classify_confirmation_conflict({"regime_label": "LONG", f"avwap_relation_{anchor}": "below_avwap"}, anchor=anchor) == "conflict"
    assert classify_confirmation_conflict({"regime_label": "SHORT", f"avwap_relation_{anchor}": "below_avwap"}, anchor=anchor) == "confirmation"
    assert classify_confirmation_conflict({"regime_label": "SHORT", f"avwap_relation_{anchor}": "above_avwap"}, anchor=anchor) == "conflict"


def test_distance_band_bucketing() -> None:
    assert classify_distance_band(0.2, near_threshold=1.0, large_threshold=4.0) == "near"
    assert classify_distance_band(2.5, near_threshold=1.0, large_threshold=4.0) == "modest_extension"
    assert classify_distance_band(-6.0, near_threshold=1.0, large_threshold=4.0) == "large_extension"
    assert classify_distance_band(None, near_threshold=1.0, large_threshold=4.0) == "unavailable"


def test_low_sample_findings_are_flagged() -> None:
    report = build_gre_avwap_evidence_research(
        [_row("LONG", 3.0)],
        crfd_rows=_crfd_rows(count=1),
        generated_at=NOW,
        rows_path="rows.jsonl",
        crfd_rows_path="crfd.jsonl",
    )

    assert report["anchor_analysis"]["london_open"]["low_sample"] is True
    assert report["distance_band_analysis"]["london_open"]["near"]["low_sample"] is True
    assert report["recommendations"]["should_add_avwap_to_gre_scoring_now"] is False


def test_chop_transition_do_not_force_directional_correctness() -> None:
    report = build_gre_avwap_evidence_research(
        [_row("CHOP", 12.0, direction_correct=None), _row("TRANSITION", -11.0, ts=START + timedelta(minutes=5), direction_correct=None)],
        crfd_rows=_crfd_rows(relations=("above_avwap", "below_avwap")),
        generated_at=NOW,
        rows_path="rows.jsonl",
        crfd_rows_path="crfd.jsonl",
    )

    metrics = report["chop_transition_analysis"]["london_open"]["strong_followthrough"]
    assert metrics["count"] == 2
    assert metrics["direction_correctness"] is None


def test_runner_writes_reports_for_empty_sample(tmp_path: Path) -> None:
    rows_path = tmp_path / "rows.jsonl"
    crfd_path = tmp_path / "crfd.jsonl"
    rows_path.write_text("", encoding="utf-8")
    crfd_path.write_text("", encoding="utf-8")

    result = run_gre_avwap_evidence_research(
        output_root=tmp_path / "outputs" / "track_b_execution_core",
        now=NOW,
        rows_path=rows_path,
        crfd_rows_path=crfd_path,
    )

    assert result.report["overall"]["observation_count"] == 0
    assert result.json_path.exists()
    assert result.anchor_comparison_path.exists()
    assert result.recommendations_path.exists()


def test_avwap_research_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_gre_avwap_evidence_research.py"),
        Path("src/mgc_v05l/app/track_b_gre_avwap_evidence_research.py"),
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


def _row(regime: str, forward_60m: float, *, ts: datetime = START, direction_correct: bool | None = True) -> dict:
    return {
        "gre_generated_at": ts.isoformat(),
        "contract": "GC",
        "session": "ASIA",
        "regime_label": regime,
        "confidence": 70,
        "validation_status": "VALIDATED",
        "forward_returns": {"5m": forward_60m / 4, "15m": forward_60m / 2, "30m": forward_60m * 0.75, "60m": forward_60m},
        "mfe": abs(forward_60m) + 1,
        "mae": -abs(forward_60m) / 2,
        "direction_correctness": direction_correct,
    }


def _crfd_rows(
    *,
    count: int = 2,
    relations: tuple[str, ...] = ("above_avwap", "above_avwap"),
    session: str = "ASIA",
) -> list[dict]:
    rows: list[dict] = []
    for idx in range(count):
        ts = START + timedelta(minutes=idx * 5)
        relation = relations[min(idx, len(relations) - 1)]
        row = {
            "observation_time": ts.isoformat(),
            "contract": "GC",
            "session": session,
            "vwap_relation": relation,
        }
        for anchor in AVWAP_ANCHORS:
            row[f"has_avwap_{anchor}"] = True
            row[f"avwap_relation_{anchor}"] = relation
            row[f"distance_from_avwap_{anchor}_points"] = 0.5 + idx * 5
            row[f"avwap_slope_{anchor}"] = 0.1
        rows.append(row)
    return rows
