from __future__ import annotations

import ast
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_gre_shadow_gate_explainer import (
    build_gre_shadow_gate_explanation,
    run_gre_shadow_gate_explainer,
    winning_policy_accepts,
)


NOW = datetime(2026, 7, 1, 17, 0, tzinfo=UTC)
START = datetime(2026, 6, 30, 10, 0, tzinfo=UTC)


def test_accepted_rejected_grouping() -> None:
    report = build_gre_shadow_gate_explanation(
        [_row("LONG", 5), _row("LONG", -4, ts=START + timedelta(minutes=5))],
        crfd_rows=_crfd_rows(relations=("above_avwap", "below_avwap")),
        generated_at=NOW,
        rows_path="rows.jsonl",
        crfd_rows_path="crfd.jsonl",
    )

    assert report["overall"]["accepted"] == 1
    assert report["overall"]["rejected"] == 1
    assert winning_policy_accepts({"regime_label": "LONG", "avwap_relation_globex_session_open_18et": "above_avwap"}) is True


def test_avoided_losers_and_missed_winners_accounting() -> None:
    report = build_gre_shadow_gate_explanation(
        [
            _row("LONG", 8),
            _row("LONG", -6, ts=START + timedelta(minutes=5)),
            _row("SHORT", -7, ts=START + timedelta(minutes=10)),
            _row("SHORT", 5, ts=START + timedelta(minutes=15)),
        ],
        crfd_rows=_crfd_rows(count=4, relations=("above_avwap", "below_avwap", "below_avwap", "above_avwap")),
        generated_at=NOW,
        rows_path="rows.jsonl",
        crfd_rows_path="crfd.jsonl",
    )

    assert report["overall"]["accepted"] == 2
    assert report["overall"]["avoided_losers"] == 2
    assert report["overall"]["missed_winners"] == 0
    assert report["case_reviews"]["largest_avoided_losers"]


def test_session_and_side_breakdown() -> None:
    report = build_gre_shadow_gate_explanation(
        [_row("LONG", 3, bias="BULLISH"), _row("SHORT", -3, ts=START + timedelta(minutes=5), bias="BEARISH")],
        crfd_rows=_crfd_rows(relations=("above_avwap", "below_avwap"), session="LONDON"),
        generated_at=NOW,
        rows_path="rows.jsonl",
        crfd_rows_path="crfd.jsonl",
    )

    accepted_breakdown = report["breakdowns"]["accepted"]
    assert accepted_breakdown["session"] == {"LONDON": 2}
    assert accepted_breakdown["side"] == {"BEARISH": 1, "BULLISH": 1}


def test_low_sample_and_selectivity_warnings() -> None:
    report = build_gre_shadow_gate_explanation(
        [_row("LONG", 3)],
        crfd_rows=_crfd_rows(count=1, relations=("above_avwap",)),
        generated_at=NOW,
        rows_path="rows.jsonl",
        crfd_rows_path="crfd.jsonl",
    )

    assert report["robustness"]["selectivity_warning"] is True
    assert report["robustness"]["sample_size_warning"] is True
    assert report["recommendations"]["production_gating_recommended"] is False


def test_robustness_notes_generated(tmp_path: Path) -> None:
    rows_path = tmp_path / "rows.jsonl"
    crfd_path = tmp_path / "crfd.jsonl"
    rows_path.write_text("", encoding="utf-8")
    crfd_path.write_text("", encoding="utf-8")

    result = run_gre_shadow_gate_explainer(
        output_root=tmp_path / "outputs" / "track_b_execution_core",
        now=NOW,
        rows_path=rows_path,
        crfd_rows_path=crfd_path,
    )

    assert result.report["robustness"]["notes"]
    assert result.json_path.exists()
    assert result.robustness_path.exists()


def test_shadow_gate_explainer_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_gre_shadow_gate_explainer.py"),
        Path("src/mgc_v05l/app/track_b_gre_shadow_gate_explainer.py"),
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


def _row(regime: str, forward_60m: float, *, ts: datetime = START, bias: str | None = None) -> dict:
    return {
        "gre_generated_at": ts.isoformat(),
        "contract": "GC",
        "regime_label": regime,
        "directional_bias": bias or ("BULLISH" if regime == "LONG" else "BEARISH" if regime == "SHORT" else "MIXED"),
        "confidence": 70,
        "validation_status": "VALIDATED",
        "forward_returns": {"5m": forward_60m / 4, "15m": forward_60m / 2, "30m": forward_60m * 0.75, "60m": forward_60m},
        "mfe": abs(forward_60m) + 1,
        "mae": -abs(forward_60m) / 2,
        "direction_correctness": (forward_60m > 0 if regime == "LONG" else forward_60m < 0 if regime == "SHORT" else None),
    }


def _crfd_rows(
    *,
    count: int = 2,
    relations: tuple[str, ...] = ("above_avwap", "below_avwap"),
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
        for anchor in ("globex_session_open_18et", "london_open", "us_rth_open"):
            row[f"has_avwap_{anchor}"] = True
            row[f"avwap_relation_{anchor}"] = relation
            row[f"distance_from_avwap_{anchor}_points"] = 1.0 + idx
        rows.append(row)
    return rows
