from __future__ import annotations

import ast
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_gre_shadow_gate_simulator import (
    build_gre_shadow_gate_simulation,
    evaluate_policy,
    run_gre_shadow_gate_simulator,
)


NOW = datetime(2026, 7, 1, 17, 0, tzinfo=UTC)
START = datetime(2026, 6, 30, 10, 0, tzinfo=UTC)


def test_baseline_accepts_all() -> None:
    rows = [_row("LONG", 50, 5), _row("TRANSITION", 30, -2, ts=START + timedelta(minutes=5))]
    policy = evaluate_policy(rows, policy_id="baseline", description="all", accept_fn=lambda row: True)

    assert policy["accepted_count"] == 2
    assert policy["rejected_count"] == 0
    assert policy["acceptance_rate"] == 1.0


def test_regime_only_gate_behavior() -> None:
    report = build_gre_shadow_gate_simulation(
        [_row("LONG", 60, 4), _row("SHORT", 60, -3, ts=START + timedelta(minutes=5)), _row("CHOP", 60, 1, ts=START + timedelta(minutes=10))],
        crfd_rows=_crfd_rows(count=3),
        generated_at=NOW,
        rows_path="rows.jsonl",
        crfd_rows_path="crfd.jsonl",
    )

    policy = report["policy_matrix"]["regime_only_directional"]
    assert policy["accepted_count"] == 2
    assert policy["rejected_count"] == 1


def test_confidence_threshold_gate_behavior() -> None:
    report = build_gre_shadow_gate_simulation(
        [_row("LONG", 45, 4), _row("LONG", 65, 5, ts=START + timedelta(minutes=5)), _row("SHORT", 75, -4, ts=START + timedelta(minutes=10))],
        crfd_rows=_crfd_rows(count=3),
        generated_at=NOW,
        rows_path="rows.jsonl",
        crfd_rows_path="crfd.jsonl",
    )

    assert report["policy_matrix"]["confidence_threshold_50"]["accepted_count"] == 2
    assert report["policy_matrix"]["confidence_threshold_70"]["accepted_count"] == 1


def test_avwap_confirmation_gate_behavior() -> None:
    report = build_gre_shadow_gate_simulation(
        [_row("LONG", 70, 6), _row("SHORT", 70, -5, ts=START + timedelta(minutes=5)), _row("LONG", 70, -3, ts=START + timedelta(minutes=10))],
        crfd_rows=_crfd_rows(relations=("above_avwap", "below_avwap", "below_avwap"), count=3),
        generated_at=NOW,
        rows_path="rows.jsonl",
        crfd_rows_path="crfd.jsonl",
    )

    policy = report["policy_matrix"]["avwap_confirmation_london_open"]
    assert policy["accepted_count"] == 2
    assert policy["rejected_count"] == 1


def test_avoided_losers_and_missed_winners_accounting() -> None:
    rows = [_row("LONG", 70, 5), _row("LONG", 70, -4, ts=START + timedelta(minutes=5))]
    report = build_gre_shadow_gate_simulation(
        rows,
        crfd_rows=_crfd_rows(relations=("above_avwap", "below_avwap"), count=2),
        generated_at=NOW,
        rows_path="rows.jsonl",
        crfd_rows_path="crfd.jsonl",
    )

    policy = report["policy_matrix"]["avwap_confirmation_london_open"]
    assert policy["avoided_losers"] == 1
    assert policy["missed_winners"] == 0


def test_low_sample_warnings(tmp_path: Path) -> None:
    rows_path = tmp_path / "rows.jsonl"
    crfd_path = tmp_path / "crfd.jsonl"
    rows_path.write_text("", encoding="utf-8")
    crfd_path.write_text("", encoding="utf-8")

    result = run_gre_shadow_gate_simulator(
        output_root=tmp_path / "outputs" / "track_b_execution_core",
        now=NOW,
        rows_path=rows_path,
        crfd_rows_path=crfd_path,
    )

    assert result.report["baseline"]["sample_size_warning"] is True
    assert result.report["recommendations"]["production_gate_recommended_now"] is False
    assert result.json_path.exists()
    assert result.policy_matrix_path.exists()


def test_shadow_gate_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_gre_shadow_gate_simulator.py"),
        Path("src/mgc_v05l/app/track_b_gre_shadow_gate_simulator.py"),
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


def _row(regime: str, confidence: int, forward_60m: float, *, ts: datetime = START) -> dict:
    return {
        "gre_generated_at": ts.isoformat(),
        "contract": "GC",
        "regime_label": regime,
        "confidence": confidence,
        "validation_status": "VALIDATED",
        "forward_returns": {"5m": forward_60m / 4, "15m": forward_60m / 2, "30m": forward_60m * 0.75, "60m": forward_60m},
        "mfe": abs(forward_60m) + 1,
        "mae": -abs(forward_60m) / 2,
        "direction_correctness": (forward_60m > 0 if regime == "LONG" else forward_60m < 0 if regime == "SHORT" else None),
    }


def _crfd_rows(*, count: int = 2, relations: tuple[str, ...] = ("above_avwap", "below_avwap")) -> list[dict]:
    rows: list[dict] = []
    for idx in range(count):
        ts = START + timedelta(minutes=idx * 5)
        relation = relations[min(idx, len(relations) - 1)]
        row = {"observation_time": ts.isoformat(), "contract": "GC"}
        for anchor in ("globex_session_open_18et", "london_open", "us_rth_open"):
            row[f"has_avwap_{anchor}"] = True
            row[f"avwap_relation_{anchor}"] = relation
        rows.append(row)
    return rows
