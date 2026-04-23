from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.atp_gc_7m_maintenance_study import _control_reconciliation_status


def test_control_reconciliation_status_marks_clean_reproduction(tmp_path: Path) -> None:
    reference_path = tmp_path / "reference.json"
    reference_payload = {
        "results": [
            {
                "variant_id": "checkpoint100_no_traction_abort_15m_2of3",
                "metrics": {
                    "net_pnl_cash": 10.0,
                    "max_drawdown": 2.0,
                    "profit_factor": 1.1,
                    "win_rate": 0.4,
                    "expectancy": 1.5,
                    "average_hold_minutes": 8.0,
                    "median_hold_minutes": 2.0,
                    "trade_count": 5,
                },
            },
            {
                "variant_id": "checkpoint075_no_traction_abort_10m_double_weak_close",
                "metrics": {
                    "net_pnl_cash": 8.0,
                    "max_drawdown": 3.0,
                    "profit_factor": 1.0,
                    "win_rate": 0.35,
                    "expectancy": 1.0,
                    "average_hold_minutes": 7.0,
                    "median_hold_minutes": 2.0,
                    "trade_count": 5,
                },
            },
        ]
    }
    reference_path.write_text(json.dumps(reference_payload), encoding="utf-8")

    status = _control_reconciliation_status(
        scope_bundle_manifest=Path("outputs/research_platform/atp_substrate/scope_bundles/7c367b0af017569b/manifest.json"),
        reference_exit_evolution_json=reference_path,
        control_15m={
            "metrics": reference_payload["results"][0]["metrics"],
        },
        control_10m={
            "metrics": reference_payload["results"][1]["metrics"],
        },
    )

    assert status["control_reproduced_cleanly"]["answer"] == "yes"
    assert status["reproduction_grade"] == "decision-grade"


def test_control_reconciliation_status_marks_missing_reference_exploratory(tmp_path: Path) -> None:
    status = _control_reconciliation_status(
        scope_bundle_manifest=Path("outputs/research_platform/atp_substrate/scope_bundles/7c367b0af017569b/manifest.json"),
        reference_exit_evolution_json=tmp_path / "missing.json",
        control_15m={"metrics": {}},
        control_10m=None,
    )

    assert status["control_reproduced_cleanly"]["answer"] == "no"
    assert status["reproduction_grade"] == "exploratory"
