from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from mgc_v05l.research.asia_drift.shadow_ops import run_asia_drift_shadow_ops
from mgc_v05l.research.trend_participation.storage import materialize_parquet_dataset
from mgc_v05l.research.warehouse_historical_evaluator.layout import build_layout as build_warehouse_layout


def _seed_warehouse(root: Path) -> None:
    layout = build_warehouse_layout(root)
    placeholder = [{"symbol": "ES", "ts": "2026-04-21T00:00:00+00:00"}]
    for dataset in ("derived_bars_5m", "derived_bars_15m", "derived_bars_60m", "derived_bars_240m", "derived_bars_daily"):
        materialize_parquet_dataset(layout[dataset] / "part.parquet", placeholder)
    materialize_parquet_dataset(
        layout["vol_regime_daily"] / "part.parquet",
        [
            {
                "vix_trade_date": "2026-04-21",
                "vix_asof_ts": "2026-04-21T20:15:00+00:00",
                "vix_close": 18.0,
                "vix_change_abs": 0.1,
                "vix_change_pct": 0.01,
                "vix_level_bucket": "MID",
                "vix_change_bucket": "UP",
                "vix_combined_bucket": "MID_UP",
            }
        ],
    )


def _seed_classification(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("environment_key,classification\nx,TRADE_FAVORABLE\n", encoding="utf-8")


def test_shadow_ops_end_to_end_with_stubbed_shadow_and_report(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    warehouse_root = tmp_path / "warehouse"
    _seed_warehouse(warehouse_root)
    classification = tmp_path / "classification.csv"
    _seed_classification(classification)

    def fake_shadow(**kwargs):
        out = kwargs["output_dir"]
        out.mkdir(parents=True, exist_ok=True)
        return {
            "artifacts": {"trade_csv": str(out / "reports" / "trade.csv")},
            "summary": {"row_counts": {"candidate_rows": 10, "simulated_trade_rows": 2}, "scope": {"symbols": ["ES"]}},
        }

    def fake_report(**kwargs):
        out = kwargs["output_dir"]
        out.mkdir(parents=True, exist_ok=True)
        return {"artifacts": {"status_json": str(out / "reports" / "status.json")}, "row_counts": {"daily_rows": 2, "rolling_rows": 3}, "process_status": "healthy"}

    monkeypatch.setattr("mgc_v05l.research.asia_drift.shadow_ops.run_asia_drift_shadow_trading", fake_shadow)
    monkeypatch.setattr("mgc_v05l.research.asia_drift.shadow_ops.run_asia_drift_shadow_report", fake_report)

    payload = run_asia_drift_shadow_ops(
        start_date=date(2026, 4, 21),
        end_date=date(2026, 4, 21),
        latest_session=False,
        warehouse_root=warehouse_root,
        pass6_classification_csv=classification,
        shadow_output_dir=tmp_path / "shadow",
        report_output_dir=tmp_path / "report",
        health_output_dir=tmp_path / "health",
        overwrite=True,
    )
    assert payload["overall_status"] == "healthy"
    assert Path(payload["artifacts"]["summary_json"]).exists()


def test_shadow_ops_health_failure_on_missing_dataset(tmp_path: Path) -> None:
    warehouse_root = tmp_path / "warehouse"
    warehouse_root.mkdir(parents=True, exist_ok=True)
    classification = tmp_path / "classification.csv"
    _seed_classification(classification)
    payload = run_asia_drift_shadow_ops(
        start_date=date(2026, 4, 21),
        end_date=date(2026, 4, 21),
        latest_session=False,
        warehouse_root=warehouse_root,
        pass6_classification_csv=classification,
        shadow_output_dir=tmp_path / "shadow",
        report_output_dir=tmp_path / "report",
        health_output_dir=tmp_path / "health",
    )
    assert payload["overall_status"] == "failed"


def test_shadow_ops_propagates_shadow_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    warehouse_root = tmp_path / "warehouse"
    _seed_warehouse(warehouse_root)
    classification = tmp_path / "classification.csv"
    _seed_classification(classification)

    def fake_shadow(**kwargs):
        raise RuntimeError("shadow broke")

    monkeypatch.setattr("mgc_v05l.research.asia_drift.shadow_ops.run_asia_drift_shadow_trading", fake_shadow)

    with pytest.raises(RuntimeError):
        run_asia_drift_shadow_ops(
            start_date=date(2026, 4, 21),
            end_date=date(2026, 4, 21),
            latest_session=False,
            warehouse_root=warehouse_root,
            pass6_classification_csv=classification,
            shadow_output_dir=tmp_path / "shadow",
            report_output_dir=tmp_path / "report",
            health_output_dir=tmp_path / "health",
            overwrite=True,
        )


def test_shadow_ops_propagates_report_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    warehouse_root = tmp_path / "warehouse"
    _seed_warehouse(warehouse_root)
    classification = tmp_path / "classification.csv"
    _seed_classification(classification)

    def fake_shadow(**kwargs):
        return {
            "artifacts": {},
            "summary": {"row_counts": {"candidate_rows": 1, "simulated_trade_rows": 1}, "scope": {}},
        }

    def fake_report(**kwargs):
        raise RuntimeError("report broke")

    monkeypatch.setattr("mgc_v05l.research.asia_drift.shadow_ops.run_asia_drift_shadow_trading", fake_shadow)
    monkeypatch.setattr("mgc_v05l.research.asia_drift.shadow_ops.run_asia_drift_shadow_report", fake_report)

    with pytest.raises(RuntimeError):
        run_asia_drift_shadow_ops(
            start_date=date(2026, 4, 21),
            end_date=date(2026, 4, 21),
            latest_session=False,
            warehouse_root=warehouse_root,
            pass6_classification_csv=classification,
            shadow_output_dir=tmp_path / "shadow",
            report_output_dir=tmp_path / "report",
            health_output_dir=tmp_path / "health",
            overwrite=True,
        )


def test_shadow_ops_warning_propagation_with_fail_on_warning(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    warehouse_root = tmp_path / "warehouse"
    _seed_warehouse(warehouse_root)
    classification = tmp_path / "classification.csv"
    _seed_classification(classification)

    def fake_shadow(**kwargs):
        return {
            "artifacts": {},
            "summary": {"row_counts": {"candidate_rows": 1, "simulated_trade_rows": 1}, "scope": {}},
        }

    def fake_report(**kwargs):
        return {"artifacts": {}, "row_counts": {"daily_rows": 1, "rolling_rows": 1}, "process_status": "warning"}

    monkeypatch.setattr("mgc_v05l.research.asia_drift.shadow_ops.run_asia_drift_shadow_trading", fake_shadow)
    monkeypatch.setattr("mgc_v05l.research.asia_drift.shadow_ops.run_asia_drift_shadow_report", fake_report)

    payload = run_asia_drift_shadow_ops(
        start_date=date(2026, 4, 21),
        end_date=date(2026, 4, 21),
        latest_session=False,
        warehouse_root=warehouse_root,
        pass6_classification_csv=classification,
        shadow_output_dir=tmp_path / "shadow",
        report_output_dir=tmp_path / "report",
        health_output_dir=tmp_path / "health",
        overwrite=True,
        fail_on_warning=True,
    )
    assert payload["overall_status"] == "failed"


def test_shadow_ops_overwrite_protection(tmp_path: Path) -> None:
    warehouse_root = tmp_path / "warehouse"
    _seed_warehouse(warehouse_root)
    classification = tmp_path / "classification.csv"
    _seed_classification(classification)
    shadow_dir = tmp_path / "shadow"
    (shadow_dir / "existing.txt").parent.mkdir(parents=True, exist_ok=True)
    (shadow_dir / "existing.txt").write_text("x", encoding="utf-8")
    payload = run_asia_drift_shadow_ops(
        start_date=date(2026, 4, 21),
        end_date=date(2026, 4, 21),
        latest_session=False,
        warehouse_root=warehouse_root,
        pass6_classification_csv=classification,
        shadow_output_dir=shadow_dir,
        report_output_dir=tmp_path / "report",
        health_output_dir=tmp_path / "health",
        overwrite=False,
    )
    assert payload["overall_status"] == "failed"
