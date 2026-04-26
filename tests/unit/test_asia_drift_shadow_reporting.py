from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path

from mgc_v05l.research.asia_drift.shadow_reporting import run_asia_drift_shadow_report
from mgc_v05l.research.warehouse_historical_evaluator.layout import build_layout as build_warehouse_layout
from mgc_v05l.research.trend_participation.storage import materialize_parquet_dataset


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _seed_shadow_output(shadow_root: Path) -> None:
    reports = shadow_root / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    candidate_rows = [
        {
            "candidate_id": "c1",
            "trade_date": "2026-04-20",
            "timestamp": "2026-04-19T20:30:00-04:00",
            "instrument": "ES",
            "direction": "LONG",
            "timing_within_asia": "LATE_ASIA",
            "state_classification": "TRADE_FAVORABLE",
            "execution_eligible": "True",
            "entry_price": "100",
            "simulated_trade": "True",
            "skip_reason": "",
            "exit_price": "106",
            "exit_reason": "TIME",
            "return_points": "6",
            "max_favorable_excursion": "8",
            "max_adverse_excursion": "2",
            "stop_points": "6",
            "target_points": "12",
            "exit_ts": "2026-04-19T22:30:00-04:00",
        },
        {
            "candidate_id": "c2",
            "trade_date": "2026-04-21",
            "timestamp": "2026-04-20T20:30:00-04:00",
            "instrument": "NQ",
            "direction": "LONG",
            "timing_within_asia": "LATE_ASIA",
            "state_classification": "TRADE_FAVORABLE",
            "execution_eligible": "False",
            "entry_price": "200",
            "simulated_trade": "False",
            "skip_reason": "first_signal_only_per_instrument_session",
            "exit_price": "",
            "exit_reason": "NOT_SIMULATED",
            "return_points": "",
            "max_favorable_excursion": "",
            "max_adverse_excursion": "",
            "stop_points": "",
            "target_points": "",
            "exit_ts": "",
        },
    ]
    trade_rows = [
        {
            "candidate_id": "c1",
            "trade_date": "2026-04-20",
            "timestamp": "2026-04-19T20:30:00-04:00",
            "instrument": "ES",
            "direction": "LONG",
            "timing_within_asia": "LATE_ASIA",
            "state_classification": "TRADE_FAVORABLE",
            "execution_eligible": "True",
            "entry_price": "100",
            "simulated_trade": "True",
            "skip_reason": "",
            "stop_points": "6",
            "target_points": "12",
            "exit_price": "106",
            "exit_reason": "TIME",
            "exit_ts": "2026-04-19T22:30:00-04:00",
            "return_points": "6",
            "max_favorable_excursion": "8",
            "max_adverse_excursion": "2",
        }
    ]
    _write_csv(reports / "asia_drift_shadow_candidate_log.csv", candidate_rows)
    _write_csv(reports / "asia_drift_shadow_daily_trade_log.csv", trade_rows)
    (reports / "asia_drift_shadow_summary.json").write_text(
        json.dumps({"row_counts": {"candidate_rows": len(candidate_rows), "simulated_trade_rows": len(trade_rows)}}),
        encoding="utf-8",
    )


def _seed_vix_dataset(warehouse_root: Path, latest_trade_date: str) -> None:
    layout = build_warehouse_layout(warehouse_root)
    rows = [
        {
            "vix_trade_date": latest_trade_date,
            "vix_asof_ts": f"{latest_trade_date}T20:15:00+00:00",
            "vix_close": 18.0,
            "vix_change_abs": 0.1,
            "vix_change_pct": 0.01,
            "vix_level_bucket": "MID",
            "vix_change_bucket": "UP",
            "vix_combined_bucket": "MID_UP",
        }
    ]
    materialize_parquet_dataset(layout["vol_regime_daily"] / "vol_regime_daily.parquet", rows)


def test_shadow_report_generates_outputs_from_synthetic_logs(tmp_path: Path) -> None:
    shadow_root = tmp_path / "shadow"
    warehouse_root = tmp_path / "warehouse"
    _seed_shadow_output(shadow_root)
    _seed_vix_dataset(warehouse_root, latest_trade_date="2026-04-21")
    payload = run_asia_drift_shadow_report(
        start_date=date(2026, 4, 20),
        end_date=date(2026, 4, 21),
        output_dir=tmp_path / "report",
        shadow_output_dir=shadow_root,
        warehouse_root=warehouse_root,
    )
    assert Path(payload["artifacts"]["daily_markdown"]).exists()
    assert Path(payload["artifacts"]["weekly_markdown"]).exists()
    assert Path(payload["artifacts"]["status_json"]).exists()


def test_rolling_windows_are_computed_correctly(tmp_path: Path) -> None:
    shadow_root = tmp_path / "shadow"
    reports = shadow_root / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    candidate_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
    for idx in range(6):
        trade_date = date(2026, 4, 13 + idx).isoformat()
        candidate_rows.append(
            {
                "candidate_id": f"c{idx}",
                "trade_date": trade_date,
                "timestamp": f"2026-04-{13 + idx:02d}T20:30:00-04:00",
                "instrument": "ES",
                "direction": "LONG",
                "timing_within_asia": "LATE_ASIA",
                "state_classification": "TRADE_FAVORABLE",
                "execution_eligible": "True",
                "entry_price": "100",
                "simulated_trade": "True",
                "skip_reason": "",
                "exit_price": "106",
                "exit_reason": "TIME",
                "return_points": "6",
                "max_favorable_excursion": "8",
                "max_adverse_excursion": "2",
                "stop_points": "6",
                "target_points": "12",
                "exit_ts": f"2026-04-{13 + idx:02d}T22:30:00-04:00",
            }
        )
        trade_rows.append(
            {
                "candidate_id": f"c{idx}",
                "trade_date": trade_date,
                "timestamp": f"2026-04-{13 + idx:02d}T20:30:00-04:00",
                "instrument": "ES",
                "direction": "LONG",
                "timing_within_asia": "LATE_ASIA",
                "state_classification": "TRADE_FAVORABLE",
                "execution_eligible": "True",
                "entry_price": "100",
                "simulated_trade": "True",
                "skip_reason": "",
                "stop_points": "6",
                "target_points": "12",
                "exit_price": "106",
                "exit_reason": "TIME",
                "exit_ts": f"2026-04-{13 + idx:02d}T22:30:00-04:00",
                "return_points": "6",
                "max_favorable_excursion": "8",
                "max_adverse_excursion": "2",
            }
        )
    _write_csv(reports / "asia_drift_shadow_candidate_log.csv", candidate_rows)
    _write_csv(reports / "asia_drift_shadow_daily_trade_log.csv", trade_rows)
    (reports / "asia_drift_shadow_summary.json").write_text(json.dumps({"row_counts": {"candidate_rows": 6}}), encoding="utf-8")
    warehouse_root = tmp_path / "warehouse"
    _seed_vix_dataset(warehouse_root, latest_trade_date="2026-04-18")
    payload = run_asia_drift_shadow_report(
        start_date=date(2026, 4, 13),
        end_date=date(2026, 4, 18),
        output_dir=tmp_path / "report",
        shadow_output_dir=shadow_root,
        warehouse_root=warehouse_root,
    )
    with Path(payload["artifacts"]["rolling_csv"]).open() as handle:
        rows = list(csv.DictReader(handle))
    last5 = next(row for row in rows if row["window_label"] == "last_5_sessions" and row["instrument"] == "ALL")
    assert last5["trade_count"] == "5"
    assert last5["cumulative_r"] == "5.0"


def test_missing_and_no_signal_days_emit_process_warnings(tmp_path: Path) -> None:
    shadow_root = tmp_path / "shadow"
    warehouse_root = tmp_path / "warehouse"
    _seed_shadow_output(shadow_root)
    _seed_vix_dataset(warehouse_root, latest_trade_date="2026-04-18")
    payload = run_asia_drift_shadow_report(
        start_date=date(2026, 4, 20),
        end_date=date(2026, 4, 21),
        output_dir=tmp_path / "report",
        shadow_output_dir=shadow_root,
        warehouse_root=warehouse_root,
    )
    status = json.loads(Path(payload["artifacts"]["status_json"]).read_text(encoding="utf-8"))
    checks = {check["name"]: check for check in status["checks"]}
    assert checks["stale_vix_regime_data"]["status"] == "warn"
    assert checks["no_signal_days"]["status"] == "warn"
