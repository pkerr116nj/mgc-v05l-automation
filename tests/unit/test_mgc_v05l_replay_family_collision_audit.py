import csv
import json
from pathlib import Path

from mgc_v05l.app.replay_family_collision_audit import build_and_write_family_collision_audit


def test_family_collision_audit_flags_structural_overlap(tmp_path: Path) -> None:
    control_ledger = tmp_path / "control.trade_ledger.csv"
    treatment_ledger = tmp_path / "treatment.trade_ledger.csv"
    control_summary = tmp_path / "control.summary.json"
    treatment_summary = tmp_path / "treatment.summary.json"

    fieldnames = [
        "trade_id",
        "direction",
        "entry_ts",
        "entry_px",
        "exit_ts",
        "exit_px",
        "qty",
        "gross_pnl",
        "fees",
        "slippage",
        "net_pnl",
        "exit_reason",
        "setup_family",
        "entry_session",
        "entry_session_phase",
        "exit_session",
        "exit_session_phase",
        "mae",
        "mfe",
        "bars_held",
        "time_to_mfe",
        "time_to_mae",
        "mfe_capture_pct",
        "entry_efficiency_3",
        "entry_efficiency_5",
        "entry_efficiency_10",
        "initial_adverse_3bar",
        "initial_favorable_3bar",
        "entry_distance_fast_ema_atr",
        "entry_distance_slow_ema_atr",
        "entry_distance_vwap_atr",
    ]

    def write_ledger(path: Path, rows: list[dict[str, str]]) -> None:
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    write_ledger(
        control_ledger,
        [
            {
                "trade_id": "1",
                "direction": "LONG",
                "entry_ts": "2025-12-01T18:50:00-05:00",
                "entry_px": "1",
                "exit_ts": "2025-12-01T18:55:00-05:00",
                "exit_px": "1",
                "qty": "1",
                "gross_pnl": "-21",
                "fees": "0",
                "slippage": "0",
                "net_pnl": "-21",
                "exit_reason": "LONG_INTEGRITY_FAIL",
                "setup_family": "firstBullSnapTurn",
                "entry_session": "ASIA",
                "entry_session_phase": "ASIA_EARLY",
                "exit_session": "ASIA",
                "exit_session_phase": "ASIA_EARLY",
                "mae": "3.7",
                "mfe": "0.5",
                "bars_held": "1",
                "time_to_mfe": "1",
                "time_to_mae": "1",
                "mfe_capture_pct": "0",
                "entry_efficiency_3": "0",
                "entry_efficiency_5": "0",
                "entry_efficiency_10": "0",
                "initial_adverse_3bar": "0",
                "initial_favorable_3bar": "0",
                "entry_distance_fast_ema_atr": "0",
                "entry_distance_slow_ema_atr": "0",
                "entry_distance_vwap_atr": "0",
            }
        ],
    )
    write_ledger(
        treatment_ledger,
        [
            {
                "trade_id": "1",
                "direction": "SHORT",
                "entry_ts": "2025-12-01T18:30:00-05:00",
                "entry_px": "1",
                "exit_ts": "2025-12-01T18:55:00-05:00",
                "exit_px": "1",
                "qty": "1",
                "gross_pnl": "56",
                "fees": "0",
                "slippage": "0",
                "net_pnl": "56",
                "exit_reason": "SHORT_TIME_EXIT",
                "setup_family": "asiaEarlyPauseResumeShortTurn",
                "entry_session": "ASIA",
                "entry_session_phase": "ASIA_EARLY",
                "exit_session": "ASIA",
                "exit_session_phase": "ASIA_EARLY",
                "mae": "1.2",
                "mfe": "9.1",
                "bars_held": "5",
                "time_to_mfe": "4",
                "time_to_mae": "1",
                "mfe_capture_pct": "0",
                "entry_efficiency_3": "0",
                "entry_efficiency_5": "0",
                "entry_efficiency_10": "0",
                "initial_adverse_3bar": "0",
                "initial_favorable_3bar": "0",
                "entry_distance_fast_ema_atr": "0",
                "entry_distance_slow_ema_atr": "0",
                "entry_distance_vwap_atr": "0",
            }
        ],
    )

    control_summary.write_text(json.dumps({"trade_ledger_path": str(control_ledger)}), encoding="utf-8")
    treatment_summary.write_text(json.dumps({"trade_ledger_path": str(treatment_ledger)}), encoding="utf-8")

    outputs = build_and_write_family_collision_audit(
        control_summary_path=control_summary,
        treatment_summary_path=treatment_summary,
        target_family="firstBullSnapTurn",
        new_family="asiaEarlyPauseResumeShortTurn",
    )

    summary = json.loads(Path(outputs["family_collision_audit_summary_path"]).read_text(encoding="utf-8"))
    assert summary["interaction_verdict"] == "structural"
    assert summary["target_family_pnl_delta"] == 21.0
    assert summary["changed_trade_count"] == 1

    detail_path = Path(outputs["family_collision_audit_detail_path"])
    detail_rows = list(csv.DictReader(detail_path.open(encoding="utf-8", newline="")))
    assert detail_rows[0]["change_type"] == "removed_in_treatment"
    assert detail_rows[0]["interaction_type"] == "structural_position_conflict"
    assert detail_rows[0]["overlap_count"] == "1"
