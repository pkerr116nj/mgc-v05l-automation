import csv
import json
from pathlib import Path

from mgc_v05l.app.us_late_pause_resume_long_carryover_robustness import (
    build_and_write_us_late_pause_resume_long_carryover_robustness,
)


def test_carryover_robustness_splits_standard_vs_1755_trades(tmp_path: Path) -> None:
    trades_path = tmp_path / "family_trades.csv"
    fieldnames = ["entry_ts", "exit_ts", "entry_session", "entry_session_phase", "net_pnl"]
    with trades_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(
            [
                {
                    "entry_ts": "2025-10-02T14:00:00-04:00",
                    "exit_ts": "2025-10-02T14:35:00-04:00",
                    "entry_session": "US",
                    "entry_session_phase": "US_LATE",
                    "net_pnl": "13.0",
                },
                {
                    "entry_ts": "2026-02-22T17:55:00-05:00",
                    "exit_ts": "2026-02-22T18:30:00-05:00",
                    "entry_session": "ASIA",
                    "entry_session_phase": "UNCLASSIFIED",
                    "net_pnl": "401.0",
                },
            ]
        )

    outputs = build_and_write_us_late_pause_resume_long_carryover_robustness(
        family_trades_csv_path=trades_path
    )
    summary = json.loads(Path(outputs["carryover_robustness_summary_path"]).read_text(encoding="utf-8"))
    assert summary["standard_us_fill"]["trade_count"] == 1
    assert summary["standard_us_fill"]["total_net_pnl"] == 13.0
    assert summary["carryover_1755"]["trade_count"] == 1
    assert summary["carryover_1755"]["total_net_pnl"] == 401.0
    assert summary["recommendation_hint"] == "promote_only_with_carryover_rule"
