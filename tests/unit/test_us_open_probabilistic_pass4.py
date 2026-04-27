from __future__ import annotations

import csv
from pathlib import Path

from mgc_v05l.research.us_open_follow_through.probabilistic_pass4 import (
    SLICE_UP_OPEN,
    run_probabilistic_pass4,
)


def test_run_probabilistic_pass4_writes_signal_extraction_outputs(tmp_path: Path) -> None:
    pass1_root = tmp_path / "pass1"
    (pass1_root / "features").mkdir(parents=True)
    (pass1_root / "signals").mkdir(parents=True)
    output_dir = tmp_path / "pass4"

    candidate_rows: list[dict[str, object]] = []
    outcome_rows: list[dict[str, object]] = []

    def add_row(
        *,
        candidate_id: str,
        instrument: str,
        sample_split: str,
        direction: str,
        open_size: float,
        overnight_direction: str,
        trend_state: str,
        cross_confirm: bool,
        vix_level: str,
        signed_1530: float,
        signed_close: float,
    ) -> None:
        candidate_rows.append(
            {
                "candidate_id": candidate_id,
                "instrument": instrument,
                "direction": direction,
                "opening_drive_signed_return_points": open_size,
                "overnight_direction": overnight_direction,
                "direction_60m_state": trend_state,
                "cross_index_confirmation": cross_confirm,
            }
        )
        outcome_rows.append(
            {
                "candidate_id": candidate_id,
                "instrument": instrument,
                "sample_split": sample_split,
                "direction": direction,
                "opening_drive_signed_return_points": open_size,
                "vix_level_bucket": vix_level,
                "vix_change_bucket": "FLAT",
                "forward_return_1530": signed_1530,
                "continuation_1530": signed_1530 > 0,
                "reversal_1530": signed_1530 < 0,
                "forward_return_close": signed_close,
                "continuation_close": signed_close > 0,
                "reversal_close": signed_close < 0,
                "mfe_close_points": abs(signed_close) + 2.0,
                "mae_close_points": 3.0,
            }
        )

    for sample_split in ("development", "holdout"):
        count = 24 if sample_split == "development" else 16
        for idx in range(count):
            instrument = "NQ" if idx % 2 == 0 else "MNQ"
            add_row(
                candidate_id=f"{sample_split}|up|{idx}",
                instrument=instrument,
                sample_split=sample_split,
                direction="UP",
                open_size=80.0 if idx < 8 else 15.0,
                overnight_direction="UP",
                trend_state="UP",
                cross_confirm=idx % 3 != 0,
                vix_level="LOW",
                signed_1530=8.0,
                signed_close=12.0,
            )
        for idx in range(count):
            instrument = "NQ" if idx % 2 == 0 else "MNQ"
            add_row(
                candidate_id=f"{sample_split}|down|{idx}",
                instrument=instrument,
                sample_split=sample_split,
                direction="DOWN",
                open_size=10.0,
                overnight_direction="DOWN",
                trend_state="DOWN",
                cross_confirm=idx % 2 == 0,
                vix_level="HIGH",
                signed_1530=-4.0,
                signed_close=-6.0,
            )

    _write_csv(pass1_root / "features" / "us_open_probabilistic_pass1_candidates.csv", candidate_rows)
    _write_csv(pass1_root / "signals" / "us_open_probabilistic_pass1_outcomes.csv", outcome_rows)

    result = run_probabilistic_pass4(pass1_root=pass1_root, output_dir=output_dir)

    assert result["summary"]["blunt_conclusion"]["signal_status"] in {"present", "weak", "absent"}
    assert Path(result["artifacts"]["signal_extraction_csv"]).exists()
    assert Path(result["artifacts"]["baseline_comparison_csv"]).exists()
    assert Path(result["artifacts"]["distribution_csv"]).exists()
    assert Path(result["artifacts"]["summary_markdown"]).exists()

    with Path(result["artifacts"]["signal_extraction_csv"]).open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    up_holdout = next(row for row in rows if row["slice_name"] == SLICE_UP_OPEN and row["sample_split"] == "holdout")
    assert float(up_holdout["avg_return_close"]) > 0.0
    assert float(up_holdout["win_rate_close"]) > 0.5


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
