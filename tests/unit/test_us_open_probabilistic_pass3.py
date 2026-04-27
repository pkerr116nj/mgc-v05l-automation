from __future__ import annotations

import csv
from pathlib import Path

from mgc_v05l.research.us_open_follow_through.probabilistic_pass3 import run_probabilistic_pass3


def test_run_probabilistic_pass3_writes_decision_artifacts(tmp_path: Path) -> None:
    pass1_root = tmp_path / "pass1"
    (pass1_root / "features").mkdir(parents=True)
    (pass1_root / "signals").mkdir(parents=True)
    output_dir = tmp_path / "pass3"

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
        vix_change: str,
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
                "vix_change_bucket": vix_change,
                "forward_return_60m": signed_close / 2.0,
                "continuation_60m": signed_close > 0,
                "reversal_60m": signed_close < 0,
                "forward_return_120m": signed_close / 1.5,
                "continuation_120m": signed_close > 0,
                "reversal_120m": signed_close < 0,
                "forward_return_1530": signed_1530,
                "continuation_1530": signed_1530 > 0,
                "reversal_1530": signed_1530 < 0,
                "forward_return_close": signed_close,
                "continuation_close": signed_close > 0,
                "reversal_close": signed_close < 0,
            }
        )

    def build_ndx_block(sample_split: str, prefix: str, holdout: bool) -> None:
        count = 120 if sample_split == "development" else 40
        for idx in range(count):
            instrument = "NQ" if idx % 2 == 0 else "MNQ"
            add_row(
                candidate_id=f"{prefix}|up_disagree|{idx}",
                instrument=instrument,
                sample_split=sample_split,
                direction="UP",
                open_size=80.0 if idx < max(40, count // 3) else 20.0,
                overnight_direction="DOWN",
                trend_state="UP",
                cross_confirm=True,
                vix_level="LOW",
                vix_change="FLAT",
                signed_1530=16.0,
                signed_close=18.0,
            )
        for idx in range(count):
            instrument = "NQ" if idx % 2 == 0 else "MNQ"
            add_row(
                candidate_id=f"{prefix}|up_aligned|{idx}",
                instrument=instrument,
                sample_split=sample_split,
                direction="UP",
                open_size=12.0,
                overnight_direction="UP",
                trend_state="DOWN",
                cross_confirm=False,
                vix_level="MID",
                vix_change="UP",
                signed_1530=2.0,
                signed_close=2.0,
            )
        down_count = 120 if sample_split == "development" else 40
        for idx in range(down_count):
            instrument = "NQ" if idx % 2 == 0 else "MNQ"
            if idx < int(down_count * 0.45):
                signed_1530 = -1.0
                signed_close = -1.0
            elif idx < int(down_count * 0.80):
                signed_1530 = 2.0
                signed_close = 2.0
            else:
                signed_1530 = 2.0
                signed_close = -8.0
            add_row(
                candidate_id=f"{prefix}|down|{idx}",
                instrument=instrument,
                sample_split=sample_split,
                direction="DOWN",
                open_size=70.0 if idx < max(35, down_count // 4) else 18.0,
                overnight_direction="DOWN" if idx % 2 == 0 else "UP",
                trend_state="DOWN" if idx % 2 == 0 else "UP",
                cross_confirm=idx % 3 != 0,
                vix_level="HIGH" if idx % 4 == 0 else "LOW",
                vix_change="DOWN" if idx % 4 == 0 else "FLAT",
                signed_1530=signed_1530,
                signed_close=signed_close,
            )

    def build_spx_block(sample_split: str, prefix: str) -> None:
        count = 120 if sample_split == "development" else 40
        for idx in range(count):
            instrument = "ES" if idx % 2 == 0 else "MES"
            add_row(
                candidate_id=f"{prefix}|spx_up|{idx}",
                instrument=instrument,
                sample_split=sample_split,
                direction="UP",
                open_size=15.0,
                overnight_direction="UP",
                trend_state="UP",
                cross_confirm=True,
                vix_level="LOW",
                vix_change="FLAT",
                signed_1530=2.0,
                signed_close=1.5,
            )
        for idx in range(count):
            instrument = "ES" if idx % 2 == 0 else "MES"
            add_row(
                candidate_id=f"{prefix}|spx_down|{idx}",
                instrument=instrument,
                sample_split=sample_split,
                direction="DOWN",
                open_size=14.0,
                overnight_direction="DOWN",
                trend_state="DOWN",
                cross_confirm=True,
                vix_level="MID",
                vix_change="DOWN",
                signed_1530=-1.0,
                signed_close=-1.0,
            )

    build_ndx_block("development", "dev", holdout=False)
    build_ndx_block("holdout", "hold", holdout=True)
    build_spx_block("development", "dev")
    build_spx_block("holdout", "hold")

    _write_csv(pass1_root / "features" / "us_open_probabilistic_pass1_candidates.csv", candidate_rows)
    _write_csv(pass1_root / "signals" / "us_open_probabilistic_pass1_outcomes.csv", outcome_rows)

    result = run_probabilistic_pass3(pass1_root=pass1_root, output_dir=output_dir)

    assert result["summary"]["decision"]["family_decision"] in {"PROMOTE_NARROW_LANE", "RETAIN_RESEARCH_ONLY", "KILL"}
    assert result["summary"]["decision"]["blunt_conclusion"]["is_up_drive_only"] in {"YES", "NO"}
    assert Path(result["artifacts"]["decision_summary_csv"]).exists()
    assert Path(result["artifacts"]["summary_markdown"]).exists()

    with Path(result["artifacts"]["decision_summary_csv"]).open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    family_row = next(row for row in rows if row["decision_key"] == "family_decision")
    assert family_row["decision_value"] == result["summary"]["decision"]["family_decision"]


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
