from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.approved_quant_materialization_repair import _current_wrong_session_rows


def test_current_wrong_session_rows_marks_remaining_london_late_blocks_as_intentional(tmp_path: Path) -> None:
    payload = {
        "lanes": [
            {
                "lane_id": "gc_lane",
                "symbol": "GC",
                "approved_long_entry_sources": ["asiaEarlyNormalBreakoutRetestHoldTurn"],
                "approved_short_entry_sources": [],
                "session_restriction": "ASIA_EARLY",
                "current_detected_session": "LONDON_LATE",
                "eligibility_reason": "wrong_session",
            },
            {
                "lane_id": "mgc_lane",
                "symbol": "MGC",
                "approved_long_entry_sources": ["usLatePauseResumeLongTurn"],
                "approved_short_entry_sources": [],
                "session_restriction": "US_LATE",
                "current_detected_session": "LONDON_LATE",
                "eligibility_reason": "wrong_session",
            },
        ]
    }
    path = tmp_path / "operator_status.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    rows = _current_wrong_session_rows(operator_status_path=path)

    assert [row["classification"] for row in rows] == [
        "intentional_and_correct",
        "intentional_and_correct",
    ]
