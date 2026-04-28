from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.atp_companion_replay_baseline_v2_legacy_intent import (
    _classification,
    _support_matrix_rows,
    _validation_observed_from_replay_jsonl,
)


def test_support_matrix_marks_documented_3m_context_as_partially_supported() -> None:
    rows = {row["semantic_field"]: row for row in _support_matrix_rows()}

    assert rows["quality_bucket_policy"]["support_status"] == "SUPPORTED"
    assert rows["allow_pre_5m_context_participation"]["support_status"] == "SUPPORTED"
    assert rows["atp_context_timeframe"]["support_status"] == "PARTIALLY_SUPPORTED"
    assert rows["entry_activation_basis"]["support_status"] == "AMBIGUOUS"


def test_classification_returns_partial_match_when_legacy_closes_but_does_not_halve_gap() -> None:
    classification = _classification(
        old_net=100.0,
        current_net=40.0,
        legacy_net=55.0,
        current_matched_delta=-60.0,
        legacy_matched_delta=-45.0,
    )

    assert classification == "LEGACY_INTENT_REPLAY_BUILT_PARTIAL_MATCH"


def test_validation_observed_from_replay_jsonl_reads_exported_trade_rows(tmp_path: Path) -> None:
    replay_path = tmp_path / "replay.jsonl"
    row = {
        "trade_id": "trade-1",
        "trade_record": {
            "entry_ts": "2024-01-02T09:35:00-05:00",
            "exit_ts": "2024-01-02T10:05:00-05:00",
            "decision_ts": "2024-01-02T09:35:00-05:00",
            "entry_price": 100.0,
            "exit_price": 102.0,
            "stop_price": 99.0,
            "pnl_cash": 12.5,
            "gross_pnl_cash": 19.0,
            "mfe_points": 2.0,
            "mae_points": 0.5,
            "hold_minutes": 30.0,
            "bars_held_1m": 30,
            "side": "LONG",
            "session_segment": "US",
            "family": "family",
            "exit_reason": "target",
        },
    }
    replay_path.write_text(json.dumps(row) + "\n", encoding="utf-8")

    observed = _validation_observed_from_replay_jsonl(replay_path)

    assert observed["trade_count"] == 1
    assert observed["net_pnl_cash"] == 12.5
    assert observed["gross_pnl_cash"] == 19.0
    assert observed["us_net_pnl_cash"] == 12.5
    assert observed["exit_reason_distribution_json"] == '{"target": 1}'
