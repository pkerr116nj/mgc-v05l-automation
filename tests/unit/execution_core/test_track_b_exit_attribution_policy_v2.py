from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution_core.track_b_exit_attribution_policy_v2 import (
    ENTRY_GOOD_EXIT_TOO_LATE,
    ENTRY_GOOD_ORDER_MANAGEMENT_BAD,
    ExitPolicyV2Config,
    run_exit_attribution_policy_v2,
)


def test_exit_attribution_detects_giveback_and_shadow_outputs_are_non_authoritative(tmp_path: Path) -> None:
    _write_ledger(
        tmp_path,
        [
            {
                "trade_id": "t1",
                "lifecycle_id": "l1",
                "strategy_id": "MNQ_FIRST_BEAR_SNAP_TURN_V1",
                "lane_id": "mnq_first_bear_snap_turn",
                "instrument_family": "MNQ",
                "local_symbol": "MNQM6",
                "side": "LONG",
                "quantity": "1",
                "entry_fill_confirmed": True,
                "exit_fill_confirmed": True,
                "entry_fill_price": "100",
                "exit_fill_price": "101",
                "entry_timestamp": "2026-05-26T10:00:00+00:00",
                "exit_timestamp": "2026-05-26T10:20:00+00:00",
                "final_position_status": "CLOSED_FLAT",
                "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
            }
        ],
    )
    _write_candles(
        tmp_path,
        "MNQ",
        [
            ("2026-05-26T10:00:00+00:00", "2026-05-26T10:05:00+00:00", 100, 104, 99, 103),
            ("2026-05-26T10:05:00+00:00", "2026-05-26T10:10:00+00:00", 103, 106, 102, 105),
            ("2026-05-26T10:10:00+00:00", "2026-05-26T10:15:00+00:00", 105, 106, 101, 102),
            ("2026-05-26T10:15:00+00:00", "2026-05-26T10:20:00+00:00", 102, 103, 100, 101),
        ],
    )

    report = run_exit_attribution_policy_v2(config=ExitPolicyV2Config(repo_root=tmp_path))

    attribution = _read_json(tmp_path / "outputs/track_b_execution_core/diagnostics/latest_exit_attribution_review.json")
    shadow = _read_json(tmp_path / "outputs/track_b_execution_core/research_shadow/latest_exit_policy_v2_shadow.json")
    assert report["classification"] == "EXIT_ATTRIBUTION_POLICY_V2_READY"
    assert attribution["trades"][0]["mfe_points"] == "6"
    assert attribution["trades"][0]["giveback_from_mfe_points"] == "5"
    assert attribution["trades"][0]["classification"] == ENTRY_GOOD_EXIT_TOO_LATE
    assert shadow["submit_allowed"] is False
    assert shadow["broker_mutation_allowed"] is False
    assert shadow["lifecycle_authority"] is False
    assert shadow["shadow_evaluations"][0]["recommendation"] == "SHADOW_POLICY_COMPARISON_WORTH_COLLECTING"


def test_missing_exit_fill_price_is_order_management_bad(tmp_path: Path) -> None:
    _write_ledger(
        tmp_path,
        [
            {
                "trade_id": "t2",
                "lifecycle_id": "l2",
                "strategy_id": "MNQ_FIRST_BEAR_SNAP_TURN_V1",
                "instrument_family": "MNQ",
                "side": "SHORT",
                "quantity": "1",
                "entry_fill_confirmed": True,
                "exit_fill_confirmed": True,
                "entry_fill_price": "100",
                "exit_fill_price": None,
                "entry_timestamp": "2026-05-26T10:00:00+00:00",
                "exit_timestamp": "2026-05-26T10:05:00+00:00",
                "final_position_status": "CLOSED_FLAT",
            }
        ],
    )
    _write_candles(tmp_path, "MNQ", [("2026-05-26T10:00:00+00:00", "2026-05-26T10:05:00+00:00", 100, 101, 99, 100)])

    run_exit_attribution_policy_v2(config=ExitPolicyV2Config(repo_root=tmp_path))

    attribution = _read_json(tmp_path / "outputs/track_b_execution_core/diagnostics/latest_exit_attribution_review.json")
    assert attribution["trades"][0]["classification"] == ENTRY_GOOD_ORDER_MANAGEMENT_BAD
    assert "exit_fill_price_missing_after_broker_effect_close" in attribution["trades"][0]["classification_reasons"]


def _write_ledger(tmp_path: Path, rows: list[dict]) -> None:
    path = tmp_path / "outputs/track_b_execution_core/paper_trade_ledger/track_b_paper_trade_ledger.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def _write_candles(tmp_path: Path, instrument: str, rows: list[tuple]) -> None:
    path = tmp_path / "outputs/track_b_execution_core/phase1_runtime_market_data" / instrument / "5m/latest_runtime_candles.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "bars": [
                    {
                        "bar_start": start,
                        "bar_end": end,
                        "open": open_,
                        "high": high,
                        "low": low,
                        "close": close,
                    }
                    for start, end, open_, high, low, close in rows
                ]
            }
        ),
        encoding="utf-8",
    )


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))
