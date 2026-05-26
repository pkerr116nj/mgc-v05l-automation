from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution_core.track_b_exit_attribution_policy_v2 import (
    ALPHA_EXIT,
    BUG_FIX_EXIT,
    DIRECT_EXECUTION_FILL,
    ENTRY_GOOD_EXIT_TOO_LATE,
    INCOMPLETE_FILL_EVIDENCE,
    ExitPolicyV2Config,
    UNKNOWN_EXIT_INTENT,
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
                "entry_exec_id": "entry-exec-1",
                "exit_exec_id": "exit-exec-1",
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
    assert attribution["trades"][0]["exit_intent_category"] == ALPHA_EXIT
    assert attribution["summary"]["alpha_exit_quality_eligible_count"] == 1
    assert attribution["summary"]["bug_fix_exit_count"] == 0
    assert attribution["trades"][0]["fill_reconstruction"]["entry_confidence"] == DIRECT_EXECUTION_FILL
    assert attribution["trades"][0]["fill_reconstruction"]["exit_confidence"] == DIRECT_EXECUTION_FILL
    assert shadow["submit_allowed"] is False
    assert shadow["broker_mutation_allowed"] is False
    assert shadow["lifecycle_authority"] is False
    assert shadow["shadow_evaluations"][0]["recommendation"] == "SHADOW_POLICY_COMPARISON_WORTH_COLLECTING"


def test_broker_flat_no_price_does_not_invent_exit_price(tmp_path: Path) -> None:
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
                "broker_reconciled": True,
            }
        ],
    )
    _write_candles(tmp_path, "MNQ", [("2026-05-26T10:00:00+00:00", "2026-05-26T10:05:00+00:00", 100, 101, 99, 100)])

    run_exit_attribution_policy_v2(config=ExitPolicyV2Config(repo_root=tmp_path))

    attribution = _read_json(tmp_path / "outputs/track_b_execution_core/diagnostics/latest_exit_attribution_review.json")
    reconstruction = _read_json(
        tmp_path / "outputs/track_b_execution_core/diagnostics/latest_closed_trade_fill_reconstruction.json"
    )
    assert reconstruction["trades"][0]["exit_confidence"] == "BROKER_FLAT_NO_PRICE"
    assert reconstruction["trades"][0]["exit_fill_price"] is None
    assert reconstruction["trades"][0]["realized_points"] is None
    assert attribution["trades"][0]["classification"] == INCOMPLETE_FILL_EVIDENCE
    assert "exit_fill_price_missing" in attribution["trades"][0]["classification_reasons"]


def test_order_status_fill_reconstructs_exit_price_from_matching_artifact(tmp_path: Path) -> None:
    _write_ledger(
        tmp_path,
        [
            {
                "trade_id": "t3",
                "lifecycle_id": "l3",
                "strategy_id": "MNQ_FIRST_BEAR_SNAP_TURN_V1",
                "instrument_family": "MNQ",
                "side": "LONG",
                "quantity": "1",
                "entry_fill_confirmed": True,
                "exit_fill_confirmed": True,
                "entry_fill_price": "100",
                "exit_fill_price": None,
                "entry_timestamp": "2026-05-26T10:00:00+00:00",
                "exit_timestamp": "2026-05-26T10:05:00+00:00",
                "exit_order_id": "42",
                "filled_bridge_close_result_path": str(
                    tmp_path / "outputs/track_b_execution_core/managed_exit_order_resolution/order_42.json"
                ),
                "final_position_status": "CLOSED_FLAT",
            }
        ],
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/managed_exit_order_resolution/order_42.json",
        {
            "lifecycle_id": "l3",
            "order_id": "42",
            "order_status": "Filled",
            "fill_price": "102.25",
            "fill_timestamp": "2026-05-26T10:05:00+00:00",
            "source": "managed_exit_order_resolution",
        },
    )
    _write_candles(tmp_path, "MNQ", [("2026-05-26T10:00:00+00:00", "2026-05-26T10:05:00+00:00", 100, 103, 99, 102)])

    run_exit_attribution_policy_v2(config=ExitPolicyV2Config(repo_root=tmp_path))

    reconstruction = _read_json(
        tmp_path / "outputs/track_b_execution_core/diagnostics/latest_closed_trade_fill_reconstruction.json"
    )
    attribution = _read_json(tmp_path / "outputs/track_b_execution_core/diagnostics/latest_exit_attribution_review.json")
    assert reconstruction["summary"]["complete_fill_evidence_count"] == 1
    assert reconstruction["trades"][0]["exit_confidence"] == "ORDER_STATUS_FILL"
    assert reconstruction["trades"][0]["exit_fill_price"] == "102.25"
    assert reconstruction["trades"][0]["realized_points"] == "2.25"
    assert attribution["trades"][0]["classification"] != INCOMPLETE_FILL_EVIDENCE


def test_remediation_close_is_bug_fix_exit_and_excluded_from_alpha_metrics(tmp_path: Path) -> None:
    _write_ledger(
        tmp_path,
        [
            {
                "trade_id": "t4",
                "lifecycle_id": "l4",
                "strategy_id": "MNQ_FIRST_BEAR_SNAP_TURN_V1",
                "instrument_family": "MNQ",
                "side": "SHORT",
                "quantity": "5",
                "entry_fill_confirmed": True,
                "exit_fill_confirmed": True,
                "entry_fill_price": "100",
                "exit_fill_price": "98",
                "entry_timestamp": "2026-05-26T10:00:00+00:00",
                "exit_timestamp": "2026-05-26T10:15:00+00:00",
                "final_position_status": "CLOSED_FLAT",
                "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
                "source": "aggregate position_unit remediation cleanup",
            }
        ],
    )
    _write_candles(
        tmp_path,
        "MNQ",
        [
            ("2026-05-26T10:00:00+00:00", "2026-05-26T10:05:00+00:00", 100, 101, 97, 98),
            ("2026-05-26T10:05:00+00:00", "2026-05-26T10:10:00+00:00", 98, 99, 96, 97),
            ("2026-05-26T10:10:00+00:00", "2026-05-26T10:15:00+00:00", 97, 99, 97, 98),
        ],
    )

    run_exit_attribution_policy_v2(config=ExitPolicyV2Config(repo_root=tmp_path))

    attribution = _read_json(tmp_path / "outputs/track_b_execution_core/diagnostics/latest_exit_attribution_review.json")
    shadow = _read_json(tmp_path / "outputs/track_b_execution_core/research_shadow/latest_exit_policy_v2_shadow.json")
    reconstruction = _read_json(
        tmp_path / "outputs/track_b_execution_core/diagnostics/latest_closed_trade_fill_reconstruction.json"
    )
    assert reconstruction["trades"][0]["exit_intent_category"] == BUG_FIX_EXIT
    assert reconstruction["trades"][0]["contamination_flags"]["remediation_trade"] is True
    assert reconstruction["trades"][0]["contamination_flags"]["aggregate_close_repair"] is True
    assert attribution["summary"]["bug_fix_exit_count"] == 1
    assert attribution["summary"]["alpha_exit_quality_eligible_count"] == 0
    assert shadow["shadow_evaluations"][0]["excluded_from_alpha_exit_policy"] is True
    assert shadow["shadow_evaluations"][0]["recommendation"] == f"EXCLUDED_{BUG_FIX_EXIT}"


def test_unknown_exit_intent_does_not_contaminate_alpha_metrics(tmp_path: Path) -> None:
    _write_ledger(
        tmp_path,
        [
            {
                "trade_id": "t5",
                "lifecycle_id": "l5",
                "strategy_id": "UNKNOWN_STRATEGY",
                "instrument_family": "MGC",
                "side": "LONG",
                "quantity": "1",
                "entry_fill_confirmed": True,
                "exit_fill_confirmed": True,
                "entry_fill_price": "4500",
                "exit_fill_price": "4510",
                "entry_timestamp": "2026-05-26T10:00:00+00:00",
                "exit_timestamp": "2026-05-26T10:05:00+00:00",
                "final_position_status": "CLOSED_FLAT",
            }
        ],
    )
    _write_candles(tmp_path, "MGC", [("2026-05-26T10:00:00+00:00", "2026-05-26T10:05:00+00:00", 4500, 4510, 4498, 4510)])

    run_exit_attribution_policy_v2(config=ExitPolicyV2Config(repo_root=tmp_path))

    attribution = _read_json(tmp_path / "outputs/track_b_execution_core/diagnostics/latest_exit_attribution_review.json")
    assert attribution["trades"][0]["exit_intent_category"] == UNKNOWN_EXIT_INTENT
    assert attribution["summary"]["unknown_exit_intent_count"] == 1
    assert attribution["summary"]["alpha_exit_quality_eligible_count"] == 0


def _write_ledger(tmp_path: Path, rows: list[dict]) -> None:
    path = tmp_path / "outputs/track_b_execution_core/paper_trade_ledger/track_b_paper_trade_ledger.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


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
