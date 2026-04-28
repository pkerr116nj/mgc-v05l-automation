from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution.ibkr_paper_strategy_exposure import (
    IbkrPaperStrategyExposureConfig,
    evaluate_paper_strategy_exposure_gate,
    run_ibkr_paper_strategy_exposure,
)


def _write_monitor(tmp_path: Path, *, broker_quantity: float = 1.0, orphan_positions: list[dict[str, object]] | None = None) -> None:
    path = tmp_path / "var" / "paper_strategy_monitor_runtime_status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
                "monitor_running": True,
                "health_classification": "HEALTHY",
                "stale": False,
                "submit_allowed": True,
                "account_id": "DUM882026",
                "broker_position_quantity": broker_quantity,
                "ledger_position_quantity": broker_quantity,
                "open_order_count": 0,
                "last_successful_broker_refresh": "2999-01-01T00:00:00+00:00",
                "freshness_window_seconds": 60.0,
                "block_reasons": [],
                "orphan_positions": orphan_positions or [],
            }
        ),
        encoding="utf-8",
    )


def _write_ledger(tmp_path: Path, positions: list[dict[str, object]]) -> None:
    path = tmp_path / "var" / "paper_strategy_position_ledger.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"positions": positions, "orphan_positions": []}), encoding="utf-8")


def _write_governance(tmp_path: Path, rows: list[dict[str, object]]) -> None:
    path = tmp_path / "var" / "per_strategy_paper_status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
                "strategies": rows,
                "summary": {
                    "strategy_count": len(rows),
                    "submit_capable_count": len([row for row in rows if row.get("submit_allowed")]),
                },
            }
        ),
        encoding="utf-8",
    )


def _position(strategy_id: str, *, qty: float = 1.0) -> dict[str, object]:
    return {
        "strategy_id": strategy_id,
        "account_id": "DUM882026",
        "symbol": "MGC",
        "contract_month": "202606",
        "expiry": "20260626",
        "con_id": 712565978,
        "local_symbol": "MGCM6",
        "quantity": qty,
        "side": "LONG",
        "average_entry_price": 4586.7,
        "realized_pnl": 14.18,
        "unrealized_pnl": 223.03,
        "order_id": 1,
        "perm_id": 490708968,
        "execution_id": "exec-1",
        "entry_timestamp": "2026-04-28T17:44:00+00:00",
        "source_intent_id": "intent-1",
        "state": "OPEN",
    }


def _governance_row(strategy_id: str, bridge_strategy_id: str, *, status: str = "PROBATION_ACTIVE") -> dict[str, object]:
    return {
        "strategy_id": strategy_id,
        "bridge_strategy_id": bridge_strategy_id,
        "strategy_status": status,
        "submit_allowed": True,
        "submit_block_reasons": [],
        "open_order_ambiguity_count": 0,
    }


def test_allows_second_strategy_buy_when_another_strategy_is_already_long(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=1.0)
    _write_ledger(tmp_path, [_position("ATP_COMPANION_V1_ASIA_US")])
    _write_governance(
        tmp_path,
        [
            _governance_row("gc_1x_all_lanes__asia_early_long", "gold_forced_session_baseline_v2__GC"),
            _governance_row("atp_companion_v1_asia_us", "ATP_COMPANION_V1_ASIA_US"),
        ],
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="gc_1x_all_lanes__asia_early_long",
        bridge_strategy_id="gold_forced_session_baseline_v2__GC",
        action="BUY",
        quantity=1.0,
    )

    assert gate["classification"] == "PAPER_EXPOSURE_STACK_ALLOWED"
    assert gate["submit_allowed"] is True
    assert gate["max_total_mgc_contracts"] == 20.0


def test_blocks_duplicate_buy_from_same_strategy_while_already_long(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=1.0)
    _write_ledger(tmp_path, [_position("ATP_COMPANION_V1_ASIA_US")])
    _write_governance(tmp_path, [_governance_row("atp_companion_v1_asia_us", "ATP_COMPANION_V1_ASIA_US")])

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="atp_companion_v1_asia_us",
        bridge_strategy_id="ATP_COMPANION_V1_ASIA_US",
        action="BUY",
        quantity=1.0,
    )

    assert gate["classification"] == "PAPER_EXPOSURE_BLOCKED_STRATEGY_LIMIT"
    assert "duplicate_strategy_entry_while_position_open" in gate["block_reasons"]


def test_allows_owning_strategy_exit(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=1.0)
    _write_ledger(tmp_path, [_position("ATP_COMPANION_V1_ASIA_US")])
    _write_governance(tmp_path, [_governance_row("atp_companion_v1_asia_us", "ATP_COMPANION_V1_ASIA_US")])

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="atp_companion_v1_asia_us",
        bridge_strategy_id="ATP_COMPANION_V1_ASIA_US",
        action="EXIT",
        quantity=1.0,
    )

    assert gate["classification"] == "PAPER_EXPOSURE_EXIT_ALLOWED"
    assert gate["submit_allowed"] is True


def test_blocks_non_owning_strategy_exit(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=1.0)
    _write_ledger(tmp_path, [_position("ATP_COMPANION_V1_ASIA_US")])
    _write_governance(
        tmp_path,
        [
            _governance_row("gc_1x_all_lanes__asia_early_long", "gold_forced_session_baseline_v2__GC"),
            _governance_row("atp_companion_v1_asia_us", "ATP_COMPANION_V1_ASIA_US"),
        ],
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="gc_1x_all_lanes__asia_early_long",
        bridge_strategy_id="gold_forced_session_baseline_v2__GC",
        action="EXIT",
        quantity=1.0,
    )

    assert gate["submit_allowed"] is False
    assert "non_owning_strategy_exit_forbidden" in gate["block_reasons"]


def test_detects_broker_net_vs_strategy_ledger_mismatch(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=2.0)
    _write_ledger(tmp_path, [_position("ATP_COMPANION_V1_ASIA_US", qty=1.0)])
    _write_governance(tmp_path, [_governance_row("atp_companion_v1_asia_us", "ATP_COMPANION_V1_ASIA_US")])

    artifacts = run_ibkr_paper_strategy_exposure(config=IbkrPaperStrategyExposureConfig(repo_root=tmp_path))

    assert artifacts.classification == "PAPER_EXPOSURE_BLOCKED_LEDGER_BROKER_MISMATCH"
    assert artifacts.aggregate_exposure_state["discrepancy_classification"] == "LEDGER_BROKER_MISMATCH"


def test_detects_orphan_broker_position(tmp_path: Path) -> None:
    _write_monitor(
        tmp_path,
        broker_quantity=1.0,
        orphan_positions=[{"symbol": "MGC", "quantity": 1.0, "detail": "unmatched"}],
    )
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [])

    artifacts = run_ibkr_paper_strategy_exposure(config=IbkrPaperStrategyExposureConfig(repo_root=tmp_path))

    assert artifacts.classification == "PAPER_EXPOSURE_BLOCKED_ORPHAN_POSITION"
    assert artifacts.aggregate_exposure_state["discrepancy_classification"] == "ORPHAN_BROKER_POSITION"


def test_honors_optional_aggregate_cap_when_configured(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=1.0)
    _write_ledger(tmp_path, [_position("ATP_COMPANION_V1_ASIA_US")])
    _write_governance(
        tmp_path,
        [
            _governance_row("gc_1x_all_lanes__asia_early_long", "gold_forced_session_baseline_v2__GC"),
            _governance_row("atp_companion_v1_asia_us", "ATP_COMPANION_V1_ASIA_US"),
        ],
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="gc_1x_all_lanes__asia_early_long",
        bridge_strategy_id="gold_forced_session_baseline_v2__GC",
        action="BUY",
        quantity=1.0,
        max_total_mgc_contracts=1.0,
    )

    assert gate["classification"] == "PAPER_EXPOSURE_BLOCKED_AGGREGATE_LIMIT"
    assert "configured_aggregate_contract_limit_exceeded" in gate["block_reasons"]


def test_does_not_impose_aggregate_cap_of_one_by_default(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=1.0)
    _write_ledger(tmp_path, [_position("ATP_COMPANION_V1_ASIA_US")])
    _write_governance(
        tmp_path,
        [
            _governance_row("gc_1x_all_lanes__asia_early_long", "gold_forced_session_baseline_v2__GC"),
            _governance_row("atp_companion_v1_asia_us", "ATP_COMPANION_V1_ASIA_US"),
        ],
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="gc_1x_all_lanes__asia_early_long",
        bridge_strategy_id="gold_forced_session_baseline_v2__GC",
        action="BUY",
        quantity=1.0,
    )

    assert gate["submit_allowed"] is True
    assert gate["max_total_mgc_contracts"] == 20.0


def test_blocks_when_default_twenty_mgc_cap_would_be_exceeded(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=20.0)
    _write_ledger(tmp_path, [_position("ATP_COMPANION_V1_ASIA_US", qty=20.0)])
    _write_governance(
        tmp_path,
        [
            _governance_row("gc_1x_all_lanes__asia_early_long", "gold_forced_session_baseline_v2__GC"),
            _governance_row("atp_companion_v1_asia_us", "ATP_COMPANION_V1_ASIA_US"),
        ],
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="gc_1x_all_lanes__asia_early_long",
        bridge_strategy_id="gold_forced_session_baseline_v2__GC",
        action="BUY",
        quantity=1.0,
    )

    assert gate["classification"] == "PAPER_EXPOSURE_BLOCKED_AGGREGATE_LIMIT"
    assert "configured_aggregate_contract_limit_exceeded" in gate["block_reasons"]
