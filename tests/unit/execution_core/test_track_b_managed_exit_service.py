from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.track_b_central_trade_registry import TradeEvent, TradeEventType
from mgc_v05l.execution_core.track_b_managed_exit_actuator import (
    MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING,
    MANAGED_EXIT_ACTUATOR_BLOCKED,
    MANAGED_EXIT_ACTUATOR_DRY_RUN_READY,
    MANAGED_EXIT_ACTUATOR_NOOP,
)
from mgc_v05l.execution_core.track_b_managed_exit_service import (
    MANAGED_EXIT_SERVICE_ACTUATOR_TIMEOUT,
    MANAGED_EXIT_SERVICE_APPLY_BLOCKED,
    MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED,
    MANAGED_EXIT_SERVICE_BLOCKED,
    MANAGED_EXIT_SERVICE_CYCLE_STARTED,
    MANAGED_EXIT_SERVICE_DRY_RUN_READY,
    MANAGED_EXIT_SERVICE_NO_ELIGIBLE_EXITS,
    MANAGED_EXIT_SERVICE_PIPELINE_UNAVAILABLE,
    MANAGED_EXIT_SERVICE_REFRESH_DEGRADED_ACTUATOR_ATTEMPTED,
    MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_ALLOWED,
    MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_BLOCKED,
    TrackBManagedExitServiceConfig,
    _build_pipeline_execution_plan,
    _modify_config_from_order_plan,
    _run_broker_truth_sweeper,
    read_track_b_managed_exit_service_status,
    run_track_b_managed_exit_service,
    run_track_b_managed_exit_service_once,
)
from mgc_v05l.execution_core.track_b_managed_order_registry import DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT


NOW = datetime(2026, 6, 8, 15, 5, tzinfo=UTC)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _broker_truth(
    tmp_path: Path,
    *,
    symbol: str = "MES",
    local_symbol: str = "MESU6",
    con_id: int = 793356217,
    expiry: str = "20260918",
    quantity: str = "-1.0",
) -> None:
    _write_json(
        tmp_path / "outputs/reports/ibkr_read_only_verification/ibkr_positions_snapshot.json",
        {
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "positions_complete": True,
            "positions": [
                {
                    "account_id": "DUM882026",
                    "security_type": "FUT",
                    "symbol": symbol,
                    "local_symbol": local_symbol,
                    "con_id": con_id,
                    "expiry": expiry,
                    "quantity": quantity,
                }
            ],
        },
    )
    _write_json(
        tmp_path / "outputs/reports/ibkr_read_only_verification/ibkr_open_orders_snapshot.json",
        {
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "open_orders_complete": True,
            "open_orders": [],
        },
    )


def _write_phase1_5m_bars(tmp_path: Path, symbol: str, bar_ends: list[str]) -> None:
    _write_json(
        tmp_path
        / "outputs/track_b_execution_core/phase1_runtime_market_data"
        / symbol
        / "5m"
        / "latest_runtime_candles.json",
        {
            "generated_at": NOW.isoformat(),
            "bars": [{"bar_end": value, "close": "106.75"} for value in bar_ends],
        },
    )


def _write_live_entry_fill(
    tmp_path: Path,
    *,
    trade_id: str,
    lifecycle_id: str,
    lane_id: str,
    generated_at: datetime,
    symbol: str = "MES",
    local_symbol: str = "MESU6",
    con_id: int = 793356217,
    side: str = "LONG",
    action: str = "BUY",
    order_id: str = "1",
    perm_id: str = "1871421812",
    exec_id: str = "0000e1a7.6a4255b6.01.01",
    price: str = "7496.5",
    append: bool = False,
) -> None:
    path = tmp_path / "outputs/track_b_execution_core/trade_registry/live_trade_events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    event = TradeEvent(
        event_id=f"{trade_id}_entry_fill",
        event_type=TradeEventType.ENTRY_FILL_BROKER_BACKED,
        generated_at=generated_at,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id=lane_id,
        thesis_strategy_id=lane_id,
        account_id="DUM882026",
        symbol=symbol,
        con_id=con_id,
        local_symbol=local_symbol,
        expiry="20260918",
        side=side,
        action=action,
        qty=Decimal("1"),
        source_artifact_path="outputs/track_b_execution_core/test_entry.json",
        order_id=order_id,
        client_id="10110",
        perm_id=perm_id,
        exec_id=exec_id,
        price=Decimal(price),
    )
    mode = "a" if append and path.exists() else "w"
    with path.open(mode, encoding="utf-8") as handle:
        handle.write(json.dumps(event.to_dict(), sort_keys=True) + "\n")


def _write_live_lifecycle_open(
    tmp_path: Path,
    *,
    trade_id: str,
    lifecycle_id: str,
    lane_id: str,
    generated_at: datetime,
    symbol: str,
    local_symbol: str,
    con_id: int,
    side: str,
    action: str,
    policy_id: str,
    append: bool = True,
) -> None:
    path = tmp_path / "outputs/track_b_execution_core/trade_registry/live_trade_events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    event = TradeEvent(
        event_id=f"{trade_id}_lifecycle_open",
        event_type=TradeEventType.LIFECYCLE_OPEN_MANAGED,
        generated_at=generated_at,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id=lane_id,
        thesis_strategy_id=lane_id,
        account_id="DUM882026",
        symbol=symbol,
        con_id=con_id,
        local_symbol=local_symbol,
        expiry="20260930",
        side=side,
        action=action,
        qty=Decimal("1"),
        source_artifact_path="outputs/track_b_execution_core/test_lifecycle.json",
        order_id="1",
        client_id="10110",
        perm_id="1477605652",
        exec_id="0000e1a7.6a4759a9.01.01",
        price=Decimal("103.1875"),
        metadata={"managed_exit_policy_id": policy_id},
    )
    mode = "a" if append and path.exists() else "w"
    with path.open(mode, encoding="utf-8") as handle:
        handle.write(json.dumps(event.to_dict(), sort_keys=True) + "\n")


def test_broker_truth_sweeper_repairs_stale_managed_contract_identity(tmp_path: Path) -> None:
    _broker_truth(tmp_path)
    registry = tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"
    _write_json(
        registry,
        {
            "classification": "OPEN_MANAGED_MATCHED",
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_MATCHED",
                    "account_id": "DUM882026",
                    "local_symbol": "MESU6",
                    "con_id": 0,
                    "quantity": "1",
                    "side": "SHORT",
                    "lane_id": "mes_globex_active_participation_short",
                    "lifecycle_id": "life-mes",
                    "trade_id": "trade-mes",
                    "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
                }
            ],
        },
    )

    report = _run_broker_truth_sweeper(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        write=True,
    )

    updated = json.loads(registry.read_text(encoding="utf-8"))
    assert report["classification"] == "MANAGED_EXIT_BROKER_TRUTH_SWEEP_REPAIRED"
    assert updated["managed_positions"][0]["con_id"] == 793356217
    assert updated["managed_positions"][0]["local_symbol"] == "MESU6"
    assert updated["managed_positions"][0]["expiry"] == "20260918"


def test_broker_truth_sweeper_recovers_entry_fill_metadata_for_existing_active_position(tmp_path: Path) -> None:
    _broker_truth(tmp_path, quantity="1.0")
    registry = tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"
    _write_json(
        registry,
        {
            "classification": "OPEN_MANAGED_MATCHED",
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_MATCHED",
                    "account_id": "DUM882026",
                    "local_symbol": "MESU6",
                    "con_id": 793356217,
                    "quantity": "1",
                    "side": "LONG",
                    "lane_id": "mes_us_active_participation_long",
                    "lifecycle_id": "life-mes",
                    "trade_id": "trade-mes",
                    "managed_exit_policy_id": None,
                    "entry_time": None,
                    "entry_order_ids": ["1"],
                    "entry_perm_ids": ["1871421812"],
                    "entry_exec_ids": ["0000e1a7.6a4255b6.01.01"],
                    "lifecycle_units": [
                        {
                            "lifecycle_id": "life-mes",
                            "trade_id": "trade-mes",
                            "lane_id": "mes_us_active_participation_long",
                            "managed_exit_policy_id": None,
                            "entry_time": None,
                        }
                    ],
                }
            ],
        },
    )
    _write_live_entry_fill(
        tmp_path,
        trade_id="trade-mes",
        lifecycle_id="life-mes",
        lane_id="mes_us_active_participation_long",
        generated_at=datetime(2026, 6, 8, 14, 50, tzinfo=UTC),
    )

    report = _run_broker_truth_sweeper(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        write=True,
    )

    updated = json.loads(registry.read_text(encoding="utf-8"))
    position = updated["managed_positions"][0]
    assert report["classification"] == "MANAGED_EXIT_BROKER_TRUTH_SWEEP_REPAIRED"
    assert position["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"
    assert position["entry_time"] == "2026-06-08T14:50:00+00:00"
    assert position["entry_price"] == "7496.5"
    assert position["lifecycle_units"][0]["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"
    assert position["lifecycle_units"][0]["entry_time"] == "2026-06-08T14:50:00+00:00"


def test_broker_truth_sweeper_prefers_freshest_broker_backed_same_contract_lifecycle(tmp_path: Path) -> None:
    _broker_truth(tmp_path, symbol="MNQ", local_symbol="MNQU6", con_id=793356225, quantity="-1.0")
    registry = tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"
    _write_json(
        registry,
        {
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "account_id": "DUM882026",
                    "local_symbol": "MNQU6",
                    "con_id": 793356225,
                    "quantity": "1",
                    "aggregate_qty": "-1",
                    "side": "SHORT",
                    "lane_id": "mnq_london_open_active_participation_short",
                    "strategy_id": "mnq_london_open_active_participation_short",
                    "lifecycle_id": "old-london-life",
                    "trade_id": "old-london-trade",
                    "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
                    "entry_time": "2026-06-12T07:35:02+00:00",
                },
                {
                    "classification": "OPEN_MANAGED_MATCHED",
                    "account_id": "DUM882026",
                    "local_symbol": "MNQU6",
                    "con_id": 793356225,
                    "quantity": "1",
                    "aggregate_qty": "-1",
                    "side": "SHORT",
                    "lane_id": "mnq_globex_active_participation_short",
                    "strategy_id": "mnq_globex_active_participation_short",
                    "lifecycle_id": "fresh-globex-life",
                    "trade_id": "fresh-globex-trade",
                    "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
                    "entry_time": None,
                },
            ],
        },
    )
    _write_live_entry_fill(
        tmp_path,
        trade_id="old-london-trade",
        lifecycle_id="old-london-life",
        lane_id="mnq_london_open_active_participation_short",
        generated_at=datetime(2026, 6, 12, 7, 35, tzinfo=UTC),
        symbol="MNQ",
        local_symbol="MNQU6",
        con_id=793356225,
        side="SHORT",
        action="SELL",
        order_id="1",
        perm_id="old-perm",
        exec_id="old-exec",
        price="29600.5",
    )
    _write_live_entry_fill(
        tmp_path,
        trade_id="fresh-globex-trade",
        lifecycle_id="fresh-globex-life",
        lane_id="mnq_globex_active_participation_short",
        generated_at=datetime(2026, 6, 14, 22, 20, 14, tzinfo=UTC),
        symbol="MNQ",
        local_symbol="MNQU6",
        con_id=793356225,
        side="SHORT",
        action="SELL",
        order_id="1",
        perm_id="fresh-perm",
        exec_id="fresh-exec",
        price="30369.75",
        append=True,
    )

    report = _run_broker_truth_sweeper(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        write=True,
    )

    updated = json.loads(registry.read_text(encoding="utf-8"))
    old_row, fresh_row = updated["managed_positions"]
    assert report["classification"] == "MANAGED_EXIT_BROKER_TRUTH_SWEEP_REPAIRED"
    assert fresh_row["lifecycle_id"] == "fresh-globex-life"
    assert fresh_row["entry_time"] == "2026-06-14T22:20:14+00:00"
    assert fresh_row["entry_price"] == "30369.75"
    assert old_row["classification"] == "STALE_SUPERSEDED_LIFECYCLE_PROJECTION"
    assert old_row["diagnostic_only"] is True
    assert old_row["superseded_by_lifecycle_id"] == "fresh-globex-life"
    assert any(
        row.get("reason") == "freshest_broker_backed_same_contract_lifecycle_selected"
        for row in report["diagnostics"]
    )


def test_broker_truth_sweeper_demotes_stale_broker_flat_rows(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "outputs/reports/ibkr_read_only_verification/ibkr_positions_snapshot.json",
        {
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "positions_complete": True,
            "positions": [
                {
                    "account_id": "DUM882026",
                    "security_type": "FUT",
                    "symbol": "GC",
                    "local_symbol": "GCQ6",
                    "con_id": 732156872,
                    "expiry": "20260827",
                    "quantity": "-1.0",
                },
                {
                    "account_id": "DUM882026",
                    "security_type": "FUT",
                    "symbol": "MGC",
                    "local_symbol": "MGCQ6",
                    "con_id": 732156883,
                    "expiry": "20260827",
                    "quantity": "0.0",
                },
                {
                    "account_id": "DUM882026",
                    "security_type": "FUT",
                    "symbol": "ZB",
                    "local_symbol": "ZBU6",
                    "con_id": 840227357,
                    "expiry": "20260921",
                    "quantity": "0.0",
                },
            ],
        },
    )
    _write_json(
        tmp_path / "outputs/reports/ibkr_read_only_verification/ibkr_open_orders_snapshot.json",
        {
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "open_orders_complete": True,
            "open_orders": [],
        },
    )
    registry = tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"
    _write_json(
        registry,
        {
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_MATCHED",
                    "account_id": "DUM882026",
                    "local_symbol": "GCQ6",
                    "con_id": 732156872,
                    "quantity": "1",
                    "aggregate_qty": "-1",
                    "side": "SHORT",
                    "lane_id": "gc_us_active_participation_short",
                    "lifecycle_id": "life-gc",
                    "trade_id": "trade-gc",
                    "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
                },
                {
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "account_id": "DUM882026",
                    "local_symbol": "MGCQ6",
                    "con_id": 732156883,
                    "quantity": "1",
                    "aggregate_qty": "-1",
                    "side": "SHORT",
                    "lane_id": "mgc_us_active_participation_short",
                    "lifecycle_id": "life-mgc",
                    "trade_id": "trade-mgc",
                    "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
                    "exit_due": True,
                },
                {
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "account_id": "DUM882026",
                    "local_symbol": "ZBU6",
                    "con_id": 840227357,
                    "quantity": "1",
                    "aggregate_qty": "-1",
                    "side": "SHORT",
                    "lane_id": "zb_us_active_participation_short",
                    "lifecycle_id": "life-zb",
                    "trade_id": "trade-zb",
                    "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
                    "exit_due": True,
                },
            ],
        },
    )

    report = _run_broker_truth_sweeper(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        write=True,
    )

    updated = json.loads(registry.read_text(encoding="utf-8"))
    by_symbol = {row["local_symbol"]: row for row in updated["managed_positions"]}
    assert report["classification"] == "MANAGED_EXIT_BROKER_TRUTH_SWEEP_DEMOTED_STALE_FLAT_ROWS"
    assert updated["classification"] == "OPEN_MANAGED_MATCHED"
    assert updated["managed_position_count"] == 1
    assert updated["diagnostic_only_position_count"] == 2
    assert by_symbol["GCQ6"]["classification"] == "OPEN_MANAGED_MATCHED"
    assert by_symbol["MGCQ6"]["classification"] == "STALE_BROKER_FLAT_MANAGED_POSITION"
    assert by_symbol["MGCQ6"]["diagnostic_only"] is True
    assert by_symbol["MGCQ6"]["exit_due"] is False
    assert by_symbol["ZBU6"]["classification"] == "STALE_BROKER_FLAT_MANAGED_POSITION"
    assert by_symbol["ZBU6"]["diagnostic_only"] is True
    assert by_symbol["ZBU6"]["exit_due"] is False


def test_broker_truth_sweeper_adopts_broker_backed_zt_position(tmp_path: Path) -> None:
    _broker_truth(
        tmp_path,
        symbol="ZT",
        local_symbol="ZTU6",
        con_id=842590391,
        expiry="20260930",
        quantity="-1.0",
    )
    registry = tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"
    _write_json(
        registry,
        {
            "classification": "NO_MANAGED_POSITIONS",
            "managed_positions": [],
        },
    )
    lifecycle_id = "bridge_fill_ZT|1m|2026-06-17T13:40:00Z|SELL_TO_OPEN"
    lifecycle_path = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
        / lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    _write_json(
        lifecycle_path,
        {
            "lifecycle_id": lifecycle_id,
            "trade_id": "trade-zt",
            "lane_id": "zt_us_active_participation_short",
            "strategy_id": "PAPER_ACTIVE_EVIDENCE_ZT_US_PARTICIPATION_SHORT_V1",
            "local_symbol": "ZTU6",
            "con_id": 842590391,
            "managed_exit_policy_id": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
            "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
            "entry_fill": {
                "price": "103.1875",
                "filled_at": "2026-06-17T14:00:23.784071+00:00",
                "broker_order_id": "1",
                "perm_id": "1477605652",
                "execution_id": "0000e1a7.6a4759a9.01.01",
            },
        },
    )
    _write_live_entry_fill(
        tmp_path,
        trade_id="trade-zt",
        lifecycle_id=lifecycle_id,
        lane_id="zt_us_active_participation_short",
        generated_at=datetime(2026, 6, 17, 14, 0, 23, 784071, tzinfo=UTC),
        symbol="ZT",
        local_symbol="ZTU6",
        con_id=842590391,
        side="SHORT",
        action="SELL",
        order_id="1",
        perm_id="1477605652",
        exec_id="0000e1a7.6a4759a9.01.01",
        price="103.1875",
    )
    _write_live_lifecycle_open(
        tmp_path,
        trade_id="trade-zt",
        lifecycle_id="bridge_fill_ZT|1m|2026-06-17T13:40:00Z|SELL_TO_OPEN",
        lane_id="zt_us_active_participation_short",
        generated_at=datetime(2026, 6, 17, 14, 2, 6, tzinfo=UTC),
        symbol="ZT",
        local_symbol="ZTU6",
        con_id=842590391,
        side="SHORT",
        action="SELL",
        policy_id="US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
    )

    report = _run_broker_truth_sweeper(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        write=True,
    )

    updated = json.loads(registry.read_text(encoding="utf-8"))
    [position] = updated["managed_positions"]
    assert report["classification"] == "MANAGED_EXIT_BROKER_TRUTH_SWEEP_ADOPTED"
    assert position["classification"] == "OPEN_MANAGED_MATCHED"
    assert position["local_symbol"] == "ZTU6"
    assert position["con_id"] == 842590391
    assert position["side"] == "SHORT"
    assert position["lane_id"] == "zt_us_active_participation_short"
    assert position["managed_exit_policy_id"] == "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"
    assert position["entry_time"] == "2026-06-17T14:00:23.784071+00:00"
    assert position["entry_price"] == "103.1875"


def test_broker_truth_sweeper_adopts_broker_backed_zf_position(tmp_path: Path) -> None:
    _broker_truth(
        tmp_path,
        symbol="ZF",
        local_symbol="ZFU6",
        con_id=842590380,
        expiry="20260930",
        quantity="1.0",
    )
    registry = tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"
    _write_json(registry, {"classification": "NO_MANAGED_POSITIONS", "managed_positions": []})
    lifecycle_id = "bridge_fill_ZF|1m|2026-06-17T23:24:00Z|BUY_TO_OPEN"
    _write_live_entry_fill(
        tmp_path,
        trade_id="trade-zf",
        lifecycle_id=lifecycle_id,
        lane_id="zf_globex_active_participation_long",
        generated_at=datetime(2026, 6, 18, 6, 30, 3, 350715, tzinfo=UTC),
        symbol="ZF",
        local_symbol="ZFU6",
        con_id=842590380,
        side="LONG",
        action="BUY",
        order_id="1",
        perm_id="2007198161",
        exec_id="0000e1a7.6a48e639.01.01",
        price="106.8359375",
    )
    _write_live_lifecycle_open(
        tmp_path,
        trade_id="trade-zf",
        lifecycle_id=lifecycle_id,
        lane_id="zf_globex_active_participation_long",
        generated_at=datetime(2026, 6, 18, 6, 31, 45, tzinfo=UTC),
        symbol="ZF",
        local_symbol="ZFU6",
        con_id=842590380,
        side="LONG",
        action="BUY",
        policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
    )

    report = _run_broker_truth_sweeper(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        write=True,
    )

    updated = json.loads(registry.read_text(encoding="utf-8"))
    [position] = updated["managed_positions"]
    assert report["classification"] == "MANAGED_EXIT_BROKER_TRUTH_SWEEP_ADOPTED"
    assert position["classification"] == "OPEN_MANAGED_MATCHED"
    assert position["local_symbol"] == "ZFU6"
    assert position["con_id"] == 842590380
    assert position["side"] == "LONG"
    assert position["lane_id"] == "zf_globex_active_participation_long"
    assert position["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"
    assert position["entry_time"] == "2026-06-18T06:30:03.350715+00:00"


def test_broker_truth_sweeper_refreshes_rates_exit_due_from_phase1_bars(tmp_path: Path) -> None:
    _broker_truth(
        tmp_path,
        symbol="ZF",
        local_symbol="ZFU6",
        con_id=842590380,
        expiry="20260930",
        quantity="-1.0",
    )
    registry = tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"
    lifecycle_id = "reserved_submit_zf_london_late_active_participation_short"
    _write_json(
        registry,
        {
            "classification": "OPEN_MANAGED_MATCHED",
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_MATCHED",
                    "account_id": "DUM882026",
                    "symbol": "ZF",
                    "track_b_root": "ZF",
                    "local_symbol": "ZFU6",
                    "con_id": 842590380,
                    "quantity": "1",
                    "aggregate_qty": "-1",
                    "side": "SHORT",
                    "lane_id": "zf_london_late_active_participation_short",
                    "strategy_id": "zf_london_late_active_participation_short",
                    "lifecycle_id": lifecycle_id,
                    "trade_id": "trade-zf",
                    "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
                    "entry_time": "2026-06-18T10:11:44+00:00",
                    "bars_since_entry": 1,
                    "exit_due": False,
                    "exit_due_state": "NOT_DUE_OR_UNKNOWN",
                }
            ],
        },
    )
    _write_phase1_5m_bars(
        tmp_path,
        "ZF",
        [
            "2026-06-18T10:15:00+00:00",
            "2026-06-18T10:20:00+00:00",
            "2026-06-18T10:25:00+00:00",
        ],
    )

    report = _run_broker_truth_sweeper(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        write=True,
    )

    updated = json.loads(registry.read_text(encoding="utf-8"))
    [position] = updated["managed_positions"]
    assert report["classification"] == "MANAGED_EXIT_BROKER_TRUTH_SWEEP_REPAIRED"
    assert position["classification"] == "OPEN_MANAGED_EXIT_DUE"
    assert position["bars_since_entry"] == 3
    assert position["exit_due"] is True
    assert position["exit_due_state"] == "EXIT_DUE"
    assert position["required_close_action"] == "BUY"
    assert position["required_close_quantity"] == "1"
    assert position["exit_due_refresh"]["source"] == "phase1_runtime_market_data_5m"


def test_broker_truth_sweeper_refreshes_validated_crypto_exit_due_from_phase1_bars(tmp_path: Path) -> None:
    _broker_truth(
        tmp_path,
        symbol="MET",
        local_symbol="METU6",
        con_id=772435602,
        expiry="20260925",
        quantity="1.0",
    )
    registry = tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"
    lifecycle_id = "reserved_submit_met_globex_active_participation_long"
    _write_json(
        registry,
        {
            "classification": "OPEN_MANAGED_MATCHED",
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_MATCHED",
                    "account_id": "DUM882026",
                    "symbol": "MET",
                    "track_b_root": "MET",
                    "local_symbol": "METU6",
                    "con_id": 772435602,
                    "quantity": "1",
                    "aggregate_qty": "1",
                    "side": "LONG",
                    "lane_id": "met_globex_active_participation_long",
                    "strategy_id": "met_globex_active_participation_long",
                    "lifecycle_id": lifecycle_id,
                    "trade_id": "trade-met",
                    "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
                    "entry_time": "2026-06-18T10:11:44+00:00",
                    "bars_since_entry": 1,
                    "exit_due": False,
                    "exit_due_state": "NOT_DUE_OR_UNKNOWN",
                }
            ],
        },
    )
    _write_phase1_5m_bars(
        tmp_path,
        "MET",
        [
            "2026-06-18T10:15:00+00:00",
            "2026-06-18T10:20:00+00:00",
            "2026-06-18T10:25:00+00:00",
        ],
    )

    report = _run_broker_truth_sweeper(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        write=True,
    )

    updated = json.loads(registry.read_text(encoding="utf-8"))
    [position] = updated["managed_positions"]
    assert report["classification"] == "MANAGED_EXIT_BROKER_TRUTH_SWEEP_REPAIRED"
    assert position["classification"] == "OPEN_MANAGED_EXIT_DUE"
    assert position["bars_since_entry"] == 3
    assert position["exit_due"] is True
    assert position["exit_due_state"] == "EXIT_DUE"
    assert position["required_close_action"] == "SELL"
    assert position["required_close_quantity"] == "1"
    assert position["exit_due_refresh"]["source"] == "phase1_runtime_market_data_5m"


def test_broker_truth_sweeper_repairs_zt_policy_from_registry_metadata(tmp_path: Path) -> None:
    _broker_truth(
        tmp_path,
        symbol="ZT",
        local_symbol="ZTU6",
        con_id=842590391,
        expiry="20260930",
        quantity="-1.0",
    )
    registry = tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"
    lifecycle_id = "reserved_submit_zt_us_active_participation_short_20260617T140022852108Z_076e1500f34d"
    _write_json(
        registry,
        {
            "classification": "OPEN_MANAGED_MATCHED",
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_MATCHED",
                    "account_id": "DUM882026",
                    "local_symbol": "ZTU6",
                    "con_id": 842590391,
                    "quantity": "1.0",
                    "side": "SHORT",
                    "lane_id": "zt_us_active_participation_short",
                    "strategy_id": "zt_us_active_participation_short",
                    "lifecycle_id": lifecycle_id,
                    "trade_id": "trade-zt",
                    "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
                    "entry_time": "2026-06-17T14:00:23.784071+00:00",
                }
            ],
        },
    )
    _write_live_entry_fill(
        tmp_path,
        trade_id="trade-zt",
        lifecycle_id=lifecycle_id,
        lane_id="zt_us_active_participation_short",
        generated_at=datetime(2026, 6, 17, 14, 0, 23, 784071, tzinfo=UTC),
        symbol="ZT",
        local_symbol="ZTU6",
        con_id=842590391,
        side="SHORT",
        action="SELL",
        order_id="1",
        perm_id="1477605652",
        exec_id="0000e1a7.6a4759a9.01.01",
        price="103.1875",
    )
    _write_live_lifecycle_open(
        tmp_path,
        trade_id="trade_bridge_fill_ZT_1m_2026-06-17T13_40_00Z_SELL_TO_OPEN",
        lifecycle_id="bridge_fill_ZT|1m|2026-06-17T13:40:00Z|SELL_TO_OPEN",
        lane_id="zt_us_active_participation_short",
        generated_at=datetime(2026, 6, 17, 14, 2, 6, tzinfo=UTC),
        symbol="ZT",
        local_symbol="ZTU6",
        con_id=842590391,
        side="SHORT",
        action="SELL",
        policy_id="US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
    )

    report = _run_broker_truth_sweeper(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        write=True,
    )

    updated = json.loads(registry.read_text(encoding="utf-8"))
    [position] = updated["managed_positions"]
    assert report["classification"] == "MANAGED_EXIT_BROKER_TRUTH_SWEEP_REPAIRED"
    assert position["managed_exit_policy_id"] == "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"


def test_broker_truth_sweeper_enriches_missing_broker_con_id_from_managed_registry(tmp_path: Path) -> None:
    _broker_truth(tmp_path, con_id=0)
    registry = tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"
    _write_json(
        registry,
        {
            "classification": "OPEN_MANAGED_MATCHED",
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_MATCHED",
                    "account_id": "DUM882026",
                    "local_symbol": "MESU6",
                    "con_id": 793356217,
                    "quantity": "1",
                    "side": "SHORT",
                    "lane_id": "mes_globex_active_participation_short",
                    "lifecycle_id": "life-mes",
                    "trade_id": "trade-mes",
                    "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
                }
            ],
        },
    )

    report = _run_broker_truth_sweeper(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        write=True,
    )

    updated = json.loads(registry.read_text(encoding="utf-8"))
    assert report["classification"] == "MANAGED_EXIT_BROKER_TRUTH_SWEEP_REPAIRED"
    assert updated["managed_positions"][0]["con_id"] == 793356217
    assert updated["managed_positions"][0]["broker_position"]["local_symbol"] == "MESU6"


def test_broker_truth_sweeper_adopts_broker_position_from_lifecycle_report(tmp_path: Path) -> None:
    _broker_truth(tmp_path)
    _write_json(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {"classification": "NO_MANAGED_POSITIONS", "managed_positions": []},
    )
    _write_json(
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle/bridge_fill_MES|1m|2026-06-12T00:58:00Z|SELL_TO_OPEN/track_b_strategy_managed_paper_lifecycle_report.json",
        {
            "lifecycle_id": "life-mes",
            "trade_id": "trade-mes",
            "lane_id": "mes_globex_active_participation_short",
            "strategy_id": "mes_globex_active_participation_short",
            "local_symbol": "MESU6",
            "con_id": 793356217,
            "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
            "entry_fill": {
                "filled_at": "2026-06-12T01:01:15+00:00",
                "price": "7472.25",
                "broker_order_id": "2",
                "perm_id": 472240307,
                "execution_id": "exec-mes",
            },
        },
    )

    report = _run_broker_truth_sweeper(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        write=True,
    )

    registry = json.loads(
        (tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json").read_text(
            encoding="utf-8"
        )
    )
    adopted = registry["managed_positions"][0]
    assert report["classification"] == "MANAGED_EXIT_BROKER_TRUTH_SWEEP_ADOPTED"
    assert adopted["classification"] == "OPEN_MANAGED_MATCHED"
    assert adopted["con_id"] == 793356217
    assert adopted["local_symbol"] == "MESU6"
    assert adopted["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"


def test_broker_truth_sweeper_selects_fresh_same_contract_owner_over_stale_opposite_side(
    tmp_path: Path,
) -> None:
    _broker_truth(
        tmp_path,
        symbol="MGC",
        local_symbol="MGCQ6",
        con_id=732156883,
        expiry="20260827",
        quantity="1.0",
    )
    registry = tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"
    _write_json(
        registry,
        {
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "account_id": "DUM882026",
                    "symbol": "MGC",
                    "track_b_root": "MGC",
                    "local_symbol": "MGCQ6",
                    "con_id": 732156883,
                    "quantity": "1",
                    "aggregate_qty": "-1",
                    "signed_lifecycle_qty": "-1",
                    "signed_broker_qty": "1",
                    "side": "SHORT",
                    "lane_id": "mgc_globex_active_participation_short",
                    "strategy_id": "mgc_globex_active_participation_short",
                    "lifecycle_id": "bridge_fill_MGC|1m|2026-06-18T23:04:00Z|SELL_TO_OPEN",
                    "trade_id": "trade-old-mgc-short",
                    "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
                    "entry_time": "2026-06-18T23:06:00+00:00",
                    "exit_due": True,
                    "exit_due_state": "EXIT_DUE",
                }
            ],
        },
    )
    _write_live_entry_fill(
        tmp_path,
        trade_id="trade-current-mgc-long",
        lifecycle_id="bridge_fill_MGC|1m|2026-06-22T06:08:00Z|BUY_TO_OPEN",
        lane_id="mgc_globex_active_participation_long",
        generated_at=datetime(2026, 6, 22, 6, 11, 8, tzinfo=UTC),
        symbol="MGC",
        local_symbol="MGCQ6",
        con_id=732156883,
        side="LONG",
        action="BUY",
        price="4213.9",
    )
    _write_phase1_5m_bars(
        tmp_path,
        "MGC",
        [
            "2026-06-22T06:15:00+00:00",
            "2026-06-22T06:20:00+00:00",
            "2026-06-22T06:25:00+00:00",
        ],
    )

    report = _run_broker_truth_sweeper(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=datetime(2026, 6, 22, 6, 26, tzinfo=UTC),
        write=True,
    )

    updated = json.loads(registry.read_text(encoding="utf-8"))
    active = [row for row in updated["managed_positions"] if row.get("diagnostic_only") is not True]
    diagnostic = [row for row in updated["managed_positions"] if row.get("diagnostic_only") is True]
    assert report["classification"] == "MANAGED_EXIT_BROKER_TRUTH_SWEEP_ADOPTED"
    assert len(active) == 1
    assert active[0]["lifecycle_id"] == "bridge_fill_MGC|1m|2026-06-22T06:08:00Z|BUY_TO_OPEN"
    assert active[0]["side"] == "LONG"
    assert active[0]["aggregate_qty"] == "1.0"
    assert active[0]["required_close_action"] == "SELL"
    assert active[0]["broker_qty_match"] is True
    assert diagnostic[0]["lifecycle_id"] == "bridge_fill_MGC|1m|2026-06-18T23:04:00Z|SELL_TO_OPEN"
    assert diagnostic[0]["classification"] == "STALE_SUPERSEDED_LIFECYCLE_PROJECTION"


def test_broker_truth_sweeper_marks_missing_policy_for_review(tmp_path: Path) -> None:
    _broker_truth(tmp_path)
    _write_json(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {"classification": "NO_MANAGED_POSITIONS", "managed_positions": []},
    )

    report = _run_broker_truth_sweeper(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        write=True,
    )

    registry = json.loads(
        (tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json").read_text(
            encoding="utf-8"
        )
    )
    assert report["classification"] == "MANAGED_EXIT_BROKER_TRUTH_SWEEP_REVIEW_REQUIRED"
    assert registry["managed_positions"][0]["classification"] == "STRAY_POSITION_REVIEW_REQUIRED"
    assert registry["managed_positions"][0]["review_reason"] == "managed_exit_policy_unresolved"


def test_broker_truth_sweeper_projects_validated_futures_without_broker_con_id(tmp_path: Path) -> None:
    symbols = (
        ("MGC", "MGCQ6", "20260827", 732156883, "MGC-202608"),
        ("GC", "GCQ6", "20260827", 732156872, "GC-202608"),
        ("NQ", "NQU6", "20260918", 770561204, "NQ-202609"),
        ("ES", "ESU6", "20260918", 649180671, "ES-202609"),
        ("MNQ", "MNQU6", "20260918", 793356225, "MNQ-202609"),
        ("MES", "MESU6", "20260918", 793356217, "MES-202609"),
    )
    _write_json(
        tmp_path / "outputs/reports/ibkr_read_only_verification/ibkr_positions_snapshot.json",
        {
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "positions_complete": True,
            "positions": [
                {
                    "account_id": "DUM882026",
                    "security_type": "FUT",
                    "symbol": symbol,
                    "local_symbol": local_symbol,
                    "con_id": 0,
                    "expiry": expiry,
                    "quantity": "1.0",
                }
                for symbol, local_symbol, expiry, _con_id, _contract_key in symbols
            ],
        },
    )
    _write_json(
        tmp_path / "outputs/reports/ibkr_read_only_verification/ibkr_open_orders_snapshot.json",
        {
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "open_orders_complete": True,
            "open_orders": [],
        },
    )
    registry_path = tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"
    _write_json(registry_path, {"classification": "NO_MANAGED_POSITIONS", "managed_positions": []})

    report = _run_broker_truth_sweeper(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        write=True,
    )

    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    by_symbol = {row["symbol"]: row for row in registry["managed_positions"]}
    assert report["classification"] == "MANAGED_EXIT_BROKER_TRUTH_SWEEP_REVIEW_REQUIRED"
    assert set(by_symbol) == {symbol for symbol, *_rest in symbols}
    assert not any(
        row.get("reason") == "broker_contract_identity_unparseable" for row in report["diagnostics"]
    )
    for symbol, local_symbol, expiry, con_id, contract_key in symbols:
        row = by_symbol[symbol]
        assert row["classification"] == "STRAY_POSITION_REVIEW_REQUIRED"
        assert row["review_reason"] == "managed_exit_policy_unresolved"
        assert row["local_symbol"] == local_symbol
        assert row["con_id"] == con_id
        assert row["expiry"] == expiry
        assert row["broker_position"]["contract_key"] == contract_key
        assert row["broker_position"]["contract_identity"]["source"] == "VALIDATED_TRACK_B_FUTURES_CONTRACT_REGISTRY"


def test_broker_truth_sweeper_recovers_lifecycle_from_registry_fill_when_report_missing(tmp_path: Path) -> None:
    _broker_truth(
        tmp_path,
        symbol="ES",
        local_symbol="ESU6",
        con_id=0,
        expiry="20260918",
        quantity="1.0",
    )
    _write_live_entry_fill(
        tmp_path,
        trade_id="trade_es_registry",
        lifecycle_id="life_es_registry",
        lane_id="es_globex_active_participation_long",
        generated_at=datetime(2026, 6, 8, 14, 50, tzinfo=UTC),
        symbol="ES",
        local_symbol="ESU6",
        con_id=649180671,
        side="LONG",
        action="BUY",
        price="7616.0",
    )
    registry_path = tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"
    _write_json(registry_path, {"classification": "NO_MANAGED_POSITIONS", "managed_positions": []})

    report = _run_broker_truth_sweeper(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        write=True,
    )

    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    position = registry["managed_positions"][0]
    assert report["classification"] == "MANAGED_EXIT_BROKER_TRUTH_SWEEP_ADOPTED"
    assert report["submit_attempted"] is False
    assert report["broker_state_mutated"] is False
    assert position["classification"] == "OPEN_MANAGED_MATCHED"
    assert position["symbol"] == "ES"
    assert position["local_symbol"] == "ESU6"
    assert position["con_id"] == 649180671
    assert position["lifecycle_id"] == "life_es_registry"
    assert position["trade_id"] == "trade_es_registry"
    assert position["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"
    assert position["entry_time"] == "2026-06-08T14:50:00+00:00"


def test_broker_truth_sweeper_flags_unparseable_contract_identity(tmp_path: Path) -> None:
    _broker_truth(tmp_path, con_id=0, local_symbol="", expiry="")

    report = _run_broker_truth_sweeper(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        write=False,
    )

    assert report["classification"] == "MANAGED_EXIT_SERVICE_RESTART_OR_SCHEMA_REPAIR_REQUIRED"



def test_pipeline_execution_plan_splits_v1_allowed_degraded_and_blocked(tmp_path: Path) -> None:
    plan = _build_pipeline_execution_plan(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        pipeline_builder=lambda config, now: _pipeline_report(
            decisions=("ALLOWED", "DEGRADED_ALLOWED", "BLOCKED")
        ),
    )

    assert plan["classification"] == "EXIT_INTENT_ALLOWED"
    assert [row["decision"] for row in plan["executable_intents"]] == ["ALLOWED", "DEGRADED_ALLOWED"]
    assert [row["decision"] for row in plan["blocked_intents"]] == ["BLOCKED"]
    assert len(plan["exit_intents"]) == 3
    assert len(plan["authority_decisions"]) == 3


def test_service_dry_run_uses_v1_allowed_plan_without_apply(tmp_path: Path) -> None:
    actuator_calls = []
    call_order = []

    def _actuator(config, now, timeout):
        call_order.append("actuator")
        actuator_calls.append(config)
        return _actuator_report(MANAGED_EXIT_ACTUATOR_DRY_RUN_READY, eligible=1)

    def _refresh(config, phase):
        call_order.append(f"refresh:{phase}")
        return _refresh_ok(config, phase)

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        actuator_runner=_actuator,
        authority_refresher=_refresh,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=("ALLOWED",)),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_DRY_RUN_READY
    assert payload["pipeline_classification"] == "EXIT_INTENT_ALLOWED"
    assert payload["executable_exit_intent_ids"] == ["exit-mes"]
    assert payload["entry_allowed"] is False
    assert payload["apply_requested"] is False
    assert actuator_calls[0].apply is False
    assert actuator_calls[0].max_closes_per_run == 1
    assert call_order == ["actuator"]


def test_apply_service_processes_one_v1_executable_intent_then_refreshes_broker_truth(tmp_path: Path) -> None:
    refresh_phases = []
    actuator_calls = []

    def _actuator(config, now, timeout):
        actuator_calls.append(config)
        return _actuator_report(
            MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING,
            eligible=3,
            submitted=1,
            local_symbol="MESM6",
        )

    def _refresh(config, phase):
        refresh_phases.append(phase)
        return _refresh_ok(config, phase)

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(
            repo_root=tmp_path,
            apply=True,
            operator_authorized_managed_exit=False,
            max_cycles_per_tick=3,
        ),
        now=NOW,
        actuator_runner=_actuator,
        authority_refresher=_refresh,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=("ALLOWED", "ALLOWED", "ALLOWED")),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED
    assert payload["submitted_count"] == 1
    assert payload["operator_authorized_managed_exit"] is False
    assert [call.apply for call in actuator_calls] == [True]
    assert [call.operator_authorized_managed_exit for call in actuator_calls] == [True]
    assert [call.max_closes_per_run for call in actuator_calls] == [3]
    assert refresh_phases == ["after_actuator_attempt"]


def test_apply_service_treats_stale_publication_as_diagnostic_when_v11_broker_risk_is_clear(tmp_path: Path) -> None:
    actuator_calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(
            repo_root=tmp_path,
            apply=True,
            max_cycles_per_tick=1,
            authority_refresh_after_attempt=True,
        ),
        now=NOW,
        actuator_runner=lambda config, now, timeout: actuator_calls.append(config)
        or _actuator_report(MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING, eligible=1, submitted=1),
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(
            decisions=("ALLOWED",),
            source_classifications={
                "open_order_truth": "ORDER_TRUTH_STALE",
                "managed_positions": "LIFECYCLE_WITHOUT_BROKER",
                "managed_orders": "CLOSE_ORDER_SUSPICIOUS",
                "reconciliation": "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            },
        ),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED
    assert payload["submitted_count"] == 1
    assert len(actuator_calls) == 1
    diagnostics = payload["service_diagnostics"][0]["managed_paper_risk_reducing_exit_authority"]["diagnostics"]
    assert {row["kind"] for row in diagnostics} >= {
        "diagnostic_open_order_truth_classification",
        "diagnostic_managed_position_classification",
        "diagnostic_managed_order_classification",
    }


def test_service_preserves_actuator_close_quantity_for_v1_plan(tmp_path: Path) -> None:
    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True, max_cycles_per_tick=1),
        now=NOW,
        actuator_runner=lambda config, now, timeout: {
            "classification": MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING,
            "exit_due_count": 1,
            "eligible_count": 1,
            "submitted_count": 1,
            "submit_attempted": True,
            "broker_state_mutated": True,
            "attempted_closes": [
                {
                    "identity": {"local_symbol": "MESM6", "lifecycle_id": "life-mes-3"},
                    "close_candidate": {"action": "SELL", "quantity": 3, "local_symbol": "MESM6"},
                    "submit_attempted": True,
                }
            ],
        },
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(
            decisions=("ALLOWED",),
            intents=[{"exit_intent_id": "exit-mes", "localSymbol": "MESM6", "close_action": "SELL", "close_qty": 3}],
        ),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED
    assert payload["attempted_closes"][0]["close_candidate"]["quantity"] == 3
    assert payload["execution_plan"]["executable_intents"][0]["close_qty"] == 3


def test_refresh_failure_is_diagnostic_for_v1_allowed_paper_risk_reducing_close(tmp_path: Path) -> None:
    calls = []
    refresh_calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(
            repo_root=tmp_path,
            apply=True,
            max_cycles_per_tick=1,
            authority_refresh_after_attempt=True,
        ),
        now=NOW,
        actuator_runner=lambda config, now, timeout: calls.append(config)
        or _actuator_report(MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING, eligible=2, submitted=1, local_symbol="MESM6"),
        authority_refresher=lambda config, phase: refresh_calls.append(phase)
        or {
            "phase": phase,
            "succeeded": False,
            "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_FAILED",
            "dependency_refresh_failures": [
                {"step": "control_plane_snapshot", "code": "control_plane_snapshot_refresh_failed", "returncode": 124}
            ],
        },
        pipeline_builder=lambda config, now: _pipeline_report(
            decisions=("ALLOWED",),
            source_classifications={
                "control_plane": "CONTROL_PLANE_SNAPSHOT_BLOCKED",
                "broker_session_authority": "BROKER_SESSION_AUTHORITY_DEGRADED_RECOVERED",
            },
        ),
        write=False,
    )

    assert calls
    assert payload["classification"] == MANAGED_EXIT_SERVICE_REFRESH_DEGRADED_ACTUATOR_ATTEMPTED
    assert refresh_calls == ["after_actuator_attempt"]
    assert payload["authority_refresh_failed"] is True
    assert payload["authority_refresh_degraded_actuator_attempted"] is True
    assert payload["service_diagnostics"][0]["code"] == "LEGACY_OPERATOR_READINESS_REFRESH_DIAGNOSTIC_ONLY"
    assert (
        payload["service_diagnostics"][0]["managed_paper_risk_reducing_exit_authority"]["classification"]
        == MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_ALLOWED
    )
    assert payload["pipeline_diagnostics"][0]["kind"] == "legacy_source_classifications"
    assert payload["submitted_count"] == 1
    assert payload["submit_attempted"] is True


def test_managed_close_fill_triggers_post_broker_mutation_refresh(tmp_path: Path) -> None:
    refresh_calls = []
    analytics_calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True, max_cycles_per_tick=1),
        now=NOW,
        actuator_runner=lambda config, now, timeout: _actuator_report(
            MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING,
            eligible=1,
            submitted=1,
            local_symbol="MESM6",
        ),
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=("ALLOWED",)),
        post_mutation_refresher=lambda **kwargs: refresh_calls.append(kwargs)
        or {"classification": "POST_BROKER_MUTATION_REFRESH_SUCCEEDED", "trigger": kwargs["trigger"]},
        post_trade_analytics_refresher=lambda **kwargs: analytics_calls.append(kwargs)
        or {
            "classification": "POST_TRADE_ANALYTICS_REFRESH_READY",
            "trigger": kwargs["trigger"],
            "analytics_only": True,
            "trading_blocking": False,
        },
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED
    assert payload["broker_state_mutated"] is True
    assert payload["post_broker_mutation_refresh"]["classification"] == "POST_BROKER_MUTATION_REFRESH_SUCCEEDED"
    assert payload["post_broker_mutation_refresh"]["trigger"] == "managed_exit_service_actuator"
    assert refresh_calls[0]["mutation_report"]["broker_state_mutated"] is True
    assert analytics_calls[0]["mutation_report"]["post_broker_mutation_refresh"]["classification"] == "POST_BROKER_MUTATION_REFRESH_SUCCEEDED"
    assert analytics_calls[0]["timeout_seconds"] == 90.0
    assert payload["post_trade_analytics_refresh"]["classification"] == "POST_TRADE_ANALYTICS_REFRESH_READY"
    assert payload["post_trade_analytics_refresh"]["trading_blocking"] is False


def test_managed_close_analytics_refresh_failure_is_diagnostic_only(tmp_path: Path) -> None:
    def _analytics_failure(**kwargs):
        raise RuntimeError("analytics refresh unavailable")

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True, max_cycles_per_tick=1),
        now=NOW,
        actuator_runner=lambda config, now, timeout: _actuator_report(
            MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING,
            eligible=1,
            submitted=1,
            local_symbol="MESM6",
        ),
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=("ALLOWED",)),
        post_mutation_refresher=lambda **kwargs: {
            "classification": "POST_BROKER_MUTATION_REFRESH_SUCCEEDED",
            "trigger": kwargs["trigger"],
        },
        post_trade_analytics_refresher=_analytics_failure,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED
    assert payload["post_trade_analytics_refresh"]["classification"] == "POST_TRADE_ANALYTICS_REFRESH_EXCEPTION_DIAGNOSTIC_ONLY"
    assert payload["post_trade_analytics_refresh"]["analytics_only"] is True
    assert payload["post_trade_analytics_refresh"]["managed_exit_authority"] is False
    assert payload["post_trade_analytics_refresh"]["trading_blocking"] is False


def test_managed_close_fill_publishes_refresh_running_status_before_post_refresh(tmp_path: Path) -> None:
    status_path = tmp_path / "status.json"
    heartbeat_path = tmp_path / "heartbeat.json"

    def _post_refresh(**kwargs):
        status = read_track_b_managed_exit_service_status(repo_root=tmp_path, status_path=status_path)
        assert status["classification"] == "TRACK_B_MANAGED_EXIT_SERVICE_POST_BROKER_MUTATION_REFRESH_RUNNING"
        assert status["trigger"] == "managed_exit_service_actuator"
        assert heartbeat_path.exists()
        return {
            "classification": "POST_BROKER_MUTATION_REFRESH_DEGRADED",
            "trigger": kwargs["trigger"],
            "steps": [{"name": "shared_truth", "returncode": 124, "succeeded": False}],
        }

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(
            repo_root=tmp_path,
            status_path=status_path,
            heartbeat_path=heartbeat_path,
            apply=True,
            max_cycles_per_tick=1,
        ),
        now=NOW,
        actuator_runner=lambda config, now, timeout: _actuator_report(
            MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING,
            eligible=1,
            submitted=1,
            local_symbol="MESM6",
        ),
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=("ALLOWED",)),
        post_mutation_refresher=_post_refresh,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED
    assert payload["post_broker_mutation_refresh"]["classification"] == "POST_BROKER_MUTATION_REFRESH_DEGRADED"
    assert payload["post_broker_mutation_refresh"]["duration_seconds"] >= 0


def test_v1_allowed_close_with_real_broker_risk_blocker_stops_before_actuator(tmp_path: Path) -> None:
    calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True, max_cycles_per_tick=1),
        now=NOW,
        actuator_runner=lambda config, now, timeout: calls.append(config)
        or _actuator_report(MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING, eligible=1, submitted=1),
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(
            decisions=("ALLOWED",),
            failed_hard_checks=("same_contract_working_close_does_not_over_close",),
        ),
        write=False,
    )

    assert calls == []
    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_BLOCKED
    assert payload["service_diagnostics"][0]["classification"] == MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_BLOCKED
    assert "same_contract_working_close_over_close_risk" in payload["service_diagnostics"][0]["blockers"]


def test_v1_blocked_plan_does_not_invoke_actuator(tmp_path: Path) -> None:
    calls = []
    refresh_calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        now=NOW,
        actuator_runner=lambda config, now, timeout: calls.append(config) or {},
        authority_refresher=lambda config, phase: refresh_calls.append(phase) or _refresh_ok(config, phase),
        pipeline_builder=lambda config, now: _pipeline_report(decisions=("BLOCKED",), block_reasons=("over_close_risk",)),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_BLOCKED
    assert payload["executable_intent_count"] == 0
    assert payload["blocked_exit_intent_ids"] == ["exit-mes"]
    assert payload["required_next_action"] == "OPERATOR_REVIEW_REQUIRED"
    assert calls == []
    assert refresh_calls == []


def test_pipeline_unavailable_does_not_invoke_actuator(tmp_path: Path) -> None:
    calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        now=NOW,
        actuator_runner=lambda config, now, timeout: calls.append(config) or {},
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(pipeline_errors=({"code": "position_state_unavailable"},)),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_PIPELINE_UNAVAILABLE
    assert payload["required_next_action"] == "REPAIR_MANAGED_EXIT_PIPELINE"
    assert payload["pipeline_diagnostics"][0]["kind"] == "pipeline_errors"
    assert calls == []


def test_legacy_bsa_false_is_diagnostic_when_v1_allows_broker_scoped_exit(tmp_path: Path) -> None:
    calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        now=NOW,
        actuator_runner=lambda config, now, timeout: calls.append(config)
        or _actuator_report(MANAGED_EXIT_ACTUATOR_DRY_RUN_READY, eligible=1),
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(
            decisions=("ALLOWED",),
            source_classifications={
                "broker_session_authority": "managed_risk_reducing_close=false",
                "control_plane": "CONTROL_PLANE_SNAPSHOT_STALE_OR_MIXED",
                "managed_registry": "historical_review_debris",
            },
        ),
        write=False,
    )

    assert calls
    assert payload["classification"] == MANAGED_EXIT_SERVICE_DRY_RUN_READY
    assert payload["pipeline_diagnostics"][0]["kind"] == "legacy_source_classifications"
    assert "close_authority_snapshots" not in payload


def test_retired_thin_residual_does_not_block_unrelated_managed_exit(tmp_path: Path) -> None:
    calls = []
    _seed_retired_thin_residual(tmp_path)

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        now=NOW,
        actuator_runner=lambda config, now, timeout: calls.append(config)
        or _actuator_report(MANAGED_EXIT_ACTUATOR_DRY_RUN_READY, eligible=1),
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(
            decisions=("ALLOWED",),
            source_classifications={
                "open_order_truth": "OPEN_CLOSE_ORDER_WORKING",
                "managed_positions": "OPEN_MANAGED_EXIT_DUE",
                "managed_orders": "CLOSE_ORDER_NOT_MARKETABLE",
                "broker_truth_lease": "INVALIDATED_CONTRADICTION",
                "broker_position_guardian": "BROKER_POSITION_GUARDIAN_HARD_HOLD",
            },
        ),
        write=False,
    )

    assert calls
    assert payload["classification"] == MANAGED_EXIT_SERVICE_DRY_RUN_READY
    diagnostics = payload["service_diagnostics"][0]["managed_paper_risk_reducing_exit_authority"]["diagnostics"]
    retired = [row for row in diagnostics if row["kind"] == "diagnostic_retired_thin_local_residual"]
    assert retired
    assert retired[0]["retired_thin_residual"]["residual_symbols"] == ["MSL"]


def test_retired_thin_residual_same_contract_remains_blocked(tmp_path: Path) -> None:
    calls = []
    _seed_retired_thin_residual(tmp_path)

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        now=NOW,
        actuator_runner=lambda config, now, timeout: calls.append(config) or {},
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(
            decisions=("ALLOWED",),
            intents=[{"exit_intent_id": "exit-msl", "symbol": "MSL", "localSymbol": "MSLU6"}],
            failed_hard_checks=("same_contract_working_close_does_not_over_close",),
            source_classifications={
                "open_order_truth": "OPEN_CLOSE_ORDER_WORKING",
                "managed_positions": "OPEN_MANAGED_EXIT_DUE",
                "managed_orders": "CLOSE_ORDER_NOT_MARKETABLE",
            },
        ),
        write=False,
    )

    assert calls == []
    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_BLOCKED
    blockers = payload["service_diagnostics"][0]["blockers"]
    assert "same_contract_working_close_over_close_risk" in blockers


def test_retired_thin_residual_does_not_demote_unknown_order_or_duplicate_or_review_required(tmp_path: Path) -> None:
    for kwargs, expected_reason in (
        ({"unknown_orders": 1}, "unknown_orders_present"),
        ({"duplicate_groups": 1}, "duplicate_close_groups_present"),
        ({"review_required": 1}, "review_required_present"),
    ):
        calls = []
        _seed_retired_thin_residual(tmp_path, **kwargs)

        payload = run_track_b_managed_exit_service_once(
            config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
            now=NOW,
            actuator_runner=lambda config, now, timeout: calls.append(config) or {},
            authority_refresher=_refresh_ok,
            pipeline_builder=lambda config, now: _pipeline_report(
                decisions=("ALLOWED",),
                source_classifications={
                    "open_order_truth": "OPEN_CLOSE_ORDER_WORKING",
                    "managed_positions": "OPEN_MANAGED_EXIT_DUE",
                    "managed_orders": "CLOSE_ORDER_NOT_MARKETABLE",
                },
            ),
            write=False,
        )

        assert calls == []
        assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_BLOCKED
        diagnostics = payload["service_diagnostics"][0]["diagnostics"]
        operator_review = [row for row in diagnostics if row["kind"] == "managed_order_operator_review"][0]
        assert operator_review["retired_thin_residual"]["reason"] == expected_reason


def test_retired_thin_residual_does_not_demote_non_risk_reducing_close(tmp_path: Path) -> None:
    calls = []
    _seed_retired_thin_residual(tmp_path)

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        now=NOW,
        actuator_runner=lambda config, now, timeout: calls.append(config) or {},
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(
            decisions=("ALLOWED",),
            failed_hard_checks=("risk_reducing_action",),
            source_classifications={
                "open_order_truth": "OPEN_CLOSE_ORDER_WORKING",
                "managed_positions": "OPEN_MANAGED_EXIT_DUE",
                "managed_orders": "CLOSE_ORDER_NOT_MARKETABLE",
            },
        ),
        write=False,
    )

    assert calls == []
    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_BLOCKED
    assert "not_risk_reducing" in payload["service_diagnostics"][0]["blockers"]


def test_non_retired_non_marketable_order_still_blocks_unrelated_exit(tmp_path: Path) -> None:
    calls = []
    _seed_retired_thin_residual(tmp_path, residual_symbol="MBT", residual_local_symbol="MBTU6")

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        now=NOW,
        actuator_runner=lambda config, now, timeout: calls.append(config) or {},
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(
            decisions=("ALLOWED",),
            source_classifications={
                "open_order_truth": "OPEN_CLOSE_ORDER_WORKING",
                "managed_positions": "OPEN_MANAGED_EXIT_DUE",
                "managed_orders": "CLOSE_ORDER_NOT_MARKETABLE",
            },
        ),
        write=False,
    )

    assert calls == []
    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_BLOCKED
    diagnostics = payload["service_diagnostics"][0]["diagnostics"]
    operator_review = [row for row in diagnostics if row["kind"] == "managed_order_operator_review"][0]
    assert operator_review["retired_thin_residual"]["reason"] == "non_retired_thin_non_marketable_close_present"


def test_service_loop_writes_status_and_heartbeat_without_starting_entries(tmp_path: Path) -> None:
    status_path = tmp_path / "latest_service_status.json"
    heartbeat_path = tmp_path / "heartbeat.json"
    sleeps = []

    run_track_b_managed_exit_service(
        config=TrackBManagedExitServiceConfig(
            repo_root=tmp_path,
            status_path=status_path,
            heartbeat_path=heartbeat_path,
            cadence_seconds=30,
            apply=True,
        ),
        actuator_runner=lambda config, now, timeout: _actuator_report(MANAGED_EXIT_ACTUATOR_BLOCKED, eligible=0),
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=("ALLOWED",)),
        sleep_func=lambda seconds: sleeps.append(seconds),
        max_iterations=1,
    )

    status = read_track_b_managed_exit_service_status(repo_root=tmp_path, status_path=status_path)
    assert status["classification"] == MANAGED_EXIT_SERVICE_APPLY_BLOCKED
    assert status["close_only"] is True
    assert status["entry_allowed"] is False
    assert status["live_money_eligible"] is False
    assert status["paper_proof_invoked"] is False
    assert heartbeat_path.exists()
    assert sleeps == []


def test_service_writes_cycle_started_status_before_actuator_call(tmp_path: Path) -> None:
    status_path = tmp_path / "status.json"

    def _actuator(config, now, timeout):
        status = read_track_b_managed_exit_service_status(repo_root=tmp_path, status_path=status_path)
        assert status["classification"] == MANAGED_EXIT_SERVICE_CYCLE_STARTED
        assert status["pid"] > 0
        assert status["service_label"] == "unit-test-service"
        assert status["apply_mode"] == "GUARDED_CLOSE_ONLY_APPLY"
        assert status["max_closes_per_run"] == 4
        assert status["detected_candidates_count"] is None
        assert status["candidate_detection"] == "deferred_to_v1_pipeline_execution_plan"
        return _actuator_report(MANAGED_EXIT_ACTUATOR_NOOP, eligible=0)

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(
            repo_root=tmp_path,
            status_path=status_path,
            apply=True,
            service_label="unit-test-service",
        ),
        now=NOW,
        actuator_runner=_actuator,
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=("ALLOWED",)),
        write=True,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_NO_ELIGIBLE_EXITS
    assert payload["phase_timings"][0]["phase"] == "pipeline_candidate_discovery"


def test_actuator_timeout_produces_terminal_status_without_further_apply(tmp_path: Path) -> None:
    status_path = tmp_path / "status.json"
    calls = []

    def _timeout_command(command, repo_root, timeout):
        calls.append(command)
        return subprocess.CompletedProcess(command, 124, stdout="", stderr="timed out")

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(
            repo_root=tmp_path,
            status_path=status_path,
            apply=True,
            max_cycles_per_tick=3,
            actuator_timeout_seconds=0.01,
        ),
        now=NOW,
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=("ALLOWED", "ALLOWED", "ALLOWED")),
        command_runner=_timeout_command,
        write=True,
    )

    status = read_track_b_managed_exit_service_status(repo_root=tmp_path, status_path=status_path)
    assert payload["classification"] == MANAGED_EXIT_SERVICE_ACTUATOR_TIMEOUT
    assert status["classification"] == MANAGED_EXIT_SERVICE_ACTUATOR_TIMEOUT
    assert payload["submit_attempted"] is False
    assert payload["submitted_count"] == 0
    assert len(calls) == 1
    assert "--apply" in calls[0]
    assert "--operator-authorized-managed-exit" in calls[0]


def test_no_candidates_produces_no_eligible_exits(tmp_path: Path) -> None:
    def _refresh_should_not_run(config, phase):
        raise AssertionError("HOLD/no-candidate cycles must not refresh operator authority")

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        actuator_runner=lambda config, now, timeout: _actuator_report(MANAGED_EXIT_ACTUATOR_NOOP, eligible=0),
        authority_refresher=_refresh_should_not_run,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=(), classification="NO_POSITIONS"),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_NO_ELIGIBLE_EXITS
    assert payload["required_next_action"] == "NO_ACTION"
    assert payload["actuator_invocation_count"] == 0
    assert payload["authority_refreshes"] == []


def test_hold_only_cycle_skips_slow_authority_refresh_and_actuator(tmp_path: Path) -> None:
    def _refresh_should_not_run(config, phase):
        raise AssertionError("HOLD_ONLY cycle reached slow authority refresh")

    def _actuator_should_not_run(config, now, timeout):
        raise AssertionError("HOLD_ONLY cycle reached actuator")

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        now=NOW,
        actuator_runner=_actuator_should_not_run,
        authority_refresher=_refresh_should_not_run,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=(), classification="HOLD_ONLY"),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_NO_ELIGIBLE_EXITS
    assert payload["pipeline_classification"] == "HOLD_ONLY"
    assert payload["authority_refreshes"] == []
    assert payload["actuator_invocation_count"] == 0
    assert payload["managed_order_maintenance_invocation_count"] == 0


def test_no_exit_intents_invokes_working_close_order_maintenance(tmp_path: Path) -> None:
    maintenance_calls = []
    actuator_calls = []
    _write_managed_close_order_registry(tmp_path)

    def _maintenance(config, now, timeout):
        maintenance_calls.append((config, timeout))
        return {
            "classification": "MANAGED_ORDER_MAINTENANCE_DRY_RUN_READY",
            "broker_mutation_attempted": False,
            "broker_mutation_performed": False,
            "results": [{"broker_order_id": "91", "classification": "MODIFY_IN_PLACE_DRY_RUN_READY"}],
        }

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        actuator_runner=lambda config, now, timeout: actuator_calls.append(config) or {},
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=(), classification="HOLD_ONLY"),
        order_maintenance_runner=_maintenance,
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_DRY_RUN_READY
    assert payload["actuator_invocation_count"] == 0
    assert len(maintenance_calls) == 1
    assert payload["managed_order_maintenance_invocation_count"] == 1
    assert payload["latest_managed_order_maintenance_classification"] == "MANAGED_ORDER_MAINTENANCE_DRY_RUN_READY"
    assert payload["broker_state_mutated"] is False


def test_apply_service_reports_working_close_order_maintenance_mutation(tmp_path: Path) -> None:
    _write_managed_close_order_registry(tmp_path)

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        now=NOW,
        actuator_runner=lambda config, now, timeout: {},
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=(), classification="HOLD_ONLY"),
        order_maintenance_runner=lambda config, now, timeout: {
            "classification": "MANAGED_ORDER_MAINTENANCE_APPLIED",
            "broker_mutation_attempted": True,
            "broker_mutation_performed": True,
            "results": [
                {
                    "broker_order_id": "91",
                    "classification": "MODIFY_IN_PLACE_APPLIED",
                    "current_known_limit": "7388.0",
                    "new_limit": "7378.0",
                }
            ],
        },
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED
    assert payload["actuator_invocation_count"] == 0
    assert payload["broker_state_mutated"] is True
    assert payload["managed_order_maintenance_mutation_attempted"] is True
    assert payload["managed_order_maintenance_mutation_performed"] is True


def test_working_close_order_maintenance_refresh_timeout_blocks_before_mutation(tmp_path: Path) -> None:
    _write_managed_close_order_registry(tmp_path)
    path = tmp_path / DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["managed_orders"][0]["account_id"] = "WRONG"
    path.write_text(json.dumps(payload), encoding="utf-8")
    maintenance_calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        now=NOW,
        actuator_runner=lambda config, now, timeout: {},
        authority_refresher=lambda config, phase: {
            "phase": phase,
            "succeeded": False,
            "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_TIMEOUT",
        },
        pipeline_builder=lambda config, now: _pipeline_report(decisions=(), classification="HOLD_ONLY"),
        order_maintenance_runner=lambda config, now, timeout: maintenance_calls.append(config) or {
            "classification": "MANAGED_ORDER_MAINTENANCE_APPLIED",
            "broker_mutation_attempted": True,
            "broker_mutation_performed": True,
        },
        write=False,
    )

    assert maintenance_calls == []
    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_BLOCKED
    assert payload["service_diagnostics"][0]["classification"] == MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_BLOCKED
    assert "wrong_account" in payload["service_diagnostics"][0]["blockers"]
    assert payload["managed_order_maintenance_invocation_count"] == 0
    assert payload["managed_order_maintenance_mutation_attempted"] is False


def test_working_close_order_maintenance_refresh_timeout_is_diagnostic_when_close_order_is_risk_reducing(
    tmp_path: Path,
) -> None:
    _write_managed_close_order_registry(tmp_path)
    maintenance_calls = []
    refresh_calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        now=NOW,
        actuator_runner=lambda config, now, timeout: {},
        authority_refresher=lambda config, phase: refresh_calls.append(phase) or {
            "phase": phase,
            "succeeded": False,
            "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_TIMEOUT",
        },
        pipeline_builder=lambda config, now: _pipeline_report(decisions=(), classification="HOLD_ONLY"),
        order_maintenance_runner=lambda config, now, timeout: maintenance_calls.append(config) or {
            "classification": "MANAGED_ORDER_MAINTENANCE_APPLIED",
            "broker_mutation_attempted": True,
            "broker_mutation_performed": True,
        },
        write=False,
    )

    assert len(maintenance_calls) == 1
    assert refresh_calls == []
    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED
    assert payload["service_diagnostics"][0]["managed_paper_risk_reducing_exit_authority"]["classification"] == (
        MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_ALLOWED
    )
    assert payload["managed_order_maintenance_mutation_performed"] is True


def test_managed_order_maintenance_mutation_triggers_post_broker_mutation_refresh(tmp_path: Path) -> None:
    _write_managed_close_order_registry(tmp_path)
    refresh_calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        now=NOW,
        actuator_runner=lambda config, now, timeout: {},
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=(), classification="HOLD_ONLY"),
        order_maintenance_runner=lambda config, now, timeout: {
            "classification": "MANAGED_ORDER_MAINTENANCE_APPLIED",
            "broker_mutation_attempted": True,
            "broker_mutation_performed": True,
        },
        post_mutation_refresher=lambda **kwargs: refresh_calls.append(kwargs)
        or {"classification": "POST_BROKER_MUTATION_REFRESH_DEGRADED", "trigger": kwargs["trigger"]},
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED
    assert payload["managed_order_maintenance_mutation_performed"] is True
    assert payload["post_broker_mutation_refresh"]["classification"] == "POST_BROKER_MUTATION_REFRESH_DEGRADED"
    assert payload["post_broker_mutation_refresh"]["trigger"] == "managed_exit_service_order_maintenance"
    assert len(refresh_calls) == 1


def test_modify_config_uses_known_order_owner_client_id(tmp_path: Path) -> None:
    config = _modify_config_from_order_plan(
        service_config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        order={
            "broker_order_id": "91",
            "perm_id": 68652733,
            "client_id": 17086,
            "account_id": "DUM882026",
            "symbol": "MES",
            "contract": "MESM6",
            "con_id": 770561194,
            "action": "SELL",
            "quantity": "1",
            "limit_price": "7388.0",
        },
        plan={
            "classification": "MODIFY_IN_PLACE_ELIGIBLE",
            "managed_close_reprice_policy": {"limit_price": "7374.25"},
        },
        timeout_seconds=30.0,
    )

    assert config is not None
    assert config.tws_client_id == 17086
    assert config.broker_order_id == "91"
    assert config.new_limit == "7374.25"


def _refresh_ok(config, phase):
    return {
        "phase": phase,
        "succeeded": True,
        "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_READY",
        "generated_at": NOW.isoformat(),
        "dependency_refresh_failures": [],
    }


def _actuator_report(classification: str, *, eligible: int, submitted: int = 0, local_symbol: str | None = None) -> dict:
    return {
        "classification": classification,
        "exit_due_count": eligible,
        "eligible_count": eligible,
        "submitted_count": submitted,
        "submit_attempted": submitted > 0,
        "broker_state_mutated": submitted > 0,
        "attempted_closes": []
        if local_symbol is None
        else [{"identity": {"local_symbol": local_symbol}, "submit_attempted": submitted > 0}],
    }


def _write_managed_close_order_registry(repo_root: Path) -> None:
    path = repo_root / DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "classification": "CLOSE_ORDER_CANCEL_REPLACE_REQUIRED",
                "managed_orders": [
                    {
                        "classification": "CLOSE_ORDER_CANCEL_REPLACE_REQUIRED",
                        "is_close_order": True,
                        "broker_order_id": "91",
                        "perm_id": 68652733,
                        "client_id": 17086,
                        "account_id": "DUM882026",
                        "symbol": "MES",
                        "contract": "MESM6",
                        "con_id": 770561194,
                        "action": "SELL",
                        "quantity": "1",
                        "limit_price": "7388.0",
                        "lifecycle_id": "life-mes",
                        "broker_position": {"quantity": "1", "local_symbol": "MESM6"},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def _seed_retired_thin_residual(
    repo_root: Path,
    *,
    residual_symbol: str = "MSL",
    residual_local_symbol: str = "MSLU6",
    unknown_orders: int = 0,
    duplicate_groups: int = 0,
    review_required: int = 0,
) -> None:
    order_state = {
        "classification": "OPEN_CLOSE_ORDER_WORKING",
        "symbol": residual_symbol,
        "local_symbol": residual_local_symbol,
        "contract": residual_local_symbol,
        "account_id": "DUM882026",
        "is_close_order": True,
        "action": "BUY",
        "quantity": "1",
        "broker_order_id": "389",
        "perm_id": "865990651",
        "status": "PreSubmitted",
        "marketable": False,
    }
    _write_json(
        repo_root / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        {
            "classification": "OPEN_CLOSE_ORDER_WORKING",
            "canonical_refresh_scope": "GLOBAL_COMPLETE",
            "canonical_scope_blockers": [],
            "summary": {
                "open_order_count": 1,
                "working_close_order_count": 1,
                "unknown_order_count": unknown_orders,
                "duplicate_close_order_group_count": duplicate_groups,
                "suspicious_order_count": 0,
            },
            "order_states": [order_state],
            "broker_open_orders": [order_state],
            "duplicate_close_order_groups": ([{"symbol": residual_symbol}] if duplicate_groups else []),
        },
    )
    _write_json(
        repo_root / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "summary": {"review_required_count": review_required},
            "managed_positions": [],
        },
    )
    _write_json(
        repo_root / DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT,
        {
            "classification": "CLOSE_ORDER_NOT_MARKETABLE",
            "summary": {
                "managed_order_count": 1,
                "close_order_not_marketable_count": 1,
                "duplicate_close_order_group_count": duplicate_groups,
            },
            "managed_orders": [
                {
                    **order_state,
                    "classification": "CLOSE_ORDER_NOT_MARKETABLE",
                    "recommended_next_action": "OPERATOR_REVIEW",
                    "canonical_managed_position": {
                        "lifecycle_id": f"life-{residual_symbol.lower()}",
                        "broker_position": {
                            "quantity": "-1",
                            "local_symbol": residual_local_symbol,
                            "symbol": residual_symbol,
                        },
                    },
                    "broker_position": {
                        "quantity": "-1",
                        "local_symbol": residual_local_symbol,
                        "symbol": residual_symbol,
                    },
                }
            ],
        },
    )


def _pipeline_report(
    *,
    decisions: tuple[str, ...] = ("ALLOWED",),
    classification: str = "EXIT_INTENT_ALLOWED",
    intents: list[dict[str, Any]] | None = None,
    block_reasons: tuple[str, ...] = (),
    source_classifications: dict[str, str] | None = None,
    pipeline_errors: tuple[dict[str, Any], ...] = (),
    pipeline_blockers: tuple[dict[str, Any], ...] = (),
    failed_hard_checks: tuple[str, ...] = (),
    failed_conditional_checks: tuple[str, ...] = (),
) -> dict[str, Any]:
    default_intents = [
        {
            "exit_intent_id": "exit-mes",
            "localSymbol": "MESM6",
            "close_action": "BUY",
            "close_qty": 1,
            "execution_domain": "TRACK_B_PAPER",
            "account": "DUM882026",
            "account_id": "DUM882026",
            "position_side": "SHORT",
            "owned_qty": "1",
            "remaining_qty_after": "0",
            "source_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
            "exit_reason": "timebox_exit_due",
            "lifecycle_id": "life-mes",
            "trade_id": "trade-mes",
            "strategy_id": "strategy-mes",
            "lane_id": "lane-mes",
            "live_money_eligible": False,
            "live_money_allowed": False,
            "paper_proof_invoked": False,
            "broad_flatten_allowed": False,
            "global_flatten_allowed": False,
            "attribution": {
                "lifecycle_id": "life-mes",
                "trade_id": "trade-mes",
                "strategy_id": "strategy-mes",
                "lane_id": "lane-mes",
            },
        },
        {
            "exit_intent_id": "exit-mnq",
            "localSymbol": "MNQM6",
            "close_action": "BUY",
            "close_qty": 1,
            "execution_domain": "TRACK_B_PAPER",
            "account": "DUM882026",
            "account_id": "DUM882026",
            "position_side": "SHORT",
            "owned_qty": "1",
            "remaining_qty_after": "0",
            "source_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
            "exit_reason": "timebox_exit_due",
            "lifecycle_id": "life-mnq",
            "trade_id": "trade-mnq",
            "live_money_eligible": False,
            "live_money_allowed": False,
            "paper_proof_invoked": False,
            "broad_flatten_allowed": False,
            "global_flatten_allowed": False,
            "attribution": {"lifecycle_id": "life-mnq", "trade_id": "trade-mnq"},
        },
        {
            "exit_intent_id": "exit-mgc",
            "localSymbol": "MGCM6",
            "close_action": "SELL",
            "close_qty": 1,
            "execution_domain": "TRACK_B_PAPER",
            "account": "DUM882026",
            "account_id": "DUM882026",
            "position_side": "LONG",
            "owned_qty": "1",
            "remaining_qty_after": "0",
            "source_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
            "exit_reason": "timebox_exit_due",
            "lifecycle_id": "life-mgc",
            "trade_id": "trade-mgc",
            "live_money_eligible": False,
            "live_money_allowed": False,
            "paper_proof_invoked": False,
            "broad_flatten_allowed": False,
            "global_flatten_allowed": False,
            "attribution": {"lifecycle_id": "life-mgc", "trade_id": "trade-mgc"},
        },
    ]
    if intents is not None:
        selected_intents = [{**default_intents[index], **intent} for index, intent in enumerate(intents)]
    else:
        selected_intents = default_intents
    selected_intents = selected_intents[: len(decisions)]
    authority_decisions = []
    for intent, decision in zip(selected_intents, decisions):
        authority_decisions.append(
            {
                "exit_intent_id": intent["exit_intent_id"],
                "decision": decision,
                "block_reasons": list(block_reasons if decision == "BLOCKED" else ()),
                "diagnostics": [],
                "authority_decision": {
                    "decision": decision,
                    "block_reasons": list(block_reasons if decision == "BLOCKED" else ()),
                    "account_id": intent.get("account_id") or intent.get("account"),
                    "execution_domain": intent.get("execution_domain"),
                    "live_money_eligible": intent.get("live_money_eligible") is True,
                    "paper_proof_invoked": intent.get("paper_proof_invoked") is True,
                    "hard_required_checks": _authority_hard_checks(failed_hard_checks),
                    "conditional_risk_checks": _authority_conditional_checks(failed_conditional_checks),
                },
            }
        )
    return {
        "classification": classification,
        "generated_exit_intents": selected_intents,
        "exit_authority_decisions": authority_decisions,
        "pipeline_errors": list(pipeline_errors),
        "pipeline_blockers": list(pipeline_blockers),
        "source_classifications": source_classifications
        or {
            "open_order_truth": "NO_OPEN_ORDERS",
            "managed_positions": "OPEN_MANAGED_EXIT_DUE" if decisions else "OPEN_MANAGED_MATCHED",
            "managed_orders": "POSITION_WITHOUT_CLOSE_ORDER" if decisions else "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING",
            "reconciliation": "TRACK_B_PAPER_BROKER_RECONCILED",
        },
    }


def _authority_hard_checks(failed: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    names = (
        "known_current_broker_position",
        "account_matches",
        "execution_domain_matches",
        "contract_matches",
        "close_qty_within_broker_position",
        "risk_reducing_action",
        "same_contract_working_close_does_not_over_close",
        "safe_state_no_hard_halt",
        "live_money_domain_allowed",
        "paper_proof_not_invoked",
        "broad_or_global_flatten_not_requested",
    )
    return {name: {"passed": name not in failed} for name in names}


def _authority_conditional_checks(failed: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    names = ("same_contract_unknown_order_risk",)
    return {name: {"passed": name not in failed} for name in names}
