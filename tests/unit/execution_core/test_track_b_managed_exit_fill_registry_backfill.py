from __future__ import annotations

import inspect
import json
from pathlib import Path

from mgc_v05l.execution_core import track_b_managed_exit_fill_registry_backfill as backfill
from mgc_v05l.execution_core import track_b_strategy_performance_attachment as perf


def test_backfill_publishes_managed_exit_fill_once(tmp_path: Path) -> None:
    _write_lifecycle_report(
        tmp_path,
        lane_id="mgc_globex_active_participation_long",
        symbol="MGC",
        local_symbol="MGCU6",
        con_id=732156871,
        side="LONG",
        close_action="SELL",
        lifecycle_id="reserved_submit_mgc_globex_active_participation_long_1",
        trade_id="trade_mgc_1",
        close_order_id="101",
        close_perm_id="9001",
        close_exec_id="exec.close.1",
    )
    _write_order_control(tmp_path, order_id="101", client_id="17086", perm_id="9001")

    first = backfill.publish_managed_exit_fill_backfill(
        repo_root=tmp_path,
        lifecycle_report_root=Path("lifecycles"),
        paper_order_control_events_path=Path("order_control.jsonl"),
        position_truth_path=Path("position_truth.json"),
        trade_registry_events_path=Path("trade_events.jsonl"),
        summary_path=Path("summary.json"),
    )
    second = backfill.publish_managed_exit_fill_backfill(
        repo_root=tmp_path,
        lifecycle_report_root=Path("lifecycles"),
        paper_order_control_events_path=Path("order_control.jsonl"),
        position_truth_path=Path("position_truth.json"),
        trade_registry_events_path=Path("trade_events.jsonl"),
        summary_path=Path("summary.json"),
    )

    events = _read_jsonl(tmp_path / "trade_events.jsonl")
    assert first.appended_count == 1
    assert second.appended_count == 0
    assert second.duplicate_count == 1
    assert [row["event_type"] for row in events] == ["EXIT_FILL_BROKER_BACKED"]
    event = events[0]
    assert event["trade_id"] == "trade_mgc_1"
    assert event["lifecycle_id"] == "reserved_submit_mgc_globex_active_participation_long_1"
    assert event["order_id"] == "101"
    assert event["client_id"] == "17086"
    assert event["perm_id"] == "9001"
    assert event["exec_id"] == "exec.close.1"
    assert event["metadata"]["source"] == "track_b_managed_exit_fill_registry_backfill"
    assert event["metadata"]["source_refs"]


def test_backfilled_exit_fill_improves_analytics_pairing_with_chronological_fallback(tmp_path: Path) -> None:
    _write_profile(tmp_path, ["mgc_globex_active_participation_long"])
    _write_jsonl(
        tmp_path / "fills.jsonl",
        [
            _entry(
                lane_id="mgc_globex_active_participation_long",
                symbol="MGC",
                side="BUY",
                intent_type="BUY_TO_OPEN",
                price="100",
                multiplier="10",
                fill_time="2026-06-24T00:01:00Z",
            )
        ],
    )
    _write_jsonl(tmp_path / "funnel.jsonl", [])
    _write_candles(tmp_path, "MGC", highs=[101, 103, 102], lows=[99, 100, 101])
    _write_lifecycle_report(
        tmp_path,
        lane_id="mgc_globex_active_participation_long",
        symbol="MGC",
        local_symbol="MGCU6",
        con_id=123,
        side="LONG",
        close_action="SELL",
        lifecycle_id="reserved_submit_mgc_globex_active_participation_long_exit",
        trade_id="trade_reserved_exit",
        close_order_id="102",
        close_perm_id="9002",
        close_exec_id="exec.close.2",
        close_price="102",
        close_time="2026-06-24T00:20:00Z",
    )
    _write_order_control(tmp_path, order_id="102", client_id="17086", perm_id="9002")

    before = perf.build_strategy_performance_attachment(
        repo_root=tmp_path,
        config_path=Path("config.json"),
        roster_path=Path("roster.json"),
        filled_bridge_results_path=Path("fills.jsonl"),
        trade_registry_events_path=Path("trade_events.jsonl"),
        funnel_events_path=Path("funnel.jsonl"),
        phase1_root=Path("phase1"),
        output_dir=Path("before"),
    )
    backfill_result = backfill.publish_managed_exit_fill_backfill(
        repo_root=tmp_path,
        lifecycle_report_root=Path("lifecycles"),
        paper_order_control_events_path=Path("order_control.jsonl"),
        position_truth_path=Path("position_truth.json"),
        trade_registry_events_path=Path("trade_events.jsonl"),
        summary_path=Path("summary.json"),
    )
    after = perf.build_strategy_performance_attachment(
        repo_root=tmp_path,
        config_path=Path("config.json"),
        roster_path=Path("roster.json"),
        filled_bridge_results_path=Path("fills.jsonl"),
        trade_registry_events_path=Path("trade_events.jsonl"),
        funnel_events_path=Path("funnel.jsonl"),
        phase1_root=Path("phase1"),
        output_dir=Path("after"),
    )

    assert before.summary["pairing"]["paired_trades"] == 0
    assert backfill_result.appended_count == 1
    assert after.summary["pairing"]["paired_trades"] == 1
    assert after.summary["pairing"]["unpaired_exit_count"] == 0
    canonical = _event(after.canonical_trades_path, "CANONICAL_TRADE_RECORD")
    assert canonical["pairing_reason"] == "chronological_same_contract_risk_reducing_match"
    assert canonical["realized_pnl_points"] == "2"


def test_backfill_skips_close_fill_without_broker_execution_identity(tmp_path: Path) -> None:
    _write_lifecycle_report(
        tmp_path,
        lane_id="gc_globex_active_participation_short",
        symbol="GC",
        local_symbol="GCQ6",
        con_id=732156872,
        side="SHORT",
        close_action="BUY",
        lifecycle_id="reserved_submit_gc_globex_active_participation_short_1",
        trade_id="trade_gc_1",
        close_order_id="103",
        close_perm_id="9003",
        close_exec_id="",
    )
    _write_order_control(tmp_path, order_id="103", client_id="17086", perm_id="9003")

    result = backfill.publish_managed_exit_fill_backfill(
        repo_root=tmp_path,
        lifecycle_report_root=Path("lifecycles"),
        paper_order_control_events_path=Path("order_control.jsonl"),
        position_truth_path=Path("position_truth.json"),
        trade_registry_events_path=Path("trade_events.jsonl"),
        summary_path=Path("summary.json"),
    )

    assert result.appended_count == 0
    assert result.skipped_count == 1
    assert result.skipped[0]["skip_reason"] == "close_fill_missing_required_broker_ids"
    assert not (tmp_path / "trade_events.jsonl").exists()


def test_backfill_module_has_no_broker_or_runtime_mutation_imports() -> None:
    source = inspect.getsource(backfill)
    forbidden_imports = (
        "ib_insync",
        "ibapi",
        "socket",
        "subprocess",
        "ibkr_broker_truth_refresher",
        "ibkr_paper_strategy_bridge",
        "track_b_managed_exit_actuator",
        "track_b_managed_exit_service",
    )
    for token in forbidden_imports:
        assert token not in source


def _write_lifecycle_report(
    tmp_path: Path,
    *,
    lane_id: str,
    symbol: str,
    local_symbol: str,
    con_id: int,
    side: str,
    close_action: str,
    lifecycle_id: str,
    trade_id: str,
    close_order_id: str,
    close_perm_id: str,
    close_exec_id: str,
    close_price: str = "101",
    close_time: str = "2026-06-24T00:05:00Z",
) -> None:
    path = tmp_path / "lifecycles" / lifecycle_id / "track_b_strategy_managed_paper_lifecycle_report.json"
    path.parent.mkdir(parents=True)
    payload = {
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "lane_id": lane_id,
        "strategy_id": lane_id,
        "account_id": "DUM882026",
        "instrument_family": symbol,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "contract_key": f"{symbol}-202608",
        "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        "entry_intent": {
            "trade_id": trade_id,
            "lifecycle_id": lifecycle_id,
            "strategy_id": lane_id,
            "account_id": "DUM882026",
            "instrument_family": symbol,
            "local_symbol": local_symbol,
            "con_id": con_id,
            "contract_key": f"{symbol}-202608",
            "order_action": "BUY" if side == "LONG" else "SELL",
            "side": side,
            "quantity": "1",
            "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        },
        "close_intent": {
            "trade_id": trade_id,
            "lifecycle_id": lifecycle_id,
            "strategy_id": lane_id,
            "account_id": "DUM882026",
            "instrument_family": symbol,
            "local_symbol": local_symbol,
            "con_id": con_id,
            "contract_key": f"{symbol}-202608",
            "expiry": "20260827",
            "order_action": close_action,
            "side": side,
            "quantity": "1",
            "close_reason": "TIME_BOXED_EXIT",
            "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        },
        "close_submit_attempt": {
            "broker_order_id": close_order_id,
            "broker_order": {"client_id": 17086},
            "close_fill": {
                "broker_order_id": close_order_id,
                "perm_id": close_perm_id,
                "execution_id": close_exec_id,
                "price": close_price,
                "quantity": "1",
                "filled_at": close_time,
            },
        },
        "close_fill": {
            "broker_order_id": close_order_id,
            "perm_id": close_perm_id,
            "execution_id": close_exec_id,
            "price": close_price,
            "quantity": "1",
            "filled_at": close_time,
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_order_control(tmp_path: Path, *, order_id: str, client_id: str, perm_id: str) -> None:
    _write_jsonl(
        tmp_path / "order_control.jsonl",
        [
            {
                "order_id": order_id,
                "client_id": client_id,
                "perm_id": perm_id,
                "status": "Filled",
                "timestamp": "2026-06-24T00:05:00Z",
            }
        ],
    )


def _write_profile(tmp_path: Path, lane_ids: list[str]) -> None:
    (tmp_path / "config.json").write_text(json.dumps({"active_lane_ids": lane_ids}), encoding="utf-8")
    (tmp_path / "roster.json").write_text(
        json.dumps({"enabled_strategy_ids": [perf._lane_metadata_from_lane_id(lane).strategy_id for lane in lane_ids]}),
        encoding="utf-8",
    )


def _entry(
    *,
    lane_id: str,
    symbol: str,
    side: str,
    intent_type: str,
    price: str,
    multiplier: str,
    fill_time: str,
) -> dict[str, object]:
    return {
        "bridge_order_status": "FILLED",
        "intent_type": intent_type,
        "lane_id": lane_id,
        "symbol": symbol,
        "instrument": symbol,
        "position_side": "LONG" if side == "BUY" else "SHORT",
        "quantity": 1,
        "fill_price": price,
        "fill_timestamp": fill_time,
        "lifecycle_id": f"bridge_fill_{symbol}|1m|2026-06-24T00:00:00Z|{intent_type}",
        "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        "contract": {"multiplier": multiplier, "local_symbol": f"{symbol}U6", "con_id": 123},
    }


def _write_candles(
    tmp_path: Path,
    symbol: str,
    *,
    highs: list[int],
    lows: list[int],
    closes: list[int] | None = None,
) -> None:
    closes = closes or highs
    bars = []
    for index, (high, low, close) in enumerate(zip(highs, lows, closes), start=2):
        bars.append(
            {
                "bar_end": f"2026-06-24T00:0{index}:00+00:00",
                "high": high,
                "low": low,
                "close": close,
                "completed": True,
            }
        )
    path = tmp_path / "phase1" / symbol / "1m" / "latest_runtime_candles.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"bars": bars}), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _event(path: Path, event_type: str) -> dict[str, object]:
    for line in path.read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        if row["event_type"] == event_type:
            return row
    raise AssertionError(f"missing event {event_type}")
