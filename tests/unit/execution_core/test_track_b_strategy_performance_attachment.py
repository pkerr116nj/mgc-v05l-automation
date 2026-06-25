from __future__ import annotations

import inspect
import json
from pathlib import Path

from mgc_v05l.execution_core import track_b_strategy_performance_attachment as perf


def test_closed_long_mfe_mae_and_pnl(tmp_path: Path) -> None:
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
    _write_jsonl(
        tmp_path / "trade_events.jsonl",
        [
            _exit(
                lane_id="mgc_globex_active_participation_long",
                symbol="MGC",
                action="SELL",
                price="103",
                generated_at="2026-06-24T00:05:00Z",
            )
        ],
    )
    _write_jsonl(tmp_path / "funnel.jsonl", [])
    _write_candles(tmp_path, "MGC", highs=[101, 105, 104], lows=[99, 98, 102])

    result = perf.build_strategy_performance_attachment(
        repo_root=tmp_path,
        config_path=Path("config.json"),
        roster_path=Path("roster.json"),
        filled_bridge_results_path=Path("fills.jsonl"),
        trade_registry_events_path=Path("trade_events.jsonl"),
        funnel_events_path=Path("funnel.jsonl"),
        phase1_root=Path("phase1"),
        output_dir=Path("out"),
    )

    closed = _event(result.events_path, "TRADE_CLOSED")
    assert closed["side"] == "LONG"
    assert closed["mfe_points"] == "5"
    assert closed["mae_points"] == "-2"
    assert closed["realized_pnl_points"] == "3"
    assert closed["realized_pnl_currency"] == "30"
    assert closed["hold_seconds"] == 240


def test_closed_short_mfe_mae_and_pnl(tmp_path: Path) -> None:
    _write_profile(tmp_path, ["gc_globex_active_participation_short"])
    _write_jsonl(
        tmp_path / "fills.jsonl",
        [
            _entry(
                lane_id="gc_globex_active_participation_short",
                symbol="GC",
                side="SELL",
                intent_type="SELL_TO_OPEN",
                price="200",
                multiplier="100",
                fill_time="2026-06-24T00:01:00Z",
            )
        ],
    )
    _write_jsonl(
        tmp_path / "trade_events.jsonl",
        [
            _exit(
                lane_id="gc_globex_active_participation_short",
                symbol="GC",
                action="BUY",
                price="197",
                generated_at="2026-06-24T00:05:00Z",
            )
        ],
    )
    _write_jsonl(tmp_path / "funnel.jsonl", [])
    _write_candles(tmp_path, "GC", highs=[201, 202, 198], lows=[199, 195, 196])

    result = perf.build_strategy_performance_attachment(
        repo_root=tmp_path,
        config_path=Path("config.json"),
        roster_path=Path("roster.json"),
        filled_bridge_results_path=Path("fills.jsonl"),
        trade_registry_events_path=Path("trade_events.jsonl"),
        funnel_events_path=Path("funnel.jsonl"),
        phase1_root=Path("phase1"),
        output_dir=Path("out"),
    )

    closed = _event(result.events_path, "TRADE_CLOSED")
    assert closed["side"] == "SHORT"
    assert closed["mfe_points"] == "5"
    assert closed["mae_points"] == "-2"
    assert closed["realized_pnl_points"] == "3"
    assert closed["realized_pnl_currency"] == "300"


def test_open_trade_mark_to_market(tmp_path: Path) -> None:
    _write_profile(tmp_path, ["es_us_active_participation_long"])
    _write_jsonl(
        tmp_path / "fills.jsonl",
        [
            _entry(
                lane_id="es_us_active_participation_long",
                symbol="ES",
                side="BUY",
                intent_type="BUY_TO_OPEN",
                price="5000",
                multiplier="50",
                fill_time="2026-06-24T00:01:00Z",
            )
        ],
    )
    _write_jsonl(tmp_path / "trade_events.jsonl", [])
    _write_jsonl(tmp_path / "funnel.jsonl", [])
    _write_candles(tmp_path, "ES", highs=[5002, 5004], lows=[4999, 4998], closes=[5001, 5003])

    result = perf.build_strategy_performance_attachment(
        repo_root=tmp_path,
        config_path=Path("config.json"),
        roster_path=Path("roster.json"),
        filled_bridge_results_path=Path("fills.jsonl"),
        trade_registry_events_path=Path("trade_events.jsonl"),
        funnel_events_path=Path("funnel.jsonl"),
        phase1_root=Path("phase1"),
        output_dir=Path("out"),
    )

    mark = _event(result.events_path, "TRADE_MARK_TO_MARKET")
    assert mark["mark_price"] == "5003"
    assert mark["mark_to_market_pnl_points"] == "3"
    assert mark["mark_to_market_pnl_currency"] == "150"
    assert mark["realized_pnl_points"] is None


def test_no_trade_blocker_capture(tmp_path: Path) -> None:
    _write_profile(tmp_path, ["nq_us_active_participation_short"])
    _write_jsonl(tmp_path / "fills.jsonl", [])
    _write_jsonl(tmp_path / "trade_events.jsonl", [])
    _write_jsonl(
        tmp_path / "funnel.jsonl",
        [
            {
                "timestamp": "2026-06-24T00:01:00Z",
                "lane_id": "nq_us_active_participation_short",
                "strategy_family": "paper_active_evidence",
                "stage": "rule_fail",
                "pass_fail": "FAIL",
                "reason": "warmup_incomplete",
                "blocker_classification": "FILTER_REJECTED",
                "session_phase": "US_CASH_OPEN_IMPULSE",
            }
        ],
    )

    result = perf.build_strategy_performance_attachment(
        repo_root=tmp_path,
        config_path=Path("config.json"),
        roster_path=Path("roster.json"),
        filled_bridge_results_path=Path("fills.jsonl"),
        trade_registry_events_path=Path("trade_events.jsonl"),
        funnel_events_path=Path("funnel.jsonl"),
        phase1_root=Path("phase1"),
        output_dir=Path("out"),
    )

    blocked = _event(result.events_path, "NO_TRADE_BLOCKED")
    assert blocked["no_trade_reason"] == "warmup_incomplete"
    assert blocked["blocker_classification"] == "FILTER_REJECTED"


def test_complete_lifecycle_pairing_uses_chronological_managed_exit_when_lifecycle_ids_differ(tmp_path: Path) -> None:
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
    _write_jsonl(
        tmp_path / "trade_events.jsonl",
        [
            _exit(
                lane_id="mgc_globex_active_participation_long",
                symbol="MGC",
                action="SELL",
                price="101",
                generated_at="2026-06-24T00:20:00Z",
                lifecycle_id="reserved_submit_mgc_globex_active_participation_long_20260624T002000Z",
                trade_id="trade_reserved_exit",
            )
        ],
    )
    _write_jsonl(tmp_path / "funnel.jsonl", [])
    _write_candles(tmp_path, "MGC", highs=[101, 102], lows=[99, 100])

    result = perf.build_strategy_performance_attachment(
        repo_root=tmp_path,
        config_path=Path("config.json"),
        roster_path=Path("roster.json"),
        filled_bridge_results_path=Path("fills.jsonl"),
        trade_registry_events_path=Path("trade_events.jsonl"),
        funnel_events_path=Path("funnel.jsonl"),
        phase1_root=Path("phase1"),
        output_dir=Path("out"),
    )

    canonical = _event(result.canonical_trades_path, "CANONICAL_TRADE_RECORD")
    assert canonical["pairing_status"] == "PAIRED"
    assert canonical["pairing_reason"] == "chronological_same_contract_risk_reducing_match"
    assert canonical["trade_status"] == "CLOSED"
    assert canonical["exit_reason"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"
    assert result.summary["pairing"]["paired_trades"] == 1
    assert result.summary["pairing"]["pairing_rate"] == 1.0


def test_pairing_summary_reports_unpaired_entry_and_exit(tmp_path: Path) -> None:
    _write_profile(tmp_path, ["gc_globex_active_participation_short"])
    _write_jsonl(
        tmp_path / "fills.jsonl",
        [
            _entry(
                lane_id="gc_globex_active_participation_short",
                symbol="GC",
                side="SELL",
                intent_type="SELL_TO_OPEN",
                price="200",
                multiplier="100",
                fill_time="2026-06-24T00:01:00Z",
            )
        ],
    )
    _write_jsonl(
        tmp_path / "trade_events.jsonl",
        [
            _exit(
                lane_id="gc_globex_active_participation_short",
                symbol="GC",
                action="SELL",
                price="201",
                generated_at="2026-06-24T00:20:00Z",
            )
        ],
    )
    _write_jsonl(tmp_path / "funnel.jsonl", [])
    _write_candles(tmp_path, "GC", highs=[202], lows=[198])

    result = perf.build_strategy_performance_attachment(
        repo_root=tmp_path,
        config_path=Path("config.json"),
        roster_path=Path("roster.json"),
        filled_bridge_results_path=Path("fills.jsonl"),
        trade_registry_events_path=Path("trade_events.jsonl"),
        funnel_events_path=Path("funnel.jsonl"),
        phase1_root=Path("phase1"),
        output_dir=Path("out"),
    )

    pairing = json.loads(result.pairing_summary_path.read_text())
    assert pairing["total_entries"] == 1
    assert pairing["total_exits"] == 1
    assert pairing["paired_trades"] == 0
    assert pairing["unpaired_entry_reasons"] == {"no_exit_fill_after_entry": 1}
    assert pairing["unpaired_exit_reasons"] == {"no_matching_entry_for_exit_fill": 1}


def test_backfilled_managed_exit_without_entry_is_ignored_for_canonical_performance(tmp_path: Path) -> None:
    _write_profile(tmp_path, ["gc_globex_active_participation_short"])
    _write_jsonl(tmp_path / "fills.jsonl", [])
    exit_row = _exit(
        lane_id="gc_globex_active_participation_short",
        symbol="GC",
        action="BUY",
        price="197",
        generated_at="2026-06-24T00:05:00Z",
    )
    exit_row["metadata"] = {
        "source": "track_b_managed_exit_fill_registry_backfill",
        "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
    }
    _write_jsonl(tmp_path / "trade_events.jsonl", [exit_row])
    _write_jsonl(tmp_path / "funnel.jsonl", [])
    _write_candles(tmp_path, "GC", highs=[201], lows=[195])

    result = perf.build_strategy_performance_attachment(
        repo_root=tmp_path,
        config_path=Path("config.json"),
        roster_path=Path("roster.json"),
        filled_bridge_results_path=Path("fills.jsonl"),
        trade_registry_events_path=Path("trade_events.jsonl"),
        funnel_events_path=Path("funnel.jsonl"),
        phase1_root=Path("phase1"),
        output_dir=Path("out"),
    )

    pairing = json.loads(result.pairing_summary_path.read_text())
    assert pairing["total_exits"] == 1
    assert pairing["unpaired_exit_count"] == 0
    assert pairing["ignored_unmatched_exit_count"] == 1
    assert pairing["ignored_unmatched_exit_reasons"] == {
        "backfilled_managed_exit_without_entry_artifact": 1
    }
    assert not result.canonical_trades_path.read_text().strip()


def test_pilot_lane_filtering_includes_crypto_active_lanes(tmp_path: Path) -> None:
    _write_profile(
        tmp_path,
        [
            "mgc_globex_active_participation_long",
            "gc_us_active_participation_long",
            "es_us_active_participation_long",
            "mbt_london_open_active_participation_short",
            "met_globex_active_participation_long",
            "zn_us_active_participation_long",
        ],
    )

    lanes = perf.load_pilot_lane_metadata(
        repo_root=tmp_path,
        config_path=Path("config.json"),
        roster_path=Path("roster.json"),
        pilot_only=True,
    )

    assert set(lanes) == {
        "mgc_globex_active_participation_long",
        "es_us_active_participation_long",
        "mbt_london_open_active_participation_short",
        "met_globex_active_participation_long",
    }
    assert lanes["mbt_london_open_active_participation_short"].session_label == "LONDON_OPEN"


def test_default_lane_metadata_covers_all_active_participation_lanes(tmp_path: Path) -> None:
    _write_profile(
        tmp_path,
        [
            "mgc_globex_active_participation_long",
            "zn_us_active_participation_long",
            "zb_london_late_active_participation_short",
            "mnq_us_derivative_bear_turn",
        ],
    )

    lanes = perf.load_pilot_lane_metadata(repo_root=tmp_path, config_path=Path("config.json"), roster_path=Path("roster.json"))

    assert set(lanes) == {
        "mgc_globex_active_participation_long",
        "zn_us_active_participation_long",
        "zb_london_late_active_participation_short",
    }


def test_attachment_module_has_no_broker_or_execution_mutation_imports() -> None:
    source = inspect.getsource(perf)
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


def _exit(
    *,
    lane_id: str,
    symbol: str,
    action: str,
    price: str,
    generated_at: str,
    lifecycle_id: str | None = None,
    trade_id: str | None = None,
) -> dict[str, object]:
    return {
        "event_type": "EXIT_FILL_BROKER_BACKED",
        "lane_id": lane_id,
        "symbol": symbol,
        "action": action,
        "side": "LONG" if action == "SELL" else "SHORT",
        "qty": "1",
        "price": price,
        "generated_at": generated_at,
        "lifecycle_id": lifecycle_id,
        "trade_id": trade_id,
        "order_id": "99",
        "perm_id": "199",
        "exec_id": "exec.1",
        "metadata": {"managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"},
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


def _event(path: Path, event_type: str) -> dict[str, object]:
    for line in path.read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        if row["event_type"] == event_type:
            return row
    raise AssertionError(f"missing event {event_type}")
