from __future__ import annotations

import inspect
import json
import time
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core import track_b_post_trade_analytics_refresh as refresh
from mgc_v05l.execution_core import track_b_strategy_performance_attachment as perf


def test_post_trade_refresh_rebuilds_path_and_overlay(tmp_path: Path) -> None:
    lane_id = "mbt_us_active_participation_long"
    _write_profile(tmp_path, [lane_id])
    lifecycle_id = "bridge_fill_MBT|1m|2026-06-26T14:30:00Z|BUY_TO_OPEN"
    _write_jsonl(
        tmp_path / "filled_bridge_results.jsonl",
        [
            _entry(
                lane_id=lane_id,
                symbol="MBT",
                intent_type="BUY_TO_OPEN",
                price="100",
                fill_time="2026-06-26T14:30:00Z",
                lifecycle_id=lifecycle_id,
            )
        ],
    )
    _write_jsonl(
        tmp_path / "trade_registry.jsonl",
        [
            _exit(
                lane_id=lane_id,
                symbol="MBT",
                action="SELL",
                price="105",
                generated_at="2026-06-26T14:50:00Z",
                lifecycle_id=lifecycle_id,
            )
        ],
    )
    _write_jsonl(tmp_path / "funnel.jsonl", [])
    _write_candle_file(
        tmp_path,
        "MBT",
        "1m",
        [
            ("2026-06-26T14:35:00Z", "102", "100", "102"),
            ("2026-06-26T14:40:00Z", "104", "101", "103"),
            ("2026-06-26T14:45:00Z", "106", "102", "105"),
            ("2026-06-26T14:50:00Z", "107", "103", "105"),
        ],
    )
    _write_candle_file(
        tmp_path,
        "MBT",
        "5m",
        [
            ("2026-06-26T14:35:00Z", "102", "100", "102"),
            ("2026-06-26T14:40:00Z", "104", "101", "103"),
            ("2026-06-26T14:45:00Z", "106", "102", "105"),
            ("2026-06-26T14:50:00Z", "107", "103", "105"),
        ],
    )

    result = refresh.run_post_trade_analytics_refresh(
        repo_root=tmp_path,
        config_path=Path("config.json"),
        roster_path=Path("roster.json"),
        filled_bridge_results_path=Path("filled_bridge_results.jsonl"),
        trade_registry_events_path=Path("trade_registry.jsonl"),
        funnel_events_path=Path("funnel.jsonl"),
        phase1_root=Path("phase1"),
        strategy_performance_output_dir=Path("strategy_performance"),
        side_session_output_dir=Path("side_session"),
        trend_output_dir=Path("trend_overlay"),
        summary_path=Path("analytics_refresh_summary.json"),
        now=datetime(2026, 6, 26, 14, 55, tzinfo=UTC),
    )

    assert result.summary["classification"] == "POST_TRADE_ANALYTICS_REFRESH_READY"
    assert result.summary["strategy_performance"]["canonical_trade_count"] == 1
    assert result.summary["side_session_attribution"]["path_available_count"] == 1
    assert result.summary["forward_path_capture"]["rows"] == 2
    assert result.summary["trend_continuation_overlay"]["path_available_count"] == 1
    states = result.summary["trend_continuation_overlay"]["state_counts"]
    assert states.get("INSUFFICIENT_PATH_DATA", 0) == 0
    assert _read_jsonl(tmp_path / "side_session" / "forward_path_capture.jsonl")


def test_post_trade_refresh_is_idempotent_for_same_capture_window(tmp_path: Path) -> None:
    lane_id = "gc_globex_active_participation_short"
    _write_profile(tmp_path, [lane_id])
    lifecycle_id = "bridge_fill_GC|1m|2026-06-26T14:30:00Z|SELL_TO_OPEN"
    _write_jsonl(
        tmp_path / "filled_bridge_results.jsonl",
        [
            _entry(
                lane_id=lane_id,
                symbol="GC",
                intent_type="SELL_TO_OPEN",
                price="100",
                fill_time="2026-06-26T14:30:00Z",
                lifecycle_id=lifecycle_id,
            )
        ],
    )
    _write_jsonl(
        tmp_path / "trade_registry.jsonl",
        [
            _exit(
                lane_id=lane_id,
                symbol="GC",
                action="BUY",
                price="95",
                generated_at="2026-06-26T14:50:00Z",
                lifecycle_id=lifecycle_id,
            )
        ],
    )
    _write_jsonl(tmp_path / "funnel.jsonl", [])
    _write_candle_file(
        tmp_path,
        "GC",
        "1m",
        [
            ("2026-06-26T14:35:00Z", "100", "98", "98"),
            ("2026-06-26T14:40:00Z", "99", "96", "97"),
            ("2026-06-26T14:45:00Z", "98", "94", "95"),
        ],
    )
    now = datetime(2026, 6, 26, 14, 55, tzinfo=UTC)

    kwargs = {
        "repo_root": tmp_path,
        "config_path": Path("config.json"),
        "roster_path": Path("roster.json"),
        "filled_bridge_results_path": Path("filled_bridge_results.jsonl"),
        "trade_registry_events_path": Path("trade_registry.jsonl"),
        "funnel_events_path": Path("funnel.jsonl"),
        "phase1_root": Path("phase1"),
        "strategy_performance_output_dir": Path("strategy_performance"),
        "side_session_output_dir": Path("side_session"),
        "trend_output_dir": Path("trend_overlay"),
        "summary_path": Path("analytics_refresh_summary.json"),
        "now": now,
    }
    refresh.run_post_trade_analytics_refresh(**kwargs)
    refresh.run_post_trade_analytics_refresh(**kwargs)

    captures = _read_jsonl(tmp_path / "side_session" / "forward_path_capture.jsonl")
    assert sum(1 for row in captures if row["symbol"] == "GC" and row["timeframe"] == "1m") == 1


def test_post_trade_refresh_failure_is_diagnostic_only(tmp_path: Path) -> None:
    def slow_stage() -> dict[str, object]:
        time.sleep(0.05)
        return {"ok": True}

    result = refresh.run_post_trade_analytics_refresh(
        repo_root=tmp_path,
        include_terminal_publisher=False,
        include_managed_exit_backfill=False,
        max_stage_seconds=0.001,
        summary_path=Path("analytics_refresh_summary.json"),
        stage_overrides={"strategy_performance_attachment": slow_stage},
        now=datetime(2026, 6, 26, 14, 55, tzinfo=UTC),
    )

    assert result.summary["classification"] == "POST_TRADE_ANALYTICS_REFRESH_DEGRADED_DIAGNOSTIC_ONLY"
    assert result.summary["trading_blocking"] is False
    assert result.summary["broker_mutation_allowed"] is False
    assert any(stage["status"] == "TIMEOUT_DIAGNOSTIC_ONLY" for stage in result.summary["stages"])


def test_post_trade_refresh_module_has_no_broker_runtime_or_authority_imports() -> None:
    source = inspect.getsource(refresh)
    forbidden = (
        "ib_insync",
        "ibapi",
        "socket",
        "probationary_runtime",
        "ibkr_paper_strategy_bridge",
        "track_b_managed_exit_service",
        "track_b_managed_exit_actuator",
        "placeOrder",
        "cancelOrder",
        "reqGlobalCancel",
    )
    for token in forbidden:
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
    intent_type: str,
    price: str,
    fill_time: str,
    lifecycle_id: str,
) -> dict[str, object]:
    return {
        "bridge_order_status": "FILLED",
        "intent_type": intent_type,
        "lane_id": lane_id,
        "symbol": symbol,
        "instrument": symbol,
        "position_side": "LONG" if intent_type == "BUY_TO_OPEN" else "SHORT",
        "quantity": 1,
        "fill_price": price,
        "fill_timestamp": fill_time,
        "lifecycle_id": lifecycle_id,
        "trade_id": f"trade_{lifecycle_id}",
        "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        "contract": {"multiplier": "1", "local_symbol": f"{symbol}U6", "con_id": 123},
    }


def _exit(
    *,
    lane_id: str,
    symbol: str,
    action: str,
    price: str,
    generated_at: str,
    lifecycle_id: str,
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
        "trade_id": f"trade_{lifecycle_id}",
        "order_id": "99",
        "perm_id": "199",
        "exec_id": f"exec.{symbol}",
        "metadata": {"managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"},
    }


def _write_candle_file(tmp_path: Path, symbol: str, timeframe: str, rows: list[tuple[str, str, str, str]]) -> None:
    bars = [
        {
            "bar_end": end,
            "completed": True,
            "high": high,
            "low": low,
            "close": close,
        }
        for end, high, low, close in rows
    ]
    path = tmp_path / "phase1" / symbol / timeframe / "latest_runtime_candles.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"bars": bars}), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
