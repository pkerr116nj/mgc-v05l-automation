from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from mgc_v05l.execution_core.track_b_asian_drift_watch_chain import (
    TrackBAsianDriftWatchChainVerdict,
    run_track_b_asian_drift_watch_chain,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 5, 1, 15, tzinfo=timezone.utc)


def one_minute_payload(closes: list[float], *, completed: bool = True) -> dict[str, object]:
    start = datetime(2026, 5, 4, 18, 1, tzinfo=ZoneInfo("America/New_York"))
    candles: list[dict[str, object]] = []
    previous_close = closes[0] if closes else 4575.0
    for index, close in enumerate(closes):
        ts = start + timedelta(minutes=index)
        open_price = previous_close if index else close
        high = max(open_price, close) + 0.2
        low = min(open_price, close) - 0.2
        candles.append(
            {
                "candle_timestamp": ts.isoformat(),
                "timeframe": "1m",
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": 4,
                "completed": completed,
            }
        )
        previous_close = close
    return {
        "contract_key": "MGC-202606",
        "instrument_family": "MGC",
        "local_symbol": "MGCM6",
        "dataset": "GLBX.MDP3",
        "timeframe": "1m",
        "quote_provider_mode": "REALTIME",
        "realtime_quote_received": True,
        "current_quote_available": True,
        "candles": candles,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }


def phase1_one_minute_payload(closes: list[float], *, completed: bool = True) -> dict[str, object]:
    start = datetime(2026, 5, 4, 18, 1, tzinfo=ZoneInfo("America/New_York"))
    previous_close = closes[0] if closes else 4575.0
    bars: list[dict[str, object]] = []
    for index, close in enumerate(closes):
        bar_start = start + timedelta(minutes=index)
        bar_end = bar_start + timedelta(minutes=1)
        open_price = previous_close if index else close
        bars.append(
            {
                "bar_start": bar_start.isoformat(),
                "bar_end": bar_end.isoformat(),
                "open": open_price,
                "high": max(open_price, close) + 0.2,
                "low": min(open_price, close) - 0.2,
                "close": close,
                "volume": 4,
                "completed": completed,
            }
        )
        previous_close = close
    return {
        "symbol": "MGC",
        "timeframe": "1m",
        "dataset": "GLBX.MDP3",
        "source_id": "DATABENTO_REALTIME_PHASE1_mgc",
        "realtime_feed_confirmed": True,
        "realtime_feed_block_reason": "READY",
        "last_completed_bar_ts": bars[-1]["bar_end"] if bars else None,
        "bars": bars,
        "submit_allowed": False,
        "paper_trade_allowed": False,
        "live_money_eligible": False,
    }


def flat_1m_closes(count: int) -> list[float]:
    return [4575.0 + (0.03 if index % 2 == 0 else -0.03) for index in range(count)]


def long_signal_1m_closes() -> list[float]:
    five_minute_closes = [4575.0, 4576.2, 4577.5, 4578.8, 4580.0, 4581.3, 4582.4, 4583.0, 4583.5, 4582.9]
    values: list[float] = []
    previous = five_minute_closes[0]
    for close in five_minute_closes:
        step = (close - previous) / 5.0
        values.extend(previous + step * (index + 1) for index in range(5))
        previous = close
    return values


def test_watch_chain_aggregates_1m_to_5m_and_reaches_no_signal(tmp_path: Path) -> None:
    result = run_track_b_asian_drift_watch_chain(
        candle_payload=one_minute_payload(flat_1m_closes(40)),
        output_root=tmp_path / "asian_drift_state",
        feature_rows_output_root=tmp_path / "asian_drift_state",
        strategy_rule_output_root=tmp_path / "rule",
        inbox_dir=tmp_path / "inbox",
        chain_id="no-signal-chain",
        now=aware_now(),
    )

    assert result.verdict == TrackBAsianDriftWatchChainVerdict.NO_SIGNAL_NO_MUTATION
    assert result.report["completed_5m_bars_available"] == 8
    assert result.report["feature_rows_available"] == 8
    assert result.report["asian_drift_state_ready"] is True
    assert result.report["rule_decision"] == "NO_SIGNAL"
    assert result.report["signal_emitted"] is False
    assert result.report["readiness_invoked"] is False
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_watch_chain_accepts_phase1_runtime_bars_schema_for_asian_drift(tmp_path: Path) -> None:
    source = (
        tmp_path
        / "outputs/track_b_execution_core/phase1_runtime_market_data/MGC/1m/latest_runtime_candles.json"
    )
    result = run_track_b_asian_drift_watch_chain(
        candle_payload=phase1_one_minute_payload(flat_1m_closes(40)),
        source_payload_path=source,
        output_root=tmp_path / "asian_drift_state",
        feature_rows_output_root=tmp_path / "asian_drift_state",
        strategy_rule_output_root=tmp_path / "rule",
        inbox_dir=tmp_path / "inbox",
        chain_id="phase1-chain",
        now=aware_now(),
    )

    assert result.verdict == TrackBAsianDriftWatchChainVerdict.NO_SIGNAL_NO_MUTATION
    assert result.completed_5m_candles_payload is not None
    assert result.completed_5m_candles_payload["source_category"] == "PHASE1_RUNTIME_MARKET_DATA"
    assert result.completed_5m_candles_payload["source_authority_path"] == str(source)
    assert result.completed_5m_candles_payload["phase1_runtime_market_data_authority"] is True
    assert result.report["submit_attempted"] is False


def test_watch_chain_rejects_legacy_runtime_path_without_explicit_flag(tmp_path: Path) -> None:
    legacy = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_runtime_candle_capture/latest_runtime_mgc_1m_candles.json"
    )
    result = run_track_b_asian_drift_watch_chain(
        candle_payload=one_minute_payload(flat_1m_closes(40)),
        source_payload_path=legacy,
        output_root=tmp_path / "asian_drift_state",
        feature_rows_output_root=tmp_path / "asian_drift_state",
        inbox_dir=tmp_path / "inbox",
        chain_id="legacy-chain",
        now=aware_now(),
    )

    assert result.verdict == TrackBAsianDriftWatchChainVerdict.BLOCKED_SCHEMA_ERROR
    assert "legacy runtime candle path is diagnostic-only" in str(result.report["primary_blocker"])
    assert result.report["submit_attempted"] is False


def test_watch_chain_blocks_with_fewer_than_eight_completed_5m_bars(tmp_path: Path) -> None:
    result = run_track_b_asian_drift_watch_chain(
        candle_payload=one_minute_payload(flat_1m_closes(35)),
        output_root=tmp_path / "asian_drift_state",
        feature_rows_output_root=tmp_path / "asian_drift_state",
        inbox_dir=tmp_path / "inbox",
        chain_id="short-chain",
        now=aware_now(),
    )

    assert result.verdict == TrackBAsianDriftWatchChainVerdict.NOT_READY_FOR_TONIGHT
    assert result.report["completed_5m_bars_available"] == 7
    assert result.report["feature_rows_available"] == 7
    assert result.report["asian_drift_state_ready"] is False
    assert result.report["rule_evaluated"] is False
    assert result.report["submit_attempted"] is False


def test_watch_chain_blocks_on_incomplete_source_bars(tmp_path: Path) -> None:
    result = run_track_b_asian_drift_watch_chain(
        candle_payload=one_minute_payload(flat_1m_closes(40), completed=False),
        output_root=tmp_path / "asian_drift_state",
        feature_rows_output_root=tmp_path / "asian_drift_state",
        inbox_dir=tmp_path / "inbox",
        chain_id="incomplete-chain",
        now=aware_now(),
    )

    assert result.verdict == TrackBAsianDriftWatchChainVerdict.NOT_READY_FOR_TONIGHT
    assert result.report["completed_5m_bars_available"] == 0
    assert result.report["paper_proof_invoked"] is False
    assert result.report["broker_state_mutated"] is False


def test_watch_chain_blocks_stale_runtime_candles_when_freshness_required(tmp_path: Path) -> None:
    result = run_track_b_asian_drift_watch_chain(
        candle_payload=one_minute_payload(flat_1m_closes(40)),
        output_root=tmp_path / "asian_drift_state",
        feature_rows_output_root=tmp_path / "asian_drift_state",
        inbox_dir=tmp_path / "inbox",
        chain_id="stale-runtime-chain",
        now=datetime(2026, 5, 5, 12, 0, tzinfo=timezone.utc),
        max_completed_5m_age_seconds=900,
    )

    assert result.verdict == TrackBAsianDriftWatchChainVerdict.STALE_RUNTIME_CONTEXT
    assert result.report["asian_drift_watch_verdict"] == "ASIAN_DRIFT_NOT_READY_FOR_TONIGHT"
    assert result.report["runtime_candle_context_stale"] is True
    assert result.report["latest_1m_candle_timestamp"] is not None
    assert result.report["latest_completed_5m_candle_timestamp"] is not None
    assert result.report["latest_completed_5m_candle_age_seconds"] > 900
    assert result.report["rule_evaluated"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False


def test_watch_chain_accepts_fresh_runtime_candles_when_freshness_required(tmp_path: Path) -> None:
    result = run_track_b_asian_drift_watch_chain(
        candle_payload=one_minute_payload(flat_1m_closes(40)),
        output_root=tmp_path / "asian_drift_state",
        feature_rows_output_root=tmp_path / "asian_drift_state",
        strategy_rule_output_root=tmp_path / "rule",
        inbox_dir=tmp_path / "inbox",
        chain_id="fresh-runtime-chain",
        now=aware_now(),
        max_completed_5m_age_seconds=18000,
    )

    assert result.verdict == TrackBAsianDriftWatchChainVerdict.NO_SIGNAL_NO_MUTATION
    assert result.report["runtime_candle_context_stale"] is False
    assert result.report["runtime_candle_context_fresh"] is True
    assert result.report["latest_completed_5m_candle_age_seconds"] <= 18000
    assert result.report["rule_evaluated"] is True
    assert result.report["submit_attempted"] is False


def test_watch_chain_reaches_signal_ready_no_submit_without_mutation(tmp_path: Path) -> None:
    result = run_track_b_asian_drift_watch_chain(
        candle_payload=one_minute_payload(long_signal_1m_closes()),
        output_root=tmp_path / "asian_drift_state",
        feature_rows_output_root=tmp_path / "asian_drift_state",
        strategy_rule_output_root=tmp_path / "rule",
        inbox_dir=tmp_path / "inbox",
        chain_id="signal-chain",
        now=aware_now(),
    )

    assert result.verdict == TrackBAsianDriftWatchChainVerdict.SIGNAL_READY_NO_SUBMIT
    assert result.report["completed_5m_bars_available"] == 10
    assert result.report["feature_rows_available"] == 10
    assert result.report["asian_drift_state_ready"] is True
    assert result.report["signal_source"] == "ASIAN_DRIFT_V1"
    assert result.report["real_strategy_signal"] is True
    assert result.report["signal_emitted"] is True
    assert result.report["signal_side"] == "LONG"
    assert result.report["readiness_invoked"] is False
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False
