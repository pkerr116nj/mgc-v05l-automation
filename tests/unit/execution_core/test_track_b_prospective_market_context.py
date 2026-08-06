from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_prospective_market_context import (
    CONTRACT_IDENTITY_MISMATCH,
    INSUFFICIENT_HISTORY,
    INVALID,
    INVALID_CONTRACT_IDENTITY,
    INVALID_DECISION_TIMESTAMP,
    NOT_APPLICABLE,
    NOT_YET_DEFINED,
    VALID,
    build_fixture_context_record,
    candle_is_usable,
    classify_opening_range_position,
    classify_trend_state,
    classify_volatility_state,
    classify_vwap_relationship,
    fingerprint,
    fixture_demo_record,
    ols_slope,
    prospective_market_context_schema,
    true_range,
    FixtureCandle,
)


DECISION = datetime(2026, 8, 7, 14, 35, tzinfo=UTC)
IDENTITY = {"instrument": "NQ", "local_symbol": "NQU6", "contract": "NQU6", "con_id": 770561204, "expiry": "202609"}


def test_decision_timestamp_missing_invalidates_record_and_prevents_computation() -> None:
    record = build_fixture_context_record(
        decision_timestamp=None,
        trade_identity=IDENTITY,
        entry_price=100.0,
        strategy_setup_family="setup",
        session="US",
        candles_1m=[],
        candles_5m=[],
    )

    assert record["record_status"] == INVALID_DECISION_TIMESTAMP
    assert all(field["status"] == INVALID for field in record["fields"].values())


def test_1m_and_5m_cutoff_independence_and_durability_boundaries() -> None:
    valid_equal = _candle(close_time=DECISION, generated_at=DECISION, timeframe_minutes=1)
    generated_after = _candle(close_time=DECISION - timedelta(minutes=1), generated_at=DECISION + timedelta(seconds=1), timeframe_minutes=1)
    future_5m = _candle(close_time=DECISION + timedelta(minutes=5), generated_at=DECISION + timedelta(minutes=5), timeframe_minutes=5)
    record = build_fixture_context_record(
        decision_timestamp=DECISION,
        trade_identity=IDENTITY,
        entry_price=100.0,
        strategy_setup_family="setup",
        session="US",
        candles_1m=[valid_equal, generated_after],
        candles_5m=[future_5m],
    )

    assert candle_is_usable(FixtureCandle.from_mapping(valid_equal), DECISION) is True
    assert candle_is_usable(FixtureCandle.from_mapping(generated_after), DECISION) is False
    assert record["completed_candle_cutoffs"]["1m"] == DECISION.isoformat()
    assert record["completed_candle_cutoffs"]["5m"] is None


def test_literal_contract_match_and_mismatch() -> None:
    candle = _candle()
    mismatch = _candle(local_symbol="MNQU6", contract="MNQU6")

    good = build_fixture_context_record(
        decision_timestamp=DECISION,
        trade_identity=IDENTITY,
        entry_price=100.0,
        strategy_setup_family="setup",
        session="US",
        candles_1m=[candle],
        candles_5m=[candle],
    )
    bad = build_fixture_context_record(
        decision_timestamp=DECISION,
        trade_identity=IDENTITY,
        entry_price=100.0,
        strategy_setup_family="setup",
        session="US",
        candles_1m=[mismatch],
        candles_5m=[mismatch],
    )
    invalid_identity = build_fixture_context_record(
        decision_timestamp=DECISION,
        trade_identity={"instrument": "NQ"},
        entry_price=100.0,
        strategy_setup_family="setup",
        session="US",
        candles_1m=[candle],
        candles_5m=[candle],
    )

    assert good["record_status"] == VALID
    assert bad["candle_series_identity"]["roll_status"] == CONTRACT_IDENTITY_MISMATCH
    assert bad["fields"]["vwap_relationship"]["status"] == INVALID
    assert invalid_identity["record_status"] == INVALID_CONTRACT_IDENTITY


def test_vwap_above_below_near() -> None:
    candles = [_candle(close=100.0, high=101.0, low=99.0, volume=10.0)]

    above = classify_vwap_relationship([FixtureCandle.from_mapping(c) for c in candles], DECISION, IDENTITY, 103.0, session="US")
    near = classify_vwap_relationship([FixtureCandle.from_mapping(c) for c in candles], DECISION, IDENTITY, 101.0, session="US")
    below = classify_vwap_relationship([FixtureCandle.from_mapping(c) for c in candles], DECISION, IDENTITY, 97.0, session="US")

    assert above["state"] == "ABOVE"
    assert near["state"] == "NEAR"
    assert below["state"] == "BELOW"


def test_opening_range_before_completion_london_not_applicable_and_states_exclusive() -> None:
    two = [_candle(close_time=DECISION - timedelta(minutes=25), generated_at=DECISION - timedelta(minutes=25), timeframe_minutes=5) for _ in range(2)]
    six = [
        _candle(close_time=DECISION - timedelta(minutes=25 - index * 5), generated_at=DECISION - timedelta(minutes=25 - index * 5), high=110.0, low=100.0, close=111.0, timeframe_minutes=5)
        for index in range(6)
    ]

    early = classify_opening_range_position([FixtureCandle.from_mapping(c) for c in two], DECISION, IDENTITY, 105.0, session="US")
    london = classify_opening_range_position([FixtureCandle.from_mapping(c) for c in six], DECISION, IDENTITY, 105.0, session="LONDON")
    extending = classify_opening_range_position([FixtureCandle.from_mapping(c) for c in six], DECISION, IDENTITY, 111.5, session="US")

    assert early["status"] == NOT_YET_DEFINED
    assert london["status"] == NOT_APPLICABLE
    assert extending["state"] == "EXTENDING_ABOVE"


def test_ols_oldest_to_newest_rising_fixture_positive_slope() -> None:
    closes = [100.0 + index * 3.0 for index in range(12)]
    candles = [
        _candle(close_time=DECISION - timedelta(minutes=(11 - index) * 5), generated_at=DECISION - timedelta(minutes=(11 - index) * 5), close=close, high=close + 1, low=close - 1, timeframe_minutes=5)
        for index, close in enumerate(closes)
    ]
    trend = classify_trend_state([FixtureCandle.from_mapping(c) for c in candles], DECISION, IDENTITY)

    assert ols_slope(closes) == 3.0
    assert trend["state"] == "UP"
    assert trend["slope_points_per_5m_bar"] == 3.0


def test_true_range_hand_calculation_and_cross_contract_rejection() -> None:
    previous = FixtureCandle.from_mapping(_candle(close=100.0, high=101.0, low=99.0))
    current = FixtureCandle.from_mapping(_candle(close=103.0, high=106.0, low=98.0))
    other = FixtureCandle.from_mapping(_candle(local_symbol="MNQU6", contract="MNQU6"))

    assert true_range(current, previous) == 8.0
    try:
        true_range(current, other)
    except ValueError as exc:
        assert "same-contract" in str(exc)
    else:
        raise AssertionError("cross-contract predecessor close was accepted")


def test_volatility_requires_85_candles_non_overlapping_windows_and_thresholds() -> None:
    short = [FixtureCandle.from_mapping(_candle(close_time=DECISION - timedelta(minutes=(84 - i) * 5), generated_at=DECISION - timedelta(minutes=(84 - i) * 5), timeframe_minutes=5)) for i in range(84)]
    assert classify_volatility_state(short, DECISION, IDENTITY)["status"] == INSUFFICIENT_HISTORY

    candles = []
    close = 100.0
    for index in range(85):
        tr = 4.0 if index <= 72 else 6.0
        candles.append(
            FixtureCandle.from_mapping(
                _candle(
                    close_time=DECISION - timedelta(minutes=(84 - index) * 5),
                    generated_at=DECISION - timedelta(minutes=(84 - index) * 5),
                    close=close,
                    high=close + tr / 2,
                    low=close - tr / 2,
                    timeframe_minutes=5,
                )
            )
        )
        close += 0.1
    result = classify_volatility_state(candles, DECISION, IDENTITY)

    assert result["status"] == VALID
    assert result["state"] == "HIGH"
    assert result["atr_ratio"] == 1.5


def test_deterministic_fingerprint_schema_and_cli_demo() -> None:
    first = fixture_demo_record()
    second = fixture_demo_record()

    assert first["deterministic_fingerprint"] == second["deterministic_fingerprint"]
    assert fingerprint(first) == first["deterministic_fingerprint"]
    assert prospective_market_context_schema()["guardrails"]["production_recommendation"] is False

    output = subprocess.check_output(
        ["./.venv/bin/python", "-m", "mgc_v05l.app.track_b_prospective_market_context", "--mode", "demo"],
        text=True,
    )
    parsed = json.loads(output)
    assert parsed["record_status"] == VALID


def test_no_real_artifact_imports_or_mutation_vocabulary() -> None:
    source = Path("src/mgc_v05l/execution_core/track_b_prospective_market_context.py").read_text()
    import_lines = [line for line in source.splitlines() if line.startswith(("import ", "from "))]
    banned_imports = ("ibapi", "mgc_v05l.runtime", "mgc_v05l.broker", "mgc_v05l.strategy", "mgc_v05l.guardian", "mgc_v05l.safe_state")
    banned_actions = ("placeOrder(", "cancelOrder(", "globalCancel(", "submit_order(", "flatten_position(")

    assert not any(term in line for line in import_lines for term in banned_imports)
    assert not any(term in source for term in banned_actions)
    assert "outputs/track_b_execution_core/research_analytics/canonical_research_record" not in source


def _candle(
    *,
    close_time: datetime | None = None,
    generated_at: datetime | None = None,
    instrument: str = "NQ",
    local_symbol: str = "NQU6",
    contract: str = "NQU6",
    con_id: int = 770561204,
    open_: float = 100.0,
    high: float = 101.0,
    low: float = 99.0,
    close: float = 100.0,
    volume: float = 10.0,
    timeframe_minutes: int = 1,
) -> dict[str, object]:
    close_time = close_time or (DECISION - timedelta(minutes=timeframe_minutes))
    generated_at = generated_at or close_time
    return {
        "close_time": close_time.isoformat(),
        "source_generated_at": generated_at.isoformat(),
        "instrument": instrument,
        "local_symbol": local_symbol,
        "contract": contract,
        "con_id": con_id,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }
