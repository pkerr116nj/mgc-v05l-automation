"""Fixture-only prospective market context contracts.

Phase 1 is deliberately pure and fixture-only. It does not read real candle
artifacts, CRR artifacts, broker state, runtime state, strategy state, Managed
Exit, Guardian, Safe-State, readiness, reconciliation, or trading gates.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from statistics import median
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "prospective_market_context_v1"
PRODUCER_VERSION = "prospective_market_context_fixture_producer_v1"
VWAP_CLASSIFIER_VERSION = "NQ_SESSION_VWAP_RELATIONSHIP_V1"
OPENING_RANGE_CLASSIFIER_VERSION = "NQ_OPENING_RANGE_POSITION_30M_V1"
TREND_CLASSIFIER_VERSION = "NQ_TREND_STATE_5M_LINEAR_SLOPE_V1"
VOLATILITY_CLASSIFIER_VERSION = "NQ_VOLATILITY_STATE_5M_ATR_RATIO_V1"

VALID = "VALID"
INVALID_DECISION_TIMESTAMP = "INVALID_DECISION_TIMESTAMP"
INVALID_CONTRACT_IDENTITY = "INVALID_CONTRACT_IDENTITY"
INVALID_SOURCE_PROVENANCE = "INVALID_SOURCE_PROVENANCE"

MISSING = "MISSING"
INVALID = "INVALID"
STALE = "STALE"
NOT_YET_DEFINED = "NOT_YET_DEFINED"
NOT_APPLICABLE = "NOT_APPLICABLE"
INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"

CONTRACT_MATCH_VALID = "CONTRACT_MATCH_VALID"
ROLL_ADJACENT = "ROLL_ADJACENT"
INSUFFICIENT_SAME_CONTRACT_HISTORY = "INSUFFICIENT_SAME_CONTRACT_HISTORY"
CONTRACT_IDENTITY_MISMATCH = "CONTRACT_IDENTITY_MISMATCH"

GUARDRAILS = {
    "diagnostic_only": True,
    "production_recommendation": False,
    "trading_gate": False,
}

SUPPORTED_OPENING_RANGE_SESSIONS = {"GLOBEX", "US"}


@dataclass(frozen=True)
class FixtureCandle:
    close_time: datetime
    source_generated_at: datetime | None
    instrument: str
    local_symbol: str
    contract: str
    con_id: int | str
    open: float
    high: float
    low: float
    close: float
    volume: float

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> "FixtureCandle":
        durability = row.get("source_generated_at") or row.get("durability_watermark")
        return cls(
            close_time=parse_timestamp(row.get("close_time")),
            source_generated_at=parse_timestamp(durability) if durability else None,
            instrument=str(row.get("instrument") or ""),
            local_symbol=str(row.get("local_symbol") or ""),
            contract=str(row.get("contract") or ""),
            con_id=row.get("con_id"),
            open=float(row.get("open")),
            high=float(row.get("high")),
            low=float(row.get("low")),
            close=float(row.get("close")),
            volume=float(row.get("volume")),
        )


def prospective_market_context_schema() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "producer_version": PRODUCER_VERSION,
        "record_statuses": [VALID, INVALID_DECISION_TIMESTAMP, INVALID_CONTRACT_IDENTITY, INVALID_SOURCE_PROVENANCE],
        "field_statuses": [VALID, MISSING, INVALID, STALE, NOT_YET_DEFINED, NOT_APPLICABLE, INSUFFICIENT_HISTORY],
        "contract_identity_statuses": [
            CONTRACT_MATCH_VALID,
            ROLL_ADJACENT,
            INSUFFICIENT_SAME_CONTRACT_HISTORY,
            CONTRACT_IDENTITY_MISMATCH,
        ],
        "classifier_versions": {
            "vwap_relationship": VWAP_CLASSIFIER_VERSION,
            "opening_range_position": OPENING_RANGE_CLASSIFIER_VERSION,
            "trend_state": TREND_CLASSIFIER_VERSION,
            "volatility_state": VOLATILITY_CLASSIFIER_VERSION,
        },
        "guardrails": dict(GUARDRAILS),
    }


def build_fixture_context_record(
    *,
    decision_timestamp: str | datetime | None,
    trade_identity: Mapping[str, Any],
    entry_price: float,
    strategy_setup_family: str | None,
    session: str | None,
    candles_1m: Sequence[Mapping[str, Any]],
    candles_5m: Sequence[Mapping[str, Any]],
    generated_at: str | datetime = "2026-08-06T12:00:00+00:00",
    observed_at: str | datetime | None = None,
) -> dict[str, Any]:
    decision = parse_optional_timestamp(decision_timestamp)
    generated = parse_timestamp(generated_at)
    if decision is None:
        return invalid_record(INVALID_DECISION_TIMESTAMP, "Missing or invalid authoritative decision timestamp.", generated)
    if not valid_trade_identity(trade_identity):
        return invalid_record(INVALID_CONTRACT_IDENTITY, "Missing or invalid literal contract identity.", generated, decision=decision)

    c1 = [FixtureCandle.from_mapping(row) for row in candles_1m]
    c5 = [FixtureCandle.from_mapping(row) for row in candles_5m]
    cutoffs = {
        "1m": latest_cutoff(c1, decision, trade_identity),
        "5m": latest_cutoff(c5, decision, trade_identity),
    }
    fields = {
        "strategy_setup_family": simple_field(strategy_setup_family, "strategy_setup_family_from_crr_entry_anchor_v1"),
        "session": simple_field(session, "session_from_ctoe_context_summary_v1"),
        "vwap_relationship": classify_vwap_relationship(c1, decision, trade_identity, entry_price, session=session),
        "opening_range_position": classify_opening_range_position(c5, decision, trade_identity, entry_price, session=session),
        "trend_state": classify_trend_state(c5, decision, trade_identity),
        "volatility_state": classify_volatility_state(c5, decision, trade_identity),
    }
    record = {
        "schema_version": SCHEMA_VERSION,
        "producer_version": PRODUCER_VERSION,
        "record_status": VALID,
        "context_record_id": deterministic_id("context", trade_identity, decision.isoformat()),
        "generated_at": generated.isoformat(),
        "observed_at": (parse_timestamp(observed_at) if observed_at else decision).isoformat(),
        "instrument": trade_identity.get("instrument"),
        "local_symbol": trade_identity.get("local_symbol"),
        "contract": trade_identity.get("contract"),
        "con_id": trade_identity.get("con_id"),
        "expiry": trade_identity.get("expiry"),
        "decision_timestamp": {
            "value": decision.isoformat(),
            "source_field": "entry_anchor.entry_time",
            "meaning": "first_opening_fill_timestamp",
            "timezone": "UTC",
            "precision": "microsecond",
            "status": VALID,
        },
        "completed_candle_cutoffs": cutoffs,
        "candle_series_identity": {
            "policy": "literal_contract_specific",
            "contract_identity_status": CONTRACT_MATCH_VALID,
            "roll_status": roll_status_for_candles([*c1, *c5], trade_identity),
            "durability_timestamp_field": "source_generated_at",
        },
        "fields": fields,
        "source_provenance": [],
        "source_fingerprints": {},
        "guardrails": dict(GUARDRAILS),
    }
    record["deterministic_fingerprint"] = fingerprint(record)
    return record


def invalid_record(status: str, reason: str, generated_at: datetime, *, decision: datetime | None = None) -> dict[str, Any]:
    record = {
        "schema_version": SCHEMA_VERSION,
        "producer_version": PRODUCER_VERSION,
        "record_status": status,
        "generated_at": generated_at.isoformat(),
        "decision_timestamp": {"value": decision.isoformat() if decision else None, "status": status},
        "fields": {
            name: {"status": INVALID, "reason": reason}
            for name in ("strategy_setup_family", "session", "vwap_relationship", "opening_range_position", "trend_state", "volatility_state")
        },
        "guardrails": dict(GUARDRAILS),
    }
    record["deterministic_fingerprint"] = fingerprint(record)
    return record


def classify_vwap_relationship(
    candles: Sequence[FixtureCandle],
    decision: datetime,
    identity: Mapping[str, Any],
    entry_price: float,
    *,
    session: str | None,
    near_threshold: float = 2.0,
) -> dict[str, Any]:
    selected, problem = eligible_same_contract_candles(candles, decision, identity)
    if problem:
        return field(INVALID, classifier=VWAP_CLASSIFIER_VERSION, reason=problem)
    if not selected:
        return field(NOT_YET_DEFINED, classifier=VWAP_CLASSIFIER_VERSION, reason="No durable completed 1m candles available.")
    total_volume = sum(c.volume for c in selected)
    if total_volume <= 0:
        return field(MISSING, classifier=VWAP_CLASSIFIER_VERSION, reason="VWAP volume source missing or zero.")
    vwap = sum(((c.high + c.low + c.close) / 3.0) * c.volume for c in selected) / total_volume
    distance = round(float(entry_price) - vwap, 6)
    if distance > near_threshold:
        state = "ABOVE"
    elif distance < -near_threshold:
        state = "BELOW"
    else:
        state = "NEAR"
    return field(
        VALID,
        state=state,
        classifier=VWAP_CLASSIFIER_VERSION,
        distance_points=distance,
        vwap=round(vwap, 6),
        vwap_identifier=f"NQ_{session_code(session)}_SESSION_VWAP_V1",
    )


def classify_opening_range_position(
    candles: Sequence[FixtureCandle],
    decision: datetime,
    identity: Mapping[str, Any],
    entry_price: float,
    *,
    session: str | None,
) -> dict[str, Any]:
    session_name = str(session or "").upper()
    if session_name not in SUPPORTED_OPENING_RANGE_SESSIONS:
        return field(NOT_APPLICABLE, classifier=OPENING_RANGE_CLASSIFIER_VERSION, reason="Opening range unsupported for this v1 session.")
    selected, problem = eligible_same_contract_candles(candles, decision, identity)
    if problem:
        return field(INVALID, classifier=OPENING_RANGE_CLASSIFIER_VERSION, reason=problem)
    if len(selected) < 6:
        return field(NOT_YET_DEFINED, classifier=OPENING_RANGE_CLASSIFIER_VERSION, reason="Six completed 5m opening-range candles are required.")
    opening = selected[:6]
    range_high = max(c.high for c in opening)
    range_low = min(c.low for c in opening)
    latest_close = selected[-1].close
    if entry_price > range_high and latest_close > range_high:
        state = "EXTENDING_ABOVE"
    elif entry_price < range_low and latest_close < range_low:
        state = "EXTENDING_BELOW"
    elif entry_price > range_high:
        state = "ABOVE"
    elif entry_price < range_low:
        state = "BELOW"
    else:
        state = "INSIDE"
    return field(
        VALID,
        state=state,
        classifier=OPENING_RANGE_CLASSIFIER_VERSION,
        range_high=round(range_high, 6),
        range_low=round(range_low, 6),
        range_identifier=f"NQ_{session_code(session)}_OPENING_RANGE_30M_V1",
    )


def classify_trend_state(candles: Sequence[FixtureCandle], decision: datetime, identity: Mapping[str, Any]) -> dict[str, Any]:
    selected, problem = eligible_same_contract_candles(candles, decision, identity)
    if problem:
        return field(INVALID, classifier=TREND_CLASSIFIER_VERSION, reason=problem)
    if len(selected) < 12:
        return field(INSUFFICIENT_HISTORY, classifier=TREND_CLASSIFIER_VERSION, reason="Twelve completed 5m candles are required.")
    closes = [c.close for c in selected[-12:]]
    slope = ols_slope(closes)
    if slope >= 3.0:
        state = "UP"
    elif slope <= -3.0:
        state = "DOWN"
    else:
        state = "SIDEWAYS"
    return field(VALID, state=state, classifier=TREND_CLASSIFIER_VERSION, slope_points_per_5m_bar=round(slope, 6))


def classify_volatility_state(candles: Sequence[FixtureCandle], decision: datetime, identity: Mapping[str, Any]) -> dict[str, Any]:
    selected, problem = eligible_same_contract_candles(candles, decision, identity)
    if problem:
        return field(INVALID, classifier=VOLATILITY_CLASSIFIER_VERSION, reason=problem)
    if len(selected) < 85:
        return field(INSUFFICIENT_HISTORY, classifier=VOLATILITY_CLASSIFIER_VERSION, reason="Eighty-five completed same-contract 5m candles are required.")
    window = selected[-85:]
    ranges = [true_range(window[index], window[index - 1]) for index in range(1, 85)]
    baseline = ranges[:72]
    current = ranges[72:]
    baseline_median = median(baseline)
    current_mean = sum(current) / len(current)
    if baseline_median <= 0:
        return field(INVALID, classifier=VOLATILITY_CLASSIFIER_VERSION, reason="Baseline true range must be positive.")
    ratio = current_mean / baseline_median
    if ratio <= 0.75:
        state = "LOW"
    elif ratio >= 1.25:
        state = "HIGH"
    else:
        state = "NORMAL"
    return field(VALID, state=state, classifier=VOLATILITY_CLASSIFIER_VERSION, atr_ratio=round(ratio, 6))


def true_range(current: FixtureCandle, previous: FixtureCandle) -> float:
    if not same_contract(current, previous_identity(previous)):
        raise ValueError("previous_close must be from the immediately preceding same-contract candle")
    return max(current.high - current.low, abs(current.high - previous.close), abs(current.low - previous.close))


def eligible_same_contract_candles(
    candles: Sequence[FixtureCandle],
    decision: datetime,
    identity: Mapping[str, Any],
) -> tuple[list[FixtureCandle], str | None]:
    if any(c.close_time <= decision and c.source_generated_at and c.source_generated_at <= decision and not same_contract(c, identity) for c in candles):
        return [], "Literal contract identity mismatch."
    selected = sorted((c for c in candles if candle_is_usable(c, decision) and same_contract(c, identity)), key=lambda c: c.close_time)
    return selected, None


def candle_is_usable(candle: FixtureCandle, decision: datetime) -> bool:
    if candle.source_generated_at is None:
        return False
    return candle.close_time <= decision and candle.source_generated_at <= decision


def latest_cutoff(candles: Sequence[FixtureCandle], decision: datetime, identity: Mapping[str, Any]) -> str | None:
    selected, _ = eligible_same_contract_candles(candles, decision, identity)
    return selected[-1].close_time.isoformat() if selected else None


def ols_slope(values: Sequence[float]) -> float:
    n = len(values)
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    numerator = sum((index - x_mean) * (value - y_mean) for index, value in enumerate(values))
    denominator = sum((index - x_mean) ** 2 for index in range(n))
    return numerator / denominator


def same_contract(candle: FixtureCandle, identity: Mapping[str, Any]) -> bool:
    return (
        candle.instrument == str(identity.get("instrument") or "")
        and candle.local_symbol == str(identity.get("local_symbol") or "")
        and candle.contract == str(identity.get("contract") or "")
        and str(candle.con_id) == str(identity.get("con_id"))
    )


def previous_identity(candle: FixtureCandle) -> dict[str, Any]:
    return {"instrument": candle.instrument, "local_symbol": candle.local_symbol, "contract": candle.contract, "con_id": candle.con_id}


def valid_trade_identity(identity: Mapping[str, Any]) -> bool:
    return all(identity.get(key) not in (None, "") for key in ("instrument", "local_symbol", "contract", "con_id"))


def simple_field(value: str | None, classifier_version: str) -> dict[str, Any]:
    if value in (None, ""):
        return field(MISSING, classifier=classifier_version, reason="Source value missing.")
    return field(VALID, state=value, classifier=classifier_version)


def field(status: str, *, classifier: str, state: str | None = None, reason: str | None = None, **extra: Any) -> dict[str, Any]:
    payload = {"status": status, "classifier_version": classifier}
    if state is not None:
        payload["state"] = state
    if reason:
        payload["reason"] = reason
    payload.update(extra)
    return payload


def roll_status_for_candles(candles: Sequence[FixtureCandle], identity: Mapping[str, Any]) -> str:
    return CONTRACT_IDENTITY_MISMATCH if any(c.source_generated_at and not same_contract(c, identity) for c in candles) else CONTRACT_MATCH_VALID


def session_code(session: str | None) -> str:
    session_name = str(session or "UNKNOWN").upper()
    return "US_RTH" if session_name == "US" else session_name


def parse_optional_timestamp(value: str | datetime | None) -> datetime | None:
    try:
        return parse_timestamp(value)
    except (TypeError, ValueError):
        return None


def parse_timestamp(value: str | datetime | None) -> datetime:
    if value is None:
        raise ValueError("timestamp is required")
    if isinstance(value, datetime):
        result = value
    else:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("timestamp must include timezone")
    return result.astimezone(UTC)


def deterministic_id(prefix: str, *parts: Any) -> str:
    return f"{prefix}_{hashlib.sha256(json.dumps(parts, sort_keys=True, default=str).encode('utf-8')).hexdigest()[:24]}"


def fingerprint(payload: Mapping[str, Any]) -> str:
    material = {key: value for key, value in payload.items() if key != "deterministic_fingerprint"}
    normalized = json.dumps(material, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(normalized).hexdigest()


def fixture_demo_record() -> dict[str, Any]:
    identity = {"instrument": "NQ", "local_symbol": "NQU6", "contract": "NQU6", "con_id": 770561204, "expiry": "202609"}
    candles_5m = [
        fixture_candle(index, close=100.0 + index * 3.0, high=101.0 + index * 3.0, low=99.0 + index * 3.0, timeframe_minutes=5)
        for index in range(85)
    ]
    candles_1m = [
        fixture_candle(index, close=100.0 + index, high=101.0 + index, low=99.0 + index, volume=10.0, timeframe_minutes=1)
        for index in range(10)
    ]
    return build_fixture_context_record(
        decision_timestamp="2026-08-07T14:35:00+00:00",
        trade_identity=identity,
        entry_price=110.0,
        strategy_setup_family="PAPER_ACTIVE_EVIDENCE_NQ_US_PARTICIPATION_LONG_V1",
        session="US",
        candles_1m=candles_1m,
        candles_5m=candles_5m,
    )


def fixture_candle(
    index: int,
    *,
    close: float,
    high: float,
    low: float,
    open_: float | None = None,
    volume: float = 1.0,
    timeframe_minutes: int,
) -> dict[str, Any]:
    minute = index * timeframe_minutes
    close_time = datetime(2026, 8, 7, 7, 30, tzinfo=UTC).replace(minute=0) if minute >= 60 else datetime(2026, 8, 7, 14, minute, tzinfo=UTC)
    if minute >= 60:
        close_time = datetime(2026, 8, 7, 14 + minute // 60, minute % 60, tzinfo=UTC)
    return {
        "close_time": close_time.isoformat(),
        "source_generated_at": close_time.isoformat(),
        "instrument": "NQ",
        "local_symbol": "NQU6",
        "contract": "NQU6",
        "con_id": 770561204,
        "open": open_ if open_ is not None else close,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fixture-only prospective market context utility.")
    parser.add_argument("--mode", choices=("schema", "demo"), default="schema")
    args = parser.parse_args(argv)
    payload = prospective_market_context_schema() if args.mode == "schema" else fixture_demo_record()
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
