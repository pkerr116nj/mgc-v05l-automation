from __future__ import annotations

import json
from datetime import UTC, datetime

from mgc_v05l.execution_core.observatory_global_venue_session import (
    SCHEMA_VERSION,
    build_snapshot,
    write_snapshot,
)


def _venue(snapshot: dict, venue_id: str) -> dict:
    return next(item for item in snapshot["venues"] if item["venue_id"] == venue_id)


def test_normal_open_session() -> None:
    snapshot = build_snapshot(datetime(2026, 8, 7, 15, 0, tzinfo=UTC))
    nyse = _venue(snapshot, "nyse")
    assert nyse["session_status"] == "OPEN"
    assert nyse["timezone"] == "America/New_York"
    assert nyse["guardrails"]["display_only"] is True
    assert nyse["guardrails"]["trading_input"] is False


def test_preopen_and_close_boundary() -> None:
    preopen = build_snapshot(datetime(2026, 8, 7, 12, 30, tzinfo=UTC))
    closed = build_snapshot(datetime(2026, 8, 7, 21, 10, tzinfo=UTC))
    assert _venue(preopen, "nyse")["session_status"] == "PREOPEN"
    assert _venue(closed, "nyse")["session_status"] == "CLOSED"


def test_dst_boundary_uses_exchange_timezone() -> None:
    winter = build_snapshot(datetime(2026, 1, 5, 14, 0, tzinfo=UTC))
    summer = build_snapshot(datetime(2026, 7, 6, 13, 45, tzinfo=UTC))
    assert _venue(winter, "nyse")["session_status"] == "PREOPEN"
    assert _venue(summer, "nyse")["session_status"] == "OPEN"


def test_holiday_classified_without_guessing_open() -> None:
    snapshot = build_snapshot(datetime(2026, 12, 25, 15, 0, tzinfo=UTC))
    nyse = _venue(snapshot, "nyse")
    assert nyse["session_status"] == "HOLIDAY"
    assert nyse["holiday"] is True


def test_early_close_special_session() -> None:
    snapshot = build_snapshot(datetime(2026, 11, 27, 17, 45, tzinfo=UTC))
    nyse = _venue(snapshot, "nyse")
    assert nyse["session_status"] in {"OPEN", "CLOSING_SOON"}
    assert nyse["special_session"] == "EARLY_CLOSE"


def test_lunch_break_market_when_supported() -> None:
    snapshot = build_snapshot(datetime(2026, 8, 7, 4, 30, tzinfo=UTC))
    hkex = _venue(snapshot, "hkex")
    assert hkex["session_status"] == "LUNCH_BREAK"
    assert hkex["calendar_source"] == "XHKG"


def test_cme_product_sessions_are_separate_from_venue_operational_state() -> None:
    snapshot = build_snapshot(datetime(2026, 8, 7, 15, 0, tzinfo=UTC))
    cme = _venue(snapshot, "cme")
    products = {row["product_group"]: row for row in cme["product_session_states"]}
    assert cme["venue_operational_state"] == "UNKNOWN"
    assert cme["session_status"] == "OPEN"
    assert products["Equity Index"]["product_session_state"] == "OPEN"
    assert products["Rates"]["product_session_state"] == "OPEN"
    assert products["Metals"]["product_session_state"] == "OPEN"
    assert products["Crypto"]["product_session_state"] == "UNKNOWN"
    assert products["Crypto"]["calendar_source"] is None


def test_cme_weekend_traditional_products_closed_without_guessing_crypto() -> None:
    snapshot = build_snapshot(datetime(2026, 8, 8, 15, 0, tzinfo=UTC))
    cme = _venue(snapshot, "cme")
    products = {row["product_group"]: row for row in cme["product_session_states"]}
    assert cme["session_status"] == "CLOSED"
    assert products["Equity Index"]["product_session_state"] == "CLOSED"
    assert products["Rates"]["product_session_state"] == "CLOSED"
    assert products["Metals"]["product_session_state"] == "CLOSED"
    assert products["Crypto"]["product_session_state"] == "UNKNOWN"


def test_cme_maintenance_window_uses_product_calendar_truth() -> None:
    snapshot = build_snapshot(datetime(2026, 8, 7, 22, 30, tzinfo=UTC))
    cme = _venue(snapshot, "cme")
    monitored = [row for row in cme["product_session_states"] if row["monitored"]]
    assert cme["session_status"] == "CLOSED"
    assert {row["product_session_state"] for row in monitored} == {"CLOSED"}


def test_unsupported_venue_fails_closed_to_unknown() -> None:
    snapshot = build_snapshot(datetime(2026, 8, 7, 15, 0, tzinfo=UTC))
    montreal = _venue(snapshot, "mx")
    assert montreal["coverage"] == "UNSUPPORTED"
    assert montreal["session_status"] == "UNKNOWN"
    assert montreal["calendar_source"] is None


def test_snapshot_mjs_output_round_trip(tmp_path) -> None:
    snapshot = build_snapshot(datetime(2026, 8, 7, 15, 0, tzinfo=UTC))
    output = tmp_path / "venue_session_snapshot.generated.mjs"
    write_snapshot(snapshot, output)
    text = output.read_text(encoding="utf-8")
    assert text.startswith("export const venueSessionSnapshot = ")
    payload = text.removeprefix("export const venueSessionSnapshot = ").removesuffix(";\n")
    parsed = json.loads(payload)
    assert parsed["schema_version"] == SCHEMA_VERSION
    assert parsed["guardrails"]["broker_authority"] is False
