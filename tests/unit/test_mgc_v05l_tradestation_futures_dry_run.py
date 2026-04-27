from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mgc_v05l.app.tradestation_futures_dry_run_ticket import main
from mgc_v05l.execution.tradestation_dry_run import generate_stage2_futures_dry_run


def _truth_payload(*, generated_at: datetime, positions=None, open_orders=None, account_type: str = "futures") -> dict:
    return {
        "environment": "sim",
        "generated_at": generated_at.isoformat(),
        "selected_accounts": {
            "selected_futures_account_id": "FU-1",
            "selected_margin_account_id": "EQ-1",
        },
        "per_account": {
            "FU-1": {
                "account_id": "FU-1",
                "account_type": account_type,
                "freshness_timestamp": generated_at.isoformat(),
            }
        },
        "positions": list(positions or []),
        "open_orders": list(open_orders or []),
    }


def _signal_payload(symbol: str = "NQ", **overrides) -> dict:
    payload = {
        "signal_id": "sig-001",
        "strategy_id": "asia_drift_continuation",
        "internal_symbol": symbol,
        "decision_state": "TRADE_FAVORABLE",
        "timing_bucket": "LATE_ASIA",
        "side": "BUY",
        "quantity": "1",
        "entry_order_type": "LIMIT",
        "entry_limit_price": "18250.25",
        "fixed_stop_points": "20",
        "fixed_target_points": "40",
        "time_stop_minutes": 120,
        "quote_snapshot": {
            "provider": "fixture",
            "bid_price": 18250.0,
            "ask_price": 18250.25,
            "last_price": 18250.1,
        },
    }
    payload.update(overrides)
    return payload


def _symbol_map() -> dict:
    return {
        "ES": {"broker_symbol": "@ESM26", "expiry": "202606", "multiplier": "50", "exchange": "CME"},
        "MES": {"broker_symbol": "@MESM26", "expiry": "202606", "multiplier": "5", "exchange": "CME"},
        "NQ": {"broker_symbol": "@NQM26", "expiry": "202606", "multiplier": "20", "exchange": "CME"},
        "MNQ": {"broker_symbol": "@MNQM26", "expiry": "202606", "multiplier": "2", "exchange": "CME"},
    }


def test_valid_nq_dry_run_generates_ticket_and_payload() -> None:
    now = datetime(2026, 4, 27, 12, 0, tzinfo=timezone.utc)
    result = generate_stage2_futures_dry_run(
        signal_payload=_signal_payload("NQ"),
        truth_payload=_truth_payload(generated_at=now),
        broker_symbol_map_payload=_symbol_map(),
        pending_ticket_registry={},
        generated_at=now,
    )

    assert result.validation_status.value == "VALID"
    assert result.ticket is not None
    assert result.dry_run_payload is not None
    assert result.trace["signal_id"] == "sig-001"
    assert result.trace["ticket_id"].startswith("ticket_")
    assert result.trace["dry_run_payload_id"].startswith("dryrun_")


def test_es_missing_quote_is_valid_with_warnings() -> None:
    now = datetime(2026, 4, 27, 12, 0, tzinfo=timezone.utc)
    signal = _signal_payload("ES", quote_snapshot=None, fixed_stop_points="6", fixed_target_points="12", entry_limit_price="5300.25")
    result = generate_stage2_futures_dry_run(
        signal_payload=signal,
        truth_payload=_truth_payload(generated_at=now),
        broker_symbol_map_payload=_symbol_map(),
        pending_ticket_registry={},
        generated_at=now,
    )

    assert result.validation_status.value == "VALID_WITH_WARNINGS"
    assert "optional but missing" in result.warnings[0]


def test_rejects_missing_contract_metadata() -> None:
    now = datetime(2026, 4, 27, 12, 0, tzinfo=timezone.utc)
    bad_map = {"NQ": {"broker_symbol": "@NQM26", "expiry": "", "multiplier": "", "exchange": ""}}
    result = generate_stage2_futures_dry_run(
        signal_payload=_signal_payload("NQ"),
        truth_payload=_truth_payload(generated_at=now),
        broker_symbol_map_payload=bad_map,
        pending_ticket_registry={},
        generated_at=now,
    )

    assert result.validation_status.value == "REJECTED"
    assert "Contract metadata is incomplete" in result.rejections[0]


def test_rejects_stale_truth() -> None:
    now = datetime(2026, 4, 27, 12, 5, tzinfo=timezone.utc)
    stale = now - timedelta(seconds=121)
    result = generate_stage2_futures_dry_run(
        signal_payload=_signal_payload("NQ"),
        truth_payload=_truth_payload(generated_at=stale),
        broker_symbol_map_payload=_symbol_map(),
        pending_ticket_registry={},
        generated_at=now,
    )

    assert result.validation_status.value == "REJECTED"
    assert "Truth snapshot is stale" in result.rejections[0]


def test_rejects_future_dated_truth() -> None:
    now = datetime(2026, 4, 27, 12, 0, tzinfo=timezone.utc)
    future = now + timedelta(seconds=1)
    result = generate_stage2_futures_dry_run(
        signal_payload=_signal_payload("NQ"),
        truth_payload=_truth_payload(generated_at=future),
        broker_symbol_map_payload=_symbol_map(),
        pending_ticket_registry={},
        generated_at=now,
    )

    assert result.validation_status.value == "REJECTED"
    assert any("in the future" in row for row in result.rejections)


def test_rejects_non_sim_environment() -> None:
    now = datetime(2026, 4, 27, 12, 0, tzinfo=timezone.utc)
    signal = _signal_payload("NQ", environment="live")
    truth = _truth_payload(generated_at=now)
    truth["environment"] = "live"
    result = generate_stage2_futures_dry_run(
        signal_payload=signal,
        truth_payload=truth,
        broker_symbol_map_payload=_symbol_map(),
        pending_ticket_registry={},
        generated_at=now,
    )

    assert result.validation_status.value == "REJECTED"
    assert any("SIM environment" in row for row in result.rejections)


def test_rejects_overlap_from_position_order_or_pending_ticket() -> None:
    now = datetime(2026, 4, 27, 12, 0, tzinfo=timezone.utc)
    truth = _truth_payload(
        generated_at=now,
        positions=[{"account_id": "FU-1", "symbol": "NQ", "quantity": "1"}],
        open_orders=[],
    )
    result = generate_stage2_futures_dry_run(
        signal_payload=_signal_payload("NQ"),
        truth_payload=truth,
        broker_symbol_map_payload=_symbol_map(),
        pending_ticket_registry={"pending_tickets": [{"symbol": "NQ", "status": "OPEN"}]},
        generated_at=now,
    )

    assert result.validation_status.value == "REJECTED"
    assert any("Active overlap detected" in row for row in result.rejections)


def test_rejects_wrong_account_type_and_wrong_gate() -> None:
    now = datetime(2026, 4, 27, 12, 0, tzinfo=timezone.utc)
    signal = _signal_payload("NQ", decision_state="TRADE_NEUTRAL")
    result = generate_stage2_futures_dry_run(
        signal_payload=signal,
        truth_payload=_truth_payload(generated_at=now, account_type="margin_equities_options"),
        broker_symbol_map_payload=_symbol_map(),
        pending_ticket_registry={},
        generated_at=now,
    )

    assert result.validation_status.value == "REJECTED"
    assert any("Decision-state gating failed" in row for row in result.rejections)
    assert any("not futures" in row for row in result.rejections)


def test_cli_writes_artifacts(tmp_path: Path) -> None:
    now = datetime.now(timezone.utc)
    signal = _signal_payload("NQ")
    truth = _truth_payload(generated_at=now)
    symbol_map = _symbol_map()
    signal_path = tmp_path / "signal.json"
    truth_path = tmp_path / "truth.json"
    map_path = tmp_path / "symbols.json"
    signal_path.write_text(json.dumps(signal), encoding="utf-8")
    truth_path.write_text(json.dumps(truth), encoding="utf-8")
    map_path.write_text(json.dumps(symbol_map), encoding="utf-8")
    output_dir = tmp_path / "out"

    result = main(
        [
            "--environment",
            "sim",
            "--signal-json",
            str(signal_path),
            "--truth-json",
            str(truth_path),
            "--broker-symbol-map",
            str(map_path),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert result == 0
    summary = json.loads((output_dir / "reports" / "tradestation_futures_dry_run_summary.json").read_text(encoding="utf-8"))
    assert summary["validation_status"] == "VALID"
