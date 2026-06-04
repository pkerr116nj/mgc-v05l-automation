from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_paper_proof_readiness import (
    DEFAULT_OUTPUT_PATH,
    MARKET_CLOSED_NO_FRESH_BARS,
    PHASE1_DATA_UNHEALTHY,
    READY_FOR_PROOF,
    RUNTIME_ALREADY_ACTIVE,
    SHARED_TRUTH_BLOCKED,
    TrackBPaperProofReadinessConfig,
    build_track_b_paper_proof_readiness,
    main,
    write_track_b_paper_proof_readiness,
)
from mgc_v05l.execution_core.track_b_shared_truth_refresh_cli import DEFAULT_RECONCILIATION_ARTIFACT


OPEN_NOW = datetime(2026, 5, 22, 14, 0, tzinfo=UTC)
SATURDAY_NOW = datetime(2026, 5, 23, 7, 15, tzinfo=UTC)
EQUITY_INDEX_HALT_NOW = datetime(2026, 5, 25, 17, 30, tzinfo=UTC)


def test_clean_shared_truth_and_fresh_bars_are_ready_for_proof(tmp_path: Path) -> None:
    _seed_clean_shared_truth(tmp_path, now=OPEN_NOW)
    _seed_required_phase1_candles(tmp_path, generated_at=OPEN_NOW)

    payload = _build(tmp_path, now=OPEN_NOW)

    assert payload["classification"] == READY_FOR_PROOF
    assert payload["ready_for_proof"] is True
    assert payload["blockers"] == []
    assert payload["shared_truth_preflight"]["classification"] == "SHARED_TRUTH_PREFLIGHT_CLEAN"
    assert all(check["ready"] for check in payload["phase1_required_checks"])


def test_clean_shared_truth_and_closed_market_stale_bars_classify_market_closed(tmp_path: Path) -> None:
    _seed_clean_shared_truth(tmp_path, now=SATURDAY_NOW)
    _seed_required_phase1_candles(tmp_path, generated_at=datetime(2026, 5, 22, 21, 0, tzinfo=UTC))

    payload = _build(tmp_path, now=SATURDAY_NOW)

    assert payload["classification"] == MARKET_CLOSED_NO_FRESH_BARS
    assert payload["ready_for_proof"] is False
    assert payload["primary_blocker"]["reason"] == MARKET_CLOSED_NO_FRESH_BARS
    assert {blocker["reason"] for blocker in payload["blockers"]} == {MARKET_CLOSED_NO_FRESH_BARS}
    assert payload["phase1_market_session"]["market_closed"] is True
    assert payload["phase1_session_reason"] == "WEEKEND_GLOBEX_HALT_SATURDAY"


def test_closed_market_with_degraded_broker_lease_keeps_market_closed_primary(tmp_path: Path) -> None:
    _seed_clean_shared_truth(tmp_path, now=SATURDAY_NOW, broker_refresh_failing=True)
    _seed_required_phase1_candles(tmp_path, generated_at=datetime(2026, 5, 22, 21, 0, tzinfo=UTC))

    payload = _build(tmp_path, now=SATURDAY_NOW)

    assert payload["classification"] == MARKET_CLOSED_NO_FRESH_BARS
    assert payload["primary_blocker"]["reason"] == MARKET_CLOSED_NO_FRESH_BARS
    assert payload["broker_lease_warning"]["lease_state"] == "ACTIVE_DEGRADED_REFRESH_FAILING"
    assert not any(
        warning.get("code") == "broker_truth_lease_not_clean_for_runtime_start"
        and warning.get("warning_type") == "shared_truth_evidence"
        for warning in payload["secondary_warnings"]
    )


def test_open_market_with_degraded_broker_lease_is_diagnostic_when_truth_is_clean(tmp_path: Path) -> None:
    _seed_clean_shared_truth(tmp_path, now=OPEN_NOW, broker_refresh_failing=True)
    _seed_required_phase1_candles(tmp_path, generated_at=OPEN_NOW)

    payload = _build(tmp_path, now=OPEN_NOW)

    assert payload["classification"] == READY_FOR_PROOF
    assert payload["primary_blocker"] is None
    assert payload["broker_lease_warning"]["lease_state"] == "ACTIVE_DEGRADED_REFRESH_FAILING"


def test_position_exposure_blocks_on_shared_truth_before_phase1(tmp_path: Path) -> None:
    _seed_clean_shared_truth(tmp_path, now=OPEN_NOW)
    _write_reconciliation(
        tmp_path,
        now=OPEN_NOW,
        classification="BROKER_TRUTH_SETTLEMENT_TIMEOUT",
        broker_reconciled=False,
        broker_positions=[{"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "1.0", "account": "DUM882026"}],
    )
    _write_broker_status(
        tmp_path,
        now=OPEN_NOW,
        positions=[{"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "1.0", "account": "DUM882026"}],
    )
    _seed_required_phase1_candles(tmp_path, generated_at=OPEN_NOW)

    payload = _build(tmp_path, now=OPEN_NOW)

    assert payload["classification"] == SHARED_TRUTH_BLOCKED
    assert payload["ready_for_proof"] is False
    assert any(blocker.get("code") == "position_truth_not_clean_for_runtime_start" for blocker in payload["blockers"])


def test_stale_open_session_bars_are_phase1_unhealthy(tmp_path: Path) -> None:
    _seed_clean_shared_truth(tmp_path, now=OPEN_NOW)
    _seed_required_phase1_candles(tmp_path, generated_at=OPEN_NOW - timedelta(minutes=30))

    payload = _build(tmp_path, now=OPEN_NOW)

    assert payload["classification"] == PHASE1_DATA_UNHEALTHY
    assert payload["ready_for_proof"] is False
    assert {blocker["reason"] for blocker in payload["blockers"]} == {"RUNTIME_CANDLES_STALE"}


def test_planned_equity_index_halt_mnq_stale_does_not_block_metals_readiness(tmp_path: Path) -> None:
    _seed_clean_shared_truth(tmp_path, now=EQUITY_INDEX_HALT_NOW)
    for timeframe in ("1m", "5m"):
        _write_phase1_candle(tmp_path, symbol="MGC", timeframe=timeframe, generated_at=EQUITY_INDEX_HALT_NOW)
        _write_phase1_candle(
            tmp_path,
            symbol="MNQ",
            timeframe=timeframe,
            generated_at=EQUITY_INDEX_HALT_NOW - timedelta(minutes=45),
        )

    payload = _build(tmp_path, now=EQUITY_INDEX_HALT_NOW)

    assert payload["classification"] == READY_FOR_PROOF
    assert payload["ready_for_proof"] is True
    assert payload["blockers"] == []
    stale_mnq = [
        warning for warning in payload["secondary_warnings"]
        if warning.get("symbol") == "MNQ"
    ]
    assert {warning["reason"] for warning in stale_mnq} == {"PLANNED_EQUITY_INDEX_FUTURES_HALT_NO_FRESH_BARS"}
    assert all(warning["blocking_for_proof"] is False for warning in stale_mnq)
    assert all(warning["scope_impact"] == "EQUITY_INDEX_ONLY" for warning in stale_mnq)


def test_scoped_required_symbols_ignore_unrelated_stale_market_data(tmp_path: Path) -> None:
    _seed_clean_shared_truth(tmp_path, now=OPEN_NOW)
    for symbol in ("MNQ", "MES"):
        for timeframe in ("1m", "5m"):
            _write_phase1_candle(tmp_path, symbol=symbol, timeframe=timeframe, generated_at=OPEN_NOW)
    for symbol in ("MGC", "GC"):
        for timeframe in ("1m", "5m"):
            _write_phase1_candle(tmp_path, symbol=symbol, timeframe=timeframe, generated_at=OPEN_NOW - timedelta(minutes=45))

    payload = build_track_b_paper_proof_readiness(
        config=TrackBPaperProofReadinessConfig(
            repo_root=tmp_path,
            broker_lease_history_path=None,
            now=OPEN_NOW,
            required_symbols=("MNQ", "MES"),
        ),
        pid_running=lambda _pid: False,
        process_root_resolver=lambda _pid: None,
        source_commit_resolver=lambda _root: "test-head",
    )

    assert payload["classification"] == READY_FOR_PROOF
    assert payload["phase1_required_symbols"] == ["MNQ", "MES"]
    assert {check["symbol"] for check in payload["phase1_required_checks"]} == {"MNQ", "MES"}
    assert payload["blockers"] == []


def test_active_runtime_blocks_as_already_active(tmp_path: Path) -> None:
    _seed_clean_shared_truth(tmp_path, now=OPEN_NOW)
    _seed_required_phase1_candles(tmp_path, generated_at=OPEN_NOW)
    _write_runtime_active_evidence(tmp_path, now=OPEN_NOW)

    payload = build_track_b_paper_proof_readiness(
        config=TrackBPaperProofReadinessConfig(
            repo_root=tmp_path,
            broker_lease_history_path=None,
            now=OPEN_NOW,
        ),
        pid_running=lambda _pid: True,
        process_root_resolver=lambda _pid: tmp_path,
        source_commit_resolver=lambda _root: "test-head",
    )

    assert payload["classification"] == RUNTIME_ALREADY_ACTIVE
    assert payload["ready_for_proof"] is False
    assert payload["shared_truth_classifications"]["Runtime Environment Truth"] == "RUNTIME_ACTIVE_TRADE_CAPABLE"


def test_writes_authority_artifact(tmp_path: Path) -> None:
    _seed_clean_shared_truth(tmp_path, now=OPEN_NOW)
    _seed_required_phase1_candles(tmp_path, generated_at=OPEN_NOW)
    payload = _build(tmp_path, now=OPEN_NOW)

    path = write_track_b_paper_proof_readiness(
        config=TrackBPaperProofReadinessConfig(repo_root=tmp_path, broker_lease_history_path=None),
        payload=payload,
    )

    assert path == tmp_path / DEFAULT_OUTPUT_PATH
    written = json.loads(path.read_text(encoding="utf-8"))
    assert written["classification"] == READY_FOR_PROOF
    assert "outputs/operator_dashboard/runtime/latest_track_b" not in json.dumps(written["artifact_paths"])


def test_main_writes_artifact_and_returns_nonzero_when_not_ready(tmp_path: Path, capsys) -> None:
    _seed_clean_shared_truth(tmp_path, now=SATURDAY_NOW)
    _seed_required_phase1_candles(tmp_path, generated_at=datetime(2026, 5, 22, 21, 0, tzinfo=UTC))

    exit_code = main(
        [
            "--repo-root",
            str(tmp_path),
            "--no-broker-lease-history",
            "--now",
            SATURDAY_NOW.isoformat(),
        ]
    )

    output = capsys.readouterr().out
    assert exit_code == 2
    assert "MARKET_CLOSED_NO_FRESH_BARS" in output
    written = json.loads((tmp_path / DEFAULT_OUTPUT_PATH).read_text(encoding="utf-8"))
    assert written["classification"] == MARKET_CLOSED_NO_FRESH_BARS


def _build(root: Path, *, now: datetime) -> dict:
    return build_track_b_paper_proof_readiness(
        config=TrackBPaperProofReadinessConfig(repo_root=root, broker_lease_history_path=None, now=now),
        pid_running=lambda _pid: False,
        process_root_resolver=lambda _pid: None,
        source_commit_resolver=lambda _root: "test-head",
    )


def _seed_clean_shared_truth(root: Path, *, now: datetime, broker_refresh_failing: bool = False) -> None:
    _write_reconciliation(root, now=now)
    _write_broker_status(root, now=now, broker_refresh_failing=broker_refresh_failing)
    _write_live_position_status(root, now=now)
    _write_trade_summary(root, now=now)


def _seed_required_phase1_candles(root: Path, *, generated_at: datetime) -> None:
    for symbol in ("MGC", "MNQ"):
        for timeframe in ("1m", "5m"):
            _write_phase1_candle(root, symbol=symbol, timeframe=timeframe, generated_at=generated_at)


def _write_phase1_candle(root: Path, *, symbol: str, timeframe: str, generated_at: datetime) -> None:
    path = (
        root
        / "outputs"
        / "track_b_execution_core"
        / "phase1_runtime_market_data"
        / symbol
        / timeframe
        / "latest_runtime_candles.json"
    )
    _write(
        path,
        {
            "generated_at": generated_at.isoformat(),
            "source_id": "databento_live:test",
            "symbol": symbol,
            "timeframe": timeframe,
            "completed_candles_only": True,
            "historical_seed_ready": True,
            "realtime_feed_confirmed": True,
            "bars": [{"bar_end": generated_at.isoformat(), "close": 100.0}],
        },
    )


def _write_runtime_active_evidence(root: Path, *, now: datetime) -> None:
    runtime_dir = root / "outputs/probationary_pattern_engine/paper_session/runtime"
    _write(
        runtime_dir / "paper_runtime_truth.json",
        {
            "generated_at": now.isoformat(),
            "last_success_at": now.isoformat(),
            "freshness_ttl_seconds": 180,
            "heartbeat_state": "HEALTHY",
            "freshness_state": "FRESH",
            "writer_authority": "SINGLE_WRITER",
            "producer_pid": 12345,
            "producer_root": str(root),
            "source_commit": "test-head",
            "config_fingerprint": "cfg",
            "runtime_instance_id": "runtime-test",
            "restart_generation": 1,
        },
    )
    _write(
        runtime_dir / "probationary_paper.pid.json",
        {
            "pid": 12345,
            "root": str(root),
            "source_commit": "test-head",
            "config_fingerprint": "cfg",
            "runtime_instance_id": "runtime-test",
            "restart_generation": 1,
        },
    )
    _write(
        root / "outputs/operator_dashboard/runtime/latest_canonical_readiness.json",
        {
            "generated_at": now.isoformat(),
            "canonical_readiness": "READY_SUBMIT_CAPABLE",
            "paper_trade_allowed": True,
        },
    )


def _write_reconciliation(
    root: Path,
    *,
    now: datetime,
    classification: str = "TRACK_B_PAPER_BROKER_RECONCILED",
    broker_reconciled: bool = True,
    broker_positions: list[dict] | None = None,
    open_orders: list[dict] | None = None,
) -> None:
    positions = broker_positions or []
    orders = open_orders or []
    _write(
        root / DEFAULT_RECONCILIATION_ARTIFACT,
        {
            "generated_at": now.isoformat(),
            "classification": classification,
            "broker_reconciled": broker_reconciled,
            "live_money_eligible": False,
            "track_b_broker_positions": positions,
            "track_b_broker_open_orders": orders,
            "track_b_lifecycle_positions": [],
            "unknown_broker_open_orders": orders if orders else [],
            "known_managed_exit_orders": [],
            "unresolved_submit_intent_ownership_records": [],
            "track_b_broker_position_count": len(positions),
            "track_b_broker_open_order_count": len(orders),
            "unknown_broker_open_order_count": len(orders),
            "review_required_count": 0,
            "unresolved_submit_intent_ownership_count": 0,
            "lifecycle_open_position_count": 0,
            "lifecycle_open_order_count": len(orders),
            "position_match_report": {"state": "BROKER_AND_LIFECYCLE_FLAT", "matched": broker_reconciled},
            "blockers": [] if broker_reconciled else [{"code": "broker_position_without_lifecycle"}],
        },
    )


def _write_broker_status(
    root: Path,
    *,
    now: datetime,
    positions: list[dict] | None = None,
    open_orders: list[dict] | None = None,
    broker_refresh_failing: bool = False,
) -> None:
    payload = {
        "classification": "BROKER_TRUTH_REFRESH_READY",
        "account": "DUM882026",
        "generated_at": now.isoformat(),
        "positions_complete": True,
        "open_orders_complete": True,
        "positions": positions or [],
        "open_orders": open_orders or [],
        "live_money_eligible": False,
    }
    latest_attempt = {
        **payload,
        "classification": "BROKER_TRUTH_REFRESH_FAILED" if broker_refresh_failing else "BROKER_TRUTH_REFRESH_READY",
        "last_failure": broker_refresh_failing,
        "last_error": "simulated refresh failure" if broker_refresh_failing else None,
    }
    _write(
        root / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_refresh_status.json",
        {
            **payload,
            "last_successful_broker_truth": payload,
            "latest_attempt_status": latest_attempt,
        },
    )
    _write(root / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_latest_attempt_status.json", latest_attempt)


def _write_live_position_status(root: Path, *, now: datetime) -> None:
    _write(
        root / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json",
        {
            "generated_at": now.isoformat(),
            "open_position_count": 0,
            "open_order_count": 0,
            "open_positions": [],
            "review_required_positions": [],
            "live_money_eligible": False,
        },
    )


def _write_trade_summary(root: Path, *, now: datetime) -> None:
    _write(
        root / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_paper_trade_summary.json",
        {
            "generated_at": now.isoformat(),
            "review_required_count": 0,
            "unknown_open_order_count": 0,
            "unresolved_intent_count": 0,
            "live_money_eligible": False,
        },
    )


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
