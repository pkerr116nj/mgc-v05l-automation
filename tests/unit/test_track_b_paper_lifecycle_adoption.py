from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.app.track_b_paper_lifecycle_adoption import (
    LifecycleAdoptionConfig,
    run_track_b_paper_lifecycle_adoption,
)


def test_pl_lifecycle_adoption_reconstructs_fill_trade_and_ledger(tmp_path: Path) -> None:
    repo = _write_pl_evidence(tmp_path)

    result = run_track_b_paper_lifecycle_adoption(
        config=LifecycleAdoptionConfig(
            repo_root=repo,
            order_intent_id="PL|1m|2026-05-13T00:41:00Z|BUY_TO_OPEN",
            apply=True,
        ),
        now=_now(),
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_APPLIED"
    fill_rows = _read_jsonl(repo / "outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_pl_asia_us/fills.jsonl")
    trade_rows = _read_jsonl(repo / "outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_pl_asia_us/trades.jsonl")
    assert len(fill_rows) == 1
    assert len(trade_rows) == 1
    assert fill_rows[0]["account_id"] == "DUM882026"
    assert fill_rows[0]["symbol"] == "PL"
    assert fill_rows[0]["local_symbol"] == "PLN6"
    assert fill_rows[0]["con_id"] == 644855286
    assert fill_rows[0]["perm_id"] == 1984099439
    assert fill_rows[0]["client_id"] == 10905
    assert fill_rows[0]["execution_id"] == "0000e1a7.6a06001d.01.01"
    assert fill_rows[0]["broker_cost_basis_adjustment"] == "0.0504"
    assert trade_rows[0]["final_position_status"] == "OPEN_MANAGED"
    assert trade_rows[0]["entry_exec_id"] == "0000e1a7.6a06001d.01.01"

    live_positions = _read_json(repo / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json")
    assert live_positions["open_position_count"] == 1
    position = live_positions["positions_by_instrument"]["PL-202607"]
    assert position["local_symbol"] == "PLN6"
    assert position["con_id"] == 644855286
    assert position["entry_perm_id"] == 1984099439


def test_lifecycle_adoption_refuses_identity_mismatch(tmp_path: Path) -> None:
    repo = _write_pl_evidence(tmp_path, broker_local_symbol="PLM6")

    result = run_track_b_paper_lifecycle_adoption(
        config=LifecycleAdoptionConfig(
            repo_root=repo,
            order_intent_id="PL|1m|2026-05-13T00:41:00Z|BUY_TO_OPEN",
            apply=True,
        ),
        now=_now(),
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_REFUSED"
    assert "Expected exactly one matching broker position, found 0." in result.report["failures"]
    fills_path = repo / "outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_pl_asia_us/fills.jsonl"
    assert _read_jsonl(fills_path) == []


def test_lifecycle_adoption_refuses_missing_bridge_evidence(tmp_path: Path) -> None:
    repo = _write_pl_evidence(tmp_path)
    bridge_path = repo / "outputs/reports/ibkr_runtime_route_dispatch/atp_companion_v1_pl_asia_us/ibkr_paper_strategy_bridge_report.json"
    bridge_path.unlink()

    result = run_track_b_paper_lifecycle_adoption(
        config=LifecycleAdoptionConfig(
            repo_root=repo,
            order_intent_id="PL|1m|2026-05-13T00:41:00Z|BUY_TO_OPEN",
            apply=True,
        ),
        now=_now(),
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_REFUSED"
    assert "Missing bridge report evidence." in result.report["failures"]
    fills_path = repo / "outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_pl_asia_us/fills.jsonl"
    assert _read_jsonl(fills_path) == []


def test_lifecycle_adoption_is_idempotent(tmp_path: Path) -> None:
    repo = _write_pl_evidence(tmp_path)
    config = LifecycleAdoptionConfig(
        repo_root=repo,
        order_intent_id="PL|1m|2026-05-13T00:41:00Z|BUY_TO_OPEN",
        apply=True,
    )

    first = run_track_b_paper_lifecycle_adoption(config=config, now=_now())
    second = run_track_b_paper_lifecycle_adoption(config=config, now=_now())

    assert first.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_APPLIED"
    assert second.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_APPLIED"
    lane_dir = repo / "outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_pl_asia_us"
    assert len(_read_jsonl(lane_dir / "fills.jsonl")) == 1
    assert len(_read_jsonl(lane_dir / "trades.jsonl")) == 1
    assert len(_read_jsonl(repo / "outputs/track_b_execution_core/paper_trade_ledger/track_b_paper_trade_ledger.jsonl")) == 1
    assert second.report["post_adoption"]["fill_written"] is False
    assert second.report["post_adoption"]["trade_written"] is False
    assert second.report["post_adoption"]["compact_ledger_trade_record_written"] is False


def _write_pl_evidence(tmp_path: Path, *, broker_local_symbol: str = "PLN6") -> Path:
    repo = tmp_path
    lane_dir = repo / "outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_pl_asia_us"
    bridge_dir = repo / "outputs/reports/ibkr_runtime_route_dispatch/atp_companion_v1_pl_asia_us"
    broker_dir = repo / "outputs/reports/ibkr_read_only_verification"
    lane_dir.mkdir(parents=True, exist_ok=True)
    bridge_dir.mkdir(parents=True, exist_ok=True)
    broker_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "fills.jsonl").write_text("", encoding="utf-8")
    (lane_dir / "trades.jsonl").write_text("", encoding="utf-8")
    _write_jsonl(
        lane_dir / "order_intents.jsonl",
        [
            {
                "order_intent_id": "PL|1m|2026-05-13T00:41:00Z|BUY_TO_OPEN",
                "lane_id": "atp_companion_v1_pl_asia_us",
                "standalone_strategy_id": "atp_companion_v1__paper_pl_asia_us",
                "symbol": "PL",
                "intent_type": "BUY_TO_OPEN",
                "quantity": 1,
                "broker_order_id": "1",
                "broker_order_status": "FILLED",
                "reason_code": "trend_participation.atp_v1_long_pullback_continuation.long.base",
            }
        ],
    )
    _write_json(
        broker_dir / "ibkr_positions_snapshot.json",
        {
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "generated_at": "2026-05-13T02:41:07.588344+00:00",
            "mode": "PAPER",
            "positions": [
                {
                    "account_id": "DUM882026",
                    "average_cost": "107257.52",
                    "currency": "USD",
                    "expiry": "20260729",
                    "local_symbol": broker_local_symbol,
                    "multiplier": "50",
                    "quantity": "1.0",
                    "security_type": "FUT",
                    "symbol": "PL",
                    "updated_at": "2026-05-13T02:41:07.581358+00:00",
                }
            ],
        },
    )
    _write_json(
        bridge_dir / "ibkr_paper_strategy_bridge_report.json",
        {
            "classification": "PAPER_STRATEGY_ORDER_FILLED",
            "selected_account_id": "DUM882026",
            "environment": {"mode": "PAPER", "host": "127.0.0.1", "port": 7497, "client_id": 10905},
            "strategy_identity": {"strategy_id": "atp_companion_v1_pl_asia_us"},
            "intent": {
                "action": "BUY",
                "strategy_id": "atp_companion_v1_pl_asia_us",
                "symbol": "PL",
                "quantity": 1.0,
                "paper_only": True,
            },
            "exact_contract_report": {
                "exact_contract": {
                    "broker_symbol": "PL",
                    "con_id": 644855286,
                    "expiry": "20260729",
                    "local_symbol": "PLN6",
                    "multiplier": "50",
                }
            },
            "qualified_contract_report": {
                "qualified_contract": {
                    "broker_symbol": "PL",
                    "con_id": 644855286,
                    "expiry": "20260729",
                    "local_symbol": "PLN6",
                    "multiplier": "50",
                }
            },
            "delegated_result": {
                "classification": "PAPER_ORDER_FILLED",
                "report": {
                    "preview_payload": {
                        "contract": {
                            "symbol": "PL",
                            "expiry": "202607",
                            "local_symbol": "PLN6",
                            "multiplier": "50",
                            "qualified_contract_identifier": 644855286,
                        }
                    },
                    "submit_cancel_lifecycle": {
                        "latest_order_status": {
                            "status": "Filled",
                            "order_id": 1,
                            "perm_id": 1984099439,
                            "client_id": 10905,
                            "filled": 1.0,
                            "avg_fill_price": 2145.1,
                            "updated_at": "2026-05-13T00:57:24.472827+00:00",
                        },
                        "executions_after_submit": [
                            {
                                "account_id": "DUM882026",
                                "broker_order_id": "1",
                                "executed_at": "2026-05-13T00:57:24.470589+00:00",
                                "execution_id": "0000e1a7.6a06001d.01.01",
                                "price": "2145.1",
                                "quantity": "1.0",
                                "symbol": "PL",
                            },
                            {
                                "account_id": "DUM882026",
                                "broker_order_id": "1",
                                "executed_at": "2026-05-13T00:57:24.570324+00:00",
                                "execution_id": "0000e1a7.6a04620e.01.01",
                                "price": "4703.3",
                                "quantity": "1.0",
                                "symbol": "GC",
                            },
                            {
                                "account_id": "DUM882026",
                                "broker_order_id": "1",
                                "executed_at": "2026-05-13T00:57:24.570513+00:00",
                                "execution_id": "0000e1a7.6a06001d.01.01",
                                "price": "2145.1",
                                "quantity": "1.0",
                                "symbol": "PL",
                            },
                        ],
                    },
                },
            },
        },
    )
    return repo


def _now() -> datetime:
    return datetime(2026, 5, 13, 3, 0, tzinfo=UTC)


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")
