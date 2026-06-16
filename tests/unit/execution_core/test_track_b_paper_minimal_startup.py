from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_paper_minimal_startup import (
    TrackBPaperMinimalStartupConfig,
    build_track_b_paper_minimal_startup,
)
from mgc_v05l.execution_core.track_b_readiness_state import classify_canonical_readiness


NOW = datetime(2026, 6, 10, 1, 58, tzinfo=timezone.utc)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _seed_minimal_ready(tmp_path: Path, *, instruments: tuple[str, ...] = ("MES", "MNQ")) -> TrackBPaperMinimalStartupConfig:
    config = TrackBPaperMinimalStartupConfig(repo_root=tmp_path)
    _write_json(
        tmp_path / config.broker_truth_lease_path,
        {
            "account_id": "DUM882026",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "lease_state": "ACTIVE",
            "broker_reconciled": True,
            "broker_position_lease": {"complete": True, "fresh": True},
            "broker_open_order_lease": {"complete": True, "fresh": True},
            "track_b_broker_position_count": 0,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
        },
    )
    _write_json(
        tmp_path / config.broker_session_authority_path,
        {
            "account_id": "DUM882026",
            "classification": "BROKER_SESSION_AUTHORITY_SUBMIT_CAPABLE_NO_RECENT_ORDER_EVENTS",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write_json(
        tmp_path / config.open_order_truth_path,
        {
            "classification": "NO_OPEN_ORDERS",
            "unknown_open_order_count": 0,
            "summary": {"open_order_count": 0},
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write_json(
        tmp_path / config.reconciliation_path,
        {
            "generated_at": "2026-06-10T01:52:00+00:00",
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "review_required_count": 0,
            "track_b_broker_position_count": 0,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    lanes = [
        {
            "lane_id": f"{instrument.lower()}_test_lane",
            "instrument": instrument,
            "paper_only": True,
            "max_position_quantity": 1,
            "runtime_overlay_params": {"expected_account_id": "DUM882026"},
        }
        for instrument in instruments
    ]
    _write_json(
        tmp_path / config.config_in_force_path,
        {
            "generated_at": "2026-06-10T01:53:00+00:00",
            "active_lane_ids": [row["lane_id"] for row in lanes],
            "lanes": lanes,
            "live_money_eligible": False,
            "runtime_mode": "PAPER",
        },
    )
    (tmp_path / config.config_paths_file).parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / config.config_paths_file).write_text(
        "\n".join(
            [
                "/repo/config/base.yaml",
                "/repo/config/probationary_pattern_engine_paper.yaml",
                "/repo/outputs/probationary_pattern_engine/paper_session/runtime/paper_stack_mnq_mes_full_session_active_evidence.yaml",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    _write_json(
        tmp_path / config.paper_runtime_truth_path,
        {
            "runtime_mode": "PAPER",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    for instrument in instruments:
        _write_json(
            tmp_path / config.phase1_market_data_root / instrument / "1m" / "latest_runtime_candles.json",
            {
                "generated_at": "2026-06-10T01:57:00+00:00",
                "symbol": instrument,
                "timeframe": "1m",
                "live_money_eligible": False,
                "source": "DATABENTO_REALTIME_PHASE1",
                "bars": [
                    {
                        "bar_end": "2026-06-10T01:56:00+00:00",
                        "open": 1,
                        "high": 2,
                        "low": 1,
                        "close": 1.5,
                    }
                ],
            },
        )
    return config


def _seed_fresh_ibkr_read_only_truth(
    tmp_path: Path,
    config: TrackBPaperMinimalStartupConfig,
    *,
    mes_qty: str = "0.0",
    mnq_qty: str = "0.0",
    extra_positions: list[dict] | None = None,
    open_orders: list[dict] | None = None,
    unknown_orders: int = 0,
) -> None:
    _write_json(
        tmp_path / config.ibkr_positions_snapshot_path,
        {
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "generated_at": "2026-06-10T01:57:30+00:00",
            "ok": True,
            "positions_complete": True,
            "positions": [
                {
                    "account_id": "DUM882026",
                    "symbol": "MES",
                    "local_symbol": "MESU6",
                    "security_type": "FUT",
                    "quantity": mes_qty,
                },
                {
                    "account_id": "DUM882026",
                    "symbol": "MNQ",
                    "local_symbol": "MNQU6",
                    "security_type": "FUT",
                    "quantity": mnq_qty,
                },
                {
                    "account_id": "DUM882026",
                    "symbol": "AAPL",
                    "local_symbol": "AAPL",
                    "security_type": "STK",
                    "quantity": "900.0",
                },
                *(extra_positions or []),
            ],
        },
    )
    _write_json(
        tmp_path / config.ibkr_open_orders_snapshot_path,
        {
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "generated_at": "2026-06-10T01:57:30+00:00",
            "ok": True,
            "open_orders_complete": True,
            "open_order_count": len(open_orders or []),
            "open_orders": open_orders or [],
        },
    )
    _write_json(
        tmp_path / config.ibkr_broker_truth_refresh_status_path,
        {
            "account": "DUM882026",
            "generated_at": "2026-06-10T01:57:30+00:00",
            "classification": "BROKER_TRUTH_REFRESH_READY",
            "unknown_open_order_count": unknown_orders,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )


def _seed_managed_position(
    tmp_path: Path,
    config: TrackBPaperMinimalStartupConfig,
    *,
    symbol: str,
    local_symbol: str,
    con_id: int,
    signed_qty: str,
) -> None:
    _write_json(
        tmp_path / config.managed_position_registry_path,
        {
            "classification": "MANAGED_POSITION_REGISTRY_READY",
            "managed_positions": [
                {
                    "account_id": "DUM882026",
                    "symbol": symbol,
                    "local_symbol": local_symbol,
                    "con_id": con_id,
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "projection_authority_owner_confirmed": True,
                    "signed_broker_qty": signed_qty,
                    "lifecycle_id": f"lc-{symbol.lower()}",
                    "trade_id": f"trade-{symbol.lower()}",
                    "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
                }
            ],
        },
    )


def _classification(tmp_path: Path) -> dict:
    config = TrackBPaperMinimalStartupConfig(repo_root=tmp_path)
    return build_track_b_paper_minimal_startup(config=config, now=NOW)


def _codes(payload: dict) -> set[str]:
    return {str(row.get("code")) for row in payload.get("blockers") or []}


def _warning_codes(payload: dict) -> set[str]:
    return {str(row.get("code")) for row in payload.get("warnings") or []}


def test_flat_broker_known_orders_price_profile_paper_route_qty_cap_allows_submit_capable(tmp_path: Path) -> None:
    _seed_minimal_ready(tmp_path)

    payload = _classification(tmp_path)

    assert payload["classification"] == "PAPER_MINIMAL_STARTUP_ALLOWED"
    assert payload["allowed"] is True
    assert payload["account_id"] == "DUM882026"
    assert payload["configured_instruments"] == ["MES", "MNQ"]
    assert all(row["available"] for row in payload["price_availability"])


def test_batch1_track_b_futures_position_blocks_minimal_startup(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    _seed_fresh_ibkr_read_only_truth(
        tmp_path,
        config,
        extra_positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MGC",
                "local_symbol": "MGCQ6",
                "security_type": "FUT",
                "quantity": "1.0",
            }
        ],
    )

    payload = _classification(tmp_path)

    assert payload["classification"] == "PAPER_MINIMAL_STARTUP_BLOCKED"
    assert "broker_positions_present" in _codes(payload)


def test_live_money_true_blocks(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    payload_path = tmp_path / config.broker_truth_lease_path
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    payload["live_money_eligible"] = True
    payload_path.write_text(json.dumps(payload), encoding="utf-8")

    result = _classification(tmp_path)

    assert result["allowed"] is False
    assert "live_money_eligible_true" in _codes(result)


def test_paper_proof_true_blocks(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    payload_path = tmp_path / config.open_order_truth_path
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    payload["paper_proof_invoked"] = True
    payload_path.write_text(json.dumps(payload), encoding="utf-8")

    result = _classification(tmp_path)

    assert result["allowed"] is False
    assert "paper_proof_invoked_true" in _codes(result)


def test_broker_positions_unavailable_blocks(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    payload_path = tmp_path / config.broker_truth_lease_path
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    payload["broker_position_lease"]["complete"] = False
    payload_path.write_text(json.dumps(payload), encoding="utf-8")

    result = _classification(tmp_path)

    assert "broker_positions_unavailable" in _codes(result)


def test_broker_open_orders_unavailable_blocks(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    payload_path = tmp_path / config.broker_truth_lease_path
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    payload["broker_open_order_lease"]["complete"] = False
    payload_path.write_text(json.dumps(payload), encoding="utf-8")

    result = _classification(tmp_path)

    assert "broker_open_orders_unavailable" in _codes(result)


def test_unknown_or_conflicting_open_orders_block(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    payload_path = tmp_path / config.open_order_truth_path
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    payload["unknown_open_order_count"] = 1
    payload_path.write_text(json.dumps(payload), encoding="utf-8")

    result = _classification(tmp_path)

    assert "unknown_open_orders_present" in _codes(result)


def test_nonzero_broker_position_blocks_flat_required_startup(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    payload_path = tmp_path / config.broker_truth_lease_path
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    payload["track_b_broker_position_count"] = 1
    payload_path.write_text(json.dumps(payload), encoding="utf-8")

    result = _classification(tmp_path)

    assert result["allowed"] is False
    assert "broker_positions_present" in _codes(result)


def test_fresh_ibkr_flat_truth_overrides_stale_broker_position_lease_count(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    lease_path = tmp_path / config.broker_truth_lease_path
    lease = json.loads(lease_path.read_text(encoding="utf-8"))
    lease["lease_state"] = "INVALIDATED_CONTRADICTION"
    lease["track_b_broker_position_count"] = 1
    lease_path.write_text(json.dumps(lease), encoding="utf-8")
    _seed_fresh_ibkr_read_only_truth(tmp_path, config)

    result = _classification(tmp_path)

    assert result["allowed"] is True
    assert result["broker_position_count"] == 0
    assert "broker_positions_present" not in _codes(result)
    assert "broker_position_lease_count_diagnostic" in _warning_codes(result)


def test_fresh_ibkr_nonflat_truth_blocks_even_when_lease_count_is_zero(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    _seed_fresh_ibkr_read_only_truth(tmp_path, config, mes_qty="-1.0")

    result = _classification(tmp_path)

    assert result["allowed"] is False
    assert result["broker_position_count"] == 1
    assert "broker_positions_present" in _codes(result)


def test_fresh_ibkr_known_managed_position_allows_supervised_startup(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    _seed_fresh_ibkr_read_only_truth(
        tmp_path,
        config,
        extra_positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MGC",
                "local_symbol": "MGCQ6",
                "con_id": 456,
                "security_type": "FUT",
                "quantity": "-1.0",
            }
        ],
    )
    _seed_managed_position(
        tmp_path,
        config,
        symbol="MGC",
        local_symbol="MGCQ6",
        con_id=456,
        signed_qty="-1",
    )

    result = _classification(tmp_path)

    assert result["allowed"] is True
    assert result["broker_position_count"] == 1
    assert "broker_positions_present" not in _codes(result)
    assert "broker_positions_known_managed_startup_diagnostic" in _warning_codes(result)


def test_fresh_ibkr_open_order_truth_blocks_even_when_lease_count_is_zero(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    _seed_fresh_ibkr_read_only_truth(
        tmp_path,
        config,
        open_orders=[
            {
                "account": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQU6",
                "order_id": 129,
                "status": "Submitted",
            }
        ],
    )

    result = _classification(tmp_path)

    assert result["allowed"] is False
    assert result["broker_open_order_count"] == 1
    assert "broker_open_orders_present" in _codes(result)


def test_fresh_ibkr_unknown_order_truth_blocks_even_when_lease_count_is_zero(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    _seed_fresh_ibkr_read_only_truth(tmp_path, config, unknown_orders=1)

    result = _classification(tmp_path)

    assert result["allowed"] is False
    assert result["unknown_open_order_count"] == 1
    assert "unknown_open_orders_present" in _codes(result)


def test_stale_dirty_reconciliation_is_diagnostic_when_broker_truth_is_flat_and_clean(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    reconciliation_path = tmp_path / config.reconciliation_path
    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    reconciliation.update(
        {
            "classification": "BROKER_TRUTH_SETTLEMENT_TIMEOUT",
            "broker_reconciled": False,
            "review_required_count": 2,
            "current_scope_review_required_count": 2,
            "track_b_broker_position_count": 2,
            "track_b_broker_open_order_count": 1,
        }
    )
    reconciliation_path.write_text(json.dumps(reconciliation), encoding="utf-8")

    result = _classification(tmp_path)

    assert result["allowed"] is True
    assert "broker_reconciliation_dirty" not in _codes(result)
    assert "review_required_current_scope" not in _codes(result)
    assert {
        "broker_reconciliation_dirty_diagnostic",
        "review_required_current_scope_diagnostic",
        "reconciliation_position_count_mismatch_diagnostic",
        "reconciliation_open_order_count_mismatch_diagnostic",
    }.issubset(_warning_codes(result))


def test_stale_open_order_truth_classification_is_diagnostic_when_broker_orders_are_zero(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    truth_path = tmp_path / config.open_order_truth_path
    truth = json.loads(truth_path.read_text(encoding="utf-8"))
    truth["classification"] = "UNKNOWN_BROKER_OPEN_ORDER"
    truth["unknown_open_order_count"] = 0
    truth["summary"] = {"open_order_count": 0, "unknown_open_order_count": 0}
    truth_path.write_text(json.dumps(truth), encoding="utf-8")

    result = _classification(tmp_path)

    assert result["allowed"] is True
    assert "open_order_truth_not_clean" not in _codes(result)
    assert "open_order_truth_stale_diagnostic" in _warning_codes(result)


def test_aggregate_broker_lease_contradiction_is_diagnostic_when_evidence_leases_are_fresh(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    lease_path = tmp_path / config.broker_truth_lease_path
    lease = json.loads(lease_path.read_text(encoding="utf-8"))
    lease["lease_state"] = "INVALIDATED_CONTRADICTION"
    lease_path.write_text(json.dumps(lease), encoding="utf-8")

    result = _classification(tmp_path)

    assert result["allowed"] is True
    assert "broker_truth_aggregate_state_diagnostic" in _warning_codes(result)


def test_missing_current_price_is_startup_diagnostic(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    (tmp_path / config.phase1_market_data_root / "MES" / "1m" / "latest_runtime_candles.json").unlink()

    result = _classification(tmp_path)

    assert result["allowed"] is True
    assert result["classification"] == "PAPER_MINIMAL_STARTUP_ALLOWED"
    assert "current_market_price_unavailable_startup_diagnostic" in _warning_codes(result)


def test_missing_profile_config_blocks(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    (tmp_path / config.config_paths_file).write_text("/repo/config/base.yaml\n", encoding="utf-8")

    result = _classification(tmp_path)

    assert "explicit_paper_profile_missing" in _codes(result)


def test_non_paper_route_or_wrong_account_blocks(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    payload_path = tmp_path / config.config_in_force_path
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    payload["lanes"][0]["paper_only"] = False
    payload["lanes"][1]["runtime_overlay_params"]["expected_account_id"] = "LIVE123"
    payload_path.write_text(json.dumps(payload), encoding="utf-8")

    result = _classification(tmp_path)

    assert "submit_route_not_paper_only" in _codes(result)
    assert "lane_account_not_allowed" in _codes(result)


def test_order_size_above_paper_cap_blocks(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    payload_path = tmp_path / config.config_in_force_path
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    payload["lanes"][0]["max_position_quantity"] = 2
    payload_path.write_text(json.dumps(payload), encoding="utf-8")

    result = _classification(tmp_path)

    assert "paper_order_size_limit_exceeded" in _codes(result)


def test_stale_publication_and_historical_debris_are_diagnostic_when_core_invariants_pass(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    reconciliation_path = tmp_path / config.reconciliation_path
    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    reconciliation["generated_at"] = "2026-06-10T01:00:00+00:00"
    reconciliation["historical_review_rows"] = [{"classification": "REVIEW_REQUIRED"}]
    reconciliation_path.write_text(json.dumps(reconciliation), encoding="utf-8")

    result = _classification(tmp_path)

    assert result["allowed"] is True
    assert "reconciliation_freshness_lag_diagnostic" in {
        row["code"] for row in result["warnings"]
    }
    assert result["diagnostics_only_categories"]


def test_canonical_readiness_uses_minimal_startup_when_runtime_alive() -> None:
    result = classify_canonical_readiness(
        {
            "generated_at": "2026-06-10T01:58:00+00:00",
            "live_money_eligible": False,
            "root_guard_summary": {"root_match": True},
            "runtime": {
                "running": True,
                "healthy": True,
                "loaded_lane_count": 1,
                "eligible_lane_count": 1,
                "runtime_ingestion_fresh": False,
            },
            "paper_minimal_startup": {
                "allowed": True,
                "classification": "PAPER_MINIMAL_STARTUP_ALLOWED",
                "warnings": [
                    {
                        "code": "control_plane_publication_diagnostic",
                        "detail": "Control-plane publication is diagnostic.",
                        "source": "paper_minimal_startup",
                    }
                ],
            },
            "control_plane_authorization": {
                "available": True,
                "fresh": False,
                "shared_truth_coherence_status": "STALE_OR_MIXED",
            },
            "phase1_reconciliation": {
                "available": True,
                "fresh": False,
                "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
                "broker_reconciled": True,
                "review_required_count": 0,
                "lifecycle_open_position_count": 0,
            },
        }
    )

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["submit_allowed"] is True
    assert result["readiness_blockers"] == []
