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


def _classification(tmp_path: Path) -> dict:
    config = TrackBPaperMinimalStartupConfig(repo_root=tmp_path)
    return build_track_b_paper_minimal_startup(config=config, now=NOW)


def _codes(payload: dict) -> set[str]:
    return {str(row.get("code")) for row in payload.get("blockers") or []}


def test_flat_broker_known_orders_price_profile_paper_route_qty_cap_allows_submit_capable(tmp_path: Path) -> None:
    _seed_minimal_ready(tmp_path)

    payload = _classification(tmp_path)

    assert payload["classification"] == "PAPER_MINIMAL_STARTUP_ALLOWED"
    assert payload["allowed"] is True
    assert payload["account_id"] == "DUM882026"
    assert payload["configured_instruments"] == ["MES", "MNQ"]
    assert all(row["available"] for row in payload["price_availability"])


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


def test_missing_current_price_blocks(tmp_path: Path) -> None:
    config = _seed_minimal_ready(tmp_path)
    (tmp_path / config.phase1_market_data_root / "MES" / "1m" / "latest_runtime_candles.json").unlink()

    result = _classification(tmp_path)

    assert "current_market_price_unavailable" in _codes(result)


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
