from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_position_state import (
    TrackBPositionStateReportConfig,
    build_track_b_position_state_report,
)


NOW = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)


def test_flat_broker_state_has_no_current_positions(tmp_path: Path) -> None:
    report = _report(tmp_path, broker_positions=[])

    assert report["classification"] == "POSITION_STATE_FLAT"
    assert report["position_count"] == 0
    assert report["positions"] == []


def test_long_broker_exposure_normalizes_long_position(tmp_path: Path) -> None:
    report = _report(tmp_path, broker_positions=[_broker_position(quantity="2")])

    assert report["classification"] == "POSITION_STATE_CURRENT_POSITIONS"
    position = report["positions"][0]
    assert position["execution_domain"] == "TRACK_B_PAPER"
    assert position["account_id"] == "DUM882026"
    assert position["con_id"] == 770561194
    assert position["local_symbol"] == "MESM6"
    assert position["instrument"] == "MES"
    assert position["side"] == "LONG"
    assert position["qty"] == "2"
    assert position["owned_qty"] is None
    assert position["attribution_status"] == "UNATTRIBUTED"
    assert position["current_scope"] is True


def test_short_broker_exposure_normalizes_short_position(tmp_path: Path) -> None:
    report = _report(tmp_path, broker_positions=[_broker_position(quantity="-3")])

    position = report["positions"][0]
    assert position["side"] == "SHORT"
    assert position["qty"] == "3"
    assert position["owned_qty"] is None
    assert position["attribution_status"] == "UNATTRIBUTED"


def test_exact_lifecycle_match_attributes_position(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        broker_positions=[_broker_position(quantity="-1")],
        managed_positions=[
            _managed_position(
                side="SHORT",
                lifecycle_id="lifecycle_1",
                trade_id="trade_1",
                strategy_id="strategy_1",
                lane_id="lane_1",
            )
        ],
    )

    position = report["positions"][0]
    assert position["attribution_status"] == "ATTRIBUTED"
    assert position["owned_qty"] == "1"
    assert position["lifecycle_id"] == "lifecycle_1"
    assert position["trade_id"] == "trade_1"
    assert position["strategy_id"] == "strategy_1"
    assert position["lane_id"] == "lane_1"


def test_replacement_contract_identity_enriched_from_current_managed_position(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        broker_positions=[
            _broker_position(
                local_symbol="",
                con_id=0,
                symbol="MNQ",
                quantity="-1",
            )
        ],
        managed_positions=[
            _managed_position(
                local_symbol="MNQU6",
                con_id=793356225,
                side="SHORT",
                lifecycle_id="life-mnq",
                trade_id="trade-mnq",
                strategy_id="mnq_strategy",
                lane_id="mnq_lane",
            )
        ],
    )

    position = report["positions"][0]
    assert position["con_id"] == 793356225
    assert position["local_symbol"] == "MNQU6"
    assert position["instrument"] == "MNQ"
    assert position["attribution_status"] == "ATTRIBUTED"
    assert any(row.get("kind") == "contract_identity_enrichment" for row in position["diagnostic_rows"])


def test_missing_lifecycle_is_unattributed_but_still_broker_risk(tmp_path: Path) -> None:
    report = _report(tmp_path, broker_positions=[_broker_position(quantity="1")], managed_positions=[])

    position = report["positions"][0]
    assert position["attribution_status"] == "UNATTRIBUTED"
    assert position["owned_qty"] is None
    assert position["local_symbol"] == "MESM6"


def test_partial_lifecycle_evidence_is_partially_attributed(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        broker_positions=[_broker_position(quantity="1")],
        managed_positions=[_managed_position(side="LONG", lifecycle_id="lifecycle_1", trade_id="trade_1")],
    )

    position = report["positions"][0]
    assert position["attribution_status"] == "PARTIALLY_ATTRIBUTED"
    assert position["owned_qty"] == "1"
    assert position["lifecycle_id"] == "lifecycle_1"
    assert position["trade_id"] == "trade_1"
    assert position["strategy_id"] is None
    assert position["lane_id"] is None


def test_historical_stale_row_does_not_create_current_exposure(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        broker_positions=[],
        managed_positions=[
            {
                "classification": "REVIEW_REQUIRED",
                "local_symbol": "MNQM6",
                "lifecycle_id": "bridge_fill_MNQ|1m|2026-05-22T17:42:00Z|BUY_TO_OPEN",
                "historical_only": True,
                "current_hot_path_scope": "HISTORICAL_UNRESOLVED_FULL_AUDIT_ONLY",
            }
        ],
    )

    assert report["classification"] == "POSITION_STATE_FLAT"
    assert report["positions"] == []
    assert report["diagnostic_rows"][0]["kind"] == "managed_positions"
    assert report["diagnostic_rows"][0]["row"]["historical_only"] is True


def test_wrong_account_position_is_not_mixed_into_position_state(tmp_path: Path) -> None:
    report = _report(tmp_path, broker_positions=[_broker_position(account_id="OTHER", quantity="1")])

    assert report["classification"] == "POSITION_STATE_BLOCKED_WRONG_SCOPE"
    assert report["positions"] == []
    assert report["wrong_scope_row_count"] == 1
    assert report["diagnostic_rows"][0]["account_id"] == "OTHER"


def test_wrong_scope_row_blocks_mixed_position_publication(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        broker_positions=[
            _broker_position(account_id="OTHER", quantity="1"),
            _broker_position(account_id="DUM882026", quantity="-1"),
        ],
    )

    assert report["classification"] == "POSITION_STATE_BLOCKED_WRONG_SCOPE"
    assert report["position_count"] == 0
    assert report["positions"] == []
    assert report["wrong_scope_row_count"] == 1


def test_multiple_current_positions_are_independent_rows(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        broker_positions=[
            _broker_position(local_symbol="MESM6", con_id=770561194, symbol="MES", quantity="2"),
            _broker_position(local_symbol="MNQM6", con_id=770561201, symbol="MNQ", quantity="-1"),
        ],
        managed_positions=[
            _managed_position(
                local_symbol="MESM6",
                con_id=770561194,
                side="LONG",
                quantity="2",
                lifecycle_id="mes_lifecycle",
                trade_id="mes_trade",
                strategy_id="mes_strategy",
                lane_id="mes_lane",
            ),
            _managed_position(
                local_symbol="MNQM6",
                con_id=770561201,
                side="SHORT",
                lifecycle_id="mnq_lifecycle",
                trade_id="mnq_trade",
                strategy_id="mnq_strategy",
                lane_id="mnq_lane",
            ),
        ],
    )

    assert report["position_count"] == 2
    rows = {row["local_symbol"]: row for row in report["positions"]}
    assert rows["MESM6"]["side"] == "LONG"
    assert rows["MESM6"]["qty"] == "2"
    assert rows["MESM6"]["attribution_status"] == "ATTRIBUTED"
    assert rows["MNQM6"]["side"] == "SHORT"
    assert rows["MNQM6"]["qty"] == "1"
    assert rows["MNQM6"]["attribution_status"] == "ATTRIBUTED"


def test_source_refs_and_diagnostics_are_preserved(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        broker_positions=[_broker_position(quantity="1")],
        managed_positions=[
            _managed_position(side="LONG", lifecycle_id="lifecycle_1"),
            {
                "classification": "STALE_DERIVED_REGISTRY_CHAIN_FULL_AUDIT_ONLY",
                "local_symbol": "MESM6",
                "diagnostic_only": True,
            },
        ],
    )

    position = report["positions"][0]
    assert [ref["name"] for ref in position["source_artifact_refs"]] == ["reconciliation", "managed_positions"]
    assert position["diagnostic_rows"][0]["row"]["diagnostic_only"] is True
    assert report["source_artifact_paths"]["reconciliation"].endswith("latest_track_b_paper_broker_reconciliation.json")


def _report(
    tmp_path: Path,
    *,
    broker_positions: list[dict],
    managed_positions: list[dict] | None = None,
) -> dict:
    config = TrackBPositionStateReportConfig(repo_root=tmp_path)
    return build_track_b_position_state_report(
        config=config,
        now=NOW,
        input_overrides={
            "reconciliation": {
                "schema_version": "track_b_paper_broker_reconciliation_v1",
                "generated_at": NOW.isoformat(),
                "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
                "broker_reconciled": True,
                "track_b_broker_position_count": len(broker_positions),
                "track_b_broker_positions": broker_positions,
                "track_b_broker_open_order_count": 0,
                "unknown_broker_open_order_count": 0,
                "live_money_eligible": False,
                "paper_proof_invoked": False,
            },
            "managed_positions": {
                "schema_version": "track_b_managed_positions_v1",
                "generated_at": NOW.isoformat(),
                "classification": "OPEN_MANAGED_MATCHED" if managed_positions else "NO_MANAGED_POSITIONS",
                "managed_positions": managed_positions or [],
                "live_money_eligible": False,
                "paper_proof_invoked": False,
            },
        },
    )


def _broker_position(
    *,
    account_id: str = "DUM882026",
    local_symbol: str = "MESM6",
    con_id: int = 770561194,
    symbol: str = "MES",
    quantity: str = "1",
) -> dict:
    return {
        "account_id": account_id,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "symbol": symbol,
        "track_b_root": symbol,
        "quantity": quantity,
    }


def _managed_position(
    *,
    account_id: str = "DUM882026",
    local_symbol: str = "MESM6",
    con_id: int = 770561194,
    side: str = "LONG",
    quantity: str = "1",
    lifecycle_id: str | None = None,
    trade_id: str | None = None,
    strategy_id: str | None = None,
    lane_id: str | None = None,
) -> dict:
    return {
        "account_id": account_id,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "side": side,
        "quantity": quantity,
        "lifecycle_id": lifecycle_id,
        "trade_id": trade_id,
        "strategy_id": strategy_id,
        "lane_id": lane_id,
        "classification": "OPEN_MANAGED_MATCHED",
    }
