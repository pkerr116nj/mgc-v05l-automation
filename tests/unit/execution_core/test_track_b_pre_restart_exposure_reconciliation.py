from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from mgc_v05l.execution_core.track_b_central_trade_registry import TradeEvent, TradeEventType, reduce_trade_events
from mgc_v05l.execution_core.track_b_pre_restart_exposure_reconciliation import (
    MANAGED_EXPOSURE_RESOLVED,
    NO_OPEN_EXPOSURE,
    PROJECTION_STALE_MANAGED_EXPOSURE_RESOLVED,
    REVIEW_REQUIRED_UNMANAGED_BROKER_EXPOSURE,
    PreRestartExposureResolverConfig,
    resolve_pre_restart_exposure_reconciliation,
)


NOW = datetime(2026, 6, 1, 22, 10, tzinfo=UTC)


def test_no_broker_position_has_no_open_exposure(tmp_path: Path) -> None:
    payload = resolve_pre_restart_exposure_reconciliation(
        config=PreRestartExposureResolverConfig(repo_root=tmp_path),
        broker_positions=[],
        lifecycle_positions=[],
    )

    assert payload["classification"] == NO_OPEN_EXPOSURE
    assert payload["no_broad_flatten_generated"] is True


def test_broker_position_and_registry_open_managed_stale_lifecycle_projection_resolves(tmp_path: Path) -> None:
    record = _registry_record(symbol="MES", local_symbol="MESM6", con_id=770561194, side="SHORT")

    payload = resolve_pre_restart_exposure_reconciliation(
        config=PreRestartExposureResolverConfig(repo_root=tmp_path),
        broker_positions=[_broker_position(symbol="MES", local_symbol="MESM6", con_id=770561194, quantity="-1")],
        lifecycle_positions=[],
        registry_records=[record],
    )

    assert payload["classification"] == PROJECTION_STALE_MANAGED_EXPOSURE_RESOLVED
    assert payload["restart_with_owned_exposure_allowed"] is True
    assert payload["resolved_lifecycle_positions"][0]["trade_id"] == "trade_mes"
    assert payload["resolved_lifecycle_positions"][0]["lifecycle_id"] == "life_mes"


def test_mes_regression_missing_broker_con_id_resolves_from_registry_identity(tmp_path: Path) -> None:
    record = _registry_record(symbol="MES", local_symbol="MESM6", con_id=770561194, side="SHORT")

    payload = resolve_pre_restart_exposure_reconciliation(
        config=PreRestartExposureResolverConfig(repo_root=tmp_path),
        broker_positions=[_broker_position(symbol="MES", local_symbol="MESM6", con_id=None, quantity="-1")],
        lifecycle_positions=[],
        registry_records=[record],
    )

    assert payload["classification"] == PROJECTION_STALE_MANAGED_EXPOSURE_RESOLVED
    assert payload["restart_with_owned_exposure_allowed"] is True
    assert payload["managed_exposures"][0]["canonical_broker_position"]["con_id"] == 770561194
    assert payload["managed_exposures"][0]["canonical_identity_resolution"]["classification"] == (
        "BROKER_POSITION_IDENTITY_READY"
    )


def test_ambiguous_missing_broker_con_id_blocks_restart(tmp_path: Path) -> None:
    first = _registry_record(symbol="MES", local_symbol="MESM6", con_id=770561194, side="SHORT")
    second = _registry_record(
        symbol="MES",
        local_symbol="MESM6",
        con_id=999999999,
        side="SHORT",
        trade_id="trade_mes_ambiguous",
        lifecycle_id="life_mes_ambiguous",
    )

    payload = resolve_pre_restart_exposure_reconciliation(
        config=PreRestartExposureResolverConfig(repo_root=tmp_path),
        broker_positions=[_broker_position(symbol="MES", local_symbol="MESM6", con_id=None, quantity="-1")],
        lifecycle_positions=[],
        registry_records=[first, second],
    )

    assert payload["classification"] == REVIEW_REQUIRED_UNMANAGED_BROKER_EXPOSURE
    assert payload["restart_with_owned_exposure_allowed"] is False
    assert payload["review_required_exposures"][0]["reason_codes"] == [
        "AMBIGUOUS_CON_ID_FOR_LOCAL_SYMBOL_ACCOUNT"
    ]


def test_broker_position_and_existing_lifecycle_projection_resolves_managed(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(symbol="MNQ", local_symbol="MNQM6", con_id=770561201, side="SHORT")

    payload = resolve_pre_restart_exposure_reconciliation(
        config=PreRestartExposureResolverConfig(repo_root=tmp_path),
        broker_positions=[_broker_position(symbol="MNQ", local_symbol="MNQM6", con_id=770561201, quantity="-1")],
        lifecycle_positions=[lifecycle],
    )

    assert payload["classification"] == MANAGED_EXPOSURE_RESOLVED
    assert payload["managed_exposures"][0]["lifecycle_id"] == "life_MNQ"


def test_broker_position_without_identity_remains_review_required(tmp_path: Path) -> None:
    payload = resolve_pre_restart_exposure_reconciliation(
        config=PreRestartExposureResolverConfig(repo_root=tmp_path),
        broker_positions=[_broker_position(symbol="MES", local_symbol="MESM6", con_id=770561194, quantity="-1")],
        lifecycle_positions=[],
        registry_records=[],
    )

    assert payload["classification"] == REVIEW_REQUIRED_UNMANAGED_BROKER_EXPOSURE
    assert payload["restart_with_owned_exposure_allowed"] is False
    assert payload["review_required_exposures"][0]["reason_codes"] == [
        "NO_EXACT_REGISTRY_OR_BROKER_BACKED_LIFECYCLE_IDENTITY"
    ]


def test_open_order_prevents_restart_even_when_exposure_identity_resolves(tmp_path: Path) -> None:
    record = _registry_record(symbol="MES", local_symbol="MESM6", con_id=770561194, side="SHORT")

    payload = resolve_pre_restart_exposure_reconciliation(
        config=PreRestartExposureResolverConfig(repo_root=tmp_path),
        broker_positions=[_broker_position(symbol="MES", local_symbol="MESM6", con_id=770561194, quantity="-1")],
        lifecycle_positions=[],
        broker_open_orders=[{"local_symbol": "MESM6", "con_id": 770561194}],
        registry_records=[record],
    )

    assert payload["classification"] == PROJECTION_STALE_MANAGED_EXPOSURE_RESOLVED
    assert payload["restart_with_owned_exposure_allowed"] is False


def _registry_record(
    *,
    symbol: str,
    local_symbol: str,
    con_id: int,
    side: str,
    trade_id: str | None = None,
    lifecycle_id: str | None = None,
):
    trade_id = trade_id or f"trade_{symbol.lower()}"
    lifecycle_id = lifecycle_id or f"life_{symbol.lower()}"
    return reduce_trade_events(
        [
            _event(
                event_type=TradeEventType.ENTRY_FILL_BROKER_BACKED,
                trade_id=trade_id,
                lifecycle_id=lifecycle_id,
                symbol=symbol,
                local_symbol=local_symbol,
                con_id=con_id,
                side=side,
                action="SELL" if side == "SHORT" else "BUY",
                perm_id=f"perm_{symbol}",
                exec_id=f"exec_{symbol}",
                generated_at=NOW,
            ),
            _event(
                event_type=TradeEventType.LIFECYCLE_OPEN_MANAGED,
                trade_id=trade_id,
                lifecycle_id=lifecycle_id,
                symbol=symbol,
                local_symbol=local_symbol,
                con_id=con_id,
                side=side,
                action="SELL" if side == "SHORT" else "BUY",
                generated_at=NOW + timedelta(seconds=1),
            ),
        ]
    )


def _event(
    *,
    event_type: TradeEventType,
    trade_id: str,
    lifecycle_id: str,
    symbol: str,
    local_symbol: str,
    con_id: int,
    side: str,
    action: str,
    generated_at: datetime,
    perm_id: str | None = None,
    exec_id: str | None = None,
) -> TradeEvent:
    return TradeEvent(
        event_id=f"{trade_id}_{event_type.value}_{generated_at.timestamp()}",
        event_type=event_type,
        generated_at=generated_at,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id=f"{symbol.lower()}_globex_active_participation_short",
        thesis_strategy_id=f"{symbol.lower()}_globex_active_participation_short",
        account_id="DUM882026",
        symbol=symbol,
        con_id=con_id,
        local_symbol=local_symbol,
        expiry="202606",
        side=side,
        action=action,
        qty=Decimal("1"),
        source_artifact_path="outputs/test.json",
        order_id="1",
        client_id="111",
        perm_id=perm_id,
        exec_id=exec_id,
        price=Decimal("100.0"),
        metadata={"managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"},
    )


def _broker_position(*, symbol: str, local_symbol: str, con_id: int | None, quantity: str) -> dict:
    row = {
        "account_id": "DUM882026",
        "symbol": symbol,
        "track_b_root": symbol,
        "local_symbol": local_symbol,
        "expiry": "202606",
        "quantity": quantity,
    }
    if con_id is not None:
        row["con_id"] = con_id
    return row


def _lifecycle_position(*, symbol: str, local_symbol: str, con_id: int, side: str) -> dict:
    return {
        "account_id": "DUM882026",
        "instrument_family": symbol,
        "track_b_root": symbol,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "quantity": "1",
        "aggregate_qty": "-1" if side == "SHORT" else "1",
        "side": side,
        "trade_id": f"trade_{symbol}",
        "lifecycle_id": f"life_{symbol}",
    }
