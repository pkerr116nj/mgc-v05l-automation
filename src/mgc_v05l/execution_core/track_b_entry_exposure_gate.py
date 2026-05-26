"""Pre-submit entry exposure gate for Track B PAPER strategy entries.

The managed position registry is the authority for strategy/lane owned PAPER
exposure.  This gate runs before entry submit authorization can reach the
broker adapter so repeated same-lane entries require an explicit pyramiding
policy instead of being discovered after the position is already open.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence


ENTRY_EXPOSURE_GATE_ALLOWED = "ENTRY_EXPOSURE_GATE_ALLOWED"
SAME_LANE_REENTRY_BLOCKED_PYRAMIDING_NOT_ALLOWED = (
    "SAME_LANE_REENTRY_BLOCKED_PYRAMIDING_NOT_ALLOWED"
)
SAME_LANE_REENTRY_BLOCKED_MAX_UNITS_PER_LANE = "SAME_LANE_REENTRY_BLOCKED_MAX_UNITS_PER_LANE"
OPPOSITE_SIDE_EXPOSURE_BLOCKED = "OPPOSITE_SIDE_EXPOSURE_BLOCKED"
INSTRUMENT_EXPOSURE_CAP_BLOCKED = "INSTRUMENT_EXPOSURE_CAP_BLOCKED"
ENTRY_EXPOSURE_BLOCKED_BROKER_LIFECYCLE_MISMATCH = (
    "ENTRY_EXPOSURE_BLOCKED_BROKER_LIFECYCLE_MISMATCH"
)
ENTRY_EXPOSURE_BLOCKED_LIVE_MONEY = "ENTRY_EXPOSURE_BLOCKED_LIVE_MONEY"
ENTRY_EXPOSURE_BLOCKED_PAPER_PROOF = "ENTRY_EXPOSURE_BLOCKED_PAPER_PROOF"

PYRAMIDING_ALLOWED = "PYRAMIDING_ALLOWED"
PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED = "PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED"
MANAGED_POSITION_REGISTRY_PATH = Path(
    "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"
)

DEFAULT_MAX_PAPER_EXPOSURE_BY_INSTRUMENT: Mapping[str, int] = {
    "MNQ": 5,
    "NQ": 1,
    "MES": 5,
    "ES": 1,
    "YM": 1,
    "MYM": 5,
    "MGC": 5,
    "GC": 2,
    "PL": 2,
}


@dataclass(frozen=True)
class TrackBEntryExposureGateConfig:
    repo_root: Path
    account_id: str
    strategy_id: str
    lane_id: str | None
    instrument_family: str
    contract_key: str | None
    local_symbol: str | None
    con_id: int | str | None
    side: str
    quantity: int | float | str | Decimal = 1
    pyramiding_policy: str = PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED
    max_units_per_lane: int = 1
    max_paper_exposure_by_instrument: Mapping[str, int] = field(
        default_factory=lambda: dict(DEFAULT_MAX_PAPER_EXPOSURE_BY_INSTRUMENT)
    )
    managed_position_registry_path: Path = MANAGED_POSITION_REGISTRY_PATH

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def evaluate_track_b_entry_exposure_gate(config: TrackBEntryExposureGateConfig) -> dict[str, Any]:
    registry_path = config.resolve(Path(config.managed_position_registry_path))
    registry = _read_json(registry_path)
    active_units = _active_lifecycle_units(registry)
    requested_side = _normal_side(config.side)
    requested_qty = _positive_decimal(config.quantity)
    instrument = _text(config.instrument_family).upper()
    max_exposure = int(config.max_paper_exposure_by_instrument.get(instrument, 5))
    matching_contract_units = [
        unit for unit in active_units if _same_account(unit, config) and _same_contract(unit, config)
    ]
    same_lane_units = [
        unit
        for unit in matching_contract_units
        if _same_strategy_lane(unit, config)
        and _normal_side(unit.get("side")) == requested_side
    ]
    opposite_side_units = [
        unit
        for unit in matching_contract_units
        if _normal_side(unit.get("side")) in {"LONG", "SHORT"}
        and requested_side in {"LONG", "SHORT"}
        and _normal_side(unit.get("side")) != requested_side
    ]
    family_units = [
        unit
        for unit in active_units
        if _same_account(unit, config)
        and _text(unit.get("instrument_family") or unit.get("track_b_root") or unit.get("symbol")).upper()
        == instrument
    ]
    current_lane_units = len(same_lane_units)
    signed_family_exposure = sum((_signed_unit_qty(unit) for unit in family_units), Decimal("0"))
    projected_abs_exposure = abs(signed_family_exposure + _signed_qty(requested_side, requested_qty))
    registry_mismatch = _registry_mismatch(registry, matching_contract_units)

    base = {
        "schema_version": "track_b_entry_exposure_gate_v1",
        "classification": ENTRY_EXPOSURE_GATE_ALLOWED,
        "allowed": True,
        "entry_block_reason": None,
        "source": "MANAGED_POSITION_REGISTRY_AUTHORITY_ARTIFACT",
        "managed_position_registry_path": str(registry_path),
        "dashboard_projection_consumed": False,
        "paper_only": True,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "account_id": config.account_id,
        "strategy_id": config.strategy_id,
        "lane_id": config.lane_id,
        "instrument_family": instrument,
        "contract_key": config.contract_key,
        "local_symbol": config.local_symbol,
        "con_id": None if config.con_id is None else str(config.con_id),
        "side": requested_side,
        "quantity": _decimal_text(requested_qty),
        "pyramiding_policy": config.pyramiding_policy,
        "max_units_per_lane": int(config.max_units_per_lane),
        "current_units_for_lane": current_lane_units,
        "instrument_family_exposure": _decimal_text(signed_family_exposure),
        "max_paper_exposure_for_instrument": max_exposure,
        "projected_abs_instrument_family_exposure": _decimal_text(projected_abs_exposure),
        "same_lane_open_unit_count": len(same_lane_units),
        "opposite_side_open_unit_count": len(opposite_side_units),
        "same_lane_units": [_unit_summary(unit) for unit in same_lane_units],
        "opposite_side_units": [_unit_summary(unit) for unit in opposite_side_units],
        "active_lifecycle_unit_count": len(active_units),
    }
    if registry.get("live_money_eligible") is True:
        return _blocked(base, ENTRY_EXPOSURE_BLOCKED_LIVE_MONEY, "live_money_eligible=true")
    if registry.get("paper_proof_invoked") is True:
        return _blocked(base, ENTRY_EXPOSURE_BLOCKED_PAPER_PROOF, "paper_proof_invoked=true")
    if registry_mismatch:
        return _blocked(
            base,
            ENTRY_EXPOSURE_BLOCKED_BROKER_LIFECYCLE_MISMATCH,
            "Managed position registry reports broker/lifecycle quantity mismatch.",
            extra={"mismatch_units": [_unit_summary(unit) for unit in registry_mismatch]},
        )
    if opposite_side_units:
        return _blocked(
            base,
            OPPOSITE_SIDE_EXPOSURE_BLOCKED,
            "Opposite-side exposure exists for the same account/contract and no hedging policy is configured.",
        )
    if same_lane_units and str(config.pyramiding_policy or "").upper() != PYRAMIDING_ALLOWED:
        return _blocked(
            base,
            SAME_LANE_REENTRY_BLOCKED_PYRAMIDING_NOT_ALLOWED,
            "Same strategy/lane/contract already has open exposure and pyramiding is not allowed.",
        )
    if same_lane_units and current_lane_units + int(requested_qty) > int(config.max_units_per_lane):
        return _blocked(
            base,
            SAME_LANE_REENTRY_BLOCKED_MAX_UNITS_PER_LANE,
            "Same-lane pyramiding would exceed max_units_per_lane.",
        )
    if projected_abs_exposure > Decimal(str(max_exposure)):
        return _blocked(
            base,
            INSTRUMENT_EXPOSURE_CAP_BLOCKED,
            "Projected instrument-family PAPER exposure would exceed configured cap.",
        )
    return base


def _active_lifecycle_units(registry: Mapping[str, Any]) -> list[dict[str, Any]]:
    units: list[dict[str, Any]] = []
    for position in _list(registry.get("managed_positions")):
        if not isinstance(position, Mapping):
            continue
        if not _active_position(position):
            continue
        nested = [dict(item) for item in _list(position.get("lifecycle_units")) if isinstance(item, Mapping)]
        if nested:
            for unit in nested:
                merged = {**dict(position), **unit}
                units.append(merged)
            continue
        units.append(dict(position))
    return units


def _active_position(position: Mapping[str, Any]) -> bool:
    classification = _text(position.get("classification")).upper()
    final_status = _text(position.get("final_position_status") or position.get("lifecycle_status")).upper()
    if "CLOSED" in classification or "CLOSED" in final_status:
        return False
    qty = _signed_unit_qty(position)
    return qty != 0


def _registry_mismatch(registry: Mapping[str, Any], matching_contract_units: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    mismatches: list[dict[str, Any]] = []
    for position in _list(registry.get("managed_positions")):
        if not isinstance(position, Mapping):
            continue
        if position.get("broker_qty_match") is False:
            mismatches.append(dict(position))
    for unit in matching_contract_units:
        if unit.get("broker_qty_match") is False:
            mismatches.append(dict(unit))
    return mismatches


def _same_account(unit: Mapping[str, Any], config: TrackBEntryExposureGateConfig) -> bool:
    account = _text(unit.get("account_id") or unit.get("account"))
    return not account or account == config.account_id


def _same_contract(unit: Mapping[str, Any], config: TrackBEntryExposureGateConfig) -> bool:
    unit_con_id = _text(unit.get("con_id") or unit.get("conId"))
    config_con_id = _text(config.con_id)
    if unit_con_id and config_con_id and unit_con_id == config_con_id:
        return True
    unit_local = _text(unit.get("local_symbol") or unit.get("contract") or unit.get("localSymbol")).upper()
    config_local = _text(config.local_symbol).upper()
    if unit_local and config_local and unit_local == config_local:
        return True
    unit_contract_key = _text(unit.get("contract_key") or unit.get("position_key")).upper()
    config_contract_key = _text(config.contract_key).upper()
    return bool(unit_contract_key and config_contract_key and unit_contract_key == config_contract_key)


def _same_strategy_lane(unit: Mapping[str, Any], config: TrackBEntryExposureGateConfig) -> bool:
    strategy = _norm(config.strategy_id)
    unit_strategy = _norm(unit.get("strategy_id"))
    if unit_strategy != strategy:
        return False
    requested_lane = _norm(config.lane_id or config.strategy_id)
    unit_lane = _norm(unit.get("lane_id") or unit.get("strategy_lane_id") or unit.get("strategy_id"))
    return unit_lane == requested_lane or unit_lane == strategy or requested_lane == strategy


def _unit_summary(unit: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "lifecycle_id": unit.get("lifecycle_id"),
        "entry_intent_id": unit.get("entry_intent_id"),
        "strategy_id": unit.get("strategy_id"),
        "lane_id": unit.get("lane_id"),
        "account_id": unit.get("account_id"),
        "instrument_family": unit.get("instrument_family") or unit.get("track_b_root") or unit.get("symbol"),
        "contract_key": unit.get("contract_key") or unit.get("position_key"),
        "local_symbol": unit.get("local_symbol") or unit.get("localSymbol"),
        "con_id": unit.get("con_id") or unit.get("conId"),
        "side": _normal_side(unit.get("side")),
        "signed_qty": _decimal_text(_signed_unit_qty(unit)),
        "entry_order_id": unit.get("entry_order_id"),
        "entry_perm_id": unit.get("entry_perm_id"),
        "exit_status": unit.get("exit_status"),
    }


def _blocked(base: Mapping[str, Any], classification: str, reason: str, *, extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {
        **dict(base),
        **dict(extra or {}),
        "classification": classification,
        "allowed": False,
        "entry_block_reason": reason,
    }


def _signed_unit_qty(unit: Mapping[str, Any]) -> Decimal:
    if unit.get("signed_qty") not in {None, ""}:
        return _decimal(unit.get("signed_qty"))
    if unit.get("aggregate_qty") not in {None, ""}:
        return _decimal(unit.get("aggregate_qty"))
    qty = _decimal(unit.get("quantity") or 0)
    return _signed_qty(_normal_side(unit.get("side")), qty)


def _signed_qty(side: str, quantity: Decimal) -> Decimal:
    return -quantity if side == "SHORT" else quantity


def _normal_side(value: Any) -> str:
    raw = _text(value).upper()
    if raw in {"SHORT", "SELL", "SELL_TO_OPEN"}:
        return "SHORT"
    if raw in {"LONG", "BUY", "BUY_TO_OPEN"}:
        return "LONG"
    return raw


def _positive_decimal(value: Any) -> Decimal:
    result = _decimal(value)
    if result <= 0:
        return Decimal("0")
    return result


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _norm(value: Any) -> str:
    return _text(value).strip().lower().replace("-", "_")


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}
