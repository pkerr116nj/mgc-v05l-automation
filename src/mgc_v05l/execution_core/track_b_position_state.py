"""Pure current-scope Track B PositionState report.

This module normalizes already-published broker/reconciliation truth into a
small position-state contract for later exit-management layers. It never
connects to a broker, submits, cancels, closes, starts services, restarts
runtime, or mutates lifecycle state.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.models import JsonSerializable, TrackBModelError, require_aware_datetime, to_jsonable
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_exit_authority_contract import (
    AttributionStatus,
    ExecutionDomain,
    PositionSide,
    SourceArtifactRef,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
POSITION_STATE_SCHEMA_VERSION = "track_b_position_state_v1"
POSITION_STATE_REPORT_SCHEMA_VERSION = "track_b_position_state_report_v1"

DEFAULT_POSITION_STATE_REPORT = (
    Path("outputs") / "track_b_execution_core" / "position_state" / "latest_position_state.json"
)
DEFAULT_RECONCILIATION_REPORT_PATH = (
    Path("outputs") / "reports" / "track_b_paper_broker_reconciliation" / "latest_track_b_paper_broker_reconciliation.json"
)
DEFAULT_MANAGED_POSITION_REGISTRY_PATH = (
    Path("outputs") / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json"
)


class PositionStateReportClassification(str, Enum):
    FLAT = "POSITION_STATE_FLAT"
    CURRENT_POSITIONS = "POSITION_STATE_CURRENT_POSITIONS"
    BLOCKED_WRONG_SCOPE = "POSITION_STATE_BLOCKED_WRONG_SCOPE"
    SOURCE_MISSING = "POSITION_STATE_SOURCE_MISSING"


@dataclass(frozen=True)
class TrackBPositionState(JsonSerializable):
    execution_domain: ExecutionDomain | str
    account_id: str
    con_id: int
    local_symbol: str
    instrument: str
    side: PositionSide | str
    qty: Decimal | int | str
    owned_qty: Decimal | int | str | None
    attribution_status: AttributionStatus | str
    current_scope: bool
    source_artifact_refs: tuple[SourceArtifactRef | Mapping[str, Any], ...]
    lifecycle_id: str | None = None
    trade_id: str | None = None
    strategy_id: str | None = None
    lane_id: str | None = None
    diagnostic_rows: tuple[Mapping[str, Any], ...] = ()
    schema_version: str = POSITION_STATE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "execution_domain", _normalize_execution_domain(self.execution_domain))
        object.__setattr__(self, "account_id", _required_text(self.account_id, "account_id"))
        con_id = int(self.con_id)
        if con_id <= 0:
            raise TrackBModelError("con_id must be positive.")
        object.__setattr__(self, "con_id", con_id)
        object.__setattr__(self, "local_symbol", _required_text(self.local_symbol, "local_symbol").upper())
        object.__setattr__(self, "instrument", _required_text(self.instrument, "instrument").upper())
        object.__setattr__(self, "side", _normalize_side(self.side))
        object.__setattr__(self, "qty", _positive_decimal(self.qty, "qty"))
        if self.owned_qty is not None:
            object.__setattr__(self, "owned_qty", _positive_decimal(self.owned_qty, "owned_qty"))
        object.__setattr__(self, "attribution_status", _normalize_attribution_status(self.attribution_status))
        object.__setattr__(self, "current_scope", bool(self.current_scope))
        object.__setattr__(self, "lifecycle_id", _optional_text(self.lifecycle_id))
        object.__setattr__(self, "trade_id", _optional_text(self.trade_id))
        object.__setattr__(self, "strategy_id", _optional_text(self.strategy_id))
        object.__setattr__(self, "lane_id", _optional_text(self.lane_id))
        object.__setattr__(
            self,
            "source_artifact_refs",
            tuple(_normalize_source_ref(row) for row in self.source_artifact_refs),
        )
        object.__setattr__(self, "diagnostic_rows", tuple(dict(row) for row in self.diagnostic_rows))


@dataclass(frozen=True)
class TrackBPositionStateReportConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_POSITION_STATE_REPORT
    reconciliation_path: Path = DEFAULT_RECONCILIATION_REPORT_PATH
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_PATH
    execution_domain: ExecutionDomain | str = ExecutionDomain.TRACK_B_PAPER
    account_id: str = "DUM882026"

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_position_state_report(
    *,
    config: TrackBPositionStateReportConfig,
    now: datetime | None = None,
    input_overrides: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    actual_now = require_aware_datetime(now or datetime.now(UTC), "now")
    inputs = _inputs(config=config, overrides=input_overrides or {})
    source_refs = _source_artifact_refs(config=config, inputs=inputs)
    diagnostics = _historical_diagnostics(inputs["managed_positions"])
    positions: list[TrackBPositionState] = []
    wrong_scope_rows: list[dict[str, Any]] = []
    for broker_position in _current_broker_positions(inputs["reconciliation"]):
        if not _position_in_scope(broker_position=broker_position, config=config):
            wrong_scope_rows.append(
                {
                    "reason": "wrong_account_or_domain",
                    "account_id": broker_position.get("account_id") or broker_position.get("account"),
                    "local_symbol": broker_position.get("local_symbol"),
                    "con_id": broker_position.get("con_id"),
                }
            )
            continue
        managed_match = _matching_current_managed_position(
            broker_position=broker_position,
            managed_positions=inputs["managed_positions"],
            account_id=config.account_id,
        )
        positions.append(
            _position_state(
                broker_position=broker_position,
                managed_match=managed_match,
                config=config,
                source_refs=source_refs,
                diagnostics=diagnostics,
            )
        )
    current_positions = [] if wrong_scope_rows else positions
    classification = _classification(positions=current_positions, wrong_scope_rows=wrong_scope_rows, inputs=inputs)
    return {
        "schema_version": POSITION_STATE_REPORT_SCHEMA_VERSION,
        "generated_at": actual_now.isoformat(),
        "read_only": True,
        "broker_state_mutated": False,
        "submit_attempted": False,
        "cancel_attempted": False,
        "close_attempted": False,
        "service_started": False,
        "runtime_restarted": False,
        "live_money_eligible": _flag_true(inputs, "live_money_eligible"),
        "paper_proof_invoked": _flag_true(inputs, "paper_proof_invoked"),
        "execution_domain": _normalize_execution_domain(config.execution_domain).value,
        "account_id": config.account_id,
        "classification": classification.value,
        "position_count": len(current_positions),
        "positions": [position.to_json_dict() for position in current_positions],
        "diagnostic_rows": diagnostics + wrong_scope_rows,
        "wrong_scope_row_count": len(wrong_scope_rows),
        "source_classifications": {
            "reconciliation": inputs["reconciliation"].get("classification"),
            "managed_positions": inputs["managed_positions"].get("classification"),
        },
        "source_artifact_paths": {
            "reconciliation": str(config.resolve(config.reconciliation_path)),
            "managed_positions": str(config.resolve(config.managed_position_registry_path)),
        },
    }


def run_track_b_position_state_report(
    *,
    config: TrackBPositionStateReportConfig,
    now: datetime | None = None,
    write: bool = True,
) -> dict[str, Any]:
    payload = build_track_b_position_state_report(config=config, now=now)
    if write:
        write_track_b_position_state_report(config=config, payload=payload)
    return payload


def write_track_b_position_state_report(*, config: TrackBPositionStateReportConfig, payload: Mapping[str, Any]) -> Path:
    return write_json_atomic(config.resolve(config.output_path), to_jsonable(dict(payload)))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="track-b-position-state")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_POSITION_STATE_REPORT)
    parser.add_argument("--reconciliation-path", type=Path, default=DEFAULT_RECONCILIATION_REPORT_PATH)
    parser.add_argument("--managed-position-registry-path", type=Path, default=DEFAULT_MANAGED_POSITION_REGISTRY_PATH)
    parser.add_argument("--account-id", default="DUM882026")
    parser.add_argument("--execution-domain", default=ExecutionDomain.TRACK_B_PAPER.value)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-write", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBPositionStateReportConfig(
        repo_root=args.repo_root,
        output_path=args.output_path,
        reconciliation_path=args.reconciliation_path,
        managed_position_registry_path=args.managed_position_registry_path,
        account_id=args.account_id,
        execution_domain=args.execution_domain,
    )
    payload = run_track_b_position_state_report(config=config, write=not args.no_write)
    if args.json or args.no_write:
        print(json.dumps(to_jsonable(payload), indent=2, sort_keys=True))
    else:
        print(
            f"{payload['classification']} positions={payload['position_count']} "
            f"diagnostics={len(payload['diagnostic_rows'])}"
        )
    return 0


def _inputs(
    *,
    config: TrackBPositionStateReportConfig,
    overrides: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {
        "reconciliation": dict(
            overrides.get("reconciliation")
            or _read_json(config.resolve(config.reconciliation_path))
        ),
        "managed_positions": dict(
            overrides.get("managed_positions")
            or _read_json(config.resolve(config.managed_position_registry_path))
        ),
    }


def _source_artifact_refs(
    *,
    config: TrackBPositionStateReportConfig,
    inputs: Mapping[str, Mapping[str, Any]],
) -> tuple[SourceArtifactRef, ...]:
    return (
        SourceArtifactRef(
            name="reconciliation",
            path=str(config.resolve(config.reconciliation_path)),
            generated_at=_parse_dt(inputs["reconciliation"].get("generated_at")),
            authority_layer="PositionState",
        ),
        SourceArtifactRef(
            name="managed_positions",
            path=str(config.resolve(config.managed_position_registry_path)),
            generated_at=_parse_dt(inputs["managed_positions"].get("generated_at")),
            authority_layer="PositionState",
        ),
    )


def _current_broker_positions(reconciliation: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        row
        for row in (_mapping(item) for item in _list(reconciliation.get("track_b_broker_positions")))
        if abs(_decimal(row.get("quantity") or row.get("position") or row.get("signed_qty"))) > Decimal("0")
    ]


def _position_state(
    *,
    broker_position: Mapping[str, Any],
    managed_match: Mapping[str, Any] | None,
    config: TrackBPositionStateReportConfig,
    source_refs: tuple[SourceArtifactRef, ...],
    diagnostics: list[dict[str, Any]],
) -> TrackBPositionState:
    broker_position = _enriched_broker_position_identity(
        broker_position=broker_position,
        managed_match=managed_match,
        diagnostics=diagnostics,
    )
    qty = abs(_decimal(broker_position.get("quantity") or broker_position.get("position") or broker_position.get("signed_qty")))
    side = PositionSide.LONG if _decimal(broker_position.get("quantity") or broker_position.get("position") or broker_position.get("signed_qty")) > 0 else PositionSide.SHORT
    match = _mapping(managed_match)
    attribution = _attribution_status(match)
    return TrackBPositionState(
        execution_domain=config.execution_domain,
        account_id=str(broker_position.get("account_id") or broker_position.get("account") or config.account_id),
        con_id=_int(broker_position.get("con_id")),
        local_symbol=str(broker_position.get("local_symbol") or ""),
        instrument=str(broker_position.get("track_b_root") or broker_position.get("symbol") or ""),
        side=side,
        qty=qty,
        owned_qty=qty if attribution != AttributionStatus.UNATTRIBUTED else None,
        attribution_status=attribution,
        lifecycle_id=_text_or_none(match.get("lifecycle_id")),
        trade_id=_text_or_none(match.get("trade_id")),
        strategy_id=_text_or_none(match.get("strategy_id")),
        lane_id=_text_or_none(match.get("lane_id")),
        current_scope=True,
        source_artifact_refs=source_refs,
        diagnostic_rows=tuple(diagnostics),
    )


def _enriched_broker_position_identity(
    *,
    broker_position: Mapping[str, Any],
    managed_match: Mapping[str, Any] | None,
    diagnostics: list[dict[str, Any]],
) -> dict[str, Any]:
    enriched = dict(broker_position)
    match = _mapping(managed_match)
    broker_nested = _mapping(match.get("broker_position"))
    identity = _contract_identity_from_managed_match(match)
    enriched_fields: list[str] = []
    for field, aliases in {
        "con_id": ("con_id", "conId"),
        "local_symbol": ("local_symbol", "localSymbol"),
        "expiry": ("expiry",),
        "symbol": ("symbol", "instrument", "track_b_root"),
        "track_b_root": ("track_b_root", "instrument", "symbol"),
    }.items():
        current = enriched.get(field)
        if field == "con_id":
            missing = _int(current) <= 0
        else:
            missing = not str(current or "").strip()
        if not missing:
            continue
        replacement = next(
            (
                identity.get(alias)
                for alias in aliases
                if str(identity.get(alias) or "").strip()
            ),
            None,
        )
        if replacement is None and broker_nested:
            replacement = next(
                (
                    broker_nested.get(alias)
                    for alias in aliases
                    if str(broker_nested.get(alias) or "").strip()
                ),
                None,
            )
        if replacement is None:
            continue
        enriched[field] = replacement
        enriched_fields.append(field)
    if enriched_fields:
        diagnostics.append(
            {
                "source": "managed_positions",
                "kind": "contract_identity_enrichment",
                "fields": enriched_fields,
                "lifecycle_id": match.get("lifecycle_id"),
                "trade_id": match.get("trade_id"),
                "local_symbol": enriched.get("local_symbol"),
                "con_id": enriched.get("con_id"),
            }
        )
    return enriched


def _contract_identity_from_managed_match(row: Mapping[str, Any]) -> dict[str, Any]:
    broker_nested = _mapping(row.get("broker_position"))
    lifecycle_nested = _mapping(row.get("lifecycle_position"))
    manifest_nested = _mapping(row.get("position_management_manifest") or row.get("manifest"))
    lifecycle_units = [_mapping(item) for item in _list(row.get("lifecycle_units"))]
    sources = [row, broker_nested, lifecycle_nested, manifest_nested, *lifecycle_units]
    identity: dict[str, Any] = {}
    for field in ("con_id", "conId", "local_symbol", "localSymbol", "expiry", "symbol", "instrument", "track_b_root"):
        for source in sources:
            value = source.get(field)
            if str(value or "").strip():
                identity[field] = value
                break
    return identity


def _matching_current_managed_position(
    *,
    broker_position: Mapping[str, Any],
    managed_positions: Mapping[str, Any],
    account_id: str,
) -> dict[str, Any] | None:
    for row in (_mapping(item) for item in _list(managed_positions.get("managed_positions"))):
        if _row_is_diagnostic_only(row):
            continue
        if not _managed_row_matches_broker(row=row, broker_position=broker_position, account_id=account_id):
            continue
        return row
    return None


def _managed_row_matches_broker(*, row: Mapping[str, Any], broker_position: Mapping[str, Any], account_id: str) -> bool:
    broker_nested = _mapping(row.get("broker_position"))
    row_account = _text_or_none(broker_nested.get("account_id") or row.get("account_id")) or account_id
    if row_account != str(broker_position.get("account_id") or broker_position.get("account") or ""):
        return False
    row_con_id = _int(broker_nested.get("con_id") or row.get("con_id"))
    broker_con_id = _int(broker_position.get("con_id"))
    if row_con_id > 0 and broker_con_id > 0 and row_con_id != broker_con_id:
        return False
    row_symbol = str(broker_nested.get("local_symbol") or row.get("local_symbol") or "").upper()
    broker_symbol = str(broker_position.get("local_symbol") or "").upper()
    if row_symbol and broker_symbol and row_symbol != broker_symbol:
        return False
    broker_qty = _decimal(broker_position.get("quantity") or broker_position.get("position") or broker_position.get("signed_qty"))
    row_qty = _decimal(broker_nested.get("quantity") or row.get("signed_qty") or row.get("quantity"))
    row_side = str(row.get("side") or "").upper()
    if row_side == "SHORT" and row_qty > 0:
        row_qty = -row_qty
    if row_side == "LONG" and row_qty < 0:
        row_qty = abs(row_qty)
    return abs(row_qty) == abs(broker_qty) and (row_qty == broker_qty or bool(row_side))


def _historical_diagnostics(managed_positions: Mapping[str, Any]) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    for key in ("managed_positions", "historical_review_positions", "diagnostic_rows", "historical_debris"):
        for row in (_mapping(item) for item in _list(managed_positions.get(key))):
            if _row_is_diagnostic_only(row):
                diagnostics.append({"source": "managed_positions", "kind": key, "row": row})
    return diagnostics


def _row_is_diagnostic_only(row: Mapping[str, Any]) -> bool:
    if row.get("historical_only") is True or row.get("diagnostic_only") is True:
        return True
    scope = str(row.get("current_hot_path_scope") or row.get("scope") or "").upper()
    if "HISTORICAL" in scope or "FULL_AUDIT_ONLY" in scope or "DIAGNOSTIC" in scope:
        return True
    classification = str(row.get("classification") or "").upper()
    return "HISTORICAL" in classification or "STALE_DERIVED" in classification


def _position_in_scope(*, broker_position: Mapping[str, Any], config: TrackBPositionStateReportConfig) -> bool:
    if _normalize_execution_domain(config.execution_domain) != ExecutionDomain.TRACK_B_PAPER:
        return False
    account = str(broker_position.get("account_id") or broker_position.get("account") or "")
    return account == config.account_id


def _classification(
    *,
    positions: list[TrackBPositionState],
    wrong_scope_rows: list[dict[str, Any]],
    inputs: Mapping[str, Mapping[str, Any]],
) -> PositionStateReportClassification:
    if wrong_scope_rows:
        return PositionStateReportClassification.BLOCKED_WRONG_SCOPE
    if not inputs["reconciliation"]:
        return PositionStateReportClassification.SOURCE_MISSING
    if positions:
        return PositionStateReportClassification.CURRENT_POSITIONS
    return PositionStateReportClassification.FLAT


def _attribution_status(row: Mapping[str, Any]) -> AttributionStatus:
    fields = (
        _text_or_none(row.get("lifecycle_id")),
        _text_or_none(row.get("trade_id")),
        _text_or_none(row.get("strategy_id")),
        _text_or_none(row.get("lane_id")),
    )
    present = sum(1 for item in fields if item)
    if present == len(fields):
        return AttributionStatus.ATTRIBUTED
    if present:
        return AttributionStatus.PARTIALLY_ATTRIBUTED
    return AttributionStatus.UNATTRIBUTED


def _normalize_execution_domain(value: ExecutionDomain | str) -> ExecutionDomain:
    try:
        return value if isinstance(value, ExecutionDomain) else ExecutionDomain(str(value))
    except ValueError as exc:
        raise TrackBModelError("execution_domain is not valid.") from exc


def _normalize_attribution_status(value: AttributionStatus | str) -> AttributionStatus:
    try:
        return value if isinstance(value, AttributionStatus) else AttributionStatus(str(value))
    except ValueError as exc:
        raise TrackBModelError("attribution_status is not valid.") from exc


def _normalize_side(value: PositionSide | str) -> PositionSide:
    try:
        return value if isinstance(value, PositionSide) else PositionSide(str(value).upper())
    except ValueError as exc:
        raise TrackBModelError("side is not valid.") from exc


def _normalize_source_ref(value: SourceArtifactRef | Mapping[str, Any]) -> SourceArtifactRef:
    if isinstance(value, SourceArtifactRef):
        return value
    return SourceArtifactRef(**dict(value))


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _flag_true(inputs: Mapping[str, Mapping[str, Any]], key: str) -> bool:
    return any(payload.get(key) is True for payload in inputs.values())


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _positive_decimal(value: Any, field_name: str) -> Decimal:
    normalized = _decimal(value)
    if normalized <= 0:
        raise TrackBModelError(f"{field_name} must be positive.")
    return normalized


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _required_text(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise TrackBModelError(f"{field_name} is required.")
    return text


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _text_or_none(value: Any) -> str | None:
    return _optional_text(value)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
