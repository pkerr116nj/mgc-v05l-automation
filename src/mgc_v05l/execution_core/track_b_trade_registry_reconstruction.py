"""Read-only reconstruction of Track B trade registry records from artifacts.

This adapter intentionally does not import broker adapters, runtime starters, or
submit/cancel/close helpers. It reads existing Track B artifacts, reconstructs
best-effort append-only :class:`TradeEvent` chains, and fails closed into
``REVIEW_REQUIRED`` whenever ownership or broker-backed evidence is ambiguous.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from mgc_v05l.execution_core.track_b_central_trade_registry import (
    TradeCurrentState,
    TradeEvent,
    TradeEventType,
    TradeRegistryRecord,
    generate_trade_id,
    reduce_trade_events,
)
from mgc_v05l.execution_core.track_b_managed_order_registry import (
    DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT,
    DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT,
)
from mgc_v05l.execution_core.track_b_paper_broker_reconciliation import (
    ReconciliationConfig,
)
from mgc_v05l.execution_core.track_b_submit_intent_ownership import (
    DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_JSONL,
)


DEFAULT_STRATEGY_BRIDGE_REPORT = (
    Path("outputs") / "track_b_execution_core" / "strategy_bridge" / "latest_strategy_bridge_submit_report.json"
)
DEFAULT_FILLED_BRIDGE_RESULT = (
    Path("outputs") / "track_b_execution_core" / "strategy_bridge" / "latest_filled_bridge_result.json"
)
DEFAULT_BROKER_TRUTH_STATUS = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json"
)
DEFAULT_BROKER_POSITIONS = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_positions_snapshot.json"
)
DEFAULT_BROKER_OPEN_ORDERS = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_open_orders_snapshot.json"
)
DEFAULT_RECONSTRUCTION_REPORT = (
    Path("outputs") / "track_b_execution_core" / "trade_registry" / "latest_reconstruction_report.json"
)

SCHEMA_VERSION = "track_b_trade_registry_reconstruction_v1"

AMBIGUOUS_TRADE_ID = "AMBIGUOUS_TRADE_ID"
REDUCER_REVIEW_REQUIRED = "REDUCER_REVIEW_REQUIRED"
SUBMIT_INTENT_NO_BROKER_EFFECT_TIMEOUT = "SUBMIT_INTENT_NO_BROKER_EFFECT_TIMEOUT"
SUBMIT_INTENT_NO_BROKER_EFFECT_CONFIRMED = "SUBMIT_INTENT_NO_BROKER_EFFECT_CONFIRMED"


@dataclass(frozen=True)
class TradeRegistryReconstructionConfig:
    repo_root: Path
    submit_intent_ownership_path: Path = DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_JSONL
    bridge_report_paths: tuple[Path, ...] = field(default_factory=lambda: (DEFAULT_STRATEGY_BRIDGE_REPORT,))
    filled_bridge_result_paths: tuple[Path, ...] = field(default_factory=lambda: (DEFAULT_FILLED_BRIDGE_RESULT,))
    lifecycle_ledger_paths: tuple[Path, ...] = ()
    broker_truth_status_path: Path = DEFAULT_BROKER_TRUTH_STATUS
    broker_positions_path: Path = DEFAULT_BROKER_POSITIONS
    broker_open_orders_path: Path = DEFAULT_BROKER_OPEN_ORDERS
    reconciliation_path: Path = ReconciliationConfig().report_path
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    output_path: Path = DEFAULT_RECONSTRUCTION_REPORT

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path

    @property
    def source_paths(self) -> tuple[Path, ...]:
        return (
            self.submit_intent_ownership_path,
            *self.bridge_report_paths,
            *self.filled_bridge_result_paths,
            *self.lifecycle_ledger_paths,
            self.broker_truth_status_path,
            self.broker_positions_path,
            self.broker_open_orders_path,
            self.reconciliation_path,
            self.managed_position_registry_path,
            self.managed_order_registry_path,
        )


@dataclass(frozen=True)
class TradeRegistryReconstructionReport:
    schema_version: str
    generated_at: datetime
    read_only: bool
    broker_mutation_allowed: bool
    runtime_restart_allowed: bool
    records: tuple[TradeRegistryRecord, ...]
    missing_links: tuple[dict[str, Any], ...]
    ambiguity_reason_codes: tuple[str, ...]
    source_paths: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at.isoformat(),
            "read_only": self.read_only,
            "broker_mutation_allowed": self.broker_mutation_allowed,
            "runtime_restart_allowed": self.runtime_restart_allowed,
            "records": [record.to_dict() for record in self.records],
            "missing_links": list(self.missing_links),
            "ambiguity_reason_codes": list(self.ambiguity_reason_codes),
            "source_paths": list(self.source_paths),
            "summary": {
                "record_count": len(self.records),
                "review_required_count": sum(
                    1 for record in self.records if record.current_state == TradeCurrentState.REVIEW_REQUIRED
                ),
                "closed_flat_count": sum(
                    1 for record in self.records if record.current_state == TradeCurrentState.CLOSED_FLAT
                ),
                "cancelled_count": sum(
                    1 for record in self.records if record.current_state == TradeCurrentState.CANCELLED
                ),
            },
        }


def reconstruct_trade_registry_from_artifacts(
    *,
    config: TradeRegistryReconstructionConfig,
    now: datetime | None = None,
) -> TradeRegistryReconstructionReport:
    generated_at = _ensure_utc(now or datetime.now(UTC))
    sources = _load_sources(config)
    events = _collect_events(sources=sources, generated_at=generated_at)
    grouped = _group_events(events)

    records: list[TradeRegistryRecord] = []
    missing_links: list[dict[str, Any]] = []
    ambiguity_codes: list[str] = []
    for trade_id, chain in sorted(grouped.items()):
        try:
            record = reduce_trade_events(chain)
        except Exception as exc:  # pragma: no cover - defensive guard for malformed historical artifacts.
            fallback = _fallback_review_event(trade_id=trade_id, source_events=chain, generated_at=generated_at, reason=str(exc))
            record = reduce_trade_events([fallback])
            ambiguity_codes.append(REDUCER_REVIEW_REQUIRED)
        records.append(record)
        if record.current_state == TradeCurrentState.REVIEW_REQUIRED:
            missing_links.append(
                {
                    "trade_id": record.trade_id,
                    "reason_codes": list(record.latest_reason_codes),
                    "event_types": [event.event_type.value for event in record.event_chain],
                }
            )
            ambiguity_codes.extend(record.latest_reason_codes)

    return TradeRegistryReconstructionReport(
        schema_version=SCHEMA_VERSION,
        generated_at=generated_at,
        read_only=True,
        broker_mutation_allowed=False,
        runtime_restart_allowed=False,
        records=tuple(records),
        missing_links=tuple(missing_links),
        ambiguity_reason_codes=tuple(dict.fromkeys(ambiguity_codes)),
        source_paths=tuple(str(config.resolve(path)) for path in config.source_paths),
    )


def write_trade_registry_reconstruction_report(
    *,
    config: TradeRegistryReconstructionConfig,
    report: TradeRegistryReconstructionReport,
) -> Path:
    path = config.resolve(config.output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _load_sources(config: TradeRegistryReconstructionConfig) -> dict[str, tuple[Path, Any]]:
    sources: dict[str, tuple[Path, Any]] = {}
    sources["submit_intent_ownership"] = (
        config.resolve(config.submit_intent_ownership_path),
        _read_jsonl(config.resolve(config.submit_intent_ownership_path)),
    )
    for index, path in enumerate(config.bridge_report_paths):
        sources[f"bridge_report_{index}"] = (config.resolve(path), _read_json(config.resolve(path)))
    for index, path in enumerate(config.filled_bridge_result_paths):
        sources[f"filled_bridge_result_{index}"] = (config.resolve(path), _read_json(config.resolve(path)))
    for index, path in enumerate(config.lifecycle_ledger_paths):
        sources[f"lifecycle_ledger_{index}"] = (config.resolve(path), _read_json_or_jsonl(config.resolve(path)))
    sources["broker_truth_status"] = (config.resolve(config.broker_truth_status_path), _read_json(config.resolve(config.broker_truth_status_path)))
    sources["broker_positions"] = (config.resolve(config.broker_positions_path), _read_json(config.resolve(config.broker_positions_path)))
    sources["broker_open_orders"] = (config.resolve(config.broker_open_orders_path), _read_json(config.resolve(config.broker_open_orders_path)))
    sources["reconciliation"] = (config.resolve(config.reconciliation_path), _read_json(config.resolve(config.reconciliation_path)))
    sources["managed_positions"] = (config.resolve(config.managed_position_registry_path), _read_json(config.resolve(config.managed_position_registry_path)))
    sources["managed_orders"] = (config.resolve(config.managed_order_registry_path), _read_json(config.resolve(config.managed_order_registry_path)))
    return sources


def _collect_events(*, sources: Mapping[str, tuple[Path, Any]], generated_at: datetime) -> list[TradeEvent]:
    events: list[TradeEvent] = []
    for source_name, (path, payload) in sources.items():
        for row in _candidate_rows(payload):
            source_path = str(path)
            direct = _direct_event(row=row, source_path=source_path)
            if direct is not None:
                events.append(direct)
                continue
            events.extend(_events_from_submit_intent(source_name=source_name, row=row, source_path=source_path, generated_at=generated_at))
            events.extend(_events_from_bridge_or_order_row(source_name=source_name, row=row, source_path=source_path, generated_at=generated_at))
            events.extend(_events_from_lifecycle_row(source_name=source_name, row=row, source_path=source_path, generated_at=generated_at))
            events.extend(_events_from_reconciliation_row(source_name=source_name, row=row, source_path=source_path, generated_at=generated_at))
    return sorted(events, key=lambda event: (event.generated_at, event.event_id))


def _direct_event(*, row: Mapping[str, Any], source_path: str) -> TradeEvent | None:
    event_type = str(row.get("event_type") or "").strip()
    if event_type not in {item.value for item in TradeEventType}:
        return None
    payload = dict(row)
    payload.setdefault("source_artifact_path", source_path)
    payload.setdefault("event_id", _stable_event_id(event_type, payload))
    payload.setdefault("trade_id", _trade_id_for_row(payload))
    return TradeEvent.from_dict(payload)


def _events_from_submit_intent(
    *,
    source_name: str,
    row: Mapping[str, Any],
    source_path: str,
    generated_at: datetime,
) -> list[TradeEvent]:
    if source_name != "submit_intent_ownership":
        return []
    if not (row.get("ownership_intent_id") or row.get("order_intent_id") or row.get("intent_type")):
        return []

    state = _upper(row.get("state") or row.get("classification"))
    event_type = TradeEventType.ENTRY_INTENT_CREATED
    reason_codes: tuple[str, ...] = ()
    if state in {"SUBMIT_DELEGATED", "BROKER_ORDER_WORKING"}:
        event_type = TradeEventType.ENTRY_ORDER_SUBMITTED
    elif state in {"NOT_FILLED_CANCELLED", "CANCELLED"}:
        event_type = TradeEventType.ENTRY_ORDER_CANCELLED
    elif state in {"NO_BROKER_EFFECT_CONFIRMED", SUBMIT_INTENT_NO_BROKER_EFFECT_TIMEOUT}:
        event_type = TradeEventType.REVIEW_REQUIRED
        reason_codes = (SUBMIT_INTENT_NO_BROKER_EFFECT_TIMEOUT if "TIMEOUT" in state else SUBMIT_INTENT_NO_BROKER_EFFECT_CONFIRMED,)
    elif state == "REVIEW_REQUIRED":
        event_type = TradeEventType.REVIEW_REQUIRED
        reason_codes = ("SUBMIT_INTENT_REVIEW_REQUIRED",)

    return [
        _event_from_row(
            row=row,
            source_path=source_path,
            generated_at=_row_time(row, generated_at),
            event_type=event_type,
            reason_codes=reason_codes,
            fallback_id=row.get("ownership_intent_id") or row.get("order_intent_id"),
        )
    ]


def _events_from_bridge_or_order_row(
    *,
    source_name: str,
    row: Mapping[str, Any],
    source_path: str,
    generated_at: datetime,
) -> list[TradeEvent]:
    if not ("bridge" in source_name or "filled_bridge" in source_name or "managed_orders" in source_name):
        return []

    classification = _upper(row.get("classification") or row.get("status") or row.get("order_status") or row.get("state"))
    raw_action = _upper(row.get("action") or row.get("order_action") or row.get("intent_action"))
    intent_type = _upper(row.get("intent_type") or row.get("order_intent_type") or row.get("position_effect"))
    is_exit = "CLOSE" in raw_action or "EXIT" in intent_type or "CLOSE" in intent_type or "WORKING_CLOSE" in classification

    events: list[TradeEvent] = []
    if row.get("submit_attempt_id") or row.get("order_id") or row.get("broker_order_id"):
        if "CANCEL" in classification:
            event_type = TradeEventType.EXIT_ORDER_CANCELLED if is_exit else TradeEventType.ENTRY_ORDER_CANCELLED
        elif "FILL" in classification and row.get("exec_id"):
            event_type = TradeEventType.EXIT_FILL_BROKER_BACKED if is_exit else TradeEventType.ENTRY_FILL_BROKER_BACKED
        else:
            event_type = TradeEventType.EXIT_ORDER_SUBMITTED if is_exit else TradeEventType.ENTRY_ORDER_SUBMITTED
        events.append(
            _event_from_row(
                row=row,
                source_path=source_path,
                generated_at=_row_time(row, generated_at),
                event_type=event_type,
                fallback_id=row.get("submit_attempt_id") or row.get("order_id") or row.get("broker_order_id"),
            )
        )
    if row.get("fill") and isinstance(row.get("fill"), Mapping):
        fill_row = {**row, **dict(row["fill"])}
        events.append(
            _event_from_row(
                row=fill_row,
                source_path=source_path,
                generated_at=_row_time(fill_row, generated_at),
                event_type=TradeEventType.EXIT_FILL_BROKER_BACKED if is_exit else TradeEventType.ENTRY_FILL_BROKER_BACKED,
                fallback_id=fill_row.get("exec_id") or fill_row.get("perm_id"),
            )
        )
    return events


def _events_from_lifecycle_row(
    *,
    source_name: str,
    row: Mapping[str, Any],
    source_path: str,
    generated_at: datetime,
) -> list[TradeEvent]:
    if "managed_positions" not in source_name and "lifecycle" not in source_name:
        return []
    if not (row.get("lifecycle_id") or row.get("lifecycle_state") or row.get("managed_exit_policy_id")):
        return []
    state = _upper(row.get("lifecycle_state") or row.get("state") or row.get("classification"))
    if "CLOSED" in state or "FLAT" in state:
        event_type = TradeEventType.RECONCILED_FLAT
    elif "ADOPTION" in state:
        event_type = TradeEventType.RECOVERY_ADOPTION_RECORDED
    else:
        event_type = TradeEventType.LIFECYCLE_OPEN_MANAGED
    return [
        _event_from_row(
            row=row,
            source_path=source_path,
            generated_at=_row_time(row, generated_at),
            event_type=event_type,
            fallback_id=row.get("lifecycle_id"),
        )
    ]


def _events_from_reconciliation_row(
    *,
    source_name: str,
    row: Mapping[str, Any],
    source_path: str,
    generated_at: datetime,
) -> list[TradeEvent]:
    if source_name != "reconciliation":
        return []
    if not (row.get("trade_id") or row.get("lifecycle_id")):
        return []
    classification = _upper(row.get("classification") or row.get("reconciliation_status") or row.get("state"))
    if "RECONCILED" in classification and "FLAT" in classification:
        event_type = TradeEventType.RECONCILED_FLAT
        reason_codes: tuple[str, ...] = ()
    elif "RECONCILED" in classification and "OPEN" in classification:
        event_type = TradeEventType.RECONCILED_OPEN
        reason_codes = ()
    elif "MANUAL" in classification and "CLOSE" in classification:
        event_type = TradeEventType.MANUAL_OPERATOR_CLOSE_RECORDED
        reason_codes = ("MANUAL_OPERATOR_CLOSE_RECORDED",)
    else:
        event_type = TradeEventType.REVIEW_REQUIRED
        reason_codes = (classification or "RECONCILIATION_REVIEW_REQUIRED",)
    return [
        _event_from_row(
            row=row,
            source_path=source_path,
            generated_at=_row_time(row, generated_at),
            event_type=event_type,
            reason_codes=reason_codes,
            fallback_id=row.get("lifecycle_id") or row.get("trade_id"),
        )
    ]


def _event_from_row(
    *,
    row: Mapping[str, Any],
    source_path: str,
    generated_at: datetime,
    event_type: TradeEventType,
    reason_codes: Sequence[str] = (),
    fallback_id: Any = None,
) -> TradeEvent:
    normalized = _normalize_identity(row)
    trade_id = str(row.get("trade_id") or "").strip() or _trade_id_for_row({**row, **normalized})
    reasons = tuple(
        dict.fromkeys(
            [
                *(str(item) for item in reason_codes if str(item or "").strip()),
                *(_reason_codes_from_row(row)),
            ]
        )
    )
    if trade_id.startswith("review_") and AMBIGUOUS_TRADE_ID not in reasons:
        reasons = (*reasons, AMBIGUOUS_TRADE_ID)
    return TradeEvent(
        event_id=str(row.get("event_id") or _stable_event_id(event_type.value, {**row, "fallback_id": fallback_id})),
        event_type=event_type,
        generated_at=generated_at,
        trade_id=trade_id,
        lifecycle_id=_text(row.get("lifecycle_id")),
        lane_id=normalized["lane_id"],
        thesis_strategy_id=normalized["thesis_strategy_id"],
        account_id=normalized["account_id"],
        symbol=normalized["symbol"],
        con_id=int(normalized["con_id"]),
        local_symbol=normalized["local_symbol"],
        expiry=normalized["expiry"],
        side=normalized["side"],
        action=normalized["action"],
        qty=normalized["qty"],
        source_artifact_path=source_path,
        order_id=_text(row.get("order_id") or row.get("broker_order_id")),
        client_id=_text(row.get("client_id")),
        perm_id=_text(row.get("perm_id") or row.get("entry_perm_id") or row.get("exit_perm_id")),
        exec_id=_text(row.get("exec_id") or row.get("entry_exec_id") or row.get("exit_exec_id")),
        price=_decimal_or_none(row.get("price") or row.get("fill_price") or row.get("avg_price") or row.get("avg_cost")),
        reason_codes=reasons,
        metadata={key: value for key, value in row.items() if key not in {"event_type", "reason_codes"}},
    )


def _normalize_identity(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "lane_id": _required_text(row, "lane_id", "strategy_id", "source_lane_id"),
        "thesis_strategy_id": _required_text(row, "thesis_strategy_id", "strategy_id", "lane_id"),
        "account_id": _required_text(row, "account_id", "account", "broker_account"),
        "symbol": _required_text(row, "symbol", "root_symbol", "underlying"),
        "con_id": _int(row.get("con_id") or row.get("conId") or row.get("contract_id")),
        "local_symbol": _required_text(row, "local_symbol", "localSymbol"),
        "expiry": _required_text(row, "expiry", "lastTradeDateOrContractMonth", "contract_month"),
        "side": _required_text(row, "side", "position_side", "direction", "action"),
        "action": _required_text(row, "action", "order_action", "intent_action"),
        "qty": _decimal(row.get("qty") or row.get("quantity") or row.get("position_qty") or row.get("filled_qty") or 1),
    }


def _trade_id_for_row(row: Mapping[str, Any]) -> str:
    lifecycle_id = _text(row.get("lifecycle_id"))
    broker_id = _text(row.get("entry_perm_id") or row.get("perm_id") or row.get("entry_exec_id") or row.get("exec_id"))
    if lifecycle_id and broker_id:
        return _slug(f"trade_{lifecycle_id}_{broker_id}")
    try:
        generated_at = _row_time(row, datetime.now(UTC))
        return generate_trade_id(
            account_id=str(row.get("account_id") or row.get("account") or ""),
            con_id=int(row.get("con_id") or row.get("conId") or 0),
            lane_id=str(row.get("lane_id") or row.get("strategy_id") or ""),
            entry_action=str(row.get("action") or row.get("order_action") or "UNKNOWN"),
            generated_at=generated_at,
            entry_perm_id=broker_id,
            order_id=_text(row.get("order_id") or row.get("broker_order_id") or row.get("ownership_intent_id")),
        )
    except Exception:
        identity = _text(row.get("lifecycle_id") or row.get("ownership_intent_id") or row.get("submit_attempt_id") or row.get("event_id"))
        return _slug(f"review_{identity or 'ambiguous_trade'}")


def _group_events(events: Iterable[TradeEvent]) -> dict[str, list[TradeEvent]]:
    grouped: dict[str, list[TradeEvent]] = {}
    for event in events:
        grouped.setdefault(event.trade_id, []).append(event)
    return grouped


def _fallback_review_event(
    *,
    trade_id: str,
    source_events: Sequence[TradeEvent],
    generated_at: datetime,
    reason: str,
) -> TradeEvent:
    first = source_events[0]
    return TradeEvent(
        event_id=_stable_event_id("fallback_review", {"trade_id": trade_id, "reason": reason}),
        event_type=TradeEventType.REVIEW_REQUIRED,
        generated_at=generated_at,
        trade_id=trade_id,
        lifecycle_id=first.lifecycle_id,
        lane_id=first.lane_id,
        thesis_strategy_id=first.thesis_strategy_id,
        account_id=first.account_id,
        symbol=first.symbol,
        con_id=first.con_id,
        local_symbol=first.local_symbol,
        expiry=first.expiry,
        side=first.side,
        action=first.action,
        qty=first.qty,
        source_artifact_path=first.source_artifact_path,
        reason_codes=(REDUCER_REVIEW_REQUIRED, reason),
    )


def _candidate_rows(payload: Any) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, Mapping):
                rows.append(item)
        return rows
    if not isinstance(payload, Mapping):
        return []
    for key in (
        "trade_events",
        "events",
        "records",
        "orders",
        "order_states",
        "fills",
        "managed_positions",
        "managed_orders",
        "positions",
        "open_orders",
        "unresolved_submit_intent_ownership_records",
    ):
        for item in payload.get(key) or ():
            if isinstance(item, Mapping):
                rows.append(item)
    rows.append(payload)
    return rows


def _read_json(path: Path) -> Any:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _read_jsonl(path: Path) -> list[Mapping[str, Any]]:
    if not path.exists():
        return []
    rows: list[Mapping[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, Mapping):
            rows.append(payload)
    return rows


def _read_json_or_jsonl(path: Path) -> Any:
    if path.suffix == ".jsonl":
        return _read_jsonl(path)
    return _read_json(path)


def _reason_codes_from_row(row: Mapping[str, Any]) -> tuple[str, ...]:
    raw = row.get("reason_codes") or row.get("reason_code") or ()
    if isinstance(raw, str):
        return (raw,) if raw.strip() else ()
    if isinstance(raw, Sequence):
        return tuple(str(item) for item in raw if str(item or "").strip())
    return ()


def _row_time(row: Mapping[str, Any], fallback: datetime) -> datetime:
    for key in (
        "generated_at",
        "created_at",
        "updated_at",
        "submitted_at",
        "filled_at",
        "closed_at",
        "entry_fill_time",
    ):
        value = row.get(key)
        if value:
            try:
                return _ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
            except ValueError:
                continue
    return _ensure_utc(fallback)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _stable_event_id(prefix: str, payload: Mapping[str, Any]) -> str:
    raw = json.dumps(_jsonable(payload), sort_keys=True, default=str)
    return _slug(f"{prefix}_{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:16]}")


def _required_text(row: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = _text(row.get(key))
        if value:
            return value
    return "UNKNOWN"


def _text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _upper(value: Any) -> str:
    return str(value or "").strip().upper()


def _int(value: Any) -> int:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        parsed = 0
    return parsed if parsed > 0 else 1


def _decimal(value: Any) -> Decimal:
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("1")


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_") or "unknown"


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(inner) for inner in value]
    return value
