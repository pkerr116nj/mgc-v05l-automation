"""Append-only resolver for historical Track B reconciliation debris.

The resolver never mutates broker state and never deletes or rewrites existing
artifacts.  It appends terminal evidence only when current broker truth proves
the affected account/contract has no exposure and no open order.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_broker_fill_evidence_resolver import (
    BrokerFillEvidenceRequest,
    RESOLVED,
    resolve_broker_backed_fill_evidence,
)
from mgc_v05l.execution_core.track_b_central_trade_registry import TradeEventType
from mgc_v05l.execution_core.track_b_live_trade_registry import (
    append_live_trade_registry_event,
    load_live_trade_registry_records,
    make_live_trade_registry_event,
    trade_id_from_live_identity,
)
from mgc_v05l.execution_core.track_b_submit_intent_ownership import (
    DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_JSONL,
    DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_LATEST_JSON,
    SubmitIntentOwnershipState,
    append_submit_intent_ownership_record,
)


SCHEMA_VERSION = "track_b_historical_reconciliation_debris_resolver_v1"
RESOLVER_CLEAN = "HISTORICAL_RECONCILIATION_DEBRIS_RESOLVED"
RESOLVER_BLOCKED = "HISTORICAL_RECONCILIATION_DEBRIS_BLOCKED"
RESOLVER_NOT_APPLICABLE = "HISTORICAL_RECONCILIATION_DEBRIS_NOT_APPLICABLE"


@dataclass(frozen=True)
class HistoricalReconciliationDebrisResolverConfig:
    repo_root: Path
    submit_intent_ownership_path: Path = DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_JSONL
    latest_submit_intent_ownership_path: Path = DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_LATEST_JSON
    stale_after_seconds: float = 300.0
    apply: bool = True
    managed_positions_path: Path = (
        Path("outputs") / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json"
    )
    position_truth_path: Path = (
        Path("outputs") / "track_b_execution_core" / "position_truth" / "latest_position_truth.json"
    )

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def resolve_historical_reconciliation_debris(
    *,
    config: HistoricalReconciliationDebrisResolverConfig,
    now: datetime,
    broker_positions: Sequence[Mapping[str, Any]],
    broker_open_orders: Sequence[Mapping[str, Any]],
    lifecycle_positions: Sequence[Mapping[str, Any]],
    unresolved_submit_intents: Sequence[Mapping[str, Any]] = (),
    lifecycle_review_required: bool = False,
    broker_flat_proof_path: str | Path | None = None,
    open_orders_proof_path: str | Path | None = None,
    source_artifact_path: str | Path | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now)
    current_exposure = [_identity_row(row) for row in broker_positions if _quantity(row.get("quantity")) != 0]
    current_orders = [_identity_row(row) for row in broker_open_orders]
    current_lifecycle = [_identity_row(row) for row in lifecycle_positions]
    base = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": actual_now.isoformat(),
        "apply": config.apply,
        "stale_after_seconds": config.stale_after_seconds,
        "broker_flat_proof_path": str(broker_flat_proof_path or ""),
        "open_orders_proof_path": str(open_orders_proof_path or ""),
        "source_artifact_path": str(source_artifact_path or ""),
        "historical_only": True,
        "not_current_exposure": not current_exposure,
        "not_current_open_order": not current_orders,
        "broker_position_count": len(current_exposure),
        "broker_open_order_count": len(current_orders),
        "lifecycle_position_count": len(current_lifecycle),
        "resolved_items": [],
        "blocked_items": [],
        "reason_codes": [],
    }
    if current_exposure or current_orders:
        return {
            **base,
            "classification": RESOLVER_BLOCKED,
            "reason_codes": ["CURRENT_EXPOSURE_OR_OPEN_ORDER_PRESENT"],
            "blocked_items": [
                {
                    "kind": "current_scope_guard",
                    "current_exposure": current_exposure,
                    "current_open_orders": current_orders,
                }
            ],
        }

    resolved: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for row in unresolved_submit_intents:
        item = _resolve_submit_intent_debris(
            config=config,
            now=actual_now,
            row=row,
            broker_flat_proof_path=broker_flat_proof_path,
            open_orders_proof_path=open_orders_proof_path,
            source_artifact_path=source_artifact_path,
        )
        (resolved if item.get("resolved") else blocked).append(item)

    if lifecycle_review_required:
        lifecycle_review_rows = _historical_lifecycle_review_rows(config)
        if not lifecycle_review_rows:
            resolved.append(
                {
                    "kind": "lifecycle_review_summary",
                    "resolved": True,
                    "historical_only": True,
                    "not_current_exposure": True,
                    "not_current_open_order": True,
                    "reason_codes": [
                        "HISTORICAL_LIFECYCLE_REVIEW_SUMMARY_RESOLVED_FLAT",
                        "BROKER_FLAT_PROOF_CONFIRMED",
                        "NO_OPEN_ORDER_PROOF_CONFIRMED",
                    ],
                }
            )
        for row in lifecycle_review_rows:
            item = _resolve_lifecycle_review_debris(
                config=config,
                now=actual_now,
                row=row,
                broker_flat_proof_path=broker_flat_proof_path,
                open_orders_proof_path=open_orders_proof_path,
                source_artifact_path=source_artifact_path,
            )
            (resolved if item.get("resolved") else blocked).append(item)

    if not resolved and not blocked:
        return {**base, "classification": RESOLVER_NOT_APPLICABLE, "reason_codes": ["NO_HISTORICAL_DEBRIS_CANDIDATES"]}
    reason_codes = ["HISTORICAL_DEBRIS_APPEND_ONLY_RESOLUTION"]
    if blocked:
        reason_codes.append("HISTORICAL_DEBRIS_PARTIAL_OR_BLOCKED")
    return {
        **base,
        "classification": RESOLVER_CLEAN if not blocked else RESOLVER_BLOCKED,
        "resolved_items": resolved,
        "blocked_items": blocked,
        "reason_codes": reason_codes,
    }


def _resolve_submit_intent_debris(
    *,
    config: HistoricalReconciliationDebrisResolverConfig,
    now: datetime,
    row: Mapping[str, Any],
    broker_flat_proof_path: str | Path | None,
    open_orders_proof_path: str | Path | None,
    source_artifact_path: str | Path | None,
) -> dict[str, Any]:
    age = _age_seconds(row.get("created_at") or row.get("updated_at"), now)
    identity = _identity_from_submit_intent(row)
    if age is None or age < config.stale_after_seconds:
        return {**identity, "kind": "submit_intent", "resolved": False, "reason_codes": ["ARTIFACT_NOT_STALE_ENOUGH"]}
    if _bool(row.get("lifecycle_position_open")):
        return {**identity, "kind": "submit_intent", "resolved": False, "reason_codes": ["LINKED_TO_CURRENT_LIFECYCLE_OPEN_STATE"]}
    if _registry_record_historical_flat(config, identity.get("trade_id")):
        return {
            **identity,
            "kind": "submit_intent",
            "resolved": True,
            "already_resolved": True,
            "artifact_age_seconds": age,
            "reason_codes": ["HISTORICAL_DEBRIS_ALREADY_RESOLVED_FLAT"],
        }
    evidence = resolve_broker_backed_fill_evidence(
        repo_root=config.repo_root,
        request=BrokerFillEvidenceRequest(
            trade_id=identity.get("trade_id"),
            submit_intent_id=row.get("ownership_intent_id"),
            order_id=row.get("broker_order_id"),
            client_id=row.get("client_id"),
            perm_id=row.get("perm_id"),
            con_id=row.get("con_id"),
            local_symbol=row.get("local_symbol"),
            account_id=row.get("account_id"),
            action=row.get("action"),
            qty=row.get("qty"),
            symbol=row.get("symbol"),
        ),
        extra_source_paths=[Path(path) for path in row.get("source_artifact_paths") or []],
    )
    reason_codes = [
        "HISTORICAL_SUBMIT_INTENT_RESOLVED_FLAT",
        "BROKER_FLAT_PROOF_CONFIRMED",
        "NO_OPEN_ORDER_PROOF_CONFIRMED",
        "NOT_CURRENT_EXPOSURE",
        "NOT_CURRENT_OPEN_ORDER",
    ]
    if evidence.classification == RESOLVED:
        reason_codes.append("BROKER_EXECUTION_EVIDENCE_FOUND")
    else:
        reason_codes.append("NO_EXACT_EXECUTION_EVIDENCE_FOUND")

    if config.apply:
        if evidence.classification == RESOLVED:
            _append_registry_event(
                config=config,
                now=now,
                event_type=TradeEventType.ENTRY_FILL_BROKER_BACKED,
                identity=identity,
                source_artifact_path=source_artifact_path,
                reason_codes=("BROKER_BACKED_FILL_EVIDENCE_RESOLVED_FOR_HISTORICAL_DEBRIS",),
                evidence=evidence.evidence,
                metadata={
                    "historical_only": True,
                    "broker_fill_evidence": evidence.evidence,
                    "broker_flat_proof_path": str(broker_flat_proof_path or ""),
                    "open_orders_proof_path": str(open_orders_proof_path or ""),
                },
            )
        _append_registry_event(
            config=config,
            now=now,
            event_type=TradeEventType.RECONCILED_FLAT_HISTORICAL_CLEANUP,
            identity=identity,
            source_artifact_path=source_artifact_path,
            reason_codes=tuple(reason_codes),
            metadata={
                "historical_only": True,
                "not_current_exposure": True,
                "not_current_open_order": True,
                "broker_flat_proof_path": str(broker_flat_proof_path or ""),
                "open_orders_proof_path": str(open_orders_proof_path or ""),
                "broker_fill_evidence_classification": evidence.classification,
                "broker_fill_evidence": evidence.evidence,
            },
        )
        _append_submit_intent_resolution(config=config, now=now, row=row, reason_codes=reason_codes, evidence=evidence.evidence)
    return {
        **identity,
        "kind": "submit_intent",
        "resolved": True,
        "artifact_age_seconds": age,
        "broker_fill_evidence_classification": evidence.classification,
        "reason_codes": reason_codes,
    }


def _resolve_lifecycle_review_debris(
    *,
    config: HistoricalReconciliationDebrisResolverConfig,
    now: datetime,
    row: Mapping[str, Any],
    broker_flat_proof_path: str | Path | None,
    open_orders_proof_path: str | Path | None,
    source_artifact_path: str | Path | None,
) -> dict[str, Any]:
    identity = _identity_from_lifecycle_review(row)
    age = _age_seconds(row.get("generated_at") or row.get("updated_at") or row.get("created_at"), now)
    if age is None or age < config.stale_after_seconds:
        return {**identity, "kind": "lifecycle_review", "resolved": False, "reason_codes": ["ARTIFACT_NOT_STALE_ENOUGH"]}
    if _registry_record_historical_flat(config, identity.get("trade_id")):
        return {
            **identity,
            "kind": "lifecycle_review",
            "resolved": True,
            "already_resolved": True,
            "artifact_age_seconds": age,
            "reason_codes": ["HISTORICAL_DEBRIS_ALREADY_RESOLVED_FLAT"],
        }
    reason_codes = [
        "HISTORICAL_LIFECYCLE_REVIEW_RESOLVED_FLAT",
        "BROKER_FLAT_PROOF_CONFIRMED",
        "NO_OPEN_ORDER_PROOF_CONFIRMED",
        "NOT_CURRENT_EXPOSURE",
        "NOT_CURRENT_OPEN_ORDER",
    ]
    if config.apply:
        _append_registry_event(
            config=config,
            now=now,
            event_type=TradeEventType.RECONCILED_FLAT_HISTORICAL_CLEANUP,
            identity=identity,
            source_artifact_path=source_artifact_path,
            reason_codes=tuple(reason_codes),
            metadata={
                "historical_only": True,
                "not_current_exposure": True,
                "not_current_open_order": True,
                "broker_flat_proof_path": str(broker_flat_proof_path or ""),
                "open_orders_proof_path": str(open_orders_proof_path or ""),
                "source_lifecycle_review": _json_safe(row),
            },
        )
    return {**identity, "kind": "lifecycle_review", "resolved": True, "artifact_age_seconds": age, "reason_codes": reason_codes}


def _append_submit_intent_resolution(
    *,
    config: HistoricalReconciliationDebrisResolverConfig,
    now: datetime,
    row: Mapping[str, Any],
    reason_codes: Sequence[str],
    evidence: Mapping[str, Any] | None,
) -> None:
    payload = dict(row)
    payload["state"] = SubmitIntentOwnershipState.HISTORICAL_FLAT_RESOLVED.value
    payload["updated_at"] = now.isoformat()
    payload["lifecycle_position_open"] = False
    extra = dict(payload.get("extra") or {})
    extra.update(
        {
            "historical_only": True,
            "not_current_exposure": True,
            "not_current_open_order": True,
            "historical_reconciliation_debris_resolved": True,
            "historical_reconciliation_reason_codes": list(reason_codes),
            "broker_fill_evidence": dict(evidence or {}),
        }
    )
    payload["extra"] = extra
    append_submit_intent_ownership_record(
        payload,
        jsonl_path=config.resolve(config.submit_intent_ownership_path),
        latest_path=config.resolve(config.latest_submit_intent_ownership_path),
    )


def _append_registry_event(
    *,
    config: HistoricalReconciliationDebrisResolverConfig,
    now: datetime,
    event_type: TradeEventType,
    identity: Mapping[str, Any],
    source_artifact_path: str | Path | None,
    reason_codes: Sequence[str],
    metadata: Mapping[str, Any],
    evidence: Mapping[str, Any] | None = None,
) -> None:
    evidence = evidence or {}
    append_live_trade_registry_event(
        repo_root=config.repo_root,
        event=make_live_trade_registry_event(
            event_type=event_type,
            generated_at=now,
            trade_id=str(identity.get("trade_id") or ""),
            lifecycle_id=_text(identity.get("lifecycle_id")) or None,
            lane_id=_text(identity.get("lane_id")) or "historical_reconciliation_debris",
            thesis_strategy_id=_text(identity.get("strategy_id")) or _text(identity.get("lane_id")) or "historical_reconciliation_debris",
            account_id=_text(identity.get("account_id")) or "DUM882026",
            symbol=_text(identity.get("symbol")).upper() or "UNKNOWN",
            con_id=identity.get("con_id") or 1,
            local_symbol=_text(identity.get("local_symbol")) or "UNKNOWN",
            expiry=_text(identity.get("expiry")) or "UNKNOWN",
            side=_side_from_action(identity.get("action") or identity.get("side")),
            action=_text(identity.get("action")).upper() or "HISTORICAL_FLAT_CLEANUP",
            qty=identity.get("qty") or 1,
            source_artifact_path=str(source_artifact_path or identity.get("source_artifact_path") or ""),
            order_id=evidence.get("order_id") if event_type == TradeEventType.ENTRY_FILL_BROKER_BACKED else None,
            client_id=evidence.get("client_id") if event_type == TradeEventType.ENTRY_FILL_BROKER_BACKED else None,
            perm_id=evidence.get("perm_id") if event_type == TradeEventType.ENTRY_FILL_BROKER_BACKED else None,
            exec_id=evidence.get("exec_id") if event_type == TradeEventType.ENTRY_FILL_BROKER_BACKED else None,
            price=evidence.get("price") if event_type == TradeEventType.ENTRY_FILL_BROKER_BACKED else None,
            reason_codes=tuple(reason_codes),
            metadata={
                "source": "track_b_historical_reconciliation_debris_resolver",
                "paper_only": True,
                "live_money_eligible": False,
                "paper_proof_invoked": False,
                **dict(metadata),
            },
        ),
    )


def _historical_lifecycle_review_rows(config: HistoricalReconciliationDebrisResolverConfig) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in (config.resolve(config.managed_positions_path), config.resolve(config.position_truth_path)):
        payload = _load_json(path)
        for key in ("managed_positions", "review_required_positions"):
            for raw in payload.get(key) or []:
                if not isinstance(raw, Mapping):
                    continue
                row = raw.get("review_required_position") if isinstance(raw.get("review_required_position"), Mapping) else raw
                if not isinstance(row, Mapping):
                    continue
                item = dict(row)
                item.setdefault("source_artifact_path", str(path))
                rows.append(item)
    unique: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        unique.setdefault((_text(row.get("trade_id")), _text(row.get("lifecycle_id"))), row)
    return list(unique.values())


def _registry_record_historical_flat(config: HistoricalReconciliationDebrisResolverConfig, trade_id: object) -> bool:
    text = _text(trade_id)
    if not text:
        return False
    try:
        records = load_live_trade_registry_records(repo_root=config.repo_root)
    except Exception:
        return False
    for record in records:
        if record.trade_id != text:
            continue
        if record.current_state.value == "CLOSED_FLAT" and "RECONCILED_FLAT_HISTORICAL_CLEANUP" in record.latest_reason_codes:
            return True
    return False


def _identity_from_submit_intent(row: Mapping[str, Any]) -> dict[str, Any]:
    extra = row.get("extra") if isinstance(row.get("extra"), Mapping) else {}
    trade_id = _text(extra.get("trade_id") or row.get("trade_id")) or trade_id_from_live_identity(
        lifecycle_id=row.get("lifecycle_id"),
        account_id=row.get("account_id"),
        con_id=row.get("con_id"),
        lane_id=row.get("lane_id") or row.get("strategy_id") or "submit_intent",
    )
    return {
        "trade_id": trade_id,
        "ownership_intent_id": row.get("ownership_intent_id"),
        "lifecycle_id": row.get("lifecycle_id"),
        "lane_id": row.get("lane_id"),
        "strategy_id": row.get("strategy_id"),
        "account_id": row.get("account_id"),
        "symbol": row.get("symbol"),
        "con_id": row.get("con_id"),
        "local_symbol": row.get("local_symbol"),
        "expiry": row.get("expiry"),
        "side": _side_from_action(row.get("action")),
        "action": row.get("action"),
        "qty": row.get("qty") or 1,
        "source_artifact_path": ",".join(str(path) for path in row.get("source_artifact_paths") or []),
    }


def _identity_from_lifecycle_review(row: Mapping[str, Any]) -> dict[str, Any]:
    trade_id = _text(row.get("trade_id")) or trade_id_from_live_identity(
        lifecycle_id=row.get("lifecycle_id"),
        account_id=row.get("account_id"),
        con_id=row.get("con_id"),
        lane_id=row.get("lane_id") or row.get("strategy_id") or "lifecycle_review",
    )
    return {
        "trade_id": trade_id,
        "lifecycle_id": row.get("lifecycle_id"),
        "lane_id": row.get("lane_id") or row.get("strategy_id"),
        "strategy_id": row.get("strategy_id") or row.get("lane_id"),
        "account_id": row.get("account_id"),
        "symbol": row.get("symbol") or row.get("instrument_family"),
        "con_id": row.get("con_id"),
        "local_symbol": row.get("local_symbol"),
        "expiry": row.get("expiry") or row.get("contract_month") or "UNKNOWN",
        "side": row.get("side") or _side_from_action(row.get("order_action") or row.get("action")),
        "action": row.get("order_action") or row.get("action") or "HISTORICAL_FLAT_CLEANUP",
        "qty": row.get("quantity") or row.get("qty") or 1,
        "source_artifact_path": row.get("source_artifact_path"),
    }


def _identity_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "account_id": row.get("account_id") or row.get("account"),
        "symbol": row.get("symbol") or row.get("instrument_family"),
        "local_symbol": row.get("local_symbol") or row.get("localSymbol"),
        "con_id": row.get("con_id") or row.get("conId"),
        "quantity": row.get("quantity") or row.get("qty") or row.get("position"),
        "order_id": row.get("order_id") or row.get("broker_order_id"),
    }


def _load_json(path: Path) -> dict[str, Any]:
    try:
        import json

        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _age_seconds(value: object, now: datetime) -> float | None:
    parsed = _parse_time(value)
    if parsed is None:
        return None
    return max((now - parsed).total_seconds(), 0.0)


def _parse_time(value: object) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _quantity(value: object) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes"}


def _text(value: object) -> str:
    return str(value or "").strip()


def _side_from_action(value: object) -> str:
    text = _text(value).upper()
    if text in {"LONG", "SHORT"}:
        return text
    return "SHORT" if text.startswith("SELL") else "LONG"


def _json_safe(value: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): item for key, item in value.items()}


__all__ = [
    "HistoricalReconciliationDebrisResolverConfig",
    "resolve_historical_reconciliation_debris",
    "RESOLVER_CLEAN",
    "RESOLVER_BLOCKED",
    "RESOLVER_NOT_APPLICABLE",
]
