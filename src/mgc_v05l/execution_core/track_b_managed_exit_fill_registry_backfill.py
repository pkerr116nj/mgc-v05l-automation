"""Backfill Managed Exit close fills into the canonical PAPER trade registry.

This module is artifact plumbing only. It reads existing lifecycle/order-control
evidence and appends missing ``EXIT_FILL_BROKER_BACKED`` rows to the append-only
trade registry. It never connects to a broker, submits, cancels, closes, starts
runtime processes, or participates in trading authority.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.track_b_central_trade_registry import TradeEventType
from mgc_v05l.execution_core.track_b_live_trade_registry import (
    DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_EVENTS_JSONL,
    append_live_trade_registry_event,
    broker_backed_fill_has_required_ids,
    make_live_trade_registry_event,
    trade_id_from_live_identity,
)


DEFAULT_LIFECYCLE_REPORT_ROOT = (
    Path("outputs") / "track_b_execution_core" / "track_b_strategy_managed_paper_lifecycle"
)
DEFAULT_PAPER_ORDER_CONTROL_EVENTS = (
    Path("outputs") / "track_b_execution_core" / "paper_order_control" / "track_b_paper_order_control.jsonl"
)
DEFAULT_POSITION_TRUTH = Path("outputs") / "track_b_execution_core" / "position_truth" / "latest_position_truth.json"
DEFAULT_BACKFILL_SUMMARY = (
    Path("outputs") / "track_b_execution_core" / "trade_registry" / "latest_managed_exit_fill_backfill_summary.json"
)
SCHEMA_VERSION = "track_b_managed_exit_fill_registry_backfill_v1"


@dataclass(frozen=True)
class ManagedExitFillBackfillResult:
    generated_at: datetime
    scanned_lifecycle_reports: int
    candidate_close_fills: int
    appended_count: int
    skipped_count: int
    duplicate_count: int
    appended_events: tuple[dict[str, Any], ...]
    skipped: tuple[dict[str, Any], ...]
    summary_path: Path | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "generated_at": self.generated_at.isoformat(),
            "read_only_sources": True,
            "broker_mutation_allowed": False,
            "runtime_restart_allowed": False,
            "managed_exit_restart_allowed": False,
            "scanned_lifecycle_reports": self.scanned_lifecycle_reports,
            "candidate_close_fills": self.candidate_close_fills,
            "appended_count": self.appended_count,
            "skipped_count": self.skipped_count,
            "duplicate_count": self.duplicate_count,
            "appended_events": list(self.appended_events),
            "skipped": list(self.skipped),
            "summary_path": None if self.summary_path is None else str(self.summary_path),
        }


def publish_managed_exit_fill_backfill(
    *,
    repo_root: Path | str = Path("."),
    lifecycle_report_root: Path | str = DEFAULT_LIFECYCLE_REPORT_ROOT,
    paper_order_control_events_path: Path | str = DEFAULT_PAPER_ORDER_CONTROL_EVENTS,
    position_truth_path: Path | str = DEFAULT_POSITION_TRUTH,
    trade_registry_events_path: Path | str = DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_EVENTS_JSONL,
    summary_path: Path | str = DEFAULT_BACKFILL_SUMMARY,
    dry_run: bool = False,
    write_summary: bool = True,
    now: datetime | None = None,
) -> ManagedExitFillBackfillResult:
    """Append missing canonical Managed Exit fill events from existing artifacts."""

    root = Path(repo_root)
    generated_at = _ensure_utc(now or datetime.now(UTC))
    registry_path = _resolve(root, Path(trade_registry_events_path))
    existing_fill_keys = _existing_exit_fill_keys(registry_path)
    order_control_rows = _read_jsonl(_resolve(root, Path(paper_order_control_events_path)))
    order_control_by_order = _order_control_by_order(order_control_rows)
    position_truth_ref = _source_ref(_resolve(root, Path(position_truth_path)))

    scanned = 0
    candidates = 0
    appended: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    duplicates = 0
    for report_path in sorted(_resolve(root, Path(lifecycle_report_root)).glob("*/track_b_strategy_managed_paper_lifecycle_report.json")):
        scanned += 1
        report = _read_json(report_path)
        if not isinstance(report, Mapping):
            skipped.append(_skip(report_path, "lifecycle_report_unreadable"))
            continue
        close_fill = _mapping(report.get("close_fill")) or _mapping(_mapping(report.get("close_submit_attempt")).get("close_fill"))
        if not close_fill:
            continue
        candidates += 1
        evidence = _event_evidence_from_lifecycle_report(
            report=report,
            report_path=report_path,
            close_fill=close_fill,
            order_control_by_order=order_control_by_order,
            position_truth_ref=position_truth_ref,
        )
        if evidence.get("skip_reason"):
            skipped.append(evidence)
            continue
        fill_key = _fill_key(evidence)
        if fill_key in existing_fill_keys:
            duplicates += 1
            continue
        event = make_live_trade_registry_event(
            event_type=TradeEventType.EXIT_FILL_BROKER_BACKED,
            trade_id=str(evidence["trade_id"]),
            lifecycle_id=str(evidence["lifecycle_id"]),
            lane_id=str(evidence["lane_id"]),
            thesis_strategy_id=str(evidence["strategy_id"]),
            account_id=str(evidence["account_id"]),
            symbol=str(evidence["symbol"]),
            con_id=evidence["con_id"],
            local_symbol=str(evidence["local_symbol"]),
            expiry=str(evidence["expiry"]),
            side=str(evidence["side"]),
            action=str(evidence["action"]),
            qty=evidence["qty"],
            source_artifact_path=str(report_path),
            generated_at=evidence["filled_at"],
            order_id=evidence["order_id"],
            client_id=evidence["client_id"],
            perm_id=evidence["perm_id"],
            exec_id=evidence["exec_id"],
            price=evidence["price"],
            reason_codes=("MANAGED_EXIT_FILL_BACKFILLED_FROM_BROKER_EVIDENCE",),
            metadata={
                "source": "track_b_managed_exit_fill_registry_backfill",
                "managed_exit_policy_id": evidence.get("managed_exit_policy_id"),
                "close_reason": evidence.get("close_reason"),
                "source_refs": evidence.get("source_refs") or [],
                "paper_only": True,
                "live_money_eligible": False,
                "paper_proof_invoked": False,
            },
        )
        if not dry_run:
            result = append_live_trade_registry_event(
                repo_root=root,
                event=event,
                jsonl_path=Path(trade_registry_events_path),
            )
            if result.get("persisted") is not True:
                skipped.append(
                    {
                        "source_artifact_path": str(report_path),
                        "skip_reason": result.get("classification") or "registry_append_failed",
                        "event_type": result.get("event_type"),
                        "trade_id": result.get("trade_id"),
                        "lifecycle_id": result.get("lifecycle_id"),
                    }
                )
                continue
        existing_fill_keys.add(fill_key)
        appended.append(
            {
                "event_type": event.event_type.value,
                "trade_id": event.trade_id,
                "lifecycle_id": event.lifecycle_id,
                "lane_id": event.lane_id,
                "symbol": event.symbol,
                "local_symbol": event.local_symbol,
                "order_id": event.order_id,
                "perm_id": event.perm_id,
                "exec_id": event.exec_id,
                "price": None if event.price is None else str(event.price),
                "generated_at": event.generated_at.isoformat(),
                "dry_run": dry_run,
            }
        )

    summary_target = _resolve(root, Path(summary_path))
    result = ManagedExitFillBackfillResult(
        generated_at=generated_at,
        scanned_lifecycle_reports=scanned,
        candidate_close_fills=candidates,
        appended_count=len(appended),
        skipped_count=len(skipped),
        duplicate_count=duplicates,
        appended_events=tuple(appended),
        skipped=tuple(skipped),
        summary_path=summary_target if write_summary else None,
    )
    if write_summary:
        summary_target.parent.mkdir(parents=True, exist_ok=True)
        summary_target.write_text(json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def _event_evidence_from_lifecycle_report(
    *,
    report: Mapping[str, Any],
    report_path: Path,
    close_fill: Mapping[str, Any],
    order_control_by_order: Mapping[str, Sequence[Mapping[str, Any]]],
    position_truth_ref: dict[str, Any] | None,
) -> dict[str, Any]:
    close_intent = _mapping(report.get("close_intent"))
    entry_intent = _mapping(report.get("entry_intent"))
    close_submit = _mapping(report.get("close_submit_attempt"))
    broker_order = _mapping(close_submit.get("broker_order"))
    order_id = _text(close_fill.get("broker_order_id") or close_fill.get("order_id") or close_submit.get("broker_order_id"))
    perm_id = _text(close_fill.get("perm_id"))
    exec_id = _text(close_fill.get("execution_id") or close_fill.get("exec_id"))
    client_id = _text(
        close_fill.get("client_id")
        or close_submit.get("client_id")
        or broker_order.get("client_id")
        or _first_order_control_value(order_control_by_order, order_id, "client_id")
    )
    if not broker_backed_fill_has_required_ids(perm_id=perm_id, exec_id=exec_id) or not order_id or not client_id:
        return _skip(
            report_path,
            "close_fill_missing_required_broker_ids",
            order_id=order_id,
            client_id=client_id,
            perm_id=perm_id,
            exec_id=exec_id,
        )
    filled_at = _parse_time(close_fill.get("filled_at"))
    if filled_at is None:
        return _skip(report_path, "close_fill_missing_filled_at", order_id=order_id, perm_id=perm_id, exec_id=exec_id)
    lifecycle_id = _text(close_fill.get("lifecycle_id") or close_intent.get("lifecycle_id") or report.get("lifecycle_id"))
    lane_id = _text(close_intent.get("strategy_id") or report.get("lane_id") or report.get("strategy_id") or entry_intent.get("strategy_id"))
    strategy_id = _text(report.get("strategy_id") or close_intent.get("strategy_id") or lane_id)
    account_id = _text(close_intent.get("account_id") or report.get("account_id") or entry_intent.get("account_id"))
    symbol = _text(report.get("instrument_family") or close_intent.get("instrument_family") or entry_intent.get("instrument_family"))
    con_id = _int(close_intent.get("con_id") or report.get("con_id") or entry_intent.get("con_id"))
    local_symbol = _text(close_intent.get("local_symbol") or report.get("local_symbol") or entry_intent.get("local_symbol"))
    expiry = _text(close_intent.get("expiry") or report.get("contract_expiry") or entry_intent.get("expiry") or _expiry_from_contract_key(report.get("contract_key") or close_intent.get("contract_key")))
    side = _text(close_intent.get("side") or entry_intent.get("side") or report.get("side"))
    action = _text(close_intent.get("order_action") or close_fill.get("action") or ("SELL" if str(side).upper() == "LONG" else "BUY" if str(side).upper() == "SHORT" else ""))
    qty = close_fill.get("quantity") or close_intent.get("quantity") or report.get("quantity")
    price = close_fill.get("price")
    trade_id = _text(report.get("trade_id") or close_intent.get("trade_id")) or trade_id_from_live_identity(
        lifecycle_id=lifecycle_id,
        account_id=account_id,
        con_id=con_id,
        lane_id=lane_id,
    )
    missing = [
        key
        for key, value in {
            "trade_id": trade_id,
            "lifecycle_id": lifecycle_id,
            "lane_id": lane_id,
            "strategy_id": strategy_id,
            "account_id": account_id,
            "symbol": symbol,
            "local_symbol": local_symbol,
            "expiry": expiry,
            "side": side,
            "action": action,
            "qty": qty,
            "price": price,
        }.items()
        if value in (None, "")
    ]
    if missing or con_id <= 0:
        return _skip(
            report_path,
            "close_fill_missing_identity",
            missing_fields=missing + ([] if con_id > 0 else ["con_id"]),
            order_id=order_id,
            perm_id=perm_id,
            exec_id=exec_id,
        )
    source_refs = [
        _source_ref(report_path),
        *_order_control_refs(order_control_by_order, order_id),
    ]
    if position_truth_ref is not None:
        source_refs.append(position_truth_ref)
    return {
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "lane_id": lane_id,
        "strategy_id": strategy_id,
        "account_id": account_id,
        "symbol": symbol,
        "con_id": con_id,
        "local_symbol": local_symbol,
        "expiry": expiry,
        "side": side,
        "action": action,
        "qty": qty,
        "price": price,
        "filled_at": filled_at,
        "order_id": order_id,
        "client_id": client_id,
        "perm_id": perm_id,
        "exec_id": exec_id,
        "managed_exit_policy_id": close_intent.get("managed_exit_policy_id") or report.get("managed_exit_policy_id"),
        "close_reason": close_intent.get("close_reason"),
        "source_refs": source_refs,
    }


def _existing_exit_fill_keys(path: Path) -> set[tuple[str, str, str, str]]:
    keys: set[tuple[str, str, str, str]] = set()
    for row in _read_jsonl(path):
        if str(row.get("event_type") or "") != TradeEventType.EXIT_FILL_BROKER_BACKED.value:
            continue
        key = _fill_key(row)
        if all(key):
            keys.add(key)
    return keys


def _fill_key(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row.get("account_id") or "").strip(),
        str(row.get("order_id") or row.get("broker_order_id") or "").strip(),
        str(row.get("perm_id") or "").strip(),
        str(row.get("exec_id") or row.get("execution_id") or "").strip(),
    )


def _order_control_by_order(rows: Sequence[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        order_id = _text(row.get("order_id") or row.get("broker_order_id"))
        if order_id:
            grouped.setdefault(order_id, []).append(row)
    return grouped


def _first_order_control_value(
    rows_by_order: Mapping[str, Sequence[Mapping[str, Any]]],
    order_id: str | None,
    key: str,
) -> Any:
    if not order_id:
        return None
    for row in rows_by_order.get(order_id) or ():
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _order_control_refs(
    rows_by_order: Mapping[str, Sequence[Mapping[str, Any]]],
    order_id: str | None,
) -> list[dict[str, Any]]:
    if not order_id or order_id not in rows_by_order:
        return []
    return [
        {
            "source": "paper_order_control",
            "order_id": order_id,
            "row_count": len(rows_by_order[order_id]),
            "statuses": sorted({str(row.get("status") or row.get("order_status") or "") for row in rows_by_order[order_id] if str(row.get("status") or row.get("order_status") or "")}),
        }
    ]


def _skip(path: Path, reason: str, **extra: Any) -> dict[str, Any]:
    return {"source_artifact_path": str(path), "skip_reason": reason, **{key: value for key, value in extra.items() if value not in (None, "")}}


def _source_ref(path: Path) -> dict[str, Any] | None:
    return {"source_artifact_path": str(path), "exists": path.exists()}


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _read_json(path: Path) -> Any:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _read_jsonl(path: Path) -> list[Mapping[str, Any]]:
    if not path.exists():
        return []
    rows: list[Mapping[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return rows
    for line in lines:
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, Mapping):
            rows.append(row)
    return rows


def _resolve(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else root / path


def _text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _int(value: Any) -> int:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed > 0 else 0


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _expiry_from_contract_key(value: Any) -> str | None:
    text = _text(value)
    if not text or "-" not in text:
        return None
    suffix = text.rsplit("-", 1)[-1].strip()
    return suffix if suffix.isdigit() else None


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


def _main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    result = publish_managed_exit_fill_backfill(repo_root=Path(args.repo_root), dry_run=args.dry_run)
    print(json.dumps(_jsonable(result.to_dict()), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI wrapper
    raise SystemExit(_main())
