"""Read-only strategy performance attachment for Track B PAPER lanes.

This module observes persisted PAPER artifacts and writes analytics-only
performance summaries. It must not own broker authority, submit orders, close
positions, cancel orders, mutate lifecycle state, or gate runtime execution.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.bounded_jsonl import write_bounded_jsonl
from mgc_v05l.execution_core.models import to_jsonable


DEFAULT_RUNTIME_DIR = Path("outputs") / "probationary_pattern_engine" / "paper_session"
DEFAULT_CONFIG_IN_FORCE = DEFAULT_RUNTIME_DIR / "runtime" / "paper_config_in_force.json"
DEFAULT_ROSTER = DEFAULT_RUNTIME_DIR / "runtime" / "paper_stack_mnq_mes_full_session_active_evidence_guarded_roster.json"
DEFAULT_FILLED_BRIDGE_RESULTS = DEFAULT_RUNTIME_DIR / "filled_bridge_results.jsonl"
DEFAULT_TRADE_REGISTRY_EVENTS = Path("outputs") / "track_b_execution_core" / "trade_registry" / "live_trade_events.jsonl"
DEFAULT_FUNNEL_EVENTS = (
    Path("outputs") / "track_b_execution_core" / "strategy_attrition_funnel" / "strategy_funnel_events.jsonl"
)
DEFAULT_PHASE1_ROOT = Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data"
DEFAULT_PHASE1_DURABLE_ROOT = Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data_intraday_backfill"
DEFAULT_OUTPUT_DIR = Path("outputs") / "track_b_execution_core" / "strategy_performance"
DEFAULT_EVENTS_PATH = DEFAULT_OUTPUT_DIR / "lane_performance_events.jsonl"
DEFAULT_SUMMARY_PATH = DEFAULT_OUTPUT_DIR / "latest_lane_performance_summary.json"
DEFAULT_CANONICAL_TRADE_RECORDS_PATH = DEFAULT_OUTPUT_DIR / "canonical_trade_records.jsonl"
DEFAULT_PAIRING_SUMMARY_PATH = DEFAULT_OUTPUT_DIR / "latest_trade_pairing_summary.json"

ENTRY_INTENT_TYPES = {"BUY_TO_OPEN", "SELL_TO_OPEN", "SELL_SHORT"}
EXIT_EVENT_TYPES = {"EXIT_FILL_BROKER_BACKED", "EXIT_ORDER_FILLED", "POSITION_CLOSED"}
PERFORMANCE_SCHEMA_VERSION = "track_b_strategy_performance_event_v1"
PATH_CAPTURE_SCHEMA_VERSION = "canonical_trade_path_capture_ref_v1"


@dataclass(frozen=True)
class LaneMetadata:
    lane_id: str
    strategy_id: str
    strategy_family: str
    variant_id: str
    entry_thesis: str
    session_label: str
    symbol: str
    side_bias: str | None
    exit_policy: str | None = None


@dataclass(frozen=True)
class AttachmentResult:
    events_path: Path
    summary_path: Path
    canonical_trades_path: Path
    pairing_summary_path: Path
    events_written: int
    summary: dict[str, Any]


def build_strategy_performance_attachment(
    *,
    repo_root: Path | str = Path("."),
    config_path: Path | str = DEFAULT_CONFIG_IN_FORCE,
    roster_path: Path | str = DEFAULT_ROSTER,
    filled_bridge_results_path: Path | str = DEFAULT_FILLED_BRIDGE_RESULTS,
    trade_registry_events_path: Path | str = DEFAULT_TRADE_REGISTRY_EVENTS,
    funnel_events_path: Path | str = DEFAULT_FUNNEL_EVENTS,
    phase1_root: Path | str = DEFAULT_PHASE1_ROOT,
    durable_phase1_root: Path | str = DEFAULT_PHASE1_DURABLE_ROOT,
    output_dir: Path | str = DEFAULT_OUTPUT_DIR,
    include_lanes: Sequence[str] | None = None,
    write_artifacts: bool = True,
) -> AttachmentResult:
    root = Path(repo_root)
    output = _resolve(root, Path(output_dir))
    lane_metadata = load_pilot_lane_metadata(
        repo_root=root,
        config_path=Path(config_path),
        roster_path=Path(roster_path),
        include_lanes=include_lanes,
    )
    entry_fills = [
        row
        for row in _read_jsonl(_resolve(root, Path(filled_bridge_results_path)))
        if _is_entry_fill(row) and str(row.get("lane_id") or "") in lane_metadata
    ]
    trade_registry_rows = _read_jsonl(_resolve(root, Path(trade_registry_events_path)))
    registry_lanes = set(lane_metadata)
    exit_fills = [
        row
        for row in trade_registry_rows
        if _is_exit_fill(row) and str(row.get("lane_id") or "") in lane_metadata
    ]
    registry_entries = [
        row
        for row in trade_registry_rows
        if _is_registry_entry_fill_or_lifecycle(row) and str(row.get("lane_id") or "") in registry_lanes
    ]
    funnel_rows = [
        row
        for row in _read_jsonl(_resolve(root, Path(funnel_events_path)))
        if str(row.get("lane_id") or "") in lane_metadata
    ]
    canonical_records, pairing_summary = build_canonical_trade_records(
        lane_metadata=lane_metadata,
        entry_fills=[*entry_fills, *registry_entries],
        exit_fills=exit_fills,
        funnel_rows=funnel_rows,
        phase1_root=_resolve(root, Path(phase1_root)),
        durable_phase1_root=_resolve(root, Path(durable_phase1_root)),
    )
    events = build_performance_events_from_canonical_records(
        lane_metadata=lane_metadata,
        canonical_records=canonical_records,
        funnel_rows=funnel_rows,
    )
    summary = build_performance_summary(events, lane_metadata=lane_metadata)
    summary["pairing"] = pairing_summary
    events_path = output / "lane_performance_events.jsonl"
    summary_path = output / "latest_lane_performance_summary.json"
    canonical_trades_path = output / "canonical_trade_records.jsonl"
    pairing_summary_path = output / "latest_trade_pairing_summary.json"
    if write_artifacts:
        output.mkdir(parents=True, exist_ok=True)
        write_bounded_jsonl(events_path, events)
        write_bounded_jsonl(canonical_trades_path, canonical_records)
        _write_json(summary_path, summary)
        _write_json(pairing_summary_path, pairing_summary)
        _write_per_lane_summaries(output / "lanes", events)
    return AttachmentResult(
        events_path=events_path,
        summary_path=summary_path,
        canonical_trades_path=canonical_trades_path,
        pairing_summary_path=pairing_summary_path,
        events_written=len(events),
        summary=summary,
    )


def load_pilot_lane_metadata(
    *,
    repo_root: Path | str = Path("."),
    config_path: Path | str = DEFAULT_CONFIG_IN_FORCE,
    roster_path: Path | str = DEFAULT_ROSTER,
    include_lanes: Sequence[str] | None = None,
    pilot_only: bool = False,
) -> dict[str, LaneMetadata]:
    root = Path(repo_root)
    configured_lanes = _active_lane_ids(_read_json(_resolve(root, Path(config_path))))
    roster_strategy_ids = set(_strategy_ids(_read_json(_resolve(root, Path(roster_path)))))
    requested = set(include_lanes or ())
    metadata: dict[str, LaneMetadata] = {}
    for lane_id in configured_lanes:
        if requested and lane_id not in requested:
            continue
        if pilot_only and not _is_pilot_lane(lane_id):
            continue
        if not _is_supported_active_participation_lane(lane_id):
            continue
        lane = _lane_metadata_from_lane_id(lane_id)
        if lane is None:
            continue
        if roster_strategy_ids and lane.strategy_id not in roster_strategy_ids:
            continue
        metadata[lane_id] = lane
    return metadata


def build_canonical_trade_records(
    *,
    lane_metadata: Mapping[str, LaneMetadata],
    entry_fills: Iterable[Mapping[str, Any]],
    exit_fills: Iterable[Mapping[str, Any]],
    funnel_rows: Iterable[Mapping[str, Any]],
    phase1_root: Path,
    durable_phase1_root: Path | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    now = datetime.now(UTC)
    sorted_entries = _dedupe_entries(entry_fills, now=now)
    sorted_exits = sorted([dict(row) for row in exit_fills], key=lambda row: _parse_ts(row.get("generated_at")) or now)
    sorted_funnel = sorted([dict(row) for row in funnel_rows], key=lambda row: _parse_ts(row.get("timestamp")) or now)

    records: list[dict[str, Any]] = []
    used_exit_indexes: set[int] = set()
    for entry in sorted_entries:
        lane_id = str(entry.get("lane_id") or "")
        metadata = lane_metadata.get(lane_id)
        if metadata is None:
            continue
        entry_time = _parse_ts(entry.get("fill_timestamp") or entry.get("created_at") or entry.get("decision_bar_timestamp"))
        if entry_time is None:
            continue
        exit_index, exit_row = _match_exit(entry, sorted_exits, used_exit_indexes)
        if exit_index is not None:
            used_exit_indexes.add(exit_index)
        performance = _performance_fields(
            entry=entry,
            exit_row=exit_row,
            metadata=metadata,
            phase1_root=phase1_root,
            mark_time=now,
        )
        record = _base_event(metadata, "CANONICAL_TRADE_RECORD")
        record.update(_entry_fields(entry, metadata))
        if exit_row is not None:
            record.update(_exit_fields(exit_row))
        record.update(performance)
        record.update(
            {
                "generated_at": _iso(now),
                "trade_status": "CLOSED" if exit_row is not None else "OPEN_OR_UNPAIRED",
                "pairing_status": "PAIRED" if exit_row is not None else "UNPAIRED_ENTRY",
                "pairing_reason": _pairing_reason(entry, exit_row),
                "exit_reason": _exit_reason(exit_row) if exit_row is not None else None,
                "blockers_before_entry": _blockers_before_entry(sorted_funnel, lane_id=lane_id, entry_time=entry_time),
                "source_refs": _source_refs(entry=entry, exit_row=exit_row),
                "path_capture": _path_capture_reference(
                    entry=entry,
                    exit_row=exit_row,
                    metadata=metadata,
                    phase1_root=phase1_root,
                    durable_phase1_root=durable_phase1_root or phase1_root,
                ),
            }
        )
        records.append(record)

    unpaired_exits: list[dict[str, Any]] = []
    ignored_unmatched_exits: list[dict[str, Any]] = []
    for index, row in enumerate(sorted_exits):
        if index in used_exit_indexes:
            continue
        if _is_backfilled_managed_exit_fill(row):
            ignored_unmatched_exits.append(
                {
                    "event_type": row.get("event_type"),
                    "trade_id": row.get("trade_id"),
                    "lifecycle_id": row.get("lifecycle_id"),
                    "lane_id": row.get("lane_id"),
                    "symbol": row.get("symbol"),
                    "order_id": row.get("order_id"),
                    "perm_id": row.get("perm_id"),
                    "exec_id": row.get("exec_id"),
                    "reason": "backfilled_managed_exit_without_entry_artifact",
                }
            )
            continue
        unpaired_exits.append(_unpaired_exit_record(row, index=index, now=now))
    records.extend(unpaired_exits)
    pairing_summary = _pairing_summary(
        canonical_records=records,
        total_entries=len(sorted_entries),
        total_exits=len(sorted_exits),
        ignored_unmatched_exits=ignored_unmatched_exits,
    )
    return records, pairing_summary


def build_performance_events_from_canonical_records(
    *,
    lane_metadata: Mapping[str, LaneMetadata],
    canonical_records: Sequence[Mapping[str, Any]],
    funnel_rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    now = datetime.now(UTC)
    sorted_funnel = sorted([dict(row) for row in funnel_rows], key=lambda row: _parse_ts(row.get("timestamp")) or now)
    events: list[dict[str, Any]] = []
    for record in canonical_records:
        lane_id = str(record.get("lane_id") or "")
        metadata = lane_metadata.get(lane_id)
        if metadata is None or record.get("pairing_status") == "UNPAIRED_EXIT":
            continue
        opened_event = _base_event(metadata, "TRADE_OPENED")
        opened_event.update({key: record.get(key) for key in _entry_event_keys()})
        opened_event["generated_at"] = record.get("generated_at")
        opened_event["blockers_before_entry"] = record.get("blockers_before_entry") or []
        opened_event["source_refs"] = record.get("source_refs")
        events.append(opened_event)
        if record.get("trade_status") == "CLOSED":
            closed_event = _base_event(metadata, "TRADE_CLOSED")
            closed_event.update({key: record.get(key) for key in [*_entry_event_keys(), *_exit_event_keys(), *_performance_event_keys()]})
            closed_event["generated_at"] = record.get("generated_at")
            closed_event["exit_reason"] = record.get("exit_reason")
            closed_event["blockers_before_entry"] = record.get("blockers_before_entry") or []
            closed_event["source_refs"] = record.get("source_refs")
            events.append(closed_event)
        else:
            mtm_event = _base_event(metadata, "TRADE_MARK_TO_MARKET")
            mtm_event.update({key: record.get(key) for key in [*_entry_event_keys(), *_performance_event_keys()]})
            mtm_event["generated_at"] = record.get("generated_at")
            mtm_event["mark_time"] = record.get("mark_time")
            mtm_event["blockers_before_entry"] = record.get("blockers_before_entry") or []
            mtm_event["source_refs"] = record.get("source_refs")
            events.append(mtm_event)

    for row in sorted_funnel:
        if row.get("pass_fail") != "FAIL":
            continue
        lane_id = str(row.get("lane_id") or "")
        metadata = lane_metadata.get(lane_id)
        if metadata is None:
            continue
        event = _base_event(metadata, "NO_TRADE_BLOCKED")
        event.update(
            {
                "generated_at": _iso(now),
                "timestamp": _iso(_parse_ts(row.get("timestamp"))),
                "no_trade_reason": row.get("reason") or row.get("blocker_classification"),
                "blocker_classification": row.get("blocker_classification"),
                "stage": row.get("stage"),
                "pass_fail": row.get("pass_fail"),
                "order_intent_id": row.get("order_intent_id"),
                "lifecycle_id": row.get("lifecycle_id"),
                "trade_id": row.get("trade_id"),
                "source_refs": {"funnel": str(DEFAULT_FUNNEL_EVENTS)},
            }
        )
        events.append(event)
    return events


def build_performance_summary(
    events: Sequence[Mapping[str, Any]],
    *,
    lane_metadata: Mapping[str, LaneMetadata],
) -> dict[str, Any]:
    generated_at = _iso(datetime.now(UTC))
    per_lane: dict[str, dict[str, Any]] = {}
    for lane_id, metadata in sorted(lane_metadata.items()):
        lane_events = [event for event in events if event.get("lane_id") == lane_id]
        counts = Counter(str(event.get("event_type") or "UNKNOWN") for event in lane_events)
        closed = [event for event in lane_events if event.get("event_type") == "TRADE_CLOSED"]
        blocked = [event for event in lane_events if event.get("event_type") == "NO_TRADE_BLOCKED"]
        per_lane[lane_id] = {
            "lane_id": lane_id,
            "strategy_id": metadata.strategy_id,
            "strategy_family": metadata.strategy_family,
            "variant_id": metadata.variant_id,
            "entry_thesis": metadata.entry_thesis,
            "session_label": metadata.session_label,
            "symbol": metadata.symbol,
            "exit_policy": metadata.exit_policy,
            "event_counts": dict(counts),
            "realized_pnl_points": _decimal_sum(closed, "realized_pnl_points"),
            "realized_pnl_currency": _decimal_sum(closed, "realized_pnl_currency"),
            "latest_event_at": max((str(event.get("generated_at") or event.get("timestamp") or "") for event in lane_events), default=None),
            "top_no_trade_reasons": dict(Counter(str(event.get("no_trade_reason") or "UNKNOWN") for event in blocked).most_common(5)),
        }
    return {
        "schema_version": "track_b_strategy_performance_summary_v1",
        "generated_at": generated_at,
        "classification": "STRATEGY_PERFORMANCE_ATTACHMENT_READY",
        "analytics_only": True,
        "broker_authority": False,
        "submit_authority": False,
        "managed_exit_authority": False,
        "lane_count": len(lane_metadata),
        "event_count": len(events),
        "event_counts": dict(Counter(str(event.get("event_type") or "UNKNOWN") for event in events)),
        "lanes": per_lane,
    }


def _dedupe_entries(entries: Iterable[Mapping[str, Any]], *, now: datetime) -> list[dict[str, Any]]:
    best: dict[tuple[str, str], dict[str, Any]] = {}
    for row in entries:
        candidate = dict(row)
        if not _is_entry_like(candidate):
            continue
        key = _entry_dedupe_key(candidate)
        existing = best.get(key)
        if existing is None:
            best[key] = candidate
            continue
        candidate_time = _entry_time(candidate) or now
        existing_time = _entry_time(existing) or now
        if candidate_time < existing_time or (candidate_time == existing_time and _entry_quality(candidate) > _entry_quality(existing)):
            best[key] = candidate
    return sorted(best.values(), key=lambda row: _entry_time(row) or now)


def _entry_dedupe_key(row: Mapping[str, Any]) -> tuple[str, str]:
    for key in ("lifecycle_id", "trade_id", "source_trade_id", "managed_position_id", "exec_id", "perm_id"):
        value = str(row.get(key) or "").strip()
        if value:
            return key, value
    lane_id = str(row.get("lane_id") or "")
    symbol = str(row.get("symbol") or row.get("instrument") or "")
    action = str(row.get("action") or row.get("side") or row.get("intent_type") or "")
    timestamp = _iso(_entry_time(row)) or ""
    price = str(row.get("fill_price") or row.get("price") or "")
    return "synthetic", "|".join((lane_id, symbol, action, timestamp, price))


def _entry_quality(row: Mapping[str, Any]) -> int:
    score = 0
    if row.get("bridge_order_status") == "FILLED":
        score += 10
    if row.get("event_type") == "LIFECYCLE_OPEN_MANAGED":
        score += 8
    if row.get("event_type") == "ENTRY_FILL_BROKER_BACKED":
        score += 6
    if row.get("managed_exit_policy_id") or _mapping(row.get("metadata")).get("managed_exit_policy_id"):
        score += 2
    if row.get("contract"):
        score += 1
    return score


def _is_entry_like(row: Mapping[str, Any]) -> bool:
    return _is_entry_fill(row) or _is_registry_entry_fill_or_lifecycle(row)


def _is_registry_entry_fill_or_lifecycle(row: Mapping[str, Any]) -> bool:
    event_type = str(row.get("event_type") or "")
    if event_type not in {"ENTRY_FILL_BROKER_BACKED", "LIFECYCLE_OPEN_MANAGED"}:
        return False
    action = str(row.get("action") or "").upper()
    side = str(row.get("side") or "").upper()
    intent_type = str(_mapping(row.get("metadata")).get("intent_type") or "").upper()
    return intent_type in ENTRY_INTENT_TYPES or action in {"BUY", "SELL"} and side in {"LONG", "SHORT"}


def _pairing_summary(
    *,
    canonical_records: Sequence[Mapping[str, Any]],
    total_entries: int,
    total_exits: int,
    ignored_unmatched_exits: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    paired = [row for row in canonical_records if row.get("pairing_status") == "PAIRED"]
    unpaired_entries = [row for row in canonical_records if row.get("pairing_status") == "UNPAIRED_ENTRY"]
    unpaired_exits = [row for row in canonical_records if row.get("pairing_status") == "UNPAIRED_EXIT"]
    return {
        "schema_version": "track_b_strategy_performance_pairing_summary_v1",
        "generated_at": _iso(datetime.now(UTC)),
        "classification": "TRADE_LIFECYCLE_PAIRING_COMPLETE" if not unpaired_exits else "TRADE_LIFECYCLE_PAIRING_HAS_UNPAIRED_EXITS",
        "analytics_only": True,
        "total_entries": total_entries,
        "total_exits": total_exits,
        "paired_trades": len(paired),
        "pairing_rate": (len(paired) / total_entries) if total_entries else 1.0,
        "unpaired_entry_count": len(unpaired_entries),
        "unpaired_exit_count": len(unpaired_exits),
        "unpaired_entry_reasons": dict(Counter(str(row.get("pairing_reason") or "UNKNOWN") for row in unpaired_entries)),
        "unpaired_exit_reasons": dict(Counter(str(row.get("pairing_reason") or "UNKNOWN") for row in unpaired_exits)),
        "ignored_unmatched_exit_count": len(ignored_unmatched_exits),
        "ignored_unmatched_exit_reasons": dict(
            Counter(str(row.get("reason") or "UNKNOWN") for row in ignored_unmatched_exits)
        ),
    }


def _unpaired_exit_record(row: Mapping[str, Any], *, index: int, now: datetime) -> dict[str, Any]:
    metadata = _mapping(row.get("metadata"))
    return {
        "schema_version": PERFORMANCE_SCHEMA_VERSION,
        "event_type": "CANONICAL_TRADE_RECORD",
        "generated_at": _iso(now),
        "trade_status": "UNPAIRED_EXIT",
        "pairing_status": "UNPAIRED_EXIT",
        "pairing_reason": "no_matching_entry_for_exit_fill",
        "trade_id": row.get("trade_id") or f"unpaired_exit_{index}",
        "lifecycle_id": row.get("lifecycle_id"),
        "lane_id": row.get("lane_id"),
        "strategy_id": row.get("thesis_strategy_id") or row.get("lane_id"),
        "strategy_family": "paper_active_evidence",
        "variant_id": f"{row.get('lane_id')}_v1" if row.get("lane_id") else None,
        "entry_thesis": "generic_active_participation",
        "session_label": _session_from_lane_id(str(row.get("lane_id") or "")),
        "symbol": row.get("symbol"),
        "local_symbol": row.get("local_symbol"),
        "con_id": row.get("con_id"),
        "side": row.get("side"),
        "qty": str(row.get("qty") or "1"),
        "exit_time": _iso(_parse_ts(row.get("generated_at") or row.get("filled_at"))),
        "exit_price": _str_decimal(row.get("price") or metadata.get("price")),
        "exit_order_id": row.get("order_id"),
        "exit_perm_id": row.get("perm_id"),
        "exit_exec_id": row.get("exec_id"),
        "exit_reason": _exit_reason(row),
        "source_refs": {"exit_fill": str(DEFAULT_TRADE_REGISTRY_EVENTS), "source_artifact_path": row.get("source_artifact_path")},
    }


def _entry_event_keys() -> tuple[str, ...]:
    return (
        "local_symbol",
        "con_id",
        "lifecycle_id",
        "trade_id",
        "side",
        "qty",
        "entry_time",
        "entry_price",
        "entry_order_id",
        "entry_perm_id",
        "entry_exec_id",
        "exit_policy",
    )


def _exit_event_keys() -> tuple[str, ...]:
    return ("exit_time", "exit_price", "exit_order_id", "exit_perm_id", "exit_exec_id", "exit_reason")


def _performance_event_keys() -> tuple[str, ...]:
    return (
        "hold_seconds",
        "mfe_points",
        "mae_points",
        "mark_price",
        "mark_time",
        "mark_to_market_pnl_points",
        "mark_to_market_pnl_currency",
        "realized_pnl_points",
        "realized_pnl_currency",
        "phase1_candle_source",
    )


def _path_capture_reference(
    *,
    entry: Mapping[str, Any],
    exit_row: Mapping[str, Any] | None,
    metadata: LaneMetadata,
    phase1_root: Path,
    durable_phase1_root: Path,
) -> dict[str, Any]:
    entry_time = _entry_time(entry)
    exit_time = _exit_time(exit_row) if exit_row else None
    decision_bar = _parse_ts(entry.get("decision_bar_timestamp"))
    retain_from = (decision_bar or entry_time) - timedelta(minutes=30) if (decision_bar or entry_time) else None
    retain_until = exit_time + timedelta(minutes=1) if exit_time else None
    trade_id = _trade_id_from_entry(entry)
    lifecycle_id = entry.get("lifecycle_id")
    capture_id = _stable_path_capture_id(trade_id, lifecycle_id, metadata.symbol, entry_time)
    durable_path = durable_phase1_root / metadata.symbol / "1m" / "latest_runtime_candles.json"
    runtime_path = phase1_root / metadata.symbol / "1m" / "latest_runtime_candles.json"
    source_path = durable_path if durable_path.exists() else runtime_path
    entry_bar = _bar_window_for_timestamp(source_path, entry_time)
    decision_window = _bar_window_for_timestamp(source_path, decision_bar) if decision_bar else None
    exit_bar = _bar_window_for_timestamp(source_path, exit_time) if exit_time else None
    return {
        "schema_version": PATH_CAPTURE_SCHEMA_VERSION,
        "capture_required": True,
        "capture_status": "CLOSED_PENDING_FINALIZATION" if exit_row is not None else "OPEN_ACCUMULATING",
        "capture_lifecycle_state": "CLOSED_PENDING_FINALIZATION" if exit_row is not None else "OPEN_ACCUMULATING",
        "capture_id": capture_id,
        "accumulator_key": "|".join(
            str(part or "")
            for part in (
                trade_id,
                lifecycle_id,
                metadata.symbol,
                entry.get("local_symbol") or _mapping(entry.get("contract")).get("local_symbol") or metadata.symbol,
                _entry_side(entry),
                _iso(entry_time),
            )
        ),
        "canonical_trade_path_id": None,
        "sample_source": {
            "source_component": "phase1_runtime_market_data_intraday_backfill",
            "source_artifact": str(durable_path),
            "fallback_source_artifact": str(runtime_path),
            "bar_granularity": "1m",
            "price_type": "OHLCV",
            "timezone": "UTC",
            "durable_source_required_for_complete_research": True,
        },
        "trade_identity": {
            "source_trade_id": trade_id,
            "managed_position_id": lifecycle_id,
            "lifecycle_id": lifecycle_id,
            "instrument": metadata.symbol,
            "contract": entry.get("local_symbol") or _mapping(entry.get("contract")).get("local_symbol"),
            "side": _entry_side(entry),
            "quantity": str(entry.get("quantity") or entry.get("qty") or "1"),
        },
        "entry_anchor": {
            "entry_fill_role": "FIRST_OPENING_FILL",
            "entry_time": _iso(entry_time),
            "entry_price": _str_decimal(_entry_price(entry)),
            "entry_bar_start": entry_bar.get("bar_start"),
            "entry_bar_end": entry_bar.get("bar_end"),
            "decision_bar_timestamp": _iso(decision_bar),
            "decision_bar_start": decision_window.get("bar_start") if decision_window else None,
            "decision_bar_end": decision_window.get("bar_end") if decision_window else None,
            "entry_fill_exec_id": entry.get("exec_id"),
            "entry_order_id": entry.get("broker_order_id") or entry.get("order_id"),
        },
        "exit_anchor": {
            "exit_fill_role": "FINAL_CLOSING_FILL" if exit_row is not None else None,
            "exit_time": _iso(exit_time),
            "exit_price": _str_decimal(_exit_price(exit_row)) if exit_row is not None else None,
            "exit_bar_start": exit_bar.get("bar_start") if exit_bar else None,
            "exit_bar_end": exit_bar.get("bar_end") if exit_bar else None,
            "exit_fill_exec_id": exit_row.get("exec_id") if exit_row is not None else None,
            "exit_order_id": exit_row.get("order_id") if exit_row is not None else None,
        },
        "fill_semantics": {
            "canonical_trade_unit": "ONE_RECORD_PER_LIFECYCLE_OR_SOURCE_TRADE_ID",
            "first_opening_fill_rule": "earliest opening fill for lifecycle/source trade id",
            "final_closing_fill_rule": "last risk-reducing close fill that pairs to the lifecycle/source trade id",
            "partial_fills_folded_into_trade": True,
            "scale_ins_folded_when_same_lifecycle_or_source_trade": True,
            "scale_outs_folded_until_final_closing_fill": True,
        },
        "retention": {
            "open_accumulator_path": "outputs/track_b_execution_core/research_analytics/live_trade_path_accumulator/open_trade_path_accumulator.jsonl",
            "finalized_capture_path": "outputs/track_b_execution_core/research_analytics/live_trade_path_accumulator/finalized_trade_path_capture.jsonl",
            "retain_from": _iso(retain_from),
            "retain_until": _iso(retain_until),
            "pre_decision_completed_1m_bars_required": 30,
            "post_exit_completed_1m_bars_required": 1,
        },
        "coverage": {
            "coverage_state": "OPEN_ACCUMULATING" if exit_row is None else "PENDING_FINALIZATION",
            "incomplete_paths_are_research_blocked": True,
            "path_sample_count": 0,
            "path_start_timestamp": None,
            "path_end_timestamp": None,
        },
        "finalized_metrics": {
            "path_coverage_status": "OPEN_ACCUMULATING" if exit_row is None else "PENDING_FINALIZATION",
            "mfe_points": None,
            "mae_points": None,
            "mfe_timestamp": None,
            "mae_timestamp": None,
            "counterfactual_ready": {"timebox": False, "trailing": False, "vwap_avwap": False, "atr": False},
        },
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
    }


def _base_event(metadata: LaneMetadata, event_type: str) -> dict[str, Any]:
    return {
        "schema_version": PERFORMANCE_SCHEMA_VERSION,
        "event_type": event_type,
        "lane_id": metadata.lane_id,
        "strategy_id": metadata.strategy_id,
        "strategy_family": metadata.strategy_family,
        "variant_id": metadata.variant_id,
        "entry_thesis": metadata.entry_thesis,
        "session_label": metadata.session_label,
        "symbol": metadata.symbol,
        "exit_policy": metadata.exit_policy,
    }


def _entry_fields(entry: Mapping[str, Any], metadata: LaneMetadata) -> dict[str, Any]:
    contract = _mapping(entry.get("contract"))
    row_metadata = _mapping(entry.get("metadata"))
    return {
        "local_symbol": entry.get("local_symbol") or contract.get("local_symbol"),
        "con_id": entry.get("con_id") or contract.get("con_id"),
        "lifecycle_id": entry.get("lifecycle_id"),
        "trade_id": _trade_id_from_entry(entry),
        "side": _entry_side(entry),
        "qty": str(entry.get("quantity") or entry.get("qty") or "1"),
        "entry_time": _iso(_entry_time(entry)),
        "entry_price": _str_decimal(_entry_price(entry)),
        "entry_order_id": entry.get("broker_order_id") or entry.get("order_id"),
        "entry_perm_id": entry.get("perm_id"),
        "entry_exec_id": entry.get("exec_id"),
        "exit_policy": entry.get("managed_exit_policy_id") or row_metadata.get("managed_exit_policy_id") or metadata.exit_policy,
    }


def _exit_fields(exit_row: Mapping[str, Any]) -> dict[str, Any]:
    exit_time = _exit_time(exit_row)
    return {
        "exit_time": _iso(exit_time),
        "exit_price": _str_decimal(_exit_price(exit_row)),
        "exit_order_id": exit_row.get("order_id"),
        "exit_perm_id": exit_row.get("perm_id"),
        "exit_exec_id": exit_row.get("exec_id"),
    }


def _performance_fields(
    *,
    entry: Mapping[str, Any],
    exit_row: Mapping[str, Any] | None,
    metadata: LaneMetadata,
    phase1_root: Path,
    mark_time: datetime,
) -> dict[str, Any]:
    entry_time = _entry_time(entry)
    exit_time = _exit_time(exit_row) if exit_row else None
    end_time = exit_time or mark_time
    entry_price = _entry_price(entry)
    exit_price = _exit_price(exit_row) if exit_row else None
    side = str(_entry_side(entry) or "").upper()
    qty = _decimal(entry.get("quantity") or entry.get("qty")) or Decimal("1")
    multiplier = _contract_multiplier(entry)
    high, low, latest_close, candle_ref = _candle_extremes(
        phase1_root=phase1_root,
        symbol=metadata.symbol,
        start=entry_time,
        end=end_time,
    )
    mfe, mae = _mfe_mae(side=side, entry_price=entry_price, high=high, low=low)
    mark_price = exit_price or latest_close
    pnl_points = _pnl_points(side=side, entry_price=entry_price, exit_price=exit_price)
    mtm_points = _pnl_points(side=side, entry_price=entry_price, exit_price=mark_price)
    hold_seconds = int((end_time - entry_time).total_seconds()) if entry_time and end_time else None
    result = {
        "hold_seconds": hold_seconds,
        "mfe_points": _str_decimal(mfe),
        "mae_points": _str_decimal(mae),
        "mfe_mae_status": "AVAILABLE" if mfe is not None and mae is not None else "NO_PHASE1_CANDLE_COVERAGE",
        "mark_price": _str_decimal(mark_price),
        "mark_to_market_pnl_points": _str_decimal(mtm_points),
        "mark_to_market_pnl_currency": _str_decimal(_money(mtm_points, multiplier, qty)),
        "phase1_candle_source": str(candle_ref) if candle_ref else None,
    }
    if exit_row is not None:
        result["realized_pnl_points"] = _str_decimal(pnl_points)
        result["realized_pnl_currency"] = _str_decimal(_money(pnl_points, multiplier, qty))
    else:
        result["realized_pnl_points"] = None
        result["realized_pnl_currency"] = None
    return result


def _mfe_mae(
    *,
    side: str,
    entry_price: Decimal | None,
    high: Decimal | None,
    low: Decimal | None,
) -> tuple[Decimal | None, Decimal | None]:
    if entry_price is None or high is None or low is None:
        return None, None
    if side == "SHORT":
        return entry_price - low, entry_price - high
    return high - entry_price, low - entry_price


def _pnl_points(*, side: str, entry_price: Decimal | None, exit_price: Decimal | None) -> Decimal | None:
    if entry_price is None or exit_price is None:
        return None
    if side == "SHORT":
        return entry_price - exit_price
    return exit_price - entry_price


def _money(points: Decimal | None, multiplier: Decimal | None, qty: Decimal | None) -> Decimal | None:
    if points is None or multiplier is None:
        return None
    return points * multiplier * (qty or Decimal("1"))


def _candle_extremes(
    *,
    phase1_root: Path,
    symbol: str,
    start: datetime | None,
    end: datetime | None,
) -> tuple[Decimal | None, Decimal | None, Decimal | None, Path | None]:
    if start is None or end is None:
        return None, None, None, None
    path = phase1_root / symbol / "1m" / "latest_runtime_candles.json"
    payload = _read_json(path)
    bars = payload.get("bars")
    if not isinstance(bars, list):
        return None, None, None, path
    highs: list[Decimal] = []
    lows: list[Decimal] = []
    latest_close: Decimal | None = None
    for bar in bars:
        if not isinstance(bar, Mapping):
            continue
        bar_end = _parse_ts(bar.get("bar_end") or bar.get("timestamp"))
        if bar_end is None or bar_end < start or bar_end > end:
            continue
        high = _decimal(bar.get("high"))
        low = _decimal(bar.get("low"))
        close = _decimal(bar.get("close"))
        if high is not None:
            highs.append(high)
        if low is not None:
            lows.append(low)
        if close is not None:
            latest_close = close
    return (max(highs) if highs else None, min(lows) if lows else None, latest_close, path)


def _bar_window_for_timestamp(path: Path, timestamp: datetime | None) -> dict[str, str | None]:
    if timestamp is None:
        return {"bar_start": None, "bar_end": None}
    payload = _read_json(path)
    bars = payload.get("bars")
    if not isinstance(bars, list):
        return {"bar_start": None, "bar_end": None}
    for bar in bars:
        if not isinstance(bar, Mapping):
            continue
        start = _parse_ts(bar.get("bar_start"))
        end = _parse_ts(bar.get("bar_end") or bar.get("timestamp"))
        if end is None:
            continue
        if start and start <= timestamp <= end:
            return {"bar_start": _iso(start), "bar_end": _iso(end)}
        if not start and end >= timestamp:
            return {"bar_start": None, "bar_end": _iso(end)}
    return {"bar_start": None, "bar_end": None}


def _stable_path_capture_id(*parts: Any) -> str:
    digest = hashlib.sha256("|".join(str(part or "") for part in parts).encode("utf-8")).hexdigest()[:24]
    return f"trade_path_capture_{digest}"


def _match_exit(
    entry: Mapping[str, Any],
    exits: Sequence[Mapping[str, Any]],
    used_indexes: set[int],
) -> tuple[int | None, Mapping[str, Any] | None]:
    entry_time = _entry_time(entry)
    if entry_time is None:
        return None, None
    lane_id = str(entry.get("lane_id") or "")
    symbol = str(entry.get("symbol") or entry.get("instrument") or "")
    side = str(_entry_side(entry) or "").upper()
    expected_action = "BUY" if side == "SHORT" else "SELL"
    entry_lifecycle = str(entry.get("lifecycle_id") or "")
    entry_trade = str(_trade_id_from_entry(entry) or "")
    exact_candidates: list[tuple[int, Mapping[str, Any], datetime]] = []
    fallback_candidates: list[tuple[int, Mapping[str, Any], datetime]] = []
    for index, row in enumerate(exits):
        if index in used_indexes:
            continue
        if not _same_contract_scope(entry, row, lane_id=lane_id, symbol=symbol):
            continue
        if str(row.get("action") or "").upper() != expected_action:
            continue
        ts = _exit_time(row)
        if ts and ts >= entry_time:
            if _exit_matches_exact_owner(row, lifecycle_id=entry_lifecycle, trade_id=entry_trade):
                exact_candidates.append((index, row, ts))
            else:
                fallback_candidates.append((index, row, ts))
    if exact_candidates:
        index, row, _ = sorted(exact_candidates, key=lambda item: item[2])[0]
        return index, row
    if fallback_candidates:
        index, row, _ = sorted(fallback_candidates, key=lambda item: item[2])[0]
        return index, row
    return None, None


def _blockers_before_entry(
    funnel_rows: Sequence[Mapping[str, Any]],
    *,
    lane_id: str,
    entry_time: datetime,
    limit: int = 10,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in reversed(funnel_rows):
        if str(row.get("lane_id") or "") != lane_id:
            continue
        ts = _parse_ts(row.get("timestamp"))
        if ts is None or ts >= entry_time:
            continue
        if row.get("pass_fail") != "FAIL":
            continue
        rows.append(
            {
                "timestamp": _iso(ts),
                "stage": row.get("stage"),
                "blocker_classification": row.get("blocker_classification"),
                "reason": row.get("reason"),
            }
        )
        if len(rows) >= limit:
            break
    rows.reverse()
    return rows


def _same_contract_scope(entry: Mapping[str, Any], exit_row: Mapping[str, Any], *, lane_id: str, symbol: str) -> bool:
    if str(exit_row.get("symbol") or "") != symbol:
        return False
    entry_con_id = str(entry.get("con_id") or _mapping(entry.get("contract")).get("con_id") or "")
    exit_con_id = str(exit_row.get("con_id") or "")
    if entry_con_id and exit_con_id and entry_con_id != exit_con_id:
        return False
    entry_local = str(entry.get("local_symbol") or _mapping(entry.get("contract")).get("local_symbol") or "")
    exit_local = str(exit_row.get("local_symbol") or "")
    if entry_local and exit_local and entry_local != exit_local:
        return False
    exit_lane = str(exit_row.get("lane_id") or "")
    return not exit_lane or exit_lane == lane_id


def _exit_matches_exact_owner(exit_row: Mapping[str, Any], *, lifecycle_id: str, trade_id: str) -> bool:
    exit_lifecycle = str(exit_row.get("lifecycle_id") or "")
    exit_trade = str(exit_row.get("trade_id") or "")
    if lifecycle_id and exit_lifecycle and lifecycle_id == exit_lifecycle:
        return True
    if trade_id and exit_trade and trade_id == exit_trade:
        return True
    return False


def _pairing_reason(entry: Mapping[str, Any], exit_row: Mapping[str, Any] | None) -> str:
    if exit_row is None:
        return "no_exit_fill_after_entry"
    if _exit_matches_exact_owner(
        exit_row,
        lifecycle_id=str(entry.get("lifecycle_id") or ""),
        trade_id=str(_trade_id_from_entry(entry) or ""),
    ):
        return "exact_lifecycle_or_trade_id_match"
    return "chronological_same_contract_risk_reducing_match"


def _exit_reason(exit_row: Mapping[str, Any] | None) -> str | None:
    if exit_row is None:
        return None
    metadata = _mapping(exit_row.get("metadata"))
    return (
        str(metadata.get("managed_exit_policy_id") or "").strip()
        or str(exit_row.get("reason") or "").strip()
        or str(exit_row.get("event_type") or "managed_exit_fill").strip()
    )


def _source_refs(*, entry: Mapping[str, Any] | None, exit_row: Mapping[str, Any] | None) -> dict[str, Any]:
    refs: dict[str, Any] = {
        "entry_fill": str(DEFAULT_FILLED_BRIDGE_RESULTS) if entry is not None else None,
        "exit_fill": str(DEFAULT_TRADE_REGISTRY_EVENTS) if exit_row is not None else None,
        "funnel": str(DEFAULT_FUNNEL_EVENTS),
    }
    if entry:
        refs["managed_lifecycle_report"] = entry.get("managed_lifecycle_report_path") or entry.get("paper_lifecycle_report_path")
        refs["position_management_manifest"] = entry.get("position_management_manifest_path")
        refs["entry_source_artifact"] = entry.get("source_artifact_path")
    if exit_row:
        refs["exit_source_artifact"] = exit_row.get("source_artifact_path")
    return refs


def _is_pilot_lane(lane_id: str) -> bool:
    normalized = lane_id.lower()
    if "active_participation" not in normalized:
        return False
    if normalized.startswith(("mgc_globex_", "gc_globex_", "es_us_", "nq_us_")):
        return True
    if normalized.startswith(("mbt_", "met_")):
        return True
    return False


def _is_supported_active_participation_lane(lane_id: str) -> bool:
    normalized = lane_id.lower()
    return "active_participation" in normalized and normalized.endswith(("_long", "_short"))


def _lane_metadata_from_lane_id(lane_id: str) -> LaneMetadata | None:
    parts = lane_id.split("_")
    if len(parts) < 4:
        return None
    symbol = parts[0].upper()
    side_bias = parts[-1].upper() if parts[-1] in {"long", "short"} else None
    session = _session_from_lane_id(lane_id)
    session_token = _strategy_session_token(session)
    side_token = "LONG" if side_bias == "LONG" else "SHORT" if side_bias == "SHORT" else "UNKNOWN"
    strategy_id = f"PAPER_ACTIVE_EVIDENCE_{symbol}_{session_token}_PARTICIPATION_{side_token}_V1"
    return LaneMetadata(
        lane_id=lane_id,
        strategy_id=strategy_id,
        strategy_family="paper_active_evidence",
        variant_id=f"{lane_id}_v1",
        entry_thesis="generic_active_participation",
        session_label=session,
        symbol=symbol,
        side_bias=side_bias,
        exit_policy=_default_exit_policy(session),
    )


def _session_from_lane_id(lane_id: str) -> str:
    normalized = lane_id.lower()
    if "_globex_" in normalized:
        return "GLOBEX"
    if "_london_open_" in normalized:
        return "LONDON_OPEN"
    if "_london_late_" in normalized:
        return "LONDON_LATE"
    if "_us_" in normalized:
        return "US"
    return "UNKNOWN"


def _strategy_session_token(session_label: str) -> str:
    return {
        "GLOBEX": "GLOBEX",
        "US": "US",
        "LONDON_OPEN": "LONDON_OPEN",
        "LONDON_LATE": "LONDON_LATE",
    }.get(session_label, session_label)


def _default_exit_policy(session_label: str) -> str:
    if session_label == "US":
        return "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"
    return "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"


def _active_lane_ids(payload: Mapping[str, Any]) -> list[str]:
    raw = payload.get("active_lane_ids") or payload.get("lane_ids") or []
    if not isinstance(raw, list):
        return []
    return [str(item) for item in raw if str(item or "").strip()]


def _strategy_ids(payload: Mapping[str, Any]) -> list[str]:
    raw = payload.get("enabled_strategy_ids") or []
    if not isinstance(raw, list):
        return []
    return [str(item) for item in raw if str(item or "").strip()]


def _is_entry_fill(row: Mapping[str, Any]) -> bool:
    return row.get("bridge_order_status") == "FILLED" and str(row.get("intent_type") or "") in ENTRY_INTENT_TYPES


def _is_exit_fill(row: Mapping[str, Any]) -> bool:
    return str(row.get("event_type") or "") in EXIT_EVENT_TYPES or "EXIT_FILL" in str(row.get("event_type") or "")


def _is_backfilled_managed_exit_fill(row: Mapping[str, Any]) -> bool:
    metadata = _mapping(row.get("metadata"))
    return metadata.get("source") == "track_b_managed_exit_fill_registry_backfill"


def _trade_id_from_entry(entry: Mapping[str, Any]) -> str | None:
    trade_id = entry.get("trade_id")
    if trade_id:
        return str(trade_id)
    lifecycle_id = str(entry.get("lifecycle_id") or "")
    if not lifecycle_id:
        return None
    return "trade_" + lifecycle_id.replace("|", "_").replace(":", "_")


def _entry_time(entry: Mapping[str, Any]) -> datetime | None:
    return _parse_ts(
        entry.get("fill_timestamp")
        or entry.get("generated_at")
        or entry.get("created_at")
        or entry.get("decision_bar_timestamp")
    )


def _exit_time(exit_row: Mapping[str, Any] | None) -> datetime | None:
    if exit_row is None:
        return None
    return _parse_ts(exit_row.get("generated_at") or exit_row.get("filled_at"))


def _entry_price(entry: Mapping[str, Any]) -> Decimal | None:
    return _decimal(entry.get("fill_price") or entry.get("price"))


def _exit_price(exit_row: Mapping[str, Any] | None) -> Decimal | None:
    if exit_row is None:
        return None
    return _decimal(exit_row.get("price") or _mapping(exit_row.get("metadata")).get("price"))


def _entry_side(entry: Mapping[str, Any]) -> str | None:
    side = str(entry.get("position_side") or entry.get("side") or "").upper()
    if side in {"LONG", "SHORT"}:
        return side
    return _side_from_intent(entry.get("intent_type") or _mapping(entry.get("metadata")).get("intent_type"))


def _side_from_intent(intent_type: Any) -> str | None:
    text = str(intent_type or "").upper()
    if text == "BUY_TO_OPEN":
        return "LONG"
    if text in {"SELL_TO_OPEN", "SELL_SHORT"}:
        return "SHORT"
    return None


def _contract_multiplier(entry: Mapping[str, Any]) -> Decimal | None:
    contract = _mapping(entry.get("contract"))
    return _decimal(contract.get("multiplier")) or _default_multiplier(str(entry.get("symbol") or entry.get("instrument") or ""))


def _default_multiplier(symbol: str) -> Decimal | None:
    multipliers = {
        "MGC": "10",
        "GC": "100",
        "ES": "50",
        "NQ": "20",
        "MES": "5",
        "MNQ": "2",
        "MBT": "0.1",
        "MET": "0.1",
        "MSL": "0.1",
        "ZT": "2000",
        "ZF": "1000",
        "ZN": "1000",
        "ZB": "1000",
    }
    return _decimal(multipliers.get(symbol.upper()))


def _decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _str_decimal(value: Any) -> str | None:
    decimal = value if isinstance(value, Decimal) else _decimal(value)
    if decimal is None:
        return None
    return format(decimal.normalize(), "f")


def _decimal_sum(events: Iterable[Mapping[str, Any]], key: str) -> str:
    total = Decimal("0")
    for event in events:
        value = _decimal(event.get(key))
        if value is not None:
            total += value
    return _str_decimal(total) or "0"


def _parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
    except FileNotFoundError:
        return []
    return rows


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(to_jsonable(dict(payload)), handle, sort_keys=True, indent=2)
            handle.write("\n")
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)
    return path


def _write_per_lane_summaries(lane_dir: Path, events: Sequence[Mapping[str, Any]]) -> None:
    by_lane: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for event in events:
        lane_id = str(event.get("lane_id") or "")
        if lane_id:
            by_lane[lane_id].append(event)
    for lane_id, lane_events in by_lane.items():
        latest = max(lane_events, key=lambda row: str(row.get("generated_at") or row.get("timestamp") or ""))
        _write_json(lane_dir / f"{lane_id}.json", {"latest_event": dict(latest), "event_count": len(lane_events)})


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--config-path", default=str(DEFAULT_CONFIG_IN_FORCE))
    parser.add_argument("--roster-path", default=str(DEFAULT_ROSTER))
    parser.add_argument("--filled-bridge-results-path", default=str(DEFAULT_FILLED_BRIDGE_RESULTS))
    parser.add_argument("--trade-registry-events-path", default=str(DEFAULT_TRADE_REGISTRY_EVENTS))
    parser.add_argument("--funnel-events-path", default=str(DEFAULT_FUNNEL_EVENTS))
    parser.add_argument("--phase1-root", default=str(DEFAULT_PHASE1_ROOT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--lane", action="append", dest="lanes", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    result = build_strategy_performance_attachment(
        repo_root=Path(args.repo_root),
        config_path=Path(args.config_path),
        roster_path=Path(args.roster_path),
        filled_bridge_results_path=Path(args.filled_bridge_results_path),
        trade_registry_events_path=Path(args.trade_registry_events_path),
        funnel_events_path=Path(args.funnel_events_path),
        phase1_root=Path(args.phase1_root),
        output_dir=Path(args.output_dir),
        include_lanes=args.lanes,
        write_artifacts=not args.dry_run,
    )
    print(
        json.dumps(
            {
                "classification": result.summary.get("classification"),
                "analytics_only": True,
                "events_written": result.events_written,
                "events_path": str(result.events_path),
                "summary_path": str(result.summary_path),
                "canonical_trades_path": str(result.canonical_trades_path),
                "pairing_summary_path": str(result.pairing_summary_path),
                "lane_count": result.summary.get("lane_count"),
                "event_counts": result.summary.get("event_counts"),
                "pairing": result.summary.get("pairing"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
