"""Research-only exit attribution and Exit Policy v2 shadow framework.

This module never submits, cancels, modifies, closes, flattens, or grants
broker/lifecycle authority. It reads Track B PAPER trade artifacts and Phase-1
candles to estimate whether exits captured favorable movement.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.models import to_jsonable
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic


DEFAULT_ATTRIBUTION_PATH = (
    Path("outputs") / "track_b_execution_core" / "diagnostics" / "latest_exit_attribution_review.json"
)
DEFAULT_FILL_RECONSTRUCTION_PATH = (
    Path("outputs") / "track_b_execution_core" / "diagnostics" / "latest_closed_trade_fill_reconstruction.json"
)
DEFAULT_FILL_RECONSTRUCTION_EVENTS_PATH = (
    Path("outputs") / "track_b_execution_core" / "diagnostics" / "closed_trade_fill_reconstruction.jsonl"
)
DEFAULT_SHADOW_PATH = (
    Path("outputs") / "track_b_execution_core" / "research_shadow" / "latest_exit_policy_v2_shadow.json"
)
DEFAULT_MARKDOWN_PATH = Path("docs") / "track_b_exit_attribution_and_policy_v2.md"
DEFAULT_LEDGER_PATH = (
    Path("outputs") / "track_b_execution_core" / "paper_trade_ledger" / "track_b_paper_trade_ledger.jsonl"
)

ENTRY_BAD = "ENTRY_BAD"
ENTRY_GOOD_EXIT_GOOD = "ENTRY_GOOD_EXIT_GOOD"
ENTRY_GOOD_EXIT_TOO_EARLY = "ENTRY_GOOD_EXIT_TOO_EARLY"
ENTRY_GOOD_EXIT_TOO_LATE = "ENTRY_GOOD_EXIT_TOO_LATE"
ENTRY_GOOD_ORDER_MANAGEMENT_BAD = "ENTRY_GOOD_ORDER_MANAGEMENT_BAD"
INCOMPLETE_FILL_EVIDENCE = "INCOMPLETE_FILL_EVIDENCE"
INCONCLUSIVE = "INCONCLUSIVE"

ALPHA_EXIT = "ALPHA_EXIT"
RISK_EXIT = "RISK_EXIT"
BUG_FIX_EXIT = "BUG_FIX_EXIT"
UNKNOWN_EXIT_INTENT = "UNKNOWN_EXIT_INTENT"

DIRECT_EXECUTION_FILL = "DIRECT_EXECUTION_FILL"
ORDER_STATUS_FILL = "ORDER_STATUS_FILL"
BROKER_POSITION_TRANSITION_INFERRED = "BROKER_POSITION_TRANSITION_INFERRED"
BROKER_FLAT_NO_PRICE = "BROKER_FLAT_NO_PRICE"
MISSING_PRICE = "MISSING_PRICE"

CONTAMINATION_FLAGS = (
    "leak_test_trade",
    "remediation_trade",
    "operator_supervised_cleanup",
    "lifecycle_bug_resolution",
    "duplicate_exit_resolution",
    "aggregate_close_repair",
    "broker_flat_reconciliation_cleanup",
    "missing_fill_price",
    "manual_intervention_required",
)

NO_BROKER_AUTHORITY_FLAGS = {
    "shadow_only": True,
    "submit_allowed": False,
    "broker_mutation_allowed": False,
    "lifecycle_authority": False,
    "not_order_authority": True,
    "not_lifecycle_authority": True,
    "live_money_eligible": False,
    "paper_proof_invoked": False,
    "dashboard_projection_consumed": False,
}


@dataclass(frozen=True)
class ExitPolicyV2Config:
    repo_root: Path
    attribution_path: Path = DEFAULT_ATTRIBUTION_PATH
    fill_reconstruction_path: Path = DEFAULT_FILL_RECONSTRUCTION_PATH
    fill_reconstruction_events_path: Path = DEFAULT_FILL_RECONSTRUCTION_EVENTS_PATH
    shadow_path: Path = DEFAULT_SHADOW_PATH
    markdown_path: Path = DEFAULT_MARKDOWN_PATH
    ledger_path: Path = DEFAULT_LEDGER_PATH

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def run_exit_attribution_policy_v2(
    *,
    config: ExitPolicyV2Config,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = now or datetime.now(UTC)
    trades = _closed_track_b_trades(config.resolve(config.ledger_path))
    reconstruction_payload = _reconstruct_closed_trade_fills(
        config=config,
        trades=trades,
        generated_at=actual_now,
    )
    reconstructed_by_trade_id = {
        str(row.get("trade_id") or row.get("lifecycle_id") or ""): row
        for row in reconstruction_payload.get("trades", [])
        if str(row.get("trade_id") or row.get("lifecycle_id") or "")
    }
    candle_cache: dict[str, list[dict[str, Any]]] = {}
    attributions = []
    shadows = []
    for trade in trades:
        instrument = str(trade.get("instrument_family") or trade.get("instrument") or trade.get("symbol") or "").upper()
        candles = candle_cache.setdefault(instrument, _load_phase1_5m_candles(config=config, instrument=instrument))
        trade_key = str(trade.get("trade_id") or trade.get("lifecycle_id") or "")
        attribution = _attribute_trade(
            trade=trade,
            reconstruction=reconstructed_by_trade_id.get(trade_key, {}),
            candles=candles,
        )
        attributions.append(attribution)
        shadows.append(_shadow_policy_alternatives(trade=trade, attribution=attribution, candles=candles))

    summary = _summary(attributions=attributions, shadows=shadows)
    attribution_payload = {
        "schema_version": "track_b_exit_attribution_review_v1",
        "generated_at": actual_now.isoformat(),
        "classification": _review_classification(attributions),
        **NO_BROKER_AUTHORITY_FLAGS,
        "closed_trade_count": len(trades),
        "attributed_trade_count": len(attributions),
        "summary": summary["attribution_summary"],
        "trades": attributions,
        "source_artifacts": {
            "trade_ledger": str(config.resolve(config.ledger_path)),
            "fill_reconstruction": str(config.resolve(config.fill_reconstruction_path)),
        },
    }
    shadow_payload = {
        "schema_version": "track_b_exit_policy_v2_shadow_v1",
        "generated_at": actual_now.isoformat(),
        "classification": _shadow_classification(shadows),
        **NO_BROKER_AUTHORITY_FLAGS,
        "shadow_policy_ids": [
            "CURRENT_ACTUAL_MANAGED_EXIT",
            "FIXED_3X5M_TIMEBOX",
            "PARTICIPATION_AWARE_HOLD_EXTENSION",
            "PROFIT_HARVEST_EXIT",
            "THESIS_FAILURE_PARTICIPATION_DECAY_EXIT",
        ],
        "summary": summary["shadow_summary"],
        "promotion_gates": _promotion_gates(),
        "shadow_evaluations": shadows,
    }
    write_json_atomic(config.resolve(config.fill_reconstruction_path), to_jsonable(reconstruction_payload))
    _append_jsonl(config.resolve(config.fill_reconstruction_events_path), reconstruction_payload)
    write_json_atomic(config.resolve(config.attribution_path), to_jsonable(attribution_payload))
    write_json_atomic(config.resolve(config.shadow_path), to_jsonable(shadow_payload))
    _write_markdown(config.resolve(config.markdown_path), attribution_payload, shadow_payload)
    return {
        "classification": "EXIT_ATTRIBUTION_POLICY_V2_READY",
        "attribution_path": str(config.resolve(config.attribution_path)),
        "fill_reconstruction_path": str(config.resolve(config.fill_reconstruction_path)),
        "shadow_path": str(config.resolve(config.shadow_path)),
        "markdown_path": str(config.resolve(config.markdown_path)),
        "closed_trade_count": len(trades),
        "complete_fill_evidence_count": reconstruction_payload["summary"]["complete_fill_evidence_count"],
        "incomplete_fill_evidence_count": reconstruction_payload["summary"]["incomplete_fill_evidence_count"],
        "attribution_summary": summary["attribution_summary"],
        "shadow_summary": summary["shadow_summary"],
        **NO_BROKER_AUTHORITY_FLAGS,
    }


def _closed_track_b_trades(path: Path) -> list[dict[str, Any]]:
    rows = _read_jsonl(path)
    closed: dict[str, dict[str, Any]] = {}
    for row in rows:
        if str(row.get("final_position_status") or "").upper() != "CLOSED_FLAT":
            continue
        if row.get("entry_fill_confirmed") is not True:
            continue
        trade_id = str(row.get("trade_id") or row.get("lifecycle_id") or "")
        if not trade_id:
            continue
        previous = closed.get(trade_id)
        if previous is None or str(row.get("created_at") or "") >= str(previous.get("created_at") or ""):
            closed[trade_id] = dict(row)
    return sorted(closed.values(), key=lambda item: str(item.get("entry_timestamp") or ""))


def _reconstruct_closed_trade_fills(
    *,
    config: ExitPolicyV2Config,
    trades: Sequence[Mapping[str, Any]],
    generated_at: datetime,
) -> dict[str, Any]:
    evidence_records = _load_reconstruction_evidence(config=config, trades=trades)
    rows = [
        _reconstruct_one_closed_trade(trade=trade, evidence_records=evidence_records)
        for trade in trades
    ]
    confidence_counts = Counter(str(row.get("exit_confidence") or MISSING_PRICE) for row in rows)
    intent_counts = Counter(str(row.get("exit_intent_category") or UNKNOWN_EXIT_INTENT) for row in rows)
    complete = sum(1 for row in rows if row.get("complete_fill_evidence") is True)
    incomplete = len(rows) - complete
    return {
        "schema_version": "track_b_closed_trade_fill_reconstruction_v1",
        "generated_at": generated_at.isoformat(),
        "classification": (
            "CLOSED_TRADE_FILL_RECONSTRUCTION_COMPLETE"
            if rows and incomplete == 0
            else "CLOSED_TRADE_FILL_RECONSTRUCTION_INCOMPLETE"
            if rows
            else "CLOSED_TRADE_FILL_RECONSTRUCTION_NO_TRADES"
        ),
        **NO_BROKER_AUTHORITY_FLAGS,
        "summary": {
            "closed_trade_count": len(rows),
            "complete_fill_evidence_count": complete,
            "incomplete_fill_evidence_count": incomplete,
            "exit_confidence_counts": dict(confidence_counts),
            "exit_intent_counts": dict(intent_counts),
            "alpha_exit_candidate_count": intent_counts.get(ALPHA_EXIT, 0),
            "bug_fix_exit_count": intent_counts.get(BUG_FIX_EXIT, 0),
            "risk_exit_count": intent_counts.get(RISK_EXIT, 0),
            "unknown_exit_intent_count": intent_counts.get(UNKNOWN_EXIT_INTENT, 0),
        },
        "trades": rows,
        "source_artifacts": {
            "trade_ledger": str(config.resolve(config.ledger_path)),
            "artifact_roots": [str(path) for path in _reconstruction_artifact_roots(config)],
        },
    }


def _reconstruct_one_closed_trade(
    *,
    trade: Mapping[str, Any],
    evidence_records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    matches = [trade, *_matching_evidence_records(trade=trade, records=evidence_records)]
    entry = _resolve_fill_leg(trade=trade, records=matches, phase="entry")
    exit_ = _resolve_fill_leg(trade=trade, records=matches, phase="exit")
    side = str(trade.get("side") or "").upper()
    quantity = _decimal(trade.get("quantity"))
    realized_points = _directional_points(
        side=side,
        entry_price=_decimal(entry.get("fill_price")),
        exit_price=_decimal(exit_.get("fill_price")),
    )
    realized_pnl = _decimal(trade.get("realized_pnl"))
    incomplete_reasons: list[str] = []
    if entry.get("fill_price") is None:
        incomplete_reasons.append("entry_fill_price_missing")
    if exit_.get("fill_price") is None:
        incomplete_reasons.append("exit_fill_price_missing")
    if entry.get("timestamp") is None:
        incomplete_reasons.append("entry_timestamp_missing")
    if exit_.get("timestamp") is None:
        incomplete_reasons.append("exit_timestamp_missing")
    complete = not incomplete_reasons
    contamination_flags = _exit_contamination_flags(trade=trade, records=matches, exit_leg=exit_)
    exit_intent_category = _exit_intent_category(trade=trade, records=matches, contamination_flags=contamination_flags)
    return {
        "trade_id": trade.get("trade_id"),
        "lifecycle_id": trade.get("lifecycle_id"),
        "strategy_id": trade.get("strategy_id"),
        "lane_id": trade.get("lane_id"),
        "instrument": str(trade.get("instrument_family") or trade.get("instrument") or trade.get("symbol") or "").upper(),
        "local_symbol": trade.get("local_symbol"),
        "account_id": trade.get("account_id"),
        "side": side,
        "quantity": _string(quantity),
        "entry_order_id": trade.get("entry_order_id") or trade.get("broker_order_id") or trade.get("order_id"),
        "entry_perm_id": trade.get("entry_perm_id") or trade.get("perm_id"),
        "exit_order_id": trade.get("exit_order_id") or trade.get("close_order_id"),
        "exit_perm_id": trade.get("exit_perm_id") or trade.get("close_perm_id"),
        "entry_timestamp": entry.get("timestamp"),
        "exit_timestamp": exit_.get("timestamp"),
        "entry_fill_price": entry.get("fill_price"),
        "exit_fill_price": exit_.get("fill_price"),
        "entry_confidence": entry.get("confidence"),
        "exit_confidence": exit_.get("confidence"),
        "exit_intent_category": exit_intent_category,
        "alpha_exit_quality_eligible": exit_intent_category == ALPHA_EXIT,
        "risk_exit_quality_eligible": exit_intent_category == RISK_EXIT,
        "contamination_flags": contamination_flags,
        "classification": "FILL_RECONSTRUCTION_COMPLETE" if complete else "FILL_RECONSTRUCTION_INCOMPLETE",
        "complete_fill_evidence": complete,
        "incomplete_reasons": incomplete_reasons,
        "realized_points": _string(realized_points),
        "realized_pnl": _string(realized_pnl) if realized_points is not None else None,
        "evidence_sources": sorted(
            {
                str(source)
                for leg in (entry, exit_)
                for source in leg.get("evidence_sources", [])
                if source
            }
        ),
        **NO_BROKER_AUTHORITY_FLAGS,
    }


def _resolve_fill_leg(
    *,
    trade: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    phase: str,
) -> dict[str, Any]:
    phase_records = _phase_records(trade=trade, records=records, phase=phase)
    for record in phase_records:
        price = _first_decimal(record, _phase_price_keys(phase))
        if price is None:
            continue
        return {
            "fill_price": _string(price),
            "timestamp": _iso(_first_time(record, _phase_time_keys(phase))),
            "confidence": _fill_confidence(record=record, phase=phase, has_price=True),
            "evidence_sources": _evidence_sources(record),
        }
    if phase == "exit" and _broker_flat_evidence(trade=trade, records=phase_records):
        return {
            "fill_price": None,
            "timestamp": _iso(_first_time_from_records(phase_records, _phase_time_keys(phase))),
            "confidence": BROKER_FLAT_NO_PRICE,
            "evidence_sources": sorted({source for record in phase_records for source in _evidence_sources(record)}),
        }
    return {
        "fill_price": None,
        "timestamp": _iso(_first_time_from_records(phase_records, _phase_time_keys(phase))),
        "confidence": MISSING_PRICE,
        "evidence_sources": sorted({source for record in phase_records for source in _evidence_sources(record)}),
    }


def _phase_records(
    *,
    trade: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    phase: str,
) -> list[Mapping[str, Any]]:
    ids = _phase_identifiers(trade, phase)
    selected: list[Mapping[str, Any]] = []
    for record in records:
        if record is trade:
            selected.append(record)
            continue
        if ids and _record_has_any_identifier(record, ids):
            selected.append(record)
            continue
        if not ids and _record_mentions_phase(record, phase):
            selected.append(record)
    return selected or [trade]


def _phase_identifiers(trade: Mapping[str, Any], phase: str) -> set[str]:
    if phase == "entry":
        keys = ("entry_order_id", "entry_perm_id", "broker_order_id", "order_id", "perm_id", "entry_intent_id")
    else:
        keys = ("exit_order_id", "exit_perm_id", "close_order_id", "close_perm_id", "managed_exit_order_id", "managed_exit_perm_id")
    return {str(trade.get(key)).strip() for key in keys if str(trade.get(key) or "").strip()}


def _record_has_any_identifier(record: Mapping[str, Any], identifiers: set[str]) -> bool:
    for key, value in _walk_key_values(record):
        normalized_key = str(key).lower()
        if not any(token in normalized_key for token in ("trade", "lifecycle", "intent", "order", "perm", "exec")):
            continue
        normalized_value = str(value).strip()
        if normalized_value in identifiers and _identifier_is_specific(normalized_value, normalized_key):
            return True
    return False


def _identifier_is_specific(value: str, key: str = "") -> bool:
    if not value:
        return False
    if any(token in key for token in ("trade", "lifecycle", "intent", "exec")):
        return len(value) >= 8
    if any(token in key for token in ("order", "perm")):
        return value.isdigit() and len(value) >= 2
    return len(value) >= 8


def _record_mentions_phase(record: Mapping[str, Any], phase: str) -> bool:
    text = json.dumps(to_jsonable(record), sort_keys=True).lower()
    return phase in text or ("close" in text if phase == "exit" else "open" in text)


def _phase_price_keys(phase: str) -> tuple[str, ...]:
    if phase == "entry":
        return (
            "entry_fill_price",
            "entry_price",
            "fill_price",
            "avg_fill_price",
            "average_fill_price",
            "average_price",
        )
    return (
        "exit_fill_price",
        "close_fill_price",
        "fill_price",
        "avg_fill_price",
        "average_fill_price",
        "average_price",
    )


def _phase_time_keys(phase: str) -> tuple[str, ...]:
    if phase == "entry":
        return ("entry_timestamp", "entry_fill_timestamp", "fill_timestamp", "filled_at", "created_at")
    return ("exit_timestamp", "exit_fill_timestamp", "close_timestamp", "fill_timestamp", "filled_at", "created_at")


def _fill_confidence(*, record: Mapping[str, Any], phase: str, has_price: bool) -> str:
    if not has_price:
        return MISSING_PRICE
    keys = ("exec_id", "execution_id", f"{phase}_exec_id")
    if any(str(record.get(key) or "").strip() for key in keys):
        return DIRECT_EXECUTION_FILL
    status = str(record.get("order_status") or record.get("status") or "").upper()
    if status in {"FILLED", "EXECUTED"} or record.get(f"{phase}_fill_confirmed") is True:
        return ORDER_STATUS_FILL
    return BROKER_POSITION_TRANSITION_INFERRED


def _broker_flat_evidence(*, trade: Mapping[str, Any], records: Sequence[Mapping[str, Any]]) -> bool:
    if str(trade.get("final_position_status") or "").upper() == "CLOSED_FLAT":
        return True
    for record in records:
        if str(record.get("final_position_status") or "").upper() == "CLOSED_FLAT":
            return True
        if str(record.get("paper_lifecycle_classification") or "").upper().endswith("CLOSED_FLAT"):
            return True
        if str(record.get("lifecycle_close_result") or "").upper() == "LIFECYCLE_CLOSED_FLAT":
            return True
        if str(record.get("position_match_classification") or "").upper() == "BROKER_AND_LIFECYCLE_FLAT":
            return True
    return False


def _load_reconstruction_evidence(
    *,
    config: ExitPolicyV2Config,
    trades: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    records = _read_jsonl(config.resolve(config.ledger_path))
    for path in _referenced_reconstruction_paths(config=config, trades=trades, ledger_rows=records):
        if path.suffix == ".jsonl":
            for row in _read_jsonl(path):
                row.setdefault("source_artifact_path", str(path))
                records.append(row)
            continue
        payload = _read_json(path)
        for row in _walk_mappings(payload)[:100]:
            item = dict(row)
            item.setdefault("source_artifact_path", str(path))
            records.append(item)
    return records


def _reconstruction_artifact_roots(config: ExitPolicyV2Config) -> list[Path]:
    roots = [
        config.repo_root / "outputs" / "track_b_execution_core" / "paper_trade_ledger",
        config.repo_root / "outputs" / "track_b_execution_core" / "managed_exit_order_resolution",
        config.repo_root / "outputs" / "track_b_execution_core" / "strategy_managed_paper_lifecycle",
        config.repo_root / "outputs" / "reports" / "track_b_paper_leak_test",
    ]
    return [root for root in roots if root.exists()]


def _referenced_reconstruction_paths(
    *,
    config: ExitPolicyV2Config,
    trades: Sequence[Mapping[str, Any]],
    ledger_rows: Sequence[Mapping[str, Any]],
) -> list[Path]:
    keys = (
        "filled_bridge_result_path",
        "filled_bridge_close_result_path",
        "paper_lifecycle_report_path",
        "strategy_paper_runner_report_path",
        "managed_exit_report_path",
        "managed_exit_order_resolution_path",
        "source_artifact_path",
    )
    paths: list[Path] = []
    seen: set[str] = set()
    for row in [*trades, *ledger_rows]:
        for key in keys:
            raw = str(row.get(key) or "").strip()
            if not raw:
                continue
            path = Path(raw)
            resolved = path if path.is_absolute() else config.repo_root / path
            if resolved.suffix not in {".json", ".jsonl"} or not resolved.exists():
                continue
            marker = str(resolved)
            if marker in seen:
                continue
            seen.add(marker)
            paths.append(resolved)
    return paths[:200]


def _matching_evidence_records(
    *,
    trade: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    identifiers = {
        str(trade.get(key)).strip()
        for key in (
            "trade_id",
            "lifecycle_id",
            "entry_intent_id",
            "entry_order_id",
            "entry_perm_id",
            "exit_order_id",
            "exit_perm_id",
            "broker_order_id",
            "order_id",
            "perm_id",
        )
        if str(trade.get(key) or "").strip()
    }
    if not identifiers:
        return []
    matches = []
    for record in records:
        if record is trade:
            continue
        if _record_has_any_identifier(record, identifiers):
            matches.append(record)
    return matches


def _exit_contamination_flags(
    *,
    trade: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    exit_leg: Mapping[str, Any],
) -> dict[str, bool]:
    text = _evidence_text([trade, *records])
    flags = {
        "leak_test_trade": any(token in text for token in ("leak_test", "leak-test", "track_b_paper_leak_test")),
        "remediation_trade": any(
            token in text
            for token in (
                "remediation",
                "cleanup",
                "repair",
                "review_cleanup",
                "scoped_guardian",
                "guardian_remediation",
                "broker-flat reconciliation",
                "broker_flat_reconciliation",
            )
        ),
        "operator_supervised_cleanup": ("operator" in text or "manual" in text)
        and any(token in text for token in ("operator_authorized", "supervised cleanup", "manual_intervention")),
        "lifecycle_bug_resolution": any(token in text for token in ("lifecycle gap", "lifecycle_gap", "lifecycle ghost", "lifecycle_ghost"))
        and any(token in text for token in ("gap", "ghost", "adoption", "mismatch", "review", "cleanup", "bug")),
        "duplicate_exit_resolution": any(
            token in text for token in ("duplicate_close", "duplicate exit", "duplicate_exit", "overfill", "reverse_exposure", "unauthorized_reverse")
        ),
        "aggregate_close_repair": any(token in text for token in ("aggregate managed exit", "aggregate_close", "position_unit", "position-unit", "unit_count", "lifecycle_unit_count")),
        "broker_flat_reconciliation_cleanup": (
            str(exit_leg.get("confidence") or "") == BROKER_FLAT_NO_PRICE
            or "broker_flat" in text
            or "broker_and_lifecycle_flat" in text
            or "broker-flat" in text
        ),
        "missing_fill_price": exit_leg.get("fill_price") is None,
        "manual_intervention_required": "manual" in text or "review_required" in text or _any_review_required(records),
    }
    if any(token in text for token in ("test_mule", "execution_test", "debug_exit", "reserved_submit_")):
        flags["leak_test_trade"] = True
    return {key: bool(flags.get(key)) for key in CONTAMINATION_FLAGS}


def _exit_intent_category(
    *,
    trade: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    contamination_flags: Mapping[str, bool],
) -> str:
    bug_flags = {
        key
        for key, value in contamination_flags.items()
        if value
        and key
        not in {
            "missing_fill_price",
            "manual_intervention_required",
        }
    }
    if bug_flags:
        return BUG_FIX_EXIT
    text = _evidence_text([trade, *records])
    if any(token in text for token in ("hard_stop", "stop_loss", "guardrail", "emergency", "risk_control")):
        return RISK_EXIT
    if trade.get("managed_exit_policy_id") or trade.get("exit_profile_id"):
        return ALPHA_EXIT
    if any(token in text for token in ("timebox", "profit_harvest", "participation_decay", "thesis_failure")):
        return ALPHA_EXIT
    return UNKNOWN_EXIT_INTENT


def _evidence_text(records: Sequence[Mapping[str, Any]]) -> str:
    return " ".join(json.dumps(to_jsonable(record), sort_keys=True, default=str).lower() for record in records)


def _any_review_required(records: Sequence[Mapping[str, Any]]) -> bool:
    return any(record.get("review_required") is True for record in records)


def _attribute_trade(
    *,
    trade: Mapping[str, Any],
    reconstruction: Mapping[str, Any],
    candles: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    entry_time = _parse_time(reconstruction.get("entry_timestamp")) or _parse_time(trade.get("entry_timestamp"))
    exit_time = _parse_time(reconstruction.get("exit_timestamp")) or _parse_time(trade.get("exit_timestamp"))
    entry_price = _decimal(reconstruction.get("entry_fill_price")) or _decimal(trade.get("entry_fill_price"))
    exit_price = _decimal(reconstruction.get("exit_fill_price"))
    side = str(trade.get("side") or "").upper()
    trade_candles = _candles_between(candles, entry_time, exit_time)
    mfe_mae = _mfe_mae(side=side, entry_price=entry_price, candles=trade_candles)
    realized = _directional_points(side=side, entry_price=entry_price, exit_price=exit_price)
    giveback = None
    if mfe_mae["mfe"] is not None and realized is not None:
        giveback = max(Decimal("0"), mfe_mae["mfe"] - realized)
    classification, reasons = _classify_exit(
        realized=realized,
        mfe=mfe_mae["mfe"],
        mae=mfe_mae["mae"],
        giveback=giveback,
        candle_count=len(trade_candles),
        entry_price_known=entry_price is not None,
        exit_price_known=exit_price is not None,
        reconstruction=reconstruction,
    )
    return {
        "trade_id": trade.get("trade_id"),
        "lifecycle_id": trade.get("lifecycle_id"),
        "strategy_id": trade.get("strategy_id"),
        "lane_id": trade.get("lane_id"),
        "instrument": str(trade.get("instrument_family") or trade.get("instrument") or trade.get("symbol") or "").upper(),
        "local_symbol": trade.get("local_symbol"),
        "side": side,
        "quantity": trade.get("quantity"),
        "entry_time": _iso(entry_time),
        "entry_price": _string(entry_price),
        "exit_time": _iso(exit_time),
        "exit_price": _string(exit_price),
        "realized_points": _string(realized),
        "realized_pnl": trade.get("realized_pnl"),
        "mfe_points": _string(mfe_mae["mfe"]),
        "mae_points": _string(mfe_mae["mae"]),
        "giveback_from_mfe_points": _string(giveback),
        "mfe_capture_ratio": _ratio(realized, mfe_mae["mfe"]),
        "time_in_trade_minutes": _minutes_between(entry_time, exit_time),
        "actual_exit_profile": trade.get("managed_exit_policy_id") or trade.get("exit_profile_id"),
        "fill_reconstruction": {
            "classification": reconstruction.get("classification"),
            "entry_confidence": reconstruction.get("entry_confidence"),
            "exit_confidence": reconstruction.get("exit_confidence"),
            "complete_fill_evidence": reconstruction.get("complete_fill_evidence") is True,
            "incomplete_reasons": reconstruction.get("incomplete_reasons") or [],
            "evidence_sources": reconstruction.get("evidence_sources") or [],
        },
        "exit_intent_category": reconstruction.get("exit_intent_category") or UNKNOWN_EXIT_INTENT,
        "alpha_exit_quality_eligible": reconstruction.get("exit_intent_category") == ALPHA_EXIT,
        "risk_exit_quality_eligible": reconstruction.get("exit_intent_category") == RISK_EXIT,
        "contamination_flags": reconstruction.get("contamination_flags") or {key: False for key in CONTAMINATION_FLAGS},
        "classification": classification,
        "classification_reasons": reasons,
        "candle_window": {
            "available": bool(trade_candles),
            "bar_count": len(trade_candles),
            "first_bar_start": _iso(_bar_time(trade_candles[0], "bar_start")) if trade_candles else None,
            "last_bar_end": _iso(_bar_time(trade_candles[-1], "bar_end")) if trade_candles else None,
        },
        **NO_BROKER_AUTHORITY_FLAGS,
    }


def _shadow_policy_alternatives(*, trade: Mapping[str, Any], attribution: Mapping[str, Any], candles: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    entry_time = _parse_time(trade.get("entry_timestamp"))
    entry_price = _decimal(trade.get("entry_fill_price"))
    side = str(trade.get("side") or "").upper()
    exit_intent_category = str(attribution.get("exit_intent_category") or UNKNOWN_EXIT_INTENT)
    if exit_intent_category != ALPHA_EXIT:
        return {
            "trade_id": trade.get("trade_id"),
            "lifecycle_id": trade.get("lifecycle_id"),
            "strategy_id": trade.get("strategy_id"),
            "lane_id": trade.get("lane_id"),
            "instrument": str(trade.get("instrument_family") or trade.get("instrument") or trade.get("symbol") or "").upper(),
            "side": side,
            "entry_time": attribution.get("entry_time"),
            "actual_exit_profile": attribution.get("actual_exit_profile"),
            "actual_classification": attribution.get("classification"),
            "exit_intent_category": exit_intent_category,
            "excluded_from_alpha_exit_policy": True,
            "exclusion_reason": f"{exit_intent_category.lower()}_excluded_from_alpha_exit_metrics",
            "best_shadow_policy_id": None,
            "recommendation": f"EXCLUDED_{exit_intent_category}",
            "policies": [_policy_current_actual(trade=trade, attribution=attribution)],
            **NO_BROKER_AUTHORITY_FLAGS,
        }
    forward_candles = _candles_after(candles, entry_time, max_minutes=90)
    policies = [
        _policy_current_actual(trade=trade, attribution=attribution),
        _policy_fixed_timebox(side=side, entry_price=entry_price, candles=forward_candles, bars=3),
        _policy_participation_hold(side=side, entry_price=entry_price, candles=forward_candles),
        _policy_profit_harvest(side=side, entry_price=entry_price, candles=forward_candles),
        _policy_thesis_failure(side=side, entry_price=entry_price, candles=forward_candles),
    ]
    best = _best_policy(policies)
    return {
        "trade_id": trade.get("trade_id"),
        "lifecycle_id": trade.get("lifecycle_id"),
        "strategy_id": trade.get("strategy_id"),
        "lane_id": trade.get("lane_id"),
        "instrument": str(trade.get("instrument_family") or trade.get("instrument") or trade.get("symbol") or "").upper(),
        "side": side,
        "entry_time": attribution.get("entry_time"),
        "actual_exit_profile": attribution.get("actual_exit_profile"),
        "actual_classification": attribution.get("classification"),
        "exit_intent_category": exit_intent_category,
        "excluded_from_alpha_exit_policy": False,
        "best_shadow_policy_id": best.get("policy_id") if best else None,
        "recommendation": _policy_recommendation(best, attribution),
        "policies": policies,
        **NO_BROKER_AUTHORITY_FLAGS,
    }


def _policy_current_actual(*, trade: Mapping[str, Any], attribution: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "policy_id": "CURRENT_ACTUAL_MANAGED_EXIT",
        "classification": "ACTUAL_EXIT_OBSERVED",
        "exit_time": attribution.get("exit_time"),
        "exit_price": attribution.get("exit_price"),
        "net_points": attribution.get("realized_points"),
        "mfe_capture_ratio": attribution.get("mfe_capture_ratio"),
        "basis": "ledger_actual_exit_price" if attribution.get("exit_price") is not None else "broker_flat_close_without_fill_price",
        **NO_BROKER_AUTHORITY_FLAGS,
    }


def _policy_fixed_timebox(*, side: str, entry_price: Decimal | None, candles: Sequence[Mapping[str, Any]], bars: int) -> dict[str, Any]:
    if entry_price is None or len(candles) < bars:
        return _policy_unclear("FIXED_3X5M_TIMEBOX", "insufficient_forward_candles")
    candle = candles[bars - 1]
    exit_price = _decimal(candle.get("close"))
    return _policy_result("FIXED_3X5M_TIMEBOX", side, entry_price, exit_price, candle, f"close_after_{bars}_completed_5m_bars")


def _policy_participation_hold(*, side: str, entry_price: Decimal | None, candles: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if entry_price is None or len(candles) < 3:
        return _policy_unclear("PARTICIPATION_AWARE_HOLD_EXTENSION", "insufficient_forward_candles")
    prior_close: Decimal | None = None
    adverse_streak = 0
    selected = candles[min(len(candles), 12) - 1]
    for candle in candles[:12]:
        close = _decimal(candle.get("close"))
        if close is None:
            continue
        if prior_close is not None:
            favorable = close >= prior_close if side == "LONG" else close <= prior_close
            adverse_streak = 0 if favorable else adverse_streak + 1
            if adverse_streak >= 2:
                selected = candle
                break
        prior_close = close
    return _policy_result("PARTICIPATION_AWARE_HOLD_EXTENSION", side, entry_price, _decimal(selected.get("close")), selected, "hold_until_two_adverse_closes_or_60m")


def _policy_profit_harvest(*, side: str, entry_price: Decimal | None, candles: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if entry_price is None or not candles:
        return _policy_unclear("PROFIT_HARVEST_EXIT", "insufficient_forward_candles")
    peak = Decimal("0")
    selected = candles[min(len(candles), 12) - 1]
    threshold = _profit_threshold(str(candles[0].get("symbol") or ""))
    for candle in candles[:12]:
        high = _decimal(candle.get("high"))
        low = _decimal(candle.get("low"))
        close = _decimal(candle.get("close"))
        if high is None or low is None or close is None:
            continue
        favorable = (high - entry_price) if side == "LONG" else (entry_price - low)
        peak = max(peak, favorable)
        current = (close - entry_price) if side == "LONG" else (entry_price - close)
        if peak >= threshold and peak - current >= peak * Decimal("0.40"):
            selected = candle
            break
    return _policy_result("PROFIT_HARVEST_EXIT", side, entry_price, _decimal(selected.get("close")), selected, "exit_on_40pct_giveback_after_material_mfe")


def _policy_thesis_failure(*, side: str, entry_price: Decimal | None, candles: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if entry_price is None or not candles:
        return _policy_unclear("THESIS_FAILURE_PARTICIPATION_DECAY_EXIT", "insufficient_forward_candles")
    selected = candles[min(len(candles), 6) - 1]
    adverse_threshold = Decimal("8") if str(candles[0].get("symbol") or "").upper() == "MNQ" else Decimal("3")
    for candle in candles[:6]:
        low = _decimal(candle.get("low"))
        high = _decimal(candle.get("high"))
        if low is None or high is None:
            continue
        adverse = (entry_price - low) if side == "LONG" else (high - entry_price)
        if adverse >= adverse_threshold:
            selected = candle
            break
    return _policy_result("THESIS_FAILURE_PARTICIPATION_DECAY_EXIT", side, entry_price, _decimal(selected.get("close")), selected, "exit_on_adverse_excursion_or_30m")


def _policy_result(policy_id: str, side: str, entry_price: Decimal, exit_price: Decimal | None, candle: Mapping[str, Any], basis: str) -> dict[str, Any]:
    net = _directional_points(side=side, entry_price=entry_price, exit_price=exit_price)
    return {
        "policy_id": policy_id,
        "classification": "SHADOW_EXIT_SIMULATED" if net is not None else "SHADOW_EXIT_UNCLEAR",
        "exit_time": candle.get("bar_end") or candle.get("timestamp"),
        "exit_price": _string(exit_price),
        "net_points": _string(net),
        "basis": basis,
        **NO_BROKER_AUTHORITY_FLAGS,
    }


def _policy_unclear(policy_id: str, reason: str) -> dict[str, Any]:
    return {
        "policy_id": policy_id,
        "classification": "SHADOW_EXIT_UNCLEAR",
        "blocker": reason,
        "exit_time": None,
        "exit_price": None,
        "net_points": None,
        **NO_BROKER_AUTHORITY_FLAGS,
    }


def _summary(*, attributions: Sequence[Mapping[str, Any]], shadows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    class_counts = Counter(str(item.get("classification") or INCONCLUSIVE) for item in attributions)
    intent_counts = Counter(str(item.get("exit_intent_category") or UNKNOWN_EXIT_INTENT) for item in attributions)
    alpha_attributions = [item for item in attributions if item.get("exit_intent_category") == ALPHA_EXIT]
    risk_attributions = [item for item in attributions if item.get("exit_intent_category") == RISK_EXIT]
    bug_fix_attributions = [item for item in attributions if item.get("exit_intent_category") == BUG_FIX_EXIT]
    unknown_attributions = [item for item in attributions if item.get("exit_intent_category") == UNKNOWN_EXIT_INTENT]
    alpha_counts = Counter(str(item.get("classification") or INCONCLUSIVE) for item in alpha_attributions)
    risk_counts = Counter(str(item.get("classification") or INCONCLUSIVE) for item in risk_attributions)
    contamination_counts = Counter(
        flag
        for item in attributions
        for flag, enabled in (item.get("contamination_flags") or {}).items()
        if enabled
    )
    by_strategy: dict[str, Counter[str]] = defaultdict(Counter)
    for item in attributions:
        by_strategy[str(item.get("strategy_id") or "UNKNOWN")][str(item.get("classification") or INCONCLUSIVE)] += 1
    recommendations = Counter(str(item.get("recommendation") or "COLLECT_MORE_EVIDENCE") for item in shadows)
    return {
        "attribution_summary": {
            "all_closed_trade_count": len(attributions),
            "classification_counts": dict(class_counts),
            "exit_intent_counts": dict(intent_counts),
            "contamination_flag_counts": dict(contamination_counts),
            "alpha_exit_quality_eligible_count": len(alpha_attributions),
            "bug_fix_exit_count": len(bug_fix_attributions),
            "risk_exit_count": len(risk_attributions),
            "unknown_exit_intent_count": len(unknown_attributions),
            "alpha_exit_quality": {
                "eligible_trade_count": len(alpha_attributions),
                "classification_counts": dict(alpha_counts),
                "top_root_causes": _root_causes(alpha_counts),
            },
            "risk_exit_quality": {
                "eligible_trade_count": len(risk_attributions),
                "classification_counts": dict(risk_counts),
            },
            "bug_fix_remediation_inventory": _bug_fix_inventory(bug_fix_attributions),
            "by_strategy": {strategy: dict(counts) for strategy, counts in sorted(by_strategy.items())},
            "top_underperformance_root_causes": _root_causes(alpha_counts),
        },
        "shadow_summary": {
            "recommendation_counts": dict(recommendations),
            "best_policy_counts": dict(Counter(str(item.get("best_shadow_policy_id") or "NONE") for item in shadows)),
            "alpha_exit_shadow_evaluation_count": sum(1 for item in shadows if not item.get("excluded_from_alpha_exit_policy")),
            "excluded_shadow_evaluation_count": sum(1 for item in shadows if item.get("excluded_from_alpha_exit_policy")),
            "recommended_next_slice": "Collect more clean ALPHA_EXIT samples with timestamp-coherent candles before changing live exits.",
        },
    }


def _bug_fix_inventory(attributions: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for item in attributions:
        rows.append(
            {
                "trade_id": item.get("trade_id"),
                "lifecycle_id": item.get("lifecycle_id"),
                "strategy_id": item.get("strategy_id"),
                "instrument": item.get("instrument"),
                "side": item.get("side"),
                "contamination_flags": [
                    flag
                    for flag, enabled in (item.get("contamination_flags") or {}).items()
                    if enabled
                ],
                "classification": item.get("classification"),
            }
        )
    return rows


def _classify_exit(
    *,
    realized: Decimal | None,
    mfe: Decimal | None,
    mae: Decimal | None,
    giveback: Decimal | None,
    candle_count: int,
    entry_price_known: bool,
    exit_price_known: bool,
    reconstruction: Mapping[str, Any],
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if not entry_price_known or not exit_price_known:
        incomplete = list(reconstruction.get("incomplete_reasons") or [])
        if not incomplete:
            if not entry_price_known:
                incomplete.append("entry_fill_price_missing")
            if not exit_price_known:
                incomplete.append("exit_fill_price_missing")
        return INCOMPLETE_FILL_EVIDENCE, incomplete
    if not exit_price_known:
        return INCOMPLETE_FILL_EVIDENCE, ["exit_fill_price_missing"]
    if realized is None or mfe is None or mae is None or candle_count == 0:
        return INCONCLUSIVE, ["timestamp_coherent_candles_or_prices_missing"]
    if mfe <= Decimal("0") and realized < Decimal("0"):
        return ENTRY_BAD, ["no_favorable_excursion_and_realized_loss"]
    capture = realized / mfe if mfe > 0 else Decimal("0")
    if realized > 0 and capture >= Decimal("0.60"):
        return ENTRY_GOOD_EXIT_GOOD, ["captured_at_least_60pct_of_mfe"]
    if mfe > 0 and giveback is not None and giveback >= mfe * Decimal("0.50"):
        reasons.append("gave_back_at_least_half_of_mfe")
        return ENTRY_GOOD_EXIT_TOO_LATE, reasons
    if realized <= 0 and mfe > abs(mae or Decimal("0")):
        return ENTRY_GOOD_EXIT_TOO_LATE, ["positive_mfe_but_exit_failed_to_capture"]
    if realized > 0 and capture < Decimal("0.35"):
        return ENTRY_GOOD_EXIT_TOO_EARLY, ["small_capture_of_available_mfe"]
    return INCONCLUSIVE, ["mixed_or_small_sample_exit_evidence"]


def _root_causes(class_counts: Counter[str]) -> list[dict[str, Any]]:
    mapping = {
        INCOMPLETE_FILL_EVIDENCE: "Entry/exit fill reconstruction is incomplete; exit quality should not be judged until price/timing evidence exists.",
        ENTRY_GOOD_ORDER_MANAGEMENT_BAD: "Missing or incomplete close fill/price evidence prevents reliable P&L and exit-quality attribution.",
        ENTRY_GOOD_EXIT_TOO_LATE: "Trades showed favorable excursion but gave back too much before exit.",
        ENTRY_GOOD_EXIT_TOO_EARLY: "Trades exited with low MFE capture; participation-aware hold may be worth shadowing.",
        ENTRY_BAD: "Entry quality did not create favorable excursion.",
        INCONCLUSIVE: "Timestamp-coherent candle or fill evidence is incomplete.",
    }
    return [
        {"cause": mapping[key], "classification": key, "count": count}
        for key, count in class_counts.most_common()
        if key in mapping and count
    ]


def _promotion_gates() -> list[dict[str, Any]]:
    return [
        {"gate": "minimum_sample_size", "requirement": "At least 30 timestamp-coherent broker-effect closed trades per strategy/exit-family."},
        {"gate": "lower_giveback", "requirement": "Shadow policy reduces median giveback from MFE versus current exit."},
        {"gate": "improved_mfe_capture", "requirement": "Shadow policy improves realized/MFE capture without relying on hindsight-only prices."},
        {"gate": "no_worse_mae", "requirement": "MAE distribution is no worse than current exit for the same entry set."},
        {"gate": "no_lifecycle_failures", "requirement": "No added duplicate close, unmanaged order, modify, or lifecycle persistence failures."},
        {"gate": "explainable_rules_only", "requirement": "Rules use participation, MFE/giveback, thesis failure, and session/regime evidence; no black-box promotion."},
    ]


def _write_markdown(path: Path, attribution: Mapping[str, Any], shadow: Mapping[str, Any]) -> None:
    summary = attribution.get("summary") if isinstance(attribution.get("summary"), Mapping) else {}
    shadow_summary = shadow.get("summary") if isinstance(shadow.get("summary"), Mapping) else {}
    lines = [
        "# Track B Exit Attribution and Policy v2",
        "",
        "Status: research/shadow only. No broker, order, lifecycle, live-money, or paper_proof authority is granted.",
        "",
        "## A. All Closed Trades / Evidence Completeness",
        f"- Closed broker-effect trades reviewed: {attribution.get('closed_trade_count', 0)}",
        f"- Attribution classification counts: `{json.dumps(summary.get('classification_counts', {}), sort_keys=True)}`",
        f"- Shadow recommendation counts: `{json.dumps(shadow_summary.get('recommendation_counts', {}), sort_keys=True)}`",
        "",
        "## B. Exit Intent Classification",
        f"- Intent counts: `{json.dumps(summary.get('exit_intent_counts', {}), sort_keys=True)}`",
        f"- Contamination flags: `{json.dumps(summary.get('contamination_flag_counts', {}), sort_keys=True)}`",
        "",
        "## C. Bug-Fix / Remediation Exit Inventory",
        f"- BUG_FIX_EXIT count: `{summary.get('bug_fix_exit_count', 0)}`",
        "- These trades are excluded from alpha-exit quality metrics by default.",
        "",
        "## D. True Strategy Exit-Quality Analysis",
        f"- ALPHA_EXIT eligible count: `{summary.get('alpha_exit_quality_eligible_count', 0)}`",
        f"- ALPHA_EXIT classification counts: `{json.dumps((summary.get('alpha_exit_quality') or {}).get('classification_counts', {}), sort_keys=True)}`",
        "",
        "## E. Risk Exits",
        f"- RISK_EXIT count: `{summary.get('risk_exit_count', 0)}`",
        f"- RISK_EXIT classification counts: `{json.dumps((summary.get('risk_exit_quality') or {}).get('classification_counts', {}), sort_keys=True)}`",
        "",
        "## F. Recommendations",
    ]
    for item in summary.get("top_underperformance_root_causes", []) or []:
        lines.append(f"- {item.get('classification')}: {item.get('cause')} ({item.get('count')})")
    if not summary.get("top_underperformance_root_causes"):
        lines.append("- No clean alpha-exit underperformance recommendation yet; collect more eligible strategy exits.")
    lines.extend(
        [
            "",
            "## Shadow Policies",
            "- CURRENT_ACTUAL_MANAGED_EXIT",
            "- FIXED_3X5M_TIMEBOX",
            "- PARTICIPATION_AWARE_HOLD_EXTENSION",
            "- PROFIT_HARVEST_EXIT",
            "- THESIS_FAILURE_PARTICIPATION_DECAY_EXIT",
            "",
            "## Promotion Gates",
        ]
    )
    for gate in shadow.get("promotion_gates", []) or []:
        lines.append(f"- {gate.get('gate')}: {gate.get('requirement')}")
    lines.extend(
        [
            "",
            "## Recommended Next Slice",
            str(shadow_summary.get("recommended_next_slice") or "Collect more timestamp-coherent closed-trade evidence before changing live exits."),
            "",
            "## Safety",
            "- `submit_allowed=false`",
            "- `broker_mutation_allowed=false`",
            "- `lifecycle_authority=false`",
            "- Existing managed-exit roster behavior is unchanged.",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def _load_phase1_5m_candles(*, config: ExitPolicyV2Config, instrument: str) -> list[dict[str, Any]]:
    if not instrument:
        return []
    path = config.repo_root / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data" / instrument / "5m" / "latest_runtime_candles.json"
    payload = _read_json(path)
    rows = []
    for row in payload.get("bars") or payload.get("candles") or []:
        if isinstance(row, Mapping):
            rows.append({**dict(row), "symbol": instrument})
    return sorted(rows, key=lambda item: str(item.get("bar_start") or item.get("timestamp") or ""))


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return rows
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(to_jsonable(payload), sort_keys=True) + "\n")


def _walk_mappings(value: Any) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    if isinstance(value, Mapping):
        rows.append(value)
        for child in value.values():
            rows.extend(_walk_mappings(child))
    elif isinstance(value, list):
        for child in value:
            rows.extend(_walk_mappings(child))
    return rows


def _walk_values(value: Any) -> list[Any]:
    if isinstance(value, Mapping):
        values: list[Any] = []
        for key, child in value.items():
            values.append(key)
            values.extend(_walk_values(child))
        return values
    if isinstance(value, list):
        values = []
        for child in value:
            values.extend(_walk_values(child))
        return values
    return [value]


def _walk_key_values(value: Any) -> list[tuple[str, Any]]:
    if isinstance(value, Mapping):
        pairs: list[tuple[str, Any]] = []
        for key, child in value.items():
            if isinstance(child, Mapping) or isinstance(child, list):
                pairs.extend(_walk_key_values(child))
            else:
                pairs.append((str(key), child))
        return pairs
    if isinstance(value, list):
        pairs = []
        for child in value:
            pairs.extend(_walk_key_values(child))
        return pairs
    return []


def _first_decimal(record: Mapping[str, Any], keys: Sequence[str]) -> Decimal | None:
    for key in keys:
        value = _nested_value(record, key)
        parsed = _decimal(value)
        if parsed is not None:
            return parsed
    return None


def _first_time(record: Mapping[str, Any], keys: Sequence[str]) -> datetime | None:
    for key in keys:
        parsed = _parse_time(_nested_value(record, key))
        if parsed is not None:
            return parsed
    return None


def _first_time_from_records(records: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> datetime | None:
    for record in records:
        parsed = _first_time(record, keys)
        if parsed is not None:
            return parsed
    return None


def _nested_value(record: Mapping[str, Any], key: str) -> Any:
    if key in record:
        return record.get(key)
    for value in record.values():
        if isinstance(value, Mapping):
            nested = _nested_value(value, key)
            if _is_present(nested):
                return nested
        elif isinstance(value, list):
            for child in value:
                if isinstance(child, Mapping):
                    nested = _nested_value(child, key)
                    if _is_present(nested):
                        return nested
    return None


def _is_present(value: Any) -> bool:
    return value is not None and value != ""


def _evidence_sources(record: Mapping[str, Any]) -> list[str]:
    sources = [
        record.get("source_artifact_path"),
        record.get("filled_bridge_result_path"),
        record.get("filled_bridge_close_result_path"),
        record.get("paper_lifecycle_report_path"),
        record.get("source"),
    ]
    return [str(source) for source in sources if str(source or "").strip()]


def _candles_between(candles: Sequence[Mapping[str, Any]], start: datetime | None, end: datetime | None) -> list[Mapping[str, Any]]:
    if start is None or end is None:
        return []
    selected = []
    for candle in candles:
        bar_start = _bar_time(candle, "bar_start") or _bar_time(candle, "timestamp")
        bar_end = _bar_time(candle, "bar_end") or bar_start
        if bar_end is not None and bar_end <= start:
            continue
        if bar_start is not None and bar_start > end:
            continue
        selected.append(candle)
    return selected


def _candles_after(candles: Sequence[Mapping[str, Any]], start: datetime | None, *, max_minutes: int) -> list[Mapping[str, Any]]:
    if start is None:
        return []
    end = start + timedelta(minutes=max_minutes)
    return _candles_between(candles, start, end)


def _mfe_mae(*, side: str, entry_price: Decimal | None, candles: Sequence[Mapping[str, Any]]) -> dict[str, Decimal | None]:
    if entry_price is None or not candles:
        return {"mfe": None, "mae": None}
    mfe = Decimal("0")
    mae = Decimal("0")
    for candle in candles:
        high = _decimal(candle.get("high"))
        low = _decimal(candle.get("low"))
        if high is None or low is None:
            continue
        if side == "LONG":
            mfe = max(mfe, high - entry_price)
            mae = min(mae, low - entry_price)
        else:
            mfe = max(mfe, entry_price - low)
            mae = min(mae, entry_price - high)
    return {"mfe": mfe, "mae": mae}


def _directional_points(*, side: str, entry_price: Decimal | None, exit_price: Decimal | None) -> Decimal | None:
    if entry_price is None or exit_price is None:
        return None
    return exit_price - entry_price if side == "LONG" else entry_price - exit_price


def _best_policy(policies: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    scored = [(policy, _decimal(policy.get("net_points"))) for policy in policies if policy.get("policy_id") != "CURRENT_ACTUAL_MANAGED_EXIT"]
    scored = [(policy, net) for policy, net in scored if net is not None]
    if not scored:
        return None
    return max(scored, key=lambda item: item[1] or Decimal("-999999"))[0]


def _policy_recommendation(best: Mapping[str, Any] | None, attribution: Mapping[str, Any]) -> str:
    if attribution.get("exit_intent_category") != ALPHA_EXIT:
        return f"EXCLUDED_{attribution.get('exit_intent_category') or UNKNOWN_EXIT_INTENT}"
    if not best:
        return "COLLECT_MORE_FORWARD_EVIDENCE"
    if attribution.get("classification") in {ENTRY_GOOD_EXIT_TOO_EARLY, ENTRY_GOOD_EXIT_TOO_LATE, ENTRY_GOOD_ORDER_MANAGEMENT_BAD}:
        return "SHADOW_POLICY_COMPARISON_WORTH_COLLECTING"
    return "KEEP_LIVE_EXIT_UNCHANGED_COLLECT_SHADOW"


def _review_classification(attributions: Sequence[Mapping[str, Any]]) -> str:
    if not attributions:
        return "EXIT_ATTRIBUTION_NO_CLOSED_TRADES"
    alpha = [item for item in attributions if item.get("exit_intent_category") == ALPHA_EXIT]
    if not alpha:
        return "EXIT_ATTRIBUTION_NO_CLEAN_ALPHA_EXITS"
    if any(item.get("classification") in {ENTRY_GOOD_EXIT_TOO_EARLY, ENTRY_GOOD_EXIT_TOO_LATE, ENTRY_GOOD_ORDER_MANAGEMENT_BAD} for item in alpha):
        return "EXIT_ATTRIBUTION_REVIEW_ACTIONABLE_DIAGNOSTICS"
    return "EXIT_ATTRIBUTION_REVIEW_READY"


def _shadow_classification(shadows: Sequence[Mapping[str, Any]]) -> str:
    if not shadows:
        return "EXIT_POLICY_V2_SHADOW_NO_TRADES"
    eligible = [item for item in shadows if not item.get("excluded_from_alpha_exit_policy")]
    if not eligible:
        return "EXIT_POLICY_V2_SHADOW_NO_CLEAN_ALPHA_EXITS"
    if any(item.get("recommendation") == "SHADOW_POLICY_COMPARISON_WORTH_COLLECTING" for item in eligible):
        return "EXIT_POLICY_V2_SHADOW_RECOMMENDATIONS_READY"
    return "EXIT_POLICY_V2_SHADOW_COLLECT_MORE_EVIDENCE"


def _profit_threshold(symbol: str) -> Decimal:
    return Decimal("10") if symbol.upper() in {"MNQ", "NQ", "ES", "MES"} else Decimal("3")


def _ratio(numerator: Decimal | None, denominator: Decimal | None) -> str | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return _string(numerator / denominator)


def _minutes_between(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return round((end - start).total_seconds() / 60.0, 3)


def _bar_time(candle: Mapping[str, Any], key: str) -> datetime | None:
    return _parse_time(candle.get(key))


def _parse_time(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _decimal(value: Any) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _string(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value.normalize())


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Track B exit attribution and Exit Policy v2 shadow artifacts.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = run_exit_attribution_policy_v2(config=ExitPolicyV2Config(repo_root=args.repo_root.expanduser().resolve()))
    if args.json:
        print(json.dumps(to_jsonable(report), indent=2, sort_keys=True))
    else:
        print(json.dumps({"classification": report["classification"], "attribution_path": report["attribution_path"], "shadow_path": report["shadow_path"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
