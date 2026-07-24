"""RA8 live trade path accumulator.

This module is research/diagnostic only. It reads existing managed-position,
canonical-trade-record, and runtime candle artifacts, then persists trade-path
samples so future closed trades can retain entry-to-exit paths after rolling
candle buffers move on. It has no broker, runtime, strategy, Managed Exit, or
gate authority.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_CANONICAL_TRADE_RECORDS = DEFAULT_OUTPUT_ROOT / "strategy_performance" / "canonical_trade_records.jsonl"
DEFAULT_MANAGED_POSITIONS = DEFAULT_OUTPUT_ROOT / "managed_positions" / "latest_managed_positions.json"
DEFAULT_RUNTIME_CANDLE_ROOT = DEFAULT_OUTPUT_ROOT / "phase1_runtime_market_data"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research_analytics" / "live_trade_path_accumulator"

OPEN_ACCUMULATOR_JSONL = "open_trade_path_accumulator.jsonl"
FINALIZED_CAPTURE_JSONL = "finalized_trade_path_capture.jsonl"
STATUS_JSON = "ra8_path_accumulator_status.json"
CADENCE_STATUS_JSON = "ra8_path_accumulator_cadence_status.json"
CADENCE_LOCK = "ra8_path_accumulator_cadence.lock"
CONTRACT_MD = "ra8_path_accumulator_contract.md"
FINALIZATION_MD = "ra8_path_finalization_report.md"
GRACE_DIAGNOSIS_MD = "ra8b_finalization_grace_diagnosis.md"
GRACE_CONTRACT_MD = "ra8b_finalization_grace_contract.md"
REPAIR_REPORT_MD = "ra8b_repair_report.md"

OPEN_SCHEMA_VERSION = "ra8_open_trade_path_accumulator_v1"
FINALIZED_SCHEMA_VERSION = "ra8_finalized_trade_path_capture_v1"
STATUS_SCHEMA_VERSION = "ra8_path_accumulator_status_v1"

MAX_INTERNAL_GAP_SECONDS = 90
DEFAULT_FINALIZATION_GRACE_SECONDS = 120
DEFAULT_CADENCE_SECONDS = 60.0
CADENCE_STATUS_SCHEMA_VERSION = "ra8_path_accumulator_cadence_status_v1"

RA8_CADENCE_SUCCEEDED = "RA8_CADENCE_SUCCEEDED"
RA8_CADENCE_FAILED = "RA8_CADENCE_FAILED"
RA8_CADENCE_SKIPPED_LOCKED = "RA8_CADENCE_SKIPPED_LOCKED"


@dataclass(frozen=True)
class LiveTradePathAccumulatorResult:
    open_rows: list[dict[str, Any]]
    finalized_rows: list[dict[str, Any]]
    status: dict[str, Any]
    open_path: Path
    finalized_path: Path
    status_path: Path
    contract_path: Path
    finalization_report_path: Path
    grace_diagnosis_path: Path
    grace_contract_path: Path
    repair_report_path: Path


@dataclass(frozen=True)
class LiveTradePathAccumulatorCadenceResult:
    status: dict[str, Any]
    status_path: Path
    lock_path: Path


AccumulatorRunner = Callable[..., LiveTradePathAccumulatorResult]
SleepFunc = Callable[[float], None]


def run_live_trade_path_accumulator(
    *,
    managed_positions_path: Path = DEFAULT_MANAGED_POSITIONS,
    canonical_records_path: Path = DEFAULT_CANONICAL_TRADE_RECORDS,
    runtime_candle_root: Path = DEFAULT_RUNTIME_CANDLE_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    accumulate_open_paths: bool = True,
    finalize_closed_paths: bool = True,
    repair_finalized: bool = False,
    finalization_grace_seconds: int = DEFAULT_FINALIZATION_GRACE_SECONDS,
    now: datetime | str | None = None,
) -> LiveTradePathAccumulatorResult:
    generated_at = _coerce_now(now)
    output_dir.mkdir(parents=True, exist_ok=True)
    open_path = output_dir / OPEN_ACCUMULATOR_JSONL
    finalized_path = output_dir / FINALIZED_CAPTURE_JSONL
    status_path = output_dir / STATUS_JSON
    contract_path = output_dir / CONTRACT_MD
    finalization_report_path = output_dir / FINALIZATION_MD
    grace_diagnosis_path = output_dir / GRACE_DIAGNOSIS_MD
    grace_contract_path = output_dir / GRACE_CONTRACT_MD
    repair_report_path = output_dir / REPAIR_REPORT_MD

    previous_open = _read_jsonl(open_path)
    previous_finalized = _read_jsonl(finalized_path)
    managed_positions = _read_json(managed_positions_path)
    canonical_records = _read_jsonl(canonical_records_path)

    open_rows = previous_open
    accumulated_count = 0
    if accumulate_open_paths:
        open_rows, accumulated_count = accumulate_open_trade_paths(
            previous_open,
            managed_positions=managed_positions,
            runtime_candle_root=runtime_candle_root,
            generated_at=generated_at,
            source_paths={
                "managed_positions": managed_positions_path,
                "runtime_candle_root": runtime_candle_root,
            },
        )

    finalized_rows = previous_finalized
    finalized_count = 0
    deferred_count = 0
    if finalize_closed_paths:
        finalized_rows, finalized_count, deferred_count = finalize_closed_trade_paths(
            open_rows,
            previous_finalized=previous_finalized,
            canonical_records=canonical_records,
            generated_at=generated_at,
            repair_finalized=repair_finalized,
            finalization_grace_seconds=finalization_grace_seconds,
            source_paths={
                "canonical_trade_records": canonical_records_path,
                "open_trade_path_accumulator": open_path,
            },
        )
    repaired_count = 0
    if repair_finalized:
        finalized_rows, repaired_count = repair_finalized_trade_paths(
            finalized_rows,
            runtime_candle_root=runtime_candle_root,
            generated_at=generated_at,
        )

    status = build_path_accumulator_status(
        open_rows=open_rows,
        finalized_rows=finalized_rows,
        canonical_records=canonical_records,
        generated_at=generated_at,
        accumulated_count=accumulated_count,
        finalized_count=finalized_count,
        deferred_count=deferred_count,
        repaired_count=repaired_count,
        finalization_grace_seconds=finalization_grace_seconds,
        source_paths={
            "managed_positions": managed_positions_path,
            "canonical_trade_records": canonical_records_path,
            "runtime_candle_root": runtime_candle_root,
            "open_trade_path_accumulator": open_path,
            "finalized_trade_path_capture": finalized_path,
        },
    )

    _write_jsonl(open_path, open_rows)
    _write_jsonl(finalized_path, finalized_rows)
    _write_json(status_path, status)
    contract_path.write_text(render_contract_markdown(), encoding="utf-8")
    finalization_report_path.write_text(render_finalization_report(status), encoding="utf-8")
    grace_diagnosis_path.write_text(render_grace_diagnosis(status, finalized_rows), encoding="utf-8")
    grace_contract_path.write_text(render_grace_contract(finalization_grace_seconds), encoding="utf-8")
    repair_report_path.write_text(render_repair_report(status), encoding="utf-8")
    return LiveTradePathAccumulatorResult(
        open_rows=open_rows,
        finalized_rows=finalized_rows,
        status=status,
        open_path=open_path,
        finalized_path=finalized_path,
        status_path=status_path,
        contract_path=contract_path,
        finalization_report_path=finalization_report_path,
        grace_diagnosis_path=grace_diagnosis_path,
        grace_contract_path=grace_contract_path,
        repair_report_path=repair_report_path,
    )


def run_live_trade_path_accumulator_cadence_once(
    *,
    managed_positions_path: Path = DEFAULT_MANAGED_POSITIONS,
    canonical_records_path: Path = DEFAULT_CANONICAL_TRADE_RECORDS,
    runtime_candle_root: Path = DEFAULT_RUNTIME_CANDLE_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    status_path: Path | None = None,
    lock_path: Path | None = None,
    finalization_grace_seconds: int = DEFAULT_FINALIZATION_GRACE_SECONDS,
    cadence_seconds: float = DEFAULT_CADENCE_SECONDS,
    now: datetime | str | None = None,
    runner: AccumulatorRunner | None = None,
) -> LiveTradePathAccumulatorCadenceResult:
    """Run one bounded out-of-process-friendly RA8 cadence pass.

    The cadence wrapper only coordinates lock/status behavior around the
    existing accumulator. It has no broker, runtime, strategy, or gate authority.
    """

    generated_at = _coerce_now(now)
    output_dir.mkdir(parents=True, exist_ok=True)
    actual_status_path = status_path or output_dir / CADENCE_STATUS_JSON
    actual_lock_path = lock_path or output_dir / CADENCE_LOCK
    previous_status = _read_json(actual_status_path)
    lock_handle = _try_acquire_lock(actual_lock_path, generated_at=generated_at)
    if lock_handle is None:
        status = _cadence_status(
            classification=RA8_CADENCE_SKIPPED_LOCKED,
            generated_at=generated_at,
            cadence_seconds=cadence_seconds,
            output_dir=output_dir,
            lock_path=actual_lock_path,
            previous_status=previous_status,
            duration_seconds=0.0,
            accumulator_status={},
            last_error="another_ra8_accumulator_pass_is_running",
        )
        _write_json(actual_status_path, status)
        return LiveTradePathAccumulatorCadenceResult(status=status, status_path=actual_status_path, lock_path=actual_lock_path)

    started = time.monotonic()
    try:
        actual_runner = runner or run_live_trade_path_accumulator
        try:
            result = actual_runner(
                managed_positions_path=managed_positions_path,
                canonical_records_path=canonical_records_path,
                runtime_candle_root=runtime_candle_root,
                output_dir=output_dir,
                accumulate_open_paths=True,
                finalize_closed_paths=True,
                repair_finalized=False,
                finalization_grace_seconds=finalization_grace_seconds,
                now=generated_at,
            )
            duration = round(time.monotonic() - started, 3)
            status = _cadence_status(
                classification=RA8_CADENCE_SUCCEEDED,
                generated_at=generated_at,
                cadence_seconds=cadence_seconds,
                output_dir=output_dir,
                lock_path=actual_lock_path,
                previous_status=previous_status,
                duration_seconds=duration,
                accumulator_status=result.status,
                last_error=None,
            )
        except Exception as exc:
            duration = round(time.monotonic() - started, 3)
            status = _cadence_status(
                classification=RA8_CADENCE_FAILED,
                generated_at=generated_at,
                cadence_seconds=cadence_seconds,
                output_dir=output_dir,
                lock_path=actual_lock_path,
                previous_status=previous_status,
                duration_seconds=duration,
                accumulator_status={},
                last_error=f"{type(exc).__name__}: {exc}",
            )
        _write_json(actual_status_path, status)
        return LiveTradePathAccumulatorCadenceResult(status=status, status_path=actual_status_path, lock_path=actual_lock_path)
    finally:
        _release_lock(actual_lock_path, lock_handle)


def run_live_trade_path_accumulator_cadence_service(
    *,
    managed_positions_path: Path = DEFAULT_MANAGED_POSITIONS,
    canonical_records_path: Path = DEFAULT_CANONICAL_TRADE_RECORDS,
    runtime_candle_root: Path = DEFAULT_RUNTIME_CANDLE_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    status_path: Path | None = None,
    lock_path: Path | None = None,
    finalization_grace_seconds: int = DEFAULT_FINALIZATION_GRACE_SECONDS,
    cadence_seconds: float = DEFAULT_CADENCE_SECONDS,
    max_iterations: int | None = None,
    sleep_func: SleepFunc = time.sleep,
    runner: AccumulatorRunner | None = None,
) -> LiveTradePathAccumulatorCadenceResult:
    iterations = 0
    latest: LiveTradePathAccumulatorCadenceResult | None = None
    while max_iterations is None or iterations < max_iterations:
        started = time.monotonic()
        latest = run_live_trade_path_accumulator_cadence_once(
            managed_positions_path=managed_positions_path,
            canonical_records_path=canonical_records_path,
            runtime_candle_root=runtime_candle_root,
            output_dir=output_dir,
            status_path=status_path,
            lock_path=lock_path,
            finalization_grace_seconds=finalization_grace_seconds,
            cadence_seconds=cadence_seconds,
            runner=runner,
        )
        iterations += 1
        if max_iterations is not None and iterations >= max_iterations:
            break
        sleep_for = max(1.0, float(cadence_seconds) - (time.monotonic() - started))
        sleep_func(sleep_for)
    if latest is None:
        return run_live_trade_path_accumulator_cadence_once(
            managed_positions_path=managed_positions_path,
            canonical_records_path=canonical_records_path,
            runtime_candle_root=runtime_candle_root,
            output_dir=output_dir,
            status_path=status_path,
            lock_path=lock_path,
            finalization_grace_seconds=finalization_grace_seconds,
            cadence_seconds=cadence_seconds,
            runner=runner,
        )
    return latest


def accumulate_open_trade_paths(
    previous_open_rows: Sequence[Mapping[str, Any]],
    *,
    managed_positions: Mapping[str, Any],
    runtime_candle_root: Path,
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    rows_by_key = {str(row.get("accumulator_key")): dict(row) for row in previous_open_rows if row.get("accumulator_key")}
    accumulated_count = 0
    for position in _open_managed_positions(managed_positions):
        trade = _open_trade_from_position(position)
        if not trade.get("entry_time") or not trade.get("instrument"):
            continue
        key = _accumulator_key(trade)
        existing = rows_by_key.get(key) or _new_open_accumulator_row(trade, generated_at=generated_at, source_paths=source_paths or {})
        candle_payload = _read_json(runtime_candle_root / str(trade["instrument"]).upper() / "1m" / "latest_runtime_candles.json")
        samples = _samples_for_trade(candle_payload.get("bars") or [], trade)
        merged_samples = _merge_samples(existing.get("path_samples") or [], samples)
        if len(merged_samples) > len(existing.get("path_samples") or []):
            accumulated_count += 1
        existing.update(
            {
                "updated_at": generated_at.isoformat(),
                "status": "OPEN_ACCUMULATING",
                "path_samples": merged_samples,
                "path_sample_count": len(merged_samples),
                "path_start_timestamp": _sample_start(merged_samples),
                "path_end_timestamp": _sample_end(merged_samples),
                "path_coverage_status": _coverage_status(entry_time=trade.get("entry_time"), exit_time=None, samples=merged_samples),
                "source_refs": {
                    **dict(existing.get("source_refs") or {}),
                    "runtime_candle_artifact": str(runtime_candle_root / str(trade["instrument"]).upper() / "1m" / "latest_runtime_candles.json"),
                },
            }
        )
        existing["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(existing))
        rows_by_key[key] = existing
    rows = sorted(rows_by_key.values(), key=lambda row: (str(row.get("entry_time") or ""), str(row.get("accumulator_key") or "")))
    return rows, accumulated_count


def finalize_closed_trade_paths(
    open_rows: Sequence[Mapping[str, Any]],
    *,
    previous_finalized: Sequence[Mapping[str, Any]],
    canonical_records: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    repair_finalized: bool = False,
    finalization_grace_seconds: int = DEFAULT_FINALIZATION_GRACE_SECONDS,
    source_paths: Mapping[str, Path | str] | None = None,
) -> tuple[list[dict[str, Any]], int, int]:
    finalized_by_key = {
        str(row.get("accumulator_key") or row.get("source_trade_id") or row.get("canonical_trade_record_id")): dict(row)
        for row in previous_finalized
        if row.get("accumulator_key") or row.get("source_trade_id") or row.get("canonical_trade_record_id")
    }
    open_index = _OpenAccumulatorIndex(open_rows)
    finalized_count = 0
    deferred_count = 0
    for record in _closed_canonical_records(canonical_records):
        open_row = open_index.find(record)
        if not open_row:
            continue
        key = str(open_row.get("accumulator_key") or _closed_record_key(record))
        was_existing = key in finalized_by_key
        if was_existing and not repair_finalized:
            continue
        candidate = _finalized_from_open_and_record(
            open_row=open_row,
            record=record,
            generated_at=generated_at,
            source_paths=source_paths or {},
        )
        if (
            candidate.get("path_coverage_status") == "PARTIAL_EXIT_MISSING"
            and _within_finalization_grace(candidate, generated_at=generated_at, finalization_grace_seconds=finalization_grace_seconds)
        ):
            deferred_count += 1
            continue
        finalized_by_key[key] = candidate
        if not was_existing:
            finalized_count += 1
    rows = sorted(finalized_by_key.values(), key=lambda row: (str(row.get("exit_time") or ""), str(row.get("accumulator_key") or "")))
    return rows, finalized_count, deferred_count


def repair_finalized_trade_paths(
    finalized_rows: Sequence[Mapping[str, Any]],
    *,
    runtime_candle_root: Path,
    generated_at: datetime,
) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    repaired_count = 0
    for row in finalized_rows:
        repaired = dict(row)
        if row.get("path_coverage_status") == "PARTIAL_EXIT_MISSING":
            instrument = str(row.get("instrument") or "").upper()
            candle_payload = _read_json(runtime_candle_root / instrument / "1m" / "latest_runtime_candles.json")
            trailing = _samples_for_closed_trade(candle_payload.get("bars") or [], row)
            merged = _merge_samples(row.get("path_samples") or [], trailing)
            if len(merged) > len(row.get("path_samples") or []):
                repaired.update(
                    {
                        "path_samples": merged,
                        "path_sample_count": len(merged),
                        "path_start_timestamp": _sample_start(merged),
                        "path_end_timestamp": _sample_end(merged),
                        "path_coverage_status": _coverage_status(
                            entry_time=row.get("entry_time"),
                            exit_time=row.get("exit_time"),
                            samples=merged,
                        ),
                        "repair_status": "REPAIRED_WITH_RUNTIME_CANDLES",
                        "repaired_at": generated_at.isoformat(),
                    }
                )
                metrics = _path_metrics(samples=merged, side=str(row.get("side") or ""), entry_price=_float_or_none(row.get("entry_price")))
                repaired.update(
                    {
                        "mfe": metrics.get("mfe"),
                        "mae": metrics.get("mae"),
                        "mfe_timestamp": metrics.get("mfe_timestamp"),
                        "mae_timestamp": metrics.get("mae_timestamp"),
                        "counterfactual_ready": {
                            "timebox": repaired.get("path_coverage_status") == "COMPLETE",
                            "trailing": repaired.get("path_coverage_status") == "COMPLETE"
                            and metrics.get("mfe") is not None
                            and metrics.get("mae") is not None,
                            "vwap_avwap": False,
                            "atr": False,
                        },
                    }
                )
                repaired["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(repaired))
                repaired_count += 1
        rows.append(repaired)
    return sorted(rows, key=lambda item: (str(item.get("exit_time") or ""), str(item.get("accumulator_key") or ""))), repaired_count


def build_path_accumulator_status(
    *,
    open_rows: Sequence[Mapping[str, Any]],
    finalized_rows: Sequence[Mapping[str, Any]],
    canonical_records: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    accumulated_count: int = 0,
    finalized_count: int = 0,
    deferred_count: int = 0,
    repaired_count: int = 0,
    finalization_grace_seconds: int = DEFAULT_FINALIZATION_GRACE_SECONDS,
    source_paths: Mapping[str, Path | str] | None = None,
) -> dict[str, Any]:
    closed_count = len(_closed_canonical_records(canonical_records))
    return {
        "schema_version": STATUS_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
        "source_paths": {key: str(value) for key, value in (source_paths or {}).items()},
        "classification": "ACCUMULATOR_READY",
        "open_accumulator_count": len(open_rows),
        "finalized_path_count": len(finalized_rows),
        "closed_canonical_record_count": closed_count,
        "accumulated_open_path_updates": accumulated_count,
        "newly_finalized_path_count": finalized_count,
        "deferred_finalization_count": deferred_count,
        "repaired_finalized_path_count": repaired_count,
        "finalization_grace_seconds": finalization_grace_seconds,
        "coverage": {
            "complete_finalized_count": sum(1 for row in finalized_rows if row.get("path_coverage_status") == "COMPLETE"),
            "partial_entry_missing_count": sum(1 for row in finalized_rows if row.get("path_coverage_status") == "PARTIAL_ENTRY_MISSING"),
            "partial_exit_missing_count": sum(1 for row in finalized_rows if row.get("path_coverage_status") == "PARTIAL_EXIT_MISSING"),
            "partial_internal_gap_count": sum(1 for row in finalized_rows if row.get("path_coverage_status") == "PARTIAL_INTERNAL_GAP"),
            "missing_source_count": sum(1 for row in finalized_rows if row.get("path_coverage_status") == "MISSING_SOURCE"),
            "open_accumulating_count": sum(1 for row in open_rows if row.get("status") == "OPEN_ACCUMULATING"),
        },
        "readiness": {
            "timebox_ready_count": sum(1 for row in finalized_rows if row.get("counterfactual_ready", {}).get("timebox") is True),
            "trailing_ready_count": sum(1 for row in finalized_rows if row.get("counterfactual_ready", {}).get("trailing") is True),
            "vwap_avwap_ready_count": sum(1 for row in finalized_rows if row.get("counterfactual_ready", {}).get("vwap_avwap") is True),
            "atr_ready_count": sum(1 for row in finalized_rows if row.get("counterfactual_ready", {}).get("atr") is True),
        },
    }


def _open_managed_positions(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    rows = payload.get("managed_positions") if isinstance(payload.get("managed_positions"), list) else []
    result: list[Mapping[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        if row.get("diagnostic_only") is True:
            continue
        classification = str(row.get("classification") or row.get("status") or row.get("position_state") or "").upper()
        if "CLOSED" in classification or "FLAT" in classification or "SUPERSEDED" in classification:
            continue
        qty = _float_or_none(row.get("aggregate_qty") or row.get("quantity"))
        if qty == 0:
            continue
        result.append(row)
    return result


def _open_trade_from_position(position: Mapping[str, Any]) -> dict[str, Any]:
    instrument = str(position.get("symbol") or position.get("instrument") or "").upper()
    side = str(position.get("side") or "").upper()
    aggregate_qty = _float_or_none(position.get("aggregate_qty"))
    if not side and aggregate_qty is not None:
        side = "LONG" if aggregate_qty > 0 else "SHORT"
    return {
        "source_trade_id": _str_or_none(position.get("source_trade_id") or position.get("trade_id")),
        "managed_position_id": _str_or_none(position.get("managed_position_id") or position.get("lifecycle_id")),
        "lifecycle_id": _str_or_none(position.get("lifecycle_id")),
        "instrument": instrument,
        "contract": _str_or_none(position.get("contract") or position.get("local_symbol") or position.get("symbol")),
        "side": side,
        "entry_time": _str_or_none(position.get("entry_time") or position.get("entry_timestamp")),
        "entry_price": _float_or_none(position.get("entry_price") or position.get("avg_entry_price")),
        "quantity": _float_or_none(position.get("quantity") or position.get("aggregate_qty")),
    }


def _new_open_accumulator_row(
    trade: Mapping[str, Any],
    *,
    generated_at: datetime,
    source_paths: Mapping[str, Path | str],
) -> dict[str, Any]:
    key = _accumulator_key(trade)
    return {
        "schema_version": OPEN_SCHEMA_VERSION,
        "accumulator_id": _stable_id("open_trade_path", key),
        "accumulator_key": key,
        "created_at": generated_at.isoformat(),
        "updated_at": generated_at.isoformat(),
        "status": "OPEN_ACCUMULATING",
        "source_trade_id": trade.get("source_trade_id"),
        "managed_position_id": trade.get("managed_position_id"),
        "lifecycle_id": trade.get("lifecycle_id"),
        "instrument": trade.get("instrument"),
        "contract": trade.get("contract"),
        "side": trade.get("side"),
        "entry_time": trade.get("entry_time"),
        "entry_price": trade.get("entry_price"),
        "quantity": trade.get("quantity"),
        "path_samples": [],
        "path_sample_count": 0,
        "path_start_timestamp": None,
        "path_end_timestamp": None,
        "path_coverage_status": "OPEN_ACCUMULATING",
        "source_refs": {key: str(value) for key, value in source_paths.items()},
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
    }


def _samples_for_trade(bars: Sequence[Any], trade: Mapping[str, Any]) -> list[dict[str, Any]]:
    entry = _parse_ts(trade.get("entry_time"))
    if entry is None:
        return []
    samples: list[dict[str, Any]] = []
    for bar in bars:
        if not isinstance(bar, Mapping):
            continue
        start = _parse_ts(bar.get("bar_start"))
        end = _parse_ts(bar.get("bar_end"))
        if end is None:
            continue
        if end < entry:
            continue
        samples.append(
            {
                "bar_start": start.isoformat() if start else None,
                "bar_end": end.isoformat(),
                "open": _float_or_none(bar.get("open")),
                "high": _float_or_none(bar.get("high")),
                "low": _float_or_none(bar.get("low")),
                "close": _float_or_none(bar.get("close")),
                "volume": _float_or_none(bar.get("volume")),
                "completed": bool(bar.get("completed", True)),
            }
        )
    return samples


def _samples_for_closed_trade(bars: Sequence[Any], trade: Mapping[str, Any]) -> list[dict[str, Any]]:
    entry = _parse_ts(trade.get("entry_time"))
    exit_ts = _parse_ts(trade.get("exit_time"))
    if entry is None or exit_ts is None:
        return []
    samples: list[dict[str, Any]] = []
    for bar in bars:
        if not isinstance(bar, Mapping):
            continue
        start = _parse_ts(bar.get("bar_start"))
        end = _parse_ts(bar.get("bar_end"))
        if end is None:
            continue
        if end < entry:
            continue
        if start is not None and start > exit_ts:
            continue
        samples.append(
            {
                "bar_start": start.isoformat() if start else None,
                "bar_end": end.isoformat(),
                "open": _float_or_none(bar.get("open")),
                "high": _float_or_none(bar.get("high")),
                "low": _float_or_none(bar.get("low")),
                "close": _float_or_none(bar.get("close")),
                "volume": _float_or_none(bar.get("volume")),
                "completed": bool(bar.get("completed", True)),
            }
        )
    return samples


def _merge_samples(existing: Sequence[Any], incoming: Sequence[Any]) -> list[dict[str, Any]]:
    by_end: dict[str, dict[str, Any]] = {}
    for sample in list(existing) + list(incoming):
        if not isinstance(sample, Mapping):
            continue
        end = sample.get("bar_end")
        if not end:
            continue
        by_end[str(end)] = dict(sample)
    return [by_end[key] for key in sorted(by_end)]


class _OpenAccumulatorIndex:
    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        self._rows = [dict(row) for row in rows]
        self._by_source: dict[str, Mapping[str, Any]] = {}
        self._by_lifecycle: dict[str, Mapping[str, Any]] = {}
        for row in self._rows:
            if row.get("source_trade_id"):
                self._by_source.setdefault(str(row.get("source_trade_id")), row)
            if row.get("lifecycle_id"):
                self._by_lifecycle.setdefault(str(row.get("lifecycle_id")), row)

    def find(self, record: Mapping[str, Any]) -> Mapping[str, Any]:
        trade_id = _str_or_none(record.get("source_trade_id") or record.get("trade_id"))
        if trade_id and trade_id in self._by_source:
            return self._by_source[trade_id]
        lifecycle_id = _str_or_none(record.get("lifecycle_id"))
        if lifecycle_id and lifecycle_id in self._by_lifecycle:
            return self._by_lifecycle[lifecycle_id]
        record_entry = _parse_ts(record.get("entry_time") or record.get("entry_timestamp"))
        record_symbol = str(record.get("symbol") or record.get("instrument") or "").upper()
        record_side = str(record.get("side") or "").upper()
        best: Mapping[str, Any] = {}
        best_delta: float | None = None
        for row in self._rows:
            if str(row.get("instrument") or "").upper() != record_symbol:
                continue
            if record_side and str(row.get("side") or "").upper() != record_side:
                continue
            row_entry = _parse_ts(row.get("entry_time"))
            if row_entry is None or record_entry is None:
                continue
            delta = abs((row_entry - record_entry).total_seconds())
            if delta <= 90 and (best_delta is None or delta < best_delta):
                best = row
                best_delta = delta
        return best


def _closed_canonical_records(rows: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    closed: list[Mapping[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        if _parse_ts(row.get("exit_time") or row.get("exit_timestamp") or row.get("closed_at") or row.get("close_timestamp")) is None:
            continue
        status = str(row.get("trade_status") or row.get("status") or row.get("lifecycle_status") or "").upper()
        if status and "CLOSED" not in status and "PAIRED" not in status:
            continue
        closed.append(row)
    return closed


def _finalized_from_open_and_record(
    *,
    open_row: Mapping[str, Any],
    record: Mapping[str, Any],
    generated_at: datetime,
    source_paths: Mapping[str, Path | str],
) -> dict[str, Any]:
    samples = [dict(sample) for sample in open_row.get("path_samples") or [] if isinstance(sample, Mapping)]
    entry_time = _str_or_none(record.get("entry_time") or record.get("entry_timestamp") or open_row.get("entry_time"))
    exit_time = _str_or_none(record.get("exit_time") or record.get("exit_timestamp") or record.get("closed_at") or record.get("close_timestamp"))
    side = str(record.get("side") or open_row.get("side") or "").upper()
    entry_price = _float_or_none(record.get("entry_price") or open_row.get("entry_price"))
    metrics = _path_metrics(samples=samples, side=side, entry_price=entry_price)
    status = _coverage_status(entry_time=entry_time, exit_time=exit_time, samples=samples)
    payload = {
        "schema_version": FINALIZED_SCHEMA_VERSION,
        "finalized_trade_path_capture_id": _stable_id("finalized_trade_path", open_row.get("accumulator_key"), record.get("trade_id"), entry_time, exit_time),
        "accumulator_key": open_row.get("accumulator_key"),
        "finalized_at": generated_at.isoformat(),
        "canonical_trade_record_id": record.get("trade_id") or record.get("source_trade_id"),
        "source_trade_id": record.get("source_trade_id") or record.get("trade_id") or open_row.get("source_trade_id"),
        "managed_position_id": open_row.get("managed_position_id"),
        "lifecycle_id": open_row.get("lifecycle_id") or record.get("lifecycle_id"),
        "instrument": record.get("symbol") or record.get("instrument") or open_row.get("instrument"),
        "contract": record.get("contract") or open_row.get("contract"),
        "side": side,
        "entry_time": entry_time,
        "exit_time": exit_time,
        "entry_price": entry_price,
        "exit_price": _float_or_none(record.get("exit_price") or open_row.get("exit_price")),
        "quantity": _float_or_none(record.get("quantity") or open_row.get("quantity")),
        "path_samples": samples,
        "path_sample_count": len(samples),
        "path_start_timestamp": _sample_start(samples),
        "path_end_timestamp": _sample_end(samples),
        "path_coverage_status": status,
        "mfe": metrics.get("mfe"),
        "mae": metrics.get("mae"),
        "mfe_timestamp": metrics.get("mfe_timestamp"),
        "mae_timestamp": metrics.get("mae_timestamp"),
        "counterfactual_ready": {
            "timebox": status == "COMPLETE",
            "trailing": status == "COMPLETE" and metrics.get("mfe") is not None and metrics.get("mae") is not None,
            "vwap_avwap": False,
            "atr": False,
        },
        "provenance": {
            "source_paths": {key: str(value) for key, value in source_paths.items()},
            "open_accumulator_id": open_row.get("accumulator_id"),
            "open_accumulator_fingerprint": open_row.get("deterministic_fingerprint"),
            "canonical_trade_record_id": record.get("trade_id") or record.get("source_trade_id"),
        },
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
    }
    payload["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(payload))
    return payload


def _coverage_status(*, entry_time: Any, exit_time: Any, samples: Sequence[Mapping[str, Any]]) -> str:
    if not samples:
        return "MISSING_SOURCE"
    if exit_time is None:
        return "OPEN_ACCUMULATING"
    entry = _parse_ts(entry_time)
    exit_ts = _parse_ts(exit_time)
    first_start = _parse_ts(samples[0].get("bar_start") or samples[0].get("bar_end"))
    last_end = _parse_ts(samples[-1].get("bar_end"))
    if entry is None or exit_ts is None or first_start is None or last_end is None:
        return "MISSING_SOURCE"
    if first_start > entry:
        return "PARTIAL_ENTRY_MISSING"
    if last_end < exit_ts:
        return "PARTIAL_EXIT_MISSING"
    if _has_internal_gap(samples):
        return "PARTIAL_INTERNAL_GAP"
    return "COMPLETE"


def _within_finalization_grace(
    row: Mapping[str, Any],
    *,
    generated_at: datetime,
    finalization_grace_seconds: int,
) -> bool:
    exit_ts = _parse_ts(row.get("exit_time"))
    if exit_ts is None:
        return False
    return (generated_at - exit_ts).total_seconds() < finalization_grace_seconds


def _has_internal_gap(samples: Sequence[Mapping[str, Any]]) -> bool:
    previous_end: datetime | None = None
    for sample in samples:
        end = _parse_ts(sample.get("bar_end"))
        if end is None:
            continue
        if previous_end is not None and (end - previous_end).total_seconds() > MAX_INTERNAL_GAP_SECONDS:
            return True
        previous_end = end
    return False


def _path_metrics(*, samples: Sequence[Mapping[str, Any]], side: str, entry_price: float | None) -> dict[str, Any]:
    if entry_price is None or not samples:
        return {"mfe": None, "mae": None, "mfe_timestamp": None, "mae_timestamp": None}
    best_favorable: float | None = None
    worst_adverse: float | None = None
    best_ts: str | None = None
    worst_ts: str | None = None
    for sample in samples:
        high = _float_or_none(sample.get("high"))
        low = _float_or_none(sample.get("low"))
        if high is None or low is None:
            continue
        if side == "SHORT":
            favorable = entry_price - low
            adverse = entry_price - high
        else:
            favorable = high - entry_price
            adverse = low - entry_price
        if best_favorable is None or favorable > best_favorable:
            best_favorable = favorable
            best_ts = _str_or_none(sample.get("bar_end"))
        if worst_adverse is None or adverse < worst_adverse:
            worst_adverse = adverse
            worst_ts = _str_or_none(sample.get("bar_end"))
    return {
        "mfe": _round(best_favorable),
        "mae": _round(worst_adverse),
        "mfe_timestamp": best_ts,
        "mae_timestamp": worst_ts,
    }


def render_contract_markdown() -> str:
    return """# RA8 Live Trade Path Accumulator Contract

The live trade path accumulator is a research-only retention layer. It reads
existing managed-position, canonical-trade-record, and runtime candle artifacts
and writes persistent path samples for open trades.

It does not call broker APIs, submit/cancel/modify orders, restart runtime
services, alter strategy behavior, or create trading gates.

Coverage states:

- COMPLETE
- PARTIAL_ENTRY_MISSING
- PARTIAL_EXIT_MISSING
- PARTIAL_INTERNAL_GAP
- MISSING_SOURCE
- OPEN_ACCUMULATING
"""


def render_finalization_report(status: Mapping[str, Any]) -> str:
    coverage = status.get("coverage", {})
    readiness = status.get("readiness", {})
    return "\n".join(
        [
            "# RA8 Path Finalization Report",
            "",
            f"- Open accumulator rows: `{status.get('open_accumulator_count')}`",
            f"- Finalized path rows: `{status.get('finalized_path_count')}`",
            f"- Newly finalized paths: `{status.get('newly_finalized_path_count')}`",
            f"- Deferred finalizations: `{status.get('deferred_finalization_count')}`",
            f"- Repaired finalized paths: `{status.get('repaired_finalized_path_count')}`",
            f"- Complete finalized paths: `{coverage.get('complete_finalized_count')}`",
            f"- Partial entry missing: `{coverage.get('partial_entry_missing_count')}`",
            f"- Partial exit missing: `{coverage.get('partial_exit_missing_count')}`",
            f"- Partial internal gap: `{coverage.get('partial_internal_gap_count')}`",
            f"- Timebox ready: `{readiness.get('timebox_ready_count')}`",
            f"- Trailing ready: `{readiness.get('trailing_ready_count')}`",
            "",
            "Diagnostic only. No production recommendation or trading gate is emitted.",
        ]
    ) + "\n"


def render_grace_diagnosis(status: Mapping[str, Any], finalized_rows: Sequence[Mapping[str, Any]]) -> str:
    partial_exit = [row for row in finalized_rows if row.get("path_coverage_status") == "PARTIAL_EXIT_MISSING"]
    lines = [
        "# RA8B Finalization Grace Diagnosis",
        "",
        f"- Finalization grace seconds: `{status.get('finalization_grace_seconds')}`",
        f"- Deferred finalizations: `{status.get('deferred_finalization_count')}`",
        f"- Existing partial-exit finalized paths: `{len(partial_exit)}`",
        "",
    ]
    for row in partial_exit[:10]:
        exit_ts = _parse_ts(row.get("exit_time"))
        end = _parse_ts(row.get("path_end_timestamp"))
        gap = (exit_ts - end).total_seconds() if exit_ts and end else None
        lines.extend(
            [
                f"## {row.get('source_trade_id')}",
                "",
                f"- Instrument: `{row.get('instrument')}`",
                f"- Entry: `{row.get('entry_time')}`",
                f"- Exit: `{row.get('exit_time')}`",
                f"- Last retained sample: `{row.get('path_end_timestamp')}`",
                f"- Exit minus last sample seconds: `{gap}`",
                "",
            ]
        )
    return "\n".join(lines) + "\n"


def render_grace_contract(finalization_grace_seconds: int) -> str:
    return f"""# RA8B Finalization Grace Contract

Closed trades whose accumulated path is missing only the exit candle are not
finalized immediately. Finalization is deferred until either:

- retained samples cover the exit timestamp, or
- `{finalization_grace_seconds}` seconds have elapsed after the exit timestamp.

Existing finalized records remain idempotent. Repair requires explicit
`repair-finalized-paths` or `--repair-finalized`.
"""


def render_repair_report(status: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# RA8B Repair Report",
            "",
            f"- Repaired finalized paths: `{status.get('repaired_finalized_path_count')}`",
            f"- Complete finalized paths: `{status.get('coverage', {}).get('complete_finalized_count')}`",
            f"- Partial exit missing paths: `{status.get('coverage', {}).get('partial_exit_missing_count')}`",
            "",
            "Repair is explicit and diagnostic-only.",
        ]
    ) + "\n"


def _accumulator_key(trade: Mapping[str, Any]) -> str:
    return "|".join(
        str(part or "")
        for part in (
            trade.get("source_trade_id"),
            trade.get("managed_position_id"),
            trade.get("instrument"),
            trade.get("contract"),
            trade.get("side"),
            trade.get("entry_time"),
        )
    )


def _closed_record_key(record: Mapping[str, Any]) -> str:
    return "|".join(str(record.get(key) or "") for key in ("source_trade_id", "trade_id", "symbol", "side", "entry_time"))


def _sample_start(samples: Sequence[Mapping[str, Any]]) -> str | None:
    if not samples:
        return None
    return _str_or_none(samples[0].get("bar_start") or samples[0].get("bar_end"))


def _sample_end(samples: Sequence[Mapping[str, Any]]) -> str | None:
    if not samples:
        return None
    return _str_or_none(samples[-1].get("bar_end"))


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _try_acquire_lock(path: Path, *, generated_at: datetime) -> int | None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        handle = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return None
    payload = {
        "schema_version": "ra8_path_accumulator_cadence_lock_v1",
        "created_at": generated_at.isoformat(),
        "pid": os.getpid(),
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
    }
    os.write(handle, (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8"))
    return handle


def _release_lock(path: Path, handle: int) -> None:
    try:
        os.close(handle)
    finally:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def _cadence_status(
    *,
    classification: str,
    generated_at: datetime,
    cadence_seconds: float,
    output_dir: Path,
    lock_path: Path,
    previous_status: Mapping[str, Any],
    duration_seconds: float,
    accumulator_status: Mapping[str, Any],
    last_error: str | None,
) -> dict[str, Any]:
    success = classification == RA8_CADENCE_SUCCEEDED
    previous_last_successful_run_at = previous_status.get("last_successful_run_at")
    previous_last_success_duration = previous_status.get("last_success_duration_seconds")
    previous_last_success_rows = previous_status.get("last_success_rows_updated")
    previous_last_success_open_count = previous_status.get("last_success_open_accumulator_count")
    previous_last_success_finalized_count = previous_status.get("last_success_finalized_path_count")
    rows_updated = _int_value(accumulator_status.get("accumulated_open_path_updates"))
    return {
        "schema_version": CADENCE_STATUS_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "classification": classification,
        "cadence_seconds": float(cadence_seconds),
        "recommended_cadence_reason": "source candles are 1m bars; 60s captures each completed bar without coupling to execution",
        "command": "python -m mgc_v05l.app.track_b_live_trade_path_accumulator service --cadence-seconds 60",
        "output_dir": str(output_dir),
        "lock_path": str(lock_path),
        "last_started_at": generated_at.isoformat(),
        "last_completed_at": generated_at.isoformat(),
        "last_duration_seconds": duration_seconds,
        "last_successful_run_at": generated_at.isoformat() if success else previous_last_successful_run_at,
        "last_success_duration_seconds": duration_seconds if success else previous_last_success_duration,
        "last_rows_updated": rows_updated,
        "last_success_rows_updated": rows_updated if success else previous_last_success_rows,
        "last_open_accumulator_count": _int_value(accumulator_status.get("open_accumulator_count")),
        "last_success_open_accumulator_count": _int_value(accumulator_status.get("open_accumulator_count"))
        if success
        else previous_last_success_open_count,
        "last_finalized_path_count": _int_value(accumulator_status.get("finalized_path_count")),
        "last_success_finalized_path_count": _int_value(accumulator_status.get("finalized_path_count"))
        if success
        else previous_last_success_finalized_count,
        "last_newly_finalized_path_count": _int_value(accumulator_status.get("newly_finalized_path_count")),
        "last_deferred_finalization_count": _int_value(accumulator_status.get("deferred_finalization_count")),
        "last_error": last_error,
        "accumulator_classification": accumulator_status.get("classification"),
        "failure_isolated_from_trading": True,
        "overlapping_runs_allowed": False,
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
    }


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _parse_ts(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _str_or_none(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _round(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 6)


def _stable_id(prefix: str, *parts: Any) -> str:
    digest = hashlib.sha256("|".join(str(part or "") for part in parts).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}_{digest}"


def _fingerprint_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in payload.items()
        if key not in {"created_at", "updated_at", "finalized_at", "deterministic_fingerprint"}
    }


def _fingerprint(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
