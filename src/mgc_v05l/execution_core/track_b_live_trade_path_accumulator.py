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
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_CANONICAL_TRADE_RECORDS = DEFAULT_OUTPUT_ROOT / "strategy_performance" / "canonical_trade_records.jsonl"
DEFAULT_MANAGED_POSITIONS = DEFAULT_OUTPUT_ROOT / "managed_positions" / "latest_managed_positions.json"
DEFAULT_RUNTIME_CANDLE_ROOT = DEFAULT_OUTPUT_ROOT / "phase1_runtime_market_data"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research_analytics" / "live_trade_path_accumulator"

OPEN_ACCUMULATOR_JSONL = "open_trade_path_accumulator.jsonl"
FINALIZED_CAPTURE_JSONL = "finalized_trade_path_capture.jsonl"
STATUS_JSON = "ra8_path_accumulator_status.json"
CONTRACT_MD = "ra8_path_accumulator_contract.md"
FINALIZATION_MD = "ra8_path_finalization_report.md"

OPEN_SCHEMA_VERSION = "ra8_open_trade_path_accumulator_v1"
FINALIZED_SCHEMA_VERSION = "ra8_finalized_trade_path_capture_v1"
STATUS_SCHEMA_VERSION = "ra8_path_accumulator_status_v1"

MAX_INTERNAL_GAP_SECONDS = 90


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


def run_live_trade_path_accumulator(
    *,
    managed_positions_path: Path = DEFAULT_MANAGED_POSITIONS,
    canonical_records_path: Path = DEFAULT_CANONICAL_TRADE_RECORDS,
    runtime_candle_root: Path = DEFAULT_RUNTIME_CANDLE_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    accumulate_open_paths: bool = True,
    finalize_closed_paths: bool = True,
    repair_finalized: bool = False,
    now: datetime | str | None = None,
) -> LiveTradePathAccumulatorResult:
    generated_at = _coerce_now(now)
    output_dir.mkdir(parents=True, exist_ok=True)
    open_path = output_dir / OPEN_ACCUMULATOR_JSONL
    finalized_path = output_dir / FINALIZED_CAPTURE_JSONL
    status_path = output_dir / STATUS_JSON
    contract_path = output_dir / CONTRACT_MD
    finalization_report_path = output_dir / FINALIZATION_MD

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
    if finalize_closed_paths:
        finalized_rows, finalized_count = finalize_closed_trade_paths(
            open_rows,
            previous_finalized=previous_finalized,
            canonical_records=canonical_records,
            generated_at=generated_at,
            repair_finalized=repair_finalized,
            source_paths={
                "canonical_trade_records": canonical_records_path,
                "open_trade_path_accumulator": open_path,
            },
        )

    status = build_path_accumulator_status(
        open_rows=open_rows,
        finalized_rows=finalized_rows,
        canonical_records=canonical_records,
        generated_at=generated_at,
        accumulated_count=accumulated_count,
        finalized_count=finalized_count,
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
    return LiveTradePathAccumulatorResult(
        open_rows=open_rows,
        finalized_rows=finalized_rows,
        status=status,
        open_path=open_path,
        finalized_path=finalized_path,
        status_path=status_path,
        contract_path=contract_path,
        finalization_report_path=finalization_report_path,
    )


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
    source_paths: Mapping[str, Path | str] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    finalized_by_key = {
        str(row.get("accumulator_key") or row.get("source_trade_id") or row.get("canonical_trade_record_id")): dict(row)
        for row in previous_finalized
        if row.get("accumulator_key") or row.get("source_trade_id") or row.get("canonical_trade_record_id")
    }
    open_index = _OpenAccumulatorIndex(open_rows)
    finalized_count = 0
    for record in _closed_canonical_records(canonical_records):
        open_row = open_index.find(record)
        if not open_row:
            continue
        key = str(open_row.get("accumulator_key") or _closed_record_key(record))
        if key in finalized_by_key and not repair_finalized:
            continue
        finalized_by_key[key] = _finalized_from_open_and_record(
            open_row=open_row,
            record=record,
            generated_at=generated_at,
            source_paths=source_paths or {},
        )
        finalized_count += 1
    rows = sorted(finalized_by_key.values(), key=lambda row: (str(row.get("exit_time") or ""), str(row.get("accumulator_key") or "")))
    return rows, finalized_count


def build_path_accumulator_status(
    *,
    open_rows: Sequence[Mapping[str, Any]],
    finalized_rows: Sequence[Mapping[str, Any]],
    canonical_records: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    accumulated_count: int = 0,
    finalized_count: int = 0,
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
