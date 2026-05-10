"""Research-only quality audit for Databento minute backfill partitions.

This module audits offline research artifacts only. It must not be used as
runtime, preflight, dashboard, or broker truth.
"""

from __future__ import annotations

import argparse
import json
import os
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence
from zoneinfo import ZoneInfo

from mgc_v05l.app.databento_research_minute_backfill import DEFAULT_OUTPUT_ROOT, REPO_ROOT, SOURCE
from mgc_v05l.research.trend_participation.storage import build_layout, write_storage_manifest

AUDIT_SOURCE = "DATABENTO_HISTORICAL_RESEARCH_BACKFILL_QUALITY_AUDIT"
TIMEFRAME = "1m"
DEFAULT_SYMBOL = "MGC"
SESSION_TIMEZONE = ZoneInfo("America/New_York")
SESSION_REPLAY_MIN_ACTIVE_RATIO = 0.75
SESSION_REPLAY_MAX_GAP_MINUTES = 15
SESSION_REPLAY_MAX_SUSPICIOUS_GAPS = 5
SESSION_EXPECTED_MINUTES = {
    "ASIA": 540,
    "LONDON": 320,
    "US": 460,
    "OFF_SESSION": 120,
}


@dataclass(frozen=True)
class ResearchBackfillAuditConfig:
    repo_root: Path = REPO_ROOT
    output_root: Path = DEFAULT_OUTPUT_ROOT
    symbol: str = DEFAULT_SYMBOL
    expected_start_date: date = date(2010, 6, 6)
    loaded_history_start_date: date = date(2020, 1, 1)
    max_gap_samples: int = 50
    now: datetime | None = None


@dataclass(frozen=True)
class ResearchBackfillAuditResult:
    report: dict[str, Any]
    report_paths: tuple[Path, ...]


def audit_research_minute_backfill(*, config: ResearchBackfillAuditConfig) -> ResearchBackfillAuditResult:
    now = _coerce_now(config.now)
    symbol = config.symbol.upper()
    raw_root = _raw_root(config) / f"symbol={symbol}"
    metadata_paths = sorted(raw_root.glob("year=*/month=*/partition_metadata.json"))
    parquet_paths = sorted(raw_root.glob("year=*/month=*/bars.parquet"))

    schema_fingerprints: dict[str, list[str]] = defaultdict(list)
    month_rows: list[dict[str, Any]] = []
    yearly: dict[str, dict[str, Any]] = {}
    gap_counts: dict[str, int] = defaultdict(int)
    gap_samples: list[dict[str, Any]] = []
    session_bucket_map: dict[tuple[str, str, str], dict[str, Any]] = {}
    total_rows = 0
    earliest_actual: str | None = None
    latest_actual: str | None = None
    metadata_mismatches: list[str] = []
    artifact_flag_failures: list[str] = []
    failed_partitions: list[str] = []
    empty_partitions = 0
    complete_partitions = 0
    suspicious_gap_count = 0

    metadata_by_month = {_month_key_from_path(path): path for path in metadata_paths}
    parquet_by_month = {_month_key_from_path(path): path for path in parquet_paths}
    all_months = sorted(set(metadata_by_month) | set(parquet_by_month))

    for month_key in all_months:
        metadata_path = metadata_by_month.get(month_key)
        parquet_path = parquet_by_month.get(month_key)
        metadata = _read_json(metadata_path) if metadata_path else {}
        status = str(metadata.get("status") or ("COMPLETE" if parquet_path else "MISSING_METADATA"))
        row_count = int(metadata.get("row_count") or 0)
        first_bar = metadata.get("actual_start")
        last_bar = metadata.get("actual_end")
        schema_hash: str | None = None
        partition_gap_counts: dict[str, int] = defaultdict(int)
        partition_suspicious = 0
        partition_gap_total = 0

        if not metadata_path:
            metadata_mismatches.append(f"{month_key}: metadata missing")
        if metadata.get("source") not in {SOURCE, None}:
            metadata_mismatches.append(f"{month_key}: unexpected source {metadata.get('source')}")
        if metadata and metadata.get("research_artifact") is not True:
            artifact_flag_failures.append(f"{month_key}: research_artifact is not true")
        if metadata and metadata.get("runtime_artifact") is not False:
            artifact_flag_failures.append(f"{month_key}: runtime_artifact is not false")

        if status == "EMPTY_NO_ROWS":
            empty_partitions += 1
            if parquet_path:
                metadata_mismatches.append(f"{month_key}: EMPTY_NO_ROWS has a Parquet file")
        elif status == "COMPLETE":
            complete_partitions += 1
            if not parquet_path:
                metadata_mismatches.append(f"{month_key}: COMPLETE metadata without Parquet")
            else:
                parquet_summary = _audit_parquet_partition(
                    parquet_path=parquet_path,
                    symbol=symbol,
                    max_gap_samples=max(config.max_gap_samples - len(gap_samples), 0),
                )
                schema_hash = parquet_summary["schema_fingerprint"]
                schema_fingerprints[schema_hash].append(str(parquet_path))
                row_count = int(parquet_summary["row_count"])
                first_bar = parquet_summary["first_bar"]
                last_bar = parquet_summary["last_bar"]
                total_rows += row_count
                partition_gap_counts.update(parquet_summary["gap_classification_counts"])
                partition_suspicious = int(parquet_summary["suspicious_gap_count"])
                partition_gap_total = int(parquet_summary["gap_count"])
                gap_samples.extend(parquet_summary["gap_samples"])
                if parquet_summary["artifact_flag_failures"]:
                    artifact_flag_failures.extend(f"{month_key}: {item}" for item in parquet_summary["artifact_flag_failures"])
                suspicious_gap_count += partition_suspicious
                for key, value in partition_gap_counts.items():
                    gap_counts[key] += int(value)
                _merge_session_buckets(session_bucket_map, parquet_summary["session_buckets"])
                earliest_actual = _min_iso(earliest_actual, first_bar)
                latest_actual = _max_iso(latest_actual, last_bar)
        elif status == "FAILED":
            failed_partitions.append(month_key)

        year = month_key[:4]
        yearly_row = yearly.setdefault(
            year,
            {
                "year": year,
                "row_count": 0,
                "complete_partitions": 0,
                "empty_partitions": 0,
                "failed_partitions": 0,
                "first_bar": None,
                "last_bar": None,
                "suspicious_gap_count": 0,
            },
        )
        yearly_row["row_count"] += row_count
        yearly_row["complete_partitions"] += 1 if status == "COMPLETE" else 0
        yearly_row["empty_partitions"] += 1 if status == "EMPTY_NO_ROWS" else 0
        yearly_row["failed_partitions"] += 1 if status == "FAILED" else 0
        yearly_row["first_bar"] = _min_iso(yearly_row["first_bar"], first_bar)
        yearly_row["last_bar"] = _max_iso(yearly_row["last_bar"], last_bar)
        yearly_row["suspicious_gap_count"] += partition_suspicious

        month_rows.append(
            {
                "month": month_key,
                "status": status,
                "row_count": row_count,
                "first_bar": first_bar,
                "last_bar": last_bar,
                "metadata_path": str(metadata_path) if metadata_path else None,
                "parquet_path": str(parquet_path) if parquet_path else None,
                "schema_fingerprint": schema_hash,
                "gap_count": partition_gap_total,
                "gap_classification_counts": dict(sorted(partition_gap_counts.items())),
                "suspicious_gap_count": partition_suspicious,
            }
        )

    no_2020_partitions = not any("/year=2020/" in f"/{path.as_posix()}/" for path in metadata_paths + parquet_paths)
    session_coverage = _build_session_coverage(session_bucket_map)
    schema_consistent = len(schema_fingerprints) <= 1 and bool(schema_fingerprints)
    artifact_flags_ok = not artifact_flag_failures
    metadata_consistent = not metadata_mismatches
    acceptable = (
        complete_partitions > 0
        and not failed_partitions
        and schema_consistent
        and metadata_consistent
        and artifact_flags_ok
        and no_2020_partitions
    )
    final_classification = (
        "MGC_2010_TO_2020_RESEARCH_DATA_QUALITY_ACCEPTABLE"
        if acceptable
        else "MGC_2010_TO_2020_RESEARCH_DATA_QUALITY_REVIEW_REQUIRED"
    )

    report = {
        "schema_version": "databento_research_minute_backfill_quality_audit_v1",
        "generated_at": now.isoformat(),
        "source": AUDIT_SOURCE,
        "audited_source": SOURCE,
        "repo_root": str(config.repo_root),
        "output_root": str(_output_root(config)),
        "raw_bars_root": str(raw_root),
        "symbol": symbol,
        "timeframe": TIMEFRAME,
        "expected_start": _start_datetime(config.expected_start_date).isoformat(),
        "loaded_history_start": _start_datetime(config.loaded_history_start_date).isoformat(),
        "metadata_partition_count": len(metadata_paths),
        "parquet_partition_count": len(parquet_paths),
        "complete_partition_count": complete_partitions,
        "empty_partition_count": empty_partitions,
        "failed_partition_count": len(failed_partitions),
        "total_row_count": total_rows,
        "earliest_actual_bar": earliest_actual,
        "latest_actual_bar": latest_actual,
        "no_2020_plus_partition_exists": no_2020_partitions,
        "schema_consistent": schema_consistent,
        "schema_fingerprint_count": len(schema_fingerprints),
        "schema_fingerprints": {key: {"partition_count": len(value), "sample_path": value[0] if value else None} for key, value in schema_fingerprints.items()},
        "metadata_consistent": metadata_consistent,
        "metadata_mismatches": metadata_mismatches,
        "artifact_flags_ok": artifact_flags_ok,
        "artifact_flag_failures": artifact_flag_failures,
        "gap_classification_counts": dict(sorted(gap_counts.items())),
        "suspicious_gap_count": suspicious_gap_count,
        "gap_samples": gap_samples[: config.max_gap_samples],
        "session_coverage": session_coverage,
        "session_coverage_summary": _session_coverage_summary(session_coverage),
        "yearly_coverage": [yearly[key] for key in sorted(yearly)],
        "monthly_coverage": month_rows,
        "failed_partitions": failed_partitions,
        "research_artifact": True,
        "runtime_artifact": False,
        "archive_artifact": False,
        "runtime_preflight_dashboard_truth": False,
        "can_submit": False,
        "live_money_eligible": False,
        "paper_trade_allowed": False,
        "final_classification": final_classification,
    }
    return _write_reports(config=config, report=report)


def _audit_parquet_partition(*, parquet_path: Path, symbol: str, max_gap_samples: int) -> dict[str, Any]:
    pq = _require_pyarrow_parquet()
    parquet_file = pq.ParquetFile(parquet_path)
    schema = parquet_file.schema_arrow
    schema_fingerprint = "|".join(f"{field.name}:{field.type}" for field in schema)
    columns = [
        column
        for column in ("symbol", "timeframe", "bar_end", "research_artifact", "runtime_artifact", "archive_artifact")
        if column in schema.names
    ]
    table = parquet_file.read(columns=columns)
    rows = table.to_pylist()
    bar_ends = sorted(_coerce_datetime(row.get("bar_end")) for row in rows if _coerce_datetime(row.get("bar_end")) is not None)
    flag_failures: list[str] = []
    for idx, row in enumerate(rows[:1000]):
        if row.get("symbol") != symbol:
            flag_failures.append(f"row {idx}: symbol={row.get('symbol')}")
        if row.get("timeframe") != TIMEFRAME:
            flag_failures.append(f"row {idx}: timeframe={row.get('timeframe')}")
        if row.get("research_artifact") is not True:
            flag_failures.append(f"row {idx}: research_artifact={row.get('research_artifact')}")
        if row.get("runtime_artifact") is not False:
            flag_failures.append(f"row {idx}: runtime_artifact={row.get('runtime_artifact')}")
    gap_counts: dict[str, int] = defaultdict(int)
    gap_samples: list[dict[str, Any]] = []
    session_buckets: dict[tuple[str, str, str], dict[str, Any]] = {}
    suspicious_count = 0
    previous: datetime | None = None
    for current in bar_ends:
        _add_session_bar(session_buckets, symbol=symbol, bar_end=current)
        if previous is not None:
            delta_minutes = int((current - previous).total_seconds() // 60)
            if delta_minutes > 1:
                classification = _classify_gap(previous, current)
                gap_counts[classification] += 1
                _add_session_gap(
                    session_buckets,
                    symbol=symbol,
                    previous=previous,
                    current=current,
                    classification=classification,
                    delta_minutes=delta_minutes,
                )
                if classification in {"SUSPICIOUS_INTRA_SESSION_GAP", "PROVIDER_OR_DATA_HOLE"}:
                    suspicious_count += 1
                if len(gap_samples) < max_gap_samples:
                    gap_samples.append(
                        {
                            "from": previous.isoformat(),
                            "to": current.isoformat(),
                            "minutes": delta_minutes,
                            "classification": classification,
                        }
                    )
        previous = current
    return {
        "row_count": len(rows),
        "first_bar": None if not bar_ends else bar_ends[0].isoformat(),
        "last_bar": None if not bar_ends else bar_ends[-1].isoformat(),
        "schema_fingerprint": schema_fingerprint,
        "gap_count": sum(gap_counts.values()),
        "gap_classification_counts": dict(sorted(gap_counts.items())),
        "suspicious_gap_count": suspicious_count,
        "gap_samples": gap_samples,
        "session_buckets": session_buckets,
        "artifact_flag_failures": flag_failures,
    }


def _classify_gap(previous: datetime, current: datetime) -> str:
    delta_minutes = int((current - previous).total_seconds() // 60)
    if _crosses_weekend(previous, current) or delta_minutes >= 36 * 60:
        return "HOLIDAY_OR_WEEKEND_GAP"
    if 45 <= delta_minutes <= 90 and previous.hour in {20, 21, 22, 23}:
        return "EXPECTED_DAILY_MAINTENANCE_GAP"
    if 2 <= delta_minutes <= 45:
        return "SUSPICIOUS_INTRA_SESSION_GAP"
    if delta_minutes <= 8 * 60 and _same_trading_date(previous, current):
        return "PROVIDER_OR_DATA_HOLE"
    return "CONTRACT_OR_SESSION_BOUNDARY_GAP"


def _add_session_bar(bucket_map: dict[tuple[str, str, str], dict[str, Any]], *, symbol: str, bar_end: datetime) -> None:
    key = _session_key(symbol=symbol, bar_end=bar_end)
    bucket = bucket_map.setdefault(
        key,
        {
            "symbol": key[0],
            "date": key[1],
            "session": key[2],
            "bar_minutes": set(),
            "first_bar": None,
            "last_bar": None,
            "largest_intra_session_gap_minutes": 0,
            "suspicious_gap_count": 0,
        },
    )
    bucket["bar_minutes"].add(bar_end.isoformat())
    bucket["first_bar"] = _min_iso(bucket["first_bar"], bar_end.isoformat())
    bucket["last_bar"] = _max_iso(bucket["last_bar"], bar_end.isoformat())


def _add_session_gap(
    bucket_map: dict[tuple[str, str, str], dict[str, Any]],
    *,
    symbol: str,
    previous: datetime,
    current: datetime,
    classification: str,
    delta_minutes: int,
) -> None:
    previous_key = _session_key(symbol=symbol, bar_end=previous)
    current_key = _session_key(symbol=symbol, bar_end=current)
    if previous_key != current_key:
        return
    bucket = bucket_map.setdefault(
        previous_key,
        {
            "symbol": previous_key[0],
            "date": previous_key[1],
            "session": previous_key[2],
            "bar_minutes": set(),
            "first_bar": None,
            "last_bar": None,
            "largest_intra_session_gap_minutes": 0,
            "suspicious_gap_count": 0,
        },
    )
    bucket["largest_intra_session_gap_minutes"] = max(int(bucket["largest_intra_session_gap_minutes"]), delta_minutes)
    if classification in {"SUSPICIOUS_INTRA_SESSION_GAP", "PROVIDER_OR_DATA_HOLE"}:
        bucket["suspicious_gap_count"] += 1


def _merge_session_buckets(target: dict[tuple[str, str, str], dict[str, Any]], source: dict[tuple[str, str, str], dict[str, Any]]) -> None:
    for key, source_bucket in source.items():
        bucket = target.setdefault(
            key,
            {
                "symbol": source_bucket["symbol"],
                "date": source_bucket["date"],
                "session": source_bucket["session"],
                "bar_minutes": set(),
                "first_bar": None,
                "last_bar": None,
                "largest_intra_session_gap_minutes": 0,
                "suspicious_gap_count": 0,
            },
        )
        bucket["bar_minutes"].update(source_bucket["bar_minutes"])
        bucket["first_bar"] = _min_iso(bucket["first_bar"], source_bucket["first_bar"])
        bucket["last_bar"] = _max_iso(bucket["last_bar"], source_bucket["last_bar"])
        bucket["largest_intra_session_gap_minutes"] = max(
            int(bucket["largest_intra_session_gap_minutes"]),
            int(source_bucket["largest_intra_session_gap_minutes"]),
        )
        bucket["suspicious_gap_count"] += int(source_bucket["suspicious_gap_count"])


def _build_session_coverage(bucket_map: dict[tuple[str, str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in sorted(bucket_map):
        bucket = bucket_map[key]
        total_bars = len(bucket["bar_minutes"])
        session = str(bucket["session"])
        expected_minutes = SESSION_EXPECTED_MINUTES.get(session, 0)
        active_minutes = total_bars
        active_ratio = None if not expected_minutes else round(active_minutes / expected_minutes, 4)
        largest_gap = int(bucket["largest_intra_session_gap_minutes"])
        suspicious = int(bucket["suspicious_gap_count"])
        eligible, reason = _session_replay_eligibility(
            session=session,
            active_minutes=active_minutes,
            expected_minutes=expected_minutes,
            largest_gap=largest_gap,
            suspicious_gap_count=suspicious,
        )
        rows.append(
            {
                "symbol": bucket["symbol"],
                "date": bucket["date"],
                "session": session,
                "total_bars": total_bars,
                "active_minutes": active_minutes,
                "expected_minutes": expected_minutes,
                "active_ratio": active_ratio,
                "largest_intra_session_gap_minutes": largest_gap,
                "suspicious_gap_count": suspicious,
                "first_bar": bucket["first_bar"],
                "last_bar": bucket["last_bar"],
                "eligible_for_replay": eligible,
                "exclusion_reason": reason,
            }
        )
    return rows


def _session_replay_eligibility(
    *,
    session: str,
    active_minutes: int,
    expected_minutes: int,
    largest_gap: int,
    suspicious_gap_count: int,
) -> tuple[bool, str | None]:
    if session == "OFF_SESSION":
        return False, "OFF_SESSION_NOT_REPLAY_SESSION"
    if active_minutes <= 0:
        return False, "NO_BARS"
    if expected_minutes and active_minutes < int(expected_minutes * SESSION_REPLAY_MIN_ACTIVE_RATIO):
        return False, "ACTIVE_MINUTES_BELOW_75_PERCENT"
    if largest_gap > SESSION_REPLAY_MAX_GAP_MINUTES:
        return False, "LARGEST_GAP_EXCEEDS_15_MINUTES"
    if suspicious_gap_count > SESSION_REPLAY_MAX_SUSPICIOUS_GAPS:
        return False, "SUSPICIOUS_GAP_COUNT_EXCEEDS_5"
    return True, None


def _session_coverage_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_session: dict[str, dict[str, int]] = defaultdict(lambda: {"row_count": 0, "eligible_count": 0, "ineligible_count": 0})
    reasons: dict[str, int] = defaultdict(int)
    for row in rows:
        session = str(row.get("session"))
        by_session[session]["row_count"] += 1
        if row.get("eligible_for_replay"):
            by_session[session]["eligible_count"] += 1
        else:
            by_session[session]["ineligible_count"] += 1
            reasons[str(row.get("exclusion_reason"))] += 1
    return {
        "row_count": len(rows),
        "eligible_count": sum(1 for row in rows if row.get("eligible_for_replay")),
        "ineligible_count": sum(1 for row in rows if not row.get("eligible_for_replay")),
        "by_session": {key: by_session[key] for key in sorted(by_session)},
        "exclusion_reasons": dict(sorted(reasons.items())),
    }


def _session_key(*, symbol: str, bar_end: datetime) -> tuple[str, str, str]:
    local = bar_end.astimezone(SESSION_TIMEZONE)
    session = _session_label(local)
    trading_date = local.date() + timedelta(days=1) if session == "ASIA" and local.time() >= time(18, 0) else local.date()
    return symbol, trading_date.isoformat(), session


def _session_label(local: datetime) -> str:
    value = local.time()
    if value >= time(18, 0) or value < time(3, 0):
        return "ASIA"
    if time(3, 0) <= value < time(8, 20):
        return "LONDON"
    if time(8, 20) <= value < time(16, 0):
        return "US"
    return "OFF_SESSION"


def _crosses_weekend(previous: datetime, current: datetime) -> bool:
    cursor = previous.date()
    while cursor <= current.date():
        if cursor.weekday() in {5, 6}:
            return True
        cursor = date.fromordinal(cursor.toordinal() + 1)
    return False


def _same_trading_date(previous: datetime, current: datetime) -> bool:
    return previous.date() == current.date()


def _write_reports(*, config: ResearchBackfillAuditConfig, report: dict[str, Any]) -> ResearchBackfillAuditResult:
    layout = _layout(config)
    report_dir = layout["reports"]
    manifest_dir = layout["manifests"]
    report_dir.mkdir(parents=True, exist_ok=True)
    manifest_dir.mkdir(parents=True, exist_ok=True)
    latest_json = report_dir / f"latest_databento_research_minute_backfill_quality_audit_{config.symbol.upper()}.json"
    markdown = report_dir / f"databento_research_minute_backfill_quality_audit_{config.symbol.upper()}.md"
    manifest = manifest_dir / f"databento_research_minute_backfill_quality_audit_{config.symbol.upper()}_manifest.json"
    _write_json_atomic(latest_json, report)
    markdown.write_text(_render_markdown(report), encoding="utf-8")
    write_storage_manifest(
        manifest,
        {
            "schema_version": "databento_research_minute_backfill_quality_audit_manifest_v1",
            "generated_at": report.get("generated_at"),
            "source": AUDIT_SOURCE,
            "symbol": report.get("symbol"),
            "timeframe": report.get("timeframe"),
            "report_path": str(latest_json),
            "raw_bars_root": report.get("raw_bars_root"),
            "research_artifact": True,
            "runtime_artifact": False,
            "archive_artifact": False,
            "can_submit": False,
            "live_money_eligible": False,
            "final_classification": report.get("final_classification"),
        },
    )
    return ResearchBackfillAuditResult(report=report, report_paths=(latest_json, markdown, manifest))


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Databento Research Minute Backfill Quality Audit",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- final_classification: `{report.get('final_classification')}`",
        f"- symbol: `{report.get('symbol')}`",
        f"- complete_partition_count: `{report.get('complete_partition_count')}`",
        f"- empty_partition_count: `{report.get('empty_partition_count')}`",
        f"- failed_partition_count: `{report.get('failed_partition_count')}`",
        f"- total_row_count: `{report.get('total_row_count')}`",
        f"- earliest_actual_bar: `{report.get('earliest_actual_bar')}`",
        f"- latest_actual_bar: `{report.get('latest_actual_bar')}`",
        f"- schema_consistent: `{report.get('schema_consistent')}`",
        f"- suspicious_gap_count: `{report.get('suspicious_gap_count')}`",
        f"- runtime_artifact: `{report.get('runtime_artifact')}`",
        f"- can_submit: `{report.get('can_submit')}`",
        "",
        "## Gap Classifications",
        "",
    ]
    for key, value in dict(report.get("gap_classification_counts") or {}).items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "## Yearly Coverage",
            "",
            "| year | rows | complete | empty | failed | suspicious gaps | first bar | last bar |",
            "|---|---:|---:|---:|---:|---:|---|---|",
        ]
        )
    for row in list(report.get("yearly_coverage") or []):
        lines.append(
            f"| {row.get('year')} | {row.get('row_count')} | {row.get('complete_partitions')} | {row.get('empty_partitions')} | {row.get('failed_partitions')} | {row.get('suspicious_gap_count')} | {row.get('first_bar')} | {row.get('last_bar')} |"
        )
    lines.extend(
        [
            "",
            "## Session Coverage",
            "",
            f"- session_rows: `{(report.get('session_coverage_summary') or {}).get('row_count')}`",
            f"- eligible_rows: `{(report.get('session_coverage_summary') or {}).get('eligible_count')}`",
            f"- ineligible_rows: `{(report.get('session_coverage_summary') or {}).get('ineligible_count')}`",
            "",
            "| symbol | date | session | bars | active minutes | largest gap | suspicious gaps | eligible | exclusion reason | first bar | last bar |",
            "|---|---|---|---:|---:|---:|---:|---|---|---|---|",
        ]
    )
    for row in list(report.get("session_coverage") or [])[:500]:
        lines.append(
            f"| {row.get('symbol')} | {row.get('date')} | {row.get('session')} | {row.get('total_bars')} | {row.get('active_minutes')} | {row.get('largest_intra_session_gap_minutes')} | {row.get('suspicious_gap_count')} | {row.get('eligible_for_replay')} | {row.get('exclusion_reason')} | {row.get('first_bar')} | {row.get('last_bar')} |"
        )
    return "\n".join(lines) + "\n"


def _raw_root(config: ResearchBackfillAuditConfig) -> Path:
    return _layout(config)["raw"] / "databento_minute_backfill"


def _output_root(config: ResearchBackfillAuditConfig) -> Path:
    root = Path(config.output_root)
    return root if root.is_absolute() else Path(config.repo_root) / root


def _layout(config: ResearchBackfillAuditConfig) -> dict[str, Path]:
    return build_layout(_output_root(config))


def _month_key_from_path(path: Path) -> str:
    year = next((part.split("=", 1)[1] for part in path.parts if part.startswith("year=")), "0000")
    month = next((part.split("=", 1)[1] for part in path.parts if part.startswith("month=")), "00")
    return f"{year}-{month}"


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)


def _coerce_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coerce_now(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _start_datetime(value: date) -> datetime:
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


def _min_iso(left: str | None, right: str | None) -> str | None:
    if not right:
        return left
    if not left:
        return right
    return right if right < left else left


def _max_iso(left: str | None, right: str | None) -> str | None:
    if not right:
        return left
    if not left:
        return right
    return right if right > left else left


def _require_pyarrow_parquet() -> Any:
    try:
        import pyarrow.parquet as pq  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError("Research minute backfill audit requires pyarrow in the repo environment.") from exc
    return pq


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit research-only Databento minute backfill partitions.")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--expected-start-date", default="2010-06-06")
    parser.add_argument("--loaded-history-start-date", default="2020-01-01")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--max-gap-samples", type=int, default=50)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = audit_research_minute_backfill(
        config=ResearchBackfillAuditConfig(
            repo_root=Path(args.repo_root),
            output_root=Path(args.output_root),
            symbol=str(args.symbol).upper(),
            expected_start_date=date.fromisoformat(str(args.expected_start_date)),
            loaded_history_start_date=date.fromisoformat(str(args.loaded_history_start_date)),
            max_gap_samples=int(args.max_gap_samples),
        )
    )
    print(
        json.dumps(
            {
                "final_classification": result.report["final_classification"],
                "symbol": result.report["symbol"],
                "complete_partition_count": result.report["complete_partition_count"],
                "empty_partition_count": result.report["empty_partition_count"],
                "failed_partition_count": result.report["failed_partition_count"],
                "total_row_count": result.report["total_row_count"],
                "suspicious_gap_count": result.report["suspicious_gap_count"],
                "schema_consistent": result.report["schema_consistent"],
                "report_paths": [str(path) for path in result.report_paths],
                "can_submit": result.report["can_submit"],
                "live_money_eligible": result.report["live_money_eligible"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
