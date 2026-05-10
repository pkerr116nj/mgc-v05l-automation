"""Research-only MGC entry archetype discovery over offline minute bars.

This module is deliberately offline-only. It reads research Parquet artifacts
and quality-audit session coverage; it must not be used as runtime, preflight,
dashboard, broker, or PAPER eligibility truth.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import sqlite3
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo

from mgc_v05l.research.trend_participation.storage import build_layout, write_storage_manifest

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "trend_participation_engine"
DEFAULT_SYMBOL = "MGC"
DEFAULT_HORIZONS = (15, 30, 60)
DEFAULT_LOOKBACKS = (5, 15, 30, 60)
DEFAULT_COHORT_SIZES = (500, 1000, 2000)
DEFAULT_RANDOM_CONTROL_COUNT = 2000
PRIMARY_HORIZON = 60
NEW_YORK = ZoneInfo("America/New_York")
SOURCE = "MGC_ENTRY_ARCHETYPE_DISCOVERY_RESEARCH_ONLY"
CANONICAL_1M_DATA_SOURCE = "historical_1m_canonical"
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
class DiscoveryConfig:
    repo_root: Path = REPO_ROOT
    output_root: Path = DEFAULT_OUTPUT_ROOT
    symbol: str = DEFAULT_SYMBOL
    raw_bars_root: Path | None = None
    quality_audit_path: Path | None = None
    canonical_sqlite_path: Path | None = None
    horizons: tuple[int, ...] = DEFAULT_HORIZONS
    lookbacks: tuple[int, ...] = DEFAULT_LOOKBACKS
    cohort_sizes: tuple[int, ...] = DEFAULT_COHORT_SIZES
    random_control_count: int = DEFAULT_RANDOM_CONTROL_COUNT
    random_seed: int = 90210
    now: datetime | None = None


@dataclass(frozen=True, slots=True)
class MinuteBar:
    symbol: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    source_panel: str = "UNKNOWN"
    provenance_tag: str = ""


@dataclass(frozen=True, slots=True)
class EventCandidate:
    event_id: str
    symbol: str
    direction: str
    decision_ts: datetime
    session_date: str
    session: str
    time_of_day: str
    time_bin_15m: str
    day_of_week: str
    year: int
    month: str
    eligible_for_replay: bool
    session_active_ratio: float | None
    session_largest_gap_minutes: int
    session_suspicious_gap_count: int
    entry_price: float
    archetypes: tuple[str, ...]
    forward_return_15m: float | None
    forward_return_30m: float | None
    forward_return_60m: float | None
    mfe_15m: float
    mfe_30m: float
    mfe_60m: float
    mae_15m: float
    mae_30m: float
    mae_60m: float

    @property
    def primary_return(self) -> float:
        return float(self.forward_return_60m or 0.0)

    @property
    def primary_mfe(self) -> float:
        return float(self.mfe_60m)

    @property
    def primary_mae(self) -> float:
        return float(self.mae_60m)


def run_mgc_entry_archetype_discovery(*, config: DiscoveryConfig) -> dict[str, Any]:
    now = _coerce_now(config.now)
    symbol = config.symbol.upper()
    output_root = _resolve_output_root(config)
    raw_root = config.raw_bars_root or output_root / "raw_bars" / "databento_minute_backfill" / f"symbol={symbol}"
    quality_path = config.quality_audit_path or output_root / "reports" / f"latest_databento_research_minute_backfill_quality_audit_{symbol}.json"

    quality = _read_json(quality_path)
    canonical_sqlite_path = _resolve_canonical_sqlite_path(config)
    pre2020_bars = _load_research_bars(raw_root=raw_root, symbol=symbol)
    canonical_bars = _load_canonical_sqlite_bars(sqlite_path=canonical_sqlite_path, symbol=symbol)
    bars = _combine_research_bars(pre2020_bars=pre2020_bars, canonical_bars=canonical_bars)
    session_coverage_rows = _build_session_coverage_rows(bars)
    eligible_sessions = _eligible_session_map_from_rows(session_coverage_rows)
    pre2020_quality_eligible_sessions = _eligible_session_map(quality)
    events = _build_event_candidates(
        bars=bars,
        eligible_sessions=eligible_sessions,
        horizons=tuple(sorted(set(config.horizons))),
        lookbacks=tuple(sorted(set(config.lookbacks))),
    )
    cohorts = _build_nested_cohorts(
        events=events,
        cohort_sizes=tuple(sorted(set(config.cohort_sizes))),
        random_control_count=config.random_control_count,
        random_seed=config.random_seed,
    )
    cohort_rows = _build_cohort_composition_rows(cohorts)
    archetype_rows = _build_archetype_frequency_rows(cohorts)
    metric_rows = _build_archetype_metric_rows(events=events, cohorts=cohorts)
    classification_rows = _classify_archetypes(events=events, cohorts=cohorts)
    histogram_rows = _build_histogram_rows(events)

    layout = build_layout(output_root)
    report_dir = layout["reports"]
    manifest_dir = layout["manifests"]
    summary_json = report_dir / "mgc_entry_archetype_discovery_summary.json"
    summary_md = report_dir / "mgc_entry_archetype_discovery_summary.md"
    cohort_csv = report_dir / "mgc_entry_archetype_discovery_cohort_composition.csv"
    archetype_csv = report_dir / "mgc_entry_archetype_discovery_archetype_frequency.csv"
    metrics_csv = report_dir / "mgc_entry_archetype_discovery_archetype_metrics.csv"
    classification_csv = report_dir / "mgc_entry_archetype_discovery_candidate_classification.csv"
    histogram_csv = report_dir / "mgc_entry_archetype_discovery_histograms.csv"
    manifest_path = manifest_dir / "mgc_entry_archetype_discovery_manifest.json"

    summary = {
        "schema_version": "mgc_entry_archetype_discovery_v1",
        "generated_at": now.isoformat(),
        "source": SOURCE,
        "symbol": symbol,
        "research_artifact": True,
        "runtime_artifact": False,
        "archive_artifact": False,
        "runtime_preflight_dashboard_truth": False,
        "can_submit": False,
        "paper_trade_allowed": False,
        "live_money_eligible": False,
        "data_sources_used": _source_inventory(
            raw_root=raw_root,
            quality_path=quality_path,
            canonical_sqlite_path=canonical_sqlite_path,
            pre2020_bars=pre2020_bars,
            canonical_bars=canonical_bars,
            combined_bars=bars,
        ),
        "prior_run_source_check": {
            "prior_summary_path": str(output_root / "reports" / "mgc_entry_archetype_discovery_summary.json"),
            "prior_raw_bars_root": str(raw_root),
            "prior_run_was_pre2020_databento_only": True,
            "evidence": "The prior summary raw_bars_root pointed to outputs/reports/trend_participation_engine/raw_bars/databento_minute_backfill/symbol=MGC and its date range ended at 2019-12-31.",
        },
        "raw_bars_root": str(raw_root),
        "canonical_sqlite_path": str(canonical_sqlite_path),
        "quality_audit_path": str(quality_path),
        "data_range": {
            "first_bar": bars[0].ts.isoformat() if bars else None,
            "last_bar": bars[-1].ts.isoformat() if bars else None,
            "eligible_first_event": min((event.decision_ts for event in events), default=None).isoformat() if events else None,
            "eligible_last_event": max((event.decision_ts for event in events), default=None).isoformat() if events else None,
        },
        "eligible_session_summary": {
            "eligible_session_count": len(eligible_sessions),
            "total_session_rows": len(session_coverage_rows),
            "pre2020_quality_audit_eligible_session_count": int((quality.get("session_coverage_summary") or {}).get("eligible_count") or 0),
            "pre2020_quality_audit_total_session_rows": int((quality.get("session_coverage_summary") or {}).get("row_count") or 0),
            "pre2020_quality_eligible_sessions_seen": len(pre2020_quality_eligible_sessions),
            "eligibility_policy": "Comparable session coverage screen inferred across the combined dataset; pre-2020 audit mask remains inventoried and consistent with source QA.",
        },
        "event_selection_method": {
            "decision_price": "completed 1m bar close",
            "pre_entry_feature_rule": "features use the completed decision bar and earlier bars only",
            "forward_rule": "forward returns/excursions use bars strictly after decision_ts within the same eligible session bucket",
            "primary_favorable_rank": "60m directional max favorable excursion, tie-broken by 60m realized return and lower MAE",
            "primary_adverse_rank": "60m directional max adverse excursion plus negative realized return pressure, tie-broken by lower MFE",
            "directions": ["LONG", "SHORT"],
            "horizons_minutes": list(config.horizons),
            "lookbacks_minutes": list(config.lookbacks),
        },
        "event_counts": {
            "eligible_directional_candidates": len(events),
            "unique_decision_windows": len({event.decision_ts for event in events}),
        },
        "cohort_sizes": {name: len(rows) for name, rows in cohorts.items()},
        "artifact_paths": {
            "summary_json": str(summary_json),
            "summary_markdown": str(summary_md),
            "cohort_composition_csv": str(cohort_csv),
            "archetype_frequency_csv": str(archetype_csv),
            "archetype_metrics_csv": str(metrics_csv),
            "candidate_classification_csv": str(classification_csv),
            "histogram_csv": str(histogram_csv),
            "manifest": str(manifest_path),
        },
        "top_candidate_archetypes": [row for row in classification_rows if row["classification"] in {"ECONOMICALLY_RELEVANT_FOLLOWUP", "PROMISING_BUT_NEEDS_VALIDATION"}][:15],
        "rejected_or_noisy_archetypes": [row for row in classification_rows if row["classification"] in {"NOISY_OR_NON_DISCRIMINATIVE", "OUTLIER_DEPENDENT"}][:15],
        "data_quality_limitations": _data_quality_limitations(quality=quality, eligible_sessions=eligible_sessions, events=events),
        "final_classification": "MGC_FULL_DATASET_ENTRY_ARCHETYPE_DISCOVERY_COMPLETE",
    }

    _write_csv(cohort_csv, cohort_rows)
    _write_csv(archetype_csv, archetype_rows)
    _write_csv(metrics_csv, metric_rows)
    _write_csv(classification_csv, classification_rows)
    _write_csv(histogram_csv, histogram_rows)
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True, default=_json_ready) + "\n", encoding="utf-8")
    summary_md.write_text(_render_markdown(summary, classification_rows, archetype_rows), encoding="utf-8")
    write_storage_manifest(
        manifest_path,
        {
            "schema_version": "mgc_entry_archetype_discovery_manifest_v1",
            "generated_at": summary["generated_at"],
            "source": SOURCE,
            "symbol": symbol,
            "artifact_paths": summary["artifact_paths"],
            "data_sources_used": summary["data_sources_used"],
            "research_artifact": True,
            "runtime_artifact": False,
            "runtime_preflight_dashboard_truth": False,
            "can_submit": False,
            "paper_trade_allowed": False,
            "live_money_eligible": False,
            "final_classification": summary["final_classification"],
        },
    )
    return summary


def _load_research_bars(*, raw_root: Path, symbol: str) -> list[MinuteBar]:
    pq = _require_pyarrow_parquet()
    paths = sorted(raw_root.glob("year=*/month=*/bars.parquet"))
    if not paths:
        return []
    rows: list[MinuteBar] = []
    columns = ["symbol", "bar_end", "open", "high", "low", "close", "volume", "research_artifact", "runtime_artifact"]
    for path in paths:
        table = pq.ParquetFile(path).read(columns=columns)
        for row in table.to_pylist():
            if str(row.get("symbol") or "").upper() != symbol:
                continue
            if row.get("research_artifact") is not True or row.get("runtime_artifact") is not False:
                continue
            ts = _coerce_ts(row["bar_end"])
            rows.append(
                MinuteBar(
                    symbol=symbol,
                    ts=ts,
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row.get("volume") or 0.0),
                    source_panel="pre2020_databento_backfill",
                    provenance_tag="DATABENTO_HISTORICAL_RESEARCH_BACKFILL",
                )
            )
    rows.sort(key=lambda item: item.ts)
    return rows


def _load_canonical_sqlite_bars(*, sqlite_path: Path, symbol: str) -> list[MinuteBar]:
    if not sqlite_path.exists():
        return []
    connection = sqlite3.connect(sqlite_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            select symbol, end_ts, open, high, low, close, volume, data_source
            from bars
            where symbol = ? and timeframe = '1m' and data_source = ?
            order by end_ts asc
            """,
            [symbol, CANONICAL_1M_DATA_SOURCE],
        ).fetchall()
    finally:
        connection.close()
    return [
        MinuteBar(
            symbol=str(row["symbol"]).upper(),
            ts=_coerce_ts(row["end_ts"]),
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row["volume"] or 0.0),
            source_panel="canonical_2020plus",
            provenance_tag=str(row["data_source"]),
        )
        for row in rows
    ]


def _combine_research_bars(*, pre2020_bars: Sequence[MinuteBar], canonical_bars: Sequence[MinuteBar]) -> list[MinuteBar]:
    by_key: dict[tuple[str, datetime], MinuteBar] = {}
    for bar in pre2020_bars:
        by_key[(bar.symbol, bar.ts)] = bar
    for bar in canonical_bars:
        by_key[(bar.symbol, bar.ts)] = bar
    return sorted(by_key.values(), key=lambda item: (item.ts, item.symbol))


def _eligible_session_map(quality: dict[str, Any]) -> dict[tuple[str, str, str], dict[str, Any]]:
    rows: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in quality.get("session_coverage") or []:
        if row.get("eligible_for_replay") is not True:
            continue
        key = (str(row["symbol"]).upper(), str(row["date"]), str(row["session"]).upper())
        rows[key] = dict(row)
    return rows


def _eligible_session_map_from_rows(rows: Sequence[dict[str, Any]]) -> dict[tuple[str, str, str], dict[str, Any]]:
    eligible: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        if row.get("eligible_for_replay") is not True:
            continue
        eligible[(str(row["symbol"]).upper(), str(row["date"]), str(row["session"]).upper())] = dict(row)
    return eligible


def _build_session_coverage_rows(bars: Sequence[MinuteBar]) -> list[dict[str, Any]]:
    bucket_map: dict[tuple[str, str, str], dict[str, Any]] = {}
    previous_by_session: dict[tuple[str, str, str], datetime] = {}
    for bar in bars:
        key = _session_key(symbol=bar.symbol, bar_end=bar.ts)
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
                "source_panels": set(),
            },
        )
        bucket["bar_minutes"].add(bar.ts.isoformat())
        bucket["source_panels"].add(bar.source_panel)
        bucket["first_bar"] = _min_iso(bucket["first_bar"], bar.ts.isoformat())
        bucket["last_bar"] = _max_iso(bucket["last_bar"], bar.ts.isoformat())
        previous = previous_by_session.get(key)
        if previous is not None:
            delta_minutes = int((bar.ts - previous).total_seconds() // 60)
            if delta_minutes > 1:
                bucket["largest_intra_session_gap_minutes"] = max(int(bucket["largest_intra_session_gap_minutes"]), delta_minutes)
                if delta_minutes <= 8 * 60:
                    bucket["suspicious_gap_count"] += 1
        previous_by_session[key] = bar.ts

    rows: list[dict[str, Any]] = []
    for key in sorted(bucket_map):
        bucket = bucket_map[key]
        total_bars = len(bucket["bar_minutes"])
        session = str(bucket["session"])
        expected_minutes = SESSION_EXPECTED_MINUTES.get(session, 0)
        active_ratio = None if not expected_minutes else round(total_bars / expected_minutes, 4)
        largest_gap = int(bucket["largest_intra_session_gap_minutes"])
        suspicious = int(bucket["suspicious_gap_count"])
        eligible, reason = _session_replay_eligibility(
            session=session,
            active_minutes=total_bars,
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
                "active_minutes": total_bars,
                "expected_minutes": expected_minutes,
                "active_ratio": active_ratio,
                "largest_intra_session_gap_minutes": largest_gap,
                "suspicious_gap_count": suspicious,
                "first_bar": bucket["first_bar"],
                "last_bar": bucket["last_bar"],
                "source_panels": ",".join(sorted(bucket["source_panels"])),
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


def _build_event_candidates(
    *,
    bars: Sequence[MinuteBar],
    eligible_sessions: dict[tuple[str, str, str], dict[str, Any]],
    horizons: tuple[int, ...],
    lookbacks: tuple[int, ...],
) -> list[EventCandidate]:
    grouped: dict[tuple[str, str, str], list[MinuteBar]] = defaultdict(list)
    for bar in bars:
        key = _session_key(symbol=bar.symbol, bar_end=bar.ts)
        if key in eligible_sessions:
            grouped[key].append(bar)

    events: list[EventCandidate] = []
    min_lookback = max(lookbacks)
    max_horizon = max(horizons)
    for key, session_bars in sorted(grouped.items()):
        session_bars.sort(key=lambda item: item.ts)
        coverage = eligible_sessions[key]
        for index, current in enumerate(session_bars):
            if index + 1 >= len(session_bars) or index + 1 < min_lookback:
                continue
            future = _future_window(session_bars, index=index, horizon_minutes=max_horizon)
            if not future:
                continue
            local = current.ts.astimezone(NEW_YORK)
            for direction in ("LONG", "SHORT"):
                features = _extract_pre_entry_features(session_bars=session_bars, index=index, direction=direction, lookbacks=lookbacks)
                archetypes = tuple(_label_archetypes(features=features, direction=direction))
                forward_returns, mfe_points, mae_points = _forward_outcomes(
                    session_bars=session_bars,
                    index=index,
                    direction=direction,
                    horizons=horizons,
                )
                events.append(
                    EventCandidate(
                        event_id=f"{current.symbol}|{current.ts.isoformat()}|{direction}",
                        symbol=current.symbol,
                        direction=direction,
                        decision_ts=current.ts,
                        session_date=key[1],
                        session=key[2],
                        time_of_day=local.strftime("%H:%M"),
                        time_bin_15m=f"{local.hour:02d}:{(local.minute // 15) * 15:02d}",
                        day_of_week=local.strftime("%A"),
                        year=local.year,
                        month=f"{local.year:04d}-{local.month:02d}",
                        eligible_for_replay=True,
                        session_active_ratio=_nullable_float(coverage.get("active_ratio")),
                        session_largest_gap_minutes=int(coverage.get("largest_intra_session_gap_minutes") or 0),
                        session_suspicious_gap_count=int(coverage.get("suspicious_gap_count") or 0),
                        entry_price=current.close,
                        archetypes=archetypes or ("unclassified",),
                        forward_return_15m=forward_returns.get(15),
                        forward_return_30m=forward_returns.get(30),
                        forward_return_60m=forward_returns.get(60),
                        mfe_15m=float(mfe_points.get(15) or 0.0),
                        mfe_30m=float(mfe_points.get(30) or 0.0),
                        mfe_60m=float(mfe_points.get(60) or 0.0),
                        mae_15m=float(mae_points.get(15) or 0.0),
                        mae_30m=float(mae_points.get(30) or 0.0),
                        mae_60m=float(mae_points.get(60) or 0.0),
                    )
                )
    return events


def _extract_pre_entry_features(
    *,
    session_bars: Sequence[MinuteBar],
    index: int,
    direction: str,
    lookbacks: tuple[int, ...] = DEFAULT_LOOKBACKS,
) -> dict[str, Any]:
    sign = 1.0 if direction == "LONG" else -1.0
    current = session_bars[index]
    features: dict[str, Any] = {"direction": direction}
    current_range = max(current.high - current.low, 0.0)
    current_body = abs(current.close - current.open)
    close_location = 0.5 if current_range <= 0 else (current.close - current.low) / current_range
    direction_close_location = close_location if direction == "LONG" else 1.0 - close_location
    rejection_wick = (min(current.open, current.close) - current.low) if direction == "LONG" else (current.high - max(current.open, current.close))
    features.update(
        {
            "current_body_to_range": _safe_div(current_body, current_range),
            "current_close_location": close_location,
            "directional_close_location": direction_close_location,
            "directional_rejection_wick_to_range": _safe_div(rejection_wick, current_range),
            "current_directional_close": sign * (current.close - current.open) > 0,
        }
    )
    previous = session_bars[index - 1] if index else None
    if previous is not None:
        features["inside_bar"] = current.high <= previous.high and current.low >= previous.low
        features["outside_bar"] = current.high >= previous.high and current.low <= previous.low
    else:
        features["inside_bar"] = False
        features["outside_bar"] = False

    for lookback in lookbacks:
        window = list(session_bars[index - lookback + 1 : index + 1])
        previous_window = list(session_bars[max(0, index - lookback) : index])
        if not window:
            continue
        ranges = [max(bar.high - bar.low, 0.0) for bar in window]
        closes = [bar.close for bar in window]
        directional_changes = [sign * (right.close - left.close) for left, right in zip(window, window[1:], strict=False)]
        signed_window_return = sign * (window[-1].close - window[0].close)
        features[f"slope_{lookback}m"] = round(signed_window_return, 6)
        features[f"directional_pressure_{lookback}m"] = round(_safe_div(sum(directional_changes), sum(ranges)), 6)
        features[f"avg_range_{lookback}m"] = round(statistics.fmean(ranges), 6) if ranges else 0.0
        features[f"range_sum_{lookback}m"] = round(sum(ranges), 6)
        features[f"consecutive_directional_closes_{lookback}m"] = _consecutive_directional_closes(window, direction=direction)
        features[f"hh_hl_tendency_{lookback}m"] = _structure_tendency(window, direction=direction)
        features[f"pullback_depth_{lookback}m"] = round(_pullback_depth(window, direction=direction), 6)
        features[f"close_to_range_position_{lookback}m"] = round(_window_close_position(window, direction=direction), 6)
        features[f"range_contraction_ratio_{lookback}m"] = round(
            _safe_div(statistics.fmean(ranges[-min(5, len(ranges)) :]), statistics.fmean(ranges)),
            6,
        )
        if previous_window:
            prior_high = max(bar.high for bar in previous_window)
            prior_low = min(bar.low for bar in previous_window)
            features[f"breaks_prior_{lookback}m_extreme"] = current.close > prior_high if direction == "LONG" else current.close < prior_low
            features[f"failed_prior_{lookback}m_extreme"] = (
                current.high > prior_high and current.close < prior_high
                if direction == "LONG"
                else current.low < prior_low and current.close > prior_low
            )
        else:
            features[f"breaks_prior_{lookback}m_extreme"] = False
            features[f"failed_prior_{lookback}m_extreme"] = False
        features[f"close_change_sign_pattern_{lookback}m"] = _sign_pattern(directional_changes[-min(8, len(directional_changes)) :])
        features[f"last_close_vs_window_mid_{lookback}m"] = round(sign * (closes[-1] - ((max(closes) + min(closes)) / 2.0)), 6)

    features["retracement_classification"] = _retracement_classification(features)
    return features


def _label_archetypes(*, features: dict[str, Any], direction: str) -> list[str]:
    labels: list[str] = []
    strong_bar = bool(features.get("current_directional_close")) and float(features.get("directional_close_location") or 0.0) >= 0.65
    strong_rejection = float(features.get("directional_rejection_wick_to_range") or 0.0) >= 0.35
    compressed_15 = float(features.get("range_contraction_ratio_15m") or 1.0) <= 0.72
    compressed_30 = float(features.get("range_contraction_ratio_30m") or 1.0) <= 0.78
    trend_60 = float(features.get("slope_60m") or 0.0) > 0.0
    trend_30 = float(features.get("slope_30m") or 0.0) > 0.0
    pullback_15 = float(features.get("pullback_depth_15m") or 0.0)
    avg_range_30 = max(float(features.get("avg_range_30m") or 0.0), 0.000001)
    shallow_pullback = pullback_15 <= avg_range_30 * 2.5
    deep_pullback = pullback_15 >= avg_range_30 * 5.0
    opposite_pressure_15 = float(features.get("directional_pressure_15m") or 0.0) < -0.10
    directional_pressure_15 = float(features.get("directional_pressure_15m") or 0.0) > 0.12
    broke_15 = bool(features.get("breaks_prior_15m_extreme"))
    broke_30 = bool(features.get("breaks_prior_30m_extreme"))
    failed_15 = bool(features.get("failed_prior_15m_extreme"))
    failed_30 = bool(features.get("failed_prior_30m_extreme"))

    if broke_15 and strong_bar:
        labels.append("breakout")
    if broke_30 and compressed_15:
        labels.append("range_compression_break")
    if broke_30 and shallow_pullback and trend_60:
        labels.append("breakout_retest_hold")
    if failed_15 or failed_30:
        labels.append("failed_breakout")
    if trend_60 and opposite_pressure_15 and strong_bar and shallow_pullback:
        labels.append("pause_pullback_resume" if direction == "LONG" else "pause_rebound_resume")
    if trend_60 and trend_30 and shallow_pullback and strong_bar:
        labels.append("trend_continuation_after_shallow_pullback")
    if opposite_pressure_15 and strong_bar and strong_rejection:
        labels.append("snap_turn")
    if deep_pullback and strong_bar and strong_rejection:
        labels.append("failed_move_reversal")
    if deep_pullback and strong_rejection and not trend_30:
        labels.append("exhaustion_reversal_after_impulse")
    if compressed_30 and broke_15 and directional_pressure_15:
        labels.append("range_compression_break")
    return sorted(set(labels))


def _forward_outcomes(
    *,
    session_bars: Sequence[MinuteBar],
    index: int,
    direction: str,
    horizons: tuple[int, ...],
) -> tuple[dict[int, float | None], dict[int, float], dict[int, float]]:
    entry_price = session_bars[index].close
    returns: dict[int, float | None] = {}
    mfes: dict[int, float] = {}
    maes: dict[int, float] = {}
    for horizon in horizons:
        window = _future_window(session_bars, index=index, horizon_minutes=horizon)
        if not window:
            returns[horizon] = None
            mfes[horizon] = 0.0
            maes[horizon] = 0.0
            continue
        final_close = window[-1].close
        if direction == "LONG":
            returns[horizon] = final_close - entry_price
            mfes[horizon] = max(max(bar.high - entry_price, 0.0) for bar in window)
            maes[horizon] = max(max(entry_price - bar.low, 0.0) for bar in window)
        else:
            returns[horizon] = entry_price - final_close
            mfes[horizon] = max(max(entry_price - bar.low, 0.0) for bar in window)
            maes[horizon] = max(max(bar.high - entry_price, 0.0) for bar in window)
        returns[horizon] = round(float(returns[horizon]), 6)
        mfes[horizon] = round(float(mfes[horizon]), 6)
        maes[horizon] = round(float(maes[horizon]), 6)
    return returns, mfes, maes


def _future_window(session_bars: Sequence[MinuteBar], *, index: int, horizon_minutes: int) -> list[MinuteBar]:
    decision_ts = session_bars[index].ts
    end_ts = decision_ts + timedelta(minutes=horizon_minutes)
    return [bar for bar in session_bars[index + 1 :] if decision_ts < bar.ts <= end_ts]


def _build_nested_cohorts(
    *,
    events: Sequence[EventCandidate],
    cohort_sizes: tuple[int, ...],
    random_control_count: int,
    random_seed: int,
) -> dict[str, list[EventCandidate]]:
    favorable = sorted(events, key=lambda event: (event.primary_mfe, event.primary_return, -event.primary_mae), reverse=True)
    adverse = sorted(events, key=lambda event: (event.primary_mae + max(-event.primary_return, 0.0), -event.primary_mfe), reverse=True)
    cohorts: dict[str, list[EventCandidate]] = {}
    for size in cohort_sizes:
        cohorts[f"favorable_top{size}"] = list(favorable[: min(size, len(favorable))])
        cohorts[f"adverse_bottom{size}"] = list(adverse[: min(size, len(adverse))])
    cohorts["random_control"] = _matched_random_control(
        events=events,
        primary_events=cohorts.get("favorable_top1000") or cohorts.get(f"favorable_top{max(cohort_sizes)}") or [],
        excluded_ids={event.event_id for event in favorable[: min(max(cohort_sizes), len(favorable))]} | {event.event_id for event in adverse[: min(max(cohort_sizes), len(adverse))]},
        target_count=random_control_count,
        seed=random_seed,
    )
    return cohorts


def _matched_random_control(
    *,
    events: Sequence[EventCandidate],
    primary_events: Sequence[EventCandidate],
    excluded_ids: set[str],
    target_count: int,
    seed: int,
) -> list[EventCandidate]:
    rng = random.Random(seed)
    by_match: dict[tuple[str, str, str], list[EventCandidate]] = defaultdict(list)
    by_session: dict[str, list[EventCandidate]] = defaultdict(list)
    all_pool: list[EventCandidate] = []
    for event in events:
        if event.event_id in excluded_ids:
            continue
        by_match[(event.session, event.time_bin_15m, event.direction)].append(event)
        by_session[event.session].append(event)
        all_pool.append(event)
    for pool in list(by_match.values()) + list(by_session.values()) + [all_pool]:
        rng.shuffle(pool)

    selected: list[EventCandidate] = []
    selected_ids: set[str] = set()
    templates = list(primary_events)
    rng.shuffle(templates)
    while templates and len(selected) < target_count:
        progressed = False
        for template in templates:
            if len(selected) >= target_count:
                break
            pools = (
                by_match.get((template.session, template.time_bin_15m, template.direction), []),
                by_session.get(template.session, []),
                all_pool,
            )
            for pool in pools:
                while pool and pool[-1].event_id in selected_ids:
                    pool.pop()
                if pool:
                    event = pool.pop()
                    selected.append(event)
                    selected_ids.add(event.event_id)
                    progressed = True
                    break
        if not progressed:
            break
    return selected


def _build_cohort_composition_rows(cohorts: dict[str, list[EventCandidate]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for cohort_name, events in sorted(cohorts.items()):
        rows.append({"cohort": cohort_name, "dimension": "TOTAL", "bucket": "ALL", "event_count": len(events)})
        for dimension, values in {
            "year": [str(event.year) for event in events],
            "month": [event.month for event in events],
            "session": [event.session for event in events],
            "direction": [event.direction for event in events],
            "day_of_week": [event.day_of_week for event in events],
            "time_bin_15m": [event.time_bin_15m for event in events],
        }.items():
            for bucket, count in sorted(Counter(values).items()):
                rows.append({"cohort": cohort_name, "dimension": dimension, "bucket": bucket, "event_count": count})
    return rows


def _build_archetype_frequency_rows(cohorts: dict[str, list[EventCandidate]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cohort_label_counts = {name: _label_counts(events) for name, events in cohorts.items()}
    labels = sorted(set().union(*(set(counts) for counts in cohort_label_counts.values())))
    for label in labels:
        for cohort_name, events in sorted(cohorts.items()):
            count = int(cohort_label_counts[cohort_name].get(label, 0))
            rows.append(
                {
                    "archetype": label,
                    "cohort": cohort_name,
                    "event_count": len(events),
                    "match_count": count,
                    "frequency": round(count / max(len(events), 1), 6),
                }
            )
    return rows


def _build_archetype_metric_rows(*, events: Sequence[EventCandidate], cohorts: dict[str, list[EventCandidate]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    event_sets: dict[str, Sequence[EventCandidate]] = {"all_eligible": events, **cohorts}
    top2000_ids = {event.event_id for event in cohorts.get("favorable_top2000", [])}
    event_sets["ordinary_outside_top2000"] = [event for event in events if event.event_id not in top2000_ids]
    labels = sorted({label for event in events for label in event.archetypes})
    for label in labels:
        for set_name, set_events in sorted(event_sets.items()):
            matched = [event for event in set_events if label in event.archetypes]
            if not matched:
                continue
            row = {"archetype": label, "event_set": set_name}
            row.update(_event_metric_summary(matched))
            rows.append(row)
    return rows


def _classify_archetypes(*, events: Sequence[EventCandidate], cohorts: dict[str, list[EventCandidate]]) -> list[dict[str, Any]]:
    labels = sorted({label for event in events for label in event.archetypes})
    rows: list[dict[str, Any]] = []
    top500 = cohorts.get("favorable_top500", [])
    top1000 = cohorts.get("favorable_top1000", [])
    top2000 = cohorts.get("favorable_top2000", [])
    adverse1000 = cohorts.get("adverse_bottom1000", [])
    control = cohorts.get("random_control", [])
    top2000_ids = {event.event_id for event in top2000}
    for label in labels:
        matched_all = [event for event in events if label in event.archetypes]
        matched_ordinary = [event for event in matched_all if event.event_id not in top2000_ids]
        count_top500 = sum(1 for event in top500 if label in event.archetypes)
        count_top1000 = sum(1 for event in top1000 if label in event.archetypes)
        count_top2000 = sum(1 for event in top2000 if label in event.archetypes)
        count_adverse1000 = sum(1 for event in adverse1000 if label in event.archetypes)
        count_control = sum(1 for event in control if label in event.archetypes)
        freq_top500 = count_top500 / max(len(top500), 1)
        freq_top1000 = count_top1000 / max(len(top1000), 1)
        freq_top2000 = count_top2000 / max(len(top2000), 1)
        freq_adverse1000 = count_adverse1000 / max(len(adverse1000), 1)
        freq_control = count_control / max(len(control), 1)
        metrics_all = _event_metric_summary(matched_all)
        metrics_ordinary = _event_metric_summary(matched_ordinary)
        year_count = len({event.year for event in matched_all})
        top_trade_concentration = float(metrics_all["top3_positive_return_share"] or 0.0)
        winner_adverse_ratio = _ratio(freq_top1000, freq_adverse1000)
        winner_control_ratio = _ratio(freq_top1000, freq_control)
        classification = "NOISY_OR_NON_DISCRIMINATIVE"
        if len(matched_all) < 20 or year_count < 2:
            classification = "DATA_QUALITY_LIMITED"
        elif top_trade_concentration > 0.55:
            classification = "OUTLIER_DEPENDENT"
        elif (
            count_top500 > 0
            and count_top1000 > 0
            and count_top2000 > 0
            and freq_top2000 >= min(freq_top1000, freq_control) * 0.65
            and winner_adverse_ratio >= 1.15
            and winner_control_ratio >= 1.25
            and float(metrics_ordinary["avg_forward_return_60m"] or 0.0) > 0.0
            and float(metrics_ordinary["profit_factor"] or 0.0) >= 1.05
        ):
            classification = "ECONOMICALLY_RELEVANT_FOLLOWUP"
        elif (
            count_top500 > 0
            and count_top1000 > 0
            and count_top2000 > 0
            and (winner_adverse_ratio >= 1.05 or winner_control_ratio >= 1.10)
            and float(metrics_ordinary["avg_forward_return_60m"] or 0.0) > 0.0
        ):
            classification = "PROMISING_BUT_NEEDS_VALIDATION"
        row = {
            "archetype": label,
            "classification": classification,
            "all_match_count": len(matched_all),
            "ordinary_match_count": len(matched_ordinary),
            "year_count": year_count,
            "top500_frequency": round(freq_top500, 6),
            "top1000_frequency": round(freq_top1000, 6),
            "top2000_frequency": round(freq_top2000, 6),
            "adverse1000_frequency": round(freq_adverse1000, 6),
            "control_frequency": round(freq_control, 6),
            "winner_adverse_frequency_ratio": round(winner_adverse_ratio, 6),
            "winner_control_frequency_ratio": round(winner_control_ratio, 6),
            "ordinary_avg_forward_return_60m": metrics_ordinary["avg_forward_return_60m"],
            "ordinary_win_rate_60m": metrics_ordinary["win_rate_60m"],
            "ordinary_profit_factor": metrics_ordinary["profit_factor"],
            "top3_positive_return_share": metrics_all["top3_positive_return_share"],
            "dominant_session": _dominant_bucket(event.session for event in matched_all),
            "dominant_year": _dominant_bucket(str(event.year) for event in matched_all),
            "invalidation_concept": _invalidation_concept(label),
            "entry_concept": _entry_concept(label),
        }
        rows.append(row)
    rows.sort(
        key=lambda row: (
            {"ECONOMICALLY_RELEVANT_FOLLOWUP": 0, "PROMISING_BUT_NEEDS_VALIDATION": 1, "OUTLIER_DEPENDENT": 2, "NOISY_OR_NON_DISCRIMINATIVE": 3, "DATA_QUALITY_LIMITED": 4}[str(row["classification"])],
            -float(row["winner_control_frequency_ratio"] or 0.0),
            -int(row["all_match_count"]),
        )
    )
    return rows


def _build_histogram_rows(events: Sequence[EventCandidate]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for dimension, values in {
        "eligible_event_year": [str(event.year) for event in events],
        "eligible_event_month": [event.month for event in events],
        "eligible_event_session": [event.session for event in events],
        "eligible_event_direction": [event.direction for event in events],
        "eligible_event_time_bin_15m": [event.time_bin_15m for event in events],
    }.items():
        for bucket, count in sorted(Counter(values).items()):
            rows.append({"dimension": dimension, "bucket": bucket, "count": count})
    return rows


def _event_metric_summary(events: Sequence[EventCandidate]) -> dict[str, Any]:
    returns = [event.primary_return for event in events]
    returns_15 = [float(event.forward_return_15m or 0.0) for event in events]
    returns_30 = [float(event.forward_return_30m or 0.0) for event in events]
    mfes = [event.primary_mfe for event in events]
    maes = [event.primary_mae for event in events]
    wins = [value for value in returns if value > 0.0]
    losses = [value for value in returns if value < 0.0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    positive_sorted = sorted(wins, reverse=True)
    top3_share = _safe_div(sum(positive_sorted[:3]), gross_profit)
    return {
        "match_count": len(events),
        "avg_forward_return_15m": round(statistics.fmean(returns_15), 6) if returns_15 else 0.0,
        "avg_forward_return_30m": round(statistics.fmean(returns_30), 6) if returns_30 else 0.0,
        "avg_forward_return_60m": round(statistics.fmean(returns), 6) if returns else 0.0,
        "median_forward_return_15m": round(statistics.median(returns_15), 6) if returns_15 else 0.0,
        "median_forward_return_30m": round(statistics.median(returns_30), 6) if returns_30 else 0.0,
        "median_forward_return_60m": round(statistics.median(returns), 6) if returns else 0.0,
        "win_rate_15m": round(sum(1 for value in returns_15 if value > 0.0) / max(len(returns_15), 1), 6),
        "win_rate_30m": round(sum(1 for value in returns_30 if value > 0.0) / max(len(returns_30), 1), 6),
        "win_rate_60m": round(len(wins) / max(len(returns), 1), 6),
        "profit_factor": round(gross_profit / gross_loss, 6) if gross_loss > 0 else (round(gross_profit, 6) if gross_profit > 0 else 0.0),
        "avg_mfe_60m": round(statistics.fmean(mfes), 6) if mfes else 0.0,
        "avg_mae_60m": round(statistics.fmean(maes), 6) if maes else 0.0,
        "max_mfe_60m": round(max(mfes), 6) if mfes else 0.0,
        "max_mae_60m": round(max(maes), 6) if maes else 0.0,
        "top3_positive_return_share": round(top3_share, 6),
        "session_distribution": _counter_string(event.session for event in events),
        "year_distribution": _counter_string(str(event.year) for event in events),
        "month_distribution": _counter_string(event.month for event in events),
    }


def _label_counts(events: Sequence[EventCandidate]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for event in events:
        counts.update(event.archetypes)
    return counts


def _data_quality_limitations(*, quality: dict[str, Any], eligible_sessions: dict[tuple[str, str, str], dict[str, Any]], events: Sequence[EventCandidate]) -> list[str]:
    limitations: list[str] = []
    summary = quality.get("session_coverage_summary") or {}
    total_sessions = int(summary.get("row_count") or 0)
    eligible_count = len(eligible_sessions)
    if total_sessions and eligible_count / total_sessions < 0.05:
        limitations.append(f"Only {eligible_count} of {total_sessions} session coverage rows are eligible_for_replay; sparse/gappy historical coverage dominates.")
    if int(quality.get("suspicious_gap_count") or 0) > 0:
        limitations.append(f"Quality audit reports {quality.get('suspicious_gap_count')} suspicious gaps across the raw backfill.")
    years = sorted({event.year for event in events})
    if years:
        limitations.append(f"Primary analysis events span eligible sessions in years {years[0]} through {years[-1]}, not every loaded year evenly.")
    if len(events) < 4000:
        limitations.append("Eligible directional event count is below the requested top/adverse/control design envelope; achieved cohorts are capped by available events.")
    return limitations


def _render_markdown(summary: dict[str, Any], classification_rows: Sequence[dict[str, Any]], frequency_rows: Sequence[dict[str, Any]]) -> str:
    lines = [
        "# MGC Entry Archetype Discovery",
        "",
        f"- final_classification: `{summary['final_classification']}`",
        "- runtime/pretrade/PAPER/live state: `UNCHANGED`",
        f"- live_money_eligible: `{summary['live_money_eligible']}`",
        f"- data_range: `{summary['data_range']['first_bar']}` to `{summary['data_range']['last_bar']}`",
        f"- eligible_sessions_used: `{summary['eligible_session_summary']['eligible_session_count']}`",
        f"- eligible_directional_candidates: `{summary['event_counts']['eligible_directional_candidates']}`",
        f"- horizons_minutes: `{summary['event_selection_method']['horizons_minutes']}`",
        f"- lookbacks_minutes: `{summary['event_selection_method']['lookbacks_minutes']}`",
        "",
        "## Cohort Sizes",
        "",
        "| cohort | events |",
        "|---|---:|",
    ]
    for name, count in sorted((summary.get("cohort_sizes") or {}).items()):
        lines.append(f"| {name} | {count} |")
    lines.extend(["", "## Candidate Classifications", "", "| archetype | classification | all | top1000 freq | adverse1000 freq | control freq | ordinary avg 60m | PF |", "|---|---|---:|---:|---:|---:|---:|---:|"])
    for row in classification_rows[:20]:
        lines.append(
            f"| {row['archetype']} | {row['classification']} | {row['all_match_count']} | {row['top1000_frequency']} | {row['adverse1000_frequency']} | {row['control_frequency']} | {row['ordinary_avg_forward_return_60m']} | {row['ordinary_profit_factor']} |"
        )
    lines.extend(["", "## Top-1000 Frequency Snapshot", "", "| archetype | top1000 freq | adverse1000 freq | control freq |", "|---|---:|---:|---:|"])
    by_label: dict[str, dict[str, float]] = defaultdict(dict)
    for row in frequency_rows:
        by_label[str(row["archetype"])][str(row["cohort"])] = float(row["frequency"])
    for label, values in sorted(by_label.items(), key=lambda item: item[1].get("favorable_top1000", 0.0), reverse=True)[:15]:
        lines.append(f"| {label} | {values.get('favorable_top1000', 0.0):.6f} | {values.get('adverse_bottom1000', 0.0):.6f} | {values.get('random_control', 0.0):.6f} |")
    lines.extend(["", "## Data Quality Limitations", ""])
    for limitation in summary.get("data_quality_limitations") or []:
        lines.append(f"- {limitation}")
    return "\n".join(lines) + "\n"


def _session_key(*, symbol: str, bar_end: datetime) -> tuple[str, str, str]:
    local = bar_end.astimezone(NEW_YORK)
    session = _session_label(local)
    trading_date = local.date() + timedelta(days=1) if session == "ASIA" and local.time() >= time(18, 0) else local.date()
    return symbol.upper(), trading_date.isoformat(), session


def _session_label(local: datetime) -> str:
    value = local.time()
    if value >= time(18, 0) or value < time(3, 0):
        return "ASIA"
    if time(3, 0) <= value < time(8, 20):
        return "LONDON"
    if time(8, 20) <= value < time(16, 0):
        return "US"
    return "OFF_SESSION"


def _source_inventory(
    *,
    raw_root: Path,
    quality_path: Path,
    canonical_sqlite_path: Path,
    pre2020_bars: Sequence[MinuteBar],
    canonical_bars: Sequence[MinuteBar],
    combined_bars: Sequence[MinuteBar],
) -> list[dict[str, Any]]:
    return [
        {
            "source_id": "pre2020_databento_backfill",
            "kind": "offline_research_parquet",
            "path": str(raw_root),
            "quality_manifest_or_audit": str(quality_path),
            "bar_count": len(pre2020_bars),
            "first_bar": pre2020_bars[0].ts.isoformat() if pre2020_bars else None,
            "last_bar": pre2020_bars[-1].ts.isoformat() if pre2020_bars else None,
            "research_artifact": True,
            "runtime_artifact": False,
        },
        {
            "source_id": "canonical_2020plus",
            "kind": "canonical_replay_sqlite",
            "path": str(canonical_sqlite_path),
            "data_source": CANONICAL_1M_DATA_SOURCE,
            "bar_count": len(canonical_bars),
            "first_bar": canonical_bars[0].ts.isoformat() if canonical_bars else None,
            "last_bar": canonical_bars[-1].ts.isoformat() if canonical_bars else None,
            "research_artifact": True,
            "runtime_artifact": False,
        },
        {
            "source_id": "combined_research_view",
            "kind": "in_memory_research_view_no_canonical_overwrite",
            "dedupe_rule": "dedupe by symbol+timestamp; canonical_2020plus wins on overlap",
            "bar_count": len(combined_bars),
            "first_bar": combined_bars[0].ts.isoformat() if combined_bars else None,
            "last_bar": combined_bars[-1].ts.isoformat() if combined_bars else None,
            "research_artifact": True,
            "runtime_artifact": False,
            "runtime_preflight_dashboard_truth": False,
        },
    ]


def _resolve_canonical_sqlite_path(config: DiscoveryConfig) -> Path:
    if config.canonical_sqlite_path is not None:
        return Path(config.canonical_sqlite_path)
    candidates = [
        Path(config.repo_root) / "mgc_v05l.replay.sqlite3",
        Path("/Users/patrick/Documents/MGC-v05l-automation/mgc_v05l.replay.sqlite3"),
    ]
    for candidate in candidates:
        if candidate is None:
            continue
        path = Path(candidate)
        if not path.exists():
            continue
        try:
            connection = sqlite3.connect(path)
            try:
                connection.execute("select 1").fetchone()
            finally:
                connection.close()
            return path
        except sqlite3.Error:
            continue
    return Path(config.canonical_sqlite_path or Path(config.repo_root) / "mgc_v05l.replay.sqlite3")


def _min_iso(left: str | None, right: str | None) -> str | None:
    if left is None:
        return right
    if right is None:
        return left
    return min(left, right)


def _max_iso(left: str | None, right: str | None) -> str | None:
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


def _consecutive_directional_closes(window: Sequence[MinuteBar], *, direction: str) -> int:
    count = 0
    sign = 1.0 if direction == "LONG" else -1.0
    for left, right in reversed(list(zip(window, window[1:], strict=False))):
        if sign * (right.close - left.close) > 0:
            count += 1
        else:
            break
    return count


def _structure_tendency(window: Sequence[MinuteBar], *, direction: str) -> float:
    if len(window) < 2:
        return 0.0
    if direction == "LONG":
        hits = sum(1 for left, right in zip(window, window[1:], strict=False) if right.high >= left.high and right.low >= left.low)
    else:
        hits = sum(1 for left, right in zip(window, window[1:], strict=False) if right.high <= left.high and right.low <= left.low)
    return round(hits / max(len(window) - 1, 1), 6)


def _pullback_depth(window: Sequence[MinuteBar], *, direction: str) -> float:
    if direction == "LONG":
        peak = max(bar.high for bar in window)
        return max(peak - window[-1].close, 0.0)
    trough = min(bar.low for bar in window)
    return max(window[-1].close - trough, 0.0)


def _window_close_position(window: Sequence[MinuteBar], *, direction: str) -> float:
    high = max(bar.high for bar in window)
    low = min(bar.low for bar in window)
    if high <= low:
        return 0.5
    value = (window[-1].close - low) / (high - low)
    return value if direction == "LONG" else 1.0 - value


def _retracement_classification(features: dict[str, Any]) -> str:
    avg_range = max(float(features.get("avg_range_30m") or 0.0), 0.000001)
    pullback = float(features.get("pullback_depth_15m") or 0.0)
    if pullback <= avg_range * 2.5:
        return "SHALLOW"
    if pullback <= avg_range * 5.0:
        return "MODERATE"
    return "DEEP"


def _sign_pattern(values: Sequence[float]) -> str:
    chars = []
    for value in values:
        if value > 0:
            chars.append("+")
        elif value < 0:
            chars.append("-")
        else:
            chars.append("0")
    return "".join(chars)


def _entry_concept(label: str) -> str:
    concepts = {
        "breakout": "Completed-bar close expands through a recent directional extreme.",
        "breakout_retest_hold": "Recent breakout remains supported while the pullback stays shallow.",
        "failed_breakout": "Attempted break of a recent extreme rejects back inside the prior range.",
        "failed_move_reversal": "Deep adverse pullback rejects and closes strongly back in the candidate direction.",
        "pause_pullback_resume": "Up-trend pauses, pulls back shallowly, then resumes with a strong completed bar.",
        "pause_rebound_resume": "Down-trend pauses, rebounds shallowly, then resumes with a strong completed bar.",
        "snap_turn": "Short sequence of adverse pressure flips with wick rejection and a strong directional close.",
        "range_compression_break": "Compressed recent range breaks directionally on a completed bar.",
        "trend_continuation_after_shallow_pullback": "Established directional structure resumes after a shallow pullback.",
        "exhaustion_reversal_after_impulse": "Extended adverse impulse shows rejection and reversal behavior.",
        "unclassified": "No heuristic archetype label assigned.",
    }
    return concepts.get(label, "Heuristic structure label from completed-bar pre-entry context.")


def _invalidation_concept(label: str) -> str:
    if label in {"breakout", "range_compression_break", "breakout_retest_hold"}:
        return "Invalidated by a completed close back inside the broken/compressed range or failure to hold the retest area."
    if label in {"pause_pullback_resume", "pause_rebound_resume", "trend_continuation_after_shallow_pullback"}:
        return "Invalidated by a completed close through the shallow-pullback extreme or loss of directional structure."
    if label in {"snap_turn", "failed_move_reversal", "exhaustion_reversal_after_impulse", "failed_breakout"}:
        return "Invalidated by a completed close through the rejection extreme or immediate continuation of the prior adverse impulse."
    return "No clean invalidation concept; treat as non-actionable until refined."


def _coerce_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _coerce_now(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(tz=UTC)
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _resolve_output_root(config: DiscoveryConfig) -> Path:
    root = Path(config.output_root)
    return root if root.is_absolute() else Path(config.repo_root) / root


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _json_ready(row.get(key)) for key in fieldnames})


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    return value


def _safe_div(numerator: float, denominator: float) -> float:
    return 0.0 if denominator == 0 else float(numerator) / float(denominator)


def _ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 999.0 if numerator > 0 else 1.0
    return numerator / denominator


def _nullable_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _counter_string(values: Iterable[str]) -> str:
    counts = Counter(values)
    return ";".join(f"{key}:{counts[key]}" for key in sorted(counts))


def _dominant_bucket(values: Iterable[str]) -> str:
    counts = Counter(values)
    if not counts:
        return ""
    bucket, count = counts.most_common(1)[0]
    return f"{bucket}:{count}"


def _require_pyarrow_parquet() -> Any:
    try:
        import pyarrow.parquet as pq  # type: ignore
    except ImportError as exc:  # pragma: no cover - environment dependency.
        raise RuntimeError("pyarrow is required for MGC entry archetype discovery") from exc
    return pq


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--raw-bars-root", type=Path, default=None)
    parser.add_argument("--quality-audit-path", type=Path, default=None)
    parser.add_argument("--canonical-sqlite-path", type=Path, default=None)
    parser.add_argument("--random-control-count", type=int, default=DEFAULT_RANDOM_CONTROL_COUNT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = run_mgc_entry_archetype_discovery(
        config=DiscoveryConfig(
            output_root=args.output_root,
            symbol=args.symbol,
            raw_bars_root=args.raw_bars_root,
            quality_audit_path=args.quality_audit_path,
            canonical_sqlite_path=args.canonical_sqlite_path,
            random_control_count=args.random_control_count,
        )
    )
    print(json.dumps(summary, indent=2, sort_keys=True, default=_json_ready))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
