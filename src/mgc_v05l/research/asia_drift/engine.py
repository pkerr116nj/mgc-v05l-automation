"""Research-only Asia Drift v1 Phase 1 orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

from ..trend_participation.models import ResearchBar
from ..trend_participation.storage import load_sqlite_bars, normalize_and_check_bars, resample_bars_from_1m
from .features import RECOVERY_CONFIRMED, build_feature_rows
from .models import AsiaDriftPhase1Artifacts
from .report import write_phase1_artifacts
from .state_machine import evaluate_state_machine, summarize_sessions


@dataclass(frozen=True)
class AsiaDriftPhase1Run:
    feature_rows: list[Any]
    state_rows: list[Any]
    session_summaries: list[Any]
    artifacts: AsiaDriftPhase1Artifacts


def run_asia_drift_phase1(
    *,
    source_sqlite_path: Path,
    output_dir: Path,
    instruments: tuple[str, ...] = ("MGC",),
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
    calibration_profile_name: str = RECOVERY_CONFIRMED,
) -> AsiaDriftPhase1Run:
    all_feature_rows: list[Any] = []
    quality_issues: list[dict[str, Any]] = []
    instrument_summary: dict[str, Any] = {}

    for instrument in instruments:
        bars_1m, bars_5m, issues = load_research_bars(
            source_sqlite_path=source_sqlite_path,
            instrument=instrument,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        quality_issues.extend(issues)
        feature_rows = build_feature_rows(bars_5m=bars_5m, calibration_profile_name=calibration_profile_name)
        all_feature_rows.extend(feature_rows)
        instrument_summary[instrument] = {
            "bar_count_1m": len(bars_1m),
            "bar_count_5m": len(bars_5m),
            "feature_count": len(feature_rows),
            "quality_issue_count": len(issues),
            "coverage_start": bars_1m[0].end_ts.isoformat() if bars_1m else None,
            "coverage_end": bars_1m[-1].end_ts.isoformat() if bars_1m else None,
        }

    state_rows = evaluate_state_machine(all_feature_rows)
    session_summaries = summarize_sessions(all_feature_rows, state_rows)
    artifacts = write_phase1_artifacts(
        output_dir=output_dir,
        feature_rows=all_feature_rows,
        state_rows=state_rows,
        session_summaries=session_summaries,
        data_summary={
            "source_sqlite_path": str(source_sqlite_path.resolve()),
            "instruments": list(instruments),
            "start_ts": start_ts.isoformat() if start_ts is not None else None,
            "end_ts": end_ts.isoformat() if end_ts is not None else None,
            "instrument_summary": instrument_summary,
            "quality_issue_count": len(quality_issues),
            "quality_issues": quality_issues[:50],
            "phase": "phase1_research_observability_only",
            "calibration_profile": calibration_profile_name,
        },
    )
    return AsiaDriftPhase1Run(
        feature_rows=all_feature_rows,
        state_rows=state_rows,
        session_summaries=session_summaries,
        artifacts=artifacts,
    )


def run_asia_drift_phase1_from_bars(
    *,
    output_dir: Path,
    bars_5m: Sequence[ResearchBar],
    bars_1m: Sequence[ResearchBar] | None = None,
    source_label: str = "synthetic",
    calibration_profile_name: str = RECOVERY_CONFIRMED,
) -> AsiaDriftPhase1Run:
    feature_rows = build_feature_rows(bars_5m=bars_5m, calibration_profile_name=calibration_profile_name)
    state_rows = evaluate_state_machine(feature_rows)
    session_summaries = summarize_sessions(feature_rows, state_rows)
    artifacts = write_phase1_artifacts(
        output_dir=output_dir,
        feature_rows=feature_rows,
        state_rows=state_rows,
        session_summaries=session_summaries,
        data_summary={
            "source_label": source_label,
            "bar_count_1m": len(bars_1m or ()),
            "bar_count_5m": len(bars_5m),
            "phase": "phase1_research_observability_only",
            "calibration_profile": calibration_profile_name,
        },
    )
    return AsiaDriftPhase1Run(
        feature_rows=list(feature_rows),
        state_rows=state_rows,
        session_summaries=session_summaries,
        artifacts=artifacts,
    )


def load_research_bars(
    *,
    source_sqlite_path: Path,
    instrument: str,
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
) -> tuple[list[ResearchBar], list[ResearchBar], list[dict[str, Any]]]:
    raw_1m = load_sqlite_bars(
        sqlite_path=source_sqlite_path,
        instrument=instrument,
        timeframe="1m",
        start_ts=start_ts,
        end_ts=end_ts,
    )
    normalized_1m, issues_1m = normalize_and_check_bars(bars=raw_1m, timeframe="1m")
    raw_5m = load_sqlite_bars(
        sqlite_path=source_sqlite_path,
        instrument=instrument,
        timeframe="5m",
        start_ts=start_ts,
        end_ts=end_ts,
    )
    if not raw_5m and normalized_1m:
        raw_5m = resample_bars_from_1m(bars_1m=normalized_1m, target_timeframe="5m")
    normalized_5m, issues_5m = normalize_and_check_bars(bars=raw_5m, timeframe="5m")
    if normalized_1m:
        first_1m_ts = normalized_1m[0].end_ts
        last_1m_ts = normalized_1m[-1].end_ts
        normalized_5m = [bar for bar in normalized_5m if first_1m_ts <= bar.end_ts <= last_1m_ts]
    issue_rows = [
        {
            "instrument": issue.instrument,
            "timeframe": issue.timeframe,
            "issue_type": issue.issue_type,
            "severity": issue.severity,
            "message": issue.message,
            "bar_end_ts": issue.bar_end_ts.isoformat() if issue.bar_end_ts is not None else None,
        }
        for issue in [*issues_1m, *issues_5m]
    ]
    return normalized_1m, normalized_5m, issue_rows
