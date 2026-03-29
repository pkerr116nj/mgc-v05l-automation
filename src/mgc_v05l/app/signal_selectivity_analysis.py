"""Read-only signal selectivity analysis for live/paper/replay persisted runs."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from ..config_models import StrategySettings
from .session_phase_labels import label_session_phase


SIGNAL_FUNNEL_FIELDS = (
    ("bull_snap_turn_candidate", "bull_snap_turn_candidate"),
    ("firstBullSnapTurn", "first_bull_snap_turn"),
    ("asia_reclaim_bar_raw", "asia_reclaim_bar_raw"),
    ("asia_hold_bar_ok", "asia_hold_bar_ok"),
    ("asia_acceptance_bar_ok", "asia_acceptance_bar_ok"),
    ("asiaVWAPLongSignal", "asia_vwap_long_signal"),
    ("bear_snap_turn_candidate", "bear_snap_turn_candidate"),
    ("firstBearSnapTurn", "first_bear_snap_turn"),
    ("longEntryRaw", "long_entry_raw"),
    ("shortEntryRaw", "short_entry_raw"),
    ("longEntry", "long_entry"),
    ("shortEntry", "short_entry"),
)

FAMILY_ORDER = ("bullSnapLong", "asiaVWAPLong", "bearSnapShort")

FAMILY_SPECS: dict[str, dict[str, Any]] = {
    "bullSnapLong": {
        "raw_field": "first_bull_snap_turn",
        "final_field": "long_entry",
        "final_source_field": "long_entry_source",
        "final_source_value": "firstBullSnapTurn",
        "candidate_field": "bull_snap_turn_candidate",
        "recent_field": "recent_long_setup",
        "predicate_fields": (
            ("downside stretch", "bull_snap_downside_stretch_ok"),
            ("range", "bull_snap_range_ok"),
            ("body", "bull_snap_body_ok"),
            ("close strength", "bull_snap_close_strong"),
            ("velocity", "bull_snap_velocity_ok"),
            ("reversal bar", "bull_snap_reversal_bar"),
            ("location", "bull_snap_location_ok"),
            ("session allowed", "session_allowed"),
        ),
    },
    "asiaVWAPLong": {
        "raw_field": "asia_vwap_long_signal",
        "final_field": "long_entry",
        "final_source_field": "long_entry_source",
        "final_source_value": "asiaVWAPLongSignal",
        "candidate_field": "asia_reclaim_bar_raw",
        "recent_field": "recent_long_setup",
        "predicate_fields": (
            ("below-vwap recent", "below_vwap_recently"),
            ("reclaim range", "reclaim_range_ok"),
            ("reclaim volume", "reclaim_vol_ok"),
            ("reclaim color", "reclaim_color_ok"),
            ("reclaim close", "reclaim_close_ok"),
            ("hold checks", "asia_hold_bar_ok"),
            ("acceptance checks", "asia_acceptance_bar_ok"),
            ("session allowed", "session_allowed"),
        ),
    },
    "bearSnapShort": {
        "raw_field": "first_bear_snap_turn",
        "final_field": "short_entry",
        "final_source_field": "short_entry_source",
        "final_source_value": "firstBearSnapTurn",
        "candidate_field": "bear_snap_turn_candidate",
        "recent_field": "recent_short_setup",
        "predicate_fields": (
            ("upside stretch", "bear_snap_up_stretch_ok"),
            ("range", "bear_snap_range_ok"),
            ("body", "bear_snap_body_ok"),
            ("weak close", "bear_snap_close_weak"),
            ("velocity", "bear_snap_velocity_ok"),
            ("reversal bar", "bear_snap_reversal_bar"),
            ("location", "bear_snap_location_ok"),
            ("session allowed", "session_allowed"),
        ),
    },
}

SESSION_SEGMENTS = ("ASIA", "LONDON", "US", "UNKNOWN")
PRIMARY_DATASET_PURPOSES = {"live", "paper", "replay"}
BEAR_SNAP_UP_STRETCH_LADDER = (
    ("1.00", "bear_snap_up_stretch_1_00"),
    ("0.90", "bear_snap_up_stretch_0_90"),
    ("0.80", "bear_snap_up_stretch_0_80"),
    ("0.70", "bear_snap_up_stretch_0_70"),
)
BEAR_SNAP_RANGE_LADDER = (
    ("0.90", "bear_snap_range_0_90"),
    ("0.80", "bear_snap_range_0_80"),
    ("0.70", "bear_snap_range_0_70"),
    ("0.60", "bear_snap_range_0_60"),
)


@dataclass(frozen=True)
class SignalSelectivityDataset:
    dataset_id: str
    label: str
    dataset_kind: str
    database_path: Path
    comparison_role: str | None = None
    note: str | None = None


def default_signal_selectivity_artifact_dir(repo_root: Path) -> Path:
    return repo_root / "outputs" / "probationary_pattern_engine" / "signal_selectivity_analysis"


def default_signal_selectivity_dataset_specs(repo_root: Path) -> list[SignalSelectivityDataset]:
    specs: list[SignalSelectivityDataset] = []
    live_runs = sorted(
        (repo_root / "outputs" / "probationary_pattern_engine" / "live_strategy_pilot_runs").glob("*/live_strategy_pilot.sqlite3"),
        key=lambda path: path.stat().st_mtime,
    )
    if live_runs:
        specs.append(
            SignalSelectivityDataset(
                dataset_id="live_pilot_latest",
                label=f"Live Pilot {live_runs[-1].parent.name}",
                dataset_kind="live",
                database_path=live_runs[-1],
                note="Latest armed live-pilot persisted signal history.",
            )
        )

    paper_unattended = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime" / "paper_soak_unattended" / "validation.sqlite3"
    if paper_unattended.exists():
        specs.append(
            SignalSelectivityDataset(
                dataset_id="paper_soak_unattended",
                label="Paper Soak Unattended",
                dataset_kind="paper",
                database_path=paper_unattended,
                note="Real paper-runtime unattended soak validation dataset.",
            )
        )

    historical_playback = repo_root / "outputs" / "historical_playback" / "historical_playback_mgc_practical_mgc_uslate_1m_to_5m.sqlite3"
    if historical_playback.exists():
        specs.append(
            SignalSelectivityDataset(
                dataset_id="historical_playback_practical",
                label="Historical Playback Practical MGC US Late",
                dataset_kind="replay",
                database_path=historical_playback,
                note="Replay-first historical playback on persisted MGC 1m bars resampled to 5m.",
            )
        )

    comparison_dir = repo_root / "outputs" / "signal_selectivity_analysis" / "replays"
    before_db = comparison_dir / "historical_playback_mgc_bear_snap_location_strict.sqlite3"
    after_db = comparison_dir / "historical_playback_mgc_bear_snap_location_current.sqlite3"
    if before_db.exists():
        specs.append(
            SignalSelectivityDataset(
                dataset_id="bear_snap_location_before",
                label="Bear Snap Location Before",
                dataset_kind="replay",
                database_path=before_db,
                comparison_role="before",
                note="Strict Bear Snap location gate replay on the historical playback slice.",
            )
        )
    if after_db.exists():
        specs.append(
            SignalSelectivityDataset(
                dataset_id="bear_snap_location_after",
                label="Bear Snap Location After",
                dataset_kind="replay",
                database_path=after_db,
                comparison_role="after",
                note="Current Bear Snap location gate replay on the same historical playback slice.",
            )
        )
    return specs


def build_signal_selectivity_analysis(
    *,
    settings: StrategySettings,
    repo_root: Path,
    dataset_specs: list[SignalSelectivityDataset] | None = None,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    generated_at = (observed_at or datetime.now(settings.timezone_info)).isoformat()
    specs = dataset_specs or default_signal_selectivity_dataset_specs(repo_root)
    datasets = [summarize_signal_selectivity_dataset(spec, settings=settings) for spec in specs if spec.database_path.exists()]
    live_dataset = next((dataset for dataset in datasets if dataset.get("dataset_kind") == "live"), {})
    primary_days = [
        day
        for dataset in datasets
        if dataset.get("dataset_kind") in PRIMARY_DATASET_PURPOSES and not dataset.get("comparison_role")
        for day in list(dataset.get("days") or [])
    ]
    regime_summary = _aggregate_regime_summary(primary_days)
    before_after = _build_before_after_comparison(datasets)
    bear_snap_up_stretch_ladder = _build_bear_snap_up_stretch_ladder(repo_root=repo_root, settings=settings)
    bear_snap_range_ladder = _build_bear_snap_range_ladder(repo_root=repo_root, settings=settings)
    key_findings = _derive_key_findings(datasets, before_after, bear_snap_up_stretch_ladder, bear_snap_range_ladder)
    summary = {
        "available": bool(datasets),
        "generated_at": generated_at,
        "dataset_count": len(datasets),
        "dataset_ids": [str(dataset.get("dataset_id") or "") for dataset in datasets],
        "datasets": datasets,
        "live_pilot_focus": {
            "why_no_trade_so_far": _live_why_no_trade_so_far(live_dataset),
            "top_failed_predicates": dict(live_dataset.get("top_failed_predicates_by_family") or {}),
            "raw_candidates_vs_final_entries": dict(live_dataset.get("raw_candidates_vs_final_entries") or {}),
            "anti_churn": dict(live_dataset.get("anti_churn") or {}),
        },
        "regime_comparison": regime_summary,
        "before_after_bear_snap_location": before_after,
        "bear_snap_up_stretch_ladder": bear_snap_up_stretch_ladder,
        "bear_snap_range_ladder": bear_snap_range_ladder,
        "key_findings": key_findings,
        "summary_line": _summary_line(live_dataset, before_after, bear_snap_up_stretch_ladder, bear_snap_range_ladder),
    }
    return summary


def write_signal_selectivity_analysis_artifacts(
    *,
    settings: StrategySettings,
    repo_root: Path,
    dataset_specs: list[SignalSelectivityDataset] | None = None,
    observed_at: datetime | None = None,
) -> tuple[dict[str, Any], Path, Path]:
    artifact_dir = default_signal_selectivity_artifact_dir(repo_root)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    summary = build_signal_selectivity_analysis(
        settings=settings,
        repo_root=repo_root,
        dataset_specs=dataset_specs,
        observed_at=observed_at,
    )
    json_path = artifact_dir / "signal_selectivity_analysis_latest.json"
    md_path = artifact_dir / "signal_selectivity_analysis_latest.md"
    json_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(_build_signal_selectivity_markdown(summary), encoding="utf-8")
    return summary, json_path, md_path


def summarize_signal_selectivity_dataset(spec: SignalSelectivityDataset, *, settings: StrategySettings) -> dict[str, Any]:
    rows = _load_signal_rows(spec.database_path)
    processed_bar_count = len(rows)
    by_segment: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_segment[row["session_segment"]].append(row)
        by_day[row["session_date"]].append(row)

    funnel = _compute_funnel_metrics(rows)
    failures = _compute_family_failure_summary(rows)
    anti_churn = _compute_anti_churn_summary(rows)
    session_breakdown = {
        segment: _build_subset_summary(segment_rows)
        for segment, segment_rows in sorted(by_segment.items())
    }
    day_summaries = []
    for session_date, day_rows in sorted(by_day.items()):
        bars = [row["bar"] for row in day_rows if row.get("bar")]
        regime = _classify_regime(bars)
        day_summaries.append(
            {
                "session_date": session_date,
                "regime": regime,
                "dataset_id": spec.dataset_id,
                "dataset_kind": spec.dataset_kind,
                **_build_subset_summary(day_rows),
            }
        )
    top_failed = {
        family: summary["primary_blockers"][:5]
        for family, summary in failures.items()
    }
    family_raw_candidate_counts = {
        family: int(spec_data["raw_candidate_count"])
        for family, spec_data in failures.items()
    }
    family_final_entry_counts = {
        family: int(spec_data["final_entry_count"])
        for family, spec_data in failures.items()
    }
    return {
        "dataset_id": spec.dataset_id,
        "label": spec.label,
        "dataset_kind": spec.dataset_kind,
        "database_path": str(spec.database_path),
        "comparison_role": spec.comparison_role,
        "note": spec.note,
        "processed_bars": processed_bar_count,
        "latest_bar_id": rows[-1]["bar_id"] if rows else None,
        "latest_bar_end_ts": rows[-1]["bar_end_ts"] if rows else None,
        "funnel": funnel,
        "family_failures": failures,
        "anti_churn": anti_churn,
        "session_breakdown": session_breakdown,
        "days": day_summaries,
        "top_failed_predicates_by_family": top_failed,
        "raw_candidates_vs_final_entries": funnel["raw_candidates_vs_final_entries"],
        "family_raw_candidate_counts": family_raw_candidate_counts,
        "family_final_entry_counts": family_final_entry_counts,
        "why_no_trade_so_far": _dataset_why_no_trade_so_far(funnel, top_failed, anti_churn),
    }


def _load_signal_rows(database_path: Path) -> list[dict[str, Any]]:
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    rows: list[dict[str, Any]] = []
    try:
        query = """
        SELECT
          s.bar_id,
          s.payload_json,
          s.created_at,
          b.end_ts,
          b.open,
          b.high,
          b.low,
          b.close,
          b.session_asia,
          b.session_london,
          b.session_us,
          b.session_allowed
        FROM signals s
        LEFT JOIN bars b ON b.bar_id = s.bar_id
        ORDER BY s.created_at ASC, s.bar_id ASC
        """
        for row in connection.execute(query):
            payload = json.loads(str(row["payload_json"])) if row["payload_json"] else {}
            if not isinstance(payload, dict):
                payload = {}
            bar_end_ts = _parse_timestamp(row["end_ts"] or row["created_at"])
            session_segment = _coarse_session_segment(row, bar_end_ts)
            session_date = bar_end_ts.date().isoformat() if bar_end_ts is not None else "unknown"
            rows.append(
                {
                    "bar_id": str(row["bar_id"] or payload.get("bar_id") or ""),
                    "payload": payload,
                    "bar_end_ts": bar_end_ts.isoformat() if bar_end_ts is not None else None,
                    "session_segment": session_segment,
                    "session_date": session_date,
                    "bar": {
                        "open": float(row["open"]) if row["open"] is not None else None,
                        "high": float(row["high"]) if row["high"] is not None else None,
                        "low": float(row["low"]) if row["low"] is not None else None,
                        "close": float(row["close"]) if row["close"] is not None else None,
                        "end_ts": bar_end_ts.isoformat() if bar_end_ts is not None else None,
                    },
                    "session_allowed": bool(row["session_allowed"]) if row["session_allowed"] is not None else bool(payload.get("session_allowed", False)),
                }
            )
    finally:
        connection.close()
    return rows


def _build_subset_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    funnel = _compute_funnel_metrics(rows)
    failures = _compute_family_failure_summary(rows)
    anti_churn = _compute_anti_churn_summary(rows)
    return {
        "completed_bars": len(rows),
        "funnel_counts": funnel["counts"],
        "entries_per_100_bars": funnel["entries_per_100_bars"],
        "raw_candidates_vs_final_entries": funnel["raw_candidates_vs_final_entries"],
        "family_shares": funnel["family_shares"],
        "top_failed_predicates_by_family": {
            family: summary["primary_blockers"][:5]
            for family, summary in failures.items()
        },
        "anti_churn": anti_churn,
    }


def _compute_funnel_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = {label: 0 for label, _ in SIGNAL_FUNNEL_FIELDS}
    family_raw_candidates = Counter[str]()
    family_final_entries = Counter[str]()
    for row in rows:
        payload = row["payload"]
        for label, field in SIGNAL_FUNNEL_FIELDS:
            if bool(payload.get(field)):
                counts[label] += 1
        if bool(payload.get("first_bull_snap_turn")):
            family_raw_candidates["bullSnapLong"] += 1
        if bool(payload.get("asia_vwap_long_signal")):
            family_raw_candidates["asiaVWAPLong"] += 1
        if bool(payload.get("first_bear_snap_turn")):
            family_raw_candidates["bearSnapShort"] += 1
        if str(payload.get("long_entry_source") or "").strip() == "firstBullSnapTurn" and bool(payload.get("long_entry")):
            family_final_entries["bullSnapLong"] += 1
        if str(payload.get("long_entry_source") or "").strip() == "asiaVWAPLongSignal" and bool(payload.get("long_entry")):
            family_final_entries["asiaVWAPLong"] += 1
        if str(payload.get("short_entry_source") or "").strip() == "firstBearSnapTurn" and bool(payload.get("short_entry")):
            family_final_entries["bearSnapShort"] += 1

    completed_bars = max(len(rows), 1)
    total_raw = sum(family_raw_candidates.values())
    total_final = sum(family_final_entries.values())
    long_raw = counts["longEntryRaw"]
    short_raw = counts["shortEntryRaw"]
    long_final = counts["longEntry"]
    short_final = counts["shortEntry"]
    return {
        "counts": counts,
        "entries_per_100_bars": {
            "long": round(long_final * 100.0 / completed_bars, 3),
            "short": round(short_final * 100.0 / completed_bars, 3),
            "total": round((long_final + short_final) * 100.0 / completed_bars, 3),
        },
        "raw_candidates_vs_final_entries": {
            "long": {
                "raw_candidates": long_raw,
                "final_entries": long_final,
                "conversion_rate_pct": round(long_final * 100.0 / long_raw, 3) if long_raw else 0.0,
            },
            "short": {
                "raw_candidates": short_raw,
                "final_entries": short_final,
                "conversion_rate_pct": round(short_final * 100.0 / short_raw, 3) if short_raw else 0.0,
            },
        },
        "family_shares": {
            family: {
                "raw_candidate_count": int(family_raw_candidates[family]),
                "raw_candidate_share_pct": round(family_raw_candidates[family] * 100.0 / total_raw, 3) if total_raw else 0.0,
                "final_entry_count": int(family_final_entries[family]),
                "final_entry_share_pct": round(family_final_entries[family] * 100.0 / total_final, 3) if total_final else 0.0,
            }
            for family in FAMILY_ORDER
        },
    }


def _compute_family_failure_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    denominator = max(len(rows), 1)
    for family in FAMILY_ORDER:
        spec = FAMILY_SPECS[family]
        false_counts = Counter[str]()
        primary_blockers = Counter[str]()
        raw_candidate_count = 0
        final_entry_count = 0
        anti_churn_suppressed_count = 0
        resolver_suppressed_count = 0

        for row in rows:
            payload = row["payload"]
            for label, field in spec["predicate_fields"]:
                value = row["session_allowed"] if field == "session_allowed" else bool(payload.get(field))
                if not value:
                    false_counts[label] += 1

            candidate = bool(payload.get(spec["candidate_field"]))
            raw_candidate = bool(payload.get(spec["raw_field"]))
            final_entry = bool(payload.get(spec["final_field"])) and str(payload.get(spec["final_source_field"]) or "").strip() == spec["final_source_value"]
            recent = bool(payload.get(spec["recent_field"]))

            if raw_candidate:
                raw_candidate_count += 1
            if final_entry:
                final_entry_count += 1

            blocker = _primary_blocker_for_family(row=row, family=family)
            if blocker:
                primary_blockers[blocker] += 1

            if candidate and not raw_candidate:
                false_counts["cooldown"] += 1
            if raw_candidate and not final_entry and recent:
                false_counts["anti-churn"] += 1
                anti_churn_suppressed_count += 1
            elif raw_candidate and not final_entry:
                false_counts["entry resolver"] += 1
                resolver_suppressed_count += 1

        summary[family] = {
            "denominator_completed_bars": len(rows),
            "raw_candidate_count": raw_candidate_count,
            "final_entry_count": final_entry_count,
            "anti_churn_suppressed_count": anti_churn_suppressed_count,
            "resolver_suppressed_count": resolver_suppressed_count,
            "predicate_failure_counts": [
                {
                    "predicate": predicate,
                    "count": count,
                    "pct_of_family_opportunities": round(count * 100.0 / denominator, 3),
                }
                for predicate, count in sorted(false_counts.items(), key=lambda item: (-item[1], item[0]))
            ],
            "primary_blockers": [
                {
                    "predicate": predicate,
                    "count": count,
                    "pct_of_family_opportunities": round(count * 100.0 / denominator, 3),
                }
                for predicate, count in sorted(primary_blockers.items(), key=lambda item: (-item[1], item[0]))
            ],
        }
    return summary


def _primary_blocker_for_family(*, row: dict[str, Any], family: str) -> str | None:
    payload = row["payload"]
    spec = FAMILY_SPECS[family]
    for label, field in spec["predicate_fields"]:
        value = row["session_allowed"] if field == "session_allowed" else bool(payload.get(field))
        if not value:
            return label
    candidate = bool(payload.get(spec["candidate_field"]))
    raw_candidate = bool(payload.get(spec["raw_field"]))
    final_entry = bool(payload.get(spec["final_field"])) and str(payload.get(spec["final_source_field"]) or "").strip() == spec["final_source_value"]
    if candidate and not raw_candidate:
        return "cooldown"
    if raw_candidate and not final_entry and bool(payload.get(spec["recent_field"])):
        return "anti-churn"
    if raw_candidate and not final_entry:
        return "entry resolver"
    return None


def _compute_anti_churn_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    recent_long_true = 0
    recent_short_true = 0
    bars_since_long_setup: int | None = None
    bars_since_short_setup: int | None = None
    recent_rows: list[dict[str, Any]] = []
    family_suppression = {
        "bullSnapLong": 0,
        "asiaVWAPLong": 0,
        "bearSnapShort": 0,
    }
    family_raw = {
        "bullSnapLong": 0,
        "asiaVWAPLong": 0,
        "bearSnapShort": 0,
    }
    for row in rows:
        payload = row["payload"]
        recent_long = bool(payload.get("recent_long_setup"))
        recent_short = bool(payload.get("recent_short_setup"))
        if recent_long:
            recent_long_true += 1
        if recent_short:
            recent_short_true += 1

        if bool(payload.get("long_entry_raw")):
            bars_since_long_setup = 0
        elif bars_since_long_setup is not None:
            bars_since_long_setup += 1

        if bool(payload.get("short_entry_raw")):
            bars_since_short_setup = 0
        elif bars_since_short_setup is not None:
            bars_since_short_setup += 1

        if bool(payload.get("first_bull_snap_turn")):
            family_raw["bullSnapLong"] += 1
            if recent_long and not (bool(payload.get("long_entry")) and str(payload.get("long_entry_source") or "") == "firstBullSnapTurn"):
                family_suppression["bullSnapLong"] += 1
        if bool(payload.get("asia_vwap_long_signal")):
            family_raw["asiaVWAPLong"] += 1
            if recent_long and not (bool(payload.get("long_entry")) and str(payload.get("long_entry_source") or "") == "asiaVWAPLongSignal"):
                family_suppression["asiaVWAPLong"] += 1
        if bool(payload.get("first_bear_snap_turn")):
            family_raw["bearSnapShort"] += 1
            if recent_short and not (bool(payload.get("short_entry")) and str(payload.get("short_entry_source") or "") == "firstBearSnapTurn"):
                family_suppression["bearSnapShort"] += 1

        recent_rows.append(
            {
                "bar_id": row["bar_id"],
                "recentLongSetup": recent_long,
                "recentShortSetup": recent_short,
                "barsSinceLongSetup": bars_since_long_setup,
                "barsSinceShortSetup": bars_since_short_setup,
            }
        )

    return {
        "recentLongSetup_true_bars": recent_long_true,
        "recentShortSetup_true_bars": recent_short_true,
        "barsSinceLongSetup_last": recent_rows[-1]["barsSinceLongSetup"] if recent_rows else None,
        "barsSinceShortSetup_last": recent_rows[-1]["barsSinceShortSetup"] if recent_rows else None,
        "suppression_by_family": {
            family: {
                "suppressed_count": family_suppression[family],
                "raw_candidate_count": family_raw[family],
                "suppression_rate_pct": round(family_suppression[family] * 100.0 / family_raw[family], 3) if family_raw[family] else 0.0,
            }
            for family in FAMILY_ORDER
        },
        "recent_rows": recent_rows[-20:],
    }


def _aggregate_regime_summary(day_rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in day_rows:
        grouped[str(row.get("regime") or "mixed_range_day")].append(row)
    regime_payload = {}
    for regime, rows in grouped.items():
        completed_bars = sum(int(row.get("completed_bars") or 0) for row in rows)
        raw_short = sum(int(row.get("funnel_counts", {}).get("shortEntryRaw", 0)) for row in rows)
        final_short = sum(int(row.get("funnel_counts", {}).get("shortEntry", 0)) for row in rows)
        raw_long = sum(int(row.get("funnel_counts", {}).get("longEntryRaw", 0)) for row in rows)
        final_long = sum(int(row.get("funnel_counts", {}).get("longEntry", 0)) for row in rows)
        family_top = _merge_top_failed_predicates(rows)
        regime_payload[regime] = {
            "session_day_count": len(rows),
            "completed_bars": completed_bars,
            "short_raw_candidates_per_100_bars": round(raw_short * 100.0 / completed_bars, 3) if completed_bars else 0.0,
            "short_final_entries_per_100_bars": round(final_short * 100.0 / completed_bars, 3) if completed_bars else 0.0,
            "long_raw_candidates_per_100_bars": round(raw_long * 100.0 / completed_bars, 3) if completed_bars else 0.0,
            "long_final_entries_per_100_bars": round(final_long * 100.0 / completed_bars, 3) if completed_bars else 0.0,
            "top_failed_predicates_by_family": family_top,
        }
    return regime_payload


def _merge_top_failed_predicates(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    merged = {family: Counter[str]() for family in FAMILY_ORDER}
    for row in rows:
        family_rows = row.get("top_failed_predicates_by_family") or {}
        for family in FAMILY_ORDER:
            for item in list(family_rows.get(family) or []):
                predicate = str(item.get("predicate") or "")
                count = int(item.get("count") or 0)
                if predicate:
                    merged[family][predicate] += count
    return {
        family: [
            {"predicate": predicate, "count": count}
            for predicate, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:5]
        ]
        for family, counter in merged.items()
    }


def _build_before_after_comparison(datasets: list[dict[str, Any]]) -> dict[str, Any]:
    before = next((dataset for dataset in datasets if dataset.get("comparison_role") == "before"), None)
    after = next((dataset for dataset in datasets if dataset.get("comparison_role") == "after"), None)
    if not before or not after:
        return {
            "available": False,
            "summary_line": "Before/after Bear Snap location comparison is unavailable until both strict and current replay datasets are present.",
        }
    before_funnel = dict(before.get("funnel") or {})
    after_funnel = dict(after.get("funnel") or {})
    before_failures = dict((before.get("family_failures") or {}).get("bearSnapShort") or {})
    after_failures = dict((after.get("family_failures") or {}).get("bearSnapShort") or {})
    before_top = list(before_failures.get("primary_blockers") or [])
    after_top = list(after_failures.get("primary_blockers") or [])
    before_location = _predicate_count(before_failures, "location")
    after_location = _predicate_count(after_failures, "location")
    short_raw_before = int(before_funnel.get("counts", {}).get("shortEntryRaw", 0))
    short_raw_after = int(after_funnel.get("counts", {}).get("shortEntryRaw", 0))
    short_final_before = int(before_funnel.get("counts", {}).get("shortEntry", 0))
    short_final_after = int(after_funnel.get("counts", {}).get("shortEntry", 0))
    return {
        "available": True,
        "before_dataset_id": before.get("dataset_id"),
        "after_dataset_id": after.get("dataset_id"),
        "before_label": before.get("label"),
        "after_label": after.get("label"),
        "counts": {
            "bear_snap_turn_candidate": {
                "before": int(before_funnel.get("counts", {}).get("bear_snap_turn_candidate", 0)),
                "after": int(after_funnel.get("counts", {}).get("bear_snap_turn_candidate", 0)),
            },
            "firstBearSnapTurn": {
                "before": int(before_funnel.get("counts", {}).get("firstBearSnapTurn", 0)),
                "after": int(after_funnel.get("counts", {}).get("firstBearSnapTurn", 0)),
            },
            "shortEntryRaw": {
                "before": short_raw_before,
                "after": short_raw_after,
            },
            "shortEntry": {
                "before": short_final_before,
                "after": short_final_after,
            },
        },
        "delta": {
            "shortEntryRaw": short_raw_after - short_raw_before,
            "shortEntry": short_final_after - short_final_before,
            "location_primary_block_count": after_location - before_location,
        },
        "top_failed_predicates_before": before_top[:5],
        "top_failed_predicates_after": after_top[:5],
        "location_primary_block_before": before_location,
        "location_primary_block_after": after_location,
        "location_no_longer_dominant": (
            bool(before_top)
            and str(before_top[0].get("predicate") or "") == "location"
            and (not after_top or str(after_top[0].get("predicate") or "") != "location")
        ),
        "materially_improved_short_opportunity_rate": short_raw_after > short_raw_before or short_final_after > short_final_before,
        "summary_line": (
            f"short raw {short_raw_before} -> {short_raw_after}, "
            f"short final {short_final_before} -> {short_final_after}, "
            f"location primary blocks {before_location} -> {after_location}"
        ),
    }


def _build_bear_snap_up_stretch_ladder(*, repo_root: Path, settings: StrategySettings) -> dict[str, Any]:
    comparison_dir = repo_root / "outputs" / "signal_selectivity_analysis" / "replays"
    rows: list[dict[str, Any]] = []
    for value, run_stamp in BEAR_SNAP_UP_STRETCH_LADDER:
        db_path = comparison_dir / f"historical_playback_mgc_{run_stamp}.sqlite3"
        if not db_path.exists():
            continue
        summary = summarize_signal_selectivity_dataset(
            SignalSelectivityDataset(
                dataset_id=run_stamp,
                label=f"Bear Snap Up Stretch {value}",
                dataset_kind="replay",
                database_path=db_path,
                comparison_role="candidate",
                note="Bear Snap upside-stretch ladder replay on the same persisted playback slice.",
            ),
            settings=settings,
        )
        bear_failures = dict((summary.get("family_failures") or {}).get("bearSnapShort") or {})
        top_failed = list(bear_failures.get("primary_blockers") or [])[:5]
        counts = dict(summary.get("funnel", {}).get("counts") or {})
        row = {
            "value": value,
            "run_stamp": run_stamp,
            "database_path": str(db_path),
            "counts": {
                "bear_snap_turn_candidate": int(counts.get("bear_snap_turn_candidate", 0)),
                "firstBearSnapTurn": int(counts.get("firstBearSnapTurn", 0)),
                "shortEntryRaw": int(counts.get("shortEntryRaw", 0)),
                "shortEntry": int(counts.get("shortEntry", 0)),
            },
            "short_entries_per_100_bars": float(summary.get("funnel", {}).get("entries_per_100_bars", {}).get("short", 0.0)),
            "top_failed_predicates": top_failed,
            "top_primary_predicate": str(top_failed[0].get("predicate") or "") if top_failed else None,
            "range_becomes_top_blocker": bool(top_failed) and str(top_failed[0].get("predicate") or "") == "range",
            "family_failure_summary": bear_failures,
        }
        rows.append(row)

    if not rows:
        return {
            "available": False,
            "summary_line": "Bear Snap upside-stretch ladder comparison is unavailable until the persisted replay ladder datasets are present.",
        }

    baseline = next((row for row in rows if row["value"] == "1.00"), rows[0])
    baseline_top = str(baseline.get("top_primary_predicate") or "")
    baseline_raw = int(baseline.get("counts", {}).get("shortEntryRaw", 0))
    baseline_final = int(baseline.get("counts", {}).get("shortEntry", 0))
    baseline_rate = float(baseline.get("short_entries_per_100_bars", 0.0))

    for row in rows:
        row["delta_vs_1_00"] = {
            "shortEntryRaw": int(row["counts"]["shortEntryRaw"]) - baseline_raw,
            "shortEntry": int(row["counts"]["shortEntry"]) - baseline_final,
            "short_entries_per_100_bars": round(float(row["short_entries_per_100_bars"]) - baseline_rate, 3),
        }
        row["material_increase_vs_1_00"] = (
            int(row["counts"]["shortEntryRaw"]) > baseline_raw
            or int(row["counts"]["shortEntry"]) > baseline_final
        )

    recommended = next(
        (
            row
            for row in rows
            if row["value"] != "1.00"
            and row["material_increase_vs_1_00"]
            and str(row.get("top_primary_predicate") or "") != baseline_top
        ),
        None,
    )
    if recommended is None:
        recommended = next(
            (row for row in rows if row["value"] != "1.00" and row["material_increase_vs_1_00"]),
            None,
        )

    if recommended is None:
        return {
            "available": True,
            "baseline_value": "1.00",
            "candidate_rows": rows,
            "recommended_value": None,
            "summary_line": "No upside-stretch candidate materially improved short opportunity rate versus 1.00 on the persisted playback slice.",
        }

    recommended_value = str(recommended["value"])
    summary_line = (
        f"1.00 -> {recommended_value}: "
        f"short raw {baseline_raw} -> {recommended['counts']['shortEntryRaw']}, "
        f"short final {baseline_final} -> {recommended['counts']['shortEntry']}, "
        f"short/100 {baseline_rate:.3f} -> {float(recommended['short_entries_per_100_bars']):.3f}, "
        f"top blocker {baseline_top or 'none'} -> {recommended.get('top_primary_predicate') or 'none'}"
    )
    return {
        "available": True,
        "baseline_value": "1.00",
        "candidate_values_tested": [row["value"] for row in rows],
        "candidate_rows": rows,
        "recommended_value": recommended_value,
        "recommended_summary": summary_line,
        "selection_rule": (
            "Choose the smallest reduction that increases short opportunities versus 1.00 and changes the top Bear Snap short blocker away from upside stretch."
        ),
        "range_becomes_next_dominant_blocker": bool(recommended.get("range_becomes_top_blocker")),
        "summary_line": summary_line,
    }


def _build_bear_snap_range_ladder(*, repo_root: Path, settings: StrategySettings) -> dict[str, Any]:
    comparison_dir = repo_root / "outputs" / "signal_selectivity_analysis" / "replays"
    rows: list[dict[str, Any]] = []
    for value, run_stamp in BEAR_SNAP_RANGE_LADDER:
        db_path = comparison_dir / f"historical_playback_mgc_{run_stamp}.sqlite3"
        if not db_path.exists():
            continue
        summary = summarize_signal_selectivity_dataset(
            SignalSelectivityDataset(
                dataset_id=run_stamp,
                label=f"Bear Snap Range {value}",
                dataset_kind="replay",
                database_path=db_path,
                comparison_role="candidate",
                note="Bear Snap range ladder replay on the same persisted playback slice.",
            ),
            settings=settings,
        )
        bear_failures = dict((summary.get("family_failures") or {}).get("bearSnapShort") or {})
        top_failed = list(bear_failures.get("primary_blockers") or [])[:5]
        counts = dict(summary.get("funnel", {}).get("counts") or {})
        row = {
            "value": value,
            "run_stamp": run_stamp,
            "database_path": str(db_path),
            "counts": {
                "bear_snap_turn_candidate": int(counts.get("bear_snap_turn_candidate", 0)),
                "firstBearSnapTurn": int(counts.get("firstBearSnapTurn", 0)),
                "shortEntryRaw": int(counts.get("shortEntryRaw", 0)),
                "shortEntry": int(counts.get("shortEntry", 0)),
            },
            "short_entries_per_100_bars": float(summary.get("funnel", {}).get("entries_per_100_bars", {}).get("short", 0.0)),
            "top_failed_predicates": top_failed,
            "top_primary_predicate": str(top_failed[0].get("predicate") or "") if top_failed else None,
            "next_blocker_after_range": str(top_failed[0].get("predicate") or "") if top_failed else None,
            "family_failure_summary": bear_failures,
        }
        rows.append(row)

    if not rows:
        return {
            "available": False,
            "summary_line": "Bear Snap range ladder comparison is unavailable until the persisted replay ladder datasets are present.",
        }

    baseline = next((row for row in rows if row["value"] == "0.90"), rows[0])
    baseline_top = str(baseline.get("top_primary_predicate") or "")
    baseline_raw = int(baseline.get("counts", {}).get("shortEntryRaw", 0))
    baseline_final = int(baseline.get("counts", {}).get("shortEntry", 0))
    baseline_rate = float(baseline.get("short_entries_per_100_bars", 0.0))

    for row in rows:
        row["delta_vs_0_90"] = {
            "shortEntryRaw": int(row["counts"]["shortEntryRaw"]) - baseline_raw,
            "shortEntry": int(row["counts"]["shortEntry"]) - baseline_final,
            "short_entries_per_100_bars": round(float(row["short_entries_per_100_bars"]) - baseline_rate, 3),
        }
        row["material_increase_vs_0_90"] = (
            int(row["counts"]["shortEntryRaw"]) > baseline_raw
            or int(row["counts"]["shortEntry"]) > baseline_final
        )

    recommended = next(
        (
            row
            for row in rows
            if row["value"] != "0.90"
            and row["material_increase_vs_0_90"]
            and str(row.get("top_primary_predicate") or "") != baseline_top
        ),
        None,
    )
    if recommended is None:
        recommended = next(
            (row for row in rows if row["value"] != "0.90" and row["material_increase_vs_0_90"]),
            None,
        )

    if recommended is None:
        return {
            "available": True,
            "baseline_value": "0.90",
            "candidate_rows": rows,
            "recommended_value": None,
            "summary_line": "No Bear Snap range candidate materially improved short opportunity rate versus 0.90 on the persisted playback slice.",
        }

    recommended_value = str(recommended["value"])
    summary_line = (
        f"0.90 -> {recommended_value}: "
        f"short raw {baseline_raw} -> {recommended['counts']['shortEntryRaw']}, "
        f"short final {baseline_final} -> {recommended['counts']['shortEntry']}, "
        f"short/100 {baseline_rate:.3f} -> {float(recommended['short_entries_per_100_bars']):.3f}, "
        f"top blocker {baseline_top or 'none'} -> {recommended.get('top_primary_predicate') or 'none'}"
    )
    return {
        "available": True,
        "baseline_value": "0.90",
        "candidate_values_tested": [row["value"] for row in rows],
        "candidate_rows": rows,
        "recommended_value": recommended_value,
        "recommended_summary": summary_line,
        "selection_rule": (
            "Choose the smallest reduction that increases short opportunities versus 0.90 and changes the top Bear Snap short blocker away from range."
        ),
        "next_dominant_blocker_after_recommended": recommended.get("top_primary_predicate"),
        "summary_line": summary_line,
    }


def _predicate_count(family_failure_summary: dict[str, Any], predicate: str) -> int:
    for item in list(family_failure_summary.get("primary_blockers") or []):
        if str(item.get("predicate") or "") == predicate:
            return int(item.get("count") or 0)
    return 0


def _derive_key_findings(
    datasets: list[dict[str, Any]],
    before_after: dict[str, Any],
    bear_snap_up_stretch_ladder: dict[str, Any],
    bear_snap_range_ladder: dict[str, Any],
) -> list[str]:
    findings: list[str] = []
    live = next((dataset for dataset in datasets if dataset.get("dataset_kind") == "live"), None)
    if live:
        raw_vs_final = dict(live.get("raw_candidates_vs_final_entries") or {})
        findings.append(
            "Live pilot: "
            f"long raw {raw_vs_final.get('long', {}).get('raw_candidates', 0)} -> final {raw_vs_final.get('long', {}).get('final_entries', 0)}, "
            f"short raw {raw_vs_final.get('short', {}).get('raw_candidates', 0)} -> final {raw_vs_final.get('short', {}).get('final_entries', 0)}."
        )
    if before_after.get("available"):
        findings.append(
            "Bear Snap location comparison: "
            + str(before_after.get("summary_line") or "")
        )
    if bear_snap_up_stretch_ladder.get("available"):
        findings.append(
            "Bear Snap upside-stretch ladder: "
            + str(bear_snap_up_stretch_ladder.get("summary_line") or "")
        )
    if bear_snap_range_ladder.get("available"):
        findings.append(
            "Bear Snap range ladder: "
            + str(bear_snap_range_ladder.get("summary_line") or "")
        )
    for family in FAMILY_ORDER:
        predicate = _global_top_primary_predicate(datasets, family)
        if predicate:
            findings.append(f"{family}: most common primary blocker = {predicate}.")
    return findings


def _global_top_primary_predicate(datasets: list[dict[str, Any]], family: str) -> str | None:
    counter = Counter[str]()
    for dataset in datasets:
        family_summary = dict((dataset.get("family_failures") or {}).get(family) or {})
        for item in list(family_summary.get("primary_blockers") or []):
            predicate = str(item.get("predicate") or "")
            count = int(item.get("count") or 0)
            if predicate:
                counter[predicate] += count
    if not counter:
        return None
    return sorted(counter.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _summary_line(
    live_dataset: dict[str, Any],
    before_after: dict[str, Any],
    bear_snap_up_stretch_ladder: dict[str, Any],
    bear_snap_range_ladder: dict[str, Any],
) -> str:
    live_reason = _live_why_no_trade_so_far(live_dataset)
    if bear_snap_range_ladder.get("available"):
        return f"{live_reason} | {bear_snap_range_ladder.get('summary_line')}"
    if bear_snap_up_stretch_ladder.get("available"):
        return f"{live_reason} | {bear_snap_up_stretch_ladder.get('summary_line')}"
    if before_after.get("available"):
        return f"{live_reason} | {before_after.get('summary_line')}"
    return live_reason


def _live_why_no_trade_so_far(live_dataset: dict[str, Any]) -> str:
    if not live_dataset:
        return "Live pilot dataset unavailable."
    return str(
        live_dataset.get("why_no_trade_so_far")
        or "Live pilot selectivity summary is not available yet."
    )


def _dataset_why_no_trade_so_far(
    funnel: dict[str, Any],
    top_failed: dict[str, list[dict[str, Any]]],
    anti_churn: dict[str, Any],
) -> str:
    long_raw = int(funnel["raw_candidates_vs_final_entries"]["long"]["raw_candidates"])
    long_final = int(funnel["raw_candidates_vs_final_entries"]["long"]["final_entries"])
    short_raw = int(funnel["raw_candidates_vs_final_entries"]["short"]["raw_candidates"])
    short_final = int(funnel["raw_candidates_vs_final_entries"]["short"]["final_entries"])
    blockers = []
    for family in FAMILY_ORDER:
        family_top = list(top_failed.get(family) or [])
        if family_top:
            blockers.append(f"{family} -> {family_top[0]['predicate']}")
    return (
        f"raw long {long_raw} -> final {long_final}, raw short {short_raw} -> final {short_final}; "
        f"top blockers: {', '.join(blockers) if blockers else 'none'}; "
        f"anti-churn short suppressions={anti_churn.get('suppression_by_family', {}).get('bearSnapShort', {}).get('suppressed_count', 0)}"
    )


def _build_signal_selectivity_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Signal Selectivity Analysis",
        "",
        f"Generated at: {summary.get('generated_at')}",
        "",
        f"Summary: {summary.get('summary_line')}",
        "",
        "## Key Findings",
    ]
    for item in list(summary.get("key_findings") or []):
        lines.append(f"- {item}")
    lines.extend(["", "## Datasets"])
    for dataset in list(summary.get("datasets") or []):
        lines.append(
            f"- {dataset.get('label')}: bars={dataset.get('processed_bars')} "
            f"longRaw={dataset.get('funnel', {}).get('counts', {}).get('longEntryRaw', 0)} "
            f"longEntry={dataset.get('funnel', {}).get('counts', {}).get('longEntry', 0)} "
            f"shortRaw={dataset.get('funnel', {}).get('counts', {}).get('shortEntryRaw', 0)} "
            f"shortEntry={dataset.get('funnel', {}).get('counts', {}).get('shortEntry', 0)}"
        )
        for family in FAMILY_ORDER:
            top = list((dataset.get("top_failed_predicates_by_family") or {}).get(family) or [])
            if top:
                lines.append(f"  - {family}: top blocker {top[0].get('predicate')} ({top[0].get('count')})")
    range_ladder = dict(summary.get("bear_snap_range_ladder") or {})
    lines.extend(["", "## Bear Snap Range Ladder"])
    lines.append(f"- {range_ladder.get('summary_line') or 'Unavailable'}")
    if range_ladder.get("available"):
        lines.append(f"- Recommended value: {range_ladder.get('recommended_value') or 'none'}")
        lines.append(f"- Next dominant blocker: {range_ladder.get('next_dominant_blocker_after_recommended') or 'none'}")
    stretch_ladder = dict(summary.get("bear_snap_up_stretch_ladder") or {})
    lines.extend(["", "## Bear Snap Upside-Stretch Ladder"])
    lines.append(f"- {stretch_ladder.get('summary_line') or 'Unavailable'}")
    if stretch_ladder.get("available"):
        lines.append(f"- Recommended value: {stretch_ladder.get('recommended_value') or 'none'}")
        lines.append(f"- Range becomes next dominant blocker: {stretch_ladder.get('range_becomes_next_dominant_blocker')}")
    before_after = dict(summary.get("before_after_bear_snap_location") or {})
    lines.extend(["", "## Bear Snap Location Before/After"])
    lines.append(f"- {before_after.get('summary_line') or 'Unavailable'}")
    return "\n".join(lines) + "\n"


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _coarse_session_segment(row: sqlite3.Row, bar_end_ts: datetime | None) -> str:
    if row["session_asia"]:
        return "ASIA"
    if row["session_london"]:
        return "LONDON"
    if row["session_us"]:
        return "US"
    if bar_end_ts is None:
        return "UNKNOWN"
    phase = label_session_phase(bar_end_ts)
    if phase.startswith("ASIA"):
        return "ASIA"
    if phase.startswith("LONDON"):
        return "LONDON"
    if phase.startswith("US"):
        return "US"
    return "UNKNOWN"


def _classify_regime(bars: list[dict[str, Any]]) -> str:
    valid = [bar for bar in bars if bar.get("open") is not None and bar.get("close") is not None and bar.get("high") is not None and bar.get("low") is not None]
    if not valid:
        return "mixed_range_day"
    first_open = float(valid[0]["open"])
    last_close = float(valid[-1]["close"])
    day_high = max(float(bar["high"]) for bar in valid)
    day_low = min(float(bar["low"]) for bar in valid)
    day_range = max(day_high - day_low, 0.0001)
    net_move = last_close - first_open
    if abs(net_move) <= day_range * 0.25:
        return "mixed_range_day"
    if net_move < 0:
        return "red_day_down_tape"
    return "green_day_up_tape"
