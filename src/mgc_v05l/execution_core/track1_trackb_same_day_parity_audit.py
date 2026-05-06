"""Read-only Track 1 vs Track B same-day strategy parity audit.

This diagnostic compares the migrated Track B strategy surfaces against the
best available Track 1/source references and retained same-day Track B runtime
evidence. It never invokes broker, paper-proof, or lifecycle code.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import require_aware_datetime, to_jsonable
from .track_b_strategy_registry import TrackBStrategyRegistryEntry, get_track_b_strategy_registry


DEFAULT_OUTPUT_ROOT = Path("outputs/track_b_execution_core/diagnostics")
DEFAULT_PARITY_AUDIT_JSON = DEFAULT_OUTPUT_ROOT / "latest_track1_trackb_same_day_parity_audit.json"
DEFAULT_PARITY_AUDIT_MD = DEFAULT_OUTPUT_ROOT / "latest_track1_trackb_same_day_parity_audit.md"
DEFAULT_FORENSIC_PATH = DEFAULT_OUTPUT_ROOT / "latest_track_b_missed_move_forensic_replay.json"
DEFAULT_POSTMORTEM_PATH = DEFAULT_OUTPUT_ROOT / "latest_track_b_same_day_postmortem.json"
MAX_JSON_BYTES = 16 * 1024 * 1024

AUDITED_STRATEGY_IDS: tuple[str, ...] = (
    "asian_drift_v1",
    "ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
    "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
    "FIRST_BULL_SNAP_TURN_V1",
    "FIRST_BEAR_SNAP_TURN_V1",
    "LONDON_LATE_PAUSE_RESUME_SHORT_V1",
    "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
    "US_DERIVATIVE_BEAR_TURN_V1",
    "US_LATE_PAUSE_RESUME_LONG_V1",
    "MNQ_US_DERIVATIVE_BEAR_TURN_V1",
    "MNQ_FIRST_BEAR_SNAP_TURN_V1",
    "MNQ_FIRST_BULL_SNAP_TURN_V1",
)

SAFETY_OR_OPERATIONAL_FIELDS = frozenset(
    {
        "submit_allowed",
        "submit_attempted",
        "paper_proof_invoked",
        "broker_state_mutated",
        "live_money_readiness",
        "paper_eligible",
        "live_money_eligible",
        "expected_account_id",
        "account_id",
        "runtime_decision_source",
        "feature_context_ready",
        "live_execution_approved",
        "paper_evaluation_allowed",
        "latest_decision_bar_source",
        "broker_state_classification",
    }
)


TRACK1_SOURCE_REFERENCES: dict[str, dict[str, Any]] = {
    "asian_drift_v1": {
        "track1_strategy_name": "Asian Drift state snapshot",
        "source_files": [
            "src/mgc_v05l/execution_core/track_b_asian_drift_state.py",
            "src/mgc_v05l/research",
        ],
        "predicate_source_status": "track_b_state_snapshot_migration_reference",
        "session_convention": "Asian drift entry window from precomputed state snapshot",
        "bar_alignment": "completed 5m state snapshot",
        "side": "LONG_OR_SHORT_FROM_STATE",
        "predicates": [
            "state_is_entry_eligible",
            "direction_is_explicit",
            "entry_ready",
            "entry_window_open",
            "in_scope",
            "session_not_timeout",
            "feature_version_present",
            "calibration_profile_present",
        ],
    },
    "ASIA_EARLY_PAUSE_RESUME_SHORT_V1": {
        "track1_strategy_name": "asiaEarlyPauseResumeShortTurn",
        "source_files": ["src/mgc_v05l/signals/bear_snap.py", "config/replay.asia_early_pause_resume_short_pattern_v1.yaml"],
        "predicate_source_status": "explicit_source_mirrored_by_track_b",
        "session_convention": "ASIA_EARLY completed 5m",
        "bar_alignment": "current completed 5m signal bar",
        "side": "SHORT",
        "predicates": [
            "rule_is_watch_only",
            "allow_asia",
            "session_asia",
            "derivative_phase_asia_early",
            "close_below_open",
            "close_below_previous_close",
            "derivative_bear_close_weak",
            "derivative_bear_range_ok",
            "derivative_bear_body_ok",
            "derivative_bear_stretch_ok",
            "normalized_curvature_at_or_below_threshold",
            "setup_bar_curvature_is_flat",
            "signal_range_expansion_below_threshold",
            "one_bar_rebound_before_signal",
            "signal_breaks_prior_1_low",
            "close_below_fast_ema",
            "derivative_bear_cooldown_ok",
            "no_competing_bear_short_candidate",
        ],
    },
    "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1": {
        "track1_strategy_name": "asiaEarlyNormalBreakoutRetestHoldTurn",
        "source_files": ["src/mgc_v05l/signals/bull_snap.py", "config/replay.asia_early_breakout_retest_hold_pattern_v1_normal.yaml"],
        "predicate_source_status": "explicit_source_mirrored_by_track_b",
        "session_convention": "ASIA_EARLY or GC/MGC London-open completed 5m",
        "bar_alignment": "current completed 5m retest/hold signal bar",
        "side": "LONG",
        "predicates": [
            "allow_asia",
            "asia_early_or_gc_mgc_london_open",
            "no_first_bull_snap_turn",
            "prior_bars_since_long_setup_gt_anti_churn",
            "breakout_bar_slope_is_flat",
            "breakout_bar_expansion_is_normal",
            "breakout_breaks_prior_1_high",
            "signal_retests_and_holds_breakout_level",
        ],
    },
    "FIRST_BULL_SNAP_TURN_V1": {
        "track1_strategy_name": "firstBullSnapTurn",
        "source_files": ["src/mgc_v05l/signals/bull_snap.py"],
        "predicate_source_status": "explicit_source_mirrored_by_track_b",
        "session_convention": "session_allowed completed 5m",
        "bar_alignment": "current completed 5m first bull snap-turn signal bar",
        "side": "LONG",
        "predicates": [
            "session_allowed",
            "prior_bars_since_bull_snap_gt_cooldown",
            "bull_snap_downside_stretch_ok",
            "bull_snap_range_ok",
            "bull_snap_body_ok",
            "bull_snap_close_strong",
            "bull_snap_velocity_ok",
            "bull_snap_reversal_bar",
            "bull_snap_location_ok",
            "bull_snap_raw",
            "bull_snap_turn_candidate",
            "first_bull_snap_turn",
        ],
    },
    "FIRST_BEAR_SNAP_TURN_V1": {
        "track1_strategy_name": "firstBearSnapTurn",
        "source_files": ["src/mgc_v05l/signals/bear_snap.py"],
        "predicate_source_status": "explicit_source_mirrored_by_track_b",
        "session_convention": "session_allowed completed 5m",
        "bar_alignment": "current completed 5m first bear snap-turn signal bar",
        "side": "SHORT",
        "predicates": [
            "session_allowed",
            "prior_bars_since_bear_snap_gt_cooldown",
            "bear_snap_up_stretch_ok",
            "bear_snap_range_ok",
            "bear_snap_body_ok",
            "bear_snap_close_weak",
            "bear_snap_velocity_ok",
            "bear_snap_reversal_bar",
            "bear_snap_location_ok",
            "bear_snap_raw",
            "bear_snap_turn_candidate",
            "first_bear_snap_turn",
        ],
    },
    "LONDON_LATE_PAUSE_RESUME_SHORT_V1": {
        "track1_strategy_name": "londonLatePauseResumeShortTurn",
        "source_files": ["src/mgc_v05l/signals/bear_snap.py", "config/replay.london_late_pause_resume_short_family.yaml"],
        "predicate_source_status": "explicit_source_mirrored_by_track_b",
        "session_convention": "LONDON_LATE completed 5m",
        "bar_alignment": "current completed 5m signal bar",
        "side": "SHORT",
        "predicates": [
            "allow_london",
            "session_london",
            "derivative_phase_london_late",
            "no_first_bear_snap_turn",
            "close_below_open",
            "close_below_previous_close",
            "derivative_bear_close_weak",
            "derivative_bear_range_ok",
            "derivative_bear_body_ok",
            "derivative_bear_stretch_ok",
            "normalized_slope_in_range",
            "normalized_curvature_in_range",
            "signal_range_expansion_below_threshold",
            "slow_ema_ok",
            "one_bar_rebound_before_signal",
            "prior_3_any_positive_curvature",
            "signal_breaks_prior_1_low",
            "derivative_bear_cooldown_ok",
            "no_competing_bear_short_candidate",
        ],
    },
    "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1": {
        "track1_strategy_name": "asiaLateFlatPullbackPauseResumeLongTurn",
        "source_files": ["src/mgc_v05l/signals/bull_snap.py", "config/replay.asia_late_pause_resume_long_pattern_v1_flat_pullback.yaml"],
        "predicate_source_status": "explicit_source_mirrored_by_track_b",
        "session_convention": "ASIA_LATE completed 5m",
        "bar_alignment": "current completed 5m pullback/resume signal bar",
        "side": "LONG",
        "predicates": [
            "allow_asia",
            "session_asia",
            "derivative_phase_asia_late",
            "no_first_bull_snap_turn",
            "close_above_open",
            "close_above_previous_close",
            "bull_snap_close_strong",
            "one_bar_pullback_before_signal",
            "signal_breaks_prior_1_high",
            "pullback_range_expansion_below_threshold",
            "signal_range_expansion_above_threshold",
            "signal_range_expansion_below_threshold",
            "pullback_curvature_flat",
            "prior_bars_since_long_setup_gt_anti_churn",
        ],
    },
    "US_DERIVATIVE_BEAR_TURN_V1": {
        "track1_strategy_name": "usDerivativeBearTurn",
        "source_files": ["src/mgc_v05l/signals/bear_snap.py"],
        "predicate_source_status": "explicit_source_mirrored_by_track_b",
        "session_convention": "US derivative-bear window completed 5m",
        "bar_alignment": "current completed 5m signal bar",
        "side": "SHORT",
        "predicates": [
            "allow_us",
            "session_us",
            "derivative_bear_window_ok",
            "derivative_bear_phase_ok",
            "normalized_slope_in_range",
            "normalized_curvature_below_threshold",
            "close_below_open",
            "close_below_previous_close",
            "derivative_bear_close_weak",
            "derivative_bear_range_ok",
            "derivative_bear_body_ok",
            "derivative_bear_stretch_ok",
            "derivative_bear_fast_ema_ok",
            "derivative_bear_vwap_ok",
            "derivative_bear_vwap_extension_ok",
            "derivative_bear_open_late_extension_floor_ok",
            "derivative_bear_open_late_body_ok",
            "derivative_bear_open_late_close_ok",
            "derivative_bear_open_late_fast_ema_extension_ok",
            "derivative_bear_slow_ema_ok",
            "derivative_bear_structure_ok",
            "derivative_bear_cooldown_ok",
        ],
    },
    "US_LATE_PAUSE_RESUME_LONG_V1": {
        "track1_strategy_name": "usLatePauseResumeLongTurn",
        "source_files": ["src/mgc_v05l/signals/bull_snap.py", "config/replay.us_late_pause_resume_long_pattern_v1.yaml"],
        "predicate_source_status": "explicit_source_mirrored_by_track_b",
        "session_convention": "US_LATE completed 5m",
        "bar_alignment": "current completed 5m pullback/resume signal bar",
        "side": "LONG",
        "predicates": [
            "allow_us",
            "session_us_late",
            "derivative_phase_us_late",
            "no_first_bull_snap_turn",
            "close_above_open",
            "close_above_previous_close",
            "bull_snap_close_strong",
            "signal_range_expansion_below_threshold",
            "one_bar_pullback_before_signal",
            "signal_breaks_prior_1_high",
            "signal_ema_location_ok",
            "setup_bar_curvature_is_positive",
            "prior_bars_since_long_setup_gt_anti_churn",
            "not_1755_carryover",
        ],
    },
    "MNQ_US_DERIVATIVE_BEAR_TURN_V1": {
        "track1_strategy_name": "MNQ usDerivativeBearTurn validation packet",
        "source_files": ["src/mgc_v05l/signals/bear_snap.py"],
        "predicate_source_status": "explicit_source_mirrored_by_track_b",
        "session_convention": "US derivative-bear window completed 5m",
        "bar_alignment": "current completed 5m signal bar",
        "side": "SHORT",
        "predicates": [
            "allow_us",
            "session_us",
            "derivative_bear_window_ok",
            "derivative_bear_phase_ok",
            "normalized_slope_in_range",
            "normalized_curvature_below_threshold",
            "close_below_open",
            "close_below_previous_close",
            "derivative_bear_close_weak",
            "derivative_bear_range_ok",
            "derivative_bear_body_ok",
            "derivative_bear_stretch_ok",
            "derivative_bear_fast_ema_ok",
            "derivative_bear_vwap_ok",
            "derivative_bear_vwap_extension_ok",
            "derivative_bear_open_late_extension_floor_ok",
            "derivative_bear_open_late_body_ok",
            "derivative_bear_open_late_close_ok",
            "derivative_bear_open_late_fast_ema_extension_ok",
            "derivative_bear_slow_ema_ok",
            "derivative_bear_structure_ok",
            "derivative_bear_cooldown_ok",
        ],
    },
    "MNQ_FIRST_BEAR_SNAP_TURN_V1": {
        "track1_strategy_name": "MNQ firstBearSnapTurn",
        "source_files": ["src/mgc_v05l/signals/bear_snap.py"],
        "predicate_source_status": "explicit_source_mirrored_by_track_b",
        "session_convention": "session_allowed completed 5m",
        "bar_alignment": "current completed 5m first bear snap-turn signal bar",
        "side": "SHORT",
        "predicates": [
            "session_allowed",
            "prior_bars_since_bear_snap_gt_cooldown",
            "bear_snap_up_stretch_ok",
            "bear_snap_range_ok",
            "bear_snap_body_ok",
            "bear_snap_close_weak",
            "bear_snap_velocity_ok",
            "bear_snap_reversal_bar",
            "bear_snap_location_ok",
            "bear_snap_raw",
            "bear_snap_turn_candidate",
            "first_bear_snap_turn",
        ],
    },
    "MNQ_FIRST_BULL_SNAP_TURN_V1": {
        "track1_strategy_name": "MNQ firstBullSnapTurn",
        "source_files": ["src/mgc_v05l/signals/bull_snap.py"],
        "predicate_source_status": "explicit_source_mirrored_by_track_b",
        "session_convention": "session_allowed completed 5m",
        "bar_alignment": "current completed 5m first bull snap-turn signal bar",
        "side": "LONG",
        "predicates": [
            "session_allowed",
            "prior_bars_since_bull_snap_gt_cooldown",
            "bull_snap_downside_stretch_ok",
            "bull_snap_range_ok",
            "bull_snap_body_ok",
            "bull_snap_close_strong",
            "bull_snap_velocity_ok",
            "bull_snap_reversal_bar",
            "bull_snap_location_ok",
            "bull_snap_raw",
            "bull_snap_turn_candidate",
            "first_bull_snap_turn",
        ],
    },
}


@dataclass(frozen=True)
class Track1TrackBSameDayParityAuditResult:
    report_json: Path
    report_markdown: Path
    report: dict[str, Any]
    markdown: str


def build_track1_trackb_same_day_parity_audit(
    *,
    repo_root: Path = Path("."),
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    forensic_path: Path = DEFAULT_FORENSIC_PATH,
    postmortem_path: Path = DEFAULT_POSTMORTEM_PATH,
    scoped_strategy_ids: Sequence[str] = AUDITED_STRATEGY_IDS,
    track1_reference_paths: Sequence[Path] = (),
    track1_predicate_overrides: Mapping[str, Sequence[str]] | None = None,
    trackb_predicate_overrides: Mapping[str, Sequence[str]] | None = None,
    feature_mapping_overrides: Mapping[str, Mapping[str, str | None]] | None = None,
    session_convention_overrides: Mapping[str, Mapping[str, str | None]] | None = None,
    bar_alignment_overrides: Mapping[str, Mapping[str, str | None]] | None = None,
    migrated_strategy_ids_override: Sequence[str] | None = None,
    now: datetime | None = None,
    write: bool = True,
) -> Track1TrackBSameDayParityAuditResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    root = Path(repo_root)
    output_root = Path(output_root)
    registry_entries = _registry_by_strategy_id()
    migrated_ids = set(migrated_strategy_ids_override) if migrated_strategy_ids_override is not None else set(registry_entries)
    forensic = _load_json(_resolve_repo_path(root, forensic_path))
    postmortem = _load_json(_resolve_repo_path(root, postmortem_path))
    track1_refs = _track1_reference_availability(root, track1_reference_paths)
    observed = _observed_track_b_activity(forensic)
    lifecycle = _lifecycle_summary(postmortem)

    strategy_rows: list[dict[str, Any]] = []
    missing_migrated: list[dict[str, Any]] = []
    aggregate_statuses: Counter[str] = Counter()
    blockers_by_class: Counter[str] = Counter()
    for strategy_id in scoped_strategy_ids:
        entry = registry_entries.get(strategy_id)
        if strategy_id not in migrated_ids or entry is None:
            row = _missing_strategy_row(strategy_id)
            missing_migrated.append(row)
            strategy_rows.append(row)
            aggregate_statuses[row["parity_status"]] += 1
            blockers_by_class["missing strategy coverage"] += 1
            continue
        source = TRACK1_SOURCE_REFERENCES.get(strategy_id, {})
        track1_predicates = _normalized_predicates(
            (track1_predicate_overrides or {}).get(strategy_id) or source.get("predicates") or ()
        )
        trackb_predicates = _normalized_predicates(
            (trackb_predicate_overrides or {}).get(strategy_id) or _track_b_predicate_surface(entry, observed.get(strategy_id, {}))
        )
        extra = sorted(set(trackb_predicates) - set(track1_predicates) - SAFETY_OR_OPERATIONAL_FIELDS)
        missing = sorted(set(track1_predicates) - set(trackb_predicates))
        safety_excluded = sorted((set(trackb_predicates) - set(track1_predicates)) & SAFETY_OR_OPERATIONAL_FIELDS)
        feature_mapping = _feature_mapping_comparison(
            entry,
            override=(feature_mapping_overrides or {}).get(strategy_id),
        )
        session_comparison = _simple_comparison(
            track1=source.get("session_convention"),
            trackb=_track_b_session_convention(entry),
            override=(session_convention_overrides or {}).get(strategy_id),
            mismatch_status="TRACK_B_SESSION_MISMATCH",
        )
        bar_alignment = _simple_comparison(
            track1=source.get("bar_alignment"),
            trackb=_track_b_bar_alignment(entry),
            override=(bar_alignment_overrides or {}).get(strategy_id),
            mismatch_status="TRACK_B_BAR_ALIGNMENT_MISMATCH",
        )
        side_comparison = {
            "track1_side": source.get("side"),
            "track_b_side": _track_b_side(strategy_id),
            "status": "MATCH" if source.get("side") == _track_b_side(strategy_id) or source.get("side") == "LONG_OR_SHORT_FROM_STATE" else "REVIEW",
        }
        parity_status = _strategy_parity_status(
            extra=extra,
            missing=missing,
            feature_mapping=feature_mapping,
            session_comparison=session_comparison,
            bar_alignment=bar_alignment,
            track1_refs=track1_refs,
        )
        aggregate_statuses[parity_status] += 1
        _count_blockers(
            blockers_by_class,
            parity_status=parity_status,
            observed=observed.get(strategy_id, {}),
            lifecycle=lifecycle if lifecycle.get("strategy") == strategy_id else {},
        )
        row = {
            "track_b_strategy_id": strategy_id,
            "corresponding_track1_strategy": source.get("track1_strategy_name"),
            "corresponding_track1_strategy_source": source.get("source_files", []),
            "source_files_found": _existing_source_files(root, source.get("source_files", [])),
            "parity_status": parity_status,
            "track1_reference_status": track1_refs["status"],
            "track1_predicate_source_status": source.get("predicate_source_status"),
            "track1_predicate_list_if_available": track1_predicates,
            "track_b_predicate_list": trackb_predicates,
            "extra_track_b_predicates": extra,
            "missing_track_b_predicates": missing,
            "safety_or_operational_track_b_fields_excluded_from_strategy_predicate_diff": safety_excluded,
            "feature_field_mapping": feature_mapping,
            "session_convention_comparison": session_comparison,
            "bar_alignment_comparison": bar_alignment,
            "side_direction_comparison": side_comparison,
            "observed_track_b_evaluations_signals_no_signals": observed.get(
                strategy_id,
                _empty_observed_json(strategy_id),
            ),
            "known_track1_replay_or_paper_activity": _known_track1_activity(track1_refs),
            "recommended_next_action": _strategy_recommendation(
                parity_status=parity_status,
                track1_refs=track1_refs,
                extra=extra,
                missing=missing,
                feature_mapping=feature_mapping,
            ),
        }
        strategy_rows.append(row)

    classifications = _classifications(
        aggregate_statuses=aggregate_statuses,
        track1_refs=track1_refs,
        missing_migrated=missing_migrated,
        lifecycle=lifecycle,
    )
    report = {
        "schema_version": "track1_trackb_same_day_parity_audit_v1",
        "generated_at": actual_now.isoformat(),
        "source_tag": "READ_ONLY_DIAGNOSTIC_ONLY",
        "forensic_replay_path": str(_resolve_repo_path(root, forensic_path)),
        "postmortem_path": str(_resolve_repo_path(root, postmortem_path)),
        "track1_reference_artifacts": track1_refs,
        "audited_strategy_count": len(strategy_rows),
        "audited_strategies": list(scoped_strategy_ids),
        "strategy_parity": strategy_rows,
        "missing_migrated_strategies": missing_migrated,
        "blockers_by_class": dict(sorted(blockers_by_class.items())),
        "classifications": classifications,
        "summary": _summary(strategy_rows, classifications, lifecycle),
        "recommended_next_action": _recommended_next_action(classifications, track1_refs, strategy_rows),
        "safety": {
            "broker_commands_invoked": False,
            "paper_proof_cli_invoked": False,
            "submit_cancel_place_order_invoked": False,
            "broker_state_mutated": False,
            "strategy_thresholds_changed": False,
            "strategies_added_or_promoted": False,
            "trend_continuation_overlay_paper_promoted": False,
            "live_money_readiness": False,
        },
    }
    markdown = _render_markdown(report)
    report_json = output_root / DEFAULT_PARITY_AUDIT_JSON.name
    report_md = output_root / DEFAULT_PARITY_AUDIT_MD.name
    if write:
        output_root.mkdir(parents=True, exist_ok=True)
        report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        report_md.write_text(markdown, encoding="utf-8")
    return Track1TrackBSameDayParityAuditResult(
        report_json=report_json,
        report_markdown=report_md,
        report=report,
        markdown=markdown,
    )


def _registry_by_strategy_id() -> dict[str, TrackBStrategyRegistryEntry]:
    return {entry.strategy_id: entry for entry in get_track_b_strategy_registry()}


def _track_b_predicate_surface(entry: TrackBStrategyRegistryEntry, observed: Mapping[str, Any]) -> list[str]:
    observed_predicates = set(observed.get("observed_predicates") or [])
    source_predicates = set(TRACK1_SOURCE_REFERENCES.get(entry.strategy_id, {}).get("predicates") or ())
    if observed_predicates:
        return sorted(observed_predicates | source_predicates)
    return sorted(source_predicates)


def _observed_track_b_activity(forensic: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in _iter_strategy_rows(forensic):
        strategy_id = str(row.get("strategy_id") or "")
        if not strategy_id:
            continue
        item = rows.setdefault(strategy_id, _empty_observed(strategy_id))
        item["evaluated_count"] += 1
        result = str(row.get("result") or "UNKNOWN").upper()
        if result == "SIGNAL":
            item["signal_count"] += 1
        elif result == "SUPPRESSED":
            item["suppressed_count"] += 1
        elif result == "NOT_READY":
            item["not_ready_count"] += 1
        else:
            item["no_signal_count"] += 1
        for predicate in row.get("failed_predicates") or []:
            if isinstance(predicate, str):
                item["failed_predicates"][predicate] += 1
                item["observed_predicates"].add(predicate)
        for predicate in row.get("passed_predicates") or []:
            if isinstance(predicate, str):
                item["passed_predicates"][predicate] += 1
                item["observed_predicates"].add(predicate)
        if row.get("missing_fields"):
            for field in row.get("missing_fields") or []:
                if isinstance(field, str):
                    item["missing_fields"][field] += 1
        item["latest_decision_bar_timestamp"] = row.get("decision_bar_timestamp") or item.get("latest_decision_bar_timestamp")
    for item in rows.values():
        item["failed_predicates"] = _top_counter(item["failed_predicates"])
        item["passed_predicates"] = _top_counter(item["passed_predicates"])
        item["missing_fields"] = _top_counter(item["missing_fields"])
        item["observed_predicates"] = sorted(item["observed_predicates"])
    return rows


def _iter_strategy_rows(forensic: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    for bar in forensic.get("per_decision_bar") or forensic.get("decision_bar_rows") or forensic.get("completed_decision_bars") or []:
        if not isinstance(bar, Mapping):
            continue
        for strategy_row in bar.get("strategy_results") or []:
            if isinstance(strategy_row, Mapping):
                rows.append(strategy_row)
    for row in forensic.get("per_strategy_decision_bar_results") or []:
        if isinstance(row, Mapping):
            rows.append(row)
    return rows


def _empty_observed(strategy_id: str) -> dict[str, Any]:
    return {
        "strategy_id": strategy_id,
        "evaluated_count": 0,
        "signal_count": 0,
        "no_signal_count": 0,
        "not_ready_count": 0,
        "suppressed_count": 0,
        "failed_predicates": Counter(),
        "passed_predicates": Counter(),
        "missing_fields": Counter(),
        "observed_predicates": set(),
        "latest_decision_bar_timestamp": None,
    }


def _empty_observed_json(strategy_id: str) -> dict[str, Any]:
    return {
        "strategy_id": strategy_id,
        "evaluated_count": 0,
        "signal_count": 0,
        "no_signal_count": 0,
        "not_ready_count": 0,
        "suppressed_count": 0,
        "failed_predicates": [],
        "passed_predicates": [],
        "missing_fields": [],
        "observed_predicates": [],
        "latest_decision_bar_timestamp": None,
    }


def _track1_reference_availability(repo_root: Path, reference_paths: Sequence[Path]) -> dict[str, Any]:
    default_candidates = [
        Path("outputs/track_b_execution_core/diagnostics/latest_track1_same_day_replay_reference.json"),
        Path("outputs/track_b_execution_core/diagnostics/latest_track1_same_day_replay_reference.csv"),
        Path("outputs/reports/track1_same_day_replay/latest_track1_same_day_replay_reference.json"),
    ]
    checked = list(reference_paths) or default_candidates
    found = []
    missing = []
    for path in checked:
        resolved = _resolve_repo_path(repo_root, path)
        if resolved and resolved.exists():
            found.append(str(resolved))
        else:
            missing.append(str(resolved or path))
    status = "TRACK1_REFERENCE_AVAILABLE" if found else "TRACK1_REFERENCE_UNAVAILABLE"
    return {
        "status": status,
        "paths_checked": [str(_resolve_repo_path(repo_root, path) or path) for path in checked],
        "found": found,
        "missing": missing,
        "same_day_signal_reference_available": bool(found),
        "note": (
            "No bounded same-day Track 1 signal reference artifact was found; parity cannot be proven from replay."
            if not found
            else "At least one bounded Track 1 reference artifact is present."
        ),
    }


def _feature_mapping_comparison(
    entry: TrackBStrategyRegistryEntry,
    *,
    override: Mapping[str, str | None] | None = None,
) -> dict[str, Any]:
    if override:
        track1_fields = set(_normalized_predicates(override.get("track1") or ()))
        trackb_fields = set(_normalized_predicates(override.get("trackb") or ()))
        missing = sorted(track1_fields - trackb_fields)
        extra = sorted(trackb_fields - track1_fields - SAFETY_OR_OPERATIONAL_FIELDS)
        status = "TRACK_B_FIELD_MISMATCH" if missing or extra else "MATCH"
        return {
            "status": status,
            "track1_fields": sorted(track1_fields),
            "track_b_fields": sorted(trackb_fields),
            "missing_track_b_fields": missing,
            "extra_track_b_fields": extra,
        }
    trackb_fields = [_field_name(path) for path in entry.required_feature_schema + entry.required_state_schema]
    return {
        "status": "BEST_AVAILABLE_SOURCE_MAPPING_USED",
        "track1_fields": "UNAVAILABLE_WITHOUT_TRACK1_REFERENCE_ARTIFACT",
        "track_b_fields": sorted(set(trackb_fields)),
        "missing_track_b_fields": [],
        "extra_track_b_fields": [],
    }


def _simple_comparison(
    *,
    track1: Any,
    trackb: Any,
    override: Mapping[str, str | None] | None = None,
    mismatch_status: str,
) -> dict[str, Any]:
    if override:
        track1 = override.get("track1")
        trackb = override.get("trackb")
    status = "MATCH" if track1 == trackb else mismatch_status
    return {"track1": track1, "track_b": trackb, "status": status}


def _strategy_parity_status(
    *,
    extra: Sequence[str],
    missing: Sequence[str],
    feature_mapping: Mapping[str, Any],
    session_comparison: Mapping[str, Any],
    bar_alignment: Mapping[str, Any],
    track1_refs: Mapping[str, Any],
) -> str:
    if extra:
        return "TRACK_B_EXTRA_GATES_MUTED_TRACK1_SIGNALS"
    if missing:
        return "TRACK_B_MISSING_TRACK1_PREDICATES"
    if feature_mapping.get("status") == "TRACK_B_FIELD_MISMATCH":
        return "TRACK_B_FIELD_MISMATCH"
    if session_comparison.get("status") == "TRACK_B_SESSION_MISMATCH":
        return "TRACK_B_SESSION_MISMATCH"
    if bar_alignment.get("status") == "TRACK_B_BAR_ALIGNMENT_MISMATCH":
        return "TRACK_B_BAR_ALIGNMENT_MISMATCH"
    if track1_refs.get("status") == "TRACK1_REFERENCE_UNAVAILABLE":
        return "TRACK1_REFERENCE_UNAVAILABLE"
    return "TRACK_B_PARITY_CONFIRMED_BUT_STRATEGIES_TOO_NARROW"


def _missing_strategy_row(strategy_id: str) -> dict[str, Any]:
    return {
        "track_b_strategy_id": strategy_id,
        "corresponding_track1_strategy": None,
        "corresponding_track1_strategy_source": [],
        "parity_status": "TRACK_B_MISSING_TRACK1_STRATEGIES",
        "track1_predicate_list_if_available": [],
        "track_b_predicate_list": [],
        "extra_track_b_predicates": [],
        "missing_track_b_predicates": [],
        "feature_field_mapping": {"status": "UNAVAILABLE_STRATEGY_NOT_MIGRATED"},
        "session_convention_comparison": {"status": "UNAVAILABLE_STRATEGY_NOT_MIGRATED"},
        "bar_alignment_comparison": {"status": "UNAVAILABLE_STRATEGY_NOT_MIGRATED"},
        "side_direction_comparison": {"status": "UNAVAILABLE_STRATEGY_NOT_MIGRATED"},
        "observed_track_b_evaluations_signals_no_signals": _empty_observed_json(strategy_id),
        "known_track1_replay_or_paper_activity": "UNKNOWN_WITHOUT_TRACK1_REFERENCE_ARTIFACT",
        "recommended_next_action": "Locate the Track 1 source/reference and migrate or explicitly reject this strategy before parity can be asserted.",
    }


def _classifications(
    *,
    aggregate_statuses: Counter[str],
    track1_refs: Mapping[str, Any],
    missing_migrated: Sequence[Mapping[str, Any]],
    lifecycle: Mapping[str, Any],
) -> list[str]:
    classifications: list[str] = []
    if track1_refs.get("status") == "TRACK1_REFERENCE_UNAVAILABLE":
        classifications.append("TRACK1_REFERENCE_UNAVAILABLE")
    if aggregate_statuses.get("TRACK_B_EXTRA_GATES_MUTED_TRACK1_SIGNALS"):
        classifications.append("TRACK_B_EXTRA_GATES_MUTED_TRACK1_SIGNALS")
    if aggregate_statuses.get("TRACK_B_FIELD_MISMATCH"):
        classifications.append("TRACK_B_FIELD_MISMATCH")
    if aggregate_statuses.get("TRACK_B_SESSION_MISMATCH"):
        classifications.append("TRACK_B_SESSION_MISMATCH")
    if aggregate_statuses.get("TRACK_B_BAR_ALIGNMENT_MISMATCH"):
        classifications.append("TRACK_B_BAR_ALIGNMENT_MISMATCH")
    if missing_migrated:
        classifications.append("TRACK_B_MISSING_TRACK1_STRATEGIES")
    if lifecycle.get("proof_style_lifecycle_trade"):
        classifications.append("TRACK_B_LIFECYCLE_PROOF_MODE_ONLY")
    if not classifications:
        classifications.append("TRACK_B_PARITY_CONFIRMED_BUT_STRATEGIES_TOO_NARROW")
    return classifications


def _count_blockers(
    blockers: Counter[str],
    *,
    parity_status: str,
    observed: Mapping[str, Any],
    lifecycle: Mapping[str, Any],
) -> None:
    if parity_status in {"TRACK_B_EXTRA_GATES_MUTED_TRACK1_SIGNALS", "TRACK_B_MISSING_TRACK1_PREDICATES", "TRACK_B_FIELD_MISMATCH", "TRACK_B_SESSION_MISMATCH", "TRACK_B_BAR_ALIGNMENT_MISMATCH"}:
        blockers["strategy predicate"] += 1
    if observed.get("not_ready_count", 0):
        blockers["operational readiness gate"] += int(observed.get("not_ready_count") or 0)
    if observed.get("suppressed_count", 0):
        blockers["arbitration gate"] += int(observed.get("suppressed_count") or 0)
    if lifecycle:
        blockers["lifecycle/trade-management behavior"] += 1


def _lifecycle_summary(postmortem: Mapping[str, Any]) -> dict[str, Any]:
    trades = postmortem.get("trade_lifecycle_reconstruction") if isinstance(postmortem, Mapping) else []
    for trade in trades or []:
        if not isinstance(trade, Mapping):
            continue
        proof_style = bool(trade.get("closed_by_guarded_paper_proof_lifecycle")) and not bool(
            trade.get("closed_by_normal_strategy_logic")
        )
        return {
            "strategy": trade.get("strategy"),
            "instrument": trade.get("instrument"),
            "hold_duration_seconds": trade.get("hold_duration_seconds"),
            "exit_reason": trade.get("exit_reason"),
            "proof_style_lifecycle_trade": proof_style,
        }
    return {}


def _summary(strategy_rows: Sequence[Mapping[str, Any]], classifications: Sequence[str], lifecycle: Mapping[str, Any]) -> dict[str, Any]:
    observed_evals = sum(
        int(_nested(row, "observed_track_b_evaluations_signals_no_signals", "evaluated_count") or 0)
        for row in strategy_rows
    )
    signals = sum(
        int(_nested(row, "observed_track_b_evaluations_signals_no_signals", "signal_count") or 0)
        for row in strategy_rows
    )
    return {
        "audited_strategy_count": len(strategy_rows),
        "track_b_strategy_evaluations_observed": observed_evals,
        "track_b_signals_observed": signals,
        "track_b_proof_style_lifecycle_trade_observed": bool(lifecycle.get("proof_style_lifecycle_trade")),
        "primary_classification": classifications[0] if classifications else "DIAGNOSTIC_INCONCLUSIVE",
        "parity_can_be_proven_from_same_day_track1_replay": "TRACK1_REFERENCE_UNAVAILABLE" not in classifications,
    }


def _recommended_next_action(
    classifications: Sequence[str],
    track1_refs: Mapping[str, Any],
    strategy_rows: Sequence[Mapping[str, Any]],
) -> str:
    if "TRACK1_REFERENCE_UNAVAILABLE" in classifications:
        return (
            "Export or preserve bounded Track 1 same-day signal/reference artifacts for the audited strategies, "
            "then rerun this audit. Source-level predicate surfaces were compared, but replay parity is not proven."
        )
    if any(row.get("extra_track_b_predicates") for row in strategy_rows):
        return "Review extra Track B strategy predicates before interpreting zero PAPER activity as genuine Track 1 parity."
    return "Replay parity is source-confirmed for the available reference set; continue coverage and lifecycle analysis."


def _strategy_recommendation(
    *,
    parity_status: str,
    track1_refs: Mapping[str, Any],
    extra: Sequence[str],
    missing: Sequence[str],
    feature_mapping: Mapping[str, Any],
) -> str:
    if extra:
        return f"Review extra Track B predicate(s): {', '.join(extra)}."
    if missing:
        return f"Track B is missing Track 1 predicate(s): {', '.join(missing)}."
    if feature_mapping.get("status") == "TRACK_B_FIELD_MISMATCH":
        return "Review Track 1-to-Track B feature mapping before evaluating parity."
    if parity_status == "TRACK1_REFERENCE_UNAVAILABLE":
        return "Preserve/export same-day Track 1 reference outputs; source-level predicate surface only is available."
    return "No predicate surface mismatch detected for the available reference data."


def _known_track1_activity(track1_refs: Mapping[str, Any]) -> str:
    if track1_refs.get("status") == "TRACK1_REFERENCE_UNAVAILABLE":
        return "UNKNOWN_WITHOUT_TRACK1_REFERENCE_ARTIFACT"
    return "REFERENCE_ARTIFACT_PRESENT_NOT_PARSED_IN_THIS_PASS"


def _render_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Track 1 vs Track B Same-Day Parity Audit",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        "## Summary",
        "",
        f"- Primary classification: `{_nested(report, 'summary', 'primary_classification')}`",
        f"- Audited strategies: `{report.get('audited_strategy_count')}`",
        f"- Track B strategy evaluations observed: `{_nested(report, 'summary', 'track_b_strategy_evaluations_observed')}`",
        f"- Track B signals observed: `{_nested(report, 'summary', 'track_b_signals_observed')}`",
        f"- Track 1 same-day reference status: `{_nested(report, 'track1_reference_artifacts', 'status')}`",
        f"- Recommended next action: {report.get('recommended_next_action')}",
        "",
        "## Classifications",
        "",
    ]
    lines.extend(f"- `{item}`" for item in report.get("classifications") or [])
    lines.extend(["", "## Per-Strategy Audit", ""])
    for row in report.get("strategy_parity") or []:
        lines.extend(
            [
                f"### {row.get('track_b_strategy_id')}",
                "",
                f"- Status: `{row.get('parity_status')}`",
                f"- Track 1 source: `{row.get('corresponding_track1_strategy')}`",
                f"- Extra Track B predicates: `{', '.join(row.get('extra_track_b_predicates') or []) or 'none'}`",
                f"- Missing Track B predicates: `{', '.join(row.get('missing_track_b_predicates') or []) or 'none'}`",
                f"- Track B evaluations/signals/no-signals: `{_nested(row, 'observed_track_b_evaluations_signals_no_signals', 'evaluated_count')}` / `{_nested(row, 'observed_track_b_evaluations_signals_no_signals', 'signal_count')}` / `{_nested(row, 'observed_track_b_evaluations_signals_no_signals', 'no_signal_count')}`",
                f"- Next action: {row.get('recommended_next_action')}",
                "",
            ]
        )
    lines.extend(
        [
            "## Safety",
            "",
            "- Broker commands invoked: `false`",
            "- paper_proof_cli invoked: `false`",
            "- submit/cancel/placeOrder invoked: `false`",
            "- Broker state mutated by audit: `false`",
        ]
    )
    return "\n".join(lines) + "\n"


def _load_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists() or not path.is_file():
        return {}
    try:
        if path.stat().st_size > MAX_JSON_BYTES:
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _resolve_repo_path(repo_root: Path, value: Any) -> Path | None:
    if value is None:
        return None
    path = Path(value)
    if path.is_absolute():
        return path
    return repo_root / path


def _existing_source_files(repo_root: Path, source_files: Sequence[str]) -> list[str]:
    return [path for path in source_files if (repo_root / path).exists()]


def _normalized_predicates(values: Any) -> list[str]:
    if isinstance(values, str):
        return [_field_name(values)] if values.strip() else []
    if not isinstance(values, Sequence):
        return []
    return sorted({_field_name(str(item)) for item in values if str(item).strip()})


def _field_name(path: str) -> str:
    text = str(path or "").strip()
    if not text:
        return text
    if text in SAFETY_OR_OPERATIONAL_FIELDS:
        return text
    return text.split(".")[-1]


def _top_counter(counter: Counter[str]) -> list[dict[str, Any]]:
    return [{"name": name, "count": count} for name, count in counter.most_common()]


def _track_b_side(strategy_id: str) -> str | None:
    if strategy_id == "asian_drift_v1":
        return "LONG_OR_SHORT_FROM_STATE"
    if "BEAR" in strategy_id or "SHORT" in strategy_id:
        return "SHORT"
    if "BULL" in strategy_id or "LONG" in strategy_id:
        return "LONG"
    return None


def _track_b_session_convention(entry: TrackBStrategyRegistryEntry) -> str:
    if "ASIA_EARLY" in entry.strategy_id:
        return "ASIA_EARLY completed 5m" if "BREAKOUT" not in entry.strategy_id else "ASIA_EARLY or GC/MGC London-open completed 5m"
    if "LONDON_LATE" in entry.strategy_id:
        return "LONDON_LATE completed 5m"
    if "ASIA_LATE" in entry.strategy_id:
        return "ASIA_LATE completed 5m"
    if "US_DERIVATIVE" in entry.strategy_id:
        return "US derivative-bear window completed 5m"
    if "US_LATE" in entry.strategy_id:
        return "US_LATE completed 5m"
    if "FIRST_" in entry.strategy_id:
        return "session_allowed completed 5m"
    if entry.strategy_id == "asian_drift_v1":
        return "Asian drift entry window from precomputed state snapshot"
    return f"{entry.instrument_family} {entry.timeframe}"


def _track_b_bar_alignment(entry: TrackBStrategyRegistryEntry) -> str:
    if entry.strategy_id == "asian_drift_v1":
        return "completed 5m state snapshot"
    if "FIRST_BULL" in entry.strategy_id:
        return "current completed 5m first bull snap-turn signal bar"
    if "FIRST_BEAR" in entry.strategy_id:
        return "current completed 5m first bear snap-turn signal bar"
    if "BREAKOUT_RETEST" in entry.strategy_id:
        return "current completed 5m retest/hold signal bar"
    if "PULLBACK" in entry.strategy_id or "PAUSE_RESUME_LONG" in entry.strategy_id:
        return "current completed 5m pullback/resume signal bar"
    return "current completed 5m signal bar"


def _nested(payload: Mapping[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current
