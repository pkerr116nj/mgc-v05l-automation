"""Track B snap-turn near-miss / MFE-MAE amplification audit.

This diagnostic is read-only. It analyzes completed-bar snap-turn evaluations
and subsequent market excursions so candidate variants can be ranked before any
production predicate or threshold changes are proposed.
"""

from __future__ import annotations

import json
import random
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping

from .models import require_aware_datetime, to_jsonable
from .track_b_snap_turn_envelope_producer import produce_track_b_snap_turn_envelopes


DEFAULT_DIAGNOSTICS_ROOT = Path("outputs/track_b_execution_core/diagnostics")
DEFAULT_RUNTIME_CYCLE_ROOT = Path("outputs/track_b_execution_core/track_b_multi_strategy_runtime_cycle")
DEFAULT_SNAP_TURN_ROOT = Path("outputs/track_b_execution_core/snap_turn_state")
DEFAULT_MGC_LIVE_5M = Path(
    "outputs/track_b_execution_core/databento_live_runtime_feed/latest_live_mgc_completed_5m_candles.json"
)
DEFAULT_MNQ_LIVE_5M = Path(
    "outputs/track_b_execution_core/databento_live_runtime_feed/latest_live_mnq_completed_5m_candles.json"
)

SNAP_TURN_STRATEGIES: dict[str, dict[str, str]] = {
    "FIRST_BULL_SNAP_TURN_V1": {
        "instrument": "MGC",
        "side": "LONG",
        "state_key": "first_bull_snap_turn_state",
        "features_key": "first_bull_snap_turn_features",
        "prefix": "bull",
    },
    "FIRST_BEAR_SNAP_TURN_V1": {
        "instrument": "MGC",
        "side": "SHORT",
        "state_key": "first_bear_snap_turn_state",
        "features_key": "first_bear_snap_turn_features",
        "prefix": "bear",
    },
    "MNQ_FIRST_BULL_SNAP_TURN_V1": {
        "instrument": "MNQ",
        "side": "LONG",
        "state_key": "mnq_first_bull_snap_turn_state",
        "features_key": "mnq_first_bull_snap_turn_features",
        "prefix": "bull",
    },
    "MNQ_FIRST_BEAR_SNAP_TURN_V1": {
        "instrument": "MNQ",
        "side": "SHORT",
        "state_key": "mnq_first_bear_snap_turn_state",
        "features_key": "mnq_first_bear_snap_turn_features",
        "prefix": "bear",
    },
}

SNAP_TURN_EXPECTED_FREQUENCY: dict[str, dict[str, Any]] = {
    "FIRST_BULL_SNAP_TURN_V1": {
        "expected_eligible_bars_per_day": [80, 260],
        "expected_hard_signals_per_day": [1.0, 2.0],
        "expectation_source": "OPERATOR_EXPECTATION_PROVISIONAL_TRACK_B_CONTRACT",
    },
    "FIRST_BEAR_SNAP_TURN_V1": {
        "expected_eligible_bars_per_day": [80, 260],
        "expected_hard_signals_per_day": [1.0, 2.0],
        "expectation_source": "OPERATOR_EXPECTATION_PROVISIONAL_TRACK_B_CONTRACT",
    },
    "MNQ_FIRST_BULL_SNAP_TURN_V1": {
        "expected_eligible_bars_per_day": [80, 260],
        "expected_hard_signals_per_day": [1.0, 2.0],
        "expectation_source": "OPERATOR_EXPECTATION_PROVISIONAL_TRACK_B_CONTRACT",
    },
    "MNQ_FIRST_BEAR_SNAP_TURN_V1": {
        "expected_eligible_bars_per_day": [80, 260],
        "expected_hard_signals_per_day": [1.0, 2.0],
        "expectation_source": "OPERATOR_EXPECTATION_PROVISIONAL_TRACK_B_CONTRACT",
    },
}


@dataclass(frozen=True)
class TrackBSnapTurnNearMissAmplificationConfig:
    repo_root: Path = Path(".")
    diagnostics_root: Path = DEFAULT_DIAGNOSTICS_ROOT
    runtime_cycle_root: Path = DEFAULT_RUNTIME_CYCLE_ROOT
    snap_turn_root: Path = DEFAULT_SNAP_TURN_ROOT
    mgc_live_5m: Path = DEFAULT_MGC_LIVE_5M
    mnq_live_5m: Path = DEFAULT_MNQ_LIVE_5M
    output_json: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_snap_turn_near_miss_amplification.json"
    output_md: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_snap_turn_near_miss_amplification.md"
    scorable_snapshots_jsonl: Path = DEFAULT_DIAGNOSTICS_ROOT / "track_b_snap_turn_scorable_snapshots.jsonl"
    latest_scorable_snapshots_json: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_snap_turn_scorable_snapshots.json"
    max_runtime_reports: int = 1800
    max_examples_per_strategy: int = 12
    future_horizon_bars: int = 6


@dataclass(frozen=True)
class TrackBSnapTurnNearMissAmplificationResult:
    report_json: Path
    report_md: Path
    report: dict[str, Any]


@dataclass(frozen=True)
class TrackBSnapTurnReplayBackfillConfig:
    repo_root: Path = Path(".")
    mgc_candle_payloads: tuple[Path, ...] = (DEFAULT_MGC_LIVE_5M,)
    mnq_candle_payloads: tuple[Path, ...] = (DEFAULT_MNQ_LIVE_5M,)
    retained_5m_context_root: Path = Path("outputs/track_b_execution_core/asian_drift_state")
    replay_envelope_root: Path = DEFAULT_DIAGNOSTICS_ROOT / "snap_turn_replay_backfill_envelopes"
    output_json: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_snap_turn_replay_backfill.json"
    output_md: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_snap_turn_replay_backfill.md"
    scorable_snapshots_jsonl: Path = DEFAULT_DIAGNOSTICS_ROOT / "track_b_snap_turn_replay_scorable_snapshots.jsonl"
    latest_scorable_snapshots_json: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_snap_turn_replay_scorable_snapshots.json"
    min_window_bars: int = 8
    max_windows_per_instrument: int = 240
    future_horizon_bars: int = 6
    max_examples_per_strategy: int = 20


@dataclass(frozen=True)
class TrackBSnapTurnReplayBackfillResult:
    report_json: Path
    report_md: Path
    report: dict[str, Any]


@dataclass(frozen=True)
class TrackBSnapTurnLocationVariantResearchConfig:
    repo_root: Path = Path(".")
    candidate_strategy_id: str = "MNQ_FIRST_BEAR_SNAP_TURN_V1"
    candidate_name: str = "MNQ_FIRST_BEAR_SNAP_TURN_LOCATION_VARIANT_RESEARCH_V1"
    candidate_predicate: str = "bear_snap_location_ok"
    replay_backfill_config: TrackBSnapTurnReplayBackfillConfig | None = None
    refresh_replay_backfill: bool = True
    snapshots_json: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_snap_turn_replay_scorable_snapshots.json"
    output_json: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_snap_turn_location_variant_research_replay.json"
    output_md: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_snap_turn_location_variant_research_replay.md"
    max_horizon_bars: int = 6
    time_box_bars: int = 3
    minimum_sample_count: int = 20
    random_baseline_seed: int = 17


@dataclass(frozen=True)
class TrackBSnapTurnLocationVariantResearchResult:
    report_json: Path
    report_md: Path
    report: dict[str, Any]


@dataclass(frozen=True)
class TrackBSnapTurnLocationVariantExitSensitivityConfig:
    repo_root: Path = Path(".")
    research_replay_config: TrackBSnapTurnLocationVariantResearchConfig | None = None
    refresh_research_replay: bool = True
    research_replay_json: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_snap_turn_location_variant_research_replay.json"
    output_json: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_snap_turn_location_variant_exit_sensitivity.json"
    output_md: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_snap_turn_location_variant_exit_sensitivity.md"
    minimum_sample_count: int = 20


@dataclass(frozen=True)
class TrackBSnapTurnLocationVariantExitSensitivityResult:
    report_json: Path
    report_md: Path
    report: dict[str, Any]


def create_track_b_snap_turn_near_miss_amplification(
    *,
    config: TrackBSnapTurnNearMissAmplificationConfig | None = None,
    now: datetime | None = None,
) -> TrackBSnapTurnNearMissAmplificationResult:
    actual_config = config or TrackBSnapTurnNearMissAmplificationConfig()
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    repo_root = Path(actual_config.repo_root)
    runtime_reports = _load_runtime_reports(
        _resolve(repo_root, actual_config.runtime_cycle_root),
        max_reports=actual_config.max_runtime_reports,
    )
    evaluated_rows = _evaluated_snap_turn_rows(runtime_reports, repo_root=repo_root)
    candle_index = _candle_index(
        evaluated_rows=evaluated_rows,
        repo_root=repo_root,
        snap_turn_root=actual_config.snap_turn_root,
        mgc_live_5m=actual_config.mgc_live_5m,
        mnq_live_5m=actual_config.mnq_live_5m,
    )
    strategies = [
        _strategy_report(
            strategy_id=strategy_id,
            rows=[row for row in evaluated_rows if row["strategy_id"] == strategy_id],
            candle_index=candle_index,
            future_horizon_bars=actual_config.future_horizon_bars,
            max_examples=actual_config.max_examples_per_strategy,
        )
        for strategy_id in SNAP_TURN_STRATEGIES
    ]
    scorable_snapshots = _scorable_snapshots(
        rows=evaluated_rows,
        candle_index=candle_index,
        future_horizon_bars=actual_config.future_horizon_bars,
    )
    scorable_snapshot_summary = _write_scorable_snapshots(
        repo_root=repo_root,
        jsonl_path=actual_config.scorable_snapshots_jsonl,
        latest_json_path=actual_config.latest_scorable_snapshots_json,
        snapshots=scorable_snapshots,
        generated_at=actual_now,
    )
    report = {
        "schema_version": "track_b_snap_turn_near_miss_amplification_v1",
        "generated_at": actual_now.isoformat(),
        "source": "TRACK_B_COMPLETED_DECISION_BAR_RUNTIME_ARTIFACTS",
        "analysis_note": (
            "Near-miss buckets use deduplicated primitive snap-turn gates. Raw/candidate/first "
            "snap-turn composites are still reported separately so they do not hide the underlying predicate."
        ),
        "near_miss_math_validation": {
            "one_predicate_away": "Exactly one failed deduplicated primitive gate after session/data/instrument eligibility.",
            "two_predicates_away": "Exactly two failed deduplicated primitive gates after session/data/instrument eligibility.",
            "composites_counted_separately": [
                "bull_snap_raw",
                "bear_snap_raw",
                "bull_snap_turn_candidate",
                "bear_snap_turn_candidate",
                "first_bull_snap_turn",
                "first_bear_snap_turn",
            ],
            "closest_failed_bars": (
                "All scoreable NO_SIGNAL bars are ranked by primitive failed-predicate count, then by "
                "aggregate numeric miss distance where available; this catches commercially close bars "
                "that are still more than two predicates away."
            ),
        },
        "predicate_hierarchy_validation": {
            "rule_runner_evaluates_all_conditions_without_early_exit": True,
            "primitive_gates_reconstructed_from_feature_envelope": True,
            "downstream_skip_risk": (
                "LOW when state/feature envelope is available because all rule_conditions are materialized. "
                "METHODOLOGY_INCONCLUSIVE for rotated/missing envelopes."
            ),
        },
        "broker_commands_invoked": False,
        "paper_proof_cli_invoked": False,
        "submit_cancel_place_order_invoked": False,
        "production_thresholds_changed": False,
        "runtime_reports_scanned": len(runtime_reports),
        "bounded_runtime_report_limit": actual_config.max_runtime_reports,
        "completed_decision_strategy_rows_after_dedup": len(evaluated_rows),
        "future_excursion_horizon_bars": actual_config.future_horizon_bars,
        "strategies": strategies,
        "variant_candidates": _variant_candidates(strategies),
        "defects_or_parity_checks": _defects_or_parity_checks(strategies),
        "scorable_snapshot_retention": scorable_snapshot_summary,
        "improvement_methods_to_test": _improvement_methods_to_test(),
        "immediate_replay_backfill_path": _immediate_replay_backfill_path(scorable_snapshot_summary),
        "output_classifications": _output_classifications(strategies, scorable_snapshot_summary),
        "conclusion": _conclusion(strategies),
        "outputs": {
            "json": str(actual_config.output_json),
            "markdown": str(actual_config.output_md),
            "scorable_snapshots_jsonl": str(actual_config.scorable_snapshots_jsonl),
            "latest_scorable_snapshots_json": str(actual_config.latest_scorable_snapshots_json),
        },
    }
    output_json = _resolve(repo_root, actual_config.output_json)
    output_md = _resolve(repo_root, actual_config.output_md)
    _write_json(output_json, report)
    _write_text(output_md, _markdown(report))
    return TrackBSnapTurnNearMissAmplificationResult(report_json=output_json, report_md=output_md, report=report)


def create_track_b_snap_turn_replay_backfill(
    *,
    config: TrackBSnapTurnReplayBackfillConfig | None = None,
    now: datetime | None = None,
) -> TrackBSnapTurnReplayBackfillResult:
    actual_config = config or TrackBSnapTurnReplayBackfillConfig()
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    repo_root = Path(actual_config.repo_root)
    instrument_candles = {
        "MGC": _load_replay_candles(
            repo_root=repo_root,
            paths=actual_config.mgc_candle_payloads,
            instrument="MGC",
            retained_5m_context_root=actual_config.retained_5m_context_root,
        ),
        "MNQ": _load_replay_candles(
            repo_root=repo_root,
            paths=actual_config.mnq_candle_payloads,
            instrument="MNQ",
            retained_5m_context_root=actual_config.retained_5m_context_root,
        ),
    }
    source_files = {
        instrument: _replay_source_file_summary(
            repo_root=repo_root,
            paths=actual_config.mgc_candle_payloads if instrument == "MGC" else actual_config.mnq_candle_payloads,
            instrument=instrument,
            retained_5m_context_root=actual_config.retained_5m_context_root,
        )
        for instrument in ("MGC", "MNQ")
    }
    rows: list[dict[str, Any]] = []
    reconstruction: dict[str, Any] = {}
    for instrument, candles in instrument_candles.items():
        instrument_rows, instrument_summary = _reconstruct_snap_turn_rows_for_instrument(
            repo_root=repo_root,
            instrument=instrument,
            candles=candles,
            config=actual_config,
            now=actual_now,
        )
        rows.extend(instrument_rows)
        reconstruction[instrument] = instrument_summary
    candle_index = {instrument: candles for instrument, candles in instrument_candles.items()}
    strategies = [
        _strategy_report(
            strategy_id=strategy_id,
            rows=[row for row in rows if row["strategy_id"] == strategy_id],
            candle_index=candle_index,
            future_horizon_bars=actual_config.future_horizon_bars,
            max_examples=actual_config.max_examples_per_strategy,
        )
        for strategy_id in SNAP_TURN_STRATEGIES
    ]
    snapshots = _scorable_snapshots(rows=rows, candle_index=candle_index, future_horizon_bars=actual_config.future_horizon_bars)
    snapshot_summary = _write_scorable_snapshots(
        repo_root=repo_root,
        jsonl_path=actual_config.scorable_snapshots_jsonl,
        latest_json_path=actual_config.latest_scorable_snapshots_json,
        snapshots=snapshots,
        generated_at=actual_now,
    )
    sample_frame = _sample_frame_from_rows(
        rows=rows,
        source_data_files=[item for values in source_files.values() for item in values],
        sample_source_type="RETAINED_ROLLING_COMPLETED_5M_CONTEXT",
        sample_unit="completed_5m_decision_bars",
        missing_limitations=_replay_missing_limitations(reconstruction=reconstruction, source_files=source_files),
        preferred_classification=None,
    )
    classifications = _replay_classifications(strategies, snapshot_summary)
    report = {
        "schema_version": "track_b_snap_turn_replay_backfill_v1",
        "generated_at": actual_now.isoformat(),
        "source": "RETAINED_COMPLETED_5M_CANDLE_REPLAY_BACKFILL",
        "sample_frame": sample_frame,
        "broker_commands_invoked": False,
        "paper_proof_cli_invoked": False,
        "submit_cancel_place_order_invoked": False,
        "production_thresholds_changed": False,
        "managed_paper_lifecycle_changed": False,
        "instrument_reconstruction": reconstruction,
        "dense_scorable_snapshot_output": snapshot_summary,
        "strategies": strategies,
        "classifications": classifications,
        "variant_candidates": _variant_candidates(strategies),
        "simplest_methodology_change_to_test_first": _simplest_methodology_change(strategies),
        "outputs": {
            "json": str(actual_config.output_json),
            "markdown": str(actual_config.output_md),
            "scorable_snapshots_jsonl": str(actual_config.scorable_snapshots_jsonl),
            "latest_scorable_snapshots_json": str(actual_config.latest_scorable_snapshots_json),
        },
    }
    output_json = _resolve(repo_root, actual_config.output_json)
    output_md = _resolve(repo_root, actual_config.output_md)
    _write_json(output_json, report)
    _write_text(output_md, _replay_markdown(report))
    return TrackBSnapTurnReplayBackfillResult(report_json=output_json, report_md=output_md, report=report)


def create_track_b_snap_turn_location_variant_research_replay(
    *,
    config: TrackBSnapTurnLocationVariantResearchConfig | None = None,
    now: datetime | None = None,
) -> TrackBSnapTurnLocationVariantResearchResult:
    actual_config = config or TrackBSnapTurnLocationVariantResearchConfig()
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    repo_root = Path(actual_config.repo_root)
    replay_config = actual_config.replay_backfill_config or TrackBSnapTurnReplayBackfillConfig(repo_root=repo_root)
    replay_report: Mapping[str, Any] = {}
    if actual_config.refresh_replay_backfill:
        replay_report = create_track_b_snap_turn_replay_backfill(config=replay_config, now=actual_now).report
    snapshots_payload = _load_json(_resolve(repo_root, actual_config.snapshots_json))
    snapshots = [item for item in snapshots_payload.get("snapshots") or [] if isinstance(item, Mapping)]
    candles = _load_replay_candles(
        repo_root=repo_root,
        paths=replay_config.mnq_candle_payloads,
        instrument="MNQ",
        retained_5m_context_root=replay_config.retained_5m_context_root,
    )
    candidate_rows = _location_variant_candidate_rows(
        snapshots=snapshots,
        strategy_id=actual_config.candidate_strategy_id,
        predicate=actual_config.candidate_predicate,
    )
    sample_results = [
        _research_sample_result(
            snapshot=snapshot,
            candles=candles,
            max_horizon_bars=actual_config.max_horizon_bars,
            time_box_bars=actual_config.time_box_bars,
        )
        for snapshot in candidate_rows
    ]
    production_rows = [
        item
        for item in snapshots
        if item.get("strategy_id") == actual_config.candidate_strategy_id and item.get("hard_signal") is True
    ]
    production_results = [
        _research_sample_result(
            snapshot=snapshot,
            candles=candles,
            max_horizon_bars=actual_config.max_horizon_bars,
            time_box_bars=actual_config.time_box_bars,
        )
        for snapshot in production_rows
    ]
    baseline_rows = _baseline_rows(
        snapshots=snapshots,
        excluded=candidate_rows,
        strategy_id=actual_config.candidate_strategy_id,
        sample_count=len(candidate_rows),
        seed=actual_config.random_baseline_seed,
    )
    baseline_results = [
        _research_sample_result(
            snapshot=snapshot,
            candles=candles,
            max_horizon_bars=actual_config.max_horizon_bars,
            time_box_bars=actual_config.time_box_bars,
        )
        for snapshot in baseline_rows
    ]
    policy_summary = _policy_summary(sample_results)
    baseline_summary = _policy_summary(baseline_results)
    production_summary = _policy_summary(production_results)
    sample_frame = _sample_frame_from_rows(
        rows=sample_results,
        source_data_files=(replay_report.get("sample_frame") or {}).get("source_data_files") or [],
        sample_source_type="LOCATION_VARIANT_RESEARCH_SAMPLE_FROM_REPLAY_BACKFILL",
        sample_unit="candidate_near_miss_samples",
        missing_limitations=_sample_frame_limitations_from_parent(replay_report),
        preferred_classification=None,
        instrument_fallback="MNQ",
    )
    classification = _location_variant_classification(
        sample_count=len(sample_results),
        policy_summary=policy_summary,
        baseline_summary=baseline_summary,
        minimum_sample_count=actual_config.minimum_sample_count,
    )
    report = {
        "schema_version": "track_b_snap_turn_location_variant_research_replay_v1",
        "generated_at": actual_now.isoformat(),
        "candidate_name": actual_config.candidate_name,
        "candidate_strategy_id": actual_config.candidate_strategy_id,
        "candidate_predicate": actual_config.candidate_predicate,
        "candidate_status": "RESEARCH_ONLY",
        "paper_eligible": False,
        "live_money_eligible": False,
        "production_thresholds_changed": False,
        "production_strategy_changed": False,
        "broker_commands_invoked": False,
        "paper_proof_cli_invoked": False,
        "submit_cancel_place_order_invoked": False,
        "sample_frame": sample_frame,
        "classification": classification,
        "sample_count": len(sample_results),
        "production_signal_sample_count": len(production_results),
        "baseline_sample_count": len(baseline_results),
        "policy_summary": policy_summary,
        "production_strategy_comparison": production_summary,
        "random_baseline_comparison": baseline_summary,
        "samples": sample_results,
        "outlier_sensitivity": _outlier_sensitivity(sample_results),
        "loss_clustering": _loss_clustering(sample_results, policy="time_boxed_3x5m"),
        "replay_backfill_source": {
            "refreshed": actual_config.refresh_replay_backfill,
            "replay_report_classifications": replay_report.get("classifications") if replay_report else None,
            "snapshots_json": str(actual_config.snapshots_json),
        },
        "expected_answer": _location_variant_answer(classification, policy_summary, baseline_summary),
    }
    output_json = _resolve(repo_root, actual_config.output_json)
    output_md = _resolve(repo_root, actual_config.output_md)
    _write_json(output_json, report)
    _write_text(output_md, _location_variant_markdown(report))
    return TrackBSnapTurnLocationVariantResearchResult(report_json=output_json, report_md=output_md, report=report)


def create_track_b_snap_turn_location_variant_exit_sensitivity(
    *,
    config: TrackBSnapTurnLocationVariantExitSensitivityConfig | None = None,
    now: datetime | None = None,
) -> TrackBSnapTurnLocationVariantExitSensitivityResult:
    actual_config = config or TrackBSnapTurnLocationVariantExitSensitivityConfig()
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    repo_root = Path(actual_config.repo_root)
    research_report: Mapping[str, Any] = {}
    if actual_config.refresh_research_replay:
        research_config = actual_config.research_replay_config or TrackBSnapTurnLocationVariantResearchConfig(
            repo_root=repo_root
        )
        research_report = create_track_b_snap_turn_location_variant_research_replay(
            config=research_config,
            now=actual_now,
        ).report
    else:
        research_report = _load_json(_resolve(repo_root, actual_config.research_replay_json))
    samples = [item for item in research_report.get("samples") or [] if isinstance(item, Mapping)]
    policy_summary = _policy_summary(samples, include_exit_sensitivity=True)
    sample_frame = _sample_frame_from_rows(
        rows=samples,
        source_data_files=(research_report.get("sample_frame") or {}).get("source_data_files") or [],
        sample_source_type="EXIT_SENSITIVITY_SAMPLE_FROM_LOCATION_VARIANT_RESEARCH_REPLAY",
        sample_unit="candidate_near_miss_samples",
        missing_limitations=_sample_frame_limitations_from_parent(research_report),
        preferred_classification=None,
        instrument_fallback="MNQ",
    )
    early_favorable_count = sum(1 for item in samples if item.get("early_favorable_move_before_failing") is True)
    immediate_loser_count = sum(1 for item in samples if item.get("loser_failed_immediately") is True)
    sample_count = len(samples)
    classification = _exit_sensitivity_classification(
        sample_count=sample_count,
        minimum_sample_count=actual_config.minimum_sample_count,
        policy_summary=policy_summary,
        early_favorable_count=early_favorable_count,
        immediate_loser_count=immediate_loser_count,
    )
    report = {
        "schema_version": "track_b_snap_turn_location_variant_exit_sensitivity_v1",
        "generated_at": actual_now.isoformat(),
        "candidate_name": research_report.get("candidate_name")
        or "MNQ_FIRST_BEAR_SNAP_TURN_LOCATION_VARIANT_RESEARCH_V1",
        "candidate_status": "RESEARCH_ONLY",
        "paper_eligible": False,
        "production_thresholds_changed": False,
        "production_strategy_changed": False,
        "broker_commands_invoked": False,
        "paper_proof_cli_invoked": False,
        "submit_cancel_place_order_invoked": False,
        "sample_frame": sample_frame,
        "classification": classification,
        "sample_count": sample_count,
        "early_favorable_move_count": early_favorable_count,
        "early_favorable_move_rate": _ratio_str(early_favorable_count, sample_count),
        "loser_failed_immediately_count": immediate_loser_count,
        "loser_failed_immediately_rate": _ratio_str(immediate_loser_count, sample_count),
        "policy_summary": policy_summary,
        "best_policy": _best_policy(policy_summary),
        "samples": samples,
        "answer": _exit_sensitivity_answer(classification, policy_summary),
        "research_replay_source": {
            "refreshed": actual_config.refresh_research_replay,
            "research_replay_json": str(actual_config.research_replay_json),
            "research_replay_classification": research_report.get("classification"),
        },
    }
    output_json = _resolve(repo_root, actual_config.output_json)
    output_md = _resolve(repo_root, actual_config.output_md)
    _write_json(output_json, report)
    _write_text(output_md, _exit_sensitivity_markdown(report))
    return TrackBSnapTurnLocationVariantExitSensitivityResult(report_json=output_json, report_md=output_md, report=report)


def _strategy_report(
    *,
    strategy_id: str,
    rows: list[dict[str, Any]],
    candle_index: Mapping[str, list[dict[str, Any]]],
    future_horizon_bars: int,
    max_examples: int,
) -> dict[str, Any]:
    meta = SNAP_TURN_STRATEGIES[strategy_id]
    failed_rule_counts: Counter[str] = Counter()
    failed_primitive_counts: Counter[str] = Counter()
    primitive_classifications: dict[str, Counter[str]] = defaultdict(Counter)
    numeric_distances: dict[str, list[Decimal]] = defaultdict(list)
    excursion_by_predicate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    session_buckets: Counter[str] = Counter()
    regime_buckets: Counter[str] = Counter()
    near_examples: list[dict[str, Any]] = []
    closest_failed_examples: list[dict[str, Any]] = []
    eligibility_counts: Counter[str] = Counter()
    hard_signals = 0
    one_predicate_away = 0
    two_predicates_away = 0
    not_scorable = 0
    no_signal = 0
    not_ready = 0
    for row in rows:
        eligibility = _eligibility(row, expected_instrument=str(meta["instrument"]))
        eligibility_counts.update(["evaluated"])
        if eligibility["instrument_match"]:
            eligibility_counts.update(["instrument_match"])
        if eligibility["data_ready"]:
            eligibility_counts.update(["data_ready"])
        if eligibility["session_phase_ready"]:
            eligibility_counts.update(["session_phase_ready"])
        if eligibility["eligible"]:
            eligibility_counts.update(["eligible"])
        result = row["result"]
        if result == "SIGNAL":
            hard_signals += 1
            continue
        if result == "NOT_READY":
            not_ready += 1
            continue
        no_signal += 1
        state = row["state"]
        features = row["features"]
        failed_rule_counts.update(row["failed_rule_predicates"])
        session_buckets.update([_session_bucket(state)])
        regime_buckets.update([_regime_bucket(row)])
        primitive = _primitive_predicates(row)
        failed_primitives = [item for item in primitive if item["passed"] is not True]
        failed_primitive_names = [str(item["predicate"]) for item in failed_primitives]
        failed_primitive_counts.update(failed_primitive_names)
        for item in failed_primitives:
            predicate = str(item["predicate"])
            classification = _predicate_classification(predicate, row)
            primitive_classifications[predicate].update([classification])
            distance = item.get("distance")
            if isinstance(distance, Decimal):
                numeric_distances[predicate].append(distance)
        bucket = (
            "NOT_SCORABLE"
            if any(str(item.get("predicate") or "") == "snap_turn_feature_envelope_missing" for item in failed_primitives)
            else _near_miss_bucket(len(failed_primitives), result=result)
        )
        if bucket == "NOT_SCORABLE":
            not_scorable += 1
        if bucket == "ONE_PREDICATE_AWAY":
            one_predicate_away += 1
        if bucket == "TWO_PREDICATES_AWAY":
            two_predicates_away += 1
        if bucket in {"ONE_PREDICATE_AWAY", "TWO_PREDICATES_AWAY"}:
            excursion = _future_excursion(
                row=row,
                candles=candle_index.get(str(meta["instrument"]), []),
                horizon_bars=future_horizon_bars,
            )
            for predicate in failed_primitive_names:
                excursion_by_predicate[predicate].append(excursion)
            near_examples.append(
                {
                    "decision_bar_timestamp": row["decision_bar_timestamp"],
                    "instrument": meta["instrument"],
                    "strategy_id": strategy_id,
                    "side": meta["side"],
                    "near_miss_bucket": bucket,
                    "failed_primitive_predicates": failed_primitive_names,
                    "failed_rule_predicates": row["failed_rule_predicates"],
                    "numeric_distances": _distance_rows(failed_primitives),
                    "session_bucket": _session_bucket(state),
                    "regime_bucket": _regime_bucket(row),
                    "future_excursion": excursion,
                    "source_event_path": row.get("input_event_path"),
                }
            )
        if bucket not in {"NOT_SCORABLE", "SIGNAL"} and failed_primitives:
            excursion = _future_excursion(
                row=row,
                candles=candle_index.get(str(meta["instrument"]), []),
                horizon_bars=future_horizon_bars,
            )
            closest_failed_examples.append(
                {
                    "decision_bar_timestamp": row["decision_bar_timestamp"],
                    "instrument": meta["instrument"],
                    "strategy_id": strategy_id,
                    "side": meta["side"],
                    "primitive_failed_predicates_count": len(failed_primitives),
                    "failed_primitive_predicates": failed_primitive_names,
                    "numeric_distances": _distance_rows(failed_primitives),
                    "aggregate_negative_pass_margin": _decimal_str(_aggregate_negative_margin(failed_primitives)),
                    "session_bucket": _session_bucket(state),
                    "regime_bucket": _regime_bucket(row),
                    "future_excursion": excursion,
                    "source_event_path": row.get("input_event_path"),
                }
            )
        _ = features
    closest_failed_examples.sort(
        key=lambda item: (
            int(item.get("primitive_failed_predicates_count") or 999),
            _decimal(item.get("aggregate_negative_pass_margin")).copy_abs(),
            str(item.get("decision_bar_timestamp") or ""),
        )
    )
    eligible_count = int(eligibility_counts.get("eligible", 0))
    frequency = _expected_frequency_validation(
        strategy_id=strategy_id,
        hard_signals=hard_signals,
        eligible_bars=eligible_count,
        evaluated_bars=len(rows),
        not_scorable=not_scorable,
        rows=rows,
    )
    return {
        "strategy_id": strategy_id,
        "instrument": meta["instrument"],
        "side": meta["side"],
        "evaluated_completed_bars_total": len(rows),
        "eligible_completed_bars_evaluated": eligible_count,
        "denominator_validation": {
            "evaluated_bars": int(eligibility_counts.get("evaluated", 0)),
            "instrument_match_bars": int(eligibility_counts.get("instrument_match", 0)),
            "data_ready_bars": int(eligibility_counts.get("data_ready", 0)),
            "session_phase_ready_bars": int(eligibility_counts.get("session_phase_ready", 0)),
            "eligible_bars": eligible_count,
            "ineligible_reason_counts": _ineligible_reason_counts(rows, expected_instrument=str(meta["instrument"])),
        },
        "hard_signals": hard_signals,
        "hard_signal_rate_total_evaluated": _rate(hard_signals, len(rows)),
        "hard_signal_rate_eligible": _rate(hard_signals, eligible_count),
        "no_signal": no_signal,
        "not_ready": not_ready,
        "one_predicate_away": one_predicate_away,
        "two_predicates_away": two_predicates_away,
        "not_scorable": not_scorable,
        "dominant_failed_rule_predicates": _counter_rows(failed_rule_counts, limit=12),
        "dominant_failed_primitive_predicates": _primitive_rows(
            failed_primitive_counts,
            primitive_classifications=primitive_classifications,
            numeric_distances=numeric_distances,
            excursion_by_predicate=excursion_by_predicate,
            limit=12,
        ),
        "session_regime_buckets": {
            "sessions": _counter_rows(session_buckets, limit=8),
            "regimes": _counter_rows(regime_buckets, limit=8),
        },
        "near_miss_examples": near_examples[:max_examples],
        "closest_failed_bars": closest_failed_examples[:max_examples],
        "expected_frequency_validation": frequency,
        "frequency_classification": frequency["classification"],
        "amplification_posture": _strategy_posture(
            hard_signals=hard_signals,
            one_predicate_away=one_predicate_away,
            two_predicates_away=two_predicates_away,
            failed_primitives=failed_primitive_counts,
            frequency_classification=str(frequency["classification"]),
        ),
    }


def _reconstruct_snap_turn_rows_for_instrument(
    *,
    repo_root: Path,
    instrument: str,
    candles: list[dict[str, Any]],
    config: TrackBSnapTurnReplayBackfillConfig,
    now: datetime,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    blocked: Counter[str] = Counter()
    windows = _replay_windows(candles, min_window_bars=config.min_window_bars, max_windows=config.max_windows_per_instrument)
    for index, window in enumerate(windows):
        payload = _replay_payload(instrument=instrument, candles=window, now=now)
        result = produce_track_b_snap_turn_envelopes(
            runtime_5m_payload=payload,
            runtime_5m_payload_path=None,
            source_id="track_b_snap_turn_replay_backfill",
            output_root=_resolve(repo_root, config.replay_envelope_root),
            min_completed_bars=config.min_window_bars,
            max_completed_5m_age_seconds=None,
            now=now,
            producer_id=f"track_b_snap_turn_replay_{instrument.lower()}_{index:05d}",
        )
        if "WROTE_ENVELOPES" not in result.verdict.value:
            blocked.update([str(result.report.get("primary_blocker") or result.verdict.value)])
            continue
        for event in (result.first_bull_snap_turn_event, result.first_bear_snap_turn_event):
            if not isinstance(event, Mapping):
                continue
            rows.append(_row_from_replay_event(event))
    return rows, {
        "instrument": instrument,
        "input_candle_count": len(candles),
        "replay_windows_attempted": len(windows),
        "strategy_rows_reconstructed": len(rows),
        "blocked_window_reasons": _counter_rows(blocked, limit=8),
        "first_candle_timestamp": candles[0]["timestamp"].isoformat() if candles else None,
        "last_candle_timestamp": candles[-1]["timestamp"].isoformat() if candles else None,
    }


def _row_from_replay_event(event: Mapping[str, Any]) -> dict[str, Any]:
    strategy_id = str(event.get("strategy_id") or "")
    meta = SNAP_TURN_STRATEGIES[strategy_id]
    raw_metadata = event.get("metadata")
    metadata = raw_metadata if isinstance(raw_metadata, Mapping) else {}
    raw_state = metadata.get(meta["state_key"])
    raw_features = metadata.get(meta["features_key"])
    raw_diagnostics = metadata.get("feature_diagnostics")
    state = raw_state if isinstance(raw_state, Mapping) else {}
    features = raw_features if isinstance(raw_features, Mapping) else {}
    diagnostics = raw_diagnostics if isinstance(raw_diagnostics, Mapping) else {}
    signal_key = "first_bull_snap_turn" if meta["prefix"] == "bull" else "first_bear_snap_turn"
    hard_signal = features.get(signal_key) is True
    primitive = _primitive_predicates(
        {
            "strategy_id": strategy_id,
            "state": dict(state),
            "features": dict(features),
            "diagnostics": dict(diagnostics),
            "event": dict(event),
        }
    )
    return {
        "strategy_id": strategy_id,
        "result": "SIGNAL" if hard_signal else "NO_SIGNAL",
        "decision_bar_timestamp": _event_timestamp(event),
        "generated_at": event.get("generated_at"),
        "state": dict(state),
        "features": dict(features),
        "diagnostics": dict(diagnostics),
        "event": dict(event),
        "failed_rule_predicates": [str(item.get("predicate")) for item in primitive if item.get("passed") is not True],
        "rule_conditions": {str(item.get("predicate")): item.get("passed") is True for item in primitive},
        "input_event_path": None,
        "ohlc": _ohlc(event),
    }


def _load_replay_candles(
    *,
    repo_root: Path,
    paths: Iterable[Path],
    instrument: str,
    retained_5m_context_root: Path,
) -> list[dict[str, Any]]:
    by_timestamp: dict[datetime, dict[str, Any]] = {}
    for path in _replay_candidate_paths(repo_root=repo_root, paths=paths, retained_5m_context_root=retained_5m_context_root):
        payload = _load_json(path)
        payload_instrument = str(payload.get("instrument_family") or payload.get("symbol") or instrument)
        if payload_instrument != instrument:
            continue
        for candle in _candles_from_payload(payload):
            timestamp = candle.get("timestamp")
            if isinstance(timestamp, datetime):
                by_timestamp[timestamp] = candle
    return [by_timestamp[key] for key in sorted(by_timestamp)]


def _replay_candidate_paths(*, repo_root: Path, paths: Iterable[Path], retained_5m_context_root: Path) -> list[Path]:
    candidate_paths = [_resolve(repo_root, path) for path in paths]
    retained_root = _resolve(repo_root, retained_5m_context_root)
    if retained_root.exists():
        candidate_paths.extend(sorted(retained_root.glob("**/asian_drift_5m_candles.json")))
    deduped: dict[str, Path] = {}
    for path in candidate_paths:
        deduped[str(path)] = path
    return list(deduped.values())


def _replay_source_file_summary(
    *,
    repo_root: Path,
    paths: Iterable[Path],
    instrument: str,
    retained_5m_context_root: Path,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in _replay_candidate_paths(repo_root=repo_root, paths=paths, retained_5m_context_root=retained_5m_context_root):
        payload = _load_json(path)
        payload_instrument = str(payload.get("instrument_family") or payload.get("symbol") or instrument)
        if payload_instrument != instrument:
            continue
        candles = _candles_from_payload(payload)
        if not candles:
            continue
        timestamps = [item["timestamp"] for item in candles if isinstance(item.get("timestamp"), datetime)]
        rows.append(
            {
                "path": _display_path(repo_root, path),
                "instrument": instrument,
                "candle_count": len(candles),
                "first_timestamp": min(timestamps).isoformat() if timestamps else None,
                "last_timestamp": max(timestamps).isoformat() if timestamps else None,
            }
        )
    rows.sort(key=lambda item: (str(item.get("instrument") or ""), str(item.get("path") or "")))
    return rows


def _display_path(repo_root: Path, path: Path) -> str:
    try:
        return str(path.relative_to(repo_root))
    except ValueError:
        return str(path)


def _sample_frame_from_rows(
    *,
    rows: Iterable[Mapping[str, Any]],
    source_data_files: Iterable[Mapping[str, Any]],
    sample_source_type: str,
    sample_unit: str,
    missing_limitations: Iterable[str],
    preferred_classification: str | None,
    instrument_fallback: str | None = None,
) -> dict[str, Any]:
    row_list = [item for item in rows if isinstance(item, Mapping)]
    source_list = [dict(item) for item in source_data_files if isinstance(item, Mapping)]
    by_instrument: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in row_list:
        instrument = _row_instrument(row, fallback=instrument_fallback)
        if instrument:
            by_instrument[instrument].append(row)
    represented_instruments = set(by_instrument)
    if represented_instruments:
        source_list = [
            item for item in source_list if str(item.get("instrument") or "") in represented_instruments
        ]
    instrument_frames: dict[str, Any] = {}
    all_timestamps: list[datetime] = []
    all_sessions: Counter[str] = Counter()
    all_session_days: set[str] = set()
    for instrument, instrument_rows in sorted(by_instrument.items()):
        timestamps = [_row_timestamp(row) for row in instrument_rows]
        valid_timestamps = [item for item in timestamps if item is not None]
        all_timestamps.extend(valid_timestamps)
        sessions = Counter(_row_session(row) for row in instrument_rows)
        sessions.pop("", None)
        all_sessions.update(sessions)
        session_days = {item.date().isoformat() for item in valid_timestamps}
        all_session_days.update(session_days)
        bars_by_session = [
            {
                "session": session,
                "completed_5m_bars": _unique_bar_count(
                    row for row in instrument_rows if _row_session(row) == session
                ),
            }
            for session in sorted(sessions)
        ]
        instrument_frames[instrument] = {
            "instrument": instrument,
            "start_timestamp": min(valid_timestamps).isoformat() if valid_timestamps else None,
            "end_timestamp": max(valid_timestamps).isoformat() if valid_timestamps else None,
            "calendar_days": _calendar_days(valid_timestamps),
            "trading_session_days": len(session_days),
            "sessions_represented": sorted(sessions),
            "completed_5m_bars_total": _unique_bar_count(instrument_rows),
            "completed_5m_bars_by_session": bars_by_session,
        }
    lookback_classification = preferred_classification or _lookback_classification(
        timestamps=all_timestamps,
        trading_session_days=len(all_session_days),
        sessions=all_sessions,
        source_type=sample_source_type,
    )
    return {
        "sample_source_type": sample_source_type,
        "sample_unit": sample_unit,
        "lookback_classification": lookback_classification,
        "source_data_files": source_list,
        "start_timestamp": min(all_timestamps).isoformat() if all_timestamps else None,
        "end_timestamp": max(all_timestamps).isoformat() if all_timestamps else None,
        "calendar_days": _calendar_days(all_timestamps),
        "trading_session_days": len(all_session_days),
        "sessions_represented": sorted(all_sessions),
        "completed_5m_bars_by_instrument_session": instrument_frames,
        "source_data_file_count": len(source_list),
        "missing_windows_or_rotated_data_limitations": list(missing_limitations),
    }


def _row_instrument(row: Mapping[str, Any], *, fallback: str | None) -> str | None:
    direct = row.get("instrument")
    if direct:
        return str(direct)
    strategy_id = str(row.get("strategy_id") or "")
    if strategy_id in SNAP_TURN_STRATEGIES:
        return SNAP_TURN_STRATEGIES[strategy_id]["instrument"]
    return fallback


def _row_timestamp(row: Mapping[str, Any]) -> datetime | None:
    return _parse_dt(str(row.get("decision_bar_timestamp") or row.get("timestamp") or ""))


def _row_session(row: Mapping[str, Any]) -> str:
    if row.get("session"):
        return str(row.get("session"))
    state = row.get("state") if isinstance(row.get("state") or {}, Mapping) else {}
    return _session_bucket(state)


def _unique_bar_count(rows: Iterable[Mapping[str, Any]]) -> int:
    return len({str(_row_timestamp(row) or row.get("decision_bar_timestamp") or row.get("timestamp") or "") for row in rows})


def _calendar_days(timestamps: Iterable[datetime]) -> int:
    valid = list(timestamps)
    if not valid:
        return 0
    return (max(valid).date() - min(valid).date()).days + 1


def _lookback_classification(
    *,
    timestamps: list[datetime],
    trading_session_days: int,
    sessions: Counter[str],
    source_type: str,
) -> str:
    if not timestamps:
        return "INSUFFICIENT_LOOKBACK"
    if "HOLDOUT" in source_type:
        return "HOLDOUT_REPLAY"
    if trading_session_days <= 1:
        return "SINGLE_WINDOW_DIAGNOSTIC"
    if "RETAINED_ROLLING" in source_type:
        return "MULTI_SESSION_SAMPLE"
    if trading_session_days > 1 and len(sessions) > 1:
        return "MULTI_DAY_REPLAY"
    return "MULTI_SESSION_SAMPLE"


def _replay_missing_limitations(
    *,
    reconstruction: Mapping[str, Any],
    source_files: Mapping[str, list[Mapping[str, Any]]],
) -> list[str]:
    limitations: list[str] = []
    for instrument, summary in reconstruction.items():
        if not source_files.get(str(instrument)):
            limitations.append(f"{instrument}: no source candle files contributed rows")
        for blocker in summary.get("blocked_window_reasons") or []:
            if isinstance(blocker, Mapping):
                limitations.append(f"{instrument}: blocked windows {blocker.get('reason')} count={blocker.get('count')}")
        attempted = int(summary.get("replay_windows_attempted") or 0)
        source_count = int(summary.get("input_candle_count") or 0)
        if attempted and source_count > attempted:
            limitations.append(
                f"{instrument}: replay uses rolling suffix of {attempted} windows from {source_count} retained candles"
            )
    return limitations


def _sample_frame_limitations_from_parent(parent_report: Mapping[str, Any]) -> list[str]:
    raw_sample_frame = parent_report.get("sample_frame")
    sample_frame = raw_sample_frame if isinstance(raw_sample_frame, Mapping) else {}
    limitations = sample_frame.get("missing_windows_or_rotated_data_limitations")
    return [str(item) for item in limitations] if isinstance(limitations, list) else []


def _candles_from_payload(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get("candles") or payload.get("candle_history") or payload.get("completed_5m_candles") or []
    if not isinstance(raw, list):
        return []
    candles: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        timeframe = str(item.get("timeframe") or payload.get("timeframe") or payload.get("source_timeframe") or "5m")
        if timeframe != "5m":
            continue
        timestamp = _parse_dt(
            str(
                item.get("candle_timestamp")
                or item.get("timestamp")
                or item.get("observed_at")
                or item.get("source_end_timestamp")
                or ""
            )
        )
        if timestamp is None:
            continue
        candles.append(
            {
                "timestamp": timestamp,
                "candle_timestamp": timestamp.isoformat(),
                "open": str(item.get("open")),
                "high": str(item.get("high")),
                "low": str(item.get("low")),
                "close": str(item.get("close")),
                "volume": str(item.get("volume") or "0"),
                "completed": True,
                "timeframe": "5m",
            }
        )
    return candles


def _replay_windows(
    candles: list[dict[str, Any]],
    *,
    min_window_bars: int,
    max_windows: int,
) -> list[list[dict[str, Any]]]:
    windows = [candles[index - min_window_bars + 1 : index + 1] for index in range(min_window_bars - 1, len(candles))]
    return windows[-max_windows:]


def _replay_payload(*, instrument: str, candles: list[dict[str, Any]], now: datetime) -> dict[str, Any]:
    is_mnq = instrument == "MNQ"
    return {
        "schema_version": "track_b_snap_turn_replay_backfill_input_v1",
        "source_id": "track_b_snap_turn_replay_backfill",
        "diagnostic_replay_only": True,
        "instrument_family": instrument,
        "symbol": instrument,
        "contract_key": "MNQ-202606" if is_mnq else "MGC-202606",
        "local_symbol": "MNQM6" if is_mnq else "MGCM6",
        "dataset": "GLBX.MDP3",
        "timeframe": "5m",
        "quote_provider_mode": "REALTIME",
        "realtime_quote_received": True,
        "current_quote_available": True,
        "quote_freshness_verdict": "REPLAY_BACKFILL_DIAGNOSTIC_ONLY",
        "generated_at": now.isoformat(),
        "candles": [
            {
                "candle_timestamp": item["timestamp"].isoformat(),
                "timestamp": item["timestamp"].isoformat(),
                "open": item["open"],
                "high": item["high"],
                "low": item["low"],
                "close": item["close"],
                "volume": item.get("volume"),
                "completed": True,
                "timeframe": "5m",
            }
            for item in candles
        ],
        "submit_allowed": False,
        "submit_attempted": False,
        "broker_state_mutated": False,
        "live_money_readiness": False,
    }


def _evaluated_snap_turn_rows(runtime_reports: Iterable[Mapping[str, Any]], *, repo_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for runtime in runtime_reports:
        generated_at = str(runtime.get("generated_at") or "")
        for strategy in runtime.get("evaluated_strategies") or []:
            if not isinstance(strategy, Mapping):
                continue
            strategy_id = _strategy_id(strategy)
            if strategy_id not in SNAP_TURN_STRATEGIES:
                continue
            event_path = strategy.get("input_event_path")
            event = _load_json(_resolve(repo_root, Path(str(event_path)))) if event_path else {}
            raw_metadata = event.get("metadata")
            metadata = raw_metadata if isinstance(raw_metadata, Mapping) else {}
            meta = SNAP_TURN_STRATEGIES[strategy_id]
            raw_state = metadata.get(meta["state_key"])
            raw_features = metadata.get(meta["features_key"])
            raw_diagnostics = metadata.get("feature_diagnostics")
            state = raw_state if isinstance(raw_state, Mapping) else {}
            features = raw_features if isinstance(raw_features, Mapping) else {}
            diagnostics = raw_diagnostics if isinstance(raw_diagnostics, Mapping) else {}
            rows.append(
                {
                    "strategy_id": strategy_id,
                    "result": _strategy_result(strategy),
                    "decision_bar_timestamp": _event_timestamp(event) or generated_at,
                    "generated_at": generated_at,
                    "state": dict(state),
                    "features": dict(features),
                    "diagnostics": dict(diagnostics),
                    "event": dict(event),
                    "failed_rule_predicates": _failed_rule_predicates(strategy),
                    "rule_conditions": dict(strategy.get("rule_conditions") or {}),
                    "input_event_path": str(event_path) if event_path else None,
                    "ohlc": _ohlc(event),
                }
            )
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row.get("strategy_id") or ""), str(row.get("decision_bar_timestamp") or ""))
        existing = deduped.get(key)
        if existing is None or _prefer_snap_row(row, existing):
            deduped[key] = row
    final_rows = list(deduped.values())
    final_rows.sort(key=lambda item: (str(item.get("decision_bar_timestamp") or ""), str(item.get("strategy_id") or "")))
    return final_rows


def _prefer_snap_row(candidate: Mapping[str, Any], existing: Mapping[str, Any]) -> bool:
    candidate_signal = candidate.get("result") == "SIGNAL"
    existing_signal = existing.get("result") == "SIGNAL"
    if candidate_signal != existing_signal:
        return candidate_signal
    candidate_ready = bool(candidate.get("state")) and bool(candidate.get("features"))
    existing_ready = bool(existing.get("state")) and bool(existing.get("features"))
    if candidate_ready != existing_ready:
        return candidate_ready
    candidate_generated = _parse_dt(str(candidate.get("generated_at") or ""))
    existing_generated = _parse_dt(str(existing.get("generated_at") or ""))
    if candidate_generated and existing_generated:
        return candidate_generated > existing_generated
    return False


def _primitive_predicates(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    strategy_id = str(row.get("strategy_id") or "")
    meta = SNAP_TURN_STRATEGIES[strategy_id]
    prefix = meta["prefix"]
    state = row.get("state") if isinstance(row.get("state") or {}, Mapping) else {}
    features = row.get("features") if isinstance(row.get("features") or {}, Mapping) else {}
    event = row.get("event") if isinstance(row.get("event") or {}, Mapping) else {}
    diagnostics = row.get("diagnostics") if isinstance(row.get("diagnostics") or {}, Mapping) else {}
    if not state or not features:
        return [
            {
                "predicate": "snap_turn_feature_envelope_missing",
                "passed": False,
                "actual": None,
                "threshold": "state_and_features_present",
                "distance": None,
            }
        ]
    if prefix == "bull":
        return [
            _bool_predicate("session_allowed", state.get("session_allowed")),
            _bool_predicate("prior_bars_since_bull_snap_gt_cooldown", state.get("prior_bars_since_bull_snap_gt_cooldown")),
            _distance_predicate(
                "bull_snap_downside_stretch_ok",
                features.get("bull_snap_downside_stretch_ok"),
                _decimal(diagnostics.get("downside_stretch")),
                _decimal(features.get("bull_snap_min_downside_stretch_atr")) * _decimal(diagnostics.get("atr")),
                mode="min",
            ),
            _distance_predicate(
                "bull_snap_range_ok",
                features.get("bull_snap_range_ok"),
                _decimal(diagnostics.get("bar_range")),
                _decimal(features.get("bull_snap_range_threshold_atr")) * _decimal(diagnostics.get("atr")),
                mode="min",
            ),
            _distance_predicate(
                "bull_snap_body_ok",
                features.get("bull_snap_body_ok"),
                _decimal(diagnostics.get("body_size")),
                _decimal(features.get("bull_snap_body_threshold_atr")) * _decimal(diagnostics.get("atr")),
                mode="min",
            ),
            _distance_predicate(
                "bull_snap_close_strong",
                features.get("bull_snap_close_strong"),
                _decimal(diagnostics.get("close_location")),
                Decimal("0.72"),
                mode="min",
            ),
            _distance_predicate(
                "bull_snap_velocity_ok",
                features.get("bull_snap_velocity_ok"),
                _decimal(diagnostics.get("velocity_delta")),
                _decimal(features.get("bull_snap_velocity_threshold_atr")) * _decimal(diagnostics.get("atr")),
                mode="min",
            ),
            _distance_predicate(
                "bull_snap_location_ok",
                features.get("bull_snap_location_ok"),
                _decimal(event.get("close")),
                _decimal(diagnostics.get("turn_ema_slow")),
                mode="max",
            ),
            _distance_predicate(
                "bull_close_above_open",
                features.get("bull_snap_reversal_bar"),
                _decimal(event.get("close")),
                _decimal(event.get("open")),
                mode="min",
            ),
        ]
    return [
        _bool_predicate("session_allowed", state.get("session_allowed")),
        _bool_predicate("prior_bars_since_bear_snap_gt_cooldown", state.get("prior_bars_since_bear_snap_gt_cooldown")),
        _distance_predicate(
            "bear_snap_up_stretch_ok",
            features.get("bear_snap_up_stretch_ok"),
            _decimal(diagnostics.get("upside_stretch")),
            _decimal(features.get("bear_snap_min_upside_stretch_atr")) * _decimal(diagnostics.get("atr")),
            mode="min",
        ),
        _distance_predicate(
            "bear_snap_range_ok",
            features.get("bear_snap_range_ok"),
            _decimal(diagnostics.get("bar_range")),
            _decimal(features.get("bear_snap_range_threshold_atr")) * _decimal(diagnostics.get("atr")),
            mode="min",
        ),
        _distance_predicate(
            "bear_snap_body_ok",
            features.get("bear_snap_body_ok"),
            _decimal(diagnostics.get("body_size")),
            _decimal(features.get("bear_snap_body_threshold_atr")) * _decimal(diagnostics.get("atr")),
            mode="min",
        ),
        _distance_predicate(
            "bear_snap_close_weak",
            features.get("bear_snap_close_weak"),
            _decimal(diagnostics.get("close_location")),
            Decimal("0.28"),
            mode="max",
        ),
        _distance_predicate(
            "bear_snap_velocity_ok",
            features.get("bear_snap_velocity_ok"),
            _decimal(diagnostics.get("velocity_delta")),
            -(_decimal(features.get("bear_snap_velocity_threshold_atr")) * _decimal(diagnostics.get("atr"))),
            mode="max",
        ),
        _distance_predicate(
            "bear_snap_location_ok",
            features.get("bear_snap_location_ok"),
            _decimal(event.get("close")),
            _decimal(diagnostics.get("turn_ema_slow")),
            mode="min",
        ),
        _distance_predicate(
            "bear_close_below_open",
            features.get("bear_snap_reversal_bar"),
            _decimal(event.get("close")),
            _decimal(event.get("open")),
            mode="max",
        ),
    ]


def _future_excursion(*, row: Mapping[str, Any], candles: list[dict[str, Any]], horizon_bars: int) -> dict[str, Any]:
    timestamp = _parse_dt(str(row.get("decision_bar_timestamp") or ""))
    if timestamp is None:
        return {"available": False, "reason": "decision_bar_timestamp_unparseable"}
    current_close = _decimal((row.get("ohlc") or {}).get("close"))
    if current_close == 0:
        return {"available": False, "reason": "current_close_unavailable"}
    strategy_id = str(row.get("strategy_id") or "")
    side = SNAP_TURN_STRATEGIES[strategy_id]["side"]
    future = [item for item in candles if item["timestamp"] > timestamp][:horizon_bars]
    if not future:
        return {"available": False, "reason": "future_bars_unavailable"}
    highs = [_decimal(item.get("high")) for item in future]
    lows = [_decimal(item.get("low")) for item in future]
    close_n = _decimal(future[-1].get("close"))
    if side == "LONG":
        mfe = max(highs) - current_close
        mae = min(lows) - current_close
        directional_close_excursion = close_n - current_close
    else:
        mfe = current_close - min(lows)
        mae = current_close - max(highs)
        directional_close_excursion = current_close - close_n
    return {
        "available": True,
        "bars_available": len(future),
        "horizon_bars": horizon_bars,
        "mfe_points": _decimal_str(mfe),
        "mae_points": _decimal_str(mae),
        "directional_close_excursion_points": _decimal_str(directional_close_excursion),
        "latest_future_bar_timestamp": future[-1]["timestamp"].isoformat(),
    }


def _scorable_snapshots(
    *,
    rows: Iterable[Mapping[str, Any]],
    candle_index: Mapping[str, list[dict[str, Any]]],
    future_horizon_bars: int,
) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []
    for row in rows:
        strategy_id = str(row.get("strategy_id") or "")
        if strategy_id not in SNAP_TURN_STRATEGIES:
            continue
        meta = SNAP_TURN_STRATEGIES[strategy_id]
        eligibility = _eligibility(row, expected_instrument=str(meta["instrument"]))
        primitives = _primitive_predicates(row)
        primitive_rows = [
            {
                "predicate": item.get("predicate"),
                "passed": item.get("passed"),
                "actual": _json_value(item.get("actual")),
                "threshold": _json_value(item.get("threshold")),
                "pass_margin": _json_value(item.get("distance")),
                "numeric_distance_available": isinstance(item.get("distance"), Decimal),
                "classification": _predicate_classification(str(item.get("predicate") or ""), row),
            }
            for item in primitives
        ]
        failed_primitives = [item for item in primitive_rows if item.get("passed") is not True]
        hard_signal = row.get("result") == "SIGNAL"
        future = _future_excursion(
            row=row,
            candles=candle_index.get(str(meta["instrument"]), []),
            horizon_bars=future_horizon_bars,
        )
        envelope_available = bool(row.get("state")) and bool(row.get("features"))
        snapshots.append(
            {
                "snapshot_schema_version": "track_b_snap_turn_scorable_snapshot_v1",
                "decision_bar_timestamp": row.get("decision_bar_timestamp"),
                "instrument": meta["instrument"],
                "strategy_id": strategy_id,
                "side": meta["side"],
                "bar_ohlc": row.get("ohlc") or {},
                "proposed_entry_price": (row.get("ohlc") or {}).get("close"),
                "session": _session_bucket(row.get("state") if isinstance(row.get("state") or {}, Mapping) else {}),
                "regime": _regime_bucket(row),
                "eligibility": eligibility,
                "feature_envelope_available": envelope_available,
                "feature_envelope_missing_is_product_defect": not envelope_available,
                "hard_signal": hard_signal,
                "result": row.get("result"),
                "primitive_predicates": primitive_rows,
                "primitive_failed_predicates_count": len(failed_primitives),
                "failed_primitive_predicates": [item.get("predicate") for item in failed_primitives],
                "failed_rule_predicates": row.get("failed_rule_predicates") or [],
                "no_signal_reason": None
                if hard_signal
                else _no_signal_reason(row.get("failed_rule_predicates") or [], failed_primitives, envelope_available),
                "near_miss_bucket": (
                    "NOT_SCORABLE"
                    if not envelope_available
                    else _near_miss_bucket(len(failed_primitives), result=str(row.get("result") or "NO_SIGNAL"))
                ),
                "numeric_distance_available": any(item.get("numeric_distance_available") is True for item in primitive_rows),
                "future_excursion": future,
                "input_event_path": row.get("input_event_path"),
            }
        )
    return snapshots


def _write_scorable_snapshots(
    *,
    repo_root: Path,
    jsonl_path: Path,
    latest_json_path: Path,
    snapshots: list[dict[str, Any]],
    generated_at: datetime,
) -> dict[str, Any]:
    actual_jsonl = _resolve(repo_root, jsonl_path)
    actual_latest = _resolve(repo_root, latest_json_path)
    actual_jsonl.parent.mkdir(parents=True, exist_ok=True)
    existing = actual_jsonl.read_text(encoding="utf-8").splitlines() if actual_jsonl.exists() else []
    existing_keys: set[str] = set()
    for line in existing:
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, Mapping):
            existing_keys.add(_snapshot_key(payload))
    appended = 0
    with actual_jsonl.open("a", encoding="utf-8") as handle:
        for snapshot in snapshots:
            key = _snapshot_key(snapshot)
            if key in existing_keys:
                continue
            handle.write(json.dumps(to_jsonable(snapshot), sort_keys=True) + "\n")
            appended += 1
            existing_keys.add(key)
    missing_envelopes = [item for item in snapshots if item.get("feature_envelope_available") is not True]
    numeric_available = [item for item in snapshots if item.get("numeric_distance_available") is True]
    latest_payload = {
        "schema_version": "track_b_snap_turn_scorable_snapshots_latest_v1",
        "generated_at": generated_at.isoformat(),
        "source": "TRACK_B_COMPLETED_DECISION_BAR_RUNTIME_ARTIFACTS",
        "snapshot_count": len(snapshots),
        "appended_snapshot_count": appended,
        "jsonl_path": str(jsonl_path),
        "feature_envelope_missing_count": len(missing_envelopes),
        "feature_envelope_missing_is_product_defect": bool(missing_envelopes),
        "numeric_distance_available_count": len(numeric_available),
        "snapshots": snapshots,
    }
    _write_json(actual_latest, latest_payload)
    return {
        "status": "EVIDENCE_RETENTION_REPAIRED",
        "jsonl_path": str(jsonl_path),
        "latest_json_path": str(latest_json_path),
        "snapshot_count": len(snapshots),
        "appended_snapshot_count": appended,
        "feature_envelope_missing_count": len(missing_envelopes),
        "feature_envelope_missing_is_product_defect": bool(missing_envelopes),
        "numeric_distance_available_count": len(numeric_available),
        "replay_backfill_required": bool(missing_envelopes) or len(numeric_available) < len(snapshots),
    }


def _snapshot_key(snapshot: Mapping[str, Any]) -> str:
    return "|".join(
        [
            str(snapshot.get("decision_bar_timestamp") or ""),
            str(snapshot.get("instrument") or ""),
            str(snapshot.get("strategy_id") or ""),
        ]
    )


def _no_signal_reason(failed_rule_predicates: Iterable[Any], failed_primitives: Iterable[Mapping[str, Any]], envelope_available: bool) -> str:
    if not envelope_available:
        return "FEATURE_ENVELOPE_MISSING_REPLAY_BACKFILL_REQUIRED"
    primitive_names = [str(item.get("predicate") or "") for item in failed_primitives]
    if primitive_names:
        return "FAILED_PRIMITIVE_PREDICATES: " + ", ".join(primitive_names)
    failed_rule_names = [str(item) for item in failed_rule_predicates]
    if failed_rule_names:
        return "FAILED_RULE_PREDICATES: " + ", ".join(failed_rule_names)
    return "NO_SIGNAL_REASON_NOT_PROVIDED"


def _eligibility(row: Mapping[str, Any], *, expected_instrument: str) -> dict[str, Any]:
    event = row.get("event") if isinstance(row.get("event") or {}, Mapping) else {}
    state = row.get("state") if isinstance(row.get("state") or {}, Mapping) else {}
    features = row.get("features") if isinstance(row.get("features") or {}, Mapping) else {}
    instrument = _event_instrument(event)
    instrument_match = instrument == expected_instrument
    data_ready = bool(state and features)
    session_phase_ready = data_ready and state.get("session_allowed") is True
    result = str(row.get("result") or "")
    eligible = instrument_match and data_ready and session_phase_ready and result != "NOT_READY"
    reasons: list[str] = []
    if not instrument_match:
        reasons.append("INSTRUMENT_MISMATCH_OR_MISSING")
    if not data_ready:
        reasons.append("FEATURE_ENVELOPE_MISSING")
    if data_ready and not session_phase_ready:
        reasons.append("SESSION_OR_PHASE_FILTER_INACTIVE")
    if result == "NOT_READY":
        reasons.append("NOT_READY")
    return {
        "eligible": eligible,
        "instrument_match": instrument_match,
        "data_ready": data_ready,
        "session_phase_ready": session_phase_ready,
        "ineligible_reasons": reasons,
    }


def _ineligible_reason_counts(rows: Iterable[Mapping[str, Any]], *, expected_instrument: str) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    for row in rows:
        eligibility = _eligibility(row, expected_instrument=expected_instrument)
        if eligibility["eligible"]:
            continue
        counter.update(eligibility["ineligible_reasons"])
    return _counter_rows(counter, limit=8)


def _expected_frequency_validation(
    *,
    strategy_id: str,
    hard_signals: int,
    eligible_bars: int,
    evaluated_bars: int,
    not_scorable: int,
    rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    expected = SNAP_TURN_EXPECTED_FREQUENCY[strategy_id]
    expected_eligible = expected["expected_eligible_bars_per_day"]
    expected_signals = expected["expected_hard_signals_per_day"]
    midpoint_eligible = Decimal(str(sum(expected_eligible) / 2))
    active_day_equivalents = Decimal(eligible_bars) / midpoint_eligible if midpoint_eligible else Decimal("0")
    observed_signals_per_active_day = (
        Decimal(hard_signals) / active_day_equivalents if active_day_equivalents > 0 else Decimal("0")
    )
    classification = "METHODOLOGY_INCONCLUSIVE"
    not_scorable_ratio = Decimal(not_scorable) / Decimal(evaluated_bars) if evaluated_bars > 0 else Decimal("0")
    if eligible_bars >= 20 and active_day_equivalents >= Decimal("0.25") and not_scorable_ratio <= Decimal("0.10"):
        if observed_signals_per_active_day < Decimal(str(expected_signals[0])):
            classification = "TOO_QUIET"
        elif observed_signals_per_active_day > Decimal(str(expected_signals[1])):
            classification = "TOO_ACTIVE"
        else:
            classification = "ACCEPTABLE"
    return {
        "expected_eligible_bars_per_day": expected_eligible,
        "expected_hard_signals_per_day": expected_signals,
        "expectation_source": expected["expectation_source"],
        "active_day_equivalents_from_eligible_bars": _decimal_str(active_day_equivalents),
        "observed_hard_signals_per_active_day": _decimal_str(observed_signals_per_active_day),
        "observed_hard_signal_rate_eligible": _rate(hard_signals, eligible_bars),
        "not_scorable_ratio": _decimal_str(not_scorable_ratio),
        "classification": classification,
        "methodology": (
            "Active-day equivalents are eligible_bars divided by midpoint expected eligible bars/day. "
            "Frequency classification requires at least 20 eligible bars, >=0.25 active-day equivalents, "
            "and <=10% not-scorable evaluated bars."
        ),
        "sample_window_start": _min_time(row.get("decision_bar_timestamp") for row in rows),
        "sample_window_end": _max_time(row.get("decision_bar_timestamp") for row in rows),
    }


def _aggregate_negative_margin(predicates: Iterable[Mapping[str, Any]]) -> Decimal:
    total = Decimal("0")
    for item in predicates:
        distance = item.get("distance")
        if isinstance(distance, Decimal) and distance < 0:
            total += distance
    return total


def _rate(numerator: int, denominator: int) -> str | None:
    if denominator <= 0:
        return None
    return format((Decimal(numerator) / Decimal(denominator)) * Decimal("100"), ".4f")


def _primitive_rows(
    counter: Counter[str],
    *,
    primitive_classifications: Mapping[str, Counter[str]],
    numeric_distances: Mapping[str, list[Decimal]],
    excursion_by_predicate: Mapping[str, list[dict[str, Any]]],
    limit: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for predicate, count in counter.most_common(limit):
        distances = numeric_distances.get(predicate) or []
        excursions = [item for item in excursion_by_predicate.get(predicate, []) if item.get("available") is True]
        avg_distance = sum(distances, Decimal("0")) / Decimal(len(distances)) if distances else None
        avg_mfe = _avg_decimal(item.get("mfe_points") for item in excursions)
        avg_mae = _avg_decimal(item.get("mae_points") for item in excursions)
        classification_counter = primitive_classifications.get(predicate) or Counter(["INCONCLUSIVE"])
        classification = classification_counter.most_common(1)[0][0]
        rows.append(
            {
                "predicate": predicate,
                "count": count,
                "classification": classification,
                "numeric_distance": {
                    "available": bool(distances),
                    "average_pass_margin_points_or_units": _decimal_str(avg_distance) if avg_distance is not None else None,
                    "negative_means_average_failure_distance": True,
                    "sample_count": len(distances),
                },
                "subsequent_excursion_after_near_misses": {
                    "sample_count": len(excursions),
                    "average_mfe_points": _decimal_str(avg_mfe) if avg_mfe is not None else None,
                    "average_mae_points": _decimal_str(avg_mae) if avg_mae is not None else None,
                },
            }
        )
    return rows


def _predicate_classification(predicate: str, row: Mapping[str, Any]) -> str:
    if "missing" in predicate:
        return "INCONCLUSIVE"
    if predicate == "session_allowed":
        return "INTENTIONAL_HARD_GATE"
    if "cooldown" in predicate:
        return "INTENTIONAL_HARD_GATE"
    if predicate.endswith("_location_ok") or predicate.endswith("_close_strong") or predicate.endswith("_close_weak"):
        return "VARIANT_CANDIDATE"
    if predicate.endswith("_range_ok") or predicate.endswith("_body_ok") or predicate.endswith("_velocity_ok"):
        return "VARIANT_CANDIDATE"
    if predicate.endswith("_stretch_ok"):
        return "VARIANT_CANDIDATE"
    if "above_open" in predicate or "below_open" in predicate:
        return "INTENTIONAL_HARD_GATE"
    _ = row
    return "INCONCLUSIVE"


def _variant_candidates(strategies: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for strategy in strategies:
        for predicate in strategy.get("dominant_failed_primitive_predicates") or []:
            if predicate.get("classification") != "VARIANT_CANDIDATE":
                continue
            sample_count = int((predicate.get("subsequent_excursion_after_near_misses") or {}).get("sample_count") or 0)
            if sample_count <= 0:
                continue
            avg_mfe = (predicate.get("subsequent_excursion_after_near_misses") or {}).get("average_mfe_points")
            avg_mae = (predicate.get("subsequent_excursion_after_near_misses") or {}).get("average_mae_points")
            if not _average_excursion_favorable(avg_mfe, avg_mae):
                continue
            candidates.append(
                {
                    "strategy_id": strategy.get("strategy_id"),
                    "instrument": strategy.get("instrument"),
                    "side": strategy.get("side"),
                    "predicate": predicate.get("predicate"),
                    "failed_count": predicate.get("count"),
                    "near_miss_excursion_sample_count": sample_count,
                    "average_mfe_points": avg_mfe,
                    "average_mae_points": avg_mae,
                    "next_step": (
                        "Replay a bounded candidate variant for this predicate before any threshold change."
                        if sample_count
                        else "Collect/replay near-miss excursion evidence before proposing a variant."
                    ),
                }
            )
        if strategy.get("frequency_classification") == "TOO_QUIET":
            for example in strategy.get("closest_failed_bars") or []:
                excursion = example.get("future_excursion") if isinstance(example.get("future_excursion") or {}, Mapping) else {}
                failed_count = int(example.get("primitive_failed_predicates_count") or 999)
                if failed_count > 4 or not _favorable_excursion(excursion):
                    continue
                candidates.append(
                    {
                        "strategy_id": strategy.get("strategy_id"),
                        "instrument": strategy.get("instrument"),
                        "side": strategy.get("side"),
                        "predicate": ",".join(str(item) for item in (example.get("failed_primitive_predicates") or [])[:4]),
                        "failed_count": failed_count,
                        "near_miss_excursion_sample_count": 1,
                        "average_mfe_points": excursion.get("mfe_points"),
                        "average_mae_points": excursion.get("mae_points"),
                        "decision_bar_timestamp": example.get("decision_bar_timestamp"),
                        "next_step": "Replay this closest rejected bar as a bounded snap-turn variant candidate before any threshold change.",
                    }
                )
    candidates.sort(
        key=lambda item: (
            -int(item.get("near_miss_excursion_sample_count") or 0),
            int(item.get("failed_count") or 999),
            str(item.get("strategy_id") or ""),
        )
    )
    return candidates[:3]


def _defects_or_parity_checks(strategies: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for strategy in strategies:
        for predicate in strategy.get("dominant_failed_primitive_predicates") or []:
            classification = predicate.get("classification")
            if classification in {"PARITY_DEFECT", "IMPLEMENTATION_DEFECT"}:
                checks.append(
                    {
                        "strategy_id": strategy.get("strategy_id"),
                        "predicate": predicate.get("predicate"),
                        "classification": classification,
                        "failed_count": predicate.get("count"),
                    }
                )
    return checks


def _conclusion(strategies: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    strategy_list = list(strategies)
    total_near = sum(int(item.get("one_predicate_away") or 0) + int(item.get("two_predicates_away") or 0) for item in strategy_list)
    total_signals = sum(int(item.get("hard_signals") or 0) for item in strategy_list)
    too_quiet = [item for item in strategy_list if item.get("frequency_classification") == "TOO_QUIET"]
    inconclusive = [item for item in strategy_list if item.get("frequency_classification") == "METHODOLOGY_INCONCLUSIVE"]
    variants = _variant_candidates(strategy_list)
    defects = _defects_or_parity_checks(strategy_list)
    if defects:
        posture = "SNAP_TURN_PARITY_OR_IMPLEMENTATION_DEFECT_FOUND"
        reason = "At least one predicate/feature path was classified as a parity or implementation defect."
    elif variants:
        posture = "SNAP_TURN_VARIANT_CANDIDATE_FOUND"
        reason = "Closest rejected bars or one/two-predicate near-misses have favorable subsequent excursion evidence."
    elif inconclusive:
        posture = "SNAP_TURN_AUDIT_METHODOLOGY_INCONCLUSIVE"
        reason = "At least one strategy lacks enough eligible/scorable denominator evidence."
    elif too_quiet:
        posture = "SNAP_TURN_TOO_QUIET_CONFIRMED"
        reason = "At least one snap-turn strategy is below the provisional expected-frequency contract on eligible bars."
    else:
        posture = "SNAP_TURN_HEALTHY"
        reason = "Observed hard-signal frequency is inside the provisional expected-frequency contract."
    return {
        "posture": posture,
        "hard_signals": total_signals,
        "near_miss_count": total_near,
        "too_quiet_strategy_count": len(too_quiet),
        "methodology_inconclusive_strategy_count": len(inconclusive),
        "reason": reason,
    }


def _output_classifications(
    strategies: Iterable[Mapping[str, Any]],
    snapshot_summary: Mapping[str, Any],
) -> list[str]:
    classifications = ["EVIDENCE_RETENTION_REPAIRED"]
    strategy_list = list(strategies)
    if snapshot_summary.get("replay_backfill_required") is True:
        classifications.append("REPLAY_BACKFILL_REQUIRED")
    if _variant_candidates(strategy_list):
        classifications.append("SNAP_TURN_IMPROVEMENT_CANDIDATE_FOUND")
    if any(item.get("frequency_classification") == "TOO_QUIET" for item in strategy_list):
        classifications.append("SNAP_TURN_TOO_QUIET_CONFIRMED")
    if not strategy_list or all(item.get("frequency_classification") == "METHODOLOGY_INCONCLUSIVE" for item in strategy_list):
        classifications.append("SNAP_TURN_INSUFFICIENT_EVIDENCE_BLOCKED")
    return classifications


def _improvement_methods_to_test() -> list[dict[str, Any]]:
    return [
        {
            "method": "remove_accidental_track_b_only_gates",
            "evidence_required": [
                "Track 1/reference snap-turn predicate list lacks the gate.",
                "Track B rejected bars pass all Track 1 predicates except the Track B-only gate.",
                "Subsequent MFE/MAE does not show systematic false positives.",
            ],
            "false_positive_risk": "May reintroduce churn or duplicate first-snap entries that Track B deliberately prevented.",
        },
        {
            "method": "simplify_over_specified_predicate_stack",
            "evidence_required": [
                "Closest failed bars repeatedly miss the same low-value predicate while passing core stretch/reversal/location logic.",
                "Replay of removed/simplified predicate improves opportunity capture without worse MAE tails.",
            ],
            "false_positive_risk": "Can turn a snap-turn strategy into generic reversal chasing.",
        },
        {
            "method": "session_window_adjustment",
            "evidence_required": [
                "Session/phase filter is dominant blocker during bars that otherwise look entry-capable.",
                "Track 1/research expected the wider session or replay validates the added window.",
            ],
            "false_positive_risk": "Adds trades in liquidity/behavior regimes the strategy was not designed for.",
        },
        {
            "method": "volatility_or_regime_specific_variant",
            "evidence_required": [
                "Failed threshold distances cluster by volatility/regime bucket.",
                "Variant replay improves MFE/MAE only in that bucket, not globally.",
            ],
            "false_positive_risk": "Overfits one volatility regime and degrades across normal active conditions.",
        },
        {
            "method": "closest_failed_bar_variant",
            "evidence_required": [
                "Closest failed bars are repeatedly commercially favorable after the decision bar.",
                "One/two/few-predicate failures are attributable and replayable.",
            ],
            "false_positive_risk": "Optimizes to hindsight if control windows and false-positive bars are not included.",
        },
        {
            "method": "separate_trend_continuation_coverage",
            "evidence_required": [
                "Rejected bars are trend-continuation regimes, not snap-turn regimes.",
                "Trend overlay detects the opportunity without weakening snap-turn semantics.",
            ],
            "false_positive_risk": "Forcing continuation into snap-turn can corrupt both strategy families.",
        },
    ]


def _immediate_replay_backfill_path(snapshot_summary: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "classification": "REPLAY_BACKFILL_REQUIRED"
        if snapshot_summary.get("replay_backfill_required") is True
        else "EVIDENCE_RETENTION_REPAIRED",
        "objective": "Reconstruct scorable snap-turn decision rows from retained completed 5m candles/features instead of waiting weeks.",
        "smallest_path": [
            "Use retained completed 5m candle payloads for MGC/MNQ decision windows.",
            "Re-run track_b_snap_turn_envelope_producer on each completed decision bar window.",
            "Persist the compact scorable snapshot before rotating full envelopes.",
            "Run this audit on the persisted snapshot JSONL, not on fragile latest envelope paths.",
        ],
        "required_inputs": [
            "completed 5m candle window with at least snap-turn feature lookback",
            "instrument family / local symbol / contract context",
            "prior snap cooldown state if available, otherwise classify cooldown distance as not provided",
        ],
        "current_blocker": "Historical event envelopes are rotated/missing for some retained runtime reports."
        if snapshot_summary.get("feature_envelope_missing_is_product_defect") is True
        else None,
    }


def _replay_classifications(strategies: Iterable[Mapping[str, Any]], snapshot_summary: Mapping[str, Any]) -> list[str]:
    strategy_list = list(strategies)
    if int(snapshot_summary.get("snapshot_count") or 0) <= 0:
        return ["REPLAY_BACKFILL_INSUFFICIENT"]
    if _defects_or_parity_checks(strategy_list):
        return ["SNAP_TURN_PARITY_OR_IMPLEMENTATION_DEFECT_FOUND"]
    if _variant_candidates(strategy_list):
        return ["SNAP_TURN_IMPROVEMENT_CANDIDATE_FOUND"]
    if any(item.get("frequency_classification") == "TOO_QUIET" for item in strategy_list):
        return ["SNAP_TURN_TOO_QUIET_CONFIRMED"]
    if any(item.get("frequency_classification") == "METHODOLOGY_INCONCLUSIVE" for item in strategy_list):
        return ["REPLAY_BACKFILL_INSUFFICIENT"]
    return ["SNAP_TURN_HEALTHY"]


def _simplest_methodology_change(strategies: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    candidates = _variant_candidates(list(strategies))
    if candidates:
        first = candidates[0]
        return {
            "method": "closest_failed_bar_variant",
            "candidate": first,
            "promotion_status": "RESEARCH_REPLAY_ONLY",
            "do_not_promote_to_paper": True,
        }
    quiet = [item for item in strategies if item.get("frequency_classification") == "TOO_QUIET"]
    if quiet:
        return {
            "method": "closest_failed_bar_variant",
            "target_strategy": quiet[0].get("strategy_id"),
            "promotion_status": "RESEARCH_REPLAY_ONLY",
            "do_not_promote_to_paper": True,
            "evidence_needed": "Replay closest failed bars and compare MFE/MAE against control windows before predicate changes.",
        }
    return {
        "method": "none_yet",
        "reason": "Replay/backfill did not produce a sufficient improvement candidate.",
        "promotion_status": "NO_PRODUCTION_CHANGE",
    }


def _location_variant_candidate_rows(
    *,
    snapshots: Iterable[Mapping[str, Any]],
    strategy_id: str,
    predicate: str,
) -> list[Mapping[str, Any]]:
    rows = []
    for snapshot in snapshots:
        if snapshot.get("strategy_id") != strategy_id:
            continue
        if snapshot.get("hard_signal") is True:
            continue
        failed = [str(item) for item in snapshot.get("failed_primitive_predicates") or []]
        if predicate not in failed:
            continue
        if str(snapshot.get("near_miss_bucket") or "") not in {"ONE_PREDICATE_AWAY", "TWO_PREDICATES_AWAY"}:
            continue
        if (snapshot.get("eligibility") or {}).get("eligible") is not True:
            continue
        rows.append(snapshot)
    rows.sort(key=lambda item: str(item.get("decision_bar_timestamp") or ""))
    return rows


def _research_sample_result(
    *,
    snapshot: Mapping[str, Any],
    candles: list[dict[str, Any]],
    max_horizon_bars: int,
    time_box_bars: int,
) -> dict[str, Any]:
    timestamp = _parse_dt(str(snapshot.get("decision_bar_timestamp") or ""))
    entry = _decimal(snapshot.get("proposed_entry_price"))
    risk = _risk_from_snapshot(snapshot)
    future = [item for item in candles if timestamp is not None and item["timestamp"] > timestamp][:max_horizon_bars]
    mfe_mae = _path_mfe_mae(entry=entry, future=future, side="SHORT")
    bar_excursions = _bar_excursions(entry=entry, risk=risk, future=future, side="SHORT", bars=(1, 2, 3, 5))
    policies = {
        "target_1r_stop_1r": _stop_target_policy(
            entry=entry,
            risk=risk,
            future=future,
            side="SHORT",
            target_r=Decimal("1.0"),
        ),
        "target_1_5r_stop_1r": _stop_target_policy(
            entry=entry,
            risk=risk,
            future=future,
            side="SHORT",
            target_r=Decimal("1.5"),
        ),
        "time_boxed_3x5m": _time_box_policy(entry=entry, risk=risk, future=future, side="SHORT", bars=time_box_bars),
        "quick_scalp_0_5r_stop_1r": _stop_target_policy(
            entry=entry,
            risk=risk,
            future=future,
            side="SHORT",
            target_r=Decimal("0.5"),
        ),
        "breakeven_after_0_5r": _breakeven_after_favorable_policy(
            entry=entry,
            risk=risk,
            future=future,
            side="SHORT",
            trigger_r=Decimal("0.5"),
            target_r=Decimal("1.0"),
        ),
        "trail_after_first_favorable_bar": _trail_after_first_favorable_bar_policy(
            entry=entry,
            risk=risk,
            future=future,
            side="SHORT",
            bars=time_box_bars,
        ),
        "failed_followthrough_exit_1bar": _failed_followthrough_exit_policy(
            entry=entry,
            risk=risk,
            future=future,
            side="SHORT",
            confirmation_bars=1,
            fallback_bars=time_box_bars,
        ),
        "failed_followthrough_exit_2bar": _failed_followthrough_exit_policy(
            entry=entry,
            risk=risk,
            future=future,
            side="SHORT",
            confirmation_bars=2,
            fallback_bars=time_box_bars,
        ),
        "vol_scaled_0_75r_target_0_75r_stop": _stop_target_policy(
            entry=entry,
            risk=risk,
            future=future,
            side="SHORT",
            target_r=Decimal("0.75"),
            stop_r=Decimal("0.75"),
        ),
        "vwap_ema_invalidation_exit": _vwap_ema_invalidation_policy(snapshot=snapshot),
        "time_stop_no_favorable_1bar": _time_stop_no_favorable_policy(
            entry=entry,
            risk=risk,
            future=future,
            side="SHORT",
            bars=time_box_bars,
        ),
    }
    return {
        "timestamp": snapshot.get("decision_bar_timestamp"),
        "session": snapshot.get("session"),
        "regime": snapshot.get("regime"),
        "proposed_entry_price": _decimal_str(entry),
        "risk_points": _decimal_str(risk),
        "failed_primitive_predicates": snapshot.get("failed_primitive_predicates") or [],
        "mfe_points": _decimal_str(mfe_mae["mfe"]),
        "mae_points": _decimal_str(mfe_mae["mae"]),
        "mfe_occurred_before_mae": mfe_mae["mfe_before_mae"],
        "time_to_mfe_bars": mfe_mae["time_to_mfe_bars"],
        "time_to_mae_bars": mfe_mae["time_to_mae_bars"],
        "bar_excursions": bar_excursions,
        "early_favorable_move_before_failing": _early_favorable_before_failing(
            bar_excursions=bar_excursions,
            policies=policies,
        ),
        "loser_failed_immediately": _loser_failed_immediately(bar_excursions=bar_excursions, policies=policies),
        "policies": policies,
    }


def _risk_from_snapshot(snapshot: Mapping[str, Any]) -> Decimal:
    for item in snapshot.get("primitive_predicates") or []:
        if not isinstance(item, Mapping):
            continue
        if item.get("predicate") == "bear_snap_range_ok":
            threshold = _decimal(item.get("threshold"))
            if threshold > 0:
                return threshold / Decimal("0.90")
    ohlc = snapshot.get("bar_ohlc") if isinstance(snapshot.get("bar_ohlc") or {}, Mapping) else {}
    bar_range = _decimal(ohlc.get("high")) - _decimal(ohlc.get("low"))
    return bar_range if bar_range > 0 else Decimal("1")


def _path_mfe_mae(*, entry: Decimal, future: list[dict[str, Any]], side: str) -> dict[str, Any]:
    mfe = Decimal("0")
    mae = Decimal("0")
    time_to_mfe: int | None = None
    time_to_mae: int | None = None
    for index, bar in enumerate(future, start=1):
        high = _decimal(bar.get("high"))
        low = _decimal(bar.get("low"))
        if side == "SHORT":
            favorable = entry - low
            adverse = entry - high
        else:
            favorable = high - entry
            adverse = low - entry
        if favorable > mfe:
            mfe = favorable
            time_to_mfe = index
        if adverse < mae:
            mae = adverse
            time_to_mae = index
    mfe_before_mae = None
    if time_to_mfe is not None and time_to_mae is not None:
        mfe_before_mae = time_to_mfe < time_to_mae
    return {"mfe": mfe, "mae": mae, "time_to_mfe_bars": time_to_mfe, "time_to_mae_bars": time_to_mae, "mfe_before_mae": mfe_before_mae}


def _stop_target_policy(
    *,
    entry: Decimal,
    risk: Decimal,
    future: list[dict[str, Any]],
    side: str,
    target_r: Decimal,
    stop_r: Decimal = Decimal("1.0"),
) -> dict[str, Any]:
    if risk <= 0 or not future:
        return {"outcome": "NO_FUTURE_BARS", "r": None}
    target = entry - target_r * risk if side == "SHORT" else entry + target_r * risk
    stop = entry + stop_r * risk if side == "SHORT" else entry - stop_r * risk
    for index, bar in enumerate(future, start=1):
        high = _decimal(bar.get("high"))
        low = _decimal(bar.get("low"))
        target_hit = low <= target if side == "SHORT" else high >= target
        stop_hit = high >= stop if side == "SHORT" else low <= stop
        if target_hit and stop_hit:
            return {"outcome": "STOP_FIRST_SAME_BAR_CONSERVATIVE", "r": _decimal_str(-stop_r), "bars": index}
        if stop_hit:
            return {"outcome": "STOP", "r": _decimal_str(-stop_r), "bars": index}
        if target_hit:
            return {"outcome": "TARGET", "r": _decimal_str(target_r), "bars": index}
    close = _decimal(future[-1].get("close"))
    r_value = (entry - close) / risk if side == "SHORT" else (close - entry) / risk
    return {"outcome": "TIMEOUT_MARK_TO_CLOSE", "r": _decimal_str(r_value), "bars": len(future)}


def _time_box_policy(*, entry: Decimal, risk: Decimal, future: list[dict[str, Any]], side: str, bars: int) -> dict[str, Any]:
    if risk <= 0 or not future:
        return {"outcome": "NO_FUTURE_BARS", "r": None}
    chosen = future[min(len(future), bars) - 1]
    close = _decimal(chosen.get("close"))
    r_value = (entry - close) / risk if side == "SHORT" else (close - entry) / risk
    return {"outcome": "TIME_BOX_EXIT", "r": _decimal_str(r_value), "bars": min(len(future), bars)}


def _bar_excursions(
    *,
    entry: Decimal,
    risk: Decimal,
    future: list[dict[str, Any]],
    side: str,
    bars: Iterable[int],
) -> dict[str, Any]:
    rows: dict[str, Any] = {}
    for count in bars:
        window = future[:count]
        if not window or risk <= 0:
            rows[str(count)] = {"available": False}
            continue
        mfe_mae = _path_mfe_mae(entry=entry, future=window, side=side)
        close = _decimal(window[-1].get("close"))
        close_excursion = entry - close if side == "SHORT" else close - entry
        rows[str(count)] = {
            "available": True,
            "bars": len(window),
            "mfe_points": _decimal_str(mfe_mae["mfe"]),
            "mae_points": _decimal_str(mfe_mae["mae"]),
            "mfe_r": _decimal_str(mfe_mae["mfe"] / risk),
            "mae_r": _decimal_str(mfe_mae["mae"] / risk),
            "close_excursion_points": _decimal_str(close_excursion),
            "close_excursion_r": _decimal_str(close_excursion / risk),
        }
    return rows


def _breakeven_after_favorable_policy(
    *,
    entry: Decimal,
    risk: Decimal,
    future: list[dict[str, Any]],
    side: str,
    trigger_r: Decimal,
    target_r: Decimal,
) -> dict[str, Any]:
    if risk <= 0 or not future:
        return {"outcome": "NO_FUTURE_BARS", "r": None}
    target = entry - target_r * risk if side == "SHORT" else entry + target_r * risk
    stop = entry + risk if side == "SHORT" else entry - risk
    breakeven_armed = False
    for index, bar in enumerate(future, start=1):
        high = _decimal(bar.get("high"))
        low = _decimal(bar.get("low"))
        target_hit = low <= target if side == "SHORT" else high >= target
        stop_hit = high >= stop if side == "SHORT" else low <= stop
        breakeven_hit = high >= entry if side == "SHORT" else low <= entry
        trigger_hit = low <= entry - trigger_r * risk if side == "SHORT" else high >= entry + trigger_r * risk
        if target_hit and stop_hit:
            return {"outcome": "STOP_FIRST_SAME_BAR_CONSERVATIVE", "r": "-1", "bars": index}
        if stop_hit:
            return {"outcome": "STOP", "r": "-1", "bars": index}
        if breakeven_armed and breakeven_hit:
            return {"outcome": "BREAKEVEN_STOP", "r": "0", "bars": index}
        if target_hit:
            return {"outcome": "TARGET", "r": _decimal_str(target_r), "bars": index}
        if trigger_hit:
            breakeven_armed = True
    close = _decimal(future[-1].get("close"))
    r_value = (entry - close) / risk if side == "SHORT" else (close - entry) / risk
    return {"outcome": "TIMEOUT_MARK_TO_CLOSE", "r": _decimal_str(r_value), "bars": len(future)}


def _trail_after_first_favorable_bar_policy(
    *,
    entry: Decimal,
    risk: Decimal,
    future: list[dict[str, Any]],
    side: str,
    bars: int,
) -> dict[str, Any]:
    if risk <= 0 or not future:
        return {"outcome": "NO_FUTURE_BARS", "r": None}
    window = future[:bars]
    first = window[0]
    first_close = _decimal(first.get("close"))
    first_favorable = first_close < entry if side == "SHORT" else first_close > entry
    if not first_favorable:
        return _time_box_policy(entry=entry, risk=risk, future=window, side=side, bars=1) | {
            "outcome": "NO_FIRST_BAR_FOLLOW_THROUGH_EXIT"
        }
    trail_stop = min(entry, _decimal(first.get("high"))) if side == "SHORT" else max(entry, _decimal(first.get("low")))
    for index, bar in enumerate(window[1:], start=2):
        high = _decimal(bar.get("high"))
        low = _decimal(bar.get("low"))
        if (side == "SHORT" and high >= trail_stop) or (side != "SHORT" and low <= trail_stop):
            r_value = (entry - trail_stop) / risk if side == "SHORT" else (trail_stop - entry) / risk
            return {"outcome": "TRAIL_STOP", "r": _decimal_str(r_value), "bars": index}
        trail_stop = min(trail_stop, high) if side == "SHORT" else max(trail_stop, low)
    close = _decimal(window[-1].get("close"))
    r_value = (entry - close) / risk if side == "SHORT" else (close - entry) / risk
    return {"outcome": "TRAIL_TIMEOUT_MARK_TO_CLOSE", "r": _decimal_str(r_value), "bars": len(window)}


def _failed_followthrough_exit_policy(
    *,
    entry: Decimal,
    risk: Decimal,
    future: list[dict[str, Any]],
    side: str,
    confirmation_bars: int,
    fallback_bars: int,
) -> dict[str, Any]:
    if risk <= 0 or not future:
        return {"outcome": "NO_FUTURE_BARS", "r": None}
    confirmation = future[:confirmation_bars]
    if not confirmation:
        return {"outcome": "NO_FUTURE_BARS", "r": None}
    close = _decimal(confirmation[-1].get("close"))
    favorable_close = close < entry if side == "SHORT" else close > entry
    if not favorable_close:
        r_value = (entry - close) / risk if side == "SHORT" else (close - entry) / risk
        return {
            "outcome": f"FAILED_FOLLOW_THROUGH_{confirmation_bars}BAR_EXIT",
            "r": _decimal_str(r_value),
            "bars": len(confirmation),
        }
    return _time_box_policy(entry=entry, risk=risk, future=future, side=side, bars=fallback_bars) | {
        "outcome": f"FOLLOW_THROUGH_CONFIRMED_TIME_BOX_{fallback_bars}BARS"
    }


def _vwap_ema_invalidation_policy(*, snapshot: Mapping[str, Any]) -> dict[str, Any]:
    predicates = {str(item.get("predicate")): item for item in snapshot.get("primitive_predicates") or [] if isinstance(item, Mapping)}
    has_invalidation_inputs = any("vwap" in key.lower() or "ema" in key.lower() for key in predicates)
    if not has_invalidation_inputs:
        return {
            "available": False,
            "outcome": "NOT_AVAILABLE",
            "r": None,
            "reason": "VWAP/EMA invalidation path values are not retained in the scorable snapshot.",
        }
    return {
        "available": False,
        "outcome": "NOT_IMPLEMENTED_FROM_STATIC_SNAPSHOT",
        "r": None,
        "reason": "VWAP/EMA predicate values exist, but per-bar invalidation path values are not retained.",
    }


def _time_stop_no_favorable_policy(
    *,
    entry: Decimal,
    risk: Decimal,
    future: list[dict[str, Any]],
    side: str,
    bars: int,
) -> dict[str, Any]:
    if risk <= 0 or not future:
        return {"outcome": "NO_FUTURE_BARS", "r": None}
    first = _path_mfe_mae(entry=entry, future=future[:1], side=side)
    if first["mfe"] <= 0:
        return _time_box_policy(entry=entry, risk=risk, future=future[:1], side=side, bars=1) | {
            "outcome": "TIME_STOP_NO_FAVORABLE_MOVE_1BAR"
        }
    return _time_box_policy(entry=entry, risk=risk, future=future, side=side, bars=bars) | {
        "outcome": f"FAVORABLE_MOVE_OBSERVED_TIME_BOX_{bars}BARS"
    }


def _early_favorable_before_failing(*, bar_excursions: Mapping[str, Any], policies: Mapping[str, Any]) -> bool:
    one_bar = bar_excursions.get("1") if isinstance(bar_excursions.get("1") or {}, Mapping) else {}
    three_bar = policies.get("time_boxed_3x5m") if isinstance(policies.get("time_boxed_3x5m") or {}, Mapping) else {}
    return _decimal(one_bar.get("mfe_r")) >= Decimal("0.25") and _decimal(three_bar.get("r")) <= 0


def _loser_failed_immediately(*, bar_excursions: Mapping[str, Any], policies: Mapping[str, Any]) -> bool:
    one_bar = bar_excursions.get("1") if isinstance(bar_excursions.get("1") or {}, Mapping) else {}
    one_r = policies.get("target_1r_stop_1r") if isinstance(policies.get("target_1r_stop_1r") or {}, Mapping) else {}
    return _decimal(one_bar.get("mae_r")) <= Decimal("-1") or (
        one_r.get("outcome") in {"STOP", "STOP_FIRST_SAME_BAR_CONSERVATIVE"} and one_r.get("bars") == 1
    )


def _policy_summary(samples: list[Mapping[str, Any]], *, include_exit_sensitivity: bool = False) -> dict[str, Any]:
    summary = {
        "sample_count": len(samples),
        "target_1r_stop_1r": _single_policy_summary(samples, "target_1r_stop_1r"),
        "target_1_5r_stop_1r": _single_policy_summary(samples, "target_1_5r_stop_1r"),
        "time_boxed_3x5m": _single_policy_summary(samples, "time_boxed_3x5m"),
    }
    if include_exit_sensitivity:
        for policy in (
            "quick_scalp_0_5r_stop_1r",
            "breakeven_after_0_5r",
            "trail_after_first_favorable_bar",
            "failed_followthrough_exit_1bar",
            "failed_followthrough_exit_2bar",
            "vol_scaled_0_75r_target_0_75r_stop",
            "vwap_ema_invalidation_exit",
            "time_stop_no_favorable_1bar",
        ):
            summary[policy] = _single_policy_summary(samples, policy)
    return summary


def _single_policy_summary(samples: list[Mapping[str, Any]], policy: str) -> dict[str, Any]:
    r_values = [_decimal(((sample.get("policies") or {}).get(policy) or {}).get("r")) for sample in samples if ((sample.get("policies") or {}).get(policy) or {}).get("r") is not None]
    if not r_values:
        return {"sample_count": 0, "win_rate": None, "average_r": None, "worst_r": None, "max_drawdown_r": None}
    wins = [value for value in r_values if value > 0]
    return {
        "sample_count": len(r_values),
        "win_rate": _decimal_str(Decimal(len(wins)) / Decimal(len(r_values))),
        "average_r": _decimal_str(sum(r_values, Decimal("0")) / Decimal(len(r_values))),
        "worst_r": _decimal_str(min(r_values)),
        "max_drawdown_r": _decimal_str(_max_drawdown(r_values)),
    }


def _baseline_rows(
    *,
    snapshots: Iterable[Mapping[str, Any]],
    excluded: Iterable[Mapping[str, Any]],
    strategy_id: str,
    sample_count: int,
    seed: int,
) -> list[Mapping[str, Any]]:
    excluded_keys = {_snapshot_key(item) for item in excluded}
    population = [
        item
        for item in snapshots
        if item.get("strategy_id") == strategy_id
        and (item.get("eligibility") or {}).get("eligible") is True
        and _snapshot_key(item) not in excluded_keys
    ]
    rng = random.Random(seed)
    if sample_count <= 0:
        return []
    if len(population) <= sample_count:
        return population
    return rng.sample(population, sample_count)


def _location_variant_classification(
    *,
    sample_count: int,
    policy_summary: Mapping[str, Any],
    baseline_summary: Mapping[str, Any],
    minimum_sample_count: int,
) -> str:
    if sample_count < minimum_sample_count:
        return "INSUFFICIENT_SAMPLE"
    candidate_time = policy_summary.get("time_boxed_3x5m") or {}
    candidate_1r = policy_summary.get("target_1r_stop_1r") or {}
    baseline_time = baseline_summary.get("time_boxed_3x5m") or {}
    candidate_avg = _decimal(candidate_time.get("average_r"))
    candidate_1r_avg = _decimal(candidate_1r.get("average_r"))
    baseline_avg = _decimal(baseline_time.get("average_r"))
    if candidate_avg > Decimal("0.05") and candidate_1r_avg > Decimal("0") and candidate_avg > baseline_avg:
        return "PROMOTE_TO_REPLAY_CANDIDATE"
    if candidate_avg <= Decimal("-0.10") and candidate_1r_avg <= Decimal("-0.10"):
        return "REJECT_FALSE_POSITIVE"
    return "KEEP_RESEARCH_ONLY"


def _location_variant_answer(classification: str, policy_summary: Mapping[str, Any], baseline_summary: Mapping[str, Any]) -> str:
    candidate_time = policy_summary.get("time_boxed_3x5m") or {}
    baseline_time = baseline_summary.get("time_boxed_3x5m") or {}
    if classification == "PROMOTE_TO_REPLAY_CANDIDATE":
        return (
            "Relaxing/modifying bear_snap_location_ok is credible enough for a bounded replay candidate, "
            f"with time-box average R {candidate_time.get('average_r')} versus baseline {baseline_time.get('average_r')}."
        )
    if classification == "REJECT_FALSE_POSITIVE":
        return "The near-misses looked attractive in raw MFE/MAE but failed simple policy replay; reject this false-positive lane."
    if classification == "INSUFFICIENT_SAMPLE":
        return "The sample is too small for a profitability claim; keep research-only and collect/replay more windows."
    return "The candidate remains research-only: raw MFE/MAE is interesting, but simple policy replay is not strong enough for promotion."


def _exit_sensitivity_classification(
    *,
    sample_count: int,
    minimum_sample_count: int,
    policy_summary: Mapping[str, Any],
    early_favorable_count: int,
    immediate_loser_count: int,
) -> str:
    if sample_count < minimum_sample_count:
        return "INSUFFICIENT_SAMPLE"
    best = _best_policy(policy_summary)
    best_avg = _decimal(best.get("average_r"))
    best_policy = str(best.get("policy") or "")
    early_rate = Decimal(early_favorable_count) / Decimal(sample_count) if sample_count else Decimal("0")
    immediate_loser_rate = Decimal(immediate_loser_count) / Decimal(sample_count) if sample_count else Decimal("0")
    if best_avg > Decimal("0.05"):
        if best_policy.startswith("quick_scalp"):
            return "SCALP_ONLY_CANDIDATE"
        if best_policy.startswith("vol_scaled"):
            return "NEEDS_VOLATILITY_SCALED_MANAGEMENT"
        return "EXIT_POLICY_MISMATCH"
    if immediate_loser_rate >= Decimal("0.50") and early_rate < Decimal("0.35"):
        return "ENTRY_EDGE_REJECTED"
    return "REJECT_ALL_TESTED_POLICIES"


def _best_policy(policy_summary: Mapping[str, Any]) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for policy, summary in policy_summary.items():
        if policy == "sample_count" or not isinstance(summary, Mapping):
            continue
        if summary.get("average_r") is None:
            continue
        candidates.append(
            {
                "policy": policy,
                "average_r": summary.get("average_r"),
                "win_rate": summary.get("win_rate"),
                "worst_r": summary.get("worst_r"),
                "max_drawdown_r": summary.get("max_drawdown_r"),
                "sample_count": summary.get("sample_count"),
            }
        )
    if not candidates:
        return {"policy": None, "average_r": None}
    return max(candidates, key=lambda item: _decimal(item.get("average_r")))


def _exit_sensitivity_answer(classification: str, policy_summary: Mapping[str, Any]) -> str:
    best = _best_policy(policy_summary)
    best_policy = best.get("policy")
    best_avg = best.get("average_r")
    if classification == "SCALP_ONLY_CANDIDATE":
        return (
            "The location variant still should not be promoted to PAPER, but entry quality is not fully rejected: "
            f"the best tested result is the quick-scalp policy ({best_policy}) at average R {best_avg}. "
            "Next work should test a research-only scalp management variant with controls, not loosen production."
        )
    if classification == "EXIT_POLICY_MISMATCH":
        return (
            "The entry may contain early signal value, but crude exits are mismatched. Keep research-only and test the "
            f"best management family ({best_policy}, average R {best_avg}) against broader replay/control windows."
        )
    if classification == "NEEDS_VOLATILITY_SCALED_MANAGEMENT":
        return (
            "The best result came from volatility-scaled management. Keep research-only and test ATR-scaled exits "
            "before changing any production snap-turn predicate."
        )
    if classification == "ENTRY_EDGE_REJECTED":
        return "The variant samples mostly failed before giving favorable excursion; reject this entry edge for now."
    if classification == "INSUFFICIENT_SAMPLE":
        return "The sample is still too small for an exit-design conclusion; retain research-only and replay more windows."
    return (
        "Do not kill the idea solely because the first exits failed, but every additional bounded management policy also "
        "failed here. Keep it research-only and require a better entry/management hypothesis before more work."
    )


def _outlier_sensitivity(samples: list[Mapping[str, Any]]) -> dict[str, Any]:
    r_values = [_decimal(((sample.get("policies") or {}).get("time_boxed_3x5m") or {}).get("r")) for sample in samples if ((sample.get("policies") or {}).get("time_boxed_3x5m") or {}).get("r") is not None]
    if len(r_values) < 3:
        return {"available": False}
    sorted_values = sorted(r_values)
    trimmed = sorted_values[1:-1]
    return {
        "available": True,
        "average_r": _decimal_str(sum(r_values, Decimal("0")) / Decimal(len(r_values))),
        "trimmed_best_worst_average_r": _decimal_str(sum(trimmed, Decimal("0")) / Decimal(len(trimmed))),
        "best_r": _decimal_str(max(r_values)),
        "worst_r": _decimal_str(min(r_values)),
    }


def _loss_clustering(samples: list[Mapping[str, Any]], *, policy: str) -> dict[str, Any]:
    r_values = [_decimal(((sample.get("policies") or {}).get(policy) or {}).get("r")) for sample in samples if ((sample.get("policies") or {}).get(policy) or {}).get("r") is not None]
    max_losses = 0
    current = 0
    for value in r_values:
        if value <= 0:
            current += 1
            max_losses = max(max_losses, current)
        else:
            current = 0
    return {"policy": policy, "max_consecutive_non_winners": max_losses, "sample_count": len(r_values)}


def _max_drawdown(r_values: list[Decimal]) -> Decimal:
    equity = Decimal("0")
    peak = Decimal("0")
    max_dd = Decimal("0")
    for value in r_values:
        equity += value
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return max_dd


def _candle_index(
    *,
    evaluated_rows: Iterable[Mapping[str, Any]],
    repo_root: Path,
    snap_turn_root: Path,
    mgc_live_5m: Path,
    mnq_live_5m: Path,
) -> dict[str, list[dict[str, Any]]]:
    by_instrument: dict[str, dict[datetime, dict[str, Any]]] = {"MGC": {}, "MNQ": {}}
    for row in evaluated_rows:
        strategy_id = str(row.get("strategy_id") or "")
        instrument = SNAP_TURN_STRATEGIES.get(strategy_id, {}).get("instrument")
        timestamp = _parse_dt(str(row.get("decision_bar_timestamp") or ""))
        ohlc = row.get("ohlc") if isinstance(row.get("ohlc") or {}, Mapping) else {}
        if instrument and timestamp and all(key in ohlc for key in ("open", "high", "low", "close")):
            by_instrument[instrument][timestamp] = {"timestamp": timestamp, **ohlc}
    for path in _resolve(repo_root, snap_turn_root).glob("**/*snap_turn_event_envelope.json"):
        event = _load_json(path)
        instrument = _event_instrument(event)
        timestamp = _parse_dt(_event_timestamp(event) or "")
        ohlc = _ohlc(event)
        if instrument in by_instrument and timestamp and all(key in ohlc for key in ("open", "high", "low", "close")):
            by_instrument[instrument][timestamp] = {"timestamp": timestamp, **ohlc}
    for instrument, path in {"MGC": mgc_live_5m, "MNQ": mnq_live_5m}.items():
        for candle in _live_candles(_load_json(_resolve(repo_root, path))):
            by_instrument[instrument][candle["timestamp"]] = candle
    return {instrument: [rows[key] for key in sorted(rows)] for instrument, rows in by_instrument.items()}


def _live_candles(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get("completed_5m_candles") or payload.get("candles") or payload.get("bars") or []
    rows = raw if isinstance(raw, list) else []
    candles: list[dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, Mapping):
            continue
        timestamp = _parse_dt(str(item.get("timestamp") or item.get("ts_event") or item.get("bar_timestamp") or ""))
        if timestamp is None:
            continue
        candles.append(
            {
                "timestamp": timestamp,
                "open": str(item.get("open")),
                "high": str(item.get("high")),
                "low": str(item.get("low")),
                "close": str(item.get("close")),
            }
        )
    return candles


def _distance_predicate(
    predicate: str,
    passed: Any,
    actual: Decimal,
    threshold: Decimal,
    *,
    mode: str,
) -> dict[str, Any]:
    if actual == 0 and threshold == 0:
        return {"predicate": predicate, "passed": _bool(passed), "actual": None, "threshold": None, "distance": None}
    distance = actual - threshold if mode == "min" else threshold - actual
    return {
        "predicate": predicate,
        "passed": _bool(passed),
        "actual": actual,
        "threshold": threshold,
        "distance": distance,
    }


def _bool_predicate(predicate: str, passed: Any) -> dict[str, Any]:
    return {"predicate": predicate, "passed": _bool(passed), "actual": passed, "threshold": True, "distance": None}


def _distance_rows(predicates: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in predicates:
        distance = item.get("distance")
        rows.append(
            {
                "predicate": item.get("predicate"),
                "actual": _decimal_str(item.get("actual")) if isinstance(item.get("actual"), Decimal) else item.get("actual"),
                "threshold": _decimal_str(item.get("threshold"))
                if isinstance(item.get("threshold"), Decimal)
                else item.get("threshold"),
                "pass_margin": _decimal_str(distance) if isinstance(distance, Decimal) else None,
            }
        )
    return rows


def _strategy_posture(
    *,
    hard_signals: int,
    one_predicate_away: int,
    two_predicates_away: int,
    failed_primitives: Counter[str],
    frequency_classification: str,
) -> str:
    if frequency_classification == "TOO_QUIET":
        return "SNAP_TURN_TOO_QUIET_REQUIRES_CLOSEST_BAR_REPLAY"
    if frequency_classification == "METHODOLOGY_INCONCLUSIVE":
        return "SNAP_TURN_AUDIT_METHODOLOGY_INCONCLUSIVE"
    if one_predicate_away or two_predicates_away:
        return "SNAP_TURN_VARIANT_REPLAY_CANDIDATE"
    if hard_signals > 0:
        return "SNAP_TURN_SIGNALING_BUT_LOW_NEAR_MISS_EVIDENCE"
    if failed_primitives:
        return "SNAP_TURN_TOO_FAR_FROM_ENTRY_IN_WINDOW"
    return "SNAP_TURN_EVIDENCE_INSUFFICIENT"


def _near_miss_bucket(failed_count: int, *, result: str) -> str:
    if result == "SIGNAL":
        return "SIGNAL"
    if failed_count == 1:
        return "ONE_PREDICATE_AWAY"
    if failed_count == 2:
        return "TWO_PREDICATES_AWAY"
    if failed_count > 2:
        return "MULTI_PREDICATE_FAIL"
    return "NOT_SCORABLE"


def _session_bucket(state: Mapping[str, Any]) -> str:
    if state.get("session_allowed") is not True:
        return "SESSION_FILTER_INACTIVE"
    phase = state.get("derivative_phase") or state.get("session") or "SESSION_ALLOWED"
    return str(phase)


def _regime_bucket(row: Mapping[str, Any]) -> str:
    diagnostics = row.get("diagnostics") if isinstance(row.get("diagnostics") or {}, Mapping) else {}
    velocity = _decimal(diagnostics.get("velocity"))
    atr = _decimal(diagnostics.get("atr"))
    if atr == 0:
        return "REGIME_UNKNOWN"
    normalized = velocity / atr
    if normalized >= Decimal("0.15"):
        return "UPSLOPE"
    if normalized <= Decimal("-0.15"):
        return "DOWNSLOPE"
    return "FLAT_OR_TRANSITIONAL"


def _strategy_id(strategy: Mapping[str, Any]) -> str:
    return str(
        strategy.get("strategy_id")
        or strategy.get("strategy_registry_id")
        or strategy.get("signal_source")
        or strategy.get("rule_id")
        or strategy.get("rule_mode")
        or ""
    )


def _favorable_excursion(excursion: Mapping[str, Any]) -> bool:
    if excursion.get("available") is not True:
        return False
    directional = _decimal(excursion.get("directional_close_excursion_points"))
    mfe = _decimal(excursion.get("mfe_points"))
    mae = _decimal(excursion.get("mae_points")).copy_abs()
    return directional > 0 or (mfe > 0 and mfe >= mae)


def _average_excursion_favorable(avg_mfe: Any, avg_mae: Any) -> bool:
    mfe = _decimal(avg_mfe)
    mae_abs = _decimal(avg_mae).copy_abs()
    return mfe > 0 and mfe >= mae_abs


def _min_time(values: Iterable[Any]) -> str | None:
    parsed = [_parse_dt(str(value or "")) for value in values]
    valid = [value for value in parsed if value is not None]
    return min(valid).isoformat() if valid else None


def _max_time(values: Iterable[Any]) -> str | None:
    parsed = [_parse_dt(str(value or "")) for value in values]
    valid = [value for value in parsed if value is not None]
    return max(valid).isoformat() if valid else None


def _strategy_result(strategy: Mapping[str, Any]) -> str:
    decision = str(strategy.get("decision") or strategy.get("rule_decision") or "").upper()
    if decision in {"LONG", "SHORT", "BUY", "SELL", "SIGNAL"} or strategy.get("signal_emitted") is True:
        return "SIGNAL"
    if "NOT_READY" in decision:
        return "NOT_READY"
    if "ERROR" in decision:
        return "ERROR"
    return "NO_SIGNAL"


def _failed_rule_predicates(strategy: Mapping[str, Any]) -> list[str]:
    raw_conditions = strategy.get("rule_conditions")
    conditions = raw_conditions if isinstance(raw_conditions, Mapping) else {}
    failed = [str(name) for name, passed in conditions.items() if passed is not True]
    if failed:
        return failed
    raw_blockers = strategy.get("rule_blockers")
    blockers = raw_blockers if isinstance(raw_blockers, list) else []
    return [str(item).split("=")[0] for item in blockers]


def _event_timestamp(event: Mapping[str, Any]) -> str | None:
    for key in ("candle_timestamp", "decision_bar_timestamp", "timestamp", "bar_timestamp"):
        if event.get(key):
            return str(event.get(key))
    return None


def _event_instrument(event: Mapping[str, Any]) -> str | None:
    value = str(event.get("instrument_family") or event.get("instrument") or "")
    if value in {"MGC", "MNQ"}:
        return value
    strategy_id = str(event.get("strategy_id") or "")
    if strategy_id in SNAP_TURN_STRATEGIES:
        return SNAP_TURN_STRATEGIES[strategy_id]["instrument"]
    return None


def _ohlc(event: Mapping[str, Any]) -> dict[str, str]:
    return {
        "open": str(event.get("open") or ""),
        "high": str(event.get("high") or ""),
        "low": str(event.get("low") or ""),
        "close": str(event.get("close") or ""),
    }


def _load_runtime_reports(root: Path, *, max_reports: int) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    paths = sorted(root.glob("**/track_b_multi_strategy_runtime_cycle_report.json"), key=lambda item: item.stat().st_mtime)
    paths = paths[-max_reports:]
    rows = [_load_json(path) for path in paths]
    return [dict(item) for item in rows if item]


def _load_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _parse_dt(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _decimal(value: Any) -> Decimal:
    try:
        if value is None or value == "":
            return Decimal("0")
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _decimal_str(value: Any) -> str | None:
    if not isinstance(value, Decimal):
        return None
    return format(value, "f")


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _decimal_str(value)
    return value


def _bool(value: Any) -> bool | None:
    if value is True:
        return True
    if value is False:
        return False
    return None


def _avg_decimal(values: Iterable[Any]) -> Decimal | None:
    decimals = [_decimal(item) for item in values if item is not None]
    if not decimals:
        return None
    return sum(decimals, Decimal("0")) / Decimal(len(decimals))


def _ratio_str(numerator: int, denominator: int) -> str | None:
    if denominator <= 0:
        return None
    return _decimal_str(Decimal(numerator) / Decimal(denominator))


def _counter_rows(counter: Counter[str], *, limit: int) -> list[dict[str, Any]]:
    return [{"reason": key, "count": value} for key, value in counter.most_common(limit)]


def _markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Track B Snap-Turn Near-Miss Amplification",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        "This is read-only evidence. It does not change thresholds, route orders, or invoke paper_proof.",
        "",
        "## Conclusion",
        "",
    ]
    conclusion = report.get("conclusion") or {}
    lines.append(f"- Posture: {conclusion.get('posture')}")
    lines.append(f"- Reason: {conclusion.get('reason')}")
    lines.append(f"- Hard signals: {conclusion.get('hard_signals')}")
    lines.append(f"- Near misses: {conclusion.get('near_miss_count')}")
    lines.extend(["", "## Strategy Summary", ""])
    for strategy in report.get("strategies") or []:
        lines.append(f"### {strategy.get('strategy_id')}")
        lines.append(f"- Instrument/side: {strategy.get('instrument')} {strategy.get('side')}")
        lines.append(f"- Total evaluated bars: {strategy.get('evaluated_completed_bars_total')}")
        lines.append(f"- Eligible completed bars: {strategy.get('eligible_completed_bars_evaluated')}")
        lines.append(
            f"- Hard signals: {strategy.get('hard_signals')} "
            f"(eligible rate {strategy.get('hard_signal_rate_eligible')}%)"
        )
        lines.append(f"- One-predicate-away: {strategy.get('one_predicate_away')}")
        lines.append(f"- Two-predicates-away: {strategy.get('two_predicates_away')}")
        expected = strategy.get("expected_frequency_validation") or {}
        lines.append(
            f"- Expected-frequency classification: {strategy.get('frequency_classification')} "
            f"(observed {expected.get('observed_hard_signals_per_active_day')} hard signals/active day, "
            f"expected {expected.get('expected_hard_signals_per_day')})"
        )
        lines.append(f"- Posture: {strategy.get('amplification_posture')}")
        top = strategy.get("dominant_failed_primitive_predicates") or []
        if top:
            lines.append("- Top primitive failed predicates:")
            for item in top[:5]:
                distance = item.get("numeric_distance") or {}
                excursion = item.get("subsequent_excursion_after_near_misses") or {}
                lines.append(
                    "  - "
                    f"{item.get('predicate')}: {item.get('count')} "
                    f"({item.get('classification')}); avg distance={distance.get('average_pass_margin_points_or_units')}; "
                    f"near-miss avg MFE={excursion.get('average_mfe_points')}, MAE={excursion.get('average_mae_points')}"
                )
        closest = strategy.get("closest_failed_bars") or []
        if closest:
            lines.append("- Closest rejected bars:")
            for item in closest[:3]:
                excursion = item.get("future_excursion") or {}
                lines.append(
                    "  - "
                    f"{item.get('decision_bar_timestamp')}: failed {item.get('primitive_failed_predicates_count')} primitives "
                    f"{item.get('failed_primitive_predicates')}; MFE={excursion.get('mfe_points')}, "
                    f"MAE={excursion.get('mae_points')}, directional={excursion.get('directional_close_excursion_points')}"
                )
        lines.append("")
    lines.extend(["## Top Variant Candidates", ""])
    for item in report.get("variant_candidates") or []:
        lines.append(
            f"- {item.get('strategy_id')} {item.get('predicate')}: failed {item.get('failed_count')}, "
            f"near-miss samples {item.get('near_miss_excursion_sample_count')}. {item.get('next_step')}"
        )
    if not report.get("variant_candidates"):
        lines.append("- None with enough one/two-predicate-away evidence in this bounded window.")
    lines.extend(["", "## Evidence Retention", ""])
    retention = report.get("scorable_snapshot_retention") or {}
    lines.append(f"- Status: {retention.get('status')}")
    lines.append(f"- Snapshots retained this run: {retention.get('snapshot_count')}")
    lines.append(f"- Appended snapshots: {retention.get('appended_snapshot_count')}")
    lines.append(f"- Missing feature envelopes: {retention.get('feature_envelope_missing_count')}")
    lines.append(f"- Replay/backfill required: {retention.get('replay_backfill_required')}")
    lines.append(f"- Snapshot JSONL: {retention.get('jsonl_path')}")
    lines.extend(["", "## Improvement Methods To Test", ""])
    for item in report.get("improvement_methods_to_test") or []:
        lines.append(f"- {item.get('method')}: risk={item.get('false_positive_risk')}")
    lines.extend(["", "## Output Classifications", ""])
    for item in report.get("output_classifications") or []:
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def _replay_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Track B Snap-Turn Replay Backfill",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        "This is diagnostic replay/backfill only. It does not change production thresholds or submit orders.",
        "",
        "## Classifications",
        "",
    ]
    for item in report.get("classifications") or []:
        lines.append(f"- {item}")
    lines.extend(_sample_frame_markdown(report.get("sample_frame") or {}))
    lines.extend(["", "## Reconstruction", ""])
    for instrument, summary in (report.get("instrument_reconstruction") or {}).items():
        lines.append(
            f"- {instrument}: candles={summary.get('input_candle_count')}, "
            f"windows={summary.get('replay_windows_attempted')}, rows={summary.get('strategy_rows_reconstructed')}"
        )
    lines.extend(["", "## Strategy Counts", ""])
    for strategy in report.get("strategies") or []:
        lines.append(f"### {strategy.get('strategy_id')}")
        lines.append(f"- Completed bars reconstructed: {strategy.get('evaluated_completed_bars_total')}")
        lines.append(f"- Eligible bars: {strategy.get('eligible_completed_bars_evaluated')}")
        lines.append(f"- Hard signals: {strategy.get('hard_signals')}")
        lines.append(f"- One-predicate-away: {strategy.get('one_predicate_away')}")
        lines.append(f"- Two-predicates-away: {strategy.get('two_predicates_away')}")
        lines.append(f"- Frequency classification: {strategy.get('frequency_classification')}")
        closest = strategy.get("closest_failed_bars") or []
        if closest:
            lines.append("- Closest failed bars:")
            for item in closest[:5]:
                excursion = item.get("future_excursion") or {}
                lines.append(
                    "  - "
                    f"{item.get('decision_bar_timestamp')}: failed={item.get('primitive_failed_predicates_count')} "
                    f"MFE={excursion.get('mfe_points')} MAE={excursion.get('mae_points')} "
                    f"directional={excursion.get('directional_close_excursion_points')}"
                )
        lines.append("")
    lines.extend(["## Next Methodology Change To Test", ""])
    method = report.get("simplest_methodology_change_to_test_first") or {}
    lines.append(f"- Method: {method.get('method')}")
    lines.append(f"- Promotion status: {method.get('promotion_status')}")
    if method.get("candidate"):
        lines.append(f"- Candidate: {method.get('candidate')}")
    if method.get("reason"):
        lines.append(f"- Reason: {method.get('reason')}")
    return "\n".join(lines) + "\n"


def _location_variant_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Track B Snap-Turn Location Variant Research Replay",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        f"Candidate: {report.get('candidate_name')}",
        f"Classification: {report.get('classification')}",
        "",
        "This is research-only. It does not change production thresholds, submit orders, or promote to PAPER.",
        "",
    ]
    lines.extend(_sample_frame_markdown(report.get("sample_frame") or {}))
    lines.extend([
        "## Policy Summary",
        "",
    ])
    policy_summary = report.get("policy_summary") or {}
    for policy in ("target_1r_stop_1r", "target_1_5r_stop_1r", "time_boxed_3x5m"):
        summary = policy_summary.get(policy) or {}
        lines.append(
            f"- {policy}: n={summary.get('sample_count')}, win_rate={summary.get('win_rate')}, "
            f"avgR={summary.get('average_r')}, worstR={summary.get('worst_r')}, maxDD={summary.get('max_drawdown_r')}"
        )
    lines.extend(["", "## Baseline", ""])
    baseline = report.get("random_baseline_comparison") or {}
    for policy in ("target_1r_stop_1r", "target_1_5r_stop_1r", "time_boxed_3x5m"):
        summary = baseline.get(policy) or {}
        lines.append(f"- {policy}: avgR={summary.get('average_r')}, win_rate={summary.get('win_rate')}")
    lines.extend(["", "## Samples", ""])
    for sample in (report.get("samples") or [])[:25]:
        time_box = ((sample.get("policies") or {}).get("time_boxed_3x5m") or {})
        lines.append(
            f"- {sample.get('timestamp')} {sample.get('session')} {sample.get('regime')}: "
            f"entry={sample.get('proposed_entry_price')} MFE={sample.get('mfe_points')} MAE={sample.get('mae_points')} "
            f"MFE_before_MAE={sample.get('mfe_occurred_before_mae')} time_box_R={time_box.get('r')}"
        )
    lines.extend(["", "## Answer", "", str(report.get("expected_answer") or "")])
    return "\n".join(lines) + "\n"


def _exit_sensitivity_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Track B Snap-Turn Location Variant Exit Sensitivity",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        f"Candidate: {report.get('candidate_name')}",
        f"Classification: {report.get('classification')}",
        "",
        "This is research-only. It does not change production thresholds, promote to PAPER, or submit orders.",
        "",
    ]
    lines.extend(_sample_frame_markdown(report.get("sample_frame") or {}))
    lines.extend([
        "## Entry-vs-Exit Evidence",
        "",
        f"- Sample count: {report.get('sample_count')}",
        f"- Early favorable move before failing: {report.get('early_favorable_move_count')} "
        f"({report.get('early_favorable_move_rate')})",
        f"- Losers failed immediately: {report.get('loser_failed_immediately_count')} "
        f"({report.get('loser_failed_immediately_rate')})",
    ])
    best = report.get("best_policy") or {}
    lines.extend(
        [
            f"- Best tested policy: {best.get('policy')} avgR={best.get('average_r')} "
            f"win_rate={best.get('win_rate')} worstR={best.get('worst_r')}",
            "",
            "## Policy Summary",
            "",
        ]
    )
    policy_summary = report.get("policy_summary") or {}
    for policy in (
        "target_1r_stop_1r",
        "target_1_5r_stop_1r",
        "time_boxed_3x5m",
        "quick_scalp_0_5r_stop_1r",
        "breakeven_after_0_5r",
        "trail_after_first_favorable_bar",
        "failed_followthrough_exit_1bar",
        "failed_followthrough_exit_2bar",
        "vol_scaled_0_75r_target_0_75r_stop",
        "vwap_ema_invalidation_exit",
        "time_stop_no_favorable_1bar",
    ):
        summary = policy_summary.get(policy) or {}
        lines.append(
            f"- {policy}: n={summary.get('sample_count')}, win_rate={summary.get('win_rate')}, "
            f"avgR={summary.get('average_r')}, worstR={summary.get('worst_r')}, maxDD={summary.get('max_drawdown_r')}"
        )
    lines.extend(["", "## Samples", ""])
    for sample in (report.get("samples") or [])[:30]:
        excursions = sample.get("bar_excursions") or {}
        one = excursions.get("1") or {}
        two = excursions.get("2") or {}
        five = excursions.get("5") or {}
        best_policy = (sample.get("policies") or {}).get(str(best.get("policy") or "")) or {}
        lines.append(
            f"- {sample.get('timestamp')} {sample.get('session')} {sample.get('regime')}: "
            f"MFE={sample.get('mfe_points')} MAE={sample.get('mae_points')} "
            f"MFE_before_MAE={sample.get('mfe_occurred_before_mae')} "
            f"1barR={one.get('close_excursion_r')} 2barR={two.get('close_excursion_r')} "
            f"5barR={five.get('close_excursion_r')} best_policy_R={best_policy.get('r')} "
            f"early_favorable={sample.get('early_favorable_move_before_failing')} "
            f"failed_immediately={sample.get('loser_failed_immediately')}"
        )
    lines.extend(["", "## Answer", "", str(report.get("answer") or "")])
    return "\n".join(lines) + "\n"


def _sample_frame_markdown(sample_frame: Mapping[str, Any]) -> list[str]:
    if not sample_frame:
        return ["", "## Sample Frame", "", "- Not provided.", ""]
    lines = [
        "",
        "## Sample Frame",
        "",
        f"- Classification: {sample_frame.get('lookback_classification')}",
        f"- Source type: {sample_frame.get('sample_source_type')}",
        f"- Sample unit: {sample_frame.get('sample_unit')}",
        f"- Start: {sample_frame.get('start_timestamp')}",
        f"- End: {sample_frame.get('end_timestamp')}",
        f"- Calendar days: {sample_frame.get('calendar_days')}",
        f"- Trading/session days: {sample_frame.get('trading_session_days')}",
        f"- Sessions represented: {', '.join(sample_frame.get('sessions_represented') or [])}",
        "",
        "### Instrument/session bars",
    ]
    instruments = sample_frame.get("completed_5m_bars_by_instrument_session") or {}
    for instrument, frame in instruments.items():
        lines.append(f"- {instrument}: total={frame.get('completed_5m_bars_total')}")
        for item in frame.get("completed_5m_bars_by_session") or []:
            lines.append(f"  - {item.get('session')}: {item.get('completed_5m_bars')} completed 5m bars")
    limitations = sample_frame.get("missing_windows_or_rotated_data_limitations") or []
    lines.extend(["", "### Limitations"])
    if limitations:
        lines.extend(f"- {item}" for item in limitations)
    else:
        lines.append("- No missing windows or rotated-data blockers were detected by this bounded audit.")
    source_files = sample_frame.get("source_data_files") or []
    lines.extend(["", "### Source Data Files"])
    if source_files:
        for item in source_files[:20]:
            lines.append(
                f"- {item.get('instrument')} {item.get('path')}: candles={item.get('candle_count')} "
                f"{item.get('first_timestamp')} -> {item.get('last_timestamp')}"
            )
        if len(source_files) > 20:
            lines.append(f"- ... {len(source_files) - 20} additional source files omitted from markdown; see JSON.")
    else:
        lines.append("- Not provided.")
    lines.append("")
    return lines
