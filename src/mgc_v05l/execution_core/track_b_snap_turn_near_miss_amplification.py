"""Track B snap-turn near-miss / MFE-MAE amplification audit.

This diagnostic is read-only. It analyzes completed-bar snap-turn evaluations
and subsequent market excursions so candidate variants can be ranked before any
production predicate or threshold changes are proposed.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping

from .models import require_aware_datetime, to_jsonable


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
    max_runtime_reports: int = 1800
    max_examples_per_strategy: int = 12
    future_horizon_bars: int = 6


@dataclass(frozen=True)
class TrackBSnapTurnNearMissAmplificationResult:
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
    report = {
        "schema_version": "track_b_snap_turn_near_miss_amplification_v1",
        "generated_at": actual_now.isoformat(),
        "source": "TRACK_B_COMPLETED_DECISION_BAR_RUNTIME_ARTIFACTS",
        "analysis_note": (
            "Near-miss buckets use deduplicated primitive snap-turn gates. Raw/candidate/first "
            "snap-turn composites are still reported separately so they do not hide the underlying predicate."
        ),
        "broker_commands_invoked": False,
        "paper_proof_cli_invoked": False,
        "submit_cancel_place_order_invoked": False,
        "production_thresholds_changed": False,
        "runtime_reports_scanned": len(runtime_reports),
        "bounded_runtime_report_limit": actual_config.max_runtime_reports,
        "future_excursion_horizon_bars": actual_config.future_horizon_bars,
        "strategies": strategies,
        "variant_candidates": _variant_candidates(strategies),
        "defects_or_parity_checks": _defects_or_parity_checks(strategies),
        "conclusion": _conclusion(strategies),
        "outputs": {
            "json": str(actual_config.output_json),
            "markdown": str(actual_config.output_md),
        },
    }
    output_json = _resolve(repo_root, actual_config.output_json)
    output_md = _resolve(repo_root, actual_config.output_md)
    _write_json(output_json, report)
    _write_text(output_md, _markdown(report))
    return TrackBSnapTurnNearMissAmplificationResult(report_json=output_json, report_md=output_md, report=report)


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
    hard_signals = 0
    one_predicate_away = 0
    two_predicates_away = 0
    not_scorable = 0
    no_signal = 0
    not_ready = 0
    for row in rows:
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
        _ = features
    return {
        "strategy_id": strategy_id,
        "instrument": meta["instrument"],
        "side": meta["side"],
        "eligible_completed_bars_evaluated": len(rows),
        "hard_signals": hard_signals,
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
        "amplification_posture": _strategy_posture(
            hard_signals=hard_signals,
            one_predicate_away=one_predicate_away,
            two_predicates_away=two_predicates_away,
            failed_primitives=failed_primitive_counts,
        ),
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
    rows.sort(key=lambda item: (str(item.get("decision_bar_timestamp") or ""), str(item.get("strategy_id") or "")))
    return rows


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
            candidates.append(
                {
                    "strategy_id": strategy.get("strategy_id"),
                    "instrument": strategy.get("instrument"),
                    "side": strategy.get("side"),
                    "predicate": predicate.get("predicate"),
                    "failed_count": predicate.get("count"),
                    "near_miss_excursion_sample_count": sample_count,
                    "average_mfe_points": (predicate.get("subsequent_excursion_after_near_misses") or {}).get("average_mfe_points"),
                    "average_mae_points": (predicate.get("subsequent_excursion_after_near_misses") or {}).get("average_mae_points"),
                    "next_step": (
                        "Replay a bounded candidate variant for this predicate before any threshold change."
                        if sample_count
                        else "Collect/replay near-miss excursion evidence before proposing a variant."
                    ),
                }
            )
    candidates.sort(key=lambda item: (int(item.get("near_miss_excursion_sample_count") or 0), int(item.get("failed_count") or 0)), reverse=True)
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
    if total_near <= 0:
        posture = "PAUSE_BROAD_SNAP_TURN_AMPLIFICATION"
        reason = "No one/two-primitive-predicate-away snap-turn bars were found in the bounded evaluated window."
    elif total_signals > 0:
        posture = "PROCEED_TO_BOUNDED_VARIANT_REPLAY"
        reason = "Hard signals and near-misses exist; rank variants with replay before predicate changes."
    else:
        posture = "PROCEED_TO_BOUNDED_VARIANT_REPLAY"
        reason = "Near-misses exist but hard signals are absent; replay top predicates before changing production."
    return {
        "posture": posture,
        "hard_signals": total_signals,
        "near_miss_count": total_near,
        "reason": reason,
    }


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
) -> str:
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
        lines.append(f"- Eligible completed bars evaluated: {strategy.get('eligible_completed_bars_evaluated')}")
        lines.append(f"- Hard signals: {strategy.get('hard_signals')}")
        lines.append(f"- One-predicate-away: {strategy.get('one_predicate_away')}")
        lines.append(f"- Two-predicates-away: {strategy.get('two_predicates_away')}")
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
        lines.append("")
    lines.extend(["## Top Variant Candidates", ""])
    for item in report.get("variant_candidates") or []:
        lines.append(
            f"- {item.get('strategy_id')} {item.get('predicate')}: failed {item.get('failed_count')}, "
            f"near-miss samples {item.get('near_miss_excursion_sample_count')}. {item.get('next_step')}"
        )
    if not report.get("variant_candidates"):
        lines.append("- None with enough one/two-predicate-away evidence in this bounded window.")
    return "\n".join(lines) + "\n"
