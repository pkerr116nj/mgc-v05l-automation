"""Paper-only experimental canary packaging for the Active Trend Participation Engine."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .backtest import backtest_decisions_with_audit
from .engine import DEFAULT_POINT_VALUES
from .features import build_feature_states
from .models import ConflictOutcome, HigherPrioritySignal, SignalDecision
from .patterns import default_pattern_variants, generate_signal_decisions
from .phase2_continuation import build_phase2_replay_package, latest_atp_entry_state_summary
from .phase3_timing import build_phase3_replay_package, latest_atp_timing_state_summary
from .phase4 import PHASE3_BEST_LONG_VARIANT_ID, _metrics_payload, _shared_1m_coverage
from .phase5 import _cost_fragility_summary
from .state_layers import latest_atp_state_summary, summarize_atp_state_diagnostics
from .storage import load_sqlite_bars, normalize_and_check_bars, resample_bars_from_1m


@dataclass(frozen=True)
class CanaryLaneSpec:
    lane_id: str
    lane_name: str
    side: str
    variant_id: str
    quality_buckets: frozenset[str]
    quality_bucket_policy: str
    experimental_status: str = "experimental_canary"
    canary_stage: str = "paper_only"
    priority_tier: str = "lower_priority_than_live_strategies"


_CANARY_LANES: tuple[CanaryLaneSpec, ...] = (
    CanaryLaneSpec(
        lane_id="atpe_long_medium_high_canary",
        lane_name="ATPE Long Medium+High Canary",
        side="LONG",
        variant_id=PHASE3_BEST_LONG_VARIANT_ID,
        quality_buckets=frozenset({"MEDIUM", "HIGH"}),
        quality_bucket_policy="MEDIUM_HIGH_ONLY",
    ),
    CanaryLaneSpec(
        lane_id="atpe_short_high_only_canary",
        lane_name="ATPE Short High-Only Canary",
        side="SHORT",
        variant_id="trend_participation.failed_countertrend_resumption.short.active",
        quality_buckets=frozenset({"HIGH"}),
        quality_bucket_policy="HIGH_ONLY",
    ),
)


def atpe_runtime_lane_id(lane: CanaryLaneSpec, instrument: str) -> str:
    return f"{lane.lane_id}__{str(instrument).strip().upper()}"


def atpe_runtime_lane_name(lane: CanaryLaneSpec, instrument: str) -> str:
    return f"{lane.lane_name} / {str(instrument).strip().upper()}"


def run_phase5_canary_package(
    *,
    source_sqlite_path: Path,
    output_dir: Path,
    instruments: tuple[str, ...] = ("MES", "MNQ"),
    higher_priority_signals: Iterable[HigherPrioritySignal] = (),
    phase5_review_json_path: Path | None = None,
) -> dict[str, Path]:
    root = output_dir.resolve()
    root.mkdir(parents=True, exist_ok=True)
    lanes_dir = root / "lanes"
    lanes_dir.mkdir(parents=True, exist_ok=True)

    generated_at = datetime.now(timezone.utc).isoformat()
    kill_switch_path = root / "DISABLE_ACTIVE_TREND_PARTICIPATION_CANARY"
    kill_switch_active = kill_switch_path.exists()

    shared_start, shared_end = _shared_1m_coverage(sqlite_path=source_sqlite_path, instruments=instruments)
    scoped_start, scoped_end = _preferred_canary_window(
        shared_start=shared_start,
        shared_end=shared_end,
        phase5_review_json_path=phase5_review_json_path,
    )
    run = _build_canary_run(
        source_sqlite_path=source_sqlite_path,
        instruments=instruments,
        start_ts=scoped_start,
        end_ts=scoped_end,
        higher_priority_signals=higher_priority_signals,
    )
    variants_by_id = run["variants_by_id"]
    point_values = run["point_values"]
    bars_1m = run["bars_1m"]
    features = run["features"]

    package_rows: list[dict[str, Any]] = []
    for lane in _CANARY_LANES:
        for instrument in instruments:
            lane_id = atpe_runtime_lane_id(lane, instrument)
            lane_name = atpe_runtime_lane_name(lane, instrument)
            instrument_bars = [bar for bar in bars_1m if bar.instrument == instrument]
            instrument_features = [feature for feature in features if feature.instrument == instrument]
            atp_phase1_diagnostics = summarize_atp_state_diagnostics(instrument_features)
            latest_atp_state = latest_atp_state_summary(instrument_features[-1] if instrument_features else None)
            phase2_package = build_phase2_replay_package(
                feature_rows=instrument_features,
                bars_1m=instrument_bars,
                higher_priority_signals=higher_priority_signals,
                point_value=point_values[instrument],
            )
            phase3_package = build_phase3_replay_package(
                entry_states=phase2_package["entry_states"],
                bars_1m=instrument_bars,
                point_value=point_values[instrument],
                old_proxy_trade_count=len(phase2_package["shadow_trades"]),
            )
            latest_atp_entry_state = latest_atp_entry_state_summary(
                phase2_package["entry_states"][-1] if phase2_package["entry_states"] else None
            )
            latest_atp_timing_state = latest_atp_timing_state_summary(
                phase3_package["timing_states"][-1] if phase3_package["timing_states"] else None
            )
            decisions = [
                decision
                for decision in sorted(run["decisions_by_variant"].get(lane.variant_id, []), key=lambda item: item.decision_ts)
                if decision.instrument == instrument and decision.setup_quality_bucket in lane.quality_buckets
            ]
            trades, audits = backtest_decisions_with_audit(
                decisions=decisions,
                bars_1m=instrument_bars,
                variants_by_id={lane.variant_id: variants_by_id[lane.variant_id]},
                point_values={instrument: point_values[instrument]},
                include_shadow_only=False,
            )
            lane_dir = lanes_dir / lane_id
            lane_dir.mkdir(parents=True, exist_ok=True)
            processed_bars_path = _write_jsonl(
                lane_dir / "processed_bars.jsonl",
                [_processed_bar_row(bar=bar, lane_id=lane_id, lane_name=lane_name, experimental_status=lane.experimental_status) for bar in instrument_bars],
            )
            features_path = _write_jsonl(
                lane_dir / "features.jsonl",
                [_feature_row(feature=feature, lane_id=lane_id, lane_name=lane_name, experimental_status=lane.experimental_status) for feature in instrument_features],
            )
            signals_path = _write_jsonl(
                lane_dir / "signals.jsonl",
                [
                    _signal_row(
                        decision=decision,
                        lane=lane,
                        lane_id=lane_id,
                        lane_name=lane_name,
                        kill_switch_active=kill_switch_active,
                    )
                    for decision in decisions
                ],
            )
            trades_path = _write_jsonl(
                lane_dir / "trades.jsonl",
                [_trade_row(trade=trade, lane=lane, lane_id=lane_id, lane_name=lane_name) for trade in trades],
            )
            events_path = _write_jsonl(
                lane_dir / "events.jsonl",
                _lane_event_rows(
                    lane=lane,
                    lane_id=lane_id,
                    generated_at=generated_at,
                    trade_count=len(trades),
                    signal_count=len(decisions),
                    kill_switch_active=kill_switch_active,
                ),
            )
            operator_status_path = lane_dir / "operator_status.json"
            operator_status_payload = {
                "generated_at": generated_at,
                "lane_id": lane_id,
                "lane_name": lane_name,
                "instrument": instrument,
                "experimental_status": lane.experimental_status,
                "paper_only": True,
                "enabled": not kill_switch_active,
                "kill_switch_active": kill_switch_active,
                "kill_switch_path": str(kill_switch_path),
                "priority_tier": lane.priority_tier,
                "quality_bucket_policy": lane.quality_bucket_policy,
                "side": lane.side,
                "signal_count": len(decisions),
                "trade_count": len(trades),
                "latest_atp_state": latest_atp_state,
                "latest_atp_entry_state": latest_atp_entry_state,
                "latest_atp_timing_state": latest_atp_timing_state,
                "atp_phase1_diagnostics": atp_phase1_diagnostics,
                "atp_phase2_entry_diagnostics": phase2_package["diagnostics"],
                "atp_phase3_timing_diagnostics": phase3_package["diagnostics"],
                "notes": [
                    "Experimental paper canary only.",
                    "Lower priority than higher-priority live strategies.",
                    "Not approved for production alpha routing.",
                ],
            }
            operator_status_path.write_text(json.dumps(operator_status_payload, indent=2, sort_keys=True), encoding="utf-8")
            metrics = _metrics_payload(trades)
            row = {
                "lane_id": lane_id,
                "lane_name": lane_name,
                "display_name": lane_name,
                "instrument": instrument,
                "variant_id": lane.variant_id,
                "side": lane.side,
                "symbols": [instrument],
                "experimental_status": lane.experimental_status,
                "canary_stage": lane.canary_stage,
                "quality_bucket_policy": lane.quality_bucket_policy,
                "priority_tier": lane.priority_tier,
                "paper_only": True,
                "kill_switch_active": kill_switch_active,
                "kill_switch_path": str(kill_switch_path),
                "latest_atp_state": latest_atp_state,
                "latest_atp_entry_state": latest_atp_entry_state,
                "latest_atp_timing_state": latest_atp_timing_state,
                "atp_phase1_diagnostics": atp_phase1_diagnostics,
                "atp_phase2_entry_diagnostics": phase2_package["diagnostics"],
                "atp_phase3_timing_diagnostics": phase3_package["diagnostics"],
                "conflict_policy": "yield_to_higher_priority_live_strategies",
                "reentry_policy": "same_setup_live_reentry_disabled",
                "metrics": metrics,
                "cost_fragility": _cost_fragility_summary(trades),
                "execution_audit": [audit.__dict__ for audit in audits],
                "artifacts": {
                    "lane_dir": str(lane_dir),
                    "processed_bars": str(processed_bars_path),
                    "features": str(features_path),
                    "signals": str(signals_path),
                    "trades": str(trades_path),
                    "events": str(events_path),
                    "operator_status": str(operator_status_path),
                },
                "operator_summary": {
                    "what_it_is": (
                        "A paper-only directional canary for validating whether the narrower Active Trend Participation "
                        "Engine lane behaves sanely in dashboard and operator workflows."
                    ),
                    "what_it_is_not": (
                        "Not a production alpha strategy, not a source of live execution authority, and not a reason to "
                        "override higher-priority live strategies."
                    ),
                },
            }
            package_rows.append(row)

    summary_line = " | ".join(
        (
            f"{row['lane_name']}: paper_only {row['side']} {row['quality_bucket_policy']} "
            f"PF={row['metrics']['profit_factor']} net={row['metrics']['net_pnl_cash']}"
        )
        for row in package_rows
    )
    snapshot_payload = {
        "generated_at": generated_at,
        "module": "Active Trend Participation Engine",
        "status": "available",
        "scope_label": "Experimental paper canaries for Active Trend Participation Engine",
        "separation_note": "Canary metrics are isolated from approved production-strategy metrics.",
        "operator_summary_line": summary_line,
        "atp_phase1_diagnostics": summarize_atp_state_diagnostics(features),
        "kill_switch": {
            "path": str(kill_switch_path),
            "active": kill_switch_active,
            "operator_action": (
                f"Create {kill_switch_path.name} to halt canary paper participation and remove it to re-enable."
            ),
        },
        "rows": package_rows,
    }
    snapshot_json_path = root / "experimental_canaries_snapshot.json"
    snapshot_md_path = root / "experimental_canaries_snapshot.md"
    operator_summary_path = root / "operator_summary.md"
    canary_profile_path = root / "canary_profile.json"
    snapshot_json_path.write_text(json.dumps(snapshot_payload, indent=2, sort_keys=True), encoding="utf-8")
    snapshot_md_path.write_text(_render_snapshot_markdown(snapshot_payload), encoding="utf-8")
    operator_summary_path.write_text(_render_operator_summary(snapshot_payload), encoding="utf-8")
    canary_profile_path.write_text(
        json.dumps(
            {
                "generated_at": generated_at,
                "module": "Active Trend Participation Engine",
                "deployment_mode": "paper_only_experimental_canary",
                "source_sqlite_path": str(source_sqlite_path.resolve()),
                "coverage": {
                    "start_ts": scoped_start.isoformat(),
                    "end_ts": scoped_end.isoformat(),
                },
                "instruments": list(instruments),
                "lanes": package_rows,
                "kill_switch_path": str(kill_switch_path),
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return {
        "snapshot_json_path": snapshot_json_path,
        "snapshot_markdown_path": snapshot_md_path,
        "operator_summary_path": operator_summary_path,
        "canary_profile_path": canary_profile_path,
    }


def _preferred_canary_window(
    *,
    shared_start: datetime,
    shared_end: datetime,
    phase5_review_json_path: Path | None,
) -> tuple[datetime, datetime]:
    candidate_path = phase5_review_json_path or Path("outputs/reports/trend_participation_engine_phase5_review/phase5_review.json")
    if not candidate_path.exists():
        return shared_start, shared_end
    try:
        payload = json.loads(candidate_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return shared_start, shared_end
    review_windows = payload.get("review_windows") or []
    starts = []
    ends = []
    for row in review_windows:
        start_raw = row.get("start")
        end_raw = row.get("end")
        if not start_raw or not end_raw:
            continue
        starts.append(datetime.fromisoformat(str(start_raw)))
        ends.append(datetime.fromisoformat(str(end_raw)))
    if not starts or not ends:
        return shared_start, shared_end
    return max(shared_start, min(starts)), min(shared_end, max(ends))


def _build_canary_run(
    *,
    source_sqlite_path: Path,
    instruments: tuple[str, ...],
    start_ts: datetime,
    end_ts: datetime,
    higher_priority_signals: Iterable[HigherPrioritySignal],
) -> dict[str, Any]:
    selected_variant_ids = {lane.variant_id for lane in _CANARY_LANES}
    variants = [variant for variant in default_pattern_variants(profile="phase3_full") if variant.variant_id in selected_variant_ids]
    variants_by_id = {variant.variant_id: variant for variant in variants}
    decisions_by_variant: dict[str, list[SignalDecision]] = {variant_id: [] for variant_id in selected_variant_ids}
    bars_1m: list[Any] = []
    features_all: list[Any] = []

    for instrument in instruments:
        raw_1m = load_sqlite_bars(
            sqlite_path=source_sqlite_path,
            instrument=instrument,
            timeframe="1m",
            start_ts=start_ts,
            end_ts=end_ts,
        )
        normalized_1m, _ = normalize_and_check_bars(bars=raw_1m, timeframe="1m")
        raw_5m = load_sqlite_bars(
            sqlite_path=source_sqlite_path,
            instrument=instrument,
            timeframe="5m",
            start_ts=start_ts,
            end_ts=end_ts,
        )
        if not raw_5m and normalized_1m:
            raw_5m = resample_bars_from_1m(bars_1m=normalized_1m, target_timeframe="5m")
        normalized_5m, _ = normalize_and_check_bars(bars=raw_5m, timeframe="5m")
        if normalized_1m:
            first_1m_ts = normalized_1m[0].end_ts
            last_1m_ts = normalized_1m[-1].end_ts
            normalized_5m = [bar for bar in normalized_5m if first_1m_ts <= bar.end_ts <= last_1m_ts]
        features = build_feature_states(bars_5m=normalized_5m, bars_1m=normalized_1m)
        decisions = generate_signal_decisions(
            feature_rows=features,
            variants=variants,
            higher_priority_signals=higher_priority_signals,
        )
        bars_1m.extend(normalized_1m)
        features_all.extend(features)
        for decision in decisions:
            decisions_by_variant.setdefault(decision.variant_id, []).append(decision)

    return {
        "variants_by_id": variants_by_id,
        "decisions_by_variant": decisions_by_variant,
        "bars_1m": bars_1m,
        "features": features_all,
        "point_values": DEFAULT_POINT_VALUES,
    }


def _processed_bar_row(*, bar: Any, lane_id: str, lane_name: str, experimental_status: str) -> dict[str, Any]:
    return {
        "lane_id": lane_id,
        "lane_name": lane_name,
        "experimental_status": experimental_status,
        "paper_only": True,
        "symbol": bar.instrument,
        "timeframe": bar.timeframe,
        "start_ts": bar.start_ts.isoformat(),
        "end_ts": bar.end_ts.isoformat(),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "session_label": bar.session_label,
        "session_segment": bar.session_segment,
        "provenance": bar.provenance,
    }


def _feature_row(*, feature: Any, lane_id: str, lane_name: str, experimental_status: str) -> dict[str, Any]:
    return {
        "lane_id": lane_id,
        "lane_name": lane_name,
        "experimental_status": experimental_status,
        "paper_only": True,
        "symbol": feature.instrument,
        "feature_timestamp": feature.decision_ts.isoformat(),
        "session_date": feature.session_date.isoformat(),
        "session_label": feature.session_label,
        "session_segment": feature.session_segment,
        "trend_state": feature.trend_state,
        "pullback_state": feature.pullback_state,
        "expansion_state": feature.expansion_state,
        "momentum_persistence": feature.momentum_persistence,
        "bar_anatomy": feature.bar_anatomy,
        "volatility_bucket": feature.volatility_bucket,
        "regime_bucket": feature.regime_bucket,
        "mtf_agreement_state": feature.mtf_agreement_state,
        "direction_bias": feature.direction_bias,
        "atp_bias_state": feature.atp_bias_state,
        "atp_bias_score": feature.atp_bias_score,
        "atp_bias_reasons": list(feature.atp_bias_reasons),
        "atp_long_bias_blockers": list(feature.atp_long_bias_blockers),
        "atp_short_bias_blockers": list(feature.atp_short_bias_blockers),
        "atp_fast_ema": feature.atp_fast_ema,
        "atp_slow_ema": feature.atp_slow_ema,
        "atp_slow_ema_slope_norm": feature.atp_slow_ema_slope_norm,
        "atp_session_vwap": feature.atp_session_vwap,
        "atp_directional_persistence_score": feature.atp_directional_persistence_score,
        "atp_trend_extension_norm": feature.atp_trend_extension_norm,
        "atp_pullback_state": feature.atp_pullback_state,
        "atp_pullback_envelope_state": feature.atp_pullback_envelope_state,
        "atp_pullback_reason": feature.atp_pullback_reason,
        "atp_pullback_depth_points": feature.atp_pullback_depth_points,
        "atp_pullback_depth_score": feature.atp_pullback_depth_score,
        "atp_pullback_violence_score": feature.atp_pullback_violence_score,
        "atp_pullback_min_reset_depth": feature.atp_pullback_min_reset_depth,
        "atp_pullback_standard_depth": feature.atp_pullback_standard_depth,
        "atp_pullback_stretched_depth": feature.atp_pullback_stretched_depth,
        "atp_pullback_disqualify_depth": feature.atp_pullback_disqualify_depth,
        "atp_pullback_retracement_ratio": feature.atp_pullback_retracement_ratio,
        "atp_countertrend_velocity_norm": feature.atp_countertrend_velocity_norm,
        "atp_countertrend_range_expansion": feature.atp_countertrend_range_expansion,
        "atp_structure_damage": feature.atp_structure_damage,
        "atp_reference_displacement": feature.atp_reference_displacement,
    }


def _signal_row(
    *,
    decision: SignalDecision,
    lane: CanaryLaneSpec,
    lane_id: str,
    lane_name: str,
    kill_switch_active: bool,
) -> dict[str, Any]:
    allow_block_reason, override_reason, signal_passed_flag = _decision_policy_row(
        decision=decision,
        kill_switch_active=kill_switch_active,
    )
    return {
        "lane_id": lane_id,
        "lane_name": lane_name,
        "experimental_status": lane.experimental_status,
        "paper_only": True,
        "symbol": decision.instrument,
        "variant_id": decision.variant_id,
        "family": decision.family,
        "side": decision.side,
        "signal_timestamp": decision.decision_ts.isoformat(),
        "session_date": decision.session_date.isoformat(),
        "session_segment": decision.session_segment,
        "decision_id": decision.decision_id,
        "decision": "allowed" if signal_passed_flag else "blocked",
        "signal_passed_flag": signal_passed_flag,
        "paper_canary_eligible": signal_passed_flag,
        "live_eligible": False,
        "shadow_only": True,
        "quality_bucket_policy": lane.quality_bucket_policy,
        "quality_bucket": decision.setup_quality_bucket,
        "setup_quality_score": decision.setup_quality_score,
        "conflict_outcome": decision.conflict_outcome.value,
        "allow_block_reason": allow_block_reason,
        "override_reason": override_reason,
        "rejection_reason_code": None if signal_passed_flag else (override_reason or allow_block_reason),
        "block_reason": None if signal_passed_flag else (override_reason or decision.block_reason or allow_block_reason),
        "priority_tier": lane.priority_tier,
        "lower_priority_policy": "yield_to_higher_priority_live_strategies",
        "setup_signature": decision.setup_signature,
        "setup_state_signature": decision.setup_state_signature,
        "feature_snapshot": decision.feature_snapshot,
    }


def _trade_row(*, trade: Any, lane: CanaryLaneSpec, lane_id: str, lane_name: str) -> dict[str, Any]:
    return {
        "lane_id": lane_id,
        "lane_name": lane_name,
        "experimental_status": lane.experimental_status,
        "paper_only": True,
        "symbol": trade.instrument,
        "trade_id": f"{lane_id}:{trade.decision_id}:{trade.entry_ts.isoformat()}",
        "decision_id": trade.decision_id,
        "direction": trade.side,
        "quality_bucket_policy": lane.quality_bucket_policy,
        "quality_bucket": trade.setup_quality_bucket,
        "entry_timestamp": trade.entry_ts.isoformat(),
        "exit_timestamp": trade.exit_ts.isoformat(),
        "entry_price": trade.entry_price,
        "exit_price": trade.exit_price,
        "realized_pnl": round(trade.pnl_cash, 4),
        "gross_pnl": round(trade.gross_pnl_cash, 4),
        "net_pnl": round(trade.pnl_cash, 4),
        "fees_paid": round(trade.fees_paid, 4),
        "slippage_cost": round(trade.slippage_cost, 4),
        "hold_minutes": round(trade.hold_minutes, 4),
        "exit_reason": trade.exit_reason,
        "setup_family": trade.family,
        "conflict_outcome": trade.conflict_outcome.value,
        "paper_execution_mode": "experimental_canary",
        "is_reentry": trade.is_reentry,
        "reentry_type": trade.reentry_type,
        "stopout": trade.stopout,
    }


def _lane_event_rows(
    *,
    lane: CanaryLaneSpec,
    lane_id: str,
    generated_at: str,
    trade_count: int,
    signal_count: int,
    kill_switch_active: bool,
) -> list[dict[str, Any]]:
    return [
        {
            "timestamp": generated_at,
            "lane_id": lane_id,
            "event_type": "PACKAGE_CREATED",
            "experimental_status": lane.experimental_status,
            "details": (
                "Experimental paper canary package created. Live execution remains disabled; only paper validation is in scope."
            ),
        },
        {
            "timestamp": generated_at,
            "lane_id": lane_id,
            "event_type": "CANARY_POLICY",
            "experimental_status": lane.experimental_status,
            "signal_count": signal_count,
            "trade_count": trade_count,
            "kill_switch_active": kill_switch_active,
            "details": (
                "Lower-priority conflict behavior is preserved. Higher-priority live strategies override this lane whenever overlap exists."
            ),
        },
    ]


def _decision_policy_row(*, decision: SignalDecision, kill_switch_active: bool) -> tuple[str, str | None, bool]:
    if kill_switch_active:
        return "blocked_kill_switch", "canary_kill_switch_active", False
    if decision.conflict_outcome == ConflictOutcome.NO_CONFLICT:
        return "allowed_no_conflict", "paper_only_experimental_canary", True
    if decision.conflict_outcome == ConflictOutcome.AGREEMENT:
        return "blocked_higher_priority_agreement", decision.block_reason or "agreement", False
    if decision.conflict_outcome == ConflictOutcome.SOFT_CONFLICT:
        return "blocked_soft_conflict", decision.block_reason or "soft_conflict", False
    return "blocked_hard_conflict_cooldown", decision.block_reason or "hard_conflict_cooldown", False


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")
    return path


def _render_snapshot_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Active Trend Participation Engine Experimental Canary Package",
        "",
        payload["scope_label"],
        "",
        f"- Separation: {payload['separation_note']}",
        f"- Kill switch: `{payload['kill_switch']['path']}`",
        f"- Kill switch active: `{payload['kill_switch']['active']}`",
        "",
        "## Lanes",
    ]
    for row in payload.get("rows", []):
        metrics = row["metrics"]
        lines.extend(
            [
                f"- `{row['lane_name']}`",
                f"  side=`{row['side']}` quality_policy=`{row['quality_bucket_policy']}` "
                f"trades=`{metrics['total_trades']}` pf=`{metrics['profit_factor']}` net=`{metrics['net_pnl_cash']}`",
            ]
        )
    return "\n".join(lines) + "\n"


def _render_operator_summary(payload: dict[str, Any]) -> str:
    lines = [
        "# Operator Summary",
        "",
        "## What This Canary Is For",
        "- Paper-only validation of the narrower Active Trend Participation Engine lanes.",
        "- Checking signal quality, conflict behavior, logging completeness, and dashboard/operator readability.",
        "- Observing whether the long MEDIUM/HIGH lane and short HIGH-only lane stay coherent outside pure research reports.",
        "",
        "## What This Canary Is Not For",
        "- Not a production alpha strategy.",
        "- Not a live execution authority.",
        "- Not a reason to override higher-priority live strategies.",
        "",
        "## Control",
        f"- Kill switch path: `{payload['kill_switch']['path']}`",
        f"- Action: {payload['kill_switch']['operator_action']}",
    ]
    return "\n".join(lines) + "\n"
