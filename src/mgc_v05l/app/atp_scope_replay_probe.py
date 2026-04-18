"""Replay a cached ATP scope bundle under isolated variant overrides."""

from __future__ import annotations

from dataclasses import replace
import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

from ..research.trend_participation.atp_promotion_add_review import (
    DEFAULT_FEE_PER_ADD,
    PromotionAddCandidate,
    default_atp_promotion_add_candidates,
    evaluate_promotion_add_candidate,
)
from ..research.trend_participation.backtest import summarize_performance
from ..research.trend_participation.models import AtpEntryState, AtpTimingState, ResearchBar, TradeRecord
from ..research.trend_participation.outcome_engine import trade_records_to_retest_rows
from ..research.trend_participation.phase2_continuation import atp_phase2_variant
from ..research.trend_participation.phase3_timing import ATP_REPLAY_EXIT_POLICY_FIXED_TARGET, simulate_timed_entries
from ..research.trend_participation.substrate import _entry_state_from_row, _timing_state_from_row, _trade_record_from_row
from . import strategy_universe_retest as retest
from .atp_loosened_history_publish import _latest_manifest_path, _load_manifest_study_rows, _merge_study_rows
from .session_phase_labels import label_session_phase
from .strategy_risk_shape_lab import _baseline_metrics, _load_strategy_study, _parse_trade_timestamp, _trade_summary_rows

REPO_ROOT = Path.cwd()
DEFAULT_PROBE_REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "atp_scope_replay_probe"
DEFAULT_HISTORICAL_PLAYBACK_DIR = REPO_ROOT / "outputs" / "historical_playback"


@dataclass(frozen=True)
class ScopeReplayProbeBundle:
    manifest: dict[str, Any]
    entry_states: list[AtpEntryState]
    timing_states: list[AtpTimingState]
    trade_records: list[TradeRecord]


@dataclass(frozen=True)
class ConfirmationSizingProfile:
    probe_size_fraction: float
    confirmation_add_size_fraction: float
    confirmation_add_candidate: PromotionAddCandidate | None


@dataclass(frozen=True)
class PreConfirmationRiskProfile:
    pre_confirmation_stop_r_multiple: float
    confirmation_release_candidate: PromotionAddCandidate


def load_scope_replay_probe_bundle(manifest_path: Path) -> ScopeReplayProbeBundle:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entry_path = Path(manifest["datasets"]["entry_states"]["jsonl_path"])
    timing_path = Path(manifest["datasets"]["timing_states"]["jsonl_path"])
    trade_path = Path(manifest["datasets"]["trade_records"]["jsonl_path"])
    entry_states = [
        _entry_state_from_row(json.loads(line))
        for line in entry_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    timing_states = [
        _timing_state_from_row(json.loads(line))
        for line in timing_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    trade_records = [
        _trade_record_from_row(json.loads(line))
        for line in trade_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    return ScopeReplayProbeBundle(
        manifest=manifest,
        entry_states=entry_states,
        timing_states=_enrich_timing_states_with_entry_signatures(
            timing_states=timing_states,
            entry_states=entry_states,
        ),
        trade_records=trade_records,
    )


def _enrich_timing_states_with_entry_signatures(
    *,
    timing_states: Sequence[AtpTimingState],
    entry_states: Sequence[AtpEntryState],
) -> list[AtpTimingState]:
    entry_by_key = {
        (row.instrument, row.decision_ts, row.side, row.family_name): row
        for row in entry_states
    }
    enriched: list[AtpTimingState] = []
    for state in timing_states:
        entry = entry_by_key.get((state.instrument, state.decision_ts, state.side, state.family_name))
        snapshot = dict(state.feature_snapshot or {})
        if entry is not None:
            snapshot.setdefault("setup_signature", entry.setup_signature)
            snapshot.setdefault("setup_state_signature", entry.setup_state_signature)
        enriched.append(
            AtpTimingState(
                **{
                    **state.__dict__,
                    "feature_snapshot": snapshot,
                }
            )
        )
    return enriched


def _merged_replay_intervals(
    *,
    timing_states: Sequence[AtpTimingState],
    window_minutes: int,
) -> list[tuple[datetime, datetime]]:
    intervals: list[tuple[datetime, datetime]] = []
    for state in timing_states:
        if not state.executable_entry:
            continue
        anchor = state.entry_ts or state.decision_ts
        start = min(state.decision_ts, anchor) - timedelta(minutes=1)
        end = anchor + timedelta(minutes=max(window_minutes, 1))
        intervals.append((start, end))
    intervals.sort()
    merged: list[tuple[datetime, datetime]] = []
    for start, end in intervals:
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
            continue
        previous_start, previous_end = merged[-1]
        merged[-1] = (previous_start, max(previous_end, end))
    return merged


def _merged_trade_intervals(
    *,
    trades: Sequence[TradeRecord],
) -> list[tuple[datetime, datetime]]:
    intervals = [
        (trade.entry_ts - timedelta(minutes=1), trade.exit_ts)
        for trade in trades
    ]
    intervals.sort()
    merged: list[tuple[datetime, datetime]] = []
    for start, end in intervals:
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
            continue
        previous_start, previous_end = merged[-1]
        merged[-1] = (previous_start, max(previous_end, end))
    return merged


def _load_bars_for_intervals(
    *,
    sqlite_path: Path,
    symbol: str,
    intervals: Sequence[tuple[datetime, datetime]],
) -> list[ResearchBar]:
    connection = sqlite3.connect(sqlite_path)
    connection.row_factory = sqlite3.Row
    try:
        bars: list[ResearchBar] = []
        for start, end in intervals:
            rows = connection.execute(
                """
                select symbol, timeframe, start_ts, end_ts, open, high, low, close, volume
                from bars
                where symbol = ? and timeframe = '1m' and end_ts >= ? and end_ts <= ?
                order by end_ts asc
                """,
                (symbol, start.isoformat(), end.isoformat()),
            ).fetchall()
            for row in rows:
                end_ts = datetime.fromisoformat(str(row["end_ts"]))
                bars.append(
                    ResearchBar(
                        instrument=str(row["symbol"]).upper(),
                        timeframe="1m",
                        start_ts=datetime.fromisoformat(str(row["start_ts"])),
                        end_ts=end_ts,
                        open=float(row["open"]),
                        high=float(row["high"]),
                        low=float(row["low"]),
                        close=float(row["close"]),
                        volume=int(row["volume"]),
                        session_label=label_session_phase(end_ts),
                        session_segment=label_session_phase(end_ts).split("_", 1)[0],
                        source="sqlite",
                    )
                )
    finally:
        connection.close()
    deduped = {(bar.instrument, bar.end_ts): bar for bar in bars}
    return sorted(deduped.values(), key=lambda item: (item.instrument, item.end_ts))


def _render_probe_markdown(payload: dict[str, Any]) -> str:
    sizing_profile = payload.get("confirmation_sizing_profile") or {}
    risk_profile = payload.get("pre_confirmation_risk_profile") or {}
    return "\n".join(
        [
            "# ATP Scope Replay Probe",
            "",
            f"- Scope bundle: `{payload['scope_bundle_manifest']}`",
            f"- Symbol: `{payload['symbol']}`",
            f"- Exit policy: `{payload['exit_policy']}`",
            f"- Variant overrides: `{json.dumps(payload.get('variant_overrides') or {}, sort_keys=True)}`",
            f"- Pre-confirmation risk profile: `{json.dumps(risk_profile, sort_keys=True)}`",
            f"- Confirmation sizing profile: `{json.dumps(sizing_profile, sort_keys=True)}`",
            f"- Loaded bars: `{payload['bars_loaded']}`",
            f"- Merged replay intervals: `{payload['merged_intervals']}`",
            f"- Trade count: `{payload['trade_count']}`",
            f"- Net P&L: `{payload['net_pnl']}`",
            f"- Profit factor: `{payload['profit_factor']}`",
            f"- Max drawdown: `{payload['max_drawdown']}`",
            f"- Re-entry trades: `{payload['reentry_trade_count']}`",
            f"- Pre-confirmation tightened stops: `{payload.get('pre_confirmation_stopout_trade_count', 0)}`",
            f"- Confirmation adds: `{payload.get('confirmation_add_trade_count', 0)}`",
            f"- Confirmation add net contribution: `{payload.get('confirmation_add_net_pnl', 0.0)}`",
            "",
        ]
    )


def _evaluate_scope_probe(
    *,
    scope_bundle_manifest: Path,
    exit_policy: str,
    variant_overrides: dict[str, Any] | None,
    pre_confirmation_stop_r_multiple: float | None,
    pre_confirmation_release_candidate_id: str | None,
    probe_size_fraction: float,
    confirmation_add_candidate_id: str | None,
    confirmation_add_size_fraction: float,
    point_value_override: float | None,
) -> tuple[dict[str, Any], list[TradeRecord], ScopeReplayProbeBundle]:
    bundle = load_scope_replay_probe_bundle(scope_bundle_manifest)
    symbol = str(bundle.manifest["symbol"]).upper()
    source_db = Path(bundle.manifest["source_db"])
    point_value = float(point_value_override if point_value_override is not None else bundle.manifest["point_value"])
    normalized_overrides = dict(variant_overrides or {})
    variant = atp_phase2_variant("LONG", variant_overrides=normalized_overrides or None)
    can_use_bundle_trade_records = (
        not normalized_overrides
        and str(bundle.manifest.get("exit_policy") or "") == exit_policy
    )
    intervals = (
        _merged_trade_intervals(trades=bundle.trade_records)
        if can_use_bundle_trade_records
        else _merged_replay_intervals(
            timing_states=bundle.timing_states,
            window_minutes=max(int(variant.max_hold_bars_1m), 1) + 2,
        )
    )
    bars = _load_bars_for_intervals(
        sqlite_path=source_db,
        symbol=symbol,
        intervals=intervals,
    )
    trades = (
        list(bundle.trade_records)
        if can_use_bundle_trade_records
        else simulate_timed_entries(
            timing_states=bundle.timing_states,
            bars_1m=bars,
            point_value=point_value,
            variant=variant,
            exit_policy=exit_policy,
            variant_overrides=normalized_overrides or None,
        )
    )
    risk_profile: PreConfirmationRiskProfile | None = None
    if pre_confirmation_stop_r_multiple is not None and pre_confirmation_release_candidate_id:
        risk_profile = PreConfirmationRiskProfile(
            pre_confirmation_stop_r_multiple=float(pre_confirmation_stop_r_multiple),
            confirmation_release_candidate=_candidate_by_id(pre_confirmation_release_candidate_id),
        )
    adjusted_for_risk, risk_summary = _apply_pre_confirmation_risk_profile(
        trades=trades,
        bars_1m=bars,
        point_value=point_value,
        profile=risk_profile,
    )
    confirmation_profile: ConfirmationSizingProfile | None = None
    if confirmation_add_candidate_id:
        confirmation_profile = ConfirmationSizingProfile(
            probe_size_fraction=probe_size_fraction,
            confirmation_add_size_fraction=confirmation_add_size_fraction,
            confirmation_add_candidate=_candidate_by_id(confirmation_add_candidate_id),
        )
    elif abs(probe_size_fraction - 1.0) > 1e-9:
        confirmation_profile = ConfirmationSizingProfile(
            probe_size_fraction=probe_size_fraction,
            confirmation_add_size_fraction=0.0,
            confirmation_add_candidate=None,
        )
    adjusted_trades, sizing_summary = _apply_confirmation_sizing_profile(
        trades=adjusted_for_risk,
        bars_1m=bars,
        point_value=point_value,
        profile=confirmation_profile,
    )
    performance = summarize_performance(adjusted_trades)
    reentry_type_counts: dict[str, int] = defaultdict(int)
    for trade in adjusted_trades:
        if trade.is_reentry:
            reentry_type_counts[trade.reentry_type] += 1
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "scope_bundle_manifest": str(scope_bundle_manifest),
        "symbol": symbol,
        "source_db": str(source_db),
        "exit_policy": exit_policy,
        "variant_overrides": normalized_overrides,
        "trade_source": "bundle_trade_records" if can_use_bundle_trade_records else "timing_replay",
        "pre_confirmation_risk_profile": {
            "pre_confirmation_stop_r_multiple": pre_confirmation_stop_r_multiple,
            "confirmation_release_candidate_id": pre_confirmation_release_candidate_id,
        },
        "confirmation_sizing_profile": {
            "probe_size_fraction": probe_size_fraction,
            "confirmation_add_candidate_id": confirmation_add_candidate_id,
            "confirmation_add_size_fraction": confirmation_add_size_fraction,
        },
        "bars_loaded": len(bars),
        "merged_intervals": len(intervals),
        "trade_count": performance.trade_count,
        "net_pnl": round(performance.net_pnl_cash, 6),
        "profit_factor": round(performance.profit_factor, 6),
        "max_drawdown": round(performance.max_drawdown, 6),
        "average_trade": round(performance.expectancy, 6),
        "long_trade_count": performance.long_trade_count,
        "short_trade_count": performance.short_trade_count,
        "reentry_trade_count": performance.reentry_trade_count,
        "reentry_type_counts": dict(sorted(reentry_type_counts.items())),
        "pre_confirmation_stopout_trade_count": risk_summary["pre_confirmation_stopout_trade_count"],
        "pre_confirmation_stopout_net_delta": risk_summary["pre_confirmation_stopout_net_delta"],
        "confirmation_add_trade_count": sizing_summary["confirmation_add_trade_count"],
        "confirmation_add_net_pnl": sizing_summary["confirmation_add_net_pnl"],
        "confirmation_add_gross_pnl": sizing_summary.get("confirmation_add_gross_pnl", 0.0),
        "confirmation_add_fees": sizing_summary.get("confirmation_add_fees", 0.0),
    }
    return payload, adjusted_trades, bundle


def _trade_window_bars(
    *,
    trade: TradeRecord,
    bars_1m: Sequence[ResearchBar],
    bars_by_end_ts: dict[datetime, int],
) -> list[ResearchBar]:
    start_index = bars_by_end_ts.get(trade.entry_ts)
    end_index = bars_by_end_ts.get(trade.exit_ts)
    if start_index is None or end_index is None or end_index <= start_index:
        return []
    return list(bars_1m[start_index + 1 : end_index + 1])


def _candidate_by_id(candidate_id: str) -> PromotionAddCandidate:
    for candidate in default_atp_promotion_add_candidates():
        if candidate.candidate_id == candidate_id:
            return candidate
    raise ValueError(f"Unknown ATP confirmation add candidate: {candidate_id}")


def _apply_pre_confirmation_risk_profile(
    *,
    trades: Sequence[TradeRecord],
    bars_1m: Sequence[ResearchBar],
    point_value: float,
    profile: PreConfirmationRiskProfile | None,
) -> tuple[list[TradeRecord], dict[str, Any]]:
    if profile is None:
        return list(trades), {
            "applied": False,
            "pre_confirmation_stop_r_multiple": None,
            "confirmation_release_candidate_id": None,
            "pre_confirmation_stopout_trade_count": 0,
            "pre_confirmation_stopout_net_delta": 0.0,
        }

    bars_by_end_ts = {bar.end_ts: index for index, bar in enumerate(bars_1m)}
    adjusted_trades: list[TradeRecord] = []
    stopout_trade_count = 0
    stopout_net_delta = 0.0
    for trade in trades:
        if trade.side != "LONG":
            adjusted_trades.append(trade)
            continue
        trade_bars = _trade_window_bars(
            trade=trade,
            bars_1m=bars_1m,
            bars_by_end_ts=bars_by_end_ts,
        )
        confirmation_result = evaluate_promotion_add_candidate(
            trade=trade,
            minute_bars=trade_bars,
            candidate=profile.confirmation_release_candidate,
            point_value=point_value,
        )
        confirmation_ts = (
            _parse_trade_timestamp(str(confirmation_result.get("add_entry_ts")))
            if confirmation_result.get("added")
            else None
        )
        initial_risk = max(float(trade.entry_price) - float(trade.stop_price), 1e-9)
        tightened_raw_stop = float(trade.entry_price) - (initial_risk * float(profile.pre_confirmation_stop_r_multiple))
        tightened_exit_price = tightened_raw_stop - float(trade.slippage_cost or 0.0) / max(point_value, 1e-9)
        early_exit_bar: ResearchBar | None = None
        for bar in trade_bars:
            if confirmation_ts is not None and bar.end_ts >= confirmation_ts:
                break
            if float(bar.low) <= tightened_raw_stop:
                early_exit_bar = bar
                break
        if early_exit_bar is None:
            adjusted_trades.append(trade)
            continue
        stopout_trade_count += 1
        gross_pnl_points = tightened_raw_stop - float(trade.entry_price)
        pnl_points = tightened_exit_price - float(trade.entry_price)
        gross_pnl_cash = gross_pnl_points * point_value
        pnl_cash = pnl_points * point_value - float(trade.fees_paid)
        stopout_net_delta += pnl_cash - float(trade.pnl_cash)
        hold_minutes = max((early_exit_bar.end_ts - trade.entry_ts).total_seconds() / 60.0, 1.0)
        adjusted_trades.append(
            replace(
                trade,
                exit_ts=early_exit_bar.end_ts,
                exit_price=tightened_exit_price,
                stop_price=tightened_raw_stop,
                target_price=None,
                pnl_points=pnl_points,
                gross_pnl_cash=gross_pnl_cash,
                pnl_cash=pnl_cash,
                mfe_points=min(float(trade.mfe_points), max(float(early_exit_bar.high) - float(trade.entry_price), 0.0)),
                mae_points=max(float(trade.entry_price) - float(early_exit_bar.low), 0.0),
                bars_held_1m=max(int(hold_minutes), 1),
                hold_minutes=float(hold_minutes),
                exit_reason="pre_confirmation_tight_stop",
                stopout=True,
            )
        )

    return adjusted_trades, {
        "applied": True,
        "pre_confirmation_stop_r_multiple": profile.pre_confirmation_stop_r_multiple,
        "confirmation_release_candidate_id": profile.confirmation_release_candidate.candidate_id,
        "pre_confirmation_stopout_trade_count": stopout_trade_count,
        "pre_confirmation_stopout_net_delta": round(stopout_net_delta, 6),
    }


def _apply_confirmation_sizing_profile(
    *,
    trades: Sequence[TradeRecord],
    bars_1m: Sequence[ResearchBar],
    point_value: float,
    profile: ConfirmationSizingProfile | None,
) -> tuple[list[TradeRecord], dict[str, Any]]:
    if profile is None:
        return list(trades), {
            "applied": False,
            "probe_size_fraction": 1.0,
            "confirmation_add_size_fraction": 0.0,
            "confirmation_add_candidate_id": None,
            "confirmation_add_trade_count": 0,
            "confirmation_add_net_pnl": 0.0,
        }

    bars_by_end_ts = {bar.end_ts: index for index, bar in enumerate(bars_1m)}
    adjusted_trades: list[TradeRecord] = []
    add_trade_count = 0
    add_net_total = 0.0
    add_gross_total = 0.0
    add_fee_total = 0.0
    for trade in trades:
        scaled_trade = replace(
            trade,
            gross_pnl_cash=float(trade.gross_pnl_cash) * profile.probe_size_fraction,
            pnl_cash=float(trade.pnl_cash) * profile.probe_size_fraction,
            fees_paid=float(trade.fees_paid) * profile.probe_size_fraction,
            slippage_cost=float(trade.slippage_cost) * profile.probe_size_fraction,
        )
        trade_bars = _trade_window_bars(
            trade=trade,
            bars_1m=bars_1m,
            bars_by_end_ts=bars_by_end_ts,
        )
        add_result = evaluate_promotion_add_candidate(
            trade=trade,
            minute_bars=trade_bars,
            candidate=profile.confirmation_add_candidate,
            point_value=point_value,
        ) if (
            trade.side == "LONG"
            and profile.confirmation_add_candidate is not None
            and profile.confirmation_add_size_fraction > 0.0
        ) else {"added": False}
        if add_result.get("added"):
            add_trade_count += 1
            add_points = float(add_result.get("add_pnl_points") or 0.0)
            add_gross_cash = add_points * point_value * profile.confirmation_add_size_fraction
            add_fee_cash = DEFAULT_FEE_PER_ADD * profile.confirmation_add_size_fraction
            add_net_cash = add_gross_cash - add_fee_cash
            add_net_total += add_net_cash
            add_gross_total += add_gross_cash
            add_fee_total += add_fee_cash
            scaled_trade = replace(
                scaled_trade,
                gross_pnl_cash=float(scaled_trade.gross_pnl_cash) + add_gross_cash,
                pnl_cash=float(scaled_trade.pnl_cash) + add_net_cash,
                fees_paid=float(scaled_trade.fees_paid) + add_fee_cash,
            )
        adjusted_trades.append(scaled_trade)

    return adjusted_trades, {
        "applied": True,
        "probe_size_fraction": profile.probe_size_fraction,
        "confirmation_add_size_fraction": profile.confirmation_add_size_fraction,
        "confirmation_add_candidate_id": (
            profile.confirmation_add_candidate.candidate_id
            if profile.confirmation_add_candidate is not None
            else None
        ),
        "confirmation_add_trade_count": add_trade_count,
        "confirmation_add_net_pnl": round(add_net_total, 6),
        "confirmation_add_gross_pnl": round(add_gross_total, 6),
        "confirmation_add_fees": round(add_fee_total, 6),
    }


def run_atp_scope_replay_probe(
    *,
    scope_bundle_manifest: Path,
    exit_policy: str = ATP_REPLAY_EXIT_POLICY_FIXED_TARGET,
    variant_overrides: dict[str, Any] | None = None,
    pre_confirmation_stop_r_multiple: float | None = None,
    pre_confirmation_release_candidate_id: str | None = None,
    probe_size_fraction: float = 1.0,
    confirmation_add_candidate_id: str | None = None,
    confirmation_add_size_fraction: float = 0.0,
    point_value_override: float | None = None,
    output_path: Path | None = None,
    markdown_output_path: Path | None = None,
) -> dict[str, Any]:
    payload, _, _ = _evaluate_scope_probe(
        scope_bundle_manifest=scope_bundle_manifest,
        exit_policy=exit_policy,
        variant_overrides=variant_overrides,
        pre_confirmation_stop_r_multiple=pre_confirmation_stop_r_multiple,
        pre_confirmation_release_candidate_id=pre_confirmation_release_candidate_id,
        probe_size_fraction=probe_size_fraction,
        confirmation_add_candidate_id=confirmation_add_candidate_id,
        confirmation_add_size_fraction=confirmation_add_size_fraction,
        point_value_override=point_value_override,
    )
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if markdown_output_path is not None:
        markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_output_path.write_text(_render_probe_markdown(payload), encoding="utf-8")
    return payload


def publish_atp_scope_replay_probe_study(
    *,
    source_study_json_path: Path,
    scope_bundle_manifest: Path,
    exit_policy: str = ATP_REPLAY_EXIT_POLICY_FIXED_TARGET,
    variant_overrides: dict[str, Any] | None = None,
    pre_confirmation_stop_r_multiple: float | None = None,
    pre_confirmation_release_candidate_id: str | None = None,
    probe_size_fraction: float = 1.0,
    confirmation_add_candidate_id: str | None = None,
    confirmation_add_size_fraction: float = 0.0,
    point_value_override: float | None = None,
    study_suffix: str = "_probe_confirmation_v1",
    label_suffix: str = " [Probe Confirmation v1]",
    report_dir: Path = DEFAULT_PROBE_REPORT_DIR,
    historical_playback_dir: Path = DEFAULT_HISTORICAL_PLAYBACK_DIR,
) -> dict[str, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    historical_playback_dir.mkdir(parents=True, exist_ok=True)
    source_payload = _load_strategy_study(source_study_json_path)
    source_summary = dict(source_payload.get("summary") or {})
    baseline_trades = [
        dict(row)
        for row in list(source_summary.get("closed_trade_breakdown") or [])
    ]
    probe_payload, adjusted_trades, _bundle = _evaluate_scope_probe(
        scope_bundle_manifest=scope_bundle_manifest,
        exit_policy=exit_policy,
        variant_overrides=variant_overrides,
        pre_confirmation_stop_r_multiple=pre_confirmation_stop_r_multiple,
        pre_confirmation_release_candidate_id=pre_confirmation_release_candidate_id,
        probe_size_fraction=probe_size_fraction,
        confirmation_add_candidate_id=confirmation_add_candidate_id,
        confirmation_add_size_fraction=confirmation_add_size_fraction,
        point_value_override=point_value_override,
    )
    adjusted_trade_rows = [
        {
            key: value
            for key, value in row.items()
            if key != "trade_record"
        }
        for row in trade_records_to_retest_rows(adjusted_trades)
    ]
    derived_summary = _trade_summary_rows(adjusted_trade_rows)
    payload = json.loads(json.dumps(source_payload))
    original_strategy_id = str(
        payload.get("standalone_strategy_id")
        or dict(payload.get("meta") or {}).get("strategy_id")
        or source_study_json_path.stem
    )
    new_strategy_id = f"{original_strategy_id}{study_suffix}"
    payload["generated_at"] = datetime.now(UTC).isoformat()
    payload["standalone_strategy_id"] = new_strategy_id
    meta = dict(payload.get("meta") or {})
    meta["study_id"] = new_strategy_id
    meta["strategy_id"] = new_strategy_id
    meta["display_name"] = f"{str(meta.get('display_name') or original_strategy_id)}{label_suffix}"
    meta["truth_provenance"] = {
        **dict(meta.get("truth_provenance") or {}),
        "scope_replay_probe_source_study": str(source_study_json_path),
        "scope_replay_probe_scope_bundle": str(scope_bundle_manifest),
        "scope_replay_probe_derived": True,
        "scope_replay_probe_exit_policy": exit_policy,
        "scope_replay_probe_variant_overrides": dict(variant_overrides or {}),
        "scope_replay_probe_pre_confirmation_stop_r_multiple": pre_confirmation_stop_r_multiple,
        "scope_replay_probe_pre_confirmation_release_candidate_id": pre_confirmation_release_candidate_id,
        "scope_replay_probe_probe_size_fraction": probe_size_fraction,
        "scope_replay_probe_confirmation_add_candidate_id": confirmation_add_candidate_id,
        "scope_replay_probe_confirmation_add_size_fraction": confirmation_add_size_fraction,
    }
    payload["meta"] = meta
    payload["summary"] = derived_summary
    payload["trade_events"] = []
    payload["pnl_points"] = []
    payload["lifecycle_records"] = []
    payload["authoritative_trade_lifecycle_records"] = []
    path_pair = retest._write_study_payload(
        payload=payload,
        artifact_prefix=f"historical_playback_{new_strategy_id}",
        historical_playback_dir=historical_playback_dir,
    )
    study_row = {
        "strategy_id": new_strategy_id,
        "symbol": str(payload.get("symbol") or meta.get("symbol") or ""),
        "label": meta["display_name"],
        "study_mode": meta.get("study_mode"),
        "execution_model": meta.get("execution_model"),
        "summary_payload": derived_summary,
        "strategy_study_json_path": str(path_pair["json"]),
        "strategy_study_markdown_path": str(path_pair["markdown"]),
    }
    latest_manifest = _latest_manifest_path(historical_playback_dir)
    existing_studies = _load_manifest_study_rows(latest_manifest) if latest_manifest is not None else []
    merged_studies = _merge_study_rows(existing_studies=existing_studies, new_studies=[study_row])
    manifest_run_stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
    manifest_path = retest._write_historical_playback_manifest(
        studies=merged_studies,
        run_stamp=manifest_run_stamp,
        historical_playback_dir=historical_playback_dir,
        shard_config=retest.RetestShardConfig(),
    )
    report_payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "source_study_json_path": str(source_study_json_path),
        "scope_bundle_manifest": str(scope_bundle_manifest),
        "published_strategy_id": new_strategy_id,
        "published_strategy_study_json_path": str(path_pair["json"]),
        "published_strategy_study_markdown_path": str(path_pair["markdown"]),
        "historical_playback_manifest": str(manifest_path),
        "baseline": _baseline_metrics(baseline_trades),
        "derived": {
            "trade_count": int(probe_payload.get("trade_count") or 0),
            "net_pnl": float(probe_payload.get("net_pnl") or 0.0),
            "max_drawdown": float(probe_payload.get("max_drawdown") or 0.0),
            "profit_factor": float(probe_payload.get("profit_factor") or 0.0),
        },
        "probe": probe_payload,
    }
    json_path = report_dir / f"{new_strategy_id}.publish.json"
    markdown_path = report_dir / f"{new_strategy_id}.publish.md"
    json_path.write_text(json.dumps(report_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_lines = [
        "# ATP Scope Replay Probe Publish",
        "",
        f"- Source study: `{source_study_json_path}`",
        f"- Published strategy id: `{new_strategy_id}`",
        f"- Manifest: `{manifest_path}`",
        f"- Baseline net / max DD: `{report_payload['baseline']['net_pnl']}` / `{report_payload['baseline']['max_drawdown']}`",
        f"- Derived net / max DD: `{report_payload['derived']['net_pnl']}` / `{report_payload['derived']['max_drawdown']}`",
        f"- Confirmation adds: `{probe_payload.get('confirmation_add_trade_count', 0)}`",
        f"- Confirmation add net: `{probe_payload.get('confirmation_add_net_pnl', 0.0)}`",
        "",
    ]
    markdown_path.write_text("\n".join(markdown_lines), encoding="utf-8")
    return {
        "report_json_path": json_path,
        "report_markdown_path": markdown_path,
        "historical_playback_manifest_path": manifest_path,
        "strategy_study_json_path": path_pair["json"],
        "strategy_study_markdown_path": path_pair["markdown"],
    }
