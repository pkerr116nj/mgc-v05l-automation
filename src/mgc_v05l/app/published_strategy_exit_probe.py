"""Reusable exit-only probe for published strategy-study artifacts.

This lets us compare alternate exit policies against a published study without
rerunning the original entry-generation pass. It is intentionally best-effort:
entries remain frozen from the published study, while the candidate exit path
is simulated from the embedded one-minute bars.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence

from . import strategy_universe_retest as retest
from .atp_loosened_history_publish import _latest_manifest_path, _load_manifest_study_rows, _merge_study_rows
from .strategy_risk_shape_lab import _trade_summary_rows

REPO_ROOT = Path.cwd()
DEFAULT_REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "published_strategy_exit_probe"
DEFAULT_HISTORICAL_PLAYBACK_DIR = REPO_ROOT / "outputs" / "historical_playback"


@dataclass(frozen=True)
class PublishedExitProbeSpec:
    policy_id: str = "checkpoint_no_traction_abort"
    label: str = "Checkpoint + No-Traction Abort"
    checkpoint_arm_r: float = 0.80
    checkpoint_lock_r: float = 0.35
    checkpoint_trail_r: float = 0.25
    no_traction_abort_bars: int = 2
    no_traction_min_favorable_r: float = 0.25
    risk_lookback_bars: int = 5
    risk_range_floor: float = 0.25


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    return datetime.fromisoformat(text)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _profit_factor(values: Sequence[float]) -> float | None:
    gross_wins = sum(value for value in values if value > 0.0)
    gross_losses = sum(-value for value in values if value < 0.0)
    if gross_losses <= 0:
        return None
    return round(gross_wins / gross_losses, 6)


def _max_drawdown(values: Sequence[float]) -> float:
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in values:
        cumulative += float(value)
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return round(max_drawdown, 6)


def _load_study(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _ordered_bars(study: dict[str, Any]) -> list[dict[str, Any]]:
    bars = [dict(row) for row in list(study.get("bars") or [])]
    return sorted(
        bars,
        key=lambda row: (
            _parse_iso(str(row.get("end_timestamp") or row.get("timestamp") or "")) or datetime.min.replace(tzinfo=UTC),
            str(row.get("bar_id") or ""),
        ),
    )


def _ordered_trades(study: dict[str, Any]) -> list[dict[str, Any]]:
    summary = dict(study.get("summary") or {})
    trades = [dict(row) for row in list(summary.get("closed_trade_breakdown") or [])]
    return sorted(
        trades,
        key=lambda row: (
            _parse_iso(str(row.get("entry_timestamp") or "")) or datetime.min.replace(tzinfo=UTC),
            str(row.get("trade_id") or ""),
        ),
    )


def _bar_index_by_timestamp(bars: Sequence[dict[str, Any]]) -> dict[str, int]:
    mapping: dict[str, int] = {}
    for index, bar in enumerate(bars):
        key = str(bar.get("end_timestamp") or bar.get("timestamp") or "").strip()
        if key:
            mapping[key] = index
    return mapping


def _rolling_risk_points(bars: Sequence[dict[str, Any]], *, entry_index: int, lookback_bars: int, floor: float) -> float:
    start_index = max(entry_index - max(lookback_bars - 1, 0), 0)
    window = bars[start_index : entry_index + 1]
    ranges = [
        max(_to_float(bar.get("high")) - _to_float(bar.get("low")), 0.0)
        for bar in window
    ]
    if not ranges:
        return float(floor)
    return max(sum(ranges) / float(len(ranges)), float(floor))


def _checkpoint_stop_price(
    *,
    side: str,
    current_stop: float,
    entry_price: float,
    risk_points: float,
    bar_low: float,
    bar_high: float,
    checkpoint_lock_r: float,
    checkpoint_trail_r: float,
) -> float:
    if side == "LONG":
        locked_profit_stop = entry_price + risk_points * checkpoint_lock_r
        structure_stop = bar_low - risk_points * checkpoint_trail_r
        return max(current_stop, locked_profit_stop, structure_stop)
    locked_profit_stop = entry_price - risk_points * checkpoint_lock_r
    structure_stop = bar_high + risk_points * checkpoint_trail_r
    return min(current_stop, locked_profit_stop, structure_stop)


def _baseline_summary(trades: Sequence[dict[str, Any]]) -> dict[str, Any]:
    pnl_values = [_to_float(trade.get("realized_pnl")) for trade in trades]
    return {
        "trade_count": len(trades),
        "net_pnl": round(sum(pnl_values), 6),
        "max_drawdown": _max_drawdown(pnl_values),
        "profit_factor": _profit_factor(pnl_values),
        "exit_reason_counts": _count_labels(str(trade.get("exit_reason") or "UNKNOWN") for trade in trades),
    }


def _count_labels(values: Sequence[str] | Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value or "UNKNOWN")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _simulate_candidate_trade(
    *,
    trade: dict[str, Any],
    bars: Sequence[dict[str, Any]],
    bar_index_lookup: dict[str, int],
    point_value: float,
    spec: PublishedExitProbeSpec,
) -> dict[str, Any] | None:
    entry_timestamp = str(trade.get("entry_timestamp") or "").strip()
    exit_timestamp = str(trade.get("exit_timestamp") or "").strip()
    side = str(trade.get("side") or "LONG").strip().upper()
    entry_price = _to_float(trade.get("entry_price"))
    if not entry_timestamp or not exit_timestamp or not entry_price:
        return None
    entry_index = bar_index_lookup.get(entry_timestamp)
    exit_index = bar_index_lookup.get(exit_timestamp)
    if entry_index is None or exit_index is None or exit_index < entry_index:
        return None

    risk_points = _rolling_risk_points(
        bars,
        entry_index=entry_index,
        lookback_bars=spec.risk_lookback_bars,
        floor=spec.risk_range_floor,
    )
    hold_bars = max(exit_index - entry_index + 1, 1)
    if side == "LONG":
        stop_price = entry_price - risk_points
    else:
        stop_price = entry_price + risk_points
    dynamic_stop_price = stop_price
    checkpoint_reached = False
    peak_mfe = 0.0
    trough_mae = 0.0
    candidate_exit_index = exit_index
    candidate_exit_price = _to_float(bars[exit_index].get("close"))
    candidate_exit_reason = "time_exit"

    for index in range(entry_index, min(entry_index + hold_bars, len(bars))):
        bar = bars[index]
        high = _to_float(bar.get("high"))
        low = _to_float(bar.get("low"))
        close = _to_float(bar.get("close"))
        if side == "LONG":
            current_mfe = high - entry_price
            current_mae = entry_price - low
        else:
            current_mfe = entry_price - low
            current_mae = high - entry_price
        peak_mfe = max(peak_mfe, current_mfe)
        trough_mae = max(trough_mae, current_mae)

        if not checkpoint_reached and peak_mfe >= risk_points * spec.checkpoint_arm_r:
            checkpoint_reached = True

        if checkpoint_reached:
            dynamic_stop_price = _checkpoint_stop_price(
                side=side,
                current_stop=dynamic_stop_price,
                entry_price=entry_price,
                risk_points=risk_points,
                bar_low=low,
                bar_high=high,
                checkpoint_lock_r=spec.checkpoint_lock_r,
                checkpoint_trail_r=spec.checkpoint_trail_r,
            )

        if side == "LONG":
            stop_hit = low <= dynamic_stop_price
        else:
            stop_hit = high >= dynamic_stop_price
        if stop_hit:
            candidate_exit_index = index
            candidate_exit_price = dynamic_stop_price
            candidate_exit_reason = "checkpoint_stop" if checkpoint_reached else "stop"
            break

        if (
            not checkpoint_reached
            and (index - entry_index + 1) >= spec.no_traction_abort_bars
            and peak_mfe < risk_points * spec.no_traction_min_favorable_r
        ):
            candidate_exit_index = index
            candidate_exit_price = close
            candidate_exit_reason = "no_traction_abort"
            break

    if side == "LONG":
        realized_pnl = (candidate_exit_price - entry_price) * point_value
    else:
        realized_pnl = (entry_price - candidate_exit_price) * point_value
    return {
        "trade_id": trade.get("trade_id"),
        "side": side,
        "family": trade.get("family"),
        "entry_timestamp": entry_timestamp,
        "exit_timestamp": str(bars[candidate_exit_index].get("end_timestamp") or bars[candidate_exit_index].get("timestamp") or ""),
        "entry_price": round(entry_price, 6),
        "exit_price": round(candidate_exit_price, 6),
        "exit_reason": candidate_exit_reason,
        "realized_pnl": round(realized_pnl, 6),
        "entry_session_phase": trade.get("entry_session_phase"),
        "holding_bars": max(candidate_exit_index - entry_index + 1, 1),
        "risk_points": round(risk_points, 6),
        "peak_mfe_points": round(peak_mfe, 6),
        "worst_mae_points": round(trough_mae, 6),
    }


def _candidate_summary(trades: Sequence[dict[str, Any]]) -> dict[str, Any]:
    pnl_values = [_to_float(trade.get("realized_pnl")) for trade in trades]
    return {
        "trade_count": len(trades),
        "net_pnl": round(sum(pnl_values), 6),
        "max_drawdown": _max_drawdown(pnl_values),
        "profit_factor": _profit_factor(pnl_values),
        "avg_hold_bars": round(sum(_to_float(trade.get("holding_bars")) for trade in trades) / float(max(len(trades), 1)), 6),
        "exit_reason_counts": _count_labels(str(trade.get("exit_reason") or "UNKNOWN") for trade in trades),
    }


def run_published_strategy_exit_probe(
    *,
    study_json_paths: Sequence[str | Path],
    report_dir: str | Path = DEFAULT_REPORT_DIR,
    spec: PublishedExitProbeSpec | None = None,
) -> dict[str, Path]:
    resolved_spec = spec or PublishedExitProbeSpec()
    resolved_report_dir = Path(report_dir)
    resolved_report_dir.mkdir(parents=True, exist_ok=True)

    result_rows: list[dict[str, Any]] = []
    for study_path_like in study_json_paths:
        study_path = Path(study_path_like)
        study = _load_study(study_path)
        bars = _ordered_bars(study)
        trades = _ordered_trades(study)
        point_value = _to_float(study.get("point_value"), 1.0)
        bar_index_lookup = _bar_index_by_timestamp(bars)
        baseline = _baseline_summary(trades)
        candidate_trades = [
            candidate
            for trade in trades
            if (candidate := _simulate_candidate_trade(
                trade=trade,
                bars=bars,
                bar_index_lookup=bar_index_lookup,
                point_value=point_value,
                spec=resolved_spec,
            )) is not None
        ]
        candidate = _candidate_summary(candidate_trades)
        result_rows.append(
            {
                "study_json_path": str(study_path),
                "standalone_strategy_id": str(study.get("standalone_strategy_id") or ""),
                "symbol": str(study.get("symbol") or ""),
                "point_value": point_value,
                "baseline": baseline,
                "candidate": candidate,
                "delta": {
                    "net_pnl": round(candidate["net_pnl"] - baseline["net_pnl"], 6),
                    "max_drawdown": round(candidate["max_drawdown"] - baseline["max_drawdown"], 6),
                    "profit_factor": (
                        round(float(candidate["profit_factor"]) - float(baseline["profit_factor"]), 6)
                        if candidate["profit_factor"] is not None and baseline["profit_factor"] is not None
                        else None
                    ),
                },
            }
        )

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "policy": {
            "policy_id": resolved_spec.policy_id,
            "label": resolved_spec.label,
            "checkpoint_arm_r": resolved_spec.checkpoint_arm_r,
            "checkpoint_lock_r": resolved_spec.checkpoint_lock_r,
            "checkpoint_trail_r": resolved_spec.checkpoint_trail_r,
            "no_traction_abort_bars": resolved_spec.no_traction_abort_bars,
            "no_traction_min_favorable_r": resolved_spec.no_traction_min_favorable_r,
            "risk_lookback_bars": resolved_spec.risk_lookback_bars,
            "risk_range_floor": resolved_spec.risk_range_floor,
        },
        "results": result_rows,
    }
    json_path = resolved_report_dir / "published_strategy_exit_probe.json"
    markdown_path = resolved_report_dir / "published_strategy_exit_probe.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload), encoding="utf-8")
    return {
        "report_json_path": json_path,
        "report_markdown_path": markdown_path,
    }


def publish_published_strategy_exit_probe_study(
    *,
    source_study_json_path: str | Path,
    spec: PublishedExitProbeSpec | None = None,
    report_dir: str | Path = DEFAULT_REPORT_DIR,
    historical_playback_dir: str | Path = DEFAULT_HISTORICAL_PLAYBACK_DIR,
    study_suffix: str = "_checkpoint_no_traction_v1",
    label_suffix: str = " [Checkpoint + No-Traction v1]",
) -> dict[str, Path]:
    resolved_spec = spec or PublishedExitProbeSpec()
    resolved_report_dir = Path(report_dir)
    resolved_report_dir.mkdir(parents=True, exist_ok=True)
    resolved_historical_playback_dir = Path(historical_playback_dir)
    resolved_historical_playback_dir.mkdir(parents=True, exist_ok=True)

    source_path = Path(source_study_json_path)
    study = _load_study(source_path)
    bars = _ordered_bars(study)
    trades = _ordered_trades(study)
    point_value = _to_float(study.get("point_value"), 1.0)
    bar_index_lookup = _bar_index_by_timestamp(bars)
    baseline = _baseline_summary(trades)
    candidate_trades = [
        candidate
        for trade in trades
        if (candidate := _simulate_candidate_trade(
            trade=trade,
            bars=bars,
            bar_index_lookup=bar_index_lookup,
            point_value=point_value,
            spec=resolved_spec,
        )) is not None
    ]
    candidate = _candidate_summary(candidate_trades)

    payload = json.loads(json.dumps(study))
    original_strategy_id = str(
        payload.get("standalone_strategy_id")
        or dict(payload.get("meta") or {}).get("strategy_id")
        or source_path.stem
    )
    new_strategy_id = f"{original_strategy_id}{study_suffix}"
    derived_summary = _trade_summary_rows(candidate_trades)
    derived_summary["bar_count"] = int((dict(payload.get("summary") or {})).get("bar_count") or len(bars))

    payload["generated_at"] = datetime.now(UTC).isoformat()
    payload["standalone_strategy_id"] = new_strategy_id
    meta = dict(payload.get("meta") or {})
    meta["study_id"] = new_strategy_id
    meta["strategy_id"] = new_strategy_id
    meta["display_name"] = f"{str(meta.get('display_name') or original_strategy_id)}{label_suffix}"
    meta["truth_provenance"] = {
        **dict(meta.get("truth_provenance") or {}),
        "published_strategy_exit_probe_source_study": str(source_path),
        "published_strategy_exit_probe_policy_id": resolved_spec.policy_id,
        "published_strategy_exit_probe_label": resolved_spec.label,
        "published_strategy_exit_probe_checkpoint_arm_r": resolved_spec.checkpoint_arm_r,
        "published_strategy_exit_probe_checkpoint_lock_r": resolved_spec.checkpoint_lock_r,
        "published_strategy_exit_probe_checkpoint_trail_r": resolved_spec.checkpoint_trail_r,
        "published_strategy_exit_probe_no_traction_abort_bars": resolved_spec.no_traction_abort_bars,
        "published_strategy_exit_probe_no_traction_min_favorable_r": resolved_spec.no_traction_min_favorable_r,
        "published_strategy_exit_probe_risk_lookback_bars": resolved_spec.risk_lookback_bars,
        "published_strategy_exit_probe_risk_range_floor": resolved_spec.risk_range_floor,
        "published_strategy_exit_probe_derived": True,
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
        historical_playback_dir=resolved_historical_playback_dir,
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
    latest_manifest = _latest_manifest_path(resolved_historical_playback_dir)
    existing_studies = _load_manifest_study_rows(latest_manifest) if latest_manifest is not None else []
    merged_studies = _merge_study_rows(existing_studies=existing_studies, new_studies=[study_row])
    manifest_run_stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
    manifest_path = retest._write_historical_playback_manifest(
        studies=merged_studies,
        run_stamp=manifest_run_stamp,
        historical_playback_dir=resolved_historical_playback_dir,
        shard_config=retest.RetestShardConfig(),
    )

    report_payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "source_study_json_path": str(source_path),
        "published_strategy_id": new_strategy_id,
        "published_strategy_study_json_path": str(path_pair["json"]),
        "published_strategy_study_markdown_path": str(path_pair["markdown"]),
        "historical_playback_manifest": str(manifest_path),
        "baseline": baseline,
        "derived": candidate,
        "policy": {
            "policy_id": resolved_spec.policy_id,
            "label": resolved_spec.label,
            "checkpoint_arm_r": resolved_spec.checkpoint_arm_r,
            "checkpoint_lock_r": resolved_spec.checkpoint_lock_r,
            "checkpoint_trail_r": resolved_spec.checkpoint_trail_r,
            "no_traction_abort_bars": resolved_spec.no_traction_abort_bars,
            "no_traction_min_favorable_r": resolved_spec.no_traction_min_favorable_r,
            "risk_lookback_bars": resolved_spec.risk_lookback_bars,
            "risk_range_floor": resolved_spec.risk_range_floor,
        },
        "delta": {
            "net_pnl": round(candidate["net_pnl"] - baseline["net_pnl"], 6),
            "max_drawdown": round(candidate["max_drawdown"] - baseline["max_drawdown"], 6),
            "profit_factor": (
                round(float(candidate["profit_factor"]) - float(baseline["profit_factor"]), 6)
                if candidate["profit_factor"] is not None and baseline["profit_factor"] is not None
                else None
            ),
        },
    }
    json_path = resolved_report_dir / f"{new_strategy_id}.publish.json"
    markdown_path = resolved_report_dir / f"{new_strategy_id}.publish.md"
    json_path.write_text(json.dumps(report_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_lines = [
        "# Published Strategy Exit Probe Publish",
        "",
        f"- Source study: `{source_path}`",
        f"- Published strategy id: `{new_strategy_id}`",
        f"- Manifest: `{manifest_path}`",
        f"- Policy: `{resolved_spec.label}`",
        f"- Baseline net / max DD / PF: `{baseline['net_pnl']}` / `{baseline['max_drawdown']}` / `{baseline['profit_factor']}`",
        f"- Derived net / max DD / PF: `{candidate['net_pnl']}` / `{candidate['max_drawdown']}` / `{candidate['profit_factor']}`",
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


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Published Strategy Exit Probe",
        "",
        f"Generated: `{payload['generated_at']}`",
        "",
        f"Policy: `{payload['policy']['label']}`",
        "",
    ]
    for result in payload["results"]:
        lines.extend(
            [
                f"## {Path(result['study_json_path']).name}",
                "",
                f"- Strategy: `{result['standalone_strategy_id']}`",
                f"- Symbol: `{result['symbol']}`",
                f"- Baseline net P&L: `{result['baseline']['net_pnl']}`",
                f"- Baseline max drawdown: `{result['baseline']['max_drawdown']}`",
                f"- Candidate net P&L: `{result['candidate']['net_pnl']}`",
                f"- Candidate max drawdown: `{result['candidate']['max_drawdown']}`",
                f"- Delta net P&L: `{result['delta']['net_pnl']}`",
                f"- Delta max drawdown: `{result['delta']['max_drawdown']}`",
                "",
            ]
        )
    return "\n".join(lines).strip() + "\n"
