"""Probationary tracking runner for approved quant baseline lanes."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from statistics import fmean
from typing import Any

from .evaluator import ApprovedQuantLaneTrade, build_symbol_store_for_approved_lanes, evaluate_approved_lane
from .runtime_boundary import (
    APPROVED_QUANT_BASELINE_RUNTIME_CONTRACT_VERSION,
    approved_quant_research_dependency_rows,
)
from .specs import (
    ApprovedQuantLaneSpec,
    approved_quant_lane_scope_fingerprint,
    approved_quant_lane_scope_payload,
    approved_quant_lane_specs,
)


@dataclass(frozen=True)
class ApprovedQuantProbationArtifacts:
    snapshot_json_path: Path
    snapshot_markdown_path: Path
    current_status_json_path: Path
    current_status_markdown_path: Path
    root_dir: Path
    report: dict[str, Any]


def run_approved_quant_baseline_probation(
    *,
    database_path: str | Path,
    execution_timeframe: str = "5m",
    output_dir: str | Path | None = None,
) -> ApprovedQuantProbationArtifacts:
    resolved_database_path = Path(database_path).resolve()
    root_dir = Path(output_dir or Path.cwd() / "outputs" / "probationary_quant_baselines").resolve()
    root_dir.mkdir(parents=True, exist_ok=True)

    specs = approved_quant_lane_specs()
    symbol_store = build_symbol_store_for_approved_lanes(
        database_path=resolved_database_path,
        execution_timeframe=execution_timeframe,
        specs=specs,
    )

    generated_at = datetime.now(UTC).isoformat()
    approval_date = _approval_date_from_source(Path(specs[0].approval_source))
    lane_rows = []
    for spec in specs:
        evaluated = evaluate_approved_lane(spec=spec, symbol_store=symbol_store)
        lane_dir = root_dir / "lanes" / spec.lane_id
        lane_dir.mkdir(parents=True, exist_ok=True)
        _write_jsonl(lane_dir / "signals.jsonl", evaluated["signals"])
        _write_jsonl(lane_dir / "trades.jsonl", evaluated["trades"])
        approval_reference = _approval_reference_summary(spec=spec, trades=evaluated["trades"])
        daily_payloads = _build_daily_payloads(spec=spec, evaluated=evaluated, approval_reference=approval_reference)
        weekly_payloads = _build_weekly_payloads(spec=spec, evaluated=evaluated, approval_reference=approval_reference)
        _write_period_payloads(lane_dir / "daily", daily_payloads, label_key="session_date")
        _write_period_payloads(lane_dir / "weekly", weekly_payloads, label_key="week_id")
        status = _build_lane_status(
            spec=spec,
            trades=evaluated["trades"],
            generated_at=generated_at,
            approval_date=approval_date,
            approval_reference=approval_reference,
        )
        (lane_dir / "status.json").write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        post_cost_read = status["post_cost_monitoring_read"]
        lane_rows.append(
            {
                "lane_id": spec.lane_id,
                "lane_name": spec.lane_name,
                "lane_classification": status["lane_classification"],
                "baseline_status": status["baseline_status"],
                "probation_status": status["probation_status"],
                "promotion_state": status["promotion_state"],
                "current_trade_count_probation": status["current_trade_count_probation"],
                "rolling_expectancy_cost_020": status["rolling_expectancy_cost_020"],
                "rolling_expectancy_cost_025": status["rolling_expectancy_cost_025"],
                "post_cost_monitoring_read": post_cost_read,
                "symbol_breadth_cost_020": status["symbol_breadth_cost_020"],
                "session_concentration_flag": status["session_concentration_flag"],
                "single_symbol_dependence_flag": status["single_symbol_dependence_flag"],
                "execution_drift_flag": status["execution_drift_flag"],
                "core_integrity_flag": status["core_integrity_flag"],
                "latest_signal_timestamp": status["latest_signal_timestamp"],
                "last_reviewed_at": status["last_reviewed_at"],
                "operator_status_line": status["operator_status_line"],
                "active_exit_logic": status["active_exit_logic"],
                "warning_flags": status["warning_flags"],
                "unknown_session_warning": status["unknown_session_warning"],
                "slice_weakness_flag": status["slice_weakness_flag"],
                "weak_symbol_slices": status["weak_symbol_slices"],
                "weak_session_slices": status["weak_session_slices"],
                "approval_baseline_reference": status["approval_baseline_reference"],
                "drift_vs_approval_baseline_cost_020": status["drift_vs_approval_baseline_cost_020"],
                "drift_vs_approval_baseline_cost_025": status["drift_vs_approval_baseline_cost_025"],
                "scope_fingerprint": status["scope_fingerprint"],
                "approved_scope": status["approved_scope"],
                "artifacts": {
                    "lane_dir": str(lane_dir),
                    "status": str((lane_dir / "status.json").resolve()),
                    "signals": str((lane_dir / "signals.jsonl").resolve()),
                    "trades": str((lane_dir / "trades.jsonl").resolve()),
                    "daily_dir": str((lane_dir / "daily").resolve()),
                    "weekly_dir": str((lane_dir / "weekly").resolve()),
                },
            }
        )

    snapshot = {
        "generated_at": generated_at,
        "status": "available",
        "source_of_truth_path": str(Path(specs[0].approval_source).resolve()),
        "root_dir": str(root_dir),
        "runtime_contract_version": APPROVED_QUANT_BASELINE_RUNTIME_CONTRACT_VERSION,
        "boundary": {
            "adapter_module": "mgc_v05l.app.approved_quant_lanes.runtime_boundary",
            "contract_version": APPROVED_QUANT_BASELINE_RUNTIME_CONTRACT_VERSION,
            "research_dependencies": approved_quant_research_dependency_rows(),
        },
        "rows": lane_rows,
        "summary_line": ", ".join(
            f"{row['lane_name']}={row['probation_status']}/{row['baseline_status']}" for row in lane_rows
        ),
        "operator_summary_line": " | ".join(row["operator_status_line"] for row in lane_rows),
        "artifacts": {
            "root_dir": str(root_dir.resolve()),
        },
    }
    snapshot_json_path = root_dir / "approved_quant_baselines_snapshot.json"
    snapshot_markdown_path = root_dir / "approved_quant_baselines_snapshot.md"
    snapshot_json_path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    snapshot_markdown_path.write_text(_snapshot_markdown(snapshot).strip() + "\n", encoding="utf-8")
    current_status = _build_current_active_baseline_status(snapshot=snapshot)
    current_status_json_path = root_dir / "current_active_baseline_status.json"
    current_status_markdown_path = root_dir / "current_active_baseline_status.md"
    current_status_json_path.write_text(json.dumps(current_status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    current_status_markdown_path.write_text(_current_active_status_markdown(current_status).strip() + "\n", encoding="utf-8")
    return ApprovedQuantProbationArtifacts(
        snapshot_json_path=snapshot_json_path,
        snapshot_markdown_path=snapshot_markdown_path,
        current_status_json_path=current_status_json_path,
        current_status_markdown_path=current_status_markdown_path,
        root_dir=root_dir,
        report=snapshot,
    )


def _build_lane_status(
    *,
    spec: ApprovedQuantLaneSpec,
    trades: list[dict[str, Any]],
    generated_at: str,
    approval_date: str | None,
    approval_reference: dict[str, Any],
) -> dict[str, Any]:
    trade_rows = [ApprovedQuantLaneTrade(**trade) for trade in trades]
    window_size = 30 if spec.family == "breakout_continuation" else 25
    last_window = trade_rows[-window_size:]
    prev_window = trade_rows[-(2 * window_size) : -window_size] if len(trade_rows) >= 2 * window_size else []
    rolling_020 = _avg([trade.net_r_cost_020 for trade in last_window])
    rolling_025 = _avg([trade.net_r_cost_025 for trade in last_window])
    prev_020 = _avg([trade.net_r_cost_020 for trade in prev_window])
    symbol_breadth = _symbol_breadth_cost_020(spec=spec, trade_rows=trade_rows)
    session_concentration_flag = _session_concentration_flag(trade_rows)
    single_symbol_dependence_flag = _single_symbol_dependence_flag(trade_rows)
    execution_drift_flag = bool(rolling_020 > 0.0 and rolling_025 < 0.0)
    core_integrity_flag = _core_integrity_flag(spec=spec, trade_rows=trade_rows)
    unknown_session_warning = _unknown_session_warning(trades)
    weak_symbol_slices = _weak_slice_rows(trades, key="symbol", min_trade_count=5)
    weak_session_slices = _weak_slice_rows(trades, key="session_label", min_trade_count=5)
    slice_weakness_flag = bool(weak_symbol_slices or weak_session_slices)
    drift_020 = round(rolling_020 - float(approval_reference["expectancy_net_020_r"]), 6)
    drift_025 = round(rolling_025 - float(approval_reference["expectancy_net_025_r"]), 6)
    review_reasons = []
    watch_reasons = []
    if last_window and rolling_020 < 0.0:
        watch_reasons.append("latest_window_negative_net_020")
    if last_window and rolling_025 < 0.0:
        review_reasons.append("latest_window_negative_net_025")
    if prev_window and rolling_020 < 0.0 and prev_020 < 0.0:
        review_reasons.append("two_consecutive_negative_windows")
    if single_symbol_dependence_flag:
        review_reasons.append("single_symbol_dependence")
    if session_concentration_flag:
        watch_reasons.append("session_concentration")
    if not core_integrity_flag:
        review_reasons.append("core_integrity_failed")
    if spec.family == "breakout_continuation" and symbol_breadth < 3:
        review_reasons.append("symbol_breadth_below_3")
    probation_status = "normal"
    if len(review_reasons) >= 2:
        probation_status = "suspend"
    elif review_reasons:
        probation_status = "review"
    elif watch_reasons:
        probation_status = "watch"
    baseline_status = "suspended" if probation_status == "suspend" else "operator_baseline_candidate"
    approved_scope = approved_quant_lane_scope_payload(spec)
    monitoring_read = _post_cost_monitoring_read(rolling_020=rolling_020, rolling_025=rolling_025)
    warning_flags = []
    if unknown_session_warning["flag"]:
        warning_flags.append("unknown_session_labeling_watch")
    if slice_weakness_flag:
        warning_flags.append("slice_weakness_watch")
    operator_status_line = (
        f"APPROVED BASELINE | {spec.lane_name} | probation={probation_status.upper()} | "
        f"promotion={baseline_status.upper()} | post_cost={monitoring_read['label']}"
    )
    return {
        "lane_id": spec.lane_id,
        "lane_name": spec.lane_name,
        "lane_classification": "approved_baseline_lane",
        "baseline_status": baseline_status,
        "probation_status": probation_status,
        "promotion_state": baseline_status,
        "approval_source": spec.approval_source,
        "approval_date": approval_date,
        "last_reviewed_at": generated_at,
        "last_status_change_at": generated_at,
        "last_status_change_reason": ", ".join(review_reasons or watch_reasons) or "no_active_flags",
        "review_owner": spec.review_owner,
        "current_trade_count_probation": len(trade_rows),
        "rolling_expectancy_cost_020": rolling_020,
        "rolling_expectancy_cost_025": rolling_025,
        "symbol_breadth_cost_020": symbol_breadth,
        "session_concentration_flag": session_concentration_flag,
        "single_symbol_dependence_flag": single_symbol_dependence_flag,
        "execution_drift_flag": execution_drift_flag,
        "core_integrity_flag": core_integrity_flag,
        "slice_weakness_flag": slice_weakness_flag,
        "weak_symbol_slices": weak_symbol_slices,
        "weak_session_slices": weak_session_slices,
        "unknown_session_warning": unknown_session_warning,
        "warning_flags": warning_flags,
        "latest_signal_timestamp": trade_rows[-1].signal_timestamp if trade_rows else None,
        "latest_trade_timestamp": trade_rows[-1].entry_timestamp if trade_rows else None,
        "active_exit_logic": _active_exit_logic_payload(spec),
        "approval_baseline_reference": approval_reference,
        "drift_vs_approval_baseline_cost_020": drift_020,
        "drift_vs_approval_baseline_cost_025": drift_025,
        "scope_fingerprint": approved_quant_lane_scope_fingerprint(spec),
        "approved_scope": approved_scope,
        "runtime_contract_version": APPROVED_QUANT_BASELINE_RUNTIME_CONTRACT_VERSION,
        "post_cost_monitoring_read": monitoring_read,
        "operator_status_line": operator_status_line,
        "recommended_action": _recommended_action(probation_status),
    }


def _build_daily_payloads(
    *,
    spec: ApprovedQuantLaneSpec,
    evaluated: dict[str, Any],
    approval_reference: dict[str, Any],
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    rejected_reason_counts = evaluated["rejected_reason_counts"]
    for signal in evaluated["signals"]:
        bucket = _signal_date(signal["signal_timestamp"])
        grouped.setdefault(bucket, {"signals": [], "trades": [], "rejected": rejected_reason_counts.get(bucket, {})})
        grouped[bucket]["signals"].append(signal)
    for trade in evaluated["trades"]:
        bucket = _signal_date(trade["entry_timestamp"])
        grouped.setdefault(bucket, {"signals": [], "trades": [], "rejected": rejected_reason_counts.get(bucket, {})})
        grouped[bucket]["trades"].append(trade)
    return [
        _period_summary(
            spec=spec,
            label_key="session_date",
            label_value=bucket,
            payload=payload,
            approval_reference=approval_reference,
        )
        for bucket, payload in sorted(grouped.items())
    ]


def _build_weekly_payloads(
    *,
    spec: ApprovedQuantLaneSpec,
    evaluated: dict[str, Any],
    approval_reference: dict[str, Any],
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for signal in evaluated["signals"]:
        bucket = _week_id(signal["signal_timestamp"])
        grouped.setdefault(bucket, {"signals": [], "trades": [], "rejected": {}})
        grouped[bucket]["signals"].append(signal)
    for trade in evaluated["trades"]:
        bucket = _week_id(trade["entry_timestamp"])
        grouped.setdefault(bucket, {"signals": [], "trades": [], "rejected": {}})
        grouped[bucket]["trades"].append(trade)
    return [
        _period_summary(
            spec=spec,
            label_key="week_id",
            label_value=bucket,
            payload=payload,
            approval_reference=approval_reference,
        )
        for bucket, payload in sorted(grouped.items())
    ]


def _period_summary(
    *,
    spec: ApprovedQuantLaneSpec,
    label_key: str,
    label_value: str,
    payload: dict[str, Any],
    approval_reference: dict[str, Any],
) -> dict[str, Any]:
    trades = payload["trades"]
    gross_values = [float(trade["gross_r"]) for trade in trades]
    net020_values = [float(trade["net_r_cost_020"]) for trade in trades]
    net025_values = [float(trade["net_r_cost_025"]) for trade in trades]
    symbol_attribution = _attribution_rows(trades, key="symbol")
    session_attribution = _attribution_rows(trades, key="session_label")
    unknown_session_warning = _unknown_session_warning(trades)
    weak_symbol_slices = _weak_slice_rows(trades, key="symbol", min_trade_count=1)
    weak_session_slices = _weak_slice_rows(trades, key="session_label", min_trade_count=1)
    summary = {
        label_key: label_value,
        "lane_id": spec.lane_id,
        "lane_name": spec.lane_name,
        "lane_classification": "approved_baseline_lane",
        "signal_count": len(payload["signals"]),
        "trade_count": len(trades),
        "rejected_reason_counts": payload.get("rejected", {}),
        "expectancy_gross_r": _avg(gross_values),
        "expectancy_net_020_r": _avg(net020_values),
        "expectancy_net_025_r": _avg(net025_values),
        "rolling_hit_rate": _hit_rate(gross_values),
        "rolling_avg_win_r": _avg([value for value in gross_values if value > 0.0]),
        "rolling_avg_loss_r": _avg([value for value in gross_values if value < 0.0]),
        "symbol_attribution": symbol_attribution,
        "session_attribution": session_attribution,
        "active_exit_logic": _active_exit_logic_payload(spec),
        "approval_baseline_reference": approval_reference,
        "drift_vs_approval_baseline_cost_020": round(_avg(net020_values) - float(approval_reference["expectancy_net_020_r"]), 6),
        "drift_vs_approval_baseline_cost_025": round(_avg(net025_values) - float(approval_reference["expectancy_net_025_r"]), 6),
        "unknown_session_warning": unknown_session_warning,
        "slice_weakness_flag": bool(weak_symbol_slices or weak_session_slices),
        "weak_symbol_slices": weak_symbol_slices,
        "weak_session_slices": weak_session_slices,
        "scope_fingerprint": approved_quant_lane_scope_fingerprint(spec),
        "approved_scope": approved_quant_lane_scope_payload(spec),
    }
    summary["post_cost_monitoring_read"] = _post_cost_monitoring_read(
        rolling_020=summary["expectancy_net_020_r"],
        rolling_025=summary["expectancy_net_025_r"],
    )
    summary["warning_flags"] = _period_warning_flags(summary)
    summary["recommended_action"] = _recommended_action_for_period(summary)
    return summary


def _write_period_payloads(period_dir: Path, payloads: list[dict[str, Any]], *, label_key: str) -> None:
    period_dir.mkdir(parents=True, exist_ok=True)
    for payload in payloads:
        label = str(payload[label_key])
        json_path = period_dir / f"{label}.json"
        md_path = period_dir / f"{label}.md"
        json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        md_path.write_text(_period_markdown(payload, label_key).strip() + "\n", encoding="utf-8")


def _attribution_rows(trades: list[dict[str, Any]], *, key: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        grouped.setdefault(str(trade[key]), []).append(trade)
    rows = []
    for label, bucket in grouped.items():
        rows.append(
            {
                key: label,
                "trade_count": len(bucket),
                "gross_r_total": round(sum(float(trade["gross_r"]) for trade in bucket), 6),
                "net_r_020_total": round(sum(float(trade["net_r_cost_020"]) for trade in bucket), 6),
                "net_r_025_total": round(sum(float(trade["net_r_cost_025"]) for trade in bucket), 6),
            }
        )
    rows.sort(key=lambda row: (-row["net_r_020_total"], row[key]))
    return rows


def _weak_slice_rows(trades: list[dict[str, Any]], *, key: str, min_trade_count: int) -> list[dict[str, Any]]:
    weak_rows = []
    for row in _attribution_rows(trades, key=key):
        if int(row["trade_count"]) < min_trade_count:
            continue
        avg_net_020 = round(float(row["net_r_020_total"]) / float(row["trade_count"]), 6)
        if avg_net_020 < 0.0:
            weak_rows.append(
                {
                    key: row[key],
                    "trade_count": row["trade_count"],
                    "avg_net_r_020": avg_net_020,
                }
            )
    return weak_rows


def _signal_date(timestamp: str) -> str:
    return datetime.fromisoformat(timestamp).date().isoformat()


def _week_id(timestamp: str) -> str:
    dt = datetime.fromisoformat(timestamp)
    iso_year, iso_week, _ = dt.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def _approval_date_from_source(path: Path) -> str | None:
    resolved = path.resolve()
    if not resolved.exists():
        return None
    return datetime.fromtimestamp(resolved.stat().st_mtime, tz=UTC).date().isoformat()


def _symbol_breadth_cost_020(*, spec: ApprovedQuantLaneSpec, trade_rows: list[ApprovedQuantLaneTrade]) -> int:
    positives = 0
    for symbol in spec.symbols:
        values = [trade.net_r_cost_020 for trade in trade_rows if trade.symbol == symbol]
        if values and _avg(values) >= 0.0:
            positives += 1
    return positives


def _session_concentration_flag(trade_rows: list[ApprovedQuantLaneTrade]) -> bool:
    totals: dict[str, float] = {}
    for trade in trade_rows:
        totals[trade.session_label] = totals.get(trade.session_label, 0.0) + trade.net_r_cost_020
    total = sum(totals.values())
    if total <= 0.0:
        return False
    return max(totals.values(), default=0.0) / total > 0.75


def _single_symbol_dependence_flag(trade_rows: list[ApprovedQuantLaneTrade]) -> bool:
    totals: dict[str, float] = {}
    for trade in trade_rows:
        totals[trade.symbol] = totals.get(trade.symbol, 0.0) + trade.net_r_cost_020
    total = sum(totals.values())
    if total <= 0.0:
        return False
    return max(totals.values(), default=0.0) / total > 0.70


def _core_integrity_flag(*, spec: ApprovedQuantLaneSpec, trade_rows: list[ApprovedQuantLaneTrade]) -> bool:
    if spec.family == "breakout_continuation":
        return _symbol_breadth_cost_020(spec=spec, trade_rows=trade_rows) >= 3
    core_symbols = {"CL", "ES", "6E", "6J"}
    core_values = [trade.net_r_cost_020 for trade in trade_rows if trade.symbol in core_symbols]
    return _avg(core_values) > 0.0 if core_values else False


def _recommended_action(probation_status: str) -> str:
    if probation_status == "suspend":
        return "suspend"
    if probation_status == "review":
        return "manual_review"
    if probation_status == "watch":
        return "continue_with_watch"
    return "continue_probation"


def _recommended_action_for_period(summary: dict[str, Any]) -> str:
    if summary["expectancy_net_025_r"] < 0.0:
        return "review"
    if summary["expectancy_net_020_r"] < 0.0:
        return "watch"
    return "continue"


def _period_warning_flags(summary: dict[str, Any]) -> list[str]:
    flags = []
    if summary["unknown_session_warning"]["flag"]:
        flags.append("unknown_session_labeling_watch")
    if summary["slice_weakness_flag"]:
        flags.append("slice_weakness_watch")
    if summary["drift_vs_approval_baseline_cost_025"] < -0.10:
        flags.append("approval_baseline_drift_watch")
    return flags


def _avg(values: list[float]) -> float:
    clean = [float(value) for value in values]
    return round(fmean(clean), 6) if clean else 0.0


def _hit_rate(values: list[float]) -> float:
    return round(sum(1 for value in values if value > 0.0) / float(len(values)), 6) if values else 0.0


def _post_cost_monitoring_read(*, rolling_020: float, rolling_025: float) -> dict[str, Any]:
    if rolling_025 >= 0.0:
        label = "stable_positive_post_cost"
    elif rolling_020 >= 0.0:
        label = "mixed_cost_read"
    else:
        label = "negative_post_cost"
    return {
        "label": label,
        "rolling_expectancy_cost_020": rolling_020,
        "rolling_expectancy_cost_025": rolling_025,
    }


def _active_exit_logic_payload(spec: ApprovedQuantLaneSpec) -> dict[str, Any]:
    return {
        "exit_style": spec.exit_style,
        "hold_bars": spec.hold_bars,
        "stop_r": spec.stop_r,
        "target_r": spec.target_r,
        "structural_invalidation_r": spec.structural_invalidation_r,
    }


def _approval_reference_summary(*, spec: ApprovedQuantLaneSpec, trades: list[dict[str, Any]]) -> dict[str, Any]:
    gross_values = [float(trade["gross_r"]) for trade in trades]
    net020_values = [float(trade["net_r_cost_020"]) for trade in trades]
    net025_values = [float(trade["net_r_cost_025"]) for trade in trades]
    return {
        "reference_type": "frozen_approved_baseline_full_history",
        "lane_id": spec.lane_id,
        "trade_count": len(trades),
        "expectancy_gross_r": _avg(gross_values),
        "expectancy_net_020_r": _avg(net020_values),
        "expectancy_net_025_r": _avg(net025_values),
    }


def _unknown_session_warning(trades: list[dict[str, Any]]) -> dict[str, Any]:
    if not trades:
        return {
            "flag": False,
            "label": "no_unknown_session_activity",
            "trade_share": 0.0,
            "abs_pnl_share_cost_020": 0.0,
        }
    unknown_trades = [trade for trade in trades if str(trade["session_label"]) == "UNKNOWN"]
    trade_share = round(len(unknown_trades) / float(len(trades)), 6)
    abs_total = sum(abs(float(trade["net_r_cost_020"])) for trade in trades)
    abs_unknown = sum(abs(float(trade["net_r_cost_020"])) for trade in unknown_trades)
    pnl_share = round(abs_unknown / abs_total, 6) if abs_total else 0.0
    flag = trade_share >= 0.35 or pnl_share >= 0.50
    label = "unknown_session_labeling_watch" if flag else "unknown_session_within_tolerance"
    if not unknown_trades:
        label = "no_unknown_session_activity"
    return {
        "flag": flag,
        "label": label,
        "trade_share": trade_share,
        "abs_pnl_share_cost_020": pnl_share,
    }


def _build_current_active_baseline_status(*, snapshot: dict[str, Any]) -> dict[str, Any]:
    generated_at = str(snapshot["generated_at"])
    rows = []
    for row in snapshot["rows"]:
        approved_scope = dict(row["approved_scope"])
        rows.append(
            {
                "lane_id": row["lane_id"],
                "lane_name": row["lane_name"],
                "symbols": approved_scope["symbols"],
                "allowed_sessions": approved_scope["allowed_sessions"],
                "excluded_sessions": approved_scope["excluded_sessions"],
                "permanent_exclusions": approved_scope["permanent_exclusions"],
                "active_exit_logic": row["active_exit_logic"],
                "probation_status": row["probation_status"],
                "promotion_state": row["promotion_state"],
                "post_cost_monitoring_read": row["post_cost_monitoring_read"],
                "symbol_breadth_cost_020": row["symbol_breadth_cost_020"],
                "warning_flags": row["warning_flags"],
                "unknown_session_warning": row["unknown_session_warning"],
                "slice_weakness_flag": row["slice_weakness_flag"],
                "drift_vs_approval_baseline_cost_020": row["drift_vs_approval_baseline_cost_020"],
                "drift_vs_approval_baseline_cost_025": row["drift_vs_approval_baseline_cost_025"],
                "latest_signal_timestamp": row["latest_signal_timestamp"],
                "operator_status_line": row["operator_status_line"],
            }
        )
    return {
        "generated_at": generated_at,
        "freeze_mode": "logic_frozen_monitoring_only",
        "source_of_truth_path": snapshot["source_of_truth_path"],
        "approved_lane_count": len(rows),
        "lanes": rows,
        "first_formal_review_checkpoint": _first_formal_review_checkpoint(snapshot),
        "carryforward_note": "Approved quant baselines are frozen. Monitoring focuses on post-cost stability, attribution, drift versus approval baseline, and UNKNOWN-session labeling quality.",
    }


def _first_formal_review_checkpoint(snapshot: dict[str, Any]) -> dict[str, Any]:
    generated_date = datetime.fromisoformat(str(snapshot["generated_at"])).date()
    checkpoint_date = _add_business_days(generated_date, 5).isoformat()
    lane_thresholds = []
    for row in snapshot["rows"]:
        threshold = 60 if "breakout" in str(row["lane_id"]) else 50
        lane_thresholds.append({"lane_id": row["lane_id"], "minimum_probation_trades_for_promotion_evidence": threshold})
    return {
        "review_type": "formal_weekly_probation_review",
        "scheduled_for": checkpoint_date,
        "cadence": "every_5_trading_days_during_probation",
        "evidence_to_examine": [
            "rolling post-cost expectancy by lane at 0.20R and 0.25R",
            "symbol attribution and symbol breadth",
            "session attribution and any UNKNOWN-session dependence",
            "drift versus frozen approval baseline reference",
            "single-symbol dependence and slice weakness flags",
            "latest daily and weekly warning flags",
            "operator recommendation: continue_probation, review, or suspend",
        ],
        "promotion_evidence_thresholds": lane_thresholds,
    }


def _add_business_days(start_date: date, days: int) -> date:
    current = start_date
    remaining = days
    while remaining > 0:
        current = date.fromordinal(current.toordinal() + 1)
        if current.weekday() < 5:
            remaining -= 1
    return current


def _snapshot_markdown(snapshot: dict[str, Any]) -> str:
    lines = [
        "# Approved Quant Baselines Snapshot",
        "",
        f"Generated at: {snapshot['generated_at']}",
        f"Runtime contract: {snapshot['runtime_contract_version']}",
        "",
    ]
    for dependency in snapshot.get("boundary", {}).get("research_dependencies", []):
        lines.append(
            f"- Dependency: {dependency['dependency_id']} -> {dependency['import_path']} ({dependency['dependency_kind']})"
        )
    if snapshot.get("boundary", {}).get("research_dependencies"):
        lines.append("")
    for row in snapshot["rows"]:
        lines.append(
            f"- {row['operator_status_line']} | rolling020={row['rolling_expectancy_cost_020']} | "
            f"rolling025={row['rolling_expectancy_cost_025']} | exit={row['active_exit_logic']['exit_style']} | "
            f"drift025={row['drift_vs_approval_baseline_cost_025']} | warnings={','.join(row['warning_flags']) or 'none'} | "
            f"scope={row['scope_fingerprint'][:12]}"
        )
    return "\n".join(lines)


def _period_markdown(payload: dict[str, Any], label_key: str) -> str:
    return "\n".join(
        [
            f"# {payload['lane_name']} {label_key}",
            "",
            f"{label_key}: {payload[label_key]}",
            f"Signal count: {payload['signal_count']}",
            f"Trade count: {payload['trade_count']}",
            f"Net expectancy 0.20R: {payload['expectancy_net_020_r']}",
            f"Net expectancy 0.25R: {payload['expectancy_net_025_r']}",
            f"Approval baseline net 0.20R: {payload['approval_baseline_reference']['expectancy_net_020_r']}",
            f"Approval baseline net 0.25R: {payload['approval_baseline_reference']['expectancy_net_025_r']}",
            f"Drift vs approval baseline 0.20R: {payload['drift_vs_approval_baseline_cost_020']}",
            f"Drift vs approval baseline 0.25R: {payload['drift_vs_approval_baseline_cost_025']}",
            f"Monitoring read: {payload['post_cost_monitoring_read']['label']}",
            f"UNKNOWN warning: {payload['unknown_session_warning']['label']}",
            f"Slice weakness flag: {payload['slice_weakness_flag']}",
            f"Warning flags: {', '.join(payload['warning_flags']) or 'none'}",
            f"Recommended action: {payload['recommended_action']}",
        ]
    )


def _current_active_status_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Current Active Baseline Status",
        "",
        f"Generated at: {payload['generated_at']}",
        f"Freeze mode: {payload['freeze_mode']}",
        f"Approved lane count: {payload['approved_lane_count']}",
        "",
    ]
    for lane in payload["lanes"]:
        lines.extend(
            [
                f"- {lane['operator_status_line']}",
                f"  symbols={','.join(lane['symbols'])} | sessions={','.join(lane['allowed_sessions'])} | excluded={','.join(lane['excluded_sessions'])}",
                (
                    f"  exit={lane['active_exit_logic']['exit_style']} "
                    f"(hold={lane['active_exit_logic']['hold_bars']}, target={lane['active_exit_logic']['target_r']}, "
                    f"struct_invalidation={lane['active_exit_logic']['structural_invalidation_r']})"
                ),
                (
                    f"  drift020={lane['drift_vs_approval_baseline_cost_020']} | "
                    f"drift025={lane['drift_vs_approval_baseline_cost_025']} | "
                    f"unknown_warning={lane['unknown_session_warning']['label']} | "
                    f"warnings={','.join(lane['warning_flags']) or 'none'}"
                ),
            ]
        )
    checkpoint = payload["first_formal_review_checkpoint"]
    lines.extend(
        [
            "",
            "## First Formal Review Checkpoint",
            f"- scheduled_for: {checkpoint['scheduled_for']}",
            f"- cadence: {checkpoint['cadence']}",
            "- evidence:",
        ]
    )
    for item in checkpoint["evidence_to_examine"]:
        lines.append(f"  - {item}")
    return "\n".join(lines)
