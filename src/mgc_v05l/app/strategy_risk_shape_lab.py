"""Reusable session-risk shaping analysis for published strategy studies."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence

from . import strategy_universe_retest as retest
from .atp_loosened_history_publish import _latest_manifest_path, _load_manifest_study_rows, _merge_study_rows

REPO_ROOT = Path.cwd()
DEFAULT_REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "strategy_risk_shape_lab"
DEFAULT_HISTORICAL_PLAYBACK_DIR = REPO_ROOT / "outputs" / "historical_playback"


@dataclass(frozen=True)
class RiskShapeProfile:
    profile_id: str
    label: str
    daily_peak_drawdown_cap: float | None = None
    daily_realized_loss_cap: float | None = None
    max_consecutive_losers: int | None = None
    max_consecutive_losers_scope: str = "global"
    equity_peak_drawdown_cap: float | None = None
    cooldown_sessions_after_equity_breach: int = 0
    realized_pnl_scale: float = 1.0


DEFAULT_RISK_SHAPE_PROFILES: tuple[RiskShapeProfile, ...] = (
    RiskShapeProfile(profile_id="baseline", label="Baseline"),
    RiskShapeProfile(
        profile_id="peak_5000",
        label="Daily Peak DD 5K",
        daily_peak_drawdown_cap=5000.0,
    ),
    RiskShapeProfile(
        profile_id="peak_5000_consec_3",
        label="Daily Peak DD 5K + 3 Loss Streak",
        daily_peak_drawdown_cap=5000.0,
        max_consecutive_losers=3,
    ),
    RiskShapeProfile(
        profile_id="standard_v1",
        label="Standard v1",
        daily_peak_drawdown_cap=5000.0,
        daily_realized_loss_cap=5000.0,
        max_consecutive_losers=3,
    ),
    RiskShapeProfile(
        profile_id="peak_7500_consec_3",
        label="Daily Peak DD 7.5K + 3 Loss Streak",
        daily_peak_drawdown_cap=7500.0,
        max_consecutive_losers=3,
    ),
    RiskShapeProfile(
        profile_id="peak_10000_consec_3",
        label="Daily Peak DD 10K + 3 Loss Streak",
        daily_peak_drawdown_cap=10000.0,
        max_consecutive_losers=3,
    ),
    RiskShapeProfile(
        profile_id="standard_v2",
        label="Standard v2",
        daily_peak_drawdown_cap=5000.0,
        daily_realized_loss_cap=5000.0,
        max_consecutive_losers=3,
        equity_peak_drawdown_cap=18000.0,
        cooldown_sessions_after_equity_breach=1,
    ),
    RiskShapeProfile(
        profile_id="tight_v1",
        label="Tight v1",
        daily_peak_drawdown_cap=5000.0,
        daily_realized_loss_cap=5000.0,
        max_consecutive_losers=3,
        equity_peak_drawdown_cap=12000.0,
        cooldown_sessions_after_equity_breach=1,
    ),
    RiskShapeProfile(
        profile_id="standard_v3",
        label="Standard v3",
        daily_peak_drawdown_cap=3000.0,
        daily_realized_loss_cap=3000.0,
        max_consecutive_losers=4,
        equity_peak_drawdown_cap=15000.0,
        cooldown_sessions_after_equity_breach=1,
    ),
    RiskShapeProfile(
        profile_id="standard_v3_075x",
        label="Standard v3 @ 75%",
        daily_peak_drawdown_cap=3000.0,
        daily_realized_loss_cap=3000.0,
        max_consecutive_losers=4,
        equity_peak_drawdown_cap=15000.0,
        cooldown_sessions_after_equity_breach=1,
        realized_pnl_scale=0.75,
    ),
    RiskShapeProfile(
        profile_id="trend_guard_v1",
        label="Trend Guard v1",
        daily_peak_drawdown_cap=3000.0,
        daily_realized_loss_cap=3000.0,
        max_consecutive_losers=4,
        equity_peak_drawdown_cap=15000.0,
        cooldown_sessions_after_equity_breach=1,
    ),
    RiskShapeProfile(
        profile_id="trend_guard_pl_v1",
        label="Trend Guard PL v1",
        daily_peak_drawdown_cap=5000.0,
        daily_realized_loss_cap=3000.0,
        max_consecutive_losers=2,
        equity_peak_drawdown_cap=12000.0,
        cooldown_sessions_after_equity_breach=1,
        realized_pnl_scale=0.75,
    ),
    RiskShapeProfile(
        profile_id="ultra_guard_gc_v1",
        label="Ultra Guard GC v1",
        daily_peak_drawdown_cap=1500.0,
        daily_realized_loss_cap=1500.0,
        max_consecutive_losers=2,
        equity_peak_drawdown_cap=4000.0,
        cooldown_sessions_after_equity_breach=1,
        realized_pnl_scale=0.5,
    ),
    RiskShapeProfile(
        profile_id="ultra_guard_pl_v1",
        label="Ultra Guard PL v1",
        daily_peak_drawdown_cap=3000.0,
        daily_realized_loss_cap=3000.0,
        max_consecutive_losers=1,
        equity_peak_drawdown_cap=6000.0,
        cooldown_sessions_after_equity_breach=1,
        realized_pnl_scale=0.75,
    ),
)


def _parse_trade_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    return datetime.fromisoformat(text)


def _load_strategy_study(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_closed_trades(path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    payload = _load_strategy_study(path)
    summary = dict(payload.get("summary") or {})
    trades = [dict(row) for row in list(summary.get("closed_trade_breakdown") or [])]
    ordered = sorted(
        trades,
        key=lambda row: (
            _parse_trade_timestamp(str(row.get("exit_timestamp") or row.get("entry_timestamp") or "")) or datetime.min.replace(tzinfo=UTC),
            str(row.get("trade_id") or ""),
        ),
    )
    return payload, ordered


def _max_drawdown(values: Sequence[float]) -> float:
    peak = 0.0
    cumulative = 0.0
    max_drawdown = 0.0
    for pnl in values:
        cumulative += float(pnl)
        if cumulative > peak:
            peak = cumulative
        drawdown = peak - cumulative
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    return max_drawdown


def _baseline_metrics(trades: Sequence[dict[str, Any]]) -> dict[str, Any]:
    pnl_values = [float(row.get("realized_pnl") or 0.0) for row in trades]
    net = sum(pnl_values)
    return {
        "trade_count": len(trades),
        "net_pnl": round(net, 6),
        "max_drawdown": round(_max_drawdown(pnl_values), 6),
    }


def _simulate_profile(trades: Sequence[dict[str, Any]], profile: RiskShapeProfile) -> dict[str, Any]:
    taken_trade_count = 0
    skipped_trade_count = 0
    halted_days: set[str] = set()
    trigger_counts: dict[str, int] = {}
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    session_key: str | None = None
    session_index = -1
    session_realized = 0.0
    session_peak = 0.0
    session_consecutive_losers = 0
    loser_scope_value: str | None = None
    halted = False
    equity_cooldown_until_session: int | None = None
    first_halt_day: str | None = None
    first_trigger: str | None = None
    realized_values: list[float] = []
    taken_trades: list[dict[str, Any]] = []

    def mark_halt(day_key: str, trigger: str) -> None:
        nonlocal halted, first_halt_day, first_trigger
        halted = True
        halted_days.add(day_key)
        trigger_counts[trigger] = trigger_counts.get(trigger, 0) + 1
        if first_halt_day is None:
            first_halt_day = day_key
            first_trigger = trigger

    for trade in trades:
        exit_ts = _parse_trade_timestamp(str(trade.get("exit_timestamp") or trade.get("entry_timestamp") or ""))
        if exit_ts is None:
            continue
        day_key = exit_ts.date().isoformat()
        if session_key != day_key:
            session_key = day_key
            session_index += 1
            session_realized = 0.0
            session_peak = 0.0
            session_consecutive_losers = 0
            loser_scope_value = None
            halted = bool(
                equity_cooldown_until_session is not None
                and session_index <= int(equity_cooldown_until_session)
            )
        if halted:
            skipped_trade_count += 1
            continue

        pnl = float(trade.get("realized_pnl") or 0.0) * float(profile.realized_pnl_scale or 1.0)
        taken_trade_count += 1
        taken_trade = dict(trade)
        taken_trade["realized_pnl"] = round(pnl, 6)
        taken_trades.append(taken_trade)
        realized_values.append(pnl)
        cumulative += pnl
        if cumulative > peak:
            peak = cumulative
        max_drawdown = max(max_drawdown, peak - cumulative)

        session_realized += pnl
        if session_realized > session_peak:
            session_peak = session_realized
        if pnl < 0:
            scope = str(profile.max_consecutive_losers_scope or "global").strip().lower()
            if scope == "same_side":
                current_scope_value = str(trade.get("side") or "")
            elif scope == "same_family":
                current_scope_value = str(trade.get("family") or "")
            elif scope == "same_session_phase":
                current_scope_value = str(trade.get("entry_session_phase") or "")
            else:
                current_scope_value = "__global__"
            if current_scope_value and current_scope_value == loser_scope_value:
                session_consecutive_losers += 1
            else:
                session_consecutive_losers = 1
                loser_scope_value = current_scope_value
        else:
            session_consecutive_losers = 0
            loser_scope_value = None

        if (
            profile.daily_realized_loss_cap is not None
            and session_realized <= -float(profile.daily_realized_loss_cap)
        ):
            mark_halt(day_key, "daily_realized_loss_cap")
            continue
        if (
            profile.equity_peak_drawdown_cap is not None
            and (peak - cumulative) >= float(profile.equity_peak_drawdown_cap)
        ):
            if int(profile.cooldown_sessions_after_equity_breach or 0) > 0:
                equity_cooldown_until_session = session_index + int(profile.cooldown_sessions_after_equity_breach)
            mark_halt(day_key, "equity_peak_drawdown_cap")
            continue
        if (
            profile.daily_peak_drawdown_cap is not None
            and (session_peak - session_realized) >= float(profile.daily_peak_drawdown_cap)
        ):
            mark_halt(day_key, "daily_peak_drawdown_cap")
            continue
        if (
            profile.max_consecutive_losers is not None
            and session_consecutive_losers >= int(profile.max_consecutive_losers)
        ):
            mark_halt(day_key, "max_consecutive_losers")

    net_pnl = sum(realized_values)
    return {
        "profile_id": profile.profile_id,
        "label": profile.label,
        "trade_count": taken_trade_count,
        "skipped_trade_count": skipped_trade_count,
        "halted_day_count": len(halted_days),
        "trigger_counts": trigger_counts,
        "first_halt_day": first_halt_day,
        "first_trigger": first_trigger,
        "net_pnl": round(net_pnl, 6),
        "max_drawdown": round(max_drawdown, 6),
        "taken_trades": taken_trades,
    }


def _profile_inputs(profile: RiskShapeProfile) -> dict[str, Any]:
    return {
        "daily_peak_drawdown_cap": profile.daily_peak_drawdown_cap,
        "daily_realized_loss_cap": profile.daily_realized_loss_cap,
        "max_consecutive_losers": profile.max_consecutive_losers,
        "max_consecutive_losers_scope": profile.max_consecutive_losers_scope,
        "equity_peak_drawdown_cap": profile.equity_peak_drawdown_cap,
        "cooldown_sessions_after_equity_breach": profile.cooldown_sessions_after_equity_breach,
        "realized_pnl_scale": profile.realized_pnl_scale,
    }


def _profile_verdict(*, baseline: dict[str, Any], candidate: dict[str, Any]) -> str:
    baseline_net = float(baseline.get("net_pnl") or 0.0)
    baseline_dd = float(baseline.get("max_drawdown") or 0.0)
    candidate_net = float(candidate.get("net_pnl") or 0.0)
    candidate_dd = float(candidate.get("max_drawdown") or 0.0)
    if baseline_dd <= 0:
        return "insufficient_baseline_drawdown"
    dd_retained = candidate_dd / baseline_dd
    pnl_retained = (candidate_net / baseline_net) if baseline_net > 0 else 0.0
    if dd_retained <= 0.5 and pnl_retained >= 0.8:
        return "promising"
    if dd_retained <= 0.7 and pnl_retained >= 0.65:
        return "watch"
    return "too_destructive"


def run_strategy_risk_shape_lab(
    *,
    study_json_paths: Sequence[str | Path],
    report_dir: Path = DEFAULT_REPORT_DIR,
    profiles: Sequence[RiskShapeProfile] = DEFAULT_RISK_SHAPE_PROFILES,
) -> dict[str, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    result_rows: list[dict[str, Any]] = []
    for study_path_like in study_json_paths:
        study_path = Path(study_path_like)
        payload, trades = _load_closed_trades(study_path)
        meta = dict(payload.get("meta") or {})
        baseline = _baseline_metrics(trades)
        profile_rows = []
        for profile in profiles:
            candidate = _simulate_profile(trades, profile)
            candidate.pop("taken_trades", None)
            candidate["inputs"] = _profile_inputs(profile)
            candidate["retained_pct"] = round(
                ((float(candidate["net_pnl"]) / float(baseline["net_pnl"])) * 100.0)
                if float(baseline["net_pnl"] or 0.0) != 0.0
                else 0.0,
                2,
            )
            candidate["drawdown_retained_pct"] = round(
                ((float(candidate["max_drawdown"]) / float(baseline["max_drawdown"])) * 100.0)
                if float(baseline["max_drawdown"] or 0.0) != 0.0
                else 0.0,
                2,
            )
            candidate["verdict"] = _profile_verdict(baseline=baseline, candidate=candidate)
            profile_rows.append(candidate)
        result_rows.append(
            {
                "study_json_path": str(study_path),
                "strategy_id": str(meta.get("standalone_strategy_id") or meta.get("strategy_id") or study_path.stem),
                "display_name": str(meta.get("display_name") or meta.get("label") or study_path.stem),
                "symbol": str(meta.get("symbol") or ""),
                "trade_count": baseline["trade_count"],
                "baseline": baseline,
                "profiles": profile_rows,
            }
        )

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "profile_count": len(profiles),
        "profiles": [{"profile_id": profile.profile_id, "label": profile.label, "inputs": _profile_inputs(profile)} for profile in profiles],
        "results": result_rows,
    }
    json_path = report_dir / "strategy_risk_shape_lab.json"
    markdown_path = report_dir / "strategy_risk_shape_lab.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_strategy_risk_shape_markdown(payload), encoding="utf-8")
    return {
        "report_json_path": json_path,
        "report_markdown_path": markdown_path,
    }


def _trade_summary_rows(trades: Sequence[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(
        (dict(row) for row in trades),
        key=lambda row: (
            _parse_trade_timestamp(str(row.get("exit_timestamp") or row.get("entry_timestamp") or "")) or datetime.min.replace(tzinfo=UTC),
            str(row.get("trade_id") or ""),
        ),
    )
    pnl_values = [float(row.get("realized_pnl") or 0.0) for row in ordered]
    gross_wins = sum(value for value in pnl_values if value > 0)
    gross_losses = sum(-value for value in pnl_values if value < 0)
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    max_run_up = 0.0
    family_counts: dict[str, int] = {}
    session_counts: dict[str, int] = {}
    for row, pnl in zip(ordered, pnl_values):
        cumulative += pnl
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
        max_run_up = max(max_run_up, cumulative)
        family = str(row.get("family") or "UNKNOWN")
        family_counts[family] = family_counts.get(family, 0) + 1
        session = str(row.get("entry_session_phase") or "UNKNOWN")
        session_counts[session] = session_counts.get(session, 0) + 1
    latest = ordered[-1] if ordered else None
    return {
        "bar_count": 0,
        "total_trades": len(ordered),
        "long_trades": sum(1 for row in ordered if str(row.get("side") or "").upper() == "LONG"),
        "short_trades": sum(1 for row in ordered if str(row.get("side") or "").upper() == "SHORT"),
        "winners": sum(1 for value in pnl_values if value > 0),
        "losers": sum(1 for value in pnl_values if value < 0),
        "profit_factor": round(gross_wins / gross_losses, 6) if gross_losses > 0 else None,
        "cumulative_realized_pnl": round(sum(pnl_values), 6),
        "cumulative_total_pnl": round(sum(pnl_values), 6),
        "max_run_up": round(max_run_up, 6),
        "max_drawdown": round(max_drawdown, 6),
        "closed_trade_breakdown": ordered,
        "session_trade_breakdown": [
            {"group": key, "trade_count": count}
            for key, count in sorted(session_counts.items(), key=lambda item: (-item[1], item[0]))
        ],
        "trade_family_breakdown": [
            {"group": key, "trade_count": count}
            for key, count in sorted(family_counts.items(), key=lambda item: (-item[1], item[0]))
        ],
        "latest_trade_summary": latest,
        "atp_summary": {
            "available": False,
            "unavailable_reason": "Derived risk-shaped artifact; ATP state transition summaries were not recomputed.",
        },
        "most_common_blocker_codes": [],
        "no_trade_regions": [],
        "session_level_behavior": [],
    }


def _build_risk_shaped_study_payload(
    *,
    source_payload: dict[str, Any],
    source_path: Path,
    profile: RiskShapeProfile,
    taken_trades: Sequence[dict[str, Any]],
    study_suffix: str,
    label_suffix: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = json.loads(json.dumps(source_payload))
    original_strategy_id = str(payload.get("standalone_strategy_id") or dict(payload.get("meta") or {}).get("strategy_id") or source_path.stem)
    new_strategy_id = f"{original_strategy_id}{study_suffix}"
    summary = _trade_summary_rows(taken_trades)
    payload["generated_at"] = datetime.now(UTC).isoformat()
    payload["standalone_strategy_id"] = new_strategy_id
    meta = dict(payload.get("meta") or {})
    meta["study_id"] = new_strategy_id
    meta["strategy_id"] = new_strategy_id
    meta.setdefault("truth_provenance", {})
    meta["truth_provenance"] = {
        **dict(meta.get("truth_provenance") or {}),
        "risk_shape_profile_id": profile.profile_id,
        "risk_shape_label": profile.label,
        "risk_shape_source_study": str(source_path),
        "risk_shape_derived": True,
        "risk_shape_realized_pnl_scale": profile.realized_pnl_scale,
    }
    meta["display_name"] = f"{str(meta.get('display_name') or original_strategy_id)}{label_suffix}"
    payload["meta"] = meta
    payload["summary"] = summary
    payload["trade_events"] = []
    payload["pnl_points"] = []
    payload["lifecycle_records"] = []
    payload["authoritative_trade_lifecycle_records"] = []
    study_row = {
        "strategy_id": new_strategy_id,
        "symbol": str(payload.get("symbol") or meta.get("symbol") or ""),
        "label": meta["display_name"],
        "study_mode": meta.get("study_mode"),
        "execution_model": meta.get("execution_model"),
        "summary_payload": summary,
    }
    return payload, study_row


def publish_strategy_risk_shaped_studies(
    *,
    study_json_paths: Sequence[str | Path],
    profile_id: str,
    report_dir: Path = DEFAULT_REPORT_DIR,
    historical_playback_dir: Path = DEFAULT_HISTORICAL_PLAYBACK_DIR,
    study_suffix: str | None = None,
    label_suffix: str | None = None,
) -> dict[str, Path]:
    profiles = {profile.profile_id: profile for profile in DEFAULT_RISK_SHAPE_PROFILES}
    profile = profiles[profile_id]
    resolved_study_suffix = study_suffix or f"_risk_shaped_{profile.profile_id}"
    resolved_label_suffix = label_suffix or f" [Risk Shaped {profile.profile_id}]"
    report_dir.mkdir(parents=True, exist_ok=True)
    historical_playback_dir.mkdir(parents=True, exist_ok=True)
    published_studies: list[dict[str, Any]] = []
    result_rows: list[dict[str, Any]] = []
    for study_path_like in study_json_paths:
        study_path = Path(study_path_like)
        payload, trades = _load_closed_trades(study_path)
        source_payload = _load_strategy_study(study_path)
        baseline = _baseline_metrics(trades)
        simulated = _simulate_profile(trades, profile)
        derived_payload, study_row = _build_risk_shaped_study_payload(
            source_payload=source_payload,
            source_path=study_path,
            profile=profile,
            taken_trades=simulated["taken_trades"],
            study_suffix=resolved_study_suffix,
            label_suffix=resolved_label_suffix,
        )
        path_pair = retest._write_study_payload(
            payload=derived_payload,
            artifact_prefix=f"historical_playback_{study_row['strategy_id']}",
            historical_playback_dir=historical_playback_dir,
        )
        published_studies.append(
            {
                **study_row,
                "strategy_study_json_path": str(path_pair["json"]),
                "strategy_study_markdown_path": str(path_pair["markdown"]),
            }
        )
        result_rows.append(
            {
                "source_study_json_path": str(study_path),
                "source_strategy_id": str(source_payload.get("standalone_strategy_id") or ""),
                "published_strategy_id": study_row["strategy_id"],
                "symbol": study_row["symbol"],
                "baseline": baseline,
                "profile": {
                    "profile_id": profile.profile_id,
                    "label": profile.label,
                    "inputs": _profile_inputs(profile),
                },
                "derived": {
                    "net_pnl": simulated["net_pnl"],
                    "max_drawdown": simulated["max_drawdown"],
                    "trade_count": simulated["trade_count"],
                    "halted_day_count": simulated["halted_day_count"],
                    "skipped_trade_count": simulated["skipped_trade_count"],
                    "trigger_counts": simulated["trigger_counts"],
                },
            }
        )
    latest_manifest = _latest_manifest_path(historical_playback_dir)
    existing_studies = _load_manifest_study_rows(latest_manifest) if latest_manifest is not None else []
    merged_studies = _merge_study_rows(existing_studies=existing_studies, new_studies=published_studies)
    manifest_run_stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
    manifest_path = retest._write_historical_playback_manifest(
        studies=merged_studies,
        run_stamp=manifest_run_stamp,
        historical_playback_dir=historical_playback_dir,
        shard_config=retest.RetestShardConfig(),
    )
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "profile_id": profile.profile_id,
        "label_suffix": resolved_label_suffix,
        "study_suffix": resolved_study_suffix,
        "historical_playback_manifest": str(manifest_path),
        "results": result_rows,
    }
    json_path = report_dir / f"strategy_risk_shape_publish_{profile.profile_id}.json"
    markdown_path = report_dir / f"strategy_risk_shape_publish_{profile.profile_id}.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_strategy_risk_shape_markdown({"generated_at": payload["generated_at"], "results": []}), encoding="utf-8")
    return {
        "report_json_path": json_path,
        "report_markdown_path": markdown_path,
        "historical_playback_manifest_path": manifest_path,
    }


def _render_strategy_risk_shape_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Strategy Risk Shape Lab",
        "",
        f"Generated: `{payload['generated_at']}`",
        "",
    ]
    for result in list(payload.get("results") or []):
        baseline = dict(result.get("baseline") or {})
        lines.extend(
            [
                f"## {result.get('display_name')}",
                "",
                f"- Strategy ID: `{result.get('strategy_id')}`",
                f"- Study JSON: `{result.get('study_json_path')}`",
                f"- Baseline net P&L: `{baseline.get('net_pnl')}`",
                f"- Baseline max drawdown: `{baseline.get('max_drawdown')}`",
                f"- Baseline trade count: `{baseline.get('trade_count')}`",
                "",
                "| Profile | Net P&L | Max DD | Retained % | DD Retained % | Halted Days | Skipped Trades | Verdict |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for profile in list(result.get("profiles") or []):
            lines.append(
                f"| {profile.get('label')} | {profile.get('net_pnl')} | {profile.get('max_drawdown')} | "
                f"{profile.get('retained_pct')} | {profile.get('drawdown_retained_pct')} | "
                f"{profile.get('halted_day_count')} | {profile.get('skipped_trade_count')} | {profile.get('verdict')} |"
            )
        lines.append("")
    return "\n".join(lines) + "\n"
