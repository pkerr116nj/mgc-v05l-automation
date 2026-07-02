"""Reusable read-only Track B trade analytics query layer."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_STRATEGY_PERFORMANCE_DIR = DEFAULT_OUTPUT_ROOT / "strategy_performance"
DEFAULT_CANONICAL_TRADE_RECORDS = DEFAULT_STRATEGY_PERFORMANCE_DIR / "canonical_trade_records.jsonl"
DEFAULT_PAIRING_SUMMARY = DEFAULT_STRATEGY_PERFORMANCE_DIR / "latest_trade_pairing_summary.json"
DEFAULT_SIDE_SESSION_REPLAY = DEFAULT_STRATEGY_PERFORMANCE_DIR / "side_session_attribution" / "side_session_trade_replay.jsonl"
DEFAULT_BLOCKED_INTENTS = Path("outputs") / "probationary_pattern_engine" / "paper_session" / "blocked_strategy_intents.jsonl"
DEFAULT_CRFD_ROWS = DEFAULT_OUTPUT_ROOT / "research" / "canonical_research_feature_dataset" / "research_feature_dataset.jsonl"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "trade_analytics_query_layer"

SCORECARD_JSON = "latest_trade_analytics_scorecard.json"
SCORECARD_MD = "latest_trade_analytics_scorecard.md"
STRATEGY_MD = "strategy_performance_scorecard.md"
EXIT_MD = "exit_quality_scorecard.md"
SESSION_MD = "session_trade_attribution.md"
DATA_QUALITY_MD = "trade_data_quality_report.md"
CONTRACT_MD = "trade_analytics_query_layer_contract.md"

SCHEMA_VERSION = "track_b_trade_analytics_query_layer_v1"
HOLD_SECONDS_PER_MINUTE = 60.0


@dataclass(frozen=True)
class TradeAnalyticsQueryResult:
    report: dict[str, Any]
    json_path: Path
    markdown_path: Path
    strategy_path: Path
    exit_path: Path
    session_path: Path
    data_quality_path: Path
    contract_path: Path


def run_trade_analytics_query_layer(
    *,
    canonical_records_path: Path = DEFAULT_CANONICAL_TRADE_RECORDS,
    pairing_summary_path: Path = DEFAULT_PAIRING_SUMMARY,
    side_session_replay_path: Path = DEFAULT_SIDE_SESSION_REPLAY,
    blocked_intents_path: Path = DEFAULT_BLOCKED_INTENTS,
    crfd_rows_path: Path = DEFAULT_CRFD_ROWS,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    now: datetime | str | None = None,
    max_snapshot_bytes: int | None = None,
) -> TradeAnalyticsQueryResult:
    generated_at = _coerce_now(now)
    canonical_records = _read_jsonl(canonical_records_path)
    side_session_rows = _read_jsonl(side_session_replay_path)
    blocked_intents = _read_jsonl(blocked_intents_path)
    crfd_rows = _read_jsonl(crfd_rows_path)
    pairing_summary = _read_json_mapping(pairing_summary_path)
    report = build_trade_analytics_scorecard(
        canonical_records,
        side_session_rows=side_session_rows,
        blocked_intents=blocked_intents,
        crfd_rows=crfd_rows,
        pairing_summary=pairing_summary,
        generated_at=generated_at,
        source_paths={
            "canonical_records": canonical_records_path,
            "pairing_summary": pairing_summary_path,
            "side_session_replay": side_session_replay_path,
            "blocked_intents": blocked_intents_path,
            "crfd_rows": crfd_rows_path,
        },
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    config = BoundedSnapshotConfig(max_bytes=max_snapshot_bytes) if max_snapshot_bytes else BoundedSnapshotConfig()
    json_path = output_dir / SCORECARD_JSON
    write_bounded_snapshot_json(json_path, report, config=config)
    markdown_path = output_dir / SCORECARD_MD
    markdown_path.write_text(render_scorecard_markdown(report), encoding="utf-8")
    strategy_path = output_dir / STRATEGY_MD
    strategy_path.write_text(render_strategy_markdown(report), encoding="utf-8")
    exit_path = output_dir / EXIT_MD
    exit_path.write_text(render_exit_markdown(report), encoding="utf-8")
    session_path = output_dir / SESSION_MD
    session_path.write_text(render_session_markdown(report), encoding="utf-8")
    data_quality_path = output_dir / DATA_QUALITY_MD
    data_quality_path.write_text(render_data_quality_markdown(report), encoding="utf-8")
    contract_path = output_dir / CONTRACT_MD
    contract_path.write_text(render_contract_markdown(report), encoding="utf-8")
    return TradeAnalyticsQueryResult(
        report=report,
        json_path=json_path,
        markdown_path=markdown_path,
        strategy_path=strategy_path,
        exit_path=exit_path,
        session_path=session_path,
        data_quality_path=data_quality_path,
        contract_path=contract_path,
    )


def build_trade_analytics_scorecard(
    canonical_records: Sequence[Mapping[str, Any]],
    *,
    side_session_rows: Sequence[Mapping[str, Any]] = (),
    blocked_intents: Sequence[Mapping[str, Any]] = (),
    crfd_rows: Sequence[Mapping[str, Any]] = (),
    pairing_summary: Mapping[str, Any] | None = None,
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> dict[str, Any]:
    trades = [normalize_trade_record(row) for row in canonical_records]
    side_session_index = _side_session_index(side_session_rows)
    for trade in trades:
        replay = side_session_index.get(_trade_join_key(trade))
        if replay:
            _merge_replay_fields(trade, replay)
    crfd_index = _CrfdIndex(crfd_rows)
    for trade in trades:
        crfd = crfd_index.latest_at_or_before(contract=str(trade.get("symbol") or ""), timestamp=_parse_datetime(trade.get("entry_time")))
        if crfd:
            trade["crfd_joined"] = True
            trade["crfd_session"] = crfd.get("session")
            trade["crfd_vwap_relation"] = crfd.get("vwap_relation")
            trade["crfd_globex_avwap_relation"] = crfd.get("avwap_relation_globex_session_open_18et")
        else:
            trade["crfd_joined"] = False

    paired = [trade for trade in trades if trade.get("pairing_status") == "PAIRED"]
    closed = [trade for trade in paired if trade.get("trade_status") == "CLOSED"]
    quality = build_data_quality_view(trades, pairing_summary=pairing_summary or {})
    strategy = aggregate_by_key(closed, key="lane_id", label_key="strategy_id")
    exits = aggregate_by_key(closed, key="exit_reason", label_key="exit_policy")
    sessions = aggregate_by_key(closed, key="session_label", label_key="session_label")
    regime = aggregate_by_key([trade for trade in closed if trade.get("crfd_joined")], key="crfd_session", label_key="crfd_vwap_relation")
    blocked = build_blocked_opportunity_view(blocked_intents)
    hold_time = build_hold_time_view(closed)
    mfe_mae = build_mfe_mae_view(closed)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "analytics_only": True,
        "diagnostic_only": True,
        "production_effect": False,
        "source_paths": {key: str(value) for key, value in (source_paths or {}).items()},
        "overall": {
            "canonical_record_count": len(trades),
            "paired_trade_count": len(paired),
            "closed_paired_trade_count": len(closed),
            "unpaired_entry_count": sum(1 for trade in trades if trade.get("pairing_status") == "UNPAIRED_ENTRY"),
            "unpaired_exit_count": sum(1 for trade in trades if trade.get("pairing_status") == "UNPAIRED_EXIT"),
            "pairing_rate": quality.get("pairing_rate"),
            "win_rate": _win_rate(closed),
            "expectancy_proxy": _average(_numeric_values(closed, "pnl_proxy_points")),
            "average_mfe": _average(_numeric_values(closed, "mfe_points")),
            "average_mae": _average(_numeric_values(closed, "mae_points")),
            "average_hold_minutes": _average(_numeric_values(closed, "hold_minutes")),
        },
        "views": {
            "paired_trade_view": _sample_rows(paired),
            "strategy_performance_view": strategy,
            "exit_quality_view": exits,
            "session_attribution_view": sessions,
            "mfe_mae_view": mfe_mae,
            "hold_time_view": hold_time,
            "blocked_missed_opportunity_view": blocked,
            "regime_crfd_join_view": regime,
            "data_quality_view": quality,
        },
        "answerable_questions": {
            "positive_expectancy_strategies": _positive_expectancy(strategy),
            "poor_exit_candidates": _poor_exit_candidates(exits),
            "edge_sessions": _positive_expectancy(sessions),
            "regime_edge_joinable": bool(regime.get("groups")),
            "analytics_incomplete_reasons": quality.get("warnings"),
        },
        "limitations": _limitations(trades, crfd_rows),
        "safety_contract": {
            "broker_actions": False,
            "runtime_restart": False,
            "managed_exit_restart": False,
            "strategy_changes": False,
            "trading_gates": False,
            "db_mutation": False,
        },
    }


def normalize_trade_record(row: Mapping[str, Any]) -> dict[str, Any]:
    pnl = _number(row.get("realized_pnl_points"))
    if pnl is None:
        pnl = _number(row.get("mark_to_market_pnl_points"))
    hold_seconds = _number(row.get("hold_seconds"))
    trade = dict(row)
    trade.update(
        {
            "pnl_proxy_points": pnl,
            "mfe_points": _number(row.get("mfe_points")),
            "mae_points": _number(row.get("mae_points")),
            "hold_minutes": round(hold_seconds / HOLD_SECONDS_PER_MINUTE, 4) if hold_seconds is not None else None,
            "entry_time": row.get("entry_time"),
            "exit_time": row.get("exit_time"),
            "strategy_id": row.get("strategy_id") or row.get("lane_id") or "UNKNOWN",
            "lane_id": row.get("lane_id") or "UNKNOWN",
            "session_label": row.get("session_label") or "UNKNOWN",
            "exit_reason": row.get("exit_reason") or row.get("exit_policy") or "UNKNOWN",
            "exit_policy": row.get("exit_policy") or row.get("exit_reason") or "UNKNOWN",
            "symbol": row.get("symbol") or row.get("local_symbol") or "UNKNOWN",
            "win": pnl is not None and pnl > 0,
            "loss": pnl is not None and pnl < 0,
        }
    )
    return trade


def aggregate_by_key(rows: Sequence[Mapping[str, Any]], *, key: str, label_key: str) -> dict[str, Any]:
    groups: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        groups.setdefault(str(row.get(key) or "UNKNOWN"), []).append(row)
    result = []
    for group_key, group_rows in sorted(groups.items()):
        pnl_values = _numeric_values(group_rows, "pnl_proxy_points")
        mfe_values = _numeric_values(group_rows, "mfe_points")
        mae_values = _numeric_values(group_rows, "mae_points")
        hold_values = _numeric_values(group_rows, "hold_minutes")
        best = max(group_rows, key=lambda row: _number(row.get("pnl_proxy_points")) if _number(row.get("pnl_proxy_points")) is not None else float("-inf"))
        worst = min(group_rows, key=lambda row: _number(row.get("pnl_proxy_points")) if _number(row.get("pnl_proxy_points")) is not None else float("inf"))
        result.append(
            {
                "key": group_key,
                "label": group_rows[0].get(label_key),
                "trade_count": len(group_rows),
                "paired_count": sum(1 for row in group_rows if row.get("pairing_status") == "PAIRED"),
                "unpaired_count": sum(1 for row in group_rows if row.get("pairing_status") != "PAIRED"),
                "win_rate": _win_rate(group_rows),
                "average_pnl_proxy_points": _average(pnl_values),
                "expectancy_proxy": _average(pnl_values),
                "average_r_proxy": None,
                "average_mfe": _average(mfe_values),
                "average_mae": _average(mae_values),
                "average_hold_minutes": _average(hold_values),
                "median_hold_minutes": _median(hold_values),
                "best_trade": _trade_ref(best),
                "worst_trade": _trade_ref(worst),
                "data_completeness_warnings": _group_warnings(group_rows),
            }
        )
    return {"group_count": len(result), "groups": sorted(result, key=lambda item: item.get("expectancy_proxy") or 0.0, reverse=True)}


def build_data_quality_view(trades: Sequence[Mapping[str, Any]], *, pairing_summary: Mapping[str, Any]) -> dict[str, Any]:
    trade_ids = [str(row.get("trade_id") or "") for row in trades if row.get("trade_id")]
    duplicate_trade_ids = {trade_id: count for trade_id, count in _counts(trade_ids).items() if count > 1}
    warnings: list[str] = []
    missing_pnl = sum(1 for row in trades if row.get("pnl_proxy_points") is None)
    missing_mfe = sum(1 for row in trades if row.get("mfe_points") is None)
    missing_mae = sum(1 for row in trades if row.get("mae_points") is None)
    if missing_pnl:
        warnings.append("missing_pnl_proxy")
    if missing_mfe or missing_mae:
        warnings.append("missing_mfe_mae")
    if pairing_summary.get("unpaired_entry_count") or pairing_summary.get("unpaired_exit_count"):
        warnings.append("unpaired_trade_records_present")
    return {
        "pairing_summary": dict(pairing_summary),
        "pairing_rate": pairing_summary.get("pairing_rate") or _rate(
            sum(1 for row in trades if row.get("pairing_status") == "PAIRED"),
            sum(1 for row in trades if row.get("pairing_status") in {"PAIRED", "UNPAIRED_ENTRY"}),
        ),
        "record_count": len(trades),
        "missing_timestamps": sum(1 for row in trades if not row.get("entry_time")),
        "missing_strategy_ids": sum(1 for row in trades if not row.get("strategy_id") or row.get("strategy_id") == "UNKNOWN"),
        "missing_pnl_proxy": missing_pnl,
        "missing_mfe": missing_mfe,
        "missing_mae": missing_mae,
        "duplicate_trade_ids": duplicate_trade_ids,
        "unsupported_instruments": sorted({str(row.get("symbol")) for row in trades if str(row.get("symbol") or "").upper() in {"MBT", "MET", "MSL", "SOL"}}),
        "warnings": warnings,
    }


def build_blocked_opportunity_view(blocked_intents: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = list(blocked_intents)
    return {
        "observation_count": len(rows),
        "by_symbol": _counts(str(row.get("symbol") or _symbol_from_bar_id(row.get("bar_id")) or "UNKNOWN") for row in rows),
        "by_blocker": _counts(str(row.get("blocker_classification") or row.get("bridge_classification") or "UNKNOWN") for row in rows),
        "latest_examples": _sample_rows(rows[-10:], keys=("generated_at", "bar_id", "symbol", "blocker_classification", "bridge_classification")),
        "analysis_status": "AVAILABLE_FOR_LATER_MISSED_OPPORTUNITY_ANALYSIS" if rows else "NO_BLOCKED_INTENTS_AVAILABLE",
    }


def build_hold_time_view(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    values = _numeric_values(rows, "hold_minutes")
    return {
        "count": len(values),
        "average_hold_minutes": _average(values),
        "median_hold_minutes": _median(values),
        "min_hold_minutes": min(values) if values else None,
        "max_hold_minutes": max(values) if values else None,
    }


def build_mfe_mae_view(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    mfe = _numeric_values(rows, "mfe_points")
    mae = _numeric_values(rows, "mae_points")
    return {
        "mfe_available_count": len(mfe),
        "mae_available_count": len(mae),
        "average_mfe": _average(mfe),
        "average_mae": _average(mae),
        "mfe_mae_warning": "MFE_MAE_PARTIAL_OR_MISSING" if len(mfe) < len(rows) or len(mae) < len(rows) else None,
    }


def render_scorecard_markdown(report: Mapping[str, Any]) -> str:
    overall = report.get("overall") or {}
    return "\n".join(
        [
            "# Trade Analytics Scorecard",
            "",
            f"- Generated at: {report.get('generated_at')}",
            f"- Canonical records: {overall.get('canonical_record_count')}",
            f"- Paired trades: {overall.get('paired_trade_count')}",
            f"- Pairing rate: {overall.get('pairing_rate')}",
            f"- Win rate: {overall.get('win_rate')}",
            f"- Expectancy proxy: {overall.get('expectancy_proxy')}",
            f"- Average MFE: {overall.get('average_mfe')}",
            f"- Average MAE: {overall.get('average_mae')}",
            "",
        ]
    )


def render_strategy_markdown(report: Mapping[str, Any]) -> str:
    return _render_group_markdown("Strategy Performance Scorecard", report, "strategy_performance_view")


def render_exit_markdown(report: Mapping[str, Any]) -> str:
    return _render_group_markdown("Exit Quality Scorecard", report, "exit_quality_view")


def render_session_markdown(report: Mapping[str, Any]) -> str:
    return _render_group_markdown("Session Trade Attribution", report, "session_attribution_view")


def render_data_quality_markdown(report: Mapping[str, Any]) -> str:
    quality = ((report.get("views") or {}).get("data_quality_view") or {})
    lines = ["# Trade Data Quality Report", "", f"- Record count: {quality.get('record_count')}", f"- Pairing rate: {quality.get('pairing_rate')}", f"- Missing P&L proxy: {quality.get('missing_pnl_proxy')}", f"- Missing MFE: {quality.get('missing_mfe')}", f"- Missing MAE: {quality.get('missing_mae')}", f"- Warnings: {', '.join(quality.get('warnings') or []) or 'none'}", ""]
    pairing = quality.get("pairing_summary") or {}
    lines.extend([f"- Total entries: {pairing.get('total_entries')}", f"- Total exits: {pairing.get('total_exits')}", f"- Unpaired entries: {pairing.get('unpaired_entry_count')}", f"- Unpaired exits: {pairing.get('unpaired_exit_count')}", ""])
    return "\n".join(lines)


def render_contract_markdown(report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Trade Analytics Query Layer Contract",
            "",
            "The query layer is read-only and analytics-only. It creates reusable views from canonical trade records and supporting diagnostics.",
            "",
            "## Views",
            "- canonical trade dataset",
            "- paired trade view",
            "- strategy performance view",
            "- exit quality view",
            "- session attribution view",
            "- MFE/MAE view",
            "- hold-time view",
            "- blocked/missed opportunity view",
            "- CRFD/regime join view when available",
            "",
            "## Limitations",
            "* " + "\n* ".join(report.get("limitations") or []),
            "",
            "## Safety",
            "- No broker actions.",
            "- No runtime or Managed Exit changes.",
            "- No strategy changes or trading gates.",
        ]
    )


def _render_group_markdown(title: str, report: Mapping[str, Any], view_key: str) -> str:
    view = ((report.get("views") or {}).get(view_key) or {})
    lines = [f"# {title}", "", "| Key | Trades | Win rate | Expectancy | Avg MFE | Avg MAE | Avg hold min | Warnings |", "|---|---:|---:|---:|---:|---:|---:|---|"]
    for row in (view.get("groups") or [])[:50]:
        lines.append(
            f"| {row.get('key')} | {row.get('trade_count')} | {row.get('win_rate')} | {row.get('expectancy_proxy')} | "
            f"{row.get('average_mfe')} | {row.get('average_mae')} | {row.get('average_hold_minutes')} | "
            f"{', '.join(row.get('data_completeness_warnings') or [])} |"
        )
    return "\n".join(lines) + "\n"


def _limitations(trades: Sequence[Mapping[str, Any]], crfd_rows: Sequence[Mapping[str, Any]]) -> list[str]:
    notes = ["P&L/R are reported as explicit proxies where native R is unavailable."]
    if any(row.get("mfe_points") is None or row.get("mae_points") is None for row in trades):
        notes.append("MFE/MAE are incomplete for part of the canonical trade dataset.")
    if not crfd_rows:
        notes.append("CRFD/regime joins unavailable.")
    elif not any(row.get("crfd_joined") for row in trades):
        notes.append("CRFD rows exist but did not join to current trade timestamps/contracts.")
    return notes


def _merge_replay_fields(trade: dict[str, Any], replay: Mapping[str, Any]) -> None:
    for key in ("mfe_points", "mae_points"):
        if trade.get(key) is None:
            trade[key] = _number(replay.get(key))
    trade["exit_efficiency_proxy"] = replay.get("current_exit_vs_best_horizon_points")
    trade["exit_quality_classification"] = replay.get("exit_policy_classification")


def _side_session_index(rows: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str, str], Mapping[str, Any]]:
    result: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    for row in rows:
        key = (str(row.get("lane_id") or ""), str(row.get("entry_time") or ""), str(row.get("exit_time") or ""))
        result[key] = row
    return result


def _trade_join_key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (str(row.get("lane_id") or ""), str(row.get("entry_time") or ""), str(row.get("exit_time") or ""))


def _positive_expectancy(view: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [row for row in view.get("groups", []) if (row.get("expectancy_proxy") or 0.0) > 0][:20]


def _poor_exit_candidates(view: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [row for row in view.get("groups", []) if (row.get("expectancy_proxy") or 0.0) < 0][:20]


def _group_warnings(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    warnings: list[str] = []
    if any(row.get("pnl_proxy_points") is None for row in rows):
        warnings.append("missing_pnl_proxy")
    if any(row.get("mfe_points") is None for row in rows):
        warnings.append("missing_mfe")
    if any(row.get("mae_points") is None for row in rows):
        warnings.append("missing_mae")
    if any(row.get("hold_minutes") is None for row in rows):
        warnings.append("missing_hold_time")
    return warnings


def _trade_ref(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "trade_id": row.get("trade_id"),
        "lane_id": row.get("lane_id"),
        "symbol": row.get("symbol"),
        "entry_time": row.get("entry_time"),
        "exit_time": row.get("exit_time"),
        "pnl_proxy_points": row.get("pnl_proxy_points"),
    }


def _sample_rows(rows: Sequence[Mapping[str, Any]], keys: Sequence[str] | None = None, limit: int = 20) -> list[dict[str, Any]]:
    keep = keys or ("trade_id", "lane_id", "symbol", "side", "entry_time", "exit_time", "pnl_proxy_points", "pairing_status")
    return [{key: row.get(key) for key in keep} for row in rows[:limit]]


class _CrfdIndex:
    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        by_contract: dict[str, list[tuple[datetime, Mapping[str, Any]]]] = {}
        for row in rows:
            ts = _parse_datetime(row.get("observation_time"))
            contract = str(row.get("contract") or "").upper()
            if ts is None or not contract:
                continue
            by_contract.setdefault(contract, []).append((ts, row))
        self._rows = {contract: sorted(values, key=lambda item: item[0]) for contract, values in by_contract.items()}

    def latest_at_or_before(self, *, contract: str, timestamp: datetime | None) -> Mapping[str, Any] | None:
        rows = self._rows.get(_root_symbol(contract), ()) or self._rows.get(str(contract or "").upper(), ())
        candidate: Mapping[str, Any] | None = None
        for row_ts, row in rows:
            if timestamp is not None and row_ts > timestamp:
                break
            candidate = row
        return candidate


def _numeric_values(rows: Sequence[Mapping[str, Any]], key: str) -> list[float]:
    return [value for value in (_number(row.get(key)) for row in rows) if value is not None]


def _win_rate(rows: Sequence[Mapping[str, Any]]) -> float | None:
    pnl = _numeric_values(rows, "pnl_proxy_points")
    return _rate(sum(1 for value in pnl if value > 0), len(pnl))


def _average(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _median(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return round(float(median(values)), 6)


def _rate(part: int, whole: int) -> float | None:
    if whole <= 0:
        return None
    return round(part / whole, 6)


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _counts(values: Sequence[Any]) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values:
        key = str(value or "UNKNOWN")
        result[key] = result.get(key, 0) + 1
    return dict(sorted(result.items(), key=lambda item: (-item[1], item[0])))


def _symbol_from_bar_id(value: Any) -> str | None:
    if not value:
        return None
    return str(value).split("|", 1)[0]


def _root_symbol(value: Any) -> str:
    root = "".join(ch for ch in str(value or "").upper() if ch.isalpha())
    if root.startswith("MGC"):
        return "MGC"
    if root.startswith("GC"):
        return "GC"
    return root


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return []
    return rows


def _read_json_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
