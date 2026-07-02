"""Diagnostic scorecards built from canonical Track B trade outcomes."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.execution_core.track_b_trade_outcome_layer import (
    DEFAULT_OUTPUT_DIR as DEFAULT_OUTCOME_LAYER_DIR,
    OUTCOMES_JSONL,
    SUMMARY_JSON as OUTCOME_SUMMARY_JSON,
)


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_OUTCOMES_PATH = DEFAULT_OUTCOME_LAYER_DIR / OUTCOMES_JSONL
DEFAULT_OUTCOME_SUMMARY_PATH = DEFAULT_OUTCOME_LAYER_DIR / OUTCOME_SUMMARY_JSON
DEFAULT_T1_SCORECARD_PATH = DEFAULT_OUTPUT_ROOT / "trade_analytics_query_layer" / "latest_trade_analytics_scorecard.json"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "trade_outcome_scorecards"

SCORECARDS_JSON = "latest_trade_outcome_scorecards.json"
SCORECARDS_MD = "latest_trade_outcome_scorecards.md"
STRATEGY_MD = "strategy_edge_scorecard.md"
EXIT_MD = "exit_policy_scorecard.md"
SESSION_MD = "session_edge_scorecard.md"
INSTRUMENT_MD = "instrument_edge_scorecard.md"
REVIEW_MD = "trade_review_candidates.md"
DATA_QUALITY_MD = "trade_scorecard_data_quality.md"

SCHEMA_VERSION = "track_b_trade_outcome_scorecards_v1"
MINIMUM_SAMPLE_THRESHOLD = 5
PROMISING_SAMPLE_THRESHOLD = 10


@dataclass(frozen=True)
class TradeOutcomeScorecardResult:
    scorecards: dict[str, Any]
    json_path: Path
    markdown_path: Path
    strategy_path: Path
    exit_path: Path
    session_path: Path
    instrument_path: Path
    review_path: Path
    data_quality_path: Path


def run_trade_outcome_scorecards(
    *,
    outcomes_path: Path = DEFAULT_OUTCOMES_PATH,
    outcome_summary_path: Path = DEFAULT_OUTCOME_SUMMARY_PATH,
    t1_scorecard_path: Path = DEFAULT_T1_SCORECARD_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    now: datetime | str | None = None,
    max_snapshot_bytes: int | None = None,
) -> TradeOutcomeScorecardResult:
    generated_at = _coerce_now(now)
    outcomes = _read_jsonl(outcomes_path)
    outcome_summary = _read_json_mapping(outcome_summary_path)
    t1_scorecard = _read_json_mapping(t1_scorecard_path)
    scorecards = build_trade_outcome_scorecards(
        outcomes,
        outcome_summary=outcome_summary,
        t1_scorecard=t1_scorecard,
        generated_at=generated_at,
        source_paths={
            "canonical_trade_outcomes": outcomes_path,
            "trade_outcome_summary": outcome_summary_path,
            "trade_analytics_scorecard": t1_scorecard_path,
        },
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    config = BoundedSnapshotConfig(max_bytes=max_snapshot_bytes) if max_snapshot_bytes else BoundedSnapshotConfig()
    json_path = output_dir / SCORECARDS_JSON
    write_bounded_snapshot_json(json_path, scorecards, config=config)
    markdown_path = output_dir / SCORECARDS_MD
    markdown_path.write_text(render_overview_markdown(scorecards), encoding="utf-8")
    strategy_path = output_dir / STRATEGY_MD
    strategy_path.write_text(render_group_markdown("Strategy Edge Scorecard", scorecards["scorecards"]["strategy_edge"]), encoding="utf-8")
    exit_path = output_dir / EXIT_MD
    exit_path.write_text(render_group_markdown("Exit Policy Scorecard", scorecards["scorecards"]["exit_policy"]), encoding="utf-8")
    session_path = output_dir / SESSION_MD
    session_path.write_text(render_group_markdown("Session Edge Scorecard", scorecards["scorecards"]["session_edge"]), encoding="utf-8")
    instrument_path = output_dir / INSTRUMENT_MD
    instrument_path.write_text(render_group_markdown("Instrument Edge Scorecard", scorecards["scorecards"]["instrument_edge"]), encoding="utf-8")
    review_path = output_dir / REVIEW_MD
    review_path.write_text(render_review_markdown(scorecards), encoding="utf-8")
    data_quality_path = output_dir / DATA_QUALITY_MD
    data_quality_path.write_text(render_data_quality_markdown(scorecards), encoding="utf-8")
    return TradeOutcomeScorecardResult(
        scorecards=scorecards,
        json_path=json_path,
        markdown_path=markdown_path,
        strategy_path=strategy_path,
        exit_path=exit_path,
        session_path=session_path,
        instrument_path=instrument_path,
        review_path=review_path,
        data_quality_path=data_quality_path,
    )


def build_trade_outcome_scorecards(
    outcomes: Sequence[Mapping[str, Any]],
    *,
    outcome_summary: Mapping[str, Any] | None = None,
    t1_scorecard: Mapping[str, Any] | None = None,
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> dict[str, Any]:
    rows = [dict(row) for row in outcomes if row.get("diagnostic_only") is True or row.get("schema_version")]
    strategy = aggregate_outcomes(rows, key_fields=("strategy_id", "lane_id"))
    exits = aggregate_outcomes(rows, key_fields=("exit_policy",))
    sessions = aggregate_outcomes(rows, key_fields=("session_at_entry",))
    instruments = aggregate_outcomes(rows, key_fields=("instrument", "contract"))
    review = build_review_candidates(rows, outcome_summary=outcome_summary or {})
    data_quality = build_scorecard_data_quality(rows, outcome_summary=outcome_summary or {}, t1_scorecard=t1_scorecard or {})
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "analytics_only": True,
        "diagnostic_only": True,
        "production_effect": False,
        "source_paths": {key: str(value) for key, value in (source_paths or {}).items()},
        "overall": {
            "outcome_count": len(rows),
            "average_realized_points": _average(_numeric_values(rows, "realized_points")),
            "average_pnl_proxy": _average(_numeric_values(rows, "realized_pnl_proxy")),
            "win_rate": _win_rate(rows),
            "mfe_available_count": sum(1 for row in rows if row.get("mfe_points") is not None),
            "mae_available_count": sum(1 for row in rows if row.get("mae_points") is not None),
            "r_proxy_available_count": sum(1 for row in rows if row.get("realized_r_proxy") is not None),
            "data_quality_limited": bool(data_quality.get("limiting_gaps")),
        },
        "scorecards": {
            "strategy_edge": {
                "grouping": "strategy_id,lane_id",
                "groups": strategy,
                "recommendations": _strategy_recommendations(strategy),
            },
            "exit_policy": {
                "grouping": "exit_policy",
                "groups": exits,
                "recommendations": _exit_recommendations(exits),
            },
            "session_edge": {
                "grouping": "session_at_entry",
                "groups": sessions,
                "recommendations": _generic_edge_recommendations(sessions, label="session"),
            },
            "instrument_edge": {
                "grouping": "instrument,contract",
                "groups": instruments,
                "recommendations": _generic_edge_recommendations(instruments, label="instrument"),
            },
        },
        "review_candidates": review,
        "data_quality": data_quality,
        "recommendations": build_overall_recommendations(strategy, exits, data_quality),
        "safety_contract": {
            "broker_actions": False,
            "runtime_restart": False,
            "managed_exit_restart": False,
            "strategy_changes": False,
            "trading_gates": False,
            "db_mutation": False,
            "production_changes_recommended": False,
        },
    }


def aggregate_outcomes(rows: Sequence[Mapping[str, Any]], *, key_fields: Sequence[str]) -> list[dict[str, Any]]:
    groups: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        group_key = " | ".join(str(row.get(field) or "UNKNOWN") for field in key_fields)
        groups.setdefault(group_key, []).append(row)
    aggregated = [_aggregate_group(key, group_rows) for key, group_rows in groups.items()]
    return sorted(
        aggregated,
        key=lambda row: (row.get("sample_adjusted_expectancy_proxy") or 0.0, row.get("average_pnl_proxy") or 0.0),
        reverse=True,
    )


def build_review_candidates(rows: Sequence[Mapping[str, Any]], *, outcome_summary: Mapping[str, Any]) -> dict[str, Any]:
    closed = list(rows)
    worst = sorted([row for row in closed if _number(row.get("realized_pnl_proxy")) is not None], key=lambda row: _number(row.get("realized_pnl_proxy")) or 0.0)[:20]
    best = sorted([row for row in closed if _number(row.get("realized_pnl_proxy")) is not None], key=lambda row: _number(row.get("realized_pnl_proxy")) or 0.0, reverse=True)[:20]
    long_hold_losers = sorted(
        [
            row
            for row in closed
            if (_number(row.get("realized_pnl_proxy")) or 0.0) < 0 and _number(row.get("hold_seconds")) is not None
        ],
        key=lambda row: _number(row.get("hold_seconds")) or 0.0,
        reverse=True,
    )[:20]
    high_mae = sorted(
        [row for row in closed if _number(row.get("mae_points")) is not None],
        key=lambda row: _number(row.get("mae_points")) or 0.0,
    )[:20]
    important_missing = [
        row
        for row in closed
        if "missing_mfe" in set(row.get("data_quality_flags") or ())
        or "missing_mae" in set(row.get("data_quality_flags") or ())
        or "missing_realized_r_proxy" in set(row.get("data_quality_flags") or ())
    ][:20]
    return {
        "worst_realized_pnl_proxy_trades": [_trade_ref(row) for row in worst],
        "best_realized_pnl_proxy_trades": [_trade_ref(row) for row in best],
        "long_hold_losers": [_trade_ref(row) for row in long_hold_losers],
        "high_mae_trades": [_trade_ref(row) for row in high_mae],
        "missing_data_important_trades": [_trade_ref(row) for row in important_missing],
        "unpaired_incomplete_summary": (outcome_summary.get("data_quality") or {}).get("unpaired_or_incomplete_count"),
    }


def build_scorecard_data_quality(
    rows: Sequence[Mapping[str, Any]],
    *,
    outcome_summary: Mapping[str, Any],
    t1_scorecard: Mapping[str, Any],
) -> dict[str, Any]:
    flags = _counts(flag for row in rows for flag in row.get("data_quality_flags", ()))
    limiting_gaps: list[str] = []
    if flags.get("missing_realized_r_proxy"):
        limiting_gaps.append("R proxy unavailable; risk-normalized strategy ranking is not supported yet.")
    if flags.get("missing_mfe") or flags.get("missing_mae"):
        limiting_gaps.append("MFE/MAE sparse; exit-quality conclusions must remain conservative.")
    if flags.get("missing_crfd_regime_join"):
        limiting_gaps.append("CRFD/regime joins sparse; regime attribution is diagnostic only.")
    unpaired = (outcome_summary.get("data_quality") or {}).get("unpaired_or_incomplete_count")
    if unpaired:
        limiting_gaps.append("Unpaired/incomplete records remain outside completed outcome rows.")
    t1_warnings = ((t1_scorecard.get("views") or {}).get("data_quality_view") or {}).get("warnings") or []
    missing_fields = {
        "strategy_id": sum(1 for row in rows if not row.get("strategy_id") or row.get("strategy_id") == "UNKNOWN"),
        "lane_id": sum(1 for row in rows if not row.get("lane_id") or row.get("lane_id") == "UNKNOWN"),
        "session_at_entry": sum(1 for row in rows if not row.get("session_at_entry") or row.get("session_at_entry") == "UNKNOWN"),
        "exit_policy": sum(1 for row in rows if not row.get("exit_policy") or row.get("exit_policy") == "UNKNOWN"),
    }
    return {
        "top_flags": flags,
        "limiting_gaps": limiting_gaps,
        "missing_fields": missing_fields,
        "t1_cross_check_warnings": t1_warnings,
        "exit_quality_conclusion_allowed": not (flags.get("missing_mfe") or flags.get("missing_mae")),
        "production_change_recommendation_allowed": False,
    }


def build_overall_recommendations(
    strategy_groups: Sequence[Mapping[str, Any]],
    exit_groups: Sequence[Mapping[str, Any]],
    data_quality: Mapping[str, Any],
) -> list[str]:
    recommendations: list[str] = []
    promising = [row for row in strategy_groups if row.get("scorecard_label") == "PROMISING_RESEARCH"]
    weak = [row for row in strategy_groups if row.get("scorecard_label") == "WEAK_REVIEW"]
    if promising:
        recommendations.append("Prioritize manual research review for the highest sample-adjusted positive strategy/lane groups.")
    if weak:
        recommendations.append("Review weak strategy/lane groups before considering any future shadow changes.")
    if any("MFE/MAE sparse" in item for item in data_quality.get("limiting_gaps") or []):
        recommendations.append("Improve MFE/MAE coverage before drawing strong exit-policy conclusions.")
    if exit_groups:
        recommendations.append("Use exit-policy scorecards for manual review only; no production exit changes are recommended.")
    return recommendations or ["Collect more completed outcomes before ranking strategy or exit quality."]


def render_overview_markdown(scorecards: Mapping[str, Any]) -> str:
    overall = scorecards.get("overall") or {}
    return "\n".join(
        [
            "# Trade Outcome Scorecards",
            "",
            f"- Generated at: {scorecards.get('generated_at')}",
            f"- Outcomes: {overall.get('outcome_count')}",
            f"- Win rate: {overall.get('win_rate')}",
            f"- Average realized points: {overall.get('average_realized_points')}",
            f"- Average P&L proxy: {overall.get('average_pnl_proxy')}",
            f"- Data-quality limited: {overall.get('data_quality_limited')}",
            "",
        ]
    )


def render_group_markdown(title: str, scorecard: Mapping[str, Any]) -> str:
    lines = [
        f"# {title}",
        "",
        "| Key | Trades | Win rate | Avg points | Avg P&L | Sample-adjusted | Label | Warning |",
        "|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in (scorecard.get("groups") or [])[:50]:
        lines.append(
            f"| {row.get('key')} | {row.get('trade_count')} | {row.get('win_rate')} | "
            f"{row.get('average_realized_points')} | {row.get('average_pnl_proxy')} | "
            f"{row.get('sample_adjusted_expectancy_proxy')} | {row.get('scorecard_label')} | "
            f"{row.get('sample_size_warning') or ''} |"
        )
    lines.append("")
    return "\n".join(lines)


def render_review_markdown(scorecards: Mapping[str, Any]) -> str:
    review = scorecards.get("review_candidates") or {}
    lines = ["# Trade Review Candidates", ""]
    for key in (
        "worst_realized_pnl_proxy_trades",
        "best_realized_pnl_proxy_trades",
        "long_hold_losers",
        "high_mae_trades",
        "missing_data_important_trades",
    ):
        lines.extend([f"## {key}", ""])
        for row in review.get(key) or []:
            lines.append(
                f"- {row.get('trade_outcome_id')} {row.get('lane_id')} {row.get('instrument')} "
                f"pnl={row.get('realized_pnl_proxy')} points={row.get('realized_points')}"
            )
        lines.append("")
    return "\n".join(lines)


def render_data_quality_markdown(scorecards: Mapping[str, Any]) -> str:
    quality = scorecards.get("data_quality") or {}
    lines = ["# Trade Scorecard Data Quality", "", "## Limiting Gaps", ""]
    for gap in quality.get("limiting_gaps") or []:
        lines.append(f"- {gap}")
    lines.extend(["", "## Top Flags", ""])
    for flag, count in (quality.get("top_flags") or {}).items():
        lines.append(f"- {flag}: {count}")
    lines.append("")
    return "\n".join(lines)


def _aggregate_group(key: str, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pnl = _numeric_values(rows, "realized_pnl_proxy")
    points = _numeric_values(rows, "realized_points")
    hold = _numeric_values(rows, "hold_seconds")
    mfe = _numeric_values(rows, "mfe_points")
    mae = _numeric_values(rows, "mae_points")
    best = max(rows, key=lambda row: _number(row.get("realized_pnl_proxy")) if _number(row.get("realized_pnl_proxy")) is not None else float("-inf"))
    worst = min(rows, key=lambda row: _number(row.get("realized_pnl_proxy")) if _number(row.get("realized_pnl_proxy")) is not None else float("inf"))
    average_pnl = _average(pnl)
    adjusted = _sample_adjusted(average_pnl, len(rows))
    flags = _counts(flag for row in rows for flag in row.get("data_quality_flags", ()))
    return {
        "key": key,
        "trade_count": len(rows),
        "win_rate": _win_rate(rows),
        "average_realized_points": _average(points),
        "median_realized_points": _median(points),
        "average_pnl_proxy": average_pnl,
        "sample_adjusted_expectancy_proxy": adjusted,
        "average_hold_seconds": _average(hold),
        "median_hold_seconds": _median(hold),
        "average_mfe": _average(mfe),
        "average_mae": _average(mae),
        "mfe_available_count": len(mfe),
        "mae_available_count": len(mae),
        "mfe_mae_availability": _rate(min(len(mfe), len(mae)), len(rows)),
        "data_quality_flags": flags,
        "sample_size_warning": _sample_warning(len(rows)),
        "scorecard_label": _scorecard_label(trade_count=len(rows), average_pnl=average_pnl, win_rate=_win_rate(rows), quality_flags=flags),
        "best_trade": _trade_ref(best),
        "worst_trade": _trade_ref(worst),
    }


def _scorecard_label(*, trade_count: int, average_pnl: float | None, win_rate: float | None, quality_flags: Mapping[str, int]) -> str:
    if trade_count < MINIMUM_SAMPLE_THRESHOLD:
        return "TOO_THIN"
    if quality_flags.get("missing_realized_r_proxy") and (quality_flags.get("missing_mfe") or quality_flags.get("missing_mae")):
        if average_pnl is not None and average_pnl > 0 and trade_count >= PROMISING_SAMPLE_THRESHOLD:
            return "PROMISING_RESEARCH_DATA_LIMITED"
        return "DATA_QUALITY_LIMITED"
    if average_pnl is not None and average_pnl > 0 and (win_rate or 0.0) >= 0.5:
        return "PROMISING_RESEARCH"
    if average_pnl is not None and average_pnl < 0:
        return "WEAK_REVIEW"
    return "WATCHLIST"


def _strategy_recommendations(groups: Sequence[Mapping[str, Any]]) -> list[str]:
    promising = [row for row in groups if str(row.get("scorecard_label", "")).startswith("PROMISING")]
    thin = [row for row in groups if row.get("scorecard_label") == "TOO_THIN"]
    recommendations = []
    if promising:
        recommendations.append("Promising strategy/lane groups merit further research review, not production changes.")
    if thin:
        recommendations.append("Thin strategy/lane groups need more outcomes before ranking.")
    return recommendations or ["No strategy/lane group has enough clean evidence for a strong research label."]


def _exit_recommendations(groups: Sequence[Mapping[str, Any]]) -> list[str]:
    if any((row.get("mfe_mae_availability") or 0.0) < 0.5 for row in groups):
        return ["MFE/MAE coverage is too sparse for strong early/late exit conclusions."]
    return ["Exit-policy groups can be reviewed with MFE/MAE support, still diagnostic only."]


def _generic_edge_recommendations(groups: Sequence[Mapping[str, Any]], *, label: str) -> list[str]:
    thin = [row for row in groups if row.get("sample_size_warning")]
    if thin:
        return [f"Some {label} groups are thin; use rankings as research triage only."]
    return [f"{label.title()} rankings are diagnostic research outputs only."]


def _sample_adjusted(value: float | None, count: int) -> float | None:
    if value is None:
        return None
    return round(value * min(count / PROMISING_SAMPLE_THRESHOLD, 1.0), 6)


def _sample_warning(count: int) -> str | None:
    if count < MINIMUM_SAMPLE_THRESHOLD:
        return "TOO_THIN"
    if count < PROMISING_SAMPLE_THRESHOLD:
        return "LOW_SAMPLE"
    return None


def _trade_ref(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "trade_outcome_id": row.get("trade_outcome_id"),
        "strategy_id": row.get("strategy_id"),
        "lane_id": row.get("lane_id"),
        "instrument": row.get("instrument"),
        "contract": row.get("contract"),
        "side": row.get("side"),
        "entry_time": row.get("entry_time"),
        "exit_time": row.get("exit_time"),
        "realized_points": row.get("realized_points"),
        "realized_pnl_proxy": row.get("realized_pnl_proxy"),
        "hold_seconds": row.get("hold_seconds"),
    }


def _numeric_values(rows: Sequence[Mapping[str, Any]], key: str) -> list[float]:
    return [value for value in (_number(row.get(key)) for row in rows) if value is not None]


def _average(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _median(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return round(float(median(values)), 6)


def _win_rate(rows: Sequence[Mapping[str, Any]]) -> float | None:
    pnl = _numeric_values(rows, "realized_pnl_proxy")
    return _rate(sum(1 for value in pnl if value > 0), len(pnl))


def _rate(part: int, whole: int) -> float | None:
    if whole <= 0:
        return None
    return round(part / whole, 6)


def _counts(values: Any) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values:
        key = str(value or "UNKNOWN")
        result[key] = result.get(key, 0) + 1
    return dict(sorted(result.items(), key=lambda item: (-item[1], item[0])))


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


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
