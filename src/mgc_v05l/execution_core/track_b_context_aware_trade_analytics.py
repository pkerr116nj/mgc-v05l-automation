"""Context-aware diagnostic analytics for canonical trade outcomes."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.execution_core.track_b_trade_outcome_enrichment import (
    DEFAULT_OUTPUT_DIR as DEFAULT_ENRICHMENT_DIR,
    ENRICHMENT_JSONL,
    SUMMARY_JSON as ENRICHMENT_SUMMARY_JSON,
)
from mgc_v05l.execution_core.track_b_trade_outcome_layer import (
    DEFAULT_OUTPUT_DIR as DEFAULT_OUTCOME_LAYER_DIR,
    OUTCOMES_JSONL,
)


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_OUTCOMES_PATH = DEFAULT_OUTCOME_LAYER_DIR / OUTCOMES_JSONL
DEFAULT_ENRICHMENTS_PATH = DEFAULT_ENRICHMENT_DIR / ENRICHMENT_JSONL
DEFAULT_ENRICHMENT_SUMMARY_PATH = DEFAULT_ENRICHMENT_DIR / ENRICHMENT_SUMMARY_JSON
DEFAULT_T3_SCORECARD_PATH = DEFAULT_OUTPUT_ROOT / "trade_outcome_scorecards" / "latest_trade_outcome_scorecards.json"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "context_aware_trade_analytics"

ANALYTICS_JSON = "latest_context_aware_trade_analytics.json"
ANALYTICS_MD = "latest_context_aware_trade_analytics.md"
STRATEGY_VIX_MD = "strategy_by_vix_regime_scorecard.md"
SESSION_VIX_MD = "session_by_vix_regime_scorecard.md"
INSTRUMENT_VIX_MD = "instrument_by_vix_regime_scorecard.md"
EXIT_VIX_MD = "exit_policy_by_vix_regime_scorecard.md"
VIX_PERCENTILE_MD = "vix_percentile_bucket_scorecard.md"
RECOMMENDATIONS_MD = "context_aware_trade_recommendations.md"

SCHEMA_VERSION = "track_b_context_aware_trade_analytics_v1"
MINIMUM_SAMPLE_THRESHOLD = 5
PROMISING_SAMPLE_THRESHOLD = 10


@dataclass(frozen=True)
class ContextAwareTradeAnalyticsResult:
    analytics: dict[str, Any]
    json_path: Path
    markdown_path: Path
    strategy_path: Path
    session_path: Path
    instrument_path: Path
    exit_policy_path: Path
    vix_percentile_path: Path
    recommendations_path: Path


def run_context_aware_trade_analytics(
    *,
    outcomes_path: Path = DEFAULT_OUTCOMES_PATH,
    enrichments_path: Path = DEFAULT_ENRICHMENTS_PATH,
    enrichment_summary_path: Path = DEFAULT_ENRICHMENT_SUMMARY_PATH,
    t3_scorecard_path: Path = DEFAULT_T3_SCORECARD_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    now: datetime | str | None = None,
    max_snapshot_bytes: int | None = None,
) -> ContextAwareTradeAnalyticsResult:
    generated_at = _coerce_now(now)
    outcomes = _read_jsonl(outcomes_path)
    enrichments = _read_jsonl(enrichments_path)
    enrichment_summary = _read_json_mapping(enrichment_summary_path)
    t3_scorecard = _read_json_mapping(t3_scorecard_path)
    analytics = build_context_aware_trade_analytics(
        outcomes,
        enrichments=enrichments,
        enrichment_summary=enrichment_summary,
        t3_scorecard=t3_scorecard,
        generated_at=generated_at,
        source_paths={
            "canonical_trade_outcomes": outcomes_path,
            "canonical_trade_outcome_enrichment": enrichments_path,
            "trade_outcome_enrichment_summary": enrichment_summary_path,
            "trade_outcome_scorecards": t3_scorecard_path,
        },
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_config = BoundedSnapshotConfig(max_bytes=max_snapshot_bytes) if max_snapshot_bytes else BoundedSnapshotConfig()
    json_path = output_dir / ANALYTICS_JSON
    write_bounded_snapshot_json(json_path, analytics, config=snapshot_config)
    markdown_path = output_dir / ANALYTICS_MD
    markdown_path.write_text(render_overview_markdown(analytics), encoding="utf-8")
    strategy_path = output_dir / STRATEGY_VIX_MD
    strategy_path.write_text(render_group_markdown("Strategy x VIX Regime", analytics["scorecards"]["strategy_by_vix_regime"]), encoding="utf-8")
    session_path = output_dir / SESSION_VIX_MD
    session_path.write_text(render_group_markdown("Session x VIX Regime", analytics["scorecards"]["session_by_vix_regime"]), encoding="utf-8")
    instrument_path = output_dir / INSTRUMENT_VIX_MD
    instrument_path.write_text(render_group_markdown("Instrument x VIX Regime", analytics["scorecards"]["instrument_by_vix_regime"]), encoding="utf-8")
    exit_policy_path = output_dir / EXIT_VIX_MD
    exit_policy_path.write_text(render_group_markdown("Exit Policy x VIX Regime", analytics["scorecards"]["exit_policy_by_vix_regime"]), encoding="utf-8")
    vix_percentile_path = output_dir / VIX_PERCENTILE_MD
    vix_percentile_path.write_text(render_group_markdown("VIX Percentile Buckets", analytics["scorecards"]["vix_percentile_buckets"]), encoding="utf-8")
    recommendations_path = output_dir / RECOMMENDATIONS_MD
    recommendations_path.write_text(render_recommendations_markdown(analytics), encoding="utf-8")
    return ContextAwareTradeAnalyticsResult(
        analytics=analytics,
        json_path=json_path,
        markdown_path=markdown_path,
        strategy_path=strategy_path,
        session_path=session_path,
        instrument_path=instrument_path,
        exit_policy_path=exit_policy_path,
        vix_percentile_path=vix_percentile_path,
        recommendations_path=recommendations_path,
    )


def build_context_aware_trade_analytics(
    outcomes: Sequence[Mapping[str, Any]],
    *,
    enrichments: Sequence[Mapping[str, Any]],
    enrichment_summary: Mapping[str, Any] | None = None,
    t3_scorecard: Mapping[str, Any] | None = None,
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> dict[str, Any]:
    rows = _merge_outcomes_with_enrichment(outcomes, enrichments)
    vix_rows = [row for row in rows if row.get("vix_level") is not None]
    regime_groups = aggregate_context_groups(rows, key_fields=("vix_regime",))
    strategy_vix = aggregate_context_groups(rows, key_fields=("strategy_id", "lane_id", "vix_regime"))
    session_vix = aggregate_context_groups(rows, key_fields=("session_at_entry", "vix_regime"))
    instrument_vix = aggregate_context_groups(rows, key_fields=("instrument", "contract", "vix_regime"))
    exit_vix = aggregate_context_groups(rows, key_fields=("exit_policy", "vix_regime"))
    percentile_buckets = aggregate_context_groups(_with_vix_buckets(rows), key_fields=("vix_percentile_bucket",))
    level_buckets = aggregate_context_groups(_with_vix_buckets(rows), key_fields=("vix_level_bucket",))
    data_quality = _data_quality(rows, enrichment_summary or {}, t3_scorecard or {})
    recommendations = build_context_recommendations(
        regime_groups=regime_groups,
        strategy_vix=strategy_vix,
        session_vix=session_vix,
        instrument_vix=instrument_vix,
        exit_vix=exit_vix,
        data_quality=data_quality,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "analytics_only": True,
        "diagnostic_only": True,
        "production_effect": False,
        "source_paths": {key: str(value) for key, value in (source_paths or {}).items()},
        "overall": {
            "outcome_count": len(outcomes),
            "enrichment_count": len(enrichments),
            "joined_outcome_count": len(rows),
            "vix_context_count": len(vix_rows),
            "vix_coverage": _rate(len(vix_rows), len(rows)),
            "gre_context_available_count": sum(1 for row in rows if row.get("gre_label") is not None),
            "average_pnl_proxy": _average(_numeric_values(rows, "realized_pnl_proxy")),
            "win_rate": _win_rate(rows),
        },
        "scorecards": {
            "vix_regime": {"grouping": "vix_regime", "groups": regime_groups},
            "strategy_by_vix_regime": {"grouping": "strategy_id,lane_id,vix_regime", "groups": strategy_vix},
            "session_by_vix_regime": {"grouping": "session_at_entry,vix_regime", "groups": session_vix},
            "instrument_by_vix_regime": {"grouping": "instrument,contract,vix_regime", "groups": instrument_vix},
            "exit_policy_by_vix_regime": {"grouping": "exit_policy,vix_regime", "groups": exit_vix},
            "vix_percentile_buckets": {"grouping": "vix_percentile_bucket", "groups": percentile_buckets},
            "vix_level_buckets": {"grouping": "vix_level_bucket", "groups": level_buckets},
        },
        "data_quality": data_quality,
        "recommendations": recommendations,
        "safety_contract": {
            "broker_actions": False,
            "runtime_restart": False,
            "managed_exit_restart": False,
            "strategy_changes": False,
            "trading_gates": False,
            "production_behavior_changes": False,
            "db_mutation": False,
            "databento_download": False,
        },
    }


def aggregate_context_groups(rows: Sequence[Mapping[str, Any]], *, key_fields: Sequence[str]) -> list[dict[str, Any]]:
    groups: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        key = " | ".join(str(row.get(field) or "UNKNOWN") for field in key_fields)
        groups.setdefault(key, []).append(row)
    aggregated = [_aggregate_group(key, values) for key, values in groups.items()]
    return sorted(
        aggregated,
        key=lambda row: (row.get("sample_adjusted_pnl_proxy") or 0.0, row.get("average_pnl_proxy") or 0.0),
        reverse=True,
    )


def build_context_recommendations(
    *,
    regime_groups: Sequence[Mapping[str, Any]],
    strategy_vix: Sequence[Mapping[str, Any]],
    session_vix: Sequence[Mapping[str, Any]],
    instrument_vix: Sequence[Mapping[str, Any]],
    exit_vix: Sequence[Mapping[str, Any]],
    data_quality: Mapping[str, Any],
) -> list[str]:
    recommendations: list[str] = []
    strong = [row for row in strategy_vix if row.get("context_label") == "PROMISING_CONTEXT_RESEARCH"]
    weak_regimes = [row for row in regime_groups if (row.get("average_pnl_proxy") or 0.0) < 0 and not row.get("sample_size_warning")]
    if strong:
        recommendations.append("Prioritize manual review of the strongest strategy/lane x VIX regime groups; no production changes are recommended.")
    if weak_regimes:
        recommendations.append("Review VIX regimes with negative average P&L proxy as possible deteriorating context conditions.")
    if any(row.get("sample_size_warning") for row in strategy_vix + session_vix + instrument_vix + exit_vix):
        recommendations.append("Many context cross-tabs are sample-limited; treat rankings as research triage only.")
    if data_quality.get("gre_context_available_count") == 0:
        recommendations.append("GRE context remains unavailable in trade enrichment, so context analytics are VIX-only for now.")
    recommendations.append("Use findings for further research, manual review, or shadow analytics only; do not change production strategy behavior.")
    return recommendations


def render_overview_markdown(analytics: Mapping[str, Any]) -> str:
    overall = analytics.get("overall") or {}
    return "\n".join(
        [
            "# Context-Aware Trade Analytics",
            "",
            f"- Generated at: {analytics.get('generated_at')}",
            f"- Joined outcomes: {overall.get('joined_outcome_count')}",
            f"- VIX coverage: {overall.get('vix_coverage')} ({overall.get('vix_context_count')})",
            f"- Average P&L proxy: {overall.get('average_pnl_proxy')}",
            f"- Win rate: {overall.get('win_rate')}",
            f"- GRE context available: {overall.get('gre_context_available_count')}",
            "",
            "This report is diagnostic/research only and has no trading authority.",
            "",
        ]
    )


def render_group_markdown(title: str, scorecard: Mapping[str, Any]) -> str:
    lines = [
        f"# {title}",
        "",
        "| Key | Trades | Win rate | Avg points | Median points | Avg P&L | Avg hold | Label | Warning |",
        "|---|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in (scorecard.get("groups") or [])[:80]:
        lines.append(
            f"| {row.get('key')} | {row.get('trade_count')} | {row.get('win_rate')} | "
            f"{row.get('average_realized_points')} | {row.get('median_realized_points')} | "
            f"{row.get('average_pnl_proxy')} | {row.get('average_hold_seconds')} | "
            f"{row.get('context_label')} | {row.get('sample_size_warning') or ''} |"
        )
    lines.append("")
    return "\n".join(lines)


def render_recommendations_markdown(analytics: Mapping[str, Any]) -> str:
    lines = ["# Context-Aware Trade Recommendations", ""]
    for item in analytics.get("recommendations") or []:
        lines.append(f"- {item}")
    lines.extend(["", "## Data Quality", ""])
    quality = analytics.get("data_quality") or {}
    for item in quality.get("limitations") or []:
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def _merge_outcomes_with_enrichment(
    outcomes: Sequence[Mapping[str, Any]],
    enrichments: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    enrichment_by_id = {str(row.get("trade_outcome_id")): row for row in enrichments if row.get("trade_outcome_id")}
    merged: list[dict[str, Any]] = []
    for outcome in outcomes:
        trade_id = str(outcome.get("trade_outcome_id") or "")
        if not trade_id:
            continue
        row = dict(outcome)
        enrichment = enrichment_by_id.get(trade_id)
        if enrichment:
            for key in (
                "vix_level",
                "vix_regime",
                "vix_daily_change",
                "vix_percentile",
                "vix_ma20",
                "vix_ma50",
                "market_context_source",
                "market_context_provider",
                "market_context_timestamp",
                "market_context_staleness",
                "market_context_join_success",
                "gre_label",
                "gre_confidence",
                "vwap_relation",
                "avwap_relation",
            ):
                row[key] = enrichment.get(key)
            row["enrichment_data_quality_flags"] = list(enrichment.get("data_quality_flags") or [])
        else:
            row["market_context_join_success"] = False
            row["enrichment_data_quality_flags"] = ["missing_trade_outcome_enrichment"]
        merged.append(row)
    return merged


def _with_vix_buckets(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    expanded: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        payload["vix_percentile_bucket"] = _vix_percentile_bucket(_number(row.get("vix_percentile")))
        payload["vix_level_bucket"] = _vix_level_bucket(_number(row.get("vix_level")))
        expanded.append(payload)
    return expanded


def _aggregate_group(key: str, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pnl = _numeric_values(rows, "realized_pnl_proxy")
    points = _numeric_values(rows, "realized_points")
    hold = _numeric_values(rows, "hold_seconds")
    best = max(rows, key=lambda row: _number(row.get("realized_pnl_proxy")) if _number(row.get("realized_pnl_proxy")) is not None else float("-inf"))
    worst = min(rows, key=lambda row: _number(row.get("realized_pnl_proxy")) if _number(row.get("realized_pnl_proxy")) is not None else float("inf"))
    average_pnl = _average(pnl)
    flags = _counts(flag for row in rows for flag in row.get("data_quality_flags", ()))
    enrichment_flags = _counts(flag for row in rows for flag in row.get("enrichment_data_quality_flags", ()))
    return {
        "key": key,
        "trade_count": len(rows),
        "win_rate": _win_rate(rows),
        "average_realized_points": _average(points),
        "median_realized_points": _median(points),
        "average_pnl_proxy": average_pnl,
        "sample_adjusted_pnl_proxy": _sample_adjusted(average_pnl, len(rows)),
        "average_hold_seconds": _average(hold),
        "median_hold_seconds": _median(hold),
        "best_trade": _trade_ref(best),
        "worst_trade": _trade_ref(worst),
        "data_quality_flags": flags,
        "enrichment_data_quality_flags": enrichment_flags,
        "sample_size_warning": _sample_warning(len(rows)),
        "context_label": _context_label(len(rows), average_pnl, _win_rate(rows)),
    }


def _data_quality(
    rows: Sequence[Mapping[str, Any]],
    enrichment_summary: Mapping[str, Any],
    t3_scorecard: Mapping[str, Any],
) -> dict[str, Any]:
    missing_vix = sum(1 for row in rows if row.get("vix_level") is None)
    missing_regime = sum(1 for row in rows if row.get("vix_regime") is None)
    gre_count = sum(1 for row in rows if row.get("gre_label") is not None)
    limitations: list[str] = []
    if missing_vix:
        limitations.append("Some trades lack VIX context; missing values are not fabricated.")
    if missing_regime:
        limitations.append("Some trades lack VIX regime labels; regime cross-tabs are incomplete.")
    if gre_count == 0:
        limitations.append("GRE context remains unavailable in enrichment, so no regime-at-entry conclusions are drawn.")
    t3_quality = t3_scorecard.get("data_quality") or {}
    for gap in t3_quality.get("limiting_gaps") or []:
        limitations.append(str(gap))
    return {
        "missing_vix_context_count": missing_vix,
        "missing_vix_regime_count": missing_regime,
        "gre_context_available_count": gre_count,
        "enrichment_summary_market_context": (enrichment_summary.get("market_context") or {}),
        "limitations": limitations,
    }


def _context_label(count: int, average_pnl: float | None, win_rate: float | None) -> str:
    if count < MINIMUM_SAMPLE_THRESHOLD:
        return "TOO_THIN"
    if count < PROMISING_SAMPLE_THRESHOLD:
        return "LOW_SAMPLE_RESEARCH"
    if average_pnl is not None and average_pnl > 0 and (win_rate or 0.0) >= 0.5:
        return "PROMISING_CONTEXT_RESEARCH"
    if average_pnl is not None and average_pnl < 0:
        return "WEAK_CONTEXT_REVIEW"
    return "WATCHLIST_CONTEXT"


def _vix_percentile_bucket(value: float | None) -> str:
    if value is None:
        return "UNKNOWN"
    pct = value * 100 if value <= 1.0 else value
    if pct < 20:
        return "0-20"
    if pct < 40:
        return "20-40"
    if pct < 60:
        return "40-60"
    if pct < 80:
        return "60-80"
    return "80-100"


def _vix_level_bucket(value: float | None) -> str:
    if value is None:
        return "UNKNOWN"
    if value < 15:
        return "under_15"
    if value < 20:
        return "15-20"
    if value < 25:
        return "20-25"
    if value < 30:
        return "25-30"
    return "over_30"


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
        "session_at_entry": row.get("session_at_entry"),
        "exit_policy": row.get("exit_policy"),
        "vix_regime": row.get("vix_regime"),
        "vix_level": row.get("vix_level"),
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
