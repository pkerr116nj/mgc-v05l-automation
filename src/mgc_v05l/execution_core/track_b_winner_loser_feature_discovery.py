"""RA9 winner vs. loser feature discovery.

This module compares observable entry/context features between winning and
losing completed trades for each research strategy candidate. It emits
diagnostic hypotheses only. It has no broker, runtime, strategy, Managed Exit,
or trading-gate authority.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable, Mapping, Sequence

from mgc_v05l.execution_core.track_b_core_expectancy_analytics import (
    DEFAULT_ENRICHMENTS_PATH,
    DEFAULT_OUTCOMES_PATH,
    merge_outcomes_with_enrichment,
    sample_class_for_count,
)
from mgc_v05l.execution_core.track_b_trade_decision_attribution import (
    ATTRIBUTION_JSONL,
    DEFAULT_OUTPUT_DIR as DEFAULT_RA3_OUTPUT_DIR,
)
from mgc_v05l.execution_core.track_b_canonical_trade_path_layer import (
    CANONICAL_TRADE_PATHS_JSONL,
    DEFAULT_OUTPUT_DIR as DEFAULT_RA7_OUTPUT_DIR,
)


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_ATTRIBUTIONS_PATH = DEFAULT_RA3_OUTPUT_DIR / ATTRIBUTION_JSONL
DEFAULT_TRADE_PATHS_PATH = DEFAULT_RA7_OUTPUT_DIR / CANONICAL_TRADE_PATHS_JSONL
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research_analytics" / "winner_loser_feature_discovery"

SUMMARY_JSON = "ra9_winner_loser_feature_discovery.json"
SUMMARY_MD = "ra9_winner_loser_feature_discovery.md"
HYPOTHESES_JSONL = "ra9_feature_hypotheses.jsonl"
LANE_SCORECARD_MD = "ra9_lane_feature_scorecard.md"
FEATURE_RANKINGS_MD = "ra9_feature_separation_rankings.md"
DATA_QUALITY_MD = "ra9_feature_discovery_data_quality.md"
CONTRACT_MD = "ra9_feature_discovery_contract.md"

SCHEMA_VERSION = "ra9_winner_loser_feature_discovery_v1"
HYPOTHESIS_SCHEMA_VERSION = "ra9_feature_hypothesis_v1"

MIN_TOTAL_SAMPLE = 4
MIN_SIDE_SAMPLE = 2

CATEGORICAL_FEATURES = (
    "instrument",
    "contract",
    "side",
    "session_at_entry",
    "entry_hour_utc_bucket",
    "session_open_proximity_bucket",
    "exit_policy",
    "vix_regime",
    "vix_percentile_bucket",
    "gre_label_valid",
    "vwap_relation",
    "avwap_relation",
    "path_coverage_status",
    "counterfactual_timebox_ready",
)

NUMERIC_FEATURES = (
    "hold_seconds",
    "vix_level",
    "vix_percentile",
    "gre_confidence_valid",
    "distance_from_vwap_points",
    "mfe_points",
    "mae_points",
    "path_mfe",
    "path_mae",
)


@dataclass(frozen=True)
class WinnerLoserFeatureDiscoveryResult:
    summary: dict[str, Any]
    hypotheses: list[dict[str, Any]]
    summary_json_path: Path
    summary_markdown_path: Path
    hypotheses_path: Path
    lane_scorecard_path: Path
    feature_rankings_path: Path
    data_quality_path: Path
    contract_path: Path


def run_winner_loser_feature_discovery(
    *,
    outcomes_path: Path = DEFAULT_OUTCOMES_PATH,
    enrichments_path: Path = DEFAULT_ENRICHMENTS_PATH,
    attributions_path: Path = DEFAULT_ATTRIBUTIONS_PATH,
    trade_paths_path: Path = DEFAULT_TRADE_PATHS_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    now: datetime | str | None = None,
) -> WinnerLoserFeatureDiscoveryResult:
    generated_at = _coerce_now(now)
    outcomes = _read_jsonl(outcomes_path)
    enrichments = _read_jsonl(enrichments_path)
    attributions = _read_jsonl(attributions_path)
    trade_paths = _read_jsonl(trade_paths_path)
    rows = build_feature_rows(outcomes, enrichments=enrichments, attributions=attributions, trade_paths=trade_paths)
    summary, hypotheses = build_winner_loser_feature_discovery(
        rows,
        generated_at=generated_at,
        source_paths={
            "canonical_trade_outcomes": outcomes_path,
            "trade_outcome_enrichment": enrichments_path,
            "trade_decision_attribution": attributions_path,
            "canonical_trade_paths": trade_paths_path,
        },
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_json_path = output_dir / SUMMARY_JSON
    summary_markdown_path = output_dir / SUMMARY_MD
    hypotheses_path = output_dir / HYPOTHESES_JSONL
    lane_scorecard_path = output_dir / LANE_SCORECARD_MD
    feature_rankings_path = output_dir / FEATURE_RANKINGS_MD
    data_quality_path = output_dir / DATA_QUALITY_MD
    contract_path = output_dir / CONTRACT_MD
    _write_json(summary_json_path, summary)
    _write_jsonl(hypotheses_path, hypotheses)
    summary_markdown_path.write_text(render_summary_markdown(summary), encoding="utf-8")
    lane_scorecard_path.write_text(render_lane_scorecard_markdown(summary), encoding="utf-8")
    feature_rankings_path.write_text(render_feature_rankings_markdown(summary), encoding="utf-8")
    data_quality_path.write_text(render_data_quality_markdown(summary), encoding="utf-8")
    contract_path.write_text(render_contract_markdown(), encoding="utf-8")
    return WinnerLoserFeatureDiscoveryResult(
        summary=summary,
        hypotheses=hypotheses,
        summary_json_path=summary_json_path,
        summary_markdown_path=summary_markdown_path,
        hypotheses_path=hypotheses_path,
        lane_scorecard_path=lane_scorecard_path,
        feature_rankings_path=feature_rankings_path,
        data_quality_path=data_quality_path,
        contract_path=contract_path,
    )


def build_feature_rows(
    outcomes: Sequence[Mapping[str, Any]],
    *,
    enrichments: Sequence[Mapping[str, Any]] = (),
    attributions: Sequence[Mapping[str, Any]] = (),
    trade_paths: Sequence[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    rows = merge_outcomes_with_enrichment(outcomes, enrichments)
    enrichment_by_id = {str(row.get("trade_outcome_id")): row for row in enrichments if row.get("trade_outcome_id")}
    attribution_by_id = {str(row.get("trade_outcome_id")): row for row in attributions if row.get("trade_outcome_id")}
    path_by_id = {str(row.get("trade_outcome_id")): row for row in trade_paths if row.get("trade_outcome_id")}
    feature_rows: list[dict[str, Any]] = []
    for row in rows:
        trade_outcome_id = str(row.get("trade_outcome_id") or "")
        enrichment = enrichment_by_id.get(trade_outcome_id, {})
        attribution = attribution_by_id.get(trade_outcome_id, {})
        path = path_by_id.get(trade_outcome_id, {})
        entry_time = _parse_ts(row.get("entry_time"))
        payload = dict(row)
        payload["winner"] = (_number(row.get("realized_pnl_proxy")) or 0.0) > 0
        payload["entry_hour_utc_bucket"] = _entry_hour_bucket(entry_time)
        payload["session_open_proximity_bucket"] = _session_open_proximity_bucket(row.get("session_at_entry"), entry_time)
        payload["vix_percentile_bucket"] = _vix_percentile_bucket(_number(row.get("vix_percentile")))
        payload["gre_label_valid"] = row.get("gre_label") if _valid_context(row.get("gre_validity_classification")) and row.get("gre_label") else "UNAVAILABLE"
        payload["gre_confidence_valid"] = row.get("gre_confidence") if _valid_context(row.get("gre_validity_classification")) else None
        payload["vwap_relation"] = row.get("vwap_relation") or "UNAVAILABLE"
        payload["avwap_relation"] = row.get("avwap_relation") or "UNAVAILABLE"
        payload["distance_from_vwap_points"] = enrichment.get("distance_from_vwap_points")
        payload["path_coverage_status"] = path.get("path_coverage_status") or "UNAVAILABLE"
        payload["path_mfe"] = path.get("mfe")
        payload["path_mae"] = path.get("mae")
        payload["counterfactual_timebox_ready"] = str(path.get("counterfactual_ready", {}).get("timebox") is True)
        payload["entry_signal"] = attribution.get("entry", {}).get("entry_signal") if attribution else None
        feature_rows.append(payload)
    return feature_rows


def build_winner_loser_feature_discovery(
    rows: Sequence[Mapping[str, Any]],
    *,
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    by_lane: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for row in rows:
        key = (str(row.get("strategy_id") or "UNKNOWN"), str(row.get("lane_id") or "UNKNOWN"))
        by_lane.setdefault(key, []).append(row)
    lane_results: list[dict[str, Any]] = []
    hypotheses: list[dict[str, Any]] = []
    for (strategy_id, lane_id), lane_rows in sorted(by_lane.items(), key=lambda item: (-len(item[1]), item[0])):
        lane_result, lane_hypotheses = analyze_lane_features(
            strategy_id=strategy_id,
            lane_id=lane_id,
            rows=lane_rows,
            generated_at=generated_at,
        )
        lane_results.append(lane_result)
        hypotheses.extend(lane_hypotheses)
    hypotheses.sort(key=lambda row: (-float(row.get("separation_score") or 0.0), -int(row.get("sample_size") or 0), str(row.get("hypothesis_id"))))
    summary = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
        "source_paths": {key: str(value) for key, value in (source_paths or {}).items()},
        "overall": {
            "trade_count": len(rows),
            "lane_count": len(lane_results),
            "hypothesis_count": len(hypotheses),
            "lanes_with_hypotheses": sum(1 for lane in lane_results if lane.get("top_hypotheses")),
            "winner_count": sum(1 for row in rows if row.get("winner") is True),
            "loser_count": sum(1 for row in rows if row.get("winner") is False),
        },
        "lanes": lane_results,
        "top_hypotheses": hypotheses[:50],
        "data_quality": _data_quality(rows),
        "guardrails": {
            "diagnostic_only": True,
            "production_recommendation": False,
            "trading_gate": False,
            "strategy_change_recommendation": False,
            "runtime_integration": False,
            "broker_actions": False,
        },
    }
    return summary, hypotheses


def analyze_lane_features(
    *,
    strategy_id: str,
    lane_id: str,
    rows: Sequence[Mapping[str, Any]],
    generated_at: datetime,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    winners = [row for row in rows if row.get("winner") is True]
    losers = [row for row in rows if row.get("winner") is False]
    categorical = [_categorical_separation(feature, winners, losers) for feature in CATEGORICAL_FEATURES]
    numeric = [_numeric_separation(feature, winners, losers) for feature in NUMERIC_FEATURES]
    feature_rankings = sorted(
        [row for row in categorical + numeric if row],
        key=lambda item: (-float(item.get("separation_score") or 0.0), str(item.get("feature"))),
    )
    sample_class = sample_class_for_count(len(rows))
    hypotheses = [
        _hypothesis_from_feature(
            strategy_id=strategy_id,
            lane_id=lane_id,
            feature=row,
            rows=rows,
            winners=winners,
            losers=losers,
            sample_class=sample_class,
            generated_at=generated_at,
        )
        for row in feature_rankings[:5]
        if row.get("separation_score", 0) >= 0.15
    ]
    hypotheses = [row for row in hypotheses if row]
    lane_result = {
        "strategy_id": strategy_id,
        "lane_id": lane_id,
        "trade_count": len(rows),
        "winner_count": len(winners),
        "loser_count": len(losers),
        "win_rate": _rate(len(winners), len(rows)),
        "sample_class": sample_class,
        "feature_rankings": feature_rankings[:10],
        "top_hypotheses": [row["hypothesis_id"] for row in hypotheses],
        "data_quality_flags": _lane_data_quality_flags(rows, winners, losers),
    }
    return lane_result, hypotheses


def _categorical_separation(feature: str, winners: Sequence[Mapping[str, Any]], losers: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    if len(winners) < MIN_SIDE_SAMPLE or len(losers) < MIN_SIDE_SAMPLE:
        return None
    winner_counts = _counts(_feature_value(row.get(feature)) for row in winners)
    loser_counts = _counts(_feature_value(row.get(feature)) for row in losers)
    values = sorted(set(winner_counts) | set(loser_counts))
    if not values:
        return None
    best: dict[str, Any] | None = None
    for value in values:
        winner_rate = winner_counts.get(value, 0) / len(winners)
        loser_rate = loser_counts.get(value, 0) / len(losers)
        diff = winner_rate - loser_rate
        candidate = {
            "feature": feature,
            "feature_type": "categorical",
            "value": value,
            "winner_rate": round(winner_rate, 6),
            "loser_rate": round(loser_rate, 6),
            "direction": "WINNER_ASSOCIATED" if diff > 0 else "LOSER_ASSOCIATED" if diff < 0 else "NEUTRAL",
            "separation_score": round(abs(diff), 6),
            "winner_count": winner_counts.get(value, 0),
            "loser_count": loser_counts.get(value, 0),
        }
        if best is None or candidate["separation_score"] > best["separation_score"]:
            best = candidate
    return best


def _numeric_separation(feature: str, winners: Sequence[Mapping[str, Any]], losers: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    winner_values = [_number(row.get(feature)) for row in winners]
    loser_values = [_number(row.get(feature)) for row in losers]
    winner_values = [value for value in winner_values if value is not None]
    loser_values = [value for value in loser_values if value is not None]
    if len(winner_values) < MIN_SIDE_SAMPLE or len(loser_values) < MIN_SIDE_SAMPLE:
        return None
    winner_mean = mean(winner_values)
    loser_mean = mean(loser_values)
    pooled = _pooled_std(winner_values, loser_values)
    raw_diff = winner_mean - loser_mean
    score = abs(raw_diff) / pooled if pooled and pooled > 0 else 0.0
    return {
        "feature": feature,
        "feature_type": "numeric",
        "winner_mean": round(winner_mean, 6),
        "loser_mean": round(loser_mean, 6),
        "winner_median": round(float(median(winner_values)), 6),
        "loser_median": round(float(median(loser_values)), 6),
        "direction": "WINNER_HIGHER" if raw_diff > 0 else "WINNER_LOWER" if raw_diff < 0 else "NEUTRAL",
        "separation_score": round(score, 6),
        "winner_coverage": len(winner_values),
        "loser_coverage": len(loser_values),
    }


def _hypothesis_from_feature(
    *,
    strategy_id: str,
    lane_id: str,
    feature: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    winners: Sequence[Mapping[str, Any]],
    losers: Sequence[Mapping[str, Any]],
    sample_class: str,
    generated_at: datetime,
) -> dict[str, Any]:
    instrument = _common_value(row.get("instrument") for row in rows)
    side = _common_value(row.get("side") for row in rows)
    title = _hypothesis_title(instrument=instrument, side=side, feature=feature)
    text = _hypothesis_text(instrument=instrument, side=side, feature=feature)
    payload = {
        "schema_version": HYPOTHESIS_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "hypothesis_id": _stable_id("ra9_hypothesis", strategy_id, lane_id, feature.get("feature"), feature.get("value"), feature.get("direction")),
        "strategy_id": strategy_id,
        "lane_id": lane_id,
        "title": title,
        "hypothesis_text": text,
        "feature": dict(feature),
        "sample_size": len(rows),
        "winner_count": len(winners),
        "loser_count": len(losers),
        "sample_class": sample_class,
        "separation_score": feature.get("separation_score"),
        "confidence_class": _confidence_class(len(rows), feature),
        "recommendation_level": "RESEARCH_HYPOTHESIS",
        "data_quality_flags": _lane_data_quality_flags(rows, winners, losers),
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
    }
    payload["deterministic_fingerprint"] = _fingerprint({k: v for k, v in payload.items() if k not in {"generated_at", "deterministic_fingerprint"}})
    return payload


def _hypothesis_title(*, instrument: str, side: str, feature: Mapping[str, Any]) -> str:
    subject = _subject(instrument, side)
    direction = str(feature.get("direction") or "")
    if feature.get("feature_type") == "categorical":
        group = "Winning" if direction == "WINNER_ASSOCIATED" else "Losing"
        return f"{group} {subject} cluster around {feature.get('feature')}={feature.get('value')}"
    if direction == "WINNER_HIGHER":
        return f"Winning {subject} show higher {feature.get('feature')}"
    if direction == "WINNER_LOWER":
        return f"Winning {subject} show lower {feature.get('feature')}"
    return f"{subject} show a possible {feature.get('feature')} separation"


def _hypothesis_text(*, instrument: str, side: str, feature: Mapping[str, Any]) -> str:
    subject = _subject(instrument, side)
    direction = str(feature.get("direction") or "")
    feature_name = str(feature.get("feature"))
    if feature.get("feature_type") == "categorical":
        value = feature.get("value")
        if direction == "WINNER_ASSOCIATED":
            return f"Winning {subject} tend to occur when {feature_name} is `{value}` more often than losing trades."
        if direction == "LOSER_ASSOCIATED":
            return f"Losing {subject} tend to occur when {feature_name} is `{value}` more often than winning trades."
        return f"{subject} may show a weak split around {feature_name}=`{value}`."
    if direction == "WINNER_HIGHER":
        return f"Winning {subject} tend to have higher `{feature_name}` than losing trades."
    if direction == "WINNER_LOWER":
        return f"Winning {subject} tend to have lower `{feature_name}` than losing trades."
    return f"{subject} may show a weak numeric split in `{feature_name}`."


def render_summary_markdown(summary: Mapping[str, Any]) -> str:
    overall = summary.get("overall", {})
    lines = [
        "# RA9 Winner vs. Loser Feature Discovery",
        "",
        f"- Trades analyzed: `{overall.get('trade_count')}`",
        f"- Lanes analyzed: `{overall.get('lane_count')}`",
        f"- Hypotheses generated: `{overall.get('hypothesis_count')}`",
        f"- Winners: `{overall.get('winner_count')}`",
        f"- Losers: `{overall.get('loser_count')}`",
        "",
        "## Top Hypotheses",
        "",
    ]
    for row in summary.get("top_hypotheses", [])[:15]:
        lines.append(f"- `{row.get('lane_id')}`: {row.get('hypothesis_text')} Score `{row.get('feature', {}).get('separation_score')}`.")
    lines.extend(["", "Diagnostic hypotheses only. No strategy changes, production recommendations, or gates."])
    return "\n".join(lines) + "\n"


def render_lane_scorecard_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# RA9 Lane Feature Scorecard",
        "",
        "| Lane | Trades | Winners | Losers | Win rate | Sample | Top feature | Score |",
        "|---|---:|---:|---:|---:|---|---|---:|",
    ]
    for row in summary.get("lanes", []):
        top = (row.get("feature_rankings") or [{}])[0]
        lines.append(
            f"| `{row.get('lane_id')}` | {row.get('trade_count')} | {row.get('winner_count')} | {row.get('loser_count')} | "
            f"{row.get('win_rate')} | `{row.get('sample_class')}` | `{top.get('feature')}` | {top.get('separation_score')} |"
        )
    return "\n".join(lines) + "\n"


def render_feature_rankings_markdown(summary: Mapping[str, Any]) -> str:
    lines = ["# RA9 Feature Separation Rankings", ""]
    for lane in summary.get("lanes", [])[:100]:
        lines.extend([f"## {lane.get('lane_id')}", "", "| Feature | Type | Direction | Value | Score |", "|---|---|---|---|---:|"])
        for row in lane.get("feature_rankings", [])[:10]:
            lines.append(f"| `{row.get('feature')}` | `{row.get('feature_type')}` | `{row.get('direction')}` | `{row.get('value', '')}` | {row.get('separation_score')} |")
        lines.append("")
    return "\n".join(lines)


def render_data_quality_markdown(summary: Mapping[str, Any]) -> str:
    dq = summary.get("data_quality", {})
    lines = ["# RA9 Feature Discovery Data Quality", ""]
    for key, value in dq.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", "Missing feature values are excluded from that feature's separation score, not fabricated."])
    return "\n".join(lines) + "\n"


def render_contract_markdown() -> str:
    return """# RA9 Winner vs. Loser Feature Discovery Contract

RA9 compares observable entry/context features between winners and losers for
each research strategy candidate. It emits deterministic research hypotheses
only.

Guardrails:

- diagnostic_only=true
- production_recommendation=false
- trading_gate=false
- no broker actions
- no runtime restart
- no strategy changes
"""


def _data_quality(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    return {
        "total_rows": total,
        "missing_vwap_relation": sum(1 for row in rows if not row.get("vwap_relation") or row.get("vwap_relation") == "UNAVAILABLE"),
        "missing_avwap_relation": sum(1 for row in rows if not row.get("avwap_relation") or row.get("avwap_relation") == "UNAVAILABLE"),
        "missing_valid_gre": sum(1 for row in rows if row.get("gre_label_valid") == "UNAVAILABLE"),
        "missing_mfe": sum(1 for row in rows if _number(row.get("mfe_points")) is None and _number(row.get("path_mfe")) is None),
        "missing_mae": sum(1 for row in rows if _number(row.get("mae_points")) is None and _number(row.get("path_mae")) is None),
        "missing_trade_path": sum(1 for row in rows if row.get("path_coverage_status") in {None, "UNAVAILABLE", "MISSING_SOURCE"}),
    }


def _lane_data_quality_flags(rows: Sequence[Mapping[str, Any]], winners: Sequence[Mapping[str, Any]], losers: Sequence[Mapping[str, Any]]) -> list[str]:
    flags: list[str] = []
    if len(rows) < MIN_TOTAL_SAMPLE:
        flags.append("low_total_sample")
    if len(winners) < MIN_SIDE_SAMPLE:
        flags.append("low_winner_sample")
    if len(losers) < MIN_SIDE_SAMPLE:
        flags.append("low_loser_sample")
    if all(row.get("gre_label_valid") == "UNAVAILABLE" for row in rows):
        flags.append("missing_valid_gre_context")
    if all(row.get("vwap_relation") in {None, "UNAVAILABLE"} for row in rows):
        flags.append("missing_vwap_context")
    if all(_number(row.get("mfe_points")) is None and _number(row.get("path_mfe")) is None for row in rows):
        flags.append("missing_mfe_mae_context")
    return flags


def _confidence_class(sample_size: int, feature: Mapping[str, Any]) -> str:
    score = float(feature.get("separation_score") or 0.0)
    if sample_size >= 40 and score >= 0.5:
        return "DEVELOPING"
    if sample_size >= 15 and score >= 0.3:
        return "PRELIMINARY"
    return "EXPLORATORY"


def _subject(instrument: str, side: str) -> str:
    text = " ".join(part for part in (instrument, side.lower() + "s" if side else "trades") if part)
    return text or "trades"


def _common_value(values: Iterable[Any]) -> str:
    counts = _counts(_feature_value(value) for value in values)
    if not counts:
        return ""
    return max(counts.items(), key=lambda item: item[1])[0]


def _entry_hour_bucket(value: datetime | None) -> str:
    if value is None:
        return "UNKNOWN"
    return f"{value.hour:02d}:00-UTC"


def _session_open_proximity_bucket(session: Any, entry_time: datetime | None) -> str:
    if entry_time is None:
        return "UNKNOWN"
    anchor_hour = {
        "GLOBEX": 22,
        "ASIA": 0,
        "LONDON_OPEN": 7,
        "LONDON_LATE": 9,
        "US": 13,
        "US_ACTIVE": 13,
    }.get(str(session or "").upper())
    if anchor_hour is None:
        return "UNKNOWN"
    anchor = entry_time.replace(hour=anchor_hour, minute=0, second=0, microsecond=0)
    if entry_time.hour < anchor_hour and str(session or "").upper() == "GLOBEX":
        anchor = anchor - timedelta(days=1)
    minutes = int((entry_time - anchor).total_seconds() // 60)
    if minutes < 0:
        return "PRE_OPEN"
    if minutes <= 20:
        return "0-20m"
    if minutes <= 60:
        return "20-60m"
    if minutes <= 180:
        return "60-180m"
    return "180m+"


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


def _valid_context(value: Any) -> bool:
    return str(value or "").upper() == "VALID"


def _feature_value(value: Any) -> str:
    if value in (None, ""):
        return "UNKNOWN"
    return str(value)


def _counts(values: Iterable[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return counts


def _pooled_std(left: Sequence[float], right: Sequence[float]) -> float | None:
    values = list(left) + list(right)
    if len(values) < 2:
        return None
    avg = mean(values)
    variance = sum((value - avg) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)


def _rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 6)


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_ts(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _stable_id(prefix: str, *parts: Any) -> str:
    digest = hashlib.sha256("|".join(str(part or "") for part in parts).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}_{digest}"


def _fingerprint(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
