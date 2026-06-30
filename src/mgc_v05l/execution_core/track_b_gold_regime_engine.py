"""Diagnostic-only Regime Engine framework with a Gold MVP plugin.

This module is intentionally research/diagnostic only. It reads local
artifacts, scores explainable regime context, and writes bounded reports. It
does not import broker, strategy, runtime, or Managed Exit mutation paths.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol, Sequence

from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.session_phase_labels import label_session_phase, phase_coarse_session_group


SCHEMA_VERSION = "track_b_gold_regime_engine_v1"
FRAMEWORK_SCHEMA_VERSION = "track_b_regime_engine_framework_v1"
DEFAULT_OUTPUT_ROOT = Path("outputs/track_b_execution_core")
GOLD_REGIME_OUTPUT_DIR = Path("research/gold_regime_engine")
LATEST_GOLD_REGIME_JSON = "latest_gold_regime_engine.json"
LATEST_GOLD_REGIME_MD = "latest_gold_regime_engine.md"

REGIME_LONG = "LONG"
REGIME_SHORT = "SHORT"
REGIME_CHOP = "CHOP"
REGIME_TRANSITION = "TRANSITION"
REGIME_INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


@dataclass(frozen=True)
class RegimeEngineContext:
    instrument: str
    symbols: tuple[str, ...]
    candles_by_symbol_timeframe: Mapping[str, Mapping[str, tuple[dict[str, Any], ...]]]
    source_refs: Mapping[str, str]
    analytics: Mapping[str, Any]
    generated_at: datetime


class RegimeEnginePlugin(Protocol):
    plugin_id: str
    instrument: str

    def evaluate(self, context: RegimeEngineContext) -> dict[str, Any]:
        """Return a diagnostic-only regime report."""


def run_gold_regime_engine(
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    now: datetime | str | None = None,
    max_snapshot_bytes: int | None = None,
) -> dict[str, Any]:
    """Run the Gold Regime Engine MVP and publish bounded diagnostic outputs."""

    generated_at = _coerce_now(now)
    plugin = GoldRegimePlugin()
    context = load_gold_regime_context(output_root=output_root, generated_at=generated_at)
    report = plugin.evaluate(context)
    output_dir = output_root / GOLD_REGIME_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / LATEST_GOLD_REGIME_JSON
    md_path = output_dir / LATEST_GOLD_REGIME_MD
    config = BoundedSnapshotConfig(max_bytes=max_snapshot_bytes) if max_snapshot_bytes else BoundedSnapshotConfig()
    write_result = write_bounded_snapshot_json(
        json_path,
        report,
        config=config,
        diagnostic_path=output_dir / "latest_gold_regime_engine_bounded_snapshot_diagnostic.json",
    )
    md_path.write_text(render_gold_regime_markdown(report), encoding="utf-8")
    return {
        **report,
        "artifact_paths": {
            "json": str(json_path),
            "markdown": str(md_path),
            "bounded_snapshot_diagnostic": str(write_result.diagnostic_path) if write_result.diagnostic_path else None,
        },
        "bounded_snapshot": {
            "degraded": write_result.degraded,
            "written_bytes": write_result.written_bytes,
            "max_bytes": config.max_bytes,
        },
    }


def load_gold_regime_context(*, output_root: Path, generated_at: datetime) -> RegimeEngineContext:
    candles: dict[str, dict[str, tuple[dict[str, Any], ...]]] = {}
    source_refs: dict[str, str] = {}
    for symbol in ("GC", "MGC"):
        symbol_rows: dict[str, tuple[dict[str, Any], ...]] = {}
        for timeframe in ("1m", "3m", "5m"):
            path = output_root / "phase1_runtime_market_data" / symbol / timeframe / "latest_runtime_candles.json"
            payload = _read_json(path)
            rows = _extract_bars(payload)
            if rows:
                symbol_rows[timeframe] = rows
                source_refs[f"phase1_{symbol}_{timeframe}"] = str(path)
        candles[symbol] = symbol_rows
    analytics_paths = {
        "side_session_summary": output_root
        / "strategy_performance"
        / "side_session_attribution"
        / "latest_side_session_attribution_summary.json",
        "trend_overlay_summary": output_root / "trend_continuation_overlay" / "latest_trend_continuation_overlay_summary.json",
        "trade_pairing_summary": output_root / "strategy_performance" / "latest_trade_pairing_summary.json",
        "lane_performance_summary": output_root / "strategy_performance" / "latest_lane_performance_summary.json",
    }
    analytics: dict[str, Any] = {}
    for name, path in analytics_paths.items():
        payload = _read_json(path)
        if payload:
            analytics[name] = payload
            source_refs[name] = str(path)
    return RegimeEngineContext(
        instrument="GOLD",
        symbols=("GC", "MGC"),
        candles_by_symbol_timeframe=candles,
        source_refs=source_refs,
        analytics=analytics,
        generated_at=generated_at,
    )


class GoldRegimePlugin:
    plugin_id = "GRE"
    instrument = "GOLD"

    def evaluate(self, context: RegimeEngineContext) -> dict[str, Any]:
        gc_1m = context.candles_by_symbol_timeframe.get("GC", {}).get("1m", ())
        gc_5m = context.candles_by_symbol_timeframe.get("GC", {}).get("5m", ())
        mgc_1m = context.candles_by_symbol_timeframe.get("MGC", {}).get("1m", ())
        primary_1m = gc_1m or mgc_1m
        primary_5m = gc_5m or context.candles_by_symbol_timeframe.get("MGC", {}).get("5m", ())
        positive: list[dict[str, Any]] = []
        negative: list[dict[str, Any]] = []
        conflicts: list[dict[str, Any]] = []
        missing: list[str] = []

        if len(primary_1m) < 8 or len(primary_5m) < 4:
            missing.extend(_base_missing_features())
            return _report(
                context=context,
                regime_label=REGIME_INSUFFICIENT_EVIDENCE,
                confidence=0,
                directional_bias="UNKNOWN",
                session_label="UNKNOWN",
                recommended_holding_horizon="NONE",
                features={
                    "completed_1m_bars": len(primary_1m),
                    "completed_5m_bars": len(primary_5m),
                },
                positive_evidence=[],
                negative_evidence=[],
                conflicting_evidence=[],
                missing_evidence=_dedupe([*missing, "minimum current 1m/5m candles"]),
                explanation="Insufficient completed Gold candles for diagnostic regime scoring.",
            )

        latest_ts = _latest_timestamp(primary_1m) or context.generated_at
        session_label = label_session_phase(latest_ts)
        session_group = phase_coarse_session_group(session_label)
        one_minute = _trend_features(primary_1m[-24:])
        five_minute = _trend_features(primary_5m[-12:])
        candle = _candle_features(primary_1m[-16:])
        chop = _chop_features(primary_1m[-24:])
        multi_tf = _multi_timeframe_agreement(one_minute, five_minute)
        analytics_features = _analytics_features(context.analytics)
        features = {
            "plugin_id": self.plugin_id,
            "session_label": session_label,
            "session_group": session_group,
            "completed_1m_bars": len(primary_1m),
            "completed_5m_bars": len(primary_5m),
            "trend_persistence_1m": one_minute,
            "trend_persistence_5m": five_minute,
            "candle_body_strength": candle,
            "range_chop_index": chop,
            "multi_timeframe_agreement": multi_tf,
            "analytics_context": analytics_features,
        }

        score = 0.0
        score += _add_directional_evidence(
            positive,
            negative,
            feature_name="trend_persistence_1m",
            value=one_minute["direction"],
            strength=one_minute["strength"],
            bullish_points=22,
            bearish_points=-22,
        )
        score += _add_directional_evidence(
            positive,
            negative,
            feature_name="trend_persistence_5m",
            value=five_minute["direction"],
            strength=five_minute["strength"],
            bullish_points=18,
            bearish_points=-18,
        )
        score += _add_directional_evidence(
            positive,
            negative,
            feature_name="candle_body_strength",
            value=candle["direction"],
            strength=candle["body_strength"],
            bullish_points=14,
            bearish_points=-14,
        )
        score += _session_evidence(positive, negative, session_label, analytics_features)
        if multi_tf["agreement"] == "BULLISH":
            positive.append(_evidence("multi_timeframe_agreement", 12, "1m and 5m trend features agree bullishly.", multi_tf))
            score += 12
        elif multi_tf["agreement"] == "BEARISH":
            negative.append(_evidence("multi_timeframe_agreement", -12, "1m and 5m trend features agree bearishly.", multi_tf))
            score -= 12
        elif multi_tf["agreement"] == "CONFLICT":
            conflicts.append(_evidence("multi_timeframe_agreement", 0, "1m and 5m trend features disagree.", multi_tf))
        if chop["chop_score"] >= 0.58:
            negative.append(_evidence("range_chop_index", -18, "Recent bars are alternating or range-bound.", chop))
        elif chop["chop_score"] <= 0.36:
            positive.append(_evidence("range_chop_index", 8, "Recent range behavior is not chop-heavy.", chop))
            score += 8
        score -= 18 if chop["chop_score"] >= 0.68 else 0

        missing.extend(_base_missing_features())
        if not analytics_features["forward_path_available"]:
            missing.append("forward-path expectancy unavailable or stale")
        if not analytics_features["side_session_available"]:
            missing.append("side/session expectancy unavailable")

        label = _label_from_score(score, chop_score=chop["chop_score"], conflicts=conflicts)
        if label == REGIME_CHOP:
            directional_bias = "NEUTRAL"
        elif label == REGIME_LONG:
            directional_bias = "BULLISH"
        elif label == REGIME_SHORT:
            directional_bias = "BEARISH"
        elif label == REGIME_TRANSITION:
            directional_bias = "MIXED"
        else:
            directional_bias = "UNKNOWN"
        confidence = _confidence(score=score, label=label, chop_score=chop["chop_score"], conflicts=conflicts, missing=missing)
        explanation = _explanation(label=label, confidence=confidence, score=score, session_label=session_label)
        return _report(
            context=context,
            regime_label=label,
            confidence=confidence,
            directional_bias=directional_bias,
            session_label=session_label,
            recommended_holding_horizon=_recommended_horizon(label, confidence, session_group),
            features=features,
            positive_evidence=positive,
            negative_evidence=negative,
            conflicting_evidence=conflicts,
            missing_evidence=_dedupe(missing),
            explanation=explanation,
        )


def render_gold_regime_markdown(report: Mapping[str, Any]) -> str:
    def section(title: str, rows: Sequence[Any]) -> str:
        if not rows:
            return f"## {title}\n\n- None\n\n"
        text = f"## {title}\n\n"
        for row in rows:
            if isinstance(row, Mapping):
                text += f"- `{row.get('feature')}`: {row.get('explanation')} ({row.get('points')} pts)\n"
            else:
                text += f"- {row}\n"
        return text + "\n"

    return (
        "# Gold Regime Engine\n\n"
        f"Generated: {report.get('generated_at')}\n\n"
        f"Regime: **{report.get('regime_label')}**\n\n"
        f"Confidence: **{report.get('confidence')}**\n\n"
        f"Directional bias: `{report.get('directional_bias')}`\n\n"
        f"Session: `{report.get('session_label')}`\n\n"
        f"Recommended holding horizon: `{report.get('recommended_holding_horizon')}`\n\n"
        f"Diagnostic only: `{report.get('diagnostic_only')}`\n\n"
        f"## Explanation\n\n{report.get('explanation')}\n\n"
        + section("Positive Evidence", report.get("positive_evidence", []))
        + section("Negative Evidence", report.get("negative_evidence", []))
        + section("Conflicting Evidence", report.get("conflicting_evidence", []))
        + section("Missing Evidence", report.get("missing_evidence", []))
        + "## Safety Boundary\n\n"
        "- No broker authority\n"
        "- No runtime authority\n"
        "- No Managed Exit authority\n"
        "- No strategy gating\n"
    )


def _report(
    *,
    context: RegimeEngineContext,
    regime_label: str,
    confidence: int,
    directional_bias: str,
    session_label: str,
    recommended_holding_horizon: str,
    features: Mapping[str, Any],
    positive_evidence: Sequence[Mapping[str, Any]],
    negative_evidence: Sequence[Mapping[str, Any]],
    conflicting_evidence: Sequence[Mapping[str, Any]],
    missing_evidence: Sequence[str],
    explanation: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "framework_schema_version": FRAMEWORK_SCHEMA_VERSION,
        "generated_at": context.generated_at.isoformat(),
        "plugin_id": "GRE",
        "plugin_architecture": {
            "instrument_plugin": "GoldRegimePlugin",
            "framework_supports": ["GRE", "NRE", "ERE", "TRE"],
            "gold_hardcoded_in_framework": False,
        },
        "instrument": context.instrument,
        "symbols": list(context.symbols),
        "regime_label": regime_label,
        "confidence": max(0, min(100, int(round(confidence)))),
        "directional_bias": directional_bias,
        "session_label": session_label,
        "trade_permission_recommendation": "DIAGNOSTIC_ONLY_NOT_FOR_GATING",
        "recommended_holding_horizon": recommended_holding_horizon,
        "features": dict(features),
        "positive_evidence": list(positive_evidence),
        "negative_evidence": list(negative_evidence),
        "conflicting_evidence": list(conflicting_evidence),
        "missing_evidence": list(missing_evidence),
        "explanation": explanation,
        "source_refs": dict(context.source_refs),
        "diagnostic_only": True,
        "analytics_only": True,
        "runtime_authority": False,
        "broker_authority": False,
        "managed_exit_authority": False,
        "strategy_authority": False,
        "submit_allowed": False,
        "order_intent_created": False,
        "broker_state_mutated": False,
    }


def _read_json(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _extract_bars(payload: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    rows = payload.get("bars") or payload.get("candles") or []
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        return ()
    normalized = [_normalize_bar(row) for row in rows if isinstance(row, Mapping)]
    valid = [row for row in normalized if row is not None]
    return tuple(valid)


def _normalize_bar(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        timestamp = _parse_datetime(row.get("bar_end") or row.get("timestamp") or row.get("ts") or row.get("bar_start"))
        open_price = float(row["open"])
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])
    except (KeyError, TypeError, ValueError):
        return None
    if timestamp is None or high < low or close <= 0:
        return None
    return {
        "timestamp": timestamp,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": _optional_float(row.get("volume")),
    }


def _trend_features(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    closes = [float(row["close"]) for row in rows if "close" in row]
    if len(closes) < 3:
        return {"direction": "UNKNOWN", "strength": 0.0, "net_change": 0.0, "up_bar_ratio": 0.0}
    net_change = closes[-1] - closes[0]
    steps = [b - a for a, b in zip(closes, closes[1:])]
    up_ratio = sum(1 for step in steps if step > 0) / max(1, len(steps))
    down_ratio = sum(1 for step in steps if step < 0) / max(1, len(steps))
    avg_abs_step = sum(abs(step) for step in steps) / max(1, len(steps))
    strength = min(1.0, abs(net_change) / max(avg_abs_step * max(1, len(steps)) * 0.65, 1e-9))
    if net_change > 0 and up_ratio >= 0.55:
        direction = "UP"
    elif net_change < 0 and down_ratio >= 0.55:
        direction = "DOWN"
    else:
        direction = "MIXED"
    return {
        "direction": direction,
        "strength": round(strength, 4),
        "net_change": round(net_change, 4),
        "up_bar_ratio": round(up_ratio, 4),
        "down_bar_ratio": round(down_ratio, 4),
    }


def _candle_features(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"direction": "UNKNOWN", "body_strength": 0.0, "average_body_to_range": 0.0}
    body_ratios: list[float] = []
    signed_bodies: list[float] = []
    for row in rows:
        candle_range = max(float(row["high"]) - float(row["low"]), 1e-9)
        body = float(row["close"]) - float(row["open"])
        body_ratios.append(abs(body) / candle_range)
        signed_bodies.append(body)
    avg_ratio = sum(body_ratios) / len(body_ratios)
    net_body = sum(signed_bodies)
    if net_body > 0 and avg_ratio >= 0.35:
        direction = "UP"
    elif net_body < 0 and avg_ratio >= 0.35:
        direction = "DOWN"
    else:
        direction = "MIXED"
    return {
        "direction": direction,
        "body_strength": round(min(1.0, avg_ratio), 4),
        "average_body_to_range": round(avg_ratio, 4),
        "net_body": round(net_body, 4),
    }


def _chop_features(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    closes = [float(row["close"]) for row in rows if "close" in row]
    if len(closes) < 5:
        return {"chop_score": 1.0, "alternation_ratio": 1.0, "net_to_range_ratio": 0.0}
    steps = [b - a for a, b in zip(closes, closes[1:])]
    signs = [1 if step > 0 else -1 if step < 0 else 0 for step in steps]
    alternations = sum(1 for a, b in zip(signs, signs[1:]) if a and b and a != b)
    alternation_ratio = alternations / max(1, len(signs) - 1)
    total_range = max(max(closes) - min(closes), 1e-9)
    net_to_range = abs(closes[-1] - closes[0]) / total_range
    chop_score = max(0.0, min(1.0, 0.65 * alternation_ratio + 0.35 * (1.0 - net_to_range)))
    return {
        "chop_score": round(chop_score, 4),
        "alternation_ratio": round(alternation_ratio, 4),
        "net_to_range_ratio": round(net_to_range, 4),
    }


def _multi_timeframe_agreement(one_minute: Mapping[str, Any], five_minute: Mapping[str, Any]) -> dict[str, Any]:
    one = one_minute.get("direction")
    five = five_minute.get("direction")
    if one == "UP" and five == "UP":
        agreement = "BULLISH"
    elif one == "DOWN" and five == "DOWN":
        agreement = "BEARISH"
    elif one in {"UP", "DOWN"} and five in {"UP", "DOWN"} and one != five:
        agreement = "CONFLICT"
    else:
        agreement = "MIXED"
    return {"agreement": agreement, "one_minute_direction": one, "five_minute_direction": five}


def _analytics_features(analytics: Mapping[str, Any]) -> dict[str, Any]:
    side_session = analytics.get("side_session_summary")
    trend_overlay = analytics.get("trend_overlay_summary")
    return {
        "side_session_available": isinstance(side_session, Mapping),
        "forward_path_available": isinstance(trend_overlay, Mapping)
        and int(trend_overlay.get("path_available_count") or 0) > 0,
        "trend_overlay_classification": trend_overlay.get("classification") if isinstance(trend_overlay, Mapping) else None,
        "paired_trade_count": _safe_int(
            analytics.get("trade_pairing_summary", {}).get("paired_trades")
            if isinstance(analytics.get("trade_pairing_summary"), Mapping)
            else None
        ),
    }


def _add_directional_evidence(
    positive: list[dict[str, Any]],
    negative: list[dict[str, Any]],
    *,
    feature_name: str,
    value: Any,
    strength: Any,
    bullish_points: int,
    bearish_points: int,
) -> float:
    try:
        scaled_strength = max(0.0, min(1.0, float(strength)))
    except (TypeError, ValueError):
        scaled_strength = 0.0
    if value == "UP" and scaled_strength >= 0.2:
        points = bullish_points * scaled_strength
        positive.append(_evidence(feature_name, points, f"{feature_name} points upward.", {"direction": value, "strength": scaled_strength}))
        return points
    if value == "DOWN" and scaled_strength >= 0.2:
        points = bearish_points * scaled_strength
        negative.append(_evidence(feature_name, points, f"{feature_name} points downward.", {"direction": value, "strength": scaled_strength}))
        return points
    return 0.0


def _session_evidence(
    positive: list[dict[str, Any]],
    negative: list[dict[str, Any]],
    session_label: str,
    analytics_features: Mapping[str, Any],
) -> float:
    session_group = phase_coarse_session_group(session_label)
    if session_label == "LONDON_LATE":
        positive.append(_evidence("session_label", 8, "London Late is an explicit GRE MVP cohort with observed directional behavior.", {"session_label": session_label}))
        return 8
    if session_group == "US":
        negative.append(_evidence("session_label", -4, "US session behavior is under review and treated conservatively in GRE MVP.", {"session_label": session_label}))
        return -4
    if session_group in {"ASIA", "LONDON"} and analytics_features.get("side_session_available"):
        positive.append(_evidence("session_label", 4, "Session analytics are available for this non-US cohort.", {"session_label": session_label}))
        return 4
    return 0


def _label_from_score(score: float, *, chop_score: float, conflicts: Sequence[Mapping[str, Any]]) -> str:
    if chop_score >= 0.72 and abs(score) < 24:
        return REGIME_CHOP
    if conflicts and abs(score) < 32:
        return REGIME_TRANSITION
    if score >= 20:
        return REGIME_LONG
    if score <= -20:
        return REGIME_SHORT
    if chop_score >= 0.58:
        return REGIME_CHOP
    return REGIME_TRANSITION


def _confidence(
    *,
    score: float,
    label: str,
    chop_score: float,
    conflicts: Sequence[Mapping[str, Any]],
    missing: Sequence[str],
) -> int:
    if label == REGIME_INSUFFICIENT_EVIDENCE:
        return 0
    base = 42 + min(34, abs(score))
    if label == REGIME_CHOP:
        base = 45 + min(30, chop_score * 35)
    if conflicts:
        base -= 10
    base -= min(18, len(missing) * 3)
    return int(max(15, min(95, round(base))))


def _recommended_horizon(label: str, confidence: int, session_group: str) -> str:
    if label in {REGIME_CHOP, REGIME_INSUFFICIENT_EVIDENCE}:
        return "NO_DIRECTIONAL_HOLD_RECOMMENDATION"
    if label == REGIME_TRANSITION:
        return "5M_TO_15M_DIAGNOSTIC_ONLY"
    if confidence >= 75 and session_group in {"LONDON", "US"}:
        return "15M_TO_30M_DIAGNOSTIC_ONLY"
    return "5M_TO_15M_DIAGNOSTIC_ONLY"


def _explanation(*, label: str, confidence: int, score: float, session_label: str) -> str:
    return (
        f"GRE classified Gold as {label} with confidence {confidence} from an explainable score "
        f"of {score:.2f} using current Phase-1 candle structure, trend persistence, range/chop, "
        f"multi-timeframe agreement, and session context for {session_label}."
    )


def _evidence(feature: str, points: float, explanation: str, value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "feature": feature,
        "points": round(points, 4),
        "explanation": explanation,
        "value": dict(value),
    }


def _base_missing_features() -> list[str]:
    return [
        "VWAP unavailable in GRE MVP",
        "anchored VWAP unavailable in GRE MVP",
        "overnight high/low unavailable in GRE MVP",
        "prior session high/low unavailable in GRE MVP",
    ]


def _latest_timestamp(rows: Sequence[Mapping[str, Any]]) -> datetime | None:
    for row in reversed(rows):
        ts = row.get("timestamp")
        if isinstance(ts, datetime):
            return ts
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if not isinstance(value, str) or not value:
        return None
    text = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _coerce_now(value: datetime | str | None) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        parsed = _parse_datetime(value)
        if parsed is not None:
            return parsed
    return datetime.now(UTC)


def _optional_float(value: Any) -> float | None:
    try:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _dedupe(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out
