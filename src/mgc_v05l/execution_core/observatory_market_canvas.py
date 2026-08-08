"""Read-only Market Canvas producer for The Observatory.

The producer consumes current Phase-1 completed 5m candles and projects
existing participation-quality evidence into the four Observatory display
dimensions. It does not create strategy signals, recommendations, readiness, or
broker/runtime authority.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_participation_quality import (
    INPUT_MODE_RUNTIME_DECISION,
    SOURCE_CATEGORY_RUNTIME,
    build_participation_quality_state,
)


SCHEMA_VERSION = "observatory_market_canvas_v1"
SOURCE_KIND = "PHASE1_COMPLETED_5M_CANDLE_PARTICIPATION_QUALITY"
DEFAULT_OUTPUT_PATH = Path("desktop/prototypes/active-desktop/market_canvas_snapshot.generated.mjs")
DEFAULT_CANDLE_ROOT = Path("outputs/track_b_execution_core/phase1_runtime_market_data")
DEFAULT_BREADTH_PATH = Path("outputs/track_b_execution_core/observatory/market_breadth/latest_market_breadth.json")
DEFAULT_SYMBOLS = ("ES", "MES", "NQ", "MNQ", "GC", "MGC", "ZT", "ZF", "ZN", "ZB")
GUARDRAILS = {
    "display_only": True,
    "trading_input": False,
    "runtime_authority": False,
    "broker_authority": False,
    "research_authority": False,
    "recommendation": False,
}


@dataclass(frozen=True)
class SourceResult:
    symbol: str
    source_artifact: str
    status: str
    report: Mapping[str, Any] | None
    error: str | None = None


def build_market_canvas_snapshot(
    *,
    repo_root: Path | str = Path("."),
    candle_root: Path | str = DEFAULT_CANDLE_ROOT,
    breadth_path: Path | str | None = DEFAULT_BREADTH_PATH,
    symbols: Sequence[str] = DEFAULT_SYMBOLS,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve()
    resolved_candle_root = _resolve(root, Path(candle_root))
    generated = generated_at or datetime.now(UTC)
    source_results = [
        _participation_source_for_symbol(
            symbol=symbol,
            root=root,
            candle_root=resolved_candle_root,
            generated_at=generated,
        )
        for symbol in symbols
    ]
    valid_reports = [result.report for result in source_results if result.status == "VALID" and result.report]
    breadth = _load_breadth(root, Path(breadth_path)) if breadth_path is not None else None
    dimensions = {
        "coherence": _coherence_dimension(valid_reports, source_results),
        "spread": _spread_dimension(breadth, source_results),
        "magnitude": _magnitude_dimension(valid_reports, source_results),
        "edge": _edge_dimension(valid_reports, source_results),
    }
    source_artifacts = [result.source_artifact for result in source_results if result.source_artifact]
    if breadth and breadth.get("source_artifact"):
        source_artifacts.append(str(breadth["source_artifact"]))
    status = _overall_status(dimensions)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated.isoformat(),
        "status": status,
        "source_kind": SOURCE_KIND,
        "input_mode": INPUT_MODE_RUNTIME_DECISION,
        "source_category": SOURCE_CATEGORY_RUNTIME,
        "instrument_universe": list(symbols),
        "timeframe": "5m",
        "completed_candle_semantics": "completed 5m candles only; incomplete bars are excluded by source classifier",
        "dimensions": dimensions,
        "source_artifacts": source_artifacts,
        "source_fingerprints": _fingerprints(root, source_artifacts),
        "source_reports": [_source_summary(result) for result in source_results],
        "breadth_source": None if breadth is None else breadth,
        "freshness": _freshness(valid_reports, generated),
        "guardrails": dict(GUARDRAILS),
    }


def write_snapshot(snapshot: Mapping[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(dict(snapshot), indent=2, sort_keys=True)
    if output_path.suffix == ".json":
        output_path.write_text(text + "\n", encoding="utf-8")
        return
    output_path.write_text(f"export const marketCanvasSnapshot = {text};\n", encoding="utf-8")


def _participation_source_for_symbol(
    *,
    symbol: str,
    root: Path,
    candle_root: Path,
    generated_at: datetime,
) -> SourceResult:
    artifact = candle_root / symbol / "5m" / "latest_runtime_candles.json"
    relative = _relative(root, artifact)
    if not artifact.exists():
        return SourceResult(symbol=symbol, source_artifact=relative, status="MISSING", report=None, error="source_missing")
    try:
        payload = json.loads(artifact.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return SourceResult(symbol=symbol, source_artifact=relative, status="INVALID", report=None, error=str(exc))
    bars = payload.get("bars")
    if not isinstance(bars, list):
        return SourceResult(symbol=symbol, source_artifact=relative, status="INVALID", report=None, error="bars_not_array")
    report = build_participation_quality_state(
        {
            "instrument": symbol,
            "source_id": f"phase1_runtime_market_data_{symbol}_5m",
            "candles": _with_timeframe(bars, "5m"),
            "input_source_path": relative,
            "input_source_category": SOURCE_CATEGORY_RUNTIME,
            "input_mode": INPUT_MODE_RUNTIME_DECISION,
        },
        now=generated_at,
        candle_timeframe="5m",
        input_mode=INPUT_MODE_RUNTIME_DECISION,
        input_source_path=relative,
        input_source_category=SOURCE_CATEGORY_RUNTIME,
    )
    failures = list(report.get("confidence_failure_reasons") or [])
    status = "VALID" if not failures and report.get("freshness_status") == "FRESH" else _source_status(report)
    return SourceResult(symbol=symbol, source_artifact=relative, status=status, report=report)


def _with_timeframe(rows: Sequence[Any], timeframe: str) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        updated = dict(row)
        updated.setdefault("timeframe", timeframe)
        if "timestamp" not in updated and "end_ts" not in updated and "bar_end" in updated:
            updated["timestamp"] = updated["bar_end"]
        output.append(updated)
    return output


def _coherence_dimension(reports: Sequence[Mapping[str, Any]], sources: Sequence[SourceResult]) -> dict[str, Any]:
    values = [_number(_features(report).get("directional_persistence")) for report in reports]
    values = [value for value in values if value is not None]
    if not values:
        return _missing_dimension("coherence", "No fresh runtime participation-quality directional persistence is available.", sources)
    value = _clamp(sum(values) / len(values), 0.0, 1.0)
    return _dimension(
        name="coherence",
        value=value,
        status="VALID",
        mapping="DIRECT_NORMALIZATION",
        producer_state=_producer_state_summary(reports, "participation_state"),
        source_ref="track_b_participation_quality.feature_summary.directional_persistence",
        explanation="Average producer-authored directional persistence across fresh Phase-1 5m symbols.",
        sources=sources,
    )


def _magnitude_dimension(reports: Sequence[Mapping[str, Any]], sources: Sequence[SourceResult]) -> dict[str, Any]:
    ratios = [_number(_features(report).get("range_expansion_ratio")) for report in reports]
    ratios = [ratio for ratio in ratios if ratio is not None]
    if not ratios:
        return _missing_dimension("magnitude", "No fresh runtime participation-quality range expansion is available.", sources)
    average_ratio = sum(ratios) / len(ratios)
    value = _clamp((average_ratio - 0.75) / (1.65 - 0.75), 0.0, 1.0)
    return _dimension(
        name="magnitude",
        value=value,
        status="VALID",
        mapping="DIRECT_NORMALIZATION",
        producer_state=f"average_range_expansion_ratio={average_ratio:.6f}",
        source_ref="track_b_participation_quality.feature_summary.range_expansion_ratio",
        explanation="Range expansion ratio normalized by existing participation-quality thresholds.",
        sources=sources,
    )


def _edge_dimension(reports: Sequence[Mapping[str, Any]], sources: Sequence[SourceResult]) -> dict[str, Any]:
    decays = [_number(_features(report).get("impulse_decay")) for report in reports]
    decays = [decay for decay in decays if decay is not None]
    if not decays:
        return _missing_dimension("edge", "No fresh runtime participation-quality impulse-decay evidence is available.", sources)
    average_decay = sum(decays) / len(decays)
    value = _clamp(1.0 - average_decay, 0.0, 1.0)
    return _dimension(
        name="edge",
        value=value,
        status="VALID",
        mapping="DIRECT_NORMALIZATION",
        producer_state=f"average_impulse_decay={average_decay:.6f}",
        source_ref="track_b_participation_quality.feature_summary.impulse_decay",
        explanation="Inverse of producer-authored impulse decay: sharper initiative is higher, exhaustion/decay is lower.",
        sources=sources,
    )


def _spread_dimension(breadth: Mapping[str, Any] | None, sources: Sequence[SourceResult]) -> dict[str, Any]:
    if not breadth or breadth.get("status") != "LOADED":
        return _unsupported_dimension(
            "spread",
            "No current producer-authored factual breadth artifact is available.",
            sources,
        )
    data = breadth.get("data")
    if not isinstance(data, Mapping):
        return _unsupported_dimension("spread", "Breadth artifact is invalid or unreadable.", sources)
    if data.get("metric_kind") != "FACTUAL_BREADTH_METRIC":
        return _unsupported_dimension("spread", "Breadth artifact does not declare FACTUAL_BREADTH_METRIC.", sources)
    valid_count = _number(data.get("valid_symbol_count")) or 0.0
    eligible_count = _number(data.get("eligible_symbol_count")) or 0.0
    coverage = _number(data.get("coverage_ratio"))
    percent_advancing = _number(data.get("percent_advancing"))
    percent_declining = _number(data.get("percent_declining"))
    if not coverage or coverage <= 0.0 or valid_count <= 0.0 or eligible_count <= 0.0:
        return _unsupported_dimension("spread", "Factual breadth has no valid symbols yet.", sources)
    if percent_advancing is None or percent_declining is None:
        return _unsupported_dimension("spread", "Factual breadth is missing advancing/declining percentages.", sources)
    directional_imbalance = abs(percent_advancing - percent_declining) / 100.0
    value = _clamp((coverage * 0.55) + (directional_imbalance * 0.45), 0.0, 1.0)
    return _dimension(
        name="spread",
        value=value,
        status="VALID",
        mapping="DIRECT_NORMALIZATION",
        producer_state=(
            f"metric_kind=FACTUAL_BREADTH_METRIC;"
            f"valid={int(valid_count)}/{int(eligible_count)};"
            f"percent_advancing={percent_advancing};percent_declining={percent_declining}"
        ),
        source_ref="observatory_market_breadth_v1",
        explanation="Factual breadth coverage and advancing/declining dispersion normalized for visual spread only.",
        sources=sources,
    )


def _dimension(
    *,
    name: str,
    value: float | None,
    status: str,
    mapping: str,
    producer_state: str,
    source_ref: str,
    explanation: str,
    sources: Sequence[SourceResult],
) -> dict[str, Any]:
    return {
        "value": None if value is None else round(value, 6),
        "status": status,
        "mapping": mapping,
        "producer_state": producer_state,
        "source_ref": source_ref,
        "explanation": explanation,
        "valid_source_count": sum(1 for source in sources if source.status == "VALID"),
        "source_count": len(sources),
        "dimension": name,
    }


def _unsupported_dimension(name: str, reason: str, sources: Sequence[SourceResult]) -> dict[str, Any]:
    return _dimension(
        name=name,
        value=None,
        status="UNKNOWN",
        mapping="UNSUPPORTED",
        producer_state="UNSUPPORTED",
        source_ref="none",
        explanation=reason,
        sources=sources,
    )


def _missing_dimension(name: str, reason: str, sources: Sequence[SourceResult]) -> dict[str, Any]:
    return _dimension(
        name=name,
        value=None,
        status="MISSING",
        mapping="UNSUPPORTED",
        producer_state="NO_VALID_RUNTIME_SOURCES",
        source_ref="track_b_participation_quality",
        explanation=reason,
        sources=sources,
    )


def _source_status(report: Mapping[str, Any]) -> str:
    failures = set(report.get("confidence_failure_reasons") or [])
    freshness = str(report.get("freshness_status") or "UNKNOWN")
    if "STALE_INPUT" in failures or freshness == "STALE":
        return "STALE"
    if failures:
        return "INVALID"
    return "UNKNOWN"


def _source_summary(result: SourceResult) -> dict[str, Any]:
    report = result.report or {}
    return {
        "symbol": result.symbol,
        "status": result.status,
        "source_artifact": result.source_artifact,
        "error": result.error,
        "schema_version": report.get("schema_version"),
        "producer": report.get("producer"),
        "input_mode": report.get("input_mode"),
        "input_source_category": report.get("input_source_category"),
        "source_provenance_status": report.get("source_provenance_status"),
        "freshness_status": report.get("freshness_status"),
        "latest_candle_timestamp": report.get("latest_candle_timestamp"),
        "participation_state": report.get("participation_state"),
        "confidence_state": report.get("confidence_state"),
        "confidence_failure_reasons": list(report.get("confidence_failure_reasons") or []),
    }


def _features(report: Mapping[str, Any]) -> Mapping[str, Any]:
    value = report.get("feature_summary")
    return value if isinstance(value, Mapping) else {}


def _producer_state_summary(reports: Sequence[Mapping[str, Any]], key: str) -> str:
    counts: dict[str, int] = {}
    for report in reports:
        value = str(report.get(key) or "UNKNOWN")
        counts[value] = counts.get(value, 0) + 1
    return ",".join(f"{name}:{count}" for name, count in sorted(counts.items())) or "UNKNOWN"


def _freshness(reports: Sequence[Mapping[str, Any]], generated_at: datetime) -> dict[str, Any]:
    latest = sorted(str(report.get("latest_candle_timestamp")) for report in reports if report.get("latest_candle_timestamp"))
    return {
        "generated_at": generated_at.isoformat(),
        "latest_candle_timestamp": latest[-1] if latest else None,
        "valid_source_count": len(reports),
        "stale_after": (generated_at + timedelta(minutes=5)).isoformat(),
    }


def _overall_status(dimensions: Mapping[str, Mapping[str, Any]]) -> str:
    statuses = {str(dimension.get("status")) for dimension in dimensions.values()}
    if "INVALID" in statuses:
        return "INVALID"
    if "VALID" in statuses and ({"UNKNOWN", "MISSING", "STALE", "NOT_READY"} & statuses):
        return "VALID_WITH_WARNINGS"
    if statuses == {"VALID"}:
        return "READY"
    return "NOT_READY"


def _fingerprints(root: Path, source_artifacts: Sequence[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for artifact in source_artifacts:
        path = _resolve(root, Path(artifact))
        if not path.exists() or not path.is_file():
            continue
        result[artifact] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def _load_breadth(root: Path, path: Path) -> dict[str, Any]:
    resolved = _resolve(root, path)
    relative = _relative(root, resolved)
    if not resolved.exists():
        return {"status": "MISSING", "source_artifact": relative, "data": None}
    try:
        return {"status": "LOADED", "source_artifact": relative, "data": json.loads(resolved.read_text(encoding="utf-8"))}
    except json.JSONDecodeError as exc:
        return {"status": "INVALID", "source_artifact": relative, "data": None, "error": str(exc)}


def _resolve(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else root / path


def _relative(root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(root))
    except ValueError:
        return str(path.resolve())


def _number(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed else None


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build Observatory Market Canvas display snapshot.")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--candle-root", default=str(DEFAULT_CANDLE_ROOT))
    parser.add_argument("--breadth-path", default=str(DEFAULT_BREADTH_PATH))
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--generated-at")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH))
    args = parser.parse_args(argv)
    snapshot = build_market_canvas_snapshot(
        repo_root=Path(args.repo_root),
        candle_root=Path(args.candle_root),
        breadth_path=Path(args.breadth_path),
        symbols=tuple(symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()),
        generated_at=parse_timestamp(args.generated_at),
    )
    write_snapshot(snapshot, Path(args.output))
    print(
        json.dumps(
            {
                "ok": True,
                "output": args.output,
                "status": snapshot["status"],
                "dimensions": {
                    key: {"status": value["status"], "value": value["value"], "mapping": value["mapping"]}
                    for key, value in snapshot["dimensions"].items()
                },
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
