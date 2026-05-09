"""Read-only Phase-1 HOT runtime candle/feature readiness checks."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.phase1_runtime_ticker_registry import (
    PHASE1_RUNTIME_DERIVED_FEATURES,
    PHASE1_RUNTIME_TICKER_ORDER,
    PHASE1_RUNTIME_TIMEFRAMES,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "phase1_runtime_data_readiness"
DEFAULT_RUNTIME_CANDLE_ROOT = Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data"
DEFAULT_RUNTIME_FEATURE_ROOT = Path("outputs") / "track_b_execution_core" / "phase1_runtime_features"
FRESHNESS_SECONDS_BY_TIMEFRAME = {
    "1m": 180.0,
    "3m": 360.0,
    "5m": 600.0,
}


@dataclass(frozen=True)
class Phase1RuntimeDataReadinessConfig:
    repo_root: Path = REPO_ROOT
    output_dir: Path = DEFAULT_OUTPUT_DIR
    runtime_candle_root: Path = DEFAULT_RUNTIME_CANDLE_ROOT
    runtime_feature_root: Path = DEFAULT_RUNTIME_FEATURE_ROOT
    now: datetime | None = None


@dataclass(frozen=True)
class Phase1RuntimeDataReadinessArtifacts:
    report: dict[str, Any]
    rows: list[dict[str, Any]]


def build_phase1_runtime_data_readiness(
    *,
    config: Phase1RuntimeDataReadinessConfig,
) -> Phase1RuntimeDataReadinessArtifacts:
    now = _coerce_now(config.now)
    rows = [
        _ticker_readiness(symbol=symbol, config=config, now=now)
        for symbol in PHASE1_RUNTIME_TICKER_ORDER
    ]
    report = {
        "schema_version": "phase1_runtime_data_readiness_v1",
        "generated_at": now.isoformat(),
        "repo_root": str(Path(config.repo_root)),
        "archive_artifact_used": False,
        "research_artifact_used": False,
        "completed_candles_only": True,
        "source_policy": "HOT runtime data only from active Dev-root outputs/track_b_execution_core paths",
        "row_count": len(rows),
        "ready_ticker_count": sum(1 for row in rows if row["runtime_candles_ready"] and row["derived_features_ready"]),
        "rows": rows,
    }
    return Phase1RuntimeDataReadinessArtifacts(report=report, rows=rows)


def write_phase1_runtime_data_readiness_artifacts(
    *,
    config: Phase1RuntimeDataReadinessConfig,
    artifacts: Phase1RuntimeDataReadinessArtifacts,
) -> None:
    output_dir = Path(config.output_dir)
    if not output_dir.is_absolute():
        output_dir = Path(config.repo_root) / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "latest_phase1_runtime_data_readiness.json").write_text(
        json.dumps(artifacts.report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _ticker_readiness(*, symbol: str, config: Phase1RuntimeDataReadinessConfig, now: datetime) -> dict[str, Any]:
    candle_checks = {
        timeframe: _artifact_check(
            path=_candle_path(config=config, symbol=symbol, timeframe=timeframe),
            symbol=symbol,
            timeframe=timeframe,
            now=now,
            kind="candles",
        )
        for timeframe in PHASE1_RUNTIME_TIMEFRAMES
    }
    feature_checks = {
        timeframe: _artifact_check(
            path=_feature_path(config=config, symbol=symbol, timeframe=timeframe),
            symbol=symbol,
            timeframe=timeframe,
            now=now,
            kind="features",
        )
        for timeframe in PHASE1_RUNTIME_TIMEFRAMES
    }
    candle_ready = all(check["ready"] for check in candle_checks.values())
    feature_ready = all(check["ready"] for check in feature_checks.values())
    return {
        "symbol": symbol,
        "timeframes": list(PHASE1_RUNTIME_TIMEFRAMES),
        "runtime_candles_ready": candle_ready,
        "runtime_candles_block_reason": "READY" if candle_ready else _first_reason(candle_checks),
        "derived_features_ready": feature_ready,
        "derived_features_block_reason": "READY" if feature_ready else _first_reason(feature_checks),
        "feature_names": list(PHASE1_RUNTIME_DERIVED_FEATURES),
        "candle_checks": candle_checks,
        "feature_checks": feature_checks,
    }


def _artifact_check(
    *,
    path: Path,
    symbol: str,
    timeframe: str,
    now: datetime,
    kind: str,
) -> dict[str, Any]:
    policy_error = _runtime_path_policy_error(path)
    if policy_error:
        return _not_ready(path=path, reason=policy_error, kind=kind)
    payload, error = _load_json(path)
    if error:
        missing_reason = "RUNTIME_CANDLES_MISSING" if kind == "candles" else "FEATURES_MISSING"
        return _not_ready(path=path, reason=missing_reason if "missing" in error else "RUNTIME_ARTIFACT_INVALID", kind=kind, detail=error)
    if not isinstance(payload, dict):
        return _not_ready(path=path, reason="RUNTIME_ARTIFACT_INVALID", kind=kind, detail="payload is not an object")
    generated_at = _parse_datetime(payload.get("generated_at"))
    source_id = str(payload.get("source_id") or "").strip()
    completed_only = payload.get("completed_candles_only")
    payload_symbol = str(payload.get("symbol") or "").strip().upper()
    payload_timeframe = str(payload.get("timeframe") or "").strip()
    bars = payload.get("bars") or payload.get("candles") or payload.get("features") or []
    bar_count = len(bars) if isinstance(bars, list) else int(payload.get("bar_count") or payload.get("feature_count") or 0)
    if payload_symbol != symbol:
        return _not_ready(path=path, reason="RUNTIME_SYMBOL_MISMATCH", kind=kind, detail=f"symbol={payload_symbol}")
    if payload_timeframe != timeframe:
        return _not_ready(path=path, reason="RUNTIME_TIMEFRAME_MISMATCH", kind=kind, detail=f"timeframe={payload_timeframe}")
    if generated_at is None:
        return _not_ready(path=path, reason="RUNTIME_GENERATED_AT_MISSING", kind=kind)
    if not source_id:
        return _not_ready(path=path, reason="RUNTIME_SOURCE_ID_MISSING", kind=kind)
    if completed_only is not True:
        return _not_ready(path=path, reason="COMPLETED_CANDLE_SEMANTICS_MISSING", kind=kind)
    if bar_count <= 0:
        return _not_ready(path=path, reason="RUNTIME_BARS_MISSING", kind=kind)
    age_seconds = max(0.0, (now - generated_at).total_seconds())
    freshness_seconds = FRESHNESS_SECONDS_BY_TIMEFRAME[timeframe]
    if age_seconds > freshness_seconds:
        stale_reason = "RUNTIME_CANDLES_STALE" if kind == "candles" else "FEATURES_STALE"
        return _not_ready(path=path, reason=stale_reason, kind=kind, age_seconds=age_seconds, bar_count=bar_count)
    return {
        "ready": True,
        "path": str(path),
        "reason": "READY",
        "generated_at": generated_at.isoformat(),
        "source_id": source_id,
        "age_seconds": age_seconds,
        "freshness_seconds": freshness_seconds,
        "bar_count": bar_count,
        "completed_candles_only": completed_only,
    }


def _not_ready(
    *,
    path: Path,
    reason: str,
    kind: str,
    detail: str = "",
    age_seconds: float | None = None,
    bar_count: int = 0,
) -> dict[str, Any]:
    if kind == "features" and reason == "FEATURES_MISSING":
        reason = "FEATURES_NOT_IMPLEMENTED"
    return {
        "ready": False,
        "path": str(path),
        "reason": reason,
        "detail": detail,
        "age_seconds": age_seconds,
        "bar_count": bar_count,
    }


def _runtime_path_policy_error(path: Path) -> str | None:
    text = str(path)
    if "/outputs/track_b_research/" in text or "/track_b_research/" in text:
        return "RESEARCH_ONLY_UNSAFE"
    if "/_archived" in text or "/archive" in text.lower():
        return "ARCHIVE_PATH_UNSAFE"
    if "/Users/patrick/Documents" in text or "Mobile Documents" in text or "iCloud" in text:
        return "OLD_ROOT_UNSAFE"
    if "/outputs/track_b_execution_core/" not in text:
        return "NON_RUNTIME_PATH_UNSAFE"
    return None


def _candle_path(*, config: Phase1RuntimeDataReadinessConfig, symbol: str, timeframe: str) -> Path:
    return Path(config.repo_root) / config.runtime_candle_root / symbol / timeframe / "latest_runtime_candles.json"


def _feature_path(*, config: Phase1RuntimeDataReadinessConfig, symbol: str, timeframe: str) -> Path:
    return Path(config.repo_root) / config.runtime_feature_root / symbol / timeframe / "latest_runtime_features.json"


def _first_reason(checks: dict[str, dict[str, Any]]) -> str:
    for check in checks.values():
        if not check.get("ready"):
            return str(check.get("reason") or "NOT_READY")
    return "NOT_READY"


def _load_json(path: Path) -> tuple[Any, str | None]:
    try:
        return json.loads(path.read_text(encoding="utf-8")), None
    except FileNotFoundError:
        return None, f"missing: {path}"
    except Exception as exc:
        return None, str(exc)


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coerce_now(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Phase-1 runtime candle/feature readiness artifacts.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = Phase1RuntimeDataReadinessConfig(repo_root=Path(args.repo_root), output_dir=Path(args.output_dir))
    artifacts = build_phase1_runtime_data_readiness(config=config)
    write_phase1_runtime_data_readiness_artifacts(config=config, artifacts=artifacts)
    print(json.dumps({"row_count": len(artifacts.rows), "ready_ticker_count": artifacts.report["ready_ticker_count"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
