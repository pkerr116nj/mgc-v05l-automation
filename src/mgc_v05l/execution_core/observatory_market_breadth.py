"""Factual US-equity breadth producer for Observatory and future regimes."""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SCHEMA_VERSION = "observatory_market_breadth_v1"
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_EQUITY_CANDLE_ROOT = Path("outputs") / "track_b_execution_core" / "observatory" / "equity_market_data"
DEFAULT_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "observatory"
    / "market_breadth"
    / "latest_market_breadth.json"
)
DEFAULT_UNIVERSE_ID = "US_EQUITY_CORE_ETF_PROXY_V1"


@dataclass(frozen=True)
class BreadthSymbolSpec:
    symbol: str
    display_name: str
    benchmark_context: str
    source_dataset: str = "EQUS.MINI"
    source_type: str = "ETF"
    timeframe: str = "1m"


DEFAULT_BREADTH_UNIVERSE: tuple[BreadthSymbolSpec, ...] = (
    BreadthSymbolSpec("SPY", "SPDR S&P 500 ETF", "SPX_PROXY"),
    BreadthSymbolSpec("QQQ", "Invesco QQQ Trust", "NDX_PROXY"),
    BreadthSymbolSpec("DIA", "SPDR Dow Jones Industrial Average ETF", "DJIA_PROXY"),
    BreadthSymbolSpec("IWM", "iShares Russell 2000 ETF", "RUT_PROXY"),
)


def build_market_breadth(
    *,
    repo_root: Path = REPO_ROOT,
    generated_at: datetime | None = None,
    equity_candle_root: Path = DEFAULT_EQUITY_CANDLE_ROOT,
    universe_id: str = DEFAULT_UNIVERSE_ID,
    universe: Sequence[BreadthSymbolSpec] = DEFAULT_BREADTH_UNIVERSE,
) -> dict[str, Any]:
    now = _coerce_datetime(generated_at) or datetime.now(UTC)
    root = _resolve(repo_root, equity_candle_root)
    rows = [_symbol_row(repo_root=repo_root, candle_root=root, spec=spec, generated_at=now) for spec in universe]
    valid = [row for row in rows if row["status"] == "VALID"]
    returns = [row["return_pct"] for row in valid if isinstance(row.get("return_pct"), (int, float))]
    advancing = sum(1 for value in returns if value > 0)
    declining = sum(1 for value in returns if value < 0)
    unchanged = sum(1 for value in returns if value == 0)
    eligible_count = len(rows)
    valid_count = len(valid)
    coverage_ratio = round(valid_count / eligible_count, 6) if eligible_count else 0.0
    status = "READY" if valid_count else "NOT_READY"
    if valid_count and valid_count < eligible_count:
        status = "VALID_WITH_WARNINGS"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now.isoformat(),
        "producer": "observatory_market_breadth",
        "status": status,
        "metric_kind": "FACTUAL_BREADTH_METRIC",
        "universe_id": universe_id,
        "source_dataset": "EQUS.MINI",
        "timeframe": "1m",
        "eligible_symbol_count": eligible_count,
        "valid_symbol_count": valid_count,
        "advancing_count": advancing,
        "declining_count": declining,
        "unchanged_count": unchanged,
        "percent_advancing": _percent(advancing, valid_count),
        "percent_declining": _percent(declining, valid_count),
        "median_return": None if not returns else round(statistics.median(returns), 8),
        "equal_weight_mean_return": None if not returns else round(sum(returns) / len(returns), 8),
        "coverage_ratio": coverage_ratio,
        "source_timestamp": _latest_timestamp(valid),
        "freshness": "FRESH" if valid and all(row.get("freshness") == "FRESH" for row in valid) else "UNKNOWN" if not valid else "STALE_OR_MIXED",
        "symbols": rows,
        "guardrails": {
            "display_only": True,
            "trading_input": False,
            "broker_authority": False,
            "runtime_authority": False,
            "strategy_input": False,
            "regime_classification": False,
            "risk_on_risk_off": False,
        },
    }
    payload["deterministic_fingerprint"] = _fingerprint(payload)
    return payload


def write_market_breadth(snapshot: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output_path


def _symbol_row(*, repo_root: Path, candle_root: Path, spec: BreadthSymbolSpec, generated_at: datetime) -> dict[str, Any]:
    path = candle_root / spec.symbol / spec.timeframe / "latest_runtime_candles.json"
    base = {
        "symbol": spec.symbol,
        "display_name": spec.display_name,
        "benchmark_context": spec.benchmark_context,
        "source_dataset": spec.source_dataset,
        "source_type": spec.source_type,
        "timeframe": spec.timeframe,
        "source_artifact": _display_path(repo_root, path),
    }
    if not path.exists():
        return {**base, "status": "MISSING_SOURCE", "last": None, "return_pct": None, "source_timestamp": None, "freshness": "MISSING"}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {**base, "status": "INVALID_SOURCE", "last": None, "return_pct": None, "source_timestamp": None, "freshness": "INVALID", "error": str(exc)}
    bars = [row for row in payload.get("bars", []) if isinstance(row, Mapping) and row.get("completed", True)]
    if len(bars) < 2:
        return {**base, "status": "INSUFFICIENT_BARS", "last": None, "return_pct": None, "source_timestamp": None, "freshness": "MISSING"}
    latest = bars[-1]
    previous = bars[-2]
    latest_close = _float(latest.get("close"))
    previous_close = _float(previous.get("close"))
    timestamp = latest.get("bar_end") or latest.get("timestamp")
    source_dt = _parse_datetime(timestamp)
    freshness_dt = _parse_datetime(payload.get("source_window_end")) or source_dt
    max_age = _float(payload.get("latest_bar_freshness_seconds")) or 180.0
    age_seconds = None if freshness_dt is None else max(0.0, (generated_at - freshness_dt).total_seconds())
    freshness = "UNKNOWN" if age_seconds is None else "FRESH" if age_seconds <= max_age else "STALE"
    if latest_close is None or previous_close in (None, 0.0):
        return {**base, "status": "INVALID_PRICE", "last": latest_close, "return_pct": None, "source_timestamp": timestamp, "freshness": freshness}
    return {
        **base,
        "status": "VALID" if freshness == "FRESH" else "STALE",
        "last": latest_close,
        "return_pct": round(((latest_close - previous_close) / previous_close) * 100.0, 8),
        "source_timestamp": None if source_dt is None else source_dt.isoformat(),
        "freshness": freshness,
        "age_seconds": None if age_seconds is None else round(age_seconds, 3),
    }


def _latest_timestamp(rows: Sequence[Mapping[str, Any]]) -> str | None:
    values = sorted(str(row.get("source_timestamp")) for row in rows if row.get("source_timestamp"))
    return values[-1] if values else None


def _percent(count: int, total: int) -> float | None:
    if total <= 0:
        return None
    return round((count / total) * 100.0, 6)


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _display_path(repo_root: Path, path: Path) -> str:
    try:
        return str(path.relative_to(repo_root))
    except ValueError:
        return str(path)


def _coerce_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _coerce_datetime(parsed)


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fingerprint(payload: Mapping[str, Any]) -> str:
    stable = {key: value for key, value in payload.items() if key != "deterministic_fingerprint"}
    return hashlib.sha256(json.dumps(stable, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build factual Observatory US-equity breadth from prepared candles.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args(list(argv) if argv is not None else None)
    snapshot = build_market_breadth(repo_root=args.repo_root)
    output_path = _resolve(args.repo_root, args.output_json)
    write_market_breadth(snapshot, output_path)
    print(json.dumps({"status": snapshot["status"], "output_json": str(output_path), "valid_symbol_count": snapshot["valid_symbol_count"]}, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
