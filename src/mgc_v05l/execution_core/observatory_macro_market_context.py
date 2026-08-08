"""Factual macro-market context producer for Observatory and research.

This module reads prepared Phase-1 completed-candle artifacts only. It does not
query brokers, start runtime services, classify regimes, or grant trading
authority.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SCHEMA_VERSION = "observatory_macro_market_context_v1"
OBSERVATION_SCHEMA_VERSION = "macro_market_observation_v1"
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PHASE1_CANDLE_ROOT = Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data"
DEFAULT_EQUITY_CANDLE_ROOT = Path("outputs") / "track_b_execution_core" / "observatory" / "equity_market_data"
DEFAULT_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "observatory"
    / "macro_market_context"
    / "latest_macro_market_context.json"
)


@dataclass(frozen=True)
class MacroSeriesSpec:
    canonical_id: str
    symbol: str
    display_name: str
    asset_class: str
    source_dataset: str | None
    publisher: str | None
    source_type: str
    timeframe: str = "1m"
    current_capture_status: str = "READY_FROM_PHASE1"
    source_limitation: str | None = None
    candle_root_kind: str = "PHASE1"


CORE_MACRO_UNIVERSE: tuple[MacroSeriesSpec, ...] = (
    MacroSeriesSpec("future.es", "ES", "E-mini S&P 500 futures", "equity_index_future", "GLBX.MDP3", "Databento", "FUTURE"),
    MacroSeriesSpec("future.nq", "NQ", "E-mini Nasdaq-100 futures", "equity_index_future", "GLBX.MDP3", "Databento", "FUTURE"),
    MacroSeriesSpec(
        "future.ym",
        "YM",
        "E-mini Dow futures",
        "equity_index_future",
        "GLBX.MDP3",
        "Databento",
        "FUTURE",
        current_capture_status="SUPPORTED_NOT_CAPTURED",
    ),
    MacroSeriesSpec(
        "future.rty",
        "RTY",
        "E-mini Russell 2000 futures",
        "equity_index_future",
        "GLBX.MDP3",
        "Databento",
        "FUTURE",
        current_capture_status="SUPPORTED_NOT_CAPTURED",
    ),
    MacroSeriesSpec(
        "cash_index.spx",
        "SPX",
        "S&P 500 Index",
        "equity_index",
        None,
        None,
        "CASH_INDEX",
        current_capture_status="SOURCE_GAP",
        source_limitation="No source-authoritative current cash-index feed is configured.",
    ),
    MacroSeriesSpec(
        "cash_index.ndx",
        "NDX",
        "Nasdaq-100 Index",
        "equity_index",
        None,
        None,
        "CASH_INDEX",
        current_capture_status="SOURCE_GAP",
        source_limitation="No source-authoritative current cash-index feed is configured.",
    ),
    MacroSeriesSpec(
        "cash_index.djia",
        "DJIA",
        "Dow Jones Industrial Average",
        "equity_index",
        None,
        None,
        "CASH_INDEX",
        current_capture_status="SOURCE_GAP",
        source_limitation="Do not substitute similarly named equities or ETFs for the cash index.",
    ),
    MacroSeriesSpec(
        "cash_index.rut",
        "RUT",
        "Russell 2000 Index",
        "equity_index",
        None,
        None,
        "CASH_INDEX",
        current_capture_status="SOURCE_GAP",
        source_limitation="No source-authoritative current cash-index feed is configured.",
    ),
    MacroSeriesSpec(
        "etf_proxy.spy",
        "SPY",
        "SPDR S&P 500 ETF proxy",
        "equity_etf_proxy",
        "EQUS.MINI",
        "Databento",
        "ETF_PROXY",
        current_capture_status="READY_FROM_EQUITY_PREPARED",
        source_limitation="ETF proxy only; not labeled or treated as the SPX cash index.",
        candle_root_kind="EQUITY",
    ),
    MacroSeriesSpec(
        "etf_proxy.qqq",
        "QQQ",
        "Invesco QQQ ETF proxy",
        "equity_etf_proxy",
        "EQUS.MINI",
        "Databento",
        "ETF_PROXY",
        current_capture_status="READY_FROM_EQUITY_PREPARED",
        source_limitation="ETF proxy only; not labeled or treated as the NDX cash index.",
        candle_root_kind="EQUITY",
    ),
    MacroSeriesSpec(
        "etf_proxy.dia",
        "DIA",
        "SPDR Dow Jones Industrial Average ETF proxy",
        "equity_etf_proxy",
        "EQUS.MINI",
        "Databento",
        "ETF_PROXY",
        current_capture_status="READY_FROM_EQUITY_PREPARED",
        source_limitation="ETF proxy only; not labeled or treated as the DJIA cash index.",
        candle_root_kind="EQUITY",
    ),
    MacroSeriesSpec(
        "etf_proxy.iwm",
        "IWM",
        "iShares Russell 2000 ETF proxy",
        "equity_etf_proxy",
        "EQUS.MINI",
        "Databento",
        "ETF_PROXY",
        current_capture_status="READY_FROM_EQUITY_PREPARED",
        source_limitation="ETF proxy only; not labeled or treated as the RUT cash index.",
        candle_root_kind="EQUITY",
    ),
    MacroSeriesSpec(
        "volatility_index.vix",
        "VIX",
        "Cboe Volatility Index",
        "volatility",
        None,
        None,
        "VOLATILITY_INDEX",
        current_capture_status="SOURCE_GAP",
        source_limitation="Spot VIX is not present in the current prepared Phase-1 feed.",
    ),
    MacroSeriesSpec("future.zb", "ZB", "30Y Treasury bond futures", "rates_future", "GLBX.MDP3", "Databento", "FUTURE"),
    MacroSeriesSpec("future.zn", "ZN", "10Y Treasury note futures", "rates_future", "GLBX.MDP3", "Databento", "FUTURE"),
    MacroSeriesSpec("future.zf", "ZF", "5Y Treasury note futures", "rates_future", "GLBX.MDP3", "Databento", "FUTURE"),
    MacroSeriesSpec("future.zt", "ZT", "2Y Treasury note futures", "rates_future", "GLBX.MDP3", "Databento", "FUTURE"),
    MacroSeriesSpec("future.cl", "CL", "WTI crude oil futures", "commodity_future", "GLBX.MDP3", "Databento", "FUTURE", current_capture_status="SUPPORTED_NOT_CAPTURED"),
    MacroSeriesSpec("future.gc", "GC", "Gold futures", "commodity_future", "GLBX.MDP3", "Databento", "FUTURE"),
    MacroSeriesSpec("future.hg", "HG", "Copper futures", "commodity_future", "GLBX.MDP3", "Databento", "FUTURE", current_capture_status="SUPPORTED_NOT_CAPTURED"),
    MacroSeriesSpec(
        "dollar_index.dxy",
        "DXY",
        "U.S. Dollar Index",
        "fx",
        None,
        None,
        "CASH_INDEX",
        current_capture_status="SOURCE_GAP",
        source_limitation="No direct DXY source is configured. FX futures must not be labeled DXY.",
    ),
    MacroSeriesSpec("future.6e", "6E", "Euro FX futures", "fx_future", "GLBX.MDP3", "Databento", "FUTURE", current_capture_status="SUPPORTED_NOT_CAPTURED"),
    MacroSeriesSpec("future.6j", "6J", "Japanese Yen futures", "fx_future", "GLBX.MDP3", "Databento", "FUTURE", current_capture_status="SUPPORTED_NOT_CAPTURED"),
    MacroSeriesSpec("future.6b", "6B", "British Pound futures", "fx_future", "GLBX.MDP3", "Databento", "FUTURE", current_capture_status="SUPPORTED_NOT_CAPTURED"),
    MacroSeriesSpec("future.6a", "6A", "Australian Dollar futures", "fx_future", "GLBX.MDP3", "Databento", "FUTURE", current_capture_status="SUPPORTED_NOT_CAPTURED"),
)


def build_macro_market_context(
    *,
    repo_root: Path = REPO_ROOT,
    generated_at: datetime | None = None,
    candle_root: Path = DEFAULT_PHASE1_CANDLE_ROOT,
    equity_candle_root: Path = DEFAULT_EQUITY_CANDLE_ROOT,
    universe: Sequence[MacroSeriesSpec] = CORE_MACRO_UNIVERSE,
) -> dict[str, Any]:
    now = _coerce_datetime(generated_at) or datetime.now(UTC)
    resolved_candle_root = _resolve(repo_root, candle_root)
    resolved_equity_candle_root = _resolve(repo_root, equity_candle_root)
    observations = [
        _build_observation(
            spec=spec,
            repo_root=repo_root,
            candle_root=resolved_equity_candle_root if spec.candle_root_kind == "EQUITY" else resolved_candle_root,
            generated_at=now,
        )
        for spec in universe
    ]
    available_count = sum(1 for row in observations if row["status"] == "AVAILABLE")
    stale_count = sum(1 for row in observations if row["status"] == "STALE")
    missing_count = sum(1 for row in observations if row["status"] in {"MISSING_SOURCE", "SUPPORTED_NOT_CAPTURED", "SOURCE_GAP"})
    status = "READY" if available_count else "NOT_READY"
    if stale_count:
        status = "VALID_WITH_WARNINGS"
    elif missing_count:
        status = "VALID_WITH_WARNINGS" if available_count else "NOT_READY"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now.isoformat(),
        "producer": "observatory_macro_market_context",
        "status": status,
        "source_contract": "PREPARED_COMPLETED_CANDLES_ONLY",
        "observations": observations,
        "summary": {
            "series_count": len(observations),
            "available_count": available_count,
            "stale_count": stale_count,
            "missing_or_unsupported_count": missing_count,
            "cash_index_source_gaps": [
                row["canonical_id"]
                for row in observations
                if row["source_type"] in {"CASH_INDEX", "VOLATILITY_INDEX"} and row["status"] == "SOURCE_GAP"
            ],
        },
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


def write_macro_market_context(snapshot: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output_path


def _build_observation(
    *,
    spec: MacroSeriesSpec,
    repo_root: Path,
    candle_root: Path,
    generated_at: datetime,
) -> dict[str, Any]:
    base = {
        "schema_version": OBSERVATION_SCHEMA_VERSION,
        "canonical_id": spec.canonical_id,
        "symbol": spec.symbol,
        "display_name": spec.display_name,
        "asset_class": spec.asset_class,
        "source_dataset": spec.source_dataset,
        "publisher": spec.publisher,
        "source_type": spec.source_type,
        "timeframe": spec.timeframe,
        "generated_at": generated_at.isoformat(),
        "guardrails": {
            "display_only": True,
            "trading_input": False,
        },
    }
    if spec.current_capture_status not in {"READY_FROM_PHASE1", "READY_FROM_EQUITY_PREPARED"}:
        return {
            **base,
            "status": spec.current_capture_status,
            "value": None,
            "last": None,
            "change_abs": None,
            "change_pct": None,
            "source_timestamp": None,
            "freshness": "UNAVAILABLE",
            "market_status": "UNKNOWN",
            "source_artifact": None,
            "source_limitation": spec.source_limitation,
        }

    path = candle_root / spec.symbol / spec.timeframe / "latest_runtime_candles.json"
    source_artifact = _display_path(repo_root, path)
    if not path.exists():
        return {
            **base,
            "status": "MISSING_SOURCE",
            "value": None,
            "last": None,
            "change_abs": None,
            "change_pct": None,
            "source_timestamp": None,
            "freshness": "MISSING",
            "market_status": "UNKNOWN",
            "source_artifact": source_artifact,
            "source_limitation": "Prepared Phase-1 candle artifact is not present.",
        }
    payload = json.loads(path.read_text(encoding="utf-8"))
    bars = [row for row in payload.get("bars", []) if isinstance(row, Mapping) and row.get("completed", True)]
    if not bars:
        return {
            **base,
            "status": "MISSING_SOURCE",
            "value": None,
            "last": None,
            "change_abs": None,
            "change_pct": None,
            "source_timestamp": None,
            "freshness": "MISSING",
            "market_status": "UNKNOWN",
            "source_artifact": source_artifact,
            "source_limitation": "Prepared Phase-1 candle artifact has no completed bars.",
        }
    latest = bars[-1]
    previous = bars[-2] if len(bars) > 1 else None
    latest_close = _to_float(latest.get("close"))
    previous_close = _to_float(previous.get("close")) if previous else None
    source_timestamp = latest.get("bar_end") or latest.get("timestamp")
    source_dt = _parse_datetime(source_timestamp)
    max_age = _to_float(payload.get("latest_bar_freshness_seconds")) or _to_float(payload.get("freshness_seconds")) or 180.0
    age_seconds = None if source_dt is None else max(0.0, (generated_at - source_dt).total_seconds())
    freshness = "UNKNOWN"
    if age_seconds is not None:
        freshness = "FRESH" if age_seconds <= max_age else "STALE"
    change_abs = None
    change_pct = None
    if latest_close is not None and previous_close not in (None, 0.0):
        change_abs = round(latest_close - previous_close, 10)
        change_pct = round((change_abs / previous_close) * 100.0, 8)
    return {
        **base,
        "status": "AVAILABLE" if freshness != "STALE" else "STALE",
        "value": latest_close,
        "last": latest_close,
        "session_reference": "PREVIOUS_COMPLETED_BAR",
        "change_abs": change_abs,
        "change_pct": change_pct,
        "source_timestamp": None if source_dt is None else source_dt.isoformat(),
        "generated_source_timestamp": payload.get("generated_at"),
        "freshness": freshness,
        "age_seconds": None if age_seconds is None else round(age_seconds, 3),
        "market_status": "CURRENT" if freshness == "FRESH" else freshness,
        "source_artifact": source_artifact,
        "source_limitation": None,
        "source_provenance": {
            "source_id": payload.get("source_id"),
            "dataset": payload.get("dataset"),
            "schema": payload.get("schema"),
            "stype_in": payload.get("stype_in"),
            "request_symbol": payload.get("request_symbol"),
            "completed_candles_only": payload.get("completed_candles_only"),
            "realtime_feed_confirmed": payload.get("realtime_feed_confirmed"),
        },
    }


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


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fingerprint(payload: Mapping[str, Any]) -> str:
    stable = {key: value for key, value in payload.items() if key != "deterministic_fingerprint"}
    encoded = json.dumps(stable, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build factual Observatory macro-market context from prepared candles.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args(list(argv) if argv is not None else None)

    snapshot = build_macro_market_context(repo_root=args.repo_root)
    output_path = _resolve(args.repo_root, args.output_json)
    write_macro_market_context(snapshot, output_path)
    print(json.dumps({"status": snapshot["status"], "output_json": str(output_path), "available_count": snapshot["summary"]["available_count"]}, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
