from __future__ import annotations

import ast
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.observatory_macro_market_context import (
    CORE_MACRO_UNIVERSE,
    SCHEMA_VERSION,
    MacroSeriesSpec,
    build_macro_market_context,
    write_macro_market_context,
)


BASE_TS = datetime(2026, 8, 7, 19, 0, tzinfo=UTC)


def test_builds_factual_macro_observation_from_phase1_candles(tmp_path: Path) -> None:
    _write_candles(tmp_path, "ES", closes=(5000.0, 5010.0), generated_at=BASE_TS)

    snapshot = build_macro_market_context(
        repo_root=tmp_path,
        generated_at=BASE_TS,
        universe=(MacroSeriesSpec("future.es", "ES", "ES futures", "equity_index_future", "GLBX.MDP3", "Databento", "FUTURE"),),
    )

    row = snapshot["observations"][0]
    assert snapshot["schema_version"] == SCHEMA_VERSION
    assert snapshot["status"] == "READY"
    assert row["status"] == "AVAILABLE"
    assert row["value"] == 5010.0
    assert row["session_reference"] == "PREVIOUS_COMPLETED_BAR"
    assert row["change_abs"] == 10.0
    assert row["change_pct"] == 0.2
    assert row["source_type"] == "FUTURE"
    assert row["source_provenance"]["completed_candles_only"] is True
    assert snapshot["guardrails"]["display_only"] is True
    assert snapshot["guardrails"]["risk_on_risk_off"] is False


def test_supported_but_uncaptured_series_are_not_fabricated(tmp_path: Path) -> None:
    spec = MacroSeriesSpec(
        "future.ym",
        "YM",
        "YM futures",
        "equity_index_future",
        "GLBX.MDP3",
        "Databento",
        "FUTURE",
        current_capture_status="SUPPORTED_NOT_CAPTURED",
    )

    snapshot = build_macro_market_context(repo_root=tmp_path, generated_at=BASE_TS, universe=(spec,))

    row = snapshot["observations"][0]
    assert row["status"] == "SUPPORTED_NOT_CAPTURED"
    assert row["value"] is None
    assert row["change_pct"] is None
    assert row["market_status"] == "UNKNOWN"


def test_cash_index_source_gaps_are_explicit(tmp_path: Path) -> None:
    spec = MacroSeriesSpec(
        "cash_index.spx",
        "SPX",
        "S&P 500 Index",
        "equity_index",
        None,
        None,
        "CASH_INDEX",
        current_capture_status="SOURCE_GAP",
        source_limitation="No source-authoritative current cash-index feed is configured.",
    )

    snapshot = build_macro_market_context(repo_root=tmp_path, generated_at=BASE_TS, universe=(spec,))

    assert snapshot["status"] == "NOT_READY"
    row = snapshot["observations"][0]
    assert row["status"] == "SOURCE_GAP"
    assert row["source_type"] == "CASH_INDEX"
    assert "cash_index.spx" in snapshot["summary"]["cash_index_source_gaps"]


def test_etf_proxies_are_labeled_separately_from_cash_indexes(tmp_path: Path) -> None:
    _write_candles(
        tmp_path,
        "SPY",
        closes=(500.0, 501.0),
        generated_at=BASE_TS,
        root_kind="equity_market_data",
        dataset="EQUS.MINI",
        source_id="DATABENTO_EQUS_MINI_OBSERVATORY_PREPARED_CANDLES",
    )
    universe = (
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
    )

    snapshot = build_macro_market_context(repo_root=tmp_path, generated_at=BASE_TS, universe=universe)

    by_id = {row["canonical_id"]: row for row in snapshot["observations"]}
    assert by_id["cash_index.spx"]["status"] == "SOURCE_GAP"
    assert by_id["etf_proxy.spy"]["status"] == "AVAILABLE"
    assert by_id["etf_proxy.spy"]["source_type"] == "ETF_PROXY"
    assert by_id["etf_proxy.spy"]["source_dataset"] == "EQUS.MINI"
    assert "cash_index.spx" in snapshot["summary"]["cash_index_source_gaps"]


def test_stale_candles_are_classified_without_inference(tmp_path: Path) -> None:
    old = BASE_TS - timedelta(minutes=10)
    _write_candles(tmp_path, "NQ", closes=(100.0, 99.0), generated_at=old, latest_bar_freshness_seconds=180)

    snapshot = build_macro_market_context(
        repo_root=tmp_path,
        generated_at=BASE_TS,
        universe=(MacroSeriesSpec("future.nq", "NQ", "NQ futures", "equity_index_future", "GLBX.MDP3", "Databento", "FUTURE"),),
    )

    row = snapshot["observations"][0]
    assert snapshot["status"] == "VALID_WITH_WARNINGS"
    assert row["status"] == "STALE"
    assert row["freshness"] == "STALE"
    assert row["change_pct"] == -1.0


def test_default_universe_keeps_treasury_futures_distinct_from_yields() -> None:
    by_id = {spec.canonical_id: spec for spec in CORE_MACRO_UNIVERSE}

    assert by_id["future.zn"].source_type == "FUTURE"
    assert "yield" not in by_id["future.zn"].display_name.lower()
    assert by_id["dollar_index.dxy"].current_capture_status == "SOURCE_GAP"


def test_fingerprints_are_deterministic(tmp_path: Path) -> None:
    _write_candles(tmp_path, "ES", closes=(5000.0, 5010.0), generated_at=BASE_TS)
    universe = (MacroSeriesSpec("future.es", "ES", "ES futures", "equity_index_future", "GLBX.MDP3", "Databento", "FUTURE"),)

    first = build_macro_market_context(repo_root=tmp_path, generated_at=BASE_TS, universe=universe)
    second = build_macro_market_context(repo_root=tmp_path, generated_at=BASE_TS, universe=universe)

    assert first["deterministic_fingerprint"] == second["deterministic_fingerprint"]


def test_json_output_round_trip(tmp_path: Path) -> None:
    snapshot = build_macro_market_context(repo_root=tmp_path, generated_at=BASE_TS, universe=())
    output = tmp_path / "latest_macro_market_context.json"

    write_macro_market_context(snapshot, output)

    parsed = json.loads(output.read_text(encoding="utf-8"))
    assert parsed["schema_version"] == SCHEMA_VERSION
    assert parsed["guardrails"]["broker_authority"] is False


def test_no_broker_runtime_mutation_imports_or_calls() -> None:
    path = Path("src/mgc_v05l/execution_core/observatory_macro_market_context.py")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    forbidden_import_roots = (
        "mgc_v05l.execution.",
        "mgc_v05l.strategy",
        "mgc_v05l.brokers",
        "ibapi",
    )
    forbidden_call_names = {"submit", "cancel", "close", "placeOrder", "flatten", "computeSafeState", "computeReadiness"}
    violations: list[str] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "mgc_v05l.execution" or alias.name.startswith(forbidden_import_roots):
                    violations.append(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            if node.module == "mgc_v05l.execution" or node.module.startswith(forbidden_import_roots):
                violations.append(node.module)
        elif isinstance(node, ast.Call):
            call_name: str | None = None
            if isinstance(node.func, ast.Name):
                call_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                call_name = node.func.attr
            if call_name in forbidden_call_names:
                violations.append(call_name)

    assert violations == []


def _write_candles(
    root: Path,
    symbol: str,
    *,
    closes: tuple[float, float],
    generated_at: datetime,
    latest_bar_freshness_seconds: int = 180,
    root_kind: str = "phase1_runtime_market_data",
    dataset: str = "GLBX.MDP3",
    source_id: str = "DATABENTO_REALTIME_PHASE1",
) -> None:
    first_end = generated_at - timedelta(minutes=2)
    second_end = generated_at - timedelta(minutes=1)
    payload = {
        "bars": [
            {
                "bar_start": (first_end - timedelta(minutes=1)).isoformat(),
                "bar_end": first_end.isoformat(),
                "open": closes[0],
                "high": closes[0],
                "low": closes[0],
                "close": closes[0],
                "volume": 10,
                "completed": True,
            },
            {
                "bar_start": (second_end - timedelta(minutes=1)).isoformat(),
                "bar_end": second_end.isoformat(),
                "open": closes[1],
                "high": closes[1],
                "low": closes[1],
                "close": closes[1],
                "volume": 12,
                "completed": True,
            },
        ],
        "completed_candles_only": True,
        "dataset": dataset,
        "generated_at": generated_at.isoformat(),
        "latest_bar_freshness_seconds": latest_bar_freshness_seconds,
        "request_symbol": f"{symbol}.v.0",
        "schema": "ohlcv-1m",
        "source_id": source_id,
        "stype_in": "continuous",
        "realtime_feed_confirmed": True,
    }
    path = root / "outputs" / "track_b_execution_core" / "observatory" / root_kind / symbol / "1m" / "latest_runtime_candles.json"
    if root_kind == "phase1_runtime_market_data":
        path = root / "outputs" / "track_b_execution_core" / root_kind / symbol / "1m" / "latest_runtime_candles.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
