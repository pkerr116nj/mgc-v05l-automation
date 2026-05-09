"""Build a read-only Phase-1 futures ticker readiness matrix."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mgc_v05l.execution.ibkr_paper_strategy_porting import submit_capable_lane_adapters
from mgc_v05l.execution.ibkr_phase1_futures_scope import (
    phase1_execution_symbol_for_source,
    phase1_execution_target_for_source,
    supported_phase1_source_instruments,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "phase1_ticker_readiness_matrix"
DEFAULT_MARKET_DATA_CONFIG_PATH = Path("config") / "market_data_providers.json"
DEFAULT_GOVERNANCE_STATUS_PATH = Path("var") / "per_strategy_paper_status.json"
DEFAULT_RUNTIME_CANDLE_DIR = Path("outputs") / "track_b_execution_core" / "track_b_runtime_candle_capture"
DEFAULT_FEATURE_STATE_DIR = Path("outputs") / "track_b_execution_core" / "track_b_runtime_feature_state"
PHASE1_TICKER_ORDER = ("GC", "NQ", "ES", "MGC", "MNQ", "MES", "ZT", "ZF", "ZN", "ZB")
FULL_SIZE_CONTRACTS = {"GC", "NQ", "ES"}
MICRO_CONTRACTS = {"MGC", "MNQ", "MES"}
RATES_CONTRACTS = {"ZT", "ZF", "ZN", "ZB"}
QUANTITY_CAP = 1.0


@dataclass(frozen=True)
class Phase1TickerReadinessMatrixConfig:
    repo_root: Path = REPO_ROOT
    output_dir: Path = DEFAULT_OUTPUT_DIR
    market_data_config_path: Path = DEFAULT_MARKET_DATA_CONFIG_PATH
    governance_status_path: Path = DEFAULT_GOVERNANCE_STATUS_PATH
    runtime_candle_dir: Path = DEFAULT_RUNTIME_CANDLE_DIR
    feature_state_dir: Path = DEFAULT_FEATURE_STATE_DIR


@dataclass(frozen=True)
class Phase1TickerReadinessMatrixArtifacts:
    report: dict[str, Any]
    rows: list[dict[str, Any]]


def build_phase1_ticker_readiness_matrix(
    *,
    config: Phase1TickerReadinessMatrixConfig,
) -> Phase1TickerReadinessMatrixArtifacts:
    repo_root = Path(config.repo_root)
    market_data = _load_json(repo_root / config.market_data_config_path, default={})
    governance = _load_json(repo_root / config.governance_status_path, default={})
    governance_rows = list(governance.get("strategies") or [])
    adapters = submit_capable_lane_adapters()
    rows = [_ticker_row(symbol, repo_root=repo_root, config=config, market_data=market_data, governance_rows=governance_rows, adapters=adapters) for symbol in PHASE1_TICKER_ORDER]
    report = {
        "schema_version": "phase1_ticker_readiness_matrix_v1",
        "generated_at": _utc_now(),
        "repo_root": str(repo_root),
        "archive_artifact_used": False,
        "runtime_truth_policy": "active runtime/preflight/dashboard must not read cold archive as operational truth",
        "approved_phase1_tickers": list(PHASE1_TICKER_ORDER),
        "row_count": len(rows),
        "can_submit_count": sum(1 for row in rows if row["can_submit"]),
        "rows": rows,
    }
    return Phase1TickerReadinessMatrixArtifacts(report=report, rows=rows)


def write_phase1_ticker_readiness_matrix_artifacts(
    *,
    config: Phase1TickerReadinessMatrixConfig,
    artifacts: Phase1TickerReadinessMatrixArtifacts,
) -> None:
    output_dir = Path(config.output_dir)
    if not output_dir.is_absolute():
        output_dir = Path(config.repo_root) / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "latest_phase1_ticker_readiness_matrix.json").write_text(
        json.dumps(artifacts.report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write_csv(output_dir / "latest_phase1_ticker_readiness_matrix.csv", artifacts.rows)
    (output_dir / "latest_phase1_ticker_readiness_matrix.md").write_text(
        render_phase1_ticker_readiness_matrix_markdown(artifacts.report) + "\n",
        encoding="utf-8",
    )


def render_phase1_ticker_readiness_matrix_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Phase-1 Ticker Readiness Matrix",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- row_count: `{report.get('row_count')}`",
        f"- can_submit_count: `{report.get('can_submit_count')}`",
        f"- archive_artifact_used: `{report.get('archive_artifact_used')}`",
        "",
        "| symbol | target | metadata | market data | candles | features | governance | adapter | strategy approved | can submit | block reason |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in list(report.get("rows") or []):
        lines.append(
            "| {symbol} | {target} | {metadata} | {market} | {candles} | {features} | {governance} | {adapter} | {approved} | {submit} | {blocker} |".format(
                symbol=row["approved_phase1_symbol"],
                target=row["executable_target_symbol"] or "",
                metadata=_yn(row["contract_metadata_present"]),
                market=_yn(row["market_data_ready"]),
                candles=_yn(row["runtime_candles_ready"]),
                features=_yn(row["derived_features_ready"]),
                governance=_yn(row["governance_visible"]),
                adapter=_yn(row["lane_adapter_present"]),
                approved=_yn(row["strategy_approved"]),
                submit=_yn(row["can_submit"]),
                blocker=row["block_reason"],
            )
        )
    return "\n".join(lines)


def _ticker_row(
    symbol: str,
    *,
    repo_root: Path,
    config: Phase1TickerReadinessMatrixConfig,
    market_data: dict[str, Any],
    governance_rows: list[dict[str, Any]],
    adapters: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    target = phase1_execution_target_for_source(symbol)
    source_supported = symbol in supported_phase1_source_instruments()
    executable_target = phase1_execution_symbol_for_source(symbol)
    metadata_present = _contract_metadata_present(target)
    qualification_ready = bool(metadata_present and target)
    market_data_ready = _market_data_ready(symbol=symbol, market_data=market_data)
    runtime_candles_ready = _runtime_candles_ready(symbol=symbol, repo_root=repo_root, config=config)
    derived_features_ready = _derived_features_ready(symbol=symbol, repo_root=repo_root, config=config)
    governance_visible = any(str(row.get("instrument") or "").upper() == symbol for row in governance_rows)
    lane_adapter_present = any(
        str(dict(adapter.get("bridge_execution_target") or {}).get("symbol") or "").upper() == symbol
        for adapter in adapters.values()
    )
    strategy_approved = any(
        str(row.get("instrument") or "").upper() == symbol
        and bool(row.get("strategy_approved") or row.get("paper_strategy_approved") or row.get("approved_phase1_strategy"))
        for row in governance_rows
    )
    can_submit, block_reason = _submit_status(
        contract_metadata_present=metadata_present,
        lane_adapter_present=lane_adapter_present,
        strategy_approved=strategy_approved,
    )
    return {
        "approved_phase1_symbol": symbol,
        "source_symbol_supported": bool(source_supported),
        "executable_target_supported": executable_target is not None and target is not None,
        "executable_target_symbol": executable_target,
        "contract_metadata_present": bool(metadata_present),
        "contract_qualification_ready": qualification_ready,
        "market_data_ready": market_data_ready,
        "runtime_candles_ready": runtime_candles_ready,
        "derived_features_ready": derived_features_ready,
        "governance_visible": governance_visible,
        "lane_adapter_present": lane_adapter_present,
        "paper_route_capable": bool(lane_adapter_present and metadata_present),
        "strategy_approved": strategy_approved,
        "can_submit": can_submit,
        "block_reason": block_reason,
        "live_money_eligible": False,
        "quantity_cap": QUANTITY_CAP,
        "full_size_contract": symbol in FULL_SIZE_CONTRACTS,
        "micro_contract": symbol in MICRO_CONTRACTS,
        "rates_contract": symbol in RATES_CONTRACTS,
        "exchange": str((target or {}).get("exchange") or ""),
        "contract_month": str((target or {}).get("contract_month") or ""),
        "multiplier": str((target or {}).get("multiplier") or ""),
    }


def _submit_status(
    *,
    contract_metadata_present: bool,
    lane_adapter_present: bool,
    strategy_approved: bool,
) -> tuple[bool, str]:
    if not contract_metadata_present:
        return False, "CONTRACT_METADATA_MISSING"
    if not lane_adapter_present:
        return False, "LANE_ADAPTER_MISSING"
    if not strategy_approved:
        return False, "NO_APPROVED_STRATEGY"
    return True, "READY"


def _contract_metadata_present(target: dict[str, Any] | None) -> bool:
    if not target:
        return False
    required = ("symbol", "contract_month", "exchange", "currency", "multiplier", "trading_class")
    return all(bool(str(target.get(key) or "").strip()) for key in required)


def _market_data_ready(*, symbol: str, market_data: dict[str, Any]) -> bool:
    pilot_symbols = (((market_data.get("databento") or {}).get("pilot_symbols")) or {})
    row = dict(pilot_symbols.get(symbol) or {})
    return bool(row.get("request_symbol") and (row.get("schema_by_timeframe") or {}).get("1m"))


def _runtime_candles_ready(*, symbol: str, repo_root: Path, config: Phase1TickerReadinessMatrixConfig) -> bool:
    root = repo_root / config.runtime_candle_dir
    candidates = (
        root / f"latest_runtime_{symbol.lower()}_1m_candles.json",
        root / f"latest_runtime_{symbol}_1m_candles.json",
    )
    return any(path.exists() for path in candidates)


def _derived_features_ready(*, symbol: str, repo_root: Path, config: Phase1TickerReadinessMatrixConfig) -> bool:
    root = repo_root / config.feature_state_dir
    candidates = (
        root / f"latest_runtime_{symbol.lower()}_features.json",
        root / f"latest_runtime_{symbol}_features.json",
    )
    return any(path.exists() for path in candidates)


def _load_json(path: Path, *, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _yn(value: Any) -> str:
    return "yes" if bool(value) else "no"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the read-only Phase-1 ticker readiness matrix.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = Phase1TickerReadinessMatrixConfig(
        repo_root=Path(args.repo_root),
        output_dir=Path(args.output_dir),
    )
    artifacts = build_phase1_ticker_readiness_matrix(config=config)
    write_phase1_ticker_readiness_matrix_artifacts(config=config, artifacts=artifacts)
    print(json.dumps({"row_count": len(artifacts.rows), "can_submit_count": artifacts.report["can_submit_count"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
