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
from mgc_v05l.execution_core.phase1_runtime_data_readiness import (
    DEFAULT_RUNTIME_CANDLE_ROOT,
    DEFAULT_RUNTIME_FEATURE_ROOT,
    Phase1RuntimeDataReadinessConfig,
    build_phase1_runtime_data_readiness,
)
from mgc_v05l.execution_core.phase1_gc_paper_candidate import (
    CHOSEN_GC_STRATEGY_ID,
    Phase1GcCandidateConfig,
    build_phase1_gc_paper_candidate,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "phase1_ticker_readiness_matrix"
DEFAULT_MARKET_DATA_CONFIG_PATH = Path("config") / "market_data_providers.json"
DEFAULT_GOVERNANCE_STATUS_PATH = Path("var") / "per_strategy_paper_status.json"
DEFAULT_RUNTIME_CANDLE_DIR = DEFAULT_RUNTIME_CANDLE_ROOT
DEFAULT_FEATURE_STATE_DIR = DEFAULT_RUNTIME_FEATURE_ROOT
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
    now: datetime | None = None


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
    runtime_data = build_phase1_runtime_data_readiness(
        config=Phase1RuntimeDataReadinessConfig(
            repo_root=repo_root,
            runtime_candle_root=config.runtime_candle_dir,
            runtime_feature_root=config.feature_state_dir,
            now=config.now,
        )
    )
    runtime_data_by_symbol = {str(row.get("symbol") or ""): row for row in runtime_data.rows}
    gc_candidate_surface = _gc_candidate_surface(
        config=config,
        governance_rows=governance_rows,
    )
    rows = [
        _ticker_row(
            symbol,
            market_data=market_data,
            governance_rows=governance_rows,
            adapters=adapters,
            runtime_data_by_symbol=runtime_data_by_symbol,
            gc_candidate_surface=gc_candidate_surface,
        )
        for symbol in PHASE1_TICKER_ORDER
    ]
    report = {
        "schema_version": "phase1_ticker_readiness_matrix_v1",
        "generated_at": _utc_now(),
        "repo_root": str(repo_root),
        "archive_artifact_used": False,
        "research_artifact_used": bool(runtime_data.report.get("research_artifact_used")),
        "runtime_truth_policy": "active runtime/preflight/dashboard must not read cold archive as operational truth",
        "runtime_data_readiness_schema_version": runtime_data.report.get("schema_version"),
        "approved_phase1_tickers": list(PHASE1_TICKER_ORDER),
        "row_count": len(rows),
        "can_submit_count": sum(1 for row in rows if row["can_submit"]),
        "paper_candidate_visible_count": sum(1 for row in rows if row["paper_candidate_visible"]),
        "paper_watch_ready_count": sum(1 for row in rows if row["paper_watch_ready"]),
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
        "| symbol | target | candidate | paper watch | metadata | market data | candles | features | governance | adapter | strategy approved | can submit | block reason |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in list(report.get("rows") or []):
        lines.append(
            "| {symbol} | {target} | {candidate} | {watch} | {metadata} | {market} | {candles} | {features} | {governance} | {adapter} | {approved} | {submit} | {blocker} |".format(
                symbol=row["approved_phase1_symbol"],
                target=row["executable_target_symbol"] or "",
                candidate=_yn(row["paper_candidate_visible"]),
                watch=_yn(row["paper_watch_ready"]),
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
    market_data: dict[str, Any],
    governance_rows: list[dict[str, Any]],
    adapters: dict[str, dict[str, Any]],
    runtime_data_by_symbol: dict[str, dict[str, Any]],
    gc_candidate_surface: dict[str, Any],
) -> dict[str, Any]:
    target = phase1_execution_target_for_source(symbol)
    source_supported = symbol in supported_phase1_source_instruments()
    executable_target = phase1_execution_symbol_for_source(symbol)
    metadata_present = _contract_metadata_present(target)
    qualification_ready = bool(metadata_present and target)
    market_data_ready = _market_data_ready(symbol=symbol, market_data=market_data)
    runtime_data = dict(runtime_data_by_symbol.get(symbol) or {})
    historical_seed_ready = bool(runtime_data.get("historical_seed_ready"))
    realtime_feed_confirmed = bool(runtime_data.get("realtime_feed_confirmed"))
    runtime_candles_ready = bool(runtime_data.get("runtime_candles_ready"))
    derived_features_ready = bool(runtime_data.get("derived_features_ready"))
    runtime_data_block_reason = _runtime_data_block_reason(runtime_data)
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
    guarded_route_authorized = any(
        str(row.get("instrument") or "").upper() == symbol
        and bool(row.get("submit_allowed"))
        and bool(row.get("strategy_approved") or row.get("paper_strategy_approved") or row.get("approved_phase1_strategy"))
        for row in governance_rows
    )
    candidate_visible = bool(gc_candidate_surface) and symbol == "GC"
    paper_candidate_approved = bool(gc_candidate_surface.get("paper_candidate_approved")) if candidate_visible else False
    candidate_eval_ready = bool(gc_candidate_surface.get("candidate_evaluation_ready")) if candidate_visible else False
    external_feature_artifacts_required = (
        bool(gc_candidate_surface.get("external_feature_artifacts_required", True)) if candidate_visible else True
    )
    external_feature_artifacts_ready = bool(derived_features_ready or not external_feature_artifacts_required)
    runtime_data_ready_for_candidate = bool(runtime_candles_ready and external_feature_artifacts_ready)
    paper_watch_ready = bool(
        candidate_visible
        and paper_candidate_approved
        and candidate_eval_ready
        and runtime_data_ready_for_candidate
        and realtime_feed_confirmed
        and guarded_route_authorized
    )
    can_submit, block_reason = _submit_status(
        contract_metadata_present=metadata_present,
        lane_adapter_present=lane_adapter_present,
        strategy_approved=strategy_approved,
        runtime_data_ready=runtime_data_ready_for_candidate if candidate_visible else runtime_candles_ready and derived_features_ready,
        guarded_route_authorized=guarded_route_authorized,
    )
    return {
        "approved_phase1_symbol": symbol,
        "source_symbol_supported": bool(source_supported),
        "executable_target_supported": executable_target is not None and target is not None,
        "executable_target_symbol": executable_target,
        "contract_metadata_present": bool(metadata_present),
        "contract_qualification_ready": qualification_ready,
        "market_data_ready": market_data_ready,
        "historical_seed_ready": historical_seed_ready,
        "realtime_feed_confirmed": realtime_feed_confirmed,
        "runtime_candles_ready": runtime_candles_ready,
        "derived_features_ready": derived_features_ready,
        "external_feature_artifacts_required": external_feature_artifacts_required,
        "external_feature_artifacts_ready": external_feature_artifacts_ready,
        "runtime_data_ready_for_candidate": runtime_data_ready_for_candidate,
        "runtime_data_block_reason": runtime_data_block_reason,
        "governance_visible": governance_visible,
        "lane_adapter_present": lane_adapter_present,
        "paper_route_capable": bool(lane_adapter_present and metadata_present),
        "paper_candidate_visible": candidate_visible,
        "paper_candidate_strategy_id": gc_candidate_surface.get("strategy_id") if candidate_visible else None,
        "paper_candidate_family": gc_candidate_surface.get("family") if candidate_visible else None,
        "paper_candidate_approved": paper_candidate_approved,
        "paper_candidate_evaluation_ready": candidate_eval_ready,
        "paper_candidate_block_reason": gc_candidate_surface.get("block_reason") if candidate_visible else None,
        "paper_candidate_feature_contract_status": gc_candidate_surface.get("feature_contract_status") if candidate_visible else None,
        "paper_candidate_required_runtime_feature_artifacts": (
            gc_candidate_surface.get("required_runtime_feature_artifacts") if candidate_visible else []
        ),
        "paper_watch_ready": paper_watch_ready,
        "paper_watch_block_reason": "READY" if paper_watch_ready else _paper_watch_block_reason(
            symbol=symbol,
            candidate_visible=candidate_visible,
            paper_candidate_approved=paper_candidate_approved,
            candidate_eval_ready=candidate_eval_ready,
            runtime_candles_ready=runtime_candles_ready,
            derived_features_ready=derived_features_ready,
            external_feature_artifacts_required=external_feature_artifacts_required,
            realtime_feed_confirmed=realtime_feed_confirmed,
            guarded_route_authorized=guarded_route_authorized,
        ),
        "guarded_route_authorized": guarded_route_authorized,
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
    runtime_data_ready: bool,
    guarded_route_authorized: bool,
) -> tuple[bool, str]:
    if not contract_metadata_present:
        return False, "CONTRACT_METADATA_MISSING"
    if not lane_adapter_present:
        return False, "LANE_ADAPTER_MISSING"
    if not strategy_approved:
        return False, "NO_APPROVED_STRATEGY"
    if not runtime_data_ready:
        return False, "RUNTIME_DATA_NOT_READY"
    if not guarded_route_authorized:
        return False, "GUARDED_ROUTE_NOT_AUTHORIZED"
    return True, "READY"


def _paper_watch_block_reason(
    *,
    symbol: str,
    candidate_visible: bool,
    paper_candidate_approved: bool,
    candidate_eval_ready: bool,
    runtime_candles_ready: bool,
    derived_features_ready: bool,
    external_feature_artifacts_required: bool,
    realtime_feed_confirmed: bool,
    guarded_route_authorized: bool,
) -> str:
    if symbol != "GC":
        return "NO_GC_PAPER_CANDIDATE"
    if not candidate_visible:
        return "GC_PAPER_CANDIDATE_NOT_VISIBLE"
    if not paper_candidate_approved:
        return "GC_PAPER_CANDIDATE_NOT_APPROVED"
    if not candidate_eval_ready:
        return "GC_PAPER_CANDIDATE_NOT_EVALUATION_READY"
    if not realtime_feed_confirmed:
        return "REALTIME_FEED_NOT_CONFIRMED"
    if not runtime_candles_ready:
        return "RUNTIME_CANDLES_NOT_READY"
    if external_feature_artifacts_required and not derived_features_ready:
        return "DERIVED_FEATURES_NOT_READY"
    if not guarded_route_authorized:
        return "GUARDED_ROUTE_NOT_AUTHORIZED"
    return "NOT_READY"


def _gc_candidate_surface(
    *,
    config: Phase1TickerReadinessMatrixConfig,
    governance_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    guarded_route_authorized = any(
        str(row.get("strategy_id") or "").strip() == CHOSEN_GC_STRATEGY_ID
        and str(row.get("instrument") or "").strip().upper() == "GC"
        and bool(row.get("submit_allowed"))
        and bool(row.get("strategy_approved") or row.get("paper_strategy_approved") or row.get("approved_phase1_strategy"))
        for row in governance_rows
    )
    try:
        artifacts = build_phase1_gc_paper_candidate(
            config=Phase1GcCandidateConfig(
                repo_root=Path(config.repo_root),
                runtime_candle_root=config.runtime_candle_dir,
                runtime_feature_root=config.feature_state_dir,
                output_dir=Path(config.output_dir),
                max_bars=240,
                write_report=False,
                guarded_route_authorized=guarded_route_authorized,
                now=config.now,
            )
        )
    except Exception as exc:  # pragma: no cover - defensive surface for operator reports
        return {
            "strategy_id": CHOSEN_GC_STRATEGY_ID,
            "candidate_evaluation_ready": False,
            "block_reason": f"GC_CANDIDATE_SURFACE_ERROR: {exc}",
            "paper_candidate_approved": True,
            "paper_watch_ready": False,
        }
    evaluation = dict(artifacts.evaluation)
    return {
        "strategy_id": artifacts.report.get("chosen_strategy") or CHOSEN_GC_STRATEGY_ID,
        "family": artifacts.report.get("chosen_strategy_family"),
        "candidate_evaluation_ready": bool(evaluation.get("candidate_evaluation_ready")),
        "block_reason": evaluation.get("block_reason") or evaluation.get("paper_watch_block_reason"),
        "paper_candidate_approved": bool(artifacts.report.get("paper_candidate_approved")),
        "paper_watch_ready": bool(evaluation.get("paper_watch_ready")),
        "external_feature_artifacts_required": bool(evaluation.get("external_feature_artifacts_required", True)),
        "external_feature_artifacts_ready": bool(evaluation.get("external_feature_artifacts_ready")),
        "required_runtime_feature_artifacts": list(evaluation.get("required_runtime_feature_artifacts") or []),
        "feature_contract_status": evaluation.get("feature_contract_status"),
    }


def _contract_metadata_present(target: dict[str, Any] | None) -> bool:
    if not target:
        return False
    required = ("symbol", "contract_month", "exchange", "currency", "multiplier", "trading_class")
    return all(bool(str(target.get(key) or "").strip()) for key in required)


def _market_data_ready(*, symbol: str, market_data: dict[str, Any]) -> bool:
    pilot_symbols = (((market_data.get("databento") or {}).get("pilot_symbols")) or {})
    row = dict(pilot_symbols.get(symbol) or {})
    return bool(row.get("request_symbol") and (row.get("schema_by_timeframe") or {}).get("1m"))


def _runtime_data_block_reason(runtime_data: dict[str, Any]) -> str:
    if not runtime_data:
        return "RUNTIME_DATA_NOT_READY"
    if not runtime_data.get("runtime_candles_ready"):
        return str(runtime_data.get("runtime_candles_block_reason") or "RUNTIME_CANDLES_NOT_READY")
    if not runtime_data.get("derived_features_ready"):
        return str(runtime_data.get("derived_features_block_reason") or "FEATURES_NOT_READY")
    return "READY"


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
