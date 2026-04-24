"""Bounded Layer 1 adaptation for the Asia-London participation family."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from mgc_v05l.app.asia_london_participation_research import POINT_VALUES

from ..data.contracts import (
    EquityPoint,
    OptimizationLinkage,
    OptimizationRun,
    OptimizationTrial,
    PositionPoint,
    SplitWindow,
    StrategyBacktest,
    TradeRecord,
)
from .producer_contract import (
    build_data_provenance,
    build_execution_assumptions,
    build_strategy_backtest_provenance,
    build_trade_validation_metadata,
    full_sample_split_window,
)


REPO_ROOT = Path.cwd()
DEFAULT_CANDIDATE_SYSTEM_JSON = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "asia_london_participation_candidate_system_archive_v2"
    / "asia_london_participation_candidate_system_research.json"
)
DEFAULT_ADMISSION_PLAN_JSON = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "asia_london_participation_candidate_admission_archive_v2"
    / "asia_london_participation_candidate_admission_plan.json"
)
DEFAULT_GC_MGC_OPTIMIZATION_JSON = Path("/private/tmp/asia_london_opt_gc_mgc/asia_london_participation_optimization.json")
DEFAULT_NQ_MNQ_OPTIMIZATION_JSON = Path("/private/tmp/asia_london_opt_nq_mnq/asia_london_participation_optimization.json")
DEFAULT_ES_VALIDATION_JSON = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "es_asia_london_candidate_validation_v1"
    / "es_asia_london_candidate_validation.json"
)

_SUPPORTED_SYMBOL_GROUPS: dict[str, tuple[str, ...]] = {
    "GC_MGC": ("GC", "MGC"),
    "NQ_MNQ": ("NQ", "MNQ"),
}


@dataclass(frozen=True)
class AsiaLondonFamilySource:
    candidate_system_json: Path
    admission_plan_json: Path
    gc_mgc_optimization_json: Path
    nq_mnq_optimization_json: Path
    es_validation_json: Path
    candidate_system_payload: dict[str, Any]
    admission_plan_payload: dict[str, Any]
    gc_mgc_payload: dict[str, Any]
    nq_mnq_payload: dict[str, Any]
    es_validation_payload: dict[str, Any]


@dataclass(frozen=True)
class AsiaLondonCandidateBundle:
    subject_label: str
    strategy_backtest: StrategyBacktest | None
    optimization_run: OptimizationRun | None
    history_payload: dict[str, Any] | None
    support_status: str
    evidence_available: tuple[str, ...]
    evidence_missing: tuple[str, ...]
    candidate_metadata: dict[str, Any]


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = REPO_ROOT / resolved
    return resolved.resolve(strict=False)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_asia_london_family_source(
    *,
    candidate_system_json: str | Path = DEFAULT_CANDIDATE_SYSTEM_JSON,
    admission_plan_json: str | Path = DEFAULT_ADMISSION_PLAN_JSON,
    gc_mgc_optimization_json: str | Path = DEFAULT_GC_MGC_OPTIMIZATION_JSON,
    nq_mnq_optimization_json: str | Path = DEFAULT_NQ_MNQ_OPTIMIZATION_JSON,
    es_validation_json: str | Path = DEFAULT_ES_VALIDATION_JSON,
) -> AsiaLondonFamilySource:
    candidate_system_path = _resolve_path(candidate_system_json)
    admission_plan_path = _resolve_path(admission_plan_json)
    gc_mgc_path = _resolve_path(gc_mgc_optimization_json)
    nq_mnq_path = _resolve_path(nq_mnq_optimization_json)
    es_validation_path = _resolve_path(es_validation_json)
    return AsiaLondonFamilySource(
        candidate_system_json=candidate_system_path,
        admission_plan_json=admission_plan_path,
        gc_mgc_optimization_json=gc_mgc_path,
        nq_mnq_optimization_json=nq_mnq_path,
        es_validation_json=es_validation_path,
        candidate_system_payload=_load_json(candidate_system_path),
        admission_plan_payload=_load_json(admission_plan_path),
        gc_mgc_payload=_load_json(gc_mgc_path),
        nq_mnq_payload=_load_json(nq_mnq_path),
        es_validation_payload=_load_json(es_validation_path),
    )


def _candidate_rows(source: AsiaLondonFamilySource) -> tuple[dict[str, Any], ...]:
    return tuple(source.candidate_system_payload["candidate_system"]["lane_sequence"])


def _variant_parameters(*, variant_key: str, variant_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_variant": variant_key,
        "variant_id": str(variant_payload["variant_id"]),
        "side": str(variant_payload["side"]).lower(),
        "gate_mode": str(variant_payload["gate_mode"]),
    }


def _optimization_id(strategy_name: str, chosen_parameters: dict[str, Any]) -> str:
    payload = {"strategy_name": strategy_name, "chosen_parameters": chosen_parameters}
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    return f"asia_london_participation:{digest[:16]}"


def _family_optimization_rows(source: AsiaLondonFamilySource) -> tuple[dict[str, Any], ...]:
    rows: list[dict[str, Any]] = []
    payloads = {
        "GC_MGC": source.gc_mgc_payload,
        "NQ_MNQ": source.nq_mnq_payload,
    }
    for symbol_group, payload in payloads.items():
        for symbol, report in sorted(payload["symbol_reports"].items()):
            for variant_key, variant_payload in sorted(report["variants"].items()):
                summary = variant_payload["trade_summary"]
                total_net_pnl_points = float(summary["total_net_pnl_points"] or 0.0)
                point_value = float(POINT_VALUES[symbol])
                rows.append(
                    {
                        "split_id": symbol,
                        "symbol_group": symbol_group,
                        "source_variant": variant_key,
                        "parameter_values": _variant_parameters(variant_key=variant_key, variant_payload=variant_payload),
                        "objective_value": round(total_net_pnl_points * point_value, 6),
                        "metrics": {
                            "entered_trade_count": int(summary["entered_trade_count"]),
                            "average_net_pnl_points": float(summary["average_net_pnl_points"] or 0.0),
                            "average_net_pnl_dollars": round(float(summary["average_net_pnl_points"] or 0.0) * point_value, 6),
                            "median_net_pnl_points": float(summary["median_net_pnl_points"] or 0.0),
                            "net_profit_factor": float(summary["net_profit_factor"] or 0.0),
                            "max_drawdown_points": float(summary["max_drawdown_points"] or 0.0),
                            "max_drawdown_dollars": round(float(summary["max_drawdown_points"] or 0.0) * point_value, 6),
                            "total_net_pnl_points": total_net_pnl_points,
                            "total_net_pnl_dollars": round(total_net_pnl_points * point_value, 6),
                        },
                    }
                )
    return tuple(rows)


def build_asia_london_optimization_run(
    *,
    source: AsiaLondonFamilySource,
    strategy_name: str,
    chosen_variant_key: str,
) -> tuple[OptimizationRun | None, dict[str, Any] | None, tuple[str, ...]]:
    rows = _family_optimization_rows(source)
    candidate_keys = sorted({row["source_variant"] for row in rows})
    if chosen_variant_key not in candidate_keys:
        return None, None, ("chosen_variant_missing_from_full_history",)

    chosen_rows = [row for row in rows if row["source_variant"] == chosen_variant_key]
    split_ids = sorted({row["split_id"] for row in rows})
    if len(split_ids) < 2:
        return None, None, ("split_aware_history_unavailable",)

    ranked_trials: list[OptimizationTrial] = []
    for split_id in split_ids:
        split_rows = [row for row in rows if row["split_id"] == split_id]
        ordered = sorted(split_rows, key=lambda item: item["objective_value"], reverse=True)
        for rank, row in enumerate(ordered, start=1):
            ranked_trials.append(
                OptimizationTrial(
                    parameter_values=dict(row["parameter_values"]),
                    in_sample_metrics=dict(row["metrics"]),
                    out_of_sample_metrics={},
                    split_id=str(split_id),
                    rank=rank,
                    objective_value=float(row["objective_value"]),
                    metadata={
                        "symbol_group": row["symbol_group"],
                        "objective_name": "total_net_pnl_dollars",
                    },
                )
            )

    chosen_parameters = dict(chosen_rows[0]["parameter_values"])
    optimization_id = _optimization_id(strategy_name, chosen_parameters)
    run = OptimizationRun(
        strategy_name=strategy_name,
        search_space={
            "source_variant": candidate_keys,
            "variant_id": sorted({row["parameter_values"]["variant_id"] for row in rows}),
            "side": sorted({row["parameter_values"]["side"] for row in rows}),
            "gate_mode": sorted({row["parameter_values"]["gate_mode"] for row in rows}),
        },
        trials=tuple(ranked_trials),
        chosen_parameters=chosen_parameters,
        objective_name="total_net_pnl_dollars",
        metadata={
            "optimization_id": optimization_id,
            "source_family": "asia_london_participation_candidate_system",
            "split_family": "cross_symbol_candidate_surface",
            "split_ids": split_ids,
            "source_artifacts": {
                "gc_mgc_optimization_json": str(source.gc_mgc_optimization_json),
                "nq_mnq_optimization_json": str(source.nq_mnq_optimization_json),
            },
        },
    )
    history_payload = {
        "optimization_id": optimization_id,
        "chosen_variant_key": chosen_variant_key,
        "objective_name": "total_net_pnl_dollars",
        "split_ids": split_ids,
        "trial_count": len(ranked_trials),
        "candidate_count": len(candidate_keys),
        "source_artifacts": {
            "gc_mgc_optimization_json": str(source.gc_mgc_optimization_json),
            "nq_mnq_optimization_json": str(source.nq_mnq_optimization_json),
        },
        "rows": [
            {
                "split_id": row["split_id"],
                "symbol_group": row["symbol_group"],
                "source_variant": row["source_variant"],
                "objective_value": row["objective_value"],
                "metrics": row["metrics"],
            }
            for row in rows
        ],
    }
    return run, history_payload, ()


def _candidate_symbols_for_variant(source: AsiaLondonFamilySource, variant_key: str) -> tuple[str, ...]:
    symbols: list[str] = []
    for row in _candidate_rows(source):
        if row["source_variant"] != variant_key:
            continue
        for symbol in _SUPPORTED_SYMBOL_GROUPS.get(str(row["symbol_group"]), ()):
            if symbol not in symbols:
                symbols.append(symbol)
    return tuple(symbols)


def _build_equity_curve(trades: tuple[TradeRecord, ...], start: datetime, starting_equity: float = 100_000.0) -> tuple[EquityPoint, ...]:
    equity = starting_equity
    curve = [EquityPoint(timestamp=start, equity=equity)]
    for trade in sorted(trades, key=lambda item: item.exit_time):
        equity += float(trade.net_pnl)
        curve.append(EquityPoint(timestamp=trade.exit_time, equity=equity, session_label=trade.session_label))
    return tuple(curve)


def _build_position_series(trades: tuple[TradeRecord, ...], start: datetime) -> tuple[PositionPoint, ...]:
    deltas: dict[datetime, float] = {start: 0.0}
    for trade in sorted(trades, key=lambda item: item.entry_time):
        signed_qty = trade.qty if trade.direction == "long" else -trade.qty
        deltas[trade.entry_time] = deltas.get(trade.entry_time, 0.0) + signed_qty
        deltas[trade.exit_time] = deltas.get(trade.exit_time, 0.0) - signed_qty

    running = 0.0
    series: list[PositionPoint] = []
    for timestamp in sorted(deltas):
        running += deltas[timestamp]
        series.append(PositionPoint(timestamp=timestamp, position=running, exposure=running))
    return tuple(series)


def _variant_payload_for_symbol(source: AsiaLondonFamilySource, symbol: str, variant_key: str) -> dict[str, Any] | None:
    payloads = (source.gc_mgc_payload, source.nq_mnq_payload)
    for payload in payloads:
        report = payload["symbol_reports"].get(symbol)
        if report is None:
            continue
        return report["variants"].get(variant_key)
    return None


def build_asia_london_candidate_bundle(
    *,
    source: AsiaLondonFamilySource,
    variant_key: str,
    strategy_name: str,
    code_version: str,
) -> AsiaLondonCandidateBundle:
    candidate_rows = [row for row in _candidate_rows(source) if row["source_variant"] == variant_key]
    if not candidate_rows:
        return AsiaLondonCandidateBundle(
            subject_label=strategy_name,
            strategy_backtest=None,
            optimization_run=None,
            history_payload=None,
            support_status="insufficient_evidence",
            evidence_available=(),
            evidence_missing=("candidate_not_present_in_archive",),
            candidate_metadata={"source_variant": variant_key},
        )

    symbols = _candidate_symbols_for_variant(source, variant_key)
    if not symbols:
        return AsiaLondonCandidateBundle(
            subject_label=strategy_name,
            strategy_backtest=None,
            optimization_run=None,
            history_payload=None,
            support_status="insufficient_evidence",
            evidence_available=("candidate_system_archive",),
            evidence_missing=("session_lineage_unavailable",),
            candidate_metadata={
                "source_variant": variant_key,
                "lane_ids": [row["lane_id"] for row in candidate_rows],
                "symbol_groups": sorted({row["symbol_group"] for row in candidate_rows}),
            },
        )

    converted_trades: list[TradeRecord] = []
    session_rows_seen = 0
    for symbol in symbols:
        variant_payload = _variant_payload_for_symbol(source, symbol, variant_key)
        if variant_payload is None:
            continue
        point_value = float(POINT_VALUES[symbol])
        for session in variant_payload["sessions"]:
            if not session.get("entered") or not session.get("entry_end_ts") or not session.get("exit_end_ts"):
                continue
            session_rows_seen += 1
            entry_time = datetime.fromisoformat(str(session["entry_end_ts"]))
            exit_time = datetime.fromisoformat(str(session["exit_end_ts"]))
            pnl_points = float(session["pnl_points"] or 0.0)
            net_pnl_points = float(session["net_pnl_points"] or 0.0)
            mae_points = float(session["mae_points"] or 0.0)
            mfe_points = float(session["mfe_points"] or 0.0)
            converted_trades.append(
                TradeRecord(
                    entry_time=entry_time,
                    exit_time=exit_time,
                    direction=str(session["side"]).lower(),
                    qty=1.0,
                    gross_pnl=round(pnl_points * point_value, 6),
                    net_pnl=round(net_pnl_points * point_value, 6),
                    mae=round(mae_points * point_value, 6),
                    mfe=round(mfe_points * point_value, 6),
                    holding_bars=max(
                        1,
                        int((session.get("exit_bar_number") or session.get("entry_bar_number") or 1))
                        - int(session.get("entry_bar_number") or 0)
                        + 1,
                    ),
                    holding_minutes=max(1.0, (exit_time - entry_time).total_seconds() / 60.0),
                    session_label=str(session.get("exit_session_phase") or "ASIA_LONDON"),
                    validation_metadata=build_trade_validation_metadata(
                        trade_id=f"{symbol}:{variant_key}:{session['trade_date']}:{entry_time.isoformat()}",
                        signal_id=f"{symbol}:{variant_key}:{session['trade_date']}",
                        setup_family="asia_london_participation",
                        setup_variant=str(session["variant_id"]),
                        decision_time=entry_time,
                        exit_reason=str(session.get("exit_reason") or ""),
                        execution_model="asia_london_participation_optimization_archive",
                        fill_policy="serialized_session_entry_exit",
                        tags={
                            "symbol": symbol,
                            "source_variant": variant_key,
                            "trade_date": session["trade_date"],
                            "entry_reason": session.get("entry_reason"),
                            "embedded_costs_in_source_net_pnl_points": True,
                        },
                    ),
                    metadata={
                        "symbol": symbol,
                        "point_value": point_value,
                        "raw_session": dict(session),
                    },
                )
            )

    if not converted_trades:
        return AsiaLondonCandidateBundle(
            subject_label=strategy_name,
            strategy_backtest=None,
            optimization_run=None,
            history_payload=None,
            support_status="insufficient_evidence",
            evidence_available=("candidate_system_archive",),
            evidence_missing=("no_entered_trade_lineage",),
            candidate_metadata={
                "source_variant": variant_key,
                "lane_ids": [row["lane_id"] for row in candidate_rows],
                "symbols": list(symbols),
            },
        )

    ordered_trades = tuple(sorted(converted_trades, key=lambda item: item.entry_time))
    start = ordered_trades[0].entry_time
    end = max(trade.exit_time for trade in ordered_trades)
    split_window = full_sample_split_window(start=start, end=end)
    optimization_run, history_payload, optimization_missing = build_asia_london_optimization_run(
        source=source,
        strategy_name=strategy_name,
        chosen_variant_key=variant_key,
    )
    optimization_linkage = None
    if optimization_run is not None:
        chosen_hash = hashlib.sha256(
            json.dumps(optimization_run.chosen_parameters, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()
        optimization_linkage = OptimizationLinkage(
            optimization_id=str(optimization_run.metadata["optimization_id"]),
            objective_name=optimization_run.objective_name,
            chosen_parameter_hash=chosen_hash,
            source_artifact_id=str(source.gc_mgc_optimization_json),
            split_family="cross_symbol_candidate_surface",
        )

    provenance = build_strategy_backtest_provenance(
        producer_id="asia_london_participation_candidate_system_research",
        producer_family="asia_london_participation_core_v1",
        study_mode="research_candidate_system_archive",
        split_method="full_sample",
        split_windows=(split_window,),
        execution_assumptions=build_execution_assumptions(
            fill_policy="serialized_session_entry_exit",
            entry_fill_basis="session_entry_bar_open_from_source_archive",
            exit_fill_basis="rule_based_exit_serialized_in_source_archive",
            execution_mode="research_replay_archive",
            slippage_model_name="embedded_trade_level_costs_from_source_study",
            slippage_model_version="v1",
            slippage_parameters={"embedded_in_net_pnl_points": True},
            fee_model_name="embedded_round_turn_commission_from_source_study",
            fee_model_version="v1",
            fee_parameters={"embedded_in_net_pnl_points": True},
            conservative_bias="source archive already applies per-trade friction before serialization",
            notes=("asia_london_participation_candidate_system_research", variant_key),
        ),
        data_provenance=build_data_provenance(
            source_type="research_archive_json",
            data_source="asia_london_participation_candidate_system_research",
            data_version="asia_london_candidate_system_archive_v2",
            provenance_id=f"asia_london_participation:{variant_key}:archive_v2",
            feature_version="asia_london_participation_research_v1",
            code_version=code_version,
            coverage_start=start,
            coverage_end=end,
            artifact_ids={
                "candidate_system_json": str(source.candidate_system_json.name),
                "admission_plan_json": str(source.admission_plan_json.name),
                "gc_mgc_optimization_json": str(source.gc_mgc_optimization_json.name),
                "nq_mnq_optimization_json": str(source.nq_mnq_optimization_json.name),
            },
            tags=("asia_london_participation", "layer1_ready"),
        ),
        optimization_linkage=optimization_linkage,
        artifact_references={
            "candidate_system_json": str(source.candidate_system_json),
            "admission_plan_json": str(source.admission_plan_json),
            "gc_mgc_optimization_json": str(source.gc_mgc_optimization_json),
            "nq_mnq_optimization_json": str(source.nq_mnq_optimization_json),
            "es_validation_json": str(source.es_validation_json),
        },
        tags=("layer1_ready", "asia_london_participation_core_v1", variant_key),
    )
    strategy_backtest = StrategyBacktest(
        strategy_name=strategy_name,
        symbol="+".join(symbols),
        timeframe="3m",
        parameters={
            "source_variant": variant_key,
            "lane_ids": tuple(row["lane_id"] for row in candidate_rows),
            "symbols": symbols,
        },
        bar_data=(),
        trades=ordered_trades,
        equity_curve=_build_equity_curve(ordered_trades, start=start),
        position_series=_build_position_series(ordered_trades, start=start),
        provenance=provenance,
        metadata={
            "pilot_family": "asia_london_participation_core_v1",
            "candidate_id": source.candidate_system_payload["candidate_system"]["candidate_id"],
            "source_variant": variant_key,
            "lane_rows": candidate_rows,
            "session_rows_seen": session_rows_seen,
            "supported_symbols": symbols,
        },
    )
    support_status = "scientifically_judgeable" if optimization_run is not None else "partially_judgeable"
    return AsiaLondonCandidateBundle(
        subject_label=strategy_name,
        strategy_backtest=strategy_backtest,
        optimization_run=optimization_run,
        history_payload=history_payload,
        support_status=support_status,
        evidence_available=("candidate_system_archive", "session_lineage", "family_optimization_history"),
        evidence_missing=tuple(optimization_missing),
        candidate_metadata={
            "source_variant": variant_key,
            "lane_ids": [row["lane_id"] for row in candidate_rows],
            "symbols": list(symbols),
            "symbol_groups": sorted({row["symbol_group"] for row in candidate_rows}),
        },
    )
