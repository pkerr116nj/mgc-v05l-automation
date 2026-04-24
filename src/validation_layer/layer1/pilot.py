"""Deterministic pilot rerun path for Layer 1 producers and Layer 2 validation."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from mgc_v05l.app.approved_quant_lanes.evaluator import evaluate_approved_lane
from mgc_v05l.app.approved_quant_lanes.specs import ApprovedQuantLaneSpec, approved_quant_lane_specs
from mgc_v05l.app.gc_mgc_ny_early_short_forced_session_research import ForcedNyEarlyShortSpec, ForcedSessionTrade
from mgc_v05l.domain.models import Bar
from mgc_v05l.research.quant_futures import _FrameSeries
from mgc_v05l.research.trend_participation.backtest import backtest_decisions_with_audit
from mgc_v05l.research.trend_participation.models import ConflictOutcome, PatternVariant, ResearchBar, SignalDecision

from ..config.defaults import build_debug_config
from ..data.contracts import ValidationSubject
from ..orchestration.pipeline import run_validation_pipeline
from ..reporting.json_report import render_json_report
from ..reporting.markdown_report import render_markdown_report
from .atp_adapter import build_strategy_backtest_from_atp_source, load_atp_layer1_source
from .asia_london_family import build_asia_london_candidate_bundle, load_asia_london_family_source
from .atp_optimization import rerun_atp_promotion_add_validation_bundle
from .adapters import (
    build_strategy_backtest_from_approved_quant_lane,
    build_strategy_backtest_from_forced_session,
    build_strategy_backtest_from_trend_participation,
)


@dataclass(frozen=True)
class Layer1PilotArtifacts:
    root_dir: Path
    manifest_path: Path
    reports: dict[str, dict[str, str]]


def run_layer1_pilot_rerun(*, output_dir: str | Path) -> Layer1PilotArtifacts:
    root_dir = Path(output_dir).resolve()
    root_dir.mkdir(parents=True, exist_ok=True)

    backtests = {
        "trend_participation_long": _build_trend_participation_pilot_backtest(),
        "forced_session_short": _build_forced_session_pilot_backtest(),
    }

    reports: dict[str, dict[str, str]] = {}
    manifest_rows: list[dict[str, Any]] = []
    for name, backtest in backtests.items():
        subject_dir = root_dir / name
        subject_dir.mkdir(parents=True, exist_ok=True)
        strategy_path, report_json_path, report_markdown_path = _write_validation_bundle(
            subject_dir=subject_dir,
            backtest=backtest,
        )
        reports[name] = {
            "strategy_backtest_json": str(strategy_path),
            "validation_report_json": str(report_json_path),
            "validation_report_markdown": str(report_markdown_path),
        }
        manifest_rows.append(
            {
                "strategy_name": backtest.strategy_name,
                "symbol": backtest.symbol,
                "timeframe": backtest.timeframe,
                "producer_id": backtest.provenance.producer_id if backtest.provenance else None,
                "split_method": backtest.provenance.split_method if backtest.provenance else None,
                "report_paths": reports[name],
            }
        )

    manifest_path = root_dir / "layer1_pilot_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "pilot_count": len(backtests),
                "reports": reports,
                "rows": manifest_rows,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return Layer1PilotArtifacts(root_dir=root_dir, manifest_path=manifest_path, reports=reports)


def run_approved_quant_layer1_pilot_rerun(*, output_dir: str | Path) -> Layer1PilotArtifacts:
    root_dir = Path(output_dir).resolve()
    root_dir.mkdir(parents=True, exist_ok=True)

    symbol_store = _approved_quant_symbol_store()
    backtests = {
        spec.lane_id: _build_approved_quant_pilot_backtest(spec=spec, symbol_store=symbol_store)
        for spec in approved_quant_lane_specs()
    }

    reports: dict[str, dict[str, str]] = {}
    manifest_rows: list[dict[str, Any]] = []
    for lane_id, backtest in backtests.items():
        subject_dir = root_dir / lane_id.replace(".", "_")
        subject_dir.mkdir(parents=True, exist_ok=True)
        strategy_path, report_json_path, report_markdown_path = _write_validation_bundle(
            subject_dir=subject_dir,
            backtest=backtest,
        )
        reports[lane_id] = {
            "strategy_backtest_json": str(strategy_path),
            "validation_report_json": str(report_json_path),
            "validation_report_markdown": str(report_markdown_path),
        }
        manifest_rows.append(
            {
                "lane_id": lane_id,
                "strategy_name": backtest.strategy_name,
                "symbol": backtest.symbol,
                "timeframe": backtest.timeframe,
                "producer_id": backtest.provenance.producer_id if backtest.provenance else None,
                "split_method": backtest.provenance.split_method if backtest.provenance else None,
                "report_paths": reports[lane_id],
            }
        )

    manifest_path = root_dir / "approved_quant_layer1_pilot_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "pilot_count": len(backtests),
                "reports": reports,
                "rows": manifest_rows,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return Layer1PilotArtifacts(root_dir=root_dir, manifest_path=manifest_path, reports=reports)


def run_atp_layer1_pilot_rerun(*, output_dir: str | Path) -> Layer1PilotArtifacts:
    root_dir = Path(output_dir).resolve()
    root_dir.mkdir(parents=True, exist_ok=True)

    sources = {
        "atp_baseline_mgc_asia_us": load_atp_layer1_source(
            lane_id="atp_companion_v1_asia_us",
            subject_label="atp_baseline_mgc_asia_us",
            source_config_path="config/probationary_pattern_engine_paper_atp_companion_v1_asia_us.yaml",
            lane_dir="outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_asia_us",
            runtime_config_in_force_path="outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json",
            family_classification="baseline_lane",
            baseline_reference_path="config/atp_companion_baseline_v1_asia_us.yaml",
        ),
        "atp_candidate_mgc_asia_promotion_1_075r_favorable_only": load_atp_layer1_source(
            lane_id="atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only",
            subject_label="atp_candidate_mgc_asia_promotion_1_075r_favorable_only",
            source_config_path="config/probationary_pattern_engine_paper_atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only.yaml",
            lane_dir="outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only",
            runtime_config_in_force_path="outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json",
            family_classification="candidate_lane",
            baseline_reference_path="config/atp_companion_baseline_v1_asia_us.yaml",
            candidate_registry_path="config/atp_promotion_add_candidate_registry.yaml",
            candidate_config_path="config/atp_companion_candidate_promotion_1_075r_favorable_only.yaml",
        ),
    }

    backtests = {
        name: build_strategy_backtest_from_atp_source(source, code_version="pilot")
        for name, source in sources.items()
    }

    reports: dict[str, dict[str, str]] = {}
    manifest_rows: list[dict[str, Any]] = []
    for name, backtest in backtests.items():
        subject_dir = root_dir / name
        subject_dir.mkdir(parents=True, exist_ok=True)
        strategy_path, report_json_path, report_markdown_path = _write_validation_bundle(
            subject_dir=subject_dir,
            backtest=backtest,
        )
        reports[name] = {
            "strategy_backtest_json": str(strategy_path),
            "validation_report_json": str(report_json_path),
            "validation_report_markdown": str(report_markdown_path),
        }
        manifest_rows.append(
            {
                "subject_label": name,
                "lane_id": backtest.parameters.get("lane_id"),
                "strategy_name": backtest.strategy_name,
                "symbol": backtest.symbol,
                "timeframe": backtest.timeframe,
                "producer_id": backtest.provenance.producer_id if backtest.provenance else None,
                "split_method": backtest.provenance.split_method if backtest.provenance else None,
                "report_paths": reports[name],
            }
        )

    manifest_path = root_dir / "atp_layer1_pilot_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "pilot_count": len(backtests),
                "reports": reports,
                "rows": manifest_rows,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return Layer1PilotArtifacts(root_dir=root_dir, manifest_path=manifest_path, reports=reports)


def run_atp_promotion_add_optimization_pilot_rerun(
    *,
    output_dir: str | Path,
    source_sqlite_path: str | Path = "mgc_v05l.replay.sqlite3",
    max_windows: int = 8,
) -> Layer1PilotArtifacts:
    root_dir = Path(output_dir).resolve()
    root_dir.mkdir(parents=True, exist_ok=True)

    bundle = rerun_atp_promotion_add_validation_bundle(
        source_sqlite_path=source_sqlite_path,
        instruments=("MGC",),
        point_value=10.0,
        max_windows=max_windows,
        code_version="pilot",
    )
    subject_dir = root_dir / "atp_promotion_add_mgc_active_candidate"
    subject_dir.mkdir(parents=True, exist_ok=True)
    paths = _write_validation_subject_bundle(
        subject_dir=subject_dir,
        subject=ValidationSubject(
            strategy_backtest=bundle.strategy_backtest,
            optimization_run=bundle.optimization_run,
        ),
        history_payload=bundle.history_payload,
    )
    reports = {"atp_promotion_add_mgc_active_candidate": paths}
    manifest_path = root_dir / "atp_promotion_add_optimization_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "pilot_count": 1,
                "reports": reports,
                "rows": [
                    {
                        "strategy_name": bundle.strategy_backtest.strategy_name,
                        "symbol": bundle.strategy_backtest.symbol,
                        "producer_id": (
                            None if bundle.strategy_backtest.provenance is None else bundle.strategy_backtest.provenance.producer_id
                        ),
                        "optimization_id": bundle.optimization_run.metadata.get("optimization_id"),
                        "window_count": bundle.history_payload.get("window_count"),
                        "report_paths": paths,
                    }
                ],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return Layer1PilotArtifacts(root_dir=root_dir, manifest_path=manifest_path, reports=reports)


def run_asia_london_participation_family_pilot_rerun(
    *,
    output_dir: str | Path,
    candidate_system_json: str | Path | None = None,
    admission_plan_json: str | Path | None = None,
    gc_mgc_optimization_json: str | Path | None = None,
    nq_mnq_optimization_json: str | Path | None = None,
    es_validation_json: str | Path | None = None,
) -> Layer1PilotArtifacts:
    root_dir = Path(output_dir).resolve()
    root_dir.mkdir(parents=True, exist_ok=True)

    source_kwargs: dict[str, object] = {}
    if candidate_system_json is not None:
        source_kwargs["candidate_system_json"] = candidate_system_json
    if admission_plan_json is not None:
        source_kwargs["admission_plan_json"] = admission_plan_json
    if gc_mgc_optimization_json is not None:
        source_kwargs["gc_mgc_optimization_json"] = gc_mgc_optimization_json
    if nq_mnq_optimization_json is not None:
        source_kwargs["nq_mnq_optimization_json"] = nq_mnq_optimization_json
    if es_validation_json is not None:
        source_kwargs["es_validation_json"] = es_validation_json
    source = load_asia_london_family_source(**source_kwargs)

    candidate_rows = tuple(source.candidate_system_payload["candidate_system"]["lane_sequence"])
    variant_candidates: list[str] = []
    for row in candidate_rows:
        variant_key = str(row["source_variant"])
        if variant_key not in variant_candidates:
            variant_candidates.append(variant_key)

    reports: dict[str, dict[str, str]] = {}
    summary_rows: list[dict[str, Any]] = []
    fully_supported = 0
    for variant_key in variant_candidates:
        subject_label = f"asia_london_participation::{variant_key}"
        bundle = build_asia_london_candidate_bundle(
            source=source,
            variant_key=variant_key,
            strategy_name=subject_label,
            code_version="pilot",
        )
        subject_dir = root_dir / variant_key.lower().replace("__", "_").replace("/", "_")
        subject_dir.mkdir(parents=True, exist_ok=True)
        row: dict[str, Any] = {
            "subject_label": subject_label,
            "source_variant": variant_key,
            "support_status": bundle.support_status,
            "evidence_available": list(bundle.evidence_available),
            "evidence_missing": list(bundle.evidence_missing),
            "candidate_metadata": bundle.candidate_metadata,
            "optimization_history_attached": bundle.optimization_run is not None,
        }
        if bundle.strategy_backtest is not None:
            subject = ValidationSubject(
                strategy_backtest=bundle.strategy_backtest,
                optimization_run=bundle.optimization_run,
            )
            paths = _write_validation_subject_bundle(
                subject_dir=subject_dir,
                subject=subject,
                history_payload=bundle.history_payload,
            )
            reports[variant_key] = paths
            report_payload = json.loads(Path(paths["validation_report_json"]).read_text(encoding="utf-8"))
            row.update(
                {
                    "report_paths": paths,
                    "trade_count": len(bundle.strategy_backtest.trades),
                    "overall_status": report_payload["overall_status"],
                    "module_statuses": {
                        item["module_name"]: item["status"] for item in report_payload.get("module_results", [])
                    },
                    "judgeability": "scientifically_judgeable" if bundle.optimization_run is not None else "partially_judgeable",
                }
            )
            if bundle.optimization_run is not None:
                fully_supported += 1
        else:
            row["judgeability"] = "insufficient_evidence"
        summary_rows.append(row)

    family_judgeability = "scientifically_judgeable" if fully_supported == len(variant_candidates) else "partially_judgeable"
    summary_payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "family_name": "asia_london_participation_core_v1",
        "candidate_count": len(variant_candidates),
        "fully_supported_candidate_count": fully_supported,
        "family_judgeability": family_judgeability,
        "core_surface_judgeability": "scientifically_judgeable" if fully_supported > 0 else "insufficient_evidence",
        "notes": [
            "GC/MGC and NQ/MNQ candidates are adapted from full optimization-session archives.",
            "ES-only candidate remains insufficient evidence because only ranked-summary validation is preserved locally.",
        ],
        "rows": summary_rows,
    }
    summary_json_path = root_dir / "asia_london_family_summary.json"
    summary_md_path = root_dir / "asia_london_family_summary.md"
    summary_json_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_md_path.write_text(_render_asia_london_family_summary(summary_payload), encoding="utf-8")

    manifest_path = root_dir / "asia_london_family_pilot_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "pilot_count": len(variant_candidates),
                "reports": reports,
                "family_summary_json": str(summary_json_path),
                "family_summary_markdown": str(summary_md_path),
                "rows": summary_rows,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return Layer1PilotArtifacts(root_dir=root_dir, manifest_path=manifest_path, reports=reports)


def _build_trend_participation_pilot_backtest():
    bars_1m = tuple(_trend_bars())
    decisions = tuple(_trend_decisions())
    variant = PatternVariant(
        variant_id="trend_participation.breakout_continuation.long.active",
        family="breakout_continuation",
        side="LONG",
        strictness="active",
        description="Synthetic deterministic ATP pilot variant.",
        entry_window_bars_1m=2,
        max_hold_bars_1m=3,
        stop_atr_multiple=1.0,
        target_r_multiple=1.5,
        allow_reentry=False,
    )
    trades, _ = backtest_decisions_with_audit(
        decisions=decisions,
        bars_1m=bars_1m,
        variants_by_id={variant.variant_id: variant},
        point_values={"MES": 5.0},
        include_shadow_only=False,
        slippage_points=0.25,
        fee_per_trade=1.50,
    )
    return build_strategy_backtest_from_trend_participation(
        strategy_name="trend_participation_pilot_long",
        variant_id=variant.variant_id,
        bars_1m=bars_1m,
        trades=tuple(trades),
        slippage_points=0.25,
        fee_per_trade=1.50,
        data_version="layer1_pilot_synthetic_v1",
        code_version="pilot",
    )


def _build_forced_session_pilot_backtest():
    spec = ForcedNyEarlyShortSpec(
        variant_id="ny_forced_short_v1_breakdown_or_bar6",
        description="Prefer break below first-four-bar low; otherwise short bar 6 open.",
    )
    session_bars = {
        "2026-03-10": tuple(_forced_session_rows(0, decline=True)),
        "2026-03-11": tuple(_forced_session_rows(100, decline=False)),
    }
    session_trades = (
        ForcedSessionTrade(
            symbol="GC",
            trade_date="2026-03-10",
            variant_id=spec.variant_id,
            entered=True,
            entry_reason="breakdown",
            entry_bar_number=5,
            entry_end_ts=session_bars["2026-03-10"][5].end_ts.isoformat(),
            entry_price=101.0,
            stop_price=101.8,
            exit_bar_number=7,
            exit_end_ts=session_bars["2026-03-10"][7].end_ts.isoformat(),
            exit_price=99.8,
            exit_reason="ema_structure",
            pnl_points=1.2,
            net_pnl_points=1.0,
            gross_r_multiple=1.2,
            net_r_multiple=1.0,
            mae_points=0.2,
            mfe_points=1.4,
            setup_return_points=-0.5,
            setup_range_points=1.0,
            setup_close_location=0.2,
            setup_vwap_displacement=-0.4,
        ),
        ForcedSessionTrade(
            symbol="GC",
            trade_date="2026-03-11",
            variant_id=spec.variant_id,
            entered=True,
            entry_reason="fallback",
            entry_bar_number=6,
            entry_end_ts=session_bars["2026-03-11"][6].end_ts.isoformat(),
            entry_price=100.9,
            stop_price=101.5,
            exit_bar_number=8,
            exit_end_ts=session_bars["2026-03-11"][8].end_ts.isoformat(),
            exit_price=101.2,
            exit_reason="time_stop",
            pnl_points=-0.3,
            net_pnl_points=-0.5,
            gross_r_multiple=-0.3,
            net_r_multiple=-0.5,
            mae_points=0.6,
            mfe_points=0.2,
            setup_return_points=0.3,
            setup_range_points=0.8,
            setup_close_location=0.7,
            setup_vwap_displacement=0.2,
        ),
    )
    return build_strategy_backtest_from_forced_session(
        strategy_name="forced_session_pilot_short",
        symbol="GC",
        spec=spec,
        session_trades=session_trades,
        session_bars=session_bars,
        data_version="layer1_pilot_synthetic_v1",
        code_version="pilot",
    )


def _build_approved_quant_pilot_backtest(
    *,
    spec: ApprovedQuantLaneSpec,
    symbol_store: dict[str, dict[str, object]],
):
    evaluated_lane = evaluate_approved_lane(spec=spec, symbol_store=symbol_store)
    return build_strategy_backtest_from_approved_quant_lane(
        spec=spec,
        evaluated_lane=evaluated_lane,
        symbol_store=symbol_store,
        execution_timeframe="5m",
        data_version="approved_quant_layer1_pilot_synthetic_v1",
        code_version="pilot",
        strategy_name=f"approved_quant_lane_pilot::{spec.variant_id}",
        cost_basis_r=0.25,
    )


def _write_validation_bundle(*, subject_dir: Path, backtest: Any) -> tuple[Path, Path, Path]:
    strategy_path = subject_dir / "strategy_backtest.json"
    strategy_path.write_text(json.dumps(_json_ready(backtest), indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = run_validation_pipeline(backtest, build_debug_config())
    report_json_path = subject_dir / "validation_report.json"
    report_markdown_path = subject_dir / "validation_report.md"
    report_json_path.write_text(render_json_report(report), encoding="utf-8")
    report_markdown_path.write_text(render_markdown_report(report), encoding="utf-8")
    return strategy_path, report_json_path, report_markdown_path


def _write_validation_subject_bundle(
    *,
    subject_dir: Path,
    subject: ValidationSubject,
    history_payload: dict[str, Any] | None = None,
) -> dict[str, str]:
    paths: dict[str, str] = {}
    if subject.strategy_backtest is not None:
        strategy_path = subject_dir / "strategy_backtest.json"
        strategy_path.write_text(json.dumps(_json_ready(subject.strategy_backtest), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        paths["strategy_backtest_json"] = str(strategy_path)
    if subject.optimization_run is not None:
        optimization_path = subject_dir / "optimization_run.json"
        optimization_path.write_text(
            json.dumps(_json_ready(subject.optimization_run), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        paths["optimization_run_json"] = str(optimization_path)
    if history_payload is not None:
        history_path = subject_dir / "optimization_history.json"
        history_path.write_text(json.dumps(_json_ready(history_payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        paths["optimization_history_json"] = str(history_path)

    report = run_validation_pipeline(subject, build_debug_config())
    report_json_path = subject_dir / "validation_report.json"
    report_markdown_path = subject_dir / "validation_report.md"
    report_json_path.write_text(render_json_report(report), encoding="utf-8")
    report_markdown_path.write_text(render_markdown_report(report), encoding="utf-8")
    paths["validation_report_json"] = str(report_json_path)
    paths["validation_report_markdown"] = str(report_markdown_path)
    return paths


def _trend_bars() -> list[ResearchBar]:
    rows: list[ResearchBar] = []
    base = datetime(2026, 1, 5, 14, 0, tzinfo=UTC)
    prices = [
        (100.0, 100.2, 99.9, 100.1),
        (100.1, 100.3, 100.0, 100.25),
        (100.25, 100.45, 100.2, 100.4),
        (100.4, 100.55, 100.35, 100.5),
        (100.5, 100.8, 100.45, 100.75),
        (100.75, 101.0, 100.7, 100.95),
        (100.95, 101.2, 100.9, 101.1),
        (101.1, 101.3, 101.0, 101.15),
    ]
    for index, (open_, high, low, close) in enumerate(prices):
        start = base + timedelta(minutes=index)
        rows.append(
            ResearchBar(
                instrument="MES",
                timeframe="1m",
                start_ts=start,
                end_ts=start + timedelta(minutes=1),
                open=open_,
                high=high,
                low=low,
                close=close,
                volume=100,
                session_label="US_MIDDAY",
                session_segment="US",
                source="synthetic",
                provenance="layer1_pilot",
            )
        )
    return rows


def _trend_decisions() -> list[SignalDecision]:
    decision_ts = datetime(2026, 1, 5, 14, 2, tzinfo=UTC)
    return [
        SignalDecision(
            decision_id="trend-decision-1",
            instrument="MES",
            variant_id="trend_participation.breakout_continuation.long.active",
            family="breakout_continuation",
            side="LONG",
            strictness="active",
            decision_ts=decision_ts,
            session_date=decision_ts.date(),
            session_segment="US",
            regime_bucket="TREND_UP",
            volatility_bucket="NORMAL",
            conflict_outcome=ConflictOutcome.NO_CONFLICT,
            live_eligible=True,
            shadow_only=False,
            block_reason=None,
            decision_bar_high=100.45,
            decision_bar_low=100.2,
            decision_bar_close=100.4,
            decision_bar_open=100.25,
            average_range=0.2,
            setup_signature="layer1-pilot-setup",
            setup_state_signature="layer1-pilot-state",
            setup_quality_score=1.5,
            setup_quality_bucket="HIGH",
            feature_snapshot={"trend_state": "UP"},
        )
    ]


def _forced_session_rows(offset_minutes: int, *, decline: bool) -> list[ResearchBar]:
    base = datetime(2026, 3, 10, 14, 0, tzinfo=UTC) + timedelta(minutes=offset_minutes)
    closes = [101.5, 101.4, 101.3, 101.2, 101.0, 100.8, 100.2, 99.8, 99.7] if decline else [100.2, 100.4, 100.5, 100.7, 100.8, 100.9, 101.0, 101.1, 101.2]
    rows = []
    for index, close in enumerate(closes):
        open_ = close + (0.1 if decline else -0.1)
        high = max(open_, close) + 0.1
        low = min(open_, close) - 0.1
        end_ts = base + timedelta(minutes=3 * index + 3)
        rows.append(
            ResearchBar(
                instrument="GC",
                timeframe="3m",
                start_ts=end_ts - timedelta(minutes=3),
                end_ts=end_ts,
                open=open_,
                high=high,
                low=low,
                close=close,
                volume=100,
                session_label="US",
                session_segment="US",
                source="synthetic",
                provenance="layer1_pilot",
            )
        )
    return rows


def _approved_quant_symbol_store() -> dict[str, dict[str, object]]:
    store: dict[str, dict[str, object]] = {}
    for spec in approved_quant_lane_specs():
        for symbol in spec.symbols:
            store[symbol] = {
                "execution": _approved_quant_execution_frame(symbol=symbol, direction=spec.direction),
                "features": _approved_quant_features(spec=spec),
            }
    return store


def _approved_quant_execution_frame(*, symbol: str, direction: str) -> _FrameSeries:
    if direction == "LONG":
        bars = [
            _approved_quant_bar(symbol=symbol, index=0, open_=100.0, high=100.2, low=99.8, close=100.0, session="US"),
            _approved_quant_bar(symbol=symbol, index=1, open_=100.0, high=100.3, low=99.9, close=100.2, session="US"),
            _approved_quant_bar(symbol=symbol, index=2, open_=100.2, high=100.8, low=100.0, close=100.6, session="US"),
            _approved_quant_bar(symbol=symbol, index=3, open_=100.6, high=101.2, low=100.4, close=101.0, session="US"),
            _approved_quant_bar(symbol=symbol, index=4, open_=101.0, high=101.4, low=100.8, close=101.2, session="UNKNOWN"),
            _approved_quant_bar(symbol=symbol, index=5, open_=101.2, high=101.5, low=100.9, close=101.1, session="UNKNOWN"),
            _approved_quant_bar(symbol=symbol, index=6, open_=101.1, high=101.3, low=100.7, close=100.9, session="UNKNOWN"),
            _approved_quant_bar(symbol=symbol, index=7, open_=100.9, high=101.0, low=100.5, close=100.7, session="UNKNOWN"),
        ]
    else:
        bars = [
            _approved_quant_bar(symbol=symbol, index=0, open_=100.0, high=100.2, low=99.8, close=100.0, session="LONDON"),
            _approved_quant_bar(symbol=symbol, index=1, open_=100.0, high=100.1, low=99.7, close=99.8, session="LONDON"),
            _approved_quant_bar(symbol=symbol, index=2, open_=99.8, high=99.9, low=99.1, close=99.3, session="LONDON"),
            _approved_quant_bar(symbol=symbol, index=3, open_=99.3, high=99.4, low=98.6, close=98.9, session="LONDON"),
            _approved_quant_bar(symbol=symbol, index=4, open_=98.9, high=99.0, low=98.3, close=98.5, session="UNKNOWN"),
            _approved_quant_bar(symbol=symbol, index=5, open_=98.5, high=98.8, low=98.1, close=98.4, session="UNKNOWN"),
            _approved_quant_bar(symbol=symbol, index=6, open_=98.4, high=98.9, low=98.2, close=98.7, session="UNKNOWN"),
            _approved_quant_bar(symbol=symbol, index=7, open_=98.7, high=99.0, low=98.5, close=98.8, session="UNKNOWN"),
        ]
    return _FrameSeries.from_bars(bars)


def _approved_quant_bar(
    *,
    symbol: str,
    index: int,
    open_: float,
    high: float,
    low: float,
    close: float,
    session: str,
) -> Bar:
    start = datetime(2026, 3, 20, 12, 0, tzinfo=UTC) + timedelta(minutes=5 * index)
    end = start + timedelta(minutes=5)
    return Bar(
        bar_id=f"{symbol}-{index}",
        symbol=symbol,
        timeframe="5m",
        start_ts=start,
        end_ts=end,
        open=Decimal(str(open_)),
        high=Decimal(str(high)),
        low=Decimal(str(low)),
        close=Decimal(str(close)),
        volume=100,
        is_final=True,
        session_asia=session == "ASIA",
        session_london=session == "LONDON",
        session_us=session == "US",
        session_allowed=True,
    )


def _approved_quant_features(*, spec: ApprovedQuantLaneSpec) -> list[dict[str, object]]:
    if spec.direction == "LONG":
        sessions = ["US", "US", "US", "US", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"]
    else:
        sessions = ["LONDON", "LONDON", "LONDON", "LONDON", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"]
    rows: list[dict[str, object]] = []
    for index, session in enumerate(sessions):
        row: dict[str, object] = {
            "ready": index == 1,
            "risk_unit": 1.0,
            "session_label": session,
            "close_pos": 0.78 if spec.direction == "LONG" else 0.20,
        }
        if spec.direction == "LONG":
            row.update(
                {
                    "regime_up": True,
                    "compression_60": 0.50,
                    "compression_5": 0.45,
                    "breakout_up": 0.55,
                    "slope_60": 0.35,
                }
            )
        else:
            row.update(
                {
                    "failed_breakout_short": True,
                    "dist_240": 1.40,
                    "body_r": 0.45,
                }
            )
        rows.append(row)
    return rows


def _json_ready(value: Any) -> Any:
    if is_dataclass(value):
        return _json_ready(asdict(value))
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _render_asia_london_family_summary(payload: dict[str, Any]) -> str:
    lines = [
        "# Asia-London Participation Family Validation Summary",
        "",
        f"- Family: `{payload['family_name']}`",
        f"- Family judgeability: `{payload['family_judgeability']}`",
        f"- Core surface judgeability: `{payload['core_surface_judgeability']}`",
        f"- Candidate count: `{payload['candidate_count']}`",
        f"- Fully supported candidate count: `{payload['fully_supported_candidate_count']}`",
        "",
        "## Notes",
    ]
    lines.extend(f"- {note}" for note in payload["notes"])
    lines.extend(["", "## Candidates"])
    for row in payload["rows"]:
        lines.append(f"### {row['source_variant']}")
        lines.append(f"- Judgeability: `{row['judgeability']}`")
        lines.append(f"- Support status: `{row['support_status']}`")
        lines.append(f"- Optimization attached: `{row['optimization_history_attached']}`")
        if row.get("overall_status") is not None:
            lines.append(f"- Overall verdict: `{row['overall_status']}`")
        if row.get("trade_count") is not None:
            lines.append(f"- Trade count: `{row['trade_count']}`")
        if row.get("module_statuses"):
            lines.append("- Module statuses:")
            for module_name, status in sorted(row["module_statuses"].items()):
                lines.append(f"  - `{module_name}`: `{status}`")
        if row["evidence_missing"]:
            lines.append("- Missing evidence:")
            for item in row["evidence_missing"]:
                lines.append(f"  - `{item}`")
    return "\n".join(lines) + "\n"
