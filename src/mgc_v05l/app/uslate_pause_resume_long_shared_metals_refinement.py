"""Shared-metals refinement hypothesis pass for usLatePauseResumeLongTurn."""

from __future__ import annotations

import argparse
import json
import math
import tempfile
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import select

from ..app.replay_reporting import build_session_lookup, build_summary_metrics, build_trade_ledger, write_summary_metrics_json, write_trade_ledger_csv
from ..config_models import load_settings_from_files
from ..domain.enums import OrderIntentType
from ..domain.events import FillReceivedEvent, OrderIntentCreatedEvent
from ..domain.models import Bar
from ..persistence import build_engine
from ..persistence.repositories import RepositorySet
from ..persistence.tables import bars_table
from ..strategy.strategy_engine import StrategyEngine
from .mnq_usDerivativeBearTurn_validation import (
    IMPULSE_REFERENCE_PATH,
    REPLAY_DB_PATH,
    REPLAY_OUTPUT_DIR,
    REPO_ROOT,
    ReplayArtifacts,
    _compute_metrics,
    _load_feature_context,
    _load_impulse_reference,
    _load_reference_metrics,
    _load_rows,
)
from .uslate_pause_resume_long_cross_metal_anatomy import _bucket_rows, _bucket_summary


REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "approved_branch_research"
LATEST_JSON_PATH = REPORT_DIR / "usLatePauseResumeLongTurn_shared_metals_refinement.json"
LATEST_MD_PATH = REPORT_DIR / "usLatePauseResumeLongTurn_shared_metals_refinement.md"

TARGET_FAMILY = "usLatePauseResumeLongTurn"
TIMEFRAME = "5m"
FEE_PER_FILL = Decimal("0")
SLIPPAGE_PER_FILL = Decimal("0")

BASE_CONFIG_PREFIX = (
    REPO_ROOT / "config" / "base.yaml",
    REPO_ROOT / "config" / "replay.yaml",
    REPO_ROOT / "config" / "replay.research_control.yaml",
)
RAW_CONFIG = REPO_ROOT / "config" / "replay.us_late_pause_resume_long_pattern_v1.yaml"

MGC_REFERENCE = {
    "summary": REPO_ROOT / "outputs/replays/persisted_bar_replay_us_late_long_pattern_treatment_full_20260317.summary.json",
    "ledger": REPO_ROOT / "outputs/replays/persisted_bar_replay_us_late_long_pattern_treatment_full_20260317.trade_ledger.csv",
}
GC_REFERENCE = {
    "summary": REPO_ROOT / "outputs/replays/persisted_bar_replay_second_pass_direct_approved_gc_20260319_130545.summary.json",
    "ledger": REPO_ROOT / "outputs/replays/persisted_bar_replay_second_pass_direct_approved_gc_20260319_130545.trade_ledger.csv",
}
MGC_DERIVATIVE_REFERENCE = REPORT_DIR / "mgc_usDerivativeBearTurn_validation.json"

CACHED_VARIANT_ARTIFACTS: dict[tuple[str, str], dict[str, str]] = {
    ("MGC", "raw_baseline"): {
        "summary": str(MGC_REFERENCE["summary"]),
        "trade_ledger": str(MGC_REFERENCE["ledger"]),
    },
    ("MGC", "shared_entry_quality_cleanup"): {
        "summary": str(REPO_ROOT / "outputs/replays/persisted_bar_replay_mgc_uslate_shared_shared_entry_quality_cleanup_20260320_121329.summary.json"),
        "trade_ledger": str(REPO_ROOT / "outputs/replays/persisted_bar_replay_mgc_uslate_shared_shared_entry_quality_cleanup_20260320_121329.trade_ledger.csv"),
    },
    ("MGC", "shared_trap_state_exclusion"): {
        "replay_db": str(REPO_ROOT / "outputs/replays/persisted_bar_replay_mgc_uslate_shared_shared_trap_state_exclusion_20260320_121741.sqlite3"),
    },
    ("GC", "raw_baseline"): {
        "summary": str(GC_REFERENCE["summary"]),
        "trade_ledger": str(GC_REFERENCE["ledger"]),
    },
    ("GC", "shared_entry_quality_cleanup"): {
        "summary": str(REPO_ROOT / "outputs/replays/persisted_bar_replay_gc_uslate_refinement_tight_curvature_approved_gc_20260320_121015.summary.json"),
        "trade_ledger": str(REPO_ROOT / "outputs/replays/persisted_bar_replay_gc_uslate_refinement_tight_curvature_approved_gc_20260320_121015.trade_ledger.csv"),
    },
    ("GC", "shared_trap_state_exclusion"): {
        "summary": str(REPO_ROOT / "outputs/replays/persisted_bar_replay_gc_uslate_refinement_tight_curvature_and_expansion_approved_gc_20260320_121113.summary.json"),
        "trade_ledger": str(REPO_ROOT / "outputs/replays/persisted_bar_replay_gc_uslate_refinement_tight_curvature_and_expansion_approved_gc_20260320_121113.trade_ledger.csv"),
    },
}


@dataclass(frozen=True)
class VariantSpec:
    slug: str
    label: str
    rationale: str
    kind: str
    override_lines: tuple[str, ...] = ()


@dataclass(frozen=True)
class InstrumentSpec:
    symbol: str
    point_value: Decimal


VARIANTS = (
    VariantSpec(
        slug="raw_baseline",
        label="Raw baseline",
        rationale="Native family form with no extra cleanup.",
        kind="raw_baseline",
    ),
    VariantSpec(
        slug="shared_entry_quality_cleanup",
        label="Shared entry-quality cleanup",
        rationale="Require slightly stronger positive setup curvature and slightly stronger resumption curvature.",
        kind="entry_quality_cleanup",
        override_lines=(
            "us_late_pause_resume_long_setup_curvature_min: 0.20",
            "us_late_pause_resume_long_min_resumption_curvature: 0.20",
        ),
    ),
    VariantSpec(
        slug="shared_trap_state_exclusion",
        label="Shared trap-state exclusion",
        rationale="Keep the same curvature cleanup and add a slightly tighter range-expansion cap to reject more exhausted resumptions.",
        kind="trap_state_exclusion",
        override_lines=(
            "us_late_pause_resume_long_setup_curvature_min: 0.20",
            "us_late_pause_resume_long_min_resumption_curvature: 0.20",
            "us_late_pause_resume_long_max_range_expansion_ratio: 1.15",
        ),
    ),
)

INSTRUMENTS = (
    InstrumentSpec(symbol="MGC", point_value=Decimal("10")),
    InstrumentSpec(symbol="GC", point_value=Decimal("100")),
)


def build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(prog="uslate-pause-resume-long-shared-metals-refinement")


def main(argv: list[str] | None = None) -> int:
    build_parser().parse_args(argv)
    payload = build_and_write_shared_refinement_pass()
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_and_write_shared_refinement_pass() -> dict[str, Any]:
    results_by_instrument: dict[str, list[dict[str, Any]]] = {}
    for instrument in INSTRUMENTS:
        results_by_instrument[instrument.symbol] = [_load_or_run_variant(instrument, variant) for variant in VARIANTS]

    best_variant = _choose_best_shared_variant(results_by_instrument)
    mgc_derivative = _load_json(MGC_DERIVATIVE_REFERENCE)
    impulse_reference = _load_impulse_reference()

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "research_scope": "research_only",
        "family": TARGET_FAMILY,
        "instruments": [instrument.symbol for instrument in INSTRUMENTS],
        "refinements_tested": [
            {
                "slug": variant.slug,
                "label": variant.label,
                "kind": variant.kind,
                "rationale": variant.rationale,
                "override_lines": list(variant.override_lines),
            }
            for variant in VARIANTS
        ],
        "results_by_instrument": results_by_instrument,
        "best_shared_variant": best_variant,
        "comparisons": {
            "vs_raw_MGC_usLatePauseResumeLongTurn": _variant_lookup(results_by_instrument["MGC"], "raw_baseline")["metrics"],
            "vs_raw_GC_usLatePauseResumeLongTurn": _variant_lookup(results_by_instrument["GC"], "raw_baseline")["metrics"],
            "vs_MGC_usDerivativeBearTurn": mgc_derivative["economic_replay_quality"],
            "vs_parked_impulse_executable_reference": impulse_reference,
        },
        "cross_metal_answers": _cross_metal_answers(results_by_instrument, best_variant),
        "verdict_bucket": _verdict_bucket(results_by_instrument, best_variant),
        "direct_answers": _direct_answers(results_by_instrument, best_variant),
    }

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_JSON_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    LATEST_MD_PATH.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "usLatePauseResumeLongTurn_shared_metals_refinement_json": str(LATEST_JSON_PATH),
        "usLatePauseResumeLongTurn_shared_metals_refinement_md": str(LATEST_MD_PATH),
        "verdict_bucket": payload["verdict_bucket"],
        "best_shared_variant": best_variant["slug"],
    }


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _run_variant(instrument: InstrumentSpec, variant: VariantSpec) -> dict[str, Any]:
    stamp = f"{instrument.symbol.lower()}_uslate_shared_{variant.slug}_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
    artifacts = ReplayArtifacts(
        run_stamp=stamp,
        replay_db_path=REPLAY_OUTPUT_DIR / f"persisted_bar_replay_{stamp}.sqlite3",
        summary_path=REPLAY_OUTPUT_DIR / f"persisted_bar_replay_{stamp}.summary.json",
        summary_metrics_path=REPLAY_OUTPUT_DIR / f"persisted_bar_replay_{stamp}.summary_metrics.json",
        trade_ledger_path=REPLAY_OUTPUT_DIR / f"persisted_bar_replay_{stamp}.trade_ledger.csv",
    )

    config_paths = [str(path) for path in BASE_CONFIG_PREFIX] + [str(RAW_CONFIG)]
    override_path: Path | None = None
    try:
        if variant.override_lines:
            with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False, encoding="utf-8") as handle:
                handle.write("\n".join(variant.override_lines) + "\n")
                override_path = Path(handle.name)
            config_paths.append(str(override_path))

        settings = load_settings_from_files(config_paths)
        source_engine = build_engine(f"sqlite:///{REPLAY_DB_PATH}")
        with source_engine.begin() as connection:
            rows = connection.execute(
                select(bars_table)
                .where(
                    bars_table.c.ticker == instrument.symbol,
                    bars_table.c.timeframe == TIMEFRAME,
                )
                .order_by(bars_table.c.timestamp.asc())
            ).mappings().all()

        bars = [
            Bar(
                bar_id=row["bar_id"],
                symbol=row["symbol"],
                timeframe=row["timeframe"],
                start_ts=datetime.fromisoformat(row["start_ts"]),
                end_ts=datetime.fromisoformat(row["end_ts"]),
                open=Decimal(str(row["open"])),
                high=Decimal(str(row["high"])),
                low=Decimal(str(row["low"])),
                close=Decimal(str(row["close"])),
                volume=int(row["volume"]),
                is_final=bool(row["is_final"]),
                session_asia=bool(row["session_asia"]),
                session_london=bool(row["session_london"]),
                session_us=bool(row["session_us"]),
                session_allowed=bool(row["session_allowed"]),
            )
            for row in rows
        ]

        replay_settings = settings.model_copy(
            update={
                "symbol": instrument.symbol,
                "timeframe": TIMEFRAME,
                "database_url": f"sqlite:///{artifacts.replay_db_path}",
            }
        )
        repositories = RepositorySet(build_engine(replay_settings.database_url))
        strategy_engine = StrategyEngine(settings=replay_settings, repositories=repositories)
        event_counts: Counter[str] = Counter()
        started = datetime.now().astimezone()
        for bar in bars:
            for event in strategy_engine.process_bar(bar):
                if isinstance(event, OrderIntentCreatedEvent):
                    event_counts["order_intents"] += 1
                    if event.intent_type == OrderIntentType.BUY_TO_OPEN:
                        event_counts["long_entries"] += 1
                    elif event.intent_type == OrderIntentType.SELL_TO_OPEN:
                        event_counts["short_entries"] += 1
                    else:
                        event_counts["exits"] += 1
                elif isinstance(event, FillReceivedEvent):
                    event_counts["fills"] += 1

        session_by_start_ts = build_session_lookup(bars)
        feature_context_by_bar_id = _load_feature_context(repositories)
        trade_ledger = build_trade_ledger(
            repositories.order_intents.list_all(),
            repositories.fills.list_all(),
            session_by_start_ts,
            point_value=instrument.point_value,
            fee_per_fill=FEE_PER_FILL,
            slippage_per_fill=SLIPPAGE_PER_FILL,
            bars=bars,
            feature_context_by_bar_id=feature_context_by_bar_id,
        )
        summary_metrics = build_summary_metrics(trade_ledger)
        write_trade_ledger_csv(trade_ledger, artifacts.trade_ledger_path)
        write_summary_metrics_json(
            summary_metrics,
            artifacts.summary_metrics_path,
            point_value=instrument.point_value,
            fee_per_fill=FEE_PER_FILL,
            slippage_per_fill=SLIPPAGE_PER_FILL,
        )
        summary_payload = {
            "source_db_path": str(REPLAY_DB_PATH),
            "symbol": instrument.symbol,
            "timeframe": TIMEFRAME,
            "replay_db_path": str(artifacts.replay_db_path),
            "summary_path": str(artifacts.summary_path),
            "trade_ledger_path": str(artifacts.trade_ledger_path),
            "summary_metrics_path": str(artifacts.summary_metrics_path),
            "processed_bars": repositories.processed_bars.count(),
            "source_bar_count": len(bars),
            "source_first_bar_ts": bars[0].end_ts.isoformat(),
            "source_last_bar_ts": bars[-1].end_ts.isoformat(),
            "runtime_started_at": started.isoformat(),
            "event_counts": dict(event_counts),
            "config_paths": config_paths,
            "assumptions": {
                "point_value": float(instrument.point_value),
                "fee_per_fill": float(FEE_PER_FILL),
                "slippage_per_fill": float(SLIPPAGE_PER_FILL),
            },
        }
        artifacts.summary_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        family_rows = [row for row in _load_rows(artifacts.trade_ledger_path) if row["setup_family"] == TARGET_FAMILY]
        metrics = asdict(
            _compute_metrics(
                summary=summary_payload,
                rows=family_rows,
                sample_start=summary_payload["source_first_bar_ts"],
                sample_end=summary_payload["source_last_bar_ts"],
            )
        )
        anatomy = _anatomy_metrics(family_rows)
        return {
            "instrument": instrument.symbol,
            "slug": variant.slug,
            "label": variant.label,
            "kind": variant.kind,
            "rationale": variant.rationale,
            "metrics": metrics,
            "anatomy": anatomy,
            "replay_artifacts": {
                "summary": str(artifacts.summary_path),
                "summary_metrics": str(artifacts.summary_metrics_path),
                "trade_ledger": str(artifacts.trade_ledger_path),
                "replay_db": str(artifacts.replay_db_path),
            },
            "delta_vs_raw": None,
        }
    finally:
        if override_path and override_path.exists():
            override_path.unlink()


def _load_or_run_variant(instrument: InstrumentSpec, variant: VariantSpec) -> dict[str, Any]:
    cached = CACHED_VARIANT_ARTIFACTS.get((instrument.symbol, variant.slug))
    if cached:
        return _load_cached_variant(instrument, variant, cached)
    return _run_variant(instrument, variant)


def _load_cached_variant(instrument: InstrumentSpec, variant: VariantSpec, cached: dict[str, str]) -> dict[str, Any]:
    if "summary" in cached and "trade_ledger" in cached:
        summary_path = Path(cached["summary"])
        ledger_path = Path(cached["trade_ledger"])
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        family_rows = [row for row in _load_rows(ledger_path) if row["setup_family"] == TARGET_FAMILY]
        metrics = asdict(
            _compute_metrics(
                summary=summary,
                rows=family_rows,
                sample_start=summary.get("slice_start_ts") or summary.get("source_first_bar_ts"),
                sample_end=summary.get("slice_end_ts") or summary.get("source_last_bar_ts"),
            )
        )
        anatomy = _anatomy_metrics(family_rows)
        return {
            "instrument": instrument.symbol,
            "slug": variant.slug,
            "label": variant.label,
            "kind": variant.kind,
            "rationale": variant.rationale,
            "metrics": metrics,
            "anatomy": anatomy,
            "replay_artifacts": {
                "summary": str(summary_path),
                "summary_metrics": str(summary_path).replace(".summary.json", ".summary_metrics.json"),
                "trade_ledger": str(ledger_path),
                "replay_db": str(summary_path).replace(".summary.json", ".sqlite3"),
            },
            "delta_vs_raw": None,
        }

    replay_db_path = Path(cached["replay_db"])
    repositories = RepositorySet(build_engine(f"sqlite:///{replay_db_path}"))
    source_engine = build_engine(f"sqlite:///{REPLAY_DB_PATH}")
    with source_engine.begin() as connection:
        rows = connection.execute(
            select(bars_table)
            .where(
                bars_table.c.ticker == instrument.symbol,
                bars_table.c.timeframe == TIMEFRAME,
            )
            .order_by(bars_table.c.timestamp.asc())
        ).mappings().all()
    bars = [
        Bar(
            bar_id=row["bar_id"],
            symbol=row["symbol"],
            timeframe=row["timeframe"],
            start_ts=datetime.fromisoformat(row["start_ts"]),
            end_ts=datetime.fromisoformat(row["end_ts"]),
            open=Decimal(str(row["open"])),
            high=Decimal(str(row["high"])),
            low=Decimal(str(row["low"])),
            close=Decimal(str(row["close"])),
            volume=int(row["volume"]),
            is_final=bool(row["is_final"]),
            session_asia=bool(row["session_asia"]),
            session_london=bool(row["session_london"]),
            session_us=bool(row["session_us"]),
            session_allowed=bool(row["session_allowed"]),
        )
        for row in rows
    ]
    session_by_start_ts = build_session_lookup(bars)
    feature_context_by_bar_id = _load_feature_context(repositories)
    trade_ledger = build_trade_ledger(
        repositories.order_intents.list_all(),
        repositories.fills.list_all(),
        session_by_start_ts,
        point_value=instrument.point_value,
        fee_per_fill=FEE_PER_FILL,
        slippage_per_fill=SLIPPAGE_PER_FILL,
        bars=bars,
        feature_context_by_bar_id=feature_context_by_bar_id,
    )
    family_rows = [
        {
            "entry_ts": row.entry_ts.isoformat(),
            "exit_ts": row.exit_ts.isoformat(),
            "net_pnl": str(row.net_pnl),
            "setup_family": row.setup_family,
            "exit_reason": row.exit_reason,
            "initial_adverse_3bar": str(row.initial_adverse_3bar),
            "initial_favorable_3bar": str(row.initial_favorable_3bar),
            "entry_efficiency_5": str(row.entry_efficiency_5),
            "mae": str(row.mae),
            "mfe": str(row.mfe),
            "bars_held": str(row.bars_held),
        }
        for row in trade_ledger
        if row.setup_family == TARGET_FAMILY
    ]
    summary = {
        "source_first_bar_ts": bars[0].end_ts.isoformat(),
        "source_last_bar_ts": bars[-1].end_ts.isoformat(),
    }
    metrics = asdict(
        _compute_metrics(
            summary=summary,
            rows=family_rows,
            sample_start=summary["source_first_bar_ts"],
            sample_end=summary["source_last_bar_ts"],
        )
    )
    anatomy = _anatomy_metrics(family_rows)
    return {
        "instrument": instrument.symbol,
        "slug": variant.slug,
        "label": variant.label,
        "kind": variant.kind,
        "rationale": variant.rationale,
        "metrics": metrics,
        "anatomy": anatomy,
        "replay_artifacts": {
            "summary": None,
            "summary_metrics": None,
            "trade_ledger": None,
            "replay_db": str(replay_db_path),
        },
        "delta_vs_raw": None,
    }


def _anatomy_metrics(rows: list[dict[str, str]]) -> dict[str, Any]:
    parsed_rows = []
    for row in rows:
        parsed = dict(row)
        for key in (
            "net_pnl",
            "mae",
            "mfe",
            "bars_held",
            "initial_adverse_3bar",
            "initial_favorable_3bar",
            "entry_efficiency_5",
        ):
            parsed[key] = float(parsed[key])
        entry_ts = datetime.fromisoformat(parsed["entry_ts"])
        hour = entry_ts.hour + entry_ts.minute / 60.0
        if 14 <= hour < 15:
            parsed["sub_pocket"] = "14:00-15:00 ET"
        elif 15 <= hour < 16:
            parsed["sub_pocket"] = "15:00-16:00 ET"
        elif 16 <= hour < 17:
            parsed["sub_pocket"] = "16:00-17:00 ET"
        else:
            parsed["sub_pocket"] = "Outside 14:00-17:00 ET"
        parsed_rows.append(parsed)
    buckets = _bucket_rows(parsed_rows)
    top3 = sorted((row["net_pnl"] for row in parsed_rows if row["net_pnl"] > 0), reverse=True)[:3]
    middle_pnl = round(sum(row["net_pnl"] for row in parsed_rows) - sum(top3), 4)
    fragile_summary = _bucket_summary(buckets["fragile_losers"])
    return {
        "middle_pnl_ex_top3": middle_pnl,
        "fragile_loser_count": len(buckets["fragile_losers"]),
        "fragile_loser_realized_pnl": fragile_summary.get("realized_pnl"),
        "fragile_loser_mean_initial_adverse_3bar": fragile_summary.get("mean_initial_adverse_3bar"),
        "fragile_loser_mean_entry_efficiency_5": fragile_summary.get("mean_entry_efficiency_5"),
    }


def _variant_lookup(rows: list[dict[str, Any]], slug: str) -> dict[str, Any]:
    return next(row for row in rows if row["slug"] == slug)


def _add_deltas(results_by_instrument: dict[str, list[dict[str, Any]]]) -> None:
    for instrument, rows in results_by_instrument.items():
        baseline = _variant_lookup(rows, "raw_baseline")
        for row in rows:
            row["delta_vs_raw"] = _delta_vs_raw(row, baseline)


def _delta_vs_raw(row: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    metrics = row["metrics"]
    base_metrics = baseline["metrics"]
    anatomy = row["anatomy"]
    base_anatomy = baseline["anatomy"]

    def delta(left: Any, right: Any) -> float | None:
        if left is None or right is None:
            return None
        return round(float(left) - float(right), 4)

    return {
        "trade_count_delta": delta(metrics["trades"], base_metrics["trades"]),
        "pnl_delta": delta(metrics["realized_pnl"], base_metrics["realized_pnl"]),
        "median_trade_delta": delta(metrics["median_trade"], base_metrics["median_trade"]),
        "profit_factor_delta": delta(metrics["profit_factor"], base_metrics["profit_factor"]),
        "drawdown_delta": delta(metrics["max_drawdown"], base_metrics["max_drawdown"]),
        "top_1_contribution_delta": delta(metrics["top_1_contribution"], base_metrics["top_1_contribution"]),
        "top_3_contribution_delta": delta(metrics["top_3_contribution"], base_metrics["top_3_contribution"]),
        "middle_pnl_ex_top3_delta": delta(anatomy["middle_pnl_ex_top3"], base_anatomy["middle_pnl_ex_top3"]),
        "fragile_loser_pnl_delta": delta(anatomy["fragile_loser_realized_pnl"], base_anatomy["fragile_loser_realized_pnl"]),
        "fragile_loser_initial_adverse_delta": delta(
            anatomy["fragile_loser_mean_initial_adverse_3bar"],
            base_anatomy["fragile_loser_mean_initial_adverse_3bar"],
        ),
    }


def _score_variant(row: dict[str, Any], baseline: dict[str, Any]) -> tuple[float, ...]:
    metrics = row["metrics"]
    base_metrics = baseline["metrics"]
    anatomy = row["anatomy"]
    base_anatomy = baseline["anatomy"]
    realized = float(metrics.get("realized_pnl") or 0.0)
    base_realized = float(base_metrics.get("realized_pnl") or 0.0)
    pf = float(metrics.get("profit_factor") or 0.0)
    base_pf = float(base_metrics.get("profit_factor") or 0.0)
    top1_improvement = float(base_metrics.get("top_1_contribution") or 0.0) - float(metrics.get("top_1_contribution") or 0.0)
    top3_improvement = float(base_metrics.get("top_3_contribution") or 0.0) - float(metrics.get("top_3_contribution") or 0.0)
    middle_improvement = float(anatomy.get("middle_pnl_ex_top3") or 0.0) - float(base_anatomy.get("middle_pnl_ex_top3") or 0.0)
    poison_improvement = abs(float(base_anatomy.get("fragile_loser_realized_pnl") or 0.0)) - abs(float(anatomy.get("fragile_loser_realized_pnl") or 0.0))
    drawdown_improvement = float(base_metrics.get("max_drawdown") or 0.0) - float(metrics.get("max_drawdown") or 0.0)
    pnl_retention = (realized / base_realized) if base_realized else 0.0
    score = (
        top1_improvement
        + top3_improvement
        + middle_improvement * 0.5
        + poison_improvement * 0.3
        + drawdown_improvement * 0.1
        + (pf - base_pf) * 40.0
    )
    if pnl_retention < 0.8:
        score -= 100.0
    if row["slug"] == "raw_baseline":
        score -= 2.0
    return (
        score,
        pnl_retention,
        top3_improvement,
        middle_improvement,
    )


def _choose_best_shared_variant(results_by_instrument: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    _add_deltas(results_by_instrument)
    scored: list[tuple[tuple[float, ...], dict[str, Any]]] = []
    for variant in VARIANTS:
        per_instrument = {
            instrument.symbol: _variant_lookup(results_by_instrument[instrument.symbol], variant.slug)
            for instrument in INSTRUMENTS
        }
        instrument_scores = [
            _score_variant(per_instrument[instrument.symbol], _variant_lookup(results_by_instrument[instrument.symbol], "raw_baseline"))
            for instrument in INSTRUMENTS
        ]
        aggregate_score = (
            sum(score[0] for score in instrument_scores),
            min(score[1] for score in instrument_scores),
            sum(score[2] for score in instrument_scores),
            sum(score[3] for score in instrument_scores),
        )
        scored.append(
            (
                aggregate_score,
                {
                    "slug": variant.slug,
                    "label": variant.label,
                    "kind": variant.kind,
                    "rationale": variant.rationale,
                    "per_instrument": per_instrument,
                },
            )
        )
    return max(scored, key=lambda item: item[0])[1]


def _material_improvement(row: dict[str, Any], baseline: dict[str, Any]) -> bool:
    metrics = row["metrics"]
    base_metrics = baseline["metrics"]
    anatomy = row["anatomy"]
    base_anatomy = baseline["anatomy"]
    realized = float(metrics.get("realized_pnl") or 0.0)
    base_realized = float(base_metrics.get("realized_pnl") or 0.0)
    if base_realized <= 0 or realized / base_realized < 0.8:
        return False
    if (float(metrics.get("profit_factor") or 0.0) + 0.2) < float(base_metrics.get("profit_factor") or 0.0):
        return False
    top3_improvement = float(base_metrics.get("top_3_contribution") or 0.0) - float(metrics.get("top_3_contribution") or 0.0)
    middle_improvement = float(anatomy.get("middle_pnl_ex_top3") or 0.0) - float(base_anatomy.get("middle_pnl_ex_top3") or 0.0)
    poison_improvement = abs(float(base_anatomy.get("fragile_loser_realized_pnl") or 0.0)) - abs(float(anatomy.get("fragile_loser_realized_pnl") or 0.0))
    return top3_improvement >= 10.0 or middle_improvement > 0.0 or poison_improvement > 0.0


def _verdict_bucket(results_by_instrument: dict[str, list[dict[str, Any]]], best_variant: dict[str, Any]) -> str:
    gc_raw = _variant_lookup(results_by_instrument["GC"], "raw_baseline")
    mgc_raw = _variant_lookup(results_by_instrument["MGC"], "raw_baseline")
    gc_trap = next((row for row in results_by_instrument["GC"] if row["slug"] == "shared_trap_state_exclusion"), None)
    mgc_trap = next((row for row in results_by_instrument["MGC"] if row["slug"] == "shared_trap_state_exclusion"), None)
    gc_specific_signal = bool(
        gc_trap
        and mgc_trap
        and (gc_trap["metrics"]["max_drawdown"] or 0.0) < (gc_raw["metrics"].get("max_drawdown") or 0.0)
        and (gc_trap["anatomy"]["middle_pnl_ex_top3"] or 0.0) > (gc_raw["anatomy"]["middle_pnl_ex_top3"] or 0.0)
        and abs(gc_trap["anatomy"]["fragile_loser_realized_pnl"] or 0.0) < abs(gc_raw["anatomy"]["fragile_loser_realized_pnl"] or 0.0)
        and (mgc_trap["metrics"]["realized_pnl"] or 0.0) < (mgc_raw["metrics"]["realized_pnl"] or 0.0)
    )
    if best_variant["slug"] == "raw_baseline":
        return "INSTRUMENT_SPECIFIC_CLEANUP_NOW_JUSTIFIED" if gc_specific_signal else "RAW_BASELINE_REMAINS_BEST"
    improvements = []
    for instrument in INSTRUMENTS:
        baseline = _variant_lookup(results_by_instrument[instrument.symbol], "raw_baseline")
        improvements.append(_material_improvement(best_variant["per_instrument"][instrument.symbol], baseline))
    if all(improvements):
        return "SHARED_REFINEMENT_WORKS"
    if any(improvements):
        return "SHARED_REFINEMENT_IMPROVES_BUT_NOT_ENOUGH"
    return "RAW_BASELINE_REMAINS_BEST"


def _cross_metal_answers(results_by_instrument: dict[str, list[dict[str, Any]]], best_variant: dict[str, Any]) -> dict[str, Any]:
    answers: dict[str, Any] = {}
    for instrument in INSTRUMENTS:
        symbol = instrument.symbol
        baseline = _variant_lookup(results_by_instrument[symbol], "raw_baseline")
        best = best_variant["per_instrument"][symbol]
        answers[symbol] = {
            "did_shared_refinement_materially_help": _material_improvement(best, baseline),
            "improved_weak_middle": (best["anatomy"]["middle_pnl_ex_top3"] or 0.0) > (baseline["anatomy"]["middle_pnl_ex_top3"] or 0.0),
            "reduced_early_poison_loser_damage": abs(best["anatomy"]["fragile_loser_realized_pnl"] or 0.0) < abs(
                baseline["anatomy"]["fragile_loser_realized_pnl"] or 0.0
            ),
            "overcleaned_family": (best["metrics"]["trades"] or 0) < max(1, math.floor((baseline["metrics"]["trades"] or 0) * 0.65)),
        }
    return answers


def _direct_answers(results_by_instrument: dict[str, list[dict[str, Any]]], best_variant: dict[str, Any]) -> dict[str, Any]:
    verdict = _verdict_bucket(results_by_instrument, best_variant)
    mgc_help = _cross_metal_answers(results_by_instrument, best_variant)["MGC"]["did_shared_refinement_materially_help"]
    gc_help = _cross_metal_answers(results_by_instrument, best_variant)["GC"]["did_shared_refinement_materially_help"]
    if verdict in {"RAW_BASELINE_REMAINS_BEST", "INSTRUMENT_SPECIFIC_CLEANUP_NOW_JUSTIFIED"}:
        single_best = "No shared refinement beat the raw family. Raw baseline remains best."
    else:
        single_best = f"{best_variant['label']} ({best_variant['slug']})"
    if verdict == "SHARED_REFINEMENT_WORKS":
        next_pass = "Stay shared one more step before splitting."
    elif verdict == "INSTRUMENT_SPECIFIC_CLEANUP_NOW_JUSTIFIED":
        next_pass = "Split into GC-specific cleanup. MGC should stay on the raw family unless a new shared idea emerges."
    elif gc_help and not mgc_help:
        next_pass = "Split into GC-specific cleanup only after this shared pass."
    else:
        next_pass = "Stay shared for interpretation; instrument-specific cleanup is not yet justified."
    return {
        "single_best_shared_refinement": single_best,
        "did_it_help_both_mgc_and_gc_or_mostly_one": (
            "Both metals improved materially." if mgc_help and gc_help else "Mostly one metal, not both."
        ),
        "is_the_family_now_more_robust_or_just_cosmetically_improved": (
            "More robust." if verdict == "SHARED_REFINEMENT_WORKS" else "Only modestly improved or still baseline-dominated."
        ),
        "should_the_next_pass_stay_shared_or_split_into_gc_specific_cleanup": next_pass,
        "single_biggest_blocker_still_remaining": (
            "The middle of the distribution is still too weak relative to standout trend days."
            if verdict != "SHARED_REFINEMENT_WORKS"
            else "Concentration is improved, but the family still needs paper-design discipline."
        ),
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# usLatePauseResumeLongTurn Shared Metals Refinement",
        "",
        "## Variants",
    ]
    for variant in payload["refinements_tested"]:
        lines.append(f"- `{variant['slug']}`: {variant['rationale']}")
    lines.extend(["", "## Results"])
    for instrument, rows in payload["results_by_instrument"].items():
        lines.append(f"- {instrument}:")
        for row in rows:
            metrics = row["metrics"]
            lines.append(
                f"  - `{row['slug']}` trades `{metrics['trades']}`, pnl `{metrics['realized_pnl']}`, median `{metrics['median_trade']}`, PF `{metrics['profit_factor']}`, top-3 `{metrics['top_3_contribution']}`"
            )
    lines.extend(
        [
            "",
            "## Verdict",
            f"- `{payload['verdict_bucket']}`",
            "",
            "## Direct Answers",
            f"1. {payload['direct_answers']['single_best_shared_refinement']}",
            f"2. {payload['direct_answers']['did_it_help_both_mgc_and_gc_or_mostly_one']}",
            f"3. {payload['direct_answers']['is_the_family_now_more_robust_or_just_cosmetically_improved']}",
            f"4. {payload['direct_answers']['should_the_next_pass_stay_shared_or_split_into_gc_specific_cleanup']}",
            f"5. {payload['direct_answers']['single_biggest_blocker_still_remaining']}",
        ]
    )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    main()
