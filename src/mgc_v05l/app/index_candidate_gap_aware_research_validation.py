"""Gap-aware research validation for selected index-futures candidates.

This module is research/offline only. It reads Databento historical research
Parquet partitions and session coverage masks, then runs replay-mode strategy
logic without touching runtime, preflight, dashboard, PAPER, or broker paths.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from mgc_v05l.app.databento_research_minute_backfill import SOURCE as BACKFILL_SOURCE
from mgc_v05l.app.databento_research_minute_backfill import build_layout
from mgc_v05l.app.replay_reporting import build_session_lookup, build_trade_ledger
from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.domain.enums import OrderIntentType
from mgc_v05l.domain.events import FillReceivedEvent, OrderIntentCreatedEvent
from mgc_v05l.domain.models import Bar
from mgc_v05l.persistence import build_engine
from mgc_v05l.persistence.repositories import RepositorySet
from mgc_v05l.research.trend_participation.storage import write_storage_manifest
from mgc_v05l.strategy.strategy_engine import StrategyEngine


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = Path("outputs") / "reports" / "trend_participation_engine"
DEFAULT_REPORT_DIR = Path("outputs") / "reports" / "approved_branch_research"
AUDIT_SOURCE = "INDEX_CANDIDATE_GAP_AWARE_RESEARCH_VALIDATION"
TIMEFRAME_1M = "1m"
DECISION_TIMEFRAME = "5m"
SESSION_TIMEZONE = ZoneInfo("America/New_York")
TARGETS: tuple[tuple[str, str], ...] = (
    ("MNQ", "usDerivativeBearTurn"),
    ("MES", "firstBearSnapTurn"),
)
POINT_VALUES: dict[str, Decimal] = {"MNQ": Decimal("2"), "MES": Decimal("5")}


@dataclass(frozen=True)
class IndexCandidateValidationConfig:
    repo_root: Path = REPO_ROOT
    output_root: Path = DEFAULT_OUTPUT_ROOT
    report_dir: Path = DEFAULT_REPORT_DIR
    targets: tuple[tuple[str, str], ...] = TARGETS
    fee_per_fill: Decimal = Decimal("0")
    slippage_per_fill: Decimal = Decimal("0")
    write_trade_ledgers: bool = True


@dataclass(frozen=True)
class ValidationRunResult:
    report: dict[str, Any]
    report_paths: tuple[Path, ...]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="index-candidate-gap-aware-research-validation")
    parser.add_argument(
        "--target",
        action="append",
        default=None,
        help="Target as SYMBOL:FAMILY. Defaults to MNQ:usDerivativeBearTurn and MES:firstBearSnapTurn.",
    )
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--no-trade-ledgers", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    targets = _parse_targets(args.target) if args.target else TARGETS
    result = run_index_candidate_gap_aware_validation(
        config=IndexCandidateValidationConfig(
            repo_root=Path(args.repo_root),
            output_root=Path(args.output_root),
            report_dir=Path(args.report_dir),
            targets=targets,
            write_trade_ledgers=not args.no_trade_ledgers,
        )
    )
    print(
        json.dumps(
            {
                "final_classification": result.report["final_classification"],
                "target_count": result.report["target_count"],
                "report_paths": [str(path) for path in result.report_paths],
                "runtime_pretrade_unchanged": result.report["runtime_pretrade_unchanged"],
                "live_money_eligible": result.report["live_money_eligible"],
            },
            sort_keys=True,
        )
    )
    return 0


def run_index_candidate_gap_aware_validation(*, config: IndexCandidateValidationConfig) -> ValidationRunResult:
    rows: list[dict[str, Any]] = []
    ledgers_by_target: dict[str, list[dict[str, Any]]] = {}
    for symbol, family in config.targets:
        target_result = _run_target(symbol=symbol.upper(), family=family, config=config)
        rows.append(target_result["summary"])
        ledgers_by_target[target_result["target_key"]] = target_result["ledger_rows"]

    report = {
        "schema_version": "index_candidate_gap_aware_research_validation_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": AUDIT_SOURCE,
        "research_only": True,
        "runtime_artifact": False,
        "runtime_pretrade_unchanged": True,
        "can_submit": False,
        "live_money_eligible": False,
        "broker_state_touched": False,
        "targets": rows,
        "target_count": len(rows),
        "final_classification": "INDEX_CANDIDATE_GAP_AWARE_RESEARCH_VALIDATION_COMPLETE",
    }
    return _write_reports(config=config, report=report, ledgers_by_target=ledgers_by_target)


def _run_target(*, symbol: str, family: str, config: IndexCandidateValidationConfig) -> dict[str, Any]:
    coverage_report = _load_coverage_report(config=config, symbol=symbol)
    session_mask = _session_mask_from_report(coverage_report)
    raw_rows = _load_research_1m_rows(config=config, symbol=symbol)
    bars_5m = _derive_decision_bars(symbol=symbol, rows=raw_rows, config=config)
    replay = _run_replay(symbol=symbol, family=family, bars=bars_5m, config=config)
    target_rows = [row for row in replay["ledger_rows"] if row["setup_family"] == family]
    annotated_rows = [_annotate_trade(row, session_mask=session_mask) for row in target_rows]

    eligible_rows = [row for row in annotated_rows if row["eligible_for_replay"] is True]
    ineligible_rows = [row for row in annotated_rows if row["eligible_for_replay"] is False]
    session_summary = coverage_report.get("session_coverage_summary") or {}
    summary = {
        "symbol": symbol,
        "strategy_family": family,
        "research_scope": "research_only_gap_aware_replay",
        "data_source": BACKFILL_SOURCE,
        "input_timeframe": TIMEFRAME_1M,
        "decision_timeframe": DECISION_TIMEFRAME,
        "can_consume_1m_parquet_research_bars": True,
        "decision_bar_derivation": "5m_from_1m_research_parquet",
        "session_coverage_mask_used": True,
        "session_coverage_report_path": str(_coverage_report_path(config=config, symbol=symbol)),
        "coverage": {
            "complete_partitions": coverage_report.get("complete_partition_count"),
            "empty_partitions": coverage_report.get("empty_partition_count"),
            "failed_partitions": coverage_report.get("failed_partition_count"),
            "total_1m_rows": coverage_report.get("total_row_count"),
            "earliest_actual_bar": coverage_report.get("earliest_actual_bar"),
            "latest_actual_bar": coverage_report.get("latest_actual_bar"),
            "session_coverage_rows": session_summary.get("row_count"),
            "eligible_session_rows": session_summary.get("eligible_count"),
            "eligible_by_session": {
                key: (session_summary.get("by_session") or {}).get(key, {}).get("eligible_count", 0)
                for key in ("ASIA", "LONDON", "US")
            },
            "gap_classification_counts": coverage_report.get("gap_classification_counts") or {},
            "suspicious_gap_count": coverage_report.get("suspicious_gap_count"),
            "schema_consistent": coverage_report.get("schema_consistent"),
            "research_artifact": coverage_report.get("research_artifact"),
            "runtime_artifact": coverage_report.get("runtime_artifact"),
        },
        "replay": {
            "decision_bars": len(bars_5m),
            "first_decision_bar": bars_5m[0].end_ts.isoformat() if bars_5m else None,
            "last_decision_bar": bars_5m[-1].end_ts.isoformat() if bars_5m else None,
            "event_counts": replay["event_counts"],
            "all_available_sessions_diagnostic_only": _summarize_trade_rows(annotated_rows),
            "eligible_sessions_only": _summarize_trade_rows(eligible_rows),
            "ineligible_sessions_diagnostic_only": _summarize_trade_rows(ineligible_rows),
            "by_session": _segment_trade_rows(annotated_rows, key="entry_session"),
            "by_month": _segment_trade_rows(annotated_rows, key="entry_month"),
            "by_eligibility": _segment_trade_rows(annotated_rows, key="eligibility_bucket"),
            "sensitivity_to_exclusion_thresholds": _threshold_sensitivity(annotated_rows, coverage_report),
        },
        "prior_research_status": _prior_research_status(symbol=symbol, family=family),
        "robustness_classification": _robustness_classification(symbol=symbol, rows=eligible_rows),
        "runtime_pretrade_unchanged": True,
        "can_submit": False,
        "live_money_eligible": False,
    }
    return {
        "target_key": f"{symbol}_{family}",
        "summary": summary,
        "ledger_rows": annotated_rows,
    }


def _run_replay(
    *,
    symbol: str,
    family: str,
    bars: Sequence[Bar],
    config: IndexCandidateValidationConfig,
) -> dict[str, Any]:
    settings = _settings_for_target(symbol=symbol, family=family, config=config)
    repositories = RepositorySet(build_engine("sqlite:///:memory:"))
    _disable_non_ledger_repository_writes(repositories)
    strategy_engine = StrategyEngine(settings=settings, repositories=repositories)
    if strategy_engine._state_repository is not None:
        strategy_engine._state_repository.save_snapshot = lambda *args, **kwargs: None
    event_counts: Counter[str] = Counter()
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
    ledger = build_trade_ledger(
        repositories.order_intents.list_all(),
        repositories.fills.list_all(),
        build_session_lookup(bars),
        point_value=POINT_VALUES[symbol],
        fee_per_fill=config.fee_per_fill,
        slippage_per_fill=config.slippage_per_fill,
        bars=bars,
    )
    return {
        "event_counts": dict(event_counts),
        "ledger_rows": [_ledger_row_to_dict(row) for row in ledger],
    }


def _disable_non_ledger_repository_writes(repositories: RepositorySet) -> None:
    """Avoid unnecessary SQLite writes while retaining intents/fills for ledgers."""
    repositories.bars.save = lambda *args, **kwargs: None
    repositories.features.save = lambda *args, **kwargs: None
    repositories.signals.save = lambda *args, **kwargs: None
    repositories.processed_bars.mark_processed = lambda *args, **kwargs: None
    repositories.alerts.save = lambda *args, **kwargs: None
    repositories.fault_events.save = lambda *args, **kwargs: None


def _settings_for_target(*, symbol: str, family: str, config: IndexCandidateValidationConfig):
    config_paths = [
        config.repo_root / "config" / "base.yaml",
        config.repo_root / "config" / "replay.yaml",
        config.repo_root / "config" / "replay.research_control.yaml",
    ]
    if family == "usDerivativeBearTurn":
        config_paths.append(config.repo_root / "config" / "replay.retest_us_derivative_bear_widen_1.yaml")
    settings = load_settings_from_files(config_paths)
    updates: dict[str, Any] = {
        "symbol": symbol,
        "timeframe": DECISION_TIMEFRAME,
        "database_url": "sqlite:///:memory:",
        "probationary_enforce_approved_branches": False,
    }
    if family == "usDerivativeBearTurn":
        updates.update(
            {
                "enable_bull_snap_longs": False,
                "enable_asia_vwap_longs": False,
                "enable_us_derivative_bear_shorts": True,
                "enable_us_derivative_bear_additive_shorts": False,
                "enable_us_midday_pause_resume_shorts": False,
                "enable_london_late_pause_resume_shorts": False,
                "enable_asia_early_pause_resume_shorts": False,
            }
        )
    elif family == "firstBearSnapTurn":
        updates.update(
            {
                "enable_bull_snap_longs": False,
                "enable_asia_vwap_longs": False,
                "enable_bear_snap_shorts": True,
                "enable_us_derivative_bear_shorts": False,
                "enable_us_derivative_bear_additive_shorts": False,
                "enable_us_midday_pause_resume_shorts": False,
                "enable_london_late_pause_resume_shorts": False,
                "enable_asia_early_pause_resume_shorts": False,
            }
        )
    else:
        raise ValueError(f"Unsupported target family for this validator: {family}")
    return settings.model_copy(update=updates)


def _load_research_1m_rows(*, config: IndexCandidateValidationConfig, symbol: str) -> list[dict[str, Any]]:
    raw_root = build_layout(_output_root(config))["raw"] / "databento_minute_backfill" / f"symbol={symbol}"
    paths = sorted(raw_root.glob("year=*/month=*/bars.parquet"))
    rows: list[dict[str, Any]] = []
    for path in paths:
        for row in pq.ParquetFile(path).read().to_pylist():
            if str(row.get("symbol") or "").upper() != symbol:
                raise ValueError(f"Wrong symbol in research partition {path}: {row.get('symbol')}")
            if str(row.get("timeframe") or "").lower() != TIMEFRAME_1M:
                raise ValueError(f"Wrong timeframe in research partition {path}: {row.get('timeframe')}")
            if row.get("research_artifact") is not True or row.get("runtime_artifact") is not False:
                raise ValueError(f"Unsafe artifact flags in research partition {path}")
            rows.append(row)
    rows.sort(key=lambda item: _coerce_datetime(item["bar_end"]))
    return rows


def _derive_decision_bars(
    *,
    symbol: str,
    rows: Sequence[dict[str, Any]],
    config: IndexCandidateValidationConfig,
) -> list[Bar]:
    settings = _settings_for_target(symbol=symbol, family="firstBearSnapTurn", config=config)
    grouped: dict[datetime, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        end = _coerce_datetime(row["bar_end"])
        grouped[_ceil_to_minutes(end, 5)].append(row)
    bars: list[Bar] = []
    for end_ts in sorted(grouped):
        bucket = sorted(grouped[end_ts], key=lambda item: _coerce_datetime(item["bar_end"]))
        first = bucket[0]
        last = bucket[-1]
        start_ts = min(_coerce_datetime(item["bar_start"]) for item in bucket)
        session_asia, session_london, session_us, session_allowed = _bar_session_flags(end_ts, settings)
        bars.append(
            Bar(
                bar_id=f"{symbol}|5m|{end_ts.isoformat()}",
                symbol=symbol,
                timeframe=DECISION_TIMEFRAME,
                start_ts=start_ts,
                end_ts=end_ts,
                open=Decimal(str(first["open"])),
                high=max(Decimal(str(item["high"])) for item in bucket),
                low=min(Decimal(str(item["low"])) for item in bucket),
                close=Decimal(str(last["close"])),
                volume=int(sum(float(item.get("volume") or 0) for item in bucket)),
                is_final=True,
                session_asia=session_asia,
                session_london=session_london,
                session_us=session_us,
                session_allowed=session_allowed,
            )
        )
    return bars


def _load_coverage_report(*, config: IndexCandidateValidationConfig, symbol: str) -> dict[str, Any]:
    path = _coverage_report_path(config=config, symbol=symbol)
    return json.loads(path.read_text(encoding="utf-8"))


def _coverage_report_path(*, config: IndexCandidateValidationConfig, symbol: str) -> Path:
    return _output_root(config) / "reports" / f"latest_databento_research_minute_backfill_quality_audit_{symbol}.json"


def _session_mask_from_report(report: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    rows = report.get("session_coverage") or []
    return {(str(row["date"]), str(row["session"])): dict(row) for row in rows}


def _annotate_trade(row: dict[str, Any], *, session_mask: dict[tuple[str, str], dict[str, Any]]) -> dict[str, Any]:
    session_date, session = _session_date_and_label(_coerce_datetime(row["entry_ts"]))
    coverage = session_mask.get((session_date, session), {})
    eligible = bool(coverage.get("eligible_for_replay")) if coverage else False
    exclusion_reason = coverage.get("exclusion_reason") if coverage else "SESSION_COVERAGE_MISSING"
    return {
        **row,
        "entry_session": session,
        "entry_session_date": session_date,
        "entry_month": session_date[:7],
        "eligible_for_replay": eligible,
        "eligibility_bucket": "eligible" if eligible else "ineligible",
        "exclusion_reason": exclusion_reason,
        "session_total_bars": coverage.get("total_bars"),
        "session_active_minutes": coverage.get("active_minutes"),
        "session_largest_gap_minutes": coverage.get("largest_intra_session_gap_minutes"),
        "session_suspicious_gap_count": coverage.get("suspicious_gap_count"),
    }


def _summarize_trade_rows(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    pnls = [Decimal(str(row["net_pnl"])) for row in rows]
    gross = [Decimal(str(row["gross_pnl"])) for row in rows]
    wins = [value for value in pnls if value > 0]
    losses = [-value for value in pnls if value < 0]
    total = sum(pnls, Decimal("0"))
    gross_total = sum(gross, Decimal("0"))
    top = sorted(pnls, reverse=True)
    return {
        "trade_count": len(rows),
        "gross_pnl": _decimal_float(gross_total),
        "net_pnl": _decimal_float(total),
        "average_trade": _decimal_float(total / Decimal(len(rows))) if rows else None,
        "max_drawdown": _decimal_float(_max_drawdown(pnls)),
        "profit_factor": _decimal_float(sum(wins, Decimal("0")) / sum(losses, Decimal("0"))) if losses else None,
        "win_rate": round(len(wins) / len(rows), 6) if rows else None,
        "top_1_contribution": _decimal_float(top[0] / total) if rows and total != 0 else None,
        "top_3_contribution": _decimal_float(sum(top[:3], Decimal("0")) / total) if rows and total != 0 else None,
        "survives_without_top_1": (total - top[0]) > 0 if top else None,
        "survives_without_top_3": (total - sum(top[:3], Decimal("0"))) > 0 if top else None,
    }


def _segment_trade_rows(rows: Sequence[dict[str, Any]], *, key: str) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(row.get(key) or "UNKNOWN")].append(row)
    return [{"bucket": bucket, **_summarize_trade_rows(groups[bucket])} for bucket in sorted(groups)]


def _threshold_sensitivity(rows: Sequence[dict[str, Any]], coverage_report: dict[str, Any]) -> list[dict[str, Any]]:
    coverage_rows = list(coverage_report.get("session_coverage") or [])
    policies = {
        "default_audit_mask": {
            (row["date"], row["session"])
            for row in coverage_rows
            if row.get("eligible_for_replay")
        },
        "strict_no_suspicious_gaps": {
            (row["date"], row["session"])
            for row in coverage_rows
            if row.get("session") != "OFF_SESSION"
            and int(row.get("active_minutes") or 0) >= int(int(row.get("expected_minutes") or 0) * Decimal("0.75"))
            and int(row.get("largest_intra_session_gap_minutes") or 0) <= 15
            and int(row.get("suspicious_gap_count") or 0) <= 0
        },
        "relaxed_suspicious_gap_limit_10": {
            (row["date"], row["session"])
            for row in coverage_rows
            if row.get("session") != "OFF_SESSION"
            and int(row.get("active_minutes") or 0) >= int(int(row.get("expected_minutes") or 0) * Decimal("0.75"))
            and int(row.get("largest_intra_session_gap_minutes") or 0) <= 15
            and int(row.get("suspicious_gap_count") or 0) <= 10
        },
    }
    result: list[dict[str, Any]] = []
    for name, mask in policies.items():
        selected = [row for row in rows if (row["entry_session_date"], row["entry_session"]) in mask]
        result.append({"policy": name, **_summarize_trade_rows(selected)})
    return result


def _robustness_classification(*, symbol: str, rows: Sequence[dict[str, Any]]) -> str:
    metrics = _summarize_trade_rows(rows)
    if metrics["trade_count"] == 0:
        return "NEEDS_ADAPTER_OR_DATA_WORK"
    if metrics["net_pnl"] is not None and metrics["net_pnl"] <= 0:
        return "FAILS_ROBUSTNESS"
    if (
        metrics["trade_count"] >= 8
        and (metrics["profit_factor"] or 0) >= 1.2
        and metrics["survives_without_top_3"] is True
    ):
        return "BRIDGE_PERIOD_CONFIRMED"
    return "WEAKENED_BUT_RETAINED"


def _prior_research_status(*, symbol: str, family: str) -> dict[str, Any]:
    if symbol == "MNQ" and family == "usDerivativeBearTurn":
        return {
            "prior_status": "SERIOUS_NEXT_CANDIDATE_STRUCTURAL_TRIAGE",
            "prior_limitation": "No dedicated long-lookback index replay artifact before this gap-aware Parquet validation.",
        }
    if symbol == "MES" and family == "firstBearSnapTurn":
        return {
            "prior_status": "LATER_REVIEW_STRUCTURAL_TRIAGE",
            "prior_limitation": "Candidate retained as MES baseline lead, but prior evidence was not a dedicated gap-aware replay.",
        }
    return {"prior_status": "UNKNOWN", "prior_limitation": "No prior status mapping in this validator."}


def _write_reports(
    *,
    config: IndexCandidateValidationConfig,
    report: dict[str, Any],
    ledgers_by_target: dict[str, list[dict[str, Any]]],
) -> ValidationRunResult:
    report_dir = _report_dir(config)
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = report_dir / "index_candidate_gap_aware_research_validation.json"
    md_path = report_dir / "index_candidate_gap_aware_research_validation.md"
    manifest_path = _output_root(config) / "manifests" / "index_candidate_gap_aware_research_validation_manifest.json"
    _write_json_atomic(json_path, report)
    md_path.write_text(_render_markdown(report), encoding="utf-8")
    paths = [json_path, md_path]
    if config.write_trade_ledgers:
        for target_key, rows in ledgers_by_target.items():
            ledger_path = report_dir / f"index_candidate_gap_aware_research_validation_{target_key}.trade_ledger.csv"
            _write_trade_rows(ledger_path, rows)
            paths.append(ledger_path)
    write_storage_manifest(
        manifest_path,
        {
            "schema_version": "index_candidate_gap_aware_research_validation_manifest_v1",
            "generated_at": report["generated_at"],
            "source": AUDIT_SOURCE,
            "report_path": str(json_path),
            "research_artifact": True,
            "runtime_artifact": False,
            "can_submit": False,
            "live_money_eligible": False,
            "target_count": report["target_count"],
        },
    )
    paths.append(manifest_path)
    return ValidationRunResult(report=report, report_paths=tuple(paths))


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Index Candidate Gap-Aware Research Validation",
        "",
        f"- generated_at: `{report['generated_at']}`",
        f"- final_classification: `{report['final_classification']}`",
        f"- runtime_pretrade_unchanged: `{report['runtime_pretrade_unchanged']}`",
        f"- can_submit: `{report['can_submit']}`",
        f"- live_money_eligible: `{report['live_money_eligible']}`",
        "",
        "## Targets",
        "",
    ]
    for row in report["targets"]:
        eligible = row["replay"]["eligible_sessions_only"]
        diagnostic = row["replay"]["all_available_sessions_diagnostic_only"]
        lines.extend(
            [
                f"### {row['symbol']} / {row['strategy_family']}",
                f"- robustness_classification: `{row['robustness_classification']}`",
                f"- data: `{row['coverage']['earliest_actual_bar']}` -> `{row['coverage']['latest_actual_bar']}`",
                f"- complete/empty/failed partitions: `{row['coverage']['complete_partitions']}` / `{row['coverage']['empty_partitions']}` / `{row['coverage']['failed_partitions']}`",
                f"- eligible sessions: `{row['coverage']['eligible_session_rows']}` of `{row['coverage']['session_coverage_rows']}`",
                f"- eligible replay trades: `{eligible['trade_count']}`, net_pnl `{eligible['net_pnl']}`, PF `{eligible['profit_factor']}`, win_rate `{eligible['win_rate']}`",
                f"- all-session diagnostic trades: `{diagnostic['trade_count']}`, net_pnl `{diagnostic['net_pnl']}`, PF `{diagnostic['profit_factor']}`, win_rate `{diagnostic['win_rate']}`",
                "",
            ]
        )
    return "\n".join(lines) + "\n"


def _write_trade_rows(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _ledger_row_to_dict(row: Any) -> dict[str, Any]:
    payload = dict(row.__dict__)
    for key, value in list(payload.items()):
        if isinstance(value, Decimal):
            payload[key] = str(value)
        elif isinstance(value, datetime):
            payload[key] = value.isoformat()
    return payload


def _bar_session_flags(end_ts: datetime, settings: Any) -> tuple[bool, bool, bool, bool]:
    local_time = end_ts.astimezone(settings.timezone_info).time()
    session_asia = _time_in_window(local_time, settings.asia_start, settings.asia_end)
    session_london = _time_in_window(local_time, settings.london_start, settings.london_end)
    session_us = _time_in_window(local_time, settings.us_start, settings.us_end)
    return (
        session_asia,
        session_london,
        session_us,
        (session_asia and settings.allow_asia) or (session_london and settings.allow_london) or (session_us and settings.allow_us),
    )


def _time_in_window(value: time, start: time, end: time) -> bool:
    if start <= end:
        return start <= value < end
    return value >= start or value < end


def _session_date_and_label(value: datetime) -> tuple[str, str]:
    local = value.astimezone(SESSION_TIMEZONE)
    session = _coverage_session_label(local)
    trading_date = local.date() + _one_day() if session == "ASIA" and local.time() >= time(18, 0) else local.date()
    return trading_date.isoformat(), session


def _coverage_session_label(local: datetime) -> str:
    value = local.time()
    if value >= time(18, 0) or value < time(3, 0):
        return "ASIA"
    if time(3, 0) <= value < time(8, 20):
        return "LONDON"
    if time(8, 20) <= value < time(16, 0):
        return "US"
    return "OFF_SESSION"


def _one_day():
    from datetime import timedelta

    return timedelta(days=1)


def _ceil_to_minutes(value: datetime, minutes: int) -> datetime:
    value = value.astimezone(timezone.utc)
    discard = (value.minute % minutes) * 60 + value.second + value.microsecond / 1_000_000
    if discard == 0:
        return value.replace(second=0, microsecond=0)
    delta_seconds = minutes * 60 - discard
    from datetime import timedelta

    return (value + timedelta(seconds=delta_seconds)).replace(second=0, microsecond=0)


def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _max_drawdown(pnls: Sequence[Decimal]) -> Decimal:
    equity = Decimal("0")
    peak = Decimal("0")
    max_drawdown = Decimal("0")
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return max_drawdown


def _decimal_float(value: Decimal) -> float:
    return round(float(value), 6)


def _output_root(config: IndexCandidateValidationConfig) -> Path:
    root = Path(config.output_root)
    return root if root.is_absolute() else config.repo_root / root


def _report_dir(config: IndexCandidateValidationConfig) -> Path:
    root = Path(config.report_dir)
    return root if root.is_absolute() else config.repo_root / root


def _parse_targets(values: Iterable[str]) -> tuple[tuple[str, str], ...]:
    targets: list[tuple[str, str]] = []
    for value in values:
        symbol, sep, family = value.partition(":")
        if not sep:
            raise ValueError(f"Target must be SYMBOL:FAMILY, got {value!r}")
        targets.append((symbol.strip().upper(), family.strip()))
    return tuple(targets)


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
