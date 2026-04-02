"""Broker-history validation workflow for frozen approved quant baseline lanes."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import and_, select

from ..config_models import load_settings_from_files
from ..market_data import (
    HistoricalBackfillService,
    SchwabHistoricalHttpClient,
    SchwabHistoricalRequest,
    SchwabOAuthClient,
    SchwabTokenStore,
    UrllibJsonTransport,
    load_schwab_auth_config_from_env,
    load_schwab_market_data_config,
    normalize_timeframe_label,
)
from ..market_data.bar_builder import BarBuilder
from ..market_data.schwab_adapter import SchwabMarketDataAdapter
from ..persistence import build_engine
from ..persistence.repositories import RepositorySet
from ..persistence.tables import bars_table
from ..research import build_resampled_bars
from .approved_quant_lanes import approved_quant_lane_specs, run_approved_quant_baseline_probation
from ..domain import Bar


RESAMPLED_TIMEFRAMES: tuple[str, ...] = ("60m", "240m", "720m", "1440m")


@dataclass(frozen=True)
class ApprovedQuantSchwabHistoryValidationArtifacts:
    json_path: Path
    markdown_path: Path
    output_dir: Path
    report: dict[str, Any]


def run_approved_quant_schwab_history_validation(
    *,
    config_paths: tuple[str, ...] | None = None,
    schwab_config_path: str | None = None,
    token_file: str | None = None,
    execution_timeframe: str = "5m",
    output_dir: str | Path | None = None,
    start_date_ms: int | None = None,
    end_date_ms: int | None = None,
    days_back: int = 180,
    need_extended_hours_data: bool = True,
    need_previous_close: bool = False,
) -> ApprovedQuantSchwabHistoryValidationArtifacts:
    resolved_output_dir = Path(
        output_dir or Path.cwd() / "outputs" / "reports" / "approved_quant_schwab_history_validation"
    ).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    probation_output_dir = resolved_output_dir / "probationary_quant_baselines"
    validation_db_path = resolved_output_dir / "approved_quant_schwab_history_validation.sqlite3"

    settings = load_settings_from_files(config_paths or ("config/base.yaml", "config/replay.yaml"))
    validation_settings = settings.model_copy(update={"database_url": f"sqlite:///{validation_db_path}"})
    execution_timeframe = normalize_timeframe_label(execution_timeframe)
    repositories = RepositorySet(build_engine(validation_settings.database_url))

    schwab_config = load_schwab_market_data_config(schwab_config_path)
    if token_file is not None:
        schwab_config = replace(schwab_config, auth=load_schwab_auth_config_from_env(token_file))
    adapter = SchwabMarketDataAdapter(validation_settings, schwab_config)
    service = HistoricalBackfillService(
        adapter=adapter,
        client=SchwabHistoricalHttpClient(
            oauth_client=_build_oauth_client(schwab_config.auth),
            market_data_config=schwab_config,
            transport=UrllibJsonTransport(),
        ),
        repositories=repositories,
    )

    approved_symbols = _approved_symbol_universe()
    resolved_end_ms = end_date_ms or int(datetime.now(validation_settings.timezone_info).timestamp() * 1000)
    resolved_start_ms = start_date_ms or int(
        (datetime.now(validation_settings.timezone_info) - timedelta(days=days_back)).timestamp() * 1000
    )

    fetch_results = []
    for symbol in approved_symbols:
        fetch_request = SchwabHistoricalRequest(
            internal_symbol=symbol,
            period_type="day",
            period=None,
            frequency_type="minute",
            frequency=5,
            start_date_ms=resolved_start_ms,
            end_date_ms=resolved_end_ms,
            need_extended_hours_data=need_extended_hours_data,
            need_previous_close=need_previous_close,
        )
        external_symbol = adapter.map_historical_symbol(symbol)
        try:
            bars = service.fetch_bars(fetch_request, internal_timeframe=execution_timeframe)
            fetch_results.append(
                {
                    "symbol": symbol,
                    "external_symbol": external_symbol,
                    "status": "fetched",
                    "bar_count": len(bars),
                    "first_bar_end_ts": bars[0].end_ts.isoformat() if bars else None,
                    "last_bar_end_ts": bars[-1].end_ts.isoformat() if bars else None,
                }
            )
        except Exception as exc:  # pragma: no cover - exercised in real validation runs
            fetch_results.append(
                {
                    "symbol": symbol,
                    "external_symbol": external_symbol,
                    "status": "error",
                    "error": str(exc),
                }
            )

    resample_results = _resample_validation_bars(
        repositories=repositories,
        settings=validation_settings,
        source_timeframe=execution_timeframe,
    )

    probation_artifacts = run_approved_quant_baseline_probation(
        database_path=validation_db_path,
        execution_timeframe=execution_timeframe,
        output_dir=probation_output_dir,
    )
    current_status = json.loads(probation_artifacts.current_status_json_path.read_text(encoding="utf-8"))
    snapshot = json.loads(probation_artifacts.snapshot_json_path.read_text(encoding="utf-8"))
    lane_behavior = _compare_lane_behavior(snapshot=snapshot)

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "validation_type": "approved_quant_schwab_history_validation",
        "workflow": [
            "Fetch Schwab historical 5m bars for the frozen approved-lane symbol universe.",
            "Persist normalized 5m bars into a dedicated validation SQLite database.",
            "Resample persisted 5m bars into 60m, 240m, 720m, and 1440m bars.",
            "Run the frozen approved baseline probation pipeline against that broker-sourced bar store.",
            "Compare signals, trades, exits, sessions, and warnings against approved-lane expectations.",
        ],
        "database_path": str(validation_db_path),
        "execution_timeframe": execution_timeframe,
        "approved_symbols": approved_symbols,
        "schwab_config_path": schwab_config_path,
        "token_file_override_used": bool(token_file),
        "fetch_window": {
            "start_date_ms": resolved_start_ms,
            "end_date_ms": resolved_end_ms,
            "days_back_assumption": None if start_date_ms is not None else days_back,
        },
        "fetch_results": fetch_results,
        "resample_results": resample_results,
        "probation_artifacts": {
            "snapshot_json_path": str(probation_artifacts.snapshot_json_path),
            "snapshot_markdown_path": str(probation_artifacts.snapshot_markdown_path),
            "current_status_json_path": str(probation_artifacts.current_status_json_path),
            "current_status_markdown_path": str(probation_artifacts.current_status_markdown_path),
            "root_dir": str(probation_artifacts.root_dir),
        },
        "current_active_status": current_status,
        "lane_behavior_validation": lane_behavior,
        "validation_ready_for_trustworthy_evaluation": _validation_ready(fetch_results, resample_results, lane_behavior),
        "blockers": _validation_blockers(fetch_results, resample_results, lane_behavior),
        "risk_notes": [
            "Session labels are based on bar end timestamps; any Schwab timestamp-semantics mismatch can move signals across session boundaries.",
            "This workflow evaluates the frozen approved quant lanes from persisted bars after resampling, not through the legacy paper/canary runtime.",
            "Bar-level exits remain shadow/probation assumptions, not broker-fill truth.",
        ],
        "recommended_next_step": _recommended_next_step(fetch_results, resample_results, lane_behavior),
    }

    json_path = resolved_output_dir / "approved_quant_schwab_history_validation_report.json"
    markdown_path = resolved_output_dir / "approved_quant_schwab_history_validation_report.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_validation_markdown(report).strip() + "\n", encoding="utf-8")
    return ApprovedQuantSchwabHistoryValidationArtifacts(
        json_path=json_path,
        markdown_path=markdown_path,
        output_dir=resolved_output_dir,
        report=report,
    )


def _approved_symbol_universe() -> list[str]:
    ordered: list[str] = []
    for spec in approved_quant_lane_specs():
        for symbol in spec.symbols:
            if symbol not in ordered:
                ordered.append(symbol)
    return ordered


def _build_oauth_client(auth_config) -> SchwabOAuthClient:
    return SchwabOAuthClient(
        config=auth_config,
        transport=UrllibJsonTransport(),
        token_store=SchwabTokenStore(auth_config.token_store_path),
    )


def _resample_validation_bars(
    *,
    repositories: RepositorySet,
    settings,
    source_timeframe: str,
) -> list[dict[str, Any]]:
    results = []
    source_timeframe = normalize_timeframe_label(source_timeframe)
    source_data_source = "schwab_history"
    for symbol in _approved_symbol_universe():
        source_bars = _load_persisted_bars(
            engine=repositories.engine,
            ticker=symbol,
            timeframe=source_timeframe,
            data_source=source_data_source,
        )
        if not source_bars:
            results.append({"symbol": symbol, "status": "missing_source_bars", "timeframes": []})
            continue
        timeframe_rows = []
        for target_timeframe in RESAMPLED_TIMEFRAMES:
            resampled = build_resampled_bars(
                source_bars,
                target_timeframe=target_timeframe,
                bar_builder=BarBuilder(settings),
            )
            data_source = f"resampled_schwab_history_{source_timeframe}_to_{target_timeframe}"
            for bar in resampled.bars:
                repositories.bars.save(bar, data_source=data_source)
            timeframe_rows.append(
                {
                    "target_timeframe": target_timeframe,
                    "bar_count": len(resampled.bars),
                    "skipped_bucket_count": resampled.skipped_bucket_count,
                    "data_source": data_source,
                }
            )
        results.append({"symbol": symbol, "status": "resampled", "timeframes": timeframe_rows})
    return results


def _load_persisted_bars(
    *,
    engine,
    ticker: str,
    timeframe: str,
    data_source: str | None,
) -> list[Bar]:
    statement = (
        select(bars_table)
        .where(
            bars_table.c.ticker == ticker,
            bars_table.c.timeframe == timeframe,
        )
        .order_by(bars_table.c.end_ts.asc())
    )
    if data_source:
        statement = statement.where(and_(bars_table.c.data_source == data_source))

    with engine.begin() as connection:
        rows = connection.execute(statement).mappings().all()

    return [
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


def _compare_lane_behavior(*, snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    results = []
    for row in snapshot.get("rows", []):
        approved_scope = row.get("approved_scope", {})
        lane_dir = Path(str(((row.get("artifacts") or {}).get("lane_dir")) or ""))
        trades = _load_jsonl(lane_dir / "trades.jsonl")
        signals = _load_jsonl(lane_dir / "signals.jsonl")
        allowed_sessions = set(approved_scope.get("allowed_sessions", []))
        allowed_symbols = set(approved_scope.get("symbols", []))
        unexpected_symbols = sorted({str(trade.get("symbol")) for trade in trades if str(trade.get("symbol")) not in allowed_symbols})
        unexpected_sessions = sorted(
            {str(trade.get("session_label")) for trade in trades if str(trade.get("session_label")) not in allowed_sessions}
        )
        expected_exit_reasons = _expected_exit_reasons(str(row.get("lane_id")))
        exit_reasons = sorted({str(trade.get("exit_reason")) for trade in trades})
        unexpected_exit_reasons = sorted(reason for reason in exit_reasons if reason not in expected_exit_reasons)
        results.append(
            {
                "lane_id": row.get("lane_id"),
                "lane_name": row.get("lane_name"),
                "signal_count": len(signals),
                "trade_count": len(trades),
                "observed_symbols": sorted({str(trade.get("symbol")) for trade in trades}),
                "observed_sessions": sorted({str(trade.get("session_label")) for trade in trades}),
                "exit_reasons_seen": exit_reasons,
                "expected_exit_reasons": sorted(expected_exit_reasons),
                "unexpected_symbols": unexpected_symbols,
                "unexpected_sessions": unexpected_sessions,
                "unexpected_exit_reasons": unexpected_exit_reasons,
                "probation_status": row.get("probation_status"),
                "promotion_state": row.get("promotion_state"),
                "post_cost_monitoring_read": row.get("post_cost_monitoring_read"),
                "warning_flags": row.get("warning_flags", []),
                "symbol_attribution_summary": row.get("symbol_attribution_summary", []),
                "session_attribution_summary": row.get("session_attribution_summary", []),
                "matches_expected_behavior": not (unexpected_symbols or unexpected_sessions or unexpected_exit_reasons),
            }
        )
    return results


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _expected_exit_reasons(lane_id: str) -> set[str]:
    if lane_id == "phase2c.breakout.metals_only.us_unknown.baseline":
        return {"stop", "time_exit"}
    if lane_id == "phase2c.failed.core4_plus_qc.no_us.baseline":
        return {"stop", "target", "time_exit", "structural_invalidation"}
    return set()


def _validation_ready(
    fetch_results: list[dict[str, Any]],
    resample_results: list[dict[str, Any]],
    lane_behavior: list[dict[str, Any]],
) -> bool:
    fetch_ok = all(row.get("status") == "fetched" and int(row.get("bar_count", 0) or 0) > 0 for row in fetch_results)
    resample_ok = all(
        row.get("status") == "resampled" and all(int(tf.get("bar_count", 0) or 0) > 0 for tf in row.get("timeframes", []))
        for row in resample_results
    )
    lane_ok = all(bool(row.get("matches_expected_behavior")) for row in lane_behavior)
    return fetch_ok and resample_ok and lane_ok


def _validation_blockers(
    fetch_results: list[dict[str, Any]],
    resample_results: list[dict[str, Any]],
    lane_behavior: list[dict[str, Any]],
) -> list[str]:
    blockers = []
    failed_fetches = [row["symbol"] for row in fetch_results if row.get("status") != "fetched" or not row.get("bar_count")]
    if failed_fetches:
        blockers.append(f"Missing or failed Schwab history for symbols: {', '.join(failed_fetches)}")
    thin_resamples = [
        f"{row['symbol']}:{tf['target_timeframe']}"
        for row in resample_results
        for tf in row.get("timeframes", [])
        if int(tf.get("bar_count", 0) or 0) <= 0
    ]
    if thin_resamples:
        blockers.append(f"Missing derived higher-timeframe bars: {', '.join(thin_resamples)}")
    for row in lane_behavior:
        if row.get("unexpected_sessions"):
            blockers.append(
                f"{row['lane_name']} produced out-of-scope sessions: {', '.join(row['unexpected_sessions'])}"
            )
        if row.get("unexpected_symbols"):
            blockers.append(
                f"{row['lane_name']} produced out-of-scope symbols: {', '.join(row['unexpected_symbols'])}"
            )
        if row.get("unexpected_exit_reasons"):
            blockers.append(
                f"{row['lane_name']} produced unexpected exits: {', '.join(row['unexpected_exit_reasons'])}"
            )
    return blockers


def _recommended_next_step(
    fetch_results: list[dict[str, Any]],
    resample_results: list[dict[str, Any]],
    lane_behavior: list[dict[str, Any]],
) -> str:
    if not _validation_ready(fetch_results, resample_results, lane_behavior):
        return (
            "Add or repair the missing broker-history fetch/resample slices, then rerun the frozen approved baseline probation "
            "workflow on the dedicated validation database before trusting replay-based economics."
        )
    return (
        "Use this dedicated broker-history validation workflow as the repeatable approved-lane replay harness and only compare "
        "future runs against the frozen approved baseline package."
    )


def _render_validation_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Approved Quant Schwab History Validation",
        "",
        f"Generated at: {report['generated_at']}",
        f"Database: {report['database_path']}",
        "",
        "## Workflow",
    ]
    for step in report["workflow"]:
        lines.append(f"- {step}")
    lines.extend(["", "## Fetch Results"])
    for row in report["fetch_results"]:
        if row["status"] == "fetched":
            lines.append(
                f"- {row['symbol']} ({row['external_symbol']}): {row['bar_count']} bars "
                f"[{row['first_bar_end_ts']} -> {row['last_bar_end_ts']}]"
            )
        else:
            lines.append(f"- {row['symbol']} ({row['external_symbol']}): ERROR {row.get('error', '-')}")
    lines.extend(["", "## Lane Behavior"])
    for row in report["lane_behavior_validation"]:
        lines.append(
            f"- {row['lane_name']}: trades={row['trade_count']} probation={row['probation_status']} "
            f"post_cost={row['post_cost_monitoring_read']['label']} exits={','.join(row['exit_reasons_seen']) or 'none'}"
        )
        if row["warning_flags"]:
            lines.append(f"  warnings: {', '.join(row['warning_flags'])}")
        if row["unexpected_sessions"] or row["unexpected_symbols"] or row["unexpected_exit_reasons"]:
            lines.append(
                "  mismatches: "
                f"sessions={','.join(row['unexpected_sessions']) or 'none'} | "
                f"symbols={','.join(row['unexpected_symbols']) or 'none'} | "
                f"exits={','.join(row['unexpected_exit_reasons']) or 'none'}"
            )
    lines.extend(["", "## Verdict"])
    lines.append(f"- Trustworthy evaluation ready: {report['validation_ready_for_trustworthy_evaluation']}")
    for blocker in report["blockers"]:
        lines.append(f"- Blocker: {blocker}")
    lines.append(f"- Recommended next step: {report['recommended_next_step']}")
    return "\n".join(lines)
