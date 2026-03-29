"""Historical playback runner over persisted SQLite bars."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence

from sqlalchemy import select

from ..config_models import StrategySettings, load_settings_from_files
from ..domain.enums import OrderIntentType
from ..market_data.replay_feed import ReplayFeed
from ..market_data.sqlite_playback import SQLiteHistoricalBarSource
from ..persistence import build_engine
from ..persistence.repositories import RepositorySet, decode_strategy_state
from ..persistence.tables import fault_events_table, fills_table, order_intents_table, signals_table, strategy_state_snapshots_table
from ..strategy.strategy_engine import StrategyEngine
from .session_phase_labels import label_session_phase
from .strategy_study import build_strategy_study_v3, write_strategy_study_json, write_strategy_study_markdown
from .container import ApplicationContainer
from .runner import StrategyServiceRunner
from .strategy_runtime_registry import build_strategy_runtime_registry


@dataclass(frozen=True)
class TriggerReportRow:
    symbol: str
    lane_family: str
    side: str
    reason_code: str
    bars_processed: int
    signals_seen: int
    intents_created: int
    fills_created: int
    first_trigger_timestamp: str | None
    first_intent_timestamp: str | None
    first_fill_timestamp: str | None
    block_or_fault_reason: str | None


@dataclass(frozen=True)
class HistoricalPlaybackSymbolResult:
    symbol: str
    run_stamp: str
    source_db_path: str
    replay_db_path: str
    summary_path: str
    trigger_report_json_path: str
    trigger_report_markdown_path: str
    strategy_study_json_path: str
    strategy_study_markdown_path: str
    source_timeframe: str
    target_timeframe: str
    environment_mode: str
    structural_signal_timeframe: str
    execution_timeframe: str
    artifact_timeframe: str
    execution_timeframe_role: str
    selected_data_source: str
    source_bar_count: int
    playback_bar_count: int
    skipped_incomplete_buckets: int
    processed_bars: int
    order_intents: int
    fills: int
    long_entries: int
    short_entries: int
    exits: int
    final_position_side: str
    final_strategy_status: str
    primary_standalone_strategy_id: str | None
    standalone_strategy_count: int
    per_strategy_summaries: list[dict[str, Any]]
    aggregate_portfolio_summary: dict[str, Any]


@dataclass(frozen=True)
class HistoricalPlaybackRunResult:
    run_stamp: str
    source_db_path: str
    output_dir: str
    config_paths: list[str]
    symbols: list[HistoricalPlaybackSymbolResult]
    manifest_path: str


@dataclass
class _GroupStats:
    signals_seen: int = 0
    intents_created: int = 0
    fills_created: int = 0
    first_trigger_timestamp: datetime | None = None
    first_intent_timestamp: datetime | None = None
    first_fill_timestamp: datetime | None = None
    first_trigger_bar_id: str | None = None


def run_historical_playback(
    *,
    config_paths: Sequence[str | Path],
    source_db_path: str | Path,
    symbols: Sequence[str],
    source_timeframe: str,
    target_timeframe: str = "5m",
    start_timestamp: datetime | None = None,
    end_timestamp: datetime | None = None,
    output_dir: str | Path,
    data_source: str | None = None,
    run_stamp: str | None = None,
) -> HistoricalPlaybackRunResult:
    if not symbols:
        raise ValueError("At least one symbol is required for historical playback.")

    stamp = run_stamp or datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    source_database_path = Path(source_db_path).resolve()
    resolved_output_dir = Path(output_dir).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    settings = load_settings_from_files(config_paths)
    results: list[HistoricalPlaybackSymbolResult] = []
    config_strings = [str(Path(path)) for path in config_paths]

    for raw_symbol in symbols:
        symbol = raw_symbol.strip().upper()
        result = _run_symbol_playback(
            settings=settings,
            config_paths=config_strings,
            source_db_path=source_database_path,
            symbol=symbol,
            source_timeframe=source_timeframe,
            target_timeframe=target_timeframe,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            output_dir=resolved_output_dir,
            data_source=data_source,
            run_stamp=stamp,
        )
        results.append(result)

    manifest_path = resolved_output_dir / f"historical_playback_{stamp}.manifest.json"
    manifest_payload = {
        "run_stamp": stamp,
        "source_db_path": str(source_database_path),
        "output_dir": str(resolved_output_dir),
        "config_paths": config_strings,
        "symbols": [asdict(result) for result in results],
    }
    manifest_path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return HistoricalPlaybackRunResult(
        run_stamp=stamp,
        source_db_path=str(source_database_path),
        output_dir=str(resolved_output_dir),
        config_paths=config_strings,
        symbols=results,
        manifest_path=str(manifest_path),
    )


def ensure_strategy_study_artifacts(
    *,
    summary_path: str | Path,
    summary_payload: dict[str, Any] | None = None,
) -> tuple[Path | None, Path | None]:
    resolved_summary_path = Path(summary_path).resolve()
    payload = dict(summary_payload or {})
    if not payload:
        if not resolved_summary_path.exists():
            return None, None
        payload = json.loads(resolved_summary_path.read_text(encoding="utf-8"))

    study_json_path, study_markdown_path = _derived_strategy_study_paths(
        summary_path=resolved_summary_path,
        summary_payload=payload,
    )
    if study_json_path.exists() and study_markdown_path.exists():
        return study_json_path, study_markdown_path

    config_paths = [Path(path) for path in payload.get("config_paths") or []]
    source_db_raw = payload.get("source_db_path")
    replay_db_raw = payload.get("replay_db_path")
    symbol = str(payload.get("symbol") or "").strip().upper()
    source_timeframe = str(payload.get("source_timeframe") or "").strip()
    target_timeframe = str(payload.get("target_timeframe") or "").strip()
    if not config_paths or not source_db_raw or not replay_db_raw or not symbol or not source_timeframe or not target_timeframe:
        return (
            study_json_path if study_json_path.exists() else None,
            study_markdown_path if study_markdown_path.exists() else None,
        )

    settings = load_settings_from_files(config_paths).model_copy(
        update={
            "symbol": symbol,
            "timeframe": target_timeframe,
            "database_url": f"sqlite:///{Path(replay_db_raw).resolve()}",
        }
    )
    loaded = SQLiteHistoricalBarSource(Path(source_db_raw).resolve(), settings).load_bars(
        symbol=symbol,
        source_timeframe=source_timeframe,
        target_timeframe=target_timeframe,
        start_timestamp=_parse_optional_iso_timestamp(payload.get("start_timestamp")),
        end_timestamp=_parse_optional_iso_timestamp(payload.get("end_timestamp")),
        data_source=str(payload.get("selected_data_source") or "").strip() or None,
    )
    repositories = RepositorySet(build_engine(settings.database_url))
    strategy_runtime_registry = build_strategy_runtime_registry(settings)
    primary_instance = strategy_runtime_registry.primary_engine_instance()
    common_metadata = {
        "mode": "REPLAY",
        "run_stamp": payload.get("run_stamp"),
        "source_db_path": str(Path(source_db_raw).resolve()),
        "replay_db_path": str(Path(replay_db_raw).resolve()),
        "summary_path": str(resolved_summary_path),
        "artifact_context": "HISTORICAL_PLAYBACK_STRATEGY_STUDY",
        "persistence_origin": "PERSISTED_RUNTIME_TRUTH",
        "artifact_rebuilt": True,
    }
    if primary_instance is not None:
        study_payload = build_strategy_study_v3(
            repositories=primary_instance.repositories or repositories,
            settings=primary_instance.settings or settings,
            bars=loaded.playback_bars,
            source_bars=loaded.source_bars,
            point_value=primary_instance.definition.point_value,
            standalone_strategy_id=primary_instance.definition.standalone_strategy_id,
            strategy_family=primary_instance.definition.strategy_family,
            instrument=primary_instance.definition.instrument,
            run_metadata=common_metadata,
        )
    else:
        study_payload = build_strategy_study_v3(
            repositories=repositories,
            settings=settings,
            bars=loaded.playback_bars,
            source_bars=loaded.source_bars,
            point_value=None,
            standalone_strategy_id=None,
            strategy_family="LEGACY_RUNTIME",
            instrument=symbol,
            run_metadata=common_metadata,
        )
    write_strategy_study_json(study_payload, study_json_path)
    write_strategy_study_markdown(study_payload, study_markdown_path)
    return study_json_path, study_markdown_path


def _run_symbol_playback(
    *,
    settings: StrategySettings,
    config_paths: Sequence[str],
    source_db_path: Path,
    symbol: str,
    source_timeframe: str,
    target_timeframe: str,
    start_timestamp: datetime | None,
    end_timestamp: datetime | None,
    output_dir: Path,
    data_source: str | None,
    run_stamp: str,
) -> HistoricalPlaybackSymbolResult:
    prefix = output_dir / f"historical_playback_{symbol.lower()}_{run_stamp}"
    replay_db_path = prefix.with_suffix(".sqlite3")
    summary_path = prefix.with_suffix(".summary.json")
    trigger_report_json_path = prefix.with_suffix(".trigger_report.json")
    trigger_report_markdown_path = prefix.with_suffix(".trigger_report.md")
    strategy_study_json_path = prefix.with_suffix(".strategy_study.json")
    strategy_study_markdown_path = prefix.with_suffix(".strategy_study.md")

    symbol_settings = settings.model_copy(
        update={
            "symbol": symbol,
            "timeframe": target_timeframe,
            "database_url": f"sqlite:///{replay_db_path}",
        }
    )
    source = SQLiteHistoricalBarSource(source_db_path, symbol_settings)
    loaded = source.load_bars(
        symbol=symbol,
        source_timeframe=source_timeframe,
        target_timeframe=target_timeframe,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        data_source=data_source,
    )

    strategy_runtime_registry = build_strategy_runtime_registry(symbol_settings)
    primary_instance = strategy_runtime_registry.primary_engine_instance()
    if primary_instance is None or primary_instance.repositories is None or primary_instance.strategy_engine is None:
        repositories_engine = build_engine(symbol_settings.database_url)
        repositories = RepositorySet(repositories_engine)
        strategy_engine = StrategyEngine(settings=symbol_settings, repositories=repositories)
    else:
        repositories = primary_instance.repositories
        strategy_engine = primary_instance.strategy_engine
    container = ApplicationContainer(
        settings=symbol_settings,
        repositories=repositories,
        replay_feed=ReplayFeed(symbol_settings),
        strategy_engine=strategy_engine,
        strategy_runtime_registry=strategy_runtime_registry,
    )
    runner = StrategyServiceRunner(container)
    summary = runner.run_bars(loaded.playback_bars)
    trigger_rows = build_trigger_report(
        replay_db_path=replay_db_path,
        settings=symbol_settings,
        playback_bars=loaded.playback_bars,
    )
    _write_trigger_report_json(trigger_rows, trigger_report_json_path)
    _write_trigger_report_markdown(trigger_rows, trigger_report_markdown_path)
    common_metadata = {
        "mode": "REPLAY",
        "run_stamp": run_stamp,
        "source_db_path": str(source_db_path),
        "replay_db_path": str(replay_db_path),
        "summary_path": str(summary_path),
        "artifact_context": "HISTORICAL_PLAYBACK_STRATEGY_STUDY",
        "persistence_origin": "PERSISTED_RUNTIME_TRUTH",
    }
    if primary_instance is not None:
        study_payload = build_strategy_study_v3(
            repositories=primary_instance.repositories or repositories,
            settings=primary_instance.settings or symbol_settings,
            bars=loaded.playback_bars,
            source_bars=loaded.source_bars,
            point_value=primary_instance.definition.point_value,
            standalone_strategy_id=primary_instance.definition.standalone_strategy_id,
            strategy_family=primary_instance.definition.strategy_family,
            instrument=primary_instance.definition.instrument,
            run_metadata=common_metadata,
        )
    else:
        study_payload = build_strategy_study_v3(
            repositories=repositories,
            settings=symbol_settings,
            bars=loaded.playback_bars,
            source_bars=loaded.source_bars,
            point_value=None,
            standalone_strategy_id=None,
            strategy_family="LEGACY_RUNTIME",
            instrument=symbol,
            run_metadata=common_metadata,
        )
    write_strategy_study_json(study_payload, strategy_study_json_path)
    write_strategy_study_markdown(study_payload, strategy_study_markdown_path)

    summary_payload = {
        "run_stamp": run_stamp,
        "source_db_path": str(source_db_path),
        "replay_db_path": str(replay_db_path),
        "symbol": symbol,
        "source_timeframe": loaded.source_timeframe,
        "target_timeframe": loaded.target_timeframe,
        "environment_mode": symbol_settings.environment_mode.value,
        "structural_signal_timeframe": symbol_settings.resolved_structural_signal_timeframe,
        "execution_timeframe": symbol_settings.resolved_execution_timeframe,
        "artifact_timeframe": symbol_settings.resolved_artifact_timeframe,
        "execution_timeframe_role": symbol_settings.execution_timeframe_role.value,
        "selected_data_source": loaded.data_source,
        "start_timestamp": start_timestamp.isoformat() if start_timestamp is not None else None,
        "end_timestamp": end_timestamp.isoformat() if end_timestamp is not None else None,
        "source_bar_count": loaded.source_bar_count,
        "playback_bar_count": len(loaded.playback_bars),
        "skipped_incomplete_buckets": loaded.skipped_incomplete_buckets,
        "processed_bars": summary.processed_bars,
        "order_intents": summary.order_intents,
        "fills": summary.fills,
        "long_entries": summary.long_entries,
        "short_entries": summary.short_entries,
        "exits": summary.exits,
        "final_position_side": summary.final_position_side.value,
        "final_strategy_status": summary.final_strategy_status.value,
        "primary_standalone_strategy_id": summary.primary_standalone_strategy_id,
        "standalone_strategy_count": len(summary.per_strategy_summaries),
        "per_strategy_summaries": [asdict(item) for item in summary.per_strategy_summaries],
        "aggregate_portfolio_summary": asdict(summary.aggregate_portfolio_summary),
        "trigger_report_json_path": str(trigger_report_json_path),
        "trigger_report_markdown_path": str(trigger_report_markdown_path),
        "strategy_study_json_path": str(strategy_study_json_path),
        "strategy_study_markdown_path": str(strategy_study_markdown_path),
        "study_contract_version": study_payload.get("contract_version"),
        "study_id": dict(study_payload.get("meta") or {}).get("study_id"),
        "entry_model": dict(study_payload.get("meta") or {}).get("entry_model"),
        "active_entry_model": dict(study_payload.get("meta") or {}).get("active_entry_model"),
        "supported_entry_models": list(dict(study_payload.get("meta") or {}).get("supported_entry_models") or []),
        "entry_model_supported": dict(study_payload.get("meta") or {}).get("entry_model_supported"),
        "execution_truth_emitter": dict(study_payload.get("meta") or {}).get("execution_truth_emitter"),
        "authoritative_intrabar_available": dict(study_payload.get("meta") or {}).get("authoritative_intrabar_available"),
        "authoritative_entry_truth_available": dict(study_payload.get("meta") or {}).get("authoritative_entry_truth_available"),
        "authoritative_exit_truth_available": dict(study_payload.get("meta") or {}).get("authoritative_exit_truth_available"),
        "authoritative_trade_lifecycle_available": dict(study_payload.get("meta") or {}).get("authoritative_trade_lifecycle_available"),
        "pnl_truth_basis": dict(study_payload.get("meta") or {}).get("pnl_truth_basis"),
        "lifecycle_truth_class": dict(study_payload.get("meta") or {}).get("lifecycle_truth_class"),
        "unsupported_reason": dict(study_payload.get("meta") or {}).get("unsupported_reason"),
        "truth_provenance": dict(dict(study_payload.get("meta") or {}).get("truth_provenance") or {}),
        "config_paths": list(config_paths),
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return HistoricalPlaybackSymbolResult(
        symbol=symbol,
        run_stamp=run_stamp,
        source_db_path=str(source_db_path),
        replay_db_path=str(replay_db_path),
        summary_path=str(summary_path),
        trigger_report_json_path=str(trigger_report_json_path),
        trigger_report_markdown_path=str(trigger_report_markdown_path),
        strategy_study_json_path=str(strategy_study_json_path),
        strategy_study_markdown_path=str(strategy_study_markdown_path),
        source_timeframe=loaded.source_timeframe,
        target_timeframe=loaded.target_timeframe,
        environment_mode=symbol_settings.environment_mode.value,
        structural_signal_timeframe=symbol_settings.resolved_structural_signal_timeframe,
        execution_timeframe=symbol_settings.resolved_execution_timeframe,
        artifact_timeframe=symbol_settings.resolved_artifact_timeframe,
        execution_timeframe_role=symbol_settings.execution_timeframe_role.value,
        selected_data_source=loaded.data_source,
        source_bar_count=loaded.source_bar_count,
        playback_bar_count=len(loaded.playback_bars),
        skipped_incomplete_buckets=loaded.skipped_incomplete_buckets,
        processed_bars=summary.processed_bars,
        order_intents=summary.order_intents,
        fills=summary.fills,
        long_entries=summary.long_entries,
        short_entries=summary.short_entries,
        exits=summary.exits,
        final_position_side=summary.final_position_side.value,
        final_strategy_status=summary.final_strategy_status.value,
        primary_standalone_strategy_id=summary.primary_standalone_strategy_id,
        standalone_strategy_count=len(summary.per_strategy_summaries),
        per_strategy_summaries=[asdict(item) for item in summary.per_strategy_summaries],
        aggregate_portfolio_summary=asdict(summary.aggregate_portfolio_summary),
    )


def _derived_strategy_study_paths(*, summary_path: Path, summary_payload: dict[str, Any]) -> tuple[Path, Path]:
    json_path = summary_payload.get("strategy_study_json_path")
    markdown_path = summary_payload.get("strategy_study_markdown_path")
    if json_path and markdown_path:
        return Path(str(json_path)).resolve(), Path(str(markdown_path)).resolve()
    prefix = Path(str(summary_path).removesuffix(".summary.json"))
    return prefix.with_suffix(".strategy_study.json"), prefix.with_suffix(".strategy_study.md")


def _parse_optional_iso_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    return datetime.fromisoformat(text)


def build_trigger_report(
    *,
    replay_db_path: str | Path,
    settings: StrategySettings,
    playback_bars: Sequence[Any],
) -> list[TriggerReportRow]:
    engine = build_engine(f"sqlite:///{Path(replay_db_path).resolve()}")
    with engine.begin() as connection:
        signal_rows = connection.execute(select(signals_table).order_by(signals_table.c.created_at.asc())).mappings().all()
        intent_rows = connection.execute(
            select(order_intents_table).order_by(order_intents_table.c.created_at.asc())
        ).mappings().all()
        fill_rows = connection.execute(select(fills_table).order_by(fills_table.c.fill_timestamp.asc())).mappings().all()
        state_rows = connection.execute(
            select(strategy_state_snapshots_table).order_by(strategy_state_snapshots_table.c.updated_at.asc())
        ).mappings().all()
        fault_rows = connection.execute(select(fault_events_table).order_by(fault_events_table.c.created_at.asc())).mappings().all()

    configured_groups = {(family, side) for family, side in _configured_trigger_families(settings)}
    bar_index_by_id = {bar.bar_id: index for index, bar in enumerate(playback_bars)}
    bar_end_ts_by_id = {bar.bar_id: bar.end_ts for bar in playback_bars}
    grouped: dict[tuple[str, str], _GroupStats] = {key: _GroupStats() for key in configured_groups}

    for row in signal_rows:
        payload = json.loads(str(row["payload_json"]))
        bar_id = str(row["bar_id"])
        trigger = _infer_trigger_family(payload)
        if trigger is None:
            continue
        stats = grouped.setdefault(trigger, _GroupStats())
        stats.signals_seen += 1
        trigger_ts = bar_end_ts_by_id.get(bar_id, datetime.fromisoformat(str(row["created_at"])))
        if stats.first_trigger_timestamp is None:
            stats.first_trigger_timestamp = trigger_ts
            stats.first_trigger_bar_id = bar_id

    intents_by_id = {str(row["order_intent_id"]): row for row in intent_rows}
    for row in intent_rows:
        intent_type = OrderIntentType(str(row["intent_type"]))
        if intent_type not in (OrderIntentType.BUY_TO_OPEN, OrderIntentType.SELL_TO_OPEN):
            continue
        key = (str(row["reason_code"]), "LONG" if intent_type == OrderIntentType.BUY_TO_OPEN else "SHORT")
        stats = grouped.setdefault(key, _GroupStats())
        stats.intents_created += 1
        created_at = datetime.fromisoformat(str(row["created_at"]))
        if stats.first_intent_timestamp is None:
            stats.first_intent_timestamp = created_at

    for row in fill_rows:
        intent_row = intents_by_id.get(str(row["order_intent_id"]))
        if intent_row is None:
            continue
        intent_type = OrderIntentType(str(intent_row["intent_type"]))
        if intent_type not in (OrderIntentType.BUY_TO_OPEN, OrderIntentType.SELL_TO_OPEN):
            continue
        key = (str(intent_row["reason_code"]), "LONG" if intent_type == OrderIntentType.BUY_TO_OPEN else "SHORT")
        stats = grouped.setdefault(key, _GroupStats())
        stats.fills_created += 1
        fill_timestamp = datetime.fromisoformat(str(row["fill_timestamp"]))
        if stats.first_fill_timestamp is None:
            stats.first_fill_timestamp = fill_timestamp

    states = [(datetime.fromisoformat(str(row["updated_at"])), decode_strategy_state(str(row["payload_json"]))) for row in state_rows]
    faults = [(datetime.fromisoformat(str(row["created_at"])), str(row["fault_code"])) for row in fault_rows]
    bars_processed = len(playback_bars)
    last_bar = playback_bars[-1] if playback_bars else None

    rows: list[TriggerReportRow] = []
    for (family, side), stats in sorted(grouped.items()):
        block_reason = _resolve_group_block_reason(
            family=family,
            side=side,
            stats=stats,
            settings=settings,
            states=states,
            faults=faults,
            bar_index_by_id=bar_index_by_id,
            last_bar_id=last_bar.bar_id if last_bar is not None else None,
        )
        rows.append(
            TriggerReportRow(
                symbol=settings.symbol,
                lane_family=family,
                side=side,
                reason_code=family,
                bars_processed=bars_processed,
                signals_seen=stats.signals_seen,
                intents_created=stats.intents_created,
                fills_created=stats.fills_created,
                first_trigger_timestamp=stats.first_trigger_timestamp.isoformat() if stats.first_trigger_timestamp else None,
                first_intent_timestamp=stats.first_intent_timestamp.isoformat() if stats.first_intent_timestamp else None,
                first_fill_timestamp=stats.first_fill_timestamp.isoformat() if stats.first_fill_timestamp else None,
                block_or_fault_reason=block_reason,
            )
        )

    if rows:
        return rows

    return [
        TriggerReportRow(
            symbol=settings.symbol,
            lane_family="ALL",
            side="UNKNOWN",
            reason_code="ALL",
            bars_processed=bars_processed,
            signals_seen=0,
            intents_created=0,
            fills_created=0,
            first_trigger_timestamp=None,
            first_intent_timestamp=None,
            first_fill_timestamp=None,
            block_or_fault_reason=faults[-1][1] if faults else "no_trigger_seen",
        )
    ]


def _resolve_group_block_reason(
    *,
    family: str,
    side: str,
    stats: _GroupStats,
    settings: StrategySettings,
    states: Sequence[tuple[datetime, Any]],
    faults: Sequence[tuple[datetime, str]],
    bar_index_by_id: dict[str, int],
    last_bar_id: str | None,
) -> str | None:
    if stats.fills_created > 0:
        return None
    if stats.intents_created > 0:
        if stats.first_intent_timestamp is not None and stats.first_trigger_bar_id is not None and stats.first_trigger_bar_id == last_bar_id:
            return "awaiting_next_bar_open_fill"
        return "intent_created_without_fill"
    if stats.signals_seen == 0:
        return faults[-1][1] if faults else "no_trigger_seen"

    if stats.first_trigger_timestamp is not None:
        runtime_block = _runtime_block_reason(settings=settings, side=side, family=family, trigger_timestamp=stats.first_trigger_timestamp)
        if runtime_block is not None:
            return runtime_block

    if stats.first_trigger_bar_id is not None:
        trigger_index = bar_index_by_id.get(stats.first_trigger_bar_id)
        if trigger_index is not None and trigger_index + 1 < settings.warmup_bars_required():
            return "warmup_not_complete"

    if stats.first_trigger_timestamp is not None:
        state = _latest_state_before(states, stats.first_trigger_timestamp)
        if state is not None:
            if state.fault_code:
                return str(state.fault_code)
            if state.operator_halt:
                return "operator_halt"
            if not state.entries_enabled:
                return "entries_disabled"
            if state.position_side.value != "FLAT":
                return f"position_{state.position_side.value.lower()}"
            if state.strategy_status.value != "READY":
                return f"strategy_status_{state.strategy_status.value.lower()}"

    if faults:
        return faults[-1][1]
    return "trigger_seen_no_intent"


def _latest_state_before(states: Sequence[tuple[datetime, Any]], trigger_timestamp: datetime):
    latest = None
    for updated_at, state in states:
        if updated_at <= trigger_timestamp:
            latest = state
        else:
            break
    return latest


def _runtime_block_reason(
    *,
    settings: StrategySettings,
    side: str,
    family: str,
    trigger_timestamp: datetime,
) -> str | None:
    if side == "LONG":
        if (
            settings.us_late_pause_resume_long_exclude_1755_carryover
            and family == "usLatePauseResumeLongTurn"
            and trigger_timestamp.astimezone(settings.timezone_info).time().strftime("%H:%M:%S") == "16:55:00"
        ):
            return "us_late_1755_carryover_exclusion"
        if settings.probationary_paper_lane_session_restriction and not _matches_session_restriction(
            family=family,
            restriction=settings.probationary_paper_lane_session_restriction,
            symbol=settings.symbol,
            trigger_timestamp=trigger_timestamp,
        ):
            return f"probationary_session_restriction_{settings.probationary_paper_lane_session_restriction.lower()}"
        if settings.probationary_enforce_approved_branches and family not in settings.approved_long_entry_sources:
            return "probationary_long_source_not_allowlisted"
        return None

    if settings.probationary_paper_lane_session_restriction and not _matches_session_restriction(
        family=family,
        restriction=settings.probationary_paper_lane_session_restriction,
        symbol=settings.symbol,
        trigger_timestamp=trigger_timestamp,
    ):
        return f"probationary_session_restriction_{settings.probationary_paper_lane_session_restriction.lower()}"
    if settings.probationary_enforce_approved_branches and family not in settings.approved_short_entry_sources:
        return "probationary_short_source_not_allowlisted"
    return None


def _matches_session_restriction(*, family: str, restriction: str, symbol: str, trigger_timestamp: datetime) -> bool:
    normalized = restriction.upper()
    if normalized == "US_LATE":
        return family in {"usLatePauseResumeLongTurn", "usLateFailedMoveReversalLongTurn", "usLateBreakoutRetestHoldTurn"}
    if normalized == "ASIA_EARLY":
        if (
            family == "asiaEarlyNormalBreakoutRetestHoldTurn"
            and str(symbol or "").upper() in {"GC", "MGC"}
            and label_session_phase(trigger_timestamp) == "LONDON_OPEN"
            and trigger_timestamp.timetz().replace(tzinfo=None).strftime("%H:%M:%S") in {"03:05:00", "03:10:00", "03:15:00"}
        ):
            return True
        return family in {
            "asiaEarlyNormalBreakoutRetestHoldTurn",
            "asiaEarlyBreakoutRetestHoldTurn",
            "asiaEarlyPauseResumeShortTurn",
            "asiaEarlyCompressedPauseResumeShortTurn",
            "asiaEarlyExpandedBreakoutRetestHoldShortTurn",
        }
    return True


def _configured_trigger_families(settings: StrategySettings) -> list[tuple[str, str]]:
    configured: list[tuple[str, str]] = []
    if settings.enable_asia_vwap_longs:
        configured.append(("asiaVWAPLongSignal", "LONG"))
    if settings.enable_bull_snap_longs:
        configured.append(("firstBullSnapTurn", "LONG"))
    if settings.enable_us_midday_pause_resume_longs:
        configured.append(("usMiddayPauseResumeLongTurn", "LONG"))
    if settings.enable_us_late_pause_resume_longs:
        configured.append(("usLatePauseResumeLongTurn", "LONG"))
    if settings.enable_us_late_failed_move_reversal_longs:
        configured.append(("usLateFailedMoveReversalLongTurn", "LONG"))
    if settings.enable_us_late_breakout_retest_hold_longs:
        configured.append(("usLateBreakoutRetestHoldTurn", "LONG"))
    if settings.enable_asia_early_normal_breakout_retest_hold_longs:
        configured.append(("asiaEarlyNormalBreakoutRetestHoldTurn", "LONG"))
    if settings.enable_asia_early_breakout_retest_hold_longs:
        configured.append(("asiaEarlyBreakoutRetestHoldTurn", "LONG"))
    if settings.enable_asia_late_pause_resume_longs:
        configured.append(("asiaLatePauseResumeLongTurn", "LONG"))
    if settings.enable_asia_late_flat_pullback_pause_resume_longs:
        configured.append(("asiaLateFlatPullbackPauseResumeLongTurn", "LONG"))
    if settings.enable_asia_late_compressed_flat_pullback_pause_resume_longs:
        configured.append(("asiaLateCompressedFlatPullbackPauseResumeLongTurn", "LONG"))
    if settings.enable_bear_snap_shorts:
        configured.append(("firstBearSnapTurn", "SHORT"))
    if settings.enable_us_derivative_bear_shorts:
        configured.append(("usDerivativeBearTurn", "SHORT"))
    if settings.enable_us_derivative_bear_additive_shorts:
        configured.append(("usDerivativeBearAdditiveTurn", "SHORT"))
    if settings.enable_us_midday_compressed_rebound_failed_move_reversal_shorts:
        configured.append(("usMiddayCompressedReboundFailedMoveReversalShortTurn", "SHORT"))
    if settings.enable_us_midday_compressed_failed_move_reversal_shorts:
        configured.append(("usMiddayCompressedFailedMoveReversalShortTurn", "SHORT"))
    if settings.enable_us_midday_expanded_pause_resume_shorts:
        configured.append(("usMiddayExpandedPauseResumeShortTurn", "SHORT"))
    if settings.enable_us_midday_compressed_pause_resume_shorts:
        configured.append(("usMiddayCompressedPauseResumeShortTurn", "SHORT"))
    if settings.enable_us_midday_pause_resume_shorts:
        configured.append(("usMiddayPauseResumeShortTurn", "SHORT"))
    if settings.enable_london_late_pause_resume_shorts:
        configured.append(("londonLatePauseResumeShortTurn", "SHORT"))
    if settings.enable_asia_early_expanded_breakout_retest_hold_shorts:
        configured.append(("asiaEarlyExpandedBreakoutRetestHoldShortTurn", "SHORT"))
    if settings.enable_asia_early_compressed_pause_resume_shorts:
        configured.append(("asiaEarlyCompressedPauseResumeShortTurn", "SHORT"))
    if settings.enable_asia_early_pause_resume_shorts:
        configured.append(("asiaEarlyPauseResumeShortTurn", "SHORT"))
    return configured


def _infer_trigger_family(payload: dict[str, Any]) -> tuple[str, str] | None:
    if payload.get("long_entry_source"):
        return str(payload["long_entry_source"]), "LONG"
    if payload.get("short_entry_source"):
        return str(payload["short_entry_source"]), "SHORT"

    long_checks = [
        ("asia_vwap_long_signal", "asiaVWAPLongSignal"),
        ("first_bull_snap_turn", "firstBullSnapTurn"),
        ("midday_pause_resume_long_turn_candidate", "usMiddayPauseResumeLongTurn"),
        ("us_late_breakout_retest_hold_long_turn_candidate", "usLateBreakoutRetestHoldTurn"),
        ("us_late_failed_move_reversal_long_turn_candidate", "usLateFailedMoveReversalLongTurn"),
        ("us_late_pause_resume_long_turn_candidate", "usLatePauseResumeLongTurn"),
        ("asia_early_normal_breakout_retest_hold_long_turn_candidate", "asiaEarlyNormalBreakoutRetestHoldTurn"),
        ("asia_early_breakout_retest_hold_long_turn_candidate", "asiaEarlyBreakoutRetestHoldTurn"),
        ("asia_late_compressed_flat_pullback_pause_resume_long_turn_candidate", "asiaLateCompressedFlatPullbackPauseResumeLongTurn"),
        ("asia_late_flat_pullback_pause_resume_long_turn_candidate", "asiaLateFlatPullbackPauseResumeLongTurn"),
        ("asia_late_pause_resume_long_turn_candidate", "asiaLatePauseResumeLongTurn"),
    ]
    for key, family in long_checks:
        if payload.get(key):
            return family, "LONG"

    short_checks = [
        ("first_bear_snap_turn", "firstBearSnapTurn"),
        ("derivative_bear_turn_candidate", "usDerivativeBearTurn"),
        ("derivative_bear_additive_turn_candidate", "usDerivativeBearAdditiveTurn"),
        ("midday_compressed_rebound_failed_move_reversal_short_turn_candidate", "usMiddayCompressedReboundFailedMoveReversalShortTurn"),
        ("midday_compressed_failed_move_reversal_short_turn_candidate", "usMiddayCompressedFailedMoveReversalShortTurn"),
        ("midday_expanded_pause_resume_short_turn_candidate", "usMiddayExpandedPauseResumeShortTurn"),
        ("midday_compressed_pause_resume_short_turn_candidate", "usMiddayCompressedPauseResumeShortTurn"),
        ("midday_pause_resume_short_turn_candidate", "usMiddayPauseResumeShortTurn"),
        ("london_late_pause_resume_short_turn_candidate", "londonLatePauseResumeShortTurn"),
        ("asia_early_expanded_breakout_retest_hold_short_turn_candidate", "asiaEarlyExpandedBreakoutRetestHoldShortTurn"),
        ("asia_early_compressed_pause_resume_short_turn_candidate", "asiaEarlyCompressedPauseResumeShortTurn"),
        ("asia_early_pause_resume_short_turn_candidate", "asiaEarlyPauseResumeShortTurn"),
    ]
    for key, family in short_checks:
        if payload.get(key):
            return family, "SHORT"
    return None


def _write_trigger_report_json(rows: Sequence[TriggerReportRow], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = [asdict(row) for row in rows]
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output_path


def _write_trigger_report_markdown(rows: Sequence[TriggerReportRow], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Historical Playback Trigger Report",
        "",
        "| Symbol | Lane Family | Side | Bars | Signals | Intents | Fills | First Trigger | First Intent | First Fill | Block/Fault |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    row.symbol,
                    row.lane_family,
                    row.side,
                    str(row.bars_processed),
                    str(row.signals_seen),
                    str(row.intents_created),
                    str(row.fills_created),
                    row.first_trigger_timestamp or "-",
                    row.first_intent_timestamp or "-",
                    row.first_fill_timestamp or "-",
                    row.block_or_fault_reason or "-",
                ]
            )
            + " |"
        )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path
