"""Publish stitched ATP historical studies that extend intact pre-gap studies with repaired post-gap playback."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence
from zoneinfo import ZoneInfo

from . import strategy_universe_retest as retest
from .atp_loosened_history_publish import (
    DEFAULT_SOURCE_DB,
    _apply_production_track_overlay,
    _latest_manifest_path,
    _load_loosened_lane_from_config,
    _load_manifest_study_rows,
    _merge_study_rows,
)

REPO_ROOT = Path.cwd()
DEFAULT_REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "atp_continuous_backfill_v1"
DEFAULT_HISTORICAL_PLAYBACK_DIR = REPO_ROOT / "outputs" / "historical_playback"
DEFAULT_START_TIMESTAMP = datetime.fromisoformat("2024-01-01T00:00:00+00:00")
DEFAULT_BACKFILL_START_TIMESTAMP = datetime.fromisoformat("2026-04-10T21:00:00+00:00")
DEFAULT_BACKFILL_END_TIMESTAMP = datetime.fromisoformat("2026-04-16T07:00:00+00:00")
DEFAULT_STUDY_SUFFIX = "_continuous_backfill_20260416"
DEFAULT_LABEL_SUFFIX = " [Continuous Backfill 2026-04-16]"
NY_TZ = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class BackfillLaneSpec:
    config_path: Path
    base_study_path: Path


DEFAULT_BACKFILL_SPECS: tuple[BackfillLaneSpec, ...] = (
    BackfillLaneSpec(
        config_path=REPO_ROOT / "config" / "probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us_production_track.yaml",
        base_study_path=REPO_ROOT
        / "outputs"
        / "historical_playback"
        / "historical_playback_atp_companion_v1__production_track_gc_asia_us_loosened_v1.strategy_study.json",
    ),
    BackfillLaneSpec(
        config_path=REPO_ROOT / "config" / "probationary_pattern_engine_paper_atp_companion_v1_gc_asia_promotion_1_075r_favorable_only.yaml",
        base_study_path=REPO_ROOT
        / "outputs"
        / "historical_playback"
        / "historical_playback_atp_companion_v1__paper_gc_asia__promotion_1_075r_favorable_only_loosened_v1.strategy_study.json",
    ),
    BackfillLaneSpec(
        config_path=REPO_ROOT / "config" / "probationary_pattern_engine_paper_atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only.yaml",
        base_study_path=REPO_ROOT
        / "outputs"
        / "historical_playback"
        / "historical_playback_atp_companion_v1__paper_mgc_asia__promotion_1_075r_favorable_only_loosened_v1.strategy_study.json",
    ),
    BackfillLaneSpec(
        config_path=REPO_ROOT / "config" / "probationary_pattern_engine_paper_atp_companion_v1_pl_asia_us.yaml",
        base_study_path=REPO_ROOT
        / "outputs"
        / "historical_playback"
        / "historical_playback_atp_companion_v1__paper_pl_asia_us_loosened_v1.strategy_study.json",
    ),
)


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(str(value))


def _ny_date(value: str | datetime) -> str:
    timestamp = value if isinstance(value, datetime) else _parse_ts(value)
    return timestamp.astimezone(NY_TZ).date().isoformat()


def _load_base_study(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = list(payload.get("summary", {}).get("closed_trade_breakdown", []) or [])
    if not rows:
        raise ValueError(f"Base study {path} has no closed_trade_breakdown rows")
    return {
        "path": path,
        "payload": payload,
        "rows": rows,
        "study_id": str(payload.get("standalone_strategy_id") or (payload.get("meta") or {}).get("study_id") or path.stem),
        "candidate_id": (payload.get("meta") or {}).get("candidate_id"),
        "last_exit": max(_parse_ts(str(row["exit_timestamp"])) for row in rows),
        "first_entry": min(_parse_ts(str(row["entry_timestamp"])) for row in rows),
    }


def _daily_rate(*, rows: Sequence[dict[str, Any]], start: datetime, end: datetime) -> float:
    if end < start:
        return 0.0
    day_count = ((end.astimezone(NY_TZ).date() - start.astimezone(NY_TZ).date()).days) + 1
    if day_count <= 0:
        return 0.0
    matched = [
        row
        for row in rows
        if start <= _parse_ts(str(row["exit_timestamp"])) <= end
    ]
    return len(matched) / float(day_count)


def _faithfulness_diagnostics(
    *,
    base_rows: Sequence[dict[str, Any]],
    appended_rows: Sequence[dict[str, Any]],
    base_last_exit: datetime,
    evaluation_end: datetime,
) -> dict[str, Any]:
    prior_window_start = base_last_exit - timedelta(days=14)
    prior_daily_rate = _daily_rate(rows=base_rows, start=prior_window_start, end=base_last_exit)
    appended_daily_rate = _daily_rate(rows=appended_rows, start=DEFAULT_BACKFILL_START_TIMESTAMP, end=evaluation_end)
    latest_appended_exit = (
        max(_parse_ts(str(row["exit_timestamp"])) for row in appended_rows)
        if appended_rows
        else None
    )
    rate_ratio = (
        round(appended_daily_rate / prior_daily_rate, 4)
        if prior_daily_rate > 0
        else None
    )
    latest_is_fresh = (
        latest_appended_exit is not None and latest_appended_exit >= (evaluation_end - timedelta(hours=8))
    )
    faithful = bool(appended_rows) and latest_is_fresh and (
        rate_ratio is None or rate_ratio >= 0.1
    )
    return {
        "prior_daily_trade_rate_14d": round(prior_daily_rate, 4),
        "backfill_daily_trade_rate": round(appended_daily_rate, 4),
        "rate_ratio": rate_ratio,
        "latest_appended_exit": latest_appended_exit.isoformat() if latest_appended_exit is not None else None,
        "latest_exit_is_fresh": latest_is_fresh,
        "faithful_expression": faithful,
    }


def _blocked_row(*, spec: BackfillLaneSpec, reason: str) -> dict[str, Any]:
    return {
        "config_path": str(spec.config_path),
        "base_study_path": str(spec.base_study_path),
        "status": "blocked",
        "reason": reason,
    }


def _render_report_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ATP Continuous Backfill",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Full history start: `{payload['full_start_timestamp']}`",
        f"- Backfill window start: `{payload['backfill_start_timestamp']}`",
        f"- Backfill window end: `{payload['backfill_end_timestamp']}`",
        "",
    ]
    for row in payload.get("lanes", []):
        lines.append(f"## {row['strategy_id']}")
        lines.append("")
        lines.append(f"- Status: `{row['status']}`")
        if row["status"] != "published":
            lines.append(f"- Reason: `{row['reason']}`")
            lines.append("")
            continue
        lines.append(f"- Base trade count: `{row['base_trade_count']}`")
        lines.append(f"- Appended trade count: `{row['appended_trade_count']}`")
        lines.append(f"- Combined trade count: `{row['combined_trade_count']}`")
        lines.append(f"- Base last exit: `{row['base_last_exit']}`")
        lines.append(f"- First appended entry: `{row['first_appended_entry']}`")
        lines.append(f"- Last appended exit: `{row['last_appended_exit']}`")
        lines.append(f"- Faithful expression: `{row['faithfulness']['faithful_expression']}`")
        lines.append(f"- Prior 14d daily trade rate: `{row['faithfulness']['prior_daily_trade_rate_14d']}`")
        lines.append(f"- Backfill daily trade rate: `{row['faithfulness']['backfill_daily_trade_rate']}`")
        lines.append(f"- Rate ratio: `{row['faithfulness']['rate_ratio']}`")
        lines.append(f"- Study JSON: `{row['strategy_study_json_path']}`")
        lines.append("")
    return "\n".join(lines) + "\n"


def run_atp_continuous_backfill_publish(
    *,
    report_dir: Path = DEFAULT_REPORT_DIR,
    historical_playback_dir: Path = DEFAULT_HISTORICAL_PLAYBACK_DIR,
    source_database_paths: Sequence[str | Path] | None = None,
    full_start_timestamp: datetime = DEFAULT_START_TIMESTAMP,
    backfill_start_timestamp: datetime = DEFAULT_BACKFILL_START_TIMESTAMP,
    backfill_end_timestamp: datetime = DEFAULT_BACKFILL_END_TIMESTAMP,
    study_suffix: str = DEFAULT_STUDY_SUFFIX,
    label_suffix: str = DEFAULT_LABEL_SUFFIX,
    specs: Sequence[BackfillLaneSpec] | None = None,
) -> dict[str, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    historical_playback_dir.mkdir(parents=True, exist_ok=True)
    resolved_specs = tuple(specs or DEFAULT_BACKFILL_SPECS)
    resolved_source_paths = tuple(Path(path) for path in (source_database_paths or (DEFAULT_SOURCE_DB,)))
    active_symbols = {
        _load_base_study(spec.base_study_path)["payload"]["symbol"]
        for spec in resolved_specs
    }
    bar_source_index = retest._discover_best_sources(
        symbols=active_symbols,
        timeframes={"1m", "5m"},
        sqlite_paths=resolved_source_paths,
    )
    loaded_contexts: dict[str, dict[str, Any] | None] = {}
    report_rows: list[dict[str, Any]] = []
    new_studies: list[dict[str, Any]] = []

    for spec in resolved_specs:
        try:
            base = _load_base_study(spec.base_study_path)
        except Exception as exc:
            report_rows.append(_blocked_row(spec=spec, reason=f"base_study_load_failed:{exc}"))
            continue
        lane = _load_loosened_lane_from_config(
            config_path=spec.config_path,
            study_suffix=study_suffix,
            label_suffix=label_suffix,
        )
        symbol = str(base["payload"]["symbol"])
        current = retest._evaluate_atp_lane(
            symbol=symbol,
            allowed_sessions=set(str(session) for session in (lane.get("allowed_sessions") or [])),
            point_value=Decimal(str(lane["point_value"])),
            bar_source_index=bar_source_index,
            start_timestamp=backfill_start_timestamp,
            end_timestamp=backfill_end_timestamp,
            include_prior_comparator=False,
            quality_bucket_policy=str(lane.get("quality_bucket_policy") or "").strip() or None,
            allow_pre_5m_context_participation=bool(lane.get("allow_pre_5m_context_participation", False)),
            sides=tuple(
                side
                for side, sources in (
                    ("LONG", lane.get("long_sources") or ()),
                    ("SHORT", lane.get("short_sources") or ()),
                )
                if sources
            )
            or ("LONG",),
        )
        if current is None:
            report_rows.append(_blocked_row(spec=spec, reason="backfill_evaluation_returned_none"))
            continue
        repaired_trade_rows = _apply_production_track_overlay(
            trade_rows=current["trade_rows"],
            bars_1m=current["bars_1m"],
            point_value=Decimal(str(lane["point_value"])),
            runtime_overlay_id=str(lane.get("runtime_overlay_id") or "").strip() or None,
            runtime_overlay_params=dict(lane.get("runtime_overlay_params") or {}),
        )
        base_rows = list(base["rows"])
        base_trade_ids = {str(row["trade_id"]) for row in base_rows}
        base_last_exit = base["last_exit"]
        appended_rows = [
            {
                key: value
                for key, value in row.items()
                if key
                in {
                    "trade_id",
                    "entry_timestamp",
                    "exit_timestamp",
                    "entry_price",
                    "exit_price",
                    "side",
                    "family",
                    "entry_session_phase",
                    "exit_reason",
                    "realized_pnl",
                }
            }
            for row in repaired_trade_rows
            if str(row.get("trade_id") or "") not in base_trade_ids
            and _parse_ts(str(row["exit_timestamp"])) > base_last_exit
        ]
        faithfulness = _faithfulness_diagnostics(
            base_rows=base_rows,
            appended_rows=appended_rows,
            base_last_exit=base_last_exit,
            evaluation_end=backfill_end_timestamp,
        )
        if not faithfulness["faithful_expression"]:
            report_rows.append(
                {
                    "strategy_id": str(lane["strategy_id"]),
                    "config_path": str(spec.config_path),
                    "base_study_path": str(spec.base_study_path),
                    "status": "blocked",
                    "reason": "faithfulness_check_failed",
                    "base_trade_count": len(base_rows),
                    "appended_trade_count": len(appended_rows),
                    "base_last_exit": base_last_exit.isoformat(),
                    "faithfulness": faithfulness,
                }
            )
            continue
        if symbol not in loaded_contexts:
            loaded_contexts[symbol] = retest._load_symbol_context(
                symbol=symbol,
                bar_source_index=bar_source_index,
                start_timestamp=full_start_timestamp,
                end_timestamp=backfill_end_timestamp,
            )
        loaded_context = loaded_contexts[symbol]
        if loaded_context is None:
            report_rows.append(_blocked_row(spec=spec, reason="full_context_load_failed"))
            continue
        combined_rows = sorted(
            [*base_rows, *appended_rows],
            key=lambda row: (_parse_ts(str(row["entry_timestamp"])), str(row["trade_id"])),
        )
        study_payload = retest._build_synthetic_strategy_study(
            symbol=symbol,
            study_id=str(lane["strategy_id"]),
            display_name=str(lane["display_name"]),
            strategy_family=str(base["payload"]["strategy_family"] or lane["family"]),
            study_mode="research_execution_mode",
            bars_1m=list(loaded_context["bars_1m"]),
            trade_rows=combined_rows,
            point_value=Decimal(str(lane["point_value"])),
            candidate_id=base["candidate_id"],
            entry_model="CURRENT_CANDLE_VWAP",
            execution_model_label=retest.EXECUTION_MODEL_ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP,
            pnl_truth_basis="ENRICHED_EXECUTION_TRUTH",
            lifecycle_truth_class="AUTHORITATIVE_INTRABAR_ENTRY_ONLY",
            intrabar_execution_authoritative=True,
            authoritative_intrabar_available=True,
        )
        study_path_pair = retest._write_study_payload(
            payload=study_payload,
            artifact_prefix=f"historical_playback_{lane['strategy_id']}",
            historical_playback_dir=historical_playback_dir,
        )
        metrics = retest._summarize_trade_rows(combined_rows, bar_count=max(len(loaded_context["bars_1m"]), 1))
        new_studies.append(
            {
                "strategy_id": lane["strategy_id"],
                "symbol": symbol,
                "label": lane["display_name"],
                "study_mode": "research_execution_mode",
                "execution_model": retest.EXECUTION_MODEL_ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP,
                "summary_payload": retest._study_summary_payload(metrics, lane["lane_status"]),
                "strategy_study_json_path": str(study_path_pair["json"]),
                "strategy_study_markdown_path": str(study_path_pair["markdown"]),
            }
        )
        report_rows.append(
            {
                "strategy_id": str(lane["strategy_id"]),
                "config_path": str(spec.config_path),
                "base_study_path": str(spec.base_study_path),
                "status": "published",
                "base_trade_count": len(base_rows),
                "appended_trade_count": len(appended_rows),
                "combined_trade_count": len(combined_rows),
                "base_last_exit": base_last_exit.isoformat(),
                "first_appended_entry": appended_rows[0]["entry_timestamp"] if appended_rows else None,
                "last_appended_exit": appended_rows[-1]["exit_timestamp"] if appended_rows else None,
                "faithfulness": faithfulness,
                "strategy_study_json_path": str(study_path_pair["json"]),
                "strategy_study_markdown_path": str(study_path_pair["markdown"]),
            }
        )

    latest_manifest = _latest_manifest_path(historical_playback_dir)
    existing_studies = _load_manifest_study_rows(latest_manifest) if latest_manifest is not None else []
    merged_studies = _merge_study_rows(existing_studies=existing_studies, new_studies=new_studies)
    manifest_run_stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    manifest_path = retest._write_historical_playback_manifest(
        studies=merged_studies,
        run_stamp=manifest_run_stamp,
        historical_playback_dir=historical_playback_dir,
        shard_config=retest.RetestShardConfig(),
    )
    blocked_gc_broad = {
        "strategy_id": "atp_companion_v1__paper_gc_asia_us_loosened_v1_continuous_backfill_20260416",
        "status": "blocked",
        "reason": "base_study_corrupted_preexisting_26_trade_overwrite",
        "base_study_path": str(
            REPO_ROOT
            / "outputs"
            / "historical_playback"
            / "historical_playback_atp_companion_v1__paper_gc_asia_us_loosened_v1.strategy_study.json"
        ),
    }
    report_payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "full_start_timestamp": full_start_timestamp.isoformat(),
        "backfill_start_timestamp": backfill_start_timestamp.isoformat(),
        "backfill_end_timestamp": backfill_end_timestamp.isoformat(),
        "historical_playback_manifest": str(manifest_path),
        "latest_manifest_before_merge": str(latest_manifest) if latest_manifest is not None else None,
        "lanes": [*report_rows, blocked_gc_broad],
    }
    json_path = report_dir / "atp_continuous_backfill_v1.json"
    markdown_path = report_dir / "atp_continuous_backfill_v1.md"
    json_path.write_text(json.dumps(retest._json_ready(report_payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_report_markdown(report_payload), encoding="utf-8")
    return {
        "report_json_path": json_path,
        "report_markdown_path": markdown_path,
        "historical_playback_manifest_path": manifest_path,
    }
