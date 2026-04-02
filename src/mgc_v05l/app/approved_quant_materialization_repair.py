"""Repair and validate silent approved-quant materialization failures."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import text

from ..config_models import load_settings_from_files
from ..domain.models import Bar
from .probationary_runtime import _configured_probationary_paper_lane_rows
from .session_phase_labels import label_session_phase
from .strategy_runtime_registry import build_strategy_runtime_registry


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "approved_quant_materialization_repair"
DEFAULT_SOURCE_DATABASE = REPO_ROOT / "mgc_v05l.replay.sqlite3"
DEFAULT_CONFIG_PATHS = (
    REPO_ROOT / "config" / "base.yaml",
    REPO_ROOT / "config" / "live.yaml",
    REPO_ROOT / "config" / "probationary_pattern_engine.yaml",
    REPO_ROOT / "config" / "probationary_pattern_engine_paper.yaml",
)
TARGET_LANES = (
    "breakout_metals_us_unknown_continuation__GC",
    "breakout_metals_us_unknown_continuation__HG",
    "breakout_metals_us_unknown_continuation__MGC",
    "breakout_metals_us_unknown_continuation__PL",
    "failed_move_no_us_reversal_short__6E",
    "failed_move_no_us_reversal_short__6J",
    "failed_move_no_us_reversal_short__CL",
    "failed_move_no_us_reversal_short__ES",
    "failed_move_no_us_reversal_short__QC",
)


def run_approved_quant_materialization_repair(
    *,
    output_dir: str | Path | None = None,
    source_database_path: str | Path | None = None,
    trailing_bars: int = 50,
) -> dict[str, Path]:
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    source_database = Path(source_database_path or DEFAULT_SOURCE_DATABASE).resolve()
    settings = load_settings_from_files(list(DEFAULT_CONFIG_PATHS))

    current_runtime_payload = json.loads(
        (settings.probationary_artifacts_path / "runtime" / "paper_config_in_force.json").read_text(encoding="utf-8")
    )
    configured_lane_ids = {
        str(row.get("lane_id"))
        for row in _configured_probationary_paper_lane_rows(settings)
        if row.get("lane_id")
    }
    current_runtime_lane_ids = {
        str(row.get("lane_id"))
        for row in current_runtime_payload.get("lanes", [])
        if row.get("lane_id")
    }
    missing_from_current_runtime = sorted(set(TARGET_LANES) - current_runtime_lane_ids)

    registry = build_strategy_runtime_registry(settings, include_approved_quant_runtime_rows=True)
    instances = {
        instance.definition.standalone_strategy_id: instance
        for instance in registry.instances
        if instance.definition.standalone_strategy_id in TARGET_LANES
    }

    before_counts = {lane_id: _table_counts(instances[lane_id]) for lane_id in TARGET_LANES}
    source_latest_timestamps = _latest_source_timestamps(source_database=source_database)

    for lane_id in TARGET_LANES:
        instance = instances[lane_id]
        bars = _load_source_bars(
            source_database=source_database,
            symbol=instance.definition.instrument,
            timeframe="5m",
            trailing_bars=trailing_bars,
            since_end_ts=instance.repositories.processed_bars.latest_end_ts(),
        )
        for bar in bars:
            instance.strategy_engine.process_bar(bar)

    after_counts = {lane_id: _table_counts(instances[lane_id]) for lane_id in TARGET_LANES}
    lane_rows = []
    for lane_id in TARGET_LANES:
        instance = instances[lane_id]
        lane_rows.append(
            {
                "lane_id": lane_id,
                "instrument": instance.definition.instrument,
                "family": instance.definition.strategy_family,
                "root_cause": (
                    "The paper-session runtime artifacts were stale and omitted this approved-quant lane from paper_config_in_force.json, "
                    "so the lane database existed on disk but the lane never received completed bars in the session."
                ),
                "fix": (
                    "Keep approved-quant rows in the active probationary paper lane set, stop trusting the stale 5-lane runtime artifact as authoritative, "
                    "and seed the lane's real SQLite store from persisted 5m bars so it has usable runtime state immediately."
                ),
                "source_latest_bar_end_ts": source_latest_timestamps.get(instance.definition.instrument),
                "before_counts": before_counts[lane_id],
                "after_counts": after_counts[lane_id],
                "materialization_repaired": all(after_counts[lane_id][table] > 0 for table in _tracked_tables()),
            }
        )

    wrong_session_rows = _current_wrong_session_rows(
        operator_status_path=settings.probationary_artifacts_path / "operator_status.json"
    )
    narrowed_wrong_session_audit = {
        "count": len(wrong_session_rows),
        "rows": wrong_session_rows,
        "classification_summary": dict(Counter(row["classification"] for row in wrong_session_rows)),
    }

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "source_database": str(source_database),
        "repair_window": {
            "timeframe": "5m",
            "trailing_bars_per_lane": trailing_bars,
        },
        "shared_root_cause": {
            "configured_lane_count_now": len(configured_lane_ids),
            "paper_config_in_force_lane_count_before": len(current_runtime_lane_ids),
            "missing_target_lanes_in_current_runtime_artifact": missing_from_current_runtime,
            "note": (
                "The stale paper-session runtime artifact only carried the original 5 classic lanes. "
                "These 9 approved-quant lanes were discoverable in audit/registry surfaces, but they were not active paper-session participants."
            ),
        },
        "lane_repairs": lane_rows,
        "narrowed_wrong_session_audit_after_materialization_fix": narrowed_wrong_session_audit,
    }

    json_path = resolved_output_dir / "approved_quant_materialization_repair.json"
    markdown_path = resolved_output_dir / "approved_quant_materialization_repair.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(report).strip() + "\n", encoding="utf-8")
    return {"json_path": json_path, "markdown_path": markdown_path}


def _load_source_bars(
    *,
    source_database: Path,
    symbol: str,
    timeframe: str,
    trailing_bars: int,
    since_end_ts: datetime | None,
) -> list[Bar]:
    connection = sqlite3.connect(source_database)
    connection.row_factory = sqlite3.Row
    try:
        if since_end_ts is None:
            rows = connection.execute(
                """
                select
                  bar_id,
                  symbol,
                  timeframe,
                  start_ts,
                  end_ts,
                  open,
                  high,
                  low,
                  close,
                  volume
                from bars
                where symbol = ? and timeframe = ?
                order by end_ts desc
                limit ?
                """,
                (symbol, timeframe, trailing_bars),
            ).fetchall()
        else:
            rows = connection.execute(
                """
                select
                  bar_id,
                  symbol,
                  timeframe,
                  start_ts,
                  end_ts,
                  open,
                  high,
                  low,
                  close,
                  volume
                from bars
                where symbol = ? and timeframe = ? and end_ts > ?
                order by end_ts asc
                """,
                (symbol, timeframe, since_end_ts.isoformat()),
            ).fetchall()
    finally:
        connection.close()
    bars: list[Bar] = []
    ordered_rows = rows if since_end_ts is not None else list(reversed(rows))
    for row in ordered_rows:
        end_ts = datetime.fromisoformat(row["end_ts"])
        session_label = label_session_phase(end_ts)
        bars.append(
            Bar(
                bar_id=row["bar_id"],
                symbol=row["symbol"],
                timeframe=row["timeframe"],
                start_ts=datetime.fromisoformat(row["start_ts"]),
                end_ts=end_ts,
                open=Decimal(str(row["open"])),
                high=Decimal(str(row["high"])),
                low=Decimal(str(row["low"])),
                close=Decimal(str(row["close"])),
                volume=int(row["volume"]),
                is_final=True,
                session_asia=session_label.startswith("ASIA"),
                session_london=session_label.startswith("LONDON"),
                session_us=session_label.startswith("US"),
                session_allowed=True,
            )
        )
    return bars


def _latest_source_timestamps(*, source_database: Path) -> dict[str, str]:
    connection = sqlite3.connect(source_database)
    try:
        rows = connection.execute(
            f"""
            select symbol, max(end_ts)
            from bars
            where symbol in ({','.join('?' for _ in TARGET_LANES)})
            group by symbol
            """,
            tuple(lane.rsplit("__", 1)[1] for lane in TARGET_LANES),
        ).fetchall()
    finally:
        connection.close()
    return {str(symbol): str(end_ts) for symbol, end_ts in rows}


def _table_counts(instance) -> dict[str, int]:
    with instance.repositories.engine.begin() as connection:
        return {
            table: connection.execute(text(f"select count(*) from {table}")).scalar_one()
            for table in _tracked_tables()
        }


def _tracked_tables() -> tuple[str, ...]:
    return ("bars", "processed_bars", "features", "signals", "strategy_state_snapshots")


def _current_wrong_session_rows(*, operator_status_path: Path) -> list[dict[str, Any]]:
    payload = json.loads(operator_status_path.read_text(encoding="utf-8"))
    rows: list[dict[str, Any]] = []
    for row in payload.get("lanes", []):
        if row.get("eligibility_reason") != "wrong_session":
            continue
        family = "UNKNOWN"
        if row.get("approved_long_entry_sources"):
            family = str(row["approved_long_entry_sources"][0])
        elif row.get("approved_short_entry_sources"):
            family = str(row["approved_short_entry_sources"][0])
        classification = "intentional_and_correct"
        if family == "asiaEarlyNormalBreakoutRetestHoldTurn" and row.get("current_detected_session") == "LONDON_OPEN":
            classification = "too_strict"
        rows.append(
            {
                "lane_id": row.get("lane_id"),
                "symbol": row.get("symbol"),
                "family": family,
                "session_restriction": row.get("session_restriction"),
                "current_detected_session": row.get("current_detected_session"),
                "classification": classification,
            }
        )
    return rows


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Approved Quant Materialization Repair",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Source database: `{report['source_database']}`",
        f"- Configured lane count now: `{report['shared_root_cause']['configured_lane_count_now']}`",
        f"- Stale paper_config_in_force lane count: `{report['shared_root_cause']['paper_config_in_force_lane_count_before']}`",
        "",
        "## Shared Root Cause",
        "",
        f"- {report['shared_root_cause']['note']}",
        "",
        "## Lane Repairs",
        "",
    ]
    for row in report["lane_repairs"]:
        lines.append(
            f"- `{row['lane_id']}`: before={row['before_counts']} after={row['after_counts']} repaired={row['materialization_repaired']}"
        )
    lines.extend(["", "## Narrowed Wrong Session Audit", ""])
    for row in report["narrowed_wrong_session_audit_after_materialization_fix"]["rows"]:
        lines.append(
            f"- `{row['lane_id']}` family=`{row['family']}` restriction=`{row['session_restriction']}` current=`{row['current_detected_session']}` classification=`{row['classification']}`"
        )
    return "\n".join(lines)
