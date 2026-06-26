"""Post-trade analytics refresh hook for Track B PAPER evidence.

This module refreshes analytics artifacts from already-persisted PAPER evidence.
It is artifact-only: it never connects to a broker, starts/stops runtime
processes, submits, cancels, modifies, closes, or gates trading authority.
Failures are diagnostic-only and must not block runtime or Managed Exit.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.track_b_managed_close_terminal_publisher import (
    ManagedCloseTerminalPublisherConfig,
    publish_managed_close_terminal_events,
)
from mgc_v05l.execution_core.track_b_managed_exit_fill_registry_backfill import publish_managed_exit_fill_backfill
from mgc_v05l.execution_core.track_b_side_session_attribution_replay import (
    DEFAULT_OUTPUT_DIR as DEFAULT_SIDE_SESSION_OUTPUT_DIR,
)
from mgc_v05l.execution_core.track_b_side_session_attribution_replay import (
    build_side_session_attribution_replay,
)
from mgc_v05l.execution_core.track_b_strategy_performance_attachment import (
    DEFAULT_CONFIG_IN_FORCE,
    DEFAULT_FILLED_BRIDGE_RESULTS,
    DEFAULT_FUNNEL_EVENTS,
    DEFAULT_OUTPUT_DIR as DEFAULT_STRATEGY_PERFORMANCE_OUTPUT_DIR,
    DEFAULT_PHASE1_ROOT,
    DEFAULT_ROSTER,
    DEFAULT_TRADE_REGISTRY_EVENTS,
)
from mgc_v05l.execution_core.track_b_strategy_performance_attachment import (
    build_strategy_performance_attachment,
)
from mgc_v05l.execution_core.track_b_trend_continuation_overlay import (
    DEFAULT_OUTPUT_DIR as DEFAULT_TREND_OUTPUT_DIR,
)
from mgc_v05l.execution_core.track_b_trend_continuation_overlay import (
    build_trend_continuation_overlay,
)


SCHEMA_VERSION = "track_b_post_trade_analytics_refresh_v1"
DEFAULT_OUTPUT_DIR = Path("outputs") / "track_b_execution_core" / "strategy_performance"
DEFAULT_SUMMARY_PATH = DEFAULT_OUTPUT_DIR / "latest_post_trade_analytics_refresh_summary.json"
DEFAULT_STAGE_TIMEOUT_SECONDS = 90.0


@dataclass(frozen=True)
class PostTradeAnalyticsRefreshResult:
    summary_path: Path
    summary: dict[str, Any]


def run_post_trade_analytics_refresh(
    *,
    repo_root: Path | str = Path("."),
    config_path: Path | str = DEFAULT_CONFIG_IN_FORCE,
    roster_path: Path | str = DEFAULT_ROSTER,
    filled_bridge_results_path: Path | str = DEFAULT_FILLED_BRIDGE_RESULTS,
    trade_registry_events_path: Path | str = DEFAULT_TRADE_REGISTRY_EVENTS,
    funnel_events_path: Path | str = DEFAULT_FUNNEL_EVENTS,
    phase1_root: Path | str = DEFAULT_PHASE1_ROOT,
    strategy_performance_output_dir: Path | str = DEFAULT_STRATEGY_PERFORMANCE_OUTPUT_DIR,
    side_session_output_dir: Path | str = DEFAULT_SIDE_SESSION_OUTPUT_DIR,
    trend_output_dir: Path | str = DEFAULT_TREND_OUTPUT_DIR,
    summary_path: Path | str = DEFAULT_SUMMARY_PATH,
    include_terminal_publisher: bool = True,
    include_managed_exit_backfill: bool = True,
    max_stage_seconds: float = DEFAULT_STAGE_TIMEOUT_SECONDS,
    write_artifacts: bool = True,
    now: datetime | None = None,
    stage_overrides: Mapping[str, Callable[[], Any]] | None = None,
) -> PostTradeAnalyticsRefreshResult:
    """Refresh post-trade analytics artifacts from local evidence only."""

    root = Path(repo_root)
    generated_at = _ensure_utc(now or datetime.now(UTC))
    strategy_output = Path(strategy_performance_output_dir)
    side_output = Path(side_session_output_dir)
    trend_output = Path(trend_output_dir)
    summary_ref = _resolve(root, Path(summary_path))
    canonical_trades_path = _resolve(root, strategy_output / "canonical_trade_records.jsonl")
    side_replay_path = _resolve(root, side_output / "side_session_trade_replay.jsonl")
    forward_capture_path = _resolve(root, side_output / "forward_path_capture.jsonl")

    overrides = dict(stage_overrides or {})
    stage_records: list[dict[str, Any]] = []
    stage_payloads: dict[str, Any] = {}

    def _terminal_stage() -> Any:
        return publish_managed_close_terminal_events(
            config=ManagedCloseTerminalPublisherConfig(repo_root=root),
            now=generated_at,
        )

    def _backfill_stage() -> Any:
        return publish_managed_exit_fill_backfill(
            repo_root=root,
            trade_registry_events_path=trade_registry_events_path,
            dry_run=False,
            write_summary=write_artifacts,
            now=generated_at,
        )

    def _strategy_stage() -> Any:
        return build_strategy_performance_attachment(
            repo_root=root,
            config_path=config_path,
            roster_path=roster_path,
            filled_bridge_results_path=filled_bridge_results_path,
            trade_registry_events_path=trade_registry_events_path,
            funnel_events_path=funnel_events_path,
            phase1_root=phase1_root,
            output_dir=strategy_output,
            write_artifacts=write_artifacts,
        )

    def _side_session_stage() -> Any:
        return build_side_session_attribution_replay(
            repo_root=root,
            canonical_trades_path=canonical_trades_path,
            phase1_root=phase1_root,
            output_dir=side_output,
            write_artifacts=write_artifacts,
            now=generated_at,
        )

    def _trend_stage() -> Any:
        return build_trend_continuation_overlay(
            repo_root=root,
            canonical_trades_path=canonical_trades_path,
            side_session_replay_path=side_replay_path,
            forward_capture_path=forward_capture_path,
            phase1_root=phase1_root,
            output_dir=trend_output,
            write_artifacts=write_artifacts,
            now=generated_at,
        )

    stage_plan: list[tuple[str, Callable[[], Any]]] = []
    if include_terminal_publisher:
        stage_plan.append(("managed_close_terminal_publisher", overrides.get("managed_close_terminal_publisher", _terminal_stage)))
    if include_managed_exit_backfill:
        stage_plan.append(("managed_exit_fill_backfill", overrides.get("managed_exit_fill_backfill", _backfill_stage)))
    stage_plan.extend(
        [
            ("strategy_performance_attachment", overrides.get("strategy_performance_attachment", _strategy_stage)),
            ("side_session_attribution_replay", overrides.get("side_session_attribution_replay", _side_session_stage)),
            ("trend_continuation_overlay", overrides.get("trend_continuation_overlay", _trend_stage)),
        ]
    )

    for stage_name, stage_fn in stage_plan:
        record, payload = _run_stage(stage_name, stage_fn, max_stage_seconds=max_stage_seconds)
        stage_records.append(record)
        if record["status"] == "OK":
            stage_payloads[stage_name] = payload

    summary = _build_summary(
        generated_at=generated_at,
        stage_records=stage_records,
        stage_payloads=stage_payloads,
        root=root,
        trade_registry_events_path=Path(trade_registry_events_path),
        canonical_trades_path=canonical_trades_path,
        side_replay_path=side_replay_path,
        forward_capture_path=forward_capture_path,
        trend_summary_path=_resolve(root, trend_output / "latest_trend_continuation_overlay_summary.json"),
    )
    if write_artifacts:
        summary_ref.parent.mkdir(parents=True, exist_ok=True)
        summary_ref.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return PostTradeAnalyticsRefreshResult(summary_path=summary_ref, summary=summary)


def _run_stage(
    name: str,
    fn: Callable[[], Any],
    *,
    max_stage_seconds: float,
) -> tuple[dict[str, Any], Any | None]:
    started = time.monotonic()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(fn)
    try:
        payload = future.result(timeout=max_stage_seconds)
    except concurrent.futures.TimeoutError:
        executor.shutdown(wait=False, cancel_futures=True)
        return (
            {
                "stage": name,
                "status": "TIMEOUT_DIAGNOSTIC_ONLY",
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "max_stage_seconds": max_stage_seconds,
                "trading_blocking": False,
            },
            None,
        )
    except Exception as exc:  # noqa: BLE001 - diagnostics should carry the stage failure.
        executor.shutdown(wait=False, cancel_futures=True)
        return (
            {
                "stage": name,
                "status": "FAILED_DIAGNOSTIC_ONLY",
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "error_type": type(exc).__name__,
                "error": str(exc),
                "trading_blocking": False,
            },
            None,
        )
    executor.shutdown(wait=True)
    elapsed = time.monotonic() - started
    status = "OK" if elapsed <= max_stage_seconds else "TIMEOUT_DIAGNOSTIC_ONLY"
    return (
        {
            "stage": name,
            "status": status,
            "elapsed_seconds": round(elapsed, 3),
            "max_stage_seconds": max_stage_seconds,
            "trading_blocking": False,
        },
        payload if status == "OK" else None,
    )


def _build_summary(
    *,
    generated_at: datetime,
    stage_records: list[dict[str, Any]],
    stage_payloads: Mapping[str, Any],
    root: Path,
    trade_registry_events_path: Path,
    canonical_trades_path: Path,
    side_replay_path: Path,
    forward_capture_path: Path,
    trend_summary_path: Path,
) -> dict[str, Any]:
    terminal = _mapping(stage_payloads.get("managed_close_terminal_publisher"))
    backfill = stage_payloads.get("managed_exit_fill_backfill")
    strategy = stage_payloads.get("strategy_performance_attachment")
    side = stage_payloads.get("side_session_attribution_replay")
    trend = stage_payloads.get("trend_continuation_overlay")
    canonical_rows = _read_jsonl(canonical_trades_path)
    side_rows = _read_jsonl(side_replay_path)
    forward_rows = _read_jsonl(forward_capture_path)
    trend_summary = _read_json(trend_summary_path)
    stage_failed = any(record["status"] != "OK" for record in stage_records)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "classification": "POST_TRADE_ANALYTICS_REFRESH_DEGRADED_DIAGNOSTIC_ONLY"
        if stage_failed
        else "POST_TRADE_ANALYTICS_REFRESH_READY",
        "analytics_only": True,
        "artifact_writes_only": True,
        "broker_mutation_allowed": False,
        "runtime_authority": False,
        "managed_exit_authority": False,
        "strategy_behavior_changed": False,
        "trading_blocking": False,
        "source_artifacts": {
            "trade_registry_events": str(_resolve(root, trade_registry_events_path)),
            "canonical_trade_records": str(canonical_trades_path),
            "side_session_replay": str(side_replay_path),
            "forward_path_capture": str(forward_capture_path),
            "trend_summary": str(trend_summary_path),
        },
        "source_freshness": {
            "trade_registry_latest_mtime": _mtime_iso(_resolve(root, trade_registry_events_path)),
            "canonical_trades_latest_mtime": _mtime_iso(canonical_trades_path),
            "side_replay_latest_mtime": _mtime_iso(side_replay_path),
            "forward_capture_latest_mtime": _mtime_iso(forward_capture_path),
            "trend_summary_latest_mtime": _mtime_iso(trend_summary_path),
        },
        "stages": stage_records,
        "managed_close_terminal_publisher": _terminal_summary(terminal),
        "managed_exit_fill_backfill": _backfill_summary(backfill),
        "strategy_performance": _strategy_summary(strategy, canonical_rows),
        "side_session_attribution": _side_summary(side, side_rows),
        "forward_path_capture": _forward_capture_summary(forward_rows),
        "trend_continuation_overlay": _trend_summary(trend, trend_summary),
    }


def _terminal_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "terminal_events_emitted": _int(payload.get("terminal_events_emitted") or payload.get("appended_count")),
        "diagnostic_only_count": _int(payload.get("diagnostic_only_count") or payload.get("diagnostic_count")),
        "duplicate_count": _int(payload.get("duplicate_count")),
    }


def _backfill_summary(payload: Any) -> dict[str, Any]:
    if hasattr(payload, "to_dict"):
        data = payload.to_dict()
    else:
        data = _mapping(payload)
    return {
        "candidate_close_fills": _int(data.get("candidate_close_fills")),
        "appended_count": _int(data.get("appended_count")),
        "duplicate_count": _int(data.get("duplicate_count")),
        "skipped_count": _int(data.get("skipped_count")),
    }


def _strategy_summary(payload: Any, canonical_rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    summary = _mapping(getattr(payload, "summary", {}))
    pairing = _mapping(summary.get("pairing"))
    return {
        "events_written": _int(getattr(payload, "events_written", 0)),
        "canonical_trade_count": len(canonical_rows),
        "paired_trades": _int(pairing.get("paired_trades") or summary.get("paired_trades")),
        "unpaired_entry_count": _int(pairing.get("unpaired_entry_count")),
        "unpaired_exit_count": _int(pairing.get("unpaired_exit_count")),
    }


def _side_summary(payload: Any, side_rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    summary = _mapping(getattr(payload, "summary", {}))
    return {
        "total_trades": _int(summary.get("total_trades") or len(side_rows)),
        "path_available_count": _int(summary.get("path_available_count")),
        "path_missing_count": _int(summary.get("path_missing_count")),
        "replay_rows": len(side_rows),
    }


def _forward_capture_summary(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    symbols = sorted({str(row.get("symbol") or "") for row in rows if row.get("symbol")})
    timeframes = sorted({str(row.get("timeframe") or "") for row in rows if row.get("timeframe")})
    return {
        "rows": len(rows),
        "symbols": symbols,
        "timeframes": timeframes,
        "latest_generated_at": max((str(row.get("generated_at") or "") for row in rows), default=None),
        "latest_bar_end": max((str(row.get("latest_bar_end") or "") for row in rows), default=None),
    }


def _trend_summary(payload: Any, summary_payload: Any) -> dict[str, Any]:
    summary = _mapping(getattr(payload, "summary", {})) or _mapping(summary_payload)
    return {
        "total_trades": _int(summary.get("total_trades")),
        "path_available_count": _int(summary.get("path_available_count")),
        "path_missing_count": _int(summary.get("path_missing_count")),
        "state_counts": dict(_mapping(summary.get("state_counts"))),
    }


def _read_jsonl(path: Path) -> list[Mapping[str, Any]]:
    if not path.exists():
        return []
    rows: list[Mapping[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, Mapping):
            rows.append(row)
    return rows


def _read_json(path: Path) -> Any:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _resolve(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else root / path


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _mtime_iso(path: Path) -> str | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat()
    except OSError:
        return None


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--config-path", default=str(DEFAULT_CONFIG_IN_FORCE))
    parser.add_argument("--roster-path", default=str(DEFAULT_ROSTER))
    parser.add_argument("--filled-bridge-results-path", default=str(DEFAULT_FILLED_BRIDGE_RESULTS))
    parser.add_argument("--trade-registry-events-path", default=str(DEFAULT_TRADE_REGISTRY_EVENTS))
    parser.add_argument("--funnel-events-path", default=str(DEFAULT_FUNNEL_EVENTS))
    parser.add_argument("--phase1-root", default=str(DEFAULT_PHASE1_ROOT))
    parser.add_argument("--summary-path", default=str(DEFAULT_SUMMARY_PATH))
    parser.add_argument("--max-stage-seconds", type=float, default=DEFAULT_STAGE_TIMEOUT_SECONDS)
    parser.add_argument("--skip-terminal-publisher", action="store_true")
    parser.add_argument("--skip-managed-exit-backfill", action="store_true")
    args = parser.parse_args()
    result = run_post_trade_analytics_refresh(
        repo_root=Path(args.repo_root),
        config_path=Path(args.config_path),
        roster_path=Path(args.roster_path),
        filled_bridge_results_path=Path(args.filled_bridge_results_path),
        trade_registry_events_path=Path(args.trade_registry_events_path),
        funnel_events_path=Path(args.funnel_events_path),
        phase1_root=Path(args.phase1_root),
        summary_path=Path(args.summary_path),
        include_terminal_publisher=not args.skip_terminal_publisher,
        include_managed_exit_backfill=not args.skip_managed_exit_backfill,
        max_stage_seconds=args.max_stage_seconds,
    )
    print(json.dumps(result.summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main())
