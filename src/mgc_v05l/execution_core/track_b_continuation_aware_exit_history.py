"""Diagnostic history rollup for Track B continuation-aware exit previews.

This module is read-only with respect to runtime, broker, order planner, and
lifecycle state. It only summarizes dry-run preview events and writes a
diagnostic artifact.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_continuation_aware_exit_decision import (
    DEFAULT_CONTINUATION_AWARE_EXIT_PREVIEW_EVENT_LOG,
    DEFAULT_CONTINUATION_AWARE_EXIT_PREVIEW_PATH,
    EXIT_HARD_MAX_DURATION,
    EXIT_LIFECYCLE_UNSAFE,
    EXIT_SAFE_STATE_OVERRIDE,
    INSUFFICIENT_DATA_HOLD_OR_FALLBACK,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONTINUATION_AWARE_EXIT_HISTORY_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "continuation_aware_exit"
    / "latest_continuation_aware_exit_history.json"
)

CONTINUATION_EXIT_HISTORY_READY = "CONTINUATION_EXIT_HISTORY_READY"
CONTINUATION_EXIT_HISTORY_EMPTY = "CONTINUATION_EXIT_HISTORY_EMPTY"
CONTINUATION_EXIT_HISTORY_PARTIAL = "CONTINUATION_EXIT_HISTORY_PARTIAL"
CONTINUATION_EXIT_HISTORY_MALFORMED = "CONTINUATION_EXIT_HISTORY_MALFORMED"


@dataclass(frozen=True)
class TrackBContinuationAwareExitHistoryConfig:
    repo_root: Path = REPO_ROOT
    event_log_path: Path = DEFAULT_CONTINUATION_AWARE_EXIT_PREVIEW_EVENT_LOG
    latest_preview_path: Path = DEFAULT_CONTINUATION_AWARE_EXIT_PREVIEW_PATH
    output_path: Path = DEFAULT_CONTINUATION_AWARE_EXIT_HISTORY_PATH
    lookback_event_count: int = 100

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_continuation_aware_exit_history(
    *,
    config: TrackBContinuationAwareExitHistoryConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    events, malformed_rows = _read_preview_events(
        event_log_path=config.resolve(config.event_log_path),
        latest_preview_path=config.resolve(config.latest_preview_path),
    )
    lookback_count = max(0, int(config.lookback_event_count))
    bounded_events = events[-lookback_count:] if lookback_count else []
    by_strategy = _strategy_rollups(bounded_events)
    latest_row = _latest_event(bounded_events)
    classification = _classification(parsed_count=len(events), malformed_count=len(malformed_rows))
    return {
        "schema_version": "track_b_continuation_aware_exit_history_v1",
        "generated_at": actual_now.isoformat(),
        "history_classification": classification,
        "strategy_count": len(by_strategy),
        "total_events": len(events),
        "lookback_event_count": len(bounded_events),
        "malformed_row_count": len(malformed_rows),
        "malformed_rows_sample": malformed_rows[:5],
        "latest_strategy_id": latest_row.get("strategy_id") if latest_row else "",
        "latest_symbol": latest_row.get("symbol") if latest_row else "",
        "latest_exit_profile_id": latest_row.get("exit_profile_id") if latest_row else "",
        "latest_exit_state": latest_row.get("exit_state") if latest_row else "",
        "latest_continuation_quality_state": latest_row.get("continuation_quality_state") if latest_row else "",
        "latest_generated_at": latest_row.get("generated_at") if latest_row else None,
        "by_strategy": by_strategy,
        "dry_run_only": True,
        "diagnostic_only": True,
        "not_order_authority": True,
        "not_lifecycle_authority": True,
        "not_routing_authority": True,
        "dashboard_projection_consumed": False,
        "paper_only": True,
        "live_money_eligible": False,
        "broker_mutation_allowed": False,
        "lifecycle_mutation_allowed": False,
        "runtime_restart_allowed": False,
        "order_planner_invoked": False,
    }


def write_continuation_aware_exit_history(
    *,
    config: TrackBContinuationAwareExitHistoryConfig,
    payload: Mapping[str, Any],
) -> Path:
    return write_json_atomic(config.resolve(config.output_path), dict(payload))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Write a diagnostic Track B continuation-aware exit preview history rollup."
    )
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--event-log-path", type=Path, default=DEFAULT_CONTINUATION_AWARE_EXIT_PREVIEW_EVENT_LOG)
    parser.add_argument("--latest-preview-path", type=Path, default=DEFAULT_CONTINUATION_AWARE_EXIT_PREVIEW_PATH)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_CONTINUATION_AWARE_EXIT_HISTORY_PATH)
    parser.add_argument("--lookback-event-count", type=int, default=100)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBContinuationAwareExitHistoryConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        event_log_path=Path(args.event_log_path),
        latest_preview_path=Path(args.latest_preview_path),
        output_path=Path(args.output_path),
        lookback_event_count=int(args.lookback_event_count),
    )
    payload = build_continuation_aware_exit_history(config=config)
    authority_path = write_continuation_aware_exit_history(config=config, payload=payload)
    summary = {
        "history_classification": payload.get("history_classification"),
        "strategy_count": payload.get("strategy_count"),
        "total_events": payload.get("total_events"),
        "lookback_event_count": payload.get("lookback_event_count"),
        "latest_strategy_id": payload.get("latest_strategy_id"),
        "latest_exit_profile_id": payload.get("latest_exit_profile_id"),
        "latest_exit_state": payload.get("latest_exit_state"),
        "dry_run_only": True,
        "not_order_authority": True,
        "not_lifecycle_authority": True,
        "authority_path": str(authority_path),
    }
    print(json.dumps(payload if bool(args.json) else summary, indent=2, sort_keys=True))
    return 0


def _read_preview_events(*, event_log_path: Path, latest_preview_path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    events: list[dict[str, Any]] = []
    malformed_rows: list[dict[str, Any]] = []
    if event_log_path.exists():
        for line_number, raw_line in enumerate(event_log_path.read_text(encoding="utf-8").splitlines(), start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError as exc:
                malformed_rows.append(
                    {
                        "line_number": line_number,
                        "error": str(exc),
                        "sample": line[:160],
                    }
                )
                continue
            if isinstance(parsed, dict):
                events.append(parsed)
            else:
                malformed_rows.append(
                    {
                        "line_number": line_number,
                        "error": "JSONL row is not an object",
                        "sample": line[:160],
                    }
                )
    if not events and latest_preview_path.exists():
        try:
            parsed_latest = json.loads(latest_preview_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            malformed_rows.append(
                {
                    "line_number": None,
                    "error": f"latest preview JSON malformed: {exc}",
                    "sample": str(latest_preview_path),
                }
            )
        else:
            if isinstance(parsed_latest, dict) and parsed_latest:
                events.append(parsed_latest)
    return events, malformed_rows


def _strategy_rollups(events: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for event in events:
        strategy_id = str(event.get("strategy_id") or "UNKNOWN_STRATEGY")
        grouped.setdefault(strategy_id, []).append(event)

    rows: list[dict[str, Any]] = []
    for strategy_id, strategy_events in grouped.items():
        latest = _latest_event(strategy_events) or {}
        rows.append(
            {
                "strategy_id": strategy_id,
                "symbol": str(latest.get("symbol") or ""),
                "latest_exit_profile_id": str(latest.get("exit_profile_id") or ""),
                "latest_exit_state": str(latest.get("exit_state") or ""),
                "latest_continuation_quality_state": str(latest.get("continuation_quality_state") or ""),
                "latest_generated_at": latest.get("generated_at"),
                "hold_count": sum(1 for event in strategy_events if _is_hold_state(event)),
                "exit_preview_count": sum(1 for event in strategy_events if _is_exit_preview(event)),
                "insufficient_data_count": sum(
                    1 for event in strategy_events if event.get("exit_state") == INSUFFICIENT_DATA_HOLD_OR_FALLBACK
                ),
                "hard_override_count": sum(1 for event in strategy_events if _is_hard_override(event)),
                "recent_states": [_compact_state(event) for event in strategy_events[-5:]],
                "latest_operator_summary": _operator_summary(latest),
            }
        )
    return sorted(rows, key=lambda row: str(row.get("latest_generated_at") or ""), reverse=True)


def _compact_state(event: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "generated_at": event.get("generated_at"),
        "exit_profile_id": event.get("exit_profile_id") or "",
        "exit_state": event.get("exit_state") or "",
        "continuation_quality_state": event.get("continuation_quality_state") or "",
        "should_request_close": event.get("should_request_close") is True,
        "dry_run_only": event.get("dry_run_only") is not False,
        "not_order_authority": event.get("not_order_authority") is not False,
        "not_lifecycle_authority": event.get("not_lifecycle_authority") is not False,
    }


def _operator_summary(event: Mapping[str, Any]) -> str:
    if not event:
        return "No continuation-aware exit preview events have been recorded."
    strategy_id = str(event.get("strategy_id") or "UNKNOWN_STRATEGY")
    symbol = str(event.get("symbol") or "")
    state = str(event.get("exit_state") or "UNKNOWN_EXIT_STATE")
    profile = str(event.get("exit_profile_id") or "UNKNOWN_PROFILE")
    close_flag = "close preview present" if event.get("should_request_close") is True else "no close requested"
    return f"{strategy_id} {symbol} latest preview is {state} via {profile}; {close_flag}; diagnostic dry-run only."


def _latest_event(events: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    if not events:
        return None
    return events[-1]


def _is_hold_state(event: Mapping[str, Any]) -> bool:
    return str(event.get("exit_state") or "").startswith("HOLD_")


def _is_exit_preview(event: Mapping[str, Any]) -> bool:
    state = str(event.get("exit_state") or "")
    return state.startswith("EXIT_") or event.get("should_request_close") is True


def _is_hard_override(event: Mapping[str, Any]) -> bool:
    return event.get("exit_state") in {
        EXIT_HARD_MAX_DURATION,
        EXIT_SAFE_STATE_OVERRIDE,
        EXIT_LIFECYCLE_UNSAFE,
    }


def _classification(*, parsed_count: int, malformed_count: int) -> str:
    if parsed_count and malformed_count:
        return CONTINUATION_EXIT_HISTORY_PARTIAL
    if parsed_count:
        return CONTINUATION_EXIT_HISTORY_READY
    if malformed_count:
        return CONTINUATION_EXIT_HISTORY_MALFORMED
    return CONTINUATION_EXIT_HISTORY_EMPTY


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
