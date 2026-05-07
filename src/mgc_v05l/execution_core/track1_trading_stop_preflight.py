"""Read-only Track 1 trading-stop preflight.

This diagnostic is intentionally cheap and bounded. It inventories available
legacy/Track 1-ish paper artifacts before any expensive Track 1 vs Track B
parity work is attempted. It never invokes broker, proof, submit, or lifecycle
paths.
"""

from __future__ import annotations

import json
import os
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from .models import require_aware_datetime, to_jsonable


DEFAULT_OUTPUT_ROOT = Path("outputs/track_b_execution_core/diagnostics")
DEFAULT_JSON = DEFAULT_OUTPUT_ROOT / "latest_track1_trading_stop_preflight.json"
DEFAULT_MD = DEFAULT_OUTPUT_ROOT / "latest_track1_trading_stop_preflight.md"
MAX_CANDIDATE_FILES = 500
MAX_FILE_BYTES = 4 * 1024 * 1024
MAX_JSONL_LINES = 5000


@dataclass(frozen=True)
class Track1TradingStopPreflightResult:
    report_json: Path
    report_md: Path
    report: dict[str, Any]
    markdown: str


def build_track1_trading_stop_preflight(
    *,
    repo_root: Path = Path("."),
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    now: datetime | None = None,
    max_files: int = MAX_CANDIDATE_FILES,
    write: bool = True,
) -> Track1TradingStopPreflightResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    root = Path(repo_root)
    candidates = _candidate_artifact_paths(root=root, max_files=max_files)
    inventories = [_summarize_artifact(path, root=root) for path in candidates]
    inventories = [item for item in inventories if item["artifact_kind"] != "IGNORED"]
    track1_like = [item for item in inventories if item["track_family"] in {"TRACK1", "TRACK_A", "LEGACY_PAPER"}]
    trade_evidence = [item for item in track1_like if int(item.get("direct_trade_count") or 0) > 0]
    signal_evidence = [item for item in track1_like if int(item.get("signal_count") or 0) > 0]
    handoff_evidence = [item for item in track1_like if int(item.get("handoff_count") or 0) > 0]
    broker_or_lifecycle_evidence = [
        item
        for item in track1_like
        if item.get("broker_or_lifecycle_issue_count", 0) or item["artifact_kind"] in {"PAPER_PROOF", "RECOVERY_OR_READINESS"}
    ]
    config_disabled_evidence = [item for item in track1_like if item.get("config_disabled_count", 0)]
    last_trade = _latest_event(trade_evidence, "last_trade_at")
    last_signal = _latest_event(signal_evidence, "last_signal_at")
    first_no_trade_after_activity = _first_no_activity_after_last_trade(track1_like, last_trade)
    classification, likely_break = _classify(
        track1_like=track1_like,
        trade_evidence=trade_evidence,
        signal_evidence=signal_evidence,
        handoff_evidence=handoff_evidence,
        broker_or_lifecycle_evidence=broker_or_lifecycle_evidence,
        config_disabled_evidence=config_disabled_evidence,
        last_trade=last_trade,
        last_signal=last_signal,
    )
    missing = _missing_evidence(track1_like, trade_evidence, signal_evidence)
    report_json = Path(output_root) / DEFAULT_JSON.name
    report_md = Path(output_root) / DEFAULT_MD.name
    report = {
        "schema_version": "track1_trading_stop_preflight_v1",
        "generated_at": actual_now.isoformat(),
        "bounded_policy": {
            "max_candidate_files": max_files,
            "max_file_bytes": MAX_FILE_BYTES,
            "max_jsonl_lines_per_file": MAX_JSONL_LINES,
            "broker_commands_invoked": False,
            "paper_proof_cli_invoked": False,
            "submit_cancel_place_order_invoked": False,
            "broker_state_mutated": False,
        },
        "artifact_inventory": inventories,
        "track1_like_artifact_count": len(track1_like),
        "trade_artifact_count": len(trade_evidence),
        "signal_artifact_count": len(signal_evidence),
        "handoff_artifact_count": len(handoff_evidence),
        "classification": classification,
        "breakpoint": {
            "last_known_track1_trade": last_trade,
            "last_known_strategy_signal": last_signal,
            "first_no_trade_after_prior_activity": first_no_trade_after_activity,
            "likely_break": likely_break,
        },
        "evidence_summary": {
            "date_range": _date_range(track1_like),
            "strategy_ids": sorted({strategy for item in track1_like for strategy in item.get("strategy_ids", [])}),
            "trade_count": sum(int(item.get("trade_count") or 0) for item in track1_like),
            "direct_trade_count": sum(int(item.get("direct_trade_count") or 0) for item in track1_like),
            "signal_count": sum(int(item.get("signal_count") or 0) for item in track1_like),
            "handoff_count": sum(int(item.get("handoff_count") or 0) for item in track1_like),
            "broker_or_lifecycle_issue_count": sum(int(item.get("broker_or_lifecycle_issue_count") or 0) for item in track1_like),
            "config_disabled_count": sum(int(item.get("config_disabled_count") or 0) for item in track1_like),
            "by_track_family": dict(Counter(str(item.get("track_family")) for item in inventories)),
            "by_artifact_kind": dict(Counter(str(item.get("artifact_kind")) for item in inventories)),
        },
        "missing_evidence": missing,
        "full_parity_audit_justified": classification not in {"TRACK1_REFERENCE_ARTIFACTS_MISSING", "DIAGNOSTIC_INCONCLUSIVE"},
        "recommended_next_action": _recommended_next_action(classification, missing),
    }
    markdown = _markdown(report)
    if write:
        report_json.parent.mkdir(parents=True, exist_ok=True)
        report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
        report_md.write_text(markdown, encoding="utf-8")
    return Track1TradingStopPreflightResult(report_json=report_json, report_md=report_md, report=report, markdown=markdown)


def _candidate_artifact_paths(*, root: Path, max_files: int) -> list[Path]:
    roots = [root / "outputs", root / "var", root / "logs", root / "config"]
    needles = (
        "trade",
        "signal",
        "ledger",
        "paper",
        "proof",
        "runtime",
        "monitor",
        "decision",
        "readiness",
        "recovery",
        "config",
    )
    paths: list[Path] = []
    for base in roots:
        if not base.exists():
            continue
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [name for name in dirnames if name not in {"node_modules", "__pycache__"}]
            for filename in filenames:
                lower = filename.lower()
                if not lower.endswith((".json", ".jsonl", ".md", ".csv")):
                    continue
                rel = str((Path(dirpath) / filename).relative_to(root)).lower()
                if any(needle in rel for needle in needles):
                    paths.append(Path(dirpath) / filename)
            if len(paths) >= max_files:
                break
        if len(paths) >= max_files:
            break
    return sorted(paths[:max_files], key=lambda item: str(item))


def _summarize_artifact(path: Path, *, root: Path) -> dict[str, Any]:
    stat = path.stat()
    rel = str(path.relative_to(root))
    payloads = _bounded_payloads(path)
    text_sample = _text_sample(path) if not payloads else ""
    strategy_ids = sorted({value for payload in payloads for value in _strategy_ids(payload)})
    timestamps = [ts for payload in payloads for ts in _timestamps(payload)]
    kind = _artifact_kind(rel, payloads, text_sample)
    family = _track_family(rel, payloads, text_sample)
    direct_trade_count = sum(_count_direct_trade(payload, rel) for payload in payloads)
    summary_trade_count = sum(_count_summary_trade(payload, rel) for payload in payloads)
    trade_count = direct_trade_count + summary_trade_count
    signal_count = sum(_count_signal(payload, rel) for payload in payloads)
    handoff_count = sum(_count_handoff(payload) for payload in payloads)
    issue_count = sum(_count_issue(payload) for payload in payloads)
    disabled_count = sum(_count_disabled(payload) for payload in payloads)
    if not payloads and text_sample:
        trade_count += _text_count(text_sample, ("trade", "fill", "filled"))
        signal_count += _text_count(text_sample, ("signal",))
        handoff_count += _text_count(text_sample, ("handoff", "submit"))
        issue_count += _text_count(text_sample, ("blocked", "review_required", "failed", "error"))
        disabled_count += _text_count(text_sample, ("disabled", "enabled=false"))
    if kind == "IGNORED":
        return {"path": rel, "artifact_kind": "IGNORED", "track_family": "UNKNOWN"}
    date_range = _range_from_timestamps(timestamps)
    mtime = datetime.fromtimestamp(stat.st_mtime, tz=UTC).isoformat()
    return {
        "path": rel,
        "artifact_kind": kind,
        "track_family": family,
        "date_range": date_range or {"start": mtime, "end": mtime, "source": "file_mtime"},
        "strategy_ids": strategy_ids[:40],
        "trade_count": trade_count,
        "direct_trade_count": direct_trade_count,
        "summary_trade_count": summary_trade_count,
        "signal_count": signal_count,
        "handoff_count": handoff_count,
        "broker_or_lifecycle_issue_count": issue_count,
        "config_disabled_count": disabled_count,
        "last_trade_at": _latest_timestamp_for(payloads, "trade", rel=rel) or (date_range or {}).get("end") if direct_trade_count else None,
        "last_signal_at": _latest_timestamp_for(payloads, "signal") or (date_range or {}).get("end") if signal_count else None,
        "last_handoff_at": _latest_timestamp_for(payloads, "handoff") or (date_range or {}).get("end") if handoff_count else None,
        "file_size_bytes": stat.st_size,
        "bounded_parse": stat.st_size <= MAX_FILE_BYTES,
    }


def _bounded_payloads(path: Path) -> list[Mapping[str, Any]]:
    if path.stat().st_size > MAX_FILE_BYTES:
        return []
    try:
        if path.suffix == ".jsonl":
            rows: list[Mapping[str, Any]] = []
            for line in path.read_text(encoding="utf-8", errors="replace").splitlines()[:MAX_JSONL_LINES]:
                try:
                    parsed = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(parsed, Mapping):
                    rows.append(parsed)
            return rows
        if path.suffix == ".json":
            parsed = json.loads(path.read_text(encoding="utf-8", errors="replace"))
            if isinstance(parsed, Mapping):
                return [parsed]
            if isinstance(parsed, list):
                return [item for item in parsed if isinstance(item, Mapping)][:MAX_JSONL_LINES]
    except (OSError, json.JSONDecodeError):
        return []
    return []


def _text_sample(path: Path) -> str:
    if path.stat().st_size > MAX_FILE_BYTES:
        return ""
    try:
        return path.read_text(encoding="utf-8", errors="replace")[:200_000].lower()
    except OSError:
        return ""


def _artifact_kind(rel: str, payloads: list[Mapping[str, Any]], sample: str) -> str:
    lower = rel.lower()
    if "track_b_execution_core" in lower and "track1" not in lower and "track_a" not in lower:
        return "TRACK_B_ARTIFACT"
    haystack = lower + " " + sample + " " + " ".join(json.dumps(dict(p), default=str)[:3000].lower() for p in payloads[:3])
    if "proof" in haystack:
        return "PAPER_PROOF"
    if "ledger" in haystack or "trade_log" in haystack or "blotter" in haystack or "fill" in haystack:
        return "TRADE_LEDGER"
    if "signal" in haystack or "intent" in haystack:
        return "SIGNAL_LOG"
    if "runtime" in haystack or "monitor" in haystack:
        return "RUNTIME_REPORT"
    if "readiness" in haystack or "recovery" in haystack:
        return "RECOVERY_OR_READINESS"
    if "config" in haystack:
        return "CONFIG_SNAPSHOT"
    return "IGNORED"


def _track_family(rel: str, payloads: list[Mapping[str, Any]], sample: str) -> str:
    lower = rel.lower()
    if "track_b_execution_core" in lower:
        return "TRACK_B"
    haystack = lower + " " + sample + " " + " ".join(json.dumps(dict(p), default=str)[:2000].lower() for p in payloads[:2])
    if "track 1" in haystack or "track1" in haystack or "track_1" in haystack:
        return "TRACK1"
    if "track a" in haystack or "track_a" in haystack:
        return "TRACK_A"
    if "paper" in haystack or "probationary" in haystack or "operator_dashboard" in lower:
        return "LEGACY_PAPER"
    if "replay" in haystack or "warehouse" in lower:
        return "REPLAY"
    return "UNKNOWN"


def _strategy_ids(payload: Mapping[str, Any]) -> set[str]:
    values: set[str] = set()
    for key, value in _walk_items(payload):
        if key in {"strategy_id", "strategy", "source", "lane_id"} and isinstance(value, str) and value:
            values.add(value)
    return values


def _timestamps(payload: Mapping[str, Any]) -> list[str]:
    stamps: list[str] = []
    for key, value in _walk_items(payload):
        if isinstance(value, str) and ("time" in key or key.endswith("_at") or "timestamp" in key):
            if _looks_like_timestamp(value):
                stamps.append(value)
    return stamps


def _walk_items(value: Any, prefix: str = "") -> Iterable[tuple[str, Any]]:
    if isinstance(value, Mapping):
        for key, child in value.items():
            name = str(key)
            yield name, child
            yield from _walk_items(child, name)
    elif isinstance(value, list):
        for child in value[:200]:
            yield from _walk_items(child, prefix)


def _looks_like_timestamp(value: str) -> bool:
    return len(value) >= 10 and value[:4].isdigit() and ("T" in value or value[4] == "-")


def _count_direct_trade(payload: Mapping[str, Any], rel: str) -> int:
    """Count direct trade/fill rows, avoiding broad dashboard counter metadata."""
    lower = rel.lower()
    direct_path = any(token in lower for token in ("trade_log", "blotter", "fill", "fills", "trade_ledger"))
    if not direct_path:
        return 0
    rows = payload.get("rows")
    if isinstance(rows, list):
        return sum(1 for row in rows[:MAX_JSONL_LINES] if isinstance(row, Mapping) and _is_direct_trade_row(row))
    if _is_direct_trade_row(payload):
        return 1
    count = _int_get(payload, ("direct_trade_count", "completed_trade_count"))
    return count


def _count_summary_trade(payload: Mapping[str, Any], rel: str) -> int:
    """Count weaker summary activity separately from direct trade evidence."""
    lower = rel.lower()
    if any(token in lower for token in ("tracked_strategies", "performance", "summary", "snapshot")):
        return _int_get(payload, ("trade_count", "trades", "filled_lanes_count"))
    return 0


def _is_direct_trade_row(payload: Mapping[str, Any]) -> bool:
    keys = set(payload.keys())
    if keys & {"entry_fill_price", "exit_fill_price", "fill_price", "filled_at"}:
        return True
    if {"entry_timestamp", "entry_price", "trade_id"} <= keys:
        return True
    if {"exit_timestamp", "exit_price", "trade_id"} <= keys:
        return True
    return False


def _count_signal(payload: Mapping[str, Any], rel: str) -> int:
    count = _int_get(payload, ("signal_count", "signals", "candidate_signals"))
    if count:
        return count
    verdict = str(payload.get("strategy_verdict") or payload.get("verdict") or "").upper()
    return 1 if "SIGNAL" in verdict and "NO_SIGNAL" not in verdict else 0


def _count_handoff(payload: Mapping[str, Any]) -> int:
    count = _int_get(payload, ("handoff_count", "paper_handoff_count", "submit_attempt_count"))
    if count:
        return count
    return 1 if payload.get("paper_proof_invoked") or payload.get("submit_attempted") or payload.get("managed_lifecycle_invoked") else 0


def _count_issue(payload: Mapping[str, Any]) -> int:
    text = json.dumps(dict(payload), default=str).lower()[:20_000]
    return int(any(token in text for token in ("review_required", "blocked", "failed", "error", "ambiguous", "stale")))


def _count_disabled(payload: Mapping[str, Any]) -> int:
    disabled = 0
    for key, value in _walk_items(payload):
        normalized_key = str(key).lower()
        if normalized_key == "enabled" and value is False:
            disabled += 1
        elif normalized_key in {"disabled", "strategy_disabled", "trading_disabled"} and value is True:
            disabled += 1
        elif normalized_key in {"status", "state", "runtime_presence"} and str(value).upper() == "DISABLED":
            disabled += 1
    return disabled


def _int_get(payload: Mapping[str, Any], keys: tuple[str, ...]) -> int:
    total = 0
    for key, value in _walk_items(payload):
        if key in keys:
            if isinstance(value, list):
                total += len(value)
            else:
                try:
                    total += int(value)
                except (TypeError, ValueError):
                    pass
    return total


def _text_count(sample: str, tokens: tuple[str, ...]) -> int:
    return sum(sample.count(token) for token in tokens)


def _range_from_timestamps(timestamps: list[str]) -> dict[str, str] | None:
    normalized = sorted(set(timestamps))
    if not normalized:
        return None
    return {"start": normalized[0], "end": normalized[-1], "source": "artifact_timestamps"}


def _latest_timestamp_for(payloads: list[Mapping[str, Any]], kind: str, *, rel: str = "") -> str | None:
    stamps: list[str] = []
    for payload in payloads:
        if kind == "trade" and _count_direct_trade(payload, rel):
            stamps.extend(_direct_trade_timestamps(payload))
        elif kind == "signal" and _count_signal(payload, rel):
            stamps.extend(_timestamps(payload))
        elif kind == "handoff" and _count_handoff(payload):
            stamps.extend(_timestamps(payload))
    return max(stamps) if stamps else None


def _direct_trade_timestamps(payload: Mapping[str, Any]) -> list[str]:
    rows = payload.get("rows")
    if isinstance(rows, list):
        stamps: list[str] = []
        for row in rows[:MAX_JSONL_LINES]:
            if isinstance(row, Mapping) and _is_direct_trade_row(row):
                stamps.extend(_direct_trade_timestamps(row))
        return stamps
    stamps = []
    for key, value in _walk_items(payload):
        if key in {"entry_timestamp", "exit_timestamp", "filled_at", "last_trade_at", "created_at"} and isinstance(value, str):
            if _looks_like_timestamp(value):
                stamps.append(value)
    return stamps


def _latest_event(items: list[Mapping[str, Any]], key: str) -> dict[str, Any] | None:
    eligible = [item for item in items if item.get(key)]
    if not eligible:
        return None
    latest = max(eligible, key=lambda item: str(item.get(key)))
    return {"timestamp": latest.get(key), "path": latest.get("path"), "strategy_ids": latest.get("strategy_ids", [])}


def _first_no_activity_after_last_trade(items: list[Mapping[str, Any]], last_trade: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not last_trade:
        return None
    ts = str(last_trade.get("timestamp") or "")
    after = [
        item
        for item in items
        if str((item.get("date_range") or {}).get("start") or "") > ts
        and int(item.get("trade_count") or 0) == 0
        and (int(item.get("signal_count") or 0) > 0 or item.get("artifact_kind") in {"RUNTIME_REPORT", "SIGNAL_LOG"})
    ]
    if not after:
        return None
    first = min(after, key=lambda item: str((item.get("date_range") or {}).get("start") or ""))
    return {"timestamp": (first.get("date_range") or {}).get("start"), "path": first.get("path"), "artifact_kind": first.get("artifact_kind")}


def _date_range(items: list[Mapping[str, Any]]) -> dict[str, str | None]:
    starts = [str((item.get("date_range") or {}).get("start")) for item in items if (item.get("date_range") or {}).get("start")]
    ends = [str((item.get("date_range") or {}).get("end")) for item in items if (item.get("date_range") or {}).get("end")]
    return {"start": min(starts) if starts else None, "end": max(ends) if ends else None}


def _classify(
    *,
    track1_like: list[Mapping[str, Any]],
    trade_evidence: list[Mapping[str, Any]],
    signal_evidence: list[Mapping[str, Any]],
    handoff_evidence: list[Mapping[str, Any]],
    broker_or_lifecycle_evidence: list[Mapping[str, Any]],
    config_disabled_evidence: list[Mapping[str, Any]],
    last_trade: Mapping[str, Any] | None,
    last_signal: Mapping[str, Any] | None,
) -> tuple[str, str | None]:
    if not track1_like or not trade_evidence:
        return "TRACK1_REFERENCE_ARTIFACTS_MISSING", None
    if signal_evidence and last_trade and last_signal and str(last_signal.get("timestamp")) > str(last_trade.get("timestamp")):
        if not handoff_evidence:
            return "TRACK1_SIGNALS_CONTINUED_HANDOFF_BROKE", "Signals appear after the last trade, but handoff evidence is missing."
        if broker_or_lifecycle_evidence:
            return "TRACK1_TRADES_STOPPED_BROKER_OR_LIFECYCLE", "Signals/handoffs exist, but broker/lifecycle issue evidence is present."
        return "TRACK1_SIGNALS_CONTINUED_HANDOFF_BROKE", "Signals continued after the last known trade."
    if config_disabled_evidence:
        return "TRACK1_CONFIG_DISABLED_OR_MISMATCHED", "Config-disabled evidence appears in legacy/Track 1 artifacts."
    if last_trade:
        return "TRACK1_ARTIFACTS_FOUND_NO_CLEAR_BREAKPOINT", "Trade evidence exists, but no clear signal/handoff break is proven."
    return "DIAGNOSTIC_INCONCLUSIVE", None


def _missing_evidence(
    track1_like: list[Mapping[str, Any]],
    trade_evidence: list[Mapping[str, Any]],
    signal_evidence: list[Mapping[str, Any]],
) -> list[str]:
    missing: list[str] = []
    if not track1_like:
        missing.append("No Track 1 / Track A / legacy paper artifacts with parseable evidence were found.")
    if not trade_evidence:
        missing.append("No Track 1-like trade/fill/blotter artifact proving the reported two-week trading period was found.")
    if not signal_evidence:
        missing.append("No Track 1-like signal artifact proving whether signals continued after trading stopped was found.")
    return missing


def _recommended_next_action(classification: str, missing: list[str]) -> str:
    if classification == "TRACK1_REFERENCE_ARTIFACTS_MISSING":
        return "Do not run a full parity audit yet. Export the missing Track 1 trade ledger and signal/handoff log covering the active two-week period and the first stopped sessions."
    if classification == "TRACK1_SIGNALS_CONTINUED_HANDOFF_BROKE":
        return "Inspect signal-to-handoff bridge and paper lifecycle changes around the identified stop point before strategy parity."
    if classification == "TRACK1_CONFIG_DISABLED_OR_MISMATCHED":
        return "Inspect config/registry enablement and artifact path changes around the stop point."
    if classification == "TRACK1_TRADES_STOPPED_BROKER_OR_LIFECYCLE":
        return "Inspect broker readiness, recovery, and lifecycle classifications around the stop point."
    return "Evidence is partial; preserve/export Track 1 trade, signal, handoff, runtime, and config snapshots before deep parity."


def _markdown(report: Mapping[str, Any]) -> str:
    summary = report.get("evidence_summary") if isinstance(report.get("evidence_summary"), Mapping) else {}
    missing = report.get("missing_evidence") if isinstance(report.get("missing_evidence"), list) else []
    breakpoint = report.get("breakpoint") if isinstance(report.get("breakpoint"), Mapping) else {}
    last_trade = _format_event(breakpoint.get("last_known_track1_trade"))
    last_signal = _format_event(breakpoint.get("last_known_strategy_signal"))
    lines = [
        "# Track 1 Trading-Stop Preflight",
        "",
        f"- Classification: `{report.get('classification')}`",
        f"- Track 1-like artifacts found: {report.get('track1_like_artifact_count')}",
        f"- Trade artifacts: {report.get('trade_artifact_count')}",
        f"- Signal artifacts: {report.get('signal_artifact_count')}",
        f"- Total trade count found: {summary.get('trade_count', 0)}",
        f"- Direct trade/fill rows found: {summary.get('direct_trade_count', 0)}",
        f"- Total signal count found: {summary.get('signal_count', 0)}",
        f"- Total handoff count found: {summary.get('handoff_count', 0)}",
        f"- Date range: {summary.get('date_range')}",
        f"- Last known Track 1-like trade: {last_trade}",
        f"- Last known strategy signal: {last_signal}",
        f"- Likely break: {breakpoint.get('likely_break')}",
        "",
        "## Missing Evidence",
    ]
    lines.extend([f"- {item}" for item in missing] or ["- None identified by this bounded preflight."])
    lines.extend(
        [
            "",
            "## Plain Answer",
            f"- Did Track 1 really trade for about two weeks? {'Direct closed-trade evidence exists; this preflight confirms activity but does not prove the exact two-week duration by itself.' if summary.get('direct_trade_count') else 'Not proven by available bounded artifacts.'}",
            f"- When did it stop? {last_trade or 'Not identifiable from available artifacts.'}",
            f"- Did signals stop, or only trades? {breakpoint.get('likely_break') or 'Inconclusive from available artifacts.'}",
            f"- Is a full Track 1 vs Track B parity audit justified? {bool(report.get('full_parity_audit_justified'))}",
            "",
            "## Recommendation",
            str(report.get("recommended_next_action")),
        ]
    )
    return "\n".join(lines) + "\n"


def _format_event(value: Any) -> str | None:
    if not isinstance(value, Mapping):
        return None
    strategies = value.get("strategy_ids") if isinstance(value.get("strategy_ids"), list) else []
    suffix = f"; strategies={len(strategies)}" if strategies else ""
    return f"{value.get('timestamp')} ({value.get('path')}{suffix})"
