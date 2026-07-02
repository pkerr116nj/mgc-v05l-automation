"""Offline diagnostic GRE shadow gate observation generator."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_jsonl import BoundedJsonlConfig, write_bounded_jsonl
from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.execution_core.track_b_gold_regime_engine import DEFAULT_OUTPUT_ROOT, GOLD_REGIME_OUTPUT_DIR
from mgc_v05l.execution_core.track_b_research_feature_dataset import RESEARCH_FEATURE_DATASET_DIR, RESEARCH_FEATURE_DATASET_JSONL


SCHEMA_VERSION = "track_b_gre_shadow_gate_observation_v1"
SUMMARY_SCHEMA_VERSION = "track_b_gre_shadow_gate_observation_summary_v1"
SOURCE_MODE = "offline_artifact_generation"
SELECTED_SHADOW_POLICY = "avwap_confirmation_globex_session_open_18et"
SELECTED_AVWAP_ANCHOR = "globex_session_open_18et"
OBSERVATIONS_JSONL = "gre_shadow_gate_observations.jsonl"
SUMMARY_JSON = "latest_gre_shadow_gate_observation_summary.json"
SUMMARY_MD = "latest_gre_shadow_gate_observation_summary.md"
DEFAULT_MAX_SOURCE_ARTIFACT_BYTES = 10 * 1024 * 1024
DEFAULT_MAX_CANDIDATES = 200
GOLD_SYMBOLS = {"GC", "MGC"}


@dataclass(frozen=True)
class ShadowObservationResult:
    report: dict[str, Any]
    rows: list[dict[str, Any]]
    rows_path: Path
    summary_json_path: Path
    summary_markdown_path: Path


def run_gre_shadow_observation_generator(
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    now: datetime | str | None = None,
    gre_path: Path | None = None,
    crfd_rows_path: Path | None = None,
    candidate_paths: Sequence[Path] | None = None,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
    max_source_artifact_bytes: int = DEFAULT_MAX_SOURCE_ARTIFACT_BYTES,
    max_jsonl_bytes: int | None = None,
) -> ShadowObservationResult:
    generated_at = _coerce_now(now)
    gold_dir = output_root / GOLD_REGIME_OUTPUT_DIR
    source_gre = gre_path or gold_dir / "latest_gold_regime_engine.json"
    source_crfd = crfd_rows_path or output_root / RESEARCH_FEATURE_DATASET_DIR / RESEARCH_FEATURE_DATASET_JSONL
    gre_payload, gre_status = _read_json_mapping(source_gre, max_bytes=max_source_artifact_bytes)
    crfd_rows = _read_jsonl(source_crfd)
    candidates, candidate_source_status = discover_candidate_intents(
        output_root=output_root,
        candidate_paths=candidate_paths,
        max_candidates=max_candidates,
        max_source_artifact_bytes=max_source_artifact_bytes,
    )
    rows = build_shadow_observation_rows(
        candidates,
        gre_payload=gre_payload,
        crfd_rows=crfd_rows,
        generated_at=generated_at,
        gre_path=source_gre,
        crfd_rows_path=source_crfd,
    )
    report = build_shadow_observation_summary(
        rows,
        candidates=candidates,
        generated_at=generated_at,
        gre_status=gre_status,
        candidate_source_status=candidate_source_status,
        gre_path=source_gre,
        crfd_rows_path=source_crfd,
    )

    gold_dir.mkdir(parents=True, exist_ok=True)
    rows_path = gold_dir / OBSERVATIONS_JSONL
    jsonl_config = BoundedJsonlConfig(max_file_bytes=max_jsonl_bytes) if max_jsonl_bytes else BoundedJsonlConfig()
    write_bounded_jsonl(rows_path, rows, config=jsonl_config)
    summary_json_path = gold_dir / SUMMARY_JSON
    write_bounded_snapshot_json(summary_json_path, report, config=BoundedSnapshotConfig())
    summary_markdown_path = gold_dir / SUMMARY_MD
    summary_markdown_path.write_text(render_shadow_observation_summary_markdown(report), encoding="utf-8")
    return ShadowObservationResult(
        report=report,
        rows=rows,
        rows_path=rows_path,
        summary_json_path=summary_json_path,
        summary_markdown_path=summary_markdown_path,
    )


def discover_candidate_intents(
    *,
    output_root: Path,
    candidate_paths: Sequence[Path] | None = None,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
    max_source_artifact_bytes: int = DEFAULT_MAX_SOURCE_ARTIFACT_BYTES,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    paths = list(candidate_paths or _default_candidate_paths(output_root))
    candidates: list[dict[str, Any]] = []
    source_status: list[dict[str, Any]] = []
    for path in paths:
        status: dict[str, Any] = {"path": str(path), "exists": path.exists()}
        if not path.exists():
            status["status"] = "missing"
            source_status.append(status)
            continue
        try:
            size = path.stat().st_size
        except OSError as exc:
            status.update({"status": "stat_failed", "error": str(exc)})
            source_status.append(status)
            continue
        status["size_bytes"] = size
        if size > max_source_artifact_bytes:
            status["status"] = "skipped_oversized_artifact"
            source_status.append(status)
            continue
        extracted = _extract_candidates_from_path(path, max_items=max_candidates - len(candidates))
        status.update({"status": "read", "candidate_count": len(extracted)})
        source_status.append(status)
        candidates.extend(extracted)
        if len(candidates) >= max_candidates:
            break
    return candidates[:max_candidates], source_status


def build_shadow_observation_rows(
    candidates: Sequence[Mapping[str, Any]],
    *,
    gre_payload: Mapping[str, Any],
    crfd_rows: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    gre_path: Path | str,
    crfd_rows_path: Path | str,
) -> list[dict[str, Any]]:
    crfd_index = _CrfdIndex(crfd_rows)
    rows: list[dict[str, Any]] = []
    gre_generated_at = _parse_datetime(gre_payload.get("generated_at"))
    for idx, candidate in enumerate(candidates, start=1):
        rows.append(
            build_shadow_observation_row(
                candidate,
                gre_payload=gre_payload,
                crfd_index=crfd_index,
                generated_at=generated_at,
                gre_generated_at=gre_generated_at,
                sequence=idx,
                gre_path=gre_path,
                crfd_rows_path=crfd_rows_path,
            )
        )
    return rows


def build_shadow_observation_row(
    candidate: Mapping[str, Any],
    *,
    gre_payload: Mapping[str, Any],
    crfd_index: "_CrfdIndex",
    generated_at: datetime,
    gre_generated_at: datetime | None,
    sequence: int,
    gre_path: Path | str,
    crfd_rows_path: Path | str,
) -> dict[str, Any]:
    contract = _contract_from_candidate(candidate)
    intended_direction = _intended_direction(candidate)
    crfd = crfd_index.latest_at_or_before(contract=contract, timestamp=gre_generated_at) or {}
    gre_label = str(gre_payload.get("regime_label") or "INSUFFICIENT_EVIDENCE")
    avwap_relation = str(crfd.get(f"avwap_relation_{SELECTED_AVWAP_ANCHOR}") or "unavailable")
    result, reason = evaluate_shadow_policy(
        gre_label=gre_label,
        intended_direction=intended_direction,
        contract=contract,
        avwap_relation=avwap_relation,
        gre_payload=gre_payload,
    )
    observation_id = _observation_id(generated_at, sequence)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "observation_id": observation_id,
        "source_mode": SOURCE_MODE,
        "event_type": "SHADOW_GATE_DECISION_COMPUTED",
        "strategy_id": _first_string(candidate, ("strategy_id", "strategy", "strategy_name")),
        "lane_id": _first_string(candidate, ("lane_id", "lane", "strategy_lane_id")),
        "symbol": _first_string(candidate, ("symbol", "root_symbol", "instrument")) or contract,
        "contract": contract,
        "side": _first_string(candidate, ("side", "action", "order_action")),
        "intended_direction": intended_direction,
        "session": _first_string(candidate, ("session", "session_label")) or crfd.get("session") or gre_payload.get("session") or "UNKNOWN",
        "gre_label": gre_label,
        "gre_confidence": _number(gre_payload.get("confidence")),
        "gre_directional_bias": gre_payload.get("directional_bias"),
        "vwap_relation": crfd.get("vwap_relation") or "unavailable",
        f"avwap_{SELECTED_AVWAP_ANCHOR}_relation": avwap_relation,
        "selected_shadow_policy": SELECTED_SHADOW_POLICY,
        "shadow_gate_result": result,
        "shadow_gate_reason": reason,
        "runtime_action_taken": _runtime_action_taken(candidate),
        "production_effect": False,
        "diagnostic_only": True,
        "source_refs": {
            "gre_output": str(gre_path),
            "crfd_rows": str(crfd_rows_path),
            "candidate_source": candidate.get("source_path"),
            "crfd_observation_time": crfd.get("observation_time"),
        },
    }


def evaluate_shadow_policy(
    *,
    gre_label: str,
    intended_direction: str | None,
    contract: str,
    avwap_relation: str,
    gre_payload: Mapping[str, Any],
) -> tuple[str, str]:
    root = _root_symbol(contract)
    if root not in GOLD_SYMBOLS:
        return "INSUFFICIENT_EVIDENCE", "GRE_GOLD_ONLY_SYMBOL_UNSUPPORTED"
    if not gre_payload:
        return "INSUFFICIENT_EVIDENCE", "GRE_OUTPUT_MISSING"
    if intended_direction not in {"LONG", "SHORT"}:
        return "INSUFFICIENT_EVIDENCE", "INTENDED_DIRECTION_UNAVAILABLE"
    if avwap_relation == "unavailable":
        return "INSUFFICIENT_EVIDENCE", "GLOBEX_AVWAP_UNAVAILABLE"
    if intended_direction == "LONG" and gre_label == "LONG" and avwap_relation == "above_avwap":
        return "WOULD_ALLOW", "GRE_LONG_CONFIRMED_BY_GLOBEX_AVWAP"
    if intended_direction == "SHORT" and gre_label == "SHORT" and avwap_relation == "below_avwap":
        return "WOULD_ALLOW", "GRE_SHORT_CONFIRMED_BY_GLOBEX_AVWAP"
    return "WOULD_BLOCK", "GRE_OR_GLOBEX_AVWAP_NOT_CONFIRMING_DIRECTION"


def build_shadow_observation_summary(
    rows: Sequence[Mapping[str, Any]],
    *,
    candidates: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    gre_status: Mapping[str, Any],
    candidate_source_status: Sequence[Mapping[str, Any]],
    gre_path: Path | str,
    crfd_rows_path: Path | str,
) -> dict[str, Any]:
    counts = _count_by(rows, "shadow_gate_result")
    source_notes = _source_notes(candidates, gre_status, candidate_source_status)
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "production_effect": False,
        "source_mode": SOURCE_MODE,
        "selected_shadow_policy": SELECTED_SHADOW_POLICY,
        "observation_count": len(rows),
        "candidate_count": len(candidates),
        "result_counts": counts,
        "source_artifacts": {
            "gre_output": str(gre_path),
            "crfd_rows": str(crfd_rows_path),
        },
        "gre_source_status": dict(gre_status),
        "candidate_source_status": [dict(item) for item in candidate_source_status],
        "source_notes": source_notes,
        "writer": {
            "observations": "bounded_jsonl",
            "summary": "bounded_snapshot_json",
        },
        "safety_contract": {
            "offline_only": True,
            "runtime_hook": False,
            "broker_actions": False,
            "managed_exit_integration": False,
            "strategy_decision_changes": False,
            "trading_gate": False,
        },
    }


def render_shadow_observation_summary_markdown(report: Mapping[str, Any]) -> str:
    counts = report.get("result_counts") or {}
    lines = [
        "# GRE Shadow Gate Observation Summary",
        "",
        f"- Generated at: {report.get('generated_at')}",
        f"- Diagnostic only: {report.get('diagnostic_only')}",
        f"- Production effect: {report.get('production_effect')}",
        f"- Policy: {report.get('selected_shadow_policy')}",
        f"- Observations: {report.get('observation_count')}",
        f"- Candidates: {report.get('candidate_count')}",
        f"- WOULD_ALLOW: {counts.get('WOULD_ALLOW', 0)}",
        f"- WOULD_BLOCK: {counts.get('WOULD_BLOCK', 0)}",
        f"- INSUFFICIENT_EVIDENCE: {counts.get('INSUFFICIENT_EVIDENCE', 0)}",
        "",
        "## Source Notes",
    ]
    for note in report.get("source_notes") or []:
        lines.append(f"- {note}")
    lines.extend(
        [
            "",
            "## Safety",
            "- Offline artifact generation only.",
            "- No runtime hook, broker action, strategy gate, or Managed Exit integration.",
        ]
    )
    return "\n".join(lines) + "\n"


class _CrfdIndex:
    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        by_contract: dict[str, list[tuple[datetime, Mapping[str, Any]]]] = {}
        for row in rows:
            ts = _parse_datetime(row.get("observation_time"))
            contract = str(row.get("contract") or "").upper()
            if ts is None or not contract:
                continue
            by_contract.setdefault(contract, []).append((ts, row))
        self._rows = {contract: sorted(values, key=lambda item: item[0]) for contract, values in by_contract.items()}

    def latest_at_or_before(self, *, contract: str, timestamp: datetime | None) -> Mapping[str, Any] | None:
        rows = self._rows.get(str(contract or "").upper(), ())
        if not rows:
            return None
        if timestamp is None:
            return rows[-1][1]
        candidate: Mapping[str, Any] | None = None
        for row_ts, row in rows:
            if row_ts > timestamp:
                break
            candidate = row
        return candidate or rows[-1][1]


def _default_candidate_paths(output_root: Path) -> list[Path]:
    outputs_root = output_root.parent if output_root.name == "track_b_execution_core" else output_root
    return [
        outputs_root / "probationary_pattern_engine" / "paper_session" / "blocked_strategy_intent_latest.json",
        outputs_root / "probationary_pattern_engine" / "paper_session" / "blocked_strategy_intents.jsonl",
        outputs_root / "reports" / "ibkr_paper_strategy_bridge_report.json",
    ]


def _extract_candidates_from_path(path: Path, *, max_items: int) -> list[dict[str, Any]]:
    if max_items <= 0:
        return []
    if path.suffix == ".jsonl":
        return _extract_candidates_from_jsonl(path, max_items=max_items)
    payload, _ = _read_json_mapping(path, max_bytes=DEFAULT_MAX_SOURCE_ARTIFACT_BYTES)
    return _extract_candidates(payload, source_path=path, max_items=max_items)


def _extract_candidates_from_jsonl(path: Path, *, max_items: int) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return []
    for line in lines[-max_items:]:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        candidates.extend(_extract_candidates(payload, source_path=path, max_items=max_items - len(candidates)))
        if len(candidates) >= max_items:
            break
    return candidates[:max_items]


def _extract_candidates(payload: Any, *, source_path: Path, max_items: int) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for item in _walk_candidate_mappings(payload, max_items=max_items * 4):
        candidate = _normalize_candidate(item, source_path=source_path)
        if candidate is not None:
            found.append(candidate)
        if len(found) >= max_items:
            break
    return found


def _walk_candidate_mappings(payload: Any, *, max_items: int) -> list[Mapping[str, Any]]:
    result: list[Mapping[str, Any]] = []

    def visit(value: Any, depth: int) -> None:
        if len(result) >= max_items or depth > 5:
            return
        if isinstance(value, Mapping):
            result.append(value)
            for nested in value.values():
                visit(nested, depth + 1)
        elif isinstance(value, list):
            for nested in value[:max_items]:
                visit(nested, depth + 1)

    visit(payload, 0)
    return result


def _normalize_candidate(item: Mapping[str, Any], *, source_path: Path) -> dict[str, Any] | None:
    contract = _contract_from_candidate(item)
    direction = _intended_direction(item)
    if not contract or not direction:
        return None
    if not any(item.get(key) for key in ("strategy_id", "lane_id", "side", "action", "order_action", "intent", "order_intent_id")):
        return None
    return {
        "strategy_id": _first_string(item, ("strategy_id", "strategy", "strategy_name")),
        "lane_id": _first_string(item, ("lane_id", "lane", "strategy_lane_id")),
        "symbol": _first_string(item, ("symbol", "root_symbol", "instrument")),
        "contract": contract,
        "side": _first_string(item, ("side", "action", "order_action")),
        "intended_direction": direction,
        "session": _first_string(item, ("session", "session_label")),
        "runtime_action_taken": _first_string(item, ("runtime_action_taken", "runtime_action", "action_taken")),
        "source_path": str(source_path),
    }


def _read_json_mapping(path: Path, *, max_bytes: int) -> tuple[dict[str, Any], dict[str, Any]]:
    status: dict[str, Any] = {"path": str(path), "exists": path.exists()}
    if not path.exists():
        status["status"] = "missing"
        return {}, status
    try:
        size = path.stat().st_size
    except OSError as exc:
        status.update({"status": "stat_failed", "error": str(exc)})
        return {}, status
    status["size_bytes"] = size
    if size > max_bytes:
        status["status"] = "skipped_oversized_artifact"
        return {}, status
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        status.update({"status": "read_failed", "error": str(exc)})
        return {}, status
    if not isinstance(payload, dict):
        status["status"] = "not_mapping"
        return {}, status
    status["status"] = "read"
    return payload, status


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return []
    return rows


def _source_notes(
    candidates: Sequence[Mapping[str, Any]],
    gre_status: Mapping[str, Any],
    candidate_source_status: Sequence[Mapping[str, Any]],
) -> list[str]:
    notes: list[str] = []
    if gre_status.get("status") != "read":
        notes.append(f"GRE source not usable: {gre_status.get('status')}")
    if not candidates:
        notes.append("No candidate strategy-intent diagnostics were available from the configured offline artifact paths.")
    for item in candidate_source_status:
        if item.get("status") in {"missing", "skipped_oversized_artifact", "read_failed"}:
            notes.append(f"{item.get('path')}: {item.get('status')}")
    return notes or ["Offline diagnostic observation generation completed."]


def _contract_from_candidate(candidate: Mapping[str, Any]) -> str:
    contract = candidate.get("contract")
    if isinstance(contract, Mapping):
        value = _first_string(contract, ("local_symbol", "symbol", "root_symbol", "instrument"))
        if value:
            return value.upper()
    value = _first_string(candidate, ("contract", "local_symbol", "symbol", "root_symbol", "instrument"))
    return str(value or "GC").upper()


def _root_symbol(contract: str) -> str:
    root = "".join(ch for ch in str(contract or "").upper() if ch.isalpha())
    if root.startswith("MGC"):
        return "MGC"
    if root.startswith("GC"):
        return "GC"
    return root


def _intended_direction(candidate: Mapping[str, Any]) -> str | None:
    explicit = _first_string(candidate, ("intended_direction", "direction", "directional_bias"))
    if explicit:
        upper = explicit.upper()
        if upper in {"LONG", "BULLISH", "BUY"}:
            return "LONG"
        if upper in {"SHORT", "BEARISH", "SELL"}:
            return "SHORT"
    side = _first_string(candidate, ("side", "action", "order_action"))
    if side:
        upper = side.upper()
        if upper == "BUY":
            return "LONG"
        if upper == "SELL":
            return "SHORT"
    return None


def _runtime_action_taken(candidate: Mapping[str, Any]) -> str:
    value = _first_string(candidate, ("runtime_action_taken", "runtime_action", "action_taken"))
    allowed = {"UNKNOWN_OFFLINE", "SUBMITTED", "BLOCKED_BY_RUNTIME", "HOLD_ONLY", "NO_INTENT"}
    if value and value.upper() in allowed:
        return value.upper()
    return "UNKNOWN_OFFLINE"


def _first_string(payload: Mapping[str, Any], keys: Sequence[str]) -> str | None:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, (Mapping, list, tuple, set)):
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _number(value: Any) -> float | int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _count_by(rows: Sequence[Mapping[str, Any]], key: str) -> dict[str, int]:
    result: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "UNKNOWN")
        result[value] = result.get(value, 0) + 1
    return dict(sorted(result.items()))


def _observation_id(generated_at: datetime, sequence: int) -> str:
    return f"gre-shadow-{generated_at.strftime('%Y%m%dT%H%M%SZ')}-{sequence:05d}"


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    text = str(value).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
