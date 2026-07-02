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
ADAPTER_VALIDATION_JSON = "shadow_candidate_adapter_validation.json"
ADAPTER_VALIDATION_MD = "shadow_candidate_adapter_validation.md"
ADAPTER_MIGRATION_MD = "shadow_candidate_adapter_migration_summary.md"
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
    adapter_validation_path: Path
    adapter_migration_path: Path


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
    mixed_candidates, candidate_source_status = discover_candidate_intents(
        output_root=output_root,
        candidate_paths=candidate_paths,
        max_candidates=max_candidates,
        max_source_artifact_bytes=max_source_artifact_bytes,
    )
    candidates, adapter_report = discover_canonical_shadow_candidates(mixed_candidates, source_status=candidate_source_status)
    rows = build_shadow_observation_rows(
        candidates,
        gre_payload=gre_payload,
        crfd_rows=crfd_rows,
        generated_at=generated_at,
        gre_path=source_gre,
        crfd_rows_path=source_crfd,
    )
    previous_rows = build_shadow_observation_rows(
        mixed_candidates,
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
        adapter_report=adapter_report,
    )
    validation = build_adapter_validation_report(
        previous_rows=previous_rows,
        canonical_rows=rows,
        mixed_candidates=mixed_candidates,
        canonical_candidates=candidates,
        adapter_report=adapter_report,
        generated_at=generated_at,
    )

    gold_dir.mkdir(parents=True, exist_ok=True)
    rows_path = gold_dir / OBSERVATIONS_JSONL
    jsonl_config = BoundedJsonlConfig(max_file_bytes=max_jsonl_bytes) if max_jsonl_bytes else BoundedJsonlConfig()
    write_bounded_jsonl(rows_path, rows, config=jsonl_config)
    summary_json_path = gold_dir / SUMMARY_JSON
    write_bounded_snapshot_json(summary_json_path, report, config=BoundedSnapshotConfig())
    summary_markdown_path = gold_dir / SUMMARY_MD
    summary_markdown_path.write_text(render_shadow_observation_summary_markdown(report), encoding="utf-8")
    adapter_validation_path = gold_dir / ADAPTER_VALIDATION_JSON
    write_bounded_snapshot_json(adapter_validation_path, validation, config=BoundedSnapshotConfig())
    adapter_validation_md_path = gold_dir / ADAPTER_VALIDATION_MD
    adapter_validation_md_path.write_text(render_adapter_validation_markdown(validation), encoding="utf-8")
    adapter_migration_path = gold_dir / ADAPTER_MIGRATION_MD
    adapter_migration_path.write_text(render_adapter_migration_markdown(validation), encoding="utf-8")
    return ShadowObservationResult(
        report=report,
        rows=rows,
        rows_path=rows_path,
        summary_json_path=summary_json_path,
        summary_markdown_path=summary_markdown_path,
        adapter_validation_path=adapter_validation_path,
        adapter_migration_path=adapter_migration_path,
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


def discover_canonical_shadow_candidates(
    candidates: Sequence[Mapping[str, Any]],
    *,
    source_status: Sequence[Mapping[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    canonical = []
    for candidate in candidates:
        if not _is_gold_candidate(candidate):
            continue
        item = dict(candidate)
        item.setdefault("candidate_observation_time", _candidate_observation_time(candidate))
        canonical.append(item)
    report = {
        "adapter_id": "gold_only_timestamped_strategy_intent_candidates",
        "diagnostic_only": True,
        "runtime_independent": True,
        "input_candidate_count": len(candidates),
        "canonical_candidate_count": len(canonical),
        "filtered_non_gold_count": len(candidates) - len(canonical),
        "preserves_intended_direction": all(_intended_direction(candidate) in {"LONG", "SHORT"} for candidate in canonical),
        "preserves_strategy_or_lane_identity_count": sum(1 for candidate in canonical if candidate.get("strategy_id") or candidate.get("lane_id")),
        "timestamp_available_count": sum(1 for candidate in canonical if candidate.get("candidate_observation_time")),
        "source_status": [dict(item) for item in source_status or ()],
    }
    return canonical, report


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
            "candidate_observation_time": candidate.get("candidate_observation_time"),
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
    adapter_report: Mapping[str, Any] | None = None,
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
        "candidate_adapter": dict(adapter_report or {}),
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


def build_adapter_validation_report(
    *,
    previous_rows: Sequence[Mapping[str, Any]],
    canonical_rows: Sequence[Mapping[str, Any]],
    mixed_candidates: Sequence[Mapping[str, Any]],
    canonical_candidates: Sequence[Mapping[str, Any]],
    adapter_report: Mapping[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    previous = _population_metrics(previous_rows, mixed_candidates)
    canonical = _population_metrics(canonical_rows, canonical_candidates)
    return {
        "schema_version": "track_b_gre_shadow_candidate_adapter_validation_v1",
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "production_effect": False,
        "adapter": dict(adapter_report),
        "previous_observation_population": previous,
        "canonical_observation_population": canonical,
        "migration_delta": {
            "candidate_count_delta": canonical["candidate_count"] - previous["candidate_count"],
            "observation_count_delta": canonical["observation_count"] - previous["observation_count"],
            "would_allow_delta": canonical["result_counts"].get("WOULD_ALLOW", 0) - previous["result_counts"].get("WOULD_ALLOW", 0),
            "would_block_delta": canonical["result_counts"].get("WOULD_BLOCK", 0) - previous["result_counts"].get("WOULD_BLOCK", 0),
            "insufficient_evidence_delta": canonical["result_counts"].get("INSUFFICIENT_EVIDENCE", 0)
            - previous["result_counts"].get("INSUFFICIENT_EVIDENCE", 0),
            "both_join_rate_delta": _delta(canonical["join_quality"]["both_join_rate"], previous["join_quality"]["both_join_rate"]),
        },
        "recommendations": {
            "shadow_framework_v1_architecturally_complete": True,
            "future_work_shift_to_general_research_platform": True,
            "remaining_shadow_work_policy_evaluation_not_infrastructure": True,
            "runtime_hook_needed": False,
            "production_gate_recommended": False,
        },
        "safety_contract": {
            "runtime_hook": False,
            "trading_gate": False,
            "broker_actions": False,
            "managed_exit_integration": False,
            "strategy_decision_changes": False,
            "gre_scoring_changes": False,
        },
    }


def _population_metrics(rows: Sequence[Mapping[str, Any]], candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "candidate_count": len(candidates),
        "observation_count": len(rows),
        "result_counts": _count_by(rows, "shadow_gate_result"),
        "contract_counts": _count_by(rows, "contract"),
        "session_counts": _count_by(rows, "session"),
        "intended_direction_counts": _count_by(rows, "intended_direction"),
        "join_quality": _join_quality(rows, candidates),
    }


def _join_quality(rows: Sequence[Mapping[str, Any]], candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    count = len(rows)
    gre_joined = sum(1 for row in rows if row.get("gre_label") not in {None, "", "INSUFFICIENT_EVIDENCE"})
    crfd_joined = sum(1 for row in rows if (row.get("source_refs") or {}).get("crfd_observation_time"))
    both_joined = sum(
        1
        for row in rows
        if row.get("gre_label") not in {None, "", "INSUFFICIENT_EVIDENCE"} and (row.get("source_refs") or {}).get("crfd_observation_time")
    )
    timestamped = sum(1 for candidate in candidates if candidate.get("candidate_observation_time"))
    missing_reasons: list[str] = []
    for row, candidate in zip(rows, candidates, strict=False):
        if row.get("gre_label") in {None, "", "INSUFFICIENT_EVIDENCE"}:
            missing_reasons.append("missing_gre_join")
        if not (row.get("source_refs") or {}).get("crfd_observation_time"):
            missing_reasons.append("missing_crfd_join")
        if not candidate.get("candidate_observation_time"):
            missing_reasons.append("missing_candidate_timestamp")
        if row.get(f"avwap_{SELECTED_AVWAP_ANCHOR}_relation") == "unavailable":
            missing_reasons.append("missing_globex_avwap")
    return {
        "gre_join_rate": _rate(gre_joined, count),
        "crfd_join_rate": _rate(crfd_joined, count),
        "both_join_rate": _rate(both_joined, count),
        "timestamp_alignment_quality": _rate(timestamped, len(candidates)),
        "missing_reasons": _count_values(missing_reasons),
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
        f"- Candidate adapter: {(report.get('candidate_adapter') or {}).get('adapter_id')}",
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


def render_adapter_validation_markdown(report: Mapping[str, Any]) -> str:
    previous = report.get("previous_observation_population") or {}
    canonical = report.get("canonical_observation_population") or {}
    delta = report.get("migration_delta") or {}
    return "\n".join(
        [
            "# Shadow Candidate Adapter Validation",
            "",
            f"- Generated at: {report.get('generated_at')}",
            f"- Adapter: {(report.get('adapter') or {}).get('adapter_id')}",
            f"- Previous observations: {previous.get('observation_count')}",
            f"- Canonical observations: {canonical.get('observation_count')}",
            f"- Previous results: {_format_counts(previous.get('result_counts'))}",
            f"- Canonical results: {_format_counts(canonical.get('result_counts'))}",
            f"- Both-join rate delta: {delta.get('both_join_rate_delta')}",
            f"- Runtime hook needed: {(report.get('recommendations') or {}).get('runtime_hook_needed')}",
            "",
        ]
    )


def render_adapter_migration_markdown(report: Mapping[str, Any]) -> str:
    previous = report.get("previous_observation_population") or {}
    canonical = report.get("canonical_observation_population") or {}
    rec = report.get("recommendations") or {}
    return "\n".join(
        [
            "# Shadow Candidate Adapter Migration Summary",
            "",
            "| Population | Candidates | Observations | Results | Both join | Timestamp quality |",
            "|---|---:|---:|---|---:|---:|",
            f"| Previous mixed offline | {previous.get('candidate_count')} | {previous.get('observation_count')} | "
            f"{_format_counts(previous.get('result_counts'))} | {previous.get('join_quality', {}).get('both_join_rate')} | "
            f"{previous.get('join_quality', {}).get('timestamp_alignment_quality')} |",
            f"| Canonical Gold-only | {canonical.get('candidate_count')} | {canonical.get('observation_count')} | "
            f"{_format_counts(canonical.get('result_counts'))} | {canonical.get('join_quality', {}).get('both_join_rate')} | "
            f"{canonical.get('join_quality', {}).get('timestamp_alignment_quality')} |",
            "",
            "## Recommendation",
            f"- Shadow Framework V1 architecturally complete: {rec.get('shadow_framework_v1_architecturally_complete')}",
            f"- Future work should shift to general research platform: {rec.get('future_work_shift_to_general_research_platform')}",
            f"- Remaining shadow work should be policy evaluation: {rec.get('remaining_shadow_work_policy_evaluation_not_infrastructure')}",
            f"- Production gate recommended: {rec.get('production_gate_recommended')}",
            "",
        ]
    )


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
    inherited_keys = ("bar_id", "generated_at", "timestamp", "session", "session_label", "strategy_id", "lane_id")

    def visit(value: Any, depth: int, inherited: Mapping[str, Any]) -> None:
        if len(result) >= max_items or depth > 5:
            return
        if isinstance(value, Mapping):
            merged = {key: nested for key, nested in inherited.items() if key not in value}
            merged.update(value)
            result.append(merged)
            next_inherited = dict(inherited)
            for key in inherited_keys:
                if value.get(key) is not None:
                    next_inherited[key] = value[key]
            for nested in value.values():
                visit(nested, depth + 1, next_inherited)
        elif isinstance(value, list):
            for nested in value[:max_items]:
                visit(nested, depth + 1, inherited)

    visit(payload, 0, {})
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
        "candidate_observation_time": _candidate_observation_time(item),
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
        notes.append("No canonical Gold strategy-intent candidates were available from the configured offline artifact paths.")
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


def _is_gold_candidate(candidate: Mapping[str, Any]) -> bool:
    return _root_symbol(_contract_from_candidate(candidate)) in GOLD_SYMBOLS


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


def _candidate_observation_time(candidate: Mapping[str, Any]) -> str | None:
    for key in ("candidate_observation_time", "observation_time", "generated_at", "timestamp", "bar_end_ts", "bar_timestamp"):
        value = candidate.get(key)
        if value:
            parsed = _parse_datetime(value)
            return parsed.isoformat() if parsed else str(value)
    bar_id = _first_string(candidate, ("bar_id",))
    if bar_id and "|" in bar_id:
        parsed = _parse_datetime(bar_id.rsplit("|", 1)[-1])
        return parsed.isoformat() if parsed else None
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


def _count_values(values: Sequence[Any]) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values:
        text = str(value or "UNKNOWN")
        result[text] = result.get(text, 0) + 1
    return dict(sorted(result.items()))


def _format_counts(value: Any) -> str:
    if not isinstance(value, Mapping):
        return "{}"
    return ", ".join(f"{key}={count}" for key, count in value.items()) or "{}"


def _rate(part: int, whole: int) -> float | None:
    if whole <= 0:
        return None
    return round(part / whole, 4)


def _delta(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return round(left - right, 4)


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
