"""Canonical Trade Path layer.

This module promotes retained trade-path captures into first-class canonical
research objects. It is diagnostic-only and has no broker, runtime, strategy,
Managed Exit, or trading-gate authority.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_trade_decision_attribution import (
    ATTRIBUTION_JSONL,
    DEFAULT_OUTPUT_DIR as DEFAULT_RA3_OUTPUT_DIR,
)
from mgc_v05l.execution_core.track_b_trade_outcome_enrichment import (
    DEFAULT_OUTPUT_DIR as DEFAULT_CTOE_OUTPUT_DIR,
    ENRICHMENT_JSONL,
)
from mgc_v05l.execution_core.track_b_trade_outcome_layer import (
    DEFAULT_OUTPUT_DIR as DEFAULT_CTOL_OUTPUT_DIR,
    OUTCOMES_JSONL,
)
from mgc_v05l.execution_core.track_b_trade_path_reconstruction import (
    DEFAULT_OUTPUT_DIR as DEFAULT_RA5_OUTPUT_DIR,
    RETAINED_PATH_JSONL,
)


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_OUTCOMES_PATH = DEFAULT_CTOL_OUTPUT_DIR / OUTCOMES_JSONL
DEFAULT_ENRICHMENTS_PATH = DEFAULT_CTOE_OUTPUT_DIR / ENRICHMENT_JSONL
DEFAULT_ATTRIBUTIONS_PATH = DEFAULT_RA3_OUTPUT_DIR / ATTRIBUTION_JSONL
DEFAULT_RETAINED_PATHS = DEFAULT_RA5_OUTPUT_DIR / RETAINED_PATH_JSONL
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research_analytics" / "canonical_trade_path_layer"

CANONICAL_TRADE_PATHS_JSONL = "canonical_trade_paths.jsonl"
CONTRACT_MD = "ra7_canonical_trade_path_contract.md"
SCHEMA_JSON = "ra7_canonical_trade_path_schema.json"
COVERAGE_MD = "ra7_trade_path_coverage_report.md"
PROVENANCE_MD = "ra7_trade_path_provenance_report.md"
READINESS_MD = "ra7_counterfactual_readiness_report.md"
SUMMARY_JSON = "ra7_canonical_trade_path_summary.json"

SCHEMA_VERSION = "canonical_trade_path_v1"
SUMMARY_SCHEMA_VERSION = "ra7_canonical_trade_path_summary_v1"


@dataclass(frozen=True)
class CanonicalTradePathLayerResult:
    rows: list[dict[str, Any]]
    summary: dict[str, Any]
    canonical_paths_path: Path
    summary_path: Path
    contract_path: Path
    schema_path: Path
    coverage_path: Path
    provenance_path: Path
    readiness_path: Path


def run_canonical_trade_path_layer(
    *,
    outcomes_path: Path = DEFAULT_OUTCOMES_PATH,
    enrichments_path: Path = DEFAULT_ENRICHMENTS_PATH,
    attributions_path: Path = DEFAULT_ATTRIBUTIONS_PATH,
    retained_paths_path: Path = DEFAULT_RETAINED_PATHS,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    now: datetime | str | None = None,
) -> CanonicalTradePathLayerResult:
    generated_at = _coerce_now(now)
    outcomes = _read_jsonl(outcomes_path)
    enrichments = _read_jsonl(enrichments_path)
    attributions = _read_jsonl(attributions_path)
    retained_paths = _read_jsonl(retained_paths_path)
    rows = build_canonical_trade_paths(
        outcomes,
        enrichments=enrichments,
        attributions=attributions,
        retained_paths=retained_paths,
        generated_at=generated_at,
        source_paths={
            "ctol": outcomes_path,
            "ctoe": enrichments_path,
            "ra3_attribution": attributions_path,
            "retained_trade_path_capture": retained_paths_path,
        },
    )
    summary = build_canonical_trade_path_summary(
        rows,
        generated_at=generated_at,
        source_paths={
            "ctol": outcomes_path,
            "ctoe": enrichments_path,
            "ra3_attribution": attributions_path,
            "retained_trade_path_capture": retained_paths_path,
        },
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    canonical_paths_path = output_dir / CANONICAL_TRADE_PATHS_JSONL
    summary_path = output_dir / SUMMARY_JSON
    contract_path = output_dir / CONTRACT_MD
    schema_path = output_dir / SCHEMA_JSON
    coverage_path = output_dir / COVERAGE_MD
    provenance_path = output_dir / PROVENANCE_MD
    readiness_path = output_dir / READINESS_MD
    _write_jsonl(canonical_paths_path, rows)
    _write_json(summary_path, summary)
    schema_path.write_text(json.dumps(_schema(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    contract_path.write_text(render_contract_markdown(), encoding="utf-8")
    coverage_path.write_text(render_coverage_markdown(summary), encoding="utf-8")
    provenance_path.write_text(render_provenance_markdown(summary), encoding="utf-8")
    readiness_path.write_text(render_readiness_markdown(summary), encoding="utf-8")
    return CanonicalTradePathLayerResult(
        rows=rows,
        summary=summary,
        canonical_paths_path=canonical_paths_path,
        summary_path=summary_path,
        contract_path=contract_path,
        schema_path=schema_path,
        coverage_path=coverage_path,
        provenance_path=provenance_path,
        readiness_path=readiness_path,
    )


def build_canonical_trade_paths(
    outcomes: Sequence[Mapping[str, Any]],
    *,
    enrichments: Sequence[Mapping[str, Any]] = (),
    attributions: Sequence[Mapping[str, Any]] = (),
    retained_paths: Sequence[Mapping[str, Any]] = (),
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> list[dict[str, Any]]:
    enrichment_index = {str(row.get("trade_outcome_id")): row for row in enrichments if row.get("trade_outcome_id")}
    attribution_index = {str(row.get("trade_outcome_id")): row for row in attributions if row.get("trade_outcome_id")}
    retained_index = _RetainedPathIndex(retained_paths)
    rows: list[dict[str, Any]] = []
    for outcome in outcomes:
        trade_outcome_id = str(outcome.get("trade_outcome_id") or "")
        retained = retained_index.find(outcome)
        enrichment = enrichment_index.get(trade_outcome_id, {})
        attribution = attribution_index.get(trade_outcome_id, {})
        path = retained.get("entry_to_exit_path") if isinstance(retained.get("entry_to_exit_path"), list) else []
        path_start = path[0].get("bar_end") if path and isinstance(path[0], Mapping) else None
        path_end = path[-1].get("bar_end") if path and isinstance(path[-1], Mapping) else None
        coverage_status = _coverage_status(outcome=outcome, path=path, path_start=path_start, path_end=path_end)
        mfe, mfe_ts = _extreme(path, "favorable_excursion_points", maximum=True)
        mae, mae_ts = _extreme(path, "adverse_excursion_points", maximum=False)
        readiness = _readiness(path=path, retained=retained)
        source_trade_id = _source_trade_id(outcome) or _str_or_none(retained.get("source_trade_id"))
        row = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": generated_at.isoformat(),
            "canonical_trade_path_id": _stable_id("canonical_trade_path", source_trade_id, trade_outcome_id, outcome.get("entry_time"), outcome.get("exit_time")),
            "source_trade_id": source_trade_id,
            "trade_outcome_id": trade_outcome_id or None,
            "trade_enrichment_id": enrichment.get("trade_outcome_enrichment_id") or enrichment.get("trade_outcome_id"),
            "trade_decision_attribution_id": attribution.get("trade_decision_attribution_id"),
            "instrument": outcome.get("instrument") or retained.get("instrument"),
            "contract": outcome.get("contract") or retained.get("contract"),
            "side": outcome.get("side") or retained.get("side"),
            "entry_timestamp": outcome.get("entry_time") or retained.get("entry_time"),
            "exit_timestamp": outcome.get("exit_time") or retained.get("exit_time"),
            "entry_price": outcome.get("entry_price") if outcome.get("entry_price") is not None else retained.get("entry_price"),
            "exit_price": outcome.get("exit_price") if outcome.get("exit_price") is not None else retained.get("exit_price"),
            "quantity": outcome.get("quantity"),
            "path_source": "RETAINED_TRADE_PATH_CAPTURE" if path else "NONE",
            "path_source_artifact": str((source_paths or {}).get("retained_trade_path_capture", "")) if path else None,
            "path_sample_count": len(path),
            "path_start_timestamp": path_start,
            "path_end_timestamp": path_end,
            "path_granularity": "1m" if path else None,
            "path_complete_entry_to_exit": coverage_status == "COMPLETE",
            "path_coverage_status": coverage_status,
            "mfe": _round(mfe),
            "mae": _round(mae),
            "mfe_timestamp": mfe_ts,
            "mae_timestamp": mae_ts,
            "max_favorable_ticks": None,
            "max_adverse_ticks": None,
            "post_exit_forward_windows": retained.get("post_exit_forward_windows") if isinstance(retained.get("post_exit_forward_windows"), Mapping) else {},
            "counterfactual_ready": readiness,
            "provenance": _provenance(
                outcome=outcome,
                enrichment=enrichment,
                attribution=attribution,
                retained=retained,
                source_paths=source_paths or {},
            ),
            "diagnostic_only": True,
            "production_recommendation": False,
            "trading_gate": False,
        }
        row["deterministic_fingerprint"] = _fingerprint({k: v for k, v in row.items() if k not in {"generated_at", "deterministic_fingerprint"}})
        rows.append(row)
    rows.sort(key=lambda row: (str(row.get("exit_timestamp") or ""), str(row.get("canonical_trade_path_id") or "")))
    return rows


def build_canonical_trade_path_summary(
    rows: Sequence[Mapping[str, Any]],
    *,
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> dict[str, Any]:
    total = len(rows)
    with_paths = sum(1 for row in rows if row.get("path_sample_count", 0) > 0)
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
        "source_paths": {key: str(value) for key, value in (source_paths or {}).items()},
        "overall": {
            "canonical_trade_path_count": total,
            "path_available_count": with_paths,
            "path_available_rate": _rate(with_paths, total),
            "complete_path_count": sum(1 for row in rows if row.get("path_coverage_status") == "COMPLETE"),
            "partial_path_count": sum(1 for row in rows if row.get("path_coverage_status") == "PARTIAL"),
            "missing_source_count": sum(1 for row in rows if row.get("path_coverage_status") == "MISSING_SOURCE"),
            "mfe_available_count": sum(1 for row in rows if row.get("mfe") is not None),
            "mae_available_count": sum(1 for row in rows if row.get("mae") is not None),
            "timebox_ready_count": sum(1 for row in rows if row.get("counterfactual_ready", {}).get("timebox") is True),
            "trailing_ready_count": sum(1 for row in rows if row.get("counterfactual_ready", {}).get("trailing") is True),
            "vwap_avwap_ready_count": sum(1 for row in rows if row.get("counterfactual_ready", {}).get("vwap_avwap") is True),
            "atr_ready_count": sum(1 for row in rows if row.get("counterfactual_ready", {}).get("atr") is True),
        },
        "distributions": {
            "path_coverage_status": _counts(row.get("path_coverage_status") for row in rows),
            "path_source": _counts(row.get("path_source") for row in rows),
            "instrument": _counts(row.get("instrument") for row in rows),
            "counterfactual_timebox": _counts(row.get("counterfactual_ready", {}).get("timebox") for row in rows),
        },
        "provenance": {
            "ctol_link_count": sum(1 for row in rows if row.get("trade_outcome_id")),
            "ctoe_link_count": sum(1 for row in rows if row.get("trade_enrichment_id")),
            "ra3_link_count": sum(1 for row in rows if row.get("trade_decision_attribution_id")),
            "retained_capture_link_count": with_paths,
        },
    }


class _RetainedPathIndex:
    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        self._by_source_trade_id = {str(row.get("source_trade_id")): row for row in rows if row.get("source_trade_id")}
        self._by_outcome_id = {str(row.get("trade_outcome_id")): row for row in rows if row.get("trade_outcome_id")}

    def find(self, outcome: Mapping[str, Any]) -> Mapping[str, Any]:
        source_trade_id = _source_trade_id(outcome)
        if source_trade_id and source_trade_id in self._by_source_trade_id:
            return self._by_source_trade_id[source_trade_id]
        trade_outcome_id = str(outcome.get("trade_outcome_id") or "")
        if trade_outcome_id and trade_outcome_id in self._by_outcome_id:
            return self._by_outcome_id[trade_outcome_id]
        return {}


def _coverage_status(*, outcome: Mapping[str, Any], path: Sequence[Any], path_start: Any, path_end: Any) -> str:
    if not path:
        return "MISSING_SOURCE"
    entry = _parse_ts(outcome.get("entry_time"))
    exit_ts = _parse_ts(outcome.get("exit_time"))
    start = _parse_ts(path_start)
    end = _parse_ts(path_end)
    if entry is None or exit_ts is None or start is None or end is None:
        return "PARTIAL"
    if start <= entry and end >= exit_ts:
        return "COMPLETE"
    return "PARTIAL"


def _readiness(*, path: Sequence[Any], retained: Mapping[str, Any]) -> dict[str, bool]:
    has_path = bool(path)
    has_mfe_mae = retained.get("mfe_points") is not None and retained.get("mae_points") is not None
    windows = retained.get("post_exit_forward_windows") if isinstance(retained.get("post_exit_forward_windows"), Mapping) else {}
    has_forward = any(isinstance(window, Mapping) and window.get("available") is True for window in windows.values())
    return {
        "timebox": bool(has_path and has_forward),
        "trailing": bool(has_path and has_mfe_mae),
        "vwap_avwap": False,
        "atr": False,
    }


def _extreme(path: Sequence[Mapping[str, Any]], key: str, *, maximum: bool) -> tuple[float | None, str | None]:
    best_value: float | None = None
    best_ts: str | None = None
    for row in path:
        if not isinstance(row, Mapping):
            continue
        value = _float_or_none(row.get(key))
        if value is None:
            continue
        if best_value is None or (value > best_value if maximum else value < best_value):
            best_value = value
            best_ts = _str_or_none(row.get("bar_end"))
    return best_value, best_ts


def _provenance(
    *,
    outcome: Mapping[str, Any],
    enrichment: Mapping[str, Any],
    attribution: Mapping[str, Any],
    retained: Mapping[str, Any],
    source_paths: Mapping[str, Path | str],
) -> dict[str, Any]:
    return {
        "source_paths": {key: str(value) for key, value in source_paths.items()},
        "source_refs": {
            "source_trade_id": _source_trade_id(outcome) or retained.get("source_trade_id"),
            "trade_outcome_id": outcome.get("trade_outcome_id"),
            "trade_enrichment_id": enrichment.get("trade_outcome_id") if enrichment else None,
            "trade_decision_attribution_id": attribution.get("trade_decision_attribution_id") if attribution else None,
            "retained_path_capture_id": retained.get("retained_path_capture_id") if retained else None,
            "retained_path_fingerprint": retained.get("deterministic_fingerprint") if retained else None,
        },
        "evidence_present": {
            "ctol": bool(outcome),
            "ctoe": bool(enrichment),
            "ra3_attribution": bool(attribution),
            "retained_path_capture": bool(retained),
        },
    }


def render_contract_markdown() -> str:
    return """# RA7 Canonical Trade Path Contract

`CanonicalTradePath` is a first-class diagnostic research object for trade path
and excursion analysis.

It normalizes retained trade-path captures and emits explicit missing-path rows
for completed trades without retained path evidence.

Guardrails:

- diagnostic_only=true
- production_recommendation=false
- trading_gate=false
- no broker/runtime/strategy/Managed Exit authority
"""


def render_coverage_markdown(summary: Mapping[str, Any]) -> str:
    overall = summary.get("overall", {})
    return "\n".join(
        [
            "# RA7 Trade Path Coverage Report",
            "",
            f"- Canonical trade paths: `{overall.get('canonical_trade_path_count')}`",
            f"- Path available: `{overall.get('path_available_count')}`",
            f"- Path available rate: `{overall.get('path_available_rate')}`",
            f"- Complete paths: `{overall.get('complete_path_count')}`",
            f"- Partial paths: `{overall.get('partial_path_count')}`",
            f"- Missing source: `{overall.get('missing_source_count')}`",
            "",
            "## Coverage Status",
            "",
            _dict_table(summary.get("distributions", {}).get("path_coverage_status", {})),
        ]
    ) + "\n"


def render_provenance_markdown(summary: Mapping[str, Any]) -> str:
    provenance = summary.get("provenance", {})
    return "\n".join(
        [
            "# RA7 Trade Path Provenance Report",
            "",
            f"- CTOL links: `{provenance.get('ctol_link_count')}`",
            f"- CTOE links: `{provenance.get('ctoe_link_count')}`",
            f"- RA3 attribution links: `{provenance.get('ra3_link_count')}`",
            f"- Retained capture links: `{provenance.get('retained_capture_link_count')}`",
        ]
    ) + "\n"


def render_readiness_markdown(summary: Mapping[str, Any]) -> str:
    overall = summary.get("overall", {})
    return "\n".join(
        [
            "# RA7 Counterfactual Readiness Report",
            "",
            f"- Timebox ready: `{overall.get('timebox_ready_count')}`",
            f"- Trailing ready: `{overall.get('trailing_ready_count')}`",
            f"- VWAP/AVWAP ready: `{overall.get('vwap_avwap_ready_count')}`",
            f"- ATR ready: `{overall.get('atr_ready_count')}`",
            "",
            "Readiness is diagnostic only and is not a live exit recommendation.",
        ]
    ) + "\n"


def _schema() -> dict[str, Any]:
    return {
        "title": "CanonicalTradePath",
        "schema_version": SCHEMA_VERSION,
        "required": [
            "canonical_trade_path_id",
            "source_trade_id",
            "path_coverage_status",
            "counterfactual_ready",
            "provenance",
            "diagnostic_only",
            "production_recommendation",
            "trading_gate",
        ],
    }


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _parse_ts(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _source_trade_id(outcome: Mapping[str, Any]) -> str | None:
    refs = outcome.get("source_refs") if isinstance(outcome.get("source_refs"), Mapping) else {}
    return _str_or_none(refs.get("source_trade_id"))


def _str_or_none(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 6)


def _round(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 6)


def _counts(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value if value not in (None, "") else "UNKNOWN")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _dict_table(values: Mapping[str, Any]) -> str:
    if not values:
        return "No rows."
    lines = ["|key|count|", "|---|---:|"]
    for key, count in values.items():
        lines.append(f"|{key}|{count}|")
    return "\n".join(lines)


def _stable_id(prefix: str, *parts: Any) -> str:
    digest = hashlib.sha256("|".join(str(part or "") for part in parts).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}_{digest}"


def _fingerprint(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
