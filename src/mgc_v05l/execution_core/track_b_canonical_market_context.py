"""Canonical market context providers for research and analytics."""

from __future__ import annotations

import csv
import json
from bisect import bisect_right
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol, Sequence

from mgc_v05l.execution_core.bounded_jsonl import BoundedJsonlConfig, write_bounded_jsonl
from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research" / "canonical_market_context"
DEFAULT_WAREHOUSE_ROOT = Path("outputs") / "research_platform" / "warehouse" / "historical_evaluator"

CONTEXT_JSONL = "canonical_market_context.jsonl"
SUMMARY_JSON = "latest_canonical_market_context_summary.json"
SUMMARY_MD = "latest_canonical_market_context_summary.md"
CONTRACT_MD = "canonical_market_context_contract.md"
DATA_QUALITY_MD = "canonical_market_context_data_quality.md"
CAPABILITY_MATRIX_MD = "market_context_provider_capability_matrix.md"
HISTORICAL_PROVIDER_REPORT_MD = "historical_market_context_provider_report.md"
VIX_HISTORICAL_DATA_QUALITY_MD = "vix_historical_provider_data_quality.md"

SCHEMA_VERSION = "track_b_canonical_market_context_v1"
SUMMARY_SCHEMA_VERSION = "track_b_canonical_market_context_summary_v1"


class MarketContextProvider(Protocol):
    provider_name: str
    context_key: str

    def load_observations(self) -> list[dict[str, Any]]:
        ...

    def context_rows(self, *, generated_at: datetime) -> list[dict[str, Any]]:
        ...

    def coverage_report(self) -> dict[str, Any]:
        ...

    def freshness_report(self, *, generated_at: datetime) -> dict[str, Any]:
        ...

    def join_at(self, timestamp: datetime) -> dict[str, Any]:
        ...

    def capability_report(self) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class CanonicalMarketContextResult:
    rows: list[dict[str, Any]]
    summary: dict[str, Any]
    context_path: Path
    summary_path: Path
    summary_markdown_path: Path
    contract_path: Path
    data_quality_path: Path
    capability_matrix_path: Path
    historical_provider_report_path: Path
    vix_historical_data_quality_path: Path


class VixProvider:
    provider_name = "VIX"
    context_key = "vix"
    symbol = "VIX"
    provider_kind = "historical_vix"

    def __init__(
        self,
        *,
        warehouse_root: Path = DEFAULT_WAREHOUSE_ROOT,
        vix_source_path: Path | None = None,
        source_kind: str | None = None,
    ) -> None:
        self.warehouse_root = Path(warehouse_root)
        self.vix_source_path = Path(vix_source_path) if vix_source_path else None
        self.source_kind = source_kind
        self._observations: list[dict[str, Any]] | None = None

    @property
    def source_ref(self) -> str:
        path = self._selected_source_path()
        return str(path) if path else str(self.warehouse_root / "datasets" / "vol_regime_daily")

    def load_observations(self) -> list[dict[str, Any]]:
        if self._observations is not None:
            return list(self._observations)
        rows = self._load_source_rows()
        observations: list[dict[str, Any]] = []
        for raw in rows:
            normalized = _normalize_vix_row(raw, source_ref=self.source_ref)
            if normalized is not None:
                observations.append(normalized)
        observations.sort(key=lambda row: _parse_datetime(row["vix_observation_time"]) or datetime.min.replace(tzinfo=UTC))
        observations = _add_vix_derived_fields(observations)
        self._observations = observations
        return list(observations)

    def context_rows(self, *, generated_at: datetime) -> list[dict[str, Any]]:
        observations = self.load_observations()
        if not observations:
            return [self._unavailable_row(generated_at=generated_at)]
        return [
            {
                "schema_version": SCHEMA_VERSION,
                "generated_at": generated_at.isoformat(),
                "provider_name": self.provider_name,
                "provider_kind": self.provider_kind,
                "context_key": self.context_key,
                "symbol": self.symbol,
                "vix_available": True,
                "vix_level": row["vix_level"],
                "vix_observation_time": row["vix_observation_time"],
                "vix_staleness_seconds": _age_seconds(row["vix_observation_time"], generated_at),
                "vix_source": row["vix_source"],
                "vix_unavailable_reason": None,
                "vix_daily_change": row.get("vix_daily_change"),
                "vix_regime": row.get("vix_regime"),
                "vix_percentile": row.get("vix_percentile"),
                "vix_ma_20": row.get("vix_ma_20"),
                "vix_ma_50": row.get("vix_ma_50"),
                "data_quality_flags": list(row.get("data_quality_flags") or []),
                "source_refs": row.get("source_refs") or {},
                "source_provenance": row.get("source_provenance") or {},
                "diagnostic_only": True,
                "production_effect": False,
            }
            for row in observations
        ]

    def coverage_report(self) -> dict[str, Any]:
        observations = self.load_observations()
        if not observations:
            return {
                "provider_name": self.provider_name,
                "provider_kind": self.provider_kind,
                "context_key": self.context_key,
                "available": False,
                "row_count": 0,
                "earliest_observation_time": None,
                "latest_observation_time": None,
                "source_ref": self.source_ref,
                "source_candidates": self.source_audit(),
                "missing_data_requirement": "Provide VIX daily/regime rows in warehouse vol_regime_daily, vix_daily, or an explicit JSONL/CSV source.",
            }
        return {
            "provider_name": self.provider_name,
            "provider_kind": self.provider_kind,
            "context_key": self.context_key,
            "available": True,
            "row_count": len(observations),
            "earliest_observation_time": observations[0]["vix_observation_time"],
            "latest_observation_time": observations[-1]["vix_observation_time"],
            "source_ref": self.source_ref,
            "source_candidates": self.source_audit(),
            "missing_data_requirement": None,
        }

    def freshness_report(self, *, generated_at: datetime) -> dict[str, Any]:
        latest = self.latest_observation()
        staleness = _age_seconds(latest.get("vix_observation_time") if latest else None, generated_at)
        return {
            "provider_name": self.provider_name,
            "provider_kind": self.provider_kind,
            "context_key": self.context_key,
            "freshness_available": latest is not None,
            "latest_observation_time": latest.get("vix_observation_time") if latest else None,
            "staleness_seconds": staleness,
            "freshness_status": _freshness_status(staleness),
        }

    def latest_observation(self) -> dict[str, Any] | None:
        observations = self.load_observations()
        return dict(observations[-1]) if observations else None

    def join_at(self, timestamp: datetime) -> dict[str, Any]:
        target = timestamp.astimezone(UTC)
        observations = self.load_observations()
        times = [_parse_datetime(row["vix_observation_time"]) for row in observations]
        valid_times = [item for item in times if item is not None]
        index = bisect_right(valid_times, target) - 1
        if index < 0:
            return {
                "provider_name": self.provider_name,
                "provider_kind": self.provider_kind,
                "context_key": self.context_key,
                "symbol": self.symbol,
                "vix_available": False,
                "vix_level": None,
                "vix_observation_time": None,
                "vix_staleness_seconds": None,
                "vix_source": self.source_ref,
                "vix_unavailable_reason": "no_prior_vix_observation",
                "data_quality_flags": ["no_prior_vix_observation"],
                "source_refs": {"vix_source": self.source_ref},
                "diagnostic_only": True,
                "production_effect": False,
            }
        matched = observations[index]
        return {
            "provider_name": self.provider_name,
            "provider_kind": self.provider_kind,
            "context_key": self.context_key,
            "symbol": self.symbol,
            "vix_available": True,
            "vix_level": matched["vix_level"],
            "vix_observation_time": matched["vix_observation_time"],
            "vix_staleness_seconds": _age_seconds(matched["vix_observation_time"], target),
            "vix_source": matched["vix_source"],
            "vix_unavailable_reason": None,
            "vix_daily_change": matched.get("vix_daily_change"),
            "vix_regime": matched.get("vix_regime"),
            "vix_percentile": matched.get("vix_percentile"),
            "vix_ma_20": matched.get("vix_ma_20"),
            "vix_ma_50": matched.get("vix_ma_50"),
            "data_quality_flags": list(matched.get("data_quality_flags") or []),
            "source_refs": matched.get("source_refs") or {},
            "source_provenance": matched.get("source_provenance") or {},
            "diagnostic_only": True,
            "production_effect": False,
        }

    def capability_report(self) -> dict[str, Any]:
        coverage = self.coverage_report()
        return {
            "provider_name": self.provider_name,
            "provider_kind": self.provider_kind,
            "context_key": self.context_key,
            "symbol": self.symbol,
            "implemented": True,
            "source_ref": self.source_ref,
            "available": coverage["available"],
            "row_count": coverage["row_count"],
            "join_method": "nearest_prior_observation_lte_timestamp",
            "fields": [
                "vix_level",
                "vix_available",
                "vix_observation_time",
                "vix_staleness_seconds",
                "vix_source",
                "vix_unavailable_reason",
                "vix_daily_change",
                "vix_regime",
                "vix_percentile",
                "vix_ma_20",
                "vix_ma_50",
            ],
            "diagnostic_only": True,
        }

    def source_audit(self) -> list[dict[str, Any]]:
        return audit_historical_vix_sources(
            warehouse_root=self.warehouse_root,
            explicit_source_path=self.vix_source_path,
        )

    def _load_source_rows(self) -> list[dict[str, Any]]:
        selected = self._selected_source_path()
        if selected is not None:
            return _read_rows_from_path(selected)
        return []

    def _resolved_source_path(self) -> Path | None:
        if self.vix_source_path is not None:
            return self.vix_source_path
        return None

    def _selected_source_path(self) -> Path | None:
        explicit = self._resolved_source_path()
        if explicit is not None:
            return explicit
        for candidate in self._default_source_candidates():
            if candidate.exists():
                return candidate
        return None

    def _default_source_candidates(self) -> tuple[Path, ...]:
        return (
            self.warehouse_root / "datasets" / "vol_regime_daily",
            self.warehouse_root / "datasets" / "vix_daily",
        )

    def _unavailable_row(self, *, generated_at: datetime) -> dict[str, Any]:
        reason = "missing_vix_source_data"
        return {
            "schema_version": SCHEMA_VERSION,
            "generated_at": generated_at.isoformat(),
            "provider_name": self.provider_name,
            "provider_kind": self.provider_kind,
            "context_key": self.context_key,
            "symbol": self.symbol,
            "vix_available": False,
            "vix_level": None,
            "vix_observation_time": None,
            "vix_staleness_seconds": None,
            "vix_source": self.source_ref,
            "vix_unavailable_reason": reason,
            "vix_daily_change": None,
            "vix_regime": None,
            "vix_percentile": None,
            "vix_ma_20": None,
            "vix_ma_50": None,
            "data_quality_flags": [reason],
            "source_refs": {"vix_source": self.source_ref},
            "source_provenance": {
                "provider_kind": self.provider_kind,
                "source_ref": self.source_ref,
                "source_audit": self.source_audit(),
            },
            "diagnostic_only": True,
            "production_effect": False,
        }


class HistoricalVixProvider(VixProvider):
    """Historical/read-only VIX context provider backed by warehouse or flat files."""


def run_canonical_market_context(
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    warehouse_root: Path = DEFAULT_WAREHOUSE_ROOT,
    vix_source_path: Path | None = None,
    providers: Sequence[MarketContextProvider] | None = None,
    now: datetime | str | None = None,
    jsonl_config: BoundedJsonlConfig | None = None,
    snapshot_config: BoundedSnapshotConfig | None = None,
) -> CanonicalMarketContextResult:
    generated_at = _coerce_now(now)
    actual_providers: Sequence[MarketContextProvider] = providers
    if actual_providers is None:
        actual_providers = [
            HistoricalVixProvider(
                warehouse_root=warehouse_root,
                vix_source_path=vix_source_path,
            )
        ]
    rows = build_canonical_market_context_rows(actual_providers, generated_at=generated_at)
    summary = build_canonical_market_context_summary(actual_providers, rows=rows, generated_at=generated_at)
    output_dir.mkdir(parents=True, exist_ok=True)
    context_path = output_dir / CONTEXT_JSONL
    write_bounded_jsonl(context_path, rows, config=jsonl_config)
    summary_path = output_dir / SUMMARY_JSON
    write_bounded_snapshot_json(summary_path, summary, config=snapshot_config or BoundedSnapshotConfig())
    summary_markdown_path = output_dir / SUMMARY_MD
    summary_markdown_path.write_text(render_summary_markdown(summary), encoding="utf-8")
    contract_path = output_dir / CONTRACT_MD
    contract_path.write_text(render_contract_markdown(), encoding="utf-8")
    data_quality_path = output_dir / DATA_QUALITY_MD
    data_quality_path.write_text(render_data_quality_markdown(summary), encoding="utf-8")
    capability_matrix_path = output_dir / CAPABILITY_MATRIX_MD
    capability_matrix_path.write_text(render_capability_matrix_markdown(summary), encoding="utf-8")
    historical_provider_report_path = output_dir / HISTORICAL_PROVIDER_REPORT_MD
    historical_provider_report_path.write_text(render_historical_provider_report_markdown(summary), encoding="utf-8")
    vix_historical_data_quality_path = output_dir / VIX_HISTORICAL_DATA_QUALITY_MD
    vix_historical_data_quality_path.write_text(render_vix_historical_data_quality_markdown(summary), encoding="utf-8")
    return CanonicalMarketContextResult(
        rows=rows,
        summary=summary,
        context_path=context_path,
        summary_path=summary_path,
        summary_markdown_path=summary_markdown_path,
        contract_path=contract_path,
        data_quality_path=data_quality_path,
        capability_matrix_path=capability_matrix_path,
        historical_provider_report_path=historical_provider_report_path,
        vix_historical_data_quality_path=vix_historical_data_quality_path,
    )


def build_canonical_market_context_rows(
    providers: Sequence[MarketContextProvider],
    *,
    generated_at: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for provider in providers:
        rows.extend(provider.context_rows(generated_at=generated_at))
    return rows


def build_canonical_market_context_summary(
    providers: Sequence[MarketContextProvider],
    *,
    rows: Sequence[Mapping[str, Any]],
    generated_at: datetime,
) -> dict[str, Any]:
    coverage = [provider.coverage_report() for provider in providers]
    freshness = [provider.freshness_report(generated_at=generated_at) for provider in providers]
    capabilities = [provider.capability_report() for provider in providers]
    vix_coverage = next((item for item in coverage if item.get("context_key") == "vix"), {})
    vix_freshness = next((item for item in freshness if item.get("context_key") == "vix"), {})
    missing_requirements = [
        item["missing_data_requirement"]
        for item in coverage
        if item.get("missing_data_requirement")
    ]
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "research_analytics_only": True,
        "diagnostic_only": True,
        "production_effect": False,
        "provider_count": len(providers),
        "row_count": len(rows),
        "providers": {
            "coverage": coverage,
            "freshness": freshness,
            "capabilities": capabilities,
        },
        "vix": {
            "available": bool(vix_coverage.get("available")),
            "row_count": int(vix_coverage.get("row_count") or 0),
            "coverage_window": {
                "start": vix_coverage.get("earliest_observation_time"),
                "end": vix_coverage.get("latest_observation_time"),
            },
            "freshness": vix_freshness,
            "join_readiness": "READY" if vix_coverage.get("available") else "MISSING_SOURCE_DATA",
            "source_ref": vix_coverage.get("source_ref"),
        },
        "missing_data_requirements": missing_requirements,
        "data_quality_flags": _summary_flags(rows=rows, coverage=coverage),
        "recommended_next_providers": [
            "SPX",
            "NDX",
            "DXY",
            "US10Y",
            "MOVE",
        ],
    }


def audit_historical_vix_sources(
    *,
    warehouse_root: Path = DEFAULT_WAREHOUSE_ROOT,
    explicit_source_path: Path | None = None,
) -> list[dict[str, Any]]:
    candidates: list[tuple[str, Path]] = []
    if explicit_source_path is not None:
        candidates.append(("explicit_source", Path(explicit_source_path)))
    candidates.extend(
        [
            ("warehouse_vol_regime_daily", Path(warehouse_root) / "datasets" / "vol_regime_daily"),
            ("warehouse_vix_daily", Path(warehouse_root) / "datasets" / "vix_daily"),
        ]
    )
    return [
        {
            "source_name": name,
            "path": str(path),
            "exists": path.exists(),
            "file_count": _count_supported_source_files(path),
            "supported": _source_path_supported(path),
            "preferred_order": index,
        }
        for index, (name, path) in enumerate(candidates, start=1)
    ]


def classify_vix_regime(level: float | None) -> str | None:
    if level is None:
        return None
    if level < 15.0:
        return "LOW"
    if level < 20.0:
        return "NORMAL"
    if level < 30.0:
        return "ELEVATED"
    return "EXTREME"


def _canonical_vix_regime(candidate: Any, level: float | None) -> str | None:
    text = str(candidate or "").strip().upper()
    if text in {"LOW", "NORMAL", "ELEVATED", "EXTREME"}:
        return text
    return classify_vix_regime(level)


def render_summary_markdown(summary: Mapping[str, Any]) -> str:
    vix = summary.get("vix") or {}
    coverage = vix.get("coverage_window") or {}
    lines = [
        "# Canonical Market Context Summary",
        "",
        f"- Generated at: `{summary.get('generated_at')}`",
        f"- Provider count: `{summary.get('provider_count')}`",
        f"- Row count: `{summary.get('row_count')}`",
        f"- VIX available: `{vix.get('available')}`",
        f"- VIX rows: `{vix.get('row_count')}`",
        f"- VIX coverage: `{coverage.get('start')}` to `{coverage.get('end')}`",
        f"- VIX join readiness: `{vix.get('join_readiness')}`",
        "",
        "## Data Quality",
    ]
    flags = summary.get("data_quality_flags") or []
    if flags:
        lines.extend(f"- `{flag}`" for flag in flags)
    else:
        lines.append("- No data-quality flags.")
    missing = summary.get("missing_data_requirements") or []
    if missing:
        lines.append("")
        lines.append("## Missing Data Requirements")
        lines.extend(f"- {item}" for item in missing)
    return "\n".join(lines) + "\n"


def render_contract_markdown() -> str:
    return "\n".join(
        [
            "# Canonical Market Context Contract",
            "",
            "Canonical Market Context is a diagnostic research/analytics layer.",
            "",
            "Providers supply timestamped market observations only. They must not know about GRE, CRFD, strategies, broker state, or trade gating.",
            "",
            "## Provider Responsibilities",
            "",
            "- provider name and context key",
            "- timestamped observations",
            "- coverage and freshness reports",
            "- nearest-prior timestamp join",
            "- data-quality flags and source refs",
            "",
            "## VIX Provider Fields",
            "",
            "- `vix_level`",
            "- `vix_available`",
            "- `vix_observation_time`",
            "- `vix_staleness_seconds`",
            "- `vix_source`",
            "- `vix_unavailable_reason`",
            "- `vix_daily_change`",
            "- `vix_regime`",
            "- `vix_percentile`",
            "- `vix_ma_20`",
            "- `vix_ma_50`",
            "",
            "All rows are `diagnostic_only=true` and `production_effect=false`.",
            "",
        ]
    )


def render_data_quality_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# Canonical Market Context Data Quality",
        "",
        "## Flags",
    ]
    flags = summary.get("data_quality_flags") or []
    if flags:
        lines.extend(f"- `{flag}`" for flag in flags)
    else:
        lines.append("- No data-quality flags.")
    lines.extend(["", "## Missing Requirements"])
    missing = summary.get("missing_data_requirements") or []
    if missing:
        lines.extend(f"- {item}" for item in missing)
    else:
        lines.append("- None.")
    return "\n".join(lines) + "\n"


def render_capability_matrix_markdown(summary: Mapping[str, Any]) -> str:
    capabilities = ((summary.get("providers") or {}).get("capabilities") or [])
    lines = [
        "# Market Context Provider Capability Matrix",
        "",
        "| Provider | Context | Available | Join Method | Rows |",
        "| --- | --- | --- | --- | --- |",
    ]
    coverage_by_key = {
        item.get("context_key"): item
        for item in ((summary.get("providers") or {}).get("coverage") or [])
    }
    for item in capabilities:
        coverage = coverage_by_key.get(item.get("context_key")) or {}
        lines.append(
            "| {provider} | {context} | {available} | {join_method} | {rows} |".format(
                provider=item.get("provider_name"),
                context=item.get("context_key"),
                available=item.get("available"),
                join_method=item.get("join_method"),
                rows=coverage.get("row_count", 0),
            )
        )
    if not capabilities:
        lines.append("| none | none | false | none | 0 |")
    return "\n".join(lines) + "\n"


def render_historical_provider_report_markdown(summary: Mapping[str, Any]) -> str:
    vix = summary.get("vix") or {}
    coverage = vix.get("coverage_window") or {}
    provider_coverage = ((summary.get("providers") or {}).get("coverage") or [])
    vix_provider = next((item for item in provider_coverage if item.get("context_key") == "vix"), {})
    lines = [
        "# Historical Market Context Provider Report",
        "",
        f"- Generated at: `{summary.get('generated_at')}`",
        "- Provider: `HistoricalVixProvider`",
        "- Context key: `vix`",
        f"- Available: `{vix.get('available')}`",
        f"- Row count: `{vix.get('row_count')}`",
        f"- Coverage start: `{coverage.get('start')}`",
        f"- Coverage end: `{coverage.get('end')}`",
        f"- Source ref: `{vix.get('source_ref')}`",
        "- Join method: `nearest_prior_observation_lte_timestamp`",
        "",
        "## Source Audit",
    ]
    candidates = vix_provider.get("source_candidates") or []
    if candidates:
        lines.extend(
            f"- `{row.get('source_name')}` exists=`{row.get('exists')}` files=`{row.get('file_count')}` path=`{row.get('path')}`"
            for row in candidates
        )
    else:
        lines.append("- No source candidates reported.")
    lines.extend(
        [
            "",
            "## Contract",
            "",
            "Historical VIX rows are read-only, diagnostic-only market context. They are not trading gates and are not integrated into trade outcome enrichment or GRE in M3.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_vix_historical_data_quality_markdown(summary: Mapping[str, Any]) -> str:
    vix = summary.get("vix") or {}
    freshness = vix.get("freshness") or {}
    flags = summary.get("data_quality_flags") or []
    missing = summary.get("missing_data_requirements") or []
    lines = [
        "# VIX Historical Provider Data Quality",
        "",
        f"- VIX available: `{vix.get('available')}`",
        f"- VIX row count: `{vix.get('row_count')}`",
        f"- Freshness status: `{freshness.get('freshness_status')}`",
        f"- Latest observation: `{freshness.get('latest_observation_time')}`",
        f"- Staleness seconds: `{freshness.get('staleness_seconds')}`",
        "",
        "## Data Quality Flags",
    ]
    if flags:
        lines.extend(f"- `{flag}`" for flag in flags)
    else:
        lines.append("- No data-quality flags.")
    lines.append("")
    lines.append("## Missing Data Requirements")
    if missing:
        lines.extend(f"- {item}" for item in missing)
    else:
        lines.append("- None.")
    return "\n".join(lines) + "\n"


def _read_rows_from_path(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    if path.is_dir():
        parquet_files = sorted(item for item in path.rglob("*.parquet") if item.name != "_schema.parquet")
        if parquet_files:
            return _read_parquet_rows(parquet_files)
        jsonl_files = sorted(path.rglob("*.jsonl"))
        if jsonl_files:
            rows: list[dict[str, Any]] = []
            for item in jsonl_files:
                rows.extend(_read_jsonl(item))
            return rows
        csv_files = sorted(path.rglob("*.csv"))
        if csv_files:
            rows = []
            for item in csv_files:
                rows.extend(_read_csv(item))
            return rows
        return []
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        return _read_jsonl(path)
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [dict(item) for item in payload if isinstance(item, Mapping)]
        if isinstance(payload, Mapping):
            rows = payload.get("rows") or payload.get("observations")
            if isinstance(rows, list):
                return [dict(item) for item in rows if isinstance(item, Mapping)]
            return [dict(payload)]
    if suffix == ".csv":
        return _read_csv(path)
    if suffix == ".parquet":
        return _read_parquet_rows([path])
    return []


def _source_path_supported(path: Path) -> bool:
    if path.is_dir():
        return _count_supported_source_files(path) > 0
    return path.suffix.lower() in {".json", ".jsonl", ".csv", ".parquet"} and path.exists()


def _count_supported_source_files(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return 1 if path.suffix.lower() in {".json", ".jsonl", ".csv", ".parquet"} else 0
    count = 0
    for suffix in ("*.parquet", "*.jsonl", "*.json", "*.csv"):
        count += sum(1 for item in path.rglob(suffix) if item.name != "_schema.parquet")
    return count


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            payload = json.loads(text)
            if isinstance(payload, Mapping):
                rows.append(dict(payload))
    return rows


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_parquet_rows(paths: Sequence[Path]) -> list[dict[str, Any]]:
    try:
        import pyarrow.dataset as pyarrow_dataset  # type: ignore
    except ModuleNotFoundError:
        return []
    dataset = pyarrow_dataset.dataset([str(path) for path in paths], format="parquet")
    return list(dataset.to_table().to_pylist())


def _normalize_vix_row(row: Mapping[str, Any], *, source_ref: str) -> dict[str, Any] | None:
    observation_time = _first_non_null(
        row.get("vix_asof_ts"),
        row.get("vix_observation_time"),
        row.get("observation_time"),
        row.get("timestamp"),
        row.get("generated_at"),
    )
    if observation_time is None and row.get("vix_trade_date"):
        observation_time = f"{row['vix_trade_date']}T21:15:00+00:00"
    parsed_time = _parse_datetime(observation_time)
    level = _parse_float(
        _first_non_null(
            row.get("vix_level"),
            row.get("vix_close"),
            row.get("close"),
            row.get("value"),
        )
    )
    if parsed_time is None or level is None:
        return None
    return {
        "vix_level": level,
        "vix_observation_time": parsed_time.astimezone(UTC).isoformat(),
        "vix_source": str(row.get("vix_source") or source_ref),
        "vix_daily_change": _parse_float(_first_non_null(row.get("vix_daily_change"), row.get("vix_change_abs"))),
        "vix_regime": _canonical_vix_regime(row.get("vix_regime"), level),
        "vix_percentile": _parse_float(row.get("vix_percentile")),
        "vix_ma_20": _parse_float(row.get("vix_ma_20")),
        "vix_ma_50": _parse_float(row.get("vix_ma_50")),
        "data_quality_flags": [],
        "source_refs": {"vix_source": source_ref},
        "source_provenance": {
            "source_ref": source_ref,
            "source_type": _source_type_from_ref(source_ref),
            "raw_fields_present": sorted(str(key) for key in row.keys()),
        },
    }


def _add_vix_derived_fields(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    levels: list[float] = []
    for raw in rows:
        row = dict(raw)
        level = float(row["vix_level"])
        prior_level = levels[-1] if levels else None
        if row.get("vix_daily_change") is None and prior_level is not None:
            row["vix_daily_change"] = round(level - prior_level, 6)
        row["vix_regime"] = _canonical_vix_regime(row.get("vix_regime"), level)
        levels.append(level)
        if row.get("vix_ma_20") is None and len(levels) >= 20:
            row["vix_ma_20"] = round(sum(levels[-20:]) / 20, 6)
        if row.get("vix_ma_50") is None and len(levels) >= 50:
            row["vix_ma_50"] = round(sum(levels[-50:]) / 50, 6)
        if row.get("vix_percentile") is None and len(levels) >= 20:
            less_or_equal = sum(1 for item in levels if item <= level)
            row["vix_percentile"] = round(less_or_equal / len(levels), 6)
        output.append(row)
    return output


def _summary_flags(*, rows: Sequence[Mapping[str, Any]], coverage: Sequence[Mapping[str, Any]]) -> list[str]:
    flags = {flag for row in rows for flag in row.get("data_quality_flags", [])}
    for item in coverage:
        if not item.get("available"):
            flags.add(f"{item.get('context_key')}_missing_source_data")
    return sorted(flags)


def _source_type_from_ref(source_ref: str) -> str:
    lowered = source_ref.lower()
    if "vol_regime_daily" in lowered:
        return "cboe_vix_regime_warehouse"
    if "vix_daily" in lowered:
        return "cboe_vix_daily_warehouse"
    if lowered.endswith(".jsonl"):
        return "jsonl"
    if lowered.endswith(".csv"):
        return "csv"
    if lowered.endswith(".parquet") or "parquet" in lowered:
        return "parquet"
    return "unknown"


def _freshness_status(staleness: int | None) -> str:
    if staleness is None:
        return "UNAVAILABLE"
    if staleness <= 3 * 24 * 60 * 60:
        return "FRESH"
    if staleness <= 14 * 24 * 60 * 60:
        return "STALE_WARN"
    return "STALE"


def _age_seconds(source_ts: Any, target_ts: datetime) -> int | None:
    source = _parse_datetime(source_ts)
    if source is None:
        return None
    return int((target_ts.astimezone(UTC) - source.astimezone(UTC)).total_seconds())


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    parsed = _parse_datetime(value)
    if parsed is None:
        raise ValueError(f"Unsupported timestamp: {value!r}")
    return parsed.astimezone(UTC)


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _parse_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_non_null(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None
