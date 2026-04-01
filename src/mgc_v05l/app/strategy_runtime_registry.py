"""Canonical standalone strategy runtime definitions and registry helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Sequence

from ..config_models import StrategySettings
from ..domain.events import DomainEvent
from ..domain.models import Bar
from ..execution.execution_engine import ExecutionEngine
from ..persistence import build_engine
from ..persistence.repositories import RepositorySet
from ..persistence.state_repository import StateRepository
from ..strategy.strategy_engine import StrategyEngine
from .approved_quant_lanes.engine import ApprovedQuantStrategyEngine
from .approved_quant_lanes.specs import approved_quant_lane_specs
from .shared_strategy_identities import get_shared_strategy_identity
from .strategy_identity import build_standalone_strategy_identity

_APPROVED_LONG_SOURCE_FIELDS = {
    "usLatePauseResumeLongTurn": "enable_us_late_pause_resume_longs",
    "asiaEarlyNormalBreakoutRetestHoldTurn": "enable_asia_early_normal_breakout_retest_hold_longs",
}

_APPROVED_SHORT_SOURCE_FIELDS = {
    "asiaEarlyPauseResumeShortTurn": "enable_asia_early_pause_resume_shorts",
}


@dataclass(frozen=True)
class StandaloneStrategyDefinition:
    standalone_strategy_id: str
    standalone_strategy_root: str
    standalone_strategy_label: str
    strategy_family: str
    instrument: str
    lane_id: str
    display_name: str
    config_source: str
    runtime_kind: str
    enabled: bool
    trade_size: int
    allowed_sessions: tuple[str, ...]
    long_sources: tuple[str, ...] = ()
    short_sources: tuple[str, ...] = ()
    database_url: str | None = None
    artifacts_dir: str | None = None
    point_value: Decimal | None = None
    legacy_derived_identity: bool = False

    @property
    def runtime_identity(self) -> dict[str, Any]:
        return {
            "standalone_strategy_id": self.standalone_strategy_id,
            "standalone_strategy_root": self.standalone_strategy_root,
            "standalone_strategy_label": self.standalone_strategy_label,
            "strategy_family": self.strategy_family,
            "instrument": self.instrument,
            "lane_id": self.lane_id,
            "display_name": self.display_name,
            "config_source": self.config_source,
            "legacy_derived_identity": self.legacy_derived_identity,
            "runtime_kind": self.runtime_kind,
        }


@dataclass
class StandaloneStrategyRuntimeInstance:
    definition: StandaloneStrategyDefinition
    settings: StrategySettings | None
    repositories: RepositorySet | None
    strategy_engine: StrategyEngine | None
    runtime_state_loaded: bool

    @property
    def runtime_instance_present(self) -> bool:
        return True

    @property
    def can_process_bars(self) -> bool:
        return self.strategy_engine is not None


class StrategyRuntimeRegistry:
    """Canonical registry of standalone strategy runtime instances."""

    def __init__(self, instances: Sequence[StandaloneStrategyRuntimeInstance]) -> None:
        self._instances = list(instances)
        self._instances_by_instrument: dict[str, list[StandaloneStrategyRuntimeInstance]] = {}
        self._instances_by_id: dict[str, StandaloneStrategyRuntimeInstance] = {}
        for instance in self._instances:
            instrument = instance.definition.instrument
            self._instances_by_instrument.setdefault(instrument, []).append(instance)
            self._instances_by_id[instance.definition.standalone_strategy_id] = instance

    @property
    def instances(self) -> tuple[StandaloneStrategyRuntimeInstance, ...]:
        return tuple(self._instances)

    @property
    def definitions(self) -> tuple[StandaloneStrategyDefinition, ...]:
        return tuple(instance.definition for instance in self._instances)

    def primary_engine_instance(self) -> StandaloneStrategyRuntimeInstance | None:
        for instance in self._instances:
            if instance.strategy_engine is not None:
                return instance
        return None

    def instances_for_instrument(self, instrument: str) -> tuple[StandaloneStrategyRuntimeInstance, ...]:
        normalized = str(instrument or "").strip().upper()
        return tuple(self._instances_by_instrument.get(normalized, ()))

    def process_bar(self, bar: Bar) -> dict[str, list[DomainEvent]]:
        routed: dict[str, list[DomainEvent]] = {}
        for instance in self.instances_for_instrument(bar.symbol):
            if instance.strategy_engine is None:
                continue
            routed[instance.definition.standalone_strategy_id] = instance.strategy_engine.process_bar(bar)
        return routed

    def summary_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for instance in self._instances:
            processed_bars = instance.repositories.processed_bars.count() if instance.repositories is not None else 0
            runtime_state_loaded = bool(instance.runtime_state_loaded)
            if not runtime_state_loaded and instance.strategy_engine is not None:
                runtime_state_loaded = bool(
                    instance.strategy_engine.state.last_signal_bar_id
                    or instance.strategy_engine.state.last_order_intent_id
                    or processed_bars > 0
                )
            rows.append(
                {
                    **instance.definition.runtime_identity,
                    "runtime_instance_present": True,
                    "runtime_state_loaded": runtime_state_loaded,
                    "can_process_bars": instance.can_process_bars,
                    "processed_bar_count": processed_bars,
                }
            )
        return rows


def build_strategy_runtime_registry(
    settings: StrategySettings,
    *,
    include_approved_quant_runtime_rows: bool = False,
) -> StrategyRuntimeRegistry:
    definitions = build_standalone_strategy_definitions(
        settings,
        include_approved_quant_runtime_rows=include_approved_quant_runtime_rows,
    )
    instances: list[StandaloneStrategyRuntimeInstance] = []
    for definition in definitions:
        if definition.runtime_kind not in {"strategy_engine", "approved_quant_strategy_engine"}:
            instances.append(
                StandaloneStrategyRuntimeInstance(
                    definition=definition,
                    settings=None,
                    repositories=None,
                    strategy_engine=None,
                    runtime_state_loaded=False,
                )
            )
            continue

        runtime_settings = build_runtime_settings(settings, definition)
        repositories = RepositorySet(build_engine(runtime_settings.database_url), runtime_identity=definition.runtime_identity)
        state_repository = StateRepository(repositories.engine, runtime_identity=definition.runtime_identity)
        persisted_state = state_repository.load_latest()
        loaded_state = _build_strategy_engine_instance(
            definition=definition,
            settings=runtime_settings,
            repositories=repositories,
            execution_engine=ExecutionEngine(),
        )
        instances.append(
            StandaloneStrategyRuntimeInstance(
                definition=definition,
                settings=runtime_settings,
                repositories=repositories,
                strategy_engine=loaded_state,
                runtime_state_loaded=persisted_state is not None,
            )
        )
    return StrategyRuntimeRegistry(instances)


def build_standalone_strategy_definitions(
    settings: StrategySettings,
    *,
    runtime_lanes: Sequence[dict[str, Any]] | None = None,
    include_approved_quant_runtime_rows: bool = False,
) -> tuple[StandaloneStrategyDefinition, ...]:
    definitions: list[StandaloneStrategyDefinition] = []
    raw_explicit = list(settings.standalone_strategy_definitions)
    if raw_explicit:
        definitions.extend(
            _coerce_runtime_definition_rows(
                settings,
                raw_explicit,
                config_source="standalone_strategy_definitions_json",
            )
        )
    else:
        raw_lane_rows = list(runtime_lanes or settings.probationary_paper_lane_specs)
        canary = settings.probationary_paper_execution_canary_spec
        if canary and not runtime_lanes:
            raw_lane_rows.append(canary)
        if raw_lane_rows:
            definitions.extend(
                _coerce_runtime_definition_rows(
                    settings,
                    raw_lane_rows,
                    config_source="probationary_paper_lanes_json" if runtime_lanes is None else "paper_config_in_force",
                )
            )
        else:
            definitions.append(_legacy_definition(settings))

    if include_approved_quant_runtime_rows:
        definitions.extend(_approved_quant_runtime_definitions(settings))

    seen: set[str] = set()
    unique: list[StandaloneStrategyDefinition] = []
    for definition in definitions:
        if definition.standalone_strategy_id in seen:
            continue
        seen.add(definition.standalone_strategy_id)
        unique.append(definition)
    return tuple(unique)


def build_runtime_settings(settings: StrategySettings, definition: StandaloneStrategyDefinition) -> StrategySettings:
    updates: dict[str, Any] = {
        "symbol": definition.instrument,
        "trade_size": definition.trade_size,
    }
    if definition.database_url:
        updates["database_url"] = definition.database_url
    if definition.artifacts_dir:
        updates["probationary_artifacts_dir"] = definition.artifacts_dir
    if definition.lane_id and not definition.legacy_derived_identity:
        updates["probationary_paper_lane_id"] = definition.lane_id
        updates["probationary_paper_lane_display_name"] = definition.display_name
        updates["probationary_paper_lane_session_restriction"] = (
            "/".join(definition.allowed_sessions) if definition.allowed_sessions else ""
        )
        updates["enable_us_late_pause_resume_longs"] = False
        updates["enable_asia_early_normal_breakout_retest_hold_longs"] = False
        updates["enable_asia_early_pause_resume_shorts"] = False
        updates["probationary_extra_approved_long_entry_sources_json"] = "[]"
        updates["probationary_extra_approved_short_entry_sources_json"] = "[]"
        updates["probationary_enforce_approved_branches"] = True
    extra_long_sources: list[str] = []
    extra_short_sources: list[str] = []
    for source in definition.long_sources:
        field_name = _APPROVED_LONG_SOURCE_FIELDS.get(source)
        if field_name:
            updates[field_name] = True
        else:
            extra_long_sources.append(source)
    for source in definition.short_sources:
        field_name = _APPROVED_SHORT_SOURCE_FIELDS.get(source)
        if field_name:
            updates[field_name] = True
        else:
            extra_short_sources.append(source)
    if extra_long_sources:
        updates["probationary_extra_approved_long_entry_sources_json"] = json.dumps(list(extra_long_sources))
    if extra_short_sources:
        updates["probationary_extra_approved_short_entry_sources_json"] = json.dumps(list(extra_short_sources))
    return settings.model_copy(update=updates)


def _coerce_runtime_definition_rows(
    settings: StrategySettings,
    raw_rows: Iterable[dict[str, Any]],
    *,
    config_source: str,
) -> list[StandaloneStrategyDefinition]:
    rows: list[StandaloneStrategyDefinition] = []
    for raw in raw_rows:
        normalized_raw = _normalize_runtime_definition_row(raw)
        instrument = str(normalized_raw.get("instrument") or normalized_raw.get("symbol") or settings.symbol).strip().upper()
        long_sources = tuple(str(value) for value in normalized_raw.get("long_sources", []) if value)
        short_sources = tuple(str(value) for value in normalized_raw.get("short_sources", []) if value)
        strategy_family = _resolve_strategy_family(normalized_raw, long_sources=long_sources, short_sources=short_sources)
        identity = build_standalone_strategy_identity(
            instrument=instrument,
            lane_id=normalized_raw.get("lane_id"),
            strategy_name=normalized_raw.get("display_name"),
            source_family=strategy_family,
            lane_name=normalized_raw.get("display_name") or normalized_raw.get("lane_name"),
            explicit_root=normalized_raw.get("strategy_identity_root"),
        )
        lane_id = str(normalized_raw.get("lane_id") or "").strip()
        rows.append(
            StandaloneStrategyDefinition(
                standalone_strategy_id=identity["standalone_strategy_id"],
                standalone_strategy_root=identity["standalone_strategy_root"],
                standalone_strategy_label=identity["standalone_strategy_label"],
                strategy_family=strategy_family,
                instrument=instrument,
                lane_id=lane_id,
                display_name=str(
                    normalized_raw.get("display_name")
                    or normalized_raw.get("lane_name")
                    or lane_id
                    or identity["standalone_strategy_label"]
                ),
                config_source=config_source,
                runtime_kind=str(normalized_raw.get("runtime_kind") or "strategy_engine"),
                enabled=bool(normalized_raw.get("enabled", True)),
                trade_size=int(normalized_raw.get("trade_size", settings.trade_size)),
                allowed_sessions=tuple(str(value) for value in normalized_raw.get("allowed_sessions", []) if value)
                or ((str(normalized_raw.get("session_restriction")),) if normalized_raw.get("session_restriction") else ()),
                long_sources=long_sources,
                short_sources=short_sources,
                database_url=str(
                    normalized_raw.get("database_url")
                    or _derive_runtime_database_url(settings.database_url, lane_id or identity["standalone_strategy_id"])
                ),
                artifacts_dir=str(
                    normalized_raw.get("artifacts_dir")
                    or (settings.probationary_artifacts_path / "lanes" / (lane_id or identity["standalone_strategy_id"]))
                ),
                point_value=Decimal(str(normalized_raw["point_value"])) if normalized_raw.get("point_value") is not None else None,
                legacy_derived_identity=False,
            )
        )
    return rows


def _normalize_runtime_definition_row(raw: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(raw)
    shared_identity_id = str(normalized.get("shared_strategy_identity") or "").strip()
    if not shared_identity_id:
        return normalized
    shared_identity = get_shared_strategy_identity(shared_identity_id)
    return {
        **shared_identity.runtime_row_defaults(),
        **normalized,
        "shared_strategy_identity": shared_identity.identity_id,
    }


def _approved_quant_runtime_definitions(settings: StrategySettings) -> list[StandaloneStrategyDefinition]:
    rows: list[StandaloneStrategyDefinition] = []
    for spec in approved_quant_lane_specs():
        for instrument in spec.symbols:
            identity = build_standalone_strategy_identity(
                instrument=instrument,
                lane_id=spec.lane_id,
                strategy_name=spec.lane_name,
                source_family=spec.family,
                lane_name=spec.lane_name,
            )
            rows.append(
                StandaloneStrategyDefinition(
                    standalone_strategy_id=identity["standalone_strategy_id"],
                    standalone_strategy_root=identity["standalone_strategy_root"],
                    standalone_strategy_label=identity["standalone_strategy_label"],
                    strategy_family=spec.family,
                    instrument=str(instrument),
                    lane_id=identity["standalone_strategy_id"],
                    display_name=f"{spec.lane_name} / {instrument}",
                    config_source="approved_quant_lane_specs",
                    runtime_kind="approved_quant_strategy_engine",
                    enabled=True,
                    trade_size=1,
                    allowed_sessions=tuple(spec.allowed_sessions),
                    long_sources=(spec.family,) if str(spec.direction).upper() == "LONG" else (),
                    short_sources=(spec.family,) if str(spec.direction).upper() == "SHORT" else (),
                    database_url=str(
                        _derive_runtime_database_url(settings.database_url, identity["standalone_strategy_id"])
                    ),
                    artifacts_dir=str(settings.probationary_artifacts_path / "lanes" / identity["standalone_strategy_id"]),
                    point_value=None,
                    legacy_derived_identity=False,
                )
            )
    return rows


def _build_strategy_engine_instance(
    *,
    definition: StandaloneStrategyDefinition,
    settings: StrategySettings,
    repositories: RepositorySet,
    execution_engine: ExecutionEngine,
) -> StrategyEngine:
    runtime_identity = definition.runtime_identity
    if definition.runtime_kind == "approved_quant_strategy_engine":
        quant_spec = next(
            spec
            for spec in approved_quant_lane_specs()
            if spec.family == definition.strategy_family and definition.instrument in spec.symbols
        )
        return ApprovedQuantStrategyEngine(
            quant_spec=quant_spec,
            settings=settings,
            repositories=repositories,
            execution_engine=execution_engine,
            runtime_identity=runtime_identity,
        )
    return StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=execution_engine,
        runtime_identity=runtime_identity,
    )


def _legacy_definition(settings: StrategySettings) -> StandaloneStrategyDefinition:
    identity = build_standalone_strategy_identity(
        instrument=settings.symbol,
        lane_id="legacy_runtime",
        strategy_name="legacy_runtime",
        source_family="LEGACY_RUNTIME",
        lane_name="legacy_runtime",
    )
    return StandaloneStrategyDefinition(
        standalone_strategy_id=identity["standalone_strategy_id"],
        standalone_strategy_root=identity["standalone_strategy_root"],
        standalone_strategy_label=identity["standalone_strategy_label"],
        strategy_family="LEGACY_RUNTIME",
        instrument=settings.symbol,
        lane_id="legacy_runtime",
        display_name=f"Legacy Runtime / {settings.symbol}",
        config_source="legacy_single_symbol_config",
        runtime_kind="strategy_engine",
        enabled=True,
        trade_size=settings.trade_size,
        allowed_sessions=(),
        database_url=settings.database_url,
        artifacts_dir=str(settings.probationary_artifacts_path),
        point_value=None,
        legacy_derived_identity=True,
    )


def _resolve_strategy_family(
    raw: dict[str, Any],
    *,
    long_sources: Sequence[str],
    short_sources: Sequence[str],
) -> str:
    for candidate in (
        raw.get("strategy_family"),
        raw.get("family"),
        raw.get("source_family"),
        long_sources[0] if long_sources else None,
        short_sources[0] if short_sources else None,
        raw.get("lane_name"),
        raw.get("display_name"),
        raw.get("lane_id"),
    ):
        text = str(candidate or "").strip()
        if text:
            return text
    return "UNKNOWN"


def _derive_runtime_database_url(database_url: str, runtime_key: str) -> str:
    if not database_url.startswith("sqlite:///"):
        return database_url
    raw_path = database_url.removeprefix("sqlite:///")
    path = Path(raw_path)
    if path.name == ":memory:":
        return database_url
    suffix = path.suffix or ".sqlite3"
    derived_path = path.with_name(f"{path.stem}__{runtime_key}{suffix}")
    return f"sqlite:///{derived_path}"
