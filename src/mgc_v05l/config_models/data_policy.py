"""Typed data acquisition and storage policy models."""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class DataDomainPolicy(BaseModel):
    """Policy metadata for one storage domain."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    owner: str
    purpose: str
    includes: tuple[str, ...]
    rules: tuple[str, ...]
    sqlite_tables: tuple[str, ...] = ()
    artifact_paths: tuple[str, ...] = ()
    acquisition_mode: str
    refresh_interval_seconds: int | None = None
    service_cache_ttl_seconds: int | None = None
    ui_rebuild_interval_seconds: int | None = None
    stale_after_seconds: int | None = None
    retention_class: str
    retention_days: int | None = None
    cleanup_policy: str


class TrackedSymbolCategoryPolicy(BaseModel):
    """Refresh and lifecycle policy for one tracked-symbol category."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    storage_domain: str
    refresh_policy: str
    refresh_interval_seconds: int | None = None
    promotion_rule: str
    symbol_discovery: str = "explicit_symbols"
    symbols: tuple[str, ...] = ()
    include_in_daily_research_capture: bool = False
    research_capture_timeframes: tuple[str, ...] = ()
    bootstrap_lookback_days: int | None = None
    expiry_hours: int | None = None
    retention_days: int | None = None


class StorageLayoutPolicy(BaseModel):
    """Resolved storage roots and files for each purpose-specific domain."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    runtime_base_database_path: str
    runtime_replay_database_path: str
    runtime_paper_database_path: str
    broker_monitor_database_path: str
    broker_monitor_selected_account_path: str
    broker_monitor_snapshot_path: str
    ui_snapshot_root: str
    research_root: str
    token_file_path: str


class CleanupPolicy(BaseModel):
    """Repo-wide cleanup and rolloff defaults."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    ui_snapshot_cache_keep_days: int
    broker_monitor_quote_snapshot_keep_days: int
    broker_monitor_order_event_keep_days: int
    broker_monitor_reconciliation_event_keep_days: int
    runtime_event_keep_days: int
    ad_hoc_symbol_expiry_hours: int


class DataStoragePolicy(BaseModel):
    """Formal project policy for data acquisition, storage, and truth separation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    version: int
    storage_layout: StorageLayoutPolicy
    domains: dict[str, DataDomainPolicy]
    tracked_symbols: dict[str, TrackedSymbolCategoryPolicy]
    truth_hierarchy: dict[str, tuple[str, ...]]
    cleanup: CleanupPolicy
    repo_root: Path | None = Field(default=None, exclude=True)
    config_path: Path | None = Field(default=None, exclude=True)

    def resolve_path(self, raw_path: str) -> Path:
        path = Path(raw_path).expanduser()
        if path.is_absolute() or self.repo_root is None:
            return path.resolve(strict=False)
        return (self.repo_root / path).resolve(strict=False)

    @property
    def broker_monitor_database_path(self) -> Path:
        return self.resolve_path(self.storage_layout.broker_monitor_database_path)

    @property
    def broker_monitor_selected_account_path(self) -> Path:
        return self.resolve_path(self.storage_layout.broker_monitor_selected_account_path)

    @property
    def broker_monitor_snapshot_path(self) -> Path:
        return self.resolve_path(self.storage_layout.broker_monitor_snapshot_path)


def load_data_storage_policy(repo_root: Path, config_path: str | Path | None = None) -> DataStoragePolicy:
    """Load the formal storage policy from the default project config or an override."""

    requested_path = config_path if config_path is not None else repo_root / "config" / "data_storage_policy.json"
    resolved_config_path = _resolve_path(repo_root, requested_path)
    if not resolved_config_path.exists():
        bundled_default = Path(__file__).resolve().parents[3] / "config" / "data_storage_policy.json"
        resolved_config_path = bundled_default.resolve(strict=False)
    payload = json.loads(resolved_config_path.read_text(encoding="utf-8"))
    policy = DataStoragePolicy.model_validate(payload)
    return policy.model_copy(update={"repo_root": repo_root.resolve(strict=False), "config_path": resolved_config_path})


def _resolve_path(repo_root: Path, raw_path: str | Path) -> Path:
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path.resolve(strict=False)
    return (repo_root / path).resolve(strict=False)
