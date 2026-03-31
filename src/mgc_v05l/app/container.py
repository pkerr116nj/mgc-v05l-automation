"""Dependency container for replay-first runs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from ..config_models import StrategySettings, load_settings_from_files
from ..market_data.replay_feed import ReplayFeed
from ..persistence import build_engine
from ..persistence.repositories import RepositorySet
from ..strategy.strategy_engine import StrategyEngine
from .strategy_runtime_registry import StrategyRuntimeRegistry, build_strategy_runtime_registry


@dataclass(frozen=True)
class ApplicationContainer:
    """Holds typed runtime settings and replay-first dependencies."""

    settings: StrategySettings
    repositories: RepositorySet
    replay_feed: ReplayFeed
    strategy_engine: StrategyEngine
    strategy_runtime_registry: StrategyRuntimeRegistry


def build_application_container(config_paths: Sequence[str | Path]) -> ApplicationContainer:
    """Construct the replay-first application container from typed config files."""
    settings = load_settings_from_files(config_paths)
    strategy_runtime_registry = build_strategy_runtime_registry(settings)
    primary_instance = strategy_runtime_registry.primary_engine_instance()
    if primary_instance is None or primary_instance.repositories is None or primary_instance.strategy_engine is None:
        repositories = RepositorySet(build_engine(settings.database_url))
        strategy_engine = StrategyEngine(settings=settings, repositories=repositories)
    else:
        repositories = primary_instance.repositories
        strategy_engine = primary_instance.strategy_engine
    replay_feed = ReplayFeed(settings)
    return ApplicationContainer(
        settings=settings,
        repositories=repositories,
        replay_feed=replay_feed,
        strategy_engine=strategy_engine,
        strategy_runtime_registry=strategy_runtime_registry,
    )
