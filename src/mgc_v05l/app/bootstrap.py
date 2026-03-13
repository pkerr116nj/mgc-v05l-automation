"""Bootstrap helpers for the replay-first application."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

from ..domain.events import ServiceStartupEvent
from .container import ApplicationContainer, build_application_container


def bootstrap_service(config_paths: Sequence[str | Path]) -> tuple[ApplicationContainer, ServiceStartupEvent]:
    """Build the application container and emit a startup event."""
    container = build_application_container(config_paths)
    return container, ServiceStartupEvent(source="app.bootstrap")
