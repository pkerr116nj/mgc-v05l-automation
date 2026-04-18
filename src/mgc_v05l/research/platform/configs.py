"""Project-level config hashing helpers for research-platform runners."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Mapping

from .datasets import stable_hash

RESEARCH_PLATFORM_CONFIG_VERSION = "research_platform_config_v1"


def config_hash(config: Any) -> str:
    payload = {
        "artifact_version": RESEARCH_PLATFORM_CONFIG_VERSION,
        "config": asdict(config) if hasattr(config, "__dataclass_fields__") else config,
    }
    return stable_hash(payload, length=24)


def config_payload(config: Any) -> Mapping[str, Any]:
    if hasattr(config, "__dataclass_fields__"):
        resolved = asdict(config)
    else:
        resolved = config
    return {
        "artifact_version": RESEARCH_PLATFORM_CONFIG_VERSION,
        "config": resolved,
        "config_hash": config_hash(config),
    }
