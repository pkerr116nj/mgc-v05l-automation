"""Shared labels for Track B research/offline diagnostic artifacts."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Mapping


RESEARCH_OFFLINE_SOURCE_CATEGORY = "research/offline"


def build_research_offline_metadata(
    *,
    producer: str,
    source_paths: Iterable[str | Path] = (),
    notes: Iterable[str] = (),
) -> dict[str, Any]:
    """Return standard non-authority metadata for research/offline artifacts."""

    return {
        "research_only": True,
        "offline_diagnostic": True,
        "not_runtime_authority": True,
        "not_broker_truth": True,
        "not_market_data_runtime_truth": True,
        "not_routing_authority": True,
        "dashboard_projection_consumed_as_authority": False,
        "source_category": RESEARCH_OFFLINE_SOURCE_CATEGORY,
        "producer": producer,
        "source_paths": [str(path) for path in source_paths],
        "notes": list(notes),
    }


def apply_research_offline_metadata(
    payload: Mapping[str, Any],
    *,
    producer: str,
    source_paths: Iterable[str | Path] = (),
    notes: Iterable[str] = (),
) -> dict[str, Any]:
    """Copy a payload and add the standard research/offline non-authority labels."""

    labeled = dict(payload)
    metadata = build_research_offline_metadata(
        producer=producer,
        source_paths=source_paths,
        notes=notes,
    )
    labeled.update(metadata)
    labeled["research_offline_metadata"] = metadata
    return labeled


__all__ = [
    "RESEARCH_OFFLINE_SOURCE_CATEGORY",
    "apply_research_offline_metadata",
    "build_research_offline_metadata",
]
