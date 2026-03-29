"""Pattern Engine v1 phase-sequence representation."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PhaseStep:
    phase_name: str
    timestamp: str
    primitive_signature: str


@dataclass(frozen=True)
class PhaseSequenceMatch:
    family_name: str
    direction: str
    session_phase: str
    anchor_timestamp: str
    steps: tuple[PhaseStep, ...]
