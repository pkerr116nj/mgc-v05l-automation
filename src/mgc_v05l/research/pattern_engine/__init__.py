"""Pattern Engine v1 research primitives and scanners."""

from .dataset import PatternEngineContext, load_pattern_engine_contexts
from .families import PatternFamilySpec, default_pattern_family_specs
from .mining import (
    PatternMatchRow,
    PatternSequenceSummaryRow,
    PatternSummaryRow,
    build_pattern_engine_v1_report,
    write_pattern_engine_v1_report,
)
from .phases import PhaseSequenceMatch, PhaseStep
from .primitives import PatternPrimitivePoint, build_pattern_primitive_points

__all__ = [
    "PatternEngineContext",
    "PatternFamilySpec",
    "PatternMatchRow",
    "PatternPrimitivePoint",
    "PatternSequenceSummaryRow",
    "PatternSummaryRow",
    "PhaseSequenceMatch",
    "PhaseStep",
    "build_pattern_engine_v1_report",
    "build_pattern_primitive_points",
    "default_pattern_family_specs",
    "load_pattern_engine_contexts",
    "write_pattern_engine_v1_report",
]
