"""Stable adapter layer between approved quant baselines and research helpers."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from inspect import signature
from pathlib import Path
from typing import Any

from ...research.quant_futures import _FrameSeries, _resolve_exit
from ...research.quant_futures_phase2a import _build_symbol_store


APPROVED_QUANT_BASELINE_RUNTIME_CONTRACT_VERSION = "approved_quant_baseline_runtime_v1"


@dataclass(frozen=True)
class ApprovedQuantResearchDependency:
    dependency_id: str
    import_path: str
    dependency_kind: str
    purpose: str
    required_parameters: tuple[str, ...] = ()


APPROVED_QUANT_RESEARCH_DEPENDENCIES: tuple[ApprovedQuantResearchDependency, ...] = (
    ApprovedQuantResearchDependency(
        dependency_id="phase2a_symbol_store_builder",
        import_path="mgc_v05l.research.quant_futures_phase2a._build_symbol_store",
        dependency_kind="callable",
        purpose="Build aligned execution bars and approved feature rows for approved-lane symbols.",
        required_parameters=("database_path", "execution_timeframe", "symbols"),
    ),
    ApprovedQuantResearchDependency(
        dependency_id="quant_exit_resolver",
        import_path="mgc_v05l.research.quant_futures._resolve_exit",
        dependency_kind="callable",
        purpose="Resolve approved-lane stop/target/time exits against the execution frame.",
        required_parameters=("direction", "execution", "entry_index", "hold_bars", "stop_price", "target_price"),
    ),
    ApprovedQuantResearchDependency(
        dependency_id="quant_frame_series",
        import_path="mgc_v05l.research.quant_futures._FrameSeries",
        dependency_kind="type",
        purpose="Typed execution frame container shared by the approved baseline evaluator.",
    ),
)


APPROVED_QUANT_REQUIRED_FEATURE_KEYS: dict[str, tuple[str, ...]] = {
    "phase2c.breakout.metals_only.us_unknown.baseline": (
        "ready",
        "risk_unit",
        "session_label",
        "regime_up",
        "compression_60",
        "compression_5",
        "breakout_up",
        "close_pos",
        "slope_60",
    ),
    "phase2c.failed.core4_plus_qc.no_us.baseline": (
        "ready",
        "risk_unit",
        "session_label",
        "failed_breakout_short",
        "dist_240",
        "close_pos",
        "body_r",
    ),
}


ApprovedQuantFrameSeries = _FrameSeries


def approved_quant_research_dependencies() -> tuple[ApprovedQuantResearchDependency, ...]:
    return APPROVED_QUANT_RESEARCH_DEPENDENCIES


def approved_quant_research_dependency_rows() -> list[dict[str, Any]]:
    return [asdict(dependency) for dependency in APPROVED_QUANT_RESEARCH_DEPENDENCIES]


def assert_approved_quant_runtime_contract() -> None:
    _assert_callable_contract(_build_symbol_store, APPROVED_QUANT_RESEARCH_DEPENDENCIES[0])
    _assert_callable_contract(_resolve_exit, APPROVED_QUANT_RESEARCH_DEPENDENCIES[1])


def build_approved_quant_symbol_store(
    *,
    database_path: Path,
    execution_timeframe: str,
    symbols: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    assert_approved_quant_runtime_contract()
    return _build_symbol_store(
        database_path=database_path,
        execution_timeframe=execution_timeframe,
        symbols=symbols,
    )


def resolve_approved_quant_exit(
    *,
    direction: str,
    execution: ApprovedQuantFrameSeries,
    entry_index: int,
    hold_bars: int,
    stop_price: float,
    target_price: float,
) -> tuple[int, float, str]:
    assert_approved_quant_runtime_contract()
    return _resolve_exit(
        direction=direction,
        execution=execution,
        entry_index=entry_index,
        hold_bars=hold_bars,
        stop_price=stop_price,
        target_price=target_price,
    )


def validate_approved_quant_feature_payload(*, lane_id: str, feature: dict[str, Any]) -> None:
    required_keys = APPROVED_QUANT_REQUIRED_FEATURE_KEYS.get(lane_id, ())
    missing = [key for key in required_keys if key not in feature]
    if missing:
        raise KeyError(
            "Approved quant lane feature payload drifted for "
            f"{lane_id}: missing keys {', '.join(sorted(missing))}"
        )


def _assert_callable_contract(func: Any, dependency: ApprovedQuantResearchDependency) -> None:
    func_signature = signature(func)
    missing = [name for name in dependency.required_parameters if name not in func_signature.parameters]
    if missing:
        raise RuntimeError(
            "Approved quant runtime dependency contract mismatch for "
            f"{dependency.import_path}: missing parameters {', '.join(missing)}"
        )
