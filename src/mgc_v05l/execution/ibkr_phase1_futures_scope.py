"""Compatibility imports for shared phase-1 futures execution targets."""

from __future__ import annotations

from mgc_v05l.phase1_futures_scope import (
    active_index_contract_month,
    active_platinum_contract_month,
    phase1_execution_symbol_for_source,
    phase1_execution_target_for_source,
    phase1_execution_target_for_symbol,
    supported_phase1_source_instruments,
)

__all__ = [
    "active_index_contract_month",
    "active_platinum_contract_month",
    "phase1_execution_symbol_for_source",
    "phase1_execution_target_for_source",
    "phase1_execution_target_for_symbol",
    "supported_phase1_source_instruments",
]
