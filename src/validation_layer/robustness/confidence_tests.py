"""Backward-compatible import shim for market-level permutation significance."""

from __future__ import annotations

from .market_permutation import run_market_permutation_module

# Legacy alias kept to avoid breaking earlier imports.
run_confidence_tests_module = run_market_permutation_module
