# Experimental Causal Momentum-Shape Features

## Purpose

This module is an isolated research layer for derivative-style momentum features that may later augment the current MGC v0.5l signal framework.

It is experimental because:
- the production strategy logic is still anchored to the finalized v0.5l release candidate
- these features are not part of the locked Bull Snap, Bear Snap, or Asia VWAP reclaim rules
- they need separate replay and parity research before any production use

## Design Intent

These features are intended to augment, not replace, the current framework.

Examples of future use:
- additional context filters
- post-signal ranking
- diagnostics around momentum acceleration or compression

They must not:
- replace candles
- replace VWAP logic
- replace current turn, snap, or reclaim rules
- alter current production behavior without an explicit future build decision

## Anti-Lookahead Requirement

All calculations in this module must remain causal:
- trailing-only smoothing
- derivative calculations from current and prior smoothed values only
- no centered windows
- no future-bar leakage

The accompanying tests explicitly check that adding future data does not change already-computed earlier feature values.
