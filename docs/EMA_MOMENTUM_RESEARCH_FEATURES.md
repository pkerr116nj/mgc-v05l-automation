# EMA Momentum Research Features

## Purpose

This layer adds research-only EMA-based momentum and volume-aware impulse features without changing the production MGC v0.5l strategy path.

It computes and persists:
- `smoothed_close`
- `momentum_raw`
- `momentum_norm`
- `momentum_delta`
- `momentum_acceleration`
- `volume_ratio`
- `signed_impulse`
- `smoothed_signed_impulse`
- `impulse_delta`

It also persists first-pass interpreted research flags:
- `momentum_compressing_up`
- `momentum_turning_positive`
- `momentum_compressing_down`
- `momentum_turning_negative`

## Default Settings

- EMA length for `smoothed_close`: `3`
- EMA length for `smoothed_signed_impulse`: `3`
- Rolling volume window: `settings.vol_len`, which is currently `20` by default
- Normalization floor: `0.01`

## Feature Meaning

- `smoothed_close`: trailing EMA of close
- `momentum_raw`: one-bar change in `smoothed_close`
- `momentum_norm`: ATR-normalized `momentum_raw`
- `momentum_delta`: change in `momentum_raw`
- `momentum_acceleration`: change in `momentum_delta`
- `volume_ratio`: current volume relative to trailing average volume
- `signed_impulse`: close-to-close move scaled by `volume_ratio`
- `smoothed_signed_impulse`: trailing EMA of `signed_impulse`
- `impulse_delta`: one-bar change in `smoothed_signed_impulse`

## Research Boundary

This layer is research-only for now:
- no entry gating changes
- no execution changes
- no Bull Snap / Bear Snap / VWAP reclaim changes
- no sizing logic changes yet
- no trailing regression yet
