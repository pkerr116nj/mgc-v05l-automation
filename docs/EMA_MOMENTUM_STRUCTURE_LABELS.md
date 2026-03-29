# EMA Momentum Structure Labels

This layer adds research-only structural labels on top of the existing EMA momentum features and evaluator outputs. It does not change production triggers, execution, or strategy state behavior.

## What The Labels Mean

### Compression

Compression tries to identify directional pressure that is weakening before a turn.

Long-side `compression_long` requires:

- `momentum_norm < 0`
- `momentum_norm` improving versus the prior bar
- `momentum_acceleration > 0`
- `signed_impulse` improving versus the prior bar

Short-side `compression_short` is the symmetric opposite.

### Reclaim / Failure

These labels mark first-pass structural recovery or loss zones using practical trailing context.

`reclaim_long` can trigger from:

- reclaiming VWAP from below
- closing above a recent trailing micro-range high

`failure_short` can trigger from:

- losing VWAP from above
- closing below a recent trailing micro-range low

The current implementation intentionally uses simple trailing micro-structure instead of more complicated pivot frameworks.

### Separation

Separation tries to confirm that price moved away from the reclaim or failure area instead of immediately snapping back through it.

Long-side `separation_long` requires:

- a recent reclaim level
- close above that level by a small ATR-based threshold
- low holding above the reclaim level
- short-horizon follow-through still supportive

`separation_short` is the symmetric opposite.

## Combined Candidate Labels

The combined labels are research-only structural candidate states:

- `structure_long_candidate`
- `structure_short_candidate`

They require recent compression plus recent reclaim/failure plus separation. This is meant to label a first-pass derivative-informed turn structure, not to place orders.

## Deferred On Purpose

- third derivative
- production gating
- live sizing logic
- execution changes

This layer is intentionally additive and remains separate from the production v0.5l strategy path.
