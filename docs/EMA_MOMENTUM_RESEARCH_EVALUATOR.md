# EMA Momentum Research Evaluator

## Purpose

This layer labels two research-only tracks on top of the persisted EMA momentum features:
- filter track
- math-trigger track

It does not change production triggers, strategy state, or execution.

## Track Meanings

### Filter Track

The filter track answers:
- if baseline setup context was present, would the math layer have allowed it?
- if allowed, was the quality stronger because both research flags were present?

Current first-pass rule:
- long filter passes only when baseline long context exists and both:
  - `momentum_compressing_up`
  - `momentum_turning_positive`
- short filter passes only when baseline short context exists and both:
  - `momentum_compressing_down`
  - `momentum_turning_negative`

### Math-Trigger Track

The math-trigger track answers:
- would the math layer itself have identified a turn, independent of baseline production triggers?

Current first-pass rule:
- long math trigger when:
  - `momentum_compressing_up OR momentum_turning_positive`
- short math trigger when:
  - `momentum_compressing_down OR momentum_turning_negative`

## What Is Labeled Now

Per bar, the evaluator labels:
- baseline long/short context present or absent
- `filter_pass_long`
- `filter_pass_short`
- `trigger_long_math`
- `trigger_short_math`
- simple research-only quality scores
- simple research-only size recommendation placeholders

## What Is Not Yet Implemented

- no production entry gating
- no trailing regression
- no live sizing logic
- no production trigger rewrites
- no live execution changes

## Baseline Context

Where available, baseline context is currently consumed from persisted research signal rows:
- `bull_snap_raw`
- `bear_snap_raw`
- `asia_vwap_reclaim_raw`

If a bar has no baseline signal context persisted yet, the evaluator stays additive and treats the baseline context as absent.
