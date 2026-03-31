# Execution Realism Annex v1

## Purpose

This annex defines a reusable execution-analysis framework for measuring the gap between quoted economics and executable economics. It is not a signal-generation module and it is not an execution engine.

It is intended to measure:

- entry quality
- exit quality
- closeability
- regime dependence

The goal is to determine whether a spread strategy remains attractive after realistic execution friction.

## Scope

Initial focus:

- vertical credit spreads
- especially SPX bull put spreads

Reusability requirement:

- the design must include `ticker` explicitly so the same analytics can later be reused for other underlyings without redesign
- `underlying_type` or `asset_class` may be included where helpful so the annex remains cleanly reusable across instrument families

This annex is reusable across underlyings and should not assume SPX-only semantics in its schema.

## Core Questions

The annex should answer:

- How close do entries fill relative to displayed mid?
- How close do exits fill relative to displayed mid or mark?
- At what debit levels do buy-to-close orders stop filling reliably?
- How do fills change by time of day, width, volatility regime, and urgency?

## Data Entities

### Trade-Level Record

Required fields:

- `trade_id`
- `ticker`
- `underlying_type`
- `asset_class`
- `strategy_name`
- `entry_ts`
- `exit_ts`
- `expiration`
- `dte_entry`
- `short_put_strike`
- `long_put_strike`
- `width`
- `quantity`
- `entry_type`
- `exit_type`
- `underlying_price_entry`
- `underlying_price_exit`
- `iv_context_entry`
- `iv_context_exit`
- `time_of_day_bucket_entry`
- `time_of_day_bucket_exit`
- `event_window_flag_entry`
- `event_window_flag_exit`
- `stress_regime_flag_entry`
- `stress_regime_flag_exit`
- `displayed_entry_bid`
- `displayed_entry_ask`
- `displayed_entry_mid`
- `submitted_entry_limit`
- `actual_entry_fill`
- `entry_fill_delta_vs_mid`
- `displayed_exit_bid`
- `displayed_exit_ask`
- `displayed_exit_mid`
- `submitted_exit_limit`
- `actual_exit_fill`
- `exit_fill_delta_vs_mid`
- `entry_time_to_fill_seconds`
- `exit_time_to_fill_seconds`
- `entry_reprice_count`
- `exit_reprice_count`
- `entry_partial_fill_flag`
- `exit_partial_fill_flag`
- `exit_unfilled_flag`
- `gross_credit_displayed_mid`
- `gross_credit_actual`
- `gross_exit_displayed_mid`
- `gross_exit_actual`
- `realized_slippage_round_trip`
- `realized_pnl_actual`
- `realized_pnl_mid_model`
- `friction_drag_amount`
- `friction_drag_pct_of_credit`

### Order-Attempt-Level Record

Required fields:

- `order_attempt_id`
- `trade_id`
- `ticker`
- `ts`
- `side`
- `quoted_bid`
- `quoted_ask`
- `quoted_mid`
- `submitted_limit`
- `status`
- `fill_price`
- `time_to_fill_seconds`
- `reprice_number`
- `time_of_day_bucket`
- `open_window_flag`
- `event_window_flag`
- `stress_regime_flag`
- `width`
- `dte`
- `debit_bucket`
- `credit_bucket`
- `urgency_type`
- `combo_flag`
- `notes`

## Standard Classifications

### Time-of-Day Buckets

First-pass buckets:

- `OPENING`
- `MORNING`
- `MIDDAY`
- `POWER_HOUR`
- `CLOSING`

### Debit Buckets for BTC

First-pass buckets:

- `<0.10`
- `0.10-0.14`
- `0.15-0.19`
- `0.20-0.29`
- `0.30-0.49`
- `>=0.50`

### Credit Buckets for STO

First-pass buckets:

- `<0.50`
- `0.50-0.74`
- `0.75-0.99`
- `1.00-1.49`
- `1.50-1.99`
- `>=2.00`

### Regime Buckets

First-pass buckets:

- `NORMAL`
- `ELEVATED_VOL`
- `STRESS`
- `EVENT`

### Urgency Buckets

First-pass buckets:

- `ROUTINE`
- `SCHEDULED_TARGET`
- `DEFENSIVE`
- `FORCED`

## Derived Metrics

### Entry Quality

- mean slippage vs displayed mid
- median slippage vs displayed mid
- fill probability at submitted mid
- fill probability after one concession
- fill probability after two concessions
- entry time to fill by bucket
- entry slippage by time bucket
- entry slippage by width
- entry slippage by regime

### Exit Quality

- mean slippage vs displayed mid
- median slippage vs displayed mid
- exit fill probability at submitted mid
- exit fill probability after one concession
- exit fill probability after two concessions
- exit time to fill by debit bucket
- exit slippage by debit bucket
- exit slippage by regime

### Closeability

- fill probability by debit bucket
- median time to fill by debit bucket
- concession needed by debit bucket
- unfilled rate by debit bucket
- partial-fill rate by debit bucket

### Round-Trip Realism

- round-trip slippage
- round-trip friction drag
- realized P/L difference: actual vs mid-modeled
- friction drag as percent of opening credit
- asymmetry between opening and closing execution quality

## Initial Hypotheses To Test

- entries after the open settles fill near mid often enough to treat mid as realistic in normal conditions
- BTC orders below `0.20` become materially less reliable
- BTC at `0.20` is more executable than BTC at `0.15`
- round-trip friction is asymmetric
- defensive exits are materially worse than routine exits

## First-Pass Decision Rules This Annex May Later Support

- opening-window assumptions should default to degraded execution realism
- routine BTC targets may need to prefer `0.20` over `0.15`
- sub-`0.20` exits should be treated as a closeability issue, not only a pricing issue
- defensive exits should use stress execution assumptions

These are downstream policy candidates only. This annex does not implement them.

## Output Reports

The annex should eventually support:

- entry quality summary
- exit quality summary
- closeability table
- strategy realism adjustment table

## House Rule

A spread strategy is not valid unless it remains attractive after realistic execution friction derived from this annex.

## Boundary

This annex is design/spec only.

It does not implement:

- execution tracking
- live broker integration
- backtesting logic
- live execution
- production strategy behavior changes

It is intended to remain concise, practical, and reusable across underlyings through explicit inclusion of `ticker`.
