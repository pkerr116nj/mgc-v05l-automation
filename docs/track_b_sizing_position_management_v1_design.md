# Track B Sizing / Position Management v1 Design

Status: architecture/design only. This document does not add runtime behavior, broker authority, order intent creation, lifecycle mutation, lane execution, paper proof, or strategy behavior changes.

## Purpose

Track B sizing and position management should be modular advisory context, not an execution authority.

The goal is to define reusable context layers that can consume:

- Entry Acceptance
- Exit Context
- Lifecycle Awareness
- Participation / Pressure
- Regime State
- read-only governance, exposure, and account context

The layers may describe whether size should be normal, reduced, blocked, held, reviewed, or potentially added later. They must not create executable quantity, submit authority, broker commands, order intents, or lifecycle mutations.

Governance and exposure gates remain authoritative for all allowed size, position caps, account constraints, and trade eligibility. Broker execution happens only after a separate authority path creates a valid order intent and passes all governance, reconciliation, and broker-safety checks.

## Layer Split

V1 should separate pre-entry sizing context from in-position size management context.

### Initial Sizing Context

Contract name: `track_b_initial_sizing_context_v1`

Purpose: provide advisory sizing quality at entry-candidate time.

Inputs:

- Entry Acceptance output, including acceptance class, score, failure reasons, candidate family, side, and timeframe context.
- Regime State output, including session bucket, market regime state, volatility/range state, trend/chop state, liquidity state, and confidence.
- Participation / Pressure context when available, including pressure state, directional bias, hold quality, exit urgency, and pressure confidence.
- optional Data Readiness / provenance context when separated later.
- read-only governance/exposure/account context, including allowed instruments, current exposure, account constraints, and risk caps.

Outputs:

- `initial_size_context`
- `size_quality_context`
- `size_multiplier_context`, advisory label only.
- `size_block_context`, advisory label only.
- `size_caution_reasons`
- `no_size_reasons`
- `confidence`
- `failure_reasons`
- safety flags all false.

Example labels:

- `NORMAL_SIZE_CONTEXT`
- `REDUCED_SIZE_CONTEXT`
- `MINIMUM_SIZE_CONTEXT`
- `NO_SIZE_LOW_CONFIDENCE`
- `NO_SIZE_GOVERNANCE_READ_MODEL_BLOCKED`
- `NO_SIZE_STALE_OR_UNRECONCILED_CONTEXT`

The layer may say that a candidate appears suitable for normal sizing, but it cannot authorize that size. It may say that sizing should be reduced or blocked, but the strategy and governance path still own executable decisions.

### In-Position Size Management Context

Contract name: `track_b_position_management_context_v1`

Purpose: provide advisory hold, add, reduce, and stop-add context after a lifecycle-managed position exists.

Inputs:

- Lifecycle Awareness output, including lifecycle state, hold quality, exit urgency, add/reduce context, patience context, MFE/MAE, bars since entry, progress, confidence, and failure reasons.
- Exit Context output, including exit profile context, hold quality, exit urgency, reduce-size context, scale-up context, protective context, and confidence.
- Participation / Pressure output, including pressure state, directional bias, hold quality, exit urgency, continuation confidence, and failure reasons.
- Regime State output, including trend/chop, volatility/range, liquidity, session, and confidence.
- Entry Acceptance context captured at entry.
- read-only lifecycle-owned position summary.
- read-only governance, account, and exposure context.

Outputs:

- `position_size_management_context`
- `hold_full_context`
- `add_size_context`
- `reduce_size_context`
- `stop_add_context`
- `position_risk_context`
- `management_reasons`
- `warning_reasons`
- `failure_reasons`
- `confidence`
- safety flags all false.

Example labels:

- `HOLD_FULL_SIZE_CONTEXT`
- `HOLD_FULL_BUT_MONITOR`
- `ADD_SIZE_NOT_INDICATED`
- `ADD_SIZE_BLOCKED_LOW_CONFIDENCE`
- `ADD_SIZE_ONLY_AFTER_SEPARATE_AUTHORITY`
- `STOP_ADD_DEGRADED_CONTEXT`
- `REDUCE_NOT_INDICATED`
- `REDUCE_SIZE_CONTEXT`
- `REDUCE_REVIEW_CONTEXT`
- `NO_POSITION_MANAGEMENT_LOW_CONFIDENCE`

This layer may describe add/reduce/hold-full context. It must not submit, cancel, close, place orders, flatten, or write lifecycle rows.

## Compatibility Contract Across Strategies

The sizing context must be strategy-family neutral.

Each strategy may supply its candidate family, side, entry acceptance context, and optional strategy-specific advisory metadata. The sizing layer should normalize those inputs into shared labels rather than creating strategy-specific sizing rules.

Required compatibility principles:

- Use stable labels that can be consumed by operator UI, post-trade attribution, future sizing research, and future governance review.
- Preserve original source contexts in nested read-only fields.
- Do not require every strategy to provide all optional layers.
- Fail closed when required context is missing, stale, malformed, or conflicting.
- Never encode broker quantity, broker account routing, order type, transmit flags, or executable instructions.

The compatibility shape should allow exact baseline strategies, adaptive-exit candidates, participation-aware candidates, and future pattern families to share the same advisory vocabulary.

## Read-Only Account And Exposure Inputs

Account, governance, and exposure inputs may be consumed only as read-only context.

Examples:

- account id or account alias, if already present in safe read models.
- current instrument exposure.
- current strategy-family exposure.
- max allowed exposure from governance read model.
- day/session risk limit status.
- loss lockout or cool-down status.
- symbol eligibility status.
- reconciliation status.

Allowed use:

- add warning reasons.
- lower confidence.
- mark `NO_SIZE_GOVERNANCE_READ_MODEL_BLOCKED`.
- mark `ADD_SIZE_BLOCKED_LOW_CONFIDENCE`.
- explain operator-facing caution.

Forbidden use:

- create allowed quantity.
- bypass governance.
- authorize a trade.
- create order intent.
- alter lifecycle ownership.
- issue broker commands.
- infer account truth from stale or missing data.

Governance and exposure remain the source of truth. Sizing context only describes how the advisory layers interpret read-only inputs.

## Authority Boundary

Hard safety flags must always be false:

- `strategy_authority=false`
- `broker_state_mutated=false`
- `submit_attempted=false`
- `cancel_attempted=false`
- `close_attempted=false`
- `place_order_attempted=false`
- `order_intent_created=false`
- `lifecycle_mutated=false`
- `runtime_trade_eligible=false`

The broker layer executes only after a separate authority path exists and only after governance, exposure, reconciliation, lifecycle ownership, and order-intent checks pass. This design does not create that authority path.

V1 sizing must be described as:

- advisory
- offline-capable
- fail-closed
- read-only with respect to account, exposure, broker, and lifecycle state
- not a strategy behavior change

## Relationship To Baseline Exit Candidates

### Exact Baseline Fixed 36b

The fixed 36-bar baseline should remain a baseline reference. Sizing context can help explain how a fixed 36b position was held, reduced, or considered risky in hindsight, but it should not alter the fixed baseline behavior in v1.

Initial sizing may record whether the entry context looked normal, reduced, or blocked at the time. In-position management may record whether the fixed 36b hold passed through:

- favorable expansion
- healthy pullback
- compression/stall
- participation decay
- adverse expansion
- session or regime risk

This improves attribution without changing the fixed 36b candidate.

### Adaptive 24/36 Candidate

The adaptive 24-to-36 candidate can consume advisory context later to explain why extension to 36 bars is attractive or risky. V1 sizing context should not decide the extension.

Useful future mappings:

- strong lifecycle progress plus supportive participation plus aligned regime may support `HOLD_FULL_SIZE_CONTEXT`.
- decaying lifecycle plus participation collapse may support `REDUCE_REVIEW_CONTEXT`.
- compression/chop may support `HOLD_FULL_BUT_MONITOR` or `STOP_ADD_DEGRADED_CONTEXT`.
- stale/unreconciled context must produce low-confidence no-action labels.

The adaptive exit module remains separate and authoritative only inside its own approved path.

## Relationship To Existing Advisory Layers

### Entry Acceptance

Entry Acceptance provides candidate quality and structure. Initial Sizing should treat it as the primary pre-entry quality input.

Examples:

- `EXACT_STRUCTURAL_MATCH` with high confidence may support normal-size context.
- `NEAR_STRUCTURAL_MATCH` may support reduced-size context.
- `DEGRADED_BUT_VALID_MATCH` may support minimum-size or caution context.
- `STRUCTURALLY_INVALID` or low-confidence classes must support no-size context.

Entry Acceptance remains advisory unless separately promoted by strategy authority.

### Lifecycle Awareness

Lifecycle Awareness classifies the open trade state. In-position management should use it as the primary lifecycle input.

Examples:

- `FAVORABLE_EXPANSION` may support hold-full context.
- `HEALTHY_PULLBACK` may support patience and stop-add context.
- `STALLED` may support monitor or reduce-review context depending on regime and participation.
- `DECAYING` and `ADVERSE_DOMINANCE` may support reduce-review context.
- `LOW_CONFIDENCE_STALE_OR_UNRECONCILED` must fail closed.

Lifecycle Awareness remains context-only and does not know account or order mechanics.

### Participation / Pressure

Participation / Pressure describes pressure quality. It should complement lifecycle state.

Examples:

- persistent favorable pressure may support hold-full context.
- healthy pullback pressure may support patience.
- impulse decay or participation collapse may support stop-add or reduce-review context.
- low-confidence pressure must lower confidence and block add-size context.

Participation / Pressure must not duplicate regime state or size authority.

### Regime State

Regime State describes session, volatility/range, trend/chop, liquidity, and directional context.

Examples:

- directional expansion aligned with the trade may support patience or hold-full context.
- high-volatility adverse expansion may increase reduce-review urgency.
- compression/chop may reduce add-size attractiveness.
- stale/thin regime state must fail closed.

Regime State must not decide size by itself.

### Exit Context

Exit Context describes exit profile, hold quality, exit urgency, protective context, reduce-size context, and scale-up context.

Sizing / Position Management should use Exit Context as a strong advisory input, especially after entry. If Exit Context marks high urgency or protective context, in-position management should avoid add-size labels and may mark reduce-review context.

Exit Context must not become order authority through sizing.

## Fail-Closed Policy

The sizing layers fail closed when:

- required advisory context is missing.
- input provenance is missing or not trusted.
- context is stale.
- timeframes are mixed or ambiguous.
- lifecycle ownership is absent for in-position management.
- broker/reconciliation read model is mismatched or stale.
- account/exposure read model is stale, missing, or contradictory.
- any input attempts to present research output as runtime truth.

Fail-closed output should:

- set confidence to `0.0` or a low bounded value.
- set no-size or no-adjustment context.
- include concrete failure reasons.
- preserve all safety flags as false.
- avoid executable quantity, order intent, broker route, and lifecycle mutation.

## Proposed Artifact Paths

Design-only proposed paths:

- `outputs/track_b_execution_core/initial_sizing/latest_initial_sizing_context.json`
- `outputs/track_b_execution_core/position_management/latest_position_management_context.json`

No writer, CLI, scheduler, runtime producer, strategy runner integration, broker integration, or generated output is introduced by this design slice.

## Future Backfill Plan

Backfill should be designed before runtime integration.

Planned stages:

1. Build pure offline evaluators for initial sizing and in-position management.
2. Add fixtures covering exact baseline, near/degraded candidate, favorable lifecycle, healthy pullback, decay, adverse dominance, participation collapse, regime compression, and stale/unreconciled fail-closed cases.
3. Add offline CLIs that write only to user-specified output directories.
4. Run historical backfill against archived Entry Acceptance, Exit Context, Lifecycle Awareness, Participation / Pressure, and Regime State artifacts.
5. Compare advisory labels against fixed 36b and adaptive 24/36 outcomes.
6. Publish attribution reports showing where sizing context would have warned, reduced, blocked add-size, or supported hold-full.
7. Only after review, design a separate runtime producer with source ownership, provenance, retention, and authority boundary documentation.

Backfill must never imply that a historical advisory label was executable. It is for analysis, explainability, and future design only.

## V1 Non-Goals

V1 does not:

- calculate executable order quantity.
- submit, cancel, close, flatten, or place orders.
- create order intent.
- mutate lifecycle rows.
- restart runtime.
- execute lanes.
- run paper proof.
- change strategy behavior.
- replace governance or exposure controls.
- promote research outputs into runtime truth.
- create a runtime producer.

The only intended result of v1 architecture is a stable advisory vocabulary and contract for future offline implementation.
