# ATP Asia Drift Staged-Add Shadow Observation Plan

## Purpose

This note defines a paper/shadow observation plan for the MGC Asia-only staged-add overlay:

- `Promotion 1` at `+0.75R`
- `VWAP Favorable Only`
- one add maximum
- same direction only
- positive reacceleration required
- low above entry required

The goal is to log future add opportunities without changing execution behavior and compare them against the current historical sample of `28` staged-add events.

## Guardrails

- No execution behavior changes.
- No live execution changes.
- No broker or IBKR changes.
- No widening to U.S.
- No London execution enablement.
- No baseline sizing changes.
- No add-threshold changes.
- No exit redesign.
- No new add rules.

## Logging Approach

Future staged-add opportunities should be recorded as shadow/paper observations only.

For each eligible base ATP Companion Asia trade:

1. evaluate whether the staged-add condition would have triggered
2. log the hypothetical add event even if no execution action is taken
3. attach the hypothetical add observation to the underlying base trade id
4. keep the frozen ATP Companion Baseline v1 trade outcome unchanged

## Minimum Logged Fields

For each shadow add opportunity, track:

- base trade id
- decision date
- session segment
- base entry timestamp
- add signal timestamp
- add trigger reason
- base trade state at add
- hypothetical add entry price
- hypothetical add exit timestamp
- hypothetical add exit reason
- hypothetical incremental add P/L
- post-add MAE
- post-add MFE
- whether the add improved the trade
- whether the add worsened the trade
- whether the add would have increased live discomfort

Recommended base-trade-state fields at add:

- unrealized P/L at add
- bars held at add
- current excursion from entry
- VWAP favorable state
- reacceleration confirmation state

## Comparison Against Historical 28-Event Sample

Each new shadow add observation should be compared against the current historical branch profile:

- add frequency
- add success rate
- average incremental add P/L
- worst add contribution
- concentration of add P/L in top events
- percent of adds that improve the trade
- percent of adds that worsen the trade
- time clustering by month/quarter/year

Useful direct comparisons:

- rolling add count vs historical `28`
- rolling add success rate vs historical `82.1429%`
- rolling average add contribution vs historical sample
- rolling worst-event severity vs historical `-2.2281`
- whether new adds remain small-loss / occasional-large-win shaped

## Useful Additional Sample Size

The current overlay should stay in paper/shadow observation until the sample is materially larger.

Minimum useful targets:

- first checkpoint: `+20` additional add observations
- stronger checkpoint: `+40` additional add observations
- preferred reconsideration zone: `50-75` total add events

Reason:

- the current `28` events are promising but thin
- a meaningful share of gains came from the top `5` adds and from `2026`
- a broader time distribution is needed before reconsidering promotion

## Review Metrics

Future observation reviews should report:

- add signal date/time
- base trade state at add
- add trigger reason
- hypothetical add entry
- hypothetical add exit
- incremental add P/L
- post-add MAE/MFE
- whether the add improved or worsened the trade
- whether it would have increased live discomfort

Recommended aggregation:

- add count
- add success rate
- average add contribution
- median add contribution
- top 1 / top 3 / top 5 concentration
- total add P/L excluding top 1 / top 3 / top 5
- quarterly and yearly spread
- worst add event
- average losing add
- discomfort-flag count

## Reconsideration Standard

The overlay should only be reconsidered after new paper/shadow data answers:

1. are positive add contributions still present after the sample grows materially?
2. does the result remain positive after removing the largest few add events?
3. are gains spread across time rather than concentrated in a short cluster?
4. do losing adds remain shallow enough to avoid obvious discomfort escalation?

## Current Status

- Historical branch status: `ENHANCEMENT_PROMISING_BUT_THIN`
- Correct next step: paper/shadow observation only
- Not approved for promotion
- Not approved for U.S. widening
- Not approved for sizing changes
