# Operator Console Refresh Field Mapping

## Screen Ownership

### Shared Shell

Primary
- Top nav for `Dashboard`, `Live P&L`, `Trade Entry`, and `Strategy Deep-Dive`
- Utility access for `Evidence` and `Settings`
- Live backend/source badges
- Session clock
- Headline symbol / price / change strip when quote truth is available
- Emergency halt action

Secondary
- Diagnostic source/detail banner
- Recent action stream

Hidden behind drilldown
- Raw launch-manager output
- Full backend retry / stale-listener diagnostics

### Dashboard

Primary
- Desktop/backend/API health state
- Shared paper runtime launch and recovery state
- Shared paper roster rows from `paper.approved_models.rows`
- Lane class, designation, participation, cadence, runtime attachment
- ATP benchmark/candidate summary
- Temporary paper enabled/loaded/audit-only summary

Secondary
- Detailed startup diagnostics
- Runtime recovery history
- Deep lane fault/operator panels

Hidden behind drilldown
- Raw launch artifacts
- Full diagnostic JSON payloads

### Live P&L

Primary
- Live account balances, buying power, day/open P&L
- Paper/simulated day/open/realized P&L
- Open positions monitor
- Working orders
- Recent fills
- Intraday equity progression
- Freshness / recent activity timestamps

Secondary
- Closed-trade ledgers
- Spread-parent detail
- Margin/conflict/activity drawers

Hidden behind drilldown
- Raw broker event payloads
- Full order/fill JSON

### Trade Entry

Primary
- Order ticket
- Quote box
- Current broker position
- Paper attribution rows for the selected symbol
- Strategy tag / intent attribution
- Live vs paper account clarity
- Working orders and recent fills for the selected symbol

Secondary
- Dry-run payload preview
- Replace / cancel / flatten specialist actions

Hidden behind drilldown
- Raw manual submit proof payloads
- Full broker validation artifacts

### Strategy Deep-Dive

Primary
- Strategy selector
- Core stats
- Strategy status
- Lane/runtime attachment truth
- Equity/history study panel
- Recent trade log
- Strategy diagnostics summary

Secondary
- Registry lens filters
- Same-underlying review workflow
- Signal / intent / fill audit detail
- Attribution tables

Hidden behind drilldown
- Raw study JSON
- Raw audit packets / lifecycle payloads

### Evidence

Primary
- Logs
- Diagnostics
- Raw payload exports
- Supporting artifacts not needed for default operator flow

## Field-Level Mapping Highlights

- `paper.approved_models.rows[].lane_class_label` -> Dashboard / Strategy Roster / Primary / keep label
- `paper.approved_models.rows[].designation_label` -> Dashboard / Strategy Roster / Primary / rename to `Designation`
- `paper.approved_models.rows[].participation_policy` -> Dashboard + Strategy Deep-Dive / Primary / keep label
- `paper.approved_models.rows[].net_side` -> Dashboard + Strategy Deep-Dive / Primary / rename to `Net Side`
- `paper.approved_models.rows[].total_quantity` -> Dashboard + Strategy Deep-Dive / Primary / rename to `Total Qty`
- `paper.approved_models.rows[].open_entry_leg_count` -> Dashboard + Strategy Deep-Dive / Primary / rename to `Open Legs`
- `paper.approved_models.rows[].additional_entry_allowed` -> Dashboard + Strategy Deep-Dive / Primary / rename to `Can Add More`
- `paper.approved_models.rows[].execution_timeframe` -> Dashboard + Strategy Deep-Dive / Primary / keep label
- `paper.approved_models.rows[].context_timeframes` -> Dashboard + Strategy Deep-Dive / Primary / keep label
- `paper.approved_models.rows[].last_execution_bar_evaluated_at` -> Dashboard + Strategy Deep-Dive / Primary / rename to `Last 1m Eval`
- `paper.approved_models.rows[].last_completed_context_bars_at` -> Dashboard + Strategy Deep-Dive / Primary / rename to `Completed Context`
- `paper.readiness.*` launch/recovery/auth blockers -> Dashboard / Primary / keep reason codes
- `desktopState.source.*` + `desktopState.backend.*` -> Shared Shell + Dashboard / Primary / rename to connection badges / backend live status
- `desktopState.startup.*` -> Evidence / Secondary / launcher recovery drilldown
- `production_link.portfolio.*` -> Live P&L / Primary / rename to account terms (`Net Liq`, `Buying Power`, `Day P&L`)
- `production_link.orders.open_rows` -> Live P&L + Trade Entry / Primary / `Working Orders`
- `production_link.orders.recent_fill_rows` -> Live P&L + Trade Entry / Primary / `Recent Fills`
- `paper.strategy_performance.rows` -> Live P&L + Strategy Deep-Dive / Primary or Secondary depending on context
- `paper.signal_intent_fill_audit.rows` -> Strategy Deep-Dive / Secondary
- `paper.non_approved_lanes.rows` + `paper.temporary_paper_runtime_integrity.*` -> Dashboard / Primary summary, Evidence / Secondary detail
- `paper.tracked_*` compatibility snapshots -> Evidence / Secondary audit-only

## Redundant Or Demoted Primary Surfaces

- Tracked paper as a primary ATP operating surface: removed from the primary path
- Artifact-first runtime pages (`Runtime`, `Replay`, `Logs`, `Configuration`) as top-level navigation: demoted into the Evidence model
- Generic “latest activity” as the only cadence signal: replaced in primary lane surfaces by explicit execution/context cadence fields
