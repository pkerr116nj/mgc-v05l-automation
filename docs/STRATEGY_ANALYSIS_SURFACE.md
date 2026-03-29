# Strategy Analysis Surface

## Evidence Lanes

The unified strategy analysis surface is strategy-centric and provenance-preserving. It does not merge replay, paper, and research evidence into one ambiguous P/L stream.

- `benchmark_replay`
  Replay / benchmark evidence from historical playback strategy-study artifacts that preserve baseline-parity replay semantics.
- `paper_runtime`
  Current or recent paper-runtime evidence from dashboard snapshots, tracked paper detail, and lane-local SQLite truth.
- `research_execution`
  Strategy-study artifacts whose `study_mode` is `research_execution_mode`; these remain separate from baseline replay evidence.
- `historical_playback`
  Legacy or backfilled playback artifacts where benchmark-vs-research semantics are not explicit enough to classify more tightly.

## Lifecycle Truth Classes

The analytics surface publishes an explicit lifecycle-truth class per lane. These labels are shown separately from lane type so benchmark, paper, and research lanes are not over-normalized.

- `FULL_LIFECYCLE_TRUTH`
  End-to-end lifecycle truth is available for the lane contract being inspected. In replay studies this maps to enriched execution truth. In paper-runtime lanes this maps to tracked runtime ledger truth from lane-local intents, fills, and state snapshots, or the tracked runtime contract when those previews are partially unavailable.
- `HYBRID_ENTRY_BASELINE_EXIT_TRUTH`
  The lane publishes authoritative entry detail, but still depends on baseline-style exit truth for part of the lifecycle. This is useful, but not equivalent to a full-lifecycle execution study.
- `BASELINE_ONLY`
  The lane reflects baseline or compatibility replay truth only. This includes baseline-parity replay studies and older playback artifacts that do not publish richer lifecycle metadata.
- `UNSUPPORTED`
  The requested execution semantics are not implemented or not supportable for the observed strategy family / artifact, so the lane should not be treated as authoritative lifecycle truth.

## Truth Boundaries

- Benchmark / replay evidence comes from `outputs/historical_playback`, including:
  - historical playback manifests
  - replay summary artifacts
  - `*.strategy_study.json` and `*.strategy_study.md`
- Paper-runtime evidence comes from:
  - `outputs/operator_dashboard/paper_strategy_performance_snapshot.json`
  - `outputs/operator_dashboard/paper_tracked_strategies_snapshot.json`
  - `outputs/operator_dashboard/paper_tracked_strategy_details_snapshot.json`
  - lane-local SQLite stores referenced by the active paper lanes
- Research-execution evidence comes from strategy-study artifacts with `study_mode=research_execution_mode`.
- Lifecycle-truth labeling comes from:
  - replay / research studies: `strategy_study.meta.pnl_truth_basis`, `entry_model_capabilities`, `authoritative_execution_events`, and `authoritative_trade_lifecycle_records`
  - paper runtime: tracked paper lane contracts, tracked strategy detail snapshots, and lane-local SQLite evidence previews
  - legacy playback compatibility loads: fallback classification when older artifacts do not publish explicit lifecycle metadata

The unified surface reads these sources through adapters. It does not overwrite them, net them together, or collapse provenance.

## Metrics Support

Universally surfaced when the underlying lane supports them:

- net P/L
- realized P/L
- trade count
- long trades / short trades
- winners / losers
- win rate
- average trade
- max drawdown
- session breakdown
- latest trade summary
- latest status
- latest update timestamp

Lane-specific or partial support:

- open P/L
  Available when the lane publishes trusted open-position mark/reference truth.
- profit factor
  Supported on paper-runtime lanes and on replay/research lanes when the artifact exposes a complete priced closed-trade path. Replay support now includes older `closed_trade_breakdown` variants and newly published study-summary closed-trade rows when those rows still carry enough trade pricing truth. It remains unavailable when artifacts only expose aggregate P/L without enough persisted closed-trade pricing truth.
- trade family breakdown
  Available when replay artifacts expose closed-trade family labels or when paper-runtime attribution rows / trade logs expose persisted family labels. Tracked paper strategies now also publish exact per-family breakdown rows directly in their tracked summaries when the underlying tracked trade log is available. Recent-trade preview fallback is still reserved for cases where full coverage can be proven.
- session breakdown
  Available from paper strategy-performance session buckets when present, from tracked-paper exact summary rows when the tracked trade log is available, and otherwise from full-history closed-trade session phases when the adapter can prove complete trade coverage. Partial recent-trade previews do not get promoted into a full session breakdown.
- lifecycle-truth class
  Available on every lane, but classification quality depends on the published contract. Legacy playback rows may fall back to compatibility labeling when explicit execution-truth metadata is absent.

When a metric is not supportable yet, the analysis surface marks it unavailable and includes the blocking artifact or persistence gap instead of synthesizing a value.

## Drill-Down Sources

The surface is built so an operator can choose strategy, choose run / lane, and inspect evidence without raw SQLite digging.

- Replay / benchmark lanes expose bars, signals, order intents, fills, ATP readiness context, and execution slices through linked strategy-study artifacts.
- Paper-runtime lanes expose recent bars, signals, order intents, fills, state snapshots, and reconciliation/session evidence through tracked detail artifacts and lane-local SQLite truth.
- Readiness / session evidence is kept explicit and separate from economic metrics.

## Known Gaps

- Some replay artifacts still lack a fully priced closed-trade event path, so replay profit factor remains unavailable for those runs even though other aggregate metrics may be present.
- Strategy-study summaries now publish closed-trade, family, session, and latest-trade rows when replay source truth supports them, but older already-written artifacts will still rely on adapter reconstruction.
- Some paper-runtime rows still load from tracked strategy summaries without full lane-local previews; those lanes keep full paper provenance, but drill-down evidence can be partial.
- Trade-family breakdown depends on family labels in replay trade events or paper trade logs. Older artifacts may only expose aggregate trade counts.
- Paper family and session breakdowns now fall back to tracked recent-trade previews only when the preview length matches the persisted trade count. When the preview is only a rolling window, the analytics surface keeps those metrics unavailable instead of implying full-history coverage.
- Legacy playback artifacts can still load, but lifecycle-truth classification may fall back to `BASELINE_ONLY` when no explicit execution-truth metadata was persisted.

## Compatibility And Guardrails

- ATP Companion Baseline v1 semantics are unchanged.
- Replay / benchmark evidence remains separate from paper-runtime evidence.
- Mode and timeframe truth stay explicit on every lane.
- Legacy replay / study artifacts are adapted forward into the unified surface without rewriting source artifacts.
