# Observatory v1 architecture and operations

The Observatory is a display-only active-desktop prototype. It summarizes prepared market, venue, runtime, broker, research, and lifecycle evidence; it is not trading, broker, runtime, or strategy authority.

## Launch

Run from the Dev repository root:

```sh
desktop/prototypes/active-desktop/launch_observatory.sh
```

The launcher serves the prototype at `http://127.0.0.1:8765/` and opens the prepared Atlantic midday review scenario. Use `--no-open` to start or reuse the server without opening a browser. Do not launch `index.html` with `file://`; ordinary browsers will not load the ES module graph correctly. Add `debug=init` to the URL only when initialization timings and captured errors are needed.

## Refresh service

Generate one display frame with:

```sh
node desktop/prototypes/active-desktop/observatory_refresh_service.mjs --once
```

Run the cadence-controlled refresh loop with:

```sh
node desktop/prototypes/active-desktop/observatory_refresh_service.mjs
```

The service uses a single-process lock, runs the existing read-only producers at their declared cadences, and publishes freshness and failure status. Its guardrails explicitly deny trading, broker, runtime, strategy, and research-runtime authority.

## Source and generated artifacts

Source files, fixtures, validation, the launcher, and map-layout logic are reviewed and versioned. Files matching `*.generated.mjs` are runtime products written by the refresh service or its producers. They are local evidence snapshots, may be stale, and are intentionally excluded from the v1 source commit. A missing or invalid prepared artifact must degrade to `MISSING`, `UNKNOWN`, or fixture-backed presentation; it must never invent authoritative state.

## Visual and state semantics

- Green: source-supported open/tradable state or healthy system evidence.
- Red: source-supported closed/holiday state or an explicitly invalid condition.
- Amber: stale or valid-with-warning evidence; it is not an inferred open/closed state.
- Gray: unknown, unavailable, unsupported, or not-ready evidence.

Market-center halo radius and opacity are state styling only. Halo circles do not use CSS/SVG filters because Safari scales SVG filter lengths differently from Chrome in the transformed world canvas.

## Market-center model

The ambient map exposes one dot and one label per city/market center. Venue detail appears only in that center's Context Panel. North America is modeled as New York (NYSE, Nasdaq), Chicago (CME, Cboe/CFE as applicable), Toronto (TSX), and Mexico City (BMV). Cboe is ambiently associated with Chicago, not New York. Mexico City is primary but its color remains entirely source-authoritative.

City aggregation selects the strongest known venue state without allowing an implicit reducer seed to turn an all-closed center into `UNKNOWN`. An actual `UNKNOWN` venue remains uncertainty and may outrank closed presentation. CME product aggregation is stricter: only monitored families with configured calendars participate; any tradable monitored family yields `OPEN`, every participating family definitively closed yields `CLOSED`, and insufficient or conflicting truth yields `UNKNOWN`. Unmonitored unsupported families do not contaminate the result.

## Context Panel

Market-center, venue, market-domain, tape, pipeline, and lower-system targets open contextual evidence without changing Observatory state. A market-center panel lists its constituent venues, source freshness, benchmarks where available, and CME product-family sessions for Chicago. The panel is explanatory and read-only; it does not submit orders or mutate producer state.

## Deferred v2 work

- Add automated native-4K Chrome/Safari visual regression capture to supplement ordinary-browser acceptance.
- Expand product-specific CME calendar authority, including currently unsupported crypto and single-stock-futures schedules.
- Revisit Pacific/Bering projection continuity only as an explicit geography project; do not couple it to peripheral limb lighting.
- Define a packaged production transport for prepared evidence if the prototype graduates beyond local HTTP and generated ES modules.
- Continue accessibility and responsive-layout verification outside the fixed 3840×2160 acceptance target.
