# Observatory v1 architecture and operations

## Accepted baseline

- Repository: `/Users/patrick/Dev/MGC-v05l-automation`
- Branch: `track-b-paper-execution-core`
- Accepted commit: `812c167f2d3de7abe3b2c86b4a09007179752c14`
- Status: **ACCEPTED / CLOSED**
- Acceptance date: 2026-08-09
- Prototype: `desktop/prototypes/active-desktop/`

Ordinary Chrome and Safari both reached `OBSERVATORY_READY` during final acceptance. A normal launch must render the world, market tape, and system flow without displaying initialization diagnostics or producing captured initialization errors/unhandled rejections. Chrome-only success is insufficient.

## Product role and authority boundary

The Observatory is a display and observability surface. It consumes prepared, source-attributed state; it does not become an authority source itself. It has no trade authority, broker mutation authority, submit authority, cancel/close/`placeOrder` authority, or live-money eligibility.

The broader Track B provenance boundary remains in force: research evidence may inform reviewed research outputs, while runtime and broker truth remain separately authoritative for execution. Observatory display artifacts must not bridge research evidence into runtime authority.

## Launch and refresh

Run from the accepted Dev repository root:

```sh
desktop/prototypes/active-desktop/launch_observatory.sh
```

The launcher serves `http://127.0.0.1:8765/` and opens the prepared Atlantic midday review scenario. Use `--no-open` to start or reuse the server without opening a browser. Do not launch with `file://`; ordinary browsers will not initialize the ES module graph correctly. Add `debug=init` only for initialization timing/error diagnosis.

Generate one display frame:

```sh
node desktop/prototypes/active-desktop/observatory_refresh_service.mjs --once
```

Run the cadence-controlled refresh loop:

```sh
node desktop/prototypes/active-desktop/observatory_refresh_service.mjs
```

The refresh service uses a single-process lock, runs existing read-only producers at declared cadences, and publishes freshness/failure status. Missing, invalid, or stale prepared evidence degrades visibly to `MISSING`, `UNKNOWN`, `STALE`, or fixture-backed presentation as implemented; it must never invent authoritative state. Service guardrails deny trading, broker, runtime, strategy, and research-runtime authority.

## Accepted visual baseline

The v1 world is an open map over black or near-black space with a restrained star field. Geography renders freely rather than through a complete globe aperture, clipping mask, or artificial oval. There is no top or bottom planetary arc. Only two subtle, independent Pacific peripheral limb-light cues imply planetary curvature.

Coastlines are subdued and cool. Solar/day-night illumination is restrained. Atlantic-overlap warmth is atmospheric, not a literal line or beam. The bottom system river and market tape remain distinct visual systems. These are product-level design contracts, not requirements to preserve incidental CSS constants.

Color semantics are:

- Green: source-supported open/tradable state or healthy system evidence.
- Red: source-supported closed/holiday state or an explicitly invalid condition.
- Amber: stale, degraded, abnormal, uncertain, or valid-with-warning evidence as defined by the model.
- Gray: unknown, unavailable, unsupported, or not-ready evidence.

Normal differences among product schedules are not inherently abnormal or degraded. Market-center halo radius and opacity are presentation only. Halo circles avoid CSS/SVG filters because Safari scales filtered SVG content differently from Chrome in the transformed world canvas.

## Market-center and label model

The ambient map normally exposes one marker and one label per geographic market center. Multiple venues belonging to one center appear in that center's Context Panel rather than as overlapping ambient dots.

Featured North American centers are:

- Chicago: CME and Cboe/CFE as applicable.
- New York: NYSE and Nasdaq.
- Toronto: TSX.
- Mexico City: BMV.

Montreal is not a primary ambient center. Cboe is associated with Chicago, not New York. Mexico City is independently session-aware and may legitimately differ from US or Canadian centers because of Mexican holidays and local trading schedules; primary status never overrides authoritative session truth.

City labels remain anchored to transformed geographic coordinates. Small bounded local displacement may resolve collisions; arbitrary screen-position offsets are not the accepted model.

## Context interaction

Selecting a new target replaces Context Panel content in place. The user does not need to close the panel between selections—for example: New York → Gold → Magic Runtime → ES → Chicago. Closing the panel returns to the ambient view.

Market-center, venue, market-domain, tape, pipeline, and lower-system selections explain evidence without changing Observatory or producer state. A market-center panel lists constituent venues, freshness/source details, benchmarks where available, and CME product-family sessions for Chicago. It cannot submit orders or mutate runtime state.

## Venue and product-session semantics

`venue_operational_state` and `product_session_states` are separate concepts. The first is not inferred from unsupported product detail. CME ambient session aggregation uses only monitored product families with configured source calendars:

- Any monitored supported family `OPEN`, `AUCTION`, or `CLOSING_SOON` → ambient CME `OPEN`.
- All monitored supported families definitively `CLOSED`/`HOLIDAY` → ambient CME `CLOSED`.
- Closed supported families plus a monitored `UNKNOWN` → ambient CME `UNKNOWN`.
- Unsupported or unmonitored `UNKNOWN` families do not contaminate supported-family truth.
- No supported product-family truth → `UNKNOWN`.

At the city layer, aggregation selects the strongest actual venue state without allowing an implicit reducer seed to turn an all-closed center into `UNKNOWN`. A real `UNKNOWN` venue remains uncertainty and may outrank closed presentation. Richer source-authoritative CME crypto and single-stock-futures schedules remain deferred.

## Performance architecture

World rendering is separated into independently invalidated conceptual layers: base/coastline, solar, venue/session, labels, tape, and lower flow/atmospheric systems. Snapshot polling must not imply a complete world redraw. Solar rendering updates on its own time/input invalidation rather than every normal evidence poll.

Avoid expensive per-cell or per-element filtering, repeated clipping/masking, and browser-sensitive compositing. Cross-browser Chrome/Safari behavior is part of correctness, not a follow-up polish concern.

## Source and generated evidence

Reviewed source, fixtures, validation, launcher, and map-layout logic are versioned. Files matching `*.generated.mjs` are operational evidence written by the refresh service or its producers. They can be stale or environment-specific, are not canonical source, and were intentionally excluded from the accepted v1 source commit. Do not stage them merely to reproduce a documentation or source change.

## Acceptance baseline

Final acceptance covered:

- CME aggregation truth-table validation.
- Venue-session unit tests.
- The 27-scenario Observatory validator.
- JavaScript syntax checks.
- `git diff --check`.
- Ordinary Chrome and Safari initialization through local HTTP.
- `OBSERVATORY_READY`, normal tape/system-flow rendering, hidden diagnostics in normal presentation, and no captured initialization errors or unhandled rejections.

Accepted validation commands:

```sh
env PYTHONPATH=src ./.venv/bin/python -c 'from mgc_v05l.execution_core.observatory_global_venue_session import _aggregate_product_session_status as aggregate; row=lambda state, monitored=True, source="CMES": {"monitored": monitored, "calendar_source": source, "product_session_state": state}; assert aggregate([])=="UNKNOWN"; assert aggregate([row("UNKNOWN", False, None)])=="UNKNOWN"; assert aggregate([row("CLOSED"), row("HOLIDAY")])=="CLOSED"; assert aggregate([row("CLOSED"), row("UNKNOWN")])=="UNKNOWN"; assert aggregate([row("OPEN"), row("UNKNOWN")])=="OPEN"; assert aggregate([row("CLOSED"), row("UNKNOWN", False, None)])=="CLOSED"'
env PYTHONPATH=src ./.venv/bin/python -m pytest -q tests/unit/execution_core/test_observatory_global_venue_session.py
node --check desktop/prototypes/active-desktop/app.mjs
node --check desktop/prototypes/active-desktop/observatory_map_layout.mjs
node desktop/prototypes/active-desktop/validate.mjs
git diff --check
```

Instrumented browser acceptance used:

```text
http://127.0.0.1:8765/?scenario=ATLANTIC_BRIDGE_MIDDAY&venues=prepared&debug=init&acceptance=v1-final-closeout
```

## Deferred v2 backlog

These are deferred enhancements, not unresolved v1 acceptance defects:

- Automated native-4K Chrome/Safari visual regression capture.
- Richer source-authoritative CME crypto session handling.
- Richer source-authoritative single-stock-futures session handling.
- Pacific/Bering projection continuity and geography refinement.
- Packaged evidence transport beyond local HTTP/generated modules.
- Broader accessibility validation.
- Broader responsive-layout and platform validation.

## Future-development guardrail

Future Observatory work must preserve these accepted v1 contracts unless a task explicitly changes one. Do not casually reopen full-globe/aperture experiments, duplicate venue dots at one market center, arbitrary label offsets, browser-sensitive expensive SVG/CSS filters, generated runtime evidence as canonical source, or any Observatory trading authority.

Future visual work should be incremental, independently reversible, and validated in ordinary Chrome and Safari.
