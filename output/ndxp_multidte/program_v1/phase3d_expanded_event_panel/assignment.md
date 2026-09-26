Proceed with Phase 3D now. Do not perform another cost-estimation exercise.

Objective: expand the NDXP event-level research dataset so the next morphology analysis is based on the broad available CMBP-1 history rather than the 60-session diagnostic panel.

Acquisition universe

* Dataset: OPRA.PILLAR
* Schema: cmbp-1
* Parent: NDXP.OPT
* History: 2023-03-28 through 2026-09-24
* Both puts and calls
* 0 through 5 calendar DTE
* Capture the economically relevant vertical surface from approximately $2 credit through and including the first $10 spread
* Capture sufficient adjacent strikes to reconstruct 5-, 10-, and 20-point verticals
* Preserve the existing frozen benchmark: $5/$6/$7 credit, 2/3 calendar DTE, 10-point short-put verticals. Do not redefine or overwrite it.

Architecture priority

Optimize in this order:

1. correctness
2. elapsed time
3. operational simplicity
4. reasonable cost
5. byte efficiency

Do not repeat Phase 3C’s fragmented architecture. Do not create hundreds or thousands of tiny retrieval jobs merely to avoid downloading overlapping data. Prefer coarse session-level direct retrieval, with reasonable concurrency, even when that means downloading redundant data.

The previous direct-recovery exercise successfully downloaded ~3.68 GB using 60 direct requests. Use that architecture as the starting point rather than the 501-job batch architecture.

Cost

There is a $100 hard ceiling on new Databento spend for this acquisition. This is a safety ceiling, not a target and not an optimization objective.

Do not run another metadata cost estimator. Do not make hundreds of pricing calls. Do not delay acquisition attempting to minimize the bill.

If Databento’s API provides an inexpensive way to enforce/check the $100 ceiling without serial pricing, use it. Otherwise structure the acquisition in resumable session blocks, maintain cumulative downloaded/requested evidence, and stop rather than knowingly exceeding $100.

Acquisition strategy

Before writing a complicated selector, determine whether a broader session-level NDXP parent retrieval is operationally reasonable. If broad retrieval is simpler and the resulting volume is manageable, favor it and filter locally.

We would rather download tens of GB of reusable NDXP data efficiently than spend hours engineering a few-GB surgical acquisition.

Use direct historical retrieval rather than hundreds of Databento batch jobs.

Cache raw DBN/Zstd data immutably by session or another similarly coarse, resumable unit.

Reuse existing local data where doing so is trivial, but do not construct elaborate interval-subtraction logic merely to eliminate overlap.

The acquisition must be restartable without repurchasing/re-downloading successfully completed units.

Opening and path coverage

The research objective is no longer tied to a single 09:31 snapshot. We want to study when qualifying rich-credit structures emerge during the opening process.

Ensure the acquired data support high-resolution reconstruction of the opening opportunity window, approximately 09:30–10:00 ET.

Also retain sufficient subsequent path information for selected structures to determine compression, adverse excursion, recovery, target passage and expiration/settlement behavior. Do not assume every option contract needs full-session/full-life event data if a much simpler two-stage architecture—broad opening capture followed by selected-contract paths—substantially reduces unnecessary volume.

Important research semantics

Do not reject or downgrade candidate spreads because displayed leg naturals do not demonstrate a 20-lot complex-order fill.

For historical research, continue treating spread midpoint with modest adverse slippage as the primary execution proxy. Leg-natural, displayed-size and persistence models are robustness diagnostics.

Actual complex-order executability will be tested prospectively, beginning with size = 1 and scaling only after observed execution supports it.

Preserve quote sizes and trade/event information available in CMBP-1 because they remain useful explanatory variables.

What I want you to do now

Implement the acquisition using the simplest high-throughput architecture consistent with the above requirements, test the downloader locally/offline where appropriate, and then start the authorized acquisition.

Do not stop merely to ask whether a harmless amount of redundant data is acceptable. It is.

Do not stop because the acquisition is larger than Phase 3C. That is intentional.

Do not perform another cost-estimation project.

Stop only for a genuine blocker: inability to determine the required contracts, evidence that the $100 ceiling could be exceeded, authentication/provider failure that prevents acquisition, or a correctness problem that risks acquiring the wrong universe.

During acquisition, report concise progress: sessions completed / total, GB downloaded, elapsed time, failures, and any available cumulative cost information.

After acquisition, validate coverage and build the expanded event research panel. Then report the resulting number of sessions/candidates by year, DTE, side and opening-credit region before beginning parameter/morphology optimization.

Commit the acquisition code, tests, manifests and non-huge research artifacts. Do not commit raw market-data files. Do not modify live trading code.