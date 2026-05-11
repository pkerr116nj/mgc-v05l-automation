# Track B Restored PAPER Runtime Package

This package restores previously admitted Track B / Track 2 PAPER evaluation lanes into one source-controlled Dev-root runtime config.

Safety invariants:
- PAPER only.
- `live_money_eligible=false` on every copied lane.
- No canary or paper_proof authority is enabled by this package.
- Submit remains guarded by existing route, monitor, governance, broker-flat/open-order, and account checks.

Evidence sources:
- `gc_forced_session_v3`: `outputs/reports/gc_mgc_forced_session_candidate_admission_archive_v3/gc_1x_all_lanes.paper_package.json` (5 lanes, package `gc_1x_all_lanes` / GC 1x All Lanes)
- `mgc_forced_session_v3`: `outputs/reports/gc_mgc_forced_session_candidate_admission_archive_v3/mgc_1x_all_lanes.paper_package.json` (5 lanes, package `mgc_1x_all_lanes` / MGC 1x All Lanes)
- `gc_asia_london_v2`: `outputs/reports/asia_london_participation_candidate_admission_archive_v2/gc_1x_asia_london_participation.paper_package.json` (2 lanes, package `gc_1x_asia_london_participation` / GC 1x Asia-London Participation)
- `mgc_asia_london_v2`: `outputs/reports/asia_london_participation_candidate_admission_archive_v2/mgc_1x_asia_london_participation.paper_package.json` (2 lanes, package `mgc_1x_asia_london_participation` / MGC 1x Asia-London Participation)
- `mnq_asia_london_v2`: `outputs/reports/asia_london_participation_candidate_admission_archive_v2/mnq_1x_asia_london_participation.paper_package.json` (3 lanes, package `mnq_1x_asia_london_participation` / MNQ 1x Asia-London Participation)
- `mnq_us_intraday_v3`: `outputs/reports/index_futures_forced_session_candidate_admission_archive_v3/mnq_1x_ny_early_core.paper_package.json` (7 lanes, package `mnq_1x_ny_early_core` / MNQ 1x US Intraday Core)

Total restored lanes: `24`

Included symbols:
- `GC`, `MGC`, `MNQ`

Explicitly excluded in this restore:
- `PL`, `ES`, `NQ`, `MES`, rates, research-only candidates, rejected-after-cost candidates, and live-money routes.
