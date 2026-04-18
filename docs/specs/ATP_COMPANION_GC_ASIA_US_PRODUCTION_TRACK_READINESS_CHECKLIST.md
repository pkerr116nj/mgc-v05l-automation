# ATP Companion GC Asia+US Production-Track Readiness Checklist

Package:
- lane id: `atp_companion_v1_gc_asia_us_production_track`
- shared strategy identity: `ATP_COMPANION_V1_GC_ASIA_US_PRODUCTION_TRACK`
- symbol: `GC`

## Required Gates

`PASS` Package identity is explicit
- raw GC candidate remains separate
- admitted package has its own lane id and shared strategy identity
- benchmark semantics remain unchanged

`PASS` Shared paper runtime path exists
- package launches from `scripts/run_probationary_paper_soak.sh`
- package uses shared operator controls
- package uses shared dashboard/operator surface

`PASS` Outer governance is explicit
- desk halt threshold is `-3000`
- outer desk flatten is intentionally out of scope for this package
- lane catastrophic open-loss semantics remain active underneath

`PASS` Narrow safeguard is explicit
- package carries the lane-local `US_LATE` 2-bar no-traction + adverse safeguard
- safeguard is lane-local, not benchmark-global

`PASS` Operator-control targeting is explicit
- `halt_entries` targets the package by shared strategy identity
- `resume_entries` targets the package by shared strategy identity
- `flatten_and_halt` targets the package by shared strategy identity
- `stop_after_cycle` is documented as runtime-wide for the exclusive single-lane package runtime

`PASS` Bounded soak acceptance proof exists
- package config loads cleanly
- package-local operator status is written
- halt-only risk behavior is proven deterministically
- bounded paper-soak validation artifact is written

`NOT YET` Connected open-market soak duration completed
- recommendation: at least `2` open-market sessions
- recommendation: at least `1` session that includes `US_LATE`

`NOT YET` Realized paper package review completed
- review against package constitution thresholds before any broader production-track usage

## Non-Gates

These remain outside this package admission step:
- benchmark promotion
- repo-wide ATP rule change
- broad live automation
- broader multi-symbol adoption
