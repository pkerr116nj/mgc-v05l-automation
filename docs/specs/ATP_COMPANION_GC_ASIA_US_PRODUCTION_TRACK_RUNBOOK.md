# ATP Companion GC Asia+US Production-Track Runbook

Package:
- lane id: `atp_companion_v1_gc_asia_us_production_track`
- shared strategy identity: `ATP_COMPANION_V1_GC_ASIA_US_PRODUCTION_TRACK`
- config: `config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us_production_track.yaml`

Purpose:
- operate the admitted GC production-track paper package on the shared ATP-style paper runtime
- operate the exact frozen paper-pilot package with no tuning changes during the pilot window
- keep the frozen ATP benchmark unchanged
- keep the raw GC candidate separate from the admitted package

## Startup Path

1. Auth/bootstrap:
```bash
bash scripts/run_schwab_auth_gate.sh
```

2. Start the shared paper runtime on the exact package config:
```bash
bash scripts/run_probationary_paper_soak.sh --background \
  --config /Users/patrick/Documents/MGC-v05l-automation/config/base.yaml \
  --config /Users/patrick/Documents/MGC-v05l-automation/config/live.yaml \
  --config /Users/patrick/Documents/MGC-v05l-automation/config/probationary_pattern_engine.yaml \
  --config /Users/patrick/Documents/MGC-v05l-automation/config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us_production_track.yaml
```

3. Start or restart the dashboard on the same config stack:
```bash
MGC_PROBATIONARY_PAPER_CONFIG_PATHS="/Users/patrick/Documents/MGC-v05l-automation/config/base.yaml,/Users/patrick/Documents/MGC-v05l-automation/config/live.yaml,/Users/patrick/Documents/MGC-v05l-automation/config/probationary_pattern_engine.yaml,/Users/patrick/Documents/MGC-v05l-automation/config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us_production_track.yaml" \
bash scripts/run_operator_dashboard.sh --no-open-browser --verify-dashboard-api --host 127.0.0.1 --port 8790
```

## Pre-Open Readiness Checklist

- confirm `paper_config_in_force.json` shows only `atp_companion_v1_gc_asia_us_production_track`
- confirm dashboard row shows `GC`, not `MGC`
- confirm row classification is production-track/admitted paper, not benchmark
- confirm `runtime_attached = true`
- confirm shared strategy identity is `ATP_COMPANION_V1_GC_ASIA_US_PRODUCTION_TRACK`
- confirm participation remains staged and point value remains `100`
- confirm desk halt threshold is `-3000`
- confirm outer governance is halt-only, not flatten-and-halt

## Operator Controls

Lane-local controls through shared strategy identity:
```bash
bash scripts/run_probationary_operator_control.sh \
  --config /Users/patrick/Documents/MGC-v05l-automation/config/base.yaml \
  --config /Users/patrick/Documents/MGC-v05l-automation/config/live.yaml \
  --config /Users/patrick/Documents/MGC-v05l-automation/config/probationary_pattern_engine.yaml \
  --config /Users/patrick/Documents/MGC-v05l-automation/config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us_production_track.yaml \
  --action halt_entries \
  --shared-strategy-identity ATP_COMPANION_V1_GC_ASIA_US_PRODUCTION_TRACK
```

```bash
bash scripts/run_probationary_operator_control.sh \
  --config /Users/patrick/Documents/MGC-v05l-automation/config/base.yaml \
  --config /Users/patrick/Documents/MGC-v05l-automation/config/live.yaml \
  --config /Users/patrick/Documents/MGC-v05l-automation/config/probationary_pattern_engine.yaml \
  --config /Users/patrick/Documents/MGC-v05l-automation/config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us_production_track.yaml \
  --action resume_entries \
  --shared-strategy-identity ATP_COMPANION_V1_GC_ASIA_US_PRODUCTION_TRACK
```

```bash
bash scripts/run_probationary_operator_control.sh \
  --config /Users/patrick/Documents/MGC-v05l-automation/config/base.yaml \
  --config /Users/patrick/Documents/MGC-v05l-automation/config/live.yaml \
  --config /Users/patrick/Documents/MGC-v05l-automation/config/probationary_pattern_engine.yaml \
  --config /Users/patrick/Documents/MGC-v05l-automation/config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us_production_track.yaml \
  --action flatten_and_halt \
  --shared-strategy-identity ATP_COMPANION_V1_GC_ASIA_US_PRODUCTION_TRACK
```

Runtime-wide controlled stop for the single-lane exclusive runtime:
```bash
bash scripts/run_probationary_operator_control.sh \
  --config /Users/patrick/Documents/MGC-v05l-automation/config/base.yaml \
  --config /Users/patrick/Documents/MGC-v05l-automation/config/live.yaml \
  --config /Users/patrick/Documents/MGC-v05l-automation/config/probationary_pattern_engine.yaml \
  --config /Users/patrick/Documents/MGC-v05l-automation/config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us_production_track.yaml \
  --action stop_after_cycle
```

## Behavior Definitions

`halt_entries`
- stops new entries for this package
- does not flatten an existing position
- keeps runtime attached

`resume_entries`
- clears operator halt after review
- should only be used after inspecting the lane-local artifacts for the halt event

`flatten_and_halt`
- immediate operator intervention path
- flatten first if exposure is open
- then halt further entries

`stop_after_cycle`
- controlled shutdown path for the single-lane runtime
- runtime-wide, but safe because this config is exclusive to the admitted package

## Breach Day Definition

A breach day for this package means:
- session-scoped desk P/L for the exclusive package runtime reaches or breaches `-3000`
- the runtime halts new entries for the rest of that session
- it does not auto-flatten solely because of this outer governance threshold

Lane catastrophic open-loss still remains underneath this package and takes precedence over ordinary operator patience.

## After A Halt

Inspect:
- latest operator status
- latest fills and order intents
- latest reconciliation payload
- latest trade ledger rows
- whether the halt was operator-driven or desk-risk driven
- whether the package was in `US_LATE` and whether the narrow safeguard triggered

Artifacts to inspect first:
- `outputs/probationary_pattern_engine/paper_session/runtime/operator_status.json`
- `outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json`
- `outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_gc_asia_us_production_track/operator_status.json`
- `outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_gc_asia_us_production_track/trades.jsonl`
- `outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_gc_asia_us_production_track/events.jsonl`

## When To Re-Enable

Re-enable only when:
- reconciliation is clean
- no fault is active
- no unresolved open-order ambiguity remains
- the halt cause has been reviewed and accepted as non-disqualifying
- the session is still acceptable to continue under the package constitution

## Stop Path

Preferred:
- `stop_after_cycle`

Emergency:
- `flatten_and_halt`
- then confirm flat state before terminating the process if needed

This runbook is package-specific. It does not alter frozen ATP benchmark semantics.
