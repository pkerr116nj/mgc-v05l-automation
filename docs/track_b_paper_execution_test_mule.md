# Track B PAPER Execution Test Mule V1

`Track B PAPER Execution Test Mule V1` is a reversible PAPER-only operational-throughput lane pair for proving the normal Track B execution chain:

`SignalPacket -> OrderIntent -> governance -> bridge -> broker -> lifecycle -> reconciliation`

It is intentionally non-production alpha. It is not a strategy promotion candidate and is not live-money eligible.

Labels:

- `PAPER_ONLY`
- `TEST_MULE`
- `OPERATIONAL_THROUGHPUT`
- `NON_PRODUCTION_ALPHA`
- `NOT_PROMOTION_ELIGIBLE`

## Scope

The generated mule lanes are:

- `track_b_paper_execution_test_mule_v1__mgc`
- `track_b_paper_execution_test_mule_v1__mnq`

The active switch is:

```yaml
probationary_paper_execution_test_mule_enabled: true
```

Set it to `false` and restart the PAPER runtime to remove the mule lanes.

## Entry And Exit

Entry is evaluated on completed 1m bars only:

- flat lane state required
- no open broker order required
- configured session restriction must match
- startup route hold must be released
- own-instrument Phase-1 freshness/provenance remains enforced by the existing runtime market-data path
- canonical readiness, broker-truth lease, reconciliation, governance, and bridge safety remain enforced by the existing Track B path
- candle body direction selects `LONG` or `SHORT`
- doji/tiny body bars below the configured tick threshold are skipped
- one open position per mule instrument
- one entry per instrument every configured frequency window

Exit is runtime-managed through the normal paper OrderIntent path:

- timed hold exit
- profit target exit
- max-loss protective exit

## Non-Goals

This is not a production strategy, not a research signal, not a global session-policy change, and not a broker shortcut. Diagnostic payloads and runtime labels are intentionally explicit so the lanes cannot be mistaken for promoted strategy logic.
