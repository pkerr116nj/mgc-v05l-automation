# Unsupported Instrument Scope Plan

- unsupported local-paper lanes: `35`
- unsupported recent traders: `29`
- by instrument: `{"ES": 8, "MES": 7, "MNQ": 10, "NQ": 10}`
- recent ES/MES traders: `14`
- recent NQ/MNQ traders: `15`
- recommended next scope: `MNQ/NQ`

## Minimal Expansion Work

- qualify exact IBKR contracts for the chosen index-futures scope
- add bridge execution mappings and contract target metadata
- add exposure attribution for the new underlying
- refresh governance and lane-port tests for the new scope
- keep all new lanes paper-only until broker-path truth is proven
