# IBKR MGC Orphan Position Resolution

- classification: `PAPER_ORPHAN_POSITION_RESTORED_TO_ATP`
- account: `DUM882026`
- exact contract: `MGC 20260626 / conId=712565978 / localSymbol=MGCM6`
- strategy restored: `ATP_COMPANION_V1_ASIA_US`
- broker quantity: `1.0`
- ledger quantity: `1.0`
- side: `LONG`
- average entry price: `4586.7`
- permId: `490708968`
- executionId: `0000e1a7.69f1fa35.01.01`
- source intent id: `acb55729-ca64-4ef5-b9d4-8892edcdb94f`
- broker minus ledger difference: `0.0`
- orphan exposure: `0.0`
- no order was submitted in this pass
- no flatten was performed in this pass

## Conclusion

The current broker `+1.0 MGC` paper position is not a true orphan. It matches preserved ATP bridge ownership evidence from the earlier bridge-created paper fill. The lost attribution was restored into the persistent ledger with a paper-only adoption event.

## Proof Chain

- prior monitor audit contained repeated `strategy_ownership_resolved` events with `classification=adopted`
- prior executor/loop artifacts carried the same ATP-owned position with:
  - `strategy_id=ATP_COMPANION_V1_ASIA_US`
  - `permId=490708968`
  - `executionId=0000e1a7.69f1fa35.01.01`
  - `quantity=1.0 LONG`
- the latest broker truth still shows the same exact `MGC` contract and `+1.0` quantity
- the restored ledger now attributes that broker lot back to ATP

## Updated State

- `var/paper_strategy_position_ledger.json` now carries ATP ownership again
- exposure attribution is clean again:
  - broker net `MGC = 1.0`
  - strategy-attributed `MGC = 1.0`
  - broker minus ledger difference `= 0.0`
  - orphan exposure `= 0.0`
- `paper_orphan_reconciliation_adoption` was appended to the monitor audit

## Remaining Blocker

The orphan-position problem is resolved, but the continuous monitor service wrapper is still unreliable. A stale/dead runtime monitor process can continue to leave disconnected/stale gate state for governance even after the ledger is corrected. So lane porting should remain paused until the hands-off monitor service path is repaired or cleanly restarted.
