# Blocking Ambiguities From Current Spec Set

The release candidate resolved most of the earlier implementation blockers. The following items remain not explicit enough to implement parity-sensitive behavior without guessing:

1. Exit-condition implementation work
   - `kLongIntegrityLost`, `shortIntegrityLost`, and `vwapWeakFollowThrough` are now defined in the release candidate.
   - Full exit-engine parity is still pending implementation work and remains a parity-sensitive area to validate carefully once coded.

2. Fill timing parity in live mode
   - Replay fill behavior is locked to `NEXT_BAR_OPEN`.
   - Live-order timing relative to bar close and broker availability still needs explicit documentation once the live adapter is implemented.

3. Broker adapter payload schemas
   - The build documents define required interface methods, but the exact payload structures for order status, open orders, broker position snapshots, and account health are still integration-specific and intentionally open.
