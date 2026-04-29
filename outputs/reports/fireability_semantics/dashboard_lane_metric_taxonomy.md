# Dashboard Lane Metric Taxonomy

- `Runtime Lanes Loaded`: lane is present in the active runtime.
- `Data Fresh Lanes`: lane has a usable completed-bar timestamp and is not effectively stale.
- `Governance Allowed Lanes`: lane is not halted, faulted, reconciling, or entry-disabled.
- `Route Ready Lanes`: lane has an allowed route destination.
- `Session Eligible Lanes`: lane is loaded, governed, route-ready, and inside its trading session.
- `Waiting For Completed Bar`: lane is session-eligible but between completed decision bars.
- `Setup Evaluated / No Setup`: the audit examined the latest bar and found no setup.
- `Actionable Now`: BUY/SELL/EXIT exists on a completed decision bar and gates pass.
- `Blocked Lanes`: operationally blocked for a real reason.
- `Ready This Bar`: lane is eligible on the exact current completed decision bar. This is stricter than open-to-trade.