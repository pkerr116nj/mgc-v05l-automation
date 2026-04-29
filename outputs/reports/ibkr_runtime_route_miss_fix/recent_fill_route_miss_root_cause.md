# Recent Fill Route-Miss Root Cause

- module/function: `src/mgc_v05l/app/probationary_runtime.py` / `_build_probationary_paper_lanes`
- historical runtime path: submit-capable lanes were still constructed with ExecutionEngine(broker=PaperBroker())
- why local fills appeared: StrategyEngine._apply_due_replay_fills() only runs for PaperBroker lanes, which created the paper-* fills
- why submit-capable status was misleading: inventory/adapter status had been ported, but the live probationary paper runtime dispatcher had not been switched to an IBKR bridge-aware broker
