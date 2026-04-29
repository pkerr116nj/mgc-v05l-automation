# Local vs IBKR Paper Trade Separation

- broker-path routed lanes: `10`
- local/internal-only lanes: `35`
- aggregate broker-path pnl: `-794.00`
- aggregate local/internal-only pnl: `6022.50`

- broker-path paper performance is the only bucket that should influence near-term live-money probation decisions.
- local legacy paper trades remain valuable for signal discovery and runtime stress, but they must not be counted as broker-path proof.
