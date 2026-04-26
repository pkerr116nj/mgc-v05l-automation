from __future__ import annotations

import argparse
import json
from pathlib import Path

from mgc_v05l.research.asia_drift.position_sizing_monte_carlo import run_position_sizing_monte_carlo


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="asia-drift-position-sizing-monte-carlo")
    parser.add_argument(
        "--trade-log-csv",
        type=Path,
        default=Path(
            "outputs/reports/asia_drift_shadow_trading/index_stack_20250101_20260421/reports/"
            "asia_drift_shadow_daily_trade_log.csv"
        ),
        help="Corrected shadow trade log CSV.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/reports/asia_drift_position_sizing_monte_carlo/index_stack_20250101_20260421"),
        help="Output directory for Monte Carlo artifacts.",
    )
    parser.add_argument("--starting-capital", type=float, default=100000.0, help="Starting capital in dollars.")
    parser.add_argument("--simulations", type=int, default=5000, help="Number of Monte Carlo simulations.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    payload = run_position_sizing_monte_carlo(
        trade_log_csv=args.trade_log_csv,
        output_dir=args.output_dir,
        starting_capital=args.starting_capital,
        simulations=args.simulations,
        seed=args.seed,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
