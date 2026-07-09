"""CLI for REF1 offline research experiment engine."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_offline_research_experiment_engine import (
    DEFAULT_ENRICHMENTS_PATH,
    DEFAULT_OUTCOMES_PATH,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_RA9_HYPOTHESES_PATH,
    DEFAULT_TRADE_PATHS_PATH,
    create_experiment,
    run_offline_research_experiments,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run offline diagnostic research experiments over canonical trade artifacts.")
    subparsers = parser.add_subparsers(dest="command")

    def add_common(sub: argparse.ArgumentParser) -> None:
        sub.add_argument("--outcomes-path", type=Path, default=DEFAULT_OUTCOMES_PATH)
        sub.add_argument("--enrichments-path", type=Path, default=DEFAULT_ENRICHMENTS_PATH)
        sub.add_argument("--trade-paths-path", type=Path, default=DEFAULT_TRADE_PATHS_PATH)
        sub.add_argument("--ra9-hypotheses-path", type=Path, default=DEFAULT_RA9_HYPOTHESES_PATH)
        sub.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
        sub.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")

    create = subparsers.add_parser("create-experiment", help="Create a deterministic experiment JSON payload.")
    create.add_argument("--experiment-id", required=True)
    create.add_argument("--title", required=True)
    create.add_argument("--hypothesis", required=True)
    create.add_argument("--experiment-type", default="CUSTOM")
    create.add_argument("--description", default="")
    create.add_argument("--now")

    for command in ("list-experiments", "show-experiment", "run-experiment", "run-sample-experiments", "export-result", "publish-ref1-artifacts"):
        sub = subparsers.add_parser(command)
        add_common(sub)
        if command in {"show-experiment", "run-experiment", "export-result"}:
            sub.add_argument("--experiment-id")

    add_common(parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    command = args.command or "run-sample-experiments"
    if command == "create-experiment":
        from datetime import UTC, datetime

        now = datetime.fromisoformat(args.now.replace("Z", "+00:00")) if args.now else datetime.now(UTC)
        experiment = create_experiment(
            experiment_id=args.experiment_id,
            title=args.title,
            description=args.description,
            hypothesis=args.hypothesis,
            experiment_type=args.experiment_type,
            generated_at=now,
        )
        print(json.dumps(experiment, sort_keys=True))
        return 0

    result = run_offline_research_experiments(
        outcomes_path=args.outcomes_path,
        enrichments_path=args.enrichments_path,
        trade_paths_path=args.trade_paths_path,
        ra9_hypotheses_path=args.ra9_hypotheses_path,
        output_dir=args.output_dir,
        now=args.now,
    )
    if command in {"show-experiment", "run-experiment", "export-result"} and args.experiment_id:
        experiments = [row for row in result.experiments if row.get("experiment_id") == args.experiment_id]
        results = [row for row in result.results if row.get("experiment_id") == args.experiment_id]
        print(json.dumps({"experiments": experiments, "results": results}, sort_keys=True))
        return 0
    print(
        json.dumps(
            {
                "schema_version": result.summary.get("schema_version"),
                "experiment_count": result.summary.get("experiment_count"),
                "result_count": result.summary.get("result_count"),
                "population_rows": result.summary.get("population_rows"),
                "status_counts": result.summary.get("status_counts"),
                "sample_experiments_path": str(result.sample_experiments_path),
                "sample_results_path": str(result.sample_results_path),
                "diagnostic_only": result.summary.get("diagnostic_only"),
                "production_recommendation": result.summary.get("production_recommendation"),
                "trading_gate": result.summary.get("trading_gate"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
