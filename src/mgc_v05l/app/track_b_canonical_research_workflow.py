"""CLI for Canonical Research Workflow RWF1."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_canonical_research_workflow import (
    DEFAULT_OUTPUT_DIR,
    create_workflow,
    list_workflows,
    load_workflow,
    publish_rwf1_artifacts,
    publish_rwf2_artifacts,
    run_workflow,
    sample_morning_gold_review,
    summarize_workflow,
    validate_workflow,
    write_workflow,
)
from mgc_v05l.execution_core.track_b_canonical_investigation_engine import (
    load_investigation,
    summarize_investigation,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage diagnostic Canonical Research Workflows.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--now")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create = subparsers.add_parser("create")
    create.add_argument("--title", required=True)
    create.add_argument("--description", required=True)
    create.add_argument("--workflow-type", default="CUSTOM")
    create.add_argument("--owner", default="operator")
    create.add_argument("--tag", action="append", default=[])
    create.add_argument("--workflow-id")
    create.add_argument("--sample-morning-gold-review", action="store_true")

    subparsers.add_parser("list")

    show = subparsers.add_parser("show")
    show.add_argument("--workflow-id", required=True)

    validate = subparsers.add_parser("validate")
    validate.add_argument("--workflow-id", required=True)

    run = subparsers.add_parser("run")
    run.add_argument("--workflow-id")
    run.add_argument("--sample-morning-gold-review", action="store_true")

    run_population = subparsers.add_parser("run-population")
    run_population.add_argument("--workflow-id")
    run_population.add_argument("--sample-morning-gold-review", action="store_true")

    export = subparsers.add_parser("export-summary")
    export.add_argument("--workflow-id", required=True)

    show_investigation = subparsers.add_parser("show-investigation")
    show_investigation.add_argument("--investigation-id", required=True)

    export_investigation = subparsers.add_parser("export-investigation")
    export_investigation.add_argument("--investigation-id", required=True)

    subparsers.add_parser("publish-rwf1-artifacts")
    subparsers.add_parser("publish-rwf2-artifacts")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "create":
        if args.sample_morning_gold_review:
            workflow = sample_morning_gold_review(now=args.now, output_dir=args.output_dir)
            result = write_workflow(workflow, output_dir=args.output_dir)
        else:
            result = create_workflow(
                title=args.title,
                description=args.description,
                workflow_type=args.workflow_type,
                owner=args.owner,
                tags=args.tag,
                workflow_id=args.workflow_id,
                now=args.now,
                output_dir=args.output_dir,
            )
        _print({"workflow_id": result.workflow["workflow_id"], "path": str(result.workflow_path), "summary_path": str(result.summary_path)})
        return 0
    if args.command == "list":
        _print({"workflows": list_workflows(output_dir=args.output_dir)})
        return 0
    if args.command == "show":
        _print(load_workflow(args.workflow_id, output_dir=args.output_dir))
        return 0
    if args.command == "validate":
        _print(validate_workflow(load_workflow(args.workflow_id, output_dir=args.output_dir)))
        return 0
    if args.command == "run":
        workflow = sample_morning_gold_review(now=args.now, output_dir=args.output_dir) if args.sample_morning_gold_review else load_workflow(args.workflow_id, output_dir=args.output_dir)
        run_record = run_workflow(workflow, output_dir=args.output_dir, now=args.now)
        _print({
            "run_id": run_record["run_id"],
            "workflow_id": run_record["workflow_id"],
            "status": run_record["status"],
            "step_count": len(run_record["step_results"]),
            "artifact_ref_count": len(run_record["artifact_refs"]),
            "guardrails": run_record["guardrails"],
        })
        return 0
    if args.command == "run-population":
        workflow = sample_morning_gold_review(now=args.now, output_dir=args.output_dir) if args.sample_morning_gold_review or not args.workflow_id else load_workflow(args.workflow_id, output_dir=args.output_dir)
        run_record = run_workflow(workflow, output_dir=args.output_dir, now=args.now)
        investigation_id = ""
        for step in reversed(run_record["step_results"]):
            details = step.get("details") or {}
            if details.get("investigation_id"):
                investigation_id = str(details["investigation_id"])
                break
        _print({
            "run_id": run_record["run_id"],
            "workflow_id": run_record["workflow_id"],
            "status": run_record["status"],
            "investigation_id": investigation_id,
            "artifact_ref_count": len(run_record["artifact_refs"]),
            "guardrails": run_record["guardrails"],
        })
        return 0
    if args.command == "export-summary":
        _print(summarize_workflow(load_workflow(args.workflow_id, output_dir=args.output_dir)))
        return 0
    if args.command == "show-investigation":
        _print(load_investigation(args.investigation_id, output_dir=args.output_dir / "investigations"))
        return 0
    if args.command == "export-investigation":
        _print(summarize_investigation(load_investigation(args.investigation_id, output_dir=args.output_dir / "investigations")))
        return 0
    if args.command == "publish-rwf1-artifacts":
        _print(publish_rwf1_artifacts(output_dir=args.output_dir, now=args.now))
        return 0
    if args.command == "publish-rwf2-artifacts":
        _print(publish_rwf2_artifacts(output_dir=args.output_dir, now=args.now))
        return 0
    raise AssertionError(f"Unhandled command: {args.command}")


def _print(payload: object) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
