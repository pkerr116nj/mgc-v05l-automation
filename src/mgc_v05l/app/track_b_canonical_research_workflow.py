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
    accept_claim_draft,
    export_claim_draft_summary,
    generate_claim_drafts,
    load_claim_draft,
    load_claim_drafts,
    load_workflow,
    publish_rwf1_artifacts,
    publish_rwf2_artifacts,
    publish_rwf3_artifacts,
    reject_claim_draft,
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
    generate_drafts = subparsers.add_parser("generate-drafts")
    generate_drafts.add_argument("--investigation-id", required=True)

    list_drafts = subparsers.add_parser("list-drafts")
    list_drafts.add_argument("--investigation-id")

    show_draft = subparsers.add_parser("show-draft")
    show_draft.add_argument("--draft-id", required=True)

    accept_draft = subparsers.add_parser("accept-draft")
    accept_draft.add_argument("--draft-id", required=True)

    reject_draft = subparsers.add_parser("reject-draft")
    reject_draft.add_argument("--draft-id", required=True)

    export_drafts = subparsers.add_parser("export-draft-summary")
    export_drafts.add_argument("--investigation-id", required=True)

    subparsers.add_parser("publish-rwf3-artifacts")
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
    if args.command == "generate-drafts":
        drafts = generate_claim_drafts(investigation_id=args.investigation_id, investigation_output_dir=args.output_dir / "investigations", evidence_output_dir=args.output_dir / "evidence", draft_output_dir=args.output_dir / "claim_drafts", now=args.now)
        _print({"investigation_id": args.investigation_id, "draft_count": len(drafts), "guardrails": {"diagnostic_only": True, "production_recommendation": False, "trading_gate": False}})
        return 0
    if args.command == "list-drafts":
        _print({"drafts": load_claim_drafts(investigation_id=args.investigation_id, draft_output_dir=args.output_dir / "claim_drafts")})
        return 0
    if args.command == "show-draft":
        _print(load_claim_draft(args.draft_id, draft_output_dir=args.output_dir / "claim_drafts"))
        return 0
    if args.command == "accept-draft":
        result = accept_claim_draft(args.draft_id, draft_output_dir=args.output_dir / "claim_drafts", evidence_output_dir=args.output_dir / "evidence", claims_output_dir=args.output_dir / "claims", investigation_output_dir=args.output_dir / "investigations", now=args.now)
        _print({"draft_id": args.draft_id, "draft_status": result["draft"]["draft_status"], "claim_id": result["claim"]["claim_id"], "claim_status": result["claim"]["status"], "guardrails": {"diagnostic_only": True, "production_recommendation": False, "trading_gate": False}})
        return 0
    if args.command == "reject-draft":
        draft = reject_claim_draft(args.draft_id, draft_output_dir=args.output_dir / "claim_drafts", investigation_output_dir=args.output_dir / "investigations", now=args.now)
        _print({"draft_id": args.draft_id, "draft_status": draft["draft_status"], "guardrails": draft["guardrails"]})
        return 0
    if args.command == "export-draft-summary":
        _print(export_claim_draft_summary(investigation_id=args.investigation_id, draft_output_dir=args.output_dir / "claim_drafts"))
        return 0
    if args.command == "publish-rwf3-artifacts":
        _print(publish_rwf3_artifacts(output_dir=args.output_dir, now=args.now))
        return 0
    raise AssertionError(f"Unhandled command: {args.command}")


def _print(payload: object) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
