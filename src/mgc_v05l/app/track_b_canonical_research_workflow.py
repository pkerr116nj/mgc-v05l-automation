"""CLI for Canonical Research Workflow RWF1."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_canonical_research_workflow import (
    DEFAULT_OUTPUT_DIR,
    activate_conclusion,
    archive_session,
    attach_session_reference,
    complete_session,
    create_sample_morning_gold_session,
    create_session,
    create_workflow,
    conclusion_review_readiness,
    list_workflows,
    accept_claim_draft,
    activate_claim,
    claim_review_readiness,
    export_session_summary,
    export_claim_draft_summary,
    export_claim_review_summary,
    export_review_summary,
    fail_session,
    generate_claim_drafts,
    generate_claim_review_queue,
    generate_conclusion_reviews,
    load_claim_draft,
    load_claim_drafts,
    load_claim_review,
    load_claim_reviews,
    load_review,
    load_reviews,
    load_session,
    load_workflow,
    list_sessions,
    publish_rwf1_artifacts,
    publish_rwf2_artifacts,
    publish_rwf3_artifacts,
    publish_rwf4_artifacts,
    publish_rwf5_artifacts,
    publish_rwf6_artifacts,
    reject_claim,
    reject_claim_draft,
    reject_conclusion,
    run_workflow,
    sample_morning_gold_review,
    start_session,
    summarize_workflow,
    validate_workflow,
    write_session,
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
    review_queue = subparsers.add_parser("list-review-queue")
    review_queue.add_argument("--investigation-id")

    show_review = subparsers.add_parser("show-review")
    show_review.add_argument("--review-id", required=True)

    validate_review = subparsers.add_parser("validate-claim-for-review")
    validate_review.add_argument("--claim-id", required=True)

    activate = subparsers.add_parser("activate-claim")
    activate.add_argument("--claim-id", required=True)
    activate.add_argument("--reviewer", default="operator")
    activate.add_argument("--operator-approved", action="store_true")

    reject_claim_cmd = subparsers.add_parser("reject-claim")
    reject_claim_cmd.add_argument("--claim-id", required=True)
    reject_claim_cmd.add_argument("--reviewer", default="operator")

    review_summary = subparsers.add_parser("export-review-summary")
    review_summary.add_argument("--investigation-id", required=True)

    subparsers.add_parser("publish-rwf4-artifacts")
    list_reviews = subparsers.add_parser("list-reviews")
    list_reviews.add_argument("--investigation-id")

    list_conclusion_review = subparsers.add_parser("list-conclusion-review")
    list_conclusion_review.add_argument("--investigation-id", required=True)

    show_conclusion_review = subparsers.add_parser("show-conclusion-review")
    show_conclusion_review.add_argument("--review-id", required=True)

    activate_conclusion_cmd = subparsers.add_parser("activate-conclusion")
    activate_conclusion_cmd.add_argument("--conclusion-id", required=True)
    activate_conclusion_cmd.add_argument("--reviewer", default="operator")
    activate_conclusion_cmd.add_argument("--operator-approved", action="store_true")

    reject_conclusion_cmd = subparsers.add_parser("reject-conclusion")
    reject_conclusion_cmd.add_argument("--conclusion-id", required=True)
    reject_conclusion_cmd.add_argument("--reviewer", default="operator")

    conclusion_review_summary = subparsers.add_parser("export-conclusion-review")
    conclusion_review_summary.add_argument("--investigation-id", required=True)

    conclusion_review_summary_alias = subparsers.add_parser("export-conclusion-review-summary")
    conclusion_review_summary_alias.add_argument("--investigation-id", required=True)

    validate_conclusion = subparsers.add_parser("validate-conclusion")
    validate_conclusion.add_argument("--conclusion-id", required=True)

    validate_investigation_for_conclusion = subparsers.add_parser("validate-investigation-for-conclusion")
    validate_investigation_for_conclusion.add_argument("--investigation-id", required=True)

    subparsers.add_parser("publish-rwf5-artifacts")
    create_session_cmd = subparsers.add_parser("create-session")
    create_session_cmd.add_argument("--title", required=True)
    create_session_cmd.add_argument("--description", required=True)
    create_session_cmd.add_argument("--session-type", default="CUSTOM")
    create_session_cmd.add_argument("--owner", default="operator")
    create_session_cmd.add_argument("--tag", action="append", default=[])
    create_session_cmd.add_argument("--session-id")
    create_session_cmd.add_argument("--sample-morning-gold-session", action="store_true")

    start_session_cmd = subparsers.add_parser("start-session")
    start_session_cmd.add_argument("--session-id", required=True)

    attach_cmd = subparsers.add_parser("attach")
    attach_cmd.add_argument("--session-id", required=True)
    attach_cmd.add_argument("--reference-type", required=True)
    attach_cmd.add_argument("--target-id", required=True)
    attach_cmd.add_argument("--target-kind", required=True)
    attach_cmd.add_argument("--target-path")
    attach_cmd.add_argument("--relationship", default="contains")

    complete_session_cmd = subparsers.add_parser("complete-session")
    complete_session_cmd.add_argument("--session-id", required=True)
    complete_session_cmd.add_argument("--with-warnings", action="store_true")

    fail_session_cmd = subparsers.add_parser("fail-session")
    fail_session_cmd.add_argument("--session-id", required=True)
    fail_session_cmd.add_argument("--reason")

    archive_session_cmd = subparsers.add_parser("archive-session")
    archive_session_cmd.add_argument("--session-id", required=True)

    subparsers.add_parser("list-sessions")

    show_session = subparsers.add_parser("show-session")
    show_session.add_argument("--session-id", required=True)

    export_session = subparsers.add_parser("export-session-summary")
    export_session.add_argument("--session-id", required=True)

    subparsers.add_parser("publish-rwf6-artifacts")
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
    if args.command == "list-review-queue":
        if args.investigation_id:
            generate_claim_review_queue(investigation_id=args.investigation_id, claims_output_dir=args.output_dir / "claims", review_output_dir=args.output_dir / "claim_reviews", investigation_output_dir=args.output_dir / "investigations", now=args.now)
        _print({"reviews": load_claim_reviews(investigation_id=args.investigation_id, review_output_dir=args.output_dir / "claim_reviews")})
        return 0
    if args.command == "show-review":
        try:
            _print(load_review(args.review_id, review_output_dir=args.output_dir / "reviews"))
        except FileNotFoundError:
            _print(load_claim_review(args.review_id, review_output_dir=args.output_dir / "claim_reviews"))
        return 0
    if args.command == "validate-claim-for-review":
        from mgc_v05l.execution_core.track_b_canonical_claims_engine import load_claim

        claim = load_claim(args.claim_id, output_dir=args.output_dir / "claims")
        _print({"claim_id": args.claim_id, "readiness": claim_review_readiness(claim), "guardrails": {"diagnostic_only": True, "production_recommendation": False, "trading_gate": False}})
        return 0
    if args.command == "activate-claim":
        if not args.operator_approved:
            _print({"claim_id": args.claim_id, "status": "FAILED_SAFE", "reason": "operator_approval_required", "guardrails": {"diagnostic_only": True, "production_recommendation": False, "trading_gate": False}})
            return 2
        result = activate_claim(args.claim_id, operator_approved=True, reviewer=args.reviewer, claims_output_dir=args.output_dir / "claims", review_output_dir=args.output_dir / "claim_reviews", investigation_output_dir=args.output_dir / "investigations", now=args.now)
        _print({"claim_id": args.claim_id, "claim_status": result["claim"]["status"], "review_status": result["review"]["review_status"], "guardrails": result["review"]["guardrails"]})
        return 0
    if args.command == "reject-claim":
        record = reject_claim(args.claim_id, reviewer=args.reviewer, claims_output_dir=args.output_dir / "claims", review_output_dir=args.output_dir / "claim_reviews", investigation_output_dir=args.output_dir / "investigations", now=args.now)
        _print({"claim_id": args.claim_id, "review_status": record["review_status"], "guardrails": record["guardrails"]})
        return 0
    if args.command == "export-review-summary":
        _print(export_claim_review_summary(investigation_id=args.investigation_id, review_output_dir=args.output_dir / "claim_reviews"))
        return 0
    if args.command == "publish-rwf4-artifacts":
        _print(publish_rwf4_artifacts(output_dir=args.output_dir, now=args.now))
        return 0
    if args.command == "list-reviews":
        _print({"reviews": load_reviews(investigation_id=args.investigation_id, review_output_dir=args.output_dir / "reviews")})
        return 0
    if args.command == "list-conclusion-review":
        generate_conclusion_reviews(investigation_id=args.investigation_id, conclusions_output_dir=args.output_dir / "conclusions", review_output_dir=args.output_dir / "reviews", investigation_output_dir=args.output_dir / "investigations", now=args.now)
        _print({"reviews": load_reviews(investigation_id=args.investigation_id, review_output_dir=args.output_dir / "reviews")})
        return 0
    if args.command == "show-conclusion-review":
        _print(load_review(args.review_id, review_output_dir=args.output_dir / "reviews"))
        return 0
    if args.command == "validate-conclusion":
        from mgc_v05l.execution_core.track_b_canonical_conclusions_engine import load_conclusion

        conclusion = load_conclusion(args.conclusion_id, output_dir=args.output_dir / "conclusions")
        _print({"conclusion_id": args.conclusion_id, "readiness": conclusion_review_readiness(conclusion), "guardrails": {"diagnostic_only": True, "production_recommendation": False, "trading_gate": False}})
        return 0
    if args.command == "validate-investigation-for-conclusion":
        reviews = generate_conclusion_reviews(investigation_id=args.investigation_id, conclusions_output_dir=args.output_dir / "conclusions", review_output_dir=args.output_dir / "reviews", investigation_output_dir=args.output_dir / "investigations", now=args.now)
        summary = export_review_summary(investigation_id=args.investigation_id, review_output_dir=args.output_dir / "reviews")
        _print({"investigation_id": args.investigation_id, "review_count": len(reviews), "readiness_counts": summary["readiness_counts"], "guardrails": summary["guardrails"]})
        return 0
    if args.command == "activate-conclusion":
        if not args.operator_approved:
            _print({"conclusion_id": args.conclusion_id, "status": "FAILED_SAFE", "reason": "operator_approval_required", "guardrails": {"diagnostic_only": True, "production_recommendation": False, "trading_gate": False}})
            return 2
        result = activate_conclusion(args.conclusion_id, operator_approved=True, reviewer=args.reviewer, conclusions_output_dir=args.output_dir / "conclusions", review_output_dir=args.output_dir / "reviews", investigation_output_dir=args.output_dir / "investigations", now=args.now)
        _print({"conclusion_id": args.conclusion_id, "conclusion_status": result["conclusion"]["status"], "review_status": result["review"]["review_status"], "guardrails": result["review"]["guardrails"]})
        return 0
    if args.command == "reject-conclusion":
        record = reject_conclusion(args.conclusion_id, reviewer=args.reviewer, conclusions_output_dir=args.output_dir / "conclusions", review_output_dir=args.output_dir / "reviews", investigation_output_dir=args.output_dir / "investigations", now=args.now)
        _print({"conclusion_id": args.conclusion_id, "review_status": record["review_status"], "guardrails": record["guardrails"]})
        return 0
    if args.command in {"export-conclusion-review", "export-conclusion-review-summary"}:
        _print(export_review_summary(investigation_id=args.investigation_id, review_output_dir=args.output_dir / "reviews"))
        return 0
    if args.command == "publish-rwf5-artifacts":
        _print(publish_rwf5_artifacts(output_dir=args.output_dir, now=args.now))
        return 0
    if args.command == "create-session":
        if args.sample_morning_gold_session:
            session = create_sample_morning_gold_session(output_dir=args.output_dir, now=args.now)
        else:
            session = create_session(title=args.title, description=args.description, session_type=args.session_type, owner=args.owner, tags=args.tag, session_id=args.session_id, output_dir=args.output_dir / "sessions", now=args.now).session
        _print({"session_id": session["session_id"], "status": session["status"], "guardrails": session["guardrails"]})
        return 0
    if args.command == "start-session":
        session = start_session(args.session_id, output_dir=args.output_dir / "sessions", now=args.now)
        _print({"session_id": session["session_id"], "status": session["status"], "guardrails": session["guardrails"]})
        return 0
    if args.command == "attach":
        session = load_session(args.session_id, output_dir=args.output_dir / "sessions")
        session = attach_session_reference(
            session,
            reference_type=args.reference_type,
            target_id=args.target_id,
            target_kind=args.target_kind,
            target_path=args.target_path,
            relationship=args.relationship,
            now=args.now,
        )
        result = write_session(session, output_dir=args.output_dir / "sessions")
        _print({"session_id": result.session["session_id"], "reference_count": len(result.session["references"]), "guardrails": result.session["guardrails"]})
        return 0
    if args.command == "complete-session":
        session = complete_session(args.session_id, output_dir=args.output_dir / "sessions", now=args.now, with_warnings=args.with_warnings)
        _print({"session_id": session["session_id"], "status": session["status"], "guardrails": session["guardrails"]})
        return 0
    if args.command == "fail-session":
        session = fail_session(args.session_id, output_dir=args.output_dir / "sessions", now=args.now, reason=args.reason)
        _print({"session_id": session["session_id"], "status": session["status"], "guardrails": session["guardrails"]})
        return 0
    if args.command == "archive-session":
        session = archive_session(args.session_id, output_dir=args.output_dir / "sessions", now=args.now)
        _print({"session_id": session["session_id"], "status": session["status"], "guardrails": session["guardrails"]})
        return 0
    if args.command == "list-sessions":
        _print({"sessions": list_sessions(output_dir=args.output_dir / "sessions")})
        return 0
    if args.command == "show-session":
        _print(load_session(args.session_id, output_dir=args.output_dir / "sessions"))
        return 0
    if args.command == "export-session-summary":
        _print(export_session_summary(args.session_id, output_dir=args.output_dir / "sessions"))
        return 0
    if args.command == "publish-rwf6-artifacts":
        _print(publish_rwf6_artifacts(output_dir=args.output_dir, now=args.now))
        return 0
    raise AssertionError(f"Unhandled command: {args.command}")


def _print(payload: object) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
