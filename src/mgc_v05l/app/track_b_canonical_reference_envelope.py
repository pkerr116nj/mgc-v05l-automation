"""CLI for IE5 Canonical Reference and Provenance envelopes."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_canonical_claims_engine import DEFAULT_OUTPUT_DIR as DEFAULT_CLAIMS_DIR
from mgc_v05l.execution_core.track_b_canonical_claims_engine import list_claims, load_claim
from mgc_v05l.execution_core.track_b_canonical_conclusions_engine import DEFAULT_OUTPUT_DIR as DEFAULT_CONCLUSIONS_DIR
from mgc_v05l.execution_core.track_b_canonical_conclusions_engine import load_conclusion
from mgc_v05l.execution_core.track_b_canonical_evidence_engine import DEFAULT_OUTPUT_DIR as DEFAULT_EVIDENCE_DIR
from mgc_v05l.execution_core.track_b_canonical_evidence_engine import list_evidence, load_evidence
from mgc_v05l.execution_core.track_b_canonical_reference_envelope import (
    DEFAULT_OUTPUT_DIR,
    build_reference,
    missing_reference_summary,
    publish_ie5_artifacts,
    validate_reference,
    verify_research_chain,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate Canonical References and verify research chains.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--claims-dir", type=Path, default=DEFAULT_CLAIMS_DIR)
    parser.add_argument("--evidence-dir", type=Path, default=DEFAULT_EVIDENCE_DIR)
    parser.add_argument("--conclusions-dir", type=Path, default=DEFAULT_CONCLUSIONS_DIR)
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate-reference")
    validate.add_argument("--reference-json", required=True)

    verify = subparsers.add_parser("verify-chain")
    verify.add_argument("--conclusion-id", required=True)

    summary = subparsers.add_parser("export-reference-summary")
    summary.add_argument("--investigation-id")

    subparsers.add_parser("publish-ie5-artifacts")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "validate-reference":
        reference = json.loads(Path(args.reference_json).read_text(encoding="utf-8"))
        print(json.dumps(validate_reference(reference), indent=2, sort_keys=True))
        return 0
    if args.command == "verify-chain":
        conclusion = load_conclusion(args.conclusion_id, output_dir=args.conclusions_dir)
        claims = {row["claim_id"]: load_claim(row["claim_id"], output_dir=args.claims_dir) for row in list_claims(output_dir=args.claims_dir)}
        evidence = {row["evidence_id"]: load_evidence(row["evidence_id"], output_dir=args.evidence_dir) for row in list_evidence(output_dir=args.evidence_dir)}
        print(json.dumps(verify_research_chain(conclusion, claims_by_id=claims, evidence_by_id=evidence), indent=2, sort_keys=True))
        return 0
    if args.command == "export-reference-summary":
        claim_rows = list_claims(output_dir=args.claims_dir, investigation_id=args.investigation_id)
        evidence_rows = list_evidence(output_dir=args.evidence_dir, investigation_id=args.investigation_id)
        refs = [
            build_reference(reference_type="claim_summary", target_id=row["claim_id"], target_kind="CLAIM", relationship="references")
            for row in claim_rows
        ] + [
            build_reference(reference_type="evidence_summary", target_id=row["evidence_id"], target_kind="EVIDENCE", relationship="references")
            for row in evidence_rows
        ]
        print(json.dumps(missing_reference_summary(refs, {ref["target_id"]: ref for ref in refs}), indent=2, sort_keys=True))
        return 0
    if args.command == "publish-ie5-artifacts":
        print(json.dumps(publish_ie5_artifacts(output_dir=args.output_dir), indent=2, sort_keys=True))
        return 0
    raise AssertionError(f"Unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
