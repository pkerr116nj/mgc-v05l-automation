# Track B Application Policy Doctrine

This doctrine is the default architectural posture for Track B application
policy work. It exists to keep future changes aligned with the shared-services
control plane instead of drifting back toward local script checks, hidden
recovery behavior, or routine human gates in PAPER mode.

## Core Doctrine

1. PAPER and LIVE are first-class policy modes.

   PAPER and LIVE should consume the same shared truth/control-plane evidence
   where possible, but they must not share the same action policy. A behavior
   that is useful in PAPER can be too permissive for LIVE; a human gate that is
   correct for LIVE can suppress useful learning in PAPER.

2. PAPER is for bounded autonomous failure discovery.

   PAPER is an autonomous, bounded, observable, evidence-rich, failure-tolerant
   learning environment. Its job is to expose abnormal behavior, classify it,
   preserve artifacts, test recovery paths, and improve the architecture.
   Routine operator acknowledgement must not become a core PAPER dependency.

3. LIVE is for capital preservation and stricter ambiguity handling.

   Future LIVE mode should use the same shared truth kernel but a more
   conservative policy adapter. Ambiguous broker/order/lifecycle evidence,
   repeated unsafe stops, crash-loop behavior, and uncertain recovery should be
   treated more conservatively when capital is at risk.

4. Safety means preventing uncontrolled behavior, not suppressing all abnormal
   behavior.

   Abnormal behavior in PAPER is a signal. The system should classify it,
   preserve evidence, and choose bounded next steps. Safety boundaries prevent
   uncontrolled cascades, duplicate writers, broad operations, hidden mutation,
   and live-money leakage; they should not hide useful failure candidates behind
   indefinite human holds.

5. Shared `execution_core` truth is the system kernel.

   Track B authority lives in reusable `execution_core` services. Broker truth,
   reconciliation, Open Order Truth, Managed Order Registry, Position Truth,
   Runtime Environment Truth, Managed Position Registry, Agent Health, Proof
   Readiness, Runtime Resume Semantics, PAPER Recovery Policy, and Runtime
   Supervisor Authority form the control-plane kernel. Dashboard/operator
   artifacts are projections only.

6. Mutation-capable paths must consume shared authority evidence.

   Any path that can submit, cancel, close, replace, modify, repair lifecycle
   state, rewrite ownership, or start/stop runtime generations must first
   consume fresh shared authority evidence. Local reconstruction from scattered
   broker snapshots, lifecycle files, or dashboard artifacts is not sufficient.

7. Recovery should be policy-driven, not hard-coded script behavior.

   Scripts should execute narrowly scoped actions only after policy services
   classify what is allowed, blocked, advisory, or unsafe. Recovery posture
   belongs in explicit policy authorities, not in ad hoc shell branches or
   one-off remediation assumptions.

8. PAPER favors bounded recovery budgets over operator holds.

   PAPER crash-loop, preflight-failure, and suspicious-state handling should
   prefer bounded retry budgets, cooldowns, quarantine-observe states,
   evidence refresh, and exact scoped recovery eligibility. Human gates may
   exist as temporary policy adapters or exceptional overrides, but not as the
   ordinary PAPER recovery spine.

9. Runtime observability and provenance are core product features.

   Runtime state, launch attempts, stop provenance, recovery decisions,
   generation identity, source commit, broker-safe-at-stop, and shared truth
   snapshots are part of the product. A failure without durable evidence is a
   product defect.

10. No hidden recovery.

    Recovery must be visible, classified, and auditable. Every recovery attempt
    needs a decision artifact, bounded target identity, input evidence summary,
    prohibited-action list, and result artifact. The system must never silently
    mutate broker/order/lifecycle/runtime state.

11. Single runtime writer is a top-tier invariant.

    Duplicate runtime writers are a hard unsafe condition in PAPER and LIVE.
    Recovery and launch paths must fail closed on duplicate writer evidence
    until the conflict is resolved by exact identity and fresh shared truth.

12. Runtime state should be generation-scoped.

    Runtime instance id, restart generation, source commit, config identity,
    stop provenance, control actions, and recovery decisions should be tied to
    a generation. Stale control actions from older generations must not affect a
    newer runtime generation.

13. Distinguish unsafe-to-trade from unsafe-to-observe.

    Many states are unsafe for submit/restart but still safe and valuable for
    read-only observation, evidence refresh, broker polling, classification, and
    artifact preservation. PAPER policy should make this distinction explicit.

14. LIVE mode should eventually be policy-swappable over the same shared truth
    control plane.

    The goal is not two separate systems. The goal is a shared evidence kernel
    with mode-specific policy adapters. PAPER can be recovery-oriented and
    failure-tolerant; LIVE can be capital-preserving and ambiguity-averse.

15. Track B is an autonomous supervisory control system with trading attached.

    Track B should not be treated as a pile of scripts. Scripts are thin entry
    points around shared authorities, policy decisions, and bounded executors.
    The architecture is the control plane; trading behavior is one attached
    application of that control plane.

## Hard Safeguards In PAPER

PAPER autonomy does not remove safety invariants. These remain hard:

- no live-money route
- no broad cancel
- no broad flatten
- no uncontrolled duplicate cascades
- no duplicate runtime writers
- no hidden recovery
- no broker/order/lifecycle mutation without fresh shared authority evidence
- no dashboard projection as authority
- no stale evidence as mutation permission
- no `paper_proof` bypass
- no pretending suspicious, contradictory, or review-required state is clean
- no bypassing broker/order identity checks

## PAPER Recovery Stance

PAPER recovery policy should answer, "What can the system safely learn next?"
before it asks, "What should a human approve?"

Preferred PAPER outcomes:

- `OBSERVE`
- `REFRESH_EVIDENCE`
- `AUTONOMOUS_RETRY_ELIGIBLE`
- `SCOPED_RECOVERY_ELIGIBLE`
- `QUARANTINE_OBSERVE_ONLY`
- `HARD_UNSAFE_HOLD`

`HARD_UNSAFE_HOLD` remains valid for top-tier invariants such as live-money
eligibility, duplicate runtime writers, broad-action risk, or uncontrolled
cascade risk. It should not be the default response to every abnormal PAPER
state.

## LIVE Policy Direction

LIVE/PRE-LIVE should eventually use a separate policy adapter over the same
shared truth kernel. It can require stronger acknowledgement, tighter ambiguity
controls, smaller recovery budgets, and stricter mutation permissions. That
future conservatism should not be back-projected into PAPER as routine human
gating.

## Design Rule

New Track B code should first ask which shared authority service owns the
evidence and which policy mode owns the action interpretation. If the answer is
"this script rebuilds the truth locally," the design is probably drifting away
from the control-plane architecture.
