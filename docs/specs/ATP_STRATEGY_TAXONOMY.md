# ATP Strategy Taxonomy

## Purpose

This note defines the strategy-status taxonomy for ATP-related lanes and packages.

It exists to keep three things explicit:
- frozen benchmark truth is preserved
- benchmark-local assumptions do not become repo-wide law by accident
- ATP-derived candidates can advance on merit without pretending to be the benchmark

## Categories

### Benchmark Lane

Definition:
- frozen comparison anchor
- stable semantics
- benchmark-local assumptions only
- not repo-wide platform law

Current occupant:
- `ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only`

Implications:
- used as a reference baseline for replay and paper comparison
- candidate success does not rewrite this lane automatically
- benchmark replacement requires explicit promotion

### Research Candidate Lane

Definition:
- ATP-derived or ATP-adjacent lane under replay or research evaluation
- explicit identity and provenance
- not benchmark truth

Examples:
- `promotion_1_075r_favorable_only`
- `atp_companion_v1__candidate_gc_asia_us`

### Paper Candidate Lane

Definition:
- explicit paper/runtime candidate lane
- allowed to run in paper monitoring, soak, or operator review
- still not benchmark truth

Examples:
- `atp_companion_v1_gc_asia_us`
- other explicit ATP candidate paper configs

### Production-Track Candidate

Definition:
- candidate package good enough to advance toward production consideration
- may include explicit governance wrappers
- remains separate from the frozen benchmark lane
- does not imply benchmark replacement

Current documented example:
- `ATP Companion Candidate v1 — GC / Asia + US Executable, London Diagnostic-Only`
- package:
  - narrow `US_LATE` safeguard
  - outer halt-only `$3,000` governance

### Benchmark Promotion

Definition:
- rare event
- explicit decision to replace or redefine the benchmark lane
- never implied by candidate success alone

Required posture:
- benchmark replacement must be deliberate, documented, and provenance-preserving

## Language Rules

Preferred language:
- `frozen benchmark lane`
- `benchmark-local assumptions`
- `ATP-derived candidate lane`
- `paper candidate`
- `production-track candidate package`
- `benchmark replacement requires explicit promotion`

Avoid language that implies:
- benchmark assumptions are universal ATP law
- non-benchmark candidates are disqualified for not matching benchmark-local assumptions
- candidate success automatically changes the benchmark

## Practical Interpretation

- The benchmark still matters.
- The benchmark does not govern all future ATP research.
- Candidates are evaluated on economics, operational validity, and provenance.
- Governance overlays can shape a production-track package without being misrepresented as core benchmark improvements.
