# Trading Workstation And Operator Workflow

## Status And Scope

Current operating practice and directional principles.

This document records the durable philosophy for the human operator environment: workstation roles, trading applications, infrastructure separation, operator ergonomics, and future broker-host placement.

This document is not a hardware inventory, not a network diagram, not a migration authorization, not a live-trading approval, not a broker-authority document, and not a procurement plan.

## Current Operator Environment

### Mac Mini

The Mac mini is the primary operator workstation.

It currently hosts Trader Workstation (TWS), serves as the main environment for supervising the trading platform, and remains the current point of human interaction with IBKR/TWS.

### Two HP Mini Towers

The two HP mini towers near the operator run Thinkorswim (TOS).

They serve as dedicated market-viewing and client workstations. Their role is operator-facing, not general infrastructure hosting.

### Windows Systems

Windows systems are primarily client platforms in the current network.

Windows is not the preferred default for headless infrastructure services. TWS should not be described as currently running on Windows.

### Linux Hosts

Linux hosts are preferred for headless services, infrastructure, research services, monitoring, and dedicated server roles.

Atlas and Jupiter should remain distinct from the interactive operator workstation role.

## Separation Of Responsibilities

Operator-facing applications and headless infrastructure should be separated where practical.

Principles:

- The primary workstation should not become the default host for every server-side service.
- Infrastructure hosts should not be treated as interactive trading desktops merely because they have spare capacity.
- Broker connectivity, operator interaction, research computation, monitoring, and storage are distinct responsibilities.
- Service placement should reflect operational ownership rather than convenience alone.

## Why TWS Remains On The Mac Mini Today

TWS currently works as part of the operator's established workflow.

The Mac mini is already the primary supervised trading environment. Moving TWS merely for architectural neatness is not justified without a clear operational benefit.

A migration could introduce avoidable instability, session-management complexity, display or interaction changes, and broker-connectivity risk. The current placement should therefore be treated as the operating baseline, not as an error requiring immediate correction.

## Future TWS-Host Evaluation

TWS may eventually move off the Mac mini to a dedicated host.

That possibility is a future consideration, not an approved decision. The purpose would be to improve isolation, uptime, restart control, remote administration, and separation from the operator desktop.

Candidate hosting approaches may include:

- a dedicated Linux host if TWS support and operational reliability are satisfactory
- a dedicated Windows host if that provides the best supported broker environment
- another purpose-built dedicated system

No target host, operating system, or migration date is currently approved.

Any future decision should evaluate:

- IBKR supportability
- stable unattended operation
- API connectivity
- session and authentication behavior
- restart recovery
- remote supervision
- operator access
- rollback
- display requirements
- security
- failure isolation
- compatibility with PAPER and future live-readiness controls

## TOS Role

Thinkorswim is primarily a discretionary market-observation and client platform.

The two HP mini towers provide dedicated TOS capacity near the operator. TOS is not the Track B broker-authority surface, and TOS placement should not be confused with TWS/API infrastructure placement.

These systems may support market awareness and discretionary trading, but they do not gain Track B runtime authority.

## Operator Workflow Philosophy

The operator should be able to observe the market, platform state, broker state, and current authority without unnecessary context switching.

Critical operational information should be visible and understandable. Mission Control should reduce the need to inspect raw artifacts during normal operation.

Current role separation:

- TWS remains the broker interaction surface.
- TOS remains a market-observation and discretionary platform.
- Mission Control remains the operational-awareness surface.
- Research applications remain analysis surfaces.

These roles should stay visually and conceptually distinct.

## Human-In-The-Loop Operation

Patrick remains the final operator and product owner.

Manual intervention remains valid when safety, broker state, or system behavior requires it. Operator intervention should be attributable where practical.

Human supervision is not a substitute for deterministic runtime safety. Runtime safety should not assume the operator is always watching.

Operator override does not silently change architecture or authority hierarchy.

## Ergonomics And Resilience

Dedicated screens and workstations may improve focus and reduce contention.

The operator environment should avoid a single-machine failure taking down every observation and control surface where practical. Redundancy should be added only when it solves a real risk.

Simplicity and familiarity have operational value. The design should favor reliable recovery over elaborate workstation orchestration.

## Relationship To Infrastructure

Atlas owns market-data and Regime Monitor services after migration from Jupiter.

Jupiter remains an infrastructure and management host.

Neither Atlas nor Jupiter should automatically become the TWS host merely because they are Linux servers. NAS and archive hosts should not run interactive trading software by default.

The Mac mini should not become the long-term archive or general infrastructure dumping ground merely because it is the primary development workstation.

## Relationship To Mission Control And Decision Intelligence

Mission Control reports operational truth and current state.

Decision Intelligence may eventually provide advisory context.

TWS and TOS remain execution and market-observation applications. Advisory outputs must not be confused with broker controls.

Display proximity does not create authority.

## Security And Access Principles

Broker applications deserve tighter access control than ordinary dashboards.

Remote administration should be explicit and controlled. Credentials, sessions, and broker access should not be spread across hosts casually.

A future dedicated TWS host should minimize unnecessary software and services. Broker-host migration must preserve clear ownership and rollback.

## Decisions Intentionally Not Made

- No decision has been made to move TWS off the Mac mini.
- No target operating system has been selected.
- No dedicated TWS host has been assigned.
- No change has been approved to the current TOS workstation arrangement.
- No workstation change grants or alters Track B authority.

## Future Direction

Continue using the current operator arrangement while it remains reliable.

Evaluate TWS migration only when the expected improvement in uptime, isolation, or operational control justifies the migration cost and risk.

Preserve client, infrastructure, research, and broker-host role separation. Use the Architecture Track for any significant TWS-host or broker-connectivity redesign.

## Boundaries

This document does not authorize moving TWS.

It does not authorize broker API changes.

It does not change runtime, PAPER, live-money, or submit authority.

It does not define a procurement requirement.

Any material migration must have a baseline, design, validation plan, rollback plan, and explicit approval.

## Related Documents

- `SYSTEM_OVERVIEW.md`
- `PROJECT_HISTORY.md`
- `PROJECT_PRINCIPLES.md`
- `ENGINEERING_PROCESS.md`
- `docs/operations/current-infrastructure-baseline.md`
- `docs/architecture/mission-control-philosophy.md`
- `docs/architecture/decision-intelligence-vision.md`
- `docs/architecture/narratives/AN-001-physical-network-evolution.md`
- `docs/architecture/narratives/AN-002-infrastructure-evolution.md`
- `docs/architecture/narratives/AN-003-trading-platform-evolution.md`
- `docs/roadmap/PARKING_LOT.md`
