"""CLI/status writer for Track B PAPER self-healing health.

The command is read-only with respect to broker/order/lifecycle state. It only
reads local PID/artifact state and writes the self-healing health status artifact.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.track_b_readiness_state import (
    DEFAULT_TRACK_B_EXPECTED_ACTIVE_ROOT,
    REPO_ROOT,
)
from mgc_v05l.execution_core.track_b_self_healing_supervisor import (
    DEFAULT_SELF_HEALING_HEALTH_ARTIFACT,
    build_track_b_self_healing_health,
    write_track_b_self_healing_health,
)


def run_track_b_self_healing_status(
    *,
    repo_root: Path = REPO_ROOT,
    expected_root: Path | None = None,
    output_path: Path | None = None,
    write: bool = True,
    process_probe=None,
    now=None,
) -> dict[str, Any]:
    repo_root = repo_root.expanduser().resolve()
    expected_root = (expected_root or DEFAULT_TRACK_B_EXPECTED_ACTIVE_ROOT).expanduser().resolve()
    health = build_track_b_self_healing_health(
        repo_root=repo_root,
        expected_root=expected_root,
        process_probe=process_probe,
        now=now,
    )
    target = output_path or (repo_root / DEFAULT_SELF_HEALING_HEALTH_ARTIFACT)
    health = {**health, "health_contract_artifact_path": str(target)}
    if write:
        write_track_b_self_healing_health(output_path=target, health=health)
    return health


def render_track_b_self_healing_status(health: Mapping[str, Any]) -> str:
    agents = health.get("agents") if isinstance(health.get("agents"), Mapping) else {}
    lines = [
        f"classification={health.get('classification') or 'UNKNOWN'}",
        f"auto_restart_allowed={str(health.get('auto_restart_allowed') is True).lower()}",
        f"restart_candidates={_csv(health.get('restart_candidates'))}",
        f"operator_required_agents={_csv(health.get('operator_required_agents'))}",
        f"blockers={_csv(health.get('blockers'))}",
        f"warnings={_csv(health.get('warnings'))}",
        f"live_money_eligible={str(health.get('live_money_eligible') is True).lower()}",
        f"artifact_path={health.get('health_contract_artifact_path') or DEFAULT_SELF_HEALING_HEALTH_ARTIFACT}",
        "agents:",
    ]
    for agent_id in sorted(agents):
        row = agents.get(agent_id) if isinstance(agents.get(agent_id), Mapping) else {}
        lines.append(
            "  "
            + str(agent_id)
            + f": health={row.get('health_state') or '-'}"
            + f" running={str(row.get('process_running') is True).lower()}"
            + f" restart_eligible={str(row.get('restart_eligible') is True).lower()}"
            + f" restart_candidate={str(row.get('restart_candidate') is True).lower()}"
            + f" blockers={_csv(row.get('blockers'))}"
            + f" restart_blockers={_csv(row.get('restart_blockers'))}"
        )
    return "\n".join(lines) + "\n"


def _csv(value: object) -> str:
    if isinstance(value, (list, tuple)):
        return ",".join(str(item) for item in value) if value else "none"
    if value in (None, ""):
        return "none"
    return str(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write/read Track B self-healing health status.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--expected-root", type=Path, default=None)
    parser.add_argument("--output-path", type=Path, default=None)
    parser.add_argument("--write", action="store_true", default=False)
    parser.add_argument("--no-write", action="store_false", dest="write")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    health = run_track_b_self_healing_status(
        repo_root=args.repo_root,
        expected_root=args.expected_root,
        output_path=args.output_path,
        write=args.write,
    )
    if args.json:
        print(json.dumps(health, indent=2, sort_keys=True))
    else:
        print(render_track_b_self_healing_status(health), end="")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
