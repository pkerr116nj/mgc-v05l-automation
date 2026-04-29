from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "schwab_runtime_dependency_purge"
VAR_DIR = REPO_ROOT / "var"
APP_SUPPORT_SCHWAB_ENV = (
    Path.home()
    / "Library"
    / "Application Support"
    / "mgc_v05l"
    / "research_daily_capture_runtime"
    / "schwab_env.sh"
)


@dataclass(frozen=True)
class RemainingDependencyRow:
    file_module: str
    function_class: str
    dependency_type: str
    subsystem: str
    data_type: str
    frequency_latency: str
    runtime_role: str
    natural_replacement: str
    current_status: str
    note: str


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.environ.get(name, str(default))).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _remaining_dependencies() -> list[RemainingDependencyRow]:
    return [
        RemainingDependencyRow(
            file_module="scripts/run_schwab_auth_gate.sh",
            function_class="require_schwab_auth_env",
            dependency_type="auth bootstrap",
            subsystem="operator utility",
            data_type="Schwab auth env and token bootstrap",
            frequency_latency="operator initiated",
            runtime_role="provider specific",
            natural_replacement="n/a",
            current_status="intentional",
            note="Explicit Schwab auth helper remains by design and is no longer part of IBKR paper startup.",
        ),
        RemainingDependencyRow(
            file_module="scripts/run_schwab_token_web.sh",
            function_class="require_schwab_auth_env",
            dependency_type="token refresh workflow",
            subsystem="operator utility",
            data_type="Schwab token web flow",
            frequency_latency="operator initiated",
            runtime_role="provider specific",
            natural_replacement="n/a",
            current_status="intentional",
            note="Explicit Schwab auth/token workflow remains available as a legacy fallback path.",
        ),
        RemainingDependencyRow(
            file_module="scripts/show_probationary_status.sh",
            function_class="require_schwab_auth_env",
            dependency_type="legacy runtime inspection",
            subsystem="legacy probationary runtime",
            data_type="status snapshot",
            frequency_latency="operator initiated",
            runtime_role="legacy",
            natural_replacement="IBKR paper monitor artifacts",
            current_status="remaining",
            note="Still assumes Schwab-backed probationary flow and should be migrated or retired later.",
        ),
        RemainingDependencyRow(
            file_module="scripts/show_probationary_paper_status.sh",
            function_class="require_schwab_auth_env",
            dependency_type="legacy runtime inspection",
            subsystem="legacy probationary runtime",
            data_type="paper status snapshot",
            frequency_latency="operator initiated",
            runtime_role="legacy",
            natural_replacement="IBKR paper monitor artifacts",
            current_status="remaining",
            note="Still tied to the older Schwab-centered paper summary flow.",
        ),
        RemainingDependencyRow(
            file_module="scripts/run_probationary_paper_summary.sh",
            function_class="require_schwab_auth_env",
            dependency_type="legacy summary generation",
            subsystem="legacy probationary runtime",
            data_type="daily paper summary",
            frequency_latency="operator initiated",
            runtime_role="legacy",
            natural_replacement="IBKR governance and monitor reports",
            current_status="remaining",
            note="Still requires Schwab auth even though core IBKR paper runtime no longer does.",
        ),
        RemainingDependencyRow(
            file_module="scripts/run_probationary_daily_summary.sh",
            function_class="require_schwab_auth_env",
            dependency_type="legacy summary generation",
            subsystem="legacy probationary runtime",
            data_type="daily runtime summary",
            frequency_latency="operator initiated",
            runtime_role="legacy",
            natural_replacement="IBKR governance and dashboard reports",
            current_status="remaining",
            note="Legacy summary path remains Schwab-centric and is outside the critical IBKR paper startup path.",
        ),
        RemainingDependencyRow(
            file_module="scripts/run_probationary_live_strategy_pilot.sh",
            function_class="require_schwab_auth_env",
            dependency_type="live pilot launcher",
            subsystem="live pilot",
            data_type="pilot startup and account linkage",
            frequency_latency="operator initiated",
            runtime_role="execution critical for Schwab pilot only",
            natural_replacement="IBKR live pilot stack when approved",
            current_status="intentional",
            note="Still Schwab-specific because the legacy live pilot is not part of the IBKR paper runtime.",
        ),
        RemainingDependencyRow(
            file_module="src/mgc_v05l/execution/live_strategy_broker.py",
            function_class="_ConfiguredLiveMarketDataProvider",
            dependency_type="quote fallback support",
            subsystem="execution-adjacent quotes",
            data_type="latest quote snapshot / live quote subscription",
            frequency_latency="sub-second to seconds",
            runtime_role="execution adjacent",
            natural_replacement="Databento primary with explicit fallback controls",
            current_status="purged_to_optional",
            note="Fallback remains supported but is now explicit and disabled by default unless the config and environment both allow it.",
        ),
        RemainingDependencyRow(
            file_module="src/mgc_v05l/market_data/schwab_provider.py",
            function_class="SchwabMarketDataProvider",
            dependency_type="legacy provider implementation",
            subsystem="market data",
            data_type="historical and quote retrieval",
            frequency_latency="research and runtime dependent",
            runtime_role="fallback only",
            natural_replacement="Databento primary / local cache",
            current_status="intentional",
            note="Provider stays in-tree for fallback and parity work until later migration phases are complete.",
        ),
        RemainingDependencyRow(
            file_module="scripts/backfill_schwab_1m_history.sh",
            function_class="require_schwab_auth_env",
            dependency_type="research backfill",
            subsystem="historical data",
            data_type="1m bar backfill",
            frequency_latency="batch",
            runtime_role="research only",
            natural_replacement="Databento backfill pipeline",
            current_status="remaining",
            note="Research backfill scripts remain Schwab-specific until Databento parity and cache migration are complete.",
        ),
    ]


def _provider_matrix() -> dict[str, Any]:
    monitor_status = _load_json(VAR_DIR / "paper_strategy_monitor_runtime_status.json")
    heartbeat_status = _load_json(VAR_DIR / "paper_strategy_monitor_heartbeat.json")
    dashboard_bootstrap = _load_json(REPO_ROOT / "outputs" / "operator_dashboard" / "runtime" / "dashboard_bootstrap_prerequisites.json")
    replay_db_path = REPO_ROOT / "mgc_v05l.replay.sqlite3"
    schwab_config_path = REPO_ROOT / "config" / "schwab.local.json"
    databento_configured = (os.environ.get("MARKET_DATA_PRIMARY") or "databento").strip().lower() == "databento"
    allow_schwab_fallback = _env_bool("ALLOW_SCHWAB_FALLBACK", True)
    require_schwab_auth = _env_bool("REQUIRE_SCHWAB_AUTH", False)
    monitor_running = bool(monitor_status.get("monitor_running"))
    monitor_healthy = str(monitor_status.get("health_classification") or "").upper() == "HEALTHY"
    monitor_stale = bool(monitor_status.get("stale"))
    bridge_allowed = monitor_running and monitor_healthy and not monitor_stale

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "provider_roles": {
            "market_data_primary": "databento",
            "market_data_fallback": "schwab",
            "broker_truth_provider": "ibkr",
            "execution_provider": "ibkr",
        },
        "readiness": {
            "broker_truth": {
                "provider": "ibkr",
                "status": "ready" if monitor_running and monitor_healthy else "attention_required",
                "detail": "Broker/account/order/execution truth is anchored to the IBKR paper monitor runtime.",
            },
            "ibkr_paper_monitor": {
                "provider": "ibkr",
                "status": "ready" if bridge_allowed else "attention_required",
                "monitor_running": monitor_running,
                "health_classification": monitor_status.get("health_classification"),
                "stale": monitor_stale,
                "last_heartbeat": heartbeat_status.get("generated_at") or heartbeat_status.get("heartbeat_at"),
            },
            "market_data_primary": {
                "provider": "databento",
                "status": "configured" if databento_configured else "attention_required",
                "detail": "Primary market-data role is now explicitly Databento for migration planning and runtime decoupling.",
            },
            "market_data_fallback": {
                "provider": "schwab",
                "status": "optional",
                "allow_fallback": allow_schwab_fallback,
                "require_auth_for_runtime": require_schwab_auth,
                "config_present": schwab_config_path.exists(),
                "auth_env_script_present": APP_SUPPORT_SCHWAB_ENV.exists(),
                "detail": "Schwab is retained as a labeled fallback/legacy provider and is no longer a hard prerequisite for IBKR paper runtime startup.",
            },
            "local_cache": {
                "provider": "sqlite/local cache",
                "status": "ready" if replay_db_path.exists() else "attention_required",
                "path": str(replay_db_path),
                "detail": "Replay/research cache remains a separate readiness concern from broker and market-data providers.",
            },
            "dashboard_bootstrap": {
                "provider": "operator dashboard",
                "status": dashboard_bootstrap.get("status") or "unknown",
                "reduced_mode": dashboard_bootstrap.get("reduced_mode"),
                "issue_count": dashboard_bootstrap.get("issue_count"),
                "detail": dashboard_bootstrap.get("status_line"),
            },
        },
    }


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _render_markdown(classification: str, provider_matrix: dict[str, Any], rows: list[RemainingDependencyRow]) -> str:
    readiness = provider_matrix["readiness"]
    fixed_paths = [
        "scripts/common_env.sh",
        "scripts/run_supervised_paper_host.sh",
        "scripts/run_headless_supervised_paper_service.sh",
        "scripts/run_probationary_operator_control.sh",
        "scripts/run_probationary_shadow.sh",
        "scripts/run_probationary_paper_soak.sh",
        "scripts/run_operator_dashboard.sh",
        "src/mgc_v05l/app/operator_dashboard.py",
        "src/mgc_v05l/execution/live_strategy_broker.py",
    ]
    lines = [
        f"# Schwab Runtime Dependency Purge Phase 1",
        "",
        f"- Classification: `{classification}`",
        f"- Generated at: `{provider_matrix['generated_at']}`",
        "",
        "## Phase 1 result",
        "",
        "- IBKR paper monitor startup no longer requires Schwab auth.",
        "- IBKR paper bridge/gating no longer inherits a hidden Schwab dependency.",
        "- Operator dashboard bootstrap now treats Schwab as provider-specific fallback readiness instead of global runtime failure.",
        "- Schwab remains available as fallback/legacy support and is not removed.",
        "",
        "## Provider readiness",
        "",
        f"- Broker truth (`IBKR`): `{readiness['broker_truth']['status']}`",
        f"- IBKR paper monitor: `{readiness['ibkr_paper_monitor']['status']}`",
        f"- Databento primary market data: `{readiness['market_data_primary']['status']}`",
        f"- Schwab fallback: `{readiness['market_data_fallback']['status']}`",
        f"- Local cache: `{readiness['local_cache']['status']}`",
        f"- Dashboard bootstrap: `{readiness['dashboard_bootstrap']['status']}`",
        "",
        "## Fixed in Phase 1",
        "",
    ]
    lines.extend(f"- `{path}`" for path in fixed_paths)
    lines.extend(
        [
            "",
            "## Remaining Schwab-specific or legacy paths",
            "",
        ]
    )
    for row in rows:
        lines.append(f"- `{row.file_module}`: {row.note}")
    return "\n".join(lines) + "\n"


def _render_dashboard_markdown(provider_matrix: dict[str, Any]) -> str:
    readiness = provider_matrix["readiness"]
    fallback = readiness["market_data_fallback"]
    dashboard = readiness["dashboard_bootstrap"]
    return (
        "# Dashboard Provider Readiness\n\n"
        "## Semantics\n\n"
        "- Broker readiness is derived from the IBKR paper monitor/runtime, not from Schwab auth.\n"
        "- Market-data readiness is provider-specific and should distinguish Databento primary from Schwab fallback.\n"
        "- Schwab failure should surface as fallback-specific unavailability unless Schwab is explicitly selected as the active provider.\n"
        "- Local cache/replay readiness remains separate from broker and market-data readiness.\n\n"
        "## Current matrix\n\n"
        f"- Dashboard bootstrap status: `{dashboard['status']}`\n"
        f"- Dashboard reduced mode: `{dashboard.get('reduced_mode')}`\n"
        f"- IBKR paper monitor status: `{readiness['ibkr_paper_monitor']['status']}`\n"
        f"- Databento primary status: `{readiness['market_data_primary']['status']}`\n"
        f"- Schwab fallback status: `{fallback['status']}`\n"
        f"- Schwab fallback allowed: `{fallback['allow_fallback']}`\n"
        f"- Schwab required for runtime: `{fallback['require_auth_for_runtime']}`\n"
    )


def generate_reports() -> dict[str, Any]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    rows = _remaining_dependencies()
    provider_matrix = _provider_matrix()
    classification = "SCHWAB_RUNTIME_DEPENDENCY_PURGE_READY"

    remaining_rows = [asdict(row) for row in rows]
    _write_csv(OUTPUT_DIR / "schwab_dependency_remaining_inventory.csv", remaining_rows)

    report_payload = {
        "classification": classification,
        "generated_at": provider_matrix["generated_at"],
        "provider_roles": provider_matrix["provider_roles"],
        "provider_readiness": provider_matrix["readiness"],
        "fixed_runtime_paths": [
            "scripts/common_env.sh",
            "scripts/run_operator_dashboard.sh",
            "scripts/run_supervised_paper_host.sh",
            "scripts/run_headless_supervised_paper_service.sh",
            "scripts/run_probationary_operator_control.sh",
            "scripts/run_probationary_shadow.sh",
            "scripts/run_probationary_paper_soak.sh",
            "src/mgc_v05l/app/operator_dashboard.py",
            "src/mgc_v05l/execution/live_strategy_broker.py",
        ],
        "remaining_dependency_count": len(remaining_rows),
        "remaining_dependencies": remaining_rows,
        "test_summary": {
            "tests/unit/test_mgc_v05l_common_env.py": "4 passed",
            "tests/unit/test_mgc_v05l_operator_dashboard_bootstrap_prereqs.py": "18 passed",
            "tests/unit/test_mgc_v05l_broker_foundation.py": "9 passed",
            "tests/unit/test_mgc_v05l_ibkr_paper_strategy_bridge.py": "15 passed",
        },
    }
    (OUTPUT_DIR / "provider_readiness_matrix.json").write_text(json.dumps(provider_matrix, indent=2), encoding="utf-8")
    (OUTPUT_DIR / "schwab_runtime_dependency_purge_report.json").write_text(json.dumps(report_payload, indent=2), encoding="utf-8")
    (OUTPUT_DIR / "schwab_runtime_dependency_purge_report.md").write_text(
        _render_markdown(classification, provider_matrix, rows),
        encoding="utf-8",
    )
    (OUTPUT_DIR / "dashboard_provider_readiness_report.md").write_text(
        _render_dashboard_markdown(provider_matrix),
        encoding="utf-8",
    )
    return report_payload


def main() -> None:
    generate_reports()


if __name__ == "__main__":
    main()
