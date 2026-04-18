from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
APP_PATH = REPO_ROOT / "desktop" / "src" / "renderer" / "App.tsx"


def test_operator_results_board_tabs_and_controls_present() -> None:
    app_source = APP_PATH.read_text(encoding="utf-8")

    assert 'Section title="Operator Results Board"' in app_source
    assert 'type StrategyAnalysisTab = "results" | "compare" | "study" | "runtime" | "diagnostics";' in app_source
    assert 'const [activeTab, setActiveTab] = useState<StrategyAnalysisTab>("results");' in app_source
    assert "strategyAnalysisPreferredStrategyKey" in app_source
    assert '{ id: "results", label: "Results", note: "Ranked board and selectors" }' in app_source
    assert '{ id: "compare", label: "Compare", note: "Baseline, candidate, and lane deltas" }' in app_source
    assert '{ id: "study", label: "Study", note: "Charts and study detail" }' in app_source
    assert '{ id: "runtime", label: "Runtime", note: "Runtime health and actions" }' in app_source
    assert '{ id: "diagnostics", label: "Diagnostics", note: "Trigger validation and raw evidence" }' in app_source
    assert 'label="Strategy"' in app_source
    assert 'label="Lane / Candidate"' in app_source
    assert 'label="Date Range"' in app_source
    assert "<span>Inspect Lane</span>" in app_source
    assert "<span>Compare To</span>" in app_source
    assert 'label: "Candidate"' in app_source
    assert 'label: "Provenance"' in app_source
    assert 'label: "Comparison Status"' in app_source
    assert 'label: "Recommendation"' in app_source
    assert "Report Status" in app_source
    assert "P/L Status" in app_source
    assert "Trade Truth Quality" in app_source
    assert "Comparable to Baseline?" in app_source
    assert "Recommended Next Action" in app_source
    assert "View Report" in app_source
    assert "View Summary" in app_source
    assert "View Evidence" in app_source
    assert "Compare Selected" in app_source
    assert "Study Available" in app_source
    assert "No reportable run loaded" in app_source
    assert "Replay loaded, but priced closed-trade path is incomplete" in app_source
    assert "Paper lane attached, but insufficient trade truth for P/L" in app_source
    assert "Trigger validation and other low-level diagnostics stay here so raw blocker counts do not dominate the operator landing board." in app_source
    assert "PaperStartupPanel" in app_source
    assert "Export Replay Output" in app_source
    assert "Export Trigger Report" in app_source
    assert "Open Study JSON" not in app_source
    assert "Open Output" not in app_source
    assert "Open Trigger Report" not in app_source
    assert 'Section title="Replay Strategy Study"' not in app_source
    assert 'Section title="Trigger Validation"' not in app_source


def test_research_analytics_calendar_navigation_uses_app_level_preference_state() -> None:
    app_source = APP_PATH.read_text(encoding="utf-8")

    assert 'const [preferredAnalysisStrategyKey, setPreferredAnalysisStrategyKey] = useState("");' in app_source
    assert 'setPreferredAnalysisStrategyKey(String(contribution.strategyId));' in app_source
    assert 'setSelectedStrategyKey(String(contribution.strategyId));' not in app_source
    assert 'preferredStrategyKey={preferredAnalysisStrategyKey || String(selectedWorkspacePerformanceRow?.strategy_key ?? "")}' in app_source


def test_research_runtime_bridge_operator_sections_present() -> None:
    app_source = APP_PATH.read_text(encoding="utf-8")

    assert 'const researchRuntimeBridge = asRecord(dashboard?.research_runtime_bridge);' in app_source
    assert 'const startupControlPlane = asRecord(dashboard?.startup_control_plane);' in app_source
    assert 'const supervisedPaperOperability = asRecord(dashboard?.supervised_paper_operability);' in app_source
    assert 'const operationalReadiness = useMemo<OperationalReadinessModel>(' in app_source
    assert 'const researchRuntimeBridgePendingIntents = asArray<JsonRecord>(researchRuntimeBridge.pending_intents);' in app_source
    assert 'const researchRuntimeBridgeRuntimeEvents = asRecord(researchRuntimeBridge.runtime_events);' in app_source
    assert 'const researchRuntimeBridgeAnomalies = asRecord(researchRuntimeBridge.anomalies);' in app_source
    assert 'const researchRuntimeBridgeCyclePolicy = asRecord(researchRuntimeBridge.cycle_policy);' in app_source
    assert 'const researchRuntimeBridgeCadence = asRecord(researchRuntimeBridge.cadence);' in app_source
    assert 'const researchRuntimeBridgeSupervisor = asRecord(researchRuntimeBridge.supervisor);' in app_source
    assert 'const researchRuntimeBridgeAnomalyQueue = asRecord(researchRuntimeBridge.anomaly_queue);' in app_source
    assert 'const researchRuntimeBridgeOperatorReviews = asRecord(researchRuntimeBridge.operator_reviews);' in app_source
    assert 'title="What Needs Attention Now"' in app_source
    assert "AttentionPanel" in app_source
    assert "Primary issue" in app_source
    assert "Primary next action" in app_source
    assert "Fallback action" in app_source
    assert 'Section title="Research Runtime Bridge"' in app_source
    assert 'Paper-only restriction: this bridge emits deterministic paper intents and fills from research truth and does not route broker orders live.' in app_source
    assert "Supervised cadence" in app_source
    assert "Start Supervision" in app_source
    assert "Run Cycle Now" in app_source
    assert "Stop Supervision" in app_source
    assert "Prospective cadence:" in app_source
    assert "Cadence runner:" in app_source
    assert "Blocked-state guidance:" in app_source
    assert "Active bridge anomaly" in app_source
    assert "Open Runtime Events" in app_source
    assert "Bridge cadence policy and lane detail" in app_source
    assert 'Section title="Research Runtime Bridge Events"' in app_source
    assert "Selected Anomaly" in app_source
    assert "Acknowledge Anomaly" in app_source
    assert "Mark Reviewed" in app_source
    assert "Resolve Anomaly" in app_source
    assert 'Section title="Startup Control Plane"' in app_source
    assert "Authoritative startup dependency truth for paper-only launch, reconciliation, and packaged-launch refusal" in app_source
    assert "Launch Allowed" in app_source
    assert "Launch Refused" in app_source
    assert "Usable Now" in app_source
    assert "Not Usable" in app_source
    assert "Supervised paper usability:" in app_source
    assert "Open Evidence" in app_source
    assert "paper-force-reconcile" in app_source
    assert "Pending Intents" in app_source
    assert "Open Runtime Positions" in app_source
    assert "Recent Bridge Intents" in app_source
    assert "Recent Bridge Fills" in app_source
    assert "Closed Runtime Positions" in app_source
    assert "Runtime Events / Anomalies" in app_source
    assert 'Section title="Runtime Anomaly Reviews"' in app_source
    assert "Needs Attention Now" in app_source
    assert "Unresolved Anomalies" in app_source
    assert "Escalated Anomalies" in app_source
    assert "Ack Pending" in app_source
    assert "Reviewed Pending" in app_source
    assert "Attention queue" in app_source
    assert "Heartbeat age:" in app_source
    assert "Occurrences:" in app_source


def test_startup_control_plane_uses_shared_operational_readiness_reducer() -> None:
    app_source = APP_PATH.read_text(encoding="utf-8")

    assert 'deriveOperationalReadiness' in app_source
    assert 'const operationalReadiness = useMemo<OperationalReadinessModel>(' in app_source
    assert 'const canRunLiveActions = operationalReadiness.liveActionsAllowed;' in app_source
    assert 'appUsableForSupervisedPaper' in app_source
    assert 'if (operationalReadiness.overallState !== "READY") {' in app_source
    assert 'operationalReadiness={operationalReadiness}' in app_source
    assert 'onRunPrimaryAction={() => void runAttentionAction(operationalReadiness.primaryAction)}' in app_source


def test_desktop_supervised_paper_mode_is_service_attached_first() -> None:
    app_source = APP_PATH.read_text(encoding="utf-8")

    assert 'Service Attached' in app_source
    assert 'Desktop Managed (Diagnostic)' in app_source
    assert 'showStartDashboardAction' in app_source
    assert 'showRestartDashboardAction' in app_source
    assert 'Desktop Mode' in app_source
