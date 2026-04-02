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
