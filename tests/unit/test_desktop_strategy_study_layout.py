from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
APP_PATH = REPO_ROOT / "desktop" / "src" / "renderer" / "App.tsx"
STYLES_PATH = REPO_ROOT / "desktop" / "src" / "renderer" / "styles.css"


def test_replay_strategy_study_uses_explicit_svg_sizing_and_height_control() -> None:
    app_source = APP_PATH.read_text(encoding="utf-8")

    assert 'const [heightPreset, setHeightPreset] = useState<StudyHeightPreset>("standard");' in app_source
    assert 'const [selectedStudyKey, setSelectedStudyKey] = useState<string>("");' in app_source
    assert 'const [selectedStrategyId, setSelectedStrategyId] = useState<string>("all");' in app_source
    assert 'const [selectedCandidateId, setSelectedCandidateId] = useState<string>("all");' in app_source
    assert 'const [selectedStudyMode, setSelectedStudyMode] = useState<string>("all");' in app_source
    assert 'const [selectedEntryModel, setSelectedEntryModel] = useState<string>("all");' in app_source
    assert 'const [pnlMode, setPnlMode] = useState<StudyPnlMode>("cumulative_total");' in app_source
    assert 'Execution detail' in app_source
    assert 'type="date"' in app_source
    assert '<strong>Structural</strong>' in app_source
    assert '<strong>Candidate</strong>' in app_source
    assert '<strong>Entry Model</strong>' in app_source
    assert '<strong>Emitter</strong>' in app_source
    assert '<strong>Lifecycle</strong>' in app_source
    assert '<strong>Entry Support</strong>' in app_source
    assert '<strong>Intrabar Truth</strong>' in app_source
    assert '<strong>Entry Truth</strong>' in app_source
    assert '<strong>Exit Truth</strong>' in app_source
    assert '<strong>Lifecycle Records</strong>' in app_source
    assert '<strong>P&amp;L Basis</strong>' in app_source
    assert '<strong>Provenance</strong>' in app_source
    assert '<strong>Supported Models</strong>' in app_source
    assert '<strong>Coverage</strong>' in app_source
    assert '<strong>Execution Role</strong>' in app_source
    assert 'All strategies' in app_source
    assert 'All candidates' in app_source
    assert 'All modes' in app_source
    assert 'All entry models' in app_source
    assert 'Unsupported entry-model combination:' in app_source
    assert 'Carry-In' in app_source
    assert 'role="group" aria-label="Strategy study size"' in app_source
    assert 'role="group" aria-label="Strategy study pnl mode"' in app_source
    assert '(["compact", "standard", "expanded"] as const)' in app_source
    assert 'width={width}' in app_source
    assert 'height={svgHeight}' in app_source
    assert 'style={{ width: `${width}px`, height: `${svgHeight}px` }}' in app_source
    assert 'function resolveStudyLayout(rowCount: number, preset: StudyHeightPreset)' in app_source
    assert 'function buildStudyEventCoords(args:' in app_source
    assert 'function studyEventTooltip(event:' in app_source
    assert 'Execution event:' in app_source
    assert 'Entry model:' in app_source
    assert 'Truth:' in app_source


def test_replay_strategy_study_styles_pin_non_collapsing_chart_height() -> None:
    styles = STYLES_PATH.read_text(encoding="utf-8")

    assert ".study-chart-shell {" in styles
    assert ".study-workbench-controls {" in styles
    assert ".study-select-field select," in styles
    assert ".study-meta-strip {" in styles
    assert "min-height: var(--study-shell-min-height, 660px);" in styles
    assert "display: flex;" in styles
    assert "flex-direction: column;" in styles
    assert ".study-chart-scroll {" in styles
    assert "min-height: var(--study-shell-min-height, 660px);" in styles
    assert "overflow: auto;" in styles
    assert "align-items: flex-start;" in styles
    assert ".study-svg {" in styles
    assert "flex: none;" in styles
    assert ".study-size-control {" in styles
    assert ".study-size-button.active {" in styles
    assert ".study-mode-control {" in styles
    assert ".study-execution-strip {" in styles
    assert ".study-event-marker {" in styles
    assert ".study-carry-in-line {" in styles


def test_replay_strategy_study_empty_state_copy_is_still_present() -> None:
    app_source = APP_PATH.read_text(encoding="utf-8")

    assert "Replay/paper study only. This view needs a replay or historical playback run that writes strategy-study artifacts." in app_source
    assert "Next: stay in Replay, run or rerun Historical Playback, then return here after the new run finishes." in app_source
