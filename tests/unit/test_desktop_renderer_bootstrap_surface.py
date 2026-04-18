from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
INDEX_PATH = REPO_ROOT / "desktop" / "index.html"
MAIN_PROCESS_PATH = REPO_ROOT / "desktop" / "src" / "main" / "main.ts"
PRELOAD_PATH = REPO_ROOT / "desktop" / "src" / "main" / "preload.ts"
RENDERER_MAIN_PATH = REPO_ROOT / "desktop" / "src" / "renderer" / "main.tsx"
RENDERER_APP_PATH = REPO_ROOT / "desktop" / "src" / "renderer" / "App.tsx"


def test_index_html_contains_visible_first_paint_bootstrap_shell() -> None:
    source = INDEX_PATH.read_text(encoding="utf-8")

    assert 'id="bootstrap-shell"' in source
    assert "Booting packaged operator UI" in source
    assert "desktop_renderer_bootstrap.json" in source
    assert "RENDERER_ENTRY_LOAD_FAILED" in source
    assert "window.__MGC_BOOTSTRAP__" in source


def test_preload_reports_bootstrap_events_and_bridge_exposure() -> None:
    source = PRELOAD_PATH.read_text(encoding="utf-8")

    assert 'ipcRenderer.send("desktop:bootstrap-event", "preload:loaded"' in source
    assert 'ipcRenderer.send("desktop:bootstrap-event", "preload:bridge-exposed"' in source
    assert 'ipcRenderer.send("desktop:bootstrap-event", "preload:error"' in source
    assert "reportBootstrapEvent: (stage: string, payload?: Record<string, unknown>): void" in source


def test_renderer_main_uses_error_boundary_and_missing_bridge_fallback() -> None:
    source = RENDERER_MAIN_PATH.read_text(encoding="utf-8")

    assert "installBootstrapGuards();" in source
    assert 'reportBootstrapEvent("renderer:root-mount-start"' in source
    assert 'bootstrapScenario === "bootstrap-fatal"' in source
    assert 'FORCED_BOOTSTRAP_FATAL' in source
    assert "<BootstrapErrorBoundary>" in source
    assert "<BootstrapReadyMarker />" in source
    assert "<MissingBridgeScreen />" in source
    assert "ROOT_ELEMENT_MISSING" in source


def test_main_process_persists_renderer_bootstrap_artifacts() -> None:
    source = MAIN_PROCESS_PATH.read_text(encoding="utf-8")

    assert 'desktop_renderer_bootstrap.json' in source
    assert 'desktop_renderer_bootstrap_events.jsonl' in source
    assert 'ipcMain.on("desktop:bootstrap-event"' in source
    assert 'writeRendererBootstrapArtifact("renderer:did-fail-load"' in source
    assert 'writeRendererBootstrapArtifact("renderer:console-message"' in source


def test_startup_control_plane_helpers_are_module_scoped_for_packaged_renderer() -> None:
    source = RENDERER_APP_PATH.read_text(encoding="utf-8")
    app_start = source.index("export function App() {")

    assert 'deriveOperationalReadiness' in source
    assert 'startupControlPlaneActionSpec' in source
    assert 'startupControlPlaneTone' in source
    for helper in (
        "function textOrFallback(",
        "function runtimeBridgeBlockedReasonLabel(",
        "function runtimeBridgeReconciliationLabel(",
        "function runtimeStartFlagLabel(",
    ):
        assert source.index(helper) < app_start, f"{helper} should stay module-scoped so packaged helper consumers can resolve it."


def test_renderer_run_command_surfaces_cancel_and_failure_states_on_positions_page() -> None:
    source = RENDERER_APP_PATH.read_text(encoding="utf-8")

    assert "function recordCommandResult(" in source
    assert "Operator canceled the confirmation dialog. No live order was sent." in source
    assert "message: `${sentenceCase(label.replaceAll(\"-\", \" \"))} canceled.`" in source
    assert "} catch (error) {" in source
    assert "const failureResult = commandFailureResult(label, error);" in source
    assert 'page === "positions"' in source
