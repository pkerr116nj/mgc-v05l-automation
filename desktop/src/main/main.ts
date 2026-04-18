import { app, BrowserWindow, Menu, ipcMain } from "electron";
import fsSync from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  appendDesktopLog,
  copyText,
  clearLocalOperatorAuthSession,
  authenticateLocalOperator,
  getDesktopState,
  openExternalUrl,
  openPathInShell,
  prepareDesktopForLaunch,
  shutdownDashboardManager,
  restartDashboard,
  runDashboardAction,
  runProductionLinkAction,
  startDashboard,
  stopDashboard,
} from "./runtime";

let mainWindow: BrowserWindow | null = null;
let createWindowInFlight = false;
const appLaunchSessionId = `${Date.now()}-${process.pid}`;
let startupStatusWriteChain: Promise<void> = Promise.resolve();
let rendererBootstrapWriteChain: Promise<void> = Promise.resolve();
let quittingAfterArtifactFlush = false;
const startupMilestones = {
  windowObserved: false,
  rendererDomReady: false,
  rendererDidFinishLoad: false,
  captureWritten: false,
  selfTestCompleted: false,
};

function desktopLaunchArtifactRoot(): string {
  const explicit = String(process.env.MGC_DESKTOP_RUNTIME_DIR || "").trim();
  if (explicit) {
    return explicit;
  }
  try {
    return path.join(app.getPath("userData"), "runtime");
  } catch {
    const tempRoot = String(process.env.TMPDIR || "/tmp").trim() || "/tmp";
    return path.join(tempRoot, "mgc-operator-runtime");
  }
}

function writeUltraEarlyStartupMarker(stage: string, extra: Record<string, unknown> = {}): void {
  try {
    const { statusPath, eventsPath } = startupArtifactPaths();
    fsSync.mkdirSync(path.dirname(statusPath), { recursive: true });
    const payload = {
      recorded_at: new Date().toISOString(),
      pid: process.pid,
      app_launch_session_id: appLaunchSessionId,
      launch_attempt_id: cliSwitchValue("mgc-launch-attempt-id") || process.env.MGC_DESKTOP_LAUNCH_ATTEMPT_ID || null,
      startup_writer: "electron",
      js_startup_observed: true,
      js_main_entry_observed: true,
      launch_mode: cliSwitchValue("mgc-launch-mode") || process.env.MGC_DESKTOP_LAUNCH_MODE || null,
      executable_path: cliSwitchValue("mgc-executable-path") || process.env.MGC_DESKTOP_EXECUTABLE_PATH || process.execPath,
      repo_root: cliSwitchValue("mgc-repo-root") || process.env.MGC_REPO_ROOT || null,
      dashboard_url: cliSwitchValue("mgc-dashboard-url") || process.env.MGC_OPERATOR_DASHBOARD_URL || null,
      renderer_entry: rendererEntry(),
      stage,
      argv: process.argv,
      capture_requested: Boolean(cliSwitchValue("mgc-capture-path") || process.env.MGC_DESKTOP_CAPTURE_PATH),
      window_observed: startupMilestones.windowObserved,
      renderer_dom_ready: startupMilestones.rendererDomReady,
      renderer_did_finish_load: startupMilestones.rendererDidFinishLoad,
      capture_written: startupMilestones.captureWritten,
      self_test_completed: startupMilestones.selfTestCompleted,
      ...extra,
    };
    fsSync.writeFileSync(statusPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
    fsSync.appendFileSync(eventsPath, `${JSON.stringify(payload)}\n`, "utf8");
  } catch {
    // Fall back to the later async startup writer if the ultra-early path cannot write yet.
  }
}

async function pathExists(targetPath: string): Promise<boolean> {
  try {
    await fs.access(targetPath);
    return true;
  } catch {
    return false;
  }
}

async function writeLatestArtifactAtomically(targetPath: string, payload: Record<string, unknown>): Promise<void> {
  const tempPath = `${targetPath}.${process.pid}.tmp`;
  await fs.writeFile(tempPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  await fs.rename(tempPath, targetPath);
}

function logStartupStage(message: string, extra: Record<string, unknown> = {}): void {
  appendDesktopLog(`[electron] ${message}`);
  console.info("[mgc-operator-desktop]", message);
  void writeElectronStartupStatus(message, extra);
}

writeUltraEarlyStartupMarker("main:module-entry");

function cliSwitchValue(name: string): string | undefined {
  return switchValueFromArgv(process.argv, name);
}

function switchValueFromArgv(argv: readonly string[], name: string): string | undefined {
  const prefix = `--${name}=`;
  for (const arg of argv.slice(1)) {
    if (arg === `--${name}`) {
      return "1";
    }
    if (arg.startsWith(prefix)) {
      return arg.slice(prefix.length);
    }
  }
  return undefined;
}

function localOperatorAuthReasonFromArgv(argv: readonly string[]): string | null {
  if (switchValueFromArgv(argv, "mgc-authenticate-local-operator") !== "1") {
    return null;
  }
  const reason = switchValueFromArgv(argv, "mgc-local-operator-auth-reason");
  return typeof reason === "string" && reason.trim()
    ? reason.trim()
    : "Authenticate local operator access for this desktop session.";
}

let pendingLocalOperatorAuthPromise: Promise<void> | null = null;

function maybeRunRequestedLocalOperatorAuth(argv: readonly string[], trigger: "startup" | "second-instance"): void {
  const reason = localOperatorAuthReasonFromArgv(argv);
  if (!reason) {
    return;
  }
  if (pendingLocalOperatorAuthPromise) {
    logStartupStage(`local-operator-auth:${trigger}:already-running`);
    return;
  }
  if (mainWindow) {
    if (mainWindow.isMinimized()) {
      mainWindow.restore();
    }
    mainWindow.focus();
  }
  logStartupStage(`local-operator-auth:${trigger}:requested`, { reason });
  pendingLocalOperatorAuthPromise = authenticateLocalOperator(reason)
    .then((result) => {
      logStartupStage(`local-operator-auth:${trigger}:completed`, {
        ok: result.ok,
        message: result.message,
        detail: result.detail ?? null,
      });
    })
    .catch((error) => {
      logStartupStage(`local-operator-auth:${trigger}:failed`, {
        error: error instanceof Error ? error.message : String(error),
      });
    })
    .finally(() => {
      pendingLocalOperatorAuthPromise = null;
    });
}

function captureDelayMs(): number {
  const raw = Number(cliSwitchValue("mgc-capture-delay-ms") || process.env.MGC_DESKTOP_CAPTURE_DELAY_MS || 5000);
  if (!Number.isFinite(raw)) {
    return 5000;
  }
  return Math.max(500, raw);
}

function configuredWindowSize(): { width: number; height: number } {
  const width = Number(cliSwitchValue("mgc-capture-window-width") || process.env.MGC_DESKTOP_CAPTURE_WINDOW_WIDTH || 1540);
  const height = Number(cliSwitchValue("mgc-capture-window-height") || process.env.MGC_DESKTOP_CAPTURE_WINDOW_HEIGHT || 980);
  return {
    width: Number.isFinite(width) ? Math.max(1180, Math.round(width)) : 1540,
    height: Number.isFinite(height) ? Math.max(760, Math.round(height)) : 980,
  };
}

async function maybeCaptureWindow(window: BrowserWindow): Promise<void> {
  const capturePath = cliSwitchValue("mgc-capture-path") || process.env.MGC_DESKTOP_CAPTURE_PATH;
  if (!capturePath) {
    return;
  }
  const requestedHash = cliSwitchValue("mgc-capture-hash") || process.env.MGC_DESKTOP_CAPTURE_HASH;
  const captureScript = cliSwitchValue("mgc-capture-js") || process.env.MGC_DESKTOP_CAPTURE_JS;
  const scrollSectionTitle = cliSwitchValue("mgc-capture-scroll-section-title") || process.env.MGC_DESKTOP_CAPTURE_SCROLL_SECTION_TITLE;
  const scrollRowText = cliSwitchValue("mgc-capture-scroll-row-text") || process.env.MGC_DESKTOP_CAPTURE_SCROLL_ROW_TEXT;
  if (requestedHash) {
    await window.webContents.executeJavaScript(`window.location.hash = ${JSON.stringify(requestedHash)};`);
  }
  await new Promise((resolve) => setTimeout(resolve, captureDelayMs()));
  if (captureScript) {
    await window.webContents.executeJavaScript(captureScript);
    await new Promise((resolve) => setTimeout(resolve, 1500));
  }
  if (scrollSectionTitle) {
    await window.webContents.executeJavaScript(`
      (() => {
        const sections = Array.from(document.querySelectorAll(".section-card"));
        const target = sections.find((section) => {
          const title = section.querySelector(".section-title");
          return (title?.textContent || "").trim() === ${JSON.stringify(scrollSectionTitle)};
        });
        if (target) {
          target.scrollIntoView({ block: "start", inline: "nearest" });
        }
      })();
    `);
    await new Promise((resolve) => setTimeout(resolve, 1200));
  }
  if (scrollRowText) {
    await window.webContents.executeJavaScript(`
      (() => {
        const scopedRows = (() => {
          const title = ${JSON.stringify(scrollSectionTitle || "")};
          if (!title) {
            return [];
          }
          const sections = Array.from(document.querySelectorAll(".section-card"));
          const targetSection = sections.find((section) => {
            const heading = section.querySelector(".section-title");
            return (heading?.textContent || "").trim() === title;
          });
          return targetSection ? Array.from(targetSection.querySelectorAll("tr")) : [];
        })();
        const rows = scopedRows.length ? scopedRows : Array.from(document.querySelectorAll("tr"));
        const target = rows.find((row) => (row.textContent || "").includes(${JSON.stringify(scrollRowText)}));
        if (target) {
          target.scrollIntoView({ block: "center", inline: "nearest" });
        }
      })();
    `);
    await new Promise((resolve) => setTimeout(resolve, 1200));
  }
  const image = await window.capturePage();
  await fs.mkdir(path.dirname(capturePath), { recursive: true });
  await fs.writeFile(capturePath, image.toPNG());
  startupMilestones.captureWritten = true;
  appendDesktopLog(`[electron] renderer capture written to ${capturePath}`);
  console.info("[mgc-operator-desktop] renderer capture written", capturePath);
  void writeElectronStartupStatus("capture:written", { capture_path: capturePath, capture_written: true });
  if ((cliSwitchValue("mgc-capture-and-exit") || process.env.MGC_DESKTOP_CAPTURE_AND_EXIT) === "1") {
    app.quit();
  }
}

async function runRendererSelfTest(window: BrowserWindow): Promise<void> {
  try {
    const result = await window.webContents.executeJavaScript(`
      (async () => {
        const expected = ["home","runtime","strategies","positions","market","replay","logs","configuration","diagnostics","settings"];
        const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
        await wait(1200);
        const navItems = Array.from(document.querySelectorAll(".nav-item")).map((node) => ({
          page: node.getAttribute("data-page"),
          label: (node.textContent || "").trim(),
        }));
        const pages = [];
        for (const page of expected) {
          window.location.hash = "#/" + page;
          await wait(120);
          pages.push({
            page,
            title: document.querySelector(".page-eyebrow")?.textContent?.trim() || null,
            sectionTitles: Array.from(document.querySelectorAll(".section-title")).map((node) => (node.textContent || "").trim()),
          });
        }
        return { navItems, pages };
      })();
    `);
    startupMilestones.selfTestCompleted = true;
    appendDesktopLog("[electron] renderer self-test completed");
    console.info("[mgc-operator-desktop] renderer self-test", JSON.stringify(result));
    void writeElectronStartupStatus("renderer:self-test-completed", { self_test_completed: true });
  } catch (error) {
    appendDesktopLog(`[electron] renderer self-test failed: ${String(error)}`);
    console.error("[mgc-operator-desktop] renderer self-test failed", error);
    void writeElectronStartupStatus("renderer:self-test-failed", { self_test_completed: false, self_test_error: String(error) });
  }
}

function rendererEntry(): string {
  const devUrl = cliSwitchValue("mgc-renderer-url") || process.env.MGC_RENDERER_URL || process.env.VITE_DEV_SERVER_URL;
  if (devUrl) {
    return devUrl;
  }
  return `file://${path.join(__dirname, "..", "renderer", "index.html")}`;
}

function startupArtifactPaths(): { statusPath: string; eventsPath: string } {
  const captureTargetPath =
    cliSwitchValue("mgc-capture-path") ||
    process.env.MGC_DESKTOP_CAPTURE_PATH ||
    path.join(desktopLaunchArtifactRoot(), "desktop_electron.log");
  const runtimeDir = path.dirname(captureTargetPath);
  return {
    statusPath: path.join(runtimeDir, "desktop_electron_startup.json"),
    eventsPath: path.join(runtimeDir, "desktop_electron_startup_events.jsonl"),
  };
}

function rendererBootstrapArtifactPaths(): { latestPath: string; eventsPath: string } {
  const { statusPath } = startupArtifactPaths();
  const runtimeDir = path.dirname(statusPath);
  return {
    latestPath: path.join(runtimeDir, "desktop_renderer_bootstrap.json"),
    eventsPath: path.join(runtimeDir, "desktop_renderer_bootstrap_events.jsonl"),
  };
}

async function writeRendererBootstrapArtifact(stage: string, extra: Record<string, unknown> = {}): Promise<void> {
  rendererBootstrapWriteChain = rendererBootstrapWriteChain.then(async () => {
    try {
      const { latestPath, eventsPath } = rendererBootstrapArtifactPaths();
      await fs.mkdir(path.dirname(latestPath), { recursive: true });
      const payload = {
        recorded_at: new Date().toISOString(),
        pid: process.pid,
        app_launch_session_id: appLaunchSessionId,
        launch_attempt_id: cliSwitchValue("mgc-launch-attempt-id") || process.env.MGC_DESKTOP_LAUNCH_ATTEMPT_ID || null,
        stage,
        launch_mode: cliSwitchValue("mgc-launch-mode") || process.env.MGC_DESKTOP_LAUNCH_MODE || null,
        executable_path: cliSwitchValue("mgc-executable-path") || process.env.MGC_DESKTOP_EXECUTABLE_PATH || process.execPath,
        renderer_entry: rendererEntry(),
        preload_path: path.join(__dirname, "preload.js"),
        window_observed: startupMilestones.windowObserved,
        renderer_dom_ready: startupMilestones.rendererDomReady,
        renderer_did_finish_load: startupMilestones.rendererDidFinishLoad,
        capture_written: startupMilestones.captureWritten,
        self_test_completed: startupMilestones.selfTestCompleted,
        ...extra,
      };
      await writeLatestArtifactAtomically(latestPath, payload);
      await fs.appendFile(eventsPath, `${JSON.stringify(payload)}\n`, "utf8");
    } catch (error) {
      appendDesktopLog(`[electron] failed to write renderer bootstrap artifact: ${String(error)}`);
    }
  });
  return rendererBootstrapWriteChain;
}

async function writeElectronStartupStatus(stage: string, extra: Record<string, unknown> = {}): Promise<void> {
  startupStatusWriteChain = startupStatusWriteChain.then(async () => {
    try {
      const { statusPath, eventsPath } = startupArtifactPaths();
      await fs.mkdir(path.dirname(statusPath), { recursive: true });
      const payload = {
        recorded_at: new Date().toISOString(),
        pid: process.pid,
        app_launch_session_id: appLaunchSessionId,
        launch_attempt_id: cliSwitchValue("mgc-launch-attempt-id") || process.env.MGC_DESKTOP_LAUNCH_ATTEMPT_ID || null,
        startup_writer: "electron",
        js_startup_observed: true,
        launch_mode: cliSwitchValue("mgc-launch-mode") || process.env.MGC_DESKTOP_LAUNCH_MODE || null,
        executable_path: cliSwitchValue("mgc-executable-path") || process.env.MGC_DESKTOP_EXECUTABLE_PATH || process.execPath,
        repo_root: cliSwitchValue("mgc-repo-root") || process.env.MGC_REPO_ROOT || null,
        dashboard_url: cliSwitchValue("mgc-dashboard-url") || process.env.MGC_OPERATOR_DASHBOARD_URL || null,
        renderer_entry: rendererEntry(),
        stage,
        argv: process.argv,
        capture_requested: Boolean(cliSwitchValue("mgc-capture-path") || process.env.MGC_DESKTOP_CAPTURE_PATH),
        window_observed: startupMilestones.windowObserved,
        renderer_dom_ready: startupMilestones.rendererDomReady,
        renderer_did_finish_load: startupMilestones.rendererDidFinishLoad,
        capture_written: startupMilestones.captureWritten,
        self_test_completed: startupMilestones.selfTestCompleted,
        ...extra,
      };
      await writeLatestArtifactAtomically(statusPath, payload);
      await fs.appendFile(eventsPath, `${JSON.stringify(payload)}\n`, "utf8");
    } catch (error) {
      appendDesktopLog(`[electron] failed to write startup status: ${String(error)}`);
    }
  });
  return startupStatusWriteChain;
}

function createMenu(): Menu {
  return Menu.buildFromTemplate([
    {
      label: "MGC Operator",
      submenu: [
        {
          label: "Refresh Operator State",
          accelerator: "CmdOrCtrl+R",
          click: () => {
            mainWindow?.webContents.reload();
          },
        },
        { type: "separator" },
        {
          label: "Quit",
          accelerator: "CmdOrCtrl+Q",
          click: () => app.quit(),
        },
      ],
    },
    {
      label: "View",
      submenu: [
        { role: "toggleDevTools" },
        { role: "resetZoom" },
        { role: "zoomIn" },
        { role: "zoomOut" },
        { role: "togglefullscreen" },
      ],
    },
  ]);
}

async function createWindow(): Promise<void> {
  if (mainWindow && !mainWindow.isDestroyed()) {
    if (mainWindow.isMinimized()) {
      mainWindow.restore();
    }
    mainWindow.focus();
    return;
  }
  if (createWindowInFlight) {
    logStartupStage("createWindow:skipped-inflight");
    return;
  }
  createWindowInFlight = true;
  try {
  logStartupStage("createWindow:start");
  const windowSize = configuredWindowSize();
  const preloadPath = path.join(__dirname, "preload.js");
  const targetEntry = rendererEntry();
  const targetEntryPath = targetEntry.startsWith("file://") ? fileURLToPath(targetEntry) : null;
  const preloadExists = await pathExists(preloadPath);
  const targetEntryExists = targetEntryPath ? await pathExists(targetEntryPath) : null;
  logStartupStage(`createWindow:config preload=${preloadPath} entry=${targetEntry}`);
  await writeRendererBootstrapArtifact("createWindow:config", {
    preload_path: preloadPath,
    preload_exists: preloadExists,
    renderer_entry: targetEntry,
    renderer_entry_path: targetEntryPath,
    renderer_entry_exists: targetEntryExists,
  });
  mainWindow = new BrowserWindow({
    width: windowSize.width,
    height: windowSize.height,
    minWidth: 1180,
    minHeight: 760,
    backgroundColor: "#0b1320",
    title: "MGC Operator",
    webPreferences: {
      preload: preloadPath,
      contextIsolation: true,
      nodeIntegration: false,
    },
  });
  logStartupStage("createWindow:browser-window-constructed");

  mainWindow.on("ready-to-show", () => {
    startupMilestones.windowObserved = true;
    logStartupStage("window:ready-to-show", { window_observed: true });
    void writeRendererBootstrapArtifact("window:ready-to-show", { window_observed: true });
  });
  mainWindow.on("closed", () => {
    logStartupStage("window:closed");
    mainWindow = null;
  });

  mainWindow.webContents.on("did-finish-load", () => {
    startupMilestones.rendererDidFinishLoad = true;
    logStartupStage("renderer:did-finish-load", { renderer_did_finish_load: true });
    void writeRendererBootstrapArtifact("renderer:did-finish-load", { renderer_did_finish_load: true });
    if (process.env.MGC_DESKTOP_SELFTEST === "1" && mainWindow) {
      void runRendererSelfTest(mainWindow);
    }
    if (mainWindow) {
      void maybeCaptureWindow(mainWindow);
    }
  });
  mainWindow.webContents.on("did-fail-load", (_event, errorCode, errorDescription, validatedURL, isMainFrame) => {
    logStartupStage(`renderer:did-fail-load code=${errorCode} description=${errorDescription}`);
    console.error("[mgc-operator-desktop] renderer failed to load", errorCode, errorDescription, validatedURL);
    void writeRendererBootstrapArtifact("renderer:did-fail-load", {
      error_code: errorCode,
      error_description: errorDescription,
      validated_url: validatedURL,
      is_main_frame: isMainFrame,
    });
  });
  mainWindow.webContents.on("did-start-loading", () => {
    logStartupStage("renderer:did-start-loading");
    void writeRendererBootstrapArtifact("renderer:did-start-loading");
  });
  mainWindow.webContents.on("console-message", (event: unknown) => {
    const payload = event as { level?: number; message?: string; lineNumber?: number; sourceId?: string };
    logStartupStage(
      `renderer:console level=${payload.level ?? "unknown"} source=${payload.sourceId ?? "unknown"}:${payload.lineNumber ?? "?"} message=${payload.message ?? ""}`,
    );
    void writeRendererBootstrapArtifact("renderer:console-message", {
      level: payload.level ?? null,
      message: payload.message ?? null,
      line_number: payload.lineNumber ?? null,
      source_id: payload.sourceId ?? null,
    });
  });
  mainWindow.webContents.on("dom-ready", () => {
    startupMilestones.rendererDomReady = true;
    logStartupStage("renderer:dom-ready", { renderer_dom_ready: true });
    void writeRendererBootstrapArtifact("renderer:dom-ready", { renderer_dom_ready: true });
  });
  mainWindow.webContents.on("render-process-gone", (_event, details) => {
    logStartupStage(`renderer:gone reason=${details.reason} exitCode=${details.exitCode}`);
    void writeRendererBootstrapArtifact("renderer:render-process-gone", {
      reason: details.reason,
      exit_code: details.exitCode,
    });
  });

  logStartupStage("createWindow:loadURL:start");
  await mainWindow.loadURL(targetEntry);
  logStartupStage("createWindow:loadURL:resolved");
  } finally {
    createWindowInFlight = false;
  }
}

function installIpcHandlers(): void {
  logStartupStage("ipc:install:start");
  ipcMain.handle("desktop:get-state", (_event, options?: { includeHeavyPayload?: boolean }) => getDesktopState(options));
  ipcMain.handle("desktop:start-dashboard", () => startDashboard());
  ipcMain.handle("desktop:stop-dashboard", () => stopDashboard());
  ipcMain.handle("desktop:restart-dashboard", () => restartDashboard());
  ipcMain.handle("desktop:run-dashboard-action", (_event, action: string, payload: Record<string, unknown>) =>
    runDashboardAction(action, payload),
  );
  ipcMain.handle("desktop:run-production-link-action", (_event, action: string, payload: Record<string, unknown>) =>
    runProductionLinkAction(action, payload),
  );
  ipcMain.handle("desktop:authenticate-local-operator", (_event, reason?: string) => authenticateLocalOperator(reason));
  ipcMain.handle("desktop:clear-local-operator-auth-session", () => clearLocalOperatorAuthSession());
  ipcMain.handle("desktop:open-path", (_event, targetPath: string) => openPathInShell(targetPath));
  ipcMain.handle("desktop:open-external-url", (_event, url: string) => openExternalUrl(url));
  ipcMain.handle("desktop:copy-text", (_event, text: string) => copyText(text));
  ipcMain.on("desktop:bootstrap-event", (_event, stage: string, payload: Record<string, unknown>) => {
    appendDesktopLog(`[electron] bootstrap-event ${stage} ${JSON.stringify(payload ?? {})}`);
    void writeRendererBootstrapArtifact(stage, payload ?? {});
    void writeElectronStartupStatus(stage, payload ?? {});
  });
  logStartupStage("ipc:install:done");
}

process.on("uncaughtException", (error) => {
  appendDesktopLog(`[electron] uncaughtException ${error?.stack || String(error)}`);
  console.error("[mgc-operator-desktop] uncaughtException", error);
});

process.on("unhandledRejection", (reason) => {
  appendDesktopLog(`[electron] unhandledRejection ${String(reason)}`);
  console.error("[mgc-operator-desktop] unhandledRejection", reason);
});

const hasSingleInstanceLock = app.requestSingleInstanceLock();
if (!hasSingleInstanceLock) {
  logStartupStage("single-instance-lock:duplicate-instance-detected");
  app.quit();
}

app.on("second-instance", (_event, argv) => {
  logStartupStage("app:second-instance");
  if (mainWindow) {
    if (mainWindow.isMinimized()) {
      mainWindow.restore();
    }
    mainWindow.focus();
  }
  maybeRunRequestedLocalOperatorAuth(argv, "second-instance");
});

app.on("child-process-gone", (_event, details) => {
  logStartupStage(`app:child-process-gone type=${details.type} reason=${details.reason} exitCode=${details.exitCode}`);
});

app.on("web-contents-created", (_event, contents) => {
  logStartupStage(`app:web-contents-created type=${contents.getType()}`);
});

app.on("will-finish-launching", () => {
  logStartupStage("app:will-finish-launching");
});

app.whenReady().then(async () => {
  logStartupStage("whenReady:begin");
  installIpcHandlers();
  logStartupStage("menu:set:start");
  Menu.setApplicationMenu(createMenu());
  logStartupStage("app ready");
  await createWindow();
  logStartupStage("whenReady:createWindow:done");
  void prepareDesktopForLaunch()
    .then(() => {
      logStartupStage("service-host:warmup-dispatched");
    })
    .catch((error) => {
      logStartupStage("service-host:prepare-failed", {
        error: error instanceof Error ? error.message : String(error),
      });
    });
  maybeRunRequestedLocalOperatorAuth(process.argv, "startup");
});

app.on("window-all-closed", () => {
  logStartupStage("app:window-all-closed");
  if (process.platform !== "darwin") {
    app.quit();
  }
});

app.on("before-quit", (event) => {
  if (!quittingAfterArtifactFlush) {
    quittingAfterArtifactFlush = true;
    event.preventDefault();
    void Promise.allSettled([startupStatusWriteChain, rendererBootstrapWriteChain]).finally(() => {
      app.exit();
    });
    return;
  }
  logStartupStage("app:before-quit");
  shutdownDashboardManager();
});

app.on("activate", async () => {
  logStartupStage("app:activate");
  if (BrowserWindow.getAllWindows().length === 0) {
    await createWindow();
  }
});
