import { app, BrowserWindow, Menu, ipcMain } from "electron";
import fs from "node:fs/promises";
import path from "node:path";
import {
  appendDesktopLog,
  copyText,
  clearLocalOperatorAuthSession,
  authenticateLocalOperator,
  getDesktopState,
  openExternalUrl,
  openPathInShell,
  shutdownDashboardManager,
  restartDashboard,
  runDashboardAction,
  runProductionLinkAction,
  startDashboard,
  stopDashboard,
} from "./runtime";

let mainWindow: BrowserWindow | null = null;

function cliSwitchValue(name: string): string | undefined {
  const prefix = `--${name}=`;
  for (const arg of process.argv.slice(1)) {
    if (arg === `--${name}`) {
      return "1";
    }
    if (arg.startsWith(prefix)) {
      return arg.slice(prefix.length);
    }
  }
  return undefined;
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
  appendDesktopLog(`[electron] renderer capture written to ${capturePath}`);
  console.info("[mgc-operator-desktop] renderer capture written", capturePath);
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
    appendDesktopLog("[electron] renderer self-test completed");
    console.info("[mgc-operator-desktop] renderer self-test", JSON.stringify(result));
  } catch (error) {
    appendDesktopLog(`[electron] renderer self-test failed: ${String(error)}`);
    console.error("[mgc-operator-desktop] renderer self-test failed", error);
  }
}

function rendererEntry(): string {
  const devUrl = cliSwitchValue("mgc-renderer-url") || process.env.MGC_RENDERER_URL || process.env.VITE_DEV_SERVER_URL;
  if (devUrl) {
    return devUrl;
  }
  return `file://${path.join(__dirname, "..", "renderer", "index.html")}`;
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
  const windowSize = configuredWindowSize();
  mainWindow = new BrowserWindow({
    width: windowSize.width,
    height: windowSize.height,
    minWidth: 1180,
    minHeight: 760,
    backgroundColor: "#0b1320",
    title: "MGC Operator",
    webPreferences: {
      preload: path.join(__dirname, "preload.js"),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });

  mainWindow.webContents.on("did-finish-load", () => {
    appendDesktopLog("[electron] renderer loaded");
    console.info("[mgc-operator-desktop] renderer loaded");
    if (process.env.MGC_DESKTOP_SELFTEST === "1" && mainWindow) {
      void runRendererSelfTest(mainWindow);
    }
    if (mainWindow) {
      void maybeCaptureWindow(mainWindow);
    }
  });
  mainWindow.webContents.on("did-fail-load", (_event, errorCode, errorDescription) => {
    appendDesktopLog(`[electron] renderer failed to load: ${errorCode} ${errorDescription}`);
    console.error("[mgc-operator-desktop] renderer failed to load", errorCode, errorDescription);
  });

  await mainWindow.loadURL(rendererEntry());
}

function installIpcHandlers(): void {
  ipcMain.handle("desktop:get-state", () => getDesktopState());
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
}

app.whenReady().then(async () => {
  installIpcHandlers();
  Menu.setApplicationMenu(createMenu());
  appendDesktopLog("[electron] app ready");
  console.info("[mgc-operator-desktop] electron ready");
  await createWindow();
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") {
    app.quit();
  }
});

app.on("before-quit", () => {
  shutdownDashboardManager();
});

app.on("activate", async () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    await createWindow();
  }
});
