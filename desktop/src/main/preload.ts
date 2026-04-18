import { contextBridge, ipcRenderer } from "electron";
import type { DesktopCommandResult, DesktopState } from "./runtime";

const preloadPathLabel = typeof __filename === "string" ? __filename : "packaged-preload";

const operatorDesktop = {
  getDesktopState: (options?: { includeHeavyPayload?: boolean }): Promise<DesktopState> =>
    ipcRenderer.invoke("desktop:get-state", options ?? {}),
  startDashboard: (): Promise<DesktopCommandResult> => ipcRenderer.invoke("desktop:start-dashboard"),
  stopDashboard: (): Promise<DesktopCommandResult> => ipcRenderer.invoke("desktop:stop-dashboard"),
  restartDashboard: (): Promise<DesktopCommandResult> => ipcRenderer.invoke("desktop:restart-dashboard"),
  reportBootstrapEvent: (stage: string, payload?: Record<string, unknown>): void => {
    ipcRenderer.send("desktop:bootstrap-event", stage, payload ?? {});
  },
  runDashboardAction: (action: string, payload?: Record<string, unknown>): Promise<DesktopCommandResult> =>
    ipcRenderer.invoke("desktop:run-dashboard-action", action, payload ?? {}),
  runProductionLinkAction: (action: string, payload: Record<string, unknown>): Promise<DesktopCommandResult> =>
    ipcRenderer.invoke("desktop:run-production-link-action", action, payload),
  authenticateLocalOperator: (reason?: string): Promise<DesktopCommandResult> =>
    ipcRenderer.invoke("desktop:authenticate-local-operator", reason),
  clearLocalOperatorAuthSession: (): Promise<DesktopCommandResult> =>
    ipcRenderer.invoke("desktop:clear-local-operator-auth-session"),
  openPath: (targetPath: string): Promise<DesktopCommandResult> => ipcRenderer.invoke("desktop:open-path", targetPath),
  openExternalUrl: (url: string): Promise<DesktopCommandResult> => ipcRenderer.invoke("desktop:open-external-url", url),
  copyText: (text: string): Promise<DesktopCommandResult> => ipcRenderer.invoke("desktop:copy-text", text),
};

try {
  ipcRenderer.send("desktop:bootstrap-event", "preload:loaded", {
    preload_path: preloadPathLabel,
    preload_loaded_at: new Date().toISOString(),
  });
  contextBridge.exposeInMainWorld("operatorDesktop", operatorDesktop);
  ipcRenderer.send("desktop:bootstrap-event", "preload:bridge-exposed", {
    preload_path: preloadPathLabel,
    bridge_keys: Object.keys(operatorDesktop),
  });
} catch (error) {
  try {
    ipcRenderer.send("desktop:bootstrap-event", "preload:error", {
      preload_path: preloadPathLabel,
      message: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack ?? null : null,
    });
  } catch {
    // If preload itself is collapsing, keep the failure local and let the main process
    // fall back to the startup artifact path it already owns.
  }
  throw error;
}

export type OperatorDesktopApi = typeof operatorDesktop;
