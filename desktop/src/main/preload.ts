import { contextBridge, ipcRenderer } from "electron";
import type { DesktopCommandResult, DesktopState } from "./runtime";

const operatorDesktop = {
  getDesktopState: (): Promise<DesktopState> => ipcRenderer.invoke("desktop:get-state"),
  startDashboard: (): Promise<DesktopCommandResult> => ipcRenderer.invoke("desktop:start-dashboard"),
  stopDashboard: (): Promise<DesktopCommandResult> => ipcRenderer.invoke("desktop:stop-dashboard"),
  restartDashboard: (): Promise<DesktopCommandResult> => ipcRenderer.invoke("desktop:restart-dashboard"),
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

contextBridge.exposeInMainWorld("operatorDesktop", operatorDesktop);

export type OperatorDesktopApi = typeof operatorDesktop;
