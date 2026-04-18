import type { OperatorDesktopApi } from "./types";

interface BootstrapShellState {
  state?: string;
  title?: string;
  detail?: string;
  tone?: "booting" | "loading" | "ready" | "fatal";
}

interface BootstrapShellBridge {
  setShellState(update: BootstrapShellState): void;
  hideShell(): void;
  showShell(): void;
  recordInline(stage: string, payload?: Record<string, unknown>): void;
}

declare global {
  interface Window {
    operatorDesktop?: OperatorDesktopApi;
    __MGC_BOOTSTRAP__?: BootstrapShellBridge;
  }
}

export {};
