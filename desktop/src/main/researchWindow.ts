import type { BrowserWindowConstructorOptions } from "electron";

export function researchControlCenterWindowOptions(preloadPath: string): BrowserWindowConstructorOptions {
  return {
    width: 1440,
    height: 920,
    minWidth: 1180,
    minHeight: 760,
    backgroundColor: "#0b1320",
    title: "Research Control Center",
    webPreferences: {
      preload: preloadPath,
      contextIsolation: true,
      nodeIntegration: false,
    },
  };
}

export function researchControlCenterHashRoute(): string {
  return "#/research-control-center";
}
