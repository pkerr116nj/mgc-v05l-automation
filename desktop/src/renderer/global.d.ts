import type { OperatorDesktopApi } from "./types";

declare global {
  interface Window {
    operatorDesktop?: OperatorDesktopApi;
  }
}

export {};
