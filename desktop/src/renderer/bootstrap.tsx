import React, { useEffect } from "react";

type ShellTone = "booting" | "loading" | "ready" | "fatal";

type ShellUpdate = {
  state?: string;
  title?: string;
  detail?: string;
  tone?: ShellTone;
};

function shellBridge() {
  return window.__MGC_BOOTSTRAP__;
}

function errorPayload(error: unknown): { message: string; stack: string | null } {
  if (error instanceof Error) {
    return {
      message: error.message || error.name || "Unknown renderer/bootstrap error.",
      stack: error.stack ?? null,
    };
  }
  return {
    message: String(error ?? "Unknown renderer/bootstrap error."),
    stack: null,
  };
}

export function reportBootstrapEvent(stage: string, payload: Record<string, unknown> = {}): void {
  try {
    window.operatorDesktop?.reportBootstrapEvent?.(stage, payload);
  } catch {
    // Keep the renderer alive even if the bridge call fails.
  }
  try {
    shellBridge()?.recordInline(stage, payload);
  } catch {
    // The visible shell should stay best-effort only.
  }
}

export function setBootstrapShellState(update: ShellUpdate): void {
  try {
    shellBridge()?.setShellState(update);
  } catch {
    // Ignore shell update failures and let the main artifact trail explain the miss.
  }
}

export function hideBootstrapShell(): void {
  try {
    shellBridge()?.hideShell();
  } catch {
    // Ignore shell hide failures.
  }
}

export function installBootstrapGuards(): void {
  setBootstrapShellState({
    state: "LOADING_DESKTOP_STATE",
    title: "Loading operator desktop state…",
    detail: "Renderer bundle loaded. Waiting for preload bridge, desktop state, and first meaningful render.",
    tone: "loading",
  });
  reportBootstrapEvent("renderer:bootstrap-script-start", {
    bridge_available: Boolean(window.operatorDesktop),
    href: window.location.href,
    user_agent: navigator.userAgent,
  });

  window.addEventListener("error", (event) => {
    const target = event.target as HTMLElement | null;
    const resourceTag = target?.tagName?.toLowerCase() || null;
    const sourceUrl =
      (target && ("src" in target ? String((target as HTMLScriptElement).src || "") : "")) ||
      (target && ("href" in target ? String((target as HTMLLinkElement).href || "") : "")) ||
      "";
    const isResourceLoadFailure = Boolean(resourceTag && sourceUrl);
    const detail = isResourceLoadFailure
      ? `Renderer asset failed to load${sourceUrl ? `: ${sourceUrl}` : "."}`
      : errorPayload(event.error || event.message).message;
    setBootstrapShellState({
      state: isResourceLoadFailure ? "RENDERER_ASSET_LOAD_FAILED" : "RENDERER_ERROR",
      title: isResourceLoadFailure ? "Renderer asset failed to load." : "Renderer bootstrap failed.",
      detail,
      tone: "fatal",
    });
    reportBootstrapEvent("renderer:window-error", {
      is_resource_load_failure: isResourceLoadFailure,
      resource_tag: resourceTag,
      source_url: sourceUrl || null,
      message: String(event.message || detail),
      filename: event.filename || null,
      lineno: event.lineno || null,
      colno: event.colno || null,
    });
  });

  window.addEventListener("unhandledrejection", (event) => {
    const payload = errorPayload(event.reason);
    setBootstrapShellState({
      state: "RENDERER_UNHANDLED_REJECTION",
      title: "Renderer bootstrap hit an unhandled promise rejection.",
      detail: payload.message,
      tone: "fatal",
    });
    reportBootstrapEvent("renderer:unhandled-rejection", payload);
  });
}

export function FatalBootstrapScreen(props: {
  title: string;
  detail: string;
  stateCode: string;
  evidenceHint?: string;
}): JSX.Element {
  return (
    <div
      style={{
        minHeight: "100vh",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        background: "linear-gradient(180deg, #101a2b 0%, #060b14 100%)",
        color: "#f5f7fb",
        padding: "28px",
        boxSizing: "border-box",
        fontFamily: "ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, sans-serif",
      }}
    >
      <div
        style={{
          width: "min(860px, 100%)",
          borderRadius: "20px",
          border: "1px solid rgba(255,255,255,0.14)",
          background: "rgba(6, 11, 20, 0.92)",
          boxShadow: "0 24px 80px rgba(0,0,0,0.45)",
          padding: "28px",
        }}
      >
        <div style={{ fontSize: "12px", letterSpacing: "0.14em", textTransform: "uppercase", color: "#f3a94b", marginBottom: "12px" }}>
          Renderer / Bootstrap Failure
        </div>
        <h1 style={{ fontSize: "32px", lineHeight: 1.1, margin: "0 0 12px" }}>{props.title}</h1>
        <p style={{ fontSize: "16px", lineHeight: 1.55, margin: "0 0 18px", color: "rgba(245,247,251,0.86)" }}>{props.detail}</p>
        <div style={{ display: "flex", gap: "10px", flexWrap: "wrap", marginBottom: "18px" }}>
          <span style={{ borderRadius: "999px", background: "rgba(255,119,119,0.18)", border: "1px solid rgba(255,119,119,0.38)", padding: "7px 12px", fontSize: "12px", fontWeight: 700 }}>
            {props.stateCode}
          </span>
          <span style={{ borderRadius: "999px", background: "rgba(243,169,75,0.18)", border: "1px solid rgba(243,169,75,0.38)", padding: "7px 12px", fontSize: "12px", fontWeight: 700 }}>
            PAPER ONLY
          </span>
        </div>
        <div style={{ fontSize: "14px", lineHeight: 1.5, color: "rgba(245,247,251,0.7)" }}>
          {props.evidenceHint ||
            "Evidence: outputs/operator_dashboard/runtime/desktop_electron_startup.json and outputs/operator_dashboard/runtime/desktop_renderer_bootstrap.json"}
        </div>
      </div>
    </div>
  );
}

type BootstrapBoundaryState = {
  error: { message: string; stack: string | null } | null;
};

export class BootstrapErrorBoundary extends React.Component<{ children: React.ReactNode }, BootstrapBoundaryState> {
  state: BootstrapBoundaryState = { error: null };

  static getDerivedStateFromError(error: unknown): BootstrapBoundaryState {
    return { error: errorPayload(error) };
  }

  componentDidCatch(error: unknown, info: React.ErrorInfo): void {
    const payload = errorPayload(error);
    setBootstrapShellState({
      state: "REACT_ROOT_CRASHED",
      title: "The packaged renderer crashed before the operator UI finished rendering.",
      detail: payload.message,
      tone: "fatal",
    });
    reportBootstrapEvent("renderer:error-boundary", {
      message: payload.message,
      stack: payload.stack,
      component_stack: info.componentStack || null,
    });
  }

  render(): React.ReactNode {
    if (this.state.error) {
      return (
        <FatalBootstrapScreen
          title="The packaged renderer crashed before first meaningful paint."
          detail={this.state.error.message}
          stateCode="REACT_ROOT_CRASHED"
        />
      );
    }
    return this.props.children;
  }
}

export function BootstrapReadyMarker(): JSX.Element | null {
  useEffect(() => {
    setBootstrapShellState({
      state: "READY",
      title: "MGC Operator renderer is ready.",
      detail: "First meaningful paint completed. The operator control plane can now load or degrade visibly.",
      tone: "ready",
    });
    reportBootstrapEvent("renderer:first-meaningful-render", {
      bridge_available: Boolean(window.operatorDesktop),
    });
    hideBootstrapShell();
  }, []);
  return null;
}

export function MissingBridgeScreen(): JSX.Element {
  useEffect(() => {
    setBootstrapShellState({
      state: "PRELOAD_BRIDGE_MISSING",
      title: "Electron preload bridge is missing.",
      detail: "The renderer loaded, but window.operatorDesktop was never exposed. This is a preload/bootstrap failure, not a backend readiness problem.",
      tone: "fatal",
    });
    reportBootstrapEvent("renderer:bridge-missing", {
      bridge_available: false,
    });
  }, []);

  return (
    <FatalBootstrapScreen
      title="Electron preload bridge is missing."
      detail="The renderer loaded, but the preload bridge never exposed the desktop API. Inspect packaged preload paths and startup artifacts."
      stateCode="PRELOAD_BRIDGE_MISSING"
    />
  );
}
