export type StartupFailureKind =
  | "none"
  | "stale_dashboard_instance"
  | "stale_listener_conflict"
  | "build_mismatch"
  | "dashboard_api_not_ready"
  | "early_process_exit"
  | "permission_or_bind_failure"
  | "environment_failure"
  | "unexpected_startup_failure";

export interface StartupFailureAssessment {
  kind: StartupFailureKind;
  hint: string | null;
  staleListenerDetected: boolean;
  healthReachable: boolean;
  dashboardApiTimedOut: boolean;
  portConflictDetected: boolean;
  buildMismatchDetected: boolean;
  earlyProcessExitDetected: boolean;
}

export interface StartupFailureOptions {
  healthReachable?: boolean;
  dashboardApiTimedOut?: boolean;
}

function parseMarkerMap(detail: string | null): Record<string, string> {
  const markers: Record<string, string> = {};
  for (const rawLine of String(detail ?? "").split(/\r?\n/)) {
    const line = rawLine.trim();
    const match = /^STARTUP_([A-Z_]+)=(.*)$/.exec(line);
    if (!match) {
      continue;
    }
    markers[match[1]] = match[2].trim();
  }
  return markers;
}

function markerBoolean(markers: Record<string, string>, key: string): boolean {
  const value = String(markers[key] ?? "").trim().toLowerCase();
  return value === "1" || value === "true" || value === "yes";
}

function markerKind(markers: Record<string, string>): StartupFailureKind | null {
  const raw = String(markers.FAILURE_KIND ?? "").trim().toLowerCase();
  switch (raw) {
    case "stale_dashboard_instance":
    case "stale_listener_conflict":
    case "build_mismatch":
    case "dashboard_api_not_ready":
    case "early_process_exit":
    case "permission_or_bind_failure":
    case "environment_failure":
    case "unexpected_startup_failure":
      return raw;
    default:
      return null;
  }
}

export function classifyStartupFailure(
  detail: string | null,
  options: StartupFailureOptions = {},
): StartupFailureAssessment {
  const text = String(detail ?? "");
  const normalized = text.toLowerCase();
  const markers = parseMarkerMap(detail);
  const healthReachable = markerBoolean(markers, "HEALTH_REACHABLE") || Boolean(options.healthReachable);
  const dashboardApiTimedOut = markerBoolean(markers, "DASHBOARD_API_TIMED_OUT") || Boolean(options.dashboardApiTimedOut);
  const staleListenerDetected =
    markerBoolean(markers, "STALE_LISTENER_DETECTED") ||
    normalized.includes("listener pid") ||
    normalized.includes("stale listener");
  const portConflictDetected =
    markerBoolean(markers, "PORT_CONFLICT_DETECTED") ||
    staleListenerDetected ||
    normalized.includes("port conflict") ||
    normalized.includes("port is already in use");
  const buildMismatchDetected =
    markerBoolean(markers, "BUILD_MISMATCH") ||
    normalized.includes("build mismatch") ||
    normalized.includes("running different build") ||
    normalized.includes("running ") && normalized.includes("local ") && normalized.includes("build");
  const earlyProcessExitDetected =
    normalized.includes("exited early") ||
    normalized.includes("process exited early") ||
    normalized.includes("early process exit");

  const explicitKind = markerKind(markers);
  const explicitHint = String(markers.NEXT_ACTION ?? "").trim() || null;
  if (explicitKind) {
    return {
      kind: explicitKind,
      hint: explicitHint,
      staleListenerDetected,
      healthReachable,
      dashboardApiTimedOut,
      portConflictDetected,
      buildMismatchDetected,
      earlyProcessExitDetected,
    };
  }

  if (
    normalized.includes("schwab auth bootstrap incomplete") ||
    normalized.includes("missing .venv/bin/activate") ||
    normalized.includes("operator bootstrap failed")
  ) {
    return {
      kind: "environment_failure",
      hint: "Restore the local virtualenv and Schwab environment files, then retry Dashboard/API start.",
      staleListenerDetected,
      healthReachable,
      dashboardApiTimedOut,
      portConflictDetected,
      buildMismatchDetected,
      earlyProcessExitDetected,
    };
  }
  if (
    normalized.includes("permission was denied") ||
    normalized.includes("operation not permitted") ||
    normalized.includes("eacces") ||
    normalized.includes("eaddrnotavail")
  ) {
    return {
      kind: "permission_or_bind_failure",
      hint: "Check local bind permissions, host/port settings, and shell environment, then retry Dashboard/API start.",
      staleListenerDetected,
      healthReachable,
      dashboardApiTimedOut,
      portConflictDetected,
      buildMismatchDetected,
      earlyProcessExitDetected,
    };
  }
  if (buildMismatchDetected) {
    return {
      kind: "build_mismatch",
      hint: "Stop the old local dashboard instance so this desktop session can start the current build cleanly.",
      staleListenerDetected,
      healthReachable,
      dashboardApiTimedOut,
      portConflictDetected,
      buildMismatchDetected,
      earlyProcessExitDetected,
    };
  }
  if (
    normalized.includes("existing dashboard process is not ready") ||
    normalized.includes("could not stop the stale dashboard process cleanly") ||
    normalized.includes("could not stop the old dashboard instance cleanly")
  ) {
    return {
      kind: "stale_dashboard_instance",
      hint: "Stop the stale local dashboard process and retry Dashboard/API start.",
      staleListenerDetected,
      healthReachable,
      dashboardApiTimedOut,
      portConflictDetected,
      buildMismatchDetected,
      earlyProcessExitDetected,
    };
  }
  if (portConflictDetected) {
    return {
      kind: "stale_listener_conflict",
      hint: "Stop the conflicting localhost listener or let the desktop retry after stale-listener cleanup succeeds.",
      staleListenerDetected,
      healthReachable,
      dashboardApiTimedOut,
      portConflictDetected,
      buildMismatchDetected,
      earlyProcessExitDetected,
    };
  }
  if (
    dashboardApiTimedOut ||
    normalized.includes("/api/dashboard is not ready") ||
    normalized.includes("api never became ready") ||
    normalized.includes("did not become responsive before timeout") ||
    normalized.includes("health is reachable") && normalized.includes("/api/dashboard")
  ) {
    return {
      kind: "dashboard_api_not_ready",
      hint: "Health is up but the dashboard payload is not ready. Review the backend log, fix the blocking backend condition, then retry.",
      staleListenerDetected,
      healthReachable,
      dashboardApiTimedOut: true,
      portConflictDetected,
      buildMismatchDetected,
      earlyProcessExitDetected,
    };
  }
  if (earlyProcessExitDetected) {
    return {
      kind: "early_process_exit",
      hint: "The managed dashboard process exited before startup completed. Review the backend log tail and fix the process-level error before retrying.",
      staleListenerDetected,
      healthReachable,
      dashboardApiTimedOut,
      portConflictDetected,
      buildMismatchDetected,
      earlyProcessExitDetected,
    };
  }
  if (!text.trim()) {
    return {
      kind: "none",
      hint: null,
      staleListenerDetected,
      healthReachable,
      dashboardApiTimedOut,
      portConflictDetected,
      buildMismatchDetected,
      earlyProcessExitDetected,
    };
  }
  return {
    kind: "unexpected_startup_failure",
    hint: "Review the desktop and backend logs, then retry Dashboard/API start once the local startup error is understood.",
    staleListenerDetected,
    healthReachable,
    dashboardApiTimedOut,
    portConflictDetected,
    buildMismatchDetected,
    earlyProcessExitDetected,
  };
}

export function isRecoverableLocalDashboardListener(command: string | null | undefined): boolean {
  const normalized = String(command ?? "").toLowerCase();
  if (!normalized.trim()) {
    return false;
  }
  return (
    normalized.includes("mgc_v05l.app.main") && normalized.includes("operator-dashboard")
  ) || (
    normalized.includes("python") && normalized.includes("operator_dashboard")
  );
}

export function shouldAutoReconnectDashboardFailure(
  assessment: StartupFailureAssessment,
  attemptCount: number,
): boolean {
  if (attemptCount >= 3) {
    return false;
  }
  return assessment.kind === "none" || assessment.kind === "early_process_exit";
}
