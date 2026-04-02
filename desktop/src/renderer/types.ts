export type JsonRecord = Record<string, any>;

export interface DesktopCommandResult {
  ok: boolean;
  message: string;
  detail?: string;
  output?: string;
  state?: DesktopState;
  payload?: JsonRecord;
}

export interface DesktopSourceStatus {
  mode: "live_api" | "snapshot_fallback" | "degraded_reconnecting" | "backend_down";
  label: string;
  detail: string;
  canRunLiveActions: boolean;
  healthReachable: boolean;
  apiReachable: boolean;
}

export interface DesktopBackendStatus {
  state: "starting" | "healthy" | "reconnecting" | "degraded" | "backend_down";
  label: string;
  detail: string;
  lastError: string | null;
  nextRetryAt: string | null;
  retryCount: number;
  pid: number | null;
  apiStatus: "responding" | "timed_out" | "unreachable" | "unknown";
  healthStatus: "ok" | "degraded" | "unreachable" | "unknown";
  managerOwned: boolean;
  startupFailureKind:
    | "none"
    | "stale_dashboard_instance"
    | "stale_listener_conflict"
    | "build_mismatch"
    | "dashboard_api_not_ready"
    | "early_process_exit"
    | "permission_or_bind_failure"
    | "environment_failure"
    | "unexpected_startup_failure";
  actionHint: string | null;
  staleListenerDetected: boolean;
  healthReachable: boolean;
  dashboardApiTimedOut: boolean;
  portConflictDetected: boolean;
}

export interface DesktopStartupStatus {
  preferredHost: string;
  preferredPort: number;
  preferredUrl: string;
  allowPortFallback: boolean;
  chosenHost: string | null;
  chosenPort: number | null;
  chosenUrl: string | null;
  ownership: "attached_existing" | "started_managed" | "snapshot_only" | "unavailable";
  latestEvent: string | null;
  recentEvents: string[];
  failureKind:
    | "none"
    | "stale_dashboard_instance"
    | "stale_listener_conflict"
    | "build_mismatch"
    | "dashboard_api_not_ready"
    | "early_process_exit"
    | "permission_or_bind_failure"
    | "environment_failure"
    | "unexpected_startup_failure";
  recommendedAction: string | null;
  staleListenerDetected: boolean;
  healthReachable: boolean;
  dashboardApiTimedOut: boolean;
  managedExitCode: number | null;
  managedExitSignal: string | null;
}

export interface LocalOperatorAuthState {
  auth_available: boolean;
  auth_platform: string;
  auth_method: "TOUCH_ID" | "PASSWORD_FALLBACK" | "NONE";
  last_authenticated_at: string | null;
  last_auth_result:
    | "NONE"
    | "SUCCEEDED"
    | "FAILED"
    | "CANCELED"
    | "UNAVAILABLE"
    | "NOT_ENROLLED"
    | "LOCKOUT"
    | "EXPIRED";
  last_auth_detail: string | null;
  auth_session_expires_at: string | null;
  auth_session_active: boolean;
  local_operator_identity: string | null;
  auth_session_id: string | null;
  touch_id_available: boolean;
  secret_protection: {
    available: boolean;
    provider: "KEYCHAIN_SAFE_STORAGE" | "NONE";
    wrapper_ready: boolean;
    wrapper_path: string | null;
    protects_token_file_directly: boolean;
    detail: string;
  };
  latest_event: JsonRecord | null;
  recent_events: JsonRecord[];
  artifacts: {
    state_path: string;
    events_path: string;
    secret_wrapper_path: string;
  };
}

export interface DesktopState {
  connection: "live" | "snapshot" | "unavailable";
  dashboard: JsonRecord | null;
  health: JsonRecord | null;
  backendUrl: string | null;
  source: DesktopSourceStatus;
  backend: DesktopBackendStatus;
  startup: DesktopStartupStatus;
  infoFiles: string[];
  errors: string[];
  runtimeLogPath: string | null;
  backendLogPath: string | null;
  desktopLogPath: string | null;
  appVersion: string;
  manager: {
    running: boolean;
    lastExitCode: number | null;
    lastExitSignal: string | null;
    recentOutput: string[];
  };
  localAuth: LocalOperatorAuthState;
  refreshedAt: string;
}

export interface OperatorDesktopApi {
  getDesktopState(): Promise<DesktopState>;
  startDashboard(): Promise<DesktopCommandResult>;
  stopDashboard(): Promise<DesktopCommandResult>;
  restartDashboard(): Promise<DesktopCommandResult>;
  runDashboardAction(action: string, payload?: JsonRecord): Promise<DesktopCommandResult>;
  runProductionLinkAction(action: string, payload: JsonRecord): Promise<DesktopCommandResult>;
  authenticateLocalOperator(reason?: string): Promise<DesktopCommandResult>;
  clearLocalOperatorAuthSession(): Promise<DesktopCommandResult>;
  openPath(targetPath: string): Promise<DesktopCommandResult>;
  openExternalUrl(url: string): Promise<DesktopCommandResult>;
  copyText(text: string): Promise<DesktopCommandResult>;
}
