type JsonRecord = Record<string, unknown>;

export type Tone = "good" | "warn" | "danger" | "muted";

export type AttentionActionKind =
  | "start-dashboard"
  | "start-paper"
  | "restart-paper-with-temp-paper"
  | "auth-gate-check"
  | "complete-pre-session-review"
  | "paper-force-reconcile"
  | "paper-resume-entries"
  | "research-runtime-bridge-start-supervisor"
  | "research-runtime-bridge-stop-supervisor"
  | "research-runtime-bridge-run-cycle-now"
  | "research-runtime-bridge-acknowledge-anomaly"
  | "research-runtime-bridge-mark-reviewed"
  | "research-runtime-bridge-resolve-anomaly"
  | "open-runtime-events"
  | "refresh";

export interface AttentionActionSpec {
  label: string;
  description: string;
  kind?: AttentionActionKind;
  disabled?: boolean;
  disabledReason?: string;
}

export interface OperationalReadinessModel {
  overallState: "READY" | "WARMING" | "RECONCILING" | "ATTENTION_REQUIRED" | "BLOCKED";
  severityLabel: "INFORMATIONAL" | "WARNING" | "BLOCKING";
  tone: Tone;
  appUsableForSupervisedPaper: boolean;
  unusableReason: string | null;
  dashboardAttached: boolean;
  liveActionsAllowed: boolean;
  launchAllowed: boolean;
  paperRuntimeReady: boolean;
  paperRuntimeState: "READY" | "HALTED" | "BLOCKED" | "RECONCILING" | "UNKNOWN";
  tempPaperBlocked: boolean;
  summaryLine: string;
  primaryIssueTitle: string;
  primaryReason: string;
  primaryStateCode: string;
  explanation: string;
  primaryAction: AttentionActionSpec;
  secondaryAction?: AttentionActionSpec;
  evidenceTarget: string | null;
  evidenceLabel: string;
  dependencyCounts: {
    ready: number;
    warming: number;
    blocked: number;
    degraded: number;
    reconciliationRequired: number;
    needsAttentionNow: number;
  };
}

export interface OperationalReadinessInput {
  connection: string | null | undefined;
  backendUrl: string | null | undefined;
  source: JsonRecord | null | undefined;
  backend: JsonRecord | null | undefined;
  startup: JsonRecord | null | undefined;
  startupControlPlane: JsonRecord | null | undefined;
  supervisedPaperOperability: JsonRecord | null | undefined;
  paperReadiness: JsonRecord | null | undefined;
  temporaryPaperRuntimeIntegrity: JsonRecord | null | undefined;
  authReadyForPaperStartup: boolean;
}

function asRecord(value: unknown): JsonRecord {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as JsonRecord) : {};
}

function asNumber(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function text(value: unknown): string {
  return value === null || value === undefined ? "" : String(value).trim();
}

function upper(value: unknown): string {
  return text(value).toUpperCase();
}

function lower(value: unknown): string {
  return text(value).toLowerCase();
}

function fallback(value: unknown, defaultValue: string): string {
  const resolved = text(value);
  return resolved || defaultValue;
}

export function startupControlPlaneTone(state: unknown): Tone {
  const normalized = upper(state);
  if (normalized === "READY") {
    return "good";
  }
  if (normalized === "WARMING" || normalized === "DEGRADED" || normalized === "RECONCILING" || normalized === "ATTENTION_REQUIRED") {
    return "warn";
  }
  if (normalized === "BLOCKED" || normalized === "RECONCILIATION_REQUIRED" || normalized === "HALTED") {
    return "danger";
  }
  return "muted";
}

function startupControlPlaneActionRequiresLive(kind: AttentionActionKind | undefined): boolean {
  return (
    kind === "start-paper" ||
    kind === "restart-paper-with-temp-paper" ||
    kind === "complete-pre-session-review" ||
    kind === "paper-force-reconcile" ||
    kind === "paper-resume-entries"
  );
}

export function startupControlPlaneActionSpec(
  row: JsonRecord | null | undefined,
  options: { canRunLiveActions: boolean },
): AttentionActionSpec {
  const normalized = asRecord(row);
  const nextActionKind = text(normalized.next_action_kind) as AttentionActionKind | "";
  const nextActionLabel = fallback(normalized.next_action_label, "Refresh");
  const nextActionDetail = fallback(normalized.next_action_detail, fallback(normalized.reason, "Refresh the startup dependency status."));
  const kind = (nextActionKind || "refresh") as AttentionActionKind;
  const requiresLive = startupControlPlaneActionRequiresLive(kind);
  return {
    label: nextActionLabel,
    description: nextActionDetail,
    kind,
    disabled: requiresLive && !options.canRunLiveActions,
    disabledReason: requiresLive && !options.canRunLiveActions ? "Dashboard/API is not attached to a live operator backend yet." : undefined,
  };
}

function unresolvedCounts(controlPlane: JsonRecord): OperationalReadinessModel["dependencyCounts"] {
  const counts = asRecord(controlPlane.counts);
  return {
    ready: asNumber(counts.ready),
    warming: asNumber(counts.warming),
    blocked: asNumber(counts.blocked),
    degraded: asNumber(counts.degraded),
    reconciliationRequired: asNumber(counts.reconciliation_required),
    needsAttentionNow: asNumber(counts.needs_attention_now),
  };
}

function primaryDependency(controlPlane: JsonRecord): JsonRecord {
  const row = asRecord(controlPlane.primary_dependency);
  if (Object.keys(row).length > 0) {
    return row;
  }
  const dependencies = Array.isArray(controlPlane.dependencies) ? (controlPlane.dependencies as unknown[]) : [];
  for (const entry of dependencies) {
    const candidate = asRecord(entry);
    if (upper(candidate.state) !== "READY") {
      return candidate;
    }
  }
  return {};
}

function dashboardAttachState(input: OperationalReadinessInput): {
  dashboardAttached: boolean;
  liveActionsAllowed: boolean;
  healthOnly: boolean;
  snapshotFallback: boolean;
  startingOrRecovering: boolean;
} {
  const source = asRecord(input.source);
  const backend = asRecord(input.backend);
  const sourceMode = lower(source.mode);
  const connection = lower(input.connection);
  const backendUrlPresent = Boolean(text(input.backendUrl));
  const sourceHealthReachable = source.healthReachable === true;
  const sourceApiReachable = source.apiReachable === true;
  const apiResponding = lower(backend.apiStatus) === "responding";
  const attachedSnapshotBridge = sourceMode === "attached_snapshot_bridge";
  const dashboardPayloadServing =
    backendUrlPresent &&
    sourceMode === "live_api" &&
    sourceApiReachable &&
    apiResponding;
  return {
    dashboardAttached: dashboardPayloadServing || attachedSnapshotBridge,
    liveActionsAllowed: dashboardPayloadServing,
    healthOnly:
      !dashboardPayloadServing &&
      !attachedSnapshotBridge &&
      ((sourceHealthReachable && !sourceApiReachable) ||
        (lower(backend.healthStatus) === "ok" && !apiResponding) ||
        backend.dashboardApiTimedOut === true),
    snapshotFallback: !attachedSnapshotBridge && (sourceMode === "snapshot_fallback" || connection === "snapshot"),
    startingOrRecovering:
      sourceMode === "degraded_reconnecting" ||
      lower(backend.state) === "starting" ||
      lower(backend.state) === "reconnecting",
  };
}

function paperRuntimeState(input: OperationalReadinessInput, dashboardAttached: boolean): {
  ready: boolean;
  state: OperationalReadinessModel["paperRuntimeState"];
  tempPaperBlocked: boolean;
  reason: string | null;
  primaryAction: AttentionActionSpec;
} {
  const paperReadiness = asRecord(input.paperReadiness);
  const integrity = asRecord(input.temporaryPaperRuntimeIntegrity);
  const mismatchStatus = upper(integrity.mismatch_status);
  const explicitTempPaperBlocked = integrity.temp_paper_blocked;
  const tempPaperBlocked =
    explicitTempPaperBlocked === true
      ? true
      : explicitTempPaperBlocked === false
        ? false
        : Boolean(mismatchStatus) && !["MATCHED", "CLEAR"].includes(mismatchStatus);
  const runtimePhase = upper(paperReadiness.runtime_phase || paperReadiness.phase || paperReadiness.status);
  const runtimeRunning = paperReadiness.runtime_running === true || runtimePhase === "RUNNING";
  const operatorHalt = paperReadiness.operator_halt === true;
  const entriesEnabled = paperReadiness.entries_enabled === true;
  const halted =
    runtimePhase === "STOPPED" ||
    runtimePhase === "HALTED" ||
    runtimePhase === "BLOCKED" ||
    runtimePhase.includes("HALT") ||
    runtimePhase.includes("STOPPED") ||
    runtimePhase.includes("SUPPRESSED");
  const reconciling =
    runtimePhase.includes("RECOVER") ||
    runtimePhase.includes("BACKOFF") ||
    runtimePhase.includes("PROGRESS") ||
    runtimePhase.includes("STARTING");

  if (!dashboardAttached) {
    return {
      ready: false,
      state: "UNKNOWN",
      tempPaperBlocked,
      reason: null,
      primaryAction: {
        label: "Start Dashboard/API",
        description: "Bring the local dashboard backend online so paper-runtime controls and fresh operator state are available.",
        kind: "start-dashboard",
      },
    };
  }

  if (tempPaperBlocked) {
    return {
      ready: false,
      state: "BLOCKED",
      tempPaperBlocked: true,
      reason: fallback(
        integrity.block_reason || integrity.summary_line || integrity.note,
        "Temp-paper runtime integrity is blocked and the running paper environment cannot be trusted yet.",
      ),
      primaryAction: {
        label: fallback(integrity.clearing_action, "Restart Runtime + Temp Paper"),
        description:
          fallback(
            integrity.block_reason,
            "Reload the paper runtime with the enabled temporary paper overlays so runtime truth matches the admitted temp-paper set.",
          ),
        kind: fallback(integrity.clearing_action, "Restart Runtime + Temp Paper") === "Restart Runtime + Temp Paper"
          ? "restart-paper-with-temp-paper"
          : "refresh",
      },
    };
  }

  if (!input.authReadyForPaperStartup) {
    return {
      ready: false,
      state: "BLOCKED",
      tempPaperBlocked: false,
      reason: fallback(paperReadiness.auth_reason, "Paper runtime auth/readiness is not yet green."),
      primaryAction: {
        label: "Auth Gate Check",
        description: "Verify the paper-runtime auth and broker readiness gates before starting or trusting paper execution.",
        kind: "auth-gate-check",
      },
    };
  }

  if (runtimeRunning && (operatorHalt || !entriesEnabled)) {
    return {
      ready: false,
      state: "HALTED",
      tempPaperBlocked: false,
      reason: fallback(
        paperReadiness.runtime_status_detail || paperReadiness.state_note || paperReadiness.summary_line,
        "Paper runtime is attached, but entries are still halted and supervised paper operation is not usable yet.",
      ),
      primaryAction: {
        label: "Resume Entries",
        description: "Re-arm the supervised paper runtime only after the current operator or risk hold is understood.",
        kind: "paper-resume-entries",
      },
    };
  }

  if (halted || !runtimeRunning) {
    return {
      ready: false,
      state: "HALTED",
      tempPaperBlocked: false,
      reason: fallback(
        paperReadiness.runtime_status_detail || paperReadiness.state_note || paperReadiness.summary_line,
        "Paper runtime is halted and requires operator action before the system can be treated as operational.",
      ),
      primaryAction: {
        label: "Start Runtime",
        description: "Start the paper-only runtime once the current startup blockers are understood.",
        kind: "start-paper",
      },
    };
  }

  if (reconciling) {
    return {
      ready: false,
      state: "RECONCILING",
      tempPaperBlocked: false,
      reason: fallback(
        paperReadiness.summary_line || paperReadiness.runtime_status_detail,
        "Paper runtime is still reconciling and is not yet in a boring operational state.",
      ),
      primaryAction: {
        label: "Refresh",
        description: "Refresh the paper-runtime state after the current reconciliation cycle completes.",
        kind: "refresh",
      },
    };
  }

  return {
    ready: true,
    state: "READY",
    tempPaperBlocked: false,
    reason: fallback(paperReadiness.summary_line, "Paper runtime is attached and operational."),
    primaryAction: {
      label: "Refresh",
      description: "Refresh the operator snapshot when you want the latest paper-runtime status.",
      kind: "refresh",
    },
  };
}

export function deriveOperationalReadiness(input: OperationalReadinessInput): OperationalReadinessModel {
  const source = asRecord(input.source);
  const backend = asRecord(input.backend);
  const startup = asRecord(input.startup);
  const controlPlane = asRecord(input.startupControlPlane);
  const supervisedPaperOperability = asRecord(input.supervisedPaperOperability);
  const attachedSnapshotBridge = lower(source.mode) === "attached_snapshot_bridge";
  const dependencyRow = primaryDependency(controlPlane);
  const attach = dashboardAttachState(input);
  const counts = unresolvedCounts(controlPlane);
  const rawLaunchAllowed = controlPlane.launch_allowed === true;
  const rawOverallState = upper(controlPlane.overall_state);
  const evidenceTarget = text(dependencyRow.authoritative_artifact || controlPlane.authoritative_artifact) || null;
  const evidenceLabel = fallback(
    dependencyRow.authoritative_artifact_label || controlPlane.authoritative_artifact_label,
    "Startup control plane artifact",
  );

  if (attach.startingOrRecovering) {
    return {
      overallState: "WARMING",
      severityLabel: "WARNING",
      tone: "warn",
      appUsableForSupervisedPaper: false,
      unusableReason: "Dashboard/API startup is still warming and supervised paper attach is not established yet.",
      dashboardAttached: false,
      liveActionsAllowed: false,
      launchAllowed: false,
      paperRuntimeReady: false,
      paperRuntimeState: "UNKNOWN",
      tempPaperBlocked: false,
      summaryLine: "Dashboard/API startup is still warming and live attachment is not established yet.",
      primaryIssueTitle: "Dashboard/API startup is still warming.",
      primaryReason: fallback(backend.detail || source.detail, "Managed startup is still converging."),
      primaryStateCode: fallback(backend.state, "STARTING"),
      explanation: "Live attachment requires both /health and /api/dashboard to agree. Until that completes, the operator surface must fail closed.",
      primaryAction: {
        label: "Refresh",
        description: "Refresh the startup state after the current warmup attempt progresses.",
        kind: "refresh",
      },
      secondaryAction: undefined,
      evidenceTarget,
      evidenceLabel,
      dependencyCounts: counts,
    };
  }

  if (!attach.dashboardAttached) {
    if (attach.healthOnly) {
      return {
        overallState: "RECONCILING",
        severityLabel: "BLOCKING",
        tone: "danger",
        appUsableForSupervisedPaper: false,
        unusableReason: "Dashboard/API health is reachable, but /api/dashboard is not attached yet.",
        dashboardAttached: false,
        liveActionsAllowed: false,
        launchAllowed: false,
        paperRuntimeReady: false,
        paperRuntimeState: "UNKNOWN",
        tempPaperBlocked: false,
        summaryLine: "Dashboard/API health is reachable, but full attachment is incomplete because /api/dashboard is not ready.",
        primaryIssueTitle: "Dashboard/API attach is incomplete.",
        primaryReason: "Live /health is reachable, but /api/dashboard is not yet responsive enough to trust the operator surface as attached.",
        primaryStateCode: "DASHBOARD_API_NOT_READY",
        explanation: "Health-only is not operational readiness. The operator surface must stay non-green until both health and dashboard payload truth agree.",
        primaryAction: {
          label: "Refresh",
          description: "Re-check dashboard/API attachment after the backend warmup path completes.",
          kind: "refresh",
        },
        secondaryAction: {
          label: "Start Dashboard/API",
          description: "Restart the local dashboard manager if the attach path does not converge on its own.",
          kind: "start-dashboard",
        },
        evidenceTarget,
        evidenceLabel,
        dependencyCounts: counts,
      };
    }

    if (attach.snapshotFallback) {
      return {
        overallState: "RECONCILING",
        severityLabel: "BLOCKING",
        tone: "danger",
        appUsableForSupervisedPaper: false,
        unusableReason: "Snapshot fallback is active, so supervised paper attach is unavailable.",
        dashboardAttached: false,
        liveActionsAllowed: false,
        launchAllowed: false,
        paperRuntimeReady: false,
        paperRuntimeState: "RECONCILING",
        tempPaperBlocked: false,
        summaryLine: "Snapshot fallback is active; the live dashboard API is not attached.",
        primaryIssueTitle: "Snapshot fallback is active.",
        primaryReason: fallback(source.detail || backend.detail, "The desktop is running from persisted snapshots because live dashboard attachment is unavailable."),
        primaryStateCode: "SNAPSHOT_FALLBACK_ONLY",
        explanation: "Snapshot fallback is useful evidence, but it is not an attached operational state and must not render as launch-ready.",
        primaryAction: {
          label: "Start Dashboard/API",
          description: "Bring the live dashboard backend online so the operator surface can attach and enable paper-runtime actions.",
          kind: "start-dashboard",
        },
        secondaryAction: {
          label: "Refresh",
          description: "Refresh the snapshot-backed operator state while waiting for live attachment.",
          kind: "refresh",
        },
        evidenceTarget,
        evidenceLabel,
        dependencyCounts: counts,
      };
    }

    return {
      overallState: "BLOCKED",
      severityLabel: "BLOCKING",
      tone: "danger",
      appUsableForSupervisedPaper: false,
      unusableReason: "Dashboard/API is not attached to the live backend.",
      dashboardAttached: false,
      liveActionsAllowed: false,
      launchAllowed: false,
      paperRuntimeReady: false,
      paperRuntimeState: "UNKNOWN",
      tempPaperBlocked: false,
      summaryLine: "Dashboard/API is not fully attached, so the system is not operational right now.",
      primaryIssueTitle: "Dashboard/API is not fully attached.",
      primaryReason: fallback(backend.detail || source.detail, "The desktop is not currently attached to the live dashboard API."),
      primaryStateCode: fallback(backend.state || startup.failureKind, "BACKEND_UNAVAILABLE"),
      explanation: "No panel should present green readiness until the desktop is attached to a live dashboard URL with responsive /health and /api/dashboard endpoints.",
      primaryAction: {
        label: "Start Dashboard/API",
        description: "Bring the local dashboard backend online so runtime controls and fresh operator state become available.",
        kind: "start-dashboard",
      },
      secondaryAction: {
        label: "Refresh",
        description: "Re-check backend attachment if another process may have already started it.",
        kind: "refresh",
      },
      evidenceTarget,
      evidenceLabel,
      dependencyCounts: counts,
    };
  }

  if (supervisedPaperOperability.app_usable_for_supervised_paper === false) {
    const contractState = upper(supervisedPaperOperability.state);
    const integrity = asRecord(input.temporaryPaperRuntimeIntegrity);
    const contractTempPaperBlocked =
      upper(supervisedPaperOperability.unusable_reason_code) === "TEMP_PAPER_RUNTIME_MISMATCH"
      || integrity.temp_paper_blocked === true
      || ["MISMATCH", "INSTANCE_ONLY"].includes(upper(integrity.mismatch_status));
    const contractReason = fallback(
      supervisedPaperOperability.unusable_reason || supervisedPaperOperability.summary_line,
      "Application is not usable for supervised paper operation yet.",
    );
    const contractActionLabel = fallback(supervisedPaperOperability.primary_next_action, "Refresh");
    const contractAction: AttentionActionSpec =
      contractActionLabel === "Resume Entries"
        ? {
            label: "Resume Entries",
            description: "Re-arm the supervised paper runtime once the current hold is understood.",
            kind: "paper-resume-entries",
          }
        : contractActionLabel === "Start Runtime"
          ? {
              label: "Start Runtime",
              description: "Start the paper-only runtime once the current startup blockers are understood.",
              kind: "start-paper",
            }
          : {
              label: contractActionLabel,
              description: "Re-check supervised paper usability after the current operator state changes.",
              kind: "refresh",
            };
    return {
      overallState: contractState === "ATTACH_INCOMPLETE" ? "RECONCILING" : "ATTENTION_REQUIRED",
      severityLabel: "BLOCKING",
      tone: "danger",
      appUsableForSupervisedPaper: false,
      unusableReason: contractReason,
      dashboardAttached: attach.dashboardAttached,
      liveActionsAllowed: attach.liveActionsAllowed,
      launchAllowed: false,
      paperRuntimeReady: false,
      paperRuntimeState: contractState === "ATTACH_INCOMPLETE" ? "UNKNOWN" : contractTempPaperBlocked ? "BLOCKED" : "HALTED",
      tempPaperBlocked: contractTempPaperBlocked,
      summaryLine: fallback(supervisedPaperOperability.summary_line, contractReason),
      primaryIssueTitle: "Application is not usable for supervised paper operation.",
      primaryReason: contractReason,
      primaryStateCode: fallback(supervisedPaperOperability.unusable_reason_code, "SUPERVISED_PAPER_NOT_USABLE"),
      explanation: "Product usability is stricter than startup reachability. The app is only usable when dashboard attach, paper-runtime truth, and operator action state all agree.",
      primaryAction: contractAction,
      secondaryAction: {
        label: "Open Runtime Events",
        description: "Inspect the current runtime evidence and hold state before retrying recovery.",
        kind: "open-runtime-events",
      },
      evidenceTarget,
      evidenceLabel,
      dependencyCounts: counts,
    };
  }

  const paper = paperRuntimeState(input, attach.dashboardAttached);
  if (!paper.ready) {
    return {
      overallState: "ATTENTION_REQUIRED",
      severityLabel: "BLOCKING",
      tone: "danger",
      appUsableForSupervisedPaper: false,
      unusableReason: paper.reason ?? "Paper runtime is attached but not yet operational.",
      dashboardAttached: true,
      liveActionsAllowed: attach.liveActionsAllowed,
      launchAllowed: false,
      paperRuntimeReady: false,
      paperRuntimeState: paper.state,
      tempPaperBlocked: paper.tempPaperBlocked,
      summaryLine: paper.reason ?? "Paper runtime is attached but not yet operational.",
      primaryIssueTitle:
        paper.state === "BLOCKED"
          ? "Paper runtime is blocked."
          : paper.state === "HALTED"
            ? "Paper runtime is halted."
            : "Paper runtime is still reconciling.",
      primaryReason: paper.reason ?? "Paper runtime needs explicit operator attention before it can be treated as ready.",
      primaryStateCode:
        paper.state === "BLOCKED"
          ? (paper.tempPaperBlocked ? "TEMP_PAPER_BLOCKED" : "PAPER_RUNTIME_BLOCKED")
          : paper.state === "HALTED"
            ? "PAPER_RUNTIME_HALTED"
            : "PAPER_RUNTIME_RECONCILING",
      explanation: "Dashboard attachment is necessary but not sufficient. Paper mode is only operational when the runtime is attached, unblocked, and actively running.",
      primaryAction: paper.primaryAction,
      secondaryAction: {
        label: "Open Runtime Events",
        description: "Inspect the current paper-runtime evidence and blocking context before retrying recovery.",
        kind: "open-runtime-events",
      },
      evidenceTarget,
      evidenceLabel,
      dependencyCounts: counts,
    };
  }

  if ((rawOverallState && rawOverallState !== "READY") || !rawLaunchAllowed) {
    const action = startupControlPlaneActionSpec(dependencyRow, { canRunLiveActions: attach.liveActionsAllowed });
    return {
      overallState: "ATTENTION_REQUIRED",
      severityLabel: "WARNING",
      tone: "warn",
      appUsableForSupervisedPaper: false,
      unusableReason: fallback(controlPlane.primary_reason, "Startup dependency reconciliation is still required."),
      dashboardAttached: true,
      liveActionsAllowed: attach.liveActionsAllowed,
      launchAllowed: false,
      paperRuntimeReady: true,
      paperRuntimeState: "READY",
      tempPaperBlocked: false,
      summaryLine: fallback(controlPlane.summary_line, rawLaunchAllowed ? "Startup dependency reconciliation is still required." : "Startup contract is still holding launch allowance."),
      primaryIssueTitle: fallback(
        controlPlane.primary_issue_title,
        rawLaunchAllowed ? "Startup dependency reconciliation is still required." : "Startup contract is still holding launch allowance.",
      ),
      primaryReason: fallback(
        controlPlane.primary_reason,
        rawLaunchAllowed
          ? fallback(controlPlane.summary_line, "Dependencies are not yet fully reconciled.")
          : "The dashboard is attached, but the startup contract has not yet allowed a clean launch-ready state.",
      ),
      primaryStateCode: fallback(controlPlane.primary_reason_code, rawLaunchAllowed ? rawOverallState : "LAUNCH_HELD"),
      explanation: "The dashboard is attached, but the published startup dependency contract still disagrees with a boring ready state. Keep the top-level verdict conservative until that contract converges.",
      primaryAction: action,
      secondaryAction:
        action.kind === "refresh"
          ? undefined
          : {
              label: "Refresh",
              description: "Refresh the startup dependency registry after the current dependency state changes.",
              kind: "refresh",
            },
      evidenceTarget,
      evidenceLabel,
      dependencyCounts: counts,
    };
  }

  return {
    overallState: "READY",
    severityLabel: "INFORMATIONAL",
    tone: "good",
    appUsableForSupervisedPaper: true,
    unusableReason: null,
    dashboardAttached: true,
    liveActionsAllowed: attach.liveActionsAllowed,
    launchAllowed: rawLaunchAllowed,
    paperRuntimeReady: true,
    paperRuntimeState: "READY",
    tempPaperBlocked: false,
    summaryLine: attachedSnapshotBridge
      ? "Attached backend readiness is healthy, paper runtime is operational, and startup dependency truth is aligned."
      : "Dashboard/API is attached, paper runtime is operational, and startup dependency truth is aligned.",
    primaryIssueTitle: "System is attached and operational.",
    primaryReason: attachedSnapshotBridge
      ? "Attached backend readiness is healthy and the paper runtime is not blocked, even though this launch context cannot use direct localhost API transport."
      : "Live /health and /api/dashboard are both responsive, and the paper runtime is not blocked.",
    primaryStateCode: rawLaunchAllowed ? "READY" : "READY_ATTACH_BUT_LAUNCH_HELD",
    explanation: rawLaunchAllowed
      ? (attachedSnapshotBridge
          ? "This launch context is using the attached backend readiness contract with the latest persisted operator snapshot. That is sufficient for trustworthy read-only operational state, even though direct live-action transport is still restricted."
          : "This is the boring operational state we want before any deeper paper-runtime work continues.")
      : "The system is attached and operational, but the startup contract is still conservatively holding launch allowance.",
    primaryAction: {
      label: "Refresh",
      description: "Refresh the operator snapshot when you want the latest startup and paper-runtime state.",
      kind: "refresh",
    },
    secondaryAction: {
      label: "Open Runtime Events",
      description: "Inspect detailed paper-runtime evidence without changing startup state.",
      kind: "open-runtime-events",
    },
    evidenceTarget,
    evidenceLabel,
    dependencyCounts: counts,
  };
}
