"use strict";

const DEFAULT_DASHBOARD_READINESS_MAX_AGE_MS = 60_000;

function asRecord(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function asString(value) {
  const normalized = String(value ?? "").trim();
  return normalized || null;
}

function asNumber(value) {
  const normalized = Number(value);
  return Number.isFinite(normalized) ? normalized : null;
}

function isoToMs(value) {
  if (!value) {
    return null;
  }
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeUrl(value) {
  const normalized = asString(value);
  if (!normalized) {
    return null;
  }
  try {
    const parsed = new URL(normalized);
    parsed.hash = "";
    return parsed.toString().replace(/\/$/, "");
  } catch {
    return normalized.replace(/\/$/, "");
  }
}

function nonNullValues(values) {
  return values.filter((value) => value !== null && value !== undefined);
}

function allEqual(values) {
  const filtered = nonNullValues(values);
  if (!filtered.length) {
    return null;
  }
  return filtered.every((value) => value === filtered[0]);
}

function summarizeReadinessSamples(samples) {
  const rows = Array.isArray(samples) ? samples : [];
  const classifications = [...new Set(rows.map((row) => String(row.classification || "")).filter(Boolean))];
  const readinessStates = [...new Set(rows.map((row) => String(row.readiness_state || "")).filter(Boolean))];
  const reasonCodes = [...new Set(rows.map((row) => String(row.reason_code || "")).filter(Boolean))];
  return {
    sample_count: rows.length,
    classifications,
    readiness_states: readinessStates,
    reason_codes: reasonCodes,
    ready_sample_count: rows.filter((row) => row.ready === true || row.readiness_state === "READY").length,
    ambiguous_sample_count: rows.filter((row) => String(row.readiness_state || "") === "AMBIGUOUS").length,
    not_ready_sample_count: rows.filter((row) => String(row.readiness_state || "") === "NOT_READY").length,
    flapping_detected: classifications.length > 1 || readinessStates.length > 1 || reasonCodes.length > 1,
    first_sample: rows[0] || null,
    last_sample: rows.length ? rows[rows.length - 1] : null,
  };
}

function deriveManagerSnapshotTruth(input) {
  const managerSnapshot = asRecord(input?.managerSnapshot);
  const snapshotMeta = asRecord(managerSnapshot.dashboard_meta);
  const startupControlPlane = asRecord(managerSnapshot.startup_control_plane);
  const startupConvergence = asRecord(startupControlPlane.convergence);
  const healthPayload = asRecord(input?.healthPayload);
  const healthChecks = asRecord(healthPayload.checks);
  const maxAgeMs = Math.max(1000, Number(input?.maxAgeMs ?? DEFAULT_DASHBOARD_READINESS_MAX_AGE_MS));
  const generatedAt = asString(managerSnapshot.generated_at);
  const generatedAtMs = isoToMs(generatedAt);
  const ageMs = generatedAtMs === null ? null : Math.max(0, Date.now() - generatedAtMs);
  const snapshotFresh = ageMs !== null && ageMs <= maxAgeMs;
  const healthGeneratedAt =
    asString(healthPayload.generated_at) ||
    asString(healthPayload.last_probe_at) ||
    asString(healthPayload.latest_dashboard_generated_at);
  const healthGeneratedAtMs = isoToMs(healthGeneratedAt);
  const heartbeatAgeMs = healthGeneratedAtMs === null ? null : Math.max(0, Date.now() - healthGeneratedAtMs);
  const heartbeatFresh = heartbeatAgeMs !== null && heartbeatAgeMs <= maxAgeMs;
  const configuredUrl = normalizeUrl(input?.configuredUrl);
  const snapshotUrl = normalizeUrl(snapshotMeta.server_url);
  const infoFilePid = asNumber(input?.infoFilePid);
  const infoFileBuildStamp = asString(input?.infoFileBuildStamp);
  const infoFileInstanceId = asString(input?.infoFileInstanceId);
  const infoFilePidAlive = input?.infoFilePidAlive === true;
  const snapshotPid = asNumber(snapshotMeta.server_pid);
  const snapshotBuildStamp = asString(snapshotMeta.build_stamp);
  const snapshotInstanceId = asString(snapshotMeta.server_instance_id || snapshotMeta.instance_id);
  const healthPid = asNumber(healthPayload.pid);
  const healthBuildStamp = asString(healthPayload.build_stamp);
  const healthInstanceId = asString(healthPayload.instance_id);
  const currentInstanceMatch = infoFileInstanceId ? snapshotInstanceId === infoFileInstanceId : true;
  const currentBuildMatch = infoFileBuildStamp ? snapshotBuildStamp === infoFileBuildStamp : true;
  const currentPidMatch = infoFilePid !== null ? snapshotPid === infoFilePid : true;
  const currentUrlMatch = configuredUrl ? snapshotUrl === configuredUrl : true;
  const heartbeatInstanceMatch = infoFileInstanceId ? healthInstanceId === infoFileInstanceId : true;
  const heartbeatBuildMatch = infoFileBuildStamp ? healthBuildStamp === infoFileBuildStamp : true;
  const heartbeatPidMatch = infoFilePid !== null ? healthPid === infoFilePid : true;
  const healthReady = healthPayload.ready === true && String(healthPayload.status || "").toLowerCase() === "ok";
  const apiRespondingOk = asRecord(healthChecks.api_dashboard_responding).ok !== false;
  const operatorSurfaceOk = asRecord(healthChecks.operator_surface_loadable).ok !== false;
  const startupConvergenceStable = asRecord(healthChecks.startup_convergence_stable).ok !== false;
  const heartbeatAuthoritative =
    heartbeatFresh &&
    heartbeatInstanceMatch &&
    heartbeatBuildMatch &&
    heartbeatPidMatch &&
    healthReady &&
    apiRespondingOk &&
    operatorSurfaceOk &&
    startupConvergenceStable;
  const fresh = snapshotFresh || heartbeatAuthoritative;
  const launchAllowed = startupControlPlane.launch_allowed === true;
  const launchCandidate =
    startupControlPlane.launch_candidate === true || startupControlPlane.dependencies_aligned === true;
  const dashboardAttached = startupConvergence.dashboard_attached === true;
  const paperRuntimeReady = startupConvergence.paper_runtime_ready === true;
  const stableReady = startupConvergence.stable_ready === true;
  const overallState = asString(startupControlPlane.overall_state || startupControlPlane.state);
  const authoritative =
    Object.keys(managerSnapshot).length > 0 &&
    currentInstanceMatch &&
    currentBuildMatch &&
    currentPidMatch &&
    currentUrlMatch;
  const ready =
    authoritative &&
    fresh &&
    infoFilePidAlive &&
    launchAllowed &&
    stableReady &&
    dashboardAttached &&
    paperRuntimeReady &&
    String(overallState || "").toUpperCase() === "READY";

  let reasonCode = "manager_snapshot_missing";
  let reasonDetail = "The manager-owned dashboard snapshot is missing.";
  if (!Object.keys(managerSnapshot).length) {
    reasonCode = "manager_snapshot_missing";
    reasonDetail = "The manager-owned dashboard snapshot is missing.";
  } else if (!authoritative) {
    reasonCode = "manager_snapshot_not_current";
    reasonDetail = "The manager-owned dashboard snapshot does not match the current listener owner, build, or configured URL.";
  } else if (!fresh) {
    reasonCode = "manager_snapshot_stale";
    reasonDetail = "The manager-owned dashboard snapshot is too old to trust for launch readiness.";
  } else if (!launchAllowed || !stableReady || !dashboardAttached || !paperRuntimeReady) {
    reasonCode = asString(startupConvergence.reason_code) || "manager_snapshot_not_ready";
    reasonDetail =
      asString(startupConvergence.reason) ||
      asString(startupControlPlane.primary_reason) ||
      "The manager-owned dashboard snapshot reports that startup convergence is not yet ready.";
  } else if (!infoFilePidAlive) {
    reasonCode = "manager_snapshot_owner_not_alive";
    reasonDetail = "The manager-owned dashboard snapshot was current, but the published owner PID is no longer alive.";
  } else {
    reasonCode = "manager_snapshot_ready";
    reasonDetail = "The manager-owned dashboard snapshot is current, stable, and launch-ready.";
  }

  return {
    present: Object.keys(managerSnapshot).length > 0,
    authoritative,
    fresh,
    snapshot_fresh: snapshotFresh,
    heartbeat_fresh: heartbeatFresh,
    heartbeat_authoritative: heartbeatAuthoritative,
    age_ms: ageMs,
    heartbeat_age_ms: heartbeatAgeMs,
    current_instance_match: currentInstanceMatch,
    current_build_match: currentBuildMatch,
    current_pid_match: currentPidMatch,
    current_url_match: currentUrlMatch,
    launch_allowed: launchAllowed,
    launch_candidate: launchCandidate,
    overall_state: overallState,
    dashboard_attached: dashboardAttached,
    paper_runtime_ready: paperRuntimeReady,
    stable_ready: stableReady,
    generated_at: generatedAt,
    reason_code: reasonCode,
    reason_detail: reasonDetail,
    ready,
    snapshot: managerSnapshot,
  };
}

function classifyDashboardReadiness(input) {
  const observedAt = asString(input?.observedAt) || new Date().toISOString();
  const configuredUrl = asString(input?.configuredUrl);
  const infoFilePresent = input?.infoFilePresent === true || Boolean(configuredUrl);
  const infoFilePid = asNumber(input?.infoFilePid);
  const infoFilePidAlive = input?.infoFilePidAlive === true;
  const infoFileStartedAt = asString(input?.infoFileStartedAt);
  const infoFileBuildStamp = asString(input?.infoFileBuildStamp);
  const infoFileInstanceId = asString(input?.infoFileInstanceId);
  const listenerOwnerPid = asNumber(input?.listenerOwnerPid);
  const listenerOwnerAlive = input?.listenerOwnerAlive === true;
  const listenerOwnerCommand = asString(input?.listenerOwnerCommand);

  const healthProbe = asRecord(input?.healthProbe);
  const dashboardProbe = asRecord(input?.dashboardProbe);
  const healthPayload = asRecord(healthProbe.parsed_json);
  const rawDashboardPayload = asRecord(dashboardProbe.parsed_json);
  const managerTruth = deriveManagerSnapshotTruth({
    managerSnapshot: input?.managerSnapshot,
    configuredUrl,
    infoFilePid,
    infoFilePidAlive,
    infoFileBuildStamp,
    infoFileInstanceId,
    healthPayload,
    maxAgeMs: input?.managerSnapshotMaxAgeMs,
  });
  const authoritativeManagerPayload =
    managerTruth.authoritative && managerTruth.fresh ? asRecord(managerTruth.snapshot) : {};
  const dashboardPayload = Object.keys(authoritativeManagerPayload).length ? authoritativeManagerPayload : rawDashboardPayload;
  const dashboardMeta = asRecord(dashboardPayload.dashboard_meta);
  const startupControlPlane = asRecord(dashboardPayload.startup_control_plane);
  const startupControlPlaneCounts = asRecord(startupControlPlane.counts);
  const startupConvergence = asRecord(startupControlPlane.convergence);
  const healthChecks = asRecord(healthPayload.checks);

  const healthPid = asNumber(healthPayload.pid);
  const payloadPid = asNumber(dashboardMeta.server_pid);
  const healthBuildStamp = asString(healthPayload.build_stamp);
  const payloadBuildStamp = asString(dashboardMeta.build_stamp);
  const healthInstanceId = asString(healthPayload.instance_id);
  const payloadInstanceId = asString(dashboardMeta.server_instance_id || dashboardMeta.instance_id);
  const buildStampMatchesPrimary = allEqual([infoFileBuildStamp, healthBuildStamp]);
  const instanceIdMatchesPrimary = allEqual([infoFileInstanceId, healthInstanceId]);

  const infoPidMatchesListener = infoFilePid !== null && listenerOwnerPid !== null ? infoFilePid === listenerOwnerPid : null;
  const infoPidMatchesHealth = infoFilePid !== null && healthPid !== null ? infoFilePid === healthPid : null;
  const infoPidMatchesPayload = infoFilePid !== null && payloadPid !== null ? infoFilePid === payloadPid : null;
  const healthPidMatchesPayload = healthPid !== null && payloadPid !== null ? healthPid === payloadPid : null;
  const buildStampMatchesAll = allEqual([infoFileBuildStamp, healthBuildStamp, payloadBuildStamp]);
  const instanceIdMatchesAll = allEqual([infoFileInstanceId, healthInstanceId, payloadInstanceId]);

  const listenerReachable = healthProbe.listener_bound === true;
  const healthReady =
    listenerReachable &&
    healthProbe.json_valid === true &&
    healthPayload.ready === true &&
    String(healthPayload.status || "").toLowerCase() === "ok";
  const healthChecksOk =
    asRecord(healthChecks.api_dashboard_responding).ok === true &&
    asRecord(healthChecks.operator_surface_loadable).ok === true;
  const startupControlPlanePresent = Object.keys(startupControlPlane).length > 0;
  const startupControlPlaneState = asString(startupControlPlane.overall_state || startupControlPlane.state);
  const startupControlPlaneReason = asString(
    startupControlPlane.primary_reason || startupControlPlane.summary_line || startupControlPlane.reason_detail,
  );
  const startupControlPlanePrimaryReasonCode = asString(startupControlPlane.primary_reason_code);
  const startupControlPlanePrimaryDependencyKey = asString(
    startupControlPlane.primary_dependency_key || asRecord(startupControlPlane.primary_dependency).key,
  );
  const startupControlPlaneLaunchAllowed = startupControlPlane.launch_allowed === true;
  const startupControlPlaneLaunchCandidate =
    startupControlPlane.launch_candidate === true || startupControlPlane.dependencies_aligned === true;
  const startupConvergencePhase = asString(startupConvergence.phase);
  const startupConvergenceReason = asString(startupConvergence.reason);
  const startupConvergenceStableReady = startupConvergence.stable_ready === true;
  const startupConvergenceDashboardAttached = startupConvergence.dashboard_attached === true;
  const startupConvergencePaperRuntimeReady = startupConvergence.paper_runtime_ready === true;
  const payloadSource = Object.keys(authoritativeManagerPayload).length ? "MANAGER_SNAPSHOT" : "HTTP_API";
  const payloadReachable = dashboardProbe.ok === true || Object.keys(authoritativeManagerPayload).length > 0;
  const payloadJsonValid = dashboardProbe.json_valid === true || Object.keys(authoritativeManagerPayload).length > 0;
  const dashboardPayloadReady =
    Boolean(dashboardPayload.operator_surface) &&
    Object.keys(dashboardMeta).length > 0 &&
    startupControlPlanePresent;
  const payloadLikelyStaleFromPriorInstance =
    dashboardProbe.ok === true &&
    dashboardPayloadReady &&
    !healthReady &&
    (
      (payloadPid !== null && infoFilePid !== null && payloadPid !== infoFilePid) ||
      (payloadInstanceId !== null && infoFileInstanceId !== null && payloadInstanceId !== infoFileInstanceId)
    );
  const ownerSignals = payloadLikelyStaleFromPriorInstance
    ? [infoPidMatchesListener, infoPidMatchesHealth]
    : [infoPidMatchesListener, infoPidMatchesHealth, infoPidMatchesPayload, healthPidMatchesPayload];
  const ownerSignalsKnown = ownerSignals.some((value) => value !== null);
  const ownerSignalsAgree = ownerSignalsKnown
    ? ownerSignals.filter((value) => value !== null).every((value) => value === true)
    : null;

  const identitySignals = payloadLikelyStaleFromPriorInstance
    ? [buildStampMatchesPrimary, instanceIdMatchesPrimary]
    : [buildStampMatchesAll, instanceIdMatchesAll];
  const identitySignalsKnown = identitySignals.some((value) => value !== null);
  const identitySignalsAgree = identitySignalsKnown
    ? identitySignals.filter((value) => value !== null).every((value) => value === true)
    : null;
  const payloadStableReady =
    !payloadLikelyStaleFromPriorInstance &&
    payloadReachable &&
    payloadJsonValid &&
    dashboardPayloadReady &&
    startupConvergenceDashboardAttached &&
    startupConvergencePaperRuntimeReady &&
    (
      (startupControlPlaneLaunchAllowed && startupConvergenceStableReady) ||
      (
        startupControlPlaneLaunchCandidate &&
        startupControlPlaneState === "WARMING" &&
        startupControlPlanePrimaryReasonCode === "stability_window_incomplete" &&
        startupControlPlanePrimaryDependencyKey === "dashboard_backend" &&
        healthChecksOk
      )
    );

  let readinessState = "READY";
  let classification = "READY";
  let reasonCode = "ready";
  let reasonDetail = "Dashboard ownership, health, and payload identity agree and are ready for packaged automation.";

  if (!configuredUrl) {
    readinessState = "NOT_READY";
    classification = "DASHBOARD_NOT_CONFIGURED";
    reasonCode = "listener_not_started";
    reasonDetail = "The dashboard manager has not yet published a configured dashboard URL.";
  } else if (!infoFilePresent || (infoFilePid !== null && !infoFilePidAlive) || (listenerOwnerPid !== null && !listenerOwnerAlive)) {
    readinessState = listenerReachable ? "AMBIGUOUS" : "NOT_READY";
    classification = "DASHBOARD_OWNERSHIP_NOT_TRUSTWORTHY";
    reasonCode = listenerReachable ? "ownership_not_trustworthy" : "listener_not_started";
    reasonDetail = listenerReachable
      ? "The dashboard listener responded, but the published owner PID was not alive for this readiness sample."
      : "The dashboard manager has not yet published a live owner for the listener.";
  } else if (
    managerTruth.ready &&
    (ownerSignalsKnown ? ownerSignalsAgree !== false : true) &&
    (identitySignalsKnown ? identitySignalsAgree !== false : true) &&
    listenerOwnerAlive
  ) {
    readinessState = "READY";
    classification = "MANAGER_SNAPSHOT_READY";
    reasonCode = "manager_snapshot_ready";
    reasonDetail =
      "The manager-owned current-instance dashboard snapshot is stable READY and takes precedence over transient probe misses.";
  } else if (!listenerReachable) {
    readinessState = ownerSignalsKnown ? "AMBIGUOUS" : "NOT_READY";
    classification = ownerSignalsKnown ? "DASHBOARD_OWNERSHIP_NOT_TRUSTWORTHY" : "DASHBOARD_LISTENER_NOT_STARTED";
    reasonCode = ownerSignalsKnown ? "ownership_not_trustworthy" : "listener_not_started";
    reasonDetail = ownerSignalsKnown
      ? "Ownership signals existed, but the listener was unreachable for this readiness sample."
      : "No healthy dashboard listener was reachable on the configured URL.";
  } else if (healthProbe.json_valid !== true) {
    readinessState = "AMBIGUOUS";
    classification = "DASHBOARD_OWNERSHIP_NOT_TRUSTWORTHY";
    reasonCode = "ownership_not_trustworthy";
    reasonDetail = "The health endpoint responded, but not with valid JSON.";
  } else if ((ownerSignalsKnown && !ownerSignalsAgree) || (identitySignalsKnown && !identitySignalsAgree)) {
    readinessState = "AMBIGUOUS";
    classification = ownerSignalsKnown && !ownerSignalsAgree ? "DASHBOARD_OWNERSHIP_NOT_TRUSTWORTHY" : "DASHBOARD_IDENTITY_MISMATCH";
    reasonCode = ownerSignalsKnown && !ownerSignalsAgree ? "ownership_not_trustworthy" : "identity_mismatch";
    reasonDetail = ownerSignalsKnown && !ownerSignalsAgree
      ? "The info file, listener owner, health endpoint, and payload did not agree on ownership for this readiness sample."
      : "The dashboard payload identity did not match the published dashboard build or instance identity.";
  } else if (payloadStableReady) {
    readinessState = "READY";
    classification = "CURRENT_PAYLOAD_READY";
    reasonCode = "payload_ready";
    reasonDetail = "The current-instance dashboard payload is already stable and launch-ready, even though health readiness is still catching up.";
  } else if (!healthReady || !healthChecksOk) {
    readinessState = "NOT_READY";
    classification = "DASHBOARD_SERVICE_WARMING";
    reasonCode = "service_warming";
    reasonDetail = "The dashboard listener is up, but health and self-check readiness are not yet fully green.";
  } else if (!dashboardPayloadReady) {
    readinessState = "NOT_READY";
    classification = "DASHBOARD_PAYLOAD_NOT_READY";
    reasonCode = "payload_not_ready";
    reasonDetail = "The dashboard listener is healthy, but the published dashboard payload is not yet reachable and valid.";
  } else if (
    !startupControlPlaneLaunchAllowed ||
    !startupConvergenceStableReady ||
    !startupConvergenceDashboardAttached ||
    !startupConvergencePaperRuntimeReady
  ) {
    readinessState = "NOT_READY";
    classification = "DASHBOARD_STARTUP_NOT_READY";
    reasonCode = "startup_dependencies_not_ready";
    reasonDetail =
      startupConvergenceReason ||
      startupControlPlaneReason ||
      "The dashboard control plane reports that startup dependencies are not yet ready for packaged automation launch.";
  }

  return {
    observed_at: observedAt,
    readiness_state: readinessState,
    classification,
    reason_code: reasonCode,
    reason_detail: reasonDetail,
    ready: readinessState === "READY",
    configured_url: configuredUrl,
    info_file: {
      present: infoFilePresent,
      pid: infoFilePid,
      pid_alive: infoFilePidAlive,
      started_at: infoFileStartedAt,
      build_stamp: infoFileBuildStamp,
      instance_id: infoFileInstanceId,
    },
    listener_owner: {
      pid: listenerOwnerPid,
      alive: listenerOwnerAlive,
      command: listenerOwnerCommand,
    },
    health: {
      reachable: listenerReachable,
      json_valid: healthProbe.json_valid === true,
      ready: healthReady,
      checks_ok: healthChecksOk,
      pid: healthPid,
      build_stamp: healthBuildStamp,
      instance_id: healthInstanceId,
      status: asString(healthPayload.status),
      generated_at: asString(healthPayload.generated_at),
    },
    payload: {
      reachable: payloadReachable,
      source: payloadSource,
      http_reachable: dashboardProbe.ok === true,
      json_valid: payloadJsonValid,
      ready: dashboardPayloadReady,
      pid: payloadPid,
      build_stamp: payloadBuildStamp,
      instance_id: payloadInstanceId,
      generated_at: asString(dashboardPayload.generated_at),
      startup_control_plane_present: startupControlPlanePresent,
      startup_control_plane_state: startupControlPlaneState,
      startup_control_plane_launch_allowed: startupControlPlaneLaunchAllowed,
      startup_control_plane_launch_candidate: startupControlPlaneLaunchCandidate,
    },
    control_plane: {
      present: startupControlPlanePresent,
      state: startupControlPlaneState,
      launch_allowed: startupControlPlaneLaunchAllowed,
      launch_candidate: startupControlPlaneLaunchCandidate,
      convergence_phase: startupConvergencePhase,
      convergence_reason: startupConvergenceReason,
      convergence_stable_ready: startupConvergenceStableReady,
      dashboard_attached: startupConvergenceDashboardAttached,
      paper_runtime_ready: startupConvergencePaperRuntimeReady,
      primary_dependency_key: startupControlPlanePrimaryDependencyKey,
      reason: startupControlPlaneReason,
      blocked_count: asNumber(startupControlPlaneCounts.blocked),
      warming_count: asNumber(startupControlPlaneCounts.warming),
      degraded_count: asNumber(startupControlPlaneCounts.degraded),
      reconciliation_required_count: asNumber(startupControlPlaneCounts.reconciliation_required),
      needs_attention_now_count: asNumber(startupControlPlaneCounts.needs_attention_now),
    },
    manager_truth: managerTruth,
    ownership: {
      owner_signals_known: ownerSignalsKnown,
      owner_signals_agree: ownerSignalsAgree,
      info_pid_matches_listener: infoPidMatchesListener,
      info_pid_matches_health: infoPidMatchesHealth,
      info_pid_matches_payload: infoPidMatchesPayload,
      health_pid_matches_payload: healthPidMatchesPayload,
    },
    identity: {
      identity_signals_known: identitySignalsKnown,
      identity_signals_agree: identitySignalsAgree,
      build_stamp_matches_all: buildStampMatchesAll,
      instance_id_matches_all: instanceIdMatchesAll,
    },
  };
}

function buildDashboardReadinessContract(options) {
  const samples = Array.isArray(options?.samples) ? options.samples : [];
  const sampleHistoryLimit = Math.max(4, Number(options?.sampleHistoryLimit ?? 8));
  const recentSamples = samples.slice(-sampleHistoryLimit);
  const stabilityWindowMs = Math.max(500, Number(options?.stabilityWindowMs ?? 1500));
  const minStableSamples = Math.max(2, Number(options?.minStableSamples ?? 3));
  const sampleIntervalMs = Math.max(100, Number(options?.sampleIntervalMs ?? 500));
  const leaseTtlMs = Math.max(sampleIntervalMs * 4, Number(options?.leaseTtlMs ?? 5000));
  const publisher = asRecord(options?.publisher);
  const summary = summarizeReadinessSamples(recentSamples);
  const lastSample = summary.last_sample || null;

  const trailingReadySamples = [];
  let trailingSignature = null;
  for (let index = recentSamples.length - 1; index >= 0; index -= 1) {
    const sample = recentSamples[index];
    const eligible =
      (
        sample?.manager_truth?.ready === true ||
        (
          sample?.readiness_state === "READY" &&
          sample?.payload?.ready === true &&
          sample?.control_plane?.launch_allowed === true &&
          sample?.control_plane?.convergence_stable_ready === true &&
          sample?.control_plane?.dashboard_attached === true &&
          sample?.control_plane?.paper_runtime_ready === true &&
          (
            (sample?.health?.ready === true && sample?.health?.checks_ok === true)
            || sample?.reason_code === "payload_ready"
          )
        )
      ) &&
      sample?.ownership?.owner_signals_agree !== false &&
      sample?.identity?.identity_signals_agree !== false &&
      sample?.info_file?.pid_alive === true &&
      sample?.listener_owner?.alive === true;
    if (!eligible) {
      break;
    }
    const signature = [
      String(sample?.configured_url || ""),
      String(sample?.info_file?.pid ?? ""),
      String(sample?.info_file?.instance_id ?? ""),
      String(sample?.info_file?.build_stamp ?? ""),
    ].join(":");
    if (trailingSignature === null) {
      trailingSignature = signature;
    } else if (trailingSignature !== signature) {
      break;
    }
    trailingReadySamples.unshift(sample);
  }

  const stableSampleCount = trailingReadySamples.length;
  const stableSince = stableSampleCount ? trailingReadySamples[0].observed_at : null;
  const stableUntil = stableSampleCount ? trailingReadySamples[stableSampleCount - 1].observed_at : null;
  const stableWindowObservedMs =
    stableSampleCount >= 2
      ? Math.max(0, (isoToMs(stableUntil) ?? 0) - (isoToMs(stableSince) ?? 0))
      : 0;
  const payloadReadyBypassCandidate =
    lastSample?.readiness_state === "READY" &&
    lastSample?.reason_code === "payload_ready" &&
    lastSample?.ownership?.owner_signals_agree !== false &&
    lastSample?.identity?.identity_signals_agree !== false &&
    lastSample?.control_plane?.dashboard_attached === true &&
    lastSample?.control_plane?.paper_runtime_ready === true;
  const contradictionCount = samples.length > 1 ? Math.max(0, samples.length - stableSampleCount) : 0;
  const stableWindowSatisfied = stableSampleCount >= minStableSamples && stableWindowObservedMs >= stabilityWindowMs;
  const disruptiveHistoryDetected = recentSamples.some((sample) =>
    sample?.ownership?.owner_signals_agree === false ||
    sample?.identity?.identity_signals_agree === false ||
    String(sample?.readiness_state || "") === "AMBIGUOUS" ||
    ["ownership_not_trustworthy", "identity_mismatch"].includes(String(sample?.reason_code || "")));
  const payloadReadyBypass = payloadReadyBypassCandidate && !disruptiveHistoryDetected;
  const launchAllowed = payloadReadyBypass || stableWindowSatisfied;

  let readinessState = lastSample?.readiness_state ?? "NOT_READY";
  let reasonCode = lastSample?.reason_code ?? "listener_not_started";
  let reasonDetail = lastSample?.reason_detail ?? "The dashboard manager has not yet produced a launch-ready dashboard contract.";

  if (launchAllowed) {
    readinessState = "READY";
    if (stableWindowSatisfied) {
      reasonCode = "ready";
      reasonDetail = "Dashboard ownership, health, payload validity, and identity remained stable for the required readiness window.";
    } else if (payloadReadyBypass) {
      reasonCode = "payload_ready";
      reasonDetail =
        "The current-instance dashboard payload is ready for launch, and the only remaining gate was passive manager warmup.";
    }
  } else if (summary.flapping_detected || (lastSample?.readiness_state === "READY" && !launchAllowed)) {
    readinessState = "AMBIGUOUS";
    reasonCode = "stability_window_not_met";
    reasonDetail =
      "Recent dashboard signals did not remain consistent for the minimum stability window, so packaged automation launch is still refused.";
  }

  const generatedAt = asString(options?.generatedAt) || new Date().toISOString();
  const generatedAtMs = isoToMs(generatedAt);
  const leaseExpiresAt =
    generatedAtMs !== null ? new Date(generatedAtMs + leaseTtlMs).toISOString() : null;
  const lastOwnership = asRecord(lastSample?.ownership);
  const lastIdentity = asRecord(lastSample?.identity);
  const lastInfoFile = asRecord(lastSample?.info_file);
  const lastListenerOwner = asRecord(lastSample?.listener_owner);
  const lastHealth = asRecord(lastSample?.health);
  const lastPayload = asRecord(lastSample?.payload);
  const lastControlPlane = asRecord(lastSample?.control_plane);

  return {
    contract_version: "dashboard_readiness_contract.v1",
    generated_at: generatedAt,
    readiness_state: readinessState,
    reason_code: reasonCode,
    reason_detail: reasonDetail,
    launch_allowed: launchAllowed,
    configured_url: lastSample?.configured_url ?? asString(options?.configuredUrl),
    publisher: {
      source: asString(publisher.source) || "dashboard_manager",
      manager_pid: asNumber(publisher.managerPid),
      manager_mode: asString(publisher.managerMode),
      manager_instance_id: asString(publisher.managerInstanceId),
      pid: asNumber(publisher.pid),
      issued_at: generatedAt,
    },
    lease: {
      renewed_at: generatedAt,
      expires_at: leaseExpiresAt,
      lease_ttl_ms: leaseTtlMs,
      heartbeat_interval_ms: sampleIntervalMs,
      owner_generation: asString(publisher.managerInstanceId),
    },
    ownership: {
      info_file_present: lastInfoFile.present === true,
      info_file_pid: asNumber(lastInfoFile.pid),
      info_file_pid_alive: lastInfoFile.pid_alive === true,
      info_file_started_at: asString(lastInfoFile.started_at),
      listener_owner_pid: asNumber(lastListenerOwner.pid),
      listener_owner_alive: lastListenerOwner.alive === true,
      listener_owner_command: asString(lastListenerOwner.command),
      owner_pid_matches_info: lastOwnership.info_pid_matches_listener ?? null,
      owner_pid_matches_health: lastOwnership.info_pid_matches_health ?? null,
      owner_pid_matches_payload: lastOwnership.info_pid_matches_payload ?? null,
      health_pid_matches_payload: lastOwnership.health_pid_matches_payload ?? null,
      owner_signals_agree: lastOwnership.owner_signals_agree ?? null,
      owner_signals_known: lastOwnership.owner_signals_known ?? false,
      info_file_instance_id: asString(lastInfoFile.instance_id),
      instance_id_matches_all: lastIdentity.instance_id_matches_all ?? null,
      build_stamp_matches_all: lastIdentity.build_stamp_matches_all ?? null,
      identity_signals_agree: lastIdentity.identity_signals_agree ?? null,
    },
    listener: {
      reachable: lastHealth.reachable === true,
      health_url: lastSample?.configured_url ? `${String(lastSample.configured_url).replace(/\/$/, "")}/health` : null,
      dashboard_api_url: lastSample?.configured_url ? `${String(lastSample.configured_url).replace(/\/$/, "")}/api/dashboard` : null,
    },
    health: {
      status: asString(lastHealth.status),
      ready: lastHealth.ready === true,
      checks_ok: lastHealth.checks_ok === true,
      pid: asNumber(lastHealth.pid),
      build_stamp: asString(lastHealth.build_stamp),
      instance_id: asString(lastHealth.instance_id),
      generated_at: asString(lastHealth.generated_at),
    },
    payload: {
      reachable: lastPayload.reachable === true,
      json_valid: lastPayload.json_valid === true,
      ready: lastPayload.ready === true,
      pid: asNumber(lastPayload.pid),
      build_stamp: asString(lastPayload.build_stamp),
      instance_id: asString(lastPayload.instance_id),
      generated_at: asString(lastPayload.generated_at),
      startup_control_plane_present: lastPayload.startup_control_plane_present === true,
      startup_control_plane_state: asString(lastPayload.startup_control_plane_state),
      startup_control_plane_launch_allowed: lastPayload.startup_control_plane_launch_allowed === true,
    },
    control_plane: {
      present: lastControlPlane.present === true,
      state: asString(lastControlPlane.state),
      launch_allowed: lastControlPlane.launch_allowed === true,
      convergence_phase: asString(lastControlPlane.convergence_phase),
      convergence_reason: asString(lastControlPlane.convergence_reason),
      convergence_stable_ready: lastControlPlane.convergence_stable_ready === true,
      dashboard_attached: lastControlPlane.dashboard_attached === true,
      paper_runtime_ready: lastControlPlane.paper_runtime_ready === true,
      primary_dependency_key: asString(lastControlPlane.primary_dependency_key),
      reason: asString(lastControlPlane.reason),
      blocked_count: asNumber(lastControlPlane.blocked_count),
      warming_count: asNumber(lastControlPlane.warming_count),
      degraded_count: asNumber(lastControlPlane.degraded_count),
      reconciliation_required_count: asNumber(lastControlPlane.reconciliation_required_count),
      needs_attention_now_count: asNumber(lastControlPlane.needs_attention_now_count),
    },
    stability: {
      required_window_ms: stabilityWindowMs,
      required_sample_count: minStableSamples,
      sample_interval_ms: sampleIntervalMs,
      sample_count: summary.sample_count,
      sample_history_limit: sampleHistoryLimit,
      stable_sample_count: stableSampleCount,
      stable_window_observed_ms: stableWindowObservedMs,
      stable_since: stableSince,
      stable_until: stableUntil,
      contradiction_count: contradictionCount,
      flapping_detected: summary.flapping_detected,
      classifications_seen: summary.classifications,
      reason_codes_seen: summary.reason_codes,
    },
    samples: recentSamples.slice(-8).map((sample) => ({
      observed_at: sample.observed_at,
      readiness_state: sample.readiness_state,
      classification: sample.classification,
      reason_code: sample.reason_code,
      info_file_pid: sample?.info_file?.pid ?? null,
      info_file_pid_alive: sample?.info_file?.pid_alive === true,
      listener_owner_pid: sample?.listener_owner?.pid ?? null,
      listener_owner_alive: sample?.listener_owner?.alive === true,
      health_ready: sample?.health?.ready === true,
      health_checks_ok: sample?.health?.checks_ok === true,
      payload_ready: sample?.payload?.ready === true,
      control_plane_launch_allowed: sample?.control_plane?.launch_allowed === true,
      convergence_stable_ready: sample?.control_plane?.convergence_stable_ready === true,
      dashboard_attached: sample?.control_plane?.dashboard_attached === true,
      paper_runtime_ready: sample?.control_plane?.paper_runtime_ready === true,
      owner_signals_agree: sample?.ownership?.owner_signals_agree ?? null,
      identity_signals_agree: sample?.identity?.identity_signals_agree ?? null,
    })),
  };
}

function verifyDashboardReadinessContract(contract, options = {}) {
  if (!contract || typeof contract !== "object") {
    return {
      ready: false,
      freshness_ok: false,
      classification: "DASHBOARD_CONTRACT_MISSING",
      reason_code: "dashboard_contract_missing",
      reason_detail: "The dashboard manager has not published a readiness contract yet.",
    };
  }

  const maxAgeMs = Math.max(1000, Number(options.maxAgeMs ?? DEFAULT_DASHBOARD_READINESS_MAX_AGE_MS));
  const generatedAtMs = isoToMs(contract.generated_at);
  const ageMs = generatedAtMs === null ? null : Math.max(0, Date.now() - generatedAtMs);
  const freshnessOk = ageMs !== null && ageMs <= maxAgeMs;
  const lease = asRecord(contract.lease);
  const leaseExpiresAtMs = isoToMs(lease.expires_at);
  const leaseValid = leaseExpiresAtMs !== null && Date.now() <= leaseExpiresAtMs;
  const configuredUrl = asString(options.configuredUrl);
  const contractUrl = asString(contract.configured_url);
  const urlMatches = !configuredUrl || !contractUrl || configuredUrl === contractUrl;

  const ownership = asRecord(contract.ownership);
  const ownerAlive = ownership.info_file_pid_alive === true && ownership.listener_owner_alive === true;
  const ownerAgrees =
    ownership.owner_signals_agree !== false &&
    ownership.owner_pid_matches_info !== false &&
    ownership.owner_pid_matches_health !== false &&
    ownership.owner_pid_matches_payload !== false;
  const identityAgrees =
    ownership.identity_signals_agree !== false &&
    ownership.instance_id_matches_all !== false &&
    ownership.build_stamp_matches_all !== false;

  if (!freshnessOk) {
    return {
      ready: false,
      freshness_ok: false,
      lease_valid: leaseValid,
      contract_age_ms: ageMs,
      classification: "DASHBOARD_CONTRACT_STALE",
      reason_code: "dashboard_contract_stale",
      reason_detail: "The published readiness contract is too old to trust for automated packaged launch.",
      contract,
    };
  }
  if (!leaseValid) {
    return {
      ready: false,
      freshness_ok: true,
      lease_valid: false,
      contract_age_ms: ageMs,
      classification: "DASHBOARD_CONTRACT_LEASE_EXPIRED",
      reason_code: "dashboard_contract_lease_expired",
      reason_detail: "The published readiness lease expired before packaged launch verification completed.",
      contract,
    };
  }
  if (!urlMatches) {
    return {
      ready: false,
      freshness_ok: true,
      lease_valid: true,
      contract_age_ms: ageMs,
      classification: "DASHBOARD_CONTRACT_URL_MISMATCH",
      reason_code: "dashboard_contract_url_mismatch",
      reason_detail: "The published readiness contract does not match the launcher-configured dashboard URL.",
      contract,
    };
  }
  if (!ownerAlive || !ownerAgrees || !identityAgrees) {
    return {
      ready: false,
      freshness_ok: true,
      lease_valid: true,
      contract_age_ms: ageMs,
      classification: "DASHBOARD_CONTRACT_OWNERSHIP_NOT_TRUSTWORTHY",
      reason_code: "dashboard_contract_ownership_not_trustworthy",
      reason_detail: "The published readiness contract does not show live, agreeing ownership and identity signals.",
      contract,
    };
  }
  if (contract.launch_allowed !== true || String(contract.readiness_state || "") !== "READY") {
    return {
      ready: false,
      freshness_ok: true,
      lease_valid: true,
      contract_age_ms: ageMs,
      classification: "DASHBOARD_CONTRACT_NOT_READY",
      reason_code: String(contract.reason_code || "dashboard_contract_not_ready"),
      reason_detail: String(
        contract.reason_detail || "The dashboard manager did not mark this contract launch-ready for packaged automation.",
      ),
      contract,
    };
  }

  return {
    ready: true,
    freshness_ok: true,
    lease_valid: true,
    contract_age_ms: ageMs,
    classification: "READY",
    reason_code: "ready",
    reason_detail: "A fresh dashboard readiness contract allows packaged automation launch.",
    contract,
  };
}

function resolveLaunchReadiness(options = {}) {
  const configuredUrl = asString(options.configuredUrl);
  const contract = asRecord(options.contract);
  const contractVerification = verifyDashboardReadinessContract(contract, {
    maxAgeMs: options.maxAgeMs,
    configuredUrl,
  });
  const infoFilePid = asNumber(options.infoFilePid);
  const infoFilePidAlive = options.infoFilePidAlive === true;
  const listenerOwnerPidAlive = options.listenerOwnerAlive === true;
  const managerTruth = deriveManagerSnapshotTruth({
    managerSnapshot: options.managerSnapshot,
    healthPayload: options.healthPayload,
    configuredUrl,
    infoFilePid,
    infoFilePidAlive,
    infoFileBuildStamp: options.infoFileBuildStamp,
    infoFileInstanceId: options.infoFileInstanceId,
    maxAgeMs: options.maxAgeMs,
  });

  if (!configuredUrl && managerTruth.ready) {
    return {
      ready: false,
      freshness_ok: true,
      lease_valid: contractVerification.lease_valid ?? true,
      contract_age_ms: contractVerification.contract_age_ms ?? null,
      classification: "MANAGER_READY_POLICY_HOLD",
      reason_code: "configured_url_missing",
      reason_detail: "Manager truth is READY, but launcher policy is holding because no configured dashboard URL was published.",
      truth_source: "MANAGER_SNAPSHOT",
      policy_state: "HOLD",
      manager_truth: managerTruth,
      contract,
    };
  }

  if (managerTruth.ready && infoFilePidAlive && listenerOwnerPidAlive) {
    const contractReason = asString(contractVerification.reason_code);
    const contractReasonSuffix =
      contractReason && contractReason !== "ready"
        ? ` Contract verification is currently ${contractReason}, so authoritative manager snapshot truth is overriding stale or lagging secondary readiness publishing.`
        : "";
    return {
      ready: true,
      freshness_ok: true,
      lease_valid: contractVerification.lease_valid ?? true,
      contract_age_ms: contractVerification.contract_age_ms ?? null,
      classification: "MANAGER_SNAPSHOT_READY",
      reason_code: "manager_snapshot_ready",
      reason_detail:
        `Manager-owned current-instance startup truth is stable READY; preferring authoritative manager snapshot over lagging readiness contract.${contractReasonSuffix}`,
      truth_source: "MANAGER_SNAPSHOT",
      policy_state: "ALLOW",
      manager_truth: managerTruth,
      contract,
    };
  }

  return {
    truth_source: "READINESS_CONTRACT",
    policy_state: contractVerification.ready ? "ALLOW" : "REFUSE",
    manager_truth: managerTruth,
    ...contractVerification,
  };
}

module.exports = {
  buildDashboardReadinessContract,
  classifyDashboardReadiness,
  deriveManagerSnapshotTruth,
  resolveLaunchReadiness,
  summarizeReadinessSamples,
  verifyDashboardReadinessContract,
};
