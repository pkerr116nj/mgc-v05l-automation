import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const prototypeRoot = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = process.env.OBSERVATORY_REPO_ROOT
  ? path.resolve(process.env.OBSERVATORY_REPO_ROOT)
  : path.resolve(prototypeRoot, "../../..");
const python = process.env.OBSERVATORY_PYTHON
  ? path.resolve(process.env.OBSERVATORY_PYTHON)
  : fs.existsSync(path.join(repoRoot, ".venv/bin/python"))
    ? path.join(repoRoot, ".venv/bin/python")
    : "python3";
const lockPath = process.env.OBSERVATORY_REFRESH_LOCK_PATH
  ? path.resolve(process.env.OBSERVATORY_REFRESH_LOCK_PATH)
  : path.join(os.tmpdir(), "mgc_observatory_refresh_service.lock");
const statusPath = process.env.OBSERVATORY_REFRESH_STATUS_PATH
  ? path.resolve(process.env.OBSERVATORY_REFRESH_STATUS_PATH)
  : path.join(prototypeRoot, "observatory_refresh_service_status.generated.mjs");

const argv = new Set(process.argv.slice(2));
const once = argv.has("--once");
const cycleLimit = numberArg("--cycles");

const CADENCES = Object.freeze({
  market_tape: 30_000,
  system_pipeline: 30_000,
  exposure: 30_000,
  venue_sessions: 60_000,
  breadth: 60_000,
  macro_context: 60_000,
  market_canvas: 300_000,
  operational_health: 30_000,
  runtime_broker_context: 30_000,
  frame_manifest: 15_000,
});

const producers = Object.freeze([
  {
    name: "market_tape",
    cadenceMs: CADENCES.market_tape,
    commands: [["node", ["desktop/prototypes/active-desktop/build_market_tape_snapshot.mjs"]]],
    liveCurrent: true,
    cheap: true,
    sideEffects: "Writes display-only market_tape_snapshot.generated.mjs from Phase-1 completed candles.",
    freshnessSla: "90-180 seconds, inherited from Phase-1 listener lag.",
  },
  {
    name: "system_pipeline",
    cadenceMs: CADENCES.system_pipeline,
    commands: [["node", ["desktop/prototypes/active-desktop/build_system_pipeline_snapshot.mjs"]]],
    liveCurrent: true,
    cheap: true,
    sideEffects: "Writes display-only system_pipeline_snapshot.generated.mjs from existing status artifacts.",
    freshnessSla: "30 seconds display cadence; producer source freshness is passed through.",
  },
  {
    name: "exposure",
    cadenceMs: CADENCES.exposure,
    commands: [["node", ["desktop/prototypes/active-desktop/build_exposure_snapshot.mjs"]]],
    liveCurrent: true,
    cheap: true,
    sideEffects: "Writes display-only exposure_snapshot.generated.mjs from managed positions, open-order truth, and reconciliation artifacts.",
    freshnessSla: "30 seconds display cadence; broker/lifecycle freshness remains source-authored.",
  },
  {
    name: "venue_sessions",
    cadenceMs: CADENCES.venue_sessions,
    commands: [[python, ["-m", "mgc_v05l.app.observatory_global_venue_session"]]],
    liveCurrent: true,
    cheap: true,
    sideEffects: "Writes display-only venue_session_snapshot.generated.mjs from exchange_calendars.",
    freshnessSla: "5 minutes per venue artifact; refreshed every 60 seconds.",
  },
  {
    name: "breadth",
    cadenceMs: CADENCES.breadth,
    commands: [[python, ["-m", "mgc_v05l.app.observatory_market_breadth"]]],
    liveCurrent: true,
    cheap: true,
    sideEffects: "Writes display-only factual breadth JSON from prepared EQUS completed candles.",
    freshnessSla: "1 minute when prepared EQUS candles are current.",
  },
  {
    name: "macro_context",
    cadenceMs: CADENCES.macro_context,
    commands: [
      [python, ["-m", "mgc_v05l.app.observatory_macro_market_context"]],
      ["node", ["desktop/prototypes/active-desktop/build_macro_market_context_snapshot.mjs"]],
    ],
    liveCurrent: true,
    cheap: true,
    sideEffects: "Writes display-only macro context JSON, then prepared macro_market_context_snapshot.generated.mjs.",
    freshnessSla: "1 minute when completed 1m inputs are current.",
  },
  {
    name: "market_canvas",
    cadenceMs: CADENCES.market_canvas,
    commands: [[python, ["-m", "mgc_v05l.app.observatory_market_canvas"]]],
    liveCurrent: true,
    cheap: true,
    sideEffects: "Writes display-only market_canvas_snapshot.generated.mjs from completed 5m participation-quality evidence and factual breadth.",
    freshnessSla: "5 minutes, aligned to completed 5m evidence.",
  },
  {
    name: "operational_health",
    cadenceMs: CADENCES.operational_health,
    commands: [
      [python, ["-m", "mgc_v05l.app.observatory_operational_health"]],
      ["node", ["desktop/prototypes/active-desktop/build_operational_health_snapshot.mjs"]],
    ],
    liveCurrent: true,
    cheap: true,
    sideEffects: "Writes display-only operational health JSON, then prepared operational_health_snapshot.generated.mjs.",
    freshnessSla: "30 seconds display cadence; source timestamps are passed through.",
  },
  {
    name: "runtime_broker_context",
    cadenceMs: CADENCES.runtime_broker_context,
    commands: [
      [python, ["-m", "mgc_v05l.app.observatory_runtime_broker_context"]],
      ["node", ["desktop/prototypes/active-desktop/build_runtime_broker_context_snapshot.mjs"]],
    ],
    liveCurrent: true,
    cheap: true,
    sideEffects: "Writes display-only runtime/broker context JSON, then prepared runtime_broker_context_snapshot.generated.mjs.",
    freshnessSla: "30 seconds display cadence; no direct broker polling.",
  },
  {
    name: "frame_manifest",
    cadenceMs: CADENCES.frame_manifest,
    commands: [["node", ["desktop/prototypes/active-desktop/build_observatory_frame_manifest.mjs"]]],
    liveCurrent: true,
    cheap: true,
    sideEffects: "Writes display-only observatory_frame_manifest.generated.mjs summarizing the frame evidence.",
    freshnessSla: "15 seconds display cadence.",
  },
]);

const serviceState = {
  schema_version: "observatory_refresh_service_v1",
  generated_at: null,
  pid: process.pid,
  repo_root: repoRoot,
  guardrails: {
    display_only: true,
    trading_input: false,
    broker_authority: false,
    runtime_authority: false,
    strategy_input: false,
    research_runtime_bridge: false,
  },
  producers: Object.fromEntries(producers.map((producer) => [producer.name, {
    cadence_ms: producer.cadenceMs,
    freshness_sla: producer.freshnessSla,
    live_current: producer.liveCurrent,
    cheap_enough_for_loop: producer.cheap,
    side_effects: producer.sideEffects,
    last_started_at: null,
    last_finished_at: null,
    last_success_at: null,
    last_duration_ms: null,
    status: "PENDING",
    consecutive_failures: 0,
    last_error: null,
  }])),
};

function numberArg(flag) {
  const index = process.argv.indexOf(flag);
  if (index === -1 || !process.argv[index + 1]) return null;
  const parsed = Number(process.argv[index + 1]);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function acquireLock() {
  try {
    const fd = fs.openSync(lockPath, "wx");
    fs.writeFileSync(fd, JSON.stringify({ pid: process.pid, started_at: new Date().toISOString() }, null, 2));
    return fd;
  } catch (error) {
    console.error(JSON.stringify({
      ok: false,
      error: "observatory_refresh_service_already_running",
      lock_path: lockPath,
    }, null, 2));
    process.exit(2);
  }
}

function releaseLock(fd) {
  try {
    fs.closeSync(fd);
  } catch {
    // The process is already exiting; lock removal below is the meaningful cleanup.
  }
  try {
    fs.unlinkSync(lockPath);
  } catch {
    // Leave cleanup to the next operator if the filesystem refuses the unlink.
  }
}

function runProducer(producer) {
  const state = serviceState.producers[producer.name];
  const started = Date.now();
  state.last_started_at = new Date(started).toISOString();
  state.status = "RUNNING";
  state.last_error = null;
  writeStatus();

  for (const [command, args] of producer.commands) {
    const result = spawnSync(command, args, {
      cwd: repoRoot,
      env: {
        ...process.env,
        PYTHONPATH: path.join(repoRoot, "src"),
        OBSERVATORY_REPO_ROOT: repoRoot,
      },
      encoding: "utf8",
      timeout: 25_000,
      stdio: ["ignore", "pipe", "pipe"],
    });
    if (result.error || result.status !== 0) {
      const finished = Date.now();
      state.last_finished_at = new Date(finished).toISOString();
      state.last_duration_ms = finished - started;
      state.status = "FAILED";
      state.consecutive_failures += 1;
      state.last_error = {
        command,
        args,
        exit_status: result.status,
        error: result.error ? result.error.message : null,
        stderr: String(result.stderr || "").slice(-1800),
      };
      writeStatus();
      return false;
    }
  }

  const finished = Date.now();
  state.last_finished_at = new Date(finished).toISOString();
  state.last_success_at = state.last_finished_at;
  state.last_duration_ms = finished - started;
  state.status = "READY";
  state.consecutive_failures = 0;
  state.last_error = null;
  writeStatus();
  return true;
}

function writeStatus() {
  serviceState.generated_at = new Date().toISOString();
  fs.writeFileSync(
    statusPath,
    `export const observatoryRefreshServiceStatus = ${JSON.stringify(serviceState, null, 2)};\n`,
    "utf8",
  );
}

function runDue(lastRunByName, force = false) {
  const now = Date.now();
  for (const producer of producers) {
    const lastRun = lastRunByName.get(producer.name) || 0;
    if (force || now - lastRun >= producer.cadenceMs) {
      lastRunByName.set(producer.name, now);
      runProducer(producer);
    }
  }
}

const lockFd = acquireLock();
process.on("exit", () => releaseLock(lockFd));
process.on("SIGINT", () => process.exit(0));
process.on("SIGTERM", () => process.exit(0));

const lastRunByName = new Map();
let cycles = 0;
runDue(lastRunByName, true);
cycles += 1;

if (once || (cycleLimit !== null && cycles >= cycleLimit)) {
  writeStatus();
  process.exit(0);
}

const timer = setInterval(() => {
  runDue(lastRunByName, false);
  cycles += 1;
  if (cycleLimit !== null && cycles >= cycleLimit) {
    clearInterval(timer);
    writeStatus();
    process.exit(0);
  }
}, 5_000);
