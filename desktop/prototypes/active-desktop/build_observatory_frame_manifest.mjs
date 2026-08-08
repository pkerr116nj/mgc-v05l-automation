import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const prototypeRoot = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = process.env.OBSERVATORY_REPO_ROOT
  ? path.resolve(process.env.OBSERVATORY_REPO_ROOT)
  : path.resolve(prototypeRoot, "../../..");
const outputPath = process.env.OBSERVATORY_FRAME_MANIFEST_OUTPUT_PATH
  ? path.resolve(process.env.OBSERVATORY_FRAME_MANIFEST_OUTPUT_PATH)
  : path.join(prototypeRoot, "observatory_frame_manifest.generated.mjs");

const COMPONENTS = Object.freeze([
  {
    name: "market_tape",
    required: false,
    path: "desktop/prototypes/active-desktop/market_tape_snapshot.generated.mjs",
    exportName: "marketTapeSnapshot",
  },
  {
    name: "system_pipeline",
    required: false,
    path: "desktop/prototypes/active-desktop/system_pipeline_snapshot.generated.mjs",
    exportName: "systemPipelineSnapshot",
  },
  {
    name: "exposure",
    required: false,
    path: "desktop/prototypes/active-desktop/exposure_snapshot.generated.mjs",
    exportName: "exposureSnapshot",
  },
  {
    name: "venue_sessions",
    required: false,
    path: "desktop/prototypes/active-desktop/venue_session_snapshot.generated.mjs",
    exportName: "venueSessionSnapshot",
  },
  {
    name: "market_canvas",
    required: false,
    path: "desktop/prototypes/active-desktop/market_canvas_snapshot.generated.mjs",
    exportName: "marketCanvasSnapshot",
  },
  {
    name: "macro_context",
    required: false,
    path: "desktop/prototypes/active-desktop/macro_market_context_snapshot.generated.mjs",
    exportName: "macroMarketContextSnapshot",
  },
  {
    name: "operational_health",
    required: false,
    path: "desktop/prototypes/active-desktop/operational_health_snapshot.generated.mjs",
    exportName: "operationalHealthSnapshot",
  },
  {
    name: "runtime_broker_context",
    required: false,
    path: "desktop/prototypes/active-desktop/runtime_broker_context_snapshot.generated.mjs",
    exportName: "runtimeBrokerContextSnapshot",
  },
]);

function readSnapshot(component) {
  const absolutePath = path.join(repoRoot, component.path);
  if (!fs.existsSync(absolutePath)) {
    return { status: "MISSING", snapshot: null, error: "source_snapshot_missing" };
  }
  const source = fs.readFileSync(absolutePath, "utf8");
  const match = source.match(new RegExp(`export\\s+const\\s+${component.exportName}\\s+=\\s+([\\s\\S]*);\\s*$`));
  if (!match) {
    return { status: "INVALID", snapshot: null, error: "snapshot_export_missing" };
  }
  try {
    return { status: "LOADED", snapshot: JSON.parse(match[1]), error: null };
  } catch (error) {
    return { status: "INVALID", snapshot: null, error: error instanceof Error ? error.message : String(error) };
  }
}

function parseTimestamp(value) {
  if (!value || typeof value !== "string") return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function freshnessFor(snapshot, status, nowMs) {
  if (status !== "LOADED" || !snapshot) return status === "INVALID" ? "INVALID" : "UNKNOWN";
  const explicit = snapshot.freshness;
  if (typeof explicit === "string") return explicit.toUpperCase();
  const generatedAt = parseTimestamp(snapshot.generated_at);
  if (!generatedAt) return "UNKNOWN";
  const ageSeconds = Math.max(0, Math.round((nowMs - generatedAt) / 1000));
  if (ageSeconds <= 90) return "FRESH";
  if (ageSeconds <= 600) return "AGING";
  return "STALE";
}

function buildManifest() {
  const now = new Date();
  const nowMs = now.getTime();
  const components = COMPONENTS.map((component) => {
    const loaded = readSnapshot(component);
    const snapshot = loaded.snapshot;
    return {
      name: component.name,
      schema_version: snapshot?.schema_version || null,
      source_snapshot_path: component.path,
      source_generated_at: snapshot?.generated_at || null,
      source_status: loaded.status,
      freshness: freshnessFor(snapshot, loaded.status, nowMs),
      required: component.required,
      model_status: snapshot?.model_status || snapshot?.status || null,
      error: loaded.error,
    };
  });
  const freshnessValues = components.map((component) => component.freshness);
  const overall = freshnessValues.includes("INVALID")
    ? "INVALID"
    : freshnessValues.includes("STALE")
      ? "STALE"
      : freshnessValues.includes("UNKNOWN")
        ? "VALID_WITH_WARNINGS"
        : freshnessValues.includes("AGING")
          ? "AGING"
          : "FRESH";
  return {
    schema_version: "observatory_frame_manifest_v1",
    generated_at: now.toISOString(),
    frame_id: `observatory_frame_${now.toISOString().replaceAll(/[:.]/g, "_")}`,
    overall_frame_freshness: overall,
    guardrails: {
      display_only: true,
      trading_input: false,
      broker_authority: false,
      runtime_authority: false,
      strategy_input: false,
    },
    components,
  };
}

const manifest = buildManifest();
fs.writeFileSync(
  outputPath,
  `export const observatoryFrameManifest = ${JSON.stringify(manifest, null, 2)};\n`,
  "utf8",
);
console.log(JSON.stringify({
  ok: true,
  output: path.relative(repoRoot, outputPath),
  freshness: manifest.overall_frame_freshness,
  components: manifest.components.length,
}, null, 2));
