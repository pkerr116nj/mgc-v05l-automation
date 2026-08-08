import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { scenarioNames, validateFixtures, venues } from "./fixtures.mjs";
import { worldLand110m } from "./world_land_110m.mjs";

const prototypeRoot = path.dirname(fileURLToPath(import.meta.url));
const requiredFiles = [
  "index.html",
  "styles.css",
  "fixtures.mjs",
  "world_land_110m.mjs",
  "app.mjs",
  "build_market_tape_snapshot.mjs",
  "build_system_pipeline_snapshot.mjs",
  "build_exposure_snapshot.mjs",
  "build_observatory_frame_manifest.mjs",
  "observatory_refresh_service.mjs",
  "build_macro_market_context_snapshot.mjs",
  "build_operational_health_snapshot.mjs",
  "build_runtime_broker_context_snapshot.mjs",
];
const forbiddenTokens = [
  "runtime state",
  "strategy input",
  "submitOrder",
  "cancelOrder",
  "readFileSync(\"outputs",
  "fetch(\"/api",
  "requestAnimationFrame(draw",
];

const errors = [];
const fixtureResult = validateFixtures();
errors.push(...fixtureResult.errors);

for (const fileName of requiredFiles) {
  const filePath = path.join(prototypeRoot, fileName);
  if (!fs.existsSync(filePath)) {
    errors.push(`missing required file: ${fileName}`);
    continue;
  }
  const content = fs.readFileSync(filePath, "utf8");
  for (const token of forbiddenTokens) {
    if (content.includes(token)) errors.push(`${fileName} contains forbidden token: ${token}`);
  }
}

const adapter = fs.readFileSync(path.join(prototypeRoot, "build_market_tape_snapshot.mjs"), "utf8");
for (const token of ["submitOrder", "cancelOrder", "strategy input"]) {
  if (adapter.includes(token)) errors.push(`build_market_tape_snapshot.mjs contains forbidden authority token: ${token}`);
}
for (const token of ["observatory_market_tape_snapshot_v1", "PHASE1_COMPLETED_1M_CANDLE", "display_only", "broker_authority: false"]) {
  if (!adapter.includes(token)) errors.push(`build_market_tape_snapshot.mjs missing required contract token: ${token}`);
}
if (adapter.includes("broker_authority: true")) {
  errors.push("build_market_tape_snapshot.mjs grants broker authority");
}

const pipelineAdapter = fs.readFileSync(path.join(prototypeRoot, "build_system_pipeline_snapshot.mjs"), "utf8");
for (const token of ["submitOrder", "cancelOrder", "strategy input", "computeSafeState", "computeReadiness"]) {
  if (pipelineAdapter.includes(token)) errors.push(`build_system_pipeline_snapshot.mjs contains forbidden authority token: ${token}`);
}
for (const token of ["observatory_system_pipeline_snapshot_v1", "PREPARED_PRODUCER_STATUS_ARTIFACTS", "display_only", "readiness_computation: false", "display_class", "producer_status"]) {
  if (!pipelineAdapter.includes(token)) errors.push(`build_system_pipeline_snapshot.mjs missing required contract token: ${token}`);
}
if (pipelineAdapter.includes("broker_authority: true") || pipelineAdapter.includes("runtime_authority: true")) {
  errors.push("build_system_pipeline_snapshot.mjs grants authority");
}

const exposureAdapter = fs.readFileSync(path.join(prototypeRoot, "build_exposure_snapshot.mjs"), "utf8");
for (const token of ["submitOrder", "cancelOrder", "strategy input", "computeSafeState", "computeReadiness"]) {
  if (exposureAdapter.includes(token)) errors.push(`build_exposure_snapshot.mjs contains forbidden authority token: ${token}`);
}
for (const token of ["observatory_exposure_snapshot_v1", "PREPARED_MANAGED_EXPOSURE_ARTIFACTS", "display_only", "trading_input: false", "broker_authority: false"]) {
  if (!exposureAdapter.includes(token)) errors.push(`build_exposure_snapshot.mjs missing required contract token: ${token}`);
}
if (exposureAdapter.includes("broker_authority: true") || exposureAdapter.includes("runtime_authority: true")) {
  errors.push("build_exposure_snapshot.mjs grants authority");
}

const appSource = fs.readFileSync(path.join(prototypeRoot, "app.mjs"), "utf8");
for (const token of ["observatory_market_canvas_v1", "marketCanvasSnapshot", "marketCanvas", "preparedMarketCanvas"]) {
  if (!appSource.includes(token)) errors.push(`app.mjs missing prepared Market Canvas token: ${token}`);
}
for (const token of ["macroMarketContextSnapshot", "operationalHealthSnapshot", "runtimeBrokerContextSnapshot", "flow-led"]) {
  if (!appSource.includes(token)) errors.push(`app.mjs missing prepared context token: ${token}`);
}
for (const token of ["worldLand110m", "ROBINSON_X", "projectLonLat", "atlas-coastline"]) {
  if (!appSource.includes(token)) errors.push(`app.mjs missing real world geometry/projection token: ${token}`);
}
for (const token of [
  "horizontalCompositionOffset",
  "observatoryProjectionV1Coordinate",
  "Observatory Projection v1",
  "Pacific-wrap composition",
  "smoothBand",
  "regions move mostly as rigid plates",
  "Americas plate, moved farther west",
  "Europe/Africa/Madagascar plate",
  "Indian Ocean",
  "Tasman / SW Pacific",
  "function lerp",
]) {
  if (!appSource.includes(token)) errors.push(`app.mjs missing horizontal-composition token: ${token}`);
}
for (const token of ["CITY_LABEL_OFFSETS", "cityGroups", "strongestVenueStatus", "explanationForStage", "data-direction=\"${direction}\""]) {
  if (!appSource.includes(token)) errors.push(`app.mjs missing usability/correctness token: ${token}`);
}
for (const token of [
  "renderContext",
  "renderMarketCenterContext",
  "renderVenueContext",
  "renderSystemStageContext",
  "renderMarketDomainContext",
  "renderTickerContext",
  "MARKET_CENTER_IDS",
  "MARKET_DOMAIN_DEFINITIONS",
  "data-context-type=\"market-center\"",
  "data-context-type=\"ticker\"",
  "click-to-trade",
]) {
  if (!appSource.includes(token)) errors.push(`app.mjs missing context-panel token: ${token}`);
}
for (const token of [
  "effectiveVenueStatus",
  "venueEvidenceFreshness",
  "Venue status stale",
  "sourceFooter",
  "technicalDetail",
  "displayFreshness",
  "Operator view",
  "Technical detail",
  "Future action layers",
]) {
  if (!appSource.includes(token)) errors.push(`app.mjs missing hardening/coherence token: ${token}`);
}
for (const token of [
  "observatoryFrameManifest",
  "loadPreparedFrameManifestIfRequested",
  "refreshPreparedSnapshots",
  "preparedRefreshTimer",
  "overall_frame_freshness",
  "renderSignatures",
  "measureRender",
  "observatoryPerformance",
  "stableSignature",
]) {
  if (!appSource.includes(token)) errors.push(`app.mjs missing refresh/frame token: ${token}`);
}
for (const token of ["contextPanel.dataset.selection", "product_session_states", "Product sessions", "session_aggregation"]) {
  if (!appSource.includes(token)) errors.push(`app.mjs missing interaction/session token: ${token}`);
}
if (appSource.includes("venue.labelDx") || appSource.includes("venue.labelDy")) {
  errors.push("app.mjs still uses obsolete fixture label offsets");
}
if (!appSource.includes('numericChange > 0 ? "up" : numericChange < 0 ? "down" : "flat"')) {
  errors.push("app.mjs does not classify market tape flat/up/down direction explicitly");
}
for (const token of ["solarPosition", "solarElevation", "solarIllumination", "subsolarLongitude", "solar-daylight", "solar-twilight", "solar-deep-twilight", "solar-night"]) {
  if (!appSource.includes(token)) errors.push(`app.mjs missing solar illumination token: ${token}`);
}
if (appSource.includes("timeTerminatorPath")) {
  errors.push("app.mjs still uses scenario-driven terminator path");
}
if (appSource.includes("readFileSync(\"outputs") || appSource.includes("fetch(\"/api")) {
  errors.push("app.mjs reads live artifacts directly instead of prepared snapshots");
}

if (worldLand110m.type !== "FeatureCollection" || !Array.isArray(worldLand110m.features) || worldLand110m.features.length < 100) {
  errors.push("world_land_110m.mjs does not expose expected Natural Earth land geometry");
}

for (const venue of venues) {
  if (!Number.isFinite(venue.latitude) || venue.latitude < -90 || venue.latitude > 90) {
    errors.push(`invalid venue latitude: ${venue.id}`);
  }
  if (!Number.isFinite(venue.longitude) || venue.longitude < -180 || venue.longitude > 180) {
    errors.push(`invalid venue longitude: ${venue.id}`);
  }
}
const cityCounts = venues.reduce((counts, venue) => {
  counts.set(venue.city, (counts.get(venue.city) || 0) + 1);
  return counts;
}, new Map());
for (const city of ["New York", "Chicago", "London", "Paris", "Frankfurt", "Tokyo", "Hong Kong", "Singapore", "Sydney", "Sao Paulo"]) {
  const cityVenues = venues.filter((venue) => venue.city === city);
  if (!cityVenues.length) {
    errors.push(`missing venue projection sanity city: ${city}`);
    continue;
  }
  const reference = cityVenues[0];
  if (!Number.isFinite(reference.latitude) || !Number.isFinite(reference.longitude)) {
    errors.push(`invalid projection anchor for city: ${city}`);
  }
}
if ((cityCounts.get("New York") || 0) < 2 || (cityCounts.get("Chicago") || 0) < 2) {
  errors.push("expected multi-venue city fixtures for duplicate-label validation");
}

for (const fileName of [
  "observatory_refresh_service.mjs",
  "build_observatory_frame_manifest.mjs",
  "build_macro_market_context_snapshot.mjs",
  "build_operational_health_snapshot.mjs",
  "build_runtime_broker_context_snapshot.mjs",
]) {
  const source = fs.readFileSync(path.join(prototypeRoot, fileName), "utf8");
  for (const token of ["submitOrder", "cancelOrder", "computeSafeState", "computeReadiness", "placeOrder", "paper_proof"]) {
    if (source.includes(token)) errors.push(`${fileName} contains forbidden authority token: ${token}`);
  }
}

const html = fs.readFileSync(path.join(prototypeRoot, "index.html"), "utf8");
for (const fileName of ["./styles.css", "./app.mjs"]) {
  if (!html.includes(fileName)) errors.push(`index.html does not reference ${fileName}`);
}
for (const token of ["context-panel", "context-content", "context-close"]) {
  if (!html.includes(token)) errors.push(`index.html missing context-panel element: ${token}`);
}

const css = fs.readFileSync(path.join(prototypeRoot, "styles.css"), "utf8");
for (const bannedPattern of [".card", ".kpi", " table", "status-tile"]) {
  if (css.includes(bannedPattern)) errors.push(`styles.css includes dashboard-like pattern: ${bannedPattern}`);
}
for (const token of [".solar-daylight", ".solar-twilight", ".solar-deep-twilight", ".solar-night"]) {
  if (!css.includes(token)) errors.push(`styles.css missing solar layer style: ${token}`);
}
for (const token of [
  ".context-panel",
  ".context-section",
  ".context-row",
  ".context-footer",
  ".context-technical",
  ".tape-change[data-direction=\"up\"]",
  ".tape-change[data-direction=\"down\"]",
  ".tape-change[data-direction=\"flat\"]",
]) {
  if (!css.includes(token)) errors.push(`styles.css missing usability/correctness style: ${token}`);
}
if (css.includes(".terminator")) {
  errors.push("styles.css still includes the old scenario terminator style");
}

if (scenarioNames().length < 27) {
  errors.push(`expected at least 27 prototype scenarios, got ${scenarioNames().length}`);
}

if (errors.length) {
  console.error(JSON.stringify({ valid: false, errors }, null, 2));
  process.exit(1);
}

console.log(JSON.stringify({
  valid: true,
  scenarios: scenarioNames(),
  required_files: requiredFiles,
}, null, 2));
