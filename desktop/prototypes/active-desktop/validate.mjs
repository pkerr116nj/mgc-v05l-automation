import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { scenarioNames, validateFixtures, venues } from "./fixtures.mjs";
import {
  FEATURED_NORTH_AMERICA_MARKET_CENTERS,
  MAX_LABEL_DISTANCE,
  labelLayoutDiagnostics,
  layoutCityLabels,
} from "./observatory_map_layout.mjs";
import { worldLand110m } from "./world_land_110m.mjs";

const prototypeRoot = path.dirname(fileURLToPath(import.meta.url));
const requiredFiles = [
  "index.html",
  "styles.css",
  "fixtures.mjs",
  "world_land_110m.mjs",
  "app.mjs",
  "launch_observatory.sh",
  "observatory_map_layout.mjs",
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
const indexSource = fs.readFileSync(path.join(prototypeRoot, "index.html"), "utf8");
for (const token of [
  "window.onerror",
  "window.onunhandledrejection",
  'record("BOOT")',
  "MODULE_GRAPH_LOAD",
  "ES modules cannot initialize from file://",
  "observatory-init-diagnostics",
  'get("debug") !== "init"',
]) {
  if (!indexSource.includes(token)) errors.push(`index.html missing initialization diagnostic token: ${token}`);
}
for (const token of [
  "diagnoseInitializationAwait",
  'markInitializationStage("MODULE_IMPORTS_READY")',
  'markInitializationStage("PREPARED_SNAPSHOTS_READY")',
  'markInitializationStage("WORLD_BASE_READY")',
  'markInitializationStage("SOLAR_READY")',
  'markInitializationStage("VENUES_READY")',
  'markInitializationStage("LABELS_READY")',
  'markInitializationStage("LOWER_SYSTEM_READY")',
  'markInitializationStage("TAPE_READY")',
  'markInitializationStage("INTERACTIONS_READY")',
  'markInitializationStage("OBSERVATORY_READY"',
]) {
  if (!appSource.includes(token)) errors.push(`app.mjs missing initialization stage token: ${token}`);
}
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
for (const token of ["layoutCityLabels", "cityGroups", "strongestVenueStatus", "explanationForStage", "data-direction=\"${direction}\""]) {
  if (!appSource.includes(token)) errors.push(`app.mjs missing usability/correctness token: ${token}`);
}
if (!appSource.includes("strongest === null")) {
  errors.push("market-center status reduction can allow UNKNOWN to mask an all-CLOSED city");
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
if (appSource.includes("CITY_LABEL_OFFSETS")) {
  errors.push("app.mjs still uses the pre-projection fixed city-label offset table");
}
if (!appSource.includes('numericChange > 0 ? "up" : numericChange < 0 ? "down" : "flat"')) {
  errors.push("app.mjs does not classify market tape flat/up/down direction explicitly");
}
for (const token of ["solarPosition", "solarElevation", "solarIllumination", "subsolarLongitude", "solar-daylight", "solar-twilight", "solar-deep-twilight", "solar-night"]) {
  if (!appSource.includes(token)) errors.push(`app.mjs missing solar illumination token: ${token}`);
}
for (const token of [
  "drawWorldBase",
  "drawSolarLayer",
  "drawMarketCenterLayer",
  "drawMarketCenterLabels",
  'world: "observatory-world-atlas-v13-css-peripheral-limbs"',
  'solar: solarMinute',
  "marketCenters: stableSignature({ venueStates, venueGeometry })",
  'layout: "bounded-collision-v1"',
]) {
  if (!appSource.includes(token)) errors.push(`app.mjs missing layer-invalidation token: ${token}`);
}
for (const token of ["atlantic-overlap-wash", "atlanticWarmth", "haloRadius", 'data-featured="${featured}"']) {
  if (!appSource.includes(token)) errors.push(`app.mjs missing venue/Atlantic visual token: ${token}`);
}
for (const token of [
  "AMBIENT_MARKET_CENTER_OVERRIDES",
  'cboe: "Chicago"',
  "MARKET_CENTER_VENUE_ORDER",
  '"New York": ["nyse", "nasdaq"]',
  'Chicago: ["cme", "cboe", "cfe"]',
  'class="market-center-group selectable"',
  'class="market-center-hit-target"',
  'data-context-type="market-center"',
]) {
  if (!appSource.includes(token)) errors.push(`app.mjs missing one-dot market-center token: ${token}`);
}
if (appSource.includes('class="venue-group selectable"')) {
  errors.push("ambient map still exposes overlapping venue-level hit targets");
}
for (const token of [
  "SPACE_STARS",
  'class="space-layer" data-background-model="open-black-space"',
  'class="space-stars"',
]) {
  if (!appSource.includes(token)) errors.push(`app.mjs missing planetary edge-backlight token: ${token}`);
}
for (const forbiddenToken of ["planetaryBoundaryPath", "earthMapClip", "spaceOutsideEarthMask", 'class="earth-ocean"', 'class="earth-wash"']) {
  if (appSource.includes(forbiddenToken)) errors.push(`open map still contains full-aperture token: ${forbiddenToken}`);
}
if (appSource.includes("<animate") || appSource.includes("<animateTransform")) {
  errors.push("static space treatment contains decorative SVG animation");
}
const planetaryLayerTokens = [
  'class="space-layer"',
  'id="solar-daylight-layer"',
  'class="atlas-coastline"',
  'id="market-center-layer"',
  'id="market-center-labels-layer"',
];
const planetaryLayerIndexes = planetaryLayerTokens.map((token) => appSource.indexOf(token));
if (planetaryLayerIndexes.some((index) => index < 0)
  || planetaryLayerIndexes.some((index, position) => position > 0 && index <= planetaryLayerIndexes[position - 1])) {
  errors.push("planetary layers do not preserve space-to-label visual order");
}
if (!appSource.includes("OPEN: 0.5") || !appSource.includes("CLOSED: 0.32")) {
  errors.push("venue core radii do not preserve the refined open/closed hierarchy");
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

const expectedNorthAmericaCenters = ["Chicago", "Mexico City", "New York", "Toronto"];
const actualNorthAmericaCenters = [...FEATURED_NORTH_AMERICA_MARKET_CENTERS].sort();
if (JSON.stringify(actualNorthAmericaCenters) !== JSON.stringify(expectedNorthAmericaCenters)) {
  errors.push(`unexpected featured North America market-center set: ${actualNorthAmericaCenters.join(", ")}`);
}
if (actualNorthAmericaCenters.includes("Montreal")) {
  errors.push("Montreal remains in the primary ambient North America market-center set");
}

const uniqueCityAnchors = [...new Map(venues.map((venue) => [venue.city, {
  city: venue.city,
  region: venue.region,
  x: (venue.longitude + 180) / 3.6,
  y: (90 - venue.latitude) / 1.8,
}])).values()];
const labelLayout = layoutCityLabels(uniqueCityAnchors);
const labelDiagnostics = labelLayoutDiagnostics(labelLayout);
if (labelDiagnostics.duplicateCities.length) {
  errors.push(`duplicate city labels: ${labelDiagnostics.duplicateCities.join(", ")}`);
}
if (labelDiagnostics.overDistance.length || labelDiagnostics.maxDistance > MAX_LABEL_DISTANCE) {
  errors.push(`city label exceeds maximum anchor distance: ${JSON.stringify(labelDiagnostics)}`);
}
if (labelLayout.some((label) => label.city === "Montreal")) {
  errors.push("Montreal receives a primary ambient city label");
}
for (const city of ["New York", "Chicago", "Toronto", "Mexico City", "Seoul", "Tokyo", "Osaka", "Shanghai", "Shenzhen", "Taipei", "Hong Kong", "Singapore"]) {
  const label = labelLayout.find((candidate) => candidate.city === city);
  const anchor = uniqueCityAnchors.find((candidate) => candidate.city === city);
  if (!label || !anchor) {
    errors.push(`missing anchor-first label layout check for ${city}`);
  } else if (label.anchorX !== anchor.x || label.anchorY !== anchor.y) {
    errors.push(`label anchor diverges from transformed city coordinate for ${city}`);
  }
}

const venueByCity = Object.fromEntries(venues.map((venue) => [venue.city, venue]));
if (!(venueByCity.Seoul.longitude < venueByCity.Tokyo.longitude && venueByCity.Seoul.latitude > venueByCity.Taipei.latitude)) {
  errors.push("Seoul geographic anchor sanity failed");
}
if (!(venueByCity.Shanghai.longitude < venueByCity.Seoul.longitude && venueByCity.Shanghai.latitude > venueByCity["Hong Kong"].latitude)) {
  errors.push("Shanghai/Hong Kong geographic anchor sanity failed");
}
if (!(venueByCity.Taipei.longitude < venueByCity.Tokyo.longitude && venueByCity.Taipei.latitude < venueByCity.Osaka.latitude)) {
  errors.push("Taipei/Tokyo/Osaka geographic anchor sanity failed");
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
if (!html.includes('id="venue-arc" viewBox="0 0 100 100" overflow="visible"')) {
  errors.push("venue-arc SVG does not expose curved geometry beyond its rectangular viewBox");
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
for (const token of [
  ".atlantic-overlap-wash",
  ".venue-label-leader",
  '.market-center-group[data-featured="false"]',
  ".market-center-hit-target",
  "--bg: #010203",
  ".active-desktop[data-time-state] { background: #010203; }",
  ".space-black",
  ".space-stars",
  ".embedded-arc::before",
  ".embedded-arc::after",
  '.venue-harbor[data-state="CLOSED"]',
  'filter: drop-shadow(0 0 3px rgba(112, 214, 145, 0.56))',
]) {
  if (!css.includes(token)) errors.push(`styles.css missing refined venue/label style: ${token}`);
}
const solarDaylightPathBlock = css.match(/\.solar-daylight path\s*\{([^}]*)\}/)?.[1] || "";
const solarTwilightPathBlock = css.match(/\.solar-twilight path\s*\{([^}]*)\}/)?.[1] || "";
const venueArcBlock = css.match(/#venue-arc\s*\{([^}]*)\}/)?.[1] || "";
const venueHarborBlocks = [...css.matchAll(/\.venue-harbor[^\{]*\{([^}]*)\}/g)].map((match) => match[1]);
if (!venueArcBlock.includes("overflow: visible")) {
  errors.push("venue-arc root clips curved planetary geometry to a rectangular SVG viewport");
}
if (venueHarborBlocks.some((block) => block.includes("filter:"))) {
  errors.push("market-center halos use SVG CSS filters that scale inconsistently across Chrome and Safari");
}
if (solarDaylightPathBlock.includes("filter:") || solarTwilightPathBlock.includes("filter:")) {
  errors.push("solar softness is applied per cell instead of once per time-driven band");
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
