import { scenarioNames, scenarios, venues } from "./fixtures.mjs";
import { worldLand110m } from "./world_land_110m.mjs";
import {
  isPrimaryMarketCenter,
  labelLayoutDiagnostics,
  layoutCityLabels,
} from "./observatory_map_layout.mjs";

const markInitializationStage = (stage, detail = null) => {
  window.__recordObservatoryStage?.(stage, detail);
};

async function diagnoseInitializationAwait(label, promise, warningAfterMs = 2500) {
  const startedAt = performance.now();
  const warningTimer = window.setTimeout(() => {
    window.__observatoryInit?.pending(label, performance.now() - startedAt);
  }, warningAfterMs);
  try {
    return await promise;
  } finally {
    window.clearTimeout(warningTimer);
  }
}

markInitializationStage("MODULE_IMPORTS_READY");

const app = document.querySelector("#app");
const arc = document.querySelector("#venue-arc");
const tape = document.querySelector("#market-tape");
const flow = document.querySelector("#system-flow");
const scenarioPanel = document.querySelector("#scenario-panel");
const scenarioToggle = document.querySelector("#scenario-toggle");
const scenarioClock = document.querySelector("#scenario-clock");
const canvasNote = document.querySelector("#canvas-note");
const contextPanel = document.querySelector("#context-panel");
const contextContent = document.querySelector("#context-content");
const contextClose = document.querySelector("#context-close");
const atmosphere = document.querySelector("#atmosphere");
const riverMaterial = document.querySelector("#river-material");
const atmosphereContext = atmosphere.getContext("2d", { alpha: true });
const riverContext = riverMaterial.getContext("2d", { alpha: true });

let currentScenarioName = new URLSearchParams(window.location.search).get("scenario") || "ATLANTIC_BRIDGE_MIDDAY";
let preparedMarketTape = null;
let preparedSystemFlow = null;
let preparedExposure = null;
let preparedVenueSessions = null;
let preparedMarketCanvas = null;
let preparedMacroContext = null;
let preparedOperationalHealth = null;
let preparedRuntimeBrokerContext = null;
let preparedFrameManifest = null;
let eventTimer = null;
let solarTimer = null;
let preparedRefreshTimer = null;
let selectedContext = null;
let initialContextApplied = false;
let initializationInProgress = true;
let currentSolarMinuteKey = null;
const renderSignatures = {
  commentary: null,
  world: null,
  solar: null,
  marketCenters: null,
  labels: null,
  tape: null,
  flow: null,
  atmosphere: null,
  river: null,
  controls: null,
};
const observatoryPerformance = {
  counts: {
    world: 0,
    solar: 0,
    marketCenters: 0,
    labels: 0,
    tape: 0,
    flow: 0,
    atmosphere: 0,
    river: 0,
    context: 0,
  },
  durations: [],
};
window.observatoryPerformance = observatoryPerformance;

const ROBINSON_X = [1, 0.9986, 0.9954, 0.99, 0.9822, 0.973, 0.96, 0.9427, 0.9216, 0.8962, 0.8679, 0.835, 0.7986, 0.7597, 0.7186, 0.6732, 0.6213, 0.5722, 0.5322];
const ROBINSON_Y = [0, 0.062, 0.124, 0.186, 0.248, 0.31, 0.372, 0.434, 0.4958, 0.5571, 0.6176, 0.6769, 0.7346, 0.7903, 0.8435, 0.8936, 0.9394, 0.9761, 1];
const ROBINSON_X_SCALE = 0.8487;
const ROBINSON_Y_SCALE = 1.3523;
const MAP_WIDTH_UNITS = Math.PI * ROBINSON_X_SCALE * 2;
const MAP_HEIGHT_UNITS = ROBINSON_Y_SCALE * 2;
const SPACE_STARS = Object.freeze([
  [1.1, 4.8, 0.055, 0.34], [5.8, 7.9, 0.038, 0.22], [12.7, 2.4, 0.072, 0.42],
  [20.4, 6.1, 0.043, 0.24], [28.9, 1.7, 0.052, 0.3], [39.6, 4.2, 0.035, 0.2],
  [53.8, 1.2, 0.065, 0.36], [65.3, 5.3, 0.041, 0.24], [74.9, 2.8, 0.052, 0.29],
  [84.6, 6.7, 0.036, 0.2], [93.1, 3.5, 0.069, 0.38], [98.7, 9.2, 0.044, 0.25],
  [0.8, 18.6, 0.04, 0.24], [7.4, 15.2, 0.061, 0.34], [91.8, 17.9, 0.047, 0.27],
  [99.1, 23.8, 0.071, 0.4], [1.7, 31.4, 0.049, 0.29], [97.8, 36.1, 0.036, 0.21],
  [0.5, 46.7, 0.074, 0.39], [99.3, 51.8, 0.048, 0.25], [1.4, 61.9, 0.038, 0.23],
  [98.1, 66.3, 0.066, 0.36], [0.9, 76.8, 0.051, 0.3], [96.6, 79.1, 0.04, 0.22],
  [4.2, 87.4, 0.069, 0.37], [10.9, 92.6, 0.037, 0.22], [18.8, 96.4, 0.054, 0.31],
  [30.5, 91.8, 0.041, 0.24], [42.1, 98.2, 0.063, 0.35], [57.6, 94.5, 0.036, 0.21],
  [68.7, 98.7, 0.049, 0.28], [78.4, 92.1, 0.07, 0.39], [88.9, 96.8, 0.042, 0.24],
  [95.7, 89.3, 0.057, 0.32], [2.9, 11.7, 0.03, 0.17], [16.5, 4.1, 0.032, 0.18],
  [47.2, 2.6, 0.039, 0.23], [70.8, 1.1, 0.031, 0.18], [96.1, 13.4, 0.035, 0.2],
  [2.2, 71.2, 0.032, 0.18], [7.6, 95.8, 0.035, 0.2], [24.3, 98.9, 0.03, 0.17],
  [62.4, 97.1, 0.034, 0.2], [90.7, 93.6, 0.031, 0.18], [98.9, 72.7, 0.034, 0.19],
]);

function interpolateTable(table, latitude) {
  const absLatitude = Math.min(90, Math.abs(latitude));
  const index = Math.min(17, Math.floor(absLatitude / 5));
  const fraction = (absLatitude - index * 5) / 5;
  return table[index] + (table[index + 1] - table[index]) * fraction;
}

function projectLonLat(longitude, latitude, options = {}) {
  if (!Number.isFinite(longitude) || !Number.isFinite(latitude)) return null;
  const sourceCoordinate = options.geometryContext
    ? observatoryGeometryCoordinate(longitude, latitude, options.geometryContext)
    : { longitude, latitude };
  const projectedCoordinate = observatoryProjectionV1Coordinate(sourceCoordinate.longitude, sourceCoordinate.latitude);
  const lonRadians = projectedCoordinate.longitude * Math.PI / 180;
  const xFactor = interpolateTable(ROBINSON_X, projectedCoordinate.latitude);
  const yFactor = interpolateTable(ROBINSON_Y, projectedCoordinate.latitude);
  const rawX = ROBINSON_X_SCALE * xFactor * lonRadians;
  const rawY = ROBINSON_Y_SCALE * yFactor * Math.sign(projectedCoordinate.latitude);
  const composedX = rawX + horizontalCompositionOffset(projectedCoordinate.longitude);
  return {
    x: 50 + (composedX / MAP_WIDTH_UNITS) * 108,
    y: 51 - (rawY / MAP_HEIGHT_UNITS) * 77,
  };
}

function smoothBand(value, left, right) {
  const t = Math.max(0, Math.min(1, (value - left) / (right - left)));
  return t * t * (3 - 2 * t);
}

function observatoryProjectionV1Coordinate(longitude, latitude) {
  // Observatory Projection v1 favors immediate operator recognition over strict
  // cartographic fidelity: plates stay coherent, oceans absorb most spacing.
  if (longitude >= 166 && latitude >= -50 && latitude <= -30) {
    const centerLongitude = 172.4;
    const centerLatitude = -41.2;
    const dx = longitude - centerLongitude;
    const dy = latitude - centerLatitude;
    return {
      longitude: centerLongitude + 3.2 + dx * 0.74,
      latitude: centerLatitude + 1.8 + dy * 0.96,
    };
  }
  if (longitude < -38) {
    const center = -92;
    const southAmericaUpperLobeGain = longitude > -74 && longitude < -42 && latitude > -24 && latitude < 13
      ? 1.055
      : 1;
    return {
      longitude: center + (longitude - center) * 1.095 * southAmericaUpperLobeGain,
      latitude,
    };
  }
  if (longitude >= 46 && longitude < 104) {
    const center = 58;
    return {
      longitude: center + (longitude - center) * 1.18,
      latitude,
    };
  }
  if (longitude >= 104 && longitude < 146) {
    const center = 104;
    return {
      longitude: center + (longitude - center) * 1.19,
      latitude,
    };
  }
  return { longitude, latitude };
}

function horizontalCompositionOffset(longitude) {
  // Pacific-wrap composition: regions move mostly as rigid plates. Transitions
  // remain in the Pacific, Atlantic, Indian, and SW Pacific ocean spans.
  if (longitude < -172) return -0.27;
  if (longitude < -158) return lerp(-0.27, -0.22, smoothBand(longitude, -172, -158));
  if (longitude < -42) return -0.22; // Americas plate, moved farther west.
  if (longitude < -14) return lerp(-0.22, 0.18, smoothBand(longitude, -42, -14)); // wider Atlantic.
  if (longitude < 46) return 0.18; // Europe/Africa/Madagascar plate.
  if (longitude < 78) return lerp(0.18, 0.31, smoothBand(longitude, 46, 78)); // Indian Ocean / Central Asia spacing.
  if (longitude < 104) return 0.31; // West/Central Asia after width restoration.
  if (longitude < 146) return 0.34; // East Asia / western Pacific opened for venue separation.
  if (longitude < 160) return lerp(0.34, 0.33, smoothBand(longitude, 146, 160));
  if (longitude < 166) return 0.33; // Australia plate.
  if (longitude < 174) return lerp(0.33, 0.48, smoothBand(longitude, 166, 174)); // Tasman / SW Pacific.
  return 0.48; // New Zealand / Pacific edge.
}

function lerp(left, right, amount) {
  return left + (right - left) * amount;
}

function observatoryGeometryCoordinate(longitude, latitude, context) {
  if (!context || !Number.isFinite(context.centerLongitude) || !Number.isFinite(context.centerLatitude)) {
    return { longitude, latitude };
  }
  if (context.smallEquatorialIsland) {
    const scale = context.region === "maritime_southeast_asia" ? 0.78 : 0.82;
    return {
      longitude: context.centerLongitude + (longitude - context.centerLongitude) * scale,
      latitude: context.centerLatitude + (latitude - context.centerLatitude) * scale,
    };
  }
  return { longitude, latitude };
}

function currentMinuteKey() {
  return new Date().toISOString().slice(0, 16);
}

function stableSignature(value) {
  return JSON.stringify(value);
}

function measureRender(name, fn) {
  const started = performance.now();
  fn();
  const durationMs = Number((performance.now() - started).toFixed(3));
  observatoryPerformance.counts[name] = (observatoryPerformance.counts[name] || 0) + 1;
  app.dataset[`render${name[0].toUpperCase()}${name.slice(1)}Count`] = String(observatoryPerformance.counts[name]);
  observatoryPerformance.durations.push({ name, duration_ms: durationMs, at: new Date().toISOString() });
  if (observatoryPerformance.durations.length > 80) observatoryPerformance.durations.shift();
}

function normalizeDegrees(value) {
  return ((value % 360) + 360) % 360;
}

function normalizeLongitude(value) {
  const normalized = normalizeDegrees(value + 180) - 180;
  return normalized === -180 ? 180 : normalized;
}

function julianDay(date) {
  return date.getTime() / 86400000 + 2440587.5;
}

function solarPosition(date) {
  const radians = Math.PI / 180;
  const degrees = 180 / Math.PI;
  const jd = julianDay(date);
  const daysSinceEpoch = jd - 2451545.0;
  const meanLongitude = normalizeDegrees(280.46 + 0.9856474 * daysSinceEpoch);
  const meanAnomaly = normalizeDegrees(357.528 + 0.9856003 * daysSinceEpoch);
  const eclipticLongitude = normalizeDegrees(
    meanLongitude
      + 1.915 * Math.sin(meanAnomaly * radians)
      + 0.02 * Math.sin(2 * meanAnomaly * radians)
  );
  const obliquity = 23.439 - 0.0000004 * daysSinceEpoch;
  const rightAscension = Math.atan2(
    Math.cos(obliquity * radians) * Math.sin(eclipticLongitude * radians),
    Math.cos(eclipticLongitude * radians)
  ) * degrees;
  const declination = Math.asin(
    Math.sin(obliquity * radians) * Math.sin(eclipticLongitude * radians)
  ) * degrees;
  const siderealTime = normalizeDegrees(280.46061837 + 360.98564736629 * daysSinceEpoch);
  return {
    declination,
    subsolarLongitude: normalizeLongitude(rightAscension - siderealTime),
  };
}

function solarElevation(longitude, latitude, sun) {
  const radians = Math.PI / 180;
  const lat = latitude * radians;
  const declination = sun.declination * radians;
  const hourAngle = normalizeLongitude(longitude - sun.subsolarLongitude) * radians;
  return Math.asin(
    Math.sin(lat) * Math.sin(declination)
      + Math.cos(lat) * Math.cos(declination) * Math.cos(hourAngle)
  ) * (180 / Math.PI);
}

function solarCellPath(lon0, lat0, lon1, lat1) {
  const corners = [
    projectLonLat(lon0, lat0),
    projectLonLat(lon1, lat0),
    projectLonLat(lon1, lat1),
    projectLonLat(lon0, lat1),
  ];
  if (corners.some((point) => !point)) return "";
  return `M ${corners[0].x.toFixed(3)} ${corners[0].y.toFixed(3)} L ${corners[1].x.toFixed(3)} ${corners[1].y.toFixed(3)} L ${corners[2].x.toFixed(3)} ${corners[2].y.toFixed(3)} L ${corners[3].x.toFixed(3)} ${corners[3].y.toFixed(3)} Z`;
}

function solarIllumination(date = new Date()) {
  const sun = solarPosition(date);
  const night = [];
  const twilight = [];
  const deepTwilight = [];
  const daylight = [];
  const lonStep = 3;
  const latStep = 3;
  for (let latitude = -60; latitude < 84; latitude += latStep) {
    for (let longitude = -180; longitude < 180; longitude += lonStep) {
      const centerLongitude = longitude + lonStep / 2;
      const centerLatitude = latitude + latStep / 2;
      const elevation = solarElevation(centerLongitude, centerLatitude, sun);
      const path = solarCellPath(longitude, latitude, longitude + lonStep, latitude + latStep);
      if (!path) continue;
      if (elevation > 6) {
        const strength = 0.56 + 0.44 * smoothBand(elevation, 6, 24);
        daylight.push(`<path d="${path}" opacity="${strength.toFixed(3)}" />`);
      } else if (elevation > -7) {
        const strength = 0.48 + 0.52 * (1 - Math.abs(elevation + 0.5) / 7.5);
        twilight.push(`<path d="${path}" opacity="${Math.max(0.34, strength).toFixed(3)}" />`);
      } else if (elevation > -18) {
        const strength = 0.5 + 0.5 * smoothBand(-elevation, 7, 18);
        deepTwilight.push(`<path d="${path}" opacity="${strength.toFixed(3)}" />`);
      } else {
        const strength = 0.58 + 0.42 * smoothBand(-elevation, 18, 32);
        night.push(`<path d="${path}" opacity="${strength.toFixed(3)}" />`);
      }
    }
  }
  return {
    daylight: daylight.join(""),
    twilight: twilight.join(""),
    deepTwilight: deepTwilight.join(""),
    night: night.join(""),
    sun,
  };
}

function geometryMaxLatitude(geometry) {
  if (!geometry) return -90;
  const rings = geometry.type === "Polygon"
    ? geometry.coordinates
    : geometry.type === "MultiPolygon"
      ? geometry.coordinates.flatMap((polygon) => polygon)
      : [];
  return rings.reduce((maxLatitude, ring) => Math.max(
    maxLatitude,
    ...ring.map((coordinate) => coordinate[1]).filter(Number.isFinite)
  ), -90);
}

function ringBounds(ring) {
  return ring.reduce((bounds, coordinate) => {
    const [longitude, latitude] = coordinate;
    if (!Number.isFinite(longitude) || !Number.isFinite(latitude)) return bounds;
    return {
      minLongitude: Math.min(bounds.minLongitude, longitude),
      maxLongitude: Math.max(bounds.maxLongitude, longitude),
      minLatitude: Math.min(bounds.minLatitude, latitude),
      maxLatitude: Math.max(bounds.maxLatitude, latitude),
    };
  }, {
    minLongitude: Infinity,
    maxLongitude: -Infinity,
    minLatitude: Infinity,
    maxLatitude: -Infinity,
  });
}

function geometryContextForRing(ring) {
  const bounds = ringBounds(ring);
  if (!Number.isFinite(bounds.minLongitude) || !Number.isFinite(bounds.minLatitude)) return null;
  const width = bounds.maxLongitude - bounds.minLongitude;
  const height = bounds.maxLatitude - bounds.minLatitude;
  const centerLongitude = (bounds.minLongitude + bounds.maxLongitude) / 2;
  const centerLatitude = (bounds.minLatitude + bounds.maxLatitude) / 2;
  const inCaribbean = centerLongitude > -86 && centerLongitude < -60 && centerLatitude > 10 && centerLatitude < 24;
  const inMaritimeSoutheastAsia = centerLongitude > 94 && centerLongitude < 128 && centerLatitude > -10 && centerLatitude < 10;
  const modestIslandScale = width < 24 && height < 12;
  if ((inCaribbean || inMaritimeSoutheastAsia) && modestIslandScale) {
    return {
      centerLongitude,
      centerLatitude,
      smallEquatorialIsland: true,
      region: inMaritimeSoutheastAsia ? "maritime_southeast_asia" : "caribbean",
    };
  }
  return { centerLongitude, centerLatitude };
}

function coordinatePath(ring) {
  let output = "";
  let previous = null;
  const geometryContext = geometryContextForRing(ring);
  for (const coordinate of ring) {
    const [longitude, latitude] = coordinate;
    const point = projectLonLat(longitude, latitude, { geometryContext });
    if (!point) continue;
    const command = previous && Math.abs(longitude - previous.longitude) <= 180 ? "L" : "M";
    output += `${command} ${point.x.toFixed(3)} ${point.y.toFixed(3)} `;
    previous = { longitude, latitude };
  }
  return output.trim();
}

function geometryPath(geometry) {
  if (!geometry) return "";
  let rings = [];
  if (geometry.type === "Polygon") {
    rings = geometry.coordinates.map(coordinatePath).filter(Boolean);
  } else if (geometry.type === "MultiPolygon") {
    rings = geometry.coordinates.flatMap((polygon) => polygon.map(coordinatePath)).filter(Boolean);
  } else {
    return "";
  }
  return rings.length ? `${rings.join(" Z ")} Z` : "";
}

const LAND_PATHS = Object.freeze(
  worldLand110m.features
    .filter((feature) => geometryMaxLatitude(feature.geometry) > -58)
    .map((feature) => geometryPath(feature.geometry))
    .filter(Boolean)
);

function projectedVenue(venue, preparedVenue) {
  const latitude = Number(preparedVenue?.latitude ?? venue.latitude);
  const longitude = Number(preparedVenue?.longitude ?? venue.longitude);
  const projected = projectLonLat(longitude, latitude);
  if (projected) return projected;
  throw new Error(`Unable to project authoritative latitude/longitude for venue ${venue.id}`);
}

const MARKET_CENTER_IDS = Object.freeze({
  "New York": "new_york",
  Chicago: "chicago",
  Toronto: "toronto",
  Montreal: "montreal",
  "Mexico City": "mexico_city",
  "Sao Paulo": "sao_paulo",
  London: "london",
  Frankfurt: "frankfurt",
  Paris: "paris",
  Amsterdam: "amsterdam",
  Zurich: "zurich",
  Tokyo: "tokyo",
  Osaka: "osaka",
  "Hong Kong": "hong_kong",
  Singapore: "singapore",
  Seoul: "seoul",
  Taipei: "taipei",
  Shanghai: "shanghai",
  Shenzhen: "shenzhen",
  Mumbai: "mumbai",
  Sydney: "sydney",
  Auckland: "auckland",
});

const AMBIENT_MARKET_CENTER_OVERRIDES = Object.freeze({
  cboe: "Chicago",
});

const MARKET_CENTER_ANCHOR_VENUES = Object.freeze({
  "New York": "nyse",
  Chicago: "cme",
  Toronto: "tsx",
  "Mexico City": "bmv",
});

const MARKET_CENTER_VENUE_ORDER = Object.freeze({
  "New York": ["nyse", "nasdaq"],
  Chicago: ["cme", "cboe", "cfe"],
  Toronto: ["tsx"],
  "Mexico City": ["bmv"],
});

function ambientMarketCenterCity(venue) {
  return AMBIENT_MARKET_CENTER_OVERRIDES[venue.id] || venue.city;
}

const MARKET_CENTER_BENCHMARKS = Object.freeze({
  "New York": ["SPY", "QQQ", "DIA", "IWM"],
  Chicago: [],
  London: [],
  Tokyo: [],
  Sydney: [],
  "Sao Paulo": [],
});

const MARKET_DOMAIN_DEFINITIONS = Object.freeze([
  { id: "equities", label: "Equities", symbols: ["ES", "NQ", "SPY", "QQQ", "DIA", "IWM"], noun: "US equities" },
  { id: "gold", label: "Gold", symbols: ["GC", "MGC"], noun: "Gold" },
  { id: "rates", label: "Rates", symbols: ["ZB", "ZN", "ZF", "ZT"], noun: "Treasury futures" },
]);

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

const STATUS_WEIGHT = Object.freeze({
  OPEN: 8,
  AUCTION: 7,
  CLOSING_SOON: 6,
  PREOPEN: 5,
  LUNCH_BREAK: 4,
  STALE: 3,
  UNKNOWN: 2,
  HOLIDAY: 1,
  CLOSED: 0,
});

function strongestVenueStatus(statuses) {
  return statuses.reduce((strongest, status) => (
    strongest === null || (STATUS_WEIGHT[status] ?? -1) > (STATUS_WEIGHT[strongest] ?? -1) ? status : strongest
  ), null) || "UNKNOWN";
}

function stateColor(state) {
  return {
    PREOPEN: "#8a9189",
    AUCTION: "#79c990",
    OPEN: "#65d18b",
    CLOSING_SOON: "#74ca8c",
    CLOSED: "#c26f67",
    LUNCH_BREAK: "#7f8782",
    HOLIDAY: "#a56560",
    UNKNOWN: "#858d8d",
    STALE: "#d2a65f",
  }[state] || "#858d8d";
}

function stateRadius(state) {
  return {
    OPEN: 0.5,
    AUCTION: 0.48,
    CLOSING_SOON: 0.46,
    PREOPEN: 0.43,
    LUNCH_BREAK: 0.4,
    STALE: 0.4,
    UNKNOWN: 0.36,
    HOLIDAY: 0.31,
    CLOSED: 0.32,
  }[state] || 0.36;
}

function haloRadius(state) {
  return stateRadius(state) * ({
    OPEN: 3.8,
    AUCTION: 3.7,
    CLOSING_SOON: 3.7,
    CLOSED: 3.55,
    HOLIDAY: 3.45,
  }[state] || 3.5);
}

function macroRowsBySymbol() {
  return Object.fromEntries((preparedMacroContext?.observations || []).map((row) => [row.symbol, row]));
}

function tapeRowsBySymbol() {
  return Object.fromEntries((preparedMarketTape?.rows || []).map((row) => [row.symbol, row]));
}

function preparedVenueById() {
  return preparedVenueSessions?.venues
    ? Object.fromEntries(preparedVenueSessions.venues.map((venue) => [venue.venue_id, venue]))
    : {};
}

function parseTimestamp(value) {
  if (!value) return null;
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) ? timestamp : null;
}

function evidenceAgeSeconds(value) {
  const timestamp = parseTimestamp(value);
  if (timestamp === null) return null;
  return Math.max(0, Math.round((Date.now() - timestamp) / 1000));
}

function relativeAge(value) {
  const seconds = evidenceAgeSeconds(value);
  if (seconds === null) return "unknown";
  if (seconds < 90) return `${seconds}s ago`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 90) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 48) return `${hours}h ago`;
  return `${Math.round(hours / 24)}d ago`;
}

function freshnessFromEvidence(evidence = {}) {
  if (typeof evidence === "string") return evidence;
  if (evidence.freshness && typeof evidence.freshness === "string") return evidence.freshness;
  const staleAfter = parseTimestamp(evidence.stale_after || evidence.expires_at || evidence.fresh_until);
  if (staleAfter !== null && Date.now() > staleAfter) return "STALE";
  const generatedAt = evidence.generated_at || evidence.source_timestamp;
  const age = evidenceAgeSeconds(generatedAt);
  if (age === null) return "UNKNOWN";
  if (age <= 120) return "FRESH";
  if (age <= 900) return "AGING";
  return "STALE";
}

function displayFreshness(value) {
  if (!value) return "UNKNOWN";
  const normalized = String(value).toUpperCase();
  if (["FRESH", "AGING", "STALE", "UNKNOWN"].includes(normalized)) return normalized;
  if (normalized === "UNAVAILABLE") return "UNKNOWN";
  return "UNKNOWN";
}

function venueEvidenceFreshness(venue) {
  return freshnessFromEvidence({
    generated_at: venue?.generated_at || preparedVenueSessions?.generated_at,
    stale_after: venue?.freshness?.stale_after,
  });
}

function effectiveVenueStatus(venue, fallbackStatus) {
  if (!venue) return fallbackStatus || "UNKNOWN";
  const freshness = venueEvidenceFreshness(venue);
  if (freshness === "STALE") return "STALE";
  return venue.session_status || fallbackStatus || "UNKNOWN";
}

function venueStateById(scenario = scenarios[currentScenarioName]) {
  return preparedVenueSessions?.venues
    ? Object.fromEntries(preparedVenueSessions.venues.map((venue) => [venue.venue_id, effectiveVenueStatus(venue, scenario.venues?.[venue.venue_id])]))
    : scenario.venues;
}

function venueLedState(status) {
  if (["OPEN", "AUCTION", "CLOSING_SOON"].includes(status)) return "GREEN";
  if (["CLOSED", "HOLIDAY"].includes(status)) return "RED";
  if (status === "STALE") return "AMBER";
  return "GRAY";
}

function statusLabel(status) {
  return {
    GREEN: "GREEN",
    AMBER: "AMBER",
    RED: "RED",
    GRAY: "GRAY",
  }[status] || "GRAY";
}

function ledStateForDisplayClass(displayClass) {
  return {
    HEALTHY: "GREEN",
    VALID_WITH_WARNINGS: "AMBER",
    DEGRADED: "AMBER",
    STALE: "AMBER",
    EVIDENCE_UNRELIABLE: "AMBER",
    NOT_READY: "GRAY",
    UNKNOWN: "GRAY",
    INVALID: "RED",
  }[displayClass] || "GRAY";
}

function changePhrase(rows, noun) {
  const usable = rows.filter((row) => row && typeof row.change_pct === "number");
  if (!usable.length) return `${noun} unavailable`;
  const positives = usable.filter((row) => row.change_pct > 0.005).length;
  const negatives = usable.filter((row) => row.change_pct < -0.005).length;
  if (positives && negatives) return `${noun} mixed`;
  if (positives) return `${noun} firm`;
  if (negatives) return `${noun} softer`;
  return `${noun} steady`;
}

function domainState(domain) {
  const bySymbol = macroRowsBySymbol();
  const rows = domain.symbols.map((symbol) => bySymbol[symbol]).filter(Boolean);
  const available = rows.filter((row) => row.status === "AVAILABLE");
  const stale = rows.some((row) => row.status === "STALE" || row.freshness === "STALE");
  return {
    ...domain,
    rows,
    phrase: changePhrase(available, domain.noun),
    ledState: available.length ? (stale ? "AMBER" : "GREEN") : "GRAY",
  };
}

function sourceAge(value) {
  return relativeAge(value);
}

function formatChangePct(row) {
  if (!row || typeof row.change_pct !== "number") return "n/a";
  return `${row.change_pct >= 0 ? "+" : ""}${row.change_pct.toFixed(2)}%`;
}

function contextRows(rows) {
  return rows
    .filter(([, value]) => value !== undefined && value !== null && value !== "")
    .map(([label, value]) => `
      <div class="context-row">
        <span>${escapeHtml(label)}</span>
        <strong>${escapeHtml(value)}</strong>
      </div>
    `).join("");
}

function contextSection(title, rows) {
  const body = Array.isArray(rows) ? contextRows(rows) : rows;
  if (!body) return "";
  return `
    <section class="context-section">
      <h4>${escapeHtml(title)}</h4>
      ${body}
    </section>
  `;
}

function sourceFooter({ label, generatedAt, freshness, source }) {
  const shownFreshness = displayFreshness(freshness || freshnessFromEvidence({ generated_at: generatedAt }));
  return `
    <footer class="context-footer">
      <div>${escapeHtml(label || "Prepared source")}</div>
      <div>Updated ${escapeHtml(relativeAge(generatedAt))} · ${escapeHtml(shownFreshness)}</div>
    </footer>
  `;
}

function technicalDetail(rows) {
  const body = contextRows(rows);
  if (!body) return "";
  return `
    <details class="context-technical">
      <summary>Technical detail</summary>
      ${body}
    </details>
  `;
}

function openContext(selection) {
  selectedContext = selection;
  contextPanel.hidden = false;
  contextPanel.dataset.open = "true";
  contextPanel.dataset.selection = `${selection.type}:${selection.id}`;
  contextContent.innerHTML = renderContext(selection);
  observatoryPerformance.counts.context += 1;
}

function closeContext() {
  selectedContext = null;
  contextPanel.dataset.open = "false";
  contextPanel.dataset.selection = "";
  contextPanel.hidden = true;
  contextContent.innerHTML = "";
}

function panelStatusLine(label, state) {
  return `<p class="context-status" data-led-state="${escapeHtml(state)}"><span></span>${escapeHtml(label)}</p>`;
}

function sourceLine(source) {
  if (!source) return "Prepared or fixture source unavailable";
  return source;
}

function renderMarketCenterContext(centerId) {
  const cityName = Object.entries(MARKET_CENTER_IDS).find(([, id]) => id === centerId)?.[0]
    || centerId.replaceAll("_", " ");
  const preparedVenuesById = preparedVenueById();
  const venueStates = venueStateById();
  const orderedVenueIds = MARKET_CENTER_VENUE_ORDER[cityName] || [];
  const cityVenues = venues
    .filter((venue) => (MARKET_CENTER_IDS[ambientMarketCenterCity(venue)] || "") === centerId)
    .sort((left, right) => {
      const leftIndex = orderedVenueIds.indexOf(left.id);
      const rightIndex = orderedVenueIds.indexOf(right.id);
      return (leftIndex === -1 ? 999 : leftIndex) - (rightIndex === -1 ? 999 : rightIndex);
    });
  const benchmarkRows = (MARKET_CENTER_BENCHMARKS[cityName] || [])
    .map((symbol) => macroRowsBySymbol()[symbol])
    .filter(Boolean);
  const venueRows = cityVenues.map((venue) => {
    const prepared = preparedVenuesById[venue.id];
    const status = effectiveVenueStatus(prepared, venueStates[venue.id]);
    return [venue.label, `${status} · ${venueLedState(status)}`];
  });
  const benchmarkBody = benchmarkRows.length
    ? benchmarkRows.map((row) => [`${row.symbol}${row.source_type === "ETF_PROXY" ? " proxy" : ""}`, `${formatChangePct(row)}${row.source_type === "ETF_PROXY" ? " · ETF proxy" : ""}`])
    : [["Benchmarks", "No source-backed local benchmark values available"]];
  const cmeProductRows = cityName === "Chicago"
    ? (preparedVenuesById.cme?.product_session_states || []).map((product) => [
      product.product_group,
      `${product.product_session_state || "UNKNOWN"}${product.monitored ? " · monitored" : " · informational"}`,
    ])
    : [];
  const updated = preparedVenueSessions?.generated_at || preparedMacroContext?.generated_at;
  const venueFreshness = freshnessFromEvidence({
    generated_at: preparedVenueSessions?.generated_at,
    stale_after: preparedVenueSessions?.venues?.[0]?.freshness?.stale_after,
  });
  return `
    <h3>${escapeHtml(cityName)}</h3>
    ${panelStatusLine(`Market center · ${venueFreshness}`, venueFreshness === "STALE" ? "AMBER" : "GRAY")}
    ${contextSection("Venues", venueRows)}
    ${contextSection("CME Product Sessions", cmeProductRows)}
    ${contextSection("Benchmarks", benchmarkBody)}
    ${contextSection("Operator note", [
      ["Identity", "One ambient market-center dot; venue detail remains in this panel"],
      ["Coordinates", "Market center remains anchored to projected latitude/longitude"],
    ])}
    ${sourceFooter({
      label: "Venue Calendar",
      generatedAt: updated,
      freshness: venueFreshness,
      source: preparedVenueSessions?.source_authority,
    })}
    ${technicalDetail([
      ["source_authority", preparedVenueSessions?.source_authority || "fixture"],
      ["source_authority_version", preparedVenueSessions?.source_authority_version || "unknown"],
      ["generated_at", preparedVenueSessions?.generated_at || "unknown"],
      ["stale_after", preparedVenueSessions?.venues?.[0]?.freshness?.stale_after || "unknown"],
      ["model_status", preparedVenueSessions?.model_status || "fixture"],
    ])}
  `;
}

function renderVenueContext(venueId) {
  const venue = venues.find((item) => item.id === venueId);
  const prepared = preparedVenueById()[venueId];
  const status = effectiveVenueStatus(prepared, venueStateById()[venueId]);
  const ledState = venueLedState(status);
  if (!venue) return `<h3>Unknown venue</h3>${panelStatusLine("UNKNOWN", "GRAY")}`;
  const rawStatus = prepared?.session_status || "UNKNOWN";
  const freshness = venueEvidenceFreshness(prepared);
  const productRows = Array.isArray(prepared?.product_session_states)
    ? prepared.product_session_states.map((row) => [
      row.product_group,
      `${row.product_session_state || "UNKNOWN"}${row.source_limitation ? ` · ${row.source_limitation}` : ""}`,
    ])
    : [];
  return `
    <h3>${escapeHtml(venue.label)}</h3>
    ${panelStatusLine(`${status} · ${ledState}`, ledState)}
    ${contextSection("Venue", [
      ["City", venue.city],
      ["Exchange", prepared?.exchange || venue.label],
      ["Venue operational state", prepared?.venue_operational_state || "unknown"],
      ["Local time", prepared?.local_time || "unknown"],
      ["Session status", status === "STALE" ? "stale evidence" : rawStatus],
      ["Next transition", prepared?.next_transition_at || "unknown"],
    ])}
    ${productRows.length ? contextSection("Product sessions", productRows) : ""}
    ${prepared?.limitation ? contextSection("Limitations", [["Source limitation", prepared.limitation]]) : ""}
    ${sourceFooter({
      label: "Venue Calendar",
      generatedAt: prepared?.generated_at || preparedVenueSessions?.generated_at,
      freshness,
      source: prepared?.calendar_source || preparedVenueSessions?.source_authority,
    })}
    ${technicalDetail([
      ["producer_status", rawStatus],
      ["display_status", status],
      ["source_authority", prepared?.source_authority || preparedVenueSessions?.source_authority || "unknown"],
      ["source_authority_version", prepared?.source_authority_version || preparedVenueSessions?.source_authority_version || "unknown"],
      ["timezone", prepared?.timezone || "unknown"],
      ["generated_at", prepared?.generated_at || preparedVenueSessions?.generated_at || "unknown"],
      ["stale_after", prepared?.freshness?.stale_after || "unknown"],
      ["coverage", prepared?.coverage || "unknown"],
      ["session_aggregation", prepared?.session_aggregation_rule || "venue-level"],
    ])}
  `;
}

function producerStageByContextId(contextId) {
  const stages = displayFlowStages(preparedSystemFlow?.stages?.length ? preparedSystemFlow.stages : scenarios[currentScenarioName].flow);
  return stages.find((stage) => (stage.context_id || stage.label) === contextId || stage.label === contextId);
}

function renderSystemStageContext(contextId) {
  const stage = producerStageByContextId(contextId) || { label: contextId, display_class: "UNKNOWN", producer_status: "UNKNOWN" };
  const ledState = stage.led_state || ledStateForDisplayClass(stage.display_class || stage.texture);
  const freshness = freshnessFromEvidence({
    generated_at: stage.generated_at,
    stale_after: stage.freshness?.expires_at || stage.freshness?.fresh_until,
  });
  const operatorRows = [
    ["Reason", explanationForStage(stage)],
    ["Last updated", relativeAge(stage.generated_at)],
  ];
  const runtime = preparedRuntimeBrokerContext?.runtime;
  const broker = preparedRuntimeBrokerContext?.broker;
  const extra = [];
  if (stage.label === "Magic Runtime" && runtime) {
    operatorRows.push(["Mode", runtime.mode || "unknown"]);
    operatorRows.push(["PID", runtime.source_pid ?? "unknown"]);
    operatorRows.push(["Lanes", runtime.active_lane_count ?? "unknown"]);
    operatorRows.push(["Submit authority", runtime.submit_authority === true ? "Enabled" : "Disabled"]);
    extra.push(["Runtime classification", runtime.classification || "unknown"]);
    extra.push(["Mode", runtime.mode || "unknown"]);
  }
  if (stage.label === "Broker / TWS" && broker) {
    operatorRows.push(["Mode", broker.mode || "unknown"]);
    operatorRows.push(["Positions", broker.broker_position_count ?? "unknown"]);
    operatorRows.push(["Open orders", broker.open_order_count ?? "unknown"]);
    operatorRows.push(["Unknown orders", broker.unknown_open_order_count ?? "unknown"]);
    operatorRows.push(["Submit authority", broker.submit_authority === true ? "Enabled" : "Disabled"]);
    extra.push(["Reconciliation", broker.reconciliation_classification || "unknown"]);
  }
  return `
    <h3>${escapeHtml(displayLabelForStage(stage.label))}</h3>
    ${panelStatusLine(ledState, ledState)}
    ${contextSection("Operator view", operatorRows)}
    ${extra.length ? contextSection("Prepared detail", extra) : ""}
    ${sourceFooter({
      label: displayLabelForStage(stage.label),
      generatedAt: stage.generated_at,
      freshness,
      source: stage.source_artifact,
    })}
    ${technicalDetail([
      ["producer_status", stage.producer_status || "UNKNOWN"],
      ["display_class", stage.display_class || stage.texture || "UNKNOWN"],
      ["source_artifact", sourceLine(stage.source_artifact)],
      ["generated_at", stage.generated_at || "unknown"],
      ["freshness", JSON.stringify(stage.freshness || {})],
      ["schema_version", preparedSystemFlow?.schema_version || "fixture"],
      ["detail", stage.detail || "none"],
    ])}
    <p class="context-note">Display only. This panel does not compute readiness, Safe-State, broker coherence, or trading authority.</p>
  `;
}

function renderMarketDomainContext(domainId) {
  const domain = MARKET_DOMAIN_DEFINITIONS.find((item) => item.id === domainId);
  if (!domain) return `<h3>Market context</h3>${panelStatusLine("UNKNOWN", "GRAY")}`;
  const state = domainState(domain);
  const rows = state.rows.length
    ? state.rows.map((row) => [
      row.symbol,
      `${row.value ?? row.last ?? "n/a"} · ${row.change_abs ?? "n/a"} · ${formatChangePct(row)} · ${row.freshness || row.status}`,
    ])
    : [["Evidence", "No prepared factual observations available"]];
  const latest = state.rows.find((row) => row.generated_at)?.generated_at || preparedMacroContext?.generated_at;
  return `
    <h3>${escapeHtml(domain.label)}</h3>
    ${panelStatusLine(`${state.phrase} · ${state.ledState}`, state.ledState)}
    ${contextSection("Current instruments", rows)}
    ${contextSection("Factual context", [
      ["Classifier", "No bullish/bearish/risk-on/risk-off classifier in this slice"],
      ["Evidence", state.rows.length ? "Prepared macro-market observations" : "No current prepared observations"],
    ])}
    ${sourceFooter({
      label: "Macro Market Context",
      generatedAt: latest,
      freshness: freshnessFromEvidence({ generated_at: latest }),
      source: "prepared macro-market context snapshot",
    })}
    ${technicalDetail([
      ["schema_version", preparedMacroContext?.schema_version || "unknown"],
      ["status", preparedMacroContext?.status || "unknown"],
      ["generated_at", preparedMacroContext?.generated_at || "unknown"],
      ["domain_symbols", domain.symbols.join(", ")],
      ["regime_classifier", "none"],
    ])}
  `;
}

function renderTickerContext(symbol) {
  const row = tapeRowsBySymbol()[symbol] || macroRowsBySymbol()[symbol];
  const macro = macroRowsBySymbol()[symbol];
  if (!row) return `<h3>${escapeHtml(symbol)}</h3>${panelStatusLine("No prepared row", "GRAY")}`;
  const generatedAt = row.source_timestamp || macro?.source_timestamp || preparedMarketTape?.generated_at;
  const freshness = freshnessFromEvidence({ generated_at: generatedAt });
  return `
    <h3>${escapeHtml(row.display_symbol || row.symbol || symbol)}</h3>
    ${panelStatusLine(row.market_status || macro?.market_status || "DISPLAY ONLY", "GRAY")}
    ${contextSection("Tape", [
      ["Last", row.last ?? macro?.value ?? "n/a"],
      ["Change", `${row.changeAbs ?? macro?.change_abs ?? "n/a"} ${row.changePct ?? formatChangePct(macro)}`],
      ["Market status", row.market_status || macro?.market_status || "unknown"],
    ])}
    ${contextSection("Guardrails", [
      ["Display only", "true"],
      ["Trading input", "false"],
    ])}
    ${sourceFooter({
      label: preparedMarketTape?.source_kind || macro?.source_dataset || "Prepared snapshot",
      generatedAt,
      freshness,
      source: row.source_artifact || macro?.source_artifact,
    })}
    ${technicalDetail([
      ["source_timestamp", row.source_timestamp || macro?.source_timestamp || "unknown"],
      ["source_id", row.source_id || macro?.source_provenance?.source_id || "unknown"],
      ["source_dataset", macro?.source_dataset || "unknown"],
      ["source_artifact", row.source_artifact || macro?.source_artifact || "unknown"],
      ["schema_version", preparedMarketTape?.schema_version || macro?.schema_version || "unknown"],
    ])}
  `;
}

function renderMarketBriefingContext() {
  const briefing = macroContextNote();
  const domainSections = MARKET_DOMAIN_DEFINITIONS.map((domain) => {
    const state = domainState(domain);
    return [domain.label, `${state.phrase} · ${state.ledState}`];
  });
  return `
    <h3>Market briefing</h3>
    ${panelStatusLine("Factual prepared context", "GRAY")}
    <p class="context-lead">${escapeHtml(briefing?.primary || scenarios[currentScenarioName].canvasNote)}</p>
    ${contextSection("Domains", domainSections)}
    ${contextSection("Detail", [
      ["Values", briefing?.detail || "Detailed values unavailable"],
      ["Regime classifier", "none"],
    ])}
    ${sourceFooter({
      label: "Macro Market Context",
      generatedAt: preparedMacroContext?.generated_at,
      freshness: freshnessFromEvidence({ generated_at: preparedMacroContext?.generated_at }),
      source: "prepared macro-market context snapshot",
    })}
    ${technicalDetail([
      ["schema_version", preparedMacroContext?.schema_version || "fixture"],
      ["generated_at", preparedMacroContext?.generated_at || "fixture fallback"],
      ["domain_source", "MARKET_DOMAIN_DEFINITIONS"],
      ["regime_classifier", "none"],
    ])}
  `;
}

function renderContext(selection) {
  if (!selection) return "";
  const actionBoundary = technicalDetail([
    ["action_boundary", "Future action layers, including click-to-trade, are deferred."],
    ["authority", "Display evidence only; no broker, runtime, strategy, or research authority."],
  ]);
  if (selection.type === "market-center") return `${renderMarketCenterContext(selection.id)}${actionBoundary}`;
  if (selection.type === "venue") return `${renderVenueContext(selection.id)}${actionBoundary}`;
  if (selection.type === "system-stage") return `${renderSystemStageContext(selection.id)}${actionBoundary}`;
  if (selection.type === "market-domain") return `${renderMarketDomainContext(selection.id)}${actionBoundary}`;
  if (selection.type === "ticker") return `${renderTickerContext(selection.id)}${actionBoundary}`;
  if (selection.type === "market-briefing") return `${renderMarketBriefingContext()}${actionBoundary}`;
  return `<h3>Context</h3>${panelStatusLine("UNKNOWN", "GRAY")}${actionBoundary}`;
}

function drawWorldBase() {
  const land = LAND_PATHS.map((path) => `<path d="${path}" />`).join("");
  const stars = SPACE_STARS.map(([x, y, radius, opacity]) => (
    `<circle cx="${x}" cy="${y}" r="${radius}" opacity="${opacity}" />`
  )).join("");
  arc.innerHTML = `
    <defs>
      <radialGradient id="atlanticWarmth">
        <stop offset="0%" stop-color="rgba(224,159,90,0.2)" />
        <stop offset="58%" stop-color="rgba(210,139,76,0.08)" />
        <stop offset="100%" stop-color="rgba(210,139,76,0)" />
      </radialGradient>
      <filter id="solarSoftness" x="-7%" y="-7%" width="114%" height="114%">
        <feGaussianBlur stdDeviation="1.45" />
      </filter>
      <filter id="twilightSoftness" x="-9%" y="-9%" width="118%" height="118%">
        <feGaussianBlur stdDeviation="2.05" />
      </filter>
      <filter id="atlanticSoftness" x="-20%" y="-45%" width="140%" height="190%">
        <feGaussianBlur stdDeviation="3.2" />
      </filter>
    </defs>
    <g class="space-layer" data-background-model="open-black-space" aria-hidden="true">
      <rect class="space-black" x="-120" y="-80" width="340" height="260" />
      <g class="space-stars">${stars}</g>
    </g>
    <g>
      <g class="atlas-graticule" aria-hidden="true">
        <path d="M -1 36 C 22 32, 75 32, 101 36" />
        <path d="M -3 50 C 22 47, 76 47, 103 50" />
        <path d="M -1 64 C 22 68, 75 68, 101 64" />
        <path d="M 16 21 C 20 39, 20 64, 15 81" />
        <path d="M 33 15 C 35 36, 35 68, 32 86" />
        <path d="M 50 13 C 50 34, 50 70, 50 87" />
        <path d="M 67 15 C 65 36, 66 68, 69 86" />
        <path d="M 84 21 C 80 39, 81 64, 86 81" />
      </g>
      <g id="solar-daylight-layer" class="solar-daylight" aria-hidden="true"></g>
      <g id="solar-twilight-layer" class="solar-twilight" aria-hidden="true"></g>
      <g id="solar-deep-twilight-layer" class="solar-deep-twilight" aria-hidden="true"></g>
      <g id="solar-night-layer" class="solar-night" aria-hidden="true"></g>
      <g class="atlas-land" aria-hidden="true">${land}</g>
      <g class="atlas-coastline" aria-hidden="true">${land}</g>
    </g>
    <g id="atlantic-overlap-layer" aria-hidden="true"></g>
    <g id="market-center-layer"></g>
    <g id="market-center-labels-layer"></g>
  `;
}

function drawSolarLayer(date = new Date()) {
  const illumination = solarIllumination(date);
  arc.querySelector("#solar-daylight-layer").innerHTML = illumination.daylight;
  arc.querySelector("#solar-twilight-layer").innerHTML = illumination.twilight;
  arc.querySelector("#solar-deep-twilight-layer").innerHTML = illumination.deepTwilight;
  arc.querySelector("#solar-night-layer").innerHTML = illumination.night;
}

function cityGroupsForScenario(scenario) {
  const venueStatesById = venueStateById(scenario);
  const preparedVenuesById = preparedVenueById();
  const cityGroups = new Map();
  for (const venue of venues) {
    const state = venueStatesById[venue.id] || "UNKNOWN";
    const ambientCity = ambientMarketCenterCity(venue);
    const city = cityGroups.get(ambientCity) || {
      city: ambientCity,
      region: venue.region,
      point: null,
      states: [],
      labels: [],
    };
    city.states.push(state);
    city.labels.push(venue.label);
    city.venueIds = [...(city.venueIds || []), venue.id];
    cityGroups.set(ambientCity, city);
  }
  for (const city of cityGroups.values()) {
    const anchorVenueId = MARKET_CENTER_ANCHOR_VENUES[city.city]
      || city.venueIds.find((venueId) => venues.find((venue) => venue.id === venueId)?.city === city.city)
      || city.venueIds[0];
    const anchorVenue = venues.find((venue) => venue.id === anchorVenueId);
    city.point = projectedVenue(anchorVenue, preparedVenuesById[anchorVenueId]);
  }
  return cityGroups;
}

function venueGeometryInputs() {
  const preparedVenuesById = preparedVenueById();
  return venues.map((venue) => ({
    id: venue.id,
    latitude: Number(preparedVenuesById[venue.id]?.latitude ?? venue.latitude),
    longitude: Number(preparedVenuesById[venue.id]?.longitude ?? venue.longitude),
  }));
}

function drawMarketCenterLayer(scenario) {
  const venueStatesById = venueStateById(scenario);
  const preparedVenuesById = preparedVenueById();
  const cityGroups = cityGroupsForScenario(scenario);
  const nodes = [...cityGroups.values()].map((city) => {
    const state = strongestVenueStatus(city.states);
    const point = city.point;
    const featured = isPrimaryMarketCenter(city.city, city.region);
    const scale = featured ? 1 : 0.72;
    const ledState = venueLedState(state);
    const centerId = MARKET_CENTER_IDS[city.city] || city.city.toLowerCase().replaceAll(" ", "_");
    return `
      <g class="market-center-group selectable" data-city="${escapeHtml(city.city)}" data-region="${city.region}" data-featured="${featured}" data-context-type="market-center" data-context-id="${centerId}" tabindex="0" role="button" aria-label="${city.city} market center ${state}">
        <circle class="market-center-hit-target" cx="${point.x.toFixed(2)}" cy="${point.y.toFixed(2)}" r="0.74" />
        <circle class="venue-harbor" data-state="${state}" cx="${point.x.toFixed(2)}" cy="${point.y.toFixed(2)}" r="${(haloRadius(state) * scale).toFixed(3)}" fill="${stateColor(state)}" />
        <circle class="venue-node" data-state="${state}" data-led-state="${ledState}" cx="${point.x.toFixed(2)}" cy="${point.y.toFixed(2)}" r="${(stateRadius(state) * scale).toFixed(3)}" fill="${stateColor(state)}">
          <title>${city.city}: ${city.labels.join(" / ")} · ${state}</title>
        </circle>
      </g>
    `;
  });

  arc.querySelector("#market-center-layer").innerHTML = nodes.join("");
  const active = (ids) => ids.some((id) => ["OPEN", "AUCTION", "CLOSING_SOON"].includes(venueStatesById[id]));
  const atlanticLayer = arc.querySelector("#atlantic-overlap-layer");
  if (active(["lse", "eurex", "paris", "amsterdam", "six"]) && active(["nyse", "nasdaq", "cboe", "cme", "cfe"])) {
    const london = projectedVenue(venues.find((venue) => venue.id === "lse"), preparedVenuesById.lse);
    const newYork = projectedVenue(venues.find((venue) => venue.id === "nyse"), preparedVenuesById.nyse);
    const centerX = (london.x + newYork.x) / 2;
    const centerY = (london.y + newYork.y) / 2;
    const distance = Math.hypot(london.x - newYork.x, london.y - newYork.y);
    const angle = Math.atan2(london.y - newYork.y, london.x - newYork.x) * 180 / Math.PI;
    atlanticLayer.innerHTML = `<ellipse class="atlantic-overlap-wash" cx="${centerX.toFixed(2)}" cy="${centerY.toFixed(2)}" rx="${(distance * 0.58).toFixed(2)}" ry="4.8" transform="rotate(${angle.toFixed(2)} ${centerX.toFixed(2)} ${centerY.toFixed(2)})" />`;
  } else {
    atlanticLayer.innerHTML = "";
  }
  updateMarketCenterLabelStates(cityGroups);
}

function drawMarketCenterLabels(scenario) {
  const cityGroups = cityGroupsForScenario(scenario);
  const anchors = [...cityGroups.values()].map((city) => ({ city: city.city, region: city.region, x: city.point.x, y: city.point.y }));
  const layout = layoutCityLabels(anchors);
  const labels = layout.map((label) => {
    const city = cityGroups.get(label.city);
    const state = strongestVenueStatus(city.states);
    const centerId = MARKET_CENTER_IDS[city.city] || city.city.toLowerCase().replaceAll(" ", "_");
    const leader = label.leader
      ? `<line class="venue-label-leader" data-city="${escapeHtml(city.city)}" x1="${label.anchorX.toFixed(2)}" y1="${label.anchorY.toFixed(2)}" x2="${label.x.toFixed(2)}" y2="${label.y.toFixed(2)}" />`
      : "";
    return `
      ${leader}
      <text class="venue-label selectable" data-city="${escapeHtml(city.city)}" data-state="${state}" data-placement="${label.placement}" data-anchor-x="${label.anchorX.toFixed(3)}" data-anchor-y="${label.anchorY.toFixed(3)}" data-label-distance="${label.distance.toFixed(3)}" data-context-type="market-center" data-context-id="${centerId}" x="${label.x.toFixed(2)}" y="${label.y.toFixed(2)}" text-anchor="${label.anchor}" tabindex="0" role="button">
        <title>${city.city}: ${city.labels.join(" / ")}</title>${city.city}
      </text>
    `;
  });
  arc.querySelector("#market-center-labels-layer").innerHTML = labels.join("");
  window.observatoryMapDiagnostics = {
    anchors,
    labels: layout,
    labelLayout: labelLayoutDiagnostics(layout),
  };
}

function updateMarketCenterLabelStates(cityGroups) {
  for (const label of arc.querySelectorAll(".venue-label")) {
    const city = cityGroups.get(label.dataset.city);
    if (city) label.dataset.state = strongestVenueStatus(city.states);
  }
}

function drawTape(scenario) {
  const sourceRows = preparedMarketTape?.rows?.length ? preparedMarketTape.rows : scenario.tape;
  const rows = [...sourceRows, ...sourceRows];
  tape.innerHTML = rows.map((row) => {
    const numericChange = Number.parseFloat(String(row.changeAbs || "0").replace(/,/g, ""));
    const direction = numericChange > 0 ? "up" : numericChange < 0 ? "down" : "flat";
    return `
      <button class="tape-item selectable" type="button" data-context-type="ticker" data-context-id="${escapeHtml(row.symbol)}" aria-label="${escapeHtml(row.symbol)} market tape context">
        <span class="tape-symbol">${row.symbol}</span>
        <span class="split-flap">${row.last}</span>
        <span class="tape-change" data-direction="${direction}">${row.changeAbs} ${row.changePct}</span>
        <span class="tape-freshness">${row.freshness}</span>
      </button>
    `;
  }).join("");
}

function drawFlow(scenario) {
  const baseFlow = preparedSystemFlow?.stages?.length ? preparedSystemFlow.stages : scenario.flow;
  const sourceFlow = displayFlowStages(baseFlow);
  const healthByLabel = preparedOperationalHealth?.indicators
    ? Object.fromEntries(preparedOperationalHealth.indicators.map((indicator) => [indicator.label, indicator]))
    : {};
  flow.innerHTML = sourceFlow.map((stage, index) => {
    const isActiveRecon = stage.texture === "RECONCILING" && scenario.reconciliationState === "ACTIVE";
    const shouldAnimate = isActiveRecon || stage.texture === "PENDING_ORDER" || stage.event;
    const displayClass = stage.display_class || stage.texture || "UNKNOWN";
    const health = healthByLabel[stage.label];
    const ledState = stage.led_state || health?.led_state || ledStateForDisplayClass(displayClass);
    const displayLabel = displayLabelForStage(stage.label);
    return `
      <section class="flow-stage selectable" data-context-type="${stage.context_type || "system-stage"}" data-context-id="${stage.context_id || stage.label}" data-texture="${stage.texture}" data-display-class="${displayClass}" data-led-state="${ledState}" data-animate="${shouldAnimate ? "true" : "false"}" data-event="${stage.event ? "true" : "false"}" style="--stage-index: ${index}; --data-velocity: ${scenario.dataVelocity}" tabindex="0" role="button" aria-label="${displayLabel} ${ledState}">
        <span class="flow-led" aria-hidden="true"></span>
        <div class="flow-lens"></div>
        <div class="flow-label">${displayLabel}</div>
      </section>
    `;
  }).join("");
}

function displayFlowStages(baseFlow) {
  const stageByLabel = Object.fromEntries(baseFlow.map((stage) => [stage.label, stage]));
  const domains = MARKET_DOMAIN_DEFINITIONS.map((domain) => {
    const state = domainState(domain);
    return {
      label: domain.label,
      texture: state.ledState === "GREEN" ? "HEALTHY" : state.ledState === "AMBER" ? "LOW_CONFIDENCE_DATA" : "LOW_CONFIDENCE_DATA",
      display_class: state.ledState === "GREEN" ? "HEALTHY" : state.ledState === "AMBER" ? "VALID_WITH_WARNINGS" : "UNKNOWN",
      led_state: state.ledState,
      context_type: "market-domain",
      context_id: domain.id,
      producer_status: state.phrase,
      event: false,
    };
  });
  return [
    stageByLabel["Market Data"],
    ...domains,
    stageByLabel["Magic Runtime"],
    stageByLabel["Broker / TWS"],
    stageByLabel["Trade Evidence"],
    stageByLabel["CRR / Research"],
    stageByLabel["Prospective Validation"],
  ].filter(Boolean);
}

function displayLabelForStage(label) {
  return {
    "Trade Evidence": "Trade Lifecycle",
    "Prospective Validation": "Forward Validation",
  }[label] || label;
}

function explanationForStage(stage) {
  const status = stage.producer_status || stage.texture || "UNKNOWN";
  const detail = stage.detail || "";
  const prospectiveTrades = detail.match(/prospective_trades=(\d+)/)?.[1];
  if (stage.label === "Market Data" && status === "PHASE1_DATABENTO_LIVE_LISTENER_RUNNING") return "Feed live";
  if (stage.label === "Regime / Context" && status === "NOT_READY") {
    return preparedMacroContext?.observations?.length ? "Regime unavailable · macro live" : "No current regime producer";
  }
  if (stage.label === "Magic Runtime" && status === "STALE_RUNTIME_TRUTH") return "Runtime truth stale";
  if (stage.label === "Magic Runtime" && status === "COMMIT_MISMATCH") return "Commit mismatch";
  if (stage.label === "Broker / TWS" && status === "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE") return "Order-status evidence unreliable";
  if (stage.label === "Trade Evidence" && status === "NO_ELIGIBLE_EXITS") return "No eligible exits";
  if (stage.label === "Trade Evidence" && status === "TRACK_B_MANAGED_EXIT_SERVICE_POST_BROKER_MUTATION_REFRESH_RUNNING") return "Managed Exit active";
  if (stage.label === "Trade Evidence" && status === "APPLY_SUCCEEDED") return "Exit apply succeeded";
  if (stage.label === "CRR / Research" && status === "VALID_WITH_WARNINGS") return "Valid with warnings";
  if (stage.label === "Prospective Validation" && status === "VALID_WITH_WARNINGS") {
    return prospectiveTrades ? `Valid with warnings · ${prospectiveTrades} trades` : "Valid with warnings";
  }
  if (stage.display_class === "STALE") return "Evidence stale";
  if (stage.display_class === "NOT_READY") return "Not ready";
  if (stage.display_class === "EVIDENCE_UNRELIABLE") return "Evidence unreliable";
  if (stage.display_class === "VALID_WITH_WARNINGS") return "Valid with warnings";
  if (stage.display_class === "HEALTHY") return "Healthy";
  return "Unknown";
}

function macroContextNote() {
  const rows = preparedMacroContext?.observations?.filter((row) => row.status === "AVAILABLE") || [];
  if (!rows.length) return null;
  const bySymbol = Object.fromEntries(rows.map((row) => [row.symbol, row]));
  const available = (symbols) => symbols.map((symbol) => bySymbol[symbol]).filter((row) => row && typeof row.change_pct === "number");
  const toneFor = (items, noun) => {
    if (!items.length) return null;
    const positives = items.filter((row) => row.change_pct > 0.005).length;
    const negatives = items.filter((row) => row.change_pct < -0.005).length;
    if (positives && negatives) return `${noun} mixed`;
    if (positives) return `${noun} firm`;
    if (negatives) return `${noun} softer`;
    return `${noun} steady`;
  };
  const primary = [
    toneFor(available(["ES", "NQ", "SPY", "QQQ", "DIA", "IWM"]), "US equities"),
    toneFor(available(["GC", "MGC"]), "Gold"),
    toneFor(available(["ZB", "ZN", "ZF", "ZT"]), "Treasury futures"),
  ].filter(Boolean);
  const detail = ["ES", "NQ", "GC", "ZN"]
    .map((symbol) => bySymbol[symbol])
    .filter((row) => row && typeof row.change_pct === "number")
    .map((row) => `${row.symbol} ${row.change_pct >= 0 ? "+" : ""}${row.change_pct.toFixed(2)}%`);
  if (!primary.length) return null;
  return {
    primary: primary.join(" • "),
    detail: detail.length ? detail.join(" · ") : "Detailed values unavailable",
  };
}

function sessionPhaseNote(scenario) {
  const venueStatesById = preparedVenueSessions?.venues
    ? Object.fromEntries(preparedVenueSessions.venues.map((venue) => [venue.venue_id, venue.session_status || "UNKNOWN"]))
    : scenario.venues;
  if (preparedVenueSessions?.venues?.length) {
    const venueFreshness = freshnessFromEvidence({
      generated_at: preparedVenueSessions.generated_at,
      stale_after: preparedVenueSessions.venues[0]?.freshness?.stale_after,
    });
    if (venueFreshness === "STALE") {
      return "Venue status stale";
    }
  }
  const active = (ids) => ids.some((id) => ["OPEN", "AUCTION", "CLOSING_SOON"].includes(venueStatesById[id]));
  const asia = active(["jpx", "osaka", "hkex", "sgx", "krx", "twse", "sse", "szse", "nse", "asx", "nzx"]);
  const europe = active(["lse", "eurex", "paris", "amsterdam", "six"]);
  const americas = active(["nyse", "nasdaq", "cboe", "cme", "cfe", "tsx", "mx", "bmv", "b3"]);
  if (europe && americas) return "Atlantic overlap";
  if (americas) return "New York afternoon";
  if (europe) return "London active";
  if (asia) return "Asia active";
  return "Global markets quiet";
}

function updateCommentary(scenario) {
  scenarioClock.textContent = sessionPhaseNote(scenario);
  scenarioClock.title = preparedVenueSessions?.generated_at
    ? `Venue snapshot generated at ${preparedVenueSessions.generated_at}`
    : `Fixture scenario time ${scenario.clock}`;
  canvasNote.classList.add("selectable");
  canvasNote.dataset.contextType = "market-briefing";
  canvasNote.dataset.contextId = "market_context";
  canvasNote.setAttribute("tabindex", "0");
  canvasNote.setAttribute("role", "button");
  const briefing = macroContextNote();
  if (briefing) {
    canvasNote.innerHTML = `
      <span class="briefing-primary">${briefing.primary}</span>
      <span class="briefing-detail">${briefing.detail}</span>
    `;
  } else {
    canvasNote.innerHTML = `
      <span class="briefing-primary">${scenario.canvasNote}</span>
      <span class="briefing-detail">${scenario.clock.split(" / ")[0]}</span>
    `;
  }
}

function drawScenarioControls() {
  scenarioPanel.innerHTML = scenarioNames().map((name) => `
    <button class="scenario-button" type="button" data-scenario-name="${name}" aria-current="${name === currentScenarioName ? "true" : "false"}">
      ${scenarios[name].title}
    </button>
  `).join("");
}

function resizeCanvas(canvas, context) {
  const scale = Math.min(window.devicePixelRatio || 1, 2);
  canvas.width = Math.floor(canvas.clientWidth * scale);
  canvas.height = Math.floor(canvas.clientHeight * scale);
  context.setTransform(scale, 0, 0, scale, 0, 0);
}

function resizeCanvases() {
  resizeCanvas(atmosphere, atmosphereContext);
  resizeCanvas(riverMaterial, riverContext);
}

function marketCanvasForScenario(scenario) {
  if (!preparedMarketCanvas?.dimensions) return scenario.marketCanvas;
  const neutral = {
    coherence: 0.22,
    spread: 0.18,
    magnitude: 0.12,
    edge: 0.28,
  };
  const output = { ...neutral };
  for (const key of ["coherence", "spread", "magnitude", "edge"]) {
    const dimension = preparedMarketCanvas.dimensions[key];
    if (dimension?.status === "VALID" && typeof dimension.value === "number") {
      output[key] = Math.max(0, Math.min(1, dimension.value));
    }
  }
  return output;
}

function drawAtmosphere(scenario) {
  const width = atmosphere.clientWidth;
  const height = atmosphere.clientHeight;
  atmosphereContext.clearRect(0, 0, width, height);
  const { coherence, spread, magnitude, edge } = marketCanvasForScenario(scenario);
  const count = Math.round(18 + spread * 46 + magnitude * 22);
  const blurRadius = 18 + spread * 44 - edge * 24;
  const grainLength = 18 + coherence * 88;
  const grainWidth = 8 + (1 - edge) * 30;
  const alpha = 0.012 + magnitude * 0.055;
  const dominantAngle = -0.28;

  const wash = atmosphereContext.createLinearGradient(0, 0, width, height);
  wash.addColorStop(0, `rgba(211, 188, 146, ${0.025 + magnitude * 0.03})`);
  wash.addColorStop(0.55, `rgba(107, 159, 170, ${0.02 + magnitude * 0.035})`);
  wash.addColorStop(1, "rgba(18, 34, 45, 0.01)");
  atmosphereContext.fillStyle = wash;
  atmosphereContext.fillRect(0, 0, width, height);

  for (let index = 0; index < count; index += 1) {
    const seed = index * 43.13;
    const clusteredX = width * (0.36 + Math.sin(seed) * 0.14);
    const clusteredY = height * (0.48 + Math.cos(seed * 0.7) * 0.18);
    const broadX = (index * 89) % Math.max(width, 1);
    const broadY = (index * 53 + Math.sin(seed) * 30) % Math.max(height, 1);
    const x = clusteredX * (1 - spread) + broadX * spread;
    const y = clusteredY * (1 - spread) + broadY * spread;
    const localAngle = dominantAngle * coherence + Math.sin(seed) * (1 - coherence) * Math.PI;
    const gradient = atmosphereContext.createRadialGradient(x, y, 0, x, y, Math.max(10, blurRadius));
    gradient.addColorStop(0, `rgba(180, 220, 210, ${alpha})`);
    gradient.addColorStop(0.58, `rgba(216, 183, 132, ${alpha * 0.42})`);
    gradient.addColorStop(1, "rgba(0,0,0,0)");
    atmosphereContext.save();
    atmosphereContext.translate(x, y);
    atmosphereContext.rotate(localAngle);
    atmosphereContext.scale(grainLength / Math.max(1, blurRadius), grainWidth / Math.max(1, blurRadius));
    atmosphereContext.fillStyle = gradient;
    atmosphereContext.beginPath();
    atmosphereContext.arc(0, 0, Math.max(10, blurRadius), 0, Math.PI * 2);
    atmosphereContext.fill();
    atmosphereContext.restore();
  }
}

function exposureBands(scenario) {
  const age = Math.max(0, scenario.exposureAgeSeconds);
  const settled = Math.min(age / 900, 1);
  const bloom = Math.max(1 - age / 120, 0);
  const spread = 1 + settled * 0.6 + bloom * 0.3;
  const alphaShift = 0.16 + settled * 0.12 + bloom * 0.2;
  const map = {
    FLAT: [],
    ONE_NEW_LONG: [{ kind: "long-new", x: 0.33, width: 0.1 * spread, alpha: alphaShift }],
    ONE_SETTLED_LONG: [{ kind: "long-settled", x: 0.43, width: 0.16 * spread, alpha: 0.3 }],
    ONE_NEW_SHORT: [{ kind: "short-new", x: 0.34, width: 0.1 * spread, alpha: alphaShift }],
    MULTIPLE_MIXED_POSITIONS: [
      { kind: "long-settled", x: 0.26, width: 0.13 * spread, alpha: 0.24 },
      { kind: "short-settled", x: 0.48, width: 0.12 * spread, alpha: 0.26 },
      { kind: "long-new", x: 0.66, width: 0.08 * spread, alpha: 0.3 },
    ],
    BUILDING_SAME_DIRECTION_EXPOSURE: [
      { kind: "long-settled", x: 0.26, width: 0.13 * spread, alpha: 0.22 },
      { kind: "long-new", x: 0.42, width: 0.12 * spread, alpha: 0.32 },
      { kind: "long-new", x: 0.58, width: 0.1 * spread, alpha: 0.36 },
    ],
  };
  return map[scenario.exposure] || [];
}

function scenarioWithPreparedExposure(scenario) {
  if (!preparedExposure) return scenario;
  return {
    ...scenario,
    exposure: preparedExposure.visual?.exposure_state || "UNKNOWN",
    exposureAgeSeconds: preparedExposure.visual?.exposure_age_seconds || 0,
  };
}

function drawSediment(scenario, width, centerY, riverHeight) {
  if (scenario.staleAgeSeconds <= 0) return;
  const sediment = Math.min(scenario.staleAgeSeconds / 1200, 1);
  const affectedIndex = Math.max(0, scenario.flow.findIndex((stage) => stage.texture === "STALE_DATA"));
  const x = width * ((affectedIndex + 0.5) / scenario.flow.length);
  riverContext.fillStyle = `rgba(137, 97, 62, ${0.1 + sediment * 0.22})`;
  for (let index = 0; index < 14 + sediment * 18; index += 1) {
    const offset = (index - 10) * 4.2;
    riverContext.beginPath();
    riverContext.ellipse(x + offset, centerY + riverHeight * (0.2 + sediment * 0.2), 2 + sediment * 4, 0.8 + sediment * 2.2, 0, 0, Math.PI * 2);
    riverContext.fill();
  }
}

function drawRiver(scenario) {
  const exposureScenario = scenarioWithPreparedExposure(scenario);
  const width = riverMaterial.clientWidth;
  const height = riverMaterial.clientHeight;
  riverContext.clearRect(0, 0, width, height);

  const centerY = height * 0.66;
  const riverHeight = height * 0.055;
  const velocity = Math.max(0, Math.min(scenario.dataVelocity, 1));
  const base = riverContext.createLinearGradient(0, centerY - riverHeight, width, centerY + riverHeight);
  base.addColorStop(0, "rgba(178, 225, 218, 0.038)");
  base.addColorStop(0.36, `rgba(112, 194, 184, ${0.19 + velocity * 0.11})`);
  base.addColorStop(0.66, `rgba(238, 207, 141, ${0.09 + velocity * 0.085})`);
  base.addColorStop(1, "rgba(20, 31, 39, 0.052)");
  riverContext.fillStyle = base;
  riverContext.beginPath();
  for (let x = 0; x <= width; x += 16) {
    const wave = Math.sin(x * 0.018) * (3 + velocity * 7) + Math.sin(x * 0.041) * (1 + velocity * 3);
    const y = centerY - riverHeight * 0.46 + wave;
    if (x === 0) riverContext.moveTo(x, y);
    else riverContext.lineTo(x, y);
  }
  for (let x = width; x >= 0; x -= 16) {
    const wave = Math.sin(x * 0.015) * (4 + velocity * 6) + Math.cos(x * 0.035) * (1 + velocity * 3);
    riverContext.lineTo(x, centerY + riverHeight * 0.58 + wave);
  }
  riverContext.closePath();
  riverContext.fill();
  riverContext.strokeStyle = `rgba(190, 229, 219, ${0.056 + velocity * 0.046})`;
  riverContext.lineWidth = 1.35;
  riverContext.stroke();

  for (const band of exposureBands(exposureScenario)) {
    const x = width * band.x;
    const bandWidth = width * band.width;
    const gradient = riverContext.createRadialGradient(x, centerY, 0, x, centerY, bandWidth);
    const warm = band.kind.includes("long");
    gradient.addColorStop(0, warm ? `rgba(238, 190, 119, ${band.alpha})` : `rgba(112, 173, 204, ${band.alpha})`);
    gradient.addColorStop(0.5, warm ? "rgba(218, 159, 100, 0.12)" : "rgba(96, 132, 175, 0.12)");
    gradient.addColorStop(1, "rgba(0,0,0,0)");
    riverContext.fillStyle = gradient;
    riverContext.beginPath();
    riverContext.ellipse(x, centerY, bandWidth, riverHeight * 0.36, warm ? -0.08 : 0.08, 0, Math.PI * 2);
    riverContext.fill();
  }
  drawSediment(scenario, width, centerY, riverHeight);
}

function initialContextFromUrl() {
  const raw = new URLSearchParams(window.location.search).get("context");
  if (!raw) return null;
  const [type, ...idParts] = raw.split(":");
  const id = idParts.join(":");
  if (!type || !id) return null;
  return { type, id };
}

function syncContextPanel() {
  if (selectedContext && !contextPanel.hidden) {
    contextContent.innerHTML = renderContext(selectedContext);
    return;
  }
  if (!initialContextApplied) {
    const initialContext = initialContextFromUrl();
    initialContextApplied = true;
    if (initialContext) openContext(initialContext);
  }
}

function renderScenario() {
  if (initializationInProgress) return;
  const scenario = scenarios[currentScenarioName];
  const solarMinute = currentMinuteKey();
  const venueStates = venueStateById(scenario);
  const canvas = marketCanvasForScenario(scenario);
  const exposureScenario = scenarioWithPreparedExposure(scenario);
  const venueGeometry = venueGeometryInputs();
  const signatures = {
    commentary: stableSignature({
      scenario: currentScenarioName,
      venues: preparedVenueSessions?.generated_at,
      macro: preparedMacroContext?.deterministic_fingerprint || preparedMacroContext?.generated_at,
    }),
    world: "observatory-world-atlas-v13-css-peripheral-limbs",
    solar: solarMinute,
    marketCenters: stableSignature({ venueStates, venueGeometry }),
    labels: stableSignature({ venueGeometry, layout: "bounded-collision-v1" }),
    tape: stableSignature({
      scenario: currentScenarioName,
      tape: preparedMarketTape?.generated_at || preparedMarketTape?.rows || scenario.tape,
    }),
    flow: stableSignature({
      scenario: currentScenarioName,
      pipeline: preparedSystemFlow?.generated_at || preparedSystemFlow?.stages,
      health: preparedOperationalHealth?.deterministic_fingerprint || preparedOperationalHealth?.generated_at,
      macro: preparedMacroContext?.deterministic_fingerprint || preparedMacroContext?.generated_at,
    }),
    atmosphere: stableSignature({ scenario: currentScenarioName, canvas }),
    river: stableSignature({
      scenario: currentScenarioName,
      exposure: exposureScenario.exposure,
      exposureAgeSeconds: Math.floor((exposureScenario.exposureAgeSeconds || 0) / 30) * 30,
      velocity: scenario.dataVelocity,
      staleAgeSeconds: scenario.staleAgeSeconds,
    }),
    controls: stableSignature({ names: scenarioNames(), currentScenarioName }),
  };
  if (renderSignatures.commentary !== signatures.commentary) {
    measureRender("commentary", () => updateCommentary(scenario));
    renderSignatures.commentary = signatures.commentary;
  }
  if (renderSignatures.world !== signatures.world) {
    measureRender("world", drawWorldBase);
    renderSignatures.world = signatures.world;
    markInitializationStage("WORLD_BASE_READY");
  }
  if (renderSignatures.solar !== signatures.solar) {
    measureRender("solar", drawSolarLayer);
    renderSignatures.solar = signatures.solar;
    currentSolarMinuteKey = solarMinute;
    markInitializationStage("SOLAR_READY");
  }
  if (renderSignatures.marketCenters !== signatures.marketCenters) {
    measureRender("marketCenters", () => drawMarketCenterLayer(scenario));
    renderSignatures.marketCenters = signatures.marketCenters;
    markInitializationStage("VENUES_READY");
  }
  if (renderSignatures.labels !== signatures.labels) {
    measureRender("labels", () => drawMarketCenterLabels(scenario));
    renderSignatures.labels = signatures.labels;
    markInitializationStage("LABELS_READY");
  }
  if (renderSignatures.flow !== signatures.flow) {
    measureRender("flow", () => drawFlow(scenario));
    renderSignatures.flow = signatures.flow;
  }
  if (renderSignatures.atmosphere !== signatures.atmosphere) {
    measureRender("atmosphere", () => drawAtmosphere(scenario));
    renderSignatures.atmosphere = signatures.atmosphere;
  }
  if (renderSignatures.river !== signatures.river) {
    measureRender("river", () => drawRiver(scenario));
    renderSignatures.river = signatures.river;
    markInitializationStage("LOWER_SYSTEM_READY");
  }
  if (renderSignatures.tape !== signatures.tape) {
    measureRender("tape", () => drawTape(scenario));
    renderSignatures.tape = signatures.tape;
    markInitializationStage("TAPE_READY");
  }
  if (renderSignatures.controls !== signatures.controls) {
    measureRender("controls", () => drawScenarioControls());
    renderSignatures.controls = signatures.controls;
  }
  syncContextPanel();
}

function applyScenario(name) {
  currentScenarioName = scenarios[name] ? name : "ATLANTIC_BRIDGE_MIDDAY";
  const scenario = scenarios[currentScenarioName];
  app.dataset.scenario = currentScenarioName;
  app.dataset.marketCharacter = scenario.marketCharacter;
  app.dataset.atmosphericFlow = scenario.atmosphericFlow;
  app.dataset.timeState = scenario.timeState;
  app.dataset.tapeIntensity = scenario.tapeIntensity;
  app.dataset.exposure = scenario.exposure;
  app.dataset.fixtureEvent = scenario.fixtureEvent;
  const canvas = marketCanvasForScenario(scenario);
  app.style.setProperty("--canvas-coherence", String(canvas.coherence));
  app.style.setProperty("--canvas-spread", String(canvas.spread));
  app.style.setProperty("--canvas-magnitude", String(canvas.magnitude));
  app.style.setProperty("--canvas-edge", String(canvas.edge));
  app.style.setProperty("--canvas-skew", `${(canvas.coherence - 0.5) * -14}deg`);
  app.style.setProperty("--canvas-rotation", `${(1 - canvas.coherence) * -5}deg`);
  app.style.setProperty("--canvas-spread-scale-a", String(0.96 + canvas.spread * 0.1));
  app.style.setProperty("--canvas-spread-scale-b", String(0.98 + canvas.spread * 0.08));
  for (const key of Object.keys(renderSignatures)) renderSignatures[key] = null;
  renderScenario();
  app.classList.remove("fixture-event");
  if (eventTimer) window.clearTimeout(eventTimer);
  if (scenario.fixtureEvent !== "NONE") {
    window.requestAnimationFrame(() => app.classList.add("fixture-event"));
    eventTimer = window.setTimeout(() => app.classList.remove("fixture-event"), 5200);
  }
}

async function loadPreparedMarketTapeIfRequested() {
  const params = new URLSearchParams(window.location.search);
  if (params.get("marketTape") !== "phase1") return;
  try {
    const module = await import(`./market_tape_snapshot.generated.mjs?generated=${Date.now()}`);
    const snapshot = module.marketTapeSnapshot;
    if (!snapshot || snapshot.schema_version !== "observatory_market_tape_snapshot_v1") {
      throw new Error("prepared market tape snapshot has an unsupported schema");
    }
    preparedMarketTape = snapshot;
    app.dataset.marketTapeSource = snapshot.model_status || "UNKNOWN";
    renderScenario();
  } catch (error) {
    preparedMarketTape = null;
    app.dataset.marketTapeSource = "MISSING";
  }
}

async function loadPreparedSystemFlowIfRequested() {
  const params = new URLSearchParams(window.location.search);
  if (params.get("systemPipeline") !== "prepared") return;
  try {
    const module = await import(`./system_pipeline_snapshot.generated.mjs?generated=${Date.now()}`);
    const snapshot = module.systemPipelineSnapshot;
    if (!snapshot || snapshot.schema_version !== "observatory_system_pipeline_snapshot_v1") {
      throw new Error("prepared system pipeline snapshot has an unsupported schema");
    }
    preparedSystemFlow = snapshot;
    app.dataset.systemPipelineSource = snapshot.model_status || "UNKNOWN";
    renderScenario();
  } catch (error) {
    preparedSystemFlow = null;
    app.dataset.systemPipelineSource = "MISSING";
  }
}

async function loadPreparedExposureIfRequested() {
  const params = new URLSearchParams(window.location.search);
  if (params.get("exposure") !== "prepared") return;
  try {
    const module = await import(`./exposure_snapshot.generated.mjs?generated=${Date.now()}`);
    const snapshot = module.exposureSnapshot;
    if (!snapshot || snapshot.schema_version !== "observatory_exposure_snapshot_v1") {
      throw new Error("prepared exposure snapshot has an unsupported schema");
    }
    preparedExposure = snapshot;
    app.dataset.exposureSource = snapshot.model_status || "UNKNOWN";
    app.dataset.exposureDisplayClass = snapshot.visual?.display_class || "UNKNOWN";
    renderScenario();
  } catch (error) {
    preparedExposure = {
      schema_version: "observatory_exposure_snapshot_v1",
      model_status: "MISSING",
      generated_at: null,
      aggregate: { position_count: null },
      visual: {
        exposure_state: "UNKNOWN",
        display_class: "UNKNOWN",
        exposure_age_seconds: 0,
      },
    };
    app.dataset.exposureSource = "MISSING";
    app.dataset.exposureDisplayClass = "UNKNOWN";
    renderScenario();
  }
}

async function loadPreparedVenueSessionsIfRequested() {
  const params = new URLSearchParams(window.location.search);
  if (params.get("venues") !== "prepared") return;
  try {
    const module = await import(`./venue_session_snapshot.generated.mjs?generated=${Date.now()}`);
    const snapshot = module.venueSessionSnapshot;
    if (!snapshot || snapshot.schema_version !== "observatory_global_venue_session_v1") {
      throw new Error("prepared venue/session snapshot has an unsupported schema");
    }
    preparedVenueSessions = snapshot;
    app.dataset.venueSessionSource = snapshot.model_status || "UNKNOWN";
    renderScenario();
  } catch (error) {
    preparedVenueSessions = {
      schema_version: "observatory_global_venue_session_v1",
      model_status: "MISSING",
      venues: venues.map((venue) => ({ venue_id: venue.id, session_status: "UNKNOWN" })),
    };
    app.dataset.venueSessionSource = "MISSING";
    renderScenario();
  }
}

async function loadPreparedMarketCanvasIfRequested() {
  const params = new URLSearchParams(window.location.search);
  if (params.get("marketCanvas") !== "prepared") return;
  try {
    const module = await import(`./market_canvas_snapshot.generated.mjs?generated=${Date.now()}`);
    const snapshot = module.marketCanvasSnapshot;
    if (!snapshot || snapshot.schema_version !== "observatory_market_canvas_v1") {
      throw new Error("prepared Market Canvas snapshot has an unsupported schema");
    }
    preparedMarketCanvas = snapshot;
    app.dataset.marketCanvasSource = snapshot.status || "UNKNOWN";
    renderScenario();
  } catch (error) {
    preparedMarketCanvas = {
      schema_version: "observatory_market_canvas_v1",
      status: "MISSING",
      dimensions: {},
    };
    app.dataset.marketCanvasSource = "MISSING";
    renderScenario();
  }
}

async function loadPreparedMacroContextIfRequested() {
  const params = new URLSearchParams(window.location.search);
  if (params.get("macro") !== "prepared") return;
  try {
    const module = await import(`./macro_market_context_snapshot.generated.mjs?generated=${Date.now()}`);
    const snapshot = module.macroMarketContextSnapshot;
    if (!snapshot || snapshot.schema_version !== "observatory_macro_market_context_v1") {
      throw new Error("prepared macro context snapshot has an unsupported schema");
    }
    preparedMacroContext = snapshot;
    app.dataset.macroContextSource = snapshot.status || "UNKNOWN";
    renderScenario();
  } catch (error) {
    preparedMacroContext = null;
    app.dataset.macroContextSource = "MISSING";
    renderScenario();
  }
}

async function loadPreparedOperationalHealthIfRequested() {
  const params = new URLSearchParams(window.location.search);
  if (params.get("health") !== "prepared") return;
  try {
    const module = await import(`./operational_health_snapshot.generated.mjs?generated=${Date.now()}`);
    const snapshot = module.operationalHealthSnapshot;
    if (!snapshot || snapshot.schema_version !== "observatory_operational_health_v1") {
      throw new Error("prepared health snapshot has an unsupported schema");
    }
    preparedOperationalHealth = snapshot;
    app.dataset.operationalHealthSource = snapshot.status || "UNKNOWN";
    renderScenario();
  } catch (error) {
    preparedOperationalHealth = null;
    app.dataset.operationalHealthSource = "MISSING";
    renderScenario();
  }
}

async function loadPreparedRuntimeBrokerContextIfRequested() {
  const params = new URLSearchParams(window.location.search);
  if (params.get("runtimeBroker") !== "prepared") return;
  try {
    const module = await import(`./runtime_broker_context_snapshot.generated.mjs?generated=${Date.now()}`);
    const snapshot = module.runtimeBrokerContextSnapshot;
    if (!snapshot || snapshot.schema_version !== "observatory_runtime_broker_context_v1") {
      throw new Error("prepared runtime/broker context snapshot has an unsupported schema");
    }
    preparedRuntimeBrokerContext = snapshot;
    app.dataset.runtimeBrokerContextSource = snapshot.status || "UNKNOWN";
    renderScenario();
  } catch (error) {
    preparedRuntimeBrokerContext = null;
    app.dataset.runtimeBrokerContextSource = "MISSING";
    renderScenario();
  }
}

async function loadPreparedFrameManifestIfRequested() {
  const params = new URLSearchParams(window.location.search);
  const preparedRequested = [
    params.get("marketTape") === "phase1",
    params.get("systemPipeline") === "prepared",
    params.get("exposure") === "prepared",
    params.get("venues") === "prepared",
    params.get("marketCanvas") === "prepared",
    params.get("macro") === "prepared",
    params.get("health") === "prepared",
    params.get("runtimeBroker") === "prepared",
  ].some(Boolean);
  if (!preparedRequested) return;
  try {
    const module = await import(`./observatory_frame_manifest.generated.mjs?generated=${Date.now()}`);
    const snapshot = module.observatoryFrameManifest;
    if (!snapshot || snapshot.schema_version !== "observatory_frame_manifest_v1") {
      throw new Error("prepared frame manifest has an unsupported schema");
    }
    preparedFrameManifest = snapshot;
    app.dataset.frameFreshness = snapshot.overall_frame_freshness || "UNKNOWN";
  } catch (error) {
    preparedFrameManifest = null;
    app.dataset.frameFreshness = "MISSING";
  }
}

async function refreshPreparedSnapshots() {
  await Promise.allSettled([
    loadPreparedMarketTapeIfRequested(),
    loadPreparedSystemFlowIfRequested(),
    loadPreparedExposureIfRequested(),
    loadPreparedVenueSessionsIfRequested(),
    loadPreparedMarketCanvasIfRequested(),
    loadPreparedMacroContextIfRequested(),
    loadPreparedOperationalHealthIfRequested(),
    loadPreparedRuntimeBrokerContextIfRequested(),
    loadPreparedFrameManifestIfRequested(),
  ]);
  if (selectedContext) {
    contextContent.innerHTML = renderContext(selectedContext);
  }
}

scenarioToggle.addEventListener("click", () => {
  scenarioPanel.hidden = !scenarioPanel.hidden;
});

scenarioPanel.addEventListener("click", (event) => {
  const button = event.target.closest("[data-scenario-name]");
  if (!button) return;
  applyScenario(button.dataset.scenarioName);
});

window.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    closeContext();
    scenarioPanel.hidden = true;
    return;
  }
  if (event.key === "`" || event.key.toLowerCase() === "s") {
    scenarioPanel.hidden = !scenarioPanel.hidden;
  }
});

contextClose.addEventListener("click", (event) => {
  event.stopPropagation();
  closeContext();
});

contextPanel.addEventListener("click", (event) => {
  event.stopPropagation();
});

document.addEventListener("click", (event) => {
  const selectable = event.target.closest("[data-context-type]");
  if (selectable) {
    event.preventDefault();
    event.stopPropagation();
    openContext({ type: selectable.dataset.contextType, id: selectable.dataset.contextId });
    return;
  }
  if (!event.target.closest("#scenario-panel") && !event.target.closest("#scenario-toggle")) {
    closeContext();
  }
});

document.addEventListener("keydown", (event) => {
  if (!["Enter", " "].includes(event.key)) return;
  const selectable = event.target.closest("[data-context-type]");
  if (!selectable) return;
  event.preventDefault();
  openContext({ type: selectable.dataset.contextType, id: selectable.dataset.contextId });
});

window.addEventListener("resize", () => {
  resizeCanvases();
  renderScenario();
});

document.addEventListener("visibilitychange", () => {
  if (!document.hidden) renderScenario();
});

solarTimer = window.setInterval(() => {
  if (!document.hidden) renderScenario();
}, 60000);

preparedRefreshTimer = window.setInterval(() => {
  if (!document.hidden) refreshPreparedSnapshots();
}, 15000);

async function initializeObservatory() {
  await diagnoseInitializationAwait("refreshPreparedSnapshots", refreshPreparedSnapshots());
  markInitializationStage("PREPARED_SNAPSHOTS_READY");
  resizeCanvases();
  initializationInProgress = false;
  applyScenario(currentScenarioName);
  markInitializationStage("INTERACTIONS_READY");
  markInitializationStage("OBSERVATORY_READY", {
    protocol: window.location.protocol,
    user_agent: navigator.userAgent,
  });
}

void initializeObservatory();
