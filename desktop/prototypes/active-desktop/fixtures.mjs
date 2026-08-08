export const venueStates = Object.freeze([
  "PREOPEN",
  "AUCTION",
  "OPEN",
  "CLOSING_SOON",
  "CLOSED",
  "LUNCH_BREAK",
  "HOLIDAY",
  "UNKNOWN",
  "STALE",
]);

export const timeStates = Object.freeze([
  "0300_ET",
  "0800_ET",
  "1100_ET",
  "1400_ET",
  "1600_ET",
  "2000_ET",
]);

export const exposureStates = Object.freeze([
  "FLAT",
  "ONE_NEW_LONG",
  "ONE_SETTLED_LONG",
  "ONE_NEW_SHORT",
  "MULTIPLE_MIXED_POSITIONS",
  "BUILDING_SAME_DIRECTION_EXPOSURE",
]);

export const riverTextures = Object.freeze([
  "HEALTHY",
  "STALE_DATA",
  "LOW_CONFIDENCE_DATA",
  "RECONCILING",
  "PENDING_ORDER",
  "BROKER_DISCONNECTED",
]);

export const venues = Object.freeze([
  { id: "nyse", city: "New York", label: "NYSE", region: "North America", latitude: 40.7128, longitude: -74.006, x: 27.2, y: 39.4, labelDx: 2.6, labelDy: -4.9 },
  { id: "nasdaq", city: "New York", label: "Nasdaq", region: "North America", latitude: 40.7128, longitude: -74.006, x: 28.2, y: 40.4, labelDx: 3.2, labelDy: -2.1 },
  { id: "cboe", city: "New York", label: "Cboe", region: "North America", latitude: 40.7128, longitude: -74.006, x: 29.2, y: 41.4, labelDx: 3.0, labelDy: 0.6 },
  { id: "cme", city: "Chicago", label: "CME", region: "North America", latitude: 41.8781, longitude: -87.6298, x: 22.6, y: 39.8, labelDx: -9.8, labelDy: -3.5 },
  { id: "cfe", city: "Chicago", label: "CFE", region: "North America", latitude: 41.8781, longitude: -87.6298, x: 21.8, y: 42.2, labelDx: -9.4, labelDy: -0.4 },
  { id: "tsx", city: "Toronto", label: "TSX", region: "North America", latitude: 43.6532, longitude: -79.3832, x: 25.1, y: 35.8, labelDx: -9.2, labelDy: -5.8 },
  { id: "mx", city: "Montreal", label: "MX", region: "North America", latitude: 45.5019, longitude: -73.5674, x: 29.1, y: 35.4, labelDx: 2.7, labelDy: -6.9 },
  { id: "bmv", city: "Mexico City", label: "BMV", region: "North America", latitude: 19.4326, longitude: -99.1332, x: 19.3, y: 52.8, labelDx: -10.4, labelDy: 2.8 },
  { id: "b3", city: "Sao Paulo", label: "B3", region: "South America", latitude: -23.5558, longitude: -46.6396, x: 36.2, y: 73.8, labelDx: 1.8, labelDy: 2.4 },
  { id: "lse", city: "London", label: "LSE / ICE", region: "Europe", latitude: 51.5072, longitude: -0.1276, x: 48.5, y: 36.1, labelDx: -9.6, labelDy: -5.8 },
  { id: "eurex", city: "Frankfurt", label: "Deutsche Borse / Eurex", region: "Europe", latitude: 50.1109, longitude: 8.6821, x: 52.1, y: 38.8, labelDx: 2.9, labelDy: -3.0 },
  { id: "paris", city: "Paris", label: "Euronext Paris", region: "Europe", latitude: 48.8566, longitude: 2.3522, x: 50.3, y: 42.0, labelDx: -9.3, labelDy: 2.1 },
  { id: "amsterdam", city: "Amsterdam", label: "Euronext Amsterdam", region: "Europe", latitude: 52.3676, longitude: 4.9041, x: 51.0, y: 36.3, labelDx: 2.7, labelDy: -6.7 },
  { id: "six", city: "Zurich", label: "SIX", region: "Europe", latitude: 47.3769, longitude: 8.5417, x: 53.1, y: 42.5, labelDx: 2.6, labelDy: 4.0 },
  { id: "jpx", city: "Tokyo", label: "JPX / TSE", region: "Asia-Pacific", latitude: 35.6762, longitude: 139.6503, x: 85.4, y: 45.4, labelDx: 2.6, labelDy: -5.3 },
  { id: "osaka", city: "Osaka", label: "Osaka", region: "Asia-Pacific", latitude: 34.6937, longitude: 135.5023, x: 83.6, y: 48.8, labelDx: 3.0, labelDy: 2.9 },
  { id: "hkex", city: "Hong Kong", label: "HKEX", region: "Asia-Pacific", latitude: 22.3193, longitude: 114.1694, x: 75.6, y: 55.9, labelDx: 2.8, labelDy: 3.8 },
  { id: "sgx", city: "Singapore", label: "SGX", region: "Asia-Pacific", latitude: 1.3521, longitude: 103.8198, x: 73.5, y: 66.3, labelDx: 2.0, labelDy: 3.0 },
  { id: "krx", city: "Seoul", label: "KRX", region: "Asia-Pacific", latitude: 37.5665, longitude: 126.978, x: 80.8, y: 45.3, labelDx: -8.2, labelDy: -4.6 },
  { id: "twse", city: "Taipei", label: "TWSE", region: "Asia-Pacific", latitude: 25.033, longitude: 121.5654, x: 78.6, y: 52.7, labelDx: 3.0, labelDy: -2.2 },
  { id: "sse", city: "Shanghai", label: "Shanghai", region: "Asia-Pacific", latitude: 31.2304, longitude: 121.4737, x: 76.9, y: 49.9, labelDx: -11.4, labelDy: -3.3 },
  { id: "szse", city: "Shenzhen", label: "Shenzhen", region: "Asia-Pacific", latitude: 22.5431, longitude: 114.0579, x: 76.1, y: 54.8, labelDx: -11.3, labelDy: 2.8 },
  { id: "nse", city: "Mumbai", label: "NSE / BSE", region: "Asia-Pacific", latitude: 19.076, longitude: 72.8777, x: 64.5, y: 59.2, labelDx: -8.8, labelDy: 2.6 },
  { id: "asx", city: "Sydney", label: "ASX", region: "Asia-Pacific", latitude: -33.8688, longitude: 151.2093, x: 85.3, y: 80.0, labelDx: -7.2, labelDy: 2.0 },
  { id: "nzx", city: "Auckland", label: "NZX", region: "Asia-Pacific", latitude: -36.8509, longitude: 174.7645, x: 93.0, y: 83.5, labelDx: -10.2, labelDy: 1.8 },
]);

const baseTape = Object.freeze([
  { symbol: "ES", last: "6,412.25", changeAbs: "+7.50", changePct: "+0.12%", freshness: "4s" },
  { symbol: "NQ", last: "23,118.00", changeAbs: "+41.25", changePct: "+0.18%", freshness: "5s" },
  { symbol: "GC", last: "3,982.4", changeAbs: "-4.2", changePct: "-0.11%", freshness: "8s" },
  { symbol: "CL", last: "67.84", changeAbs: "+0.31", changePct: "+0.46%", freshness: "9s" },
  { symbol: "ZN", last: "111'185", changeAbs: "-0'025", changePct: "-0.04%", freshness: "7s" },
  { symbol: "NVDA", last: "184.72", changeAbs: "+1.18", changePct: "+0.64%", freshness: "12s" },
  { symbol: "AAPL", last: "229.40", changeAbs: "-0.24", changePct: "-0.10%", freshness: "15s" },
  { symbol: "MSFT", last: "527.16", changeAbs: "+0.88", changePct: "+0.17%", freshness: "13s" },
  { symbol: "SPY", last: "639.82", changeAbs: "+0.44", changePct: "+0.07%", freshness: "6s" },
  { symbol: "QQQ", last: "571.08", changeAbs: "+1.16", changePct: "+0.20%", freshness: "6s" },
]);

const stages = Object.freeze([
  "Market Data",
  "Regime / Context",
  "Magic Runtime",
  "Broker / TWS",
  "Trade Evidence",
  "CRR / Research",
  "Prospective Validation",
]);

export const marketCanvasStates = Object.freeze({
  DIRECTIONAL_BROAD: { coherence: 0.92, spread: 0.9, magnitude: 0.62, edge: 0.62 },
  DIRECTIONAL_NARROW: { coherence: 0.9, spread: 0.16, magnitude: 0.58, edge: 0.72 },
  ROTATIONAL_BROAD: { coherence: 0.14, spread: 0.86, magnitude: 0.46, edge: 0.34 },
  ROTATIONAL_EXPLOSIVE: { coherence: 0.1, spread: 0.62, magnitude: 0.92, edge: 0.82 },
  QUIET_BROAD: { coherence: 0.28, spread: 0.88, magnitude: 0.16, edge: 0.28 },
  QUIET_NARROW: { coherence: 0.22, spread: 0.14, magnitude: 0.12, edge: 0.26 },
  INITIATIVE_NARROW: { coherence: 0.74, spread: 0.18, magnitude: 0.5, edge: 0.88 },
  EXHAUSTION_AFTER_DIRECTIONAL: { coherence: 0.78, spread: 0.7, magnitude: 0.32, edge: 0.18 },
});

function venueState(overrides, fallback = "CLOSED") {
  return Object.fromEntries(venues.map((venue) => [venue.id, overrides[venue.id] || fallback]));
}

function systemFlow(texture = "HEALTHY", affectedStage = null, eventStage = null) {
  return stages.map((label) => ({
    label,
    texture: label === affectedStage ? texture : "HEALTHY",
    event: label === eventStage,
  }));
}

function busyTape() {
  return baseTape.map((row, index) => ({
    ...row,
    freshness: `${1 + (index % 5)}s`,
  }));
}

function scenario({
  title,
  clock,
  timeState,
  marketCharacter,
  atmosphericFlow,
  marketCanvas = marketCanvasStates.QUIET_BROAD,
  tapeIntensity = "normal",
  exposure = "FLAT",
  exposureAgeSeconds = 0,
  dataVelocity = 0.35,
  staleAgeSeconds = 0,
  reconciliationState = "IDLE",
  fixtureEvent = "NONE",
  canvasNote,
  venues: venueOverrides,
  fallback = "CLOSED",
  flow = systemFlow(),
  tape = baseTape,
}) {
  return {
    title,
    clock,
    timeState,
    marketCharacter,
    atmosphericFlow,
    marketCanvas,
    tapeIntensity,
    exposure,
    exposureAgeSeconds,
    dataVelocity,
    staleAgeSeconds,
    reconciliationState,
    fixtureEvent,
    canvasNote,
    venues: venueState(venueOverrides, fallback),
    flow,
    tape,
  };
}

export const scenarios = Object.freeze({
  QUIET_ASIA_HEALTHY: scenario({
    title: "Quiet Asia Healthy",
    clock: "03:00 ET / Asia carrying the night",
    timeState: "0300_ET",
    marketCharacter: "calm",
    atmosphericFlow: "rotational",
    marketCanvas: marketCanvasStates.QUIET_BROAD,
    tapeIntensity: "quiet",
    canvasNote: "Deep, composed futures tone with Asia-Pacific venues carrying the field.",
    venues: {
      jpx: "OPEN", osaka: "OPEN", krx: "OPEN", twse: "OPEN", hkex: "PREOPEN", sgx: "OPEN",
      sse: "PREOPEN", szse: "PREOPEN", nse: "CLOSED", asx: "OPEN", nzx: "CLOSING_SOON",
    },
  }),
  LONDON_OPEN_HEALTHY: scenario({
    title: "London Open Healthy",
    clock: "08:00 ET / Europe fully awake",
    timeState: "0800_ET",
    marketCharacter: "moderate",
    atmosphericFlow: "directional",
    marketCanvas: marketCanvasStates.INITIATIVE_NARROW,
    canvasNote: "European liquidity wakes into an orderly Atlantic morning.",
    venues: {
      lse: "OPEN", eurex: "OPEN", paris: "OPEN", amsterdam: "OPEN", six: "OPEN",
      jpx: "CLOSED", hkex: "CLOSING_SOON", sgx: "OPEN", asx: "CLOSED",
    },
  }),
  LONDON_US_OVERLAP_ACTIVE: scenario({
    title: "London / US Overlap Active",
    clock: "11:00 ET / Transatlantic overlap",
    timeState: "1100_ET",
    marketCharacter: "directional",
    atmosphericFlow: "directional",
    marketCanvas: marketCanvasStates.DIRECTIONAL_BROAD,
    tapeIntensity: "busy",
    dataVelocity: 0.74,
    canvasNote: "Strongest participation field, with Europe and New York both lit.",
    venues: {
      nyse: "OPEN", nasdaq: "OPEN", cboe: "OPEN", cme: "OPEN", cfe: "OPEN", tsx: "OPEN", mx: "OPEN",
      lse: "OPEN", eurex: "OPEN", paris: "OPEN", amsterdam: "OPEN", six: "OPEN", bmv: "PREOPEN",
    },
    tape: busyTape(),
  }),
  ATLANTIC_BRIDGE_MIDDAY: scenario({
    title: "Atlantic Bridge Midday",
    clock: "11:00 ET / London and New York joined",
    timeState: "1100_ET",
    marketCharacter: "directional",
    atmosphericFlow: "directional",
    marketCanvas: marketCanvasStates.DIRECTIONAL_BROAD,
    tapeIntensity: "busy",
    exposure: "MULTIPLE_MIXED_POSITIONS",
    exposureAgeSeconds: 870,
    dataVelocity: 0.82,
    canvasNote: "Warm Atlantic corridor, broad market participation, and a full, clear system river.",
    venues: {
      nyse: "OPEN", nasdaq: "OPEN", cboe: "OPEN", cme: "OPEN", cfe: "OPEN", tsx: "OPEN", mx: "OPEN",
      lse: "OPEN", eurex: "OPEN", paris: "OPEN", amsterdam: "OPEN", six: "OPEN", b3: "OPEN",
    },
    flow: systemFlow(),
    tape: busyTape(),
  }),
  CALM_MARKET_BROKEN_REGIME_FEED: scenario({
    title: "Calm Market / Broken Regime Feed",
    clock: "08:00 ET / Europe orderly",
    timeState: "0800_ET",
    marketCharacter: "calm",
    atmosphericFlow: "rotational",
    marketCanvas: marketCanvasStates.QUIET_BROAD,
    tapeIntensity: "quiet",
    dataVelocity: 0.18,
    staleAgeSeconds: 780,
    canvasNote: "Market tone remains quiet while one system stage is locally impaired.",
    venues: { lse: "OPEN", eurex: "OPEN", paris: "OPEN", amsterdam: "OPEN", six: "OPEN" },
    flow: systemFlow("STALE_DATA", "Regime / Context"),
  }),
  VOLATILE_MARKET_HEALTHY_MAGIC: scenario({
    title: "Volatile Market / Healthy Magic",
    clock: "11:00 ET / US cash impulse",
    timeState: "1100_ET",
    marketCharacter: "volatile",
    atmosphericFlow: "directional",
    marketCanvas: marketCanvasStates.ROTATIONAL_EXPLOSIVE,
    tapeIntensity: "busy",
    dataVelocity: 0.78,
    canvasNote: "Stormy price action over a coherent, healthy system river.",
    venues: { nyse: "OPEN", nasdaq: "OPEN", cboe: "OPEN", cme: "OPEN", cfe: "OPEN", lse: "OPEN", eurex: "OPEN" },
    tape: busyTape(),
  }),
  BROKER_DISCONNECTED: scenario({
    title: "Broker Disconnected",
    clock: "14:00 ET / US session",
    timeState: "1400_ET",
    marketCharacter: "moderate",
    atmosphericFlow: "directional",
    marketCanvas: marketCanvasStates.DIRECTIONAL_NARROW,
    dataVelocity: 0.42,
    canvasNote: "Upstream context continues, but flow is interrupted at Broker / TWS.",
    venues: { nyse: "OPEN", nasdaq: "OPEN", cme: "OPEN", cfe: "OPEN", tsx: "OPEN", lse: "OPEN" },
    flow: systemFlow("BROKER_DISCONNECTED", "Broker / TWS"),
  }),
  STALE_EXCHANGE_CALENDAR: scenario({
    title: "Stale Exchange Calendar",
    clock: "20:00 ET / Calendar uncertainty",
    timeState: "2000_ET",
    marketCharacter: "calm",
    atmosphericFlow: "rotational",
    marketCanvas: marketCanvasStates.QUIET_NARROW,
    tapeIntensity: "quiet",
    canvasNote: "Calendar uncertainty stays local to the living Earth layer.",
    venues: { cme: "OPEN", cfe: "OPEN", jpx: "UNKNOWN", hkex: "STALE", sgx: "OPEN", asx: "HOLIDAY", nzx: "STALE" },
  }),
  RESEARCH_CHECKPOINT_REACHED: scenario({
    title: "Research Checkpoint Reached",
    clock: "16:00 ET / Research cadence",
    timeState: "1600_ET",
    marketCharacter: "exhaustion",
    atmosphericFlow: "exhaustion",
    marketCanvas: marketCanvasStates.EXHAUSTION_AFTER_DIRECTIONAL,
    fixtureEvent: "CHECKPOINT_REACHED",
    canvasNote: "One slow research ripple appears and fades back into the room.",
    venues: { nyse: "OPEN", nasdaq: "OPEN", cme: "OPEN", cfe: "OPEN", lse: "CLOSING_SOON", tsx: "OPEN" },
    flow: systemFlow("HEALTHY", null, "Prospective Validation"),
  }),
  FLAT_CLEAR_RIVER: scenario({
    title: "Flat Clear River",
    clock: "14:00 ET / Clear operating state",
    timeState: "1400_ET",
    marketCharacter: "calm",
    atmosphericFlow: "rotational",
    marketCanvas: marketCanvasStates.QUIET_NARROW,
    exposure: "FLAT",
    dataVelocity: 0.08,
    canvasNote: "No fixture exposure: the river is exceptionally clear.",
    venues: { nyse: "OPEN", nasdaq: "OPEN", cme: "OPEN", cfe: "OPEN" },
  }),
  ONE_NEW_LONG: scenario({
    title: "One New Long",
    clock: "11:00 ET / New exposure entering",
    timeState: "1100_ET",
    marketCharacter: "moderate",
    atmosphericFlow: "directional",
    marketCanvas: marketCanvasStates.INITIATIVE_NARROW,
    exposure: "ONE_NEW_LONG",
    exposureAgeSeconds: 18,
    fixtureEvent: "EXPOSURE_OPENED",
    canvasNote: "A new long enters like warm ink blooming into fluid.",
    venues: { nyse: "OPEN", nasdaq: "OPEN", cme: "OPEN", lse: "OPEN" },
  }),
  ONE_SETTLED_LONG: scenario({
    title: "One Settled Long",
    clock: "14:00 ET / Integrated exposure",
    timeState: "1400_ET",
    marketCharacter: "moderate",
    atmosphericFlow: "directional",
    marketCanvas: marketCanvasStates.DIRECTIONAL_NARROW,
    exposure: "ONE_SETTLED_LONG",
    exposureAgeSeconds: 1200,
    canvasNote: "A settled long becomes part of the river material.",
    venues: { nyse: "OPEN", nasdaq: "OPEN", cme: "OPEN", cfe: "OPEN" },
  }),
  ONE_NEW_SHORT: scenario({
    title: "One New Short",
    clock: "11:00 ET / Cool exposure entering",
    timeState: "1100_ET",
    marketCharacter: "moderate",
    atmosphericFlow: "directional",
    marketCanvas: marketCanvasStates.INITIATIVE_NARROW,
    exposure: "ONE_NEW_SHORT",
    exposureAgeSeconds: 20,
    fixtureEvent: "EXPOSURE_OPENED",
    canvasNote: "A new short enters as cooler, denser ink folding against the flow.",
    venues: { nyse: "OPEN", nasdaq: "OPEN", cme: "OPEN", lse: "OPEN" },
  }),
  MULTIPLE_MIXED_POSITIONS: scenario({
    title: "Multiple Mixed Positions",
    clock: "11:00 ET / Mixed carried exposure",
    timeState: "1100_ET",
    marketCharacter: "directional",
    atmosphericFlow: "directional",
    marketCanvas: marketCanvasStates.DIRECTIONAL_BROAD,
    tapeIntensity: "busy",
    exposure: "MULTIPLE_MIXED_POSITIONS",
    exposureAgeSeconds: 540,
    dataVelocity: 0.64,
    canvasNote: "Mixed positions add weight without becoming icons.",
    venues: { nyse: "OPEN", nasdaq: "OPEN", cme: "OPEN", cfe: "OPEN", lse: "OPEN", eurex: "OPEN" },
  }),
  BUILDING_SAME_DIRECTION_EXPOSURE: scenario({
    title: "Building Same-Direction Exposure",
    clock: "11:00 ET / Increasing inertia",
    timeState: "1100_ET",
    marketCharacter: "directional",
    atmosphericFlow: "directional",
    marketCanvas: marketCanvasStates.DIRECTIONAL_NARROW,
    exposure: "BUILDING_SAME_DIRECTION_EXPOSURE",
    exposureAgeSeconds: 95,
    dataVelocity: 0.7,
    fixtureEvent: "EXPOSURE_OPENED",
    canvasNote: "Exposure builds as river inertia, not alarm.",
    venues: { nyse: "OPEN", nasdaq: "OPEN", cme: "OPEN", cfe: "OPEN", lse: "OPEN" },
  }),
  STALE_DATA_SEDIMENT: scenario({
    title: "Stale Data Sediment",
    clock: "08:00 ET / Data aging locally",
    timeState: "0800_ET",
    marketCharacter: "calm",
    atmosphericFlow: "rotational",
    marketCanvas: marketCanvasStates.QUIET_NARROW,
    dataVelocity: 0.12,
    staleAgeSeconds: 1240,
    canvasNote: "Stale data appears as sediment settling at the affected stage.",
    venues: { lse: "OPEN", eurex: "OPEN", paris: "OPEN" },
    flow: systemFlow("STALE_DATA", "Market Data"),
  }),
  LOW_CONFIDENCE_HAZE: scenario({
    title: "Low Confidence Haze",
    clock: "14:00 ET / Context unclear",
    timeState: "1400_ET",
    marketCharacter: "moderate",
    atmosphericFlow: "directional",
    marketCanvas: marketCanvasStates.ROTATIONAL_BROAD,
    dataVelocity: 0.31,
    canvasNote: "Low-confidence context clouds the fluid without sediment.",
    venues: { nyse: "OPEN", nasdaq: "OPEN", cme: "OPEN" },
    flow: systemFlow("LOW_CONFIDENCE_DATA", "Regime / Context"),
  }),
  RECONCILIATION_SHIMMER: scenario({
    title: "Reconciliation Shimmer",
    clock: "14:00 ET / Truth settling",
    timeState: "1400_ET",
    marketCharacter: "moderate",
    atmosphericFlow: "directional",
    marketCanvas: marketCanvasStates.DIRECTIONAL_NARROW,
    reconciliationState: "ACTIVE",
    fixtureEvent: "RECONCILIATION_STARTED",
    canvasNote: "Reconciliation appears as a double image that resolves.",
    venues: { nyse: "OPEN", nasdaq: "OPEN", cme: "OPEN" },
    flow: systemFlow("RECONCILING", "Trade Evidence"),
  }),
  PENDING_ORDER_EDDY: scenario({
    title: "Pending Order Eddy",
    clock: "11:00 ET / Local order event",
    timeState: "1100_ET",
    marketCharacter: "moderate",
    atmosphericFlow: "directional",
    marketCanvas: marketCanvasStates.INITIATIVE_NARROW,
    exposure: "ONE_NEW_LONG",
    exposureAgeSeconds: 8,
    fixtureEvent: "ORDER_PENDING",
    canvasNote: "A pending order is a small local eddy, not a dashboard alert.",
    venues: { nyse: "OPEN", nasdaq: "OPEN", cme: "OPEN", cfe: "OPEN", lse: "OPEN" },
    flow: systemFlow("PENDING_ORDER", "Broker / TWS"),
  }),
  SKY_DIRECTIONAL_BROAD: scenario({
    title: "Sky Grammar - Directional Broad",
    clock: "11:00 ET / grammar comparison",
    timeState: "1100_ET",
    marketCharacter: "grammar",
    atmosphericFlow: "parameterized",
    marketCanvas: marketCanvasStates.DIRECTIONAL_BROAD,
    canvasNote: "High coherence with broad pigment coverage.",
    venues: { nyse: "OPEN", nasdaq: "OPEN", cme: "OPEN", lse: "OPEN" },
  }),
  SKY_DIRECTIONAL_NARROW: scenario({
    title: "Sky Grammar - Directional Narrow",
    clock: "11:00 ET / grammar comparison",
    timeState: "1100_ET",
    marketCharacter: "grammar",
    atmosphericFlow: "parameterized",
    marketCanvas: marketCanvasStates.DIRECTIONAL_NARROW,
    canvasNote: "High coherence with localized pigment.",
    venues: { nyse: "OPEN", nasdaq: "OPEN", cme: "OPEN", lse: "OPEN" },
  }),
  SKY_ROTATIONAL_BROAD: scenario({
    title: "Sky Grammar - Rotational Broad",
    clock: "08:00 ET / grammar comparison",
    timeState: "0800_ET",
    marketCharacter: "grammar",
    atmosphericFlow: "parameterized",
    marketCanvas: marketCanvasStates.ROTATIONAL_BROAD,
    canvasNote: "Fragmented grain with broad, even pigment coverage.",
    venues: { lse: "OPEN", eurex: "OPEN", paris: "OPEN" },
  }),
  SKY_ROTATIONAL_EXPLOSIVE: scenario({
    title: "Sky Grammar - Rotational Explosive",
    clock: "11:00 ET / grammar comparison",
    timeState: "1100_ET",
    marketCharacter: "grammar",
    atmosphericFlow: "parameterized",
    marketCanvas: marketCanvasStates.ROTATIONAL_EXPLOSIVE,
    canvasNote: "Fragmented grain with high pigment intensity and sharp edges.",
    venues: { nyse: "OPEN", nasdaq: "OPEN", cme: "OPEN" },
  }),
  SKY_QUIET_BROAD: scenario({
    title: "Sky Grammar - Quiet Broad",
    clock: "03:00 ET / grammar comparison",
    timeState: "0300_ET",
    marketCharacter: "grammar",
    atmosphericFlow: "parameterized",
    marketCanvas: marketCanvasStates.QUIET_BROAD,
    canvasNote: "Low magnitude spread broadly across the sky.",
    venues: { jpx: "OPEN", sgx: "OPEN", asx: "OPEN" },
  }),
  SKY_QUIET_NARROW: scenario({
    title: "Sky Grammar - Quiet Narrow",
    clock: "20:00 ET / grammar comparison",
    timeState: "2000_ET",
    marketCharacter: "grammar",
    atmosphericFlow: "parameterized",
    marketCanvas: marketCanvasStates.QUIET_NARROW,
    canvasNote: "Low magnitude and localized pigment; nearly absent by design.",
    venues: { cme: "OPEN", cfe: "OPEN" },
  }),
  SKY_INITIATIVE_NARROW: scenario({
    title: "Sky Grammar - Initiative Narrow",
    clock: "08:00 ET / grammar comparison",
    timeState: "0800_ET",
    marketCharacter: "grammar",
    atmosphericFlow: "parameterized",
    marketCanvas: marketCanvasStates.INITIATIVE_NARROW,
    canvasNote: "Some coherence, localized pigment, and a sharper edge.",
    venues: { lse: "OPEN", eurex: "OPEN" },
  }),
  SKY_EXHAUSTION_AFTER_DIRECTIONAL: scenario({
    title: "Sky Grammar - Exhaustion After Directional",
    clock: "16:00 ET / grammar comparison",
    timeState: "1600_ET",
    marketCharacter: "grammar",
    atmosphericFlow: "parameterized",
    marketCanvas: marketCanvasStates.EXHAUSTION_AFTER_DIRECTIONAL,
    canvasNote: "Coherence persists while edge quality decays and pigment thins.",
    venues: { nyse: "OPEN", nasdaq: "OPEN", cme: "OPEN" },
  }),
});

export function scenarioNames() {
  return Object.keys(scenarios);
}

export function validateFixtures() {
  const errors = [];
  const requiredScenarios = [
    "ATLANTIC_BRIDGE_MIDDAY",
    "FLAT_CLEAR_RIVER",
    "ONE_NEW_LONG",
    "ONE_SETTLED_LONG",
    "ONE_NEW_SHORT",
    "MULTIPLE_MIXED_POSITIONS",
    "BUILDING_SAME_DIRECTION_EXPOSURE",
    "STALE_DATA_SEDIMENT",
    "LOW_CONFIDENCE_HAZE",
    "RECONCILIATION_SHIMMER",
    "PENDING_ORDER_EDDY",
    "SKY_DIRECTIONAL_BROAD",
    "SKY_DIRECTIONAL_NARROW",
    "SKY_ROTATIONAL_BROAD",
    "SKY_ROTATIONAL_EXPLOSIVE",
    "SKY_QUIET_BROAD",
    "SKY_QUIET_NARROW",
    "SKY_INITIATIVE_NARROW",
    "SKY_EXHAUSTION_AFTER_DIRECTIONAL",
  ];
  for (const name of requiredScenarios) {
    if (!scenarios[name]) errors.push(`missing required scenario: ${name}`);
  }
  for (const state of timeStates) {
    if (!Object.values(scenarios).some((candidate) => candidate.timeState === state)) {
      errors.push(`missing time-of-day fixture: ${state}`);
    }
  }
  for (const exposure of exposureStates) {
    if (!Object.values(scenarios).some((candidate) => candidate.exposure === exposure)) {
      errors.push(`missing exposure fixture: ${exposure}`);
    }
  }
  for (const venue of venues) {
    if (!Number.isFinite(venue.x) || !Number.isFinite(venue.y)) errors.push(`invalid venue coordinate: ${venue.id}`);
  }
  for (const [name, candidate] of Object.entries(scenarios)) {
    if (!timeStates.includes(candidate.timeState)) errors.push(`${name} has invalid time state ${candidate.timeState}`);
    if (!exposureStates.includes(candidate.exposure)) errors.push(`${name} has invalid exposure ${candidate.exposure}`);
    if (!Number.isFinite(candidate.dataVelocity) || candidate.dataVelocity < 0 || candidate.dataVelocity > 1) {
      errors.push(`${name} has invalid data velocity ${candidate.dataVelocity}`);
    }
    if (!Number.isFinite(candidate.staleAgeSeconds) || candidate.staleAgeSeconds < 0) {
      errors.push(`${name} has invalid stale age ${candidate.staleAgeSeconds}`);
    }
    if (!Number.isFinite(candidate.exposureAgeSeconds) || candidate.exposureAgeSeconds < 0) {
      errors.push(`${name} has invalid exposure age ${candidate.exposureAgeSeconds}`);
    }
    for (const [key, value] of Object.entries(candidate.marketCanvas)) {
      if (!["coherence", "spread", "magnitude", "edge"].includes(key)) {
        errors.push(`${name} has unexpected market canvas parameter ${key}`);
      }
      if (!Number.isFinite(value) || value < 0 || value > 1) {
        errors.push(`${name} has invalid market canvas ${key}: ${value}`);
      }
    }
    if (!["IDLE", "ACTIVE", "RESOLVED"].includes(candidate.reconciliationState)) {
      errors.push(`${name} has invalid reconciliation state ${candidate.reconciliationState}`);
    }
    for (const [venueId, state] of Object.entries(candidate.venues)) {
      if (!venues.some((venue) => venue.id === venueId)) errors.push(`${name} references unknown venue ${venueId}`);
      if (!venueStates.includes(state)) errors.push(`${name} has invalid venue state ${state}`);
    }
    if (candidate.flow.length !== stages.length) errors.push(`${name} has incomplete system flow`);
    for (const stage of candidate.flow) {
      if (!riverTextures.includes(stage.texture)) errors.push(`${name} has invalid river texture ${stage.texture}`);
    }
    if (candidate.tape.length < 10) errors.push(`${name} has insufficient tape rows`);
  }
  return { valid: errors.length === 0, errors };
}
