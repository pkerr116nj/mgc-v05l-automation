import { scenarioNames, scenarios, venues } from "./fixtures.mjs";

const app = document.querySelector("#app");
const arc = document.querySelector("#venue-arc");
const tape = document.querySelector("#market-tape");
const flow = document.querySelector("#system-flow");
const scenarioPanel = document.querySelector("#scenario-panel");
const scenarioToggle = document.querySelector("#scenario-toggle");
const scenarioClock = document.querySelector("#scenario-clock");
const canvasNote = document.querySelector("#canvas-note");
const atmosphere = document.querySelector("#atmosphere");
const riverMaterial = document.querySelector("#river-material");
const atmosphereContext = atmosphere.getContext("2d", { alpha: true });
const riverContext = riverMaterial.getContext("2d", { alpha: true });

let currentScenarioName = new URLSearchParams(window.location.search).get("scenario") || "ATLANTIC_BRIDGE_MIDDAY";
let eventTimer = null;

function stateColor(state) {
  return {
    PREOPEN: "#8aaed1",
    AUCTION: "#f4c87b",
    OPEN: "#f8d68a",
    CLOSING_SOON: "#d7aa72",
    CLOSED: "#42535d",
    LUNCH_BREAK: "#7296a2",
    HOLIDAY: "#515b64",
    UNKNOWN: "#9da0ad",
    STALE: "#a88976",
  }[state] || "#8b91a4";
}

function stateRadius(state) {
  return {
    OPEN: 1.55,
    AUCTION: 1.35,
    CLOSING_SOON: 1.2,
    PREOPEN: 1,
    LUNCH_BREAK: 0.86,
    STALE: 0.82,
    UNKNOWN: 0.76,
    HOLIDAY: 0.6,
    CLOSED: 0.46,
  }[state] || 0.7;
}

function timeTerminatorPath(timeState) {
  return {
    "0300_ET": "M 2 18 C 28 38, 58 42, 98 24 L 98 96 L 2 96 Z",
    "0800_ET": "M 4 30 C 30 43, 56 45, 96 35 L 96 96 L 4 96 Z",
    "1100_ET": "M 6 42 C 28 46, 58 46, 94 42 L 94 96 L 6 96 Z",
    "1400_ET": "M 5 50 C 30 45, 60 42, 96 44 L 96 96 L 5 96 Z",
    "1600_ET": "M 5 58 C 31 48, 62 40, 96 42 L 96 96 L 5 96 Z",
    "2000_ET": "M 4 24 C 29 34, 62 40, 96 61 L 96 96 L 4 96 Z",
  }[timeState] || "M 6 42 C 28 46, 58 46, 94 42 L 94 96 L 6 96 Z";
}

function drawVenueArc(scenario) {
  const activeEurope = ["lse", "eurex", "paris", "amsterdam", "six"].some((id) => scenario.venues[id] === "OPEN");
  const activeUs = ["nyse", "nasdaq", "cboe", "cme", "cfe"].some((id) => scenario.venues[id] === "OPEN");
  const bridge = activeEurope && activeUs
    ? `<path class="atlantic-bridge" d="M 24 42 C 33 30, 39 28, 47 39" />`
    : "";
  const world = `
    <defs>
      <radialGradient id="earthGlow" cx="46%" cy="42%" r="64%">
        <stop offset="0%" stop-color="rgba(119,154,171,0.18)" />
        <stop offset="62%" stop-color="rgba(40,69,82,0.12)" />
        <stop offset="100%" stop-color="rgba(4,10,14,0)" />
      </radialGradient>
      <linearGradient id="terminator" x1="0%" x2="100%">
        <stop offset="0%" stop-color="rgba(2,7,11,0.44)" />
        <stop offset="48%" stop-color="rgba(2,7,11,0.03)" />
        <stop offset="100%" stop-color="rgba(246,210,122,0.08)" />
      </linearGradient>
    </defs>
    <ellipse class="earth-wash" cx="53" cy="56" rx="44" ry="31" />
    <path class="continent-impression" d="M 13 43 C 20 36, 27 37, 31 44 C 27 52, 20 55, 15 50 Z" />
    <path class="continent-impression" d="M 42 36 C 51 29, 60 34, 63 43 C 58 49, 47 49, 42 43 Z" />
    <path class="continent-impression" d="M 64 50 C 77 42, 91 49, 88 64 C 78 70, 67 64, 64 50 Z" />
    <path class="terminator" d="${timeTerminatorPath(scenario.timeState)}" />
    ${bridge}
  `;
  const nodes = venues.map((venue) => {
    const state = scenario.venues[venue.id] || "CLOSED";
    const labelVisible = ["OPEN", "AUCTION", "CLOSING_SOON", "UNKNOWN", "STALE"].includes(state);
    return `
      <g class="venue-group" data-region="${venue.region}">
        <circle class="venue-harbor" data-state="${state}" cx="${venue.x}" cy="${venue.y}" r="${stateRadius(state) * 3.6}" fill="${stateColor(state)}" />
        <circle class="venue-node" data-state="${state}" cx="${venue.x}" cy="${venue.y}" r="${stateRadius(state)}" fill="${stateColor(state)}">
          <title>${venue.city} - ${venue.label}: ${state}</title>
        </circle>
        ${labelVisible ? `<text class="venue-label" x="${venue.x + 1.9}" y="${venue.y + 0.75}">${venue.city}</text>` : ""}
      </g>
    `;
  });
  arc.innerHTML = `${world}${nodes.join("")}`;
}

function drawTape(scenario) {
  const rows = [...scenario.tape, ...scenario.tape];
  tape.innerHTML = rows.map((row) => {
    const direction = row.changeAbs.trim().startsWith("-") ? "down" : "up";
    return `
      <span class="tape-item">
        <span class="tape-symbol">${row.symbol}</span>
        <span class="split-flap">${row.last}</span>
        <span class="tape-change" data-direction="${direction}">${row.changeAbs} ${row.changePct}</span>
        <span class="tape-freshness">${row.freshness}</span>
      </span>
    `;
  }).join("");
}

function drawFlow(scenario) {
  flow.innerHTML = scenario.flow.map((stage, index) => {
    const isActiveRecon = stage.texture === "RECONCILING" && scenario.reconciliationState === "ACTIVE";
    const shouldAnimate = isActiveRecon || stage.texture === "PENDING_ORDER" || stage.event;
    return `
      <section class="flow-stage" data-texture="${stage.texture}" data-animate="${shouldAnimate ? "true" : "false"}" data-event="${stage.event ? "true" : "false"}" style="--stage-index: ${index}; --data-velocity: ${scenario.dataVelocity}">
        <div class="flow-lens"></div>
        <div class="flow-label">${stage.label}</div>
      </section>
    `;
  }).join("");
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

function drawAtmosphere(scenario) {
  const width = atmosphere.clientWidth;
  const height = atmosphere.clientHeight;
  atmosphereContext.clearRect(0, 0, width, height);
  const { coherence, spread, magnitude, edge } = scenario.marketCanvas;
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
  const width = riverMaterial.clientWidth;
  const height = riverMaterial.clientHeight;
  riverContext.clearRect(0, 0, width, height);

  const centerY = height * 0.45;
  const riverHeight = height * 0.34;
  const velocity = Math.max(0, Math.min(scenario.dataVelocity, 1));
  const base = riverContext.createLinearGradient(0, centerY - riverHeight, width, centerY + riverHeight);
  base.addColorStop(0, "rgba(178, 225, 218, 0.018)");
  base.addColorStop(0.42, `rgba(112, 194, 184, ${0.12 + velocity * 0.09})`);
  base.addColorStop(0.62, `rgba(238, 207, 141, ${0.05 + velocity * 0.07})`);
  base.addColorStop(1, "rgba(20, 31, 39, 0.02)");
  riverContext.fillStyle = base;
  riverContext.beginPath();
  for (let x = 0; x <= width; x += 16) {
    const wave = Math.sin(x * 0.018) * (3 + velocity * 7) + Math.sin(x * 0.041) * (1 + velocity * 3);
    const y = centerY - riverHeight * 0.36 + wave;
    if (x === 0) riverContext.moveTo(x, y);
    else riverContext.lineTo(x, y);
  }
  for (let x = width; x >= 0; x -= 16) {
    const wave = Math.sin(x * 0.015) * (4 + velocity * 6) + Math.cos(x * 0.035) * (1 + velocity * 3);
    riverContext.lineTo(x, centerY + riverHeight * 0.48 + wave);
  }
  riverContext.closePath();
  riverContext.fill();

  for (const band of exposureBands(scenario)) {
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

function renderScenario() {
  const scenario = scenarios[currentScenarioName];
  drawVenueArc(scenario);
  drawTape(scenario);
  drawFlow(scenario);
  drawAtmosphere(scenario);
  drawRiver(scenario);
  drawScenarioControls();
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
  app.style.setProperty("--canvas-coherence", String(scenario.marketCanvas.coherence));
  app.style.setProperty("--canvas-spread", String(scenario.marketCanvas.spread));
  app.style.setProperty("--canvas-magnitude", String(scenario.marketCanvas.magnitude));
  app.style.setProperty("--canvas-edge", String(scenario.marketCanvas.edge));
  app.style.setProperty("--canvas-skew", `${(scenario.marketCanvas.coherence - 0.5) * -14}deg`);
  app.style.setProperty("--canvas-rotation", `${(1 - scenario.marketCanvas.coherence) * -5}deg`);
  app.style.setProperty("--canvas-spread-scale-a", String(0.96 + scenario.marketCanvas.spread * 0.1));
  app.style.setProperty("--canvas-spread-scale-b", String(0.98 + scenario.marketCanvas.spread * 0.08));
  scenarioClock.textContent = scenario.clock;
  canvasNote.textContent = scenario.canvasNote;
  renderScenario();
  app.classList.remove("fixture-event");
  if (eventTimer) window.clearTimeout(eventTimer);
  if (scenario.fixtureEvent !== "NONE") {
    window.requestAnimationFrame(() => app.classList.add("fixture-event"));
    eventTimer = window.setTimeout(() => app.classList.remove("fixture-event"), 5200);
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
  if (event.key === "`" || event.key.toLowerCase() === "s") {
    scenarioPanel.hidden = !scenarioPanel.hidden;
  }
});

window.addEventListener("resize", () => {
  resizeCanvases();
  renderScenario();
});

document.addEventListener("visibilitychange", () => {
  if (!document.hidden) renderScenario();
});

resizeCanvases();
applyScenario(currentScenarioName);
