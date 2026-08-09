export const FEATURED_NORTH_AMERICA_MARKET_CENTERS = Object.freeze([
  "Chicago",
  "New York",
  "Toronto",
  "Mexico City",
]);

export const MAX_LABEL_DISTANCE = 4.6;

const FEATURED_NORTH_AMERICA = new Set(FEATURED_NORTH_AMERICA_MARKET_CENTERS);

const PREFERRED_SIDES = Object.freeze({
  "New York": "SE",
  Chicago: "NW",
  Toronto: "NE",
  "Mexico City": "SW",
  "Sao Paulo": "SE",
  London: "NW",
  Frankfurt: "NE",
  Paris: "SW",
  Amsterdam: "NE",
  Zurich: "SE",
  Seoul: "NW",
  Tokyo: "NE",
  Osaka: "SE",
  Shanghai: "NW",
  Shenzhen: "SW",
  Taipei: "SE",
  "Hong Kong": "NE",
  Singapore: "SE",
  Mumbai: "SW",
  Sydney: "SW",
  Auckland: "SW",
});

const LOCAL_OFFSETS = Object.freeze({
  NE: { dx: 1.25, dy: -1.05, anchor: "start" },
  NW: { dx: -1.25, dy: -1.05, anchor: "end" },
  SE: { dx: 1.25, dy: 1.55, anchor: "start" },
  SW: { dx: -1.25, dy: 1.55, anchor: "end" },
});

const LEADER_OFFSETS = Object.freeze({
  NE: { dx: 2.75, dy: -2.4, anchor: "start" },
  NW: { dx: -2.75, dy: -2.4, anchor: "end" },
  SE: { dx: 2.75, dy: 2.7, anchor: "start" },
  SW: { dx: -2.75, dy: 2.7, anchor: "end" },
});

function unique(values) {
  return [...new Set(values)];
}

function estimatedTextWidth(text) {
  return Math.max(2.1, String(text).length * 0.51);
}

function candidateBox(city, candidate) {
  const width = estimatedTextWidth(city);
  const left = candidate.anchor === "end"
    ? candidate.x - width
    : candidate.anchor === "middle"
      ? candidate.x - width / 2
      : candidate.x;
  return {
    left,
    right: left + width,
    top: candidate.y - 0.92,
    bottom: candidate.y + 0.24,
  };
}

function boxesOverlap(left, right, margin = 0.24) {
  return !(
    left.right + margin < right.left
    || right.right + margin < left.left
    || left.bottom + margin < right.top
    || right.bottom + margin < left.top
  );
}

function coversOtherAnchor(box, city, anchors) {
  return anchors.some((anchor) => (
    anchor.city !== city
    && anchor.x >= box.left - 0.34
    && anchor.x <= box.right + 0.34
    && anchor.y >= box.top - 0.34
    && anchor.y <= box.bottom + 0.34
  ));
}

function candidateFor(anchor, side, offsets, leader = false) {
  const offset = offsets[side];
  const x = anchor.x + offset.dx;
  const y = anchor.y + offset.dy;
  return {
    city: anchor.city,
    region: anchor.region,
    anchorX: anchor.x,
    anchorY: anchor.y,
    x,
    y,
    anchor: offset.anchor,
    placement: side,
    leader,
    distance: Math.hypot(x - anchor.x, y - anchor.y),
  };
}

export function isPrimaryMarketCenter(city, region) {
  return region !== "North America" || FEATURED_NORTH_AMERICA.has(city);
}

export function layoutCityLabels(cityAnchors, options = {}) {
  const maxDistance = Number.isFinite(options.maxDistance) ? options.maxDistance : MAX_LABEL_DISTANCE;
  const anchors = cityAnchors.filter((anchor) => isPrimaryMarketCenter(anchor.city, anchor.region));
  const placed = [];
  const results = [];

  for (const cityAnchor of anchors) {
    const preferred = PREFERRED_SIDES[cityAnchor.city] || "NE";
    const sideOrder = unique([preferred, "NE", "NW", "SE", "SW"]);
    let selected = null;

    for (const side of sideOrder) {
      const candidate = candidateFor(cityAnchor, side, LOCAL_OFFSETS, false);
      const box = candidateBox(cityAnchor.city, candidate);
      if (!placed.some((other) => boxesOverlap(box, other.box)) && !coversOtherAnchor(box, cityAnchor.city, anchors)) {
        selected = { ...candidate, box };
        break;
      }
    }

    if (!selected) {
      for (const side of sideOrder) {
        const candidate = candidateFor(cityAnchor, side, LEADER_OFFSETS, true);
        const box = candidateBox(cityAnchor.city, candidate);
        if (!placed.some((other) => boxesOverlap(box, other.box)) && !coversOtherAnchor(box, cityAnchor.city, anchors)) {
          selected = { ...candidate, box };
          break;
        }
      }
    }

    if (!selected) {
      const fallback = candidateFor(cityAnchor, preferred, LEADER_OFFSETS, true);
      selected = { ...fallback, box: candidateBox(cityAnchor.city, fallback), collisionFallback: true };
    }

    if (selected.distance > maxDistance) {
      const scale = maxDistance / selected.distance;
      selected.x = selected.anchorX + (selected.x - selected.anchorX) * scale;
      selected.y = selected.anchorY + (selected.y - selected.anchorY) * scale;
      selected.distance = maxDistance;
      selected.box = candidateBox(cityAnchor.city, selected);
      selected.distanceClamped = true;
    }

    placed.push(selected);
    results.push(selected);
  }

  return results;
}

export function labelLayoutDiagnostics(layout) {
  const duplicateCities = layout
    .map((label) => label.city)
    .filter((city, index, cities) => cities.indexOf(city) !== index);
  const overDistance = layout.filter((label) => label.distance > MAX_LABEL_DISTANCE + 1e-9);
  return {
    duplicateCities: [...new Set(duplicateCities)],
    overDistance: overDistance.map((label) => ({ city: label.city, distance: label.distance })),
    maxDistance: layout.reduce((maximum, label) => Math.max(maximum, label.distance), 0),
  };
}
