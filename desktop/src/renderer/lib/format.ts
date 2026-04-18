const NY_TIMESTAMP_FORMATTER = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York",
  year: "numeric",
  month: "numeric",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
  second: "2-digit",
});

export function formatValue(value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "Unavailable";
  }
  if (typeof value === "boolean") {
    return value ? "Yes" : "No";
  }
  if (Array.isArray(value)) {
    return value.length ? value.join(", ") : "None";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

export function formatTimestamp(value: unknown): string {
  if (!value || typeof value !== "string") {
    return "Unavailable";
  }
  const parsed = Date.parse(value);
  if (Number.isNaN(parsed)) {
    return value;
  }
  return `${NY_TIMESTAMP_FORMATTER.format(new Date(parsed))} ET`;
}

export function formatRelativeAge(value: unknown): string {
  if (!value || typeof value !== "string") {
    return "Unavailable";
  }
  const parsed = Date.parse(value);
  if (Number.isNaN(parsed)) {
    return value;
  }
  const deltaMs = Date.now() - parsed;
  const absoluteSeconds = Math.max(0, Math.round(deltaMs / 1000));
  if (absoluteSeconds < 60) {
    return `${absoluteSeconds}s ago`;
  }
  const minutes = Math.round(absoluteSeconds / 60);
  if (minutes < 60) {
    return `${minutes}m ago`;
  }
  const hours = Math.round(minutes / 60);
  if (hours < 48) {
    return `${hours}h ago`;
  }
  const days = Math.round(hours / 24);
  return `${days}d ago`;
}

export function formatShortNumber(value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "Unavailable";
  }
  const numeric = Number(value);
  if (Number.isNaN(numeric)) {
    return String(value);
  }
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits: Math.abs(numeric) >= 1000 ? 0 : 2,
  }).format(numeric);
}

export function formatMaybePnL(value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "Unavailable";
  }
  const numeric = Number(value);
  if (Number.isNaN(numeric)) {
    return String(value);
  }
  return `${numeric > 0 ? "+" : ""}${numeric.toFixed(1)}`;
}

export function sentenceCase(value: unknown): string {
  const text = formatValue(value);
  return text
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function asArray<T>(value: unknown): T[] {
  return Array.isArray(value) ? (value as T[]) : [];
}

export function asRecord(value: unknown): Record<string, any> {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, any>) : {};
}
