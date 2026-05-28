import assert from "node:assert/strict";
import test from "node:test";

import {
  brokerBackedTradeEvidence,
  deriveNotificationEvents,
  makeTestNotificationEvent,
  normalizeNotificationPolicy,
  notificationPolicyDecision,
  type OperatorNotificationEvent,
  type OperatorNotificationSnapshot,
} from "./operatorNotifications";

function snapshot(overrides: Partial<OperatorNotificationSnapshot> = {}): OperatorNotificationSnapshot {
  return {
    generated_at: "2026-05-28T14:00:00.000Z",
    market_data_connected: true,
    broker_connected: true,
    runtime_active: true,
    readiness_key: "READY",
    readiness_label: "Ready",
    lifecycle_blocked: false,
    lifecycle_block_reason: null,
    duplicate_writer_detected: false,
    stale_authority_truth_artifact: false,
    hard_blocked: false,
    hard_block_reason: null,
    backend_ui_degraded_runtime_active: false,
    backend_ui_degraded_reason: null,
    paper_orders: [],
    paper_fills: [],
    ...overrides,
  };
}

test("classifies major state transitions", () => {
  const events = deriveNotificationEvents({
    previous: snapshot(),
    current: snapshot({
      market_data_connected: false,
      broker_connected: false,
      runtime_active: false,
      readiness_key: "BLOCKED",
      readiness_label: "Runtime blocked by preflight.",
      hard_blocked: true,
      hard_block_reason: "Guardian hard block active.",
    }),
    timestamp: "2026-05-28T14:01:00.000Z",
  });

  assert.deepEqual(
    events.map((event) => event.event_type),
    [
      "market_data_disconnected",
      "broker_disconnected",
      "runtime_down",
      "readiness_changed",
      "guardian_control_safe_state_hard_block",
    ],
  );
  assert.equal(events.find((event) => event.event_type === "guardian_control_safe_state_hard_block")?.severity, "critical");
});

test("dedupes and throttles repeated events", () => {
  const event = makeTestNotificationEvent("2026-05-28T14:01:00.000Z");
  const recent: OperatorNotificationEvent[] = [
    {
      ...event,
      timestamp: "2026-05-28T14:00:30.000Z",
      delivery_status: "delivered",
    },
  ];

  const decision = notificationPolicyDecision(
    event,
    { enabled: true, dedupe_window_seconds: 300, default_throttle_seconds: 60 },
    recent,
    new Date("2026-05-28T14:01:00.000Z"),
  );

  assert.equal(decision.allowed, false);
  assert.equal(decision.status, "suppressed_duplicate");
});

test("suppresses disabled event types", () => {
  const event = makeTestNotificationEvent("2026-05-28T14:01:00.000Z");
  const policy = normalizeNotificationPolicy({
    enabled_event_types: {
      test_notification: false,
    },
  });

  const decision = notificationPolicyDecision(event, policy, [], new Date("2026-05-28T14:01:00.000Z"));

  assert.equal(decision.allowed, false);
  assert.equal(decision.status, "suppressed_policy");
});

test("trade alerts can bypass severity threshold and quiet hours", () => {
  const events = deriveNotificationEvents({
    previous: snapshot(),
    current: snapshot({
      paper_fills: [
        {
          fill_id: "fill-1",
          broker_order_id: "paper-MGC-1",
          order_status: "FILLED",
          intent_type: "SELL_TO_OPEN",
          quantity: 1,
          instrument: "MGC",
          fill_price: "4370.10",
        },
      ],
    }),
    timestamp: "2026-05-28T14:01:00.000Z",
  });
  const fillEvent = events.find((event) => event.event_type === "paper_fill_received");

  assert.ok(fillEvent);
  const decision = notificationPolicyDecision(
    fillEvent,
    {
      severity_threshold: "critical",
      quiet_hours: { enabled: true, start: "00:00", end: "23:59" },
      trade_alerts_always_on: true,
    },
    [],
    new Date("2026-05-28T14:01:00.000Z"),
  );

  assert.equal(decision.allowed, true);
});

test("trade fill notifications require broker-backed evidence", () => {
  assert.equal(
    brokerBackedTradeEvidence({
      fill_id: "local-fill-only",
      order_status: "FILLED",
      intent_type: "BUY_TO_OPEN",
    }),
    false,
  );

  const events = deriveNotificationEvents({
    previous: snapshot(),
    current: snapshot({
      paper_fills: [
        {
          fill_id: "local-fill-only",
          order_status: "FILLED",
          intent_type: "BUY_TO_OPEN",
          quantity: 1,
          instrument: "MGC",
          fill_price: "4370.10",
        },
      ],
    }),
    timestamp: "2026-05-28T14:01:00.000Z",
  });

  assert.equal(events.some((event) => event.event_type === "paper_fill_received"), false);
});
