import assert from "node:assert/strict";
import test from "node:test";

import { buildOperatorTriageContract } from "./operatorTriage";

function baseInput(overrides: Record<string, unknown> = {}) {
  return {
    desktopSourceMode: "attached_snapshot_bridge",
    desktopRefreshedAt: new Date().toISOString(),
    dashboardGeneratedAt: new Date().toISOString(),
    global: {
      mode: "PAPER",
      mode_label: "PAPER",
      live_disabled: true,
      market_data_status: "LIVE",
      market_data_label: "LIVE",
      reconciliation_status: "CLEAN",
      stale: false,
    },
    operatorSurface: { generated_at: new Date().toISOString() },
    runtimeReadiness: {
      runtime_status: "RUNNING",
      paper_enabled: true,
      entries_enabled: true,
      blocking_faults_active: false,
    },
    runtimeValues: { runtime_recovery_state: "RUNNING" },
    paperReadiness: {
      generated_at: new Date().toISOString(),
      runtime_running: true,
      entries_enabled: true,
    },
    portfolio: {},
    productionLinkEnabled: true,
    productionLink: {},
    productionHealth: {
      broker_reachable: { ok: true, detail: "reachable" },
      auth_healthy: { ok: true, detail: "healthy" },
      account_selected: { ok: true, detail: "selected" },
      positions_fresh: { ok: true, detail: "fresh" },
      quotes_fresh: { ok: true, detail: "fresh" },
    },
    productionReconciliation: { blocked: false, mismatch_count: 0, detail: "clear" },
    productionDiagnostics: {},
    productionBalances: {},
    localOperatorAuth: {},
    operatorActiveAlertRows: [],
    operatorRecentAlertRows: [],
    sameUnderlyingConflictSummary: {},
    ...overrides,
  };
}

test("Track B PAPER ready status overrides legacy stale market-data wording", () => {
  const contract = buildOperatorTriageContract(baseInput({
    global: {
      mode: "PAPER",
      mode_label: "PAPER",
      live_disabled: true,
      market_data_status: "STALE",
      market_data_label: "STALE",
      reconciliation_status: "CLEAN",
      stale: true,
    },
    runtimeValues: {
      runtime_recovery_state: "RUNNING",
      paper_trade_allowed: false,
      paper_trade_block_reason: "paper_market_data_stale_or_unavailable",
    },
    paperReadiness: {
      generated_at: new Date().toISOString(),
      runtime_running: true,
      entries_enabled: true,
      paper_trade_allowed: false,
      paper_trade_block_reason: "paper_market_data_stale_or_unavailable",
    },
    trackBPaperTrading: {
      available: true,
      review_required_count: 0,
      startup_readiness_diagnostic: {
        instruments: {
          MGC: { instrument: "MGC", context_ready: true, live_execution_approved: true, paper_evaluation_allowed: true },
          MNQ: { instrument: "MNQ", context_ready: true, live_execution_approved: true, paper_evaluation_allowed: true },
        },
      },
      zero_activity_diagnostic: {
        diagnosis_classification: "NORMAL_NO_SIGNAL",
        signals_seen: 0,
        recent_cycles_evaluated: 4,
        strategies_evaluated: 10,
        completed_decision_bar_audit: { classification: "EVALUATING_EACH_COMPLETED_BAR" },
      },
    },
  }));

  const marketDataGate = contract.operator_triage.hard_gates.find((gate) => gate.key === "market-data");
  assert.equal(contract.operator_triage.paper_trade_allowed, true);
  assert.equal(contract.operator_triage.paper_trade_block_reason, null);
  assert.equal(contract.operator_triage.track_b_paper_status_code, "TRACK_B_PAPER_READY_NO_SIGNAL");
  assert.equal(
    contract.operator_triage.verdict_sentence,
    "Track B PAPER evaluating live decision bars; no trade signals observed.",
  );
  assert.equal(contract.operator_triage.root_cause.code, "TRACK_B_PAPER_READY_NO_SIGNAL");
  assert.equal(marketDataGate?.status, "pass");
  assert.equal(marketDataGate?.label, "Track B Live Market Data");
  assert.ok(!contract.operator_triage.verdict_sentence.includes("paper_market_data_stale_or_unavailable"));
  assert.equal(
    contract.operator_triage.track_b_legacy_market_data_note,
    "Legacy paper market-data status is separate from Track B Databento Live execution readiness.",
  );
});

test("Track B PAPER live-execution blocker is reported precisely", () => {
  const contract = buildOperatorTriageContract(baseInput({
    global: {
      mode: "PAPER",
      mode_label: "PAPER",
      live_disabled: true,
      market_data_status: "STALE",
      market_data_label: "STALE",
      reconciliation_status: "CLEAN",
      stale: true,
    },
    trackBPaperTrading: {
      available: true,
      startup_readiness_diagnostic: {
        instruments: {
          MGC: { instrument: "MGC", context_ready: true, live_execution_approved: false, paper_evaluation_allowed: false },
        },
      },
      zero_activity_diagnostic: {
        diagnosis_classification: "STALE_LIVE_FEED",
        dominant_blocker: "Execution freshness failing for MGC",
      },
    },
  }));

  assert.equal(contract.operator_triage.track_b_paper_status_code, "TRACK_B_PAPER_BLOCKED_LIVE_EXECUTION");
  assert.equal(contract.operator_triage.hard_gates.find((gate) => gate.key === "market-data")?.status, "fail");
  assert.equal(
    contract.operator_triage.root_cause.detail,
    "Track B PAPER blocked: live execution freshness failed for MGC.",
  );
});

test("GC Phase-1 preflight readiness overrides stale legacy zero-activity diagnostic", () => {
  const contract = buildOperatorTriageContract(baseInput({
    trackBPaperTrading: {
      available: true,
      review_required_count: 0,
      phase1_gc_readiness: {
        available: true,
        classification: "GC_PHASE1_READY_FOR_GUARDED_PAPER_WATCH",
        ready_for_guarded_paper_watch: true,
        strategy_id: "gc_1x_asia_london_participation__asia_london_long_v5",
        candidate_evaluation_ready: true,
        paper_candidate_approved: true,
        realtime_feed_confirmed: true,
        can_submit: false,
        live_money_eligible: false,
      },
      startup_readiness_diagnostic: {
        instruments: {},
      },
      zero_activity_diagnostic: {
        stale: true,
        diagnosis_classification: "STALE_DIAGNOSTIC",
        generated_at: "2026-05-06T19:43:32Z",
        signals_seen: 0,
      },
    },
  }));

  assert.equal(contract.operator_triage.track_b_paper_status_code, "GC_PHASE1_READY_FOR_GUARDED_PAPER_WATCH");
  assert.equal(
    contract.operator_triage.track_b_paper_status_message,
    "GC Phase-1 candidate is ready for guarded PAPER watch after current monday-live preflight; legacy lifecycle diagnostics remain read-only context.",
  );
  assert.equal(contract.operator_triage.paper_trade_allowed, true);
  assert.equal(contract.operator_triage.root_cause.code, "GC_PHASE1_READY_FOR_GUARDED_PAPER_WATCH");
  assert.equal(contract.operator_triage.hard_gates.find((gate) => gate.key === "market-data")?.status, "pass");
});

test("GC Phase-1 blocked readiness overrides stale legacy zero-activity diagnostic", () => {
  const contract = buildOperatorTriageContract(baseInput({
    trackBPaperTrading: {
      available: true,
      review_required_count: 0,
      phase1_gc_readiness: {
        available: true,
        classification: "GC_PHASE1_PREFLIGHT_BLOCKED",
        ready_for_guarded_paper_watch: false,
        strategy_id: "gc_1x_asia_london_participation__asia_london_long_v5",
        blocker: "paper_trade_allowed_true: paper_trade_allowed=False",
        candidate_evaluation_ready: true,
        paper_candidate_approved: true,
        realtime_feed_confirmed: true,
        can_submit: false,
        live_money_eligible: false,
      },
      startup_readiness_diagnostic: {
        instruments: {},
      },
      zero_activity_diagnostic: {
        stale: true,
        diagnosis_classification: "STALE_DIAGNOSTIC",
        generated_at: "2026-05-06T19:43:32Z",
        signals_seen: 0,
      },
    },
  }));

  assert.equal(contract.operator_triage.track_b_paper_status_code, "GC_PHASE1_PREFLIGHT_BLOCKED");
  assert.equal(
    contract.operator_triage.track_b_paper_status_message,
    "GC Phase-1 guarded PAPER watch is not ready: paper_trade_allowed_true: paper_trade_allowed=False. Legacy lifecycle diagnostics remain read-only context.",
  );
  assert.equal(contract.operator_triage.root_cause.code, "GC_PHASE1_PREFLIGHT_BLOCKED");
});

test("Track B PAPER feature-context blocker stays separate from live market-data freshness", () => {
  const contract = buildOperatorTriageContract(baseInput({
    trackBPaperTrading: {
      available: true,
      startup_readiness_diagnostic: {
        instruments: {
          MNQ: { instrument: "MNQ", context_ready: false, live_execution_approved: true, paper_evaluation_allowed: false },
        },
      },
      zero_activity_diagnostic: { diagnosis_classification: "INPUTS_NOT_READY" },
    },
  }));

  assert.equal(contract.operator_triage.track_b_paper_status_code, "TRACK_B_PAPER_BLOCKED_FEATURE_CONTEXT");
  assert.equal(
    contract.operator_triage.verdict_sentence,
    "Track B PAPER blocked: feature context not ready for MNQ.",
  );
});
