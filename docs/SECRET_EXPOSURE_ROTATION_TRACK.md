# Secret Exposure Rotation Track

Opened: 2026-04-23
Status: cleared for the audited 8-file shortlist
Scope: filename-only triage from repo search surfaces; this is a cleanup and rotation track, not a claim that every match is an active credential.

## Current Tree Assessment

- No literal private-key block was confirmed in the current tracked tree during this pass.
- Most scan hits in the current tree are one of:
  - contract or field names such as `access_token`, `refresh_token`, `api_key`, and `client_secret`
  - command names such as `schwab-refresh-token`
  - false positives from the substring `sk-` inside `risk-shaped`
- A small set of Schwab fixtures/tests used token-looking example strings. Those were replaced with explicit redacted-example values in this pass.
- The dedicated git-history audit for the 8-file manual shortlist is complete.
- No real historical secret material was confirmed in that shortlist.

## Immediate Rules

- Do not copy suspected token or private-key contents into issues, chat, PR text, or new fixtures.
- Treat any live credential-looking material as compromised until rotation proves otherwise.
- Replace sensitive fixtures with redacted or synthetic payloads before wider cleanup work.

## Filename-Only Triage Buckets

### Provider auth and token flows

- `config/market_data_providers.json`
- `src/mgc_v05l/market_data/databento_provider.py`
- `src/mgc_v05l/market_data/provider_config.py`
- `src/mgc_v05l/market_data/schwab_auth.py`
- `src/mgc_v05l/market_data/schwab_http.py`
- `src/mgc_v05l/market_data/schwab_local_auth.py`
- `src/mgc_v05l/market_data/schwab_models.py`
- `src/mgc_v05l/app/schwab_token_bootstrap_web.py`
- `src/mgc_v05l/app/schwab_token_bootstrap_web 2.py`
- `tests/fixtures/schwab_token_response.json`
- `tests/unit/test_mgc_v05l_auth_bootstrap.py`
- `tests/unit/test_mgc_v05l_schwab_local_auth.py`
- `tests/unit/test_mgc_v05l_schwab_market_data.py`
- `tests/unit/test_mgc_v05l_schwab_token_bootstrap_web.py`

### Docs and runbooks

- `README.md`
- `docs/DEVELOPER_RUNBOOK.md`
- `docs/SCHWAB_MARKET_DATA_ADAPTER.md`
- `docs/specs/ATP_COMPANION_GC_ASIA_US_PRODUCTION_TRACK_RUNBOOK.md`
- `docs/specs/MGC_v0.5l_External_Automation_Build_Phase_2_System_Architecture_and_Execution_Design.md`
- `src/mgc_v05l_automation.egg-info/PKG-INFO`

### App, dashboard, and runtime surfaces

- `desktop/src/main/runtime.ts`
- `desktop/src/main/runtime.test.ts`
- `desktop/src/renderer/App.tsx`
- `desktop/src/renderer/styles.css`
- `src/mgc_v05l/app/dashboard_assets/operator_dashboard.css`
- `src/mgc_v05l/app/dashboard_assets/operator_dashboard.html`
- `src/mgc_v05l/app/dashboard_assets/operator_dashboard.js`
- `src/mgc_v05l/app/main.py`
- `src/mgc_v05l/app/operator_dashboard.py`
- `src/mgc_v05l/app/probationary_runtime.py`
- `src/mgc_v05l/production_link/client.py`
- `src/mgc_v05l/production_link/service.py`
- `tests/unit/test_mgc_v05l_operator_dashboard.py`
- `tests/unit/test_mgc_v05l_probationary_runtime.py`
- `tests/unit/test_mgc_v05l_production_link.py`

### Strategy and research configs

- `config/probationary_pattern_engine_paper_atp_companion_v1_pl_asia_us_risk_shaped_v1.yaml`
- `src/mgc_v05l/app/atp_companion_drawdown_limit_governance.py`
- `src/mgc_v05l/app/gc_mgc_forced_session_portfolio_shaping_research.py`
- `src/mgc_v05l/app/strategy_risk_shape_lab.py`
- `tests/unit/test_mgc_v05l_databento_market_data.py`
- `tests/unit/test_mgc_v05l_market_data_provider_ingest.py`

## Rotation and Cleanup Checklist

- Inventory each flagged file and classify it as: real secret, redacted example, false positive, code-path-only term, or operator placeholder.
- Rotate any live Schwab, Databento, production-link, or other provider credentials that ever appeared in committed material.
- Replace fixture and documentation payloads with redacted or synthetic examples.
- Remove any secret-bearing values from generated assets and dashboard snapshots.
- Purge or rewrite git history only after replacement artifacts and rotations are ready.
- Add a pre-commit or CI secret scan once the immediate cleanup pass is complete.

## Initial Classification From This Pass

### False positives from term collisions

- `risk-shaped` contains the substring `sk-`
- `desk-risk` and related dashboard ids can also collide with naive token scans

Representative files:

- `config/probationary_pattern_engine_paper_atp_companion_v1_pl_asia_us_risk_shaped_v1.yaml`
- `src/mgc_v05l/app/operator_dashboard.py`
- `src/mgc_v05l/app/dashboard_assets/operator_dashboard.html`
- `src/mgc_v05l/app/dashboard_assets/operator_dashboard.js`

### Code or contract terms only

These files currently appear because they legitimately define auth field names, env var names, or operator commands, not because they embed confirmed live secret values.

Representative files:

- `config/market_data_providers.json`
- `src/mgc_v05l/market_data/databento_provider.py`
- `src/mgc_v05l/market_data/provider_config.py`
- `src/mgc_v05l/market_data/schwab_http.py`
- `README.md`
- `docs/DEVELOPER_RUNBOOK.md`

### Redacted or synthetic examples

These are safe to keep only if they remain explicitly synthetic.

- `tests/fixtures/schwab_token_response.json`
- `tests/unit/test_mgc_v05l_schwab_local_auth.py`
- `tests/unit/test_mgc_v05l_schwab_market_data.py`

### Historical shortlist verdict

The 8-file manual-review shortlist has now been audited in git history and is cleared for real-secret exposure.

## 8-File History Audit

### Cleared with no real historical secret material

- `src/mgc_v05l/app/schwab_token_bootstrap_web 2.py`
  - Current tracked file: clean
  - History result: no real sensitive material found
  - Secret class implicated: none committed; runtime Schwab OAuth workflow code only
  - Rotation required: no
  - History rewrite required: no
  - Confidence: medium
  - Rationale: single tracked commit only; contains auth workflow code and token-state field names, but no literal token, key, secret, bearer, or private-key material

- `src/mgc_v05l/app/schwab_token_bootstrap_web.py`
  - Current tracked file: clean
  - History result: no real sensitive material found
  - Secret class implicated: none committed; runtime Schwab OAuth workflow code only
  - Rotation required: no
  - History rewrite required: no
  - Confidence: high
  - Rationale: three tracked revisions audited; history contains command strings, token metadata checks, and refresh plumbing only

- `src/mgc_v05l/market_data/schwab_auth.py`
  - Current tracked file: clean
  - History result: no real sensitive material found
  - Secret class implicated: none committed; Schwab client credentials and refresh tokens are loaded at runtime from env or token storage
  - Rotation required: no
  - History rewrite required: no
  - Confidence: high
  - Rationale: four tracked revisions audited back to file introduction; the file builds auth headers and token requests from runtime values only

- `src/mgc_v05l/market_data/schwab_http.py`
  - Current tracked file: clean
  - History result: no real sensitive material found
  - Secret class implicated: none committed; bearer token usage is dynamic only
  - Rotation required: no
  - History rewrite required: no
  - Confidence: high
  - Rationale: four tracked revisions audited; Authorization headers are built from runtime access-token getters only

- `src/mgc_v05l/market_data/schwab_local_auth.py`
  - Current tracked file: clean
  - History result: no real sensitive material found
  - Secret class implicated: none committed; local auth helper fields only
  - Rotation required: no
  - History rewrite required: no
  - Confidence: medium-high
  - Rationale: two tracked revisions audited; contains auth state and timestamp fields only, with no literal token or client-secret payloads

- `src/mgc_v05l/market_data/schwab_models.py`
  - Current tracked file: clean
  - History result: no real sensitive material found
  - Secret class implicated: none committed; token model serialization code only
  - Rotation required: no
  - History rewrite required: no
  - Confidence: high
  - Rationale: four tracked revisions audited; history shows token field definitions and serialization from instance attributes, not embedded token values

- `src/mgc_v05l/production_link/client.py`
  - Current tracked file: clean
  - History result: no real sensitive material found
  - Secret class implicated: none committed; runtime bearer-token client only
  - Rotation required: no
  - History rewrite required: no
  - Confidence: medium-high
  - Rationale: three tracked revisions audited; bearer auth is assembled from a runtime token accessor and no literal bearer material appears in history

### Cleared as synthetic-example history only

- `tests/unit/test_mgc_v05l_schwab_token_bootstrap_web.py`
  - Current tracked file: clean with synthetic examples
  - History result: synthetic examples only
  - Secret class implicated: test-only dummy access and refresh token strings
  - Rotation required: no
  - History rewrite required: no
  - Confidence: high
  - Rationale: the only tracked revision contains obvious dummy values such as `stale-token` and `bad-refresh-token`, with no evidence of real credential material

## Audit Outcome

- Files cleared: all 8 shortlisted files
- Files needing rotation: none from the audited shortlist
- Files needing history rewrite: none from the audited shortlist
- Files still requiring manual judgment: none in the audited shortlist
- Narrowed exposure-track closure recommendation: yes

## Exit Criteria

- No committed real credentials remain in tracked files.
- Rotations are complete for every confirmed live token or key.
- Replacement fixtures and docs are non-sensitive.
- The audited shortlist is either cleared or remediated with evidence.

## Recommended Next Sequence

1. Close the narrowed 8-file exposure shortlist.
2. Keep synthetic fixture values explicitly marked as examples or redacted placeholders.
3. Add or retain a pre-commit or CI secret scan so future auth/token regressions are caught earlier.
4. Return focus to validation work.
