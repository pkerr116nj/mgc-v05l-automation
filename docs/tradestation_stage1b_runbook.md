# TradeStation Stage 1B Runbook

## Purpose
This runbook is the operator guide for the first real read-only TradeStation API validation.

Stage 1B is limited to:
- authentication bootstrap
- account discovery
- explicit account selection
- read-only account truth validation

Stage 1B does not allow:
- order submission
- order cancel/replace
- flatten actions
- order preview
- strategy-triggered execution activity

The goal is to prove that:
- TradeStation credentials are wired correctly
- SIM and LIVE environments are kept separate
- the margin equities/options account and futures account can both be identified
- balances, positions, and open orders can be normalized into the application truth model

## Prerequisites

### Required access
- TradeStation API key / client id
- TradeStation client secret
- registered callback URL
- TradeStation API application access approved for the user/account
- awareness of which accounts are SIM versus LIVE
- awareness of which account is intended to be:
  - margin equities/options
  - futures

### Expected config and environment inputs
Expected environment/config values for the first real run:
- `TRADESTATION_CLIENT_ID`
- `TRADESTATION_CLIENT_SECRET`
- `TRADESTATION_CALLBACK_URL`

Expected local config/runtime paths:
- SIM token store:
  - `var/tradestation/sim_tokens.json`
- LIVE token store:
  - `var/tradestation/live_tokens.json`
- selected account store:
  - `var/tradestation/selected_accounts.json`

Expected code entrypoint:
- [tradestation_account_truth.py](/Users/patrick/Documents/MGC-v05l-automation/src/mgc_v05l/app/tradestation_account_truth.py)

## Safety Boundaries
Stage 1B is strictly read-only.

Allowed:
- auth bootstrap
- account discovery
- balances
- positions
- open orders

Not allowed:
- submit
- cancel
- replace
- flatten
- order preview
- strategy-triggered calls

Safety requirements:
- SIM and LIVE runs must remain separate
- SIM and LIVE token paths must remain separate
- SIM and LIVE account selections must remain separate
- no order-capable scope may be requested
- if any step suggests order-capable access is required, stop immediately

## Command Sequence

### 1. Credential readiness check
Before any real call, confirm:
- `TRADESTATION_CLIENT_ID` is set
- `TRADESTATION_CLIENT_SECRET` is set
- `TRADESTATION_CALLBACK_URL` is set
- token paths are writable
- selected account config path is writable

Recommended command:

```bash
PYTHONPATH=src ./.venv/bin/python -m mgc_v05l.app.tradestation_account_truth \
  --environment sim \
  --auth-bootstrap \
  --allow-real-api \
  --scopes "openid profile offline_access ReadAccount" \
  --output-dir outputs/reports/tradestation_account_truth/sim_bootstrap
```

Expected behavior at this stage:
- read-only safety banner printed
- readiness failure if required credentials are missing
- rejection if any order-capable scope is requested

### 2. Auth bootstrap SIM
Run the SIM bootstrap first.

Command:

```bash
PYTHONPATH=src ./.venv/bin/python -m mgc_v05l.app.tradestation_account_truth \
  --environment sim \
  --auth-bootstrap \
  --allow-real-api \
  --scopes "openid profile offline_access ReadAccount" \
  --output-dir outputs/reports/tradestation_account_truth/sim_bootstrap
```

Expected result:
- operator safety banner
- authorize URL / bootstrap artifact
- SIM token persisted only to:
  - `var/tradestation/sim_tokens.json`
- audit artifact written

### 3. Account discovery SIM
After SIM bootstrap succeeds, discover accessible accounts.

Command:

```bash
PYTHONPATH=src ./.venv/bin/python -m mgc_v05l.app.tradestation_account_truth \
  --environment sim \
  --discover-accounts \
  --allow-real-api \
  --output-dir outputs/reports/tradestation_account_truth/sim_discovery
```

Expected result:
- normalized accounts table
- raw account classification evidence
- inferred account types:
  - `margin_equities_options`
  - `futures`
  - `unknown`

### 4. Account selection SIM
Only after discovery should the selected accounts be persisted.

Command:

```bash
PYTHONPATH=src ./.venv/bin/python -m mgc_v05l.app.tradestation_account_truth \
  --environment sim \
  --select-margin-account <SIM_MARGIN_ACCOUNT_ID> \
  --select-futures-account <SIM_FUTURES_ACCOUNT_ID> \
  --output-dir outputs/reports/tradestation_account_truth/sim_select_accounts
```

Expected result:
- explicit SIM account selection written
- no fallback/default account behavior

### 5. Read-only truth SIM
After SIM accounts are selected, fetch read-only truth.

Command:

```bash
PYTHONPATH=src ./.venv/bin/python -m mgc_v05l.app.tradestation_account_truth \
  --environment sim \
  --read-only-truth \
  --allow-real-api \
  --output-dir outputs/reports/tradestation_account_truth/sim_truth
```

Expected result:
- balances loaded
- positions loaded
- open orders loaded
- per-account normalized truth
- combined application-level summary
- safety/audit artifact

### 6. LIVE flow
Do not run the LIVE flow automatically after SIM.

Repeat the equivalent LIVE read-only sequence only when explicitly approved:
- auth bootstrap LIVE
- account discovery LIVE
- account selection LIVE
- read-only truth LIVE

LIVE commands should mirror SIM commands with:
- `--environment live`
- separate output directories
- separate token storage
- separate account selections

## Expected Outputs
Each Stage 1B run should produce artifacts under `outputs/reports/tradestation_account_truth/...`.

Expected artifact classes:
- auth artifact
- discovery artifact
- selected account artifact
- truth snapshot artifact
- safety/audit log

Expected content by step:

### Auth artifact
- safety banner
- environment
- requested scopes
- bootstrap status
- audit metadata

### Discovery artifact
- discovered account list
- raw account classification evidence
- normalized account table
- environment

### Selected account artifact
- selected margin account id
- selected futures account id
- environment
- persistence status

### Truth snapshot artifact
- provider id
- environment
- selected accounts
- per-account balances
- per-account positions
- per-account open orders
- combined application-level portfolio summary
- freshness timestamps

### Safety/audit log
- attempted mode
- environment
- requested scopes
- read-only banner
- timestamp
- status

## Validation Checklist
Use this checklist after the first real SIM truth run.

- margin account identified
- futures account identified
- balances loaded
- positions loaded
- open orders loaded
- combined summary makes sense
- no order-capable scope requested
- SIM token written only to SIM token path
- selected SIM accounts written correctly
- no unexpected LIVE crossover in paths or artifacts

Questions to answer before advancing:
- does the inferred `margin_equities_options` account match operator expectations?
- does the inferred `futures` account match operator expectations?
- do balances and equity look plausible?
- do positions by account and by symbol look coherent?
- is the combined summary clearly application-level rather than broker-native netting?

## Common Failure Modes

### Auth failure
Symptoms:
- login/consent does not complete
- token exchange fails

Possible causes:
- wrong client id
- wrong client secret
- invalid callback URL
- TradeStation app access not enabled

### Callback mismatch
Symptoms:
- redirect completes incorrectly
- token exchange fails after browser auth

Possible cause:
- configured callback URL does not match registered app callback

### Missing scopes
Symptoms:
- auth succeeds but read-only calls fail

Possible cause:
- `ReadAccount` missing from requested scopes

### Expired token
Symptoms:
- discovery or truth call fails after prior bootstrap

Possible cause:
- stale access token
- refresh token missing or invalid

### No futures account found
Symptoms:
- discovery returns no account classifiable as futures

Action:
- stop
- verify account access with TradeStation

### No margin account found
Symptoms:
- discovery returns no account classifiable as margin/equities/options

Action:
- stop
- verify account access with TradeStation

### Account classification unknown
Symptoms:
- returned account metadata does not map cleanly to known account classes

Action:
- stop
- inspect raw classification evidence
- do not guess

### Endpoint shape mismatch
Symptoms:
- read-only endpoints return unexpected payload structure

Action:
- stop
- preserve artifact
- update parser only after review

## Stop Conditions
Stop immediately if any of the following occurs:

- any order-capable scope is required
- account ambiguity remains unresolved
- truth snapshot is incomplete
- SIM and LIVE paths appear mixed
- unexpected endpoint behavior appears
- account classification is unknown and cannot be verified
- discovery/truth calls succeed only with widened permissions beyond read-only account access

If stopped:
- preserve artifacts
- do not retry with broader scopes
- do not proceed to any dry-run or execution stage

## What Success Means
Stage 1B is successful only if:
- read-only truth works
- accounts are selected correctly
- combined portfolio view is coherent
- safety boundaries remain intact
- no order-capable scope was requested

Success at this stage means the system is ready for:
- one real dry-run ticket generated from a real truth snapshot

Success at this stage does not mean:
- TradeStation execution is ready
- SIM submission is ready
- LIVE submission is allowed

## Post-Run Operator Note
After a successful Stage 1B run:
- keep artifacts
- record which SIM and LIVE accounts were confirmed
- record any endpoint-shape quirks
- confirm broker symbol verification is still pending
- do not advance to submission design until the Stage 1B validation gate is formally accepted
