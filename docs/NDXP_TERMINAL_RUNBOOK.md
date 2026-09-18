# NDXP Credit Terminal — Locked Review Build

## Product direction

The current interface is an engineering preview of the Schwab connection, spread ticket, broker truth, and diagnostic model. It is not the final operator design.

The production interface will visually and operationally approximate the Thinkorswim Mobile workflow, using Patrick's screenshots as the reference for information density, navigation, option-chain interaction, order review, working-order management, and touch behavior. It will remain purpose-built for NDX/NDXP credit verticals rather than reproduce unrelated Thinkorswim functions.

The client will be a responsive progressive web application for Mac, iPad, and potentially iPhone. Schwab OAuth credentials, tokens, broker polling, audit records, and order transmission stay on the trusted Mars backend. Mobile clients receive the application surface over an authenticated HTTPS connection, including when reached through the existing remote-access path. No Schwab app secret or refresh token is stored on iPad or iPhone.

### Market-data boundary

The preferred live mode deliberately separates two sources:

- Databento `OPRA.PILLAR` consolidated `cmbp-1` supplies NDX/NDXP option NBBO bid and ask values.
- Schwab supplies the contract roster, NDX cash-index spot, account positions, working orders, and eventual execution truth.

One long-lived Databento session serves the terminal. It subscribes only the selected expiration's 35 strikes on either side of spot (calls and puts) and adds symbols when the operator changes expiration. The session never opens one connection per symbol and never subscribes the entire OPRA universe. Databento Live cannot remove subscriptions from an active session, so restarting the terminal clears accumulated expiration subscriptions.

Schwab option bid and ask values are not used as an invisible fallback in Databento mode. Until a contract has a current Databento quote, its price is blank and analytics fail closed. The interface labels the mixed boundary as `Databento OPRA NBBO · Schwab NDX spot` and reports subscription, mapping, quote, timestamp, and stream-error evidence.

Before mobile use is enabled, the localhost-only preview server will be replaced with an authenticated service boundary, TLS, explicit device/session authorization, and operator confirmation suitable for broker mutations.

### Screenshot-derived interaction contract

The September 14 Thinkorswim iPad references establish these initial chain requirements:

- calls and puts appear side by side around a central strike-pair column;
- each row represents a selectable 10-point vertical rather than an unpaired contract;
- valid OTM call-credit spreads above spot and OTM put-credit spreads below spot receive a subtle side-specific tint while the spread straddling spot retains the stronger full-row highlight;
- the chain provides at least 25 shared call-and-put strike levels below spot and 25 above spot, with an on-screen coverage warning if Schwab returns less;
- the page scrolls vertically through strikes and the chain scrolls horizontally through fields;
- quote fields, net and percentage change, Greeks, IV, volume, and open interest are selectable display columns;
- tapping a positive bid launches a sell-to-open credit ticket and tapping a positive ask launches the inverse buy-to-close debit ticket;
- both opening and closing tickets default their limit price to the displayed spread mid, not the touched bid or ask;
- ticket quantity defaults to 20 contracts; and
- until a Thinkorswim order-entry screenshot is available, the ticket uses the terminal's explicit risk and preview layout rather than guessing at Thinkorswim's precise order-entry presentation.

The displayed spread delta is position delta per one short credit spread: `long-leg delta - short-leg delta`. Total position delta multiplies that result by ticket quantity. Derived theta and gamma use the same signed position convention. These are transparent leg-derived values, not Schwab-provided complex-spread Greeks; they still inherit any error in Schwab's individual-leg inputs. IV is therefore labeled as short-leg IV rather than represented as a net spread IV. A separately derived observed delta based on synchronized changes in spread mid versus NDX remains a future diagnostic enhancement.

The live Schwab chain request asks for 120 strike levels, providing a buffer beyond the 25-per-side display requirement. The demo supplies 30 levels on each side. On first load and after an expiration change, the terminal centers the page on the spread row containing spot; all returned rows remain vertically scrollable.

### Independent IV and range analytics

The selected-expiration range is calculated independently from current option bid/ask midpoints. The model does not consume Schwab's supplied IV or Greeks:

1. synchronized call and put mids infer the forward through call/put parity;
2. out-of-the-money contract mids are inverted to implied volatilities with Black-76;
3. a piecewise-linear smile supplies ATM, short-strike, and breakeven volatility;
4. ATM IV and exact time remaining to the 4:00 p.m. ET NDXP expiration produce the expected move; and
5. the fitted distribution produces the model-implied probability of finishing beyond the short strike and beyond the spread's midpoint breakeven.

The prominent range is ±1 standard deviation. ±0.5 and ±1.5 standard-deviation ranges remain visible as secondary context. The chain defaults to breakeven distance, breakeven/expected-move multiple, probability beyond breakeven, and credit/risk columns. Schwab short-leg IV remains selectable as a comparison diagnostic.

Analytics fail closed when spot or expiration is missing, fewer than six usable option mids are available, call/put parity is internally inconsistent, the inferred forward or ATM IV is implausible, or any near-ATM model input is more than 15 seconds old. An invalid model displays its reason and publishes no per-spread probabilities.

Opportunity highlighting is informational and transparent. The initial device-local filters are:

- breakeven distance of at least 1.0 expected move;
- opening midpoint credit of at least $1.00;
- credit-to-maximum-risk of at least 10%; and
- combined spread market width no greater than $0.75.

Patrick's preferred opening-credit band is $2.00–$2.50. Spreads passing all filters in that band receive the strongest highlight; qualifying spreads outside it remain distinguishable, and credits above $2.50 are labeled elevated rather than automatically preferred. The ticket shows the $1.00–$1.40 target closing-debit band and projected gross profit for the selected quantity. Filters and highlights never construct or transmit an order.

## Safety state

This build can read live Schwab market and account data and construct exact order payloads. It cannot transmit, cancel, or replace a Schwab order.

Two independent gates protect the broker mutation methods:

1. `LIVE_TRANSMISSION_COMPILED = False` in `src/mgc_v05l/ndxp_terminal/orders.py`.
2. `MGC_NDXP_LIVE_TRANSMISSION_ENABLED` must equal `1`.

The source gate remains false until Patrick separately authorizes a controlled live pilot. Setting an environment variable alone cannot unlock this build.

## Credential setup on Mars

The preferred macOS setup stores the app key and secret in Keychain and creates a local environment loader containing only Keychain lookup commands:

```bash
cd /Users/patrick/Dev/MGC-v05l-automation
bash scripts/setup_schwab_credentials_macos.sh
```

Enter the application key and secret at the local hidden prompts. Do not place application secrets or tokens in source control, issue text, chat, or screenshots. The generated `.local/schwab_env.sh` has this structure and contains no literal credentials:

```bash
export SCHWAB_APP_KEY="$(security find-generic-password ...)"
export SCHWAB_APP_SECRET="$(security find-generic-password ...)"
export SCHWAB_CALLBACK_URL="https://127.0.0.1:8182/callback"
export SCHWAB_TOKEN_FILE="/Users/patrick/Dev/MGC-v05l-automation/.local/schwab/tokens.json"
```

OAuth access and refresh tokens remain in the existing mode-600 local token file because the current OAuth client rotates them there. The directory and file are ignored by Git.

Databento mode additionally requires `DATABENTO_API_KEY` and a live `OPRA.PILLAR` entitlement. Keep the key in the existing local Mars environment file. For a separate preview worktree, point the launcher at that file rather than copying the secret:

```bash
export MGC_DATABENTO_ENV_FILE=/Users/patrick/Dev/MGC-v05l-automation/.env.local
```

The launcher sources the file without displaying its contents. A missing key fails immediately; a missing OPRA live entitlement is reported by the Databento stream status in the interface.

If the existing token is absent or expired beyond refresh, run the repository's established local OAuth bootstrap:

```bash
bash scripts/run_schwab_token_web.sh
```

The Schwab password and any multifactor challenge stay on Schwab's site. The terminal reads the resulting local OAuth token file.

## Launch

```bash
bash scripts/run_ndxp_terminal.sh
```

The terminal binds only to `127.0.0.1` and opens `http://127.0.0.1:8810/`.

To inspect the interface without Schwab credentials:

```bash
bash scripts/run_ndxp_terminal.sh --demo
```

Demo mode displays a persistent `SYNTHETIC DEMO DATA` banner. Its repeated prices, IV, changes, and Greeks exist only to exercise layout and ticket behavior and must never be interpreted as a market snapshot.

To install and run the hybrid live-data build on Mars:

```bash
.venv/bin/python -m pip install -e '.[databento]'

export MGC_DATABENTO_ENV_FILE=/Users/patrick/Dev/MGC-v05l-automation/.env.local
bash scripts/run_ndxp_terminal.sh --databento --no-browser
```

By default it listens only on `127.0.0.1:8810`. At home, the explicit source-locked LAN review mode can instead be opened normally from a Mac or iPad on the trusted private network:

```bash
bash scripts/run_ndxp_terminal.sh --databento --lan-live --no-browser
```

Open `http://<MARS_LAN_IP>:8810/`. No SSH tunnel or router port-forward is required. This mode exposes read-only Schwab account truth to devices already on the private LAN, while all broker mutations remain source-locked. It is not suitable for public exposure; authenticated HTTPS remains required before enabling live trading or internet-facing access.

## Roaming preview over UniFi Teleport

The roaming preview is intentionally limited to synthetic demo data. It does not load Schwab credentials or tokens, and the source-level broker transmission lock remains in force.

On Mars, start the preview with:

```bash
bash scripts/run_ndxp_terminal.sh --demo --teleport-demo --no-browser
```

This explicit combination listens on all Mars interfaces. The server refuses a remote bind for live Schwab mode or when either demo safety flag is missing.

Find Mars's LAN address in the UniFi console, or on Mars with:

```bash
MARS_INTERFACE="$(route -n get default | awk '/interface:/{print $2}')"
ipconfig getifaddr "${MARS_INTERFACE}"
```

While the roaming device is connected to the homelab through UniFi Teleport, open:

```text
http://<MARS_LAN_IP>:8810/
```

If the page does not open, confirm that the terminal is still running and permit incoming connections for Python in the macOS firewall. Do not add a public router port-forward. This HTTP demo is appropriate only for synthetic data across the trusted LAN/Teleport tunnel; authenticated HTTPS is required before any remote live-account view is enabled.

## Read-only access verification

Choose **Verify API access** in the terminal. It performs:

- `GET /trader/v1/accounts/accountNumbers`
- `GET /trader/v1/accounts?fields=positions`
- `GET /trader/v1/accounts/{hash}/orders?status=WORKING`
- `GET /marketdata/v1/chains?symbol=NDX`
- `GET /marketdata/v1/quotes?symbols=$NDX`

It does not call a broker mutation endpoint. A successful result proves account enumeration, account truth, working-order access, and NDX option-chain access for the current OAuth token. Schwab does not provide a dry-run order-validation endpoint, so NDXP order acceptance still requires a separately authorized minimum-size live pilot.

## Diagnostic meanings

| Classification | Evidence |
|---|---|
| `CLIENT_OR_UI_STALL` | The browser heartbeat stopped for more than 3.5 seconds. |
| `APPLICATION_WORKER_STALL` | The local background poller missed its schedule by more than 2.5 seconds. |
| `SCHWAB_RESPONSE_DELAY` | Schwab market data took more than 2.5 seconds or broker truth took more than 4 seconds. |
| `SCHWAB_OR_NETWORK_ERROR` | A Schwab HTTP, OAuth, DNS, TLS, or local network request failed. |
| `DATABENTO_STREAM_ERROR` | The Databento OPRA stream reported an entitlement, connection, protocol, or provider error. |
| `DATABENTO_QUOTES_PENDING` | The OPRA subscription exists, but no selected-contract quotes have arrived yet. |
| `DATABENTO_PARTIAL_QUOTES` | Fewer than 80% of selected OPRA contracts currently have a quote in the session cache. |
| `MARKET_POLLER_STALE` | No successful market poll completed for more than 5 seconds. |
| `STALE_MARKET_DATA` | The newest selected option quote timestamp is more than 5 seconds old. |
| `HEALTHY` | All measured layers remain within their thresholds. |

Diagnostics are appended to `outputs/ndxp_terminal/diagnostics.jsonl` without credentials, tokens, account numbers, positions, or order payloads.

## Initial order policy

- NDX or NDXP option symbols obtained from the current Schwab chain
- same root, expiration, and option type on both legs
- exact 10-point width
- credit-spread strike orientation enforced for calls and puts
- `NET_CREDIT`, `NORMAL`, and `DAY`
- quantity 1–100
- positive credit below the spread width
- preview account must exist in current broker truth
- both legs must exist in the latest chain snapshot
