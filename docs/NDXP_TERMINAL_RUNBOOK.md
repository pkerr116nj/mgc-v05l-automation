# Schwab NDXP Credit Terminal — Initial Locked Build

## Product direction

The current interface is an engineering preview of the Schwab connection, spread ticket, broker truth, and diagnostic model. It is not the final operator design.

The production interface will visually and operationally approximate the Thinkorswim Mobile workflow, using Patrick's screenshots as the reference for information density, navigation, option-chain interaction, order review, working-order management, and touch behavior. It will remain purpose-built for NDX/NDXP credit verticals rather than reproduce unrelated Thinkorswim functions.

The client will be a responsive progressive web application for Mac, iPad, and potentially iPhone. Schwab OAuth credentials, tokens, broker polling, audit records, and order transmission stay on the trusted Mars backend. Mobile clients receive the application surface over an authenticated HTTPS connection, including when reached through the existing remote-access path. No Schwab app secret or refresh token is stored on iPad or iPhone.

Before mobile use is enabled, the localhost-only preview server will be replaced with an authenticated service boundary, TLS, explicit device/session authorization, and operator confirmation suitable for broker mutations.

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
| `MARKET_POLLER_STALE` | No successful market poll completed for more than 5 seconds. |
| `STALE_MARKET_DATA` | Schwab responded, but the newest quote timestamp is more than 5 seconds old. |
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
