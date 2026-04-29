# Route Destination Policy

- `ibkr_paper_bridge_submit_capable`: create standardized order intent, invoke IBKR bridge path, and record broker-path fill only from broker truth.
- bridge blocked: emit `BLOCKED_NOT_SENT_TO_BROKER` and do not create a local paper fill.
- `legacy_app_paper_runtime`: local fills allowed, but labeled internal-only and excluded from IBKR broker-path performance.

## Focus Lane Runtime Policy
- `mes_1x_ny_early_core__us_early_long` -> destination=`ibkr_paper_bridge_submit_capable` policy=`BROKER_PATH_ONLY_OR_BLOCKED_NOT_SENT` remediation=`FIXED_FOR_FUTURE_SIGNALS`
- `mes_1x_ny_early_core__us_early_long` -> destination=`ibkr_paper_bridge_submit_capable` policy=`BROKER_PATH_ONLY_OR_BLOCKED_NOT_SENT` remediation=`FIXED_FOR_FUTURE_SIGNALS`
- `mnq_1x_ny_early_core__us_early_long` -> destination=`ibkr_paper_bridge_submit_capable` policy=`BROKER_PATH_ONLY_OR_BLOCKED_NOT_SENT` remediation=`FIXED_FOR_FUTURE_SIGNALS`
- `mnq_1x_ny_early_core__us_early_long` -> destination=`ibkr_paper_bridge_submit_capable` policy=`BROKER_PATH_ONLY_OR_BLOCKED_NOT_SENT` remediation=`FIXED_FOR_FUTURE_SIGNALS`
- `nq_1x_ny_early_core__us_early_long` -> destination=`ibkr_paper_bridge_submit_capable` policy=`BROKER_PATH_ONLY_OR_BLOCKED_NOT_SENT` remediation=`FIXED_FOR_FUTURE_SIGNALS`
- `nq_1x_ny_early_core__us_early_long` -> destination=`ibkr_paper_bridge_submit_capable` policy=`BROKER_PATH_ONLY_OR_BLOCKED_NOT_SENT` remediation=`FIXED_FOR_FUTURE_SIGNALS`
- `es_1x_ny_early_core__us_early_long` -> destination=`ibkr_paper_bridge_submit_capable` policy=`BROKER_PATH_ONLY_OR_BLOCKED_NOT_SENT` remediation=`FIXED_FOR_FUTURE_SIGNALS`
- `es_1x_ny_early_core__us_early_long` -> destination=`ibkr_paper_bridge_submit_capable` policy=`BROKER_PATH_ONLY_OR_BLOCKED_NOT_SENT` remediation=`FIXED_FOR_FUTURE_SIGNALS`
