# CAE4 Dimension Catalog

| Identifier | Display | Type | Source | Validity | Availability | Limitations |
| --- | --- | --- | --- | --- | --- | --- |
| contract | Contract | string | canonical_trade_outcomes |  | READY |  |
| crfd_regime | CRFD Regime | string | canonical_research_feature_dataset | crfd_validity_classification=VALID | PARTIAL | Availability depends on CRFD provider coverage. |
| exit_policy | Exit Policy | string | canonical_trade_outcomes |  | READY | Exit-efficiency interpretation still limited by sparse MFE/MAE. |
| gre_confidence_bucket | GRE Confidence Bucket | enum | historical_gre | gre_validity_classification=VALID | PARTIAL | Gold-scoped historical coverage only. |
| gre_label | GRE Label | enum | historical_gre | gre_validity_classification=VALID | PARTIAL | Gold-scoped historical coverage only. |
| instrument | Instrument | string | canonical_trade_outcomes |  | READY |  |
| lane | Lane | string | canonical_trade_outcomes |  | READY |  |
| session | Session | enum | canonical_trade_outcomes |  | READY |  |
| side | Side | enum | canonical_trade_outcomes |  | READY |  |
| strategy | Strategy | string | canonical_trade_outcomes |  | READY |  |
| vix_percentile_bucket | VIX Percentile Bucket | enum | canonical_market_context | market_context_validity_classification=VALID | READY | Requires valid CMC/VIX enrichment. |
| vix_regime | VIX Regime | enum | canonical_market_context | market_context_validity_classification=VALID | READY | Requires valid CMC/VIX enrichment. |

Diagnostic/research only. No production recommendations, runtime changes, broker actions, strategy changes, or gates.
