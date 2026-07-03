# CAE2 Dimension Registry

Generated: 2026-07-03

Built-in dimensions:

| Name | Field | Validity |
|---|---|---|
| `strategy` | `strategy_id` | none |
| `lane` | `lane_id` | none |
| `session` | `session_at_entry` | none |
| `instrument` | `instrument` | none |
| `contract` | `contract` | none |
| `side` | `side` | none |
| `vix_regime` | `vix_regime` | `market_context_validity_classification=VALID` |
| `vix_percentile_bucket` | `vix_percentile_bucket` | `market_context_validity_classification=VALID` |
| `gre_label` | `gre_label` | `gre_validity_classification=VALID` |
| `gre_confidence_bucket` | `gre_confidence_bucket` | `gre_validity_classification=VALID` |
| `crfd_regime` | `crfd_regime` | `crfd_validity_classification=VALID` |
| `exit_policy` | `exit_policy` | none |

Unknown dimension names are treated as raw field names for backward-compatible analytics migration.
