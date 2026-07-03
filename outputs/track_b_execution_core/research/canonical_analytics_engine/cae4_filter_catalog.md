# CAE4 Filter Catalog

| Identifier | Display | Field | Operation | Type | Validity |
| --- | --- | --- | --- | --- | --- |
| date_window | Date Window | entry_time | date_gte/date_lte | datetime |  |
| instrument | Instrument Filter | instrument | eq | string |  |
| session | Session Filter | session_at_entry | eq | enum |  |
| side | Side Filter | side | eq | enum |  |
| strategy | Strategy Filter | strategy_id | eq | string |  |
| valid_gre_only | Valid GRE Only |  |  | validity | gre_validity_classification=VALID |
| valid_vix_only | Valid VIX Only |  |  | validity | market_context_validity_classification=VALID |

Diagnostic/research only. No production recommendations, runtime changes, broker actions, strategy changes, or gates.
