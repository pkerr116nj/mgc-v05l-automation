# Session Label Gap Diagnostic

- current fine-grained phase label: `UNCLASSIFIED`
- current broad trading session: `US_EARLY`
- session label gap active rows: `44`
- session label gap blocked rows: `0`

The fine-grained phase label can be `UNCLASSIFIED` during gaps such as 08:30–09:00 ET and 10:30–11:00 ET.
Broad session matching is tracked separately, so lanes can remain session-eligible even while the fine-grained label is `UNCLASSIFIED`.

## Sample Rows

- `es_1x_asia_london_participation__asia_london_long_v6_vol_floor_125`: phase `UNCLASSIFIED`, broad `US_EARLY`, session_eligible `False`, blocked `False`
- `es_1x_ny_early_core__us_early_long`: phase `UNCLASSIFIED`, broad `US_EARLY`, session_eligible `True`, blocked `False`
- `es_1x_ny_early_core__us_early_short_breakdown`: phase `UNCLASSIFIED`, broad `US_EARLY`, session_eligible `True`, blocked `False`
- `es_1x_ny_early_core__us_early_short_reclaim_fail`: phase `UNCLASSIFIED`, broad `US_EARLY`, session_eligible `True`, blocked `False`
- `es_1x_ny_early_core__us_late_long`: phase `UNCLASSIFIED`, broad `US_EARLY`, session_eligible `False`, blocked `False`
- `es_1x_ny_early_core__us_late_short_reclaim_fail`: phase `UNCLASSIFIED`, broad `US_EARLY`, session_eligible `False`, blocked `False`
- `es_1x_ny_early_core__us_midday_long`: phase `UNCLASSIFIED`, broad `US_EARLY`, session_eligible `False`, blocked `False`
- `es_1x_ny_early_core__us_midday_short_breakdown`: phase `UNCLASSIFIED`, broad `US_EARLY`, session_eligible `False`, blocked `False`
- `gc_1x_all_lanes__asia_early_long`: phase `UNCLASSIFIED`, broad `US_EARLY`, session_eligible `False`, blocked `False`
- `gc_1x_all_lanes__asia_early_short`: phase `UNCLASSIFIED`, broad `US_EARLY`, session_eligible `False`, blocked `False`