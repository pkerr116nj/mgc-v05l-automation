# CAE2 Filter Registry

Generated: 2026-07-03

Built-in named filters:

| Name | Behavior |
|---|---|
| `instrument` | template for `instrument == value` explicit filters |
| `strategy` | template for `strategy_id == value` explicit filters |
| `session` | template for `session_at_entry == value` explicit filters |
| `valid_gre_only` | require `gre_validity_classification=VALID` |
| `valid_vix_only` | require `market_context_validity_classification=VALID` |
| `side` | template for `side == value` explicit filters |
| `date_window` | use explicit `entry_time date_gte/date_lte` filters |

Supported explicit filter operations:

- `eq`
- `ne`
- `in`
- `not_in`
- `exists`
- `missing`
- `gt`
- `gte`
- `lt`
- `lte`
- `date_gte`
- `date_lte`
