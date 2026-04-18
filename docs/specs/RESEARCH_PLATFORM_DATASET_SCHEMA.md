# Research Platform Dataset Schema

## Storage Recommendation

- durable dataset bundles under `outputs/research_platform/...`
- JSONL for exact rehydration into domain objects
- Parquet for queryability and cheap experiment consumption
- DuckDB catalog per bundle for ad hoc SQL

## Feature Bundle

Key:
- `bundle_id`
- `symbol`
- `source_db`
- `selected_sources`
- `start_timestamp`
- `end_timestamp`
- `feature_version`

Dataset:
- `feature_states`
- row key: `instrument + decision_ts`
- schema source: `FeatureState` in `src/mgc_v05l/research/trend_participation/models.py`

## Scope Bundle

Key:
- `bundle_id`
- `feature_bundle_id`
- `symbol`
- `allowed_sessions`
- `point_value`
- `candidate_version`
- `outcome_engine_version`

Datasets:
- `entry_states`
- `timing_states`
- `trade_records`

Row keys:
- `entry_states`: `instrument + decision_ts`
- `timing_states`: `instrument + decision_ts`
- `trade_records`: `decision_id`

## Manifest Fields

- `artifact_version`
- `bundle_type`
- `bundle_id`
- `generated_at`
- `source_db`
- `source_date_span`
- `selected_sources`
- `versions`
- `datasets`
- `duckdb_catalog_path`

## Versioning Model

- feature changes bump `feature_version`
- entry/timing schema or rule changes bump `candidate_version`
- outcome rule changes bump `outcome_engine_version`
- bundle identity hash changes when any scientific dependency changes

## Registry Direction

First tranche stops at durable bundle manifests.

Next tranche should add a project-level experiment registry keyed by:
- `run_id`
- `code_version`
- `data_version`
- `feature_version`
- `candidate_version`
- `outcome_engine_version`
- `config_hash`
- `target_hash`
- `control_hash`
- `date_span`
- `artifact_manifest_paths`
