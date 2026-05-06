# PostgreSQL Schema Management

This directory contains the PostgreSQL schema used by the simulation export and
inventory flows.

## Managed Baseline

Schema evolution is now controlled by `infra.postgres.migrations`.

Current migration registry:

1. `0001_core_schema`
   - applies `schema.sql`
   - creates `topo` and legacy `inventory`
2. `0002_v2_and_explicit_schema`
   - applies `schema_v2.sql`
   - applies `explicit_scenario_schema.sql`
   - creates `topo_v2`, `inventory_v2`, and `inventory_explicit`
3. `0003_topo_export_runtime_delta`
   - promotes exporter runtime DDL for the default `topo` schema into a managed migration
   - keeps old runtime DDL as a compatibility fallback
4. `0004_explicit_resource_limit_runtime_fields`
   - adds explicit resource-limit fields used by runtime topology settings
   - stores satellite `access_model` and HAPS feeder ray counts in `inventory_explicit`

Applied versions are recorded in `topo.schema_migration`.

## Applying Migrations

From the repository root:

```powershell
.\.venv\Scripts\python.exe scripts\apply_postgres_migrations.py
```

To stop at a specific baseline:

```powershell
.\.venv\Scripts\python.exe scripts\apply_postgres_migrations.py --target-version 0002_v2_and_explicit_schema
```

## Transitional Runtime Fallbacks

Some runtime code still executes idempotent `CREATE TABLE IF NOT EXISTS` and
`ALTER TABLE ... ADD COLUMN IF NOT EXISTS` statements. These statements are kept
temporarily so older developer databases can still run, but managed migrations
are now the primary upgrade path.
