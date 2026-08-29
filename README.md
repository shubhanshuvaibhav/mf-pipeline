# Mutual Fund Data Platform

An observable, idempotent data platform that ingests mutual-fund NAV histories from the MFAPI service and publishes typed analytical models with Airflow and dbt.

## Architecture

```text
MFAPI -> Airflow mapped ingestion tasks
             |-> data/bronze/raw_nav_data_<scheme>.json
             |-> bronze.raw_dump
             |-> control.ingestion_audit
                         |
                         v
              dbt staging and marts
                         |-> analytics_staging.stg_mf_nav
                         |-> analytics_marts.dim_fund
                         `-> analytics_marts.fct_fund_performance
```

## Design

- Airflow dynamically maps one ingestion task per scheme code.
- Transient HTTP failures are retried with bounded exponential backoff.
- Raw payloads are content-addressed with SHA-256 and protected by a scheme/hash uniqueness constraint.
- `bronze` preserves source payloads; `control` stores ingestion metadata.
- dbt deduplicates repeated API histories and publishes one NAV row per scheme/date.
- `dim_fund` contains one current descriptive row per scheme.
- `fct_fund_performance` contains one row per scheme/date and calculates returns against the previous available NAV observation.
- dbt tests cover required fields, positive NAV values, uniqueness, referential integrity, and source freshness.

## Run locally

1. Copy `.env.example` to `.env` and set local credentials and `SCHEME_CODES`.
2. Start the stack:

   ```bash
   docker compose up -d --build
   ```

3. Open Airflow at `http://localhost:8080`, trigger `mf_data_pipeline`, and inspect the mapped ingestion tasks.

4. Run dbt checks directly when needed:

   ```bash
   docker compose exec airflow bash -lc 'cd /opt/airflow/dbt_project && dbt build --profiles-dir .'
   docker compose exec airflow bash -lc 'cd /opt/airflow/dbt_project && dbt source freshness --profiles-dir .'
   ```

## Inspect results

```bash
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

Useful relations:

```text
bronze.raw_dump
control.ingestion_audit
analytics_staging.stg_mf_nav
analytics_marts.dim_fund
analytics_marts.fct_fund_performance
```

The `.env` file contains local secrets and is ignored by Git. Use a managed secret store for production deployments.