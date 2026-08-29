CREATE SCHEMA IF NOT EXISTS bronze;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS bronze.raw_dump (
    raw_dump_id BIGSERIAL PRIMARY KEY,
    run_id VARCHAR(250) NOT NULL,
    scheme_code VARCHAR(20) NOT NULL,
    snapshot_hash CHAR(64) NOT NULL,
    raw_payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (scheme_code, snapshot_hash)
);
