CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.ingestion_audit (
    ingestion_id BIGSERIAL PRIMARY KEY,
    run_id VARCHAR(250) NOT NULL,
    scheme_code VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    nav_record_count INTEGER,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    snapshot_hash CHAR(64),
    inserted BOOLEAN,
    http_status INTEGER,
    response_bytes BIGINT,
    latest_nav_date DATE
);
