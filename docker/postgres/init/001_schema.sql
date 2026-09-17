CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.dim_process (
    process_key CHAR(64) PRIMARY KEY,
    tightening_unit_id BIGINT NOT NULL,
    process_step_id BIGINT NOT NULL,
    process_step_name TEXT NOT NULL,
    substep_id BIGINT NOT NULL,
    substep_type BIGINT NOT NULL,
    factory_id BIGINT,
    factory_name TEXT,
    line_id BIGINT,
    line_name TEXT,
    work_area_id BIGINT,
    work_area_name TEXT,
    tightening_unit_name TEXT,
    process_step_number BIGINT,
    substep_number BIGINT,
    tool_id_primary BIGINT,
    tool_id_variant_count INTEGER NOT NULL DEFAULT 0 CHECK (tool_id_variant_count >= 0),
    tool_number_primary BIGINT,
    tool_number_variant_count INTEGER NOT NULL DEFAULT 0 CHECK (tool_number_variant_count >= 0),
    tool_serial_number_primary TEXT,
    tool_serial_number_variant_count INTEGER NOT NULL DEFAULT 0 CHECK (tool_serial_number_variant_count >= 0),
    target_torque DOUBLE PRECISION,
    min_torque DOUBLE PRECISION,
    max_torque DOUBLE PRECISION,
    has_torque_spec BOOLEAN NOT NULL,
    CONSTRAINT uq_dim_process_business_grain UNIQUE
      (tightening_unit_id, process_step_id, process_step_name, substep_id, substep_type)
);

CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key BIGINT PRIMARY KEY,
    local_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    iso_week INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name TEXT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.fact_process_daily (
    date_key BIGINT NOT NULL,
    process_key CHAR(64) NOT NULL,
    event_count BIGINT NOT NULL CHECK (event_count >= 0),
    torque_applicable_count BIGINT NOT NULL CHECK (torque_applicable_count >= 0),
    torque_not_applicable_count BIGINT NOT NULL CHECK (torque_not_applicable_count >= 0),
    torque_applicable_rate_pct DOUBLE PRECISION,
    torque_in_spec_count BIGINT NOT NULL CHECK (torque_in_spec_count >= 0),
    torque_out_of_spec_count BIGINT NOT NULL CHECK (torque_out_of_spec_count >= 0),
    torque_in_spec_rate_pct DOUBLE PRECISION,
    torque_out_of_spec_rate_pct DOUBLE PRECISION,
    applied_torque_mean DOUBLE PRECISION,
    applied_torque_median DOUBLE PRECISION,
    applied_torque_std DOUBLE PRECISION,
    applied_torque_min DOUBLE PRECISION,
    applied_torque_max DOUBLE PRECISION,
    mean_torque_delta_from_target DOUBLE PRECISION,
    mean_abs_torque_delta_from_target DOUBLE PRECISION,
    first_event_timestamp_utc TIMESTAMPTZ,
    last_event_timestamp_utc TIMESTAMPTZ,
    source_file_count BIGINT NOT NULL CHECK (source_file_count >= 0),
    PRIMARY KEY (date_key, process_key),
    FOREIGN KEY (date_key) REFERENCES analytics.dim_date(date_key),
    FOREIGN KEY (process_key) REFERENCES analytics.dim_process(process_key),
    CONSTRAINT ck_fact_torque_applicability CHECK
      (torque_applicable_count + torque_not_applicable_count = event_count),
    CONSTRAINT ck_fact_torque_conformity CHECK
      (torque_in_spec_count + torque_out_of_spec_count = torque_applicable_count)
);

CREATE TABLE IF NOT EXISTS analytics.etl_load_state (
    dataset TEXT NOT NULL,
    source_path TEXT NOT NULL,
    sha256 CHAR(64) NOT NULL,
    row_count BIGINT NOT NULL,
    event_count BIGINT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset, source_path)
);


CREATE TABLE IF NOT EXISTS analytics.streaming_events (
    result_id TEXT PRIMARY KEY,
    event_timestamp TIMESTAMPTZ NOT NULL,
    process_key CHAR(64) NOT NULL,
    factory_id BIGINT,
    factory_name TEXT,
    line_id BIGINT,
    line_name TEXT,
    work_area_id BIGINT,
    work_area_name TEXT,
    tightening_unit_id BIGINT NOT NULL,
    tightening_unit_name TEXT,
    process_step_id BIGINT NOT NULL,
    process_step_number BIGINT,
    process_step_name TEXT NOT NULL,
    substep_id BIGINT NOT NULL,
    substep_number BIGINT,
    substep_type BIGINT NOT NULL,
    tool_id BIGINT,
    tool_number BIGINT,
    tool_serial_number TEXT,
    target_torque DOUBLE PRECISION,
    min_torque DOUBLE PRECISION,
    max_torque DOUBLE PRECISION,
    applied_torque DOUBLE PRECISION,
    source_result_status TEXT,
    torque_applicable BOOLEAN NOT NULL,
    torque_spec_status TEXT NOT NULL CHECK (torque_spec_status IN ('IN_SPEC', 'OUT_OF_SPEC', 'NOT_APPLICABLE')),
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    kafka_timestamp TIMESTAMPTZ,
    stream_processing_timestamp TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_streaming_kafka_position UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE TABLE IF NOT EXISTS analytics.streaming_events_stage
(LIKE analytics.streaming_events INCLUDING DEFAULTS);

CREATE INDEX IF NOT EXISTS idx_fact_process_daily_process_key
    ON analytics.fact_process_daily(process_key);
CREATE INDEX IF NOT EXISTS idx_dim_process_line_tu
    ON analytics.dim_process(line_id, tightening_unit_id);
CREATE INDEX IF NOT EXISTS idx_dim_process_ps_step
    ON analytics.dim_process(process_step_id, substep_id);
CREATE INDEX IF NOT EXISTS idx_streaming_events_timestamp
    ON analytics.streaming_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_streaming_events_process_key
    ON analytics.streaming_events(process_key);

DROP VIEW IF EXISTS analytics.vw_process_daily_kpis;
CREATE VIEW analytics.vw_process_daily_kpis AS
WITH batch_rows AS (
    SELECT
        d.local_date,
        d.year,
        d.quarter,
        d.month,
        d.month_name,
        d.iso_week,
        d.day_name,
        p.process_key,
        p.factory_id,
        p.factory_name,
        p.line_id,
        p.line_name,
        p.work_area_id,
        p.work_area_name,
        p.tightening_unit_id,
        p.tightening_unit_name,
        p.process_step_id,
        p.process_step_number,
        p.process_step_name,
        p.substep_id,
        p.substep_number,
        p.substep_type,
        p.tool_id_primary,
        p.tool_number_primary,
        p.tool_serial_number_primary,
        p.target_torque,
        p.min_torque,
        p.max_torque,
        p.has_torque_spec,
        f.event_count,
        f.torque_applicable_count,
        f.torque_not_applicable_count,
        f.torque_applicable_rate_pct,
        f.torque_in_spec_count,
        f.torque_out_of_spec_count,
        f.torque_in_spec_rate_pct,
        f.torque_out_of_spec_rate_pct,
        f.applied_torque_mean,
        f.applied_torque_median,
        f.applied_torque_std,
        f.applied_torque_min,
        f.applied_torque_max,
        f.mean_torque_delta_from_target,
        f.mean_abs_torque_delta_from_target,
        f.first_event_timestamp_utc,
        f.last_event_timestamp_utc,
        f.source_file_count
    FROM analytics.fact_process_daily f
    JOIN analytics.dim_date d ON d.date_key = f.date_key
    JOIN analytics.dim_process p ON p.process_key = f.process_key
),
streaming_base AS (
    SELECT
        (event_timestamp AT TIME ZONE 'Europe/Madrid')::date AS local_date,
        *
    FROM analytics.streaming_events
),
streaming_rows AS (
    SELECT
        local_date,
        EXTRACT(YEAR FROM local_date)::INTEGER AS year,
        EXTRACT(QUARTER FROM local_date)::INTEGER AS quarter,
        EXTRACT(MONTH FROM local_date)::INTEGER AS month,
        TO_CHAR(local_date, 'FMMonth') AS month_name,
        EXTRACT(WEEK FROM local_date)::INTEGER AS iso_week,
        TO_CHAR(local_date, 'FMDay') AS day_name,
        process_key,
        MAX(factory_id) AS factory_id,
        MAX(factory_name) AS factory_name,
        MAX(line_id) AS line_id,
        MAX(line_name) AS line_name,
        MAX(work_area_id) AS work_area_id,
        MAX(work_area_name) AS work_area_name,
        MAX(tightening_unit_id) AS tightening_unit_id,
        MAX(tightening_unit_name) AS tightening_unit_name,
        MAX(process_step_id) AS process_step_id,
        MAX(process_step_number) AS process_step_number,
        MAX(process_step_name) AS process_step_name,
        MAX(substep_id) AS substep_id,
        MAX(substep_number) AS substep_number,
        MAX(substep_type) AS substep_type,
        MIN(tool_id) AS tool_id_primary,
        MIN(tool_number) AS tool_number_primary,
        MIN(tool_serial_number) AS tool_serial_number_primary,
        MAX(target_torque) AS target_torque,
        MAX(min_torque) AS min_torque,
        MAX(max_torque) AS max_torque,
        BOOL_OR(target_torque IS NOT NULL) AS has_torque_spec,
        COUNT(*)::BIGINT AS event_count,
        COUNT(*) FILTER (WHERE torque_applicable)::BIGINT AS torque_applicable_count,
        COUNT(*) FILTER (WHERE NOT torque_applicable)::BIGINT AS torque_not_applicable_count,
        COUNT(*) FILTER (WHERE torque_applicable)::DOUBLE PRECISION / NULLIF(COUNT(*), 0) * 100.0 AS torque_applicable_rate_pct,
        COUNT(*) FILTER (WHERE torque_spec_status = 'IN_SPEC')::BIGINT AS torque_in_spec_count,
        COUNT(*) FILTER (WHERE torque_spec_status = 'OUT_OF_SPEC')::BIGINT AS torque_out_of_spec_count,
        COUNT(*) FILTER (WHERE torque_spec_status = 'IN_SPEC')::DOUBLE PRECISION
            / NULLIF(COUNT(*) FILTER (WHERE torque_applicable), 0) * 100.0 AS torque_in_spec_rate_pct,
        COUNT(*) FILTER (WHERE torque_spec_status = 'OUT_OF_SPEC')::DOUBLE PRECISION
            / NULLIF(COUNT(*) FILTER (WHERE torque_applicable), 0) * 100.0 AS torque_out_of_spec_rate_pct,
        AVG(applied_torque) FILTER (WHERE torque_applicable) AS applied_torque_mean,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY applied_torque)
            FILTER (WHERE torque_applicable) AS applied_torque_median,
        STDDEV_SAMP(applied_torque) FILTER (WHERE torque_applicable) AS applied_torque_std,
        MIN(applied_torque) FILTER (WHERE torque_applicable) AS applied_torque_min,
        MAX(applied_torque) FILTER (WHERE torque_applicable) AS applied_torque_max,
        AVG(applied_torque - target_torque) FILTER (WHERE torque_applicable) AS mean_torque_delta_from_target,
        AVG(ABS(applied_torque - target_torque)) FILTER (WHERE torque_applicable) AS mean_abs_torque_delta_from_target,
        MIN(event_timestamp) AS first_event_timestamp_utc,
        MAX(event_timestamp) AS last_event_timestamp_utc,
        0::BIGINT AS source_file_count
    FROM streaming_base
    GROUP BY local_date, process_key
)
SELECT *, day_name AS day_of_week
FROM batch_rows

UNION ALL

SELECT *, day_name AS day_of_week
FROM streaming_rows;