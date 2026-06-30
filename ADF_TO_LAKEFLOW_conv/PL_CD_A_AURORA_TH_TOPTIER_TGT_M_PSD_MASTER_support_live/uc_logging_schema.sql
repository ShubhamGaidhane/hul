-- Design for Unity Catalog Logging Tables

CREATE SCHEMA IF NOT EXISTS etl_log;

-- Batch Table: Corresponds to uspBatchStartLog / uspBatchEndLog
CREATE TABLE IF NOT EXISTS etl_log.batch (
    batch_id BIGINT GENERATED ALWAYS AS IDENTITY,
    job_name STRING,
    run_id STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status STRING,
    slice_date_time TIMESTAMP,
    parent_batch_id BIGINT
) USING DELTA;

-- Batch Detail Table: Corresponds to uspBatchDetailStartLog / uspBatchDetailEndLog
-- Also stores execution logs (SourceRowCount, TargetRowCount, etc.)
CREATE TABLE IF NOT EXISTS etl_log.batch_detail (
    batch_detail_id BIGINT GENERATED ALWAYS AS IDENTITY,
    batch_id BIGINT,
    activity_name STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status STRING,
    source_name STRING,
    source_object STRING,
    target_object STRING,
    source_row_count INT,
    target_row_count INT,
    error_row_count INT,
    error_message STRING
) USING DELTA;
