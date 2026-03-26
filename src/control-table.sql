use role sysadmin;
use warehouse adhoc_wh;
use schema sp_pipeline_db.stage_sch;

CREATE or replace TABLE common.control_tbl (
    control_id int AUTOINCREMENT primary key,  -- Auto-incremented unique identifier
    run_counter INT default 1,      -- Run counter: 1 to 5
    processing_day DATE NOT NULL, -- Date of processing
    processing_hour INT NOT NULL , -- Hour of processing: 0-23
    status TEXT NOT NULL default 'STARTED', -- Status of the process
    status_msg TEXT default '', -- Error message in case of failure
    created_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Record creation timestamp
    updated_ts TIMESTAMP  -- Record update timestamp
);


INSERT INTO common.control_tbl (
    run_counter, processing_day, processing_hour, status
) VALUES (
    1,'2024-12-21', 0, 'COMPLETED'
);