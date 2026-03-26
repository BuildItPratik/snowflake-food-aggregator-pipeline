import os
from datetime import datetime
from snowflake.snowpark import Session
import sys
import pytz
import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(levelname)s - %(message)s')

# IST Time
ist_timezone = pytz.timezone('Asia/Kolkata')
current_time_ist = datetime.now(ist_timezone)
timestamp = current_time_ist.strftime('%Y_%m_%d_%H_%M_%S')
today_string = current_time_ist.strftime('%Y_%m_%d')

# SQL
next_run_specification_sql = '''
WITH latest_processed AS (
    SELECT processing_day, processing_hour, run_counter, status
    FROM common.control_tbl
    WHERE processing_day = (SELECT MAX(processing_day) FROM common.control_tbl)
    AND ((run_counter < 3) OR (status in ('STARTED', 'FAILED') AND run_counter = 3))
    ORDER BY created_ts desc
    LIMIT 1
)
SELECT 
    CASE 
        WHEN ((status = 'FAILED' OR status = 'STARTED') AND run_counter < 3) THEN processing_day
        WHEN processing_hour = 23 THEN DATEADD(DAY, 1, processing_day)
        ELSE processing_day
    END,
    CASE 
        WHEN ((status = 'FAILED' OR status = 'STARTED') AND run_counter < 3) THEN processing_hour
        WHEN processing_hour = 23 THEN 0
        ELSE processing_hour + 1
    END,
    CASE 
        WHEN ((status = 'FAILED' OR status = 'STARTED') AND run_counter < 3) THEN run_counter + 1
        ELSE 1
    END
FROM latest_processed
'''

# 🔐 Secure connection using env variables
def snowpark_basic_auth() -> Session:
    return Session.builder.configs({
        "ACCOUNT": os.getenv("ACCOUNT"),
        "USER": os.getenv("USER"),
        "PASSWORD": os.getenv("PASSWORD"),
        "ROLE": os.getenv("ROLE"),
        "DATABASE": os.getenv("DATABASE"),
        "SCHEMA": os.getenv("SCHEMA"),
        "WAREHOUSE": os.getenv("WAREHOUSE"),
    }).create()


def get_next_run_specification(sf_session):
    result = sf_session.sql(next_run_specification_sql).collect()[0]
    return result[0], result[1], result[2]


def main():
    base_location = 'data-files'
    sf_session = snowpark_basic_auth()

    next_run_day, next_run_hour, next_run_counter = get_next_run_specification(sf_session)

    next_run_hour_text = f'{next_run_hour:02d}-Hour'

    try:
        # INSERT STARTED
        sf_session.sql(f"""
        INSERT INTO common.control_tbl 
        (run_counter, processing_day, processing_hour, status)
        VALUES ({next_run_counter}, '{next_run_day}', {next_run_hour}, 'STARTED')
        """).collect()

        # File paths
        order_file = f'{base_location}/{next_run_day}/{next_run_hour_text}/orders.csv'
        order_item_file = f'{base_location}/{next_run_day}/{next_run_hour_text}/order-item.csv'
        delivery_file = f'{base_location}/{next_run_day}/{next_run_hour_text}/delivery.csv'

        order_stage = f'@bigdata_db.stage_sch.csv_stg/orders/{next_run_day}/{next_run_hour_text}'
        oi_stage = f'@bigdata_db.stage_sch.csv_stg/order-items/{next_run_day}/{next_run_hour_text}'
        d_stage = f'@bigdata_db.stage_sch.csv_stg/delivery/{next_run_day}/{next_run_hour_text}'

        # Upload files
        o_res = sf_session.file.put(order_file, order_stage, overwrite=True)
        oi_res = sf_session.file.put(order_item_file, oi_stage, overwrite=True)
        d_res = sf_session.file.put(delivery_file, d_stage, overwrite=True)

        logging.info("Files uploaded successfully")

        # Build status msg
        status_msg = f"""
        Orders: {o_res[0][1]},
        OrderItems: {oi_res[0][1]},
        Delivery: {d_res[0][1]}
        """

        # UPDATE SUCCESS
        sf_session.sql(f"""
        UPDATE common.control_tbl 
        SET status = 'COMPLETED',
            updated_ts = current_timestamp(),
            status_msg = '{status_msg}'
        WHERE run_counter = {next_run_counter}
          AND processing_day = '{next_run_day}'
          AND processing_hour = {next_run_hour}
          AND status = 'STARTED'
        """).collect()

    except Exception as e:
        logging.error(f"Error: {str(e)}")

        # UPDATE FAILURE
        sf_session.sql(f"""
        UPDATE common.control_tbl 
        SET status = 'FAILED',
            updated_ts = current_timestamp(),
            error_msg = '{str(e)}'
        WHERE run_counter = {next_run_counter}
          AND processing_day = '{next_run_day}'
          AND processing_hour = {next_run_hour}
        """).collect()

        raise e


if __name__ == "__main__":
    main()