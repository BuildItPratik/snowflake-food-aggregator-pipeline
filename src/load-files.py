from datetime import datetime
import os
from snowflake.snowpark import Session
import sys
import pytz
import logging


# Read the control-table
# Pick the last processed record
# add 1 to it
# and then move the file to the stage location.

# control table structure
# control_id
# run_counter start with 1 and will go upto 5
# processing_day 2024/12/21
# processing_hour 1,2 max (24)
# status (started, completed, failed)
# error_msg
# created ts and updated ts
# initiate logging at info level


logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(levelname)s - %(message)s')

# Set the IST time zone
ist_timezone = pytz.timezone('Asia/Kolkata')

# Get the current time in IST
current_time_ist = datetime.now(ist_timezone)

# Format the timestamp
timestamp = current_time_ist.strftime('%Y_%m_%d_%H_%M_%S')

today_string = current_time_ist.strftime('%Y_%m_%d')

#query to get the next run value
next_run_specification_sql ='''
                                WITH latest_processed AS (
                                SELECT 
                                    processing_day,
                                    processing_hour,
                                    run_counter,
                                    status
                                FROM common.control_tbl
                                WHERE 
                                    processing_day = (SELECT MAX(processing_day) FROM common.control_tbl)
                                    AND 
                                    (
                                        (run_counter < 3) 
                                        OR 
                                        (status in ('STARTED', 'FAILED') AND run_counter = 3)
                                    )
                                ORDER BY created_ts desc, processing_day DESC, processing_hour DESC
                                LIMIT 1
                            )
                            SELECT 
                                CASE 
                                    WHEN ((status = 'FAILED' OR status = 'STARTED')  and run_counter < 3) then processing_day
                                    WHEN processing_hour = 23 THEN DATEADD(DAY, 1, processing_day)
                                    ELSE processing_day
                                END AS next_processing_day,
                                CASE 
                                    WHEN ((status = 'FAILED' OR status = 'STARTED')  and run_counter < 3) then processing_hour
                                    WHEN processing_hour = 23 THEN 0
                                    ELSE processing_hour + 1
                                END AS next_processing_hour,
                                CASE 
                                    WHEN ((status = 'FAILED' OR status = 'STARTED')  and run_counter < 3) then run_counter+1
                                    ELSE 1
                                END AS next_run_counter
                            FROM latest_processed
                                '''

# Following credential has to come using secret whie running in automated way
def snowpark_basic_auth() -> Session:
    connection_parameters = {
       "ACCOUNT": os.getenv("ACCOUNT"),
        "USER": os.getenv("USER"),
        "PASSWORD": os.getenv("PASSWORD"),
        "ROLE": os.getenv("ROLE"),
        "DATABASE": os.getenv("DATABASE"),
        "SCHEMA": os.getenv("SCHEMA"),
        "WAREHOUSE": os.getenv("WAREHOUSE"),
    }
    # creating snowflake session object
    return Session.builder.configs(connection_parameters).create()


def get_next_run_specification(sf_session):
    """This function fetch the next run specification from the control table"""
    next_run_day = '2024/12/21'
    next_run_hour = 0
    next_run_counter = 1
    result_lst = sf_session.sql(next_run_specification_sql).collect()

    for element in result_lst:
        next_run_day = element[0]
        next_run_hour = element[1]
        next_run_counter = element[2]
        break

    return next_run_day,next_run_hour,next_run_counter



def main():
    """Main function to run the program."""
    base_location = os.path.abspath('data-files')
    # Input from the user
    sf_session = snowpark_basic_auth()
    next_run_day,next_run_hour,next_run_counter = get_next_run_specification(sf_session)
    next_run_hour_text = ''


    #build the file path based on next run specification
    if next_run_hour < 10:
        next_run_hour_text = f'0{str(next_run_hour)}-Hour'
    else:
        next_run_hour_text = f'{str(next_run_hour)}-Hour'

    #create an insert statement before copying the data
    insert_sql = f'''   insert into 
                        common.control_tbl 
                        (run_counter, processing_day, processing_hour, status)
                        values
                        ({next_run_counter},'{next_run_day}',{next_run_hour},'STARTED')

                        '''
    sf_session.sql(insert_sql).collect()

    order_file_name = f'{next_run_day}/{next_run_hour_text}/orders.csv'
    order_stg_location = f'@bigdata_db.stage_sch.csv_stg/orders/{next_run_day}/{next_run_hour_text}'
    order_item_file_name = f'{next_run_day}/{next_run_hour_text}/order-items.csv'
    order_item_stg_location = f'@bigdata_db.stage_sch.csv_stg/order-items/{next_run_day}/{next_run_hour_text}'
    delivery_file_name = f'{next_run_day}/{next_run_hour_text}/delivery.csv'
    delivery_stg_location = f'@bigdata_db.stage_sch.csv_stg/delivery/{next_run_day}/{next_run_hour_text}'

    logging.info('-----------------------')

    # pushing order file
    o_result = sf_session.file.put(f'{base_location}/{order_file_name}', order_stg_location,overwrite=True)
    logging.info('Order file placed successfully in stage location in snowflake')
    lst_query = f'list {order_stg_location}/orders.csv'
    logging.info(f'list query to fetch the stage file to check if they exist there or not = {lst_query}')
    result_lst = sf_session.sql(lst_query).collect()
    logging.info(f'File is placed in snowflake stage location= {result_lst}')
    logging.info('-----------------------1')

    # pushing order item file
    oi_result = sf_session.file.put(f'{base_location}/{order_item_file_name}', order_item_stg_location,overwrite=True)
    logging.info('Order Item File placed successfully in stage location in snowflake')
    lst_query = f'list {order_item_stg_location}/order-items.csv'
    logging.info(f'list query to fetch the stage file to check if they exist there or not = {lst_query}')
    result_lst = sf_session.sql(lst_query).collect()
    logging.info(f'File is placed in snowflake stage location= {result_lst}')
    logging.info('-----------------------2')

    # pushing order item file
    d_result = sf_session.file.put(f'{base_location}/{delivery_file_name}', delivery_stg_location,overwrite=True)
    logging.info('Delivery file placed successfully in stage location in snowflake')
    lst_query = f'list {delivery_stg_location}/delivery.csv'
    logging.info(f'list query to fetch the stage file to check if they exist there or not = {lst_query}')
    result_lst = sf_session.sql(lst_query).collect()
    logging.info(f'File is placed in snowflake stage location= {result_lst}')
    logging.info('-----------------------3')

    status_msg = ''

    for element in o_result:
        status_msg += f'Order File: {element[6]}/Name: {element[1]}/Size: {element[3]}, '
        break

    for element in oi_result:
        status_msg += f'Order Item File: {element[6]}/Name: {element[1]}/Size: {element[3]}, '
        break

    for element in d_result:
        status_msg += f'Order Item File: {element[6]}/Name: {element[1]}/Size: {element[3]}'
        break

    update_sql = f'''update 
                    common.control_tbl 
                    set status = 'COMPLETED',
                    updated_ts = current_timestamp(),
                    status_msg = '{status_msg}'
                    where 
                        run_counter = {next_run_counter} and 
                        processing_day = '{next_run_day}' and 
                        processing_hour = {next_run_hour} and 
                        status = 'STARTED'
                    '''

    #update the control table
    sf_session.sql(update_sql).collect()




# Ensure the program runs only when executed directly
if __name__ == "__main__":
    main()