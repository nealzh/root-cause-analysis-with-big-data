# This script is to clean database for old records

from mu_f10ds_common.mssql import MSSQLUtil
from mu_f10ds_common.util import get_logger, get_now_str, get_now, format_datetime_string
from datetime import timedelta
import base64
import time
import argparse
import yaml


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get input from command line.')
    parser.add_argument('--config', action="store", dest='config', required=True, help='configuration file')

    user_input_config = parser.parse_args()
    config_file = user_input_config.config


    # Decide which configuration:
    try:
        with open(config_file, 'r') as stream:
            config = yaml.load(stream)
    except Exception as e:
        raise ValueError("Invalid configuration path")

    tz = config.get('tz')
    log_folders = config.get('folders')
    log_expiry_in_days = config.get('log_expiry_in_days')
    mssql_config = config.get('MSSQL')
    table_config = config.get('tables')

    mssql_server = mssql_config.get('server')
    mssql_user = mssql_config.get('user')
    mssql_password = base64.b64decode(mssql_config.get('password'))
    mssql_database = mssql_config.get('database')
    mssql_port = mssql_config.get('port')
    mssql_query_instance = MSSQLUtil(mssql_server, mssql_user, mssql_password, mssql_database, mssql_port)



    # Define logger
    logger = get_logger(tz=tz, identifier='database_clean')

    logger.info("#################################################")
    logger.info("=" * 40 + " log clean script starts " + "=" * 40)
    session_start_time = time.time()
    today_dt = get_now(tz=tz)
    expiry_date_str = str((today_dt - timedelta(days=log_expiry_in_days)).date())

    logger.info("Today is " + str(today_dt.date()))
    logger.info("Log before " + expiry_date_str + " will be expired.")



    for table, column in table_config.items():
        sql_templte = "DELETE FROM {table} where {column} < '{expiry_date}';"
        sql = sql_templte.format(table=table, column=column, expiry_date=expiry_date_str)
        mssql_query_instance.execute_sql(sql, execution_only=True)
        logger.info("table " + table + " is cleaned.")

    logger.info("-----------------Analysis Completes---------------")
    logger.info("Session total time takes --- %s seconds ---" % int(time.time() - session_start_time))
    logger.info("#################################################")
    logger.info("#################################################")

