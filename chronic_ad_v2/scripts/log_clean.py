# This script is to clean log files and report for old records

import sys
from mu_f10ds_common.util import get_logger, get_now_str, get_now, format_datetime_string
from datetime import timedelta
import glob
import os
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

    # Define logger
    logger = get_logger(tz=tz, identifier='log_clean')

    logger.info("#################################################")
    logger.info("=" * 40 + " log clean script starts " + "=" * 40)
    session_start_time = time.time()
    today_dt = get_now(tz=tz)
    expiry_date_str = str((today_dt - timedelta(days=log_expiry_in_days)).date())

    logger.info("Today is " + str(today_dt.date()))
    logger.info("Log before " + expiry_date_str + " will be expired.")

    for log_folder in log_folders:
        log_files = log_folder + '/*.log'
        logger.info("Going to check folder " + log_folder)
        all_log_files = glob.glob(log_files)
        for file_path in all_log_files:
            if file_path < (log_folder + expiry_date_str):
                try:
                    # os.remove(file_path)
                    logger.info("File is deleted. file: " + file_path)
                except Exception as e:
                    logger.error(str(e), exc_info=True)
                    logger.info("Error when deleting file: " + file_path)
        logger.info("folder " + log_folder + " check complete!")

    logger.info("-----------------Analysis Completes---------------")
    logger.info("Session total time takes --- %s seconds ---" % int(time.time() - session_start_time))
    logger.info("#################################################")
    logger.info("#################################################")

