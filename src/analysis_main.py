import warnings
warnings.filterwarnings("ignore")
import argparse
import time
import pandas as pd
from orion_msg_parser import *
from datetime import datetime as dt
from mu_f10ds_common.hbase import HBaseUtil
from mu_f10ds_common.util import read_file, get_now_str, merge_two_dicts
from mu_f10ds_common.mssql import MSSQLUtil
from mu_f10ds_sigma.sigma_query import *
from mu_f10ds_space.space_query import *
from mu_f10ds_swr.swr import *
from mu_f10ds_qdr.qdr import *
from mu_f10ds_lotattribute.lot_attribute import *
from mu_f10ds_pmcm.pmcm import *
from mu_f10ds_test_trv.test_trv import *
from update_tracking import *
from conf import domain_knowledge, region_mapping
from scipy.stats import pearsonr
from trending_check import TrendingCheck
from functions import *
import yaml
import json
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', -1)
pd.set_option('display.precision', 2)

if __name__ == "__main__":
    # Get Input from command line
    parser = argparse.ArgumentParser(description='Get input from command line.')
    parser.add_argument('--fab', action="store", dest='fab', type=int, required=True, help='fab number, eg, 10, 7...')
    parser.add_argument('--config', action="store", dest='config', required=True, help='configuration file')
    parser.add_argument('--mode', action="store", dest='mode', required=True,
                        choices=['daily', 'single', 'daily_test', 'orion'], help='running mode, eg, all or single')
    parser.add_argument('--save_data', action="store_true", dest='save_data', required=False, default=False,
                        help='disable or enable save_data. ')
    parser.add_argument('--no_tracking', action="store_true", dest='no_tracking', required=False, default=False,
                        help='disable or enable save_data. ')
    parser.add_argument('--debug', action="store_true", dest='debug', required=False, default=False,
                        help='disable or enable debug mode in logging. ')
    # Below is required for single/daily_test mode
    parser.add_argument('--channel_id', action="store", dest='channel_id', required=False,default="",
                        help='channel_id used for running single model , eg, 705426')
    parser.add_argument('--ckc_id', action="store", dest='ckc_id', required=False,default="",
                        help='ckc_id used for running single model , eg, 0')
    parser.add_argument('--query_session', action="store", dest='query_session', required=False,default="",
                        help='query_session for running daily_test model , eg, 2018-08-07 20:00:00')
    # Below is required for daily mode
    parser.add_argument('--min_query_date', action="store", dest='min_query_date', required=False,default="",
                        help='Userd for running daily model , eg, 2018-08-07')

    parser.add_argument('--instance_id', action="store", dest='instance_id', required=False, default="",
                        help='ORION instance_id')

    parser.add_argument('--lot_id', action="store", dest='lot_id', required=False, default="",
                        help='ORION lot_id')
    parser.add_argument('--parameter', action="store", dest='parameter', required=False, default="",
                        help='ORION parameter')
    parser.add_argument('--violation', action="store", dest='violation', required=False, default="",
                        help='ORION parameter')
    parser.add_argument('--rowkey', action="store", dest='tracking_rowkey', required=False, default="",
                        help='ORION parameter')
    parser.add_argument('--ad_result_ws_url', action="store", dest='http_call_url', required=False, default="",
                        help='web service URL to insert ad_result')

    user_input_config = parser.parse_args()
    fab = user_input_config.fab
    config_file = user_input_config.config
    save_data_flag = user_input_config.save_data
    verbose = user_input_config.debug
    mode = user_input_config.mode
    channel_id = user_input_config.channel_id
    ckc_id = user_input_config.ckc_id
    query_session = user_input_config.query_session
    min_query_date = user_input_config.min_query_date
    no_tracking_flag = user_input_config.no_tracking
    instance_id = user_input_config.instance_id
    lot_id = user_input_config.lot_id
    parameter = user_input_config.parameter
    violation = user_input_config.violation
    tracking_rowkey = user_input_config.tracking_rowkey
    http_call_url = user_input_config.http_call_url    

    # Check whether input requirements are met
    if mode == 'daily':
        if min_query_date == "":
            raise ValueError("Input missed for min_query_date. Current input is => " + min_query_date + " <= ")
    elif (mode == 'daily_test') | (mode == 'single'):
        if channel_id == "":
            raise ValueError("Input missed for ch_id. Current input is => " + channel_id + " <= ")
        if ckc_id == "":
            raise ValueError("Input missed for ckc_id. Current input is => " + ckc_id + " <= ")
        if query_session == "":
            raise ValueError("Input missed for query_session. Current input is => " + query_session + " <= ")
    elif mode == 'orion':
        if tracking_rowkey == "":
            if instance_id == "":
                raise ValueError("Input missed for instance_id. Current input is => " + instance_id + " <= ")
            if channel_id == "":
                raise ValueError("Input missed for ch_id. Current input is => " + channel_id + " <= ")
            if ckc_id == "":
                raise ValueError("Input missed for ckc_id. Current input is => " + ckc_id + " <= ")
            if lot_id == "":
                raise ValueError("Input missed for lot_id. Current input is => " + lot_id + " <= ")
            if parameter == "":
                raise ValueError("Input missed for parameter. Current input is => " + parameter + " <= ")
            if violation == "":
                raise ValueError("Input missed for violation. Current input is => " + violation + " <= ")
        else:
            tracking_rowkey_list = tracking_rowkey.split('||')
            instance_id = str(tracking_rowkey_list[0])[::-1]
            channel_id = tracking_rowkey_list[1]
            ckc_id = tracking_rowkey_list[2]
            lot_id = tracking_rowkey_list[3]
            parameter = tracking_rowkey_list[4]
            violation = tracking_rowkey_list[5]
    else:
        raise ValueError("Mode is not supported. Supported Mode is ['daily', 'single', 'daily_test', 'orion']" +
                         "Current mode is => " + mode + " <= ")

    # Decide which configuration:
    try:
        with open(config_file, 'r') as stream:
            config = yaml.load(stream)
    except Exception as e:
        raise ValueError("Invalid configuration path")

    # Parse all the configurations:
    COLUMN_DELIMITER = "::"
    tz = config.get('tz')
    datetime_standard_format = config.get('datetime_standard_format')
    log_relative_dir = config.get('log_relative_dir')
    # Tracking configuration
    tracking_db = config.get('tracking_db')
    tracking_tbl = config.get('tracking_tbl', "")
    upon_error_update_result = config.get('upon_error_update_result', False)
    analysis_type = config.get('analysis_type')
    HBASE_CONFIG = config.get('HBASE')
    SIGMA_CONFIG = config.get('SIGMA')
    TERADATA_CONFIG = config.get('TERADATA')
    ANALYSIS_CONFIG = config.get('ANALYSIS')
    REPORT_CONFIG = config.get('REPORT')
    MSSQL_CONFIG = config.get('MSSQL')
    SPARK_CONFIG = config.get('SPARK')
    FILTER_CONFIG = config.get('FILTER')

    # Initialize logger
    logger = get_logger(tz, debug=verbose, log_relative_dir=log_relative_dir)
    logger.info("####################"*4)
    logger.info("=" * 40 + " script starts " + "=" * 40)
    logger.info("Analysis mode is " + mode)
    logger.info("Analysis type is " + analysis_type)
    session_start_time = time.time()
    final_status_prefix = ""

    # HBase Configuration
    cluster_type = HBASE_CONFIG.get('cluster_type')
    hbase_host = HBASE_CONFIG.get('host')
    hbase_port = HBASE_CONFIG.get('port')
    # spark configuration
    spark_config = SPARK_CONFIG
    # Filter Configuration
    filter_config = FILTER_CONFIG
    vio_type_list_csv = filter_config.get('vio_type_list_csv')
    space_sample_period_in_hours = int(filter_config.get('space_sample_period_in_hours'))
    # Sigma Configuration
    step_prefix = SIGMA_CONFIG.get('step_prefix')
    columns = SIGMA_CONFIG.get('columns')
    all_columns = {}
    # merge columns together
    for k, v in columns.get('keep').items():
        all_columns[k] = columns.get('keep').get(k)
        if k in columns.get('non_keep').keys():
            all_columns[k] = merge_two_dicts(all_columns[k], columns.get('non_keep').get(k))
    columns_to_remove = []
    for k, v in columns.get('non_keep').items():
        # print(v)
        for k2, v2 in v.items():
            columns_to_remove.append(v2)

    # Teradata Configuration
    teradata_config = TERADATA_CONFIG
    if teradata_config is not None:
        teradata_config['password'] = base64.b64decode(teradata_config['password'])

    pm_cm_config = ANALYSIS_CONFIG.get('pm_cm')
    feedback_check_days = ANALYSIS_CONFIG.get('feedback').get('feedback_check_days')
    channel_level_analysis = ANALYSIS_CONFIG.get('channel_level_analysis')
    trending_check_config = ANALYSIS_CONFIG.get('trending_check')
    # Analysis block configuration
    analysis_block_list = ANALYSIS_CONFIG.get('analysis_blocks', [])

    # Report Configuration
    report_config = REPORT_CONFIG
    # Analysis Configuration
    fmea_config = ANALYSIS_CONFIG.get('fmea')
    chart_filter_config = ANALYSIS_CONFIG.get('chart_filter')
    save_root_cause_flag = ANALYSIS_CONFIG.get('result').get('save_root_cause')
    save_root_cause_as_http_call_flag = ANALYSIS_CONFIG.get('result').get('save_root_cause_as_http_call')
    #http_call_url = ANALYSIS_CONFIG.get('result').get('http_call_url')
    last_ooc_number = ANALYSIS_CONFIG.get('last_ooc_number')
    # MSSQL Configuration
    mssql_config = MSSQL_CONFIG


    if mssql_config is not None:
        tracking_table_name = mssql_config.get('tracking_table_name')
        # Samples table for AD
        samples_table_name = mssql_config.get('samples_table_name')
        # Feedback table for AD
        feedback_table_name = mssql_config.get('feedback_table_name')
        # Root Cause table for AD
        root_cause_table_name = mssql_config.get('root_cause_table_name')
        # ch_folder_table_name
        ch_folder_table_name = mssql_config.get('ch_folder_table_name')
        plots_table_name = mssql_config.get('plots_table_name')

        insert_result_flag = mssql_config.get('insert_result', False)
        mssql_server = mssql_config.get('server')
        mssql_user = mssql_config.get('user')
        mssql_password = base64.b64decode(mssql_config.get('password'))
        mssql_database = mssql_config.get('database')
        mssql_port = mssql_config.get('port')
        mssql_query_instance = MSSQLUtil(mssql_server, mssql_user, mssql_password, mssql_database, mssql_port)

    file_dir = os.path.dirname(os.path.abspath(__file__))
    # Decide which charts to analyze base on Mode

    # if mode is daily, load configuration file and run daily AD analysis based on charts from MSSQL database
    if mode == 'daily':
        # Query Charts from MSSQL database
        sql_template = util.read_file(file_dir + '/../sql/samples_query_template.sql')
        sql = sql_template.format(tracking_table_name=tracking_table_name, samples_table_name=samples_table_name,
                                  ch_folder_table_name=ch_folder_table_name,
                                  fab=fab, analysis_type=analysis_type, min_query_date=min_query_date)
        space_df = mssql_query_instance.execute_sql(sql, execution_only=False)
        logger.info("SPACE dataframe size is " + str(space_df.shape[0]))
        space_df_channel = space_df

    # if mode is orion, load configuration file and run ORION parser
    elif mode == 'orion':
        # no_tracking_flag = True
        tracking_tbl = config.get('tracking_tbl').format(fab=fab, region=region_mapping.get(fab))
        rowkey = "||".join([str(instance_id)[::-1], str(channel_id), str(ckc_id),
                            str(lot_id), str(parameter), str(violation)])
        msg_columns = {
            'cf:measurement_stepname': 'measurement_step',
            'cf:instance_id': 'instance_id',
            'cf:updated_datetime': 'updated_datetime',
            'cf:chart_violation_type': 'chart_violation_type',
            'cf:parameter_name': 'parameter_name',
            'cf:violation_step_info': 'violation_step_info',
            'cf:process_stepname': 'process_step',
            'cf:ecap_datetime': 'ecap_datetime',
            'cf:channel_id': 'channel_id',
            'cf:sample_tool_info': 'sample_tool_info',
            'cf:lot_id': 'lot_id',
            'cf:ckc_id': 'ckc_id',
            'cf:area': 'area',
            'cf:sample_detail_info': 'sample_detail_info',
            'cf:sigma_data_datetime': 'sigma_data_datetime'
        }

        hbase_instance = HBaseUtil(host=hbase_host, port=hbase_port,
                                   table=tracking_tbl,
                                   tz=tz)
        orion_result_instance = OrionResultUpdating(instance_id=instance_id,
                                                    post_url=http_call_url,
                                                    tz=tz)
        df = hbase_instance.get(rowkey=rowkey, columns=msg_columns)
        logger.info("Rowkey is " + rowkey)

        hbase_instance.put(rowkey, data={'received_ad_datetime': get_now_str(tz)})
        if df.shape[0] < 1:
            logger.info("No tracking information is available.")
            final_status_prefix = final_status_prefix + "No Tracking Data."
            hbase_instance.put(rowkey, data={'ad_completed': str(1)})
            hbase_instance.put(rowkey, data={'final_status': final_status_prefix})

        chart_violation_type = df[df['column'] == 'chart_violation_type']['value'].iloc[0]
        input_xml_str1 = df[df['column'] == 'violation_step_info']['value'].iloc[0]
        input_xml_str2 = df[df['column'] == 'sample_tool_info']['value'].iloc[0]
        input_xml_str3 = df[df['column'] == 'sample_detail_info']['value'].iloc[0]
        input_xml_str = input_xml_str1 + input_xml_str2 + input_xml_str3
        d = parser_space_xml(input_xml_str)
        if len(d) < 1:
            logger.error("Error parsing the msg.")
            final_status_prefix = final_status_prefix + "No SPACE Data."
            orion_result_instance.root_cause = final_status_prefix
            status_code = orion_result_instance.post_json()
            if status_code != 200:
                logger.error("http call update response code is " + str(status_code))
                sys.exit(0)

            hbase_instance.put(rowkey, data={'final_status': final_status_prefix})
            hbase_instance.put(rowkey, data={'ad_completed': str(1)})
            logger.info(final_status_prefix)
            sys.exit(0)

        space_df = get_final_df(d)
        space_df_channel = space_df
        chart_violation_type = [int(x.split(':')[0]) for x in chart_violation_type.split(';')]
        if 3 in chart_violation_type or 4 in chart_violation_type:
            chart_type = 'Mean'
        elif 5 in chart_violation_type or 17 in chart_violation_type:
            chart_type = 'Sigma'
        elif 6 in chart_violation_type or 18 in chart_violation_type:
            chart_type = 'Range'
        elif 11 in chart_violation_type or 12 in chart_violation_type:
            chart_type = 'EWMA-Mean'
        else:
            logger.info("Chart type is not supported.")
            final_status = final_status_prefix + "Chart type is not supported."

            orion_result_instance.root_cause = final_status
            status_code = orion_result_instance.post_json()
            if status_code != 200:
                logger.error("http call update response code is " + str(status_code))
                sys.exit(0)

            hbase_instance.put(rowkey, data={'ad_completed': str(1)})
            hbase_instance.put(rowkey, data={'final_status': final_status})
            logger.info(final_status)
            sys.exit(0)

        space_df['fab'] = fab
        space_df['ch_name'] = ""
        space_df['chart_type'] = chart_type
        space_df['instance_id'] = int(instance_id)
        space_df['query_session'] = df[df['column'] == 'ecap_datetime']['value'].iloc[0][0:19]
        space_df['ch_id'] = space_df['channel_id'] + '_' + space_df['ckc_id']
        space_df['module'] = df[df['column'] == 'area']['value'].iloc[0]
        space_df['channel_type'] = ""
        space_df_channel = space_df

    # if mode is daily test, load configuration file and
    # run single chart AD analysis based on charts from MSSQL database
    elif mode == 'daily_test':
        ch_id = str(channel_id) + "_" + str(ckc_id)
        # Query Charts from MSSQL database
        sql_template = util.read_file(file_dir + '/../sql/single_chart_samples_query_template.sql')
        sql = sql_template.format(tracking_table_name=tracking_table_name, samples_table_name=samples_table_name,
                                  fab=fab, ch_id=ch_id, ch_folder_table_name=ch_folder_table_name,
                                  analysis_type=analysis_type, query_session=query_session)
        space_df = mssql_query_instance.execute_sql(sql, execution_only=False)
        logger.info("SPACE dataframe size is " + str(space_df.shape[0]))
        space_df_channel = space_df

    # if mode is single, load configuration file and run single chart AD analysis based on charts from SPACE database
    elif mode == 'single':
        no_tracking_flag = True
        ch_id = str(channel_id) + "_" + str(ckc_id)
        space_query_instance = SpaceQuery(fab=fab, spark_config=spark_config)
        end_sample_date = dt.strptime(query_session, choose_datetime_standard_format(query_session))
        start_sample_date = end_sample_date - timedelta(hours=space_sample_period_in_hours)
        ch_ckc_list = [ch_id]
        area_list = []
        latest_ooc_min_count = 0
        ooc_sample_num_check_in_hours = int((end_sample_date - start_sample_date).total_seconds()/60) + 1
        space_df = space_query_instance.space_query_ooc_with_good(from_datetime=start_sample_date,
                                                                  to_datetime=end_sample_date,
                                                                  vio_type_list_csv=vio_type_list_csv,
                                                                  ch_ckc_list=ch_ckc_list,
                                                                  area_condition_str=area_list,
                                                                  latest_ooc_min_count=latest_ooc_min_count,
                                                                  start_date_latest_ooc_in_hours=
                                                                  ooc_sample_num_check_in_hours)
        space_df['query_session'] = end_sample_date
        ch_name_sql = """SELECT ch_name 
                         FROM {ch_folder_table_name} 
                         WHERE fab = {fab} and ch_id = {ch_id};"""
        ch_name = mssql_query_instance.execute_sql(ch_name_sql.format(
                                            ch_folder_table_name=ch_folder_table_name,
                                            fab=fab,
                                            ch_id=int(channel_id)), execution_only=False)
        if ch_name.shape[0] > 0:
            ch_name_str = ch_name['ch_name'].iloc[0]
        else:
            ch_name_str =""


        space_df['ch_name'] = ch_name_str

        logger.info("Channel name is " + ch_name_str)
        logger.info("SPACE dataframe size is " + str(space_df.shape[0]))

        if channel_level_analysis and ckc_id > 0:
            main_ch_ckc_id = str(channel_id) + "_" + str(0)
            space_df_channel = space_query_instance.space_query_ooc_with_good(from_datetime=start_sample_date,
                                                                              to_datetime=end_sample_date,
                                                                              vio_type_list_csv=vio_type_list_csv,
                                                                              ch_ckc_list=[main_ch_ckc_id, ch_id],
                                                                              area_condition_str=area_list,
                                                                              latest_ooc_min_count=latest_ooc_min_count,
                                                                              start_date_latest_ooc_in_hours=
                                                                              ooc_sample_num_check_in_hours)
            space_df_channel['query_session'] = end_sample_date

            space_df_channel['ch_name'] = ch_name_str
            space_df_channel = space_df_channel[space_df_channel['ch_id'] == main_ch_ckc_id].reset_index(drop=True)
            logger.info("SPACE data frame size for channel is " + ch_name_str)
        else:
            space_df_channel = space_df
    else:
        raise ValueError("Mode is not supported. Supported Mode is ['daily', 'single', 'daily_test', 'orion']" +
                         "Current mode is => " + mode + " <= ")

    if space_df.shape[0] < 0:
        logger.info("No charts to analysis.")
        logger.info("-----------------Analysis Completes---------------")
        logger.info("Session total time takes --- %s seconds ---" % int(time.time() - session_start_time))
        logger.info("####################" * 4)
        sys.exit(0)

    # Filter Chart based on required
    if (chart_filter_config is not None) & (space_df.shape[0] > 0):
        for key, value in chart_filter_config.items():
            space_df = space_df[space_df[key] != value]

        space_df = space_df.reset_index(drop=True)

    # Read FMEA Steps
    FMEA_df = pd.read_csv(file_dir + '/../reference/' + fmea_config['fmea_file_name'].format(fab=fab),
                          header=None, low_memory=False)
    FMEA_df.columns = ['measurement_step', 'process_step', 'channel_id']
    FMEA_df['channel_id'] = FMEA_df['channel_id'].astype('int')
    # Check Chart Type
    df_list = [x for _, x in space_df.groupby(['fab', 'ch_id', 'chart_type', 'query_session', 'current_step'])]
    logger.info("Number of chart to analyze is " + str(len(df_list)))
    final_status_prefix = final_status_prefix + "Space query complete. "

    # Loop to analysing chart
    for single_chart_df in df_list:
        chart_start_time = time.time()
        log_prefix = ""
        final_status = final_status_prefix
        tracking_instance = None
        logger.info("This chart sample size is " + str(single_chart_df.shape[0]))
        try:
            if single_chart_df.shape[0] < 1:
                logger.info(log_prefix + "Total time takes --- %s seconds ---" % int(time.time() - chart_start_time))
                logger.error(log_prefix + "No sample data is available.")
                final_status = final_status + " Not space data."

                # tracking_instance.update_tracking_single_column('status', final_status)
                continue
            y_min = single_chart_df['value'].min()
            y_max = single_chart_df['value'].max()
            y_lim = (y_min - (y_max - y_min) * 0.1, y_max + (y_max - y_min) * 0.1)

            single_chart_df = single_chart_df.sort_values('sample_date', ascending=True).reset_index(drop=True)

            area = str(single_chart_df['module'].iloc[0])
            if isinstance(single_chart_df['query_session'].iloc[0], dt):
                query_session_dt = single_chart_df['query_session'].iloc[0]
                query_session_str = dt.strftime(single_chart_df['query_session'].iloc[0], datetime_standard_format)
            elif isinstance(single_chart_df['query_session'].iloc[0], str) | \
                    isinstance(single_chart_df['query_session'].iloc[0], unicode):
                query_session_str = single_chart_df['query_session'].iloc[0]
                query_session_dt = dt.strptime(query_session_str, choose_datetime_standard_format(query_session_str))
            else:
                raise ValueError("Query session format is wrong.")

            query_session = query_session_str
            today_str = str(query_session)[0:10]
            if 'instance_id' in single_chart_df.columns:
                instance_id = int(single_chart_df['instance_id'].iloc[0])
            else:
                instance_id = int(dt.strftime(single_chart_df['query_session'].iloc[0], "%Y%m%d%H"))
            channel_id = int(single_chart_df['ch_id'].iloc[0].split('_')[0])
            ch_name = str(single_chart_df['ch_name'].iloc[0])
            ckc_id = int(single_chart_df['ch_id'].iloc[0].split('_')[1])
            design_id = str(single_chart_df['design_id'].iloc[0])
            channel_type = str(single_chart_df['channel_type'].iloc[0])
            measurement_step = str(single_chart_df['current_step'].iloc[0])
            process_step = str(single_chart_df['process_step'].iloc[0])
            parameter = str(single_chart_df['parameter_name'].iloc[0])
            current_loop = get_loop_id(measurement_step)
            chart_type = str(single_chart_df['chart_type'].iloc[0])
            lcl = single_chart_df.tail(1)['lcl'].iloc[0]
            ucl = single_chart_df.tail(1)['ucl'].iloc[0]
            log_prefix = "_".join([str(fab), str(channel_id), str(ckc_id), str(chart_type), str(instance_id)]) + " :: "
            ch_ckc_id = single_chart_df['ch_id'].iloc[0]
            fb_min_query_date = dt.strftime(query_session_dt - timedelta(days=feedback_check_days),
                                            datetime_standard_format)
            qual_wafer_mapping_hidden = 'style=\"display:none;\"'
            qual_wafer_mapping_file_link = ""
            num_of_ooc = single_chart_df[single_chart_df['label'] == 1].shape[0]
            logger.debug(log_df(single_chart_df[single_chart_df['label'] == 1]))
            num_of_normal = single_chart_df[single_chart_df['label'] == 0].shape[0]
            logger.info(log_prefix + "Number of ooc is " + str(num_of_ooc))
            logger.info(log_prefix + "Number of normal is " + str(num_of_normal))

            if tracking_db == 'mssql':
                tracking_instance = SQLUpdateTracking(analysis_type=analysis_type, fab=fab, tz=tz, ch_id=ch_ckc_id,
                                                      query_session=query_session, chart_type=chart_type,
                                                      mssql_conn=mssql_query_instance,
                                                      tracking_table_name=tracking_table_name,
                                                      no_tracking_flag=no_tracking_flag)

            elif tracking_db == 'hbase':
                tracking_tbl = tracking_tbl.format(fab=fab, region=region_mapping.get(int(fab)))
                hbase_instance = hbase.HBaseUtil(host=hbase_host, port=hbase_port, table=tracking_tbl, tz=tz)
                tracking_instance = HBaseUpdateTracking(tz=tz, hbase_instance=hbase_instance, rowkey=rowkey,
                                                    no_tracking_flag=no_tracking_flag)
            else:
                raise ValueError("tracking_db is not supported. Supported tracking_db is ['mssql', 'hbase']" +
                                 "Current tracking_db is => " + tracking_db + " <= ")

            # Update Tracking about received
            tracking_instance.update_tracking_single_column('received', get_now_str(tz))
            # Get HBase Connection

            if num_of_ooc < 2:
                logger.info(log_prefix + "Total time takes --- %s seconds ---" % int(time.time() - chart_start_time))
                logger.error(log_prefix + "Not enough OOC points.")
                final_status = final_status + " Not enough OOC points."

                if mode == 'orion':
                    orion_result_instance.root_cause = final_status
                    status_code = orion_result_instance.post_json()
                    if status_code != 200:
                        logger.error(log_prefix + "http call update response code is " + str(status_code))
                tracking_instance.update_tracking_single_column('status', final_status, ad_complete=True)
                continue

            if num_of_normal < 2:
                logger.info(log_prefix + "Total time takes --- %s seconds ---" % int(time.time() - chart_start_time))
                logger.error(log_prefix + "Not enough good points.")
                final_status = final_status + " Not enough good points."
                if mode == 'orion':
                    orion_result_instance.root_cause = final_status
                    status_code = orion_result_instance.post_json()
                    if status_code != 200:
                        logger.error(log_prefix + "http call update response code is " + str(status_code))

                tracking_instance.update_tracking_single_column('status', final_status, ad_complete=True)
                continue

            # Channel Level Analysis Query
            single_chart_ckc_df = single_chart_df
            if channel_level_analysis:
                logger.info(log_prefix + "Channel Level analysis is enabled.")
                if ckc_id > 0:
                    logger.info(log_prefix + "Query of main channel is required.")
                    if mode == 'daily' or mode == 'daily_test':
                        main_ch_ckc_id = str(channel_id) + "_" + str(0)
                        # Query Charts from MSSQL database
                        sql_template = util.read_file(file_dir + '/../sql/single_chart_channel_samples_query_template.sql')
                        sql = sql_template.format(samples_table_name=samples_table_name,
                                                  ch_folder_table_name=ch_folder_table_name,
                                                  fab=fab, analysis_type=analysis_type,
                                                  query_session=query_session, chart_type=chart_type,
                                                  ch_id=main_ch_ckc_id)
                        single_chart_df = mssql_query_instance.execute_sql(sql, execution_only=False)

                        if single_chart_df.shape[0] <= single_chart_ckc_df.shape[0]:
                            logger.info(log_prefix + "Total time takes --- %s seconds ---" % int(
                                time.time() - chart_start_time))
                            logger.info(log_prefix + "This chart sample size for ckc level analysis is "
                                        + str(single_chart_ckc_df.shape[0]))
                            logger.info(log_prefix + "This chart sample size for channel level analysis is "
                                        + str(single_chart_df.shape[0]))
                            logger.error(log_prefix + "Channel data size is same as or less than ckc data size. "
                                                      "Suspect data missing for channel level.")
                            final_status = final_status + " channel missing data."
                            tracking_instance.update_tracking_single_column('status', final_status, ad_complete=True)
                            continue


                        single_chart_df = single_chart_df.sort_values('sample_date', ascending=True)\
                                            .tail(1500)\
                                            .reset_index(drop=True)

                    elif mode == 'single':
                        single_chart_df = space_df_channel[space_df_channel['chart_type'] == chart_type].copy()
                        single_chart_df = single_chart_df.sort_values('sample_date', ascending=True)\
                                            .tail(1500)\
                                            .reset_index(drop=True)
                    else:
                        pass

                    logger.info(log_prefix + "This chart sample size for channel level analysis is "
                                + str(single_chart_df.shape[0]))

                else:
                    logger.info(log_prefix + "ckc id is " + str(ckc_id) + ". No need to query data")
            else:
                logger.info(log_prefix + "Channel Level analysis is disabled.")

            try:
                # Check how many point got wafer id
                if single_chart_df[single_chart_df['wafer_id'].str.len() == 7].shape[0] >= (0.9*single_chart_df.shape[0]):
                    chart_level = 'wafer'
                else:
                    chart_level = 'lot'

                single_chart_df_merge = single_chart_df
                test_trv_step_list = []
                test_trv_df_trim = pd.DataFrame()
                test_trv_df = pd.DataFrame()
                # Check whether wafer level qual chart
                test_trv_flag = False
                if (chart_level == 'wafer') & ('qual' in ch_name.lower()):
                    try:
                        test_trv_start_time = time.time()
                        lot_list = list(single_chart_df['lot_id'])
                        test_trv_instance = TestTravelerQuery(fab=fab, lot_list=lot_list, current_step=measurement_step,
                                                              query_session=query_session, teradata_config=teradata_config)

                        test_trv_df = test_trv_instance.test_trv()
                        logger.info(log_prefix + "Test traveller Query takes --- %s seconds ---" % int(
                            time.time() - test_trv_start_time))
                        # Merge test_trv_df into space df
                        single_chart_df_cp = single_chart_df.copy()
                        test_trv_df_trim = test_trv_df[['lot_id', 'wafer_id', 'from_lot_id',
                                                        'from_wafer_id', 'wafer_scribe']] \
                            .drop_duplicates() \
                            .reset_index(drop=True)
                        single_chart_df_cp = single_chart_df_cp.merge(test_trv_df_trim, on=['lot_id', 'wafer_id'],
                                                                      how='inner')
                        if single_chart_df_cp.shape[0] > 0:
                            single_chart_df_cp = single_chart_df_cp.drop(['lot_id', 'wafer_id'],
                                                                         errors='ignore', axis=1)
                            single_chart_df_cp = single_chart_df_cp.rename(columns={'from_lot_id': 'lot_id',
                                                                                    'from_wafer_id': 'wafer_id',
                                                                                    'from_step': ''})
                            single_chart_df_cp = single_chart_df_cp[single_chart_df.columns]
                            single_chart_df_merge = single_chart_df.append(single_chart_df_cp)
                            test_trv_flag = True
                    #
                    except Exception as e:
                        logger.error(str(e), exc_info=True)
                        logger.error(log_prefix + "test traveller query got error.")


                # Check Analysis Type
                if chart_level == 'wafer':
                    logger.info(log_prefix + "Chart level is " + chart_level)
                    sigma_lot_start_time = time.time()
                    sigma_query_lot_instance = SigmaQuery(fab=fab, data_type='lot', host=hbase_host, port=hbase_port,
                                                          cluster_type=cluster_type, columns=all_columns['lot'],
                                                          include_timestamp=True, stop_step=measurement_step,
                                                          step_prefix=step_prefix)

                    lot_list_df = pd.DataFrame()
                    lot_list_df['lot_id'] = [str(x)[0:7] for x in single_chart_df_merge['lot_id']]
                    lot_list_df = lot_list_df.drop_duplicates()

                    sigma_lot_df = pd.concat(list(lot_list_df.apply(lambda row: sigma_query_lot_instance.sigma_query(row['lot_id']),
                                                            axis=1)))
                    # print(sigma_lot_df)
                    if sigma_lot_df.shape[0] > 0:
                        sigma_lot_df = sigma_lot_df[['lot_id', 'timestamp', 'mfg_process_step', 'column', 'value']]
                        logger.debug(log_df(sigma_lot_df.head(10)))
                        # Remove nan or empty
                        sigma_lot_df = sigma_lot_df[sigma_lot_df['value'] > ""]
                        logger.debug(log_df(sigma_lot_df.head(10)))
                        sigma_lot_df = sigma_lot_df.pivot_table(index=['lot_id', 'timestamp', 'mfg_process_step'],
                                                                columns='column',
                                                                values='value', aggfunc='first').reset_index(drop=False)

                        sigma_lot_df['lot_id'] = [str(x)[0:7] for x in sigma_lot_df['lot_id']]
                    logger.info(log_prefix + "Sigma Lot Query takes --- %s seconds ---"
                                    % int(time.time() - sigma_lot_start_time))
                    logger.info(
                            log_prefix + "Sigma Lot Query size is %s" % sigma_lot_df.shape[0])
                    sigma_wafer_start_time = time.time()
                    # columns = {'cf:WAFER_TYPE': 'wafer_type', 'cf:PROCESS_CHAMBER - WAFER_ATTR||1': 'chamber'}
                    sigma_query_wafer_instance = SigmaQuery(fab=fab, data_type='wafer', host=hbase_host, port=hbase_port,
                                                            cluster_type=cluster_type, columns=all_columns['wafer'],
                                                            include_timestamp=True, stop_step=measurement_step,
                                                            step_prefix=step_prefix)
                    # sigma_query_wafer_instance.
                    lot_wafer_list_df = single_chart_df_merge[['lot_id', 'wafer_id']]
                    lot_wafer_list_df['lot_id'] = [str(x)[0:7] for x in lot_wafer_list_df['lot_id']]
                    lot_wafer_list_df = lot_wafer_list_df.drop_duplicates()

                    sigma_wafer_df = pd.concat(list(lot_wafer_list_df.apply(lambda row:
                                                              sigma_query_wafer_instance.sigma_query(lot_id=row['lot_id'],
                                                                                                     wafer_id=row['wafer_id']),
                                                              axis=1)))
                    if sigma_wafer_df.shape[0] > 0:
                        sigma_wafer_df = sigma_wafer_df[['lot_id', 'wafer_id', 'timestamp',
                                                         'mfg_process_step', 'column', 'value']]
                        sigma_wafer_df = sigma_wafer_df[sigma_wafer_df['value'] > ""]
                        sigma_wafer_df = sigma_wafer_df.drop_duplicates()

                        sigma_wafer_df = sigma_wafer_df.pivot_table(index=['lot_id', 'wafer_id', 'timestamp', 'mfg_process_step'],
                                                                columns='column',
                                                                values='value', aggfunc='first').reset_index(drop=False)
                        logger.info(log_prefix + "Sigma Wafer Query takes --- %s seconds ---" % int(time.time() - sigma_wafer_start_time))
                        logger.info(
                            log_prefix + "Sigma Wafer Query size is %s" % sigma_wafer_df.shape[0])
                    sigma_df = sigma_wafer_df.merge(sigma_lot_df, on=['lot_id', 'mfg_process_step',
                                                                          'timestamp'], how='left')
                    sigma_df = sigma_df.sort_values('timestamp', ascending=True).reset_index(drop=True)
                    logger.debug(log_df(sigma_df.head(10)))
                    sigma_df = sigma_df.rename(columns={'timestamp': 'run_complete_datetime'})
                    if test_trv_flag and test_trv_df_trim.shape[0] > 0:
                        test_trv_df_trim = test_trv_df[['lot_id', 'wafer_id', 'wafer_scribe', 'from_step']] \
                            .drop_duplicates() \
                            .reset_index(drop=True)\
                            .rename(columns={'from_step': 'mfg_process_step'})
                        test_trv_df_trim['to_lot_id'] = [str(x)[0:7] for x in test_trv_df_trim['lot_id']]
                        test_trv_df_trim['to_wafer_id'] = test_trv_df_trim['wafer_id']
                        sigma_df_merge = sigma_df.merge(test_trv_df_trim[['to_lot_id', 'to_wafer_id', 'wafer_scribe',
                                                                             'mfg_process_step']],
                                                           on=['wafer_scribe', 'mfg_process_step'],
                                                           how='outer', indicator=True)
                        sigma_df_origin = sigma_df_merge[sigma_df_merge['_merge'] == 'left_only']
                        sigma_df_origin = sigma_df_origin.drop(['to_lot_id', 'to_wafer_id', '_merge'],errors='ignore', axis=1)
                        sigma_df_test_trv = sigma_df_merge[sigma_df_merge['_merge']=='both']

                        sigma_df_test_trv = sigma_df_test_trv.drop(['lot_id', 'wafer_id', '_merge'],errors='ignore', axis=1)\
                                                             .rename(columns={'to_lot_id': 'lot_id',
                                                                              'to_wafer_id': 'wafer_id'})
                        test_trv_step_list = list(sigma_df_test_trv['mfg_process_step'].unique())
                        sigma_df = sigma_df_origin.append(sigma_df_test_trv)

                    logger.info(log_prefix + "Sigma dataframe size is %s" % sigma_df.shape[0])

                else:
                    logger.info(log_prefix + "Chart level is " + chart_level)
                    single_chart_df['wafer_id'] = ""
                    single_chart_ckc_df['wafer_id'] = ""
                    sigma_lot_start_time = time.time()
                    sigma_query_lot_instance = SigmaQuery(fab=fab, data_type='lot', host=hbase_host, port=hbase_port,
                                                          cluster_type=cluster_type, columns=all_columns['lot'],
                                                          include_timestamp=True, stop_step=measurement_step,
                                                          step_prefix=step_prefix)
                    lot_list_df = pd.DataFrame()
                    lot_list_df['lot_id'] = [str(x)[0:7] for x in single_chart_df_merge['lot_id']]
                    lot_list_df = lot_list_df.drop_duplicates()

                    sigma_lot_df = pd.concat(list(lot_list_df.apply(lambda row:
                                                                    sigma_query_lot_instance.sigma_query(row['lot_id']),
                                                            axis=1)))
                    # print(sigma_lot_df.head(5))
                    sigma_lot_df['lot_id'] = [str(x)[0:7] for x in sigma_lot_df['lot_id']]
                    sigma_lot_df['wafer_id'] = ""
                    sigma_lot_df = sigma_lot_df[sigma_lot_df['value'] > ""]
                    sigma_lot_df = sigma_lot_df.pivot_table(columns=['column'], values='value',
                                                                                  index=['lot_id', 'wafer_id', 'timestamp',
                                                                                         'mfg_process_step'],
                                                                                  aggfunc='first').reset_index()
                    logger.info(log_prefix + "Sigma Lot Query takes --- %s seconds ---" % int(time.time() - sigma_lot_start_time))

                    sigma_df = sigma_lot_df.copy()
                    # sigma_df = sigma_df.fillna("")
                    single_chart_df_tmp = single_chart_df.rename(columns={'process_step': 'mfg_process_step',
                                                                          'current_process_position': 'process_position'})
                    single_chart_df_tmp['lot_id'] = [str(x)[0:7] for x in single_chart_df_tmp['lot_id']]
                    sigma_df = sigma_df.merge(single_chart_df_tmp[['lot_id', 'wafer_id', 'mfg_process_step', 'process_position']],
                                              on=['lot_id', 'wafer_id', 'mfg_process_step'], how='left')
                    sigma_df = sigma_df.sort_values('timestamp', ascending=True).reset_index(drop=True)
                    sigma_df = sigma_df.rename(columns={'timestamp': 'run_complete_datetime'})

                if sigma_df.shape[0] < 1:
                    logger.info(
                        log_prefix + "Total time takes --- %s seconds ---" % int(time.time() - chart_start_time))
                    logger.error(log_prefix + "No sigma data is available.")
                    final_status = final_status + "No sigma data. "
                    tracking_instance.update_tracking_single_column('status', final_status)
                    continue
                else:
                    final_status = final_status + "Sigma query complete. "
                    logger.info(log_prefix + "Sigma data length is " + str(sigma_df.shape[0]))
            except Exception as e:
                logger.info(log_prefix + "Total time takes --- %s seconds ---" % int(time.time() - chart_start_time))
                logger.error(str(e), exc_info=True)
                logger.error(log_prefix + "sigma query got error")
                if mode == 'orion':
                    orion_result_instance.root_cause = final_status
                    status_code = orion_result_instance.post_json()
                    if status_code != 200:
                        logger.error(log_prefix + "http call update response code is " + str(status_code))
                        sys.exit(0)
                final_status = final_status + "sigma query error."
                tracking_instance.update_tracking_single_column('status', final_status, ad_complete=True)
                continue

            # Get DF Lot Level
            sigma_df['df_tool_pos'] = ""
            sigma_df['df_pos'] = ""
            sigma_df[['df_tool_pos', 'df_pos']] = sigma_df.apply(lambda row: append_tool(row, mode='df'),
                                                                          axis=1, result_type='expand')
            # Get Sigma Data
            if chart_level == 'wafer':
                sigma_df['process_position'] = sigma_df.apply(lambda row: append_tool(row), axis=1)

            sigma_df = sigma_df.drop(columns_to_remove + ['process_chamber'],
                                     errors='ignore', axis=1)

            logger.info(log_prefix + "Sigma data process_position is re-formatted. ")
            logger.debug(log_df(sigma_df.head(10)))

            lot_id_list = list(single_chart_df['lot_id'])
            wafer_id_list = list(single_chart_df['wafer_id'])

            # Get SWR Data
            if 'swr' in analysis_block_list:
                try:
                    swr_start_time = time.time()
                    swr_query_instance = SWRQuery(lot_id_list=lot_id_list, wafer_id_list=wafer_id_list,
                                                  fab=fab, teradata_config=teradata_config)
                    swr_df = swr_query_instance.swr(chart_level)
                    logger.info(log_prefix + "SWR Query takes --- %s seconds ---" % int(time.time() - swr_start_time))
                    logger.debug(log_df(swr_df.head(5)))
                    if swr_df.shape[0] > 0:
                        swr_df['lot_id'] = [str(x)[0:7] for x in swr_df['lot_id']]
                        sigma_df = sigma_df.merge(swr_df, on=['lot_id', 'wafer_id', 'mfg_process_step'], how='left')
                        sigma_df['swr'] = sigma_df['swr'].fillna("NO SWR")

                        logger.info(log_prefix + "SWR Check is complete. SWR information is available. " +
                                    "Size is " + str(swr_df.shape[0]))
                    else:
                        logger.info(log_prefix + "SWR Check is complete. No SWR information is available")
                except Exception as e:
                    logger.error(str(e), exc_info=True)
                    logger.error(log_prefix + "SWR Check error. No SWR information is available during the analysis")

            # qdr check
            if 'qdr' in analysis_block_list:
                # Get QDR Data
                try:
                    qdr_start_time = time.time()
                    qdr_query_instance = QDRQuery(lot_id_list=lot_id_list, wafer_id_list=wafer_id_list,
                                                  fab=fab, teradata_config=teradata_config)
                    qdr_df = qdr_query_instance.qdr(chart_level)
                    logger.info(log_prefix + "QDR Query takes --- %s seconds ---" % int(time.time() - qdr_start_time))
                    logger.debug(log_df(qdr_df.head(5)))
                    if qdr_df.shape[0] > 0:
                        qdr_df['lot_id'] = [str(x)[0:7] for x in qdr_df['lot_id']]
                        sigma_df = sigma_df.merge(qdr_df, on=['lot_id', 'wafer_id', 'mfg_process_step'], how='left')
                        sigma_df['qdr'] = sigma_df['qdr'].fillna("NO QDR")
                        logger.info(log_prefix + "QDR Check is complete. QDR information is available. " +
                                    "Size is " + str(qdr_df.shape[0]))
                    else:
                        logger.info(log_prefix + "QDR Check is complete. No QDR information is available")
                except Exception as e:
                    logger.error(str(e), exc_info=True)
                    logger.error(log_prefix + "QDR Check error. No QDR information is available during the analysis")

            # lot attribute check
            if 'lot_attribute' in analysis_block_list:
                # Get Lot Attribute
                try:
                    lot_attribute_check_start_time = time.time()
                    lot_attribute_check = LotAttributeQuery(fab=fab, lot_id_list=lot_id_list, teradata_config=teradata_config)
                    lot_attribute_df = lot_attribute_check.lot_attribute_query()
                    logger.info(log_prefix + "Lot Attribute Query takes --- %s seconds ---"
                                % int(time.time() - lot_attribute_check_start_time))

                    logger.debug(log_df(lot_attribute_df.head(5)))

                    if lot_attribute_df.shape[0] > 0:
                        lot_attribute_df['lot_id'] = [str(x)[0:7] for x in lot_attribute_df['lot_id']]

                        sigma_df = sigma_df.merge(lot_attribute_df, on=['mfg_process_step', 'lot_id'], how='left')
                        sigma_df['lot_attribute'] = sigma_df['lot_attribute'].fillna("NO LOT ATTRIBUTE")

                        logger.info(log_prefix + "Lot attribute Check is complete. "
                                                 "Lot attribute information is available. " +
                                    "Size is " + str(lot_attribute_df.shape[0]))
                    else:
                        logger.info(log_prefix + "Lot attribute Check is complete. "
                                                 "No lot attribute information is available")
                except Exception as e:
                    logger.error(str(e), exc_info=True)
                    logger.error(log_prefix + "Lot Attribute Check got error. " +
                                              "No Lot Attribute information is available during the analysis.")

            # Prepare to Check Step Type
            # Get FMEA Data
            if 'fmea' in analysis_block_list:
                fmea_step_list = list(FMEA_df[FMEA_df['channel_id'] == channel_id]['process_step'])
                if len(fmea_step_list) > 0:
                    logger.info(log_prefix + "FMEA step query is completed! FMEA Step is " + str(fmea_step_list))
                else:
                    logger.info(log_prefix + "FMEA step query is completed! No FMEA step is found.")
            else:
                fmea_step_list = []

            # Check Last OOC point
            if chart_level == 'lot':
                sigma_df['wafer_id'] = ""
            single_chart_ckc_df = single_chart_ckc_df.sort_values('sample_date', ascending=True)
            df_space_last_ooc = single_chart_ckc_df[single_chart_ckc_df['label'] == 1].tail(last_ooc_number)
            df_space_last_ooc['lot_id'] = df_space_last_ooc['lot_id'].str[0:7]
            sigma_df_last_ooc = sigma_df.merge(df_space_last_ooc[['lot_id', 'wafer_id']], how='inner',
                                               on=['lot_id', 'wafer_id'])
            # sigma_df_last_ooc = sigma_df
            logger.info(log_prefix + "last ooc sigma data length is " + str(sigma_df_last_ooc.shape[0]))

            lot_id = str(df_space_last_ooc['lot_id'].iloc[-1])
            wafer_id = str(df_space_last_ooc['wafer_id'].iloc[-1])
            logger.info(log_prefix + "Reference lot id is " + str(lot_id))
            logger.info(log_prefix + "Reference wafer id is " + str(wafer_id))

            if sigma_df_last_ooc.shape[0] < 1:
                logger.info(
                    log_prefix + "Total time takes --- %s seconds ---" % int(time.time() - chart_start_time))
                logger.error(log_prefix + "No sigma data is for last OOC point")

                if mode == 'orion':
                    orion_result_instance.root_cause = final_status
                    status_code = orion_result_instance.post_json()
                    if status_code != 200:
                        logger.error(log_prefix + "http call update response code is " + str(status_code))
                        sys.exit(0)
                final_status = final_status + "No last OOC sigma data. "
                tracking_instance.update_tracking_single_column('status', final_status, ad_complete=True)

                continue

            sigma_df_last_ooc['loop'] = sigma_df_last_ooc['mfg_process_step'].apply(get_loop_id)

            # Get Domain Data
            # Find Domain Knowledge
            if 'domain' in analysis_block_list:
                try:
                    domain_knowledge_step_list = []
                    search_key_dict = {
                        'parameter': parameter,
                        'mfg_process_step': measurement_step,
                        'design_id': design_id
                    }
                    if domain_knowledge.get(area) is not None:
                        search_key = domain_knowledge.get(area).get('search_key')
                        if search_key == "NA":
                            key = 'all'
                            regex = 'no'
                        else:
                            key = search_key_dict.get(search_key)
                            regex = domain_knowledge.get(area).get('regex')
                        # print(key)
                        # print(regex)

                        need_to_check_flag = False
                        if regex == 'yes':
                            domain_knowledge_instance = domain_knowledge.get(area).get('all')
                            regex_pattern = domain_knowledge_instance['regex']
                            if re.match(regex_pattern, key) is not None:
                                domain_knowledge_instance = domain_knowledge.get(area).get('all')
                                need_to_check_flag = True
                        else:
                            domain_knowledge_instance = domain_knowledge.get(area).get(key)
                            need_to_check_flag = True
                        # print(domain_knowledge_instance)
                        # print(need_to_check_flag)
                        if need_to_check_flag and domain_knowledge_instance is not None:
                            logger.info(log_prefix + "Domain knowledge is " + str(domain_knowledge_instance))
                            loop_range = domain_knowledge_instance.get('loop_range')
                            domain_mode = domain_knowledge_instance.get('mode')
                            step_key_words = domain_knowledge_instance.get('step_key_words')
                            # print(sigma_df_last_ooc[sigma_df_last_ooc['mfg_process_step'] == '3010-57 CHOP PHOTO'])

                            domain_knowledge_df = find_domain_steps(sigma_df_last_ooc,
                                                                    measurement_step,
                                                                    step_key_words,
                                                                    domain_mode,
                                                                    loop_range)
                            # print(domain_knowledge_df)
                            if domain_knowledge_df.shape[0] > 0:
                                domain_knowledge_step_list = list(set(list(domain_knowledge_df['mfg_process_step'])))
                                logger.info(log_prefix + "Domain knowledge found: " + str(domain_knowledge_step_list))
                            else:
                                logger.info(log_prefix + "No domain knowledge found.")
                        else:
                            logger.info(log_prefix + "No domain knowledge found.")
                except Exception as e:
                    logger.error(str(e), exc_info=True)
                    logger.error(log_prefix + "domain knowledge query got error")
                    domain_knowledge_step_list = []
            else:
                domain_knowledge_step_list = []

            if 'feedback' in analysis_block_list:
                # Get Feedback Data
                try:
                    sql_template = util.read_file(file_dir + '/../sql/feedback_query_template.sql')
                    sql = sql_template.format(feedback_table_name=feedback_table_name,
                                              fab=fab, ch_id=ch_ckc_id, fb_min_query_date=fb_min_query_date,
                                              query_session=query_session)
                    feedback_df = mssql_query_instance.execute_sql(sql, execution_only=False)

                    if feedback_df.shape[0] > 0:
                        feedback_step_list = list(feedback_df['actual_root_cause_step'].unique())
                        logger.info(log_prefix + "feedback cases are " + str(len(feedback_step_list)))
                        logger.info(log_prefix + "feedback actual_root_cause_step are " + str(feedback_step_list))
                    else:
                        logger.info(log_prefix + "No feedback cases is found. ")
                        feedback_step_list = []
                except Exception as e:
                    logger.error(str(e), exc_info=True)
                    feedback_step_list = []
                    feedback_df = pd.DataFrame()
                    logger.error(log_prefix + "feedback query got error. ")
            else:
                feedback_df = pd.DataFrame()
                feedback_step_list = []

            # Step Type Conversion
            sigma_df['loop'] = sigma_df['mfg_process_step'].apply(get_loop_id)
            sigma_df['step_type'] = [get_step_type(row, fmea_step_list, domain_knowledge_step_list,
                                                   feedback_step_list, process_step, measurement_step,
                                                   current_loop, test_trv_step_list)
                                     for idx, row in sigma_df[['mfg_process_step', 'loop']].iterrows()]
            # print(sigma_df.head(100))
            # Calculate correlation
            excluded_columns = ['lot_id', 'wafer_id', 'run_complete_datetime', 'mfg_process_step', 'loop', 'step_type']
            feature_columns = list(set(sigma_df.columns) - set(excluded_columns))
            # print(feature_columns)
            res_df_unpivot = sigma_df.melt(id_vars=excluded_columns, value_vars=feature_columns).rename(
                columns={'variable': 'feature', 'column': 'feature'})
            sigma_df_last_ooc_unpivot = sigma_df_last_ooc.melt(id_vars=excluded_columns, value_vars=feature_columns)
            sigma_df_last_ooc_unpivot = sigma_df_last_ooc_unpivot.rename(
                columns={'variable': 'feature', 'value': 'context_value'})

            single_chart_df_trim = single_chart_df[['lot_id', 'wafer_id', 'label', 'sample_date']].copy()
            single_chart_df_trim['lot_id'] = [str(x)[0:7] for x in single_chart_df_trim['lot_id']]
            # print(single_chart_df_trim.head(5))
            # print(res_df_unpivot)
            res_df_unpivot_merge = res_df_unpivot.merge(single_chart_df_trim,
                                                        on=['lot_id', 'wafer_id'], how='inner')
            # print(res_df_unpivot_merge)
            res_df_unpivot_merge['context_type'] = res_df_unpivot_merge['mfg_process_step'] + COLUMN_DELIMITER + \
                                                   res_df_unpivot_merge['feature']

            res_df_unpivot_merge['context_idx'] = res_df_unpivot_merge['step_type'] + COLUMN_DELIMITER + \
                                                  res_df_unpivot_merge['context_type'] + COLUMN_DELIMITER + \
                                                  res_df_unpivot_merge['value']
            res_df_unpivot_merge['flag'] = 1

            res_df_unpivot_merge_pivot = res_df_unpivot_merge.pivot_table(columns=['context_idx'], values='flag',
                                                                          index=['lot_id', 'wafer_id', 'label', 'sample_date'],
                                                                          fill_value=0).reset_index()

            res_df_unpivot_merge_pivot = res_df_unpivot_merge_pivot\
                                        .sort_values('sample_date', ascending=True)\
                                        .reset_index(drop=True)

            weight = ((res_df_unpivot_merge_pivot.index + 1.0) / max(res_df_unpivot_merge_pivot.index)).astype(float)
            result = {}
            for x in res_df_unpivot_merge_pivot.drop(['lot_id', 'wafer_id', 'sample_date', 'label'], axis=1).columns:
                # If only one value is existing in the whole column, for SWR/QDR/LOT ATTRIBUTE, it will removed
                if x.split(COLUMN_DELIMITER)[2] in ('lot_attribute', 'qdr', 'swr'):
                    if len(res_df_unpivot_merge_pivot[x].astype(int).unique()) < 2:
                        continue
                percentage_of_ooc = 1 - np.sum(np.isnan(res_df_unpivot_merge_pivot[x]))*1.0/len(res_df_unpivot_merge_pivot['label'])
                result[x] = pearsonr(res_df_unpivot_merge_pivot[x].astype(float) * weight,
                                     res_df_unpivot_merge_pivot['label'].astype(float) * weight)[0] * percentage_of_ooc

            feature_imp_df = pd.DataFrame.from_dict(result, 'index', columns=['corr']).reset_index(
                drop=False).sort_values("corr", ascending=False).rename(columns={'index': 'context_idx'})

            feature_imp_df['step_type'], feature_imp_df['mfg_process_step'], feature_imp_df['feature'], feature_imp_df[
                'context_value'] = feature_imp_df['context_idx'].str.split(COLUMN_DELIMITER).str

            feature_imp_df = feature_imp_df.merge(
                sigma_df_last_ooc_unpivot[['mfg_process_step', 'feature', 'context_value']],
                how="inner", on=['mfg_process_step', 'feature', 'context_value'])

            feature_imp_df = feature_imp_df[(feature_imp_df['context_value'] != 'NO SWR') & \
                                            (feature_imp_df['context_value'] != 'NO QDR') & \
                                            (feature_imp_df['context_value'] != 'NO LOT ATTRIBUTE') &
                                            (feature_imp_df['context_value'] != "")]

            # remove single context value
            res_df_unpivot = res_df_unpivot[(~ res_df_unpivot['value'].isnull()) & (res_df_unpivot['value'] > "")]
            res_df_unpivot_count = res_df_unpivot.drop_duplicates(['mfg_process_step', 'step_type', 'feature', 'value'],
                                                                  keep='first').groupby(
                ['mfg_process_step', 'step_type', 'feature']).lot_id.count().reset_index(drop=False)
            res_df_unpivot_count_filter = res_df_unpivot_count[
                (res_df_unpivot_count['lot_id'] > 1) |
                (((res_df_unpivot_count['feature'] == 'equipment_id') |
                  (res_df_unpivot_count['feature'] == 'process_position')) &
                 (res_df_unpivot_count['step_type'] != '7_incoming'))].reset_index(
                drop=True)

            feature_imp_df = feature_imp_df.merge(res_df_unpivot_count_filter[['mfg_process_step', 'step_type', 'feature']],
                                                  how='inner')
            feature_imp_df = feature_imp_df.drop_duplicates()

            trend_detect_df = feature_imp_df.sort_values("corr", ascending=False) \
                .drop_duplicates(['step_type', 'mfg_process_step', 'feature'], keep='first') \
                .groupby('step_type') \
                .apply(lambda x: x.nlargest(n=10, columns='corr')) \
                .reset_index(drop=True)
            # trend_detect_df.to_csv('trend_detect_df.csv', index=False, header=True)
            # Trend check
            try:

                logger.info(log_prefix + "Result table size before trending check is " + str(trend_detect_df.shape[0]))
                single_chart_df_trim = single_chart_df[['lot_id', 'wafer_id', 'label', 'value']].copy()
                single_chart_df_trim['lot_id'] = [str(x)[0:7] for x in single_chart_df_trim['lot_id']]
                trending_check_start_time = time.time()
                trend_detect_df['detection_result'] = trend_detect_df.apply(lambda row:
                                                                          trend_detect_result_per_row(row,
                                                                            sigma_df,
                                                                            single_chart_df_trim,
                                                                            ucl,
                                                                            lcl,
                                                                            trending_check_config), axis=1)

                trend_detect_df_final = trend_detect_df[trend_detect_df['detection_result'] == 'include'].drop(
                    'detection_result', axis=1)

                final_result = trend_detect_df_final.groupby('step_type') \
                    .apply(lambda r: r.nlargest(n=3, columns='corr')) \
                    .reset_index(drop=True)

                logger.info(log_prefix + "Trending Checking is completed. Trending information is available. " +
                    "Size is " + str(final_result.shape[0]))
                logger.info(
                    log_prefix + "Trending Checking takes --- %s seconds ---" % int(time.time()
                                                                                    - trending_check_start_time))

            except Exception as e:
                logger.error(str(e), exc_info=True)
                logger.error(log_prefix + "Trending Checking got error")

                final_result = feature_imp_df.sort_values("corr", ascending=False) \
                    .drop_duplicates(['step_type', 'mfg_process_step', 'feature'], keep='first') \
                    .groupby('step_type') \
                    .apply(lambda r: r.nlargest(n=3, columns='corr')) \
                    .reset_index(drop=True)

            logger.info(log_prefix + "Result table is out.")
            logger.info(log_prefix + "Result table size is " + str(final_result.shape[0]))
            # logger.info(final_result)
            if final_result.shape[0] > 0:

                # Get Top result and apply PM/CM check
                final_result['mfg_process_step'] = final_result['mfg_process_step'].astype(str)
                final_result['context_value'] = final_result['context_value'].astype(str)
                if 'pmcm' in analysis_block_list:
                    try:
                        pm_cm_check_start_time = time.time()
                        pm_cm_step_list = \
                            list(final_result[(final_result['feature'] == 'equipment_id') | (
                                    final_result['feature'] == 'process_position')][
                                     'mfg_process_step'])
                        pm_cm_step_list = [str(pm_cm_step_list_element) for pm_cm_step_list_element in pm_cm_step_list]

                        pm_cm_equipment_list = \
                            list(final_result[(final_result['feature'] == 'equipment_id') | (
                                    final_result['feature'] == 'process_position')][
                                     'context_value'])
                        pm_cm_equipment_list = [str(pm_cm_equipment_list_element)[0:10] for pm_cm_equipment_list_element in
                                                pm_cm_equipment_list]

                        # generate a lot list with the same length as pm_cm_step_list and pm_cm_equipment_list
                        pm_cm_lot_list = []
                        if len(pm_cm_step_list) == len(pm_cm_equipment_list):
                            for index in range(len(pm_cm_step_list)):
                                pm_cm_lot_list.append(str(lot_id) + "%")

                        pm_cm_sample_date = pm_cm_config['pm_cm_sample_date']
                        long_period = pm_cm_config['pm_cm_query_day_long']
                        short_period = pm_cm_config['pm_cm_query_day_short']
                        pm_cm_depth = pm_cm_config['depth']

                        pm_cm_start_date, pm_cm_end_date = pm_cm_query_date(pm_cm_sample_date, long_period, short_period)

                        pm_cm_check = PmCmCheck(fab=fab,
                                                lot_list=pm_cm_lot_list,
                                                equip_list=pm_cm_equipment_list,
                                                step_list=pm_cm_step_list,
                                                start_date=pm_cm_start_date,
                                                end_date=pm_cm_end_date,
                                                depth=pm_cm_depth,
                                                all_lot=lot_id_list,
                                                teradata_config=teradata_config)

                        pmcm_query_result = pm_cm_check.pm_cm_query()
                        # pmcm_query_result.to_csv('pmcm_query_result.csv',index=False, header=True)
                        pmcm_staging = pm_cm_check.filter_non_measured(pmcm_query_result)
                        pmcm_result = pm_cm_check.recursive_pm_cm(pmcm_staging)
                        #pmcm_dt = list(set(pmcm_result['datetime'][pmcm_result['reason'].str.contains('PM/CM', na=False)]))
                        logger.info(log_prefix + "PM/CM Query takes --- %s seconds ---"
                                    % int(time.time() - pm_cm_check_start_time))

                        logger.debug(log_df(pmcm_result))

                        if (pmcm_result.shape[0] > 0) & ('reason' in pmcm_result.columns) :
                            if len(pmcm_result['reason'].unique()) > 1:
                                pmcm_result = pmcm_result.drop(['lot_id'], axis=1) \
                                    .drop_duplicates(['mfg_process_step', 'context_value', 'reason'], keep='first')

                                # print(final_result)
                                final_result['mfg_process_step'] = final_result['mfg_process_step'].astype(str)
                                final_result['context_value'] = final_result['context_value'].astype(str)
                                pmcm_result['mfg_process_step'] = pmcm_result['mfg_process_step'].astype(str)
                                pmcm_result['context_value'] = pmcm_result['context_value'].astype(str)
                                # '''
                                # merge result table and pm_cm_result
                                # '''
                                final_result = pd.merge(final_result, pmcm_result, how='left',
                                                        on=['mfg_process_step', 'context_value']).fillna('') \
                                    .drop_duplicates(['step_type', 'mfg_process_step', 'feature', 'context_value'],
                                                     keep='first').reset_index(drop=True)
                                #print(final_result)

                                logger.info(log_prefix + "PM/CM Check is complete. PM/CM information is available. " +
                                            "Size is " + str(final_result.shape[0]))
                            else:
                                logger.info(log_prefix + "PM/CM Check is complete. No PM/CM information is available")
                        else:
                            logger.info(log_prefix + "PM/CM Check is complete. No PM/CM information is available")

                    except Exception as e:
                        logger.error(str(e), exc_info=True)
                        logger.error(log_prefix + "PM/CM Check got error.")

                final_result['corr'] = final_result['corr'].round(2)
                # final_result = final_result[final_result['corr'] >= 0]
                final_result = final_result[(final_result['corr'] >= 0.05) |
                                            (final_result['step_type'] == '2_past_feedback') |
                                            (final_result['step_type'] == '3_process_step') |
                                            (final_result['step_type'] == '1_fmea')].head(15)
                # final_result = final_result[final_result['corr'] <= 1.0]
                final_result = final_result.sort_values(['step_type', 'corr'], ascending=[True, False]).reset_index(
                    drop=True)

                logger.info(log_prefix + "Final results table is ready")
                logger.info(log_prefix + "Final result table size is " + str(final_result.shape[0]))
            else:
                logger.info(log_prefix + "Total time takes --- %s seconds ---" % int(time.time() - chart_start_time))
                logger.error(log_prefix + "No toggle found.")
                tracking_instance.update_tracking_single_column('analysis', get_now_str(tz))

                if mode == 'orion':
                    orion_result_instance.root_cause = final_status
                    status_code = orion_result_instance.post_json()
                    if status_code != 200:
                        logger.error(log_prefix + "http call update response code is " + str(status_code))
                        sys.exit(0)
                final_status = final_status + "Analysis complete. No toggle Found."
                tracking_instance.update_tracking_single_column('status', final_status, ad_complete=True)
                continue

            tracking_instance.update_tracking_single_column('analysis', get_now_str(tz))
            final_status = final_status + "Analysis complete. "

            logger.debug(log_df(final_result.head(25)))

            report_idx = "_".join([str(fab), str(channel_id), str(ckc_id), str(chart_type), str(instance_id)])
            ooc_plot_file_name = report_idx + '.png'
            html_file_name = report_idx + '.html'



            # Build Report
            if final_result.shape[0] > 0:
                results_plot_template = read_file(file_dir + "/../report_templates/orion_ad_report_plot_div_template.html")
                html_template = read_file(file_dir + "/../report_templates/orion_ad_report_template.html")
                style_template = "<style>" + read_file(file_dir + "/../report_templates/orion_ad_report.css") + "</style>"
                js_template = "<script>" + read_file(file_dir + "/../report_templates/orion_ad_report.js") + "</script>"
                single_chart_df_trim = single_chart_df[['lot_id', 'wafer_id', 'label', 'value']].copy()
                single_chart_df_trim['lot_id'] = [str(x)[0:7] for x in single_chart_df_trim['lot_id']]

                plots_start_time = time.time()

                result_plot = final_result.apply(lambda row: plot_result_per_row(row, sigma_df,
                                                                                 single_chart_df_trim,
                                                                                 lcl, ucl, parameter), axis=1)
                logger.info(log_prefix + "Plots Generation takes --- %s seconds ---"
                            % int(time.time() - plots_start_time))

                ## SPACE CHART
                try:

                    ooc_img_str = plot_ooc(single_chart_df, 'sample_date', 'label', 'value',
                                           channel_id, ckc_id, chart_type, lcl, ucl, parameter)
                except Exception as e:
                    logger.error(str(e), exc_info=True)
                    logger.error(log_prefix + "ooc plots got error")
                    ooc_img_str = ""

                # FMEA TABLE
                fmea_table_html = FMEA_df[FMEA_df['channel_id'] == channel_id][['channel_id', 'process_step']].to_html(
                    index=False)

                # FEEDBACK TABLE
                feedback_table_html = feedback_df.head(5).to_html(index=False)

                # RESULT TABLE
                if 'reason' in final_result.columns:
                    result_table_df = final_result[
                        ['mfg_process_step', 'step_type', 'feature', 'context_value', 'corr', 'reason']] \
                        .rename(columns={'corr': 'Rsquare'})
                else:
                    result_table_df = final_result[
                        ['mfg_process_step', 'step_type', 'feature', 'context_value', 'corr']] \
                        .rename(columns={'corr': 'Rsquare'})

                result_table_html = result_table_df.to_html(index=False)

                # RESULT PLOTS
                pattern = re.compile('[\W_]+')
                result_plots = "".join([results_plot_template.format(chart_id=pattern.sub('', x[0]).lower(),
                                                                     step_type=x[0].split("::")[0],
                                                                     step_name=x[0].split("::")[1],
                                                                     feature=x[0].split("::")[2],
                                                                     feature_name=x[0].split("::")[3],
                                                                     boxplot=x[1],
                                                                     trending_plot=x[2]) for x in result_plot])
                logger.info(log_prefix + "Length of plots is " + str(len(result_plots)))
                logger.info(log_prefix + "Report building preparation is completed!")

                # FEEDBACK SECTION
                actual_root_cause_category_to_show = ''
                misclassification_option_to_show = 'hidden'

                # Compile Report
                report_filer_path = report_config['report_filer_path'].format(analysis_type=analysis_type)
                web_link_prefix = report_config['web_link_prefix'].format(analysis_type=analysis_type)
                html_dir = report_filer_path + 'report/' + today_str
                feedback_url = report_config['feedback_url']
                Plot_Link = web_link_prefix + 'report/' + today_str + "/" + html_file_name
                if not os.path.exists(html_dir):
                    os.makedirs(html_dir)

                img_dir = report_filer_path + 'img/' + today_str + "/" + report_idx + '/'
                if not os.path.exists(img_dir):
                    os.makedirs(img_dir)

                img_tracking_instance = ImgUpdateTracking(analysis_type, fab, tz, Plot_Link, mssql_query_instance,
                                                          plots_table_name, no_tracking_flag)
                ooc_img_path = img_dir + ooc_plot_file_name
                ooc_img_link = plot_ooc(single_chart_df, 'sample_date', 'label', 'value',
                                        channel_id, ckc_id, chart_type, lcl, ucl, parameter,
                                        fmt='png', path=ooc_img_path)


                result_plot_link = final_result.apply(lambda row: plot_result_per_row(row, sigma_df,
                                                                                      single_chart_df_trim,
                                                                                      lcl, ucl, parameter, "png",
                                                                                      img_dir),
                                                      axis=1)
                result_plot_link_df = pd.DataFrame(list(result_plot_link))
                result_plot_link_df.columns = ['chart_idx', 'boxplot', 'trendplot', 'rank']

                result_plot_link_df_box = result_plot_link_df[['boxplot', 'rank']].rename(columns={'boxplot': 'pic_url'})
                result_plot_link_df_box['analysis_type'] = analysis_type
                result_plot_link_df_box['fab'] = fab
                result_plot_link_df_box['report_url'] = Plot_Link
                result_plot_link_df_box['plot_type'] = 'result_plot'
                result_plot_link_df_box['pic_type'] = 'box'
                result_plot_link_df_trend = result_plot_link_df[['trendplot', 'rank']].rename(columns={'trendplot': 'pic_url'})
                result_plot_link_df_trend['analysis_type'] = analysis_type
                result_plot_link_df_trend['fab'] = fab
                result_plot_link_df_trend['report_url'] = Plot_Link
                result_plot_link_df_trend['plot_type'] = 'result_plot'
                result_plot_link_df_trend['pic_type'] = 'trend'
                result_plot_link_df = pd.concat([result_plot_link_df_box, result_plot_link_df_trend], axis=0)

                # print(result_plot_link_df)
                img_tracking_instance.update_pic_url(ooc_img_link, result_plot_link_df)
                logger.info(log_prefix + "ooc plot is saved.")

                    # Data Download
                data_file_link = ""
                if save_data_flag:
                    try:
                        csv_file_name = "_".join(
                            [str(analysis_type), str(fab), str(channel_id),
                             str(ckc_id), str(chart_type), str(instance_id)]) + '.csv'
                        csv_file_dir = report_filer_path + 'data/' + today_str
                        if not os.path.exists(csv_file_dir):
                            os.makedirs(csv_file_dir)

                        res_df_unpivot['context_type'] = res_df_unpivot['mfg_process_step'] + COLUMN_DELIMITER + \
                                                         res_df_unpivot['feature']

                        res_df_unpivot_merge_pivot_variable = res_df_unpivot.pivot_table(columns='context_type',
                                                                                         values='value',
                                                                                         index=['lot_id', 'wafer_id'],
                                                                                         aggfunc='first').reset_index()
                        res_df_unpivot_merge_pivot_datetime = res_df_unpivot.pivot_table(columns='mfg_process_step',
                                                                                         values='run_complete_datetime',
                                                                                         index=['lot_id', 'wafer_id'],
                                                                                         aggfunc='first').reset_index()
                        # Rename
                        c = []
                        for x in res_df_unpivot_merge_pivot_datetime.columns:
                            if x not in ('lot_id', 'wafer_id'):
                                c.append(x + COLUMN_DELIMITER + 'run_complete_datetime')
                            else:
                                c.append(x)
                        res_df_unpivot_merge_pivot_datetime.columns = c

                        res_df_unpivot_merge_pivot = res_df_unpivot_merge_pivot_variable \
                            .merge(res_df_unpivot_merge_pivot_datetime,
                                   on=['lot_id', 'wafer_id'], how='left')
                        single_chart_df_trim = single_chart_df[['lot_id', 'wafer_id', 'label', 'sample_date', 'value']] \
                            .rename(columns={'value': measurement_step + COLUMN_DELIMITER + parameter})
                        single_chart_df_trim['lot_id'] = [str(x)[0:7] for x in single_chart_df_trim['lot_id']]
                        res_df_unpivot_merge = res_df_unpivot_merge_pivot.merge(single_chart_df_trim,
                                                                                on=['lot_id', 'wafer_id'], how='inner')

                        res_df_unpivot_merge.to_csv(csv_file_dir + "/" + csv_file_name, index=False, header=True)
                        data_file_link = web_link_prefix + "data/" + today_str + "/" + csv_file_name
                        if test_trv_flag:
                            test_trv_df.to_csv(csv_file_dir + "/qual_wafer_" + csv_file_name, index=False, header=True)
                            qual_wafer_mapping_file_link = web_link_prefix + "data/" + today_str + "/qual_wafer_" + csv_file_name
                            qual_wafer_mapping_hidden = ""
                        logger.info(log_prefix + "Data file link is " + data_file_link)
                    except Exception as e:
                        logger.error(log_prefix + str(e), exc_info=True)
                        logger.error(log_prefix + "Data file is not saved.")

                html_path = os.path.join(html_dir, html_file_name)
                html = html_template.format(
                    analysis_type=analysis_type,
                    fab=fab,
                    area=area,
                    design_id=design_id,
                    session=query_session,
                    report_link=Plot_Link,
                    result_plots=result_plots,
                    channel_id=channel_id,
                    ckc_id=ckc_id,
                    ch_name=ch_name,
                    chart_type=chart_type,
                    current_step=measurement_step,
                    process_step=process_step,
                    ooc_img=ooc_img_str,
                    feedback_url=feedback_url,
                    lot_id=str(lot_id),
                    wafer_id=wafer_id,
                    fmea_table=fmea_table_html,
                    feedback_table=feedback_table_html,
                    result_table=result_table_html,
                    style_template=style_template,
                    js_template=js_template,
                    data_file_link=data_file_link,
                    qual_wafer_mapping_file_link=qual_wafer_mapping_file_link,
                    qual_wafer_mapping_hidden=qual_wafer_mapping_hidden,
                    misclassification_option_to_show=misclassification_option_to_show
                )
                final_status = final_status + "Report built."
                logger.info(log_prefix + "Length of final html is " + str(len(html)))

                with open(html_path, 'w') as f:
                    f.write(html)
                logger.info(log_prefix + "Report  is saved as  " + html_path)
                logger.info(log_prefix + "report link is : " + Plot_Link)
                logger.info(log_prefix + "Total time takes --- %s seconds ---" % int(time.time() - chart_start_time))

                if save_root_cause_as_http_call_flag:
                    # try:
                    result_table_df = result_table_df.sort_values('Rsquare', ascending=False)
                    if len(result_table_html) < 4000:
                        result_table_html = result_table_df.to_html(index=False)
                    else:
                        result_table_html = result_table_df.head(5).to_html(index=False)

                    result_table_html = re.sub(r'\n\s+', '', result_table_html)
                    result_table_html = re.sub(r'\n', '', result_table_html)
                    root_cause_category =  result_table_df.iloc[0]['mfg_process_step'] + \
                                      COLUMN_DELIMITER + result_table_df.iloc[0]['feature']
                    score_of_the_root_cause = result_table_df.iloc[0]['Rsquare']
                    logger.info(log_prefix + "Root cause saved into http call.")

                    if mode == 'orion':
                        orion_result_instance.root_cause = result_table_html
                        orion_result_instance.score_of_the_root_cause = score_of_the_root_cause
                        orion_result_instance.root_cause_category = root_cause_category
                        orion_result_instance.data_readiness_flag = "YES"
                        orion_result_instance.plot_link = Plot_Link
                        status_code = orion_result_instance.post_json()
                        if status_code != 200:
                            logger.error(log_prefix + "http call update response code is " + str(status_code))
                            sys.exit(0)
                    tracking_instance.update_tracking_single_column("result", get_now_str(tz))
                    tracking_instance.update_tracking_single_column("url", Plot_Link)
                    tracking_instance.update_tracking_single_column('status', final_status, ad_complete=True)
                    # except Exception as e:
                    #     sys.exit(1)

                if save_root_cause_flag:
                    # Load Result into DB
                    try:
                        # Result into DB
                        mssql_query_instance.empty_table(table=root_cause_table_name, fab=fab,
                                                         analysis_type=analysis_type,
                                                         query_session=query_session, ch_id=ch_ckc_id,
                                                         instance_id=instance_id,
                                                         chart_type=chart_type)
                        # Add Context Information
                        result_table_df[['mfg_process_step', 'step_type', 'feature', 'context_value']] = \
                          result_table_df[['mfg_process_step', 'step_type', 'feature', 'context_value']].astype(str)
                        result_table_df['analysis_type'] = analysis_type
                        result_table_df['fab'] = fab
                        result_table_df['module'] = area
                        result_table_df['ch_id'] = ch_ckc_id
                        result_table_df['chart_type'] = chart_type
                        result_table_df['channel_type'] = channel_type
                        result_table_df['design_id'] = design_id
                        result_table_df['query_session'] = query_session
                        result_table_df['current_step'] = measurement_step
                        result_table_df['parameter_name'] = parameter
                        result_table_df['process_step'] = process_step
                        result_table_df['updated_datetime'] = get_now_str(tz)
                        result_table_df['rank'] = result_table_df.index + 1
                        result_table_df['report_url'] = Plot_Link
                        result_table_df['instance_id'] = instance_id
                        result_table_df['table_html'] = ""
                        mssql_query_instance.insert_df(result_table_df, root_cause_table_name)
                        logger.info(log_prefix + "Root cause saved into database.")
                    except Exception as e:
                        logger.error(log_prefix + str(e), exc_info=True)
                        logger.error(log_prefix + "Root cause failed to save into database.")

                tracking_instance.update_tracking_single_column("result", get_now_str(tz))
                tracking_instance.update_tracking_single_column("url", Plot_Link)
                tracking_instance.update_tracking_single_column('status', final_status)

            else:
                logger.info(log_prefix + "Total time takes --- %s seconds ---" % int(time.time() - chart_start_time))
                logger.info(log_prefix + "No toggle found. ")
                final_status = final_status + "No toggle found"

                if mode == 'orion':
                    orion_result_instance.root_cause = final_status
                    status_code = orion_result_instance.post_json()
                    if status_code != 200:
                        logger.error(log_prefix + "http call update response code is " + str(status_code))
                        sys.exit(0)
                tracking_instance.update_tracking_single_column('status', final_status, ad_complete=True)
                continue
        except Exception as e:
            logger.error(log_prefix + "Total time takes --- %s seconds ---" % int(time.time() - chart_start_time))
            logger.error(log_prefix + str(e), exc_info=True)
            logger.error(log_prefix + "Analysis error for channel_id: " + str(channel_id)
                         + " ckc_id: " + str(ckc_id)
                         + " chart_type: " + str(chart_type))
            final_status = final_status + "Unexpected error during analysis."
            if tracking_instance is not None:

                if mode == 'orion':
                    orion_result_instance.root_cause = final_status
                    orion_result_instance.post_json()
                    if status_code != 200:
                        logger.error(log_prefix + "http call update response code is " + str(status_code))
                        sys.exit(0)
                tracking_instance.update_tracking_single_column('status', final_status, ad_complete=True)
            continue

    logger.info("-----------------Analysis Completes---------------")
    logger.info("Session total time takes --- %s seconds ---" % int(time.time() - session_start_time))
    logger.info("####################" * 4)
    logger.info("####################" * 4)
