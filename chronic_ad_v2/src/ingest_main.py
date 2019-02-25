import argparse
import yaml
import pprint
from functions import get_logger, format_datetime_string, valid_step_name, choose_datetime_standard_format
from mu_f10ds_space.space_query import SpaceQuery
from mu_f10ds_common.util import get_now, get_now_str, get_now_offset, read_file
from mu_f10ds_common.mssql import MSSQLUtil
import pandas as pd
from datetime import datetime as dt
from datetime import timedelta
import base64
from pytz import timezone
from conf import *
import sys
import os

pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', -1)
pd.set_option('display.precision', 2)
if __name__ == "__main__":
    # Get Input from command line
    parser = argparse.ArgumentParser(description='Get input from command line.')
    parser.add_argument('--fab', action="store", dest='fab', type=int, required=True, help='fab number, eg, 10, 7...')
    parser.add_argument('--config', action="store", dest='config', required=True, help='configuration file')
    parser.add_argument('--debug', action="store_true", dest='debug', required=False, default=False,
                        help='disable or enable debug mode in logging. ')
    parser.add_argument('--start_sample_date', action="store", dest='start_sample_date', default="", required=False,
                        help='provide start_sample_date, format yyyy-mm-dd')
    parser.add_argument('--end_sample_date', action="store", dest='end_sample_date', default="", required=False,
                        help='provide end_sample_date, format yyyy-mm-dd')

    user_input_config = parser.parse_args()
    fab = user_input_config.fab
    config_file = user_input_config.config
    verbose = user_input_config.debug
    start_sample_date = user_input_config.start_sample_date
    end_sample_date = user_input_config.end_sample_date

    # Decide which configuration
    with open(config_file, 'r') as stream:
        config = yaml.load(stream)
    # pprint.pprint(config)
    tz = config.get('tz')
    datetime_standard_format = config.get('datetime_standard_format')
    analysis_type = config.get('analysis_type')

    spark_config = config.get('SPARK', "")
    mssql_config = config.get('MSSQL', "")
    filter_config = config.get('FILTER', "")
    chronic_config = config.get('CHRONIC', "")
    logger = get_logger(tz=tz, identifier=str(fab) + analysis_type, debug=verbose)

    logger.info("=" * 40 + " script starts " + "=" * 40)
    logger.info("Analysis type is " + analysis_type)

    if 'rda' in analysis_type:
        area_list = filter_config.get('area')
        folder_list = filter_config.get('folder')
        query_ooc_only_flag = filter_config.get('query_ooc_only_flag')
        vio_type_list_csv = filter_config.get('vio_type_list_csv')
        cutoff_hour = int(filter_config.get('cutoff_hour'))
        query_interval_in_seconds = int(filter_config.get('query_interval_in_seconds'))
        buffer_seconds = int(filter_config.get('buffer_seconds'))
        latest_ooc_min_count = int(filter_config.get('latest_ooc_min_count'))
        start_date_latest_ooc_in_hours = filter_config.get('ooc_sample_latest_check_in_hours')
        ooc_sample_num_check_in_hours = int(filter_config.get('ooc_sample_num_check_in_hours'))
        space_sample_period_in_hours = int(filter_config.get('space_sample_period_in_hours'))
        tracking_table_name = mssql_config.get('tracking_table_name')
        # Staging table for AD Tracking
        tracking_staging_table_name = mssql_config.get('tracking_staging_table_name')
        # Samples table for AD
        sample_table_name = mssql_config.get('sample_table_name')
        # Staging table for AD Samples
        sample_staging_table_name = mssql_config.get('sample_staging_table_name')
        ch_folder_table_name = mssql_config.get('ch_folder_table_name')

        server = mssql_config.get('server')
        user = mssql_config.get('user')
        password = base64.b64decode(mssql_config.get('password'))
        database = mssql_config.get('database')
        port = mssql_config.get('port')
        mssql_query_instance = MSSQLUtil(server, user, password, database, port)

        if end_sample_date == "":
            today_dt = get_now(tz)
            today_str = get_now_str(tz)
            today_date_dt = today_dt.date()
            if cutoff_hour >= 0:
                end_sample_cutoff_datetime_str = format_datetime_string(today_date_dt, "d")[0:10] \
                                                 + " " + str(cutoff_hour).zfill(2) + ":00:00"
            else:
                end_sample_cutoff_datetime_str = format_datetime_string(today_date_dt, "h")

            end_sample_cutoff_datetime_dt = dt.strptime(end_sample_cutoff_datetime_str, datetime_standard_format)

            if end_sample_cutoff_datetime_str > today_str:
                today_dt = get_now_offset(1, tz)
                today_date_dt = today_dt.date()
                if cutoff_hour >= 0:
                    end_sample_cutoff_datetime_str = format_datetime_string(today_date_dt, "d")[0:10] \
                                                     + " " + str(cutoff_hour).zfill(2) + ":00:00"
                    end_sample_cutoff_datetime_dt = dt.strptime(end_sample_cutoff_datetime_str, datetime_standard_format)
                else:
                    end_sample_cutoff_datetime_str = format_datetime_string(today_date_dt, "h")
                    end_sample_cutoff_datetime_dt = dt.strptime(end_sample_cutoff_datetime_str, datetime_standard_format)
        else:
            if cutoff_hour >= 0:
                end_sample_date = dt.strptime(end_sample_date, choose_datetime_standard_format(end_sample_date)).date()
                end_sample_cutoff_datetime_str = format_datetime_string(end_sample_date, "d")[0:10]\
                                                    + " " + str(cutoff_hour).zfill(2) + ":00:00"
                end_sample_cutoff_datetime_dt = dt.strptime(end_sample_cutoff_datetime_str, datetime_standard_format)
            else:
                end_sample_date = dt.strptime(end_sample_date, choose_datetime_standard_format(end_sample_date))
                end_sample_cutoff_datetime_str = format_datetime_string(end_sample_date, "h")
                end_sample_cutoff_datetime_dt = dt.strptime(end_sample_cutoff_datetime_str, datetime_standard_format)

        # Find start sample cut off datetime
        if start_sample_date == "":
            start_sample_cutoff_datetime_dt = end_sample_cutoff_datetime_dt - timedelta(
                seconds=query_interval_in_seconds) - timedelta(seconds=buffer_seconds)
            start_sample_cutoff_datetime_str = dt.strftime(start_sample_cutoff_datetime_dt, datetime_standard_format)
        else:
            if cutoff_hour >= 0:
                start_sample_cutoff_datetime_dt = dt.strptime(start_sample_date,
                                                              choose_datetime_standard_format(start_sample_date)).date()
                start_sample_cutoff_datetime_str = format_datetime_string(start_sample_cutoff_datetime_dt, "d")[0:10] \
                                                   + " " + str(cutoff_hour).zfill(2) + ":00:00"
                start_sample_cutoff_datetime_dt = dt.strptime(start_sample_cutoff_datetime_str, datetime_standard_format)
            else:
                start_sample_cutoff_datetime_dt = dt.strptime(start_sample_date,
                                                              choose_datetime_standard_format(start_sample_date)).date()
                start_sample_cutoff_datetime_str = format_datetime_string(start_sample_cutoff_datetime_dt, "h")
                start_sample_cutoff_datetime_dt = dt.strptime(start_sample_cutoff_datetime_str, datetime_standard_format)

        if start_sample_cutoff_datetime_str >= end_sample_cutoff_datetime_str:
            logger.error("start and end datetime is invalid. ")
            logger.error("start datetime is " + start_sample_cutoff_datetime_str)
            logger.error("end datetime is " + end_sample_cutoff_datetime_str)
            raise ValueError("start and end datetime is invalid.")

        now_str = get_now_str(tz)
        logger.info("Now is " + now_str)
        logger.info("Fab is " + str(fab))
        logger.info("start datetime is " + start_sample_cutoff_datetime_str)
        logger.info("end datetime is " + end_sample_cutoff_datetime_str)

        time_diff_seconds = (end_sample_cutoff_datetime_dt - start_sample_cutoff_datetime_dt).total_seconds()

        while time_diff_seconds >= query_interval_in_seconds:
            end_sample_cutoff_datetime_dt_tmp = start_sample_cutoff_datetime_dt +\
                                                timedelta(seconds=query_interval_in_seconds)
            end_sample_cutoff_datetime_str_tmp = format_datetime_string(end_sample_cutoff_datetime_dt_tmp, "s")
            logger.info("Start to query from " + start_sample_cutoff_datetime_str +
                        " to " + end_sample_cutoff_datetime_str_tmp)

            start_date_latest_ooc = end_sample_cutoff_datetime_dt_tmp - timedelta(hours=start_date_latest_ooc_in_hours)

            space_query_instance = SpaceQuery(fab=fab, spark_config=spark_config)

            df = space_query_instance.space_query_ooc_by_time(from_datetime=start_date_latest_ooc,
                                                              to_datetime=end_sample_cutoff_datetime_dt_tmp,
                                                              vio_type_list_csv=vio_type_list_csv,
                                                              area_condition_str=area_list)
            logger.info("=" * 40 + " channel and ckc query completes " + "=" * 40)
            logger.info("OOC charts size is " + str(df.shape[0]))

            # print(df.head(10))
            if query_ooc_only_flag:
                # @TODO add daily ooc query check
                df['analysis_type'] = analysis_type
                mssql_query_instance.empty_table(table=sample_staging_table_name, fab=fab, analysis_type=analysis_type)
                mssql_query_instance.insert_df(df, sample_staging_table_name)
                mssql_query_instance.left_join_insert(staging_table=sample_staging_table_name,
                                                      final_table=sample_table_name,
                                                      join_keys=['fab', 'ch_id', 'query_session',
                                                                 'analysis_type', 'chart_type', 'sample_id'],
                                                      min_datetime=start_sample_cutoff_datetime_str,
                                                      datetime_col="query_session",
                                                      fab=fab, analysis_type=analysis_type)
                logger.info("Charts are inserted into " + sample_table_name)
                start_sample_cutoff_datetime_dt = start_sample_cutoff_datetime_dt + \
                                                  timedelta(seconds=query_interval_in_seconds)
                start_sample_cutoff_datetime_str = format_datetime_string(start_sample_cutoff_datetime_dt, "s")
                time_diff_seconds = (end_sample_cutoff_datetime_dt - start_sample_cutoff_datetime_dt).total_seconds()
                continue

            if df.shape[0] > 0:
                # Get Unique ch_ckc_list
                ch_ckc_df = df[['ch_id']].drop_duplicates(keep='first')

                if len(folder_list) > 0:
                    file_dir = os.path.dirname(os.path.abspath(__file__))
                    ch_list_sql_template = read_file(file_dir + '/../sql/folder_query_template.sql')

                    if isinstance(folder_list, str):
                        folder_list_str = ' , '.join('\'' + str(x) + '\'' for x in folder_list.split(','))
                    elif isinstance(folder_list, list):
                        folder_list_str = ' , '.join('\'' + str(x) + '\'' for x in folder_list)
                    else:
                        raise ValueError("Invalid folder_list format. Required format is str or list.")

                    ch_list_sql = ch_list_sql_template.format(fab=fab, ch_folder_table_name=ch_folder_table_name,
                                                              folder_list=folder_list_str)
                    ch_list_df = mssql_query_instance.execute_sql(sql=ch_list_sql, execution_only=False)
                    logger.info("=" * 40 + " folder channel query completes " + "=" * 40)
                    logger.info("channel charts size is " + str(ch_list_df.shape[0]))
                    if ch_list_df.shape[0] > 0:
                        ch_list_df = ch_list_df.rename(columns={'ch_id': 'channel_id'})
                        ch_ckc_df['channel_id'] = [int(x.split("_")[0]) for x in ch_ckc_df['ch_id']]
                        ch_ckc_df = ch_ckc_df.merge(ch_list_df, on='channel_id', how='inner')
                        ch_ckc_df = ch_ckc_df.drop('channel_id', errors='ignore', axis=1)
                    else:
                        raise ValueError("No valid channel available.")
                logger.info("=" * 40 + " channel ckc query completes " + "=" * 40)
                logger.info("ch_ckc_list charts size is " + str(ch_ckc_df.shape[0]))
                # print(ch_ckc_df)

                ch_ckc_list = []
                # ch_ckc_df = df[['ch_id', 'ckc_id']].drop_duplicates(keep='first')
                for idx, row in ch_ckc_df.iterrows():
                    ch_id, ckc_id = row['ch_id'].split("_")
                    if ckc_id == 0:
                        ch_ckc_id = "_".join([str(ch_id), str(ckc_id)])
                        ch_ckc_list.append(ch_ckc_id)
                    else:
                        ch_ckc_id = "_".join([str(ch_id), str(ckc_id)])
                        ch_ckc_list.append(ch_ckc_id)
                        ch_ckc_id = "_".join([str(ch_id), '0'])
                        ch_ckc_list.append(ch_ckc_id)
                ch_ckc_list = list(set(ch_ckc_list))
                # print(ch_ckc_list)

                start_sample_date = end_sample_cutoff_datetime_dt_tmp - timedelta(hours=space_sample_period_in_hours)

                df_samples = space_query_instance.space_query_ooc_with_good(from_datetime=start_sample_date,
                                                                            to_datetime=end_sample_cutoff_datetime_dt_tmp,
                                                                            vio_type_list_csv=vio_type_list_csv,
                                                                            ch_ckc_list=ch_ckc_list,
                                                                            area_condition_str=area_list,
                                                                            latest_ooc_min_count=latest_ooc_min_count,
                                                                            start_date_latest_ooc_in_hours=
                                                                            ooc_sample_num_check_in_hours)
                logger.info("=" * 40 + " samples query completes " + "=" * 40)
                logger.info("samples size is " + str(df_samples.shape[0]))

                df_summary = df_samples.groupby(['fab', 'module', 'design_id', 'ch_id', 'current_step', 'process_step',
                                                 'parameter_name', 'chart_type', 'channel_type',
                                                 'query_session', 'label'])['sample_id'] \
                    .count().reset_index(drop=False) \
                    .pivot_table(index=['fab', 'module', 'design_id', 'ch_id', 'current_step', 'process_step',
                                        'parameter_name', 'chart_type', 'channel_type', 'query_session'],
                                 columns='label', values='sample_id', aggfunc='first', fill_value=0) \
                    .reset_index(drop=False).rename(columns={0: 'normal', 1: 'ooc'})

                # ch_ckc_df['ch_id'] = ch_ckc_df['ch_id'].astype(str) + '_' + ch_ckc_df['ckc_id'].astype(str)
                # ch_ckc_df['violation_chart'] = 1
                df_summary = df_summary.merge(ch_ckc_df[['ch_id']], on='ch_id', how='inner')
                # df_summary['violation_chart'] = df_summary['violation_chart'].fillna(0)

                logger.info("=" * 40 + " count summary completes " + "=" * 40)
                logger.info("Final chart size is " + str(df_summary.shape[0]))
                # print(df[df['ch_id'].str.startswith('728344')].head(1000))
                # print(df_summary.head(5))

                if df_summary.shape[0] > 0:
                    df_summary['analysis_type'] = analysis_type
                    mssql_query_instance.empty_table(table=tracking_staging_table_name, fab=fab,
                                                     analysis_type=analysis_type)
                    mssql_query_instance.insert_df(df_summary, tracking_staging_table_name)
                    mssql_query_instance.left_join_insert(staging_table=tracking_staging_table_name,
                                                          final_table=tracking_table_name,
                                                          join_keys=['fab', 'ch_id', 'query_session', 'analysis_type',
                                                                     'current_step', 'process_step'],
                                                          min_datetime=start_sample_cutoff_datetime_str,
                                                          datetime_col="query_session",
                                                          fab=fab, analysis_type=analysis_type)
                    logger.info("Charts are inserted into " + tracking_table_name)

                    df_samples['analysis_type'] = analysis_type
                    mssql_query_instance.empty_table(table=sample_staging_table_name, fab=fab, analysis_type=analysis_type)
                    mssql_query_instance.insert_df(df_samples, sample_staging_table_name)
                    mssql_query_instance.left_join_insert(staging_table=sample_staging_table_name,
                                                          final_table=sample_table_name,
                                                          join_keys=['fab', 'ch_id', 'query_session',
                                                                     'analysis_type', 'chart_type', 'sample_id'],
                                                          min_datetime=start_sample_cutoff_datetime_str,
                                                          datetime_col="query_session",
                                                          fab=fab, analysis_type=analysis_type)
                    logger.info("Charts are inserted into " + sample_table_name)

            else:
                logger.info("No OOC Charts available")


            start_sample_cutoff_datetime_dt = start_sample_cutoff_datetime_dt + \
                                              timedelta(seconds=query_interval_in_seconds)
            start_sample_cutoff_datetime_str = format_datetime_string(start_sample_cutoff_datetime_dt, "s")
            time_diff_seconds = (end_sample_cutoff_datetime_dt - start_sample_cutoff_datetime_dt).total_seconds()

    elif 'chronic' in analysis_type:
        chronic_chart_csv_path = chronic_config.get('chronic_chart_csv_path')
        datetime_origin_format = chronic_config.get('datetime_origin_format')
        back_check_days = chronic_config.get('back_check_days')

        space_sample_period_in_hours = int(filter_config.get('space_sample_period_in_hours'))
        vio_type_list_csv = filter_config.get('vio_type_list_csv')
        cutoff_hour = int(filter_config.get('cutoff_hour'))
        area_list = filter_config.get('area')
        query_interval_in_seconds = int(filter_config.get('query_interval_in_seconds'))
        buffer_seconds = int(filter_config.get('buffer_seconds'))
        latest_ooc_min_count = int(filter_config.get('latest_ooc_min_count'))
        start_date_latest_ooc_in_hours = filter_config.get('ooc_sample_latest_check_in_hours')

        tracking_table_name = mssql_config.get('tracking_table_name')
        # Staging table for AD Tracking
        tracking_staging_table_name = mssql_config.get('tracking_staging_table_name')
        # Samples table for AD
        sample_table_name = mssql_config.get('sample_table_name')
        # Staging table for AD Samples
        sample_staging_table_name = mssql_config.get('sample_staging_table_name')
        spc_chronic_table_name = mssql_config.get('spc_chronic_table_name')
        server = mssql_config.get('server')
        user = mssql_config.get('user')
        password = base64.b64decode(mssql_config.get('password'))
        database = mssql_config.get('database')
        port = mssql_config.get('port')
        mssql_query_instance = MSSQLUtil(server, user, password, database, port)

        if end_sample_date == "":
            today_dt = get_now(tz)
            today_str = get_now_str(tz)
            today_date_dt = today_dt.date()
            if cutoff_hour >= 0:
                end_sample_cutoff_datetime_str = format_datetime_string(today_date_dt, "d")[0:10] \
                                                 + " " + str(cutoff_hour).zfill(2) + ":00:00"
            else:
                end_sample_cutoff_datetime_str = format_datetime_string(today_date_dt, "h")

            end_sample_cutoff_datetime_dt = dt.strptime(end_sample_cutoff_datetime_str, datetime_standard_format)

            if end_sample_cutoff_datetime_str > today_str:
                today_dt = get_now_offset(1, tz)
                today_date_dt = today_dt.date()
                if cutoff_hour >= 0:
                    end_sample_cutoff_datetime_str = format_datetime_string(today_date_dt, "d")[0:10] \
                                                     + " " + str(cutoff_hour).zfill(2) + ":00:00"
                    end_sample_cutoff_datetime_dt = dt.strptime(end_sample_cutoff_datetime_str,
                                                                datetime_standard_format)
                else:
                    end_sample_cutoff_datetime_str = format_datetime_string(today_date_dt, "h")
                    end_sample_cutoff_datetime_dt = dt.strptime(end_sample_cutoff_datetime_str,
                                                                datetime_standard_format)
        else:
            if cutoff_hour >= 0:

                end_sample_date = dt.strptime(end_sample_date, choose_datetime_standard_format(end_sample_date)).date()
                end_sample_cutoff_datetime_str = format_datetime_string(end_sample_date, "d")[0:10]\
                                                    + " " + str(cutoff_hour).zfill(2) + ":00:00"
                end_sample_cutoff_datetime_dt = dt.strptime(end_sample_cutoff_datetime_str, datetime_standard_format)
            else:
                end_sample_date = dt.strptime(end_sample_date, choose_datetime_standard_format(end_sample_date))
                end_sample_cutoff_datetime_str = format_datetime_string(end_sample_date, "h")
                end_sample_cutoff_datetime_dt = dt.strptime(end_sample_cutoff_datetime_str, datetime_standard_format)

        # Find start sample cut off datetime
        if start_sample_date == "":
            start_sample_cutoff_datetime_dt = end_sample_cutoff_datetime_dt - timedelta(
                seconds=query_interval_in_seconds) - timedelta(seconds=buffer_seconds)
            start_sample_cutoff_datetime_str = dt.strftime(start_sample_cutoff_datetime_dt, datetime_standard_format)
        else:
            if cutoff_hour >= 0:
                start_sample_cutoff_datetime_dt = dt.strptime(start_sample_date,
                                                              choose_datetime_standard_format(str(start_sample_date))).date()
                start_sample_cutoff_datetime_str = format_datetime_string(start_sample_cutoff_datetime_dt, "d")[0:10] \
                                                   + " " + str(cutoff_hour).zfill(2) + ":00:00"
                start_sample_cutoff_datetime_dt = dt.strptime(start_sample_cutoff_datetime_str,
                                                              datetime_standard_format)
            else:
                start_sample_cutoff_datetime_dt = dt.strptime(start_sample_date,
                                                              choose_datetime_standard_format(start_sample_date)).date()
                start_sample_cutoff_datetime_str = format_datetime_string(start_sample_cutoff_datetime_dt, "h")
                start_sample_cutoff_datetime_dt = dt.strptime(start_sample_cutoff_datetime_str,
                                                              datetime_standard_format)

        if start_sample_cutoff_datetime_str >= end_sample_cutoff_datetime_str:
            logger.error("start and end datetime is invalid. ")
            logger.error("start datetime is " + start_sample_cutoff_datetime_str)
            logger.error("end datetime is " + end_sample_cutoff_datetime_str)
            raise ValueError("start and end datetime is invalid.")

        chronic_charts_df = pd.read_csv(chronic_chart_csv_path, header=0, low_memory=False)
        chronic_charts_df = chronic_charts_df[
            ['FAB', 'Area', 'CHART_TYPE', 'CHANNEL_ID', 'CKC_ID', 'DESIGN_ID', 'Review_Day', 'Category', 'Type',
             'PROCESS_STEP_NAME', 'STEP_NAME', 'Parameter_Name']].copy()

        old_column_list = ['FAB', 'Area', 'ch_id', 'Type', 'Category', 'DESIGN_ID', 'Review_Day', 'STEP_NAME',
                           'Parameter_Name', 'PROCESS_STEP_NAME']
        new_column_list = ['fab', 'module', 'ch_id', 'chart_type', 'channel_type', 'design_id', 'query_session',
                           'current_step', 'parameter_name', 'process_step']
        new_column_key_list = ['fab', 'ch_id', 'chart_type', 'query_session']
        column_mapping = dict(zip(old_column_list, new_column_list))

        chronic_charts_df['FAB'] = chronic_charts_df['FAB'].map(fab_mapping)
        chronic_charts_df = chronic_charts_df[chronic_charts_df['FAB'] == fab].copy()
        chronic_charts_df['category_code'] = chronic_charts_df['Category'].map(catrgory_mapping)
        chronic_charts_df['Type'] = chronic_charts_df['Type'].str.split('/').str[0]
        chronic_charts_df['Type'] = chronic_charts_df['Type'].map(type_mapping)
        chronic_charts_df['ch_id'] = chronic_charts_df['CHANNEL_ID'].astype(str) + '_' + chronic_charts_df[
            'CKC_ID'].astype(str)
        chronic_charts_df['Review_Day'] = [dt.strftime(dt.strptime(x, datetime_origin_format), datetime_standard_format)
                                           for x in chronic_charts_df['Review_Day']]

        # add into mssql
        # print(chronic_charts_df.head(5))
        # print(chronic_charts_df.columns)

        mssql_query_instance.empty_table(spc_chronic_table_name, fab=fab)
        mssql_query_instance.insert_df(chronic_charts_df.fillna("").drop_duplicates(['FAB', 'ch_id', 'Review_Day'],
                                                                                    keep="first"),
                                       spc_chronic_table_name)
        logger.info("Chronic OOC list is ingested into database. ")
        chronic_charts_df = chronic_charts_df[(chronic_charts_df['Review_Day'] >= start_sample_cutoff_datetime_str)
                                              & (chronic_charts_df['Review_Day'] <= end_sample_cutoff_datetime_str)]
        chronic_charts_df['STEP_NAME'] = [valid_step_name(x) for x in chronic_charts_df['STEP_NAME']]
        chronic_charts_df_renamed = chronic_charts_df.rename(columns=column_mapping)
        chronic_charts_df_renamed = chronic_charts_df_renamed[
            chronic_charts_df_renamed['category_code'] >= 1.5].reset_index(drop=True)
        chronic_charts_df_renamed = chronic_charts_df_renamed[new_column_list]
        chronic_charts_df_renamed = chronic_charts_df_renamed.drop_duplicates(new_column_key_list, keep="first")
        chronic_charts_df_renamed['analysis_type'] = analysis_type

        df_list = [x for _, x in chronic_charts_df_renamed.groupby(['fab', 'query_session', 'analysis_type'])]

        for single_chart_df in df_list:
            query_session = single_chart_df['query_session'].iloc[0]
            query_session_dt = dt.strptime(query_session, datetime_standard_format)
            end_sample_cutoff_datetime_dt = query_session_dt
            if cutoff_hour >= 0:
                end_sample_date = query_session_dt.date()
                end_sample_cutoff_datetime_str = format_datetime_string(end_sample_date, "d")[0:10]\
                                                    + " " + str(cutoff_hour).zfill(2) + ":00:00"
                end_sample_cutoff_datetime_dt = dt.strptime(end_sample_cutoff_datetime_str, datetime_standard_format)
            else:
                end_sample_cutoff_datetime_str = format_datetime_string(query_session_dt, "h")
                end_sample_cutoff_datetime_dt = dt.strptime(end_sample_cutoff_datetime_str, datetime_standard_format)

            start_sample_date = end_sample_cutoff_datetime_dt - timedelta(hours=space_sample_period_in_hours,
                                                                          seconds=buffer_seconds)

            single_chart_df['query_session'] = end_sample_cutoff_datetime_str
            ch_ckc_list = []
            # ch_ckc_df = df[['ch_id', 'ckc_id']].drop_duplicates(keep='first')
            for idx, row in single_chart_df.iterrows():
                ch_id, ckc_id = row['ch_id'].split("_")
                if ckc_id == 0:
                    ch_ckc_id = "_".join([str(ch_id), str(ckc_id)])
                    ch_ckc_list.append(ch_ckc_id)
                else:
                    ch_ckc_id = "_".join([str(ch_id), str(ckc_id)])
                    ch_ckc_list.append(ch_ckc_id)
                    ch_ckc_id = "_".join([str(ch_id), '0'])
                    ch_ckc_list.append(ch_ckc_id)
            ch_ckc_list = list(set(ch_ckc_list))
            # print(ch_ckc_list)

            logger.info("Start to query from " + str(start_sample_date) +
                        " to " + str(end_sample_cutoff_datetime_str))

            space_query_instance = SpaceQuery(fab=fab, spark_config=spark_config)
            df_samples = space_query_instance.space_query_ooc_with_good(from_datetime=start_sample_date,
                                                                        to_datetime=end_sample_cutoff_datetime_dt,
                                                                        vio_type_list_csv=vio_type_list_csv,
                                                                        ch_ckc_list=ch_ckc_list,
                                                                        area_condition_str=area_list,
                                                                        latest_ooc_min_count=latest_ooc_min_count,
                                                                        start_date_latest_ooc_in_hours=
                                                                        start_date_latest_ooc_in_hours)
            # print(df_samples.head(5))
            df_samples = df_samples.drop_duplicates(['fab', 'ch_id', 'sample_id', 'chart_type', 'query_session'])
            logger.info("=" * 40 + " samples query completes " + "=" * 40)
            logger.info("Samples query size is " + str(df_samples.shape[0]))
            # print(df_samples[df_samples['module'] == 'DRY ETCH'])

            df_summary = df_samples.groupby(['fab', 'module', 'design_id', 'ch_id', 'current_step', 'process_step',
                                             'parameter_name', 'chart_type', 'channel_type', 'query_session',
                                             'label'])['sample_id'] \
                .count().reset_index(drop=False) \
                .pivot_table(index=['fab', 'module', 'design_id', 'ch_id', 'current_step', 'process_step',
                                    'parameter_name', 'chart_type', 'channel_type', 'query_session'],
                             columns='label', values='sample_id', aggfunc='first', fill_value=0) \
                .reset_index(drop=False).rename(columns={0: 'normal', 1: 'ooc'})
            # print(df_summary[df_summary['module'] == 'DRY ETCH'])

            logger.info("=" * 40 + " chart query completes " + "=" * 40)
            logger.info("Charts query size is " + str(df_summary.shape[0]))

            single_chart_df = single_chart_df[['fab', 'ch_id', 'query_session', 'analysis_type']]\
                                        .merge(df_summary, on=['fab', 'ch_id', 'query_session'], how='inner')

            # print(single_chart_df[single_chart_df['module'] == 'DRY ETCH'])
            if single_chart_df.shape[0] > 0:
                logger.info("Final chart size to insert is " + str(single_chart_df.shape[0]))
                mssql_query_instance.empty_table(table=tracking_staging_table_name, fab=fab, analysis_type=analysis_type)
                mssql_query_instance.insert_df(single_chart_df, tracking_staging_table_name)
                mssql_query_instance.left_join_insert(staging_table=tracking_staging_table_name,
                                                      final_table=tracking_table_name,
                                                      join_keys=['fab', 'ch_id', 'query_session', 'analysis_type',
                                                                 'current_step', 'process_step'],
                                                      min_datetime=start_sample_cutoff_datetime_str,
                                                      datetime_col="query_session")
                logger.info("Charts are inserted into " + tracking_table_name + " Completed")

                logger.info("Final sample size to insert is " + str(df_samples.shape[0]))
                df_samples['analysis_type'] = analysis_type
                mssql_query_instance.empty_table(table=sample_staging_table_name, fab=fab, analysis_type=analysis_type)
                mssql_query_instance.insert_df(df_samples, sample_staging_table_name)
                mssql_query_instance.left_join_insert(staging_table=sample_staging_table_name,
                                                      final_table=sample_table_name,
                                                      join_keys=['fab', 'ch_id', 'query_session',
                                                                 'analysis_type', 'chart_type', 'sample_id'],
                                                      min_datetime=start_sample_cutoff_datetime_str,
                                                      datetime_col="query_session")
                logger.info("Samples are inserted into " + sample_table_name + " Completed")

        # print(chronic_charts_df_renamed.head(5))
    else:
        logger.error("Analysis_type " + analysis_type + " is invalid.")
        raise ValueError("Analysis_type " + analysis_type + " is invalid.")

    logger.info("Script run complete. ")

    sys.exit(0)




