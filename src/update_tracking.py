from mu_f10ds_common.util import *
import logging
import requests
import pprint
import pandas as pd

logger = logging.getLogger(__name__)


class SQLUpdateTracking:
    def __init__(self, analysis_type, fab, tz, ch_id, query_session, chart_type, mssql_conn,
                 tracking_table_name, no_tracking_flag):
        self.analysis_type = analysis_type
        self.fab = fab
        self.tz = tz
        self.ch_id = ch_id
        self.query_session = query_session
        self.chart_type = chart_type
        self.mssql_conn = mssql_conn
        self.tracking_table_name = tracking_table_name
        self.no_tracking_flag = no_tracking_flag

    def update_tracking_single_column(self, column, value, ad_complete=False):
        if self.no_tracking_flag is False:
            column_mapping = {
                'chart_type': 'chart_type',
                'ooc': 'ooc',
                'normal': 'normal',
                'process_step': 'process_step',
                'measurement_step': 'current_step',
                'analysis': 'analysis_ad_datetime',
                'received': 'received_ad_datetime',
                'result': 'result_update_ad',
                'status': 'final_status',
                'url': 'report_url',
                'update': 'updated_datetime'
            }

            conn = self.mssql_conn
            analysis_type = self.analysis_type
            fab = self.fab
            tz = self.tz
            chart_type = self.chart_type
            query_session = self.query_session
            ch_id = self.ch_id
            table_name = self.tracking_table_name

            column_list = ['analysis_type', 'fab', 'ch_id', 'chart_type', 'query_session']
            value_list = update_value_list([analysis_type, fab, ch_id, chart_type, query_session])
            value = update_value(value)
            now_str = format_datetime_string(get_now(tz), 'second')
            now = update_value(now_str)

            set_value_sql_str = ",".join([column_mapping[column] + "=" + value, column_mapping['update'] + "=" + now])
            where_condition_sql_str = " AND ".join([c + "=" + v for c, v in zip(column_list, value_list)])

            sql_template = """
                     UPDATE {table_name} 
                     SET {set_value_sql_str} 
                     WHERE {where_condition_sql_str} ;"""
            sql = sql_template.format(table_name=table_name,
                                      set_value_sql_str=set_value_sql_str,
                                      where_condition_sql_str=where_condition_sql_str)
            try:
                conn.execute_sql(sql, execution_only=True)
            except Exception as e:
                logger.error(str(e), exc_info=True)
                logger.error("Sql update failed. ")
                logger.error(sql)
        else:
            pass


class HBaseUpdateTracking:
    def __init__(self, tz, hbase_instance, rowkey, no_tracking_flag):
        self.tz = tz
        self.hbase_instance = hbase_instance
        self.rowkey = rowkey
        self.no_tracking_flag = no_tracking_flag

    def update_tracking_single_column(self, column, value, ad_complete=False):
        if self.no_tracking_flag is False:
            column_mapping = {
                'chart_type': 'cf:chart_type',
                'ad_completed': 'cf:ad_completed',
                'ooc': 'cf:ooc',
                'normal': 'cf:normal',
                'process_step': 'cf:process_step',
                'measurement_step': 'cf:current_step',
                'analysis': 'cf:analysis_ad_datetime',
                'received': 'cf:received_ad_datetime',
                'result': 'cf:result_update_datetime',
                'status': 'cf:final_status',
                'url': 'cf:report_url',
                'update': 'cf:updated_datetime'
            }

            conn = self.hbase_instance
            tz = self.tz
            rowkey = self.rowkey
            column = column_mapping.get(column)
            # value = update_value(value)
            now_str = format_datetime_string(get_now(tz), 'second')
            # now = update_value(now_str)

            try:
                data = {column: str(value)}
                ts = {'cf:updated_datetime': now_str}
                conn.put(rowkey=rowkey, data=data)
                conn.put(rowkey=rowkey, data=ts)
                if ad_complete:
                    ad_complete_column = column_mapping.get('ad_completed')
                    ad_complete_dict = {ad_complete_column: str(1)}
                    conn.put(rowkey=rowkey, data=ad_complete_dict)
            except Exception as e:
                logger.error(str(e), exc_info=True)
                logger.error("hbase tracking update failed. ")
                logger.error("rowkey is " + rowkey)
                logger.error("column is " + column)
                logger.error("value is " + str(value))
        else:
            pass


class OrionResultUpdating:
    def __init__(self, instance_id, post_url,  tz,
                 root_cause_category="Unknown",
                 root_cause="",
                 score_of_the_root_cause=0,
                 data_readiness_flag="NO",
                 plot_link=""):
        self.instance_id = instance_id
        self.post_url = post_url
        self.root_cause_category = root_cause_category
        self.root_cause = root_cause
        self.score_of_the_root_cause = score_of_the_root_cause
        self.data_readiness_flag = data_readiness_flag
        self.plot_link = plot_link
        self.tz = tz

    def compose_json_data(self):

        json_data_dict = {
            'recordType': 'adResultInsert',
            'instance_id': self.instance_id,
            'root_cause_category': self.root_cause_category,
            'root_cause': self.root_cause,
            'score_of_the_root_cause': self.score_of_the_root_cause,
            'data_readiness_flag': self.data_readiness_flag,
            'plot_link': self.plot_link,
            'updated_datetime': get_now_str(self.tz)
        }
        return json_data_dict

    def post_json(self):

        json_data = self.compose_json_data()
        pprint.pprint(json_data)
        response = requests.post(self.post_url, json=json_data)
        return response.status_code


class ImgUpdateTracking:
    def __init__(self, analysis_type, fab, tz, report_url, mssql_conn,
                 tracking_table_name, no_tracking_flag):
        self.analysis_type = analysis_type
        self.fab = fab
        self.tz = tz
        self.report_url = report_url
        self.mssql_conn = mssql_conn
        self.tracking_table_name = tracking_table_name
        self.no_tracking_flag = no_tracking_flag

    def update_pic_url(self, ooc_plot_url, results_plot_link_df):
        if self.no_tracking_flag is False:

            conn = self.mssql_conn
            analysis_type = self.analysis_type
            fab = self.fab
            tz = self.tz
            report_url = self.report_url
            table_name = self.tracking_table_name

            # column_list = ['analysis_type', 'fab', 'report_url', 'plot_type', 'pic_type', 'rank']
            # value_list = update_value_list([analysis_type, fab, report_url, plot_type, pic_type, rank])
            pic_url = ooc_plot_url
            now_str = format_datetime_string(get_now(tz), 'second')
            ooc_plot_dict = {'analysis_type': [analysis_type],
                             'fab': [fab],
                             'report_url': [report_url],
                             'plot_type': ['ooc_plot'],
                             'pic_type': ['trend'],
                             'rank': [1],
                             'pic_url': [pic_url],
                             'updated_datetime': [now_str]}
            ooc_plot_df = pd.DataFrame.from_dict(data=ooc_plot_dict)
            results_plot_link_df['updated_datetime'] = now_str
            try:
                # print(ooc_plot_df)
                conn.empty_table(table=table_name, report_url=report_url)
                conn.insert_df(df=ooc_plot_df, table=table_name)
                conn.insert_df(df=results_plot_link_df, table=table_name)
                # conn.execute_sql(sql, execution_only=True)
            except Exception as e:
                logger.error(str(e), exc_info=True)
                logger.error("Sql update failed. ")
        else:
            pass
