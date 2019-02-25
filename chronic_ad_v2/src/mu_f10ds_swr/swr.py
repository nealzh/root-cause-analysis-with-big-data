from mu_f10ds_common import teradata
from mu_f10ds_common import util
import pandas as pd
import swr_config as config
import os


'''
Note that when specifying lot id, wafer id, fab and timezone,
a standard format should be like as follows,

CASE1:
lot_id = 9395201.001
wafer_id = '5201-21'
fab = 10
tz = 'Asia/Singapore'

OR

CASE2:
lot_id = 9395201
wafer_id = '5201-21'
fab = 10
tz = 'Asia/Singapore'

As for CASE 2:
this class will automatically add '%' at the end of lot id 

'''

class SWRQuery:
    def __init__(self, lot_id_list, wafer_id_list, fab, teradata_config):
        self.lot_id_quoted_csv = util.update_value_list_csv([str(x)[0:7] + "%" for x in set(lot_id_list)], ',')
        self.wafer_id_quoted_csv = util.update_value_list_csv(set(wafer_id_list), ',')
        self.fab = fab
        self.config = teradata_config

    def swr(self, data_type):
        teradata_config = self.config
        server = teradata_config.get('server')
        user = teradata_config.get('user')
        password = teradata_config.get('password')

        td = teradata.TeradataUtil(server=server, user=user, password=password)
        teradata_connection = td.initial_connection()

        # read file
        file_dir = os.path.dirname(os.path.abspath(__file__))

        if data_type == 'wafer':
            swr_wafer_sql_template = config.SWR_CONFIG['swr_wafer_sql_template']

            swr_sql_template = util.read_file(file_dir + "/tql/" + swr_wafer_sql_template)

            swr_sql = swr_sql_template.format(fab=self.fab,
                                              lot_id=self.lot_id_quoted_csv,
                                              wafer_id=self.wafer_id_quoted_csv
                                              )
            swr_df = pd.read_sql(swr_sql, teradata_connection)
            if swr_df.shape[0] > 0:
                swr_df.columns = ['SWR_NO', 'SWR_TITLE', 'MT_LOT_ID', 'WAFER_ID', 'STEP_NAME']
                swr_df['STEP_NAME'] = swr_df['STEP_NAME'].astype(str)
                swr_df['SWR_NO'] = swr_df['SWR_NO'].astype(str)
                swr_df['SWR_TITLE'] = swr_df['SWR_NO'] + '_' + swr_df['SWR_TITLE']

                swr_result = swr_df[['MT_LOT_ID', 'WAFER_ID', 'STEP_NAME', 'SWR_TITLE']].copy()
                swr_result = swr_result.rename(columns={'MT_LOT_ID': 'lot_id',
                                                        'WAFER_ID': 'wafer_id',
                                                        'SWR_TITLE': 'swr',
                                                        'STEP_NAME': 'mfg_process_step'})
                swr_result = swr_result.drop_duplicates()
                td._close_conn()

                return swr_result

            else:
                td._close_conn()
                return pd.DataFrame()

        elif data_type == 'lot':
            swr_lot_sql_template = config.SWR_CONFIG['swr_lot_sql_template']

            swr_sql_template = util.read_file(file_dir + "/tql/" + swr_lot_sql_template)

            swr_sql = swr_sql_template.format(fab=self.fab,
                                              lot_id=self.lot_id_quoted_csv
                                              )
            swr_df = pd.read_sql(swr_sql, teradata_connection)

            if swr_df.shape[0] > 0:
                swr_df.columns = ['SWR_NO', 'SWR_TITLE', 'MT_LOT_ID', 'WAFER_ID', 'STEP_NAME']

                swr_df['STEP_NAME'] = swr_df['STEP_NAME'].astype(str)
                swr_df['SWR_NO'] = swr_df['SWR_NO'].astype(str)
                swr_df['SWR_TITLE'] = swr_df['SWR_TITLE'].astype(str)
                swr_df['SWR_TITLE'] = swr_df['SWR_NO'] + '_' + swr_df['SWR_TITLE']
                swr_result = swr_df[['MT_LOT_ID', 'WAFER_ID', 'STEP_NAME', 'SWR_TITLE']].copy()
                swr_result = swr_result.rename(columns={'MT_LOT_ID': 'lot_id',
                                                        'WAFER_ID': 'wafer_id',
                                                        'SWR_TITLE': 'swr',
                                                        'STEP_NAME': 'mfg_process_step'})
                swr_result['wafer_id'] = ""
                swr_result = swr_result.drop_duplicates()
                td._close_conn()
                return swr_result
            else:
                td._close_conn()
                return pd.DataFrame()