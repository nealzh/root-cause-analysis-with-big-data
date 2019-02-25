from mu_f10ds_common import teradata
from mu_f10ds_common import util
import pandas as pd
import qdr_config as config
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

class QDRQuery:
    def __init__(self, lot_id_list, wafer_id_list, fab, teradata_config):
        self.lot_id_quoted_csv = util.update_value_list_csv([str(x)[0:7] + "%" for x in list(set(lot_id_list))], ',')
        self.wafer_id_quoted_csv = util.update_value_list_csv(list(set(wafer_id_list)), ',')
        self.fab = fab
        self.config = teradata_config

    def qdr(self, data_type):
        teradata_config = self.config
        server = teradata_config.get('server')
        user = teradata_config.get('user')
        password = teradata_config.get('password')

        td = teradata.TeradataUtil(server=server, user=user, password=password)
        teradata_connection = td.initial_connection()

        # read file
        file_dir = os.path.dirname(os.path.abspath(__file__))

        if data_type == 'wafer':

            qdr_wafer_sql_template = config.QDR_CONFIG['qdr_wafer_sql_template']
            qdr_sql_template = util.read_file(file_dir + "/tql/" + qdr_wafer_sql_template)

            qdr_sql = qdr_sql_template.format(fab=self.fab,
                                              lot_id=self.lot_id_quoted_csv,
                                              wafer_id=self.wafer_id_quoted_csv
                                              )

            qdr_df = pd.read_sql(qdr_sql, teradata_connection)

            if qdr_df.shape[0] > 0:
                qdr_df.columns = ['QDR_NO', 'LOT_NO', 'WAFER_ID', 'EQUIP_ID', 'QDR_TEXT', 'StepName']

                qdr_df['StepName'] = qdr_df['StepName'].astype(str)
                qdr_df['EQUIP_ID'] = qdr_df['EQUIP_ID'].astype(str)
                qdr_df['QDR_NO'] = qdr_df['QDR_NO'].astype(str)

                qdr_result = qdr_df[['LOT_NO', 'WAFER_ID', 'StepName', 'QDR_NO']].copy()
                qdr_result = qdr_result.rename(columns={'LOT_NO': 'lot_id',
                                                        'WAFER_ID': 'wafer_id',
                                                        'StepName': 'mfg_process_step',
                                                        'QDR_NO': 'qdr'})
                td._close_conn()
                return qdr_result

            else:
                td._close_conn()
                return pd.DataFrame()

        elif data_type == 'lot':

            qdr_lot_sql_template = config.QDR_CONFIG['qdr_lot_sql_template']
            qdr_sql_template = util.read_file(file_dir + "/tql/" + qdr_lot_sql_template)

            qdr_sql = qdr_sql_template.format(fab=self.fab,
                                              lot_id=self.lot_id_quoted_csv
                                              )
            qdr_df = pd.read_sql(qdr_sql, teradata_connection)

            if qdr_df.shape[0] > 0:
                qdr_df.columns = ['QDR_NO', 'LOT_NO', 'WAFER_ID', 'EQUIP_ID', 'QDR_TEXT', 'StepName']

                qdr_df['StepName'] = qdr_df['StepName'].astype(str)
                qdr_df['EQUIP_ID'] = qdr_df['EQUIP_ID'].astype(str)
                qdr_df['QDR_NO'] = qdr_df['QDR_NO'].astype(str)

                qdr_result = qdr_df[['LOT_NO', 'WAFER_ID', 'StepName', 'QDR_NO']].copy()
                qdr_result['WAFER_ID'] = ""
                qdr_result = qdr_result.rename(columns={'LOT_NO': 'lot_id',
                                                        'WAFER_ID': 'wafer_id',
                                                        'StepName': 'mfg_process_step',
                                                        'QDR_NO': 'qdr'})
                qdr_result = qdr_result.drop_duplicates()
                td._close_conn()

                return qdr_result
            else:
                td._close_conn()
                return pd.DataFrame()


