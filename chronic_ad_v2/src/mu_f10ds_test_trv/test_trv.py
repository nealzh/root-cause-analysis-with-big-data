from mu_f10ds_common.teradata import TeradataUtil
from mu_f10ds_common import util
import pandas as pd
import test_trv_config as config
import os
from datetime import datetime as dt
from datetime import timedelta


class TestTravelerQuery:
    def __init__(self, fab, lot_list, current_step, query_session, teradata_config):
        self.fab = fab
        self.lot_list = lot_list
        self.current_step = current_step
        self.query_session = query_session
        self.config = teradata_config

    def test_trv(self):
        teradata_config = self.config
        server = teradata_config.get('server')
        user = teradata_config.get('user')
        password = teradata_config.get('password')
        td = TeradataUtil(server=server, user=user, password=password)
        file_dir = os.path.dirname(os.path.abspath(__file__))
        datetime_standard_format = config.TEST_TRV_CONFIG['datetime_standard_format']
        query_period_in_days = config.TEST_TRV_CONFIG['query_period_in_days']
        test_trv_sql_template_name = config.TEST_TRV_CONFIG['test_trv_sql_template']
        test_trv_sql_template = util.read_file(file_dir + "/tql/" + test_trv_sql_template_name)

        # reformat lot list
        if isinstance(self.lot_list, list):
            lot_list_csv_quoted = util.update_value_list_csv(self.lot_list)
        elif isinstance(self.lot_list, str):
            lot_list = self.lot_list.split(",")
            lot_list = [x.strip() for x in lot_list.split(",")]
            lot_list_csv_quoted = util.update_value_list_csv(lot_list)
        else:
            raise ValueError("Invalid format for lot list.")

        current_step = self.current_step
        # query_session_validation
        query_session = self.query_session

        if isinstance(query_session, dt):
            end_datetime_dt = query_session + timedelta(days=1)
            end_datetime_str = dt.strftime(query_session, datetime_standard_format)

        elif isinstance(query_session, str):
            end_datetime_dt = dt.strptime(query_session, datetime_standard_format)
            end_datetime_dt = end_datetime_dt + timedelta(days=1)
            end_datetime_str = dt.strftime(end_datetime_dt, datetime_standard_format)
        else:
            raise ValueError("Invalid format for query_session.")

        start_datetime_dt = end_datetime_dt - timedelta(days=query_period_in_days)
        start_datetime_str = dt.strftime(start_datetime_dt, datetime_standard_format)

        test_trv_sql = test_trv_sql_template.format(fab=self.fab,
                                                    lot_list_csv_quoted=lot_list_csv_quoted,
                                                    start_datetime=start_datetime_str,
                                                    end_datetime=end_datetime_str,
                                                    current_step=current_step)
        test_trv_df = td.execute_sql(sql=test_trv_sql, execution_only=False)

        if test_trv_df.shape[0] > 0:
            test_trv_df.columns = ['wafer_scribe', 'from_lot_id', 'lot_id', 'wafer_vendor', 'to_step',
                                   'wafer_id', 'from_step', 'from_wafer_id']
            test_trv_df = test_trv_df.drop_duplicates()
            return test_trv_df
        else:
            return pd.DataFrame()