from mu_f10ds_common import util
from mu_f10ds_common import teradata
import pandas as pd
import lt_config as config
import os
import logging
logger = logging.getLogger(__name__)

class LotAttributeQuery:
    def __init__(self, fab, lot_id_list, teradata_config):
        self.fab = fab
        self.lot_id_list = util.update_value_list_csv([str(x)[0:7] + "%" for x in list(set(lot_id_list))], ',')
        self.config = teradata_config

    def lot_attribute_query(self):
        try:
            teradata_config = self.config
            # initialize Teradata config
            server = teradata_config.get('server')
            user = teradata_config.get('user')
            password = teradata_config.get('password')

            td = teradata.TeradataUtil(server=server, user=user, password=password)
            teradata_connection = td.initial_connection()

            # read file
            file_dir = os.path.dirname(os.path.abspath(__file__))

            # lot attribute query
            lot_attribute_sql_template = config.LOT_ATTRIBUTE_CONFIG['lot_attribute_template']

            lot_attribute_sql = util.read_file(file_dir + "/tql/" + lot_attribute_sql_template)

            lot_attribute_exe = lot_attribute_sql.format(fab=self.fab,
                                                         num='{7}',
                                                         lot_id=self.lot_id_list)
            # logger.info(lot_attribute_exe)
            lot_attribute_df = pd.read_sql(lot_attribute_exe, teradata_connection)

            if lot_attribute_df.shape[0] > 0:
                lot_attribute_df.columns = ['mfg_process_step', 'lot_id', 'lot_attribute']

                lot_attribute_result = lot_attribute_df[['mfg_process_step', 'lot_id', 'lot_attribute']].copy().drop_duplicates()

                return lot_attribute_result
            else:
                return pd.DataFrame()
        except Exception as e:
            print(str(e))
            logger.error(str(e), exc_info=True)
            logger.error("Lot Attribute Query error.")
            return pd.DataFrame()
