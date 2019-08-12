from mu_f10ds_common import hbase
import numpy as np
import sigma_conf


class SigmaQuery:
    """
    This class is to
    """
    def __init__(self, fab, data_type, host, port, cluster_type='prod',
                 columns=None, include_timestamp=True, stop_step=None, step_prefix=None):
        self.fab = fab
        self.data_type = data_type
        self.host = host
        self.port = port
        self.cluster_type = cluster_type
        self.columns = columns
        self.include_timestamp = include_timestamp
        self.stop_step = stop_step
        self.step_prefix = step_prefix
        self.table_name = None
        self.tz = None

    def get_table_name(self):
        if self.table_name is None:
            try:
                table_name = sigma_conf.table_name.get(self.cluster_type).get(self.data_type)
            except Exception:
                raise ValueError("Sigma HBase table is not found.")

            try:
                region = sigma_conf.region_mapping.get(self.fab)
            except Exception:
                raise ValueError("Fab region is not defined.")
            self.table_name = table_name.format(fab=self.fab, region=region)
            return self.table_name
        else:
            return self.table_name

    def get_rowkey(self, lot_id, wafer_id):
        if self.data_type == 'lot':
            rowkey = self.reverse_lot_id(lot_id)
        else:
            rowkey = self.reverse_lot_id(lot_id)[0:7] + "_" + wafer_id
        return rowkey

    def get_timezone(self):
        tz = sigma_conf.tz_mapping.get(self.fab, "")

        if tz == "":
            raise ValueError("Fab timezone is not defined.")
        else:
            self.tz = tz
            return tz

    @staticmethod
    def reverse_lot_id(lot_id):
        """
        :return: reversed lot id, 8765431.001 --> 3456781.001
        """
        return str(lot_id)[0:6][::-1] + str(lot_id)[6:]

    def sigma_query(self, lot_id, wafer_id=""):
        table_name = self.get_table_name()
        tz = self.get_timezone()
        rowkey = self.get_rowkey(lot_id=lot_id, wafer_id=wafer_id)
        hbase_instance = hbase.HBaseUtil(host=self.host, port=self.port, table=table_name, tz=tz)
        df = hbase_instance.scan(rowkey, self.columns, self.include_timestamp)
        if df.shape[0] > 0:
            df = self.split_rowkey(df)
            df = self.filter_before_stop_step(df)
            df = self.filter_step_prefix(df)
            return df
        else:
            print("No data returned from Sigma HBase")
            return df

    def split_rowkey(self, df):
        df_tmp = df.copy()
        if self.data_type == 'lot':
            df_tmp['lot_id'], df_tmp['mfg_process_step'] = df_tmp['rowkey'].str.split('_', 1).str
        else:
            df_tmp['lot_id'], df_tmp['wafer_id'], df_tmp['mfg_process_step'] = df_tmp['rowkey'].str.split('_', 2).str

        df_tmp['lot_id'] = [self.reverse_lot_id(x) for x in df_tmp['lot_id']]
        df_tmp = df_tmp.drop(['rowkey'], axis=1)
        return df_tmp

    def filter_before_stop_step(self, df):
        df_tmp = df.copy()
        if self.include_timestamp and (self.stop_step is not None):

            if df_tmp[df_tmp['mfg_process_step'] == self.stop_step].shape[0] > 0:
                stop_step_timestamp = np.max(
                    df_tmp[df_tmp['mfg_process_step'] == self.stop_step]['timestamp'])
                df_tmp = df_tmp[df_tmp['timestamp'] <= stop_step_timestamp]\
                    .sort_values('timestamp', ascending=True).reset_index(drop=True)
        return df_tmp

    def filter_step_prefix(self, df):
        if self.step_prefix is not None:
            step_prefix_list = self.step_prefix
            selected_step_flag = [x[0] in step_prefix_list for x in df['mfg_process_step']]
            df = df[selected_step_flag].reset_index(drop=True)
        return df

