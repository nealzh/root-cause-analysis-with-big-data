import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import mu_hbasethrift
from pytz import timezone, utc
from datetime import datetime as dt


class HBaseUtil:
    """
    This function is to initialize a connection to HBase and get/scan/put data from HBase
    """
    def __init__(self, host, port, table, tz):
        """

        :param host: HBase host
        :param port: port
        :param table: table name
        :param tz: timezone information for timestamp
        """
        self.host = host
        self.port = port
        self.table = table
        self.tz = tz
        self.conn = None
        self.tbl_conn = None

    def initial_connection(self):
        """

        :return: HBase connection
        """
        connection = mu_hbasethrift.Connection(self.host, self.port)
        connection.open()
        tbl_conn = connection.table(self.table)
        return connection, tbl_conn

    def get(self, rowkey, columns=None, include_timestamp=True):
        """

        :param rowkey: rowkey
        :param columns: column, support single column, list of column, dict of column {'original_name': 'new_name'}
        :param include_timestamp: flag to choose whether timestamp field is kept in the final pandas
        :return: pandas dataframe
        """
        if self.conn is None:
            self._get_or_create_connection()

        tbl_conn = self.tbl_conn
        if columns is not None:
            if isinstance(columns, list):
                row = tbl_conn.row(rowkey, include_timestamp=include_timestamp, columns=columns)
            elif isinstance(columns, str):
                row = tbl_conn.row(rowkey, include_timestamp=include_timestamp, columns=[columns])
            elif isinstance(columns, dict):
                row = tbl_conn.row(rowkey, include_timestamp=include_timestamp, columns=list(columns.keys()))
                row = dict((columns.get(key, key), value) for (key, value) in row.items())
            else:
                raise ("Wrong format of columns are passed")
        else:
            row = tbl_conn.row(rowkey, include_timestamp=include_timestamp)
        if include_timestamp:
            row_reconstruct = {}
            for col, cell in row.items():
                val, unix_ts = cell
                ts = self._utc_to_time(dt.fromtimestamp(int(unix_ts) / 1000), self.tz).strftime('%Y-%m-%d %H:%M:%S')
                row_reconstruct = self._merge_two_dicts(row_reconstruct, {col: [val, ts]})

            df = pd.DataFrame.from_dict(row_reconstruct, orient='index', columns=['value', 'timestamp']) \
                .reset_index(drop=False) \
                .rename(columns={'index': 'column'})

            df['rowkey'] = rowkey
        else:
            df = pd.DataFrame.from_dict(row, orient='index', columns=['value']) \
                .reset_index(drop=False) \
                .rename(columns={'index': 'column'})
            df['rowkey'] = rowkey
        df = df.reindex(columns=(['rowkey'] + list([a for a in df.columns if a != 'rowkey'])))
        self._close_conn()
        return df

    def scan(self, rowkey, columns=None, include_timestamp=True):
        """

        :param rowkey: rowkey
        :param columns: column, support single column, list of column, dict of column {'original_name': 'new_name'}
        :param include_timestamp: flag to choose whether timestamp field is kept in the final pandas
        :return: pandas dataframe
        """
        if self.conn is None:
            self._get_or_create_connection()

        tbl_conn = self.tbl_conn
        if columns is not None:
            if isinstance(columns, list):
                rows = tbl_conn.scan(row_prefix=rowkey, include_timestamp=include_timestamp, columns=columns)
            elif isinstance(columns, str):
                rows = tbl_conn.scan(row_prefix=rowkey, include_timestamp=include_timestamp, columns=[columns])
            elif isinstance(columns, dict):
                rows = tbl_conn.scan(row_prefix=rowkey, include_timestamp=include_timestamp,
                                     columns=list(columns.keys()))
            else:
                raise ("Wrong format of columns are passed")
        else:
            rows = tbl_conn.scan(row_prefix=rowkey, include_timestamp=include_timestamp)

        if include_timestamp:
            row_reconstruct = []
            for row in rows:
                rowkey, row = row
                col_reconstruct = []

                for col, cell in row.items():

                    val, unix_ts = cell
                    ts = self._utc_to_time(dt.fromtimestamp(int(unix_ts) / 1000), self.tz).strftime('%Y-%m-%d %H:%M:%S')
                    if isinstance(columns, dict):
                        col = columns.get(col, col)
                    col_reconstruct.append({'column': col, 'value': val, 'timestamp': ts, 'rowkey': rowkey})

                row_reconstruct = row_reconstruct + col_reconstruct
            df = pd.DataFrame(row_reconstruct).reset_index(drop=True)

        else:
            row_reconstruct = []
            for row in rows:
                rowkey, row = row
                col_reconstruct = []
                if isinstance(columns, dict):
                    row = dict((columns.get(key, key), value) for (key, value) in row.items())
                for col, val in row.items():
                    col_reconstruct.append({'column': col, 'value': val, 'rowkey': rowkey})
                row_reconstruct = row_reconstruct + col_reconstruct

            df = pd.DataFrame(row_reconstruct).reset_index(drop=True)
        df = df.reindex(columns=(['rowkey'] + list([a for a in df.columns if a != 'rowkey'])))
        self._close_conn()
        return df

    def put(self, rowkey, data=None, cf='cf'):
        """

        :param rowkey: rowkey
        :param data: dict like {column: value} or {'cf:column': value}
        :param cf: column family
        :return: None
        """
        if self.conn is None:
            self._get_or_create_connection()
        tbl_conn = self.tbl_conn
        if data is not None:
            if cf is not None:
                data_reformat = {}
                for col, val in data.items():
                    if ':' not in col:
                        col = cf + ':' + col
                    data_reformat = self._merge_two_dicts(data_reformat, {col: str(val)})
                tbl_conn.put(rowkey, data_reformat)
            else:
                tbl_conn.put(rowkey, data)

        self._close_conn()

    def _close_conn(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None
            self.tbl_conn = None

    def _get_or_create_connection(self):
        if self.conn is not None:
            return self.conn
        else:
            self.conn, self.tbl_conn = self.initial_connection()
            return self.conn

    def _merge_two_dicts(self, x, y):
        z = x.copy()  # start with x's keys and values
        z.update(y)  # modifies z with y's keys and values & returns None
        return z

    def _utc_to_time(self, naive, tz):
        return naive.replace(tzinfo=utc).astimezone(timezone(tz))