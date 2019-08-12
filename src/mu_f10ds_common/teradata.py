import warnings
warnings.filterwarnings("ignore")
import os
import pyodbc
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class TeradataUtil:
    """
    This class is to create Teradata data connection and execute sql command.
    It support two mode, with data return or without data return
    with data return, it will return pandas dataframe

    """
    def __init__(self, server, user, password):
        """

        :param server: Teradata server name
        :param user: login user name
        :param password: login password
        """
        self.server = server
        self.user = user
        self.password = password
        self.conn = None

    def initial_connection(self):
        """
        This will initialize a Teradata connection by server, user and password provieded.

        :return: a Teradata connection
        """
        os.environ["ODBCINI"] = "/mucfg/unixodbc/2.3.1/odbc.ini"
        os.environ["ODBCSYSINI"] = "/mu/sdk/unixODBC/2.3.1-gcc443-rhel5-64/etc"
        os.environ["LD_LIBRARY_PATH"] = "/mu/sdk/teradata/teradata/client/15.10/lib64"
        conn = pyodbc.connect('DSN=' + self.server + ';UID=' + self.user + ';PWD=' + self.password + ';CHARSET=UTF8')
        conn.setencoding(str, encoding='utf-8')
        conn.setencoding(unicode, encoding='utf-8')
        return conn

    def get_or_create_connection(self):
        """
        This function is a get connection if it is already created or initialize one if not.

        :return: a Teradata connection
        """
        if self.conn is not None:
            return self.conn
        else:
            self.conn = self.initial_connection()
            return self.conn

    def execute_sql(self, sql, execution_only=False, new_col=None):
        """
        This function is to execute Teradata sql query.

        :param sql: sql to execute
        :param execution_only: if true, no data will return back.
        :param new_col: a list to rename columns of pandas dataframe
        :return: if execution_only is True, return 0/1. if execution_only is False, a pandas DataFrame will return
        """
        if self.conn is None:
            self.get_or_create_connection()

        if execution_only:
            try:
                cursor = self.conn.cursor()
                logger.debug(sql)
                cursor.execute(sql)
                self.conn.commit()
                self._close_conn()
                return 0
            except Exception as e:
                logger.error("sql failed.")
                logger.error("============================>>")
                logger.error(str(sql))
                logger.error("<<============================")
                logger.error(str(e))
                self._close_conn()
                return 1
        else:
            try:
                if new_col is None:
                    logger.debug(sql)
                    df = pd.read_sql(sql, self.conn)
                else:
                    logger.debug(sql)
                    df = pd.read_sql(sql, self.conn)
                    df.columns = new_col
                self._close_conn()
                return df
            except Exception as e:
                logger.error("sql failed.")
                logger.error("============================>>")
                logger.error(str(sql))
                logger.error("<<============================")
                logger.error(str(e))
                self._close_conn()
                return pd.DataFrame()

    def insert_df(self, df, table):
        if self.conn is None:
            self.get_or_create_connection()
        conn = self.conn

        types = df.apply(lambda x: pd.api.types.infer_dtype(x.values))
        for col in types[types == 'unicode'].index:
            df[col] = df[col].apply(lambda x: x.encode('utf-8').strip())

        records = [str(tuple(x)) for x in df.values]
        cursor = conn.cursor()
        column_list = df.columns.tolist()

        insert_ = """
                INSERT INTO """ + table + """
                (""" + ",".join(column_list) + """)
                VALUES """
        # print (insert_)
        for batch in self.chunker(records, 100):
            try:
                rows = ';'.join(batch)
                # insert_rows = insert_ + rows
                insert_rows = self.combine(insert_, rows)
                # print(insert_rows)
                cursor.execute(insert_rows)
                conn.commit()
            except:
                logger.error(insert_rows)
                continue

    def _close_conn(self):
        """
        This function is to close the connection of Teradata

        """
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    @staticmethod
    def chunker(seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    @staticmethod
    def combine(insert_, rows):
        insert = ""
        for i in range(len(rows.split(';'))):
            insert = insert + insert_ + rows.split(';')[i] + ';'
        insert = insert[2:]
        return insert

