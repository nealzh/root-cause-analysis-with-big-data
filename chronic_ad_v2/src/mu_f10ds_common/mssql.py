import warnings
warnings.filterwarnings("ignore")
import pymssql
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class MSSQLUtil:
    """

    """
    def __init__(self, server, user, password, database, port=1433):
        self.server = server
        self.user = user
        self.password = password
        self.port = port
        self.db = database
        self.conn = None

    def _initial_connection(self):
        """

        :return:
        """
        conn = pymssql.connect(server=self.server, user=self.user,
                               password=self.password, port=self.port, database=self.db)
        return conn

    def _get_or_create_connection(self):
        """

        :return:
        """
        if self.conn is not None:
            return self.conn
        else:
            self.conn = self._initial_connection()
            return self.conn

    def execute_sql(self, sql, execution_only=False):
        """

        :param sql: sql to query
        :param execution_only: if true, no data will return back.
        :return:
        """
        if self.conn is None:
            self._get_or_create_connection()

        if execution_only:
            try:
                cursor = self.conn.cursor()
                cursor.execute(sql)
                self.conn.commit()
                self._close_conn()
                return 0
            except Exception as e:
                print(sql)
                logger.error("sql failed.")
                logger.error("============================>>")
                logger.error(str(sql))
                logger.error("<<============================")
                logger.error(str(e))
                self._close_conn()
                return 1
        else:
            try:
                df = pd.read_sql(sql, self.conn)
                self._close_conn()
                return df
            except Exception as e:
                print(sql)
                logger.error("sql failed.")
                logger.error("============================>>")
                logger.error(str(sql))
                logger.error("<<============================")
                logger.error(str(e))
                self._close_conn()
                return pd.DataFrame()

    def str_plus(self, x):
        if isinstance(x, int) | isinstance(x, float):
            return x
        else:
            return str(x).replace('\'', '_')

    def insert_df(self, df, table):
        if self.conn is None:
            self._get_or_create_connection()
        conn = self.conn

        types = df.apply(lambda x: pd.api.types.infer_dtype(x.values))
        for col in types[types == 'unicode'].index:
            # df[col] = df[col].apply(lambda x: x.encode('utf-8').strip())
            df[col] = df[col].str.encode('utf-8')

        records = [str(tuple(map(self.str_plus, x))) for x in df.values]
        cursor = conn.cursor()
        column_list = df.columns.tolist()

        insert_ = """
                INSERT INTO """ + table + """
                (""" + ",".join(column_list) + """)
                VALUES """
        for batch in self.chunker(records, 5):
            try:
                rows = ','.join(batch)
                insert_rows = insert_ + rows
                cursor.execute(insert_rows)
                conn.commit()
            except:
                print(insert_rows)
                logger.error(insert_rows)
                continue

    def empty_table(self, table, **kwargs):
        where_condition_str = ""
        sql_template = "DELETE FROM {table} {where_condition_str};"

        if len(kwargs) > 0:
            where_condition_list = []

            for key, value in kwargs.items():
                where_condition_list.append(str(key) + "=" + str(self.update_value(value)))
            where_condition_str = " WHERE " + " AND ".join(where_condition_list)

        sql = sql_template.format(table=table, where_condition_str=where_condition_str)
        logger.debug(sql)
        self.execute_sql(sql, execution_only=True)

    def left_join_insert(self, staging_table, final_table, join_keys, min_datetime, datetime_col="query_session", **kwargs):
        join_condition_str = " AND ".join(["stage.{key} = origin.{key}".format(key=key) for key in join_keys])

        where_condition_str = ""
        if len(kwargs) > 0:
            where_condition_list = [""]

            for key, value in kwargs.items():
                where_condition_list.append(str(key) + "=" + str(self.update_value(value)))
            where_condition_str = " AND ".join(where_condition_list)

        sql_template = """
            INSERT INTO {final_table}
            SELECT stage.* FROM 
                (SELECT * FROM {staging_table} WHERE {datetime_col} >= '{min_datetime}' {where_condition_str}) stage
            LEFT JOIN (SELECT * FROM {final_table} WHERE {datetime_col} >= '{min_datetime}' {where_condition_str}) origin
            ON 
                {join_condition_str}
            WHERE 
                origin.{datetime_col} is NULL;"""
        sql = sql_template.format(staging_table=staging_table, final_table=final_table, datetime_col=datetime_col,
                                  join_condition_str=join_condition_str,
                                  min_datetime=min_datetime, where_condition_str=where_condition_str)
        logger.debug(sql)
        self.execute_sql(sql, execution_only=True)

    @staticmethod
    def chunker(seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    def _close_conn(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    @staticmethod
    def update_value(value):
        if isinstance(value, int):
            return value
        else:
            return("\'" + str(value) + "\'")

    @staticmethod
    def update_value_list(value_list):
        value_list_quoted = []
        for value in value_list:
            if isinstance(value, int):
                value_list_quoted.append(str(value))
            else:
                value_list_quoted.append("\'" + str(value) + "\'")
        return value_list_quoted