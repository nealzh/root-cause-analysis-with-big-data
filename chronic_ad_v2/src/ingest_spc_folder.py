from mu_f10ds_common.util import get_now_str
from functions import get_logger
from mu_f10ds_common.mssql import MSSQLUtil
from mu_f10ds_common.teradata import TeradataUtil
import pandas as pd
import yaml
import argparse
import base64
import sys
import pprint

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get input from command line.')
    parser.add_argument('--fab', action="store", dest='fab', type=int, required=True, help='fab number, eg, 10, 7...')
    parser.add_argument('--config', action="store", dest='config', required=True, help='configuration file')
    parser.add_argument('--debug', action="store_true", dest='debug', required=False, default=False,
                        help='disable or enable debug mode in logging. ')

    user_input_config = parser.parse_args()
    fab = user_input_config.fab
    config_file = user_input_config.config
    verbose = user_input_config.debug


    # Decide which configuration
    with open(config_file, 'r') as stream:
        config = yaml.load(stream)
    # pprint.pprint(config)
    tz = config.get('tz')
    logger = get_logger(tz, debug=verbose)
    logger.info("####################"*4)
    logger.info("=" * 40 + " script starts " + "=" * 40)
    datetime_standard_format = config.get('datetime_standard_format')

    mssql_config = config.get('MSSQL', "")
    teradata_config = config.get('TERADATA', "")
    teradata_server = teradata_config.get('server')
    teradata_user = teradata_config.get('user')
    teradata_password = base64.b64decode(teradata_config.get('password'))
    td = TeradataUtil(server=teradata_server, user=teradata_user, password=teradata_password)
    mssql_server = mssql_config.get('server')
    mssql_user = mssql_config.get('user')
    mssql_password = base64.b64decode(mssql_config.get('password'))
    mssql_database = mssql_config.get('database')
    mssql_port = mssql_config.get('port')
    mssql_query_instance = MSSQLUtil(mssql_server, mssql_user, mssql_password, mssql_database, mssql_port)
    table = mssql_config.get('table_name')

    sql_template = """
            SELECT 
                {fab} AS fab,
                tcd.CF_VALUE_09 as col_type, 
                tcd.CF_VALUE_10 as channel_type, 
                tcd.CF_VALUE_11 as module, 
                tcd.CH_ID as ch_id, 
                tcd.PARAMETER_NAME as parameter_name, 
                tcd.CH_NAME as ch_name, 
                tf.FOLDER_NAME AS folder,  
                tf2.FOLDER_NAME AS parent_folder
            FROM FAB_{fab}_SPC_DM.T_CHANNEL_DEF tcd
            INNER JOIN 
            FAB_{fab}_SPC_DM.T_FOLDER tf
            ON tcd.FOLDER_ID = tf.FOLDER_ID
            INNER JOIN 
            FAB_{fab}_SPC_DM.T_FOLDER tf2
            ON tf.PARENT_FOLDER_ID = tf2.FOLDER_ID
            WHERE 
            tcd.ACTIVE = 'Y' AND 
            (not module in ('PROBE', 'CHEM LAB', 'AMHS', 'WFRCHAR'));  
    """

    sql = sql_template.format(fab=fab)

    df = td.execute_sql(sql, execution_only=False)

    df.columns = ['fab', 'col_type', 'channel_type', 'module', 'ch_id', 'parameter_name', 'ch_name', 'folder', 'parent_folder']

    logger.info("Teradata query is complete. Data size is " + str(df.shape[0]))


    if df.shape[0] > 0:
        df['updated_datetime'] = get_now_str(tz)
        df = df.fillna("")
        # print(df.head(5))
        mssql_query_instance.empty_table(table=table, fab=fab)
        logger.info("Table is emptied. Ready to insert new records")
        mssql_query_instance.insert_df(df=df, table=table)
        logger.info("Loading is complete.")

    logger.info("Script ends....")
    logger.info("####################" * 4)
    sys.exit(0)
