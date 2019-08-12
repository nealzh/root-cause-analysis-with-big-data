import pandas as pd
from space_query import SpaceQuery
from sigma_query import SigmaQuery
import logging

logger = logging.getLogger(__name__)


class DataQueryInterface:
    """
    each query method support at least 2 modes
    None or "": no need to query, skip and return empty dataframe
    dataset specific mode, basically this is to cater different query method.
    for example: space data, normally we can query from Hive by Pyspark, but for test, we can query from MSSQL
                 if data is already in MSSQL. for ORION, it can only get msg.


    space_data: hive, mssql, hbase
    sigma_data: hbase
    qdr_data: teradata
    swr_data: teradata
    pmcm_data: teradata
    lot_attribute_data: teradata
    fmea_data: file
    domain_knowledge: mapping


    """
    def __init__(self, fab, config):
        self.fab = int(fab)
        self.config = config

    def query_space_data(self, ch_ckc_list, from_datetime, to_datetime):
        mode = self.config.get('space_data_mode', None)
        if mode is None:
            return pd.DataFrame()

        mode = mode.lower()

        if mode not in ('hive', 'mssql', 'msg'):
            raise ValueError("Space Data Query Mode is not supported. Supported mode is 'hive', 'mssql', 'msg'")

        if mode == 'hive':
            # Use Spark to query hive.
            try:
                spark_config = self.config.get('spark_config')
                vio_type_list_csv = self.config.get('vio_type_list_csv')
                area_condition_str = self.config.get('area_condition_str')
                latest_ooc_min_count = self.config.get('latest_ooc_min_count')
                start_date_latest_ooc_in_days = self.config.get('start_date_latest_ooc_in_days')
            except Exception as e:
                logger.error(str(e), exc_info=True)
                raise ValueError("Required configuration parameter cannot be found. Please check your config.")

            space_query_instance = SpaceQuery(fab=self.fab, spark_config=spark_config)
            df = space_query_instance.space_query_ooc_with_good(from_datetime=from_datetime, to_datetime=to_datetime,
                                                                vio_type_list_csv=vio_type_list_csv,
                                                                ch_ckc_list=ch_ckc_list,
                                                                area_condition_str=area_condition_str,
                                                                latest_ooc_min_count=latest_ooc_min_count,
                                                                start_date_latest_ooc_in_days=
                                                                start_date_latest_ooc_in_days)
            return df

        elif mode == 'mssql':
            # Use mssql to query space.
            try:
                #@TODO
                pass

            except Exception as e:
                logger.error(str(e), exc_info=True)
                raise ValueError("Required configuration parameter cannot be found. Please check your config.")

            # @TODO
            pass

        elif mode == 'msg':
            # Parse ORION Msg to get SPACE Data
            try:


                pass
            except Exception as e:
                logger.error(str(e), exc_info=True)
                raise ValueError("Required configuration parameter cannot be found. Please check your config.")


            pass

    def query_sigma_data(self):
        mode = self.config.get('sigma_data_mode', None)
        if mode is None:
            return pd.DataFrame()

        mode = mode.lower()

        if mode not in ('hbase'):
            raise ValueError("Sigma Data Query Mode is not supported. Supported mode is 'hbase'")

        try:
            host = self.config.get('hbase_host')
            port = self.config.get('hbase_port')
            cluster_type = self.config.get('hbase_cluster_type')

        except Exception as e:
            logger.error(str(e), exc_info=True)
            raise ValueError("Required configuration parameter cannot be found. Please check your config.")



        sigma_query_instance = SigmaQuery(self.fab, data_type)

        pass

    def query_qdr_data(self, mode):
        pass

    def query_swr_data(self, mode):
        pass

    def query_pmcm_data(self, mode):
        pass

    def query_fmea_data(self, mode):
        pass

    def query_domain_knowledge_data(self, mode):
        pass

    def query_lot_attribute_data(self, mode):
        pass
