# from mu_pysparksetup import MuPySparkSetup
import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import sys
import os
import logging

logger = logging.getLogger(__name__)

class PySparkUtil:
    """
    This class is to initialize a PySpark connection and execute query
    """
    def __init__(self, app_name, master, spark_config, log_level='ERROR'):
        """

        :param app_name: application name
        :param master: local or yarn
        :param spark_config: dict like configuration for spark
        :param log_level: logging level
        """
        self.app_name = app_name
        self.master = master
        self.config = spark_config
        self.log_level = log_level
        self.spark = None

    def initial_connection(self, version=2):
        """

        :param version: spark version
        :return: pyspark connection
        """

        ss = MuPySparkSetup('/usr/hdp/current', spark_version=version)
        spark = ss.init(name=self.app_name, master=self.master, config_parameters=self.config)
        sc = spark.sparkContext
        sc.setLogLevel(self.log_level)
        return spark

    def get_or_create_connection(self):
        if self.spark is not None:
            return self.spark
        else:
            self.spark = self.initial_connection()
            return self.spark

    def execute_sql(self, sql):
        """

        :param sql: hive query
        :return: pandas dataframe with data queried
        """
        if self.spark is None:
            self.get_or_create_connection()

        try:
            spark = self.spark
            logger.debug(sql)
            df_count_spark = spark.sql(sql)
            df = df_count_spark.toPandas()
            self.close_spark()
            return df
        except Exception as e:
            logger.error("sql failed.")
            logger.error("============================>>")
            logger.error(str(sql))
            logger.error("<<============================")
            logger.error(str(e))
            return pd.DataFrame()

    def close_spark(self):
        if self.spark is not None:
            self.spark.stop()
            self.spark = None

class MuPySparkSetup:

    def __init__(self, hdp_home=None, spark_version=None):
        import os
        from os.path import join
        self.spark_version = spark_version

        # Hadoop root
        if hdp_home is not None:
            os.environ['HDPBASE'] = hdp_home
        elif 'HDPBASE' in list(os.environ.keys()):
            pass
        else:
            os.environ['HDPBASE'] = "/usr/hdp/current/"

        # Root of spark installation
        if spark_version == 2:
            if 'spark2' in os.listdir(os.environ['HDPBASE']):
                os.environ['SPARK_HOME'] = join(os.environ['HDPBASE'], "spark2")
                self.shell_arg = "pyspark-shell"
            elif 'spark2-client' in os.listdir(os.environ['HDPBASE']):
                os.environ['SPARK_HOME'] = join(os.environ['HDPBASE'], "spark2-client")
                self.shell_arg = "pyspark-shell"
            os.environ['SPARK_MAJOR_VERSION'] = '2'
        else:
            if 'spark' in os.listdir(os.environ['HDPBASE']):
                os.environ['SPARK_HOME'] = join(os.environ['HDPBASE'], "spark")
                self.shell_arg = "pyspark-shell"
            elif 'spark-client' in os.listdir(os.environ['HDPBASE']):
                os.environ['SPARK_HOME'] = join(os.environ['HDPBASE'], "spark-client")
                self.shell_arg = "pyspark-shell"

        # Root of Spark related python libraries
        os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"

        # Specific python package paths
        python_libs = [l for l in os.listdir(os.environ["PYLIB"]) if os.path.splitext(l)[1] == ".zip"]
        for lib in python_libs:
            sys.path.insert(0, join(os.environ["PYLIB"], lib))
        sys.path.insert(0, os.environ["SPARK_HOME"] + "/python")

        self.jars = []
        self.coordinates = []

    def add_jars(self, jars):
        self.jars += jars

    def get_jars(self):
        return self.jars

    def add_coordinates(self, coord):
        """
        Adds maven coordinates to the list of dependency jars
        :param: coord  A list of maven coordinates
        """
        self.coordinates += coord

    def init(self, name=None, master='yarn-client', config_parameters=None, interpreter_path=None,
             dependent_jars=None, pypath='pyspark.zip:py4j-0.10.6-src.zip'):
        from pyspark import SparkConf
        from pyspark.context import SparkContext
        all_env_var_str = ''

        if name is None:
            raise ValueError('Please specify a name for the spark application')
        else:
            name_str = " --name " + name.replace(" ", "")

        if dependent_jars is None:
            dependent_jars = []
        else:
            for dep_jars in dependent_jars:
                self.add_jars(dep_jars)
            driver_jars = ":".join(self.get_jars())
            all_env_var_str = "--driver-class-path " + driver_jars

        if 'spark.driver.memory' in list(config_parameters.keys()):
            d_mem_str = " --driver-memory " + config_parameters['spark.driver.memory'].replace(" ", "")
            all_env_var_str = all_env_var_str + d_mem_str + name_str + " " + self.shell_arg
            os.environ["PYSPARK_SUBMIT_ARGS"] = all_env_var_str.replace("  ", " ")
        else:
            all_env_var_str = all_env_var_str + name_str + " " + self.shell_arg
            os.environ["PYSPARK_SUBMIT_ARGS"] = all_env_var_str.replace("  ", " ")

        if interpreter_path is None:
            interpreter_path = sys.executable
        os.environ["PYSPARK_PYTHON"] = interpreter_path

        conf = SparkConf()
        conf.setMaster(master)
        conf.set('spark.jars', ",".join(self.get_jars()))
        conf.set('spark.jars.packages', ",".join(self.coordinates))
        conf.set('spark.jars', ",".join(self.get_jars()))
        conf.set('spark.yarn.dist.files',
                 'file:/usr/hdp/current/spark2-client/python/lib/pyspark.zip,'
                 'file:/usr/hdp/current/spark2-client/python/lib/py4j-0.10.6-src.zip')
        conf.set('spark.sql.codegen.wholeStage', 'false')
        if config_parameters is None:
            config_parameters = {}
        else:
            for option in list(config_parameters.keys()):
                conf.set(option, config_parameters[option].replace(" ", ""))

        if self.spark_version == 2:
            from pyspark.sql import SparkSession
            conf.setExecutorEnv('PYTHONPATH', pypath)
            spark = SparkSession \
                .builder \
                .enableHiveSupport() \
                .appName(name) \
                .config(conf=conf) \
                .getOrCreate()
            sc = spark.sparkContext
            sc.setLogLevel("ERROR")
            # sc.setLogLevel("info")

        else:
            conf.setAppName(name)
            sc = SparkContext.getOrCreate(conf=conf)

        sc.setCheckpointDir("tmp")

        print("Application Name: ", sc.appName)
        print("Application ID: ", sc.applicationId)
        print("Tracking URL: http://{}name3:8088/cluster/app/{}/".format(os.uname()[1][:7],
                                                                         sc.applicationId))
        return spark if self.spark_version == 2 else sc
