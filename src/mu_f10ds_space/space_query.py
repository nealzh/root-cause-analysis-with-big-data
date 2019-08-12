from mu_f10ds_common import initial_pyspark
from mu_f10ds_common.util import read_file
from space_conf import region_mapping, default_spark_config, time_format_config, tz_mapping
from datetime import datetime as dt
from datetime import timedelta
import os
from pytz import timezone

class SpaceQuery:
    def __init__(self, fab, spark_config):
        self.fab = fab
        self.region = region_mapping.get(fab, "")
        self.spark_config = spark_config

    def space_query_hql(self, hql):
        app_name = self.spark_config.get("spark.app.name", default_spark_config.get('spark.app.name'))
        master = self.spark_config.get("spark.master", default_spark_config.get('spark.master'))
        spark = initial_pyspark.PySparkUtil(app_name=app_name, master=master, spark_config=self.spark_config)
        df = spark.execute_sql(hql)
        return df

    def space_query_ooc_by_time(self, from_datetime, to_datetime, vio_type_list_csv,
                                area_condition_str=""):
        file_name = 'ooc_samples_query.tql'
        ch_ckc_condition_str = ""

        if isinstance(vio_type_list_csv, list):
            vio_type_list_csv = ','.join(str(x) for x in vio_type_list_csv)

        if len(area_condition_str) > 0:
            if isinstance(area_condition_str, str):
                area_condition_str = ' , '.join('\'' + str(x) + '\'' for x in area_condition_str.split(','))
            elif isinstance(area_condition_str, list):
                area_condition_str = ' , '.join('\'' + str(x) + '\'' for x in area_condition_str)
            else:
                raise ValueError("Invalid area_condition_str format. Required format is str or list.")
            area_condition_str = " and channel_def.module in (" + area_condition_str + ") "
        else:
            area_condition_str = ""

        df = self._space_query(vio_type_list_csv=vio_type_list_csv,
                               from_datetime=from_datetime, to_datetime=to_datetime,
                               file_name=file_name, ch_ckc_condition_str=ch_ckc_condition_str,
                               area_condition_str=area_condition_str)
        return df

    def space_query_ooc_by_ch_ckc(self, from_datetime, to_datetime, vio_type_list_csv,
                                  ch_ckc_list, area_condition_str=""):
        file_name = 'ooc_samples_query.tql'
        if isinstance(vio_type_list_csv, list):
            vio_type_list_csv = ','.join(str(x) for x in vio_type_list_csv)

        if len(area_condition_str) > 0:
            if isinstance(area_condition_str, str):
                area_condition_str = ' , '.join('\'' + str(x) + '\'' for x in area_condition_str.split(','))
            elif isinstance(area_condition_str, list):
                area_condition_str = ' , '.join('\'' + str(x) + '\'' for x in area_condition_str)
            else:
                raise ValueError("Invalid area_condition_str format. Required format is str or list.")
            area_condition_str = " and channel_def.module in (" + area_condition_str + ") "
        else:
            area_condition_str = ""

        ch_ckc_condition_list = []
        for x in set(ch_ckc_list):
            n = len(str(x).split("_"))
            if n == 1:
                ch_ckc_condition_list.append("(samples_viol.ch_id=" + str(x) + ")")
            elif n == 2:
                ch_ckc_condition_list.append(
                    "(samples_viol.ch_id=" + str(str(x).split("_")[0]) + " and " +
                    "samples_viol.ckc_id=" + str(str(x).split("_")[1]) + ")")
            else:
                raise ValueError("Invalid ch_ckc_list format. Required format is ch_ckc or ch.")
        if len(ch_ckc_condition_list) == 1:
            ch_ckc_condition_str = " and " + "".join(ch_ckc_condition_list)
        elif len(ch_ckc_condition_list) >= 2:
            ch_ckc_condition_str = " and (" + " or ".join(ch_ckc_condition_list) + ")"
        else:
            ch_ckc_condition_str = ""

        df = self._space_query(vio_type_list_csv=vio_type_list_csv, ch_ckc_condition_str=ch_ckc_condition_str,
                               area_condition_str=area_condition_str,
                               from_datetime=from_datetime, to_datetime=to_datetime,
                               file_name=file_name)
        return df

    def space_query_ooc_with_good(self, from_datetime, to_datetime, vio_type_list_csv,
                                  ch_ckc_list, area_condition_str="", latest_ooc_min_count=1,
                                  start_date_latest_ooc_in_hours=24):
        file_name = 'samples_query_with_label.tql'
        if isinstance(vio_type_list_csv, list):
            vio_type_list_csv = ','.join(str(x) for x in vio_type_list_csv)

        if len(area_condition_str) > 0:
            if isinstance(area_condition_str, str):
                area_condition_str = ' , '.join('\'' + str(x) + '\'' for x in area_condition_str.split(','))
            elif isinstance(area_condition_str, list):
                area_condition_str = ' , '.join('\'' + str(x) + '\'' for x in area_condition_str)
            else:
                raise ValueError("Invalid area_condition_str format. Required format is str or list.")
            area_condition_str = " and channel_def.module in (" + area_condition_str + ") "
        else:
            area_condition_str = ""

        ch_ckc_condition_list = []
        for x in set(ch_ckc_list):
            n = len(str(x).split("_"))
            if n == 1:
                ch_ckc_condition_list.append("(ch_id=" + str(x) + ")")
            elif n == 2:
                ch_ckc_condition_list.append(
                    "(ch_id=" + str(str(x).split("_")[0]) + " and " +
                    "ckc_id=" + str(str(x).split("_")[1]) + ")")
            else:
                raise ValueError("Invalid ch_ckc_list format. Required format is ch_ckc or ch.")
        if len(ch_ckc_condition_list) == 1:
            ch_ckc_condition_str = " and " + "".join(ch_ckc_condition_list)
        elif len(ch_ckc_condition_list) >= 2:
            ch_ckc_condition_str = " and (" + " or ".join(ch_ckc_condition_list) + ")"
        else:
            ch_ckc_condition_str = ""

        if isinstance(to_datetime, str):
            to_datetime_naive_dt = dt.strptime(to_datetime, time_format_config.get('datetime_standard_format'))
            to_datetime_dt = timezone(tz_mapping.get(self.fab)).localize(to_datetime_naive_dt)
        elif isinstance(to_datetime, dt):
            to_datetime_dt = to_datetime
        else:
            raise ValueError("Invalid datetime format")

        start_date_latest_ooc = to_datetime_dt - timedelta(hours=start_date_latest_ooc_in_hours)
        start_date_latest_ooc_str = start_date_latest_ooc.strftime(time_format_config.get('datetime_standard_format'))
        df = self._space_query(vio_type_list_csv=vio_type_list_csv, ch_ckc_condition_str=ch_ckc_condition_str,
                               start_sample_date_latest_ooc=start_date_latest_ooc_str,
                               latest_ooc_min_count=latest_ooc_min_count,
                               area_condition_str=area_condition_str,
                               from_datetime=from_datetime, to_datetime=to_datetime,
                               file_name=file_name)
        return df

    def space_query_ooc_with_good_and_ckc_0(self, from_datetime, to_datetime, vio_type_list_csv,
                                  ch_ckc_list, area_condition_str="", latest_ooc_min_count=1,
                                            start_date_latest_ooc_in_hours=1):
        file_name = 'samples_query_with_label_and_ckc_0.tql'
        if isinstance(vio_type_list_csv, list):
            vio_type_list_csv = ','.join(str(x) for x in vio_type_list_csv)

        if len(area_condition_str) > 0:
            if isinstance(area_condition_str, str):
                area_condition_str = ' , '.join('\'' + str(x) + '\'' for x in area_condition_str.split(','))
            elif isinstance(area_condition_str, list):
                area_condition_str = ' , '.join('\'' + str(x) + '\'' for x in area_condition_str)
            else:
                raise ValueError("Invalid area_condition_str format. Required format is str or list.")
            area_condition_str = " and channel_def.module in (" + area_condition_str + ") "
        else:
            area_condition_str = ""

        ch_ckc_condition_list = []
        for x in set(ch_ckc_list):
            n = len(str(x).split("_"))
            if n == 1:
                ch_ckc_condition_list.append("(ch_id=" + str(x) + ")")
            elif n == 2:
                ch_ckc_condition_list.append(
                    "(ch_id=" + str(str(x).split("_")[0]) + " and " +
                    "ckc_id=" + str(str(x).split("_")[1]) + ")")
            else:
                raise ValueError("Invalid ch_ckc_list format. Required format is ch_ckc or ch.")
        if len(ch_ckc_condition_list) == 1:
            ch_ckc_condition_str = " and " + "".join(ch_ckc_condition_list)
        elif len(ch_ckc_condition_list) >= 2:
            ch_ckc_condition_str = " and (" + " or ".join(ch_ckc_condition_list) + ")"
        else:
            ch_ckc_condition_str = ""

        if isinstance(to_datetime, str):
            to_datetime_naive_dt = dt.strptime(to_datetime, time_format_config.get('datetime_standard_format'))
            to_datetime_dt = timezone(tz_mapping.get(self.fab)).localize(to_datetime_naive_dt)
        elif isinstance(to_datetime, dt):
            to_datetime_dt = to_datetime
        else:
            raise ValueError("Invalid datetime format")

        start_date_latest_ooc = to_datetime_dt - timedelta(hours=start_date_latest_ooc_in_hours)
        start_date_latest_ooc_str = start_date_latest_ooc.strftime(time_format_config.get('datetime_standard_format'))
        df = self._space_query(vio_type_list_csv=vio_type_list_csv, ch_ckc_condition_str=ch_ckc_condition_str,
                               start_sample_date_latest_ooc=start_date_latest_ooc_str,
                               latest_ooc_min_count=latest_ooc_min_count,
                               area_condition_str=area_condition_str,
                               from_datetime=from_datetime, to_datetime=to_datetime,
                               file_name=file_name)
        return df

    def _space_query(self, from_datetime, to_datetime, file_name, **kwargs):
        file_dir = os.path.dirname(os.path.abspath(__file__))
        template_path = os.path.join(file_dir, "hql", file_name)
        hive_sql_template = read_file(template_path)

        if isinstance(from_datetime, str):
            from_datetime_naive_dt = dt.strptime(from_datetime, time_format_config.get('datetime_standard_format'))
            from_datetime_dt = timezone(tz_mapping.get(self.fab)).localize(from_datetime_naive_dt)
            from_datetime_str = from_datetime_dt.strftime(time_format_config.get('datetime_standard_format'))

        elif isinstance(from_datetime, dt):
            from_datetime_dt = from_datetime
            from_datetime_str = from_datetime_dt.strftime(time_format_config.get('datetime_standard_format'))
        else:
            raise ValueError("Invalid datetime format")

        if isinstance(to_datetime, str):
            to_datetime_naive_dt = dt.strptime(to_datetime, time_format_config.get('datetime_standard_format'))
            to_datetime_dt = timezone(tz_mapping.get(self.fab)).localize(to_datetime_naive_dt)
            to_datetime_str = to_datetime_dt.strftime(time_format_config.get('datetime_standard_format'))

        elif isinstance(to_datetime, dt):
            to_datetime_dt = to_datetime
            to_datetime_str = to_datetime_dt.strftime(time_format_config.get('datetime_standard_format'))

        else:
            raise ValueError("Invalid datetime format")

        start_year = from_datetime_dt.year
        end_year = to_datetime_dt.year
        start_month = str(from_datetime_dt.month).zfill(2)
        end_month = str(to_datetime_dt.month).zfill(2)
        year_month_partition_str = self.get_year_month_partition(start_year, end_year, start_month, end_month)
        hive_sql = hive_sql_template.format(fab=self.fab, region=self.region,
                                            year_month_partition_str=year_month_partition_str,
                                            start_date=from_datetime_str, end_date=to_datetime_str, **kwargs)

        # print(hive_sql)
        df = self.space_query_hql(hive_sql)
        return df

    def space_query_multiple_ch_ckc(self, ch_ckc_list, from_datetime, to_datetime):
        if isinstance(ch_ckc_list, str):
            ch_ckc_list = [ch_ckc_list]
        elif isinstance(ch_ckc_list, list):
            ch_ckc_list = ch_ckc_list
        else:
            raise ValueError("Invalid ch_ckc_list format. Required format is a list.")

        ch_ckc_condition_list = []
        for x in set(ch_ckc_list):
            n = len(str(x).split("_"))
            if n == 1:
                ch_ckc_condition_list.append("(ch_id=" + str(x) + ")")
            elif n == 2:
                ch_ckc_condition_list.append("(ch_id=" + str(x.split("_")[0]) + " and " + "ckc_id=" + str(x.split("_")[1]) + ")")
            else:
                raise ValueError("Invalid ch_ckc_list format. Required format is ch_ckc or ch.")

        ch_ckc_condition_str = "(" + " or ".join(ch_ckc_condition_list) + ")"

        df = self._space_query(ch_ckc_condition_list=ch_ckc_condition_str,
                               from_datetime=from_datetime, to_datetime=to_datetime,
                               file_name='samples_query_by_multiple_ch.tql')
        return df

    def space_query_by_lot(self, lot_id, from_datetime, to_datetime, exact_match_lot_id=True):
        if exact_match_lot_id:
            where_lot_pattern = " = " + str(lot_id)
        else:
            where_lot_pattern = " like " + str(lot_id)[0:7]
        pass

    def get_year_month_partition(self, start_year, end_year,start_month, end_month):
        """
        This is to create year month partition in script
        start_year-start_month --> end_year-end_month

        :param start_year:
        :param end_year:
        :param start_month:
        :param end_month:
        :return:
        """
        start_partition = str(start_year) + "_" + str(start_month).zfill(2)
        end_partition = str(end_year) + "_" + str(end_month).zfill(2)
        partition_list = []

        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                current_partition = str(year) + "_" + str(month).zfill(2)
                if (current_partition <= end_partition ) & (current_partition >= start_partition):
                    partition_str = '(sys_part_yyyy = {start_year} and sys_part_mm = {start_month})'\
                                        .format(start_year=year, start_month=month)
                    partition_list.append(partition_str)
        year_month_partition_str = "(" + ' or '.join(partition_list) + ")"
        return year_month_partition_str
