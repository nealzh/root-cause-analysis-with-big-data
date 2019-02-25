from pandas import datetime
from sklearn import linear_model


class TrendingCheck:
    def __init__(self, df_per_row, context_value_per_row, column_index, ucl, lcl, tc_config):
        '''

        :param df_per_row:
        :param context_value_per_row:
        :param column_index: default 2
        :param ucl:
        :param lcl:
        '''

        self.df_per_row = df_per_row
        self.context_value_per_row = context_value_per_row
        self.ucl = ucl
        self.lcl = lcl
        self.column_index = column_index
        self.config = tc_config

    # generate dataset: root cause and non root cause

    def separate_dataset(self):
        '''

        :return: two dataframes: root cause & non root cause
        columns of df: lot_id/wafer_id/process_chamber/mfg_process_step/run_complete_datetime/label/value
        '''

        df_per_row = self.df_per_row
        column_index = self.column_index
        context_value = self.context_value_per_row

        df_per_row.iloc[:, column_index] = df_per_row.iloc[:, column_index].astype(str)
        dt_root_cause = df_per_row[df_per_row.iloc[:, column_index] == context_value][['run_complete_datetime','value']]
        dt_non_root_cause = df_per_row[df_per_row.iloc[:, column_index] != context_value][['run_complete_datetime', 'value']]
        return dt_root_cause, dt_non_root_cause


    def generate_dataset(self):
        '''

        :return: two dataframes: root cause & non root cause
        columns of df: datetime, value
        '''
        dt_root_cause, dt_non_root_cause = self.separate_dataset()

        def parser(x):
            return datetime.strptime(x, '%Y-%m-%d %H:%M:%S')

        datetime_rt = dt_root_cause['run_complete_datetime'].apply(lambda x: parser(x))
        datetime_nrt = dt_non_root_cause['run_complete_datetime'].apply(lambda x: parser(x))
        dt_root_cause['datetime'] = datetime_rt
        dt_non_root_cause['datetime'] = datetime_nrt
        dt_root_cause = dt_root_cause.drop('run_complete_datetime', axis=1)
        dt_non_root_cause = dt_non_root_cause.drop('run_complete_datetime', axis=1)

        cols = dt_root_cause.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        dt_root_cause = dt_root_cause[cols]
        dt_non_root_cause = dt_non_root_cause[cols]

        return dt_root_cause, dt_non_root_cause

    def get_slope(self, x, y):
        linreg = linear_model.LinearRegression()
        linreg.fit(x, y)
        slope = linreg.coef_
        intercept = linreg.intercept_
        return slope

    def get_rt_and_nrt_slope(self):
        dt_root_cause, dt_non_root_cause = self.generate_dataset()
        x_of_root_cause = dt_root_cause.iloc[:, 0].values.reshape(-1, 1)
        y_of_root_cause = dt_root_cause.iloc[:, 1].values

        x_of_non_root_cause = dt_non_root_cause.iloc[:, 0].values.reshape(-1, 1)
        y_of_non_root_cause = dt_non_root_cause.iloc[:, 1].values

        if len(list(x_of_root_cause)) >= 2 and len(list(y_of_root_cause)) >= 2 and len(list(x_of_non_root_cause)) >= 2 \
                and len(list(y_of_non_root_cause)) >= 2:
                slope_rt = self.get_slope(x_of_root_cause, y_of_root_cause)[0]
                slope_nrt = self.get_slope(x_of_non_root_cause, y_of_non_root_cause)[0]
                return slope_rt, slope_nrt
        else:
            # only one label
            return 1, 1

    def get_slope_diff(self):
        slope_rt, slope_nrt = self.get_rt_and_nrt_slope()
        slope_diff = abs((slope_rt - slope_nrt)/slope_rt)
        return slope_diff

    def calculate_ooc_percentage(self):
        ucl = self.ucl
        lcl = self.lcl
        dt_root_cause, dt_non_root_cause = self.separate_dataset()
        slope_rt, slope_nrt = self.get_rt_and_nrt_slope()
        if lcl is None:
            nrt_value = list(dt_non_root_cause['value'])
            rt_value = list(dt_root_cause['value'])
            merged_list = nrt_value + rt_value
            lcl = min(merged_list)
        elif ucl is None:
            nrt_value = list(dt_non_root_cause['value'])
            rt_value = list(dt_root_cause['value'])
            merged_list = nrt_value + rt_value
            ucl = max(merged_list)

        rt_ooc_lcl = dt_root_cause[dt_root_cause['value'] <= lcl].shape[0]
        rt_ooc_ucl = dt_root_cause[dt_root_cause['value'] >= ucl].shape[0]
        rt_total = dt_root_cause.shape[0]

        nrt_ooc_lcl = dt_non_root_cause[dt_non_root_cause['value'] <= lcl].shape[0]
        nrt_ooc_ucl = dt_non_root_cause[dt_non_root_cause['value'] >= ucl].shape[0]
        nrt_total = dt_non_root_cause.shape[0]

        if slope_rt > 0 and slope_nrt > 0:
                rt_ooc_ucl_pct = float(rt_ooc_ucl) / float(rt_total)
                nrt_ooc_ucl_pct = float(nrt_ooc_ucl) / float(nrt_total)
                return rt_ooc_ucl_pct, nrt_ooc_ucl_pct

        elif slope_rt < 0 and slope_nrt < 0:
                rt_ooc_lcl_pct = float(rt_ooc_lcl) / float(rt_total)
                nrt_ooc_lcl_pct = float(nrt_ooc_lcl) / float(nrt_total)
                return rt_ooc_lcl_pct, nrt_ooc_lcl_pct

    def ooc_value_multiple(self):
        ucl = self.ucl
        lcl = self.lcl
        slope_rt, slope_nrt = self.get_rt_and_nrt_slope()
        slope_diff = self.get_slope_diff()
        dt_root_cause, dt_non_root_cause = self.generate_dataset()
        if lcl is None:
            nrt_value = list(dt_non_root_cause['value'])
            rt_value = list(dt_root_cause['value'])
            merged_list = nrt_value + rt_value
            lcl = min(merged_list)
        elif ucl is None:
            nrt_value = list(dt_non_root_cause['value'])
            rt_value = list(dt_root_cause['value'])
            merged_list = nrt_value + rt_value
            ucl = max(merged_list)

        slope_diff_upper_limit = self.config.get('slope_diff_upper_limit')

        if slope_rt > 0 and slope_nrt > 0 and slope_diff > slope_diff_upper_limit:
            nrt_ooc_value = list(dt_non_root_cause[dt_non_root_cause['value'] >= ucl]['value'])
            if len(nrt_ooc_value) > 0:
                multiple = abs((max(nrt_ooc_value) - ucl)/(ucl - lcl))
                return multiple
            else:
                return -1
        elif slope_rt < 0 and slope_nrt < 0 and slope_diff > slope_diff_upper_limit:
            nrt_ooc_value = list(dt_non_root_cause[dt_non_root_cause['value'] <= lcl]['value'])
            if len(nrt_ooc_value) > 0:
                multiple = abs(lcl - min(nrt_ooc_value)/(ucl - lcl))
                return multiple
            else:
                return -1
