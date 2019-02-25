import warnings
warnings.filterwarnings("ignore")
import io
import numpy as np
import math
import base64
import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt
plt.switch_backend('Agg')
from datetime import datetime as dt
from datetime import timedelta
from matplotlib import dates
from smoothing import smoothing_method
import seaborn as sns


class MUPlots:
    def __init__(self, data, category_col, value_col, datetime_col, ucl=None, lcl=None,
                 xlabel='', ylabel='', title='', highlighted_context=None, pmcm_dt=None):
        '''

        :param data:
        :param category_col:
        :param value_col:
        :param datetime_col:
        :param ucl:
        :param lcl:
        :param xlabel:
        :param ylabel:
        :param title:
        :param highlighted_context:
        :param pmcm_dt:
        '''
        self.data = data
        self.category_col = category_col
        self.value_col = value_col
        self.datetime_col = datetime_col
        self.ucl = ucl
        self.lcl = lcl
        self.xlabel = xlabel
        self.ylabel = ylabel
        self.title = title
        self.highlighted_context = highlighted_context
        self.order_list = None
        self.ymax = None
        self.ymin = None
        self.color_list = None
        self.pmcm_dt = pmcm_dt

    def get_chart_limit(self):
        y = self.data[self.value_col].sort_values(ascending=True)
        # Use quantile check for ymax and ymin setup
        quantiles = y.quantile([0.1, 0.9])
        q_1 = quantiles.loc[0.1]
        q_9 = quantiles.loc[0.9]

        if np.isnan(self.ucl):
            ymax = np.min([q_9 + (q_9 - q_1) * 1.5, np.max(y)])
        else:
            ymax = np.max([np.min([q_9 + (q_9 - q_1) * 1.5, np.max(y)]), self.ucl])

        if np.isnan(self.lcl):
            ymin = np.max([q_1 - (q_9 - q_1) * 1.5, np.min(y)])
        else:
            ymin = np.min([np.max([q_1 - (q_9 - q_1) * 1.5, np.min(y)]), self.lcl])

        # ymax = y.iloc[-1] + (y.iloc[-1] - y.iloc[0]) * 0.05
        # ymin = y.iloc[0] - (y.iloc[-1] - y.iloc[0]) * 0.05
        self.ymax = ymax
        self.ymin = ymin

    def get_order_list(self):
        category_list = self.data[self.category_col].sort_values().unique()
        if self.highlighted_context is not None:
            category_list = [self.highlighted_context] + [x for x in category_list if x != self.highlighted_context]
        self.order_list = category_list

    def get_color_list(self):
        colors = ["red", "windows blue", "amber", "reddish orange", "faded green", "dark pink",
                  "baby pink", "orange", "royal blue", "midnight purple", "hunter green", "salmon", "tangerine",
                  "aqua blue", "electric blue", "dusty rose", "cerulean", "cerise", "dark red",
                  "gold", "terracotta", "greenish brown", "charcoal", "neon pink", "mahogany", "dark sky blue",
                  "brownish yellow", "forest", "light indigo", "pine", "dark periwinkle"]

        color_list = sns.xkcd_palette(colors)
        self.color_list = color_list

    def plot_setup(self):
        plt.clf()
        plt.close()
        plt.figure(figsize=(16, 12))
        matplotlib.rcParams['xtick.labelsize'] = 10
        #         plt.subplots_adjust(top=0.85, bottom=0.15)

        if self.order_list is None:
            self.get_order_list()
        if self.ymax is None or self.ymin is None:
            self.get_chart_limit()
        if self.color_list is None:
            self.get_color_list()

    def plot_add_label(self):
        plt.title(self.title, fontsize=25)
        plt.ylabel(self.ylabel, fontsize=18)
        plt.xlabel(self.xlabel, fontsize=18)

    @staticmethod
    def convert_stringdate_to_floatdate(l):
        d = [dt.strptime(x, "%Y-%m-%d  %H:%M:%S") for x in l]
        time_diff = [(x - d[0]).total_seconds() for x in d]
        return time_diff

    def fake_dates(x, pos):
        """ Custom formater to turn floats into e.g., 2016-05-08"""
        return dates.num2date(x).strftime('%m-%d')

    def save_as_png(self, plt, path):
        try:
            plt.savefig(path, bbox_inches='tight')
            return path
        except:
            return ""

    def save_as_base64(self, plt):
        fig_file = io.BytesIO()
        plt.savefig(fig_file, format='png', bbox_inches='tight')
        fig_file.seek(0)
        img_str = base64.b64encode(fig_file.read()).decode("utf-8")
        return img_str

    def save_as(self, plt, fmt="", path=None):
        # print(fmt)
        if fmt == 'base64':
            return self.save_as_base64(plt)
        elif fmt == 'png':
            return self.save_as_png(plt, path)
        else:
            plt.show()
            return 0

    def plot_trend(self, fmt='', path=None):

        self.plot_setup()
        data_trend_df = self.data.copy()
        data_trend_df = data_trend_df.sort_values(self.datetime_col, ascending=True)

        date_value = data_trend_df[self.datetime_col]
        initial_datetime = dt.strptime(date_value.iloc[0], "%Y-%m-%d  %H:%M:%S")
        data_trend_df['time_diff'] = self.convert_stringdate_to_floatdate(date_value)
        #print(data_trend_df['time_diff'].dtypes())

        # Add Jitter
        # data_trend_df['time_diff'] = [x + np.random.randint(low=0, high=1000) for x in data_trend_df['time_diff']]
        # data_trend_df[self.value_col] = [x + (np.random.rand() - 1) / 20 * x for x in data_trend_df[self.value_col]]

        line_plot = sns.lmplot(x='time_diff', y=self.value_col, data=data_trend_df, hue=self.category_col,
                               hue_order=self.order_list,
                               palette=self.color_list, fit_reg=False, lowess=True, size=6, aspect=1.5, legend=False,
                               legend_out=True, scatter_kws={'alpha': 0.8})
        category_color_list = dict(zip(self.order_list, self.color_list))

        # Add Smooth Line
        df_highlighted = None
        for idx, df_tmp in enumerate(data_trend_df.groupby(self.category_col)):
            category, df_tmp = df_tmp
            if (self.highlighted_context is None) | (category != self.highlighted_context):
                kernel_x_size = 500
                kernel_instance = smoothing_method(df_tmp['time_diff'], df_tmp[self.value_col])
                y_kernel = kernel_instance.smoothing_pred()
                time_range = np.asarray(np.linspace(min(df_tmp['time_diff']), max(df_tmp['time_diff']),
                                                    kernel_x_size), dtype=np.float64)
                y_pred = y_kernel.fit(time_range)
                plt.plot(time_range, y_pred[0], color=category_color_list.get(category), lw=2, alpha=0.8)
            else:
                category_highlighted = category
                df_highlighted = df_tmp.copy()
        if df_highlighted is not None:
            kernel_x_size = 500
            kernel_instance = smoothing_method(df_highlighted['time_diff'], df_highlighted[self.value_col])
            y_kernel = kernel_instance.smoothing_pred()
            time_range = np.asarray(np.linspace(min(df_highlighted['time_diff']), max(df_highlighted['time_diff']),
                                                kernel_x_size), dtype=np.float64)
            y_pred = y_kernel.fit(time_range)
            plt.plot(time_range, y_pred[0], color=category_color_list.get(category_highlighted), lw=8, alpha=0.8)
        # Add label
        get_x, get_x_labels = plt.xticks()
        plt.xticks(fontsize=14, rotation=45)
        plt.ylim(self.ymin, self.ymax)
        if self.ucl is not None:
            plt.axhline(y=self.ucl, linestyle="--", color='blue', label="ucl")
            plt.text(y=self.ucl, x=get_x[0] + 1, s="ucl", color='b')
        if self.lcl is not None:
            plt.axhline(y=self.lcl, linestyle="--", color='blue', label="lcl")
            plt.text(y=self.lcl, x=get_x[0] + 1, s="lcl", color='b')

        #-----------plot multiple vertical lines for pm/cm---------------------------=

        pmcm_dt = str(self.pmcm_dt)
        if len(pmcm_dt) > 4:
            pm_dt_new = pmcm_dt[0:19]
            pmcm_convert_dt = dt.strptime(pm_dt_new, "%Y-%m-%d  %H:%M:%S")
            time_diff_pmcm = (pmcm_convert_dt - initial_datetime).total_seconds()
            plt.axvline(x=time_diff_pmcm, linestyle="--", color='green', lw=3, label="pmcm")
        # -----------plot multiple vertical lines for pm/cm---------------------------

        plt.xticks(get_x, [dt.strftime(initial_datetime + timedelta(seconds=x), "%m-%d") for x in get_x])

        self.plot_add_label()

        num_of_col = max(int(len(self.order_list)/25.0) + 1, 1)
        # print(num_of_col)
        plt.legend(bbox_to_anchor=(1, -.2, 0.5, 1.25), loc='best', ncol=int(num_of_col),
                    borderaxespad=0., fontsize='large')
        res = self.save_as(plt, fmt, path)
        return res

    def plot_boxplot(self, fmt='', path=None):
        self.plot_setup()
        data_box_df = self.data
        plotting_box = sns.boxplot(x=self.category_col, y=self.value_col, data=data_box_df, order=self.order_list,
                                   palette=self.color_list)
        get_x, get_x_labels = plt.xticks()
        plt.ylim(self.ymin, self.ymax)
        if self.ucl is not None:
            plotting_box.axhline(y=self.ucl, linestyle="--", color='blue', label="ucl")
            plt.text(y=self.ucl, x=get_x[0] - 0.5, s="ucl", color='b')
        if self.lcl is not None:
            plotting_box.axhline(y=self.lcl, linestyle="--", color='blue', label="lcl")
            plt.text(y=self.lcl, x=get_x[0] - 0.5, s="lcl", color='b')
        sns.set_style("whitegrid")
        sns.swarmplot(x=self.category_col, y=self.value_col, data=self.data, size=6, color=".3", linewidth=0,
                      order=self.order_list)
        plotting_box.set_xticklabels(plotting_box.get_xticklabels(), fontsize=18, rotation=45)
        sns.despine(left=True)
        self.plot_add_label()
        res = self.save_as(plt, fmt, path)
        return res

    def plot_boxplot_trend(self):
        pass