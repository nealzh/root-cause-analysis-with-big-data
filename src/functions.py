import warnings
warnings.filterwarnings("ignore")
from datetime import datetime as dt
from datetime import timedelta
from pytz import timezone, utc
from mu_f10ds_common.plots import *
from trending_check import TrendingCheck
import pandas as pd
import logging
import requests
import os
import sys
import re
import numpy as np
import matplotlib.pylab as pylab
params = {'legend.fontsize': 'medium',
          'figure.figsize': (16, 9),
          'axes.labelsize': 'medium',
          'axes.titlesize': 'large',
          'xtick.labelsize': 'medium',
          'ytick.labelsize': 'medium'}
pylab.rcParams.update(params)

def get_loop_id(step):
    """
    need to handle 3 cases:
    normal step: 1210-52 WL PHOTO SEM CD
    BDIS step: BDIS_4200-28 W0 OXIDE DEP
    D3 step: D3_1001-21_L06B_21_AD_PIDSC_1001

    :param step:
    :return:
    """
    try:
        if step.startswith("BDIS_"):
            loop = str(step.split("-")[1].split(" ")[0])
        elif step.startswith("D3_"):
            loop = str(step.split("-")[1].split("_")[0])
        else:
            loop = str(step.split("-")[1].split(" ")[0])
        if len(loop) > 2:
            loop = "EXCEPTION"
    except:
        loop = "EXCEPTION"

    return loop


def find_align_loop(df, current_step):
    current_process_id = df[df['mfg_process_step'] == current_step].tail(1)['process_id'].values[0]
    align_loop = str(current_process_id)[8:10]
    return align_loop


def find_domain_steps(df, current_step, step_key_words_csv, mode, loop_range_csv):
    # mode support "gt", "lt", "ge", "le", "eq"
    # loop support "incoming", "at_loop", "all", "align_loop"
    # step_key_words support "STEP NAME", LIKE "DRY ETCH", "CMP"
    # step_key_words support regex LIKE "DRY ETCH$", "^5200"

    df_cp = df.copy()
    step_key_words_list = [x.strip() for x in step_key_words_csv.split(',')]
    loop_range_list = [x.strip() for x in loop_range_csv.split(',')]
    align_loop = "NA"
    if 'align_loop' in loop_range_list:
        align_loop = find_align_loop(df, current_step)

    if df_cp[df_cp['mfg_process_step'] == current_step].shape[0] > 0:
        current_step_time = df_cp[df_cp['mfg_process_step'] == current_step].tail(1)['run_complete_datetime'].values[0]
        current_loop = df_cp[df_cp['mfg_process_step'] == current_step].tail(1)['loop'].values[0]
        loop_list = df_cp.sort_values("run_complete_datetime", ascending=True)['loop'].drop_duplicates(keep="last")
        previous_loop = loop_list[loop_list.index < loop_list[loop_list == current_loop].index.values[0]].iloc[-1]
        # filter by loop first

        df_loops = [pd.DataFrame()] * len(loop_range_list)
        for idx, loop_range in enumerate(loop_range_list):
            df_tmp = df.copy()
            if loop_range == "at_loop":
                df_tmp = df_tmp[(df_tmp['loop'] == current_loop) & (df_tmp['run_complete_datetime'] <= current_step_time)]
            elif loop_range == "previous_loop":
                df_tmp = df_tmp[(df_tmp['loop'] == previous_loop) & (df_tmp['run_complete_datetime'] <= current_step_time)]
            elif loop_range == "align_loop":
                df_tmp = df_tmp[(df_tmp['loop'] == align_loop) & (df_tmp['run_complete_datetime'] <= current_step_time)]
            elif loop_range == "incoming":
                df_tmp = df_tmp[(df_tmp['loop'] != current_loop) & (df_tmp['run_complete_datetime'] <= current_step_time)]
            else:
                df_tmp = df_tmp[df_tmp['run_complete_datetime'] <= current_step_time]
            df_loops[idx] = df_tmp

        df_tmp = pd.concat(df_loops)
        # print(df_tmp)
        if mode == "eq":
            df_tmp = df_tmp[df_tmp['mfg_process_step'].str.contains('|'.join(step_key_words_list), regex=True)]
        elif mode == "gt":
            cut_off_datetime = \
            df_tmp[df_tmp['mfg_process_step'].str.contains('|'.join(step_key_words_list), regex=True)].sort_values(
                "run_complete_datetime", ascending=True).head(1)['run_complete_datetime'].values[0]
            df_tmp = df_tmp[df_tmp['run_complete_datetime'] > cut_off_datetime]

        elif mode == "lt":
            cut_off_datetime = \
            df_tmp[df_tmp['mfg_process_step'].str.contains('|'.join(step_key_words_list), regex=True)].sort_values(
                "run_complete_datetime", ascending=True).tail(1)['run_complete_datetime'].values[0]
            df_tmp = df_tmp[df_tmp['run_complete_datetime'] < cut_off_datetime]
        elif mode == "ge":
            cut_off_datetime = \
            df_tmp[df_tmp['mfg_process_step'].str.contains('|'.join(step_key_words_list), regex=True)].sort_values(
                "run_complete_datetime", ascending=True).head(1)['run_complete_datetime'].values[0]
            df_tmp = df_tmp[df_tmp['run_complete_datetime'] >= cut_off_datetime]
        elif mode == "le":
            cut_off_datetime = \
            df_tmp[df_tmp['mfg_process_step'].str.contains('|'.join(step_key_words_list), regex=True)].sort_values(
                "run_complete_datetime", ascending=True).tail(1)['run_complete_datetime'].values[0]
            df_tmp = df_tmp[df_tmp['run_complete_datetime'] <= cut_off_datetime]

        return df_tmp.reset_index(drop=True)
    else:
        return pd.DataFrame()


def get_step_type(row, fmea_step, domain_step, feedback_step, process_step, measurement_step,
                  current_loop, qual_step=[]):
    # type fmea >  feedback > process_step > domain knowledge >  measurement_step > loop >incoming

    if row['mfg_process_step'] in fmea_step:
        return '1_fmea'
    elif row['mfg_process_step'] in feedback_step:
        return '2_past_feedback'
    elif (row['mfg_process_step'] == process_step) & (process_step[0] in ['3', '4', '5', '6', '8']):
        return '3_process_step'
    elif row['mfg_process_step'] in qual_step:
        return '4_qual_build'
    elif row['mfg_process_step'] in domain_step:
        return '4_domain'
    elif row['mfg_process_step'] == measurement_step:
        return '5_measurement_step'
    elif str(row['loop']) == str(current_loop):
        return '6_at_loop'
    else:
        return '7_incoming'


def get_logger(tz, identifier="chronic_ad", debug=False, log_relative_dir="../logs"):
    logger = logging.getLogger(__name__)
    if not len(logger.handlers):
        today_dt = get_now_dt(tz)
        today_str = format_datetime_string(today_dt, 'd')[0:10]
        file_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(file_dir, log_relative_dir)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_path = os.path.join(file_dir, log_relative_dir, today_str + "_" + identifier + ".log")
        if debug:
            logging.basicConfig(level=logging.DEBUG,
                                format='%(asctime)s %(levelname)-8s: %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S',
                                filename=log_path,
                                filemode='a')
        else:
            logging.basicConfig(level=logging.INFO,
                                format='%(asctime)s %(levelname)-8s: %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S',
                                filename=log_path,
                                filemode='a')
        stdoutLogger = logging.StreamHandler()
        if debug:
            stdoutLogger.setLevel(logging.DEBUG)
        else:
            stdoutLogger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
        stdoutLogger.setFormatter(formatter)

        logger.addHandler(stdoutLogger)
    return logger


def get_now_dt(tz):
    now_dt = dt.now(tz=timezone(tz))
    return now_dt

def format_datetime_string(date_dt, resolution):
    """

    :param date_dt: a datetime object
    :param resolution: different resolution for datetime string. support year, month, day, hour, minute and second.
        year -> 'y' or 'year',
        month -> 'm' or 'month',
        day -> 'd' or 'day',
        hour -> 'h' or 'hour'
        minite -> 'M' or 'minute',
        second ->  's' or 'second'
    :return: a yyyy-mm-dd hh:mm:ss formatted string
    """
    resolution = str(resolution).lower()

    if isinstance(date_dt, str):
        date_dt = dt.strptime(date_dt, "%Y-%m-%d %H:%M:%S"[0:len(date_dt)])

    if resolution in ['y', 'year']:
        date_str = date_dt.strftime('%Y-01-01 00:00:00')
    elif resolution in ['mon', 'month']:
        date_str = date_dt.strftime('%Y-%m-01 00:00:00')
    elif resolution in ['d', 'day']:
        date_str = date_dt.strftime('%Y-%m-%d 00:00:00')
    elif resolution in ['h', 'hour']:
        date_str = date_dt.strftime('%Y-%m-%d %H:00:00')
    elif resolution in ['min', 'minute']:
        date_str = date_dt.strftime('%Y-%m-%d %H:%M:00')
    else:
        date_str = date_dt.strftime('%Y-%m-%d %H:%M:%S')
    return date_str




def append_tool(row, mode=''):
    # @TODO this function needs to re-write to make it more concise and easy to understand
    # try:

    # format A, B, C, D
    p1 = re.compile('^[A-Z]$')
    # format A1, A0, B0, B2
    p2 = re.compile('^[A-Z][0-9]$')
    # format 10, 20, 30
    # p3 = re.compile('^[1-9]0$')
    # format position
    # p4 = re.compile('^[-+]?[0-9]+$')
    p_num = re.compile('^[0-9]+$')
    p_alp = re.compile('^[a-zA-Z]+$')
    row = row.fillna("")
    equipment_id = row['equipment_id']
    lot_id = row['lot_id']
    if p1.match(lot_id[0]):
        lot_type = 'QUAL'
    else:
        lot_type = 'PROD'

    if equipment_id == "" or str(equipment_id) == 'nan':
        if mode == 'df':
            return "", ""
        else:
            return ""

    if mode == 'df':
        try:
            if ((equipment_id.startswith('KOKU')) | (equipment_id.startswith('TELF'))) & (lot_type == 'PROD'):
                try:
                    if 'lot_position' in row.keys():
                        lot_position = row['lot_position']
                        if lot_position == "":
                            return "", ""
                        elif p_alp.match(lot_position) is not None:
                            return equipment_id + "_" + lot_position, lot_position
                        elif p_num.match(lot_position) is not None:
                            lot_position = int(lot_position)

                            BOTCTR_LL = int(row['BOTCTR_LL'])
                            BOTCTR_UL = int(row['BOTCTR_UL'])
                            BOT_LL = int(row['BOT_LL'])
                            BOT_UL = int(row['BOT_UL'])
                            CTRBOT_LL = int(row['CTRBOT_LL'])
                            CTRBOT_UL = int(row['CTRBOT_UL'])
                            CTRTOP_LL = int(row['CTRTOP_LL'])
                            CTRTOP_UL = int(row['CTRTOP_UL'])
                            CTR_LL = int(row['CTR_LL'])
                            CTR_UL = int(row['CTR_UL'])
                            TOPCTR_LL = int(row['TOPCTR_LL'])
                            TOPCTR_UL = int(row['TOPCTR_UL'])
                            TOP_LL = int(row['TOP_LL'])
                            TOP_UL = int(row['TOP_UL'])

                            if (lot_position <= BOTCTR_UL) & (lot_position >= BOTCTR_LL):
                                return equipment_id + "_BOTCTR", "BOTCTR"
                            elif (lot_position <= BOT_UL) & (lot_position >= BOT_LL):
                                return equipment_id + "_BOT", "BOT"
                            elif (lot_position <= CTRBOT_UL) & (lot_position >= CTRBOT_LL):
                                return equipment_id + "_CTRBOT", "CTRBOT"
                            elif (lot_position <= CTRTOP_UL) & (lot_position >= CTRTOP_LL):
                                return equipment_id + "_TRTOP", "TRTOP"
                            elif (lot_position <= CTR_UL) & (lot_position >= CTR_LL):
                                return equipment_id + "_CTR", "CTR"
                            elif (lot_position <= TOPCTR_UL) & (lot_position >= TOPCTR_LL):
                                return equipment_id + "_TOPCTR", "TOPCTR"
                            elif (lot_position <= TOP_UL) & (lot_position >= TOP_LL):
                                return equipment_id + "_TOP", "TOP"
                            else:
                                return "", ""
                        else:
                            return "", ""
                    else:
                        return "", ""
                except:
                    return "", ""

            else:
                return "", ""
        except:
            # print(equipment_id)
            return "", ""

    if 'process_position' in row.keys():
        process_position = row['process_position']
    else:
        process_position = ""

    if 'process_chamber' in row.keys():
        process_chamber = row['process_chamber']
    else:
        process_chamber = ""

    if ((str(process_position) == "nan") | (process_position == "")) &\
            ((str(process_chamber) == "nan") | (process_chamber == "")):
        return ""

    # Convert DF Process Position
    else:
        if (equipment_id.startswith('KOKU')) | (equipment_id.startswith('TELF')):
            try:
            # print(row.keys())
                if 'process_position' in row.keys():
                    process_position = row['process_position']
                    process_position = int(process_position)

                    BOTCTR_LL = int(row['BOTCTR_LL'])
                    BOTCTR_UL = int(row['BOTCTR_UL'])
                    BOT_LL = int(row['BOT_LL'])
                    BOT_UL = int(row['BOT_UL'])
                    CTRBOT_LL = int(row['CTRBOT_LL'])
                    CTRBOT_UL = int(row['CTRBOT_UL'])
                    CTRTOP_LL = int(row['CTRTOP_LL'])
                    CTRTOP_UL = int(row['CTRTOP_UL'])
                    CTR_LL = int(row['CTR_LL'])
                    CTR_UL = int(row['CTR_UL'])
                    TOPCTR_LL = int(row['TOPCTR_LL'])
                    TOPCTR_UL = int(row['TOPCTR_UL'])
                    TOP_LL = int(row['TOP_LL'])
                    TOP_UL = int(row['TOP_UL'])

                    if (process_position <= BOTCTR_UL) & (process_position >= BOTCTR_LL):
                        return equipment_id + "_BOTCTR"
                    elif (process_position <= BOT_UL) & (process_position >= BOT_LL):
                        return equipment_id + "_BOT"
                    elif (process_position <= CTRBOT_UL) & (process_position >= CTRBOT_LL):
                        return equipment_id + "_CTRBOT"
                    elif (process_position <= CTRTOP_UL) & (process_position >= CTRTOP_LL):
                        return equipment_id + "_TRTOP"
                    elif (process_position <= CTR_UL) & (process_position >= CTR_LL):
                        return equipment_id + "_CTR"
                    elif (process_position <= TOPCTR_UL) & (process_position >= TOPCTR_LL):
                        return equipment_id + "_TOPCTR"
                    elif (process_position <= TOP_UL) & (process_position >= TOP_LL):
                        return equipment_id + "_TOP"
                    else:
                        return ""
            except:
                return ""
        else:
            pass
    if process_position == "" or str(process_position) == 'nan':
        pass
    else:
        if (len(str(process_position)) == 10) & \
                (process_position[0:8] == equipment_id[0:8]):
            return process_position
    if process_chamber == "" or str(process_chamber) == 'nan':
        pass
    else:
        if (len(str(process_chamber)) == 10) & \
                    (process_chamber[0:8] == equipment_id[0:8]):
            return process_chamber

    if (len(str(process_position)) < 10) & (str(process_position) != 'nan') \
        & (((p1.match(process_position)) is not None) | ((p2.match(process_position)) is not None)):
        return str(equipment_id) + "_" + str(process_position)
    else:
        return ""
    # except Exception as e:
    #     print(e)
    #     return ""


def log_df(df):
    return "="*80 + "\n" + df.to_string() + "\n" + "="*80


def pm_cm_query_date(sample_date, long_period, short_period):
    if len(sample_date) > 0:
        start_date = str(sample_date - timedelta(days=short_period)[0:10])
        end_date = str(sample_date + timedelta(days=short_period)[0:10])
        return start_date, end_date
    else:
        start_date = str(dt.now() - timedelta(days=long_period))[0:10]
        end_date = str(dt.now())[0:10]
        return start_date, end_date


def plot_result_per_row(row, res_df, df_space_trim, lcl, ucl, parameter, fmt="base64", path=None):
    """
    def __init__(self, data, category_col, value_col, datetime_col, ucl=None, lcl=None,
                 xlabel='', ylabel='', title='', highlighted_context=None):
    :param row:
    :param res_df:
    :param df_space_trim:
    :param lcl:
    :param ucl:
    :param ylim:
    :param parameter:
    :return:
    """
    rank = row.name
    res_feature = row['feature']
    res_mfg_process_step = row['mfg_process_step']
    context_value = row['context_value']
    chart_idx = row['context_idx']

    df_space_trim['lot_id'] = df_space_trim['lot_id'].astype(str)
    res_df['lot_id'] = res_df['lot_id'].astype(str)

    res_df = res_df.drop_duplicates()

    df_space_trim = df_space_trim.drop_duplicates()

    df = res_df[res_df['mfg_process_step'] == res_mfg_process_step][
        ['lot_id', 'wafer_id', res_feature, 'mfg_process_step', 'run_complete_datetime']].merge(df_space_trim,
                                                                                                on=['lot_id',
                                                                                                    'wafer_id'],
                                                                                                how='inner')
    df = df.sort_values('run_complete_datetime', ascending=True)
    df = df[df[res_feature] > ""]
    df = df.drop_duplicates()
    title = res_feature + "::" + context_value

    # lot_id wafer_id       lot_attribute   mfg_process_step run_complete_datetime  label  value
    if (res_feature == 'lot_attribute') | (res_feature == 'swr') | (res_feature == 'qdr'):
        df_tmp = df[['lot_id', 'wafer_id', 'mfg_process_step', 'run_complete_datetime', 'value']].drop_duplicates(keep='last')
        df_tmp_highlight = df[df[res_feature] == context_value][['lot_id', 'wafer_id', 'mfg_process_step',
                                                                 'run_complete_datetime', res_feature]]
        # m = {context_value: 'YES'}
        df = df_tmp.merge(df_tmp_highlight, on=['lot_id', 'wafer_id', 'mfg_process_step', 'run_complete_datetime'], how='left')

        # df[res_feature] = df[res_feature].map(m)
        df[res_feature] = df[res_feature].fillna('NO')
        # print(df)
    if row.size == 8:
        if len(str(row['reason'])) > 0:
            pmcm_datetime = row['datetime']
            plot_instance = MUPlots(data=df, category_col=res_feature, value_col='value', datetime_col='run_complete_datetime',
                            ucl=ucl, lcl=lcl,
                            xlabel=res_mfg_process_step, ylabel=parameter, title=title, highlighted_context=context_value, pmcm_dt=pmcm_datetime)
        else:
            plot_instance = MUPlots(data=df, category_col=res_feature, value_col='value',
                                    datetime_col='run_complete_datetime',
                                    ucl=ucl, lcl=lcl,
                                    xlabel=res_mfg_process_step, ylabel=parameter, title=title,
                                    highlighted_context=context_value)

    else:
        plot_instance = MUPlots(data=df, category_col=res_feature, value_col='value', datetime_col='run_complete_datetime',
                            ucl=ucl, lcl=lcl,
                            xlabel=res_mfg_process_step, ylabel=parameter, title=title, highlighted_context=context_value)

    try:
        if df.shape[0] > 0:
            # step, step_df, catogory_col, value_col, lcl, ucl, ylim, parameter, chart_idx
            if fmt == 'base64':
                boxplot_str = plot_instance.plot_boxplot(fmt='base64')
                trending_str = plot_instance.plot_trend(fmt='base64')
            elif fmt == 'png':
                boxplot_str = plot_instance.plot_boxplot(fmt='png', path=path + str(rank) + '_boxplot.png')
                trending_str = plot_instance.plot_trend(fmt='png',  path=path + str(rank) + '_trending.png')
            else:
                boxplot_str = ""
                trending_str = ""

            return chart_idx, boxplot_str, trending_str, rank
        else:
            return chart_idx, "", "", rank
    except:
        return chart_idx, "", "", rank


def plot_ooc(df, sample_date_col, label_col, value_col,
             channel_id, ckc_id, chart_type, lcl, ucl, parameter, fmt="base64", path=None):
    """

    :param df:
    :param sample_date_col:
    :param label_col:
    :param value_col:
    :param channel_id:
    :param ckc_id:
    :param chart_type:
    :param lcl:
    :param ucl:
    :param parameter:
    :return:
    """
    img_str = ""
    df = df.reset_index(drop=True)
    plt.clf()
    fig, ax = plt.subplots()
    X = df[sample_date_col]
    Y = df[value_col]
    C = df[label_col].map({0: 'black', 1: 'red'})
    for i in range(len(df[sample_date_col])):
        ax.plot_date(x=X[i], y=Y[i], xdate=True, marker="s", color=C[i], markersize=4)
    fig.autofmt_xdate()
    plt.title("Channel: " + str(channel_id) + " ckc: " + str(ckc_id) + " chart type: " + str(chart_type))
    plt.ylabel(parameter)
    plt.xlabel("sample date")
    xmin, xmax = ax.get_xlim()
    plt.axhline(y=ucl, color='b', linestyle='--', label="ucl")
    plt.text(y=ucl, x=xmin, s="ucl", color='b')
    plt.axhline(y=lcl, color='b', linestyle='--', label="lcl")
    plt.text(y=lcl, x=xmin, s="lcl", color='b')
    if fmt == 'base64':
        fig_file = io.BytesIO()
        plt.savefig(fig_file, format='png')
        fig_file.seek(0)
        img_str = base64.b64encode(fig_file.read()).decode("utf-8")
    elif fmt == 'png':
        plt.savefig(path, format='png')
        img_str = path
    else:
        plt.show()
        img_str = ""
    plt.clf()
    return img_str


def valid_step_name(x):
    try:
        if (len(x) > 0) & ('-' in x):
            return x
        else:
            return ""
    except:
        return ""


def choose_datetime_standard_format(datetime_str):
    if len(datetime_str) == 10:
        return "%Y-%m-%d"
    elif len(datetime_str) == 13:
        return "%Y-%m-%d %H"
    elif len(datetime_str) == 16:
        return "%Y-%m-%d %H:%M"
    elif len(datetime_str) == 19:
        return "%Y-%m-%d %H:%M:%S"
    else:
        raise ValueError("Wrong datetime format.")


def trend_detect_result_per_row(row, res_df, df_space_trim, ucl, lcl, tc_config):
    context_value = row['context_value']
    # chart_idx = row['context_idx']
    res_feature = row['feature']
    res_mfg_process_step = row['mfg_process_step']

    df_space_trim['lot_id'] = df_space_trim['lot_id'].astype(str)
    res_df['lot_id'] = res_df['lot_id'].astype(str)

    res_df = res_df.drop_duplicates()
    df_space_trim = df_space_trim.drop_duplicates()

    df = res_df[res_df['mfg_process_step'] == res_mfg_process_step][
        ['lot_id', 'wafer_id', res_feature, 'mfg_process_step', 'run_complete_datetime']].merge(df_space_trim,
                                                                                                on=['lot_id',
                                                                                                    'wafer_id'],
                                                                                                how='inner')
    df = df.sort_values('run_complete_datetime', ascending=True)

    df = df.drop_duplicates()
    if df.shape[0] > 0:
        column_index = tc_config.get('column_index')
        slope_diff_lower_limit = tc_config.get('slope_diff_lower_limit')
        slope_diff_upper_limit = tc_config.get('slope_diff_upper_limit')
        rt_ooc_pct_upper_limit = tc_config.get('rt_ooc_pct_upper_limit')
        rt_ooc_pct_lower_limit = tc_config.get('rt_ooc_pct_lower_limit')
        nrt_ooc_pct_limit = tc_config.get('nrt_ooc_pct_limit')
        multiple_limit = tc_config.get('multiple_limit')

        drift_detect_result = TrendingCheck(df_per_row=df,
                                           context_value_per_row=context_value,
                                           column_index=column_index,
                                           ucl=ucl,
                                           lcl=lcl, tc_config=tc_config)
        slope_rt, slope_nrt = drift_detect_result.get_rt_and_nrt_slope()
        slope_diff = drift_detect_result.get_slope_diff()
        row['detection_result'] = ''

        if (row['step_type'] == '1_fmea') | (row['step_type'] == '3_process_step') | \
                (row['step_type'] == '2_past_feedback') | (row['step_type'] == '4_qual_build')\
                | (row['step_type'] == '4_domain'):
            row['detection_result'] = 'include'
        else:
            if (slope_rt > 0) & (slope_nrt > 0):
                if (slope_rt == 1) & (slope_nrt == 1):
                    #--------------------single context value filter-------------------------
                    # single context value or non root cause labels only have one point meaning no slope
                    if (row['feature'] == 'equipment_id') | (row['feature'] == 'process_position') \
                            | (row['feature'] == 'process_chamber') & (row['step_type'] != '7_incoming'):
                        row['detection_result'] = 'include'
                    else:
                        row['detection_result'] = 'exclude'

                    #--------------------single context value filter-------------------------
                else:
                    rt_ooc_pct, nrt_ooc_pct = drift_detect_result.calculate_ooc_percentage()

                    if slope_diff < slope_diff_lower_limit:
                        if (rt_ooc_pct > rt_ooc_pct_upper_limit) & (nrt_ooc_pct < nrt_ooc_pct_limit):
                            row['detection_result'] = 'include'
                        else:
                            row['detection_result'] = 'exclude'
                    elif slope_diff > slope_diff_upper_limit:
                        multiple = drift_detect_result.ooc_value_multiple()
                        if (rt_ooc_pct < rt_ooc_pct_lower_limit) & (multiple >=multiple_limit):
                            row['detection_result'] = 'exclude'
                        else:
                            row['detection_result'] = 'include'
                    else:
                        row['detection_result'] = 'exclude'
            elif (slope_rt < 0) & (slope_nrt < 0):
                rt_ooc_pct, nrt_ooc_pct = drift_detect_result.calculate_ooc_percentage()
                if slope_diff < slope_diff_lower_limit:
                    if (rt_ooc_pct > rt_ooc_pct_upper_limit) & (nrt_ooc_pct < nrt_ooc_pct_limit):
                        row['detection_result'] = 'include'
                    else:
                        row['detection_result'] = 'exclude'
                elif slope_diff > slope_diff_upper_limit:
                    multiple = drift_detect_result.ooc_value_multiple()
                    if (rt_ooc_pct < rt_ooc_pct_lower_limit) & (multiple >= multiple_limit):
                        row['detection_result'] = 'exclude'
                    else:
                        row['detection_result'] = 'include'
                else:
                    row['detection_result'] = 'exclude'
            else:
                row['detection_result'] = 'include'
        return row['detection_result']

