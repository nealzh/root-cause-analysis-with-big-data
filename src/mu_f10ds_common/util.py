import warnings
warnings.filterwarnings("ignore")
from datetime import datetime as dt
from datetime import timedelta
from pytz import timezone, utc
import os
import logging

#============================file operation===========================
def read_file(path):
    """

    :param path: file path
    :return: file content as string
    """
    with open(path, 'r') as f:
        file_str = f.read()
    return file_str

#============================dict operation===========================
def merge_two_dicts(x, y):
    """

    :param x: a dict type variable
    :param y: a dict type variable
    :return: new dict combining two dict
    """
    z = x.copy()  # start with x's keys and values
    z.update(y)  # modifies z with y's keys and values & returns None
    return z


def update_value(value):
    """
    This function is used for sql database query
    This is to add quotation mark for string value before add them in the sql query
    :param value: A value type containing string or number
    :return: A new value with string added single quotation mark
    """
    if isinstance(value, int):
        return value
    else:
        return("\'" + str(value) + "\'")


#============================list operation===========================
def update_value_list(value_list):
    """
    This function is used for sql database query
    This is to add quotation mark in the list for string value before add them in the sql query

    :param value_list: A list type containing string or number
    :return: A new list with string added single quotation mark
    """
    value_list_quoted = []
    for value in value_list:
        if isinstance(value, int):
            value_list_quoted.append(str(value))
        else:
            value_list_quoted.append("\'" + str(value) + "\'")
    return value_list_quoted


def update_value_list_csv(value_list, sep=","):
    """
    This function is an extension to update_value_list

    :param value_list: A list type containing string or number
    :param sep: delimiter used to seperate different values
    :return: A new list with string added single quotation mark
    """
    value_list_quoted = []
    for value in value_list:
        if isinstance(value, int):
            value_list_quoted.append(str(value))
        else:
            value_list_quoted.append("\'" + str(value) + "\'")
    value_list_quoted_csv = sep.join(value_list_quoted)
    return value_list_quoted_csv

#============================time/date operation======================

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

def get_now(tz):
    """
    This function is to get an datetime object to represent current system time.

    :param tz: timezone information, eg, 'Asia/Singapore' for Singapore
    :return: a datetime object with current system time in timezone provided.
    """
    now_dt = dt.now(tz=timezone(tz))
    return now_dt

def get_now_offset(delta_days, tz):
    """
    This function is to get an datetime object which has time offset compared to current time. for example, 5 days ago

    :param delta_days: an integer to represent time offset in days
    :param tz: timezone information, eg, 'Asia/Singapore' for Singapore
    :return: a datetime object with time offset and timezone provided.
    """
    delta_dt = get_now(tz) - timedelta(days=int(delta_days))
    return delta_dt

def get_now_str(tz):
    """
    This function is to get an string to represent current system time.

    :param tz: timezone information, eg, 'Asia/Singapore' for Singapore
    :return: a string with current system time in timezone provided.
    """
    now_str = format_datetime_string(get_now(tz), "s")
    return now_str

def utc_to_time(naive, tz):
    """
    This function is to convert one time from one timezone to another timezone.

    :param naive: a datetime object
    :param tz: timezone information, eg, 'Asia/Singapore' for Singapore
    :return: a new datetime object with new timezone
    """
    return naive.replace(tzinfo=utc).astimezone(timezone(tz))

#=====================logger operation======================


def get_logger(tz, identifier, log_dir_path=""):
    """
    This function is to initial a logger object

    :param tz: timezone information, eg, 'Asia/Singapore' for Singapore
    :param identifier: file name for log
    :param log_dir_path: file path for log
    :return: a logger object with both stream and file available
    """

    logger = logging.getLogger(__name__)

    if not len(logger.handlers):
        today_dt = get_now(tz)
        today_str = format_datetime_string(today_dt, 'd')[0:10]
        file_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(file_dir, "..", "logs")
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        if log_dir_path == "":
            log_dir_path = log_dir
        log_path = os.path.join(log_dir_path, today_str + "_" + identifier + ".log")
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(levelname)-8s: %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S',
                            filename=log_path,
                            filemode='a')
        stdoutLogger = logging.StreamHandler()
        stdoutLogger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
        stdoutLogger.setFormatter(formatter)
        logger.addHandler(stdoutLogger)
    return logger


