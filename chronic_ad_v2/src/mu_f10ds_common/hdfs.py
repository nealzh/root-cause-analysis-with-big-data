import warnings
warnings.filterwarnings("ignore")
import subprocess
import os
import logging
import time
logger = logging.getLogger(__name__)

def hdfs_list_path(hdfs_file_path):
    """

    :param hdfs_file_path:
    :return:
    """
    try:
        cmd = "hadoop fs -ls " + hdfs_file_path
        stdout = run_cmd(cmd)
        if 'No such file or directory' in stdout.split('\n'):
            return None
        else:
            file_list = [line.rsplit(None, 1)[-1] for line in stdout.split('\n') if len(line.rsplit(None, 1))][1:]
            return file_list
    except Exception as e:
        log_cmd(cmd)
        log_error(e)


def hdfs_read_file(hdfs_file_path):
    """

    :param hdfs_file_path:
    :return:
    """
    try:
        cmd = "hadoop fs -text " + hdfs_file_path
        return run_cmd(cmd)
    except Exception as e:
        log_cmd(cmd)
        log_error(e)


def hdfs_read_directory(hdfs_directory_path):
    """

    :param hdfs_directory_path:
    :return:
    """
    try:
        cmd = "hadoop fs -text " + hdfs_directory_path + "/*"
        logger.debug("hdfs read directory:" + cmd)
        return run_cmd(cmd)
    except Exception as e:
        log_cmd(cmd)
        log_error(e)


def hdfs_create_directory(hdfs_directory):
    """

    :param hdfs_directory:
    :return:
    """
    try:
        cmd = "hadoop fs -mkdir " + hdfs_directory
        run_cmd(cmd)
    except Exception as e:
        log_cmd(cmd)
        log_error(e)


def hdfs_remove_directory(hdfs_directory):
    """

    :param hdfs_directory:
    :return:
    """
    try:
        cmd = "hadoop fs -rm -r " + hdfs_directory
        run_cmd(cmd)
    except Exception as e:
        log_cmd(cmd)
        log_error(e)


def hdfs_remove_file(hdfs_file):
    """

    :param hdfs_file:
    :return:
    """
    try:
        cmd = "hadoop fs -rm " + hdfs_file
        run_cmd(cmd)
    except Exception as e:
        log_cmd(cmd)
        log_error(e)


def hdfs_copy_file(source_path, dest_path):
    """

    :param source_path:
    :param dest_path:
    :return:
    """
    try:
        cmd = "hadoop fs -cp " + source_path + " " + dest_path
        run_cmd(cmd)
    except Exception as e:
        log_cmd(cmd)
        log_error(e)



def hdfs_merge_copy_file(source_hdfs_path, dest_hdfs_path, dest_file_name):
    """

    :param source_hdfs_path:
    :param dest_hdfs_path:
    :param dest_file_name:
    :return:
    """
    try:
        hdfs_create_directory(dest_hdfs_path)
        dest_path_file_name = os.path.join(dest_hdfs_path, dest_file_name)
        cmd = "hadoop fs -text " + source_hdfs_path + " | " + "hadoop fs -put -" + dest_path_file_name
        run_cmd(cmd)
    except Exception as e:
        log_cmd(cmd)
        log_error(e)


def hdfs_copy_file_create_directory(source_hdfs_path, dest_hdfs_path, dest_file_name):
    """

    :param source_hdfs_path:
    :param dest_hdfs_path:
    :param dest_file_name:
    :return:
    """
    try:
        hdfs_create_directory(dest_hdfs_path)
        dest_path_file_name = os.path.join(dest_hdfs_path, dest_file_name)
        hdfs_copy_file(source_hdfs_path, dest_path_file_name)
    except Exception as e:
        log_error(e)
        logger.error("Error in copy file from " + source_hdfs_path + " to " + dest_path_file_name)


def hdfs_copy_from_local(source_local_file, des_hdfs_path):
    """

    :param source_local_file:
    :param des_hdfs_path:
    :return:
    """
    try:
        cmd = "hadoop fs -copyFromLocal -f " + source_local_file + " " + des_hdfs_path
        run_cmd(cmd)
    except Exception as e:
        log_cmd(cmd)
        log_error(e)


def run_cmd(cmd):
    """

    :param cmd:
    :return:
    """
    try:
        start_time = time.time()
        output = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
        elapsed_time = time.time() - start_time
        logging.info('command is complete in {:10.2f} seconds.'.format(elapsed_time))
        return output
    except Exception as e:
        log_cmd(cmd)
        log_error(e)

def log_cmd(cmd):
    """

    :param cmd:
    :return:
    """
    logger.error("============================>>")
    logger.error(str(cmd))
    logger.error("<<============================")

def log_error(e):
    """

    :param e:
    :return:
    """
    logger.error("============================>>")
    logger.error(str(e), exc_info=True)
    logger.error("<<============================")
