#!/bin/python3
#==================================================================================================================================================
#
#          FILE:  Archive_utils.py
#
#         USAGE:  python3 Archive_utils.py 'from Archive_utils import *; deleteOldArchives()'
#
#   DESCRIPTION: Consists of Archive utility functions like - deleteOldArchives which deletes Archives older than given time frame in days 
#
#       OPTIONS:  ---
#  REQUIREMENTS:  Hadoop-2.10.1, Python3
#          BUGS:  ---
#         NOTES:  ---
#        AUTHOR:  Yuvraj Singh Kiraula, yuvraj.kiraula@impetus.com
#       COMPANY:  Impetus Technologies
#       VERSION:  2.0
#       CREATED:  23/11/2022 14:17 PM IST
#      REVISION:  ---
#==================================================================================================================================================


import subprocess

from datetime import date
from snakebite.client import Client
from decouple import Config, RepositoryEnv, config as cnf


## Deletes Data Older than 7 days
def deleteOldArchives(env="dev") -> None:
    """
        Deletes Archives Older than 7 days(default)

        `Parameters:` env: Environment (default = "dev")
        `ReturnType:` None
    """

    CLICKSTREAM_HOME = cnf('CLICKSTREAM_HOME', default="/home/hadoopusr/BigDataCaseStudy/Clickstream")

    config = Config(RepositoryEnv(f"{CLICKSTREAM_HOME}/envs/{env}.env"))
    CLICKSTREAM_HOME_HDFS = config('CLICKSTREAM_HOME_HDFS', default="/user/hadoopusr/clickstream")
    HADOOP_HOME = config('HADOOP_HOME', default="/usr/local/hadoop")
    
    ARCHIVE_DATA_PATH=f"{CLICKSTREAM_HOME_HDFS}/archive_data"

    try:
        client = Client('localhost', 9000)
        for x in client.ls([f"{ARCHIVE_DATA_PATH}/"]):
            if len(x['path'].split("/")[-1].split(".")) == 2 and x['path'].split("/")[-1].split(".")[1] == "har":
                file_date = x['path'].split("/")[-1].split(".")[0]
                date_split = list(map(int, file_date.split("-")))

                d1 = date(date_split[2], date_split[1], date_split[0])
                span = date.today()-d1

                if span.days > 7:
                    subprocess.call([f"{HADOOP_HOME}/bin/hdfs", "dfs", "-rm", "-r", x['path']])
    except Exception as e:
        print(e)



def archiveFolderData(env="dev") -> None:
    """
        Archives datewise folders in .har files

        `Parameters:` env: Environment (default = "dev")
        `ReturnType:` None
    """

    CLICKSTREAM_HOME = cnf('CLICKSTREAM_HOME', default="/home/hadoopusr/BigDataCaseStudy/Clickstream")

    config = Config(RepositoryEnv(f"{CLICKSTREAM_HOME}/envs/{env}.env"))
    CLICKSTREAM_HOME_HDFS = config('CLICKSTREAM_HOME_HDFS', default="/user/hadoopusr/clickstream")
    HADOOP_HOME = config('HADOOP_HOME', default="/usr/local/hadoop")
    
    ARCHIVE_DATA_PATH=f"{CLICKSTREAM_HOME_HDFS}/archive_data"

    try:
        client = Client('localhost', 9000)
        for x in client.ls([f"{ARCHIVE_DATA_PATH}/"]):
            ## If fileType is directory and has no extension
            if x['file_type'] == 'd' and len(x['path'].split("/")[-1].split(".")) == 1:
                curr_date = x['path'].split("/")[-1]

                ## Creating archive
                subprocess.call([f"{HADOOP_HOME}/bin/hadoop", "archive", "-archiveName", f"{curr_date}.har", "-p", f"{ARCHIVE_DATA_PATH}", f"{curr_date}", f"{ARCHIVE_DATA_PATH}"])

                ## Removing folder
                subprocess.call([f"{HADOOP_HOME}/bin/hdfs", "dfs", "-rm", "-r", x['path']])

    except Exception as e:
        print(e)
