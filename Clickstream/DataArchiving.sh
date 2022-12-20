#!/bin/bash
#===================================================================================================================
#
#          FILE:  DataArchiving.sh
#
#         USAGE:  bash DataArchiving.sh 
#                      OR 
#                 ./DataArchiving.sh
#
#   DESCRIPTION: Runs Data Archiving process
#
#       OPTIONS:  ---
#  REQUIREMENTS:  Java8, Hadoop-2.10.1, Python3
#          BUGS:  ---
#         NOTES:  ---
#        AUTHOR:  Yuvraj Singh Kiraula, yuvraj.kiraula@impetus.com
#       COMPANY:  Impetus Technologies
#       VERSION:  1.0
#       CREATED:  01/12/2022 01:20 PM IST
#      REVISION:  ---
#===================================================================================================================


## Exporting environment variables
export $(grep -v '^#' /home/hadoopusr/BigDataCaseStudy/Clickstream/envs/cron.env | xargs)

### Directories & file paths
DATA_GENERATION_PATH=$CLICKSTREAM_HOME_HDFS/data/
ARCHIVE_DATA_PATH=$CLICKSTREAM_HOME_HDFS/archive_data/

## Logs dir & file
LOG_DIR=$CLICKSTREAM_HOME/logs/
LogFile=$LOG_DIR`date +"%d-%m-%Y_%H".log`

CurrentDay=`date +"%d-%m-%Y"`


## Checking & Archiving data
echo -e "\n\n[+] Checking Older Archives..." >> $LogFile
/bin/python3 $CLICKSTREAM_HOME/archive_helper/checkArchives.py >> $LogFile 2>&1

## Archiving Data
echo -e "\n\n[+] Archiving Data..." >> $LogFile
python3 $CLICKSTREAM_HOME/archive_helper/archiveData.py >> $LogFile 2>&1

## Creating current day directory for archiving
echo -e "\n\n[+] Creating current day directory..." >> $LogFile
$HADOOP_HOME/bin/hdfs dfs -mkdir $ARCHIVE_DATA_PATH/$CurrentDay