#!/bin/bash
#===================================================================================================================
#
#          FILE:  Clickstream_analysis_script.sh
#
#         USAGE:  bash Clickstream_analysis_script.sh 
#                      OR 
#                 ./Clickstream_analysis_script.sh
#
#   DESCRIPTION: Runs Clickstream Analysis step-by-step & stores logs 
#
#       OPTIONS:  ---
#  REQUIREMENTS:  Java8, Hadoop-2.10.1, Hive-2.3.9, Spark-3.3.0, Zeppelin-0.10.1-bin-all, Python3, Scala 2.12.15
#          BUGS:  ---
#         NOTES:  ---
#        AUTHOR:  Yuvraj Singh Kiraula, yuvraj.kiraula@impetus.com
#       COMPANY:  Impetus Technologies
#       VERSION:  3.0
#       CREATED:  23/11/2022 14:17 PM IST
#      REVISION:  ---
#===================================================================================================================


## Exporting environment variables
export $(grep -v '^#' /home/hadoopusr/BigDataCaseStudy/Clickstream/envs/cron.env | xargs)


### Directories & file paths
DATA_GENERATION_PATH=$CLICKSTREAM_HOME_HDFS/data/
ARCHIVE_DATA_PATH=$CLICKSTREAM_HOME_HDFS/archive_data/
## Jar paths
DATA_GENERATOR_JAR=$CLICKSTREAM_HOME/jars/DataGenerator.jar
MR_JAR=$CLICKSTREAM_HOME/jars/MapReduce_LocationWiseAnalysis.jar
## Logs dir & file
LOG_DIR=$CLICKSTREAM_HOME/logs/
LogFile=$LOG_DIR`date +"%d-%m-%Y_%H".log`
CurrentDay=`date +"%d-%m-%Y"`
ENV=cron

declare -A jobs_name=(
        [1]="[+] Generating data"
        [2]="\n\n[+] Adding new JSON to the Archive location...\n[+] Completed!" 
        [3]="\n\n[+] Calling MR Job for LocationWise Analysis"
        [4]="\n\n[+] Inserting Data into clickstream table"
        [5]="\n\n[+] Calling HIVE Job MostActiveUsers.hql"
        [6]="\n\n[+] Calling HIVE Job UserItemVisitAnalysis.hql"
        [7]="\n\n[+] Calling Spark Job DayWiseTrafficAnalysis.py"
        [8]="\n\n[+] Calling Spark Job ShoppingCartAnalysis_item.py"
        [9]="\n\n[+] Calling Spark Job UserActionAnalysis.py"
        [10]="\n\n[+] Starting Visualization of Analysis"
        [11]="\n\n[+] Opening Visualization WebPage\n\n"
)

declare -A jobs=(
        [1]="java -jar $DATA_GENERATOR_JAR $CLICKSTREAM_HOME $ENV" 
        [2]="$HADOOP_HOME/bin/hdfs dfs -cp $DATA_GENERATION_PATH/* $ARCHIVE_DATA_PATH/$CurrentDay/"
        [3]="java -jar $MR_JAR $CLICKSTREAM_HOME $ENV"
        [4]="$HIVE_HOME/bin/hive --hivevar data_path=$CLICKSTREAM_HOME_HDFS/data/ -f $CLICKSTREAM_HOME/insertIntoClickstreamTable.hql"
        [5]="$HIVE_HOME/bin/hive -f $CLICKSTREAM_HOME/Hive_Analysis/MostActiveUsers.hql"
        [6]="$HIVE_HOME/bin/hive -f $CLICKSTREAM_HOME/Hive_Analysis/UserItemVisitAnalysis.hql"
        [7]="/bin/python3 $CLICKSTREAM_HOME/Spark_Analysis/DayWiseTrafficAnalysis.py"
        [8]="/bin/python3 $CLICKSTREAM_HOME/Spark_Analysis/ShoppingCartAnalysis_item.py"
        [9]="/bin/python3 $CLICKSTREAM_HOME/Spark_Analysis/UserActionAnalysis.py"
        [10]="/bin/python3  $CLICKSTREAM_HOME/outputVisualization.py"
        [11]="bash $CLICKSTREAM_HOME/urlopener.sh"
)


## Fetching first failed job
typeset -i START_JOB=$(head -n 1 $CLICKSTREAM_HOME/failed_jobs.txt)

if [[ "$START_JOB" == 0 ]]
then
      START_JOB=1
fi

## Clearing failed_jobs.txt
eval > $CLICKSTREAM_HOME/failed_jobs.txt

## Starting jobs
JOBS_COUNT=11
for ((i=START_JOB;i<=JOBS_COUNT;i++)); do
    echo -e ${jobs_name[$i]} >> $LogFile
    eval ${jobs[$i]} >> $LogFile 2>&1

    ## Checking for any failure in current job
    eval "/bin/python3 $CLICKSTREAM_HOME/checkLastJobSuccess.py -n $i -f $LogFile -p $CLICKSTREAM_HOME"
    
    ## Exit if found some error
    if [[ $(head -n 1 $CLICKSTREAM_HOME/failed_jobs.txt) ]]; then
        exit 1
    fi
done