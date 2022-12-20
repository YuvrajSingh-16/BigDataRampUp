#!/bin/bash

CLICKSTREAM_HOME=/home/hadoopusr/BigDataCaseStudy/Clickstream
### HIVE Analysis
## Calling HIVE Job MostActiveUsers.hql
# LOG_DIR=$CLICKSTREAM_HOME"/logs/"
# LOG_FILE=$LOG_DIR`date +"%d-%m-%Y_%H".log`
# LOG_FILE+=".log"


LOG_DIR=$CLICKSTREAM_HOME/logs/
LOG_FILE=$LOG_DIR`date +"%d-%m-%Y_%H".log`
# echo $LOG_FILE
#echo -e "[+] Generating data......\n\n" >> $LOG_FILE


# brave "http://localhost:8085/#/notebook/2HG2CKNQQ"
# bash $CLICKSTREAM_HOME/urlopener.sh >> $LOG_FILE


# declare -A animals=( [1]="cow" 
#                      [2]="dog")

# echo "${animals[1]}"
# for sound in "${!animals[@]}"; do echo "$sound - ${animals[$sound]}"; done

# DATA_GENERATOR_JAR=/path/to/data-generator
# ENV=dev

# STRING="java -jar $DATA_GENERATOR_JAR $CLICKSTREAM_HOME $ENV >> $LogFile 2>&1"
# echo $STRING


# END=5
# for ((i=1;i<=END;i++)); do
#     echo $i
# done


DATA_GENERATOR_JAR=$CLICKSTREAM_HOME/jars/DataGenerator.jar
ENV=cron

declare -A jobs=( 
                [1]="java -jar $DATA_GENERATOR_JAR $CLICKSTREAM_HOME $ENV" 
                )

# eval ${jobs[1]} >> $LOG_FILE 2>&1

typeset -i START_JOB=$(head -n 1 /home/hadoopusr/BigDataCaseStudy/Clickstream/failed_jobs.txt)

echo $START_JOB

if [[ "$START_JOB" == 0 ]]
then
      START_JOB=1
fi

echo $START_JOB

if [[ $(head -n 1 $CLICKSTREAM_HOME/failed_jobs.txt) ]]; then
    exit 1
fi

echo "All good!!!"
# eval > $CLICKSTREAM_HOME/failed_jobs.txt