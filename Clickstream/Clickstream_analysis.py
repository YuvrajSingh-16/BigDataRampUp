#!/bin/python3
#===================================================================================================================
#
#          FILE:  Clickstream_analysis.py
#
#         USAGE:  python3 Clickstream_analysis.py 
#
#   DESCRIPTION: Runs Clickstream Analysis step-by-step & shows logs on console
#
#       OPTIONS:  --time, --env, --help
#  REQUIREMENTS:  Java8, Hadoop-2.10.1, Hive-2.3.9, Spark-3.3.0, Zeppelin-0.10.1-bin-all, Python3, Scala 2.12.15
#          BUGS:  ---
#         NOTES:  ---
#        AUTHOR:  Yuvraj Singh Kiraula, yuvraj.kiraula@impetus.com
#       COMPANY:  Impetus Technologies
#       VERSION:  2.0
#       CREATED:  23/11/2022 14:17 PM IST
#      REVISION:  ---
#===================================================================================================================


import sys
import time
import schedule
import subprocess
import argparse

from decouple import Config, RepositoryEnv
from simple_colors import green, red
from datetime import datetime

args = None

def run() -> None:
    """
        Runs Clickstream Analysis ``step-by-step`` with Live logs.

        `Steps:`
        1. Data Generation
        2. Adding new JSON to Archives
        3. Runs MR Job
        4. Inserting data into clickstream table
        5. Runs Hive Analysis
        6. Runs Spark Analysis
        7. Visualization

        `Parameters:` None
        `ReturnType:` None
    """

    ## Importing Env Variables
    config = Config(RepositoryEnv(f"envs/{args.env}.env"))

    HIVE_HOME = config('HIVE_HOME', default='/usr/local/hive')
    HADOOP_HOME = config('HADOOP_HOME', default="/usr/local/hadoop")
    CLICKSTREAM_HOME=config('CLICKSTREAM_HOME', default="/home/hadoopusr/BigDataCaseStudy/Clickstream")
    CLICKSTREAM_HOME_HDFS=config('CLICKSTREAM_HOME_HDFS', default="/user/hadoopusr/clickstream")
    VISUALIZATION_WEBPAGE_URL=config('VISUALIZATION_WEBPAGE_URL', "file:///home/hadoopusr/BigDataCaseStudy/Clickstream/index.html")


    # Using Env variables to create other variables
    HIVE = f"{HIVE_HOME}/bin/hive"
    MR_JAR=f"{CLICKSTREAM_HOME}/jars/MapReduce_LocationWiseAnalysis.jar"
    DATA_GENERATOR_JAR=f"{CLICKSTREAM_HOME}/jars/DataGenerator.jar"

    DATA_GENERATION_PATH=f"{CLICKSTREAM_HOME_HDFS}/data"
    ARCHIVE_DATA_PATH=f"{CLICKSTREAM_HOME_HDFS}/archive_data"

    
    ## Defining all the required local variables
    LogFile = f"{CLICKSTREAM_HOME}/logs/{day}-{month}-{year}_{hour}.log"
    job_num = 1

    day = datetime.now().day
    if day < 10:
        day = "0"+str(day)

    month = datetime.now().month
    if month < 10:
        month = "0"+str(month)

    year = datetime.now().year
     
    hour = datetime.now().hour
    if hour < 10:
        hour = "0"+str(hour)


    ## Starting Jobs
    print(green("[+] Calling Scheduled Job.....\n", 'bold'))

    jobs = [  ## [job heading log, command]

        ## Generating Data using jar
        ["[+] Generating data.....\n", ["java","-jar",DATA_GENERATOR_JAR, CLICKSTREAM_HOME, args.env]],

        ## Adding new JSON to Archive Location
        ["[+] Adding new JSON to Archive Location..\n", [f"{HADOOP_HOME}/bin/hdfs", "dfs", "-cp", f"{DATA_GENERATION_PATH}/*", f"{ARCHIVE_DATA_PATH}/{day}-{month}-{year}/"]],
        
        ## MR Job
        ["[+] Calling MR Job for Location Wise Analysis\n", ["java","-jar",MR_JAR, CLICKSTREAM_HOME, args.env]],
        # ["[+] Removing _SUCCESS file\n", [f"{HADOOP_HOME}/bin/hdfs", "dfs", "-rm", f"{CLICKSTREAM_HOME_HDFS}/mr_output/_SUCCESS"]],

        ## Insert Data into clickstream table
        ["[+] Inserting Data into clickstream table\n", [HIVE, "--hivevar", f"data_path={CLICKSTREAM_HOME_HDFS}/data/", "-f", f"{CLICKSTREAM_HOME}/insertIntoClickstreamTable.hql"]],

        ### HIVE Analysis
        ["[+] Calling HIVE Job MostActiveUsers.hql\n", [HIVE, "-f", f"{CLICKSTREAM_HOME}/Hive_Analysis/MostActiveUsers.hql"]],
        ["[+] Calling HIVE Job UserItemVisitAnalysis.hql\n", [HIVE, "-f", f"{CLICKSTREAM_HOME}/Hive_Analysis/UserItemVisitAnalysis.hql"]],

        ## Spark Analysis
        ["[+] Calling Spark Job DayWiseTrafficAnalysis.py\n", ["python3", f"{CLICKSTREAM_HOME}/Spark_Analysis/DayWiseTrafficAnalysis.py"]],
        ["[+] Calling Spark Job ShoppingCartAnalysis_item.py\n", ["python3", f"{CLICKSTREAM_HOME}/Spark_Analysis/ShoppingCartAnalysis_item.py"]],
        ["[+] Calling Spark Job UserActionAnalysis.py\n", ["python3", f"{CLICKSTREAM_HOME}/Spark_Analysis/UserActionAnalysis.py"]],

        ## Visualization
        ["[+] Starting Analysis Visualization\n", ["python3", "outputVisualization.py"]],
        ["[+] Opening Visualization Web Page\n", ["python3", "-m", "webbrowser", VISUALIZATION_WEBPAGE_URL]]
    ]

    ## Running all the Jobs one-by-one
    for job, cmd in jobs:
        print(green(job, 'bold'))
        subprocess.call(cmd)

        ## Checking if any process failed
        # subprocess.call(["python3", f"{CLICKSTREAM_HOME}/checkLastJobSuccess.py", "-n", f"{job_num}", "-f", f"{LogFile}", "-p", f"{CLICKSTREAM_HOME}"])
        # job_num += 1

    print(green("[+] All jobs Completed !!\n", 'bold'))
    sys.exit()



## ArgParser
parser = argparse.ArgumentParser()
parser.add_argument("-t", "--time", help="Scheduled Run Time")
parser.add_argument("-e", "--env", help="Environment to use i.e, prod, dev or test")
args = parser.parse_args()

## Environment check
envs = ["prod", "test", "dev"]
if args.env not in envs:
    print(red("[-] Please choose correct env from: "+", ".join(envs)))
    sys.exit()



## Adding job to the Scheduler
schedule.every().day.at(str(args.time)).do(run)

print(green(f"\n[+] Job Scheduled for {args.time}, for {args.env} environment", 'bold'))


## Checking if any pending scheduled jobs to be run, every second
while True:
    try:
        schedule.run_pending()
        time.sleep(1)
    except KeyboardInterrupt:
        print(red("\n\n[-] KeyboardInterrupt detected!! \nQuiting...", 'bold'))
        sys.exit()
    except Exception as e:
        print(red(e))
        sys.exit()
