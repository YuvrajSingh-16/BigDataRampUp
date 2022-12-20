import sys
import argparse
import subprocess

## ArgParser
parser = argparse.ArgumentParser()
parser.add_argument("-n", "--num", help="Job number to check for")
parser.add_argument("-f", "--file", help="Log file path")
parser.add_argument("-p", "--path", help="Clickstream Home path")

args = parser.parse_args()


## Getting values form argparser
CLICKSTREAM_HOME = args.path
job_num = int(args.num)
log_file = args.file

required_jobs_status = []
## Reading required_jobs_status from txt file
with open(f"{CLICKSTREAM_HOME}/required_jobs_status") as f:
    required_jobs_status = list(f.read().split("\n"))

## Fetching last line of log file
last_line = subprocess.getoutput(f"tac {log_file} |egrep -m 1 .")

# print("[[+]] Last Line: "+last_line)

## Exceptional case for moving data to archive path
if job_num == 2 and required_jobs_status[job_num-1] == "exists":
    sys.exit()

## Checking if current job status matches desired status
if required_jobs_status[job_num-1] != last_line.split(" ")[-1]:
    with open(f"{CLICKSTREAM_HOME}/failed_jobs.txt", "a") as f:
        f.write(args.num)
        f.write("\n")
        f.close()