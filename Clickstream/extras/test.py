import subprocess
from datetime import date, datetime
from snakebite.client import Client


def deleteOldArchives():
	client = Client('localhost', 9000)
	for x in client.ls(['/user/hadoopusr/clickstream/archive_data/']):
		file_date = x['path'].split("/")[-1].split(".")[0].split("_")[1]
		date_split = list(map(int, file_date.split("-")))

		print(x['path'])
		d1 = date(date_split[2], date_split[1], date_split[0])
		span = date.today()-d1
		
		if span.days > 7:
			subprocess.call(["hdfs", "dfs", "-rm", x['path']])
	

# out = subprocess.run(["/usr/local/zeppelin-0.10.1-bin-all/bin/zeppelin-daemon.sh", "start"], capture_output=True, text=True)
# print(out.stdout)
# print(out)

log_file_name = "logs/logs"+datetime.now().strftime("%d-%m-%Y_%H")+".log"
log_file = open(log_file_name, "a+")


# print("Generating data")
# ## Data generate
# subprocess.call(["java","-jar","/home/hadoopusr/BigDataCaseStudy/Clickstream/DataGenerator-100k.jar",">",log_file_name,"2>&1"])

# print("Calling MR")
# ## MR
# subprocess.call(["java","-jar","/home/hadoopusr/BigDataCaseStudy/Clickstream/MapReduce_LocationWiseAnalysis.jar",">",log_file_name,"2>&1"])

# print("Creating DB")
# ## Create db & table
# subprocess.call(["/usr/local/hive/bin/hive", "-f", "/home/hadoopusr/BigDataCaseStudy/Clickstream/createClickstreamTable.hql",">",log_file_name,"2>&1"])


# ### HIVE Query For MR Output to Hive Table
# print("[+] Inserting MR output into Hive table\n")
# subprocess.call(["/usr/local/hive/bin/hive", "-f", "MR_out_to_hiveTable.hql",">",log_file_name,"2>&1"])
print(log_file_name)
subprocess.call(["python3", "Spark_Analysis/DayWiseTrafficAnalysis.py"], stdout=log_file)

subprocess.call(["python3", "Spark_Analysis/ShoppingCartAnalysis_item.py"], stdout=log_file)
log_file.close()


"""
CREATE TABLE IF NOT EXISTS clickstream(
userID string,
sessionId string,
action string,
url string,
logTime string,
payment_method string,
logDate string)
partitioned by (location string)
clustered by (action) into 4 buckets
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe';
"""