
CREATE EXTERNAL TABLE IF NOT EXISTS clickstream_db.LocationWiseTraffic ( Location String, Traffic_Count int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
Stored as textfile
location '${mr_out}';