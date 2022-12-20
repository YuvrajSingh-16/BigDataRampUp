USE clickstream_db;

CREATE TEMPORARY TABLE clickstream_tmp(
userID string,
location string,
sessionId string,
action string,
url string,
logTime string,
payment_method string,
logDate string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe';

LOAD DATA INPATH '${data_path}' INTO TABLE clickstream_tmp;


TRUNCATE TABLE clickstream;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE clickstream
partition(location)
SELECT userID, sessionId, action, url, logTime, payment_method, logDate, location FROM clickstream_tmp;