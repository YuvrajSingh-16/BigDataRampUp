USE clickstream_db;

TRUNCATE TABLE UserItemVisit;

INSERT INTO UserItemVisit SELECT SUBSTRING_INDEX(url, '?', -1) AS product, COUNT(*) as count FROM clickstream_db.clickstream WHERE url LIKE '%view%' GROUP BY url ORDER BY count DESC;
