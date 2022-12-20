USE clickstream_db;

TRUNCATE TABLE ActiveUsers;

INSERT INTO TABLE ActiveUsers SELECT userID, COUNT(*) AS Count FROM clickstream GROUP BY userID ORDER BY Count DESC;
