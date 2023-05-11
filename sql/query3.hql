USE projectdb;
INSERT OVERWRITE LOCAL DIRECTORY 'output/q3' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
SELECT from_unixtime(train.`timestamp`) as time, MAX(train.open-train.close) as max_dif FROM train GROUP BY `timestamp`, train.asset_id LIMIT 300;
