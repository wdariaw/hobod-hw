add jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;
add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE ovchinnikovada;

DROP TABLE IF EXISTS user_part_sum;
DROP TABLE IF EXISTS user_first_part_sum;
DROP TABLE IF EXISTS user_second_part_sum;

CREATE TABLE user_part_sum
STORED AS TEXTFILE 
AS SELECT
    content.userInn as inn
    , COALESCE(content.totalSum, 0) as totalSum
    , CASE WHEN HOUR(content.datetime.date) < 13 THEN 0 ELSE 1 END as part
FROM kkt_document_json
WHERE content.userInn != 'NULL';

CREATE TABLE user_first_part_sum
STORED AS TEXTFILE 
AS SELECT
    inn
    , AVG(totalSum) as first_part_totalSum
FROM user_part_sum
WHERE part = 0
GROUP BY inn;

CREATE TABLE user_second_part_sum
STORED AS TEXTFILE 
AS SELECT
    inn
    , AVG(totalSum) as second_part_totalSum
FROM user_part_sum
WHERE part = 1
GROUP BY inn;

SELECT 
    l.inn
    , ROUND(first_part_totalSum) as first_part_totalSum
    , ROUND(second_part_totalSum) as second_part_totalSum
FROM 
    user_first_part_sum l LEFT JOIN user_second_part_sum r
    ON l.inn == r.inn
WHERE l.first_part_totalSum > r.second_part_totalSum
ORDER BY first_part_totalSum
LIMIT 50;
