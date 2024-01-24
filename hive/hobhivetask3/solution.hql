add jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;
add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE ovchinnikovada;

DROP TABLE IF EXISTS user_day_profit;
DROP TABLE IF EXISTS user_day_profit_with_position;

CREATE TABLE user_day_profit
STORED AS TEXTFILE 
AS SELECT
    content.userInn as inn
    , DAY(content.datetime.date) as day
    , COALESCE(SUM(content.totalSum), 0) as profit
FROM kkt_document_json
WHERE content.userInn != 'NULL'
GROUP BY DAY(content.datetime.date), content.userInn;

CREATE TABLE user_day_profit_with_position
STORED AS TEXTFILE 
AS SELECT
    inn
    , day
    , profit
    , ROW_NUMBER() OVER (PARTITION BY inn ORDER BY profit DESC) AS rn
FROM user_day_profit;

SELECT 
    inn
    , day
    , profit
FROM user_day_profit_with_position
WHERE rn == 1;
