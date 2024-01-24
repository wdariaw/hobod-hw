add jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;
add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE ovchinnikovada;
DROP TABLE IF EXISTS transactions;

CREATE TABLE transactions
STORED AS TEXTFILE 
AS SELECT 
    content.userInn as inn
    , subtype as transaction_type
    , LEAD(subtype) OVER (
        PARTITION BY content.userInn ORDER BY content.datetime.date
    ) as next_transaction_type
    , LAG(subtype) OVER (
        PARTITION BY content.userInn ORDER BY content.datetime.date
    ) as previous_transaction_type
FROM kkt_document_json
WHERE content.userInn != 'NULL';

SELECT DISTINCT inn
FROM transactions
WHERE 
    transaction_type == "receipt" AND (previous_transaction_type == "closeShift" OR next_transaction_type == "openShift")
LIMIT 50;
