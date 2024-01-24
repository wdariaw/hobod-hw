add jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;
add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE ovchinnikovada;

DROP TABLE IF EXISTS kkt_document_json;

CREATE external TABLE kkt_document_json (
    id Struct <oid: String>
    , kktRegId String
    , subtype String
    , content Struct< 
        userInn: String
        , totalSum: Bigint
        , dateTime: struct<date: Timestamp> 
    >
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'true',
    'mapping.id' = '_id',
    'mapping.oid' = '$oid',
    'mapping.date' = '$date'
)
STORED AS TEXTFILE
LOCATION '/data/hive/fns2';


SELECT * FROM kkt_document_json LIMIT 50;
