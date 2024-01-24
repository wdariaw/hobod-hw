add jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;
add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE ovchinnikovada;

DROP TABLE IF EXISTS kkt_document_json_parquet;
DROP TABLE IF EXISTS kkt_document_json_orc;
DROP TABLE IF EXISTS kkt_document_json_textfile;

CREATE TABLE kkt_document_json_parquet
STORED AS PARQUET
AS SELECT * FROM kkt_document_json;

CREATE TABLE kkt_document_json_orc
STORED AS ORC
AS SELECT * FROM kkt_document_json;

CREATE TABLE kkt_document_json_textfile
STORED AS TEXTFILE
AS SELECT * FROM kkt_document_json;
