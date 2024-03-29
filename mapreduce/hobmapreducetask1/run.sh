#!/usr/bin/env bash

OUT_DIR="out"
NUM_REDUCERS=4

hadoop fs -rm -r -skipTrash ${OUT_DIR}.tmp > /dev/null

hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.name="dashatask1" \
    -D mapreduce.job.reduces=$NUM_REDUCERS \
    -files mapper.py,reducer.py \
    -mapper mapper.py \
    -reducer reducer.py \
    -input /data/wiki/en_articles \
    -output ${OUT_DIR}.tmp > /dev/null

hadoop fs -rm -r -skipTrash ${OUT_DIR} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.name="dashatask12" \
    -D mapreduce.job.reduces=1 \
    -D stream.num.map.output.key.fields=2 \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D mapreduce.partition.keycomparator.options='-k2,nr' \
    -mapper cat \
    -reducer cat \
    -input ${OUT_DIR}.tmp \
    -output ${OUT_DIR} > /dev/null
    
hdfs dfs -cat ${OUT_DIR}/part-00000 | head -10
