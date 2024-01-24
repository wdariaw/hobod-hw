from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from pyspark.sql.types import *
from pyspark.sql.functions import col, min, max, count, row_number, floor
from pyspark.sql.window import Window


def process_data(data_path, spark):
    schema = StructType(fields=[
        StructField("killed_by", StringType()),
        StructField("killer_name", StringType()),
        StructField("killer_placement", FloatType()),
        StructField("killer_position_x", FloatType()),
        StructField("killer_position_y", FloatType()),
        StructField("map", StringType()),
        StructField("match_id", StringType()),
        StructField("time", IntegerType()),
        StructField("victim_name", StringType()),
        StructField("victim_placement", FloatType()),
        StructField("victim_position_x", FloatType()),
        StructField("victim_position_y", FloatType()),
    ])
    
    df = spark.read\
        .schema(schema)\
        .option("header", "true")\
        .option("sep", ",")\
        .csv(data_path)
    
    w = Window.partitionBy('match_id').orderBy(col("count").desc())

    res = df.filter(col('time') < 600)\
        .withColumn('ID', floor(((col('killer_position_x') - 400000)**2 + (col('killer_position_y') - 400000)**2) / 100))\
        .groupBy('match_id', 'ID').count()\
        .dropna()\
        .withColumn("row", row_number().over(w))\
        .filter((col("row") <= 10))\
        .drop("row")

    return res


def write_to_cassandra(df):
    cluster = Cluster(['93.175.29.116'])
    
    # session = cluster.connect()
    # session.execute("create keyspace if not exists ovchinnikova with replication={'DC1': '1', 'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'dc1': '3'} and durable_writes = 'True';")
    
    session = cluster.connect('ovchinnikova')
    session.execute('drop table if exists ovchinnikova.top_ids_my_match')
    session.execute(
        '''create table top_ids_my_match ( 
               "match_id" text,
               "ID" bigint,
               "count" int,
            primary key (("match_id"), "count")
            ) 
            with clustering order by ("count" desc);''')
    
    df.write\
        .format('org.apache.spark.sql.cassandra')\
        .mode('append')\
        .options(table='top_ids_my_match', keyspace='ovchinnikova')\
        .save()
        
    cluster.shutdown()
    
    
if __name__ == '__main__':
    spark = SparkSession.builder.appName('ovchinnikova_cassandra')\
                            .master('yarn')\
                            .config("spark.cassandra.connection.host", "mipt-node05")\
                            .getOrCreate()
    df = process_data("/data/hobod/pubg", spark)
    write_to_cassandra(df)
    
    spark.stop()
