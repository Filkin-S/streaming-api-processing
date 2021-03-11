#!/usr/bin/env python
# -*- coding: utf-8 -*-
#export SPARK_KAFKA_VERSION=0.10
#/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --driver-memory 512m --driver-cores 1 --master local[1]
#/spark2.4/bin/spark-submit --driver-memory 512m --driver-cores 1 --master local[1] spark_hdfs_processing_initial.py


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("filkin_spark").getOrCreate()

df_raw = spark.read.parquet("meetup_stream_api_files/raw/")

#схема для разбора json из value
schema = StructType([
    StructField('venue', StructType([
        StructField('venue_name', StringType(), True),
        StructField('lon', StringType(), True),
        StructField('lat', StringType(), True),
        StructField('venue_id', StringType(), True)
    ])),
    StructField('visibility', StringType(), True),
    StructField('response', StringType(), True),
    StructField('guests', StringType(), True),
    StructField('member', StructType([
        StructField('member_id', StringType(), True),
        StructField('photo', StringType(), True),
        StructField('member_name', StringType(), True)
    ])),
    StructField('rsvp_id', StringType(), True),
    StructField('mtime', StringType(), True),
    StructField('event', StructType([
        StructField('event_name', StringType(), True),
        StructField('event_id', StringType(), True),
        StructField('time', StringType(), True),
        StructField('event_url', StringType(), True)
    ])),
    StructField('group', StructType([
        StructField('group_topics', StringType(), True),
        StructField('group_city', StringType(), True),
        StructField('group_country', StringType(), True),
        StructField('group_id', StringType(), True),
        StructField('group_name', StringType(), True),
        StructField('group_lon', StringType(), True),
        StructField('group_urlname', StringType(), True),
        StructField('group_lat', StringType(), True)
    ])),
])


#парсим json из string используя schema
df_parsed = df_raw. \
    select(F.from_json(F.col("value").cast("String"), schema).alias("value"), "date")


#раскрываем "nested" структуры в финальный датасет
df_processed = df_parsed.select("value.*", "date").\
    select("venue.*", "visibility", "response",
           "guests", "member.*", "rsvp_id", "mtime",
           "event.*", "group.*", "date")


#сохраняем в формате parquet в hdfs
df_processed.write.\
    partitionBy("date").\
    mode(saveMode="overwrite").\
    parquet("meetup_stream_api_files/processed/")

spark.stop()