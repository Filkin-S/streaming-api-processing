#!/usr/bin/env python
# -*- coding: utf-8 -*-
#export SPARK_KAFKA_VERSION=0.10
#/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --driver-memory 512m --driver-cores 1 --master local[1]
#/spark2.4/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --driver-memory 512m --driver-cores 1 --master local[1] /home/BD_256_sfilkin/streaming-api-processing/spark/spark_hdfs_aggregates.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import datetime


PATH_PROCESSED = "meetup_stream_api_files/processed/date="
PATH_AGGREGATES = "meetup_stream_api_files/aggregates/"
previous_day = datetime.date.today() - datetime.timedelta(days=1)

spark = SparkSession.builder.appName("filkin_spark").getOrCreate()

# загружаем "обработанные" данные предыдущего дня
df_to_aggregate = spark.read.parquet(PATH_PROCESSED + str(previous_day))

# считаем несколько простых агрегатов и сохраняем в соотвествующие папки
df_groups = df_to_aggregate.groupBy("group_name", "group_country",
                                    "group_city", "group_lat", "group_lon", "response").\
            agg(F.count(F.col("response")).alias("response_count"))

df_groups.write.mode("overwrite").parquet(PATH_AGGREGATES + "groups" + str(previous_day))

df_countries = df_to_aggregate.groupBy("group_country").\
               agg(F.countDistinct(F.col("venue_id")).alias("venues_count"))

df_countries.write.mode("overwrite").parquet(PATH_AGGREGATES + "countries" + str(previous_day))


df_cities = df_to_aggregate.groupBy("group_city"). \
    agg(F.countDistinct(F.col("venue_id")).alias("venues_count"))

df_cities.write.mode("overwrite").parquet(PATH_AGGREGATES + "cities" + str(previous_day))

spark.stop()
