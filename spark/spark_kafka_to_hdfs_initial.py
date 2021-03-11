#!/usr/bin/env python
# -*- coding: utf-8 -*-
#export SPARK_KAFKA_VERSION=0.10
#/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --driver-memory 512m --driver-cores 1 --master local[1]
#/spark2.4/bin/spark-submit --driver-memory 512m --driver-cores 1 --master local[1] spark_kafka_to_hdfs_initial.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

spark = SparkSession.builder.appName("filkin_spark").getOrCreate()

kafka_brokers = "bigdataanalytics-worker-0.novalocal:6667"

#вычитываем всё, что есть в Кафке
raw_orders = spark.read. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "meetup_topic"). \
    option("startingOffsets", "earliest"). \
    load()

#raw_orders.show(1, False)

#преобразуем в string
string_orders = raw_orders.select(F.col("value").cast("String").alias("value"),
                                  F.col("timestamp").cast(DateType()).alias("date"))

#string_orders.show(1, False)

#сохраняем в формате parquet в hdfs
string_orders.write.\
    partitionBy("date").\
    mode(saveMode="overwrite").\
    parquet("meetup_stream_api_files/raw/")

spark.stop()

#смотрим, что получилось
#df = spark.read.parquet("meetup_stream_api_files/")

#df.show(1, False)
#df.count()