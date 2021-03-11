#!/usr/bin/env python
# -*- coding: utf-8 -*-
#export SPARK_KAFKA_VERSION=0.10
#/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --driver-memory 512m --driver-cores 1 --master local[1]
#/spark2.4/bin/spark-submit --driver-memory 512m --driver-cores 1 --master local[1] spark_streaming_kafka_to_hdfs.py.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import datetime

spark = SparkSession.builder.appName("filkin_spark").getOrCreate()

kafka_brokers = "bigdataanalytics-worker-0.novalocal:6667"

# подключаемся к Кафке в режиме стрима
raw_orders = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "meetup_topic"). \
    option("startingOffsets", "latest"). \
    load()

# преобразуем данные в string
string_orders = raw_orders.select(F.col("value").cast("String").alias("value"),
                                  F.col("timestamp").cast(DateType()).alias("date"))

#пишем стрим в foreachBatch, чтобы делать логику в зависимости от каждого микробатча
def file_sink(df, freq):
    return df.writeStream.foreachBatch(foreach_batch_function) \
        .trigger(processingTime='%s seconds' % freq) \
        .option("checkpointLocation", "meetup_checkpoint/") \
        .start()

#в каждом микробатче фиксируем дату и пишем файлы в свою директорию, по датам
def foreach_batch_function(df, epoch_id):
    load_time = datetime.datetime.now().strftime("%Y-%m-%d")
    df.write \
      .mode("append") \
      .parquet("meetup_stream_api_files/raw/date=" + str(load_time))


stream = file_sink(string_orders, 60)

#запускаем бесконечный цикл
while(True):
    print("I'M STILL ALIVE")
    stream.awaitTermination(10)

#unreachable
spark.stop()

