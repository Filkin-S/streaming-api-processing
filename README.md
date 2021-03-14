# Streaming API processing
Код решает задачу обработки данных поступающих в реальном времени из внешнего API при помощи Kafka и Spark.  
С работой API можно ознакомиться по ссылке: https://stream.meetup.com/2/rsvps
![](https://github.com/Filkin-S/streaming-api-processing/blob/main/imgs/arch.jpg)
### Структура
**kafka_producer/producer.py** - простой python скрипт принимающий строки в формате json и загружающий их в Kafka


**spark/spark_streaming_kafka_to_hdfs.py** - скрипт PySpark сохраняющий данные из Kafka на HDFS в реальном времени.   
Данные сохраняются в необработанном виде для возможности восстановления в будущем.


**spark/spark_hdfs_compact_and_process.py** - скрипт PySpark раз в день обрабатыващий "сырые" данные предыдущего дня в табличный формат согласно текущей схеме.   
Также производит операцию "compaction" - сжимает необработанные данные предыдущего дня, уменьшая количество файлов. Этим экономится место на HDFS.


**spark/spark_hdfs_aggregates.py** - скрипт PySpark раз в день считающий и сохраняющий агрегаты из обработанных данных предыдущего дня.


Витрины данных и различные аналитические инструменты могут обращаться за данными как в папку с агрегатами, так и в папку с обработанными данными.


### Примеры
#### Схема обработанных данных:
![](https://github.com/Filkin-S/streaming-api-processing/blob/main/imgs/schema.bmp)


#### Агрегаты:

![](https://github.com/Filkin-S/streaming-api-processing/blob/main/imgs/groups.bmp)
![](https://github.com/Filkin-S/streaming-api-processing/blob/main/imgs/countries.bmp) ![](https://github.com/Filkin-S/streaming-api-processing/blob/main/imgs/cities.bmp)


#### Презентация:
https://docs.google.com/presentation/d/1JjHcKNyDOy4jnSQw1-xt92bazagvuRvCoiSzFEkOiIU/edit#slide=id.p
