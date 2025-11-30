import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

KAFKA_BROKER = "kafka:9092" 
KAFKA_TOPIC = "flight_bookings"
HDFS_OUTPUT_PATH = "hdfs://namenode:9000/data/flight_bookings_stream" 

booking_schema = StructType([
    StructField("booking_id", IntegerType(), True),
    StructField("passenger_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    
    StructField("flight_number", StringType(), True),
    StructField("carrier_name", StringType(), True),
    StructField("origin_airport_code", StringType(), True),
    StructField("destination_airport_code", StringType(), True),
    StructField("scheduled_departure", StringType(), True),
    StructField("flight_status", StringType(), True)
])

spark = SparkSession \
    .builder \
    .appName("FlightBookingHDFSSink") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session initialized. Starting Kafka stream listener and HDFS sink...")

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

booking_stream = kafka_df.selectExpr("CAST(value AS STRING) as json_payload", "timestamp AS kafka_time") \
    .select(from_json(col("json_payload"), booking_schema).alias("data"), col("kafka_time")) \
    .select(
        col("data.booking_id"),
        col("data.carrier_name"),
        col("data.flight_number"),
        col("data.origin_airport_code"),
        col("data.destination_airport_code"),
        col("data.age"),
        col("data.flight_status"),
        col("kafka_time").alias("ingestion_timestamp")
    )

print(f"Streaming data to HDFS path: {HDFS_OUTPUT_PATH}")

query = booking_stream \
    .writeStream \
    .format("parquet") \
    .option("checkpointLocation", f"{HDFS_OUTPUT_PATH}/checkpoint") \
    .option("path", HDFS_OUTPUT_PATH) \
    .trigger(processingTime='5 seconds')\
    .start()

print(f"Spark Streaming Query Started. Sinking to HDFS.")

query.awaitTermination()

# docker cp receiver.py spark-master:/opt/kafka_consumer.py       

# docker exec -it spark-master /spark/bin/spark-submit \                        
#   --master spark://spark-master:7077 \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.2 \
#   /opt/kafka_consumer.py    