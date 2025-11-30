from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json, col, current_timestamp, upper, trim, year, month, dayofmonth, lit
from pyspark.sql import SparkSession


booking_schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("ticket_id", StringType(), True),
    StructField("flight_info", StructType([
        StructField("flight_id", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("dest", StringType(), True),
        StructField("airline", StringType(), True)
    ]), True),
    StructField("passenger", StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("gender", StringType(), True)
    ]), True),
    StructField("price", IntegerType(), True),
    StructField("weather", StringType(), True)
])


spark = SparkSession.builder.appName("KafkaToHDFSStream").config("spark.extraListeners.forceEventType", "true").config("spark.sql.streaming.checkpointLocation", "hdfs://namenode:9000/stream_checkpoints").config("spark.driver.memory", "512m").config("spark.executor.memory", "1g").config("spark.executor.cores", "1").config("spark.executor.instances", "1").getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session initialized successfully, ready for streaming!")

KAFKA_SERVER = "kafka:9092"
TOPIC_NAME = "bookings"

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .load()

# 4. Transform (Bronze Layer ETL)
bronze_df = kafka_df \
    .selectExpr("CAST(value AS STRING) AS json_payload", "timestamp") \
    .withColumn("data", from_json(col("json_payload"), booking_schema)) \
    .select(
        col("data.event_time").alias("event_time_str"),
        col("timestamp").alias("kafka_ingest_time"),
        col("data.ticket_id").alias("ticket_id"),
        col("data.flight_info.flight_id").alias("flight_id"),
        col("data.flight_info.origin").alias("origin_airport"),
        col("data.flight_info.dest").alias("dest_airport"),
        col("data.flight_info.airline").alias("airline_name"),
        col("data.passenger.id").alias("passenger_id"),
        col("data.price").alias("ticket_price")
    )

# 5. Write Stream to HDFS (Data Lake)
HDFS_OUTPUT_PATH = "hdfs://namenode:9000/data_lake/bronze/live_bookings"
CHECKPOINT_PATH = "hdfs://namenode:9000/stream_checkpoints/bookings_bronze" 

query = bronze_df.writeStream \
    .format("parquet") \
    .option("path", HDFS_OUTPUT_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .outputMode("append") \
    .trigger(processingTime='15 seconds') \
    .start()

print(f"Spark Streaming Query Started. Data is being written to: {HDFS_OUTPUT_PATH}")

# Use this to keep the job running in the notebook
query.awaitTermination()

# Flatten and select necessary columns for Silver processing
base_silver_df = bronze_df.select(
    col("data.ticket_id").alias("ticket_id"),
    col("data.price").alias("ticket_price"),
    col("data.weather").alias("weather_condition"),
    col("data.flight_info.flight_id").alias("flight_id"),
    col("data.flight_info.origin").alias("origin_airport_code"),
    col("data.flight_info.dest").alias("dest_airport_code"),
    col("data.flight_info.airline").alias("airline_name"),
    col("data.passenger.id").alias("passenger_id"),
    col("kafka_ingest_time")
)

silver_df = base_silver_df \
    .withColumn("origin_airport_code", upper(trim(col("origin_airport_code")))) \
    .withColumn("dest_airport_code", upper(trim(col("dest_airport_code")))) \
    .filter(col("ticket_price").isNotNull()) \
    .filter(col("flight_id").isNotNull()) \
    .withColumn("ingest_year", year(col("kafka_ingest_time"))) \
    .withColumn("ingest_month", month(col("kafka_ingest_time"))) \
    .withColumn("ingest_day", dayofmonth(col("kafka_ingest_time"))) \
    .withColumn("data_layer", lit("SILVER"))

# CLEANED date
query_silver = silver_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/data_lake/silver/live_bookings") \
    .option("checkpointLocation", "hdfs://namenode:9000/stream_checkpoints/bookings_silver") \
    .partitionBy("ingest_year", "ingest_month") \
    .outputMode("append") \
    .trigger(processingTime='15 seconds') \
    .start()

print(f"Silver Stream Started. Cleaned data being written to HDFS, partitioned by date.")

# This keeps both the Bronze and Silver streaming jobs running continuously
print("\nWaiting for all streaming queries to terminate (Running continuously)...")
spark.streams.awaitAnyTermination()

# docker exec -it spark-master /spark/bin/spark-submit \
#   --master spark://spark-master:7077 \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.2 \
#   /opt/kafka_consumer.py