from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType
)


PG_HOST = "dwh-postgres"  
PG_PORT = "5432"         
PG_DATABASE = "dwh_database"
PG_USER = "postgres"
PG_PASSWORD = "postgres"
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
JDBC_DRIVER = "org.postgresql.Driver"

spark = SparkSession.builder \
    .appName("postgres-to-hive") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.warehouse.dir", "/hive/warehouse")\
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")\
    .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.7.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/postgresql-42.7.7.jar") \
    .enableHiveSupport()\
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS flight_data")
spark.sql("USE flight_data")

def process_table(source_pg_table: str, target_hive_table: str):
    try:
        print(f"\n--- Reading from PG table: {source_pg_table} ---")
        
        df = spark.read.format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", f"public.{source_pg_table}") \
            .option("user", PG_USER) \
            .option("password", PG_PASSWORD) \
            .option("driver", JDBC_DRIVER) \
            .load()
        
        print(f"Schema inferred from PostgreSQL table '{source_pg_table}':")
        df.printSchema()

        # Write the resulting DataFrame to Hive
        df.write.mode("overwrite").saveAsTable(f"flight_data.{target_hive_table}")
        print(f"Successfully created Hive table: flight_data.{target_hive_table}")

    except Exception as e:
        print(f"Error processing table {source_pg_table}: {e}")



process_table("gold_dim_airport", "dim_airport")
process_table("gold_dim_date", "dim_date")
process_table("gold_dim_flight", "dim_flight")
process_table("gold_dim_passenger", "dim_passenger")
process_table("gold_dim_ticket", "dim_ticket")
process_table("gold_dim_weather", "dim_weather")
process_table("gold_fact_booking", "fact_booking")


print("\nSHOWING TABLES in Hive:")
spark.sql("SHOW TABLES IN flight_data").show()
spark.sql("SELECT * FROM dim_flight").show()

spark.stop()
# --- PHASE 1: DOWNLOAD JDBC DRIVER ---
# wget https://jdbc.postgresql.org/download/postgresql-42.7.7.jar

# # --- PHASE 2: COPY DRIVER TO ALL SPARK NODES ---
# # Master (Driver)
# docker exec spark-master mkdir -p /tmp/spark_drivers/
# docker cp ./postgresql-42.7.7.jar spark-master:/tmp/spark_drivers/

# # Worker 1 (Executor)
# docker exec spark-worker-1 mkdir -p /tmp/spark_drivers/
# docker cp ./postgresql-42.7.7.jar spark-worker-1:/tmp/spark_drivers/

# # Worker 2 (Executor)
# docker exec spark-worker-2 mkdir -p /tmp/spark_drivers/
# docker cp ./postgresql-42.7.7.jar spark-worker-2:/tmp/spark_drivers/

# # --- PHASE 3: RUN THE PYSPARK JOB WITH EXPLICIT --JARS FLAG ---
# docker exec -it spark-master /spark/bin/spark-submit \
#   --master spark://spark-master:7077 \
#   --jars /tmp/spark_drivers/postgresql-42.7.7.jar \
#   /opt/create_tables.py