# corrected_cleanup_checkpoint.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HDFSCheckpointCleanup").getOrCreate()
sc = spark.sparkContext

CHECKPOINT_PATH = "hdfs://namenode:9000/data/flight_bookings_stream/checkpoint"

print(f"Attempting to delete HDFS checkpoint path: {CHECKPOINT_PATH}")

try:
    Path = sc._jvm.org.apache.hadoop.fs.Path
    
    fs = Path(CHECKPOINT_PATH).getFileSystem(sc._jsc.hadoopConfiguration())
    
    if fs.exists(Path(CHECKPOINT_PATH)):
        success = fs.delete(Path(CHECKPOINT_PATH), True)
        if success:
            print("✅ Checkpoint successfully deleted.")
        else:
            print("❌ Failed to delete checkpoint (check HDFS permissions).")
    else:
        print("⚠️ Checkpoint path not found. Proceeding to next step.")

except Exception as e:
    print(f"An error occurred during HDFS operation: {e}")

spark.stop()