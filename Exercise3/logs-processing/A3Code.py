from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lower, count, desc, window
from pyspark.sql.types import StructType, StructField, StringType, LongType

# 1. Configuration & Session Setup
CHECKPOINT_PATH = "/tmp/spark-checkpoints/logs-processing"

spark = (
    SparkSession.builder
    .appName("LogsProcessor")
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
    .getOrCreate()
)
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.sparkContext.setLogLevel("ERROR")

# 2. Define schema
schema = StructType([
    StructField("timestamp", LongType()), 
    StructField("status", StringType()),
    StructField("severity", StringType()),
    StructField("source_ip", StringType()),
    StructField("user_id", StringType()),
    StructField("content", StringType())
])

# 3. Read Stream
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "logs")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# 4. Processing, Filtering & Aggregation
# We filter first to keep the state store small, then group and count.
analysis_df = (
    raw_df.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .filter(
        (lower(col("content")).contains("crash")) & 
        ((col("severity") == "High") | (col("severity") == "Critical"))
    )
    .withColumn("event_time", (col("timestamp") / 1000).cast("timestamp")) # first ms to s than to timestamp (so we don't have long numbers)
    .withWatermark("event_time", "10 seconds") # handling 10 s late events
    .groupBy(
        window(col("event_time"), "10 seconds"),
        col("user_id")
    )
    .agg(count("*").alias("crash_count"))
    # .sort(col("window_start"), desc(col("crash_count"))) # :( not allowd for updating outputs
)

# 5. Writing
query = (
    analysis_df
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("user_id"),
        col("crash_count")
    )
    .filter(col("crash_count") > 2)
    .writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()