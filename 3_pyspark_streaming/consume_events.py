from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import col, from_json

# --- 1. Initialize SparkSession ---
# SparkSession is the entry point to programming Spark with the Dataset and DataFrame API.
# It allows us to create DataFrames, read data, and interact with Spark functionalities.

spark = SparkSession.builder \
        .appName("ClickstreamKafkaToConsole") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
        .getOrCreate()

# set log level to warn to reduce the verbosity in the console output
spark.sparkContext.setLogLevel("WARN")

print("SparkSession created successfully")

# --- 2. Define Kafka Configuration ---
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "raw_clickstream" # This should match the topic you created

# --- 3. Define the Schema for your Clickstream Events ---
# This schema must precisly match the structure of the json events that we consume
clickstream_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", TimestampType(), True), # Use TimestampType for date/time fields
    StructField("page_url", StringType(), True),
    StructField("referrer_url", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("product_id", StringType(), True), # Can be null for non-product events
    StructField("dwell_time_ms", IntegerType(), True) # Can be null for non-page_view events    
])

# --- 4. Read Stream from Kafka ---
#  We use readStream to create a dataFrame representing the stream of data from Kafka
# .format("kafka") specifies the source as kafka
# .option("kafka.bootstrap.servers") points to the kafka broker
# .option("subscribe", ....) specifies the topic to consume
# .option("startingOffsets", "earliest") ensures that when the application starts,
#   it reads from the beginning of the topic. For production, you might use "latest"
#   or a specific offset.

kafka_stream_df = spark.readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                    .option("subscribe", kafka_topic) \
                    .option("startingOffsets", "earliest") \
                    .load()

print(f"Reading stream from kafka topic {kafka_topic}")


# --- 5. Process the Stream Data ---
# The raw Kafka message comes with 'key', 'value', 'topic', 'partition', 'offset', 'timestamp', 'timestampType'.
# The actual clickstream event data is in the 'value' column, which is in binary format.
# 1. Cast 'value' to StringType.
# 2. Parse the JSON string from the 'value' column using the predefined schema.
# 3. Select all fields from the parsed JSON.

processed_df = kafka_stream_df \
               .selectExpr("CAST(value as STRING) as json_data", "timestamp as kafka_ingestion_time") \
               .select(from_json(col("json_data"), clickstream_schema).alias("data"), col("kafka_ingestion_time")) \
               .select("data.*", "kafka_ingestion_time")

# You can add further transformations here, e.g., filtering, adding new columns, etc.
# For example, let's add a simple filter for 'page_view' events
page_views_df = processed_df.filter(col("event_type") == "page_view")


# --- 6. Write Stream to a Console Sink (for testing) ---
# For initial verification, writing to console is very helpful.
# .format("console") outputs the DataFrame content to the console.
# .outputMode("append") is suitable for aggregations or when you only care about new rows.
#   Other modes: "complete" (full result table), "update" (only rows that changed since last trigger).
# .trigger(processingTime="5 seconds") tells Spark to process data in micro-batches every 5 seconds.
# .option("truncate", "false") ensures that the full content of columns is displayed in the console.
# .option("checkpointLocation", ...) is crucial for fault tolerance and recovery in Structured Streaming.
#   Spark uses this directory to store metadata about the stream (offsets, schemas, etc.).
#   It must be a HDFS compatible path (e.g., local file system path, S3, Azure Blob Storage).
checkpoint_location = "/tmp/spark_clickstream_checkpoint" # Create this directory if it doesn't exist

print(f"Writing stream to console with checkpoint location: {checkpoint_location}")

query = page_views_df \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("checkpointLocation", checkpoint_location) \
    .trigger(processingTime="5 seconds") \
    .start()

print("PySpark Structured Streaming query started. Press Ctrl+C to stop.")

# --- 7. Await Termination ---
# This keeps the Spark application running until it is manually terminated (e.g., Ctrl+C).
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stream terminated by user.")
finally:
    spark.stop()
    print("SparkSession stopped.")