from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder \
    .appName("SolarWindKafkaToHDFS") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("timestamp", StringType()) \
    .add("speed", StringType()) \
    .add("density", StringType()) \
    .add("temperature", StringType()) \
    .add("bx", StringType()) \
    .add("by", StringType()) \
    .add("bz", StringType()) \
    .add("bt", StringType())

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "master:9092") \
    .option("subscribe", "solar-wind-raw") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

query = parsed_df.writeStream \
    .format("json") \
    .option("path", "hdfs://master:9000/space-weather/raw") \
    .option("checkpointLocation", "hdfs://master:9000/space-weather/checkpoints/solar") \
    .outputMode("append") \
    .start()

query.awaitTermination()
