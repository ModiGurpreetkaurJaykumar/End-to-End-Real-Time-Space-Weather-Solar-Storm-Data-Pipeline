from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import DoubleType

spark = SparkSession.builder \
    .appName("HDFSRawToSilver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw_df = spark.read.json("/space-weather/raw")

silver_df = raw_df \
    .withColumn("timestamp", to_timestamp("timestamp")) \
    .withColumn("speed", col("speed").cast(DoubleType())) \
    .withColumn("density", col("density").cast(DoubleType())) \
    .withColumn("temperature", col("temperature").cast(DoubleType())) \
    .withColumn("bx", col("bx").cast(DoubleType())) \
    .withColumn("by", col("by").cast(DoubleType())) \
    .withColumn("bz", col("bz").cast(DoubleType())) \
    .withColumn("bt", col("bt").cast(DoubleType())) \
    .dropna()

silver_df.write.mode("overwrite").parquet("/space-weather/silver")

print("Silver layer written successfully")
