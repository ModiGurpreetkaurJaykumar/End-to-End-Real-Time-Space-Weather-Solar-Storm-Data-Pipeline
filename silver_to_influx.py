from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
import requests

INFLUX_URL = "http://master:8086/api/v2/write"
ORG = "mcsc"
BUCKET = "space_weather"
TOKEN = "ugUi53flu4TYRnHMjlYXdCIaknLajpYU3n-NyI5XW2yQztyiv35rUKC8m5-62QG4jpUIDGsoFyJPILqsBpUz_A=="

spark = SparkSession.builder \
    .appName("SilverToInfluxDB") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("/space-weather/silver")

df = df.withColumn(
    "ts",
    to_timestamp(col("timestamp"))
)

def write_to_influx(partition):
    headers = {
        "Authorization": f"Token {TOKEN}",
        "Content-Type": "text/plain"
    }

    for row in partition:
        if row.ts is None:
            continue

        line = (
            f"solar_wind "
            f"speed={row.speed},"
            f"density={row.density},"
            f"temperature={row.temperature},"
            f"bx={row.bx},"
            f"by={row.by},"
            f"bz={row.bz},"
            f"bt={row.bt} "
            f"{int(row.ts.timestamp() * 1e9)}"
        )

        requests.post(
            f"{INFLUX_URL}?org={ORG}&bucket={BUCKET}&precision=ns",
            headers=headers,
            data=line
        )

df.foreachPartition(write_to_influx)

print("✔ Data successfully written to InfluxDB")
