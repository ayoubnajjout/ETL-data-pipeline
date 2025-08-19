from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("KafkaPySparkConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("location", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("cell", StringType(), True)
])

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9093") \
    .option("subscribe", "user_data") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_string")
df_json = df_parsed.select(from_json(col("json_string"), schema).alias("data"))

df_users = df_json.select("data.*")

query = df_users.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()