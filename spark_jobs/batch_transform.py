from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType



spark = SparkSession.builder.appName("YouTubeBatchTransform").getOrCreate()

schema = StructType([
    StructField("kind", StringType(), True),
    StructField("etag", StringType(), True),
    StructField("id", StringType(), True),
    StructField("snippet", StructType([
        StructField("publishedAt", StringType(), True),
        StructField("channelId", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("channelTitle", StringType(), True)
    ])),
    StructField("statistics", StructType([
        StructField("viewCount", StringType(), True),
        StructField("likeCount", StringType(), True),
        StructField("favoriteCount", StringType(), True),
        StructField("commentCount", StringType(), True)
    ]))
])

df_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "youtube_trending") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# Les valeurs sont en bytes, donc il faut les convertir
#df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Convertir value en string et parser JSON
df_parsed = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Extraire des colonnes de snippet et statistics
df_final = df_parsed.select(
    "id",
    "kind",
    "etag",
    col("snippet.publishedAt").alias("publishedAt"),
    col("snippet.channelId").alias("channelId"),
    col("snippet.title").alias("title"),
    col("snippet.description").alias("description"),
    col("snippet.channelTitle").alias("channelTitle"),
    col("statistics.viewCount").alias("viewCount"),
    col("statistics.likeCount").alias("likeCount"),
    col("statistics.commentCount").alias("commentCount")
)

df_final.show(truncate=False)

df_final.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/youtube") \
    .option("dbtable", "youtube_trending") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()
print("Batch transformation and load completed.")
