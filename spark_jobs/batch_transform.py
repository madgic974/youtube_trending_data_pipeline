import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType


spark = SparkSession.builder.appName("YouTubeBatchTransform").getOrCreate()

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = "youtube_trending"
PG_HOST = "postgres"
PG_DB = os.getenv("POSTGRES_DB", "youtube")

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

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = df_raw.select(
    from_json(col("value").cast("string"), schema).alias("data"),
).select("data.*")

# Extraire des colonnes de snippet et statistics
df_final = df_parsed.select(
    "id",
    "kind",
    "etag",
    to_timestamp(col("snippet.publishedAt")).alias("publishedAt"),
    col("snippet.channelId").alias("channelId"),
    col("snippet.title").alias("title"),
    col("snippet.description").alias("description"),
    col("snippet.channelTitle").alias("channelTitle"),
    col("statistics.viewCount").cast(LongType()).alias("viewCount"),
    col("statistics.likeCount").cast(LongType()).alias("likeCount"),
    col("statistics.commentCount").cast(LongType()).alias("commentCount")
)

def upsert_to_postgres(df, epoch_id):
    """
    Writes a micro-batch to PostgreSQL using an UPSERT logic.
    This function handles duplicates by updating existing rows.
    """
    # Deduplicate within the micro-batch itself
    df_upsert = df.dropDuplicates(["id"])
    df_upsert.persist()
    print(f"Processing batch {epoch_id} with {df_upsert.count()} records.")

    def write_partition(partition):
        import psycopg2
        db_params = {
            "dbname": PG_DB,
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "host": PG_HOST
        }
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        sql = """
            INSERT INTO youtube_trending (id, kind, etag, "publishedAt", "channelId", title, description, "channelTitle", "viewCount", "likeCount", "commentCount")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                etag = EXCLUDED.etag,
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                "viewCount" = EXCLUDED."viewCount",
                "likeCount" = EXCLUDED."likeCount",
                "commentCount" = EXCLUDED."commentCount";
        """
        for row in partition:
            cursor.execute(sql, tuple(row))
        conn.commit()
        cursor.close()
        conn.close()

    cols_in_order = ["id", "kind", "etag", "publishedAt", "channelId", "title", "description", "channelTitle", "viewCount", "likeCount", "commentCount"]
    df_upsert.select(cols_in_order).foreachPartition(write_partition)
    df_upsert.unpersist()

query = df_final.writeStream \
    .foreachBatch(upsert_to_postgres) \
    .option("checkpointLocation", "/tmp/spark_checkpoints/youtube_trending") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()
print("Streaming batch completed.")
