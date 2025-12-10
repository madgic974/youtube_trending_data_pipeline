import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """
    Fonction principale pour exécuter le job Spark en mode batch.
    """
    spark = SparkSession.builder.appName("YouTubeBatchTransform").getOrCreate()
    logging.info("Session Spark créée.")

    # --- Configuration ---
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKER", "kafka:9092")
    KAFKA_TOPIC = "youtube_trending"
    PG_HOST = "postgres"
    PG_DB = os.getenv("POSTGRES_DB", "youtube")
    PG_USER = os.getenv("POSTGRES_USER")
    PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")

    # --- Schéma pour les données Kafka ---
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

    # --- Lecture des données depuis Kafka en mode batch ---
    logging.info(f"Lecture depuis le topic Kafka : {KAFKA_TOPIC}")
    df_raw = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    # Vérifier si le DataFrame est vide
    if df_raw.rdd.isEmpty():
        logging.info("Aucune nouvelle donnée trouvée dans le topic Kafka. Fin du job.")
        spark.stop()
        return

    logging.info("Données lues depuis Kafka, début des transformations.")
    # --- Transformations ---
    df_parsed = df_raw.select(
        from_json(col("value").cast("string"), schema).alias("data"),
    ).select("data.*")

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

    # --- Écriture dans PostgreSQL avec une logique d'UPSERT ---
    df_upsert = df_final.dropDuplicates(["id"])
    
    record_count = df_upsert.count()
    logging.info(f"Traitement et insertion/mise à jour de {record_count} enregistrements dans PostgreSQL.")

    def write_partition(partition):
        import psycopg2
        db_params = {
            "dbname": PG_DB,
            "user": PG_USER,
            "password": PG_PASSWORD,
            "host": PG_HOST
        }
        conn = None
        try:
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
        except Exception as e:
            logging.error(f"Erreur lors de l'écriture de la partition sur PostgreSQL : {e}")
            if conn: conn.rollback()
            raise
        finally:
            if 'cursor' in locals() and cursor: cursor.close()
            if conn: conn.close()

    cols_in_order = ["id", "kind", "etag", "publishedAt", "channelId", "title", "description", "channelTitle", "viewCount", "likeCount", "commentCount"]
    df_upsert.select(cols_in_order).foreachPartition(write_partition)

    logging.info("Données insérées/mises à jour avec succès dans PostgreSQL.")
    
    spark.stop()
    logging.info("Job Spark terminé.")

if __name__ == "__main__":
    main()
