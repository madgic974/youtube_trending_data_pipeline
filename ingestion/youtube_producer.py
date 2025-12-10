import os
import json
import time
import requests
from kafka import KafkaProducer, errors
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()  # charge le fichier .env automatiquement

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = "youtube_trending"

def create_producer(retries=5, delay=5):
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            logging.info(f"Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except errors.NoBrokersAvailable:
            logging.warning(f"Kafka unavailable, retry {i+1}/{retries}")
            time.sleep(delay)
    raise Exception("Kafka not available after retries")

def fetch_trending_videos(region="FR", max_results=10):
    url = (
        f"https://www.googleapis.com/youtube/v3/videos?"
        f"part=snippet,statistics&chart=mostPopular"
        f"&regionCode={region}&maxResults={max_results}&key={YOUTUBE_API_KEY}"
    )
    try:
        res = requests.get(url)
        res.raise_for_status()
        return res.json().get("items", [])
    except Exception as e:
        logging.error("YouTube API error: %s", e)
        return []

def main():
    """
    Fonction principale pour récupérer les vidéos, les envoyer à Kafka, et se terminer.
    """
    logging.info("Starting YouTube data ingestion batch job.")
    producer = None
    try:
        producer = create_producer()
        videos = fetch_trending_videos()

        if not videos:
            logging.info("No new videos found to send. Exiting.")
            return

        logging.info(f"Found {len(videos)} videos to send to Kafka topic '{TOPIC}'.")
        for video in videos:
            producer.send(TOPIC, video)

        # Attendre que tous les messages soient envoyés avant de continuer
        producer.flush()
        logging.info(f"Successfully sent {len(videos)} videos to Kafka.")

    except Exception as e:
        logging.error(f"An error occurred during the ingestion job: {e}")
        raise  # Propage l'exception pour qu'Airflow marque la tâche comme échouée
    finally:
        if producer:
            producer.close()
            logging.info("Kafka producer closed.")
        logging.info("YouTube data ingestion batch job finished.")

if __name__ == "__main__":
    main()
