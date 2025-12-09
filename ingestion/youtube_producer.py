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

producer = create_producer()

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

if __name__ == "__main__":
    logging.info("YouTube Producer started.")
    while True:
        videos = fetch_trending_videos()
        for video in videos:
            producer.send(TOPIC, video)
        producer.flush()
        logging.info("Sent %d videos to Kafka", len(videos))
        time.sleep(300)  # every 5 min
