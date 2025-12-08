import os
import json
import time
import requests
from kafka import KafkaProducer, errors
from dotenv import load_dotenv

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
            print(f"[INFO] Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except errors.NoBrokersAvailable:
            print(f"[WARN] Kafka unavailable, retry {i+1}/{retries}")
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
        print("[ERROR] YouTube API error:", e)
        return []

if __name__ == "__main__":
    print("[INFO] YouTube Producer started.")
    while True:
        videos = fetch_trending_videos()
        for video in videos:
            producer.send(TOPIC, video)
        producer.flush()
        print(f"[INFO] Sent {len(videos)} videos to Kafka")
        time.sleep(300)  # every 5 min
