"""
Storage Server
---------------
Vai trò: KAFKA CONSUMER

Nhận kết quả nhận diện (bounding box + số người) từ topic `detection-results`
và lưu vào MongoDB để truy vấn/thống kê sau này.
"""

import os
import json
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
RESULT_TOPIC = os.getenv("RESULT_TOPIC", "detection-results")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
DB_NAME = os.getenv("DB_NAME", "people_counting")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "detections")


def connect_mongo() -> MongoClient:
    while True:
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
            client.admin.command("ping")
            print(f"[storage-server] Connected to MongoDB at {MONGO_URI}")
            return client
        except ServerSelectionTimeoutError:
            print(f"[storage-server] MongoDB not ready at {MONGO_URI}, retrying in 5s...")
            time.sleep(5)


def connect_kafka() -> KafkaConsumer:
    while True:
        try:
            consumer = KafkaConsumer(
                RESULT_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="storage-group",
            )
            print(f"[storage-server] Connected to Kafka broker at {KAFKA_BROKER}")
            return consumer
        except NoBrokersAvailable:
            print(f"[storage-server] Kafka not ready at {KAFKA_BROKER}, retrying in 5s...")
            time.sleep(5)


def main():
    client = connect_mongo()
    collection = client[DB_NAME][COLLECTION_NAME]
    print(f"[storage-server] Sẽ lưu dữ liệu vào database '{DB_NAME}', collection '{COLLECTION_NAME}'")

    consumer = connect_kafka()
    print(f"[storage-server] Đang lắng nghe topic '{RESULT_TOPIC}'...")

    for message in consumer:
        doc = message.value
        collection.insert_one(doc)
        print(f"[storage-server] Đã lưu frame_id={doc.get('frame_id')} -> "
              f"person_count={doc.get('person_count')}")


if __name__ == "__main__":
    main()
