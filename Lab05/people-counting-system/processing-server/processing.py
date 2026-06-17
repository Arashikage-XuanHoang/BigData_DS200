"""
Processing Server
------------------
Vai trò: KAFKA CONSUMER (đọc từ raw-frames) + KAFKA PRODUCER (gửi vào detection-results)

Nhận khung hình từ topic `raw-frames`, chạy mô hình YOLO để phát hiện người (class "person"),
trích bounding box, đếm số người trong khung hình, rồi gửi kết quả sang topic `detection-results`
để Storage Server lưu vào MongoDB.
"""

import os
import json
import time
import base64

import numpy as np
import cv2
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from ultralytics import YOLO

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "raw-frames")
RESULT_TOPIC = os.getenv("RESULT_TOPIC", "detection-results")
MODEL_PATH = os.getenv("MODEL_PATH", "yolov8n.pt")
CONF_THRESHOLD = float(os.getenv("CONF_THRESHOLD", "0.4"))


def connect_kafka():
    """Kết nối consumer + producer tới Kafka, retry nếu broker chưa sẵn sàng."""
    while True:
        try:
            consumer = KafkaConsumer(
                RAW_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="processing-group",
            )
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"[processing-server] Connected to Kafka broker at {KAFKA_BROKER}")
            return consumer, producer
        except NoBrokersAvailable:
            print(f"[processing-server] Kafka not ready at {KAFKA_BROKER}, retrying in 5s...")
            time.sleep(5)


def decode_frame(image_data_b64: str):
    img_bytes = base64.b64decode(image_data_b64)
    np_arr = np.frombuffer(img_bytes, dtype=np.uint8)
    return cv2.imdecode(np_arr, cv2.IMREAD_COLOR)


def detect_persons(model: YOLO, frame) -> list:
    """Chạy YOLO trên frame, trả về list bounding box của riêng class 'person'."""
    results = model(frame, verbose=False)[0]
    detections = []

    for box in results.boxes:
        cls_id = int(box.cls[0])
        cls_name = model.names[cls_id]
        conf = float(box.conf[0])

        if cls_name == "person" and conf >= CONF_THRESHOLD:
            x1, y1, x2, y2 = [round(float(c), 2) for c in box.xyxy[0]]
            detections.append({
                "bbox": [x1, y1, x2, y2],
                "confidence": round(conf, 4),
                "class": cls_name,
            })

    return detections


def main():
    print("[processing-server] Đang load YOLO model...")
    model = YOLO(MODEL_PATH)
    print("[processing-server] Model đã sẵn sàng. Đang kết nối Kafka...")

    consumer, producer = connect_kafka()
    print(f"[processing-server] Đang lắng nghe topic '{RAW_TOPIC}'...")

    for message in consumer:
        data = message.value
        frame_id = data.get("frame_id")
        camera_id = data.get("camera_id")
        timestamp = data.get("timestamp")

        frame = decode_frame(data["image_data"])
        if frame is None:
            print(f"[processing-server] Không decode được frame {frame_id}, bỏ qua.")
            continue

        detections = detect_persons(model, frame)

        result_message = {
            "frame_id": frame_id,
            "camera_id": camera_id,
            "timestamp": timestamp,
            "person_count": len(detections),
            "detections": detections,
        }

        producer.send(RESULT_TOPIC, value=result_message)
        producer.flush()

        print(f"[processing-server] frame_id={frame_id} -> "
              f"{len(detections)} người được phát hiện -> gửi tới '{RESULT_TOPIC}'")


if __name__ == "__main__":
    main()
