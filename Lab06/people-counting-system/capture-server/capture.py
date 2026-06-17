"""
Capture Server
---------------
Vai trò: KAFKA PRODUCER
Đọc ảnh từ thư mục local (giả lập khung hình từ camera), encode base64,
đóng gói metadata và gửi lên Kafka topic `raw-frames` để Processing Server xử lý.
"""

import os
import json
import time
import base64
import glob
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "raw-frames")
IMAGE_DIR = os.getenv("IMAGE_DIR", "/app/sample-images")
CAMERA_ID = os.getenv("CAMERA_ID", "cam01")
LOOP = os.getenv("LOOP", "true").lower() == "true"
INTERVAL_SECONDS = float(os.getenv("INTERVAL_SECONDS", "10"))


def connect_producer() -> KafkaProducer:
    """Kết nối tới Kafka broker, retry nếu broker chưa sẵn sàng."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"[capture-server] Connected to Kafka broker at {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            print(f"[capture-server] Kafka not ready at {KAFKA_BROKER}, retrying in 5s...")
            time.sleep(5)


def load_images(image_dir: str) -> list:
    paths = []
    for ext in ("*.jpg", "*.jpeg", "*.png"):
        paths.extend(glob.glob(os.path.join(image_dir, ext)))
    return sorted(paths)


def send_frame(producer: KafkaProducer, image_path: str) -> None:
    with open(image_path, "rb") as f:
        img_bytes = f.read()

    message = {
        "frame_id": str(uuid.uuid4()),
        "camera_id": CAMERA_ID,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "image_format": os.path.splitext(image_path)[1].lstrip(".").lower(),
        "image_data": base64.b64encode(img_bytes).decode("utf-8"),
    }

    producer.send(RAW_TOPIC, value=message)
    producer.flush()
    print(f"[capture-server] Sent frame_id={message['frame_id']} "
          f"file={os.path.basename(image_path)} -> topic '{RAW_TOPIC}'")


def main():
    image_paths = load_images(IMAGE_DIR)
    if not image_paths:
        print(f"[capture-server] Không tìm thấy ảnh nào trong {IMAGE_DIR}. "
              f"Hãy thêm file .jpg/.jpeg/.png vào thư mục sample-images.")
        return

    print(f"[capture-server] Tìm thấy {len(image_paths)} ảnh. Đang kết nối Kafka...")
    producer = connect_producer()

    while True:
        for path in image_paths:
            send_frame(producer, path)
            time.sleep(2)  # giả lập khoảng cách giữa các khung hình

        if not LOOP:
            print("[capture-server] Đã gửi hết ảnh, LOOP=false -> kết thúc.")
            break

        print(f"[capture-server] Đợi {INTERVAL_SECONDS}s trước khi gửi lại "
              f"(giả lập luồng camera liên tục)...")
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
