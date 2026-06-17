# Hệ thống đếm số lượng người trong camera (Big Data Lab)

Hệ thống mô phỏng pipeline xử lý dữ liệu phân tán theo kiến trúc streaming, gồm 3 server
giao tiếp với nhau qua **Apache Kafka** (message broker) và lưu trữ kết quả vào **MongoDB**.

## 1. Kiến trúc hệ thống

```
                  Kafka topic                         Kafka topic
                 "raw-frames"                     "detection-results"
                       |                                    |
[Capture Server] ----->|-----> [Processing Server] -------->|-----> [Storage Server]
   (Producer)          |        (Consumer + Producer)        |          (Consumer)
                        |          YOLOv8n - "person"         |              |
                                                                              v
                                                                          MongoDB
```

| Server | Vai trò Kafka | Nhiệm vụ |
|---|---|---|
| **capture-server** | Producer | Đọc ảnh (giả lập khung hình camera) từ thư mục `sample-images/`, gửi lên topic `raw-frames` |
| **processing-server** | Consumer + Producer | Nhận khung hình, chạy YOLOv8n để phát hiện người, đếm số người + bounding box, gửi kết quả lên topic `detection-results` |
| **storage-server** | Consumer | Nhận kết quả, lưu vào MongoDB (collection `detections`) |

**Công nghệ Big Data sử dụng:**
- **Apache Kafka** (chế độ KRaft, không cần Zookeeper) — message broker, tách rời 3 service, mỗi service có thể scale độc lập, chịu được trường hợp consumer xử lý chậm hơn producer (backpressure).
- **MongoDB** — NoSQL document store, phù hợp lưu dữ liệu JSON bán cấu trúc (bounding box có số lượng phần tử thay đổi theo từng frame).
- **YOLOv8 (Ultralytics)** — mô hình object detection cho bước xử lý.

> Bài tập: input demo chỉ gồm 1-2 ảnh tĩnh để chạy nhanh trên máy cấu hình thấp, nhưng kiến trúc
> (Kafka streaming + tách 3 service độc lập) hoàn toàn tổng quát hóa được cho luồng video/camera
> thực tế với throughput lớn.

## 2. Cấu trúc project

```
people-counting-system/
├── docker-compose.yml
├── README.md
├── view_results.py          # script xem kết quả đã lưu trong MongoDB
├── sample-images/           # ảnh test (capture-server đọc từ đây)
│   ├── sample_01.jpg
│   └── sample_02.jpg
├── capture-server/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── capture.py
├── processing-server/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── processing.py
└── storage-server/
    ├── Dockerfile
    ├── requirements.txt
    └── storage.py
```

## 3. Cách chạy

**Yêu cầu:** Docker + Docker Compose, máy có kết nối internet (lần đầu cần tải image Docker và
tải weight model `yolov8n.pt` ~6MB).

```bash
# Build và chạy toàn bộ hệ thống
docker compose up --build

# Chạy ngầm (detached)
docker compose up --build -d

# Xem log của từng server
docker compose logs -f capture-server
docker compose logs -f processing-server
docker compose logs -f storage-server

# Dừng hệ thống
docker compose down
```

Khi chạy sẽ thấy log dạng:

```
capture-server     | [capture-server] Sent frame_id=... file=sample_01.jpg -> topic 'raw-frames'
processing-server   | [processing-server] frame_id=... -> 2 người được phát hiện -> gửi tới 'detection-results'
storage-server      | [storage-server] Đã lưu frame_id=... -> person_count=2
```

## 4. Kiểm tra kết quả đã lưu trong MongoDB

Cách 1 — dùng script có sẵn (chạy ngoài máy host, không cần vào container):

```bash
pip install pymongo
python view_results.py
```

Cách 2 — dùng mongosh trực tiếp:

```bash
docker exec -it mongodb mongosh people_counting --eval "db.detections.find().pretty()"
```

## 5. Định dạng dữ liệu (message format)

**Topic `raw-frames`** (capture-server → processing-server):
```json
{
  "frame_id": "uuid",
  "camera_id": "cam01",
  "timestamp": "2026-06-17T01:00:00+00:00",
  "image_format": "jpg",
  "image_data": "<base64 string>"
}
```

**Topic `detection-results`** (processing-server → storage-server, cũng là document lưu MongoDB):
```json
{
  "frame_id": "uuid",
  "camera_id": "cam01",
  "timestamp": "2026-06-17T01:00:00+00:00",
  "person_count": 2,
  "detections": [
    {"bbox": [120.5, 80.2, 300.7, 420.1], "confidence": 0.91, "class": "person"}
  ]
}
```

## 6. Tùy chỉnh

Các biến môi trường có thể chỉnh trong `docker-compose.yml`:

- `IMAGE_DIR`, `CAMERA_ID`, `LOOP`, `INTERVAL_SECONDS` (capture-server)
- `MODEL_PATH`, `CONF_THRESHOLD` (processing-server) — đổi `yolov8n.pt` thành `yolov8s.pt`/`yolov8m.pt` nếu cần độ chính xác cao hơn (đổi lại tốc độ chậm hơn)
- `DB_NAME`, `COLLECTION_NAME` (storage-server)

Muốn dùng camera thật/RTSP thay vì ảnh tĩnh: sửa `capture.py` để đọc qua `cv2.VideoCapture(0)`
hoặc URL RTSP, encode mỗi frame bằng `cv2.imencode(".jpg", frame)` rồi gửi lên Kafka tương tự.



