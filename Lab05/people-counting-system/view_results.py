"""
Script tiện ích: kết nối tới MongoDB (port đã được expose ra host: 27017)
và in ra toàn bộ kết quả đếm người đã được lưu, để kiểm tra/chụp màn hình nộp bài.

Chạy: python view_results.py
(cần: pip install pymongo)
"""

from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "people_counting"
COLLECTION_NAME = "detections"


def main():
    client = MongoClient(MONGO_URI)
    collection = client[DB_NAME][COLLECTION_NAME]

    docs = list(collection.find().sort("timestamp", -1))
    print(f"Tổng số bản ghi: {len(docs)}\n")

    for doc in docs:
        print(f"frame_id      : {doc.get('frame_id')}")
        print(f"camera_id     : {doc.get('camera_id')}")
        print(f"timestamp     : {doc.get('timestamp')}")
        print(f"person_count  : {doc.get('person_count')}")
        for i, det in enumerate(doc.get("detections", []), 1):
            print(f"  - person {i}: bbox={det['bbox']}, confidence={det['confidence']}")
        print("-" * 50)


if __name__ == "__main__":
    main()
