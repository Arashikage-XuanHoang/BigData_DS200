FROM python:3.11-slim

WORKDIR /app

# OpenCV cần các lib hệ thống này để chạy trong môi trường headless
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY processing.py .

CMD ["python", "processing.py"]
