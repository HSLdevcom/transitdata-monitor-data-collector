FROM python:3.8-slim

WORKDIR /app

COPY src /app
COPY requirements.txt .

RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-requests

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 80

CMD ["python3", "/app/mqtt_data_collector.py"]