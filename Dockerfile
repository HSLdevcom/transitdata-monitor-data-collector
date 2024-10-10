FROM python:3.8-slim

WORKDIR /app

COPY src /app
COPY requirements.txt .

RUN apt-get update && apt-get install -y \
    python3-pip

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 80

CMD ["/usr/local/bin/python3", "/app/gtfsrt_data_collector.py"]