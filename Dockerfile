FROM python:3.8-slim

WORKDIR /app

COPY src /app
COPY requirements.txt .
COPY crontab crontab

RUN apt-get update
RUN apt-get install -y cron

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 80

RUN crontab crontab

CMD ["cron", "-f"]