FROM python:3

COPY *.py requirements.txt /app/
WORKDIR /app

RUN ["pip3", "install", "-r", "requirements.txt"]