FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && apt install protobuf-compiler \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY ./app .

EXPOSE 8080

CMD ["python", "app/main.py"]
