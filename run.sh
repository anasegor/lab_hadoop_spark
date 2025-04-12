#!/bin/bash

set -e 
set -o pipefail

log() {
    echo -e "\033[1;34m[INFO]\033[0m $1"
}

error() {
    echo -e "\033[1;31m[ERROR]\033[0m $1"
    exit 1
}

log "Копируем spotify-tracks-dataset.csv в контейнер namenode..."
docker cp spotify-tracks-dataset.csv namenode:/ || error "Не удалось скопировать CSV в namenode"

log "Создаём директорию в HDFS и загружаем туда CSV..."
docker exec namenode hdfs dfs -mkdir -p /data || error "Не удалось создать директорию в HDFS"
docker exec namenode hdfs dfs -put -f spotify-tracks-dataset.csv /data || error "Не удалось загрузить CSV в HDFS"

log "Устанавливаем зависимости в spark-worker-1..."
docker exec spark-worker-1 apk add --no-cache make automake gcc g++ python3-dev linux-headers || error "Не удалось установить зависимости на spark-worker-1"

log "Копируем spark_app.py в spark-master..."
docker cp spark_app.py spark-master:/tmp/spark_app.py || error "Не удалось скопировать spark_app.py в spark-master"

log "Устанавливаем зависимости в spark-master..."
docker exec spark-master apk add --no-cache make automake gcc g++ python3-dev linux-headers py3-pip && \
docker exec spark-master python3 -m pip install --upgrade pip setuptools wheel && \
pip3 install psutil || error "Не удалось установить зависимости в spark-master"

log "Запускаем Spark-приложение..."
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /tmp/spark_app.py True && \
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /tmp/spark_app.py False || error "Ошибка при выполнении spark-submit"

log "Скрипт завершён успешно!"
