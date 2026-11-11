## Локальный запуск

Копирование проекта:
```bash
git clone git@github.com:oxyiramus/rolling-anomalies.git
cd rolling-anomalies
```

Правка переменных окружения:
```bash
cp kafka/.env.example kafka/.env
cp producer/.env.example producer/.env
cp clickhouse/.env.example clickhouse/.env
```
При необходимости сконфигурируйте окружение, как вам нужно, через файл переменных.

Генерация CLUSTER_ID перед первым запуском Kafka:
```bash
echo "CLUSTER_ID=$(docker run --rm confluentinc/cp-kafka kafka-storage random-uuid)" >> kafka/.env
```

Запуск контейнеров:
```bash
docker compose up -d
```

Создание топиков:
```bash
docker compose exec kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --topic train-locations --partitions 3 --replication-factor 1
```

Применение миграций ClickHouse:
```bash
docker run --rm \
  --network rolling-anomalies_default \
  --env-file clickhouse/.env \
  -v "$(pwd)/clickhouse/migrations:/scripts" \
  mikeamputer/ch-migrate:latest up
```
