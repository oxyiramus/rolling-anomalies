## Локальный запуск

Правка переменных окружения:
```bash
cp kafka/.env.example kafka/.env
```
При необходимости сконфигурируйте окружение, как вам нужно, через файл переменных.

Генерация CLUSTER_ID перед первым запуском Kafka:
```bash
echo "CLUSTER_ID=$(docker run --rm confluentinc/cp-kafka kafka-storage random-uuid)" >> kafka/.env
```

Создание топика:
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic my-topic --partitions 3 --replication-factor 1
```

Запуск контейнеров:
```bash
docker compose up -d
```
