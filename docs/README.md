# Rail Anomaly Detection

**Детекция аномалий в реальном времени на данных железных дорог Финляндии (Digitraffic) — Kafka → ClickHouse → dbt → Metabase/Grafana**

## Описание

Пайплайн обработки потоковых данных железнодорожных перевозок из открытого API Digitraffic (Финляндия) с детекцией аномалий в режиме, близком к реальному времени. Проект демонстрирует полный цикл: от сырых событий из внешнего API до моделированных витрин в ClickHouse и мониторинга в Grafana.

## Источник данных

**Digitraffic (железные дороги Финляндии)** — GraphQL/REST API с живыми данными о поездах: `trainNumber`, `departureDate`, `location`, `speed`, `timestamp`, а также расписания и отклонения от них. Real-time фид обновляется каждые **10 секунд**. Открытая лицензия Creative Commons, ключ не требуется.

## Архитектура

```
Источник (Digitraffic API, polling каждые 15 сек)
        ↓
Python Producer
        ↓
Kafka topic: "transport_events"
(partitions: 3, key: train_number)
        ↓
┌───────────────────┬───────────────────┬───────────────────┐
↓                   ↓                   ↓                   ↓
Consumer 1:        Consumer 2:        Consumer 3:        Consumer 4:
dedup              windowing          anomaly            persistence
(train_number,     (5-min agg)        detection          (ClickHouse)
 event_time)
        ↓               ↓                 ↓                   ↓
        └───────────────┴─────────────────┴───────────────────┘
                                    ↓
                    ClickHouse (MergeTree, партиции по дате)
                                    ↓
                    ┌───────────────┴───────────────┐
                    ↓                               ↓
                    Airflow (batch)           dbt (staging → marts)
                    ├── dbt_run                     ↓
                    ├── hourly_baselines       витрины в ClickHouse
                    └── pipeline_health             ↓
                            ↓                 ┌─────┴─────┐
                            ↓                 ↓           ↓
                            └─────────────→ Grafana    Metabase
                                           (health)    (analytics)
```

## Компоненты

**Ingestion.** Python-продюсер опрашивает API GraphQL-запросом (`currentlyRunningTrains`), парсит `trainLocations` и пишет в Kafka с ключом `train_number` — все события одного поезда попадают в одну партицию, порядок сохраняется.

**Дедупликация.** Digitraffic может возвращать дубли между циклами опроса. Консьюмер дедуплицирует по `(train_number, event_time)` или хешу payload перед записью в ClickHouse.

**Windowing (5-минутные агрегаты).** Консьюмер группирует события по `train_number` в скользящие окна, считает количество событий и среднюю скорость. Это основа для детекции аномалий.

**Детекция аномалий.** Для Digitraffic: отклонение `speed` от расписания, «поезд стоит дольше baseline», опоздание > 5 минут.

**Оркестрация (Airflow).** DAG `dbt_run` (каждые 15 мин: `dbt run` + `dbt test`), `hourly_baselines` (пересчёт baseline по маршрутам), `pipeline_health` (freshness, consumer lag, алерты). `SqlSensor` ждёт новые данные перед запуском dbt.

**Хранение (ClickHouse).** Таблицы:
- `transport_events` — сырые события, MergeTree, партиции по `toYYYYMMDD(event_time)`, `ORDER BY (train_number, event_time)`
- `event_aggregates_5min` — оконные агрегаты
- `anomalies` — детектированные аномалии с JSON-деталями

**Трансформации (dbt).** Staging (нормализация) → intermediate (траектории, baseline по маршрутам/времени) → marts (топ аномалий, карта отклонений, статистика по типам аномалий). Тесты: `not_null` на `train_number`, `unique` на `(train_number, event_time)`, `accepted_values` на тип аномалии.

**Визуализация.** Grafana: freshness (лаг между `event_time` и временем записи в ClickHouse), throughput (событий/сек), consumer lag по партициям, количество активных аномалий. Metabase: топ аномальных поездов, распределение по часам, карта отклонений.

## Что демонстрирует

- **Python** — продюсер и консьюмеры: polling внешнего API, парсинг ответов, запись в Kafka, обработка окон и дедупликации, запись в ClickHouse. Асинхронность и обработка ошибок (retry, DLQ).
- **GraphQL** — запросы к Digitraffic API (`currentlyRunningTrains`), работа с вложенной структурой ответа, пагинация и фильтрация на стороне API.
- **Airflow** — DAG-архитектура, сенсоры (`SqlSensor` — ожидание новых данных), retry, XCom для передачи метаданных между тасками, оркестрация dbt и батч-агрегаций (baseline, health-check).
- **Kafka** — партиционирование по ключу (`train_number`), consumer groups, идемпотентность (дедуп по `(train_number, event_time)`), мониторинг consumer lag.
- **ClickHouse** — MergeTree, партиционирование по дате (`toYYYYMMDD(event_time)`), `ORDER BY` под запросы, агрегации по большим объёмам, вставка батчами.
- **Дедупликация** — отсечение дублей между циклами polling по составному ключу или хешу payload; идемпотентная запись в ClickHouse.
- **Windowing** — 5-минутные скользящие агрегаты по `train_number` (количество событий, средняя скорость), baseline для детекции аномалий.
- **Детекция аномалий** — сравнение текущего окна с историческим baseline: отклонение скорости, простой дольше нормы, накопление задержки > 5 минут.
- **SQL** — сложные запросы для оконных агрегатов и витрин: CTE, `GROUP BY`, оконные функции, джойны траекторий с baseline.
- **dbt** — staging → intermediate → marts, инкрементальные модели (`is_incremental()`), 20+ data-тестов (`not_null`, `unique`, `accepted_values`), документация и lineage.
- **Observability** — freshness (лаг между `event_time` и записью в ClickHouse), throughput (событий/сек), consumer lag по партициям, anomaly rate, алерты при отклонениях.
- **Docker Compose** — 7 сервисов (Kafka, ClickHouse, producer, consumers, Airflow, Grafana, Metabase), healthchecks, volumes, сеть, секреты через env.
- **Grafana** — дашборды здоровья пайплайна: freshness, throughput, consumer lag, количество активных аномалий, алерты.
- **Metabase** — бизнес-витрины: топ аномальных поездов, распределение по часам, карта отклонений, динамика задержек.

## Roadmap

**Релиз 1 (MVP):** поток данных от источника до сырого хранилища. Продюсер, консьюмер, запись в ClickHouse. Проверка, что данные идут и сохраняются.

**Релиз 2:** дедупликация событий, оконные агрегаты, первые dbt-модели (staging и intermediate). Подключение Airflow для оркестрации dbt-запусков.

**Релиз 3:** детекция аномалий на основе baseline, витрины marts, дашборды в Grafana и Metabase. Расширение Airflow: пересчёт baseline, мониторинг здоровья пайплайна, ожидание новых данных перед запуском dbt.

**Релиз 4:** README, CI/CD (линт + тесты dbt), финализация Docker Compose со всеми сервисами, включая Airflow.
