[![AI Policy: Human Voice](https://img.shields.io/badge/AI_Policy-Human_Voice-blue?logo=data:image%2Fsvg%2Bxml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCIgd2lkdGg9IjI0IiBoZWlnaHQ9IjI0Ij4KICA8c3R5bGU%2BCiAgICAuaWNvbiB7CiAgICAgIGZpbGw6IG5vbmU7CiAgICAgIHN0cm9rZTogIzAwMDAwMDsKICAgICAgc3Ryb2tlLXdpZHRoOiAyLjI1OwogICAgICBzdHJva2UtbGluZWNhcDogcm91bmQ7CiAgICAgIHN0cm9rZS1saW5lam9pbjogcm91bmQ7CiAgICB9CiAgICBAbWVkaWEgKHByZWZlcnMtY29sb3Itc2NoZW1lOiBkYXJrKSB7CiAgICAgIC5pY29uIHsgc3Ryb2tlOiAjZmZmZmZmOyB9CiAgICB9CiAgPC9zdHlsZT4KICA8cGF0aCBjbGFzcz0iaWNvbiIgZD0iTTE5LjQxNCAxNC40MTRDMjEgMTIuODI4IDIyIDExLjUgMjIgOS41YTUuNSA1LjUgMCAwIDAtOS41OTEtMy42NzYuNi42IDAgMCAxLS44MTguMDAxQTUuNSA1LjUgMCAwIDAgMiA5LjVjMCAyLjMgMS41IDQgMyA1LjVsNS41MzUgNS4zNjJhMiAyIDAgMCAwIDIuODc5LjA1MiAyLjEyIDIuMTIgMCAwIDAtLjAwNC0zIDIuMTI0IDIuMTI0IDAgMSAwIDMtMyAyLjEyNCAyLjEyNCAwIDAgMCAzLjAwNCAwIDIgMiAwIDAgMCAwLTIuODI4bC0xLjg4MS0xLjg4MmEyLjQxIDIuNDEgMCAwIDAtMy40MDkgMGwtMS43MSAxLjcxYTIgMiAwIDAgMS0yLjgyOCAwIDIgMiAwIDAgMSAwLTIuODI4bDIuODIzLTIuNzYyIi8%2BCjwvc3ZnPgo%3D)](./AI_POLICY.md)

# Rail Anomaly Detection

**Детекция аномалий в реальном времени на данных железных дорог Финляндии (Digitraffic) — Kafka → ClickHouse → dbt → Metabase/Grafana**

## Описание

Пайплайн обработки потоковых данных железнодорожных перевозок из открытого API Digitraffic (Финляндия) с детекцией аномалий в режиме, близком к реальному времени (near-real-time). Проект демонстрирует полный цикл: от сырых событий из внешнего API до моделированных витрин в ClickHouse и мониторинга.

## Источник данных

**Digitraffic (железные дороги Финляндии)** — GraphQL/REST API с живыми данными о поездах: `trainNumber`, `departureDate`, `location`, `speed`, `timestamp`, а также расписания и отклонения от них. Real-time фид обновляется каждые 10 секунд. Открытая лицензия Creative Commons, ключ не требуется.

## Архитектура

Пайплайн построен по гибридной схеме (Stream + Micro-batch), чтобы избежать лишних перекладываний данных («спагетти») и обеспечить отказоустойчивость.

```
[API Digitraffic] ──(polling 15s)──> [Python Producer]
│
▼
[Kafka] <──topic "train-locations"── (key: train_number)
│
├──> [ClickHouse Kafka Engine] ──> [MV] ──> raw_locations (ReplacingMergeTree)
│
└──> [ClickHouse Kafka Engine] ──> [MV] ──> raw_train_info

(Далее — слой обогащения и аналитики)

[raw_locations] ──> [Python Geo-Enricher (Airflow DAG, батч раз в минуту)]
│ (R-tree матчинг к перегонам/станциям)
▼
[geo_enriched_locations] <── (JOIN) ──> [schedule_rows]
│ (Airflow DAG синхронизации)
│ ▼
└──────────────> [dbt: staging → intermediate → marts]
│
▼
[anomalies] (ClickHouse)
│
┌────────────┴────────────┐
▼ ▼
[Grafana] [Metabase]
(health/alerts) (analytics)
```

*Подробное описание потоков данных, обоснование выбора инструментов и обработка граничных случаев — в файле [ARCHITECTURE.md](ARCHITECTURE.md).*

## Ключевые компоненты

**Ingestion.** Python-продюсер опрашивает GraphQL API (`currentlyRunningTrains`), парсит `trainLocations` и пишет в Kafka с ключом `train_number` (сохраняется порядок событий одного поезда).

**Landing & Deduplication (ClickHouse).** Вместо Python-консьюмера используется нативный Kafka Engine в ClickHouse. Данные попадают в сырые таблицы (движок `ReplacingMergeTree`), который автоматически отсекает дубликаты по ключу.

**Geo-Enrichment (Python + Airflow).** Отдельный батч-процесс (раз в минуту) забирает новые точки из `raw_locations`, использует локальный пространственный индекс (R-tree на базе OSM) для определения перегона и станции, и пишет результат в `geo_enriched_locations`.

**Schedule Sync (Airflow).** DAG обновляет расписание поездов (с проверкой версии), складывая его в нормализованную таблицу `schedule_rows`. JOIN с расписанием (ASOF JOIN по времени) выполняется в dbt на этапе трансформаций.

**Windowing & Anomaly Detection (dbt + Airflow).** Расчёт baseline (медиана + MAD) вынесен в отдельный ночной DAG (`hourly_baselines`), так как это тяжелая операция. Детекция аномалий работает как микро-батч (раз в минуту) поверх дедуплицированных, обогащенных и связанных с расписанием данных.

**Визуализация.** Grafana: freshness, throughput, consumer lag, количество аномалий. Metabase: топ аномалий, распределение по часам и маршрутам.

## Что демонстрирует

- **Python** — продюсер (polling API), фоновый обогатитель (R-tree матчинг).
- **GraphQL** — работа с вложенной структурой ответа API.
- **Kafka** — партиционирование по ключу, топики как буфер между слоями.
- **ClickHouse** — нативный Kafka Engine (Materialized Views), движок `ReplacingMergeTree`, отказ от `FINAL` на горячем пути, партиционирование по дате, `ASOF JOIN`.
- **Airflow** — DAG-архитектура для батч-слоя (синхронизация расписания, пересчёт baseline, гео-обогащение).
- **dbt** — staging → intermediate → marts, оконные функции, тесты, документация.
- **Docker Compose** — сервисы (Kafka, ClickHouse, Producer, Airflow, Grafana, Metabase).
- **Observability** — мониторинг задержек (freshness), consumer lag, health-check.

## Roadmap

**Релиз 1 (MVP):** Продюсер → Kafka → ClickHouse (Kafka Engine + MV) → сырая таблица. Проверка, что данные идут и сохраняются.

**Релиз 2:** Подключение гео-матчинга (Python) к сырым данным. Настройка dbt для базовых трансформаций.

**Релиз 3:** DAG для синхронизации расписания, JOIN с расписанием в dbt. Расчёт baseline (DAG) и детекция аномалий.

**Релиз 4:** Визуализация в Grafana/Metabase, CI/CD, финализация Docker Compose и документации.