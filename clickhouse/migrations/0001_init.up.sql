CREATE TABLE kafka_train_locations
(
    trainNumber UInt32,
    departureDate Date,
    version String,
    trainLocations String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'train-locations',
    kafka_group_name = 'clickhouse-work-group',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536;

CREATE TABLE raw_train_locations (
    train_number UInt32,
    timestamp DateTime,
    departure_date Date,
    lat Nullable(Float64),
    lon Nullable(Float64),
    speed UInt32,
    version String
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (train_number, timestamp)
TTL timestamp + INTERVAL 730 DAY DELETE;

CREATE MATERIALIZED VIEW kafka_to_raw_train_locations_mv
TO raw_train_locations
AS SELECT
    trainNumber AS train_number,
    parseDateTimeBestEffort(JSONExtractString(loc, 'timestamp')) AS timestamp,
    departureDate AS departure_date,
    if(
        length(JSONExtractArrayRaw(loc, 'location')) > 0,
        JSONExtractFloat(loc, 'location', 2),
        NULL
    ) AS lat,
    if(
        length(JSONExtractArrayRaw(loc, 'location')) > 0,
        JSONExtractFloat(loc, 'location', 1),
        NULL
    ) AS lon,
    JSONExtractUInt(loc, 'speed') AS speed,
    version
FROM kafka_train_locations
ARRAY JOIN JSONExtractArrayRaw(trainLocations) AS loc;