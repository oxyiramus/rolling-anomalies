CREATE TABLE train_locations (
    train_number UInt32,
    timestamp DateTime,
    departure_date Date,
    lat Float64,
    lon Float64,
    speed UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (train_number, timestamp)
TTL timestamp + INTERVAL 720 DAY DELETE;