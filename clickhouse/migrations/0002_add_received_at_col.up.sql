ALTER TABLE raw_train_locations
ADD COLUMN received_at DateTime DEFAULT now();

CREATE OR REPLACE MATERIALIZED VIEW kafka_to_raw_train_locations_mv
TO raw_train_locations
AS SELECT
    trainNumber AS train_number,
    parseDateTimeBestEffort(JSONExtractString(loc, 'timestamp')) AS timestamp,
    now() AS received_at,
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