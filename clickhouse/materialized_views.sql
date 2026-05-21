CREATE DATABASE IF NOT EXISTS opensignal;

CREATE TABLE IF NOT EXISTS opensignal.source_volume_5m
(
    bucket DateTime('UTC'),
    source LowCardinality(String),
    event_count UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (bucket, source);

CREATE TABLE IF NOT EXISTS opensignal.title_activity_5m
(
    bucket DateTime('UTC'),
    source LowCardinality(String),
    title String,
    event_count UInt64,
    magnitude_sum Int64
)
ENGINE = SummingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (bucket, source, title);

CREATE MATERIALIZED VIEW IF NOT EXISTS opensignal.mv_source_volume_5m
TO opensignal.source_volume_5m
AS
SELECT
    toStartOfFiveMinutes(timestamp) AS bucket,
    source,
    count() AS event_count
FROM opensignal.events_raw
GROUP BY
    bucket,
    source;

CREATE MATERIALIZED VIEW IF NOT EXISTS opensignal.mv_title_activity_5m
TO opensignal.title_activity_5m
AS
SELECT
    toStartOfFiveMinutes(timestamp) AS bucket,
    source,
    title,
    count() AS event_count,
    sum(coalesce(magnitude, 0)) AS magnitude_sum
FROM opensignal.events_raw
GROUP BY
    bucket,
    source,
    title;

INSERT INTO opensignal.source_volume_5m
SELECT
    toStartOfFiveMinutes(timestamp) AS bucket,
    source,
    count() AS event_count
FROM opensignal.events_raw
WHERE (bucket, source) NOT IN
(
    SELECT bucket, source
    FROM opensignal.source_volume_5m
)
GROUP BY
    bucket,
    source;

INSERT INTO opensignal.title_activity_5m
SELECT
    toStartOfFiveMinutes(timestamp) AS bucket,
    source,
    title,
    count() AS event_count,
    sum(coalesce(magnitude, 0)) AS magnitude_sum
FROM opensignal.events_raw
WHERE (bucket, source, title) NOT IN
(
    SELECT bucket, source, title
    FROM opensignal.title_activity_5m
)
GROUP BY
    bucket,
    source,
    title;

