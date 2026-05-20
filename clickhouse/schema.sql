CREATE DATABASE IF NOT EXISTS opensignal;

CREATE TABLE IF NOT EXISTS opensignal.events_raw
(
    event_id String,
    source LowCardinality(String),
    source_subtype LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC'),
    language Nullable(String),
    geography_hint Nullable(String),
    title String,
    url Nullable(String),
    actor Nullable(String),
    is_bot Bool,
    magnitude Nullable(Int64),
    raw_json String
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (source, timestamp, event_id);

CREATE TABLE IF NOT EXISTS opensignal.events_raw_kafka
(
    message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'events.raw',
    kafka_group_name = 'clickhouse-events-raw',
    kafka_format = 'RawBLOB',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS opensignal.events_raw_mv
TO opensignal.events_raw
AS
SELECT
    JSONExtractString(message, 'event_id') AS event_id,
    JSONExtractString(message, 'source') AS source,
    JSONExtractString(message, 'source_subtype') AS source_subtype,
    parseDateTime64BestEffort(JSONExtractString(message, 'timestamp'), 3, 'UTC') AS timestamp,
    parseDateTime64BestEffort(JSONExtractString(message, 'ingested_at'), 3, 'UTC') AS ingested_at,
    nullIf(JSONExtractString(message, 'language'), '') AS language,
    nullIf(JSONExtractString(message, 'geography_hint'), '') AS geography_hint,
    JSONExtractString(message, 'title') AS title,
    nullIf(JSONExtractString(message, 'url'), '') AS url,
    nullIf(JSONExtractString(message, 'actor'), '') AS actor,
    JSONExtractBool(message, 'is_bot') AS is_bot,
    JSONExtract(message, 'magnitude', 'Nullable(Int64)') AS magnitude,
    JSONExtractRaw(message, 'raw') AS raw_json
FROM opensignal.events_raw_kafka
WHERE JSONExtractString(message, 'source') = 'wikipedia';

