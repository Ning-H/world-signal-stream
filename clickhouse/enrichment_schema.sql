CREATE DATABASE IF NOT EXISTS opensignal;

CREATE TABLE IF NOT EXISTS opensignal.events_enriched
(
    event_id String,
    source LowCardinality(String),
    source_subtype LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    title String,
    url Nullable(String),
    category LowCardinality(String),
    sentiment LowCardinality(String),
    geography Array(String),
    entities Array(String),
    confidence Float32,
    summary String,
    model String,
    prompt_version String,
    enriched_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(enriched_at)
PARTITION BY toDate(timestamp)
ORDER BY (source, timestamp, event_id);

CREATE TABLE IF NOT EXISTS opensignal.enrichment_costs
(
    run_id String,
    provider LowCardinality(String),
    model String,
    input_tokens UInt64,
    output_tokens UInt64,
    estimated_cost_usd Float64,
    events_enriched UInt64,
    recorded_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toDate(recorded_at)
ORDER BY (recorded_at, provider, model, run_id);

CREATE TABLE IF NOT EXISTS opensignal.enrichment_dlq
(
    event_id String,
    source LowCardinality(String),
    reason String,
    error String,
    payload String,
    recorded_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toDate(recorded_at)
ORDER BY (recorded_at, source, event_id);

CREATE TABLE IF NOT EXISTS opensignal.category_volume_5m
(
    bucket DateTime('UTC'),
    category LowCardinality(String),
    sentiment LowCardinality(String),
    source LowCardinality(String),
    event_count UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (bucket, category, sentiment, source);

CREATE MATERIALIZED VIEW IF NOT EXISTS opensignal.mv_category_volume_5m
TO opensignal.category_volume_5m
AS
SELECT
    toStartOfFiveMinutes(timestamp) AS bucket,
    category,
    sentiment,
    source,
    count() AS event_count
FROM opensignal.events_enriched
GROUP BY
    bucket,
    category,
    sentiment,
    source;

CREATE TABLE IF NOT EXISTS opensignal.sentiment_by_geo_15m
(
    bucket DateTime('UTC'),
    geography String,
    category LowCardinality(String),
    sentiment LowCardinality(String),
    event_count UInt64,
    sentiment_score_sum Int64
)
ENGINE = SummingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (bucket, geography, category, sentiment);

CREATE MATERIALIZED VIEW IF NOT EXISTS opensignal.mv_sentiment_by_geo_15m
TO opensignal.sentiment_by_geo_15m
AS
SELECT
    toStartOfInterval(timestamp, INTERVAL 15 MINUTE) AS bucket,
    geography,
    category,
    sentiment,
    count() AS event_count,
    sum(multiIf(
        sentiment IN ('positive', 'celebratory'), 1,
        sentiment IN ('negative', 'alarming'), -1,
        0
    )) AS sentiment_score_sum
FROM opensignal.events_enriched
ARRAY JOIN geography
GROUP BY
    bucket,
    geography,
    category,
    sentiment;

CREATE TABLE IF NOT EXISTS opensignal.top_entities_1h
(
    bucket DateTime('UTC'),
    entity String,
    category LowCardinality(String),
    source LowCardinality(String),
    event_count UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (bucket, entity, category, source);

CREATE MATERIALIZED VIEW IF NOT EXISTS opensignal.mv_top_entities_1h
TO opensignal.top_entities_1h
AS
SELECT
    toStartOfHour(timestamp) AS bucket,
    entity,
    category,
    source,
    count() AS event_count
FROM opensignal.events_enriched
ARRAY JOIN entities AS entity
WHERE entity != ''
GROUP BY
    bucket,
    entity,
    category,
    source;

CREATE TABLE IF NOT EXISTS opensignal.cross_source_topic_overlap_1h
(
    bucket DateTime('UTC'),
    topic_key String,
    top_entities Array(String),
    wikipedia_events UInt64,
    gdelt_events UInt64,
    hackernews_events UInt64,
    total_events UInt64,
    first_seen DateTime64(3, 'UTC'),
    last_seen DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(last_seen)
PARTITION BY toDate(bucket)
ORDER BY (bucket, topic_key);

CREATE TABLE IF NOT EXISTS opensignal.topic_clusters
(
    topic_id String,
    window_start DateTime('UTC'),
    window_end DateTime('UTC'),
    top_entities Array(String),
    category LowCardinality(String),
    sentiment LowCardinality(String),
    wikipedia_events UInt64,
    gdelt_events UInt64,
    hackernews_events UInt64,
    total_events UInt64,
    first_seen DateTime64(3, 'UTC'),
    last_seen DateTime64(3, 'UTC'),
    sample_titles Array(String),
    computed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toDate(window_start)
ORDER BY (window_start, topic_id);

INSERT INTO opensignal.category_volume_5m
SELECT
    toStartOfFiveMinutes(timestamp) AS bucket,
    category,
    sentiment,
    source,
    count() AS event_count
FROM opensignal.events_enriched
WHERE (bucket, category, sentiment, source) NOT IN
(
    SELECT bucket, category, sentiment, source
    FROM opensignal.category_volume_5m
)
GROUP BY
    bucket,
    category,
    sentiment,
    source;

INSERT INTO opensignal.sentiment_by_geo_15m
SELECT
    toStartOfInterval(timestamp, INTERVAL 15 MINUTE) AS bucket,
    geography,
    category,
    sentiment,
    count() AS event_count,
    sum(multiIf(
        sentiment IN ('positive', 'celebratory'), 1,
        sentiment IN ('negative', 'alarming'), -1,
        0
    )) AS sentiment_score_sum
FROM opensignal.events_enriched
ARRAY JOIN geography
WHERE (bucket, geography, category, sentiment) NOT IN
(
    SELECT bucket, geography, category, sentiment
    FROM opensignal.sentiment_by_geo_15m
)
GROUP BY
    bucket,
    geography,
    category,
    sentiment;

INSERT INTO opensignal.top_entities_1h
SELECT
    toStartOfHour(timestamp) AS bucket,
    entity,
    category,
    source,
    count() AS event_count
FROM opensignal.events_enriched
ARRAY JOIN entities AS entity
WHERE entity != ''
  AND (bucket, entity, category, source) NOT IN
(
    SELECT bucket, entity, category, source
    FROM opensignal.top_entities_1h
)
GROUP BY
    bucket,
    entity,
    category,
    source;
