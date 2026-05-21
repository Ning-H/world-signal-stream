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
