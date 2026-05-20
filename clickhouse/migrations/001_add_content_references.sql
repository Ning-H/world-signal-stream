ALTER TABLE opensignal.events_raw
    ADD COLUMN IF NOT EXISTS content_id Nullable(String) AFTER magnitude,
    ADD COLUMN IF NOT EXISTS parent_content_id Nullable(String) AFTER content_id,
    ADD COLUMN IF NOT EXISTS content_url Nullable(String) AFTER parent_content_id,
    ADD COLUMN IF NOT EXISTS content_hint Nullable(String) AFTER content_url;

DROP VIEW IF EXISTS opensignal.events_raw_mv;

CREATE MATERIALIZED VIEW opensignal.events_raw_mv
TO opensignal.events_raw
AS
WITH
    JSONExtractRaw(message, 'raw') AS raw_payload,
    JSONExtractRaw(raw_payload, 'revision') AS revision_payload
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
    coalesce(
        nullIf(JSONExtractString(message, 'content_id'), ''),
        toString(JSONExtract(revision_payload, 'new', 'Nullable(UInt64)'))
    ) AS content_id,
    coalesce(
        nullIf(JSONExtractString(message, 'parent_content_id'), ''),
        toString(JSONExtract(revision_payload, 'old', 'Nullable(UInt64)'))
    ) AS parent_content_id,
    coalesce(
        nullIf(JSONExtractString(message, 'content_url'), ''),
        nullIf(JSONExtractString(raw_payload, 'notify_url'), '')
    ) AS content_url,
    coalesce(
        nullIf(JSONExtractString(message, 'content_hint'), ''),
        nullIf(JSONExtractString(raw_payload, 'comment'), '')
    ) AS content_hint,
    raw_payload AS raw_json
FROM opensignal.events_raw_kafka
WHERE JSONExtractString(message, 'source') = 'wikipedia';
