# OpenSignal Architecture

## Target End State

```text
Open public signals
  ├─ Wikipedia recent changes
  ├─ GDELT global news events
  ├─ Reddit submissions
  └─ Bluesky Jetstream (optional)
        │
        ▼
Kafka topic: events.raw
        │
        ├─ ClickHouse events_raw for raw observability
        │
        ▼
PyFlink normalization, trend, anomaly, and correlation jobs
        │
        ▼
LLM enrichment
  category, sentiment, geography, entities, confidence
        │
        ▼
Kafka topic: events.enriched
        │
        ▼
ClickHouse analytical tables and materialized views
        │
        ▼
Streamlit dashboard
  firehose, volume trends, cross-source clusters, maps, breaking stories
```

## Phase 1.1 Local Infrastructure

The first local stack is intentionally small and free:

- Kafka runs as a single KRaft-mode broker with no Zookeeper.
- ClickHouse runs as a single local node with the `opensignal` database.
- Kafka UI connects to the internal Kafka listener for topic inspection and debugging.
- Host ports use `19092` for Kafka and `18080` for Kafka UI so OpenSignal can coexist with other local Kafka projects.

All services are local Docker Compose services. Cloud deployment is intentionally deferred to Stage 4 and requires explicit human approval.

## Canonical Raw Event Schema

All source producers write normalized JSON events to Kafka topic `events.raw`. Source-specific payloads are preserved in `raw`, but downstream systems should rely on the canonical top-level fields:

```json
{
  "event_id": "wikipedia:enwiki:1234567",
  "source": "wikipedia",
  "source_subtype": "edit",
  "timestamp": "2026-05-20T14:32:11Z",
  "ingested_at": "2026-05-20T14:32:13Z",
  "language": "en",
  "geography_hint": null,
  "title": "Some Article",
  "url": "https://en.wikipedia.org/wiki/Some_Article",
  "actor": "username_or_handle",
  "is_bot": false,
  "magnitude": 142,
  "content_id": "7654322",
  "parent_content_id": "7654321",
  "content_url": "https://en.wikipedia.org/w/index.php?diff=7654322&oldid=7654321",
  "content_hint": "tightened wording",
  "raw": {}
}
```

ClickHouse stores the canonical fields in typed columns and preserves the original source payload as `raw_json`. The Kafka-engine table uses `RawBLOB` plus JSON extraction in the materialized view so the Kafka contract can stay a normal nested JSON object instead of bending around ClickHouse Kafka-engine type constraints.

The content fields are references, not full content:

- `content_id`: source-specific immutable content/version ID, such as a Wikipedia new revision ID.
- `parent_content_id`: prior content/version ID when available, such as a Wikipedia old revision ID.
- `content_url`: a source URL suitable for fetching or inspecting the content/diff.
- `content_hint`: source-provided short text about the event, such as a Wikipedia edit comment.

Full content and diffs should be fetched later only for selected high-value events. This keeps the firehose cheap while giving the LLM pipeline enough pointers to retrieve context when an edit is large, repeated, cross-source correlated, or otherwise interesting.

## Wikipedia Normalization Decisions

- `event_id` is `wikipedia:{wiki}:{id}` so IDs remain globally unique across Wikimedia projects.
- `source_subtype` uses Wikimedia's `type` field, which can include edits, new pages, logs, and categorization events.
- `language` is inferred from the Wikimedia domain first, then from the wiki code. Commons appears as `commons`, not an ISO language code.
- `magnitude` is the absolute byte delta when Wikimedia provides old and new lengths; log-like events leave it null.
- `content_id`, `parent_content_id`, and `content_url` come from Wikimedia revision IDs and `notify_url`, giving later workers a cheap path to fetch the diff via MediaWiki APIs.
- `content_hint` stores the Wikimedia edit comment, which is often enough for first-pass filtering but should not be treated as ground truth.
- `geography_hint` is null for Wikipedia until enrichment or source-specific geo inference is added later.

## GDELT Normalization Decisions

- `event_id` is `gdelt:{GLOBALEVENTID}`.
- `source_subtype` is `cameo:{EventCode}` to preserve the GDELT/CAMEO event taxonomy without inventing our own categories before the LLM layer.
- `timestamp` uses `DATEADDED`, which matches the 15-minute GDELT update cadence better than day-level `SQLDATE`.
- `geography_hint` uses `ActionGeo_CountryCode`, falling back to actor geo country codes if the action geography is empty.
- `magnitude` uses `NumMentions`, which is the best first-pass attention measure in the event export.
- `content_url` stores `SOURCEURL`, while `content_id` stores `GLOBALEVENTID`.
- Rows with `EventRootCode = 03` are filtered as low-intensity verbal cooperation noise for this dashboard's first version.

## Hacker News Normalization Decisions

Reddit ingestion is skipped in Stage 1 because classic API app creation is blocked by the current Reddit developer flow. Hacker News is used as the credential-free discussion source instead.

- `event_id` is `hackernews:{id}`.
- `source_subtype` is `story:{list_name}`, such as `story:top`, `story:new`, or `story:best`.
- `timestamp` uses the HN story `time`.
- `language` is `en`.
- `magnitude` is `score + descendants`, combining voting and comment attention.
- `content_id` is the HN item ID.
- `content_url` is the external story URL, falling back to the HN item discussion URL.
- `content_hint` stores the title plus story text when available.

## Bluesky Normalization Decisions

Bluesky Jetstream is implemented as an optional experimental source, but is not the default Stage 1 social/discussion signal because raw firehose quality is noisy.

- `event_id` is `bluesky:{did}:{rkey}`.
- `source_subtype` is `post`.
- `timestamp` uses the post record's `createdAt`, falling back to Jetstream `time_us`.
- `language` uses the first value in the post record's `langs` array.
- `magnitude` is `0` at ingestion because Jetstream create events do not include like/repost counts.
- `content_id` is the AT URI, and `content_url` is the public `bsky.app` post URL.
- `content_hint` stores the post text so the LLM layer can classify social signals without a second content fetch.
