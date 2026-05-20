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
