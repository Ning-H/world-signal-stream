# OpenSignal Project Statement

OpenSignal is a real-time global attention and sentiment dashboard for open public signals.

The project ingests high-volume public streams such as Wikipedia edits, GDELT global news, Reddit submissions, and optionally Bluesky posts. Each source is normalized into a shared event schema, stored in Kafka and ClickHouse, then enriched and visualized so journalists, OSINT researchers, and curious humans can see what the world is paying attention to right now.

## Product Thesis

Most tools that correlate public signals across multiple sources are expensive enterprise products or locked to one source. OpenSignal aims to show that a lightweight open-data platform can surface useful early signals by combining:

- raw attention volume,
- source diversity,
- geography,
- language,
- sentiment,
- entities,
- and cross-source topic overlap.

## Engineering Focus

The project is intentionally built around a data platform stack used in production AI and infrastructure teams:

- Kafka for streaming ingestion,
- PyFlink for stream processing,
- ClickHouse for low-latency analytical storage,
- LLM enrichment for classification and summarization,
- Streamlit for a demoable dashboard,
- Docker Compose for local reproducibility.

The local-first build keeps the system cheap and inspectable before any cloud deployment.

## Current Build Constraint

Raw ingestion should stay lightweight. For sources like Wikipedia, OpenSignal stores metadata and content references such as revision IDs and diff URLs, then fetches full content only for selected high-value events. This keeps the firehose scalable while giving later LLM workers enough context to analyze what changed.

