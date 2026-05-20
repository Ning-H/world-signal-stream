# OpenSignal

A live, multi-source dashboard that ingests open public signals and surfaces what the world is paying attention to right now.

Status: under construction.

## Local Infrastructure

Phase 1.1 brings up the local Kafka, ClickHouse, and Kafka UI services:

```bash
docker compose up -d
```

Once healthy:

- Kafka UI: http://localhost:18080
- ClickHouse HTTP: http://localhost:8123

ClickHouse smoke test:

```bash
curl "http://localhost:8123/?query=SELECT%201"
```

Note: the local Kafka and Kafka UI host ports are `19092` and `18080` to avoid common conflicts with other local Kafka stacks. Docker-internal service names remain stable.

## Wikipedia Ingestion

Phase 1.2 adds a Wikimedia recent-change producer that normalizes each event into the OpenSignal canonical schema and writes to Kafka topic `events.raw`.

Apply the ClickHouse raw-event schema:

```bash
docker compose exec -T clickhouse clickhouse-client < clickhouse/schema.sql
```

Run a short smoke test:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python -m ingestion.wikipedia_producer --max-events 100
```

For tests:

```bash
pip install -r requirements-dev.txt
pytest -q
```

Check ingested rows:

```bash
curl "http://localhost:8123/?query=SELECT%20count()%20FROM%20opensignal.events_raw%20WHERE%20source%3D%27wikipedia%27"
```

For continuous ingestion, omit `--max-events`. The producer reconnects to Wikimedia with exponential backoff and publishes malformed parse/normalization failures to `events.dlq`.

Wikipedia edit bodies are not included in the live stream. OpenSignal stores revision references (`content_id`, `parent_content_id`), the diff URL (`content_url`), and the edit comment (`content_hint`) so later workers can fetch diffs only for events worth LLM analysis.

### Phase 1.2 Soak Result

A 30-minute Wikipedia producer soak ran on 2026-05-20 from 19:46:29Z to 20:16:29Z.

- Events produced and stored: 87,845
- Observed rate: about 176K events/hour
- ClickHouse stayed current with Kafka during the run
- DLQ entries: 2, both understood stream disconnect events from Wikimedia
- Reconnect behavior: successful automatic reconnects after premature stream endings
- Content reference coverage: 46,508 rows with revision IDs, 84,149 with content URLs, 84,309 with edit comments

The live Wikimedia stream includes substantial bot and maintenance activity, especially Commons, Wikidata, and categorization events. Later trend and enrichment stages should filter or prioritize events instead of treating every raw change as equally meaningful.

## GDELT Ingestion

Phase 1.3 adds a GDELT 2.0 producer that polls `lastupdate.txt`, downloads the latest `*.export.CSV.zip`, filters low-intensity verbal cooperation events, normalizes rows into the canonical schema, and writes to `events.raw`.

```bash
python -m ingestion.gdelt_producer --once
```

Restart safety uses a local SQLite marker at `data/gdelt_state.sqlite3`, so the same export file is not reprocessed after a producer restart.
