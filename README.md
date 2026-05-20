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
