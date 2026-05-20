"""Poll GDELT 2.0 export files and publish normalized events to Kafka."""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import signal
import sqlite3
import sys
import time
import zipfile
from datetime import UTC, datetime
from typing import Any, Iterable

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError


LOGGER = logging.getLogger("opensignal.gdelt_producer")

DEFAULT_LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:19092"
DEFAULT_STATE_DB = "data/gdelt_state.sqlite3"
RAW_TOPIC = "events.raw"
DLQ_TOPIC = "events.dlq"

STOP_REQUESTED = False

GDELT_EVENT_COLUMNS = [
    "GLOBALEVENTID",
    "SQLDATE",
    "MonthYear",
    "Year",
    "FractionDate",
    "Actor1Code",
    "Actor1Name",
    "Actor1CountryCode",
    "Actor1KnownGroupCode",
    "Actor1EthnicCode",
    "Actor1Religion1Code",
    "Actor1Religion2Code",
    "Actor1Type1Code",
    "Actor1Type2Code",
    "Actor1Type3Code",
    "Actor2Code",
    "Actor2Name",
    "Actor2CountryCode",
    "Actor2KnownGroupCode",
    "Actor2EthnicCode",
    "Actor2Religion1Code",
    "Actor2Religion2Code",
    "Actor2Type1Code",
    "Actor2Type2Code",
    "Actor2Type3Code",
    "IsRootEvent",
    "EventCode",
    "EventBaseCode",
    "EventRootCode",
    "QuadClass",
    "GoldsteinScale",
    "NumMentions",
    "NumSources",
    "NumArticles",
    "AvgTone",
    "Actor1Geo_Type",
    "Actor1Geo_FullName",
    "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code",
    "Actor1Geo_ADM2Code",
    "Actor1Geo_Lat",
    "Actor1Geo_Long",
    "Actor1Geo_FeatureID",
    "Actor2Geo_Type",
    "Actor2Geo_FullName",
    "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code",
    "Actor2Geo_ADM2Code",
    "Actor2Geo_Lat",
    "Actor2Geo_Long",
    "Actor2Geo_FeatureID",
    "ActionGeo_Type",
    "ActionGeo_FullName",
    "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code",
    "ActionGeo_ADM2Code",
    "ActionGeo_Lat",
    "ActionGeo_Long",
    "ActionGeo_FeatureID",
    "DATEADDED",
    "SOURCEURL",
]

# CAMEO root 03 is low-intensity verbal cooperation. It creates lots of
# procedural noise for this dashboard, so v1 excludes it.
FILTERED_EVENT_ROOT_CODES = {"03"}


def _request_stop(_signum: int, _frame: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_gdelt_datetime(value: str) -> str:
    for fmt in ("%Y%m%d%H%M%S", "%Y%m%d"):
        try:
            parsed = datetime.strptime(value, fmt).replace(tzinfo=UTC)
            return parsed.isoformat(timespec="seconds").replace("+00:00", "Z")
        except ValueError:
            continue
    return utc_now_iso()


def to_int(value: str | None) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def to_float(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def first_nonempty(*values: str | None) -> str | None:
    for value in values:
        if value:
            return value
    return None


def normalize_gdelt_event(row: dict[str, str]) -> dict[str, Any]:
    event_id = row.get("GLOBALEVENTID")
    if not event_id:
        raise ValueError("GDELT row is missing GLOBALEVENTID")

    actor1 = row.get("Actor1Name") or "Unknown actor"
    actor2 = row.get("Actor2Name") or "Unknown actor"
    event_code = row.get("EventCode") or "unknown"
    action_geo = row.get("ActionGeo_FullName") or row.get("ActionGeo_CountryCode") or "unknown location"
    date_added = row.get("DATEADDED") or row.get("SQLDATE") or ""

    title = f"{actor1} -> {actor2} ({event_code}) in {action_geo}"
    content_hint = (
        f"EventCode={event_code}; Root={row.get('EventRootCode') or ''}; "
        f"Goldstein={row.get('GoldsteinScale') or ''}; AvgTone={row.get('AvgTone') or ''}; "
        f"Mentions={row.get('NumMentions') or ''}"
    )

    return {
        "event_id": f"gdelt:{event_id}",
        "source": "gdelt",
        "source_subtype": f"cameo:{event_code}",
        "timestamp": parse_gdelt_datetime(date_added),
        "ingested_at": utc_now_iso(),
        "language": None,
        "geography_hint": first_nonempty(row.get("ActionGeo_CountryCode"), row.get("Actor1Geo_CountryCode"), row.get("Actor2Geo_CountryCode")),
        "title": title,
        "url": row.get("SOURCEURL") or None,
        "actor": " / ".join(value for value in [row.get("Actor1Name"), row.get("Actor2Name")] if value) or None,
        "is_bot": False,
        "magnitude": to_int(row.get("NumMentions")),
        "content_id": event_id,
        "parent_content_id": None,
        "content_url": row.get("SOURCEURL") or None,
        "content_hint": content_hint,
        "raw": {
            **row,
            "GoldsteinScale": to_float(row.get("GoldsteinScale")),
            "AvgTone": to_float(row.get("AvgTone")),
            "NumMentions": to_int(row.get("NumMentions")),
            "NumSources": to_int(row.get("NumSources")),
            "NumArticles": to_int(row.get("NumArticles")),
        },
    }


def should_keep_row(row: dict[str, str]) -> bool:
    return row.get("EventRootCode") not in FILTERED_EVENT_ROOT_CODES


def kafka_json_serializer(value: dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def create_topics(bootstrap_servers: str, raw_topic: str, dlq_topic: str) -> None:
    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="opensignal-gdelt-topic-admin",
    )
    try:
        existing_topics = set(admin.list_topics())
        topics = []
        if raw_topic not in existing_topics:
            topics.append(NewTopic(name=raw_topic, num_partitions=12, replication_factor=1))
        if dlq_topic not in existing_topics:
            topics.append(NewTopic(name=dlq_topic, num_partitions=3, replication_factor=1))
        if topics:
            try:
                admin.create_topics(new_topics=topics, validate_only=False)
                LOGGER.info("Created Kafka topics: %s", ", ".join(topic.name for topic in topics))
            except TopicAlreadyExistsError:
                LOGGER.info("Kafka topics already exist")
    finally:
        admin.close()


def init_state_db(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    connection = sqlite3.connect(path)
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS gdelt_processed_files (
            file_url TEXT PRIMARY KEY,
            processed_at TEXT NOT NULL,
            rows_seen INTEGER NOT NULL,
            rows_produced INTEGER NOT NULL,
            rows_filtered INTEGER NOT NULL
        )
        """
    )
    connection.commit()
    return connection


def is_processed(connection: sqlite3.Connection, file_url: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM gdelt_processed_files WHERE file_url = ?",
        (file_url,),
    ).fetchone()
    return row is not None


def mark_processed(
    connection: sqlite3.Connection,
    file_url: str,
    rows_seen: int,
    rows_produced: int,
    rows_filtered: int,
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO gdelt_processed_files
            (file_url, processed_at, rows_seen, rows_produced, rows_filtered)
        VALUES (?, ?, ?, ?, ?)
        """,
        (file_url, utc_now_iso(), rows_seen, rows_produced, rows_filtered),
    )
    connection.commit()


def latest_export_urls(lastupdate_url: str) -> list[str]:
    response = requests.get(lastupdate_url, timeout=30)
    response.raise_for_status()
    urls = []
    for line in response.text.splitlines():
        parts = line.strip().split()
        if len(parts) >= 3 and parts[2].endswith(".export.CSV.zip"):
            urls.append(parts[2])
    return urls


def iter_export_rows(file_url: str) -> Iterable[dict[str, str]]:
    response = requests.get(file_url, timeout=120)
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        csv_names = [name for name in archive.namelist() if name.endswith(".CSV")]
        if len(csv_names) != 1:
            raise ValueError(f"Expected one GDELT CSV in {file_url}, found {csv_names}")

        with archive.open(csv_names[0]) as raw_file:
            wrapper = io.TextIOWrapper(raw_file, encoding="utf-8", errors="replace", newline="")
            reader = csv.reader(wrapper, delimiter="\t")
            for values in reader:
                if len(values) != len(GDELT_EVENT_COLUMNS):
                    raise ValueError(
                        f"Expected {len(GDELT_EVENT_COLUMNS)} GDELT columns, got {len(values)} in {file_url}"
                    )
                yield dict(zip(GDELT_EVENT_COLUMNS, values, strict=True))


def publish_dlq(
    producer: KafkaProducer,
    dlq_topic: str,
    reason: str,
    payload: Any,
    exception: Exception | None = None,
) -> None:
    dlq_event = {
        "source": "gdelt",
        "stage": "ingestion",
        "reason": reason,
        "error": repr(exception) if exception else None,
        "ingested_at": utc_now_iso(),
        "raw": payload,
    }
    producer.send(dlq_topic, key=f"gdelt:dlq:{time.time_ns()}".encode("utf-8"), value=dlq_event)


def process_file(
    producer: KafkaProducer,
    file_url: str,
    raw_topic: str,
    dlq_topic: str,
    max_events: int | None = None,
) -> tuple[int, int, int, bool]:
    rows_seen = 0
    rows_produced = 0
    rows_filtered = 0
    completed = True

    for row in iter_export_rows(file_url):
        rows_seen += 1
        if not should_keep_row(row):
            rows_filtered += 1
            continue

        try:
            event = normalize_gdelt_event(row)
            producer.send(raw_topic, key=event["event_id"].encode("utf-8"), value=event)
            rows_produced += 1
            if max_events is not None and rows_produced >= max_events:
                completed = False
                break
        except (TypeError, ValueError, KafkaError) as exc:
            LOGGER.warning("Sending malformed GDELT event to DLQ: %s", exc)
            publish_dlq(producer, dlq_topic, "normalize_or_produce_failed", row, exc)

    producer.flush(timeout=30)
    return rows_seen, rows_produced, rows_filtered, completed


def run(
    lastupdate_url: str,
    bootstrap_servers: str,
    raw_topic: str,
    dlq_topic: str,
    state_db: str,
    poll_interval_seconds: int,
    once: bool = False,
    max_events: int | None = None,
) -> int:
    create_topics(bootstrap_servers, raw_topic, dlq_topic)
    state = init_state_db(state_db)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=kafka_json_serializer,
        linger_ms=25,
        retries=5,
        acks="all",
        client_id="opensignal-gdelt-producer",
    )
    total_produced = 0

    try:
        while not STOP_REQUESTED:
            for file_url in latest_export_urls(lastupdate_url):
                if is_processed(state, file_url):
                    LOGGER.info("Skipping already processed GDELT file: %s", file_url)
                    continue

                LOGGER.info("Processing GDELT file: %s", file_url)
                rows_seen, rows_produced, rows_filtered, completed = process_file(
                    producer,
                    file_url,
                    raw_topic,
                    dlq_topic,
                    max_events=max_events,
                )
                if completed:
                    mark_processed(state, file_url, rows_seen, rows_produced, rows_filtered)
                else:
                    LOGGER.info(
                        "Leaving %s unmarked because max-events stopped processing early",
                        file_url,
                    )
                total_produced += rows_produced
                LOGGER.info(
                    "Processed %s: seen=%s produced=%s filtered=%s completed=%s",
                    file_url,
                    rows_seen,
                    rows_produced,
                    rows_filtered,
                    completed,
                )

            if once:
                break
            time.sleep(poll_interval_seconds)
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)
        state.close()

    return total_produced


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lastupdate-url", default=os.getenv("GDELT_LASTUPDATE_URL", DEFAULT_LASTUPDATE_URL))
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
    )
    parser.add_argument("--raw-topic", default=os.getenv("KAFKA_TOPIC_RAW", RAW_TOPIC))
    parser.add_argument("--dlq-topic", default=os.getenv("KAFKA_TOPIC_DLQ", DLQ_TOPIC))
    parser.add_argument("--state-db", default=os.getenv("GDELT_STATE_DB", DEFAULT_STATE_DB))
    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=int(os.getenv("GDELT_POLL_INTERVAL_SECONDS", "300")),
    )
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--max-events", type=int, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    args = parse_args(argv or sys.argv[1:])
    produced = run(
        lastupdate_url=args.lastupdate_url,
        bootstrap_servers=args.bootstrap_servers,
        raw_topic=args.raw_topic,
        dlq_topic=args.dlq_topic,
        state_db=args.state_db,
        poll_interval_seconds=args.poll_interval_seconds,
        once=args.once,
        max_events=args.max_events,
    )
    LOGGER.info("Stopped after producing %s GDELT events", produced)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
