"""Poll Hacker News stories and publish normalized events to Kafka."""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sqlite3
import sys
import time
from datetime import UTC, datetime
from typing import Any, Iterable

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError


LOGGER = logging.getLogger("opensignal.hackernews_producer")

DEFAULT_BOOTSTRAP_SERVERS = "localhost:19092"
DEFAULT_BASE_URL = "https://hacker-news.firebaseio.com/v0"
DEFAULT_STATE_DB = "data/hackernews_state.sqlite3"
RAW_TOPIC = "events.raw"
DLQ_TOPIC = "events.dlq"

STOP_REQUESTED = False


def _request_stop(_signum: int, _frame: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def timestamp_to_iso(timestamp: Any) -> str:
    try:
        parsed = datetime.fromtimestamp(int(timestamp), tz=UTC)
        return parsed.isoformat(timespec="seconds").replace("+00:00", "Z")
    except (TypeError, ValueError, OSError):
        return utc_now_iso()


def to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_hackernews_item(item: dict[str, Any], list_name: str) -> dict[str, Any]:
    item_id = item.get("id")
    if item_id is None:
        raise ValueError("Hacker News item is missing id")
    if item.get("type") != "story":
        raise ValueError(f"Unsupported Hacker News item type: {item.get('type')}")

    title = str(item.get("title") or "").strip()
    text = str(item.get("text") or "").strip()
    if not title and not text:
        raise ValueError("Hacker News story is missing title/text")

    score = to_int(item.get("score")) or 0
    descendants = to_int(item.get("descendants")) or 0
    magnitude = score + descendants

    url = item.get("url") or f"https://news.ycombinator.com/item?id={item_id}"
    content_hint_parts = [part for part in [title, text] if part]
    content_hint = "\n\n".join(content_hint_parts)[:1000]

    return {
        "event_id": f"hackernews:{item_id}",
        "source": "hackernews",
        "source_subtype": f"story:{list_name}",
        "timestamp": timestamp_to_iso(item.get("time")),
        "ingested_at": utc_now_iso(),
        "language": "en",
        "geography_hint": None,
        "title": title or text[:240],
        "url": url,
        "actor": item.get("by"),
        "is_bot": False,
        "magnitude": magnitude,
        "content_id": str(item_id),
        "parent_content_id": None,
        "content_url": url,
        "content_hint": content_hint,
        "raw": item,
    }


def kafka_json_serializer(value: dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def create_topics(bootstrap_servers: str, raw_topic: str, dlq_topic: str) -> None:
    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="opensignal-hackernews-topic-admin",
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
        CREATE TABLE IF NOT EXISTS hackernews_seen_items (
            item_id INTEGER PRIMARY KEY,
            first_seen_at TEXT NOT NULL,
            list_name TEXT NOT NULL
        )
        """
    )
    connection.commit()
    return connection


def already_seen(connection: sqlite3.Connection, item_id: int) -> bool:
    row = connection.execute(
        "SELECT 1 FROM hackernews_seen_items WHERE item_id = ?",
        (item_id,),
    ).fetchone()
    return row is not None


def mark_seen(connection: sqlite3.Connection, item_id: int, list_name: str) -> None:
    connection.execute(
        """
        INSERT OR IGNORE INTO hackernews_seen_items (item_id, first_seen_at, list_name)
        VALUES (?, ?, ?)
        """,
        (item_id, utc_now_iso(), list_name),
    )
    connection.commit()


def fetch_story_ids(base_url: str, list_name: str) -> list[int]:
    response = requests.get(f"{base_url}/{list_name}stories.json", timeout=30)
    response.raise_for_status()
    return [int(item_id) for item_id in response.json()]


def fetch_item(base_url: str, item_id: int) -> dict[str, Any] | None:
    response = requests.get(f"{base_url}/item/{item_id}.json", timeout=30)
    response.raise_for_status()
    item = response.json()
    return item if isinstance(item, dict) else None


def iter_items(base_url: str, list_names: Iterable[str], limit_per_list: int) -> Iterable[tuple[str, dict[str, Any]]]:
    for list_name in list_names:
        story_ids = fetch_story_ids(base_url, list_name)[:limit_per_list]
        LOGGER.info("Fetched %s Hacker News IDs from %sstories", len(story_ids), list_name)
        for item_id in story_ids:
            item = fetch_item(base_url, item_id)
            if item:
                yield list_name, item


def publish_dlq(
    producer: KafkaProducer,
    dlq_topic: str,
    reason: str,
    payload: Any,
    exception: Exception | None = None,
) -> None:
    dlq_event = {
        "source": "hackernews",
        "stage": "ingestion",
        "reason": reason,
        "error": repr(exception) if exception else None,
        "ingested_at": utc_now_iso(),
        "raw": payload,
    }
    producer.send(dlq_topic, key=f"hackernews:dlq:{time.time_ns()}".encode("utf-8"), value=dlq_event)


def run(
    base_url: str,
    bootstrap_servers: str,
    raw_topic: str,
    dlq_topic: str,
    state_db: str,
    list_names: list[str],
    limit_per_list: int,
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
        client_id="opensignal-hackernews-producer",
    )

    total_produced = 0
    try:
        while not STOP_REQUESTED:
            produced_this_poll = 0
            skipped_seen = 0
            for list_name, item in iter_items(base_url, list_names, limit_per_list):
                item_id = to_int(item.get("id"))
                if item_id is None:
                    publish_dlq(producer, dlq_topic, "missing_item_id", item)
                    continue
                if already_seen(state, item_id):
                    skipped_seen += 1
                    continue
                try:
                    event = normalize_hackernews_item(item, list_name)
                    producer.send(raw_topic, key=event["event_id"].encode("utf-8"), value=event)
                    mark_seen(state, item_id, list_name)
                    produced_this_poll += 1
                    total_produced += 1
                    if max_events is not None and total_produced >= max_events:
                        producer.flush(timeout=30)
                        LOGGER.info(
                            "Produced %s Hacker News events; skipped_seen=%s",
                            produced_this_poll,
                            skipped_seen,
                        )
                        return total_produced
                except (TypeError, ValueError, KafkaError) as exc:
                    LOGGER.warning("Sending malformed Hacker News item to DLQ: %s", exc)
                    publish_dlq(producer, dlq_topic, "normalize_or_produce_failed", item, exc)

            producer.flush(timeout=30)
            LOGGER.info(
                "Produced %s Hacker News events this poll; skipped_seen=%s",
                produced_this_poll,
                skipped_seen,
            )
            if once:
                break
            time.sleep(poll_interval_seconds)
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)
        state.close()

    return total_produced


def parse_list_names(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=os.getenv("HACKERNEWS_BASE_URL", DEFAULT_BASE_URL))
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
    )
    parser.add_argument("--raw-topic", default=os.getenv("KAFKA_TOPIC_RAW", RAW_TOPIC))
    parser.add_argument("--dlq-topic", default=os.getenv("KAFKA_TOPIC_DLQ", DLQ_TOPIC))
    parser.add_argument("--state-db", default=os.getenv("HACKERNEWS_STATE_DB", DEFAULT_STATE_DB))
    parser.add_argument("--lists", default=os.getenv("HACKERNEWS_LISTS", "top,new,best"))
    parser.add_argument("--limit-per-list", type=int, default=int(os.getenv("HACKERNEWS_LIMIT_PER_LIST", "100")))
    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=int(os.getenv("HACKERNEWS_POLL_INTERVAL_SECONDS", "300")),
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
        base_url=args.base_url,
        bootstrap_servers=args.bootstrap_servers,
        raw_topic=args.raw_topic,
        dlq_topic=args.dlq_topic,
        state_db=args.state_db,
        list_names=parse_list_names(args.lists),
        limit_per_list=args.limit_per_list,
        poll_interval_seconds=args.poll_interval_seconds,
        once=args.once,
        max_events=args.max_events,
    )
    LOGGER.info("Stopped after producing %s Hacker News events", produced)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

