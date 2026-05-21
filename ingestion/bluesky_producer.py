"""Stream Bluesky Jetstream posts into Kafka using OpenSignal's canonical schema."""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError
from websockets.exceptions import WebSocketException
from websockets.sync.client import connect


LOGGER = logging.getLogger("opensignal.bluesky_producer")

DEFAULT_JETSTREAM_URL = "wss://jetstream1.us-east.bsky.network/subscribe"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:19092"
RAW_TOPIC = "events.raw"
DLQ_TOPIC = "events.dlq"
POST_COLLECTION = "app.bsky.feed.post"

STOP_REQUESTED = False


def _request_stop(_signum: int, _frame: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_timestamp(value: Any, fallback_time_us: Any = None) -> str:
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
            return parsed.isoformat(timespec="seconds").replace("+00:00", "Z")
        except ValueError:
            pass

    try:
        parsed = datetime.fromtimestamp(int(fallback_time_us) / 1_000_000, tz=UTC)
        return parsed.isoformat(timespec="seconds").replace("+00:00", "Z")
    except (TypeError, ValueError, OSError):
        return utc_now_iso()


def first_language(record: dict[str, Any]) -> str | None:
    langs = record.get("langs")
    if isinstance(langs, list) and langs:
        lang = langs[0]
        return str(lang) if lang else None
    return None


def post_url(did: str, rkey: str) -> str:
    return f"https://bsky.app/profile/{did}/post/{rkey}"


def jetstream_url(base_url: str) -> str:
    separator = "&" if "?" in base_url else "?"
    query = urlencode({"wantedCollections": POST_COLLECTION})
    return f"{base_url}{separator}{query}"


def normalize_jetstream_post(payload: dict[str, Any]) -> dict[str, Any] | None:
    if payload.get("kind") != "commit":
        return None

    commit = payload.get("commit")
    if not isinstance(commit, dict):
        return None
    if commit.get("operation") != "create" or commit.get("collection") != POST_COLLECTION:
        return None

    record = commit.get("record")
    if not isinstance(record, dict):
        return None

    text = str(record.get("text") or "").strip()
    if not text:
        return None

    did = str(payload.get("did") or "")
    rkey = str(commit.get("rkey") or "")
    if not did or not rkey:
        raise ValueError("Bluesky post payload missing did or rkey")

    content_id = f"at://{did}/{POST_COLLECTION}/{rkey}"
    url = post_url(did, rkey)

    return {
        "event_id": f"bluesky:{did}:{rkey}",
        "source": "bluesky",
        "source_subtype": "post",
        "timestamp": parse_timestamp(record.get("createdAt"), payload.get("time_us")),
        "ingested_at": utc_now_iso(),
        "language": first_language(record),
        "geography_hint": None,
        "title": text[:240],
        "url": url,
        "actor": did,
        "is_bot": False,
        "magnitude": 0,
        "content_id": content_id,
        "parent_content_id": None,
        "content_url": url,
        "content_hint": text[:500],
        "raw": payload,
    }


def language_allowed(event: dict[str, Any], allowed_languages: set[str]) -> bool:
    if not allowed_languages:
        return True
    language = event.get("language")
    return language in allowed_languages


def kafka_json_serializer(value: dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def create_topics(bootstrap_servers: str, raw_topic: str, dlq_topic: str) -> None:
    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="opensignal-bluesky-topic-admin",
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


def publish_dlq(
    producer: KafkaProducer,
    dlq_topic: str,
    reason: str,
    payload: Any,
    exception: Exception | None = None,
) -> None:
    dlq_event = {
        "source": "bluesky",
        "stage": "ingestion",
        "reason": reason,
        "error": repr(exception) if exception else None,
        "ingested_at": utc_now_iso(),
        "raw": payload,
    }
    producer.send(dlq_topic, key=f"bluesky:dlq:{time.time_ns()}".encode("utf-8"), value=dlq_event)


def run(
    stream_url: str,
    bootstrap_servers: str,
    raw_topic: str,
    dlq_topic: str,
    languages: set[str],
    max_events: int | None = None,
) -> int:
    create_topics(bootstrap_servers, raw_topic, dlq_topic)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=kafka_json_serializer,
        linger_ms=25,
        retries=5,
        acks="all",
        client_id="opensignal-bluesky-producer",
    )

    produced = 0
    backoff_seconds = 1
    subscribe_url = jetstream_url(stream_url)

    try:
        while not STOP_REQUESTED:
            try:
                LOGGER.info("Connecting to Bluesky Jetstream: %s", subscribe_url)
                with connect(subscribe_url, open_timeout=30, close_timeout=10) as websocket:
                    backoff_seconds = 1
                    for message in websocket:
                        if STOP_REQUESTED:
                            break
                        try:
                            payload = json.loads(message)
                            event = normalize_jetstream_post(payload)
                            if event is None or not language_allowed(event, languages):
                                continue
                            producer.send(raw_topic, key=event["event_id"].encode("utf-8"), value=event)
                            produced += 1
                            if produced % 1000 == 0:
                                producer.flush(timeout=15)
                                LOGGER.info("Produced %s Bluesky events", produced)
                            if max_events is not None and produced >= max_events:
                                producer.flush(timeout=30)
                                return produced
                        except (TypeError, ValueError, json.JSONDecodeError, KafkaError) as exc:
                            LOGGER.warning("Sending malformed Bluesky event to DLQ: %s", exc)
                            publish_dlq(producer, dlq_topic, "parse_normalize_or_produce_failed", message, exc)
            except WebSocketException as exc:
                LOGGER.warning(
                    "Bluesky Jetstream disconnected: %s; reconnecting in %ss",
                    exc,
                    backoff_seconds,
                )
                publish_dlq(producer, dlq_topic, "stream_disconnected", None, exc)
                producer.flush(timeout=15)
                time.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 60)
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)

    return produced


def parse_languages(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stream-url", default=os.getenv("BLUESKY_JETSTREAM_URL", DEFAULT_JETSTREAM_URL))
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
    )
    parser.add_argument("--raw-topic", default=os.getenv("KAFKA_TOPIC_RAW", RAW_TOPIC))
    parser.add_argument("--dlq-topic", default=os.getenv("KAFKA_TOPIC_DLQ", DLQ_TOPIC))
    parser.add_argument("--languages", default=os.getenv("BLUESKY_LANGUAGES", "en"))
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
        stream_url=args.stream_url,
        bootstrap_servers=args.bootstrap_servers,
        raw_topic=args.raw_topic,
        dlq_topic=args.dlq_topic,
        languages=parse_languages(args.languages),
        max_events=args.max_events,
    )
    LOGGER.info("Stopped after producing %s Bluesky events", produced)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

