"""Stream Wikimedia recent-change events into Kafka using OpenSignal's canonical schema."""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import UTC, datetime
from typing import Any, Iterator

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError


LOGGER = logging.getLogger("opensignal.wikipedia_producer")

DEFAULT_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:19092"
RAW_TOPIC = "events.raw"
DLQ_TOPIC = "events.dlq"

STOP_REQUESTED = False


def _request_stop(_signum: int, _frame: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def timestamp_to_iso(timestamp: Any) -> str:
    if timestamp is None:
        return utc_now_iso()

    try:
        parsed = datetime.fromtimestamp(int(timestamp), tz=UTC)
    except (TypeError, ValueError, OSError):
        return utc_now_iso()

    return parsed.isoformat(timespec="seconds").replace("+00:00", "Z")


def infer_language(payload: dict[str, Any]) -> str | None:
    meta = payload.get("meta") or {}
    domain = meta.get("domain") or payload.get("server_name") or ""
    wiki = payload.get("wiki") or ""

    if domain.endswith(".wikipedia.org"):
        return domain.split(".", maxsplit=1)[0]
    if wiki.endswith("wiki") and len(wiki) > 4:
        return wiki[:-4]
    return None


def normalize_recentchange(payload: dict[str, Any]) -> dict[str, Any]:
    """Convert a Wikimedia recentchange payload into the canonical event schema."""

    wiki = str(payload.get("wiki") or "unknown")
    change_id = payload.get("id") or (payload.get("meta") or {}).get("id")
    if change_id is None:
        raise ValueError("Wikimedia payload is missing id/meta.id")

    title = payload.get("title") or ""
    server_url = payload.get("server_url") or ""
    title_url = payload.get("title_url") or ""
    url = title_url if str(title_url).startswith("http") else f"{server_url}{title_url}"

    length = payload.get("length") if isinstance(payload.get("length"), dict) else {}
    old_length = length.get("old")
    new_length = length.get("new")
    magnitude = None
    if isinstance(old_length, int) and isinstance(new_length, int):
        magnitude = abs(new_length - old_length)

    return {
        "event_id": f"wikipedia:{wiki}:{change_id}",
        "source": "wikipedia",
        "source_subtype": str(payload.get("type") or "edit"),
        "timestamp": timestamp_to_iso(payload.get("timestamp")),
        "ingested_at": utc_now_iso(),
        "language": infer_language(payload),
        "geography_hint": None,
        "title": str(title),
        "url": url or None,
        "actor": payload.get("user"),
        "is_bot": bool(payload.get("bot", False)),
        "magnitude": magnitude,
        "raw": payload,
    }


def kafka_json_serializer(value: dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def create_topics(bootstrap_servers: str, raw_topic: str, dlq_topic: str) -> None:
    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="opensignal-wikipedia-topic-admin",
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


def iter_sse_json(stream_url: str, timeout_seconds: int = 90) -> Iterator[dict[str, Any]]:
    """Yield JSON payloads from an SSE endpoint.

    The Wikimedia stream emits Server-Sent Events with one or more `data:` lines per
    event. Requests handles network IO; this parser keeps only the current event in
    memory and lets the caller own reconnect behavior.
    """

    headers = {
        "Accept": "text/event-stream",
        "User-Agent": "opensignal-wikipedia-producer/0.1",
    }
    with requests.get(stream_url, headers=headers, stream=True, timeout=timeout_seconds) as response:
        response.raise_for_status()
        data_lines: list[str] = []

        for raw_line in response.iter_lines(decode_unicode=True):
            if STOP_REQUESTED:
                return
            if raw_line is None:
                continue

            line = raw_line.strip()
            if not line:
                if data_lines:
                    data = "\n".join(data_lines)
                    data_lines.clear()
                    yield json.loads(data)
                continue

            if line.startswith(":"):
                continue
            if line.startswith("data:"):
                data_lines.append(line.removeprefix("data:").strip())


def publish_dlq(
    producer: KafkaProducer,
    dlq_topic: str,
    reason: str,
    payload: Any,
    exception: Exception | None = None,
) -> None:
    dlq_event = {
        "source": "wikipedia",
        "stage": "ingestion",
        "reason": reason,
        "error": repr(exception) if exception else None,
        "ingested_at": utc_now_iso(),
        "raw": payload,
    }
    producer.send(dlq_topic, key=f"wikipedia:dlq:{time.time_ns()}".encode("utf-8"), value=dlq_event)


def run(
    stream_url: str,
    bootstrap_servers: str,
    raw_topic: str,
    dlq_topic: str,
    max_events: int | None = None,
) -> int:
    create_topics(bootstrap_servers, raw_topic, dlq_topic)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=kafka_json_serializer,
        linger_ms=25,
        retries=5,
        acks="all",
        client_id="opensignal-wikipedia-producer",
    )

    produced = 0
    backoff_seconds = 1

    try:
        while not STOP_REQUESTED:
            try:
                LOGGER.info("Connecting to Wikimedia stream: %s", stream_url)
                for payload in iter_sse_json(stream_url):
                    try:
                        event = normalize_recentchange(payload)
                        key = event["event_id"].encode("utf-8")
                        producer.send(raw_topic, key=key, value=event)
                        produced += 1

                        if produced % 1000 == 0:
                            producer.flush(timeout=15)
                            LOGGER.info("Produced %s Wikipedia events", produced)

                        if max_events is not None and produced >= max_events:
                            producer.flush(timeout=30)
                            return produced
                    except (TypeError, ValueError, KafkaError) as exc:
                        LOGGER.warning("Sending malformed Wikimedia event to DLQ: %s", exc)
                        publish_dlq(producer, dlq_topic, "normalize_or_produce_failed", payload, exc)

                backoff_seconds = 1
            except (requests.RequestException, json.JSONDecodeError) as exc:
                LOGGER.warning(
                    "Wikimedia stream disconnected or emitted invalid JSON: %s; reconnecting in %ss",
                    exc,
                    backoff_seconds,
                )
                publish_dlq(producer, dlq_topic, "stream_or_parse_failed", None, exc)
                producer.flush(timeout=15)
                time.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 60)
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)

    return produced


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stream-url", default=os.getenv("WIKIPEDIA_STREAM_URL", DEFAULT_STREAM_URL))
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
    )
    parser.add_argument("--raw-topic", default=os.getenv("KAFKA_TOPIC_RAW", RAW_TOPIC))
    parser.add_argument("--dlq-topic", default=os.getenv("KAFKA_TOPIC_DLQ", DLQ_TOPIC))
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
        max_events=args.max_events,
    )
    LOGGER.info("Stopped after producing %s events", produced)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

