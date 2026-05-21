"""Entity-overlap topic clustering for enriched OpenSignal events.

This module is intentionally written as a bounded ClickHouse-backed job first.
The clustering logic is the same logic a PyFlink job should use later, but this
version is easier to validate locally before adding Flink runtime overhead.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import clickhouse_connect
from dotenv import load_dotenv


SOURCE_COLUMNS = {
    "wikipedia": "wikipedia_events",
    "gdelt": "gdelt_events",
    "hackernews": "hackernews_events",
}
GENERIC_ENTITIES = {
    "government",
    "company",
    "president",
    "police",
    "prison",
    "unknown actor",
    "united states",
    "state",
    "states",
    "minister",
    "student",
    "school",
    "college",
    "deputy",
    "senator",
    "mayor",
    "attorney",
}


@dataclass(frozen=True)
class EnrichedEvent:
    event_id: str
    source: str
    timestamp: datetime
    title: str
    category: str
    sentiment: str
    entities: tuple[str, ...]


@dataclass(frozen=True)
class TopicCluster:
    topic_id: str
    window_start: datetime
    window_end: datetime
    top_entities: tuple[str, ...]
    category: str
    sentiment: str
    source_counts: dict[str, int]
    total_events: int
    first_seen: datetime
    last_seen: datetime
    sample_titles: tuple[str, ...]


class UnionFind:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))

    def find(self, index: int) -> int:
        while self.parent[index] != index:
            self.parent[index] = self.parent[self.parent[index]]
            index = self.parent[index]
        return index

    def union(self, left: int, right: int) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root != right_root:
            self.parent[right_root] = left_root


def clickhouse_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "opensignal"),
    )


def normalize_entity(entity: str) -> str:
    normalized = re.sub(r"\s+", " ", entity.strip().lower())
    normalized = normalized.strip(".,:;()[]{}")
    return normalized


def useful_entities(entities: tuple[str, ...] | list[str]) -> set[str]:
    useful = set()
    for entity in entities:
        normalized = normalize_entity(str(entity))
        if len(normalized) < 3:
            continue
        if normalized in GENERIC_ENTITIES:
            continue
        useful.add(normalized)
    return useful


def display_entity(entity: str) -> str:
    known_upper = {"ai", "fbi", "nasa", "nyse", "ndis", "mhra", "dei"}
    return entity.upper() if entity in known_upper else entity.title()


def floor_to_five_minutes(timestamp: datetime) -> datetime:
    timestamp = timestamp.astimezone(UTC).replace(second=0, microsecond=0)
    minute = timestamp.minute - (timestamp.minute % 5)
    return timestamp.replace(minute=minute)


def event_window_starts(event_time: datetime, first_start: datetime, last_start: datetime) -> list[datetime]:
    latest_start = min(floor_to_five_minutes(event_time), last_start)
    earliest_start = max(first_start, floor_to_five_minutes(event_time - timedelta(minutes=55)))
    starts = []
    current = earliest_start
    while current <= latest_start:
        starts.append(current)
        current += timedelta(minutes=5)
    return starts


def topic_id(window_start: datetime, entities: tuple[str, ...]) -> str:
    seed = f"{window_start.isoformat()}|{'|'.join(sorted(entities))}"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:24]


def cluster_window(
    events: list[EnrichedEvent],
    window_start: datetime,
    min_shared_entities: int,
    min_events: int,
) -> list[TopicCluster]:
    entity_sets = [useful_entities(event.entities) for event in events]
    candidates = [(index, entities) for index, entities in enumerate(entity_sets) if len(entities) >= min_shared_entities]
    if len(candidates) < min_events:
        return []

    union_find = UnionFind(len(events))
    for left_pos, (left_index, left_entities) in enumerate(candidates):
        for right_index, right_entities in candidates[left_pos + 1 :]:
            if len(left_entities & right_entities) >= min_shared_entities:
                union_find.union(left_index, right_index)

    grouped: dict[int, list[EnrichedEvent]] = defaultdict(list)
    for index, event in enumerate(events):
        if len(entity_sets[index]) >= min_shared_entities:
            grouped[union_find.find(index)].append(event)

    clusters = []
    for grouped_events in grouped.values():
        if len(grouped_events) < min_events:
            continue
        entity_counter: Counter[str] = Counter()
        category_counter: Counter[str] = Counter()
        sentiment_counter: Counter[str] = Counter()
        source_counter: Counter[str] = Counter()
        for event in grouped_events:
            entity_counter.update(useful_entities(event.entities))
            category_counter[event.category] += 1
            sentiment_counter[event.sentiment] += 1
            source_counter[event.source] += 1

        top_entities = tuple(display_entity(entity) for entity, _ in entity_counter.most_common(8))
        normalized_top_entities = tuple(entity for entity, _ in entity_counter.most_common(8))
        first_seen = min(event.timestamp for event in grouped_events)
        last_seen = max(event.timestamp for event in grouped_events)
        sample_titles = tuple(dict.fromkeys(event.title for event in grouped_events))[:5]
        clusters.append(
            TopicCluster(
                topic_id=topic_id(window_start, normalized_top_entities),
                window_start=window_start,
                window_end=window_start + timedelta(hours=1),
                top_entities=top_entities,
                category=category_counter.most_common(1)[0][0],
                sentiment=sentiment_counter.most_common(1)[0][0],
                source_counts=dict(source_counter),
                total_events=len(grouped_events),
                first_seen=first_seen,
                last_seen=last_seen,
                sample_titles=sample_titles,
            )
        )
    return sorted(clusters, key=lambda cluster: cluster.total_events, reverse=True)


def cluster_events(
    events: list[EnrichedEvent],
    min_shared_entities: int = 2,
    min_events: int = 2,
) -> list[TopicCluster]:
    if not events:
        return []

    first_start = floor_to_five_minutes(min(event.timestamp for event in events) - timedelta(minutes=55))
    last_start = floor_to_five_minutes(max(event.timestamp for event in events))
    windows: dict[datetime, list[EnrichedEvent]] = defaultdict(list)
    for event in events:
        for window_start in event_window_starts(event.timestamp, first_start, last_start):
            windows[window_start].append(event)

    clusters: list[TopicCluster] = []
    for window_start, window_events in sorted(windows.items()):
        clusters.extend(cluster_window(window_events, window_start, min_shared_entities, min_events))
    return clusters


def fetch_events(hours: int) -> list[EnrichedEvent]:
    rows = clickhouse_client().query_df(
        f"""
        SELECT
            e.event_id AS event_id,
            e.source AS source,
            coalesce(r.ingested_at, e.enriched_at, e.timestamp) AS attention_time,
            e.title AS title,
            e.category AS category,
            e.sentiment AS sentiment,
            e.entities AS entities
        FROM events_enriched AS e
        LEFT JOIN events_raw AS r ON e.event_id = r.event_id
        WHERE attention_time >= now() - INTERVAL {hours} HOUR
          AND length(e.entities) >= 2
        ORDER BY attention_time
        """
    )
    events = []
    for _, row in rows.iterrows():
        timestamp = row["attention_time"]
        if getattr(timestamp, "tzinfo", None) is None:
            timestamp = timestamp.replace(tzinfo=UTC)
        events.append(
            EnrichedEvent(
                event_id=str(row["event_id"]),
                source=str(row["source"]),
                timestamp=timestamp.astimezone(UTC),
                title=str(row["title"]),
                category=str(row["category"]),
                sentiment=str(row["sentiment"]),
                entities=tuple(str(entity) for entity in row["entities"]),
            )
        )
    return events


def insert_clusters(clusters: list[TopicCluster], min_shared_entities: int, min_events: int) -> None:
    if not clusters:
        return
    columns = [
        "topic_id",
        "window_start",
        "window_end",
        "top_entities",
        "category",
        "sentiment",
        "wikipedia_events",
        "gdelt_events",
        "hackernews_events",
        "total_events",
        "first_seen",
        "last_seen",
        "sample_titles",
        "min_shared_entities",
        "min_events",
        "computed_at",
    ]
    computed_at = datetime.now(UTC)
    first_window = min(cluster.window_start for cluster in clusters)
    last_window = max(cluster.window_start for cluster in clusters)
    clickhouse_client().command(
        "ALTER TABLE topic_clusters DELETE "
        f"WHERE window_start >= toDateTime('{first_window.strftime('%Y-%m-%d %H:%M:%S')}', 'UTC') "
        f"AND window_start <= toDateTime('{last_window.strftime('%Y-%m-%d %H:%M:%S')}', 'UTC') "
        "SETTINGS mutations_sync = 1"
    )
    rows: list[list[Any]] = []
    for cluster in clusters:
        row = {
            "topic_id": cluster.topic_id,
            "window_start": cluster.window_start,
            "window_end": cluster.window_end,
            "top_entities": list(cluster.top_entities),
            "category": cluster.category,
            "sentiment": cluster.sentiment,
            "wikipedia_events": cluster.source_counts.get("wikipedia", 0),
            "gdelt_events": cluster.source_counts.get("gdelt", 0),
            "hackernews_events": cluster.source_counts.get("hackernews", 0),
            "total_events": cluster.total_events,
            "first_seen": cluster.first_seen,
            "last_seen": cluster.last_seen,
            "sample_titles": list(cluster.sample_titles),
            "min_shared_entities": min_shared_entities,
            "min_events": min_events,
            "computed_at": computed_at,
        }
        rows.append([row[column] for column in columns])
    clickhouse_client().insert("topic_clusters", rows, column_names=columns)


def run(hours: int, min_shared_entities: int, min_events: int, dry_run: bool) -> dict[str, Any]:
    events = fetch_events(hours)
    clusters = cluster_events(events, min_shared_entities=min_shared_entities, min_events=min_events)
    if not dry_run:
        insert_clusters(clusters, min_shared_entities=min_shared_entities, min_events=min_events)
    return {
        "events_loaded": len(events),
        "clusters": len(clusters),
        "min_shared_entities": min_shared_entities,
        "min_events": min_events,
        "dry_run": dry_run,
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--min-shared-entities", type=int, default=2)
    parser.add_argument("--min-events", type=int, default=2)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = parse_args(argv or sys.argv[1:])
    result = run(
        hours=args.hours,
        min_shared_entities=args.min_shared_entities,
        min_events=args.min_events,
        dry_run=args.dry_run,
    )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
