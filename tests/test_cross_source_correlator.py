from datetime import UTC, datetime, timedelta

from flink_jobs.cross_source_correlator import (
    EnrichedEvent,
    cluster_events,
    normalize_entity,
    useful_entities,
)


def event(event_id: str, source: str, timestamp: datetime, entities: tuple[str, ...]) -> EnrichedEvent:
    return EnrichedEvent(
        event_id=event_id,
        source=source,
        timestamp=timestamp,
        title=f"title {event_id}",
        category="politics",
        sentiment="contested",
        entities=entities,
    )


def test_normalize_entity_strips_case_and_spacing() -> None:
    assert normalize_entity("  Federal Judge  ") == "federal judge"
    assert normalize_entity("(Trump)") == "trump"


def test_useful_entities_drops_generic_terms() -> None:
    entities = useful_entities(("Government", "Trump", "US", "Federal Judge"))

    assert "government" not in entities
    assert "trump" not in entities
    assert "federal judge" in entities


def test_cluster_events_connects_events_with_two_shared_entities() -> None:
    now = datetime(2026, 5, 21, 12, 0, tzinfo=UTC)
    clusters = cluster_events(
        [
            event("a", "gdelt", now, ("Federal Judge", "Washington DC", "Illinois Firearm ID Law")),
            event("b", "wikipedia", now + timedelta(minutes=3), ("Federal Judge", "Illinois Firearm ID Law", "Illinois")),
            event("c", "hackernews", now, ("STMicroelectronics", "Geneva")),
        ],
        min_shared_entities=2,
        min_events=2,
    )

    assert clusters
    assert any(cluster.total_events == 2 for cluster in clusters)
    assert any(cluster.source_counts == {"gdelt": 1, "wikipedia": 1} for cluster in clusters)


def test_cluster_events_ignores_single_entity_overlap() -> None:
    now = datetime(2026, 5, 21, 12, 0, tzinfo=UTC)
    clusters = cluster_events(
        [
            event("a", "gdelt", now, ("Trump", "Federal Judge")),
            event("b", "wikipedia", now + timedelta(minutes=3), ("Trump", "Illinois")),
        ],
        min_shared_entities=2,
        min_events=2,
    )

    assert clusters == []
