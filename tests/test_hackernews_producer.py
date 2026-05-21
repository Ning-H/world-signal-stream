from unittest.mock import Mock

from ingestion.hackernews_producer import normalize_hackernews_item, run


def story_item() -> dict:
    return {
        "by": "tedsanders",
        "descendants": 464,
        "id": 48212493,
        "score": 654,
        "time": 1779303930,
        "title": "An OpenAI model has disproved a central conjecture in discrete geometry",
        "type": "story",
        "url": "https://openai.com/index/model-disproves-discrete-geometry-conjecture/",
    }


def test_normalize_hackernews_item_canonical_schema() -> None:
    event = normalize_hackernews_item(story_item(), "top")

    assert event["event_id"] == "hackernews:48212493"
    assert event["source"] == "hackernews"
    assert event["source_subtype"] == "story:top"
    assert event["timestamp"] == "2026-05-20T19:05:30Z"
    assert event["language"] == "en"
    assert event["title"] == "An OpenAI model has disproved a central conjecture in discrete geometry"
    assert event["url"] == "https://openai.com/index/model-disproves-discrete-geometry-conjecture/"
    assert event["actor"] == "tedsanders"
    assert event["magnitude"] == 1118
    assert event["content_id"] == "48212493"
    assert event["content_url"] == "https://openai.com/index/model-disproves-discrete-geometry-conjecture/"


def test_run_skips_seen_items_on_second_poll(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("ingestion.hackernews_producer.create_topics", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "ingestion.hackernews_producer.iter_items",
        lambda *_args, **_kwargs: iter([("top", story_item())]),
    )

    producer = Mock()
    producer.send.return_value = None
    producer.flush.return_value = None
    producer.close.return_value = None
    monkeypatch.setattr("ingestion.hackernews_producer.KafkaProducer", lambda **_kwargs: producer)

    state_db = str(tmp_path / "hn.sqlite3")
    first_count = run(
        base_url="https://example.com",
        bootstrap_servers="localhost:19092",
        raw_topic="events.raw",
        dlq_topic="events.dlq",
        state_db=state_db,
        list_names=["top"],
        limit_per_list=1,
        poll_interval_seconds=300,
        once=True,
    )
    second_count = run(
        base_url="https://example.com",
        bootstrap_servers="localhost:19092",
        raw_topic="events.raw",
        dlq_topic="events.dlq",
        state_db=state_db,
        list_names=["top"],
        limit_per_list=1,
        poll_interval_seconds=300,
        once=True,
    )

    assert first_count == 1
    assert second_count == 0
