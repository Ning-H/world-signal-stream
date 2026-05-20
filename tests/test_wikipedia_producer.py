from ingestion.wikipedia_producer import normalize_recentchange


def test_normalize_recentchange_canonical_schema() -> None:
    payload = {
        "id": 1234567,
        "type": "edit",
        "timestamp": 1779297131,
        "wiki": "enwiki",
        "server_name": "en.wikipedia.org",
        "server_url": "https://en.wikipedia.org",
        "title": "Some Article",
        "title_url": "/wiki/Some_Article",
        "user": "username_or_handle",
        "bot": False,
        "length": {"old": 100, "new": 242},
        "revision": {"old": 7654321, "new": 7654322},
        "notify_url": "https://en.wikipedia.org/w/index.php?diff=7654322&oldid=7654321",
        "comment": "tightened wording",
        "meta": {"domain": "en.wikipedia.org"},
    }

    event = normalize_recentchange(payload)

    assert event["event_id"] == "wikipedia:enwiki:1234567"
    assert event["source"] == "wikipedia"
    assert event["source_subtype"] == "edit"
    assert event["timestamp"] == "2026-05-20T17:12:11Z"
    assert event["language"] == "en"
    assert event["geography_hint"] is None
    assert event["url"] == "https://en.wikipedia.org/wiki/Some_Article"
    assert event["actor"] == "username_or_handle"
    assert event["is_bot"] is False
    assert event["magnitude"] == 142
    assert event["content_id"] == "7654322"
    assert event["parent_content_id"] == "7654321"
    assert event["content_url"] == "https://en.wikipedia.org/w/index.php?diff=7654322&oldid=7654321"
    assert event["content_hint"] == "tightened wording"
    assert event["raw"] == payload
