from ingestion.bluesky_producer import jetstream_url, normalize_jetstream_post


def post_payload() -> dict:
    return {
        "did": "did:plc:abc123",
        "time_us": 1779305536000000,
        "kind": "commit",
        "commit": {
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": "3abcxyz",
            "record": {
                "$type": "app.bsky.feed.post",
                "createdAt": "2026-05-20T19:32:16.123Z",
                "langs": ["en"],
                "text": "Breaking signal from an open social firehose",
            },
        },
    }


def test_normalize_jetstream_post_canonical_schema() -> None:
    event = normalize_jetstream_post(post_payload())

    assert event is not None
    assert event["event_id"] == "bluesky:did:plc:abc123:3abcxyz"
    assert event["source"] == "bluesky"
    assert event["source_subtype"] == "post"
    assert event["timestamp"] == "2026-05-20T19:32:16Z"
    assert event["language"] == "en"
    assert event["geography_hint"] is None
    assert event["title"] == "Breaking signal from an open social firehose"
    assert event["actor"] == "did:plc:abc123"
    assert event["magnitude"] == 0
    assert event["content_id"] == "at://did:plc:abc123/app.bsky.feed.post/3abcxyz"
    assert event["content_url"] == "https://bsky.app/profile/did:plc:abc123/post/3abcxyz"
    assert event["content_hint"] == "Breaking signal from an open social firehose"


def test_ignores_non_post_records() -> None:
    payload = post_payload()
    payload["commit"]["collection"] = "app.bsky.graph.follow"

    assert normalize_jetstream_post(payload) is None


def test_adds_collection_filter_to_jetstream_url() -> None:
    assert (
        jetstream_url("wss://jetstream1.us-east.bsky.network/subscribe")
        == "wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
    )

