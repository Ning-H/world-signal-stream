import pytest

from enrichment.llm_classifier import RawEvent, dry_run_enrichment, event_cache_key, validate_enrichment


def test_validate_enrichment_normalizes_values() -> None:
    data = validate_enrichment(
        {
            "category": "tech",
            "sentiment": "neutral",
            "geography": ["us", " fr "],
            "entities": [" OpenAI ", ""],
            "confidence": 1.3,
            "summary": "A model result.",
        }
    )

    assert data["geography"] == ["US", "FR"]
    assert data["entities"] == ["OpenAI"]
    assert data["confidence"] == 1.0


def test_validate_enrichment_rejects_unknown_category() -> None:
    with pytest.raises(ValueError, match="Invalid category"):
        validate_enrichment(
            {
                "category": "education",
                "sentiment": "neutral",
                "geography": [],
                "entities": [],
                "confidence": 0.8,
                "summary": "Schema drift should go to the DLQ.",
            }
        )


def test_dry_run_enrichment_uses_source_hints() -> None:
    event = RawEvent(
        event_id="hackernews:1",
        source="hackernews",
        source_subtype="story:top",
        timestamp="2026-05-20T00:00:00Z",
        title="New AI software model",
        url=None,
        actor="alice",
        magnitude=10,
        geography_hint=None,
        content_hint="A post about AI infrastructure.",
    )

    result = dry_run_enrichment(event)

    assert result["category"] == "tech"
    assert result["sentiment"] == "neutral"
    assert result["entities"] == ["alice"]


def test_event_cache_key_dedupes_same_title_and_content() -> None:
    event_one = RawEvent("a", "hackernews", "story:top", "", "Same Title", None, None, None, None, "body")
    event_two = RawEvent("b", "hackernews", "story:new", "", "same title", None, None, None, None, "body")

    assert event_cache_key(event_one) == event_cache_key(event_two)
