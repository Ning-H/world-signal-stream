from unittest.mock import Mock

from ingestion.gdelt_producer import (
    GDELT_EVENT_COLUMNS,
    normalize_gdelt_event,
    process_file,
    should_keep_row,
)


def base_row() -> dict[str, str]:
    row = {column: "" for column in GDELT_EVENT_COLUMNS}
    row.update(
        {
            "GLOBALEVENTID": "12345",
            "SQLDATE": "20260520",
            "Actor1Name": "GOVERNMENT",
            "Actor2Name": "CITIZENS",
            "EventCode": "141",
            "EventRootCode": "14",
            "GoldsteinScale": "-6.5",
            "NumMentions": "12",
            "NumSources": "3",
            "NumArticles": "4",
            "AvgTone": "-2.25",
            "ActionGeo_FullName": "Paris, Ile-de-France, France",
            "ActionGeo_CountryCode": "FR",
            "DATEADDED": "20260520210000",
            "SOURCEURL": "https://example.com/story",
        }
    )
    return row


def test_normalize_gdelt_event_canonical_schema() -> None:
    event = normalize_gdelt_event(base_row())

    assert event["event_id"] == "gdelt:12345"
    assert event["source"] == "gdelt"
    assert event["source_subtype"] == "cameo:141"
    assert event["timestamp"] == "2026-05-20T21:00:00Z"
    assert event["geography_hint"] == "FR"
    assert event["title"] == "GOVERNMENT -> CITIZENS (141) in Paris, Ile-de-France, France"
    assert event["url"] == "https://example.com/story"
    assert event["actor"] == "GOVERNMENT / CITIZENS"
    assert event["is_bot"] is False
    assert event["magnitude"] == 12
    assert event["content_id"] == "12345"
    assert event["content_url"] == "https://example.com/story"
    assert event["raw"]["AvgTone"] == -2.25


def test_filters_low_intensity_verbal_cooperation() -> None:
    row = base_row()
    row["EventRootCode"] = "03"

    assert should_keep_row(row) is False


def test_max_events_does_not_mark_file_complete(monkeypatch) -> None:
    rows = [base_row(), {**base_row(), "GLOBALEVENTID": "12346"}]
    monkeypatch.setattr("ingestion.gdelt_producer.iter_export_rows", lambda _url: iter(rows))

    producer = Mock()
    rows_seen, rows_produced, rows_filtered, completed = process_file(
        producer=producer,
        file_url="https://example.com/file.export.CSV.zip",
        raw_topic="events.raw",
        dlq_topic="events.dlq",
        max_events=1,
    )

    assert rows_seen == 1
    assert rows_produced == 1
    assert rows_filtered == 0
    assert completed is False
