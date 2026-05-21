"""Selected-event LLM enrichment for OpenSignal."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import anthropic
import clickhouse_connect
from dotenv import load_dotenv


LOGGER = logging.getLogger("opensignal.llm_classifier")

PROMPT_VERSION = "v1"
CATEGORIES = {
    "politics",
    "conflict",
    "economy",
    "climate",
    "tech",
    "health",
    "culture",
    "sports",
    "entertainment",
    "science",
    "disaster",
    "other",
}
SENTIMENTS = {"positive", "negative", "neutral", "alarming", "celebratory", "contested"}

DEFAULT_MODEL = "claude-haiku-4-5-20251001"

# Keep pricing configurable because available Anthropic models can vary by
# workspace. Defaults use the public Haiku-class no-cache price point that
# OpenSignal targets for budget tracking.
DEFAULT_INPUT_PER_MTOK = 0.80
DEFAULT_OUTPUT_PER_MTOK = 4.00


@dataclass(frozen=True)
class RawEvent:
    event_id: str
    source: str
    source_subtype: str
    timestamp: Any
    title: str
    url: str | None
    actor: str | None
    magnitude: int | None
    geography_hint: str | None
    content_hint: str | None


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def clickhouse_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "opensignal"),
    )


def read_prompt(name: str) -> str:
    return (Path(__file__).parent / "prompts" / name).read_text(encoding="utf-8")


def event_cache_key(event: RawEvent) -> str:
    text = f"{event.source}|{event.title}|{event.content_hint or ''}".lower()
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def selected_events_sql(limit: int) -> str:
    return f"""
    WITH candidates AS
    (
        SELECT
            event_id,
            source,
            source_subtype,
            timestamp,
            title,
            url,
            actor,
            magnitude,
            geography_hint,
            content_hint,
            multiIf(
                source = 'gdelt', 3,
                source = 'hackernews', 2,
                source = 'wikipedia'
                    AND source_subtype IN ('edit', 'new')
                    AND coalesce(magnitude, 0) >= 500
                    AND is_bot = 0
                    AND NOT startsWith(title, 'Category:')
                    AND NOT startsWith(title, 'File:'),
                1,
                0
            ) AS priority
        FROM events_raw
        WHERE event_id NOT IN (SELECT event_id FROM events_enriched)
          AND event_id NOT IN (
              SELECT event_id
              FROM enrichment_dlq
              WHERE recorded_at >= now() - INTERVAL 1 DAY
          )
    )
    SELECT
        event_id,
        source,
        source_subtype,
        timestamp,
        title,
        url,
        actor,
        magnitude,
        geography_hint,
        content_hint
    FROM candidates
    WHERE priority > 0
    ORDER BY priority DESC, coalesce(magnitude, 0) DESC, timestamp DESC
    LIMIT {limit}
    """


def fetch_selected_events(limit: int) -> list[RawEvent]:
    rows = clickhouse_client().query_df(selected_events_sql(limit))
    events = []
    for _, row in rows.iterrows():
        events.append(
            RawEvent(
                event_id=str(row["event_id"]),
                source=str(row["source"]),
                source_subtype=str(row["source_subtype"]),
                timestamp=row["timestamp"],
                title=str(row["title"]),
                url=None if row["url"] is None else str(row["url"]),
                actor=None if row["actor"] is None else str(row["actor"]),
                magnitude=None if row["magnitude"] is None else int(row["magnitude"]),
                geography_hint=None if row["geography_hint"] is None else str(row["geography_hint"]),
                content_hint=None if row["content_hint"] is None else str(row["content_hint"]),
            )
        )
    return events


def event_prompt_payload(event: RawEvent) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "source": event.source,
        "source_subtype": event.source_subtype,
        "timestamp": str(event.timestamp),
        "title": event.title[:500],
        "actor": event.actor,
        "magnitude": event.magnitude,
        "geography_hint": event.geography_hint,
        "url": event.url,
        "content_hint": (event.content_hint or "")[:1000],
    }


def enrichment_tool_schema() -> dict[str, Any]:
    return {
        "name": "record_enrichment",
        "description": "Record category, sentiment, geography, entities, confidence, and summary for a public signal event.",
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "enum": sorted(CATEGORIES)},
                "sentiment": {"type": "string", "enum": sorted(SENTIMENTS)},
                "geography": {"type": "array", "items": {"type": "string"}},
                "entities": {"type": "array", "items": {"type": "string"}},
                "confidence": {"type": "number"},
                "summary": {"type": "string"},
            },
            "required": ["category", "sentiment", "geography", "entities", "confidence", "summary"],
        },
    }


def validate_enrichment(data: dict[str, Any]) -> dict[str, Any]:
    category = data.get("category")
    sentiment = data.get("sentiment")
    if category not in CATEGORIES:
        raise ValueError(f"Invalid category: {category}")
    if sentiment not in SENTIMENTS:
        raise ValueError(f"Invalid sentiment: {sentiment}")

    geography = data.get("geography") or []
    entities = data.get("entities") or []
    if not isinstance(geography, list) or not all(isinstance(item, str) for item in geography):
        raise ValueError("geography must be a string array")
    if not isinstance(entities, list) or not all(isinstance(item, str) for item in entities):
        raise ValueError("entities must be a string array")

    confidence = float(data.get("confidence", 0.0))
    confidence = max(0.0, min(1.0, confidence))
    summary = str(data.get("summary") or "")[:500]

    return {
        "category": category,
        "sentiment": sentiment,
        "geography": [item.strip().upper() for item in geography if item.strip()][:10],
        "entities": [item.strip() for item in entities if item.strip()][:20],
        "confidence": confidence,
        "summary": summary,
    }


def extract_tool_result(message: Any) -> dict[str, Any]:
    for block in message.content:
        if getattr(block, "type", None) == "tool_use" and getattr(block, "name", None) == "record_enrichment":
            return validate_enrichment(block.input)
    raise ValueError("Anthropic response did not include record_enrichment tool use")


def classify_event(client: anthropic.Anthropic, event: RawEvent, model: str) -> tuple[dict[str, Any], int, int]:
    system_prompt = "\n\n".join([read_prompt("categorize.txt"), read_prompt("geo_entity.txt")])
    message = client.messages.create(
        model=model,
        max_tokens=600,
        temperature=0,
        system=system_prompt,
        tools=[enrichment_tool_schema()],
        tool_choice={"type": "tool", "name": "record_enrichment"},
        messages=[
            {
                "role": "user",
                "content": "Classify this OpenSignal event:\n"
                + json.dumps(event_prompt_payload(event), ensure_ascii=False),
            }
        ],
    )
    input_tokens = int(getattr(message.usage, "input_tokens", 0) or 0)
    output_tokens = int(getattr(message.usage, "output_tokens", 0) or 0)
    return extract_tool_result(message), input_tokens, output_tokens


def dry_run_enrichment(event: RawEvent) -> dict[str, Any]:
    category = "other"
    sentiment = "neutral"
    text = f"{event.title} {event.content_hint or ''}".lower()
    if event.source == "hackernews" or any(term in text for term in ["ai", "model", "software", "chip", "app", "api"]):
        category = "tech"
    elif event.source == "gdelt" and event.geography_hint:
        category = "politics"
        sentiment = "contested"
    if re.search(r"\b(war|attack|killed|disaster|crash)\b", text):
        sentiment = "alarming"
    geography = [event.geography_hint] if event.geography_hint else []
    entities = [part.strip() for part in (event.actor or "").split("/") if part.strip() and part.strip() != "Unknown actor"]
    return {
        "category": category,
        "sentiment": sentiment,
        "geography": geography,
        "entities": entities[:10],
        "confidence": 0.25,
        "summary": event.title[:240],
    }


def estimate_cost(input_tokens: int, output_tokens: int) -> float:
    input_price = float(os.getenv("LLM_INPUT_PRICE_PER_MTOK", str(DEFAULT_INPUT_PER_MTOK)))
    output_price = float(os.getenv("LLM_OUTPUT_PRICE_PER_MTOK", str(DEFAULT_OUTPUT_PER_MTOK)))
    return (
        (input_tokens / 1_000_000) * input_price
        + (output_tokens / 1_000_000) * output_price
    )


def insert_enrichments(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    columns = [
        "event_id",
        "source",
        "source_subtype",
        "timestamp",
        "title",
        "url",
        "category",
        "sentiment",
        "geography",
        "entities",
        "confidence",
        "summary",
        "model",
        "prompt_version",
        "enriched_at",
    ]
    client = clickhouse_client()
    client.insert(
        "events_enriched",
        [[row[column] for column in columns] for row in rows],
        column_names=columns,
    )


def insert_cost(run_id: str, provider: str, model: str, input_tokens: int, output_tokens: int, events_enriched: int) -> None:
    columns = [
        "run_id",
        "provider",
        "model",
        "input_tokens",
        "output_tokens",
        "estimated_cost_usd",
        "events_enriched",
        "recorded_at",
    ]
    row = {
        "run_id": run_id,
        "provider": provider,
        "model": model,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "estimated_cost_usd": estimate_cost(input_tokens, output_tokens),
        "events_enriched": events_enriched,
        "recorded_at": datetime.now(UTC),
    }
    clickhouse_client().insert(
        "enrichment_costs",
        [[row[column] for column in columns]],
        column_names=columns,
    )


def insert_dlq(event: RawEvent | None, reason: str, error: Exception, payload: Any) -> None:
    columns = ["event_id", "source", "reason", "error", "payload", "recorded_at"]
    row = {
        "event_id": event.event_id if event else "",
        "source": event.source if event else "",
        "reason": reason,
        "error": repr(error),
        "payload": json.dumps(payload, ensure_ascii=False, default=str)[:5000],
        "recorded_at": datetime.now(UTC),
    }
    clickhouse_client().insert(
        "enrichment_dlq",
        [[row[column] for column in columns]],
        column_names=columns,
    )


def run(limit: int, dry_run: bool) -> dict[str, Any]:
    provider = os.getenv("LLM_PROVIDER", "anthropic")
    model = os.getenv("LLM_MODEL", DEFAULT_MODEL)
    if provider != "anthropic" and not dry_run:
        raise ValueError(f"Unsupported LLM_PROVIDER for live enrichment: {provider}")

    events = fetch_selected_events(limit)
    run_id = str(uuid.uuid4())
    enriched_rows: list[dict[str, Any]] = []
    total_input_tokens = 0
    total_output_tokens = 0
    client = None if dry_run else anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    local_cache: dict[str, dict[str, Any]] = {}

    for event in events:
        try:
            key = event_cache_key(event)
            if key in local_cache:
                enrichment = local_cache[key]
                input_tokens = 0
                output_tokens = 0
            elif dry_run:
                enrichment = dry_run_enrichment(event)
                input_tokens = 0
                output_tokens = 0
                local_cache[key] = enrichment
            else:
                enrichment, input_tokens, output_tokens = classify_event(client, event, model)  # type: ignore[arg-type]
                local_cache[key] = enrichment
                time.sleep(0.1)

            total_input_tokens += input_tokens
            total_output_tokens += output_tokens
            enriched_rows.append(
                {
                    "event_id": event.event_id,
                    "source": event.source,
                    "source_subtype": event.source_subtype,
                    "timestamp": event.timestamp,
                    "title": event.title,
                    "url": event.url,
                    "category": enrichment["category"],
                    "sentiment": enrichment["sentiment"],
                    "geography": enrichment["geography"],
                    "entities": enrichment["entities"],
                    "confidence": enrichment["confidence"],
                    "summary": enrichment["summary"],
                    "model": "dry-run" if dry_run else model,
                    "prompt_version": PROMPT_VERSION,
                    "enriched_at": datetime.now(UTC),
                }
            )
        except Exception as exc:  # noqa: BLE001 - route bad provider/model rows to DLQ
            LOGGER.exception("Failed to enrich event %s", event.event_id)
            insert_dlq(event, "enrichment_failed", exc, event_prompt_payload(event))

    insert_enrichments(enriched_rows)
    insert_cost(
        run_id,
        "dry-run" if dry_run else provider,
        "dry-run" if dry_run else model,
        total_input_tokens,
        total_output_tokens,
        len(enriched_rows),
    )
    return {
        "run_id": run_id,
        "selected": len(events),
        "enriched": len(enriched_rows),
        "input_tokens": total_input_tokens,
        "output_tokens": total_output_tokens,
        "estimated_cost_usd": estimate_cost(total_input_tokens, total_output_tokens),
        "cache_size": len(local_cache),
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--dry-run", action="store_true", help="Use deterministic local labels without calling an LLM.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = parse_args(argv or sys.argv[1:])
    result = run(limit=args.limit, dry_run=args.dry_run)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
