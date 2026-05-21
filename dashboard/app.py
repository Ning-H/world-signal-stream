from __future__ import annotations

import os
from datetime import UTC, datetime

import clickhouse_connect
import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh


SOURCE_COLORS = {
    "wikipedia": "#3b82f6",
    "gdelt": "#ef4444",
    "hackernews": "#f59e0b",
}
SOURCES = ["wikipedia", "gdelt", "hackernews"]


@st.cache_resource
def clickhouse_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "opensignal"),
    )


@st.cache_data(ttl=5)
def query_df(sql: str) -> pd.DataFrame:
    return clickhouse_client().query_df(sql)


def render_metrics() -> None:
    df = query_df(
        """
        SELECT
            source,
            count() AS events,
            max(ingested_at) AS latest,
            countIf(geography_hint IS NOT NULL) AS with_geo
        FROM events_raw
        GROUP BY source
        ORDER BY source
        """
    )

    cols = st.columns(3)
    for index, source in enumerate(SOURCES):
        row = df[df["source"] == source]
        events = int(row["events"].iloc[0]) if not row.empty else 0
        latest = row["latest"].iloc[0] if not row.empty else "none"
        with_geo = int(row["with_geo"].iloc[0]) if not row.empty else 0
        geo_label = f"{with_geo:,} geo hints"
        cols[index].metric(source.title(), f"{events:,}", delta=geo_label, help=f"Latest local ingest: {latest}")


def render_firehose() -> None:
    st.subheader("Balanced Event Firehose")
    df = query_df(
        """
        WITH ranked AS
        (
            SELECT
                ingested_at,
                timestamp,
                source,
                source_subtype,
                language,
                geography_hint,
                title,
                actor,
                magnitude,
                content_url,
                row_number() OVER (PARTITION BY source ORDER BY ingested_at DESC) AS source_rank
            FROM events_raw
        )
        SELECT
            ingested_at,
            timestamp,
            source,
            source_subtype,
            language,
            geography_hint,
            title,
            actor,
            magnitude,
            content_url
        FROM ranked
        WHERE source_rank <= 35
        ORDER BY source, ingested_at DESC
        """
    )
    st.caption("Shows the latest rows from each source, so a recently-run producer does not hide quieter or older local sources.")
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "content_url": st.column_config.LinkColumn("content_url"),
            "title": st.column_config.TextColumn("title", width="large"),
        },
    )


def render_volume() -> None:
    st.subheader("Volume By Source, Last 24h")
    df = query_df(
        """
        SELECT
            bucket,
            source,
            sum(event_count) AS events
        FROM source_volume_5m
        WHERE bucket >= now() - INTERVAL 24 HOUR
        GROUP BY bucket, source
        ORDER BY bucket, source
        """
    )
    if df.empty:
        st.info("No volume data yet.")
        return

    fig = px.area(
        df,
        x="bucket",
        y="events",
        color="source",
        color_discrete_map=SOURCE_COLORS,
    )
    fig.update_layout(
        height=360,
        margin=dict(l=12, r=12, t=20, b=12),
        legend_title_text="",
        xaxis_title="",
        yaxis_title="Events",
    )
    st.plotly_chart(fig, use_container_width=True)


def top_titles(source: str, label: str, limit: int = 15) -> None:
    df = query_df(
        f"""
        WITH latest AS
        (
            SELECT max(bucket) AS max_bucket
            FROM title_activity_5m
            WHERE source = '{source}'
        )
        SELECT
            title,
            sum(event_count) AS events,
            sum(magnitude_sum) AS magnitude
        FROM title_activity_5m
        WHERE
            source = '{source}'
            AND bucket >= (SELECT max_bucket FROM latest) - INTERVAL 1 HOUR
        GROUP BY title
        ORDER BY events DESC, magnitude DESC
        LIMIT {limit}
        """
    )
    st.markdown(f"**{label}**")
    if df.empty:
        st.caption("No recent events.")
        return

    for _, row in df.iterrows():
        events = int(row["events"])
        magnitude = int(row["magnitude"])
        title = str(row["title"])
        st.write(f"{events:,} events · {magnitude:,} magnitude")
        st.caption(title)


def render_top_titles() -> None:
    st.subheader("Top Titles By Source, Latest Available 1h")
    st.caption("Each column uses that source's latest available local timestamp. Producers are run manually during Stage 1, so one global wall-clock hour would hide inactive sources.")
    cols = st.columns(3)
    with cols[0]:
        top_titles("wikipedia", "Wikipedia Articles")
    with cols[1]:
        top_titles("gdelt", "GDELT Actors/Events")
    with cols[2]:
        top_titles("hackernews", "Hacker News Stories")


def render_geography() -> None:
    st.subheader("Geography Hints")
    coverage = query_df(
        """
        SELECT
            source,
            count() AS events,
            countIf(geography_hint IS NOT NULL) AS with_geo,
            round(100 * with_geo / events, 1) AS geo_pct
        FROM events_raw
        GROUP BY source
        ORDER BY source
        """
    )
    st.caption("GDELT provides source-level country codes. Wikipedia and Hacker News do not provide reliable geography in raw ingestion; those are left for enrichment.")
    st.dataframe(coverage, use_container_width=True, hide_index=True)

    gdelt_geo = query_df(
        """
        SELECT
            geography_hint,
            count() AS events
        FROM events_raw
        WHERE source = 'gdelt' AND geography_hint IS NOT NULL
        GROUP BY geography_hint
        ORDER BY events DESC
        LIMIT 20
        """
    )
    if not gdelt_geo.empty:
        fig = px.bar(
            gdelt_geo,
            x="geography_hint",
            y="events",
            color_discrete_sequence=[SOURCE_COLORS["gdelt"]],
        )
        fig.update_layout(
            height=300,
            margin=dict(l=12, r=12, t=20, b=12),
            xaxis_title="Country code",
            yaxis_title="GDELT events",
        )
        st.plotly_chart(fig, use_container_width=True)


def main() -> None:
    st.set_page_config(
        page_title="OpenSignal",
        page_icon="",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    refresh_seconds = int(os.getenv("STREAMLIT_AUTO_REFRESH_SECONDS", "5"))
    st_autorefresh(interval=refresh_seconds * 1000, key="opensignal_refresh")

    st.title("OpenSignal")
    st.caption(
        "A local, real-time dashboard for public attention signals across Wikipedia, GDELT, and Hacker News."
    )

    render_metrics()
    st.divider()
    render_firehose()
    st.divider()
    render_volume()
    st.divider()
    render_geography()
    st.divider()
    render_top_titles()

    st.caption(f"Last refreshed at {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}")


if __name__ == "__main__":
    main()
