import os
import psycopg2
import pandas as pd
import streamlit as st
import plotly.express as px
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="DataXplore Pipeline", layout="wide")

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


@st.cache_data(ttl=60)
def load_data():
    conn = psycopg2.connect(**DB_CONFIG)

    raw = pd.read_sql("""
        SELECT source_channel, DATE(ingested_at) as date, COUNT(*) as posts
        FROM raw_posts
        GROUP BY source_channel, DATE(ingested_at)
        ORDER BY date DESC
    """, conn)

    processed = pd.read_sql("""
        SELECT topic_tag, status, composite_score, depth_score,
               relevance_score, novelty_score, processed_at
        FROM processed_posts
    """, conn)

    scores = pd.read_sql("""
        SELECT composite_score, status, topic_tag
        FROM processed_posts
        WHERE composite_score IS NOT NULL
    """, conn)

    conn.close()
    return raw, processed, scores


st.title("DataXplore Pipeline Dashboard")
st.caption("Live view of your content pipeline")

try:
    raw, processed, scores = load_data()

# --- Top metrics ---
    total_ingested = int(raw["posts"].sum()) if not raw.empty else 0
    total_scored = len(processed)
    total_approved = len(processed[processed["status"] == "approved"]) if not processed.empty else 0
    avg_score = f"{processed['composite_score'].mean():.2f}" if not processed.empty else "N/A"

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Ingested", total_ingested)
    col2.metric("Total Scored", total_scored)
    col3.metric("Auto Approved", total_approved)
    col4.metric("Avg Score", avg_score)

    st.divider()

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Posts Ingested by Channel")
        if not raw.empty:
            fig = px.bar(raw, x="date", y="posts", color="source_channel",
                        title="Daily Ingestion Volume")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No ingestion data yet.")

    with col_right:
        st.subheader("Score Distribution")
        if not scores.empty:
            fig2 = px.histogram(scores, x="composite_score", nbins=20,
                               color="status", title="Composite Score Distribution")
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No scored posts yet.")

    st.divider()

    col_left2, col_right2 = st.columns(2)

    with col_left2:
        st.subheader("Posts by Topic Tag")
        if not processed.empty and "topic_tag" in processed.columns:
            topic_counts = processed["topic_tag"].value_counts().reset_index()
            topic_counts.columns = ["topic", "count"]
            fig3 = px.pie(topic_counts, names="topic", values="count",
                         title="Topic Distribution")
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("No topic data yet.")

    with col_right2:
        st.subheader("Approval Rate")
        if not processed.empty:
            status_counts = processed["status"].value_counts().reset_index()
            status_counts.columns = ["status", "count"]
            fig4 = px.pie(status_counts, names="status", values="count",
                         color="status",
                         color_discrete_map={
                             "approved": "#00cc44",
                             "manual_review": "#ffaa00",
                             "rejected": "#ff4444"
                         },
                         title="Post Status Breakdown")
            st.plotly_chart(fig4, use_container_width=True)
        else:
            st.info("No processed posts yet.")

    st.divider()
    st.subheader("Recent Scored Posts")
    if not processed.empty:
        st.dataframe(
            processed.sort_values("processed_at", ascending=False).head(20),
            use_container_width=True
        )
    else:
        st.info("Run the scorer to see data here.")

except Exception as e:
    st.error(f"Database connection failed: {e}")
    st.info("Make sure PostgreSQL is running and your .env is configured.")